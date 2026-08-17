//! Ingestion support: checkpoints, receipt sinks, and idempotent processing guards.
//!
//! Every module tracks its own checkpoint per chain (`indexer_state`), restarts
//! resume from it, and re-processing an already-processed block is a no-op
//! (ARCHITECTURE.md §9/§15). The contract is async end-to-end since Phase 1 so
//! the Postgres backend is a first-class implementation, not a bolt-on.

pub mod balances;
pub mod bounties;
pub mod decode;
pub mod gov;
pub mod live;
pub mod module;
pub mod tip;
pub mod treasury;
pub mod votes;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use raw_store::IngestReceipt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug, thiserror::Error)]
pub enum CheckpointError {
    #[error("storage error: {0}")]
    Storage(String),
    #[error(
        "checkpoint regression for {chain}/{module}: have {have}, attempted {attempted} \
         (rollbacks must be explicit, not accidental)"
    )]
    Regression {
        chain: String,
        module: String,
        have: u64,
        attempted: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    pub chain_id: String,
    pub module: String,
    pub last_height: u64,
    pub last_hash: String,
    pub updated_at: DateTime<Utc>,
}

/// What happened when a block was offered to a module.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngestOutcome {
    Processed,
    /// Height at or below the checkpoint — idempotent skip.
    AlreadyProcessed,
}

#[async_trait]
pub trait CheckpointStore: Send + Sync {
    async fn get(&self, chain_id: &str, module: &str)
        -> Result<Option<Checkpoint>, CheckpointError>;
    /// Advance the checkpoint. Never moves backwards (returns `Regression`);
    /// explicit rollback (reorg handling, later in Phase 1) will be a separate
    /// operation.
    async fn advance(&self, cp: Checkpoint) -> Result<(), CheckpointError>;
}

/// Idempotency guard shared by all modules: should `height` be processed?
pub async fn should_process(
    store: &dyn CheckpointStore,
    chain_id: &str,
    module: &str,
    height: u64,
) -> Result<IngestOutcome, CheckpointError> {
    match store.get(chain_id, module).await? {
        Some(cp) if height <= cp.last_height => Ok(IngestOutcome::AlreadyProcessed),
        _ => Ok(IngestOutcome::Processed),
    }
}

// ------------------------------------------------------------- receipt sinks

#[derive(Debug, thiserror::Error)]
#[error("receipt sink error: {0}")]
pub struct ReceiptError(pub String);

/// Where raw-store receipts get durably recorded (`core.ingest_receipts`).
/// Provenance of the FIRST fetch wins: recording the same key again is a no-op.
#[async_trait]
pub trait ReceiptSink: Send + Sync {
    async fn record(&self, receipt: &IngestReceipt) -> Result<(), ReceiptError>;
}

/// For DB-less runs (Phase 0 mode): receipts are observable in logs only.
#[derive(Default)]
pub struct NoopReceiptSink;

#[async_trait]
impl ReceiptSink for NoopReceiptSink {
    async fn record(&self, _receipt: &IngestReceipt) -> Result<(), ReceiptError> {
        Ok(())
    }
}

// ---------------------------------------------------------------- memory impl

#[derive(Default)]
pub struct MemoryCheckpointStore {
    inner: Mutex<HashMap<(String, String), Checkpoint>>,
}

impl MemoryCheckpointStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl CheckpointStore for MemoryCheckpointStore {
    async fn get(
        &self,
        chain_id: &str,
        module: &str,
    ) -> Result<Option<Checkpoint>, CheckpointError> {
        let map = self.inner.lock().map_err(|e| CheckpointError::Storage(e.to_string()))?;
        Ok(map.get(&(chain_id.to_string(), module.to_string())).cloned())
    }

    async fn advance(&self, cp: Checkpoint) -> Result<(), CheckpointError> {
        let mut map = self.inner.lock().map_err(|e| CheckpointError::Storage(e.to_string()))?;
        let key = (cp.chain_id.clone(), cp.module.clone());
        if let Some(existing) = map.get(&key) {
            if cp.last_height <= existing.last_height {
                return Err(CheckpointError::Regression {
                    chain: cp.chain_id,
                    module: cp.module,
                    have: existing.last_height,
                    attempted: cp.last_height,
                });
            }
        }
        map.insert(key, cp);
        Ok(())
    }
}

// -------------------------------------------------------------------- pg impl

#[cfg(feature = "pg")]
pub mod pg {
    use super::{Checkpoint, CheckpointError, ReceiptError, ReceiptSink};
    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use raw_store::IngestReceipt;
    use sqlx::PgPool;

    /// Postgres-backed checkpoints over `core.indexer_state` (migration 0001).
    /// Runtime queries only — no compile-time DB needed.
    pub struct PgCheckpointStore {
        pool: PgPool,
    }

    impl PgCheckpointStore {
        pub fn new(pool: PgPool) -> Self {
            Self { pool }
        }
    }

    #[async_trait]
    impl super::CheckpointStore for PgCheckpointStore {
        async fn get(
            &self,
            chain_id: &str,
            module: &str,
        ) -> Result<Option<Checkpoint>, CheckpointError> {
            let row: Option<(String, String, i64, String, DateTime<Utc>)> = sqlx::query_as(
                "select chain_id, module, last_height, last_hash, updated_at \
                 from core.indexer_state where chain_id = $1 and module = $2",
            )
            .bind(chain_id)
            .bind(module)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| CheckpointError::Storage(e.to_string()))?;
            Ok(row.map(|(chain_id, module, h, last_hash, updated_at)| Checkpoint {
                chain_id,
                module,
                last_height: h as u64,
                last_hash,
                updated_at,
            }))
        }

        async fn advance(&self, cp: Checkpoint) -> Result<(), CheckpointError> {
            // single-statement conditional upsert: never regress.
            // binds cp.updated_at so both impls of the contract agree.
            let res = sqlx::query(
                "insert into core.indexer_state (chain_id, module, last_height, last_hash, updated_at) \
                 values ($1, $2, $3, $4, $5) \
                 on conflict (chain_id, module) do update \
                 set last_height = excluded.last_height, \
                     last_hash = excluded.last_hash, \
                     updated_at = excluded.updated_at \
                 where core.indexer_state.last_height < excluded.last_height",
            )
            .bind(&cp.chain_id)
            .bind(&cp.module)
            .bind(cp.last_height as i64)
            .bind(&cp.last_hash)
            .bind(cp.updated_at)
            .execute(&self.pool)
            .await
            .map_err(|e| CheckpointError::Storage(e.to_string()))?;
            if res.rows_affected() == 0 {
                let have = super::CheckpointStore::get(self, &cp.chain_id, &cp.module)
                    .await?
                    .map(|c| c.last_height)
                    .unwrap_or(0);
                return Err(CheckpointError::Regression {
                    chain: cp.chain_id,
                    module: cp.module,
                    have,
                    attempted: cp.last_height,
                });
            }
            Ok(())
        }
    }

    /// Durable provenance over `core.ingest_receipts` (migration 0001 + 0003).
    /// First fetch wins: `on conflict do nothing` keeps original provenance.
    pub struct PgReceiptSink {
        pool: PgPool,
    }

    impl PgReceiptSink {
        pub fn new(pool: PgPool) -> Self {
            Self { pool }
        }
    }

    #[async_trait]
    impl ReceiptSink for PgReceiptSink {
        async fn record(&self, receipt: &IngestReceipt) -> Result<(), ReceiptError> {
            sqlx::query(
                "insert into core.ingest_receipts (key, byte_len, source, content_hash, fetched_at) \
                 values ($1, $2, $3, $4, $5) on conflict (key) do nothing",
            )
            .bind(&receipt.key)
            .bind(receipt.byte_len as i64)
            .bind(&receipt.source)
            .bind(&receipt.content_hash)
            .bind(receipt.fetched_at)
            .execute(&self.pool)
            .await
            .map_err(|e| ReceiptError(e.to_string()))?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cp(chain: &str, module: &str, h: u64) -> Checkpoint {
        Checkpoint {
            chain_id: chain.into(),
            module: module.into(),
            last_height: h,
            last_hash: format!("0x{h:064x}"),
            updated_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn fresh_chain_processes_then_skips_duplicates() {
        let store = MemoryCheckpointStore::new();
        assert_eq!(
            should_process(&store, "polkadot-asset-hub", "blocks", 100).await.unwrap(),
            IngestOutcome::Processed
        );
        store.advance(cp("polkadot-asset-hub", "blocks", 100)).await.unwrap();
        // same block again → idempotent skip
        assert_eq!(
            should_process(&store, "polkadot-asset-hub", "blocks", 100).await.unwrap(),
            IngestOutcome::AlreadyProcessed
        );
        // next block → processed
        assert_eq!(
            should_process(&store, "polkadot-asset-hub", "blocks", 101).await.unwrap(),
            IngestOutcome::Processed
        );
    }

    #[tokio::test]
    async fn checkpoints_are_per_module_and_per_chain() {
        let store = MemoryCheckpointStore::new();
        store.advance(cp("polkadot-asset-hub", "blocks", 500)).await.unwrap();
        assert_eq!(
            should_process(&store, "polkadot-asset-hub", "governance", 100).await.unwrap(),
            IngestOutcome::Processed
        );
        assert_eq!(
            should_process(&store, "polkadot", "blocks", 100).await.unwrap(),
            IngestOutcome::Processed
        );
    }

    #[tokio::test]
    async fn regression_is_refused() {
        let store = MemoryCheckpointStore::new();
        store.advance(cp("polkadot", "blocks", 200)).await.unwrap();
        assert!(matches!(
            store.advance(cp("polkadot", "blocks", 150)).await,
            Err(CheckpointError::Regression { .. })
        ));
    }

    /// Simulated kill/restart: a new store view over the same state resumes
    /// exactly where the old one stopped. (With MemoryCheckpointStore the
    /// state IS the store; the Pg integration tests in dotlens-node prove
    /// real persistence across store instances.)
    #[tokio::test]
    async fn restart_resumes_from_checkpoint() {
        let store = MemoryCheckpointStore::new();
        for h in 1..=50u64 {
            if should_process(&store, "polkadot", "blocks", h).await.unwrap()
                == IngestOutcome::Processed
            {
                store.advance(cp("polkadot", "blocks", h)).await.unwrap();
            }
        }
        // "restart": re-offer the whole range; only new heights process
        let mut processed_again = 0;
        for h in 1..=60u64 {
            if should_process(&store, "polkadot", "blocks", h).await.unwrap()
                == IngestOutcome::Processed
            {
                store.advance(cp("polkadot", "blocks", h)).await.unwrap();
                processed_again += 1;
            }
        }
        assert_eq!(processed_again, 10); // exactly 51..=60
    }
}
