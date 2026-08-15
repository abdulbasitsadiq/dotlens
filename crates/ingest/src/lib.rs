//! Ingestion support: checkpoints and idempotent processing guards.
//!
//! Every module tracks its own checkpoint per chain (`indexer_state`), restarts
//! resume from it, and re-processing an already-processed block is a no-op
//! (ARCHITECTURE.md §9/§15). Live fetchers arrive in Phase 1; the checkpoint
//! contract they'll run on is proven here.

use chrono::{DateTime, Utc};
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

pub trait CheckpointStore: Send + Sync {
    fn get(&self, chain_id: &str, module: &str) -> Result<Option<Checkpoint>, CheckpointError>;
    /// Advance the checkpoint. Never moves backwards (returns `Regression`);
    /// explicit rollback (reorg handling, Phase 1) will be a separate operation.
    fn advance(&self, cp: Checkpoint) -> Result<(), CheckpointError>;
}

/// Idempotency guard shared by all modules: should `height` be processed?
pub fn should_process(
    store: &dyn CheckpointStore,
    chain_id: &str,
    module: &str,
    height: u64,
) -> Result<IngestOutcome, CheckpointError> {
    match store.get(chain_id, module)? {
        Some(cp) if height <= cp.last_height => Ok(IngestOutcome::AlreadyProcessed),
        _ => Ok(IngestOutcome::Processed),
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

impl CheckpointStore for MemoryCheckpointStore {
    fn get(&self, chain_id: &str, module: &str) -> Result<Option<Checkpoint>, CheckpointError> {
        let map = self.inner.lock().map_err(|e| CheckpointError::Storage(e.to_string()))?;
        Ok(map.get(&(chain_id.to_string(), module.to_string())).cloned())
    }

    fn advance(&self, cp: Checkpoint) -> Result<(), CheckpointError> {
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
    use super::{Checkpoint, CheckpointError};
    use chrono::{DateTime, Utc};
    use sqlx::PgPool;

    /// Postgres-backed checkpoints over `core.indexer_state` (migration 0001).
    /// Runtime queries only — no compile-time DB needed.
    ///
    /// NOTE (Phase 0 honesty): this impl is async and therefore does NOT yet
    /// implement the sync `CheckpointStore` trait — the Phase 0 node uses
    /// `MemoryCheckpointStore`. When live ingestion lands in Phase 1 the
    /// checkpoint contract becomes async end-to-end and this store gets wired
    /// in as the persistent backend (with kill/restart integration tests).
    pub struct PgCheckpointStore {
        pool: PgPool,
    }

    impl PgCheckpointStore {
        pub fn new(pool: PgPool) -> Self {
            Self { pool }
        }

        pub async fn get(
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

        pub async fn advance(&self, cp: Checkpoint) -> Result<(), CheckpointError> {
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
                let have = self
                    .get(&cp.chain_id, &cp.module)
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

    #[test]
    fn fresh_chain_processes_then_skips_duplicates() {
        let store = MemoryCheckpointStore::new();
        assert_eq!(
            should_process(&store, "polkadot-asset-hub", "blocks", 100).unwrap(),
            IngestOutcome::Processed
        );
        store.advance(cp("polkadot-asset-hub", "blocks", 100)).unwrap();
        // same block again → idempotent skip
        assert_eq!(
            should_process(&store, "polkadot-asset-hub", "blocks", 100).unwrap(),
            IngestOutcome::AlreadyProcessed
        );
        // next block → processed
        assert_eq!(
            should_process(&store, "polkadot-asset-hub", "blocks", 101).unwrap(),
            IngestOutcome::Processed
        );
    }

    #[test]
    fn checkpoints_are_per_module_and_per_chain() {
        let store = MemoryCheckpointStore::new();
        store.advance(cp("polkadot-asset-hub", "blocks", 500)).unwrap();
        assert_eq!(
            should_process(&store, "polkadot-asset-hub", "governance", 100).unwrap(),
            IngestOutcome::Processed
        );
        assert_eq!(
            should_process(&store, "polkadot", "blocks", 100).unwrap(),
            IngestOutcome::Processed
        );
    }

    #[test]
    fn regression_is_refused() {
        let store = MemoryCheckpointStore::new();
        store.advance(cp("polkadot", "blocks", 200)).unwrap();
        assert!(matches!(
            store.advance(cp("polkadot", "blocks", 150)),
            Err(CheckpointError::Regression { .. })
        ));
    }

    /// Simulated kill/restart: a new store view over the same state resumes
    /// exactly where the old one stopped. (With MemoryCheckpointStore the
    /// state IS the store; the Pg impl gives real persistence in Phase 1 tests.)
    #[test]
    fn restart_resumes_from_checkpoint() {
        let store = MemoryCheckpointStore::new();
        for h in 1..=50u64 {
            if should_process(&store, "polkadot", "blocks", h).unwrap() == IngestOutcome::Processed {
                store.advance(cp("polkadot", "blocks", h)).unwrap();
            }
        }
        // "restart": re-offer the whole range; only new heights process
        let mut processed_again = 0;
        for h in 1..=60u64 {
            if should_process(&store, "polkadot", "blocks", h).unwrap() == IngestOutcome::Processed {
                store.advance(cp("polkadot", "blocks", h)).unwrap();
                processed_again += 1;
            }
        }
        assert_eq!(processed_again, 10); // exactly 51..=60
    }
}
