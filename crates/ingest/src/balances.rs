//! Balances worker: walk canonical events → per-account balance deltas
//! (ARCHITECTURE.md §9: an independent module consuming from its own
//! checkpoint, writing only to its own schema, idempotent restarts).
//!
//! The worker is family-agnostic: WHAT an event means for balances lives
//! behind `DeltaMapper` in adapter crates (Invariant 4). It chases the decode
//! worker's `blocks` checkpoint the same way decode chases `raw_blocks`.
//!
//! Deltas track the TOTAL balance (free + reserved): reserve/freeze moves
//! within one account don't change total and map to nothing.
//!
//! Checkpoint module: `balances`. Like decode, the checkpoint is a frontier —
//! `balances_range` may reprocess anything behind it (inserts are
//! conflict-ignored, cheap) and never regresses it; heights the canonical
//! store hasn't decoded yet are skipped and advanced past (decode gap-fill +
//! a later `balances-range` re-run covers them).

use crate::decode::MODULE_DECODE;
use crate::{Checkpoint, CheckpointError, CheckpointStore};
use async_trait::async_trait;
use canonical::CanonicalEvent;

pub const MODULE_BALANCES: &str = "balances";

#[derive(Debug, thiserror::Error)]
pub enum BalancesWorkerError {
    #[error("event source: {0}")]
    Source(String),
    #[error("delta sink: {0}")]
    Sink(String),
    #[error(
        "mapper failed for {chain}/{height} event {event_index} ({event}): {reason} — \
         balances halt loudly rather than record gaps in money"
    )]
    Mapper {
        chain: String,
        height: u64,
        event_index: u32,
        event: String,
        reason: String,
    },
    #[error(transparent)]
    Checkpoint(#[from] CheckpointError),
}

/// One account's balance movement from one event. Amounts are unsigned
/// magnitude + sign so u128-scale balances never squeeze through i128.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BalanceDelta {
    /// Raw account bytes (32 for Substrate; family-encoded).
    pub account: Vec<u8>,
    pub magnitude: u128,
    pub negative: bool,
    /// e.g. "transfer_out", "deposit" — the mapper's vocabulary.
    pub reason: String,
    /// Transfer peer, if any.
    pub counterparty: Option<Vec<u8>>,
    /// "native" for the balances pallet; assets module extends this later.
    pub asset: String,
}

/// Event → deltas. Pure; errors are LOUD (a malformed money event must halt,
/// not silently drop). `mapper_version` is lineage: bump on any rule change,
/// rebuild rows from canonical events.
pub trait DeltaMapper: Send + Sync {
    fn deltas(&self, event: &CanonicalEvent) -> Result<Vec<BalanceDelta>, String>;
    fn mapper_version(&self) -> u32;
}

/// The decoded events of one canonical block, as the worker needs them.
pub struct BlockEvents {
    pub runtime_version: u32,
    pub events: Vec<CanonicalEvent>,
}

/// Where canonical events come from (node-side: core.blocks + core.events).
/// `None` = this height isn't decoded (a decode gap) — skip, don't fail.
#[async_trait]
pub trait EventSource: Send + Sync {
    async fn decoded_events(
        &self,
        chain_id: &str,
        height: u64,
    ) -> Result<Option<BlockEvents>, String>;
}

/// Where deltas land (node-side: balances.balance_changes, insert-ignore).
#[async_trait]
pub trait DeltaSink: Send + Sync {
    async fn write(
        &self,
        chain_id: &str,
        height: u64,
        runtime_version: u32,
        mapper_version: u32,
        rows: &[(u32, BalanceDelta)], // (event_index, delta)
    ) -> Result<(), String>;
}

pub struct BalancesDeps<'a> {
    pub checkpoints: &'a dyn CheckpointStore,
    pub source: &'a dyn EventSource,
    pub sink: &'a dyn DeltaSink,
}

/// Map heights `from..=to`. Heights behind the frontier are reprocessed
/// freely (insert-ignore makes it a no-op) without touching the checkpoint;
/// past it, rows first, checkpoint last (crash = re-map, never skip).
pub async fn balances_range(
    chain_id: &str,
    mapper: &dyn DeltaMapper,
    deps: &BalancesDeps<'_>,
    from: u64,
    to: u64,
) -> Result<u64, BalancesWorkerError> {
    let frontier = deps
        .checkpoints
        .get(chain_id, MODULE_BALANCES)
        .await?
        .map(|cp| cp.last_height);
    let mut processed = 0u64;

    for height in from..=to {
        let behind_frontier = frontier.is_some_and(|f| height <= f);
        let block = deps
            .source
            .decoded_events(chain_id, height)
            .await
            .map_err(BalancesWorkerError::Source)?;
        let Some(block) = block else {
            // decode gap: skip. Ahead of the frontier we still advance so the
            // follower never wedges on a hole; the height gets its deltas when
            // decode gap-fill + balances-range revisit it.
            tracing::debug!(chain = %chain_id, height, "no canonical block — skipped");
            if !behind_frontier {
                advance(deps, chain_id, height, "-").await?;
            }
            continue;
        };

        let mut rows: Vec<(u32, BalanceDelta)> = Vec::new();
        for ev in &block.events {
            let deltas =
                mapper
                    .deltas(ev)
                    .map_err(|reason| BalancesWorkerError::Mapper {
                        chain: chain_id.to_string(),
                        height,
                        event_index: ev.index,
                        event: ev.name.clone(),
                        reason,
                    })?;
            for d in deltas {
                rows.push((ev.index, d));
            }
        }
        if !rows.is_empty() {
            deps.sink
                .write(
                    chain_id,
                    height,
                    block.runtime_version,
                    mapper.mapper_version(),
                    &rows,
                )
                .await
                .map_err(BalancesWorkerError::Sink)?;
        }
        if !behind_frontier {
            advance(deps, chain_id, height, "-").await?;
        }
        processed += 1;
    }
    Ok(processed)
}

async fn advance(
    deps: &BalancesDeps<'_>,
    chain_id: &str,
    height: u64,
    hash: &str,
) -> Result<(), CheckpointError> {
    deps.checkpoints
        .advance(Checkpoint {
            chain_id: chain_id.to_string(),
            module: MODULE_BALANCES.to_string(),
            last_height: height,
            last_hash: hash.to_string(),
            updated_at: chrono::Utc::now(),
        })
        .await
}

/// One follower step: chase the decode (`blocks`) checkpoint. First run starts
/// at the decode tip — history is `balances-range`'s job (same first-run
/// semantics as decode_tick vs raw).
pub async fn balances_tick(
    chain_id: &str,
    mapper: &dyn DeltaMapper,
    deps: &BalancesDeps<'_>,
) -> Result<u64, BalancesWorkerError> {
    let Some(decode_cp) = deps.checkpoints.get(chain_id, MODULE_DECODE).await? else {
        return Ok(0); // nothing decoded yet
    };
    let target = decode_cp.last_height;
    let from = match deps.checkpoints.get(chain_id, MODULE_BALANCES).await? {
        Some(cp) if cp.last_height >= target => return Ok(0),
        Some(cp) => cp.last_height + 1,
        None => target, // first run: start at the decode tip
    };
    balances_range(chain_id, mapper, deps, from, target).await
}

/// Follow forever, same backoff discipline as the other followers.
pub async fn balances_follow(
    chain_id: &str,
    mapper: &dyn DeltaMapper,
    deps: &BalancesDeps<'_>,
    poll: std::time::Duration,
) {
    let mut consecutive_failures = 0u32;
    loop {
        match balances_tick(chain_id, mapper, deps).await {
            Ok(n) => {
                consecutive_failures = 0;
                if n > 0 {
                    tracing::debug!(chain = %chain_id, blocks = n, "balances tick");
                }
            }
            Err(e) => {
                consecutive_failures += 1;
                tracing::warn!(chain = %chain_id, error = %e, consecutive_failures, "balances tick failed");
            }
        }
        let factor = 1 + consecutive_failures.min(10);
        tokio::time::sleep(poll * factor).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryCheckpointStore;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// Mock source: heights present in the map are "decoded".
    struct MemSource(HashMap<u64, Vec<CanonicalEvent>>);

    #[async_trait]
    impl EventSource for MemSource {
        async fn decoded_events(
            &self,
            _chain: &str,
            height: u64,
        ) -> Result<Option<BlockEvents>, String> {
            Ok(self.0.get(&height).map(|events| BlockEvents {
                runtime_version: 100,
                events: events.clone(),
            }))
        }
    }

    /// Mock mapper: event data {"who": [..], "amount": n, "neg": bool} → one delta.
    struct MockMapper;
    impl DeltaMapper for MockMapper {
        fn deltas(&self, event: &CanonicalEvent) -> Result<Vec<BalanceDelta>, String> {
            if event.name == "mock.Bad" {
                return Err("unmappable".into());
            }
            if event.name != "mock.Move" {
                return Ok(vec![]);
            }
            let amount = event.data["amount"].as_u64().ok_or("no amount")? as u128;
            Ok(vec![BalanceDelta {
                account: vec![7u8; 32],
                magnitude: amount,
                negative: event.data["neg"].as_bool().unwrap_or(false),
                reason: "mock".into(),
                counterparty: None,
                asset: "native".into(),
            }])
        }
        fn mapper_version(&self) -> u32 {
            1
        }
    }

    #[derive(Default)]
    struct MemSink(Mutex<Vec<(u64, u32, BalanceDelta)>>);
    #[async_trait]
    impl DeltaSink for MemSink {
        async fn write(
            &self,
            _chain: &str,
            height: u64,
            _rv: u32,
            _mv: u32,
            rows: &[(u32, BalanceDelta)],
        ) -> Result<(), String> {
            let mut g = self.0.lock().unwrap();
            for (idx, d) in rows {
                g.push((height, *idx, d.clone()));
            }
            Ok(())
        }
    }

    fn ev(index: u32, name: &str, data: serde_json::Value) -> CanonicalEvent {
        CanonicalEvent {
            index,
            transaction_index: None,
            name: name.into(),
            data,
        }
    }

    #[tokio::test]
    async fn range_maps_events_skips_gaps_and_advances() {
        let mut src = HashMap::new();
        src.insert(1, vec![ev(0, "mock.Move", serde_json::json!({"amount": 5}))]);
        // height 2 is a decode gap
        src.insert(3, vec![
            ev(0, "mock.Other", serde_json::json!({})),
            ev(1, "mock.Move", serde_json::json!({"amount": 9, "neg": true})),
        ]);
        let checkpoints = MemoryCheckpointStore::new();
        let sink = MemSink::default();
        let deps = BalancesDeps {
            checkpoints: &checkpoints,
            source: &MemSource(src),
            sink: &sink,
        };
        let n = balances_range("mock", &MockMapper, &deps, 1, 3).await.unwrap();
        assert_eq!(n, 2, "two decoded heights, one gap skipped");
        let rows = sink.0.lock().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, 1);
        assert!(!rows[0].2.negative);
        assert_eq!(rows[1], (3, 1, BalanceDelta {
            account: vec![7u8; 32],
            magnitude: 9,
            negative: true,
            reason: "mock".into(),
            counterparty: None,
            asset: "native".into(),
        }));
        drop(rows);
        // checkpoint advanced through the gap
        let cp = checkpoints.get("mock", MODULE_BALANCES).await.unwrap().unwrap();
        assert_eq!(cp.last_height, 3);
    }

    #[tokio::test]
    async fn rerun_behind_frontier_rewrites_without_moving_checkpoint() {
        let mut src = HashMap::new();
        src.insert(1, vec![ev(0, "mock.Move", serde_json::json!({"amount": 5}))]);
        src.insert(2, vec![]);
        let checkpoints = MemoryCheckpointStore::new();
        let sink = MemSink::default();
        let deps = BalancesDeps {
            checkpoints: &checkpoints,
            source: &MemSource(src),
            sink: &sink,
        };
        balances_range("mock", &MockMapper, &deps, 1, 2).await.unwrap();
        let n = balances_range("mock", &MockMapper, &deps, 1, 1).await.unwrap();
        assert_eq!(n, 1, "behind-frontier reprocess is allowed (sink dedupes)");
        let cp = checkpoints.get("mock", MODULE_BALANCES).await.unwrap().unwrap();
        assert_eq!(cp.last_height, 2, "frontier untouched by reprocess");
    }

    #[tokio::test]
    async fn mapper_errors_halt_loudly_and_advance_nothing() {
        let mut src = HashMap::new();
        src.insert(1, vec![ev(0, "mock.Bad", serde_json::json!({}))]);
        let checkpoints = MemoryCheckpointStore::new();
        let sink = MemSink::default();
        let deps = BalancesDeps {
            checkpoints: &checkpoints,
            source: &MemSource(src),
            sink: &sink,
        };
        let err = balances_range("mock", &MockMapper, &deps, 1, 1).await.unwrap_err();
        assert!(matches!(err, BalancesWorkerError::Mapper { height: 1, .. }));
        assert!(sink.0.lock().unwrap().is_empty());
        assert!(checkpoints.get("mock", MODULE_BALANCES).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn tick_chases_the_decode_checkpoint() {
        let mut src = HashMap::new();
        for h in 5..=9 {
            src.insert(h, vec![ev(0, "mock.Move", serde_json::json!({"amount": h}))]);
        }
        let checkpoints = MemoryCheckpointStore::new();
        let sink = MemSink::default();
        let deps = BalancesDeps {
            checkpoints: &checkpoints,
            source: &MemSource(src),
            sink: &sink,
        };
        // nothing decoded yet → no-op
        assert_eq!(balances_tick("mock", &MockMapper, &deps).await.unwrap(), 0);

        let decode_cp = |h: u64| Checkpoint {
            chain_id: "mock".into(),
            module: MODULE_DECODE.into(),
            last_height: h,
            last_hash: "0x".into(),
            updated_at: chrono::Utc::now(),
        };
        checkpoints.advance(decode_cp(7)).await.unwrap();
        // first run starts at the decode tip (history is balances-range's job)
        assert_eq!(balances_tick("mock", &MockMapper, &deps).await.unwrap(), 1);
        checkpoints.advance(decode_cp(9)).await.unwrap();
        assert_eq!(balances_tick("mock", &MockMapper, &deps).await.unwrap(), 2);
        let cp = checkpoints.get("mock", MODULE_BALANCES).await.unwrap().unwrap();
        assert_eq!(cp.last_height, 9);
        // caught up → no-op
        assert_eq!(balances_tick("mock", &MockMapper, &deps).await.unwrap(), 0);
    }
}
