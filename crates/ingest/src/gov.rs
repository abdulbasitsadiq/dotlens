//! Governance worker: walk canonical events → referendum timeline entries
//! (ARCHITECTURE.md §9: an independent module consuming from its own
//! checkpoint, writing only to its own schema, idempotent restarts).
//!
//! The worker is family-agnostic: WHAT an event means for governance lives
//! behind `GovMapper` in adapter crates (Invariant 4). It chases the decode
//! worker's `blocks` checkpoint exactly like the balances worker, and shares
//! its `EventSource` contract ("decoded canonical events per height") —
//! deliberately, so one Pg backend serves both.
//!
//! Checkpoint module: `gov`. The checkpoint is a frontier — `gov-range` may
//! reprocess anything behind it (event inserts are conflict-ignored, the
//! projection upsert is ordering-guarded, so replay in any order converges)
//! and never regresses it; heights the canonical store hasn't decoded yet are
//! skipped and advanced past (decode gap-fill + a later `gov-range` re-run
//! covers them).

use crate::balances::{BlockEvents, EventSource};
use crate::decode::MODULE_DECODE;
use crate::{Checkpoint, CheckpointError, CheckpointStore};
use async_trait::async_trait;
use canonical::CanonicalEvent;

pub const MODULE_GOV: &str = "gov";

#[derive(Debug, thiserror::Error)]
pub enum GovWorkerError {
    #[error("event source: {0}")]
    Source(String),
    #[error("timeline sink: {0}")]
    Sink(String),
    #[error(
        "gov mapper failed for {chain}/{height} event {event_index} ({event}): {reason} — \
         governance halts loudly rather than record gaps in referendum history"
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

/// One referendum-timeline fact from one event. The mapper (adapter) is the
/// only place that knows pallet vocabulary; the sink applies this without
/// re-interpreting event names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefTimelineEntry {
    /// Referenda instance, snake_case: "referenda" | "fellowship_referenda".
    pub class: String,
    pub referendum_id: u64,
    /// snake_case timeline kind, e.g. "submitted", "deciding", "approved".
    pub kind: String,
    /// The status this event moves the referendum to, if status-bearing.
    /// None = informational only (deposit refunds, metadata) — the projection
    /// must NOT change status for these.
    pub status: Option<String>,
    /// Track id, when the event carries it (Submitted / DecisionStarted).
    pub track_id: Option<u32>,
    /// The Bounded<Call> proposal JSON, when carried (Submitted / DecisionStarted).
    pub proposal: Option<serde_json::Value>,
    /// 0x-hex proposal hash when knowable (Lookup / Legacy forms).
    pub proposal_hash: Option<String>,
    /// Preimage length for Lookup proposals.
    pub proposal_len: Option<u64>,
    /// Full event fields — schema-on-read for the timeline row.
    pub data: serde_json::Value,
}

/// Event → timeline entries. Pure; errors are LOUD (a malformed referenda
/// event must halt, not silently drop history). `mapper_version` is lineage:
/// bump on any rule change, rebuild rows from canonical events.
pub trait GovMapper: Send + Sync {
    fn timeline(&self, event: &CanonicalEvent) -> Result<Vec<RefTimelineEntry>, String>;
    fn mapper_version(&self) -> u32;
}

/// Where timeline entries land (node-side: gov.referendum_events insert-ignore
/// + gov.referenda ordering-guarded upsert, atomically per block).
#[async_trait]
pub trait TimelineSink: Send + Sync {
    async fn write(
        &self,
        chain_id: &str,
        height: u64,
        runtime_version: u32,
        mapper_version: u32,
        rows: &[(u32, RefTimelineEntry)], // (event_index, entry)
    ) -> Result<(), String>;
}

pub struct GovDeps<'a> {
    pub checkpoints: &'a dyn CheckpointStore,
    pub source: &'a dyn EventSource,
    pub sink: &'a dyn TimelineSink,
}

/// Map heights `from..=to`. Heights behind the frontier are reprocessed freely
/// (insert-ignore + guarded upsert make it a no-op) without touching the
/// checkpoint; past it, rows first, checkpoint last (crash = re-map, never skip).
pub async fn gov_range(
    chain_id: &str,
    mapper: &dyn GovMapper,
    deps: &GovDeps<'_>,
    from: u64,
    to: u64,
) -> Result<u64, GovWorkerError> {
    let frontier = deps
        .checkpoints
        .get(chain_id, MODULE_GOV)
        .await?
        .map(|cp| cp.last_height);
    let mut processed = 0u64;

    for height in from..=to {
        let behind_frontier = frontier.is_some_and(|f| height <= f);
        let block: Option<BlockEvents> = deps
            .source
            .decoded_events(chain_id, height)
            .await
            .map_err(GovWorkerError::Source)?;
        let Some(block) = block else {
            // decode gap: skip. Ahead of the frontier we still advance so the
            // follower never wedges on a hole; the height gets its timeline
            // entries when decode gap-fill + gov-range revisit it.
            tracing::debug!(chain = %chain_id, height, "no canonical block — skipped");
            if !behind_frontier {
                advance(deps, chain_id, height).await?;
            }
            continue;
        };

        let mut rows: Vec<(u32, RefTimelineEntry)> = Vec::new();
        for ev in &block.events {
            let entries = mapper
                .timeline(ev)
                .map_err(|reason| GovWorkerError::Mapper {
                    chain: chain_id.to_string(),
                    height,
                    event_index: ev.index,
                    event: ev.name.clone(),
                    reason,
                })?;
            for e in entries {
                rows.push((ev.index, e));
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
                .map_err(GovWorkerError::Sink)?;
        }
        if !behind_frontier {
            advance(deps, chain_id, height).await?;
        }
        processed += 1;
    }
    Ok(processed)
}

async fn advance(
    deps: &GovDeps<'_>,
    chain_id: &str,
    height: u64,
) -> Result<(), CheckpointError> {
    deps.checkpoints
        .advance(Checkpoint {
            chain_id: chain_id.to_string(),
            module: MODULE_GOV.to_string(),
            last_height: height,
            last_hash: "-".to_string(),
            updated_at: chrono::Utc::now(),
        })
        .await
}

/// One follower step: chase the decode (`blocks`) checkpoint. First run starts
/// at the decode tip — history is `gov-range`'s job (same first-run semantics
/// as balances_tick vs decode).
pub async fn gov_tick(
    chain_id: &str,
    mapper: &dyn GovMapper,
    deps: &GovDeps<'_>,
) -> Result<u64, GovWorkerError> {
    let Some(decode_cp) = deps.checkpoints.get(chain_id, MODULE_DECODE).await? else {
        return Ok(0); // nothing decoded yet
    };
    let target = decode_cp.last_height;
    let from = match deps.checkpoints.get(chain_id, MODULE_GOV).await? {
        Some(cp) if cp.last_height >= target => return Ok(0),
        Some(cp) => cp.last_height + 1,
        None => target, // first run: start at the decode tip
    };
    gov_range(chain_id, mapper, deps, from, target).await
}

/// Follow forever, same backoff discipline as the other followers.
pub async fn gov_follow(
    chain_id: &str,
    mapper: &dyn GovMapper,
    deps: &GovDeps<'_>,
    poll: std::time::Duration,
) {
    let mut consecutive_failures = 0u32;
    loop {
        match gov_tick(chain_id, mapper, deps).await {
            Ok(n) => {
                consecutive_failures = 0;
                if n > 0 {
                    tracing::debug!(chain = %chain_id, blocks = n, "gov tick");
                }
            }
            Err(e) => {
                consecutive_failures += 1;
                tracing::warn!(chain = %chain_id, error = %e, consecutive_failures, "gov tick failed");
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

    /// Mock mapper: {"index": n} on "mock.Ref" → one submitted entry.
    struct MockMapper;
    impl GovMapper for MockMapper {
        fn timeline(&self, event: &CanonicalEvent) -> Result<Vec<RefTimelineEntry>, String> {
            if event.name == "mock.Bad" {
                return Err("unmappable".into());
            }
            if event.name != "mock.Ref" {
                return Ok(vec![]);
            }
            let id = event.data["index"].as_u64().ok_or("no index")?;
            Ok(vec![RefTimelineEntry {
                class: "referenda".into(),
                referendum_id: id,
                kind: "submitted".into(),
                status: Some("submitted".into()),
                track_id: Some(0),
                proposal: None,
                proposal_hash: None,
                proposal_len: None,
                data: event.data.clone(),
            }])
        }
        fn mapper_version(&self) -> u32 {
            1
        }
    }

    #[derive(Default)]
    struct MemSink(Mutex<Vec<(u64, u32, RefTimelineEntry)>>);
    #[async_trait]
    impl TimelineSink for MemSink {
        async fn write(
            &self,
            _chain: &str,
            height: u64,
            _rv: u32,
            _mv: u32,
            rows: &[(u32, RefTimelineEntry)],
        ) -> Result<(), String> {
            let mut g = self.0.lock().unwrap();
            for (idx, e) in rows {
                g.push((height, *idx, e.clone()));
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
        src.insert(1, vec![ev(0, "mock.Ref", serde_json::json!({"index": 42}))]);
        // height 2 is a decode gap
        src.insert(3, vec![
            ev(0, "mock.Other", serde_json::json!({})),
            ev(1, "mock.Ref", serde_json::json!({"index": 43})),
        ]);
        let checkpoints = MemoryCheckpointStore::new();
        let sink = MemSink::default();
        let deps = GovDeps {
            checkpoints: &checkpoints,
            source: &MemSource(src),
            sink: &sink,
        };
        let n = gov_range("mock", &MockMapper, &deps, 1, 3).await.unwrap();
        assert_eq!(n, 2, "two decoded heights, one gap skipped");
        let rows = sink.0.lock().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!((rows[0].0, rows[0].2.referendum_id), (1, 42));
        assert_eq!((rows[1].0, rows[1].1, rows[1].2.referendum_id), (3, 1, 43));
        drop(rows);
        // checkpoint advanced through the gap
        let cp = checkpoints.get("mock", MODULE_GOV).await.unwrap().unwrap();
        assert_eq!(cp.last_height, 3);
    }

    #[tokio::test]
    async fn rerun_behind_frontier_rewrites_without_moving_checkpoint() {
        let mut src = HashMap::new();
        src.insert(1, vec![ev(0, "mock.Ref", serde_json::json!({"index": 1}))]);
        src.insert(2, vec![]);
        let checkpoints = MemoryCheckpointStore::new();
        let sink = MemSink::default();
        let deps = GovDeps {
            checkpoints: &checkpoints,
            source: &MemSource(src),
            sink: &sink,
        };
        gov_range("mock", &MockMapper, &deps, 1, 2).await.unwrap();
        let n = gov_range("mock", &MockMapper, &deps, 1, 1).await.unwrap();
        assert_eq!(n, 1, "behind-frontier reprocess is allowed (sink converges)");
        let cp = checkpoints.get("mock", MODULE_GOV).await.unwrap().unwrap();
        assert_eq!(cp.last_height, 2, "frontier untouched by reprocess");
    }

    #[tokio::test]
    async fn mapper_errors_halt_loudly_and_advance_nothing() {
        let mut src = HashMap::new();
        src.insert(1, vec![ev(0, "mock.Bad", serde_json::json!({}))]);
        let checkpoints = MemoryCheckpointStore::new();
        let sink = MemSink::default();
        let deps = GovDeps {
            checkpoints: &checkpoints,
            source: &MemSource(src),
            sink: &sink,
        };
        let err = gov_range("mock", &MockMapper, &deps, 1, 1).await.unwrap_err();
        assert!(matches!(err, GovWorkerError::Mapper { height: 1, .. }));
        assert!(sink.0.lock().unwrap().is_empty());
        assert!(checkpoints.get("mock", MODULE_GOV).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn tick_chases_the_decode_checkpoint() {
        let mut src = HashMap::new();
        for h in 5..=9 {
            src.insert(h, vec![ev(0, "mock.Ref", serde_json::json!({"index": h}))]);
        }
        let checkpoints = MemoryCheckpointStore::new();
        let sink = MemSink::default();
        let deps = GovDeps {
            checkpoints: &checkpoints,
            source: &MemSource(src),
            sink: &sink,
        };
        // nothing decoded yet → no-op
        assert_eq!(gov_tick("mock", &MockMapper, &deps).await.unwrap(), 0);

        let decode_cp = |h: u64| Checkpoint {
            chain_id: "mock".into(),
            module: MODULE_DECODE.into(),
            last_height: h,
            last_hash: "0x".into(),
            updated_at: chrono::Utc::now(),
        };
        checkpoints.advance(decode_cp(7)).await.unwrap();
        // first run starts at the decode tip (history is gov-range's job)
        assert_eq!(gov_tick("mock", &MockMapper, &deps).await.unwrap(), 1);
        checkpoints.advance(decode_cp(9)).await.unwrap();
        assert_eq!(gov_tick("mock", &MockMapper, &deps).await.unwrap(), 2);
        let cp = checkpoints.get("mock", MODULE_GOV).await.unwrap().unwrap();
        assert_eq!(cp.last_height, 9);
        // caught up → no-op
        assert_eq!(gov_tick("mock", &MockMapper, &deps).await.unwrap(), 0);
    }
}
