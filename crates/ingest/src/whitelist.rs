//! Whitelist worker: walk canonical events → whitelisted-call facts
//! (ARCHITECTURE.md §9: own checkpoint, own schema, idempotent restarts).
//!
//! **This is the first module written directly against `crate::module`** rather
//! than as a sixth hand-copy of the loop. The whole file below the trait
//! definitions is a sink bridge and three delegations; the frontier rule,
//! decode-gap skip-and-advance, loud mapper halt, rows-before-checkpoint
//! ordering and follower backoff are inherited, not restated. Slice 8 exists
//! so that this file could be this short.
//!
//! Checkpoint module: `whitelist`.
//!
//! THE ONE SEMANTIC THIS WORKER MUST NOT BLUR, and it is the reason the fact
//! carries two fields where one would look sufficient: a
//! `WhitelistedCallDispatched` event is emitted whether the whitelisted call
//! SUCCEEDED or FAILED, because `clean_and_dispatch` discards the inner error
//! and returns `Ok` to the extrinsic. "Was dispatched" and "worked" are
//! different facts. See migration 0012's header for the pallet source.

use crate::module::{self, impl_module_error, EventSource, FactWriter, Mapping, ModuleRun};
use crate::{CheckpointError, CheckpointStore};
use async_trait::async_trait;
use canonical::CanonicalEvent;

pub const MODULE_WHITELIST: &str = "whitelist";

#[derive(Debug, thiserror::Error)]
pub enum WhitelistWorkerError {
    #[error("event source: {0}")]
    Source(String),
    #[error("whitelist sink: {0}")]
    Sink(String),
    #[error(
        "whitelist mapper failed for {chain}/{height} event {event_index} ({event}): {reason} — \
         the whitelist halts loudly rather than record a Fellowship authorization it cannot \
         read. NOTE an unpublished pallet-whitelist adds three deferred-dispatch variants \
         (DispatchDeferred / DeferredDispatchRemoved / DeferredDispatchExecuted) at indices \
         3/4/5; a runtime carrying them is the expected cause, and the fix is to extend the \
         mapper and re-run the range"
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

impl_module_error!(WhitelistWorkerError);

/// One whitelist fact from one event.
///
/// There is no `status` field beside `kind`: all three of the pallet's events
/// are status-bearing, so the kind IS the status. That is a real difference
/// from gov, treasury and bounties — each of those has info-only events that
/// must not move a projection, and each therefore needs the two separately.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WhitelistFact {
    /// 0x-hex 32 bytes. Every variant carries `call_hash` first, so unlike a
    /// treasury pot flow this fact can never be subject-less.
    pub call_hash: String,
    /// "whitelisted" | "removed" | "dispatched"
    pub kind: String,
    /// Only meaningful for "dispatched": Some(true) = the inner call returned
    /// Ok, Some(false) = it returned Err. None for the other two kinds — and
    /// None must never be read as "fine", because for a dispatched call it
    /// would mean the mapper could not read the result at all, which is a
    /// loud error rather than a NULL.
    pub dispatch_ok: Option<bool>,
    /// The inner call's `DispatchError`, kept whole, when `dispatch_ok` is
    /// Some(false).
    pub dispatch_error: Option<serde_json::Value>,
    /// Full event fields — schema-on-read.
    pub data: serde_json::Value,
}

/// Event → whitelist facts. Pure; errors are LOUD (an unreadable authorization
/// must halt, not silently drop). `mapper_version` is lineage.
pub trait WhitelistMapper: Send + Sync {
    fn facts(&self, event: &CanonicalEvent) -> Result<Vec<WhitelistFact>, String>;
    fn mapper_version(&self) -> u32;
}

/// Where whitelist facts land (node-side: gov.whitelist_events insert-ignore +
/// the ordering-guarded gov.whitelisted_calls projection, atomically per block).
///
/// CONTRACT: at most ONE fact per event index — the fact table is keyed
/// (chain, block, event), so a second fact from one event has nowhere to go.
/// Sinks REFUSE such a batch loudly rather than let insert-ignore swallow it
/// (the rule the votes sink established).
#[async_trait]
pub trait WhitelistSink: Send + Sync {
    async fn write(
        &self,
        chain_id: &str,
        height: u64,
        runtime_version: u32,
        mapper_version: u32,
        rows: &[(u32, WhitelistFact)],
    ) -> Result<(), String>;
}

pub struct WhitelistDeps<'a> {
    pub checkpoints: &'a dyn CheckpointStore,
    pub source: &'a dyn EventSource,
    pub sink: &'a dyn WhitelistSink,
}

/// Bridges the whitelist sink to the runtime's generic writer.
struct SinkBridge<'a>(&'a dyn WhitelistSink);

#[async_trait]
impl<'a> FactWriter<WhitelistFact> for SinkBridge<'a> {
    async fn write_facts(
        &self,
        chain_id: &str,
        height: u64,
        runtime_version: u32,
        mapper_version: u32,
        rows: &[(u32, WhitelistFact)],
    ) -> Result<(), String> {
        self.0
            .write(chain_id, height, runtime_version, mapper_version, rows)
            .await
    }
}

fn run<'a>(
    mapper: &'a dyn WhitelistMapper,
    deps: &'a WhitelistDeps<'a>,
) -> ModuleRun<'a, WhitelistFact> {
    ModuleRun {
        module: MODULE_WHITELIST,
        checkpoints: deps.checkpoints,
        source: deps.source,
        map: Mapping::PerEvent(Box::new(move |ev: &CanonicalEvent| mapper.facts(ev))),
        mapper_version: mapper.mapper_version(),
        sink: Box::new(SinkBridge(deps.sink)),
    }
}

/// Map heights `from..=to`. Behind the frontier: reprocess freely
/// (insert-ignore + guarded upsert converge), checkpoint untouched. Past it:
/// rows first, checkpoint last (crash = re-map, never skip).
pub async fn whitelist_range(
    chain_id: &str,
    mapper: &dyn WhitelistMapper,
    deps: &WhitelistDeps<'_>,
    from: u64,
    to: u64,
) -> Result<u64, WhitelistWorkerError> {
    module::run_range(chain_id, &run(mapper, deps), from, to).await
}

/// One follower step: chase the decode (`blocks`) checkpoint. First run starts
/// at the decode tip — history is `whitelist-range`'s job.
pub async fn whitelist_tick(
    chain_id: &str,
    mapper: &dyn WhitelistMapper,
    deps: &WhitelistDeps<'_>,
) -> Result<u64, WhitelistWorkerError> {
    module::run_tick(chain_id, &run(mapper, deps)).await
}

/// Follow forever, same backoff discipline as the other followers.
pub async fn whitelist_follow(
    chain_id: &str,
    mapper: &dyn WhitelistMapper,
    deps: &WhitelistDeps<'_>,
    poll: std::time::Duration,
) {
    module::run_follow::<WhitelistFact, WhitelistWorkerError>(chain_id, &run(mapper, deps), poll)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::MODULE_DECODE;
    use crate::module::BlockEvents;
    use crate::Checkpoint;
    use crate::MemoryCheckpointStore;
    use std::collections::HashMap;
    use std::sync::Mutex;

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

    /// Mock mapper: "mock.White" → one whitelisted fact carrying the hash.
    struct MockMapper;
    impl WhitelistMapper for MockMapper {
        fn facts(&self, event: &CanonicalEvent) -> Result<Vec<WhitelistFact>, String> {
            if event.name == "mock.Bad" {
                return Err("unmappable".into());
            }
            if event.name != "mock.White" {
                return Ok(vec![]);
            }
            Ok(vec![WhitelistFact {
                call_hash: event.data["hash"].as_str().ok_or("no hash")?.to_string(),
                kind: "whitelisted".into(),
                dispatch_ok: None,
                dispatch_error: None,
                data: event.data.clone(),
            }])
        }
        fn mapper_version(&self) -> u32 {
            1
        }
    }

    #[derive(Default)]
    struct MemSink(Mutex<Vec<(u64, u32, WhitelistFact)>>);
    #[async_trait]
    impl WhitelistSink for MemSink {
        async fn write(
            &self,
            _chain: &str,
            height: u64,
            _rv: u32,
            _mv: u32,
            rows: &[(u32, WhitelistFact)],
        ) -> Result<(), String> {
            let mut g = self.0.lock().unwrap();
            for (idx, f) in rows {
                g.push((height, *idx, f.clone()));
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
        src.insert(
            1,
            vec![ev(0, "mock.White", serde_json::json!({"hash": "0xaa"}))],
        );
        // height 2 is a decode gap
        src.insert(
            3,
            vec![
                ev(0, "mock.Other", serde_json::json!({})),
                ev(1, "mock.White", serde_json::json!({"hash": "0xbb"})),
            ],
        );
        let checkpoints = MemoryCheckpointStore::new();
        let sink = MemSink::default();
        let deps = WhitelistDeps {
            checkpoints: &checkpoints,
            source: &MemSource(src),
            sink: &sink,
        };
        let n = whitelist_range("mock", &MockMapper, &deps, 1, 3)
            .await
            .unwrap();
        assert_eq!(n, 2, "two decoded heights, one gap skipped");
        let rows = sink.0.lock().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!((rows[0].0, rows[0].2.call_hash.as_str()), (1, "0xaa"));
        assert_eq!(
            (rows[1].0, rows[1].1, rows[1].2.call_hash.as_str()),
            (3, 1, "0xbb")
        );
        drop(rows);
        let cp = checkpoints
            .get("mock", MODULE_WHITELIST)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cp.last_height, 3);
    }

    #[tokio::test]
    async fn rerun_behind_frontier_rewrites_without_moving_checkpoint() {
        let mut src = HashMap::new();
        src.insert(
            1,
            vec![ev(0, "mock.White", serde_json::json!({"hash": "0xaa"}))],
        );
        src.insert(2, vec![]);
        let checkpoints = MemoryCheckpointStore::new();
        let sink = MemSink::default();
        let deps = WhitelistDeps {
            checkpoints: &checkpoints,
            source: &MemSource(src),
            sink: &sink,
        };
        whitelist_range("mock", &MockMapper, &deps, 1, 2)
            .await
            .unwrap();
        let n = whitelist_range("mock", &MockMapper, &deps, 1, 1)
            .await
            .unwrap();
        assert_eq!(
            n, 1,
            "behind-frontier reprocess is allowed (sink converges)"
        );
        let cp = checkpoints
            .get("mock", MODULE_WHITELIST)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cp.last_height, 2, "frontier untouched by reprocess");
    }

    #[tokio::test]
    async fn mapper_errors_halt_loudly_and_advance_nothing() {
        let mut src = HashMap::new();
        src.insert(1, vec![ev(0, "mock.Bad", serde_json::json!({}))]);
        let checkpoints = MemoryCheckpointStore::new();
        let sink = MemSink::default();
        let deps = WhitelistDeps {
            checkpoints: &checkpoints,
            source: &MemSource(src),
            sink: &sink,
        };
        let err = whitelist_range("mock", &MockMapper, &deps, 1, 1)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            WhitelistWorkerError::Mapper { height: 1, .. }
        ));
        assert!(sink.0.lock().unwrap().is_empty());
        assert!(checkpoints
            .get("mock", MODULE_WHITELIST)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn tick_chases_the_decode_checkpoint() {
        let mut src = HashMap::new();
        for h in 5..=9 {
            src.insert(
                h,
                vec![ev(0, "mock.White", serde_json::json!({"hash": "0xaa"}))],
            );
        }
        let checkpoints = MemoryCheckpointStore::new();
        let sink = MemSink::default();
        let deps = WhitelistDeps {
            checkpoints: &checkpoints,
            source: &MemSource(src),
            sink: &sink,
        };
        assert_eq!(whitelist_tick("mock", &MockMapper, &deps).await.unwrap(), 0);

        let decode_cp = |h: u64| Checkpoint {
            chain_id: "mock".into(),
            module: MODULE_DECODE.into(),
            last_height: h,
            last_hash: "0x".into(),
            updated_at: chrono::Utc::now(),
        };
        checkpoints.advance(decode_cp(7)).await.unwrap();
        assert_eq!(whitelist_tick("mock", &MockMapper, &deps).await.unwrap(), 1);
        checkpoints.advance(decode_cp(9)).await.unwrap();
        assert_eq!(whitelist_tick("mock", &MockMapper, &deps).await.unwrap(), 2);
        let cp = checkpoints
            .get("mock", MODULE_WHITELIST)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cp.last_height, 9);
        assert_eq!(whitelist_tick("mock", &MockMapper, &deps).await.unwrap(), 0);
    }
}
