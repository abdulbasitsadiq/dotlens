//! Bounty worker: walk canonical events → bounty facts (ARCHITECTURE.md §9:
//! own checkpoint, own schema, idempotent restarts).
//!
//! Structurally identical to the treasury, gov and votes workers — same
//! `EventSource` contract, same frontier discipline, same loud-mapper-halt rule
//! — because WHAT an event means for a bounty lives behind `BountyMapper` in
//! adapter crates (Invariant 4). Three pallets map into this one worker for the
//! same reason they map into one table: they are three generations of one idea.
//!
//! WHY THIS WORKER EXISTS AT ALL, from migration 0009: bounty funding leaves
//! the treasury through the `SpendFunds` HOOK, emitting `bounties.
//! BountyBecameActive` rather than `treasury.Awarded` or any pot event. Every
//! table dotlens had before this slice was blind to it.
//!
//! Checkpoint module: `bounties`.

use crate::module::{self, impl_module_error, EventSource, FactWriter, Mapping, ModuleRun};
use crate::{CheckpointError, CheckpointStore};
use async_trait::async_trait;
use canonical::CanonicalEvent;

pub const MODULE_BOUNTIES: &str = "bounties";

#[derive(Debug, thiserror::Error)]
pub enum BountyWorkerError {
    #[error("event source: {0}")]
    Source(String),
    #[error("bounty sink: {0}")]
    Sink(String),
    #[error(
        "bounty mapper failed for {chain}/{height} event {event_index} ({event}): {reason} — \
         bounties halt loudly rather than record money they cannot explain. NOTE \
         pallet-multi-asset-bounties is pre-1.0, so a runtime upgrade adding a variant is \
         the expected cause; extend the mapper and re-run the range"
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

impl_module_error!(BountyWorkerError);

/// One bounty fact from one event.
///
/// `child_id` is `Option<u64>` here and `-1` in the table: None IS the parent
/// bounty, and the projection's identity is (instance, bounty, child), which a
/// NULL cannot be part of. The sink translates; nothing above it ever sees the
/// sentinel (migration 0011 states the rule, the API renders it back to null).
///
/// `status` is optional because not every bounty event moves a bounty's state:
/// a curator deposit poke, an expiry extension and a value raise all say
/// something true about a bounty while leaving its status exactly where it was.
/// Those facts still name a subject and still carry columns worth keeping — a
/// raise carries the bounty's VALUE — so unlike a treasury pot flow they belong
/// in the projection, just not in its status.
///
/// `figure_kind` is 0009's doctrine, and here it separates two things a page
/// would otherwise add together: a payout ('flow', money that left the bounty)
/// and `BountyValueIncreased`'s new_value ('snapshot', how big the bounty now
/// is). Summing snapshots as flows would report every raise as a payment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BountyFact {
    /// "bounties" | "child_bounties" | "multi_asset_bounties".
    pub instance: String,
    pub bounty_id: u64,
    /// None = the parent bounty itself.
    pub child_id: Option<u64>,
    /// proposed|approved|became_active|awarded|claimed|canceled|extended|
    /// curator_proposed|curator_accepted|curator_unassigned|deposit_poked|
    /// created|child_created|payout_processed|funding_processed|
    /// refund_processed|paid|payment_failed|value_increased|rejected.
    pub kind: String,
    /// The status this event moves the bounty to. None = information only.
    pub status: Option<String>,
    /// The figure this event carries, meaning given by `figure_kind`.
    pub amount: Option<u128>,
    /// "flow" (money moved) | "snapshot" (the bounty's size, reported).
    pub figure_kind: Option<String>,
    /// `BountyRejected.bond` — the PROPOSER'S slashed bond, never spending.
    pub bond: Option<u128>,
    pub curator: Option<Vec<u8>>,
    /// 32-byte beneficiary where one is actually named; never invented from a
    /// location that names none.
    pub beneficiary: Option<Vec<u8>>,
    /// The beneficiary exactly as the event carried it (the modern pallet's is
    /// a location, not an account).
    pub beneficiary_location: Option<serde_json::Value>,
    /// MULTI-ASSET ONLY: the raw `VersionedLocatableAsset` a payout names.
    pub asset_kind: Option<serde_json::Value>,
    /// The same asset kind normalized into `{"chain": …, "asset": …}` — the
    /// join handle to `core.assets.location_key`, identical in shape and
    /// producer to a treasury spend's. A bounty payout and a treasury spend are
    /// denominated the same way and must read the same way.
    pub asset_location: Option<serde_json::Value>,
    /// The resolved key where no metadata is needed; None means "not resolvable
    /// without the chain's pallet indices", never "not an asset".
    pub asset_key: Option<String>,
    pub payment_id: Option<String>,
    pub data: serde_json::Value,
}

/// Event → bounty facts. Pure; errors are LOUD (an unknown bounty event must
/// halt, not silently drop money). `mapper_version` is lineage.
pub trait BountyMapper: Send + Sync {
    fn facts(&self, event: &CanonicalEvent) -> Result<Vec<BountyFact>, String>;
    fn mapper_version(&self) -> u32;
}

/// Where bounty facts land (node-side: `treasury.bounty_events` insert-ignore
/// plus the ordering-guarded `treasury.bounties` projection, atomically per
/// block).
///
/// CONTRACT: at most ONE fact per event index — the fact table is keyed
/// (chain, block, event), so a second fact from one event has nowhere to go.
/// Sinks must REFUSE such a batch loudly rather than let insert-ignore swallow
/// it (the rule the votes sink established).
///
/// SECOND CONTRACT, unique to this sink: `paid_out` ACCUMULATES, so unlike
/// every other projection in the project this one is not idempotent under a
/// replayed event. A sink must add a payout only when the FACT ROW was really
/// inserted — see `PgBountySink`, which learns that from `insert … returning`.
#[async_trait]
pub trait BountySink: Send + Sync {
    async fn write(
        &self,
        chain_id: &str,
        height: u64,
        runtime_version: u32,
        mapper_version: u32,
        rows: &[(u32, BountyFact)],
    ) -> Result<(), String>;
}

pub struct BountyDeps<'a> {
    pub checkpoints: &'a dyn CheckpointStore,
    pub source: &'a dyn EventSource,
    pub sink: &'a dyn BountySink,
}

/// Bridges the bounty sink to the runtime's generic writer.
struct SinkBridge<'a>(&'a dyn BountySink);

#[async_trait]
impl<'a> FactWriter<BountyFact> for SinkBridge<'a> {
    async fn write_facts(
        &self,
        chain_id: &str,
        height: u64,
        runtime_version: u32,
        mapper_version: u32,
        rows: &[(u32, BountyFact)],
    ) -> Result<(), String> {
        self.0
            .write(chain_id, height, runtime_version, mapper_version, rows)
            .await
    }
}

fn run<'a>(mapper: &'a dyn BountyMapper, deps: &'a BountyDeps<'a>) -> ModuleRun<'a, BountyFact> {
    ModuleRun {
        module: MODULE_BOUNTIES,
        checkpoints: deps.checkpoints,
        source: deps.source,
        map: Mapping::PerEvent(Box::new(move |ev: &CanonicalEvent| mapper.facts(ev))),
        mapper_version: mapper.mapper_version(),
        sink: Box::new(SinkBridge(deps.sink)),
    }
}

/// Map heights `from..=to`. Behind the frontier: reprocess freely (insert-ignore
/// + guarded upsert converge, and the accumulating `paid_out` is guarded by the
/// fact insert), checkpoint untouched. Past it: rows first, checkpoint last
/// (crash = re-map, never skip).
pub async fn bounties_range(
    chain_id: &str,
    mapper: &dyn BountyMapper,
    deps: &BountyDeps<'_>,
    from: u64,
    to: u64,
) -> Result<u64, BountyWorkerError> {
    module::run_range(chain_id, &run(mapper, deps), from, to).await
}

/// One follower step: chase the decode (`blocks`) checkpoint. First run starts
/// at the decode tip — history is `bounties-range`'s job.
pub async fn bounties_tick(
    chain_id: &str,
    mapper: &dyn BountyMapper,
    deps: &BountyDeps<'_>,
) -> Result<u64, BountyWorkerError> {
    module::run_tick(chain_id, &run(mapper, deps)).await
}

/// Follow forever, same backoff discipline as the other followers.
pub async fn bounties_follow(
    chain_id: &str,
    mapper: &dyn BountyMapper,
    deps: &BountyDeps<'_>,
    poll: std::time::Duration,
) {
    module::run_follow::<BountyFact, BountyWorkerError>(chain_id, &run(mapper, deps), poll).await
}

#[cfg(test)]
mod tests {
    use super::*;
    // Both moved out of the file header when `advance` did; the tests below are
    // otherwise untouched, which is the point of this refactor.
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

    fn fact(id: u64, child: Option<u64>, kind: &str, status: Option<&str>) -> BountyFact {
        BountyFact {
            instance: "bounties".into(),
            bounty_id: id,
            child_id: child,
            kind: kind.into(),
            status: status.map(str::to_string),
            amount: Some(100),
            figure_kind: Some("flow".into()),
            bond: None,
            curator: None,
            beneficiary: None,
            beneficiary_location: None,
            asset_kind: None,
            asset_location: None,
            asset_key: None,
            payment_id: None,
            data: serde_json::json!({}),
        }
    }

    struct MockMapper;
    impl BountyMapper for MockMapper {
        fn facts(&self, event: &CanonicalEvent) -> Result<Vec<BountyFact>, String> {
            match event.name.as_str() {
                "mock.Bad" => Err("unmappable".into()),
                "mock.Bounty" => {
                    let id = event.data["index"].as_u64().ok_or("no index")?;
                    Ok(vec![fact(id, None, "claimed", Some("claimed"))])
                }
                // an info-only event: names a bounty, moves no status
                "mock.Raise" => Ok(vec![BountyFact {
                    figure_kind: Some("snapshot".into()),
                    ..fact(7, Some(3), "value_increased", None)
                }]),
                _ => Ok(vec![]),
            }
        }
        fn mapper_version(&self) -> u32 {
            1
        }
    }

    #[derive(Default)]
    struct MemSink(Mutex<Vec<(u64, u32, BountyFact)>>);
    #[async_trait]
    impl BountySink for MemSink {
        async fn write(
            &self,
            _chain: &str,
            height: u64,
            _rv: u32,
            _mv: u32,
            rows: &[(u32, BountyFact)],
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
            transaction_index: Some(0),
            name: name.into(),
            data,
        }
    }

    #[tokio::test]
    async fn range_maps_bounties_and_info_events_skips_gaps_and_advances() {
        let mut src = HashMap::new();
        src.insert(
            1,
            vec![ev(0, "mock.Bounty", serde_json::json!({"index": 42}))],
        );
        // height 2 is a decode gap
        src.insert(
            3,
            vec![
                ev(0, "mock.Other", serde_json::json!({})),
                ev(1, "mock.Raise", serde_json::json!({})),
            ],
        );
        let checkpoints = MemoryCheckpointStore::new();
        let sink = MemSink::default();
        let deps = BountyDeps {
            checkpoints: &checkpoints,
            source: &MemSource(src),
            sink: &sink,
        };
        let n = bounties_range("mock", &MockMapper, &deps, 1, 3)
            .await
            .unwrap();
        assert_eq!(n, 2, "two decoded heights, one gap skipped");
        let rows = sink.0.lock().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!((rows[0].2.bounty_id, rows[0].2.child_id), (42, None));
        // the info-only fact still names its subject — it is not a pot flow
        assert_eq!((rows[1].2.bounty_id, rows[1].2.child_id), (7, Some(3)));
        assert!(rows[1].2.status.is_none());
        drop(rows);
        let cp = checkpoints
            .get("mock", MODULE_BOUNTIES)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cp.last_height, 3, "checkpoint advanced through the gap");
    }

    #[tokio::test]
    async fn rerun_behind_frontier_rewrites_without_moving_checkpoint() {
        let mut src = HashMap::new();
        src.insert(
            1,
            vec![ev(0, "mock.Bounty", serde_json::json!({"index": 1}))],
        );
        src.insert(2, vec![]);
        let checkpoints = MemoryCheckpointStore::new();
        let sink = MemSink::default();
        let deps = BountyDeps {
            checkpoints: &checkpoints,
            source: &MemSource(src),
            sink: &sink,
        };
        bounties_range("mock", &MockMapper, &deps, 1, 2)
            .await
            .unwrap();
        let n = bounties_range("mock", &MockMapper, &deps, 1, 1)
            .await
            .unwrap();
        assert_eq!(
            n, 1,
            "behind-frontier reprocess is allowed (sink converges)"
        );
        let cp = checkpoints
            .get("mock", MODULE_BOUNTIES)
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
        let deps = BountyDeps {
            checkpoints: &checkpoints,
            source: &MemSource(src),
            sink: &sink,
        };
        let err = bounties_range("mock", &MockMapper, &deps, 1, 1)
            .await
            .unwrap_err();
        assert!(matches!(err, BountyWorkerError::Mapper { height: 1, .. }));
        assert!(sink.0.lock().unwrap().is_empty());
        assert!(checkpoints
            .get("mock", MODULE_BOUNTIES)
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
                vec![ev(0, "mock.Bounty", serde_json::json!({"index": h}))],
            );
        }
        let checkpoints = MemoryCheckpointStore::new();
        let sink = MemSink::default();
        let deps = BountyDeps {
            checkpoints: &checkpoints,
            source: &MemSource(src),
            sink: &sink,
        };
        assert_eq!(bounties_tick("mock", &MockMapper, &deps).await.unwrap(), 0);

        let decode_cp = |h: u64| Checkpoint {
            chain_id: "mock".into(),
            module: MODULE_DECODE.into(),
            last_height: h,
            last_hash: "0x".into(),
            updated_at: chrono::Utc::now(),
        };
        checkpoints.advance(decode_cp(7)).await.unwrap();
        assert_eq!(bounties_tick("mock", &MockMapper, &deps).await.unwrap(), 1);
        checkpoints.advance(decode_cp(9)).await.unwrap();
        assert_eq!(bounties_tick("mock", &MockMapper, &deps).await.unwrap(), 2);
        let cp = checkpoints
            .get("mock", MODULE_BOUNTIES)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cp.last_height, 9);
        assert_eq!(bounties_tick("mock", &MockMapper, &deps).await.unwrap(), 0);
    }
}
