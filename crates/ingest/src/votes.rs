//! Voting worker: walk canonical events → vote + delegation facts
//! (ARCHITECTURE.md §9: an independent module consuming from its own
//! checkpoint, writing only to its own schema, idempotent restarts).
//!
//! Structurally identical to the gov (timeline) worker — same `EventSource`
//! contract, same frontier discipline, same loud-mapper-halt rule — because
//! WHAT an event means for voting lives behind `VoteMapper` in adapter crates
//! (Invariant 4). One Pg event source serves balances, gov and votes.
//!
//! Checkpoint module: `votes`. The checkpoint is a frontier — `votes-range`
//! may reprocess anything behind it (fact inserts are conflict-ignored, the
//! projections are ordering-guarded, so replay in any order converges) and
//! never regresses it; heights the canonical store hasn't decoded yet are
//! skipped and advanced past.

use crate::module::{self, impl_module_error, EventSource, FactWriter, ModuleRun};
use crate::{CheckpointError, CheckpointStore};
use async_trait::async_trait;
use canonical::CanonicalEvent;

pub const MODULE_VOTES: &str = "votes";

#[derive(Debug, thiserror::Error)]
pub enum VotesWorkerError {
    #[error("event source: {0}")]
    Source(String),
    #[error("vote sink: {0}")]
    Sink(String),
    #[error(
        "vote mapper failed for {chain}/{height} event {event_index} ({event}): {reason} — \
         voting halts loudly rather than record gaps in who decided what"
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

impl_module_error!(VotesWorkerError);

/// One vote fact from one event.
///
/// `referendum_id` is OPTIONAL on purpose: pre-v43 `Voted`/`VoteRemoved`
/// events carry no poll index, so the vote is real but its subject is not
/// derivable from the event alone. Such rows are recorded with
/// `attribution = "unattributed"` and stay out of the projection — honest
/// coverage, never a silent drop and never a guess.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoteRecord {
    /// Referenda instance: "referenda" | "fellowship_referenda".
    pub class: String,
    pub referendum_id: Option<u64>,
    pub voter: Vec<u8>,
    /// "voted" | "vote_removed".
    pub kind: String,
    /// "standard" | "split" | "split_abstain" | "ranked".
    pub vote_type: String,
    /// Capital behind each side (plancks). None for rank-weighted votes.
    pub aye_balance: Option<u128>,
    pub nay_balance: Option<u128>,
    pub abstain_balance: Option<u128>,
    /// 0..=6 for standard votes; None otherwise.
    pub conviction: Option<u8>,
    pub conviction_label: Option<String>,
    /// Post-conviction weights, computed exactly as the pallet tallies them.
    pub aye_votes: u128,
    pub nay_votes: u128,
    pub support: u128,
    /// "event" | "unattributed".
    pub attribution: String,
    pub data: serde_json::Value,
}

/// One delegation edge from one event. `track_id` is optional for the same
/// reason `referendum_id` is: the pre-v43 `Delegated`/`Undelegated` shape
/// carries no class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DelegationRecord {
    pub class: String,
    pub track_id: Option<u32>,
    pub delegator: Vec<u8>,
    /// None for "undelegated".
    pub target: Option<Vec<u8>>,
    /// "delegated" | "undelegated".
    pub kind: String,
    pub attribution: String,
    pub data: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VoteFact {
    Vote(VoteRecord),
    Delegation(DelegationRecord),
}

/// Event → vote facts. Pure; errors are LOUD (an unknown voting event must
/// halt, not silently drop a decision). `mapper_version` is lineage.
pub trait VoteMapper: Send + Sync {
    fn facts(&self, event: &CanonicalEvent) -> Result<Vec<VoteFact>, String>;
    fn mapper_version(&self) -> u32;
}

/// Where vote facts land (node-side: gov.votes / gov.delegation_events
/// insert-ignore + the two ordering-guarded projections, atomically per block).
///
/// CONTRACT: at most ONE fact of each kind per event index. Both fact tables
/// are keyed (chain, block, event) — precisely so an unattributable event can
/// still be stored — so a second vote (or second delegation) from the same
/// event has nowhere to go. Sinks must REFUSE such a batch loudly rather than
/// let insert-ignore swallow it; a future event that carries two decisions
/// needs a schema change, not a silent drop.
#[async_trait]
pub trait VoteSink: Send + Sync {
    async fn write(
        &self,
        chain_id: &str,
        height: u64,
        runtime_version: u32,
        mapper_version: u32,
        rows: &[(u32, VoteFact)], // (event_index, fact)
    ) -> Result<(), String>;
}

pub struct VotesDeps<'a> {
    pub checkpoints: &'a dyn CheckpointStore,
    pub source: &'a dyn EventSource,
    pub sink: &'a dyn VoteSink,
}

/// Bridges the vote sink to the runtime's generic writer. The at-most-one-fact-
/// per-kind-per-event CONTRACT above is the SINK's to enforce, and still is —
/// the shared runtime hands over whatever the mapper produced, exactly as the
/// hand-written loop did.
struct SinkBridge<'a>(&'a dyn VoteSink);

#[async_trait]
impl<'a> FactWriter<VoteFact> for SinkBridge<'a> {
    async fn write_facts(
        &self,
        chain_id: &str,
        height: u64,
        runtime_version: u32,
        mapper_version: u32,
        rows: &[(u32, VoteFact)],
    ) -> Result<(), String> {
        self.0
            .write(chain_id, height, runtime_version, mapper_version, rows)
            .await
    }
}

fn run<'a>(mapper: &'a dyn VoteMapper, deps: &'a VotesDeps<'a>) -> ModuleRun<'a, VoteFact> {
    ModuleRun {
        module: MODULE_VOTES,
        checkpoints: deps.checkpoints,
        source: deps.source,
        map: Box::new(move |ev: &CanonicalEvent| mapper.facts(ev)),
        mapper_version: mapper.mapper_version(),
        sink: Box::new(SinkBridge(deps.sink)),
    }
}

/// Map heights `from..=to`. Heights behind the frontier are reprocessed freely
/// (insert-ignore + guarded upserts make it converge) without touching the
/// checkpoint; past it, rows first, checkpoint last (crash = re-map, never skip).
pub async fn votes_range(
    chain_id: &str,
    mapper: &dyn VoteMapper,
    deps: &VotesDeps<'_>,
    from: u64,
    to: u64,
) -> Result<u64, VotesWorkerError> {
    module::run_range(chain_id, &run(mapper, deps), from, to).await
}

/// One follower step: chase the decode (`blocks`) checkpoint. First run starts
/// at the decode tip — history is `votes-range`'s job.
pub async fn votes_tick(
    chain_id: &str,
    mapper: &dyn VoteMapper,
    deps: &VotesDeps<'_>,
) -> Result<u64, VotesWorkerError> {
    module::run_tick(chain_id, &run(mapper, deps)).await
}

/// Follow forever, same backoff discipline as the other followers.
pub async fn votes_follow(
    chain_id: &str,
    mapper: &dyn VoteMapper,
    deps: &VotesDeps<'_>,
    poll: std::time::Duration,
) {
    module::run_follow::<VoteFact, VotesWorkerError>(chain_id, &run(mapper, deps), poll).await
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

    /// Mock mapper: "mock.Vote" → one vote, "mock.Deleg" → one delegation.
    struct MockMapper;
    impl VoteMapper for MockMapper {
        fn facts(&self, event: &CanonicalEvent) -> Result<Vec<VoteFact>, String> {
            match event.name.as_str() {
                "mock.Bad" => Err("unmappable".into()),
                "mock.Vote" => {
                    let id = event.data["poll"].as_u64().ok_or("no poll")?;
                    Ok(vec![VoteFact::Vote(VoteRecord {
                        class: "referenda".into(),
                        referendum_id: Some(id),
                        voter: vec![1u8; 32],
                        kind: "voted".into(),
                        vote_type: "standard".into(),
                        aye_balance: Some(100),
                        nay_balance: None,
                        abstain_balance: None,
                        conviction: Some(1),
                        conviction_label: Some("locked1x".into()),
                        aye_votes: 100,
                        nay_votes: 0,
                        support: 100,
                        attribution: "event".into(),
                        data: event.data.clone(),
                    })])
                }
                "mock.Deleg" => Ok(vec![VoteFact::Delegation(DelegationRecord {
                    class: "referenda".into(),
                    track_id: Some(0),
                    delegator: vec![2u8; 32],
                    target: Some(vec![3u8; 32]),
                    kind: "delegated".into(),
                    attribution: "event".into(),
                    data: event.data.clone(),
                })]),
                _ => Ok(vec![]),
            }
        }
        fn mapper_version(&self) -> u32 {
            1
        }
    }

    #[derive(Default)]
    struct MemSink(Mutex<Vec<(u64, u32, VoteFact)>>);
    #[async_trait]
    impl VoteSink for MemSink {
        async fn write(
            &self,
            _chain: &str,
            height: u64,
            _rv: u32,
            _mv: u32,
            rows: &[(u32, VoteFact)],
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
    async fn range_maps_votes_and_delegations_skips_gaps_and_advances() {
        let mut src = HashMap::new();
        src.insert(1, vec![ev(0, "mock.Vote", serde_json::json!({"poll": 42}))]);
        // height 2 is a decode gap
        src.insert(
            3,
            vec![
                ev(0, "mock.Other", serde_json::json!({})),
                ev(1, "mock.Deleg", serde_json::json!({})),
            ],
        );
        let checkpoints = MemoryCheckpointStore::new();
        let sink = MemSink::default();
        let deps = VotesDeps {
            checkpoints: &checkpoints,
            source: &MemSource(src),
            sink: &sink,
        };
        let n = votes_range("mock", &MockMapper, &deps, 1, 3).await.unwrap();
        assert_eq!(n, 2, "two decoded heights, one gap skipped");
        let rows = sink.0.lock().unwrap();
        assert_eq!(rows.len(), 2);
        assert!(matches!(rows[0].2, VoteFact::Vote(_)));
        assert!(matches!(rows[1].2, VoteFact::Delegation(_)));
        assert_eq!((rows[1].0, rows[1].1), (3, 1));
        drop(rows);
        let cp = checkpoints.get("mock", MODULE_VOTES).await.unwrap().unwrap();
        assert_eq!(cp.last_height, 3, "checkpoint advanced through the gap");
    }

    #[tokio::test]
    async fn rerun_behind_frontier_rewrites_without_moving_checkpoint() {
        let mut src = HashMap::new();
        src.insert(1, vec![ev(0, "mock.Vote", serde_json::json!({"poll": 1}))]);
        src.insert(2, vec![]);
        let checkpoints = MemoryCheckpointStore::new();
        let sink = MemSink::default();
        let deps = VotesDeps {
            checkpoints: &checkpoints,
            source: &MemSource(src),
            sink: &sink,
        };
        votes_range("mock", &MockMapper, &deps, 1, 2).await.unwrap();
        let n = votes_range("mock", &MockMapper, &deps, 1, 1).await.unwrap();
        assert_eq!(n, 1, "behind-frontier reprocess is allowed (sink converges)");
        let cp = checkpoints.get("mock", MODULE_VOTES).await.unwrap().unwrap();
        assert_eq!(cp.last_height, 2, "frontier untouched by reprocess");
    }

    #[tokio::test]
    async fn mapper_errors_halt_loudly_and_advance_nothing() {
        let mut src = HashMap::new();
        src.insert(1, vec![ev(0, "mock.Bad", serde_json::json!({}))]);
        let checkpoints = MemoryCheckpointStore::new();
        let sink = MemSink::default();
        let deps = VotesDeps {
            checkpoints: &checkpoints,
            source: &MemSource(src),
            sink: &sink,
        };
        let err = votes_range("mock", &MockMapper, &deps, 1, 1).await.unwrap_err();
        assert!(matches!(err, VotesWorkerError::Mapper { height: 1, .. }));
        assert!(sink.0.lock().unwrap().is_empty());
        assert!(checkpoints.get("mock", MODULE_VOTES).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn tick_chases_the_decode_checkpoint() {
        let mut src = HashMap::new();
        for h in 5..=9 {
            src.insert(h, vec![ev(0, "mock.Vote", serde_json::json!({"poll": h}))]);
        }
        let checkpoints = MemoryCheckpointStore::new();
        let sink = MemSink::default();
        let deps = VotesDeps {
            checkpoints: &checkpoints,
            source: &MemSource(src),
            sink: &sink,
        };
        assert_eq!(votes_tick("mock", &MockMapper, &deps).await.unwrap(), 0);

        let decode_cp = |h: u64| Checkpoint {
            chain_id: "mock".into(),
            module: MODULE_DECODE.into(),
            last_height: h,
            last_hash: "0x".into(),
            updated_at: chrono::Utc::now(),
        };
        checkpoints.advance(decode_cp(7)).await.unwrap();
        // first run starts at the decode tip (history is votes-range's job)
        assert_eq!(votes_tick("mock", &MockMapper, &deps).await.unwrap(), 1);
        checkpoints.advance(decode_cp(9)).await.unwrap();
        assert_eq!(votes_tick("mock", &MockMapper, &deps).await.unwrap(), 2);
        let cp = checkpoints.get("mock", MODULE_VOTES).await.unwrap().unwrap();
        assert_eq!(cp.last_height, 9);
        assert_eq!(votes_tick("mock", &MockMapper, &deps).await.unwrap(), 0);
    }
}
