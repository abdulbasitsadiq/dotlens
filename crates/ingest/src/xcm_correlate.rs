//! XCM correlation worker: one block's XCM events → the id aliases inside it
//! (Phase 3, slice 3).
//!
//! The eighth module on `crate::module`, and the first to use its BLOCK-level
//! mapping, because the subject here is a relationship between two events
//! rather than a fact about one.
//!
//! Checkpoint module: `xcm_correlate` — its OWN key, deliberately, and not
//! `xcm`'s. The two workers read the same events and write different tables, so
//! sharing a checkpoint would make each one's progress silently skip the
//! other's work. It chases the same decode frontier `xcm` does, so in practice
//! they run neck and neck.
//!
//! WHY THIS IS A SEPARATE WORKER RATHER THAN A FEW MORE LINES IN `ingest::xcm`.
//! The observation and the inference have different lifetimes. `xcm.messages`
//! rows are what a chain SAID and change only when the decoder does;
//! `xcm.message_links` rows are what we CONCLUDED and change whenever the
//! pairing rule does. Folding them together would mean bumping
//! XCM_MAPPER_VERSION — and rewriting every observation row's lineage — to fix a
//! correlation rule that never touched an observation. Two versions, two
//! checkpoints, two commands.
//!
//! TWO WORKER TESTS HERE, and the count is the argument. Slice 8's rule is that
//! the shared loop is covered twenty times over in five other modules and a
//! sixth copy of `range_maps_events_skips_gaps_and_advances` proves nothing —
//! which is why this file does not have one. But `Mapping::PerBlock` is a NEW
//! arm of that loop, and its only other exercise is a pg test that returns early
//! without `DATABASE_URL`: `cargo test` on a laptop with no database would run
//! the one genuinely new line in `module.rs` zero times. So the two tests below
//! cover exactly what is new and nothing that is not — that the arm reaches the
//! sink at all, and that its halt names the offending EVENT rather than just the
//! block. The pairing RULE is tested where it lives, in
//! `adapter_substrate::xcm_correlate`.

use crate::module::{self, impl_module_error, BlockMapError, EventSource, FactWriter, Mapping,
                    ModuleRun};
use crate::{CheckpointError, CheckpointStore};
use async_trait::async_trait;
use canonical::CanonicalEvent;

pub const MODULE_XCM_CORRELATE: &str = "xcm_correlate";

#[derive(Debug, thiserror::Error)]
pub enum XcmCorrelateWorkerError {
    #[error("event source: {0}")]
    Source(String),
    #[error("xcm link sink: {0}")]
    Sink(String),
    #[error(
        "xcm correlator failed for {chain}/{height} event {event_index} ({event}): {reason} — the \
         correlator halts loudly rather than emit a link it is not sure of. It re-derives XCM \
         facts with the same mapper the `xcm` worker uses, so the usual cause is a new event \
         variant that halts THAT worker too; extend the mapper and re-run both ranges"
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

impl_module_error!(XcmCorrelateWorkerError);

/// Two ids that name ONE message, established inside one block on one chain.
///
/// This is an INFERENCE and every field after `transport` exists to say so out
/// loud. There is no event on any chain that states this relationship: the
/// sending pallet emits the topic, the router emits the hash it computed, and
/// `WithUniqueTopic::deliver` discards the latter without ever mentioning the
/// former. What connects them is that they happened in the same block, in that
/// order, with nothing else they could belong to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XcmLink {
    /// blake2_256 of the queued bytes — `xcmpQueue.XcmpMessageSent` /
    /// `parachainSystem.UpwardMessageSent`. The row is keyed by ITS event index
    /// (the `u32` the mapper returns alongside this struct), because one queued
    /// message is delivered exactly once.
    pub wire_hash: String,
    /// `pallet_xcm.Sent.message_id` — `frame_system::unique`, not a hash of
    /// anything, and the id the receiving chain will almost always report.
    pub topic: String,
    pub topic_event_index: u32,
    /// hrmp | ump. A pair is never established across transports.
    pub transport: String,
    /// unique_in_block | interleaved
    pub rule: String,
    /// high | medium
    pub confidence: String,
    /// The counts and offsets the rule was decided on — enough to audit the
    /// claim without re-deriving the block.
    pub evidence: serde_json::Value,
}

/// One block's XCM events → its id aliases. Pure.
///
/// Returns `(wire_event_index, link)`. Unknown XCM events are a LOUD error for
/// the same reason they are in the mapper: a new variant is how a runtime
/// upgrade silently changes what "all the XCM traffic" means, and a correlator
/// that shrugged would quietly stop pairing.
pub trait XcmCorrelator: Send + Sync {
    fn links(&self, events: &[CanonicalEvent]) -> Result<Vec<(u32, XcmLink)>, BlockMapError>;
    fn correlator_version(&self) -> u32;
}

/// Where links land.
///
/// CONTRACT, and it differs from every other sink in the project: a block's
/// links are written DELETE-then-INSERT, not insert-ignore. They are a pure
/// function of the block under the current rule, so a re-run after a rule change
/// must be able to REPLACE them — an append-only table would keep serving the
/// old inference forever. (The one asymmetry: the shared runtime does not call a
/// sink with zero rows, so a rule that stops firing on a block leaves its old
/// row behind. Migration 0016 records the one-line fix.)
#[async_trait]
pub trait XcmLinkSink: Send + Sync {
    async fn write(
        &self,
        chain_id: &str,
        height: u64,
        runtime_version: u32,
        correlator_version: u32,
        rows: &[(u32, XcmLink)],
    ) -> Result<(), String>;
}

pub struct XcmCorrelateDeps<'a> {
    pub checkpoints: &'a dyn CheckpointStore,
    pub source: &'a dyn EventSource,
    pub sink: &'a dyn XcmLinkSink,
}

struct SinkBridge<'a>(&'a dyn XcmLinkSink);

#[async_trait]
impl FactWriter<XcmLink> for SinkBridge<'_> {
    async fn write_facts(
        &self,
        chain_id: &str,
        height: u64,
        runtime_version: u32,
        mapper_version: u32,
        rows: &[(u32, XcmLink)],
    ) -> Result<(), String> {
        // The runtime calls its lineage field `mapper_version`; for this module
        // that number IS the correlator version, and the sink names it so.
        self.0
            .write(chain_id, height, runtime_version, mapper_version, rows)
            .await
    }
}

fn run<'a>(
    correlator: &'a dyn XcmCorrelator,
    deps: &'a XcmCorrelateDeps<'a>,
) -> ModuleRun<'a, XcmLink> {
    ModuleRun {
        module: MODULE_XCM_CORRELATE,
        checkpoints: deps.checkpoints,
        source: deps.source,
        map: Mapping::PerBlock(Box::new(move |events: &[CanonicalEvent]| {
            correlator.links(events)
        })),
        mapper_version: correlator.correlator_version(),
        sink: Box::new(SinkBridge(deps.sink)),
    }
}

pub async fn xcm_correlate_range(
    chain_id: &str,
    correlator: &dyn XcmCorrelator,
    deps: &XcmCorrelateDeps<'_>,
    from: u64,
    to: u64,
) -> Result<u64, XcmCorrelateWorkerError> {
    module::run_range(chain_id, &run(correlator, deps), from, to).await
}

pub async fn xcm_correlate_tick(
    chain_id: &str,
    correlator: &dyn XcmCorrelator,
    deps: &XcmCorrelateDeps<'_>,
) -> Result<u64, XcmCorrelateWorkerError> {
    module::run_tick(chain_id, &run(correlator, deps)).await
}

pub async fn xcm_correlate_follow(
    chain_id: &str,
    correlator: &dyn XcmCorrelator,
    deps: &XcmCorrelateDeps<'_>,
    poll: std::time::Duration,
) {
    module::run_follow::<XcmLink, XcmCorrelateWorkerError>(chain_id, &run(correlator, deps), poll)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::module::BlockEvents;
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

    /// Mock correlator: pairs the FIRST two events of a block, and refuses a
    /// block containing `mock.Bad`. It deliberately knows nothing about XCM —
    /// what is under test here is the runtime's block-level arm, not the rule.
    struct MockCorrelator;
    impl XcmCorrelator for MockCorrelator {
        fn links(&self, events: &[CanonicalEvent]) -> Result<Vec<(u32, XcmLink)>, BlockMapError> {
            if let Some(bad) = events.iter().find(|e| e.name == "mock.Bad") {
                return Err(BlockMapError {
                    event_index: bad.index,
                    event: bad.name.clone(),
                    reason: "unpairable".into(),
                });
            }
            let [wire, topic, ..] = events else {
                return Ok(vec![]);
            };
            Ok(vec![(
                wire.index,
                XcmLink {
                    wire_hash: "0x77".into(),
                    topic: "0xee".into(),
                    topic_event_index: topic.index,
                    transport: "hrmp".into(),
                    rule: "unique_in_block".into(),
                    confidence: "high".into(),
                    evidence: serde_json::json!({}),
                },
            )])
        }
        fn correlator_version(&self) -> u32 {
            1
        }
    }

    #[derive(Default)]
    struct MemSink(Mutex<Vec<(u64, u32, XcmLink)>>);
    #[async_trait]
    impl XcmLinkSink for MemSink {
        async fn write(
            &self,
            _chain: &str,
            height: u64,
            _rv: u32,
            _cv: u32,
            rows: &[(u32, XcmLink)],
        ) -> Result<(), String> {
            let mut g = self.0.lock().unwrap();
            for (idx, l) in rows {
                g.push((height, *idx, l.clone()));
            }
            Ok(())
        }
    }

    fn ev(index: u32, name: &str) -> CanonicalEvent {
        CanonicalEvent {
            index,
            transaction_index: None,
            name: name.into(),
            data: serde_json::json!({}),
        }
    }

    /// The block-level arm reaches the sink, keyed by the event index the MAPPER
    /// chose rather than by the event it was iterating — which is the whole
    /// difference between this arm and the per-event one.
    #[tokio::test]
    async fn a_block_level_mapper_keys_its_rows_by_the_index_it_chose() {
        let mut src = HashMap::new();
        src.insert(7, vec![ev(3, "mock.Wire"), ev(4, "mock.Sent"), ev(5, "mock.Other")]);
        let checkpoints = MemoryCheckpointStore::new();
        let sink = MemSink::default();
        let deps = XcmCorrelateDeps {
            checkpoints: &checkpoints,
            source: &MemSource(src),
            sink: &sink,
        };
        let n = xcm_correlate_range("polkadot-asset-hub", &MockCorrelator, &deps, 7, 7)
            .await
            .expect("no halt");
        assert_eq!(n, 1);
        let rows = sink.0.lock().unwrap();
        assert_eq!(rows.len(), 1, "one link from three events");
        assert_eq!((rows[0].0, rows[0].1), (7, 3), "keyed by the WIRE event, not by 0");
        assert_eq!(rows[0].2.topic_event_index, 4);
    }

    /// A block-level halt still names the event. The per-event arm gets that for
    /// free because the runtime knows which event it passed in; this arm has to
    /// carry it back, and a halt that said only "block 7" would leave whoever
    /// reads it at 3am to find the row themselves.
    #[tokio::test]
    async fn a_block_level_halt_names_the_offending_event_and_advances_nothing() {
        let mut src = HashMap::new();
        src.insert(7, vec![ev(0, "mock.Wire"), ev(1, "mock.Bad")]);
        let checkpoints = MemoryCheckpointStore::new();
        let sink = MemSink::default();
        let deps = XcmCorrelateDeps {
            checkpoints: &checkpoints,
            source: &MemSource(src),
            sink: &sink,
        };
        let err = xcm_correlate_range("polkadot-asset-hub", &MockCorrelator, &deps, 7, 7)
            .await
            .expect_err("a correlator error must halt the range");
        assert!(
            matches!(
                &err,
                XcmCorrelateWorkerError::Mapper { height: 7, event_index: 1, event, .. }
                    if event.as_str() == "mock.Bad"
            ),
            "{err:?}"
        );
        assert!(sink.0.lock().unwrap().is_empty(), "a halt writes nothing");
        assert!(
            CheckpointStore::get(&checkpoints, "polkadot-asset-hub", MODULE_XCM_CORRELATE)
                .await
                .unwrap()
                .is_none(),
            "a halt advances nothing"
        );
    }
}
