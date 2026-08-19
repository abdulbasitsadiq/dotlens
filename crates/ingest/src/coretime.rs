//! Coretime occupancy worker: canonical events → core occupancy facts
//! (Phase 3, slice 11).
//!
//! The EIGHTH module on `crate::module`, and the payoff for slice 8's refactor
//! is the whole of this file: a sink bridge, a `run()`, three delegations. The
//! frontier rule, decode-gap skip-and-advance, the loud mapper halt,
//! rows-before-checkpoint ordering and the capped follower backoff are all
//! INHERITED. Nothing about the loop is restated here, and the four worker tests
//! that cover it live in five other modules.
//!
//! Checkpoint module: `coretime`.
//!
//! ---------------------------------------------------------------------------
//! IT RUNS ON THE RELAY, and that is the slice's cheapest and least obvious
//! fact. Occupancy is a question about parachains answered entirely by
//! relay-chain events — `paraInclusion` names both the para id and the core
//! index — so this worker needs NO parachain indexed, no Coretime chain
//! registered, and no new backfill: Phase 1 already put the relay blocks in the
//! raw store. The prep pass decoded 1,542 of them across six runtimes with no
//! RPC at all.
//!
//! NO WORKER TESTS HERE, on slice 8's own argument: the loop is one algorithm in
//! one file and a sixth copy of `range_maps_events_skips_gaps_and_advances`
//! would assert the same code path again. What is NEW in this slice — the
//! per-variant shape rule, the newtype layer on the core index, and the refusal
//! to read the descriptor's core index — is tested where it lives, in
//! `adapter_substrate::coretime`.

use crate::module::{self, impl_module_error, EventSource, FactWriter, Mapping, ModuleRun};
use crate::{CheckpointError, CheckpointStore};
use async_trait::async_trait;
use canonical::CanonicalEvent;

pub const MODULE_CORETIME: &str = "coretime";

#[derive(Debug, thiserror::Error)]
pub enum CoretimeWorkerError {
    #[error("event source: {0}")]
    Source(String),
    #[error("coretime sink: {0}")]
    Sink(String),
    #[error(
        "coretime mapper failed for {chain}/{height} event {event_index} ({event}): {reason} — \
         the occupancy worker halts loudly rather than under-report which cores did work. The \
         expected causes are a new `parainclusion` variant or a candidate event whose positional \
         shape moved (the receipt's descriptor has already changed twice across the runtimes \
         dotlens indexes), and the fix is to extend the mapper and re-run the range"
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

impl_module_error!(CoretimeWorkerError);

/// One candidate's occupancy of one core, as it lands in the sink.
///
/// A near-copy of `adapter_substrate::coretime::OccupancyFact` on purpose:
/// `crates/ingest` must not depend on an adapter (Invariant 4 — the runtime is
/// generic and the adapter is where protocol lives), so the shared shape travels
/// as a plain struct and the mapper trait is what bridges them. Same split every
/// other module here uses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OccupancyRow {
    /// included | backed | timed_out.
    ///
    /// `included` is THE occupancy fact — a para-block produced on this core is
    /// part of the chain. `backed` is kept because backed-without-included is
    /// the wasted-coretime signal, and a table that cannot express it cannot
    /// detect it. `timed_out` is the runtime saying so directly, and has zero
    /// live instances (see the migration).
    pub kind: String,
    pub core_index: u32,
    pub para_id: u32,
    /// None on `timed_out`, which carries three fields where the others carry
    /// four — not a zero.
    pub group_index: Option<u32>,
    /// 0x-hex. Resolving it to a height needs `core.blocks` and is the SINK's
    /// job: a pure mapper has no index to look it up in.
    pub relay_parent_hash: Option<String>,
    /// 0x-hex, unique per candidate — what matches a `backed` row to its
    /// `included` one across the 2–6 block async-backing lag.
    pub pov_hash: Option<String>,
}

/// Event → occupancy facts. Pure; unknown events inside the mapped pallet are
/// LOUD, and events of other pallets are silently not ours.
pub trait OccupancyMapper: Send + Sync {
    fn facts(&self, event: &CanonicalEvent) -> Result<Vec<OccupancyRow>, String>;
    fn mapper_version(&self) -> u32;
}

/// Where occupancy facts land.
///
/// CONTRACT: at most ONE fact per event index — the table is keyed
/// (chain, block, event), so a second fact from one event has nowhere to go and
/// a sink refuses such a batch loudly rather than let insert-ignore swallow half
/// of it. The mapper returns a `Vec` only to share the runtime's signature; it
/// emits zero or one.
#[async_trait]
pub trait OccupancySink: Send + Sync {
    async fn write(
        &self,
        chain_id: &str,
        height: u64,
        runtime_version: u32,
        mapper_version: u32,
        rows: &[(u32, OccupancyRow)],
    ) -> Result<(), String>;
}

pub struct CoretimeDeps<'a> {
    pub checkpoints: &'a dyn CheckpointStore,
    pub source: &'a dyn EventSource,
    pub sink: &'a dyn OccupancySink,
}

struct SinkBridge<'a>(&'a dyn OccupancySink);

#[async_trait]
impl FactWriter<OccupancyRow> for SinkBridge<'_> {
    async fn write_facts(
        &self,
        chain_id: &str,
        height: u64,
        runtime_version: u32,
        mapper_version: u32,
        rows: &[(u32, OccupancyRow)],
    ) -> Result<(), String> {
        self.0
            .write(chain_id, height, runtime_version, mapper_version, rows)
            .await
    }
}

fn run<'a>(
    mapper: &'a dyn OccupancyMapper,
    deps: &'a CoretimeDeps<'a>,
) -> ModuleRun<'a, OccupancyRow> {
    ModuleRun {
        module: MODULE_CORETIME,
        checkpoints: deps.checkpoints,
        source: deps.source,
        map: Mapping::PerEvent(Box::new(move |ev: &CanonicalEvent| mapper.facts(ev))),
        mapper_version: mapper.mapper_version(),
        sink: Box::new(SinkBridge(deps.sink)),
    }
}

pub async fn coretime_range(
    chain_id: &str,
    mapper: &dyn OccupancyMapper,
    deps: &CoretimeDeps<'_>,
    from: u64,
    to: u64,
) -> Result<u64, CoretimeWorkerError> {
    module::run_range(chain_id, &run(mapper, deps), from, to).await
}

pub async fn coretime_tick(
    chain_id: &str,
    mapper: &dyn OccupancyMapper,
    deps: &CoretimeDeps<'_>,
) -> Result<u64, CoretimeWorkerError> {
    module::run_tick(chain_id, &run(mapper, deps)).await
}

pub async fn coretime_follow(
    chain_id: &str,
    mapper: &dyn OccupancyMapper,
    deps: &CoretimeDeps<'_>,
    poll: std::time::Duration,
) {
    module::run_follow::<OccupancyRow, CoretimeWorkerError>(chain_id, &run(mapper, deps), poll)
        .await
}
