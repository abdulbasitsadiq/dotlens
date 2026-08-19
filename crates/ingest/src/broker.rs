//! Broker entitlement worker: canonical events → entitlement facts + the seam
//! (Phase 3, slice 13).
//!
//! The NINTH module on `crate::module`. A sink bridge, a `run()`, three
//! delegations — the frontier rule, decode-gap skip-and-advance, the loud mapper
//! halt, rows-before-checkpoint ordering and the capped follower backoff are all
//! INHERITED. Nothing about the loop is restated here.
//!
//! Checkpoint module: **`broker`**, and the name is a decision rather than a
//! default. `crate::coretime::MODULE_CORETIME` is already `"coretime"`, for
//! slice 11's RELAY occupancy worker. Checkpoints key on `(chain_id, module)` so
//! there is no literal collision — that worker runs on `polkadot` and this one
//! on the Coretime chain — but two different rule sets under one module name is
//! exactly what `xcm` vs `xcm_correlate` was split to avoid, and the two
//! `mapper_version` columns would be indistinguishable in a query.
//!
//! ---------------------------------------------------------------------------
//! IT RUNS ON A CHAIN THIS PROJECT HAD NEVER INDEXED, which is the opposite of
//! slice 11's economics and is why this landed as its own slice. Occupancy was
//! free — relay events Phase 1 had already backfilled. Entitlement needs chain
//! 1005 registered and backfilled first, and the measured cost is ~36.6 KB per
//! block (the parachain inherent, paid whether or not anything happens), i.e.
//! ordinary per BLOCK even though this chain is nearly silent per EVENT.
//!
//! NO WORKER TESTS HERE, on slice 8's own argument: the loop is one algorithm in
//! one file and a ninth copy of `range_maps_events_skips_gaps_and_advances`
//! would assert the same code path again. What is NEW in this slice — the
//! 37-variant vocabulary, the per-variant subject rule and the `CoreAssigned`
//! expansion — is tested where it lives, in `adapter_substrate::broker`.

use crate::module::{self, impl_module_error, EventSource, FactWriter, Mapping, ModuleRun};
use crate::{CheckpointError, CheckpointStore};
use async_trait::async_trait;
use canonical::CanonicalEvent;

pub const MODULE_BROKER: &str = "broker";

#[derive(Debug, thiserror::Error)]
pub enum BrokerWorkerError {
    #[error("event source: {0}")]
    Source(String),
    #[error("broker sink: {0}")]
    Sink(String),
    #[error(
        "broker mapper failed for {chain}/{height} event {event_index} ({event}): {reason} — the \
         entitlement worker halts loudly rather than under-state what was bought. An incomplete \
         entitlement vocabulary reads as waste that did not happen, so the delta would be wrong in \
         the direction that makes the product look most interesting. The expected causes are a new \
         `Broker` variant (the runtime declared 37 when this mapper was written) or a scalar that \
         grew a newtype layer, and the fix is to extend the mapper and re-run the range"
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

impl_module_error!(BrokerWorkerError);

/// One `(CoreAssignment, PartsOf57600)` pair from one `CoreAssigned`, as it
/// lands in the sink.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreAssignmentRow {
    pub assignment_index: u32,
    pub core_index: u32,
    /// A RELAY block number, stated by the chain — which is what makes the join
    /// to `coretime.core_occupancy` an equality rather than a correlation.
    pub relay_block: u64,
    /// idle | pool | task
    pub kind: String,
    /// Set exactly when `kind == "task"`; the schema enforces the pairing.
    pub task_id: Option<u32>,
    /// PartsOf57600. The only surviving trace of the region's core mask.
    pub parts: u32,
}

/// One `Broker.*` event, as it lands in the sink.
///
/// A near-copy of `adapter_substrate::broker::BrokerFact` on purpose:
/// `crates/ingest` must not depend on an adapter (Invariant 4), so the shared
/// shape travels as a plain struct and the mapper trait is what bridges them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerRow {
    /// The variant name without the pallet prefix.
    pub variant: String,
    /// NULL on the 18 variants that name no core, and on those it is honest
    /// rather than missing. (19 of 37 name one; the 15 that name NEITHER a core
    /// nor a task are a different, smaller count.)
    pub core_index: Option<u32>,
    /// The DURABLE identity across sale cycles — a renewal moves the core index
    /// and does not move this.
    pub task_id: Option<u32>,
    /// The variant's whole decoded payload, kept intact (schema-on-read).
    pub data: serde_json::Value,
    /// Empty for 36 of the 37 variants; N rows for `CoreAssigned`.
    pub assignments: Vec<CoreAssignmentRow>,
}

/// Event → entitlement facts. Pure; unknown variants inside the mapped pallet
/// are LOUD, and events of other pallets are silently not ours.
pub trait BrokerMapper: Send + Sync {
    fn facts(&self, event: &CanonicalEvent) -> Result<Vec<BrokerRow>, String>;
    fn mapper_version(&self) -> u32;
}

/// Where entitlement facts land.
///
/// CONTRACT: at most ONE `BrokerRow` per event index — `coretime.broker_events`
/// is keyed by it, so a second row from one event has nowhere to go and a sink
/// refuses such a batch loudly rather than let insert-ignore swallow half of it.
/// The mapper returns a `Vec` only to share the runtime's signature; it emits
/// zero or one.
///
/// The EXPANSION lives inside the row, not in a second fact: one `CoreAssigned`
/// is one event and N assignments, and `coretime.core_assignments` carries an
/// ordinal for exactly that reason.
#[async_trait]
pub trait BrokerSink: Send + Sync {
    async fn write(
        &self,
        chain_id: &str,
        height: u64,
        runtime_version: u32,
        mapper_version: u32,
        rows: &[(u32, BrokerRow)],
    ) -> Result<(), String>;
}

pub struct BrokerDeps<'a> {
    pub checkpoints: &'a dyn CheckpointStore,
    pub source: &'a dyn EventSource,
    pub sink: &'a dyn BrokerSink,
}

struct SinkBridge<'a>(&'a dyn BrokerSink);

#[async_trait]
impl FactWriter<BrokerRow> for SinkBridge<'_> {
    async fn write_facts(
        &self,
        chain_id: &str,
        height: u64,
        runtime_version: u32,
        mapper_version: u32,
        rows: &[(u32, BrokerRow)],
    ) -> Result<(), String> {
        self.0
            .write(chain_id, height, runtime_version, mapper_version, rows)
            .await
    }
}

fn run<'a>(mapper: &'a dyn BrokerMapper, deps: &'a BrokerDeps<'a>) -> ModuleRun<'a, BrokerRow> {
    ModuleRun {
        module: MODULE_BROKER,
        checkpoints: deps.checkpoints,
        source: deps.source,
        map: Mapping::PerEvent(Box::new(move |ev: &CanonicalEvent| mapper.facts(ev))),
        mapper_version: mapper.mapper_version(),
        sink: Box::new(SinkBridge(deps.sink)),
    }
}

pub async fn broker_range(
    chain_id: &str,
    mapper: &dyn BrokerMapper,
    deps: &BrokerDeps<'_>,
    from: u64,
    to: u64,
) -> Result<u64, BrokerWorkerError> {
    module::run_range(chain_id, &run(mapper, deps), from, to).await
}

pub async fn broker_tick(
    chain_id: &str,
    mapper: &dyn BrokerMapper,
    deps: &BrokerDeps<'_>,
) -> Result<u64, BrokerWorkerError> {
    module::run_tick(chain_id, &run(mapper, deps)).await
}

pub async fn broker_follow(
    chain_id: &str,
    mapper: &dyn BrokerMapper,
    deps: &BrokerDeps<'_>,
    poll: std::time::Duration,
) {
    module::run_follow::<BrokerRow, BrokerWorkerError>(chain_id, &run(mapper, deps), poll).await
}
