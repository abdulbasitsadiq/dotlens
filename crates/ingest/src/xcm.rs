//! XCM worker: canonical events → XCM message facts (Phase 3, slice 2).
//!
//! The seventh module on `crate::module`, and the shortest yet — a sink bridge,
//! a `run()`, three delegations. The frontier rule, decode-gap skip-and-advance,
//! loud mapper halt, rows-before-checkpoint ordering and follower backoff are
//! all inherited.
//!
//! Checkpoint module: `xcm`.
//!
//! NO WORKER TESTS HERE, deliberately, and slice 8's own argument is the reason:
//! the loop is one algorithm in one file, already covered by twenty tests living
//! in five other modules. A sixth and seventh copy of
//! `range_maps_events_skips_gaps_and_advances` would assert the same code path
//! again and prove nothing new. What IS new in this slice — the event vocabulary
//! and the two id kinds — is tested where it lives, in
//! `adapter_substrate::xcm`.

use crate::module::{self, impl_module_error, EventSource, FactWriter, Mapping, ModuleRun};
use crate::{CheckpointError, CheckpointStore};
use async_trait::async_trait;
use canonical::CanonicalEvent;

pub const MODULE_XCM: &str = "xcm";

#[derive(Debug, thiserror::Error)]
pub enum XcmWorkerError {
    #[error("event source: {0}")]
    Source(String),
    #[error("xcm sink: {0}")]
    Sink(String),
    #[error(
        "xcm mapper failed for {chain}/{height} event {event_index} ({event}): {reason} — the \
         xcm worker halts loudly rather than under-report cross-chain traffic. A new variant in \
         pallet-xcm, the queue pallets or pallet-message-queue is the expected cause (pallet-xcm \
         19.1.0 already inserted two mid-enum once), and the fix is to extend the mapper and \
         re-run the range"
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

impl_module_error!(XcmWorkerError);

/// One XCM observation — one chain's half of one message.
///
/// It is emphatically NOT a journey. Everything here is what a single event on a
/// single chain said; whether a `sent` row and a `received` row are the same
/// message is a question for the correlation layer, and on the runtimes dotlens
/// indexes it is frequently unanswerable (see the adapter's module header).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XcmFact {
    /// sent | received | local
    pub side: String,
    /// hrmp | ump | dmp | local | unknown
    pub transport: String,
    /// 0x-hex, 32 bytes. None only where the event genuinely carries no id.
    pub message_id: Option<String>,
    /// topic | wire_hash | ambiguous | none — WHICH id this is, because the two
    /// kinds are not interchangeable and `messageQueue` does not say which of
    /// them it is reporting.
    pub id_kind: String,
    /// para:<id> | parent | here | None
    pub counterparty: Option<String>,
    pub origin_location: Option<serde_json::Value>,
    pub destination: Option<serde_json::Value>,
    pub message: Option<serde_json::Value>,
    /// True when the sender's `message` was empty — the executor's deliberate
    /// signal that this send came from a forwarded XCM rather than from a
    /// pallet-xcm call.
    pub forwarded: bool,
    pub status: String,
    pub success: Option<bool>,
    pub error: Option<serde_json::Value>,
    pub weight_used: Option<serde_json::Value>,
    pub data: serde_json::Value,
}

/// Event → XCM facts. Pure; unknown events inside a mapped pallet are LOUD.
pub trait XcmMapper: Send + Sync {
    fn facts(&self, event: &CanonicalEvent) -> Result<Vec<XcmFact>, String>;
    fn mapper_version(&self) -> u32;
}

/// Where XCM facts land. CONTRACT: at most ONE fact per event index — the table
/// is keyed (chain, block, event), so a second fact from one event has nowhere
/// to go, and a sink refuses such a batch loudly rather than let insert-ignore
/// swallow half of it.
#[async_trait]
pub trait XcmSink: Send + Sync {
    async fn write(
        &self,
        chain_id: &str,
        height: u64,
        runtime_version: u32,
        mapper_version: u32,
        rows: &[(u32, XcmFact)],
    ) -> Result<(), String>;
}

pub struct XcmDeps<'a> {
    pub checkpoints: &'a dyn CheckpointStore,
    pub source: &'a dyn EventSource,
    pub sink: &'a dyn XcmSink,
}

struct SinkBridge<'a>(&'a dyn XcmSink);

#[async_trait]
impl FactWriter<XcmFact> for SinkBridge<'_> {
    async fn write_facts(
        &self,
        chain_id: &str,
        height: u64,
        runtime_version: u32,
        mapper_version: u32,
        rows: &[(u32, XcmFact)],
    ) -> Result<(), String> {
        self.0
            .write(chain_id, height, runtime_version, mapper_version, rows)
            .await
    }
}

fn run<'a>(mapper: &'a dyn XcmMapper, deps: &'a XcmDeps<'a>) -> ModuleRun<'a, XcmFact> {
    ModuleRun {
        module: MODULE_XCM,
        checkpoints: deps.checkpoints,
        source: deps.source,
        map: Mapping::PerEvent(Box::new(move |ev: &CanonicalEvent| mapper.facts(ev))),
        mapper_version: mapper.mapper_version(),
        sink: Box::new(SinkBridge(deps.sink)),
    }
}

pub async fn xcm_range(
    chain_id: &str,
    mapper: &dyn XcmMapper,
    deps: &XcmDeps<'_>,
    from: u64,
    to: u64,
) -> Result<u64, XcmWorkerError> {
    module::run_range(chain_id, &run(mapper, deps), from, to).await
}

pub async fn xcm_tick(
    chain_id: &str,
    mapper: &dyn XcmMapper,
    deps: &XcmDeps<'_>,
) -> Result<u64, XcmWorkerError> {
    module::run_tick(chain_id, &run(mapper, deps)).await
}

pub async fn xcm_follow(
    chain_id: &str,
    mapper: &dyn XcmMapper,
    deps: &XcmDeps<'_>,
    poll: std::time::Duration,
) {
    module::run_follow::<XcmFact, XcmWorkerError>(chain_id, &run(mapper, deps), poll).await
}
