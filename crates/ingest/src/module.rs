//! The one worker algorithm every domain module runs.
//!
//! MEASURED, not assumed. ROADMAP §Phase 2 recorded four hand-copies of this
//! loop after the treasury slice; the bounties slice made five. Normalizing the
//! five `*_range`/`advance`/`*_tick`/`*_follow` bodies for type names left them
//! **identical** — the only substantive difference in the whole set was a dead
//! `hash: &str` parameter on the balances copy of `advance`, which every call
//! site passed `"-"`. Five copies of one algorithm means a fix to the frontier
//! rule lands in one file and silently does not land in four others.
//!
//! WHAT STAYS PER-MODULE, deliberately:
//!   * the mapper's rules — they differ genuinely and live in adapter crates
//!     (Invariant 4). Nothing in `adapter-substrate` changed for this refactor.
//!   * the sink's SQL — five schemas, five sets of ordering guards.
//!   * the wording of the halt — "gaps in money", "gaps in referendum history"
//!     and "money they cannot explain" are different warnings to whoever reads
//!     the log at 3am, and the existing tests match on the concrete variants.
//!     Hence `ModuleError` rather than one blurred error owned by this file.
//!
//! WHAT LIVES HERE, once:
//!   * the checkpoint-as-FRONTIER rule — anything behind it may be reprocessed
//!     freely (sinks are insert-ignore + ordering-guarded, so replay in any
//!     order converges) and the frontier is never regressed by a reprocess;
//!   * decode-gap skip-and-advance — a height the canonical store has not
//!     decoded is skipped, and AHEAD of the frontier we still advance, so a
//!     follower can never wedge on a hole (the height gets its facts when
//!     decode gap-fill plus a later `*-range` re-run revisit it);
//!   * the loud mapper halt — a mapper error stops the range, writes nothing
//!     and advances nothing;
//!   * rows first, checkpoint last — a crash re-maps, never skips;
//!   * first tick starts at the DECODE tip, because history is `*-range`'s job;
//!   * follower backoff, linear in consecutive failures, capped at 11×.
//!
//! The five public entry points (`balances_range`, `gov_tick`, `votes_follow`,
//! …) keep their exact names and signatures, so `dotlens-node` and every test
//! call them unchanged. That is the proof obligation for this refactor: the
//! suite must pass at the same count, with the same test names, having been
//! edited nowhere.

use crate::decode::MODULE_DECODE;
use crate::{Checkpoint, CheckpointError, CheckpointStore};
use async_trait::async_trait;
use canonical::CanonicalEvent;

/// The decoded events of one canonical block, as the workers need them.
pub struct BlockEvents {
    pub runtime_version: u32,
    pub events: Vec<CanonicalEvent>,
}

/// Where canonical events come from (node-side: core.blocks + core.events).
/// `None` = this height isn't decoded (a decode gap) — skip, don't fail.
///
/// One contract, one Pg implementation, five consumers. That was already true
/// before this refactor — gov, votes, treasury and bounties all imported it
/// from `balances`, which made every domain module depend on the balances
/// module for a type that was never about balances. It now lives where it
/// belongs; `balances` re-exports it, so `ingest::balances::EventSource` (which
/// `dotlens-node::balances_pg` imports) keeps resolving.
#[async_trait]
pub trait EventSource: Send + Sync {
    async fn decoded_events(
        &self,
        chain_id: &str,
        height: u64,
    ) -> Result<Option<BlockEvents>, String>;
}

/// How the shared runtime builds a module's OWN error.
///
/// The runtime is generic over the error rather than owning one because the
/// halt message is the module's editorial voice, not the runtime's, and because
/// `matches!(err, BalancesWorkerError::Mapper { height: 1, .. })` in the
/// existing tests must keep compiling and meaning the same thing.
pub trait ModuleError: std::error::Error + From<CheckpointError> + Send + Sync + 'static {
    fn source_failed(reason: String) -> Self;
    fn sink_failed(reason: String) -> Self;
    fn mapper_failed(
        chain: String,
        height: u64,
        event_index: u32,
        event: String,
        reason: String,
    ) -> Self;
}

/// Implements [`ModuleError`] for an error enum shaped like every domain
/// worker's — a `Source(String)`, a `Sink(String)`, and a `Mapper { chain,
/// height, event_index, event, reason }`.
///
/// A macro rather than five hand-written impls for the reason this whole file
/// exists: five mechanical copies is where a transposition hides. Wiring
/// `source_failed` to `Self::Sink` in one of five files compiles perfectly and
/// mislabels every source failure in that module for as long as nobody reads
/// it. Written once, it cannot happen.
macro_rules! impl_module_error {
    ($ty:ty) => {
        impl $crate::module::ModuleError for $ty {
            fn source_failed(reason: String) -> Self {
                Self::Source(reason)
            }
            fn sink_failed(reason: String) -> Self {
                Self::Sink(reason)
            }
            fn mapper_failed(
                chain: String,
                height: u64,
                event_index: u32,
                event: String,
                reason: String,
            ) -> Self {
                Self::Mapper {
                    chain,
                    height,
                    event_index,
                    event,
                    reason,
                }
            }
        }
    };
}
pub(crate) use impl_module_error;

/// Why a BLOCK-level mapper refused, in the coordinates the halt message needs.
///
/// A per-event mapper is handed one event and returns a `String`, because the
/// runtime already knows which event it passed in. A block-level one is handed
/// the whole list and must SAY which event it choked on, or the halt names a
/// block and leaves whoever reads it at 3am to find the row themselves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockMapError {
    pub event_index: u32,
    pub event: String,
    pub reason: String,
}

/// How a module turns one decoded block into facts.
///
/// Seven modules map ONE EVENT AT A TIME and cannot see their neighbours, which
/// is the right shape for "this event says money moved". The XCM correlator
/// cannot work that way: its whole subject is a RELATIONSHIP between two events
/// in one block (a wire-hash send and the topic send that discarded it), so a
/// per-event closure would have to keep state between calls — and a mapper with
/// hidden state is a mapper whose output depends on how it was invoked.
///
/// So the runtime learns one new shape rather than the correlator growing a
/// seventh hand-copy of `run_range`. That is the same trade slice 8 made: the
/// loop stays in one file, and the price is one enum here instead of ~200
/// duplicated lines there.
pub enum Mapping<'a, F> {
    /// `Fn(&event) -> facts`. The row's event index is the event's own.
    PerEvent(Box<dyn Fn(&CanonicalEvent) -> Result<Vec<F>, String> + Send + Sync + 'a>),
    /// `Fn(&[event]) -> (event index, fact)`. The mapper CHOOSES each row's
    /// event index, because a fact about a relationship has to be keyed to one
    /// of its two ends and only the mapper knows which.
    PerBlock(
        #[allow(clippy::type_complexity)]
        Box<dyn Fn(&[CanonicalEvent]) -> Result<Vec<(u32, F)>, BlockMapError> + Send + Sync + 'a>,
    ),
}

/// Where one block's facts land.
///
/// Every domain sink already had precisely this signature with only the fact
/// type changing — `(chain, height, runtime_version, mapper_version, [(event
/// index, fact)])` — which is what makes the runtime generic over `F` instead
/// of over five near-identical sink traits. The domain traits (`DeltaSink`,
/// `TimelineSink`, `VoteSink`, `TreasurySink`, `BountySink`) stay exactly as
/// they are and reach the runtime through a per-module bridge, so no sink
/// implementation in `dotlens-node` changed either.
#[async_trait]
pub trait FactWriter<F>: Send + Sync {
    async fn write_facts(
        &self,
        chain_id: &str,
        height: u64,
        runtime_version: u32,
        mapper_version: u32,
        rows: &[(u32, F)],
    ) -> Result<(), String>;
}

/// One module's wiring, as values.
///
/// The mapper stays behind its own domain trait in its own file and reaches the
/// runtime as a boxed closure — one allocation per range or per follower, not
/// per block. `mapper_version` is captured once here for the same reason it was
/// read once per block before: it is a constant for the life of a run, and it
/// is lineage, so it travels with every row the run writes.
pub struct ModuleRun<'a, F> {
    /// Checkpoint module name in `indexer_state`, e.g. "balances". Also the
    /// log discriminator.
    ///
    /// A first draft carried a separate `label` field "in case a checkpoint key
    /// and a log label ever need to differ". Review killed it, correctly: the
    /// five `module` values are each proven by a test (get the constant wrong
    /// and that module's own `range_maps_events_skips_gaps_and_advances`
    /// unwraps a `None` checkpoint and panics), while a `label` reaches nothing
    /// but a log line — so a votes worker labelled "gov" would compile, pass
    /// all 186 tests, and lie in production for as long as nobody read the log.
    /// That is the exact failure mode `impl_module_error!` exists to prevent,
    /// one field over. Reintroduce the split the day something needs it.
    pub module: &'static str,
    pub checkpoints: &'a dyn CheckpointStore,
    pub source: &'a dyn EventSource,
    pub map: Mapping<'a, F>,
    /// REQUIREMENT, not an optimisation: this is read ONCE, when the run is
    /// built, and every row the run writes carries it. For a `*_range` that is
    /// one range; for a `*_follow` it is the lifetime of the process. The five
    /// mappers all return a `const` today, so this matches the old
    /// per-block read exactly — but a mapper that ever derived its version from
    /// state would pin stale lineage on every row for days, and lineage is the
    /// one field Invariant 3 exists to protect. A mapper version must be
    /// constant for the life of a run.
    pub mapper_version: u32,
    pub sink: Box<dyn FactWriter<F> + 'a>,
}

/// Map heights `from..=to`. Heights behind the frontier are reprocessed freely
/// (the sinks converge) without touching the checkpoint; past it, rows first,
/// checkpoint last (crash = re-map, never skip).
pub async fn run_range<F, E>(
    chain_id: &str,
    run: &ModuleRun<'_, F>,
    from: u64,
    to: u64,
) -> Result<u64, E>
where
    F: Send + Sync,
    E: ModuleError,
{
    let frontier = run
        .checkpoints
        .get(chain_id, run.module)
        .await?
        .map(|cp| cp.last_height);
    let mut processed = 0u64;

    for height in from..=to {
        let behind_frontier = frontier.is_some_and(|f| height <= f);
        let block: Option<BlockEvents> = run
            .source
            .decoded_events(chain_id, height)
            .await
            .map_err(E::source_failed)?;
        let Some(block) = block else {
            // decode gap: skip. Ahead of the frontier we still advance so the
            // follower never wedges on a hole; the height gets its facts when
            // decode gap-fill + a later `*-range` re-run revisit it.
            //
            // `module` is a FIELD here where the five copies never needed one:
            // they lived in five files, so `tracing`'s default target
            // (`module_path!()`) said which worker skipped. One file means one
            // target, and this line — unlike the two below — does not name the
            // module in its message. Without the field, five workers would log
            // the same sentence indistinguishably during a decode gap.
            tracing::debug!(module = run.module, chain = %chain_id, height, "no canonical block — skipped");
            if !behind_frontier {
                advance(run, chain_id, height).await?;
            }
            continue;
        };

        let rows: Vec<(u32, F)> = match &run.map {
            Mapping::PerEvent(map) => {
                let mut rows = Vec::new();
                for ev in &block.events {
                    let facts = map(ev).map_err(|reason| {
                        E::mapper_failed(
                            chain_id.to_string(),
                            height,
                            ev.index,
                            ev.name.clone(),
                            reason,
                        )
                    })?;
                    for f in facts {
                        rows.push((ev.index, f));
                    }
                }
                rows
            }
            // Same halt, same wording, same "writes nothing and advances
            // nothing" — the only difference is who names the offending event.
            Mapping::PerBlock(map) => map(&block.events).map_err(|e| {
                E::mapper_failed(
                    chain_id.to_string(),
                    height,
                    e.event_index,
                    e.event,
                    e.reason,
                )
            })?,
        };
        if !rows.is_empty() {
            run.sink
                .write_facts(
                    chain_id,
                    height,
                    block.runtime_version,
                    run.mapper_version,
                    &rows,
                )
                .await
                .map_err(E::sink_failed)?;
        }
        if !behind_frontier {
            advance(run, chain_id, height).await?;
        }
        processed += 1;
    }
    Ok(processed)
}

async fn advance<F>(
    run: &ModuleRun<'_, F>,
    chain_id: &str,
    height: u64,
) -> Result<(), CheckpointError> {
    run.checkpoints
        .advance(Checkpoint {
            chain_id: chain_id.to_string(),
            module: run.module.to_string(),
            last_height: height,
            // No module has ever recorded a block hash on a derived checkpoint:
            // the hash that matters is on the `blocks` checkpoint this one
            // chases. The balances copy carried a `hash` parameter that every
            // call site passed "-"; it is gone, and the literal stays.
            last_hash: "-".to_string(),
            updated_at: chrono::Utc::now(),
        })
        .await
}

/// One follower step: chase the decode (`blocks`) checkpoint. First run starts
/// at the decode tip — history is `*-range`'s job (the same first-run semantics
/// decode itself has against raw).
pub async fn run_tick<F, E>(chain_id: &str, run: &ModuleRun<'_, F>) -> Result<u64, E>
where
    F: Send + Sync,
    E: ModuleError,
{
    let Some(decode_cp) = run.checkpoints.get(chain_id, MODULE_DECODE).await? else {
        return Ok(0); // nothing decoded yet
    };
    let target = decode_cp.last_height;
    let from = match run.checkpoints.get(chain_id, run.module).await? {
        Some(cp) if cp.last_height >= target => return Ok(0),
        Some(cp) => cp.last_height + 1,
        None => target, // first run: start at the decode tip
    };
    run_range::<F, E>(chain_id, run, from, target).await
}

/// Follow forever. Backoff is linear in consecutive failures and capped, so a
/// wedged module keeps complaining at a steady rate rather than going quiet.
pub async fn run_follow<F, E>(chain_id: &str, run: &ModuleRun<'_, F>, poll: std::time::Duration)
where
    F: Send + Sync,
    E: ModuleError,
{
    let mut consecutive_failures = 0u32;
    loop {
        match run_tick::<F, E>(chain_id, run).await {
            Ok(n) => {
                consecutive_failures = 0;
                if n > 0 {
                    tracing::debug!(chain = %chain_id, blocks = n, "{} tick", run.module);
                }
            }
            Err(e) => {
                consecutive_failures += 1;
                tracing::warn!(
                    chain = %chain_id,
                    error = %e,
                    consecutive_failures,
                    "{} tick failed",
                    run.module
                );
            }
        }
        let factor = 1 + consecutive_failures.min(10);
        tokio::time::sleep(poll * factor).await;
    }
}
