//! Per-module freshness, as a PURE function with no database in it.
//!
//! ROADMAP's operational floor, item 2: *"the metric is FOLLOWER LAG PER MODULE
//! — not CPU. You need to know a module halted before a consumer tells you."*
//! And Phase 3.5's exit criterion wants the same numbers rendered on every
//! response whose data depends on a module.
//!
//! Those are two consumers of ONE rule, which is why this file has no database
//! in it: *"two implementations of one rule will diverge"* is a standing defect
//! class here (the addability rule had already drifted `<= 1` vs `== 1` before
//! anyone noticed). The operator surface and the per-response object call this,
//! or one of them will eventually say a module is fine while the other says it
//! stopped.
//!
//! ```text
//! rows from core.indexer_state + core.module_halts  ──(pure fn)──▶  report
//! ```
//!
//! # THE STACK, AND WHY THERE IS NEVER ONE NUMBER
//!
//! Three frontiers already live in `core.indexer_state`, and a module is behind
//! all of them independently:
//!
//! ```text
//!   the chain           ← NOT KNOWN HERE. No RPC in a read path; see `not_covered`.
//!     raw_blocks        ← how far raw ingestion has followed
//!       blocks          ← how far decode has got from raw
//!         balances …    ← how far this module has got from decode
//! ```
//!
//! A module sitting exactly on the decode frontier while decode is 40,000 blocks
//! behind raw is NOT current, and calling it current would be wrong in the
//! flattering direction — the direction this project checks first. So the state
//! word is `at_decode_frontier`, which cannot be misread as "up to date", and
//! the two lags are reported as separate fields that are never summed. Same
//! discipline as the coretime delta's two ratios (47.0% vs 34.69%), which exist
//! precisely so nobody reads them as one figure.
//!
//! # WHY THE LAG IS SIGNED
//!
//! A module can be AHEAD of the decode frontier. The shared runtime skips a
//! height the canonical store has not decoded and *still advances past it*, "so
//! a follower can never wedge on a hole". `saturating_sub` would render that as
//! `0` — indistinguishable from sitting on the frontier — which is the defect
//! slice 16 shipped one field over, where a decreasing session index would have
//! been reported as the rarest event on the chain. It is an `i64`, and negative
//! means the module ran past a decode gap.
//!
//! # WHAT IS NOT DECIDED HERE
//!
//! There is no staleness THRESHOLD in this file and no boolean derived from a
//! clock. `seconds_since_update` is reported; what counts as too long is the
//! caller's to state, because a default here would be a number nobody chose
//! sitting underneath an alert somebody trusts.

use chrono::{DateTime, Utc};

/// Bumped when the derivation below changes shape or meaning, so a stored or
/// cached report can be identified and rebuilt. Lineage, per Invariant 3.
///
/// `2` (Phase 3.5, the freshness route): every module row gained `declared`, and
/// [`ChainFreshness::with_declared`] made `never_run` reachable for the first
/// time. Both change the SHAPE and the module SET, so a v1 report and a v2
/// report over identical tables are not the same object.
pub const FRESHNESS_READER_VERSION: u32 = 2;

/// The `indexer_state.module` key of the raw-ingest frontier.
///
/// A SECOND DEFINITION of `ingest::live::MODULE_LIVE`, and it has to be: `api`
/// does not depend on `ingest` and must not start, because the dependency runs
/// generic ← protocol and this crate is on the generic side. Two definitions of
/// one string is exactly the shape that drifts, so the equality is pinned by a
/// test in `dotlens-node`, which is the one crate that can see both.
pub const MODULE_RAW: &str = "raw_blocks";
/// The `indexer_state.module` key of the decode frontier. A second definition of
/// `ingest::decode::MODULE_DECODE` — see [`MODULE_RAW`].
pub const MODULE_DECODE: &str = "blocks";

/// Is this `indexer_state.module` key a FRONTIER, or a bounded backfill chunk,
/// rather than a domain follower?
///
/// **It is used in BOTH directions and that is the whole reason it exists as a
/// function.** [`derive`] excludes these rows from the module list, because the
/// frontiers are the yardstick and not entries measured against themselves; and
/// [`ChainFreshness::with_declared`] must exclude the same NAMES from the seed's
/// list, because **every chain seed legitimately declares a module called
/// `blocks` and that is also [`MODULE_DECODE`]'s key.** Widening without this
/// filter would add `blocks` as a `never_run` domain module while the frontier
/// it actually names sits populated three fields above it, in the same payload.
///
/// Two hand-written copies of this predicate would drift the first time a fourth
/// reserved key appeared, and the drift would be silent because both sides would
/// still compile.
fn is_reserved_module(name: &str) -> bool {
    name == MODULE_RAW || name == MODULE_DECODE || name.starts_with("raw_backfill")
}

/// One row of `core.indexer_state`, as this reader needs it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct CheckpointRow {
    pub module: String,
    pub height: u64,
    pub updated_at: DateTime<Utc>,
}

/// One row of `core.module_halts` (migration 0028).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct HaltRow {
    pub module: String,
    pub height: u64,
    pub event_index: u32,
    pub event: String,
    pub reason: String,
    pub runtime_version: u64,
    pub mapper_version: u32,
    pub first_seen_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub seen_count: u64,
}

/// What a module's checkpoint says about it.
///
/// Four states, and the absent one is a state rather than a zero: a module that
/// has never run has no checkpoint row, and reporting that as "0 blocks behind"
/// is "we did not look" rendering as "there is nothing there".
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ModuleState {
    /// No checkpoint row at all. Never started, or started and never completed a
    /// single height. NOT the same as being up to date.
    NeverRun,
    /// A recorded mapper refusal sits above this module's checkpoint. A human is
    /// needed; nothing will move on its own.
    Halted,
    /// Behind the decode frontier with no halt recorded. A backfill, a cold
    /// start, or a stop nobody recorded — this reader cannot tell which from one
    /// observation, and `reads_as` says so.
    Behind,
    /// At (or past) the decode frontier. Deliberately not called "current": the
    /// decode frontier may itself be far behind raw, and `decode_behind_raw`
    /// is the field that says so.
    AtDecodeFrontier,
}

impl ModuleState {
    pub fn as_str(self) -> &'static str {
        match self {
            ModuleState::NeverRun => "never_run",
            ModuleState::Halted => "halted",
            ModuleState::Behind => "behind",
            ModuleState::AtDecodeFrontier => "at_decode_frontier",
        }
    }
}

/// One module's freshness.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ModuleFreshness {
    pub module: String,
    pub state: ModuleState,
    /// `None` when the module has never run — never `0`.
    pub height: Option<u64>,
    /// SIGNED: negative means the module advanced past a decode gap. `None` when
    /// either end of the subtraction is missing.
    pub blocks_behind_decode: Option<i64>,
    pub updated_at: Option<DateTime<Utc>>,
    /// Wall-clock age of the checkpoint. Reported, never thresholded here.
    pub seconds_since_update: Option<i64>,
    /// The halt that BLOCKS this module: the lowest recorded refusal above its
    /// checkpoint, never the most recently observed one. A module stuck at 150
    /// with a later refusal recorded at 320 is stopped by 150, and naming 320
    /// sends whoever reads it at 3am to the wrong block.
    pub blocking_halt: Option<HaltRow>,
    /// Does this chain's registry seed declare this module?
    ///
    /// **`None` means THE CALLER DID NOT SAY — it does not mean "no".** The
    /// operator CLI has no registry and leaves it null; the HTTP route has one
    /// and fills it via [`ChainFreshness::with_declared`]. Rendering "not
    /// declared" for "nobody told me" would be this project's second-most-
    /// repeated defect one field further out.
    ///
    /// `Some(false)` is a real and interesting state rather than a leftover: the
    /// module has a checkpoint or a halt on this chain, and the seed no longer
    /// lists it. That is a registry question, not a pipeline one.
    pub declared: Option<bool>,
}

/// The frontiers every module is measured against.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Frontiers {
    pub raw: Option<u64>,
    pub decode: Option<u64>,
    /// SIGNED, for the same reason as `blocks_behind_decode`: decode advances
    /// past raw gaps too.
    pub decode_behind_raw: Option<i64>,
    /// Always `None` in this reader, and present so its absence is a FIELD
    /// rather than a silence. See `not_covered`.
    pub raw_behind_chain: Option<i64>,
}

/// One chain's freshness report.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ChainFreshness {
    pub chain_id: String,
    pub reader_version: u32,
    pub observed_at: DateTime<Utc>,
    pub frontiers: Frontiers,
    /// Sorted by module name, so the memory and Postgres backends agree rather
    /// than differing by insertion order.
    pub modules: Vec<ModuleFreshness>,
    pub reads_as: String,
    pub not_covered: Vec<String>,
}

impl ChainFreshness {
    /// Modules a human must act on. Nothing moves on these without a mapper fix.
    pub fn halted(&self) -> Vec<&ModuleFreshness> {
        self.modules
            .iter()
            .filter(|m| m.state == ModuleState::Halted)
            .collect()
    }

    /// Modules further behind decode than `blocks` and not halted.
    ///
    /// The threshold is the CALLER's, deliberately — see the module header.
    pub fn behind_by_more_than(&self, blocks: u64) -> Vec<&ModuleFreshness> {
        self.modules
            .iter()
            .filter(|m| {
                m.state == ModuleState::Behind
                    && m.blocks_behind_decode.is_some_and(|d| d > blocks as i64)
            })
            .collect()
    }

    /// Widen this report with the module list the chain's REGISTRY SEED declares.
    ///
    /// **THIS IS WHAT MAKES [`ModuleState::NeverRun`] REACHABLE**, and until this
    /// slice nothing produced it. [`derive`] cannot: a module that has never
    /// started has no row in either table it reads, so from the checkpoints alone
    /// it is indistinguishable from a module nobody ever intended to run. Only
    /// the registry knows the difference — and this file has no database, no
    /// config and no registry in it on purpose, so the knowledge arrives here as
    /// an argument rather than as a lookup.
    ///
    /// **Widening is NOT the filtering the module header refuses.** A caller that
    /// FILTERS the checkpoint rows decides which modules exist and would report
    /// its own filter as the chain's silence. A caller that DECLARES adds rows
    /// this reader would otherwise have to leave out, and every added row says so
    /// in the payload: `state = never_run`, a null `height` (never `0`), and
    /// `declared = true`.
    ///
    /// **`reads_as` and `not_covered` are RECOMPUTED, not appended to.** Both are
    /// statements about the module list, and the module list has just changed. A
    /// `not_covered` line still saying *"this reader … cannot list what was never
    /// started"* beside a `never_run` row would be false about the payload it sits
    /// in — this project's oldest defect — and appending the correction after the
    /// claim rather than substituting it is the second-oldest.
    ///
    /// **Call it ONCE.** Chaining it leaves rows added by the first list present
    /// and stamped `declared: false` by the second, which the payload's own prose
    /// describes as "it has run or refused here under a seed that no longer lists
    /// it" — false of a row that never ran. `#[must_use]` catches the other half
    /// of the same mistake, discarding the result.
    #[must_use]
    pub fn with_declared(mut self, declared: &[String]) -> Self {
        for name in declared {
            // The frontiers are the yardstick, not modules — and a seed
            // declaring `blocks` means the decode frontier, which is already
            // reported as `frontiers.decode`. See `is_reserved_module`.
            if is_reserved_module(name) {
                continue;
            }
            if !self.modules.iter().any(|m| &m.module == name) {
                self.modules.push(ModuleFreshness {
                    module: name.clone(),
                    state: ModuleState::NeverRun,
                    height: None,
                    blocks_behind_decode: None,
                    updated_at: None,
                    seconds_since_update: None,
                    blocking_halt: None,
                    declared: Some(true),
                });
            }
        }
        // Set on EVERY row, including the ones just pushed (which are declared by
        // construction) and the ones that were already here (which may not be).
        for m in &mut self.modules {
            m.declared = Some(declared.iter().any(|d| d == &m.module));
        }
        self.modules.sort_by(|a, b| a.module.cmp(&b.module));
        self.reads_as = reads_as(&self.frontiers, &self.modules, Some(declared));
        self.not_covered = not_covered(&self.frontiers, &self.modules, Some(declared));
        self
    }

    /// Modules the chain's seed declares that have never produced a height.
    ///
    /// **It sits beside [`Self::halted`] and [`Self::behind_by_more_than`]
    /// because without it a fully-unstarted chain reads GREEN to anything that
    /// alerts.** Both of those return empty for a chain whose every declared
    /// module is `never_run`, and empty from both is exactly what a monitor
    /// treats as healthy — the state an operator most needs would have been the
    /// only one with no accessor.
    ///
    /// Always empty unless [`Self::with_declared`] has been called, because
    /// nothing else can produce the state.
    pub fn never_run(&self) -> Vec<&ModuleFreshness> {
        self.modules
            .iter()
            .filter(|m| m.state == ModuleState::NeverRun)
            .collect()
    }
}

/// Subtract two frontiers without lying about either end.
///
/// `None` on either side means the answer is unknown, and unknown must not
/// render as `0` — that is this project's second-most-repeated defect and it has
/// been caught at field level four times.
fn behind(ahead: Option<u64>, behind_of: Option<u64>) -> Option<i64> {
    match (ahead, behind_of) {
        (Some(a), Some(b)) => Some(a as i64 - b as i64),
        _ => None,
    }
}

/// The whole derivation.
///
/// `checkpoints` is every `core.indexer_state` row for the chain (including the
/// two frontier rows) and `halts` every `core.module_halts` row for it. Both are
/// taken whole rather than filtered by the caller, because a caller that filters
/// decides which modules exist, and this reader would then report a module's
/// absence as its own blind spot.
pub fn derive(
    chain_id: &str,
    checkpoints: &[CheckpointRow],
    halts: &[HaltRow],
    observed_at: DateTime<Utc>,
) -> ChainFreshness {
    let at = |m: &str| checkpoints.iter().find(|c| c.module == m);
    let raw = at(MODULE_RAW).map(|c| c.height);
    let decode = at(MODULE_DECODE).map(|c| c.height);

    // Domain modules only: the two frontiers are the yardstick, not entries
    // measured against themselves. `raw_backfill` and its per-chunk `:a-b`
    // siblings are excluded too — a backfill chunk is a bounded job with its own
    // end, so "behind the decode frontier" is not a defect for it and rendering
    // it beside the followers would put a permanent red row on the surface.
    let mut modules: Vec<ModuleFreshness> = checkpoints
        .iter()
        .filter(|c| !is_reserved_module(&c.module))
        .map(|c| {
            let blocking_halt = blocking_halt_for(&c.module, c.height, halts);
            let state = if blocking_halt.is_some() {
                ModuleState::Halted
            } else if behind(decode, Some(c.height)).is_some_and(|d| d > 0) {
                ModuleState::Behind
            } else {
                ModuleState::AtDecodeFrontier
            };
            ModuleFreshness {
                module: c.module.clone(),
                state,
                height: Some(c.height),
                blocks_behind_decode: behind(decode, Some(c.height)),
                updated_at: Some(c.updated_at),
                seconds_since_update: Some(
                    observed_at
                        .signed_duration_since(c.updated_at)
                        .num_seconds(),
                ),
                blocking_halt,
                // This reader has no registry. `with_declared` fills it, and
                // null means "nobody told me" rather than "no".
                declared: None,
            }
        })
        .collect();

    // A halt recorded for a module with NO checkpoint row is not a row we may
    // drop. It means the module refused before it ever completed a height, which
    // is precisely the cold-start case an operator most needs to see, and
    // dropping it would make the worst state the most invisible one.
    for h in halts {
        // The SAME predicate `derive`'s checkpoint filter and `with_declared`
        // use. It was hand-rolled here and was two-thirds of it — a
        // `raw_backfill:100-200` refusal would have been pushed as a permanently
        // red follower row, which is the exact thing the checkpoint filter above
        // excludes bounded chunks to prevent.
        if is_reserved_module(&h.module) {
            continue;
        }
        if modules.iter().any(|m| m.module == h.module) {
            continue;
        }
        // THE LOWEST refusal for this module, not the first one the caller
        // happened to hand us. The checkpointed path uses `min_by_key` and this
        // one took input order, so a module with refusals at 320 and 150 would
        // name 320 here and 150 there — "sends whoever reads it at 3am to the
        // wrong block", on the very field whose doc says so. It was correct only
        // by the Postgres backend's `order by`, which made a file that says it
        // has no database in it depend on one.
        let blocking = halts
            .iter()
            .filter(|c| c.module == h.module)
            .min_by_key(|c| (c.height, c.event_index))
            .unwrap_or(h);
        modules.push(ModuleFreshness {
            module: h.module.clone(),
            state: ModuleState::Halted,
            height: None,
            blocks_behind_decode: None,
            updated_at: None,
            seconds_since_update: None,
            blocking_halt: Some(blocking.clone()),
            declared: None,
        });
    }

    modules.sort_by(|a, b| a.module.cmp(&b.module));

    let frontiers = Frontiers {
        raw,
        decode,
        decode_behind_raw: behind(raw, decode),
        raw_behind_chain: None,
    };

    // `None`: this reader was told nothing about what SHOULD be running.
    // `with_declared` recomputes both with the seed's list when it is.
    let reads_as = reads_as(&frontiers, &modules, None);
    let not_covered = not_covered(&frontiers, &modules, None);

    ChainFreshness {
        chain_id: chain_id.to_string(),
        reader_version: FRESHNESS_READER_VERSION,
        observed_at,
        frontiers,
        modules,
        reads_as,
        not_covered,
    }
}

/// The lowest recorded refusal ABOVE the checkpoint.
///
/// Ordering by height and taking the first is the whole rule. A reader taking
/// the most recent by `last_seen_at` would name a halt the module cannot even
/// have reached yet.
fn blocking_halt_for(module: &str, checkpoint: u64, halts: &[HaltRow]) -> Option<HaltRow> {
    halts
        .iter()
        .filter(|h| h.module == module && h.height > checkpoint)
        .min_by_key(|h| (h.height, h.event_index))
        .cloned()
}

/// What this report does and does not say, in the payload beside it.
///
/// Built from the SAME values the payload is built from — never from a parallel
/// summary — because a `reads_as` line false about the object beside it is this
/// project's oldest and most repeated defect, at seven recurrences by slice 10.
fn reads_as(
    frontiers: &Frontiers,
    modules: &[ModuleFreshness],
    declared: Option<&[String]>,
) -> String {
    let mut s = String::new();

    // AN EMPTY MODULE LIST IS THE ONE ROW THIS CANNOT DRAW, and a blank space
    // under healthy frontiers reads as "all clear" — the container-level form of
    // this project's second-most-repeated defect, and the same one the operator
    // CLI already guards. Said FIRST, because everything after it would
    // otherwise be a reassuring paragraph about nothing.
    if modules.is_empty() {
        s.push_str(match declared {
            Some(d) if d.iter().all(|m| is_reserved_module(m)) => {
                "THIS CHAIN'S SEED DECLARES NO DOMAIN MODULE — only frontiers. Nothing below \
                 is missing; there is nothing to be behind. ",
            }
            Some(_) => {
                "NO MODULE HAS A CHECKPOINT OR A RECORDED HALT ON THIS CHAIN, though its seed \
                 declares some — read the `never_run` rows below as 'not started', never as \
                 'nothing to report'. ",
            }
            None => {
                "NO MODULE HAS A CHECKPOINT OR A RECORDED HALT ON THIS CHAIN. This reader was \
                 not told what SHOULD be running here, so an empty list below is the absence \
                 of an OBSERVATION and not the absence of a problem. ",
            }
        });
    }

    match (frontiers.raw, frontiers.decode) {
        (None, None) => {
            s.push_str(
                "NOTHING HAS RUN ON THIS CHAIN. Neither the raw-ingest nor the decode \
                 frontier has a checkpoint, so every module below is reported against \
                 no yardstick at all — read this as 'not started', never as 'idle'. ",
            );
        }
        (_, None) => {
            s.push_str(
                "THE DECODE FRONTIER DOES NOT EXIST YET, so `blocks_behind_decode` is \
                 null for every module rather than 0 — nothing has been decoded for \
                 anything to be behind. ",
            );
        }
        (None, Some(_)) => {
            s.push_str(
                "Every module's lag is measured against the DECODE frontier. THE \
                 RAW-INGEST FRONTIER HAS NO CHECKPOINT, so `decode_behind_raw` is null \
                 and nothing here can say how far decode itself is from the chain — \
                 read ANY `at_decode_frontier` row below as a statement about this \
                 pipeline's own progress and nothing more. ",
            );
        }
        _ => {
            s.push_str(
                "Every module's lag is measured against the DECODE frontier, which is \
                 itself measured against the raw-ingest frontier. The two are reported \
                 separately and MUST NOT be added: a module at the decode frontier is \
                 not up to date if decode is far behind raw, which is why the state \
                 word is `at_decode_frontier` and never `current`. ",
            );
        }
    }

    if let Some(d) = frontiers.decode_behind_raw {
        if d > 0 {
            s.push_str(&format!(
                "DECODE IS {d} BLOCKS BEHIND RAW INGESTION, so ANY `at_decode_frontier` row \
                 below is at least that far from the chain. "
            ));
        } else if d < 0 {
            s.push_str(
                "Decode reads AHEAD of raw ingestion, which means it advanced past a raw \
                 gap rather than that raw is complete to that height. ",
            );
        }
    }

    let halted: Vec<&str> = modules
        .iter()
        .filter(|m| m.state == ModuleState::Halted)
        .map(|m| m.module.as_str())
        .collect();
    if halted.is_empty() {
        s.push_str(
            "No module has a recorded refusal above its checkpoint. That is not a \
             guarantee that none has stopped — a worker killed by the operating system \
             records nothing, and shows here as `behind` with an old `updated_at`. ",
        );
    } else {
        s.push_str(&format!(
            "HALTED, and a human is needed — nothing below will move on its own: {}. \
             Each carries the lowest refusal above its checkpoint, which is the one \
             actually blocking it; later refusals for the same module are recorded but \
             unreachable until this one is fixed. ",
            halted.join(", ")
        ));
    }

    let ahead: Vec<&str> = modules
        .iter()
        .filter(|m| m.blocks_behind_decode.is_some_and(|d| d < 0))
        .map(|m| m.module.as_str())
        .collect();
    if !ahead.is_empty() {
        s.push_str(&format!(
            "AHEAD of the decode frontier, which is expected rather than wrong: {}. The \
             shared runtime skips an undecoded height and still advances, so a follower \
             cannot wedge on a hole; those heights get their facts when decode gap-fill \
             and a later `*-range` re-run revisit them. ",
            ahead.join(", ")
        ));
    }

    let never: Vec<&str> = modules
        .iter()
        .filter(|m| m.state == ModuleState::NeverRun)
        .map(|m| m.module.as_str())
        .collect();
    if !never.is_empty() {
        s.push_str(&format!(
            "NEVER RUN, reported as null rather than as zero blocks behind: {}. ",
            never.join(", ")
        ));
    }

    s.trim_end().to_string()
}

/// What this reader cannot answer, named rather than left silent.
///
/// `declared` is the chain's seed list when the caller had one. It is the LIST
/// and not a boolean, because two of these entries have to say different things
/// depending on what the list actually contains — a review found both of them
/// shipping a claim that was false about the very payload they sat in:
///
/// - the `blocks`/`raw_blocks` entry explained an omission that had not occurred
///   when the seed declared no reserved name at all;
/// - the no-decode-frontier entry said every module's `state` "reflects only
///   their own halt records", which stops being true the moment widening can put
///   a `never_run` row there — whose state reflects the REGISTRY.
///
/// A report over a chain whose seed declares nothing and a report nobody told
/// anything are also different facts, and a boolean collapses them.
fn not_covered(
    frontiers: &Frontiers,
    modules: &[ModuleFreshness],
    declared: Option<&[String]>,
) -> Vec<String> {
    let mut out = vec![
        "raw_behind_chain: THE CHAIN'S OWN HEAD IS NOT READ. Every number here is \
         measured against dotlens's own raw-ingest frontier, so a raw follower that \
         stopped an hour ago makes the whole stack look internally consistent and \
         current. Closing this needs the live follower to record the head it observed; \
         it is not read here because a read path must not make an RPC call."
            .to_string(),
        "BEHIND does not distinguish 'catching up' from 'stopped without recording a \
         halt'. One observation cannot: both are a checkpoint that is not at the \
         frontier. `seconds_since_update` is the only signal, and it is reported as a \
         number rather than thresholded, because the threshold is the operator's."
            .to_string(),
    ];

    if modules.iter().any(|m| m.seconds_since_update.is_some()) {
        out.push(
            "`updated_at` only moves when a checkpoint ADVANCES (the upsert is guarded \
             `last_height < excluded.last_height`), so a module that is running \
             normally with nothing new to process ages exactly like one that stopped."
                .to_string(),
        );
    }

    if frontiers.decode.is_none() {
        out.push(if declared.is_some() {
            // `never_run` rows come from the REGISTRY, not from halt records, so
            // the un-widened wording is false the moment widening is possible.
            "No decode frontier, so nothing below is placed relative to one. Each `state` \
             is either `never_run` — which this reader knows from the chain's seed and not \
             from either table it reads — or that module's own halt record."
                .to_string()
        } else {
            "No decode frontier, so no module below can be placed relative to one; \
             their `state` reflects only their own halt records."
                .to_string()
        });
    }

    out.push(
        "A refusal recorded at or BELOW a module's checkpoint is not shown: the module has \
         since passed it. The rows are kept — this reader reports only what still blocks, \
         so a module that refused repeatedly last week and recovered reads exactly like one \
         that never refused."
            .to_string(),
    );

    match declared {
        Some(declared) => {
            out.push(
                "Every module this chain's registry seed declares appears below: one that has \
                 never started is `never_run` with a NULL height rather than absent. A row \
                 carrying `declared: false` is the other case — it has run or refused here \
                 under a seed that no longer lists it, which is a registry question and not a \
                 pipeline one."
                    .to_string(),
            );
            out.push(
                "Declaring a module is not the same as it being able to answer. This reader \
                 reports how far a module's CHECKPOINT has got and says nothing about whether \
                 a mapper exists for every pallet that module will meet — an unmapped variant \
                 is a future halt, not a state visible here before it fires."
                    .to_string(),
            );
            // ONLY when the seed actually declares a reserved name. Emitting it
            // unconditionally explained an omission that had not occurred, and
            // pointed at `frontiers.decode` on chains where that field is null.
            let reserved: Vec<&str> = declared
                .iter()
                .filter(|d| is_reserved_module(d))
                .map(|d| d.as_str())
                .collect();
            if !reserved.is_empty() {
                out.push(format!(
                    "This chain's seed declares {}, which {} NOT missing from the list below \
                     — {} the raw-ingest and decode FRONTIERS, reported under `frontiers` \
                     (currently raw {}, decode {}). The frontiers are what every module here \
                     is measured against, so listing them as modules would measure them \
                     against themselves. A declared name beginning `raw_backfill` is excluded \
                     the same way and for the same reason: a bounded chunk is not a follower.",
                    reserved.join(" and "),
                    if reserved.len() == 1 { "is" } else { "are" },
                    if reserved.len() == 1 { "it is one of" } else { "they are" },
                    opt_height(frontiers.raw),
                    opt_height(frontiers.decode),
                ));
            }
        }
        None => out.push(
            "Modules with no checkpoint row AND no recorded halt do not appear at all, and \
             every `declared` field below is null for the same reason. This reader lists \
             what has run or refused; it was not told what SHOULD be running, which is \
             registry data rather than checkpoint data."
                .to_string(),
        ),
    }

    out
}

/// A height for prose, where absence must read as absence.
fn opt_height(h: Option<u64>) -> String {
    match h {
        Some(v) => v.to_string(),
        None => "not started".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    fn now() -> DateTime<Utc> {
        ts("2026-08-24T12:00:00Z")
    }

    fn cp(module: &str, height: u64, at: &str) -> CheckpointRow {
        CheckpointRow {
            module: module.to_string(),
            height,
            updated_at: ts(at),
        }
    }

    fn halt(module: &str, height: u64, event_index: u32, event: &str) -> HaltRow {
        HaltRow {
            module: module.to_string(),
            height,
            event_index,
            event: event.to_string(),
            reason: "unknown balances event — mapper update required".to_string(),
            runtime_version: 2_003_002,
            mapper_version: 4,
            first_seen_at: ts("2026-08-24T11:00:00Z"),
            last_seen_at: ts("2026-08-24T11:59:00Z"),
            seen_count: 41,
        }
    }

    fn base() -> Vec<CheckpointRow> {
        vec![
            cp(MODULE_RAW, 1000, "2026-08-24T11:59:50Z"),
            cp(MODULE_DECODE, 900, "2026-08-24T11:59:40Z"),
        ]
    }

    #[test]
    fn the_two_lags_are_separate_and_the_state_word_never_says_current() {
        let mut cps = base();
        cps.push(cp("balances", 900, "2026-08-24T11:59:30Z"));
        let r = derive("polkadot-asset-hub", &cps, &[], now());

        let b = &r.modules[0];
        assert_eq!(b.state, ModuleState::AtDecodeFrontier);
        assert_eq!(b.blocks_behind_decode, Some(0));
        // The module is level with decode and decode is 100 behind raw. The two
        // must be readable as two numbers.
        assert_eq!(r.frontiers.decode_behind_raw, Some(100));
        // The word on the wire is the load-bearing part: `current` would be read
        // as up to date by someone who never opens `decode_behind_raw`.
        assert_eq!(b.state.as_str(), "at_decode_frontier");
        assert!(r.reads_as.contains("never `current`"), "{}", r.reads_as);
        assert!(r.reads_as.contains("MUST NOT be added"), "{}", r.reads_as);
        assert!(
            r.reads_as.contains("100 BLOCKS BEHIND RAW"),
            "{}",
            r.reads_as
        );
    }

    #[test]
    fn a_module_ahead_of_decode_reads_negative_not_zero() {
        // The runtime skips an undecoded height and still advances, so this is a
        // real state. saturating_sub would render it as 0 — the flattering
        // reading, and the defect slice 16 shipped one field over.
        let mut cps = base();
        cps.push(cp("balances", 950, "2026-08-24T11:59:30Z"));
        let r = derive("polkadot", &cps, &[], now());
        assert_eq!(r.modules[0].blocks_behind_decode, Some(-50));
        assert_eq!(r.modules[0].state, ModuleState::AtDecodeFrontier);
        assert!(
            r.reads_as.contains("AHEAD of the decode frontier"),
            "{}",
            r.reads_as
        );
    }

    #[test]
    fn the_blocking_halt_is_the_lowest_above_the_checkpoint_not_the_newest() {
        let mut cps = base();
        cps.push(cp("balances", 100, "2026-08-24T10:00:00Z"));
        // Recorded out of order on purpose, and the later one observed more
        // recently — a max(last_seen_at) reader would pick 320.
        let mut later = halt("balances", 320, 0, "balances.Later");
        later.last_seen_at = ts("2026-08-24T11:59:59Z");
        let halts = vec![later, halt("balances", 150, 7, "balances.BurnedDebt")];

        let r = derive("polkadot", &cps, &halts, now());
        let b = &r.modules[0];
        assert_eq!(b.state, ModuleState::Halted);
        let h = b.blocking_halt.as_ref().expect("blocking halt");
        assert_eq!(h.height, 150, "the module cannot have reached 320");
        assert_eq!(h.event, "balances.BurnedDebt");
    }

    #[test]
    fn a_halt_at_or_below_the_checkpoint_is_resolved_and_not_stored_as_such() {
        // The mapper was fixed and a re-run advanced past it. Nothing cleared the
        // row; the comparison is what makes it stop counting.
        let mut cps = base();
        cps.push(cp("balances", 200, "2026-08-24T11:59:30Z"));
        let r = derive("polkadot", &cps, &[halt("balances", 150, 7, "x")], now());
        assert_eq!(r.modules[0].state, ModuleState::Behind);
        assert!(r.modules[0].blocking_halt.is_none());
    }

    #[test]
    fn a_halt_with_no_checkpoint_still_appears_rather_than_being_dropped() {
        // Refused before completing a single height — the cold-start case, and
        // the one an operator most needs to see.
        let r = derive(
            "polkadot",
            &base(),
            &[halt("gov", 5, 0, "referenda.New")],
            now(),
        );
        let g = r
            .modules
            .iter()
            .find(|m| m.module == "gov")
            .expect("gov listed");
        assert_eq!(g.state, ModuleState::Halted);
        assert_eq!(g.height, None, "never ran is null, never 0");
        assert_eq!(g.blocks_behind_decode, None);
    }

    #[test]
    fn an_absent_decode_frontier_nulls_the_lag_rather_than_reading_zero() {
        let cps = vec![
            cp(MODULE_RAW, 1000, "2026-08-24T11:59:50Z"),
            cp("balances", 400, "2026-08-24T11:00:00Z"),
        ];
        let r = derive("polkadot", &cps, &[], now());
        assert_eq!(r.frontiers.decode, None);
        assert_eq!(r.modules[0].blocks_behind_decode, None);
        assert_eq!(
            r.modules[0].state,
            ModuleState::AtDecodeFrontier,
            "with no frontier there is nothing to be behind, and inventing one would be worse"
        );
        assert!(
            r.reads_as.contains("DECODE FRONTIER DOES NOT EXIST"),
            "{}",
            r.reads_as
        );
    }

    #[test]
    fn nothing_run_at_all_says_not_started_rather_than_idle() {
        let r = derive("polkadot", &[], &[], now());
        assert!(r.modules.is_empty());
        assert!(r.reads_as.contains("NOTHING HAS RUN"), "{}", r.reads_as);
        assert_eq!(r.frontiers.raw, None);
        assert_eq!(r.frontiers.decode, None);
    }

    #[test]
    fn the_frontiers_and_backfill_chunks_are_not_listed_as_modules() {
        let mut cps = base();
        cps.push(cp("raw_backfill", 10, "2026-08-24T11:00:00Z"));
        cps.push(cp("raw_backfill:100-200", 150, "2026-08-24T11:00:00Z"));
        cps.push(cp("balances", 900, "2026-08-24T11:59:30Z"));
        let r = derive("polkadot", &cps, &[], now());
        let names: Vec<&str> = r.modules.iter().map(|m| m.module.as_str()).collect();
        assert_eq!(
            names,
            vec!["balances"],
            "a bounded backfill chunk is not a follower"
        );
    }

    #[test]
    fn modules_are_sorted_so_the_two_backends_cannot_disagree_by_insertion_order() {
        let mut cps = base();
        cps.push(cp("xcm", 900, "2026-08-24T11:59:30Z"));
        cps.push(cp("balances", 900, "2026-08-24T11:59:30Z"));
        cps.push(cp("gov", 900, "2026-08-24T11:59:30Z"));
        let r = derive("polkadot", &cps, &[], now());
        let names: Vec<&str> = r.modules.iter().map(|m| m.module.as_str()).collect();
        assert_eq!(names, vec!["balances", "gov", "xcm"]);
    }

    #[test]
    fn no_recorded_halt_is_never_reported_as_proof_that_nothing_stopped() {
        let mut cps = base();
        cps.push(cp("balances", 400, "2026-08-24T09:00:00Z"));
        let r = derive("polkadot", &cps, &[], now());
        assert_eq!(r.modules[0].state, ModuleState::Behind);
        assert!(
            r.reads_as.contains("not a guarantee"),
            "silence about halts must not read as health: {}",
            r.reads_as
        );
        assert_eq!(r.modules[0].seconds_since_update, Some(10_800));
    }

    #[test]
    fn the_chain_head_is_a_null_field_and_a_named_gap_rather_than_a_silence() {
        let mut cps = base();
        cps.push(cp("balances", 900, "2026-08-24T11:59:30Z"));
        let r = derive("polkadot", &cps, &[], now());
        assert_eq!(r.frontiers.raw_behind_chain, None);
        assert!(
            r.not_covered.iter().any(|s| s.contains("raw_behind_chain")),
            "the absent top of the stack must be named: {:?}",
            r.not_covered
        );
        // Every not_covered line must name something the payload actually has,
        // or omits — the oldest defect class here is a line false about the
        // object beside it.
        assert!(r.not_covered.iter().any(|s| s.contains("updated_at")));
    }

    #[test]
    fn the_helpers_select_what_an_alert_acts_on() {
        let mut cps = base();
        cps.push(cp("balances", 100, "2026-08-24T10:00:00Z"));
        cps.push(cp("gov", 500, "2026-08-24T11:59:00Z"));
        cps.push(cp("xcm", 900, "2026-08-24T11:59:00Z"));
        let r = derive("polkadot", &cps, &[halt("balances", 150, 7, "x")], now());

        assert_eq!(r.halted().len(), 1);
        assert_eq!(r.halted()[0].module, "balances");
        // gov is 400 behind; xcm is level. A halted module is NOT also counted
        // as behind — it needs a different action.
        let behind = r.behind_by_more_than(100);
        assert_eq!(behind.len(), 1);
        assert_eq!(behind[0].module, "gov");
        assert!(
            r.behind_by_more_than(400).is_empty(),
            "the bound is exclusive"
        );
    }

    // ------------------------------------------------- with_declared (the route)

    fn declared(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn a_declared_module_that_never_started_is_never_run_and_not_absent() {
        let mut cps = base();
        cps.push(cp("balances", 900, "2026-08-24T11:59:30Z"));
        let r = derive("polkadot", &cps, &[], now())
            .with_declared(&declared(&["balances", "gov", "xcm"]));

        let names: Vec<&str> = r.modules.iter().map(|m| m.module.as_str()).collect();
        assert_eq!(names, vec!["balances", "gov", "xcm"], "declared rows are added");

        let gov = r.modules.iter().find(|m| m.module == "gov").expect("gov");
        assert_eq!(gov.state, ModuleState::NeverRun);
        // NULL, never 0 — the whole reason this state exists as a state.
        assert_eq!(gov.height, None);
        assert_eq!(gov.blocks_behind_decode, None);
        assert_eq!(gov.updated_at, None);
        assert_eq!(gov.seconds_since_update, None);
        assert_eq!(gov.declared, Some(true));
    }

    /// Without a registry this state is UNREACHABLE, which is why nothing
    /// produced it before this slice. The assertion is the pair, not the arm.
    #[test]
    fn never_run_is_unreachable_without_the_declared_list_and_reachable_with_it() {
        let cps = base();
        let bare = derive("polkadot", &cps, &[], now());
        assert!(
            !bare
                .modules
                .iter()
                .any(|m| m.state == ModuleState::NeverRun),
            "derive alone cannot know a module was supposed to run"
        );
        let widened = bare.with_declared(&declared(&["gov"]));
        assert_eq!(widened.modules[0].state, ModuleState::NeverRun);
    }

    #[test]
    fn a_module_that_ran_under_a_seed_no_longer_listing_it_reports_declared_false() {
        let mut cps = base();
        cps.push(cp("balances", 900, "2026-08-24T11:59:30Z"));
        // the seed lists gov and NOT balances
        let r = derive("polkadot", &cps, &[], now()).with_declared(&declared(&["gov"]));

        let bal = r.modules.iter().find(|m| m.module == "balances").unwrap();
        assert_eq!(
            bal.declared,
            Some(false),
            "it ran here; the registry no longer says it should"
        );
        // and it is still REPORTED — an undeclared module is not filtered away,
        // which would be the caller deciding which modules exist.
        assert_eq!(bal.state, ModuleState::AtDecodeFrontier);
        assert!(
            r.not_covered.iter().any(|s| s.contains("declared: false")),
            "the state must be named where it can occur: {:?}",
            r.not_covered
        );
    }

    #[test]
    fn declared_is_null_before_widening_because_null_is_not_no() {
        let mut cps = base();
        cps.push(cp("balances", 900, "2026-08-24T11:59:30Z"));
        let r = derive("polkadot", &cps, &[], now());
        assert_eq!(r.modules[0].declared, None);
        assert!(
            r.not_covered
                .iter()
                .any(|s| s.contains("was not told what SHOULD be running")),
            "{:?}",
            r.not_covered
        );
    }

    /// A3: a prose fix is done only when the old text is GONE. The un-widened
    /// line says this reader "cannot list what was never started" — beside a
    /// `never_run` row that would be false about the payload it sits in.
    #[test]
    fn widening_substitutes_the_coverage_line_rather_than_appending_to_it() {
        let cps = base();
        let bare = derive("polkadot", &cps, &[], now());
        assert!(bare
            .not_covered
            .iter()
            .any(|s| s.contains("was not told what SHOULD be running")));

        let widened = bare.with_declared(&declared(&["gov"]));
        assert!(
            !widened
                .not_covered
                .iter()
                .any(|s| s.contains("was not told what SHOULD be running")),
            "the superseded claim must be GONE, not sitting beside its correction: {:?}",
            widened.not_covered
        );
        assert!(widened
            .not_covered
            .iter()
            .any(|s| s.contains("never_run` with a NULL height")));
    }

    #[test]
    fn widening_recomputes_reads_as_so_it_names_the_never_run_modules() {
        let cps = base();
        let r = derive("polkadot", &cps, &[], now()).with_declared(&declared(&["gov", "xcm"]));
        assert!(
            r.reads_as.contains("NEVER RUN"),
            "the arm was dead until this slice: {}",
            r.reads_as
        );
        assert!(r.reads_as.contains("gov"), "{}", r.reads_as);
        assert!(r.reads_as.contains("xcm"), "{}", r.reads_as);
    }

    #[test]
    fn widening_keeps_a_halt_visible_and_does_not_overwrite_its_state() {
        let mut cps = base();
        cps.push(cp("balances", 100, "2026-08-24T10:00:00Z"));
        let r = derive("polkadot", &cps, &[halt("balances", 150, 7, "x")], now())
            .with_declared(&declared(&["balances", "gov"]));

        let bal = r.modules.iter().find(|m| m.module == "balances").unwrap();
        assert_eq!(bal.state, ModuleState::Halted, "a declared module can be halted");
        assert_eq!(bal.declared, Some(true));
        assert!(bal.blocking_halt.is_some());
        assert_eq!(r.halted().len(), 1, "the helper still selects it");
    }

    #[test]
    fn widening_with_an_empty_seed_list_is_not_the_same_as_not_widening() {
        let mut cps = base();
        cps.push(cp("balances", 900, "2026-08-24T11:59:30Z"));
        let r = derive("polkadot", &cps, &[], now()).with_declared(&[]);
        // "this chain declares nothing" is a STATEMENT; it is not "nobody told me".
        assert_eq!(r.modules[0].declared, Some(false));
        assert!(r
            .not_covered
            .iter()
            .any(|s| s.contains("never_run` with a NULL height")));
    }

    /// EVERY chain seed declares a module called `blocks`, and that string is
    /// also `MODULE_DECODE`. Without the reserved-name filter, widening reports
    /// the decode frontier as a never-run domain module in the same payload that
    /// shows it populated — a line false about the object beside it.
    #[test]
    fn a_seed_declaring_blocks_does_not_resurrect_the_decode_frontier_as_a_module() {
        let mut cps = base();
        cps.push(cp("balances", 900, "2026-08-24T11:59:30Z"));
        let r = derive("polkadot", &cps, &[], now()).with_declared(&declared(&[
            "blocks",
            "raw_blocks",
            "extrinsics",
            "balances",
        ]));

        let names: Vec<&str> = r.modules.iter().map(|m| m.module.as_str()).collect();
        assert_eq!(
            names,
            vec!["balances", "extrinsics"],
            "the frontiers are the yardstick, not modules"
        );
        assert_eq!(r.frontiers.decode, Some(900), "and it is still reported here");
        assert!(
            r.not_covered.iter().any(|s| s.contains("FRONTIERS")),
            "the omission must be stated where a reader would otherwise call it a gap: {:?}",
            r.not_covered
        );

        // AND THE PAIR, without which the assertion above cannot fail: a seed
        // that declares no reserved name must NOT carry the explanation, because
        // there is no omission to explain.
        let plain =
            derive("polkadot", &cps, &[], now()).with_declared(&declared(&["balances", "gov"]));
        assert!(
            !plain.not_covered.iter().any(|s| s.contains("FRONTIERS")),
            "nothing was omitted, so nothing may be explained away: {:?}",
            plain.not_covered
        );
    }

    /// The un-widened wording said every state "reflects only their own halt
    /// records". Widening makes that false — a `never_run` state comes from the
    /// registry — and `hydration` (declared modules, nothing indexed) is the
    /// shipped payload that would have carried the false line.
    #[test]
    fn the_no_decode_frontier_line_is_true_in_both_the_widened_and_bare_cases() {
        let bare = derive("polkadot", &[], &[], now());
        assert!(bare
            .not_covered
            .iter()
            .any(|s| s.contains("reflects only their own halt records")));

        let widened = derive("polkadot", &[], &[], now()).with_declared(&declared(&["gov"]));
        assert!(
            !widened
                .not_covered
                .iter()
                .any(|s| s.contains("reflects only their own halt records")),
            "there is a never_run row below, whose state came from the seed: {:?}",
            widened.not_covered
        );
        assert!(widened
            .not_covered
            .iter()
            .any(|s| s.contains("knows from the chain's seed")));
    }

    /// A bounded backfill chunk is not a follower, and a halt recorded against
    /// one must not become a permanently red module row.
    #[test]
    fn a_halt_against_a_backfill_chunk_or_a_frontier_is_not_a_module() {
        let r = derive(
            "polkadot",
            &base(),
            &[
                halt("raw_backfill:100-200", 150, 1, "x"),
                halt(MODULE_RAW, 150, 1, "x"),
                halt("balances", 950, 1, "x"),
            ],
            now(),
        );
        let names: Vec<&str> = r.modules.iter().map(|m| m.module.as_str()).collect();
        assert_eq!(names, vec!["balances"], "{names:?}");
    }

    /// The checkpointed path takes the LOWEST refusal; the no-checkpoint path
    /// took whatever the caller listed first, so the two disagreed and the
    /// answer depended on Postgres's `order by` — in a file whose header says it
    /// has no database in it.
    #[test]
    fn a_halt_with_no_checkpoint_still_names_the_lowest_refusal_not_the_first_listed() {
        let r = derive(
            "polkadot",
            &base(),
            // deliberately NOT in height order
            &[halt("gov", 320, 0, "later"), halt("gov", 150, 4, "earlier")],
            now(),
        );
        let gov = r.modules.iter().find(|m| m.module == "gov").expect("gov");
        assert_eq!(gov.state, ModuleState::Halted);
        assert_eq!(gov.height, None, "it refused before completing a height");
        let blocking = gov.blocking_halt.as_ref().expect("blocking");
        assert_eq!(
            (blocking.height, blocking.event_index),
            (150, 4),
            "naming 320 sends whoever reads it at 3am to the wrong block"
        );
    }

    /// Without this selector a fully-unstarted chain returns empty from BOTH
    /// existing accessors, which is what a monitor reads as healthy.
    #[test]
    fn never_run_has_a_selector_so_an_unstarted_chain_does_not_read_green() {
        let r = derive("polkadot", &base(), &[], now())
            .with_declared(&declared(&["gov", "xcm", "balances"]));
        assert!(r.halted().is_empty());
        assert!(r.behind_by_more_than(0).is_empty());
        assert_eq!(
            r.never_run().len(),
            3,
            "the state an operator most needs must be selectable"
        );
    }

    #[test]
    fn an_empty_module_list_says_nothing_has_run_rather_than_leaving_a_blank() {
        // frontiers are healthy, and NO domain module exists
        let bare = derive("polkadot", &base(), &[], now());
        assert!(bare.modules.is_empty());
        assert!(
            bare.reads_as.contains("NO MODULE HAS A CHECKPOINT"),
            "a blank list under healthy frontiers reads as all-clear: {}",
            bare.reads_as
        );

        // a seed declaring ONLY frontier names is a different fact again
        let only_frontiers =
            derive("polkadot", &base(), &[], now()).with_declared(&declared(&["blocks"]));
        assert!(only_frontiers.modules.is_empty());
        assert!(
            only_frontiers
                .reads_as
                .contains("DECLARES NO DOMAIN MODULE"),
            "{}",
            only_frontiers.reads_as
        );
    }

    #[test]
    fn widening_is_idempotent_and_stays_sorted() {
        let mut cps = base();
        cps.push(cp("xcm", 900, "2026-08-24T11:59:30Z"));
        let once = derive("polkadot", &cps, &[], now()).with_declared(&declared(&["gov", "xcm"]));
        let twice = once.clone().with_declared(&declared(&["gov", "xcm"]));
        assert_eq!(once, twice, "re-widening the same list is a no-op");
        let names: Vec<&str> = twice.modules.iter().map(|m| m.module.as_str()).collect();
        assert_eq!(names, vec!["gov", "xcm"]);
    }
}
