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
pub const FRESHNESS_READER_VERSION: u32 = 1;

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
        .filter(|c| {
            c.module != MODULE_RAW
                && c.module != MODULE_DECODE
                && !c.module.starts_with("raw_backfill")
        })
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
                    observed_at.signed_duration_since(c.updated_at).num_seconds(),
                ),
                blocking_halt,
            }
        })
        .collect();

    // A halt recorded for a module with NO checkpoint row is not a row we may
    // drop. It means the module refused before it ever completed a height, which
    // is precisely the cold-start case an operator most needs to see, and
    // dropping it would make the worst state the most invisible one.
    for h in halts {
        if h.module == MODULE_RAW || h.module == MODULE_DECODE {
            continue;
        }
        if modules.iter().any(|m| m.module == h.module) {
            continue;
        }
        modules.push(ModuleFreshness {
            module: h.module.clone(),
            state: ModuleState::Halted,
            height: None,
            blocks_behind_decode: None,
            updated_at: None,
            seconds_since_update: None,
            blocking_halt: Some(h.clone()),
        });
    }

    modules.sort_by(|a, b| a.module.cmp(&b.module));

    let frontiers = Frontiers {
        raw,
        decode,
        decode_behind_raw: behind(raw, decode),
        raw_behind_chain: None,
    };

    let reads_as = reads_as(&frontiers, &modules);
    let not_covered = not_covered(&frontiers, &modules);

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
fn reads_as(frontiers: &Frontiers, modules: &[ModuleFreshness]) -> String {
    let mut s = String::new();

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
                 read `at_decode_frontier` below as a statement about this pipeline's \
                 own progress and nothing more. ",
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
                "DECODE IS {d} BLOCKS BEHIND RAW INGESTION, so every `at_decode_frontier` \
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
fn not_covered(frontiers: &Frontiers, modules: &[ModuleFreshness]) -> Vec<String> {
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
        out.push(
            "No decode frontier, so no module below can be placed relative to one; \
             their `state` reflects only their own halt records."
                .to_string(),
        );
    }

    out.push(
        "Modules with no checkpoint row AND no recorded halt do not appear at all. This \
         reader lists what has run or refused, and cannot list what was never started — \
         which chains SHOULD be running which modules is registry data, not checkpoint \
         data."
            .to_string(),
    );

    out
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
        assert!(r.reads_as.contains("100 BLOCKS BEHIND RAW"), "{}", r.reads_as);
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
        assert!(r.reads_as.contains("AHEAD of the decode frontier"), "{}", r.reads_as);
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
        let r = derive("polkadot", &base(), &[halt("gov", 5, 0, "referenda.New")], now());
        let g = r.modules.iter().find(|m| m.module == "gov").expect("gov listed");
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
        assert!(r.reads_as.contains("DECODE FRONTIER DOES NOT EXIST"), "{}", r.reads_as);
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
        assert_eq!(names, vec!["balances"], "a bounded backfill chunk is not a follower");
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
        assert!(r.behind_by_more_than(400).is_empty(), "the bound is exclusive");
    }
}
