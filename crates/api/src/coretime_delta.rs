//! The coretime DELTA: entitlement purchased vs occupancy realized (Phase 3,
//! slice 14).
//!
//! ROADMAP's coretime bullet is the whole reason this file exists: "the
//! comparison — entitlement purchased vs. occupancy realized — is the claim
//! nobody else can make. NEITHER HALF ALONE IS THE PRODUCT." 0024 shipped the
//! occupancy half, 0025 shipped the entitlement half and said in its own header
//! that it shipped no reader, and this is the join.
//!
//! ---------------------------------------------------------------------------
//! IT IS A PURE FUNCTION, AND THAT IS A DESIGN DECISION WITH TWO REASONS
//! ---------------------------------------------------------------------------
//! (1) The delta is never materialised — both sides carry lineage, so a stored
//! copy would be the one without it (0026's header lists the four previous
//! refusals of the same shape). Computed per request, it needs a home, and a
//! function with no database in it is the smallest honest one.
//!
//! (2) EVERY NUMBER THIS FILE PRODUCES WAS MEASURED BY HAND IN SQL BEFORE IT WAS
//! COMPUTED IN RUST. Slice 13's verification ran the join manually and recorded
//! the answers; the tests at the bottom of this file assert those exact figures
//! against a fixture shaped like the measured window. A pure function is what
//! makes that possible offline — a reader that could only be checked against a
//! live database would have its central claim tested by nothing.
//!
//! The figures, from slice 13's verification, over relay 32613537–32614536:
//!
//! ```text
//!     AGREE 47 / DISAGREE 0 / 34,690 of 34,690 candidates attributed
//!     57 Task-entitled cores, 43 Pool; 60.86% of TASK-ENTITLED slots used
//!     22,310 entitled slots bought or reserved and unused
//!     10 idle Task cores, every index >= 11 (waste is market-side)
//!     43 + 10 = 53 = slice 11's own "53 cores producing nothing"
//!     cores 53 and 13 are Task-entitled bulk using 1.0% and 2.9%
//! ```
//!
//! **`first_core = 11` WAS AN INFERENCE WHEN THIS FILE WAS WRITTEN, AND SLICE
//! 14's VERIFICATION MEASURED IT.** Slice 13 never captured `Broker.SaleInfo` —
//! its own known-gaps list says so — so the 11 came from `Broker.Reservations`
//! holding 11 entries at a DIFFERENT moment, and "all ten idle cores are above
//! `first_core`" was a conclusion drawn across two readings never taken
//! together. `sync-broker-config` has now read it: **`first_core = 11` at
//! coretime #4928280, spec 2003002**, and the two agree. So the claim is a dated
//! reading rather than an inference, which is the whole reason 0026 adds the
//! column — and it is why the market/reserved split is ABSENT rather than
//! assumed when no reading exists.
//!
//! ---------------------------------------------------------------------------
//! THE THREE RULES THIS FILE REFUSES TO BREAK
//! ---------------------------------------------------------------------------
//! **A CORE WITH NO ASSIGNMENT AT OR BEFORE THE ANCHOR IS `unknown`, NEVER
//! `idle`.** `Broker.CoreAssigned` fires only at sale boundaries — slice 13 had
//! to hunt the sale governing slice 11's window ~157,000 coretime blocks (~335,000
//! relay blocks) back from
//! it — so "our index does not reach the previous sale" is the ordinary case for
//! a shallow backfill, and it is not "nobody bought this core". Rendering it as
//! idle would invent waste that did not happen, which is the direction
//! `adapter_substrate::broker`'s own halt message calls the worst one to be
//! wrong in, because it is the direction that flatters the product. 0025 states
//! the rule and nothing enforced it until this file.
//!
//! **POOL CORES ARE NOT WASTE.** A `Pool` core was sold and donated to the
//! instantaneous market; its time is unattributable to any purchaser BY
//! CONSTRUCTION, not by our ignorance. 43 of 100 cores in the measured sale, so
//! folding them in would invent 43,000 wasted slots nobody bought. They are
//! counted, reported, and kept out of every denominator.
//!
//! **THE KEY IS (core, task, relay-block window), NEVER THE REGION.** A renewal
//! MOVES the core index — measured at coretime 4919882, where para 3428 renewed
//! five cores and every index changed (35→43, 36→44, 37→45, 40→46, 41→47) — so a
//! core index identifies an entitlement only within one region and the durable
//! identity is the task.
//!
//! ---------------------------------------------------------------------------
//! WHEN THE WASTE FIGURE REFUSES TO BE COMPUTED
//! ---------------------------------------------------------------------------
//! The ATTRIBUTION (agree / disagree / attributed candidates) is always served:
//! it is a count over rows we hold, and "34,690 of 34,690 attributed" is itself
//! the coverage statement.
//!
//! The WASTE figures are gated, because each of the EIGHT gates below is a
//! condition under which the number would be wrong in the flattering direction:
//!
//!   1. no indexed blocks — 0/0 is undefined, not zero (slice 11's rule);
//!   2. any core with no known entitlement — an unknown core might be task-
//!      entitled and idle, i.e. pure waste we would be omitting;
//!   3. the entitlement changed inside the window — the slots before the change
//!      were entitled to something else, so one figure averages two questions;
//!   4. the two denominators disagree — 0025 says plainly that the reader "must
//!      refuse rather than pick one";
//!   5. a core carries a MIXED or FRACTIONAL entitlement — an interlaced core is
//!      entitled to a FRACTION of itself and this reader does not divide by
//!      `parts`, because the mask's pattern does not cross to the relay and a
//!      ratio built on the bit count alone would look exact while not being;
//!   6. a core carries more inclusions than the window holds blocks, which
//!      0024's partial unique index forbids — the occupancy side is not
//!      self-consistent and dividing by it would launder the contradiction;
//!   7. no `num_cores` reading is on record — and THIS is what makes gate 2 able
//!      to fire at all: without a declared core count the universe is only what
//!      we HOLD, so an idle task-entitled core we never indexed is never even
//!      enumerated and gate 2 stays silent while the denominator is truncated;
//!   8. no core is task-entitled at all — 0/0 one more time, and the one arm
//!      that would otherwise slip through, since a ratio of 0.0 there reads as
//!      "nothing bought was used" when nothing was bought as a task.
//!
//! All eight are false on the measured data, which is why the drill produces a
//! served waste block — and every one has an offline test, because a gate that
//! never fires is a gate nobody has checked.

use std::collections::{BTreeMap, BTreeSet};

/// `PartsOf57600` for a whole core. A smaller total means the entitlement covers
/// a FRACTION of the core, and this reader refuses to divide by it — see the
/// module header.
pub const PARTS_WHOLE_CORE: u32 = 57_600;

/// How many declared cores this reader will enumerate.
///
/// `num_cores` read 100 on the measured chain, and the elastic-scaling ceiling
/// is nowhere near this. It exists because the value comes from a database row
/// and drives an allocation on a public GET — a bad reading must degrade into a
/// reported contradiction, not into a million-row `cores` array.
pub const MAX_DECLARED_CORES: u32 = 10_000;

pub const KIND_IDLE: &str = "idle";
pub const KIND_POOL: &str = "pool";
pub const KIND_TASK: &str = "task";

// ------------------------------------------------------------------- inputs

/// One (core, para) pair's inclusion count over the window.
///
/// PER (core, para) AND NOT PER CORE, deliberately. A core that carried two
/// paras in one window would have part of its occupancy attributable and part
/// not, and a per-core total could not express that. Core→para rotation is
/// UNEXERCISED on live data (all 47 used cores served exactly one para in slice
/// 11's window), so this is the shape the reader is designed against rather than
/// one anyone has seen — the same standing as `CandidateTimedOut`'s arity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OccupancyCell {
    pub core_index: u32,
    pub para_id: u32,
    pub included_blocks: u64,
}

/// One row of `coretime.core_assignments` — one `(CoreAssignment, PartsOf57600)`
/// pair from the announcement governing this core at the anchor.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct EntitlementRow {
    pub core_index: u32,
    pub assignment_index: u32,
    /// `CoreAssigned.when` — the RELAY block this assignment takes effect from,
    /// stated by the chain. NOT the coretime block's relay parent: slice 13's
    /// verification found its own VERIFY doc had conflated the two (29053200 vs
    /// 29053193), and only the first is a multiple of 80.
    pub relay_block: u64,
    /// idle | pool | task
    pub kind: String,
    pub task_id: Option<u32>,
    pub parts: u32,
    pub runtime_version: u64,
    pub mapper_version: u32,
}

/// Everything the delta is computed from. No database, no clock, no registry.
#[derive(Debug, Clone, Copy)]
pub struct DeltaInput<'a> {
    /// Occupancy over the relay window, `kind = 'included'` only.
    pub occupancy: &'a [OccupancyCell],
    /// The governing assignment per core at `anchor_relay_block`, expanded.
    pub entitlement: &'a [EntitlementRow],
    /// Relay blocks we actually hold in the window — the slot denominator, and
    /// never the requested span (slice 11's rule: a window with gaps must not be
    /// divided by its nominal width).
    pub blocks_indexed: u64,
    /// The window's first relay block. An assignment taking effect ABOVE this
    /// governed only part of the window.
    pub window_from: u64,
    /// The relay height entitlement was read as of — the window's last block.
    pub anchor_relay_block: u64,
    /// The relay's own `Configuration.ActiveConfig.scheduler_params.num_cores`.
    pub relay_num_cores: Option<u32>,
    /// The broker's own `Status.core_count`. Read on a DIFFERENT CHAIN at a
    /// height on a DIFFERENT NUMBER LINE — the two are compared by VALUE and
    /// never ordered in time.
    pub broker_core_count: Option<u32>,
    /// `SaleInfo.first_core`: cores below it are reserved system cores, cores at
    /// or above it are the bulk market. NULL when sales never started or no
    /// reading was taken, and the reader says "cannot separate" rather than
    /// assuming 0 — see 0026.
    pub first_core: Option<u32>,
}

// ------------------------------------------------------------------ outputs

/// What one core's entitlement resolved to, as a kind rather than as a row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EntitlementKind {
    /// No assignment at or before the anchor. NOT idle — see the header.
    Unknown,
    Idle,
    Pool,
    /// Every assignment on this core is a `Task`, and together they cover the
    /// whole core.
    Task,
    /// Assignments of more than one kind on one core (task + pool, say). Legal
    /// on the wire, never observed, and excluded from every denominator because
    /// the core is entitled to a FRACTION of itself.
    Mixed,
    /// One kind, but the parts do not sum to a whole core.
    Fractional,
}

impl EntitlementKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            EntitlementKind::Unknown => "unknown",
            EntitlementKind::Idle => "idle",
            EntitlementKind::Pool => "pool",
            EntitlementKind::Task => "task",
            EntitlementKind::Mixed => "mixed",
            EntitlementKind::Fractional => "fractional",
        }
    }
}

/// The verdict on one core: what it was entitled to do, and what it did.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CoreVerdict {
    /// Task-entitled, and every candidate it produced belongs to an entitled
    /// task. The headline case: 47 of 100 in the measured window.
    Agree,
    /// Task-entitled, and some or all of its occupancy belongs to a para it was
    /// not entitled to serve. ZERO live instances — expressible and unobserved.
    Disagree,
    /// Task-entitled and produced nothing at all. The waste, core by core.
    EntitledUnused,
    /// Pool-entitled and produced blocks. NOT waste and NOT attributable: the
    /// core's time went to whoever bought instantaneous coretime. Zero live
    /// instances (no pool core produced anything in the measured window).
    PoolUsed,
    /// Pool-entitled and produced nothing. 43 of 100, and not waste.
    PoolUnused,
    /// Entitled to `Idle` and produced blocks — a contradiction the runtime
    /// produced, worth its own bucket rather than folding into disagreement.
    IdleUsed,
    /// Entitled to `Idle` and produced nothing. Genuinely unsold.
    IdleUnused,
    /// Produced blocks with NO entitlement on record. Our index does not reach
    /// the sale that governs this core — not "nobody bought it".
    UnknownUsed,
    /// No entitlement on record and no occupancy. Says nothing either way.
    UnknownIdle,
    /// Mixed or fractional entitlement, with or without occupancy. Counted,
    /// never divided.
    Unattributable,
}

impl CoreVerdict {
    pub fn as_str(&self) -> &'static str {
        match self {
            CoreVerdict::Agree => "agree",
            CoreVerdict::Disagree => "disagree",
            CoreVerdict::EntitledUnused => "entitled_unused",
            CoreVerdict::PoolUsed => "pool_used",
            CoreVerdict::PoolUnused => "pool_unused",
            CoreVerdict::IdleUsed => "idle_used",
            CoreVerdict::IdleUnused => "idle_unused",
            CoreVerdict::UnknownUsed => "unknown_used",
            CoreVerdict::UnknownIdle => "unknown_idle",
            CoreVerdict::Unattributable => "unattributable",
        }
    }
}

/// One row of the per-core table the endpoint renders.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct CoreDelta {
    pub core_index: u32,
    pub entitlement_kind: &'static str,
    /// Every task this core is entitled to serve, ascending. More than one means
    /// interlacing, which has never been observed.
    pub entitled_tasks: Vec<u32>,
    /// The RELAY block this core's governing assignment took effect from. NULL
    /// when the entitlement is unknown — and the DISTANCE between this and the
    /// window is what says whether the entitlement is contemporary with the
    /// occupancy or 3.5M blocks away from it.
    pub governing_relay_block: Option<u64>,
    /// `PartsOf57600`, summed. 57600 is a whole core.
    pub parts: Option<u32>,
    pub included_blocks: u64,
    /// Every para this core actually carried, ascending.
    pub paras: Vec<u32>,
    /// Candidates belonging to an entitled task.
    pub attributed_blocks: u64,
    /// `included_blocks / blocks_indexed`. NULL without blocks to divide by.
    ///
    /// THIS FIELD IS WHERE SLICE 11'S ON-DEMAND INFERENCE DIES: cores 53 and 13
    /// read 1.0% and 2.9% here while being Task-entitled bulk with a full mask,
    /// i.e. the most under-used purchases on the chain rather than on-demand
    /// traffic.
    pub used_ratio: Option<f64>,
    pub verdict: &'static str,
    /// `core_index >= first_core` — the bulk market rather than a reserved
    /// system core. NULL when no `first_core` reading is on record.
    pub market_side: Option<bool>,
}

/// A check that can come back `contradicted` in public — slice 3's doctrine,
/// where a journey's own consistency checks are part of the payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Check {
    Ok,
    Contradicted,
    /// Not checkable from what is on record. Never rendered as `ok`.
    Unknown,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct DeltaReport {
    // ---- entitlement census (always served) ----
    pub anchor_relay_block: u64,
    pub cores_seen: u64,
    pub cores_with_entitlement: u64,
    pub cores_without_entitlement: u64,
    pub unknown_cores: Vec<u32>,
    pub task_entitled_cores: u64,
    pub pool_cores: u64,
    pub idle_cores: u64,
    pub unattributable_cores: u64,
    /// Distinct relay blocks the governing assignments took effect at,
    /// ascending. One entry means one sale governs the whole window.
    pub governing_relay_blocks: Vec<u64>,
    /// Cores whose governing assignment took effect INSIDE the window, i.e.
    /// after `window_from`. Non-empty means the entitlement moved under the
    /// occupancy and a single figure would average two questions.
    pub changed_in_window: Vec<CoreChange>,
    pub entitlement_stable_across_window: bool,

    // ---- attribution (always served) ----
    pub agree_cores: u64,
    pub disagree_cores: u64,
    pub pool_cores_with_occupancy: u64,
    pub idle_cores_with_occupancy: u64,
    pub unknown_cores_with_occupancy: u64,
    pub candidates_total: u64,
    pub attributed_candidates: u64,
    pub unattributed_candidates: u64,
    /// Why each unattributed candidate is unattributed. Sums to
    /// `unattributed_candidates`.
    pub unattributed_by_reason: UnattributedBreakdown,

    // ---- waste (gated; see the module header) ----
    pub waste: Option<Waste>,
    /// Which gate refused, when `waste` is null. Empty when it is served.
    pub waste_withheld_because: Vec<&'static str>,

    // ---- denominators ----
    pub relay_num_cores: Option<u32>,
    pub broker_core_count: Option<u32>,
    pub first_core: Option<u32>,

    // ---- checks ----
    pub checks: Checks,

    pub cores: Vec<CoreDelta>,
    pub reads_as: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct CoreChange {
    pub core_index: u32,
    pub relay_block: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize)]
pub struct UnattributedBreakdown {
    /// On a task-entitled core, produced for a para it was not entitled to.
    pub wrong_task: u64,
    /// On a pool core — unattributable BY CONSTRUCTION, not by our ignorance.
    pub pool: u64,
    /// On a core entitled to Idle.
    pub idle: u64,
    /// On a core with no entitlement on record.
    pub unknown: u64,
    /// On a mixed or fractional core.
    pub unattributable: u64,
}

impl UnattributedBreakdown {
    pub fn total(&self) -> u64 {
        self.wrong_task + self.pool + self.idle + self.unknown + self.unattributable
    }
}

/// The number ROADMAP promised, and the one slice 11 could not compute: how much
/// of what was BOUGHT was USED.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
pub struct Waste {
    /// `task_entitled_cores * blocks_indexed`. THE DENOMINATOR SLICE 11 DID NOT
    /// HAVE: it could only say "34.69% of ALL slots", which silently adds
    /// "nobody bought it" to "somebody bought it and did not use it".
    pub task_entitled_slots: u64,
    pub used_by_entitled_task: u64,
    /// Occupancy on a task-entitled core belonging to some other para. Zero live
    /// instances.
    pub used_by_another_task: u64,
    /// Bought or reserved and never used. 22,310 in the measured window.
    pub unused: u64,
    /// `used_by_entitled_task / task_entitled_slots`. A window with no
    /// task-entitled core at all withholds the whole block rather than serving
    /// 0/0 here, so this is always a real division.
    pub used_ratio: f64,
    /// Task-entitled cores that produced nothing at all. 10 in the measured
    /// window, and every one of them at index >= first_core.
    pub idle_task_cores: u64,
    pub idle_task_cores_reserved: Option<u64>,
    pub idle_task_cores_market: Option<u64>,
    /// `pool_cores * blocks_indexed`. **NOT WASTE** — reported so nobody has to
    /// derive it, and named so nobody adds it to the figure above.
    pub pool_slots: u64,
    /// The identity slice 13 verified: pool + idle-task + idle-entitled +
    /// unknown-idle + unattributable-idle == cores producing nothing.
    /// 43 + 10 = 53 = slice 11's own count.
    pub cores_producing_nothing: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct Checks {
    /// The two chains' core counts. 0025: "if they ever disagree, one of the two
    /// halves is being counted against the wrong denominator and the delta
    /// reader must refuse rather than pick one."
    pub denominators_agree: Check,
    /// Every core carrying occupancy or entitlement is below the declared core
    /// count. A core at or above it is a contradiction the runtime produced.
    pub cores_within_denominator: Check,
    /// The cores that produced nothing are fully accounted for by the buckets.
    ///
    /// STATED HONESTLY: THIS IS A TRIPWIRE, NOT A MEASUREMENT. Every verdict a
    /// non-producing core can carry is in the bucket list today, so it reads
    /// `ok` by construction and cannot currently be contradicted by data. It
    /// exists to fail the day a verdict arm is added without a bucket — which is
    /// how a core would silently drop out of "producing nothing" while still
    /// being counted in the denominator. Recorded as a tripwire because this
    /// project counts a green assertion that cannot fail as a defect.
    pub cores_account_for_the_denominator: Check,
    /// No core's occupancy exceeds the blocks we hold — one candidate per core
    /// per block is what 0024's partial unique index enforces.
    pub occupancy_within_slots: Check,
    /// One sale governs the whole window.
    pub entitlement_stable: Check,
}

// ------------------------------------------------------------------ the work

/// Join entitlement to occupancy over one relay-block window.
pub fn compute(input: DeltaInput<'_>) -> DeltaReport {
    let have_blocks = input.blocks_indexed > 0;

    // ---- fold the entitlement rows into one verdict per core ----
    struct Ent {
        kinds: BTreeSet<String>,
        tasks: BTreeSet<u32>,
        parts: u32,
        relay_block: u64,
    }
    let mut ents: BTreeMap<u32, Ent> = BTreeMap::new();
    for r in input.entitlement {
        let e = ents.entry(r.core_index).or_insert_with(|| Ent {
            kinds: BTreeSet::new(),
            tasks: BTreeSet::new(),
            parts: 0,
            relay_block: r.relay_block,
        });
        e.kinds.insert(r.kind.clone());

        if let Some(t) = r.task_id {
            e.tasks.insert(t);
        }
        e.parts = e.parts.saturating_add(r.parts);
        // One announcement per core, so every row shares its `when`. Taking the
        // max rather than asserting it keeps a re-announcement at the same
        // height from silently picking the older number.
        e.relay_block = e.relay_block.max(r.relay_block);
    }

    let kind_of = |e: &Ent| -> EntitlementKind {
        if e.kinds.len() != 1 {
            return EntitlementKind::Mixed;
        }
        // Length checked above, so `first` is the only kind present.
        let only = e.kinds.iter().next().map(String::as_str).unwrap_or("");
        if e.parts != PARTS_WHOLE_CORE {
            return EntitlementKind::Fractional;
        }
        match only {
            KIND_TASK => EntitlementKind::Task,
            KIND_POOL => EntitlementKind::Pool,
            KIND_IDLE => EntitlementKind::Idle,
            // A fourth kind cannot reach here: `adapter_substrate::broker` halts
            // on an unknown `CoreAssignment` variant and 0025's CHECK constrains
            // the column to three values. Treated as unattributable rather than
            // guessed at, so a schema change downstream degrades into a counted
            // bucket instead of a silent reclassification.
            _ => EntitlementKind::Mixed,
        }
    };

    // ---- fold occupancy per core ----
    let mut occ: BTreeMap<u32, BTreeMap<u32, u64>> = BTreeMap::new();
    for c in input.occupancy {
        *occ.entry(c.core_index)
            .or_default()
            .entry(c.para_id)
            .or_default() += c.included_blocks;
    }

    // ---- the universe of cores: everything declared, plus anything observed ----
    //
    // BOTH HALVES MATTER. Enumerating only what we observed would hide a
    // task-entitled core that produced nothing, which is the waste. Enumerating
    // only [0, num_cores) would hide a core the runtime scheduled work onto
    // beyond the declared count, which is the stale-denominator contradiction
    // slice 11 built a detector for.
    let mut universe: BTreeSet<u32> = BTreeSet::new();
    if let Some(n) = input.relay_num_cores {
        // BOUNDED, because `n` is a value read out of a database on a public
        // GET and this loop renders one JSON object per core. Slice 10's
        // `?to=u64::MAX` overflow was the same class one input source removed.
        // A reading beyond the bound is a contradiction the response reports
        // (`cores_within_denominator`) rather than an allocation.
        universe.extend(0..n.min(MAX_DECLARED_CORES));
    }
    universe.extend(occ.keys().copied());
    universe.extend(ents.keys().copied());

    let mut cores: Vec<CoreDelta> = Vec::with_capacity(universe.len());
    let mut unknown_cores: Vec<u32> = Vec::new();
    let mut changed_in_window: Vec<CoreChange> = Vec::new();
    let mut governing: BTreeSet<u64> = BTreeSet::new();

    let (mut task_cores, mut pool_cores, mut idle_cores, mut unattributable_cores) = (0u64, 0, 0, 0);
    let (mut agree_cores, mut disagree_cores) = (0u64, 0u64);
    let (mut pool_used, mut idle_used, mut unknown_used) = (0u64, 0u64, 0u64);
    let mut candidates_total = 0u64;
    let mut attributed = 0u64;
    let mut unattributed = UnattributedBreakdown::default();
    let mut task_slots_used = 0u64;
    let mut task_slots_used_by_others = 0u64;
    let mut idle_task_cores = 0u64;
    let (mut idle_task_reserved, mut idle_task_market) = (0u64, 0u64);
    let mut cores_producing_nothing = 0u64;
    let mut accounted_producing_nothing = 0u64;
    let mut occupancy_exceeds_slots = false;

    for core in universe {
        let paras_map = occ.get(&core);
        let included: u64 = paras_map.map(|m| m.values().sum()).unwrap_or(0);
        let paras: Vec<u32> = paras_map.map(|m| m.keys().copied().collect()).unwrap_or_default();
        candidates_total += included;
        if have_blocks && included > input.blocks_indexed {
            occupancy_exceeds_slots = true;
        }

        let ent = ents.get(&core);
        let kind = ent.map(|e| kind_of(e)).unwrap_or(EntitlementKind::Unknown);
        let tasks: Vec<u32> = ent.map(|e| e.tasks.iter().copied().collect()).unwrap_or_default();
        let governing_relay_block = ent.map(|e| e.relay_block);
        if let Some(b) = governing_relay_block {
            governing.insert(b);
            // An assignment taking effect AT `window_from` governs the whole
            // window; one taking effect above it does not. Strictly greater.
            if b > input.window_from {
                changed_in_window.push(CoreChange { core_index: core, relay_block: b });
            }
        }

        let entitled: BTreeSet<u32> = tasks.iter().copied().collect();
        let mut core_attributed = 0u64;
        let mut core_wrong = 0u64;
        if let Some(m) = paras_map {
            for (para, n) in m {
                if kind == EntitlementKind::Task && entitled.contains(para) {
                    core_attributed += n;
                } else {
                    core_wrong += n;
                }
            }
        }

        let market_side = input.first_core.map(|f| core >= f);
        let verdict = match (kind, included > 0) {
            (EntitlementKind::Task, true) => {
                task_cores += 1;
                task_slots_used += core_attributed;
                task_slots_used_by_others += core_wrong;
                attributed += core_attributed;
                unattributed.wrong_task += core_wrong;
                if core_wrong == 0 {
                    agree_cores += 1;
                    CoreVerdict::Agree
                } else {
                    disagree_cores += 1;
                    CoreVerdict::Disagree
                }
            }
            (EntitlementKind::Task, false) => {
                task_cores += 1;
                idle_task_cores += 1;
                match market_side {
                    Some(true) => idle_task_market += 1,
                    Some(false) => idle_task_reserved += 1,
                    None => {}
                }
                CoreVerdict::EntitledUnused
            }
            (EntitlementKind::Pool, true) => {
                pool_cores += 1;
                pool_used += 1;
                unattributed.pool += core_wrong;
                CoreVerdict::PoolUsed
            }
            (EntitlementKind::Pool, false) => {
                pool_cores += 1;
                CoreVerdict::PoolUnused
            }
            (EntitlementKind::Idle, true) => {
                idle_cores += 1;
                idle_used += 1;
                unattributed.idle += core_wrong;
                CoreVerdict::IdleUsed
            }
            (EntitlementKind::Idle, false) => {
                idle_cores += 1;
                CoreVerdict::IdleUnused
            }
            (EntitlementKind::Unknown, true) => {
                unknown_cores.push(core);
                unknown_used += 1;
                unattributed.unknown += core_wrong;
                CoreVerdict::UnknownUsed
            }
            (EntitlementKind::Unknown, false) => {
                unknown_cores.push(core);
                CoreVerdict::UnknownIdle
            }
            (EntitlementKind::Mixed | EntitlementKind::Fractional, used) => {
                unattributable_cores += 1;
                if used {
                    unattributed.unattributable += core_wrong;
                }
                CoreVerdict::Unattributable
            }
        };

        if included == 0 {
            cores_producing_nothing += 1;
            // Every bucket a non-producing core can be in. The identity below is
            // this sum against `cores_producing_nothing`, and it can only fail
            // if a verdict arm is added without a bucket.
            if matches!(
                verdict,
                CoreVerdict::PoolUnused
                    | CoreVerdict::EntitledUnused
                    | CoreVerdict::IdleUnused
                    | CoreVerdict::UnknownIdle
                    | CoreVerdict::Unattributable
            ) {
                accounted_producing_nothing += 1;
            }
        }

        cores.push(CoreDelta {
            core_index: core,
            entitlement_kind: kind.as_str(),
            entitled_tasks: tasks,
            governing_relay_block,
            parts: ent.map(|e| e.parts),
            included_blocks: included,
            paras,
            attributed_blocks: core_attributed,
            used_ratio: have_blocks.then(|| included as f64 / input.blocks_indexed as f64),
            verdict: verdict.as_str(),
            market_side,
        });
    }

    let cores_seen = cores.len() as u64;
    let cores_without_entitlement = unknown_cores.len() as u64;
    let cores_with_entitlement = cores_seen - cores_without_entitlement;
    let entitlement_stable = changed_in_window.is_empty();

    // ---- the checks, each able to come back contradicted in public ----
    let denominators_agree = match (input.relay_num_cores, input.broker_core_count) {
        (Some(a), Some(b)) if a == b => Check::Ok,
        (Some(_), Some(_)) => Check::Contradicted,
        _ => Check::Unknown,
    };
    let max_core = cores.iter().map(|c| c.core_index).max();
    let cores_within_denominator = match (max_core, input.relay_num_cores) {
        (Some(m), Some(n)) if m >= n => Check::Contradicted,
        (Some(_), Some(_)) => Check::Ok,
        _ => Check::Unknown,
    };
    let checks = Checks {
        denominators_agree,
        cores_within_denominator,
        cores_account_for_the_denominator: if accounted_producing_nothing == cores_producing_nothing
        {
            Check::Ok
        } else {
            Check::Contradicted
        },
        occupancy_within_slots: if !have_blocks {
            Check::Unknown
        } else if occupancy_exceeds_slots {
            Check::Contradicted
        } else {
            Check::Ok
        },
        entitlement_stable: if entitlement_stable { Check::Ok } else { Check::Contradicted },
    };

    // ---- the gates ----
    let mut withheld: Vec<&'static str> = Vec::new();
    if !have_blocks {
        withheld.push(
            "this window holds no indexed relay blocks, so there are no core-block slots to fill \
             and every ratio would be 0/0 — undefined, not zero. Backfill and decode the range, \
             then run `coretime-range`",
        );
    }
    if cores_without_entitlement > 0 {
        withheld.push(
            "at least one core has NO assignment at or before the anchor, so our index does not \
             reach the sale that governs it. An unknown core may be task-entitled and idle, i.e. \
             waste this figure would silently omit — and omitting waste is the direction that \
             flatters the product. Widen the entitlement backfill until every core resolves",
        );
    }
    if !entitlement_stable {
        withheld.push(
            "the entitlement CHANGED inside this window (see `changed_in_window`), so the slots \
             before the change were entitled to something else and one figure would average two \
             questions. Ask for a window that one sale governs",
        );
    }
    if denominators_agree == Check::Contradicted {
        withheld.push(
            "the relay's `num_cores` and the broker's `Status.core_count` DISAGREE, so one of the \
             two halves is being counted against the wrong denominator. Migration 0025 says the \
             reader must refuse rather than pick one",
        );
    }
    if unattributable_cores > 0 {
        withheld.push(
            "at least one core carries a MIXED or FRACTIONAL entitlement — interlacing. Such a \
             core is entitled to a fraction of itself, and this reader does not divide by `parts` \
             because the mask's PATTERN never crosses to the relay: a ratio built on the bit count \
             alone would look exact while not being",
        );
    }
    if checks.occupancy_within_slots == Check::Contradicted {
        withheld.push(
            "a core carries more inclusions than the window holds blocks, which 0024's partial \
             unique index forbids — one candidate per core per block. The occupancy side is not \
             self-consistent and dividing by it would launder the contradiction",
        );
    }
    if input.relay_num_cores.is_none() {
        // THE GATE THAT MAKES THE UNKNOWN-CORE GATE ABLE TO FIRE AT ALL, and it
        // was missing. Without a declared core count the universe is only what
        // we HOLD, so a core that exists on chain, was task-entitled and sat
        // idle — and whose sale we never indexed — is not enumerated, is
        // therefore never `unknown`, and the gate above stays silent while the
        // denominator is quietly truncated. That is waste omitted, which this
        // file's own header calls the direction that flatters the product.
        //
        // It is reachable on a DB where `broker-range` and `coretime-range` ran
        // and `sync-core-config` did not — and `/occupancy` refuses to serve a
        // ratio in exactly that state, so without this the two endpoints would
        // disagree about whether the denominator exists.
        withheld.push(
            "no `num_cores` reading is on record for the relay, so the set of cores that EXIST \
             cannot be enumerated and the unknown-core gate above cannot fire: a task-entitled \
             core that sat idle and whose sale we never indexed would be silently omitted from \
             the denominator, which under-states waste. Run `sync-core-config`",
        );
    }
    if task_cores == 0 {
        // 0/0 ONE MORE TIME, and it is the one arm that would otherwise slip
        // through: every other gate above is about a number being wrong, and
        // this one is about there being no denominator at all. A window where
        // every core is pooled has no entitled slots, and a ratio of 0.0 there
        // would read as "nothing bought was used".
        withheld.push(
            "no core in this window is task-entitled, so there are no entitled slots to divide by. \
             That is not a used ratio of zero — there is nothing to have used",
        );
    }

    let waste = withheld.is_empty().then(|| {
        let slots = task_cores * input.blocks_indexed;
        Waste {
            task_entitled_slots: slots,
            used_by_entitled_task: task_slots_used,
            used_by_another_task: task_slots_used_by_others,
            unused: slots
                .saturating_sub(task_slots_used)
                .saturating_sub(task_slots_used_by_others),
            // `slots > 0` is guaranteed by the two gates above (task_cores > 0
            // and blocks_indexed > 0), so this is a real division and not a
            // guarded one pretending to be.
            used_ratio: task_slots_used as f64 / slots as f64,
            idle_task_cores,
            idle_task_cores_reserved: input.first_core.map(|_| idle_task_reserved),
            idle_task_cores_market: input.first_core.map(|_| idle_task_market),
            pool_slots: pool_cores * input.blocks_indexed,
            cores_producing_nothing,
        }
    });

    let reads_as = reads_as(
        &withheld,
        waste.as_ref(),
        task_cores,
        pool_cores,
        attributed,
        candidates_total,
        input.blocks_indexed,
    );

    DeltaReport {
        anchor_relay_block: input.anchor_relay_block,
        cores_seen,
        cores_with_entitlement,
        cores_without_entitlement,
        unknown_cores,
        task_entitled_cores: task_cores,
        pool_cores,
        idle_cores,
        unattributable_cores,
        governing_relay_blocks: governing.into_iter().collect(),
        changed_in_window,
        entitlement_stable_across_window: entitlement_stable,
        agree_cores,
        disagree_cores,
        pool_cores_with_occupancy: pool_used,
        idle_cores_with_occupancy: idle_used,
        unknown_cores_with_occupancy: unknown_used,
        candidates_total,
        attributed_candidates: attributed,
        unattributed_candidates: unattributed.total(),
        unattributed_by_reason: unattributed,
        waste,
        waste_withheld_because: withheld,
        relay_num_cores: input.relay_num_cores,
        broker_core_count: input.broker_core_count,
        first_core: input.first_core,
        checks,
        cores,
        reads_as,
    }
}

/// One sentence a person can quote, and it must be true of the payload beside
/// it.
///
/// FOUR ARMS, and the fourth is the one both reviewers caught missing. A
/// three-arm version discriminated on `total == 0` — no occupancy — and then
/// said "that is a statement about our index and NOT about the network", which
/// is false the moment `blocks_indexed > 0`: we looked at a thousand blocks and
/// found nothing, which is a statement about the network exactly. That is slice
/// 11's own verified defect in MIRROR IMAGE (it reported "we did not look" as
/// "there is nothing there"; this reported "there is nothing there" as "we did
/// not look"), and it was reachable from a shipped test.
fn reads_as(
    withheld: &[&'static str],
    waste: Option<&Waste>,
    task_cores: u64,
    pool_cores: u64,
    attributed: u64,
    total: u64,
    blocks_indexed: u64,
) -> String {
    // UP TO SEVEN GATES CAN FIRE and the full list is in
    // `waste_withheld_because`. "because X" would read as the sole cause.
    let because = || match withheld.len() {
        0 => "no precondition failed".to_string(),
        1 => withheld[0].to_string(),
        n => format!(
            "{n} preconditions failed, the first being: {} (see `waste_withheld_because` for all \
             of them)",
            withheld[0]
        ),
    };
    match waste {
        Some(w) => format!(
            "{:.2}% of TASK-ENTITLED core-block slots were used: {} of {} slots across {} \
             entitled core(s), leaving {} bought or reserved and unused, of which {} core(s) \
             produced nothing at all. THIS IS NOT THE OCCUPANCY RATIO AND MUST NOT BE READ AS \
             ONE — occupancy divides by every DECLARED core, which silently adds 'nobody bought \
             it' to 'somebody bought it and did not use it'. The {} pool core(s) here are NOT \
             waste: their {} slots were sold and donated to the instantaneous market and belong \
             to no purchaser by construction. {} of {} observed candidate(s) are attributed to \
             the task that was entitled to them.",
            w.used_ratio * 100.0,
            w.used_by_entitled_task,
            w.task_entitled_slots,
            task_cores,
            w.unused,
            w.idle_task_cores,
            pool_cores,
            w.pool_slots,
            attributed,
            total
        ),
        None if total > 0 => format!(
            "NO WASTE FIGURE IS SERVED, and the counts below are still complete: {attributed} of \
             {total} observed candidate(s) are attributed to the task entitled to them. What is \
             withheld is the entitled-slot ratio, because {}. An absent ratio is not a low one.",
            because()
        ),
        None if blocks_indexed == 0 => format!(
            "NEITHER A WASTE FIGURE NOR AN ATTRIBUTION IS SERVED, because this window holds no \
             indexed relay blocks at all. That is a statement about our index and NOT about the \
             network — nothing here says whether any core did anything. {}",
            because()
        ),
        None => format!(
            "THE ATTRIBUTION IS SERVED AND EVERY COUNT IN IT IS ZERO: we hold {blocks_indexed} \
             relay block(s) in this window and NO core produced a candidate in any of them. THAT \
             IS A STATEMENT ABOUT THE NETWORK, not about our index. The entitled-slot ratio is \
             withheld separately, because {}.",
            because()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ent(core: u32, kind: &str, task: Option<u32>, relay_block: u64) -> EntitlementRow {
        EntitlementRow {
            core_index: core,
            assignment_index: 0,
            relay_block,
            kind: kind.into(),
            task_id: task,
            parts: PARTS_WHOLE_CORE,
            runtime_version: 2_003_002,
            mapper_version: 1,
        }
    }

    fn cell(core: u32, para: u32, n: u64) -> OccupancyCell {
        OccupancyCell { core_index: core, para_id: para, included_blocks: n }
    }

    /// THE GOVERNING SALE, at relay 32278800 — the one slice 13's verification
    /// had to hunt ~157,000 coretime blocks back from the window it governs.
    const SALE: u64 = 32_278_800;
    const WINDOW_FROM: u64 = 32_613_537;
    const WINDOW_TO: u64 = 32_614_536;
    const BLOCKS: u64 = 1_000;
    /// `SaleInfo.first_core` as measured: cores 0..11 are reserved.
    const FIRST_CORE: u32 = 11;

    /// The measured window, rebuilt: 100 cores, 57 Task / 43 Pool, 47 producing,
    /// 34,690 candidates, with cores 53 and 13 as the two barely-used ones.
    ///
    /// The per-core distribution is not the measured one core for core (the
    /// bands were 10/19/4/12/0/2); what IS the measured one is every figure this
    /// fixture is asserted against — the totals, the split, the ratio, the idle
    /// count and the two outliers.
    fn measured_window() -> (Vec<EntitlementRow>, Vec<OccupancyCell>) {
        // 57 task cores: 47 producing + 10 idle. The ten idle ones are the
        // measured indices, every one >= FIRST_CORE.
        let idle_task: [u32; 10] = [11, 14, 15, 23, 31, 33, 45, 55, 56, 58];
        // 47 producing cores, chosen to avoid the idle ten and to include the
        // two outliers 53 and 13.
        let mut producing: Vec<u32> = Vec::new();
        let mut c = 0u32;
        while producing.len() < 47 {
            if !idle_task.contains(&c) {
                producing.push(c);
            }
            c += 1;
        }
        assert!(producing.contains(&53) && producing.contains(&13));

        let mut ents = Vec::new();
        let mut occ = Vec::new();
        // Candidate counts: cores 53 -> 10 (1.0%) and 13 -> 29 (2.9%), and the
        // other 45 summing to 34,651 so the total is exactly 34,690.
        let mut others = 0u64;
        for &core in &producing {
            let task = 2000 + core;
            ents.push(ent(core, KIND_TASK, Some(task), SALE));
            let n = match core {
                53 => 10,
                13 => 29,
                _ => {
                    others += 1;
                    // 44 cores at 770 plus one at 771 = 34,651, which with the
                    // two outliers below makes exactly 34,690.
                    if others == 45 { 771 } else { 770 }
                }
            };
            occ.push(cell(core, task, n));
        }
        for &core in &idle_task {
            ents.push(ent(core, KIND_TASK, Some(2000 + core), SALE));
        }
        // 43 pool cores, on the indices nothing else claimed.
        let claimed: BTreeSet<u32> = ents.iter().map(|e| e.core_index).collect();
        let mut pool = 0;
        for core in 0..200u32 {
            if pool == 43 {
                break;
            }
            if !claimed.contains(&core) {
                ents.push(ent(core, KIND_POOL, None, SALE));
                pool += 1;
            }
        }
        assert_eq!(ents.len(), 100);
        (ents, occ)
    }

    fn input<'a>(
        ents: &'a [EntitlementRow],
        occ: &'a [OccupancyCell],
    ) -> DeltaInput<'a> {
        DeltaInput {
            occupancy: occ,
            entitlement: ents,
            blocks_indexed: BLOCKS,
            window_from: WINDOW_FROM,
            anchor_relay_block: WINDOW_TO,
            relay_num_cores: Some(100),
            broker_core_count: Some(100),
            first_core: Some(FIRST_CORE),
        }
    }

    /// THE SLICE'S WHOLE CLAIM, asserted as the numbers slice 13's verification
    /// computed by hand in SQL. If any of these move, either the arithmetic
    /// changed or the measurement was wrong, and both are worth stopping for.
    #[test]
    fn the_delta_reproduces_the_measured_window() {
        let (ents, occ) = measured_window();
        let r = compute(input(&ents, &occ));

        // Attribution: AGREE 47 / DISAGREE 0 / 34,690 of 34,690.
        assert_eq!(r.agree_cores, 47);
        assert_eq!(r.disagree_cores, 0);
        assert_eq!(r.candidates_total, 34_690);
        assert_eq!(r.attributed_candidates, 34_690);
        assert_eq!(r.unattributed_candidates, 0);

        // Census: 57 Task / 43 Pool, and nothing unknown.
        assert_eq!(r.task_entitled_cores, 57);
        assert_eq!(r.pool_cores, 43);
        assert_eq!(r.idle_cores, 0, "no core is entitled to Idle in this sale");
        assert_eq!(r.cores_without_entitlement, 0);
        assert!(r.unknown_cores.is_empty());
        assert_eq!(r.cores_seen, 100);

        // One sale governs the whole window, and it sits BELOW the window — the
        // ~335,000-relay-block distance is the reason `governing_relay_blocks`
        // is served rather than assumed contemporary.
        assert_eq!(r.governing_relay_blocks, vec![SALE]);
        assert!(r.entitlement_stable_across_window);
        assert!(r.changed_in_window.is_empty());

        // Waste: 60.86% of 57,000 entitled slots used, 22,310 unused.
        let w = r.waste.expect("every gate passes on the measured window");
        assert_eq!(w.task_entitled_slots, 57_000);
        assert_eq!(w.used_by_entitled_task, 34_690);
        assert_eq!(w.used_by_another_task, 0);
        assert_eq!(w.unused, 22_310);
        assert!(
            (w.used_ratio * 10_000.0).round() as u64 == 6086,
            "60.86% of TASK-ENTITLED slots, not 34.69% of all slots: {}",
            w.used_ratio
        );
        // AND IT IS NOT SLICE 11'S NUMBER. 34,690 / (100 * 1000) = 34.69%, which
        // is the figure the occupancy endpoint serves; the whole point of the
        // entitled denominator is that these two differ.
        let occupancy_ratio = 34_690.0 / 100_000.0;
        assert!(w.used_ratio > occupancy_ratio);

        // 10 idle task cores, EVERY ONE market-side. This is the claim that the
        // waste is entirely on the purchased side and that no reserved system
        // core sat idle.
        assert_eq!(w.idle_task_cores, 10);
        assert_eq!(w.idle_task_cores_market, Some(10));
        assert_eq!(w.idle_task_cores_reserved, Some(0));

        // The identity: 43 pool + 10 idle task = 53 = slice 11's own "53 cores
        // producing nothing", which that slice recorded without being able to
        // say why.
        assert_eq!(w.cores_producing_nothing, 53);
        assert_eq!(r.pool_cores + w.idle_task_cores, 53);
        assert_eq!(w.pool_slots, 43_000, "sold and donated, and NOT waste");

        // Every check comes back ok on real-shaped data.
        assert_eq!(r.checks.denominators_agree, Check::Ok);
        assert_eq!(r.checks.cores_within_denominator, Check::Ok);
        assert_eq!(r.checks.cores_account_for_the_denominator, Check::Ok);
        assert_eq!(r.checks.occupancy_within_slots, Check::Ok);
        assert_eq!(r.checks.entitlement_stable, Check::Ok);
        assert!(r.waste_withheld_because.is_empty());
    }

    /// SLICE 11'S ON-DEMAND INFERENCE, REFUTED BY THE PER-CORE ROW.
    ///
    /// Slice 11 recorded cores 53 and 13 as "the on-demand/low-activity signal"
    /// and correctly flagged the reading as an INFERENCE, because relay data
    /// cannot distinguish bulk from on-demand at all. Entitlement says both are
    /// Task-entitled bulk cores with a FULL 80-bit mask, using 1.0% and 2.9% of
    /// what they bought — the most under-used bulk entitlements on the chain.
    #[test]
    fn the_two_barely_used_cores_are_bulk_purchases_and_not_on_demand() {
        let (ents, occ) = measured_window();
        let r = compute(input(&ents, &occ));
        let find = |c: u32| r.cores.iter().find(|x| x.core_index == c).expect("core present");

        for (core, blocks, pct) in [(53u32, 10u64, 1.0f64), (13, 29, 2.9)] {
            let row = find(core);
            assert_eq!(row.entitlement_kind, "task");
            assert_eq!(row.verdict, "agree");
            assert_eq!(row.parts, Some(PARTS_WHOLE_CORE), "a WHOLE core, not a slice of one");
            assert_eq!(row.included_blocks, blocks);
            let got = row.used_ratio.expect("blocks are indexed") * 100.0;
            assert!((got - pct).abs() < 0.05, "core {core}: {got}% vs {pct}%");
            assert_eq!(row.market_side, Some(true));
        }
        // And no pool core produced anything, which is the independent
        // corroboration: an on-demand core would have to be a pool core.
        assert_eq!(r.pool_cores_with_occupancy, 0);
    }

    /// THE NON-NEGOTIABLE RULE, and the reason it is a gate rather than a label.
    ///
    /// `CoreAssigned` fires only at sale boundaries, so a shallow entitlement
    /// backfill leaves cores with no assignment at all. Rendering those as
    /// `idle` would report cores nobody bought as bought-and-wasted; worse, a
    /// waste figure computed over the few cores that DID resolve would read as
    /// near-perfect efficiency, which is the flattering direction.
    #[test]
    fn a_core_with_no_assignment_is_unknown_and_the_waste_figure_refuses() {
        // 100 declared cores, entitlement for only three of them.
        let ents = vec![
            ent(0, KIND_TASK, Some(2004), SALE),
            ent(1, KIND_TASK, Some(2034), SALE),
            ent(2, KIND_POOL, None, SALE),
        ];
        let occ = vec![cell(0, 2004, 900), cell(7, 3344, 500)];
        let r = compute(input(&ents, &occ));

        assert_eq!(r.cores_without_entitlement, 97);
        assert!(r.unknown_cores.contains(&7));
        let seven = r.cores.iter().find(|c| c.core_index == 7).unwrap();
        assert_eq!(seven.entitlement_kind, "unknown");
        assert_eq!(seven.verdict, "unknown_used");
        assert_eq!(seven.governing_relay_block, None);
        // NOT idle, anywhere.
        assert!(r.cores.iter().all(|c| c.entitlement_kind != "idle"));

        // The attribution is still served — it is a count, and 900 of 1400 is
        // itself the coverage statement.
        assert_eq!(r.candidates_total, 1_400);
        assert_eq!(r.attributed_candidates, 900);
        assert_eq!(r.unattributed_by_reason.unknown, 500);

        // But the waste figure refuses, and says which gate refused.
        assert!(r.waste.is_none());
        assert!(
            r.waste_withheld_because
                .iter()
                .any(|s| s.contains("NO assignment at or before the anchor")),
            "{:?}",
            r.waste_withheld_because
        );
        assert!(r.reads_as.contains("NO WASTE FIGURE IS SERVED"));
    }

    /// POOL TIME IS NOT WASTE, asserted as an arithmetic property rather than as
    /// wording — which is what catches a later change helpfully folding the two.
    #[test]
    fn pool_slots_never_reach_the_waste_denominator() {
        let ents = vec![
            ent(0, KIND_TASK, Some(2004), SALE),
            ent(1, KIND_POOL, None, SALE),
            ent(2, KIND_POOL, None, SALE),
        ];
        let occ = vec![cell(0, 2004, 400)];
        let r = compute(DeltaInput {
            relay_num_cores: Some(3),
            broker_core_count: Some(3),
            ..input(&ents, &occ)
        });
        let w = r.waste.expect("gates pass");
        // ONE task core, so 1,000 entitled slots — not 3,000.
        assert_eq!(w.task_entitled_slots, 1_000);
        assert_eq!(w.unused, 600);
        assert_eq!(w.pool_slots, 2_000);
        // The two pool cores produced nothing and are not idle waste.
        assert_eq!(w.cores_producing_nothing, 2);
        assert_eq!(w.idle_task_cores, 0);
        assert!(r.reads_as.contains("NOT waste"));
    }

    /// A core producing blocks for a para it was not entitled to serve. Zero
    /// live instances — expressible and unobserved, the same standing as slice
    /// 13's "97 sent, 97 applied, zero unpartnered".
    #[test]
    fn a_core_serving_the_wrong_para_disagrees_and_its_blocks_are_unattributed() {
        let ents = vec![ent(0, KIND_TASK, Some(2004), SALE)];
        let occ = vec![cell(0, 2004, 300), cell(0, 3344, 100)];
        let r = compute(DeltaInput {
            relay_num_cores: Some(1),
            broker_core_count: Some(1),
            ..input(&ents, &occ)
        });
        assert_eq!(r.disagree_cores, 1);
        assert_eq!(r.agree_cores, 0);
        assert_eq!(r.attributed_candidates, 300);
        assert_eq!(r.unattributed_by_reason.wrong_task, 100);
        let w = r.waste.expect("gates pass");
        // The core DID work, so the unused figure must not count the 100 blocks
        // it spent on somebody else as unused capacity.
        assert_eq!(w.used_by_entitled_task, 300);
        assert_eq!(w.used_by_another_task, 100);
        assert_eq!(w.unused, 600);
    }

    /// AN ASSIGNMENT LANDING INSIDE THE WINDOW MEANS TWO ENTITLEMENTS UNDER ONE
    /// OCCUPANCY, and one figure would average two questions.
    #[test]
    fn an_entitlement_that_moves_inside_the_window_withholds_the_ratio() {
        let ents = vec![
            ent(0, KIND_TASK, Some(2004), SALE),
            // takes effect halfway through the window
            ent(1, KIND_TASK, Some(2034), WINDOW_FROM + 500),
        ];
        let occ = vec![cell(0, 2004, 900), cell(1, 2034, 400)];
        let r = compute(DeltaInput {
            relay_num_cores: Some(2),
            broker_core_count: Some(2),
            ..input(&ents, &occ)
        });
        assert!(!r.entitlement_stable_across_window);
        assert_eq!(
            r.changed_in_window,
            vec![CoreChange { core_index: 1, relay_block: WINDOW_FROM + 500 }]
        );
        assert_eq!(r.checks.entitlement_stable, Check::Contradicted);
        assert!(r.waste.is_none());

        // AND THE BOUNDARY IS STRICT: an assignment landing exactly ON the
        // window's first block governs the whole window and is not a change.
        let ents = vec![ent(0, KIND_TASK, Some(2004), WINDOW_FROM)];
        let occ = vec![cell(0, 2004, 900)];
        let r = compute(DeltaInput {
            relay_num_cores: Some(1),
            broker_core_count: Some(1),
            ..input(&ents, &occ)
        });
        assert!(r.entitlement_stable_across_window, "at `from` governs [from, to]");
        assert!(r.waste.is_some());
    }

    /// 0025 IN ITS OWN WORDS: "if they ever disagree, one of the two halves is
    /// being counted against the wrong denominator and the delta reader must
    /// REFUSE rather than pick one."
    #[test]
    fn disagreeing_denominators_refuse_rather_than_pick_one() {
        let ents = vec![ent(0, KIND_TASK, Some(2004), SALE)];
        let occ = vec![cell(0, 2004, 900)];
        let r = compute(DeltaInput {
            relay_num_cores: Some(1),
            broker_core_count: Some(2),
            ..input(&ents, &occ)
        });
        assert_eq!(r.checks.denominators_agree, Check::Contradicted);
        assert!(r.waste.is_none());
        assert!(r
            .waste_withheld_because
            .iter()
            .any(|s| s.contains("DISAGREE")));
        // Both numbers are still reported — refusing to divide is not refusing
        // to say what was read.
        assert_eq!(r.relay_num_cores, Some(1));
        assert_eq!(r.broker_core_count, Some(2));
    }

    /// An interlaced core is entitled to a FRACTION of itself, and this reader
    /// does not divide by `parts` — the mask's pattern never crosses to the
    /// relay, so a ratio built on the bit count would look exact while not
    /// being. Unexercised on live data: every measured assignment carries a full
    /// 57,600.
    #[test]
    fn an_interlaced_or_mixed_core_is_counted_and_never_divided() {
        // Two half-core task entitlements on one core.
        let mut a = ent(0, KIND_TASK, Some(2004), SALE);
        a.parts = 28_800;
        let mut b = ent(0, KIND_TASK, Some(2000), SALE);
        b.assignment_index = 1;
        b.parts = 28_800;
        // parts sum to a whole core but there are two tasks: still one kind, so
        // this is an ordinary (if unobserved) Task core.
        let ents = vec![a.clone(), b];
        let occ = vec![cell(0, 2004, 500)];
        let r = compute(DeltaInput {
            relay_num_cores: Some(1),
            broker_core_count: Some(1),
            ..input(&ents, &occ)
        });
        assert_eq!(r.cores[0].entitlement_kind, "task");
        assert_eq!(r.cores[0].entitled_tasks, vec![2000, 2004]);
        assert_eq!(r.cores[0].verdict, "agree", "either entitled task attributes");
        assert!(r.waste.is_some());

        // A core split between a task and the pool is MIXED: entitled to half of
        // itself, so it enters no denominator and the ratio is withheld.
        let mut p = ent(0, KIND_POOL, None, SALE);
        p.assignment_index = 1;
        p.parts = 28_800;
        let ents = vec![a, p];
        let r = compute(DeltaInput {
            relay_num_cores: Some(1),
            broker_core_count: Some(1),
            ..input(&ents, &occ)
        });
        assert_eq!(r.cores[0].entitlement_kind, "mixed");
        assert_eq!(r.cores[0].verdict, "unattributable");
        assert_eq!(r.unattributable_cores, 1);
        assert_eq!(r.task_entitled_cores, 0);
        assert!(r.waste.is_none());
        assert!(r
            .waste_withheld_because
            .iter()
            .any(|s| s.contains("MIXED or FRACTIONAL")));

        // And a single assignment covering less than a whole core is
        // FRACTIONAL for the same reason.
        let mut short = ent(0, KIND_TASK, Some(2004), SALE);
        short.parts = 28_800;
        let ents = vec![short];
        let r = compute(DeltaInput {
            relay_num_cores: Some(1),
            broker_core_count: Some(1),
            ..input(&ents, &occ)
        });
        assert_eq!(r.cores[0].entitlement_kind, "fractional");
        assert!(r.waste.is_none());
    }

    /// A WINDOW WE HOLD NO BLOCKS FOR HAS NO RATIO AND NO PER-CORE RATIO —
    /// slice 11's rule, which its own verification found violated on the sibling
    /// figure: 0/100 is arithmetically defined and factually "we did not look"
    /// rendered as "there is nothing there".
    #[test]
    fn an_empty_window_yields_no_ratios_rather_than_zeroes() {
        let ents = vec![ent(0, KIND_TASK, Some(2004), SALE)];
        let r = compute(DeltaInput {
            occupancy: &[],
            entitlement: &ents,
            blocks_indexed: 0,
            window_from: WINDOW_FROM,
            anchor_relay_block: WINDOW_TO,
            relay_num_cores: Some(1),
            broker_core_count: Some(1),
            first_core: Some(FIRST_CORE),
        });
        assert!(r.waste.is_none());
        assert!(r.cores.iter().all(|c| c.used_ratio.is_none()));
        assert_eq!(r.checks.occupancy_within_slots, Check::Unknown);
        assert!(r.reads_as.contains("NEITHER A WASTE FIGURE NOR AN ATTRIBUTION"));
        // The entitlement census survives — we DID look at that.
        assert_eq!(r.task_entitled_cores, 1);
    }

    /// A window in which every core is pooled has NO entitled denominator, and
    /// a used ratio of 0.0 there would read as "nothing bought was used" when
    /// nothing was bought as a task at all.
    #[test]
    fn a_window_with_no_task_entitlement_withholds_rather_than_dividing_by_zero() {
        let ents = vec![ent(0, KIND_POOL, None, SALE), ent(1, KIND_POOL, None, SALE)];
        let occ: Vec<OccupancyCell> = vec![];
        let r = compute(DeltaInput {
            relay_num_cores: Some(2),
            broker_core_count: Some(2),
            ..input(&ents, &occ)
        });
        assert_eq!(r.pool_cores, 2);
        assert_eq!(r.task_entitled_cores, 0);
        assert!(r.waste.is_none(), "0/0 is not a used ratio of zero");
        assert!(r
            .waste_withheld_because
            .iter()
            .any(|s| s.contains("no core in this window is task-entitled")));

        // AND THE SENTENCE MUST NOT CALL THIS "a statement about our index".
        // We looked at 1,000 blocks and found nothing produced — that is a
        // statement about the NETWORK, and rendering it as a coverage gap is
        // slice 11's verified defect in mirror image.
        assert!(
            r.reads_as.contains("STATEMENT ABOUT THE NETWORK"),
            "{}",
            r.reads_as
        );
        assert!(!r.reads_as.contains("NOT about the network"), "{}", r.reads_as);
    }

    /// GATE 6: a core cannot be included more times than the window holds
    /// blocks — 0024's partial unique index is one candidate per core per
    /// block. If it happens anyway the occupancy side is not self-consistent,
    /// and dividing by it would launder the contradiction into a ratio.
    #[test]
    fn a_core_with_more_inclusions_than_blocks_withholds_the_ratio() {
        let ents = vec![ent(0, KIND_TASK, Some(2004), SALE)];
        let occ = vec![cell(0, 2004, BLOCKS + 1)];
        let r = compute(DeltaInput {
            relay_num_cores: Some(1),
            broker_core_count: Some(1),
            ..input(&ents, &occ)
        });
        assert_eq!(r.checks.occupancy_within_slots, Check::Contradicted);
        assert!(r.waste.is_none());
        assert!(r
            .waste_withheld_because
            .iter()
            .any(|s| s.contains("more inclusions than the window holds blocks")));
    }

    /// GATE 7's SECOND HALF, and it is what makes the unknown-core gate able to
    /// fire at all.
    ///
    /// With no `num_cores` reading the core universe is only what we HOLD, so a
    /// task-entitled core that sat idle and whose sale we never indexed is not
    /// enumerated, is therefore never `unknown`, and the denominator is quietly
    /// truncated — waste omitted, which is the flattering direction. Reachable
    /// on any DB where `broker-range` ran and `sync-core-config` did not, and
    /// `/occupancy` refuses to serve a ratio in exactly that state.
    #[test]
    fn without_a_declared_core_count_the_waste_figure_refuses() {
        let ents = vec![ent(0, KIND_TASK, Some(2004), SALE)];
        let occ = vec![cell(0, 2004, 900)];
        let r = compute(DeltaInput {
            relay_num_cores: None,
            broker_core_count: Some(1),
            ..input(&ents, &occ)
        });
        // Everything looks complete — one core, entitled, used — and that is
        // exactly the trap: nothing here can say how many cores exist.
        assert_eq!(r.cores_without_entitlement, 0);
        assert_eq!(r.task_entitled_cores, 1);
        assert!(r.waste.is_none());
        assert!(r
            .waste_withheld_because
            .iter()
            .any(|s| s.contains("no `num_cores` reading is on record")));
        assert_eq!(r.checks.denominators_agree, Check::Unknown);
        assert_eq!(r.checks.cores_within_denominator, Check::Unknown);
    }

    /// A core the runtime scheduled work onto beyond the declared count is a
    /// contradiction the runtime produced — slice 11's stale-denominator
    /// detector, on this endpoint's own data.
    #[test]
    fn a_core_beyond_the_denominator_contradicts_it() {
        let ents = vec![ent(9, KIND_TASK, Some(2004), SALE)];
        let occ = vec![cell(9, 2004, 100)];
        let r = compute(DeltaInput {
            relay_num_cores: Some(4),
            broker_core_count: Some(4),
            ..input(&ents, &occ)
        });
        assert_eq!(r.checks.cores_within_denominator, Check::Contradicted);
        assert_eq!(r.cores_seen, 5, "0..4 declared, plus the core that exists anyway");
    }

    /// Without a `first_core` reading the reserved/market split is NOT assumed.
    /// Assuming 0 would move every reserved system core into the market and put
    /// the waste on the wrong side of the boundary — see 0026.
    #[test]
    fn without_first_core_the_market_split_is_absent_rather_than_guessed() {
        let ents = vec![ent(0, KIND_TASK, Some(2004), SALE), ent(1, KIND_TASK, Some(2034), SALE)];
        let occ = vec![cell(0, 2004, 900)];
        let r = compute(DeltaInput {
            relay_num_cores: Some(2),
            broker_core_count: Some(2),
            first_core: None,
            ..input(&ents, &occ)
        });
        let w = r.waste.expect("first_core is not a gate — the split is, not the ratio");
        assert_eq!(w.idle_task_cores, 1);
        assert_eq!(w.idle_task_cores_market, None);
        assert_eq!(w.idle_task_cores_reserved, None);
        assert!(r.cores.iter().all(|c| c.market_side.is_none()));
    }
}
