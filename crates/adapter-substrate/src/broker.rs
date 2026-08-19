//! `pallet-broker` → entitlement facts (Phase 3, slice 13).
//!
//! The ENTITLEMENT half of coretime. `adapter_substrate::coretime` (slice 11)
//! maps the relay's candidate events into OCCUPANCY — what each core actually
//! did — and ROADMAP's coretime bullet says plainly that neither half alone is
//! the product. This file is the other half, and
//! [`CoreAssignmentFact`] is the seam between them.
//!
//! ---------------------------------------------------------------------------
//! THE JOIN IS AN EQUALITY, NOT A CORRELATION, AND IT WAS MEASURED BEFORE IT WAS
//! DESIGNED AGAINST
//! ---------------------------------------------------------------------------
//! `Broker.CoreAssigned { core, when, assignment }` carries a RELAY BLOCK NUMBER
//! in `when`, so joining this half to `coretime.core_occupancy` needs no
//! timeslice arithmetic at all — the chain states the number line itself.
//!
//! And the core indices are the SAME number line, measured over 97 pairs at the
//! sale boundary (coretime block 3188739 → relay 29053195–29053207):
//! `set(broker cores) == set(relay cores)`, both 97 distinct, `min diff 0 /
//! max diff 0`. The SDK's own source says why there is no offset — the relay's
//! `coretime::assign_core` does `u32::from(core).into()`, a widening and nothing
//! else, beside a comment noting the broker's `CoreIndex` is `u16` where the
//! relay's is `CoreIndex(u32)`.
//!
//! ---------------------------------------------------------------------------
//! THE SHAPE TRAP THAT WOULD HAVE BEEN INVISIBLE: THE TWO SIDES RENDER THE CORE
//! INDEX DIFFERENTLY
//! ---------------------------------------------------------------------------
//! The broker's is a bare `u16` — `"core": 0` — because `pallet_broker::
//! CoreIndex` is a plain type alias with NO newtype layer. The relay's is
//! `{"core":[0]}`, because `polkadot_primitives::CoreIndex` IS a newtype over
//! `u32`, and slice 11 met that layer for the seventh time in this project.
//!
//! So a mapper that peels on both sides or on neither gets one of them wrong,
//! and the wrongness is silent: peeling a bare number yields None (a NULL core
//! index, i.e. an entitlement row that cannot say which core), while not peeling
//! a wrapped one yields None too. [`bare_u64`] therefore REFUSES an array rather
//! than tolerating both shapes — a tolerant reader here would hide the exact
//! runtime change this file most needs to hear about.
//!
//! ---------------------------------------------------------------------------
//! THE VOCABULARY IS CLOSED AND THE ZEROES ARE PART OF IT
//! ---------------------------------------------------------------------------
//! The live runtime declares **37** variants. Six fire in the sampled windows
//! (653 coretime blocks): `CoreAssigned` 97, `HistoryInitialized` 18,
//! `Renewable` 11, `Renewed` 11, `AutoRenewalEnabled` 5, `SaleInitialized` 1.
//! **31 have no live instance**, so their field lists are pinned against
//! `pallet-broker` 0.28.0 — the first published version whose 37 variant names
//! match the runtime's list byte-for-byte, in order — rather than against a
//! measurement that does not exist. THAT COMPARISON WAS MADE WHILE AUTHORING,
//! NOT IN THE PREP PASS (which pinned 0.6.0-0.18.0), so it is corroboration
//! from upstream and NOT a measurement of this chain. That is the same standing as slice 11's
//! `CandidateTimedOut` arity and slice 6's eleven unexercised orml variants, and
//! it is stated rather than glossed.
//!
//! **`Purchased` having zero instances is a GAP IN THE SAMPLE, NOT A FINDING
//! ABOUT THE MARKET.** 653 blocks is a fraction of a 28-day cycle and purchases
//! spread across a 14-day leadin.
//!
//! TWO RENAMES CONFIRMED ON THE LIVE RUNTIME, both of which a mapper built from
//! older sources would have read as ABSENT rather than as an error: the variant
//! is `PotentialRenewalDropped` (not `AllowedRenewalDropped`) and the field is
//! `SaleInitialized.end_price` (not `regular_price`). An addition halts loudly;
//! an absence reads as a field that simply is not there, which is why the
//! vocabulary below is enumerated rather than pattern-matched on a prefix.
//!
//! There are NO `#[codec(index)]` attributes anywhere in the pallet's Event
//! enum, so variant indices are implicit and positional — a variant inserted in
//! the middle shifts every later one. Nothing here reads an index (the decoder
//! hands us names), but it is why the decoder's own spec-keyed metadata is what
//! makes this safe and why a row without `runtime_version` would be unreadable.

use canonical::CanonicalEvent;
use ingest::broker::{BrokerMapper, BrokerRow, CoreAssignmentRow};

/// Bump when the RULES change — which variants are mapped, or where a core or a
/// task is read from.
///
/// 1 — the initial vocabulary: all 37 declared variants mapped, none ∅, the
/// `CoreAssigned` vector expanded into the seam, and core/task promoted per
/// variant from the field the runtime actually declares.
pub const BROKER_MAPPER_VERSION: u32 = 1;

/// The pallet, as the decoder spells it. The runtime declares `Broker` (pallet
/// index 50) and `frame_decoder` lowercases.
pub const BROKER_PALLET: &str = "broker";

pub const ASSIGNMENT_IDLE: &str = "idle";
pub const ASSIGNMENT_POOL: &str = "pool";
pub const ASSIGNMENT_TASK: &str = "task";

/// Where a variant's core index comes from, if anywhere.
///
/// AN ENUM RATHER THAN A NAME SEARCH, and that is the whole defence against this
/// file's sharpest trap. Three variants carry a field whose name contains
/// "core" and whose value is a COUNT rather than an index —
/// `CoreCountRequested.core_count`, `CoreCountChanged.core_count`,
/// `SalesStarted.core_count` — and two more carry `ideal_cores_sold` /
/// `cores_offered`. A depth-first search for something core-shaped finds 100 and
/// records it as core 100, which does not exist and is exactly the class of
/// plausible-number-from-the-wrong-node that slice 11's `num_cores` path and
/// slice 6's `Issued.total_supply` were both written to avoid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CoreFrom {
    /// A named `core` field, bare `u16`.
    Field,
    /// `region_id.core` — the region id is a STRUCT on the wire, so its `core`
    /// is a named sub-field. (The packed `u128` form exists only as the
    /// nonfungible ItemId and appears in no event and no storage entry.)
    RegionId,
    /// `old_region_id.core`, on the two split variants.
    OldRegionId,
    /// `region.core` — RevenueClaimBegun names its field `region`, NOT
    /// `region_id`, alone among the region-shaped variants.
    Region,
    None,
}

/// Where a variant's task id comes from, if anywhere. Always a named `task`
/// field where present.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskFrom {
    Field,
    None,
}

/// The whole declared vocabulary, with the SUBJECT of each variant.
///
/// 19 name a core, 3 name only a task, 15 name neither. Enumerated in the
/// runtime's own declaration order so a diff against a future metadata dump is a
/// diff and not a puzzle.
///
/// NOTHING IS ∅ HERE, unlike every other mapper in this project. `broker_events`
/// is "every `Broker.*` event", because on this pallet the announcement IS the
/// entitlement fact — there is no second vocabulary saying the same thing
/// (contrast `parainclusion.UpwardMessagesReceived`, which `xcm.messages`
/// already owns). An unknown 38th variant is still a LOUD halt.
const VOCABULARY: &[(&str, CoreFrom, TaskFrom)] = &[
    ("Purchased", CoreFrom::RegionId, TaskFrom::None),
    ("Renewable", CoreFrom::Field, TaskFrom::None),
    // BOTH `old_core` AND `core`, and this takes `core`. The renewal MOVED the
    // index — measured at coretime 4919882, where para 3428 renewed five cores
    // and every one changed (35→43, 36→44, 37→45, 40→46, 41→47) — so the row's
    // subject is where the entitlement IS, not where it was. `old_core` stays in
    // `data`, and the pair is what a cross-cycle reader needs.
    ("Renewed", CoreFrom::Field, TaskFrom::None),
    ("Transferred", CoreFrom::RegionId, TaskFrom::None),
    ("Partitioned", CoreFrom::OldRegionId, TaskFrom::None),
    ("Interlaced", CoreFrom::OldRegionId, TaskFrom::None),
    ("Assigned", CoreFrom::RegionId, TaskFrom::Field),
    ("AssignmentRemoved", CoreFrom::RegionId, TaskFrom::None),
    ("Pooled", CoreFrom::RegionId, TaskFrom::None),
    // `core_count` is a COUNT. See CoreFrom's doc comment.
    ("CoreCountRequested", CoreFrom::None, TaskFrom::None),
    ("CoreCountChanged", CoreFrom::None, TaskFrom::None),
    // `index` is a RESERVATION index into a bounded list, not a core index.
    ("ReservationMade", CoreFrom::None, TaskFrom::None),
    ("ReservationCancelled", CoreFrom::None, TaskFrom::None),
    // `ideal_cores_sold` and `cores_offered` are counts.
    ("SaleInitialized", CoreFrom::None, TaskFrom::None),
    ("Leased", CoreFrom::None, TaskFrom::Field),
    ("LeaseRemoved", CoreFrom::None, TaskFrom::Field),
    ("LeaseEnding", CoreFrom::None, TaskFrom::Field),
    ("SalesStarted", CoreFrom::None, TaskFrom::None),
    // The one variant that spells it `region`.
    ("RevenueClaimBegun", CoreFrom::Region, TaskFrom::None),
    ("RevenueClaimItem", CoreFrom::None, TaskFrom::None),
    // `next` is an `Option<RegionId>` naming the NEXT region to claim, not this
    // event's subject. Promoting from it would key a payment to a region it did
    // not pay for.
    ("RevenueClaimPaid", CoreFrom::None, TaskFrom::None),
    ("CreditPurchased", CoreFrom::None, TaskFrom::None),
    ("RegionDropped", CoreFrom::RegionId, TaskFrom::None),
    ("ContributionDropped", CoreFrom::RegionId, TaskFrom::None),
    ("RegionUnpooled", CoreFrom::RegionId, TaskFrom::None),
    ("HistoryInitialized", CoreFrom::None, TaskFrom::None),
    ("HistoryDropped", CoreFrom::None, TaskFrom::None),
    ("HistoryIgnored", CoreFrom::None, TaskFrom::None),
    ("ClaimsReady", CoreFrom::None, TaskFrom::None),
    // THE SEAM. Also expands — see `assignments_from` below.
    ("CoreAssigned", CoreFrom::Field, TaskFrom::None),
    ("PotentialRenewalDropped", CoreFrom::Field, TaskFrom::None),
    ("AutoRenewalEnabled", CoreFrom::Field, TaskFrom::Field),
    ("AutoRenewalDisabled", CoreFrom::Field, TaskFrom::Field),
    // `payer` is an `Option<AccountId>` and renders THREE array layers deep.
    // Nothing here reads it; it stays in `data`.
    ("AutoRenewalFailed", CoreFrom::Field, TaskFrom::None),
    // A UNIT variant with no fields at all — measured on the live runtime. The
    // decoder renders a fieldless variant as an empty array or object, and the
    // subject lookup below must not require either.
    ("AutoRenewalLimitReached", CoreFrom::None, TaskFrom::None),
    ("ForceReservationFailed", CoreFrom::None, TaskFrom::None),
    ("PotentialRenewalRemoved", CoreFrom::Field, TaskFrom::None),
];

/// The variant this file expands into the seam.
pub const CORE_ASSIGNED: &str = "CoreAssigned";

/// One `Broker.*` event, as it lands in the fact table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerFact {
    /// The variant name WITHOUT the pallet prefix, exactly as the runtime spells
    /// it.
    pub variant: String,
    pub core_index: Option<u32>,
    pub task_id: Option<u32>,
    /// Zero rows for 36 of the 37 variants; N for `CoreAssigned`.
    pub assignments: Vec<CoreAssignmentFact>,
}

/// One `(CoreAssignment, PartsOf57600)` pair from one `CoreAssigned`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreAssignmentFact {
    pub assignment_index: u32,
    pub core_index: u32,
    /// A RELAY block number, stated by the chain. Always an exact multiple of 80
    /// (one timeslice) on live data.
    pub relay_block: u64,
    /// idle | pool | task
    pub kind: &'static str,
    pub task_id: Option<u32>,
    /// 57600 is a whole core. THE ONLY SURVIVING TRACE OF THE CORE MASK:
    /// `pallet-broker`'s tick converts an 80-bit mask as `count_ones() * 720`
    /// and discards the pattern, so the relay learns a ratio and never a
    /// schedule. Measured 57600 on every one of the 97 live assignments.
    pub parts: u32,
}

/// Map one event. `Ok(None)` means "not our pallet"; `Err` halts the worker.
pub fn broker_fact_for_event(event: &CanonicalEvent) -> Result<Option<BrokerFact>, String> {
    let Some(variant) = event
        .name
        .strip_prefix(BROKER_PALLET)
        .and_then(|rest| rest.strip_prefix('.'))
    else {
        return Ok(None);
    };

    let Some(&(_, core_from, task_from)) = VOCABULARY.iter().find(|(v, _, _)| *v == variant) else {
        return Err(format!(
            "unknown {BROKER_PALLET} variant '{variant}': this mapper covers the {} variants the \
             runtime declared when it was written, and a new one means the entitlement vocabulary \
             grew. Reporting entitlement from an incomplete vocabulary would UNDER-STATE what was \
             bought, which reads as waste that did not happen — the delta would be wrong in the \
             direction that makes the product look most interesting, which is the worst direction \
             for it to be wrong in. Extend VOCABULARY and re-run the range",
            VOCABULARY.len()
        ));
    };

    // Every Broker variant carries NAMED fields (or none at all), so the decoder
    // renders an object. A positional arm would be illegitimate here and is not
    // offered: slice 11's rule is that shape is decided per VARIANT, and on this
    // pallet every variant answers the same way.
    //
    // `AutoRenewalLimitReached` is the exception that proves it needs handling
    // rather than asserting: a fieldless variant has no object to look into, and
    // its subject is None anyway, so the lookup is skipped entirely.
    let core_index = match core_from {
        CoreFrom::None => None,
        CoreFrom::Field => Some(read_u32(event, variant, "core", &["core"])?),
        CoreFrom::RegionId => Some(read_u32(event, variant, "region_id.core", &["region_id", "core"])?),
        CoreFrom::OldRegionId => Some(read_u32(
            event,
            variant,
            "old_region_id.core",
            &["old_region_id", "core"],
        )?),
        CoreFrom::Region => Some(read_u32(event, variant, "region.core", &["region", "core"])?),
    };

    let task_id = match task_from {
        TaskFrom::None => None,
        TaskFrom::Field => Some(read_u32(event, variant, "task", &["task"])?),
    };

    let assignments = if variant == CORE_ASSIGNED {
        assignments_from(event, core_index.ok_or_else(|| {
            format!("{BROKER_PALLET}.{CORE_ASSIGNED} carries no readable `core`, so its assignment \
                     vector cannot be attributed to a core and the seam would be a row with no \
                     join key")
        })?)?
    } else {
        Vec::new()
    };

    Ok(Some(BrokerFact {
        variant: variant.to_string(),
        core_index,
        task_id,
        assignments,
    }))
}

/// Expand `CoreAssigned.assignment` — a `Vec<(CoreAssignment, PartsOf57600)>` —
/// into one row per pair.
///
/// EVERY LIVE VECTOR HAS LENGTH 1 (measured: all 97 at the boundary, all 100
/// `Workload` entries, all 45 live `Regions` keys carry a full 80-bit mask), so
/// this loop has never run twice. It is a loop anyway because the type is a
/// vector, and the day one region interlaces, a reader that took the first
/// element would silently report one of two entitlements on that core and drop
/// the other permanently.
fn assignments_from(event: &CanonicalEvent, core_index: u32) -> Result<Vec<CoreAssignmentFact>, String> {
    let relay_block = read_u64(event, CORE_ASSIGNED, "when", &["when"])?;

    let vector = field(&event.data, &["assignment"]).ok_or_else(|| {
        format!(
            "{BROKER_PALLET}.{CORE_ASSIGNED} carries no `assignment`: {}",
            event.data
        )
    })?;
    let vector = vector.as_array().ok_or_else(|| {
        format!(
            "{BROKER_PALLET}.{CORE_ASSIGNED}.assignment is {} rather than an array — it is a \
             Vec<(CoreAssignment, PartsOf57600)> and the seam cannot be built from a scalar",
            shape_of(vector)
        )
    })?;

    let mut out = Vec::with_capacity(vector.len());
    for (i, pair) in vector.iter().enumerate() {
        // A tuple renders as an array. Two elements exactly: the assignment and
        // its ratio.
        let pair = pair.as_array().filter(|p| p.len() == 2).ok_or_else(|| {
            format!(
                "{BROKER_PALLET}.{CORE_ASSIGNED}.assignment[{i}] is not a 2-element tuple: {pair}"
            )
        })?;

        let (kind, task_id) = assignment_kind(&pair[0], i)?;
        let parts = bare_u64(&pair[1]).ok_or_else(|| {
            format!(
                "{BROKER_PALLET}.{CORE_ASSIGNED}.assignment[{i}] ratio is not a bare number: {}. \
                 PartsOf57600 is a plain u16 with no newtype layer",
                pair[1]
            )
        })? as u32;

        out.push(CoreAssignmentFact {
            assignment_index: i as u32,
            core_index,
            relay_block,
            kind,
            task_id,
            parts,
        });
    }
    Ok(out)
}

/// Read a `CoreAssignment` — `Idle | Pool | Task(TaskId)`.
///
/// `Idle` and `Pool` are UNIT variants and render as `{"Idle":[]}` / `{"Pool":[]}`
/// (slice 9 measured a unit variant rendering as an empty ARRAY, never as
/// nothing). `Task` is a NEWTYPE variant, so its payload sits one array layer
/// deeper — `{"Task":[2034]}` — which is the layer this project has now met
/// eight times.
///
/// THE THREE ARE NOT COLLAPSED INTO "task or not", and that is a product
/// decision rather than a stylistic one: an `Idle` core is unsold, a `Pool` core
/// was sold and donated to the instantaneous market, and folding them together
/// would turn 43,000 pool slots into waste they are not part of.
fn assignment_kind(value: &serde_json::Value, i: usize) -> Result<(&'static str, Option<u32>), String> {
    let obj = value.as_object().ok_or_else(|| {
        format!(
            "{BROKER_PALLET}.{CORE_ASSIGNED}.assignment[{i}] kind is {} rather than an enum \
             object: {value}",
            shape_of(value)
        )
    })?;
    let (name, payload) = obj.iter().next().ok_or_else(|| {
        format!("{BROKER_PALLET}.{CORE_ASSIGNED}.assignment[{i}] kind is an empty object")
    })?;
    if obj.len() != 1 {
        return Err(format!(
            "{BROKER_PALLET}.{CORE_ASSIGNED}.assignment[{i}] kind has {} keys, expected exactly \
             one enum variant: {value}",
            obj.len()
        ));
    }

    match name.as_str() {
        "Idle" => Ok((ASSIGNMENT_IDLE, None)),
        "Pool" => Ok((ASSIGNMENT_POOL, None)),
        "Task" => {
            let id = bare_u64(payload)
                .or_else(|| payload.as_array().and_then(|a| a.first()).and_then(bare_u64))
                .ok_or_else(|| {
                    format!(
                        "{BROKER_PALLET}.{CORE_ASSIGNED}.assignment[{i}] Task payload is not a \
                         readable TaskId: {payload}. It is a newtype variant, so the id sits one \
                         array layer deep"
                    )
                })?;
            Ok((ASSIGNMENT_TASK, Some(id as u32)))
        }
        other => Err(format!(
            "{BROKER_PALLET}.{CORE_ASSIGNED}.assignment[{i}] has unknown CoreAssignment variant \
             '{other}': the enum is Idle | Pool | Task and a fourth means the relay's assignment \
             vocabulary grew. Recording it as any of the three would mis-state what the core was \
             entitled to do"
        )),
    }
}

// ------------------------------------------------------------------- helpers

fn field<'a>(data: &'a serde_json::Value, path: &[&str]) -> Option<&'a serde_json::Value> {
    let mut cur = data;
    for key in path {
        cur = cur.get(key)?;
    }
    Some(cur)
}

/// A number as the broker renders it: BARE, never wrapped.
///
/// REFUSES AN ARRAY ON PURPOSE. The relay's `CoreIndex` is a newtype and renders
/// `[0]`; the broker's is a type alias and renders `0`. A helper that accepted
/// both would let a future newtype layer through silently, and the symptom would
/// be a correct-looking entitlement row on the wrong core.
///
/// A decimal STRING is accepted because balances above `u64::MAX` render that
/// way (slice 6 measured 4.5% of orml amounts doing it) — but no field this
/// mapper reads is a balance, so that arm exists for shape robustness rather
/// than for a case anyone has seen here.
fn bare_u64(value: &serde_json::Value) -> Option<u64> {
    if let Some(n) = value.as_u64() {
        return Some(n);
    }
    value.as_str().and_then(|s| s.parse::<u64>().ok())
}

fn read_u64(event: &CanonicalEvent, variant: &str, label: &str, path: &[&str]) -> Result<u64, String> {
    let node = field(&event.data, path).ok_or_else(|| {
        format!(
            "{BROKER_PALLET}.{variant} carries no `{label}`: {}",
            event.data
        )
    })?;
    bare_u64(node).ok_or_else(|| {
        format!(
            "{BROKER_PALLET}.{variant} `{label}` is not a bare number: {node}. Broker scalars \
             carry no newtype layer — if this is an array, the runtime wrapped a type it did not \
             wrap before and every index this mapper promotes is now suspect"
        )
    })
}

fn read_u32(event: &CanonicalEvent, variant: &str, label: &str, path: &[&str]) -> Result<u32, String> {
    let raw = read_u64(event, variant, label, path)?;
    u32::try_from(raw).map_err(|_| {
        format!("{BROKER_PALLET}.{variant} `{label}` does not fit in u32: {raw}")
    })
}

fn shape_of(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "a bool",
        serde_json::Value::Number(_) => "a number",
        serde_json::Value::String(_) => "a string",
        serde_json::Value::Array(_) => "an array",
        serde_json::Value::Object(_) => "an object",
    }
}

/// The Substrate half of `ingest::broker::BrokerMapper` — generic runtime ←
/// protocol, the same direction every other module here points.
pub struct SubstrateBrokerMapper;

impl BrokerMapper for SubstrateBrokerMapper {
    fn facts(&self, event: &CanonicalEvent) -> Result<Vec<BrokerRow>, String> {
        Ok(broker_fact_for_event(event)?
            .map(|f| BrokerRow {
                variant: f.variant,
                core_index: f.core_index,
                task_id: f.task_id,
                data: event.data.clone(),
                assignments: f
                    .assignments
                    .into_iter()
                    .map(|a| CoreAssignmentRow {
                        assignment_index: a.assignment_index,
                        core_index: a.core_index,
                        relay_block: a.relay_block,
                        kind: a.kind.to_string(),
                        task_id: a.task_id,
                        parts: a.parts,
                    })
                    .collect(),
            })
            .into_iter()
            .collect())
    }
    fn mapper_version(&self) -> u32 {
        BROKER_MAPPER_VERSION
    }
}

// ------------------------------------------------- the entitlement denominator

/// `Broker.Status` and `Broker.Configuration` are both **Plain** storage values,
/// so each key is `twox128("Broker") ++ twox128(entry)` with NOTHING appended —
/// the same 32-byte shape slice 11 used for `Configuration.ActiveConfig`, and
/// the reason no key encoder is needed here either.
pub const BROKER_STORAGE_PALLET: &str = "Broker";
pub const STATUS_ENTRY: &str = "Status";
pub const CONFIGURATION_ENTRY: &str = "Configuration";

/// One dated reading of the broker's own view of how many cores exist.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerConfigView {
    /// `Status.core_count`. THE ENTITLEMENT-SIDE DENOMINATOR, and the cross-check
    /// that makes the join believable: it read **100** on the Coretime chain at
    /// the same time the RELAY's `scheduler_params.num_cores` read **100**. Two
    /// chains, two storage items, one number.
    pub core_count: u32,
    /// The whole `Status` record, kept intact (schema-on-read).
    pub status: serde_json::Value,
    /// The whole `Configuration` record, kept intact. It carries the sale
    /// geometry a price curve is evaluated against, and NOTHING READS IT YET —
    /// it is captured because a reading not taken cannot be taken later.
    pub configuration: serde_json::Value,
}

pub fn status_key() -> Vec<u8> {
    crate::assets::map_prefix(BROKER_STORAGE_PALLET, STATUS_ENTRY)
}

pub fn configuration_key() -> Vec<u8> {
    crate::assets::map_prefix(BROKER_STORAGE_PALLET, CONFIGURATION_ENTRY)
}

/// Decode both readings against the block's own metadata.
///
/// `core_count` is looked up as a NAMED field of `Status` specifically, never by
/// a depth-first search for anything count-shaped — the same rule slice 11
/// applied to `num_cores`, and for the same reason: this pallet has three other
/// fields with "core" and "count" in their names and picking one up from the
/// wrong node produces a denominator nobody can check.
pub fn decode_broker_config(
    metadata_blob: &[u8],
    status_bytes: &[u8],
    configuration_bytes: &[u8],
) -> Result<BrokerConfigView, String> {
    let status = decode_plain(metadata_blob, STATUS_ENTRY, status_bytes)?;
    let configuration = decode_plain(metadata_blob, CONFIGURATION_ENTRY, configuration_bytes)?;

    let core_count = status
        .get("core_count")
        .and_then(bare_u64)
        .ok_or_else(|| {
            format!(
                "{BROKER_STORAGE_PALLET}.{STATUS_ENTRY} carries no readable `core_count`: {status}. \
                 It is the entitlement half's denominator and there is no other source for it — a \
                 ratio computed without it would be divided by a number nobody can date"
            )
        })? as u32;

    Ok(BrokerConfigView {
        core_count,
        status,
        configuration,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The real decoded shape, copied from the prep's own dumps rather than
    /// hand-written — slice 6's lesson, where a hand-written fixture in the wrong
    /// shape made a test pass while production failed, and slice 13's own measured
    /// traps (the broker's BARE core index, the `Task` newtype layer) are the
    /// shapes these fixtures carry.
    fn ev(name: &str, data: serde_json::Value) -> CanonicalEvent {
        CanonicalEvent {
            index: 0,
            transaction_index: None,
            name: name.into(),
            data,
        }
    }

    fn fact(name: &str, data: serde_json::Value) -> BrokerFact {
        broker_fact_for_event(&ev(name, data))
            .expect("mapper halted")
            .expect("not recognised as a broker event")
    }

    /// THE VOCABULARY IS CLOSED AND ITS SIZE IS THE CLAIM.
    ///
    /// The live runtime declares 37 variants and `pallet-broker` 0.28.0 is the
    /// first published version whose 37 names match it byte-for-byte. If this
    /// count moves without the runtime moving, the mapper has grown a variant
    /// nobody measured.
    #[test]
    fn the_declared_vocabulary_is_the_measured_one_and_carries_no_duplicates() {
        assert_eq!(VOCABULARY.len(), 37, "the runtime declared 37 variants");
        let mut names: Vec<&str> = VOCABULARY.iter().map(|(v, _, _)| *v).collect();
        names.sort_unstable();
        let before = names.len();
        names.dedup();
        assert_eq!(before, names.len(), "a variant is listed twice");
        // Both renames confirmed on the live runtime. Asserting the OLD spellings
        // are absent is the half that bites: a mapper built from the older
        // sources would carry them, and a stale name does not halt — it simply
        // never matches, so the variant reads as one the runtime never emits.
        assert!(!names.contains(&"AllowedRenewalDropped"));
        assert!(names.contains(&"PotentialRenewalDropped"));
    }

    /// The 31 variants with no live instance are still mapped, because a fact
    /// table that refused them would report a market quieter than it is.
    #[test]
    fn a_variant_with_no_live_instance_still_maps() {
        // `[vec![7u8; 32]]`, NOT `[[7u8; 32]]`. `serde_json::json!` cannot parse a
        // Rust array-repeat expression in array position — it fails with "no
        // rules expected `7u8`" and the crate does not build. This project has
        // now hit that in six separate slices; the `vec!` form is the fix.
        let f = fact("broker.Purchased", json!({
            "who": [vec![7u8; 32]],
            "region_id": {"begin": 322663, "core": 61, "mask": [vec![255u8; 10]]},
            "price": "12345678901234567890",
            "duration": 5040
        }));
        assert_eq!(f.variant, "Purchased");
        assert_eq!(f.core_index, Some(61), "core comes from region_id.core");
        assert_eq!(f.task_id, None);
        assert!(f.assignments.is_empty());
    }

    /// THE TRAP THIS MAPPER IS BUILT AROUND: a count is not an index.
    ///
    /// Three variants carry a `core_count` and two carry `cores_offered` /
    /// `ideal_cores_sold`. A depth-first search for something core-shaped finds
    /// 100 and records core 100, which does not exist — the same class as slice
    /// 11's `num_cores` path and slice 6's `Issued.total_supply`.
    #[test]
    fn a_core_count_is_never_read_as_a_core_index() {
        for (name, data) in [
            ("broker.CoreCountRequested", json!({"core_count": 100})),
            ("broker.CoreCountChanged", json!({"core_count": 100})),
            ("broker.SalesStarted", json!({"price": "1", "core_count": 100})),
        ] {
            let f = fact(name, data);
            assert_eq!(f.core_index, None, "{name} names a COUNT, not a core");
        }
        // And the reservation index is not a core index either.
        let f = fact("broker.ReservationMade", json!({"index": 3, "workload": []}));
        assert_eq!(f.core_index, None);
    }

    /// `RevenueClaimBegun` is the only variant that spells its region field
    /// `region` rather than `region_id`. A shared lookup would return None here
    /// and the row would lose its core silently.
    #[test]
    fn the_one_variant_that_spells_it_region_still_finds_its_core() {
        let f = fact("broker.RevenueClaimBegun", json!({
            "region": {"begin": 318007, "core": 12, "mask": [vec![255u8; 10]]},
            "max_timeslices": 10
        }));
        assert_eq!(f.core_index, Some(12));
    }

    /// A renewal carries BOTH `old_core` and `core`, and the row's subject is
    /// where the entitlement IS. Measured: para 3428 renewed five cores and every
    /// index moved (35→43, 36→44, 37→45, 40→46, 41→47), so taking `old_core`
    /// would key every renewal to the tenant's previous slot.
    #[test]
    fn a_renewal_takes_the_new_core_and_leaves_the_old_one_in_data() {
        let f = fact("broker.Renewed", json!({
            "who": [vec![9u8; 32]],
            "price": "1000",
            "old_core": 35,
            "core": 43,
            "begin": 322663,
            "duration": 5040,
            "workload": []
        }));
        assert_eq!(f.core_index, Some(43), "the NEW core, never old_core");
    }

    /// THE SEAM, in the shape the chain really emits: a bare `u16` core, a relay
    /// block number, and a one-element vector whose assignment is a NEWTYPE
    /// variant one array layer deep.
    #[test]
    fn core_assigned_expands_into_the_seam_with_its_relay_block() {
        let f = fact("broker.CoreAssigned", json!({
            "core": 47,
            "when": 29053200u64,
            "assignment": [[{"Task": [3428]}, 57600]]
        }));
        assert_eq!(f.core_index, Some(47));
        assert_eq!(f.assignments.len(), 1);
        let a = &f.assignments[0];
        assert_eq!(a.assignment_index, 0);
        assert_eq!(a.core_index, 47);
        assert_eq!(a.relay_block, 29053200);
        assert_eq!(a.kind, ASSIGNMENT_TASK);
        assert_eq!(a.task_id, Some(3428));
        assert_eq!(a.parts, 57600, "a whole core");
    }

    /// `Idle` and `Pool` are UNIT variants and render as an empty ARRAY payload.
    ///
    /// THE THREE KINDS ARE NOT COLLAPSED, and this is the assertion that catches
    /// a later change helpfully folding them: an idle core is unsold while a pool
    /// core was sold and donated, and 43 of 100 cores are Pool. Reporting pool
    /// time as waste would invent 43,000 wasted slots that nobody bought.
    #[test]
    fn pool_and_idle_are_distinct_kinds_and_name_no_task() {
        let pool = fact("broker.CoreAssigned", json!({
            "core": 3, "when": 29053200u64, "assignment": [[{"Pool": []}, 57600]]
        }));
        assert_eq!(pool.assignments[0].kind, ASSIGNMENT_POOL);
        assert_eq!(pool.assignments[0].task_id, None);

        let idle = fact("broker.CoreAssigned", json!({
            "core": 4, "when": 29053200u64, "assignment": [[{"Idle": []}, 57600]]
        }));
        assert_eq!(idle.assignments[0].kind, ASSIGNMENT_IDLE);
        assert_ne!(idle.assignments[0].kind, pool.assignments[0].kind);
    }

    /// An interlaced core produces TWO entries, and the ordinal is what keeps
    /// both. Unexercised on live data (every measured vector has length 1 and a
    /// full 80-bit mask), so this is the shape the schema is designed against
    /// rather than one anyone has seen — stated the same way slice 11 stated
    /// `CandidateTimedOut`.
    #[test]
    fn an_interlaced_core_keeps_both_entitlements() {
        let f = fact("broker.CoreAssigned", json!({
            "core": 61,
            "when": 29053200u64,
            "assignment": [[{"Task": [2034]}, 28800], [{"Task": [2000]}, 28800]]
        }));
        assert_eq!(f.assignments.len(), 2);
        assert_eq!(f.assignments[0].assignment_index, 0);
        assert_eq!(f.assignments[1].assignment_index, 1);
        assert_eq!(f.assignments[1].task_id, Some(2000));
        // Half a core each. `parts` is the only surviving trace of the mask.
        assert!(f.assignments.iter().all(|a| a.parts == 28800));
    }

    /// THE MEASURED SHAPE ASYMMETRY, asserted as a refusal.
    ///
    /// The broker's core index is a BARE u16; the relay's is a newtype rendering
    /// `[0]`. A helper that tolerated both would let a future newtype layer
    /// through silently, and the symptom would be a correct-looking entitlement
    /// row on the wrong core.
    #[test]
    fn a_wrapped_core_index_is_refused_rather_than_peeled() {
        let err = broker_fact_for_event(&ev(
            "broker.PotentialRenewalRemoved",
            json!({"core": [61], "timeslice": 322663}),
        ))
        .expect_err("a newtype-wrapped core must halt");
        assert!(err.contains("bare number"), "{err}");
    }

    /// A fieldless variant has no object to look into, and its subject is None
    /// anyway. Measured on the live runtime as a UNIT variant.
    #[test]
    fn a_unit_variant_maps_without_a_subject() {
        for payload in [json!([]), json!({}), json!(null)] {
            let f = fact("broker.AutoRenewalLimitReached", payload);
            assert_eq!(f.core_index, None);
            assert_eq!(f.task_id, None);
        }
    }

    /// An unknown variant HALTS. A new variant means the entitlement vocabulary
    /// grew, and an incomplete one under-states what was bought — which reads as
    /// waste that did not happen, i.e. wrong in the direction that flatters the
    /// product.
    #[test]
    fn an_unknown_variant_halts_loudly_and_a_foreign_pallet_does_not() {
        let err = broker_fact_for_event(&ev("broker.SomethingNew", json!({})))
            .expect_err("an unknown variant must halt");
        assert!(err.contains("unknown broker variant"), "{err}");

        // Not our pallet — silently not ours, never an error.
        assert!(broker_fact_for_event(&ev("balances.Transfer", json!({})))
            .expect("a foreign pallet must not halt")
            .is_none());
        // And a pallet whose NAME merely starts with ours must not be swallowed:
        // the prefix strip requires the dot.
        assert!(broker_fact_for_event(&ev("brokerage.Thing", json!({})))
            .expect("a differently-named pallet must not halt")
            .is_none());
    }

    /// A fourth `CoreAssignment` variant halts rather than being recorded as one
    /// of the three — mis-stating what a core was entitled to do is the one
    /// error the delta cannot survive.
    #[test]
    fn an_unknown_assignment_kind_halts() {
        let err = broker_fact_for_event(&ev(
            "broker.CoreAssigned",
            json!({"core": 1, "when": 80u64, "assignment": [[{"Reserved": []}, 57600]]}),
        ))
        .expect_err("a fourth CoreAssignment variant must halt");
        assert!(err.contains("unknown CoreAssignment variant"), "{err}");
    }

    /// The mapper trait's contract: zero or one row per event, and the sink is
    /// keyed on that.
    #[test]
    fn the_mapper_emits_at_most_one_row_per_event() {
        let rows = SubstrateBrokerMapper
            .facts(&ev("broker.CoreAssigned", json!({
                "core": 1, "when": 80u64,
                "assignment": [[{"Task": [2000]}, 28800], [{"Pool": []}, 28800]]
            })))
            .expect("mapper halted");
        assert_eq!(rows.len(), 1, "one event is one broker_events row");
        assert_eq!(rows[0].assignments.len(), 2, "the expansion rides inside it");
        assert_eq!(SubstrateBrokerMapper.mapper_version(), BROKER_MAPPER_VERSION);
    }
}

fn decode_plain(
    metadata_blob: &[u8],
    entry: &str,
    value_bytes: &[u8],
) -> Result<serde_json::Value, String> {
    let info = crate::assets::storage_entry_info(metadata_blob, BROKER_STORAGE_PALLET, entry)?;
    if !info.hashers.is_empty() {
        return Err(format!(
            "{BROKER_STORAGE_PALLET}.{entry} declares {} hasher(s): this runtime makes it a MAP, \
             and a plain 32-byte key would read the wrong bytes",
            info.hashers.len()
        ));
    }
    let mut cursor = value_bytes;
    let value = scale_value::scale::decode_as_type(&mut cursor, info.value_type, &info.types)
        .map_err(|e| format!("{BROKER_STORAGE_PALLET}.{entry} decode: {e}"))?;
    // Same discipline as `coretime::decode_active_config` and
    // `calls::decode_call`: bytes left over mean the shape moved, and a
    // partially-read record would hand back a plausible number from the wrong
    // offset — which for `core_count` is a denominator.
    if !cursor.is_empty() {
        return Err(format!(
            "{BROKER_STORAGE_PALLET}.{entry} left {} trailing byte(s) undecoded — the shape this \
             runtime declares is not the shape these bytes carry",
            cursor.len()
        ));
    }
    Ok(crate::frame_decoder::value_to_json(&value.remove_context()))
}
