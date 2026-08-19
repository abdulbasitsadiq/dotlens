//! Core OCCUPANCY from the relay's own candidate events (Phase 3, slice 11).
//!
//! Pure: an event in, facts out. No network, no state, no chain named.
//!
//! ---------------------------------------------------------------------------
//! THE ONE RULE THIS FILE EXISTS TO STATE: SHAPE IS DECIDED PER VARIANT, NEVER
//! PER PALLET.
//!
//! Every other mapper in this project reads fields BY NAME, and slice 6
//! established why with a measurement: `core.events.data` is jsonb, which does
//! not preserve key order, so a positional index into a decoded NAMED-field
//! event is meaningless whatever the runtime declared.
//!
//! `para_inclusion` is the first pallet dotlens maps where that rule does not
//! apply uniformly, and the prep pass found it the hard way — the first query
//! that grouped all four variants together died with `cannot get array length of
//! a non-array`, and that error IS the finding:
//!
//!   * `CandidateBacked(receipt, head_data, CoreIndex, GroupIndex)`   → ARRAY, 4
//!   * `CandidateIncluded(receipt, head_data, CoreIndex, GroupIndex)` → ARRAY, 4
//!   * `CandidateTimedOut(receipt, head_data, CoreIndex)`             → ARRAY, 3
//!   * `UpwardMessagesReceived { from, count }`                       → OBJECT
//!
//! The first three are TUPLE variants with unnamed fields, so the decoder
//! renders an array and POSITION IS THE CONTRACT — an array has an order, an
//! object does not. The fourth is the one named-field variant in the same
//! pallet. A mapper carrying one rule for `parainclusion.*` is wrong about one
//! of them, and which one depends on which rule it picked.
//!
//! ARITY 4 HOLDS ON ALL SIX SPECS SAMPLED (247 … 14,322 rows each, spec 9431
//! through 2003002, ~2.5 years of runtimes), so the positional contract is
//! stable even though the descriptor nested inside field 0 changed shape twice.
//! `CandidateTimedOut`'s 3-field arity is from upstream source ONLY — zero rows
//! exist on live data — which is exactly why it gets its own arity check rather
//! than sharing the others'.
//!
//! ---------------------------------------------------------------------------
//! AND EVERY POSITION IS NEWTYPE-WRAPPED, WHICH THIS PROJECT HAS NOW MET SEVEN
//! TIMES.
//!
//!   `[0]` CandidateReceipt = `{descriptor, commitments_hash}`
//!   `[1]` HeadData         — an array (and deliberately not stored, see below)
//!   `[2]` CoreIndex        — rendered `[32]`, NOT `32`
//!   `[3]` GroupIndex       — rendered `[13]`, NOT `13`
//!
//! `CoreIndex(pub u32)` is a newtype, so a mapper reading `data[2]` as a scalar
//! gets a JSON array rather than a number — silently, since serde's `as_u64()`
//! returns None and a careless `unwrap_or(0)` would put every candidate on core
//! zero. Same layer as orml's `Processed.id`, `Sent.message`, `X1`,
//! `Ump(Para(id))` and the nested-call detection in `calls`. [`peeled_u64`] is
//! the one place it is handled.
//!
//! ---------------------------------------------------------------------------
//! WHAT IS READ FROM THE DESCRIPTOR, AND WHAT IS REFUSED FROM IT.
//!
//! READ: `para_id` (there is no other source — the event does not carry it) and
//! `relay_parent` (async backing's other end).
//!
//! REFUSED: `core_index`. It is absent on every pre-RFC-103 runtime and, where
//! both exist, disagrees with the event's on 14.4% of rows. Migration 0024
//! carries the measurement and the exact validity predicate; the short version
//! is that the descriptor's is a COLLATOR'S CLAIM and the event's is the
//! RUNTIME'S ASSIGNMENT, and this table is about the second.
//!
//! NOT STORED AT ALL: `head_data`. It is a parachain header — min 633 / avg
//! 1,127 / max 15,658 chars of JSON, 20.0 MB across 17,763 rows in a 555-block
//! subset — and occupancy needs none of it.

use canonical::CanonicalEvent;
use ingest::coretime::{OccupancyMapper, OccupancyRow};

/// Bump when the RULES change — which events are mapped, or how a field is read.
///
/// 1 — the initial vocabulary: three candidate variants mapped, one deliberate
/// ∅, the core index taken from the event and the descriptor's refused.
pub const CORETIME_MAPPER_VERSION: u32 = 1;

/// The pallet, as the decoder spells it. The runtime declares `ParaInclusion`
/// and `frame_decoder` lowercases, so this is `parainclusion` — CONFIRMED
/// against dotlens's own decoded events rather than assumed, because a mapper
/// keyed on a name nobody checked is a mapper that silently maps nothing.
pub const INCLUSION_PALLET: &str = "parainclusion";

pub const KIND_INCLUDED: &str = "included";
pub const KIND_BACKED: &str = "backed";
pub const KIND_TIMED_OUT: &str = "timed_out";

/// One candidate's occupancy of one core, as the relay reported it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OccupancyFact {
    /// included | backed | timed_out
    pub kind: &'static str,
    pub core_index: u32,
    pub para_id: u32,
    /// None on `timed_out`, which carries no `GroupIndex` — three fields where
    /// the others carry four.
    pub group_index: Option<u32>,
    /// 0x-hex. The descriptor's `relay_parent`, which is the other end of the
    /// async-backing lag. Resolving it to a HEIGHT needs `core.blocks` and is
    /// therefore the sink's job, not a pure mapper's.
    pub relay_parent_hash: Option<String>,
    /// 0x-hex, unique per candidate. What matches a `backed` row to its
    /// `included` one across the 2–6 block lag.
    pub pov_hash: Option<String>,
}

/// Map one event. `Ok(None)` is a deliberate ∅; `Err` halts the worker.
///
/// THE ∅ SET IS ONE VARIANT AND IT IS ENUMERATED RATHER THAN DEFAULTED.
/// `UpwardMessagesReceived` names a para and a message count and no core at all
/// — it is the transport module's subject (`xcm.messages` already maps the
/// relay's receiving side) and duplicating it here would be a second
/// vocabulary for one fact. An UNKNOWN `parainclusion.*` variant is a LOUD
/// error, on this project's standard rule: a new variant in a pallet we claim to
/// cover must stop the worker rather than be silently skipped, because coverage
/// that degrades quietly is worse than coverage that fails.
pub fn occupancy_for_event(event: &CanonicalEvent) -> Result<Option<OccupancyFact>, String> {
    let Some(variant) = event
        .name
        .strip_prefix(INCLUSION_PALLET)
        .and_then(|rest| rest.strip_prefix('.'))
    else {
        return Ok(None);
    };

    let (kind, arity) = match variant {
        "CandidateIncluded" => (KIND_INCLUDED, 4),
        "CandidateBacked" => (KIND_BACKED, 4),
        // THREE, not four. Unexercised on live data — zero rows in 1,542 relay
        // blocks, and `ilike '%TimedOut%'` across the whole events table returns
        // nothing — so this arity is from upstream source only. It is checked
        // separately for exactly that reason: if the shape is ever wrong, it is
        // wrong here, and a shared arity check would have hidden it behind the
        // two variants that do fire.
        "CandidateTimedOut" => (KIND_TIMED_OUT, 3),
        // ∅ — named-field variant, no core, and the transport module's subject.
        "UpwardMessagesReceived" => return Ok(None),
        other => {
            return Err(format!(
                "unknown {INCLUSION_PALLET} variant '{other}': this mapper covers \
                 CandidateIncluded, CandidateBacked, CandidateTimedOut and deliberately ignores \
                 UpwardMessagesReceived. A new variant means the runtime changed what it reports \
                 about cores, and reporting occupancy from an incomplete vocabulary would \
                 under-count it silently"
            ))
        }
    };

    // POSITION IS THE CONTRACT HERE — and only here. The three candidate
    // variants are TUPLE variants, so the decoder renders an array; an object at
    // this point means the runtime declared named fields and every index below
    // is meaningless.
    let fields = event.data.as_array().ok_or_else(|| {
        format!(
            "{}.{variant} decoded to {} rather than an array. The three candidate variants are \
             TUPLE variants and this mapper reads them positionally; a named-field shape means \
             the runtime changed and the positions cannot be trusted",
            INCLUSION_PALLET,
            shape_of(&event.data)
        )
    })?;
    if fields.len() != arity {
        return Err(format!(
            "{INCLUSION_PALLET}.{variant} has {} fields, expected {arity}. Arity 4 was measured \
             stable for Included/Backed across six specs spanning ~2.5 years; TimedOut's 3 is \
             from upstream source and has no live instance. A different count means the \
             positional contract moved",
            fields.len()
        ));
    }

    // `[2]` is CoreIndex, one newtype array layer deep.
    let core_index = peeled_u64(&fields[2]).ok_or_else(|| {
        format!(
            "{INCLUSION_PALLET}.{variant} field 2 is not a readable CoreIndex: {}. It is a \
             newtype over u32 and renders as [n] rather than n",
            fields[2]
        )
    })? as u32;

    // `[3]` is GroupIndex, same layer — and absent by construction on TimedOut.
    let group_index = if arity == 4 {
        Some(peeled_u64(&fields[3]).ok_or_else(|| {
            format!(
                "{INCLUSION_PALLET}.{variant} field 3 is not a readable GroupIndex: {}",
                fields[3]
            )
        })? as u32)
    } else {
        None
    };

    // `[0]` is the CandidateReceipt: {descriptor, commitments_hash}. The para id
    // has NO other source — the event does not carry one — so an unreadable
    // descriptor is a halt rather than a NULL. An occupancy row that cannot say
    // WHICH chain occupied the core is not an occupancy row.
    let descriptor = fields[0].get("descriptor").ok_or_else(|| {
        format!(
            "{INCLUSION_PALLET}.{variant} field 0 carries no `descriptor`: {}. Field 0 is a \
             CandidateReceipt and the para id has no other source in this event",
            fields[0]
        )
    })?;
    let para_id = peeled_u64(descriptor.get("para_id").ok_or_else(|| {
        format!("{INCLUSION_PALLET}.{variant} descriptor carries no `para_id`: {descriptor}")
    })?)
    .ok_or_else(|| {
        format!("{INCLUSION_PALLET}.{variant} descriptor `para_id` is not a number: {descriptor}")
    })? as u32;

    Ok(Some(OccupancyFact {
        kind,
        core_index,
        para_id,
        group_index,
        // BOTH OF THESE ARE OPTIONAL AND THAT IS NOT LAZINESS. `relay_parent` is
        // a double-nested H256 (`[[32 bytes]]`) and `pov_hash` sits beside it;
        // neither is load-bearing for the occupancy fact itself — the core, the
        // para and the height are — so a shape change in either degrades the
        // lag and the backed↔included match rather than halting a worker that
        // could still record which core ran what.
        relay_parent_hash: descriptor.get("relay_parent").and_then(hash_hex),
        pov_hash: descriptor.get("pov_hash").and_then(hash_hex),
    }))
}

/// The Substrate half of `ingest::coretime::OccupancyMapper` — generic runtime ←
/// protocol, the same direction every other module here points.
pub struct SubstrateOccupancyMapper;

impl OccupancyMapper for SubstrateOccupancyMapper {
    fn facts(&self, event: &CanonicalEvent) -> Result<Vec<OccupancyRow>, String> {
        // ZERO OR ONE, NEVER MORE. The sink's contract is at most one fact per
        // event index (the table is keyed by it), and this is where that is
        // guaranteed rather than hoped: one candidate event describes one
        // candidate on one core.
        Ok(occupancy_for_event(event)?
            .map(|f| OccupancyRow {
                kind: f.kind.to_string(),
                core_index: f.core_index,
                para_id: f.para_id,
                group_index: f.group_index,
                relay_parent_hash: f.relay_parent_hash,
                pov_hash: f.pov_hash,
            })
            .into_iter()
            .collect())
    }
    fn mapper_version(&self) -> u32 {
        CORETIME_MAPPER_VERSION
    }
}

// ------------------------------------------------------- the denominator, read

/// The storage prefix and entry the denominator comes from.
///
/// `Configuration.ActiveConfig` is a **Plain** entry, so its key is
/// `twox128("Configuration") ++ twox128("ActiveConfig")` with NOTHING appended —
/// no hasher, no key bytes, 32 bytes total. That is why [`active_config_key`]
/// can reuse `assets::map_prefix` verbatim instead of needing a key encoder.
pub const CONFIG_PALLET: &str = "Configuration";
pub const ACTIVE_CONFIG_ENTRY: &str = "ActiveConfig";

/// `SchedulerParams`, as one reading at one block.
///
/// `num_cores` is the ONLY field promoted to a column, because it is the only
/// one a ratio divides by. The rest travel as JSON on the same
/// schema-on-read rule 0024 states: a column arrives with its query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerParamsView {
    /// "How many cores are managed by the coretime chain." Measured 100 at relay
    /// #32614536 — and it MOVES at session boundaries, which is why the reading
    /// carries its own block height into `coretime.core_config` rather than
    /// becoming a constant anywhere.
    pub num_cores: u32,
    /// The whole `scheduler_params` composite, kept intact. Contains
    /// `group_rotation_frequency`, `paras_availability_period`,
    /// `max_validators_per_core`, **`lookahead`** (the field is `lookahead`, NOT
    /// `scheduling_lookahead` — confirmed by dumping the field list rather than
    /// trusting a depth-first hit on the name), `on_demand_queue_max_size`,
    /// `on_demand_target_queue_utilization`, `on_demand_fee_variability` and
    /// `on_demand_base_fee`.
    ///
    /// There is NO `on_demand_cores` field: on-demand shares the same
    /// `num_cores` rather than having a pool of its own, so "bulk cores" and
    /// "on-demand cores" are not two denominators.
    pub scheduler_params: serde_json::Value,
}

/// `twox128("Configuration") ++ twox128("ActiveConfig")`, 32 bytes.
pub fn active_config_key() -> Vec<u8> {
    crate::assets::map_prefix(CONFIG_PALLET, ACTIVE_CONFIG_ENTRY)
}

/// Decode `Configuration.ActiveConfig`'s value and lift `scheduler_params` out.
///
/// THE PATH IS READ RATHER THAN GUESSED. `num_cores` is looked up as a NAMED
/// field of the `scheduler_params` composite specifically, not by a depth-first
/// search of the whole `HostConfiguration` for anything called `num_cores` — a
/// name search finds whatever the newest runtime happens to add first, and a
/// denominator picked up from the wrong node is the exact failure this table's
/// provenance columns exist to make impossible.
pub fn decode_active_config(
    metadata_blob: &[u8],
    value_bytes: &[u8],
) -> Result<SchedulerParamsView, String> {
    let info =
        crate::assets::storage_entry_info(metadata_blob, CONFIG_PALLET, ACTIVE_CONFIG_ENTRY)?;
    if !info.hashers.is_empty() {
        return Err(format!(
            "{CONFIG_PALLET}.{ACTIVE_CONFIG_ENTRY} declares {} hasher(s): this runtime makes it a \
             MAP, and a plain 32-byte key would read the wrong bytes",
            info.hashers.len()
        ));
    }
    let mut cursor = value_bytes;
    let value = scale_value::scale::decode_as_type(&mut cursor, info.value_type, &info.types)
        .map_err(|e| format!("HostConfiguration decode: {e}"))?;
    // Same discipline as `calls::decode_call` and `assets::decode_value`: bytes
    // left over mean the shape moved, and a partially-read host configuration
    // would hand back a plausible number from the wrong offset.
    if !cursor.is_empty() {
        return Err(format!(
            "HostConfiguration decode left {} trailing byte(s) — shape mismatch",
            cursor.len()
        ));
    }
    let value = value.remove_context();
    let params = named(&value, "scheduler_params").ok_or_else(|| {
        "HostConfiguration carries no `scheduler_params`: on pre-RFC-103 runtimes the scheduler \
         knobs are flat fields of the host configuration, and this reading refuses rather than \
         guessing which of them is the core count"
            .to_string()
    })?;
    let num_cores = named(params, "num_cores")
        .and_then(scalar_u128)
        .ok_or_else(|| {
            "scheduler_params carries no readable `num_cores`: without it there is no denominator, \
             and a utilization ratio against an assumed core count is the figure this table exists \
             not to produce"
                .to_string()
        })?;
    let num_cores = u32::try_from(num_cores)
        .map_err(|_| format!("num_cores {num_cores} does not fit a u32 — that is not a core count"))?;
    Ok(SchedulerParamsView {
        num_cores,
        scheduler_params: crate::frame_decoder::value_to_json(params),
    })
}

fn named<'a>(v: &'a scale_value::Value<()>, name: &str) -> Option<&'a scale_value::Value<()>> {
    match &v.value {
        scale_value::ValueDef::Composite(scale_value::Composite::Named(fields)) => {
            fields.iter().find(|(n, _)| n == name).map(|(_, v)| v)
        }
        _ => None,
    }
}

fn scalar_u128(v: &scale_value::Value<()>) -> Option<u128> {
    match &v.value {
        scale_value::ValueDef::Primitive(scale_value::Primitive::U128(n)) => Some(*n),
        _ => None,
    }
}

/// A number that may be wrapped in any number of single-element arrays.
///
/// THE SEVENTH RECURRENCE, so it is one function rather than a peel at each
/// site. `CoreIndex(pub u32)` renders `[32]`; a doubly-wrapped newtype would
/// render `[[32]]`. Bounded at four layers because an unbounded walk into
/// attacker-shaped JSON is a different kind of bug, and nothing in this pallet
/// is deeper than two.
fn peeled_u64(v: &serde_json::Value) -> Option<u64> {
    let mut cur = v;
    for _ in 0..4 {
        if let Some(n) = cur.as_u64() {
            return Some(n);
        }
        // A decimal STRING is how this project prints a number above u64::MAX
        // and how `frame_decoder` renders some primitives; a core index will
        // never be that large, but reading it costs nothing and refusing it
        // would be a silent None.
        if let Some(s) = cur.as_str() {
            return s.parse().ok();
        }
        match cur.as_array() {
            Some(inner) if inner.len() == 1 => cur = &inner[0],
            _ => return None,
        }
    }
    None
}

/// A hash rendered as bytes at any nesting, back to 0x-hex.
///
/// `relay_parent` is a `[[32 bytes]]` double-nested H256 — H256 is a newtype
/// over `[u8; 32]`, so the bytes sit two layers in. Reuses `gov::json_h256_hex`,
/// which slice 2 wrote for exactly this and which already handles both the
/// bare-array and the newtype-wrapped forms; a second implementation of "read a
/// hash out of decoded JSON" is two that can disagree.
fn hash_hex(v: &serde_json::Value) -> Option<String> {
    crate::gov::json_h256_hex(v)
}

fn shape_of(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Object(_) => "an object",
        serde_json::Value::Array(_) => "an array",
        serde_json::Value::Null => "null",
        serde_json::Value::String(_) => "a string",
        serde_json::Value::Number(_) => "a number",
        serde_json::Value::Bool(_) => "a boolean",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The real decoded shape, copied from `core.events` rather than
    /// hand-written — slice 6's lesson, where a hand-written fixture in the
    /// wrong shape made a test pass while production failed.
    fn ev(name: &str, data: serde_json::Value) -> CanonicalEvent {
        CanonicalEvent {
            index: 0,
            transaction_index: None,
            name: name.into(),
            data,
        }
    }

    fn included(core: u32, group: u32, para: u32) -> CanonicalEvent {
        ev(
            "parainclusion.CandidateIncluded",
            json!([
                {
                    "descriptor": {
                        "para_id": [para],
                        "relay_parent": [vec![1u8; 32]],
                        "pov_hash": [vec![2u8; 32]],
                        "collator": [vec![3u8; 32]],
                        "signature": [vec![4u8; 64]]
                    },
                    "commitments_hash": [vec![5u8; 32]]
                },
                [0u8, 1, 2, 3],
                [core],
                [group]
            ]),
        )
    }

    #[test]
    fn the_core_index_is_read_through_its_newtype_layer_and_not_as_a_scalar() {
        // THE DEFECT THIS PINS, and it is the seventh recurrence of the layer:
        // `CoreIndex(pub u32)` renders as `[32]`. A mapper reading `data[2]` as
        // a scalar gets None from `as_u64()`, and an `unwrap_or(0)` there would
        // put every candidate in the network on core zero — a wrong number that
        // looks entirely plausible.
        let fact = occupancy_for_event(&included(32, 13, 2034))
            .expect("maps")
            .expect("a fact");
        assert_eq!(fact.core_index, 32);
        assert_eq!(fact.group_index, Some(13));
        assert_eq!(fact.para_id, 2034);
        assert_eq!(fact.kind, KIND_INCLUDED);
        // …and the bare-scalar form still reads, because a future runtime that
        // un-wrapped it must not break the mapper.
        let mut bare = included(7, 1, 1000);
        bare.data[2] = json!(7);
        assert_eq!(occupancy_for_event(&bare).unwrap().unwrap().core_index, 7);
    }

    #[test]
    fn shape_is_decided_per_variant_and_the_named_one_is_not_read_positionally() {
        // The finding the prep pass hit as an error: one pallet, both shapes.
        // `UpwardMessagesReceived` is the single named-field variant, and a
        // mapper that read it positionally would index into an object.
        let ump = ev(
            "parainclusion.UpwardMessagesReceived",
            json!({ "from": [1005], "count": 1 }),
        );
        assert_eq!(
            occupancy_for_event(&ump).expect("a deliberate ∅, not an error"),
            None,
            "it names a para and a count and no core; the transport module owns it"
        );

        // …while a CANDIDATE variant arriving as an object is a shape change and
        // must halt rather than be read positionally.
        let mut wrong = included(1, 1, 1000);
        wrong.data = json!({ "core_index": 1 });
        let err = occupancy_for_event(&wrong).unwrap_err();
        assert!(err.contains("rather than an array"), "{err}");
        assert!(err.contains("an object"), "{err}");
    }

    #[test]
    fn timed_out_carries_three_fields_and_its_arity_is_checked_on_its_own() {
        // UNEXERCISED ON LIVE DATA — zero rows in 1,542 relay blocks — so this
        // arity comes from upstream source and nothing else. It is checked
        // separately from the other two precisely because a shared check would
        // hide a wrong shape behind the variants that do fire.
        let timed = ev(
            "parainclusion.CandidateTimedOut",
            json!([
                { "descriptor": { "para_id": [3344] }, "commitments_hash": [vec![0u8; 32]] },
                [9u8, 9],
                [41]
            ]),
        );
        let fact = occupancy_for_event(&timed).expect("maps").expect("a fact");
        assert_eq!(fact.kind, KIND_TIMED_OUT);
        assert_eq!(fact.core_index, 41);
        assert_eq!(
            fact.group_index, None,
            "three fields, so there is no GroupIndex to read — not a zero"
        );

        // A four-field TimedOut is a shape change, and the arity check is what
        // catches it rather than field 3 being read as a group index.
        let mut four = timed.clone();
        four.data.as_array_mut().unwrap().push(json!([2]));
        let err = occupancy_for_event(&four).unwrap_err();
        assert!(err.contains("has 4 fields, expected 3"), "{err}");
    }

    #[test]
    fn the_descriptors_core_index_is_never_read_even_when_it_is_present() {
        // MEASURED: the two disagree on 14.4% of rows where both exist, and the
        // descriptor's is absent entirely on pre-RFC-103 runtimes. This asserts
        // the mapper takes the EVENT's — the runtime's assignment — rather than
        // the descriptor's, which is a collator's claim.
        let mut v2 = included(32, 13, 2034);
        v2.data[0]["descriptor"]["core_index"] = json!(35184);
        v2.data[0]["descriptor"]["session_index"] = json!(2_973_429_320u64);
        v2.data[0]["descriptor"]["version"] = json!(0);
        let fact = occupancy_for_event(&v2).unwrap().unwrap();
        assert_eq!(
            fact.core_index, 32,
            "the event's field wins; 35184 is the garbage a V1 descriptor \
             reinterpreted as V2 produces, and `version == 0` does not catch it"
        );
    }

    #[test]
    fn a_para_id_that_cannot_be_read_halts_rather_than_recording_a_coreless_row() {
        // The para id has NO other source in this event, so an unreadable
        // descriptor cannot become a NULL: a row that says a core was busy
        // without saying what ran on it is not an occupancy row.
        let mut bad = included(1, 1, 1000);
        bad.data[0]["descriptor"] = json!({ "collator": [vec![0u8; 32]] });
        let err = occupancy_for_event(&bad).unwrap_err();
        assert!(err.contains("no `para_id`"), "{err}");

        let mut bad = included(1, 1, 1000);
        bad.data[0] = json!({ "commitments_hash": [vec![0u8; 32]] });
        let err = occupancy_for_event(&bad).unwrap_err();
        assert!(err.contains("no `descriptor`"), "{err}");
    }

    #[test]
    fn an_unknown_variant_halts_loudly_and_names_what_is_covered() {
        let unknown = ev("parainclusion.CandidateEvicted", json!([]));
        let err = occupancy_for_event(&unknown).unwrap_err();
        assert!(err.contains("CandidateEvicted"), "{err}");
        assert!(err.contains("under-count"), "{err}");

        // …and another pallet's event is simply not ours.
        let other = ev("balances.Transfer", json!({}));
        assert_eq!(occupancy_for_event(&other).unwrap(), None);
    }

    #[test]
    fn the_denominators_storage_entry_is_plain_and_its_key_is_the_bare_prefix() {
        // THE ONE CLAIM `sync-core-config` RESTS ON, checked against the real
        // relay runtime rather than reasoned about: `Configuration.ActiveConfig`
        // is a PLAIN entry, so its key is twox128(pallet) ++ twox128(entry) with
        // nothing appended — 32 bytes, no hasher, no key bytes. Get that wrong
        // and the read returns None, which reads exactly like "this chain
        // declares no cores" and would leave every ratio null for a reason
        // nobody could see.
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/real/polkadot-32566550/metadata.scale");
        let Ok(blob) = std::fs::read(&path) else {
            eprintln!("SKIP: real relay fixture metadata not present at {}", path.display());
            return;
        };
        let info = crate::assets::storage_entry_info(&blob, CONFIG_PALLET, ACTIVE_CONFIG_ENTRY)
            .expect("the relay runtime declares Configuration.ActiveConfig");
        assert!(
            info.hashers.is_empty(),
            "a Plain entry has no hashers; {} means it is a map and the 32-byte key is wrong",
            info.hashers.len()
        );
        assert!(info.key_type.is_none(), "a Plain entry has no key type");

        let key = active_config_key();
        assert_eq!(key.len(), 32, "nothing is appended to a Plain entry's key");
        assert_eq!(
            key[..16],
            crate::votes::twox_128(CONFIG_PALLET.as_bytes())[..],
            "the first half is twox128 of the PALLET's storage prefix"
        );
        assert_eq!(key[16..], crate::votes::twox_128(ACTIVE_CONFIG_ENTRY.as_bytes())[..]);

        // …and a value this runtime cannot decode is refused rather than
        // yielding a plausible number from the wrong offset.
        let err = decode_active_config(&blob, &[0u8; 4]).unwrap_err();
        assert!(
            err.contains("HostConfiguration decode"),
            "a truncated host configuration must not half-decode: {err}"
        );
    }

    #[test]
    fn the_relay_parent_and_pov_hash_survive_their_double_nesting() {
        // `relay_parent` is a [[32 bytes]] double-nested H256 — the same layer
        // slice 2 met on `Processed.id`, which is why this reuses that reader.
        let fact = occupancy_for_event(&included(1, 1, 1000))
            .unwrap()
            .unwrap();
        assert_eq!(
            fact.relay_parent_hash.as_deref(),
            Some(format!("0x{}", "01".repeat(32)).as_str())
        );
        assert_eq!(
            fact.pov_hash.as_deref(),
            Some(format!("0x{}", "02".repeat(32)).as_str())
        );

        // Absent is absent, not an error: neither is load-bearing for the
        // occupancy fact, so a shape change degrades the lag rather than
        // halting a worker that could still say which core ran what.
        let mut thin = included(1, 1, 1000);
        thin.data[0]["descriptor"] = json!({ "para_id": [1000] });
        let fact = occupancy_for_event(&thin).unwrap().unwrap();
        assert_eq!(fact.relay_parent_hash, None);
        assert_eq!(fact.pov_hash, None);
        assert_eq!(fact.core_index, 1, "the occupancy fact itself is unaffected");
    }
}
