//! Governance semantics of the Substrate family (Invariant 4: protocol
//! specifics live in adapters only). Two pure pieces:
//!
//! 1. `SubstrateGovMapper` — canonical referenda-pallet events → referendum
//!    timeline entries. Instances are recognized by pallet name (the decoder
//!    lowercases pallet names):
//!      referenda           → class "referenda"           (public OpenGov)
//!      fellowshipreferenda → class "fellowship_referenda" (Collectives)
//!    so registering Collectives later needs ZERO code here. Kinds/statuses:
//!      Submitted                  → submitted           (status: submitted)
//!      DecisionDepositPlaced      → decision_deposit_placed      (info)
//!      DecisionDepositRefunded    → decision_deposit_refunded    (info)
//!      DecisionStarted            → decision_started    (status: deciding)
//!      ConfirmStarted             → confirm_started     (status: confirming)
//!      ConfirmAborted             → confirm_aborted     (status: deciding)
//!      Confirmed                  → confirmed           (status: confirmed)
//!      Approved                   → approved            (status: approved)
//!      Rejected                   → rejected            (status: rejected)
//!      TimedOut                   → timed_out           (status: timed_out)
//!      Cancelled                  → cancelled           (status: cancelled)
//!      Killed                     → killed              (status: killed)
//!      SubmissionDepositRefunded  → submission_deposit_refunded  (info)
//!      MetadataSet / MetadataCleared → metadata_set/_cleared     (info)
//!    Deliberately ∅: DepositSlashed (carries who/amount but NO referendum
//!    index — unattributable by design of the pallet event).
//!    UNKNOWN referenda.* events are ERRORS — a runtime upgrade adding a
//!    status-moving event must halt the mapper loudly, never drop history.
//!
//! 2. `tracks_from_metadata` — decode every referenda instance's `Tracks`
//!    constant from a runtime metadata blob. Track definitions come from the
//!    runtime's OWN metadata (generate, don't curate — same doctrine as
//!    PalletId walking in `accounts`). Handles both the classic
//!    `[(TrackId, TrackInfo)]` shape and the newer `Track { id, info }`
//!    struct shape, and names as either `str` or padded byte arrays.
//!
//! GOV_MAPPER_VERSION is lineage: bump on ANY rule change; rows rebuild from
//! canonical events.

use crate::frame_decoder::value_to_json;
use canonical::CanonicalEvent;
use frame_metadata::{RuntimeMetadata, RuntimeMetadataPrefixed};
use ingest::gov::{GovMapper, RefTimelineEntry};
use parity_scale_codec::Decode;
use scale_value::{Composite, Value, ValueDef};

pub const GOV_MAPPER_VERSION: u32 = 1;

pub struct SubstrateGovMapper;

impl GovMapper for SubstrateGovMapper {
    fn timeline(&self, event: &CanonicalEvent) -> Result<Vec<RefTimelineEntry>, String> {
        timeline_for_event(event)
    }
    fn mapper_version(&self) -> u32 {
        GOV_MAPPER_VERSION
    }
}

/// Referenda instance pallets (decoder-lowercased) → class. Instances are
/// adapter vocabulary, like "balances.Transfer" is for the balances mapper.
///
/// KNOWN-UNMAPPED instances, stated rather than implied (these became
/// REACHABLE the moment Collectives was registered, and they map to ∅ here
/// rather than halting the worker):
///   ambassadorreferenda.* — pallet-referenda Instance2 on Collectives
///   secretarycollective.* / ambassadorcollective.* — further ranked-collective
///                           instances there
///   democracy.*           — the pre-OpenGov relay model
/// Each needs a registry `referenda_classes` entry alongside its class here, so
/// adding one stays a two-line change in two files.
fn class_of(pallet: &str) -> Option<&'static str> {
    match pallet {
        "referenda" => Some("referenda"),
        "fellowshipreferenda" => Some("fellowship_referenda"),
        _ => None,
    }
}

/// The pure mapping. Non-referenda events map to ∅; malformed referenda
/// events are ERRORS (governance history must never silently drop).
pub fn timeline_for_event(event: &CanonicalEvent) -> Result<Vec<RefTimelineEntry>, String> {
    let Some((pallet, variant)) = event.name.split_once('.') else {
        return Ok(vec![]);
    };
    let Some(class) = class_of(pallet) else {
        return Ok(vec![]);
    };
    let data = &event.data;
    let ctx = |what: &str| format!("{}: {what} (data: {data})", event.name);

    // DepositSlashed carries who/amount but NO referendum index — the pallet
    // event is unattributable by design. Deliberate ∅, documented.
    if variant == "DepositSlashed" {
        return Ok(vec![]);
    }

    // (kind, status). status None = informational, must not move the projection.
    let (kind, status): (&str, Option<&str>) = match variant {
        "Submitted" => ("submitted", Some("submitted")),
        "DecisionDepositPlaced" => ("decision_deposit_placed", None),
        "DecisionDepositRefunded" => ("decision_deposit_refunded", None),
        "DecisionStarted" => ("decision_started", Some("deciding")),
        "ConfirmStarted" => ("confirm_started", Some("confirming")),
        "ConfirmAborted" => ("confirm_aborted", Some("deciding")),
        "Confirmed" => ("confirmed", Some("confirmed")),
        "Approved" => ("approved", Some("approved")),
        "Rejected" => ("rejected", Some("rejected")),
        "TimedOut" => ("timed_out", Some("timed_out")),
        "Cancelled" => ("cancelled", Some("cancelled")),
        "Killed" => ("killed", Some("killed")),
        "SubmissionDepositRefunded" => ("submission_deposit_refunded", None),
        "MetadataSet" => ("metadata_set", None),
        "MetadataCleared" => ("metadata_cleared", None),
        // an UNKNOWN referenda event is a mapper gap, never silently ∅
        _ => {
            return Err(format!(
                "unknown referenda event {} — gov mapper update required",
                event.name
            ))
        }
    };

    let referendum_id = field_u64(data, "index", 0).ok_or_else(|| ctx("no index"))?;

    // Submitted { index, track, proposal }; DecisionStarted { index, track,
    // proposal, tally } — both carry track + proposal, positionally 1 and 2.
    let (track_id, proposal) = match variant {
        "Submitted" | "DecisionStarted" => {
            let track = field_u64(data, "track", 1).ok_or_else(|| ctx("no track"))? as u32;
            let proposal = field(data, "proposal", 2).cloned();
            (Some(track), proposal)
        }
        _ => (None, None),
    };
    let (proposal_hash, proposal_len) = proposal
        .as_ref()
        .map(bounded_call_hash_len)
        .unwrap_or((None, None));

    Ok(vec![RefTimelineEntry {
        class: class.to_string(),
        referendum_id,
        kind: kind.to_string(),
        status: status.map(str::to_string),
        track_id,
        proposal,
        proposal_hash,
        proposal_len,
        data: data.clone(),
    }])
}

/// Bounded<RuntimeCall> JSON → (0x-hash, len) where knowable:
///   {"Lookup": {hash, len}} → both; {"Legacy": {hash}} → hash only;
///   {"Inline": bytes} → neither (the preimage slice hashes inline bytes).
fn bounded_call_hash_len(proposal: &serde_json::Value) -> (Option<String>, Option<u64>) {
    let serde_json::Value::Object(map) = proposal else {
        return (None, None);
    };
    if let Some(lookup) = map.get("Lookup") {
        return (
            field(lookup, "hash", 0).and_then(json_h256_hex),
            field_u64(lookup, "len", 1),
        );
    }
    if let Some(legacy) = map.get("Legacy") {
        return (field(legacy, "hash", 0).and_then(json_h256_hex), None);
    }
    (None, None)
}

// ------------------------------------------------------- JSON field plumbing
// Event data is schema-on-read JSON written by our own decoders: named fields
// as objects, positional as arrays; H256 as (nested) byte arrays; small
// numbers as JSON numbers, >u64 as decimal strings.

pub(crate) fn field<'a>(data: &'a serde_json::Value, name: &str, index: usize) -> Option<&'a serde_json::Value> {
    match data {
        serde_json::Value::Object(map) => map.get(name),
        serde_json::Value::Array(items) => items.get(index),
        _ => None,
    }
}

fn field_u64(data: &serde_json::Value, name: &str, index: usize) -> Option<u64> {
    match field(data, name, index)? {
        serde_json::Value::Number(n) => n.as_u64(),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// Collect exactly 32 bytes out of arbitrarily nested arrays/objects (H256
/// renders as a newtype over the byte array) → 0x-hex.
pub(crate) fn json_h256_hex(v: &serde_json::Value) -> Option<String> {
    fn walk(v: &serde_json::Value, out: &mut Vec<u8>) -> bool {
        match v {
            serde_json::Value::Number(n) => match n.as_u64() {
                Some(b) if b <= 255 => {
                    out.push(b as u8);
                    true
                }
                _ => false,
            },
            serde_json::Value::Array(items) => items.iter().all(|i| walk(i, out)),
            serde_json::Value::Object(map) => map.values().all(|i| walk(i, out)),
            _ => false,
        }
    }
    let mut out = Vec::with_capacity(32);
    if walk(v, &mut out) && out.len() == 32 {
        Some(format!("0x{}", hex::encode(out)))
    } else {
        None
    }
}

// ------------------------------------------------------- preimage storage key

/// twox128("Preimage") ++ twox128("PreimageFor"). Verified in-sandbox against
/// reference xxhash with the same derivation that reproduces
/// accounts::SYSTEM_ACCOUNT_PREFIX byte-for-byte.
pub const PREIMAGE_FOR_PREFIX: [u8; 32] = [
    0xd8, 0xf3, 0x14, 0xb7, 0xf4, 0xe6, 0xb0, 0x95, 0xf0, 0xf8, 0xee, 0x46, 0x56, 0xa4, 0x48,
    0x25, 0x7c, 0x7d, 0xda, 0x85, 0xc9, 0xc2, 0x97, 0x99, 0x9f, 0xd0, 0x22, 0x15, 0xe8, 0xc8,
    0xf9, 0xde,
];

/// Full preimage.preimageFor storage key for one (hash, len). The map's
/// hasher is Identity, so the key is prefix ++ SCALE((H256, u32)) =
/// prefix ++ hash ++ len_le. The stored VALUE is a BoundedVec<u8> (compact
/// length prefix + call bytes) — callers strip the prefix before decoding.
pub fn preimage_for_key(hash: &[u8; 32], len: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(32 + 32 + 4);
    key.extend_from_slice(&PREIMAGE_FOR_PREFIX);
    key.extend_from_slice(hash);
    key.extend_from_slice(&len.to_le_bytes());
    key
}

// ------------------------------------------------------------- tracks decode

/// One track definition decoded from a runtime's own metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackDef {
    /// Lowercased instance pallet ("referenda", "fellowshipreferenda") —
    /// matches the event-name prefix the decoder writes.
    pub pallet: String,
    pub track_id: u32,
    pub name: String,
    /// Full TrackInfo as JSON (max_deciding, deposits, periods, curves) —
    /// schema-on-read; period units are the chain's own clock.
    pub params: serde_json::Value,
}

/// Decode the `Tracks` constant of every pallet that has one. Pure — bytes
/// in, track definitions out; decode against the SAME runtime's metadata the
/// constant came from (the blob is self-describing).
pub fn tracks_from_metadata(metadata_blob: &[u8]) -> Result<Vec<TrackDef>, String> {
    let prefixed = RuntimeMetadataPrefixed::decode(&mut &metadata_blob[..])
        .map_err(|e| format!("metadata blob undecodable: {e}"))?;

    // unlike decode_account_info, this walk needs no versioned imports —
    // constants have the same shape across v14/v15/v16, so one $m suffices
    macro_rules! walk_tracks {
        ($m:expr) => {{
            let mut out: Vec<TrackDef> = Vec::new();
            for pallet in &$m.pallets {
                for constant in &pallet.constants {
                    if constant.name != "Tracks" {
                        continue;
                    }
                    let mut cursor = &constant.value[..];
                    let value =
                        scale_value::scale::decode_as_type(&mut cursor, constant.ty.id, &$m.types)
                            .map_err(|e| {
                                format!("{}.Tracks constant decode: {e}", pallet.name)
                            })?;
                    let defs = tracks_from_value(&value.remove_context())
                        .map_err(|e| format!("{}.Tracks walk: {e}", pallet.name))?;
                    for (track_id, name, params) in defs {
                        out.push(TrackDef {
                            pallet: pallet.name.to_lowercase(),
                            track_id,
                            name,
                            params,
                        });
                    }
                }
            }
            out
        }};
    }

    let tracks = match &prefixed.1 {
        RuntimeMetadata::V14(m) => walk_tracks!(m),
        RuntimeMetadata::V15(m) => walk_tracks!(m),
        RuntimeMetadata::V16(m) => walk_tracks!(m),
        _ => return Err("unsupported metadata version (v14/v15/v16 only)".into()),
    };
    Ok(tracks)
}

/// The decoded Tracks constant is a sequence of entries, each either the
/// classic `(TrackId, TrackInfo)` tuple or the newer `Track { id, info }`
/// struct. Names are either `str` or padded byte arrays (`s("root")` style).
fn tracks_from_value(v: &Value<()>) -> Result<Vec<(u32, String, serde_json::Value)>, String> {
    let entries = match &v.value {
        ValueDef::Composite(Composite::Unnamed(items)) => items,
        _ => return Err("Tracks constant is not a sequence".into()),
    };
    let mut out = Vec::with_capacity(entries.len());
    for (i, entry) in entries.iter().enumerate() {
        let (id_v, info_v) = match &entry.value {
            ValueDef::Composite(Composite::Unnamed(pair)) if pair.len() == 2 => {
                (&pair[0], &pair[1])
            }
            ValueDef::Composite(Composite::Named(fields)) => {
                let id = fields.iter().find(|(n, _)| n == "id").map(|(_, v)| v);
                let info = fields.iter().find(|(n, _)| n == "info").map(|(_, v)| v);
                match (id, info) {
                    (Some(id), Some(info)) => (id, info),
                    _ => return Err(format!("track entry {i}: no id/info fields")),
                }
            }
            _ => return Err(format!("track entry {i}: unrecognized shape")),
        };
        let track_id = value_u128(id_v)
            .and_then(|n| u32::try_from(n).ok())
            .ok_or_else(|| format!("track entry {i}: id is not a u32"))?;
        let name = track_name(info_v).ok_or_else(|| {
            format!("track entry {i} (id {track_id}): TrackInfo has no readable name")
        })?;
        // overwrite the raw name inside params with the cleaned one: StringLike
        // names carry NUL padding, and NUL is unrepresentable in Postgres
        // TEXT/JSONB — leaving it in would make every tracks upsert fail
        let mut params = value_to_json(info_v);
        if let Some(obj) = params.as_object_mut() {
            obj.insert("name".into(), serde_json::Value::String(name.clone()));
        }
        out.push((track_id, name, params));
    }
    Ok(out)
}

fn value_u128(v: &Value<()>) -> Option<u128> {
    match &v.value {
        ValueDef::Primitive(scale_value::Primitive::U128(n)) => Some(*n),
        _ => None,
    }
}

fn track_name(info: &Value<()>) -> Option<String> {
    let ValueDef::Composite(Composite::Named(fields)) = &info.value else {
        return None;
    };
    let name = fields.iter().find(|(n, _)| n == "name").map(|(_, v)| v)?;
    match &name.value {
        // &'static str — BUT since polkadot-sdk #7671 the runtime uses
        // StringLike<25>, whose TypeInfo claims `str` while ENCODING as 25
        // NUL-padded bytes: scale-value hands us "root\0\0…" here (review
        // catch, verified byte-level in the spec-2003002 fixture) — trim it
        ValueDef::Primitive(scale_value::Primitive::String(s)) => {
            let s = s.trim_end_matches('\0').trim();
            (!s.is_empty()).then(|| s.to_string())
        }
        // newer: fixed byte array padded with NULs (StringLike / s("root"))
        _ => {
            let mut bytes = Vec::new();
            fn walk(v: &Value<()>, out: &mut Vec<u8>) -> bool {
                match &v.value {
                    ValueDef::Primitive(scale_value::Primitive::U128(n)) if *n <= 255 => {
                        out.push(*n as u8);
                        true
                    }
                    ValueDef::Composite(Composite::Unnamed(items)) => {
                        items.iter().all(|i| walk(i, out))
                    }
                    ValueDef::Composite(Composite::Named(items)) => {
                        items.iter().all(|(_, i)| walk(i, out))
                    }
                    _ => false,
                }
            }
            if !walk(name, &mut bytes) {
                return None;
            }
            let trimmed: Vec<u8> = bytes.into_iter().take_while(|b| *b != 0).collect();
            let s = String::from_utf8(trimmed).ok()?;
            let s = s.trim().to_string();
            (!s.is_empty()).then_some(s)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(name: &str, data: serde_json::Value) -> CanonicalEvent {
        CanonicalEvent {
            index: 0,
            transaction_index: None,
            name: name.into(),
            data,
        }
    }

    fn h256_json(byte: u8) -> serde_json::Value {
        // the shape our decoder writes: newtype over the byte array
        serde_json::json!([vec![byte; 32]])
    }

    #[test]
    fn submitted_maps_with_track_and_lookup_proposal() {
        let e = ev(
            "referenda.Submitted",
            serde_json::json!({
                "index": 1828,
                "track": 34,
                "proposal": {"Lookup": {"hash": h256_json(0xab), "len": 142}}
            }),
        );
        let es = timeline_for_event(&e).unwrap();
        assert_eq!(es.len(), 1);
        let t = &es[0];
        assert_eq!(t.class, "referenda");
        assert_eq!(t.referendum_id, 1828);
        assert_eq!(t.kind, "submitted");
        assert_eq!(t.status.as_deref(), Some("submitted"));
        assert_eq!(t.track_id, Some(34));
        assert_eq!(t.proposal_hash.as_deref(), Some(&format!("0x{}", "ab".repeat(32))[..]));
        assert_eq!(t.proposal_len, Some(142));
        assert!(t.proposal.is_some());
    }

    #[test]
    fn decision_started_positional_fields_work() {
        // positional array form: [index, track, proposal, tally]
        let e = ev(
            "referenda.DecisionStarted",
            serde_json::json!([
                1900,
                11,
                {"Inline": [1, 2, 3]},
                {"ayes": 1, "nays": 0, "support": 1}
            ]),
        );
        let t = &timeline_for_event(&e).unwrap()[0];
        assert_eq!(t.kind, "decision_started");
        assert_eq!(t.status.as_deref(), Some("deciding"));
        assert_eq!(t.track_id, Some(11));
        // Inline: no hash/len derivable here (preimage slice hashes inline bytes)
        assert_eq!(t.proposal_hash, None);
        assert_eq!(t.proposal_len, None);
    }

    #[test]
    fn status_flow_kinds_and_infos() {
        let cases: &[(&str, &str, Option<&str>)] = &[
            ("ConfirmStarted", "confirm_started", Some("confirming")),
            ("ConfirmAborted", "confirm_aborted", Some("deciding")),
            ("Confirmed", "confirmed", Some("confirmed")),
            ("Approved", "approved", Some("approved")),
            ("Rejected", "rejected", Some("rejected")),
            ("TimedOut", "timed_out", Some("timed_out")),
            ("Cancelled", "cancelled", Some("cancelled")),
            ("Killed", "killed", Some("killed")),
            ("DecisionDepositPlaced", "decision_deposit_placed", None),
            ("SubmissionDepositRefunded", "submission_deposit_refunded", None),
            ("MetadataSet", "metadata_set", None),
        ];
        for (variant, kind, status) in cases {
            let e = ev(
                &format!("referenda.{variant}"),
                serde_json::json!({"index": 7, "who": [vec![0u8; 32]], "amount": 1, "tally": {}, "hash": h256_json(1)}),
            );
            let t = &timeline_for_event(&e).unwrap()[0];
            assert_eq!((t.kind.as_str(), *variant), (*kind, *variant));
            assert_eq!(t.status.as_deref(), *status, "{variant}");
            assert_eq!(t.referendum_id, 7);
        }
    }

    #[test]
    fn fellowship_instance_maps_to_its_own_class() {
        let e = ev(
            "fellowshipreferenda.Approved",
            serde_json::json!({"index": 300}),
        );
        let t = &timeline_for_event(&e).unwrap()[0];
        assert_eq!(t.class, "fellowship_referenda");
        assert_eq!(t.referendum_id, 300);
    }

    #[test]
    fn deposit_slashed_is_unattributable_and_maps_to_nothing() {
        let e = ev(
            "referenda.DepositSlashed",
            serde_json::json!({"who": [vec![0u8; 32]], "amount": 100}),
        );
        assert!(timeline_for_event(&e).unwrap().is_empty());
    }

    #[test]
    fn other_pallets_are_not_governance() {
        for name in ["balances.Transfer", "system.ExtrinsicSuccess", "whitelist.CallWhitelisted"] {
            let e = ev(name, serde_json::json!({"index": 1}));
            assert!(timeline_for_event(&e).unwrap().is_empty(), "{name} must map to ∅");
        }
    }

    #[test]
    fn unknown_and_malformed_referenda_events_are_loud_errors() {
        let unknown = ev("referenda.SomeFutureEvent", serde_json::json!({"index": 1}));
        assert!(timeline_for_event(&unknown).is_err());
        let no_index = ev("referenda.Approved", serde_json::json!({"nope": true}));
        assert!(timeline_for_event(&no_index).is_err());
        let no_track = ev(
            "referenda.Submitted",
            serde_json::json!({"index": 5, "proposal": {"Inline": []}}),
        );
        assert!(timeline_for_event(&no_track).is_err());
    }

    #[test]
    fn legacy_proposal_hash_extracts() {
        let e = ev(
            "referenda.Submitted",
            serde_json::json!({
                "index": 12, "track": 0,
                "proposal": {"Legacy": {"hash": h256_json(0x0d)}}
            }),
        );
        let t = &timeline_for_event(&e).unwrap()[0];
        assert_eq!(t.proposal_hash.as_deref(), Some(&format!("0x{}", "0d".repeat(32))[..]));
        assert_eq!(t.proposal_len, None);
    }

    #[test]
    fn nul_padded_stringlike_track_names_are_trimmed() {
        // StringLike<25> decodes as a String with NUL padding (sdk #7671);
        // the name AND the copy inside params must come out clean — Postgres
        // rejects NUL in TEXT/JSONB
        let info = Value::named_composite([
            ("name", Value::string("big_spender\0\0\0\0\0\0\0\0\0\0\0\0\0\0")),
            ("max_deciding", Value::u128(50)),
        ]);
        let entry = Value::unnamed_composite([Value::u128(34), info]);
        let tracks = tracks_from_value(&Value::unnamed_composite([entry])).unwrap();
        assert_eq!(tracks.len(), 1);
        let (id, name, params) = &tracks[0];
        assert_eq!((*id, name.as_str()), (34, "big_spender"));
        assert_eq!(params["name"], "big_spender");
        assert_eq!(params["max_deciding"], 50);
    }

    #[test]
    fn tracks_decode_from_real_metadata() {
        // real committed AH metadata (spec 2003002) — governance lives on AH,
        // so its runtime carries the Referenda pallet + Tracks constant
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/real/polkadot-asset-hub-19498783/metadata.scale");
        let Ok(blob) = std::fs::read(&path) else {
            eprintln!("SKIP: real fixture metadata not present at {}", path.display());
            return;
        };
        let tracks = tracks_from_metadata(&blob).expect("tracks decode");
        let referenda: Vec<&TrackDef> =
            tracks.iter().filter(|t| t.pallet == "referenda").collect();
        assert_eq!(referenda.len(), 16, "OpenGov has 16 tracks (ECOSYSTEM §7)");
        let root = referenda.iter().find(|t| t.track_id == 0).expect("track 0");
        assert_eq!(root.name, "root");
        let big_spender = referenda.iter().find(|t| t.track_id == 34).expect("track 34");
        assert!(big_spender.name.contains("big_spender"), "got {}", big_spender.name);
        // params carry the decision-period machinery, schema-on-read
        assert!(root.params.get("max_deciding").is_some());
        assert!(root.params.get("decision_period").is_some());
    }
}
