//! XCM message facts (Phase 3, slice 2) — the SENDING and RECEIVING halves,
//! recorded separately and never joined here.
//!
//! Pure: one canonical event → zero or one `XcmFact`. The correlation layer
//! (ARCHITECTURE §10) reads these rows later; this file's only job is to record
//! each half accurately, including which KIND of id it saw, because that turns
//! out to be the whole difficulty.
//!
//! THERE ARE TWO IDS PER MESSAGE AND THEY ARE NOT THE SAME THING.
//!   * `pallet_xcm.Sent.message_id` is the TOPIC — `WithUniqueTopic` derives it
//!     from `frame_system::unique`, which mixes INTRABLOCK_ENTROPY, and appends
//!     it to the wire as a trailing `SetTopic`. It is not a hash of anything and
//!     cannot be recomputed after the fact: capture it or lose it forever.
//!   * `xcmpQueue.XcmpMessageSent.message_hash` and
//!     `parachainSystem.UpwardMessageSent.message_hash` are blake2_256 over the
//!     encoded `VersionedXcm` that was queued.
//!   * `messageQueue.{Processed,ProcessingFailed}.id` is EITHER — the topic if
//!     the message carried one and the receiving runtime's barrier is wrapped in
//!     `TrailingSetTopicAsId`, else the blake2 hash. **The event does not say
//!     which**, so it is recorded as `ambiguous` and a later successful join is
//!     what proves the kind.
//! A sender emits the topic AND the wire hash for one HRMP/UMP message, in the
//! same block, from two different pallets, because `WithUniqueTopic::deliver`
//! discards the inner router's hash and returns the topic. Two rows, two kinds,
//! one message — which is why `id_kind` exists rather than a single "message id".
//!
//! WHAT THIS MAPPER CANNOT SEE, stated because a silent hole here would look
//! like a quiet chain rather than a gap:
//!   * A chain may set `type XcmEventEmitter = ()`, in which case it emits NO
//!     `Sent` for executor-forwarded messages. **Hydration does exactly this**,
//!     and forwarded sends are most of its outbound traffic — its only outbound
//!     record is the queue pallet's `message_hash`.
//!   * Topic propagation ACROSS a hop only exists from `staging-xcm-executor`
//!     20.0.0 (2025-07-15), and `Sent` for forwarded legs only from 19.1.0. The
//!     Polkadot relay runtime dotlens indexes at spec 1003004 runs executor
//!     15.0.0 and has NEITHER: a multi-hop journey from that era has unrelated
//!     ids per leg and no sender event for the forwarded ones.
//!   * DMP has no sender-side hash event at all — `ChildParachainRouter` computes
//!     blake2_256 and discards it, and `parachains_dmp` has zero events. Relay →
//!     parachain is topic-or-nothing.
//!   * A `Sent` with no `Processed` is a legitimate outcome, not necessarily a
//!     gap: weight-starved XCMP enqueueing drops whole batches with a
//!     `defensive!` log and no event on either side.
//!
//! XCM_MAPPER_VERSION is lineage: bump on any rule change; rows rebuild from raw.

use crate::gov::{field, json_h256_hex};
use canonical::CanonicalEvent;
use ingest::xcm::{XcmFact, XcmMapper};

pub const XCM_MAPPER_VERSION: u32 = 1;

pub struct SubstrateXcmMapper;

impl XcmMapper for SubstrateXcmMapper {
    fn facts(&self, event: &CanonicalEvent) -> Result<Vec<XcmFact>, String> {
        facts_for_event(event)
    }
    fn mapper_version(&self) -> u32 {
        XCM_MAPPER_VERSION
    }
}

/// The pure mapping. Events outside the XCM pallets map to ∅; an UNKNOWN event
/// inside one of them is a loud error, because a new variant is exactly how a
/// runtime upgrade silently changes what "all the XCM traffic" means.
pub fn facts_for_event(event: &CanonicalEvent) -> Result<Vec<XcmFact>, String> {
    let Some((pallet, variant)) = event.name.split_once('.') else {
        return Ok(vec![]);
    };
    let data = &event.data;
    let ctx = |what: &str| format!("{}: {what} (data: {data})", event.name);

    let fact = match (pallet, variant) {
        // ------------------------------------------------------------ SENDING
        // pallet_xcm's own send path. `message` is the instruction list the
        // pallet was given — note the trailing SetTopic that actually went on
        // the wire is NOT in it, because the event is built from a clone taken
        // before the router appends it.
        ("polkadotxcm" | "xcmpallet", "Sent") => {
            let message = field(data, "message", 2).cloned();
            // EMPTY IS DATA, not a decode failure: the executor passes `None`
            // for forwarded sends deliberately ("Avoid logging the full XCM
            // message…"), so emptiness is the discriminator between a
            // pallet-originated send and an executor-forwarded one.
            let forwarded = message
                .as_ref()
                .and_then(instructions)
                .map(|list| list.is_empty())
                .unwrap_or(false);
            let destination = field(data, "destination", 1).cloned();
            // A pre-#7234 `Sent(origin, destination, message)` has no fourth
            // field at all. A NULL id labelled "topic" would be a small lie in a
            // schema whose whole point is which kind of id a row holds.
            let message_id = field(data, "message_id", 3).and_then(json_h256_hex);
            XcmFact {
                side: "sent".into(),
                transport: destination
                    .as_ref()
                    .map(transport_for_destination)
                    .unwrap_or("unknown")
                    .into(),
                id_kind: if message_id.is_some() { "topic" } else { "none" }.into(),
                message_id,
                counterparty: destination.as_ref().and_then(location_counterparty),
                origin_location: field(data, "origin", 0).cloned(),
                destination,
                message,
                forwarded,
                status: "sent".into(),
                success: None,
                error: None,
                weight_used: None,
                data: data.clone(),
            }
        }
        // Added in pallet-xcm 19.1.0. A message that was never queued has no
        // receiving half by construction — recording it keeps "we tried and it
        // failed" distinct from "we have not seen the other side yet".
        ("polkadotxcm" | "xcmpallet", "SendFailed") => XcmFact {
            side: "sent".into(),
            transport: field(data, "destination", 1)
                .map(transport_for_destination)
                .unwrap_or("unknown")
                .into(),
            message_id: field(data, "message_id", 3).and_then(json_h256_hex),
            id_kind: "topic".into(),
            counterparty: field(data, "destination", 1).and_then(location_counterparty),
            origin_location: field(data, "origin", 0).cloned(),
            destination: field(data, "destination", 1).cloned(),
            message: None,
            forwarded: false,
            status: "send_failed".into(),
            success: Some(false),
            error: field(data, "error", 2).cloned(),
            weight_used: None,
            data: data.clone(),
        },
        // The transport-level sends. These are the ONLY outbound record on a
        // chain that does not wire an XcmEventEmitter.
        ("xcmpqueue", "XcmpMessageSent") => XcmFact {
            side: "sent".into(),
            transport: "hrmp".into(),
            message_id: field(data, "message_hash", 0).and_then(json_h256_hex),
            id_kind: "wire_hash".into(),
            counterparty: None, // the event names no recipient
            origin_location: None,
            destination: None,
            message: None,
            forwarded: false,
            status: "sent".into(),
            success: None,
            error: None,
            weight_used: None,
            data: data.clone(),
        },
        // `message_hash: Option<XcmHash>` — one wrapping level more than its
        // XCMP sibling, so `{"Some": [[…]]}`. json_h256_hex walks through it.
        ("parachainsystem", "UpwardMessageSent") => XcmFact {
            side: "sent".into(),
            transport: "ump".into(),
            message_id: field(data, "message_hash", 0).and_then(json_h256_hex),
            id_kind: "wire_hash".into(),
            counterparty: Some("parent".into()),
            origin_location: None,
            destination: None,
            message: None,
            forwarded: false,
            status: "sent".into(),
            success: None,
            error: None,
            weight_used: None,
            data: data.clone(),
        },

        // ---------------------------------------------------------- RECEIVING
        // `success: false` means Outcome::Incomplete — the message ran and did
        // not finish. The pallet's own doc warns that `true` "solely means that
        // the MQ pallet will treat this as a success condition and discard the
        // message", so this boolean is about the QUEUE, not about intent.
        ("messagequeue", "Processed") => {
            let origin = field(data, "origin", 1);
            XcmFact {
                side: "received".into(),
                transport: origin.map(transport_for_origin).unwrap_or("unknown").into(),
                message_id: field(data, "id", 0)
                    .and_then(json_h256_hex)
                    .ok_or_else(|| ctx("no id"))
                    .map(Some)?,
                id_kind: "ambiguous".into(),
                counterparty: origin.and_then(origin_counterparty),
                origin_location: origin.cloned(),
                destination: None,
                message: None,
                forwarded: false,
                status: "processed".into(),
                success: field(data, "success", 3).and_then(|v| v.as_bool()),
                error: None,
                weight_used: field(data, "weight_used", 2).cloned(),
                data: data.clone(),
            }
        }
        ("messagequeue", "ProcessingFailed") => {
            let origin = field(data, "origin", 1);
            XcmFact {
                side: "received".into(),
                transport: origin.map(transport_for_origin).unwrap_or("unknown").into(),
                message_id: field(data, "id", 0)
                    .and_then(json_h256_hex)
                    .ok_or_else(|| ctx("no id"))
                    .map(Some)?,
                id_kind: "ambiguous".into(),
                counterparty: origin.and_then(origin_counterparty),
                origin_location: origin.cloned(),
                destination: None,
                message: None,
                forwarded: false,
                status: "processing_failed".into(),
                success: Some(false),
                // `ProcessMessageError`: BadFormat | Corrupt | Unsupported |
                // Overweight | Yield | StackLimitReached. `Unsupported` is
                // usually a VERSION fact — XCM v2 is undecodable by
                // staging-xcm >= 17 — not a corruption fact.
                error: field(data, "error", 2).cloned(),
                weight_used: None,
                data: data.clone(),
            }
        }
        // A message too heavy to execute is a real outcome with a real subject.
        // NOTE its `id` is `[u8; 32]` where Processed's is `H256` — one array
        // layer shallower in the decoded JSON, which json_h256_hex absorbs.
        ("messagequeue", "OverweightEnqueued") => {
            let origin = field(data, "origin", 1);
            XcmFact {
                side: "received".into(),
                transport: origin.map(transport_for_origin).unwrap_or("unknown").into(),
                message_id: field(data, "id", 0).and_then(json_h256_hex),
                id_kind: "ambiguous".into(),
                counterparty: origin.and_then(origin_counterparty),
                origin_location: origin.cloned(),
                destination: None,
                message: None,
                forwarded: false,
                status: "overweight_enqueued".into(),
                success: None,
                error: None,
                weight_used: None,
                data: data.clone(),
            }
        }

        // ------------------------------------------------------------- LOCAL
        // `pallet_xcm.execute` — neither half of a journey, and it carries no
        // id, so it can never be correlated. Recorded because "this chain
        // executed an XCM locally" is still an XCM fact.
        //
        // The outcome's SHAPE changes with the runtime's XCM version (v3 tuple
        // variants → v4 named → v5 `Error(InstructionError{index, error})`), so
        // this reads only the VARIANT NAME, which is stable across all three.
        ("polkadotxcm" | "xcmpallet", "Attempted") => {
            let outcome = field(data, "outcome", 0).ok_or_else(|| ctx("no outcome"))?;
            let name = variant_name(outcome).ok_or_else(|| ctx("outcome is not a variant"))?;
            XcmFact {
                side: "local".into(),
                transport: "local".into(),
                message_id: None,
                id_kind: "none".into(),
                counterparty: None,
                origin_location: None,
                destination: None,
                message: None,
                forwarded: false,
                status: "attempted".into(),
                success: Some(name == "Complete"),
                error: (name != "Complete").then(|| outcome.clone()),
                weight_used: None,
                data: data.clone(),
            }
        }

        // --------------------------------------------------- HISTORIC SHAPES
        // Below Asset Hub spec 1_002_000 (cumulus-pallet-xcmp-queue 0.4.0 →
        // 0.5.0, Nov 2023) the queue pallets reported delivery themselves and
        // `messageQueue` did not exist. This is the ONE place both ids appear on
        // the RECEIVING side; the topic wins as the journey key and the wire
        // hash stays in `data`.
        ("xcmpqueue", "Success" | "Fail") => XcmFact {
            side: "received".into(),
            transport: "hrmp".into(),
            message_id: field(data, "message_id", 1)
                .and_then(json_h256_hex)
                .or_else(|| field(data, "message_hash", 0).and_then(json_h256_hex)),
            id_kind: if field(data, "message_id", 1).and_then(json_h256_hex).is_some() {
                "topic".into()
            } else {
                "wire_hash".into()
            },
            counterparty: None,
            origin_location: None,
            destination: None,
            message: None,
            forwarded: false,
            status: "processed".into(),
            success: Some(variant == "Success"),
            error: (variant == "Fail")
                .then(|| field(data, "error", 2).cloned())
                .flatten(),
            weight_used: field(data, "weight", if variant == "Success" { 2 } else { 3 }).cloned(),
            data: data.clone(),
        },
        ("xcmpqueue", "BadVersion" | "BadFormat") => XcmFact {
            side: "received".into(),
            transport: "hrmp".into(),
            message_id: field(data, "message_hash", 0).and_then(json_h256_hex),
            id_kind: "wire_hash".into(),
            counterparty: None,
            origin_location: None,
            destination: None,
            message: None,
            forwarded: false,
            status: "processing_failed".into(),
            success: Some(false),
            error: Some(serde_json::json!(variant)),
            weight_used: None,
            data: data.clone(),
        },
        // Historic DMP. `cumulusXcm`'s three variants are DECLARED in modern
        // runtimes but never deposited (the crate has zero deposit_event calls),
        // so rows from them can only come from a pre-1_002_000 backfill.
        ("cumulusxcm" | "dmpqueue", "ExecutedDownward") => XcmFact {
            side: "received".into(),
            transport: "dmp".into(),
            message_id: field(data, "message_id", 0)
                .or_else(|| field(data, "message_hash", 0))
                .and_then(json_h256_hex),
            id_kind: "wire_hash".into(),
            counterparty: Some("parent".into()),
            origin_location: None,
            destination: None,
            message: None,
            forwarded: false,
            status: "processed".into(),
            success: field(data, "outcome", 1)
                .and_then(variant_name)
                .map(|n| n == "Complete"),
            error: None,
            weight_used: None,
            data: data.clone(),
        },
        ("cumulusxcm" | "dmpqueue", "InvalidFormat" | "UnsupportedVersion") => XcmFact {
            side: "received".into(),
            transport: "dmp".into(),
            message_id: field(data, "message_id", 0)
                .or_else(|| field(data, "message_hash", 0))
                .and_then(json_h256_hex),
            id_kind: "wire_hash".into(),
            counterparty: Some("parent".into()),
            origin_location: None,
            destination: None,
            message: None,
            forwarded: false,
            status: "processing_failed".into(),
            success: Some(false),
            error: Some(serde_json::json!(variant)),
            weight_used: None,
            data: data.clone(),
        },

        // ------------------------------------------------------- DELIBERATE ∅
        // Queue bookkeeping and aggregate counters. `UpwardMessagesReceived`
        // and `DownwardMessagesReceived` carry a COUNT, not a subject: useful
        // later as a reconciliation total against our per-message rows, useless
        // as a journey record, and actively misleading if treated as arrival.
        // The historic xcmpQueue overweight pair names a SENDER and a queue
        // index but no message hash, so there is no subject to record — unlike
        // messageQueue's OverweightEnqueued, which carries an id and is mapped.
        ("messagequeue", "PageReaped")
        | ("xcmpqueue", "OverweightEnqueued" | "OverweightServiced")
        | ("parachainsystem", _)
        | ("parainclusion", _) => return Ok(vec![]),
        // pallet-xcm's query/version/asset bookkeeping. Enumerated rather than
        // wildcarded so that a NEW variant halts loudly: this is the list as of
        // pallet-xcm 28.0.3, and 19.1.0 already inserted two variants mid-enum
        // once.
        (
            "polkadotxcm" | "xcmpallet",
            "UnexpectedResponse"
            | "ResponseReady"
            | "Notified"
            | "NotifyOverweight"
            | "NotifyDispatchError"
            | "NotifyDecodeFailed"
            | "InvalidResponder"
            | "InvalidResponderVersion"
            | "ResponseTaken"
            | "AssetsTrapped"
            | "VersionChangeNotified"
            | "SupportedVersionChanged"
            | "NotifyTargetSendFail"
            | "NotifyTargetMigrationFail"
            | "InvalidQuerierVersion"
            | "InvalidQuerier"
            | "VersionNotifyStarted"
            | "VersionNotifyRequested"
            | "VersionNotifyUnrequested"
            | "FeesPaid"
            | "AssetsClaimed"
            | "VersionMigrationFinished"
            | "ProcessXcmError"
            | "AliasAuthorized"
            | "AliasAuthorizationRemoved"
            | "AliasesAuthorizationsRemoved",
        ) => return Ok(vec![]),

        // Anything else in a pallet we claim to map is a HALT.
        ("polkadotxcm" | "xcmpallet" | "xcmpqueue" | "messagequeue" | "cumulusxcm"
        | "dmpqueue", _) => {
            return Err(format!(
                "unknown XCM event {} — the xcm mapper must be extended and the range re-run \
                 rather than silently miss cross-chain traffic",
                event.name
            ))
        }
        _ => return Ok(vec![]),
    };
    Ok(vec![fact])
}

/// The instruction list inside a decoded `Xcm<Call>`.
///
/// `Xcm` is a NEWTYPE over `Vec<Instruction>`, so the decoder renders it one
/// array deeper than a human writes it — `[[{"WithdrawAsset":…}]]`, and an empty
/// program as `[[]]`, whose outer length is 1 and never 0. Reading `.is_empty()`
/// on the outer array would therefore report EVERY send as pallet-originated and
/// quietly delete the forwarded/originated distinction this module is built on.
/// (Slice 6's lesson, and slice 9 already saw the same layer on `call_hash`.)
///
/// The peel is unambiguous because an XCM instruction is an ENUM VARIANT, which
/// renders as an object and never as a bare array — so a 1-element outer array
/// whose only element is itself an array can only be the newtype wrapper.
fn instructions(message: &serde_json::Value) -> Option<&Vec<serde_json::Value>> {
    let outer = message.as_array()?;
    match outer.as_slice() {
        [inner] if inner.is_array() => inner.as_array(),
        _ => Some(outer),
    }
}

/// `AggregateMessageOrigin` → transport. Parachain side: `Parent` = DMP,
/// `Sibling(id)` = HRMP. Relay side: `Ump(Para(id))` = UMP (its enum has
/// exactly one variant, so every relay MQ message is upward).
fn transport_for_origin(origin: &serde_json::Value) -> &'static str {
    match variant_name(origin) {
        Some("Parent") => "dmp",
        Some("Sibling") => "hrmp",
        Some("Ump") => "ump",
        Some("Here") => "local",
        _ => "unknown",
    }
}

fn origin_counterparty(origin: &serde_json::Value) -> Option<String> {
    match variant_name(origin)? {
        "Parent" => Some("parent".into()),
        // Sibling(ParaId) and Ump(UmpQueueId::Para(ParaId)) — the para id is the
        // only number in either, at different depths.
        "Sibling" | "Ump" => first_number(origin).map(|n| format!("para:{n}")),
        "Here" => Some("here".into()),
        _ => None,
    }
}

/// A destination Location says where, not how — but the parent count settles it:
/// `parents: 1` with no junction is the relay (UMP), `parents: 1` plus a
/// `Parachain` is a sibling (HRMP), and `parents: 0` plus a `Parachain` is a
/// CHILD, i.e. the relay addressing a parachain (DMP). Anything else stays
/// honest at "unknown" rather than guessing a transport.
fn transport_for_destination(dest: &serde_json::Value) -> &'static str {
    let parents = field(dest, "parents", 0).and_then(|v| v.as_u64());
    let has_para = location_counterparty(dest)
        .map(|c| c.starts_with("para:"))
        .unwrap_or(false);
    match (parents, has_para) {
        (Some(1), false) => "ump",
        (Some(1), true) => "hrmp",
        // The relay's own `xcmPallet.Sent` looks exactly like this, and calling
        // it hrmp would mislabel every downward message the relay sends.
        (Some(0), true) => "dmp",
        _ => "unknown",
    }
}

/// "para:2034" | "parent" | None, from a Location.
fn location_counterparty(dest: &serde_json::Value) -> Option<String> {
    if let Some(id) = find_parachain(dest) {
        return Some(format!("para:{id}"));
    }
    let parents = field(dest, "parents", 0).and_then(|v| v.as_u64());
    (parents == Some(1)).then(|| "parent".to_string())
}

/// Walk a decoded Location for a `Parachain` junction, whatever the version
/// wrapper and however many newtype array layers the decoder added.
fn find_parachain(v: &serde_json::Value) -> Option<u64> {
    match v {
        serde_json::Value::Object(map) => {
            for (k, val) in map {
                if k == "Parachain" {
                    return first_number(val);
                }
                if let Some(found) = find_parachain(val) {
                    return Some(found);
                }
            }
            None
        }
        serde_json::Value::Array(items) => items.iter().find_map(find_parachain),
        _ => None,
    }
}

fn first_number(v: &serde_json::Value) -> Option<u64> {
    match v {
        serde_json::Value::Number(n) => n.as_u64(),
        serde_json::Value::String(s) => s.parse().ok(),
        serde_json::Value::Array(items) => items.iter().find_map(first_number),
        serde_json::Value::Object(map) => map.values().find_map(first_number),
        _ => None,
    }
}

/// The single key of a decoded enum variant — `{"Complete": …}` → "Complete".
fn variant_name(v: &serde_json::Value) -> Option<&str> {
    let map = v.as_object()?;
    if map.len() != 1 {
        return None;
    }
    map.keys().next().map(|s| s.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ev(name: &str, data: serde_json::Value) -> CanonicalEvent {
        CanonicalEvent {
            index: 0,
            transaction_index: Some(0),
            name: name.into(),
            data,
        }
    }
    /// `[u8; 32]` as the decoder renders it — a bare array.
    fn bytes32(b: u8) -> serde_json::Value {
        json!(vec![b; 32])
    }
    /// `H256` as the decoder renders it — a NEWTYPE over the array, one layer
    /// deeper. Both must normalise to the same hex or the join is dead.
    fn h256(b: u8) -> serde_json::Value {
        json!([vec![b; 32]])
    }
    fn hex32(b: u8) -> String {
        format!("0x{}", hex::encode([b; 32]))
    }
    fn one(name: &str, data: serde_json::Value) -> XcmFact {
        let f = facts_for_event(&ev(name, data)).expect("maps");
        assert_eq!(f.len(), 1, "{name} must produce exactly one fact");
        f.into_iter().next().unwrap()
    }

    #[test]
    fn the_two_id_shapes_normalise_to_the_same_hex() {
        // THE TRAP THIS PINS: `Sent.message_id` is `XcmHash = [u8;32]` and
        // `Processed.id` is `H256`, a newtype over the same array — so the two
        // sides of the ONLY join this module exists to enable arrive one array
        // layer apart. If these two assertions ever disagree, every cross-chain
        // correlation silently returns nothing.
        let sent = one(
            "polkadotxcm.Sent",
            json!({"origin": {}, "destination": {}, "message": [], "message_id": bytes32(0xab)}),
        );
        let processed = one(
            "messagequeue.Processed",
            json!({"id": h256(0xab), "origin": {"Parent": []}, "weight_used": {}, "success": true}),
        );
        assert_eq!(sent.message_id, Some(hex32(0xab)));
        assert_eq!(processed.message_id, sent.message_id);
        // …but they are NOT the same KIND of id, and the row says so.
        assert_eq!(sent.id_kind, "topic");
        assert_eq!(processed.id_kind, "ambiguous");
    }

    #[test]
    fn a_sent_event_records_the_topic_and_whether_it_was_forwarded() {
        // `Xcm<Call>` is a NEWTYPE over Vec<Instruction>, and v4/v5 `X1` wraps
        // `[Junction; 1]` — so the real decoded shapes are one array deeper than
        // a human writes them. Writing the shallow form here is exactly how
        // slice 6 shipped a dead join, so these fixtures are in the deep form.
        let pallet_originated = one(
            "polkadotxcm.Sent",
            json!({
                "origin": {"parents": 0, "interior": {"Here": []}},
                "destination": {"parents": 1, "interior": {"X1": [[{"Parachain": [[2034]]}]]}},
                "message": [[{"WithdrawAsset": []}]],
                "message_id": bytes32(0x01),
            }),
        );
        assert_eq!(pallet_originated.side, "sent");
        assert_eq!(pallet_originated.transport, "hrmp");
        assert_eq!(pallet_originated.counterparty, Some("para:2034".into()));
        assert!(!pallet_originated.forwarded, "a non-empty message is pallet-originated");

        // An EMPTY program is the executor's deliberate `None`, not corruption —
        // and through the newtype it is `[[]]`, whose OUTER length is 1. Reading
        // the outer array would report every send as pallet-originated.
        let forwarded = one(
            "polkadotxcm.Sent",
            json!({
                "origin": {}, "destination": {"parents": 1, "interior": {"Here": []}},
                "message": [[]], "message_id": bytes32(0x02),
            }),
        );
        assert!(forwarded.forwarded, "[[]] is an empty program, not a one-item one");
        assert_eq!(forwarded.transport, "ump");
        assert_eq!(forwarded.counterparty, Some("parent".into()));
        // …and the un-newtyped spelling must give the same answer, because the
        // decoder's layering is a property of the pipeline, not of XCM.
        assert!(one(
            "polkadotxcm.Sent",
            json!({"origin": {}, "destination": {}, "message": [], "message_id": bytes32(3)}),
        )
        .forwarded);

        // THE RELAY'S OWN SEND: parents 0 + a Parachain junction is a CHILD,
        // i.e. downward. Calling it hrmp would mislabel every DMP the relay
        // sends.
        let downward = one(
            "xcmpallet.Sent",
            json!({
                "origin": {"parents": 0, "interior": {"Here": []}},
                "destination": {"parents": 0, "interior": {"X1": [[{"Parachain": [[1000]]}]]}},
                "message": [[{"Transact": []}]],
                "message_id": bytes32(0x04),
            }),
        );
        assert_eq!(downward.transport, "dmp");
        assert_eq!(downward.counterparty, Some("para:1000".into()));
    }

    #[test]
    fn the_transport_events_are_the_only_record_a_silent_chain_leaves() {
        // Hydration sets `XcmEventEmitter = ()` and emits no Sent for forwarded
        // messages — these two events are all it produces outbound.
        let hrmp = one("xcmpqueue.XcmpMessageSent", json!({"message_hash": bytes32(0x03)}));
        assert_eq!((hrmp.side.as_str(), hrmp.transport.as_str()), ("sent", "hrmp"));
        assert_eq!(hrmp.id_kind, "wire_hash");
        assert_eq!(hrmp.message_id, Some(hex32(0x03)));

        // `Option<XcmHash>` — one wrapping level more than its XCMP sibling.
        let ump = one(
            "parachainsystem.UpwardMessageSent",
            json!({"message_hash": {"Some": [bytes32(0x04)]}}),
        );
        assert_eq!((ump.side.as_str(), ump.transport.as_str()), ("sent", "ump"));
        assert_eq!(ump.message_id, Some(hex32(0x04)));
        assert_eq!(ump.counterparty, Some("parent".into()));
    }

    #[test]
    fn the_receiving_side_reads_its_transport_from_the_queue_origin() {
        let cases = [
            (json!({"Parent": []}), "dmp", Some("parent".to_string())),
            (json!({"Sibling": [2034]}), "hrmp", Some("para:2034".to_string())),
            (json!({"Ump": [{"Para": [2034]}]}), "ump", Some("para:2034".to_string())),
        ];
        for (origin, transport, counterparty) in cases {
            let f = one(
                "messagequeue.Processed",
                json!({"id": h256(0x05), "origin": origin, "weight_used": {"ref_time": 1},
                       "success": true}),
            );
            assert_eq!(f.transport, transport);
            assert_eq!(f.counterparty, counterparty);
            assert_eq!(f.success, Some(true));
        }
    }

    #[test]
    fn processed_false_is_a_result_and_processing_failed_keeps_its_reason() {
        // success:false is Outcome::Incomplete — it ARRIVED and did not finish.
        let incomplete = one(
            "messagequeue.Processed",
            json!({"id": h256(0x06), "origin": {"Parent": []}, "weight_used": {},
                   "success": false}),
        );
        assert_eq!(incomplete.status, "processed");
        assert_eq!(incomplete.success, Some(false));

        // Unsupported is usually a VERSION fact (XCM v2 into a modern runtime),
        // not a corruption fact — so the reason is kept, not flattened.
        let failed = one(
            "messagequeue.ProcessingFailed",
            json!({"id": h256(0x07), "origin": {"Sibling": [1000]},
                   "error": {"Unsupported": []}}),
        );
        assert_eq!(failed.status, "processing_failed");
        assert_eq!(failed.success, Some(false));
        assert_eq!(failed.error, Some(json!({"Unsupported": []})));

        // An id we cannot read is a HALT: a receiving half with no subject
        // cannot be joined to anything and must not be filed as if it could.
        assert!(facts_for_event(&ev(
            "messagequeue.Processed",
            json!({"origin": {"Parent": []}, "success": true})
        ))
        .is_err());
    }

    #[test]
    fn attempted_reads_only_the_variant_name_so_every_xcm_version_works() {
        // v4-shaped (named fields) and v5-shaped (Error is a newtype again).
        for (outcome, ok) in [
            (json!({"Complete": {"used": {"ref_time": 1}}}), true),
            (json!({"Incomplete": {"used": {}, "error": {"Overflow": []}}}), false),
            (json!({"Error": [{"index": 0, "error": {"Barrier": []}}]}), false),
            // v3-shaped tuple variant — same names, different bodies.
            (json!({"Complete": [{"ref_time": 1}]}), true),
        ] {
            let f = one("polkadotxcm.Attempted", json!({"outcome": outcome}));
            assert_eq!(f.side, "local", "a local execute is neither half of a journey");
            assert_eq!(f.id_kind, "none");
            assert_eq!(f.message_id, None);
            assert_eq!(f.success, Some(ok));
        }
    }

    #[test]
    fn the_historic_queue_events_carry_both_ids_and_the_topic_wins() {
        // Below AH spec 1_002_000 the receiving side reported BOTH — the only
        // place in the whole vocabulary where that happens.
        let f = one(
            "xcmpqueue.Success",
            json!({"message_hash": bytes32(0x08), "message_id": bytes32(0x09),
                   "weight": {"ref_time": 5}}),
        );
        assert_eq!(f.message_id, Some(hex32(0x09)), "the topic is the journey key");
        assert_eq!(f.id_kind, "topic");
        assert_eq!(f.success, Some(true));
        // and the hash is not lost — schema-on-read keeps the whole event
        assert_eq!(f.data["message_hash"], bytes32(0x08));

        let f = one("xcmpqueue.Fail", json!({"message_hash": bytes32(0x0a), "error": {"Barrier": []}}));
        assert_eq!(f.id_kind, "wire_hash", "no topic in this one, and it says so");
        assert_eq!(f.success, Some(false));
    }

    #[test]
    fn bookkeeping_is_nothing_and_an_unknown_xcm_event_is_loud() {
        for (name, data) in [
            ("messagequeue.PageReaped", json!({"origin": {"Parent": []}, "index": 1})),
            ("parachainsystem.DownwardMessagesReceived", json!({"count": 3})),
            ("parainclusion.UpwardMessagesReceived", json!({"from": 2034, "count": 2})),
            ("polkadotxcm.FeesPaid", json!({"paying": {}, "fees": []})),
            ("polkadotxcm.AssetsTrapped", json!({"hash": bytes32(1), "origin": {}, "assets": []})),
            ("balances.Transfer", json!({})),
        ] {
            assert!(
                facts_for_event(&ev(name, data)).unwrap().is_empty(),
                "{name} must map to nothing"
            );
        }
        // A variant we have never seen inside a pallet we claim to map halts —
        // a runtime upgrade adding one must not quietly shrink our coverage.
        let err = facts_for_event(&ev("polkadotxcm.SomethingNew", json!({}))).unwrap_err();
        assert!(err.contains("unknown XCM event"), "{err}");
        assert!(facts_for_event(&ev("messagequeue.Whatever", json!({}))).is_err());
    }
}
