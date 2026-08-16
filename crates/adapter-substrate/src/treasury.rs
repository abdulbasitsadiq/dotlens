//! Treasury semantics of the Substrate family (Invariant 4: protocol specifics
//! live in adapters only).
//!
//! `SubstrateTreasuryMapper` — canonical pallet-treasury events → spend facts
//! and pot flows. Instances are recognized by pallet name (the decoder
//! lowercases), so registering another treasury-bearing chain needs ZERO code:
//!   treasury            → instance "treasury"             (Asset Hub; relay pre-2025-11-04)
//!   fellowshiptreasury  → instance "fellowship_treasury"   (Collectives)
//!   ambassadortreasury  → instance "ambassador_treasury"   (Collectives)
//!
//! TWO GENERATIONS, both mapped (read from pallet-treasury 4.0.0 / 28.0.0 /
//! 48.0.0 on crates.io — the vocabulary SHRANK, which is unusual and matters):
//!
//!   LEGACY proposal flow, id space `proposal_index`:
//!     Proposed{proposal_index}                       → proposed   (status proposed)
//!     SpendApproved{proposal_index,amount,beneficiary} → approved (status approved)
//!     Awarded{proposal_index,award,account}          → awarded    (status awarded)
//!     Rejected{proposal_index,slashed}               → rejected   (status rejected)
//!   `Proposed` and `Rejected` were REMOVED in v35 (reviewer diffed all 67
//!   published versions). They appear only in relay-era history — which
//!   dotlens indexes, so they are mapped, and a modern runtime never emits
//!   them. Field names and field ORDER are otherwise unchanged 4.0.0 → 48.0.0,
//!   so the positional fallbacks below are safe across the whole range.
//!
//!   MODERN asset-spend flow, id space `SpendIndex` (DISJOINT from the above —
//!   hence `spend_kind`):
//!     AssetSpendApproved{index,asset_kind,amount,beneficiary,valid_from,expire_at}
//!                                                    → approved   (status approved)
//!     Paid{index,payment_id}                         → paid       (status paid)
//!     PaymentFailed{index,payment_id}                → payment_failed
//!     SpendProcessed{index}                          → processed
//!     AssetSpendVoided{index}                        → voided
//!   `valid_from` and `expire_at` are in the units of the pallet's OWN block
//!   number provider, which on Asset Hub is the RELAY chain, not Asset Hub
//!   (verified 2026-08-16: AH block 17523677 has relay_parent_number 31860850,
//!   which is exactly the valid_from of the twelve spends approved in it; and
//!   spend 265's payout at AH #18508317 would have failed EarlyPayout had its
//!   valid_from of 32109039 been an AH height). We store what the event said
//!   and name no clock — but nobody may compare these against a block height
//!   without first asking which chain's clock it is. Note also that `payout()`
//!   RE-ARMS the stored expire_at to `now + PayoutPeriod` on each attempt, so
//!   our approval-time value legitimately differs from live storage for any
//!   spend that has been paid. Ours is the event's truth.
//!
//!   `SpendProcessed` does NOT mean "paid": the pallet emits it both when a
//!   payment concluded and when the spend EXPIRED unclaimed. The payout truth
//!   is the `paid` row and its payment_id; the projection says `processed` and
//!   nothing more. `PaymentFailed` is retryable, so a later `paid` legitimately
//!   supersedes it — which the ordering guard already handles.
//!
//!   POT EVENTS, no subject: Spending{budget_remaining}, Burnt{burnt_funds},
//!   Rollover{rollover_balance}, Deposit{value}, UpdatedInactive{reactivated,
//!   deactivated}. Recorded with attribution "pot", never projected.
//!   UpdatedInactive carries TWO figures, so `amount` stays NULL and both live
//!   in `data` — no picking one and calling it "the" amount.
//!   AND THEY ARE NOT ALL FLOWS (reviewer catch, read from `spend_funds()`):
//!   Deposit and Burnt are money MOVING (figure_kind "flow"), while Spending
//!   and Rollover report the pot BALANCE at the start and end of a spend
//!   period (figure_kind "snapshot"). Summing snapshots as flows double-counts
//!   the whole pot twice per period.
//!
//! UNKNOWN events of a known treasury instance are ERRORS: a runtime upgrade
//! that adds a money-moving event must halt the mapper loudly.
//!
//! KNOWN-UNMAPPED, stated rather than implied:
//!   * BOUNTIES — Asset Hub runs THREE bounty pallets (`bounties`,
//!     `childbounties`, and `multiassetbounties`, the newer asset-denominated
//!     one that referendum 1930 funds). Separate vocabulary, separate
//!     lifecycle, own slice. Note that bounty funding leaves the pot through
//!     the `SpendFunds` HOOK — it emits `bounties.BountyBecameActive`, not
//!     `treasury.Awarded` and not a pot event — so that outflow is invisible
//!     to these tables entirely, not merely unlabelled.
//!   * SUBSTRATE 2.x TREASURY (`NewTip`, `TipClosing`, `TipClosed`,
//!     `TipRetracted`, and the seven `Bounty*` variants) — tips and bounties
//!     lived INSIDE pallet-treasury until 3.0.0. Those runtimes carry metadata
//!     v11/v12, below the v14 floor our decoder enforces, so today these can
//!     never reach the mapper. They WILL the moment a pre-v14 decoding slice
//!     lands, and they would halt it loudly — which is correct, but it should
//!     be a planned halt, not a mid-backfill surprise.
//!
//! TREASURY_MAPPER_VERSION is lineage: bump on ANY rule change.

use canonical::CanonicalEvent;
use ingest::treasury::{SpendFact, TreasuryMapper};

pub const TREASURY_MAPPER_VERSION: u32 = 1;

pub struct SubstrateTreasuryMapper;

impl TreasuryMapper for SubstrateTreasuryMapper {
    fn facts(&self, event: &CanonicalEvent) -> Result<Vec<SpendFact>, String> {
        facts_for_event(event)
    }
    fn mapper_version(&self) -> u32 {
        TREASURY_MAPPER_VERSION
    }
}

/// Treasury instance pallets (decoder-lowercased) → instance name.
fn instance_of(pallet: &str) -> Option<&'static str> {
    match pallet {
        "treasury" => Some("treasury"),
        "fellowshiptreasury" => Some("fellowship_treasury"),
        "ambassadortreasury" => Some("ambassador_treasury"),
        _ => None,
    }
}

const KIND_PROPOSAL: &str = "proposal";
const KIND_ASSET_SPEND: &str = "asset_spend";

/// The pure mapping. Non-treasury events map to ∅; malformed treasury events
/// are ERRORS (money must never silently drop).
pub fn facts_for_event(event: &CanonicalEvent) -> Result<Vec<SpendFact>, String> {
    let Some((pallet, variant)) = event.name.split_once('.') else {
        return Ok(vec![]);
    };
    let Some(instance) = instance_of(pallet) else {
        return Ok(vec![]);
    };
    let data = &event.data;
    let ctx = |what: &str| format!("{}: {what} (data: {data})", event.name);

    // a spend fact with everything empty; each arm fills what its event carries
    let base = |kind: &str, status: Option<&str>| SpendFact {
        instance: instance.to_string(),
        spend_kind: None,
        spend_id: None,
        kind: kind.to_string(),
        status: status.map(str::to_string),
        amount: None,
        // set only where an amount exists — it describes THAT number
        figure_kind: None,
        slashed: None,
        asset_kind: None,
        beneficiary: None,
        beneficiary_location: None,
        payment_id: None,
        valid_from: None,
        expire_at: None,
        attribution: "event".to_string(),
        data: data.clone(),
    };
    // `figure` says what the number MEANS: a flow moved money, a snapshot
    // reports the pot's balance at a moment in the spend period.
    let pot = |kind: &str, amount: Option<u128>, figure: Option<&str>| SpendFact {
        attribution: "pot".to_string(),
        amount,
        figure_kind: figure.map(str::to_string),
        ..base(kind, None)
    };

    let fact = match variant {
        // ---------------------------------------------- legacy proposal flow
        "Proposed" => SpendFact {
            spend_kind: Some(KIND_PROPOSAL.into()),
            spend_id: Some(field_u64(data, "proposal_index", 0).ok_or_else(|| ctx("no proposal_index"))?),
            ..base("proposed", Some("proposed"))
        },
        "SpendApproved" => SpendFact {
            spend_kind: Some(KIND_PROPOSAL.into()),
            spend_id: Some(field_u64(data, "proposal_index", 0).ok_or_else(|| ctx("no proposal_index"))?),
            amount: Some(field_u128(data, "amount", 1).ok_or_else(|| ctx("no amount"))?),
            figure_kind: Some("flow".into()),
            beneficiary: field_account(data, "beneficiary", 2).map(|a| a.to_vec()),
            ..base("approved", Some("approved"))
        },
        "Awarded" => SpendFact {
            spend_kind: Some(KIND_PROPOSAL.into()),
            spend_id: Some(field_u64(data, "proposal_index", 0).ok_or_else(|| ctx("no proposal_index"))?),
            amount: Some(field_u128(data, "award", 1).ok_or_else(|| ctx("no award"))?),
            figure_kind: Some("flow".into()),
            // the field is `account` here, not `beneficiary` (pallet naming)
            beneficiary: field_account(data, "account", 2).map(|a| a.to_vec()),
            ..base("awarded", Some("awarded"))
        },
        "Rejected" => SpendFact {
            spend_kind: Some(KIND_PROPOSAL.into()),
            spend_id: Some(field_u64(data, "proposal_index", 0).ok_or_else(|| ctx("no proposal_index"))?),
            // NOT a spend: this is the proposer's bond being slashed
            slashed: field_u128(data, "slashed", 1),
            ..base("rejected", Some("rejected"))
        },

        // ------------------------------------------------ modern spend flow
        "AssetSpendApproved" => {
            let beneficiary_raw = field(data, "beneficiary", 3).cloned();
            SpendFact {
                spend_kind: Some(KIND_ASSET_SPEND.into()),
                spend_id: Some(field_u64(data, "index", 0).ok_or_else(|| ctx("no index"))?),
                // MANDATORY in every version that has this event. Absent
                // means our decode is wrong, and a silent None would read as
                // "native DOT" — turning an 83,760 USDT spend into 83,760
                // planck of DOT. Halt instead (reviewer catch: the two fields
                // that cannot be wrong were guarded and the one that would be
                // catastrophically wrong was not).
                asset_kind: Some(
                    field(data, "asset_kind", 1)
                        .cloned()
                        .ok_or_else(|| ctx("no asset_kind"))?,
                ),
                amount: Some(field_u128(data, "amount", 2).ok_or_else(|| ctx("no amount"))?),
                figure_kind: Some("flow".into()),
                // the beneficiary is a LOCATION here; extract an account only
                // if the location actually names one
                beneficiary: beneficiary_raw.as_ref().and_then(account_in_location),
                beneficiary_location: beneficiary_raw,
                valid_from: field_u64(data, "valid_from", 4),
                expire_at: field_u64(data, "expire_at", 5),
                ..base("approved", Some("approved"))
            }
        }
        "Paid" => SpendFact {
            spend_kind: Some(KIND_ASSET_SPEND.into()),
            spend_id: Some(field_u64(data, "index", 0).ok_or_else(|| ctx("no index"))?),
            payment_id: field(data, "payment_id", 1).map(json_scalar_string),
            ..base("paid", Some("paid"))
        },
        "PaymentFailed" => SpendFact {
            spend_kind: Some(KIND_ASSET_SPEND.into()),
            spend_id: Some(field_u64(data, "index", 0).ok_or_else(|| ctx("no index"))?),
            payment_id: field(data, "payment_id", 1).map(json_scalar_string),
            ..base("payment_failed", Some("payment_failed"))
        },
        // deliberately NOT "paid": the pallet emits this both for a concluded
        // payment and for a spend that EXPIRED unclaimed
        "SpendProcessed" => SpendFact {
            spend_kind: Some(KIND_ASSET_SPEND.into()),
            spend_id: Some(field_u64(data, "index", 0).ok_or_else(|| ctx("no index"))?),
            ..base("processed", Some("processed"))
        },
        "AssetSpendVoided" => SpendFact {
            spend_kind: Some(KIND_ASSET_SPEND.into()),
            spend_id: Some(field_u64(data, "index", 0).ok_or_else(|| ctx("no index"))?),
            ..base("voided", Some("voided"))
        },

        // ------------------------------------------------------- pot events
        // real flows: money in and money destroyed. Their figures are as
        // load-bearing as a spend's, so a missing one halts, exactly like the
        // spend arms above (reviewer catch: these used to fail silently).
        "Deposit" => pot(
            "pot_deposit",
            Some(field_u128(data, "value", 0).ok_or_else(|| ctx("no value"))?),
            Some("flow"),
        ),
        "Burnt" => pot(
            "pot_burnt",
            Some(field_u128(data, "burnt_funds", 0).ok_or_else(|| ctx("no burnt_funds"))?),
            Some("flow"),
        ),
        // BALANCE SNAPSHOTS, not flows: the pot at the start and the end of a
        // spend period. Summing these as spending double-counts the treasury.
        "Spending" => pot(
            "pot_spending",
            field_u128(data, "budget_remaining", 0),
            Some("snapshot"),
        ),
        "Rollover" => pot(
            "pot_rollover",
            field_u128(data, "rollover_balance", 0),
            Some("snapshot"),
        ),
        // two figures, so no single `amount` — both stay in `data`
        "UpdatedInactive" => pot("pot_inactive_updated", None, None),

        _ => {
            return Err(format!(
                "unknown treasury event {} — treasury mapper update required",
                event.name
            ))
        }
    };
    Ok(vec![fact])
}

// ------------------------------------------------------- JSON field plumbing
// Event data is schema-on-read JSON written by our own decoders: named fields
// as objects, positional as arrays; AccountId32 as (nested) byte arrays or
// SS58 strings (decoder v1); u128 as numbers when small, decimal strings when big.

fn field<'a>(data: &'a serde_json::Value, name: &str, index: usize) -> Option<&'a serde_json::Value> {
    match data {
        serde_json::Value::Object(map) => map.get(name),
        serde_json::Value::Array(items) => items.get(index),
        _ => None,
    }
}

fn json_u128(v: &serde_json::Value) -> Option<u128> {
    match v {
        serde_json::Value::Number(n) => n.as_u64().map(u128::from),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn field_u128(data: &serde_json::Value, name: &str, index: usize) -> Option<u128> {
    field(data, name, index).and_then(json_u128)
}

fn field_u64(data: &serde_json::Value, name: &str, index: usize) -> Option<u64> {
    field(data, name, index).and_then(json_u128).and_then(|n| u64::try_from(n).ok())
}

fn json_account_bytes(v: &serde_json::Value) -> Option<[u8; 32]> {
    if let serde_json::Value::String(s) = v {
        return crate::accounts::parse_account(s).ok();
    }
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
    if walk(v, &mut out) {
        <[u8; 32]>::try_from(out.as_slice()).ok()
    } else {
        None
    }
}

fn field_account(data: &serde_json::Value, name: &str, index: usize) -> Option<[u8; 32]> {
    field(data, name, index).and_then(json_account_bytes)
}

/// Pull a 32-byte account out of a VersionedLocation, if it names one.
///
/// Modern spends pay a LOCATION, and the exact type differs BY CHAIN (decoded
/// from the committed metadata fixtures at spec 2003002): the relay uses
/// `VersionedLocation`, while Asset Hub — where the treasury actually lives —
/// uses `VersionedLocatableAccount { location, account_id }`, TWO locations,
/// because the payee may sit on a different chain than the payer.
///
/// So: prefer the named `account_id` sub-field when present (the payee), and
/// never let the sibling `location` (the chain) be mistaken for it — a
/// reviewer catch: the generic walk below happened to find the right answer
/// only because serde_json's Map is a BTreeMap and "account_id" sorts before
/// "location". Then look for the `AccountId32` junction by name — XCM's own
/// vocabulary, not a guess. A location naming no account (a parachain, a
/// pallet instance, a 20-byte key) yields None, and the raw form is stored
/// regardless.
///
/// Junction shapes seen in the real metadata: `AccountId32 { network:
/// Option<NetworkId>, id: [u8;32] }` — so `network` renders as `{"None":[]}`,
/// not JSON null — and v4/v5 `X1` wraps `[Junction; 1]`, so the junction sits
/// one array deeper than the v3 shape.
fn account_in_location(v: &serde_json::Value) -> Option<Vec<u8>> {
    match v {
        serde_json::Value::Object(map) => {
            // VersionedLocatableAccount: the payee is account_id, never location
            if let Some(a) = map.get("account_id") {
                if let Some(bytes) = account_in_location(a) {
                    return Some(bytes);
                }
            }
            if let Some(j) = map.get("AccountId32") {
                if let Some(id) = field(j, "id", 0) {
                    if let Some(bytes) = json_account_bytes(id) {
                        return Some(bytes.to_vec());
                    }
                }
            }
            map.values().find_map(account_in_location)
        }
        serde_json::Value::Array(items) => items.iter().find_map(account_in_location),
        _ => None,
    }
}

/// A payment id is an opaque `Pay::Id` — a number for most paymasters, a
/// composite for some. Render it as a stable string rather than pretending it
/// is always an integer.
fn json_scalar_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounts::{pallet_account, para_sovereign};

    fn ev(name: &str, data: serde_json::Value) -> CanonicalEvent {
        CanonicalEvent {
            index: 0,
            transaction_index: Some(0),
            name: name.into(),
            data,
        }
    }

    fn acct_json(a: &[u8; 32]) -> serde_json::Value {
        serde_json::json!([a.to_vec()])
    }

    fn one(f: Vec<SpendFact>) -> SpendFact {
        f.into_iter().next().expect("one fact")
    }

    #[test]
    fn legacy_proposal_flow_maps_with_its_own_id_space() {
        let who = para_sovereign(2034);
        let proposed = one(facts_for_event(&ev(
            "treasury.Proposed",
            serde_json::json!({"proposal_index": 42}),
        ))
        .unwrap());
        assert_eq!(proposed.spend_kind.as_deref(), Some("proposal"));
        assert_eq!((proposed.spend_id, proposed.status.as_deref()), (Some(42), Some("proposed")));

        let approved = one(facts_for_event(&ev(
            "treasury.SpendApproved",
            serde_json::json!({
                "proposal_index": 42, "amount": "12000000000000", "beneficiary": acct_json(&who)
            }),
        ))
        .unwrap());
        assert_eq!(approved.amount, Some(12_000_000_000_000));
        assert_eq!(approved.beneficiary.as_deref(), Some(&who[..]));
        assert_eq!(approved.asset_kind, None, "the legacy flow is always native");

        let awarded = one(facts_for_event(&ev(
            "treasury.Awarded",
            serde_json::json!({"proposal_index": 42, "award": 500, "account": acct_json(&who)}),
        ))
        .unwrap());
        assert_eq!((awarded.kind.as_str(), awarded.amount), ("awarded", Some(500)));
        assert_eq!(awarded.beneficiary.as_deref(), Some(&who[..]));
    }

    #[test]
    fn rejected_records_the_bond_as_slashed_never_as_spending() {
        let r = one(facts_for_event(&ev(
            "treasury.Rejected",
            serde_json::json!({"proposal_index": 7, "slashed": 1000}),
        ))
        .unwrap());
        assert_eq!(r.status.as_deref(), Some("rejected"));
        assert_eq!(r.slashed, Some(1000));
        assert_eq!(r.amount, None, "a slashed bond is not treasury spending");
    }

    #[test]
    fn asset_spend_carries_asset_kind_and_a_location_beneficiary() {
        let who = pallet_account(b"py/trsry");
        // the real shape: USDT (asset 1984) on Asset Hub, paid to an account
        // named inside a VersionedLocation
        let e = ev(
            "treasury.AssetSpendApproved",
            serde_json::json!({
                "index": 313,
                "asset_kind": {"V4": {
                    "location": {"parents": 0, "interior": {"X1": [{"Parachain": 1000}]}},
                    "asset_id": {"parents": 0, "interior": {"X2": [
                        {"PalletInstance": 50}, {"GeneralIndex": 1984}
                    ]}}
                }},
                "amount": "83760000000",
                "beneficiary": {"V4": {"parents": 0, "interior": {"X1": [
                    {"AccountId32": {"network": null, "id": who.to_vec()}}
                ]}}},
                "valid_from": 19_000_000,
                "expire_at": 19_500_000
            }),
        );
        let s = one(facts_for_event(&e).unwrap());
        assert_eq!(s.spend_kind.as_deref(), Some("asset_spend"));
        assert_eq!((s.spend_id, s.amount), (Some(313), Some(83_760_000_000)));
        assert_eq!(s.status.as_deref(), Some("approved"));
        assert!(s.asset_kind.is_some(), "asset kind kept whole, schema-on-read");
        assert_eq!(s.beneficiary.as_deref(), Some(&who[..]), "AccountId32 junction extracted");
        assert!(s.beneficiary_location.is_some(), "and the raw location is kept");
        assert_eq!((s.valid_from, s.expire_at), (Some(19_000_000), Some(19_500_000)));
    }

    #[test]
    fn the_real_asset_hub_beneficiary_shape_resolves_to_the_payee() {
        // THE production shape, decoded from the committed AH metadata:
        // `VersionedLocatableAccount { location, account_id }` — TWO locations,
        // v5 `X1` wrapping `[Junction; 1]` (so the junction is one array
        // deeper), and `network: Option<NetworkId>` rendering as {"None":[]}.
        // The payee is account_id; `location` names the CHAIN and must never
        // be mistaken for it.
        let payee = para_sovereign(2034);
        let e = ev(
            "treasury.AssetSpendApproved",
            serde_json::json!({
                "index": 400,
                "asset_kind": {"V5": {"location": {"parents": 1, "interior": {"X1": [[
                    {"Parachain": 1000}
                ]]}}, "asset_id": {"parents": 0, "interior": "Here"}}},
                "amount": 42,
                "beneficiary": {"V5": {
                    "location": {"parents": 1, "interior": {"X1": [[{"Parachain": 1000}]]}},
                    "account_id": {"parents": 0, "interior": {"X1": [[
                        {"AccountId32": {"network": {"None": []}, "id": payee.to_vec()}}
                    ]]}}
                }},
                "valid_from": 1, "expire_at": 2
            }),
        );
        let s = one(facts_for_event(&e).unwrap());
        assert_eq!(s.beneficiary.as_deref(), Some(&payee[..]));
        assert!(s.beneficiary_location.is_some());
    }

    #[test]
    fn a_missing_asset_kind_halts_instead_of_reading_as_native_dot() {
        // asset_kind is mandatory in every version that has this event; a
        // silent None would turn a USDT spend into a DOT spend
        let e = ev(
            "treasury.AssetSpendApproved",
            serde_json::json!({"index": 1, "amount": 10, "beneficiary": {"V4": {}}}),
        );
        assert!(facts_for_event(&e).is_err());
    }

    #[test]
    fn a_location_naming_no_account_yields_no_beneficiary_bytes() {
        let e = ev(
            "treasury.AssetSpendApproved",
            serde_json::json!({
                "index": 9, "asset_kind": {"V4": {}}, "amount": 1,
                "beneficiary": {"V4": {"parents": 1, "interior": {"X1": [{"Parachain": 2034}]}}}
            }),
        );
        let s = one(facts_for_event(&e).unwrap());
        assert_eq!(s.beneficiary, None, "no account named — not invented");
        assert!(s.beneficiary_location.is_some());
    }

    #[test]
    fn payment_lifecycle_and_the_processed_caveat() {
        let paid = one(facts_for_event(&ev(
            "treasury.Paid",
            serde_json::json!({"index": 313, "payment_id": 5551}),
        ))
        .unwrap());
        assert_eq!((paid.kind.as_str(), paid.payment_id.as_deref()), ("paid", Some("5551")));

        let failed = one(facts_for_event(&ev(
            "treasury.PaymentFailed",
            serde_json::json!({"index": 313, "payment_id": 5551}),
        ))
        .unwrap());
        assert_eq!(failed.status.as_deref(), Some("payment_failed"));

        // processed means "removed from storage" — paid OR expired
        let processed = one(facts_for_event(&ev(
            "treasury.SpendProcessed",
            serde_json::json!({"index": 313}),
        ))
        .unwrap());
        assert_eq!(processed.status.as_deref(), Some("processed"));
        assert_eq!(processed.amount, None);

        let voided = one(facts_for_event(&ev(
            "treasury.AssetSpendVoided",
            serde_json::json!({"index": 313}),
        ))
        .unwrap());
        assert_eq!(voided.status.as_deref(), Some("voided"));
    }

    #[test]
    fn pot_flows_are_recorded_but_never_projected() {
        // the fourth column is figure_kind: a FLOW is money moving, a
        // SNAPSHOT is the pot's balance reported at a point in the period.
        // Summing snapshots as flows double-counts the treasury twice a period.
        let cases: &[(&str, &str, Option<u128>, Option<&str>, serde_json::Value)] = &[
            ("Deposit", "pot_deposit", Some(700), Some("flow"),
                serde_json::json!({"value": 700})),
            ("Burnt", "pot_burnt", Some(11), Some("flow"),
                serde_json::json!({"burnt_funds": 11})),
            ("Spending", "pot_spending", Some(90), Some("snapshot"),
                serde_json::json!({"budget_remaining": 90})),
            ("Rollover", "pot_rollover", Some(5), Some("snapshot"),
                serde_json::json!({"rollover_balance": 5})),
            ("UpdatedInactive", "pot_inactive_updated", None, None,
                serde_json::json!({"reactivated": 1, "deactivated": 2})),
        ];
        for (variant, kind, amount, figure, data) in cases {
            let f = one(facts_for_event(&ev(&format!("treasury.{variant}"), data.clone())).unwrap());
            assert_eq!(f.kind.as_str(), *kind);
            assert_eq!(f.amount, *amount, "{variant}");
            assert_eq!(f.figure_kind.as_deref(), *figure, "{variant}");
            assert_eq!(f.attribution, "pot");
            assert_eq!(f.spend_id, None, "{variant} names no spend");
            assert_eq!(f.status, None, "{variant} must not move any projection");
        }
        // a real flow with no figure is a decode failure, not a zero
        assert!(facts_for_event(&ev("treasury.Deposit", serde_json::json!({}))).is_err());
        assert!(facts_for_event(&ev("treasury.Burnt", serde_json::json!({}))).is_err());
    }

    #[test]
    fn instances_map_by_pallet_name_so_collectives_needs_no_code() {
        let f = one(facts_for_event(&ev(
            "fellowshiptreasury.AssetSpendApproved",
            serde_json::json!({"index": 1, "asset_kind": {}, "amount": 10}),
        ))
        .unwrap());
        assert_eq!(f.instance, "fellowship_treasury");
        let a = one(facts_for_event(&ev(
            "ambassadortreasury.Deposit",
            serde_json::json!({"value": 1}),
        ))
        .unwrap());
        assert_eq!(a.instance, "ambassador_treasury");
    }

    #[test]
    fn positional_event_data_works_for_the_oldest_runtimes() {
        // pre-FRAMEv2 relay events decode as positional arrays
        let who = para_sovereign(1000);
        let f = one(facts_for_event(&ev(
            "treasury.Awarded",
            serde_json::json!([12, 34, acct_json(&who)]),
        ))
        .unwrap());
        assert_eq!((f.spend_id, f.amount), (Some(12), Some(34)));
        assert_eq!(f.beneficiary.as_deref(), Some(&who[..]));
    }

    #[test]
    fn other_pallets_and_bounties_map_to_nothing_unknown_treasury_events_are_loud() {
        // bounties are a KNOWN-UNMAPPED vocabulary (own slice), not an error
        for name in [
            "balances.Transfer",
            "bounties.BountyAwarded",
            "childbounties.Added",
            "multiassetbounties.BountyCreated",
        ] {
            let e = ev(name, serde_json::json!({"index": 1}));
            assert!(facts_for_event(&e).unwrap().is_empty(), "{name} must map to ∅");
        }
        // …but an unknown event of a mapped instance halts loudly
        let unknown = ev("treasury.SomeFutureEvent", serde_json::json!({"index": 1}));
        assert!(facts_for_event(&unknown).is_err());
    }

    #[test]
    fn malformed_treasury_events_are_loud_errors() {
        assert!(facts_for_event(&ev("treasury.Proposed", serde_json::json!({}))).is_err());
        assert!(facts_for_event(&ev(
            "treasury.SpendApproved",
            serde_json::json!({"proposal_index": 1})
        ))
        .is_err());
        assert!(facts_for_event(&ev("treasury.Paid", serde_json::json!({"payment_id": 1}))).is_err());
    }
}
