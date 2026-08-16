//! Balances semantics of the Substrate family (Invariant 4: protocol
//! specifics live in adapters only). Two pure pieces:
//!
//! 1. `SubstrateDeltaMapper` — canonical balances-pallet events → TOTAL-balance
//!    deltas (free + reserved). The rules:
//!      Transfer            → -amount(from), +amount(to)   [self-transfer → ∅]
//!      Deposit/Minted/
//!      Restored            → +amount(who)
//!      Withdraw/Burned/
//!      Slashed/Suspended   → -amount(who)
//!      DustLost            → -amount(account)
//!      ReserveRepatriated  → -amount(from), +amount(to)
//!      BurnedHeld          → -amount(who)          [held funds sit in reserved]
//!      TransferOnHold      → -amount(source), +amount(dest)
//!      TransferAndHold     → -transferred(source), +transferred(dest)
//!      BalanceSet          → zero-magnitude MARKER (absolute value, no delta
//!                            derivable — running totals invalidate until the
//!                            next anchor)
//!    Deliberately NO delta (total unchanged, or double-count):
//!      Endowed (funds arrive via the paired Transfer/Deposit/Minted),
//!      Reserved/Unreserved/Held/Released/Frozen/Thawed/Locked/Unlocked (intra-account),
//!      Issued/Rescinded/MintedCredit/BurnedDebt/TotalIssuanceForced (issuance,
//!      no account), Upgraded, Unexpected (defensive, moves nothing).
//!    COVERAGE is 100% of the 30 event variants pallet-balances declares in the
//!    Polkadot/Asset Hub runtime at spec 2003002 (enumerated from the runtime's
//!    own metadata, not from a hand list) — so the loud-halt arm below is
//!    unreachable until a runtime upgrade adds a variant, which is the point.
//!    UNKNOWN balances.* events are ERRORS — a runtime upgrade adding a
//!    total-moving event must halt the mapper loudly, never drop money.
//!    Fees are covered by Withdraw + Deposit events; XCM teleports by
//!    Burned/Minted — no special cases needed.
//!
//! 2. `decode_account_info` — raw System.Account storage value → absolute
//!    free/reserved balances, decoded against block-correct metadata. Anchors
//!    for history reconstruction (incl. across the Nov 2025 migration, whose
//!    bulk moves emitted no ordinary transfer events).
//!
//! MAPPER_VERSION is lineage: bump on ANY rule change; rows rebuild from
//! canonical events.

use canonical::CanonicalEvent;
use frame_metadata::{RuntimeMetadata, RuntimeMetadataPrefixed};
use ingest::balances::{BalanceDelta, DeltaMapper};
use parity_scale_codec::Decode;
use scale_value::{Composite, Value, ValueDef};

/// Lineage for every row in `balances.balance_changes`. Bump on ANY rule
/// change; rows rebuild from canonical events.
///
/// 1 → 2 (Phase 2, slice 6): this mapper now also carries the ASSETS pallets.
/// The rules for `balances.*` did not change by a single byte, but the mapper's
/// COVERAGE did, and a version that only moved when the native rules moved
/// would say the pre-slice-6 rows are equivalent to today's. They are not:
/// they are missing every USDT and USDC movement in their range. A rebuild is
/// `balances-range` over the range again, and the version column is what tells
/// you which ranges still need it.
pub const MAPPER_VERSION: u32 = 2;

pub struct SubstrateDeltaMapper;

impl DeltaMapper for SubstrateDeltaMapper {
    fn deltas(&self, event: &CanonicalEvent) -> Result<Vec<BalanceDelta>, String> {
        deltas_for_event(event)
    }
    fn mapper_version(&self) -> u32 {
        MAPPER_VERSION
    }
}

/// The pure mapping. Non-balances events map to ∅; malformed balances events
/// are ERRORS (money must never silently drop).
pub fn deltas_for_event(event: &CanonicalEvent) -> Result<Vec<BalanceDelta>, String> {
    let d = |account: [u8; 32], magnitude: u128, negative: bool, reason: &str,
             counterparty: Option<[u8; 32]>| BalanceDelta {
        account: account.to_vec(),
        magnitude,
        negative,
        reason: reason.to_string(),
        counterparty: counterparty.map(|c| c.to_vec()),
        asset: "native".to_string(),
    };
    let data = &event.data;
    let ctx = |what: &str| format!("{}: {what} (data: {data})", event.name);

    Ok(match event.name.as_str() {
        "balances.Transfer" => {
            let from = field_account(data, "from", 0).ok_or_else(|| ctx("no from"))?;
            let to = field_account(data, "to", 1).ok_or_else(|| ctx("no to"))?;
            let amount = field_amount(data, "amount", 2).ok_or_else(|| ctx("no amount"))?;
            if from == to {
                vec![] // self-transfer: net zero, and both rows would collide
            } else {
                vec![
                    d(from, amount, true, "transfer_out", Some(to)),
                    d(to, amount, false, "transfer_in", Some(from)),
                ]
            }
        }
        "balances.ReserveRepatriated" => {
            let from = field_account(data, "from", 0).ok_or_else(|| ctx("no from"))?;
            let to = field_account(data, "to", 1).ok_or_else(|| ctx("no to"))?;
            let amount = field_amount(data, "amount", 2).ok_or_else(|| ctx("no amount"))?;
            if from == to {
                vec![]
            } else {
                vec![
                    d(from, amount, true, "reserve_repatriated_out", Some(to)),
                    d(to, amount, false, "reserve_repatriated_in", Some(from)),
                ]
            }
        }
        // The HOLDS vocabulary (pallet-balances gained it with the fungible
        // holds API). `Held`/`Released` are intra-account and stay ∅ below, but
        // these three MOVE TOTAL and must not be mistaken for their neighbours:
        // a hold sits in `reserved`, so burning it or moving it to another
        // account changes free+reserved. Note the field ORDER — `reason` comes
        // FIRST, so the positional fallbacks are 1/2 and 1/2/3, not 0/1.
        "balances.BurnedHeld" => {
            // "Held balance was burned from an account" — reserved shrinks,
            // total shrinks with it. The only one-sided event of the three.
            let who = field_account(data, "who", 1).ok_or_else(|| ctx("no who"))?;
            let amount = field_amount(data, "amount", 2).ok_or_else(|| ctx("no amount"))?;
            vec![d(who, amount, true, "burned_held", None)]
        }
        // "A transfer of `amount` on hold from `source` to `dest`" and "the
        // `transferred` balance is placed on hold at the `dest` account" — both
        // are double-entry between two accounts, exactly like
        // ReserveRepatriated, and differ only in which field carries the amount.
        "balances.TransferOnHold" | "balances.TransferAndHold" => {
            let on_hold = event.name == "balances.TransferOnHold";
            let from = field_account(data, "source", 1).ok_or_else(|| ctx("no source"))?;
            let to = field_account(data, "dest", 2).ok_or_else(|| ctx("no dest"))?;
            let amount = if on_hold {
                field_amount(data, "amount", 3).ok_or_else(|| ctx("no amount"))?
            } else {
                field_amount(data, "transferred", 3).ok_or_else(|| ctx("no transferred"))?
            };
            let (out, into) = if on_hold {
                ("transfer_on_hold_out", "transfer_on_hold_in")
            } else {
                ("transfer_and_hold_out", "transfer_and_hold_in")
            };
            if from == to {
                vec![] // net zero, and both rows would collide on the PK
            } else {
                vec![
                    d(from, amount, true, out, Some(to)),
                    d(to, amount, false, into, Some(from)),
                ]
            }
        }
        "balances.Deposit" | "balances.Minted" | "balances.Withdraw" | "balances.Burned"
        | "balances.Slashed" | "balances.DustLost" | "balances.Suspended"
        | "balances.Restored" => {
            // single-account events: {who|account, amount}
            let who = field_account(data, "who", 0)
                .or_else(|| field_account(data, "account", 0))
                .ok_or_else(|| ctx("no who/account"))?;
            let amount = field_amount(data, "amount", 1).ok_or_else(|| ctx("no amount"))?;
            let (negative, reason) = match event.name.as_str() {
                "balances.Deposit" => (false, "deposit"),
                "balances.Minted" => (false, "minted"),
                "balances.Restored" => (false, "restored"),
                "balances.Withdraw" => (true, "withdraw"),
                "balances.Burned" => (true, "burned"),
                "balances.Slashed" => (true, "slashed"),
                "balances.Suspended" => (true, "suspended"),
                _ => (true, "dust_lost"),
            };
            vec![d(who, amount, negative, reason, None)]
        }
        // force_set_balance: the event carries the NEW ABSOLUTE value, not a
        // delta — no delta row is derivable. Emit a zero-magnitude marker so
        // consumers (the history API) invalidate running totals past it and a
        // re-anchor is visibly required. Root-only, rare, but never silent.
        "balances.BalanceSet" => {
            let who = field_account(data, "who", 0).ok_or_else(|| ctx("no who"))?;
            vec![d(who, 0, false, "balance_set_unquantified", None)]
        }
        // deliberate ∅: intra-account moves (total unchanged), Endowed (funds
        // arrive via the paired Transfer/Deposit/Minted), issuance-only events
        // Held/Released: the fungible holds API — free↔reserved within one
        // account (the hold sits in `reserved`), so total is unchanged
        //
        // MintedCredit/BurnedDebt are the IMBALANCE half of the same money the
        // account-side events already carry, and they name NO account:
        // "some credit was balanced and added to the TotalIssuance" and "some
        // debt has been dropped from the Total Issuance". They are `Issued`
        // and `Rescinded` in the fungible vocabulary, and counting them would
        // double the account leg that sits beside them — verified on live data
        // at AH #19542603, where `DustLost{account, 10000000}` is immediately
        // followed by `BurnedDebt{10000000}` for the SAME dust.
        //
        // Unexpected(UnexpectedKind) is a DEFENSIVE event (emitted from
        // update_locks/update_freezes when an invariant fails). It moves no
        // money, so it maps to ∅ here — but its presence in a block is a
        // runtime-level anomaly worth surfacing when a slice exists to do so.
        "balances.Endowed" | "balances.Reserved" | "balances.Unreserved"
        | "balances.Held" | "balances.Released"
        | "balances.Locked" | "balances.Unlocked" | "balances.Frozen"
        | "balances.Thawed" | "balances.Issued" | "balances.Rescinded"
        | "balances.MintedCredit" | "balances.BurnedDebt" | "balances.Unexpected"
        | "balances.Upgraded" | "balances.TotalIssuanceForced" => vec![],
        // an UNKNOWN balances event is a mapper gap, never silently ∅ — a
        // runtime upgrade adding a total-moving event must halt us loudly
        name if name.starts_with("balances.") => {
            return Err(format!("unknown balances event {name} — mapper update required"))
        }
        // The ASSETS pallets are the same fact about a different asset, so
        // they go through the same worker, the same tables and the same
        // loud-halt discipline — just a different vocabulary. Delegated
        // rather than inlined because the rules there are long and are
        // pinned by their own tests (crate::assets).
        _ => return crate::assets::deltas_for_assets_event(event),
    })
}

// ------------------------------------------------------- JSON field plumbing
// Event data is schema-on-read JSON written by our own decoders: named fields
// as objects, positional as arrays; AccountId32 as (nested) byte arrays
// (decoder v2) or SS58 strings (decoder v1 fixtures); u128 amounts as numbers
// when small, decimal strings when big.

pub(crate) fn field<'a>(data: &'a serde_json::Value, name: &str, index: usize) -> Option<&'a serde_json::Value> {
    match data {
        serde_json::Value::Object(map) => map.get(name),
        serde_json::Value::Array(items) => items.get(index),
        _ => None,
    }
}

/// Collect exactly 32 bytes out of arbitrarily nested arrays/objects —
/// AccountId32 renders as [[b0..b31]] (newtype), sometimes deeper
/// (MultiAddress). Decoder-version-1 rows (the fixture pipeline) render
/// accounts as SS58 strings instead; accept those too (checksum-verified).
pub(crate) fn json_account_bytes(v: &serde_json::Value) -> Option<[u8; 32]> {
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

pub(crate) fn json_u128(v: &serde_json::Value) -> Option<u128> {
    match v {
        serde_json::Value::Number(n) => n.as_u64().map(u128::from),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

pub(crate) fn field_account(data: &serde_json::Value, name: &str, index: usize) -> Option<[u8; 32]> {
    field(data, name, index).and_then(json_account_bytes)
}

pub(crate) fn field_amount(data: &serde_json::Value, name: &str, index: usize) -> Option<u128> {
    field(data, name, index).and_then(json_u128)
}

// ------------------------------------------------------------ anchor decode

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountBalances {
    pub free: u128,
    pub reserved: u128,
    /// Sum of frozen locks where the runtime exposes them (v2 `frozen`, or
    /// legacy misc_frozen + fee_frozen). Informational; total = free + reserved.
    pub frozen: Option<u128>,
}

impl AccountBalances {
    pub fn total(&self) -> u128 {
        self.free + self.reserved
    }
}

/// Decode a raw System.Account storage VALUE (AccountInfo) against the
/// metadata blob archived for that block's spec_version. Pure — bytes in,
/// balances out.
pub fn decode_account_info(
    metadata_blob: &[u8],
    value_bytes: &[u8],
) -> Result<AccountBalances, String> {
    let prefixed = RuntimeMetadataPrefixed::decode(&mut &metadata_blob[..])
        .map_err(|e| format!("metadata blob undecodable: {e}"))?;

    // $ver is an ident (v14/v15/v16), not a path fragment: `$path::More` in a
    // pattern is the classic macro parse trap (review catch).
    macro_rules! account_value_type {
        ($m:expr, $ver:ident) => {{
            use frame_metadata::$ver::StorageEntryType;
            let pallet = $m
                .pallets
                .iter()
                .find(|p| p.name == "System")
                .ok_or("no System pallet in metadata")?;
            let storage = pallet.storage.as_ref().ok_or("System has no storage")?;
            let entry = storage
                .entries
                .iter()
                .find(|e| e.name == "Account")
                .ok_or("System.Account entry not found")?;
            match &entry.ty {
                StorageEntryType::Map { value, .. } => (value.id, $m.types.clone()),
                _ => return Err("System.Account is not a Map".into()),
            }
        }};
    }

    let (value_type_id, types) = match &prefixed.1 {
        RuntimeMetadata::V14(m) => account_value_type!(m, v14),
        RuntimeMetadata::V15(m) => account_value_type!(m, v15),
        RuntimeMetadata::V16(m) => account_value_type!(m, v16),
        _ => return Err("unsupported metadata version (v14/v15/v16 only)".into()),
    };

    let mut cursor = value_bytes;
    let value = scale_value::scale::decode_as_type(&mut cursor, value_type_id, &types)
        .map_err(|e| format!("AccountInfo decode: {e}"))?;
    // remove the type-id context so walking sees plain Composite<()>
    let value = value.remove_context();

    // AccountInfo { nonce, consumers, providers, sufficients, data: AccountData }
    let data = named_field(&value, "data").ok_or("AccountInfo has no `data` field")?;
    let free = named_u128(data, "free").ok_or("AccountData has no `free`")?;
    let reserved = named_u128(data, "reserved").ok_or("AccountData has no `reserved`")?;
    let frozen = named_u128(data, "frozen").or_else(|| {
        // pre-balances-v2 runtimes: misc_frozen + fee_frozen
        match (named_u128(data, "misc_frozen"), named_u128(data, "fee_frozen")) {
            (Some(m), Some(f)) => Some(m + f),
            _ => None,
        }
    });
    Ok(AccountBalances {
        free,
        reserved,
        frozen,
    })
}

fn named_field<'a>(v: &'a Value<()>, name: &str) -> Option<&'a Value<()>> {
    match &v.value {
        ValueDef::Composite(Composite::Named(fields)) => {
            fields.iter().find(|(n, _)| n == name).map(|(_, v)| v)
        }
        _ => None,
    }
}

fn named_u128(v: &Value<()>, name: &str) -> Option<u128> {
    match &named_field(v, name)?.value {
        ValueDef::Primitive(scale_value::Primitive::U128(n)) => Some(*n),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounts::{pallet_account, para_sovereign};

    fn ev(name: &str, data: serde_json::Value) -> CanonicalEvent {
        CanonicalEvent {
            index: 0,
            transaction_index: None,
            name: name.into(),
            data,
        }
    }

    fn acct_json(a: &[u8; 32]) -> serde_json::Value {
        // the shape our decoder writes: newtype over the byte array
        serde_json::json!([a.to_vec()])
    }

    #[test]
    fn transfer_maps_to_double_entry() {
        let from = pallet_account(b"py/trsry");
        let to = para_sovereign(1000);
        let e = ev(
            "balances.Transfer",
            serde_json::json!({"from": acct_json(&from), "to": acct_json(&to), "amount": 636165600000u64}),
        );
        let ds = deltas_for_event(&e).unwrap();
        assert_eq!(ds.len(), 2);
        assert_eq!(ds[0].account, from.to_vec());
        assert!(ds[0].negative);
        assert_eq!(ds[0].reason, "transfer_out");
        assert_eq!(ds[0].counterparty.as_deref(), Some(&to[..]));
        assert_eq!(ds[1].account, to.to_vec());
        assert!(!ds[1].negative);
        assert_eq!(ds[0].magnitude, 636_165_600_000);
        assert_eq!(ds[1].magnitude, 636_165_600_000);
    }

    #[test]
    fn self_transfer_is_empty_not_a_collision() {
        let a = pallet_account(b"py/trsry");
        let e = ev(
            "balances.Transfer",
            serde_json::json!({"from": acct_json(&a), "to": acct_json(&a), "amount": 5}),
        );
        assert!(deltas_for_event(&e).unwrap().is_empty());
    }

    #[test]
    fn positional_fields_and_big_string_amounts_work() {
        let who = para_sovereign(2034);
        // positional array form + >u64 amount as decimal string
        let e = ev(
            "balances.Minted",
            serde_json::json!([acct_json(&who), "36893488147419103232"]),
        );
        let ds = deltas_for_event(&e).unwrap();
        assert_eq!(ds.len(), 1);
        assert_eq!(ds[0].magnitude, 36_893_488_147_419_103_232u128);
        assert!(!ds[0].negative);
        assert_eq!(ds[0].reason, "minted");
    }

    #[test]
    fn fee_events_map_and_intra_account_events_do_not() {
        let who = para_sovereign(1000);
        let w = ev(
            "balances.Withdraw",
            serde_json::json!({"who": acct_json(&who), "amount": 160000000}),
        );
        assert_eq!(deltas_for_event(&w).unwrap()[0].reason, "withdraw");
        for name in [
            "balances.Endowed",
            "balances.Reserved",
            "balances.Unreserved",
            "balances.Frozen",
            "balances.Thawed",
            "balances.Locked",
            "balances.Unlocked",
            "balances.Issued",
            "balances.Upgraded",
            "system.ExtrinsicSuccess",
        ] {
            let e = ev(name, serde_json::json!({"who": acct_json(&who), "amount": 1}));
            assert!(deltas_for_event(&e).unwrap().is_empty(), "{name} must map to ∅");
        }
        // unknown balances events halt loudly (mapper gap, not silent ∅)
        let unknown = ev(
            "balances.SomeFutureEvent",
            serde_json::json!({"who": acct_json(&who), "amount": 1}),
        );
        assert!(deltas_for_event(&unknown).is_err());
        // BalanceSet → zero-magnitude marker
        let set = ev(
            "balances.BalanceSet",
            serde_json::json!({"who": acct_json(&who), "free": 123}),
        );
        let ds = deltas_for_event(&set).unwrap();
        assert_eq!((ds[0].magnitude, ds[0].reason.as_str()), (0, "balance_set_unquantified"));
    }

    #[test]
    fn held_and_released_are_intra_account_noops() {
        let who = para_sovereign(1000);
        // shape observed live on asset-hub 19501590 (holds API carries a reason)
        for name in ["balances.Held", "balances.Released"] {
            let e = ev(
                name,
                serde_json::json!({"who": acct_json(&who), "amount": 0, "reason": {"Revive": [{"AddressMapping": []}]}}),
            );
            assert!(deltas_for_event(&e).unwrap().is_empty(), "{name} must map to ∅");
        }
    }

    /// The three holds-vocabulary events that DO move total. Their field order
    /// puts `reason` FIRST, so this also pins the positional fallbacks — a
    /// mapper that reused the 0/1 indices of the `Deposit` group would read the
    /// hold reason as an account and halt (or worse, not).
    #[test]
    fn holds_that_move_total_are_double_entry_or_one_sided() {
        let a = pallet_account(b"py/trsry");
        let b = para_sovereign(1000);
        let reason = serde_json::json!({"Revive": [{"AddressMapping": []}]});

        let burned = ev(
            "balances.BurnedHeld",
            serde_json::json!({"reason": reason, "who": acct_json(&a), "amount": 700}),
        );
        let ds = deltas_for_event(&burned).unwrap();
        assert_eq!(ds.len(), 1);
        assert_eq!(ds[0].account, a.to_vec());
        assert_eq!(ds[0].magnitude, 700);
        assert!(ds[0].negative, "burning held funds shrinks reserved, so total");
        assert_eq!(ds[0].reason, "burned_held");

        // positional form, reason at 0 — source/dest/amount at 1/2/3
        let on_hold = ev(
            "balances.TransferOnHold",
            serde_json::json!([reason, acct_json(&a), acct_json(&b), 900]),
        );
        let ds = deltas_for_event(&on_hold).unwrap();
        assert_eq!(ds.len(), 2);
        assert_eq!((ds[0].account.clone(), ds[0].negative, ds[0].magnitude), (a.to_vec(), true, 900));
        assert_eq!((ds[1].account.clone(), ds[1].negative, ds[1].magnitude), (b.to_vec(), false, 900));
        assert_eq!(ds[0].counterparty.as_deref(), Some(&b[..]));

        // TransferAndHold carries the amount under a DIFFERENT name
        let and_hold = ev(
            "balances.TransferAndHold",
            serde_json::json!({"reason": reason, "source": acct_json(&a), "dest": acct_json(&b), "transferred": 1234}),
        );
        let ds = deltas_for_event(&and_hold).unwrap();
        assert_eq!(ds.len(), 2);
        assert_eq!(ds[0].magnitude, 1234);
        assert_eq!(ds[1].magnitude, 1234);
        assert_eq!(ds[0].reason, "transfer_and_hold_out");

        // and a self-move is ∅, not a PK collision
        let self_move = ev(
            "balances.TransferOnHold",
            serde_json::json!({"reason": reason, "source": acct_json(&a), "dest": acct_json(&a), "amount": 5}),
        );
        assert!(deltas_for_event(&self_move).unwrap().is_empty());
    }

    /// The imbalance events name no account and must not double-count the
    /// account leg beside them. Pinned to the REAL block that surfaced the gap:
    /// asset-hub #19542603 ev5 `DustLost{account, 10000000}` is immediately
    /// followed by ev6 `BurnedDebt{10000000}` — the same 10000000, once.
    #[test]
    fn imbalance_events_name_no_account_and_never_double_count() {
        let who = para_sovereign(1000);
        let dust = ev(
            "balances.DustLost",
            serde_json::json!({"account": acct_json(&who), "amount": 10_000_000u64}),
        );
        let debt = ev("balances.BurnedDebt", serde_json::json!({"amount": 10_000_000u64}));
        let total: u128 = [dust, debt]
            .iter()
            .flat_map(|e| deltas_for_event(e).unwrap())
            .map(|d| d.magnitude)
            .sum();
        assert_eq!(total, 10_000_000, "the dust must be counted once, not twice");

        for name in ["balances.MintedCredit", "balances.BurnedDebt"] {
            let e = ev(name, serde_json::json!({"amount": 5}));
            assert!(deltas_for_event(&e).unwrap().is_empty(), "{name} must map to ∅");
        }
        // the defensive variant is a newtype over UnexpectedKind, not a balance
        let e = ev("balances.Unexpected", serde_json::json!([{"Underflow": []}]));
        assert!(deltas_for_event(&e).unwrap().is_empty());
    }

    #[test]
    fn fixture_era_ss58_string_accounts_map() {
        // decoder-version-1 rows render accounts as SS58 strings; the treasury
        // golden vector round-trips to the same bytes as the derived account
        let e = ev(
            "balances.Deposit",
            serde_json::json!({"who": "13UVJyLnbVp9RBZYFwFGyDvVd1y27Tt8tkntv6Q7JVPhFsTB", "amount": "1204000000000"}),
        );
        let ds = deltas_for_event(&e).unwrap();
        assert_eq!(ds[0].account, pallet_account(b"py/trsry").to_vec());
        assert_eq!(ds[0].magnitude, 1_204_000_000_000);
        assert!(!ds[0].negative);
    }

    #[test]
    fn malformed_balances_events_are_loud_errors() {
        let e = ev("balances.Transfer", serde_json::json!({"from": [[1,2]], "amount": 5}));
        assert!(deltas_for_event(&e).is_err());
        let e2 = ev("balances.Deposit", serde_json::json!({"who": "not-bytes", "amount": 5}));
        assert!(deltas_for_event(&e2).is_err());
    }

    #[test]
    fn dust_lost_uses_the_account_field() {
        let who = para_sovereign(3344);
        let e = ev(
            "balances.DustLost",
            serde_json::json!({"account": acct_json(&who), "amount": 42}),
        );
        let ds = deltas_for_event(&e).unwrap();
        assert_eq!(ds[0].reason, "dust_lost");
        assert!(ds[0].negative);
    }

    #[test]
    fn account_info_decodes_against_real_metadata() {
        // real committed AH metadata + hand-built AccountInfo bytes:
        // nonce/consumers/providers/sufficients (4×u32 LE) then
        // AccountData { free, reserved, frozen, flags } (4×u128 LE)
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/real/polkadot-asset-hub-19498783/metadata.scale");
        let Ok(blob) = std::fs::read(&path) else {
            eprintln!("SKIP: real fixture metadata not present at {}", path.display());
            return;
        };
        let mut bytes = Vec::new();
        for n in [7u32, 0, 1, 0] {
            bytes.extend_from_slice(&n.to_le_bytes());
        }
        let free: u128 = 636_165_600_000;
        let reserved: u128 = 5_000_000_000;
        let frozen: u128 = 1_000_000_000;
        let flags: u128 = 0x8000_0000_0000_0000_0000_0000_0000_0000; // new-logic flag
        for n in [free, reserved, frozen, flags] {
            bytes.extend_from_slice(&n.to_le_bytes());
        }
        let ab = decode_account_info(&blob, &bytes).expect("decodes");
        assert_eq!(ab.free, free);
        assert_eq!(ab.reserved, reserved);
        assert_eq!(ab.frozen, Some(frozen));
        assert_eq!(ab.total(), free + reserved);
    }
}
