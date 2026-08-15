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
//!      BalanceSet          → zero-magnitude MARKER (absolute value, no delta
//!                            derivable — running totals invalidate until the
//!                            next anchor)
//!    Deliberately NO delta (total unchanged, or double-count):
//!      Endowed (funds arrive via the paired Transfer/Deposit/Minted),
//!      Reserved/Unreserved/Held/Released/Frozen/Thawed/Locked/Unlocked (intra-account),
//!      Issued/Rescinded/TotalIssuanceForced (issuance, no account), Upgraded.
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

pub const MAPPER_VERSION: u32 = 1;

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
        "balances.Endowed" | "balances.Reserved" | "balances.Unreserved"
        | "balances.Held" | "balances.Released"
        | "balances.Locked" | "balances.Unlocked" | "balances.Frozen"
        | "balances.Thawed" | "balances.Issued" | "balances.Rescinded"
        | "balances.Upgraded" | "balances.TotalIssuanceForced" => vec![],
        // an UNKNOWN balances event is a mapper gap, never silently ∅ — a
        // runtime upgrade adding a total-moving event must halt us loudly
        name if name.starts_with("balances.") => {
            return Err(format!("unknown balances event {name} — mapper update required"))
        }
        _ => vec![], // other pallets: not this mapper's money
    })
}

// ------------------------------------------------------- JSON field plumbing
// Event data is schema-on-read JSON written by our own decoders: named fields
// as objects, positional as arrays; AccountId32 as (nested) byte arrays
// (decoder v2) or SS58 strings (decoder v1 fixtures); u128 amounts as numbers
// when small, decimal strings when big.

fn field<'a>(data: &'a serde_json::Value, name: &str, index: usize) -> Option<&'a serde_json::Value> {
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

fn json_u128(v: &serde_json::Value) -> Option<u128> {
    match v {
        serde_json::Value::Number(n) => n.as_u64().map(u128::from),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn field_account(data: &serde_json::Value, name: &str, index: usize) -> Option<[u8; 32]> {
    field(data, name, index).and_then(json_account_bytes)
}

fn field_amount(data: &serde_json::Value, name: &str, index: usize) -> Option<u128> {
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
