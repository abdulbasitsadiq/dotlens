//! Assets semantics of the Substrate family (Invariant 4: protocol specifics
//! live in adapters only). Four pure pieces, no I/O anywhere:
//!
//!   1. `deltas_for_assets_event` — pallet-assets events → the SAME
//!      `BalanceDelta` the balances mapper emits, tagged with an asset key.
//!      Asset balances are not a new kind of fact; they are the existing fact
//!      about a different asset, so they ride the existing worker and the
//!      existing tables (migration 0005 has carried an `asset` column, in the
//!      PK, since Phase 1).
//!   2. `canonical_location` — the version-free name of an XCM Location.
//!   3. `decode_asset_account` / `decode_asset_metadata` / `decode_asset_details`
//!      — state reads, decoded against block-correct metadata.
//!   4. storage-key plumbing driven by METADATA rather than assumption:
//!      hashers are read from the runtime's own storage entry, and the raw
//!      key bytes are lifted back out of concat hashes.
//!
//! ------------------------------------------------------------------- events
//!
//! THE VOCABULARY, diffed across every published pallet-assets from 3.0.0 to
//! 52.0.0 (54 versions fetched from crates.io at authoring time). Growth only —
//! nothing was ever removed after 4.0.0, so a mapper that covers 52.0.0 covers
//! every runtime we can decode:
//!
//!   4.0.0   the named-field shape we rely on (3.0.0 was tuple variants and is
//!           below our v14 metadata floor — unreachable, like the pre-v14
//!           treasury shapes in 0009)
//!   15.0.0  `Issued.total_supply` RENAMED to `Issued.amount`
//!   20.0.0  +AssetMinBalanceChanged
//!   23.0.0  +Touched +Blocked
//!   35.0.0  +Deposited +Withdrawn   (the fungibles `Balanced` impl)
//!   48.0.0  +ReservesUpdated +ReservesRemoved
//!   49.0.0  +IssuedCredit +BurnedCredit +IssuedDebt +BurnedDebt
//!
//! THE TRAP IN THAT LIST, and it is a factor-of-everything trap: pre-15.0.0
//! `Issued` has a field literally named `total_supply` which does NOT hold the
//! total supply — `do_mint` emits `Event::Issued { asset_id, owner,
//! total_supply: amount }`, i.e. the amount just minted. A mapper that read
//! the name at face value would post the entire supply of an asset as a credit
//! to one account, every mint, forever. We read position 2 by name in BOTH
//! spellings and treat it as an amount either way.
//!
//! MONEY MOVES (per-account; an asset account has ONE balance, no free/reserved):
//!   Transferred{asset_id, from, to, amount}          → −from, +to
//!   TransferredApproved{asset_id, owner, delegate,
//!                       destination, amount}         → −owner, +destination
//!       the `delegate` is the authority spending someone else's allowance,
//!       not a party to the movement — crediting it would double the money.
//!       NOTE (verified against every published version): the pallet DOES NOT
//!       ACTUALLY EMIT THIS — `transfer_approved` goes through
//!       `do_transfer_approved` → `transfer_and_die`, which emits an ordinary
//!       `Transferred`, despite the pallet's own doc-comment saying otherwise.
//!       The arm is mapped anyway, because a dead arm that is correct costs
//!       nothing and an unmapped one halts the worker the day it revives —
//!       but do not read it as "how approved transfers are indexed".
//!   Issued{asset_id, owner, amount|total_supply}     → +owner
//!   Burned{asset_id, owner, balance}                 → −owner
//!   Deposited{asset_id, who, amount}                 → +who
//!   Withdrawn{asset_id, who, amount}                 → −who
//!
//! DELIBERATE ∅, with the reason, because "no delta" must never mean "we did
//! not think about it":
//!   Frozen / Thawed / Blocked        an account's STATUS, not its balance —
//!                                    and status has its own anchor column, so
//!                                    it is recorded, just not as money
//!   ApprovedTransfer / ApprovalCancelled / ApprovalsDestroyed
//!                                    an approval is permission, not payment;
//!                                    the deposit it reserves is NATIVE and
//!                                    pallet-balances already reports it
//!   Created / ForceCreated / Destroyed / DestructionStarted /
//!   AssetStatusChanged / AssetMinBalanceChanged / MetadataSet /
//!   MetadataCleared / TeamChanged / OwnerChanged / AssetFrozen / AssetThawed
//!                                    asset-level administration, no account
//!   Touched                          creates an account, always at zero
//!   ReservesUpdated / ReservesRemoved (48.0.0)
//!                                    reserve CONFIGURATION for an asset
//!   IssuedCredit / BurnedCredit / IssuedDebt / BurnedDebt (49.0.0)
//!                                    imbalance bookkeeping from the fungibles
//!                                    `Balanced` impl — supply moves, and the
//!                                    event names NO account, so there is no
//!                                    per-account delta to derive. These DO
//!                                    change total issuance; supply history is
//!                                    a later slice and `core.assets.supply`
//!                                    carries the state-read truth meanwhile.
//!
//! THE ONE HONEST GAP, stated here so nobody discovers it in a balance sheet:
//! `AccountsDestroyed{asset_id, accounts_destroyed, accounts_remaining}` wipes
//! account balances during asset destruction and names NONE of the accounts —
//! only how many. There is no per-account delta to emit, so we emit none, and
//! any holder's running total for that asset silently stops being true at that
//! block. It is not silent in the product: destruction sets the asset's status
//! to `Destroying`, `sync-assets` records that in `core.assets.status`, and the
//! holdings endpoint refuses to treat a non-`Live` asset's running total as
//! current. Re-anchoring is the fix, exactly as it is for `BalanceSet`.
//!
//! An UNKNOWN event on a mapped assets pallet is a LOUD error, same rule as
//! balances and treasury: a runtime upgrade that adds a money-moving event
//! must halt us, never quietly drop money.
//!
//! KNOWN-UNMAPPED PALLETS (different models, each needing its own slice, none
//! of them silently wrong here — they simply are not this mapper's money):
//!   nfts / uniques        non-fungible; a "balance" is an item, not an amount
//!   assetconversion       the DEX; its LP shares ARE PoolAssets and appear
//!                         through that instance, but swap events belong to a
//!                         DEX slice
//!   assetrate             a conversion-rate registry, no balances at all
//!   tokens / currencies   the ORML family (Hydration, Phase 3) — same idea,
//!                         a completely different event vocabulary
//!
//! ---------------------------------------------------------------- asset keys
//!
//! `asset_key` is what lands in `balances.*.asset`, and it is the whole reason
//! this module can share the balances tables:
//!     native            pallet-balances (written by the balances mapper)
//!     assets:1984       integer-keyed instance (TrustBacked — USDT on AH)
//!     pool:12           integer-keyed instance (Pool — LP shares)
//!     foreign:<loc>     Location-keyed instance, `<loc>` from
//!                       `canonical_location`
//!
//! CANONICAL LOCATIONS, and why a naive key would have been wrong: the same
//! asset does not decode to the same JSON twice. Slice 5 met both shapes on
//! real data — the relay's V3 wraps the asset id in `Concrete` and its `X1`
//! holds `[Junction]`, while Asset Hub's V4/V5 drop `Concrete` and their `X1`
//! holds `[[Junction]]` (one array deeper, because `X1` became
//! `X1([Junction; 1])`). `Option::None` renders as `{"None":[]}`, never null.
//! Keyed raw, one USDC would be two assets. So `canonical_location`:
//!     strips the version wrapper (V2/V3/V4/V5, newtype array or not)
//!     strips `Concrete`
//!     normalizes `{"None":[]}` → null and `{"Some":[x]}` → x
//!     flattens `Here`/`X1..X8` into a plain junction ARRAY
//!     renders byte arrays (≥4 bytes) as 0x-hex, so a key stays legible
//!     serializes as JSON, whose object keys serde_json sorts — the ordering
//!     guarantee that makes this a stable database key rather than a hope
//!
//! What it deliberately does NOT do: rewrite numbers. A `GeneralIndex` past
//! u64 renders as a decimal string (the decoder's rule) and a small one as a
//! number, so the SAME value always renders the same way — deterministic,
//! just not uniform. Normalizing it would mean re-deciding numeric types we
//! were careful not to guess at decode time.

use canonical::CanonicalEvent;
use frame_metadata::{RuntimeMetadata, RuntimeMetadataPrefixed};
use ingest::balances::BalanceDelta;
use parity_scale_codec::Decode;
use scale_value::{Composite, Value, ValueDef};

use crate::balances::{field, field_account, field_amount, json_u128};

/// Which pallet-assets instance a pallet is, in dotlens vocabulary.
///
/// Instance → representation is ADAPTER vocabulary, exactly like the gov
/// slice's pallet → referenda class: the names come from the runtime, the
/// meaning is ours. Where each representation LIVES is registry data (chains
/// declare `capabilities.assets.instances`), so this enum never names a chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Representation {
    /// `Assets` — integer ids, backed by a trusted issuer (USDT 1984, USDC 1337).
    TrustBacked,
    /// `PoolAssets` — integer ids, LP shares of the asset-conversion DEX.
    Pool,
    /// `ForeignAssets` — keyed by XCM Location (bridged ERC-20s, other chains'
    /// natives). ECOSYSTEM.md §4: `parents: 2` means outside our consensus.
    Foreign,
    /// An assets-shaped pallet this adapter has no name for. Its STATE is
    /// still indexed (keys and values are self-describing through metadata),
    /// under a key prefix taken from the pallet's own name; its EVENTS are
    /// not mapped, and `assets_pallets_from_metadata` says so loudly rather
    /// than letting a chain's money go quietly unindexed.
    Unmapped,
}

impl Representation {
    /// The `core.assets.representation_kind` value.
    pub fn kind_str(&self) -> &'static str {
        match self {
            Representation::TrustBacked => "trust_backed",
            Representation::Pool => "pool",
            Representation::Foreign => "foreign",
            Representation::Unmapped => "other",
        }
    }
}

/// Pallet name (as the decoder lowercases it) → representation. The three
/// system-parachain instances; anything else is `Unmapped`.
pub fn representation_for_pallet(lowercased_pallet: &str) -> Representation {
    match lowercased_pallet {
        "assets" => Representation::TrustBacked,
        "poolassets" => Representation::Pool,
        "foreignassets" => Representation::Foreign,
        _ => Representation::Unmapped,
    }
}

/// The `asset_key` prefix for a pallet. Mapped instances get a stable dotlens
/// word; an unmapped one gets its own lowercased pallet name, so its assets
/// are still uniquely and honestly named.
pub fn key_prefix_for_pallet(lowercased_pallet: &str) -> String {
    match representation_for_pallet(lowercased_pallet) {
        Representation::TrustBacked => "assets".to_string(),
        Representation::Pool => "pool".to_string(),
        Representation::Foreign => "foreign".to_string(),
        Representation::Unmapped => lowercased_pallet.to_string(),
    }
}

/// Is this event name one this mapper owns? `pallet.Variant`, lowercased
/// pallet — the decoder's shape.
fn split_event_name(name: &str) -> Option<(&str, &str)> {
    name.split_once('.')
}

/// Does this pallet's money belong to this mapper?
fn is_mapped_assets_pallet(pallet: &str) -> bool {
    !matches!(representation_for_pallet(pallet), Representation::Unmapped)
}

// ------------------------------------------------------------------ the mapper

/// pallet-assets event → per-account deltas. Returns ∅ for every event that is
/// not a mapped assets pallet's; errors LOUDLY for a mapped pallet's unknown
/// or malformed event.
pub fn deltas_for_assets_event(event: &CanonicalEvent) -> Result<Vec<BalanceDelta>, String> {
    let Some((pallet, variant)) = split_event_name(&event.name) else {
        return Ok(vec![]);
    };
    if !is_mapped_assets_pallet(pallet) {
        return Ok(vec![]);
    }
    let data = &event.data;
    let ctx = |what: &str| format!("{}: {what} (data: {data})", event.name);
    let prefix = key_prefix_for_pallet(pallet);

    // Every mapped variant carries `asset_id` first. Resolve it ONCE, and fail
    // loudly if it is unreadable: an asset delta with the wrong asset is worse
    // than no delta, because it silently credits a different currency.
    let key = || -> Result<String, String> {
        let id = field(data, "asset_id", 0).ok_or_else(|| ctx("no asset_id"))?;
        asset_key_from_id(&prefix, id).ok_or_else(|| ctx("asset_id is not an integer or a location"))
    };

    let d = |asset: &str,
             account: [u8; 32],
             magnitude: u128,
             negative: bool,
             reason: &str,
             counterparty: Option<[u8; 32]>| BalanceDelta {
        account: account.to_vec(),
        magnitude,
        negative,
        reason: reason.to_string(),
        counterparty: counterparty.map(|c| c.to_vec()),
        asset: asset.to_string(),
    };

    Ok(match variant {
        "Transferred" => {
            let asset = key()?;
            let from = field_account(data, "from", 1).ok_or_else(|| ctx("no from"))?;
            let to = field_account(data, "to", 2).ok_or_else(|| ctx("no to"))?;
            let amount = field_amount(data, "amount", 3).ok_or_else(|| ctx("no amount"))?;
            if from == to {
                vec![] // net zero, and both rows would collide on the PK
            } else {
                vec![
                    d(&asset, from, amount, true, "transfer_out", Some(to)),
                    d(&asset, to, amount, false, "transfer_in", Some(from)),
                ]
            }
        }
        // the delegate spends someone else's allowance; the money moves
        // owner → destination and the delegate is not a party to it
        "TransferredApproved" => {
            let asset = key()?;
            let owner = field_account(data, "owner", 1).ok_or_else(|| ctx("no owner"))?;
            let destination =
                field_account(data, "destination", 3).ok_or_else(|| ctx("no destination"))?;
            let amount = field_amount(data, "amount", 4).ok_or_else(|| ctx("no amount"))?;
            if owner == destination {
                vec![]
            } else {
                vec![
                    d(&asset, owner, amount, true, "transfer_out", Some(destination)),
                    d(&asset, destination, amount, false, "transfer_in", Some(owner)),
                ]
            }
        }
        // `Issued` field 2 is `amount` from 15.0.0 and `total_supply` before
        // it — SAME VALUE (the minted amount), different spelling. Both names
        // are tried; the positional fallback covers either.
        "Issued" => {
            let asset = key()?;
            let who = field_account(data, "owner", 1).ok_or_else(|| ctx("no owner"))?;
            let amount = field_amount(data, "amount", 2)
                .or_else(|| field_amount(data, "total_supply", 2))
                .ok_or_else(|| ctx("no amount/total_supply"))?;
            vec![d(&asset, who, amount, false, "issued", None)]
        }
        // `Burned` field 2 is spelled `balance` and carries the amount burned
        "Burned" => {
            let asset = key()?;
            let who = field_account(data, "owner", 1).ok_or_else(|| ctx("no owner"))?;
            let amount = field_amount(data, "balance", 2)
                .or_else(|| field_amount(data, "amount", 2))
                .ok_or_else(|| ctx("no balance"))?;
            vec![d(&asset, who, amount, true, "burned", None)]
        }
        "Deposited" | "Withdrawn" => {
            let asset = key()?;
            let who = field_account(data, "who", 1).ok_or_else(|| ctx("no who"))?;
            let amount = field_amount(data, "amount", 2).ok_or_else(|| ctx("no amount"))?;
            let negative = variant == "Withdrawn";
            let reason = if negative { "withdraw" } else { "deposit" };
            vec![d(&asset, who, amount, negative, reason, None)]
        }
        // deliberate ∅ — see the module docs for the reason attached to each
        "Created" | "ForceCreated" | "Destroyed" | "DestructionStarted"
        | "AccountsDestroyed" | "ApprovalsDestroyed" | "AssetStatusChanged"
        | "AssetMinBalanceChanged" | "MetadataSet" | "MetadataCleared" | "TeamChanged"
        | "OwnerChanged" | "AssetFrozen" | "AssetThawed" | "Frozen" | "Thawed"
        | "Blocked" | "Touched" | "ApprovedTransfer" | "ApprovalCancelled"
        | "ReservesUpdated" | "ReservesRemoved" | "IssuedCredit" | "BurnedCredit"
        | "IssuedDebt" | "BurnedDebt" => vec![],
        other => {
            return Err(format!(
                "unknown {pallet} event {other} — assets mapper update required \
                 (vocabulary verified against pallet-assets 4.0.0..=52.0.0)"
            ))
        }
    })
}

/// `asset_id` field (integer id or XCM Location) → the `<prefix>:<id>` key.
/// One function for both shapes: an integer-keyed instance renders the number,
/// a Location-keyed one renders its canonical form.
pub fn asset_key_from_id(prefix: &str, id: &serde_json::Value) -> Option<String> {
    if let Some(n) = json_u128(id) {
        return Some(format!("{prefix}:{n}"));
    }
    // a newtype-wrapped id decodes one array layer deep: `[1984]`
    if let serde_json::Value::Array(items) = id {
        if items.len() == 1 {
            if let Some(n) = json_u128(&items[0]) {
                return Some(format!("{prefix}:{n}"));
            }
        }
    }
    canonical_location(id).map(|loc| format!("{prefix}:{loc}"))
}

// ----------------------------------------------------------- XCM locations

/// XCM version wrapper keys we unwrap. Listed rather than pattern-matched on
/// `^V\d+$` so a future non-version single-key object can never be mistaken
/// for a version — the same reason the treasury mapper names its variants.
const VERSION_KEYS: [&str; 6] = ["V2", "V3", "V4", "V5", "V6", "V7"];

/// Normalize a decoded XCM Location (or anything wrapping one) into the
/// version-free shape `{"parents": n, "interior": [junction, …]}`.
/// `None` when the value is not location-shaped.
pub fn normalize_location(v: &serde_json::Value) -> Option<serde_json::Value> {
    let inner = strip_wrappers(v);
    let obj = inner.as_object()?;
    let parents = obj.get("parents").and_then(json_u128).unwrap_or(0);
    let interior = obj.get("interior")?;
    let junctions = flatten_interior(interior)?;
    Some(serde_json::json!({
        "parents": json_num(parents),
        "interior": junctions.iter().map(normalize_value).collect::<Vec<_>>(),
    }))
}

/// Render a u128 the way the decoder renders one: a JSON number while it fits
/// in u64, a decimal STRING beyond that (`frame_decoder::primitive_to_json`).
/// Constructed locations must be spelled exactly like decoded ones or the
/// canonical strings would not compare equal.
fn json_num(n: u128) -> serde_json::Value {
    match u64::try_from(n) {
        Ok(small) => serde_json::Value::from(small),
        Err(_) => serde_json::Value::from(n.to_string()),
    }
}

/// The canonical STRING form — what `balances.*.asset` and
/// `core.assets.location_key` hold. serde_json serializes object keys in
/// sorted order (its `Map` is a `BTreeMap` unless `preserve_order` is on,
/// which we do not enable), so this is stable across runs and processes.
pub fn canonical_location(v: &serde_json::Value) -> Option<String> {
    normalize_location(v).map(|n| n.to_string())
}

/// Peel the wrappers that mean "the same location, spelled differently":
/// the XCM version (`{"V4": …}`, sometimes newtype-wrapped in a 1-element
/// array), V3's `Concrete` around an asset id, and the UNNAMED struct-newtype
/// layer around a bare `Location`.
///
/// That last one has no key to recognise it by. `AssetId` is declared
/// `pub struct AssetId(pub Location)`, so a `VersionedLocatableAsset`'s
/// `asset_id` field decodes as `[{parents, interior}]` — one array deeper than
/// its sibling `location`, which is a plain `Location` and decodes as
/// `{parents, interior}`. Peeling it is unambiguous because a Location is an
/// object and never an array, so a 1-element array can only be a wrapper.
///
/// This is the SECOND time this newtype layer has broken the spend→asset join
/// (the first was `PalletInstance(50)` → `{"PalletInstance":[50]}` inside the
/// junctions). Both times the pinning test passed while production data failed,
/// because the test hand-wrote the scalar form on BOTH sides of the comparison.
/// The tests below now use shapes captured from real decoded events.
fn strip_wrappers(v: &serde_json::Value) -> &serde_json::Value {
    let mut cur = v;
    for _ in 0..6 {
        // the unnamed newtype layer, peeled before anything that needs a key
        if let serde_json::Value::Array(items) = cur {
            if items.len() == 1 {
                cur = &items[0];
                continue;
            }
            break;
        }
        let Some(obj) = cur.as_object() else { break };
        if obj.len() != 1 {
            break;
        }
        let (k, val) = obj.iter().next().expect("len 1");
        if !VERSION_KEYS.contains(&k.as_str()) && k != "Concrete" {
            break;
        }
        // enum variants with UNNAMED fields decode to an array; a newtype
        // variant is therefore a 1-element array around the real value
        cur = match val {
            serde_json::Value::Array(items) if items.len() == 1 => &items[0],
            other => other,
        };
    }
    cur
}

/// `Junctions` → a plain array. `Here` (string or `{"Here":[]}`) is empty;
/// `X1..X8` hold an array of junctions, which in XCM v4/v5 is itself wrapped
/// in one more array because `X1` became `X1([Junction; 1])`.
fn flatten_interior(v: &serde_json::Value) -> Option<Vec<serde_json::Value>> {
    match v {
        serde_json::Value::String(s) if s == "Here" => Some(vec![]),
        serde_json::Value::Array(items) => Some(items.clone()),
        serde_json::Value::Object(map) => {
            if map.len() != 1 {
                return None;
            }
            let (k, val) = map.iter().next().expect("len 1");
            if k == "Here" {
                return Some(vec![]);
            }
            if !(k.len() == 2 && k.starts_with('X') && k[1..].chars().all(|c| c.is_ascii_digit())) {
                return None;
            }
            let expected: usize = k[1..].parse().ok()?;
            let items = val.as_array()?;
            // ONE rule covers both spellings: XCM ≤v3 declares `X2(Junction,
            // Junction)` — N unnamed fields, so N array elements — while
            // v4/v5 declare `X2([Junction; 2])`, ONE unnamed field that is
            // itself an array. A single element that is an array is therefore
            // always the outer wrapper, because no junction is an array.
            let items = match items.first() {
                Some(serde_json::Value::Array(inner)) if items.len() == 1 => inner,
                _ => items,
            };
            // the variant's own name states the length; a mismatch means the
            // shape is not what we think it is, and guessing past that is how
            // two different locations end up sharing one key
            (items.len() == expected).then(|| items.clone())
        }
        _ => None,
    }
}

/// Recursive normalization of junction arguments: `Option` collapsed, byte
/// arrays hexed, everything else structurally unchanged.
fn normalize_value(v: &serde_json::Value) -> serde_json::Value {
    match v {
        serde_json::Value::Object(map) => {
            // NEWTYPE VARIANTS. Our decoder renders an enum variant as
            // `{"Name": <fields>}` with UNNAMED fields as an array, so every
            // single-field junction decodes one array layer deep:
            // `PalletInstance(50)` → `{"PalletInstance":[50]}`. A location we
            // CONSTRUCT (`local_asset_location`) has no such layer, and a
            // reviewer caught that the two therefore never compared equal —
            // which would have silently killed the entire spend → asset join
            // this slice exists for, while the test that "pinned" it passed
            // because both of its sides were hand-written in the scalar form.
            // Collapsing here makes the two spellings one, and is the same
            // one-layer, non-recursive unwrap the votes slice needed.
            //
            // `Option` is the special case that drops its wrapper entirely:
            // `{"None":[]}` → null, `{"Some":[x]}` → x.
            if map.len() == 1 {
                let (k, val) = map.iter().next().expect("len 1");
                if k == "None" {
                    return serde_json::Value::Null;
                }
                if let serde_json::Value::Array(items) = val {
                    if items.len() == 1 {
                        let inner = normalize_value(&items[0]);
                        return if k == "Some" {
                            inner
                        } else {
                            serde_json::json!({ k.clone(): inner })
                        };
                    }
                }
                if k == "Some" {
                    return normalize_value(val);
                }
            }
            serde_json::Value::Object(
                map.iter()
                    .map(|(k, val)| (k.clone(), normalize_value(val)))
                    .collect(),
            )
        }
        serde_json::Value::Array(items) => {
            if let Some(hex) = as_byte_hex(items) {
                return serde_json::Value::String(hex);
            }
            serde_json::Value::Array(items.iter().map(normalize_value).collect())
        }
        other => other.clone(),
    }
}

/// An array of ≥4 numbers all in 0..=255 is a byte string (`AccountKey20.key`,
/// `AccountId32.id`, `GeneralKey.data`); render it as hex so a location key
/// stays readable. Below 4 elements the shape is ambiguous enough that we
/// leave it alone — and no XCM byte field is that short.
fn as_byte_hex(items: &[serde_json::Value]) -> Option<String> {
    if items.len() < 4 {
        return None;
    }
    let mut out = Vec::with_capacity(items.len());
    for i in items {
        let n = i.as_u64()?;
        if n > 255 {
            return None;
        }
        out.push(n as u8);
    }
    Some(format!("0x{}", hex::encode(out)))
}

/// The XCM name of an integer-keyed asset held by the pallet at runtime index
/// `pallet_index`: `[PalletInstance(index), GeneralIndex(id)]`, parents 0.
///
/// CONSTRUCTED, not read — the assets pallet does not know its own XCM name.
/// This is what lets a treasury spend of "20895000000 of {PalletInstance 50,
/// GeneralIndex 1984}" find asset `assets:1984` and be rendered in USDT.
pub fn local_asset_location(pallet_index: u8, id: u128) -> serde_json::Value {
    serde_json::json!({
        "parents": 0,
        "interior": [
            {"PalletInstance": pallet_index},
            {"GeneralIndex": json_num(id)},
        ],
    })
}

/// Split a `VersionedLocatableAsset` into its two normalized halves:
/// `chain` (WHICH chain holds the asset — `Here` on the paying chain itself)
/// and `asset` (WHICH asset on it). Either may be absent on malformed data;
/// the caller decides whether that is fatal.
///
/// The relay and Asset Hub disagree on the spelling of both halves (V3
/// `Concrete` + flat `X1` vs V4/V5 nested `X1`) and agree on nothing but the
/// meaning — which is exactly what normalization is for.
pub fn locatable_asset_parts(asset_kind: &serde_json::Value) -> Option<serde_json::Value> {
    let inner = strip_wrappers(asset_kind);
    let obj = inner.as_object()?;
    let chain = obj.get("location").and_then(normalize_location);
    let asset = obj.get("asset_id").and_then(normalize_location);
    if chain.is_none() && asset.is_none() {
        return None;
    }
    Some(serde_json::json!({ "chain": chain, "asset": asset }))
}

/// Symbol and decimals of a chain's NATIVE token, out of `system_properties`.
///
/// Both fields are sometimes arrays (a chain that reports several tokens lists
/// its native one first, by convention) and sometimes scalars — accept both,
/// and return None per field rather than guessing. `decimals: None` is why the
/// holdings endpoint will show an exact integer with no rendered form instead
/// of a plausible wrong number.
pub fn native_token_from_properties(props: &serde_json::Value) -> AssetMeta {
    let first = |v: Option<&serde_json::Value>| -> Option<serde_json::Value> {
        match v {
            Some(serde_json::Value::Array(items)) => items.first().cloned(),
            other => other.cloned(),
        }
    };
    let symbol = first(props.get("tokenSymbol"))
        .and_then(|v| v.as_str().map(str::to_string));
    let decimals = first(props.get("tokenDecimals"))
        .and_then(|v| json_u128(&v))
        .and_then(|d| u8::try_from(d).ok());
    AssetMeta {
        // system_properties carries no NAME, and "DOT" is a symbol, not a
        // name — this module's own rule is that NULL means unknown
        name: None,
        symbol,
        decimals,
    }
}

/// The asset key a location names WITHOUT consulting any chain's metadata.
///
/// Only an EMPTY interior is unambiguous: that is the holding chain's own
/// native currency, whatever chain that turns out to be. Anything else names
/// an asset by pallet index (`PalletInstance(50)`), and which instance index
/// 50 is depends on the runtime — so it is resolved at READ time by joining
/// `location_key` against `core.assets`, and stays None here. Guessing would
/// mean guessing a pallet index on a chain we are not currently looking at,
/// which is exactly the class of assumption that turned 20,895 USDT into
/// "20895000000" in the first place.
/// PARENTS ARE PART OF THE NAME. `{parents: 0, interior: Here}` is the holding
/// chain's own currency; `{parents: 1, interior: Here}` is its RELAY's, which
/// happens to be the same token on every system parachain and will not be on
/// the first chain with a token of its own (Hydration, Phase 3). Only
/// parents 0 is resolved here; anything else goes to the read-time join, which
/// can consult the registry.
pub fn metadata_free_asset_key(asset_location: &serde_json::Value) -> Option<String> {
    let parents = asset_location.get("parents").and_then(json_u128).unwrap_or(0);
    let interior = asset_location.get("interior")?.as_array()?;
    (parents == 0 && interior.is_empty()).then(|| "native".to_string())
}

// --------------------------------------------------------- metadata plumbing

/// One pallet-assets instance found in a runtime's own metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetsPallet {
    /// Metadata pallet name ("Assets", "ForeignAssets").
    pub name: String,
    /// STORAGE prefix — what twox128 hashes. Usually equal to `name`, but it
    /// is a separate field in metadata and the runtime, not us, decides it.
    pub storage_prefix: String,
    /// Runtime pallet index — the `PalletInstance` an XCM location names.
    pub index: u8,
    /// Lowercased name, matching the event-name prefix the decoder writes.
    pub event_pallet: String,
    pub representation: Representation,
    /// `asset_key` prefix for this instance.
    pub key_prefix: String,
}

/// Find every pallet-assets instance in a metadata blob, by SHAPE rather than
/// by name: a pallet with storage entries `Asset`, `Account` and `Metadata` is
/// an assets pallet. So a chain that runs a fourth instance is indexed without
/// a code change, and one that renames an instance keeps working.
pub fn assets_pallets_from_metadata(metadata_blob: &[u8]) -> Result<Vec<AssetsPallet>, String> {
    let prefixed = RuntimeMetadataPrefixed::decode(&mut &metadata_blob[..])
        .map_err(|e| format!("metadata blob undecodable: {e}"))?;

    macro_rules! walk {
        ($m:expr) => {{
            let mut out: Vec<AssetsPallet> = Vec::new();
            for pallet in &$m.pallets {
                let Some(storage) = pallet.storage.as_ref() else {
                    continue;
                };
                let has = |n: &str| storage.entries.iter().any(|e| e.name == n);
                if !(has("Asset") && has("Account") && has("Metadata")) {
                    continue;
                }
                let event_pallet = pallet.name.to_lowercase();
                out.push(AssetsPallet {
                    name: pallet.name.clone(),
                    storage_prefix: storage.prefix.clone(),
                    index: pallet.index,
                    representation: representation_for_pallet(&event_pallet),
                    key_prefix: key_prefix_for_pallet(&event_pallet),
                    event_pallet,
                });
            }
            out
        }};
    }

    Ok(match &prefixed.1 {
        RuntimeMetadata::V14(m) => walk!(m),
        RuntimeMetadata::V15(m) => walk!(m),
        RuntimeMetadata::V16(m) => walk!(m),
        _ => return Err("unsupported metadata version (v14/v15/v16 only)".into()),
    })
}

/// A storage map's hashers and its key/value types, read from the runtime's
/// own metadata. Nothing here assumes pallet-assets uses Blake2_128Concat —
/// it does (verified stable 4.0.0 → 52.0.0), but a chain that differs would
/// otherwise be silently mis-keyed, which is the worst possible failure for a
/// balance read.
#[derive(Debug, Clone)]
pub struct StorageEntryInfo {
    pub hashers: Vec<StorageKeyHasher>,
    /// Key type id — a tuple type for a multi-key map.
    pub key_type: Option<u32>,
    pub value_type: u32,
    pub types: scale_info::PortableRegistry,
}

/// The hasher set, mirrored out of frame-metadata so callers (and tests) need
/// no versioned imports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageKeyHasher {
    Blake2_128,
    Blake2_256,
    Blake2_128Concat,
    Twox128,
    Twox256,
    Twox64Concat,
    Identity,
}

impl StorageKeyHasher {
    /// Hash `bytes` for use in a storage key.
    pub fn hash(&self, bytes: &[u8]) -> Vec<u8> {
        match self {
            StorageKeyHasher::Blake2_128 => blake2_128(bytes).to_vec(),
            StorageKeyHasher::Blake2_256 => crate::calls::blake2_256(bytes).to_vec(),
            StorageKeyHasher::Blake2_128Concat => {
                let mut out = blake2_128(bytes).to_vec();
                out.extend_from_slice(bytes);
                out
            }
            StorageKeyHasher::Twox128 => crate::votes::twox_128(bytes).to_vec(),
            StorageKeyHasher::Twox256 => {
                let mut out = Vec::with_capacity(32);
                out.extend_from_slice(&crate::votes::twox_128(bytes));
                // twox_256 = xxh64(seed 0..3) concatenated; the third and
                // fourth words continue the same series
                out.extend_from_slice(&twox_128_high(bytes));
                out
            }
            StorageKeyHasher::Twox64Concat => {
                let mut out = crate::votes::twox_64(bytes).to_vec();
                out.extend_from_slice(bytes);
                out
            }
            StorageKeyHasher::Identity => bytes.to_vec(),
        }
    }

    /// How many bytes of HASH precede the raw key inside a key segment, and
    /// whether the raw key follows it at all. Only concat (and identity)
    /// hashers keep the key, which is what makes key enumeration possible.
    pub fn concat_prefix_len(&self) -> Option<usize> {
        match self {
            StorageKeyHasher::Blake2_128Concat => Some(16),
            StorageKeyHasher::Twox64Concat => Some(8),
            StorageKeyHasher::Identity => Some(0),
            _ => None,
        }
    }
}

fn blake2_128(bytes: &[u8]) -> [u8; 16] {
    use blake2::digest::consts::U16;
    use blake2::digest::Digest;
    let mut hasher = blake2::Blake2b::<U16>::new();
    hasher.update(bytes);
    let out = hasher.finalize();
    let mut fixed = [0u8; 16];
    fixed.copy_from_slice(&out);
    fixed
}

/// The upper half of twox_256 (words 2 and 3 of the xxh64 series).
fn twox_128_high(bytes: &[u8]) -> [u8; 16] {
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&crate::votes::xxh64(bytes, 2).to_le_bytes());
    out[8..].copy_from_slice(&crate::votes::xxh64(bytes, 3).to_le_bytes());
    out
}

/// Read one storage entry's hashers and types out of a metadata blob.
pub fn storage_entry_info(
    metadata_blob: &[u8],
    storage_prefix: &str,
    entry_name: &str,
) -> Result<StorageEntryInfo, String> {
    let prefixed = RuntimeMetadataPrefixed::decode(&mut &metadata_blob[..])
        .map_err(|e| format!("metadata blob undecodable: {e}"))?;

    macro_rules! entry_info {
        ($m:expr, $ver:ident) => {{
            use frame_metadata::$ver::{StorageEntryType, StorageHasher};
            let pallet = $m
                .pallets
                .iter()
                .find(|p| {
                    p.storage
                        .as_ref()
                        .is_some_and(|s| s.prefix == storage_prefix)
                })
                .ok_or_else(|| format!("no pallet with storage prefix '{storage_prefix}'"))?;
            let storage = pallet.storage.as_ref().expect("filtered on Some");
            let entry = storage
                .entries
                .iter()
                .find(|e| e.name == entry_name)
                .ok_or_else(|| format!("{storage_prefix}.{entry_name} not found"))?;
            let map_hasher = |h: &StorageHasher| match h {
                StorageHasher::Blake2_128 => StorageKeyHasher::Blake2_128,
                StorageHasher::Blake2_256 => StorageKeyHasher::Blake2_256,
                StorageHasher::Blake2_128Concat => StorageKeyHasher::Blake2_128Concat,
                StorageHasher::Twox128 => StorageKeyHasher::Twox128,
                StorageHasher::Twox256 => StorageKeyHasher::Twox256,
                StorageHasher::Twox64Concat => StorageKeyHasher::Twox64Concat,
                StorageHasher::Identity => StorageKeyHasher::Identity,
            };
            match &entry.ty {
                StorageEntryType::Map {
                    hashers,
                    key,
                    value,
                } => StorageEntryInfo {
                    hashers: hashers.iter().map(map_hasher).collect(),
                    key_type: Some(key.id),
                    value_type: value.id,
                    types: $m.types.clone(),
                },
                StorageEntryType::Plain(value) => StorageEntryInfo {
                    hashers: vec![],
                    key_type: None,
                    value_type: value.id,
                    types: $m.types.clone(),
                },
            }
        }};
    }

    Ok(match &prefixed.1 {
        RuntimeMetadata::V14(m) => entry_info!(m, v14),
        RuntimeMetadata::V15(m) => entry_info!(m, v15),
        RuntimeMetadata::V16(m) => entry_info!(m, v16),
        _ => return Err("unsupported metadata version (v14/v15/v16 only)".into()),
    })
}

/// twox128(prefix) ++ twox128(entry) — the map prefix every key starts with.
pub fn map_prefix(storage_prefix: &str, entry_name: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    out.extend_from_slice(&crate::votes::twox_128(storage_prefix.as_bytes()));
    out.extend_from_slice(&crate::votes::twox_128(entry_name.as_bytes()));
    out
}

/// The `<Pallet>.Account(asset_id, who)` key, built from the asset id's RAW
/// ENCODED BYTES rather than a re-encoded value.
///
/// This is the trick that makes foreign assets readable without a
/// metadata-driven SCALE encoder: a concat hasher stores the encoded key
/// alongside its hash, so `sync-assets` lifts those bytes out of the key it
/// enumerated, `core.assets.raw_key_bytes` keeps them, and every later read
/// hashes bytes we already have. An XCM Location we have only ever seen as
/// JSON never has to be encoded back.
pub fn asset_account_key(
    storage_prefix: &str,
    hashers: &[StorageKeyHasher],
    asset_id_bytes: &[u8],
    account: &[u8; 32],
) -> Result<Vec<u8>, String> {
    if hashers.len() != 2 {
        return Err(format!(
            "{storage_prefix}.Account has {} hasher(s); a double map needs 2",
            hashers.len()
        ));
    }
    let mut key = map_prefix(storage_prefix, "Account");
    key.extend_from_slice(&hashers[0].hash(asset_id_bytes));
    key.extend_from_slice(&hashers[1].hash(&account[..]));
    Ok(key)
}

/// The single-key `<Pallet>.<Entry>(asset_id)` key (Asset, Metadata).
pub fn asset_map_key(
    storage_prefix: &str,
    entry_name: &str,
    hashers: &[StorageKeyHasher],
    asset_id_bytes: &[u8],
) -> Result<Vec<u8>, String> {
    if hashers.len() != 1 {
        return Err(format!(
            "{storage_prefix}.{entry_name} has {} hasher(s); a map needs 1",
            hashers.len()
        ));
    }
    let mut key = map_prefix(storage_prefix, entry_name);
    key.extend_from_slice(&hashers[0].hash(asset_id_bytes));
    Ok(key)
}

/// Lift the RAW ENCODED asset id back out of a `<Pallet>.Asset` storage key.
/// Errors loudly on a non-concat hasher: with those, the key is unrecoverable
/// and enumerating assets is impossible — which must be said, never guessed
/// around.
pub fn asset_id_bytes_from_key(
    key: &[u8],
    hashers: &[StorageKeyHasher],
) -> Result<Vec<u8>, String> {
    let hasher = hashers
        .first()
        .ok_or("storage entry has no hashers — not a map")?;
    let skip = hasher.concat_prefix_len().ok_or_else(|| {
        format!(
            "hasher {hasher:?} does not keep the key (no `Concat`) — asset ids \
             cannot be enumerated from storage keys on this runtime"
        )
    })?;
    if key.len() < 32 + skip {
        return Err(format!("storage key too short ({} bytes)", key.len()));
    }
    Ok(key[32 + skip..].to_vec())
}

// ------------------------------------------------------------- state decoding

/// One account's holding of one asset. There is NO free/reserved split:
/// `AssetAccount { balance, <frozen-or-status>, reason, extra }` — four fields
/// in every version 4.0.0 → 52.0.0, but field two was renamed and retyped
/// (`is_frozen: bool` → `status: AccountStatus`) at 23.0.0.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetHolding {
    pub balance: u128,
    /// liquid | frozen | blocked, LOWERCASED — note `AssetDetailsView.status`
    /// keeps the runtime's own capitalisation (`Live`), because that one is an
    /// asset-level enum a reader will compare against pallet docs while this
    /// one is dotlens vocabulary shared with the anchors table.
    /// Pre-23.0.0 runtimes carry `is_frozen: bool` instead of an
    /// `AccountStatus`; both map to this vocabulary so a historic anchor is
    /// comparable to a modern one.
    pub status: Option<String>,
}

/// Decode a raw `<Pallet>.Account` storage VALUE against the metadata blob
/// archived for that block's spec_version.
pub fn decode_asset_account(
    metadata_blob: &[u8],
    storage_prefix: &str,
    value_bytes: &[u8],
) -> Result<AssetHolding, String> {
    let info = storage_entry_info(metadata_blob, storage_prefix, "Account")?;
    decode_asset_account_with(&info, value_bytes)
}

/// As above, against a storage entry resolved ONCE.
///
/// `storage_entry_info` re-decodes the whole (~580 KB) metadata blob and clones
/// the type registry on every call, which is invisible in a unit test and
/// ruinous in the holdings sweep: accounts × assets calls would be thousands of
/// full metadata decodes, and the operator would read the resulting minutes of
/// CPU as a hung RPC (both reviewers caught this independently). Callers in a
/// loop resolve the entry once and use these.
pub fn decode_asset_account_with(
    info: &StorageEntryInfo,
    value_bytes: &[u8],
) -> Result<AssetHolding, String> {
    let value = decode_value(info, value_bytes, "AssetAccount")?;
    let balance = named_u128(&value, "balance").ok_or("AssetAccount has no `balance`")?;
    let status = named_field(&value, "status")
        .and_then(variant_name)
        .map(|n| n.to_lowercase())
        .or_else(|| {
            named_bool(&value, "is_frozen").map(|f| {
                if f {
                    "frozen".to_string()
                } else {
                    "liquid".to_string()
                }
            })
        });
    Ok(AssetHolding { balance, status })
}

/// `<Pallet>.Metadata` — the symbol/decimals without which an amount is just
/// a number. NULL fields stay NULL; nothing here is defaulted.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AssetMeta {
    pub name: Option<String>,
    pub symbol: Option<String>,
    pub decimals: Option<u8>,
}

pub fn decode_asset_metadata(
    metadata_blob: &[u8],
    storage_prefix: &str,
    value_bytes: &[u8],
) -> Result<AssetMeta, String> {
    let info = storage_entry_info(metadata_blob, storage_prefix, "Metadata")?;
    decode_asset_metadata_with(&info, value_bytes)
}

pub fn decode_asset_metadata_with(
    info: &StorageEntryInfo,
    value_bytes: &[u8],
) -> Result<AssetMeta, String> {
    let value = decode_value(info, value_bytes, "AssetMetadata")?;
    Ok(AssetMeta {
        name: named_string(&value, "name"),
        symbol: named_string(&value, "symbol"),
        decimals: named_u128(&value, "decimals").and_then(|d| u8::try_from(d).ok()),
    })
}

/// `<Pallet>.Asset` — chain-wide facts about the asset itself. `supply` is
/// total issuance, NOT anyone's holding; the column it lands in says so.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AssetDetailsView {
    pub supply: Option<u128>,
    pub min_balance: Option<u128>,
    pub is_sufficient: Option<bool>,
    pub accounts: Option<u64>,
    /// Live | Frozen | Destroying — `Destroying` is why a holder's running
    /// total may stop being true without a single per-account event.
    pub status: Option<String>,
}

pub fn decode_asset_details(
    metadata_blob: &[u8],
    storage_prefix: &str,
    value_bytes: &[u8],
) -> Result<AssetDetailsView, String> {
    let info = storage_entry_info(metadata_blob, storage_prefix, "Asset")?;
    decode_asset_details_with(&info, value_bytes)
}

pub fn decode_asset_details_with(
    info: &StorageEntryInfo,
    value_bytes: &[u8],
) -> Result<AssetDetailsView, String> {
    let value = decode_value(info, value_bytes, "AssetDetails")?;
    Ok(AssetDetailsView {
        supply: named_u128(&value, "supply"),
        min_balance: named_u128(&value, "min_balance"),
        is_sufficient: named_bool(&value, "is_sufficient"),
        accounts: named_u128(&value, "accounts").and_then(|a| u64::try_from(a).ok()),
        status: named_field(&value, "status")
            .and_then(variant_name)
            .map(|s| s.to_string()),
    })
}

/// Decode the asset id in a storage key's raw bytes back into JSON, using the
/// key TYPE from metadata. For a double map the key type is a tuple, so the
/// caller passes the single-key `Asset` entry.
pub fn decode_asset_id(
    metadata_blob: &[u8],
    storage_prefix: &str,
    id_bytes: &[u8],
) -> Result<serde_json::Value, String> {
    let info = storage_entry_info(metadata_blob, storage_prefix, "Asset")?;
    decode_asset_id_with(&info, id_bytes)
}

pub fn decode_asset_id_with(
    info: &StorageEntryInfo,
    id_bytes: &[u8],
) -> Result<serde_json::Value, String> {
    let type_id = info.key_type.ok_or("Asset entry is not a map")?;
    let mut cursor = id_bytes;
    let value = scale_value::scale::decode_as_type(&mut cursor, type_id, &info.types)
        .map_err(|e| format!("asset id decode: {e}"))?;
    if !cursor.is_empty() {
        return Err(format!(
            "asset id decode left {} trailing byte(s) — key shape mismatch",
            cursor.len()
        ));
    }
    Ok(crate::frame_decoder::value_to_json(&value.remove_context()))
}

fn decode_value(
    info: &StorageEntryInfo,
    bytes: &[u8],
    what: &str,
) -> Result<Value<()>, String> {
    let mut cursor = bytes;
    let value = scale_value::scale::decode_as_type(&mut cursor, info.value_type, &info.types)
        .map_err(|e| format!("{what} decode: {e}"))?;
    // same discipline as `decode_asset_id` and `calls::decode_call`: a value
    // that decodes successfully but leaves bytes behind is a shape mismatch,
    // not a success
    if !cursor.is_empty() {
        return Err(format!(
            "{what} decode left {} trailing byte(s) — shape mismatch",
            cursor.len()
        ));
    }
    Ok(value.remove_context())
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

fn named_bool(v: &Value<()>, name: &str) -> Option<bool> {
    match &named_field(v, name)?.value {
        ValueDef::Primitive(scale_value::Primitive::Bool(b)) => Some(*b),
        _ => None,
    }
}

fn variant_name(v: &Value<()>) -> Option<&str> {
    match &v.value {
        ValueDef::Variant(var) => Some(var.name.as_str()),
        _ => None,
    }
}

/// `BoundedVec<u8, StringLimit>` → String. Bytes are collected out of however
/// many composite layers the newtype adds; invalid UTF-8 yields None rather
/// than a lossy string that would then be stored as if it were the symbol.
fn named_string(v: &Value<()>, name: &str) -> Option<String> {
    let field = named_field(v, name)?;
    if let ValueDef::Primitive(scale_value::Primitive::String(s)) = &field.value {
        return Some(s.clone());
    }
    let mut bytes = Vec::new();
    if !collect_u8(field, &mut bytes) || bytes.is_empty() {
        return None;
    }
    String::from_utf8(bytes).ok()
}

fn collect_u8(v: &Value<()>, out: &mut Vec<u8>) -> bool {
    match &v.value {
        ValueDef::Primitive(scale_value::Primitive::U128(n)) if *n <= 255 => {
            out.push(*n as u8);
            true
        }
        ValueDef::Composite(Composite::Unnamed(items)) => items.iter().all(|i| collect_u8(i, out)),
        ValueDef::Composite(Composite::Named(fields)) => {
            fields.iter().all(|(_, i)| collect_u8(i, out))
        }
        _ => false,
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

    fn acct(a: &[u8; 32]) -> serde_json::Value {
        serde_json::json!([a.to_vec()])
    }

    #[test]
    fn transfer_maps_to_double_entry_on_the_asset_key() {
        let from = pallet_account(b"py/trsry");
        let to = para_sovereign(2034);
        let e = ev(
            "assets.Transferred",
            serde_json::json!({
                "asset_id": 1984, "from": acct(&from), "to": acct(&to),
                "amount": 20895000000u64
            }),
        );
        let ds = deltas_for_assets_event(&e).unwrap();
        assert_eq!(ds.len(), 2);
        assert_eq!(ds[0].asset, "assets:1984");
        assert_eq!(ds[1].asset, "assets:1984");
        assert_eq!(ds[0].account, from.to_vec());
        assert!(ds[0].negative);
        assert_eq!(ds[0].magnitude, 20_895_000_000);
        assert_eq!(ds[0].counterparty.as_deref(), Some(&to[..]));
        assert!(!ds[1].negative);
    }

    #[test]
    fn pool_and_foreign_instances_produce_their_own_key_space() {
        let who = para_sovereign(1000);
        let pool = ev(
            "poolassets.Issued",
            serde_json::json!({"asset_id": 12, "owner": acct(&who), "amount": 5}),
        );
        assert_eq!(deltas_for_assets_event(&pool).unwrap()[0].asset, "pool:12");

        // a bridged ERC-20: parents 2 = outside our consensus (ECOSYSTEM §4)
        let foreign = ev(
            "foreignassets.Issued",
            serde_json::json!({
                "asset_id": {"parents": 2, "interior": {"X2": [[
                    {"GlobalConsensus": {"Ethereum": {"chain_id": 1}}},
                    {"AccountKey20": {"network": {"None": []}, "key": vec![0xdau8; 20]}}
                ]]}},
                "owner": acct(&who), "amount": 7
            }),
        );
        let key = &deltas_for_assets_event(&foreign).unwrap()[0].asset;
        assert!(key.starts_with("foreign:"), "{key}");
        assert!(key.contains("\"parents\":2"), "{key}");
        assert!(key.contains("0xdadadada"), "byte arrays render as hex: {key}");
        assert!(key.contains("\"network\":null"), "None collapses: {key}");
    }

    /// The version-shape trap slice 5 met on real data, as a test: the SAME
    /// asset spelled V3 (Concrete + flat X1) and V4 (nested X1) must produce
    /// ONE key, or one logical USDC becomes two assets.
    /// THE KEY-COLLISION TEST, written in the shapes our DECODER really
    /// produces rather than the shapes a human would type.
    ///
    /// That distinction is the whole test: a reviewer found that
    /// `PalletInstance(50)` decodes as `{"PalletInstance":[50]}` — one array
    /// layer, because our decoder renders a variant's unnamed fields as an
    /// array — while a location we CONSTRUCT has none. Written by hand on both
    /// sides, this test passed while the constructed and decoded keys could
    /// never compare equal, which would have silently killed the spend → asset
    /// join. So every spelling below is the decoder's, and the constructed
    /// form is checked AGAINST them.
    #[test]
    fn every_spelling_of_one_asset_shares_a_key() {
        // VersionedLocatableAsset::V3 → newtype variant → 1-element array;
        // V3 wraps the asset id in `Concrete` (another newtype) and its `X2`
        // holds the junctions FLAT
        let v3 = serde_json::json!({"V3": [{"Concrete": [{
            "parents": 0,
            "interior": {"X2": [{"PalletInstance": [50]}, {"GeneralIndex": [1984]}]}
        }]}]});
        // V4/V5 drop `Concrete`, and `X2([Junction; 2])` adds an array layer
        let v4 = serde_json::json!({"V4": [{
            "parents": 0,
            "interior": {"X2": [[{"PalletInstance": [50]}, {"GeneralIndex": [1984]}]]}
        }]});
        // and the same thing typed by a human, which is what a CONSTRUCTED
        // location looks like
        let scalar = serde_json::json!({
            "parents": 0,
            "interior": {"X2": [[{"PalletInstance": 50}, {"GeneralIndex": 1984}]]}
        });
        let a = canonical_location(&v3).expect("v3 normalizes");
        let b = canonical_location(&v4).expect("v4 normalizes");
        assert_eq!(a, b);
        assert_eq!(a, canonical_location(&scalar).unwrap());
        // the newtype layer is GONE, not preserved on one side
        assert_eq!(
            a,
            r#"{"interior":[{"PalletInstance":50},{"GeneralIndex":1984}],"parents":0}"#
        );
        // and it is the location `local_asset_location` constructs for the
        // trust-backed instance at pallet index 50 — the join that renders
        // spend 265 as USDT rather than as a bare number
        assert_eq!(a, canonical_location(&local_asset_location(50, 1984)).unwrap());
        // X1 in both spellings, too
        let x1_flat =
            serde_json::json!({"V3": {"parents": 1, "interior": {"X1": [{"Parachain": [1000]}]}}});
        let x1_nested =
            serde_json::json!({"V4": {"parents": 1, "interior": {"X1": [[{"Parachain": [1000]}]]}}});
        assert_eq!(
            canonical_location(&x1_flat).unwrap(),
            canonical_location(&x1_nested).unwrap()
        );
        // Here, in each of its spellings
        for here in [
            serde_json::json!({"parents": 0, "interior": "Here"}),
            serde_json::json!({"parents": 0, "interior": {"Here": []}}),
            serde_json::json!({"V4": {"parents": 0, "interior": {"Here": []}}}),
        ] {
            assert_eq!(
                canonical_location(&here).unwrap(),
                r#"{"interior":[],"parents":0}"#
            );
        }
    }

    #[test]
    fn locatable_asset_splits_chain_from_asset() {
        // spend 265's real shape, COPIED VERBATIM out of
        // `treasury.spend_events.asset_kind` for Asset Hub #17523677 event 12.
        // Note what a hand-written version gets wrong: `asset_id` is
        // `AssetId(pub Location)`, so it decodes ONE ARRAY DEEPER than its
        // sibling `location`, and every junction argument is newtype-wrapped
        // too (`{"PalletInstance": [50]}`, not `{"PalletInstance": 50}`).
        let ah = serde_json::json!({"V4": {
            "asset_id": [{"parents": 0, "interior": {"X2": [[
                {"PalletInstance": [50]}, {"GeneralIndex": [1984]}
            ]]}}],
            "location": {"parents": 0, "interior": {"Here": []}}
        }});
        let parts = locatable_asset_parts(&ah).expect("splits");
        assert_eq!(parts["chain"]["interior"], serde_json::json!([]));
        assert_eq!(parts["asset"]["interior"][1]["GeneralIndex"], 1984);

        // THE PROPERTY THAT ACTUALLY MATTERS, and the one both previous
        // versions of this test missed: the spend's normalized asset must
        // compare EQUAL to the location dotlens CONSTRUCTS for that asset in
        // `core.assets`. If these two strings differ the join is dead and a
        // spend can never be rendered in USDT, which is the whole point of the
        // column. Comparing the decoded form against the constructed form is
        // what makes this a real check rather than two copies of one guess.
        assert_eq!(
            canonical_location(&parts["asset"]).expect("canonical"),
            canonical_location(&local_asset_location(50, 1984)).expect("canonical"),
            "a decoded spend asset and a constructed core.assets location must \
             render to the same key, or the spend→asset join can never match"
        );

        // spend 202's real shape: approved on the RELAY, so the chain is
        // Parachain(1000) — Asset Hub — and V3 wraps the asset in Concrete
        let relay = serde_json::json!({"V3": {
            "location": {"parents": 0, "interior": {"X1": [{"Parachain": 1000}]}},
            "asset_id": {"Concrete": {"parents": 0, "interior":
                {"X2": [{"PalletInstance": 50}, {"GeneralIndex": 1337}]}}}
        }});
        let parts = relay_parts(&relay);
        assert_eq!(parts["chain"]["interior"][0]["Parachain"], 1000);
        assert_eq!(parts["asset"]["interior"][1]["GeneralIndex"], 1337);
    }

    fn relay_parts(v: &serde_json::Value) -> serde_json::Value {
        locatable_asset_parts(v).expect("splits")
    }

    #[test]
    fn issued_reads_the_amount_under_both_of_its_names() {
        let who = para_sovereign(1000);
        // pre-15.0.0: the field is named total_supply and carries the AMOUNT
        let old = ev(
            "assets.Issued",
            serde_json::json!({"asset_id": 1337, "owner": acct(&who), "total_supply": 500}),
        );
        let ds = deltas_for_assets_event(&old).unwrap();
        assert_eq!((ds[0].magnitude, ds[0].reason.as_str()), (500, "issued"));
        // 15.0.0+
        let new = ev(
            "assets.Issued",
            serde_json::json!({"asset_id": 1337, "owner": acct(&who), "amount": 500}),
        );
        assert_eq!(deltas_for_assets_event(&new).unwrap()[0].magnitude, 500);
        // positional form works for both, since the field never moved
        let positional = ev(
            "assets.Issued",
            serde_json::json!([1337, acct(&who), 500]),
        );
        assert_eq!(deltas_for_assets_event(&positional).unwrap()[0].magnitude, 500);
    }

    #[test]
    fn approved_transfer_credits_the_destination_not_the_delegate() {
        let owner = para_sovereign(1000);
        let delegate = para_sovereign(1001);
        let destination = para_sovereign(1004);
        let e = ev(
            "assets.TransferredApproved",
            serde_json::json!({
                "asset_id": 1984, "owner": acct(&owner), "delegate": acct(&delegate),
                "destination": acct(&destination), "amount": 100
            }),
        );
        let ds = deltas_for_assets_event(&e).unwrap();
        assert_eq!(ds.len(), 2);
        assert_eq!(ds[0].account, owner.to_vec());
        assert_eq!(ds[1].account, destination.to_vec());
        assert!(
            ds.iter().all(|d| d.account != delegate.to_vec()),
            "the delegate is an authority, not a party"
        );
    }

    #[test]
    fn status_events_and_supply_events_move_no_balance() {
        let who = para_sovereign(1000);
        for name in [
            "assets.Frozen",
            "assets.Thawed",
            "assets.Blocked",
            "assets.Touched",
            "assets.ApprovedTransfer",
            "assets.ApprovalCancelled",
            "assets.AccountsDestroyed",
            "assets.ReservesUpdated",
            "assets.IssuedCredit",
            "assets.BurnedDebt",
            "assets.MetadataSet",
            "assets.Created",
        ] {
            let e = ev(
                name,
                serde_json::json!({"asset_id": 1984, "who": acct(&who), "amount": 1}),
            );
            assert!(
                deltas_for_assets_event(&e).unwrap().is_empty(),
                "{name} must map to ∅"
            );
        }
        // other pallets are not this mapper's money
        let other = ev("balances.Transfer", serde_json::json!({}));
        assert!(deltas_for_assets_event(&other).unwrap().is_empty());
        // …and an unmapped assets-shaped pallet is left alone too, rather than
        // being half-mapped under a guessed key
        let unmapped = ev(
            "someotherassets.Transferred",
            serde_json::json!({"asset_id": 1, "from": acct(&who), "to": acct(&who), "amount": 1}),
        );
        assert!(deltas_for_assets_event(&unmapped).unwrap().is_empty());
    }

    #[test]
    fn unknown_and_malformed_events_halt_loudly() {
        let who = para_sovereign(1000);
        let unknown = ev(
            "assets.SomeFutureEvent",
            serde_json::json!({"asset_id": 1984, "who": acct(&who), "amount": 1}),
        );
        assert!(deltas_for_assets_event(&unknown).is_err());
        // a transfer whose asset_id is unreadable must NOT fall back to a
        // default asset — that would credit the wrong currency
        let bad_asset = ev(
            "assets.Transferred",
            serde_json::json!({"from": acct(&who), "to": acct(&who), "amount": 1}),
        );
        assert!(deltas_for_assets_event(&bad_asset).is_err());
        let bad_amount = ev(
            "assets.Burned",
            serde_json::json!({"asset_id": 1984, "owner": acct(&who)}),
        );
        assert!(deltas_for_assets_event(&bad_amount).is_err());
    }

    #[test]
    fn self_transfer_is_empty_not_a_pk_collision() {
        let a = para_sovereign(1000);
        let e = ev(
            "assets.Transferred",
            serde_json::json!({"asset_id": 1, "from": acct(&a), "to": acct(&a), "amount": 5}),
        );
        assert!(deltas_for_assets_event(&e).unwrap().is_empty());
    }

    #[test]
    fn concat_hashers_round_trip_a_key_and_others_refuse() {
        let hashers = vec![StorageKeyHasher::Blake2_128Concat];
        let id = 1984u32.encode_le();
        let key = asset_map_key("Assets", "Asset", &hashers, &id).unwrap();
        assert_eq!(key.len(), 32 + 16 + 4);
        assert_eq!(asset_id_bytes_from_key(&key, &hashers).unwrap(), id);
        // a non-concat hasher cannot give the key back, and says so
        let opaque = vec![StorageKeyHasher::Blake2_128];
        assert!(asset_id_bytes_from_key(&key, &opaque).is_err());
        // the double-map key is prefix ++ hash(asset) ++ hash(account)
        let account = para_sovereign(1000);
        let two = vec![
            StorageKeyHasher::Blake2_128Concat,
            StorageKeyHasher::Blake2_128Concat,
        ];
        let akey = asset_account_key("Assets", &two, &id, &account).unwrap();
        assert_eq!(akey.len(), 32 + 16 + 4 + 16 + 32);
        assert_eq!(&akey[..32], &map_prefix("Assets", "Account")[..]);
        assert_eq!(&akey[akey.len() - 32..], &account[..]);
        // and the hasher count is checked rather than assumed
        assert!(asset_account_key("Assets", &hashers, &id, &account).is_err());
    }

    /// blake2_128 against an independently computed vector, the same
    /// discipline as `accounts::SYSTEM_ACCOUNT_PREFIX` and
    /// `votes::VOTING_FOR_PREFIX`: a storage hasher that is silently wrong
    /// reads someone else's balance.
    #[test]
    fn blake2_128_matches_a_known_vector() {
        assert_eq!(hex::encode(blake2_128(b"")), "cae66941d9efbd404e4d88758ea67670");
        assert_eq!(
            hex::encode(blake2_128(b"abc")),
            "cf4ab791c62b8d2b2109c90275287816"
        );
    }

    trait EncodeLe {
        fn encode_le(&self) -> Vec<u8>;
    }
    impl EncodeLe for u32 {
        fn encode_le(&self) -> Vec<u8> {
            self.to_le_bytes().to_vec()
        }
    }

    /// The assets pallets of a REAL runtime, found by shape. The committed
    /// Asset Hub fixture is spec 2003002.
    #[test]
    fn assets_pallets_and_storage_shapes_come_from_real_metadata() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/real/polkadot-asset-hub-19498783/metadata.scale");
        let Ok(blob) = std::fs::read(&path) else {
            eprintln!("SKIP: real fixture metadata not present at {}", path.display());
            return;
        };
        let pallets = assets_pallets_from_metadata(&blob).expect("walks");
        let by_key: std::collections::BTreeMap<_, _> = pallets
            .iter()
            .map(|p| (p.key_prefix.clone(), p.clone()))
            .collect();
        for expected in ["assets", "foreign", "pool"] {
            assert!(
                by_key.contains_key(expected),
                "expected an instance keyed '{expected}', found {:?}",
                by_key.keys().collect::<Vec<_>>()
            );
        }
        let trust_backed = &by_key["assets"];
        assert_eq!(trust_backed.representation, Representation::TrustBacked);
        assert_eq!(trust_backed.event_pallet, "assets");

        // hashers read from the runtime, not assumed
        let info = storage_entry_info(&blob, &trust_backed.storage_prefix, "Account").unwrap();
        assert_eq!(
            info.hashers,
            vec![
                StorageKeyHasher::Blake2_128Concat,
                StorageKeyHasher::Blake2_128Concat
            ]
        );
        let asset_info = storage_entry_info(&blob, &trust_backed.storage_prefix, "Asset").unwrap();
        assert_eq!(asset_info.hashers, vec![StorageKeyHasher::Blake2_128Concat]);

        // an integer asset id round-trips through the key and back to JSON
        let id_bytes = 1984u32.to_le_bytes().to_vec();
        let key = asset_map_key(
            &trust_backed.storage_prefix,
            "Asset",
            &asset_info.hashers,
            &id_bytes,
        )
        .unwrap();
        let back = asset_id_bytes_from_key(&key, &asset_info.hashers).unwrap();
        let id_json = decode_asset_id(&blob, &trust_backed.storage_prefix, &back).unwrap();
        assert_eq!(
            asset_key_from_id(&trust_backed.key_prefix, &id_json).as_deref(),
            Some("assets:1984")
        );

        // the foreign instance's key type is a Location, and a location id
        // decodes back to a location-shaped key
        let foreign = &by_key["foreign"];
        assert_eq!(foreign.representation, Representation::Foreign);
        let finfo = storage_entry_info(&blob, &foreign.storage_prefix, "Asset").unwrap();
        assert_eq!(finfo.hashers, vec![StorageKeyHasher::Blake2_128Concat]);
    }

    /// AssetAccount / AssetMetadata / AssetDetails decoded from hand-built
    /// SCALE bytes against REAL metadata — the shapes are pinned by the
    /// runtime, never by a hand-written JSON fixture (the rule that caught
    /// slice 3's two blockers).
    #[test]
    fn asset_state_decodes_against_real_metadata() {
        use parity_scale_codec::Encode;
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/real/polkadot-asset-hub-19498783/metadata.scale");
        let Ok(blob) = std::fs::read(&path) else {
            eprintln!("SKIP: real fixture metadata not present at {}", path.display());
            return;
        };
        let pallets = assets_pallets_from_metadata(&blob).expect("walks");
        let tb = pallets
            .iter()
            .find(|p| p.representation == Representation::TrustBacked)
            .expect("trust-backed instance");

        // AssetAccount { balance: u128, status: AccountStatus, reason:
        // ExistenceReason, extra: () } — status index 0 = Liquid, reason
        // index 1 = Sufficient (both are unit-ish variants at these indices in
        // pallet-assets; if the runtime disagrees the decode fails loudly and
        // this test is how we find out)
        let mut bytes = Vec::new();
        20_895_000_000u128.encode_to(&mut bytes);
        bytes.push(0); // AccountStatus::Liquid
        bytes.push(1); // ExistenceReason::Sufficient
        let holding = decode_asset_account(&blob, &tb.storage_prefix, &bytes);
        match holding {
            Ok(h) => {
                assert_eq!(h.balance, 20_895_000_000);
                assert_eq!(h.status.as_deref(), Some("liquid"));
            }
            Err(e) => panic!("AssetAccount decode failed against real metadata: {e}"),
        }

        // AssetMetadata { deposit: u128, name: BoundedVec<u8>,
        // symbol: BoundedVec<u8>, decimals: u8, is_frozen: bool }
        let mut m = Vec::new();
        0u128.encode_to(&mut m);
        b"Tether USD".to_vec().encode_to(&mut m);
        b"USDT".to_vec().encode_to(&mut m);
        m.push(6);
        m.push(0);
        let meta = decode_asset_metadata(&blob, &tb.storage_prefix, &m)
            .expect("AssetMetadata decodes");
        assert_eq!(meta.symbol.as_deref(), Some("USDT"));
        assert_eq!(meta.name.as_deref(), Some("Tether USD"));
        assert_eq!(meta.decimals, Some(6));
    }
}
