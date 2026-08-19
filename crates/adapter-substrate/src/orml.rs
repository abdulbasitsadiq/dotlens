//! ORML money semantics — the vocabulary Hydration keeps its money in, and the
//! asset identity that makes it addable to anybody else's (Invariant 4:
//! protocol specifics live in adapters only). Four pure pieces, no I/O:
//!
//!   1. `deltas_for_orml_event` — `tokens.*` events → the SAME `BalanceDelta`
//!      the balances and assets mappers emit, tagged `tokens:<currency_id>`.
//!      Third vocabulary, same fact, same tables, same worker.
//!   2. `absolutize` — a Location is RELATIVE TO ITS OBSERVER, so two chains
//!      spell one asset differently and version-stripping does not make them
//!      equal. This rebases an observed Location onto the global-consensus root.
//!   3. AssetRegistry state reads (`Assets`, `AssetLocations`), decoded against
//!      block-correct metadata.
//!   4. The orml-tokens `Accounts` double-map key, whose two halves are in the
//!      OPPOSITE order to pallet-assets' — with a lifter so a caller can prove
//!      it built the key right instead of hoping.
//!
//! ------------------------------------------------------------------- events
//!
//! THE VOCABULARY IS BYTE-STABLE. Every published orml-tokens (0.6.7 → 0.10.0)
//! was diffed programmatically: **17 variants**, identical names, identical
//! field names, identical field ORDER, no `#[codec(index)]`, nothing added and
//! nothing removed in the pallet's entire published history. So unlike
//! pallet-assets — 52 versions with six separate additions — the loud-halt arm
//! at the bottom of this mapper is UNREACHABLE on any runtime we can decode, and
//! coverage is 100% without a version matrix.
//!
//! **READ EVERY FIELD BY NAME. THERE IS NO POSITIONAL FALLBACK HERE, AND ITS
//! ABSENCE IS THE POINT.** `crate::balances::field` falls back to an array index
//! when the data is not an object, which is right for pallet-balances and wrong
//! twice over here:
//!   (a) `core.events.data` is **jsonb**, and jsonb DOES NOT PRESERVE KEY ORDER.
//!       A positional index into a decoded named-field event is meaningless
//!       whatever order the runtime declared it in.
//!   (b) All 17 variants carry NAMED fields, so the decoder renders every one of
//!       them as an OBJECT and a positional arm could never fire anyway.
//! Copying pallet-balances' `field(data, "who", 0)` shape would therefore add a
//! limb that is dead where it is safe and dangerous where it is not. `named()`
//! below reads objects only.
//!
//! And the field-ORDER folklore that a positional fallback would have encoded is
//! false on its own terms: `currency_id` is field 0 for **15 of 17** variants,
//! not all — `LockSet(lock_id, currency_id, who, amount)` and
//! `LockRemoved(lock_id, currency_id, who)` put `lock_id` first. Both are in the
//! ∅ set, so nothing was ever going to bite, which is exactly why that kind of
//! sentence survives unchallenged until somebody checks it.
//!
//! MONEY MOVES — 6 variants, read from the pallet's EMISSION SITES rather than
//! from their names (an orml account is `{free, reserved, frozen}`, so total =
//! free + reserved and a reserve move within one account changes nothing):
//!   Transfer{currency_id, from, to, amount}            → −from, +to
//!   ReserveRepatriated{currency_id, from, to, amount,
//!                      status}                         → −from, +to
//!       `status` says whether it lands free or reserved at the destination,
//!       which is a placement question and not a total one.
//!   Deposited{currency_id, who, amount}                → +who
//!   Withdrawn{currency_id, who, amount}                → −who
//!   DustLost{currency_id, who, amount}                 → −who
//!   Slashed{currency_id, who, free_amount,
//!           reserved_amount}                           → −(free + reserved)
//!       TWO amount fields, both required. A reader that took only
//!       `free_amount` would under-report every slash of a reserved position,
//!       and one that took the first number it found would under-report all of
//!       them.
//!
//! ABSOLUTE, NOT A DELTA:
//!   BalanceSet{currency_id, who, free, reserved}       → zero-magnitude MARKER
//!       Exactly what `balances.BalanceSet` gets, for exactly the same reason:
//!       the event carries the NEW VALUE, no delta is derivable, and the history
//!       API nulls running totals past a `balance_set_unquantified` row until
//!       the next anchor. Never silent, never guessed.
//!
//! DELIBERATE ∅ — 10 variants, with the reason attached, because "no delta"
//! must never mean "we did not think about it":
//!   Endowed              fires from `mutate_account` when an account starts
//!                        EXISTING, ALONGSIDE whatever actually credited it
//!                        (orml-tokens `lib.rs:801`). Counting it would double
//!                        every first deposit. **MEASURED, not reasoned: 438 of
//!                        438 `tokens.Endowed` in a 2,000-block Hydration window
//!                        have a same-block credit with matching account, asset
//!                        AND amount — zero orphans.**
//!   Reserved / Unreserved            free ↔ reserved inside one account
//!   Locked / Unlocked / LockSet /
//!   LockRemoved                      lock bookkeeping; a lock is a freeze on
//!                                    `free`, not a move out of it
//!   TotalIssuanceSet / Issued /
//!   Rescinded                        supply-side, and they name NO ACCOUNT, so
//!                                    there is no per-account delta to derive
//!
//! ------------------------------------------------- and why `currencies.*` is ∅
//!
//! Hydration runs a FORK of orml-currencies specifically because (their comment)
//! "the latest versions of the orml-currencies pallet don't emit events". It is
//! a WRAPPER: it dispatches to pallet-balances or to orml-tokens depending on
//! the asset, and emits its own event either way. Mapping both vocabularies
//! double-counts; mapping the wrapper instead of the inner one loses money. Both
//! halves of that were measured over the same 2,000 blocks rather than argued:
//!
//!   `currencies.Transferred` 1275 = **458** mirrored exactly by `tokens.Transfer`
//!       (asset_type Token, id ≠ 0)
//!                                  + **378** mirrored exactly by `balances.Transfer`
//!       (id 0, HDX)
//!                                  + **439** mirrored by NOTHING
//!   `currencies.Deposited`    835 = 711 + 124, zero residue
//!   `currencies.Withdrawn`    154 =  56 +  98, zero residue
//!
//! So mapping both would double-count **1,225 movements in 2,000 blocks**, while
//! mapping `currencies.*` INSTEAD of `tokens.*` would miss 45% of token
//! transfers (370 of 828 have no wrapper mirror) and 83% of the HDX ones. Map
//! `tokens.*`; `currencies.*` is ∅ with that measurement as the reason.
//!
//! THE 439-ROW RESIDUE IS THE MONEY MARKET, and it sharpens the scope boundary
//! rather than moving it. All 439 span exactly 13 asset ids, every one of which
//! the registry calls **`asset_type: Erc20`** (222 HOLLAR, 1001 aDOT, 1002 aUSDT,
//! 1003 aUSDC, 1007 aETH, 1039 aPAXG, 55 BIL, 550 uBIL, 69 GDOT, 420 GETH,
//! 1110 HUSDC, 1111 HUSDT, 4444 HEURC). Their movements ARE visible in the event
//! stream, carrying account, asset and amount — and they are still not mapped
//! here, for two reasons that are about the MAPPER rather than about the data:
//!   * a pure event mapper cannot tell an Erc20 asset from a Token one, because
//!     `asset_type` lives in registry STATE, and a mapper that consulted state
//!     would stop being a pure function of an event;
//!   * a delta with no anchorable balance is not a position — the balance itself
//!     is in `pallet_evm` storage, so there is nothing to anchor it against.
//! Worth revisiting the day a contracts module lands, with the residue already
//! identified. **AND THE CLEAN SPLIT IS FALSE AT THE BOUNDARY**: Erc20 assets
//! 222, 4444 and 55 DO appear in `tokens.*` — but only as `Unreserved`/
//! `Withdrawn`, never `Transfer`/`Deposited`. So "Erc20 ⇒ not in orml-tokens" is
//! wrong, and this mapper must not filter on asset type at all. It does not.
//!
//! `Currencies` has **FOUR** variants — Transferred, BalanceUpdated, Deposited,
//! Withdrawn — so the ∅ arm enumerates all four and halts on a fifth, which is
//! the same discipline the money arms get.
//!
//! ------------------------------------------------------------- the asset key
//!
//! `tokens:<currency_id>`, joining the key space `balances.*.asset` already
//! holds (`native`, `assets:1984`, `pool:12`, `foreign:<loc>`). `CurrencyId` is
//! a plain `AssetId = u32` on Hydration, so the key is the number.
//!
//! **AND `tokens:0` IS REFUSED, LOUDLY.** On Hydration `NativeAssetId =
//! CORE_ASSET_ID = 0` (`runtime/hydradx/src/system.rs:267`), so asset 0 is HDX,
//! which is ALSO the `pallet_balances` token — a `tokens:0` row would count HDX
//! twice and the wrong number would look entirely plausible. MEASURED: asset 0
//! appears in **0 of 2,209** `tokens.*` events in a 2,000-block window; HDX moves
//! exclusively through `pallet_balances`, and id 0 surfaces in the token
//! vocabulary only through `currencies.Transferred` (378 rows), which is ∅
//! anyway. So this is a guard against a SHAPE, not against observed traffic.
//!
//! `ORML_NATIVE_CURRENCY_ID` below is the orml convention and is NOT read from
//! metadata — a pure event mapper has no metadata. It is not left as a bare
//! assumption either: `native_currency_id_from_metadata` reads the runtime's OWN
//! declaration out of its constants, and `sync-assets` refuses to run against a
//! chain whose runtime disagrees. The guess is pinned by a check, in the one
//! place that can afford to make it.

use canonical::CanonicalEvent;
use frame_metadata::{RuntimeMetadata, RuntimeMetadataPrefixed};
use ingest::balances::BalanceDelta;
use parity_scale_codec::Decode;
use scale_value::{Composite, Value, ValueDef};

use crate::assets::{StorageEntryInfo, StorageKeyHasher};
use crate::balances::{json_account_bytes, json_u128};

/// This mapper's own rule-set generation — **documentation only.**
///
/// It is deliberately NOT the lineage recorded on a row: orml deltas ride the
/// balances worker and are stamped with `balances::MAPPER_VERSION`, which is
/// exactly why that constant moved 2 → 3 with this slice. Two version numbers
/// for one rule set, one of them unread, is how the next orml rule change gets
/// bumped in the constant nobody records — so this one says so out loud, and
/// **any change to the rules below must move `balances::MAPPER_VERSION` too.**
pub const ORML_MAPPER_VERSION: u32 = 1;

/// The `asset_key` prefix for orml-tokens balances.
pub const TOKENS_KEY_PREFIX: &str = "tokens";

/// The currency id that duplicates the chain's `pallet_balances` token. See the
/// module header: this is the orml convention, checked against the runtime's own
/// constant at sync time rather than trusted.
pub const ORML_NATIVE_CURRENCY_ID: u128 = 0;

/// Pallet names (as the decoder lowercases them) this mapper owns. `tokens` is
/// mapped; `currencies` is the wrapper and is deliberately ∅ — but it is OWNED,
/// so a fifth variant appearing there halts us instead of passing silently.
const TOKENS_PALLET: &str = "tokens";
const CURRENCIES_PALLET: &str = "currencies";

// ------------------------------------------------------------------ the mapper

/// orml event → per-account deltas. ∅ for every event that is not this mapper's;
/// LOUD errors for an unknown or malformed event on a pallet it does own.
pub fn deltas_for_orml_event(event: &CanonicalEvent) -> Result<Vec<BalanceDelta>, String> {
    let Some((pallet, variant)) = event.name.split_once('.') else {
        return Ok(vec![]);
    };
    match pallet {
        TOKENS_PALLET => tokens_deltas(event, variant),
        CURRENCIES_PALLET => currencies_deltas(event, variant),
        _ => Ok(vec![]),
    }
}

fn tokens_deltas(event: &CanonicalEvent, variant: &str) -> Result<Vec<BalanceDelta>, String> {
    let data = &event.data;
    let ctx = |what: &str| format!("{}: {what} (data: {data})", event.name);

    // resolved ONCE, and never defaulted: a delta on the wrong currency is worse
    // than no delta, because it silently credits a different asset.
    let key = || -> Result<String, String> {
        let id = named(data, "currency_id").ok_or_else(|| ctx("no currency_id"))?;
        let n = currency_id_from_json(id).ok_or_else(|| ctx("currency_id is not an integer"))?;
        if n == ORML_NATIVE_CURRENCY_ID {
            return Err(format!(
                "{}: currency_id {n} is the chain's NATIVE token, which \
                 pallet_balances already reports — recording it as \
                 `{TOKENS_KEY_PREFIX}:{n}` would count the same money twice. \
                 Measured zero occurrences in 2,000 Hydration blocks; if this \
                 fires, the runtime's NativeAssetId is not {n} and \
                 ORML_NATIVE_CURRENCY_ID must be revisited (data: {data})",
                event.name
            ));
        }
        Ok(format!("{TOKENS_KEY_PREFIX}:{n}"))
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
        // double entry between two accounts. `ReserveRepatriated` differs only
        // in WHERE the money lands at the destination (its `status` field), and
        // placement is not a total.
        "Transfer" | "ReserveRepatriated" => {
            let asset = key()?;
            let from = named_account(data, "from").ok_or_else(|| ctx("no from"))?;
            let to = named_account(data, "to").ok_or_else(|| ctx("no to"))?;
            let amount = named_amount(data, "amount").ok_or_else(|| ctx("no amount"))?;
            let (out, into) = if variant == "Transfer" {
                ("transfer_out", "transfer_in")
            } else {
                ("reserve_repatriated_out", "reserve_repatriated_in")
            };
            if from == to {
                vec![] // net zero, and both rows would collide on the PK
            } else {
                vec![
                    d(&asset, from, amount, true, out, Some(to)),
                    d(&asset, to, amount, false, into, Some(from)),
                ]
            }
        }
        "Deposited" | "Withdrawn" | "DustLost" => {
            let asset = key()?;
            let who = named_account(data, "who").ok_or_else(|| ctx("no who"))?;
            let amount = named_amount(data, "amount").ok_or_else(|| ctx("no amount"))?;
            let (negative, reason) = match variant {
                "Deposited" => (false, "deposit"),
                "Withdrawn" => (true, "withdraw"),
                _ => (true, "dust_lost"),
            };
            vec![d(&asset, who, amount, negative, reason, None)]
        }
        // TWO amounts, both required. A slash can take free funds, reserved
        // funds, or both, and the pallet reports them separately.
        "Slashed" => {
            let asset = key()?;
            let who = named_account(data, "who").ok_or_else(|| ctx("no who"))?;
            let free = named_amount(data, "free_amount").ok_or_else(|| ctx("no free_amount"))?;
            let reserved =
                named_amount(data, "reserved_amount").ok_or_else(|| ctx("no reserved_amount"))?;
            let total = free
                .checked_add(reserved)
                .ok_or_else(|| ctx("free_amount + reserved_amount overflows u128"))?;
            if total == 0 {
                // a slash of nothing is not a movement; emitting a zero row
                // would be indistinguishable from the BalanceSet marker
                vec![]
            } else {
                vec![d(&asset, who, total, true, "slashed", None)]
            }
        }
        // ABSOLUTE value, no delta derivable — the pallet-balances rule, applied
        // to the pallet-balances shape one vocabulary over.
        "BalanceSet" => {
            let asset = key()?;
            let who = named_account(data, "who").ok_or_else(|| ctx("no who"))?;
            vec![d(&asset, who, 0, false, "balance_set_unquantified", None)]
        }
        // deliberate ∅ — see the module docs for the reason attached to each
        "Endowed" | "Reserved" | "Unreserved" | "Locked" | "Unlocked" | "LockSet"
        | "LockRemoved" | "TotalIssuanceSet" | "Issued" | "Rescinded" => vec![],
        other => {
            return Err(format!(
                "unknown tokens event {other} — orml mapper update required. The \
                 orml-tokens Event enum has carried exactly 17 variants, \
                 byte-identical, in every published version 0.6.7..=0.10.0, so \
                 this arm should be unreachable: reaching it means a fork or a \
                 vocabulary this project has never seen"
            ))
        }
    })
}

/// The wrapper pallet. Every variant is ∅ — see the module header for the
/// measurement — but the pallet is OWNED, so a fifth variant halts loudly rather
/// than being silently ignored as somebody else's money.
fn currencies_deltas(event: &CanonicalEvent, variant: &str) -> Result<Vec<BalanceDelta>, String> {
    match variant {
        "Transferred" | "BalanceUpdated" | "Deposited" | "Withdrawn" => Ok(vec![]),
        other => Err(format!(
            "unknown currencies event {other} — orml mapper update required. \
             All four known variants map to ∅ because they MIRROR \
             `tokens.*`/`balances.*` events that are already counted (measured: \
             1,225 duplicate movements in 2,000 blocks); a fifth variant must be \
             checked for the same overlap before it is given the same treatment \
             (event: {})",
            event.name
        )),
    }
}

// -------------------------------------------------------- JSON field plumbing
//
// NAMED ONLY. See the module header: `core.events.data` is jsonb (no key order)
// and all 17 variants carry named fields, so a positional fallback would be
// dead where it is safe and wrong where it is not.

fn named<'a>(data: &'a serde_json::Value, name: &str) -> Option<&'a serde_json::Value> {
    data.as_object()?.get(name)
}

/// Accounts arrive DOUBLE-NESTED — `[[32 bytes]]`, the AccountId32 newtype layer
/// this project first met on `Processed.id` — and never as SS58.
/// `json_account_bytes` walks arbitrary nesting, so it absorbs both that and any
/// further layer a future decoder adds. Measured 724/724 consistent.
fn named_account(data: &serde_json::Value, name: &str) -> Option<[u8; 32]> {
    named(data, name).and_then(json_account_bytes)
}

/// Amounts above `u64::MAX` render as a decimal STRING (the decoder's rule) —
/// **100 of 2,209 events, 4.5%**, all on 18-decimal assets. A reader calling
/// `as_u64()` drops every one of them silently. `json_u128` takes both.
fn named_amount(data: &serde_json::Value, name: &str) -> Option<u128> {
    named(data, name).and_then(json_u128)
}

/// A newtype-wrapped scalar decodes one array layer deep (`[5]`). Kept for the
/// day a runtime declares `CurrencyId` as a newtype rather than a bare `u32`;
/// Hydration's is bare, so this is unexercised there.
fn newtype_number(v: &serde_json::Value) -> Option<u128> {
    match v {
        serde_json::Value::Array(items) if items.len() == 1 => json_u128(&items[0]),
        _ => None,
    }
}

/// A decoded `CurrencyId` (from an event field or from a storage key) → the
/// number.
///
/// PUBLIC, AND SHARED WITH THE SYNC ON PURPOSE. `sync-assets` decides which
/// registry entry is the native alias and this mapper decides which event to
/// refuse; both questions are "what number is this currency id", and two
/// implementations of that could disagree — which is precisely the defect class
/// that made `api` depend on `sim` in slice 5 rather than re-implement thirty
/// lines of set arithmetic.
pub fn currency_id_from_json(v: &serde_json::Value) -> Option<u128> {
    json_u128(v).or_else(|| newtype_number(v))
}

/// The `balances.*.asset` key for an orml currency. One spelling, one place.
pub fn asset_key_for_currency(currency_id: u128) -> String {
    format!("{TOKENS_KEY_PREFIX}:{currency_id}")
}

// ------------------------------------------------------- absolute locations
//
// THE PROBLEM, and it is the reason a treasury consolidator cannot simply add
// two `location_key` columns together: **a Location is relative to its
// observer**. Hydration calls USDT
//     {parents: 1, X3[Parachain(1000), PalletInstance(50), GeneralIndex(1984)]}
// while Asset Hub calls the SAME asset
//     {parents: 0, X2[PalletInstance(50), GeneralIndex(1984)]}
// Version-stripping (`assets::canonical_location`) makes the SPELLINGS agree and
// cannot make the FRAMES agree — those two normalize to different strings and
// always will. DOT happens to match from both (`{parents: 1, Here}`), which is
// exactly the kind of coincidence that would have made a naive join look like it
// worked.
//
// THE FIX IS DERIVABLE FROM REGISTRY DATA ALONE, with no chain named in code:
// a chain's own absolute path is `[GlobalConsensus(network)]` for a relay and
// `[GlobalConsensus(network), Parachain(id)]` for a parachain, so absolutizing
// `{parents: p, interior: J}` observed from chain C is
//
//     drop the last `p` junctions of C's path, then append `J`
//
// Hand-checked on the two cases above: from Hydration, USDT drops
// `Parachain(2034)` and appends → `[GC(Polkadot), Parachain(1000),
// PalletInstance(50), GeneralIndex(1984)]`; from Asset Hub it drops nothing and
// appends → the identical list. DOT absolutizes to `[GC(Polkadot)]` from both.
//
// AN ABSOLUTE LOCATION IS A BARE JUNCTION ARRAY, deliberately — it carries no
// `parents`, because there is nothing left to be relative to. That also makes it
// structurally impossible to confuse with a relative Location (always an
// object), so the two can never accidentally compare equal.

/// One chain's absolute path, from registry data. `network` is the registry's
/// network name and `para_id` is `None` for a relay.
///
/// THE ONE SPELLING DECISION, stated because it is a guess: a `NetworkId` unit
/// variant renders from our decoder with its Rust capitalisation (`{"Polkadot":
/// []}`), while the registry stores network names lower-cased. Registry names
/// are single lower-case words matching those variants, so the first character
/// is upper-cased and nothing else is touched. A network whose `NetworkId`
/// variant is not simply its capitalised registry name would need the registry
/// to carry the variant explicitly — which is a seed field, not a code list.
pub fn chain_path(network: &str, para_id: Option<u32>) -> Vec<serde_json::Value> {
    let mut path = vec![serde_json::json!({
        "GlobalConsensus": { network_variant(network): [] }
    })];
    if let Some(id) = para_id {
        path.push(serde_json::json!({ "Parachain": id }));
    }
    path
}

fn network_variant(network: &str) -> String {
    let mut chars = network.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

/// Rebase a Location observed from `observer_path` onto the global-consensus
/// root. Returns the absolute JUNCTION ARRAY.
///
/// `None` when the location is not location-shaped, or when `parents` reaches
/// ABOVE the global consensus root — which is not a place, so refusing is the
/// only honest answer. (`parents: 2` from a parachain is fine and common: it
/// leaves the consensus entirely, and the appended interior names where it went.)
pub fn absolutize(
    observer_path: &[serde_json::Value],
    location: &serde_json::Value,
) -> Option<serde_json::Value> {
    let normalized = crate::assets::normalize_location(location)?;
    // `as usize` would WRAP a nonsense parent count into a plausible small one,
    // and the bounds check below would then pass on it. Convert fallibly: a
    // `parents` that does not fit is refused with everything else that cannot
    // be absolutized.
    let parents = usize::try_from(normalized.get("parents").and_then(json_u128)?).ok()?;
    let interior = normalized.get("interior")?.as_array()?;
    if parents > observer_path.len() {
        return None;
    }
    // AN ALREADY-ABSOLUTE LOCATION IS REFUSED RATHER THAN RE-ROOTED. A Location
    // whose interior STARTS with `GlobalConsensus` names a consensus system
    // directly, so prepending the observer's path would produce
    // `[GC(ours), Parachain(us), GC(theirs), …]` — a nonsense key that would
    // still join against itself and therefore never look broken. Slice 4
    // recorded that the in-network absolute form (`remote:polkadot/…`) is
    // unobserved on live data, so this is a shape guard rather than a live bug.
    //
    // THE CONDITION IS "the path is NOT fully consumed", not "parents > 0" —
    // because `{parents: 2, X1[GlobalConsensus(Ethereum)]}` from a parachain is
    // the ORDINARY way to leave the consensus: it drops the whole path and the
    // interior is then correctly the entire absolute name. Only a `GlobalConsensus`
    // arriving while some of the observer's own path still remains is nonsense.
    if parents < observer_path.len()
        && interior
            .first()
            .is_some_and(|j| j.get("GlobalConsensus").is_some())
    {
        return None;
    }
    let mut out: Vec<serde_json::Value> =
        observer_path[..observer_path.len() - parents].to_vec();
    out.extend(interior.iter().cloned());
    Some(serde_json::Value::Array(out))
}

/// The canonical STRING form of an absolute location — the join handle across
/// chains, and the fungible half of PRODUCT.md's asset-identity graph arriving
/// early. serde_json sorts object keys (its `Map` is a `BTreeMap` unless
/// `preserve_order` is enabled, which this workspace does not enable), so this
/// is stable across runs and processes, exactly like `canonical_location`.
pub fn absolute_key(
    observer_path: &[serde_json::Value],
    location: &serde_json::Value,
) -> Option<String> {
    absolutize(observer_path, location).map(|v| v.to_string())
}

// ------------------------------------------------------ AssetRegistry (state)

/// The pallets found in a runtime's metadata by the SHAPE of their storage,
/// never by their names — the same discipline `assets_pallets_from_metadata`
/// uses, so a chain that renames an instance keeps working.
///
/// **AT MOST ONE OF EACH, AND A SECOND IS AN ERROR RATHER THAN A SILENT DROP.**
/// This differs from `assets_pallets_from_metadata`, which returns a `Vec`
/// because pallet-assets is genuinely instantiable (Asset Hub runs three). No
/// chain we index runs two AssetRegistrys or two orml-tokens, and the honest
/// response to one that does is to halt: quietly keeping the first would leave
/// an entire second money pallet reading as zero, which is the failure this
/// module's header is written against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrmlPallets {
    /// The `AssetRegistry`-shaped pallet: `Assets` + `AssetLocations`.
    pub registry: Option<OrmlPallet>,
    /// The `orml-tokens`-shaped pallet: `Accounts` + `TotalIssuance`.
    pub tokens: Option<OrmlPallet>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrmlPallet {
    pub name: String,
    /// STORAGE prefix — what twox128 hashes. A separate field in metadata from
    /// the pallet name, and the runtime decides it, not us.
    pub storage_prefix: String,
    pub index: u8,
    /// Lowercased name, matching the event-name prefix the decoder writes.
    pub event_pallet: String,
}

/// Find the AssetRegistry and orml-tokens pallets in a metadata blob by shape.
///
/// `AssetRegistry` is recognised by `Assets` + `AssetLocations` — the pair that
/// makes the location join possible — and orml-tokens by `Accounts` +
/// `TotalIssuance`. Neither collides with pallet-assets, which is recognised by
/// `Asset` + `Account` + `Metadata` (all singular, and with a `Metadata` map
/// that neither orml pallet has), so a chain with no ORML pallets returns two
/// `None`s rather than a false positive.
pub fn orml_pallets_from_metadata(metadata_blob: &[u8]) -> Result<OrmlPallets, String> {
    let prefixed = RuntimeMetadataPrefixed::decode(&mut &metadata_blob[..])
        .map_err(|e| format!("metadata blob undecodable: {e}"))?;

    macro_rules! walk {
        ($m:expr) => {{
            let mut registry: Option<OrmlPallet> = None;
            let mut tokens: Option<OrmlPallet> = None;
            for pallet in &$m.pallets {
                let Some(storage) = pallet.storage.as_ref() else {
                    continue;
                };
                let has = |n: &str| storage.entries.iter().any(|e| e.name == n);
                let found = OrmlPallet {
                    name: pallet.name.clone(),
                    storage_prefix: storage.prefix.clone(),
                    index: pallet.index,
                    event_pallet: pallet.name.to_lowercase(),
                };
                // a SECOND match halts; see the type's doc for why keeping the
                // first would hide an entire money pallet
                for (matches, slot, what) in [
                    (has("Assets") && has("AssetLocations"), &mut registry, "AssetRegistry"),
                    (has("Accounts") && has("TotalIssuance"), &mut tokens, "orml-tokens"),
                ] {
                    if !matches {
                        continue;
                    }
                    if let Some(first) = slot.as_ref() {
                        return Err(format!(
                            "two {what}-shaped pallets in one runtime ('{}' and \
                             '{}') — this adapter indexes one of each, and \
                             silently keeping the first would leave the other's \
                             balances reading as zero",
                            first.name, pallet.name
                        ));
                    }
                    *slot = Some(found.clone());
                }
            }
            OrmlPallets { registry, tokens }
        }};
    }

    Ok(match &prefixed.1 {
        RuntimeMetadata::V14(m) => walk!(m),
        RuntimeMetadata::V15(m) => walk!(m),
        RuntimeMetadata::V16(m) => walk!(m),
        _ => return Err("unsupported metadata version (v14/v15/v16 only)".into()),
    })
}

/// The runtime's OWN declaration of which currency id is the native token.
///
/// This is what pins `ORML_NATIVE_CURRENCY_ID` — the mapper's guard is a
/// constant because a pure event mapper has no metadata, and this is the check
/// that constant is measured against at sync time. Read from pallet CONSTANTS by
/// name: orml-currencies declares `GetNativeCurrencyId`, and some runtimes also
/// carry `NativeAssetId` on the registry pallet.
///
/// `None` means the runtime declares neither — in which case a caller must NOT
/// assume 0, because the whole point of this function is to stop assuming. And
/// a runtime declaring BOTH names with DIFFERENT values is an error rather than
/// a first-one-wins: resolving that by metadata pallet order would be a guess in
/// the one function written to eliminate one.
pub fn native_currency_id_from_metadata(metadata_blob: &[u8]) -> Result<Option<u128>, String> {
    let prefixed = RuntimeMetadataPrefixed::decode(&mut &metadata_blob[..])
        .map_err(|e| format!("metadata blob undecodable: {e}"))?;

    macro_rules! walk {
        ($m:expr) => {{
            let mut found: Option<(String, u128)> = None;
            for pallet in &$m.pallets {
                for constant in &pallet.constants {
                    if constant.name != "GetNativeCurrencyId" && constant.name != "NativeAssetId" {
                        continue;
                    }
                    let mut cursor = &constant.value[..];
                    let Ok(value) =
                        scale_value::scale::decode_as_type(&mut cursor, constant.ty.id, &$m.types)
                    else {
                        continue;
                    };
                    let Some(n) = flat_u128(&value.remove_context()) else {
                        continue;
                    };
                    let here = format!("{}.{}", pallet.name, constant.name);
                    match &found {
                        Some((where_, prev)) if *prev != n => {
                            return Err(format!(
                                "runtime declares two DIFFERENT native currency \
                                 ids: {where_} = {prev}, {here} = {n}. Resolving \
                                 this by metadata order would be a guess in the \
                                 one function that exists to stop guessing"
                            ))
                        }
                        Some(_) => {}
                        None => found = Some((here, n)),
                    }
                }
            }
            found.map(|(_, n)| n)
        }};
    }

    Ok(match &prefixed.1 {
        RuntimeMetadata::V14(m) => walk!(m),
        RuntimeMetadata::V15(m) => walk!(m),
        RuntimeMetadata::V16(m) => walk!(m),
        _ => return Err("unsupported metadata version (v14/v15/v16 only)".into()),
    })
}

/// One asset as the AssetRegistry describes it.
///
/// **`name`, `symbol` and `decimals` are all `Option` in the pallet and really
/// are absent on real assets** — id 1000019 (`asset_type: External`) has none of
/// the three — so they stay `Option` here, `core.assets.decimals` stays
/// nullable, and the unit formatter REFUSES rather than assuming 12. Confirmed
/// values: 0 HDX 12dp, 5 DOT 10dp, 10 USDT 6dp, 15 vDOT 10dp, 22 USDC 6dp.
///
/// `xcm_rate_limit` is present in the value and deliberately NOT read: it is a
/// throughput policy knob with no reader here, and this project's rule is that a
/// column costing every write and serving no query gets added WITH its query.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RegistryAsset {
    pub name: Option<String>,
    pub symbol: Option<String>,
    pub decimals: Option<u8>,
    pub existential_deposit: Option<u128>,
    pub is_sufficient: Option<bool>,
    /// Token | XYK | StableSwap | Bond | External | Erc20 — the runtime's own
    /// capitalisation. THIS IS THE COLUMN THAT MAKES THIS SLICE'S SCOPE
    /// BOUNDARY VISIBLE RATHER THAN ONLY DOCUMENTED: an `Erc20` asset's balance
    /// lives in `pallet_evm` storage, so it has anchors from nowhere, and a
    /// reader can see that from the data instead of having to be told.
    pub asset_type: Option<String>,
}

/// Decode an `AssetRegistry.Assets` value against a resolved storage entry.
pub fn decode_registry_asset(
    info: &StorageEntryInfo,
    value_bytes: &[u8],
) -> Result<RegistryAsset, String> {
    let value = decode_value(info, value_bytes, "AssetDetails")?;
    Ok(RegistryAsset {
        name: named_string(&value, "name"),
        symbol: named_string(&value, "symbol"),
        decimals: named_u128_field(&value, "decimals").and_then(|d| u8::try_from(d).ok()),
        existential_deposit: named_u128_field(&value, "existential_deposit"),
        is_sufficient: named_bool(&value, "is_sufficient"),
        asset_type: named_field(&value, "asset_type")
            .and_then(variant_name)
            .map(str::to_string),
    })
}

/// Decode an `AssetRegistry.AssetLocations` value — an `AssetNativeLocation`,
/// which is a newtype over a `Location` — into normalized (version-stripped)
/// form, ready to be absolutized.
///
/// **A MISSING ENTRY IS A FACT, NOT A GAP.** HDX has no entry in
/// `AssetLocations` at all, because a native token has no XCM location: it IS
/// the chain. Callers must record that as "no location" and move on, never as a
/// failed read.
pub fn decode_asset_location(
    info: &StorageEntryInfo,
    value_bytes: &[u8],
) -> Result<serde_json::Value, String> {
    let value = decode_value(info, value_bytes, "AssetNativeLocation")?;
    let json = crate::frame_decoder::value_to_json(&value);
    crate::assets::normalize_location(&json)
        .ok_or_else(|| format!("AssetLocations value is not location-shaped: {json}"))
}

// -------------------------------------------------- orml-tokens storage keys

/// The `<Tokens>.Accounts(who, currency_id)` key.
///
/// **THE KEY HALVES ARE IN THE OPPOSITE ORDER TO pallet-assets', and that is the
/// one thing here that fails silently.** `assets::asset_account_key` builds
/// `hash(asset) ++ hash(account)`; orml-tokens declares
/// `Accounts: StorageDoubleMap<_, Blake2_128Concat, AccountId, Twox64Concat,
/// CurrencyId, AccountData>` — ACCOUNT FIRST. Building it the other way round
/// produces a well-formed key that simply never matches anything, which reads
/// exactly like "this account holds nothing" and would make an entire chain's
/// treasury position look like zero.
///
/// The hashers are still read from METADATA rather than assumed; only the ORDER
/// is stated here, and `accounts_key_parts` exists so a caller can prove the
/// order it built rather than hoping.
pub fn accounts_key(
    storage_prefix: &str,
    hashers: &[StorageKeyHasher],
    account: &[u8; 32],
    currency_id_bytes: &[u8],
) -> Result<Vec<u8>, String> {
    if hashers.len() != 2 {
        return Err(format!(
            "{storage_prefix}.Accounts has {} hasher(s); a double map needs 2",
            hashers.len()
        ));
    }
    let mut key = crate::assets::map_prefix(storage_prefix, "Accounts");
    key.extend_from_slice(&hashers[0].hash(&account[..]));
    key.extend_from_slice(&hashers[1].hash(currency_id_bytes));
    Ok(key)
}

/// Lift `(account, currency_id_bytes)` back out of an `Accounts` key.
///
/// Both hashers are concat on every orml runtime, which is what makes this
/// possible — and what makes it a CHECK rather than a convenience: a caller that
/// builds a key and lifts it back gets a 32-byte account only if it built the
/// halves in the right order. A non-concat hasher is refused loudly, because
/// with one the key is unrecoverable and that must be said, never guessed around.
pub fn accounts_key_parts(
    key: &[u8],
    hashers: &[StorageKeyHasher],
) -> Result<([u8; 32], Vec<u8>), String> {
    if hashers.len() != 2 {
        return Err(format!(
            "Accounts key needs 2 hashers, got {}",
            hashers.len()
        ));
    }
    let skip0 = hashers[0].concat_prefix_len().ok_or_else(|| {
        format!(
            "hasher {:?} does not keep the key (no `Concat`) — an Accounts key \
             cannot be taken apart on this runtime",
            hashers[0]
        )
    })?;
    let skip1 = hashers[1].concat_prefix_len().ok_or_else(|| {
        format!(
            "hasher {:?} does not keep the key (no `Concat`)",
            hashers[1]
        )
    })?;
    let start = 32 + skip0;
    if key.len() < start + 32 + skip1 {
        return Err(format!("Accounts key too short ({} bytes)", key.len()));
    }
    let account = <[u8; 32]>::try_from(&key[start..start + 32])
        .map_err(|_| "account segment is not 32 bytes".to_string())?;
    Ok((account, key[start + 32 + skip1..].to_vec()))
}

/// One account's orml holding of one currency.
///
/// **UNLIKE pallet-assets, THIS HAS A FREE/RESERVED SPLIT.** `orml_tokens::
/// AccountData { free, reserved, frozen }` is the pallet-balances shape, and
/// total is `free + reserved`. Migration 0010 records asset anchors as
/// `free = balance, reserved = 0` because a pallet-assets account genuinely has
/// one number; pushing an orml holding through that path would throw away its
/// reserved half. These anchors take the NATIVE writer instead, and this struct
/// converts so that rule lives in exactly one place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrmlHolding {
    pub free: u128,
    pub reserved: u128,
    pub frozen: Option<u128>,
}

impl OrmlHolding {
    pub fn total(&self) -> u128 {
        self.free + self.reserved
    }

    /// The shape `balances_pg::insert_anchor` takes — same struct the native
    /// System.Account path produces, because it is the same kind of fact.
    pub fn as_account_balances(&self) -> crate::balances::AccountBalances {
        crate::balances::AccountBalances {
            free: self.free,
            reserved: self.reserved,
            frozen: self.frozen,
        }
    }
}

/// Decode an `<Tokens>.Accounts` value against a resolved storage entry.
pub fn decode_orml_account(
    info: &StorageEntryInfo,
    value_bytes: &[u8],
) -> Result<OrmlHolding, String> {
    let value = decode_value(info, value_bytes, "orml AccountData")?;
    Ok(OrmlHolding {
        free: named_u128_field(&value, "free").ok_or("orml AccountData has no `free`")?,
        reserved: named_u128_field(&value, "reserved")
            .ok_or("orml AccountData has no `reserved`")?,
        frozen: named_u128_field(&value, "frozen"),
    })
}

// ------------------------------------------------------------ value plumbing
//
// Deliberately not shared with `assets.rs`: these read a DIFFERENT pallet's
// values, and a shared private helper set is how one module's field-name
// assumption quietly becomes another's.

fn decode_value(info: &StorageEntryInfo, bytes: &[u8], what: &str) -> Result<Value<()>, String> {
    let mut cursor = bytes;
    let value = scale_value::scale::decode_as_type(&mut cursor, info.value_type, &info.types)
        .map_err(|e| format!("{what} decode: {e}"))?;
    // a value that decodes successfully but leaves bytes behind is a shape
    // mismatch, not a success — the `decode_call`/`decode_asset_id` discipline
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

/// A field that may be wrapped in `Option` (the registry wraps `name`, `symbol`
/// and `decimals`) or in a newtype. Peels one variant/composite layer looking
/// for a number, and returns None for a genuine `None` — which is a fact, not a
/// failure.
fn named_u128_field(v: &Value<()>, name: &str) -> Option<u128> {
    flat_u128(named_field(v, name)?)
}

fn flat_u128(v: &Value<()>) -> Option<u128> {
    match &v.value {
        ValueDef::Primitive(scale_value::Primitive::U128(n)) => Some(*n),
        ValueDef::Variant(var) if var.name == "Some" => match &var.values {
            Composite::Unnamed(items) if items.len() == 1 => flat_u128(&items[0]),
            _ => None,
        },
        ValueDef::Composite(Composite::Unnamed(items)) if items.len() == 1 => flat_u128(&items[0]),
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

/// A `BoundedVec<u8>` (possibly inside an `Option`) → String. Invalid UTF-8
/// yields None rather than a lossy string that would then be stored as if it
/// were the symbol.
fn named_string(v: &Value<()>, name: &str) -> Option<String> {
    let field = named_field(v, name)?;
    let field = match &field.value {
        ValueDef::Variant(var) if var.name == "Some" => match &var.values {
            Composite::Unnamed(items) if items.len() == 1 => &items[0],
            _ => return None,
        },
        ValueDef::Variant(var) if var.name == "None" => return None,
        _ => field,
    };
    // NUL-PADDED, AND POSTGRES REFUSES A NUL IN TEXT OR JSONB — so an untrimmed
    // name is not a cosmetic blemish, it aborts the whole `sync-assets` run at
    // whichever asset happens to carry one. `AssetDetails.name`/`symbol` are
    // BoundedVec<u8>, and Hydration really does store trailing NULs in them
    // (asset 1000787, found at verification). This is slice 1's track-name
    // defect (`gov::track_name`, sdk #7671) on a second data path.
    let raw = if let ValueDef::Primitive(scale_value::Primitive::String(s)) = &field.value {
        s.clone()
    } else {
        let mut bytes = Vec::new();
        if !collect_u8(field, &mut bytes) || bytes.is_empty() {
            return None;
        }
        String::from_utf8(bytes).ok()?
    };
    let trimmed = raw.trim_end_matches('\0').trim();
    // An INTERIOR NUL survives the trim, and there is no honest way to store it:
    // `name` is already nullable and genuinely absent on real assets (1000019),
    // so a refusal reads as the absence it is rather than as a mangled string.
    if trimmed.is_empty() || trimmed.contains('\0') {
        return None;
    }
    Some(trimmed.to_string())
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
    use crate::accounts::{pallet_account, para_sovereign, sibling_sovereign};

    fn ev(name: &str, data: serde_json::Value) -> CanonicalEvent {
        CanonicalEvent {
            index: 0,
            transaction_index: None,
            name: name.into(),
            data,
        }
    }

    /// The shape our decoder really writes for an orml account: DOUBLE-nested
    /// bytes, measured 724/724 consistent on live Hydration data.
    fn acct(a: &[u8; 32]) -> serde_json::Value {
        serde_json::json!([[a.to_vec()]])
    }

    #[test]
    fn transfer_is_double_entry_on_the_currency_key() {
        let from = sibling_sovereign(1000);
        let to = para_sovereign(2034);
        let e = ev(
            "tokens.Transfer",
            serde_json::json!({
                "currency_id": 10, "from": acct(&from), "to": acct(&to),
                "amount": 20895000000u64
            }),
        );
        let ds = deltas_for_orml_event(&e).unwrap();
        assert_eq!(ds.len(), 2);
        assert_eq!(ds[0].asset, "tokens:10");
        assert_eq!(ds[1].asset, "tokens:10");
        assert_eq!(ds[0].account, from.to_vec());
        assert!(ds[0].negative);
        assert_eq!(ds[0].magnitude, 20_895_000_000);
        assert_eq!(ds[0].counterparty.as_deref(), Some(&to[..]));
        assert!(!ds[1].negative);
        assert_eq!(ds[1].account, to.to_vec());
        // a self-transfer is ∅, not a PK collision
        let self_move = ev(
            "tokens.Transfer",
            serde_json::json!({"currency_id": 10, "from": acct(&from), "to": acct(&from), "amount": 5}),
        );
        assert!(deltas_for_orml_event(&self_move).unwrap().is_empty());
    }

    /// 4.5% of live events carry an amount above u64::MAX as a decimal STRING
    /// (all on 18-decimal assets). A reader on `as_u64()` drops every one.
    #[test]
    fn amounts_above_u64_arrive_as_strings_and_are_not_dropped() {
        let who = para_sovereign(2034);
        let e = ev(
            "tokens.Deposited",
            serde_json::json!({
                "currency_id": 222, "who": acct(&who),
                "amount": "36893488147419103232"
            }),
        );
        let ds = deltas_for_orml_event(&e).unwrap();
        assert_eq!(ds.len(), 1);
        assert_eq!(ds[0].magnitude, 36_893_488_147_419_103_232u128);
        assert!(!ds[0].negative);
        assert_eq!(ds[0].reason, "deposit");
    }

    /// Two amount fields, both counted. A slash of a reserved position is
    /// invisible to a reader that takes only `free_amount`, and a reader that
    /// takes "the first number" gets the free half of every slash.
    #[test]
    fn slashed_sums_both_of_its_amounts() {
        let who = para_sovereign(2034);
        let e = ev(
            "tokens.Slashed",
            serde_json::json!({
                "currency_id": 5, "who": acct(&who),
                "free_amount": 700, "reserved_amount": 300
            }),
        );
        let ds = deltas_for_orml_event(&e).unwrap();
        assert_eq!(ds.len(), 1);
        assert_eq!(ds[0].magnitude, 1000, "free + reserved, not either alone");
        assert!(ds[0].negative);
        // a missing second field HALTS rather than silently halving the slash
        let half = ev(
            "tokens.Slashed",
            serde_json::json!({"currency_id": 5, "who": acct(&who), "free_amount": 700}),
        );
        assert!(deltas_for_orml_event(&half).is_err());
        // and a slash of nothing is not a movement (it would be
        // indistinguishable from the BalanceSet marker)
        let nothing = ev(
            "tokens.Slashed",
            serde_json::json!({
                "currency_id": 5, "who": acct(&who),
                "free_amount": 0, "reserved_amount": 0
            }),
        );
        assert!(deltas_for_orml_event(&nothing).unwrap().is_empty());
    }

    #[test]
    fn balance_set_is_an_unquantified_marker_not_a_delta() {
        let who = para_sovereign(2034);
        let e = ev(
            "tokens.BalanceSet",
            serde_json::json!({
                "currency_id": 15, "who": acct(&who), "free": 123, "reserved": 4
            }),
        );
        let ds = deltas_for_orml_event(&e).unwrap();
        assert_eq!(ds.len(), 1);
        assert_eq!(
            (ds[0].magnitude, ds[0].reason.as_str()),
            (0, "balance_set_unquantified"),
            "an absolute value yields no delta; the marker invalidates running \
             totals until the next anchor"
        );
    }

    /// The ∅ set, one arm per reason. `Endowed` is the load-bearing one:
    /// measured 438/438 accompanied by a same-block credit with matching
    /// account, asset AND amount, so counting it would have doubled exactly 438
    /// first deposits in one 2,000-block window.
    #[test]
    fn the_empty_set_is_deliberate_and_endowed_would_double_count() {
        let who = para_sovereign(2034);
        for name in [
            "tokens.Endowed",
            "tokens.Reserved",
            "tokens.Unreserved",
            "tokens.Locked",
            "tokens.Unlocked",
            "tokens.TotalIssuanceSet",
            "tokens.Issued",
            "tokens.Rescinded",
        ] {
            let e = ev(
                name,
                serde_json::json!({"currency_id": 5, "who": acct(&who), "amount": 42}),
            );
            assert!(
                deltas_for_orml_event(&e).unwrap().is_empty(),
                "{name} must map to ∅"
            );
        }
        // the two lock variants that put `lock_id` FIRST — the field-order
        // exception. Both are ∅, which is why nothing was ever going to bite.
        for name in ["tokens.LockSet", "tokens.LockRemoved"] {
            let e = ev(
                name,
                serde_json::json!({
                    "lock_id": [1,2,3,4,5,6,7,8], "currency_id": 5,
                    "who": acct(&who), "amount": 42
                }),
            );
            assert!(deltas_for_orml_event(&e).unwrap().is_empty(), "{name} → ∅");
        }

        // THE MEASUREMENT, as an assertion: Endowed beside the Deposited that
        // really credited the account must count the money ONCE.
        let endowed = ev(
            "tokens.Endowed",
            serde_json::json!({"currency_id": 5, "who": acct(&who), "amount": 1_000}),
        );
        let deposited = ev(
            "tokens.Deposited",
            serde_json::json!({"currency_id": 5, "who": acct(&who), "amount": 1_000}),
        );
        let total: u128 = [endowed, deposited]
            .iter()
            .flat_map(|e| deltas_for_orml_event(e).unwrap())
            .map(|d| d.magnitude)
            .sum();
        assert_eq!(total, 1_000, "the first deposit must be counted once");
    }

    /// All four `currencies.*` variants are ∅ — they MIRROR events already
    /// counted — and a fifth halts. Mapping both vocabularies would have
    /// double-counted 1,225 movements in 2,000 blocks.
    #[test]
    fn the_wrapper_pallet_is_empty_and_a_fifth_variant_halts() {
        let who = para_sovereign(2034);
        for name in [
            "currencies.Transferred",
            "currencies.BalanceUpdated",
            "currencies.Deposited",
            "currencies.Withdrawn",
        ] {
            let e = ev(
                name,
                serde_json::json!({"currency_id": 5, "who": acct(&who), "amount": 1}),
            );
            assert!(deltas_for_orml_event(&e).unwrap().is_empty(), "{name} → ∅");
        }
        let fifth = ev("currencies.SomeFutureEvent", serde_json::json!({}));
        assert!(deltas_for_orml_event(&fifth).is_err());

        // and the double-count this prevents, stated as arithmetic: one
        // movement seen through both vocabularies is counted once.
        let from = sibling_sovereign(1000);
        let inner = ev(
            "tokens.Transfer",
            serde_json::json!({
                "currency_id": 5, "from": acct(&from), "to": acct(&who), "amount": 500
            }),
        );
        let wrapper = ev(
            "currencies.Transferred",
            serde_json::json!({
                "currency_id": 5, "from": acct(&from), "to": acct(&who), "amount": 500
            }),
        );
        let out: u128 = [inner, wrapper]
            .iter()
            .flat_map(|e| deltas_for_orml_event(e).unwrap())
            .filter(|d| d.negative)
            .map(|d| d.magnitude)
            .sum();
        assert_eq!(out, 500, "one movement, counted once");
    }

    /// FOUND AT VERIFICATION, on live data, at asset 1000787: `AssetDetails.name`
    /// is a BoundedVec that Hydration pads with NULs, and Postgres refuses a NUL
    /// in TEXT — so an untrimmed name does not render badly, it ABORTS the whole
    /// `sync-assets` run at whichever asset happens to carry one. Slice 1 met the
    /// identical defect in `gov::track_name` (sdk #7671); this is its second data
    /// path. The interior-NUL arm refuses rather than mangles, because `name` is
    /// nullable and really is absent on real assets.
    #[test]
    fn nul_padded_names_are_trimmed_and_never_reach_the_database() {
        fn named(fields: Vec<(&str, Value<()>)>) -> Value<()> {
            Value::named_composite(
                fields.into_iter().map(|(k, v)| (k.to_string(), v)),
            )
        }
        fn bytes_of(s: &[u8]) -> Value<()> {
            Value::unnamed_composite(s.iter().map(|b| Value::u128(*b as u128)))
        }

        // trailing NUL padding, the live shape
        let v = named(vec![("name", bytes_of(b"HOLLAR\0\0\0\0"))]);
        assert_eq!(named_string(&v, "name").as_deref(), Some("HOLLAR"));

        // the String-primitive branch pads the same way
        let v = named(vec![("name", Value::string("HOLLAR\0\0"))]);
        assert_eq!(named_string(&v, "name").as_deref(), Some("HOLLAR"));

        // all padding and no name is an ABSENCE, not an empty string
        let v = named(vec![("name", bytes_of(b"\0\0\0"))]);
        assert_eq!(named_string(&v, "name"), None);

        // an INTERIOR NUL cannot be stored and is refused rather than mangled
        let v = named(vec![("name", bytes_of(b"HOL\0LAR"))]);
        assert_eq!(named_string(&v, "name"), None);

        // and nothing that survives may carry a NUL, which is the property the
        // database actually enforces
        for probe in [&b"USDT\0"[..], &b"  vDOT \0\0"[..], &b"aDOT"[..]] {
            if let Some(got) = named_string(&named(vec![("name", bytes_of(probe))]), "name") {
                assert!(!got.contains('\0'), "a stored name may never carry a NUL: {got:?}");
            }
        }
    }

    /// The native-token guard. Asset 0 on Hydration is HDX, which pallet_balances
    /// also reports — measured 0 of 2,209 events, so this guards a SHAPE.
    #[test]
    fn the_native_currency_id_is_refused_rather_than_double_counted() {
        let who = para_sovereign(2034);
        let e = ev(
            "tokens.Deposited",
            serde_json::json!({"currency_id": 0, "who": acct(&who), "amount": 1}),
        );
        let err = deltas_for_orml_event(&e).unwrap_err();
        assert!(err.contains("NATIVE"), "{err}");
        // The guard sits inside key resolution, and an ∅ arm never resolves a
        // key — so a LOCK on the native asset is still simply nothing, rather
        // than a halt on an event that was never going to move money anyway.
        let locked = ev(
            "tokens.Locked",
            serde_json::json!({"currency_id": 0, "who": acct(&who), "amount": 1}),
        );
        assert!(deltas_for_orml_event(&locked).unwrap().is_empty());
    }

    #[test]
    fn unknown_and_malformed_events_halt_loudly() {
        let who = para_sovereign(2034);
        let unknown = ev(
            "tokens.SomeFutureEvent",
            serde_json::json!({"currency_id": 5, "who": acct(&who), "amount": 1}),
        );
        assert!(deltas_for_orml_event(&unknown).is_err());
        // an unreadable currency must NOT fall back to a default asset
        let no_currency = ev(
            "tokens.Deposited",
            serde_json::json!({"who": acct(&who), "amount": 1}),
        );
        assert!(deltas_for_orml_event(&no_currency).is_err());
        let no_amount = ev(
            "tokens.Withdrawn",
            serde_json::json!({"currency_id": 5, "who": acct(&who)}),
        );
        assert!(deltas_for_orml_event(&no_amount).is_err());
        // other pallets are not this mapper's money
        let other = ev("balances.Transfer", serde_json::json!({}));
        assert!(deltas_for_orml_event(&other).unwrap().is_empty());
    }

    /// THE RULE THAT REPLACED THE POSITIONAL FALLBACK, as an assertion. jsonb
    /// does not preserve key order, so an array-shaped payload cannot be read
    /// positionally — and since every orml variant carries NAMED fields, an
    /// array here means the shape is not what we think it is. Halting is the
    /// only honest answer; guessing an index would credit whichever field
    /// happened to sort first.
    #[test]
    fn positional_payloads_are_refused_rather_than_indexed() {
        let who = para_sovereign(2034);
        let positional = ev(
            "tokens.Deposited",
            serde_json::json!([5, acct(&who), 1_000]),
        );
        assert!(
            deltas_for_orml_event(&positional).is_err(),
            "no positional fallback: jsonb loses key order and every orml \
             variant is named, so an index would be a guess"
        );
    }

    // ------------------------------------------------- absolute locations

    /// THE MEASUREMENT THIS SLICE RESTS ON, as a test: Hydration and Asset Hub
    /// spell USDT differently, `canonical_location` cannot make them equal, and
    /// absolutizing them against their own chain paths does.
    #[test]
    fn one_asset_seen_from_two_chains_absolutizes_to_one_key() {
        let hydration = chain_path("polkadot", Some(2034));
        let asset_hub = chain_path("polkadot", Some(1000));

        // as Hydration's AssetRegistry stores it
        let from_hydration = serde_json::json!({
            "parents": 1,
            "interior": {"X3": [[
                {"Parachain": [1000]}, {"PalletInstance": [50]}, {"GeneralIndex": [1984]}
            ]]}
        });
        // as Asset Hub names its own asset 1984
        let from_ah = crate::assets::local_asset_location(50, 1984);

        // FIRST, the negative half — without this the test would be proving
        // nothing, because two things that were already equal would still be.
        assert_ne!(
            crate::assets::canonical_location(&from_hydration).unwrap(),
            crate::assets::canonical_location(&from_ah).unwrap(),
            "version-stripping alone CANNOT reconcile two observers' frames — \
             that is the whole reason this function exists"
        );

        let a = absolute_key(&hydration, &from_hydration).expect("absolutizes");
        let b = absolute_key(&asset_hub, &from_ah).expect("absolutizes");
        assert_eq!(a, b);
        assert_eq!(
            a,
            r#"[{"GlobalConsensus":{"Polkadot":[]}},{"Parachain":1000},{"PalletInstance":50},{"GeneralIndex":1984}]"#
        );
    }

    /// DOT, which matches from both frames ALREADY — exactly the coincidence
    /// that would have made a naive join look like it worked. It must still
    /// absolutize correctly, and to the RELAY's own path.
    #[test]
    fn the_relay_token_absolutizes_to_the_relay_from_every_frame() {
        let parent = serde_json::json!({"parents": 1, "interior": {"Here": []}});
        let expected = r#"[{"GlobalConsensus":{"Polkadot":[]}}]"#;
        for path in [
            chain_path("polkadot", Some(2034)),
            chain_path("polkadot", Some(1000)),
        ] {
            assert_eq!(absolute_key(&path, &parent).unwrap(), expected);
        }
        // and the relay observing itself: `{parents: 0, Here}` from a chain with
        // no parachain junction is that chain, which for the relay is the
        // consensus root
        let here = serde_json::json!({"parents": 0, "interior": {"Here": []}});
        assert_eq!(
            absolute_key(&chain_path("polkadot", None), &here).unwrap(),
            expected
        );
        // a chain's own native token absolutizes to the chain itself — which is
        // how HDX gets an absolute name despite having NO AssetLocations entry
        assert_eq!(
            absolute_key(&chain_path("polkadot", Some(2034)), &here).unwrap(),
            r#"[{"GlobalConsensus":{"Polkadot":[]}},{"Parachain":2034}]"#
        );
    }

    #[test]
    fn leaving_the_consensus_absolutizes_and_going_above_it_refuses() {
        let hydration = chain_path("polkadot", Some(2034));
        // parents 2 from a parachain empties the path entirely and the interior
        // names where it went — no special case needed
        let ethereum = serde_json::json!({
            "parents": 2,
            "interior": {"X1": [[{"GlobalConsensus": {"Ethereum": {"chain_id": 1}}}]]}
        });
        assert_eq!(
            absolute_key(&hydration, &ethereum).unwrap(),
            r#"[{"GlobalConsensus":{"Ethereum":{"chain_id":1}}}]"#
        );
        // parents 3 from a parachain reaches ABOVE the consensus root, which is
        // not a place. Refused, never saturated to "the root".
        let nonsense = serde_json::json!({"parents": 3, "interior": {"Here": []}});
        assert!(absolutize(&hydration, &nonsense).is_none());
        // and something that is not location-shaped is None, not a panic
        assert!(absolutize(&hydration, &serde_json::json!("Here")).is_none());

        // AN ALREADY-ABSOLUTE LOCATION arriving while the observer's own path is
        // only PARTLY consumed is refused too. Re-rooting it would give
        // `[GC(Polkadot), Parachain(2034), GC(Kusama), Parachain(1000)]` — a key
        // that is nonsense AND self-consistent, so it would join against itself
        // and never look broken. (Slice 4 recorded that this shape is unobserved
        // on live data; it is guarded anyway.)
        let already_absolute = serde_json::json!({
            "parents": 0,
            "interior": {"X2": [[
                {"GlobalConsensus": {"Kusama": []}}, {"Parachain": [1000]}
            ]]}
        });
        assert!(
            absolutize(&hydration, &already_absolute).is_none(),
            "a GlobalConsensus junction with path left over is not re-rooted"
        );
        // …while the SAME junction with the path fully consumed is the ordinary
        // way to leave the consensus, and must still work (the Ethereum case
        // above). The distinction is the parent count, not the junction.
        assert!(absolutize(&hydration, &ethereum).is_some());
    }

    /// An absolute location is a bare ARRAY and a relative one is an OBJECT, so
    /// the two can never be confused for each other in a column or a join.
    #[test]
    fn absolute_and_relative_locations_are_structurally_distinguishable() {
        let path = chain_path("polkadot", Some(2034));
        let loc = serde_json::json!({"parents": 1, "interior": {"Here": []}});
        assert!(absolutize(&path, &loc).unwrap().is_array());
        assert!(crate::assets::normalize_location(&loc).unwrap().is_object());
    }

    // ------------------------------------------------------ storage keys

    /// THE ORDER TRAP, pinned. orml-tokens keys `(account, currency)` where
    /// pallet-assets keys `(asset, account)`; building it the wrong way round
    /// yields a well-formed key that matches nothing, which reads exactly like
    /// an empty account.
    #[test]
    fn the_accounts_key_puts_the_account_first_and_can_prove_it() {
        let account = pallet_account(b"py/trsry");
        let currency = 5u32.to_le_bytes().to_vec();
        let hashers = vec![
            StorageKeyHasher::Blake2_128Concat,
            StorageKeyHasher::Twox64Concat,
        ];
        let key = accounts_key("Tokens", &hashers, &account, &currency).unwrap();
        assert_eq!(key.len(), 32 + 16 + 32 + 8 + 4);
        assert_eq!(&key[..32], &crate::assets::map_prefix("Tokens", "Accounts")[..]);
        // the ACCOUNT sits in the first segment — which is the whole claim
        assert_eq!(&key[48..80], &account[..]);
        assert_eq!(&key[key.len() - 4..], &currency[..]);

        // and the round trip is the check a caller can run against a real key
        let (back_account, back_currency) = accounts_key_parts(&key, &hashers).unwrap();
        assert_eq!(back_account, account);
        assert_eq!(back_currency, currency);

        // Building the halves the pallet-assets way round must produce a
        // DIFFERENT key — stated as an assertion so nobody "simplifies" the two
        // helpers into one. NOTE the wrong key is built with the SAME map prefix
        // (`Tokens.Accounts`): a reviewer caught that comparing against
        // `assets::asset_account_key` proved nothing, because that function
        // hashes entry name `"Account"` (singular), so its first 32 bytes differ
        // regardless of argument order and the assertion would have passed even
        // if `accounts_key` swapped its halves.
        let mut wrong = crate::assets::map_prefix("Tokens", "Accounts");
        wrong.extend_from_slice(&hashers[0].hash(&currency));
        wrong.extend_from_slice(&hashers[1].hash(&account[..]));
        assert_ne!(
            wrong, key,
            "the halves are ordered (account, currency); reversing them yields a \
             well-formed key that matches nothing"
        );
        assert_eq!(&wrong[..32], &key[..32], "same map, different key halves");

        // a non-concat hasher cannot give the key back, and says so
        let opaque = vec![StorageKeyHasher::Blake2_128, StorageKeyHasher::Twox64Concat];
        assert!(accounts_key_parts(&key, &opaque).is_err());
        assert!(accounts_key("Tokens", &hashers[..1], &account, &currency).is_err());
    }

    /// An orml holding has a free/reserved split, so it converts to the NATIVE
    /// anchor shape. Pushing it through the pallet-assets anchor path (which
    /// writes `reserved = 0`) would silently drop the reserved half.
    #[test]
    fn an_orml_holding_keeps_its_reserved_half() {
        let h = OrmlHolding {
            free: 5_000,
            reserved: 700,
            frozen: Some(100),
        };
        assert_eq!(h.total(), 5_700);
        let ab = h.as_account_balances();
        assert_eq!((ab.free, ab.reserved, ab.frozen), (5_000, 700, Some(100)));
        assert_eq!(ab.total(), h.total());
    }

    #[test]
    fn network_variants_capitalise_and_the_path_shape_is_relay_then_para() {
        assert_eq!(chain_path("polkadot", None).len(), 1);
        assert_eq!(chain_path("kusama", Some(2000))[0]["GlobalConsensus"]["Kusama"],
                   serde_json::json!([]));
        assert_eq!(chain_path("polkadot", Some(2034))[1]["Parachain"], 2034);
    }
}
