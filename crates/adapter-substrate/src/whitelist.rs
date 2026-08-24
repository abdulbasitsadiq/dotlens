//! pallet-whitelist event → whitelist facts (Invariant 4: the only place that
//! knows this pallet's vocabulary).
//!
//! UPSTREAM VERIFICATION, done the way slices 5–7 did it and unusually
//! conclusive: all 51 published `pallet-whitelist` versions (4.0.0 → 48.0.0)
//! were diffed. The `Event` enum has carried exactly THREE named-field
//! variants — `CallWhitelisted { call_hash }`, `WhitelistedCallRemoved {
//! call_hash }`, `WhitelistedCallDispatched { call_hash, result }` — with the
//! same names, the same field ORDER and the same implicit codec indices 0/1/2
//! in every one of them. No variant was ever added, removed, renamed or
//! reordered; no field ever moved; there are no `#[codec(index)]` attributes
//! anywhere. The single diff in the pallet's history is `PreimageHash` →
//! `T::Hash` at 23.0.0, and both are `H256`, so even the encoding never
//! changed. Coverage is therefore 100% of every runtime that has ever carried
//! this pallet, and the loud-halt arm below is unreachable on any of them.
//!
//! THE PLANNED HALT, stated so it is not a surprise: an as-yet unpublished
//! `master` adds a deferred-dispatch/relayer mechanism with three new variants
//! — `DispatchDeferred`, `DeferredDispatchRemoved`, `DeferredDispatchExecuted`
//! — APPENDED at indices 3/4/5, plus `remove_deferred_dispatch` at call index
//! 4. Existing indices do not shift, so nothing here silently misreads; the
//! mapper simply halts on the new names, which is the designed behaviour.
//! It also changes what the two existing dispatch calls MEAN without changing
//! their signatures (a dispatch of a non-whitelisted hash defers instead of
//! erroring), so the day a runtime carries it, re-read this header.
//!
//! THE ACCURACY RULE THIS PALLET FORCES, and it is the whole reason
//! `dispatch_ok` exists: `WhitelistedCallDispatched` is emitted for a FAILED
//! call exactly as for a successful one. From `clean_and_dispatch`:
//!
//! ```text
//!     let result = call.dispatch(frame_system::Origin::<T>::Root.into());
//!     let call_actual_weight = match result {
//!         Ok(call_post_info) => call_post_info.actual_weight,
//!         Err(call_err) => call_err.post_info.actual_weight,
//!     };
//!     Self::deposit_event(Event::<T>::WhitelistedCallDispatched { call_hash, result });
//!     call_actual_weight
//! ```
//!
//! The error is dropped on the floor and the extrinsic returns `Ok`, so
//! `system.ExtrinsicSuccess` fires either way, the whitelist entry is consumed
//! either way, and the fee is charged either way. The ONLY on-chain signal
//! distinguishing "the Fellowship's call took effect" from "it reverted" is
//! the `result` variant tag inside this event. Read it, and never infer
//! success from the event's presence.

use crate::gov::{field, json_h256_hex};
use canonical::CanonicalEvent;
use ingest::whitelist::{WhitelistFact, WhitelistMapper};

/// Lineage. Bump on any rule change and re-run `whitelist-range`; the
/// projection is a pure function of the fact table, so it rebuilds exactly.
pub const WHITELIST_MAPPER_VERSION: u32 = 1;

pub struct SubstrateWhitelistMapper;

impl WhitelistMapper for SubstrateWhitelistMapper {
    fn facts(&self, event: &CanonicalEvent) -> Result<Vec<WhitelistFact>, String> {
        facts_for_event(event)
    }
    fn mapper_version(&self) -> u32 {
        WHITELIST_MAPPER_VERSION
    }
}

/// The pure mapping. Non-whitelist events map to ∅; malformed whitelist events
/// are ERRORS.
pub fn facts_for_event(event: &CanonicalEvent) -> Result<Vec<WhitelistFact>, String> {
    let Some((pallet, variant)) = event.name.split_once('.') else {
        return Ok(vec![]);
    };
    // The decoder lowercases pallet names. A chain that instantiates the pallet
    // under another name would need a registry entry, not a code branch — but
    // no Polkadot-family runtime has ever done so, and inventing the mechanism
    // before there is a second case would be guessing.
    if pallet != "whitelist" {
        return Ok(vec![]);
    }
    let data = &event.data;
    let ctx = |what: &str| format!("{}: {what} (data: {data})", event.name);

    let kind = match variant {
        "CallWhitelisted" => "whitelisted",
        "WhitelistedCallRemoved" => "removed",
        "WhitelistedCallDispatched" => "dispatched",
        _ => {
            return Err(format!(
                "unknown whitelist event {} — whitelist mapper update required \
                 (see the deferred-dispatch note in this module's header)",
                event.name
            ))
        }
    };

    let call_hash = field(data, "call_hash", 0)
        .and_then(json_h256_hex)
        .ok_or_else(|| ctx("no call_hash"))?;

    // Only the dispatch variant carries a result — and when it does, an
    // unreadable result is a HALT, never a NULL and never an assumed success.
    // Defaulting here would report a reverted runtime upgrade as enacted.
    let (dispatch_ok, dispatch_error) = if variant == "WhitelistedCallDispatched" {
        let result = field(data, "result", 1).ok_or_else(|| ctx("no result"))?;
        let (ok, err) = dispatch_result(result).ok_or_else(|| {
            ctx("result is neither Ok nor Err — cannot tell whether the \
                                whitelisted call succeeded, and guessing is not an option")
        })?;
        (Some(ok), err)
    } else {
        (None, None)
    };

    Ok(vec![WhitelistFact {
        call_hash,
        kind: kind.to_string(),
        dispatch_ok,
        dispatch_error,
        data: data.clone(),
    }])
}

/// `DispatchResultWithPostInfo` = `Result<PostDispatchInfo,
/// DispatchErrorWithPostInfo>` → `(succeeded, error)`.
///
/// TWO SHAPES ARE ACCEPTED ON PURPOSE. `Ok`/`Err` are single-unnamed-field
/// (newtype) variants, and dotlens's decoder renders those one array deeper —
/// `{"Err":[{…}]}` — exactly as it renders `PalletInstance(50)` as
/// `{"PalletInstance":[50]}` and `Voting::Casting` as `{"Casting":[{…}]}`.
/// A normalized pipeline yields `{"Err":{…}}`. Slice 6 shipped a dead join
/// because a test hand-wrote the scalar form while production produced the
/// wrapped one; handling both is cheaper than being wrong, and the drill is
/// what confirms which shape reality actually emits.
fn dispatch_result(v: &serde_json::Value) -> Option<(bool, Option<serde_json::Value>)> {
    let map = v.as_object()?;
    if map.contains_key("Ok") {
        return Some((true, None));
    }
    let err = newtype_inner(map.get("Err")?);
    // `DispatchErrorWithPostInfo { post_info, error }` — POST_INFO IS FIRST.
    // The positional fallback is therefore 1, not 0. Reading index 0 would
    // store a weight where an error belongs, and it would look plausible.
    // An Err with no readable `error` is a result we could not read, NOT a
    // failure without an error: a unit DispatchError variant still renders as
    // `{"Other": []}`, never as nothing. So `?` here, and let the caller halt.
    Some((false, Some(field(err, "error", 1)?.clone())))
}

/// Peel one newtype layer: a 1-element array is the decoder's rendering of a
/// single-unnamed-field variant. Non-recursive on purpose — a genuine
/// 1-element Vec deeper in the structure must survive.
fn newtype_inner(v: &serde_json::Value) -> &serde_json::Value {
    match v {
        serde_json::Value::Array(items) if items.len() == 1 => &items[0],
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ev(name: &str, data: serde_json::Value) -> CanonicalEvent {
        CanonicalEvent {
            index: 0,
            transaction_index: None,
            name: name.into(),
            data,
        }
    }

    fn hash_json(byte: u8) -> serde_json::Value {
        // H256 renders as a newtype over the byte array, hence the outer array.
        json!([vec![byte; 32]])
    }

    fn hash_hex(byte: u8) -> String {
        format!("0x{}", hex::encode([byte; 32]))
    }

    #[test]
    fn non_whitelist_events_map_to_nothing() {
        for name in [
            "balances.Transfer",
            "referenda.Submitted",
            "system.ExtrinsicSuccess",
        ] {
            assert!(facts_for_event(&ev(name, json!({}))).unwrap().is_empty());
        }
    }

    #[test]
    fn call_whitelisted_and_removed_carry_the_hash_and_no_dispatch_verdict() {
        let f = facts_for_event(&ev(
            "whitelist.CallWhitelisted",
            json!({"call_hash": hash_json(0xab)}),
        ))
        .unwrap();
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].call_hash, hash_hex(0xab));
        assert_eq!(f[0].kind, "whitelisted");
        assert_eq!(f[0].dispatch_ok, None);
        assert_eq!(f[0].dispatch_error, None);

        let f = facts_for_event(&ev(
            "whitelist.WhitelistedCallRemoved",
            json!({"call_hash": hash_json(0xcd)}),
        ))
        .unwrap();
        assert_eq!(f[0].kind, "removed");
        assert_eq!(f[0].dispatch_ok, None);
    }

    #[test]
    fn a_successful_dispatch_is_ok_in_both_decoder_shapes() {
        for result in [
            json!({"Ok": {"actual_weight": {"Some": {"ref_time": 187000000u64, "proof_size": 3593}}, "pays_fee": "Yes"}}),
            json!({"Ok": [{"actual_weight": [{"Some": [{"ref_time": 187000000u64, "proof_size": 3593}]}], "pays_fee": "Yes"}]}),
        ] {
            let f = facts_for_event(&ev(
                "whitelist.WhitelistedCallDispatched",
                json!({"call_hash": hash_json(1), "result": result}),
            ))
            .unwrap();
            assert_eq!(f[0].dispatch_ok, Some(true));
            assert_eq!(f[0].dispatch_error, None);
        }
    }

    /// The slice's headline rule: the event fires for a FAILED call too, and
    /// the error must be read out of `error`, not out of `post_info` — they are
    /// adjacent fields and post_info comes FIRST.
    #[test]
    fn a_failed_dispatch_is_not_success_and_reads_error_not_post_info() {
        let post_info =
            json!({"actual_weight": {"Some": {"ref_time": 1, "proof_size": 2}}, "pays_fee": "Yes"});
        let error = json!({"Module": {"index": 31, "error": "0x02000000"}});

        // named form, and the array form where post_info sits at index 0
        for result in [
            json!({"Err": {"post_info": post_info, "error": error}}),
            json!({"Err": [{"post_info": post_info, "error": error}]}),
            json!({"Err": [[post_info, error]]}),
        ] {
            let f = facts_for_event(&ev(
                "whitelist.WhitelistedCallDispatched",
                json!({"call_hash": hash_json(2), "result": result}),
            ))
            .unwrap();
            assert_eq!(
                f[0].dispatch_ok,
                Some(false),
                "a dispatched call that reverted is NOT ok"
            );
            assert_eq!(
                f[0].kind, "dispatched",
                "it was still dispatched, and storage was still cleaned"
            );
            assert_eq!(
                f[0].dispatch_error.as_ref().unwrap(),
                &error,
                "the DispatchError, never the PostDispatchInfo that precedes it"
            );
        }
    }

    #[test]
    fn an_unreadable_dispatch_result_halts_rather_than_assuming_success() {
        for result in [json!("Dispatched"), json!(null), json!({"Weird": 1})] {
            let err = facts_for_event(&ev(
                "whitelist.WhitelistedCallDispatched",
                json!({"call_hash": hash_json(3), "result": result}),
            ))
            .unwrap_err();
            assert!(err.contains("guessing is not an option"), "got: {err}");
        }
        // a dispatch event with no result at all is equally a halt
        let err = facts_for_event(&ev(
            "whitelist.WhitelistedCallDispatched",
            json!({"call_hash": hash_json(3)}),
        ))
        .unwrap_err();
        assert!(err.contains("no result"), "got: {err}");
    }

    #[test]
    fn an_unknown_whitelist_event_halts_loudly() {
        // exactly the shape the unpublished deferred-dispatch work will take
        let err = facts_for_event(&ev(
            "whitelist.DispatchDeferred",
            json!({"call_hash": hash_json(4)}),
        ))
        .unwrap_err();
        assert!(err.contains("unknown whitelist event"), "got: {err}");
        assert!(
            err.contains("deferred-dispatch"),
            "the halt must point at the likely cause"
        );
    }

    #[test]
    fn a_whitelist_event_without_a_hash_halts() {
        let err = facts_for_event(&ev("whitelist.CallWhitelisted", json!({}))).unwrap_err();
        assert!(err.contains("no call_hash"), "got: {err}");
    }
}
