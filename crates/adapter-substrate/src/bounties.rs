//! Bounty semantics of the Substrate family (Invariant 4). Three pallets, one
//! vocabulary — because they are three generations of one idea, and a treasury
//! page that shows them as three unrelated things is showing an accident of
//! history rather than where the money is.
//!
//! WHY THIS SLICE EXISTS, in one sentence from migration 0009: bounty funding
//! leaves the treasury through the `SpendFunds` HOOK, which emits
//! `bounties.BountyBecameActive` and NOT `treasury.Awarded` and NOT a pot
//! event — so until now every DOT that became a bounty was invisible to every
//! table dotlens had.
//!
//! ------------------------------------------------------------- the vocabulary
//!
//! Diffed across every published version at authoring time:
//!
//! `bounties` — pallet-bounties, 52 versions, 12 variants at 48.0.0. Named
//! fields since 4.0.0 (3.0.0 was tuple variants, below our v14 metadata floor
//! and therefore unreachable). GROWTH ONLY: `BountyApproved`, `CuratorProposed`,
//! `CuratorUnassigned`, `CuratorAccepted` at 24.0.0; `DepositPoked` at 40.0.0.
//! Nothing removed or renamed after 4.0.0.
//!     BountyProposed{index}                      → proposed
//!     BountyApproved{index}                      → approved
//!     BountyBecameActive{index}                  → funded      ← THE HOOK EVENT
//!     CuratorProposed{bounty_id, curator}        → curator_proposed
//!     CuratorAccepted{bounty_id, curator}        → active
//!     CuratorUnassigned{bounty_id}               → funded (back to unassigned)
//!     BountyAwarded{index, beneficiary}          → awarded
//!     BountyClaimed{index, payout, beneficiary}  → claimed   (payout = flow,
//!                                                  and NET of the curator fee)
//!     BountyCanceled{index}                      → canceled
//!     BountyRejected{index, bond}                → rejected  (bond = the
//!                                                  PROPOSER'S, never spending)
//!     BountyExtended{index}                      → info only
//!     DepositPoked{bounty_id, proposer, …}       → info only
//!
//! THE FIELD-NAME TRAP, and it is exactly slice 6's shape: the curator events
//! call the id `bounty_id` while every other variant calls it `index`. What
//! saves them is the explicit `.or_else(|| field_u64(data, "bounty_id", 0))`
//! NAME lookup below — NOT a positional fallback. `crate::balances::field` is
//! either/or, never one then the other: an OBJECT is read by name and an ARRAY
//! by index. So a mapper that looked up only `index` would silently drop every
//! curator change no matter what position it named.
//!
//! `child_bounties` — pallet-child-bounties, 4 variants, UNCHANGED 4.0.0 →
//! 48.0.0: Added / Awarded / Claimed / Canceled, all carrying `(index,
//! child_index)` with BOTH mandatory. Native-token only, like its parent.
//! `Added` maps to `funded`, NOT `active`: `ChildBountyStatus::Added` means
//! "added and waiting for curator assignment", and this pallet emits NO event
//! for the assignment — those four variants are the whole vocabulary — so a row
//! mapped `active` here would claim a curator from creation to award regardless
//! of reality. `funded` is the parent pallet's word for exactly that state,
//! which is the point of having one vocabulary.
//!
//! ITS ACCOUNTS ARE AN OPEN QUESTION, and the derivation REFUSES rather than
//! guesses (see `dotlens_node::bounties_pg::bounty_account`): pallet-child-
//! bounties ≤37.0.0 derived a child's account from `("cb", child_id)` with a
//! GLOBAL child id, and 38.0.0 changed it to `("cb", parent_id, child_id)` with
//! a PER-PARENT id — shipping `migration::v1::MigrateToV1Impl`, which RENUMBERS
//! the existing child bounties and TRANSFERS each old account's balance to its
//! new address. Polkadot has had child bounties since 2022, so both eras sit
//! inside the history this slice indexes: today's 3-part rule applied to a
//! pre-migration id yields an address that never existed, and one logical child
//! bounty can appear under two ids across the renumbering.
//!
//! `multi_asset_bounties` — pallet-multi-asset-bounties, 13 variants,
//! vocabulary verified across 0.2.0..=0.7.0 (its highest published version),
//! where the event enum is byte-identical. Different in kind, in two ways that
//! shape everything below:
//!   1. ONE ID SPACE. Parent and child live in one pallet, and almost every
//!      variant carries `(index, child_index: Option<_>)` where None IS the
//!      parent. The legacy pair is back-fitted into that shape rather than
//!      given a fourth model of its own.
//!   2. ASSET-DENOMINATED. `BountyPayoutProcessed` carries an `asset_kind`,
//!      so a bounty payout is denominated exactly like a treasury spend — and
//!      normalizes through the SAME `crate::assets` renderer, joining the same
//!      `core.assets` rows. 83,760 USDT reads as 83,760 USDT in both places.
//!   It also carries `Paid`/`PaymentFailed` with a `payment_id`, i.e. the
//!   retryable payout shape, which is why the projection gives payments their
//!   own coordinate (0009's lesson: a status coordinate cannot stand in) — and
//!   why `Paid` moves NO status: it fires for funding, refund and payout alike,
//!   and `retry_payment` fires it alone. See its arm below.
//!
//! POSITIONS DIFFER BETWEEN THE GENERATIONS, and that is what the indices below
//! are for. In the legacy pallets position 1 is a beneficiary or a payout; in
//! the modern one position 1 is `child_index`, so its beneficiary is at 2 and
//! its `value` at 3 — hence per-variant indices, never a shared one. Those
//! indices only ever apply to ARRAY-shaped data, i.e. the pre-4.0.0 tuple
//! variants, which are below our v14 metadata floor and therefore unreachable:
//! on every runtime we can decode, the NAME reads the field. They are kept
//! because a decoder change could make them live again — but a dormant index
//! that would be WRONG is still a defect, so a variant with no `child_index`
//! gets no index at all (position 1 on `BountyValueIncreased` is `old_value`).
//!
//! `BountyValueIncreased{index, old_value, new_value}` reports a bounty's SIZE,
//! not money moving: `figure_kind` is 'snapshot', the same distinction 0009
//! drew between `Deposit`/`Burnt` and `Spending`/`Rollover`. Summing it as a
//! flow would inflate bounty spending by the whole value of every raise.
//!
//! LOUD HALT, and here it is genuinely reachable. pallet-multi-asset-bounties
//! is pre-1.0 (13 versions; `BountyValueIncreased` appeared between 0.2.0 and
//! 0.7.0, its highest published version), so unlike pallet-assets its
//! vocabulary is still moving and a runtime upgrade CAN introduce a variant
//! this mapper has never seen. We halt anyway, by deliberate choice: a halt is
//! loud, recoverable and refuses rather than drops, and it is what caught five
//! unmapped pallet-balances events during slice 6's verification — three of
//! which moved money.
//!
//! -------------------------------------- what these numbers do NOT say
//!
//! `BountyClaimed.payout`, and child `Claimed.payout`, is the BENEFICIARY'S
//! SHARE, NET OF THE CURATOR FEE. `claim_bounty` transfers the fee to the
//! curator and the payout to the beneficiary in the SAME call, and NO event
//! carries the fee — so summing `figure_kind = 'flow'` understates what a
//! bounty actually spent, by exactly the fees. Every reader of that sum must
//! say so; `paid_out` in migration 0011 does.
//!
//! WHERE THE REAL NUMBER IS, and it is a stronger version of this slice's own
//! thesis: `pallet-bounties::spend_funds` funds a bounty with
//! `T::Currency::deposit_creating(&bounty_account_id(index), bounty.value)` — a
//! MINT into the DERIVED bounty account. So the funding, the curator fee, the
//! payout and every other movement ARE visible to dotlens already, as ordinary
//! `balances.*` rows on that account, at exactly the `BountyBecameActive`
//! height. The event stream says what was PROMISED; the derived ACCOUNT says
//! what MOVED and what is LEFT. That is why the accounts matter more than the
//! events, and why deriving them is this slice's other half.
//!
//! THE STATUSES BELOW ARE OUR WORDS FOR WHAT THE EVENTS SAY, not a copy of the
//! pallets' `BountyStatus`, and two real transitions emit no event at all:
//!   - `check_status` sends a child bounty whose curator IS its parent's
//!     curator straight to Active, emitting no `BountyBecameActive` — so a
//!     child can be worked on while we still read `funded`;
//!   - `spend_funds` sends an `ApprovedWithCurator` bounty straight to
//!     CuratorProposed, likewise silently — so we read `funded` where the
//!     chain reads CuratorProposed.
//! Both leave our status one step behind storage. A state read would show the
//! difference; we report the pallet's events, the same doctrine as slice 4's
//! `scheduler.Dispatched` finding.

use canonical::CanonicalEvent;
use ingest::bounties::BountyFact;

use crate::balances::{field, field_account, field_amount, json_scalar_string, json_u128};

/// Bumped on ANY rule change; rows rebuild from canonical events.
pub const BOUNTIES_MAPPER_VERSION: u32 = 1;

/// Pallet name (decoder-lowercased) → instance. Registering another
/// bounty-bearing chain needs zero code, exactly like the treasury instances.
pub fn instance_for_pallet(pallet: &str) -> Option<&'static str> {
    match pallet {
        "bounties" => Some("bounties"),
        "childbounties" => Some("child_bounties"),
        "multiassetbounties" => Some("multi_asset_bounties"),
        _ => None,
    }
}

/// The `-1` the projection stores for "the parent bounty itself". A sentinel
/// rather than a NULL because the projection's identity is (instance, bounty,
/// child) and a NULL cannot sit in a primary key; migration 0011 states it and
/// every read path renders it back to null.
pub const PARENT_SENTINEL: i64 = -1;

pub struct SubstrateBountyMapper;

impl ingest::bounties::BountyMapper for SubstrateBountyMapper {
    fn facts(&self, event: &CanonicalEvent) -> Result<Vec<BountyFact>, String> {
        facts_for_event(event)
    }
    fn mapper_version(&self) -> u32 {
        BOUNTIES_MAPPER_VERSION
    }
}

/// The pure mapping. Non-bounty events map to ∅; malformed or unknown bounty
/// events are LOUD errors.
pub fn facts_for_event(event: &CanonicalEvent) -> Result<Vec<BountyFact>, String> {
    let Some((pallet, variant)) = event.name.split_once('.') else {
        return Ok(vec![]);
    };
    let Some(instance) = instance_for_pallet(pallet) else {
        return Ok(vec![]);
    };
    let data = &event.data;
    let ctx = |what: &str| format!("{}: {what} (data: {data})", event.name);

    // id extraction. Position 0 in every variant of every generation; the name
    // is `index` except on the legacy curator events, which say `bounty_id`.
    let bounty_id = || -> Result<u64, String> {
        field_u64(data, "index", 0)
            .or_else(|| field_u64(data, "bounty_id", 0))
            .ok_or_else(|| ctx("no index/bounty_id"))
    };

    let base = |kind: &str, status: Option<&str>| BountyFact {
        instance: instance.to_string(),
        bounty_id: 0,
        child_id: None,
        kind: kind.to_string(),
        status: status.map(str::to_string),
        amount: None,
        figure_kind: None,
        bond: None,
        curator: None,
        beneficiary: None,
        beneficiary_location: None,
        asset_kind: None,
        asset_location: None,
        asset_key: None,
        payment_id: None,
        data: data.clone(),
    };

    let fact = match (instance, variant) {
        // ------------------------------------------------ legacy `bounties`
        ("bounties", "BountyProposed") => BountyFact {
            bounty_id: bounty_id()?,
            ..base("proposed", Some("proposed"))
        },
        ("bounties", "BountyApproved") => BountyFact {
            bounty_id: bounty_id()?,
            ..base("approved", Some("approved"))
        },
        // THE HOOK EVENT: the treasury paid this bounty's account, and no
        // treasury event says so. This row IS the outflow.
        ("bounties", "BountyBecameActive") => BountyFact {
            bounty_id: bounty_id()?,
            ..base("became_active", Some("funded"))
        },
        ("bounties", "BountyRejected") => BountyFact {
            bounty_id: bounty_id()?,
            // the PROPOSER'S bond, slashed. Its own column so it can never be
            // summed as bounty spending (0009's rule for treasury `slashed`).
            bond: field_amount(data, "bond", 1),
            ..base("rejected", Some("rejected"))
        },
        ("bounties", "BountyAwarded") => BountyFact {
            bounty_id: bounty_id()?,
            beneficiary: field_account(data, "beneficiary", 1).map(|a| a.to_vec()),
            ..base("awarded", Some("awarded"))
        },
        ("bounties", "BountyClaimed") => BountyFact {
            bounty_id: bounty_id()?,
            amount: Some(field_amount(data, "payout", 1).ok_or_else(|| ctx("no payout"))?),
            figure_kind: Some("flow".into()),
            beneficiary: field_account(data, "beneficiary", 2).map(|a| a.to_vec()),
            ..base("claimed", Some("claimed"))
        },
        ("bounties", "BountyCanceled") => BountyFact {
            bounty_id: bounty_id()?,
            ..base("canceled", Some("canceled"))
        },
        // an extension moves the EXPIRY, not the state or the money
        ("bounties", "BountyExtended") => BountyFact {
            bounty_id: bounty_id()?,
            ..base("extended", None)
        },
        ("bounties", "CuratorProposed") => BountyFact {
            bounty_id: bounty_id()?,
            curator: field_account(data, "curator", 1).map(|a| a.to_vec()),
            ..base("curator_proposed", Some("curator_proposed"))
        },
        ("bounties", "CuratorAccepted") => BountyFact {
            bounty_id: bounty_id()?,
            curator: field_account(data, "curator", 1).map(|a| a.to_vec()),
            ..base("curator_accepted", Some("active"))
        },
        // unassignment returns a funded bounty to "waiting for a curator" —
        // it does NOT unfund it, and reporting it as anything else would make
        // the bounty look like it gave its money back
        ("bounties", "CuratorUnassigned") => BountyFact {
            bounty_id: bounty_id()?,
            ..base("curator_unassigned", Some("funded"))
        },
        // a deposit adjustment on the PROPOSER'S bond; touches no bounty funds
        ("bounties", "DepositPoked") => BountyFact {
            bounty_id: bounty_id()?,
            ..base("deposit_poked", None)
        },

        // ------------------------------------------- legacy `child_bounties`
        // both ids mandatory here, and `child_index` is at position 1 — where
        // the modern pallet ALSO keeps it, which is the one thing the two
        // generations agree on.
        //
        // THE VARIANT IS MATCHED FIRST, deliberately: extracting the child id
        // before matching made an unknown future event halt with "no
        // child_index" instead of the unknown-variant message, and made the
        // unknown-variant test pass for the wrong reason.
        ("child_bounties", v) => {
            let f = match v {
                // NOT "active": `ChildBountyStatus::Added` is "added, waiting
                // for curator assignment", and this pallet emits no event for
                // the assignment — see the module header.
                "Added" => base("child_created", Some("funded")),
                "Awarded" => BountyFact {
                    beneficiary: field_account(data, "beneficiary", 2).map(|a| a.to_vec()),
                    ..base("awarded", Some("awarded"))
                },
                "Claimed" => BountyFact {
                    amount: Some(field_amount(data, "payout", 2).ok_or_else(|| ctx("no payout"))?),
                    figure_kind: Some("flow".into()),
                    beneficiary: field_account(data, "beneficiary", 3).map(|a| a.to_vec()),
                    ..base("claimed", Some("claimed"))
                },
                "Canceled" => base("canceled", Some("canceled")),
                other => {
                    return Err(format!(
                        "unknown childbounties event {other} — bounties mapper update \
                         required (vocabulary verified 4.0.0..=48.0.0, where it never changed)"
                    ))
                }
            };
            // every variant of this pallet carries it, so it is extracted once
            // here — but only AFTER the variant was recognised
            let child = field_u64(data, "child_index", 1).ok_or_else(|| ctx("no child_index"))?;
            BountyFact {
                bounty_id: bounty_id()?,
                child_id: Some(child),
                ..f
            }
        }

        // --------------------------------------- modern `multi_asset_bounties`
        // `child_index` is an OPTION at position 1: None IS the parent. Every
        // other field therefore sits one position later than a reader coming
        // from the legacy pallets would expect.
        ("multi_asset_bounties", v) => {
            // …in every variant that HAS one. `BountyCreated` (a parent, by
            // definition) and `BountyValueIncreased` do not, and they get NO
            // positional index rather than a wrong one: position 1 on the
            // latter is `old_value`, which would read as a child id the day
            // array-shaped data becomes reachable.
            let child_at = match v {
                "BountyCreated" | "BountyValueIncreased" => None,
                _ => Some(1),
            };
            let child = field_optional_u64(data, "child_index", child_at);
            let f = match v {
                "BountyCreated" => base("created", Some("proposed")),
                "ChildBountyCreated" => base("child_created", Some("proposed")),
                "BountyBecameActive" => BountyFact {
                    curator: field_account(data, "curator", 2).map(|a| a.to_vec()),
                    ..base("became_active", Some("active"))
                },
                "BountyAwarded" => BountyFact {
                    beneficiary: beneficiary_account(data, 2),
                    beneficiary_location: field(data, "beneficiary", 2).cloned(),
                    ..base("awarded", Some("awarded"))
                },
                // the payout actually concluded, in whatever asset the bounty
                // is denominated in
                "BountyPayoutProcessed" => {
                    let asset_kind = field(data, "asset_kind", 2).cloned();
                    let asset_location = asset_kind
                        .as_ref()
                        .and_then(crate::assets::locatable_asset_parts);
                    BountyFact {
                        amount: Some(
                            field_amount(data, "value", 3).ok_or_else(|| ctx("no value"))?,
                        ),
                        figure_kind: Some("flow".into()),
                        beneficiary: beneficiary_account(data, 4),
                        beneficiary_location: field(data, "beneficiary", 4).cloned(),
                        asset_key: asset_location
                            .as_ref()
                            .and_then(|p| p.get("asset"))
                            .and_then(crate::assets::metadata_free_asset_key),
                        asset_location,
                        asset_kind,
                        ..base("payout_processed", Some("claimed"))
                    }
                }
                // funding concluded — the modern generation's equivalent of
                // the legacy hook event, and equally invisible to the treasury
                "BountyFundingProcessed" => base("funding_processed", Some("funded")),
                "BountyRefundProcessed" => base("refund_processed", Some("canceled")),
                "BountyCanceled" => base("canceled", Some("canceled")),
                "CuratorProposed" => BountyFact {
                    curator: field_account(data, "curator", 2).map(|a| a.to_vec()),
                    ..base("curator_proposed", Some("curator_proposed"))
                },
                "CuratorUnassigned" => base("curator_unassigned", Some("funded")),
                // INFORMATION ONLY, and this is not a nicety. `Paid` is emitted
                // by all THREE payment flows — `do_process_funding_payment`,
                // `do_process_refund_payment`, `do_process_payout_payment` —
                // and `retry_payment` emits a LONE `Paid` with no accompanying
                // status event. Mapping it to a status would flip a bounty
                // whose FUNDING payment is merely being retried to "paid",
                // i.e. announce money that has not even reached it. The
                // surrounding status events already say which state the bounty
                // is in; this row's job is the payment id and its coordinate.
                "Paid" => BountyFact {
                    payment_id: field(data, "payment_id", 2).map(json_scalar_string),
                    ..base("paid", None)
                },
                // a failure IS an outcome, and names itself unambiguously
                "PaymentFailed" => BountyFact {
                    payment_id: field(data, "payment_id", 2).map(json_scalar_string),
                    ..base("payment_failed", Some("payment_failed"))
                },
                // the bounty's SIZE changed. A snapshot, not a flow — summing
                // these as spending would count the whole new value as paid.
                "BountyValueIncreased" => BountyFact {
                    amount: field_amount(data, "new_value", 2),
                    figure_kind: Some("snapshot".into()),
                    ..base("value_increased", None)
                },
                other => {
                    return Err(format!(
                        "unknown multiassetbounties event {other} — bounties mapper update \
                         required. NOTE this pallet is pre-1.0 and its vocabulary is still \
                         moving, so this halt is expected to fire on a runtime upgrade one \
                         day; that is the design, not a surprise"
                    ))
                }
            };
            BountyFact {
                bounty_id: bounty_id()?,
                child_id: child,
                ..f
            }
        }

        (_, other) => {
            return Err(format!(
                "unknown {pallet} event {other} — bounties mapper update required"
            ))
        }
    };
    Ok(vec![fact])
}

// ------------------------------------------------------------- field plumbing

fn field_u64(data: &serde_json::Value, name: &str, index: usize) -> Option<u64> {
    field(data, name, index)
        .and_then(json_u128)
        .and_then(|v| u64::try_from(v).ok())
}

/// An `Option<BountyIndex>` field. Our decoder renders `Option` as an enum
/// variant, never as JSON null: `{"None":[]}` and `{"Some":[n]}` (unnamed
/// fields → array). A reader expecting null would see an object and record
/// every parent bounty as a child of itself.
///
/// `index` is itself optional, because some variants carry no `child_index` at
/// all and there is no position to fall back to (see the caller).
fn field_optional_u64(data: &serde_json::Value, name: &str, index: Option<usize>) -> Option<u64> {
    let v = match data {
        serde_json::Value::Object(map) => map.get(name)?,
        serde_json::Value::Array(items) => items.get(index?)?,
        _ => return None,
    };
    if let Some(map) = v.as_object() {
        if map.len() == 1 {
            let (k, inner) = map.iter().next().expect("len 1");
            if k == "None" {
                return None;
            }
            if k == "Some" {
                let unwrapped = match inner {
                    serde_json::Value::Array(items) if items.len() == 1 => &items[0],
                    other => other,
                };
                return json_u128(unwrapped).and_then(|n| u64::try_from(n).ok());
            }
        }
        return None;
    }
    // a decoder that ever renders it as a bare number or null still works
    json_u128(v).and_then(|n| u64::try_from(n).ok())
}

/// The modern pallet's beneficiary is a LOCATION (`T::Beneficiary`), like the
/// treasury's. Extract a 32-byte account only where the location actually
/// names one; never invent an account for a location that names none.
fn beneficiary_account(data: &serde_json::Value, index: usize) -> Option<Vec<u8>> {
    let v = field(data, "beneficiary", index)?;
    // THE LOCATION WALK GOES FIRST. `json_account_bytes` collects every numeric
    // leaf in document order, so a Location whose fields happen to total 32
    // bytes would be accepted as an account — a plausible address for something
    // that names none. `account_in_location` only answers when an AccountId32
    // junction really is there, so it is asked first and the bare 32-byte walk
    // is the fallback for runtimes that configure `T::Beneficiary = AccountId`.
    crate::treasury::account_in_location(v)
        .or_else(|| crate::balances::json_account_bytes(v).map(|a| a.to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounts::{para_sovereign, sub_account, SubKey};

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
    fn one(e: &CanonicalEvent) -> BountyFact {
        let mut f = facts_for_event(e).expect("maps");
        assert_eq!(f.len(), 1, "one fact per bounty event");
        f.remove(0)
    }

    #[test]
    fn the_hook_event_is_the_outflow_no_treasury_table_can_see() {
        let f = one(&ev(
            "bounties.BountyBecameActive",
            serde_json::json!({"index": 17}),
        ));
        assert_eq!(
            (f.instance.as_str(), f.bounty_id, f.child_id),
            ("bounties", 17, None)
        );
        assert_eq!(f.status.as_deref(), Some("funded"));
        // it carries NO amount: the pallet event does not say how much was
        // funded, which is exactly why the bounty ACCOUNT matters
        assert!(f.amount.is_none());
    }

    #[test]
    fn curator_events_use_a_different_id_field_name() {
        let c = para_sovereign(1000);
        // `bounty_id`, not `index` — and it is the second NAME lookup that
        // finds it, not a position: `field` reads an object by name only
        let f = one(&ev(
            "bounties.CuratorAccepted",
            serde_json::json!({"bounty_id": 9, "curator": acct(&c)}),
        ));
        assert_eq!(f.bounty_id, 9);
        assert_eq!(f.curator.as_deref(), Some(&c[..]));
        assert_eq!(f.status.as_deref(), Some("active"));
        // unassignment goes BACK to funded, never to unfunded
        let u = one(&ev(
            "bounties.CuratorUnassigned",
            serde_json::json!({"bounty_id": 9}),
        ));
        assert_eq!(u.status.as_deref(), Some("funded"));
    }

    #[test]
    fn a_rejected_bond_is_never_bounty_spending() {
        let f = one(&ev(
            "bounties.BountyRejected",
            serde_json::json!({"index": 4, "bond": 20000000000u64}),
        ));
        assert_eq!(f.bond, Some(20_000_000_000));
        assert!(f.amount.is_none(), "a slashed bond is not an amount paid");
        assert!(f.figure_kind.is_none());
    }

    #[test]
    fn child_bounties_carry_both_ids_and_their_payout_is_a_flow() {
        let b = para_sovereign(2034);
        let f = one(&ev(
            "childbounties.Claimed",
            serde_json::json!({
                "index": 17, "child_index": 3, "payout": 500000000000u64,
                "beneficiary": acct(&b)
            }),
        ));
        assert_eq!((f.bounty_id, f.child_id), (17, Some(3)));
        assert_eq!(f.amount, Some(500_000_000_000));
        assert_eq!(f.figure_kind.as_deref(), Some("flow"));
        assert_eq!(f.beneficiary.as_deref(), Some(&b[..]));
        // …and `Added` is FUNDED, not active: this pallet emits no event for
        // curator assignment, so `active` would be a claim nothing supports
        let added = one(&ev(
            "childbounties.Added",
            serde_json::json!({"index": 17, "child_index": 3}),
        ));
        assert_eq!(added.status.as_deref(), Some("funded"));
    }

    /// `Paid` fires for FUNDING, REFUND and PAYOUT alike, and `retry_payment`
    /// fires it alone — so it must move no status, or a bounty whose funding is
    /// being retried would read as paid.
    #[test]
    fn a_payment_is_not_an_outcome_but_a_failure_is() {
        let paid = one(&ev(
            "multiassetbounties.Paid",
            serde_json::json!({"index": 5, "child_index": {"None": []}, "payment_id": 249}),
        ));
        assert_eq!(paid.kind, "paid");
        assert!(paid.status.is_none(), "a payment attempt is not a state");
        assert_eq!(paid.payment_id.as_deref(), Some("249"));
        let failed = one(&ev(
            "multiassetbounties.PaymentFailed",
            serde_json::json!({"index": 5, "child_index": {"None": []}, "payment_id": 249}),
        ));
        assert_eq!(failed.status.as_deref(), Some("payment_failed"));
    }

    /// The modern pallet's Option child_index — and the shape our decoder
    /// really produces for it, which is NOT null.
    #[test]
    fn a_none_child_index_means_the_parent_itself() {
        let c = para_sovereign(1000);
        let parent = one(&ev(
            "multiassetbounties.BountyBecameActive",
            serde_json::json!({"index": 5, "child_index": {"None": []}, "curator": acct(&c)}),
        ));
        assert_eq!(parent.child_id, None, "None is the parent, not child 0");
        let child = one(&ev(
            "multiassetbounties.BountyBecameActive",
            serde_json::json!({"index": 5, "child_index": {"Some": [2]}, "curator": acct(&c)}),
        ));
        assert_eq!(child.child_id, Some(2));
        // and child 0 must be distinguishable from the parent
        let zero = one(&ev(
            "multiassetbounties.BountyCanceled",
            serde_json::json!({"index": 5, "child_index": {"Some": [0]}}),
        ));
        assert_eq!(zero.child_id, Some(0));
    }

    /// The slice's payoff: a bounty payout is denominated exactly like a
    /// treasury spend, and normalizes through the same renderer to the same
    /// key — so 83,760 USDT reads as USDT in both places.
    #[test]
    fn a_modern_payout_carries_an_asset_that_normalizes_like_a_spend() {
        let b = para_sovereign(2034);
        let f = one(&ev(
            "multiassetbounties.BountyPayoutProcessed",
            serde_json::json!({
                "index": 1,
                "child_index": {"None": []},
                // the shape a V4 VersionedLocatableAsset really decodes to:
                // newtype variants one array deeper than a human would write
                "asset_kind": {"V4": [{
                    "location": {"parents": 0, "interior": {"Here": []}},
                    "asset_id": [{"parents": 0, "interior": {"X2": [[
                        {"PalletInstance": [50]}, {"GeneralIndex": [1984]}
                    ]]}}]
                }]},
                "value": 83760000000u64,
                "beneficiary": acct(&b)
            }),
        ));
        assert_eq!(f.amount, Some(83_760_000_000));
        assert_eq!(f.figure_kind.as_deref(), Some("flow"));
        assert_eq!(f.beneficiary.as_deref(), Some(&b[..]));
        // the asset half normalizes to the SAME canonical string
        // `core.assets.location_key` holds for USDT — compared against the
        // CONSTRUCTED form, which is what makes this a check and not two
        // copies of one guess (slice 6's twice-repeated mistake)
        let asset = f
            .asset_location
            .as_ref()
            .expect("asset location")
            .get("asset")
            .cloned();
        assert_eq!(
            asset.map(|a| a.to_string()),
            crate::assets::canonical_location(&crate::assets::local_asset_location(50, 1984))
        );
        // …and it is NOT resolvable to a key without metadata, honestly
        assert_eq!(f.asset_key, None);
    }

    #[test]
    fn a_value_increase_is_a_snapshot_not_a_payment() {
        let f = one(&ev(
            "multiassetbounties.BountyValueIncreased",
            serde_json::json!({"index": 1, "old_value": 100u64, "new_value": 250u64}),
        ));
        assert_eq!(f.amount, Some(250));
        assert_eq!(f.figure_kind.as_deref(), Some("snapshot"));
        assert!(
            f.status.is_none(),
            "a raise does not move the bounty's state"
        );
    }

    #[test]
    fn unknown_and_malformed_bounty_events_halt_loudly() {
        for name in [
            "bounties.SomeFutureEvent",
            "childbounties.SomeFutureEvent",
            "multiassetbounties.SomeFutureEvent",
        ] {
            let e =
                facts_for_event(&ev(name, serde_json::json!({"index": 1}))).expect_err("must halt");
            // and halt saying WHAT it did not understand. The child-bounties
            // arm used to extract the child id before matching the variant, so
            // this line read "no child_index" — a true statement about the
            // wrong problem, and the reason this assertion is on the message.
            assert!(
                e.contains("SomeFutureEvent"),
                "{name} halted with the wrong message: {e}"
            );
        }
        // a claim with no payout is a money event we cannot quantify
        assert!(facts_for_event(&ev(
            "bounties.BountyClaimed",
            serde_json::json!({"index": 1})
        ))
        .is_err());
        // other pallets are not this mapper's money
        assert!(
            facts_for_event(&ev("treasury.Awarded", serde_json::json!({})))
                .unwrap()
                .is_empty()
        );
    }

    /// The account list this slice closes: every bounty's funds live at a
    /// DERIVED sub-account of the treasury's pallet id, so registering them
    /// needs no curation — just the id the event already carried.
    #[test]
    fn every_bounty_id_yields_a_derivable_account() {
        let f = one(&ev(
            "bounties.BountyBecameActive",
            serde_json::json!({"index": 17}),
        ));
        let acct = sub_account(
            b"py/trsry",
            &[SubKey::Str("bt"), SubKey::Index(f.bounty_id as u32)],
        )
        .expect("derives");
        assert_eq!(
            format!("0x{}", hex::encode(acct)),
            "0x6d6f646c70792f74727372790862741100000000000000000000000000000000"
        );
    }
}
