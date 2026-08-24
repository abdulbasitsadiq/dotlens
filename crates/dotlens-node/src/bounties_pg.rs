//! Postgres backend for the bounty worker: facts into `treasury.bounty_events`
//! (insert-ignore) plus the ordering-guarded `treasury.bounties` projection,
//! atomically per block — and `sync_bounty_accounts`, which turns every indexed
//! bounty into a `treasury.treasury_accounts` row so the holdings sweep can see
//! money that no treasury event ever mentioned.
//!
//! The canonical-event SOURCE is `balances_pg::PgEventSource`, shared with the
//! balances/gov/votes/treasury workers by design.
//!
//! NUMERIC values are bound as text and cast server-side (`$n::numeric`).
//!
//! ------------------------------------------------- the one thing that is new
//!
//! `treasury.bounties.paid_out` ACCUMULATES. Every other projection in this
//! project is a pure function of the facts it has seen, so replaying a range is
//! free; a running total is not. The sink therefore adds a payout ONLY when the
//! fact row was really inserted, which it learns from `insert … returning` on
//! the append-only table — the fact table's own idempotence is what makes the
//! non-idempotent column safe. Delete the fact rows and the total must be
//! rebuilt with them, which is the same rule `mapper_version` already implies —
//! and stated beside the column in migration 0011 too, because deleting the
//! PROJECTION alone and re-ranging leaves `paid_out` NULL forever, silently.

use anyhow::{Context, Result};
use async_trait::async_trait;
use ingest::bounties::{BountyFact, BountySink};
use sqlx::PgPool;
use std::collections::HashSet;

use adapter_substrate::accounts::{self as acct, SubKey};
use adapter_substrate::bounties::PARENT_SENTINEL;

/// The treasury PalletId whose sub-accounts hold this chain's bounty funds.
/// Passed in rather than looked up per write: it is a property of the chain's
/// runtime, it does not change between blocks, and a sink that re-decoded
/// ~580KB of metadata per event would be slice 6's hoisted defect all over
/// again. `None` is honest — a chain with no archived metadata yet writes
/// `account_id` NULL, and `sync_bounty_accounts` fills it in later.
pub struct PgBountySink {
    pool: PgPool,
    pallet_id: Option<[u8; 8]>,
}

impl PgBountySink {
    pub fn new(pool: PgPool, pallet_id: Option<[u8; 8]>) -> Self {
        Self { pool, pallet_id }
    }
}

fn opt_num(v: Option<u128>) -> Option<String> {
    v.map(|n| n.to_string())
}

fn child_column(child_id: Option<u64>) -> i64 {
    child_id.map_or(PARENT_SENTINEL, |c| c as i64)
}

/// The four derivation shapes migration 0011 states, as SubKey parts.
///
/// The two generations encode their prefix DIFFERENTLY — a `&str` carries a
/// SCALE compact length byte, a `[u8; 3]` does not — and getting that wrong
/// yields a plausible address that holds nothing. The distinction lives in
/// `SubKey`, is pinned by golden vectors in `adapter_substrate::accounts`, and
/// is checked against the chain by `verify-labels`.
///
/// Returns None where the shape cannot name an account: an id above u32 (the
/// pallets' `BountyIndex` is a u32, so this is a corrupt row rather than a big
/// bounty), a `bounties` row with a child — or ANY legacy `child_bounties` row,
/// for the reason below. Refusing beats deriving an address that belongs to
/// something else.
///
/// THE LEGACY CHILD-BOUNTY ADDRESS IS NOT A FUNCTION OF (parent, child) ALONE,
/// and that is why there is no `("child_bounties", Some(_))` arm here.
/// pallet-child-bounties ≤37.0.0 derived `("cb", child_id)` from a GLOBAL child
/// id; 38.0.0 changed it to `("cb", parent_id, child_id)` with a PER-PARENT id
/// and shipped `migration::v1::MigrateToV1Impl`, which RENUMBERS the existing
/// child bounties and TRANSFERS each old account's balance to its new address.
/// Polkadot has had child bounties since 2022, so a projection row can belong
/// to either era, and this table records no era: applying today's 3-part rule
/// to a pre-migration id yields an address that never existed, and the same
/// logical child bounty may sit here twice under two ids. So we refuse, count
/// it in `underivable`, and register no phantom account. Lifting the refusal
/// needs a per-chain migration height (or the block's spec_version threaded
/// through the sink) — data we do not have yet, not a rule we can guess.
fn bounty_account_parts(
    instance: &str,
    bounty_id: u64,
    child_id: Option<u64>,
) -> Option<(Vec<SubKey<'static>>, String)> {
    let parent = u32::try_from(bounty_id).ok()?;
    let child = match child_id {
        Some(c) => Some(u32::try_from(c).ok()?),
        None => None,
    };
    match (instance, child) {
        ("bounties", None) => Some((
            vec![SubKey::Str("bt"), SubKey::Index(parent)],
            format!("bt/{parent}"),
        )),
        ("multi_asset_bounties", None) => Some((
            vec![SubKey::Bytes(b"mbt"), SubKey::Index(parent)],
            format!("mbt/{parent}"),
        )),
        // the modern pallet has only ever had per-parent child ids, so it has
        // no renumbering behind it and its children ARE derivable
        ("multi_asset_bounties", Some(c)) => Some((
            vec![
                SubKey::Bytes(b"mcb"),
                SubKey::Index(parent),
                SubKey::Index(c),
            ],
            format!("mcb/{parent}/{c}"),
        )),
        _ => None,
    }
}

/// A bounty's own account, plus the derivation string that lets a reader
/// re-derive it: `modl:py/trsry/bt/17`.
pub fn bounty_account(
    pallet_id: &[u8; 8],
    instance: &str,
    bounty_id: u64,
    child_id: Option<u64>,
) -> Option<([u8; 32], String)> {
    let (parts, tail) = bounty_account_parts(instance, bounty_id, child_id)?;
    let account = acct::sub_account(pallet_id, &parts)?;
    Some((
        account,
        format!("modl:{}/{}", acct::ascii_pallet_id(pallet_id), tail),
    ))
}

/// How a bounty account is named on the treasury page. The instance is part of
/// the name for the modern pallet because the legacy and modern id spaces are
/// different number lines that both start at 0 — the same collision
/// `spend_kind` exists to prevent on spends.
fn bounty_label(instance: &str, bounty_id: u64, child_id: Option<u64>) -> String {
    match (instance, child_id) {
        ("multi_asset_bounties", None) => format!("Multi-asset bounty {bounty_id}"),
        ("multi_asset_bounties", Some(c)) => {
            format!("Multi-asset child bounty {bounty_id}-{c}")
        }
        (_, None) => format!("Bounty {bounty_id}"),
        (_, Some(c)) => format!("Child bounty {bounty_id}-{c}"),
    }
}

#[async_trait]
impl BountySink for PgBountySink {
    async fn write(
        &self,
        chain_id: &str,
        height: u64,
        runtime_version: u32,
        mapper_version: u32,
        rows: &[(u32, BountyFact)],
    ) -> Result<(), String> {
        // the fact table is keyed (chain, block, event): one fact per event by
        // contract, and insert-ignore would silently swallow a second one
        let mut seen: HashSet<u32> = HashSet::new();
        for (event_index, _) in rows {
            if !seen.insert(*event_index) {
                return Err(format!(
                    "two bounty facts for {chain_id}/{height} event {event_index} — \
                     the fact table is keyed by event; mapper contract violated"
                ));
            }
        }

        // deterministic global lock order for the projection upserts (by
        // projection key, NOT by height), so a concurrent bounties-range and
        // follower cannot deadlock on DO UPDATE row locks
        let mut ordered: Vec<&(u32, BountyFact)> = rows.iter().collect();
        ordered.sort_by(|(ai, a), (bi, b)| {
            (&a.instance, a.bounty_id, child_column(a.child_id), ai).cmp(&(
                &b.instance,
                b.bounty_id,
                child_column(b.child_id),
                bi,
            ))
        });

        let mut tx = self.pool.begin().await.map_err(|e| e.to_string())?;
        for (event_index, f) in ordered {
            let child = child_column(f.child_id);
            // `returning event_index` distinguishes an INSERT from a
            // conflict-ignored no-op: None means this fact was already here, so
            // its payout has already been counted into `paid_out` and must not
            // be counted again. This is the whole reason the accumulation is
            // safe under replay.
            let inserted: Option<(i32,)> = sqlx::query_as(
                "insert into treasury.bounty_events \
                     (chain_id, block_height, event_index, instance, bounty_id, child_id, \
                      kind, status, amount, figure_kind, bond, curator, beneficiary, \
                      beneficiary_location, asset_kind, asset_location, asset_key, \
                      payment_id, data, runtime_version, mapper_version) \
                 values ($1,$2,$3,$4,$5,$6,$7,$8,$9::numeric,$10,$11::numeric,$12,$13,$14,$15, \
                         $16,$17,$18,$19,$20,$21) \
                 on conflict (chain_id, block_height, event_index) do nothing \
                 returning event_index",
            )
            .bind(chain_id)
            .bind(height as i64)
            .bind(*event_index as i32)
            .bind(&f.instance)
            .bind(f.bounty_id as i64)
            .bind(child)
            .bind(&f.kind)
            .bind(&f.status)
            .bind(opt_num(f.amount))
            .bind(&f.figure_kind)
            .bind(opt_num(f.bond))
            .bind(&f.curator)
            .bind(&f.beneficiary)
            .bind(&f.beneficiary_location)
            .bind(&f.asset_kind)
            .bind(&f.asset_location)
            .bind(&f.asset_key)
            .bind(&f.payment_id)
            .bind(&f.data)
            .bind(runtime_version as i64)
            .bind(mapper_version as i32)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
            let first_sighting = inserted.is_some();

            // WHAT THE FIGURE MEANS decides which column it lands in — 0009's
            // rule. A payout is money that left the bounty; a raise is the
            // bounty's new SIZE. Adding the second to the first would report
            // every raise as a payment.
            let flow = f.figure_kind.as_deref() == Some("flow");
            let snapshot = f.figure_kind.as_deref() == Some("snapshot");
            let account = self
                .pallet_id
                .as_ref()
                .and_then(|pid| bounty_account(pid, &f.instance, f.bounty_id, f.child_id))
                .map(|(a, _)| a.to_vec());

            // UNLIKE THE TREASURY SINK, a status-less fact is NOT skipped. A
            // treasury fact without a status is a POT flow that names no spend
            // and has nowhere to go; a bounty fact without a status still names
            // a bounty and still carries its columns — `BountyValueIncreased`
            // is the only event in any of the three pallets that reports a
            // bounty's value at all. Dropping it would drop the value.
            //
            // Such a row lands at coordinate (0, 0) with the placeholder status
            // 'unknown', the same device gov.referenda uses: any real status
            // event sits at a height ≥ 1 and therefore always wins the ladder
            // below, whatever order the ranges are ingested in.
            let (status, status_height, status_event_index) = match f.status.as_deref() {
                Some(s) => (s, height as i64, *event_index as i32),
                None => ("unknown", 0i64, 0i32),
            };

            // The projection's polarity is treasury.spends': a CASE ladder on
            // the status coordinate (a WHERE guard would skip the whole row
            // update for an older event and lose the value columns forever),
            // first-non-null-wins on the value columns, and the payment triple
            // winning by its OWN coordinate because `Paid` and `PaymentFailed`
            // both carry an id while other events move the status carrying none.
            //
            // TWO RULES ARE THIS TABLE'S OWN:
            //   `value` takes greatest(). The only event that reports a value
            //   is `BountyValueIncreased`, and the pallets have no event that
            //   LOWERS one, so the largest snapshot is the latest snapshot —
            //   no fourth coordinate column needed. The day a decrease event
            //   exists this is wrong, and the mapper's loud halt on unknown
            //   variants is what stops it landing silently.
            //   `paid_out` ACCUMULATES, guarded by `first_sighting` above.
            //   `coalesce(…, 0) + excluded` rather than a bare sum so that a
            //   bounty that has never paid keeps a NULL (which means "no payout
            //   seen") instead of a 0 (which means "paid nothing").
            sqlx::query(
                "insert into treasury.bounties \
                     (chain_id, instance, bounty_id, child_id, status, value, paid_out, bond, \
                      curator, beneficiary, beneficiary_location, asset_kind, asset_location, \
                      asset_key, payment_id, payment_height, payment_event_index, account_id, \
                      first_seen_height, status_height, status_event_index, \
                      runtime_version, mapper_version) \
                 values ($1,$2,$3,$4,$5,$6::numeric,$7::numeric,$8::numeric,$9,$10,$11,$12,$13, \
                         $14,$15,$16,$17,$18,$19,$20,$21,$22,$23) \
                 on conflict (chain_id, instance, bounty_id, child_id) do update set \
                     status = case when (excluded.status_height, excluded.status_event_index) \
                                        > (treasury.bounties.status_height, \
                                           treasury.bounties.status_event_index) \
                                   then excluded.status else treasury.bounties.status end, \
                     status_height = greatest(excluded.status_height, \
                                              treasury.bounties.status_height), \
                     status_event_index = case when (excluded.status_height, \
                                                     excluded.status_event_index) \
                                                    > (treasury.bounties.status_height, \
                                                       treasury.bounties.status_event_index) \
                                               then excluded.status_event_index \
                                               else treasury.bounties.status_event_index end, \
                     runtime_version = case when (excluded.status_height, \
                                                  excluded.status_event_index) \
                                                 > (treasury.bounties.status_height, \
                                                    treasury.bounties.status_event_index) \
                                            then excluded.runtime_version \
                                            else treasury.bounties.runtime_version end, \
                     mapper_version = case when (excluded.status_height, \
                                                 excluded.status_event_index) \
                                                > (treasury.bounties.status_height, \
                                                   treasury.bounties.status_event_index) \
                                           then excluded.mapper_version \
                                           else treasury.bounties.mapper_version end, \
                     value = greatest(treasury.bounties.value, excluded.value), \
                     paid_out = case when excluded.paid_out is null \
                                     then treasury.bounties.paid_out \
                                     else coalesce(treasury.bounties.paid_out, 0) \
                                          + excluded.paid_out end, \
                     bond = coalesce(treasury.bounties.bond, excluded.bond), \
                     curator = coalesce(treasury.bounties.curator, excluded.curator), \
                     beneficiary = coalesce(treasury.bounties.beneficiary, \
                                            excluded.beneficiary), \
                     beneficiary_location = coalesce(treasury.bounties.beneficiary_location, \
                                                     excluded.beneficiary_location), \
                     asset_kind = coalesce(treasury.bounties.asset_kind, excluded.asset_kind), \
                     asset_location = coalesce(treasury.bounties.asset_location, \
                                               excluded.asset_location), \
                     asset_key = coalesce(treasury.bounties.asset_key, excluded.asset_key), \
                     payment_id = case when excluded.payment_id is not null \
                                        and (coalesce(treasury.bounties.payment_height, -1), \
                                             coalesce(treasury.bounties.payment_event_index, -1)) \
                                          < (excluded.payment_height, \
                                             excluded.payment_event_index) \
                                       then excluded.payment_id \
                                       else treasury.bounties.payment_id end, \
                     payment_height = case when excluded.payment_id is not null \
                                            and (coalesce(treasury.bounties.payment_height, -1), \
                                                 coalesce(treasury.bounties.payment_event_index, -1)) \
                                              < (excluded.payment_height, \
                                                 excluded.payment_event_index) \
                                           then excluded.payment_height \
                                           else treasury.bounties.payment_height end, \
                     payment_event_index = case when excluded.payment_id is not null \
                                                 and (coalesce(treasury.bounties.payment_height, -1), \
                                                      coalesce(treasury.bounties.payment_event_index, -1)) \
                                                   < (excluded.payment_height, \
                                                      excluded.payment_event_index) \
                                                then excluded.payment_event_index \
                                                else treasury.bounties.payment_event_index end, \
                     account_id = coalesce(treasury.bounties.account_id, excluded.account_id), \
                     first_seen_height = least(excluded.first_seen_height, \
                                               treasury.bounties.first_seen_height), \
                     updated_at = now()",
            )
            .bind(chain_id)
            .bind(&f.instance)
            .bind(f.bounty_id as i64)
            .bind(child)
            .bind(status)
            .bind(snapshot.then(|| opt_num(f.amount)).flatten())
            // the accumulating column, and the ONLY place `first_sighting`
            // is consulted: a replayed event contributes nothing
            .bind((flow && first_sighting).then(|| opt_num(f.amount)).flatten())
            .bind(opt_num(f.bond))
            .bind(&f.curator)
            .bind(&f.beneficiary)
            .bind(&f.beneficiary_location)
            .bind(&f.asset_kind)
            .bind(&f.asset_location)
            .bind(&f.asset_key)
            .bind(&f.payment_id)
            // the payment's OWN coordinate, NULL unless this event carried one
            .bind(f.payment_id.as_ref().map(|_| height as i64))
            .bind(f.payment_id.as_ref().map(|_| *event_index as i32))
            .bind(account)
            .bind(height as i64)
            .bind(status_height)
            .bind(status_event_index)
            .bind(runtime_version as i64)
            .bind(mapper_version as i32)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        }
        tx.commit().await.map_err(|e| e.to_string())
    }
}

// ---------------------------------------------------------- bounty accounts

#[derive(Debug, Default)]
pub struct BountyAccountsReport {
    /// Bounty accounts registered ACTIVE — i.e. swept for holdings.
    pub accounts: usize,
    /// Bounty accounts registered INACTIVE because their bounty reached a
    /// terminal status. The row is still a fact; the sweep just stops paying
    /// for it. Counted apart so a run that only retires accounts still says so.
    pub deactivated: usize,
    /// Projection rows whose `account_id` this run filled in (they were
    /// written before the chain's metadata was archived).
    pub linked: usize,
    /// Bounties whose account could NOT be derived — a corrupt id, a shape the
    /// derivations do not cover, or a legacy child bounty (whose address
    /// depends on an era this table does not record; see `bounty_account`).
    /// Loud, never zero silently.
    pub underivable: usize,
    /// Chains that HAVE bounties but no archived metadata yet: a BOOTSTRAP
    /// condition, resolved by the next sync after live ingestion, exactly like
    /// pallet labels.
    pub chains_missing_metadata: Vec<String>,
    /// Chains whose archived metadata declares no treasury pallet. PERMANENT
    /// for that runtime, and a different thing entirely from the line above —
    /// conflating them made a bootstrap look like a defect and a defect look
    /// like a bootstrap. Chains with no bounties at all appear in neither.
    pub chains_without_treasury_pallet: Vec<String>,
}

/// What a chain's metadata had to say about its treasury PalletId. Three
/// answers, not two: "not archived yet" and "this runtime has no treasury" are
/// different facts with different fixes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TreasuryPalletId {
    Found([u8; 8]),
    NoMetadata,
    NoTreasuryPallet,
}

impl TreasuryPalletId {
    /// For callers that only need the id and treat every absence alike.
    pub fn found(self) -> Option<[u8; 8]> {
        match self {
            Self::Found(id) => Some(id),
            _ => None,
        }
    }
}

/// The treasury PalletId this chain's bounty sub-accounts hang off, read from
/// the chain's OWN metadata.
///
/// Not a constant and not a seed: bounty funds live at sub-accounts of the
/// TREASURY's pallet id (`modl ++ py/trsry ++ SCALE(("bt", 17))`), and which
/// pallet is the treasury is something the runtime declares. The same
/// pallet → instance vocabulary the treasury mapper uses picks it, so the
/// bounty accounts and the spend list can never disagree about what "treasury"
/// means.
///
/// HONEST GAP: pallet-multi-asset-bounties takes its funding PalletId from its
/// own config, which today is the treasury's on every runtime that has it. If a
/// runtime ever configures a different one, the `mbt`/`mcb` addresses derived
/// here are wrong — and wrong LOUDLY, because `verify-labels` probes them and a
/// funded bounty whose derived address does not exist is that check failing.
pub async fn treasury_pallet_id(
    pool: &PgPool,
    raw: &dyn raw_store::RawStore,
    chain_id: &str,
) -> Result<TreasuryPalletId> {
    let blob_loc: Option<(String,)> = sqlx::query_as(
        "select metadata_blob_location from substrate.runtime_versions \
         where chain_id = $1 and metadata_blob_location is not null \
         order by spec_version desc limit 1",
    )
    .bind(chain_id)
    .fetch_optional(pool)
    .await
    .with_context(|| format!("looking up metadata for {chain_id}"))?;
    let Some(blob) = blob_loc.and_then(|(loc,)| raw.get(&loc).ok()) else {
        return Ok(TreasuryPalletId::NoMetadata);
    };
    let ids = match acct::pallet_ids_from_metadata(&blob) {
        Ok(ids) => ids,
        Err(e) => {
            // the blob is here but unreadable — a bootstrap problem in kind
            // (the next archived spec_version may decode), not a statement
            // about which pallets the runtime has
            tracing::warn!(chain = %chain_id, error = %e,
                "PalletId walk failed — bounty accounts cannot be derived for this chain");
            return Ok(TreasuryPalletId::NoMetadata);
        }
    };
    Ok(ids
        .into_iter()
        .find(|pc| {
            adapter_substrate::treasury::instance_for_pallet(&pc.pallet.to_lowercase())
                == Some("treasury")
        })
        .map_or(TreasuryPalletId::NoTreasuryPallet, |pc| {
            TreasuryPalletId::Found(pc.id)
        }))
}

/// Register every indexed bounty's own account as treasury money.
///
/// THIS IS WHAT CLOSES THE HOLDINGS GAP migration 0010 left open, and it closes
/// it by DERIVATION rather than curation: the rows come from `treasury.bounties`
/// — i.e. from events we decoded — and their addresses from arithmetic, so a
/// bounty created tomorrow contributes its account the next time this runs with
/// nothing typed by hand. Once the rows exist, `snapshot_holdings` covers them
/// with no change to any endpoint.
///
/// It also back-fills `treasury.bounties.account_id` where the sink could not
/// derive it (a `bounties-range` that ran before any metadata was archived) —
/// so the join column converges rather than staying null forever.
pub async fn sync_bounty_accounts(
    pool: &PgPool,
    registry: &registry::Registry,
    raw: &dyn raw_store::RawStore,
) -> Result<BountyAccountsReport> {
    use adapter_substrate::frame_decoder::ss58_encode;

    let mut report = BountyAccountsReport::default();

    for chain in registry
        .chains()
        .filter(|c| c.family == registry::ChainFamily::Substrate)
    {
        // ASK WHAT THERE IS TO DO FIRST. A chain with no indexed bounties is
        // not missing anything, and reporting it as missing metadata put
        // chains with nothing to sync in a list that is supposed to name
        // problems.
        let bounties: Vec<(String, i64, i64, String)> = sqlx::query_as(
            "select instance, bounty_id, child_id, status from treasury.bounties \
             where chain_id = $1 order by instance, bounty_id, child_id",
        )
        .bind(&chain.id)
        .fetch_all(pool)
        .await
        .with_context(|| format!("listing bounties on {}", chain.id))?;
        if bounties.is_empty() {
            continue;
        }
        let pallet_id = match treasury_pallet_id(pool, raw, &chain.id).await? {
            TreasuryPalletId::Found(id) => id,
            // fixes itself after live ingestion archives a metadata blob
            TreasuryPalletId::NoMetadata => {
                report.chains_missing_metadata.push(chain.id.clone());
                continue;
            }
            // does NOT fix itself: this runtime has no treasury pallet, so its
            // bounty accounts hang off nothing we can name
            TreasuryPalletId::NoTreasuryPallet => {
                report.chains_without_treasury_pallet.push(chain.id.clone());
                continue;
            }
        };

        let prefix = chain.ss58_prefix.unwrap_or(42);
        let mut tx = pool.begin().await.context("begin bounty account sync")?;
        for (instance, bounty_id, child_id, status) in bounties {
            let child = (child_id > PARENT_SENTINEL).then_some(child_id as u64);
            let Some((account, derivation)) =
                bounty_account(&pallet_id, &instance, bounty_id as u64, child)
            else {
                tracing::warn!(
                    chain = %chain.id, instance, bounty_id, child_id,
                    "bounty account not derivable — refusing to label an address we \
                     cannot derive unambiguously"
                );
                report.underivable += 1;
                continue;
            };
            // A TERMINAL BOUNTY IS REGISTERED INACTIVE, and this is a cost
            // decision with a real number behind it: slice 6's holdings sweep
            // is accounts × assets INCLUDING zeros (measured 3 × 892 = 2676
            // probes on one chain), while Polkadot carries ~40 live parent
            // bounties and several hundred children. Keeping every claimed,
            // canceled and rejected bounty active forever would turn one sweep
            // into ~10^5 probes and as many anchor rows, essentially all of
            // them permanently zero — the account is emptied and removed from
            // pallet storage when the bounty concludes. The ROW stays, because
            // a concluded bounty's address is still a fact and its history is
            // still readable; only the sweep stops paying for it.
            let active = !matches!(status.as_str(), "claimed" | "canceled" | "rejected");
            crate::assets_pg::upsert_treasury_account(
                &mut tx,
                &chain.id,
                &account,
                "bounty",
                &chain.network,
                Some(&instance),
                &bounty_label(&instance, bounty_id as u64, child),
                Some(&derivation),
                "derived",
                &ss58_encode(prefix, &account),
                Some(
                    "derived from an indexed bounty: this account exists in the list \
                     only because its bounty's events were decoded",
                ),
                active,
            )
            .await?;
            if active {
                report.accounts += 1;
            } else {
                report.deactivated += 1;
            }

            // fill the projection's join column where the sink could not
            let filled = sqlx::query(
                "update treasury.bounties set account_id = $1, updated_at = now() \
                 where chain_id = $2 and instance = $3 and bounty_id = $4 \
                   and child_id = $5 and account_id is null",
            )
            .bind(&account[..])
            .bind(&chain.id)
            .bind(&instance)
            .bind(bounty_id)
            .bind(child_id)
            .execute(&mut *tx)
            .await
            .with_context(|| format!("linking bounty {instance}/{bounty_id} on {}", chain.id))?;
            report.linked += filled.rows_affected() as usize;
        }
        tx.commit().await.context("commit bounty account sync")?;
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The derivations migration 0011 names, against the vectors
    /// `adapter_substrate::accounts` pins — so the SQL layer and the adapter
    /// cannot drift apart about where a bounty's money is.
    #[test]
    fn every_instance_derives_its_own_account_and_the_rest_are_refused() {
        let t = b"py/trsry";
        let (a, d) = bounty_account(t, "bounties", 17, None).expect("legacy parent");
        assert_eq!(
            format!("0x{}", hex::encode(a)),
            "0x6d6f646c70792f74727372790862741100000000000000000000000000000000"
        );
        assert_eq!(d, "modl:py/trsry/bt/17");

        // A LEGACY CHILD BOUNTY IS REFUSED, not derived — its address depends
        // on which side of pallet-child-bounties 38.0.0 the row belongs to (a
        // global child id before, a per-parent one after, with an on-chain
        // renumbering + balance transfer between), and nothing here records
        // that. A confident address for the wrong era is worse than none.
        assert!(bounty_account(t, "child_bounties", 17, Some(3)).is_none());

        // the modern pallet's prefix is a fixed-size array: no compact length
        let (m, d) = bounty_account(t, "multi_asset_bounties", 17, None).expect("modern parent");
        assert_ne!(a, m, "the two generations must not collide at one index");
        assert_eq!(d, "modl:py/trsry/mbt/17");
        let (mc, _) = bounty_account(t, "multi_asset_bounties", 17, Some(0)).expect("modern child");
        assert_ne!(m, mc, "child 0 is not the parent");

        // shapes that cannot name an account are REFUSED, never guessed
        assert!(bounty_account(t, "bounties", 17, Some(3)).is_none());
        assert!(bounty_account(t, "child_bounties", 17, None).is_none());
        assert!(bounty_account(t, "some_future_pallet", 1, None).is_none());
        assert!(bounty_account(t, "bounties", u64::MAX, None).is_none());
    }

    #[test]
    fn the_parent_sentinel_never_escapes_into_a_child_id() {
        assert_eq!(child_column(None), PARENT_SENTINEL);
        assert_eq!(child_column(Some(0)), 0, "child 0 is not the parent");
        assert_eq!(child_column(Some(3)), 3);
    }

    #[test]
    fn labels_disambiguate_the_two_id_spaces() {
        assert_eq!(bounty_label("bounties", 17, None), "Bounty 17");
        assert_eq!(
            bounty_label("child_bounties", 17, Some(3)),
            "Child bounty 17-3"
        );
        // the modern id space starts at 0 too, so its labels say so
        assert_eq!(
            bounty_label("multi_asset_bounties", 17, None),
            "Multi-asset bounty 17"
        );
    }
}
