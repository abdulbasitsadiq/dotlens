//! Postgres backend for the treasury worker: facts into
//! `treasury.spend_events` (insert-ignore — re-mapping any range is a no-op)
//! plus the ordering-guarded `treasury.spends` projection, atomically per block.
//!
//! The canonical-event SOURCE is `balances_pg::PgEventSource`, shared with the
//! balances/gov/votes workers by design.
//!
//! NUMERIC values are bound as text and cast server-side (`$n::numeric`).

use async_trait::async_trait;
use ingest::treasury::{SpendFact, TreasurySink};
use sqlx::PgPool;
use std::collections::HashSet;

pub struct PgSpendSink {
    pool: PgPool,
}

impl PgSpendSink {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

fn opt_num(v: Option<u128>) -> Option<String> {
    v.map(|n| n.to_string())
}

#[async_trait]
impl TreasurySink for PgSpendSink {
    async fn write(
        &self,
        chain_id: &str,
        height: u64,
        runtime_version: u32,
        mapper_version: u32,
        rows: &[(u32, SpendFact)],
    ) -> Result<(), String> {
        // the fact table is keyed (chain, block, event): one fact per event by
        // contract, and insert-ignore would silently swallow a second one
        let mut seen: HashSet<u32> = HashSet::new();
        for (event_index, _) in rows {
            if !seen.insert(*event_index) {
                return Err(format!(
                    "two treasury facts for {chain_id}/{height} event {event_index} — \
                     the fact table is keyed by event; mapper contract violated"
                ));
            }
        }

        // deterministic global lock order for the projection upserts (by
        // projection key, NOT by height), so a concurrent treasury-range and
        // follower cannot deadlock on DO UPDATE row locks
        let mut ordered: Vec<&(u32, SpendFact)> = rows.iter().collect();
        ordered.sort_by(|(ai, a), (bi, b)| {
            (&a.instance, &a.spend_kind, a.spend_id, ai)
                .cmp(&(&b.instance, &b.spend_kind, b.spend_id, bi))
        });

        let mut tx = self.pool.begin().await.map_err(|e| e.to_string())?;
        for (event_index, f) in ordered {
            sqlx::query(
                "insert into treasury.spend_events \
                     (chain_id, block_height, event_index, instance, spend_kind, spend_id, \
                      kind, amount, figure_kind, slashed, asset_kind, beneficiary, \
                      beneficiary_location, payment_id, valid_from, expire_at, attribution, \
                      data, runtime_version, mapper_version, asset_location, asset_key) \
                 values ($1,$2,$3,$4,$5,$6,$7,$8::numeric,$9,$10::numeric,$11,$12,$13,$14,$15, \
                         $16,$17,$18,$19,$20,$21,$22) \
                 on conflict (chain_id, block_height, event_index) do nothing",
            )
            .bind(chain_id)
            .bind(height as i64)
            .bind(*event_index as i32)
            .bind(&f.instance)
            .bind(&f.spend_kind)
            .bind(f.spend_id.map(|i| i as i64))
            .bind(&f.kind)
            .bind(opt_num(f.amount))
            .bind(&f.figure_kind)
            .bind(opt_num(f.slashed))
            .bind(&f.asset_kind)
            .bind(&f.beneficiary)
            .bind(&f.beneficiary_location)
            .bind(&f.payment_id)
            .bind(f.valid_from.map(|v| v as i64))
            .bind(f.expire_at.map(|v| v as i64))
            .bind(&f.attribution)
            .bind(&f.data)
            .bind(runtime_version as i64)
            .bind(mapper_version as i32)
            .bind(&f.asset_location)
            .bind(&f.asset_key)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;

            // pot flows name no spend, and status-less facts must not move the
            // projection — both stay out of it, honestly
            let (Some(spend_kind), Some(spend_id), Some(status)) =
                (f.spend_kind.as_ref(), f.spend_id, f.status.as_ref())
            else {
                continue;
            };

            // A CASE ladder, NOT a WHERE guard — and the difference is a real
            // bug the pg test caught. Treasury events do not each carry the
            // whole picture: only the approval knows the amount, asset kind
            // and beneficiary, while `Paid`/`SpendProcessed` know only the
            // status. A `where … > …` guard skips the ENTIRE row update for an
            // older event, so ingesting a range out of order (terminal block
            // first, which `treasury-range` legitimately does) would drop the
            // approval's value columns forever.
            //
            // So: the status triplet (and its lineage) moves only when
            // strictly newer, while the value columns are FIRST-NON-NULL-WINS
            // — `coalesce(existing, excluded)`, matching migration 0006's
            // polarity. That matters because "exactly one source" is false for
            // the legacy id space: amount comes from both SpendApproved.amount
            // and Awarded.award, beneficiary from both .beneficiary and
            // .account (reviewer catch). They agree in practice, and
            // first-wins makes the row stable rather than order-dependent.
            //
            // `payment_id` needs MORE than that: both `Paid` and
            // `PaymentFailed` carry one, and the status coordinate cannot
            // stand in for them because `SpendProcessed` moves the status
            // while carrying no payment at all — so a terminal-first replay
            // would leave a paid spend labelled with the FAILED payment's id.
            // It therefore gets its own (payment_height, payment_event_index)
            // coordinate and wins by that, converging under any order.
            //
            // `first_seen` takes the minimum. NOTE least()/greatest() ignore
            // NULLs in Postgres; safe here only because both columns are NOT
            // NULL and always bound.
            sqlx::query(
                "insert into treasury.spends \
                     (chain_id, instance, spend_kind, spend_id, status, amount, slashed, \
                      asset_kind, beneficiary, beneficiary_location, payment_id, \
                      payment_height, payment_event_index, valid_from, expire_at, \
                      first_seen_height, status_height, status_event_index, \
                      runtime_version, mapper_version, asset_location, asset_key) \
                 values ($1,$2,$3,$4,$5,$6::numeric,$7::numeric,$8,$9,$10,$11,$12,$13,$14,$15, \
                         $16,$17,$18,$19,$20,$21,$22) \
                 on conflict (chain_id, instance, spend_kind, spend_id) do update set \
                     status = case when (excluded.status_height, excluded.status_event_index) \
                                        > (treasury.spends.status_height, \
                                           treasury.spends.status_event_index) \
                                   then excluded.status else treasury.spends.status end, \
                     status_height = greatest(excluded.status_height, \
                                              treasury.spends.status_height), \
                     status_event_index = case when (excluded.status_height, \
                                                     excluded.status_event_index) \
                                                    > (treasury.spends.status_height, \
                                                       treasury.spends.status_event_index) \
                                               then excluded.status_event_index \
                                               else treasury.spends.status_event_index end, \
                     runtime_version = case when (excluded.status_height, \
                                                  excluded.status_event_index) \
                                                 > (treasury.spends.status_height, \
                                                    treasury.spends.status_event_index) \
                                            then excluded.runtime_version \
                                            else treasury.spends.runtime_version end, \
                     mapper_version = case when (excluded.status_height, \
                                                  excluded.status_event_index) \
                                                 > (treasury.spends.status_height, \
                                                    treasury.spends.status_event_index) \
                                            then excluded.mapper_version \
                                            else treasury.spends.mapper_version end, \
                     amount = coalesce(treasury.spends.amount, excluded.amount), \
                     slashed = coalesce(treasury.spends.slashed, excluded.slashed), \
                     asset_kind = coalesce(treasury.spends.asset_kind, excluded.asset_kind), \
                     beneficiary = coalesce(treasury.spends.beneficiary, excluded.beneficiary), \
                     beneficiary_location = coalesce(treasury.spends.beneficiary_location, \
                                                     excluded.beneficiary_location), \
                     payment_id = case when excluded.payment_id is not null \
                                        and (coalesce(treasury.spends.payment_height, -1), \
                                             coalesce(treasury.spends.payment_event_index, -1)) \
                                          < (excluded.payment_height, \
                                             excluded.payment_event_index) \
                                       then excluded.payment_id \
                                       else treasury.spends.payment_id end, \
                     payment_height = case when excluded.payment_id is not null \
                                            and (coalesce(treasury.spends.payment_height, -1), \
                                                 coalesce(treasury.spends.payment_event_index, -1)) \
                                              < (excluded.payment_height, \
                                                 excluded.payment_event_index) \
                                           then excluded.payment_height \
                                           else treasury.spends.payment_height end, \
                     payment_event_index = case when excluded.payment_id is not null \
                                                 and (coalesce(treasury.spends.payment_height, -1), \
                                                      coalesce(treasury.spends.payment_event_index, -1)) \
                                                   < (excluded.payment_height, \
                                                      excluded.payment_event_index) \
                                                then excluded.payment_event_index \
                                                else treasury.spends.payment_event_index end, \
                     valid_from = coalesce(treasury.spends.valid_from, excluded.valid_from), \
                     expire_at = coalesce(treasury.spends.expire_at, excluded.expire_at), \
                     first_seen_height = least(excluded.first_seen_height, \
                                               treasury.spends.first_seen_height), \
                     asset_location = coalesce(treasury.spends.asset_location, \
                                               excluded.asset_location), \
                     asset_key = coalesce(treasury.spends.asset_key, excluded.asset_key), \
                     updated_at = now()",
            )
            .bind(chain_id)
            .bind(&f.instance)
            .bind(spend_kind)
            .bind(spend_id as i64)
            .bind(status)
            .bind(opt_num(f.amount))
            .bind(opt_num(f.slashed))
            .bind(&f.asset_kind)
            .bind(&f.beneficiary)
            .bind(&f.beneficiary_location)
            .bind(&f.payment_id)
            // the payment's OWN coordinate, NULL unless this event carried one
            .bind(f.payment_id.as_ref().map(|_| height as i64))
            .bind(f.payment_id.as_ref().map(|_| *event_index as i32))
            .bind(f.valid_from.map(|v| v as i64))
            .bind(f.expire_at.map(|v| v as i64))
            .bind(height as i64)
            .bind(height as i64)
            .bind(*event_index as i32)
            .bind(runtime_version as i64)
            .bind(mapper_version as i32)
            .bind(&f.asset_location)
            .bind(&f.asset_key)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        }
        tx.commit().await.map_err(|e| e.to_string())
    }
}
