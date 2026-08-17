//! Postgres backend for the whitelist worker: facts into `gov.whitelist_events`
//! (insert-ignore) + the `gov.whitelisted_calls` projection (ordering-guarded
//! upsert), atomically per block.
//!
//! The canonical-event SOURCE is `balances_pg::PgEventSource`, shared with
//! every other domain worker.
//!
//! THREE INDEPENDENT COORDINATES, not one. `status` moves on the latest event
//! of any kind; the most recent `CallWhitelisted` keeps its own
//! `(whitelisted_height, whitelisted_event_index)`; and the dispatch outcome
//! keeps a third. They are separate because a hash can legitimately be
//! whitelisted, dispatched, and whitelisted AGAIN — a re-submission after a
//! failed enactment — and if the dispatch verdict rode the status coordinate
//! that re-whitelisting would either erase the earlier outcome or, worse,
//! re-attach it to the new attempt. Slice 5 learned this with `payment_id`.

use async_trait::async_trait;
use ingest::whitelist::{WhitelistFact, WhitelistSink};
use sqlx::PgPool;
use std::collections::HashSet;

pub struct PgWhitelistSink {
    pool: PgPool,
}

impl PgWhitelistSink {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl WhitelistSink for PgWhitelistSink {
    async fn write(
        &self,
        chain_id: &str,
        height: u64,
        runtime_version: u32,
        mapper_version: u32,
        rows: &[(u32, WhitelistFact)],
    ) -> Result<(), String> {
        // The fact table is keyed (chain, block, event), so two facts at one
        // event index have nowhere to go. REFUSE loudly rather than let
        // insert-ignore swallow the second one — the rule the votes sink
        // established. The mapper cannot currently produce this; a future one
        // that could needs a schema change, not a silent drop.
        let mut seen: HashSet<u32> = HashSet::new();
        for (event_index, _) in rows {
            if !seen.insert(*event_index) {
                return Err(format!(
                    "two whitelist facts at {chain_id}/{height} event {event_index} — the fact \
                     table is keyed by event and cannot hold both; this needs a schema change, \
                     not a silent drop"
                ));
            }
        }

        let mut tx = self.pool.begin().await.map_err(|e| e.to_string())?;

        // Deterministic upsert order (call_hash, event_index): the projection
        // uses DO UPDATE (row locks), so a concurrent whitelist-range and
        // follower touching the same hashes in different orders could deadlock.
        // A global lock order prevents it — keyed by the PROJECTION key, not by
        // height, for the same reason the votes sink is.
        let mut rows: Vec<&(u32, WhitelistFact)> = rows.iter().collect();
        rows.sort_by(|(ai, a), (bi, b)| (&a.call_hash, ai).cmp(&(&b.call_hash, bi)));

        for (event_index, f) in rows {
            sqlx::query(
                "insert into gov.whitelist_events \
                     (chain_id, block_height, event_index, call_hash, kind, \
                      dispatch_ok, dispatch_error, data, runtime_version, mapper_version) \
                 values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
                 on conflict (chain_id, block_height, event_index) do nothing",
            )
            .bind(chain_id)
            .bind(height as i64)
            .bind(*event_index as i32)
            .bind(&f.call_hash)
            .bind(&f.kind)
            .bind(f.dispatch_ok)
            .bind(&f.dispatch_error)
            .bind(&f.data)
            .bind(runtime_version as i64)
            .bind(mapper_version as i32)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;

            // Only the kind's own coordinate columns are offered; the guards
            // below decide whether they win. All three kinds are status-bearing
            // (see the fact's doc comment), so there is no info-only arm here
            // and no 'unknown' placeholder to guard against.
            let whitelisted_h = (f.kind == "whitelisted").then_some(height as i64);
            let whitelisted_i = (f.kind == "whitelisted").then_some(*event_index as i32);
            let dispatch_h = (f.kind == "dispatched").then_some(height as i64);
            let dispatch_i = (f.kind == "dispatched").then_some(*event_index as i32);

            // NOTE FOR ANYONE EDITING THE QUERY BELOW: it is one Rust string
            // with `\` line-continuations, so every line joins into a SINGLE
            // line of SQL. A `--` comment inside it would therefore comment out
            // the entire remainder of the statement. Keep the commentary here,
            // in Rust, and keep the SQL comment-free.
            //
            // `first_seen_height` is a plain LEAST, and that is safe ONLY
            // because this key includes chain_id: unlike merge_spend and
            // merge_bounty at the API layer, the two heights being compared
            // always come from ONE chain's number line. Taking a min across two
            // chains' block numbers is exactly the defect slice 7 found.
            //
            // The `whitelisted_*` and `dispatch_*` guards are NULL-safe by an
            // explicit is-null arm: a row comparison against NULL yields NULL,
            // which is not true, so without it the FIRST whitelisting (or the
            // first dispatch) could never land. `dispatch_ok` and
            // `dispatch_error` share one guard on purpose — they are a single
            // observation, and separate guards would let a later Ok keep an
            // earlier Err's error attached to it.
            sqlx::query(
                "insert into gov.whitelisted_calls \
                     (chain_id, call_hash, status, dispatch_ok, dispatch_error, \
                      dispatch_height, dispatch_event_index, first_seen_height, \
                      whitelisted_height, whitelisted_event_index, \
                      status_height, status_event_index, runtime_version, mapper_version) \
                 values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) \
                 on conflict (chain_id, call_hash) do update set \
                     first_seen_height = least(gov.whitelisted_calls.first_seen_height, \
                                               excluded.first_seen_height), \
                     status = case when (excluded.status_height, excluded.status_event_index) \
                                        > (gov.whitelisted_calls.status_height, gov.whitelisted_calls.status_event_index) \
                                   then excluded.status else gov.whitelisted_calls.status end, \
                     status_height = case when (excluded.status_height, excluded.status_event_index) \
                                               > (gov.whitelisted_calls.status_height, gov.whitelisted_calls.status_event_index) \
                                          then excluded.status_height else gov.whitelisted_calls.status_height end, \
                     status_event_index = case when (excluded.status_height, excluded.status_event_index) \
                                                    > (gov.whitelisted_calls.status_height, gov.whitelisted_calls.status_event_index) \
                                               then excluded.status_event_index else gov.whitelisted_calls.status_event_index end, \
                     runtime_version = case when (excluded.status_height, excluded.status_event_index) \
                                                 > (gov.whitelisted_calls.status_height, gov.whitelisted_calls.status_event_index) \
                                            then excluded.runtime_version else gov.whitelisted_calls.runtime_version end, \
                     mapper_version = case when (excluded.status_height, excluded.status_event_index) \
                                                > (gov.whitelisted_calls.status_height, gov.whitelisted_calls.status_event_index) \
                                           then excluded.mapper_version else gov.whitelisted_calls.mapper_version end, \
                     whitelisted_height = case when excluded.whitelisted_height is not null \
                                                and (gov.whitelisted_calls.whitelisted_height is null \
                                                     or (excluded.whitelisted_height, excluded.whitelisted_event_index) \
                                                        > (gov.whitelisted_calls.whitelisted_height, gov.whitelisted_calls.whitelisted_event_index)) \
                                               then excluded.whitelisted_height else gov.whitelisted_calls.whitelisted_height end, \
                     whitelisted_event_index = case when excluded.whitelisted_height is not null \
                                                     and (gov.whitelisted_calls.whitelisted_height is null \
                                                          or (excluded.whitelisted_height, excluded.whitelisted_event_index) \
                                                             > (gov.whitelisted_calls.whitelisted_height, gov.whitelisted_calls.whitelisted_event_index)) \
                                                    then excluded.whitelisted_event_index else gov.whitelisted_calls.whitelisted_event_index end, \
                     dispatch_ok = case when excluded.dispatch_height is not null \
                                         and (gov.whitelisted_calls.dispatch_height is null \
                                              or (excluded.dispatch_height, excluded.dispatch_event_index) \
                                                 > (gov.whitelisted_calls.dispatch_height, gov.whitelisted_calls.dispatch_event_index)) \
                                        then excluded.dispatch_ok else gov.whitelisted_calls.dispatch_ok end, \
                     dispatch_error = case when excluded.dispatch_height is not null \
                                            and (gov.whitelisted_calls.dispatch_height is null \
                                                 or (excluded.dispatch_height, excluded.dispatch_event_index) \
                                                    > (gov.whitelisted_calls.dispatch_height, gov.whitelisted_calls.dispatch_event_index)) \
                                           then excluded.dispatch_error else gov.whitelisted_calls.dispatch_error end, \
                     dispatch_height = case when excluded.dispatch_height is not null \
                                             and (gov.whitelisted_calls.dispatch_height is null \
                                                  or (excluded.dispatch_height, excluded.dispatch_event_index) \
                                                     > (gov.whitelisted_calls.dispatch_height, gov.whitelisted_calls.dispatch_event_index)) \
                                            then excluded.dispatch_height else gov.whitelisted_calls.dispatch_height end, \
                     dispatch_event_index = case when excluded.dispatch_height is not null \
                                                  and (gov.whitelisted_calls.dispatch_height is null \
                                                       or (excluded.dispatch_height, excluded.dispatch_event_index) \
                                                          > (gov.whitelisted_calls.dispatch_height, gov.whitelisted_calls.dispatch_event_index)) \
                                                 then excluded.dispatch_event_index else gov.whitelisted_calls.dispatch_event_index end, \
                     updated_at = now()",
            )
            .bind(chain_id)
            .bind(&f.call_hash)
            .bind(&f.kind)
            .bind(f.dispatch_ok)
            .bind(&f.dispatch_error)
            .bind(dispatch_h)
            .bind(dispatch_i)
            .bind(height as i64)
            .bind(whitelisted_h)
            .bind(whitelisted_i)
            .bind(height as i64)
            .bind(*event_index as i32)
            .bind(runtime_version as i64)
            .bind(mapper_version as i32)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        }

        tx.commit().await.map_err(|e| e.to_string())
    }
}
