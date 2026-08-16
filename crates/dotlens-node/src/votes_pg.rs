//! Postgres backends for the votes worker: vote + delegation facts into
//! `gov.votes` / `gov.delegation_events` (insert-ignore — re-mapping any range
//! is a no-op) plus the two ordering-guarded projections
//! (`gov.vote_positions`, `gov.delegations`), atomically per block. Also
//! `insert_voting_anchor`: one immutable `ConvictionVoting.VotingFor` state
//! observation.
//!
//! The canonical-event SOURCE is `balances_pg::PgEventSource` — the votes
//! worker shares the balances/gov EventSource contract by design.
//!
//! NUMERIC values are bound as text and cast server-side (`$n::numeric`) —
//! plancks exceed u64 and we deliberately carry no decimal dep.

use anyhow::{Context, Result};
use async_trait::async_trait;
use ingest::votes::{DelegationRecord, VoteFact, VoteRecord, VoteSink};
use sqlx::PgPool;
use std::collections::HashSet;

pub struct PgVoteSink {
    pool: PgPool,
}

impl PgVoteSink {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

fn num(v: u128) -> String {
    v.to_string()
}

fn opt_num(v: Option<u128>) -> Option<String> {
    v.map(|n| n.to_string())
}

#[async_trait]
impl VoteSink for PgVoteSink {
    async fn write(
        &self,
        chain_id: &str,
        height: u64,
        runtime_version: u32,
        mapper_version: u32,
        rows: &[(u32, VoteFact)],
    ) -> Result<(), String> {
        // Both fact tables are keyed (chain, height, event_index): the mapper
        // contract is ONE fact per event, and insert-ignore would silently
        // swallow a second one. Refuse loudly instead of losing a decision.
        let mut seen_votes: HashSet<u32> = HashSet::new();
        let mut seen_delegations: HashSet<u32> = HashSet::new();
        for (event_index, fact) in rows {
            let fresh = match fact {
                VoteFact::Vote(_) => seen_votes.insert(*event_index),
                VoteFact::Delegation(_) => seen_delegations.insert(*event_index),
            };
            if !fresh {
                return Err(format!(
                    "two facts of the same kind for {chain_id}/{height} event {event_index} — \
                     the vote fact tables are keyed by event; mapper contract violated"
                ));
            }
        }

        let mut votes: Vec<(u32, &VoteRecord)> = Vec::new();
        let mut delegations: Vec<(u32, &DelegationRecord)> = Vec::new();
        for (event_index, fact) in rows {
            match fact {
                VoteFact::Vote(v) => votes.push((*event_index, v)),
                VoteFact::Delegation(d) => delegations.push((*event_index, d)),
            }
        }
        // Deterministic GLOBAL lock order for the projection upserts (all
        // votes, then all delegations, each ordered by its projection key and
        // NOT by height): concurrent votes-range and follower transactions
        // touching the same referenda / delegators in different orders could
        // otherwise deadlock on the DO UPDATE row locks — the same review
        // catch as the gov timeline sink. Borrowing comparators, no clones.
        votes.sort_by(|(ai, a), (bi, b)| {
            (&a.class, a.referendum_id.unwrap_or(u64::MAX), &a.voter, ai)
                .cmp(&(&b.class, b.referendum_id.unwrap_or(u64::MAX), &b.voter, bi))
        });
        delegations.sort_by(|(ai, a), (bi, b)| {
            (&a.class, a.track_id.unwrap_or(u32::MAX), &a.delegator, ai)
                .cmp(&(&b.class, b.track_id.unwrap_or(u32::MAX), &b.delegator, bi))
        });

        let mut tx = self.pool.begin().await.map_err(|e| e.to_string())?;

        for (event_index, v) in votes {
            sqlx::query(
                "insert into gov.votes \
                     (chain_id, block_height, event_index, class, referendum_id, voter, kind, \
                      vote_type, aye_balance, nay_balance, abstain_balance, conviction, \
                      conviction_label, aye_votes, nay_votes, support, attribution, data, \
                      runtime_version, mapper_version) \
                 values ($1,$2,$3,$4,$5,$6,$7,$8,$9::numeric,$10::numeric,$11::numeric,$12,$13, \
                         $14::numeric,$15::numeric,$16::numeric,$17,$18,$19,$20) \
                 on conflict (chain_id, block_height, event_index) do nothing",
            )
            .bind(chain_id)
            .bind(height as i64)
            .bind(event_index as i32)
            .bind(&v.class)
            .bind(v.referendum_id.map(|r| r as i64))
            .bind(&v.voter)
            .bind(&v.kind)
            .bind(&v.vote_type)
            .bind(opt_num(v.aye_balance))
            .bind(opt_num(v.nay_balance))
            .bind(opt_num(v.abstain_balance))
            .bind(v.conviction.map(|c| c as i16))
            .bind(&v.conviction_label)
            .bind(num(v.aye_votes))
            .bind(num(v.nay_votes))
            .bind(num(v.support))
            .bind(&v.attribution)
            .bind(&v.data)
            .bind(runtime_version as i64)
            .bind(mapper_version as i32)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;

            // unattributed facts never reach the projection: there is no
            // referendum to attach them to, and we will not invent one
            let Some(referendum_id) = v.referendum_id else {
                continue;
            };
            sqlx::query(
                "insert into gov.vote_positions \
                     (chain_id, class, referendum_id, voter, active, vote_type, aye_balance, \
                      nay_balance, abstain_balance, conviction, conviction_label, aye_votes, \
                      nay_votes, support, status_height, status_event_index, runtime_version, \
                      mapper_version) \
                 values ($1,$2,$3,$4,$5,$6,$7::numeric,$8::numeric,$9::numeric,$10,$11, \
                         $12::numeric,$13::numeric,$14::numeric,$15,$16,$17,$18) \
                 on conflict (chain_id, class, referendum_id, voter) do update set \
                     active = excluded.active, vote_type = excluded.vote_type, \
                     aye_balance = excluded.aye_balance, nay_balance = excluded.nay_balance, \
                     abstain_balance = excluded.abstain_balance, \
                     conviction = excluded.conviction, \
                     conviction_label = excluded.conviction_label, \
                     aye_votes = excluded.aye_votes, nay_votes = excluded.nay_votes, \
                     support = excluded.support, \
                     status_height = excluded.status_height, \
                     status_event_index = excluded.status_event_index, \
                     runtime_version = excluded.runtime_version, \
                     mapper_version = excluded.mapper_version, updated_at = now() \
                 where (excluded.status_height, excluded.status_event_index) \
                       > (gov.vote_positions.status_height, gov.vote_positions.status_event_index)",
            )
            .bind(chain_id)
            .bind(&v.class)
            .bind(referendum_id as i64)
            .bind(&v.voter)
            .bind(v.kind == "voted") // vote_removed → the position goes inactive
            .bind(&v.vote_type)
            .bind(opt_num(v.aye_balance))
            .bind(opt_num(v.nay_balance))
            .bind(opt_num(v.abstain_balance))
            .bind(v.conviction.map(|c| c as i16))
            .bind(&v.conviction_label)
            .bind(num(v.aye_votes))
            .bind(num(v.nay_votes))
            .bind(num(v.support))
            .bind(height as i64)
            .bind(event_index as i32)
            .bind(runtime_version as i64)
            .bind(mapper_version as i32)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        }

        for (event_index, d) in delegations {
            sqlx::query(
                "insert into gov.delegation_events \
                     (chain_id, block_height, event_index, class, track_id, delegator, target, \
                      kind, attribution, data, runtime_version, mapper_version) \
                 values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) \
                 on conflict (chain_id, block_height, event_index) do nothing",
            )
            .bind(chain_id)
            .bind(height as i64)
            .bind(event_index as i32)
            .bind(&d.class)
            .bind(d.track_id.map(|t| t as i32))
            .bind(&d.delegator)
            .bind(&d.target)
            .bind(&d.kind)
            .bind(&d.attribution)
            .bind(&d.data)
            .bind(runtime_version as i64)
            .bind(mapper_version as i32)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;

            // track-less legacy events stay out of the projection (same rule
            // as unattributed votes)
            let Some(track_id) = d.track_id else {
                continue;
            };
            sqlx::query(
                "insert into gov.delegations \
                     (chain_id, class, track_id, delegator, target, active, status_height, \
                      status_event_index, runtime_version, mapper_version) \
                 values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) \
                 on conflict (chain_id, class, track_id, delegator) do update set \
                     target = excluded.target, active = excluded.active, \
                     status_height = excluded.status_height, \
                     status_event_index = excluded.status_event_index, \
                     runtime_version = excluded.runtime_version, \
                     mapper_version = excluded.mapper_version, updated_at = now() \
                 where (excluded.status_height, excluded.status_event_index) \
                       > (gov.delegations.status_height, gov.delegations.status_event_index)",
            )
            .bind(chain_id)
            .bind(&d.class)
            .bind(track_id as i32)
            .bind(&d.delegator)
            .bind(&d.target)
            .bind(d.kind == "delegated")
            .bind(height as i64)
            .bind(event_index as i32)
            .bind(runtime_version as i64)
            .bind(mapper_version as i32)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        }

        tx.commit().await.map_err(|e| e.to_string())
    }
}

/// Record one voting anchor (VotingFor read at the END of `height`). Anchors
/// are immutable observations: conflicts are ignored, never updated.
#[allow(clippy::too_many_arguments)]
pub async fn insert_voting_anchor(
    pool: &PgPool,
    chain_id: &str,
    account_id: &[u8],
    class: &str,
    track_id: u32,
    height: u64,
    p: &adapter_substrate::votes::VotingPosition,
    spec_version: Option<u32>,
    source: &str,
    note: Option<&str>,
) -> Result<()> {
    sqlx::query(
        "insert into gov.voting_anchors \
             (chain_id, account_id, class, track_id, block_height, mode, delegating_target, \
              delegating_balance, delegating_conviction, delegating_conviction_label, \
              casting_vote_count, delegations_votes, delegations_capital, prior_until, \
              prior_balance, raw, spec_version, decoder_version, source, note) \
         values ($1,$2,$3,$4,$5,$6,$7,$8::numeric,$9,$10,$11,$12::numeric,$13::numeric,$14, \
                 $15::numeric,$16,$17,$18,$19,$20) \
         on conflict (chain_id, account_id, class, track_id, block_height) do nothing",
    )
    .bind(chain_id)
    .bind(account_id)
    .bind(class)
    .bind(track_id as i32)
    .bind(height as i64)
    .bind(&p.mode)
    .bind(&p.delegating_target)
    .bind(opt_num(p.delegating_balance))
    .bind(p.delegating_conviction.map(|c| c as i16))
    .bind(&p.delegating_conviction_label)
    .bind(p.casting_vote_count.map(|c| c as i32))
    .bind(opt_num(p.delegations_votes))
    .bind(opt_num(p.delegations_capital))
    .bind(p.prior_until.map(|h| h as i64))
    .bind(opt_num(p.prior_balance))
    .bind(&p.raw)
    .bind(spec_version.map(|s| s as i64))
    .bind(adapter_substrate::votes::VOTING_DECODER_VERSION as i32)
    .bind(source)
    .bind(note)
    .execute(pool)
    .await
    .with_context(|| format!("inserting voting anchor for {chain_id} track {track_id}"))?;
    Ok(())
}
