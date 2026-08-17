//! Postgres store for Tier 1 simulation results (Phase 3, slice 1).
//!
//! `sim.simulation_results` holds OBSERVATIONS, so this file is deliberately
//! the least clever sink in the project: one insert, `on conflict do nothing`,
//! no projection, no ordering guard, no merge. There is nothing to converge —
//! an answer given by a specific runtime at a specific state does not improve
//! when asked again, and if a second run ever differed, silently overwriting
//! would erase the only evidence that it did.

use anyhow::{Context, Result};
use async_trait::async_trait;
use sim::{SimError, SimRecord, SimStore};
use sqlx::{PgPool, Row};

pub struct PgSimStore {
    pool: PgPool,
}

impl PgSimStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// Read one recorded run. Keyed by (chain, BLOCK HASH, input hash, TIER) — the
/// state, the question and how it was asked — which is what makes the cache hit
/// sound: the same call with the same origin against the same block, answered by
/// the same tier, is by construction the same answer.
pub async fn simulation_at(
    pool: &PgPool,
    chain_id: &str,
    at_block_hash: &str,
    input_hash: &str,
    tier: &str,
) -> Result<Option<SimRecord>> {
    let row = sqlx::query(
        "select chain_id, at_block_hash, input_hash, at_height, tier, call_hash, call_summary, \
                origin_spec, origin_json, xcm_version, status, dispatch_ok, dispatch_error, \
                emitted_events, event_count, local_xcm, forwarded_xcms, effects, note, \
                spec_version, api_version, metadata_version, sim_version, raw_location \
         from sim.simulation_results \
         where chain_id = $1 and at_block_hash = $2 and input_hash = $3 and tier = $4",
    )
    .bind(chain_id)
    .bind(at_block_hash)
    .bind(input_hash)
    .bind(tier)
    .fetch_optional(pool)
    .await
    .context("reading a recorded simulation")?;

    let Some(r) = row else { return Ok(None) };
    Ok(Some(SimRecord {
        chain_id: r.try_get("chain_id")?,
        at_block_hash: r.try_get("at_block_hash")?,
        input_hash: r.try_get("input_hash")?,
        at_height: r.try_get::<i64, _>("at_height")? as u64,
        tier: r.try_get("tier")?,
        call_hash: r.try_get("call_hash")?,
        call_summary: r.try_get("call_summary")?,
        origin_spec: r.try_get("origin_spec")?,
        origin_json: r.try_get("origin_json")?,
        xcm_version: r.try_get::<i32, _>("xcm_version")? as u32,
        status: r.try_get("status")?,
        dispatch_ok: r.try_get("dispatch_ok")?,
        dispatch_error: r.try_get("dispatch_error")?,
        emitted_events: r.try_get("emitted_events")?,
        event_count: r.try_get::<i32, _>("event_count")? as u32,
        local_xcm: r.try_get("local_xcm")?,
        forwarded_xcms: r.try_get("forwarded_xcms")?,
        effects: r.try_get("effects")?,
        note: r.try_get("note")?,
        spec_version: r.try_get::<i64, _>("spec_version")? as u32,
        api_version: r.try_get::<i32, _>("api_version")? as u32,
        metadata_version: r.try_get::<i32, _>("metadata_version")? as u32,
        sim_version: r.try_get::<i32, _>("sim_version")? as u32,
        raw_location: r.try_get("raw_location")?,
    }))
}

/// Record one run. Immutable: a second insert of the same (chain, block, input)
/// is a no-op, which is what makes re-running a drill safe.
pub async fn insert_simulation(pool: &PgPool, r: &SimRecord) -> Result<()> {
    sqlx::query(
        "insert into sim.simulation_results ( \
             chain_id, at_block_hash, input_hash, at_height, tier, call_hash, call_summary, \
             origin_spec, origin_json, xcm_version, status, dispatch_ok, dispatch_error, \
             emitted_events, event_count, local_xcm, forwarded_xcms, effects, note, \
             spec_version, api_version, metadata_version, sim_version, raw_location) \
         values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24) \
         on conflict (chain_id, at_block_hash, input_hash, tier) do nothing",
    )
    .bind(&r.chain_id)
    .bind(&r.at_block_hash)
    .bind(&r.input_hash)
    .bind(r.at_height as i64)
    .bind(&r.tier)
    .bind(&r.call_hash)
    .bind(&r.call_summary)
    .bind(&r.origin_spec)
    .bind(&r.origin_json)
    .bind(r.xcm_version as i32)
    .bind(&r.status)
    .bind(r.dispatch_ok)
    .bind(&r.dispatch_error)
    .bind(&r.emitted_events)
    .bind(r.event_count as i32)
    .bind(&r.local_xcm)
    .bind(&r.forwarded_xcms)
    .bind(&r.effects)
    .bind(&r.note)
    .bind(r.spec_version as i64)
    .bind(r.api_version as i32)
    .bind(r.metadata_version as i32)
    .bind(r.sim_version as i32)
    .bind(&r.raw_location)
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "recording simulation {} at {}",
            r.input_hash, r.at_block_hash
        )
    })?;
    Ok(())
}

#[async_trait]
impl SimStore for PgSimStore {
    async fn get(
        &self,
        chain_id: &str,
        at_block_hash: &str,
        input_hash: &str,
        tier: &str,
    ) -> Result<Option<SimRecord>, SimError> {
        simulation_at(&self.pool, chain_id, at_block_hash, input_hash, tier)
            .await
            .map_err(|e| SimError::Store(e.to_string()))
    }

    async fn put(&self, record: &SimRecord) -> Result<(), SimError> {
        insert_simulation(&self.pool, record)
            .await
            .map_err(|e| SimError::Store(e.to_string()))
    }
}
