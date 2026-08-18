//! Postgres store for Tier 1 simulation results (Phase 3, slice 1).
//!
//! `sim.simulation_results` holds OBSERVATIONS, so this file is deliberately
//! the least clever sink in the project: one insert, `on conflict do nothing`,
//! no projection, no ordering guard, no merge. There is nothing to converge —
//! an answer given by a specific runtime at a specific state does not improve
//! when asked again, and if a second run ever differed, silently overwriting
//! would erase the only evidence that it did.

//! **SLICE 5** adds `sim.xcm_simulations` beside it, on the same terms, plus the
//! one nullable column that turns a forwarded list into an attributable one.

use anyhow::{Context, Result};
use async_trait::async_trait;
use sim::{SimError, SimRecord, SimStore, XcmSimRecord, XcmSimStore};
use sqlx::{PgPool, Row};

pub struct PgSimStore {
    pool: PgPool,
}

impl PgSimStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

pub struct PgXcmSimStore {
    pool: PgPool,
}

impl PgXcmSimStore {
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
                spec_version, api_version, metadata_version, sim_version, raw_location, \
                baseline_input_hash \
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
        baseline_input_hash: r.try_get("baseline_input_hash")?,
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
             spec_version, api_version, metadata_version, sim_version, raw_location, \
             baseline_input_hash) \
         values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,\
                 $23,$24,$25) \
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
    .bind(&r.baseline_input_hash)
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

// ---------------------------------------------------------------- dry_run_xcm

/// THE ORDER OF THIS LIST IS LOAD-BEARING: the insert below binds positionally
/// against it, so reordering it "for readability" silently transposes any two
/// columns of the same SQL type — `weight_used`/`xcm_error` and
/// `program`/`origin_location`/`effects` are all jsonb, and
/// `source_forwarded_index`/`source_message_index` are both integer.
///
/// It is deliberately NOT shared with the select below, even though the two
/// lists are identical today: a name-read select is indifferent to order and an
/// ordinal-bound insert is not, so one const serving both would make a
/// dangerous edit look safe in half the places it is read.
const XCM_INSERT_COLUMNS: &str =
    "chain_id, at_block_hash, input_hash, at_height, tier, program_hash, \
     program, program_summary, origin_location, origin_ref, status, weight_used, xcm_error, \
     emitted_events, event_count, forwarded_xcms, baseline_input_hash, effects, note, \
     source_chain_id, source_at_block_hash, source_input_hash, source_forwarded_index, \
     source_message_index, spec_version, api_version, metadata_version, sim_version, raw_location";

/// Read BY NAME, so this order is free to differ and does not matter.
const XCM_SELECT_COLUMNS: &str =
    "chain_id, at_block_hash, input_hash, at_height, tier, program_hash, \
     program, program_summary, origin_location, origin_ref, status, weight_used, xcm_error, \
     emitted_events, event_count, forwarded_xcms, baseline_input_hash, effects, note, \
     source_chain_id, source_at_block_hash, source_input_hash, source_forwarded_index, \
     source_message_index, spec_version, api_version, metadata_version, sim_version, raw_location";

pub async fn xcm_simulation_at(
    pool: &PgPool,
    chain_id: &str,
    at_block_hash: &str,
    input_hash: &str,
    tier: &str,
) -> Result<Option<XcmSimRecord>> {
    let row = sqlx::query(&format!(
        "select {XCM_SELECT_COLUMNS} from sim.xcm_simulations \
         where chain_id = $1 and at_block_hash = $2 and input_hash = $3 and tier = $4"
    ))
    .bind(chain_id)
    .bind(at_block_hash)
    .bind(input_hash)
    .bind(tier)
    .fetch_optional(pool)
    .await
    .context("reading a recorded xcm simulation")?;

    let Some(r) = row else { return Ok(None) };
    Ok(Some(XcmSimRecord {
        chain_id: r.try_get("chain_id")?,
        at_block_hash: r.try_get("at_block_hash")?,
        input_hash: r.try_get("input_hash")?,
        at_height: r.try_get::<i64, _>("at_height")? as u64,
        tier: r.try_get("tier")?,
        program_hash: r.try_get("program_hash")?,
        program: r.try_get("program")?,
        program_summary: r.try_get("program_summary")?,
        origin_location: r.try_get("origin_location")?,
        origin_ref: r.try_get("origin_ref")?,
        status: r.try_get("status")?,
        weight_used: r.try_get("weight_used")?,
        xcm_error: r.try_get("xcm_error")?,
        emitted_events: r.try_get("emitted_events")?,
        event_count: r.try_get::<i32, _>("event_count")? as u32,
        forwarded_xcms: r.try_get("forwarded_xcms")?,
        baseline_input_hash: r.try_get("baseline_input_hash")?,
        effects: r.try_get("effects")?,
        note: r.try_get("note")?,
        source_chain_id: r.try_get("source_chain_id")?,
        source_at_block_hash: r.try_get("source_at_block_hash")?,
        source_input_hash: r.try_get("source_input_hash")?,
        source_forwarded_index: r
            .try_get::<Option<i32>, _>("source_forwarded_index")?
            .map(|n| n as u32),
        source_message_index: r
            .try_get::<Option<i32>, _>("source_message_index")?
            .map(|n| n as u32),
        spec_version: r.try_get::<i64, _>("spec_version")? as u32,
        api_version: r.try_get::<i32, _>("api_version")? as u32,
        metadata_version: r.try_get::<i32, _>("metadata_version")? as u32,
        sim_version: r.try_get::<i32, _>("sim_version")? as u32,
        raw_location: r.try_get("raw_location")?,
    }))
}

pub async fn insert_xcm_simulation(pool: &PgPool, r: &XcmSimRecord) -> Result<()> {
    sqlx::query(&format!(
        "insert into sim.xcm_simulations ({XCM_INSERT_COLUMNS}) values \
         ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,\
          $24,$25,$26,$27,$28,$29) \
         on conflict (chain_id, at_block_hash, input_hash, tier) do nothing"
    ))
    .bind(&r.chain_id)
    .bind(&r.at_block_hash)
    .bind(&r.input_hash)
    .bind(r.at_height as i64)
    .bind(&r.tier)
    .bind(&r.program_hash)
    .bind(&r.program)
    .bind(&r.program_summary)
    .bind(&r.origin_location)
    .bind(&r.origin_ref)
    .bind(&r.status)
    .bind(&r.weight_used)
    .bind(&r.xcm_error)
    .bind(&r.emitted_events)
    .bind(r.event_count as i32)
    .bind(&r.forwarded_xcms)
    .bind(&r.baseline_input_hash)
    .bind(&r.effects)
    .bind(&r.note)
    .bind(&r.source_chain_id)
    .bind(&r.source_at_block_hash)
    .bind(&r.source_input_hash)
    .bind(r.source_forwarded_index.map(|n| n as i32))
    .bind(r.source_message_index.map(|n| n as i32))
    .bind(r.spec_version as i64)
    .bind(r.api_version as i32)
    .bind(r.metadata_version as i32)
    .bind(r.sim_version as i32)
    .bind(&r.raw_location)
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "recording xcm simulation {} at {}",
            r.input_hash, r.at_block_hash
        )
    })?;
    Ok(())
}

#[async_trait]
impl XcmSimStore for PgXcmSimStore {
    async fn get(
        &self,
        chain_id: &str,
        at_block_hash: &str,
        input_hash: &str,
        tier: &str,
    ) -> Result<Option<XcmSimRecord>, SimError> {
        xcm_simulation_at(&self.pool, chain_id, at_block_hash, input_hash, tier)
            .await
            .map_err(|e| SimError::Store(e.to_string()))
    }

    async fn put(&self, record: &XcmSimRecord) -> Result<(), SimError> {
        insert_xcm_simulation(&self.pool, record)
            .await
            .map_err(|e| SimError::Store(e.to_string()))
    }
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
