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
use sim::{JobStore, NewSimJob, SimError, SimJob, SimRecord, SimStore, XcmSimRecord, XcmSimStore};
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
                baseline_input_hash, overrides, override_hash, storage_diff, storage_diff_count, \
                diff_status, built_block_hash, harness, dispatch_route, agenda_anchor \
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
        // NULLABLE SINCE 0021, and read as Option rather than defaulted: a fork
        // row has no `result_xcms_version` and no DryRunApi version, and reading
        // a NULL as 0 would put "XCM v0" and "DryRunApi v0" on a row that called
        // neither.
        xcm_version: r
            .try_get::<Option<i32>, _>("xcm_version")?
            .map(|n| n as u32),
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
        api_version: r
            .try_get::<Option<i32>, _>("api_version")?
            .map(|n| n as u32),
        metadata_version: r.try_get::<i32, _>("metadata_version")? as u32,
        sim_version: r.try_get::<i32, _>("sim_version")? as u32,
        raw_location: r.try_get("raw_location")?,
        baseline_input_hash: r.try_get("baseline_input_hash")?,
        overrides: r.try_get("overrides")?,
        override_hash: r.try_get("override_hash")?,
        storage_diff: r.try_get("storage_diff")?,
        storage_diff_count: r
            .try_get::<Option<i32>, _>("storage_diff_count")?
            .map(|n| n as u32),
        diff_status: r.try_get("diff_status")?,
        built_block_hash: r.try_get("built_block_hash")?,
        harness: r.try_get("harness")?,
        dispatch_route: r.try_get("dispatch_route")?,
        agenda_anchor: r.try_get("agenda_anchor")?,
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
             baseline_input_hash, overrides, override_hash, storage_diff, storage_diff_count, \
             diff_status, built_block_hash, harness, dispatch_route, agenda_anchor) \
         values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,\
                 $23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34) \
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
    .bind(r.xcm_version.map(|n| n as i32))
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
    .bind(r.api_version.map(|n| n as i32))
    .bind(r.metadata_version as i32)
    .bind(r.sim_version as i32)
    .bind(&r.raw_location)
    .bind(&r.baseline_input_hash)
    .bind(&r.overrides)
    .bind(&r.override_hash)
    .bind(&r.storage_diff)
    .bind(r.storage_diff_count.map(|n| n as i32))
    .bind(&r.diff_status)
    .bind(&r.built_block_hash)
    .bind(&r.harness)
    .bind(&r.dispatch_route)
    .bind(&r.agenda_anchor)
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

// ============================================================================
// THE TIER 2 QUEUE (Phase 3, slice 8)
// ============================================================================

pub struct PgJobStore {
    pool: PgPool,
}

impl PgJobStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// A constant lock id, taken for the DURATION OF A CLAIM.
///
/// WHY A LOCK AND NOT JUST `for update skip locked`: the concurrency cap is a
/// COUNT of running jobs and the claim is an UPDATE, and under READ COMMITTED a
/// second worker's count does not see a first worker's uncommitted claim — so
/// two workers can both read `running = 0`, both claim, and the cap of one is
/// quietly a cap of two. `skip locked` prevents them taking the SAME row; it does
/// not make count-then-claim atomic. Serialising the claim does, and a claim is
/// rare enough (a Tier 2 job is a Node process) that serialising it costs
/// nothing. Same device `PgBlockIndex::insert` and `xcm_links_pg` already use,
/// for the same class of reason.
const JOB_CLAIM_LOCK: i64 = 0x646f_746c_5f73_696d; // "dotl_sim"

fn job_from_row(r: &sqlx::postgres::PgRow) -> Result<SimJob> {
    let overrides: serde_json::Value = r.try_get("overrides")?;
    Ok(SimJob {
        id: r.try_get("id")?,
        chain_id: r.try_get("chain_id")?,
        tier: r.try_get("tier")?,
        at_height: r.try_get::<Option<i64>, _>("at_height")?.map(|h| h as u64),
        call: r.try_get("call_bytes")?,
        call_hash: r.try_get("call_hash")?,
        origin_spec: r.try_get("origin_spec")?,
        override_specs: overrides
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default(),
        requested_by: r.try_get("requested_by")?,
        note: r.try_get("note")?,
        status: r.try_get("status")?,
        attempts: r.try_get::<i32, _>("attempts")? as u32,
        max_attempts: r.try_get::<i32, _>("max_attempts")? as u32,
        signer: r
            .try_get::<Option<Vec<u8>>, _>("signer")?
            .and_then(|b| <[u8; 32]>::try_from(b.as_slice()).ok()),
        error: r.try_get("error")?,
        result_at_block_hash: r.try_get("result_at_block_hash")?,
        result_input_hash: r.try_get("result_input_hash")?,
    })
}

const JOB_COLUMNS: &str = "id, chain_id, tier, at_height, call_bytes, call_hash, origin_spec, \
     overrides, requested_by, note, status, attempts, max_attempts, signer, error, \
     result_at_block_hash, result_input_hash, created_at, started_at, finished_at";

/// Jobs for one chain, newest first — the read side of the queue.
pub async fn list_jobs(
    pool: &PgPool,
    chain_id: &str,
    status: Option<&str>,
    limit: i64,
) -> Result<Vec<SimJob>> {
    let rows = sqlx::query(&format!(
        "select {JOB_COLUMNS} from sim.simulation_jobs \
         where chain_id = $1 and ($2::text is null or status = $2) \
         order by id desc limit $3"
    ))
    .bind(chain_id)
    .bind(status)
    .bind(limit)
    .fetch_all(pool)
    .await
    .context("listing simulation jobs")?;
    rows.iter().map(job_from_row).collect()
}

#[async_trait]
impl JobStore for PgJobStore {
    async fn enqueue(&self, job: &NewSimJob) -> Result<i64, SimError> {
        let specs = serde_json::Value::Array(
            job.override_specs
                .iter()
                .map(|s| serde_json::Value::String(s.clone()))
                .collect(),
        );
        let row = sqlx::query(
            "insert into sim.simulation_jobs \
                 (chain_id, tier, at_height, call_bytes, call_hash, origin_spec, overrides, \
                  requested_by, note, status, max_attempts, signer) \
             values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) returning id",
        )
        .bind(&job.chain_id)
        .bind(&job.tier)
        .bind(job.at_height.map(|h| h as i64))
        .bind(&job.call)
        .bind(&job.call_hash)
        .bind(&job.origin_spec)
        .bind(&specs)
        .bind(&job.requested_by)
        .bind(&job.note)
        .bind(sim::JOB_QUEUED)
        .bind(job.max_attempts as i32)
        .bind(job.signer.map(|s| s.to_vec()))
        .fetch_one(&self.pool)
        .await
        .map_err(|e| SimError::Store(e.to_string()))?;
        row.try_get::<i64, _>("id")
            .map_err(|e| SimError::Store(e.to_string()))
    }

    async fn claim(
        &self,
        worker: &str,
        lease_secs: u32,
        max_concurrent: u32,
        only: Option<i64>,
    ) -> Result<Option<SimJob>, SimError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| SimError::Store(e.to_string()))?;
        sqlx::query("select pg_advisory_xact_lock($1)")
            .bind(JOB_CLAIM_LOCK)
            .execute(&mut *tx)
            .await
            .map_err(|e| SimError::Store(e.to_string()))?;

        // A `running` row whose lease has EXPIRED is reclaimable — otherwise a
        // worker that died holding one wedges the queue forever, and `running`
        // becomes a terminal state nobody chose. It is picked up by the same
        // statement rather than by a separate sweep, so there is one definition
        // of "a job that is available".
        //
        // THE ATTEMPT BUDGET IS CHECKED ONLY ON THE `queued` BRANCH, and that is
        // load-bearing rather than a simplification. `claim` INCREMENTS
        // `attempts`, so a job claimed once with the default `max_attempts = 1`
        // already sits at the limit — requiring `attempts < max_attempts` on the
        // reclaim branch too would make an expired lease unreclaimable, which is
        // precisely the wedge the lease exists to prevent. A reclaimed job is
        // then re-run once more and, if it fails again, `fail` makes it terminal
        // because its attempts really are spent.
        let row = sqlx::query(&format!(
            "update sim.simulation_jobs set \
                 status = $1, worker = $2, attempts = attempts + 1, \
                 started_at = coalesce(started_at, now()), \
                 lease_expires_at = now() + make_interval(secs => $3::double precision) \
             where id = ( \
                 select j.id from sim.simulation_jobs j \
                 where (( j.status = $4 and j.attempts < j.max_attempts ) \
                       or ( j.status = $1 and j.lease_expires_at < now() )) \
                   and ($6::bigint is null or j.id = $6) \
                   and ( select count(*) from sim.simulation_jobs r \
                         where r.status = $1 and r.lease_expires_at > now() ) < $5 \
                 order by j.created_at, j.id \
                 for update skip locked limit 1 ) \
             returning {JOB_COLUMNS}"
        ))
        .bind(sim::JOB_RUNNING)
        .bind(worker)
        .bind(lease_secs as f64)
        .bind(sim::JOB_QUEUED)
        .bind(max_concurrent as i64)
        .bind(only)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| SimError::Store(e.to_string()))?;

        let out = match row.as_ref() {
            None => None,
            Some(r) => Some(job_from_row(r).map_err(|e| SimError::Store(e.to_string()))?),
        };
        tx.commit()
            .await
            .map_err(|e| SimError::Store(e.to_string()))?;
        Ok(out)
    }

    async fn complete(
        &self,
        id: i64,
        at_block_hash: &str,
        input_hash: &str,
    ) -> Result<(), SimError> {
        sqlx::query(
            "update sim.simulation_jobs set status = $1, finished_at = now(), \
                 lease_expires_at = null, error = null, \
                 result_at_block_hash = $2, result_input_hash = $3 \
             where id = $4 and status = $5",
        )
        .bind(sim::JOB_DONE)
        .bind(at_block_hash)
        .bind(input_hash)
        .bind(id)
        // FENCED ON `running`: a worker whose lease expired and whose job was
        // reclaimed must not be able to stamp a terminal state onto somebody
        // else's run. It finishes, writes nothing, and the reclaiming worker's
        // answer is the one that stands.
        .bind(sim::JOB_RUNNING)
        .execute(&self.pool)
        .await
        .map_err(|e| SimError::Store(e.to_string()))?;
        Ok(())
    }

    async fn fail(&self, id: i64, error: &str, refused: bool) -> Result<(), SimError> {
        // A retryable failure goes back to `queued` ONLY while attempts remain;
        // otherwise it is terminal. Deciding that here rather than in the worker
        // keeps "how many times may this run" a property of the row instead of
        // of whichever process happened to pick it up.
        sqlx::query(
            "update sim.simulation_jobs set \
                 status = case when $2 then $3 \
                               when attempts < max_attempts then $4 \
                               else $5 end, \
                 error = $6, lease_expires_at = null, \
                 finished_at = case when $2 or attempts >= max_attempts then now() else null end \
             where id = $1 and status = $7",
        )
        .bind(id)
        .bind(refused)
        .bind(sim::JOB_REFUSED)
        .bind(sim::JOB_QUEUED)
        .bind(sim::JOB_FAILED)
        .bind(error)
        .bind(sim::JOB_RUNNING)
        .execute(&self.pool)
        .await
        .map_err(|e| SimError::Store(e.to_string()))?;
        Ok(())
    }

    async fn get(&self, id: i64) -> Result<Option<SimJob>, SimError> {
        let row = sqlx::query(&format!(
            "select {JOB_COLUMNS} from sim.simulation_jobs where id = $1"
        ))
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| SimError::Store(e.to_string()))?;
        match row.as_ref() {
            None => Ok(None),
            Some(r) => job_from_row(r)
                .map(Some)
                .map_err(|e| SimError::Store(e.to_string())),
        }
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
