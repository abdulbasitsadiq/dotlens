//! Postgres sink for broker entitlement facts, and the entitlement
//! denominator's writer (Phase 3, slice 13).
//!
//! Append-only, insert-ignore, one transaction per block — the same shape as
//! `coretime_pg` and `xcm_pg`. There is no projection to converge and no
//! ordering guard: a row is what one `Broker` event on one Coretime block said.
//!
//! ---------------------------------------------------------------------------
//! ONE EVENT, TWO TABLES, ONE TRANSACTION.
//!
//! `CoreAssigned` is the only variant that expands, and it writes a
//! `broker_events` row AND N `core_assignments` rows. Both go in the same
//! transaction because a seam row whose announcing event is missing would be an
//! entitlement with no provenance, and an event row whose seam rows are missing
//! would silently remove a core from the delta. Neither half is useful alone, so
//! neither is committed alone.
//!
//! ---------------------------------------------------------------------------
//! WHY THERE IS NO MONOTONE FILL HERE, unlike the occupancy sink one file over.
//!
//! `coretime_pg` fills `relay_parent_height` on replay because the mapper hands
//! it a HASH and the height depends on how much of the chain is indexed — so the
//! same row genuinely improves as the index grows. Nothing in this half has that
//! shape: `relay_block` is stated by the chain inside the event itself
//! (`CoreAssigned.when`), so a row written today and a row written after a wider
//! backfill are byte-identical. `do nothing` is therefore the correct conflict
//! action and a replay is a genuine no-op rather than a row-version churn.

use anyhow::{Context, Result};
use async_trait::async_trait;
use ingest::broker::{BrokerRow, BrokerSink};
use sqlx::PgPool;

pub struct PgBrokerSink {
    pool: PgPool,
}

impl PgBrokerSink {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl BrokerSink for PgBrokerSink {
    async fn write(
        &self,
        chain_id: &str,
        height: u64,
        runtime_version: u32,
        mapper_version: u32,
        rows: &[(u32, BrokerRow)],
    ) -> Result<(), String> {
        write_facts(
            &self.pool,
            chain_id,
            height,
            runtime_version,
            mapper_version,
            rows,
        )
        .await
        .map_err(|e| format!("{e:#}"))
    }
}

pub async fn write_facts(
    pool: &PgPool,
    chain_id: &str,
    height: u64,
    runtime_version: u32,
    mapper_version: u32,
    rows: &[(u32, BrokerRow)],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    // One event index cannot hold two facts — `broker_events` is keyed by it, so
    // insert-ignore would silently keep the first and drop the second. Refuse
    // instead (the rule the votes sink established in slice 3).
    let mut seen = std::collections::HashSet::new();
    for (index, _) in rows {
        anyhow::ensure!(
            seen.insert(*index),
            "two broker facts at {chain_id}/{height} event {index} — the fact table is keyed by \
             event index and the second would be silently dropped"
        );
    }

    let mut tx = pool.begin().await.context("begin broker tx")?;
    for (event_index, r) in rows {
        sqlx::query(
            "insert into coretime.broker_events \
                 (chain_id, block_height, event_index, variant, core_index, task_id, data, \
                  runtime_version, mapper_version) \
             values ($1,$2,$3,$4,$5,$6,$7,$8,$9) \
             on conflict (chain_id, block_height, event_index) do nothing",
        )
        .bind(chain_id)
        .bind(height as i64)
        .bind(*event_index as i32)
        .bind(&r.variant)
        .bind(r.core_index.map(|c| c as i32))
        .bind(r.task_id.map(|t| t as i32))
        .bind(&r.data)
        .bind(runtime_version as i64)
        .bind(mapper_version as i32)
        .execute(&mut *tx)
        .await
        .with_context(|| {
            format!("inserting broker event {chain_id}/{height}/{event_index} ({})", r.variant)
        })?;

        for a in &r.assignments {
            sqlx::query(
                "insert into coretime.core_assignments \
                     (chain_id, block_height, event_index, assignment_index, core_index, \
                      relay_block, assignment_kind, task_id, parts, runtime_version, \
                      mapper_version) \
                 values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) \
                 on conflict (chain_id, block_height, event_index, assignment_index) do nothing",
            )
            .bind(chain_id)
            .bind(height as i64)
            .bind(*event_index as i32)
            .bind(a.assignment_index as i32)
            .bind(a.core_index as i32)
            .bind(a.relay_block as i64)
            .bind(&a.kind)
            .bind(a.task_id.map(|t| t as i32))
            .bind(a.parts as i32)
            .bind(runtime_version as i64)
            .bind(mapper_version as i32)
            .execute(&mut *tx)
            .await
            .map_err(|e| assignment_refused(chain_id, height, *event_index, a, e))?;
        }
    }
    tx.commit().await.context("commit broker tx")?;
    Ok(())
}

/// Name the constraint when the database refuses an assignment row.
///
/// 0025 carries `core_assignments_task_names_its_para`:
/// `(assignment_kind = 'task') = (task_id is not null)`. A bare
/// `new row violates check constraint` says nothing about what it means, and
/// what it means here is specific — the mapper read a `CoreAssignment` variant
/// and its payload inconsistently, so a core would be recorded as running a task
/// nobody can name, or as idle while naming one. Either way the delta would
/// attribute occupancy to the wrong side.
///
/// MATCHED ON SQLSTATE `23514` rather than on the constraint name, for the same
/// reason `coretime_pg::invariant_violation` matches `23505`: the table is
/// PARTITIONED, and a check constraint is enforced by the CHILD, so the name
/// that comes back is the partition's. The real name is printed rather than
/// assumed.
fn assignment_refused(
    chain_id: &str,
    height: u64,
    event_index: u32,
    a: &ingest::broker::CoreAssignmentRow,
    e: sqlx::Error,
) -> anyhow::Error {
    if let sqlx::Error::Database(db) = &e {
        if db.code().as_deref() == Some("23514") {
            let constraint = db.constraint().unwrap_or("<unnamed>").to_string();
            return anyhow::anyhow!(
                "ASSIGNMENT KIND AND TASK DISAGREE at {chain_id}/{height} event {event_index} \
                 assignment {} — kind `{}` with task_id {:?} on core {}. A `task` assignment must \
                 name its para and a `pool` or `idle` one must not pretend to, because the delta \
                 attributes occupancy by task: a task row with no task is an entitlement nobody \
                 can be credited with, and a pool row carrying one would credit the wrong chain. \
                 Refused by `{constraint}`. Underlying: {e}",
                a.assignment_index,
                a.kind,
                a.task_id,
                a.core_index
            );
        }
    }
    anyhow::Error::new(e).context(format!(
        "inserting core assignment {chain_id}/{height}/{event_index}/{}",
        a.assignment_index
    ))
}

// ------------------------------------------------- the entitlement denominator

/// One reading of `Broker.Status` + `Broker.Configuration`, with the block it was
/// read at.
///
/// IMMUTABLE PER (chain, height), like `coretime.core_config` and
/// `balances.balance_anchors`: two readings at one block are the same
/// observation, and a re-run must not rewrite history. `do nothing` for exactly
/// that reason.
///
/// THIS IS THE FIFTH INSTANCE of the state-with-no-event pattern and it is
/// hand-rolled deliberately — see 0025's header for why the shared mechanism is
/// deferred again rather than built on the count going up. (`xcm.channels`,
/// owed since slice 2, would be the sixth and is the only one that needs its
/// readings DIFFED rather than merely recorded.)
/// `first_core` and `sale_info` were added by migration 0026 (slice 14), which
/// is also the slice that first READS `first_core` — cores below it are reserved
/// system cores, and without it the delta cannot say whether idle entitlement is
/// market-side. Both are `Option` because `Broker.SaleInfo` is an `OptionQuery`
/// StorageValue that is genuinely absent before the first sale, and because rows
/// written by slice 13 predate the columns entirely.
#[allow(clippy::too_many_arguments)]
pub async fn insert_broker_config(
    pool: &PgPool,
    chain_id: &str,
    block_height: u64,
    core_count: u32,
    status: &serde_json::Value,
    configuration: &serde_json::Value,
    first_core: Option<u32>,
    sale_info: Option<&serde_json::Value>,
    runtime_version: u32,
) -> Result<()> {
    sqlx::query(
        "insert into coretime.broker_config \
             (chain_id, block_height, core_count, status, configuration, first_core, sale_info, \
              runtime_version) \
         values ($1,$2,$3,$4,$5,$6,$7,$8) \
         on conflict (chain_id, block_height) do nothing",
    )
    .bind(chain_id)
    .bind(block_height as i64)
    .bind(core_count as i32)
    .bind(status)
    .bind(configuration)
    .bind(first_core.map(|c| c as i32))
    .bind(sale_info)
    .bind(runtime_version as i64)
    .execute(pool)
    .await
    .with_context(|| format!("recording broker config {chain_id}@{block_height}"))?;
    Ok(())
}
