//! Postgres sink for core occupancy facts, and the denominator's writer
//! (Phase 3, slice 11).
//!
//! Append-only, insert-ignore, one transaction per block — the same shape as
//! `xcm_pg`. There is no projection to converge and no ordering guard: a row is
//! what one candidate event on one relay block said.
//!
//! ---------------------------------------------------------------------------
//! THE RELAY-PARENT RESOLUTION LIVES HERE, AND THAT IS A DELIBERATE PLACEMENT
//! RATHER THAN A CONVENIENCE.
//!
//! `adapter_substrate::coretime` is a PURE mapper: an event in, facts out, no
//! index to consult. But the descriptor states its relay parent as a HASH, and
//! the useful number is a HEIGHT — the async-backing lag is `block_height −
//! relay_parent_height`, measured at min 2, avg 3.261, max 6, never 0 or 1 and
//! never above 6. Turning one into the other needs `core.blocks`, so it is the
//! sink's job.
//!
//! AND THE NULL MEANS SOMETHING PRECISE. A relay parent 2–6 blocks back falls
//! outside the indexed window at EVERY window edge, so the lag is computable
//! only inside a contiguously-indexed range: 50,221 of 51,998 resolved (96.6%)
//! in the prep sample, and the 1,777 that did not are edges and always will be.
//! A NULL here therefore reads "the parent is outside our data", never "the lag
//! is zero" — which is why `relay_parent_hash` is stored BESIDE the height
//! rather than replaced by it. The hash is what a later, wider backfill would
//! resolve; a row that had thrown it away could never be improved.
//!
//! AND "WOULD RESOLVE" IS A PROMISE THE INSERT HAS TO KEEP. A plain
//! `do nothing` would have made it a lie: `coretime-range` is the only writer,
//! so once a row was written with a NULL it would keep that NULL forever, and
//! the column's meaning would drift from "outside the window" to "outside the
//! window WHEN WE FIRST LOOKED" without anything saying so. The conflict action
//! is therefore a MONOTONE FILL — it writes the height only where there is none
//! and one is now available — so re-running a range after a wider backfill
//! resolves the edges and can never rewrite a height already resolved. The
//! `where` clause is what keeps an ordinary replay a genuine no-op rather than
//! a row-version churn.
//!
//! ---------------------------------------------------------------------------
//! THE MEASURED INVARIANT IS ENFORCED BY THE DATABASE AND NAMED BY THIS FILE.
//!
//! 0024 carries a PARTIAL UNIQUE INDEX on `(chain_id, block_height, core_index)
//! where kind = 'included'` — zero collisions across 51,998 included candidates.
//! The insert deliberately arbitrates on the PRIMARY KEY only, so a replay is a
//! no-op while a genuine second inclusion on one core in one block RAISES. That
//! is the intended outcome: it would mean our reading of the runtime is wrong,
//! and an occupancy figure that double-counts a core is worse than an ingestion
//! that stops. [`invariant_violation`] turns the constraint name into a sentence
//! that says which invariant broke, rather than leaving a bare Postgres error
//! for somebody to look up.

use anyhow::{Context, Result};
use async_trait::async_trait;
use ingest::coretime::{OccupancyRow, OccupancySink};
use sqlx::PgPool;
use std::collections::HashMap;

pub struct PgOccupancySink {
    pool: PgPool,
}

impl PgOccupancySink {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl OccupancySink for PgOccupancySink {
    async fn write(
        &self,
        chain_id: &str,
        height: u64,
        runtime_version: u32,
        mapper_version: u32,
        rows: &[(u32, OccupancyRow)],
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
    rows: &[(u32, OccupancyRow)],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    // One event index cannot hold two facts — the table is keyed by it, so
    // insert-ignore would silently keep the first and drop the second. Refuse
    // instead (the rule the votes sink established in slice 3).
    let mut seen = std::collections::HashSet::new();
    for (index, _) in rows {
        anyhow::ensure!(
            seen.insert(*index),
            "two occupancy facts at {chain_id}/{height} event {index} — the fact table is keyed \
             by event index and the second would be silently dropped"
        );
    }

    let parents = resolve_relay_parents(pool, chain_id, rows).await?;

    let mut tx = pool.begin().await.context("begin coretime tx")?;
    for (event_index, r) in rows {
        let parent_height = r
            .relay_parent_hash
            .as_deref()
            .and_then(|h| parents.get(h).copied());
        sqlx::query(
            "insert into coretime.core_occupancy as o ( \
                 chain_id, block_height, event_index, kind, core_index, para_id, group_index, \
                 relay_parent_hash, relay_parent_height, pov_hash, runtime_version, \
                 mapper_version) \
             values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) \
             on conflict (chain_id, block_height, event_index) do update \
                 set relay_parent_height = excluded.relay_parent_height \
                 where o.relay_parent_height is null \
                   and excluded.relay_parent_height is not null",
        )
        .bind(chain_id)
        .bind(height as i64)
        .bind(*event_index as i32)
        .bind(&r.kind)
        .bind(r.core_index as i32)
        .bind(r.para_id as i32)
        .bind(r.group_index.map(|g| g as i32))
        .bind(&r.relay_parent_hash)
        .bind(parent_height.map(|h| h as i64))
        .bind(&r.pov_hash)
        .bind(runtime_version as i64)
        .bind(mapper_version as i32)
        .execute(&mut *tx)
        .await
        .map_err(|e| invariant_violation(chain_id, height, *event_index, r, e))?;
    }
    tx.commit().await.context("commit coretime tx")?;
    Ok(())
}

/// Every distinct relay parent in one block's rows, resolved to a height in ONE
/// query.
///
/// A relay block carries up to ~45 candidates but only a handful of distinct
/// relay parents (they cluster inside the 2–6 block lag window), so this is a
/// small `= any($2)` probe on `blocks_hash_idx` rather than one lookup per row.
/// It is scoped to `chain_id` because a hash is unique per chain and joining
/// across chains would resolve a relay parent against a parachain's block.
async fn resolve_relay_parents(
    pool: &PgPool,
    chain_id: &str,
    rows: &[(u32, OccupancyRow)],
) -> Result<HashMap<String, u64>> {
    let mut hashes: Vec<String> = rows
        .iter()
        .filter_map(|(_, r)| r.relay_parent_hash.clone())
        .collect();
    hashes.sort();
    hashes.dedup();
    if hashes.is_empty() {
        return Ok(HashMap::new());
    }
    let found: Vec<(String, i64)> = sqlx::query_as(
        "select hash, height from core.blocks where chain_id = $1 and hash = any($2)",
    )
    .bind(chain_id)
    .bind(&hashes[..])
    .fetch_all(pool)
    .await
    .context("resolving relay parents against core.blocks")?;
    Ok(found.into_iter().map(|(h, n)| (h, n as u64)).collect())
}

/// Name the invariant when the database refuses a row.
///
/// A bare `duplicate key value violates unique constraint "..."` is technically
/// complete and tells an operator nothing about what it means. This says what
/// was measured, what broke it, and why stopping is the right outcome.
///
/// MATCHED ON SQLSTATE, NOT ON THE INDEX NAME, AND THAT IS THE WHOLE TRAP.
///
/// `coretime.core_occupancy` is PARTITIONED, and Postgres clones a parent index
/// onto each partition under a GENERATED name (`DefineIndex` clears `idxname`
/// before recursing, because two relations in one schema cannot share a name).
/// The violation is raised by the CHILD btree, so `constraint()` comes back as
/// `core_occupancy_p_polkadot_chain_id_block_height_core_index_idx` and a
/// comparison against the parent's name would never once match — the loud
/// message would be dead code and the operator would get a bare Postgres error
/// after all. This will recur on every partitioned table this project ever
/// matches a constraint on.
///
/// `23505` is safe to branch on here because the PRIMARY KEY is arbitrated away
/// by the `on conflict` clause and 0024 declares no other unique index, so a
/// unique violation reaching this statement can only be the partial one. The
/// index's real name is printed rather than assumed.
fn invariant_violation(
    chain_id: &str,
    height: u64,
    event_index: u32,
    row: &OccupancyRow,
    e: sqlx::Error,
) -> anyhow::Error {
    // `code()` and `constraint()` are trait methods, but `db` is a `dyn
    // DatabaseError` — the trait object resolves them through its own vtable, so
    // no `use` of the trait is needed (importing it warns as unused).
    if let sqlx::Error::Database(db) = &e {
        if db.code().as_deref() == Some("23505") {
            let index = db.constraint().unwrap_or("<unnamed>").to_string();
            return anyhow::anyhow!(
                "TWO INCLUDED CANDIDATES ON CORE {} AT {chain_id}/{height} (event {event_index}, \
                 para {}). One candidate per core per block was MEASURED across 51,998 inclusions \
                 with zero collisions and 0024 enforces it, so this is not a duplicate to skip — \
                 it means our reading of the runtime's core assignment is wrong, and an occupancy \
                 ratio computed from it would double-count a core. Ingestion stops here on \
                 purpose. Refused by index `{index}` (a partition's clone of \
                 core_occupancy_one_candidate_per_core_idx). Underlying: {e}",
                row.core_index,
                row.para_id
            );
        }
    }
    anyhow::Error::new(e).context(format!(
        "inserting occupancy fact {chain_id}/{height}/{event_index}"
    ))
}

// ------------------------------------------------------------- the denominator

/// One reading of `SchedulerParams`, with the block it was read at.
///
/// IMMUTABLE PER (chain, height), like `balances.balance_anchors`: two readings
/// at one block are the same observation, and a re-run must not rewrite history.
/// `do nothing` rather than `do update` for exactly that reason.
pub async fn insert_core_config(
    pool: &PgPool,
    chain_id: &str,
    block_height: u64,
    num_cores: u32,
    scheduler_params: &serde_json::Value,
    runtime_version: u32,
) -> Result<()> {
    sqlx::query(
        "insert into coretime.core_config \
             (chain_id, block_height, num_cores, scheduler_params, runtime_version) \
         values ($1,$2,$3,$4,$5) \
         on conflict (chain_id, block_height) do nothing",
    )
    .bind(chain_id)
    .bind(block_height as i64)
    .bind(num_cores as i32)
    .bind(scheduler_params)
    .bind(runtime_version as i64)
    .execute(pool)
    .await
    .with_context(|| format!("recording core config {chain_id}@{block_height}"))?;
    Ok(())
}
