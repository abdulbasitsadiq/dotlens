//! Postgres sink for XCM message facts (Phase 3, slice 2).
//!
//! Append-only, insert-ignore, one transaction per block. There is no
//! projection to converge and no ordering guard to get right: a row is what one
//! event on one chain said, and re-mapping a range must be a no-op.

use anyhow::{Context, Result};
use async_trait::async_trait;
use ingest::xcm::{XcmFact, XcmSink};
use sqlx::PgPool;

pub struct PgXcmSink {
    pool: PgPool,
}

impl PgXcmSink {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl XcmSink for PgXcmSink {
    async fn write(
        &self,
        chain_id: &str,
        height: u64,
        runtime_version: u32,
        mapper_version: u32,
        rows: &[(u32, XcmFact)],
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
        .map_err(|e| e.to_string())
    }
}

pub async fn write_facts(
    pool: &PgPool,
    chain_id: &str,
    height: u64,
    runtime_version: u32,
    mapper_version: u32,
    rows: &[(u32, XcmFact)],
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
            "two XCM facts at {chain_id}/{height} event {index} — the fact table is keyed by \
             event index and the second would be silently dropped"
        );
    }

    let mut tx = pool.begin().await.context("begin xcm tx")?;
    for (event_index, f) in rows {
        sqlx::query(
            "insert into xcm.messages ( \
                 chain_id, block_height, event_index, side, transport, message_id, id_kind, \
                 counterparty, origin_location, destination, message, forwarded, status, \
                 success, error, weight_used, data, runtime_version, mapper_version) \
             values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19) \
             on conflict (chain_id, block_height, event_index) do nothing",
        )
        .bind(chain_id)
        .bind(height as i64)
        .bind(*event_index as i32)
        .bind(&f.side)
        .bind(&f.transport)
        .bind(&f.message_id)
        .bind(&f.id_kind)
        .bind(&f.counterparty)
        .bind(&f.origin_location)
        .bind(&f.destination)
        .bind(&f.message)
        .bind(f.forwarded)
        .bind(&f.status)
        .bind(f.success)
        .bind(&f.error)
        .bind(&f.weight_used)
        .bind(&f.data)
        .bind(runtime_version as i64)
        .bind(mapper_version as i32)
        .execute(&mut *tx)
        .await
        .with_context(|| format!("inserting xcm fact {chain_id}/{height}/{event_index}"))?;
    }
    tx.commit().await.context("commit xcm tx")?;
    Ok(())
}
