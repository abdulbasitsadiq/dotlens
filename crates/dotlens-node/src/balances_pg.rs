//! Postgres backends for the balances worker: canonical events out of
//! `core.blocks/events`, deltas into `balances.balance_changes`
//! (insert-ignore — re-mapping any range is a no-op), anchors into
//! `balances.balance_anchors`.
//!
//! NUMERIC values are bound as text and cast server-side (`$n::numeric`) —
//! plancks exceed u64 and we deliberately carry no float/decimal dep.

use async_trait::async_trait;
use canonical::CanonicalEvent;
use ingest::balances::{BalanceDelta, BlockEvents, DeltaSink, EventSource};
use sqlx::PgPool;

pub struct PgEventSource {
    pool: PgPool,
}

impl PgEventSource {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl EventSource for PgEventSource {
    async fn decoded_events(
        &self,
        chain_id: &str,
        height: u64,
    ) -> Result<Option<BlockEvents>, String> {
        let head: Option<(i64,)> = sqlx::query_as(
            "select runtime_version from core.blocks where chain_id = $1 and height = $2",
        )
        .bind(chain_id)
        .bind(height as i64)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        let Some((runtime_version,)) = head else {
            return Ok(None); // not decoded (yet) — worker skips
        };
        let rows: Vec<(i32, Option<i32>, String, serde_json::Value)> = sqlx::query_as(
            "select event_index, tx_index, name, data from core.events \
             where chain_id = $1 and block_height = $2 order by event_index",
        )
        .bind(chain_id)
        .bind(height as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(Some(BlockEvents {
            runtime_version: runtime_version as u32,
            events: rows
                .into_iter()
                .map(|(index, tx, name, data)| CanonicalEvent {
                    index: index as u32,
                    transaction_index: tx.map(|t| t as u32),
                    name,
                    data,
                })
                .collect(),
        }))
    }
}

fn signed_text(d: &BalanceDelta) -> String {
    if d.negative {
        format!("-{}", d.magnitude)
    } else {
        d.magnitude.to_string()
    }
}

pub struct PgDeltaSink {
    pool: PgPool,
}

impl PgDeltaSink {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl DeltaSink for PgDeltaSink {
    async fn write(
        &self,
        chain_id: &str,
        height: u64,
        runtime_version: u32,
        mapper_version: u32,
        rows: &[(u32, BalanceDelta)],
    ) -> Result<(), String> {
        let mut tx = self.pool.begin().await.map_err(|e| e.to_string())?;
        for (event_index, d) in rows {
            sqlx::query(
                "insert into balances.balance_changes \
                     (chain_id, account_id, asset, block_height, event_index, \
                      delta, reason, counterparty, runtime_version, mapper_version) \
                 values ($1, $2, $3, $4, $5, $6::numeric, $7, $8, $9, $10) \
                 on conflict (chain_id, block_height, event_index, account_id, asset) do nothing",
            )
            .bind(chain_id)
            .bind(&d.account)
            .bind(&d.asset)
            .bind(height as i64)
            .bind(*event_index as i32)
            .bind(signed_text(d))
            .bind(&d.reason)
            .bind(&d.counterparty)
            .bind(runtime_version as i64)
            .bind(mapper_version as i32)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        }
        tx.commit().await.map_err(|e| e.to_string())
    }
}

/// Record one absolute-balance anchor (state read at END of `height`).
/// Anchors are immutable observations: conflicts are ignored, never updated.
#[allow(clippy::too_many_arguments)]
pub async fn insert_anchor(
    pool: &PgPool,
    chain_id: &str,
    account_id: &[u8],
    asset: &str,
    height: u64,
    balances: &adapter_substrate::balances::AccountBalances,
    spec_version: Option<u32>,
    source: &str,
    note: Option<&str>,
) -> anyhow::Result<()> {
    sqlx::query(
        "insert into balances.balance_anchors \
             (chain_id, account_id, asset, block_height, free, reserved, frozen, \
              total, spec_version, source, note) \
         values ($1, $2, $3, $4, $5::numeric, $6::numeric, $7::numeric, \
                 $8::numeric, $9, $10, $11) \
         on conflict (chain_id, account_id, asset, block_height) do nothing",
    )
    .bind(chain_id)
    .bind(account_id)
    .bind(asset)
    .bind(height as i64)
    .bind(balances.free.to_string())
    .bind(balances.reserved.to_string())
    .bind(balances.frozen.map(|f| f.to_string()))
    .bind(balances.total().to_string())
    .bind(spec_version.map(|s| s as i64))
    .bind(source)
    .bind(note)
    .execute(pool)
    .await?;
    Ok(())
}
