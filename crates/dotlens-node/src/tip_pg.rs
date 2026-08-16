//! Postgres backend for the tip worker's unfinalized-row bookkeeping.
//! The replacement rule itself lives in api::pg::PgBlockIndex::insert;
//! this adds the reorg probe (which hash do we hold?) and the prune
//! (shortening reorgs / finalization passing by).

use async_trait::async_trait;
use ingest::live::SinkError;
use ingest::tip::UnfinalizedStore;
use sqlx::PgPool;

pub struct PgUnfinalizedStore {
    pool: PgPool,
}

impl PgUnfinalizedStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl UnfinalizedStore for PgUnfinalizedStore {
    async fn unfinalized_hash(
        &self,
        chain_id: &str,
        height: u64,
    ) -> Result<Option<String>, SinkError> {
        let row: Option<(String,)> = sqlx::query_as(
            "select hash from core.blocks \
             where chain_id = $1 and height = $2 and not finalized",
        )
        .bind(chain_id)
        .bind(height as i64)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| SinkError(e.to_string()))?;
        Ok(row.map(|(h,)| h))
    }

    async fn prune_unfinalized_above(
        &self,
        chain_id: &str,
        height: u64,
    ) -> Result<u64, SinkError> {
        let err = |e: sqlx::Error| SinkError(e.to_string());
        let mut tx = self.pool.begin().await.map_err(err)?;
        // children first (no FK, but never leave orphans even mid-crash)
        for table in ["events", "transactions"] {
            sqlx::query(&format!(
                "delete from core.{table} t using core.blocks b \
                 where b.chain_id = t.chain_id and b.height = t.block_height \
                   and b.chain_id = $1 and b.height > $2 and not b.finalized"
            ))
            .bind(chain_id)
            .bind(height as i64)
            .execute(&mut *tx)
            .await
            .map_err(err)?;
        }
        let res = sqlx::query(
            "delete from core.blocks \
             where chain_id = $1 and height > $2 and not finalized",
        )
        .bind(chain_id)
        .bind(height as i64)
        .execute(&mut *tx)
        .await
        .map_err(err)?;
        tx.commit().await.map_err(err)?;
        Ok(res.rows_affected())
    }
}
