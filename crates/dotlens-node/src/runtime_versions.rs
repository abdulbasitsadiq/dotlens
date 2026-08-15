//! Pg sink for runtime-version lineage → `substrate.runtime_versions`
//! (migration 0002). Node-level wiring, like registry_sync: the generic worker
//! only knows the `RuntimeVersionSink` trait.
//!
//! FK note: `substrate.runtime_versions.chain_id` references `core.chains`,
//! so registry sync MUST run before any live worker starts (main.rs enforces
//! this ordering).

use async_trait::async_trait;
use ingest::live::{RuntimeContextRecord, RuntimeVersionSink, SinkError};
use sqlx::PgPool;

pub struct PgRuntimeVersionSink {
    pool: PgPool,
}

impl PgRuntimeVersionSink {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl RuntimeVersionSink for PgRuntimeVersionSink {
    async fn record(&self, rec: &RuntimeContextRecord) -> Result<(), SinkError> {
        // idempotent: first_block only ever moves DOWN (earliest observation);
        // metadata fields fill in once and stick (coalesce keeps existing).
        sqlx::query(
            "insert into substrate.runtime_versions \
                 (chain_id, spec_version, transaction_version, metadata_version, \
                  metadata_blob_location, first_block) \
             values ($1, $2, $3, $4, $5, $6) \
             on conflict (chain_id, spec_version) do update set \
                 first_block = least( \
                     coalesce(substrate.runtime_versions.first_block, excluded.first_block), \
                     excluded.first_block), \
                 transaction_version = coalesce( \
                     substrate.runtime_versions.transaction_version, excluded.transaction_version), \
                 metadata_version = coalesce( \
                     substrate.runtime_versions.metadata_version, excluded.metadata_version), \
                 metadata_blob_location = coalesce( \
                     substrate.runtime_versions.metadata_blob_location, excluded.metadata_blob_location)",
        )
        .bind(&rec.chain_id)
        .bind(rec.runtime_version as i64)
        .bind(rec.transaction_version.map(|v| v as i64))
        .bind(rec.metadata_version.map(|v| v as i32))
        .bind(&rec.metadata_location)
        .bind(rec.first_seen_block as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| SinkError(e.to_string()))?;
        Ok(())
    }
}
