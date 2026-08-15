//! REST API (Axum). Read-only endpoints over a block index plus the registry.
//! The `BlockIndex` contract is async: Postgres-backed in real runs (`pg`
//! feature), in-memory for DB-less runs and tests.

use async_trait::async_trait;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
    routing::get,
    Router,
};
use canonical::CanonicalBlock;
use chrono::{DateTime, Utc};
use registry::Registry;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Debug, thiserror::Error)]
#[error("block index error: {0}")]
pub struct IndexError(pub String);

/// Storage abstraction the API reads blocks from. Inserts are idempotent:
/// re-inserting an already-indexed (chain, height) is a no-op, never an error.
#[async_trait]
pub trait BlockIndex: Send + Sync {
    async fn get(&self, chain_id: &str, height: u64) -> Result<Option<CanonicalBlock>, IndexError>;
    async fn insert(&self, block: CanonicalBlock) -> Result<(), IndexError>;
    async fn count(&self) -> Result<u64, IndexError>;
}

#[derive(Default)]
pub struct MemoryBlockIndex {
    inner: RwLock<HashMap<(String, u64), CanonicalBlock>>,
}

impl MemoryBlockIndex {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl BlockIndex for MemoryBlockIndex {
    async fn get(
        &self,
        chain_id: &str,
        height: u64,
    ) -> Result<Option<CanonicalBlock>, IndexError> {
        let map = self.inner.read().map_err(|e| IndexError(e.to_string()))?;
        Ok(map.get(&(chain_id.to_string(), height)).cloned())
    }
    async fn insert(&self, block: CanonicalBlock) -> Result<(), IndexError> {
        let mut map = self.inner.write().map_err(|e| IndexError(e.to_string()))?;
        map.insert((block.chain_id.clone(), block.height), block);
        Ok(())
    }
    async fn count(&self) -> Result<u64, IndexError> {
        let map = self.inner.read().map_err(|e| IndexError(e.to_string()))?;
        Ok(map.len() as u64)
    }
}

// -------------------------------------------------------------------- pg impl

#[cfg(feature = "pg")]
pub mod pg {
    use super::{BlockIndex, IndexError};
    use async_trait::async_trait;
    use canonical::{CanonicalBlock, CanonicalEvent, CanonicalTransaction, Lineage};
    use chrono::{DateTime, Utc};
    use sqlx::PgPool;

    /// Postgres-backed block index over `core.blocks/transactions/events`.
    /// One transaction per block insert; every row carries lineage
    /// (Invariant 3). Conflicts are ignored: rows are immutable, re-ingesting
    /// an identical block is a no-op (ARCHITECTURE.md §15).
    pub struct PgBlockIndex {
        pool: PgPool,
    }

    impl PgBlockIndex {
        pub fn new(pool: PgPool) -> Self {
            Self { pool }
        }
    }

    #[async_trait]
    impl BlockIndex for PgBlockIndex {
        async fn get(
            &self,
            chain_id: &str,
            height: u64,
        ) -> Result<Option<CanonicalBlock>, IndexError> {
            let err = |e: sqlx::Error| IndexError(e.to_string());
            let head: Option<(String, String, Option<DateTime<Utc>>, bool, i64, i32, String)> =
                sqlx::query_as(
                    "select hash, parent_hash, timestamp, finalized, \
                            runtime_version, decoder_version, raw_location \
                     from core.blocks where chain_id = $1 and height = $2",
                )
                .bind(chain_id)
                .bind(height as i64)
                .fetch_optional(&self.pool)
                .await
                .map_err(err)?;
            let Some((hash, parent_hash, timestamp, finalized, runtime_version, decoder_version, raw_location)) =
                head
            else {
                return Ok(None);
            };

            let txs: Vec<(i32, Option<String>, Option<String>, String, serde_json::Value, bool)> =
                sqlx::query_as(
                    "select tx_index, hash, signer, call_name, args, success \
                     from core.transactions where chain_id = $1 and block_height = $2 \
                     order by tx_index",
                )
                .bind(chain_id)
                .bind(height as i64)
                .fetch_all(&self.pool)
                .await
                .map_err(err)?;

            let events: Vec<(i32, Option<i32>, String, serde_json::Value)> = sqlx::query_as(
                "select event_index, tx_index, name, data \
                 from core.events where chain_id = $1 and block_height = $2 \
                 order by event_index",
            )
            .bind(chain_id)
            .bind(height as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(err)?;

            Ok(Some(CanonicalBlock {
                chain_id: chain_id.to_string(),
                height,
                hash,
                parent_hash,
                timestamp,
                finalized,
                lineage: Lineage {
                    runtime_version: runtime_version as u32,
                    decoder_version: decoder_version as u32,
                    raw_location,
                },
                transactions: txs
                    .into_iter()
                    .map(|(index, hash, signer, call, args, success)| CanonicalTransaction {
                        index: index as u32,
                        hash,
                        signer,
                        call,
                        args,
                        success,
                    })
                    .collect(),
                events: events
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

        async fn insert(&self, block: CanonicalBlock) -> Result<(), IndexError> {
            let err = |e: sqlx::Error| IndexError(e.to_string());
            let mut tx = self.pool.begin().await.map_err(err)?;

            sqlx::query(
                "insert into core.blocks (chain_id, height, hash, parent_hash, timestamp, \
                     finalized, runtime_version, decoder_version, raw_location) \
                 values ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
                 on conflict (chain_id, height) do nothing",
            )
            .bind(&block.chain_id)
            .bind(block.height as i64)
            .bind(&block.hash)
            .bind(&block.parent_hash)
            .bind(block.timestamp)
            .bind(block.finalized)
            .bind(block.lineage.runtime_version as i64)
            .bind(block.lineage.decoder_version as i32)
            .bind(&block.lineage.raw_location)
            .execute(&mut *tx)
            .await
            .map_err(err)?;

            for t in &block.transactions {
                sqlx::query(
                    "insert into core.transactions (chain_id, block_height, tx_index, hash, \
                         signer, call_name, args, success, \
                         runtime_version, decoder_version, raw_location) \
                     values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) \
                     on conflict (chain_id, block_height, tx_index) do nothing",
                )
                .bind(&block.chain_id)
                .bind(block.height as i64)
                .bind(t.index as i32)
                .bind(&t.hash)
                .bind(&t.signer)
                .bind(&t.call)
                .bind(&t.args)
                .bind(t.success)
                .bind(block.lineage.runtime_version as i64)
                .bind(block.lineage.decoder_version as i32)
                .bind(&block.lineage.raw_location)
                .execute(&mut *tx)
                .await
                .map_err(err)?;
            }

            for e in &block.events {
                sqlx::query(
                    "insert into core.events (chain_id, block_height, event_index, tx_index, \
                         name, data, runtime_version, decoder_version) \
                     values ($1, $2, $3, $4, $5, $6, $7, $8) \
                     on conflict (chain_id, block_height, event_index) do nothing",
                )
                .bind(&block.chain_id)
                .bind(block.height as i64)
                .bind(e.index as i32)
                .bind(e.transaction_index.map(|t| t as i32))
                .bind(&e.name)
                .bind(&e.data)
                .bind(block.lineage.runtime_version as i64)
                .bind(block.lineage.decoder_version as i32)
                .execute(&mut *tx)
                .await
                .map_err(err)?;
            }

            tx.commit().await.map_err(err)
        }

        async fn count(&self) -> Result<u64, IndexError> {
            let (n,): (i64,) = sqlx::query_as("select count(*) from core.blocks")
                .fetch_one(&self.pool)
                .await
                .map_err(|e| IndexError(e.to_string()))?;
            Ok(n as u64)
        }
    }
}

#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<Registry>,
    pub blocks: Arc<dyn BlockIndex>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/chains", get(list_chains))
        .route("/v1/blocks/{chain}/{height}", get(get_block))
        .route("/v1/domains/{network}/{domain}", get(resolve_domain))
        .with_state(state)
}

async fn health() -> &'static str {
    "ok"
}

async fn list_chains(State(state): State<AppState>) -> Json<serde_json::Value> {
    let now = Utc::now();
    let chains: Vec<serde_json::Value> = state
        .registry
        .chains()
        .map(|c| {
            serde_json::json!({
                "id": c.id,
                "name": c.name,
                "family": c.family,
                "network": c.network,
                "para_id": c.para_id,
                "relay": c.relay,
                "status": c.status_at(now),
                "modules": c.modules,
            })
        })
        .collect();
    Json(serde_json::json!({ "chains": chains }))
}

async fn get_block(
    State(state): State<AppState>,
    Path((chain, height)): Path<(String, u64)>,
) -> Response {
    if state.registry.chain(&chain).is_none() {
        return error(StatusCode::NOT_FOUND, format!("unknown chain: {chain}"));
    }
    match state.blocks.get(&chain, height).await {
        Ok(Some(block)) => Json(block).into_response(),
        Ok(None) => error(
            StatusCode::NOT_FOUND,
            format!("block {chain}/{height} not indexed"),
        ),
        Err(e) => error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

#[derive(Deserialize)]
struct AtQuery {
    /// RFC3339; defaults to now. The migration-aware knob.
    at: Option<DateTime<Utc>>,
}

async fn resolve_domain(
    State(state): State<AppState>,
    Path((network, domain)): Path<(String, String)>,
    Query(q): Query<AtQuery>,
) -> Response {
    let at = q.at.unwrap_or_else(Utc::now);
    match state.registry.resolve_domain(&domain, &network, at) {
        Ok(chain) => Json(serde_json::json!({
            "domain": domain, "network": network, "at": at, "chain": chain.id,
        }))
        .into_response(),
        Err(e) => error(StatusCode::NOT_FOUND, e.to_string()),
    }
}

fn error(status: StatusCode, message: String) -> Response {
    (status, Json(serde_json::json!({ "error": message }))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use std::path::Path as FsPath;
    use tower::util::ServiceExt;

    async fn test_state() -> AppState {
        let seeds = FsPath::new(env!("CARGO_MANIFEST_DIR")).join("../../registry-seeds");
        let registry = Arc::new(Registry::load_from_dir(&seeds).expect("seeds"));
        let blocks: Arc<dyn BlockIndex> = Arc::new(MemoryBlockIndex::new());

        // index the synthetic fixture through the real decode path
        let fixture = FsPath::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/synthetic/polkadot-asset-hub-19000001.json");
        let bytes = std::fs::read(&fixture).expect("fixture");
        let block =
            adapter_substrate::decode_block(&bytes, "raw/polkadot-asset-hub/0001900/19000001/block.json")
                .expect("decode");
        blocks.insert(block).await.expect("insert");

        AppState { registry, blocks }
    }

    async fn get_json(app: &Router, uri: &str) -> (StatusCode, serde_json::Value) {
        let res = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = res.status();
        let body = axum::body::to_bytes(res.into_body(), 1_000_000).await.unwrap();
        let json = if body.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null)
        };
        (status, json)
    }

    #[tokio::test]
    async fn fixture_block_roundtrips_with_lineage() {
        let app = router(test_state().await);
        let (status, json) = get_json(&app, "/v1/blocks/polkadot-asset-hub/19000001").await;
        assert_eq!(status, StatusCode::OK);
        // THE Phase 0 exit criterion: lineage visible at the API surface
        assert_eq!(json["lineage"]["runtime_version"], 2_000_006);
        assert_eq!(json["lineage"]["decoder_version"], 1);
        assert!(json["lineage"]["raw_location"]
            .as_str()
            .unwrap()
            .starts_with("raw/"));
        assert_eq!(json["transactions"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn unknown_chain_and_missing_block_are_404() {
        let app = router(test_state().await);
        let (s1, _) = get_json(&app, "/v1/blocks/no-such-chain/1").await;
        assert_eq!(s1, StatusCode::NOT_FOUND);
        let (s2, _) = get_json(&app, "/v1/blocks/polkadot/1").await;
        assert_eq!(s2, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn domain_resolution_is_migration_aware_over_http() {
        let app = router(test_state().await);
        let (_, before) =
            get_json(&app, "/v1/domains/polkadot/governance?at=2025-06-01T00:00:00Z").await;
        assert_eq!(before["chain"], "polkadot");
        let (_, after) =
            get_json(&app, "/v1/domains/polkadot/governance?at=2026-01-27T12:00:00Z").await;
        assert_eq!(after["chain"], "polkadot-asset-hub");
    }

    #[tokio::test]
    async fn chains_list_reflects_registry_only() {
        let app = router(test_state().await);
        let (status, json) = get_json(&app, "/v1/chains").await;
        assert_eq!(status, StatusCode::OK);
        let ids: Vec<&str> = json["chains"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["id"].as_str().unwrap())
            .collect();
        assert!(ids.contains(&"polkadot") && ids.contains(&"polkadot-asset-hub"));
    }
}
