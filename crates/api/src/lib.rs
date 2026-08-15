//! REST API (Axum). Phase 0: read-only endpoints over an in-memory block index
//! plus the registry. The block index becomes Postgres-backed in Phase 1 behind
//! the same `BlockIndex` trait.

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

/// Storage abstraction the API reads blocks from.
pub trait BlockIndex: Send + Sync {
    fn get(&self, chain_id: &str, height: u64) -> Option<CanonicalBlock>;
    fn insert(&self, block: CanonicalBlock);
    fn count(&self) -> usize;
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

impl BlockIndex for MemoryBlockIndex {
    fn get(&self, chain_id: &str, height: u64) -> Option<CanonicalBlock> {
        self.inner
            .read()
            .ok()?
            .get(&(chain_id.to_string(), height))
            .cloned()
    }
    fn insert(&self, block: CanonicalBlock) {
        if let Ok(mut map) = self.inner.write() {
            map.insert((block.chain_id.clone(), block.height), block);
        }
    }
    fn count(&self) -> usize {
        self.inner.read().map(|m| m.len()).unwrap_or(0)
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
    match state.blocks.get(&chain, height) {
        Some(block) => Json(block).into_response(),
        None => error(
            StatusCode::NOT_FOUND,
            format!("block {chain}/{height} not indexed"),
        ),
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

    fn test_state() -> AppState {
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
        blocks.insert(block);

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
        let app = router(test_state());
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
        let app = router(test_state());
        let (s1, _) = get_json(&app, "/v1/blocks/no-such-chain/1").await;
        assert_eq!(s1, StatusCode::NOT_FOUND);
        let (s2, _) = get_json(&app, "/v1/blocks/polkadot/1").await;
        assert_eq!(s2, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn domain_resolution_is_migration_aware_over_http() {
        let app = router(test_state());
        let (_, before) =
            get_json(&app, "/v1/domains/polkadot/governance?at=2025-06-01T00:00:00Z").await;
        assert_eq!(before["chain"], "polkadot");
        let (_, after) =
            get_json(&app, "/v1/domains/polkadot/governance?at=2026-01-27T12:00:00Z").await;
        assert_eq!(after["chain"], "polkadot-asset-hub");
    }

    #[tokio::test]
    async fn chains_list_reflects_registry_only() {
        let app = router(test_state());
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
