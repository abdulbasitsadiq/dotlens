//! dotlens-node: wires registry → raw store → decode → index → API.
//!
//! Phase 0 behavior:
//!   1. load registry seeds (./registry-seeds)
//!   2. if DATABASE_URL is set (and `pg` feature on): run migrations
//!   3. ingest fixture blocks (./fixtures/synthetic) into the raw store,
//!      guarded by checkpoints (idempotent)
//!   4. decode raw → canonical, index in memory
//!   5. serve the REST API
//!
//! Usage:
//!   dotlens-node            # run everything
//!   dotlens-node migrate    # run migrations only, then exit

use anyhow::{Context, Result};
use api::{AppState, BlockIndex, MemoryBlockIndex};
use ingest::{should_process, Checkpoint, CheckpointStore, IngestOutcome, MemoryCheckpointStore};
use raw_store::{keys, FsRawStore, RawStore};
use registry::Registry;
use std::path::Path;
use std::sync::Arc;

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv(); // load .env if present; real env always wins
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let migrate_only = std::env::args().nth(1).as_deref() == Some("migrate");

    // -- migrations (optional in Phase 0: node runs fully without a DB) -------
    #[cfg(feature = "pg")]
    if let Ok(db_url) = std::env::var("DATABASE_URL") {
        run_migrations(&db_url).await?;
        if migrate_only {
            return Ok(());
        }
    } else if migrate_only {
        anyhow::bail!("`migrate` requires DATABASE_URL");
    }
    #[cfg(not(feature = "pg"))]
    if migrate_only {
        anyhow::bail!("built without the `pg` feature");
    }

    // -- registry -------------------------------------------------------------
    let seeds_dir = env_or("REGISTRY_SEEDS", "registry-seeds");
    let registry = Arc::new(
        Registry::load_from_dir(Path::new(&seeds_dir))
            .with_context(|| format!("loading registry seeds from {seeds_dir}"))?,
    );
    tracing::info!(
        chains = registry.chains().count(),
        residency_entries = registry.residency().len(),
        "registry loaded"
    );

    // -- raw store + fixture ingestion (checkpointed, idempotent) -------------
    let raw_root = env_or("RAW_STORE_PATH", "./data/raw");
    let raw: Arc<dyn RawStore> = Arc::new(FsRawStore::new(&raw_root));
    let checkpoints = MemoryCheckpointStore::new();
    let blocks: Arc<dyn BlockIndex> = Arc::new(MemoryBlockIndex::new());

    let fixtures_dir = env_or("FIXTURES_PATH", "fixtures/synthetic");
    ingest_fixtures(
        Path::new(&fixtures_dir),
        registry.as_ref(),
        raw.as_ref(),
        &checkpoints,
        blocks.as_ref(),
    )?;
    tracing::info!(indexed = blocks.count(), "fixture ingestion complete");

    // -- API ------------------------------------------------------------------
    let bind = env_or("API_BIND", "127.0.0.1:8080");
    let app = api::router(AppState {
        registry,
        blocks,
    });
    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .with_context(|| format!("binding {bind}"))?;
    tracing::info!(%bind, "dotlens api listening");
    axum_serve(listener, app).await?;
    Ok(())
}

async fn axum_serve(listener: tokio::net::TcpListener, app: axum::Router) -> Result<()> {
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .context("api server")
}

/// Ingest every fixture envelope: raw-store put (write-once) → checkpoint guard
/// → pure decode → index. Re-running is a no-op end to end.
fn ingest_fixtures(
    dir: &Path,
    registry: &Registry,
    raw: &dyn RawStore,
    checkpoints: &dyn CheckpointStore,
    blocks: &dyn BlockIndex,
) -> Result<()> {
    // read + peek everything first, then process in (chain, height) order so
    // the high-water-mark checkpoint never silently drops an out-of-order file
    let mut items: Vec<(String, u64, String, Vec<u8>)> = Vec::new();
    for entry in std::fs::read_dir(dir)
        .with_context(|| format!("reading fixtures dir {}", dir.display()))?
    {
        let path = entry?.path();
        if !path.extension().map(|x| x == "json").unwrap_or(false) {
            continue;
        }
        let bytes = std::fs::read(&path)?;
        let peek: serde_json::Value = serde_json::from_slice(&bytes)
            .with_context(|| format!("fixture {} is not valid JSON", path.display()))?;
        let chain_id = peek["chain_id"].as_str().unwrap_or_default().to_string();
        let height = peek["height"].as_u64().unwrap_or_default();
        let hash = peek["hash"].as_str().unwrap_or_default().to_string();
        if registry.chain(&chain_id).is_none() {
            tracing::warn!(%chain_id, fixture = %path.display(), "fixture chain not in registry — skipped");
            continue;
        }
        items.push((chain_id, height, hash, bytes));
    }
    items.sort_by(|a, b| (&a.0, a.1).cmp(&(&b.0, b.1)));

    for (chain_id, height, hash, bytes) in items {
        let key = keys::block(&chain_id, height, "block.json");
        let receipt = raw
            .put(&key, &bytes, "fixture")
            .with_context(|| format!("raw put {key}"))?;
        // receipts land in core.ingest_receipts when the pg write path arrives
        // (Phase 1); until then they are at least observable
        tracing::debug!(key = %receipt.key, bytes = receipt.byte_len, source = %receipt.source, "raw stored");

        match should_process(checkpoints, &chain_id, "blocks", height)? {
            IngestOutcome::AlreadyProcessed => {
                tracing::debug!(%chain_id, height, "already processed — skipped");
                continue;
            }
            IngestOutcome::Processed => {}
        }

        let block = adapter_substrate::decode_block(&bytes, &key)
            .with_context(|| format!("decoding {key}"))?;
        blocks.insert(block);

        checkpoints.advance(Checkpoint {
            chain_id: chain_id.clone(),
            module: "blocks".into(),
            last_height: height,
            last_hash: hash,
            updated_at: chrono_now(),
        })?;
        tracing::info!(%chain_id, height, "ingested");
    }
    Ok(())
}

fn chrono_now() -> chrono::DateTime<chrono::Utc> {
    chrono::Utc::now()
}

#[cfg(feature = "pg")]
async fn run_migrations(db_url: &str) -> Result<()> {
    use sqlx::postgres::PgPoolOptions;
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(db_url)
        .await
        .context("connecting to postgres")?;
    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .context("running migrations")?;
    tracing::info!("migrations up to date");
    Ok(())
}
