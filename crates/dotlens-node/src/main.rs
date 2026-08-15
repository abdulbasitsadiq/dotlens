//! dotlens-node: wires registry → raw store → decode → index → API.
//!
//! With DATABASE_URL set (and the default `pg` feature): migrations run,
//! registry seeds sync into `core`, and checkpoints/blocks/receipts are all
//! Postgres-backed — restart-safe end to end. Without it, everything runs
//! in memory (Phase 0 mode: fixtures + API, no persistence).
//!
//! The backends are chosen as a SET, never mixed: durable checkpoints with an
//! in-memory block index would "resume" past blocks nobody stored.
//!
//! Usage:
//!   dotlens-node            # run everything
//!   dotlens-node migrate    # run migrations only, then exit

use anyhow::{Context, Result};
use api::{AppState, BlockIndex, MemoryBlockIndex};
use dotlens_node::pipeline::ingest_fixtures;
use ingest::{CheckpointStore, MemoryCheckpointStore, NoopReceiptSink, ReceiptSink};
use raw_store::{FsRawStore, RawStore};
use registry::Registry;
use std::path::Path;
use std::sync::Arc;

struct Backends {
    checkpoints: Arc<dyn CheckpointStore>,
    receipts: Arc<dyn ReceiptSink>,
    blocks: Arc<dyn BlockIndex>,
}

fn memory_backends() -> Backends {
    Backends {
        checkpoints: Arc::new(MemoryCheckpointStore::new()),
        receipts: Arc::new(NoopReceiptSink),
        blocks: Arc::new(MemoryBlockIndex::new()),
    }
}

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

    // -- backends: postgres when DATABASE_URL is set, memory otherwise --------
    #[allow(unused_mut)]
    let mut backends: Option<Backends> = None;

    #[cfg(feature = "pg")]
    if let Ok(db_url) = std::env::var("DATABASE_URL") {
        use api::pg::PgBlockIndex;
        use ingest::pg::{PgCheckpointStore, PgReceiptSink};
        use sqlx::postgres::PgPoolOptions;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&db_url)
            .await
            .context("connecting to postgres")?;
        sqlx::migrate!("../../migrations")
            .run(&pool)
            .await
            .context("running migrations")?;
        tracing::info!("migrations up to date");
        if migrate_only {
            return Ok(());
        }
        dotlens_node::registry_sync::sync_registry(&pool, registry.as_ref())
            .await
            .context("registry → DB sync")?;
        tracing::info!("registry synced to postgres");

        backends = Some(Backends {
            checkpoints: Arc::new(PgCheckpointStore::new(pool.clone())),
            receipts: Arc::new(PgReceiptSink::new(pool.clone())),
            blocks: Arc::new(PgBlockIndex::new(pool)),
        });
    }
    if migrate_only {
        // reachable only without pg feature or without DATABASE_URL
        anyhow::bail!("`migrate` requires the `pg` feature and DATABASE_URL");
    }
    #[cfg(not(feature = "pg"))]
    if std::env::var("DATABASE_URL").is_ok() {
        tracing::warn!("built without the `pg` feature — DATABASE_URL is IGNORED");
    }
    let backends = backends.unwrap_or_else(|| {
        tracing::warn!("no persistent backend — running in-memory (nothing persists)");
        memory_backends()
    });

    // -- raw store + fixture ingestion (checkpointed, idempotent) -------------
    let raw_root = env_or("RAW_STORE_PATH", "./data/raw");
    let raw: Arc<dyn RawStore> = Arc::new(FsRawStore::new(&raw_root));

    let fixtures_dir = env_or("FIXTURES_PATH", "fixtures/synthetic");
    let processed = ingest_fixtures(
        Path::new(&fixtures_dir),
        registry.as_ref(),
        raw.as_ref(),
        backends.checkpoints.as_ref(),
        backends.receipts.as_ref(),
        backends.blocks.as_ref(),
    )
    .await?;
    tracing::info!(
        processed,
        indexed = backends.blocks.count().await.map_err(|e| anyhow::anyhow!(e))?,
        "fixture ingestion complete"
    );

    // -- API ------------------------------------------------------------------
    let bind = env_or("API_BIND", "127.0.0.1:8080");
    let app = api::router(AppState {
        registry,
        blocks: backends.blocks.clone(),
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
