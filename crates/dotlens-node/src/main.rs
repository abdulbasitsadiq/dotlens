//! dotlens-node: wires registry → raw store → decode → index → API.
//!
//! With DATABASE_URL set (and the default `pg` feature): migrations run,
//! registry seeds sync into `core`, and checkpoints/blocks/receipts/runtime
//! lineage are all Postgres-backed — restart-safe end to end. Without it,
//! everything runs in memory (fixtures + API, nothing persists).
//!
//! Backends are chosen as a SET, never mixed: durable checkpoints with an
//! in-memory block index would "resume" past blocks nobody stored.
//!
//! Usage:
//!   dotlens-node                              # fixtures + API (+ live, see below)
//!   dotlens-node migrate                      # run migrations only, then exit
//!   dotlens-node backfill <chain> <from> <to> # raw backfill a height range
//!   dotlens-node sync-labels                  # derive + project account labels, exit
//!   dotlens-node verify-labels <chain>        # probe labels on-chain, record, exit
//!
//! Live ingestion (`live` feature + LIVE_INGEST=1 + DATABASE_URL): every
//! registered chain with rpc endpoints and the `blocks` module gets a follower
//! task ingesting finalized heads raw-first (no decode yet — that's the
//! frame-decode slice). POLL_INTERVAL_SECS tunes the poll (default 6).

use anyhow::{Context, Result};
use api::{AppState, BlockIndex, MemoryBlockIndex};
use dotlens_node::pipeline::ingest_fixtures;
use ingest::live::{NoopRuntimeVersionSink, RuntimeVersionSink};
use ingest::{CheckpointStore, MemoryCheckpointStore, NoopReceiptSink, ReceiptSink};
use raw_store::{FsRawStore, RawStore};
use registry::Registry;
use std::path::Path;
use std::sync::Arc;

struct Backends {
    checkpoints: Arc<dyn CheckpointStore>,
    receipts: Arc<dyn ReceiptSink>,
    blocks: Arc<dyn BlockIndex>,
    labels: Arc<dyn api::LabelIndex>,
    runtime_versions: Arc<dyn RuntimeVersionSink>,
    /// Kept for label sync/verify (they need direct SQL, not a trait).
    #[cfg(feature = "pg")]
    pool: Option<sqlx::PgPool>,
    persistent: bool,
}

fn memory_backends() -> Backends {
    Backends {
        checkpoints: Arc::new(MemoryCheckpointStore::new()),
        receipts: Arc::new(NoopReceiptSink),
        blocks: Arc::new(MemoryBlockIndex::new()),
        labels: Arc::new(api::MemoryLabelIndex::new()),
        runtime_versions: Arc::new(NoopRuntimeVersionSink),
        #[cfg(feature = "pg")]
        pool: None,
        persistent: false,
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_flag(key: &str) -> bool {
    matches!(
        std::env::var(key).unwrap_or_default().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

enum Command {
    Run,
    Migrate,
    Backfill { chain: String, from: u64, to: u64 },
    DecodeRange { chain: String, from: u64, to: u64 },
    CaptureFixture { chain: String, height: u64 },
    SyncLabels,
    VerifyLabels { chain: String },
}

fn parse_args() -> Result<Command> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let range = |usage: &'static str| -> Result<(String, u64, u64)> {
        let chain = args.get(1).context(usage)?.clone();
        let from: u64 = args.get(2).context(usage)?.parse().context(usage)?;
        let to: u64 = args.get(3).context(usage)?.parse().context(usage)?;
        anyhow::ensure!(from <= to, "from must be <= to");
        Ok((chain, from, to))
    };
    match args.first().map(String::as_str) {
        None => Ok(Command::Run),
        Some("migrate") => Ok(Command::Migrate),
        Some("backfill") => {
            let (chain, from, to) = range("usage: dotlens-node backfill <chain> <from> <to>")?;
            Ok(Command::Backfill { chain, from, to })
        }
        Some("decode-range") => {
            let (chain, from, to) = range("usage: dotlens-node decode-range <chain> <from> <to>")?;
            Ok(Command::DecodeRange { chain, from, to })
        }
        Some("capture-fixture") => {
            let usage = "usage: dotlens-node capture-fixture <chain> <height>";
            let chain = args.get(1).context(usage)?.clone();
            let height: u64 = args.get(2).context(usage)?.parse().context(usage)?;
            Ok(Command::CaptureFixture { chain, height })
        }
        Some("sync-labels") => Ok(Command::SyncLabels),
        Some("verify-labels") => {
            let usage = "usage: dotlens-node verify-labels <chain>";
            let chain = args.get(1).context(usage)?.clone();
            Ok(Command::VerifyLabels { chain })
        }
        Some(other) => anyhow::bail!("unknown command: {other}"),
    }
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

    let command = parse_args()?;

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
        use dotlens_node::runtime_versions::PgRuntimeVersionSink;
        use ingest::pg::{PgCheckpointStore, PgReceiptSink};
        use sqlx::postgres::PgPoolOptions;

        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(&db_url)
            .await
            .context("connecting to postgres")?;
        sqlx::migrate!("../../migrations")
            .run(&pool)
            .await
            .context("running migrations")?;
        tracing::info!("migrations up to date");
        if matches!(command, Command::Migrate) {
            return Ok(());
        }
        // registry sync BEFORE any worker: substrate.runtime_versions FKs core.chains
        dotlens_node::registry_sync::sync_registry(&pool, registry.as_ref())
            .await
            .context("registry → DB sync")?;
        tracing::info!("registry synced to postgres");

        backends = Some(Backends {
            checkpoints: Arc::new(PgCheckpointStore::new(pool.clone())),
            receipts: Arc::new(PgReceiptSink::new(pool.clone())),
            blocks: Arc::new(PgBlockIndex::new(pool.clone())),
            labels: Arc::new(api::pg::PgLabelIndex::new(pool.clone())),
            runtime_versions: Arc::new(PgRuntimeVersionSink::new(pool.clone())),
            pool: Some(pool),
            persistent: true,
        });
    }
    if matches!(command, Command::Migrate) {
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

    let raw_root = env_or("RAW_STORE_PATH", "./data/raw");
    let raw: Arc<dyn RawStore> = Arc::new(FsRawStore::new(&raw_root));

    // -- one-shot subcommands: run, report, exit ------------------------------
    if let Command::Backfill { chain, from, to } = &command {
        anyhow::ensure!(
            backends.persistent,
            "backfill requires DATABASE_URL (raw receipts + checkpoints must persist)"
        );
        return run_backfill(&registry, &backends, raw.as_ref(), chain, *from, *to).await;
    }
    if let Command::DecodeRange { chain, from, to } = &command {
        anyhow::ensure!(
            backends.persistent,
            "decode-range requires DATABASE_URL (canonical rows must persist)"
        );
        return run_decode_range(&registry, &backends, raw.as_ref(), chain, *from, *to).await;
    }
    if let Command::CaptureFixture { chain, height } = &command {
        return run_capture_fixture(&registry, chain, *height).await;
    }
    if matches!(command, Command::SyncLabels) {
        #[cfg(feature = "pg")]
        if let Some(pool) = &backends.pool {
            let report =
                dotlens_node::labels::sync_labels(pool, registry.as_ref(), raw.as_ref()).await?;
            println!("label sync: {report:?}");
            return Ok(());
        }
        anyhow::bail!("sync-labels requires the `pg` feature and DATABASE_URL");
    }
    if let Command::VerifyLabels { chain } = &command {
        return run_verify_labels(&registry, &backends, chain).await;
    }

    // -- account labels: derive + project on every start (idempotent) ---------
    #[cfg(feature = "pg")]
    if let Some(pool) = &backends.pool {
        let report =
            dotlens_node::labels::sync_labels(pool, registry.as_ref(), raw.as_ref()).await?;
        tracing::info!(
            sovereigns = report.sovereign_labels,
            pallets = report.pallet_labels,
            seeded = report.seeded_labels,
            missing_metadata = ?report.chains_missing_metadata,
            "account labels synced"
        );
    }

    // -- fixture ingestion (checkpointed, idempotent) -------------------------
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

    // -- live + decode followers ----------------------------------------------
    let backends = Arc::new(backends);
    spawn_live_followers(&registry, &backends, &raw);
    spawn_decode_followers(&registry, &backends, &raw);

    // -- API ------------------------------------------------------------------
    let bind = env_or("API_BIND", "127.0.0.1:8080");
    let app = api::router(AppState {
        registry,
        blocks: backends.blocks.clone(),
        labels: backends.labels.clone(),
        // family-encoded address parsing is adapter-owned (Invariant 4); with
        // more families this becomes registry-driven dispatch
        parse_account: Arc::new(|s| {
            adapter_substrate::accounts::parse_account(s).map(|a| a.to_vec())
        }),
    });
    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .with_context(|| format!("binding {bind}"))?;
    tracing::info!(%bind, "dotlens api listening");
    axum_serve(listener, app).await?;
    Ok(())
}

#[cfg(feature = "live")]
fn spawn_live_followers(
    registry: &Arc<Registry>,
    backends: &Arc<Backends>,
    raw: &Arc<dyn RawStore>,
) {
    use adapter_substrate::source::SubstrateSource;

    if !env_flag("LIVE_INGEST") {
        tracing::info!("live ingestion disabled (set LIVE_INGEST=1 to enable)");
        return;
    }
    if !backends.persistent {
        tracing::warn!("LIVE_INGEST=1 but no DATABASE_URL — refusing to live-ingest into memory");
        return;
    }
    let poll = std::time::Duration::from_secs(
        env_or("POLL_INTERVAL_SECS", "6").parse().unwrap_or(6),
    );
    let now = chrono::Utc::now();
    for chain in registry.chains() {
        let live = chain.status_at(now) == Some(registry::LifecycleStatus::Live);
        if !live || !chain.has_module("blocks") || chain.endpoints.rpc.is_empty() {
            tracing::debug!(chain = %chain.id, "not eligible for live ingestion — skipped");
            continue;
        }
        let source = match SubstrateSource::new(&chain.id, chain.endpoints.rpc.clone()) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(chain = %chain.id, error = %e, "live follower not started");
                continue;
            }
        };
        let chain_id = chain.id.clone();
        let backends = backends.clone();
        let raw = raw.clone();
        tokio::spawn(async move {
            tracing::info!(chain = %chain_id, "live follower started");
            let deps = ingest::live::IngestDeps {
                raw: raw.as_ref(),
                checkpoints: backends.checkpoints.as_ref(),
                receipts: backends.receipts.as_ref(),
                runtime_versions: backends.runtime_versions.as_ref(),
            };
            ingest::live::follow(&chain_id, &source, &deps, poll).await;
        });
    }
}

#[cfg(not(feature = "live"))]
fn spawn_live_followers(_: &Arc<Registry>, _: &Arc<Backends>, _: &Arc<dyn RawStore>) {
    if env_flag("LIVE_INGEST") {
        tracing::warn!("built without the `live` feature — LIVE_INGEST is IGNORED");
    }
}

#[cfg(feature = "live")]
async fn run_backfill(
    registry: &Registry,
    backends: &Backends,
    raw: &dyn RawStore,
    chain: &str,
    from: u64,
    to: u64,
) -> Result<()> {
    use adapter_substrate::source::SubstrateSource;

    let cfg = registry
        .chain(chain)
        .with_context(|| format!("unknown chain: {chain}"))?;
    let source = SubstrateSource::new(&cfg.id, cfg.endpoints.rpc.clone())
        .map_err(|e| anyhow::anyhow!(e))?;
    let deps = ingest::live::IngestDeps {
        raw,
        checkpoints: backends.checkpoints.as_ref(),
        receipts: backends.receipts.as_ref(),
        runtime_versions: backends.runtime_versions.as_ref(),
    };
    // per-range checkpoint module: disjoint ranges never fight each other,
    // and re-running the same range resumes exactly where it stopped
    let module = format!("{}:{from}-{to}", ingest::live::MODULE_BACKFILL);
    let n = ingest::live::ingest_range(&cfg.id, &source, &deps, &module, from, to, &mut None)
        .await
        .with_context(|| format!("backfill {chain} {from}..={to}"))?;
    tracing::info!(chain, from, to, processed = n, "backfill complete");
    println!("backfill {chain} {from}..={to}: processed {n} blocks");
    Ok(())
}

#[cfg(not(feature = "live"))]
async fn run_backfill(
    _: &Registry,
    _: &Backends,
    _: &dyn RawStore,
    _: &str,
    _: u64,
    _: u64,
) -> Result<()> {
    anyhow::bail!("backfill requires the `live` feature")
}

/// Decode followers: chase each chain's raw_blocks checkpoint, decoding with
/// spec-correct archived metadata into the canonical tables. Pure decode — no
/// network — so this needs only the `pg` feature, not `live`.
fn spawn_decode_followers(
    registry: &Arc<Registry>,
    backends: &Arc<Backends>,
    raw: &Arc<dyn RawStore>,
) {
    use adapter_substrate::frame_decoder::SubstrateFrameDecoder;

    if !env_flag("DECODE_FOLLOW") {
        tracing::info!("decode follower disabled (set DECODE_FOLLOW=1 to enable)");
        return;
    }
    if !backends.persistent {
        tracing::warn!("DECODE_FOLLOW=1 but no DATABASE_URL — refusing to decode into memory");
        return;
    }
    let poll = std::time::Duration::from_secs(
        env_or("POLL_INTERVAL_SECS", "6").parse().unwrap_or(6),
    );
    for chain in registry.chains() {
        if !chain.has_module("blocks") {
            continue;
        }
        let decoder = SubstrateFrameDecoder::new(chain.ss58_prefix.unwrap_or(42));
        let chain_id = chain.id.clone();
        let backends = backends.clone();
        let raw = raw.clone();
        tokio::spawn(async move {
            tracing::info!(chain = %chain_id, "decode follower started");
            let sink = dotlens_node::pipeline::BlockIndexSink(backends.blocks.clone());
            let deps = ingest::decode::DecodeDeps {
                raw: raw.as_ref(),
                checkpoints: backends.checkpoints.as_ref(),
                sink: &sink,
            };
            ingest::decode::decode_follow(&chain_id, &decoder, &deps, poll).await;
        });
    }
}

async fn run_decode_range(
    registry: &Registry,
    backends: &Backends,
    raw: &dyn RawStore,
    chain: &str,
    from: u64,
    to: u64,
) -> Result<()> {
    use adapter_substrate::frame_decoder::SubstrateFrameDecoder;

    let cfg = registry
        .chain(chain)
        .with_context(|| format!("unknown chain: {chain}"))?;
    let decoder = SubstrateFrameDecoder::new(cfg.ss58_prefix.unwrap_or(42));
    let sink = dotlens_node::pipeline::BlockIndexSink(backends.blocks.clone());
    let deps = ingest::decode::DecodeDeps {
        raw,
        checkpoints: backends.checkpoints.as_ref(),
        sink: &sink,
    };
    let n = ingest::decode::decode_range(&cfg.id, &decoder, &deps, from, to)
        .await
        .with_context(|| format!("decode-range {chain} {from}..={to}"))?;
    tracing::info!(chain, from, to, decoded = n, "decode-range complete");
    println!("decode-range {chain} {from}..={to}: decoded {n} blocks");
    Ok(())
}

/// Snapshot one real block (+ events + metadata) into fixtures/real/ so the
/// decode tests exercise genuine SCALE. Network-touching → `live` feature.
#[cfg(feature = "live")]
async fn run_capture_fixture(registry: &Registry, chain: &str, height: u64) -> Result<()> {
    use adapter_substrate::source::SubstrateSource;
    use ingest::live::ChainSource;

    let cfg = registry
        .chain(chain)
        .with_context(|| format!("unknown chain: {chain}"))?;
    let source = SubstrateSource::new(&cfg.id, cfg.endpoints.rpc.clone())
        .map_err(|e| anyhow::anyhow!(e))?;
    let fetched = source.fetch_block(height).await.map_err(|e| anyhow::anyhow!(e))?;
    let metadata = source.metadata_at(height).await.map_err(|e| anyhow::anyhow!(e))?;

    let dir = std::path::PathBuf::from(env_or("FIXTURES_REAL_PATH", "fixtures/real"))
        .join(format!("{}-{height}", cfg.id));
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    for artifact in &fetched.artifacts {
        std::fs::write(dir.join(&artifact.item), &artifact.bytes)?;
    }
    std::fs::write(dir.join("metadata.scale"), &metadata)?;
    println!(
        "captured {}/{height} (spec {}, {} artifacts + metadata {} bytes) → {}",
        cfg.id,
        fetched.runtime_version,
        fetched.artifacts.len(),
        metadata.len(),
        dir.display()
    );
    println!("commit fixtures/real so decode tests cover this block permanently");
    Ok(())
}

#[cfg(not(feature = "live"))]
async fn run_capture_fixture(_: &Registry, _: &str, _: u64) -> Result<()> {
    anyhow::bail!("capture-fixture requires the `live` feature")
}

/// verify-labels <chain>: probe System.Account for every label scoped to the
/// chain at the current finalized head and record exists/absent. This is the
/// ROADMAP step that resolves ECOSYSTEM.md's UNVERIFIED addresses as DATA.
#[cfg(all(feature = "pg", feature = "live"))]
async fn run_verify_labels(registry: &Registry, backends: &Backends, chain: &str) -> Result<()> {
    use adapter_substrate::{accounts, source::SubstrateSource};
    use ingest::live::ChainSource;

    let pool = backends
        .pool
        .as_ref()
        .context("verify-labels requires DATABASE_URL")?;
    let cfg = registry
        .chain(chain)
        .with_context(|| format!("unknown chain: {chain}"))?;
    let source = SubstrateSource::new(&cfg.id, cfg.endpoints.rpc.clone())
        .map_err(|e| anyhow::anyhow!(e))?;
    let height = source
        .finalized_height()
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    // one hash for the whole probe batch (avoid N+1 chain_getBlockHash)
    let at_hash = source.block_hash(height).await.map_err(|e| anyhow::anyhow!(e))?;

    let rows = dotlens_node::labels::labels_for_chain(pool, &cfg.id).await?;
    anyhow::ensure!(
        !rows.is_empty(),
        "no labels scoped to {chain} — run sync-labels (or a normal node start) first"
    );
    let (mut exists_n, mut absent_n) = (0u32, 0u32);
    for row in &rows {
        let key = accounts::system_account_key(&row.account_id);
        let exists = source
            .storage_contains_at(&key, at_hash)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        dotlens_node::labels::record_verification(pool, row, height, exists).await?;
        if exists {
            exists_n += 1;
        } else {
            absent_n += 1;
        }
        println!(
            "{:6} {:15} {} — {}",
            if exists { "EXISTS" } else { "absent" },
            row.kind,
            row.ss58.as_deref().unwrap_or("?"),
            row.label
        );
    }
    println!(
        "verify-labels {chain} at #{height}: {exists_n} exist, {absent_n} absent \
         (absent = no System.Account entry — honest data, not an error)"
    );
    Ok(())
}

#[cfg(not(all(feature = "pg", feature = "live")))]
async fn run_verify_labels(_: &Registry, _: &Backends, _: &str) -> Result<()> {
    anyhow::bail!("verify-labels requires the `pg` and `live` features")
}

async fn axum_serve(listener: tokio::net::TcpListener, app: axum::Router) -> Result<()> {
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .context("api server")
}
