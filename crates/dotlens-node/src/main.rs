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
//!   dotlens-node balances-range <chain> <a> <b>       # map deltas over a range, exit
//!   dotlens-node anchor-balance <chain> <acct> <h>    # record absolute balance anchor
//!   dotlens-node gov-range <chain> <a> <b>    # map referendum timelines over a range
//!   dotlens-node sync-tracks                  # decode gov tracks from metadata, exit
//!
//! backfill accepts an optional worker count (`backfill <chain> <a> <b> 8`) —
//! deterministic chunks, per-chunk checkpoints, re-run the same command to
//! resume. TIP_FOLLOW=1 follows the unfinalized head with reorg handling
//! (finalized rows immutable; unfinalized replaced/pruned).
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
    balances: Arc<dyn api::BalanceIndex>,
    gov: Arc<dyn api::GovIndex>,
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
        balances: Arc::new(api::MemoryBalanceIndex::new()),
        gov: Arc::new(api::MemoryGovIndex::new()),
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
    Backfill { chain: String, from: u64, to: u64, workers: u64 },
    DecodeRange { chain: String, from: u64, to: u64 },
    CaptureFixture { chain: String, height: u64 },
    SyncLabels,
    VerifyLabels { chain: String },
    BalancesRange { chain: String, from: u64, to: u64 },
    AnchorBalance { chain: String, account: String, height: u64 },
    GovRange { chain: String, from: u64, to: u64 },
    SyncTracks,
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
            let usage = "usage: dotlens-node backfill <chain> <from> <to> [workers]";
            let (chain, from, to) = range(usage)?;
            let workers: u64 = match args.get(4) {
                Some(w) => {
                    let w: u64 = w.parse().context(usage)?;
                    anyhow::ensure!((1..=64).contains(&w), "workers must be 1..=64");
                    w
                }
                None => 1,
            };
            Ok(Command::Backfill { chain, from, to, workers })
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
        Some("balances-range") => {
            let (chain, from, to) = range("usage: dotlens-node balances-range <chain> <from> <to>")?;
            Ok(Command::BalancesRange { chain, from, to })
        }
        Some("gov-range") => {
            let (chain, from, to) = range("usage: dotlens-node gov-range <chain> <from> <to>")?;
            Ok(Command::GovRange { chain, from, to })
        }
        Some("sync-tracks") => Ok(Command::SyncTracks),
        Some("anchor-balance") => {
            let usage = "usage: dotlens-node anchor-balance <chain> <account> <height>";
            let chain = args.get(1).context(usage)?.clone();
            let account = args.get(2).context(usage)?.clone();
            let height: u64 = args.get(3).context(usage)?.parse().context(usage)?;
            Ok(Command::AnchorBalance { chain, account, height })
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
            balances: Arc::new(api::pg::PgBalanceIndex::new(pool.clone())),
            gov: Arc::new(api::pg::PgGovIndex::new(pool.clone())),
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
    if let Command::Backfill { chain, from, to, workers } = &command {
        anyhow::ensure!(
            backends.persistent,
            "backfill requires DATABASE_URL (raw receipts + checkpoints must persist)"
        );
        return run_backfill(&registry, &backends, &raw, chain, *from, *to, *workers).await;
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
    if let Command::BalancesRange { chain, from, to } = &command {
        anyhow::ensure!(
            backends.persistent,
            "balances-range requires DATABASE_URL (canonical events + deltas must persist)"
        );
        return run_balances_range(&registry, &backends, chain, *from, *to).await;
    }
    if let Command::AnchorBalance { chain, account, height } = &command {
        return run_anchor_balance(&registry, &backends, raw.as_ref(), chain, account, *height)
            .await;
    }
    if let Command::GovRange { chain, from, to } = &command {
        anyhow::ensure!(
            backends.persistent,
            "gov-range requires DATABASE_URL (canonical events + timelines must persist)"
        );
        return run_gov_range(&registry, &backends, chain, *from, *to).await;
    }
    if matches!(command, Command::SyncTracks) {
        #[cfg(feature = "pg")]
        if let Some(pool) = &backends.pool {
            let report =
                dotlens_node::gov_pg::sync_tracks(pool, registry.as_ref(), raw.as_ref()).await?;
            println!("track sync: {report:?}");
            return Ok(());
        }
        anyhow::bail!("sync-tracks requires the `pg` feature and DATABASE_URL");
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
        let tracks =
            dotlens_node::gov_pg::sync_tracks(pool, registry.as_ref(), raw.as_ref()).await?;
        tracing::info!(
            tracks = tracks.tracks,
            skipped = ?tracks.chains_skipped,
            "governance tracks synced"
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
    spawn_balances_followers(&registry, &backends);
    spawn_gov_followers(&registry, &backends);
    spawn_tip_followers(&registry, &backends, &raw);

    // -- API ------------------------------------------------------------------
    let bind = env_or("API_BIND", "127.0.0.1:8080");
    let app = api::router(AppState {
        registry,
        blocks: backends.blocks.clone(),
        labels: backends.labels.clone(),
        balances: backends.balances.clone(),
        gov: backends.gov.clone(),
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

/// Backfill a height range, optionally across N concurrent workers.
/// Chunking is DETERMINISTIC (dotlens_node::backfill_chunks): each chunk owns
/// checkpoint module `raw_backfill:{a}-{b}`, so any crash/kill resumes
/// per-chunk by re-running the exact same command — the restart-safety story
/// for the ≥1M-block drill. workers=1 keeps the historical single-range
/// module name (existing checkpoints stay valid).
#[cfg(feature = "live")]
async fn run_backfill(
    registry: &Registry,
    backends: &Backends,
    raw: &Arc<dyn RawStore>,
    chain: &str,
    from: u64,
    to: u64,
    workers: u64,
) -> Result<()> {
    use adapter_substrate::source::SubstrateSource;

    let cfg = registry
        .chain(chain)
        .with_context(|| format!("unknown chain: {chain}"))?;
    let chunks = dotlens_node::backfill_chunks(from, to, workers);
    tracing::info!(chain, from, to, chunks = chunks.len(), "backfill starting");

    let mut handles = Vec::with_capacity(chunks.len());
    for (a, b) in chunks {
        let chain_id = cfg.id.clone();
        let endpoints = cfg.endpoints.rpc.clone();
        let raw = raw.clone();
        let checkpoints = backends.checkpoints.clone();
        let receipts = backends.receipts.clone();
        let runtime_versions = backends.runtime_versions.clone();
        handles.push(tokio::spawn(async move {
            // each worker owns its connection + failover rotation
            let source =
                SubstrateSource::new(&chain_id, endpoints).map_err(|e| anyhow::anyhow!(e))?;
            let deps = ingest::live::IngestDeps {
                raw: raw.as_ref(),
                checkpoints: checkpoints.as_ref(),
                receipts: receipts.as_ref(),
                runtime_versions: runtime_versions.as_ref(),
            };
            let module = format!("{}:{a}-{b}", ingest::live::MODULE_BACKFILL);
            let n = ingest::live::ingest_range(&chain_id, &source, &deps, &module, a, b, &mut None)
                .await
                .with_context(|| format!("chunk {a}..={b}"))?;
            Ok::<(u64, u64, u64), anyhow::Error>((a, b, n))
        }));
    }

    let (mut total, mut failed) = (0u64, 0u32);
    for handle in handles {
        match handle.await {
            Ok(Ok((a, b, n))) => {
                total += n;
                tracing::info!(from = a, to = b, processed = n, "chunk complete");
            }
            Ok(Err(e)) => {
                failed += 1;
                tracing::error!(error = %e, "chunk FAILED");
            }
            Err(e) => {
                failed += 1;
                tracing::error!(error = %e, "chunk task panicked");
            }
        }
    }
    anyhow::ensure!(
        failed == 0,
        "{failed} chunk(s) failed — re-run the SAME command to resume from per-chunk checkpoints"
    );
    tracing::info!(chain, from, to, processed = total, "backfill complete");
    println!("backfill {chain} {from}..={to}: processed {total} blocks");
    Ok(())
}

#[cfg(not(feature = "live"))]
async fn run_backfill(
    _: &Registry,
    _: &Backends,
    _: &Arc<dyn RawStore>,
    _: &str,
    _: u64,
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

/// Tip followers: keep the finalized+1..=best window fresh with reorg
/// handling. Needs `live` (best-head RPC) + `pg` (unfinalized bookkeeping).
#[cfg(all(feature = "pg", feature = "live"))]
fn spawn_tip_followers(
    registry: &Arc<Registry>,
    backends: &Arc<Backends>,
    raw: &Arc<dyn RawStore>,
) {
    use adapter_substrate::frame_decoder::SubstrateFrameDecoder;
    use adapter_substrate::source::SubstrateSource;

    if !env_flag("TIP_FOLLOW") {
        tracing::info!("tip follower disabled (set TIP_FOLLOW=1 to enable)");
        return;
    }
    if !backends.persistent {
        tracing::warn!("TIP_FOLLOW=1 but no DATABASE_URL — refusing to track tips in memory");
        return;
    }
    let poll = std::time::Duration::from_secs(
        env_or("TIP_POLL_INTERVAL_SECS", "3").parse().unwrap_or(3),
    );
    let now = chrono::Utc::now();
    for chain in registry.chains() {
        let live = chain.status_at(now) == Some(registry::LifecycleStatus::Live);
        if !live || !chain.has_module("blocks") || chain.endpoints.rpc.is_empty() {
            continue;
        }
        let source = match SubstrateSource::new(&chain.id, chain.endpoints.rpc.clone()) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(chain = %chain.id, error = %e, "tip follower not started");
                continue;
            }
        };
        let decoder = SubstrateFrameDecoder::new(chain.ss58_prefix.unwrap_or(42));
        let Some(pool) = backends.pool.clone() else { continue };
        let chain_id = chain.id.clone();
        let backends = backends.clone();
        let raw = raw.clone();
        tokio::spawn(async move {
            tracing::info!(chain = %chain_id, "tip follower started");
            let store = dotlens_node::tip_pg::PgUnfinalizedStore::new(pool);
            let sink = dotlens_node::pipeline::BlockIndexSink(backends.blocks.clone());
            let deps = ingest::tip::TipDeps {
                raw: raw.as_ref(),
                receipts: backends.receipts.as_ref(),
                store: &store,
                sink: &sink,
            };
            ingest::tip::tip_follow(&chain_id, &source, &decoder, &deps, poll).await;
        });
    }
}

#[cfg(not(all(feature = "pg", feature = "live")))]
fn spawn_tip_followers(_: &Arc<Registry>, _: &Arc<Backends>, _: &Arc<dyn RawStore>) {
    if env_flag("TIP_FOLLOW") {
        tracing::warn!("built without `pg`+`live` — TIP_FOLLOW is IGNORED");
    }
}

/// Balances followers: chase each chain's decode checkpoint, mapping canonical
/// events into balance deltas. Pure mapping over Pg — no network, `pg` only.
/// Eligibility is registry data: the chain must enable the `balances` module.
fn spawn_balances_followers(registry: &Arc<Registry>, backends: &Arc<Backends>) {
    if !env_flag("BALANCES_FOLLOW") {
        tracing::info!("balances follower disabled (set BALANCES_FOLLOW=1 to enable)");
        return;
    }
    if !backends.persistent {
        tracing::warn!("BALANCES_FOLLOW=1 but no DATABASE_URL — refusing to map into memory");
        return;
    }
    #[cfg(feature = "pg")]
    {
        use adapter_substrate::balances::SubstrateDeltaMapper;

        let poll = std::time::Duration::from_secs(
            env_or("POLL_INTERVAL_SECS", "6").parse().unwrap_or(6),
        );
        for chain in registry.chains() {
            if !chain.has_module("balances") {
                continue;
            }
            if chain.family != registry::ChainFamily::Substrate {
                tracing::debug!(chain = %chain.id, "no delta mapper for this family — skipped");
                continue;
            }
            let Some(pool) = backends.pool.clone() else { continue };
            let chain_id = chain.id.clone();
            let backends = backends.clone();
            tokio::spawn(async move {
                tracing::info!(chain = %chain_id, "balances follower started");
                let source = dotlens_node::balances_pg::PgEventSource::new(pool.clone());
                let sink = dotlens_node::balances_pg::PgDeltaSink::new(pool);
                let deps = ingest::balances::BalancesDeps {
                    checkpoints: backends.checkpoints.as_ref(),
                    source: &source,
                    sink: &sink,
                };
                ingest::balances::balances_follow(&chain_id, &SubstrateDeltaMapper, &deps, poll)
                    .await;
            });
        }
    }
}

/// Gov followers: chase each chain's decode checkpoint, mapping canonical
/// referenda events into referendum timelines. Pure mapping over Pg — no
/// network, `pg` only. Eligibility is registry data: the chain must enable
/// the `governance` module.
fn spawn_gov_followers(registry: &Arc<Registry>, backends: &Arc<Backends>) {
    if !env_flag("GOV_FOLLOW") {
        tracing::info!("gov follower disabled (set GOV_FOLLOW=1 to enable)");
        return;
    }
    if !backends.persistent {
        tracing::warn!("GOV_FOLLOW=1 but no DATABASE_URL — refusing to map into memory");
        return;
    }
    #[cfg(feature = "pg")]
    {
        use adapter_substrate::gov::SubstrateGovMapper;

        let poll = std::time::Duration::from_secs(
            env_or("POLL_INTERVAL_SECS", "6").parse().unwrap_or(6),
        );
        for chain in registry.chains() {
            if !chain.has_module("governance") {
                continue;
            }
            if chain.family != registry::ChainFamily::Substrate {
                tracing::debug!(chain = %chain.id, "no gov mapper for this family — skipped");
                continue;
            }
            let Some(pool) = backends.pool.clone() else { continue };
            let chain_id = chain.id.clone();
            let backends = backends.clone();
            tokio::spawn(async move {
                tracing::info!(chain = %chain_id, "gov follower started");
                let source = dotlens_node::balances_pg::PgEventSource::new(pool.clone());
                let sink = dotlens_node::gov_pg::PgTimelineSink::new(pool);
                let deps = ingest::gov::GovDeps {
                    checkpoints: backends.checkpoints.as_ref(),
                    source: &source,
                    sink: &sink,
                };
                ingest::gov::gov_follow(&chain_id, &SubstrateGovMapper, &deps, poll).await;
            });
        }
    }
}

#[cfg(feature = "pg")]
async fn run_gov_range(
    registry: &Registry,
    backends: &Backends,
    chain: &str,
    from: u64,
    to: u64,
) -> Result<()> {
    use adapter_substrate::gov::SubstrateGovMapper;

    let cfg = registry
        .chain(chain)
        .with_context(|| format!("unknown chain: {chain}"))?;
    anyhow::ensure!(
        cfg.family == registry::ChainFamily::Substrate,
        "no gov mapper for family {:?}",
        cfg.family
    );
    let pool = backends.pool.as_ref().context("gov-range requires DATABASE_URL")?;
    let source = dotlens_node::balances_pg::PgEventSource::new(pool.clone());
    let sink = dotlens_node::gov_pg::PgTimelineSink::new(pool.clone());
    let deps = ingest::gov::GovDeps {
        checkpoints: backends.checkpoints.as_ref(),
        source: &source,
        sink: &sink,
    };
    let n = ingest::gov::gov_range(&cfg.id, &SubstrateGovMapper, &deps, from, to)
        .await
        .with_context(|| format!("gov-range {chain} {from}..={to}"))?;
    tracing::info!(chain, from, to, mapped = n, "gov-range complete");
    println!("gov-range {chain} {from}..={to}: mapped {n} blocks");
    Ok(())
}

#[cfg(not(feature = "pg"))]
async fn run_gov_range(_: &Registry, _: &Backends, _: &str, _: u64, _: u64) -> Result<()> {
    anyhow::bail!("gov-range requires the `pg` feature")
}

#[cfg(feature = "pg")]
async fn run_balances_range(
    registry: &Registry,
    backends: &Backends,
    chain: &str,
    from: u64,
    to: u64,
) -> Result<()> {
    use adapter_substrate::balances::SubstrateDeltaMapper;

    let cfg = registry
        .chain(chain)
        .with_context(|| format!("unknown chain: {chain}"))?;
    anyhow::ensure!(
        cfg.family == registry::ChainFamily::Substrate,
        "no delta mapper for family {:?}",
        cfg.family
    );
    let pool = backends.pool.as_ref().context("balances-range requires DATABASE_URL")?;
    let source = dotlens_node::balances_pg::PgEventSource::new(pool.clone());
    let sink = dotlens_node::balances_pg::PgDeltaSink::new(pool.clone());
    let deps = ingest::balances::BalancesDeps {
        checkpoints: backends.checkpoints.as_ref(),
        source: &source,
        sink: &sink,
    };
    let n = ingest::balances::balances_range(&cfg.id, &SubstrateDeltaMapper, &deps, from, to)
        .await
        .with_context(|| format!("balances-range {chain} {from}..={to}"))?;
    tracing::info!(chain, from, to, mapped = n, "balances-range complete");
    println!("balances-range {chain} {from}..={to}: mapped {n} blocks");
    Ok(())
}

#[cfg(not(feature = "pg"))]
async fn run_balances_range(_: &Registry, _: &Backends, _: &str, _: u64, _: u64) -> Result<()> {
    anyhow::bail!("balances-range requires the `pg` feature")
}

/// anchor-balance <chain> <account> <height>: read System.Account from state
/// at `height`, decode against block-correct metadata, record an absolute
/// anchor. Anchors seed running totals — and are the honest bridge across
/// events we can't see (the Nov 2025 migration's bulk moves).
#[cfg(all(feature = "pg", feature = "live"))]
async fn run_anchor_balance(
    registry: &Registry,
    backends: &Backends,
    raw: &dyn RawStore,
    chain: &str,
    account: &str,
    height: u64,
) -> Result<()> {
    use adapter_substrate::{accounts, balances as ab, source::SubstrateSource};
    use ingest::live::ChainSource;

    let pool = backends
        .pool
        .as_ref()
        .context("anchor-balance requires DATABASE_URL")?;
    let cfg = registry
        .chain(chain)
        .with_context(|| format!("unknown chain: {chain}"))?;
    let account_id = accounts::parse_account(account)
        .map_err(|e| anyhow::anyhow!("bad account '{account}': {e}"))?;
    let source = SubstrateSource::new(&cfg.id, cfg.endpoints.rpc.clone())
        .map_err(|e| anyhow::anyhow!(e))?;
    let hash = source.block_hash(height).await.map_err(|e| anyhow::anyhow!(e))?;
    let spec = source
        .runtime_version_at(hash)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    // metadata: archived blob if we have it, else fetch AND archive (so the
    // anchor's lineage is reproducible from the raw store forever)
    let meta_key = raw_store::keys::metadata(&cfg.id, spec);
    let metadata = match raw.get(&meta_key) {
        Ok(blob) => blob,
        Err(raw_store::RawStoreError::NotFound(_)) => {
            let blob = source.metadata_at(height).await.map_err(|e| anyhow::anyhow!(e))?;
            raw.put(&meta_key, &blob, "anchor-balance")?;
            tracing::info!(chain = %cfg.id, spec, "metadata archived while anchoring");
            blob
        }
        Err(e) => return Err(e.into()),
    };

    let key = accounts::system_account_key(&account_id);
    let (balances, note) = match source
        .storage_at(&key, hash)
        .await
        .map_err(|e| anyhow::anyhow!(e))?
    {
        Some(bytes) => (
            ab::decode_account_info(&metadata, &bytes).map_err(|e| anyhow::anyhow!(e))?,
            None,
        ),
        None => (
            // absent from state = balance zero; recorded honestly as such
            ab::AccountBalances { free: 0, reserved: 0, frozen: None },
            Some("absent"),
        ),
    };
    dotlens_node::balances_pg::insert_anchor(
        pool,
        &cfg.id,
        &account_id,
        "native",
        height,
        &balances,
        Some(spec),
        "anchor-balance",
        note,
    )
    .await?;
    println!(
        "anchor {chain}/{account} at #{height} (spec {spec}): free={} reserved={} total={}{}",
        balances.free,
        balances.reserved,
        balances.total(),
        note.map(|n| format!(" [{n}]")).unwrap_or_default()
    );
    Ok(())
}

#[cfg(not(all(feature = "pg", feature = "live")))]
async fn run_anchor_balance(
    _: &Registry,
    _: &Backends,
    _: &dyn RawStore,
    _: &str,
    _: &str,
    _: u64,
) -> Result<()> {
    anyhow::bail!("anchor-balance requires the `pg` and `live` features")
}

async fn axum_serve(listener: tokio::net::TcpListener, app: axum::Router) -> Result<()> {
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .context("api server")
}
