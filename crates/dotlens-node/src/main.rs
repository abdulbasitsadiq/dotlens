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
//!   dotlens-node votes-range <chain> <a> <b>  # map votes + delegations over a range
//!   dotlens-node treasury-range <chain> <a> <b>  # map treasury spends + pot flows
//!   dotlens-node bounties-range <chain> <a> <b>  # map bounties (all three pallets)
//!   dotlens-node sync-bounty-accounts         # register indexed bounties' own accounts
//!   dotlens-node anchor-voting <chain> <acct> <track> [height]  # VotingFor state anchor
//!   dotlens-node sync-tracks                  # decode gov tracks from metadata, exit
//!   dotlens-node fetch-preimage <chain> <hash> <len> [height]  # fetch+decode one preimage
//!   dotlens-node decode-preimages <chain> [height]   # decode all pending proposals
//!   dotlens-node simulate-call <chain> <0x-call> <origin> [height]   # Tier 1 dry run
//!   dotlens-node simulate-referendum <chain> <class> <id> <origin> [height]
//!
//! `origin` is root | none | signed:<ss58|0x-hex> | <Pallet>:<Variant> (e.g.
//! `Origins:MediumSpender`). It is REQUIRED and never inferred from a track:
//! the track→origin map lives in runtime Rust, not in any artifact we index, so
//! guessing it would silently simulate the wrong thing. SIM_XCM_VERSION (default
//! 4) sets the XCM version returned programs are rendered in.
//!
//! PASS A HEIGHT if you want the answer to be reusable: results are keyed by the
//! block HASH they ran against, so omitting it pins the finalized head and every
//! run is a fresh state — correct, but never a cache hit.
//!
//!   dotlens-node whitelist-range <chain> <from> <to>   # map whitelist events
//!
//! Per-module followers are opt-in flags: LIVE_INGEST, DECODE_FOLLOW,
//! BALANCES_FOLLOW, GOV_FOLLOW, VOTES_FOLLOW, TREASURY_FOLLOW, BOUNTIES_FOLLOW,
//! WHITELIST_FOLLOW, TIP_FOLLOW (all `=1`).
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
    treasury: Arc<dyn api::TreasuryIndex>,
    bounties: Arc<dyn api::BountyIndex>,
    assets: Arc<dyn api::AssetIndex>,
    sim: Arc<dyn api::SimIndex>,
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
        treasury: Arc::new(api::MemoryTreasuryIndex::new()),
        bounties: Arc::new(api::MemoryBountyIndex::new()),
        assets: Arc::new(api::MemoryAssetIndex::new()),
        sim: Arc::new(api::MemorySimIndex::new()),
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
    VotesRange { chain: String, from: u64, to: u64 },
    TreasuryRange { chain: String, from: u64, to: u64 },
    BountiesRange { chain: String, from: u64, to: u64 },
    WhitelistRange { chain: String, from: u64, to: u64 },
    SyncBountyAccounts,
    AnchorVoting { chain: String, account: String, track: u32, height: Option<u64> },
    SyncTracks,
    SyncAssets { chain: String, height: Option<u64> },
    SyncTreasuryAccounts,
    TreasuryHoldings { chain: String, height: Option<u64> },
    FetchPreimage { chain: String, hash: String, len: u64, height: Option<u64> },
    DecodePreimages { chain: String, height: Option<u64> },
    SimulateCall {
        chain: String,
        call_hex: String,
        origin: String,
        height: Option<u64>,
    },
    SimulateReferendum {
        chain: String,
        class: String,
        referendum_id: i64,
        origin: String,
        height: Option<u64>,
    },
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
        Some("votes-range") => {
            let (chain, from, to) = range("usage: dotlens-node votes-range <chain> <from> <to>")?;
            Ok(Command::VotesRange { chain, from, to })
        }
        Some("treasury-range") => {
            let (chain, from, to) =
                range("usage: dotlens-node treasury-range <chain> <from> <to>")?;
            Ok(Command::TreasuryRange { chain, from, to })
        }
        Some("bounties-range") => {
            let (chain, from, to) =
                range("usage: dotlens-node bounties-range <chain> <from> <to>")?;
            Ok(Command::BountiesRange { chain, from, to })
        }
        Some("whitelist-range") => {
            let (chain, from, to) =
                range("usage: dotlens-node whitelist-range <chain> <from> <to>")?;
            Ok(Command::WhitelistRange { chain, from, to })
        }
        Some("sync-bounty-accounts") => Ok(Command::SyncBountyAccounts),
        Some("anchor-voting") => {
            let usage = "usage: dotlens-node anchor-voting <chain> <account> <track> [height]";
            let chain = args.get(1).context(usage)?.clone();
            let account = args.get(2).context(usage)?.clone();
            let track: u32 = args.get(3).context(usage)?.parse().context(usage)?;
            anyhow::ensure!(track <= u16::MAX as u32, "track id must fit in u16");
            let height = match args.get(4) {
                Some(h) => Some(h.parse::<u64>().context(usage)?),
                None => None,
            };
            Ok(Command::AnchorVoting { chain, account, track, height })
        }
        Some("sync-tracks") => Ok(Command::SyncTracks),
        Some("sync-treasury-accounts") => Ok(Command::SyncTreasuryAccounts),
        Some("sync-assets") => {
            let usage = "usage: dotlens-node sync-assets <chain> [height]";
            let chain = args.get(1).context(usage)?.clone();
            let height = match args.get(2) {
                Some(h) => Some(h.parse::<u64>().context(usage)?),
                None => None,
            };
            Ok(Command::SyncAssets { chain, height })
        }
        Some("treasury-holdings") => {
            let usage = "usage: dotlens-node treasury-holdings <chain> [height]";
            let chain = args.get(1).context(usage)?.clone();
            let height = match args.get(2) {
                Some(h) => Some(h.parse::<u64>().context(usage)?),
                None => None,
            };
            Ok(Command::TreasuryHoldings { chain, height })
        }
        Some("fetch-preimage") => {
            let usage = "usage: dotlens-node fetch-preimage <chain> <hash> <len> [height]";
            let chain = args.get(1).context(usage)?.clone();
            let hash = args.get(2).context(usage)?.clone();
            let len: u64 = args.get(3).context(usage)?.parse().context(usage)?;
            let height = match args.get(4) {
                Some(h) => Some(h.parse::<u64>().context(usage)?),
                None => None,
            };
            Ok(Command::FetchPreimage { chain, hash, len, height })
        }
        Some("decode-preimages") => {
            let usage = "usage: dotlens-node decode-preimages <chain> [height]";
            let chain = args.get(1).context(usage)?.clone();
            let height = match args.get(2) {
                Some(h) => Some(h.parse::<u64>().context(usage)?),
                None => None,
            };
            Ok(Command::DecodePreimages { chain, height })
        }
        Some("simulate-call") => {
            let usage =
                "usage: dotlens-node simulate-call <chain> <0x-call-hex> <origin> [height]\n\
                 origin: root | none | signed:<ss58|0x-hex> | <Pallet>:<Variant>";
            let chain = args.get(1).context(usage)?.clone();
            let call_hex = args.get(2).context(usage)?.clone();
            let origin = args.get(3).context(usage)?.clone();
            let height = match args.get(4) {
                Some(h) => Some(h.parse::<u64>().context(usage)?),
                None => None,
            };
            Ok(Command::SimulateCall { chain, call_hex, origin, height })
        }
        Some("simulate-referendum") => {
            let usage =
                "usage: dotlens-node simulate-referendum <chain> <class> <id> <origin> [height]\n\
                 origin: root | none | signed:<ss58|0x-hex> | <Pallet>:<Variant>";
            let chain = args.get(1).context(usage)?.clone();
            let class = args.get(2).context(usage)?.clone();
            let referendum_id: i64 = args.get(3).context(usage)?.parse().context(usage)?;
            let origin = args.get(4).context(usage)?.clone();
            let height = match args.get(5) {
                Some(h) => Some(h.parse::<u64>().context(usage)?),
                None => None,
            };
            Ok(Command::SimulateReferendum { chain, class, referendum_id, origin, height })
        }
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
            treasury: Arc::new(api::pg::PgTreasuryIndex::new(pool.clone())),
            bounties: Arc::new(api::pg::PgBountyIndex::new(pool.clone())),
            assets: Arc::new(api::pg::PgAssetIndex::new(pool.clone())),
            sim: Arc::new(api::pg::PgSimIndex::new(pool.clone())),
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
    if let Command::VotesRange { chain, from, to } = &command {
        anyhow::ensure!(
            backends.persistent,
            "votes-range requires DATABASE_URL (canonical events + vote facts must persist)"
        );
        return run_votes_range(&registry, &backends, chain, *from, *to).await;
    }
    if let Command::TreasuryRange { chain, from, to } = &command {
        anyhow::ensure!(
            backends.persistent,
            "treasury-range requires DATABASE_URL (canonical events + spend facts must persist)"
        );
        return run_treasury_range(&registry, &backends, chain, *from, *to).await;
    }
    if let Command::BountiesRange { chain, from, to } = &command {
        anyhow::ensure!(
            backends.persistent,
            "bounties-range requires DATABASE_URL (canonical events + bounty facts must persist)"
        );
        return run_bounties_range(&registry, &backends, raw.as_ref(), chain, *from, *to).await;
    }
    if let Command::WhitelistRange { chain, from, to } = &command {
        anyhow::ensure!(
            backends.persistent,
            "whitelist-range requires DATABASE_URL (canonical events + whitelist facts must persist)"
        );
        return run_whitelist_range(&registry, &backends, chain, *from, *to).await;
    }
    if matches!(command, Command::SyncBountyAccounts) {
        #[cfg(feature = "pg")]
        if let Some(pool) = &backends.pool {
            let report = dotlens_node::bounties_pg::sync_bounty_accounts(
                pool,
                registry.as_ref(),
                raw.as_ref(),
            )
            .await?;
            println!("bounty account sync: {report:?}");
            return Ok(());
        }
        anyhow::bail!("sync-bounty-accounts requires the `pg` feature and DATABASE_URL");
    }
    if let Command::AnchorVoting { chain, account, track, height } = &command {
        return run_anchor_voting(
            &registry, &backends, raw.as_ref(), chain, account, *track, *height,
        )
        .await;
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
    if matches!(command, Command::SyncTreasuryAccounts) {
        #[cfg(feature = "pg")]
        if let Some(pool) = &backends.pool {
            let report = dotlens_node::assets_pg::sync_treasury_accounts(
                pool,
                registry.as_ref(),
                raw.as_ref(),
            )
            .await?;
            println!("treasury account sync: {report:?}");
            return Ok(());
        }
        anyhow::bail!("sync-treasury-accounts requires the `pg` feature and DATABASE_URL");
    }
    if let Command::SyncAssets { chain, height } = &command {
        return run_sync_assets(&registry, &backends, raw.as_ref(), chain, *height).await;
    }
    if let Command::TreasuryHoldings { chain, height } = &command {
        return run_treasury_holdings(&registry, &backends, raw.as_ref(), chain, *height).await;
    }
    if let Command::FetchPreimage { chain, hash, len, height } = &command {
        return run_fetch_preimage(&registry, &backends, raw.as_ref(), chain, hash, *len, *height)
            .await;
    }
    if let Command::DecodePreimages { chain, height } = &command {
        return run_decode_preimages(&registry, &backends, raw.as_ref(), chain, *height).await;
    }
    if let Command::SimulateCall { chain, call_hex, origin, height } = &command {
        let bytes = decode_call_hex(call_hex)?;
        return run_simulate(
            &registry, &backends, raw.as_ref(), chain, bytes, origin, *height, None,
        )
        .await;
    }
    if let Command::SimulateReferendum { chain, class, referendum_id, origin, height } = &command {
        return run_simulate_referendum(
            &registry, &backends, raw.as_ref(), chain, class, *referendum_id, origin, *height,
        )
        .await;
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
        // the treasury ACCOUNT list, derived from the same metadata the labels
        // came from — so a chain that gains a treasury pallet contributes its
        // pot on the next start, with nothing typed by hand
        let ta = dotlens_node::assets_pg::sync_treasury_accounts(
            pool,
            registry.as_ref(),
            raw.as_ref(),
        )
        .await?;
        tracing::info!(
            pots = ta.pots,
            seeded = ta.seeded,
            missing_metadata = ?ta.chains_missing_metadata,
            "treasury accounts synced"
        );
        // …and every bounty we have indexed contributes its OWN account, which
        // is where bounty money actually sits. Runs here rather than only on
        // demand because the list has to grow as bounties are created, and
        // because holdings sweeps read whatever is in the table.
        let ba = dotlens_node::bounties_pg::sync_bounty_accounts(
            pool,
            registry.as_ref(),
            raw.as_ref(),
        )
        .await?;
        tracing::info!(
            accounts = ba.accounts,
            deactivated = ba.deactivated,
            linked = ba.linked,
            underivable = ba.underivable,
            missing_metadata = ?ba.chains_missing_metadata,
            no_treasury_pallet = ?ba.chains_without_treasury_pallet,
            "bounty accounts synced"
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
    spawn_votes_followers(&registry, &backends);
    spawn_treasury_followers(&registry, &backends);
    spawn_bounties_followers(&registry, &backends, &raw);
    spawn_whitelist_followers(&registry, &backends);
    spawn_tip_followers(&registry, &backends, &raw);

    // -- API ------------------------------------------------------------------
    let bind = env_or("API_BIND", "127.0.0.1:8080");
    let app = api::router(AppState {
        registry,
        blocks: backends.blocks.clone(),
        labels: backends.labels.clone(),
        balances: backends.balances.clone(),
        gov: backends.gov.clone(),
        treasury: backends.treasury.clone(),
        bounties: backends.bounties.clone(),
        assets: backends.assets.clone(),
        sim: backends.sim.clone(),
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

/// Votes followers: chase each chain's decode checkpoint, mapping canonical
/// conviction-voting / ranked-collective events into vote + delegation facts.
/// Pure mapping over Pg — no network, `pg` only. Eligibility is registry data:
/// the chain must enable the `governance` module.
fn spawn_votes_followers(registry: &Arc<Registry>, backends: &Arc<Backends>) {
    if !env_flag("VOTES_FOLLOW") {
        tracing::info!("votes follower disabled (set VOTES_FOLLOW=1 to enable)");
        return;
    }
    if !backends.persistent {
        tracing::warn!("VOTES_FOLLOW=1 but no DATABASE_URL — refusing to map into memory");
        return;
    }
    #[cfg(feature = "pg")]
    {
        use adapter_substrate::votes::SubstrateVoteMapper;

        let poll = std::time::Duration::from_secs(
            env_or("POLL_INTERVAL_SECS", "6").parse().unwrap_or(6),
        );
        for chain in registry.chains() {
            if !chain.has_module("governance") {
                continue;
            }
            if chain.family != registry::ChainFamily::Substrate {
                tracing::debug!(chain = %chain.id, "no votes mapper for this family — skipped");
                continue;
            }
            let Some(pool) = backends.pool.clone() else { continue };
            let chain_id = chain.id.clone();
            let backends = backends.clone();
            tokio::spawn(async move {
                tracing::info!(chain = %chain_id, "votes follower started");
                let source = dotlens_node::balances_pg::PgEventSource::new(pool.clone());
                let sink = dotlens_node::votes_pg::PgVoteSink::new(pool);
                let deps = ingest::votes::VotesDeps {
                    checkpoints: backends.checkpoints.as_ref(),
                    source: &source,
                    sink: &sink,
                };
                ingest::votes::votes_follow(&chain_id, &SubstrateVoteMapper, &deps, poll).await;
            });
        }
    }
}

#[cfg(feature = "pg")]
async fn run_votes_range(
    registry: &Registry,
    backends: &Backends,
    chain: &str,
    from: u64,
    to: u64,
) -> Result<()> {
    use adapter_substrate::votes::SubstrateVoteMapper;

    let cfg = registry
        .chain(chain)
        .with_context(|| format!("unknown chain: {chain}"))?;
    anyhow::ensure!(
        cfg.family == registry::ChainFamily::Substrate,
        "no votes mapper for family {:?}",
        cfg.family
    );
    let pool = backends.pool.as_ref().context("votes-range requires DATABASE_URL")?;
    let source = dotlens_node::balances_pg::PgEventSource::new(pool.clone());
    let sink = dotlens_node::votes_pg::PgVoteSink::new(pool.clone());
    let deps = ingest::votes::VotesDeps {
        checkpoints: backends.checkpoints.as_ref(),
        source: &source,
        sink: &sink,
    };
    let n = ingest::votes::votes_range(&cfg.id, &SubstrateVoteMapper, &deps, from, to)
        .await
        .with_context(|| format!("votes-range {chain} {from}..={to}"))?;
    tracing::info!(chain, from, to, mapped = n, "votes-range complete");
    println!("votes-range {chain} {from}..={to}: mapped {n} blocks");
    Ok(())
}

#[cfg(not(feature = "pg"))]
async fn run_votes_range(_: &Registry, _: &Backends, _: &str, _: u64, _: u64) -> Result<()> {
    anyhow::bail!("votes-range requires the `pg` feature")
}

/// anchor-voting <chain> <account> <track> [height]: read
/// ConvictionVoting.VotingFor(account, track) from state, decode against
/// block-correct metadata, record an immutable anchor. This is the ONLY source
/// for delegation AMOUNTS and for delegated power RECEIVED — no event carries
/// either (see adapter_substrate::votes docs).
#[cfg(all(feature = "pg", feature = "live"))]
#[allow(clippy::too_many_arguments)]
async fn run_anchor_voting(
    registry: &Registry,
    backends: &Backends,
    raw: &dyn RawStore,
    chain: &str,
    account: &str,
    track: u32,
    height: Option<u64>,
) -> Result<()> {
    use adapter_substrate::{accounts, source::SubstrateSource, votes as av};
    use ingest::live::ChainSource;

    let pool = backends
        .pool
        .as_ref()
        .context("anchor-voting requires DATABASE_URL")?;
    let cfg = registry
        .chain(chain)
        .with_context(|| format!("unknown chain: {chain}"))?;
    let account_id = accounts::parse_account(account)
        .map_err(|e| anyhow::anyhow!("bad account '{account}': {e}"))?;
    let track_u16 = u16::try_from(track).context("track id must fit in u16")?;
    let source = SubstrateSource::new(&cfg.id, cfg.endpoints.rpc.clone())
        .map_err(|e| anyhow::anyhow!(e))?;

    let height = match height {
        Some(h) => h,
        None => source.finalized_height().await.map_err(|e| anyhow::anyhow!(e))?,
    };
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
            raw.put(&meta_key, &blob, "anchor-voting")?;
            tracing::info!(chain = %cfg.id, spec, "metadata archived while anchoring votes");
            blob
        }
        Err(e) => return Err(e.into()),
    };

    let key = av::voting_for_key(&account_id, track_u16);
    let (position, note) = match source
        .storage_at(&key, hash)
        .await
        .map_err(|e| anyhow::anyhow!(e))?
    {
        Some(bytes) => (
            av::decode_voting_for(&metadata, &bytes).map_err(|e| anyhow::anyhow!(e))?,
            None,
        ),
        // ValueQuery: no entry means the storage DEFAULT (casting nothing),
        // recorded honestly as such rather than skipped
        None => (av::VotingPosition::empty(), Some("absent")),
    };
    dotlens_node::votes_pg::insert_voting_anchor(
        pool,
        &cfg.id,
        &account_id,
        // the ConvictionVoting instance is the public OpenGov class
        "referenda",
        track,
        height,
        &position,
        Some(spec),
        "anchor-voting",
        note,
    )
    .await?;
    println!(
        "voting anchor {chain}/{account} track {track} at #{height} (spec {spec}): mode={} \
         delegating={:?} conviction={:?} received_votes={:?}{}",
        position.mode,
        position.delegating_balance,
        position.delegating_conviction_label,
        position.delegations_votes,
        note.map(|n| format!(" [{n}]")).unwrap_or_default()
    );
    Ok(())
}

#[cfg(not(all(feature = "pg", feature = "live")))]
#[allow(clippy::too_many_arguments)]
async fn run_anchor_voting(
    _: &Registry,
    _: &Backends,
    _: &dyn RawStore,
    _: &str,
    _: &str,
    _: u32,
    _: Option<u64>,
) -> Result<()> {
    anyhow::bail!("anchor-voting requires the `pg` and `live` features")
}

/// Treasury followers: chase each chain's decode checkpoint, mapping canonical
/// treasury-pallet events into spend facts and pot flows. Pure mapping over Pg
/// — no network, `pg` only. Eligibility is registry data: the chain must enable
/// the `treasury` module.
fn spawn_treasury_followers(registry: &Arc<Registry>, backends: &Arc<Backends>) {
    if !env_flag("TREASURY_FOLLOW") {
        tracing::info!("treasury follower disabled (set TREASURY_FOLLOW=1 to enable)");
        return;
    }
    if !backends.persistent {
        tracing::warn!("TREASURY_FOLLOW=1 but no DATABASE_URL — refusing to map into memory");
        return;
    }
    #[cfg(feature = "pg")]
    {
        use adapter_substrate::treasury::SubstrateTreasuryMapper;

        let poll = std::time::Duration::from_secs(
            env_or("POLL_INTERVAL_SECS", "6").parse().unwrap_or(6),
        );
        for chain in registry.chains() {
            if !chain.has_module("treasury") {
                continue;
            }
            if chain.family != registry::ChainFamily::Substrate {
                tracing::debug!(chain = %chain.id, "no treasury mapper for this family — skipped");
                continue;
            }
            let Some(pool) = backends.pool.clone() else { continue };
            let chain_id = chain.id.clone();
            let backends = backends.clone();
            tokio::spawn(async move {
                tracing::info!(chain = %chain_id, "treasury follower started");
                let source = dotlens_node::balances_pg::PgEventSource::new(pool.clone());
                let sink = dotlens_node::treasury_pg::PgSpendSink::new(pool);
                let deps = ingest::treasury::TreasuryDeps {
                    checkpoints: backends.checkpoints.as_ref(),
                    source: &source,
                    sink: &sink,
                };
                ingest::treasury::treasury_follow(&chain_id, &SubstrateTreasuryMapper, &deps, poll)
                    .await;
            });
        }
    }
}

#[cfg(feature = "pg")]
async fn run_treasury_range(
    registry: &Registry,
    backends: &Backends,
    chain: &str,
    from: u64,
    to: u64,
) -> Result<()> {
    use adapter_substrate::treasury::SubstrateTreasuryMapper;

    let cfg = registry
        .chain(chain)
        .with_context(|| format!("unknown chain: {chain}"))?;
    anyhow::ensure!(
        cfg.family == registry::ChainFamily::Substrate,
        "no treasury mapper for family {:?}",
        cfg.family
    );
    let pool = backends
        .pool
        .as_ref()
        .context("treasury-range requires DATABASE_URL")?;
    let source = dotlens_node::balances_pg::PgEventSource::new(pool.clone());
    let sink = dotlens_node::treasury_pg::PgSpendSink::new(pool.clone());
    let deps = ingest::treasury::TreasuryDeps {
        checkpoints: backends.checkpoints.as_ref(),
        source: &source,
        sink: &sink,
    };
    let n = ingest::treasury::treasury_range(&cfg.id, &SubstrateTreasuryMapper, &deps, from, to)
        .await
        .with_context(|| format!("treasury-range {chain} {from}..={to}"))?;
    tracing::info!(chain, from, to, mapped = n, "treasury-range complete");
    println!("treasury-range {chain} {from}..={to}: mapped {n} blocks");
    Ok(())
}

#[cfg(not(feature = "pg"))]
async fn run_treasury_range(_: &Registry, _: &Backends, _: &str, _: u64, _: u64) -> Result<()> {
    anyhow::bail!("treasury-range requires the `pg` feature")
}

/// Bounty followers: chase each chain's decode checkpoint, mapping all three
/// bounty pallets into one table. Pure mapping over Pg — no network.
///
/// ELIGIBILITY IS THE `treasury` MODULE, not a `bounties` one, and that is a
/// claim rather than a shortcut: bounty funds are sub-accounts of the TREASURY's
/// pallet id and bounty funding is a treasury outflow, so a chain that carries
/// the treasury is exactly the chain that can carry its bounties. Giving them a
/// separate module would invite a chain to declare one without the other, which
/// would describe nothing real.
#[cfg_attr(not(feature = "pg"), allow(unused_variables))]
fn spawn_bounties_followers(
    registry: &Arc<Registry>,
    backends: &Arc<Backends>,
    raw: &Arc<dyn RawStore>,
) {
    if !env_flag("BOUNTIES_FOLLOW") {
        tracing::info!("bounties follower disabled (set BOUNTIES_FOLLOW=1 to enable)");
        return;
    }
    if !backends.persistent {
        tracing::warn!("BOUNTIES_FOLLOW=1 but no DATABASE_URL — refusing to map into memory");
        return;
    }
    #[cfg(feature = "pg")]
    {
        use adapter_substrate::bounties::SubstrateBountyMapper;

        let poll = std::time::Duration::from_secs(
            env_or("POLL_INTERVAL_SECS", "6").parse().unwrap_or(6),
        );
        for chain in registry.chains() {
            if !chain.has_module("treasury") {
                continue;
            }
            if chain.family != registry::ChainFamily::Substrate {
                tracing::debug!(chain = %chain.id, "no bounty mapper for this family — skipped");
                continue;
            }
            let Some(pool) = backends.pool.clone() else { continue };
            let chain_id = chain.id.clone();
            let backends = backends.clone();
            let raw = raw.clone();
            tokio::spawn(async move {
                // read ONCE per follower, not per event: the treasury PalletId
                // is a property of the runtime, and re-decoding metadata in the
                // write path is slice 6's hoisted defect
                let pallet_id = match dotlens_node::bounties_pg::treasury_pallet_id(
                    &pool,
                    raw.as_ref(),
                    &chain_id,
                )
                .await
                {
                    Ok(id) => id.found(),
                    Err(e) => {
                        tracing::warn!(chain = %chain_id, error = %e,
                            "treasury PalletId unavailable — bounty accounts stay null \
                             until sync-bounty-accounts runs");
                        None
                    }
                };
                tracing::info!(chain = %chain_id, derives_accounts = pallet_id.is_some(),
                    "bounties follower started");
                let source = dotlens_node::balances_pg::PgEventSource::new(pool.clone());
                let sink = dotlens_node::bounties_pg::PgBountySink::new(pool, pallet_id);
                let deps = ingest::bounties::BountyDeps {
                    checkpoints: backends.checkpoints.as_ref(),
                    source: &source,
                    sink: &sink,
                };
                ingest::bounties::bounties_follow(&chain_id, &SubstrateBountyMapper, &deps, poll)
                    .await;
            });
        }
    }
}

#[cfg(feature = "pg")]
async fn run_bounties_range(
    registry: &Registry,
    backends: &Backends,
    raw: &dyn RawStore,
    chain: &str,
    from: u64,
    to: u64,
) -> Result<()> {
    use adapter_substrate::bounties::SubstrateBountyMapper;

    let cfg = registry
        .chain(chain)
        .with_context(|| format!("unknown chain: {chain}"))?;
    anyhow::ensure!(
        cfg.family == registry::ChainFamily::Substrate,
        "no bounty mapper for family {:?}",
        cfg.family
    );
    let pool = backends
        .pool
        .as_ref()
        .context("bounties-range requires DATABASE_URL")?;
    let pallet_id = dotlens_node::bounties_pg::treasury_pallet_id(pool, raw, &cfg.id)
        .await?
        .found();
    if pallet_id.is_none() {
        tracing::warn!(
            chain,
            "no archived metadata carrying a treasury PalletId — bounty rows will land \
             with a null account_id; run `sync-bounty-accounts` once metadata exists"
        );
    }
    let source = dotlens_node::balances_pg::PgEventSource::new(pool.clone());
    let sink = dotlens_node::bounties_pg::PgBountySink::new(pool.clone(), pallet_id);
    let deps = ingest::bounties::BountyDeps {
        checkpoints: backends.checkpoints.as_ref(),
        source: &source,
        sink: &sink,
    };
    let n = ingest::bounties::bounties_range(&cfg.id, &SubstrateBountyMapper, &deps, from, to)
        .await
        .with_context(|| format!("bounties-range {chain} {from}..={to}"))?;
    tracing::info!(chain, from, to, mapped = n, "bounties-range complete");
    println!("bounties-range {chain} {from}..={to}: mapped {n} blocks");
    Ok(())
}

#[cfg(not(feature = "pg"))]
async fn run_bounties_range(
    _: &Registry,
    _: &Backends,
    _: &dyn RawStore,
    _: &str,
    _: u64,
    _: u64,
) -> Result<()> {
    anyhow::bail!("bounties-range requires the `pg` feature")
}

/// Whitelist followers: chase each chain's decode checkpoint, mapping
/// pallet-whitelist events into whitelisted-call facts. Pure mapping over Pg —
/// no network, `pg` only. Eligibility is registry data: the chain must enable
/// the `governance` module, which is why this needed no registry change.
///
/// It runs on Collectives too, where the pallet does not exist — a follower
/// that maps nothing. That is correct rather than wasteful: whether a chain
/// carries the pallet is a fact about its runtime, not a fact for a seed file
/// to assert, and the day Collectives gains one it is already covered.
fn spawn_whitelist_followers(registry: &Arc<Registry>, backends: &Arc<Backends>) {
    if !env_flag("WHITELIST_FOLLOW") {
        tracing::info!("whitelist follower disabled (set WHITELIST_FOLLOW=1 to enable)");
        return;
    }
    if !backends.persistent {
        tracing::warn!("WHITELIST_FOLLOW=1 but no DATABASE_URL — refusing to map into memory");
        return;
    }
    #[cfg(feature = "pg")]
    {
        use adapter_substrate::whitelist::SubstrateWhitelistMapper;

        let poll = std::time::Duration::from_secs(
            env_or("POLL_INTERVAL_SECS", "6").parse().unwrap_or(6),
        );
        for chain in registry.chains() {
            if !chain.has_module("governance") {
                continue;
            }
            if chain.family != registry::ChainFamily::Substrate {
                tracing::debug!(chain = %chain.id, "no whitelist mapper for this family — skipped");
                continue;
            }
            let Some(pool) = backends.pool.clone() else { continue };
            let chain_id = chain.id.clone();
            let backends = backends.clone();
            tokio::spawn(async move {
                tracing::info!(chain = %chain_id, "whitelist follower started");
                let source = dotlens_node::balances_pg::PgEventSource::new(pool.clone());
                let sink = dotlens_node::whitelist_pg::PgWhitelistSink::new(pool);
                let deps = ingest::whitelist::WhitelistDeps {
                    checkpoints: backends.checkpoints.as_ref(),
                    source: &source,
                    sink: &sink,
                };
                ingest::whitelist::whitelist_follow(
                    &chain_id,
                    &SubstrateWhitelistMapper,
                    &deps,
                    poll,
                )
                .await;
            });
        }
    }
}

#[cfg(feature = "pg")]
async fn run_whitelist_range(
    registry: &Registry,
    backends: &Backends,
    chain: &str,
    from: u64,
    to: u64,
) -> Result<()> {
    use adapter_substrate::whitelist::SubstrateWhitelistMapper;

    let cfg = registry
        .chain(chain)
        .with_context(|| format!("unknown chain: {chain}"))?;
    anyhow::ensure!(
        cfg.family == registry::ChainFamily::Substrate,
        "no whitelist mapper for family {:?}",
        cfg.family
    );
    let pool = backends
        .pool
        .as_ref()
        .context("whitelist-range requires DATABASE_URL")?;
    let source = dotlens_node::balances_pg::PgEventSource::new(pool.clone());
    let sink = dotlens_node::whitelist_pg::PgWhitelistSink::new(pool.clone());
    let deps = ingest::whitelist::WhitelistDeps {
        checkpoints: backends.checkpoints.as_ref(),
        source: &source,
        sink: &sink,
    };
    let n = ingest::whitelist::whitelist_range(&cfg.id, &SubstrateWhitelistMapper, &deps, from, to)
        .await
        .with_context(|| format!("whitelist-range {chain} {from}..={to}"))?;
    tracing::info!(chain, from, to, mapped = n, "whitelist-range complete");
    println!("whitelist-range {chain} {from}..={to}: mapped {n} blocks");
    Ok(())
}

#[cfg(not(feature = "pg"))]
async fn run_whitelist_range(_: &Registry, _: &Backends, _: &str, _: u64, _: u64) -> Result<()> {
    anyhow::bail!("whitelist-range requires the `pg` feature")
}

/// Everything one preimage decode needs from a block context: the archived
/// (or fetched-and-archived) metadata plus lineage.
#[cfg(all(feature = "pg", feature = "live"))]
struct PreimageCtx {
    metadata: Vec<u8>,
    spec: u32,
    block_hash: adapter_substrate::source::BlockHash,
    height: u64,
}

#[cfg(all(feature = "pg", feature = "live"))]
async fn preimage_ctx(
    source: &adapter_substrate::source::SubstrateSource,
    raw: &dyn RawStore,
    chain_id: &str,
    height: Option<u64>,
) -> Result<PreimageCtx> {
    use ingest::live::ChainSource;

    let height = match height {
        Some(h) => h,
        None => source.finalized_height().await.map_err(|e| anyhow::anyhow!(e))?,
    };
    let block_hash = source.block_hash(height).await.map_err(|e| anyhow::anyhow!(e))?;
    let spec = source
        .runtime_version_at(block_hash)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    // archived blob if we have it, else fetch AND archive (lineage stays
    // reproducible from the raw store — same discipline as anchor-balance)
    let meta_key = raw_store::keys::metadata(chain_id, spec);
    let metadata = match raw.get(&meta_key) {
        Ok(blob) => blob,
        Err(raw_store::RawStoreError::NotFound(_)) => {
            let blob = source.metadata_at(height).await.map_err(|e| anyhow::anyhow!(e))?;
            raw.put(&meta_key, &blob, "preimage-decode")?;
            tracing::info!(chain = %chain_id, spec, "metadata archived while decoding preimages");
            blob
        }
        Err(e) => return Err(e.into()),
    };
    Ok(PreimageCtx { metadata, spec, block_hash, height })
}

/// Fetch one preimage from state, archive the raw value, verify the hash,
/// decode the call tree, record the row. Returns the decode_status recorded.
#[cfg(all(feature = "pg", feature = "live"))]
async fn fetch_and_record_preimage(
    pool: &sqlx::PgPool,
    receipts: &dyn ingest::ReceiptSink,
    source: &adapter_substrate::source::SubstrateSource,
    raw: &dyn RawStore,
    chain_id: &str,
    ctx: &PreimageCtx,
    hash32: &[u8; 32],
    len: u64,
) -> Result<String> {
    use adapter_substrate::{calls, gov as agov};
    use dotlens_node::gov_pg::{upsert_preimage, PreimageRecord};

    let hash_hex = format!("0x{}", hex::encode(hash32));
    let record = |status: &str,
                  bytes_location: Option<String>,
                  decoded: Option<calls::DecodedCall>,
                  note: Option<String>| PreimageRecord {
        proposal_hash: hash_hex.clone(),
        len,
        bytes_location,
        call_summary: decoded.as_ref().map(|d| d.summary.clone()),
        decoded_call: decoded.map(|d| d.tree),
        decode_status: status.to_string(),
        source: "state".to_string(),
        note,
        spec_version: Some(ctx.spec),
        decoder_version: calls::CALL_DECODER_VERSION,
        fetched_at_height: Some(ctx.height),
    };

    let key = agov::preimage_for_key(hash32, u32::try_from(len).context("len exceeds u32")?);
    let Some(value) = source
        .storage_at(&key, ctx.block_hash)
        .await
        .map_err(|e| anyhow::anyhow!(e))?
    else {
        // cleared after enactment, or never noted — honest coverage, not an error
        upsert_preimage(pool, chain_id, &record("missing", None, None, None)).await?;
        return Ok("missing".into());
    };

    // the stored value is BoundedVec<u8>: compact length prefix + call bytes
    let call_bytes = <Vec<u8> as parity_scale_codec::Decode>::decode(&mut &value[..])
        .map_err(|e| anyhow::anyhow!("preimage value is not a BoundedVec<u8>: {e}"))?;
    // verify BEFORE archiving (review catch): the raw store is write-once, so
    // a garbage endpoint response archived under this key would poison it
    // forever — a value failing these checks is by definition NOT the
    // preimage for this key, so raw-first loses nothing by rejecting it.
    // Deliberate (documented): a mismatch aborts the whole batch loudly
    // rather than recording 'undecodable' — never file corrupt bytes as data.
    anyhow::ensure!(
        call_bytes.len() as u64 == len,
        "preimage length mismatch: state has {} bytes, key says {len}",
        call_bytes.len()
    );
    let computed = calls::blake2_256(&call_bytes);
    anyhow::ensure!(
        &computed == hash32,
        "preimage hash mismatch: blake2(bytes) = 0x{} ≠ {hash_hex} — refusing to record",
        hex::encode(computed)
    );
    let raw_key = raw_store::keys::preimage(chain_id, &hex::encode(hash32), len);
    let receipt = raw.put(&raw_key, &value, "fetch-preimage")?;
    if let Err(e) = receipts.record(&receipt).await {
        tracing::warn!(error = %e, "preimage receipt not recorded — continuing");
    }

    match calls::decode_call(&ctx.metadata, &call_bytes) {
        Ok(d) => {
            upsert_preimage(pool, chain_id, &record("decoded", Some(raw_key), Some(d), None))
                .await?;
            Ok("decoded".into())
        }
        Err(e) => {
            // bytes are archived; a future decoder version rebuilds from raw
            upsert_preimage(
                pool,
                chain_id,
                &record("undecodable", Some(raw_key), None, Some(e)),
            )
            .await?;
            Ok("undecodable".into())
        }
    }
}

#[cfg(all(feature = "pg", feature = "live"))]
async fn run_fetch_preimage(
    registry: &Registry,
    backends: &Backends,
    raw: &dyn RawStore,
    chain: &str,
    hash: &str,
    len: u64,
    height: Option<u64>,
) -> Result<()> {
    use adapter_substrate::source::SubstrateSource;

    let pool = backends.pool.as_ref().context("fetch-preimage requires DATABASE_URL")?;
    let cfg = registry
        .chain(chain)
        .with_context(|| format!("unknown chain: {chain}"))?;
    let hash32 = parse_h256(hash)?;
    let source = SubstrateSource::new(&cfg.id, cfg.endpoints.rpc.clone())
        .map_err(|e| anyhow::anyhow!(e))?;
    let ctx = preimage_ctx(&source, raw, &cfg.id, height).await?;
    let status = fetch_and_record_preimage(
        pool, backends.receipts.as_ref(), &source, raw, &cfg.id, &ctx, &hash32, len,
    )
    .await?;
    println!(
        "preimage {chain}/{hash} len {len} at #{} (spec {}): {status}",
        ctx.height, ctx.spec
    );
    Ok(())
}

/// decode-preimages <chain> [height]: work through every referendum proposal
/// on the chain that has no decoded preimage yet — Inline bytes decode
/// directly, Lookup hashes fetch from state, Legacy (democracy-era, length
/// unknown) records an honest 'missing'.
#[cfg(all(feature = "pg", feature = "live"))]
async fn run_decode_preimages(
    registry: &Registry,
    backends: &Backends,
    raw: &dyn RawStore,
    chain: &str,
    height: Option<u64>,
) -> Result<()> {
    use adapter_substrate::{calls, source::SubstrateSource};
    use dotlens_node::gov_pg::{
        fill_inline_proposal_hash, referenda_needing_preimages, upsert_preimage, PreimageRecord,
    };

    let pool = backends.pool.as_ref().context("decode-preimages requires DATABASE_URL")?;
    let cfg = registry
        .chain(chain)
        .with_context(|| format!("unknown chain: {chain}"))?;
    let source = SubstrateSource::new(&cfg.id, cfg.endpoints.rpc.clone())
        .map_err(|e| anyhow::anyhow!(e))?;
    let ctx = preimage_ctx(&source, raw, &cfg.id, height).await?;

    let rows = referenda_needing_preimages(pool, &cfg.id).await?;
    let (mut decoded, mut missing, mut undecodable, mut inline, mut legacy) =
        (0u32, 0u32, 0u32, 0u32, 0u32);
    for (class, referendum_id, proposal, proposal_hash, proposal_len, submitted_at) in &rows {
        if let Some(inline_body) = proposal.get("Inline") {
            // bytes travel in the referendum itself — no state fetch needed
            let Some(bytes) = calls::json_bytes(inline_body) else {
                anyhow::bail!(
                    "{}/{class}/{referendum_id}: Inline proposal bytes unreadable — \
                     mapper output corrupt?",
                    cfg.id
                );
            };
            // decode against SUBMISSION-era metadata when we know the height:
            // call indices reshuffle across upgrades, and tip metadata could
            // decode old inline bytes successfully-but-WRONG (review catch).
            // Pruned endpoints may refuse old-state queries → loud warn +
            // tip fallback (spec_version lineage keeps it auditable).
            let ref_ctx_owned: Option<PreimageCtx> = match submitted_at {
                Some(h) => match preimage_ctx(&source, raw, &cfg.id, Some(*h as u64)).await {
                    Ok(c) => Some(c),
                    Err(e) => {
                        tracing::warn!(chain = %cfg.id, class, referendum_id, height = h,
                            error = %e,
                            "submission-era metadata unavailable — decoding against tip");
                        None
                    }
                },
                None => None,
            };
            let ref_ctx = ref_ctx_owned.as_ref().unwrap_or(&ctx);
            let hash_hex = format!("0x{}", hex::encode(calls::blake2_256(&bytes)));
            let len = bytes.len() as u64;
            let (status, d, note) = match calls::decode_call(&ref_ctx.metadata, &bytes) {
                Ok(d) => {
                    inline += 1;
                    ("decoded", Some(d), None)
                }
                Err(e) => {
                    undecodable += 1;
                    ("undecodable", None, Some(e))
                }
            };
            upsert_preimage(pool, &cfg.id, &PreimageRecord {
                proposal_hash: hash_hex.clone(),
                len,
                bytes_location: None, // bytes live in gov.referenda.proposal (canonical)
                call_summary: d.as_ref().map(|d| d.summary.clone()),
                decoded_call: d.map(|d| d.tree),
                decode_status: status.to_string(),
                source: "inline".to_string(),
                note,
                spec_version: Some(ref_ctx.spec),
                decoder_version: calls::CALL_DECODER_VERSION,
                fetched_at_height: None,
            })
            .await?;
            fill_inline_proposal_hash(pool, &cfg.id, class, *referendum_id, &hash_hex, len)
                .await?;
        } else if proposal.get("Legacy").is_some() {
            // democracy-era: preimageFor is keyed (hash, len) and Legacy carries
            // no length — record honest 'missing' (len 0 sentinel; excluded from
            // future pending lists), covered by a later slice if ever needed
            let Some(hash_hex) = proposal_hash else {
                tracing::warn!(chain = %cfg.id, class, referendum_id,
                    "Legacy proposal without hash — skipped");
                continue;
            };
            upsert_preimage(pool, &cfg.id, &PreimageRecord {
                proposal_hash: hash_hex.clone(),
                len: 0,
                bytes_location: None,
                call_summary: None,
                decoded_call: None,
                decode_status: "missing".to_string(),
                source: "state".to_string(),
                note: Some("legacy proposal: length unknown, democracy-era preimage".into()),
                spec_version: Some(ctx.spec),
                decoder_version: calls::CALL_DECODER_VERSION,
                fetched_at_height: Some(ctx.height),
            })
            .await?;
            legacy += 1;
        } else {
            let (Some(hash_hex), Some(plen)) = (proposal_hash, proposal_len) else {
                tracing::warn!(chain = %cfg.id, class, referendum_id,
                    "Lookup proposal without hash/len — skipped");
                continue;
            };
            let hash32 = parse_h256(hash_hex)?;
            let status = fetch_and_record_preimage(
                pool, backends.receipts.as_ref(), &source, raw, &cfg.id, &ctx,
                &hash32, *plen as u64,
            )
            .await?;
            match status.as_str() {
                "decoded" => decoded += 1,
                "missing" => missing += 1,
                _ => undecodable += 1,
            }
        }
    }
    println!(
        "decode-preimages {chain} at #{} (spec {}): {} pending → \
         {decoded} fetched+decoded, {inline} inline decoded, {missing} missing, \
         {undecodable} undecodable, {legacy} legacy skipped",
        ctx.height, ctx.spec, rows.len()
    );
    Ok(())
}

#[cfg(all(feature = "pg", feature = "live"))]
fn parse_h256(s: &str) -> Result<[u8; 32]> {
    let hexpart = s.strip_prefix("0x").unwrap_or(s);
    let bytes = hex::decode(hexpart).context("hash is not hex")?;
    <[u8; 32]>::try_from(bytes.as_slice())
        .map_err(|_| anyhow::anyhow!("hash must be 32 bytes, got {}", bytes.len()))
}

/// `0x…` (or bare hex) call bytes → SCALE. Rejected loudly rather than
/// truncated: half a call decodes into a different call.
fn decode_call_hex(hex_str: &str) -> Result<Vec<u8>> {
    let trimmed = hex_str.trim();
    let body = trimmed.strip_prefix("0x").unwrap_or(trimmed);
    let bytes = hex::decode(body).context("call bytes are not hex")?;
    anyhow::ensure!(!bytes.is_empty(), "call bytes are empty");
    Ok(bytes)
}

/// simulate-call / simulate-referendum: one Tier 1 dry run, recorded.
///
/// THE CHAIN IS NAMED HERE ON PURPOSE, unlike almost every other command. A
/// simulation is an answer about one runtime at one state, so "which chain" is
/// part of the question rather than something residency should resolve away —
/// previewing a relay-era call against Asset Hub is a legitimate thing to ask
/// for, and it must not be silently redirected.
#[cfg(all(feature = "pg", feature = "live"))]
#[allow(clippy::too_many_arguments)]
async fn run_simulate(
    registry: &Registry,
    backends: &Backends,
    raw: &dyn RawStore,
    chain: &str,
    call_bytes: Vec<u8>,
    origin_spec: &str,
    height: Option<u64>,
    referendum: Option<(String, i64)>,
) -> Result<()> {
    use adapter_substrate::source::SubstrateSource;
    use dotlens_node::sim_pg::PgSimStore;
    use dotlens_node::sim_run::SubstrateDryRunner;

    let pool = backends.pool.as_ref().context("simulate requires DATABASE_URL")?;
    let cfg = registry
        .chain(chain)
        .with_context(|| format!("unknown chain: {chain}"))?;
    let origin = sim::OriginSpec::parse(origin_spec, |s| {
        adapter_substrate::accounts::parse_account(s).map_err(|e| e.to_string())
    })
    .map_err(|e| anyhow::anyhow!(e))?;

    let xcm_version: u32 = env_or("SIM_XCM_VERSION", "4")
        .parse()
        .context("SIM_XCM_VERSION must be a number")?;

    let source = SubstrateSource::new(&cfg.id, cfg.endpoints.rpc.clone())
        .map_err(|e| anyhow::anyhow!(e))?;
    let runner = SubstrateDryRunner::new(&cfg.id, &source, raw, backends.receipts.as_ref());
    let store = PgSimStore::new(pool.clone());

    let req = sim::SimRequest {
        chain_id: cfg.id.clone(),
        at_height: height,
        call: call_bytes,
        origin,
        origin_spec: origin_spec.to_string(),
        xcm_version,
    };
    let run = sim::run_simulation(&runner, &store, raw, backends.receipts.as_ref(), &req)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    let r = &run.record;

    if let Some((class, id)) = &referendum {
        println!("referendum {chain}/{class}/{id}");
    }
    println!(
        "simulate {chain} at #{} (spec {}, DryRunApi v{}){}",
        r.at_height,
        r.spec_version,
        r.api_version,
        if run.cached { " [recorded earlier]" } else { "" }
    );
    println!(
        "  call     {} ({})",
        r.call_summary.as_deref().unwrap_or("?"),
        r.call_hash
    );
    println!("  origin   {} → {}", r.origin_spec, r.origin_json["resolved"]);
    println!("  status   {}", r.status);
    if let Some(e) = &r.dispatch_error {
        println!("  error    {}", e["error"]);
    }
    println!("  events   {}", r.event_count);
    for ev in r.emitted_events.as_array().into_iter().flatten() {
        println!("    - {}", ev["name"].as_str().unwrap_or("?"));
    }
    let forwarded = r.forwarded_xcms.as_array().map(Vec::len).unwrap_or(0);
    println!("  xcm      local {} · forwarded to {} destination(s)",
        if r.local_xcm.is_some() { "yes" } else { "none" },
        forwarded
    );
    println!("  evidence {}", r.raw_location);
    Ok(())
}

/// simulate-referendum: the same dry run, sourced from an indexed proposal.
///
/// The bytes come from the RAW STORE first (the archived preimage value, which
/// is what `decode-preimages` filed) and from the referendum's own Inline
/// proposal second. Never re-fetched from state here: if we never archived the
/// preimage, that is a coverage gap with a named fix, not something to paper
/// over with a live read that might now return nothing.
#[cfg(all(feature = "pg", feature = "live"))]
#[allow(clippy::too_many_arguments)]
async fn run_simulate_referendum(
    registry: &Registry,
    backends: &Backends,
    raw: &dyn RawStore,
    chain: &str,
    class: &str,
    referendum_id: i64,
    origin_spec: &str,
    height: Option<u64>,
) -> Result<()> {
    use adapter_substrate::calls;

    let pool = backends
        .pool
        .as_ref()
        .context("simulate-referendum requires DATABASE_URL")?;
    let cfg = registry
        .chain(chain)
        .with_context(|| format!("unknown chain: {chain}"))?;
    let (proposal, proposal_hash, proposal_len) =
        dotlens_node::gov_pg::referendum_proposal(pool, &cfg.id, class, referendum_id)
            .await?
            .with_context(|| {
                format!("referendum {chain}/{class}/{referendum_id} is not indexed")
            })?;

    let call_bytes = match (&proposal, &proposal_hash, proposal_len) {
        // Inline: the bytes are in the referendum itself.
        (Some(p), _, _) if p.get("Inline").is_some() => calls::json_bytes(
            p.get("Inline").expect("checked"),
        )
        .context("this referendum's Inline proposal is not a byte sequence")?,
        // Lookup: the archived preimage value (compact length prefix + call).
        (_, Some(hash), Some(len)) => {
            let key = raw_store::keys::preimage(
                &cfg.id,
                hash.trim_start_matches("0x"),
                len as u64,
            );
            let stored = raw.get(&key).with_context(|| {
                format!(
                    "no archived preimage at {key} — run `decode-preimages {chain}` first \
                     (a preimage cleared from state after enactment can no longer be fetched, \
                     which is a coverage gap, not a retry)"
                )
            })?;
            <Vec<u8> as parity_scale_codec::Decode>::decode(&mut &stored[..])
                .map_err(|e| anyhow::anyhow!("archived preimage is not a BoundedVec<u8>: {e}"))?
        }
        _ => anyhow::bail!(
            "referendum {chain}/{class}/{referendum_id} has no proposal bytes to simulate \
             (no Inline body and no (hash, len) to find an archived preimage by)"
        ),
    };

    // The hash we simulate under must be the hash of the bytes we ran, always.
    if let Some(hash) = &proposal_hash {
        let computed = format!("0x{}", hex::encode(calls::blake2_256(&call_bytes)));
        anyhow::ensure!(
            &computed == hash,
            "the bytes for {chain}/{class}/{referendum_id} hash to {computed}, not the \
             recorded proposal hash {hash} — refusing to record a simulation under a hash \
             it did not run"
        );
    }

    run_simulate(
        registry,
        backends,
        raw,
        chain,
        call_bytes,
        origin_spec,
        height,
        Some((class.to_string(), referendum_id)),
    )
    .await
}

#[cfg(not(all(feature = "pg", feature = "live")))]
#[allow(clippy::too_many_arguments)]
async fn run_simulate(
    _: &Registry, _: &Backends, _: &dyn RawStore, _: &str, _: Vec<u8>, _: &str, _: Option<u64>,
    _: Option<(String, i64)>,
) -> Result<()> {
    anyhow::bail!("simulate-call requires the `pg` and `live` features")
}

#[cfg(not(all(feature = "pg", feature = "live")))]
#[allow(clippy::too_many_arguments)]
async fn run_simulate_referendum(
    _: &Registry, _: &Backends, _: &dyn RawStore, _: &str, _: &str, _: i64, _: &str,
    _: Option<u64>,
) -> Result<()> {
    anyhow::bail!("simulate-referendum requires the `pg` and `live` features")
}

#[cfg(not(all(feature = "pg", feature = "live")))]
async fn run_fetch_preimage(
    _: &Registry, _: &Backends, _: &dyn RawStore, _: &str, _: &str, _: u64, _: Option<u64>,
) -> Result<()> {
    anyhow::bail!("fetch-preimage requires the `pg` and `live` features")
}

#[cfg(not(all(feature = "pg", feature = "live")))]
async fn run_decode_preimages(
    _: &Registry, _: &Backends, _: &dyn RawStore, _: &str, _: Option<u64>,
) -> Result<()> {
    anyhow::bail!("decode-preimages requires the `pg` and `live` features")
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

/// Build the asset registry for a chain from its own storage. Idempotent:
/// re-running at a later height refreshes supply/metadata and adds anything
/// new, and never erases an asset that has stopped existing (a destroyed asset
/// is history, not a mistake).
#[cfg(all(feature = "pg", feature = "live"))]
async fn run_sync_assets(
    registry: &Registry,
    backends: &Backends,
    raw: &dyn RawStore,
    chain: &str,
    height: Option<u64>,
) -> Result<()> {
    use adapter_substrate::source::SubstrateSource;
    let pool = backends
        .pool
        .as_ref()
        .context("sync-assets requires DATABASE_URL")?;
    let cfg = registry
        .chain(chain)
        .with_context(|| format!("unknown chain: {chain}"))?;
    let source = SubstrateSource::new(&cfg.id, cfg.endpoints.rpc.clone())
        .map_err(|e| anyhow::anyhow!(e))?;
    let report =
        dotlens_node::assets_pg::sync_assets(pool, raw, &source, &cfg.id, height).await?;
    println!(
        "asset sync {chain} @#{} (spec {}): {} assets {:?}{}{}",
        report.height,
        report.spec_version,
        report.total(),
        report.per_instance,
        if report.unmapped_instances.is_empty() {
            String::new()
        } else {
            format!(
                " — EVENTS NOT MAPPED for instances {:?}",
                report.unmapped_instances
            )
        },
        if report.undecodable_ids == 0 {
            String::new()
        } else {
            format!(" — {} undecodable id(s)", report.undecodable_ids)
        }
    );
    Ok(())
}

#[cfg(not(all(feature = "pg", feature = "live")))]
async fn run_sync_assets(
    _: &Registry,
    _: &Backends,
    _: &dyn RawStore,
    _: &str,
    _: Option<u64>,
) -> Result<()> {
    anyhow::bail!("sync-assets requires the `pg` and `live` features")
}

/// Anchor every registered treasury account against every registered asset at
/// ONE block — the "where the funds are" snapshot the holdings endpoint reads.
#[cfg(all(feature = "pg", feature = "live"))]
async fn run_treasury_holdings(
    registry: &Registry,
    backends: &Backends,
    raw: &dyn RawStore,
    chain: &str,
    height: Option<u64>,
) -> Result<()> {
    use adapter_substrate::source::SubstrateSource;
    let pool = backends
        .pool
        .as_ref()
        .context("treasury-holdings requires DATABASE_URL")?;
    let cfg = registry
        .chain(chain)
        .with_context(|| format!("unknown chain: {chain}"))?;
    let source = SubstrateSource::new(&cfg.id, cfg.endpoints.rpc.clone())
        .map_err(|e| anyhow::anyhow!(e))?;
    let report =
        dotlens_node::assets_pg::snapshot_holdings(pool, raw, &source, &cfg.id, height).await?;
    println!(
        "holdings {chain} @#{} (spec {}): {} accounts × assets = {} probes, \
         {} non-zero{}",
        report.height,
        report.spec_version,
        report.accounts,
        report.assets_probed,
        report.non_zero,
        if report.skipped_no_key == 0 {
            String::new()
        } else {
            format!(
                " — {} asset(s) SKIPPED with no storage key (run sync-assets first)",
                report.skipped_no_key
            )
        }
    );
    if report.accounts == 0 {
        println!(
            "note: no treasury accounts registered on {chain} — run \
             `sync-treasury-accounts` (it needs archived metadata, so backfill first)"
        );
    }
    Ok(())
}

#[cfg(not(all(feature = "pg", feature = "live")))]
async fn run_treasury_holdings(
    _: &Registry,
    _: &Backends,
    _: &dyn RawStore,
    _: &str,
    _: Option<u64>,
) -> Result<()> {
    anyhow::bail!("treasury-holdings requires the `pg` and `live` features")
}

async fn axum_serve(listener: tokio::net::TcpListener, app: axum::Router) -> Result<()> {
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .context("api server")
}
