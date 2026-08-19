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
//!   dotlens-node simulate-xcm <chain> <origin-location> <0x-program> [height]
//!   dotlens-node simulate-forwarded <chain> <at-block-hash> <input-hash> [height]
//!
//! `simulate-xcm` is the RECEIVING side: what a chain would do with a program
//! that arrived from `origin-location` (here | parent | sibling:<para> |
//! child:<para> — a sibling and a child are the same para id at different parent
//! counts, so `para:<n>` is refused as ambiguous). `simulate-forwarded` takes a
//! recorded call simulation, works out which of its forwarded messages are
//! actually ITS OWN by differencing against the no-op baseline recorded beside
//! it, resolves each destination to a registered chain, and previews the leg
//! there — stopping and saying so at a destination dotlens does not index.
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
//!   dotlens-node xcm-range <chain> <from> <to>         # map XCM message facts
//!   dotlens-node xcm-correlate <chain> <from> <to>     # pair the two ids of one message
//!   dotlens-node coretime-range <chain> <from> <to>    # map core occupancy from relay events
//!   dotlens-node sync-core-config <chain> [height]     # read + DATE the num_cores denominator
//!   dotlens-node broker-range <chain> <from> <to>      # map coretime ENTITLEMENT from broker events
//!   dotlens-node sync-broker-config <chain> [height]   # read + DATE the entitlement denominator
//!
//! Per-module followers are opt-in flags: LIVE_INGEST, DECODE_FOLLOW,
//! BALANCES_FOLLOW, GOV_FOLLOW, VOTES_FOLLOW, TREASURY_FOLLOW, BOUNTIES_FOLLOW,
//! WHITELIST_FOLLOW, XCM_FOLLOW, XCM_CORRELATE_FOLLOW, CORETIME_FOLLOW,
//! TIP_FOLLOW (all `=1`).
//! XCM_FOLLOW and XCM_CORRELATE_FOLLOW are separate on purpose: they write
//! different tables under different versions, and re-deriving links after a
//! correlation-rule change must not touch a single observation row.
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
    xcm_sim: Arc<dyn api::XcmSimIndex>,
    xcm: Arc<dyn api::XcmIndex>,
    coretime: Arc<dyn api::CoretimeIndex>,
    broker: Arc<dyn api::BrokerIndex>,
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
        xcm_sim: Arc::new(api::MemoryXcmSimIndex::new()),
        xcm: Arc::new(api::MemoryXcmIndex::new()),
        coretime: Arc::new(api::MemoryCoretimeIndex::new()),
        broker: Arc::new(api::MemoryBrokerIndex::new()),
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
    CompactRaw { chain: String, from: u64, to: u64 },
    VerifyRaw { chain: String, from: u64, to: u64 },
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
    XcmRange { chain: String, from: u64, to: u64 },
    XcmCorrelate { chain: String, from: u64, to: u64 },
    CoretimeRange { chain: String, from: u64, to: u64 },
    SyncCoreConfig { chain: String, height: Option<u64> },
    BrokerRange { chain: String, from: u64, to: u64 },
    SyncBrokerConfig { chain: String, height: Option<u64> },
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
        opts: SimOptions,
    },
    SimulateReferendum {
        chain: String,
        class: String,
        referendum_id: i64,
        origin: String,
        height: Option<u64>,
        opts: SimOptions,
    },
    /// The read side of the Tier 2 queue.
    SimJobs { chain: String, status: Option<String> },
    SimulateXcm {
        chain: String,
        origin_location: String,
        program_hex: String,
        height: Option<u64>,
    },
    SimulateForwarded {
        chain: String,
        at_block_hash: String,
        input_hash: String,
        height: Option<u64>,
    },
}

/// The flags both `simulate-call` and `simulate-referendum` take.
///
/// THEY ARE FLAGS ON THE EXISTING COMMANDS RATHER THAN A NEW `simulate-fork`,
/// and that is deliberate: a separate command would have needed its own copy of
/// `run_simulate_referendum`'s preimage lookup and hash check, and two copies of
/// "where do a referendum's bytes come from" is the duplication this project has
/// had to unpick in four separate slices. The tier is the thing that varies, so
/// the tier is the argument. Every shipped invocation keeps working unchanged,
/// because the default is still `dry_run`.
#[derive(Debug, Clone, Default)]
struct SimOptions {
    /// "dry_run" (default) | "fork".
    tier: Option<String>,
    /// Storage overrides, as written. See `adapter_substrate::fork::OverrideSpec`.
    override_specs: Vec<String>,
    /// Enqueue and return the job id instead of running it here.
    queue: bool,
    /// Who signs the extrinsic the fork applies. Required on the fork tier when
    /// the origin is privileged — see `sim::ForkRequest::signer`.
    signer: Option<String>,
}

/// Pull `--tier`, `--set`, `--overrides` and `--queue` out of the argument list,
/// leaving the positional arguments contiguous so the existing index-based
/// parsing is untouched.
fn take_sim_flags(args: &mut Vec<String>) -> Result<SimOptions> {
    let mut opts = SimOptions::default();
    let mut i = 0;
    while i < args.len() {
        let take_value = |args: &Vec<String>, i: usize, flag: &str| -> Result<String> {
            args.get(i + 1)
                .cloned()
                .with_context(|| format!("{flag} needs a value"))
        };
        match args[i].as_str() {
            "--tier" => {
                opts.tier = Some(take_value(args, i, "--tier")?);
                args.drain(i..i + 2);
            }
            "--set" => {
                opts.override_specs.push(take_value(args, i, "--set")?);
                args.drain(i..i + 2);
            }
            "--overrides" => {
                // A FILE, because a storage value is JSON and shell quoting is
                // where a counterfactual quietly becomes a different one. One
                // spec per line; blank lines and `#` comments ignored.
                let path = take_value(args, i, "--overrides")?;
                let text = std::fs::read_to_string(&path)
                    .with_context(|| format!("reading overrides from {path}"))?;
                for line in text.lines() {
                    let line = line.trim();
                    if line.is_empty() || line.starts_with('#') {
                        continue;
                    }
                    opts.override_specs.push(line.to_string());
                }
                args.drain(i..i + 2);
            }
            "--signer" => {
                opts.signer = Some(take_value(args, i, "--signer")?);
                args.drain(i..i + 2);
            }
            "--queue" => {
                opts.queue = true;
                args.remove(i);
            }
            _ => i += 1,
        }
    }
    // AN OVERRIDE ON TIER 1 IS AN ERROR, NOT A NO-OP. `dry_run_call` executes
    // against the chain's real state and cannot be told otherwise, so silently
    // ignoring `--set` would answer a different question than the one asked and
    // look exactly like an answer to the one that was.
    let tier = opts.tier.as_deref().unwrap_or(sim::TIER_DRY_RUN);
    anyhow::ensure!(
        opts.override_specs.is_empty() || tier == sim::TIER_FORK,
        "--set/--overrides inject storage, which only the fork tier can do — Tier 1 dry-runs \
         against the chain's real state and cannot be told otherwise. Add --tier fork, or drop \
         the overrides; they will not be silently ignored"
    );
    anyhow::ensure!(
        !opts.queue || tier == sim::TIER_FORK,
        "--queue enqueues a job for the fork worker; the dry-run tier is one round trip and has \
         no queue"
    );
    anyhow::ensure!(
        matches!(tier, "dry_run" | "fork"),
        "--tier must be dry_run or fork, not '{tier}'"
    );
    Ok(opts)
}

fn parse_args() -> Result<Command> {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    // ONLY the simulate commands take these flags. Stripping them globally would
    // silently re-index every other command's positional arguments, so a typo in
    // `backfill` would shift a height rather than fail.
    let opts = match args.first().map(String::as_str) {
        Some("simulate-call") | Some("simulate-referendum") => take_sim_flags(&mut args)?,
        _ => SimOptions::default(),
    };
    let args = args;
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
        Some("compact-raw") => {
            let (chain, from, to) = range("usage: dotlens-node compact-raw <chain> <from> <to>")?;
            Ok(Command::CompactRaw { chain, from, to })
        }
        Some("verify-raw") => {
            let (chain, from, to) = range("usage: dotlens-node verify-raw <chain> <from> <to>")?;
            Ok(Command::VerifyRaw { chain, from, to })
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
        Some("xcm-range") => {
            let (chain, from, to) = range("usage: dotlens-node xcm-range <chain> <from> <to>")?;
            Ok(Command::XcmRange { chain, from, to })
        }
        Some("xcm-correlate") => {
            let (chain, from, to) =
                range("usage: dotlens-node xcm-correlate <chain> <from> <to>")?;
            Ok(Command::XcmCorrelate { chain, from, to })
        }
        Some("coretime-range") => {
            let (chain, from, to) =
                range("usage: dotlens-node coretime-range <chain> <from> <to>")?;
            Ok(Command::CoretimeRange { chain, from, to })
        }
        Some("sync-core-config") => {
            let usage = "usage: dotlens-node sync-core-config <chain> [height]";
            let chain = args.get(1).context(usage)?.clone();
            let height = match args.get(2) {
                Some(h) => Some(h.parse::<u64>().context(usage)?),
                None => None,
            };
            Ok(Command::SyncCoreConfig { chain, height })
        }
        Some("broker-range") => {
            let (chain, from, to) = range("usage: dotlens-node broker-range <chain> <from> <to>")?;
            Ok(Command::BrokerRange { chain, from, to })
        }
        Some("sync-broker-config") => {
            let usage = "usage: dotlens-node sync-broker-config <chain> [height]";
            let chain = args.get(1).context(usage)?.clone();
            let height = match args.get(2) {
                Some(h) => Some(h.parse::<u64>().context(usage)?),
                None => None,
            };
            Ok(Command::SyncBrokerConfig { chain, height })
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
            Ok(Command::SimulateCall { chain, call_hex, origin, height, opts })
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
            Ok(Command::SimulateReferendum { chain, class, referendum_id, origin, height, opts })
        }
        Some("simulate-xcm") => {
            let usage =
                "usage: dotlens-node simulate-xcm <chain> <origin-location> <0x-program-hex> \
                 [height]\n\
                 origin-location: here | parent | sibling:<para> | child:<para>";
            let chain = args.get(1).context(usage)?.clone();
            let origin_location = args.get(2).context(usage)?.clone();
            let program_hex = args.get(3).context(usage)?.clone();
            let height = match args.get(4) {
                Some(h) => Some(h.parse::<u64>().context(usage)?),
                None => None,
            };
            Ok(Command::SimulateXcm { chain, origin_location, program_hex, height })
        }
        Some("simulate-forwarded") => {
            // BOTH coordinates are required, and that is not verbosity. A
            // simulation is keyed by (chain, BLOCK HASH, input hash): the same
            // question asked at two states shares an input hash and differs only
            // by block hash, so an input hash alone would make the lookup pick
            // one of two answers on the caller's behalf. `simulate-call` prints
            // both, in the evidence path.
            let usage = "usage: dotlens-node simulate-forwarded <chain> <at-block-hash> \
                         <input-hash> [destination-height]";
            let chain = args.get(1).context(usage)?.clone();
            let at_block_hash = args.get(2).context(usage)?.clone();
            let input_hash = args.get(3).context(usage)?.clone();
            let height = match args.get(4) {
                Some(h) => Some(h.parse::<u64>().context(usage)?),
                None => None,
            };
            Ok(Command::SimulateForwarded { chain, at_block_hash, input_hash, height })
        }
        Some("sim-jobs") => {
            let usage = "usage: dotlens-node sim-jobs <chain> [status]";
            let chain = args.get(1).context(usage)?.clone();
            Ok(Command::SimJobs { chain, status: args.get(2).cloned() })
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
            xcm_sim: Arc::new(api::pg::PgXcmSimIndex::new(pool.clone())),
            xcm: Arc::new(api::pg::PgXcmIndex::new(pool.clone())),
            coretime: Arc::new(api::pg::PgCoretimeIndex::new(pool.clone())),
            broker: Arc::new(api::pg::PgBrokerIndex::new(pool.clone())),
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
    let bucket_blocks: u64 = env_or("RAW_BUCKET_BLOCKS", "1000").parse().unwrap_or(1000);
    // Every READ goes through the bucket layer: a block artifact resolves to its
    // per-object copy or to the compacted bucket holding it, and no caller can
    // tell which. Writes delegate straight through — buckets are written only by
    // `compact-raw`, and nothing else may write one.
    let raw: Arc<dyn RawStore> = Arc::new(raw_store::BucketedStore::new(
        FsRawStore::new(&raw_root),
        bucket_blocks,
    ));

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
    if let Command::CompactRaw { chain, from, to } = &command {
        // deliberately the PLAIN store, not the bucket layer: compaction reads
        // the per-object copies, and reading through the bucket layer would let
        // it pack a bucket's own output back into a bucket.
        let plain = FsRawStore::new(&raw_root);
        let level: i32 = env_or("RAW_ZSTD_LEVEL", "3").parse().unwrap_or(3);
        let report = dotlens_node::compact::compact_range(
            &plain, &dotlens_node::compact::NoopReceipts, chain, *from, *to, bucket_blocks, level,
        )
        .await?;
        println!(
            "compact {chain} {from}..={to}: buckets_written={} already_present={} \
heights_packed={} members={} absent={} unpacked={} raw_bytes={} stored_bytes={} ratio={:.1}x",
            report.buckets_written, report.buckets_already_present, report.heights_packed,
            report.members_packed, report.heights_absent.len(), report.heights_unpacked.len(),
            report.raw_bytes, report.stored_bytes, report.ratio()
        );
        if !report.heights_absent.is_empty() {
            println!(
                "  {} height(s) in range had no artifact at all (first few: {:?}) — a hole here is \
a gap in the raw store, not something compaction may close over",
                report.heights_absent.len(),
                &report.heights_absent[..report.heights_absent.len().min(10)]
            );
        }
        if !report.heights_unpacked.is_empty() {
            println!(
                "  {} height(s) in range have a per-object copy that the EXISTING bucket does not hold (first few: {:?}) — buckets are write-once, so nothing will ever pack these; their per-object copies must not be retired, and the fix is to compact a span only once its backfill is complete",
                report.heights_unpacked.len(),
                &report.heights_unpacked[..report.heights_unpacked.len().min(10)]
            );
        }
        return Ok(());
    }
    if let Command::VerifyRaw { chain, from, to } = &command {
        let plain = FsRawStore::new(&raw_root);
        let report =
            dotlens_node::compact::verify_range(&plain, chain, *from, *to, bucket_blocks).await?;
        println!(
            "verify {chain} {from}..={to}: buckets_checked={} missing={} members_verified={} \
mismatched={} retirable_heights={}",
            report.buckets_checked, report.buckets_missing, report.members_verified,
            report.mismatched.len(), report.retirable_heights
        );
        if !report.mismatched.is_empty() {
            println!("  MISMATCHED (do NOT retire these): {:?}", report.mismatched);
            anyhow::bail!("bucket contents disagree with their per-object copies");
        }
        return Ok(());
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
    if let Command::XcmRange { chain, from, to } = &command {
        anyhow::ensure!(
            backends.persistent,
            "xcm-range requires DATABASE_URL (canonical events + xcm facts must persist)"
        );
        return run_xcm_range(&registry, &backends, chain, *from, *to).await;
    }
    if let Command::XcmCorrelate { chain, from, to } = &command {
        anyhow::ensure!(
            backends.persistent,
            "xcm-correlate requires DATABASE_URL (canonical events + xcm links must persist)"
        );
        return run_xcm_correlate(&registry, &backends, chain, *from, *to).await;
    }
    if let Command::CoretimeRange { chain, from, to } = &command {
        anyhow::ensure!(
            backends.persistent,
            "coretime-range requires DATABASE_URL (canonical events + occupancy facts must persist)"
        );
        return run_coretime_range(&registry, &backends, chain, *from, *to).await;
    }
    if let Command::SyncCoreConfig { chain, height } = &command {
        return run_sync_core_config(&registry, &backends, raw.as_ref(), chain, *height).await;
    }
    if let Command::BrokerRange { chain, from, to } = &command {
        anyhow::ensure!(
            backends.persistent,
            "broker-range requires DATABASE_URL (canonical events + entitlement facts must persist)"
        );
        return run_broker_range(&registry, &backends, chain, *from, *to).await;
    }
    if let Command::SyncBrokerConfig { chain, height } = &command {
        return run_sync_broker_config(&registry, &backends, raw.as_ref(), chain, *height).await;
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
    if let Command::SimulateCall { chain, call_hex, origin, height, opts } = &command {
        let bytes = decode_scale_hex(call_hex, "call")?;
        return run_simulate(
            &registry, &backends, raw.as_ref(), chain, bytes, origin, *height, None, opts,
        )
        .await;
    }
    if let Command::SimulateReferendum {
        chain, class, referendum_id, origin, height, opts,
    } = &command
    {
        return run_simulate_referendum(
            &registry, &backends, raw.as_ref(), chain, class, *referendum_id, origin, *height,
            opts,
        )
        .await;
    }
    if let Command::SimJobs { chain, status } = &command {
        return run_sim_jobs(&backends, chain, status.as_deref()).await;
    }
    if let Command::SimulateXcm { chain, origin_location, program_hex, height } = &command {
        let bytes = decode_scale_hex(program_hex, "program")?;
        return run_simulate_xcm(
            &registry, &backends, raw.as_ref(), chain, origin_location, bytes, *height, None,
        )
        .await;
    }
    if let Command::SimulateForwarded { chain, at_block_hash, input_hash, height } = &command {
        return run_simulate_forwarded(
            &registry, &backends, raw.as_ref(), chain, at_block_hash, input_hash, *height,
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
    spawn_xcm_followers(&registry, &backends);
    spawn_xcm_correlate_followers(&registry, &backends);
    spawn_coretime_followers(&registry, &backends);
    spawn_broker_followers(&registry, &backends);
    spawn_fork_job_worker(&registry, &backends, &raw);
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
        xcm_sim: backends.xcm_sim.clone(),
        xcm: backends.xcm.clone(),
        coretime: backends.coretime.clone(),
        broker: backends.broker.clone(),
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

/// XCM followers, gated on the `xcm` module — which relay, Asset Hub,
/// Collectives, People and Hydration all declare.
fn spawn_xcm_followers(registry: &Arc<Registry>, backends: &Arc<Backends>) {
    if !env_flag("XCM_FOLLOW") {
        tracing::info!("xcm follower disabled (set XCM_FOLLOW=1 to enable)");
        return;
    }
    if !backends.persistent {
        tracing::warn!("XCM_FOLLOW=1 but no DATABASE_URL — refusing to map into memory");
        return;
    }
    #[cfg(feature = "pg")]
    {
        use adapter_substrate::xcm::SubstrateXcmMapper;

        let poll = std::time::Duration::from_secs(
            env_or("POLL_INTERVAL_SECS", "6").parse().unwrap_or(6),
        );
        for chain in registry.chains() {
            if !chain.has_module("xcm") {
                continue;
            }
            if chain.family != registry::ChainFamily::Substrate {
                tracing::debug!(chain = %chain.id, "no xcm mapper for this family — skipped");
                continue;
            }
            let Some(pool) = backends.pool.clone() else { continue };
            let chain_id = chain.id.clone();
            let backends = backends.clone();
            tokio::spawn(async move {
                tracing::info!(chain = %chain_id, "xcm follower started");
                let source = dotlens_node::balances_pg::PgEventSource::new(pool.clone());
                let sink = dotlens_node::xcm_pg::PgXcmSink::new(pool);
                let deps = ingest::xcm::XcmDeps {
                    checkpoints: backends.checkpoints.as_ref(),
                    source: &source,
                    sink: &sink,
                };
                ingest::xcm::xcm_follow(&chain_id, &SubstrateXcmMapper, &deps, poll).await;
            });
        }
    }
}

/// Core occupancy followers, gated on the `coretime` module — which ONLY the
/// relay declares, and deliberately so.
///
/// `paraInclusion` is a relay pallet: it names both the para id and the core
/// index, so which core produced a block is a relay fact and a parachain that
/// declared this module would start a follower that maps nothing forever. A
/// registry test asserts the gate holds in that direction (any chain declaring
/// `coretime` must be a relay), because a MISSING declaration is the failure
/// this project has paid for before — the follower silently never starts, and an
/// empty occupancy table is indistinguishable from a network where no core did
/// any work.
fn spawn_coretime_followers(registry: &Arc<Registry>, backends: &Arc<Backends>) {
    if !env_flag("CORETIME_FOLLOW") {
        tracing::info!("coretime follower disabled (set CORETIME_FOLLOW=1 to enable)");
        return;
    }
    if !backends.persistent {
        tracing::warn!("CORETIME_FOLLOW=1 but no DATABASE_URL — refusing to map into memory");
        return;
    }
    #[cfg(feature = "pg")]
    {
        use adapter_substrate::coretime::SubstrateOccupancyMapper;

        let poll = std::time::Duration::from_secs(
            env_or("POLL_INTERVAL_SECS", "6").parse().unwrap_or(6),
        );
        for chain in registry.chains() {
            if !chain.has_module("coretime") {
                continue;
            }
            if chain.family != registry::ChainFamily::Substrate {
                tracing::debug!(chain = %chain.id, "no coretime mapper for this family — skipped");
                continue;
            }
            let Some(pool) = backends.pool.clone() else { continue };
            let chain_id = chain.id.clone();
            let backends = backends.clone();
            tokio::spawn(async move {
                tracing::info!(chain = %chain_id, "coretime follower started");
                let source = dotlens_node::balances_pg::PgEventSource::new(pool.clone());
                let sink = dotlens_node::coretime_pg::PgOccupancySink::new(pool);
                let deps = ingest::coretime::CoretimeDeps {
                    checkpoints: backends.checkpoints.as_ref(),
                    source: &source,
                    sink: &sink,
                };
                ingest::coretime::coretime_follow(
                    &chain_id,
                    &SubstrateOccupancyMapper,
                    &deps,
                    poll,
                )
                .await;
            });
        }
    }
}

/// Broker entitlement followers.
///
/// Gated on the `broker` module, which exactly one chain in the registry
/// declares — and it is found by ASKING THE REGISTRY, never by naming 1005.
/// Adding a second network's coretime chain is then a seed file and nothing
/// else, which is the whole of Invariant 2 on this surface.
///
/// The gate matters more than usual here because the failure is silent in the
/// most convincing way: a follower that never starts leaves an empty
/// `broker_events`, and an empty entitlement table reads exactly like a network
/// where nobody bought a core — which is a claim about the market rather than
/// about our coverage.
fn spawn_broker_followers(registry: &Arc<Registry>, backends: &Arc<Backends>) {
    if !env_flag("BROKER_FOLLOW") {
        tracing::info!("broker follower disabled (set BROKER_FOLLOW=1 to enable)");
        return;
    }
    if !backends.persistent {
        tracing::warn!("BROKER_FOLLOW=1 but no DATABASE_URL — refusing to map into memory");
        return;
    }
    #[cfg(feature = "pg")]
    {
        use adapter_substrate::broker::SubstrateBrokerMapper;

        let poll = std::time::Duration::from_secs(
            env_or("POLL_INTERVAL_SECS", "6").parse().unwrap_or(6),
        );
        for chain in registry.chains() {
            if !chain.has_module("broker") {
                continue;
            }
            if chain.family != registry::ChainFamily::Substrate {
                tracing::debug!(chain = %chain.id, "no broker mapper for this family — skipped");
                continue;
            }
            let Some(pool) = backends.pool.clone() else { continue };
            let chain_id = chain.id.clone();
            let backends = backends.clone();
            tokio::spawn(async move {
                tracing::info!(chain = %chain_id, "broker follower started");
                let source = dotlens_node::balances_pg::PgEventSource::new(pool.clone());
                let sink = dotlens_node::broker_pg::PgBrokerSink::new(pool);
                let deps = ingest::broker::BrokerDeps {
                    checkpoints: backends.checkpoints.as_ref(),
                    source: &source,
                    sink: &sink,
                };
                ingest::broker::broker_follow(&chain_id, &SubstrateBrokerMapper, &deps, poll).await;
            });
        }
    }
}

/// XCM correlation followers. Same gate as the `xcm` module — a chain whose
/// messages we do not record has nothing to correlate — and its own env flag,
/// because the two workers write different tables under different versions and
/// running one without the other is a legitimate thing to want (re-deriving
/// links after a rule change, without touching a single observation row).
fn spawn_xcm_correlate_followers(registry: &Arc<Registry>, backends: &Arc<Backends>) {
    if !env_flag("XCM_CORRELATE_FOLLOW") {
        tracing::info!(
            "xcm correlate follower disabled (set XCM_CORRELATE_FOLLOW=1 to enable)"
        );
        return;
    }
    if !backends.persistent {
        tracing::warn!("XCM_CORRELATE_FOLLOW=1 but no DATABASE_URL — refusing to map into memory");
        return;
    }
    #[cfg(feature = "pg")]
    {
        use adapter_substrate::xcm_correlate::SubstrateXcmCorrelator;

        let poll = std::time::Duration::from_secs(
            env_or("POLL_INTERVAL_SECS", "6").parse().unwrap_or(6),
        );
        for chain in registry.chains() {
            if !chain.has_module("xcm") {
                continue;
            }
            if chain.family != registry::ChainFamily::Substrate {
                tracing::debug!(chain = %chain.id, "no xcm correlator for this family — skipped");
                continue;
            }
            let Some(pool) = backends.pool.clone() else { continue };
            let chain_id = chain.id.clone();
            let backends = backends.clone();
            tokio::spawn(async move {
                tracing::info!(chain = %chain_id, "xcm correlate follower started");
                let source = dotlens_node::balances_pg::PgEventSource::new(pool.clone());
                let sink = dotlens_node::xcm_links_pg::PgXcmLinkSink::new(pool);
                let deps = ingest::xcm_correlate::XcmCorrelateDeps {
                    checkpoints: backends.checkpoints.as_ref(),
                    source: &source,
                    sink: &sink,
                };
                ingest::xcm_correlate::xcm_correlate_follow(
                    &chain_id,
                    &SubstrateXcmCorrelator,
                    &deps,
                    poll,
                )
                .await;
            });
        }
    }
}

#[cfg(feature = "pg")]
async fn run_xcm_correlate(
    registry: &Registry,
    backends: &Backends,
    chain: &str,
    from: u64,
    to: u64,
) -> Result<()> {
    use adapter_substrate::xcm_correlate::SubstrateXcmCorrelator;

    let cfg = registry
        .chain(chain)
        .with_context(|| format!("unknown chain: {chain}"))?;
    anyhow::ensure!(
        cfg.family == registry::ChainFamily::Substrate,
        "no xcm correlator for family {:?}",
        cfg.family
    );
    let pool = backends
        .pool
        .as_ref()
        .context("xcm-correlate requires DATABASE_URL")?;
    let source = dotlens_node::balances_pg::PgEventSource::new(pool.clone());
    let sink = dotlens_node::xcm_links_pg::PgXcmLinkSink::new(pool.clone());
    let deps = ingest::xcm_correlate::XcmCorrelateDeps {
        checkpoints: backends.checkpoints.as_ref(),
        source: &source,
        sink: &sink,
    };
    let n = ingest::xcm_correlate::xcm_correlate_range(
        &cfg.id,
        &SubstrateXcmCorrelator,
        &deps,
        from,
        to,
    )
    .await
    .with_context(|| format!("xcm-correlate {chain} {from}..={to}"))?;
    tracing::info!(chain, from, to, correlated = n, "xcm-correlate complete");
    println!("xcm-correlate {chain} {from}..={to}: correlated {n} blocks");
    Ok(())
}

#[cfg(not(feature = "pg"))]
async fn run_xcm_correlate(_: &Registry, _: &Backends, _: &str, _: u64, _: u64) -> Result<()> {
    anyhow::bail!("xcm-correlate requires the `pg` feature")
}

#[cfg(feature = "pg")]
async fn run_xcm_range(
    registry: &Registry,
    backends: &Backends,
    chain: &str,
    from: u64,
    to: u64,
) -> Result<()> {
    use adapter_substrate::xcm::SubstrateXcmMapper;

    let cfg = registry
        .chain(chain)
        .with_context(|| format!("unknown chain: {chain}"))?;
    anyhow::ensure!(
        cfg.family == registry::ChainFamily::Substrate,
        "no xcm mapper for family {:?}",
        cfg.family
    );
    let pool = backends.pool.as_ref().context("xcm-range requires DATABASE_URL")?;
    let source = dotlens_node::balances_pg::PgEventSource::new(pool.clone());
    let sink = dotlens_node::xcm_pg::PgXcmSink::new(pool.clone());
    let deps = ingest::xcm::XcmDeps {
        checkpoints: backends.checkpoints.as_ref(),
        source: &source,
        sink: &sink,
    };
    let n = ingest::xcm::xcm_range(&cfg.id, &SubstrateXcmMapper, &deps, from, to)
        .await
        .with_context(|| format!("xcm-range {chain} {from}..={to}"))?;
    tracing::info!(chain, from, to, mapped = n, "xcm-range complete");
    println!("xcm-range {chain} {from}..={to}: mapped {n} blocks");
    Ok(())
}

#[cfg(not(feature = "pg"))]
async fn run_xcm_range(_: &Registry, _: &Backends, _: &str, _: u64, _: u64) -> Result<()> {
    anyhow::bail!("xcm-range requires the `pg` feature")
}

/// Map core occupancy over a decoded relay range.
///
/// NO BACKFILL IS NEEDED FOR THIS, which is the cheapest fact in the slice:
/// occupancy is a question about parachains answered entirely by relay events,
/// and Phase 1 already put the relay blocks in the raw store. The prep pass
/// decoded 1,542 of them across six runtimes with no RPC at all.
#[cfg(feature = "pg")]
async fn run_coretime_range(
    registry: &Registry,
    backends: &Backends,
    chain: &str,
    from: u64,
    to: u64,
) -> Result<()> {
    use adapter_substrate::coretime::SubstrateOccupancyMapper;

    let cfg = registry
        .chain(chain)
        .with_context(|| format!("unknown chain: {chain}"))?;
    anyhow::ensure!(
        cfg.family == registry::ChainFamily::Substrate,
        "no coretime mapper for family {:?}",
        cfg.family
    );
    // A LOUD REFUSAL RATHER THAN AN EMPTY RUN. Mapping a chain that does not
    // declare the module would report "mapped N blocks" and write nothing, which
    // is the exact shape of the silent gap the follower gate exists to prevent.
    anyhow::ensure!(
        cfg.has_module("coretime"),
        "{chain} does not declare the `coretime` module. Occupancy is read from `paraInclusion`, \
         a RELAY pallet, so a parachain has no candidate events to map and this run would report \
         success while writing nothing"
    );
    let pool = backends
        .pool
        .as_ref()
        .context("coretime-range requires DATABASE_URL")?;
    let source = dotlens_node::balances_pg::PgEventSource::new(pool.clone());
    let sink = dotlens_node::coretime_pg::PgOccupancySink::new(pool.clone());
    let deps = ingest::coretime::CoretimeDeps {
        checkpoints: backends.checkpoints.as_ref(),
        source: &source,
        sink: &sink,
    };
    let n = ingest::coretime::coretime_range(&cfg.id, &SubstrateOccupancyMapper, &deps, from, to)
        .await
        .with_context(|| format!("coretime-range {chain} {from}..={to}"))?;
    tracing::info!(chain, from, to, mapped = n, "coretime-range complete");
    println!("coretime-range {chain} {from}..={to}: mapped {n} blocks");
    Ok(())
}

#[cfg(not(feature = "pg"))]
async fn run_coretime_range(_: &Registry, _: &Backends, _: &str, _: u64, _: u64) -> Result<()> {
    anyhow::bail!("coretime-range requires the `pg` feature")
}

/// Read `Configuration.ActiveConfig` at one block and record `num_cores` with
/// the height it was read at.
///
/// THIS IS THE DENOMINATOR AND IT HAS ITS OWN COMMAND FOR A REASON. `num_cores`
/// is host configuration that moves at SESSION boundaries; the prep read it at
/// exactly one block (#32614536 → 100) and named a stale denominator as a live
/// risk. Making it a periodic reading rather than a constant is what lets every
/// ratio the API serves say which reading it divided by — and lets a reader see
/// when two readings inside one window disagree.
#[cfg(all(feature = "pg", feature = "live"))]
async fn run_sync_core_config(
    registry: &Registry,
    backends: &Backends,
    raw: &dyn RawStore,
    chain: &str,
    height: Option<u64>,
) -> Result<()> {
    use adapter_substrate::{coretime as ac, source::SubstrateSource};
    use ingest::live::ChainSource;

    let pool = backends
        .pool
        .as_ref()
        .context("sync-core-config requires DATABASE_URL")?;
    let cfg = registry
        .chain(chain)
        .with_context(|| format!("unknown chain: {chain}"))?;
    anyhow::ensure!(
        cfg.has_module("coretime"),
        "{chain} does not declare the `coretime` module — the scheduler's core count is relay \
         host configuration and a parachain has none"
    );
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

    // Archived blob if we have it, else fetch AND archive — same rule as
    // `anchor-balance`, so the reading stays re-derivable from the raw store.
    let meta_key = raw_store::keys::metadata(&cfg.id, spec);
    let metadata = match raw.get(&meta_key) {
        Ok(blob) => blob,
        Err(raw_store::RawStoreError::NotFound(_)) => {
            let blob = source.metadata_at(height).await.map_err(|e| anyhow::anyhow!(e))?;
            raw.put(&meta_key, &blob, "sync-core-config")?;
            tracing::info!(chain = %cfg.id, spec, "metadata archived while reading core config");
            blob
        }
        Err(e) => return Err(e.into()),
    };

    let key = ac::active_config_key();
    let bytes = source
        .storage_at(&key, hash)
        .await
        .map_err(|e| anyhow::anyhow!(e))?
        // ABSENT IS AN ERROR HERE, unlike an absent account balance. An account
        // that does not exist holds zero and that is a fact; a runtime with no
        // active host configuration is not a runtime, so an empty read means the
        // key or the entry name is wrong and recording "0 cores" would make
        // every ratio silently infinite.
        .with_context(|| {
            format!(
                "{}.{} is absent from state at #{height} — the entry name or the key derivation \
                 is wrong, and a missing host configuration is not a chain with no cores",
                ac::CONFIG_PALLET,
                ac::ACTIVE_CONFIG_ENTRY
            )
        })?;
    let view = ac::decode_active_config(&metadata, &bytes).map_err(|e| anyhow::anyhow!(e))?;
    dotlens_node::coretime_pg::insert_core_config(
        pool,
        &cfg.id,
        height,
        view.num_cores,
        &view.scheduler_params,
        spec,
    )
    .await?;
    println!(
        "core config {chain} @#{height} (spec {spec}): num_cores={}",
        view.num_cores
    );
    println!(
        "  this reading DATES the denominator. num_cores moves at session boundaries, so a \
         ratio computed against it is only as current as #{height} — /v1/coretime/{chain}/occupancy \
         names which reading it used."
    );
    Ok(())
}

#[cfg(not(all(feature = "pg", feature = "live")))]
async fn run_sync_core_config(
    _: &Registry,
    _: &Backends,
    _: &dyn RawStore,
    _: &str,
    _: Option<u64>,
) -> Result<()> {
    anyhow::bail!("sync-core-config requires the `pg` and `live` features")
}

/// Map `Broker.*` events into entitlement facts and the seam.
#[cfg(feature = "pg")]
async fn run_broker_range(
    registry: &Registry,
    backends: &Backends,
    chain: &str,
    from: u64,
    to: u64,
) -> Result<()> {
    use adapter_substrate::broker::SubstrateBrokerMapper;

    let cfg = registry
        .chain(chain)
        .with_context(|| format!("unknown chain: {chain}"))?;
    anyhow::ensure!(
        cfg.family == registry::ChainFamily::Substrate,
        "no broker mapper for chain family {:?}",
        cfg.family
    );
    // A LOUD REFUSAL RATHER THAN AN EMPTY RUN — the mirror of coretime-range's,
    // and the failure it prevents is the more misleading of the two. Running this
    // against the RELAY would report "mapped N blocks" and write nothing, because
    // `pallet-broker` lives on the Coretime chain and the relay has no `Broker`
    // events at all. An empty entitlement table beside a full occupancy table
    // reads as "everything that ran was unpaid for", which is the most
    // interesting possible wrong answer.
    anyhow::ensure!(
        cfg.has_module("broker"),
        "{chain} does not declare the `broker` module. Entitlement is read from `pallet-broker`, \
         which lives on the CORETIME chain — the relay carries the occupancy half (coretime-range) \
         and no broker events whatsoever, so this run would report success while writing nothing"
    );
    let pool = backends
        .pool
        .as_ref()
        .context("broker-range requires DATABASE_URL")?;
    let source = dotlens_node::balances_pg::PgEventSource::new(pool.clone());
    let sink = dotlens_node::broker_pg::PgBrokerSink::new(pool.clone());
    let deps = ingest::broker::BrokerDeps {
        checkpoints: backends.checkpoints.as_ref(),
        source: &source,
        sink: &sink,
    };
    let n = ingest::broker::broker_range(&cfg.id, &SubstrateBrokerMapper, &deps, from, to)
        .await
        .with_context(|| format!("broker-range {chain} {from}..={to}"))?;
    tracing::info!(chain, from, to, mapped = n, "broker-range complete");
    println!("broker-range {chain} {from}..={to}: mapped {n} blocks");
    Ok(())
}

#[cfg(not(feature = "pg"))]
async fn run_broker_range(_: &Registry, _: &Backends, _: &str, _: u64, _: u64) -> Result<()> {
    anyhow::bail!("broker-range requires the `pg` feature")
}

/// Read `Broker.Status` + `Broker.Configuration` at one block and record
/// `core_count` with the height it was read at.
///
/// THE ENTITLEMENT DENOMINATOR, and its own command for the same reason
/// `sync-core-config` is: it MOVES. But it earns the command twice over, because
/// it is also the cross-check that makes the join believable — it read 100 on the
/// Coretime chain at the same time the RELAY's `num_cores` read 100. Two chains,
/// two storage items, one number, and if they ever disagree one of the two halves
/// is being counted against the wrong denominator.
#[cfg(all(feature = "pg", feature = "live"))]
async fn run_sync_broker_config(
    registry: &Registry,
    backends: &Backends,
    raw: &dyn RawStore,
    chain: &str,
    height: Option<u64>,
) -> Result<()> {
    use adapter_substrate::{broker as ab, source::SubstrateSource};
    use ingest::live::ChainSource;

    let pool = backends
        .pool
        .as_ref()
        .context("sync-broker-config requires DATABASE_URL")?;
    let cfg = registry
        .chain(chain)
        .with_context(|| format!("unknown chain: {chain}"))?;
    anyhow::ensure!(
        cfg.has_module("broker"),
        "{chain} does not declare the `broker` module — `Broker.Status` is the coretime market's \
         own state and no other chain has one"
    );
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

    // Archived blob if we have it, else fetch AND archive — same rule as
    // `sync-core-config` and `anchor-balance`, so the reading stays re-derivable
    // from the raw store without a chain.
    let meta_key = raw_store::keys::metadata(&cfg.id, spec);
    let metadata = match raw.get(&meta_key) {
        Ok(blob) => blob,
        Err(raw_store::RawStoreError::NotFound(_)) => {
            let blob = source.metadata_at(height).await.map_err(|e| anyhow::anyhow!(e))?;
            raw.put(&meta_key, &blob, "sync-broker-config")?;
            tracing::info!(chain = %cfg.id, spec, "metadata archived while reading broker config");
            blob
        }
        Err(e) => return Err(e.into()),
    };

    // BOTH READS ARE REQUIRED AND AN ABSENCE IS AN ERROR. A chain running
    // `pallet-broker` with no `Status` has not started sales, but it also cannot
    // be distinguished from a wrong key derivation — and recording "0 cores"
    // would make every entitlement ratio silently infinite, which is the same
    // failure `sync-core-config` refuses one file over.
    let status_bytes = source
        .storage_at(&ab::status_key(), hash)
        .await
        .map_err(|e| anyhow::anyhow!(e))?
        .with_context(|| {
            format!(
                "{}.{} is absent from state at #{height} — either sales have never started on \
                 this chain or the key derivation is wrong, and those must not be recorded as the \
                 same thing",
                ab::BROKER_STORAGE_PALLET,
                ab::STATUS_ENTRY
            )
        })?;
    let configuration_bytes = source
        .storage_at(&ab::configuration_key(), hash)
        .await
        .map_err(|e| anyhow::anyhow!(e))?
        .with_context(|| {
            format!(
                "{}.{} is absent from state at #{height} — the sale geometry a price curve is \
                 evaluated against has no other source, and a reading not taken cannot be taken \
                 later",
                ab::BROKER_STORAGE_PALLET,
                ab::CONFIGURATION_ENTRY
            )
        })?;

    // AND THE THIRD READ, WHOSE ABSENCE IS A FACT RATHER THAN A FAILURE.
    //
    // `Broker.SaleInfo` is an `OptionQuery` StorageValue: before the first sale
    // ever starts there is no key at all, so `None` here means "sales have not
    // started" and is recorded as such. That is the one place this command
    // differs from the two reads above, which treat an absence as a wrong key
    // derivation — and the difference is load-bearing, because `first_core = 0`
    // would move every reserved system core into the bulk market and put the
    // delta's waste on the wrong side of the boundary.
    //
    // It is read HERE rather than in a later slice because `first_core` has a
    // reader now (the delta's reserved-vs-market split), and because
    // `cores_sold` moves on every purchase with NO EVENT — a reading not taken
    // cannot be taken later, and Phase 4's backfill is the deadline.
    let sale_info_bytes = source
        .storage_at(&ab::sale_info_key(), hash)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    if sale_info_bytes.is_none() {
        tracing::warn!(
            chain = %cfg.id,
            height,
            "{}.{} is ABSENT from state — recording that sales have not started. This is a fact, \
             not a failure, and it is NOT the same as first_core = 0",
            ab::BROKER_STORAGE_PALLET,
            ab::SALE_INFO_ENTRY
        );
    }

    let view = ab::decode_broker_config(
        &metadata,
        &status_bytes,
        &configuration_bytes,
        sale_info_bytes.as_deref(),
    )
    .map_err(|e| anyhow::anyhow!(e))?;
    dotlens_node::broker_pg::insert_broker_config(
        pool,
        &cfg.id,
        height,
        view.core_count,
        &view.status,
        &view.configuration,
        view.first_core,
        view.sale_info.as_ref(),
        spec,
    )
    .await?;
    println!(
        "broker config {chain} @#{height} (spec {spec}): core_count={} first_core={}",
        view.core_count,
        match view.first_core {
            Some(c) => c.to_string(),
            None => "none (SaleInfo absent — sales have not started)".to_string(),
        }
    );
    println!(
        "  this reading DATES the entitlement denominator, and it is also the CROSS-CHECK: \
         compare it against the relay's own num_cores at a nearby height (sync-core-config). Two \
         chains agreeing on one number is what makes the entitlement-vs-occupancy delta a \
         comparison rather than two unrelated ratios."
    );
    println!(
        "  `first_core` is the boundary between RESERVED system cores and the bulk market, and \
         /v1/coretime/<network>/delta uses it to say which side idle entitlement sits on. It \
         moves every sale, and this reading is dated on THIS chain's heights — which cannot be \
         ordered against a relay window."
    );
    Ok(())
}

#[cfg(not(all(feature = "pg", feature = "live")))]
async fn run_sync_broker_config(
    _: &Registry,
    _: &Backends,
    _: &dyn RawStore,
    _: &str,
    _: Option<u64>,
) -> Result<()> {
    anyhow::bail!("sync-broker-config requires the `pg` and `live` features")
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

/// `0x…` (or bare hex) SCALE bytes — a `RuntimeCall` for `simulate-call`, a
/// `VersionedXcm` for `simulate-xcm`. Rejected loudly rather than truncated:
/// half a call decodes into a different call, and half a program into a
/// different program.
/// `0x`-prefix a hex identifier that may have been copied from a raw-store path,
/// where the prefix is not part of the key. Lower-cased for the same reason
/// `normalize_call_hash` is: `0X…` is what `to_uppercase()` produces and it must
/// not 404.
///
/// GATED TO MATCH ITS ONLY CALLER (slice 5 carry-in). `run_simulate_forwarded`
/// is `#[cfg(all(feature = "pg", feature = "live"))]` and this was not, so
/// `cargo check --workspace --no-default-features` warned it was never used —
/// a warning that is invisible under `--all-features`, which is exactly the
/// build-matrix blind spot slice 9's review found twice.
#[cfg(all(feature = "pg", feature = "live"))]
fn prefixed(hash: &str) -> String {
    let t = hash.trim().to_ascii_lowercase();
    let body = t.strip_prefix("0x").unwrap_or(&t);
    format!("0x{body}")
}

fn decode_scale_hex(hex_str: &str, what: &str) -> Result<Vec<u8>> {
    let trimmed = hex_str.trim();
    let body = trimmed.strip_prefix("0x").unwrap_or(trimmed);
    let bytes = hex::decode(body).with_context(|| format!("{what} bytes are not hex"))?;
    anyhow::ensure!(!bytes.is_empty(), "{what} bytes are empty");
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
    opts: &SimOptions,
) -> Result<()> {
    use adapter_substrate::source::SubstrateSource;
    use dotlens_node::sim_pg::PgSimStore;
    use dotlens_node::sim_run::SubstrateDryRunner;

    if opts.tier.as_deref() == Some(sim::TIER_FORK) {
        return run_simulate_fork(
            registry, backends, raw, chain, call_bytes, origin_spec, height, referendum, opts,
        )
        .await;
    }

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
        r.api_version
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".into()),
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
    let forwarded = r
        .forwarded_xcms
        .as_ref()
        .and_then(|f| f.as_array())
        .map(Vec::len)
        .unwrap_or(0);
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
    opts: &SimOptions,
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
        opts,
    )
    .await
}


/// simulate-call/-referendum `--tier fork`: one Tier 2 run.
///
/// IT ALWAYS CREATES A JOB ROW, and then runs it through the SAME
/// `sim::run_job` the worker calls. `--queue` stops after the row. There is
/// deliberately no synchronous path beside the queued one: two implementations
/// of "run a Tier 2 simulation" that can disagree about what a Tier 2 answer is
/// is the defect class this project has found in four separate slices, and the
/// queue is a TRIGGER rather than a second engine.
#[cfg(all(feature = "pg", feature = "live"))]
#[allow(clippy::too_many_arguments)]
async fn run_simulate_fork(
    registry: &Registry,
    backends: &Backends,
    raw: &dyn RawStore,
    chain: &str,
    call_bytes: Vec<u8>,
    origin_spec: &str,
    height: Option<u64>,
    referendum: Option<(String, i64)>,
    opts: &SimOptions,
) -> Result<()> {
    use dotlens_node::sim_pg::{PgJobStore, PgSimStore};
    use sim::JobStore;

    let pool = backends
        .pool
        .as_ref()
        .context("a fork simulation requires DATABASE_URL")?;
    let cfg = registry
        .chain(chain)
        .with_context(|| format!("unknown chain: {chain}"))?;
    // Parsed HERE and not in the worker: turning `signed:13UVJ…` into 32 bytes
    // is adapter knowledge, and a job whose origin cannot be parsed should be
    // refused by whoever wrote it rather than discovered by a worker an hour
    // later.
    sim::OriginSpec::parse(origin_spec, |s| {
        adapter_substrate::accounts::parse_account(s).map_err(|e| e.to_string())
    })
    .map_err(|e| anyhow::anyhow!(e))?;

    let jobs = PgJobStore::new(pool.clone());
    let call_hash = format!(
        "0x{}",
        hex::encode(adapter_substrate::calls::blake2_256(&call_bytes))
    );
    let id = jobs
        .enqueue(&sim::NewSimJob {
            chain_id: cfg.id.clone(),
            tier: sim::TIER_FORK.to_string(),
            at_height: height,
            call: call_bytes,
            call_hash: call_hash.clone(),
            origin_spec: origin_spec.to_string(),
            override_specs: opts.override_specs.clone(),
            signer: match &opts.signer {
                None => None,
                Some(s) => Some(
                    adapter_substrate::accounts::parse_account(s)
                        .map_err(|e| anyhow::anyhow!("--signer {s}: {e}"))?,
                ),
            },
            requested_by: Some("cli".into()),
            note: referendum
                .as_ref()
                .map(|(class, id)| format!("referendum {chain}/{class}/{id}")),
            max_attempts: 1,
        })
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    if opts.queue {
        println!("queued fork job {id} on {chain} ({call_hash})");
        println!("  run it with FORK_JOBS=1, or without --queue to run it here");
        return Ok(());
    }

    // Claimed BY ID, so a worker and this command cannot both take one job — and,
    // more importantly, so this command cannot take somebody ELSE's. `claim`
    // otherwise returns the oldest queued job, and taking one only to bail would
    // have spent its single attempt and held a lease on it.
    let job = jobs
        .claim("cli", fork_lease_secs(), fork_max_concurrent(), Some(id))
        .await
        .map_err(|e| anyhow::anyhow!(e))?
        .with_context(|| {
            format!(
                "job {id} could not be claimed: the concurrency cap ({}) is full, so it stays \
                 queued and a worker will take it. A fork run is a Node process and a WASM \
                 execution; the cap is what keeps that bounded",
                fork_max_concurrent()
            )
        })?;

    let store = PgSimStore::new(pool.clone());
    let run = run_one_fork_job(registry, backends, raw, &jobs, &store, &job).await?;
    let r = &run.record;

    if let Some((class, id)) = &referendum {
        println!("referendum {chain}/{class}/{id}");
    }
    println!(
        "fork {chain} at #{} (spec {}, metadata v{}){}",
        r.at_height,
        r.spec_version,
        r.metadata_version,
        if run.cached { " [recorded earlier]" } else { "" }
    );
    println!(
        "  call     {} ({})",
        r.call_summary.as_deref().unwrap_or("?"),
        r.call_hash
    );
    println!("  origin   {} → {}", r.origin_spec, r.origin_json["resolved"]);
    if let Some(overrides) = r.overrides.as_ref().and_then(|o| o.as_array()) {
        println!(
            "  COUNTERFACTUAL — {} storage key(s) injected; this is NOT what the chain did",
            overrides.len()
        );
        for o in overrides {
            println!(
                "    {} \n      was {}\n      now {}",
                o["resolved"].as_str().unwrap_or("?"),
                o["before"].as_str().unwrap_or("(absent)"),
                o["value"].as_str().unwrap_or("(deleted)")
            );
        }
    }
    println!("  status   {}", r.status);
    if let Some(e) = &r.dispatch_error {
        println!("  error    {e}");
    }
    if let Some(n) = &r.note {
        println!("  note     {n}");
    }
    println!("  events   {}", r.event_count);
    for ev in r.emitted_events.as_array().into_iter().flatten() {
        println!("    - {}", ev["name"].as_str().unwrap_or("?"));
    }
    println!(
        "  diff     {} ({} entr{})",
        r.diff_status.as_deref().unwrap_or("?"),
        r.storage_diff_count.unwrap_or(0),
        if r.storage_diff_count == Some(1) { "y" } else { "ies" }
    );
    for e in r.storage_diff.as_ref().and_then(|d| d.as_array()).into_iter().flatten() {
        println!(
            "    {} [{}]{}",
            e["readable"].as_str().unwrap_or("?"),
            e["change"].as_str().unwrap_or("?"),
            if e["from_override"].as_bool() == Some(true) {
                "  (its BEFORE is our injected value, not the chain's)"
            } else {
                ""
            }
        );
    }
    println!(
        "  built    {} — A FORK'S BLOCK: no canonical chain has this hash",
        r.built_block_hash.as_deref().unwrap_or("?")
    );
    println!(
        "  harness  {}",
        r.harness
            .as_ref()
            .and_then(|h| h.get("version"))
            .map(|v| v.to_string())
            .unwrap_or_else(|| "unknown version".into())
    );
    println!("  evidence {}", r.raw_location);
    Ok(())
}

#[cfg(all(feature = "pg", feature = "live"))]
fn fork_max_concurrent() -> u32 {
    env_or("SIM_FORK_MAX_CONCURRENT", "1").parse().unwrap_or(1)
}

#[cfg(all(feature = "pg", feature = "live"))]
fn fork_lease_secs() -> u32 {
    env_or("SIM_FORK_LEASE_SECS", "900").parse().unwrap_or(900)
}

/// Run one claimed job. THE one place a Tier 2 run happens, whether the trigger
/// was a CLI invocation or the worker loop.
#[cfg(all(feature = "pg", feature = "live"))]
async fn run_one_fork_job(
    registry: &Registry,
    backends: &Backends,
    raw: &dyn RawStore,
    jobs: &dotlens_node::sim_pg::PgJobStore,
    store: &dotlens_node::sim_pg::PgSimStore,
    job: &sim::SimJob,
) -> Result<sim::SimRun> {
    use adapter_substrate::source::SubstrateSource;
    use dotlens_node::fork_run::{ForkConfig, SubstrateForkRunner};

    let cfg = registry
        .chain(&job.chain_id)
        .with_context(|| format!("unknown chain: {}", job.chain_id))?;
    let endpoint = cfg
        .endpoints
        .rpc
        .first()
        .cloned()
        .with_context(|| format!("chain {} has no rpc endpoint to fork from", job.chain_id))?;
    let origin = sim::OriginSpec::parse(&job.origin_spec, |s| {
        adapter_substrate::accounts::parse_account(s).map_err(|e| e.to_string())
    })
    .map_err(|e| anyhow::anyhow!(e))?;

    let source = SubstrateSource::new(&cfg.id, cfg.endpoints.rpc.clone())
        .map_err(|e| anyhow::anyhow!(e))?;
    let runner = SubstrateForkRunner::new(
        &cfg.id,
        &source,
        endpoint,
        raw,
        cfg.ss58_prefix.unwrap_or(0),
        ForkConfig::from_env(),
    );
    sim::run_job(
        &runner,
        store,
        jobs,
        raw,
        backends.receipts.as_ref(),
        job,
        origin,
    )
    .await
    .map_err(|e| anyhow::anyhow!(e))
}

/// sim-jobs: the read side of the queue, from the shell.
#[cfg(feature = "pg")]
async fn run_sim_jobs(backends: &Backends, chain: &str, status: Option<&str>) -> Result<()> {
    let pool = backends
        .pool
        .as_ref()
        .context("sim-jobs requires DATABASE_URL")?;
    let jobs = dotlens_node::sim_pg::list_jobs(pool, chain, status, 50).await?;
    if jobs.is_empty() {
        println!("no simulation jobs on {chain}{}", match status {
            Some(s) => format!(" with status '{s}'"),
            None => String::new(),
        });
        return Ok(());
    }
    for j in &jobs {
        println!(
            "{:>6}  {:<9} {:<8} {} {}{}",
            j.id,
            j.status,
            j.tier,
            j.call_hash,
            j.origin_spec,
            if j.override_specs.is_empty() {
                String::new()
            } else {
                format!("  [{} override(s) — COUNTERFACTUAL]", j.override_specs.len())
            }
        );
        if let Some(e) = &j.error {
            println!("        error: {e}");
        }
        if let (Some(h), Some(i)) = (&j.result_at_block_hash, &j.result_input_hash) {
            println!("        result: /v1/sim/{chain}/calls/{} ({h} {i})", j.call_hash);
        }
    }
    Ok(())
}

/// FORK_JOBS=1: drain the Tier 2 queue.
///
/// The loop is deliberately dumb — claim, run, repeat — because everything that
/// could go wrong is already a property of the row: the cap is enforced at claim
/// time inside one transaction, the lease makes a crashed worker's job
/// reclaimable, and `max_attempts` decides whether a failure is retried. A
/// worker that made those decisions itself would be a second policy beside the
/// one in the table.
#[cfg(all(feature = "pg", feature = "live"))]
fn spawn_fork_job_worker(registry: &Arc<Registry>, backends: &Arc<Backends>, raw: &Arc<dyn RawStore>) {
    if !env_flag("FORK_JOBS") {
        tracing::info!("fork job worker disabled (set FORK_JOBS=1 to enable)");
        return;
    }
    let Some(pool) = backends.pool.clone() else {
        tracing::warn!("FORK_JOBS=1 but no DATABASE_URL — the queue lives in Postgres");
        return;
    };
    let registry = registry.clone();
    let backends = backends.clone();
    let raw = raw.clone();
    let poll = env_or("POLL_INTERVAL_SECS", "6").parse::<u64>().unwrap_or(6);
    let worker = format!(
        "{}#{}",
        hostname_or("node"),
        std::process::id()
    );
    tokio::spawn(async move {
        use dotlens_node::sim_pg::{PgJobStore, PgSimStore};
        use sim::JobStore;
        let jobs = PgJobStore::new(pool.clone());
        let store = PgSimStore::new(pool);
        loop {
            let claimed = jobs
                .claim(&worker, fork_lease_secs(), fork_max_concurrent(), None)
                .await;
            match claimed {
                Ok(Some(job)) => {
                    tracing::info!(job = job.id, chain = %job.chain_id, "fork job claimed");
                    // A failed run has ALREADY been recorded against the row by
                    // `sim::run_job`; the worker logs it and carries on rather
                    // than dying, because one bad request must not stop a queue.
                    if let Err(e) =
                        run_one_fork_job(&registry, &backends, raw.as_ref(), &jobs, &store, &job)
                            .await
                    {
                        // `sim::run_job` records its own failures — but everything
                        // BEFORE it (unknown chain, no rpc endpoint, an origin that
                        // does not parse) happens in `run_one_fork_job` and would
                        // otherwise leave the row `running` until its lease expired.
                        // A setup failure is deterministic, so it is REFUSED.
                        let _ = jobs.fail(job.id, &e.to_string(), true).await;
                        tracing::warn!(job = job.id, error = %e, "fork job did not produce a result");
                    }
                }
                Ok(None) => tokio::time::sleep(std::time::Duration::from_secs(poll)).await,
                Err(e) => {
                    tracing::warn!(error = %e, "fork job claim failed");
                    tokio::time::sleep(std::time::Duration::from_secs(poll)).await;
                }
            }
        }
    });
}

#[cfg(not(all(feature = "pg", feature = "live")))]
fn spawn_fork_job_worker(_registry: &Arc<Registry>, _backends: &Arc<Backends>, _raw: &Arc<dyn RawStore>) {
    if env_flag("FORK_JOBS") {
        tracing::warn!("built without `pg`+`live` — FORK_JOBS is IGNORED");
    }
}

#[cfg(not(feature = "pg"))]
async fn run_sim_jobs(_backends: &Backends, _chain: &str, _status: Option<&str>) -> Result<()> {
    anyhow::bail!("built without `pg`: the simulation queue lives in Postgres")
}

#[cfg(all(feature = "pg", feature = "live"))]
fn hostname_or(default: &str) -> String {
    std::env::var("HOSTNAME").unwrap_or_else(|_| default.to_string())
}

/// simulate-xcm: one Tier 1 preview of an arriving PROGRAM (Phase 3, slice 5).
///
/// The chain is named here for the same reason it is on `simulate-call`, and one
/// more: this is the RECEIVING side, so "which chain" is not a routing detail but
/// the entire question — the same program is accepted on one chain and rejected
/// at another's barrier.
#[cfg(all(feature = "pg", feature = "live"))]
#[allow(clippy::too_many_arguments)]
async fn run_simulate_xcm(
    registry: &Registry,
    backends: &Backends,
    raw: &dyn RawStore,
    chain: &str,
    origin_location: &str,
    program: Vec<u8>,
    height: Option<u64>,
    source: Option<sim::ProgramSource>,
) -> Result<()> {
    use adapter_substrate::source::SubstrateSource;
    use dotlens_node::sim_pg::PgXcmSimStore;
    use dotlens_node::sim_run::SubstrateDryRunner;

    let pool = backends
        .pool
        .as_ref()
        .context("simulate-xcm requires DATABASE_URL")?;
    let cfg = registry
        .chain(chain)
        .with_context(|| format!("unknown chain: {chain}"))?;
    let origin = sim::LocationSpec::parse(origin_location).map_err(|e| anyhow::anyhow!(e))?;

    let source_chain = SubstrateSource::new(&cfg.id, cfg.endpoints.rpc.clone())
        .map_err(|e| anyhow::anyhow!(e))?;
    let runner = SubstrateDryRunner::new(&cfg.id, &source_chain, raw, backends.receipts.as_ref());
    let store = PgXcmSimStore::new(pool.clone());

    let req = sim::XcmSimRequest {
        chain_id: cfg.id.clone(),
        at_height: height,
        origin,
        program,
        source,
    };
    let run = sim::run_xcm_simulation(&runner, &store, raw, backends.receipts.as_ref(), &req)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    print_xcm_run(&run);
    Ok(())
}

#[cfg(all(feature = "pg", feature = "live"))]
fn print_xcm_run(run: &sim::XcmSimRun) {
    let r = &run.record;
    println!(
        "simulate-xcm {} at #{} (spec {}, DryRunApi v{}){}",
        r.chain_id,
        r.at_height,
        r.spec_version,
        r.api_version,
        if run.cached { " [recorded earlier]" } else { "" }
    );
    println!(
        "  program  {} ({})",
        r.program_summary.as_deref().unwrap_or("?"),
        r.program_hash
    );
    println!("  from     {} {}", r.origin_ref, r.origin_location);
    println!("  outcome  {}", r.status);
    if let Some(e) = &r.xcm_error {
        println!("  error    {e}");
    }
    if let Some(n) = &r.note {
        println!("  note     {n}");
    }
    println!("  events   {}", r.event_count);
    for ev in r.emitted_events.as_array().into_iter().flatten() {
        println!("    - {}", ev["name"].as_str().unwrap_or("?"));
    }
    println!("  evidence {}", r.raw_location);
}

/// simulate-forwarded: follow a recorded call simulation's OWN messages to the
/// chains they are addressed to (Phase 3, slice 5) — the stitch.
///
/// THE ATTRIBUTION IS A PRECONDITION, NOT A GARNISH. A recorded run with no
/// baseline is refused rather than followed, because on the relay the forwarded
/// list is 74 messages that belong to other people, and previewing them here
/// would attribute somebody else's traffic to the referendum being examined.
/// Slice 1 measured that; this command is where believing it costs something.
#[cfg(all(feature = "pg", feature = "live"))]
async fn run_simulate_forwarded(
    registry: &Registry,
    backends: &Backends,
    raw: &dyn RawStore,
    chain: &str,
    at_block_hash: &str,
    input_hash: &str,
    height: Option<u64>,
) -> Result<()> {
    use dotlens_node::sim_run::{origin_of, resolve_destination, SubstrateDryRunner};

    let pool = backends
        .pool
        .as_ref()
        .context("simulate-forwarded requires DATABASE_URL")?;
    let cfg = registry
        .chain(chain)
        .with_context(|| format!("unknown chain: {chain}"))?;

    // Both coordinates are stored WITH the `0x` prefix and printed WITHOUT it in
    // the evidence path (`raw/<chain>/sim/<block>/<input>/…`), which is exactly
    // where a person copies them from. Normalizing here is the difference
    // between a working paste and a "no recorded simulation" that looks like the
    // run never happened.
    let at_block_hash = &prefixed(at_block_hash);
    let input_hash = &prefixed(input_hash);

    let subject = dotlens_node::sim_pg::simulation_at(
        pool,
        &cfg.id,
        at_block_hash,
        input_hash,
        sim::TIER_DRY_RUN,
    )
    .await?
    .with_context(|| {
        format!(
            "no recorded simulation {input_hash} at {at_block_hash} on {chain} — run \
             `simulate-call` (or `simulate-referendum`) first; this command never starts one"
        )
    })?;

    let baseline_hash = subject.baseline_input_hash.clone().context(
        "this simulation was recorded without a baseline, so its forwarded_xcms is not \
         attributable to the call — on the relay that list is dominated by messages already \
         in flight. Re-run the simulation to record a baseline; following an unattributed \
         list would preview other people's traffic as this call's",
    )?;
    let baseline = dotlens_node::sim_pg::simulation_at(
        pool,
        &cfg.id,
        at_block_hash,
        &baseline_hash,
        sim::TIER_DRY_RUN,
    )
    .await?
    .with_context(|| format!("the recorded baseline {baseline_hash} is missing"))?;

    // Both are dry_run rows by construction here (this command follows a
    // `dry_run_call`'s forwarded list), so a missing list is a contradiction and
    // is refused rather than defaulted to empty — an empty baseline would
    // attribute every ambient message to the run.
    let subject_forwarded = subject
        .forwarded_xcms
        .as_ref()
        .context("this simulation has no forwarded_xcms — only the dry_run tier produces one")?;
    let baseline_forwarded = baseline
        .forwarded_xcms
        .as_ref()
        .context("this simulation's baseline has no forwarded_xcms")?;
    let attribution = sim::attribute_forwarded(subject_forwarded, baseline_forwarded);
    println!(
        "forwarded from {} at #{} ({}): {} message(s) total, {} already in flight, \
         {} attributable to this call",
        cfg.id,
        subject.at_height,
        subject.call_summary.as_deref().unwrap_or("?"),
        attribution.total_messages,
        attribution.ambient_messages,
        attribution.attributed_messages
    );
    if attribution.destinations.is_empty() {
        println!("  nothing to follow: this call queues no messages of its own");
        return Ok(());
    }

    // The archived response is where the message BYTES come from. The database
    // row holds our rendering of them, and re-encoding a rendering would be a
    // guess about what the rendering dropped.
    let response = raw.get(&subject.raw_location).with_context(|| {
        format!(
            "the archived response {} is missing, so no forwarded message can be lifted back \
             out of it",
            subject.raw_location
        )
    })?;
    let source_rpc = adapter_substrate::source::SubstrateSource::new(
        &cfg.id,
        cfg.endpoints.rpc.clone(),
    )
    .map_err(|e| anyhow::anyhow!(e))?;
    let source_runner =
        SubstrateDryRunner::new(&cfg.id, &source_rpc, raw, backends.receipts.as_ref());
    let ctx = source_runner
        .context_for(subject.spec_version, subject.metadata_version)
        .map_err(|e| anyhow::anyhow!(e))?;

    for destination in &attribution.destinations {
        let target = match resolve_destination(registry, cfg, &destination.destination) {
            Ok(t) => t,
            Err(why) => {
                // THE COVERAGE EDGE, STATED. Not a skip and not a failure: the
                // journey is followed as far as dotlens can see and then stops
                // on purpose, naming where it stopped.
                println!("  ↦ STOPS HERE: {why}");
                continue;
            }
        };
        let Some(origin) = origin_of(target, cfg) else {
            println!(
                "  ↦ STOPS HERE: the registry cannot say how {} would address {} — no origin \
                 location to preview it from",
                target.id, cfg.id
            );
            continue;
        };

        for message in &destination.messages {
            let lifted = ctx
                .forwarded_program(&response, destination.destination_index, message.message_index)
                .map_err(|e| anyhow::anyhow!(e))?;
            println!(
                "  ↦ {} (as {}) · destination {} message {}",
                target.id,
                origin.as_spec(),
                destination.destination_index,
                message.message_index
            );
            run_simulate_xcm(
                registry,
                backends,
                raw,
                &target.id,
                &origin.as_spec(),
                lifted.bytes,
                height,
                Some(sim::ProgramSource {
                    chain_id: cfg.id.clone(),
                    at_block_hash: subject.at_block_hash.clone(),
                    input_hash: subject.input_hash.clone(),
                    forwarded_index: destination.destination_index as u32,
                    message_index: message.message_index as u32,
                }),
            )
            .await?;
        }
    }
    Ok(())
}

#[cfg(not(all(feature = "pg", feature = "live")))]
#[allow(clippy::too_many_arguments)]
async fn run_simulate(
    _: &Registry, _: &Backends, _: &dyn RawStore, _: &str, _: Vec<u8>, _: &str, _: Option<u64>,
    _: Option<(String, i64)>, _: &SimOptions,
) -> Result<()> {
    anyhow::bail!("simulate-call requires the `pg` and `live` features")
}

#[cfg(not(all(feature = "pg", feature = "live")))]
#[allow(clippy::too_many_arguments)]
async fn run_simulate_xcm(
    _: &Registry, _: &Backends, _: &dyn RawStore, _: &str, _: &str, _: Vec<u8>, _: Option<u64>,
    _: Option<sim::ProgramSource>,
) -> Result<()> {
    anyhow::bail!("simulate-xcm requires the `pg` and `live` features")
}

#[cfg(not(all(feature = "pg", feature = "live")))]
async fn run_simulate_forwarded(
    _: &Registry, _: &Backends, _: &dyn RawStore, _: &str, _: &str, _: &str, _: Option<u64>,
) -> Result<()> {
    anyhow::bail!("simulate-forwarded requires the `pg` and `live` features")
}

#[cfg(not(all(feature = "pg", feature = "live")))]
#[allow(clippy::too_many_arguments)]
async fn run_simulate_referendum(
    _: &Registry, _: &Backends, _: &dyn RawStore, _: &str, _: &str, _: i64, _: &str,
    _: Option<u64>, _: &SimOptions,
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
    let report = dotlens_node::assets_pg::sync_assets(pool, raw, &source, cfg, height).await?;
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
    // The three numbers 0019 added, printed unconditionally rather than only
    // when non-zero: the gap between `absolutized` and `total` is the population
    // a cross-chain consolidation cannot add up, and a line that disappears when
    // it is healthy is a line nobody learns to read.
    println!(
        "  identity: {} of {} assets carry an absolute (observer-free) name",
        report.absolutized,
        report.total()
    );
    if report.native_alias_skipped > 0 {
        println!(
            "  {} registry entr(y/ies) name the chain's NATIVE token — folded onto \
             the `native` row rather than given a second key",
            report.native_alias_skipped
        );
    }
    if report.erc20_unanchorable > 0 {
        println!(
            "  {} asset(s) are Erc20: registered, named and located, and NOT \
             anchorable — their balances live in pallet_evm storage, which is \
             outside this slice (see core.assets.asset_type)",
            report.erc20_unanchorable
        );
    }
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
        dotlens_node::assets_pg::snapshot_holdings(pool, raw, &source, cfg, height).await?;
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
    if report.skipped_unanchorable > 0 {
        println!(
            "  {} asset(s) NOT PROBED: their balances are not in a pallet this \
             sweep reads (Erc20 — pallet_evm storage). Not probed is not zero, \
             and this line is the difference",
            report.skipped_unanchorable
        );
    }
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
