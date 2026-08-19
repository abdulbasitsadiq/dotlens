//! The live Tier 2 runner: `sim::ForkRunner` over a chopsticks subprocess
//! (Phase 3, slice 8).
//!
//! Wiring only, exactly like `sim_run.rs`: the orchestration is in `sim`
//! (prepare → cache → archive → dispatch → archive → interpret → record), the
//! SCALE and the metadata walking are in `adapter_substrate::fork`, and the
//! `dev_*` vocabulary is in `adapter_substrate::chopsticks`. What is here is the
//! part that needs all of them at once: a process, its port, its lifetime, and
//! the ordering of the six RPC calls that turn "run this call under this origin
//! at this block" into a block somebody can read.
//!
//! ---------------------------------------------------------------------------
//! THE SEQUENCE, and every step is here because the one before it made it
//! possible:
//!
//!   1. Resolve the state on the REAL chain — height → block hash → spec
//!      version. The fork is then pinned by HASH, so `at_block_hash` on the row
//!      means exactly what it means on a Tier 1 row.
//!   2. Read what the real chain holds at every key the counterfactual is about
//!      to overwrite. This is the `before` that makes the fabrication legible,
//!      and it has to happen HERE, against the real endpoint, because after step
//!      4 that value is gone.
//!   3. Start the fork at that hash.
//!   4. DECIDE THE AGENDA ANCHOR from the chain's own live agenda key space —
//!      `System.Number` and `ParachainSystem.LastRelayChainBlockNumber` are two
//!      different number lines and nothing in metadata says which one
//!      `pallet_scheduler` counts in (slice 9's blocker 1).
//!   5. `dev_setStorage` — the overrides, then one scheduled task plus the
//!      resume point that guarantees it is swept.
//!   6. `dev_dryRun` — ONE call: `initialize_block` (where the task fires) → the
//!      chain's own inherents → the vehicle extrinsic → `finalize_block`. Then
//!      the BEFORE side of each changed key, at the parent.
//!
//! `dev_newBlock`/`dev_runBlock` are gone from this path: slice 8's drill proved
//! they do not compose on a parachain, and `dev_dryRun` creates the inherents
//! rather than replaying a built block, which is why it does.
//!
//! ---------------------------------------------------------------------------
//! WHAT THAT ONE CALL RETURNS, CORRECTED. Slice 9 wrote here that the diff and
//! the events are "the same answer rather than two reads that can disagree", and
//! that it is therefore "structurally impossible for the events and the diff to
//! describe different executions". Measured at its own verification: FALSE. The
//! diff covers the `apply_extrinsic` phase only — `Core_initialize_block` and the
//! inherents are consumed into storage layers inside chopsticks and never
//! returned — so on the SCHEDULED route, whose entire purpose is dispatching a
//! privileged call in `on_initialize`, the diff omits everything the simulation
//! is about.
//!
//! The events looked like they proved the opposite because of one detail:
//! `apply_extrinsic` WRITES `System.Events`, appending to the list already
//! there, so the single events key in the diff carries the whole block while
//! every other key in it carries one extrinsic. Both halves really do come from
//! one execution — they just describe different amounts of it.
//!
//! NOT WORKED AROUND, RECORDED. The status is
//! [`fork::DIFF_STATUS_EXTRINSIC_ONLY`], the row says so, and the response says
//! what to read instead. See that constant for the source this was read from and
//! for the two routes that were measured and refused.
//!
//! ---------------------------------------------------------------------------
//! FIVE OPERATIONAL FACTS, each of which cost something to find out:
//!
//!   * **`PORT` in the environment OVERRIDES `--port`.** chopsticks' CLI
//!     middleware applies the env var AFTER merging argv, so a box with `PORT`
//!     set — every PaaS, most CI — silently listens somewhere else and the
//!     harness reads as "did not come up". The child's environment has it
//!     REMOVED rather than the flag trusted.
//!   * **A busy port is not an error to chopsticks.** It walks `port+1 … port+9`
//!     and listens on the first free one, saying so only in its log. We bind a
//!     free port ourselves and pass it, so a collision surfaces as a failed
//!     connection to the port we asked for rather than as a fork answering
//!     somewhere we are not looking. Loud, not silent.
//!   * **Readiness is an RPC probe, not a log line.** The ready line goes
//!     through pino-pretty and its exact rendering is not a contract. The log is
//!     still captured — when a fork fails to start, the log is the only evidence
//!     — and archived beside the run.
//!   * **The child's output must be DRAINED.** A subprocess whose stdout pipe
//!     fills blocks forever, and a fork that hangs at 40% of a state fetch looks
//!     exactly like a slow one. Both streams are read continuously into a capped
//!     buffer.
//!   * **The sqlite cache is per (chain, block).** chopsticks fetches state
//!     lazily and persists it, so the second counterfactual at one block pays
//!     almost no RPC — which is the whole reason Tier 2 is affordable against
//!     public endpoints that this project has already had one backfill killed
//!     by. TWO CONCURRENT RUNS AT ONE BLOCK WOULD SHARE THAT FILE, and nothing
//!     here prevents it: what prevents it today is that
//!     `SIM_FORK_MAX_CONCURRENT` defaults to 1 and the claim is serialised, so
//!     one worker runs one job at a time. Raising the cap above 1 is safe only
//!     across DIFFERENT (chain, block) pairs; the guard for the same pair is an
//!     advisory lock held for the run's duration and it is not written yet.
//!     Stated as a known gap rather than as protection that exists.

use adapter_substrate::chopsticks::{ChopsticksClient, ChopsticksError};
use adapter_substrate::fork::{self, StorageKeyIndex};
use adapter_substrate::source::SubstrateSource;
use async_trait::async_trait;
use ingest::live::ChainSource;
use raw_store::RawStore;
use sim::{
    ForkOutcome, ForkOverride, ForkRequest, ForkRunner, PreparedFork, SimError, SimEvent,
    TIER_FORK,
};

/// How many changed keys get their BEFORE value read back.
///
/// Each one is an RPC round trip into the fork (which may itself fetch from
/// upstream), so an unbounded diff on a runtime-upgrade block would be hundreds
/// of them. Above the cap the entries are still listed with their AFTER value
/// and the response says the before side was not read — "we did not look" stated
/// rather than rendered as "it did not exist", which is the same distinction
/// `diff_status` draws one level up.
const DEFAULT_MAX_BEFORE_READS: usize = 250;

/// The Substrate `System.Events` key. Substrate-protocol knowledge, and this
/// file is node wiring rather than an adapter — so it is DERIVED from the
/// adapter's own hasher rather than written down, and the derivation is the
/// same one `assets::map_prefix` performs for every other storage read here.
fn system_events_key() -> Vec<u8> {
    adapter_substrate::assets::map_prefix("System", "Events")
}

pub struct ForkConfig {
    /// How chopsticks is invoked. A COMMAND rather than a pinned npm spec,
    /// because how the operator installs it is their business and `npx` fetching
    /// from the network at run time is not a dependency this project wants
    /// inside a simulation.
    pub command: String,
    pub extra_args: Vec<String>,
    /// Where the lazy-fetch sqlite caches live. Deliberately NOT under the raw
    /// store: the raw store is write-once and content-addressed, and this is a
    /// mutable cache.
    pub db_dir: std::path::PathBuf,
    pub startup_timeout_secs: u64,
    pub max_before_reads: usize,
}

impl ForkConfig {
    pub fn from_env() -> Self {
        let get = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
        Self {
            command: get("CHOPSTICKS_CMD", "chopsticks"),
            extra_args: get("CHOPSTICKS_ARGS", "")
                .split_whitespace()
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect(),
            db_dir: std::path::PathBuf::from(get("SIM_FORK_DB_DIR", "data/chopsticks")),
            startup_timeout_secs: get("SIM_FORK_STARTUP_SECS", "180")
                .parse()
                .unwrap_or(180),
            max_before_reads: get("SIM_FORK_MAX_BEFORE_READS", "")
                .parse()
                .unwrap_or(DEFAULT_MAX_BEFORE_READS),
        }
    }
}

pub struct SubstrateForkRunner<'a> {
    chain_id: &'a str,
    /// The REAL chain. Used to pin the state and to read the pre-override
    /// values, and for nothing else — everything after step 3 talks to the fork.
    source: &'a SubstrateSource,
    /// The endpoint the fork is told to fork FROM. Recorded in `harness`,
    /// because a fork of a pruned endpoint and a fork of an archive endpoint are
    /// different answers at the same height.
    endpoint: String,
    raw: &'a dyn RawStore,
    config: ForkConfig,
    ss58_prefix: u16,
}

impl<'a> SubstrateForkRunner<'a> {
    pub fn new(
        chain_id: &'a str,
        source: &'a SubstrateSource,
        endpoint: String,
        raw: &'a dyn RawStore,
        ss58_prefix: u16,
        config: ForkConfig,
    ) -> Self {
        Self {
            chain_id,
            source,
            endpoint,
            raw,
            config,
            ss58_prefix,
        }
    }

    /// An ARCHIVED metadata blob and the version it is.
    ///
    /// THIS TIER DOES NOT NEED v15, and saying so is worth a paragraph. Tier 1
    /// had to pay the v15 debt because v14 carries no runtime-API section and a
    /// dry run cannot be built without one. Tier 2 calls no runtime API: it needs
    /// a type registry and a storage-entry list, and v14 has both. So the
    /// preference order is "whatever is already on disk" — v15 first only so that
    /// a fork row and a dry-run row at one spec share a lineage stamp — and a
    /// chain that has never had a Tier 1 simulation can still be forked from the
    /// v14 blob every live tick has archived since Phase 1.
    ///
    /// Nothing is FETCHED here. A missing blob is a named, fixable gap
    /// ("run a live tick, or a Tier 1 simulation, on this spec first") rather
    /// than a silent network call in the middle of a prepare.
    fn archived_metadata(&self, spec: u32) -> Result<(Vec<u8>, u32), SimError> {
        for version in [15u32, 16] {
            let key = raw_store::keys::metadata_at_version(self.chain_id, spec, version);
            if let Ok(blob) = self.raw.get(&key) {
                return Ok((blob, version));
            }
        }
        let key = raw_store::keys::metadata(self.chain_id, spec);
        match self.raw.get(&key) {
            Ok(blob) => Ok((blob, 14)),
            Err(e) => Err(SimError::Encode(format!(
                "no archived metadata for {} at spec {spec} ({key}: {e}). Tier 2 reads the \
                 runtime's own metadata to build the scheduled task, to resolve overrides and to \
                 read the diff back — archive one first with a live tick (LIVE_INGEST=1) or with \
                 any Tier 1 simulation on this spec",
                self.chain_id
            ))),
        }
    }
}

/// A running fork. Killed on drop, because chopsticks has no shutdown RPC and a
/// leaked Node process holding a sqlite file is how the next run at that state
/// fails for a reason nobody can see.
struct Harness {
    child: tokio::process::Child,
    client: ChopsticksClient,
    log: std::sync::Arc<tokio::sync::Mutex<String>>,
    port: u16,
    command_line: String,
    version: Option<String>,
    db_path: String,
}

impl Harness {
    async fn start(
        config: &ForkConfig,
        chain_id: &str,
        endpoint: &str,
        block_hash: &str,
    ) -> Result<Self, SimError> {
        std::fs::create_dir_all(&config.db_dir).map_err(|e| {
            SimError::Source(format!(
                "cannot create the fork cache directory {}: {e}",
                config.db_dir.display()
            ))
        })?;
        let db_path = config
            .db_dir
            .join(format!(
                "{chain_id}-{}.sqlite",
                block_hash.trim_start_matches("0x")
            ))
            .display()
            .to_string();

        let port = free_port()?;
        let mut args: Vec<String> = vec![
            format!("--endpoint={endpoint}"),
            format!("--block={block_hash}"),
            format!("--port={port}"),
            format!("--db={db_path}"),
            // WITHOUT THIS NEITHER ROUTE RUNS. Both vehicles reach the chain as an
            // extrinsic through `dev_dryRun` — the subject itself on the
            // `dry_run_extrinsic` route, a no-op remark on the `scheduled` one —
            // and dotlens holds no private key for anybody, so the signature is
            // FAKED. chopsticks rejects a faked signature unless the mock host is
            // on, with "Cannot fake signature because mock signature host is not
            // enabled", which surfaces as a refusal that reads like the signer
            // being unfunded rather than like a missing flag.
            //
            // It is recorded in `fork_mocked_surfaces` for the same reason it is
            // passed here: a run in which any signature is accepted did not check
            // signatures, and that is a property of the answer, not of the setup.
            "--mock-signature-host".to_string(),
        ];
        args.extend(config.extra_args.iter().cloned());
        let command_line = format!("{} {}", config.command, args.join(" "));

        let version = tokio::process::Command::new(&config.command)
            .arg("--version")
            .output()
            .await
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .filter(|s| !s.is_empty());

        let mut child = tokio::process::Command::new(&config.command)
            .args(&args)
            // THE ENV VAR THAT OVERRIDES THE FLAG. See the module header.
            .env_remove("PORT")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| {
                SimError::Source(format!(
                    "could not start the fork harness ('{}'): {e}. Tier 2 shells out to \
                     chopsticks; set CHOPSTICKS_CMD if it is installed under another name",
                    config.command
                ))
            })?;

        let log = std::sync::Arc::new(tokio::sync::Mutex::new(String::new()));
        drain(child.stdout.take(), log.clone());
        drain(child.stderr.take(), log.clone());

        // Readiness by PROBE. The deadline is generous because the first fork at
        // a state pulls its storage lazily over the network; the second, against
        // the same sqlite, is fast.
        let url = format!("ws://127.0.0.1:{port}");
        let deadline =
            std::time::Instant::now() + std::time::Duration::from_secs(config.startup_timeout_secs);
        let client = loop {
            if let Ok(Some(status)) = child.try_wait() {
                let tail = log.lock().await.clone();
                return Err(SimError::Source(format!(
                    "the fork harness exited before it was ready ({status}). Command: \
                     {command_line}\n--- harness log ---\n{tail}"
                )));
            }
            if let Ok(c) = ChopsticksClient::connect(&url).await {
                if c.system_chain().await.is_ok() {
                    break c;
                }
            }
            if std::time::Instant::now() >= deadline {
                let tail = log.lock().await.clone();
                let _ = child.kill().await;
                return Err(SimError::Source(format!(
                    "the fork harness did not answer on {url} within {}s. Command: \
                     {command_line}\n--- harness log ---\n{tail}",
                    config.startup_timeout_secs
                )));
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        };

        Ok(Self {
            child,
            client,
            log,
            port,
            command_line,
            version,
            db_path,
        })
    }

    async fn log_text(&self) -> String {
        self.log.lock().await.clone()
    }

    async fn stop(&mut self) {
        let _ = self.child.kill().await;
    }
}

fn drain(
    stream: Option<impl tokio::io::AsyncRead + Unpin + Send + 'static>,
    log: std::sync::Arc<tokio::sync::Mutex<String>>,
) {
    let Some(stream) = stream else { return };
    tokio::spawn(async move {
        use tokio::io::AsyncBufReadExt;
        let mut lines = tokio::io::BufReader::new(stream).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let mut guard = log.lock().await;
            // Capped: a harness that loops printing is not allowed to become a
            // memory leak, and the interesting part of a failure is the end.
            if guard.len() > 64 * 1024 {
                let cut = guard.len() - 32 * 1024;
                *guard = guard[cut..].to_string();
            }
            guard.push_str(&line);
            guard.push('\n');
        }
    });
}

/// Bind :0, read the port, drop the listener. A tiny race remains between the
/// drop and chopsticks' bind, and losing it produces a connection failure to a
/// port we named — which is loud. The alternative, letting chopsticks pick, is
/// silent: it walks to port+1 and only its log says so.
fn free_port() -> Result<u16, SimError> {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0))
        .map_err(|e| SimError::Source(format!("cannot reserve a port for the fork: {e}")))?;
    let port = listener
        .local_addr()
        .map_err(|e| SimError::Source(format!("cannot read the reserved port: {e}")))?
        .port();
    drop(listener);
    Ok(port)
}

fn chops(e: ChopsticksError) -> SimError {
    match e {
        ChopsticksError::Shape { .. } => SimError::Decode(e.to_string()),
        other => SimError::Source(other.to_string()),
    }
}

#[async_trait]
impl ForkRunner for SubstrateForkRunner<'_> {
    async fn prepare_fork(&self, req: &ForkRequest) -> Result<PreparedFork, SimError> {
        // ---- 1. pin the state on the REAL chain
        let height = match req.at_height {
            Some(h) => h,
            None => self
                .source
                .finalized_height()
                .await
                .map_err(|e| SimError::Source(e.to_string()))?,
        };
        let block_hash = self
            .source
            .block_hash(height)
            .await
            .map_err(|e| SimError::Source(e.to_string()))?;
        let at_block_hash = format!("0x{}", hex::encode(block_hash.as_ref()));
        let rv = self
            .source
            .runtime_version_info(block_hash)
            .await
            .map_err(|e| SimError::Source(e.to_string()))?;
        let (metadata, metadata_version) = self.archived_metadata(rv.spec_version)?;
        let index = StorageKeyIndex::from_metadata(&metadata)
            .map_err(|e| SimError::Encode(e.to_string()))?;

        // The call is decoded here for its SUMMARY and, more importantly, as a
        // check that these bytes are a RuntimeCall for THIS runtime at all —
        // scheduling bytes that are not a call produces a `CallUnavailable` two
        // steps later, which reads like a harness fault.
        let decoded = adapter_substrate::calls::decode_call(&metadata, &req.call)
            .map_err(|e| SimError::Encode(format!(
                "these bytes are not a RuntimeCall for {} at spec {}: {e}",
                self.chain_id, rv.spec_version
            )))?;

        // ---- 2. resolve the overrides, and read what the REAL chain holds
        let mut resolved = Vec::new();
        let mut records: Vec<ForkOverride> = Vec::new();
        for spec_text in &req.override_specs {
            let spec = fork::OverrideSpec::parse(spec_text)
                .map_err(|e| SimError::Encode(e.to_string()))?;
            let r = fork::resolve_override(&index, &spec, spec_text)
                .map_err(|e| SimError::Encode(e.to_string()))?;
            let before = self
                .source
                .storage_at(&r.key, block_hash)
                .await
                .map_err(|e| SimError::Source(e.to_string()))?;
            let described = index.describe(&r.key);
            let decode = |bytes: &[u8]| {
                described
                    .value_type
                    .and_then(|ty| index.decode_value(ty, bytes).ok())
            };
            records.push(ForkOverride {
                spec: r.spec.clone(),
                resolved: r.resolved.clone(),
                key: format!("0x{}", hex::encode(&r.key)),
                value: r.value.as_ref().map(|v| format!("0x{}", hex::encode(v))),
                before: before.as_ref().map(|b| format!("0x{}", hex::encode(b))),
                decoded_before: before.as_deref().and_then(decode),
                decoded_after: r.value.as_deref().and_then(decode),
            });
            resolved.push(r);
        }

        // ---- 4. the agenda anchor, decided from the REAL chain's own data
        //
        // Read here rather than on the fork, because it is a property of the
        // chain at this block and the fork has not started yet — and because a
        // refusal should happen before a Node process is spawned.
        let route = match req.origin {
            sim::OriginSpec::Signed(_) => sim::ROUTE_DRY_RUN_EXTRINSIC,
            _ => sim::ROUTE_SCHEDULED,
        };
        let agenda_anchor = if route == sim::ROUTE_SCHEDULED {
            let prefix =
                adapter_substrate::assets::map_prefix(fork::SCHEDULER_PALLET, fork::AGENDA_ENTRY);
            let keys = self
                .source
                .storage_keys_paged(&prefix, 64, None, block_hash)
                .await
                .map_err(|e| SimError::Source(e.to_string()))?;
            let heights = fork::agenda_key_heights(&index, &keys);
            let relay = match index
                .entry(fork::PARACHAIN_SYSTEM_PALLET, fork::LAST_RELAY_NUMBER_ENTRY)
            {
                None => None,
                Some(entry) => {
                    let raw = self
                        .source
                        .storage_at(&entry.prefix, block_hash)
                        .await
                        .map_err(|e| SimError::Source(e.to_string()))?;
                    raw.and_then(|b| {
                        index
                            .decode_value(entry.value_type, &b)
                            .ok()
                            .and_then(|v| v.as_u64())
                    })
                }
            };
            let anchor = fork::decide_agenda_anchor(height, relay, &heights)
                .map_err(|e| SimError::Encode(e.to_string()))?;
            tracing::info!(
                chain = %self.chain_id,
                provider = anchor.provider.as_str(),
                at_parent = anchor.at_parent,
                written_at = anchor.written_at,
                "agenda anchor decided: {}", anchor.decided_by
            );
            Some(anchor)
        } else {
            None
        };

        // ---- the canonical request, and the two hashes taken over it
        //
        // The ORIGIN in the input hash is its ENCODED form, against the type the
        // scheduler declares — not its rendering, which is a name that two
        // runtimes can spell differently for one origin. These are the exact
        // bytes `dispatch_fork` will inject.
        let (encoded_origin, origin_json) =
            fork::origin_bytes_for_scheduler(&metadata, &req.origin)
                .map_err(|e| SimError::Encode(e.to_string()))?;

        let request = fork::fork_input_bytes(&encoded_origin, &req.call, &resolved);
        let input_hash = format!(
            "0x{}",
            hex::encode(adapter_substrate::calls::blake2_256(&request))
        );
        let override_hash = if resolved.is_empty() {
            None
        } else {
            Some(format!(
                "0x{}",
                hex::encode(adapter_substrate::calls::blake2_256(
                    &fork::canonical_override_bytes(&resolved)
                ))
            ))
        };

        Ok(PreparedFork {
            chain_id: self.chain_id.to_string(),
            at_height: height,
            at_block_hash,
            spec_version: rv.spec_version,
            metadata_version,
            tier: TIER_FORK.to_string(),
            method: fork::FORK_METHOD.to_string(),
            input_hash,
            override_hash,
            overrides: records,
            call_hash: format!(
                "0x{}",
                hex::encode(adapter_substrate::calls::blake2_256(&req.call))
            ),
            call_summary: Some(decoded.summary),
            origin_spec: req.origin_spec.clone(),
            origin_json,
            request,
            dispatch_route: route.to_string(),
            agenda_anchor: agenda_anchor.as_ref().map(|a| a.to_json()),
            signer: req.signer,
        })
    }

    async fn dispatch_fork(&self, prepared: &PreparedFork) -> Result<Vec<u8>, SimError> {
        let (metadata, _) = self.archived_metadata(prepared.spec_version)?;
        let index = StorageKeyIndex::from_metadata(&metadata)
            .map_err(|e| SimError::Encode(e.to_string()))?;

        // Rebuilt from the SAME metadata at the SAME height as `prepare_fork`
        // resolved, so the origin that goes in is the origin the row records.
        // THE ORIGIN AND THE CALL BOTH COME OUT OF THE ARCHIVED REQUEST, not out
        // of a second parse. `prepare_fork` hashed those exact bytes into
        // `input_hash`, so re-deriving them here from `origin_spec` would be a
        // second encoder that can disagree with the one the cache key was taken
        // over — the row would then describe a counterfactual other than the one
        // injected, which is the failure this whole slice is arranged to prevent.
        let (encoded_origin, call) = fork::origin_and_call_from_request(&prepared.request)
            .map_err(|e| SimError::Decode(e.to_string()))?;
        // THE ROUTE DECIDES WHETHER THERE IS AN AGENDA AT ALL. A `signed:` origin
        // is an extrinsic and needs no scheduler; a privileged origin cannot be
        // expressed to any RPC and has to go through one.
        let dispatch = match &prepared.agenda_anchor {
            None => None,
            Some(anchor_json) => {
                let anchor = anchor_from_json(anchor_json)?;
                Some(
                    fork::scheduled_dispatch_with_origin_bytes(
                        &metadata,
                        &index,
                        &encoded_origin,
                        &call,
                        &anchor,
                    )
                    .map_err(|e| SimError::Encode(e.to_string()))?,
                )
            }
        };
        // On the scheduled route the extrinsic is a NO-OP whose only job is to
        // make the block execute; on the extrinsic route it IS the call.
        let vehicle = match &dispatch {
            Some(_) => fork::noop_call_bytes(&metadata).map_err(|e| SimError::Encode(e.to_string()))?,
            None => call.clone(),
        };

        let mut writes: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();
        let mut override_keys: Vec<Vec<u8>> = Vec::new();
        for o in &prepared.overrides {
            let key = hex_bytes(&o.key)?;
            let value = match &o.value {
                None => None,
                Some(v) => Some(hex_bytes(v)?),
            };
            override_keys.push(key.clone());
            writes.push((key, value));
        }
        if let Some(d) = &dispatch {
            for (k, v) in &d.writes {
                writes.push((k.clone(), Some(v.clone())));
            }
        }

        let mut harness = Harness::start(
            &self.config,
            self.chain_id,
            &self.endpoint,
            &prepared.at_block_hash,
        )
        .await?;

        let result = self
            .drive(
                &mut harness,
                prepared,
                &writes,
                &override_keys,
                dispatch.as_ref(),
                &vehicle,
                prepared.signer,
            )
            .await;
        let log = harness.log_text().await;
        harness.stop().await;

        // THE LOG IS ARCHIVED EITHER WAY, and especially on failure: a fork that
        // did not start leaves nothing else behind, and "it failed" without the
        // harness's own words is not evidence.
        // ONE stamp for both run-scoped keys, taken once so the log and the run
        // facts that points at it cannot land under different stamps.
        let run_stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or_default();

        // THE LOG IS A PROPERTY OF THE RUN, NOT OF (state, input), so its key
        // carries the stamp too. Slice 9 split the run facts out of the answer and
        // left the log on the shared key, so a second run at one state hit the
        // write-once refusal and its log — the only artifact a failed fork leaves —
        // was silently dropped with a warning.
        let log_key = raw_store::keys::simulation(
            self.chain_id,
            prepared.at_block_hash.trim_start_matches("0x"),
            prepared.input_hash.trim_start_matches("0x"),
            &format!("{}.run-{}.harness.log", prepared.method, run_stamp),
        );
        if let Err(e) = self.raw.put(&log_key, log.as_bytes(), "simulate-fork") {
            tracing::warn!(error = %e, key = %log_key, "harness log not archived — continuing");
        }

        // THE ARTIFACT IS SPLIT, and this is slice 9's blocker 3. The ANSWER is
        // archived under the write-once (chain, block, input) key and must
        // therefore contain only what is a function of those three — slice 8 put
        // `--port=58859` in it, so the bytes differed on every run BY
        // CONSTRUCTION, the re-put was refused forever, and after any
        // `docker compose down -v` every previously-simulated state became
        // permanently un-re-runnable.
        //
        // So the run-specific facts go to a SIBLING key that carries the input
        // hash of this attempt's own wall clock, and the answer keeps only the
        // engine's identity — version and mocked surfaces — which is the part
        // that is genuinely lineage.
        let run_key = raw_store::keys::simulation(
            self.chain_id,
            prepared.at_block_hash.trim_start_matches("0x"),
            prepared.input_hash.trim_start_matches("0x"),
            &format!("{}.run-{}.harness.json", prepared.method, run_stamp),
        );
        // The stable half of the run-facts location: everything up to the file
        // name. Two runs at one state share it; neither run's own stamp is in it.
        let run_facts_prefix = run_key
            .rsplit_once('/')
            .map(|(dir, _)| format!("{dir}/{}.run-*.harness.json", prepared.method))
            .unwrap_or_else(|| run_key.clone());
        let run_facts = serde_json::json!({
            "command": harness.command_line,
            "port": harness.port,
            "db": harness.db_path,
            "log": log_key,
        });
        if let Ok(bytes) = serde_json::to_vec_pretty(&run_facts) {
            if let Err(e) = self.raw.put(&run_key, &bytes, "simulate-fork") {
                tracing::warn!(error = %e, key = %run_key, "harness run facts not archived");
            }
        }

        let mut answer = result?;
        if let Some(obj) = answer.as_object_mut() {
            obj.insert(
                "harness".into(),
                serde_json::json!({
                    "tool": "chopsticks",
                    "version": harness.version,
                    "endpoint": self.endpoint,
                    "mocked": fork_mocked_surfaces(),
                    // A STABLE POINTER — a PREFIX, not this run's key.
                    //
                    // THIS IS BLOCKER 3, and slice 9 moved it rather than fixing
                    // it: the port came out of the answer and a
                    // `run-<millis>.harness.json` key went in, so the answer's
                    // bytes still differed on every run and the write-once re-put
                    // was still refused forever. Measured — a second run at one
                    // (chain, block, input) died on "refusing to overwrite
                    // immutable object", which is the exact failure the split was
                    // for. A clock is as run-specific as a port.
                    //
                    // So the answer names the PREFIX its run facts are archived
                    // under, which is a function of (state, input) and nothing
                    // else; the timestamped files sit under it and are listed, not
                    // linked.
                    "run_facts_prefix": run_facts_prefix,
                }),
            );
        }
        serde_json::to_vec_pretty(&answer)
            .map_err(|e| SimError::Encode(format!("serializing the harness answer: {e}")))
    }

    fn interpret_fork(
        &self,
        prepared: &PreparedFork,
        response: &[u8],
    ) -> Result<ForkOutcome, SimError> {
        let answer: serde_json::Value = serde_json::from_slice(response)
            .map_err(|e| SimError::Decode(format!("the archived harness answer is not JSON: {e}")))?;
        let (metadata, _) = self.archived_metadata(prepared.spec_version)?;
        let index = StorageKeyIndex::from_metadata(&metadata)
            .map_err(|e| SimError::Decode(e.to_string()))?;

        // ---- events, through the decoder every indexed block goes through
        let events_hex = answer
            .get("events")
            .and_then(|v| v.as_str())
            .ok_or_else(|| SimError::Decode("the harness answer carries no `events`".into()))?;
        let events_bytes = hex_bytes(events_hex)?;
        let decoder = adapter_substrate::frame_decoder::FrameDecoder::from_metadata_bytes(
            prepared.spec_version,
            self.ss58_prefix,
            &metadata,
        )
        .map_err(|e| SimError::Decode(format!("building the event decoder: {e}")))?;
        let decoded = decoder
            .decode_events(&events_bytes)
            .map_err(|e| SimError::Decode(format!("decoding the built block's events: {e}")))?;
        let named: Vec<(String, serde_json::Value)> = decoded
            .iter()
            .map(|e| (e.name.clone(), e.data.clone()))
            .collect();
        let events: Vec<SimEvent> = decoded
            .iter()
            .map(|e| SimEvent {
                name: e.name.clone(),
                data: e.data.clone(),
            })
            .collect();

        // ---- the dispatch verdict, from whichever event is the authority HERE
        //
        // The two routes model different things and are judged by different
        // events: the scheduled route by `Scheduler.Dispatched`, the extrinsic
        // route by the trailing `system.Extrinsic{Success,Failed}`. Reading the
        // scheduler's event on the extrinsic route reports a call that ran and
        // reverted as one that never ran.
        let (status, dispatch_error, note) =
            if prepared.dispatch_route == sim::ROUTE_DRY_RUN_EXTRINSIC {
                fork::read_extrinsic_outcome(&named)
            } else {
                fork::read_dispatch_outcome(&named)
            };

        // ---- the diff
        //
        // DERIVED FROM THE ANSWER, NOT COPIED OUT OF IT. `unwrap_or("unavailable")`
        // used to sit here, which would have read an answer whose shape changed
        // as "this build of the harness has no diff method" — sending somebody to
        // reinstall a tool that is working, which is the exact conflation the
        // `refused` value was added to prevent one slice ago.
        let diff_status = fork::diff_scope_from_answer(&answer)
            .map_err(|e| SimError::Decode(e.to_string()))?
            .to_string();
        let override_keys: Vec<Vec<u8>> = prepared
            .overrides
            .iter()
            .map(|o| hex_bytes(&o.key))
            .collect::<Result<_, _>>()?;
        // The keys the HARNESS wrote to make the dispatch happen — the agenda
        // entry and, for a large call, the preimage and its status. Their
        // `before` is our own injection and their `after` is the runtime
        // consuming it; unflagged they read as things the call did.
        let harness_keys: Vec<Vec<u8>> = answer
            .get("harness_keys")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str())
                    .filter_map(|s| hex_bytes(s).ok())
                    .collect()
            })
            .unwrap_or_default();
        // AN ABSENT DIFF IS NOT AN EMPTY ONE. A status that carries no bytes must
        // leave the column NULL rather than `Some([])` — a count of 0 beside
        // `diff_status: unavailable` is exactly the "we did not look" / "nothing
        // changed" conflation that migration 0021 and `fork_not_covered` both
        // promise is impossible.
        //
        // THE GATE IS A FUNCTION, NOT `== "decoded"`. Written as a literal it
        // silently blanks the column the moment a value is added to the
        // vocabulary — which is precisely what would have happened to
        // `extrinsic_only` this slice, on the one route the tier actually runs.
        //
        // AND A `diff` THAT IS PRESENT BUT UNREADABLE IS REFUSED, not silently
        // blanked. `.and_then(as_array)` collapsed "no diff key" with "a diff key
        // in a shape this version cannot read", and the second produced a NULL
        // column beside a status saying the bytes were read — the very
        // conflation `diff_is_present` exists to prevent, one line below it.
        let diff_entries = match answer.get("diff") {
            _ if !fork::diff_is_present(&diff_status) => None,
            None => None,
            Some(serde_json::Value::Array(entries)) => Some(entries),
            Some(other) => {
                return Err(SimError::Decode(format!(
                    "the archived answer records diff_status '{diff_status}', which says its \
                     bytes were read, but its `diff` is not an array: {other}"
                )))
            }
        };
        let (storage_diff, storage_diff_count) = match diff_entries {
            None => (None, None),
            Some(entries) => {
                let mut pairs = Vec::with_capacity(entries.len());
                for e in entries {
                    let key = hex_bytes(e.get("key").and_then(|k| k.as_str()).ok_or_else(|| {
                        SimError::Decode("a recorded diff entry has no key".into())
                    })?)?;
                    let before = match e.get("before").and_then(|v| v.as_str()) {
                        Some(s) => Some(hex_bytes(s)?),
                        None => None,
                    };
                    let after = match e.get("after").and_then(|v| v.as_str()) {
                        Some(s) => Some(hex_bytes(s)?),
                        None => None,
                    };
                    pairs.push(fork::DiffPair {
                        from_harness: harness_keys.iter().any(|k| *k == key),
                        before_read: e
                            .get("before_read")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(true),
                        key,
                        before,
                        after,
                    });
                }
                let decoded = fork::decode_diff(&index, &pairs, &override_keys);
                let count = decoded.len() as u32;
                (
                    Some(serde_json::to_value(&decoded).map_err(|e| {
                        SimError::Encode(format!("serializing the storage diff: {e}"))
                    })?),
                    Some(count),
                )
            }
        };

        Ok(ForkOutcome {
            status: status.as_str().to_string(),
            dispatch_ok: match status {
                fork::ForkStatus::Executed => Some(true),
                fork::ForkStatus::DispatchFailed => Some(false),
                // NOT `Some(false)`. "the call ran and reverted" and "the call
                // never ran" are different facts and a boolean cannot hold both.
                fork::ForkStatus::NotDispatched => None,
            },
            dispatch_error,
            events,
            effects: answer.clone(),
            storage_diff,
            storage_diff_count,
            diff_status,
            // NULL on this route, and that is a correction rather than a loss:
            // no block is built, and slice 8's measurement showed the value it
            // used to carry did not identify the counterfactual anyway
            // (chopsticks builds every block with stateRoot 0x00…0, so a faithful
            // run and a counterfactual shared one hash).
            built_block_hash: answer
                .get("built_block_hash")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            harness: answer.get("harness").cloned().unwrap_or(serde_json::json!({})),
            // The diff's own caveat travels with the row. Built in `drive` and
            // dropped on the floor before this fix, so the "only the first N
            // keys had their before value read" sentence never reached anybody.
            note: match (note, answer.get("diff_note").and_then(|v| v.as_str())) {
                (Some(a), Some(b)) => Some(format!("{a} · {b}")),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b.to_string()),
                (None, None) => None,
            },
        })
    }

    fn sim_version(&self) -> u32 {
        fork::FORK_VERSION
    }
}

impl SubstrateForkRunner<'_> {
    /// Steps 4–6. Split out so `dispatch_fork` can archive the harness log
    /// whatever happens in here.
    /// Steps 4–6, on the PARACHAIN-SAFE route.
    ///
    /// Slice 8 did `dev_setStorage` → `dev_newBlock` → `dev_runBlock`, and its
    /// drill proved the last two do not compose on a parachain: the built block
    /// omits `set_validation_data` and `dev_runBlock` re-applies extrinsics for
    /// real, so the runtime traps. `dev_dryRun` never replays a built block — it
    /// CREATES the inherents through the chain's own providers — which is what
    /// chopsticks' own preimage plugin does and is therefore safe by construction.
    ///
    /// It also collapses two reads into one: `dev_dryRun` does NOT exclude
    /// `System.Events` the way `dev_runBlock` does, so the events come out of the
    /// same diff and there is no second storage read to get out of step with it.
    ///
    /// THAT IS TRUE OF THE EVENTS AND NOT OF THE REST OF THE DIFF, and the
    /// difference is this slice's subject: the events key carries the whole
    /// block because `apply_extrinsic` appends to the list already there, while
    /// every other key carries the extrinsic's own writes. See the module header.
    async fn drive(
        &self,
        harness: &mut Harness,
        prepared: &PreparedFork,
        writes: &[(Vec<u8>, Option<Vec<u8>>)],
        override_keys: &[Vec<u8>],
        dispatch: Option<&fork::ScheduledDispatch>,
        extrinsic: &[u8],
        signer: [u8; 32],
    ) -> Result<serde_json::Value, SimError> {
        let parent = harness.client.set_storage(writes).await.map_err(chops)?;

        // ONE CALL. `on_initialize` runs and an injected scheduler task fires
        // in it — but the diff that comes back is the `apply_extrinsic` phase's,
        // NOT the block's. Only `System.Events` carries the whole block, because
        // `apply_extrinsic` appends to the list already there. See the module
        // header; this is the fact slice 9 had backwards.
        let pairs = match harness
            .client
            .dry_run_extrinsic_raw(extrinsic, &signer)
            .await
        {
            Ok(p) => p,
            // A validity failure is the harness REFUSING to apply the vehicle —
            // usually an unfunded signer — and it is reported as itself rather
            // than as an empty diff, because an empty diff would say "this call
            // changes nothing".
            Err(e) => {
                return Err(SimError::Source(format!(
                    "the fork refused to apply the dry-run extrinsic ({e}). On the scheduled \
                     route the extrinsic is a no-op whose only job is to make the block execute, \
                     so this is about the signer or the harness rather than about the call"
                )))
            }
        };

        // THE EVENTS COME OUT OF THE DIFF. `System.Events` is an ordinary storage
        // item and the dry run rewrites it, so its post-value IS the block's
        // event list — read here rather than fetched separately.
        //
        // THIS IS THE ONE KEY THAT IS WHOLE-BLOCK. Slice 9 argued from it that
        // the events and the diff "cannot describe different executions"; the
        // truth is narrower and is the reason this slice exists — they come from
        // one execution and describe different AMOUNTS of it, because every
        // other key in the answer is the extrinsic's alone.
        let events_key = system_events_key();
        let events = pairs
            .iter()
            .find(|(k, _)| *k == events_key)
            .and_then(|(_, v)| v.clone())
            .ok_or_else(|| {
                SimError::Decode(
                    "the dry run's storage diff carries no System.Events, so there is nothing to \
                     read the dispatch verdict from"
                        .into(),
                )
            })?;

        // The BEFORE side, read at the parent — which already carries every
        // injected write, which is why an entry whose key we injected is flagged.
        let mut entries: Vec<serde_json::Value> = Vec::new();
        let mut reads = 0usize;
        let mut capped = false;
        for (key, after) in &pairs {
            // System.Events is the answer, not a change worth diffing: it is
            // rewritten every block by construction and its "before" is last
            // block's events, which tells a reader nothing.
            if *key == events_key {
                continue;
            }
            let read_it = reads < self.config.max_before_reads;
            let before = if read_it {
                reads += 1;
                harness.client.storage_at(key, &parent).await.map_err(chops)?
            } else {
                capped = true;
                None
            };
            entries.push(serde_json::json!({
                "key": format!("0x{}", hex::encode(key)),
                "before": before.map(|b| format!("0x{}", hex::encode(b))),
                "before_read": read_it,
                "after": after.as_ref().map(|a| format!("0x{}", hex::encode(a))),
            }));
        }

        let route = if dispatch.is_some() {
            sim::ROUTE_SCHEDULED
        } else {
            sim::ROUTE_DRY_RUN_EXTRINSIC
        };
        Ok(serde_json::json!({
            "forked_at": prepared.at_block_hash,
            "parent": parent,
            "dispatch_route": route,
            "agenda_anchor": dispatch.map(|d| d.anchor.to_json()),
            "call_binding": dispatch.map(|d| d.call_binding),
            "injected_keys": writes.len(),
            "events": format!("0x{}", hex::encode(&events)),
            // THE METHOD, NOT THE VERDICT. What was DONE is a fact about this
            // run and belongs in the archived answer; what it MEANS is an
            // interpretation and belongs behind `FORK_VERSION`, where it can be
            // corrected without re-running a fork. Slice 9 archived the
            // interpretation — a flat `"diff_status": "decoded"` — and that is
            // why its rows over-claim: a `dev_dryRun` diff covers the
            // `apply_extrinsic` phase and nothing else, so on the scheduled
            // route it describes the no-op vehicle rather than the dispatch.
            // `fork::diff_scope_from_answer` reads this and still reads the old
            // spelling, so every already-archived answer re-interprets to the
            // truth with no chain and no harness.
            "diff_method": fork::DIFF_METHOD_DRY_RUN,
            "diff_note": capped.then(|| format!(
                "only the first {} changed keys had their BEFORE value read back; the rest are \
                 listed with their new value and no old one, which is 'not read' and not 'did \
                 not exist'",
                self.config.max_before_reads
            )),
            "diff": entries,
            "harness_keys": dispatch
                .map(|d| d.writes.iter().map(|(k, _)| format!("0x{}", hex::encode(k))).collect())
                .unwrap_or_else(Vec::new),
            "overridden_keys": override_keys.iter()
                .map(|k| format!("0x{}", hex::encode(k)))
                .collect::<Vec<_>>(),
        }))
    }
}

/// Rebuild the anchor decision `prepare_fork` recorded.
///
/// It is REPLAYED rather than re-decided, because the decision is part of the
/// question: re-reading the chain at dispatch time could reach a different answer
/// (the agenda moves) and the row would then describe a run other than the one
/// its own column claims. Same rule as the origin bytes coming out of the
/// archived request rather than out of a second parse.
fn anchor_from_json(v: &serde_json::Value) -> Result<fork::AgendaAnchor, SimError> {
    let num = |k: &str| -> Result<u64, SimError> {
        v.get(k)
            .and_then(|x| x.as_u64())
            .ok_or_else(|| SimError::Decode(format!("the recorded agenda anchor has no `{k}`")))
    };
    let provider = match v.get("provider").and_then(|p| p.as_str()) {
        Some("relay") => fork::AnchorProvider::Relay,
        Some("local") => fork::AnchorProvider::Local,
        other => {
            return Err(SimError::Decode(format!(
                "the recorded agenda anchor names an unknown provider {other:?}"
            )))
        }
    };
    Ok(fork::AgendaAnchor {
        provider,
        at_parent: num("at_parent")?,
        written_at: num("written_at")?,
        system_number: num("system_number")?,
        relay_number: v.get("relay_number").and_then(|x| x.as_u64()),
        agenda_keys_observed: v
            .get("agenda_keys_observed")
            .and_then(|x| x.as_u64())
            .unwrap_or(0) as usize,
        agenda_key_range: None,
        decided_by: v
            .get("decided_by")
            .and_then(|x| x.as_str())
            .unwrap_or("replayed from the prepared request")
            .to_string(),
    })
}

fn hex_bytes(s: &str) -> Result<Vec<u8>, SimError> {
    hex::decode(s.trim_start_matches("0x"))
        .map_err(|e| SimError::Decode(format!("'{s}' is not hex: {e}")))
}

/// What chopsticks does not model faithfully, in its own words.
///
/// Recorded on the ROW rather than only in the API's prose, because a row that
/// outlives this code is the thing somebody will read, and "which engine said
/// this and what did it fake" is lineage in the Invariant 3 sense.
pub fn fork_mocked_surfaces() -> Vec<&'static str> {
    vec![
        "mocked tx pool",
        // The signature is faked, so NOTHING about the signer is verified: not the
        // key, not that the account consented. Everything else in the transaction
        // pipeline (nonce, mortality, fee, weight) does run on the
        // `dry_run_extrinsic` route — which is exactly why that route's coverage
        // differs from the scheduled one's, and why this line is here rather than
        // in prose.
        "mocked signature host — any signature is accepted, the signer is not verified",
        "no real block finalization",
        "mocked inherents",
        "simulated XCM channels",
        "runtime constants cannot be changed without replacing the wasm",
    ]
}
