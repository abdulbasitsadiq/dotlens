//! Live ingestion: the generic, family-agnostic worker (ARCHITECTURE.md §14:
//! "ingest — fetchers, finality, backfill, checkpoints").
//!
//! RAW-FIRST (Invariant 1): this worker stores original bytes, receipts,
//! runtime-version lineage, and metadata blobs. It does NOT decode — canonical
//! decode of real SCALE arrives with the frame-decode slice and is rebuildable
//! from what this worker archives. Protocol specifics (how to fetch) live in
//! adapter crates behind the `ChainSource` trait; this file must never learn
//! what FRAME or metadata are beyond "an opaque blob with a version number".
//!
//! Only FINALIZED heights are ingested in this slice — no reorg handling is
//! needed yet. Unfinalized-head tracking is a later Phase 1 slice.

use crate::{
    should_process, Checkpoint, CheckpointError, CheckpointStore, IngestOutcome, ReceiptSink,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use raw_store::{keys, RawStore};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    #[error("rpc error: {0}")]
    Rpc(String),
    #[error("all endpoints failed: {0}")]
    Exhausted(String),
    #[error("block {0} not found (beyond finalized head?)")]
    NotFound(u64),
}

/// One raw artifact fetched for a block ("block.json", "events.scale", ...).
#[derive(Debug, Clone)]
pub struct RawArtifact {
    pub item: String,
    pub bytes: Vec<u8>,
}

/// Everything a source returns for one height. `runtime_version` comes with
/// the block so the worker can detect version boundaries without knowing how
/// versions are discovered (that's the adapter's business).
#[derive(Debug, Clone)]
pub struct FetchedBlock {
    pub height: u64,
    pub hash: String,
    pub parent_hash: String,
    pub runtime_version: u32,
    pub transaction_version: Option<u32>,
    pub artifacts: Vec<RawArtifact>,
}

/// The fetch side of a chain adapter (ARCHITECTURE.md §5), family-agnostic.
/// Implementations own endpoints, failover, and protocol details.
#[async_trait]
pub trait ChainSource: Send + Sync {
    async fn finalized_height(&self) -> Result<u64, SourceError>;
    async fn fetch_block(&self, height: u64) -> Result<FetchedBlock, SourceError>;
    /// The runtime context blob (Substrate: SCALE metadata) valid at `height`.
    async fn metadata_at(&self, height: u64) -> Result<Vec<u8>, SourceError>;
}

// ------------------------------------------------------- runtime version sink

#[derive(Debug, thiserror::Error)]
#[error("sink error: {0}")]
pub struct SinkError(pub String);

/// A detected runtime context era, ready for the lineage table
/// (`substrate.runtime_versions` for the Substrate family).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeContextRecord {
    pub chain_id: String,
    pub runtime_version: u32,
    pub transaction_version: Option<u32>,
    /// Version byte of the metadata blob (Substrate: 14|15|16), if parseable.
    pub metadata_version: Option<u8>,
    /// Raw-store key of the archived blob.
    pub metadata_location: String,
    /// Lowest block this version was OBSERVED at — refined downward by upserts;
    /// the true first block is discoverable later (binary search, next slices).
    pub first_seen_block: u64,
}

#[async_trait]
pub trait RuntimeVersionSink: Send + Sync {
    /// Idempotent: recording the same (chain, version) again must be a no-op
    /// except for lowering `first_seen_block`.
    async fn record(&self, rec: &RuntimeContextRecord) -> Result<(), SinkError>;
}

#[derive(Default)]
pub struct NoopRuntimeVersionSink;

#[async_trait]
impl RuntimeVersionSink for NoopRuntimeVersionSink {
    async fn record(&self, _rec: &RuntimeContextRecord) -> Result<(), SinkError> {
        Ok(())
    }
}

// ----------------------------------------------------------- chain head sink

/// Where the head this worker already reads gets durably recorded
/// (`core.chain_head`, migration 0029).
///
/// # WHY THIS IS ITS OWN SINK AND NOT A METHOD ON `CheckpointStore`
///
/// A checkpoint means PROCESSED UP TO and a head means OBSERVED AT, and the two
/// verbs want opposite guards: `CheckpointStore::advance` refuses a
/// non-advancing write and deliberately does not move `updated_at` unless the
/// height moves, which is exactly what a head must do on every tick. 0029
/// rejects the conflation at the table; accepting it at the trait would leave
/// the conflation in place one layer up.
///
/// The shape it copies is [`RuntimeVersionSink`]: a fact the live worker
/// observes in passing, handed to a durable sink, with a Noop for DB-less runs.
///
/// # WHAT A DROPPED OBSERVATION COSTS, AND WHY IT IS SAFE TO DROP
///
/// [`NoopChainHeadSink`] discards it, and a `record` that fails is logged and
/// stepped over rather than failing the tick — see [`tick`]. Both are safe for
/// one reason and only that reason: **the loss renders as NULL or as an ageing
/// row, never as zero.** The freshness reader reports "no chain head has ever
/// been RECORDED here" or "this observation is N seconds old", so a dropped head
/// cannot render as being level with the chain.
///
/// **It is not, however, DISTINGUISHABLE.** A discarded head and a chain nobody
/// has ever followed produce the same null, and the reader says exactly that
/// rather than picking one. The wording is `RECORDED` and never `OBSERVED`
/// throughout, because the follower did observe it — the observation is what was
/// thrown away.
#[async_trait]
pub trait ChainHeadSink: Send + Sync {
    /// Record that at `observed_at` the source reported `finalized_height` as
    /// this chain's finalized head.
    ///
    /// **LATEST WRITE WINS, INCLUDING DOWNWARD.** `finalized_height` rotates
    /// endpoints and a lagging one legitimately reports a lower head than the
    /// previous call did. Taking `greatest()` would manufacture a monotonic head
    /// no single observation supports; the reader renders the delta signed
    /// instead and says so in the payload.
    ///
    /// Primitives rather than a shared struct, because the READER of this row
    /// lives in `api` and `api` does not depend on `ingest` — the dependency
    /// runs generic ← protocol. A struct defined twice is the shape that drifts;
    /// three primitives cannot. The two SQL statements are pinned against each
    /// other by a write-then-read integration test rather than by a constant.
    async fn record(
        &self,
        chain_id: &str,
        finalized_height: u64,
        observed_at: DateTime<Utc>,
    ) -> Result<(), SinkError>;
}

/// For DB-less runs, and for the backfill, which observes no head at all.
///
/// **NOTHING LOGS THE DISCARDED HEAD** — an earlier draft of this comment said
/// "observable in logs only", which was false about the code beside it. The
/// observation is dropped without a trace, and what makes that acceptable is the
/// READ side rather than a log line: the freshness report says no chain head has
/// ever been RECORDED on this chain, which is true, and which is not the same
/// sentence as "nobody looked". [`ChainHeadSink`] carries the argument.
///
/// The consequence worth knowing: **a run with no database can never surface a
/// head**, because the writer here and `api::MemoryFreshnessIndex` are unrelated
/// stores with nothing between them. That is the same shape the checkpoint
/// stores already have and it is not this slice's to change.
#[derive(Default)]
pub struct NoopChainHeadSink;

#[async_trait]
impl ChainHeadSink for NoopChainHeadSink {
    async fn record(&self, _: &str, _: u64, _: DateTime<Utc>) -> Result<(), SinkError> {
        Ok(())
    }
}

/// One recorded observation, as the memory sink keeps it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordedHead {
    pub finalized_height: u64,
    pub observed_at: DateTime<Utc>,
    /// How many times `record` has been called for this chain.
    ///
    /// **It is here because the height cannot prove what has to be proved.** The
    /// case that matters is a CAUGHT-UP follower still refreshing the head, and
    /// in that case the height is unchanged by construction — only a counter (or
    /// the timestamp, which a fast test may not advance) can tell a tick that
    /// re-observed the same head from a tick that never looked.
    pub writes: u64,
}

/// In-memory head observations — a TEST DOUBLE, and nothing else uses it.
///
/// It is deliberately not described as the DB-less backend: the DB-less wiring
/// takes [`NoopChainHeadSink`], because nothing reads this store (the freshness
/// reader lives in `api` and cannot see it). Saying otherwise would name a
/// caller that does not exist.
#[derive(Default)]
pub struct MemoryChainHeadSink {
    rows: Mutex<HashMap<String, RecordedHead>>,
}

impl MemoryChainHeadSink {
    pub fn new() -> Self {
        Self::default()
    }

    /// The latest observation for `chain_id`, or `None` when none was recorded.
    pub fn get(&self, chain_id: &str) -> Option<RecordedHead> {
        self.rows
            .lock()
            .expect("chain head lock")
            .get(chain_id)
            .copied()
    }
}

#[async_trait]
impl ChainHeadSink for MemoryChainHeadSink {
    async fn record(
        &self,
        chain_id: &str,
        finalized_height: u64,
        observed_at: DateTime<Utc>,
    ) -> Result<(), SinkError> {
        let mut rows = self.rows.lock().map_err(|e| SinkError(e.to_string()))?;
        let entry = rows
            .entry(chain_id.to_string())
            .or_insert(RecordedHead {
                finalized_height,
                observed_at,
                writes: 0,
            });
        // LATEST WINS, INCLUDING DOWNWARD — the same rule as the Postgres
        // upsert, written the same way here so the two backends cannot disagree
        // about what "the head" means. No `max`: a lower finalized head from a
        // lagging endpoint is a real observation, and the reader is signed.
        entry.finalized_height = finalized_height;
        entry.observed_at = observed_at;
        entry.writes += 1;
        Ok(())
    }
}

/// Substrate metadata blobs start with the magic b"meta" then a version byte.
/// Returns None (never guesses) if the blob doesn't look like that.
pub fn metadata_version_byte(blob: &[u8]) -> Option<u8> {
    (blob.len() > 4 && &blob[0..4] == b"meta").then(|| blob[4])
}

// --------------------------------------------------------------- worker deps

#[derive(Debug, thiserror::Error)]
pub enum LiveError {
    #[error(transparent)]
    Source(#[from] SourceError),
    #[error(transparent)]
    Checkpoint(#[from] CheckpointError),
    #[error("raw store: {0}")]
    Raw(String),
    #[error("receipt sink: {0}")]
    Receipt(String),
    #[error(transparent)]
    Sink(#[from] SinkError),
}

pub struct IngestDeps<'a> {
    pub raw: &'a dyn RawStore,
    pub checkpoints: &'a dyn CheckpointStore,
    pub receipts: &'a dyn ReceiptSink,
    pub runtime_versions: &'a dyn RuntimeVersionSink,
    /// Written by [`tick`] only. `ingest_range` never touches it: a bounded
    /// backfill chunk observes no head, and recording one from a range command
    /// would date the chain's head to whenever somebody last ran a backfill.
    pub chain_head: &'a dyn ChainHeadSink,
}

/// Checkpoint module for the live/backfill RAW pipeline. Distinct from the
/// canonical "blocks" module: decode workers will re-walk raw independently.
pub const MODULE_LIVE: &str = "raw_blocks";
pub const MODULE_BACKFILL: &str = "raw_backfill";

/// Ingest heights `from..=to` (inclusive), resuming from the checkpoint if it
/// is already inside the range. Order per height: artifacts + receipts +
/// runtime lineage FIRST, checkpoint LAST — a crash re-processes (idempotent)
/// rather than skips (data loss). Returns the number of heights processed.
///
/// `last_runtime_version` is caller-owned so it survives across calls:
/// `follow` holds one across ticks (otherwise EVERY tick would re-download
/// the multi-MB metadata blob). Pass `&mut None` for one-shot runs; the one
/// redundant re-archive after a cold start is absorbed by the write-once raw
/// store and idempotent sink.
pub async fn ingest_range(
    chain_id: &str,
    source: &dyn ChainSource,
    deps: &IngestDeps<'_>,
    module: &str,
    from: u64,
    to: u64,
    last_runtime_version: &mut Option<u32>,
) -> Result<u64, LiveError> {
    // resume: skip the already-processed prefix without a per-height query
    let start = match deps.checkpoints.get(chain_id, module).await? {
        Some(cp) if cp.last_height >= from => cp.last_height + 1,
        _ => from,
    };
    let mut processed = 0u64;

    for height in start..=to {
        // guard stays (cheap safety net for concurrent workers / odd states)
        if should_process(deps.checkpoints, chain_id, module, height).await?
            == IngestOutcome::AlreadyProcessed
        {
            continue;
        }
        let fetched = source.fetch_block(height).await?;

        for artifact in &fetched.artifacts {
            let key = keys::block(chain_id, height, &artifact.item);
            let receipt = deps
                .raw
                .put(&key, &artifact.bytes, "live")
                .map_err(|e| LiveError::Raw(e.to_string()))?;
            deps.receipts
                .record(&receipt)
                .await
                .map_err(|e| LiveError::Receipt(e.to_string()))?;
        }

        // runtime-version boundary detection: archive metadata once per era.
        // After a cold start *last_runtime_version is None → one redundant
        // archive attempt, absorbed by write-once raw store + idempotent sink.
        if *last_runtime_version != Some(fetched.runtime_version) {
            let blob = source.metadata_at(height).await?;
            let meta_key = keys::metadata(chain_id, fetched.runtime_version);
            let receipt = deps
                .raw
                .put(&meta_key, &blob, "live")
                .map_err(|e| LiveError::Raw(e.to_string()))?;
            deps.receipts
                .record(&receipt)
                .await
                .map_err(|e| LiveError::Receipt(e.to_string()))?;
            deps.runtime_versions
                .record(&RuntimeContextRecord {
                    chain_id: chain_id.to_string(),
                    runtime_version: fetched.runtime_version,
                    transaction_version: fetched.transaction_version,
                    metadata_version: metadata_version_byte(&blob),
                    metadata_location: meta_key,
                    first_seen_block: height,
                })
                .await?;
            *last_runtime_version = Some(fetched.runtime_version);
        }

        deps.checkpoints
            .advance(Checkpoint {
                chain_id: chain_id.to_string(),
                module: module.to_string(),
                last_height: height,
                last_hash: fetched.hash.clone(),
                updated_at: chrono::Utc::now(),
            })
            .await?;
        processed += 1;
        tracing::info!(chain = %chain_id, height, module, "raw ingested");
    }
    Ok(processed)
}

/// One follow step: catch up from the checkpoint (or the tip, on first run —
/// history is backfill's job, not follow's) to the current finalized head.
pub async fn tick(
    chain_id: &str,
    source: &dyn ChainSource,
    deps: &IngestDeps<'_>,
    last_runtime_version: &mut Option<u32>,
) -> Result<u64, LiveError> {
    let target = source.finalized_height().await?;

    // RECORDED HERE, BEFORE THE CAUGHT-UP EARLY RETURN BELOW, AND THE ORDER IS
    // THE POINT. A follower with nothing to ingest takes that return on every
    // tick, and a caught-up follower is precisely when this row is the only
    // evidence it is still alive: the whole failure this observation exists to
    // expose is a dead follower whose frozen head makes `raw_behind_chain` fall
    // to 0 and read as "caught up with the chain". Recording after the return
    // would freeze the head exactly when the pipeline looked healthiest.
    //
    // A FAILURE HERE IS LOGGED AND STEPPED OVER RATHER THAN FAILING THE TICK.
    // Raw ingestion is the load-bearing job (Invariant 1) and a monitoring write
    // must not be able to stop it. The drop is not silent: the row stops being
    // refreshed, its age grows, and the freshness report renders that age beside
    // the delta — so this failure surfaces on the surface it feeds. Continuing
    // also drives `raw_behind_chain` NEGATIVE as the frontier climbs past the
    // frozen head, which the reader reports signed and explains as an
    // observation being behind us.
    if let Err(e) = deps.chain_head.record(chain_id, target, Utc::now()).await {
        tracing::warn!(
            chain = %chain_id, head = target, error = %e,
            "chain head observation not recorded — ingestion continues; \
             the freshness report will show this observation ageing"
        );
    }

    let from = match deps.checkpoints.get(chain_id, MODULE_LIVE).await? {
        Some(cp) if cp.last_height >= target => return Ok(0), // nothing new
        Some(cp) => cp.last_height + 1,
        None => target, // first run: start at the tip
    };
    ingest_range(
        chain_id,
        source,
        deps,
        MODULE_LIVE,
        from,
        target,
        last_runtime_version,
    )
    .await
}

/// Follow finalized heads forever, polling every `poll`. Errors are logged
/// and retried next tick (the source already rotates endpoints internally);
/// the caller owns cancellation (abort the task).
pub async fn follow(
    chain_id: &str,
    source: &dyn ChainSource,
    deps: &IngestDeps<'_>,
    poll: Duration,
) {
    let mut consecutive_failures = 0u32;
    // survives across ticks: metadata is re-archived only on real era changes
    let mut last_runtime_version: Option<u32> = None;
    loop {
        match tick(chain_id, source, deps, &mut last_runtime_version).await {
            Ok(n) => {
                consecutive_failures = 0;
                if n > 0 {
                    tracing::debug!(chain = %chain_id, blocks = n, "follow tick");
                }
            }
            Err(e) => {
                consecutive_failures += 1;
                tracing::warn!(chain = %chain_id, error = %e, consecutive_failures, "follow tick failed");
            }
        }
        // linear backoff on repeated failure, capped at 11× the poll interval
        let factor = 1 + consecutive_failures.min(10);
        tokio::time::sleep(poll * factor).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemoryCheckpointStore, ReceiptError};
    use raw_store::IngestReceipt;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// Scripted source: runtime v100 through height 5, v101 from height 6.
    struct MockSource {
        finalized: Mutex<u64>,
        fail_next: Mutex<u32>,
    }

    impl MockSource {
        fn new(finalized: u64) -> Self {
            Self {
                finalized: Mutex::new(finalized),
                fail_next: Mutex::new(0),
            }
        }
        fn version_for(height: u64) -> u32 {
            if height <= 5 {
                100
            } else {
                101
            }
        }
    }

    #[async_trait]
    impl ChainSource for MockSource {
        async fn finalized_height(&self) -> Result<u64, SourceError> {
            Ok(*self.finalized.lock().unwrap())
        }
        async fn fetch_block(&self, height: u64) -> Result<FetchedBlock, SourceError> {
            {
                let mut f = self.fail_next.lock().unwrap();
                if *f > 0 {
                    *f -= 1;
                    return Err(SourceError::Rpc("injected failure".into()));
                }
            }
            if height > *self.finalized.lock().unwrap() {
                return Err(SourceError::NotFound(height));
            }
            Ok(FetchedBlock {
                height,
                hash: format!("0x{height:064x}"),
                parent_hash: format!("0x{:064x}", height.saturating_sub(1)),
                runtime_version: Self::version_for(height),
                transaction_version: Some(1),
                artifacts: vec![
                    RawArtifact {
                        item: "block.json".into(),
                        bytes: format!("{{\"h\":{height}}}").into_bytes(),
                    },
                    RawArtifact {
                        item: "events.scale".into(),
                        bytes: vec![height as u8],
                    },
                ],
            })
        }
        async fn metadata_at(&self, height: u64) -> Result<Vec<u8>, SourceError> {
            let v = Self::version_for(height);
            let mut blob = b"meta".to_vec();
            blob.push(15); // pretend metadata v15
            blob.extend_from_slice(&v.to_le_bytes());
            Ok(blob)
        }
    }

    #[derive(Default)]
    struct MemReceipts(Mutex<Vec<IngestReceipt>>);
    #[async_trait]
    impl ReceiptSink for MemReceipts {
        async fn record(&self, r: &IngestReceipt) -> Result<(), ReceiptError> {
            self.0.lock().unwrap().push(r.clone());
            Ok(())
        }
    }

    #[derive(Default)]
    struct MemRuntimeSink(Mutex<HashMap<(String, u32), RuntimeContextRecord>>);
    #[async_trait]
    impl RuntimeVersionSink for MemRuntimeSink {
        async fn record(&self, rec: &RuntimeContextRecord) -> Result<(), SinkError> {
            let mut map = self.0.lock().unwrap();
            map.entry((rec.chain_id.clone(), rec.runtime_version))
                .and_modify(|e| e.first_seen_block = e.first_seen_block.min(rec.first_seen_block))
                .or_insert_with(|| rec.clone());
            Ok(())
        }
    }

    fn tmp_raw(tag: &str) -> (raw_store::FsRawStore, std::path::PathBuf) {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("dotlens-live-{}-{tag}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        (raw_store::FsRawStore::new(&dir), dir)
    }

    #[tokio::test]
    async fn range_ingest_archives_metadata_once_per_version() {
        let source = MockSource::new(10);
        let (raw, dir) = tmp_raw("range");
        let checkpoints = MemoryCheckpointStore::new();
        let receipts = MemReceipts::default();
        let versions = MemRuntimeSink::default();
        let heads = MemoryChainHeadSink::new();
        let deps = IngestDeps {
            raw: &raw,
            checkpoints: &checkpoints,
            receipts: &receipts,
            runtime_versions: &versions,
            chain_head: &heads,
        };

        let mut rv = None;
        let n = ingest_range("mockchain", &source, &deps, MODULE_BACKFILL, 1, 10, &mut rv)
            .await
            .unwrap();
        assert_eq!(n, 10);

        // metadata archived exactly once per era, with correct boundaries
        let map = versions.0.lock().unwrap();
        assert_eq!(map.len(), 2);
        let v100 = &map[&("mockchain".into(), 100)];
        assert_eq!(v100.first_seen_block, 1);
        assert_eq!(v100.metadata_version, Some(15));
        assert!(v100.metadata_location.contains("/meta/100/"));
        let v101 = &map[&("mockchain".into(), 101)];
        assert_eq!(v101.first_seen_block, 6);
        drop(map);

        // receipts: 2 artifacts × 10 blocks + 2 metadata blobs
        assert_eq!(receipts.0.lock().unwrap().len(), 22);

        // re-run: clean no-op
        let n = ingest_range(
            "mockchain",
            &source,
            &deps,
            MODULE_BACKFILL,
            1,
            10,
            &mut None,
        )
        .await
        .unwrap();
        assert_eq!(n, 0);

        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn tick_starts_at_tip_then_follows_finalized() {
        let source = MockSource::new(7);
        let (raw, dir) = tmp_raw("tick");
        let checkpoints = MemoryCheckpointStore::new();
        let receipts = MemReceipts::default();
        let versions = MemRuntimeSink::default();
        let heads = MemoryChainHeadSink::new();
        let deps = IngestDeps {
            raw: &raw,
            checkpoints: &checkpoints,
            receipts: &receipts,
            runtime_versions: &versions,
            chain_head: &heads,
        };

        // first tick: no checkpoint → tip only (history is backfill's job)
        let mut rv = None;
        assert_eq!(tick("mockchain", &source, &deps, &mut rv).await.unwrap(), 1);
        // nothing new → no-op
        assert_eq!(tick("mockchain", &source, &deps, &mut rv).await.unwrap(), 0);
        // finality advances → exactly the delta is ingested
        *source.finalized.lock().unwrap() = 12;
        assert_eq!(tick("mockchain", &source, &deps, &mut rv).await.unwrap(), 5);
        let cp = checkpoints
            .get("mockchain", MODULE_LIVE)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cp.last_height, 12);

        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn tick_error_leaves_checkpoint_resumable() {
        let source = MockSource::new(5);
        let (raw, dir) = tmp_raw("err");
        let checkpoints = MemoryCheckpointStore::new();
        let receipts = MemReceipts::default();
        let versions = MemRuntimeSink::default();
        let heads = MemoryChainHeadSink::new();
        let deps = IngestDeps {
            raw: &raw,
            checkpoints: &checkpoints,
            receipts: &receipts,
            runtime_versions: &versions,
            chain_head: &heads,
        };
        let mut rv = None;
        assert_eq!(tick("mockchain", &source, &deps, &mut rv).await.unwrap(), 1); // at tip=5

        *source.finalized.lock().unwrap() = 9;
        *source.fail_next.lock().unwrap() = 2; // heights 6 fails twice
        assert!(tick("mockchain", &source, &deps, &mut rv).await.is_err());
        assert!(tick("mockchain", &source, &deps, &mut rv).await.is_err());
        // third tick succeeds and picks up exactly where it left off
        assert_eq!(tick("mockchain", &source, &deps, &mut rv).await.unwrap(), 4);
        let cp = checkpoints
            .get("mockchain", MODULE_LIVE)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cp.last_height, 9);

        let _ = std::fs::remove_dir_all(dir);
    }

    /// Always fails. Proves the head write cannot stop ingestion.
    #[derive(Default)]
    struct FailingChainHeadSink;

    #[async_trait]
    impl ChainHeadSink for FailingChainHeadSink {
        async fn record(&self, _: &str, _: u64, _: DateTime<Utc>) -> Result<(), SinkError> {
            Err(SinkError("chain_head write refused".into()))
        }
    }

    /// **THE ORDERING THAT IS THE POINT OF THE SLICE.** `tick` returns early
    /// when the checkpoint is already at the finalized head, and that early
    /// return is taken on nearly every tick of a healthy follower. If the head
    /// were recorded after it, the observation would stop being refreshed
    /// exactly when the pipeline is caught up — and a frozen head makes
    /// `raw_behind_chain` read 0, which is "level with the chain", the
    /// flattering direction.
    ///
    /// The height cannot show this: it is unchanged by construction on a
    /// caught-up tick. The WRITE COUNT can.
    #[tokio::test]
    async fn a_caught_up_tick_still_records_the_head_because_that_is_when_it_matters() {
        let source = MockSource::new(7);
        let (raw, dir) = tmp_raw("head-early-return");
        let checkpoints = MemoryCheckpointStore::new();
        let receipts = MemReceipts::default();
        let versions = MemRuntimeSink::default();
        let heads = MemoryChainHeadSink::new();
        let deps = IngestDeps {
            raw: &raw,
            checkpoints: &checkpoints,
            receipts: &receipts,
            runtime_versions: &versions,
            chain_head: &heads,
        };

        let mut rv = None;
        assert_eq!(tick("mockchain", &source, &deps, &mut rv).await.unwrap(), 1);
        let first = heads.get("mockchain").expect("recorded on the first tick");
        assert_eq!(first.finalized_height, 7);
        assert_eq!(first.writes, 1);

        // Nothing new: this tick takes `return Ok(0)`.
        assert_eq!(tick("mockchain", &source, &deps, &mut rv).await.unwrap(), 0);
        let second = heads.get("mockchain").expect("still recorded");
        assert_eq!(
            second.finalized_height, 7,
            "the head has not moved, and that is the case being tested"
        );
        assert_eq!(
            second.writes, 2,
            "a caught-up follower is precisely when this row is the only evidence it is alive"
        );

        // And a head that goes DOWN is stored as observed, not maxed away: a
        // lagging endpoint is a real observation and the reader is signed.
        *source.finalized.lock().unwrap() = 3;
        assert_eq!(tick("mockchain", &source, &deps, &mut rv).await.unwrap(), 0);
        let third = heads.get("mockchain").expect("still recorded");
        assert_eq!(
            third.finalized_height, 3,
            "greatest() would invent a monotonic head no observation supports"
        );
        assert_eq!(third.writes, 3);

        let _ = std::fs::remove_dir_all(dir);
    }

    /// Raw ingestion is the load-bearing job (Invariant 1) and a monitoring
    /// write must not be able to stop it. The drop is not silent — the row stops
    /// being refreshed and the freshness report renders that age — so the
    /// failure surfaces on the surface it feeds.
    #[tokio::test]
    async fn a_head_sink_that_refuses_does_not_stop_ingestion() {
        let source = MockSource::new(9);
        let (raw, dir) = tmp_raw("head-fails");
        let checkpoints = MemoryCheckpointStore::new();
        let receipts = MemReceipts::default();
        let versions = MemRuntimeSink::default();
        let heads = FailingChainHeadSink;
        let deps = IngestDeps {
            raw: &raw,
            checkpoints: &checkpoints,
            receipts: &receipts,
            runtime_versions: &versions,
            chain_head: &heads,
        };

        let mut rv = None;
        assert_eq!(
            tick("mockchain", &source, &deps, &mut rv).await.unwrap(),
            1,
            "the tick must succeed and ingest"
        );
        *source.finalized.lock().unwrap() = 12;
        assert_eq!(tick("mockchain", &source, &deps, &mut rv).await.unwrap(), 3);
        let cp = checkpoints
            .get("mockchain", MODULE_LIVE)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cp.last_height, 12);

        let _ = std::fs::remove_dir_all(dir);
    }

    /// `ingest_range` records NO head, and the construction site that keeps it
    /// that way is the backfill's explicit `NoopChainHeadSink`. A bounded chunk
    /// observes no head, and a head dated to whenever somebody last ran a
    /// backfill would be worse than none.
    #[tokio::test]
    async fn a_range_ingest_records_no_head_at_all() {
        let source = MockSource::new(10);
        let (raw, dir) = tmp_raw("head-range");
        let checkpoints = MemoryCheckpointStore::new();
        let receipts = MemReceipts::default();
        let versions = MemRuntimeSink::default();
        let heads = MemoryChainHeadSink::new();
        let deps = IngestDeps {
            raw: &raw,
            checkpoints: &checkpoints,
            receipts: &receipts,
            runtime_versions: &versions,
            chain_head: &heads,
        };

        let mut rv = None;
        let n = ingest_range("mockchain", &source, &deps, MODULE_BACKFILL, 1, 5, &mut rv)
            .await
            .unwrap();
        assert_eq!(n, 5);
        assert_eq!(
            heads.get("mockchain"),
            None,
            "a backfill chunk has no head to observe"
        );

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn metadata_version_byte_never_guesses() {
        assert_eq!(metadata_version_byte(b"meta\x0e_rest"), Some(14));
        assert_eq!(metadata_version_byte(b"meta\x10_rest"), Some(16));
        assert_eq!(metadata_version_byte(b"nope\x0e"), None);
        assert_eq!(metadata_version_byte(b"meta"), None);
        assert_eq!(metadata_version_byte(b""), None);
    }
}
