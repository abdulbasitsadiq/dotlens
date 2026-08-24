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
use raw_store::{keys, RawStore};
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
        let deps = IngestDeps {
            raw: &raw,
            checkpoints: &checkpoints,
            receipts: &receipts,
            runtime_versions: &versions,
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
        let deps = IngestDeps {
            raw: &raw,
            checkpoints: &checkpoints,
            receipts: &receipts,
            runtime_versions: &versions,
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
        let deps = IngestDeps {
            raw: &raw,
            checkpoints: &checkpoints,
            receipts: &receipts,
            runtime_versions: &versions,
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

    #[test]
    fn metadata_version_byte_never_guesses() {
        assert_eq!(metadata_version_byte(b"meta\x0e_rest"), Some(14));
        assert_eq!(metadata_version_byte(b"meta\x10_rest"), Some(16));
        assert_eq!(metadata_version_byte(b"nope\x0e"), None);
        assert_eq!(metadata_version_byte(b"meta"), None);
        assert_eq!(metadata_version_byte(b""), None);
    }
}
