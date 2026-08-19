//! Decode workers: walk archived raw blocks → canonical rows, independently of
//! ingestion (ARCHITECTURE.md §9: each module consumes from its own checkpoint).
//!
//! This worker is family-agnostic: it knows the raw-store key layout and its
//! own envelope field `spec_version` (we wrote it — reading it back is not
//! FRAME knowledge). HOW bytes become canonical rows lives behind
//! `RawBlockDecoder` in adapter crates (Invariant 4). Decoding is pure:
//! everything a decoder needs (envelope, events, metadata blob) is handed to
//! it as bytes — no I/O in the decode path, re-runnable over history.
//!
//! Checkpoint module: `blocks` (the canonical module) — the same high-water
//! mark the fixture pipeline uses. Raw workers (`raw_blocks`, `raw_backfill*`)
//! stay ahead of or equal to this one; the decode follower chases `raw_blocks`.

use crate::live::SinkError;
use crate::{Checkpoint, CheckpointError, CheckpointStore};
use async_trait::async_trait;
use canonical::CanonicalBlock;
use raw_store::{keys, RawStore, RawStoreError};

#[derive(Debug, thiserror::Error)]
pub enum DecodeWorkerError {
    #[error("raw store: {0}")]
    Raw(#[from] RawStoreError),
    #[error(
        "metadata blob missing for {chain}/{spec_version} (key {key}) — \
         raw ingestion archives it; run live/backfill over this range first"
    )]
    MetadataMissing {
        chain: String,
        spec_version: u32,
        key: String,
    },
    #[error("envelope for {chain}/{height} is unreadable: {reason}")]
    BadEnvelope {
        chain: String,
        height: u64,
        reason: String,
    },
    #[error("decode failed for {chain}/{height}: {reason}")]
    Decode {
        chain: String,
        height: u64,
        reason: String,
    },
    #[error(transparent)]
    Checkpoint(#[from] CheckpointError),
    #[error(transparent)]
    Sink(#[from] SinkError),
}

/// Pure decoder: (envelope bytes, events bytes, metadata blob) → canonical.
/// Implementations may cache per-spec_version machinery internally; the worker
/// treats them as stateless. NO I/O allowed inside.
pub trait RawBlockDecoder: Send + Sync {
    fn decode(
        &self,
        chain_id: &str,
        envelope: &[u8],
        events: Option<&[u8]>,
        metadata: &[u8],
        spec_version: u32,
        raw_location: &str,
    ) -> Result<CanonicalBlock, String>;

    /// Read `spec_version` out of a raw envelope WITHOUT decoding the block —
    /// the worker needs it to pick which metadata blob to decode against, and a
    /// block always decodes against the metadata of its own spec_version.
    ///
    /// This is on the adapter and not in the worker because an envelope's layout
    /// is protocol knowledge (Invariant 4). It used to be a `serde_json` parse
    /// inline in this file, which was invisible while there was exactly one
    /// envelope format and became a bug the moment there were two.
    fn spec_version_of(&self, envelope: &[u8]) -> Result<u32, String>;
}

/// Where canonical blocks land (the API's BlockIndex, adapted node-side —
/// ingest must not depend on the api crate).
#[async_trait]
pub trait CanonicalSink: Send + Sync {
    async fn insert(&self, block: CanonicalBlock) -> Result<(), SinkError>;
    /// Is (chain, height) already durably decoded? Gap-fill probes this behind
    /// the checkpoint frontier — the checkpoint tracks the frontier only, not
    /// holes behind it.
    async fn contains(&self, chain_id: &str, height: u64) -> Result<bool, SinkError>;
}

pub struct DecodeDeps<'a> {
    pub raw: &'a dyn RawStore,
    pub checkpoints: &'a dyn CheckpointStore,
    pub sink: &'a dyn CanonicalSink,
}

pub const MODULE_DECODE: &str = "blocks";

/// The envelope peek: our own written field, nothing family-specific.
/// Fetch a block's envelope, trying the item names newest-first, and return the
/// key it was found under so lineage records where the bytes actually came from.
///
/// A height may hold either generation: v1 for everything ingested before the
/// format slice (compaction never re-encodes, so those stay v1 forever), v2 for
/// everything after. Behind a `BucketedStore` the bytes may equally come out of
/// a compacted bucket — the caller cannot tell and does not need to.
fn read_block_envelope(
    raw: &dyn RawStore,
    chain_id: &str,
    height: u64,
) -> Result<(String, Vec<u8>), RawStoreError> {
    let mut last = None;
    for item in keys::BLOCK_ITEMS {
        let key = keys::block(chain_id, height, item);
        match raw.get(&key) {
            Ok(bytes) => return Ok((key, bytes)),
            Err(e @ RawStoreError::NotFound(_)) => last = Some(e),
            Err(e) => return Err(e),
        }
    }
    // report the NEWEST name in the not-found error: it is the one a fresh
    // ingestion would have written, so it is the one worth looking for.
    Err(last.unwrap_or_else(|| {
        RawStoreError::NotFound(keys::block(chain_id, height, keys::BLOCK_ITEM_V2))
    }))
}

fn block_envelope_exists(
    raw: &dyn RawStore,
    chain_id: &str,
    height: u64,
) -> Result<bool, RawStoreError> {
    for item in keys::BLOCK_ITEMS {
        if raw.exists(&keys::block(chain_id, height, item))? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn peek_spec_version(
    decoder: &dyn RawBlockDecoder,
    chain: &str,
    height: u64,
    envelope: &[u8],
) -> Result<u32, DecodeWorkerError> {
    decoder
        .spec_version_of(envelope)
        .map_err(|reason| DecodeWorkerError::BadEnvelope {
            chain: chain.to_string(),
            height,
            reason,
        })
}

/// Decode heights `from..=to`, including gaps behind the `blocks` checkpoint
/// (skipping heights the sink already holds). Rows first, checkpoint last
/// (crash = re-decode, never skip).
pub async fn decode_range(
    chain_id: &str,
    decoder: &dyn RawBlockDecoder,
    deps: &DecodeDeps<'_>,
    from: u64,
    to: u64,
) -> Result<u64, DecodeWorkerError> {
    // The checkpoint is a frontier, not a bitmap: heights at or below it may
    // still be holes (decode_tick jumps fixture-era gaps and leaves them to
    // this worker). Behind the frontier, probe the sink per height and never
    // touch the checkpoint (advance would refuse the regression anyway); past
    // it, advance as usual.
    let frontier = deps
        .checkpoints
        .get(chain_id, MODULE_DECODE)
        .await?
        .map(|cp| cp.last_height);
    let mut processed = 0u64;

    for height in from..=to {
        let behind_frontier = frontier.is_some_and(|f| height <= f);
        if behind_frontier && deps.sink.contains(chain_id, height).await? {
            continue;
        }
        let (envelope_key, envelope) = read_block_envelope(deps.raw, chain_id, height)?;
        let events = match deps.raw.get(&keys::block(chain_id, height, keys::EVENTS_ITEM)) {
            Ok(bytes) => Some(bytes),
            Err(RawStoreError::NotFound(_)) => {
                // legitimately absent — but success flags then default true
                tracing::debug!(chain = %chain_id, height, "no events.scale for block");
                None
            }
            Err(e) => return Err(e.into()),
        };
        let spec_version = peek_spec_version(decoder, chain_id, height, &envelope)?;
        let meta_key = keys::metadata(chain_id, spec_version);
        let metadata = deps.raw.get(&meta_key).map_err(|e| match e {
            RawStoreError::NotFound(_) => DecodeWorkerError::MetadataMissing {
                chain: chain_id.to_string(),
                spec_version,
                key: meta_key.clone(),
            },
            other => other.into(),
        })?;

        let block = decoder
            .decode(
                chain_id,
                &envelope,
                events.as_deref(),
                &metadata,
                spec_version,
                &envelope_key,
            )
            .map_err(|reason| DecodeWorkerError::Decode {
                chain: chain_id.to_string(),
                height,
                reason,
            })?;
        let block_hash = block.hash.clone();
        deps.sink.insert(block).await?;

        if !behind_frontier {
            deps.checkpoints
                .advance(Checkpoint {
                    chain_id: chain_id.to_string(),
                    module: MODULE_DECODE.to_string(),
                    last_height: height,
                    last_hash: block_hash,
                    updated_at: chrono::Utc::now(),
                })
                .await?;
        }
        processed += 1;
        tracing::info!(chain = %chain_id, height, spec_version, "decoded");
    }
    Ok(processed)
}

/// One decode-follower step: chase the raw_blocks checkpoint. First run starts
/// at the raw follower's current position (history is decode-range's job).
pub async fn decode_tick(
    chain_id: &str,
    decoder: &dyn RawBlockDecoder,
    deps: &DecodeDeps<'_>,
) -> Result<u64, DecodeWorkerError> {
    let Some(raw_cp) = deps.checkpoints.get(chain_id, crate::live::MODULE_LIVE).await? else {
        return Ok(0); // raw follower hasn't started yet
    };
    let target = raw_cp.last_height;
    let mut from = match deps.checkpoints.get(chain_id, MODULE_DECODE).await? {
        Some(cp) if cp.last_height >= target => return Ok(0),
        Some(cp) => cp.last_height + 1,
        None => target, // first run: start where raw currently is
    };
    // The `blocks` checkpoint may predate live raw coverage (e.g. the fixture
    // pipeline's high-water mark, far below the live tip). If the next height
    // has no raw block, jump to the raw tip instead of erroring forever —
    // covering the gap is `decode-range`'s job, exactly like the None branch.
    if from < target && !block_envelope_exists(deps.raw, chain_id, from)? {
        tracing::warn!(
            chain = %chain_id, from, target,
            "no raw block at decode resume point — jumping to raw tip (backfill gap via decode-range)"
        );
        from = target;
    }
    decode_range(chain_id, decoder, deps, from, target).await
}

/// Follow forever: decode whatever the raw follower has landed. Same backoff
/// discipline as `live::follow`; caller owns cancellation.
pub async fn decode_follow(
    chain_id: &str,
    decoder: &dyn RawBlockDecoder,
    deps: &DecodeDeps<'_>,
    poll: std::time::Duration,
) {
    let mut consecutive_failures = 0u32;
    loop {
        match decode_tick(chain_id, decoder, deps).await {
            Ok(n) => {
                consecutive_failures = 0;
                if n > 0 {
                    tracing::debug!(chain = %chain_id, blocks = n, "decode tick");
                }
            }
            Err(e) => {
                consecutive_failures += 1;
                tracing::warn!(chain = %chain_id, error = %e, consecutive_failures, "decode tick failed");
            }
        }
        let factor = 1 + consecutive_failures.min(10);
        tokio::time::sleep(poll * factor).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::MODULE_LIVE;
    use crate::MemoryCheckpointStore;
    use canonical::Lineage;
    use std::sync::Mutex;

    /// Mock decoder: trusts the envelope JSON, records metadata it was given.
    struct MockDecoder {
        seen_metadata: Mutex<Vec<(u64, Vec<u8>)>>,
    }

    impl RawBlockDecoder for MockDecoder {
        fn decode(
            &self,
            chain_id: &str,
            envelope: &[u8],
            events: Option<&[u8]>,
            metadata: &[u8],
            spec_version: u32,
            raw_location: &str,
        ) -> Result<CanonicalBlock, String> {
            let v: serde_json::Value = serde_json::from_slice(envelope).map_err(|e| e.to_string())?;
            let height = v["height"].as_u64().ok_or("no height")?;
            self.seen_metadata.lock().unwrap().push((height, metadata.to_vec()));
            if events.is_none() {
                return Err(format!("mock requires events for {height}"));
            }
            Ok(CanonicalBlock {
                chain_id: chain_id.to_string(),
                height,
                hash: format!("0x{height:064x}"),
                parent_hash: format!("0x{:064x}", height - 1),
                timestamp: None,
                finalized: true,
                lineage: Lineage {
                    runtime_version: spec_version,
                    decoder_version: 2,
                    raw_location: raw_location.to_string(),
                },
                transactions: vec![],
                events: vec![],
            })
        }

        fn spec_version_of(&self, envelope: &[u8]) -> Result<u32, String> {
            let v: serde_json::Value =
                serde_json::from_slice(envelope).map_err(|e| e.to_string())?;
            v["spec_version"].as_u64().map(|s| s as u32).ok_or("no spec_version".into())
        }

    }

    #[derive(Default)]
    struct MemSink(Mutex<Vec<CanonicalBlock>>);
    #[async_trait]
    impl CanonicalSink for MemSink {
        async fn insert(&self, block: CanonicalBlock) -> Result<(), SinkError> {
            self.0.lock().unwrap().push(block);
            Ok(())
        }
        async fn contains(&self, chain_id: &str, height: u64) -> Result<bool, SinkError> {
            Ok(self
                .0
                .lock()
                .unwrap()
                .iter()
                .any(|b| b.chain_id == chain_id && b.height == height))
        }
    }

    fn tmp_raw(tag: &str) -> (raw_store::FsRawStore, std::path::PathBuf) {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("dotlens-dec-{}-{tag}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        (raw_store::FsRawStore::new(&dir), dir)
    }

    fn seed_raw(raw: &raw_store::FsRawStore, chain: &str, h: u64, spec: u32) {
        let env = serde_json::json!({"chain_id": chain, "height": h, "spec_version": spec});
        raw.put(&keys::block(chain, h, "block.json"), &serde_json::to_vec(&env).unwrap(), "t")
            .unwrap();
        raw.put(&keys::block(chain, h, "events.scale"), &[h as u8], "t").unwrap();
        // metadata blob per spec (idempotent re-put for same spec)
        let _ = raw.put(&keys::metadata(chain, spec), format!("meta-{spec}").as_bytes(), "t");
    }

    #[tokio::test]
    async fn decode_range_walks_raw_and_uses_spec_correct_metadata() {
        let (raw, dir) = tmp_raw("range");
        for h in 1..=8 {
            seed_raw(&raw, "mock", h, if h <= 4 { 100 } else { 101 });
        }
        let checkpoints = MemoryCheckpointStore::new();
        let sink = MemSink::default();
        let decoder = MockDecoder { seen_metadata: Mutex::new(vec![]) };
        let deps = DecodeDeps { raw: &raw, checkpoints: &checkpoints, sink: &sink };

        let n = decode_range("mock", &decoder, &deps, 1, 8).await.unwrap();
        assert_eq!(n, 8);
        assert_eq!(sink.0.lock().unwrap().len(), 8);
        // spec-correct metadata reached the decoder at the era boundary
        let seen = decoder.seen_metadata.lock().unwrap();
        assert_eq!(seen[3], (4, b"meta-100".to_vec()));
        assert_eq!(seen[4], (5, b"meta-101".to_vec()));
        drop(seen);

        // re-run: no-op
        assert_eq!(decode_range("mock", &decoder, &deps, 1, 8).await.unwrap(), 0);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn decode_range_fills_gaps_behind_the_checkpoint_frontier() {
        // decode_tick jumped a fixture-era gap: checkpoint sits at 10, but
        // only block 10 was ever decoded. decode_range must fill 1..=5 without
        // moving the checkpoint, and a re-run must be a no-op.
        let (raw, dir) = tmp_raw("gapfill");
        for h in 1..=5 {
            seed_raw(&raw, "mock", h, 100);
        }
        seed_raw(&raw, "mock", 10, 100);
        let checkpoints = MemoryCheckpointStore::new();
        let sink = MemSink::default();
        let decoder = MockDecoder { seen_metadata: Mutex::new(vec![]) };
        let deps = DecodeDeps { raw: &raw, checkpoints: &checkpoints, sink: &sink };
        decode_range("mock", &decoder, &deps, 10, 10).await.unwrap();
        assert_eq!(checkpoints.get("mock", MODULE_DECODE).await.unwrap().unwrap().last_height, 10);

        let n = decode_range("mock", &decoder, &deps, 1, 5).await.unwrap();
        assert_eq!(n, 5);
        assert_eq!(sink.0.lock().unwrap().len(), 6);
        let cp = checkpoints.get("mock", MODULE_DECODE).await.unwrap().unwrap();
        assert_eq!(cp.last_height, 10, "gap-fill must not move the frontier");

        assert_eq!(decode_range("mock", &decoder, &deps, 1, 5).await.unwrap(), 0);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn missing_metadata_is_a_loud_error_not_a_guess() {
        let (raw, dir) = tmp_raw("nometa");
        let env = serde_json::json!({"chain_id": "mock", "height": 1, "spec_version": 999});
        raw.put(&keys::block("mock", 1, "block.json"), &serde_json::to_vec(&env).unwrap(), "t")
            .unwrap();
        raw.put(&keys::block("mock", 1, "events.scale"), &[1], "t").unwrap();

        let checkpoints = MemoryCheckpointStore::new();
        let sink = MemSink::default();
        let decoder = MockDecoder { seen_metadata: Mutex::new(vec![]) };
        let deps = DecodeDeps { raw: &raw, checkpoints: &checkpoints, sink: &sink };

        let err = decode_range("mock", &decoder, &deps, 1, 1).await.unwrap_err();
        assert!(matches!(err, DecodeWorkerError::MetadataMissing { spec_version: 999, .. }));
        // nothing advanced, nothing inserted
        assert!(checkpoints.get("mock", MODULE_DECODE).await.unwrap().is_none());
        assert!(sink.0.lock().unwrap().is_empty());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn decode_tick_chases_the_raw_checkpoint() {
        let (raw, dir) = tmp_raw("tick");
        let checkpoints = MemoryCheckpointStore::new();
        let sink = MemSink::default();
        let decoder = MockDecoder { seen_metadata: Mutex::new(vec![]) };
        let deps = DecodeDeps { raw: &raw, checkpoints: &checkpoints, sink: &sink };

        // raw follower hasn't started → decode does nothing
        assert_eq!(decode_tick("mock", &decoder, &deps).await.unwrap(), 0);

        // raw lands 5..=7 with checkpoint at 7 → first tick decodes at raw tip
        for h in 5..=7 {
            seed_raw(&raw, "mock", h, 100);
        }
        checkpoints
            .advance(Checkpoint {
                chain_id: "mock".into(),
                module: MODULE_LIVE.into(),
                last_height: 7,
                last_hash: "0x7".into(),
                updated_at: chrono::Utc::now(),
            })
            .await
            .unwrap();
        assert_eq!(decode_tick("mock", &decoder, &deps).await.unwrap(), 1); // height 7

        // raw advances to 9 → decode follows exactly the delta
        for h in 8..=9 {
            seed_raw(&raw, "mock", h, 100);
        }
        checkpoints
            .advance(Checkpoint {
                chain_id: "mock".into(),
                module: MODULE_LIVE.into(),
                last_height: 9,
                last_hash: "0x9".into(),
                updated_at: chrono::Utc::now(),
            })
            .await
            .unwrap();
        assert_eq!(decode_tick("mock", &decoder, &deps).await.unwrap(), 2);
        let cp = checkpoints.get("mock", MODULE_DECODE).await.unwrap().unwrap();
        assert_eq!(cp.last_height, 9);
        let _ = std::fs::remove_dir_all(dir);
    }
}
