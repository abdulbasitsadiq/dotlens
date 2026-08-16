//! Tip worker: follow the UNFINALIZED head between the finalized frontier and
//! the best block, with reorg handling (ARCHITECTURE.md §15: track finalized
//! separately; unfinalized rows are flagged and replaced/pruned on reorg).
//!
//! Design:
//! - The finalized pipeline (live.rs → decode.rs) stays the source of truth;
//!   this worker only fills the small `finalized+1..=best` window for explorer
//!   freshness. Balances/decode checkpoints chase FINALIZED data only.
//! - Raw artifacts for unfinalized blocks are HASH-KEYED
//!   (`raw/{chain}/unfinalized/{height}/{hash}/…`) so write-once immutability
//!   holds across forks — superseded fork blocks stay archived forever.
//! - Reorg detection is per-tick polling: for each height in the window,
//!   compare the node's canonical hash with what we stored; different or
//!   absent → refetch + replace. The canonical sink's REPLACEMENT RULE does
//!   the swap: finalized rows are immutable, unfinalized rows are always
//!   replaceable (api::BlockIndex insert semantics).
//! - A shortening reorg leaves stale rows above the new best → pruned.
//! - No checkpoint: the window is tiny (best − finalized ≈ 2–3 on Polkadot)
//!   and every tick re-verifies it from scratch. Stateless = restart-safe.

use crate::decode::{CanonicalSink, RawBlockDecoder};
use crate::live::{ChainSource, SinkError, SourceError};
use async_trait::async_trait;
use raw_store::{keys, RawStore, RawStoreError};

#[derive(Debug, thiserror::Error)]
pub enum TipError {
    #[error("source: {0}")]
    Source(#[from] SourceError),
    #[error("raw store: {0}")]
    Raw(#[from] RawStoreError),
    #[error("sink: {0}")]
    Sink(#[from] SinkError),
    #[error("decode failed for {chain}/{height} (unfinalized): {reason}")]
    Decode {
        chain: String,
        height: u64,
        reason: String,
    },
    #[error(
        "metadata blob missing for {chain}/{spec_version} — tip decode needs it; \
         the finalized follower archives it (or it will on the next boundary)"
    )]
    MetadataMissing { chain: String, spec_version: u32 },
}

/// A ChainSource that can also see past finality: the best height, and
/// block fetches marked unfinalized in the envelope.
#[async_trait]
pub trait TipSource: ChainSource {
    async fn best_height(&self) -> Result<u64, SourceError>;
    /// The node's CURRENT canonical-chain hash at `height` — the cheap reorg
    /// probe (1 RPC) so an unchanged window costs no block fetches.
    async fn canonical_hash_at(&self, height: u64) -> Result<String, SourceError>;
    /// Like `fetch_block` but the stored envelope says `finalized: false`
    /// (the decoder propagates the flag into the canonical row).
    async fn fetch_unfinalized(
        &self,
        height: u64,
    ) -> Result<crate::live::FetchedBlock, SourceError>;
}

/// Node-side view of unfinalized canonical rows (direct SQL; the api crate's
/// BlockIndex handles replacement — this adds the probe + prune).
#[async_trait]
pub trait UnfinalizedStore: Send + Sync {
    /// Hash of the UNFINALIZED row at (chain, height), None if there is no
    /// row or the row is finalized.
    async fn unfinalized_hash(
        &self,
        chain_id: &str,
        height: u64,
    ) -> Result<Option<String>, SinkError>;
    /// Delete unfinalized rows ABOVE `height` (shortening reorg leftovers).
    /// Returns how many block rows were pruned.
    async fn prune_unfinalized_above(
        &self,
        chain_id: &str,
        height: u64,
    ) -> Result<u64, SinkError>;
}

pub struct TipDeps<'a> {
    pub raw: &'a dyn RawStore,
    pub receipts: &'a dyn crate::ReceiptSink,
    pub store: &'a dyn UnfinalizedStore,
    pub sink: &'a dyn CanonicalSink,
}

/// Short hash for raw keys: first 12 hex chars (0x stripped) — 48 bits is
/// plenty to disambiguate forks at one height, and keeps paths readable.
fn short_hash(hash: &str) -> String {
    let h = hash.trim_start_matches("0x");
    h[..h.len().min(12)].to_string()
}

/// One tick: verify/refresh the finalized+1..=best window. Returns how many
/// blocks were (re)ingested.
pub async fn tip_tick(
    chain_id: &str,
    source: &dyn TipSource,
    decoder: &dyn RawBlockDecoder,
    deps: &TipDeps<'_>,
) -> Result<u64, TipError> {
    let finalized = source.finalized_height().await?;
    let best = source.best_height().await?;

    // finalization passed (or a shortening reorg): drop stale unfinalized rows
    let pruned = deps
        .store
        .prune_unfinalized_above(chain_id, best.max(finalized))
        .await?;
    if pruned > 0 {
        tracing::info!(chain = %chain_id, pruned, best, "stale unfinalized rows pruned");
    }
    if best <= finalized {
        return Ok(0);
    }

    let mut ingested = 0u64;
    for height in (finalized + 1)..=best {
        // cheap reorg probe first (1 RPC): only fetch the full block when the
        // node's canonical hash differs from what we hold. NOTE: with endpoint
        // failover, probe and fetch can land on different nodes that disagree
        // about the best chain — harmless, the next tick converges.
        let canonical = source.canonical_hash_at(height).await?;
        match deps.store.unfinalized_hash(chain_id, height).await? {
            Some(existing) if existing == canonical => continue, // unchanged
            Some(existing) => {
                tracing::info!(
                    chain = %chain_id, height, old = %existing, new = %canonical,
                    "reorg at unfinalized height — replacing"
                );
            }
            None => {}
        }
        let fetched = source.fetch_unfinalized(height).await?;

        // raw first, hash-keyed (write-once safe across forks; superseded
        // blocks remain archived), receipts recorded like every other pipeline
        let sh = short_hash(&fetched.hash);
        let mut envelope: Option<Vec<u8>> = None;
        let mut events: Option<Vec<u8>> = None;
        let mut envelope_key = String::new();
        for artifact in &fetched.artifacts {
            let key = keys::unfinalized_block(chain_id, height, &sh, &artifact.item);
            let receipt = deps.raw.put(&key, &artifact.bytes, "tip")?;
            deps.receipts
                .record(&receipt)
                .await
                .map_err(|e| SinkError(e.to_string()))?;
            match artifact.item.as_str() {
                "block.json" => {
                    envelope = Some(artifact.bytes.clone());
                    envelope_key = key;
                }
                "events.scale" => events = Some(artifact.bytes.clone()),
                _ => {}
            }
        }
        let Some(envelope) = envelope else {
            return Err(TipError::Decode {
                chain: chain_id.to_string(),
                height,
                reason: "source returned no block.json artifact".into(),
            });
        };

        // decode against the archived metadata for this spec (tip blocks run
        // the current runtime, archived by the finalized follower)
        let spec_version = fetched.runtime_version;
        let metadata = deps
            .raw
            .get(&keys::metadata(chain_id, spec_version))
            .map_err(|e| match e {
                RawStoreError::NotFound(_) => TipError::MetadataMissing {
                    chain: chain_id.to_string(),
                    spec_version,
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
            .map_err(|reason| TipError::Decode {
                chain: chain_id.to_string(),
                height,
                reason,
            })?;
        debug_assert!(!block.finalized, "tip envelopes must carry finalized:false");
        deps.sink.insert(block).await?; // replacement rule swaps forks
        ingested += 1;
    }
    Ok(ingested)
}

/// Follow forever; same backoff discipline as the other followers.
pub async fn tip_follow(
    chain_id: &str,
    source: &dyn TipSource,
    decoder: &dyn RawBlockDecoder,
    deps: &TipDeps<'_>,
    poll: std::time::Duration,
) {
    let mut consecutive_failures = 0u32;
    loop {
        match tip_tick(chain_id, source, decoder, deps).await {
            Ok(n) => {
                consecutive_failures = 0;
                if n > 0 {
                    tracing::debug!(chain = %chain_id, blocks = n, "tip tick");
                }
            }
            Err(e) => {
                consecutive_failures += 1;
                tracing::warn!(chain = %chain_id, error = %e, consecutive_failures, "tip tick failed");
            }
        }
        let factor = 1 + consecutive_failures.min(10);
        tokio::time::sleep(poll * factor).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::{FetchedBlock, RawArtifact};
    use canonical::{CanonicalBlock, Lineage};
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// Mock chain: a controllable (height → hash) view with finalized/best.
    struct MockTip {
        finalized: Mutex<u64>,
        best: Mutex<u64>,
        hashes: Mutex<HashMap<u64, String>>,
    }

    impl MockTip {
        fn new(finalized: u64, best: u64) -> Self {
            let hashes = (1..=best)
                .map(|h| (h, format!("0xaa{h:02x}")))
                .collect();
            Self {
                finalized: Mutex::new(finalized),
                best: Mutex::new(best),
                hashes: Mutex::new(hashes),
            }
        }
        fn reorg(&self, height: u64, new_best: u64) {
            let mut hashes = self.hashes.lock().unwrap();
            for h in height..=new_best {
                hashes.insert(h, format!("0xbb{h:02x}"));
            }
            *self.best.lock().unwrap() = new_best;
        }
    }

    #[async_trait]
    impl ChainSource for MockTip {
        async fn finalized_height(&self) -> Result<u64, SourceError> {
            Ok(*self.finalized.lock().unwrap())
        }
        async fn fetch_block(&self, height: u64) -> Result<FetchedBlock, SourceError> {
            self.fetch_unfinalized(height).await
        }
        async fn metadata_at(&self, _height: u64) -> Result<Vec<u8>, SourceError> {
            Ok(b"meta-100".to_vec())
        }
    }

    #[async_trait]
    impl TipSource for MockTip {
        async fn best_height(&self) -> Result<u64, SourceError> {
            Ok(*self.best.lock().unwrap())
        }
        async fn canonical_hash_at(&self, height: u64) -> Result<String, SourceError> {
            self.hashes
                .lock()
                .unwrap()
                .get(&height)
                .cloned()
                .ok_or(SourceError::NotFound(height))
        }
        async fn fetch_unfinalized(&self, height: u64) -> Result<FetchedBlock, SourceError> {
            let hash = self
                .hashes
                .lock()
                .unwrap()
                .get(&height)
                .cloned()
                .ok_or(SourceError::NotFound(height))?;
            let env = serde_json::json!({
                "chain_id": "mock", "height": height, "hash": hash,
                "parent_hash": "0x00", "spec_version": 100, "finalized": false,
                "extrinsics": [],
            });
            Ok(FetchedBlock {
                height,
                hash: hash.clone(),
                parent_hash: "0x00".into(),
                runtime_version: 100,
                transaction_version: None,
                artifacts: vec![
                    RawArtifact {
                        item: "block.json".into(),
                        bytes: serde_json::to_vec(&env).unwrap(),
                    },
                    RawArtifact { item: "events.scale".into(), bytes: vec![0] },
                ],
            })
        }
    }

    /// Mock decoder: trusts the envelope (incl. its finalized flag).
    struct EnvelopeDecoder;
    impl RawBlockDecoder for EnvelopeDecoder {
        fn decode(
            &self,
            chain_id: &str,
            envelope: &[u8],
            _events: Option<&[u8]>,
            _metadata: &[u8],
            spec_version: u32,
            raw_location: &str,
        ) -> Result<CanonicalBlock, String> {
            let v: serde_json::Value =
                serde_json::from_slice(envelope).map_err(|e| e.to_string())?;
            Ok(CanonicalBlock {
                chain_id: chain_id.to_string(),
                height: v["height"].as_u64().ok_or("height")?,
                hash: v["hash"].as_str().ok_or("hash")?.to_string(),
                parent_hash: v["parent_hash"].as_str().unwrap_or_default().to_string(),
                timestamp: None,
                finalized: v["finalized"].as_bool().unwrap_or(true),
                lineage: Lineage {
                    runtime_version: spec_version,
                    decoder_version: 2,
                    raw_location: raw_location.to_string(),
                },
                transactions: vec![],
                events: vec![],
            })
        }
    }

    /// Mock canonical store with the REPLACEMENT RULE (finalized immutable,
    /// unfinalized replaceable) + the UnfinalizedStore probe/prune.
    #[derive(Default)]
    struct MemStore(Mutex<HashMap<u64, CanonicalBlock>>);

    #[async_trait]
    impl CanonicalSink for MemStore {
        async fn insert(&self, block: CanonicalBlock) -> Result<(), SinkError> {
            let mut map = self.0.lock().unwrap();
            if let Some(existing) = map.get(&block.height) {
                if existing.finalized {
                    return Ok(()); // immutable
                }
            }
            map.insert(block.height, block);
            Ok(())
        }
        async fn contains(&self, _chain: &str, height: u64) -> Result<bool, SinkError> {
            Ok(self.0.lock().unwrap().contains_key(&height))
        }
    }

    #[async_trait]
    impl UnfinalizedStore for MemStore {
        async fn unfinalized_hash(
            &self,
            _chain: &str,
            height: u64,
        ) -> Result<Option<String>, SinkError> {
            Ok(self
                .0
                .lock()
                .unwrap()
                .get(&height)
                .filter(|b| !b.finalized)
                .map(|b| b.hash.clone()))
        }
        async fn prune_unfinalized_above(
            &self,
            _chain: &str,
            height: u64,
        ) -> Result<u64, SinkError> {
            let mut map = self.0.lock().unwrap();
            let stale: Vec<u64> = map
                .iter()
                .filter(|(h, b)| **h > height && !b.finalized)
                .map(|(h, _)| *h)
                .collect();
            let n = stale.len() as u64;
            for h in stale {
                map.remove(&h);
            }
            Ok(n)
        }
    }

    fn tmp_raw(tag: &str) -> (raw_store::FsRawStore, std::path::PathBuf) {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("dotlens-tip-{}-{tag}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let raw = raw_store::FsRawStore::new(&dir);
        // tip decode needs archived metadata for the current spec
        raw.put(&keys::metadata("mock", 100), b"meta-100", "t").unwrap();
        (raw, dir)
    }

    #[tokio::test]
    async fn tip_fills_the_unfinalized_window_and_is_stable() {
        let source = MockTip::new(5, 8);
        let (raw, dir) = tmp_raw("window");
        let store = MemStore::default();
        let deps = TipDeps { raw: &raw, receipts: &crate::NoopReceiptSink, store: &store, sink: &store };

        let n = tip_tick("mock", &source, &EnvelopeDecoder, &deps).await.unwrap();
        assert_eq!(n, 3, "heights 6..=8");
        {
            let map = store.0.lock().unwrap();
            assert!(map.values().all(|b| !b.finalized));
            assert_eq!(map.get(&7).unwrap().hash, "0xaa07");
        }
        // second tick: nothing changed on-chain → no-op
        let n = tip_tick("mock", &source, &EnvelopeDecoder, &deps).await.unwrap();
        assert_eq!(n, 0);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn reorg_replaces_unfinalized_rows_and_raw_keeps_both_forks() {
        let source = MockTip::new(5, 8);
        let (raw, dir) = tmp_raw("reorg");
        let store = MemStore::default();
        let deps = TipDeps { raw: &raw, receipts: &crate::NoopReceiptSink, store: &store, sink: &store };
        tip_tick("mock", &source, &EnvelopeDecoder, &deps).await.unwrap();

        // reorg at height 7: new fork 7..=9
        source.reorg(7, 9);
        let n = tip_tick("mock", &source, &EnvelopeDecoder, &deps).await.unwrap();
        assert_eq!(n, 3, "7 and 8 replaced, 9 new");
        {
            let map = store.0.lock().unwrap();
            assert_eq!(map.get(&6).unwrap().hash, "0xaa06", "pre-fork untouched");
            assert_eq!(map.get(&7).unwrap().hash, "0xbb07");
            assert_eq!(map.get(&9).unwrap().hash, "0xbb09");
        }
        // BOTH forks' raw artifacts exist (immutability across reorgs)
        assert!(raw.exists(&keys::unfinalized_block("mock", 7, "aa07", "block.json")).unwrap());
        assert!(raw.exists(&keys::unfinalized_block("mock", 7, "bb07", "block.json")).unwrap());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn shortening_reorg_prunes_stale_heights_and_finalized_rows_survive() {
        let source = MockTip::new(5, 9);
        let (raw, dir) = tmp_raw("shorten");
        let store = MemStore::default();
        // height 6 is already FINALIZED in the store (the finalized pipeline won)
        store
            .insert(CanonicalBlock {
                chain_id: "mock".into(),
                height: 6,
                hash: "0xfinal6".into(),
                parent_hash: "0x00".into(),
                timestamp: None,
                finalized: true,
                lineage: Lineage {
                    runtime_version: 100,
                    decoder_version: 2,
                    raw_location: "raw/x".into(),
                },
                transactions: vec![],
                events: vec![],
            })
            .await
            .unwrap();
        let deps = TipDeps { raw: &raw, receipts: &crate::NoopReceiptSink, store: &store, sink: &store };
        tip_tick("mock", &source, &EnvelopeDecoder, &deps).await.unwrap();
        assert!(store.0.lock().unwrap().contains_key(&9));
        // finalized row was NOT overwritten by the tip fetch at height 6
        assert_eq!(store.0.lock().unwrap().get(&6).unwrap().hash, "0xfinal6");

        // chain shrinks: best falls back to 7
        *source.best.lock().unwrap() = 7;
        source.hashes.lock().unwrap().remove(&8);
        source.hashes.lock().unwrap().remove(&9);
        tip_tick("mock", &source, &EnvelopeDecoder, &deps).await.unwrap();
        let map = store.0.lock().unwrap();
        assert!(!map.contains_key(&8) && !map.contains_key(&9), "stale rows pruned");
        assert!(map.get(&6).unwrap().finalized, "finalized rows never pruned");
        let _ = std::fs::remove_dir_all(dir);
    }
}
