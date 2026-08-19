//! Raw store: immutable, write-once storage for original bytes (Invariant 1).
//!
//! Phase 0 ships the filesystem backend; an S3/MinIO backend implements the same
//! trait later without touching callers. Keys follow ARCHITECTURE.md §6:
//!   raw/{chain}/{bucket}/{height}/{item}     (bucket = height / 10_000)
//!   raw/{chain}/meta/{runtime_version}/metadata.scale

pub mod bucket;

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Debug, thiserror::Error)]
pub enum RawStoreError {
    #[error("io error at {path}: {source}")]
    Io {
        path: String,
        source: std::io::Error,
    },
    #[error("refusing to overwrite immutable object: {0}")]
    WouldOverwrite(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("invalid key (must be relative, no '..'): {0}")]
    InvalidKey(String),
    #[error("bucket: {0}")]
    Bucket(String),
}

/// A receipt recording provenance for every stored object (who/where/when/what).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestReceipt {
    pub key: String,
    pub byte_len: u64,
    pub source: String, // e.g. endpoint URL or "fixture"
    /// blake2b-256 of the stored bytes, 0x-hex — ARCHITECTURE.md §6 provenance.
    pub content_hash: String,
    pub fetched_at: chrono::DateTime<chrono::Utc>,
}

/// blake2b-256 of `bytes`, 0x-prefixed hex. The one hash used for receipts.
pub fn content_hash(bytes: &[u8]) -> String {
    use blake2::digest::{consts::U32, Digest};
    let mut hasher = blake2::Blake2b::<U32>::new();
    hasher.update(bytes);
    format!("0x{}", hex::encode(hasher.finalize()))
}

pub trait RawStore: Send + Sync {
    /// Write-once. Returns error if the key already exists with different bytes;
    /// identical re-puts are idempotent no-ops (safe re-ingestion).
    fn put(&self, key: &str, bytes: &[u8], source: &str) -> Result<IngestReceipt, RawStoreError>;
    fn get(&self, key: &str) -> Result<Vec<u8>, RawStoreError>;
    fn exists(&self, key: &str) -> Result<bool, RawStoreError>;
}

/// Canonical key layout helpers — the only place key strings are built.
pub mod keys {
    /// Block-envelope item names. TWO of them, and both are permanent: the store
    /// is write-once, so re-ingesting a v1-era height after the format change
    /// must not present different bytes at an existing key. Kept beside, never
    /// instead of — the same rule as `metadata-v15.scale` next to
    /// `metadata.scale`. The FORMAT itself is sniffed from the bytes; the item
    /// name exists so the two generations can coexist under one height.
    pub const BLOCK_ITEM_V2: &str = "block.bin";
    pub const BLOCK_ITEM_V1: &str = "block.json";
    /// Newest first — the order a reader must try them in.
    pub const BLOCK_ITEMS: [&str; 2] = [BLOCK_ITEM_V2, BLOCK_ITEM_V1];
    pub const EVENTS_ITEM: &str = "events.scale";

    pub fn block(chain: &str, height: u64, item: &str) -> String {
        format!("raw/{chain}/{:07}/{height}/{item}", height / 10_000)
    }
    /// A compacted bucket of consecutive block artifacts. The boundaries are
    /// ALIGNED (`from = height / n * n`) and therefore computable from a height
    /// alone — a reader must never need a listing to find the object holding a
    /// block. Nests inside the existing `height / 10_000` directory exactly as
    /// the per-object keys do, so the key scheme needed no redesign.
    pub fn bucket(chain: &str, from: u64, to: u64) -> String {
        format!("raw/{chain}/{:07}/blocks-{from}-{to}.dlb", from / 10_000)
    }

    /// Aligned bucket boundaries containing `height`.
    pub fn bucket_bounds(height: u64, blocks: u64) -> (u64, u64) {
        let from = height / blocks * blocks;
        (from, from + blocks - 1)
    }

    /// Inverse of [`block`] — the only place a block key is taken apart, so the
    /// layout is owned in one module in both directions.
    pub fn parse_block(key: &str) -> Option<(String, u64, String)> {
        let rest = key.strip_prefix("raw/")?;
        let mut it = rest.splitn(4, '/');
        let chain = it.next()?;
        let dir = it.next()?;
        let height: u64 = it.next()?.parse().ok()?;
        let item = it.next()?;
        // reject the sibling namespaces that are not height-keyed block artifacts
        if !dir.chars().all(|c| c.is_ascii_digit()) || item.contains('/') {
            return None;
        }
        Some((chain.to_string(), height, item.to_string()))
    }

    pub fn metadata(chain: &str, runtime_version: u32) -> String {
        format!("raw/{chain}/meta/{runtime_version}/metadata.scale")
    }
    /// Preimage bytes as read from state (the RAW storage value, compact
    /// length prefix included). `hash_hex` without 0x; keyed by (hash, len)
    /// exactly like pallet-preimage's own storage.
    pub fn preimage(chain: &str, hash_hex: &str, len: u64) -> String {
        format!("raw/{chain}/preimage/{hash_hex}/{len}.scale")
    }
    /// Metadata at an explicit metadata VERSION, fetched through the
    /// `Metadata_metadata_at_version` runtime API rather than `state_getMetadata`
    /// (which serves v14 on every runtime we index). Kept beside, not instead of,
    /// `metadata()`: the v14 blob decodes blocks and the v15 blob is the only one
    /// carrying the runtime-API section, so both are real artifacts of the same
    /// spec_version and neither may overwrite the other.
    pub fn metadata_at_version(chain: &str, runtime_version: u32, meta_version: u32) -> String {
        format!("raw/{chain}/meta/{runtime_version}/metadata-v{meta_version}.scale")
    }
    /// One simulation artifact, keyed by the STATE it ran against (block hash)
    /// and the exact input (hash of the encoded params) — the same two things
    /// that key `sim.simulation_results`.
    ///
    /// Height is deliberately absent from the key: two forks at one height are
    /// two states and would collide under a height key, which write-once storage
    /// would then report as an overwrite of an object that was never wrong.
    /// `item` names the runtime-API method as well as the direction —
    /// `DryRunApi_dry_run_call.params.scale` (what we sent) and
    /// `.response.scale` (what came back) — because Tier 2 and `dry_run_xcm`
    /// will file artifacts in this same directory, and bytes whose meaning
    /// depends on knowing which call produced them are not evidence.
    pub fn simulation(chain: &str, block_hash_hex: &str, input_hash_hex: &str, item: &str) -> String {
        format!("raw/{chain}/sim/{block_hash_hex}/{input_hash_hex}/{item}")
    }
    /// Unfinalized blocks are HASH-KEYED: forks at one height coexist under
    /// write-once immutability, and superseded blocks stay archived.
    /// `short_hash` = hex hash without 0x, truncated by the caller.
    pub fn unfinalized_block(chain: &str, height: u64, short_hash: &str, item: &str) -> String {
        format!("raw/{chain}/unfinalized/{height}/{short_hash}/{item}")
    }
}

pub struct FsRawStore {
    root: PathBuf,
}

impl FsRawStore {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    /// Keys are store-internal identifiers, not paths: reject absolute keys and
    /// any traversal so a key can never escape the root.
    fn path_for(&self, key: &str) -> Result<PathBuf, RawStoreError> {
        let bad = key.is_empty()
            || key.starts_with('/')
            || key.split('/').any(|seg| seg.is_empty() || seg == "." || seg == "..");
        if bad {
            return Err(RawStoreError::InvalidKey(key.to_string()));
        }
        Ok(self.root.join(key))
    }
}

impl RawStore for FsRawStore {
    fn put(&self, key: &str, bytes: &[u8], source: &str) -> Result<IngestReceipt, RawStoreError> {
        let path = self.path_for(key)?;
        if path.exists() {
            let existing = std::fs::read(&path).map_err(|source| RawStoreError::Io {
                path: path.display().to_string(),
                source,
            })?;
            if existing == bytes {
                // idempotent re-put
                return Ok(IngestReceipt {
                    key: key.to_string(),
                    byte_len: bytes.len() as u64,
                    source: source.to_string(),
                    content_hash: content_hash(bytes),
                    fetched_at: chrono::Utc::now(),
                });
            }
            return Err(RawStoreError::WouldOverwrite(key.to_string()));
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| RawStoreError::Io {
                path: parent.display().to_string(),
                source,
            })?;
        }
        // write via unique temp file + rename for atomicity (unique suffix so
        // sibling keys sharing a stem never collide on the temp name)
        static TMP_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let seq = TMP_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let tmp = path.with_extension(format!("tmp.{}.{}", std::process::id(), seq));
        std::fs::write(&tmp, bytes).map_err(|source| RawStoreError::Io {
            path: tmp.display().to_string(),
            source,
        })?;
        std::fs::rename(&tmp, &path).map_err(|source| RawStoreError::Io {
            path: path.display().to_string(),
            source,
        })?;
        Ok(IngestReceipt {
            key: key.to_string(),
            byte_len: bytes.len() as u64,
            source: source.to_string(),
            content_hash: content_hash(bytes),
            fetched_at: chrono::Utc::now(),
        })
    }

    fn get(&self, key: &str) -> Result<Vec<u8>, RawStoreError> {
        let path = self.path_for(key)?;
        if !path.exists() {
            return Err(RawStoreError::NotFound(key.to_string()));
        }
        std::fs::read(&path).map_err(|source| RawStoreError::Io {
            path: path.display().to_string(),
            source,
        })
    }

    fn exists(&self, key: &str) -> Result<bool, RawStoreError> {
        Ok(self.path_for(key)?.exists())
    }
}

/// A read layer that resolves a block artifact to EITHER its per-object copy or
/// the compacted bucket holding it — so every existing caller keeps calling
/// `get`/`exists` with a block key and never learns that buckets exist.
///
/// # Why a wrapper and not four new trait methods
///
/// `RawStore` is three methods that each take one key, and `exists()` drives
/// resume decisions where "this block exists" and "the bucket containing it
/// exists" are different questions. Answering the second in terms of the first,
/// once, in one place, is what keeps `decode.rs`, `live.rs` and `tip.rs`
/// unchanged. `put` delegates straight through: buckets are written only by
/// compaction, and nothing else may write one.
///
/// # The cache is the whole read design
///
/// A solid bucket must be decompressed whole (0.03 s for 1,000 Asset Hub
/// blocks), so a naive reader would pay that per block — 1,000x. `decode-range`
/// reads sequentially, so a ONE-entry cache turns a bucket's 2,000 artifact
/// reads into one decompression. This is the "bucket-level CACHE, not a seek
/// index" the prep concluded: framing per block would make offsets addressable
/// and cost 48x the compression.
pub struct BucketedStore<S: RawStore> {
    inner: S,
    bucket_blocks: u64,
    cache: Mutex<Option<(String, Arc<bucket::OpenBucket>)>>,
}

impl<S: RawStore> BucketedStore<S> {
    pub fn new(inner: S, bucket_blocks: u64) -> Self {
        assert!(bucket_blocks > 0, "bucket_blocks must be positive");
        Self { inner, bucket_blocks, cache: Mutex::new(None) }
    }

    pub fn with_default_bucket(inner: S) -> Self {
        Self::new(inner, bucket::DEFAULT_BUCKET_BLOCKS)
    }

    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Serve from the ALREADY-OPEN bucket, doing no I/O at all. Consulted before
    /// the per-object probe: once a bucket is open and holds the member, probing
    /// the per-object key first would be a wasted round trip per artifact — 400
    /// of them across one bucket, which on S3 is 400 HTTP requests rather than a
    /// cheap `stat`. Safe because while both copies exist they are byte-identical
    /// by construction (compaction copies bytes; it never re-derives them).
    fn cached_member(&self, chain: &str, height: u64, item: &str) -> Option<Vec<u8>> {
        let (from, to) = keys::bucket_bounds(height, self.bucket_blocks);
        let want = keys::bucket(chain, from, to);
        let hit = self.cache.lock().unwrap();
        let (k, b) = hit.as_ref()?;
        (*k == want).then(|| b.get(height, item).map(|s| s.to_vec()))?
    }

    /// The bucket that WOULD hold `height`, opened and cached. `Ok(None)` means
    /// no such bucket has been written — never an error, because the ordinary
    /// state of the store is per-object with compaction lagging behind.
    fn open_bucket_for(
        &self,
        chain: &str,
        height: u64,
    ) -> Result<Option<Arc<bucket::OpenBucket>>, RawStoreError> {
        let (from, to) = keys::bucket_bounds(height, self.bucket_blocks);
        let key = keys::bucket(chain, from, to);
        {
            let hit = self.cache.lock().unwrap();
            if let Some((k, b)) = hit.as_ref() {
                if *k == key {
                    return Ok(Some(b.clone()));
                }
            }
        }
        if !self.inner.exists(&key)? {
            return Ok(None);
        }
        let raw = self.inner.get(&key)?;
        let opened = Arc::new(bucket::OpenBucket::open(&raw)?);
        *self.cache.lock().unwrap() = Some((key, opened.clone()));
        Ok(Some(opened))
    }
}

impl<S: RawStore> RawStore for BucketedStore<S> {
    fn put(&self, key: &str, bytes: &[u8], source: &str) -> Result<IngestReceipt, RawStoreError> {
        self.inner.put(key, bytes, source)
    }

    fn get(&self, key: &str) -> Result<Vec<u8>, RawStoreError> {
        let parsed = keys::parse_block(key);
        if let Some((chain, height, item)) = &parsed {
            if let Some(b) = self.cached_member(chain, *height, item) {
                return Ok(b);
            }
        }
        // per-object next: it is the authoritative copy until it is retired.
        match self.inner.get(key) {
            Ok(b) => return Ok(b),
            Err(RawStoreError::NotFound(_)) => {}
            Err(e) => return Err(e),
        }
        let Some((chain, height, item)) = parsed else {
            return Err(RawStoreError::NotFound(key.to_string()));
        };
        match self.open_bucket_for(&chain, height)? {
            Some(b) => b
                .get(height, &item)
                .map(|s| s.to_vec())
                .ok_or_else(|| RawStoreError::NotFound(key.to_string())),
            None => Err(RawStoreError::NotFound(key.to_string())),
        }
    }

    fn exists(&self, key: &str) -> Result<bool, RawStoreError> {
        let parsed = keys::parse_block(key);
        if let Some((chain, height, item)) = &parsed {
            if self.cached_member(chain, *height, item).is_some() {
                return Ok(true);
            }
        }
        if self.inner.exists(key)? {
            return Ok(true);
        }
        let Some((chain, height, item)) = parsed else {
            return Ok(false);
        };
        Ok(self
            .open_bucket_for(&chain, height)?
            .map(|b| b.contains(height, &item))
            .unwrap_or(false))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Unique dir per call — tests run in parallel and must never share state.
    fn tmp_store(tag: &str) -> (FsRawStore, PathBuf) {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "dotlens-rawstore-{}-{tag}-{n}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        (FsRawStore::new(&dir), dir)
    }

    #[test]
    fn put_get_roundtrip_and_idempotent_reput() {
        let (store, dir) = tmp_store("roundtrip");
        let key = keys::block("polkadot", 12_345, "block.scale");
        assert_eq!(key, "raw/polkadot/0000001/12345/block.scale");

        let first = store.put(&key, b"raw-bytes", "fixture").unwrap();
        assert_eq!(store.get(&key).unwrap(), b"raw-bytes");
        assert!(first.content_hash.starts_with("0x") && first.content_hash.len() == 66);
        // identical re-put: fine (idempotent ingestion), same content hash
        let again = store.put(&key, b"raw-bytes", "fixture").unwrap();
        assert_eq!(again.content_hash, first.content_hash);
        // different bytes at same key: refused (immutability)
        assert!(matches!(
            store.put(&key, b"tampered", "fixture"),
            Err(RawStoreError::WouldOverwrite(_))
        ));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn missing_key_is_not_found() {
        let (store, dir) = tmp_store("missing");
        assert!(matches!(
            store.get("raw/nope"),
            Err(RawStoreError::NotFound(_))
        ));
        assert!(!store.exists("raw/nope").unwrap());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn traversal_and_absolute_keys_are_rejected() {
        let (store, dir) = tmp_store("badkeys");
        for key in ["../escape", "raw/../../etc/passwd", "/abs", "", "raw//x", "raw/./x"] {
            assert!(
                matches!(store.put(key, b"x", "t"), Err(RawStoreError::InvalidKey(_))),
                "key should be rejected: {key}"
            );
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    // ---------- the raw-store format slice ----------

    /// Bucket boundaries must be computable from a height ALONE — a reader that
    /// needed a listing to find the object holding a block would turn every
    /// `exists()` into a directory scan.
    #[test]
    fn bucket_bounds_are_aligned_and_need_no_listing() {
        assert_eq!(keys::bucket_bounds(19_410_000, 1000), (19_410_000, 19_410_999));
        assert_eq!(keys::bucket_bounds(19_410_999, 1000), (19_410_000, 19_410_999));
        assert_eq!(keys::bucket_bounds(19_411_000, 1000), (19_411_000, 19_411_999));
        assert_eq!(keys::bucket_bounds(0, 1000), (0, 999));
        // and the bucket object nests inside the existing height/10_000 directory
        assert_eq!(
            keys::bucket("polkadot-asset-hub", 19_410_000, 19_410_999),
            "raw/polkadot-asset-hub/0001941/blocks-19410000-19410999.dlb"
        );
    }

    #[test]
    fn parse_block_inverts_block_and_refuses_the_sibling_namespaces() {
        let k = keys::block("polkadot", 32_613_527, "block.bin");
        assert_eq!(
            keys::parse_block(&k),
            Some(("polkadot".into(), 32_613_527, "block.bin".into()))
        );
        // these are NOT height-keyed block artifacts and must never be resolved
        // against a bucket: a false positive here would serve one chain's bytes
        // for another namespace's key.
        for key in [
            keys::metadata("polkadot", 2003002),
            keys::preimage("polkadot", "abcd", 83),
            keys::unfinalized_block("polkadot", 7, "aa07", "block.bin"),
            keys::simulation("polkadot", "aa", "bb", "x.scale"),
        ] {
            assert_eq!(keys::parse_block(&key), None, "must not parse: {key}");
        }
    }

    #[test]
    fn a_bucket_round_trips_every_member_and_verifies_its_own_hashes() {
        let members: Vec<(u64, String, Vec<u8>)> = (0u64..5)
            .flat_map(|i| {
                [
                    (100 + i, "block.bin".to_string(), vec![i as u8; 64]),
                    (100 + i, "events.scale".to_string(), vec![0xEE, i as u8]),
                ]
            })
            .collect();
        let packed = bucket::pack("mock", &members, 3).unwrap();
        assert_eq!(&packed[..7], bucket::BUCKET_MAGIC);

        let open = bucket::OpenBucket::open(&packed).unwrap();
        assert_eq!(open.manifest.from_height, 100);
        assert_eq!(open.manifest.to_height, 104);
        assert_eq!(open.manifest.level, 3);
        assert_eq!(open.manifest.heights(), vec![100, 101, 102, 103, 104]);
        for (h, item, bytes) in &members {
            assert_eq!(open.get(*h, item), Some(&bytes[..]), "member {h}/{item}");
            assert!(open.contains(*h, item));
        }
        assert!(!open.contains(999, "block.bin"));
        assert!(open.get(100, "nope.scale").is_none());
        open.verify().expect("hashes must match the manifest");
    }

    /// The manifest carries the per-block accounting that `core.ingest_receipts`
    /// can no longer hold once N blocks share one key — so it MUST be readable
    /// without pulling the payload, or a gap audit would download the archive.
    #[test]
    fn the_manifest_reads_from_a_prefix_without_decompressing_the_payload() {
        let members: Vec<(u64, String, Vec<u8>)> = (0u64..50)
            .map(|i| (1000 + i, "block.bin".to_string(), vec![i as u8; 4096]))
            .collect();
        let packed = bucket::pack("mock", &members, 3).unwrap();

        // a short prefix is "fetch more", never an error
        assert!(bucket::read_manifest(&packed[..8]).unwrap().is_none());

        let mlen = u32::from_le_bytes(packed[8..12].try_into().unwrap()) as usize;
        let prefix = &packed[..12 + mlen];
        assert!(prefix.len() < packed.len(), "prefix must be shorter than the object");
        let m = bucket::read_manifest(prefix).unwrap().expect("manifest from prefix");
        assert_eq!(m.members.len(), 50);
        assert_eq!(m.heights().len(), 50);
        // and every member's hash is the SAME hash a per-object receipt holds,
        // which is what makes retiring the per-object copy checkable.
        assert_eq!(m.members[7].content_hash, content_hash(&members[7].2));
    }

    #[test]
    fn a_corrupt_member_is_caught_and_a_disagreeing_manifest_is_refused() {
        let members = vec![(1u64, "block.bin".to_string(), vec![7u8; 32])];
        let mut packed = bucket::pack("mock", &members, 3).unwrap();
        bucket::OpenBucket::open(&packed).unwrap().verify().unwrap();

        // claim a length the frame does not hold -> refuse to serve members at all
        let mlen = u32::from_le_bytes(packed[8..12].try_into().unwrap()) as usize;
        let mut man: bucket::BucketManifest =
            serde_json::from_slice(&packed[12..12 + mlen]).unwrap();
        man.members[0].len = 31;
        let re = serde_json::to_vec(&man).unwrap();
        assert_eq!(re.len(), mlen, "test needs an equal-length manifest");
        packed[12..12 + mlen].copy_from_slice(&re);
        let err = bucket::OpenBucket::open(&packed).unwrap_err();
        assert!(
            format!("{err}").contains("manifest and payload disagree"),
            "got: {err}"
        );
    }

    #[test]
    fn packing_nothing_is_refused_and_a_foreign_object_is_not_a_bucket() {
        assert!(bucket::pack("mock", &[], 3).is_err());
        assert!(bucket::read_manifest(b"{\"height\":1}--------------").is_err());
    }

    /// The point of the whole read layer: a caller asks for a block key and gets
    /// bytes, whether the per-object copy is still there or only the bucket is.
    #[test]
    fn a_bucket_serves_a_block_whose_per_object_copy_is_gone() {
        let (store, dir) = tmp_store("bucketed");
        let per_object = keys::block("mock", 2_000_007, "block.bin");
        store.put(&per_object, b"the-block-bytes", "t").unwrap();

        let bucketed = BucketedStore::new(store, 1000);
        // 1. per-object copy present -> served from it
        assert_eq!(bucketed.get(&per_object).unwrap(), b"the-block-bytes");
        assert!(bucketed.exists(&per_object).unwrap());

        // 2. compaction writes the bucket beside it (write-once: nothing is
        //    rewritten, the originals stay) and both answers agree
        let packed = bucket::pack(
            "mock",
            &[(2_000_007, "block.bin".into(), b"the-block-bytes".to_vec())],
            3,
        )
        .unwrap();
        let (from, to) = keys::bucket_bounds(2_000_007, 1000);
        bucketed.put(&keys::bucket("mock", from, to), &packed, "compact").unwrap();
        assert_eq!(bucketed.get(&per_object).unwrap(), b"the-block-bytes");

        // 3. RETIREMENT: delete the per-object copy; the bucket still answers
        std::fs::remove_file(dir.join(&per_object)).unwrap();
        assert!(!bucketed.inner().exists(&per_object).unwrap());
        assert!(bucketed.exists(&per_object).unwrap(), "bucket must answer exists()");
        assert_eq!(bucketed.get(&per_object).unwrap(), b"the-block-bytes");

        // 4. a height with neither copy is honestly absent
        let missing = keys::block("mock", 2_000_008, "block.bin");
        assert!(!bucketed.exists(&missing).unwrap());
        assert!(matches!(bucketed.get(&missing), Err(RawStoreError::NotFound(_))));
        let _ = std::fs::remove_dir_all(dir);
    }

    /// A solid bucket is decompressed WHOLE, so without a cache a sequential
    /// reader pays that once per artifact. One entry is enough because
    /// `decode-range` walks heights in order and both artifacts of a height live
    /// in the same bucket.
    #[test]
    fn the_bucket_cache_decompresses_once_for_a_sequential_walk() {
        let (store, dir) = tmp_store("bucketcache");
        let members: Vec<(u64, String, Vec<u8>)> = (0u64..200)
            .flat_map(|i| {
                [
                    (5000 + i, "block.bin".to_string(), vec![i as u8; 512]),
                    (5000 + i, "events.scale".to_string(), vec![i as u8; 8]),
                ]
            })
            .collect();
        let packed = bucket::pack("mock", &members, 3).unwrap();
        store.put(&keys::bucket("mock", 5000, 5999), &packed, "t").unwrap();

        // CountingStore records how many times the bucket object is fetched
        struct Counting {
            inner: FsRawStore,
            gets: std::sync::atomic::AtomicUsize,
        }
        impl RawStore for Counting {
            fn put(&self, k: &str, b: &[u8], s: &str) -> Result<IngestReceipt, RawStoreError> {
                self.inner.put(k, b, s)
            }
            fn get(&self, k: &str) -> Result<Vec<u8>, RawStoreError> {
                self.gets.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.inner.get(k)
            }
            fn exists(&self, k: &str) -> Result<bool, RawStoreError> {
                self.inner.exists(k)
            }
        }
        let counting = Counting { inner: store, gets: Default::default() };
        let bucketed = BucketedStore::new(counting, 1000);
        for i in 0..200u64 {
            for item in ["block.bin", "events.scale"] {
                let got = bucketed.get(&keys::block("mock", 5000 + i, item)).unwrap();
                assert_eq!(got[0], i as u8);
            }
        }
        // 400 artifact reads must cost ONE per-object miss + ONE bucket fetch.
        // Anything that scales with the read count is a wasted round trip per
        // artifact, which on S3 is an HTTP request rather than a cheap stat.
        let inner_gets = bucketed
            .inner()
            .gets
            .load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(
            inner_gets, 2,
            "400 sequential artifact reads must not scale I/O with the read count"
        );
        let _ = std::fs::remove_dir_all(dir);
    }
}
