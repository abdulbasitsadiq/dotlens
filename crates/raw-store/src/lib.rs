//! Raw store: immutable, write-once storage for original bytes (Invariant 1).
//!
//! Phase 0 ships the filesystem backend; an S3/MinIO backend implements the same
//! trait later without touching callers. Keys follow ARCHITECTURE.md §6:
//!   raw/{chain}/{bucket}/{height}/{item}     (bucket = height / 10_000)
//!   raw/{chain}/meta/{runtime_version}/metadata.scale

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

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
    pub fn block(chain: &str, height: u64, item: &str) -> String {
        format!("raw/{chain}/{:07}/{height}/{item}", height / 10_000)
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
}
