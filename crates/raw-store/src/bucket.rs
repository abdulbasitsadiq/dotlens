//! Solid-bucket container: many artifacts, one key, one zstd frame.
//!
//! MEASURED, and the shape of the win is not what ROADMAP assumed
//! (`PREP-raw-store-format.md` + its addendum, both against the real 11 GB store):
//!
//! ```text
//!   bucket=1000, zstd -3     Asset Hub 101.2x     relay 2.4x
//!   bucket size on AH        250 -> 89.9x   500 -> 97.0x   1000 -> 101.2x   2000 -> 101.9x
//!   bucket size on the relay FLAT at 2.4x for every size tested
//! ```
//!
//! So bucketing is TWO different changes wearing one name, and the slice must
//! not describe it as one. On a PARACHAIN it is the entire compression story: a
//! block is dominated by `set_validation_data`'s relay state proof, which barely
//! changes between consecutive blocks, so an LZ window spanning several blocks
//! finds it again (per-object is 2.3x; bucketed is 101x). On the RELAY it is
//! purely the Class-A-write optimisation ROADMAP originally described — per-block
//! validator signatures and candidate receipts are high entropy, there is nothing
//! to match against, and the ratio does not move with bucket size at all.
//!
//! # Why the frame is SOLID and there is no seek index
//!
//! One zstd frame per member would make a bucket offset-addressable — and drops
//! Asset Hub from 116x to 2.39x, because a frame boundary is exactly where
//! cross-block matching stops. It buys nothing we need: decompressing a whole
//! 1,000-block bucket takes 0.03 s and `decode-range` reads sequentially anyway.
//! The read path wants a CACHE (see `BucketedStore`), not an index.
//!
//! # Why the manifest is outside the frame
//!
//! `core.ingest_receipts` is per KEY, and a bucket is one key — so compaction
//! would silently drop the per-block accounting the Phase 1 endurance drill
//! rests on ("98,909 receipts = 98,909 distinct heights, ZERO dups, missing =
//! exactly one contiguous tail"). The manifest carries every member's height,
//! item, length and `content_hash`, uncompressed and at a known offset, so that
//! accounting survives INSIDE the object and can be read with a ranged GET
//! without pulling 100 MB.
//!
//! ```text
//!   "DOTLBKT" | version u8 | manifest_len u32 LE | manifest (JSON) | zstd frame
//! ```

use crate::{content_hash, RawStoreError};
use serde::{Deserialize, Serialize};

pub const BUCKET_MAGIC: &[u8; 7] = b"DOTLBKT";
pub const BUCKET_VERSION: u8 = 1;

/// Measured knee. -1 is too low (AH 56.9x), -9 buys 3.7% over -3, and -19 buys
/// 17% for 4.6x the time on a parachain while being 150x slower on the relay —
/// which is ~6.6 TB of input, i.e. the difference between hours and weeks.
/// Configurable because compression level is NOT monotonic on this data.
pub const DEFAULT_LEVEL: i32 = 3;

/// Measured: the AH knee is 250–500 and 2000 buys 0.7% over 1000. Above 1000 is
/// read amplification for nothing.
pub const DEFAULT_BUCKET_BLOCKS: u64 = 1000;

/// One artifact inside a bucket. `content_hash` is over the member's LOGICAL
/// bytes — the same hash `core.ingest_receipts` holds for the per-object copy,
/// so a compacted bucket can be audited against receipts written before it existed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BucketMember {
    pub height: u64,
    pub item: String,
    pub len: u64,
    pub content_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BucketManifest {
    pub chain_id: String,
    pub from_height: u64,
    pub to_height: u64,
    /// Recorded rather than assumed: a later bucket written at a different level
    /// is still readable, and the figure that produced these bytes is on the object.
    pub level: i32,
    pub members: Vec<BucketMember>,
}

impl BucketManifest {
    /// Heights present, deduplicated and ascending — the gap-accounting answer.
    pub fn heights(&self) -> Vec<u64> {
        let mut h: Vec<u64> = self.members.iter().map(|m| m.height).collect();
        h.sort_unstable();
        h.dedup();
        h
    }
}

/// Build a bucket object. `members` is (height, item, bytes) in the order they
/// will be laid down; ordering by (height, item) is what makes consecutive
/// blocks adjacent and is therefore the entire compression win — a caller that
/// shuffles gets a correct object and a useless ratio.
pub fn pack(
    chain_id: &str,
    members: &[(u64, String, Vec<u8>)],
    level: i32,
) -> Result<Vec<u8>, RawStoreError> {
    if members.is_empty() {
        return Err(RawStoreError::Bucket(
            "refusing to pack an empty bucket".into(),
        ));
    }
    let mut payload = Vec::new();
    let mut entries = Vec::with_capacity(members.len());
    for (height, item, bytes) in members {
        entries.push(BucketMember {
            height: *height,
            item: item.clone(),
            len: bytes.len() as u64,
            content_hash: content_hash(bytes),
        });
        payload.extend_from_slice(bytes);
    }
    let manifest = BucketManifest {
        chain_id: chain_id.to_string(),
        from_height: entries.iter().map(|m| m.height).min().unwrap(),
        to_height: entries.iter().map(|m| m.height).max().unwrap(),
        level,
        members: entries,
    };
    let mjson = serde_json::to_vec(&manifest)
        .map_err(|e| RawStoreError::Bucket(format!("serializing manifest: {e}")))?;
    let frame = zstd::stream::encode_all(&payload[..], level)
        .map_err(|e| RawStoreError::Bucket(format!("zstd encode: {e}")))?;

    let mut out = Vec::with_capacity(9 + mjson.len() + frame.len());
    out.extend_from_slice(BUCKET_MAGIC);
    out.push(BUCKET_VERSION);
    out.extend_from_slice(&(mjson.len() as u32).to_le_bytes());
    out.extend_from_slice(&mjson);
    out.extend_from_slice(&frame);
    Ok(out)
}

/// Read the manifest WITHOUT decompressing the payload. `bytes` may be a prefix
/// of the object (a ranged GET); this returns `Ok(None)` if the prefix is too
/// short to contain the whole manifest, which is a "fetch more" answer and never
/// an error.
pub fn read_manifest(bytes: &[u8]) -> Result<Option<BucketManifest>, RawStoreError> {
    if bytes.len() < 12 {
        return Ok(None);
    }
    if &bytes[..7] != BUCKET_MAGIC {
        return Err(RawStoreError::Bucket(
            "not a bucket object (bad magic)".into(),
        ));
    }
    let ver = bytes[7];
    if ver != BUCKET_VERSION {
        return Err(RawStoreError::Bucket(format!(
            "bucket container version {ver} is not readable by this build (knows {BUCKET_VERSION})"
        )));
    }
    let mlen = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
    if bytes.len() < 12 + mlen {
        return Ok(None);
    }
    let manifest: BucketManifest = serde_json::from_slice(&bytes[12..12 + mlen])
        .map_err(|e| RawStoreError::Bucket(format!("manifest: {e}")))?;
    Ok(Some(manifest))
}

/// A bucket decompressed once, ready to serve its members.
pub struct OpenBucket {
    pub manifest: BucketManifest,
    payload: Vec<u8>,
    /// Byte offset of each member in `payload`, parallel to `manifest.members`.
    offsets: Vec<usize>,
}

/// Hand-written so a panic message never prints a decompressed 100 MB payload —
/// `unwrap_err()` on `open()` requires the Ok type to be `Debug`.
impl std::fmt::Debug for OpenBucket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenBucket")
            .field("chain_id", &self.manifest.chain_id)
            .field("from_height", &self.manifest.from_height)
            .field("to_height", &self.manifest.to_height)
            .field("members", &self.manifest.members.len())
            .field("payload_bytes", &self.payload.len())
            .finish()
    }
}

impl OpenBucket {
    /// Decompress the whole object. This is the read path: 0.03 s for a
    /// 1,000-block Asset Hub bucket, which is why there is no seek index.
    pub fn open(bytes: &[u8]) -> Result<Self, RawStoreError> {
        let manifest = read_manifest(bytes)?
            .ok_or_else(|| RawStoreError::Bucket("truncated bucket object".into()))?;
        let mlen = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
        let payload = zstd::stream::decode_all(&bytes[12 + mlen..])
            .map_err(|e| RawStoreError::Bucket(format!("zstd decode: {e}")))?;

        let mut offsets = Vec::with_capacity(manifest.members.len());
        let mut at = 0usize;
        for m in &manifest.members {
            offsets.push(at);
            at += m.len as usize;
        }
        if at != payload.len() {
            return Err(RawStoreError::Bucket(format!(
                "manifest describes {at} bytes but the frame holds {} — refusing to serve members \
                 from an object whose manifest and payload disagree",
                payload.len()
            )));
        }
        Ok(Self {
            manifest,
            payload,
            offsets,
        })
    }

    pub fn get(&self, height: u64, item: &str) -> Option<&[u8]> {
        let i = self
            .manifest
            .members
            .iter()
            .position(|m| m.height == height && m.item == item)?;
        Some(
            &self.payload[self.offsets[i]..self.offsets[i] + self.manifest.members[i].len as usize],
        )
    }

    pub fn contains(&self, height: u64, item: &str) -> bool {
        self.manifest
            .members
            .iter()
            .any(|m| m.height == height && m.item == item)
    }

    /// Every member's stored bytes must hash to the `content_hash` its manifest
    /// claims. This is what makes retiring the per-object copies safe: the check
    /// is against the SAME hash `core.ingest_receipts` recorded at ingestion.
    pub fn verify(&self) -> Result<(), RawStoreError> {
        for (i, m) in self.manifest.members.iter().enumerate() {
            let b = &self.payload[self.offsets[i]..self.offsets[i] + m.len as usize];
            let got = content_hash(b);
            if got != m.content_hash {
                return Err(RawStoreError::Bucket(format!(
                    "member {}/{} hashes {} but the manifest says {}",
                    m.height, m.item, got, m.content_hash
                )));
            }
        }
        Ok(())
    }
}
