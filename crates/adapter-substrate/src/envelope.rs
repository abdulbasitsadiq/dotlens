//! The block envelope: v1 (`serde_json` + 0x-hex) and v2 (binary), one reader.
//!
//! Invariant 4 puts this here rather than in `raw-store`: a block envelope is
//! protocol knowledge. `raw-store` compresses and buckets opaque bytes and knows
//! nothing about what is in them.
//!
//! # Why v2 exists, measured rather than assumed
//!
//! v1 is `serde_json` with hex-encoded extrinsics, and 99.8% of its bytes are
//! hex strings. A binary envelope is **2.00x smaller uncompressed** on both
//! chains (Asset Hub 102,834 -> 51,316 B/blk; relay 166,274 -> 83,041) — but
//! after bucketed zstd it is only **1.07x**, because the entropy coder already
//! recovers hex's redundancy. The case for v2 is therefore NOT storage. It is
//! decode CPU, measured through this crate's own `FrameDecoder::decode_block`:
//!
//! ```text
//!                                Asset Hub          relay
//!   full decode_block            1.95 ms/blk        6.60 ms/blk
//!   v1 JSON+hex envelope parse   0.30 ms = 15.3%    0.55 ms = 8.4%
//!   v2 binary envelope parse     0.00 ms =  0.06%   0.00 ms = 0.01%
//! ```
//!
//! i.e. **8.4%-15.2% of every decode**, and Invariant 1 makes that recurring
//! rather than one-off: a `decoder_version` bump re-decodes the archive.
//!
//! # Self-describing bytes, and why the ITEM NAME also changes
//!
//! The format is sniffed from the bytes (v1 starts `{`, v2 starts `D`), so no
//! sidecar records it. But the two forms are stored under DIFFERENT item names —
//! `block.json` and `block.bin` — because the store is write-once: re-ingesting
//! a v1-era block after the format change would otherwise present different
//! bytes at an existing key and be refused as a contradiction, when it is only a
//! format change. Kept beside, never instead of — the same rule that lets
//! `metadata-v15.scale` sit next to `metadata.scale`.
//!
//! # Compaction never re-encodes
//!
//! A v1 envelope stays v1 inside a bucket. Re-encoding it to v2 would change its
//! `content_hash`, and that hash is what `core.ingest_receipts` recorded at
//! ingestion — the one thing that makes retiring a per-object copy checkable.
//! So the ~104K blocks already in the store stay hex-JSON forever and only new
//! writes are v2. Against a ~52M-block backfill that is ~0.2% of the archive,
//! and it keeps the v1 path exercised by real data rather than by fixtures only.

pub const ENVELOPE_MAGIC: &[u8; 7] = b"DOTLBLK";
pub const ENVELOPE_V2: u8 = 2;

// Item names are defined once, in `raw_store::keys` — the crate that already
// owns every key string. Re-exported so this module reads naturally without
// becoming a second place they could drift.
pub use raw_store::keys::{BLOCK_ITEMS, BLOCK_ITEM_V1 as ITEM_V1, BLOCK_ITEM_V2 as ITEM_V2};

#[derive(Debug, Clone, PartialEq)]
pub struct BlockEnvelope {
    pub chain_id: String,
    pub height: u64,
    pub hash: String,
    pub parent_hash: String,
    pub state_root: String,
    pub extrinsics_root: String,
    pub spec_version: u32,
    pub finalized: bool,
    /// Extrinsics as BYTES. v1 stores them 0x-hex and is decoded here, so the
    /// rest of the decoder sees one shape whichever format it came from.
    pub extrinsics: Vec<Vec<u8>>,
}

#[derive(Debug, thiserror::Error)]
#[error("envelope: {0}")]
pub struct EnvelopeError(pub String);

fn err<T>(m: impl Into<String>) -> Result<T, EnvelopeError> {
    Err(EnvelopeError(m.into()))
}

fn hex32(s: &str) -> Result<[u8; 32], EnvelopeError> {
    let t = s.strip_prefix("0x").unwrap_or(s);
    let v = hex::decode(t).map_err(|e| EnvelopeError(format!("hash {s}: {e}")))?;
    v.try_into()
        .map_err(|_| EnvelopeError(format!("hash {s} is not 32 bytes")))
}

/// Encode a v2 envelope. Refuses a non-32-byte hash rather than padding one:
/// the format is fixed-width there, and a silently truncated block hash is a
/// wrong join key in every table downstream.
pub fn encode_v2(e: &BlockEnvelope) -> Result<Vec<u8>, EnvelopeError> {
    let mut out = Vec::with_capacity(128 + e.extrinsics.iter().map(|x| x.len() + 4).sum::<usize>());
    out.extend_from_slice(ENVELOPE_MAGIC);
    out.push(ENVELOPE_V2);
    let cb = e.chain_id.as_bytes();
    out.extend_from_slice(&(cb.len() as u32).to_le_bytes());
    out.extend_from_slice(cb);
    out.extend_from_slice(&e.height.to_le_bytes());
    for h in [&e.hash, &e.parent_hash, &e.state_root, &e.extrinsics_root] {
        out.extend_from_slice(&hex32(h)?);
    }
    out.extend_from_slice(&e.spec_version.to_le_bytes());
    out.push(e.finalized as u8);
    out.extend_from_slice(&(e.extrinsics.len() as u32).to_le_bytes());
    for x in &e.extrinsics {
        out.extend_from_slice(&(x.len() as u32).to_le_bytes());
        out.extend_from_slice(x);
    }
    Ok(out)
}

struct Cursor<'a> {
    b: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], EnvelopeError> {
        if self.at + n > self.b.len() {
            return err(format!(
                "truncated: wanted {n} bytes at offset {} of {}",
                self.at,
                self.b.len()
            ));
        }
        let s = &self.b[self.at..self.at + n];
        self.at += n;
        Ok(s)
    }
    fn u32(&mut self) -> Result<u32, EnvelopeError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, EnvelopeError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn hash(&mut self) -> Result<String, EnvelopeError> {
        Ok(format!("0x{}", hex::encode(self.take(32)?)))
    }
}

fn decode_v2(bytes: &[u8]) -> Result<BlockEnvelope, EnvelopeError> {
    let mut c = Cursor { b: bytes, at: 0 };
    c.take(7)?;
    let ver = c.take(1)?[0];
    if ver != ENVELOPE_V2 {
        return err(format!(
            "envelope version {ver} is not readable by this build (knows {ENVELOPE_V2})"
        ));
    }
    let cl = c.u32()? as usize;
    let chain_id = String::from_utf8(c.take(cl)?.to_vec())
        .map_err(|e| EnvelopeError(format!("chain_id: {e}")))?;
    let height = c.u64()?;
    let (hash, parent_hash, state_root, extrinsics_root) =
        (c.hash()?, c.hash()?, c.hash()?, c.hash()?);
    let spec_version = c.u32()?;
    let finalized = match c.take(1)?[0] {
        0 => false,
        1 => true,
        // never coerce: `finalized` decides whether a row is replaceable, and a
        // byte we cannot read must not silently become the permissive value.
        other => return err(format!("finalized byte {other} is neither 0 nor 1")),
    };
    let n = c.u32()? as usize;
    let mut extrinsics = Vec::with_capacity(n.min(4096));
    for i in 0..n {
        let l = c.u32()? as usize;
        extrinsics.push(
            c.take(l)
                .map_err(|e| EnvelopeError(format!("extrinsic {i}: {e}")))?
                .to_vec(),
        );
    }
    if c.at != bytes.len() {
        return err(format!(
            "{} trailing bytes after the envelope",
            bytes.len() - c.at
        ));
    }
    Ok(BlockEnvelope {
        chain_id,
        height,
        hash,
        parent_hash,
        state_root,
        extrinsics_root,
        spec_version,
        finalized,
        extrinsics,
    })
}

fn decode_v1(bytes: &[u8]) -> Result<BlockEnvelope, EnvelopeError> {
    #[derive(serde::Deserialize)]
    struct V1 {
        #[serde(default)]
        chain_id: String,
        height: u64,
        hash: String,
        parent_hash: String,
        #[serde(default)]
        state_root: String,
        #[serde(default)]
        extrinsics_root: String,
        spec_version: u32,
        // default TRUE: `finalized: false` means "replaceable + invisible to
        // balances", so an absent field must fail SAFE (immutable), never open.
        #[serde(default = "yes")]
        finalized: bool,
        extrinsics: Vec<String>,
    }
    fn yes() -> bool {
        true
    }
    let v: V1 = serde_json::from_slice(bytes).map_err(|e| EnvelopeError(e.to_string()))?;
    let mut extrinsics = Vec::with_capacity(v.extrinsics.len());
    for (i, x) in v.extrinsics.iter().enumerate() {
        let t = x.strip_prefix("0x").unwrap_or(x);
        extrinsics.push(
            hex::decode(t).map_err(|e| EnvelopeError(format!("extrinsic {i}: {e}")))?,
        );
    }
    Ok(BlockEnvelope {
        chain_id: v.chain_id,
        height: v.height,
        hash: v.hash,
        parent_hash: v.parent_hash,
        state_root: v.state_root,
        extrinsics_root: v.extrinsics_root,
        spec_version: v.spec_version,
        finalized: v.finalized,
        extrinsics,
    })
}

/// Read either format. The discriminator is the first byte and nothing else:
/// v1 is JSON and begins `{` (0x7B), v2 begins with `D` (0x44). Bytes that are
/// neither are refused by name rather than guessed at.
pub fn decode_any(bytes: &[u8]) -> Result<BlockEnvelope, EnvelopeError> {
    match bytes.first() {
        Some(b'{') => decode_v1(bytes),
        Some(_) if bytes.len() >= 8 && &bytes[..7] == ENVELOPE_MAGIC => decode_v2(bytes),
        Some(b) => err(format!(
            "not a block envelope: first byte 0x{b:02x} is neither '{{' (v1 JSON) nor the v2 magic"
        )),
        None => err("empty envelope"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> BlockEnvelope {
        BlockEnvelope {
            chain_id: "polkadot-asset-hub".into(),
            height: 19_410_000,
            hash: format!("0x{}", "11".repeat(32)),
            parent_hash: format!("0x{}", "22".repeat(32)),
            state_root: format!("0x{}", "33".repeat(32)),
            extrinsics_root: format!("0x{}", "44".repeat(32)),
            spec_version: 2_003_002,
            finalized: true,
            extrinsics: vec![vec![0xde, 0xad, 0xbe, 0xef], vec![], vec![7u8; 300]],
        }
    }

    #[test]
    fn v2_round_trips_including_a_zero_length_extrinsic() {
        let e = sample();
        let bytes = encode_v2(&e).unwrap();
        assert_eq!(&bytes[..7], ENVELOPE_MAGIC);
        assert_eq!(bytes[7], ENVELOPE_V2);
        assert_eq!(decode_any(&bytes).unwrap(), e);
    }

    /// THE load-bearing test of the whole format change: the same block written
    /// both ways must parse to the same thing, field for field. If this fails,
    /// a v2 re-decode is not comparable with its v1 result and the migration
    /// cannot be proven byte-identical.
    #[test]
    fn v1_json_and_v2_binary_of_one_block_decode_identically() {
        let e = sample();
        let v1 = serde_json::json!({
            "chain_id": e.chain_id,
            "height": e.height,
            "hash": e.hash,
            "parent_hash": e.parent_hash,
            "state_root": e.state_root,
            "extrinsics_root": e.extrinsics_root,
            "spec_version": e.spec_version,
            "finalized": e.finalized,
            "extrinsics": e.extrinsics.iter()
                .map(|x| format!("0x{}", hex::encode(x))).collect::<Vec<_>>(),
        });
        let from_v1 = decode_any(&serde_json::to_vec(&v1).unwrap()).unwrap();
        let from_v2 = decode_any(&encode_v2(&e).unwrap()).unwrap();
        assert_eq!(from_v1, from_v2);
        assert_eq!(from_v1, e);
    }

    #[test]
    fn an_absent_finalized_field_fails_safe_and_an_unreadable_one_is_refused() {
        // v1: absent means immutable, never replaceable
        let j = serde_json::json!({
            "height": 1, "hash": format!("0x{}", "00".repeat(32)),
            "parent_hash": format!("0x{}", "00".repeat(32)),
            "spec_version": 1, "extrinsics": [] });
        assert!(decode_any(&serde_json::to_vec(&j).unwrap()).unwrap().finalized);
        // v2: a byte that is neither 0 nor 1 halts rather than becoming `true`
        let mut b = encode_v2(&sample()).unwrap();
        let pos = 7 + 1 + 4 + "polkadot-asset-hub".len() + 8 + 128 + 4;
        assert_eq!(b[pos], 1);
        b[pos] = 2;
        assert!(format!("{}", decode_any(&b).unwrap_err()).contains("neither 0 nor 1"));
    }

    #[test]
    fn truncation_trailing_bytes_and_foreign_formats_are_all_refused_by_name() {
        let full = encode_v2(&sample()).unwrap();
        assert!(format!("{}", decode_any(&full[..full.len() - 5]).unwrap_err()).contains("truncated"));
        let mut extra = full.clone();
        extra.push(0);
        assert!(format!("{}", decode_any(&extra).unwrap_err()).contains("trailing"));
        assert!(format!("{}", decode_any(b"\x00nonsense").unwrap_err()).contains("neither"));
        assert!(decode_any(b"").is_err());
        // a bucket object must never be mistaken for an envelope
        assert!(decode_any(b"DOTLBKT\x01").is_err());
    }

    #[test]
    fn a_short_hash_is_refused_rather_than_padded() {
        let mut e = sample();
        e.hash = "0xdeadbeef".into();
        assert!(format!("{}", encode_v2(&e).unwrap_err()).contains("not 32 bytes"));
    }
}
