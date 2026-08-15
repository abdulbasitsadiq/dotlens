//! Substrate chain adapter — Phase 0: decode-only, driven by fixtures.
//!
//! Phase 0 scope (deliberate): the decoder consumes the *raw block envelope*
//! format we archive (JSON wrapper around hex SCALE payloads + pre-decoded
//! call/event JSON captured at fixture time). It proves the pipeline shape:
//!
//! ```text
//! raw bytes ──(pure fn, keyed by runtime_version)──▶ CanonicalBlock + lineage
//! ```
//!
//! Phase 1 replaces the inner decode with real SCALE decoding via
//! subxt ≥0.50 / frame-decode against archived metadata blobs, WITHOUT changing
//! this crate's public interface. `DECODER_VERSION` exists precisely so every
//! row written by this version can be identified and rebuilt later.
//!
//! Invariant: decoding is a pure function of (bytes, runtime context, decoder
//! version). No I/O, no clocks, no network in the decode path.

/// Live fetch side (subxt) — see `SubstrateSource`. Decode stays pure below.
#[cfg(feature = "live")]
pub mod source;

/// Real SCALE decoding against archived metadata (decoder_version 2).
pub mod frame_decoder;

/// System-account derivation + labeling primitives (modl/para/sibl, SS58
/// decode, PalletId extraction from metadata). Pure — no I/O.
pub mod accounts;

use canonical::{CanonicalBlock, CanonicalEvent, CanonicalTransaction, Lineage};
use serde::{Deserialize, Serialize};

/// Bump on ANY change to decode behavior. Derived tables record it (lineage);
/// a bump is what makes "rebuild from raw" meaningful.
pub const DECODER_VERSION: u32 = 1;

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("envelope parse error: {0}")]
    Envelope(#[from] serde_json::Error),
    #[error("envelope chain_id is empty")]
    MissingChain,
    #[error("extrinsic {index}: scale_hex is not valid hex: {source}")]
    BadHex {
        index: usize,
        source: hex::FromHexError,
    },
}

/// The archived raw-block envelope (what `raw-store` holds per block, alongside
/// the original SCALE payloads it references).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawBlockEnvelope {
    pub chain_id: String,
    pub height: u64,
    pub hash: String,
    pub parent_hash: String,
    /// Substrate spec_version this block must be decoded against.
    pub spec_version: u32,
    /// Unix millis from the timestamp inherent, if captured.
    pub timestamp_ms: Option<i64>,
    pub finalized: bool,
    pub extrinsics: Vec<RawExtrinsic>,
    pub events: Vec<RawEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawExtrinsic {
    /// Original SCALE bytes, hex-encoded ("0x…"). Ground truth.
    pub scale_hex: String,
    /// Fixture-time decode (Phase 0) — replaced by real decoding in Phase 1.
    pub call: String,
    pub signer: Option<String>,
    #[serde(default)]
    pub args: serde_json::Value,
    pub success: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawEvent {
    pub name: String,
    #[serde(default)]
    pub transaction_index: Option<u32>,
    #[serde(default)]
    pub data: serde_json::Value,
}

/// Pure decode: envelope bytes → CanonicalBlock with full lineage.
pub fn decode_block(
    envelope_bytes: &[u8],
    raw_location: &str,
) -> Result<CanonicalBlock, DecodeError> {
    let env: RawBlockEnvelope = serde_json::from_slice(envelope_bytes)?;
    if env.chain_id.is_empty() {
        return Err(DecodeError::MissingChain);
    }

    let timestamp = env
        .timestamp_ms
        .and_then(|ms| chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms));

    let transactions = env
        .extrinsics
        .iter()
        .enumerate()
        .map(|(i, x)| {
            Ok(CanonicalTransaction {
                index: i as u32,
                hash: Some(extrinsic_hash(i, &x.scale_hex)?),
                signer: x.signer.clone(),
                call: x.call.clone(),
                args: x.args.clone(),
                success: x.success,
            })
        })
        .collect::<Result<Vec<_>, DecodeError>>()?;

    let events = env
        .events
        .iter()
        .enumerate()
        .map(|(i, e)| CanonicalEvent {
            index: i as u32,
            transaction_index: e.transaction_index,
            name: e.name.clone(),
            data: e.data.clone(),
        })
        .collect();

    Ok(CanonicalBlock {
        chain_id: env.chain_id,
        height: env.height,
        hash: env.hash,
        parent_hash: env.parent_hash,
        timestamp,
        finalized: env.finalized,
        lineage: Lineage {
            runtime_version: env.spec_version,
            decoder_version: DECODER_VERSION,
            raw_location: raw_location.to_string(),
        },
        transactions,
        events,
    })
}

/// blake2b-256 of the extrinsic SCALE bytes — Substrate's extrinsic hash.
/// Invalid hex is an error, never a silent empty-hash (that would give every
/// malformed extrinsic the same "hash").
fn extrinsic_hash(index: usize, scale_hex: &str) -> Result<String, DecodeError> {
    use blake2::digest::{consts::U32, Digest};
    type Blake2b256 = blake2::Blake2b<U32>;
    let bytes = hex::decode(scale_hex.trim_start_matches("0x"))
        .map_err(|source| DecodeError::BadHex { index, source })?;
    let mut hasher = Blake2b256::new();
    hasher.update(&bytes);
    Ok(format!("0x{}", hex::encode(hasher.finalize())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn fixture(name: &str) -> Vec<u8> {
        let p = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/synthetic")
            .join(name);
        std::fs::read(&p).unwrap_or_else(|e| panic!("fixture {} unreadable: {e}", p.display()))
    }

    #[test]
    fn decodes_fixture_with_full_lineage() {
        let bytes = fixture("polkadot-asset-hub-19000001.json");
        let block = decode_block(&bytes, "raw/polkadot-asset-hub/0001900/19000001/block.json")
            .expect("decodes");
        assert_eq!(block.chain_id, "polkadot-asset-hub");
        assert_eq!(block.height, 19_000_001);
        assert_eq!(block.lineage.runtime_version, 2_000_006);
        assert_eq!(block.lineage.decoder_version, DECODER_VERSION);
        assert!(block.lineage.raw_location.starts_with("raw/"));
        assert_eq!(block.transactions.len(), 2);
        assert_eq!(block.events.len(), 3);
        // signed extrinsic got a real blake2b-256 hash — and NOT the empty-input
        // hash (0x0e5751c0…), which is what silent hex failure would produce
        let h = block.transactions[1].hash.as_deref().unwrap();
        assert!(h.starts_with("0x") && h.len() == 66);
        assert_ne!(
            h,
            "0x0e5751c026e543b2e8ab2eb06099daa1d1e5df47778f7787faab45cdf12fe3a8"
        );
    }

    #[test]
    fn invalid_hex_is_an_error_not_a_fake_hash() {
        let bytes = fixture("polkadot-asset-hub-19000001.json");
        let mut env: RawBlockEnvelope = serde_json::from_slice(&bytes).unwrap();
        env.extrinsics[1].scale_hex = "0xnot-hex".into();
        let bad = serde_json::to_vec(&env).unwrap();
        assert!(matches!(
            decode_block(&bad, "loc"),
            Err(DecodeError::BadHex { index: 1, .. })
        ));
    }

    #[test]
    fn decode_is_deterministic() {
        let bytes = fixture("polkadot-asset-hub-19000001.json");
        let a = decode_block(&bytes, "loc").unwrap();
        let b = decode_block(&bytes, "loc").unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn garbage_is_a_decode_error_not_a_panic() {
        assert!(decode_block(b"not json", "loc").is_err());
    }
}
