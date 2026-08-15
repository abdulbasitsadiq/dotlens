//! System-account derivation + labeling primitives (ECOSYSTEM.md §6).
//!
//! Pure — no I/O, no network. All Substrate-protocol knowledge about how
//! system accounts derive lives HERE (Invariant 4: adapters are the only
//! place protocol specifics live). The labeling engine in dotlens-node walks
//! the registry and calls these; it never hardcodes an address.
//!
//! Derivations (all zero-padded to 32 bytes):
//!   pallet account     b"modl" ++ pallet_id(8B)
//!   para sovereign     b"para" ++ u32_le(para_id)   (valid on the RELAY)
//!   sibling sovereign  b"sibl" ++ u32_le(para_id)   (valid on SIBLING parachains)
//!
//! Golden vectors (verified independently against ECOSYSTEM.md §6):
//!   modl py/trsry → 13UVJyLnbVp9RBZYFwFGyDvVd1y27Tt8tkntv6Q7JVPhFsTB
//!   modl py/feltr → 13UVJyLnbVp7X3BCpfkkg8hf1bLAEtCHpnyPBbTPZMyaReQR
//!   para 1000     → 13YMK2edbuhwMBxeUWm9c643A2wyYHwSVh1bCM7tShtg7Dtk

use blake2::digest::consts::{U16, U64 as BlakeU64};
use blake2::digest::Digest;
use frame_metadata::{RuntimeMetadata, RuntimeMetadataPrefixed};
use parity_scale_codec::Decode;

use crate::frame_decoder::ss58_encode;

fn padded(prefix: &[u8; 4], body: &[u8]) -> [u8; 32] {
    debug_assert!(body.len() <= 28);
    let mut out = [0u8; 32];
    out[..4].copy_from_slice(prefix);
    out[4..4 + body.len()].copy_from_slice(body);
    out
}

/// `b"modl" ++ pallet_id`, zero-padded. The address of e.g. Treasury (py/trsry).
pub fn pallet_account(pallet_id: &[u8; 8]) -> [u8; 32] {
    padded(b"modl", pallet_id)
}

/// `b"para" ++ u32_le(para_id)`, zero-padded — the parachain's sovereign
/// account ON THE RELAY chain.
pub fn para_sovereign(para_id: u32) -> [u8; 32] {
    padded(b"para", &para_id.to_le_bytes())
}

/// `b"sibl" ++ u32_le(para_id)`, zero-padded — the parachain's sovereign
/// account ON A SIBLING parachain (same address on every sibling).
pub fn sibling_sovereign(para_id: u32) -> [u8; 32] {
    padded(b"sibl", &para_id.to_le_bytes())
}

/// Human form of an 8-byte pallet id ("py/trsry"). Trailing NUL/space padding
/// trimmed; non-ASCII bytes escaped so the result is always printable.
pub fn ascii_pallet_id(id: &[u8; 8]) -> String {
    let trimmed: &[u8] = {
        let mut end = id.len();
        while end > 0 && (id[end - 1] == 0 || id[end - 1] == b' ') {
            end -= 1;
        }
        &id[..end]
    };
    trimmed
        .iter()
        .map(|&b| {
            if b.is_ascii_graphic() || b == b' ' {
                (b as char).to_string()
            } else {
                format!("\\x{b:02x}")
            }
        })
        .collect()
}

// ---------------------------------------------------------------- ss58 decode

/// Inverse of `ss58_encode` (frame_decoder): base58 → (prefix, 32-byte account),
/// checksum-verified. Rejects the mangled-address failure mode ECOSYSTEM.md §6
/// warns about (corrupted variants fail here, never round-trip silently).
pub fn ss58_decode(s: &str) -> Result<(u16, [u8; 32]), String> {
    let data = bs58::decode(s)
        .into_vec()
        .map_err(|e| format!("base58: {e}"))?;
    let (prefix, body_start) = match data.first() {
        Some(&b) if b < 64 => (b as u16, 1usize),
        Some(&b) if b < 128 => {
            let b2 = *data.get(1).ok_or("truncated ss58 prefix")?;
            // inverse of the two-byte packing in ss58_encode
            let lower = (((b as u16) & 0x3F) << 2) | ((b2 as u16) >> 6);
            (lower | (((b2 as u16) & 0x3F) << 8), 2usize)
        }
        _ => return Err("invalid ss58 prefix byte".into()),
    };
    if data.len() != body_start + 32 + 2 {
        return Err(format!(
            "unexpected ss58 payload length {} (32-byte accounts only)",
            data.len()
        ));
    }
    let (payload, checksum) = data.split_at(data.len() - 2);
    let mut hasher = blake2::Blake2b::<BlakeU64>::new();
    hasher.update(b"SS58PRE");
    hasher.update(payload);
    let expect = hasher.finalize();
    if checksum != &expect[0..2] {
        return Err("ss58 checksum mismatch (corrupted address)".into());
    }
    let mut account = [0u8; 32];
    account.copy_from_slice(&payload[body_start..]);
    Ok((prefix, account))
}

/// Parse an account reference as either 0x-prefixed 32-byte hex or SS58.
/// The API's account parser (family-encoded strings stay an adapter concern).
pub fn parse_account(s: &str) -> Result<[u8; 32], String> {
    if let Some(hexpart) = s.strip_prefix("0x") {
        let bytes = hex::decode(hexpart).map_err(|e| format!("hex: {e}"))?;
        return <[u8; 32]>::try_from(bytes.as_slice())
            .map_err(|_| format!("expected 32 bytes, got {}", bytes.len()));
    }
    ss58_decode(s).map(|(_, account)| account)
}

// ------------------------------------------------------------- storage probes

/// twox128("System") ++ twox128("Account") — the System.Account map prefix.
/// First half matches SYSTEM_EVENTS_KEY (source.rs), verified live in slice 2;
/// both halves re-verified against reference xxhash at authoring time.
pub const SYSTEM_ACCOUNT_PREFIX: [u8; 32] = [
    0x26, 0xaa, 0x39, 0x4e, 0xea, 0x56, 0x30, 0xe0, 0x7c, 0x48, 0xae, 0x0c, 0x95, 0x58, 0xce,
    0xf7, 0xb9, 0x9d, 0x88, 0x0e, 0xc6, 0x81, 0x79, 0x9c, 0x0c, 0xf3, 0x0e, 0x88, 0x86, 0x37,
    0x1d, 0xa9,
];

/// Full System.Account storage key for one account: prefix ++
/// blake2_128_concat(account) — the on-chain existence probe for verify-labels.
pub fn system_account_key(account: &[u8; 32]) -> Vec<u8> {
    let mut key = Vec::with_capacity(32 + 16 + 32);
    key.extend_from_slice(&SYSTEM_ACCOUNT_PREFIX);
    let mut hasher = blake2::Blake2b::<U16>::new();
    hasher.update(account);
    key.extend_from_slice(&hasher.finalize());
    key.extend_from_slice(account);
    key
}

// ----------------------------------------------- pallet ids out of metadata

/// A pallet that declared a `frame_support::PalletId` constant — i.e. one that
/// owns a derivable `modl` account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PalletIdConstant {
    /// Pallet name from metadata, e.g. "Treasury".
    pub pallet: String,
    /// The raw 8-byte id, e.g. b"py/trsry".
    pub id: [u8; 8],
}

/// Walk a metadata blob's pallet constants and return every
/// `frame_support::PalletId` found. This is what makes pallet-account labeling
/// DATA-driven: the runtime itself declares which pallets own accounts —
/// nothing is hand-maintained (ECOSYSTEM.md §6: "generate, don't curate").
///
/// Detection is by resolved type path ending in `PalletId`; the constant value
/// is the SCALE encoding of `PalletId([u8; 8])` = the raw 8 bytes.
pub fn pallet_ids_from_metadata(blob: &[u8]) -> Result<Vec<PalletIdConstant>, String> {
    let prefixed = RuntimeMetadataPrefixed::decode(&mut &blob[..])
        .map_err(|e| format!("metadata blob undecodable: {e}"))?;

    macro_rules! extract {
        ($m:expr) => {{
            let mut out = Vec::new();
            for pallet in &$m.pallets {
                for c in &pallet.constants {
                    let Some(ty) = $m.types.resolve(c.ty.id) else { continue };
                    if ty.path.segments.last().map(String::as_str) != Some("PalletId") {
                        continue;
                    }
                    if c.value.len() != 8 {
                        return Err(format!(
                            "pallet {}: PalletId constant is {} bytes, expected 8",
                            pallet.name,
                            c.value.len()
                        ));
                    }
                    let mut id = [0u8; 8];
                    id.copy_from_slice(&c.value);
                    out.push(PalletIdConstant {
                        pallet: pallet.name.clone(),
                        id,
                    });
                }
            }
            out
        }};
    }

    let mut found = match &prefixed.1 {
        RuntimeMetadata::V14(m) => extract!(m),
        RuntimeMetadata::V15(m) => extract!(m),
        // v16 constants carry extra deprecation info; the fields we read are
        // the same. If field names moved, this arm is the suspect (API:).
        RuntimeMetadata::V16(m) => extract!(m),
        other => {
            return Err(format!(
                "unsupported metadata version (v14/v15/v16 only): {:?}",
                std::mem::discriminant(other)
            ))
        }
    };
    found.sort_by(|a, b| a.pallet.cmp(&b.pallet));
    found.dedup();
    Ok(found)
}

/// Label text for a derived pallet account — the exit-criterion format:
/// "Treasury (py/trsry)".
pub fn pallet_label(c: &PalletIdConstant) -> String {
    format!("{} ({})", c.pallet, ascii_pallet_id(&c.id))
}

/// Convenience: derived account + its SS58 under `prefix`.
pub fn account_with_ss58(account: [u8; 32], prefix: u16) -> ([u8; 32], String) {
    let ss58 = ss58_encode(prefix, &account);
    (account, ss58)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- golden vectors from ECOSYSTEM.md §6, independently re-derived at
    // authoring time (python blake2/base58) — these pin the derivation forever.

    #[test]
    fn treasury_pallet_account_matches_ecosystem_golden_vector() {
        let acct = pallet_account(b"py/trsry");
        assert_eq!(
            ss58_encode(0, &acct),
            "13UVJyLnbVp9RBZYFwFGyDvVd1y27Tt8tkntv6Q7JVPhFsTB"
        );
        assert_eq!(
            hex::encode(acct),
            "6d6f646c70792f74727372790000000000000000000000000000000000000000"
        );
    }

    #[test]
    fn fellowship_pallet_account_matches_ecosystem_golden_vector() {
        let acct = pallet_account(b"py/feltr");
        assert_eq!(
            ss58_encode(0, &acct),
            "13UVJyLnbVp7X3BCpfkkg8hf1bLAEtCHpnyPBbTPZMyaReQR"
        );
    }

    #[test]
    fn asset_hub_para_sovereign_matches_ecosystem_golden_vector() {
        let acct = para_sovereign(1000);
        assert_eq!(
            ss58_encode(0, &acct),
            "13YMK2edbuhwMBxeUWm9c643A2wyYHwSVh1bCM7tShtg7Dtk"
        );
    }

    #[test]
    fn sibling_sovereign_derives_and_roundtrips() {
        // self-derived vector (derivation fn validated by the goldens above)
        let acct = sibling_sovereign(1000);
        let ss58 = ss58_encode(0, &acct);
        assert_eq!(ss58, "13cKp89SgdtqUngo2WiEijPrQWdHFhzYZLf2TJePKRvExk7o");
        assert_eq!(ss58_decode(&ss58).unwrap(), (0, acct));
    }

    #[test]
    fn ss58_decode_rejects_corruption() {
        let good = ss58_encode(0, &pallet_account(b"py/trsry"));
        // flip one character → checksum must fail (the ECOSYSTEM.md mangled-
        // address trap: corrupted addresses never decode silently)
        let mut chars: Vec<char> = good.chars().collect();
        let i = chars.len() / 2;
        chars[i] = if chars[i] == '9' { '8' } else { '9' };
        let bad: String = chars.into_iter().collect();
        assert!(ss58_decode(&bad).is_err());
        assert!(ss58_decode("").is_err());
        assert!(ss58_decode("0x00").is_err());
    }

    #[test]
    fn ss58_two_byte_prefix_roundtrips() {
        let acct = pallet_account(b"py/trsry");
        for prefix in [0u16, 2, 42, 63, 64, 255, 2034, 16383] {
            let s = ss58_encode(prefix, &acct);
            assert_eq!(ss58_decode(&s).unwrap(), (prefix, acct), "prefix {prefix}");
        }
    }

    #[test]
    fn parse_account_accepts_hex_and_ss58() {
        let acct = para_sovereign(1000);
        let hexs = format!("0x{}", hex::encode(acct));
        assert_eq!(parse_account(&hexs).unwrap(), acct);
        assert_eq!(parse_account(&ss58_encode(0, &acct)).unwrap(), acct);
        assert!(parse_account("0xdeadbeef").is_err());
        assert!(parse_account("not-an-address").is_err());
    }

    #[test]
    fn system_account_key_is_prefix_hash_concat() {
        let acct = pallet_account(b"py/trsry");
        let key = system_account_key(&acct);
        assert_eq!(key.len(), 32 + 16 + 32);
        assert_eq!(&key[..32], &SYSTEM_ACCOUNT_PREFIX);
        // blake2_128 of the account, independently computed at authoring time
        assert_eq!(
            hex::encode(&key[32..48]),
            "5ecffd7b6c0f78751baa9d281e0bfa3a"
        );
        assert_eq!(&key[48..], &acct);
    }

    #[test]
    fn ascii_pallet_id_trims_and_escapes() {
        assert_eq!(ascii_pallet_id(b"py/trsry"), "py/trsry");
        assert_eq!(ascii_pallet_id(b"aa\x00\x00\x00\x00\x00\x00"), "aa");
        assert_eq!(ascii_pallet_id(b"a\x01b\x00\x00\x00\x00\x00"), "a\\x01b");
    }

    #[test]
    fn real_metadata_yields_treasury_pallet_id() {
        // uses the committed real AH fixture (slice 3); skips loudly if absent
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/real/polkadot-asset-hub-19498783/metadata.scale");
        let Ok(blob) = std::fs::read(&path) else {
            eprintln!("SKIP: real fixture metadata not present at {}", path.display());
            return;
        };
        let ids = pallet_ids_from_metadata(&blob).expect("metadata walks");
        assert!(!ids.is_empty(), "AH runtime declares PalletId constants");
        let treasury = ids
            .iter()
            .find(|c| c.id == *b"py/trsry")
            .expect("treasury PalletId present on AH (post-migration home)");
        assert_eq!(pallet_label(treasury), format!("{} (py/trsry)", treasury.pallet));
    }
}
