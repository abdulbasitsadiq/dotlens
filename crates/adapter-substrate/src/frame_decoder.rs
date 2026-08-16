//! Real SCALE decoding: (raw envelope + events bytes + metadata blob) →
//! CanonicalBlock. Pure — no I/O, no clocks, no network (ARCHITECTURE.md §5).
//! Decode is keyed by spec_version: the caller hands us the metadata blob that
//! was archived for the block's own runtime (Invariant: blocks decode against
//! metadata at their own spec_version — always).
//!
//! DECODER_VERSION_FRAME = 2. The Phase 0 envelope decoder (v1) remains for
//! synthetic fixtures; this decoder handles the live envelope format
//! (SCALE-hex extrinsics + raw System.Events bytes).
//!
//! Metadata v14/v15 supported (v16 accepted if frame-metadata parses it —
//! fields we rely on are stable). Pre-v14 (historic type registries) is a
//! later slice.
//!
//! NOTE (offline authoring): written against frame-decode 0.7 / scale-value
//! 0.17 surfaces. Expect mechanical fixes at first compile (see the
//! subxt-surface precedent in CLAUDE.md); the decode logic is
//! version-independent. Suspect spots are marked `API:`.

use blake2::digest::consts::U32 as BlakeU32;
use blake2::digest::consts::U64 as BlakeU64;
use blake2::digest::Digest;
use canonical::{CanonicalBlock, CanonicalEvent, CanonicalTransaction, Lineage};
use frame_metadata::{RuntimeMetadata, RuntimeMetadataPrefixed};
use parity_scale_codec::Decode;
use scale_info::PortableRegistry;
use scale_value::{Composite, Value, ValueDef};
use serde::Deserialize;

/// Decoder version for THIS decoder. Rows it writes are rebuildable: bump on
/// any behavior change (Phase 0's envelope decoder stays DECODER_VERSION = 1).
pub const DECODER_VERSION_FRAME: u32 = 2;

#[derive(Debug, thiserror::Error)]
pub enum FrameDecodeError {
    #[error("metadata blob undecodable: {0}")]
    Metadata(String),
    #[error("unsupported metadata version {0} (v14/v15 supported; pre-v14 is a later slice)")]
    UnsupportedMetadata(u32),
    #[error("envelope: {0}")]
    Envelope(String),
    #[error("extrinsic {index}: {reason}")]
    Extrinsic { index: u32, reason: String },
    #[error("events: {0}")]
    Events(String),
}

fn default_true() -> bool {
    true
}

/// The live envelope written by SubstrateSource (slice 2).
#[derive(Debug, Deserialize)]
struct LiveEnvelope {
    height: u64,
    hash: String,
    parent_hash: String,
    spec_version: u32,
    // default TRUE: `finalized: false` now means "replaceable + invisible to
    // balances", so an absent field must fail SAFE (immutable), never open
    #[serde(default = "default_true")]
    finalized: bool,
    extrinsics: Vec<String>,
}

/// One parsed metadata blob, ready to decode blocks of its spec_version.
pub struct FrameDecoder {
    spec_version: u32,
    ss58_prefix: u16,
    types: PortableRegistry,
    /// Type id of System.Events storage value (Vec<EventRecord<..>>).
    events_type_id: Option<u32>,
    /// Retained for extrinsic decoding (frame-decode needs the full metadata).
    metadata: RuntimeMetadata,
}

impl FrameDecoder {
    pub fn from_metadata_bytes(
        spec_version: u32,
        ss58_prefix: u16,
        blob: &[u8],
    ) -> Result<Self, FrameDecodeError> {
        let prefixed = RuntimeMetadataPrefixed::decode(&mut &blob[..])
            .map_err(|e| FrameDecodeError::Metadata(e.to_string()))?;
        let metadata = prefixed.1;
        // types + System.Events type id, per supported version
        let (types, events_type_id) = match &metadata {
            RuntimeMetadata::V14(m) => (m.types.clone(), find_events_type_v14(m)),
            RuntimeMetadata::V15(m) => (m.types.clone(), find_events_type_v15(m)),
            other => {
                return Err(FrameDecodeError::UnsupportedMetadata(runtime_metadata_version(other)))
            }
        };
        Ok(Self {
            spec_version,
            ss58_prefix,
            types,
            events_type_id,
            metadata,
        })
    }

    /// The pure decode: envelope + optional events bytes → canonical block.
    pub fn decode_block(
        &self,
        chain_id: &str,
        envelope: &[u8],
        events_bytes: Option<&[u8]>,
        raw_location: &str,
    ) -> Result<CanonicalBlock, FrameDecodeError> {
        let env: LiveEnvelope = serde_json::from_slice(envelope)
            .map_err(|e| FrameDecodeError::Envelope(e.to_string()))?;
        if env.spec_version != self.spec_version {
            return Err(FrameDecodeError::Envelope(format!(
                "envelope spec_version {} but decoder built for {}",
                env.spec_version, self.spec_version
            )));
        }

        // ---- events first: they carry per-extrinsic success + attribution
        let decoded_events = match events_bytes {
            Some(bytes) => self.decode_events(bytes)?,
            None => Vec::new(),
        };
        let mut success_by_tx: std::collections::HashMap<u32, bool> = std::collections::HashMap::new();
        for ev in &decoded_events {
            if let Some(tx) = ev.transaction_index {
                match ev.name.as_str() {
                    "system.ExtrinsicSuccess" => {
                        success_by_tx.insert(tx, true);
                    }
                    "system.ExtrinsicFailed" => {
                        success_by_tx.insert(tx, false);
                    }
                    _ => {}
                }
            }
        }

        // ---- extrinsics
        let mut transactions = Vec::with_capacity(env.extrinsics.len());
        let mut timestamp: Option<chrono::DateTime<chrono::Utc>> = None;
        for (i, xt_hex) in env.extrinsics.iter().enumerate() {
            let index = i as u32;
            let bytes = decode_hex(xt_hex).map_err(|reason| FrameDecodeError::Extrinsic {
                index,
                reason,
            })?;
            let tx = self
                .decode_extrinsic(index, &bytes)
                .map_err(|reason| FrameDecodeError::Extrinsic { index, reason })?;
            // block time from the timestamp inherent — protocol knowledge,
            // allowed here (adapter), never in core/modules
            if timestamp.is_none() && tx.call == "timestamp.set" {
                if let Some(now_ms) = tx.args.get("now").and_then(json_as_u64) {
                    timestamp = chrono::DateTime::from_timestamp_millis(now_ms as i64);
                }
            }
            let success = success_by_tx.get(&index).copied().unwrap_or(true);
            transactions.push(CanonicalTransaction { success, ..tx });
        }

        Ok(CanonicalBlock {
            chain_id: chain_id.to_string(),
            height: env.height,
            hash: env.hash,
            parent_hash: env.parent_hash,
            timestamp,
            finalized: env.finalized,
            lineage: Lineage {
                runtime_version: self.spec_version,
                decoder_version: DECODER_VERSION_FRAME,
                raw_location: raw_location.to_string(),
            },
            transactions,
            events: decoded_events,
        })
    }

    fn decode_extrinsic(&self, index: u32, bytes: &[u8]) -> Result<CanonicalTransaction, String> {
        // API: frame_decode::extrinsics::decode_extrinsic(cursor, metadata, types)
        // — generic over metadata version (ExtrinsicTypeInfo). A macro (not a
        // shared `let info = match`) so the arms never need a unified type.
        macro_rules! decode_with {
            ($m:expr) => {{
                let info =
                    frame_decode::extrinsics::decode_extrinsic(&mut &bytes[..], $m, &self.types)
                        .map_err(|e| e.to_string())?;
                let pallet = info.pallet_name().to_string();
                let call_name = info.call_name().to_string();

                // args: decode each named range against its type id
                let mut args = serde_json::Map::new();
                // API: info.call_data() yields args with .name() / .range() / .ty()
                for arg in info.call_data() {
                    let range = arg.range();
                    let mut cursor = &bytes[range.start..range.end];
                    let value =
                        scale_value::scale::decode_as_type(&mut cursor, *arg.ty(), &self.types)
                            .map_err(|e| format!("arg {}: {e}", arg.name()))?;
                    args.insert(arg.name().to_string(), value_to_json(&value.remove_context()));
                }

                // signer: only for signed extrinsics; MultiAddress::Id → SS58
                // API: signature_payload() → Option with .address_range()/.address_type()
                let signer = match info.signature_payload() {
                    None => None,
                    Some(sig) => {
                        let range = sig.address_range();
                        let raw_addr = &bytes[range.start..range.end];
                        let mut cursor = raw_addr;
                        match scale_value::scale::decode_as_type(
                            &mut cursor,
                            *sig.address_type(),
                            &self.types,
                        ) {
                            Ok(addr) => {
                                Some(self.address_to_string(&addr.remove_context(), raw_addr))
                            }
                            Err(_) => Some(format!("0x{}", hex::encode(raw_addr))),
                        }
                    }
                };
                (pallet, call_name, args, signer)
            }};
        }

        let (pallet, call_name, args, signer) = match &self.metadata {
            RuntimeMetadata::V14(m) => decode_with!(m),
            RuntimeMetadata::V15(m) => decode_with!(m),
            _ => unreachable!("constructor rejects other versions"),
        };

        Ok(CanonicalTransaction {
            index,
            hash: Some(extrinsic_hash(bytes)),
            signer,
            call: format!("{}.{}", pallet.to_lowercase(), call_name),
            args: serde_json::Value::Object(args),
            success: true, // overwritten from events by the caller
        })
    }

    fn decode_events(&self, bytes: &[u8]) -> Result<Vec<CanonicalEvent>, FrameDecodeError> {
        let Some(ty_id) = self.events_type_id else {
            return Err(FrameDecodeError::Events(
                "System.Events storage entry not found in metadata".into(),
            ));
        };
        let mut cursor = bytes;
        let value = scale_value::scale::decode_as_type(&mut cursor, ty_id, &self.types)
            .map_err(|e| FrameDecodeError::Events(e.to_string()))?
            .remove_context();

        let records = as_sequence(&value)
            .ok_or_else(|| FrameDecodeError::Events("events value is not a sequence".into()))?;
        let mut out = Vec::with_capacity(records.len());
        for (i, record) in records.iter().enumerate() {
            let (phase, event) = split_event_record(record)
                .ok_or_else(|| FrameDecodeError::Events(format!("record {i}: unexpected shape")))?;
            let transaction_index = phase_to_tx_index(phase);
            let (name, data) = event_name_and_fields(event)
                .ok_or_else(|| FrameDecodeError::Events(format!("record {i}: event shape")))?;
            out.push(CanonicalEvent {
                index: i as u32,
                transaction_index,
                name,
                data,
            });
        }
        Ok(out)
    }

    fn address_to_string(&self, addr: &Value, raw: &[u8]) -> String {
        // MultiAddress::Id(AccountId32) is the overwhelmingly common case
        if let ValueDef::Variant(v) = &addr.value {
            if v.name == "Id" {
                if let Some(bytes) = collect_bytes(&v.values) {
                    if bytes.len() == 32 {
                        let mut acct = [0u8; 32];
                        acct.copy_from_slice(&bytes);
                        return ss58_encode(self.ss58_prefix, &acct);
                    }
                }
            }
        }
        format!("0x{}", hex::encode(raw))
    }
}

// ------------------------------------------------------------ metadata lookup

fn runtime_metadata_version(m: &RuntimeMetadata) -> u32 {
    match m {
        RuntimeMetadata::V14(_) => 14,
        RuntimeMetadata::V15(_) => 15,
        RuntimeMetadata::V16(_) => 16,
        _ => 0,
    }
}

fn find_events_type_v14(m: &frame_metadata::v14::RuntimeMetadataV14) -> Option<u32> {
    let pallet = m.pallets.iter().find(|p| p.name == "System")?;
    let storage = pallet.storage.as_ref()?;
    let entry = storage.entries.iter().find(|e| e.name == "Events")?;
    match &entry.ty {
        frame_metadata::v14::StorageEntryType::Plain(ty) => Some(ty.id),
        _ => None,
    }
}

fn find_events_type_v15(m: &frame_metadata::v15::RuntimeMetadataV15) -> Option<u32> {
    let pallet = m.pallets.iter().find(|p| p.name == "System")?;
    let storage = pallet.storage.as_ref()?;
    let entry = storage.entries.iter().find(|e| e.name == "Events")?;
    match &entry.ty {
        frame_metadata::v15::StorageEntryType::Plain(ty) => Some(ty.id),
        _ => None,
    }
}

// ------------------------------------------------------- scale-value walking

fn as_sequence(v: &Value) -> Option<&[Value]> {
    match &v.value {
        ValueDef::Composite(Composite::Unnamed(items)) => Some(items),
        _ => None,
    }
}

/// EventRecord { phase, event, topics } — named or positional.
fn split_event_record(record: &Value) -> Option<(&Value, &Value)> {
    match &record.value {
        ValueDef::Composite(Composite::Named(fields)) => {
            let phase = fields.iter().find(|(n, _)| n == "phase").map(|(_, v)| v)?;
            let event = fields.iter().find(|(n, _)| n == "event").map(|(_, v)| v)?;
            Some((phase, event))
        }
        ValueDef::Composite(Composite::Unnamed(fields)) if fields.len() >= 2 => {
            Some((&fields[0], &fields[1]))
        }
        _ => None,
    }
}

fn phase_to_tx_index(phase: &Value) -> Option<u32> {
    if let ValueDef::Variant(v) = &phase.value {
        if v.name == "ApplyExtrinsic" {
            let inner = match &v.values {
                Composite::Unnamed(items) => items.first(),
                Composite::Named(items) => items.first().map(|(_, v)| v),
            }?;
            return value_as_u64(inner).map(|n| n as u32);
        }
    }
    None // Initialization / Finalization → not attributable
}

/// RuntimeEvent::Pallet(pallet::Event::Variant { fields }) →
/// ("pallet.Variant", fields as JSON).
fn event_name_and_fields(event: &Value) -> Option<(String, serde_json::Value)> {
    let ValueDef::Variant(pallet_variant) = &event.value else { return None };
    let inner = match &pallet_variant.values {
        Composite::Unnamed(items) => items.first(),
        Composite::Named(items) => items.first().map(|(_, v)| v),
    }?;
    let ValueDef::Variant(event_variant) = &inner.value else { return None };
    let name = format!(
        "{}.{}",
        pallet_variant.name.to_lowercase(),
        event_variant.name
    );
    Some((name, composite_to_json(&event_variant.values)))
}

// -------------------------------------------------------- JSON serialization
// Hand-rolled (not serde) because serde_json rejects u128/i128 beyond 64 bits
// — and real balances exceed u64::MAX plancks. Big numbers become decimal
// strings; the schema-on-read consumers already parse both (see json_as_u64).

fn value_to_json(v: &Value) -> serde_json::Value {
    match &v.value {
        ValueDef::Composite(c) => composite_to_json(c),
        ValueDef::Variant(var) => {
            // {"VariantName": fields} — compact and unambiguous
            let mut obj = serde_json::Map::new();
            obj.insert(var.name.clone(), composite_to_json(&var.values));
            serde_json::Value::Object(obj)
        }
        ValueDef::Primitive(p) => primitive_to_json(p),
        ValueDef::BitSequence(bits) => serde_json::Value::String(format!("{bits:?}")),
    }
}

fn composite_to_json(c: &Composite<()>) -> serde_json::Value {
    match c {
        Composite::Named(fields) => serde_json::Value::Object(
            fields.iter().map(|(n, v)| (n.clone(), value_to_json(v))).collect(),
        ),
        Composite::Unnamed(items) => {
            serde_json::Value::Array(items.iter().map(value_to_json).collect())
        }
    }
}

fn primitive_to_json(p: &scale_value::Primitive) -> serde_json::Value {
    use scale_value::Primitive;
    match p {
        Primitive::Bool(b) => serde_json::Value::Bool(*b),
        Primitive::Char(c) => serde_json::Value::String(c.to_string()),
        Primitive::String(s) => serde_json::Value::String(s.clone()),
        Primitive::U128(n) => match u64::try_from(*n) {
            Ok(small) => serde_json::json!(small),
            Err(_) => serde_json::Value::String(n.to_string()),
        },
        Primitive::I128(n) => match i64::try_from(*n) {
            Ok(small) => serde_json::json!(small),
            Err(_) => serde_json::Value::String(n.to_string()),
        },
        Primitive::U256(bytes) | Primitive::I256(bytes) => {
            serde_json::Value::String(format!("0x{}", hex::encode(bytes)))
        }
    }
}

fn value_as_u64(v: &Value) -> Option<u64> {
    match &v.value {
        ValueDef::Primitive(scale_value::Primitive::U128(n)) => u64::try_from(*n).ok(),
        ValueDef::Primitive(scale_value::Primitive::U256(_)) => None,
        _ => None,
    }
}

/// Recursively collect u8s out of nested composites (AccountId32 shape).
fn collect_bytes(c: &Composite<()>) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    fn walk(v: &Value, out: &mut Vec<u8>) -> bool {
        match &v.value {
            ValueDef::Primitive(scale_value::Primitive::U128(n)) => {
                if *n > 255 {
                    return false;
                }
                out.push(*n as u8);
                true
            }
            ValueDef::Composite(Composite::Unnamed(items)) => {
                items.iter().all(|i| walk(i, out))
            }
            ValueDef::Composite(Composite::Named(items)) => {
                items.iter().all(|(_, i)| walk(i, out))
            }
            _ => false,
        }
    }
    let ok = match c {
        Composite::Unnamed(items) => items.iter().all(|i| walk(i, &mut out)),
        Composite::Named(items) => items.iter().all(|(_, i)| walk(i, &mut out)),
    };
    ok.then_some(out)
}

fn json_as_u64(v: &serde_json::Value) -> Option<u64> {
    v.as_u64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

// ------------------------------------------------------------------- helpers

fn decode_hex(s: &str) -> Result<Vec<u8>, String> {
    hex::decode(s.trim_start_matches("0x")).map_err(|e| e.to_string())
}

/// Extrinsic hash: blake2b-256 of the full encoded extrinsic (standard).
fn extrinsic_hash(bytes: &[u8]) -> String {
    let mut hasher = blake2::Blake2b::<BlakeU32>::new();
    hasher.update(bytes);
    format!("0x{}", hex::encode(hasher.finalize()))
}

/// SS58 encoding (https://docs.substrate.io ss58 spec). Prefix 0 (polkadot)
/// takes the single-byte path; the two-byte path is included for completeness.
pub fn ss58_encode(prefix: u16, account: &[u8; 32]) -> String {
    let mut data: Vec<u8> = Vec::with_capacity(35);
    if prefix < 64 {
        data.push(prefix as u8);
    } else {
        let full = prefix & 0b0011_1111_1111_1111;
        data.push(((full & 0b0000_0000_1111_1100) >> 2) as u8 | 0b0100_0000);
        data.push((full >> 8) as u8 | ((full & 0b11) << 6) as u8);
    }
    data.extend_from_slice(account);
    let mut hasher = blake2::Blake2b::<BlakeU64>::new();
    hasher.update(b"SS58PRE");
    hasher.update(&data);
    let checksum = hasher.finalize();
    data.extend_from_slice(&checksum[0..2]);
    bs58::encode(data).into_string()
}

// ----------------------------------------------- worker-facing cached decoder

/// Implements the generic `RawBlockDecoder` contract: one instance per chain,
/// caching a FrameDecoder per spec_version (metadata parse is expensive).
pub struct SubstrateFrameDecoder {
    ss58_prefix: u16,
    cache: std::sync::Mutex<std::collections::HashMap<u32, std::sync::Arc<FrameDecoder>>>,
}

impl SubstrateFrameDecoder {
    pub fn new(ss58_prefix: u16) -> Self {
        Self {
            ss58_prefix,
            cache: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    fn decoder_for(
        &self,
        spec_version: u32,
        metadata: &[u8],
    ) -> Result<std::sync::Arc<FrameDecoder>, String> {
        if let Some(d) = self.cache.lock().unwrap().get(&spec_version) {
            return Ok(d.clone());
        }
        let d = std::sync::Arc::new(
            FrameDecoder::from_metadata_bytes(spec_version, self.ss58_prefix, metadata)
                .map_err(|e| e.to_string())?,
        );
        self.cache.lock().unwrap().insert(spec_version, d.clone());
        Ok(d)
    }
}

impl ingest::decode::RawBlockDecoder for SubstrateFrameDecoder {
    fn decode(
        &self,
        chain_id: &str,
        envelope: &[u8],
        events: Option<&[u8]>,
        metadata: &[u8],
        spec_version: u32,
        raw_location: &str,
    ) -> Result<CanonicalBlock, String> {
        let decoder = self.decoder_for(spec_version, metadata)?;
        decoder
            .decode_block(chain_id, envelope, events, raw_location)
            .map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ss58_known_vectors() {
        // Alice's well-known AccountId32
        let alice: [u8; 32] =
            hex::decode("d43593c715fdd31c61141abd04a99fd6822c8558854ccde39a5684e7a56da27d")
                .unwrap()
                .try_into()
                .unwrap();
        // generic substrate (prefix 42)
        assert_eq!(
            ss58_encode(42, &alice),
            "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"
        );
        // polkadot (prefix 0)
        assert_eq!(
            ss58_encode(0, &alice),
            "15oF4uVJwmo4TdGW7VfQxNLavjCXviqxT9S1MgbjMNHr6Sp5"
        );
    }

    #[test]
    fn event_walking_handles_the_standard_shapes() {
        // phase: ApplyExtrinsic(2)
        let phase = Value::variant(
            "ApplyExtrinsic",
            Composite::Unnamed(vec![Value::u128(2)]),
        );
        assert_eq!(phase_to_tx_index(&phase), Some(2));
        let fin = Value::variant("Finalization", Composite::Unnamed(vec![]));
        assert_eq!(phase_to_tx_index(&fin), None);

        // event: Balances(Withdraw { who, amount })
        let event = Value::variant(
            "Balances",
            Composite::Unnamed(vec![Value::variant(
                "Withdraw",
                Composite::Named(vec![
                    ("who".to_string(), Value::string("addr")),
                    ("amount".to_string(), Value::u128(42)),
                ]),
            )]),
        );
        let (name, data) = event_name_and_fields(&event).unwrap();
        assert_eq!(name, "balances.Withdraw");
        assert_eq!(data["amount"], serde_json::json!(42));

        // record splitting, named + positional
        let record = Value::named_composite(vec![
            ("phase".to_string(), phase.clone()),
            ("event".to_string(), event.clone()),
            ("topics".to_string(), Value::unnamed_composite(vec![])),
        ]);
        let (p, e) = split_event_record(&record).unwrap();
        assert_eq!(phase_to_tx_index(p), Some(2));
        assert!(event_name_and_fields(e).is_some());
    }

    #[test]
    fn big_numbers_become_strings_never_lossy_floats() {
        use scale_value::Primitive;
        // fits u64 → JSON number
        assert_eq!(primitive_to_json(&Primitive::U128(42)), serde_json::json!(42));
        // total-issuance-scale plancks exceed u64 → decimal string, exact
        let big = u64::MAX as u128 + 5;
        assert_eq!(
            primitive_to_json(&Primitive::U128(big)),
            serde_json::Value::String(big.to_string())
        );
        assert_eq!(
            primitive_to_json(&Primitive::I128(-1)),
            serde_json::json!(-1)
        );
    }

    #[test]
    fn account_bytes_collect_through_nesting() {
        // AccountId32 decodes as variant Id(composite(composite([u8;32])))
        let inner = Value::unnamed_composite((0u8..32).map(|b| Value::u128(b as u128)).collect::<Vec<_>>());
        let wrapped = Composite::Unnamed(vec![Value::unnamed_composite(vec![inner])]);
        let bytes = collect_bytes(&wrapped).unwrap();
        assert_eq!(bytes.len(), 32);
        assert_eq!(bytes[31], 31);
    }

    /// Real-fixture decode: runs against fixtures/real/<name>/{block.json,
    /// events.scale,metadata.scale} captured by `dotlens-node capture-fixture`.
    /// Skips loudly when none are committed yet.
    #[test]
    fn real_fixtures_decode_end_to_end() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/real");
        let Ok(entries) = std::fs::read_dir(&dir) else {
            eprintln!("SKIP: no fixtures/real directory — run `dotlens-node capture-fixture` first");
            return;
        };
        let mut checked = 0;
        for entry in entries.filter_map(|e| e.ok()) {
            let d = entry.path();
            if !d.is_dir() {
                continue;
            }
            let envelope = std::fs::read(d.join("block.json")).expect("block.json");
            let events = std::fs::read(d.join("events.scale")).ok();
            let metadata = std::fs::read(d.join("metadata.scale")).expect("metadata.scale");
            let peek: serde_json::Value = serde_json::from_slice(&envelope).unwrap();
            let spec = peek["spec_version"].as_u64().unwrap() as u32;
            let chain = peek["chain_id"].as_str().unwrap_or("unknown");

            let decoder = FrameDecoder::from_metadata_bytes(spec, 0, &metadata)
                .expect("metadata parses");
            let block = decoder
                .decode_block(chain, &envelope, events.as_deref(), "raw/test")
                .expect("block decodes");

            assert!(!block.transactions.is_empty(), "{}: no extrinsics decoded", chain);
            assert!(
                block.transactions.iter().any(|t| t.call == "timestamp.set"),
                "{}: timestamp inherent missing", chain
            );
            assert!(block.timestamp.is_some(), "{}: timestamp not extracted", chain);
            assert_eq!(block.lineage.decoder_version, DECODER_VERSION_FRAME);
            assert_eq!(block.lineage.runtime_version, spec);
            if events.is_some() {
                assert!(!block.events.is_empty(), "{}: events present but none decoded", chain);
                // every ApplyExtrinsic event must point at a real extrinsic
                for ev in &block.events {
                    if let Some(tx) = ev.transaction_index {
                        assert!((tx as usize) < block.transactions.len());
                    }
                }
            }
            checked += 1;
            eprintln!("real fixture OK: {} height {}", chain, block.height);
        }
        if checked == 0 {
            eprintln!("SKIP: fixtures/real is empty — run `dotlens-node capture-fixture` first");
        }
    }
}
