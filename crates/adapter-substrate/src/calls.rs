//! RuntimeCall decoding — the "what does this referendum actually DO" engine
//! (Phase 2, slice 2; ECOSYSTEM.md §7: no incumbent does this well).
//!
//! Pure: (metadata blob, call bytes) → a CALL TREE. Nested calls —
//! `utility.batch(Vec<RuntimeCall>)`, `whitelist.dispatch_whitelisted_call_
//! with_preimage(Box<RuntimeCall>)`, scheduler agendas, proxy/sudo wrappers —
//! are detected by TYPE ID, never by pallet-name heuristics: we decode
//! WITHOUT discarding the per-node type annotation, and any node whose type
//! is the runtime's own `RuntimeCall` type becomes a nested tree node. A
//! runtime adding a new wrapping pallet needs ZERO code here.
//!
//! Tree shape (schema-on-read, UI renders it directly):
//!   {"call": "utility.batch", "args": {"calls": [
//!       {"call": "system.remark", "args": {"remark": [104, 105]}},
//!       …
//!   ]}}
//!
//! CALL_DECODER_VERSION is lineage: bump on ANY shape change; rows rebuild
//! from archived preimage bytes.

use crate::frame_decoder::primitive_to_json;
use blake2::digest::consts::U32;
use blake2::digest::Digest;
use frame_metadata::{RuntimeMetadata, RuntimeMetadataPrefixed};
use parity_scale_codec::Decode;
use scale_value::{Composite, Value, ValueDef};

pub const CALL_DECODER_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedCall {
    /// The normalized call tree (see module docs for the shape).
    pub tree: serde_json::Value,
    /// Root "pallet.call" — list-view text.
    pub summary: String,
}

/// blake2b-256 — preimage hashes (verify fetched bytes, hash inline bytes).
pub fn blake2_256(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = blake2::Blake2b::<U32>::new();
    hasher.update(bytes);
    hasher.finalize().into()
}

/// Decode SCALE call bytes against a metadata blob into a call tree.
/// Pure — bytes in, tree out; trailing bytes are an error (a truncated or
/// mis-keyed preimage must never decode silently).
pub fn decode_call(metadata_blob: &[u8], call_bytes: &[u8]) -> Result<DecodedCall, String> {
    let prefixed = RuntimeMetadataPrefixed::decode(&mut &metadata_blob[..])
        .map_err(|e| format!("metadata blob undecodable: {e}"))?;

    let (call_ty, types) = runtime_call_type(&prefixed)?;
    decode_call_with(&types, call_ty, call_bytes)
}

/// Same decode against a registry + call type the caller already resolved.
///
/// The dry-run path uses this: the type it must encode against is the one the
/// `DryRunApi` method DECLARES for its `call` parameter, which is stricter than
/// re-deriving `extrinsic.call_ty` and costs no second metadata decode.
pub fn decode_call_with(
    types: &scale_info::PortableRegistry,
    call_ty: u32,
    call_bytes: &[u8],
) -> Result<DecodedCall, String> {
    let mut cursor = call_bytes;
    let value = scale_value::scale::decode_as_type(&mut cursor, call_ty, types)
        .map_err(|e| format!("RuntimeCall decode: {e}"))?;
    if !cursor.is_empty() {
        return Err(format!(
            "{} trailing bytes after RuntimeCall — truncated or mis-keyed preimage",
            cursor.len()
        ));
    }
    let tree = call_node(&value, call_ty)?;
    let summary = tree["call"]
        .as_str()
        .ok_or("call tree has no root call name")?
        .to_string();
    Ok(DecodedCall { tree, summary })
}

/// The runtime's outer `RuntimeCall` type id + the type registry.
/// v15/v16 declare it directly (`extrinsic.call_ty`); v14 only exposes the
/// UncheckedExtrinsic type, whose `Call` type parameter is the call type.
fn runtime_call_type(
    prefixed: &RuntimeMetadataPrefixed,
) -> Result<(u32, scale_info::PortableRegistry), String> {
    match &prefixed.1 {
        RuntimeMetadata::V14(m) => {
            let extrinsic_ty = m
                .types
                .resolve(m.extrinsic.ty.id)
                .ok_or("v14 extrinsic type not in registry")?;
            let call_param = extrinsic_ty
                .type_params
                .iter()
                .find(|p| p.name == "Call")
                .ok_or("v14 extrinsic type has no Call parameter")?;
            let ty = call_param.ty.ok_or("v14 Call parameter has no type")?;
            Ok((ty.id, m.types.clone()))
        }
        RuntimeMetadata::V15(m) => Ok((m.extrinsic.call_ty.id, m.types.clone())),
        RuntimeMetadata::V16(m) => Ok((m.extrinsic.call_ty.id, m.types.clone())),
        _ => Err("unsupported metadata version (v14/v15/v16 only)".into()),
    }
}

/// One call node: Variant(pallet, [Variant(call, args)]) — the same two-level
/// shape events use. `v.context` must be the RuntimeCall type id.
fn call_node(v: &Value<u32>, call_ty: u32) -> Result<serde_json::Value, String> {
    let ValueDef::Variant(pallet_var) = &v.value else {
        return Err("call is not a pallet variant".into());
    };
    let inner = match &pallet_var.values {
        Composite::Unnamed(items) => items.first(),
        Composite::Named(items) => items.first().map(|(_, v)| v),
    }
    .ok_or_else(|| format!("pallet variant {} has no call", pallet_var.name))?;
    let ValueDef::Variant(call_var) = &inner.value else {
        return Err(format!("{}: inner value is not a call variant", pallet_var.name));
    };
    Ok(serde_json::json!({
        // matches canonical transaction call naming ("balances.transfer_keep_alive")
        "call": format!("{}.{}", pallet_var.name.to_lowercase(), call_var.name),
        "args": composite_to_json(&call_var.values, call_ty)?,
    }))
}

/// value_to_json, context-aware: any node typed as RuntimeCall becomes a
/// nested call node — this is where batch/whitelist/scheduler/proxy nesting
/// unwraps, purely type-driven.
fn value_to_json(v: &Value<u32>, call_ty: u32) -> Result<serde_json::Value, String> {
    if v.context == call_ty {
        return call_node(v, call_ty);
    }
    Ok(match &v.value {
        ValueDef::Composite(c) => composite_to_json(c, call_ty)?,
        ValueDef::Variant(var) => {
            let mut obj = serde_json::Map::new();
            obj.insert(var.name.clone(), composite_to_json(&var.values, call_ty)?);
            serde_json::Value::Object(obj)
        }
        ValueDef::Primitive(p) => primitive_to_json(p),
        ValueDef::BitSequence(bits) => serde_json::Value::String(format!("{bits:?}")),
    })
}

fn composite_to_json(c: &Composite<u32>, call_ty: u32) -> Result<serde_json::Value, String> {
    Ok(match c {
        Composite::Named(fields) => serde_json::Value::Object(
            fields
                .iter()
                .map(|(n, v)| Ok((n.clone(), value_to_json(v, call_ty)?)))
                .collect::<Result<_, String>>()?,
        ),
        Composite::Unnamed(items) => serde_json::Value::Array(
            items
                .iter()
                .map(|v| value_to_json(v, call_ty))
                .collect::<Result<_, String>>()?,
        ),
    })
}

/// Collect raw bytes out of proposal JSON (`{"Inline": […]}` bodies and
/// similar) — arbitrarily nested arrays of byte-sized numbers, any length.
pub fn json_bytes(v: &serde_json::Value) -> Option<Vec<u8>> {
    fn walk(v: &serde_json::Value, out: &mut Vec<u8>) -> bool {
        match v {
            serde_json::Value::Number(n) => match n.as_u64() {
                Some(b) if b <= 255 => {
                    out.push(b as u8);
                    true
                }
                _ => false,
            },
            serde_json::Value::Array(items) => items.iter().all(|i| walk(i, out)),
            serde_json::Value::Object(map) => map.values().all(|i| walk(i, out)),
            _ => false,
        }
    }
    let mut out = Vec::new();
    walk(v, &mut out).then_some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build call bytes AGAINST the fixture metadata itself (pallet + call
    /// variant indices read from the registry, never hardcoded) so the test
    /// exercises a true encode→decode roundtrip on the real runtime.
    struct CallIndex {
        meta: RuntimeMetadataPrefixed,
    }

    impl CallIndex {
        fn load() -> Option<Self> {
            let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../fixtures/real/polkadot-asset-hub-19498783/metadata.scale");
            let blob = std::fs::read(&path).ok()?;
            let meta = RuntimeMetadataPrefixed::decode(&mut &blob[..]).ok()?;
            Some(Self { meta })
        }
        fn blob(&self) -> Vec<u8> {
            use parity_scale_codec::Encode;
            self.meta.encode()
        }
        /// (pallet_index, call_variant_index) for pallet.call, from metadata.
        fn indices(&self, pallet: &str, call: &str) -> (u8, u8) {
            let RuntimeMetadata::V14(m) = &self.meta.1 else {
                panic!("fixture is v14");
            };
            let p = m.pallets.iter().find(|p| p.name == pallet).expect("pallet");
            let calls = p.calls.as_ref().expect("pallet has calls");
            let ty = m.types.resolve(calls.ty.id).expect("call type");
            let scale_info::TypeDef::Variant(var) = &ty.type_def else {
                panic!("call type is a variant");
            };
            let v = var.variants.iter().find(|v| v.name == call).expect("call variant");
            (p.index, v.index)
        }
    }

    fn compact(n: u64) -> Vec<u8> {
        use parity_scale_codec::{Compact, Encode};
        Compact(n).encode()
    }

    #[test]
    fn single_call_decodes_to_a_leaf_node() {
        let Some(ix) = CallIndex::load() else {
            eprintln!("SKIP: real fixture metadata not present");
            return;
        };
        let (sys, remark) = ix.indices("System", "remark");
        // system.remark(b"hi") = [pallet, variant, compact(2), 'h', 'i']
        let mut bytes = vec![sys, remark];
        bytes.extend(compact(2));
        bytes.extend(b"hi");
        let d = decode_call(&ix.blob(), &bytes).expect("decodes");
        assert_eq!(d.summary, "system.remark");
        assert_eq!(d.tree["call"], "system.remark");
        // the remark arg is the raw bytes
        let args = d.tree["args"].as_object().expect("named args");
        assert_eq!(args["remark"], serde_json::json!([104, 105]));
    }

    #[test]
    fn batch_unwraps_into_nested_call_nodes_by_type_id() {
        let Some(ix) = CallIndex::load() else {
            eprintln!("SKIP: real fixture metadata not present");
            return;
        };
        let (sys, remark) = ix.indices("System", "remark");
        let (utility, batch) = ix.indices("Utility", "batch");
        let inner = |body: &[u8]| {
            let mut c = vec![sys, remark];
            c.extend(compact(body.len() as u64));
            c.extend(body);
            c
        };
        // utility.batch([system.remark(b"hi"), system.remark(b"yo")])
        let mut bytes = vec![utility, batch];
        bytes.extend(compact(2)); // Vec<RuntimeCall> length
        bytes.extend(inner(b"hi"));
        bytes.extend(inner(b"yo"));

        let d = decode_call(&ix.blob(), &bytes).expect("decodes");
        assert_eq!(d.summary, "utility.batch");
        let calls = d.tree["args"]["calls"].as_array().expect("nested calls array");
        assert_eq!(calls.len(), 2, "both nested calls unwrap");
        assert_eq!(calls[0]["call"], "system.remark");
        assert_eq!(calls[0]["args"]["remark"], serde_json::json!([104, 105]));
        assert_eq!(calls[1]["args"]["remark"], serde_json::json!([121, 111]));
    }

    #[test]
    fn trailing_bytes_and_garbage_are_loud_errors() {
        let Some(ix) = CallIndex::load() else {
            eprintln!("SKIP: real fixture metadata not present");
            return;
        };
        let (sys, remark) = ix.indices("System", "remark");
        let mut bytes = vec![sys, remark];
        bytes.extend(compact(0));
        bytes.push(0xFF); // trailing garbage
        assert!(decode_call(&ix.blob(), &bytes).unwrap_err().contains("trailing"));
        assert!(decode_call(&ix.blob(), &[0xFF, 0xFF, 0xFF]).is_err());
    }

    #[test]
    fn blake2_256_matches_known_vector() {
        // blake2b-256 of empty input — standard vector
        assert_eq!(
            hex::encode(blake2_256(b"")),
            "0e5751c026e543b2e8ab2eb06099daa1d1e5df47778f7787faab45cdf12fe3a8"
        );
    }

    #[test]
    fn json_bytes_collects_nested_and_rejects_non_bytes() {
        assert_eq!(json_bytes(&serde_json::json!([[1, 2], 3])), Some(vec![1, 2, 3]));
        assert_eq!(json_bytes(&serde_json::json!([256])), None);
        assert_eq!(json_bytes(&serde_json::json!("nope")), None);
        assert_eq!(json_bytes(&serde_json::json!([])), Some(vec![]));
    }
}
