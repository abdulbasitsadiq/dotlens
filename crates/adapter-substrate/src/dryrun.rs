//! Tier 1 simulation: the Substrate half of `sim` (Phase 3, slice 1).
//!
//! PURE — bytes and metadata in, an outcome out. No network: the RPC lives in
//! `source.rs` and the wiring in `dotlens-node`. This file knows one thing:
//! how to ask a runtime `DryRunApi::dry_run_call` and how to read what it says.
//!
//! THE WHOLE REQUEST IS BUILT FROM THE RUNTIME'S OWN METADATA, and that is the
//! design, not a flourish (Invariant 2 — no chain-specific branching):
//!   * whether the chain supports the API at all, and at which version, comes
//!     from `RuntimeVersion.apis` — a list of (blake2b-64(trait name), version)
//!     the runtime publishes about itself;
//!   * the ARITY comes from the metadata's own `apis` section, not from that
//!     version number, so a v1 runtime (`dry_run_call(origin, call)`) and a v2
//!     runtime (`… , result_xcms_version`) are one code path reading two
//!     different declarations;
//!   * the origin is two variant INDICES looked up by name in the runtime's type
//!     registry, so `Origins:MediumSpender` needs no table of ours;
//!   * the response is decoded against the method's declared output type id, so
//!     `CallDryRunEffects` is never written down in Rust here.
//! Adding a chain adds nothing to this file. A chain that spells its origins
//! differently is refused with the names it DOES have, never guessed at.
//!
//! THE METADATA MUST BE v15 OR v16. v14 has no runtime-API section at all, and
//! `state_getMetadata` returns v14 on every runtime we index — which is the
//! "v15/v16 via the Metadata runtime API" debt `source.rs` has carried since
//! Phase 1 slice 2. This slice pays it: `Metadata_metadata_at_version(15)`.
//!
//! WHAT A TIER 1 ANSWER IS NOT (stated here because the API repeats it to
//! callers): the runtime dispatches the call directly, so no signature, nonce,
//! fee, mortality or length/weight check is modelled; the state is the state
//! NOW, not the state at a future enactment; and `forwarded_xcms` is not what a
//! destination chain would do with the messages. Those are Tier 2's job, and
//! pretending otherwise is how a simulation feature becomes a lie.
//!
//! AND `forwarded_xcms` IS NOT NECESSARILY THIS CALL'S DOING — measured, not
//! assumed. On the Polkadot relay it comes back identical for two different
//! calls at one state and for one call at two states (64 destinations, 74 real
//! in-flight messages) because the relay's router reports every parachain's
//! existing downward queue; Asset Hub returns an empty list for the same call.
//! This file records what the runtime said, verbatim; attribution needs a
//! difference against a no-op run at the same state, which is a later slice.

use crate::calls::{self, DecodedCall};
use crate::frame_decoder::value_to_json;
use frame_metadata::{RuntimeMetadata, RuntimeMetadataPrefixed};
use parity_scale_codec::Decode;
use scale_info::{PortableRegistry, TypeDef, TypeDefPrimitive};
use scale_value::{Composite, Value, ValueDef};
use sim::{OriginSpec, SimError, SimEvent, SimOutcome, SimStatus};

/// Lineage for every row this runner writes. Bump when the INTERPRETATION of a
/// response changes; rows below it rebuild from the archived bytes.
pub const DRY_RUN_VERSION: u32 = 1;

pub const DRY_RUN_API: &str = "DryRunApi";
pub const DRY_RUN_CALL_METHOD: &str = "dry_run_call";
/// The wire name is `Trait_method` (sp-api's `prefix_function_with_trait`).
pub const DRY_RUN_CALL_FUNCTION: &str = "DryRunApi_dry_run_call";

/// BLAKE2b with an **8-byte digest** — how `sp_api` derives the runtime-API ids
/// in `RuntimeVersion.apis` (`Blake2b::<U8>::digest(trait_name)`).
///
/// NOT a truncation of BLAKE2b-512: the digest length is mixed into BLAKE2b's
/// parameter block, so truncating gives a plausible-looking wrong id — and a
/// wrong id here fails in the worst possible way, by reporting every chain as
/// not supporting the API. The unit test pins `Core` and `Metadata` against
/// their well-known ids for exactly that reason.
pub fn blake2_64(bytes: &[u8]) -> [u8; 8] {
    use blake2::digest::{consts::U8, Digest};
    let mut hasher = blake2::Blake2b::<U8>::new();
    hasher.update(bytes);
    hasher.finalize().into()
}

/// The 8-byte id a runtime publishes for `trait_name` (the BARE trait ident —
/// no module path, no generics).
pub fn runtime_api_id(trait_name: &str) -> [u8; 8] {
    blake2_64(trait_name.as_bytes())
}

/// The version of `trait_name` this runtime declares, from the `apis` field of
/// `state_getRuntimeVersion` (`[["0x…8 bytes", 2], …]`). `None` = the runtime
/// does not implement it, which is an answer, not a failure.
pub fn declared_api_version(apis: &serde_json::Value, trait_name: &str) -> Option<u32> {
    let want = runtime_api_id(trait_name);
    for entry in apis.as_array()? {
        // Skip a malformed entry, never abandon the scan: `?` here would turn
        // one odd element anywhere in the list into "this chain does not
        // implement the API", which is a confident false statement about a
        // chain rather than the honest refusal `SimError::Unsupported` claims.
        let Some(pair) = entry.as_array() else { continue };
        let (Some(id), Some(version)) = (pair.first(), pair.get(1)) else {
            continue;
        };
        let Some(id_hex) = id.as_str() else { continue };
        let Ok(id_bytes) = hex::decode(id_hex.trim_start_matches("0x")) else {
            continue;
        };
        if id_bytes.as_slice() == want.as_slice() {
            return version.as_u64().map(|v| v as u32);
        }
    }
    None
}

/// The version a metadata blob DECLARES about itself, or `None` if it is not
/// decodable metadata at all.
///
/// Used to VERIFY a blob before it is written to the write-once raw store: a
/// node that answers `metadata_at_version(15)` with something else would
/// otherwise poison that key permanently. Lives here rather than in the node
/// because decoding metadata is protocol knowledge (Invariant 4).
pub fn metadata_version(blob: &[u8]) -> Option<u32> {
    RuntimeMetadataPrefixed::decode(&mut &blob[..])
        .ok()
        .map(|p| p.1.version())
}

/// Everything the request and the response need, read once from one metadata
/// blob: the type registry, the pallet index → error-type map (for naming a
/// dispatch failure), and the four type ids of `dry_run_call`.
#[derive(Debug)]
pub struct DryRunContext {
    types: PortableRegistry,
    /// (pallet index, pallet name, error type id) — the dispatch-error decoder.
    pallets: Vec<(u8, String, Option<u32>)>,
    origin_ty: u32,
    call_ty: u32,
    /// `Some` iff the method takes `result_xcms_version` — i.e. DryRunApi v2.
    /// Read from the metadata's ARITY, never from a version number.
    xcm_version_ty: Option<u32>,
    output_ty: u32,
    /// v16 declares the API version in metadata; v15 does not. Informational —
    /// `RuntimeVersion.apis` remains the portable source.
    metadata_api_version: Option<u32>,
}

/// Pull the DryRunApi method + pallet error map out of a v15/v16 metadata. One
/// macro arm covers both: the field names are identical, only the surrounding
/// struct type differs (the same device the `Tracks` constant walk uses).
macro_rules! dry_run_method {
    ($m:expr) => {{
        let m = $m;
        let api = m
            .apis
            .iter()
            .find(|a| a.name == DRY_RUN_API)
            .ok_or_else(|| {
                let have: Vec<&str> = m.apis.iter().map(|a| a.name.as_ref()).collect();
                format!(
                    "this runtime's metadata declares no {DRY_RUN_API}; it has: {}",
                    have.join(", ")
                )
            })?;
        let method = api
            .methods
            .iter()
            .find(|f| f.name == DRY_RUN_CALL_METHOD)
            .ok_or_else(|| {
                format!("{DRY_RUN_API} exists but has no {DRY_RUN_CALL_METHOD} method")
            })?;
        let inputs: Vec<(String, u32)> = method
            .inputs
            .iter()
            .map(|i| (i.name.to_string(), i.ty.id))
            .collect();
        let pallets: Vec<(u8, String, Option<u32>)> = m
            .pallets
            .iter()
            .map(|p| (p.index, p.name.to_string(), p.error.as_ref().map(|e| e.ty.id)))
            .collect();
        (m.types.clone(), pallets, inputs, method.output.id)
    }};
}

impl DryRunContext {
    /// Read the context out of a **v15 or v16** metadata blob.
    pub fn from_metadata(metadata_blob: &[u8]) -> Result<Self, SimError> {
        Self::from_metadata_inner(metadata_blob).map_err(SimError::Encode)
    }

    fn from_metadata_inner(metadata_blob: &[u8]) -> Result<Self, String> {
        let prefixed = RuntimeMetadataPrefixed::decode(&mut &metadata_blob[..])
            .map_err(|e| format!("metadata blob undecodable: {e}"))?;
        let (types, pallets, inputs, output_ty) = match &prefixed.1 {
            RuntimeMetadata::V15(m) => dry_run_method!(m),
            RuntimeMetadata::V16(m) => dry_run_method!(m),
            RuntimeMetadata::V14(_) => {
                return Err(
                    "metadata v14 carries no runtime-API section, so a dry run cannot be \
                     built from it — fetch v15 via Metadata_metadata_at_version(15). \
                     (state_getMetadata returns v14 on every runtime we index; that is \
                     why the v15 blob is archived beside it, not instead of it.)"
                        .into(),
                )
            }
            other => {
                return Err(format!(
                    "unsupported metadata version {} (v15/v16 carry runtime APIs)",
                    other.version()
                ))
            }
        };
        let metadata_api_version = match &prefixed.1 {
            RuntimeMetadata::V16(m) => m
                .apis
                .iter()
                .find(|a| a.name == DRY_RUN_API)
                .map(|a| a.version.0),
            _ => None,
        };

        // The trait's own parameter names, checked rather than assumed. If a
        // future version renames or reorders them, this halts here instead of
        // sending a well-formed request that means something else.
        let names: Vec<&str> = inputs.iter().map(|(n, _)| n.as_str()).collect();
        let (origin_ty, call_ty, xcm_version_ty) = match names.as_slice() {
            ["origin", "call", "result_xcms_version"] => {
                (inputs[0].1, inputs[1].1, Some(inputs[2].1))
            }
            ["origin", "call"] => (inputs[0].1, inputs[1].1, None),
            other => {
                return Err(format!(
                    "{DRY_RUN_CALL_METHOD} has parameters {other:?}, which is neither the v1 \
                     (origin, call) nor the v2 (origin, call, result_xcms_version) shape — \
                     refusing to guess how to encode them"
                ))
            }
        };
        Ok(Self {
            types,
            pallets,
            origin_ty,
            call_ty,
            xcm_version_ty,
            output_ty,
            metadata_api_version,
        })
    }

    /// Build a context from parts. Used by Tier 2 and by tests, which register
    /// the shapes they need in a `scale_info::Registry` — the v15 walk above
    /// cannot be exercised offline, because every metadata blob in `fixtures/`
    /// is v14 by construction.
    pub fn from_parts(
        types: PortableRegistry,
        pallets: Vec<(u8, String, Option<u32>)>,
        origin_ty: u32,
        call_ty: u32,
        xcm_version_ty: Option<u32>,
        output_ty: u32,
    ) -> Self {
        Self {
            types,
            pallets,
            origin_ty,
            call_ty,
            xcm_version_ty,
            output_ty,
            metadata_api_version: None,
        }
    }

    /// 3 for DryRunApi v2, 2 for v1 — as the runtime's metadata declares it.
    pub fn arity(&self) -> usize {
        if self.xcm_version_ty.is_some() {
            3
        } else {
            2
        }
    }

    pub fn metadata_api_version(&self) -> Option<u32> {
        self.metadata_api_version
    }

    /// Decode the call we are about to submit, against the type the API itself
    /// declares for its `call` parameter — a stricter check than decoding
    /// against `extrinsic.call_ty`, and it doubles as the human summary.
    pub fn decode_call(&self, call_bytes: &[u8]) -> Result<DecodedCall, SimError> {
        calls::decode_call_with(&self.types, self.call_ty, call_bytes).map_err(|e| {
            SimError::Encode(format!(
                "these bytes are not a RuntimeCall for this runtime: {e}"
            ))
        })
    }

    /// `origin ++ call [++ result_xcms_version]`, plus the origin as JSON.
    ///
    /// The call bytes are appended RAW: a preimage already holds exactly the
    /// SCALE encoding of `RuntimeCall`, so re-encoding it would be a round trip
    /// through our own decoder with nothing to gain and a shape to lose.
    pub fn encode_params(
        &self,
        origin: &OriginSpec,
        call_bytes: &[u8],
        xcm_version: u32,
    ) -> Result<(Vec<u8>, serde_json::Value), SimError> {
        let (mut params, origin_json) = self.encode_origin(origin)?;
        params.extend_from_slice(call_bytes);
        if let Some(ty) = self.xcm_version_ty {
            if !self.is_u32(ty) {
                return Err(SimError::Encode(
                    "result_xcms_version is not a u32 in this runtime's registry — refusing \
                     to encode it blind"
                        .into(),
                ));
            }
            // Runtime-API parameters are plain concatenated SCALE: a u32 is four
            // little-endian bytes, NOT a compact.
            params.extend_from_slice(&xcm_version.to_le_bytes());
        }
        Ok((params, origin_json))
    }

    /// An origin is two variant indices (and, for `Signed`, 32 bytes), read out
    /// of the runtime's own `OriginCaller` type. No general encoder is needed
    /// and none is used: this is small enough to be exact and testable, and it
    /// keeps the encoding independent of any library's newtype heuristics.
    pub fn encode_origin(
        &self,
        origin: &OriginSpec,
    ) -> Result<(Vec<u8>, serde_json::Value), SimError> {
        self.encode_origin_inner(origin).map_err(SimError::Encode)
    }

    fn encode_origin_inner(
        &self,
        origin: &OriginSpec,
    ) -> Result<(Vec<u8>, serde_json::Value), String> {
        let (pallet, variant, account) = match origin {
            OriginSpec::Variant { pallet, variant } => (pallet.as_str(), variant.as_str(), None),
            // `signed:…` is `system:Signed(who)` — the one origin with a field.
            OriginSpec::Signed(who) => ("system", "Signed", Some(who)),
        };

        let outer = self.variants_of(self.origin_ty, "the runtime's OriginCaller")?;
        let outer_variant = find_variant(outer, pallet).ok_or_else(|| {
            format!(
                "no origin '{pallet}' in this runtime; it has: {}",
                variant_names(outer)
            )
        })?;
        let inner_ty = match outer_variant.fields.len() {
            1 => outer_variant.fields[0].ty.id,
            n => {
                return Err(format!(
                    "origin '{}' holds {n} fields, not one nested origin enum",
                    outer_variant.name
                ))
            }
        };
        let inner = self.variants_of(inner_ty, &format!("origin '{}'", outer_variant.name))?;
        let inner_variant = find_variant(inner, variant).ok_or_else(|| {
            format!(
                "no variant '{variant}' in origin '{}'; it has: {}",
                outer_variant.name,
                variant_names(inner)
            )
        })?;

        let mut bytes = vec![outer_variant.index, inner_variant.index];
        match (account, inner_variant.fields.len()) {
            (None, 0) => {}
            (None, n) => {
                return Err(format!(
                    "origin '{}:{}' carries {n} field(s); only fieldless origins and \
                     signed:<account> are expressible today",
                    outer_variant.name, inner_variant.name
                ))
            }
            (Some(who), 1) => {
                let field_ty = inner_variant.fields[0].ty.id;
                if !self.is_32_byte_account(field_ty) {
                    return Err(format!(
                        "'{}:{}' does not take a 32-byte account id on this chain (an \
                         AccountId20 chain, most likely) — not expressible today",
                        outer_variant.name, inner_variant.name
                    ));
                }
                bytes.extend_from_slice(who);
            }
            (Some(_), n) => {
                return Err(format!(
                    "'{}:{}' takes {n} fields, so it is not Signed(who)",
                    outer_variant.name, inner_variant.name
                ))
            }
        }

        // These are `OriginCaller` VARIANT indices, which are unrelated to the
        // pallet indices in `construct_runtime!` — naming them `pallet_index`
        // would invite a join against `core.events`' pallet numbering that would
        // silently match the wrong pallet.
        let json = serde_json::json!({
            "resolved": format!("{}:{}", outer_variant.name, inner_variant.name),
            "origin_variant_index": outer_variant.index,
            "inner_variant_index": inner_variant.index,
            "account": account.map(|w| format!("0x{}", hex::encode(w))),
        });
        Ok((bytes, json))
    }

    /// Response bytes → outcome. Every field is looked up BY NAME and a missing
    /// one halts: a silently-renamed field would otherwise turn into a null and
    /// be reported as "this call emits no events".
    pub fn interpret(&self, response: &[u8]) -> Result<SimOutcome, SimError> {
        self.interpret_inner(response).map_err(SimError::Decode)
    }

    fn interpret_inner(&self, response: &[u8]) -> Result<SimOutcome, String> {
        let mut cursor = response;
        let value = scale_value::scale::decode_as_type(&mut cursor, self.output_ty, &self.types)
            .map_err(|e| format!("dry-run response decode: {e}"))?;
        if !cursor.is_empty() {
            return Err(format!(
                "{} trailing bytes after the dry-run response — the declared output type and \
                 the bytes disagree",
                cursor.len()
            ));
        }
        let effects_json = json_of(&value);

        let ValueDef::Variant(outer) = &value.value else {
            return Err("dry-run response is not a Result variant".into());
        };
        match outer.name.as_str() {
            // The API itself refused. No dispatch happened, so there is no
            // dispatch verdict — `dispatch_ok` stays NULL rather than false,
            // because "the call failed" and "we never got to try" are different
            // facts and a treasury actor needs to tell them apart.
            "Err" => {
                let reason = variant_inner(outer)
                    .and_then(|v| match &v.value {
                        ValueDef::Variant(e) => Some(e.name.clone()),
                        _ => None,
                    })
                    .unwrap_or_else(|| "unknown".into());
                Ok(SimOutcome {
                    status: SimStatus::ApiError,
                    dispatch_ok: None,
                    dispatch_error: None,
                    events: vec![],
                    local_xcm: None,
                    forwarded_xcms: vec![],
                    effects: effects_json,
                    note: Some(format!(
                        "the runtime's dry-run API refused the request ({reason}); no dispatch \
                         was attempted"
                    )),
                })
            }
            "Ok" => {
                let effects = variant_inner(outer)
                    .ok_or("dry-run Ok carries no effects payload")?;

                let execution = named(effects, "execution_result")
                    .ok_or("effects has no execution_result field")?;
                let ValueDef::Variant(exec) = &execution.value else {
                    return Err("execution_result is not a Result variant".into());
                };
                let (status, dispatch_ok, dispatch_error) = match exec.name.as_str() {
                    "Ok" => (SimStatus::Executed, Some(true), None),
                    // A dispatch failure is a RESULT: the runtime answered, and
                    // the answer is "this would not work". Never an error of ours.
                    "Err" => {
                        let err = variant_inner(exec)
                            .and_then(|v| named(v, "error").cloned())
                            .ok_or(
                                "execution_result Err carries no `error` field — refusing to \
                                 report a failure whose reason we could not read",
                            )?;
                        (
                            SimStatus::DispatchFailed,
                            Some(false),
                            Some(self.describe_dispatch_error(&err)),
                        )
                    }
                    other => return Err(format!("execution_result variant '{other}' is neither Ok nor Err")),
                };

                let events_value =
                    named(effects, "emitted_events").ok_or("effects has no emitted_events field")?;
                let events = self.read_events(events_value)?;

                // Option<VersionedXcm> — Some/None, not a nullable field.
                let local_xcm = match named(effects, "local_xcm").map(|v| &v.value) {
                    Some(ValueDef::Variant(opt)) if opt.name == "Some" => {
                        variant_inner(opt).map(json_of)
                    }
                    Some(ValueDef::Variant(opt)) if opt.name == "None" => None,
                    Some(_) => return Err("local_xcm is not an Option variant".into()),
                    None => return Err("effects has no local_xcm field".into()),
                };

                let forwarded = named(effects, "forwarded_xcms")
                    .ok_or("effects has no forwarded_xcms field")?;
                let forwarded_xcms = read_forwarded(forwarded)?;

                // A SUCCESSFUL DISPATCH IS NOT A SUCCESSFUL BATCH, and most
                // governance proposals are batches. `utility.batch` returns Ok
                // when an inner call fails — it stops and emits
                // `BatchInterrupted{index, error}` — and `force_batch` emits
                // `ItemFailed` and carries on. So `status: executed` alone would
                // report a half-applied referendum as fine. Same doctrine as
                // slice 9's "dispatched ≠ succeeded"; the events already say it,
                // this makes sure nobody has to notice on their own.
                let inner_failures: Vec<&str> = events
                    .iter()
                    .map(|e| e.name.as_str())
                    .filter(|n| *n == "utility.BatchInterrupted" || *n == "utility.ItemFailed")
                    .collect();
                let note = (status == SimStatus::Executed && !inner_failures.is_empty()).then(
                    || {
                        format!(
                            "the call dispatched successfully but an INNER call did not — {} \
                             present. utility.batch returns Ok when it stops early, so the \
                             status is about the outer call only; read emitted_events",
                            inner_failures.join(" + ")
                        )
                    },
                );

                Ok(SimOutcome {
                    status,
                    dispatch_ok,
                    dispatch_error,
                    events,
                    local_xcm,
                    forwarded_xcms,
                    effects: effects_json,
                    note,
                })
            }
            other => Err(format!(
                "dry-run response variant '{other}' is neither Ok nor Err"
            )),
        }
    }

    /// `RuntimeEvent::Pallet(pallet::Event::Variant{…})` → `pallet.Variant`,
    /// the same name `core.events` stores, so a simulated effect and a real one
    /// are the same string.
    fn read_events(&self, events: &Value<u32>) -> Result<Vec<SimEvent>, String> {
        // NO newtype peeling here, deliberately: a Vec of exactly one element is
        // `Composite::Unnamed([x])`, which is byte-for-byte the shape of a
        // newtype wrapper, so a peeler would turn a one-event list into that
        // event's fields. The declared type is already `Vec<Event>`.
        let ValueDef::Composite(Composite::Unnamed(items)) = &events.value else {
            return Err("emitted_events is not a sequence".into());
        };
        items
            .iter()
            .map(|ev| {
                let ValueDef::Variant(pallet) = &ev.value else {
                    return Err("an emitted event is not a pallet variant".into());
                };
                let inner = variant_inner(pallet)
                    .ok_or_else(|| format!("event pallet {} carries no event", pallet.name))?;
                let ValueDef::Variant(event) = &inner.value else {
                    return Err(format!("{}: inner value is not an event variant", pallet.name));
                };
                Ok(SimEvent {
                    name: format!("{}.{}", pallet.name.to_lowercase(), event.name),
                    data: json_of_composite(&event.values),
                })
            })
            .collect()
    }

    /// Name a dispatch failure through the runtime's own error metadata:
    /// `{"error": "assets.NoAccount", "raw": {…}}`. `BadOrigin` and friends need
    /// no lookup — they already name themselves.
    fn describe_dispatch_error(&self, err: &Value<u32>) -> serde_json::Value {
        let raw = json_of(err);
        let named_error = match &err.value {
            ValueDef::Variant(v) if v.name == "Module" => {
                // DispatchError::Module(ModuleError { index, error }) — a newtype
                // over a struct, so the fields sit one layer in.
                let index = variant_inner(v)
                    .and_then(|inner| named(inner, "index"))
                    .and_then(value_as_u64);
                let error_byte = variant_inner(v)
                    .and_then(|inner| named(inner, "error"))
                    .and_then(first_byte);
                match (index, error_byte) {
                    (Some(i), Some(e)) if i <= u8::MAX as u64 => {
                        self.module_error_name(i as u8, e)
                    }
                    _ => None,
                }
            }
            // `Token(FundsUnavailable)`, `Arithmetic(Overflow)` and friends name
            // themselves, but the useful half is the INNER variant — "Token"
            // alone is nearly content-free next to "assets.NoAccount".
            ValueDef::Variant(v) => Some(match variant_inner(v).map(|i| &i.value) {
                Some(ValueDef::Variant(inner)) => format!("{}.{}", v.name, inner.name),
                _ => v.name.clone(),
            }),
            _ => None,
        };
        serde_json::json!({ "error": named_error, "raw": raw })
    }

    fn module_error_name(&self, pallet_index: u8, error_index: u8) -> Option<String> {
        let (_, pallet_name, error_ty) =
            self.pallets.iter().find(|(i, _, _)| *i == pallet_index)?;
        let ty = self.types.resolve((*error_ty)?)?;
        let TypeDef::Variant(var) = &ty.type_def else {
            return None;
        };
        let v = var.variants.iter().find(|v| v.index == error_index)?;
        Some(format!("{}.{}", pallet_name.to_lowercase(), v.name))
    }

    fn variants_of<'a>(
        &'a self,
        ty_id: u32,
        what: &str,
    ) -> Result<&'a [scale_info::Variant<scale_info::form::PortableForm>], String> {
        let ty = self
            .types
            .resolve(ty_id)
            .ok_or_else(|| format!("{what}: type {ty_id} is not in the registry"))?;
        match &ty.type_def {
            TypeDef::Variant(v) => Ok(&v.variants),
            _ => Err(format!("{what}: type {ty_id} is not an enum")),
        }
    }

    fn is_u32(&self, ty_id: u32) -> bool {
        matches!(
            self.types.resolve(ty_id).map(|t| &t.type_def),
            Some(TypeDef::Primitive(TypeDefPrimitive::U32))
        )
    }

    /// Is this type an `AccountId32`? Walks single-field newtypes down to the
    /// array, so `AccountId32(pub [u8; 32])` and a bare `[u8; 32]` both pass and
    /// an AccountId20 chain fails honestly instead of being sent 32 bytes.
    fn is_32_byte_account(&self, ty_id: u32) -> bool {
        let mut id = ty_id;
        for _ in 0..4 {
            let Some(ty) = self.types.resolve(id) else {
                return false;
            };
            match &ty.type_def {
                TypeDef::Array(a) => {
                    return a.len == 32
                        && matches!(
                            self.types.resolve(a.type_param.id).map(|t| &t.type_def),
                            Some(TypeDef::Primitive(TypeDefPrimitive::U8))
                        )
                }
                TypeDef::Composite(c) if c.fields.len() == 1 => id = c.fields[0].ty.id,
                TypeDef::Tuple(t) if t.fields.len() == 1 => id = t.fields[0].id,
                _ => return false,
            }
        }
        false
    }
}

// ------------------------------------------------------------------- helpers

fn find_variant<'a>(
    variants: &'a [scale_info::Variant<scale_info::form::PortableForm>],
    name: &str,
) -> Option<&'a scale_info::Variant<scale_info::form::PortableForm>> {
    variants
        .iter()
        .find(|v| v.name == name)
        // Case-insensitive only as a FALLBACK: `construct_runtime!` spells the
        // frame-system origin lowercase (`system`) and pallet origins as
        // declared, so exact matches must win before any folding happens.
        .or_else(|| variants.iter().find(|v| v.name.eq_ignore_ascii_case(name)))
}

fn variant_names(variants: &[scale_info::Variant<scale_info::form::PortableForm>]) -> String {
    variants
        .iter()
        .map(|v| v.name.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

/// The single field inside a newtype-ish variant (`Ok(x)`, `Some(x)`,
/// `Module(e)`).
fn variant_inner<'a>(v: &'a scale_value::Variant<u32>) -> Option<&'a Value<u32>> {
    match &v.values {
        Composite::Unnamed(items) => items.first(),
        Composite::Named(items) => items.first().map(|(_, v)| v),
    }
}

/// Look up a named field, peeling single-field UNNAMED wrappers on the way in.
///
/// The peeling is the slice-6 rule: our decoder renders newtypes one array layer
/// deeper than a human writes them, and `DispatchError::Module(ModuleError{…})`
/// is exactly that shape. Bounded at four layers so a pathological type cannot
/// spin, and never applied to sequences (see `read_events`).
fn named<'a>(v: &'a Value<u32>, field: &str) -> Option<&'a Value<u32>> {
    let mut cur = v;
    for _ in 0..4 {
        match &cur.value {
            ValueDef::Composite(Composite::Named(fields)) => {
                return fields.iter().find(|(n, _)| n == field).map(|(_, v)| v)
            }
            ValueDef::Composite(Composite::Unnamed(items)) if items.len() == 1 => {
                cur = &items[0];
            }
            _ => return None,
        }
    }
    None
}

fn value_as_u64(v: &Value<u32>) -> Option<u64> {
    match &v.value {
        ValueDef::Primitive(scale_value::Primitive::U128(n)) => u64::try_from(*n).ok(),
        _ => None,
    }
}

/// First byte of a `[u8; 4]`-ish value — pallet error indices. Older runtimes
/// carried a bare `u8` there, so both shapes are read.
fn first_byte(v: &Value<u32>) -> Option<u8> {
    match &v.value {
        ValueDef::Primitive(scale_value::Primitive::U128(n)) => u8::try_from(*n).ok(),
        ValueDef::Composite(Composite::Unnamed(items)) => {
            items.first().and_then(value_as_u64).and_then(|n| u8::try_from(n).ok())
        }
        _ => None,
    }
}

/// `Vec<(VersionedLocation, Vec<VersionedXcm>)>` → one object per destination.
fn read_forwarded(v: &Value<u32>) -> Result<Vec<serde_json::Value>, String> {
    let ValueDef::Composite(Composite::Unnamed(items)) = &v.value else {
        return Err("forwarded_xcms is not a sequence".into());
    };
    items
        .iter()
        .map(|pair| {
            let ValueDef::Composite(Composite::Unnamed(parts)) = &pair.value else {
                return Err("a forwarded_xcms entry is not a (destination, messages) pair".into());
            };
            let (Some(dest), Some(messages)) = (parts.first(), parts.get(1)) else {
                return Err("a forwarded_xcms entry has fewer than two parts".into());
            };
            Ok(serde_json::json!({
                "destination": json_of(dest),
                "messages": json_of(messages),
            }))
        })
        .collect()
}

/// Render through the SAME value→JSON path every other decoded row uses, so a
/// simulated event body and an indexed event body are the same shapes.
fn json_of(v: &Value<u32>) -> serde_json::Value {
    value_to_json(&v.clone().remove_context())
}

fn json_of_composite(c: &Composite<u32>) -> serde_json::Value {
    json_of(&Value::with_context(ValueDef::Composite(c.clone()), 0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use parity_scale_codec::Encode;
    use scale_info::{MetaType, Registry, TypeInfo};

    // ---------------------------------------------------------------- fixtures
    // Shapes that mimic the runtime's, registered in a real `scale_info`
    // registry so every assertion below runs against REAL SCALE bytes produced
    // by codec::Encode — never against hand-written JSON on both sides of the
    // comparison (the slice-6 anti-pattern).

    #[derive(Encode, Decode, TypeInfo, Debug, PartialEq)]
    struct TAccount([u8; 32]);

    #[derive(Encode, Decode, TypeInfo, Debug, PartialEq)]
    struct TAccount20([u8; 20]);

    #[derive(Encode, Decode, TypeInfo, Debug, PartialEq)]
    enum TRawOrigin {
        Root,
        Signed(TAccount),
        None,
    }

    #[derive(Encode, Decode, TypeInfo, Debug, PartialEq)]
    enum TRawOrigin20 {
        Root,
        Signed(TAccount20),
    }

    #[derive(Encode, Decode, TypeInfo, Debug, PartialEq)]
    enum TGovOrigins {
        SmallSpender,
        MediumSpender,
        Treasurer,
    }

    #[allow(non_camel_case_types)]
    #[derive(Encode, Decode, TypeInfo, Debug, PartialEq)]
    enum TOriginCaller {
        // construct_runtime! spells this one lowercase; the test keeps that.
        system(TRawOrigin),
        Void,
        Origins(TGovOrigins),
    }

    #[allow(non_camel_case_types)]
    #[derive(Encode, Decode, TypeInfo)]
    enum TOriginCaller20 {
        system(TRawOrigin20),
    }

    #[derive(Encode, Decode, TypeInfo)]
    struct TPostInfo {
        actual_weight: Option<u64>,
        pays_fee: bool,
    }

    #[derive(Encode, Decode, TypeInfo)]
    struct TModuleError {
        index: u8,
        error: [u8; 4],
    }

    #[derive(Encode, Decode, TypeInfo)]
    enum TTokenError {
        FundsUnavailable,
        BelowMinimum,
    }

    #[derive(Encode, Decode, TypeInfo)]
    enum TDispatchError {
        Other,
        BadOrigin,
        Module(TModuleError),
        Token(TTokenError),
    }

    #[derive(Encode, Decode, TypeInfo)]
    struct TDispatchErrorWithPostInfo {
        post_info: TPostInfo,
        error: TDispatchError,
    }

    #[derive(Encode, Decode, TypeInfo)]
    enum TAssetsError {
        BalanceLow,
        NoAccount,
        NoPermission,
    }

    #[derive(Encode, Decode, TypeInfo)]
    enum TBalancesEvent {
        Transfer {
            from: TAccount,
            to: TAccount,
            amount: u128,
        },
    }

    #[derive(Encode, Decode, TypeInfo)]
    enum TSystemEvent {
        ExtrinsicSuccess,
    }

    #[derive(Encode, Decode, TypeInfo)]
    enum TUtilityEvent {
        BatchInterrupted { index: u32 },
    }

    #[derive(Encode, Decode, TypeInfo)]
    enum TRuntimeEvent {
        System(TSystemEvent),
        Balances(TBalancesEvent),
        Utility(TUtilityEvent),
    }

    #[derive(Encode, Decode, TypeInfo)]
    enum TVersionedXcm {
        V4(Vec<u8>),
    }

    #[derive(Encode, Decode, TypeInfo)]
    enum TVersionedLocation {
        V4 { parents: u8, parachain: u32 },
    }

    #[derive(Encode, Decode, TypeInfo)]
    struct TEffects {
        execution_result: Result<TPostInfo, TDispatchErrorWithPostInfo>,
        emitted_events: Vec<TRuntimeEvent>,
        local_xcm: Option<TVersionedXcm>,
        forwarded_xcms: Vec<(TVersionedLocation, Vec<TVersionedXcm>)>,
    }

    #[derive(Encode, Decode, TypeInfo)]
    enum TApiError {
        Unimplemented,
        VersionedConversionFailed,
    }

    type TOutput = Result<TEffects, TApiError>;

    /// A minimal RuntimeCall so `call_ty` resolves to something real.
    #[allow(non_camel_case_types)]
    #[derive(Encode, Decode, TypeInfo)]
    enum TRuntimeCall {
        System(TSystemCall),
    }

    #[allow(non_camel_case_types)]
    #[derive(Encode, Decode, TypeInfo)]
    enum TSystemCall {
        remark { remark: Vec<u8> },
    }

    fn context() -> DryRunContext {
        context_with_origin::<TOriginCaller>()
    }

    fn context_with_origin<O: TypeInfo + 'static>() -> DryRunContext {
        let mut registry = Registry::new();
        let origin = registry.register_type(&MetaType::new::<O>()).id;
        let call = registry.register_type(&MetaType::new::<TRuntimeCall>()).id;
        let xcm_version = registry.register_type(&MetaType::new::<u32>()).id;
        let output = registry.register_type(&MetaType::new::<TOutput>()).id;
        let assets_error = registry.register_type(&MetaType::new::<TAssetsError>()).id;
        let types: PortableRegistry = registry.into();
        DryRunContext::from_parts(
            types,
            vec![
                (0, "System".into(), None),
                (50, "Assets".into(), Some(assets_error)),
            ],
            origin,
            call,
            Some(xcm_version),
            output,
        )
    }

    // ------------------------------------------------------------------ tests

    #[test]
    fn runtime_api_ids_are_the_parameterised_blake2b_not_a_truncation() {
        // Well-known ids every Substrate node publishes — if these two match,
        // the hash function is right for every other trait too.
        assert_eq!(hex::encode(runtime_api_id("Core")), "df6acb689907609b");
        assert_eq!(hex::encode(runtime_api_id("Metadata")), "37e397fc7c91f5e4");
        assert_eq!(hex::encode(runtime_api_id(DRY_RUN_API)), "91b1c8b16328eb92");
        // The trap this pins: truncating BLAKE2b-512 gives a different id, and a
        // wrong id would report every chain as "does not support the API".
        assert_ne!(hex::encode(runtime_api_id("Metadata")), "965d0d233b88892a");
    }

    #[test]
    fn the_api_version_comes_from_the_runtimes_own_apis_list() {
        let apis = serde_json::json!([
            ["0xdf6acb689907609b", 5],
            ["0x91b1c8b16328eb92", 2],
            ["0x37e397fc7c91f5e4", 2]
        ]);
        assert_eq!(declared_api_version(&apis, DRY_RUN_API), Some(2));
        assert_eq!(declared_api_version(&apis, "Core"), Some(5));
        // A runtime without it says so by absence — the honest "unsupported".
        assert_eq!(declared_api_version(&apis, "XcmPaymentApi"), None);
        assert_eq!(declared_api_version(&serde_json::json!(null), "Core"), None);
    }

    #[test]
    fn an_origin_is_two_variant_indices_read_from_the_runtimes_registry() {
        let ctx = context();

        let (root, json) = ctx
            .encode_origin(&OriginSpec::Variant {
                pallet: "system".into(),
                variant: "Root".into(),
            })
            .expect("root encodes");
        // The strong form of the assertion: the bytes we built decode back into
        // the real Rust value through codec, so encoder and decoder are not two
        // copies of the same guess.
        assert_eq!(
            TOriginCaller::decode(&mut &root[..]).unwrap(),
            TOriginCaller::system(TRawOrigin::Root)
        );
        assert_eq!(root, vec![0u8, 0u8]);
        assert_eq!(json["resolved"], "system:Root");
        assert_eq!(json["origin_variant_index"], 0);
        assert_eq!(json["account"], serde_json::Value::Null);

        // A pallet origin two variants along — nothing about it is hardcoded.
        let (medium, json) = ctx
            .encode_origin(&OriginSpec::Variant {
                pallet: "Origins".into(),
                variant: "MediumSpender".into(),
            })
            .expect("pallet origin encodes");
        assert_eq!(
            TOriginCaller::decode(&mut &medium[..]).unwrap(),
            TOriginCaller::Origins(TGovOrigins::MediumSpender)
        );
        assert_eq!(json["resolved"], "Origins:MediumSpender");

        let who = [7u8; 32];
        let (signed, json) = ctx
            .encode_origin(&OriginSpec::Signed(who))
            .expect("signed encodes");
        assert_eq!(
            TOriginCaller::decode(&mut &signed[..]).unwrap(),
            TOriginCaller::system(TRawOrigin::Signed(TAccount(who)))
        );
        assert_eq!(signed.len(), 34, "two indices plus the account, no prefix");
        assert_eq!(json["account"], format!("0x{}", hex::encode(who)));
    }

    #[test]
    fn an_unknown_origin_is_refused_with_the_names_the_runtime_does_have() {
        let ctx = context();
        let err = ctx
            .encode_origin(&OriginSpec::Variant {
                pallet: "Origins".into(),
                variant: "MassiveSpender".into(),
            })
            .unwrap_err()
            .to_string();
        assert!(err.contains("MassiveSpender"), "{err}");
        assert!(err.contains("MediumSpender") && err.contains("Treasurer"), "{err}");

        let err = ctx
            .encode_origin(&OriginSpec::Variant {
                pallet: "Council".into(),
                variant: "Members".into(),
            })
            .unwrap_err()
            .to_string();
        assert!(err.contains("no origin 'Council'") && err.contains("Origins"), "{err}");
    }

    #[test]
    fn a_signed_origin_is_refused_on_a_chain_whose_accounts_are_not_32_bytes() {
        let ctx = context_with_origin::<TOriginCaller20>();
        let err = ctx
            .encode_origin(&OriginSpec::Signed([7u8; 32]))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("32-byte account"),
            "an AccountId20 chain must be refused, not sent 32 bytes: {err}"
        );
    }

    #[test]
    fn params_are_origin_then_call_then_the_xcm_version_by_declared_arity() {
        let ctx = context();
        let call = TRuntimeCall::System(TSystemCall::remark {
            remark: b"hi".to_vec(),
        })
        .encode();
        let origin = OriginSpec::Variant {
            pallet: "system".into(),
            variant: "Root".into(),
        };

        assert_eq!(ctx.arity(), 3, "the registered method takes result_xcms_version");
        let (params, _) = ctx.encode_params(&origin, &call, 4).unwrap();
        let mut expected = vec![0u8, 0u8];
        expected.extend_from_slice(&call);
        expected.extend_from_slice(&4u32.to_le_bytes());
        assert_eq!(params, expected, "u32 LE, never a compact");

        // The same code path against a v1 runtime: arity comes from metadata, so
        // the version simply is not appended.
        let v1 = DryRunContext::from_parts(
            ctx.types.clone(),
            vec![],
            ctx.origin_ty,
            ctx.call_ty,
            None,
            ctx.output_ty,
        );
        assert_eq!(v1.arity(), 2);
        let (params, _) = v1.encode_params(&origin, &call, 4).unwrap();
        assert_eq!(params.len(), 2 + call.len(), "no version on a v1 runtime");

        // And the call must really be this runtime's RuntimeCall.
        assert_eq!(ctx.decode_call(&call).unwrap().summary, "system.remark");
        assert!(ctx.decode_call(&[0xff, 0xff]).is_err());
    }

    fn effects(execution_result: Result<TPostInfo, TDispatchErrorWithPostInfo>) -> Vec<u8> {
        effects_with(
            execution_result,
            vec![
                TRuntimeEvent::System(TSystemEvent::ExtrinsicSuccess),
                TRuntimeEvent::Balances(TBalancesEvent::Transfer {
                    from: TAccount([1u8; 32]),
                    to: TAccount([2u8; 32]),
                    // > u64::MAX: must render as a decimal STRING, never a float
                    amount: 20_000_000_000_000_000_000_000u128,
                }),
            ],
        )
    }

    fn effects_with(
        execution_result: Result<TPostInfo, TDispatchErrorWithPostInfo>,
        emitted_events: Vec<TRuntimeEvent>,
    ) -> Vec<u8> {
        let out: TOutput = Ok(TEffects {
            execution_result,
            emitted_events,
            local_xcm: Some(TVersionedXcm::V4(vec![1, 2, 3])),
            forwarded_xcms: vec![(
                TVersionedLocation::V4 {
                    parents: 1,
                    parachain: 2034,
                },
                vec![TVersionedXcm::V4(vec![9])],
            )],
        });
        out.encode()
    }

    fn ok_post() -> TPostInfo {
        TPostInfo {
            actual_weight: Some(1_000),
            pays_fee: true,
        }
    }

    #[test]
    fn a_successful_dry_run_reads_as_events_named_the_way_core_events_names_them() {
        let ctx = context();
        let outcome = ctx.interpret(&effects(Ok(ok_post()))).expect("interprets");

        assert_eq!(outcome.status, SimStatus::Executed);
        assert_eq!(outcome.dispatch_ok, Some(true));
        assert!(outcome.dispatch_error.is_none());

        assert_eq!(outcome.events.len(), 2, "a one-element Vec is not a newtype");
        assert_eq!(outcome.events[0].name, "system.ExtrinsicSuccess");
        assert_eq!(outcome.events[1].name, "balances.Transfer");
        assert_eq!(
            outcome.events[1].data["amount"], "20000000000000000000000",
            "a u128 beyond u64 is a decimal string — the treasury-grid rule"
        );

        assert!(outcome.local_xcm.is_some(), "Some(xcm) is not None");
        assert_eq!(outcome.forwarded_xcms.len(), 1);
        assert_eq!(
            outcome.forwarded_xcms[0]["destination"]["V4"]["parachain"],
            2034
        );
        assert_eq!(
            outcome.forwarded_xcms[0]["messages"]
                .as_array()
                .expect("messages is a list")
                .len(),
            1
        );
        // the untouched decoded payload is kept whole beside the projection
        assert!(outcome.effects["Ok"].is_array());
    }

    #[test]
    fn a_failed_dispatch_is_a_result_and_names_the_module_error() {
        let ctx = context();
        let bytes = effects(Err(TDispatchErrorWithPostInfo {
            post_info: ok_post(),
            // pallet 50 = Assets, error 1 = NoAccount
            error: TDispatchError::Module(TModuleError {
                index: 50,
                error: [1, 0, 0, 0],
            }),
        }));
        let outcome = ctx.interpret(&bytes).expect("interprets");

        assert_eq!(
            outcome.status,
            SimStatus::DispatchFailed,
            "the runtime answered; the answer is that this would not work"
        );
        assert_eq!(outcome.dispatch_ok, Some(false));
        let err = outcome.dispatch_error.expect("carries the reason");
        assert_eq!(
            err["error"], "assets.NoAccount",
            "resolved through the runtime's own error metadata, not a lookup table"
        );
        assert!(err["raw"]["Module"].is_array(), "the raw shape is kept too");
        // events still come back: a failed dispatch can still have emitted some
        assert_eq!(outcome.events.len(), 2);

        // A self-naming variant needs no metadata at all.
        let bytes = effects(Err(TDispatchErrorWithPostInfo {
            post_info: ok_post(),
            error: TDispatchError::BadOrigin,
        }));
        let outcome = ctx.interpret(&bytes).expect("interprets");
        assert_eq!(outcome.dispatch_error.unwrap()["error"], "BadOrigin");

        // A nested self-naming error keeps its inner half: "Token" alone says
        // almost nothing beside "assets.NoAccount".
        let bytes = effects(Err(TDispatchErrorWithPostInfo {
            post_info: ok_post(),
            error: TDispatchError::Token(TTokenError::FundsUnavailable),
        }));
        let outcome = ctx.interpret(&bytes).expect("interprets");
        assert_eq!(
            outcome.dispatch_error.unwrap()["error"],
            "Token.FundsUnavailable"
        );

        // An unknown pallet index must not invent a name.
        let bytes = effects(Err(TDispatchErrorWithPostInfo {
            post_info: ok_post(),
            error: TDispatchError::Module(TModuleError {
                index: 99,
                error: [0, 0, 0, 0],
            }),
        }));
        let outcome = ctx.interpret(&bytes).expect("interprets");
        assert_eq!(
            outcome.dispatch_error.unwrap()["error"],
            serde_json::Value::Null,
            "unnameable is null, never a guess"
        );
    }

    #[test]
    fn a_batch_that_stopped_early_is_not_reported_as_a_clean_success() {
        let ctx = context();
        // utility.batch returns Ok and emits BatchInterrupted when an inner call
        // fails — the shape most governance proposals have.
        let bytes = effects_with(
            Ok(ok_post()),
            vec![
                TRuntimeEvent::System(TSystemEvent::ExtrinsicSuccess),
                TRuntimeEvent::Utility(TUtilityEvent::BatchInterrupted { index: 1 }),
            ],
        );
        let outcome = ctx.interpret(&bytes).expect("interprets");

        // The outer dispatch really did succeed, and we do not pretend otherwise…
        assert_eq!(outcome.status, SimStatus::Executed);
        assert_eq!(outcome.dispatch_ok, Some(true));
        // …but the row must not read as "this referendum would work".
        let note = outcome.note.expect("a half-applied batch must say so");
        assert!(note.contains("utility.BatchInterrupted"), "{note}");
        assert!(note.contains("INNER call"), "{note}");

        // and a clean batch stays clean — no note where there is nothing to say
        let clean = ctx.interpret(&effects(Ok(ok_post()))).expect("interprets");
        assert!(clean.note.is_none());
    }

    #[test]
    fn an_api_refusal_is_distinguishable_from_a_call_that_failed() {
        let ctx = context();
        let out: TOutput = Err(TApiError::Unimplemented);
        let outcome = ctx.interpret(&out.encode()).expect("interprets");
        assert_eq!(outcome.status, SimStatus::ApiError);
        assert_eq!(
            outcome.dispatch_ok, None,
            "we never got to try — that is not the same as failing, and NULL is how \
             the difference survives into the database"
        );
        assert!(outcome.note.unwrap().contains("Unimplemented"));
        assert!(outcome.events.is_empty());
    }

    #[test]
    fn a_response_that_does_not_fit_the_declared_output_is_a_loud_error() {
        let ctx = context();
        let mut bytes = effects(Ok(ok_post()));
        bytes.push(0xff);
        let err = ctx.interpret(&bytes).unwrap_err().to_string();
        assert!(err.contains("trailing"), "{err}");
        assert!(ctx.interpret(&[0xff, 0xff, 0xff]).is_err());
        assert!(ctx.interpret(&[]).is_err());
    }

    #[test]
    fn metadata_v14_is_refused_and_says_how_to_get_a_usable_blob() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/real/polkadot-asset-hub-19498783/metadata.scale");
        let Ok(blob) = std::fs::read(&path) else {
            eprintln!("SKIP: real fixture metadata not present");
            return;
        };
        let err = DryRunContext::from_metadata(&blob).unwrap_err().to_string();
        assert!(
            err.contains("v14") && err.contains("metadata_at_version"),
            "the refusal must name the fix, not just the problem: {err}"
        );
    }
}
