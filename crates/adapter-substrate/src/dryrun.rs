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
//! This file records what the runtime said, verbatim; attribution is a
//! difference against a no-op run at the same state, which slice 5 builds out of
//! `noop_call` below and `sim::attribute_forwarded`.
//!
//! ---------------------------------------------------------------------------
//! **PHASE 3, SLICE 5 — THE RECEIVING SIDE.** `dry_run_xcm(origin_location, xcm)`
//! is the other method on the same trait, and it answers the question this
//! file's own `not_covered` had to refuse: what the DESTINATION would do.
//!
//! THREE FACTS ABOUT IT, EACH VERIFIED AGAINST PINNED UPSTREAM SOURCE
//! (`xcm-runtime-apis` 0.4.0 / 0.6.0 / 0.7.0 `src/dry_run.rs`), because each one
//! would otherwise have been a guess with a plausible wrong answer:
//!   1. **Its signature is IDENTICAL in DryRunApi v1 and v2.** Only
//!      `dry_run_call` changed: v2's three-argument form is the plain
//!      declaration and the OLD two-argument one carries `#[changed_in(2)]`.
//!      `dry_run_xcm` is declared once and takes no version parameter at all, so
//!      there is no arity fork here and nothing to fold into the input hash
//!      beyond the two parameters — the VERSION of the program is inside its own
//!      bytes, because a `VersionedXcm` is a tagged enum. That version is also
//!      the version the ANSWER comes back in (`pallet_xcm` reads
//!      `xcm.identify_version()` and converts the forwarded messages into it),
//!      which is why a baseline must be built at the subject's version and not
//!      at the newest one.
//!   2. **`XcmDryRunEffects` has NO `local_xcm` field.** It is
//!      `{execution_result: Outcome, emitted_events, forwarded_xcms}` — three
//!      fields where `CallDryRunEffects` has four. Reading it with the call
//!      shape's field list would halt on a field that was never there.
//!   3. **`execution_result` is an XCM `Outcome`, not a dispatch `Result`**, and
//!      it has THREE states rather than two: `Complete{used}`,
//!      `Incomplete{used, error}` and `Error(..)`. The last one means execution
//!      NEVER STARTED — a barrier rejection — which is the single most valuable
//!      answer a receiving-side preview can give, and is exactly what a sender
//!      cannot see from its own chain. It is recorded as `not_started` rather
//!      than under upstream's own name, because "Error" reads as a failure of
//!      the request. And the SHAPE of those variants changed between XCM v4
//!      (`Incomplete{used, error: Error}`, `Error{error}`) and v5
//!      (`Incomplete{used, error: InstructionError{index, error}}`,
//!      `Error(InstructionError)`), so this file reads the VARIANT NAME — stable
//!      across both — and keeps the payload verbatim. Same device
//!      `xcm::facts_for_event` already uses on `polkadotXcm.Attempted`.
//!
//! AND ONE FACT THAT MAKES THE STITCH POSSIBLE AT ALL: `forwarded_xcms` carries
//! `VersionedXcm<()>` while `dry_run_xcm` wants `VersionedXcm<Call>` — and their
//! SCALE encodings are identical, because the only place the type parameter
//! appears is inside `DoubleEncoded<T>`, whose `decoded: Option<T>` field is
//! `#[codec(skip)]` and whose derive carries `#[codec(encode_bound())]`
//! (staging-xcm 24.0.0 `src/double_encoded.rs:29-33`). A message this chain
//! queued can therefore be handed straight to the next chain's `dry_run_xcm`.

use crate::calls::{self, DecodedCall};
use crate::frame_decoder::value_to_json;
use frame_metadata::{RuntimeMetadata, RuntimeMetadataPrefixed};
use parity_scale_codec::Decode;
use scale_info::{PortableRegistry, TypeDef, TypeDefPrimitive};
use scale_value::{Composite, Value, ValueDef};
use sim::{
    LocationSpec, OriginSpec, SimError, SimEvent, SimOutcome, SimStatus, XcmSimOutcome,
    XcmSimStatus,
};

/// Lineage for every row this runner writes. Bump when the INTERPRETATION of a
/// response changes; rows below it rebuild from the archived bytes.
///
/// **2 (slice 5)** — not because `dry_run_call`'s reading changed (it did not:
/// every column a v1 row carries still means exactly what it meant), but because
/// this runner now also writes `sim.xcm_simulations` rows and links a call row
/// to its baseline. One version stamp covers one runner, and a row that cannot
/// say which runner wrote it is not lineage. Rebuilding a v1 row from its
/// archived response would produce a byte-identical v2 row.
pub const DRY_RUN_VERSION: u32 = 2;

pub const DRY_RUN_API: &str = "DryRunApi";
pub const DRY_RUN_CALL_METHOD: &str = "dry_run_call";
pub const DRY_RUN_XCM_METHOD: &str = "dry_run_xcm";
/// The wire name is `Trait_method` (sp-api's `prefix_function_with_trait`).
pub const DRY_RUN_CALL_FUNCTION: &str = "DryRunApi_dry_run_call";
pub const DRY_RUN_XCM_FUNCTION: &str = "DryRunApi_dry_run_xcm";

/// The pallet and call the baseline no-op is built from, looked up BY NAME in
/// the runtime's own registry and never by index. `system.remark` is the
/// smallest call in FRAME that provably queues nothing and is dispatchable under
/// Root on every runtime we index — which is also the call the relay measurement
/// in slice 1 was made with, so the baseline is literally the experiment that
/// found the problem.
const NOOP_PALLET: &str = "System";
const NOOP_CALL: &str = "remark";

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
        let Some(pair) = entry.as_array() else {
            continue;
        };
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
    /// `dry_run_xcm`'s three type ids, or `None` on a runtime whose metadata
    /// does not declare the method. Upstream has declared it in every published
    /// version of the trait, so `None` is not expected — which is exactly why it
    /// is an Option and not an assumption.
    xcm: Option<XcmMethod>,
    /// v16 declares the API version in metadata; v15 does not. Informational —
    /// `RuntimeVersion.apis` remains the portable source.
    metadata_api_version: Option<u32>,
}

/// The `dry_run_xcm` method as this runtime declares it.
#[derive(Debug, Clone, Copy)]
struct XcmMethod {
    origin_location_ty: u32,
    program_ty: u32,
    output_ty: u32,
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
        // The second method on the same trait. ABSENT IS DATA, not an error:
        // this context is built for every simulation, and a chain that can dry
        // run calls but not XCM must still be able to dry run calls.
        let xcm: Option<(Vec<(String, u32)>, u32)> = api
            .methods
            .iter()
            .find(|f| f.name == DRY_RUN_XCM_METHOD)
            .map(|f| {
                (
                    f.inputs
                        .iter()
                        .map(|i| (i.name.to_string(), i.ty.id))
                        .collect(),
                    f.output.id,
                )
            });
        let pallets: Vec<(u8, String, Option<u32>)> = m
            .pallets
            .iter()
            .map(|p| {
                (
                    p.index,
                    p.name.to_string(),
                    p.error.as_ref().map(|e| e.ty.id),
                )
            })
            .collect();
        (m.types.clone(), pallets, inputs, method.output.id, xcm)
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
        let (types, pallets, inputs, output_ty, xcm_method) = match &prefixed.1 {
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

        // Same treatment for the XCM method, and it needs no version fork: the
        // declaration is byte-for-byte the same in DryRunApi v1 and v2. An
        // unfamiliar parameter list halts rather than being encoded positionally
        // — two Locations in the wrong order is a well-formed request that
        // previews a message from the wrong sender.
        let xcm = match xcm_method {
            None => None,
            Some((inputs, output)) => {
                let names: Vec<&str> = inputs.iter().map(|(n, _)| n.as_str()).collect();
                match names.as_slice() {
                    ["origin_location", "xcm"] => Some(XcmMethod {
                        origin_location_ty: inputs[0].1,
                        program_ty: inputs[1].1,
                        output_ty: output,
                    }),
                    other => {
                        return Err(format!(
                            "{DRY_RUN_XCM_METHOD} has parameters {other:?}, not the \
                             (origin_location, xcm) shape every published version of the trait \
                             declares — refusing to guess how to encode them"
                        ))
                    }
                }
            }
        };

        Ok(Self {
            types,
            pallets,
            origin_ty,
            call_ty,
            xcm_version_ty,
            output_ty,
            xcm,
            metadata_api_version,
        })
    }

    /// Build a context from parts. Used by Tier 2 and by tests, which register
    /// the shapes they need in a `scale_info::Registry` — the v15 walk above
    /// cannot be exercised offline, because every metadata blob in `fixtures/`
    /// is v14 by construction.
    ///
    /// The signature is unchanged from slice 1 on purpose: `dry_run_xcm`'s three
    /// type ids arrive through [`Self::with_xcm`] instead, so every existing
    /// call site keeps compiling and a context built without them is a
    /// call-only context rather than a half-initialised one.
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
            xcm: None,
            metadata_api_version: None,
        }
    }

    /// Attach `dry_run_xcm`'s declared type ids to a context built by
    /// [`Self::from_parts`].
    pub fn with_xcm(mut self, origin_location_ty: u32, program_ty: u32, output_ty: u32) -> Self {
        self.xcm = Some(XcmMethod {
            origin_location_ty,
            program_ty,
            output_ty,
        });
        self
    }

    /// Does this runtime's metadata declare `dry_run_xcm`?
    pub fn has_xcm(&self) -> bool {
        self.xcm.is_some()
    }

    fn xcm_method(&self) -> Result<XcmMethod, String> {
        self.xcm.ok_or_else(|| {
            format!(
                "this runtime's metadata declares {DRY_RUN_API} but no {DRY_RUN_XCM_METHOD} \
                 method, so the receiving side of a journey cannot be previewed on it"
            )
        })
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
        encode_origin_for(&self.types, self.origin_ty, origin)
    }
}

/// Resolve an origin expression against a runtime's own `OriginCaller` enum.
///
/// SPLIT OUT OF `DryRunContext` IN SLICE 8, and the reason is worth stating:
/// Tier 2 encodes an origin too, into `pallet_scheduler`'s `Scheduled.origin`,
/// and that field's TYPE ID comes from the scheduler's agenda rather than from
/// `dry_run_call`'s first parameter. Two implementations of "resolve
/// `<Pallet>:<Variant>` to bytes" that could disagree is the defect class this
/// project keeps finding (`api` depending on `sim` for `attribute_forwarded` was
/// the same call), so the type id became a parameter instead.
///
/// The consequence is a real one and not incidental: Tier 2 no longer needs the
/// runtime to declare `DryRunApi` at all. A chain that cannot be dry-run can
/// still be forked.
pub fn encode_origin_for(
    types: &PortableRegistry,
    origin_ty: u32,
    origin: &OriginSpec,
) -> Result<(Vec<u8>, serde_json::Value), String> {
    {
        let (pallet, variant, account) = match origin {
            OriginSpec::Variant { pallet, variant } => (pallet.as_str(), variant.as_str(), None),
            // `signed:…` is `system:Signed(who)` — the one origin with a field.
            OriginSpec::Signed(who) => ("system", "Signed", Some(who)),
        };

        let outer = variants_in(types, origin_ty, "the runtime's OriginCaller")?;
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
        let inner = variants_in(types, inner_ty, &format!("origin '{}'", outer_variant.name))?;
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
                if !is_32_byte_account_in(types, field_ty) {
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
}

impl DryRunContext {
    // ------------------------------------------------------ the baseline call

    /// The SCALE bytes of `system.remark()` with an empty payload — the no-op
    /// whose `forwarded_xcms` is the ambient queue at a state.
    ///
    /// Built from the runtime's own `RuntimeCall` enum by NAME, exactly like an
    /// origin: two variant indices and a compact zero. Nothing is hardcoded, so
    /// a runtime that spells its system pallet differently is refused with the
    /// names it does have rather than sent three bytes that mean something else.
    ///
    /// AND THE SHAPE IS CHECKED, not just the name. A call named `remark` whose
    /// single argument is not a byte sequence is a different call, and building
    /// a "no-op" out of it would silently make the baseline a real transaction.
    /// The last line decodes what was built and refuses anything that does not
    /// read back as `system.remark`.
    pub fn noop_call(&self) -> Result<Vec<u8>, SimError> {
        self.noop_call_inner().map_err(SimError::Encode)
    }

    fn noop_call_inner(&self) -> Result<Vec<u8>, String> {
        let pallets = self.variants_of(self.call_ty, "the runtime's RuntimeCall")?;
        let pallet = find_variant(pallets, NOOP_PALLET).ok_or_else(|| {
            format!(
                "no '{NOOP_PALLET}' pallet in this runtime's RuntimeCall, so no baseline no-op \
                 can be built; it has: {}",
                variant_names(pallets)
            )
        })?;
        let inner_ty = match pallet.fields.len() {
            1 => pallet.fields[0].ty.id,
            n => {
                return Err(format!(
                    "RuntimeCall variant '{}' holds {n} fields, not one pallet Call enum",
                    pallet.name
                ))
            }
        };
        let calls = self.variants_of(inner_ty, &format!("pallet '{}'", pallet.name))?;
        let call = find_variant(calls, NOOP_CALL).ok_or_else(|| {
            format!(
                "'{NOOP_PALLET}' has no '{NOOP_CALL}' call in this runtime; it has: {}",
                variant_names(calls)
            )
        })?;
        match call.fields.len() {
            1 if self.is_byte_sequence(call.fields[0].ty.id) => {}
            _ => {
                return Err(format!(
                    "'{}.{}' does not take a single byte sequence on this runtime, so it is not \
                     the no-op this baseline needs",
                    pallet.name, call.name
                ))
            }
        }

        // compact(0) is a single zero byte: the empty Vec<u8> argument.
        let bytes = vec![pallet.index, call.index, 0u8];
        let decoded = calls::decode_call_with(&self.types, self.call_ty, &bytes)
            .map_err(|e| format!("the baseline no-op we built does not decode: {e}"))?;
        let want = format!("{NOOP_PALLET}.{NOOP_CALL}").to_ascii_lowercase();
        if decoded.summary.to_ascii_lowercase() != want {
            return Err(format!(
                "the baseline no-op we built reads back as '{}', not '{want}' — refusing to \
                 dispatch it",
                decoded.summary
            ));
        }
        Ok(bytes)
    }

    // ------------------------------------------------------------ dry_run_xcm

    /// An origin LOCATION, built from the registry-resolved relationship between
    /// two chains and encoded against the type the runtime declares for
    /// `origin_location`.
    ///
    /// Returns the bytes and the location as the runtime's own registry renders
    /// it — obtained by DECODING what was just encoded, so the JSON in the
    /// recorded row is never a second hand-built copy of the same guess and a
    /// shape the runtime would reject cannot be filed as one it accepted.
    ///
    /// WHY THIS IS BUILT RATHER THAN COPIED FROM THE SENDER: the sender's
    /// `forwarded_xcms` names the DESTINATION in the sender's frame, and the
    /// receiver needs the SENDER in the receiver's frame — the mirror image.
    /// Asset Hub addressing `{parents:1, X1[Parachain(2034)]}` arrives on
    /// Hydration as `{parents:1, X1[Parachain(1000)]}`, and handing the
    /// destination back unchanged would preview a message a chain sent to
    /// itself. The mirror is computed from registry data (para ids and relay
    /// membership), which is the same derivation `counterparty_mirror` already
    /// checks observed journeys with.
    ///
    /// The VERSION is the newest `V<n>` variant the runtime declares, read from
    /// the registry rather than pinned: a receiving runtime always understands
    /// its own latest, while an older one risks a `VersionedConversionFailed`
    /// that says nothing about the message.
    pub fn encode_location(
        &self,
        spec: &LocationSpec,
    ) -> Result<(Vec<u8>, serde_json::Value), SimError> {
        self.encode_location_inner(spec).map_err(SimError::Encode)
    }

    fn encode_location_inner(
        &self,
        spec: &LocationSpec,
    ) -> Result<(Vec<u8>, serde_json::Value), String> {
        let m = self.xcm_method()?;
        let variants = self.variants_of(m.origin_location_ty, "VersionedLocation")?;
        let version = newest_version_variant(variants).ok_or_else(|| {
            format!(
                "the origin_location type declares no V<n> variant; it has: {}",
                variant_names(variants)
            )
        })?;

        let junction = Value::variant(
            "Parachain",
            Composite::Unnamed(vec![
                Value::u128(spec.para_id().unwrap_or_default() as u128),
            ]),
        );
        let interior = match spec.para_id() {
            // X1's PAYLOAD IS ITS OWN LAYER, and getting that wrong does not
            // fail at the boundary — it fails deep inside scale-encode with a
            // shape error. From XCM v4 the field is `[Junction; 1]`, so the
            // value handed to it must be a COMPOSITE (which scale-value's
            // encoder length-checks against the array in `visit_array`), not the
            // junction variant itself: `encode_variant` has no array arm and no
            // fallback, so a bare variant there can never encode. XCM v3's
            // `X1(Junction)` accepts the same one-element composite through the
            // encoder's peel-one-value arm, so one construction serves both.
            //
            // The project's own rendering says the same thing from the other
            // side: a decoded X1 reads `{"X1": [[{"Parachain": [2034]}]]}` —
            // two layers, not one.
            Some(_) => Value::variant(
                "X1",
                Composite::Unnamed(vec![Value::unnamed_composite(vec![junction])]),
            ),
            None => Value::variant("Here", Composite::Unnamed(vec![])),
        };
        let location = Value::named_composite(vec![
            ("parents".to_string(), Value::u128(spec.parents() as u128)),
            ("interior".to_string(), interior),
        ]);
        let versioned = Value::variant(version.name.clone(), Composite::Unnamed(vec![location]));

        let mut bytes = Vec::new();
        scale_value::scale::encode_as_type(
            &versioned,
            m.origin_location_ty,
            &self.types,
            &mut bytes,
        )
        .map_err(|e| format!("encoding origin location {}: {e}", spec.as_token()))?;

        // Decode it straight back. This is the check AND the rendering: bytes
        // that do not read as a location are refused here rather than sent, and
        // what the row records is the runtime's own shape.
        let mut cursor = &bytes[..];
        let value =
            scale_value::scale::decode_as_type(&mut cursor, m.origin_location_ty, &self.types)
                .map_err(|e| format!("the origin location we encoded does not decode back: {e}"))?;
        if !cursor.is_empty() {
            return Err(format!(
                "{} trailing bytes after re-reading the origin location we built",
                cursor.len()
            ));
        }
        Ok((bytes, json_of(&value)))
    }

    /// The SCALE bytes of an EMPTY `VersionedXcm` **at a named XCM version** —
    /// the receiving side's no-op.
    ///
    /// A program with no instructions executes nothing, so whatever the runtime
    /// reports as forwarded beside it was already in flight. Same role as
    /// `noop_call` on the sending side, and the same reason: without it, a
    /// previewed hop on a chain whose router enumerates ambient queues would
    /// report other people's traffic as its own onward legs.
    ///
    /// THE VERSION IS A PARAMETER AND NOT A CHOICE, which is the one thing about
    /// this function that is not obvious. `pallet_xcm::dry_run_xcm` renders its
    /// WHOLE ANSWER in the version of the program it was given — it reads
    /// `xcm.identify_version()` and converts the router's messages into it — so
    /// a baseline built at the newest version while the subject arrived as V4
    /// produces two forwarded lists that render differently and can never be
    /// differenced. Every ambient message would then be attributed to the run,
    /// which is precisely the defect this baseline exists to prevent, one hop
    /// along. Worse, converting an ambient V5 message down for a V4 subject can
    /// fail outright, so the two runs would not even agree on succeeding.
    ///
    /// Two bytes on today's runtimes (a version index and a compact zero), but
    /// they are BUILT and then CHECKED: the encoding is decoded back and must
    /// read as an empty instruction list. A "no-op" that turned out to execute
    /// something would subtract this run's own messages from its own
    /// attribution, which is the one failure mode a baseline must not have.
    pub fn encode_empty_program(&self, xcm_version: u32) -> Result<Vec<u8>, SimError> {
        self.encode_empty_program_inner(xcm_version)
            .map_err(SimError::Encode)
    }

    fn encode_empty_program_inner(&self, xcm_version: u32) -> Result<Vec<u8>, String> {
        let m = self.xcm_method()?;
        let variants = self.variants_of(m.program_ty, "VersionedXcm")?;
        let want = format!("V{xcm_version}");
        let version = find_variant(variants, &want).ok_or_else(|| {
            format!(
                "this runtime's VersionedXcm has no {want} variant, so no baseline can be \
                 built at the subject program's own version; it has: {}",
                variant_names(variants)
            )
        })?;
        // `Xcm` is a newtype over `Vec<Instruction>`; an empty unnamed composite
        // encodes into it as a compact zero, and scale-encode's newtype
        // unwrapping handles the layer between.
        let empty = Value::unnamed_composite(Vec::<Value<()>>::new());
        let versioned = Value::variant(version.name.clone(), Composite::Unnamed(vec![empty]));

        let mut bytes = Vec::new();
        scale_value::scale::encode_as_type(&versioned, m.program_ty, &self.types, &mut bytes)
            .map_err(|e| format!("encoding the empty baseline program: {e}"))?;

        let mut cursor = &bytes[..];
        let value = scale_value::scale::decode_as_type(&mut cursor, m.program_ty, &self.types)
            .map_err(|e| format!("the empty program we built does not decode back: {e}"))?;
        if !cursor.is_empty() {
            return Err(format!(
                "{} trailing bytes after re-reading the empty program",
                cursor.len()
            ));
        }
        match instruction_list(&json_of(&value)) {
            Some(list) if list.is_empty() => Ok(bytes),
            _ => Err(
                "the program we built as a baseline does not read back as an empty instruction \
                 list — refusing to use it, because a baseline that executes anything would \
                 subtract a run's own messages from its own attribution"
                    .into(),
            ),
        }
    }

    /// `origin_location ++ xcm`. Plain concatenation of two already-encoded
    /// parameters, with no version byte appended: `dry_run_xcm` has taken
    /// exactly these two arguments in every published version of the trait.
    pub fn encode_xcm_params(
        &self,
        origin_location: &[u8],
        program: &[u8],
    ) -> Result<Vec<u8>, SimError> {
        self.xcm_method().map_err(SimError::Encode)?;
        let mut params = Vec::with_capacity(origin_location.len() + program.len());
        params.extend_from_slice(origin_location);
        params.extend_from_slice(program);
        Ok(params)
    }

    /// Decode a `VersionedXcm` against the type `dry_run_xcm` declares for its
    /// `xcm` parameter — which doubles as the check that the bytes a caller
    /// pasted are a program this runtime could accept at all.
    pub fn decode_program(&self, program: &[u8]) -> Result<serde_json::Value, SimError> {
        let m = self.xcm_method().map_err(SimError::Encode)?;
        let mut cursor = program;
        let value = scale_value::scale::decode_as_type(&mut cursor, m.program_ty, &self.types)
            .map_err(|e| {
                SimError::Encode(format!(
                    "these bytes are not a VersionedXcm for this runtime: {e}"
                ))
            })?;
        if !cursor.is_empty() {
            return Err(SimError::Encode(format!(
                "{} trailing bytes after the XCM program — truncated or not a program at all",
                cursor.len()
            )));
        }
        Ok(json_of(&value))
    }

    /// One message out of a recorded CALL simulation's `forwarded_xcms`, as
    /// BYTES that can be handed to the destination chain's `dry_run_xcm`.
    ///
    /// THE BYTES ARE RE-ENCODED, NOT SLICED, and that is worth stating plainly
    /// because it is the one place in this project where a wire artifact is
    /// reconstructed rather than kept. `scale_value`'s decoder annotates every
    /// node with the type id it was decoded against (the same property
    /// `calls.rs` detects nested calls by), so a forwarded message can be
    /// re-encoded against its own declared type — and the result is verified by
    /// decoding it again and requiring the rendering to match, byte range and
    /// all. A re-encoding that does not round-trip is refused rather than sent.
    ///
    /// It reads the ARCHIVED RESPONSE, not the database row: the row holds our
    /// JSON rendering, and re-encoding from a rendering would be a guess about
    /// what the rendering dropped. The bytes are the evidence, which is why they
    /// were archived.
    ///
    /// `VersionedXcm<()>` here becomes `VersionedXcm<Call>` there, and the two
    /// encode identically — see the module header on `DoubleEncoded`.
    pub fn forwarded_program(
        &self,
        call_response: &[u8],
        destination_index: usize,
        message_index: usize,
    ) -> Result<ForwardedProgram, SimError> {
        self.forwarded_program_inner(call_response, destination_index, message_index)
            .map_err(SimError::Decode)
    }

    fn forwarded_program_inner(
        &self,
        call_response: &[u8],
        destination_index: usize,
        message_index: usize,
    ) -> Result<ForwardedProgram, String> {
        let mut cursor = call_response;
        let value = scale_value::scale::decode_as_type(&mut cursor, self.output_ty, &self.types)
            .map_err(|e| format!("archived dry-run response decode: {e}"))?;
        let ValueDef::Variant(outer) = &value.value else {
            return Err("archived response is not a Result variant".into());
        };
        if outer.name != "Ok" {
            return Err(format!(
                "the archived response is `{}`, which carries no forwarded messages",
                outer.name
            ));
        }
        let effects = variant_inner(outer).ok_or("dry-run Ok carries no effects payload")?;
        let forwarded =
            named(effects, "forwarded_xcms").ok_or("effects has no forwarded_xcms field")?;
        let ValueDef::Composite(Composite::Unnamed(entries)) = &forwarded.value else {
            return Err("forwarded_xcms is not a sequence".into());
        };
        let entry = entries.get(destination_index).ok_or_else(|| {
            format!(
                "forwarded_xcms has {} destination(s); there is no index {destination_index}",
                entries.len()
            )
        })?;
        let ValueDef::Composite(Composite::Unnamed(parts)) = &entry.value else {
            return Err("a forwarded_xcms entry is not a (destination, messages) pair".into());
        };
        let (Some(dest), Some(messages)) = (parts.first(), parts.get(1)) else {
            return Err("a forwarded_xcms entry has fewer than two parts".into());
        };
        let ValueDef::Composite(Composite::Unnamed(list)) = &messages.value else {
            return Err("a forwarded_xcms entry's messages are not a sequence".into());
        };
        let message = list.get(message_index).ok_or_else(|| {
            format!(
                "destination {destination_index} carries {} message(s); there is no index \
                 {message_index}",
                list.len()
            )
        })?;

        // `context` IS the type id this node was decoded against.
        let program_ty = message.context;
        let mut bytes = Vec::new();
        scale_value::scale::encode_as_type(message, program_ty, &self.types, &mut bytes).map_err(
            |e| format!("re-encoding forwarded message {destination_index}/{message_index}: {e}"),
        )?;

        let mut check = &bytes[..];
        let round_trip = scale_value::scale::decode_as_type(&mut check, program_ty, &self.types)
            .map_err(|e| format!("the re-encoded forwarded message does not decode back: {e}"))?;
        if !check.is_empty() {
            return Err(format!(
                "{} trailing bytes after re-reading the message we re-encoded",
                check.len()
            ));
        }
        let program = json_of(message);
        if json_of(&round_trip) != program {
            return Err(
                "re-encoding this forwarded message did not round-trip — refusing to preview \
                 bytes that are not the message the runtime reported"
                    .into(),
            );
        }

        Ok(ForwardedProgram {
            destination: json_of(dest),
            destination_index,
            message_index,
            program,
            bytes,
        })
    }

    /// `dry_run_xcm` response bytes → outcome.
    ///
    /// Reads `XcmDryRunEffects`, which is NOT `CallDryRunEffects`: three fields,
    /// no `local_xcm`, and an `execution_result` that is an XCM `Outcome` with
    /// three states rather than a dispatch `Result` with two.
    pub fn interpret_xcm(&self, response: &[u8]) -> Result<XcmSimOutcome, SimError> {
        self.interpret_xcm_inner(response).map_err(SimError::Decode)
    }

    fn interpret_xcm_inner(&self, response: &[u8]) -> Result<XcmSimOutcome, String> {
        let m = self.xcm_method()?;
        let mut cursor = response;
        let value = scale_value::scale::decode_as_type(&mut cursor, m.output_ty, &self.types)
            .map_err(|e| format!("dry-run-xcm response decode: {e}"))?;
        if !cursor.is_empty() {
            return Err(format!(
                "{} trailing bytes after the dry-run-xcm response — the declared output type \
                 and the bytes disagree",
                cursor.len()
            ));
        }
        let effects_json = json_of(&value);

        let ValueDef::Variant(outer) = &value.value else {
            return Err("dry-run-xcm response is not a Result variant".into());
        };
        match outer.name.as_str() {
            "Err" => {
                let reason = variant_inner(outer)
                    .and_then(|v| match &v.value {
                        ValueDef::Variant(e) => Some(e.name.clone()),
                        _ => None,
                    })
                    .unwrap_or_else(|| "unknown".into());
                Ok(XcmSimOutcome {
                    status: XcmSimStatus::ApiError,
                    weight_used: None,
                    xcm_error: None,
                    events: vec![],
                    forwarded_xcms: vec![],
                    effects: effects_json,
                    note: Some(format!(
                        "the runtime's dry-run API refused the request ({reason}); the program \
                         was never executed"
                    )),
                })
            }
            "Ok" => {
                let effects = variant_inner(outer).ok_or("dry-run-xcm Ok carries no payload")?;
                let execution = named(effects, "execution_result")
                    .ok_or("effects has no execution_result field")?;
                let ValueDef::Variant(outcome) = &execution.value else {
                    return Err("execution_result is not an Outcome variant".into());
                };

                // ONLY THE VARIANT NAME IS READ. The payloads changed shape
                // between XCM v4 and v5 (Incomplete's `error` became an
                // InstructionError, and Error went from a named field to a
                // newtype), and the names did not — the same reason
                // `xcm::facts_for_event` reads `Attempted` this way.
                let (status, note) = match outcome.name.as_str() {
                    "Complete" => (XcmSimStatus::Complete, None),
                    "Incomplete" => (
                        XcmSimStatus::Incomplete,
                        Some(
                            "the program STARTED and did not finish — the arrival would be \
                             recorded as processed, with success=false, exactly like an \
                             observed Outcome::Incomplete"
                                .to_string(),
                        ),
                    ),
                    // Upstream names this variant `Error`. It is not an error of
                    // ours and not an API failure: the program never began.
                    "Error" => (
                        XcmSimStatus::NotStarted,
                        Some(
                            "execution NEVER STARTED (Outcome::Error) — typically a barrier \
                             rejection or a version the receiver cannot read. The sending chain \
                             cannot see this: it would still record a successful send"
                                .to_string(),
                        ),
                    ),
                    other => {
                        return Err(format!(
                            "execution_result variant '{other}' is none of Complete, Incomplete \
                             or Error — the Outcome enum has changed and this reading must be \
                             re-checked rather than guessed at"
                        ))
                    }
                };

                let weight_used = variant_field(outcome, "used").map(json_of);
                // v4: `Incomplete { used, error }` / `Error { error }` — a named
                // field. v5: `Error(InstructionError)` — a newtype. Both, in
                // that order, and neither invented.
                let xcm_error = match status {
                    XcmSimStatus::Complete | XcmSimStatus::ApiError => None,
                    _ => variant_field(outcome, "error")
                        .or_else(|| variant_inner(outcome))
                        .map(json_of),
                };

                let events_value = named(effects, "emitted_events")
                    .ok_or("effects has no emitted_events field")?;
                let events = self.read_events(events_value)?;
                let forwarded = named(effects, "forwarded_xcms")
                    .ok_or("effects has no forwarded_xcms field")?;
                let forwarded_xcms = read_forwarded(forwarded)?;

                Ok(XcmSimOutcome {
                    status,
                    weight_used,
                    xcm_error,
                    events,
                    forwarded_xcms,
                    effects: effects_json,
                    note,
                })
            }
            other => Err(format!(
                "dry-run-xcm response variant '{other}' is neither Ok nor Err"
            )),
        }
    }

    /// Is this type a `Vec<u8>`? Guards the baseline no-op's one argument.
    fn is_byte_sequence(&self, ty_id: u32) -> bool {
        let Some(ty) = self.types.resolve(ty_id) else {
            return false;
        };
        match &ty.type_def {
            TypeDef::Sequence(s) => matches!(
                self.types.resolve(s.type_param.id).map(|t| &t.type_def),
                Some(TypeDef::Primitive(TypeDefPrimitive::U8))
            ),
            _ => false,
        }
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
                let effects =
                    variant_inner(outer).ok_or("dry-run Ok carries no effects payload")?;

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
                    other => {
                        return Err(format!(
                            "execution_result variant '{other}' is neither Ok nor Err"
                        ))
                    }
                };

                let events_value = named(effects, "emitted_events")
                    .ok_or("effects has no emitted_events field")?;
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
                let note =
                    (status == SimStatus::Executed && !inner_failures.is_empty()).then(|| {
                        format!(
                            "the call dispatched successfully but an INNER call did not — {} \
                             present. utility.batch returns Ok when it stops early, so the \
                             status is about the outer call only; read emitted_events",
                            inner_failures.join(" + ")
                        )
                    });

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
                    return Err(format!(
                        "{}: inner value is not an event variant",
                        pallet.name
                    ));
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
                    (Some(i), Some(e)) if i <= u8::MAX as u64 => self.module_error_name(i as u8, e),
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
        variants_in(&self.types, ty_id, what)
    }

    fn is_u32(&self, ty_id: u32) -> bool {
        matches!(
            self.types.resolve(ty_id).map(|t| &t.type_def),
            Some(TypeDef::Primitive(TypeDefPrimitive::U32))
        )
    }
}

/// The variants of an enum type, or a message naming what was expected.
/// Free-standing so `fork.rs` can reach the origin encoder without a
/// `DryRunContext` (which requires the runtime to declare `DryRunApi`).
fn variants_in<'a>(
    types: &'a PortableRegistry,
    ty_id: u32,
    what: &str,
) -> Result<&'a [scale_info::Variant<scale_info::form::PortableForm>], String> {
    let ty = types
        .resolve(ty_id)
        .ok_or_else(|| format!("{what}: type {ty_id} is not in the registry"))?;
    match &ty.type_def {
        TypeDef::Variant(v) => Ok(&v.variants),
        _ => Err(format!("{what}: type {ty_id} is not an enum")),
    }
}

/// Is this type an `AccountId32`? Walks single-field newtypes down to the array.
fn is_32_byte_account_in(types: &PortableRegistry, ty_id: u32) -> bool {
    let mut id = ty_id;
    for _ in 0..4 {
        let Some(ty) = types.resolve(id) else {
            return false;
        };
        match &ty.type_def {
            TypeDef::Array(a) => {
                return a.len == 32
                    && matches!(
                        types.resolve(a.type_param.id).map(|t| &t.type_def),
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

// ------------------------------------------------------------------- helpers

/// One message lifted out of a recorded call simulation's `forwarded_xcms`,
/// ready to be previewed on the chain it is addressed to.
#[derive(Debug, Clone)]
pub struct ForwardedProgram {
    /// The destination as the SENDER addressed it — not the origin the receiver
    /// will be told, which is its mirror image.
    pub destination: serde_json::Value,
    pub destination_index: usize,
    pub message_index: usize,
    pub program: serde_json::Value,
    /// Re-encoded and round-trip verified — see `forwarded_program`.
    pub bytes: Vec<u8>,
}

/// The highest `V<n>` variant an enum declares (`VersionedLocation`,
/// `VersionedXcm`). Read from the registry so the choice follows the runtime
/// rather than a constant that ages.
fn newest_version_variant(
    variants: &[scale_info::Variant<scale_info::form::PortableForm>],
) -> Option<&scale_info::Variant<scale_info::form::PortableForm>> {
    variants
        .iter()
        .filter_map(|v| {
            v.name
                .strip_prefix('V')
                .and_then(|d| d.parse::<u32>().ok())
                .map(|n| (n, v))
        })
        .max_by_key(|(n, _)| *n)
        .map(|(_, v)| v)
}

/// A NAMED field of a variant (`Complete { used }`). Returns `None` for a
/// newtype variant, which is what makes the v4/v5 `Outcome::Error` fallback in
/// `interpret_xcm` an either/or rather than a guess.
fn variant_field<'a>(v: &'a scale_value::Variant<u32>, name: &str) -> Option<&'a Value<u32>> {
    match &v.values {
        Composite::Named(items) => items.iter().find(|(n, _)| n == name).map(|(_, v)| v),
        Composite::Unnamed(_) => None,
    }
}

/// "WithdrawAsset → BuyExecution → DepositAsset" from a decoded `VersionedXcm`.
///
/// List-view text only — the authority is always the `program` column. The peel
/// is the same one `xcm::instructions` documents: `Xcm` is a newtype over
/// `Vec<Instruction>`, so an empty program renders as `[[]]` and reading the
/// outer array's length would call every program non-empty.
pub fn program_summary(program: &serde_json::Value) -> String {
    const SHOWN: usize = 6;
    let Some(list) = instruction_list(program) else {
        return "(unreadable program)".into();
    };
    if list.is_empty() {
        return "(empty program)".into();
    }
    let names: Vec<&str> = list
        .iter()
        .take(SHOWN)
        .map(|i| {
            i.as_object()
                .filter(|m| m.len() == 1)
                .and_then(|m| m.keys().next())
                .map(|s| s.as_str())
                .unwrap_or("?")
        })
        .collect();
    let mut text = names.join(" → ");
    if list.len() > SHOWN {
        text.push_str(&format!(" → +{} more", list.len() - SHOWN));
    }
    text
}

/// The XCM version a decoded `VersionedXcm` carries — the `V<n>` key it renders
/// under. `None` when the shape is not a single-key version wrapper, which is
/// the honest answer for bytes that are not a versioned program.
///
/// It exists because the BASELINE must be built at this exact version (see
/// [`DryRunContext::encode_empty_program`]), and reading it from the decoded
/// program is the only place the answer is unambiguous: the encoded bytes' first
/// byte is the codec INDEX, which happens to equal the version today and is not
/// promised to.
pub fn program_version(program: &serde_json::Value) -> Option<u32> {
    let map = program.as_object()?;
    if map.len() != 1 {
        return None;
    }
    map.keys().next()?.strip_prefix('V')?.parse().ok()
}

/// The instruction list inside a decoded `VersionedXcm`.
///
/// THE NESTING IS DEEPER HERE THAN ANYWHERE ELSE IN THE PROJECT, and counting it
/// wrong is how a program silently reads as one instruction called "V5".
/// `VersionedXcm::V5(Xcm(Vec<Instruction>))` is a newtype VARIANT over a newtype
/// STRUCT over a Vec, and this decoder renders each of those layers, so the JSON
/// is `{"V5": [[[…instructions…]]]}` — one object key and then THREE array
/// layers, where `xcm::instructions` (which reads a bare `Xcm`, not a versioned
/// one) sees only two.
///
/// So the peel is a bounded loop rather than a fixed count: strip the version
/// key, then keep unwrapping single-element arrays whose only element is itself
/// an array. It terminates on the real list because an XCM instruction is an
/// enum variant and renders as an OBJECT — never as a bare array — which is the
/// same argument `xcm::instructions` rests on, applied repeatedly.
fn instruction_list(program: &serde_json::Value) -> Option<&Vec<serde_json::Value>> {
    let mut cur = match program.as_object() {
        Some(map) if map.len() == 1 => map.values().next()?,
        _ => program,
    };
    for _ in 0..4 {
        let items = cur.as_array()?;
        match items.as_slice() {
            [only] if only.is_array() => cur = only,
            _ => return cur.as_array(),
        }
    }
    cur.as_array()
}

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
fn variant_inner(v: &scale_value::Variant<u32>) -> Option<&Value<u32>> {
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
        ValueDef::Composite(Composite::Unnamed(items)) => items
            .first()
            .and_then(value_as_u64)
            .and_then(|n| u8::try_from(n).ok()),
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
        // The FORWARDED messages are real programs, not opaque bytes, because
        // the re-encoding test below is only worth anything against a shape with
        // nesting in it — a version wrapper, an Xcm newtype, and a DoubleEncoded
        // inside an instruction.
        forwarded_xcms: Vec<(TVersionedLocation, Vec<TProgram>)>,
    }

    // ------------------------------------------------- the dry_run_xcm shapes
    // Mimics of the real types, close enough that the assertions are about
    // encodings rather than about our own fixtures: compact para ids, X1 as a
    // one-element ARRAY (as XCM v4/v5 declare it), `DoubleEncoded` carrying only
    // its `encoded` field, and BOTH Outcome shapes.

    #[derive(Encode, Decode, TypeInfo, Debug, PartialEq)]
    enum TJunction {
        Parachain(#[codec(compact)] u32),
    }

    #[derive(Encode, Decode, TypeInfo, Debug, PartialEq)]
    enum TJunctions {
        Here,
        X1([TJunction; 1]),
    }

    #[derive(Encode, Decode, TypeInfo, Debug, PartialEq)]
    struct TLocation {
        parents: u8,
        interior: TJunctions,
    }

    #[derive(Encode, Decode, TypeInfo, Debug, PartialEq)]
    enum TVersionedLoc {
        #[codec(index = 3)]
        V3(TLocation),
        #[codec(index = 4)]
        V4(TLocation),
        #[codec(index = 5)]
        V5(TLocation),
    }

    /// Codec-identical to the real `DoubleEncoded<T>`, whose `decoded` field is
    /// `#[codec(skip)]` — which is why `VersionedXcm<()>` and
    /// `VersionedXcm<Call>` encode the same and a forwarded message can be
    /// handed to another chain's dry_run_xcm.
    #[derive(Encode, Decode, TypeInfo, Debug, PartialEq, Clone)]
    struct TDoubleEncoded {
        encoded: Vec<u8>,
    }

    #[derive(Encode, Decode, TypeInfo, Debug, PartialEq, Clone)]
    enum TInstruction {
        ClearOrigin,
        Transact { call: TDoubleEncoded },
        SetTopic([u8; 32]),
    }

    /// `Xcm` is a NEWTYPE over the instruction list — the layer that makes an
    /// empty program render as `[[]]` and never as `[]`.
    #[derive(Encode, Decode, TypeInfo, Debug, PartialEq, Clone)]
    struct TXcm(Vec<TInstruction>);

    #[derive(Encode, Decode, TypeInfo, Debug, PartialEq, Clone)]
    enum TProgram {
        #[codec(index = 4)]
        V4(TXcm),
        #[codec(index = 5)]
        V5(TXcm),
    }

    #[derive(Encode, Decode, TypeInfo, Debug, PartialEq)]
    struct TWeight {
        ref_time: u64,
        proof_size: u64,
    }

    #[derive(Encode, Decode, TypeInfo, Debug, PartialEq)]
    enum TXcmError {
        Barrier,
        UntrustedReserveLocation,
    }

    #[derive(Encode, Decode, TypeInfo, Debug, PartialEq)]
    struct TInstructionError {
        index: u8,
        error: TXcmError,
    }

    /// XCM v5: `Error` is a NEWTYPE over InstructionError.
    #[derive(Encode, Decode, TypeInfo)]
    enum TOutcomeV5 {
        Complete {
            used: TWeight,
        },
        Incomplete {
            used: TWeight,
            error: TInstructionError,
        },
        Error(TInstructionError),
    }

    /// XCM v4: `Error` is a STRUCT variant and `Incomplete.error` is a bare
    /// Error. Same three names, different payloads — which is why only the name
    /// is read.
    #[derive(Encode, Decode, TypeInfo)]
    enum TOutcomeV4 {
        Complete { used: TWeight },
        Incomplete { used: TWeight, error: TXcmError },
        Error { error: TXcmError },
    }

    /// `XcmDryRunEffects` — THREE fields, and no `local_xcm`.
    #[derive(Encode, Decode, TypeInfo)]
    struct TXcmEffects<O> {
        execution_result: O,
        emitted_events: Vec<TRuntimeEvent>,
        forwarded_xcms: Vec<(TVersionedLocation, Vec<TProgram>)>,
    }

    #[derive(Encode, Decode, TypeInfo)]
    enum TApiError {
        Unimplemented,
        VersionedConversionFailed,
    }

    type TOutput = Result<TEffects, TApiError>;
    type TXcmOutput<O> = Result<TXcmEffects<O>, TApiError>;

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

    /// A context that also declares `dry_run_xcm`, with the Outcome shape of
    /// whichever XCM version the test is about. Everything is registered in ONE
    /// registry, because the type ids the two methods hand each other (a
    /// forwarded message's own id, for instance) only mean anything within one.
    fn xcm_context<O: TypeInfo + 'static>() -> DryRunContext {
        let mut registry = Registry::new();
        let origin = registry.register_type(&MetaType::new::<TOriginCaller>()).id;
        let call = registry.register_type(&MetaType::new::<TRuntimeCall>()).id;
        let xcm_version = registry.register_type(&MetaType::new::<u32>()).id;
        let output = registry.register_type(&MetaType::new::<TOutput>()).id;
        let assets_error = registry.register_type(&MetaType::new::<TAssetsError>()).id;
        let location = registry.register_type(&MetaType::new::<TVersionedLoc>()).id;
        let program = registry.register_type(&MetaType::new::<TProgram>()).id;
        let xcm_output = registry.register_type(&MetaType::new::<TXcmOutput<O>>()).id;
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
        .with_xcm(location, program, xcm_output)
    }

    fn program(instructions: Vec<TInstruction>) -> TProgram {
        TProgram::V5(TXcm(instructions))
    }

    fn transact(payload: &[u8]) -> TInstruction {
        TInstruction::Transact {
            call: TDoubleEncoded {
                encoded: payload.to_vec(),
            },
        }
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
        assert!(
            err.contains("MediumSpender") && err.contains("Treasurer"),
            "{err}"
        );

        let err = ctx
            .encode_origin(&OriginSpec::Variant {
                pallet: "Council".into(),
                variant: "Members".into(),
            })
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("no origin 'Council'") && err.contains("Origins"),
            "{err}"
        );
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

        assert_eq!(
            ctx.arity(),
            3,
            "the registered method takes result_xcms_version"
        );
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
                vec![forwarded_message()],
            )],
        });
        out.encode()
    }

    /// The message the re-encoding test lifts back out — deliberately one with
    /// every layer in it: a version wrapper, the Xcm newtype, a DoubleEncoded
    /// payload and a 32-byte topic.
    fn forwarded_message() -> TProgram {
        program(vec![
            TInstruction::ClearOrigin,
            transact(&[0x00, 0x07, 0xff]),
            TInstruction::SetTopic([9u8; 32]),
        ])
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

        assert_eq!(
            outcome.events.len(),
            2,
            "a one-element Vec is not a newtype"
        );
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

    // ------------------------------------------------- dry_run_xcm (slice 5)

    #[test]
    fn an_origin_location_is_built_from_the_registry_and_decodes_as_the_real_type() {
        let ctx = xcm_context::<TOutcomeV5>();

        // A SIBLING: parents 1, X1[Parachain(n)]. The strong form of the
        // assertion — the bytes we built decode back through codec into the real
        // Rust value, so encoder and decoder are not two copies of one guess.
        let (bytes, json) = ctx
            .encode_location(&LocationSpec::Sibling(1000))
            .expect("sibling encodes");
        assert_eq!(
            TVersionedLoc::decode(&mut &bytes[..]).unwrap(),
            TVersionedLoc::V5(TLocation {
                parents: 1,
                interior: TJunctions::X1([TJunction::Parachain(1000)]),
            }),
            "an unnamed composite of one encodes into X1's one-element ARRAY"
        );
        assert!(
            json.get("V5").is_some(),
            "the NEWEST version the runtime declares is chosen, read from the registry \
             rather than pinned: {json}"
        );

        // A CHILD is the same para id at parents 0 — the distinction that stops
        // every downward message being called HRMP.
        let (bytes, _) = ctx
            .encode_location(&LocationSpec::Child(2034))
            .expect("child encodes");
        assert_eq!(
            TVersionedLoc::decode(&mut &bytes[..]).unwrap(),
            TVersionedLoc::V5(TLocation {
                parents: 0,
                interior: TJunctions::X1([TJunction::Parachain(2034)]),
            })
        );

        let (bytes, _) = ctx
            .encode_location(&LocationSpec::Parent)
            .expect("parent encodes");
        assert_eq!(
            TVersionedLoc::decode(&mut &bytes[..]).unwrap(),
            TVersionedLoc::V5(TLocation {
                parents: 1,
                interior: TJunctions::Here,
            })
        );

        let (bytes, _) = ctx
            .encode_location(&LocationSpec::Here)
            .expect("here encodes");
        assert_eq!(
            TVersionedLoc::decode(&mut &bytes[..]).unwrap(),
            TVersionedLoc::V5(TLocation {
                parents: 0,
                interior: TJunctions::Here,
            })
        );

        // A call-only context has no dry_run_xcm and says so instead of
        // encoding something.
        let err = context()
            .encode_location(&LocationSpec::Parent)
            .unwrap_err()
            .to_string();
        assert!(err.contains("dry_run_xcm"), "{err}");
    }

    #[test]
    fn both_baselines_are_built_by_name_and_checked_before_use() {
        let ctx = xcm_context::<TOutcomeV5>();

        // THE SENDING SIDE: system.remark(), two variant indices and a compact
        // zero, byte-identical to what codec produces for the real call.
        let noop = ctx.noop_call().expect("the no-op builds");
        assert_eq!(
            noop,
            TRuntimeCall::System(TSystemCall::remark { remark: vec![] }).encode()
        );
        assert_eq!(ctx.decode_call(&noop).unwrap().summary, "system.remark");

        // THE RECEIVING SIDE: an empty program, which executes nothing — AT A
        // NAMED VERSION, because dry_run_xcm answers in the version it was
        // asked in, and a baseline at another version cannot be differenced
        // against its subject.
        let empty = ctx
            .encode_empty_program(5)
            .expect("the empty program builds");
        assert_eq!(empty, TProgram::V5(TXcm(vec![])).encode());
        assert_eq!(
            program_summary(&ctx.decode_program(&empty).unwrap()),
            "(empty program)",
            "and it reads back as empty — a baseline that executed anything would \
             subtract a run's own messages from its own attribution"
        );
        // …and the SAME call at v4 produces v4 bytes, which is the whole point.
        let empty4 = ctx.encode_empty_program(4).expect("v4 builds too");
        assert_eq!(empty4, TProgram::V4(TXcm(vec![])).encode());
        assert_ne!(empty, empty4);
        assert_eq!(
            program_version(&ctx.decode_program(&empty4).unwrap()),
            Some(4),
            "and the version a program carries is readable back out of it — which is how \
             the baseline learns which one to build"
        );
        // A version this runtime does not declare is refused, not approximated.
        let err = ctx.encode_empty_program(2).unwrap_err().to_string();
        assert!(err.contains("V2") && err.contains("V4, V5"), "{err}");
    }

    #[test]
    fn an_xcm_outcome_has_three_states_and_error_means_execution_never_started() {
        let ctx = xcm_context::<TOutcomeV5>();
        let effects = |o: TOutcomeV5| -> Vec<u8> {
            let out: TXcmOutput<TOutcomeV5> = Ok(TXcmEffects {
                execution_result: o,
                emitted_events: vec![TRuntimeEvent::Balances(TBalancesEvent::Transfer {
                    from: TAccount([1u8; 32]),
                    to: TAccount([2u8; 32]),
                    amount: 5,
                })],
                forwarded_xcms: vec![(
                    TVersionedLocation::V4 {
                        parents: 1,
                        parachain: 2000,
                    },
                    vec![forwarded_message()],
                )],
            });
            out.encode()
        };
        let weight = || TWeight {
            ref_time: 100,
            proof_size: 200,
        };

        let complete = ctx
            .interpret_xcm(&effects(TOutcomeV5::Complete { used: weight() }))
            .expect("interprets");
        assert_eq!(complete.status, XcmSimStatus::Complete);
        assert_eq!(complete.weight_used.as_ref().unwrap()["ref_time"], 100);
        assert!(complete.xcm_error.is_none());
        assert!(complete.note.is_none());
        assert_eq!(complete.events.len(), 1);
        assert_eq!(complete.events[0].name, "balances.Transfer");
        assert_eq!(complete.forwarded_xcms.len(), 1);

        // STARTED and stopped partway — the same fact an observed
        // Outcome::Incomplete records, previewed before it happens.
        let incomplete = ctx
            .interpret_xcm(&effects(TOutcomeV5::Incomplete {
                used: weight(),
                error: TInstructionError {
                    index: 2,
                    error: TXcmError::UntrustedReserveLocation,
                },
            }))
            .expect("interprets");
        assert_eq!(incomplete.status, XcmSimStatus::Incomplete);
        assert!(incomplete.weight_used.is_some());
        let err = incomplete.xcm_error.expect("carries the reason");
        assert_eq!(
            err["index"], 2,
            "XCM v5 names the failing instruction, and that index is kept"
        );

        // NEVER STARTED. Upstream calls this variant `Error`; it is not an error
        // of ours, and it is the answer the sending chain cannot give — its own
        // `Sent` would look perfectly successful.
        let rejected = ctx
            .interpret_xcm(&effects(TOutcomeV5::Error(TInstructionError {
                index: 0,
                error: TXcmError::Barrier,
            })))
            .expect("interprets");
        assert_eq!(rejected.status, XcmSimStatus::NotStarted);
        assert_eq!(rejected.status.as_str(), "not_started");
        assert!(
            rejected.weight_used.is_none(),
            "nothing ran, so no weight was used"
        );
        let why = rejected.xcm_error.expect("a rejection names its reason");
        assert_eq!(why["index"], 0, "v5 isolates the offending instruction");
        assert_eq!(
            why["error"],
            serde_json::json!({"Barrier": []}),
            "a unit variant renders as an empty ARRAY, never as a bare string"
        );
        let note = rejected.note.expect("a rejection must say what it means");
        assert!(note.contains("NEVER STARTED"), "{note}");

        // The API refusing is a fourth, different thing: nothing was attempted
        // at all, so there is no outcome to report.
        let out: TXcmOutput<TOutcomeV5> = Err(TApiError::VersionedConversionFailed);
        let refused = ctx.interpret_xcm(&out.encode()).expect("interprets");
        assert_eq!(refused.status, XcmSimStatus::ApiError);
        assert!(refused.events.is_empty());
        assert!(refused.forwarded_xcms.is_empty());

        // And a response that does not fit the declared output is loud.
        let mut bad = effects(TOutcomeV5::Complete { used: weight() });
        bad.push(0xff);
        assert!(ctx
            .interpret_xcm(&bad)
            .unwrap_err()
            .to_string()
            .contains("trailing"));
    }

    #[test]
    fn the_v4_outcome_shape_reads_as_the_same_three_states() {
        // v4's `Error` is a STRUCT variant and its `Incomplete.error` is a bare
        // Error rather than an InstructionError. The variant NAMES did not
        // change, which is why only they are read — and this test is the proof
        // that reading them is enough.
        let ctx = xcm_context::<TOutcomeV4>();
        let effects = |o: TOutcomeV4| -> Vec<u8> {
            let out: TXcmOutput<TOutcomeV4> = Ok(TXcmEffects {
                execution_result: o,
                emitted_events: vec![],
                forwarded_xcms: vec![],
            });
            out.encode()
        };

        let rejected = ctx
            .interpret_xcm(&effects(TOutcomeV4::Error {
                error: TXcmError::Barrier,
            }))
            .expect("interprets");
        assert_eq!(rejected.status, XcmSimStatus::NotStarted);
        assert_eq!(
            rejected.xcm_error.unwrap(),
            serde_json::json!({"Barrier": []}),
            "a v4 rejection carries no instruction index, and none is invented — the \
             payload is the bare Error, kept in the shape the runtime rendered it"
        );

        let incomplete = ctx
            .interpret_xcm(&effects(TOutcomeV4::Incomplete {
                used: TWeight {
                    ref_time: 1,
                    proof_size: 2,
                },
                error: TXcmError::Barrier,
            }))
            .expect("interprets");
        assert_eq!(incomplete.status, XcmSimStatus::Incomplete);
        assert_eq!(incomplete.weight_used.unwrap()["proof_size"], 2);
    }

    #[test]
    fn a_forwarded_message_re_encodes_to_the_bytes_the_runtime_reported() {
        let ctx = xcm_context::<TOutcomeV5>();
        let response = effects(Ok(ok_post()));

        let lifted = ctx
            .forwarded_program(&response, 0, 0)
            .expect("the message is lifted back out");
        // THE ASSERTION THE WHOLE STITCH RESTS ON: re-encoding a decoded value
        // against its own declared type reproduces the encoder's bytes exactly,
        // DoubleEncoded payload and 32-byte topic included. If this ever stops
        // holding, the next chain would be previewed on a message the first one
        // did not send.
        assert_eq!(
            lifted.bytes,
            forwarded_message().encode(),
            "re-encoded, not sliced — and byte-identical to codec's own output"
        );
        assert_eq!(lifted.destination_index, 0);
        assert_eq!(lifted.message_index, 0);
        assert_eq!(lifted.destination["V4"]["parachain"], 2034);
        assert_eq!(
            program_summary(&lifted.program),
            "ClearOrigin → Transact → SetTopic",
            "read through the version wrapper AND the Xcm newtype layer"
        );
        // …and those bytes are exactly what the other chain's dry_run_xcm would
        // accept as its `xcm` parameter.
        assert_eq!(ctx.decode_program(&lifted.bytes).unwrap(), lifted.program);

        // Indices that do not exist are refused with the counts, never with an
        // empty program.
        let err = ctx
            .forwarded_program(&response, 3, 0)
            .unwrap_err()
            .to_string();
        assert!(err.contains("1 destination"), "{err}");
        let err = ctx
            .forwarded_program(&response, 0, 5)
            .unwrap_err()
            .to_string();
        assert!(err.contains("1 message"), "{err}");
    }

    #[test]
    fn a_program_summary_survives_every_layer_and_never_invents_one() {
        let ctx = xcm_context::<TOutcomeV5>();
        let three = program(vec![
            TInstruction::ClearOrigin,
            transact(&[1]),
            TInstruction::SetTopic([0u8; 32]),
        ])
        .encode();
        assert_eq!(
            program_summary(&ctx.decode_program(&three).unwrap()),
            "ClearOrigin → Transact → SetTopic"
        );

        // ONE instruction is the case a fixed-depth peel gets wrong: the
        // innermost list has a single element, and it is an OBJECT, which is
        // what stops the unwrapping at the right layer.
        let one = program(vec![TInstruction::ClearOrigin]).encode();
        assert_eq!(
            program_summary(&ctx.decode_program(&one).unwrap()),
            "ClearOrigin"
        );

        assert_eq!(
            program_summary(&serde_json::json!({"V5": [[[]]]})),
            "(empty program)"
        );
        assert_eq!(
            program_summary(&serde_json::json!("nonsense")),
            "(unreadable program)"
        );

        // Bytes that are not a program at all are refused rather than summarised.
        assert!(ctx.decode_program(&[0xff, 0xff]).is_err());
        let mut trailing = one.clone();
        trailing.push(0x00);
        assert!(ctx
            .decode_program(&trailing)
            .unwrap_err()
            .to_string()
            .contains("trailing"));
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
