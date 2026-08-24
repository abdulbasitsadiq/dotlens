//! The live Tier 1 runner: `sim::DryRunner` over a real chain (Phase 3, slice 1).
//!
//! Wiring only. The decisions live elsewhere on purpose — the orchestration in
//! `sim` (prepare → cache → archive → dispatch → archive → interpret → record),
//! the SCALE in `adapter_substrate::dryrun`, the RPC in `source.rs`. What is
//! here is the part that needs all three at once: resolving a block, getting a
//! metadata blob that actually carries runtime APIs, and archiving it.
//!
//! THE v15 FETCH IS THE INTERESTING PART. Every metadata blob dotlens has
//! archived since Phase 1 is v14, because that is what `state_getMetadata`
//! returns — and v14 has no runtime-API section at all, so a dry run cannot be
//! built from it. This runner fetches v15 through `Metadata_metadata_at_version`
//! and archives it BESIDE the v14 blob under the same spec_version (a different
//! key, never overwriting: both are true artifacts of that runtime, and the v14
//! one still decodes every block). Archive-or-fetch, exactly like the preimage
//! path — so a re-run at the same spec costs no RPC.

//! **SLICE 5** adds the receiving side on the same struct: one runner, two
//! traits, one `api_metadata` between them. And it adds the two BASELINES —
//! `system.remark()` for a call, the empty program for an XCM — which are
//! ordinary requests handed back to `sim`, not a side channel: the orchestration
//! runs, archives and caches them like anything else.
//!
//! KNOWN COST, RESTATED WITH ITS NEW NUMBER. Slice 1 recorded that the metadata
//! blob is SCALE-decoded twice per simulation (`prepare` + `interpret`) and that
//! a per-`(chain, spec)` context cache is the fix. The baseline multiplies that:
//! a simulation with one now decodes the blob up to FIVE times — `prepare` and
//! `baseline_request` and `interpret` for the subject, `prepare` and `interpret`
//! for the baseline — at 614KB for a v15 blob. Every one of those reads is from
//! the local raw store rather than the chain, so it is CPU and not RPC, and the
//! fix is the same cache it always was. Written down rather than discovered in a
//! profile.

use adapter_substrate::dryrun::{self, DryRunContext};
use adapter_substrate::source::SubstrateSource;
use async_trait::async_trait;
use ingest::live::ChainSource;
use raw_store::RawStore;
use sim::{
    DryRunner, OriginSpec, PreparedRun, PreparedXcmRun, SimError, SimOutcome, SimRequest,
    XcmDryRunner, XcmSimOutcome, XcmSimRequest, TIER_DRY_RUN,
};

/// Metadata versions this runner will accept, in preference order. Both carry
/// the `apis` section; v15 first because it is the version every runtime we
/// index has offered for years, and determinism beats novelty for a lineage
/// stamp. `Metadata_metadata_versions` says which a chain actually has.
const WANTED_METADATA_VERSIONS: [u32; 2] = [15, 16];

pub struct SubstrateDryRunner<'a> {
    chain_id: &'a str,
    source: &'a SubstrateSource,
    raw: &'a dyn RawStore,
    receipts: &'a dyn ingest::ReceiptSink,
}

impl<'a> SubstrateDryRunner<'a> {
    pub fn new(
        chain_id: &'a str,
        source: &'a SubstrateSource,
        raw: &'a dyn RawStore,
        receipts: &'a dyn ingest::ReceiptSink,
    ) -> Self {
        Self {
            chain_id,
            source,
            raw,
            receipts,
        }
    }

    /// A metadata blob that carries runtime APIs: archived if we have one,
    /// fetched, VERIFIED and archived if not. Returns the blob and its version.
    async fn api_metadata(
        &self,
        spec: u32,
        hash: adapter_substrate::source::BlockHash,
        apis: &serde_json::Value,
    ) -> Result<(Vec<u8>, u32), SimError> {
        for version in WANTED_METADATA_VERSIONS {
            let key = raw_store::keys::metadata_at_version(self.chain_id, spec, version);
            match self.raw.get(&key) {
                Ok(blob) => return Ok((blob, version)),
                Err(raw_store::RawStoreError::NotFound(_)) => {}
                Err(e) => return Err(SimError::Raw(e)),
            }
        }

        // `metadata_versions` only exists from Metadata API v2. Asking a runtime
        // too old for it produces an RPC error that reads as "the node is
        // unwell", which is precisely the confusion `RuntimeVersion.apis` exists
        // to prevent — so check first and say the true thing.
        match dryrun::declared_api_version(apis, "Metadata") {
            Some(v) if v >= 2 => {}
            other => {
                return Err(SimError::Source(format!(
                    "chain {} declares Metadata API version {other:?}; \
                     Metadata_metadata_at_version needs v2+, so no runtime-API metadata can be \
                     fetched from this runtime",
                    self.chain_id
                )))
            }
        }

        let available = self
            .source
            .metadata_versions(hash)
            .await
            .map_err(|e| SimError::Source(e.to_string()))?;
        for version in WANTED_METADATA_VERSIONS {
            if !available.contains(&version) {
                continue;
            }
            let Some(blob) = self
                .source
                .metadata_at_version(version, hash)
                .await
                .map_err(|e| SimError::Source(e.to_string()))?
            else {
                // it listed the version and then declined to produce it
                continue;
            };

            // VERIFY BEFORE ARCHIVING. The raw store is write-once, so a node
            // that answers `metadata_at_version(15)` with a v14 blob or with
            // garbage would poison this key permanently — every later run on
            // this spec would read the poison back and fail, with no code path
            // able to repair it. Slice 2 learned this on the preimage path
            // ("verify BEFORE archiving; a value failing these checks is by
            // definition not the value for this key"); the same rule applies
            // here, and unlike a preimage there is no hash to check against, so
            // the check is that the bytes decode AND declare the version asked
            // for.
            match dryrun::metadata_version(&blob) {
                Some(actual) if actual == version => {}
                other => {
                    return Err(SimError::Source(format!(
                        "chain {} answered metadata_at_version({version}) with {} — refusing \
                         to archive it under a key that can never be rewritten",
                        self.chain_id,
                        match other {
                            Some(v) => format!("v{v}"),
                            None => "bytes that are not metadata at all".to_string(),
                        }
                    )))
                }
            }

            let key = raw_store::keys::metadata_at_version(self.chain_id, spec, version);
            let receipt = self.raw.put(&key, &blob, "simulate")?;
            if let Err(e) = self.receipts.record(&receipt).await {
                tracing::warn!(error = %e, "metadata receipt not recorded — continuing");
            }
            tracing::info!(
                chain = %self.chain_id, spec, version,
                "runtime-API metadata archived (v14 stays beside it — it decodes the blocks)"
            );
            return Ok((blob, version));
        }
        Err(SimError::Source(format!(
            "chain {} at spec {spec} offers metadata versions {available:?}; none of \
             {WANTED_METADATA_VERSIONS:?} is available, and v14 carries no runtime-API section",
            self.chain_id
        )))
    }

    /// The decode context for a run that has already happened, rebuilt from the
    /// ARCHIVED blob rather than from the chain.
    ///
    /// It reads the EXACT version `prepare` used, not "whichever is present": a
    /// chain can offer both v15 and v16 for one spec, and interpreting bytes
    /// against a different registry than built them is a different claim wearing
    /// the same lineage. Public because `simulate-forwarded` needs it to lift a
    /// message back out of a response archived days ago.
    pub fn context_for(
        &self,
        spec_version: u32,
        metadata_version: u32,
    ) -> Result<DryRunContext, SimError> {
        let key =
            raw_store::keys::metadata_at_version(self.chain_id, spec_version, metadata_version);
        let blob = self.raw.get(&key).map_err(|e| {
            SimError::Decode(format!(
                "no archived v{metadata_version} metadata for {} spec {spec_version} ({key}) — \
                 cannot read what it produced: {e}",
                self.chain_id
            ))
        })?;
        DryRunContext::from_metadata(&blob)
    }

    /// Resolve the state a request names, and everything the runtime says about
    /// itself there. Shared by both `prepare` paths, which ask the same four
    /// questions in the same order.
    async fn resolve_state(&self, at_height: Option<u64>) -> Result<ResolvedState, SimError> {
        let height = match at_height {
            Some(h) => h,
            None => self
                .source
                .finalized_height()
                .await
                .map_err(|e| SimError::Source(e.to_string()))?,
        };
        let block_hash = self
            .source
            .block_hash(height)
            .await
            .map_err(|e| SimError::Source(e.to_string()))?;
        let rv = self
            .source
            .runtime_version_info(block_hash)
            .await
            .map_err(|e| SimError::Source(e.to_string()))?;

        // Ask the runtime whether it can do this AT ALL, before building
        // anything. An absent api id is a fact about the chain, not a failure of
        // ours, and it is reported with the id we looked for so the claim is
        // checkable rather than mysterious.
        let Some(api_version) = dryrun::declared_api_version(&rv.apis, dryrun::DRY_RUN_API) else {
            return Err(SimError::Unsupported {
                chain: self.chain_id.to_string(),
                api: dryrun::DRY_RUN_API.to_string(),
                spec: rv.spec_version,
                id: format!(
                    "0x{}",
                    hex::encode(dryrun::runtime_api_id(dryrun::DRY_RUN_API))
                ),
            });
        };

        let (metadata, metadata_version) = self
            .api_metadata(rv.spec_version, block_hash, &rv.apis)
            .await?;
        let ctx = DryRunContext::from_metadata(&metadata)?;
        Ok(ResolvedState {
            height,
            block_hash,
            spec_version: rv.spec_version,
            api_version,
            metadata_version,
            ctx,
        })
    }
}

struct ResolvedState {
    height: u64,
    block_hash: adapter_substrate::source::BlockHash,
    spec_version: u32,
    api_version: u32,
    metadata_version: u32,
    ctx: DryRunContext,
}

#[async_trait]
impl DryRunner for SubstrateDryRunner<'_> {
    async fn prepare(&self, req: &SimRequest) -> Result<PreparedRun, SimError> {
        let state = self.resolve_state(req.at_height).await?;
        let ctx = &state.ctx;

        // The two sources of arity must AGREE, and a disagreement is refused
        // rather than resolved. sp-api dispatches against the IMPLEMENTED trait
        // version and decodes with no tolerance for trailing bytes, so guessing
        // wrong does not produce a clear error — it produces an opaque
        // `state_call` failure that looks like a sick node. This file already
        // refuses to guess when the parameter NAMES are unfamiliar; the arity
        // deserves the same treatment.
        //
        // NOTE it checks `dry_run_call` only, and correctly: `dry_run_xcm` takes
        // the same two arguments in DryRunApi v1 and v2, so there is no arity to
        // reconcile on that method and a check would compare a constant with
        // itself.
        let expected = if state.api_version >= 2 { 3 } else { 2 };
        if ctx.arity() != expected {
            return Err(SimError::Encode(format!(
                "chain {} declares {} v{} (arity {expected}) but its metadata \
                 declares dry_run_call with {} parameters — refusing to send a request that \
                 one of the two would reject",
                self.chain_id,
                dryrun::DRY_RUN_API,
                state.api_version,
                ctx.arity()
            )));
        }

        let decoded = ctx.decode_call(&req.call)?;
        let (params, origin_json) = ctx.encode_params(&req.origin, &req.call, req.xcm_version)?;

        Ok(PreparedRun {
            chain_id: self.chain_id.to_string(),
            at_height: state.height,
            at_block_hash: format!("0x{}", hex::encode(state.block_hash.as_ref())),
            spec_version: state.spec_version,
            api_version: state.api_version,
            metadata_version: state.metadata_version,
            tier: TIER_DRY_RUN.to_string(),
            method: dryrun::DRY_RUN_CALL_FUNCTION.to_string(),
            input_hash: format!(
                "0x{}",
                hex::encode(adapter_substrate::calls::blake2_256(&params))
            ),
            params,
            call_hash: format!(
                "0x{}",
                hex::encode(adapter_substrate::calls::blake2_256(&req.call))
            ),
            call_summary: Some(decoded.summary),
            origin_spec: req.origin_spec.clone(),
            origin_json,
            xcm_version: req.xcm_version,
        })
    }

    async fn dispatch(&self, prepared: &PreparedRun) -> Result<Vec<u8>, SimError> {
        let hash = parse_hash(&prepared.at_block_hash)?;
        self.source
            .state_call(&prepared.method, &prepared.params, hash)
            .await
            .map_err(|e| SimError::Source(e.to_string()))
    }

    fn interpret(&self, prepared: &PreparedRun, response: &[u8]) -> Result<SimOutcome, SimError> {
        // Re-read the archived blob rather than carrying a context across the
        // await: the blob is on disk by now and this keeps `interpret` pure and
        // callable on its own, which is what a later sim_version rebuild needs.
        self.context_for(prepared.spec_version, prepared.metadata_version)?
            .interpret(response)
    }

    fn sim_version(&self) -> u32 {
        dryrun::DRY_RUN_VERSION
    }

    /// `system.remark()` under Root, pinned to the subject's height.
    ///
    /// A RUNTIME THAT CANNOT EXPRESS THE NO-OP GIVES `None`, NOT AN ERROR: such
    /// a run is still a perfectly good run whose forwarded list is simply
    /// unattributed, and refusing the whole simulation over it would trade an
    /// answer for a caveat. The reason is logged, because "no baseline" and "no
    /// baseline BECAUSE" are different things to a person debugging it.
    ///
    /// A MISSING ARCHIVED METADATA BLOB IS STILL FATAL, and deliberately so —
    /// `interpret` reads the same blob two steps later, so degrading here would
    /// only move the failure. In practice it cannot happen: `prepare` fetched or
    /// found that blob moments ago.
    fn baseline_request(&self, prepared: &PreparedRun) -> Result<Option<SimRequest>, SimError> {
        let ctx = self.context_for(prepared.spec_version, prepared.metadata_version)?;
        match ctx.noop_call() {
            Ok(call) => Ok(Some(SimRequest {
                chain_id: prepared.chain_id.clone(),
                at_height: Some(prepared.at_height),
                call,
                origin: OriginSpec::Variant {
                    pallet: "system".into(),
                    variant: "Root".into(),
                },
                origin_spec: "root".into(),
                xcm_version: prepared.xcm_version,
            })),
            Err(e) => {
                tracing::warn!(
                    chain = %prepared.chain_id, error = %e,
                    "no baseline no-op can be built on this runtime — the forwarded list will \
                     be recorded unattributed"
                );
                Ok(None)
            }
        }
    }
}

/// The receiving side, on the same runner: one struct, two traits, one
/// `api_metadata` between them — so a journey whose two halves are previewed on
/// two chains still archives one v15 blob per (chain, spec).
#[async_trait]
impl XcmDryRunner for SubstrateDryRunner<'_> {
    async fn prepare_xcm(&self, req: &XcmSimRequest) -> Result<PreparedXcmRun, SimError> {
        let state = self.resolve_state(req.at_height).await?;
        let ctx = &state.ctx;

        // Decoding the program against the type the METHOD declares is the check
        // that these bytes are a program this runtime could accept at all — the
        // same stricter-than-necessary move `decode_call` makes on the call side.
        let program = ctx.decode_program(&req.program)?;
        let (origin_bytes, origin_location) = ctx.encode_location(&req.origin)?;
        let params = ctx.encode_xcm_params(&origin_bytes, &req.program)?;

        Ok(PreparedXcmRun {
            chain_id: self.chain_id.to_string(),
            at_height: state.height,
            at_block_hash: format!("0x{}", hex::encode(state.block_hash.as_ref())),
            spec_version: state.spec_version,
            api_version: state.api_version,
            metadata_version: state.metadata_version,
            tier: TIER_DRY_RUN.to_string(),
            method: dryrun::DRY_RUN_XCM_FUNCTION.to_string(),
            input_hash: format!(
                "0x{}",
                hex::encode(adapter_substrate::calls::blake2_256(&params))
            ),
            params,
            program_hash: format!(
                "0x{}",
                hex::encode(adapter_substrate::calls::blake2_256(&req.program))
            ),
            program_summary: Some(dryrun::program_summary(&program)),
            program,
            origin_ref: req.origin.as_token(),
            origin: req.origin.clone(),
            origin_location,
            source: req.source.clone(),
        })
    }

    async fn dispatch_xcm(&self, prepared: &PreparedXcmRun) -> Result<Vec<u8>, SimError> {
        let hash = parse_hash(&prepared.at_block_hash)?;
        self.source
            .state_call(&prepared.method, &prepared.params, hash)
            .await
            .map_err(|e| SimError::Source(e.to_string()))
    }

    fn interpret_xcm(
        &self,
        prepared: &PreparedXcmRun,
        response: &[u8],
    ) -> Result<XcmSimOutcome, SimError> {
        self.context_for(prepared.spec_version, prepared.metadata_version)?
            .interpret_xcm(response)
    }

    fn sim_version(&self) -> u32 {
        dryrun::DRY_RUN_VERSION
    }

    /// The empty program FROM THE SAME ORIGIN, AT THE SAME XCM VERSION, at the
    /// same state.
    ///
    /// All three "same"s are load-bearing. The ORIGIN is carried over because a
    /// runtime's routers can behave differently for different senders. The
    /// VERSION is carried over because `dry_run_xcm` renders its forwarded list
    /// in the version of the program it was given, so a baseline at a different
    /// version produces a list that cannot be differenced against the subject's
    /// — and every ambient message would then be attributed to the run.
    ///
    /// `Ok(None)` where the version cannot be expressed or the program cannot be
    /// built; a missing archived metadata blob is still a hard error, because
    /// `interpret` needs the same blob and would fail two lines later anyway.
    fn baseline_request(
        &self,
        prepared: &PreparedXcmRun,
    ) -> Result<Option<XcmSimRequest>, SimError> {
        let ctx = self.context_for(prepared.spec_version, prepared.metadata_version)?;
        let Some(version) = dryrun::program_version(&prepared.program) else {
            tracing::warn!(
                chain = %prepared.chain_id,
                "this program does not read as a versioned XCM, so no baseline can be built \
                 at its version — the forwarded list will be recorded unattributed"
            );
            return Ok(None);
        };
        match ctx.encode_empty_program(version) {
            Ok(program) => Ok(Some(XcmSimRequest {
                chain_id: prepared.chain_id.clone(),
                at_height: Some(prepared.at_height),
                origin: prepared.origin.clone(),
                program,
                // A baseline came from nowhere: it is not a leg of any journey,
                // and giving it a provenance would put a message in the stitch
                // that nobody sent.
                source: None,
            })),
            Err(e) => {
                tracing::warn!(
                    chain = %prepared.chain_id, error = %e,
                    "no empty baseline program can be built on this runtime — the forwarded \
                     list will be recorded unattributed"
                );
                Ok(None)
            }
        }
    }
}

// ------------------------------------------- following a message to its chain

/// Which registered chain a `forwarded_xcms` destination names, from REGISTRY
/// DATA alone — para ids, relay membership and the network name, with no chain
/// id in code (Invariant 2).
///
/// It reads the destination through `xcm::location_counterparty`, the SAME
/// function that labels observed messages, so a previewed leg and an indexed one
/// can never disagree about where a message was addressed.
///
/// `Err` IS THE COVERAGE EDGE AND IT IS A FEATURE. A destination this cannot
/// resolve is a place dotlens does not index, and the caller's job is to say so
/// and stop — the roadmap's own rule for traces, applied to previews: "a trace
/// that reaches an unindexed chain must STOP AND SAY SO". Silently skipping it
/// would render as "this call sends nothing there".
pub fn resolve_destination<'a>(
    registry: &'a registry::Registry,
    from: &registry::ChainConfig,
    destination: &serde_json::Value,
) -> Result<&'a registry::ChainConfig, String> {
    let observed = adapter_substrate::xcm::location_counterparty(unversioned(destination))
        .ok_or_else(|| format!("this destination names no chain we can read: {destination}"))?;

    // `remote:<consensus>[/para:<id>]` addresses a named consensus system. When
    // that consensus IS this network the address is merely ABSOLUTE rather than
    // foreign, and resolves normally — the read-side half of slice 4's known gap
    // (2), decided the same way `api::comparable_counterparty` decides it.
    let token: &str = match observed.strip_prefix("remote:") {
        Some(rest) => match rest.split_once('/') {
            Some((consensus, tail)) if consensus == from.network => tail,
            // `remote:<this network>` with NO parachain junction is the absolute
            // address of the network's own RELAY. It is not foreign, and calling
            // it foreign would be a false sentence about our own chain.
            None if rest == from.network => "parent",
            _ => {
                return Err(format!(
                    "addressed to {observed}, which is another consensus system — dotlens does \
                     not index it, so the journey is followed to this boundary and no further"
                ))
            }
        },
        None => &observed,
    };

    if token == "parent" {
        let relay = from
            .relay
            .as_deref()
            .ok_or_else(|| format!("{} has no parent to address", from.id))?;
        return registry
            .chain(relay)
            .ok_or_else(|| format!("{relay} is not registered"));
    }
    let para: u32 = token
        .strip_prefix("para:")
        .ok_or_else(|| format!("'{token}' does not name a chain"))?
        .parse()
        .map_err(|_| format!("'{token}' does not carry a parachain id"))?;

    // A sibling (same relay) or a child (this chain IS the relay), never a
    // parachain of some other network that happens to share an id.
    registry
        .chains()
        .find(|c| {
            c.para_id == Some(para)
                && c.network == from.network
                && (c.relay == from.relay || c.relay.as_deref() == Some(from.id.as_str()))
        })
        .ok_or_else(|| {
            format!(
                "parachain {para} is addressed but not registered in the {} network — the \
                 journey is followed to this boundary and no further",
                from.network
            )
        })
}

/// Peel a `VersionedLocation`'s version wrapper, if there is one.
///
/// THIS IS NEEDED HERE AND NOWHERE ELSE, and the asymmetry is the reason it is a
/// separate function rather than a fix inside the mapper. `pallet_xcm.Sent`
/// carries a BARE `Location`, which is what `location_counterparty` was written
/// against; `forwarded_xcms` carries a `VersionedLocation`. The two differ by
/// one variant wrapper and one array layer, and `location_counterparty` finds a
/// `Parachain` junction recursively but reads `parents` from the TOP object — so
/// a versioned `{parents: 1, Here}` (a parachain forwarding to the relay) would
/// resolve to nothing at all, while a versioned sibling would resolve fine. A
/// gap that only bites one destination shape is exactly the kind that ships.
///
/// Fixed on the READING side, deliberately: making the mapper's own walk
/// recursive would change what `xcm.messages.counterparty` means and cost an
/// XCM_MAPPER_VERSION bump and a re-map of every indexed row, to fix a shape
/// that pallet never emits.
fn unversioned(v: &serde_json::Value) -> &serde_json::Value {
    let Some(map) = v.as_object() else { return v };
    if map.len() != 1 {
        return v;
    }
    let (key, inner) = map.iter().next().expect("checked len == 1");
    let versioned =
        key.len() > 1 && key.starts_with('V') && key[1..].chars().all(|c| c.is_ascii_digit());
    if !versioned {
        return v;
    }
    // A newtype variant renders its payload one array layer in.
    match inner.as_array().map(|a| a.as_slice()) {
        Some([only]) => only,
        _ => inner,
    }
}

/// How `receiver` must be told to address `sender` — the MIRROR of
/// `api::xcm_counterparty_name`, which makes the same derivation for the
/// observed-journey mirror check.
///
/// It returns a [`LocationSpec`] rather than a token because the preview needs
/// the parent count as well as the id: a sibling and a child are the same
/// parachain number at different depths, and telling a receiving runtime the
/// wrong one previews a message from a chain that did not send it.
pub fn origin_of(
    receiver: &registry::ChainConfig,
    sender: &registry::ChainConfig,
) -> Option<sim::LocationSpec> {
    if receiver.id == sender.id {
        return Some(sim::LocationSpec::Here);
    }
    if receiver.relay.as_deref() == Some(sender.id.as_str()) {
        return Some(sim::LocationSpec::Parent);
    }
    let para = sender.para_id?;
    if sender.relay.as_deref() == Some(receiver.id.as_str()) {
        return Some(sim::LocationSpec::Child(para));
    }
    if sender.relay.is_some() && sender.relay == receiver.relay {
        return Some(sim::LocationSpec::Sibling(para));
    }
    None
}

fn parse_hash(hex_hash: &str) -> Result<adapter_substrate::source::BlockHash, SimError> {
    let bytes = hex::decode(hex_hash.trim_start_matches("0x"))
        .map_err(|e| SimError::Source(format!("block hash {hex_hash} is not hex: {e}")))?;
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| SimError::Source(format!("block hash {hex_hash} is not 32 bytes")))?;
    Ok(arr.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn registry() -> registry::Registry {
        // crates/dotlens-node -> workspace root -> registry-seeds
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../registry-seeds");
        registry::Registry::load_from_dir(&dir).expect("seeds load")
    }

    /// A `VersionedLocation` as this decoder renders one: version variant, one
    /// array layer, and `X1` wrapping a one-element array of junctions.
    fn versioned(parents: u64, para: Option<u32>) -> serde_json::Value {
        let interior = match para {
            Some(p) => json!({"X1": [[{"Parachain": [p]}]]}),
            None => json!({"Here": []}),
        };
        json!({"V4": [{"parents": parents, "interior": interior}]})
    }

    #[test]
    fn a_destination_resolves_to_a_registered_chain_or_stops_at_the_coverage_edge() {
        let reg = registry();
        let ah = reg.chain("polkadot-asset-hub").unwrap();
        let relay = reg.chain("polkadot").unwrap();

        // sibling: Asset Hub → Hydration
        assert_eq!(
            resolve_destination(&reg, ah, &versioned(1, Some(2034)))
                .unwrap()
                .id,
            "hydration"
        );
        // upward: Asset Hub → the relay. THE CASE THE VERSION WRAPPER BREAKS if
        // it is not peeled — there is no Parachain junction to find recursively,
        // so `parents` has to be readable.
        assert_eq!(
            resolve_destination(&reg, ah, &versioned(1, None))
                .unwrap()
                .id,
            "polkadot"
        );
        // downward: the relay → one of its children
        assert_eq!(
            resolve_destination(&reg, relay, &versioned(0, Some(1001)))
                .unwrap()
                .id,
            "polkadot-collectives"
        );
        // and an UNVERSIONED location, which is the shape an observed
        // `Sent.destination` has, reads identically
        assert_eq!(
            resolve_destination(
                &reg,
                ah,
                &json!({"parents": 1, "interior": {"X1": [[{"Parachain": [1004]}]]}})
            )
            .unwrap()
            .id,
            "polkadot-people"
        );

        // THE COVERAGE EDGE. A Snowbridge export names another consensus system;
        // the journey stops there and says so, rather than being skipped as if
        // nothing were sent.
        let ethereum = json!({"V4": [{"parents": 2, "interior":
            {"X1": [[{"GlobalConsensus": [{"Ethereum": {"chain_id": 1}}]}]]}}]});
        let err = resolve_destination(&reg, ah, &ethereum).unwrap_err();
        assert!(err.contains("remote:ethereum:1"), "{err}");
        assert!(err.contains("does not index"), "{err}");

        // An ABSOLUTE address of one of OUR chains is not foreign, and resolves
        // — the read-side half of slice 4's known gap (2).
        let absolute = json!({"V4": [{"parents": 2, "interior": {"X2": [[
            {"GlobalConsensus": [{"Polkadot": []}]}, {"Parachain": [2034]}]]}}]});
        assert_eq!(
            resolve_destination(&reg, ah, &absolute).unwrap().id,
            "hydration"
        );

        // A parachain nobody registered is a boundary too, named by its id.
        let err = resolve_destination(&reg, ah, &versioned(1, Some(2999))).unwrap_err();
        assert!(
            err.contains("2999") && err.contains("not registered"),
            "{err}"
        );
    }

    #[test]
    fn the_origin_a_receiver_is_told_is_the_mirror_of_the_destination() {
        let reg = registry();
        let ah = reg.chain("polkadot-asset-hub").unwrap();
        let relay = reg.chain("polkadot").unwrap();
        let hydration = reg.chain("hydration").unwrap();

        // Asset Hub → Hydration: the destination says para 2034, and Hydration
        // must be told para 1000. Handing the destination back unchanged would
        // preview a message Hydration sent to itself.
        assert_eq!(
            origin_of(hydration, ah),
            Some(sim::LocationSpec::Sibling(1000))
        );
        assert_eq!(
            origin_of(ah, hydration),
            Some(sim::LocationSpec::Sibling(2034))
        );
        // Upward and downward differ by the PARENT COUNT at the same id.
        assert_eq!(origin_of(relay, ah), Some(sim::LocationSpec::Child(1000)));
        assert_eq!(origin_of(ah, relay), Some(sim::LocationSpec::Parent));
        assert_eq!(origin_of(ah, ah), Some(sim::LocationSpec::Here));
        assert_eq!(origin_of(relay, ah).unwrap().parents(), 0);
        assert_eq!(origin_of(hydration, ah).unwrap().parents(), 1);
    }

    #[test]
    fn peeling_a_version_wrapper_never_eats_a_real_field() {
        let bare = json!({"parents": 1, "interior": {"Here": []}});
        assert_eq!(unversioned(&bare), &bare, "nothing to peel");
        assert_eq!(
            unversioned(&json!({"V5": [{"parents": 2}]})),
            &json!({"parents": 2})
        );
        // A single-key object whose key is NOT V<n> is left alone — otherwise a
        // one-junction interior would be mistaken for a wrapper.
        let x1 = json!({"X1": [[{"Parachain": [2034]}]]});
        assert_eq!(unversioned(&x1), &x1);
        let vote = json!({"Vault": [1]});
        assert_eq!(unversioned(&vote), &vote);
    }
}
