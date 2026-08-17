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

use adapter_substrate::dryrun::{self, DryRunContext};
use adapter_substrate::source::SubstrateSource;
use async_trait::async_trait;
use ingest::live::ChainSource;
use raw_store::RawStore;
use sim::{DryRunner, PreparedRun, SimError, SimOutcome, SimRequest, TIER_DRY_RUN};

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
}

#[async_trait]
impl DryRunner for SubstrateDryRunner<'_> {
    async fn prepare(&self, req: &SimRequest) -> Result<PreparedRun, SimError> {
        let height = match req.at_height {
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
                id: format!("0x{}", hex::encode(dryrun::runtime_api_id(dryrun::DRY_RUN_API))),
            });
        };

        let (metadata, metadata_version) = self
            .api_metadata(rv.spec_version, block_hash, &rv.apis)
            .await?;
        let ctx = DryRunContext::from_metadata(&metadata)?;

        // The two sources of arity must AGREE, and a disagreement is refused
        // rather than resolved. sp-api dispatches against the IMPLEMENTED trait
        // version and decodes with no tolerance for trailing bytes, so guessing
        // wrong does not produce a clear error — it produces an opaque
        // `state_call` failure that looks like a sick node. This file already
        // refuses to guess when the parameter NAMES are unfamiliar; the arity
        // deserves the same treatment.
        let expected = if api_version >= 2 { 3 } else { 2 };
        if ctx.arity() != expected {
            return Err(SimError::Encode(format!(
                "chain {} declares {} v{api_version} (arity {expected}) but its metadata \
                 declares dry_run_call with {} parameters — refusing to send a request that \
                 one of the two would reject",
                self.chain_id,
                dryrun::DRY_RUN_API,
                ctx.arity()
            )));
        }

        let decoded = ctx.decode_call(&req.call)?;
        let (params, origin_json) = ctx.encode_params(&req.origin, &req.call, req.xcm_version)?;

        Ok(PreparedRun {
            chain_id: self.chain_id.to_string(),
            at_height: height,
            at_block_hash: format!("0x{}", hex::encode(block_hash.as_ref())),
            spec_version: rv.spec_version,
            api_version,
            metadata_version,
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
        //
        // It reads the EXACT version `prepare` used, not "whichever is present":
        // a chain can offer both v15 and v16 for one spec, and interpreting
        // bytes against a different registry than built them is a different
        // claim wearing the same lineage.
        let key = raw_store::keys::metadata_at_version(
            &prepared.chain_id,
            prepared.spec_version,
            prepared.metadata_version,
        );
        let blob = self.raw.get(&key).map_err(|e| {
            SimError::Decode(format!(
                "no archived v{} metadata for {} spec {} ({key}) — cannot interpret the \
                 response it produced: {e}",
                prepared.metadata_version, prepared.chain_id, prepared.spec_version
            ))
        })?;
        DryRunContext::from_metadata(&blob)?.interpret(response)
    }

    fn sim_version(&self) -> u32 {
        dryrun::DRY_RUN_VERSION
    }
}

fn parse_hash(hex_hash: &str) -> Result<adapter_substrate::source::BlockHash, SimError> {
    let bytes = hex::decode(hex_hash.trim_start_matches("0x"))
        .map_err(|e| SimError::Source(format!("block hash {hex_hash} is not hex: {e}")))?;
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| SimError::Source(format!("block hash {hex_hash} is not 32 bytes")))?;
    Ok(arr.into())
}
