//! The simulation service (ARCHITECTURE §11) — tier-agnostic half.
//!
//! Two tiers sit behind one contract:
//!   Tier 1 (this slice) — `DryRunApi::dry_run_call` against a live RPC. No
//!     fork, one round trip: "what would this call do, at this state, under
//!     this origin". Powers the referendum page's [Simulate].
//!   Tier 2 (next)      — chopsticks fork, state diff, queued jobs.
//!
//! NO SCALE, NO METADATA, NO SUBXT LIVE HERE: this crate owns the ORCHESTRATION
//! (prepare → cache → archive → dispatch → archive → interpret → record) and the
//! vocabulary both tiers share. The protocol half is
//! `adapter_substrate::dryrun`, reached through the `DryRunner` trait — the same
//! split as `ingest::module` over the five domain mappers (Invariant 4).
//!
//! IT IS NOT PROTOCOL-FREE, AND SAYING SO WOULD BE FALSE (review finding):
//! `OriginSpec` knows that `root` means `system:Root`, that `signed:` is a thing,
//! and that a signed origin carries 32 bytes. Those are FRAME facts sitting in a
//! generic crate. The trade was deliberate — the origin expression is what a
//! HUMAN types and what Tier 2 will have to accept verbatim, so it belongs where
//! both tiers can reach it — and its cost is visible: an AccountId20 chain is
//! refused by `dryrun.rs` rather than supported, because the generic layer
//! already fixed the width. If a second family ever needs this, the payload
//! becomes opaque bytes and the sugar moves into the adapter.
//!
//! THE ORDERING IS THE POINT, and it is raw-first (Invariant 1): the request
//! bytes and the response bytes are archived BEFORE they are interpreted, so a
//! response we cannot decode still leaves evidence behind and a later
//! `sim_version` re-derives from bytes rather than from a chain whose state has
//! since been pruned. A dry run is the one artifact in this project that
//! genuinely cannot be re-fetched later: the block stays, the STATE does not.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// The tier tag written to `sim.simulation_results.tier`.
pub const TIER_DRY_RUN: &str = "dry_run";

#[derive(Debug, thiserror::Error)]
pub enum SimError {
    /// Talking to the chain failed (RPC, endpoints exhausted, no such block).
    #[error("{0}")]
    Source(String),
    /// The runtime does not declare the API at all. This is an honest refusal,
    /// not a failure: it is reported and NOTHING is recorded, because without
    /// the runtime's own parameter types there is no request to hash and
    /// therefore no key a row could sit under.
    #[error(
        "chain {chain}: the runtime at spec_version {spec} does not declare {api} \
         (runtime-api id {id} absent from RuntimeVersion.apis) — nothing was simulated"
    )]
    Unsupported {
        chain: String,
        api: String,
        spec: u32,
        id: String,
    },
    /// The request could not be built (unknown origin variant, call bytes that
    /// are not a RuntimeCall, metadata without a runtime-API section).
    #[error("{0}")]
    Encode(String),
    /// The response could not be read. Loud on purpose — see the migration.
    #[error("{0}")]
    Decode(String),
    #[error("raw store: {0}")]
    Raw(#[from] raw_store::RawStoreError),
    #[error("simulation store: {0}")]
    Store(String),
}

// ------------------------------------------------------------------- the ask

/// A dispatch origin, expressed the way a person types it and resolved the way
/// the runtime spells it.
///
/// WHY THE CALLER MUST SUPPLY THIS RATHER THAN US DERIVING IT FROM THE TRACK —
/// and it is a finding, not a shortcut. A referendum's dispatch origin is NOT in
/// the chain's metadata: pallet-referenda's `Tracks` constant carries name,
/// deciding limits, deposits and curves, and no origin at all (our own
/// `gov.tracks.params` shows exactly those fields). The track↔origin mapping
/// lives in `TracksInfo::track_for`, which is Rust in the runtime, unreachable
/// from any artifact we index. So a track→origin table inside dotlens would be a
/// hardcoded guess about a specific runtime — precisely the chain-specific
/// branching Invariant 2 forbids — and a wrong guess here does not fail loudly:
/// it silently simulates the wrong thing and reports it confidently.
///
/// The chain DOES record the answer, once, in the `referenda.submit` call's
/// `proposal_origin` argument. Reading it back is a later slice (see the API's
/// `not_covered`); until then the caller says which origin they mean.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OriginSpec {
    /// `<pallet>:<Variant>` — a nested, FIELDLESS origin variant, e.g.
    /// `system:Root` or `Origins:MediumSpender`.
    Variant { pallet: String, variant: String },
    /// `signed:<ss58|0x-hex>` — `system:Signed(AccountId32)`, the one origin
    /// that carries a field.
    Signed([u8; 32]),
}

impl OriginSpec {
    /// Parse an origin expression. `parse_account` is injected because turning
    /// an SS58 string into 32 bytes is protocol knowledge and this crate has
    /// none (same device as `api::AppState.parse_account`).
    ///
    /// Accepted: `root`, `none`, `signed:<account>`, `<pallet>:<Variant>`.
    /// `root`/`none` are sugar for `system:Root` / `system:None`, so there is
    /// one encoder path, not three.
    pub fn parse<F>(spec: &str, parse_account: F) -> Result<Self, SimError>
    where
        F: Fn(&str) -> Result<[u8; 32], String>,
    {
        let trimmed = spec.trim();
        if trimmed.is_empty() {
            return Err(SimError::Encode(
                "empty origin — expected root | none | signed:<account> | <pallet>:<Variant>".into(),
            ));
        }
        match trimmed.to_ascii_lowercase().as_str() {
            "root" => {
                return Ok(Self::Variant {
                    pallet: "system".into(),
                    variant: "Root".into(),
                })
            }
            "none" => {
                return Ok(Self::Variant {
                    pallet: "system".into(),
                    variant: "None".into(),
                })
            }
            _ => {}
        }
        let Some((head, rest)) = trimmed.split_once(':') else {
            return Err(SimError::Encode(format!(
                "'{trimmed}' is not an origin — expected root | none | signed:<account> | \
                 <pallet>:<Variant>"
            )));
        };
        // `signed:` is checked case-insensitively; a pallet named "Signed" with
        // a variant would be ambiguous, and no runtime has one.
        if head.eq_ignore_ascii_case("signed") {
            let who = parse_account(rest.trim()).map_err(SimError::Encode)?;
            return Ok(Self::Signed(who));
        }
        if head.trim().is_empty() || rest.trim().is_empty() {
            return Err(SimError::Encode(format!(
                "'{trimmed}' is not an origin — both sides of ':' must be non-empty"
            )));
        }
        Ok(Self::Variant {
            pallet: head.trim().to_string(),
            variant: rest.trim().to_string(),
        })
    }

    /// The normalized spelling, for logs and for the recorded row.
    pub fn normalized(&self) -> String {
        match self {
            Self::Variant { pallet, variant } => format!("{pallet}:{variant}"),
            Self::Signed(who) => format!("signed:0x{}", hex::encode(who)),
        }
    }
}

/// One thing to simulate.
#[derive(Debug, Clone)]
pub struct SimRequest {
    pub chain_id: String,
    /// None = the chain's finalized head. A height pins the STATE the answer is
    /// true of, which is why it is lineage rather than a convenience.
    pub at_height: Option<u64>,
    /// SCALE-encoded `RuntimeCall` — exactly what a preimage holds.
    pub call: Vec<u8>,
    pub origin: OriginSpec,
    /// The origin expression as the caller wrote it, kept verbatim.
    pub origin_spec: String,
    /// `result_xcms_version`: the XCM version returned programs are rendered in.
    /// It changes the response bytes, so it is part of the input hash.
    pub xcm_version: u32,
}

/// Everything resolved and encoded, before anything is sent. Produced by
/// `DryRunner::prepare`; it is what the cache is keyed on, so the cache can be
/// consulted without a chain round trip for the dispatch itself.
#[derive(Debug, Clone)]
pub struct PreparedRun {
    pub chain_id: String,
    pub at_height: u64,
    pub at_block_hash: String,
    pub spec_version: u32,
    pub api_version: u32,
    /// Which metadata version's type registry built this request and must read
    /// its answer. Lineage (Invariant 3): the same bytes decoded against a
    /// different registry are a different claim, and a chain can offer both v15
    /// and v16 for one spec_version.
    pub metadata_version: u32,
    pub tier: String,
    /// The runtime-API method as it goes on the wire, e.g.
    /// `DryRunApi_dry_run_call`.
    pub method: String,
    /// The exact parameter bytes.
    pub params: Vec<u8>,
    /// blake2b-256 of `params`, 0x-hex.
    pub input_hash: String,
    pub call_hash: String,
    pub call_summary: Option<String>,
    pub origin_spec: String,
    pub origin_json: serde_json::Value,
    pub xcm_version: u32,
}

// ---------------------------------------------------------------- the answer

/// What the runtime said. `dispatch_failed` is a RESULT, never an error —
/// see the migration's note on why that distinction is load-bearing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SimStatus {
    Executed,
    DispatchFailed,
    ApiError,
}

impl SimStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Executed => "executed",
            Self::DispatchFailed => "dispatch_failed",
            Self::ApiError => "api_error",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimEvent {
    /// Named exactly as `core.events.name` names it — "balances.Transfer" — so
    /// a simulated effect and an indexed effect are comparable without a
    /// translation layer.
    pub name: String,
    pub data: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct SimOutcome {
    pub status: SimStatus,
    pub dispatch_ok: Option<bool>,
    pub dispatch_error: Option<serde_json::Value>,
    pub events: Vec<SimEvent>,
    pub local_xcm: Option<serde_json::Value>,
    /// `[{ "destination": <location>, "messages": [<xcm>, …] }, …]`
    pub forwarded_xcms: Vec<serde_json::Value>,
    /// The whole decoded effects payload, schema-on-read.
    pub effects: serde_json::Value,
    pub note: Option<String>,
}

/// One immutable observation, ready for `sim.simulation_results`.
#[derive(Debug, Clone)]
pub struct SimRecord {
    pub chain_id: String,
    pub at_block_hash: String,
    pub input_hash: String,
    pub at_height: u64,
    pub tier: String,
    pub call_hash: String,
    pub call_summary: Option<String>,
    pub origin_spec: String,
    pub origin_json: serde_json::Value,
    pub xcm_version: u32,
    pub status: String,
    pub dispatch_ok: Option<bool>,
    pub dispatch_error: Option<serde_json::Value>,
    pub emitted_events: serde_json::Value,
    pub event_count: u32,
    pub local_xcm: Option<serde_json::Value>,
    pub forwarded_xcms: serde_json::Value,
    pub effects: serde_json::Value,
    pub note: Option<String>,
    pub spec_version: u32,
    pub api_version: u32,
    pub metadata_version: u32,
    pub sim_version: u32,
    pub raw_location: String,
}

impl SimRecord {
    pub fn new(
        prepared: &PreparedRun,
        outcome: &SimOutcome,
        sim_version: u32,
        raw_location: String,
    ) -> Result<Self, SimError> {
        // Serialize BEFORE counting, and fail loudly if it ever cannot be done.
        // The tempting `.unwrap_or(json!([]))` would write `event_count: 2`
        // beside `emitted_events: []` — a row that says "this call emits no
        // events" while its own neighbouring column disagrees. Unreachable with
        // today's SimEvent, which is exactly when such a default gets written
        // and then survives a shape change unnoticed.
        let emitted_events = serde_json::to_value(&outcome.events)
            .map_err(|e| SimError::Encode(format!("serializing simulated events: {e}")))?;
        Ok(Self {
            chain_id: prepared.chain_id.clone(),
            at_block_hash: prepared.at_block_hash.clone(),
            input_hash: prepared.input_hash.clone(),
            at_height: prepared.at_height,
            tier: prepared.tier.clone(),
            call_hash: prepared.call_hash.clone(),
            call_summary: prepared.call_summary.clone(),
            origin_spec: prepared.origin_spec.clone(),
            origin_json: prepared.origin_json.clone(),
            xcm_version: prepared.xcm_version,
            status: outcome.status.as_str().to_string(),
            dispatch_ok: outcome.dispatch_ok,
            dispatch_error: outcome.dispatch_error.clone(),
            emitted_events,
            event_count: outcome.events.len() as u32,
            local_xcm: outcome.local_xcm.clone(),
            forwarded_xcms: serde_json::Value::Array(outcome.forwarded_xcms.clone()),
            effects: outcome.effects.clone(),
            note: outcome.note.clone(),
            spec_version: prepared.spec_version,
            api_version: prepared.api_version,
            metadata_version: prepared.metadata_version,
            sim_version,
            raw_location,
        })
    }
}

// ------------------------------------------------------------------ the seams

/// The protocol half. `prepare` and `dispatch` are separate so the cache can be
/// consulted between them: an input hash exists only once the params are
/// encoded, and encoding needs metadata but not the dispatch itself.
#[async_trait]
pub trait DryRunner: Send + Sync {
    /// Resolve state + encode the request. Nothing is executed here.
    async fn prepare(&self, req: &SimRequest) -> Result<PreparedRun, SimError>;
    /// Send the prepared parameters; returns the raw response bytes.
    async fn dispatch(&self, prepared: &PreparedRun) -> Result<Vec<u8>, SimError>;
    /// Pure: response bytes → outcome. Failure is loud and writes no row.
    fn interpret(&self, prepared: &PreparedRun, response: &[u8]) -> Result<SimOutcome, SimError>;
    /// Lineage stamp for rows this runner produces.
    fn sim_version(&self) -> u32;
}

#[async_trait]
pub trait SimStore: Send + Sync {
    /// The recorded answer for this TIER at this state and input.
    ///
    /// The tier is part of the key, not a filter applied afterwards, because the
    /// two tiers answer differently on purpose: a fork models scheduled dispatch
    /// and a dry run does not. Without it, the first Tier 2 run at a state a
    /// Tier 1 run already touched would report `cached` and hand back the
    /// dry-run answer — a fork simulation that never forked.
    async fn get(
        &self,
        chain_id: &str,
        at_block_hash: &str,
        input_hash: &str,
        tier: &str,
    ) -> Result<Option<SimRecord>, SimError>;
    async fn put(&self, record: &SimRecord) -> Result<(), SimError>;
}

#[derive(Debug)]
pub struct SimRun {
    pub record: SimRecord,
    /// True when the answer came from `sim.simulation_results` and the chain was
    /// never asked. An exact identifier at an exact state is immutable, so this
    /// is a real hit rate, not a heuristic one (ARCHITECTURE §9a).
    pub cached: bool,
}

/// prepare → cache → archive request → dispatch → archive response → interpret
/// → record. The one place the ordering lives, so Tier 2 inherits it.
pub async fn run_simulation(
    runner: &dyn DryRunner,
    store: &dyn SimStore,
    raw: &dyn raw_store::RawStore,
    receipts: &dyn ingest::ReceiptSink,
    req: &SimRequest,
) -> Result<SimRun, SimError> {
    let prepared = runner.prepare(req).await?;

    if let Some(hit) = store
        .get(
            &prepared.chain_id,
            &prepared.at_block_hash,
            &prepared.input_hash,
            &prepared.tier,
        )
        .await?
    {
        tracing::info!(
            chain = %prepared.chain_id, height = prepared.at_height,
            input = %prepared.input_hash, tier = %prepared.tier,
            "simulation already recorded for this tier, state and input — not re-running"
        );
        return Ok(SimRun {
            record: hit,
            cached: true,
        });
    }

    let block_hash_hex = prepared.at_block_hash.trim_start_matches("0x");
    let input_hex = prepared.input_hash.trim_start_matches("0x");
    // The artifacts NAME THE METHOD they belong to. `dry_run_xcm` and Tier 2's
    // fork inputs will land in this same directory, and bytes whose meaning
    // depends on knowing which call produced them are not self-describing
    // evidence — which is the whole justification for archiving them.
    let key = |item: &str| {
        raw_store::keys::simulation(
            &prepared.chain_id,
            block_hash_hex,
            input_hex,
            &format!("{}.{item}", prepared.method),
        )
    };

    // The request is archived first and unconditionally: if the dispatch dies
    // mid-flight, what we asked is still on record.
    archive(raw, receipts, &key("params.scale"), &prepared.params).await?;

    let response = runner.dispatch(&prepared).await?;
    let response_key = key("response.scale");
    archive(raw, receipts, &response_key, &response).await?;

    // Interpretation comes AFTER the archive, so an undecodable answer still
    // leaves the bytes that prove it was undecodable.
    let outcome = runner.interpret(&prepared, &response)?;
    let record = SimRecord::new(&prepared, &outcome, runner.sim_version(), response_key)?;
    store.put(&record).await?;
    Ok(SimRun {
        record,
        cached: false,
    })
}

async fn archive(
    raw: &dyn raw_store::RawStore,
    receipts: &dyn ingest::ReceiptSink,
    key: &str,
    bytes: &[u8],
) -> Result<(), SimError> {
    // Write-once. An identical re-put is a no-op; DIFFERENT bytes under the same
    // (state, input) key is a genuine contradiction — the same question, at the
    // same state, answered twice differently — and the store refusing it loudly
    // is the correct outcome, not an inconvenience to work around.
    let receipt = raw.put(key, bytes, "simulate")?;
    if let Err(e) = receipts.record(&receipt).await {
        tracing::warn!(error = %e, key, "simulation receipt not recorded — continuing");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    // `FsRawStore`'s get/put live on the RawStore TRAIT, not on the struct —
    // without this the tests do not compile (the same missing-trait-import that
    // was slice 4's only compile fix).
    use raw_store::RawStore as _;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    fn account(s: &str) -> Result<[u8; 32], String> {
        if s == "alice" {
            Ok([1u8; 32])
        } else {
            Err(format!("not an account: {s}"))
        }
    }

    #[test]
    fn origins_parse_into_one_encoder_path() {
        assert_eq!(
            OriginSpec::parse("root", account).unwrap(),
            OriginSpec::Variant {
                pallet: "system".into(),
                variant: "Root".into()
            },
            "root is sugar for system:Root, not a third case"
        );
        assert_eq!(
            OriginSpec::parse("  ROOT ", account).unwrap(),
            OriginSpec::parse("root", account).unwrap(),
            "case and surrounding space do not change an origin"
        );
        assert_eq!(
            OriginSpec::parse("none", account).unwrap(),
            OriginSpec::Variant {
                pallet: "system".into(),
                variant: "None".into()
            }
        );
        assert_eq!(
            OriginSpec::parse("Origins:MediumSpender", account).unwrap(),
            OriginSpec::Variant {
                pallet: "Origins".into(),
                variant: "MediumSpender".into()
            },
            "pallet-origin case is PRESERVED — the runtime's registry is case-sensitive"
        );
        assert_eq!(
            OriginSpec::parse("SIGNED:alice", account).unwrap(),
            OriginSpec::Signed([1u8; 32])
        );
        assert_eq!(
            OriginSpec::parse("signed:alice", account).unwrap().normalized(),
            format!("signed:0x{}", hex::encode([1u8; 32]))
        );
        // an unparseable account is the ACCOUNT's error, surfaced verbatim
        assert!(OriginSpec::parse("signed:bob", account)
            .unwrap_err()
            .to_string()
            .contains("not an account: bob"));
        for bad in ["", "   ", "medium_spender", ":Root", "Origins:"] {
            assert!(
                OriginSpec::parse(bad, account).is_err(),
                "'{bad}' must not parse as an origin"
            );
        }
    }

    // ------------------------------------------------------- orchestration
    struct MockRunner {
        dispatches: AtomicUsize,
        /// Set to make `interpret` fail, to prove archiving happened first.
        interpret_fails: bool,
    }

    #[async_trait]
    impl DryRunner for MockRunner {
        async fn prepare(&self, req: &SimRequest) -> Result<PreparedRun, SimError> {
            Ok(PreparedRun {
                chain_id: req.chain_id.clone(),
                at_height: req.at_height.unwrap_or(100),
                at_block_hash: "0xaa".into(),
                spec_version: 2003002,
                api_version: 2,
                metadata_version: 15,
                tier: TIER_DRY_RUN.into(),
                method: "DryRunApi_dry_run_call".into(),
                params: req.call.clone(),
                input_hash: "0xbb".into(),
                call_hash: "0xcc".into(),
                call_summary: Some("system.remark".into()),
                origin_spec: req.origin_spec.clone(),
                origin_json: serde_json::json!({"system": "Root"}),
                xcm_version: req.xcm_version,
            })
        }
        async fn dispatch(&self, _p: &PreparedRun) -> Result<Vec<u8>, SimError> {
            self.dispatches.fetch_add(1, Ordering::SeqCst);
            Ok(vec![0x11, 0x22])
        }
        fn interpret(&self, _p: &PreparedRun, _r: &[u8]) -> Result<SimOutcome, SimError> {
            if self.interpret_fails {
                return Err(SimError::Decode("response shape not recognised".into()));
            }
            Ok(SimOutcome {
                status: SimStatus::Executed,
                dispatch_ok: Some(true),
                dispatch_error: None,
                events: vec![SimEvent {
                    name: "balances.Transfer".into(),
                    data: serde_json::json!({"amount": 1}),
                }],
                local_xcm: None,
                forwarded_xcms: vec![],
                effects: serde_json::json!({"ok": true}),
                note: None,
            })
        }
        fn sim_version(&self) -> u32 {
            1
        }
    }

    #[derive(Default)]
    struct MemStore {
        rows: Mutex<Vec<SimRecord>>,
    }

    #[async_trait]
    impl SimStore for MemStore {
        async fn get(
            &self,
            chain_id: &str,
            at_block_hash: &str,
            input_hash: &str,
            tier: &str,
        ) -> Result<Option<SimRecord>, SimError> {
            Ok(self
                .rows
                .lock()
                .unwrap()
                .iter()
                .find(|r| {
                    r.chain_id == chain_id
                        && r.at_block_hash == at_block_hash
                        && r.input_hash == input_hash
                        && r.tier == tier
                })
                .cloned())
        }
        async fn put(&self, record: &SimRecord) -> Result<(), SimError> {
            self.rows.lock().unwrap().push(record.clone());
            Ok(())
        }
    }

    fn tmp_raw(tag: &str) -> (raw_store::FsRawStore, std::path::PathBuf) {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "dotlens-sim-{}-{tag}-{n}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        (raw_store::FsRawStore::new(&dir), dir)
    }

    fn request() -> SimRequest {
        SimRequest {
            chain_id: "polkadot-asset-hub".into(),
            at_height: Some(19_368_576),
            call: vec![0x00, 0x01],
            origin: OriginSpec::Variant {
                pallet: "system".into(),
                variant: "Root".into(),
            },
            origin_spec: "root".into(),
            xcm_version: 4,
        }
    }

    #[tokio::test]
    async fn a_recorded_answer_is_never_re_run_and_both_artifacts_are_archived() {
        let (raw, dir) = tmp_raw("cache");
        let runner = MockRunner {
            dispatches: AtomicUsize::new(0),
            interpret_fails: false,
        };
        let store = MemStore::default();
        let receipts = ingest::NoopReceiptSink;
        let req = request();

        let first = run_simulation(&runner, &store, &raw, &receipts, &req)
            .await
            .expect("first run");
        assert!(!first.cached);
        assert_eq!(first.record.event_count, 1);
        assert_eq!(first.record.status, "executed");
        assert_eq!(first.record.tier, TIER_DRY_RUN);
        assert_eq!(
            first.record.raw_location,
            "raw/polkadot-asset-hub/sim/aa/bb/DryRunApi_dry_run_call.response.scale",
            "the recorded location is the RESPONSE artifact, and it names the method \
             that produced it — the evidence a later sim_version re-derives from"
        );
        // BOTH sides of the exchange are on disk: what we asked and what we got.
        assert_eq!(
            raw.get("raw/polkadot-asset-hub/sim/aa/bb/DryRunApi_dry_run_call.params.scale")
                .unwrap(),
            vec![0x00, 0x01]
        );
        assert_eq!(
            raw.get("raw/polkadot-asset-hub/sim/aa/bb/DryRunApi_dry_run_call.response.scale")
                .unwrap(),
            vec![0x11, 0x22]
        );

        let second = run_simulation(&runner, &store, &raw, &receipts, &req)
            .await
            .expect("second run");
        assert!(second.cached, "same state + same input = the recorded answer");
        assert_eq!(
            runner.dispatches.load(Ordering::SeqCst),
            1,
            "the chain is asked once, however many times the question is"
        );

        // A DIFFERENT TIER at the same state and input is NOT this answer. The
        // fork tier models scheduled dispatch and the dry run does not, so
        // reusing one for the other would be a fork simulation that never
        // forked — hence the tier is part of the key, not a later filter.
        assert!(
            store
                .get("polkadot-asset-hub", "0xaa", "0xbb", "fork")
                .await
                .unwrap()
                .is_none(),
            "a Tier 1 row must not answer a Tier 2 question"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn an_undecodable_response_records_no_row_but_keeps_the_bytes() {
        let (raw, dir) = tmp_raw("undecodable");
        let runner = MockRunner {
            dispatches: AtomicUsize::new(0),
            interpret_fails: true,
        };
        let store = MemStore::default();
        let err = run_simulation(&runner, &store, &raw, &ingest::NoopReceiptSink, &request())
            .await
            .expect_err("interpretation failed, so the run must fail");
        assert!(matches!(err, SimError::Decode(_)), "loud, not swallowed");
        assert!(
            store.rows.lock().unwrap().is_empty(),
            "no row: bytes we could not read are never filed as a result"
        );
        assert_eq!(
            raw.get("raw/polkadot-asset-hub/sim/aa/bb/DryRunApi_dry_run_call.response.scale")
                .unwrap(),
            vec![0x11, 0x22],
            "but the response IS archived — it is the evidence of the failure, and \
             the state it came from cannot be re-read once pruned"
        );
        let _ = std::fs::remove_dir_all(dir);
    }
}
