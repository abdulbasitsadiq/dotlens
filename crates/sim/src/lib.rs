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
//!
//! ---------------------------------------------------------------------------
//! **PHASE 3, SLICE 5** adds the receiving side and the thing that makes it
//! honest.
//!
//! THE RECEIVING SIDE is `run_xcm_simulation` over the same ordering: a PROGRAM
//! from a LOCATION rather than a call under an origin, answered by an XCM
//! `Outcome` with three states rather than a dispatch `Result` with two.
//!
//! THE HONEST PART IS THE BASELINE. Slice 1 measured that `forwarded_xcms` is
//! not attributable to the simulated call: on the relay a Root `system.remark`,
//! which queues nothing, returns 64 destinations carrying 74 real in-flight
//! messages, byte-identical across two different calls and two different blocks.
//! The list is a property of the STATE. Chaining a call's forwarded messages
//! into the next chain's `dry_run_xcm` without differencing that away would
//! preview 74 unrelated messages as if a referendum had sent them — so the
//! difference lands here, in the layer both tiers share, rather than in a
//! reader that could forget it.
//!
//! A BASELINE IS AN ORDINARY RUN, which is why this costs almost nothing: the
//! no-op is prepared, dispatched, archived and recorded through the same path,
//! keyed the same way, and therefore CACHED — so every later simulation at that
//! state reuses it. On a chain whose list is empty the baseline is empty too and
//! the difference changes nothing, which is the correct outcome for a chain that
//! never had the problem.
//!
//! TWO ORCHESTRATIONS ARE NOT FIVE, and this file deliberately does not
//! generalise them. `ingest::module` was extracted after five hand-copies had
//! proved identical; here there are two, they differ in their subject and their
//! answer, and what they genuinely share — the archive-before-interpret ordering
//! and the artifact keys — is factored into [`SimArtifacts`]. A third tier or a
//! third method is when the loop itself is worth extracting.

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

/// Where an incoming XCM program is coming FROM, expressed in the RECEIVER's
/// frame of reference.
///
/// THE MIRROR IS THE WHOLE POINT, and getting it wrong does not fail loudly. A
/// sender's `forwarded_xcms` names the DESTINATION as the sender sees it —
/// Asset Hub addressing Hydration writes `{parents:1, X1[Parachain(2034)]}` —
/// while `dry_run_xcm` on Hydration must be told where the message came from,
/// which is `{parents:1, X1[Parachain(1000)]}`. Handing the destination back
/// unchanged would preview a message a chain sent to itself, and since barrier
/// checks and origin conversion are precisely what the receiving side turns on,
/// the answer would be confidently wrong rather than obviously broken.
///
/// It is FOUR CASES rather than a Location, because the four are what the
/// registry can derive without guessing: para ids and relay membership. An
/// address this vocabulary cannot express — a bridged origin, an account
/// origin — is refused rather than approximated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocationSpec {
    /// `{parents: 0, Here}` — the chain itself.
    Here,
    /// `{parents: 1, Here}` — the relay, as a parachain sees it.
    Parent,
    /// `{parents: 1, X1[Parachain(n)]}` — a sibling parachain.
    Sibling(u32),
    /// `{parents: 0, X1[Parachain(n)]}` — a child parachain, as the relay sees
    /// it. Distinct from `Sibling` by the parent count alone, which is exactly
    /// the distinction `xcm::transport_for_destination` had to make to stop
    /// calling every downward message HRMP.
    Child(u32),
}

impl LocationSpec {
    /// `here` | `parent` | `sibling:<para>` | `child:<para>`.
    ///
    /// `para:<n>` is deliberately NOT accepted: it does not say whether the
    /// sender is a sibling or a child, the two differ by a parent count, and a
    /// default would be a guess about the shape of the network.
    pub fn parse(spec: &str) -> Result<Self, SimError> {
        let trimmed = spec.trim();
        match trimmed.to_ascii_lowercase().as_str() {
            "here" => return Ok(Self::Here),
            "parent" => return Ok(Self::Parent),
            _ => {}
        }
        let Some((head, rest)) = trimmed.split_once(':') else {
            return Err(SimError::Encode(format!(
                "'{trimmed}' is not an origin location — expected here | parent | \
                 sibling:<para> | child:<para>"
            )));
        };
        let para: u32 = rest.trim().parse().map_err(|_| {
            SimError::Encode(format!("'{}' is not a parachain id", rest.trim()))
        })?;
        match head.trim().to_ascii_lowercase().as_str() {
            "sibling" => Ok(Self::Sibling(para)),
            "child" => Ok(Self::Child(para)),
            "para" => Err(SimError::Encode(format!(
                "'para:{para}' does not say whether the sender is a SIBLING (parents 1) or a \
                 CHILD (parents 0) of the receiving chain, and the two are different \
                 locations — say sibling:{para} or child:{para}"
            ))),
            other => Err(SimError::Encode(format!(
                "'{other}' is not an origin location kind — expected here | parent | \
                 sibling:<para> | child:<para>"
            ))),
        }
    }

    pub fn parents(&self) -> u8 {
        match self {
            Self::Here | Self::Child(_) => 0,
            Self::Parent | Self::Sibling(_) => 1,
        }
    }

    pub fn para_id(&self) -> Option<u32> {
        match self {
            Self::Sibling(n) | Self::Child(n) => Some(*n),
            _ => None,
        }
    }

    /// The COMPARABLE token — `here` | `parent` | `para:2034` — deliberately the
    /// same vocabulary `xcm.messages.counterparty` records for an OBSERVED
    /// arrival, so a previewed leg and a real one can be compared without a
    /// translation layer. It loses the sibling/child distinction on purpose; the
    /// parent count survives in the encoded location beside it.
    pub fn as_token(&self) -> String {
        match self {
            Self::Here => "here".into(),
            Self::Parent => "parent".into(),
            Self::Sibling(n) | Self::Child(n) => format!("para:{n}"),
        }
    }

    /// The round-trippable spelling this type parses.
    pub fn as_spec(&self) -> String {
        match self {
            Self::Here => "here".into(),
            Self::Parent => "parent".into(),
            Self::Sibling(n) => format!("sibling:{n}"),
            Self::Child(n) => format!("child:{n}"),
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
    /// `result_xcms_version` — an argument of `dry_run_call`. `None` on a fork
    /// row, which calls no runtime API and therefore has no such version;
    /// writing 0 would read as XCM v0, a real version and a wrong answer.
    pub xcm_version: Option<u32>,
    pub status: String,
    pub dispatch_ok: Option<bool>,
    pub dispatch_error: Option<serde_json::Value>,
    pub emitted_events: serde_json::Value,
    pub event_count: u32,
    pub local_xcm: Option<serde_json::Value>,
    /// `None` = this tier does not produce a forwarded list. An empty array
    /// would read as "this call queues no messages", which is a claim about the
    /// CALL where the truth is a fact about the TIER.
    pub forwarded_xcms: Option<serde_json::Value>,
    pub effects: serde_json::Value,
    pub note: Option<String>,
    pub spec_version: u32,
    /// The DryRunApi version that answered. `None` on a fork row — no runtime
    /// API was called, and recording the version the runtime happens to declare
    /// would invite a reader to believe it was used.
    pub api_version: Option<u32>,
    pub metadata_version: u32,
    pub sim_version: u32,
    pub raw_location: String,
    /// The no-op run at this same (chain, block hash, tier) whose
    /// `forwarded_xcms` is the ambient queue at this state. `None` = no baseline
    /// was recorded, so `forwarded_xcms` means "messages present" and never
    /// "this call would send these".
    pub baseline_input_hash: Option<String>,

    // ---------------------------------------------------- Tier 2 (slice 8)
    // All `None` on a dry_run row. See migration 0021.
    /// The resolved override set. `None` = NOT A COUNTERFACTUAL; an empty array
    /// would be indistinguishable from a counterfactual that injects nothing,
    /// and every rendering of this row turns on the difference.
    pub overrides: Option<serde_json::Value>,
    /// blake2b-256 over the canonical override encoding, folded into
    /// `input_hash`. Kept beside the set so the fold is verifiable without
    /// re-deriving the whole request — and so a database CHECK can insist that a
    /// counterfactual is never recorded without it.
    pub override_hash: Option<String>,
    pub storage_diff: Option<serde_json::Value>,
    pub storage_diff_count: Option<u32>,
    /// decoded | extrinsic_only | undecodable | unavailable | refused — see
    /// [`DIFF_STATUSES`]. Not a boolean: "we did not look" and
    /// "nothing changed" must never be the same value.
    pub diff_status: Option<String>,
    /// The hash of a block that exists ONLY ON THE FORK.
    pub built_block_hash: Option<String>,
    /// Which engine spoke, and what it mocks.
    pub harness: Option<serde_json::Value>,
    /// scheduled | dry_run_extrinsic — see [`PreparedFork::dispatch_route`].
    pub dispatch_route: Option<String>,
    /// The agenda anchor decision and its evidence. The column that turns an
    /// unexplained `not_dispatched` into a readable one.
    pub agenda_anchor: Option<serde_json::Value>,
}

impl SimRecord {
    pub fn new(
        prepared: &PreparedRun,
        outcome: &SimOutcome,
        sim_version: u32,
        raw_location: String,
        baseline_input_hash: Option<String>,
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
            xcm_version: Some(prepared.xcm_version),
            status: outcome.status.as_str().to_string(),
            dispatch_ok: outcome.dispatch_ok,
            dispatch_error: outcome.dispatch_error.clone(),
            emitted_events,
            event_count: outcome.events.len() as u32,
            local_xcm: outcome.local_xcm.clone(),
            forwarded_xcms: Some(serde_json::Value::Array(outcome.forwarded_xcms.clone())),
            effects: outcome.effects.clone(),
            note: outcome.note.clone(),
            spec_version: prepared.spec_version,
            api_version: Some(prepared.api_version),
            metadata_version: prepared.metadata_version,
            sim_version,
            raw_location,
            baseline_input_hash,
            overrides: None,
            override_hash: None,
            storage_diff: None,
            storage_diff_count: None,
            diff_status: None,
            built_block_hash: None,
            harness: None,
            dispatch_route: None,
            agenda_anchor: None,
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

    /// The no-op request whose `forwarded_xcms` is the ambient queue at
    /// `prepared`'s state — same chain, same height, an origin and a call that
    /// provably queue nothing.
    ///
    /// `None` is an honest answer, not a failure: a runtime with no expressible
    /// no-op simply has no baseline, `baseline_input_hash` stays NULL, and every
    /// reader of that row is told the forwarded list is unattributed. Guessing
    /// one would be worse than not having it — a baseline that is not really a
    /// no-op would SUBTRACT this call's own messages.
    ///
    /// It is derived from the PREPARED run rather than the request, because the
    /// baseline must land at the same state and `prepare` is what resolved the
    /// height.
    fn baseline_request(&self, prepared: &PreparedRun) -> Result<Option<SimRequest>, SimError>;
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

/// The archive half of the ordering, shared by both methods.
///
/// The artifacts NAME THE METHOD they belong to, which is what lets
/// `dry_run_call`, `dry_run_xcm` and Tier 2's fork inputs share one directory
/// per (state, input): bytes whose meaning depends on knowing which call
/// produced them are not self-describing evidence, and being self-describing
/// evidence is the entire justification for archiving them.
pub struct SimArtifacts<'a> {
    raw: &'a dyn raw_store::RawStore,
    receipts: &'a dyn ingest::ReceiptSink,
    chain_id: &'a str,
    block_hash_hex: &'a str,
    input_hex: &'a str,
    method: &'a str,
}

impl<'a> SimArtifacts<'a> {
    pub fn new(
        raw: &'a dyn raw_store::RawStore,
        receipts: &'a dyn ingest::ReceiptSink,
        chain_id: &'a str,
        at_block_hash: &'a str,
        input_hash: &'a str,
        method: &'a str,
    ) -> Self {
        Self {
            raw,
            receipts,
            chain_id,
            block_hash_hex: at_block_hash.trim_start_matches("0x"),
            input_hex: input_hash.trim_start_matches("0x"),
            method,
        }
    }

    pub fn key(&self, item: &str) -> String {
        raw_store::keys::simulation(
            self.chain_id,
            self.block_hash_hex,
            self.input_hex,
            &format!("{}.{item}", self.method),
        )
    }

    /// Archived first and unconditionally: if the dispatch dies mid-flight, what
    /// we asked is still on record.
    pub async fn put_params(&self, params: &[u8]) -> Result<(), SimError> {
        self.archive(&self.key("params.scale"), params).await
    }

    /// Archived BEFORE interpretation, so an answer we cannot read still leaves
    /// the bytes that prove it was unreadable. Returns the key, which is the
    /// row's `raw_location`.
    pub async fn put_response(&self, response: &[u8]) -> Result<String, SimError> {
        let key = self.key("response.scale");
        self.archive(&key, response).await?;
        Ok(key)
    }

    async fn archive(&self, key: &str, bytes: &[u8]) -> Result<(), SimError> {
        // Write-once. An identical re-put is a no-op; DIFFERENT bytes under the
        // same (state, input) key is a genuine contradiction — the same
        // question, at the same state, answered twice differently — and the
        // store refusing it loudly is the correct outcome, not an inconvenience
        // to work around.
        let receipt = self.raw.put(key, bytes, "simulate")?;
        if let Err(e) = self.receipts.record(&receipt).await {
            tracing::warn!(error = %e, key, "simulation receipt not recorded — continuing");
        }
        Ok(())
    }
}

/// prepare → cache → baseline → archive request → dispatch → archive response →
/// interpret → record. The one place the ordering lives, so Tier 2 inherits it.
///
/// IT IS FLAT RATHER THAN RECURSIVE, deliberately. A baseline run is itself a
/// simulation, so "call this function again with the no-op" reads well — and
/// would need a boxed indirection to compile at all, and would make the one
/// invariant that matters (a baseline never triggers a baseline) a property of
/// an argument rather than of the shape. Splitting the DISPATCH half out into
/// [`execute_and_record`] says it structurally: only this function decides about
/// baselines, and it can only decide once.
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

    // THE BASELINE RUNS FIRST, and it is recorded exactly like any other answer
    // — so it is CACHED by the same key, and the second simulation at a given
    // state pays nothing for it.
    let mut baseline_input_hash = None;
    if let Some(baseline_req) = runner.baseline_request(&prepared)? {
        let baseline = runner.prepare(&baseline_req).await?;
        if baseline.input_hash == prepared.input_hash {
            // The subject IS the no-op. It is its own baseline: differencing a
            // list against itself is the empty set, which is exactly what a call
            // that queues nothing sent — and there is one dispatch, not two.
            baseline_input_hash = Some(prepared.input_hash.clone());
        } else if baseline.at_block_hash != prepared.at_block_hash {
            // Both requests name a HEIGHT, and a height can name two forks.
            // Rather than record a link a reader would have to distrust, record
            // none — and the API then says nothing here is attributable.
            tracing::warn!(
                chain = %prepared.chain_id,
                subject = %prepared.at_block_hash,
                baseline = %baseline.at_block_hash,
                "baseline resolved to a different block hash at the same height — recording \
                 no attribution link"
            );
        } else {
            let own = baseline.input_hash.clone();
            if store
                .get(
                    &baseline.chain_id,
                    &baseline.at_block_hash,
                    &own,
                    &baseline.tier,
                )
                .await?
                .is_none()
            {
                execute_and_record(
                    runner,
                    store,
                    raw,
                    receipts,
                    &baseline,
                    Some(own.clone()),
                )
                .await?;
            }
            baseline_input_hash = Some(own);
        }
    }

    execute_and_record(runner, store, raw, receipts, &prepared, baseline_input_hash).await
}

/// archive request → dispatch → archive response → interpret → record.
async fn execute_and_record(
    runner: &dyn DryRunner,
    store: &dyn SimStore,
    raw: &dyn raw_store::RawStore,
    receipts: &dyn ingest::ReceiptSink,
    prepared: &PreparedRun,
    baseline_input_hash: Option<String>,
) -> Result<SimRun, SimError> {
    let artifacts = SimArtifacts::new(
        raw,
        receipts,
        &prepared.chain_id,
        &prepared.at_block_hash,
        &prepared.input_hash,
        &prepared.method,
    );
    artifacts.put_params(&prepared.params).await?;
    let response = runner.dispatch(prepared).await?;
    let response_key = artifacts.put_response(&response).await?;

    let outcome = runner.interpret(prepared, &response)?;
    let record = SimRecord::new(
        prepared,
        &outcome,
        runner.sim_version(),
        response_key,
        baseline_input_hash,
    )?;
    store.put(&record).await?;
    Ok(SimRun {
        record,
        cached: false,
    })
}

// ============================================================================
// THE RECEIVING SIDE — dry_run_xcm
// ============================================================================

/// What the receiving runtime said about a program.
///
/// FOUR STATES, AND THE THIRD IS THE ONE THIS EXISTS FOR. Upstream's `Outcome`
/// enum is `Complete | Incomplete | Error`, where `Error` means execution NEVER
/// STARTED — a barrier rejection, an unreadable version — and is renamed here to
/// `NotStarted` because "Error" reads as a failure of the request. It is the
/// answer a sending chain structurally cannot give: a message rejected on
/// arrival still leaves a perfectly successful `Sent` behind it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum XcmSimStatus {
    Complete,
    Incomplete,
    NotStarted,
    ApiError,
}

impl XcmSimStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Incomplete => "incomplete",
            Self::NotStarted => "not_started",
            Self::ApiError => "api_error",
        }
    }
}

#[derive(Debug, Clone)]
pub struct XcmSimOutcome {
    pub status: XcmSimStatus,
    pub weight_used: Option<serde_json::Value>,
    /// The XCM error in the shape the runtime rendered it — a bare `Error` on
    /// XCM v4, an `InstructionError {index, error}` on v5. Not normalised: the
    /// index is real information on v5 and inventing one for v4 would be a
    /// guess.
    pub xcm_error: Option<serde_json::Value>,
    pub events: Vec<SimEvent>,
    pub forwarded_xcms: Vec<serde_json::Value>,
    pub effects: serde_json::Value,
    pub note: Option<String>,
}

/// Where a program came from, when it came from a recorded call simulation
/// rather than a caller's hands. Both indices, because a destination carries a
/// LIST of messages.
#[derive(Debug, Clone)]
pub struct ProgramSource {
    pub chain_id: String,
    pub at_block_hash: String,
    pub input_hash: String,
    pub forwarded_index: u32,
    pub message_index: u32,
}

/// One program to preview on the chain it is addressed to.
#[derive(Debug, Clone)]
pub struct XcmSimRequest {
    pub chain_id: String,
    pub at_height: Option<u64>,
    /// The sender, in the RECEIVER's frame. See [`LocationSpec`].
    pub origin: LocationSpec,
    /// SCALE-encoded `VersionedXcm`.
    pub program: Vec<u8>,
    pub source: Option<ProgramSource>,
}

#[derive(Debug, Clone)]
pub struct PreparedXcmRun {
    pub chain_id: String,
    pub at_height: u64,
    pub at_block_hash: String,
    pub spec_version: u32,
    pub api_version: u32,
    pub metadata_version: u32,
    pub tier: String,
    pub method: String,
    pub params: Vec<u8>,
    pub input_hash: String,
    /// blake2b-256 of the encoded `VersionedXcm` — recomputable by anyone
    /// holding the same bytes.
    pub program_hash: String,
    pub program: serde_json::Value,
    pub program_summary: Option<String>,
    /// The origin as it was ASKED FOR, kept whole beside its encoded form. The
    /// baseline run needs to restate it exactly, and reconstructing it from
    /// `origin_ref` alone is impossible — `para:2034` is a sibling at parents 1
    /// and a child at parents 0 — while reconstructing it from the encoded JSON
    /// would be a parser standing where the value already is.
    pub origin: LocationSpec,
    pub origin_ref: String,
    pub origin_location: serde_json::Value,
    pub source: Option<ProgramSource>,
}

#[derive(Debug, Clone)]
pub struct XcmSimRecord {
    pub chain_id: String,
    pub at_block_hash: String,
    pub input_hash: String,
    pub at_height: u64,
    pub tier: String,
    pub program_hash: String,
    pub program: serde_json::Value,
    pub program_summary: Option<String>,
    pub origin_location: serde_json::Value,
    pub origin_ref: String,
    pub status: String,
    pub weight_used: Option<serde_json::Value>,
    pub xcm_error: Option<serde_json::Value>,
    pub emitted_events: serde_json::Value,
    pub event_count: u32,
    pub forwarded_xcms: serde_json::Value,
    pub baseline_input_hash: Option<String>,
    pub effects: serde_json::Value,
    pub note: Option<String>,
    pub source_chain_id: Option<String>,
    pub source_at_block_hash: Option<String>,
    pub source_input_hash: Option<String>,
    pub source_forwarded_index: Option<u32>,
    pub source_message_index: Option<u32>,
    pub spec_version: u32,
    pub api_version: u32,
    pub metadata_version: u32,
    pub sim_version: u32,
    pub raw_location: String,
}

impl XcmSimRecord {
    pub fn new(
        prepared: &PreparedXcmRun,
        outcome: &XcmSimOutcome,
        sim_version: u32,
        raw_location: String,
        baseline_input_hash: Option<String>,
    ) -> Result<Self, SimError> {
        // Serialized before it is counted, and loudly — the same rule the call
        // side states: `event_count: 2` beside `emitted_events: []` is a row
        // that contradicts itself.
        let emitted_events = serde_json::to_value(&outcome.events)
            .map_err(|e| SimError::Encode(format!("serializing simulated events: {e}")))?;
        Ok(Self {
            chain_id: prepared.chain_id.clone(),
            at_block_hash: prepared.at_block_hash.clone(),
            input_hash: prepared.input_hash.clone(),
            at_height: prepared.at_height,
            tier: prepared.tier.clone(),
            program_hash: prepared.program_hash.clone(),
            program: prepared.program.clone(),
            program_summary: prepared.program_summary.clone(),
            origin_location: prepared.origin_location.clone(),
            origin_ref: prepared.origin_ref.clone(),
            status: outcome.status.as_str().to_string(),
            weight_used: outcome.weight_used.clone(),
            xcm_error: outcome.xcm_error.clone(),
            emitted_events,
            event_count: outcome.events.len() as u32,
            forwarded_xcms: serde_json::Value::Array(outcome.forwarded_xcms.clone()),
            baseline_input_hash,
            effects: outcome.effects.clone(),
            note: outcome.note.clone(),
            source_chain_id: prepared.source.as_ref().map(|s| s.chain_id.clone()),
            source_at_block_hash: prepared.source.as_ref().map(|s| s.at_block_hash.clone()),
            source_input_hash: prepared.source.as_ref().map(|s| s.input_hash.clone()),
            source_forwarded_index: prepared.source.as_ref().map(|s| s.forwarded_index),
            source_message_index: prepared.source.as_ref().map(|s| s.message_index),
            spec_version: prepared.spec_version,
            api_version: prepared.api_version,
            metadata_version: prepared.metadata_version,
            sim_version,
            raw_location,
        })
    }
}

#[async_trait]
pub trait XcmDryRunner: Send + Sync {
    async fn prepare_xcm(&self, req: &XcmSimRequest) -> Result<PreparedXcmRun, SimError>;
    async fn dispatch_xcm(&self, prepared: &PreparedXcmRun) -> Result<Vec<u8>, SimError>;
    fn interpret_xcm(
        &self,
        prepared: &PreparedXcmRun,
        response: &[u8],
    ) -> Result<XcmSimOutcome, SimError>;
    fn sim_version(&self) -> u32;
    /// The EMPTY PROGRAM from the same origin at the same state — a program that
    /// executes nothing, so whatever it reports as forwarded was already in
    /// flight. Same role as the call side's `system.remark`, and the same
    /// honest `None` when a runtime cannot express one.
    fn baseline_request(&self, prepared: &PreparedXcmRun) -> Result<Option<XcmSimRequest>, SimError>;
}

#[async_trait]
pub trait XcmSimStore: Send + Sync {
    async fn get(
        &self,
        chain_id: &str,
        at_block_hash: &str,
        input_hash: &str,
        tier: &str,
    ) -> Result<Option<XcmSimRecord>, SimError>;
    async fn put(&self, record: &XcmSimRecord) -> Result<(), SimError>;
}

#[derive(Debug)]
pub struct XcmSimRun {
    pub record: XcmSimRecord,
    pub cached: bool,
}

/// The receiving side's orchestration — the same ordering and the same flat
/// baseline shape as `run_simulation`, over a different subject and a different
/// answer.
pub async fn run_xcm_simulation(
    runner: &dyn XcmDryRunner,
    store: &dyn XcmSimStore,
    raw: &dyn raw_store::RawStore,
    receipts: &dyn ingest::ReceiptSink,
    req: &XcmSimRequest,
) -> Result<XcmSimRun, SimError> {
    let prepared = runner.prepare_xcm(req).await?;

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
            "xcm simulation already recorded for this tier, state and input — not re-running"
        );
        return Ok(XcmSimRun {
            record: hit,
            cached: true,
        });
    }

    let mut baseline_input_hash = None;
    if let Some(baseline_req) = runner.baseline_request(&prepared)? {
        let baseline = runner.prepare_xcm(&baseline_req).await?;
        if baseline.input_hash == prepared.input_hash {
            // The subject IS the empty program. It is its own baseline.
            baseline_input_hash = Some(prepared.input_hash.clone());
        } else if baseline.at_block_hash != prepared.at_block_hash {
            tracing::warn!(
                chain = %prepared.chain_id,
                subject = %prepared.at_block_hash,
                baseline = %baseline.at_block_hash,
                "xcm baseline resolved to a different block hash at the same height — \
                 recording no attribution link"
            );
        } else {
            let own = baseline.input_hash.clone();
            if store
                .get(
                    &baseline.chain_id,
                    &baseline.at_block_hash,
                    &own,
                    &baseline.tier,
                )
                .await?
                .is_none()
            {
                execute_and_record_xcm(
                    runner,
                    store,
                    raw,
                    receipts,
                    &baseline,
                    Some(own.clone()),
                )
                .await?;
            }
            baseline_input_hash = Some(own);
        }
    }

    execute_and_record_xcm(runner, store, raw, receipts, &prepared, baseline_input_hash).await
}

async fn execute_and_record_xcm(
    runner: &dyn XcmDryRunner,
    store: &dyn XcmSimStore,
    raw: &dyn raw_store::RawStore,
    receipts: &dyn ingest::ReceiptSink,
    prepared: &PreparedXcmRun,
    baseline_input_hash: Option<String>,
) -> Result<XcmSimRun, SimError> {
    let artifacts = SimArtifacts::new(
        raw,
        receipts,
        &prepared.chain_id,
        &prepared.at_block_hash,
        &prepared.input_hash,
        &prepared.method,
    );
    artifacts.put_params(&prepared.params).await?;
    let response = runner.dispatch_xcm(prepared).await?;
    let response_key = artifacts.put_response(&response).await?;

    let outcome = runner.interpret_xcm(prepared, &response)?;
    let record = XcmSimRecord::new(
        prepared,
        &outcome,
        runner.sim_version(),
        response_key,
        baseline_input_hash,
    )?;
    store.put(&record).await?;
    Ok(XcmSimRun {
        record,
        cached: false,
    })
}

// ============================================================================
// ATTRIBUTION — which forwarded messages are actually this run's doing
// ============================================================================

#[derive(Debug, Clone, Serialize)]
pub struct AttributedMessage {
    /// Position within the destination's message list, so the caller can ask
    /// the adapter for exactly these bytes.
    pub message_index: usize,
    pub program: serde_json::Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct AttributedDestination {
    /// Position in the SUBJECT's `forwarded_xcms`.
    pub destination_index: usize,
    pub destination: serde_json::Value,
    pub messages: Vec<AttributedMessage>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Attribution {
    pub destinations: Vec<AttributedDestination>,
    /// How many messages this run is responsible for.
    pub attributed_messages: usize,
    /// How many the no-op reported at the same state — messages that were
    /// already in flight and belong to nobody in this request.
    pub ambient_messages: usize,
    /// The subject's own total, so the two numbers can be read against each
    /// other without re-counting the raw list.
    pub total_messages: usize,
}

/// Subject `forwarded_xcms` minus baseline `forwarded_xcms`.
///
/// BOTH ARGUMENTS ARE THE STORED SHAPE — `[{"destination": …, "messages": […]},
/// …]` — and equality is plain JSON equality, deliberately, with no
/// canonicalisation. The two lists were produced by the SAME runtime against the
/// SAME state through the SAME decoder, so identical bytes give identical
/// renderings; a canonicaliser here would be a second opinion about what
/// "identical" means, and the first one is already exact.
///
/// It is a MULTISET difference: two copies of one message in the subject and one
/// in the baseline leave one attributed. Destinations are matched by their
/// rendered location, and a destination absent from the baseline is attributable
/// in full.
///
/// THE ONE WAY THIS IS WRONG, stated rather than discovered later: a message
/// this run really sends that is BYTE-IDENTICAL to one already in flight is
/// consumed by the difference and disappears from the attributed set. That is
/// under-attribution, never over-attribution — the direction that refuses to
/// claim rather than the one that invents — and the raw list is kept beside the
/// difference so the discrepancy is visible rather than hidden. In practice a
/// `SetTopic` carrying `frame_system::unique` entropy makes two genuinely
/// distinct messages differ, but that is a property of today's runtimes and not
/// a guarantee, which is why it is written down here.
pub fn attribute_forwarded(
    subject: &serde_json::Value,
    baseline: &serde_json::Value,
) -> Attribution {
    let empty = Vec::new();
    let subject_entries = subject.as_array().unwrap_or(&empty);
    let baseline_entries = baseline.as_array().unwrap_or(&empty);

    let messages_of = |entry: &serde_json::Value| -> Vec<serde_json::Value> {
        entry
            .get("messages")
            .and_then(|m| m.as_array())
            .cloned()
            .unwrap_or_default()
    };

    let ambient_messages: usize = baseline_entries.iter().map(|e| messages_of(e).len()).sum();
    let total_messages: usize = subject_entries.iter().map(|e| messages_of(e).len()).sum();

    let mut destinations = Vec::new();
    let mut attributed_messages = 0usize;
    for (destination_index, entry) in subject_entries.iter().enumerate() {
        let destination = entry.get("destination").cloned().unwrap_or_default();
        // A pool per destination, consumed as messages are matched — this is
        // what makes it a multiset difference rather than a set one.
        //
        // EVERY matching baseline entry contributes, not just the first: nothing
        // promises a runtime lists a destination once, and taking only the first
        // pool would leave a second entry's messages unmatched and attribute
        // them to this run.
        let mut pool: Vec<serde_json::Value> = baseline_entries
            .iter()
            .filter(|b| b.get("destination") == entry.get("destination"))
            .flat_map(messages_of)
            .collect();

        let mut kept = Vec::new();
        for (message_index, program) in messages_of(entry).into_iter().enumerate() {
            if let Some(pos) = pool.iter().position(|p| *p == program) {
                pool.remove(pos);
                continue;
            }
            kept.push(AttributedMessage {
                message_index,
                program,
            });
        }
        if !kept.is_empty() {
            attributed_messages += kept.len();
            destinations.push(AttributedDestination {
                destination_index,
                destination,
                messages: kept,
            });
        }
    }

    Attribution {
        destinations,
        attributed_messages,
        ambient_messages,
        total_messages,
    }
}

// ============================================================================
// TIER 2 — THE FORK (Phase 3, slice 8)
// ============================================================================
//
// The same ordering as Tier 1 — prepare → cache → archive request → dispatch →
// archive response → interpret → record — over a subject that is asked
// differently and answered differently.
//
// THREE THINGS ARE DELIBERATELY ABSENT, and each absence is a decision:
//
//   * NO BASELINE. Tier 1 needs one because `forwarded_xcms` is a property of
//     the STATE and not of the call, so a no-op run is the only way to tell the
//     two apart. This tier produces no forwarded list at all (it calls no runtime
//     API), so there is nothing to difference and a baseline here would be a
//     second expensive run answering a question nobody asked.
//   * NO SECOND ORCHESTRATION FOR THE QUEUE. `run_fork_simulation` is what the
//     job worker calls and it is also what the CLI calls; the queue is a trigger,
//     not a second path. Two implementations of "run a Tier 2 simulation" that
//     could diverge is exactly the defect class this project keeps finding.
//   * NO GENERALISATION OF THE THREE ORCHESTRATIONS. `ingest::module` was
//     extracted after FIVE hand-copies had proved identical. There are three
//     here, they differ in their subject, their answer and (for this one) their
//     absence of a baseline, and what they genuinely share is already factored
//     into [`SimArtifacts`].

/// The tier tag written to `sim.simulation_results.tier` for a fork run.
pub const TIER_FORK: &str = "fork";

// ============================================================================
// WHAT A STORAGE DIFF COVERS — the vocabulary, and the two rules that read it
// ============================================================================
//
// THIS LIVES IN `sim` AND NOT IN THE ADAPTER, and the reason is the same one
// that made `api` depend on this crate in slice 5. Both the RUNNER (which
// decides what to record) and the READER (which decides how to render it) have
// to answer "does this diff cover the call", and two implementations of that
// question that could disagree is the defect class this project keeps finding —
// `shared_decimals` in slice 7 was the last one. The adapter owns the part that
// is genuinely protocol: mapping a particular harness's answer onto this
// vocabulary (`adapter_substrate::fork::diff_scope_from_answer`). What a status
// MEANS is tier vocabulary, and tier vocabulary is this crate's, beside
// [`TIER_FORK`].

/// Every phase of the block was returned and read.
pub const DIFF_STATUS_DECODED: &str = "decoded";
/// The bytes were read and decoded and cover the `apply_extrinsic` phase ONLY —
/// block initialization and the inherents are not in them.
///
/// The ordinary case on the live Tier 2 route, measured 2026-08-18 from the
/// harness's own source. See `adapter_substrate::fork::DIFF_METHOD_DRY_RUN` for
/// where it comes from and [`diff_covers_subject`] for when it matters.
pub const DIFF_STATUS_EXTRINSIC_ONLY: &str = "extrinsic_only";
/// A diff came back in a shape that version could not read. The bytes are
/// archived; nothing is guessed.
pub const DIFF_STATUS_UNDECODABLE: &str = "undecodable";
/// This build of the harness exposes no diff method at all.
pub const DIFF_STATUS_UNAVAILABLE: &str = "unavailable";
/// It HAS a diff method and it failed on this block.
pub const DIFF_STATUS_REFUSED: &str = "refused";

/// The whole vocabulary, in the order migration 0023's CHECK lists it.
pub const DIFF_STATUSES: [&str; 5] = [
    DIFF_STATUS_DECODED,
    DIFF_STATUS_EXTRINSIC_ONLY,
    DIFF_STATUS_UNDECODABLE,
    DIFF_STATUS_UNAVAILABLE,
    DIFF_STATUS_REFUSED,
];

/// Is there a diff to record at all?
///
/// GATED ON THE PRESENT VALUES, NOT ON `!= "unavailable"`. Slice 8 wrote this
/// rule as a literal `== "decoded"` at its one call site, and adding a fifth
/// value to the vocabulary would then have made every diff on the live route
/// fall through to NULL — a column that silently stops being written, beside a
/// status claiming the bytes were read. So it is a function with a test, and a
/// sixth value has to be classified here or the test fails.
pub fn diff_is_present(status: &str) -> bool {
    matches!(status, DIFF_STATUS_DECODED | DIFF_STATUS_EXTRINSIC_ONLY)
}

/// Does the diff cover the thing that was SIMULATED?
///
/// NOT THE SAME QUESTION AS [`diff_is_present`], and on the scheduled route the
/// two answers differ — which is the defect slice 10 exists for. A privileged
/// call is dispatched by `pallet_scheduler` in `on_initialize`, and
/// `extrinsic_only` is precisely the scope that omits it, so a scheduled row's
/// diff describes the NO-OP VEHICLE and nothing else. Reading it as "this call
/// changed twelve keys" is the most confidently wrong thing this tier can serve.
///
/// ON THE EXTRINSIC ROUTE THE SAME BYTES ARE COMPLETE, and that is worth saying
/// rather than carrying the limitation onto a route where it is not one: the
/// subject IS the extrinsic, so its whole effect is in the phase the diff covers.
/// A whole-block diff there would be strictly WORSE for attribution — it would
/// mix in every pallet's `on_initialize` bookkeeping, which the call did not do.
///
/// THE ROUTE IS A PARAMETER RATHER THAN A COLUMN OF ITS OWN for the reason
/// migration 0023 states: "does the diff cover the subject" is a function of two
/// facts the row already carries, and a stored third copy would be the derived
/// column this project has refused four times.
pub fn diff_covers_subject(status: &str, route: &str) -> bool {
    match status {
        DIFF_STATUS_DECODED => true,
        DIFF_STATUS_EXTRINSIC_ONLY => route == ROUTE_DRY_RUN_EXTRINSIC,
        _ => false,
    }
}

/// `dispatch_route` — the call went through `pallet_scheduler`'s agenda, because
/// no RPC can express a privileged origin.
pub const ROUTE_SCHEDULED: &str = "scheduled";
/// `dispatch_route` — the call was applied as an extrinsic, which a `signed:`
/// origin simply is.
pub const ROUTE_DRY_RUN_EXTRINSIC: &str = "dry_run_extrinsic";

/// One resolved storage override, as it is stored on the result row.
///
/// EVERYTHING IS HEX AND JSON, so this type is protocol-free: the resolution
/// happened in `adapter_substrate::fork` against a runtime's own metadata, and
/// what arrives here is bytes and a rendering.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForkOverride {
    /// The spec exactly as the caller wrote it.
    pub spec: String,
    /// What it resolved to — `Assets.Account(1337, 0x…)`.
    pub resolved: String,
    pub key: String,
    /// The value INJECTED. `None` = this override deletes the key.
    pub value: Option<String>,
    /// The value the REAL chain held at this block, read over ordinary RPC from
    /// the real endpoint before anything was forked.
    ///
    /// THIS IS THE FIELD THAT MAKES THE FABRICATION LEGIBLE. Without it a
    /// counterfactual row and a faithful one look alike; with it, the row itself
    /// says "the chain held X here and the fork was told Y". `None` means the key
    /// did not exist on the real chain — i.e. this override CREATED it, which is
    /// a stronger fabrication than changing a number and reads as one.
    pub before: Option<String>,
    /// `before`/`value` decoded against the item's declared type, where they
    /// could be. A rendering, never the authority — the hex is.
    pub decoded_before: Option<serde_json::Value>,
    pub decoded_after: Option<serde_json::Value>,
}

/// One Tier 2 ask.
#[derive(Debug, Clone)]
pub struct ForkRequest {
    pub chain_id: String,
    pub at_height: Option<u64>,
    /// SCALE-encoded `RuntimeCall`.
    pub call: Vec<u8>,
    pub origin: OriginSpec,
    pub origin_spec: String,
    /// Override specs AS WRITTEN. Resolved during `prepare_fork`, because
    /// resolving them needs the runtime's metadata AND a read of the real chain,
    /// both of which are only available once the state is pinned.
    pub override_specs: Vec<String>,
    /// Who signs the extrinsic the fork actually applies.
    ///
    /// ON THE SCHEDULED ROUTE THIS IS NOT THE ORIGIN. The call is dispatched by
    /// `pallet_scheduler` under whatever privileged origin was asked for; the
    /// extrinsic is a no-op whose only job is to make the block execute, and this
    /// account pays its fee. It therefore has to be one that CAN — which is why
    /// it is a required argument rather than a derived throwaway: funding an
    /// invented account means encoding an `AccountInfo` whose field names have
    /// changed across pallet-balances versions, and guessing that shape wrong
    /// produces a fork that refuses to apply anything for a reason that looks
    /// like the call. Auto-funding it is a later slice; naming a funded account
    /// is one flag.
    ///
    /// On the extrinsic route it is the origin's own account, so the two coincide.
    pub signer: [u8; 32],
}

#[derive(Debug, Clone)]
pub struct PreparedFork {
    pub chain_id: String,
    pub at_height: u64,
    pub at_block_hash: String,
    pub spec_version: u32,
    pub metadata_version: u32,
    pub tier: String,
    pub method: String,
    /// blake2b-256 over the canonical fork request encoding — the domain tag,
    /// the origin bytes, the call bytes and the resolved override set. See
    /// `adapter_substrate::fork::fork_input_bytes`.
    pub input_hash: String,
    /// blake2b-256 over the override set alone. `None` when there are none.
    pub override_hash: Option<String>,
    pub overrides: Vec<ForkOverride>,
    pub call_hash: String,
    pub call_summary: Option<String>,
    pub origin_spec: String,
    pub origin_json: serde_json::Value,
    /// The canonical request bytes, archived before anything is run — so a run
    /// that dies mid-flight still leaves what was asked on record. It carries
    /// the origin AND the call, which is why neither is a second field here:
    /// two copies of the call in one struct is one copy that can go stale, and
    /// the hash was taken over these bytes.
    pub request: Vec<u8>,
    /// scheduled | dry_run_extrinsic. Decided by the ORIGIN: a privileged origin
    /// has to go through the scheduler because no RPC can express one, while a
    /// `signed:` origin is just an extrinsic. The two model different things —
    /// only the second runs transaction extensions — so the coverage list served
    /// with the row is chosen from this.
    pub dispatch_route: String,
    /// Which block-number line the scheduler counts on, and the evidence that
    /// decided it. `None` on the extrinsic route, which involves no agenda.
    pub agenda_anchor: Option<serde_json::Value>,
    /// The account the vehicle extrinsic is signed as. It pays the fee, so it has
    /// to be one that can.
    pub signer: [u8; 32],
}

/// What the fork did.
#[derive(Debug, Clone)]
pub struct ForkOutcome {
    /// executed | dispatch_failed | not_dispatched.
    pub status: String,
    pub dispatch_ok: Option<bool>,
    pub dispatch_error: Option<serde_json::Value>,
    pub events: Vec<SimEvent>,
    /// The whole harness result, schema-on-read — the built block's header, the
    /// extrinsic count, the runtime logs, the raw diff sizes. The part no future
    /// question has to re-run a fork to answer.
    pub effects: serde_json::Value,
    pub storage_diff: Option<serde_json::Value>,
    pub storage_diff_count: Option<u32>,
    pub diff_status: String,
    pub built_block_hash: Option<String>,
    pub harness: serde_json::Value,
    pub note: Option<String>,
}

impl SimRecord {
    /// A fork row. Same table, same key shape, `tier = 'fork'`.
    pub fn from_fork(
        prepared: &PreparedFork,
        outcome: &ForkOutcome,
        sim_version: u32,
        raw_location: String,
    ) -> Result<Self, SimError> {
        // Serialized before it is counted, and loudly, for the reason
        // `SimRecord::new` states: `event_count: 2` beside `emitted_events: []`
        // is a row that contradicts itself.
        let emitted_events = serde_json::to_value(&outcome.events)
            .map_err(|e| SimError::Encode(format!("serializing simulated events: {e}")))?;
        // An empty override set is NOT a counterfactual, and the two must not be
        // recorded alike — the migration's CHECK enforces the pairing, and this
        // is where the pairing is decided.
        let overrides = if prepared.overrides.is_empty() {
            None
        } else {
            Some(serde_json::to_value(&prepared.overrides).map_err(|e| {
                SimError::Encode(format!("serializing storage overrides: {e}"))
            })?)
        };
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
            xcm_version: None,
            status: outcome.status.clone(),
            dispatch_ok: outcome.dispatch_ok,
            dispatch_error: outcome.dispatch_error.clone(),
            emitted_events,
            event_count: outcome.events.len() as u32,
            local_xcm: None,
            forwarded_xcms: None,
            effects: outcome.effects.clone(),
            note: outcome.note.clone(),
            spec_version: prepared.spec_version,
            api_version: None,
            metadata_version: prepared.metadata_version,
            sim_version,
            raw_location,
            baseline_input_hash: None,
            overrides,
            override_hash: prepared.override_hash.clone(),
            storage_diff: outcome.storage_diff.clone(),
            storage_diff_count: outcome.storage_diff_count,
            diff_status: Some(outcome.diff_status.clone()),
            built_block_hash: outcome.built_block_hash.clone(),
            harness: Some(outcome.harness.clone()),
            dispatch_route: Some(prepared.dispatch_route.clone()),
            agenda_anchor: prepared.agenda_anchor.clone(),
        })
    }
}

#[async_trait]
pub trait ForkRunner: Send + Sync {
    /// Pin the state, resolve the overrides against the runtime's metadata, read
    /// what the REAL chain holds at each overridden key, and build the canonical
    /// request. Nothing is forked here.
    async fn prepare_fork(&self, req: &ForkRequest) -> Result<PreparedFork, SimError>;
    /// Start the harness, inject, build a block, read the answer back. Returns
    /// the RAW harness output, archived before it is interpreted.
    async fn dispatch_fork(&self, prepared: &PreparedFork) -> Result<Vec<u8>, SimError>;
    /// Pure: harness output → outcome. Failure is loud and writes no row.
    fn interpret_fork(
        &self,
        prepared: &PreparedFork,
        response: &[u8],
    ) -> Result<ForkOutcome, SimError>;
    fn sim_version(&self) -> u32;
}

/// prepare → cache → archive request → dispatch → archive response → interpret
/// → record.
pub async fn run_fork_simulation(
    runner: &dyn ForkRunner,
    store: &dyn SimStore,
    raw: &dyn raw_store::RawStore,
    receipts: &dyn ingest::ReceiptSink,
    req: &ForkRequest,
) -> Result<SimRun, SimError> {
    let prepared = runner.prepare_fork(req).await?;

    if let Some(hit) = store
        .get(
            &prepared.chain_id,
            &prepared.at_block_hash,
            &prepared.input_hash,
            &prepared.tier,
        )
        .await?
    {
        // The harness version is NOT part of the key (migration 0021 says why),
        // so a cache hit can have been produced by a different chopsticks than
        // the one installed now. The row records which; this line makes it
        // visible without anyone having to go and look.
        tracing::info!(
            chain = %prepared.chain_id, height = prepared.at_height,
            input = %prepared.input_hash, tier = %prepared.tier,
            recorded_harness = %hit.harness.as_ref()
                .and_then(|h| h.get("version"))
                .map(|v| v.to_string())
                .unwrap_or_else(|| "unknown".into()),
            "fork simulation already recorded for this tier, state, input and override set — \
             not re-running"
        );
        return Ok(SimRun {
            record: hit,
            cached: true,
        });
    }

    let artifacts = SimArtifacts::new(
        raw,
        receipts,
        &prepared.chain_id,
        &prepared.at_block_hash,
        &prepared.input_hash,
        &prepared.method,
    );
    artifacts.put_params(&prepared.request).await?;
    let response = runner.dispatch_fork(&prepared).await?;
    let response_key = artifacts.put_response(&response).await?;

    let outcome = runner.interpret_fork(&prepared, &response)?;
    let record = SimRecord::from_fork(&prepared, &outcome, runner.sim_version(), response_key)?;
    store.put(&record).await?;
    Ok(SimRun {
        record,
        cached: false,
    })
}

// ---------------------------------------------------------------- the queue

pub const JOB_QUEUED: &str = "queued";
pub const JOB_RUNNING: &str = "running";
pub const JOB_DONE: &str = "done";
pub const JOB_FAILED: &str = "failed";
pub const JOB_REFUSED: &str = "refused";

/// A request to run a simulation, before anybody has run it.
#[derive(Debug, Clone)]
pub struct NewSimJob {
    pub chain_id: String,
    pub tier: String,
    pub at_height: Option<u64>,
    pub call: Vec<u8>,
    pub call_hash: String,
    pub origin_spec: String,
    pub override_specs: Vec<String>,
    pub requested_by: Option<String>,
    pub note: Option<String>,
    pub max_attempts: u32,
    pub signer: Option<[u8; 32]>,
}

#[derive(Debug, Clone)]
pub struct SimJob {
    pub id: i64,
    pub chain_id: String,
    pub tier: String,
    pub at_height: Option<u64>,
    pub call: Vec<u8>,
    pub call_hash: String,
    pub origin_spec: String,
    pub override_specs: Vec<String>,
    pub requested_by: Option<String>,
    pub note: Option<String>,
    pub status: String,
    pub attempts: u32,
    pub max_attempts: u32,
    pub signer: Option<[u8; 32]>,
    pub error: Option<String>,
    pub result_at_block_hash: Option<String>,
    pub result_input_hash: Option<String>,
}

#[async_trait]
pub trait JobStore: Send + Sync {
    async fn enqueue(&self, job: &NewSimJob) -> Result<i64, SimError>;
    /// Take the oldest queued job, if the concurrency cap allows one more.
    ///
    /// `max_concurrent` is enforced by the STORE and not by the caller, because
    /// the cap has to hold across processes: two workers on one box, or a worker
    /// beside a CLI run, must not both decide there is room. The Pg
    /// implementation serialises claims on an advisory lock for exactly that
    /// reason — `for update skip locked` alone does not make a COUNT and a CLAIM
    /// atomic against a concurrent uncommitted claim.
    /// `only` restricts the claim to ONE job id. Without it the CLI's inline
    /// run would take whatever the oldest queued job happens to be, flip it to
    /// running, spend its single attempt and hold a lease on it — wedging a
    /// bystander job on the way to reporting that it could not run its own.
    async fn claim(
        &self,
        worker: &str,
        lease_secs: u32,
        max_concurrent: u32,
        only: Option<i64>,
    ) -> Result<Option<SimJob>, SimError>;
    async fn complete(
        &self,
        id: i64,
        at_block_hash: &str,
        input_hash: &str,
    ) -> Result<(), SimError>;
    /// `refused` marks a DETERMINISTIC failure — bad call bytes, an origin this
    /// runtime does not have, a chain with no fork endpoint. It is a separate
    /// terminal state from `failed` because retrying it is guaranteed to spend a
    /// Node process on an answer that cannot change.
    async fn fail(&self, id: i64, error: &str, refused: bool) -> Result<(), SimError>;
    async fn get(&self, id: i64) -> Result<Option<SimJob>, SimError>;
}

/// Run ONE job to a terminal state.
///
/// THIS IS THE ONLY PLACE A TIER 2 RUN HAPPENS. `simulate-call --tier fork`
/// creates a job row and calls this; `FORK_JOBS=1` claims a job row and calls
/// this. One implementation, two triggers — the alternative (a synchronous path
/// beside a queued one) is two codepaths that can disagree about what a Tier 2
/// answer is, which is the defect this project has found in four separate
/// slices.
///
/// A [`SimError::Encode`] is REFUSED rather than failed: it means the request
/// itself cannot be built on this runtime, and no number of retries changes that.
///
/// THE ORIGIN ARRIVES ALREADY PARSED, and that is not an inconvenience: turning
/// `signed:13UVJ…` into 32 bytes is protocol knowledge this crate does not have
/// (the same reason `OriginSpec::parse` takes an injected parser), and a worker
/// loop must not be the place that discovers it. The caller parses it, and a job
/// whose origin does not parse is refused before it ever gets here.
pub async fn run_job(
    runner: &dyn ForkRunner,
    store: &dyn SimStore,
    jobs: &dyn JobStore,
    raw: &dyn raw_store::RawStore,
    receipts: &dyn ingest::ReceiptSink,
    job: &SimJob,
    origin: OriginSpec,
) -> Result<SimRun, SimError> {
    if job.tier != TIER_FORK {
        let msg = format!(
            "job {} asks for tier '{}' and this worker runs '{}' — refusing rather than running \
             a different tier than was asked for",
            job.id, job.tier, TIER_FORK
        );
        jobs.fail(job.id, &msg, true).await?;
        return Err(SimError::Encode(msg));
    }
    // THE SIGNER IS RESOLVED HERE, ONCE, and a scheduled run without one is
    // REFUSED rather than defaulted: the vehicle extrinsic needs an account that
    // can pay for it, and an invented one produces a fork that declines to apply
    // anything for a reason that reads like the call being bad.
    let signer = match (&origin, job.signer) {
        (OriginSpec::Signed(who), _) => *who,
        (_, Some(s)) => s,
        (_, None) => {
            let msg = format!(
                "job {} dispatches through the scheduler, which needs a funded account to sign \
                 the no-op extrinsic that makes the block execute — pass --signer <account>. It \
                 is NOT the dispatch origin ({}), it only pays the fee",
                job.id, job.origin_spec
            );
            jobs.fail(job.id, &msg, true).await?;
            return Err(SimError::Encode(msg));
        }
    };
    let req = ForkRequest {
        chain_id: job.chain_id.clone(),
        at_height: job.at_height,
        call: job.call.clone(),
        origin,
        origin_spec: job.origin_spec.clone(),
        override_specs: job.override_specs.clone(),
        signer,
    };
    match run_fork_simulation(runner, store, raw, receipts, &req).await {
        Ok(run) => {
            jobs.complete(
                job.id,
                &run.record.at_block_hash,
                &run.record.input_hash,
            )
            .await?;
            Ok(run)
        }
        Err(e) => {
            // Encode = the request cannot be built on this runtime at all.
            // Everything else may be transient (an endpoint, a busy box, a
            // harness that failed to start), so it stays retryable.
            let refused = matches!(e, SimError::Encode(_) | SimError::Unsupported { .. });
            jobs.fail(job.id, &e.to_string(), refused).await?;
            Err(e)
        }
    }
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

    #[test]
    fn origin_locations_parse_and_keep_the_parent_count() {
        assert_eq!(LocationSpec::parse("parent").unwrap(), LocationSpec::Parent);
        assert_eq!(LocationSpec::parse(" HERE ").unwrap(), LocationSpec::Here);
        assert_eq!(
            LocationSpec::parse("sibling:2034").unwrap(),
            LocationSpec::Sibling(2034)
        );
        assert_eq!(
            LocationSpec::parse("child:1000").unwrap(),
            LocationSpec::Child(1000)
        );

        // THE DISTINCTION THAT MATTERS: a sibling and a child are the same
        // parachain id at different parent counts, and they are different
        // locations. Both render to the same COMPARABLE token, which is the
        // vocabulary an observed counterparty uses.
        assert_eq!(LocationSpec::Sibling(2034).parents(), 1);
        assert_eq!(LocationSpec::Child(2034).parents(), 0);
        assert_eq!(LocationSpec::Sibling(2034).as_token(), "para:2034");
        assert_eq!(LocationSpec::Child(2034).as_token(), "para:2034");
        assert_eq!(LocationSpec::Sibling(2034).as_spec(), "sibling:2034");
        assert_eq!(LocationSpec::Parent.as_token(), "parent");
        assert_eq!(LocationSpec::Here.parents(), 0);
        assert!(LocationSpec::Parent.para_id().is_none());

        // …so the ambiguous spelling is REFUSED rather than defaulted, and the
        // refusal names both ways to say what was meant.
        let err = LocationSpec::parse("para:2034").unwrap_err().to_string();
        assert!(err.contains("sibling:2034") && err.contains("child:2034"), "{err}");
        for bad in ["", "  ", "sibling", "sibling:abc", "cousin:2034", "2034"] {
            assert!(
                LocationSpec::parse(bad).is_err(),
                "'{bad}' must not parse as an origin location"
            );
        }
    }

    // ------------------------------------------------------- orchestration
    /// The baseline no-op this mock builds. Distinct bytes from any subject, so
    /// the two runs get distinct input hashes and the link is real.
    const NOOP: &[u8] = &[0xba];

    struct MockRunner {
        dispatches: AtomicUsize,
        /// Set to make `interpret` fail, to prove archiving happened first.
        interpret_fails: bool,
        /// Whether this runtime can express a no-op at all.
        has_baseline: bool,
        /// Make the baseline resolve to a DIFFERENT block hash at the same
        /// height — two forks, which is the one case where a link must not be
        /// recorded.
        baseline_forks: bool,
    }

    impl MockRunner {
        fn new(has_baseline: bool) -> Self {
            Self {
                dispatches: AtomicUsize::new(0),
                interpret_fails: false,
                has_baseline,
                baseline_forks: false,
            }
        }
    }

    #[async_trait]
    impl DryRunner for MockRunner {
        async fn prepare(&self, req: &SimRequest) -> Result<PreparedRun, SimError> {
            Ok(PreparedRun {
                chain_id: req.chain_id.clone(),
                at_height: req.at_height.unwrap_or(100),
                at_block_hash: if self.baseline_forks && req.call == NOOP {
                    "0xff".into()
                } else {
                    "0xaa".to_string()
                },
                spec_version: 2003002,
                api_version: 2,
                metadata_version: 15,
                tier: TIER_DRY_RUN.into(),
                method: "DryRunApi_dry_run_call".into(),
                params: req.call.clone(),
                // Derived from the call, as the real one is: a baseline and a
                // subject MUST hash differently or the link means nothing.
                input_hash: format!("0x{}", hex::encode(&req.call)),
                call_hash: "0xcc".into(),
                call_summary: Some("system.remark".into()),
                origin_spec: req.origin_spec.clone(),
                origin_json: serde_json::json!({"system": "Root"}),
                xcm_version: req.xcm_version,
            })
        }

        fn baseline_request(&self, prepared: &PreparedRun) -> Result<Option<SimRequest>, SimError> {
            if !self.has_baseline {
                return Ok(None);
            }
            Ok(Some(SimRequest {
                chain_id: prepared.chain_id.clone(),
                at_height: Some(prepared.at_height),
                call: NOOP.to_vec(),
                origin: OriginSpec::Variant {
                    pallet: "system".into(),
                    variant: "Root".into(),
                },
                origin_spec: "root".into(),
                xcm_version: prepared.xcm_version,
            }))
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
        // No baseline on this runtime, so the row is honest about having none.
        let runner = MockRunner::new(false);
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
            first.record.baseline_input_hash, None,
            "a runtime with no expressible no-op has no baseline, and NULL is how that \
             survives into the database — it must never read as 'nothing was queued'"
        );
        assert_eq!(
            first.record.raw_location,
            "raw/polkadot-asset-hub/sim/aa/0001/DryRunApi_dry_run_call.response.scale",
            "the recorded location is the RESPONSE artifact, and it names the method \
             that produced it — the evidence a later sim_version re-derives from"
        );
        // BOTH sides of the exchange are on disk: what we asked and what we got.
        assert_eq!(
            raw.get("raw/polkadot-asset-hub/sim/aa/0001/DryRunApi_dry_run_call.params.scale")
                .unwrap(),
            vec![0x00, 0x01]
        );
        assert_eq!(
            raw.get("raw/polkadot-asset-hub/sim/aa/0001/DryRunApi_dry_run_call.response.scale")
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
                .get("polkadot-asset-hub", "0xaa", "0x0001", "fork")
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
        let mut runner = MockRunner::new(false);
        runner.interpret_fails = true;
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
            raw.get("raw/polkadot-asset-hub/sim/aa/0001/DryRunApi_dry_run_call.response.scale")
                .unwrap(),
            vec![0x11, 0x22],
            "but the response IS archived — it is the evidence of the failure, and \
             the state it came from cannot be re-read once pruned"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn a_baseline_runs_first_and_a_second_simulation_at_that_state_is_free() {
        let (raw, dir) = tmp_raw("baseline");
        let runner = MockRunner::new(true);
        let store = MemStore::default();
        let receipts = ingest::NoopReceiptSink;

        let first = run_simulation(&runner, &store, &raw, &receipts, &request())
            .await
            .expect("first run");
        assert_eq!(
            first.record.baseline_input_hash.as_deref(),
            Some("0xba"),
            "the row points at the no-op run whose forwarded list is the ambient queue"
        );
        assert_eq!(
            runner.dispatches.load(Ordering::SeqCst),
            2,
            "the baseline is a real dispatch — the FIRST one, before the subject"
        );
        assert_eq!(
            store.rows.lock().unwrap().len(),
            2,
            "and it is an ordinary recorded row, not a side value"
        );

        // A SECOND simulation at the same state pays nothing for the baseline:
        // the subject is cached, and the cache is consulted before any baseline
        // work happens.
        let again = run_simulation(&runner, &store, &raw, &receipts, &request())
            .await
            .expect("second run");
        assert!(again.cached);
        assert_eq!(runner.dispatches.load(Ordering::SeqCst), 2);

        // The baseline was dispatched FIRST, not merely dispatched: it is the
        // first row in the store. Asserting only the count would leave the
        // ordering — which is what makes the subject's own archive land after a
        // usable baseline exists — unchecked.
        assert_eq!(
            store.rows.lock().unwrap()[0].input_hash,
            "0xba",
            "the no-op runs before the subject"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn simulating_the_no_op_itself_dispatches_once_and_is_its_own_baseline() {
        // A FRESH store, so the branch is actually reached: run the no-op as the
        // SUBJECT before any baseline exists. With a store that already holds the
        // baseline row the cache answers first and this arm is never executed —
        // which is how it could have been deleted with every test still green.
        let (raw, dir) = tmp_raw("self-baseline");
        let runner = MockRunner::new(true);
        let store = MemStore::default();

        let mut noop = request();
        noop.call = NOOP.to_vec();
        let run = run_simulation(&runner, &store, &raw, &ingest::NoopReceiptSink, &noop)
            .await
            .expect("no-op run");

        assert_eq!(run.record.input_hash, "0xba");
        assert_eq!(
            run.record.baseline_input_hash.as_deref(),
            Some("0xba"),
            "a no-op is its own baseline: differencing a list against itself is empty, \
             which is exactly what a call that queues nothing sent"
        );
        assert_eq!(
            runner.dispatches.load(Ordering::SeqCst),
            1,
            "and it costs ONE dispatch, not two — the subject is not asked twice"
        );
        assert_eq!(store.rows.lock().unwrap().len(), 1);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn a_baseline_at_another_fork_records_no_link_at_all() {
        // Both requests name a HEIGHT, and a height can name two forks. A
        // baseline that landed at a different state is not this row's baseline,
        // and a link a reader would have to distrust is worse than none — the
        // API then says nothing here is attributable.
        let (raw, dir) = tmp_raw("fork");
        let mut runner = MockRunner::new(true);
        runner.baseline_forks = true;
        let store = MemStore::default();

        let run = run_simulation(&runner, &store, &raw, &ingest::NoopReceiptSink, &request())
            .await
            .expect("subject still runs");
        assert_eq!(
            run.record.baseline_input_hash, None,
            "the subject is answered, and it honestly claims no attribution"
        );
        assert_eq!(
            runner.dispatches.load(Ordering::SeqCst),
            1,
            "and the mismatched baseline is never dispatched"
        );
        assert_eq!(store.rows.lock().unwrap().len(), 1);
        let _ = std::fs::remove_dir_all(dir);
    }

    // ------------------------------------------------- the receiving side
    /// The empty program this mock uses as its baseline.
    const EMPTY: &[u8] = &[0x05, 0x00];

    struct MockXcmRunner {
        dispatches: AtomicUsize,
        interpret_fails: bool,
    }

    #[async_trait]
    impl XcmDryRunner for MockXcmRunner {
        async fn prepare_xcm(&self, req: &XcmSimRequest) -> Result<PreparedXcmRun, SimError> {
            Ok(PreparedXcmRun {
                chain_id: req.chain_id.clone(),
                at_height: req.at_height.unwrap_or(200),
                at_block_hash: "0xde".into(),
                spec_version: 435,
                api_version: 2,
                metadata_version: 15,
                tier: TIER_DRY_RUN.into(),
                method: "DryRunApi_dry_run_xcm".into(),
                params: req.program.clone(),
                input_hash: format!("0x{}", hex::encode(&req.program)),
                program_hash: format!("0xprog{}", hex::encode(&req.program)),
                program: serde_json::json!({"V5": [[[]]]}),
                program_summary: Some("(test program)".into()),
                origin: req.origin.clone(),
                origin_ref: req.origin.as_token(),
                origin_location: serde_json::json!({"V5": [{"parents": 1}]}),
                source: req.source.clone(),
            })
        }
        async fn dispatch_xcm(&self, _p: &PreparedXcmRun) -> Result<Vec<u8>, SimError> {
            self.dispatches.fetch_add(1, Ordering::SeqCst);
            Ok(vec![0x33, 0x44])
        }
        fn interpret_xcm(
            &self,
            _p: &PreparedXcmRun,
            _r: &[u8],
        ) -> Result<XcmSimOutcome, SimError> {
            if self.interpret_fails {
                return Err(SimError::Decode("outcome shape not recognised".into()));
            }
            Ok(XcmSimOutcome {
                status: XcmSimStatus::NotStarted,
                weight_used: None,
                xcm_error: Some(serde_json::json!({"Barrier": []})),
                events: vec![],
                forwarded_xcms: vec![],
                effects: serde_json::json!({"Ok": []}),
                note: Some("never started".into()),
            })
        }
        fn sim_version(&self) -> u32 {
            2
        }
        fn baseline_request(
            &self,
            prepared: &PreparedXcmRun,
        ) -> Result<Option<XcmSimRequest>, SimError> {
            Ok(Some(XcmSimRequest {
                chain_id: prepared.chain_id.clone(),
                at_height: Some(prepared.at_height),
                origin: prepared.origin.clone(),
                program: EMPTY.to_vec(),
                source: None,
            }))
        }
    }

    #[derive(Default)]
    struct MemXcmStore {
        rows: Mutex<Vec<XcmSimRecord>>,
    }

    #[async_trait]
    impl XcmSimStore for MemXcmStore {
        async fn get(
            &self,
            chain_id: &str,
            at_block_hash: &str,
            input_hash: &str,
            tier: &str,
        ) -> Result<Option<XcmSimRecord>, SimError> {
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
        async fn put(&self, record: &XcmSimRecord) -> Result<(), SimError> {
            self.rows.lock().unwrap().push(record.clone());
            Ok(())
        }
    }

    fn xcm_request() -> XcmSimRequest {
        XcmSimRequest {
            chain_id: "hydration".into(),
            at_height: Some(13_663_124),
            origin: LocationSpec::Sibling(1000),
            program: vec![0x05, 0x08, 0x0b],
            source: Some(ProgramSource {
                chain_id: "polkadot-asset-hub".into(),
                at_block_hash: "0xcd".into(),
                input_hash: "0x01".into(),
                forwarded_index: 0,
                message_index: 1,
            }),
        }
    }

    #[tokio::test]
    async fn an_arrival_archives_both_artifacts_runs_its_baseline_first_and_keeps_provenance() {
        let (raw, dir) = tmp_raw("xcm");
        let runner = MockXcmRunner {
            dispatches: AtomicUsize::new(0),
            interpret_fails: false,
        };
        let store = MemXcmStore::default();
        let receipts = ingest::NoopReceiptSink;
        let req = xcm_request();

        let first = run_xcm_simulation(&runner, &store, &raw, &receipts, &req)
            .await
            .expect("first run");
        assert!(!first.cached);
        assert_eq!(first.record.status, "not_started");
        assert_eq!(first.record.origin_ref, "para:1000");
        assert_eq!(first.record.sim_version, 2);
        assert_eq!(
            first.record.baseline_input_hash.as_deref(),
            Some("0x0500"),
            "the empty-program run at the same state is this row's baseline"
        );
        assert_eq!(
            runner.dispatches.load(Ordering::SeqCst),
            2,
            "baseline then subject"
        );
        assert_eq!(store.rows.lock().unwrap()[0].input_hash, "0x0500");

        // PROVENANCE: the five source columns are the stitch, and this is the
        // only place `XcmSimRecord::new` fans a ProgramSource out into them —
        // exactly the transposition the migration's own comment warns about.
        assert_eq!(first.record.source_chain_id.as_deref(), Some("polkadot-asset-hub"));
        assert_eq!(first.record.source_at_block_hash.as_deref(), Some("0xcd"));
        assert_eq!(first.record.source_input_hash.as_deref(), Some("0x01"));
        assert_eq!(first.record.source_forwarded_index, Some(0));
        assert_eq!(
            first.record.source_message_index,
            Some(1),
            "the destination index and the message index are DIFFERENT numbers and must \
             not be swapped"
        );
        // …and the baseline came from nowhere: giving it a provenance would put
        // a message in the stitch that nobody sent.
        assert!(store.rows.lock().unwrap()[0].source_input_hash.is_none());

        assert_eq!(
            first.record.raw_location,
            "raw/hydration/sim/de/05080b/DryRunApi_dry_run_xcm.response.scale"
        );
        assert_eq!(
            raw.get("raw/hydration/sim/de/05080b/DryRunApi_dry_run_xcm.params.scale")
                .unwrap(),
            vec![0x05, 0x08, 0x0b],
            "what we asked is on record too, archived before the dispatch"
        );

        let again = run_xcm_simulation(&runner, &store, &raw, &receipts, &req)
            .await
            .expect("second run");
        assert!(again.cached);
        assert_eq!(runner.dispatches.load(Ordering::SeqCst), 2);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn an_unreadable_arrival_records_no_row_but_keeps_the_bytes() {
        let (raw, dir) = tmp_raw("xcm-undecodable");
        let runner = MockXcmRunner {
            dispatches: AtomicUsize::new(0),
            interpret_fails: true,
        };
        let store = MemXcmStore::default();
        let err = run_xcm_simulation(
            &runner,
            &store,
            &raw,
            &ingest::NoopReceiptSink,
            &xcm_request(),
        )
        .await
        .expect_err("interpretation failed, so the run must fail");
        assert!(matches!(err, SimError::Decode(_)), "loud, not swallowed");
        assert!(
            store.rows.lock().unwrap().is_empty(),
            "not even the baseline's row survives a failure to read the baseline itself"
        );
        assert_eq!(
            raw.get("raw/hydration/sim/de/0500/DryRunApi_dry_run_xcm.response.scale")
                .unwrap(),
            vec![0x33, 0x44],
            "the bytes are archived either way — they are the evidence of the failure"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn attribution_subtracts_the_ambient_queue_and_never_over_claims() {
        let msg = |n: u32| serde_json::json!({"V4": [[{"Transact": {"n": n}}]]});
        let dest = |para: u32| serde_json::json!({"V4": {"parents": 0, "parachain": para}});

        // The relay shape, in miniature: the ambient queue already holds one
        // message for 2040 and one for 2000, and the call adds one for 2034 and
        // a second for 2040.
        let baseline = serde_json::json!([
            {"destination": dest(2040), "messages": [msg(1)]},
            {"destination": dest(2000), "messages": [msg(2)]},
        ]);
        let subject = serde_json::json!([
            {"destination": dest(2040), "messages": [msg(1), msg(9)]},
            {"destination": dest(2000), "messages": [msg(2)]},
            {"destination": dest(2034), "messages": [msg(7)]},
        ]);

        let a = attribute_forwarded(&subject, &baseline);
        assert_eq!(a.total_messages, 4);
        assert_eq!(a.ambient_messages, 2);
        assert_eq!(a.attributed_messages, 2, "only the two the call really added");
        assert_eq!(
            a.destinations.len(),
            2,
            "a destination whose every message was already in flight drops out entirely"
        );
        assert_eq!(a.destinations[0].destination_index, 0);
        assert_eq!(a.destinations[0].messages.len(), 1);
        assert_eq!(
            a.destinations[0].messages[0].message_index, 1,
            "the index is the position in the SUBJECT's list — what the adapter needs \
             to hand back the right bytes"
        );
        assert_eq!(a.destinations[0].messages[0].program, msg(9));
        assert_eq!(a.destinations[1].destination_index, 2);
        assert_eq!(a.destinations[1].messages[0].program, msg(7));

        // It is a MULTISET difference: two copies against one leaves one.
        let doubled = serde_json::json!([
            {"destination": dest(2040), "messages": [msg(1), msg(1)]},
        ]);
        let a = attribute_forwarded(&doubled, &baseline);
        assert_eq!(
            a.attributed_messages, 1,
            "one copy is ambient, the other is ours — a set difference would have \
             claimed neither"
        );

        // Asset Hub's case, which is most chains: nothing ambient, so the
        // difference changes nothing and the machinery costs only a cached row.
        let none = serde_json::json!([]);
        let a = attribute_forwarded(&subject, &none);
        assert_eq!(a.attributed_messages, 4);
        assert_eq!(a.ambient_messages, 0);
        assert_eq!(a.destinations.len(), 3);

        // And with no subject messages at all there is nothing to attribute,
        // which must be an empty list rather than the whole ambient queue.
        let a = attribute_forwarded(&none, &baseline);
        assert_eq!(a.attributed_messages, 0);
        assert_eq!(a.total_messages, 0);
        assert!(a.destinations.is_empty());
    }

    #[test]
    fn the_diff_gate_lists_every_present_value_so_a_new_one_cannot_blank_the_column() {
        // THE REGRESSION THIS PINS: slice 8's gate was `== "decoded"`, so adding
        // `extrinsic_only` to the vocabulary would have made every diff on the
        // live route fall through to NULL — a column that silently stops being
        // written, beside a status saying it was read.
        assert!(diff_is_present(DIFF_STATUS_DECODED));
        assert!(diff_is_present(DIFF_STATUS_EXTRINSIC_ONLY));
        assert!(!diff_is_present(DIFF_STATUS_UNDECODABLE));
        assert!(!diff_is_present(DIFF_STATUS_UNAVAILABLE));
        assert!(!diff_is_present(DIFF_STATUS_REFUSED));
        // Every value in the vocabulary is CLASSIFIED. A sixth added without a
        // decision here fails this rather than defaulting to "no diff".
        assert_eq!(
            // `into_iter()` rather than `iter()`: the array yields `&'static str`
            // in one step, where `iter()` would hand the closure a `&&&str` and
            // lean on transitive deref coercion at the call site.
            DIFF_STATUSES.into_iter().filter(|s| diff_is_present(s)).count(),
            2,
            "exactly two of the five statuses carry bytes; update this test AND the gate together"
        );
    }

    #[test]
    fn the_same_bytes_cover_the_call_on_one_route_and_not_on_the_other() {
        // THE MEASURED DEFECT, as a property. On the scheduled route the call is
        // dispatched in `on_initialize`, which is exactly the phase an
        // `extrinsic_only` diff omits — so the diff describes the no-op vehicle.
        assert!(
            !diff_covers_subject(DIFF_STATUS_EXTRINSIC_ONLY, ROUTE_SCHEDULED),
            "a scheduled dispatch runs in on_initialize and is not in an apply_extrinsic diff"
        );
        // On the extrinsic route the subject IS the extrinsic, so the same bytes
        // are complete — and this is not a lucky exception, it is why the status
        // is about SCOPE and the coverage sentence is chosen by ROUTE.
        assert!(diff_covers_subject(DIFF_STATUS_EXTRINSIC_ONLY, ROUTE_DRY_RUN_EXTRINSIC));

        // A whole-block diff covers the call on either route.
        assert!(diff_covers_subject(DIFF_STATUS_DECODED, ROUTE_SCHEDULED));
        assert!(diff_covers_subject(DIFF_STATUS_DECODED, ROUTE_DRY_RUN_EXTRINSIC));

        // No bytes, no coverage — on either route, and never confused with
        // "nothing changed".
        for s in [
            DIFF_STATUS_UNDECODABLE,
            DIFF_STATUS_UNAVAILABLE,
            DIFF_STATUS_REFUSED,
        ] {
            assert!(!diff_covers_subject(s, ROUTE_SCHEDULED));
            assert!(!diff_covers_subject(s, ROUTE_DRY_RUN_EXTRINSIC));
        }
    }
}
