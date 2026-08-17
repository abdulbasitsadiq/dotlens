//! REST API (Axum). Read-only endpoints over a block index plus the registry.
//! The `BlockIndex` contract is async: Postgres-backed in real runs (`pg`
//! feature), in-memory for DB-less runs and tests.

use async_trait::async_trait;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
    routing::get,
    Router,
};
use canonical::{AccountLabel, CanonicalBlock};
use chrono::{DateTime, Utc};
pub mod search;

use registry::Registry;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Debug, thiserror::Error)]
#[error("block index error: {0}")]
pub struct IndexError(pub String);

/// Storage abstraction the API reads blocks from. Inserts are idempotent:
/// re-inserting an already-indexed (chain, height) is a no-op, never an error.
#[async_trait]
pub trait BlockIndex: Send + Sync {
    async fn get(&self, chain_id: &str, height: u64) -> Result<Option<CanonicalBlock>, IndexError>;
    async fn insert(&self, block: CanonicalBlock) -> Result<(), IndexError>;
    async fn count(&self) -> Result<u64, IndexError>;
    /// Which (chain, height) carries this block hash?
    ///
    /// Added for the search resolver, and deliberately NOT chain-scoped: a block
    /// hash is effectively globally unique, so the useful question is "which
    /// chain is this on", which is exactly what a caller who pasted a hash into
    /// a box cannot tell us. Backed by `blocks_hash_idx` (migration 0013 — there
    /// was no index on this column at all before that, which is why the roadmap
    /// made the index audit part of this slice).
    ///
    /// Returns EVERY match rather than the first: two chains sharing a hash
    /// would be extraordinary, but the resolver's contract is to list what it
    /// found and let the caller see the ambiguity.
    async fn blocks_by_hash(&self, hash: &str) -> Result<Vec<(String, u64)>, IndexError>;
    /// Which (chain, height, index) carries this extrinsic hash? Same reasoning.
    async fn extrinsics_by_hash(
        &self,
        hash: &str,
    ) -> Result<Vec<(String, u64, u32)>, IndexError>;
}

#[derive(Default)]
pub struct MemoryBlockIndex {
    inner: RwLock<HashMap<(String, u64), CanonicalBlock>>,
}

impl MemoryBlockIndex {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl BlockIndex for MemoryBlockIndex {
    async fn get(
        &self,
        chain_id: &str,
        height: u64,
    ) -> Result<Option<CanonicalBlock>, IndexError> {
        let map = self.inner.read().map_err(|e| IndexError(e.to_string()))?;
        Ok(map.get(&(chain_id.to_string(), height)).cloned())
    }
    async fn insert(&self, block: CanonicalBlock) -> Result<(), IndexError> {
        let mut map = self.inner.write().map_err(|e| IndexError(e.to_string()))?;
        // THE replacement rule (reorg safety): finalized rows are immutable;
        // unfinalized rows are always replaceable (tip worker fork swaps).
        if let Some(existing) = map.get(&(block.chain_id.clone(), block.height)) {
            if existing.finalized {
                return Ok(());
            }
        }
        map.insert((block.chain_id.clone(), block.height), block);
        Ok(())
    }
    async fn count(&self) -> Result<u64, IndexError> {
        let map = self.inner.read().map_err(|e| IndexError(e.to_string()))?;
        Ok(map.len() as u64)
    }
    async fn blocks_by_hash(&self, hash: &str) -> Result<Vec<(String, u64)>, IndexError> {
        let map = self.inner.read().map_err(|e| IndexError(e.to_string()))?;
        let mut out: Vec<(String, u64)> = map
            .values()
            .filter(|b| b.hash == hash)
            .map(|b| (b.chain_id.clone(), b.height))
            .collect();
        // deterministic, and identical to the Pg backend's ORDER BY
        out.sort();
        Ok(out)
    }
    async fn extrinsics_by_hash(
        &self,
        hash: &str,
    ) -> Result<Vec<(String, u64, u32)>, IndexError> {
        let map = self.inner.read().map_err(|e| IndexError(e.to_string()))?;
        let mut out: Vec<(String, u64, u32)> = map
            .values()
            .flat_map(|b| {
                b.transactions
                    .iter()
                    .filter(|t| t.hash.as_deref() == Some(hash))
                    .map(|t| (b.chain_id.clone(), b.height, t.index))
            })
            .collect();
        out.sort();
        Ok(out)
    }
}

// ------------------------------------------------------------------ labels

/// Read side of `core.account_labels`. `chain_id` scoping: rows scoped to the
/// chain OR to every chain ('*') are both returned.
#[async_trait]
pub trait LabelIndex: Send + Sync {
    async fn labels_for(
        &self,
        chain_id: &str,
        account_id: &[u8],
    ) -> Result<Vec<AccountLabel>, IndexError>;
}

/// Family-encoded address string → raw account bytes. Injected by the node
/// (adapter-owned parsing — the API crate stays family-agnostic, Invariant 4).
pub type AccountParser = Arc<dyn Fn(&str) -> Result<Vec<u8>, String> + Send + Sync>;

#[derive(Default)]
pub struct MemoryLabelIndex {
    /// key: (chain_scope, account_id) — '*' scope applies everywhere.
    inner: RwLock<HashMap<(String, Vec<u8>), Vec<AccountLabel>>>,
}

impl MemoryLabelIndex {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn insert(&self, chain_scope: &str, account_id: &[u8], label: AccountLabel) {
        self.inner
            .write()
            .expect("label lock")
            .entry((chain_scope.to_string(), account_id.to_vec()))
            .or_default()
            .push(label);
    }
}

#[async_trait]
impl LabelIndex for MemoryLabelIndex {
    async fn labels_for(
        &self,
        chain_id: &str,
        account_id: &[u8],
    ) -> Result<Vec<AccountLabel>, IndexError> {
        let map = self.inner.read().map_err(|e| IndexError(e.to_string()))?;
        let mut out = Vec::new();
        for scope in [chain_id, "*"] {
            if let Some(ls) = map.get(&(scope.to_string(), account_id.to_vec())) {
                out.extend(ls.iter().cloned());
            }
        }
        Ok(out)
    }
}

// ------------------------------------------------------------------ balances

/// One balance change, query-shaped (numeric as text — plancks exceed u64/f64).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct BalanceChangeRow {
    pub height: u64,
    pub timestamp: Option<DateTime<Utc>>,
    pub event_index: u32,
    /// Signed decimal string, plancks.
    pub delta: String,
    pub reason: String,
    /// 0x-hex peer account, if any.
    pub counterparty: Option<String>,
}

/// An absolute balance read from state at the END of `height`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct BalanceAnchorRow {
    pub height: u64,
    pub free: String,
    pub reserved: String,
    pub total: String,
    pub spec_version: Option<u64>,
    pub source: String,
    pub note: Option<String>,
    /// liquid | frozen | blocked, for ASSET anchors only. A pallet-assets
    /// account has one balance and a status where a native account has a
    /// free/reserved split; null here means "native", not "unknown".
    pub status: Option<String>,
}

/// Read side of the balances schema, per chain.
#[async_trait]
pub trait BalanceIndex: Send + Sync {
    async fn changes(
        &self,
        chain_id: &str,
        account_id: &[u8],
        asset: &str,
        from: Option<DateTime<Utc>>,
        to: Option<DateTime<Utc>>,
    ) -> Result<Vec<BalanceChangeRow>, IndexError>;
    async fn anchors(
        &self,
        chain_id: &str,
        account_id: &[u8],
        asset: &str,
    ) -> Result<Vec<BalanceAnchorRow>, IndexError>;
    /// Every (account, asset) position on one chain, for a set of accounts.
    ///
    /// Pairs are taken from anchors UNION changes, not from anchors alone: an
    /// account that has been moving an asset we never anchored still has to
    /// appear, with a null amount. Reporting only what we anchored would make
    /// coverage look complete by omitting what is missing.
    async fn holdings(
        &self,
        chain_id: &str,
        accounts: &[Vec<u8>],
        at_height: Option<u64>,
    ) -> Result<Vec<HoldingRow>, IndexError>;
}

#[derive(Default)]
pub struct MemoryBalanceIndex {
    changes: RwLock<HashMap<(String, Vec<u8>, String), Vec<BalanceChangeRow>>>,
    anchors: RwLock<HashMap<(String, Vec<u8>, String), Vec<BalanceAnchorRow>>>,
}

impl MemoryBalanceIndex {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn insert_change(&self, chain: &str, account: &[u8], asset: &str, row: BalanceChangeRow) {
        self.changes
            .write()
            .expect("lock")
            .entry((chain.into(), account.to_vec(), asset.into()))
            .or_default()
            .push(row);
    }
    pub fn insert_anchor(&self, chain: &str, account: &[u8], asset: &str, row: BalanceAnchorRow) {
        self.anchors
            .write()
            .expect("lock")
            .entry((chain.into(), account.to_vec(), asset.into()))
            .or_default()
            .push(row);
    }
}

#[async_trait]
impl BalanceIndex for MemoryBalanceIndex {
    async fn changes(
        &self,
        chain_id: &str,
        account_id: &[u8],
        asset: &str,
        from: Option<DateTime<Utc>>,
        to: Option<DateTime<Utc>>,
    ) -> Result<Vec<BalanceChangeRow>, IndexError> {
        let map = self.changes.read().map_err(|e| IndexError(e.to_string()))?;
        let mut rows: Vec<BalanceChangeRow> = map
            .get(&(chain_id.into(), account_id.to_vec(), asset.into()))
            .map(|v| {
                v.iter()
                    .filter(|r| match r.timestamp {
                        Some(ts) => {
                            from.map(|f| ts >= f).unwrap_or(true)
                                && to.map(|t| ts < t).unwrap_or(true)
                        }
                        None => from.is_none() && to.is_none(),
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        rows.sort_by_key(|r| (r.height, r.event_index));
        Ok(rows)
    }
    async fn anchors(
        &self,
        chain_id: &str,
        account_id: &[u8],
        asset: &str,
    ) -> Result<Vec<BalanceAnchorRow>, IndexError> {
        let map = self.anchors.read().map_err(|e| IndexError(e.to_string()))?;
        let mut rows = map
            .get(&(chain_id.into(), account_id.to_vec(), asset.into()))
            .cloned()
            .unwrap_or_default();
        rows.sort_by_key(|r| r.height);
        Ok(rows)
    }

    async fn holdings(
        &self,
        chain_id: &str,
        accounts: &[Vec<u8>],
        at_height: Option<u64>,
    ) -> Result<Vec<HoldingRow>, IndexError> {
        let anchors = self.anchors.read().map_err(|e| IndexError(e.to_string()))?;
        let changes = self.changes.read().map_err(|e| IndexError(e.to_string()))?;
        let within = |h: u64| at_height.is_none_or(|at| h <= at);

        // pairs from BOTH sides, same rule the SQL uses — INCLUDING the
        // height filter. Filtering only the rows and not the pair would make
        // this backend emit a null-amount row where the SQL emits nothing at
        // all, and the two are required to agree (reviewer catch).
        let mut pairs: std::collections::BTreeSet<(Vec<u8>, String)> =
            std::collections::BTreeSet::new();
        for (chain, account, asset) in anchors.keys().chain(changes.keys()) {
            if chain != chain_id || !accounts.contains(account) {
                continue;
            }
            let key = (chain.clone(), account.clone(), asset.clone());
            let has_row = anchors
                .get(&key)
                .is_some_and(|rows| rows.iter().any(|a| within(a.height)))
                || changes
                    .get(&key)
                    .is_some_and(|rows| rows.iter().any(|c| within(c.height)));
            if has_row {
                pairs.insert((account.clone(), asset.clone()));
            }
        }

        let mut out = Vec::with_capacity(pairs.len());
        for (account, asset) in pairs {
            let key = (chain_id.to_string(), account.clone(), asset.clone());
            let anchor = anchors
                .get(&key)
                .and_then(|rows| {
                    rows.iter()
                        .filter(|a| within(a.height))
                        .max_by_key(|a| a.height)
                })
                .cloned();
            let floor = anchor.as_ref().map(|a| a.height);
            let relevant: Vec<&BalanceChangeRow> = changes
                .get(&key)
                .map(|rows| {
                    rows.iter()
                        .filter(|c| within(c.height) && floor.is_none_or(|f| c.height > f))
                        .collect()
                })
                .unwrap_or_default();
            let delta_sum: i128 = relevant
                .iter()
                .filter_map(|c| c.delta.parse::<i128>().ok())
                .sum();
            out.push(HoldingRow {
                account_id: account,
                asset,
                anchor_total: anchor.as_ref().map(|a| a.total.clone()),
                anchor_height: anchor.as_ref().map(|a| a.height),
                anchor_spec_version: anchor.as_ref().and_then(|a| a.spec_version),
                anchor_source: anchor.as_ref().map(|a| a.source.clone()),
                anchor_note: anchor.as_ref().and_then(|a| a.note.clone()),
                anchor_status: anchor.as_ref().and_then(|a| a.status.clone()),
                delta_sum: delta_sum.to_string(),
                delta_count: relevant.len() as u64,
                last_delta_height: relevant.iter().map(|c| c.height).max(),
            });
        }
        Ok(out)
    }
}

// --------------------------------------------------------------------- assets

/// One asset REPRESENTATION on one chain (`core.assets`). `decimals` is the
/// difference between "20895000000" and "20,895 USDT" — and it is Option
/// because an asset whose metadata was never set genuinely has none, which the
/// API must say rather than default to 0 (a default of 0 would render 20,895
/// USDT as 20,895,000,000).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct AssetRow {
    pub asset_key: String,
    pub representation_kind: String,
    pub symbol: Option<String>,
    pub name: Option<String>,
    pub decimals: Option<u32>,
    pub supply: Option<String>,
    /// Live | Frozen | Destroying. A non-Live asset's running totals cannot be
    /// trusted forward of a destruction, so the holdings endpoint says so.
    pub status: Option<String>,
    /// Canonical (version-stripped) XCM name — the join handle to a treasury
    /// spend's `asset_location`.
    #[serde(skip_serializing)]
    pub location_key: Option<String>,
    pub xcm_location: Option<serde_json::Value>,
}

/// One XCM observation (`xcm.messages`) — one chain's half of one message.
#[derive(Debug, Clone, serde::Serialize)]
pub struct XcmMessageRow {
    pub chain_id: String,
    pub block_height: u64,
    pub event_index: u32,
    /// sent | received | local
    pub side: String,
    /// hrmp | ump | dmp | local | unknown
    pub transport: String,
    pub message_id: Option<String>,
    /// topic | wire_hash | ambiguous | none — see `xcm_not_covered()`.
    pub id_kind: String,
    pub counterparty: Option<String>,
    pub origin_location: Option<serde_json::Value>,
    pub destination: Option<serde_json::Value>,
    pub message: Option<serde_json::Value>,
    pub forwarded: bool,
    pub status: String,
    pub success: Option<bool>,
    pub error: Option<serde_json::Value>,
    pub weight_used: Option<serde_json::Value>,
    pub runtime_version: u64,
    pub mapper_version: u32,
    /// The containing block's timestamp, joined from `core.blocks`.
    ///
    /// It is on the OBSERVATION rather than left to the caller because two
    /// chains' block heights are not comparable and a journey has to be ordered
    /// by something. Nullable, and honestly so: `core.blocks.timestamp` is
    /// nullable, and a step with no time is why the journey's `time_order`
    /// check can come back `unknown` rather than `ok`.
    pub timestamp: Option<DateTime<Utc>>,
}

/// One recorded id alias (`xcm.message_links`) — the correlator's only stored
/// inference.
///
/// Two ids that name ONE message, established inside one block on one chain:
/// the wire hash the router computed and the topic `WithUniqueTopic::deliver`
/// returned after discarding it. No event on any chain states this
/// relationship, which is why `rule`, `confidence` and `evidence` travel with
/// it everywhere it is rendered.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct XcmLinkRow {
    pub chain_id: String,
    pub block_height: u64,
    pub wire_event_index: u32,
    pub topic_event_index: u32,
    pub wire_hash: String,
    pub topic: String,
    /// hrmp | ump — never dmp, which has no sender-side hash event to pair.
    pub transport: String,
    /// unique_in_block | interleaved
    pub rule: String,
    /// high | medium
    pub confidence: String,
    pub evidence: serde_json::Value,
    /// Lineage, both halves of it: which runtime's events the inference was
    /// drawn from, and which version of the rule drew it. `correlator_version`
    /// alone would say how we concluded but not what from.
    pub runtime_version: u64,
    pub correlator_version: u32,
}

/// Read side of `xcm.messages` + `xcm.message_links`.
#[async_trait]
pub trait XcmIndex: Send + Sync {
    /// Recent observations on one chain, newest first.
    async fn messages(&self, chain_id: &str, limit: u32)
        -> Result<Vec<XcmMessageRow>, IndexError>;
    /// Every observation carrying this id, on ANY chain.
    async fn by_message_id(&self, message_id: &str) -> Result<Vec<XcmMessageRow>, IndexError>;
    /// Every observation carrying ANY of these ids — the journey read, once the
    /// alias set is known. One query rather than one per id, because the point
    /// of the endpoint is a bounded fan-out.
    async fn by_message_ids(&self, ids: &[String]) -> Result<Vec<XcmMessageRow>, IndexError>;
    /// Links naming this id on EITHER side. Two indexed probes; the result is
    /// what turns a wire hash into the topic its receiver reported, and vice
    /// versa.
    async fn aliases(&self, message_id: &str) -> Result<Vec<XcmLinkRow>, IndexError>;
}

#[derive(Default)]
pub struct MemoryXcmIndex {
    rows: RwLock<Vec<XcmMessageRow>>,
    links: RwLock<Vec<XcmLinkRow>>,
}

impl MemoryXcmIndex {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn insert(&self, row: XcmMessageRow) {
        self.rows.write().expect("lock").push(row);
    }
    pub fn insert_link(&self, link: XcmLinkRow) {
        self.links.write().expect("lock").push(link);
    }
}

#[async_trait]
impl XcmIndex for MemoryXcmIndex {
    async fn messages(
        &self,
        chain_id: &str,
        limit: u32,
    ) -> Result<Vec<XcmMessageRow>, IndexError> {
        let rows = self.rows.read().map_err(|e| IndexError(e.to_string()))?;
        let mut out: Vec<XcmMessageRow> =
            rows.iter().filter(|r| r.chain_id == chain_id).cloned().collect();
        // identical to the Pg ordering, tie-break included
        out.sort_by(|a, b| {
            b.block_height
                .cmp(&a.block_height)
                .then_with(|| a.event_index.cmp(&b.event_index))
        });
        out.truncate(limit as usize);
        Ok(out)
    }
    async fn by_message_id(&self, message_id: &str) -> Result<Vec<XcmMessageRow>, IndexError> {
        let ids = [message_id.to_string()];
        self.by_message_ids(&ids).await
    }
    async fn by_message_ids(&self, ids: &[String]) -> Result<Vec<XcmMessageRow>, IndexError> {
        let rows = self.rows.read().map_err(|e| IndexError(e.to_string()))?;
        let mut out: Vec<XcmMessageRow> = rows
            .iter()
            .filter(|r| {
                r.message_id
                    .as_ref()
                    .is_some_and(|id| ids.iter().any(|want| want == id))
            })
            .cloned()
            .collect();
        // identical to the Pg ordering, tie-break included — a `limit` that
        // returned different rows from the two backends is slice 1's defect
        out.sort_by(|a, b| {
            a.chain_id
                .cmp(&b.chain_id)
                .then_with(|| a.block_height.cmp(&b.block_height))
                .then_with(|| a.event_index.cmp(&b.event_index))
        });
        Ok(out)
    }
    async fn aliases(&self, message_id: &str) -> Result<Vec<XcmLinkRow>, IndexError> {
        let links = self.links.read().map_err(|e| IndexError(e.to_string()))?;
        let mut out: Vec<XcmLinkRow> = links
            .iter()
            .filter(|l| l.wire_hash == message_id || l.topic == message_id)
            .cloned()
            .collect();
        out.sort_by(|a, b| {
            a.chain_id
                .cmp(&b.chain_id)
                .then_with(|| a.block_height.cmp(&b.block_height))
                .then_with(|| a.wire_event_index.cmp(&b.wire_event_index))
        });
        Ok(out)
    }
}

/// What an XCM row is NOT. Shipped with every XCM response, because the gap
/// between "we recorded both halves" and "this is one journey" is the entire
/// difficulty of this module, and a consumer who assumes the former would build
/// on sand.
pub fn xcm_not_covered() -> Vec<&'static str> {
    vec![
        "these are one-sided OBSERVATIONS, not journeys: a `sent` row and a `received` row \
         carrying the same id are strong evidence of one message, and nothing on THIS \
         endpoint asserts it. /v1/xcm/journeys/{id} is the endpoint that does, and it ships \
         the rule, the evidence and the checks its stitch rests on rather than a verdict",
        "there are TWO ids per message and `id_kind` says which one a row holds: `topic` \
         (pallet_xcm's message_id, derived from frame_system::unique — not a hash of \
         anything and not recomputable), `wire_hash` (blake2_256 of the queued bytes), and \
         `ambiguous` (messageQueue's id, which is the topic if the message carried one and \
         the receiving runtime sets TrailingSetTopicAsId, else the wire hash — the event \
         does not say which)",
        "a chain may emit NO `Sent` for executor-forwarded messages if it does not wire an \
         XcmEventEmitter — Hydration does exactly this, so its outbound traffic appears only \
         as queue-pallet wire hashes and a sender-side view built on `Sent` would show it as \
         nearly silent",
        "topic propagation ACROSS a hop only exists from staging-xcm-executor 20.0.0 (Jul \
         2025), and `Sent` for forwarded legs only from 19.1.0 — the Polkadot relay runtime \
         indexed here at spec 1003004 has neither, so multi-hop journeys from that era have \
         unrelated ids per leg",
        "DMP has no sender-side hash event at all: the relay computes one and discards it, \
         and parachains_dmp emits nothing. Relay→parachain is topic-or-nothing",
        "a `sent` with no `received` is a legitimate outcome, not necessarily a gap in our \
         indexing: weight-starved XCMP enqueueing drops whole batches with no event on \
         either side",
        "`status: processed` with `success: true` means the message queue discarded the \
         message as handled — pallet-message-queue's own doc says it 'solely' means that. It \
         is not a claim that the XCM achieved what it intended",
        "a chain appears here at all only if its registry seed enables the `xcm` module — \
         /v1/chains lists which do. An absent half may therefore be a chain WE do not map \
         rather than a chain nobody indexed, and the two are not the same claim",
        "HRMP channel history is absent on purpose: channels open and close at SESSION \
         boundaries with no event, so an events-only channel table would miss every genesis \
         channel and every offboarding teardown. It needs a storage snapshot, which is its \
         own slice",
    ]
}

/// What a JOURNEY is not. Everything an observation is not (above), plus the
/// five things the stitch itself cannot claim.
///
/// This list is the reason the endpoint exists in this shape: a journey here is
/// two mechanical rules and a set of checks, all of them named in the payload,
/// rather than a confident single answer. A consumer who wants the confident
/// answer can have it — `shape` and `outcome` say it in one line — but they can
/// also see exactly what it rests on.
pub fn xcm_journey_not_covered() -> Vec<&'static str> {
    // SKIPPING THE FIRST LINE IS THE POINT, not an oversight. `xcm_not_covered`
    // opens with "nothing on THIS endpoint asserts it — /v1/xcm/journeys/{id} is
    // the endpoint that does", which is written for the OBSERVATIONS endpoint and
    // is false here twice over: this endpoint does assert the stitch (that is what
    // `shape`, `steps` and `checks` are), and it points the reader at the endpoint
    // they are already reading. The review caught this exact class one endpoint
    // over — a stale line promising the correlation layer as "a later slice" — and
    // it survived here because the two lists share a helper. The honest version of
    // that line for THIS endpoint is the first one added below, which says what
    // the stitch rests on instead of deferring it.
    let mut v: Vec<&'static str> = xcm_not_covered().into_iter().skip(1).collect();
    v.extend([
        "a journey is assembled from exactly two things: id EQUALITY across chains, and the \
         wire_hash<->topic aliases the correlator recorded WITHIN one block. Nothing else is \
         inferred. Two legs whose ids differ and that share no recorded link are two journeys \
         here, and saying so is the point",
        "the wire_hash<->topic link is only recorded where the block makes it unambiguous — one \
         queued send and one `Sent` of that transport, or n of each strictly alternating. A \
         block with 2 queued sends and 1 `Sent` records NO link, so a wire hash from that block \
         reaches only its own half. `aliases` in this response is every link found while \
         expanding, with the rule and evidence each rests on — a superset of those used \
         when `alias_limit_reached` is true",
        "there is NO hop rule (an inbound message linked to a forwarded outbound send in the \
         same block), and its window of applicability may be empty: from staging-xcm-executor \
         20.0.0 the topic PROPAGATES across a hop so equality already stitches it, and below \
         19.1.0 the forwarded leg emits no `Sent` to link to at all. Only [19.1.0, 20.0.0) — \
         and a chain that re-wraps with a NEW topic — would need one",
        "a wire hash is a hash of CONTENT: two byte-identical queued messages have the same \
         wire hash. A unique topic normally makes the bytes differ, but where it does not, one \
         id names two messages — and because the alias set is expanded through that id, their \
         two distinct topics would be pulled into ONE journey. `checks.id_uniqueness` reports \
         that shape (same chain, same side, one id, two coordinates) rather than merging them \
         silently",
        "the observations and the links are written by two workers with two checkpoints and \
         two versions, on purpose — a rule change must be able to re-derive links without \
         touching a single observation row. The cost is that a range correlated but not yet \
         mapped yields an alias whose ends have no steps, which reads the same as a chain we \
         do not index. `xcm-correlate` chases the same decode frontier `xcm-range` does, so \
         in practice they run neck and neck",
        "steps are ordered by BLOCK TIMESTAMP, which is the only ordering two chains share; \
         `core.blocks.timestamp` is nullable, and a step without one falls back to chain and \
         height, which are not comparable across chains. `checks.time_order` says which case \
         this journey is in",
    ]);
    v
}

/// How chain `from` names chain `to` in an XCM counterparty, from REGISTRY DATA
/// alone — para ids and relay membership, no chain ids in code (Invariant 2).
///
/// This is what makes `counterparty_mirror` a check rather than a decoration:
/// the receiving chain says which QUEUE a message came from and the sending
/// chain says where it addressed one, and the two must be mirror images of each
/// other. `None` means the registry cannot say, which is reported as `unknown`
/// and never as a contradiction.
fn xcm_counterparty_name(from: &registry::ChainConfig, to: &registry::ChainConfig)
    -> Option<String> {
    if from.id == to.id {
        return Some("here".into());
    }
    if from.relay.as_deref() == Some(to.id.as_str()) {
        return Some("parent".into());
    }
    let para = to.para_id?;
    // A sibling (same relay) or a child (this chain IS the relay).
    let sibling = to.relay.is_some() && to.relay == from.relay;
    let child = to.relay.as_deref() == Some(from.id.as_str());
    (sibling || child).then(|| format!("para:{para}"))
}

/// One recorded Tier 1 simulation (`sim.simulation_results`).
///
/// An IMMUTABLE OBSERVATION: what this runtime, at this exact state, answered
/// when asked this exact question. Serving it is a read like any other — the API
/// never triggers a dry run itself. That is deliberate, not a missing feature: a
/// public GET must not fan out to an external node (ROADMAP's cost rules), and
/// the [Simulate] button belongs to Tier 2's job queue, which is where a request
/// that costs real work gets to be a POST.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SimulationRow {
    pub chain_id: String,
    pub at_height: u64,
    pub at_block_hash: String,
    pub input_hash: String,
    pub tier: String,
    pub call_hash: String,
    pub call_summary: Option<String>,
    pub origin_spec: String,
    pub origin: serde_json::Value,
    pub xcm_version: u32,
    /// executed | dispatch_failed | api_error — see the migration on why
    /// `dispatch_failed` is a result rather than an error.
    pub status: String,
    pub dispatch_ok: Option<bool>,
    pub dispatch_error: Option<serde_json::Value>,
    pub emitted_events: serde_json::Value,
    pub event_count: u32,
    pub local_xcm: Option<serde_json::Value>,
    pub forwarded_xcms: serde_json::Value,
    pub effects: serde_json::Value,
    pub note: Option<String>,
    pub spec_version: u64,
    pub api_version: u32,
    pub metadata_version: u32,
    pub sim_version: u32,
    pub raw_location: String,
    pub observed_at: Option<DateTime<Utc>>,
}

/// Read side of `sim.simulation_results`.
#[async_trait]
pub trait SimIndex: Send + Sync {
    /// Recorded simulations of one call on one chain, newest state first.
    ///
    /// The ordering is `at_height desc, input_hash, at_block_hash, tier` — the
    /// full primary key after the height — and BOTH backends must produce it
    /// exactly, ties included, or `limit` returns different rows from each. The
    /// tail keys are not padding: two forks at one height share a height AND an
    /// input hash (same call, same origin → same params) and differ only by
    /// block hash, and two TIERS differ in neither.
    async fn simulations(
        &self,
        chain_id: &str,
        call_hash: &str,
        limit: u32,
    ) -> Result<Vec<SimulationRow>, IndexError>;
}

/// The empty backend — what a memory-mode node serves, and what every endpoint
/// sees before a single simulation has been run.
#[derive(Default)]
pub struct MemorySimIndex {
    rows: RwLock<Vec<SimulationRow>>,
}

impl MemorySimIndex {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn insert(&self, row: SimulationRow) {
        self.rows.write().expect("lock").push(row);
    }
}

#[async_trait]
impl SimIndex for MemorySimIndex {
    async fn simulations(
        &self,
        chain_id: &str,
        call_hash: &str,
        limit: u32,
    ) -> Result<Vec<SimulationRow>, IndexError> {
        let rows = self.rows.read().map_err(|e| IndexError(e.to_string()))?;
        let mut out: Vec<SimulationRow> = rows
            .iter()
            .filter(|r| r.chain_id == chain_id && r.call_hash == call_hash)
            .cloned()
            .collect();
        // byte-identical ordering to the Pg backend (Rust's String Ord is
        // byte-wise, which is what `collate "C"` asks Postgres for), including
        // the at_block_hash tie-break — without it two forks at one height are
        // an unstable pair and `limit 1` disagrees between backends
        out.sort_by(|a, b| {
            b.at_height
                .cmp(&a.at_height)
                .then_with(|| a.input_hash.cmp(&b.input_hash))
                .then_with(|| a.at_block_hash.cmp(&b.at_block_hash))
                .then_with(|| a.tier.cmp(&b.tier))
        });
        out.truncate(limit as usize);
        Ok(out)
    }
}

/// What a Tier 1 answer does NOT model. Repeated in every simulation response
/// because a preview that is silent about its limits is worse than none: each
/// line here is a real difference between this answer and what enactment would
/// do, and the first three are why Tier 2 exists at all.
pub fn sim_not_covered() -> Vec<&'static str> {
    vec![
        "the state is the state at the block named here, NOT the state at enactment — a \
         referendum that would pass in a week is previewed against today's balances, \
         today's whitelist entries and today's scheduler agenda",
        "the call is dispatched directly, so no transaction extension runs: no signature \
         check, no nonce, no mortality, no fee withdrawal and no length or weight limit",
        "a whitelisted-call flow needs its authorization to already exist in storage; \
         previewing one before the Fellowship has whitelisted it fails honestly rather \
         than predicting the enacted outcome (Tier 2 sets that state up — Phase 3)",
        "forwarded_xcms is not what the DESTINATION would do — the receiving side is \
         dry_run_xcm on that chain, which lands with the XCM module",
        "forwarded_xcms IS NOT ALWAYS ATTRIBUTABLE TO THE SIMULATED CALL, and on the \
         Polkadot relay it is not attributable at all: measured at relay #24448717 and \
         #24448722, a `system.remark` under Root — which queues nothing — returns 64 \
         destinations carrying 74 messages, including a real 8,935 DOT \
         ReserveAssetDeposited bound for parachain 2040. Two DIFFERENT calls at one state, \
         and one call at two states, all return byte-identical lists, so the content is a \
         property of the STATE (the relay router enumerating every parachain's existing \
         downward queue) and not of the call. Asset Hub does not behave this way: every \
         call simulated there returns an empty list. Until a later slice differences the \
         answer against a no-op run at the same state, treat a non-empty forwarded_xcms as \
         'messages present', never as 'this call would send these'",
        "a status of `executed` is about the OUTER call: utility.batch returns Ok when an \
         inner call fails (emitting BatchInterrupted) and force_batch carries on past one \
         (ItemFailed), so a half-applied batch dispatches successfully. Such a run carries a \
         `note` saying so, but the authority is emitted_events, not the status",
        "no fee estimate: XcmPaymentApi is not called by this tier yet",
        "the dispatch origin is supplied by the caller and is NOT derived from the \
         referendum's track — the track→origin map lives in runtime Rust, not in any \
         artifact we index. The chain does record it once, in the submitting \
         `referenda.submit` call's `proposal_origin` argument; reading that back as a \
         suggested origin is a later slice",
    ]
}

/// Read side of `core.assets`.
#[async_trait]
pub trait AssetIndex: Send + Sync {
    async fn assets(&self, chain_id: &str) -> Result<Vec<AssetRow>, IndexError>;
    /// Every representation carrying this SYMBOL, across every chain.
    ///
    /// The search resolver's reader for `assets_symbol_idx`, which migration
    /// 0010 deliberately deferred with the note "add it WITH its reader"
    /// (0013 creates it). Case-insensitive because people type `usdt` and the
    /// chain stores `USDt`.
    ///
    /// Not chain-scoped, on purpose: "one logical asset, every representation,
    /// every chain" is the question no chain-shaped explorer can ask
    /// (PRODUCT.md gap 6), and scoping it by chain would throw that away.
    async fn assets_by_symbol(
        &self,
        symbol: &str,
    ) -> Result<Vec<(String, AssetRow)>, IndexError>;
}

#[derive(Default)]
pub struct MemoryAssetIndex {
    assets: RwLock<HashMap<String, Vec<AssetRow>>>,
}

impl MemoryAssetIndex {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn insert(&self, chain: &str, row: AssetRow) {
        self.assets
            .write()
            .expect("lock")
            .entry(chain.into())
            .or_default()
            .push(row);
    }
}

#[async_trait]
impl AssetIndex for MemoryAssetIndex {
    async fn assets(&self, chain_id: &str) -> Result<Vec<AssetRow>, IndexError> {
        let map = self.assets.read().map_err(|e| IndexError(e.to_string()))?;
        let mut rows = map.get(chain_id).cloned().unwrap_or_default();
        rows.sort_by(|a, b| a.asset_key.cmp(&b.asset_key));
        Ok(rows)
    }
    async fn assets_by_symbol(
        &self,
        symbol: &str,
    ) -> Result<Vec<(String, AssetRow)>, IndexError> {
        let want = symbol.to_lowercase();
        let map = self.assets.read().map_err(|e| IndexError(e.to_string()))?;
        let mut out: Vec<(String, AssetRow)> = map
            .iter()
            .flat_map(|(chain, rows)| {
                rows.iter()
                    .filter(|r| {
                        r.symbol.as_deref().map(str::to_lowercase).as_deref() == Some(&want)
                    })
                    .map(move |r| (chain.clone(), r.clone()))
            })
            .collect();
        // same ordering as the Pg backend, so both return the same subset
        out.sort_by(|(ac, ar), (bc, br)| (ac, &ar.asset_key).cmp(&(bc, &br.asset_key)));
        Ok(out)
    }
}

/// One (account, asset) position: the latest anchor at or before the query
/// height, plus the deltas recorded after it.
///
/// EVERY FIELD HERE IS PROVENANCE except `amount`. That is deliberate — the
/// Phase 2 exit criterion asks for "an itemized where the funds are WITH
/// per-account provenance", and a number whose origin the reader has to take
/// on trust is exactly what the incumbents already offer.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct HoldingRow {
    pub account_id: Vec<u8>,
    pub asset: String,
    /// The anchored absolute balance, or null when this pair has only ever
    /// been seen MOVING and never read from state — an honest "we know money
    /// went through here and we do not know the balance".
    pub anchor_total: Option<String>,
    pub anchor_height: Option<u64>,
    pub anchor_spec_version: Option<u64>,
    pub anchor_source: Option<String>,
    pub anchor_note: Option<String>,
    pub anchor_status: Option<String>,
    /// Sum of `balance_changes` strictly after the anchor (and within the
    /// query height, if one was given).
    pub delta_sum: String,
    pub delta_count: u64,
    pub last_delta_height: Option<u64>,
}

impl HoldingRow {
    /// anchor + deltas, or None when there is no anchor to add them to.
    /// Returned as a decimal STRING: asset units and plancks both exceed u64.
    pub fn amount(&self) -> Option<String> {
        let anchor: i128 = self.anchor_total.as_ref()?.parse().ok()?;
        let delta: i128 = self.delta_sum.parse().unwrap_or(0);
        Some((anchor + delta).to_string())
    }
    /// "anchor" when the anchor is the whole story, "anchor+deltas" when
    /// events have moved it since, "deltas_only" when there is no anchor.
    pub fn basis(&self) -> &'static str {
        match (self.anchor_total.is_some(), self.delta_count > 0) {
            (true, false) => "anchor",
            (true, true) => "anchor+deltas",
            (false, _) => "deltas_only",
        }
    }
}

/// One treasury account (`treasury.treasury_accounts`) — the WHY behind every
/// holdings row.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct TreasuryAccountRow {
    pub chain_id: String,
    #[serde(skip_serializing)]
    pub account_id: Vec<u8>,
    pub role: String,
    pub instance: Option<String>,
    pub label: String,
    /// 'modl:py/trsry' — how this account was DERIVED, or null when it came
    /// from a reviewed registry seed because it cannot be derived.
    pub derivation: Option<String>,
    pub source: String,
    pub ss58: Option<String>,
}

// ---------------------------------------------------------------------- gov

/// The `gov.referenda` projection, query-shaped.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ReferendumRow {
    pub class: String,
    pub referendum_id: u64,
    pub track_id: Option<u32>,
    /// submitted|deciding|confirming|confirmed|approved|rejected|timed_out|
    /// cancelled|killed|unknown ('unknown' = only info events seen so far).
    pub status: String,
    pub status_height: u64,
    pub proposal: Option<serde_json::Value>,
    pub proposal_hash: Option<String>,
    pub proposal_len: Option<u64>,
    pub submitted_at_height: Option<u64>,
}

/// One referendum timeline entry (a `gov.referendum_events` row).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ReferendumEventRow {
    pub height: u64,
    pub timestamp: Option<DateTime<Utc>>,
    pub event_index: u32,
    pub kind: String,
    pub data: serde_json::Value,
}

/// One track definition (a `gov.tracks` row — decoded from runtime metadata).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct GovTrackRow {
    pub pallet: String,
    pub track_id: u32,
    pub name: String,
    pub params: serde_json::Value,
    pub spec_version: u64,
}

/// One decoded proposal (a `gov.preimages` row) — the call tree a referendum
/// executes, with honest coverage (`decode_status`: decoded|missing|undecodable).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct PreimageRow {
    pub proposal_hash: String,
    pub len: u64,
    pub decode_status: String,
    pub source: String,
    pub call_summary: Option<String>,
    pub decoded_call: Option<serde_json::Value>,
    pub note: Option<String>,
    pub spec_version: Option<u64>,
    pub fetched_at_height: Option<u64>,
}

/// One whitelisted call — a `gov.whitelisted_calls` projection row.
///
/// `status` and `dispatch_ok` answer two DIFFERENT questions and must be read
/// as such: `status = "dispatched"` says the whitelisted call was handed to
/// `dispatch`, its whitelist entry consumed and its preimage unrequested;
/// `dispatch_ok` says whether the call then SUCCEEDED. pallet-whitelist emits
/// the same event either way and swallows the error, so a false here is an
/// enactment that silently did not happen.
///
/// `status = "whitelisted"` with no dispatch is a legitimate TERMINAL state,
/// not a pending one — see migration 0012 on the three ways a dispatch fails
/// with no event at all.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct WhitelistedCallRow {
    pub call_hash: String,
    pub status: String,
    pub dispatch_ok: Option<bool>,
    pub dispatch_error: Option<serde_json::Value>,
    pub dispatch_height: Option<u64>,
    pub first_seen_height: u64,
    pub whitelisted_height: Option<u64>,
    pub status_height: u64,
    pub runtime_version: u64,
    pub mapper_version: u32,
}

/// One `gov.whitelist_events` row — the append-only history of a call hash.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct WhitelistEventRow {
    pub block_height: u64,
    pub event_index: u32,
    pub kind: String,
    pub dispatch_ok: Option<bool>,
    pub dispatch_error: Option<serde_json::Value>,
    pub data: serde_json::Value,
    pub runtime_version: u64,
}

/// One account's current vote on one referendum (a `gov.vote_positions` row).
/// Amounts are decimal strings — plancks exceed u64/f64.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct VoteRow {
    pub class: String,
    pub referendum_id: u64,
    /// 0x-hex account.
    pub voter: String,
    /// false once the vote was removed (history is kept, not deleted).
    pub active: bool,
    /// standard | split | split_abstain | ranked.
    pub vote_type: String,
    pub aye_balance: Option<String>,
    pub nay_balance: Option<String>,
    pub abstain_balance: Option<String>,
    pub conviction: Option<u8>,
    pub conviction_label: Option<String>,
    /// Post-conviction weights, as the pallet tallies them. DIRECT votes only:
    /// delegated power is not in any event — see `voting_anchors`.
    pub aye_votes: String,
    pub nay_votes: String,
    pub support: String,
    /// Height of the event this position came from.
    pub height: u64,
}

/// Total post-conviction weight of a vote, for ordering only (the backends
/// must agree on it, or a `limit` returns different subsets).
fn weight(v: &VoteRow) -> u128 {
    v.aye_votes
        .parse::<u128>()
        .unwrap_or(0)
        .saturating_add(v.nay_votes.parse::<u128>().unwrap_or(0))
}

/// One delegation edge (a `gov.delegations` row).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct DelegationRow {
    pub class: String,
    pub track_id: u32,
    /// 0x-hex accounts.
    pub delegator: String,
    pub target: Option<String>,
    pub active: bool,
    pub height: u64,
}

/// One `ConvictionVoting.VotingFor` state observation (a `gov.voting_anchors`
/// row) — the only honest source for delegation AMOUNTS and for delegated
/// power RECEIVED, neither of which any event carries.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct VotingAnchorRow {
    pub class: String,
    pub track_id: u32,
    pub height: u64,
    /// casting | delegating.
    pub mode: String,
    pub delegating_target: Option<String>,
    pub delegating_balance: Option<String>,
    pub delegating_conviction_label: Option<String>,
    pub delegations_votes: Option<String>,
    pub delegations_capital: Option<String>,
    pub spec_version: Option<u64>,
    pub note: Option<String>,
}

// ----------------------------------------------------------------- treasury

/// One treasury spend (a `treasury.spends` row), query-shaped. Amounts are
/// decimal strings — plancks exceed u64/f64.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SpendRow {
    pub instance: String,
    /// proposal (legacy id space) | asset_spend (modern id space).
    pub spend_kind: String,
    pub spend_id: u64,
    /// proposed|approved|awarded|rejected|paid|processed|payment_failed|voided.
    /// `processed` does NOT assert success — the pallet emits it for expiry too.
    pub status: String,
    pub amount: Option<String>,
    /// The proposer's slashed bond on a rejection. NEVER treasury spending.
    pub slashed: Option<String>,
    /// VersionedLocatableAsset; null = the native token (legacy flow).
    pub asset_kind: Option<serde_json::Value>,
    /// 0x-hex, when the beneficiary is (or names) a plain account.
    pub beneficiary: Option<String>,
    pub beneficiary_location: Option<serde_json::Value>,
    pub payment_id: Option<String>,
    pub valid_from: Option<u64>,
    pub expire_at: Option<u64>,
    pub first_seen_height: u64,
    pub status_height: u64,
    /// `{"location": {"chain": …, "asset": …}, "key": …}` — the normalized,
    /// version-stripped name of what this spend is DENOMINATED in, and the
    /// handle that resolves it to a symbol and decimals in `core.assets`.
    /// Null on the legacy proposal flow (always native) and on rows written
    /// before TREASURY_MAPPER_VERSION 2.
    pub asset_ref: Option<serde_json::Value>,
}

/// One treasury event (a `treasury.spend_events` row) — a step in a spend's
/// life, or a pot flow that names no spend.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SpendEventRow {
    pub height: u64,
    pub timestamp: Option<DateTime<Utc>>,
    pub event_index: u32,
    pub kind: String,
    pub amount: Option<String>,
    pub data: serde_json::Value,
}

/// Read side of the treasury schema, per chain. Instances are stitched across
/// chains by their own residency domain, exactly like referenda classes.
#[async_trait]
pub trait TreasuryIndex: Send + Sync {
    /// Latest spends for an instance, newest first. `status` and `spend_kind`
    /// filter; `spend_kind` matters because the legacy and modern id spaces
    /// are different number lines that both start at 0.
    async fn spends(
        &self,
        chain_id: &str,
        instance: &str,
        status: Option<&str>,
        spend_kind: Option<&str>,
        limit: u64,
    ) -> Result<Vec<SpendRow>, IndexError>;
    async fn spend(
        &self,
        chain_id: &str,
        instance: &str,
        spend_kind: &str,
        spend_id: u64,
    ) -> Result<Option<SpendRow>, IndexError>;
    async fn spend_events(
        &self,
        chain_id: &str,
        instance: &str,
        spend_kind: &str,
        spend_id: u64,
    ) -> Result<Vec<SpendEventRow>, IndexError>;
    /// Pot flows (deposits, burns, rollovers) — money that names no spend.
    async fn pot_events(
        &self,
        chain_id: &str,
        instance: &str,
        limit: u64,
    ) -> Result<Vec<SpendEventRow>, IndexError>;
    /// The treasury account list for a NETWORK, across every chain that holds
    /// treasury money. Keyed by network rather than chain because "where is
    /// the treasury" is a question about Polkadot, not about Asset Hub — the
    /// rows say which chains the answer came from.
    async fn accounts(&self, network: &str) -> Result<Vec<TreasuryAccountRow>, IndexError>;
}

#[derive(Default)]
pub struct MemoryTreasuryIndex {
    spends: RwLock<HashMap<(String, String, String, u64), SpendRow>>,
    events: RwLock<HashMap<(String, String), Vec<(Option<(String, u64)>, SpendEventRow)>>>,
    accounts: RwLock<HashMap<String, Vec<TreasuryAccountRow>>>,
}

impl MemoryTreasuryIndex {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn insert_account(&self, network: &str, row: TreasuryAccountRow) {
        self.accounts
            .write()
            .expect("lock")
            .entry(network.into())
            .or_default()
            .push(row);
    }
    pub fn insert_spend(&self, chain: &str, row: SpendRow) {
        self.spends.write().expect("lock").insert(
            (
                chain.into(),
                row.instance.clone(),
                row.spend_kind.clone(),
                row.spend_id,
            ),
            row,
        );
    }
    /// `subject` = (spend_kind, spend_id); None for a pot flow.
    pub fn insert_event(
        &self,
        chain: &str,
        instance: &str,
        subject: Option<(String, u64)>,
        row: SpendEventRow,
    ) {
        self.events
            .write()
            .expect("lock")
            .entry((chain.into(), instance.into()))
            .or_default()
            .push((subject, row));
    }
}

#[async_trait]
impl TreasuryIndex for MemoryTreasuryIndex {
    async fn spends(
        &self,
        chain_id: &str,
        instance: &str,
        status: Option<&str>,
        spend_kind: Option<&str>,
        limit: u64,
    ) -> Result<Vec<SpendRow>, IndexError> {
        let map = self.spends.read().map_err(|e| IndexError(e.to_string()))?;
        let mut rows: Vec<SpendRow> = map
            .iter()
            .filter(|((c, i, k, _), r)| {
                c == chain_id
                    && i == instance
                    && status.is_none_or(|s| r.status == s)
                    && spend_kind.is_none_or(|want| k == want)
            })
            .map(|(_, r)| r.clone())
            .collect();
        rows.sort_by(|a, b| {
            b.first_seen_height
                .cmp(&a.first_seen_height)
                .then_with(|| b.spend_id.cmp(&a.spend_id))
        });
        rows.truncate(limit as usize);
        Ok(rows)
    }
    async fn spend(
        &self,
        chain_id: &str,
        instance: &str,
        spend_kind: &str,
        spend_id: u64,
    ) -> Result<Option<SpendRow>, IndexError> {
        let map = self.spends.read().map_err(|e| IndexError(e.to_string()))?;
        Ok(map
            .get(&(chain_id.into(), instance.into(), spend_kind.into(), spend_id))
            .cloned())
    }
    async fn spend_events(
        &self,
        chain_id: &str,
        instance: &str,
        spend_kind: &str,
        spend_id: u64,
    ) -> Result<Vec<SpendEventRow>, IndexError> {
        let map = self.events.read().map_err(|e| IndexError(e.to_string()))?;
        let mut rows: Vec<SpendEventRow> = map
            .get(&(chain_id.into(), instance.into()))
            .map(|rows| {
                rows.iter()
                    .filter(|(subject, _)| {
                        subject.as_ref().is_some_and(|(k, i)| k == spend_kind && *i == spend_id)
                    })
                    .map(|(_, r)| r.clone())
                    .collect()
            })
            .unwrap_or_default();
        rows.sort_by_key(|r| (r.height, r.event_index));
        Ok(rows)
    }
    async fn pot_events(
        &self,
        chain_id: &str,
        instance: &str,
        limit: u64,
    ) -> Result<Vec<SpendEventRow>, IndexError> {
        let map = self.events.read().map_err(|e| IndexError(e.to_string()))?;
        let mut rows: Vec<SpendEventRow> = map
            .get(&(chain_id.into(), instance.into()))
            .map(|rows| {
                rows.iter()
                    .filter(|(subject, _)| subject.is_none())
                    .map(|(_, r)| r.clone())
                    .collect()
            })
            .unwrap_or_default();
        rows.sort_by(|a, b| (b.height, b.event_index).cmp(&(a.height, a.event_index)));
        rows.truncate(limit as usize);
        Ok(rows)
    }
    async fn accounts(&self, network: &str) -> Result<Vec<TreasuryAccountRow>, IndexError> {
        let map = self.accounts.read().map_err(|e| IndexError(e.to_string()))?;
        let mut rows = map.get(network).cloned().unwrap_or_default();
        rows.sort_by(|a, b| (&a.chain_id, &a.role, &a.label).cmp(&(&b.chain_id, &b.role, &b.label)));
        Ok(rows)
    }
}

// ----------------------------------------------------------------- bounties

/// The instance a bounty request means when it does not say. The legacy pallet
/// holds nearly every bounty Polkadot has ever funded; the modern one is where
/// new ones land. Echoed in every response, so the default is stated rather
/// than assumed.
pub const DEFAULT_BOUNTY_INSTANCE: &str = "bounties";

/// The `child_id` migration 0011 stores for "the parent bounty itself". Stated
/// here because this crate depends on no adapter (Invariant 4); a test pins it
/// against `adapter_substrate::bounties::PARENT_SENTINEL`.
pub const PARENT_SENTINEL: i64 = -1;

/// One bounty or child bounty (a `treasury.bounties` row), query-shaped.
///
/// `child_id` is `Option<u64>` and NEVER the -1 the table stores: the sentinel
/// exists so the projection can have a primary key, and migration 0011 makes
/// rendering it back to null a contract. `None` means "the parent bounty".
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct BountyRow {
    /// bounties | child_bounties | multi_asset_bounties.
    pub instance: String,
    pub bounty_id: u64,
    pub child_id: Option<u64>,
    /// proposed|approved|funded|curator_proposed|active|awarded|claimed|
    /// canceled|rejected|payment_failed|unknown ('unknown' = only information
    /// events seen so far, e.g. a value raise before the creation block was
    /// indexed).
    pub status: String,
    /// What the bounty is WORTH. NOT what it has left — a bounty's remaining
    /// balance is its ACCOUNT balance, which is why `account_id` is here.
    pub value: Option<String>,
    /// Sum of concluded payouts, as the pallets announce them: the
    /// BENEFICIARY'S SHARE, NET OF THE CURATOR FEE, which no event carries. A
    /// bounty spent more than this. Null means no payout has been seen, which
    /// is not the same as zero.
    pub paid_out: Option<String>,
    /// The PROPOSER'S slashed bond on a rejection. Never bounty spending.
    pub bond: Option<String>,
    pub curator: Option<String>,
    pub beneficiary: Option<String>,
    pub beneficiary_location: Option<serde_json::Value>,
    pub payment_id: Option<String>,
    /// The bounty's own derived account, 0x-hex — the join to holdings, and
    /// the only place the answer to "how much is left" can come from. Null
    /// until `sync-bounty-accounts` has run against archived metadata.
    pub account_id: Option<String>,
    pub first_seen_height: u64,
    pub status_height: u64,
    /// `{"location": {"chain": …, "asset": …}, "key": …, "kind": …}` — what a
    /// multi-asset payout was DENOMINATED in, in the same shape a treasury
    /// spend carries, resolving through the same `core.assets` join. Null on
    /// the two legacy pallets, which are native-token-only by construction.
    pub asset_ref: Option<serde_json::Value>,
}

/// One bounty event (a `treasury.bounty_events` row) — a step in a bounty's
/// life. Shaped like a spend event and deliberately not the same type: these
/// are different tables with different vocabularies, and one struct serving
/// both would make the next column added to either a lie about the other.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct BountyEventRow {
    pub height: u64,
    pub timestamp: Option<DateTime<Utc>>,
    pub event_index: u32,
    pub kind: String,
    pub amount: Option<String>,
    pub data: serde_json::Value,
}

/// Read side of the bounty schema, per chain. Stitched across chains through
/// the TREASURY residency domain, because bounty funds are sub-accounts of the
/// treasury's pallet id and move where the treasury moves.
#[async_trait]
pub trait BountyIndex: Send + Sync {
    /// Bounties of one instance, newest id first. `child_id` is left as stored
    /// per row, so a listing shows parents and children together — which is
    /// how the money is actually held.
    async fn bounties(
        &self,
        chain_id: &str,
        instance: &str,
        status: Option<&str>,
        limit: u64,
    ) -> Result<Vec<BountyRow>, IndexError>;
    /// `child_id = None` addresses the parent bounty itself.
    async fn bounty(
        &self,
        chain_id: &str,
        instance: &str,
        bounty_id: u64,
        child_id: Option<u64>,
    ) -> Result<Option<BountyRow>, IndexError>;
    async fn bounty_events(
        &self,
        chain_id: &str,
        instance: &str,
        bounty_id: u64,
        child_id: Option<u64>,
    ) -> Result<Vec<BountyEventRow>, IndexError>;
}

#[derive(Default)]
pub struct MemoryBountyIndex {
    bounties: RwLock<HashMap<(String, String, u64, Option<u64>), BountyRow>>,
    events: RwLock<HashMap<(String, String), Vec<((u64, Option<u64>), BountyEventRow)>>>,
}

impl MemoryBountyIndex {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn insert_bounty(&self, chain: &str, row: BountyRow) {
        self.bounties.write().expect("lock").insert(
            (
                chain.into(),
                row.instance.clone(),
                row.bounty_id,
                row.child_id,
            ),
            row,
        );
    }
    pub fn insert_event(
        &self,
        chain: &str,
        instance: &str,
        subject: (u64, Option<u64>),
        row: BountyEventRow,
    ) {
        self.events
            .write()
            .expect("lock")
            .entry((chain.into(), instance.into()))
            .or_default()
            .push((subject, row));
    }
}

#[async_trait]
impl BountyIndex for MemoryBountyIndex {
    async fn bounties(
        &self,
        chain_id: &str,
        instance: &str,
        status: Option<&str>,
        limit: u64,
    ) -> Result<Vec<BountyRow>, IndexError> {
        let map = self.bounties.read().map_err(|e| IndexError(e.to_string()))?;
        let mut rows: Vec<BountyRow> = map
            .iter()
            .filter(|((c, i, _, _), r)| {
                c == chain_id && i == instance && status.is_none_or(|s| r.status == s)
            })
            .map(|(_, r)| r.clone())
            .collect();
        // by ID, never by height: two residency windows are two block-number
        // lines, and sorting merged rows by height floats every relay-era
        // bounty above every recent one (the rule the spend list learned)
        rows.sort_by(|a, b| {
            b.bounty_id
                .cmp(&a.bounty_id)
                .then_with(|| b.child_id.cmp(&a.child_id))
        });
        rows.truncate(limit as usize);
        Ok(rows)
    }
    async fn bounty(
        &self,
        chain_id: &str,
        instance: &str,
        bounty_id: u64,
        child_id: Option<u64>,
    ) -> Result<Option<BountyRow>, IndexError> {
        let map = self.bounties.read().map_err(|e| IndexError(e.to_string()))?;
        Ok(map
            .get(&(chain_id.into(), instance.into(), bounty_id, child_id))
            .cloned())
    }
    async fn bounty_events(
        &self,
        chain_id: &str,
        instance: &str,
        bounty_id: u64,
        child_id: Option<u64>,
    ) -> Result<Vec<BountyEventRow>, IndexError> {
        let map = self.events.read().map_err(|e| IndexError(e.to_string()))?;
        let mut rows: Vec<BountyEventRow> = map
            .get(&(chain_id.into(), instance.into()))
            .map(|rows| {
                rows.iter()
                    .filter(|(subject, _)| *subject == (bounty_id, child_id))
                    .map(|(_, r)| r.clone())
                    .collect()
            })
            .unwrap_or_default();
        rows.sort_by_key(|r| (r.height, r.event_index));
        Ok(rows)
    }
}

/// Read side of the gov schema, per chain. The API stitches chains together
/// via governance domain residency (referendum numbering is continuous across
/// the Nov 2025 migration; one referendum may have rows on both chains).
#[async_trait]
pub trait GovIndex: Send + Sync {
    async fn referendum(
        &self,
        chain_id: &str,
        class: &str,
        id: u64,
    ) -> Result<Option<ReferendumRow>, IndexError>;
    async fn referendum_events(
        &self,
        chain_id: &str,
        class: &str,
        id: u64,
    ) -> Result<Vec<ReferendumEventRow>, IndexError>;
    /// Latest referenda by id, descending.
    async fn list_referenda(
        &self,
        chain_id: &str,
        class: &str,
        limit: u64,
    ) -> Result<Vec<ReferendumRow>, IndexError>;
    async fn tracks(&self, chain_id: &str) -> Result<Vec<GovTrackRow>, IndexError>;
    /// One whitelisted call by its hash.
    async fn whitelisted_call(
        &self,
        chain_id: &str,
        call_hash: &str,
    ) -> Result<Option<WhitelistedCallRow>, IndexError>;
    /// Everything that ever happened to one call hash, oldest first.
    async fn whitelist_events(
        &self,
        chain_id: &str,
        call_hash: &str,
    ) -> Result<Vec<WhitelistEventRow>, IndexError>;
    /// Latest whitelisted calls, most recently moved first.
    async fn list_whitelisted_calls(
        &self,
        chain_id: &str,
        limit: u64,
    ) -> Result<Vec<WhitelistedCallRow>, IndexError>;
    /// Best preimage row for a proposal hash (a decoded row wins over
    /// missing/undecodable attempts).
    async fn preimage(
        &self,
        chain_id: &str,
        proposal_hash: &str,
    ) -> Result<Option<PreimageRow>, IndexError>;
    /// Current votes on one referendum, heaviest first.
    async fn referendum_votes(
        &self,
        chain_id: &str,
        class: &str,
        id: u64,
        limit: u64,
    ) -> Result<Vec<VoteRow>, IndexError>;
    /// One account's current vote positions, most recent first.
    async fn account_votes(
        &self,
        chain_id: &str,
        voter: &[u8],
        limit: u64,
    ) -> Result<Vec<VoteRow>, IndexError>;
    /// One account's delegation edges (as delegator).
    async fn account_delegations(
        &self,
        chain_id: &str,
        delegator: &[u8],
    ) -> Result<Vec<DelegationRow>, IndexError>;
    /// One account's voting-state anchors, oldest first.
    async fn voting_anchors(
        &self,
        chain_id: &str,
        account: &[u8],
    ) -> Result<Vec<VotingAnchorRow>, IndexError>;
}

#[derive(Default)]
pub struct MemoryGovIndex {
    referenda: RwLock<HashMap<(String, String, u64), ReferendumRow>>,
    events: RwLock<HashMap<(String, String, u64), Vec<ReferendumEventRow>>>,
    tracks: RwLock<HashMap<String, Vec<GovTrackRow>>>,
    preimages: RwLock<HashMap<(String, String), Vec<PreimageRow>>>,
    whitelisted: RwLock<HashMap<(String, String), WhitelistedCallRow>>,
    whitelist_events: RwLock<HashMap<(String, String), Vec<WhitelistEventRow>>>,
    votes: RwLock<HashMap<String, Vec<VoteRow>>>,
    delegations: RwLock<HashMap<String, Vec<DelegationRow>>>,
    anchors: RwLock<HashMap<(String, String), Vec<VotingAnchorRow>>>,
}

impl MemoryGovIndex {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn insert_whitelisted_call(&self, chain: &str, row: WhitelistedCallRow) {
        self.whitelisted
            .write()
            .unwrap()
            .insert((chain.to_string(), row.call_hash.clone()), row);
    }
    pub fn insert_whitelist_event(&self, chain: &str, call_hash: &str, row: WhitelistEventRow) {
        self.whitelist_events
            .write()
            .unwrap()
            .entry((chain.to_string(), call_hash.to_string()))
            .or_default()
            .push(row);
    }
    pub fn insert_referendum(&self, chain: &str, row: ReferendumRow) {
        self.referenda
            .write()
            .expect("lock")
            .insert((chain.into(), row.class.clone(), row.referendum_id), row);
    }
    pub fn insert_event(&self, chain: &str, class: &str, id: u64, row: ReferendumEventRow) {
        self.events
            .write()
            .expect("lock")
            .entry((chain.into(), class.into(), id))
            .or_default()
            .push(row);
    }
    pub fn insert_track(&self, chain: &str, row: GovTrackRow) {
        self.tracks.write().expect("lock").entry(chain.into()).or_default().push(row);
    }
    pub fn insert_preimage(&self, chain: &str, row: PreimageRow) {
        self.preimages
            .write()
            .expect("lock")
            .entry((chain.into(), row.proposal_hash.clone()))
            .or_default()
            .push(row);
    }
    pub fn insert_vote(&self, chain: &str, row: VoteRow) {
        self.votes.write().expect("lock").entry(chain.into()).or_default().push(row);
    }
    pub fn insert_delegation(&self, chain: &str, row: DelegationRow) {
        self.delegations
            .write()
            .expect("lock")
            .entry(chain.into())
            .or_default()
            .push(row);
    }
    pub fn insert_voting_anchor(&self, chain: &str, account: &[u8], row: VotingAnchorRow) {
        self.anchors
            .write()
            .expect("lock")
            .entry((chain.into(), format!("0x{}", hex_lower(account))))
            .or_default()
            .push(row);
    }
}

#[async_trait]
impl GovIndex for MemoryGovIndex {
    async fn referendum(
        &self,
        chain_id: &str,
        class: &str,
        id: u64,
    ) -> Result<Option<ReferendumRow>, IndexError> {
        let map = self.referenda.read().map_err(|e| IndexError(e.to_string()))?;
        Ok(map.get(&(chain_id.into(), class.into(), id)).cloned())
    }
    async fn referendum_events(
        &self,
        chain_id: &str,
        class: &str,
        id: u64,
    ) -> Result<Vec<ReferendumEventRow>, IndexError> {
        let map = self.events.read().map_err(|e| IndexError(e.to_string()))?;
        let mut rows = map
            .get(&(chain_id.into(), class.into(), id))
            .cloned()
            .unwrap_or_default();
        rows.sort_by_key(|r| (r.height, r.event_index));
        Ok(rows)
    }
    async fn list_referenda(
        &self,
        chain_id: &str,
        class: &str,
        limit: u64,
    ) -> Result<Vec<ReferendumRow>, IndexError> {
        let map = self.referenda.read().map_err(|e| IndexError(e.to_string()))?;
        let mut rows: Vec<ReferendumRow> = map
            .iter()
            .filter(|((c, cl, _), _)| c == chain_id && cl == class)
            .map(|(_, r)| r.clone())
            .collect();
        rows.sort_by(|a, b| b.referendum_id.cmp(&a.referendum_id));
        rows.truncate(limit as usize);
        Ok(rows)
    }
    async fn tracks(&self, chain_id: &str) -> Result<Vec<GovTrackRow>, IndexError> {
        let map = self.tracks.read().map_err(|e| IndexError(e.to_string()))?;
        let mut rows = map.get(chain_id).cloned().unwrap_or_default();
        rows.sort_by(|a, b| (&a.pallet, a.track_id).cmp(&(&b.pallet, b.track_id)));
        Ok(rows)
    }
    async fn preimage(
        &self,
        chain_id: &str,
        proposal_hash: &str,
    ) -> Result<Option<PreimageRow>, IndexError> {
        let map = self.preimages.read().map_err(|e| IndexError(e.to_string()))?;
        let rows = map.get(&(chain_id.into(), proposal_hash.into()));
        Ok(rows.and_then(|rows| {
            rows.iter()
                .find(|r| r.decode_status == "decoded")
                .or_else(|| rows.first())
                .cloned()
        }))
    }
    async fn whitelisted_call(
        &self,
        chain_id: &str,
        call_hash: &str,
    ) -> Result<Option<WhitelistedCallRow>, IndexError> {
        let map = self.whitelisted.read().map_err(|e| IndexError(e.to_string()))?;
        Ok(map.get(&(chain_id.into(), call_hash.into())).cloned())
    }
    async fn whitelist_events(
        &self,
        chain_id: &str,
        call_hash: &str,
    ) -> Result<Vec<WhitelistEventRow>, IndexError> {
        let map = self
            .whitelist_events
            .read()
            .map_err(|e| IndexError(e.to_string()))?;
        let mut rows = map
            .get(&(chain_id.into(), call_hash.into()))
            .cloned()
            .unwrap_or_default();
        rows.sort_by_key(|r| (r.block_height, r.event_index));
        Ok(rows)
    }
    async fn list_whitelisted_calls(
        &self,
        chain_id: &str,
        limit: u64,
    ) -> Result<Vec<WhitelistedCallRow>, IndexError> {
        let map = self.whitelisted.read().map_err(|e| IndexError(e.to_string()))?;
        let mut rows: Vec<WhitelistedCallRow> = map
            .iter()
            .filter(|((c, _), _)| c == chain_id)
            .map(|(_, r)| r.clone())
            .collect();
        // identical ordering to the Pg backend, so `limit` returns the same
        // subset from either — the rule slice 3 set for paired backends. The
        // hash is the tiebreak because two rows can share a status coordinate
        // only across different hashes.
        rows.sort_by(|a, b| {
            (b.status_height, &b.call_hash).cmp(&(a.status_height, &a.call_hash))
        });
        rows.truncate(limit as usize);
        Ok(rows)
    }
    async fn referendum_votes(
        &self,
        chain_id: &str,
        class: &str,
        id: u64,
        limit: u64,
    ) -> Result<Vec<VoteRow>, IndexError> {
        let map = self.votes.read().map_err(|e| IndexError(e.to_string()))?;
        let mut rows: Vec<VoteRow> = map
            .get(chain_id)
            .map(|rows| {
                rows.iter()
                    .filter(|r| r.class == class && r.referendum_id == id)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        // same order as the Pg backend: heaviest first, then voter — otherwise
        // a `limit` returns different subsets from the two backends
        rows.sort_by(|a, b| {
            weight(b).cmp(&weight(a)).then_with(|| a.voter.cmp(&b.voter))
        });
        rows.truncate(limit as usize);
        Ok(rows)
    }
    async fn account_votes(
        &self,
        chain_id: &str,
        voter: &[u8],
        limit: u64,
    ) -> Result<Vec<VoteRow>, IndexError> {
        let hex = format!("0x{}", hex_lower(voter));
        let map = self.votes.read().map_err(|e| IndexError(e.to_string()))?;
        let mut rows: Vec<VoteRow> = map
            .get(chain_id)
            .map(|rows| rows.iter().filter(|r| r.voter == hex).cloned().collect())
            .unwrap_or_default();
        rows.sort_by(|a, b| {
            b.height
                .cmp(&a.height)
                .then_with(|| b.referendum_id.cmp(&a.referendum_id))
        });
        rows.truncate(limit as usize);
        Ok(rows)
    }
    async fn account_delegations(
        &self,
        chain_id: &str,
        delegator: &[u8],
    ) -> Result<Vec<DelegationRow>, IndexError> {
        let hex = format!("0x{}", hex_lower(delegator));
        let map = self.delegations.read().map_err(|e| IndexError(e.to_string()))?;
        let mut rows: Vec<DelegationRow> = map
            .get(chain_id)
            .map(|rows| rows.iter().filter(|r| r.delegator == hex).cloned().collect())
            .unwrap_or_default();
        rows.sort_by_key(|r| (r.class.clone(), r.track_id));
        Ok(rows)
    }
    async fn voting_anchors(
        &self,
        chain_id: &str,
        account: &[u8],
    ) -> Result<Vec<VotingAnchorRow>, IndexError> {
        let hex = format!("0x{}", hex_lower(account));
        let map = self.anchors.read().map_err(|e| IndexError(e.to_string()))?;
        let mut rows = map.get(&(chain_id.into(), hex)).cloned().unwrap_or_default();
        rows.sort_by_key(|r| (r.track_id, r.height));
        Ok(rows)
    }
}

// -------------------------------------------------------------------- pg impl

#[cfg(feature = "pg")]
pub mod pg {
    use super::{BlockIndex, IndexError};
    use async_trait::async_trait;
    use canonical::{CanonicalBlock, CanonicalEvent, CanonicalTransaction, Lineage};
    use chrono::{DateTime, Utc};
    use sqlx::PgPool;

    /// Postgres-backed block index over `core.blocks/transactions/events`.
    /// One transaction per block insert; every row carries lineage
    /// (Invariant 3). Conflicts are ignored: rows are immutable, re-ingesting
    /// an identical block is a no-op (ARCHITECTURE.md §15).
    pub struct PgBlockIndex {
        pool: PgPool,
    }

    impl PgBlockIndex {
        pub fn new(pool: PgPool) -> Self {
            Self { pool }
        }
    }

    #[async_trait]
    impl BlockIndex for PgBlockIndex {
        async fn blocks_by_hash(&self, hash: &str) -> Result<Vec<(String, u64)>, IndexError> {
            // index probe on blocks_hash_idx (0013), NOT a scan; and never a
            // prefix match — a partial hash is a range scan and is the one
            // thing in this grammar that genuinely does not scale.
            let rows: Vec<(String, i64)> = sqlx::query_as(
                "select chain_id, height from core.blocks where hash = $1 \
                 order by chain_id collate \"C\", height",
            )
            .bind(hash)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| IndexError(e.to_string()))?;
            Ok(rows.into_iter().map(|(c, h)| (c, h as u64)).collect())
        }

        async fn extrinsics_by_hash(
            &self,
            hash: &str,
        ) -> Result<Vec<(String, u64, u32)>, IndexError> {
            let rows: Vec<(String, i64, i32)> = sqlx::query_as(
                "select chain_id, block_height, tx_index from core.transactions \
                 where hash = $1 order by chain_id collate \"C\", block_height, tx_index",
            )
            .bind(hash)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| IndexError(e.to_string()))?;
            Ok(rows
                .into_iter()
                .map(|(c, h, i)| (c, h as u64, i as u32))
                .collect())
        }

        async fn get(
            &self,
            chain_id: &str,
            height: u64,
        ) -> Result<Option<CanonicalBlock>, IndexError> {
            let err = |e: sqlx::Error| IndexError(e.to_string());
            let head: Option<(String, String, Option<DateTime<Utc>>, bool, i64, i32, String)> =
                sqlx::query_as(
                    "select hash, parent_hash, timestamp, finalized, \
                            runtime_version, decoder_version, raw_location \
                     from core.blocks where chain_id = $1 and height = $2",
                )
                .bind(chain_id)
                .bind(height as i64)
                .fetch_optional(&self.pool)
                .await
                .map_err(err)?;
            let Some((hash, parent_hash, timestamp, finalized, runtime_version, decoder_version, raw_location)) =
                head
            else {
                return Ok(None);
            };

            let txs: Vec<(i32, Option<String>, Option<String>, String, serde_json::Value, bool)> =
                sqlx::query_as(
                    "select tx_index, hash, signer, call_name, args, success \
                     from core.transactions where chain_id = $1 and block_height = $2 \
                     order by tx_index",
                )
                .bind(chain_id)
                .bind(height as i64)
                .fetch_all(&self.pool)
                .await
                .map_err(err)?;

            let events: Vec<(i32, Option<i32>, String, serde_json::Value)> = sqlx::query_as(
                "select event_index, tx_index, name, data \
                 from core.events where chain_id = $1 and block_height = $2 \
                 order by event_index",
            )
            .bind(chain_id)
            .bind(height as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(err)?;

            Ok(Some(CanonicalBlock {
                chain_id: chain_id.to_string(),
                height,
                hash,
                parent_hash,
                timestamp,
                finalized,
                lineage: Lineage {
                    runtime_version: runtime_version as u32,
                    decoder_version: decoder_version as u32,
                    raw_location,
                },
                transactions: txs
                    .into_iter()
                    .map(|(index, hash, signer, call, args, success)| CanonicalTransaction {
                        index: index as u32,
                        hash,
                        signer,
                        call,
                        args,
                        success,
                    })
                    .collect(),
                events: events
                    .into_iter()
                    .map(|(index, tx, name, data)| CanonicalEvent {
                        index: index as u32,
                        transaction_index: tx.map(|t| t as u32),
                        name,
                        data,
                    })
                    .collect(),
            }))
        }

        async fn insert(&self, block: CanonicalBlock) -> Result<(), IndexError> {
            let err = |e: sqlx::Error| IndexError(e.to_string());
            let mut tx = self.pool.begin().await.map_err(err)?;

            // THE replacement rule (reorg safety, ARCHITECTURE §15): finalized
            // rows are immutable (insert is a no-op); unfinalized rows are
            // always replaceable — fork swaps and the finalized pipeline
            // superseding the tip both land here. Row + children replaced
            // atomically in this transaction.
            //
            // Advisory lock = the serialization point for (chain, height).
            // `select for update` alone cannot serialize the missing-row race
            // (tip inserting unfinalized vs decode inserting finalized at the
            // same height: both see None, children interleave) nor survive the
            // delete-reinsert pattern (EvalPlanQual returns zero rows to the
            // waiter). Review catch — without this, a finalized block could
            // permanently carry a losing fork's events.
            sqlx::query("select pg_advisory_xact_lock(hashtextextended($1, $2))")
                .bind(&block.chain_id)
                .bind(block.height as i64)
                .execute(&mut *tx)
                .await
                .map_err(err)?;
            let existing: Option<(bool,)> = sqlx::query_as(
                "select finalized from core.blocks where chain_id = $1 and height = $2 \
                 for update",
            )
            .bind(&block.chain_id)
            .bind(block.height as i64)
            .fetch_optional(&mut *tx)
            .await
            .map_err(err)?;
            match existing {
                Some((true,)) => {
                    // immutable — nothing to do (identical re-ingest or a
                    // late tip fetch racing finalization)
                    return tx.commit().await.map_err(err);
                }
                Some((false,)) => {
                    for table in ["events", "transactions"] {
                        sqlx::query(&format!(
                            "delete from core.{table} where chain_id = $1 and block_height = $2"
                        ))
                        .bind(&block.chain_id)
                        .bind(block.height as i64)
                        .execute(&mut *tx)
                        .await
                        .map_err(err)?;
                    }
                    sqlx::query("delete from core.blocks where chain_id = $1 and height = $2")
                        .bind(&block.chain_id)
                        .bind(block.height as i64)
                        .execute(&mut *tx)
                        .await
                        .map_err(err)?;
                }
                None => {}
            }

            sqlx::query(
                "insert into core.blocks (chain_id, height, hash, parent_hash, timestamp, \
                     finalized, runtime_version, decoder_version, raw_location) \
                 values ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
                 on conflict (chain_id, height) do nothing",
            )
            .bind(&block.chain_id)
            .bind(block.height as i64)
            .bind(&block.hash)
            .bind(&block.parent_hash)
            .bind(block.timestamp)
            .bind(block.finalized)
            .bind(block.lineage.runtime_version as i64)
            .bind(block.lineage.decoder_version as i32)
            .bind(&block.lineage.raw_location)
            .execute(&mut *tx)
            .await
            .map_err(err)?;

            for t in &block.transactions {
                sqlx::query(
                    "insert into core.transactions (chain_id, block_height, tx_index, hash, \
                         signer, call_name, args, success, \
                         runtime_version, decoder_version, raw_location) \
                     values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) \
                     on conflict (chain_id, block_height, tx_index) do nothing",
                )
                .bind(&block.chain_id)
                .bind(block.height as i64)
                .bind(t.index as i32)
                .bind(&t.hash)
                .bind(&t.signer)
                .bind(&t.call)
                .bind(&t.args)
                .bind(t.success)
                .bind(block.lineage.runtime_version as i64)
                .bind(block.lineage.decoder_version as i32)
                .bind(&block.lineage.raw_location)
                .execute(&mut *tx)
                .await
                .map_err(err)?;
            }

            for e in &block.events {
                sqlx::query(
                    "insert into core.events (chain_id, block_height, event_index, tx_index, \
                         name, data, runtime_version, decoder_version) \
                     values ($1, $2, $3, $4, $5, $6, $7, $8) \
                     on conflict (chain_id, block_height, event_index) do nothing",
                )
                .bind(&block.chain_id)
                .bind(block.height as i64)
                .bind(e.index as i32)
                .bind(e.transaction_index.map(|t| t as i32))
                .bind(&e.name)
                .bind(&e.data)
                .bind(block.lineage.runtime_version as i64)
                .bind(block.lineage.decoder_version as i32)
                .execute(&mut *tx)
                .await
                .map_err(err)?;
            }

            tx.commit().await.map_err(err)
        }

        async fn count(&self) -> Result<u64, IndexError> {
            let (n,): (i64,) = sqlx::query_as("select count(*) from core.blocks")
                .fetch_one(&self.pool)
                .await
                .map_err(|e| IndexError(e.to_string()))?;
            Ok(n as u64)
        }
    }

    /// Postgres-backed balance reads over `balances.*`. Numeric columns come
    /// back as text (`::text`) — plancks routinely exceed u64.
    pub struct PgBalanceIndex {
        pool: PgPool,
    }

    impl PgBalanceIndex {
        pub fn new(pool: PgPool) -> Self {
            Self { pool }
        }
    }

    #[async_trait]
    impl super::BalanceIndex for PgBalanceIndex {
        async fn changes(
            &self,
            chain_id: &str,
            account_id: &[u8],
            asset: &str,
            from: Option<DateTime<Utc>>,
            to: Option<DateTime<Utc>>,
        ) -> Result<Vec<super::BalanceChangeRow>, IndexError> {
            let rows: Vec<(i64, Option<DateTime<Utc>>, i32, String, String, Option<Vec<u8>>)> =
                sqlx::query_as(
                    "select c.block_height, b.timestamp, c.event_index, c.delta::text, \
                            c.reason, c.counterparty \
                     from balances.balance_changes c \
                     left join core.blocks b \
                       on b.chain_id = c.chain_id and b.height = c.block_height \
                     where c.chain_id = $1 and c.account_id = $2 and c.asset = $3 \
                       and ($4::timestamptz is null or b.timestamp >= $4) \
                       and ($5::timestamptz is null or b.timestamp < $5) \
                     order by c.block_height, c.event_index",
                )
                .bind(chain_id)
                .bind(account_id)
                .bind(asset)
                .bind(from)
                .bind(to)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| IndexError(e.to_string()))?;
            Ok(rows
                .into_iter()
                .map(|(height, timestamp, event_index, delta, reason, cp)| {
                    super::BalanceChangeRow {
                        height: height as u64,
                        timestamp,
                        event_index: event_index as u32,
                        delta,
                        reason,
                        counterparty: cp.map(|b| format!("0x{}", super::hex_lower(&b))),
                    }
                })
                .collect())
        }

        async fn anchors(
            &self,
            chain_id: &str,
            account_id: &[u8],
            asset: &str,
        ) -> Result<Vec<super::BalanceAnchorRow>, IndexError> {
            let rows: Vec<(
                i64,
                String,
                String,
                String,
                Option<i64>,
                String,
                Option<String>,
                Option<String>,
            )> = sqlx::query_as(
                "select block_height, free::text, reserved::text, total::text, \
                            spec_version, source, note, status \
                     from balances.balance_anchors \
                     where chain_id = $1 and account_id = $2 and asset = $3 \
                     order by block_height",
            )
            .bind(chain_id)
            .bind(account_id)
            .bind(asset)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| IndexError(e.to_string()))?;
            Ok(rows
                .into_iter()
                .map(|(height, free, reserved, total, spec, source, note, status)| {
                    super::BalanceAnchorRow {
                        height: height as u64,
                        free,
                        reserved,
                        total,
                        spec_version: spec.map(|s| s as u64),
                        source,
                        note,
                        status,
                    }
                })
                .collect())
        }

        /// ONE query for the whole snapshot, in three steps:
        ///
        ///   `pairs`  every (account, asset) this chain has ever ANCHORED or
        ///           MOVED for these accounts — the union is what stops the
        ///           answer from looking complete by leaving out what we never
        ///           anchored
        ///   `anch`   the latest anchor at or before the query height per pair
        ///           (`distinct on`, `nulls last` so a pair with no anchor
        ///           survives the join instead of vanishing)
        ///   lateral  the deltas strictly after that anchor — `coalesce(…, -1)`
        ///           so a pair with no anchor sums its whole history
        async fn holdings(
            &self,
            chain_id: &str,
            accounts: &[Vec<u8>],
            at_height: Option<u64>,
        ) -> Result<Vec<super::HoldingRow>, IndexError> {
            if accounts.is_empty() {
                return Ok(vec![]);
            }
            type Row = (
                Vec<u8>,
                String,
                Option<String>,
                Option<i64>,
                Option<i64>,
                Option<String>,
                Option<String>,
                Option<String>,
                Option<String>,
                i64,
                Option<i64>,
            );
            let rows: Vec<Row> = sqlx::query_as(
                "with acct as (select unnest($2::bytea[]) as account_id), \
                      pairs as ( \
                        select a.account_id, a.asset from balances.balance_anchors a \
                          join acct using (account_id) \
                         where a.chain_id = $1 \
                           and ($3::bigint is null or a.block_height <= $3) \
                        union \
                        select c.account_id, c.asset from balances.balance_changes c \
                          join acct using (account_id) \
                         where c.chain_id = $1 \
                           and ($3::bigint is null or c.block_height <= $3) \
                      ), \
                      anch as ( \
                        select distinct on (p.account_id, p.asset) \
                               p.account_id, p.asset, a.total::text as total, \
                               a.block_height, a.spec_version, a.source, a.note, a.status \
                          from pairs p \
                          left join balances.balance_anchors a \
                            on a.chain_id = $1 and a.account_id = p.account_id \
                           and a.asset = p.asset \
                           and ($3::bigint is null or a.block_height <= $3) \
                         order by p.account_id, p.asset, a.block_height desc nulls last \
                      ) \
                 select anch.account_id, anch.asset, anch.total, anch.block_height, \
                        anch.spec_version, anch.source, anch.note, anch.status, \
                        coalesce(d.delta_sum, '0') as delta_sum, \
                        coalesce(d.delta_count, 0) as delta_count, d.last_height \
                   from anch \
                   left join lateral ( \
                        select sum(c.delta)::text as delta_sum, count(*) as delta_count, \
                               max(c.block_height) as last_height \
                          from balances.balance_changes c \
                         where c.chain_id = $1 and c.account_id = anch.account_id \
                           and c.asset = anch.asset \
                           and c.block_height > coalesce(anch.block_height, -1) \
                           and ($3::bigint is null or c.block_height <= $3) \
                   ) d on true \
                  order by anch.account_id, anch.asset collate \"C\"",
            )
            .bind(chain_id)
            .bind(accounts)
            .bind(at_height.map(|h| h as i64))
            .fetch_all(&self.pool)
            .await
            .map_err(|e| IndexError(e.to_string()))?;
            Ok(rows
                .into_iter()
                .map(
                    |(
                        account_id,
                        asset,
                        total,
                        height,
                        spec,
                        source,
                        note,
                        status,
                        delta_sum,
                        delta_count,
                        last_height,
                    )| super::HoldingRow {
                        account_id,
                        asset,
                        anchor_total: total,
                        anchor_height: height.map(|h| h as u64),
                        anchor_spec_version: spec.map(|s| s as u64),
                        anchor_source: source,
                        anchor_note: note,
                        anchor_status: status,
                        delta_sum: delta_sum.unwrap_or_else(|| "0".into()),
                        delta_count: delta_count as u64,
                        last_delta_height: last_height.map(|h| h as u64),
                    },
                )
                .collect())
        }
    }

    /// Postgres-backed XCM reads over `xcm.messages`.
    pub struct PgXcmIndex {
        pool: PgPool,
    }

    impl PgXcmIndex {
        pub fn new(pool: PgPool) -> Self {
            Self { pool }
        }
    }

    impl PgXcmIndex {
        fn row(r: &sqlx::postgres::PgRow) -> Result<super::XcmMessageRow, IndexError> {
            use sqlx::Row as _;
            let err = |e: sqlx::Error| IndexError(e.to_string());
            Ok(super::XcmMessageRow {
                chain_id: r.try_get("chain_id").map_err(err)?,
                block_height: r.try_get::<i64, _>("block_height").map_err(err)? as u64,
                event_index: r.try_get::<i32, _>("event_index").map_err(err)? as u32,
                side: r.try_get("side").map_err(err)?,
                transport: r.try_get("transport").map_err(err)?,
                message_id: r.try_get("message_id").map_err(err)?,
                id_kind: r.try_get("id_kind").map_err(err)?,
                counterparty: r.try_get("counterparty").map_err(err)?,
                origin_location: r.try_get("origin_location").map_err(err)?,
                destination: r.try_get("destination").map_err(err)?,
                message: r.try_get("message").map_err(err)?,
                forwarded: r.try_get("forwarded").map_err(err)?,
                status: r.try_get("status").map_err(err)?,
                success: r.try_get("success").map_err(err)?,
                error: r.try_get("error").map_err(err)?,
                weight_used: r.try_get("weight_used").map_err(err)?,
                runtime_version: r.try_get::<i64, _>("runtime_version").map_err(err)? as u64,
                mapper_version: r.try_get::<i32, _>("mapper_version").map_err(err)? as u32,
                timestamp: r.try_get("block_timestamp").map_err(err)?,
            })
        }

        fn link(r: &sqlx::postgres::PgRow) -> Result<super::XcmLinkRow, IndexError> {
            use sqlx::Row as _;
            let err = |e: sqlx::Error| IndexError(e.to_string());
            Ok(super::XcmLinkRow {
                chain_id: r.try_get("chain_id").map_err(err)?,
                block_height: r.try_get::<i64, _>("block_height").map_err(err)? as u64,
                wire_event_index: r.try_get::<i32, _>("wire_event_index").map_err(err)? as u32,
                topic_event_index: r.try_get::<i32, _>("topic_event_index").map_err(err)? as u32,
                wire_hash: r.try_get("wire_hash").map_err(err)?,
                topic: r.try_get("topic").map_err(err)?,
                transport: r.try_get("transport").map_err(err)?,
                rule: r.try_get("rule").map_err(err)?,
                confidence: r.try_get("confidence").map_err(err)?,
                evidence: r.try_get("evidence").map_err(err)?,
                runtime_version: r.try_get::<i64, _>("runtime_version").map_err(err)? as u64,
                correlator_version: r.try_get::<i32, _>("correlator_version").map_err(err)? as u32,
            })
        }
    }

    /// Every column is `m.`-qualified and the timestamp is aliased, because
    /// `core.blocks` carries `chain_id` and `runtime_version` too — an
    /// unqualified list here would be ambiguous at best and silently return the
    /// BLOCK's runtime version at worst.
    const XCM_COLS: &str = "m.chain_id, m.block_height, m.event_index, m.side, m.transport, \
         m.message_id, m.id_kind, m.counterparty, m.origin_location, m.destination, m.message, \
         m.forwarded, m.status, m.success, m.error, m.weight_used, m.runtime_version, \
         m.mapper_version, b.timestamp as block_timestamp";

    /// LEFT join: an observation whose block row is missing (or whose timestamp
    /// is null, which `core.blocks` permits) must still be returned. Dropping it
    /// would make a journey silently lose a step for want of a clock.
    const XCM_FROM: &str = "from xcm.messages m left join core.blocks b \
         on b.chain_id = m.chain_id and b.height = m.block_height";

    const LINK_COLS: &str = "chain_id, block_height, wire_event_index, topic_event_index, \
         wire_hash, topic, transport, rule, confidence, evidence, runtime_version, \
         correlator_version";

    #[async_trait]
    impl super::XcmIndex for PgXcmIndex {
        async fn messages(
            &self,
            chain_id: &str,
            limit: u32,
        ) -> Result<Vec<super::XcmMessageRow>, IndexError> {
            let rows = sqlx::query(&format!(
                "select {XCM_COLS} {XCM_FROM} where m.chain_id = $1 \
                 order by m.block_height desc, m.event_index limit $2"
            ))
            .bind(chain_id)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| IndexError(e.to_string()))?;
            rows.iter().map(Self::row).collect()
        }

        async fn by_message_id(
            &self,
            message_id: &str,
        ) -> Result<Vec<super::XcmMessageRow>, IndexError> {
            let ids = [message_id.to_string()];
            self.by_message_ids(&ids).await
        }

        async fn by_message_ids(
            &self,
            ids: &[String],
        ) -> Result<Vec<super::XcmMessageRow>, IndexError> {
            if ids.is_empty() {
                return Ok(vec![]);
            }
            // messages_id_idx (0015) — a point probe per partition per id, never
            // a scan and never a prefix match.
            let rows = sqlx::query(&format!(
                "select {XCM_COLS} {XCM_FROM} where m.message_id = any($1) \
                 order by m.chain_id collate \"C\", m.block_height, m.event_index"
            ))
            .bind(ids)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| IndexError(e.to_string()))?;
            rows.iter().map(Self::row).collect()
        }

        async fn aliases(&self, message_id: &str) -> Result<Vec<super::XcmLinkRow>, IndexError> {
            // message_links_wire_idx + message_links_topic_idx (0016). An `or`
            // over two indexed columns is a BitmapOr of two index scans, which
            // is what those two indexes exist for.
            let rows = sqlx::query(&format!(
                "select {LINK_COLS} from xcm.message_links \
                 where wire_hash = $1 or topic = $1 \
                 order by chain_id collate \"C\", block_height, wire_event_index"
            ))
            .bind(message_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| IndexError(e.to_string()))?;
            rows.iter().map(Self::link).collect()
        }
    }

    /// Postgres-backed simulation reads over `sim.simulation_results`.
    pub struct PgSimIndex {
        pool: PgPool,
    }

    impl PgSimIndex {
        pub fn new(pool: PgPool) -> Self {
            Self { pool }
        }
    }

    #[async_trait]
    impl super::SimIndex for PgSimIndex {
        async fn simulations(
            &self,
            chain_id: &str,
            call_hash: &str,
            limit: u32,
        ) -> Result<Vec<super::SimulationRow>, IndexError> {
            // (chain_id, call_hash, at_height desc) is simulation_results_call_idx
            // verbatim — the index and its one reader ship together (0014).
            //
            // Read by COLUMN NAME rather than into a tuple: this row has 24
            // columns and sqlx only implements FromRow for tuples up to 16, so a
            // tuple here does not compile. Naming the columns is also the safer
            // shape for a row this wide — a reordered select cannot silently
            // transpose two same-typed fields.
            use sqlx::Row as _;
            let rows = sqlx::query(
                "select chain_id, at_height, at_block_hash, input_hash, tier, call_hash, \
                        call_summary, origin_spec, origin_json, xcm_version, status, \
                        dispatch_ok, dispatch_error, emitted_events, event_count, local_xcm, \
                        forwarded_xcms, effects, note, spec_version, api_version, \
                        metadata_version, sim_version, raw_location, observed_at \
                 from sim.simulation_results \
                 where chain_id = $1 and call_hash = $2 \
                 order by at_height desc, input_hash collate \"C\", \
                          at_block_hash collate \"C\", tier collate \"C\" \
                 limit $3",
            )
            .bind(chain_id)
            .bind(call_hash)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| IndexError(e.to_string()))?;

            let err = |e: sqlx::Error| IndexError(e.to_string());
            rows.into_iter()
                .map(|r| {
                    Ok(super::SimulationRow {
                        chain_id: r.try_get("chain_id").map_err(err)?,
                        at_height: r.try_get::<i64, _>("at_height").map_err(err)? as u64,
                        at_block_hash: r.try_get("at_block_hash").map_err(err)?,
                        input_hash: r.try_get("input_hash").map_err(err)?,
                        tier: r.try_get("tier").map_err(err)?,
                        call_hash: r.try_get("call_hash").map_err(err)?,
                        call_summary: r.try_get("call_summary").map_err(err)?,
                        origin_spec: r.try_get("origin_spec").map_err(err)?,
                        origin: r.try_get("origin_json").map_err(err)?,
                        xcm_version: r.try_get::<i32, _>("xcm_version").map_err(err)? as u32,
                        status: r.try_get("status").map_err(err)?,
                        dispatch_ok: r.try_get("dispatch_ok").map_err(err)?,
                        dispatch_error: r.try_get("dispatch_error").map_err(err)?,
                        emitted_events: r.try_get("emitted_events").map_err(err)?,
                        event_count: r.try_get::<i32, _>("event_count").map_err(err)? as u32,
                        local_xcm: r.try_get("local_xcm").map_err(err)?,
                        forwarded_xcms: r.try_get("forwarded_xcms").map_err(err)?,
                        effects: r.try_get("effects").map_err(err)?,
                        note: r.try_get("note").map_err(err)?,
                        spec_version: r.try_get::<i64, _>("spec_version").map_err(err)? as u64,
                        api_version: r.try_get::<i32, _>("api_version").map_err(err)? as u32,
                        metadata_version: r
                            .try_get::<i32, _>("metadata_version")
                            .map_err(err)? as u32,
                        sim_version: r.try_get::<i32, _>("sim_version").map_err(err)? as u32,
                        raw_location: r.try_get("raw_location").map_err(err)?,
                        observed_at: r.try_get("observed_at").map_err(err)?,
                    })
                })
                .collect()
        }
    }

    /// Postgres-backed asset registry reads over `core.assets`.
    pub struct PgAssetIndex {
        pool: PgPool,
    }

    impl PgAssetIndex {
        pub fn new(pool: PgPool) -> Self {
            Self { pool }
        }
    }

    #[async_trait]
    impl super::AssetIndex for PgAssetIndex {
        async fn assets_by_symbol(
            &self,
            symbol: &str,
        ) -> Result<Vec<(String, super::AssetRow)>, IndexError> {
            // `lower(symbol)` matches assets_symbol_idx (0013) EXACTLY — a
            // functional index is only used when the query spells the
            // expression the same way, so this is not a stylistic choice.
            type Row = (
                String,
                String,
                String,
                Option<String>,
                Option<String>,
                Option<i32>,
                Option<String>,
                Option<String>,
                Option<String>,
                Option<serde_json::Value>,
            );
            let rows: Vec<Row> = sqlx::query_as(
                "select chain_id, asset_key, representation_kind, symbol, name, decimals, \
                        supply::text, status, location_key, xcm_location \
                 from core.assets where lower(symbol) = lower($1) \
                 order by chain_id collate \"C\", asset_key collate \"C\"",
            )
            .bind(symbol)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| IndexError(e.to_string()))?;
            Ok(rows
                .into_iter()
                .map(
                    |(
                        chain_id,
                        asset_key,
                        representation_kind,
                        symbol,
                        name,
                        decimals,
                        supply,
                        status,
                        location_key,
                        xcm_location,
                    )| {
                        (
                            chain_id,
                            super::AssetRow {
                                asset_key,
                                representation_kind,
                                symbol,
                                name,
                                decimals: decimals.map(|d| d as u32),
                                supply,
                                status,
                                location_key,
                                xcm_location,
                            },
                        )
                    },
                )
                .collect())
        }

        async fn assets(&self, chain_id: &str) -> Result<Vec<super::AssetRow>, IndexError> {
            type Row = (
                String,
                String,
                Option<String>,
                Option<String>,
                Option<i32>,
                Option<String>,
                Option<String>,
                Option<String>,
                Option<serde_json::Value>,
            );
            let rows: Vec<Row> = sqlx::query_as(
                "select asset_key, representation_kind, symbol, name, decimals, \
                        supply::text, status, location_key, xcm_location \
                 from core.assets where chain_id = $1 order by asset_key collate \"C\"",
            )
            .bind(chain_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| IndexError(e.to_string()))?;
            Ok(rows
                .into_iter()
                .map(
                    |(
                        asset_key,
                        representation_kind,
                        symbol,
                        name,
                        decimals,
                        supply,
                        status,
                        location_key,
                        xcm_location,
                    )| super::AssetRow {
                        asset_key,
                        representation_kind,
                        symbol,
                        name,
                        decimals: decimals.map(|d| d as u32),
                        supply,
                        status,
                        location_key,
                        xcm_location,
                    },
                )
                .collect())
        }
    }

    /// Postgres-backed label reads over `core.account_labels`.
    pub struct PgLabelIndex {
        pool: PgPool,
    }

    impl PgLabelIndex {
        pub fn new(pool: PgPool) -> Self {
            Self { pool }
        }
    }

    #[async_trait]
    impl super::LabelIndex for PgLabelIndex {
        async fn labels_for(
            &self,
            chain_id: &str,
            account_id: &[u8],
        ) -> Result<Vec<canonical::AccountLabel>, IndexError> {
            let rows: Vec<(
                String,
                String,
                Option<String>,
                String,
                Option<String>,
                Option<DateTime<Utc>>,
                Option<i64>,
                Option<String>,
            )> = sqlx::query_as(
                "select kind, label, derivation, source, ss58, \
                        verified_at, verified_block, verified_note \
                 from core.account_labels \
                 where account_id = $1 and (chain_scope = $2 or chain_scope = '*') \
                 order by kind, label",
            )
            .bind(account_id)
            .bind(chain_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| IndexError(e.to_string()))?;
            Ok(rows
                .into_iter()
                .map(
                    |(kind, label, derivation, source, ss58, verified_at, verified_block, verified_note)| {
                        canonical::AccountLabel {
                            kind,
                            label,
                            derivation,
                            source,
                            ss58,
                            verified_at,
                            verified_block: verified_block.map(|b| b as u64),
                            verified_note,
                        }
                    },
                )
                .collect())
        }
    }

    /// Postgres-backed gov reads over `gov.*`.
    pub struct PgGovIndex {
        pool: PgPool,
    }

    impl PgGovIndex {
        pub fn new(pool: PgPool) -> Self {
            Self { pool }
        }
    }

    /// Shared by the point lookup and the list so the two cannot disagree
    /// about column order — the projection has ten columns and two of them are
    /// nullable i64 heights, which is exactly where a transposition hides.
    #[allow(clippy::type_complexity)]
    fn whitelisted_row(
        (
            call_hash,
            status,
            dispatch_ok,
            dispatch_error,
            dispatch_height,
            first_seen_height,
            whitelisted_height,
            status_height,
            runtime_version,
            mapper_version,
        ): (
            String,
            String,
            Option<bool>,
            Option<serde_json::Value>,
            Option<i64>,
            i64,
            Option<i64>,
            i64,
            i64,
            i32,
        ),
    ) -> super::WhitelistedCallRow {
        super::WhitelistedCallRow {
            call_hash,
            status,
            dispatch_ok,
            dispatch_error,
            dispatch_height: dispatch_height.map(|h| h as u64),
            first_seen_height: first_seen_height as u64,
            whitelisted_height: whitelisted_height.map(|h| h as u64),
            status_height: status_height as u64,
            runtime_version: runtime_version as u64,
            mapper_version: mapper_version as u32,
        }
    }

    #[allow(clippy::type_complexity)]
    fn referendum_from_row(
        (class, referendum_id, track_id, status, status_height, proposal, proposal_hash, proposal_len, submitted_at): (
            String,
            i64,
            Option<i32>,
            String,
            i64,
            Option<serde_json::Value>,
            Option<String>,
            Option<i64>,
            Option<i64>,
        ),
    ) -> super::ReferendumRow {
        super::ReferendumRow {
            class,
            referendum_id: referendum_id as u64,
            track_id: track_id.map(|t| t as u32),
            status,
            status_height: status_height as u64,
            proposal,
            proposal_hash,
            proposal_len: proposal_len.map(|l| l as u64),
            submitted_at_height: submitted_at.map(|h| h as u64),
        }
    }

    const REFERENDUM_COLS: &str = "class, referendum_id, track_id, status, status_height, \
                                   proposal, proposal_hash, proposal_len, submitted_at_height";

    #[async_trait]
    impl super::GovIndex for PgGovIndex {
        async fn referendum(
            &self,
            chain_id: &str,
            class: &str,
            id: u64,
        ) -> Result<Option<super::ReferendumRow>, IndexError> {
            let row = sqlx::query_as(&format!(
                "select {REFERENDUM_COLS} from gov.referenda \
                 where chain_id = $1 and class = $2 and referendum_id = $3"
            ))
            .bind(chain_id)
            .bind(class)
            .bind(id as i64)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| IndexError(e.to_string()))?;
            Ok(row.map(referendum_from_row))
        }

        async fn referendum_events(
            &self,
            chain_id: &str,
            class: &str,
            id: u64,
        ) -> Result<Vec<super::ReferendumEventRow>, IndexError> {
            let rows: Vec<(i64, Option<DateTime<Utc>>, i32, String, serde_json::Value)> =
                sqlx::query_as(
                    "select e.block_height, b.timestamp, e.event_index, e.kind, e.data \
                     from gov.referendum_events e \
                     left join core.blocks b \
                       on b.chain_id = e.chain_id and b.height = e.block_height \
                     where e.chain_id = $1 and e.class = $2 and e.referendum_id = $3 \
                     order by e.block_height, e.event_index",
                )
                .bind(chain_id)
                .bind(class)
                .bind(id as i64)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| IndexError(e.to_string()))?;
            Ok(rows
                .into_iter()
                .map(|(height, timestamp, event_index, kind, data)| super::ReferendumEventRow {
                    height: height as u64,
                    timestamp,
                    event_index: event_index as u32,
                    kind,
                    data,
                })
                .collect())
        }

        async fn list_referenda(
            &self,
            chain_id: &str,
            class: &str,
            limit: u64,
        ) -> Result<Vec<super::ReferendumRow>, IndexError> {
            let rows: Vec<_> = sqlx::query_as(&format!(
                "select {REFERENDUM_COLS} from gov.referenda \
                 where chain_id = $1 and class = $2 \
                 order by referendum_id desc limit $3"
            ))
            .bind(chain_id)
            .bind(class)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| IndexError(e.to_string()))?;
            Ok(rows.into_iter().map(referendum_from_row).collect())
        }

        async fn tracks(&self, chain_id: &str) -> Result<Vec<super::GovTrackRow>, IndexError> {
            let rows: Vec<(String, i32, String, serde_json::Value, i64)> = sqlx::query_as(
                "select pallet, track_id, name, params, spec_version from gov.tracks \
                 where chain_id = $1 order by pallet, track_id",
            )
            .bind(chain_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| IndexError(e.to_string()))?;
            Ok(rows
                .into_iter()
                .map(|(pallet, track_id, name, params, spec_version)| super::GovTrackRow {
                    pallet,
                    track_id: track_id as u32,
                    name,
                    params,
                    spec_version: spec_version as u64,
                })
                .collect())
        }

        async fn whitelisted_call(
            &self,
            chain_id: &str,
            call_hash: &str,
        ) -> Result<Option<super::WhitelistedCallRow>, IndexError> {
            let row: Option<(
                String,
                String,
                Option<bool>,
                Option<serde_json::Value>,
                Option<i64>,
                i64,
                Option<i64>,
                i64,
                i64,
                i32,
            )> = sqlx::query_as(
                "select call_hash, status, dispatch_ok, dispatch_error, dispatch_height, \
                        first_seen_height, whitelisted_height, status_height, \
                        runtime_version, mapper_version \
                 from gov.whitelisted_calls \
                 where chain_id = $1 and call_hash = $2",
            )
            .bind(chain_id)
            .bind(call_hash)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| IndexError(e.to_string()))?;
            Ok(row.map(whitelisted_row))
        }

        async fn whitelist_events(
            &self,
            chain_id: &str,
            call_hash: &str,
        ) -> Result<Vec<super::WhitelistEventRow>, IndexError> {
            let rows: Vec<(i64, i32, String, Option<bool>, Option<serde_json::Value>, serde_json::Value, i64)> =
                sqlx::query_as(
                    "select block_height, event_index, kind, dispatch_ok, dispatch_error, \
                            data, runtime_version \
                     from gov.whitelist_events \
                     where chain_id = $1 and call_hash = $2 \
                     order by block_height, event_index",
                )
                .bind(chain_id)
                .bind(call_hash)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| IndexError(e.to_string()))?;
            Ok(rows
                .into_iter()
                .map(|(h, i, kind, ok, err, data, rv)| super::WhitelistEventRow {
                    block_height: h as u64,
                    event_index: i as u32,
                    kind,
                    dispatch_ok: ok,
                    dispatch_error: err,
                    data,
                    runtime_version: rv as u64,
                })
                .collect())
        }

        async fn list_whitelisted_calls(
            &self,
            chain_id: &str,
            limit: u64,
        ) -> Result<Vec<super::WhitelistedCallRow>, IndexError> {
            // ordering identical to MemoryGovIndex so `limit` returns the same
            // subset from either backend; `collate "C"` so the hash tiebreak
            // sorts byte-wise in both, the fix slice 6 made for assets.
            let rows: Vec<(
                String,
                String,
                Option<bool>,
                Option<serde_json::Value>,
                Option<i64>,
                i64,
                Option<i64>,
                i64,
                i64,
                i32,
            )> = sqlx::query_as(
                "select call_hash, status, dispatch_ok, dispatch_error, dispatch_height, \
                        first_seen_height, whitelisted_height, status_height, \
                        runtime_version, mapper_version \
                 from gov.whitelisted_calls \
                 where chain_id = $1 \
                 order by status_height desc, call_hash collate \"C\" desc \
                 limit $2",
            )
            .bind(chain_id)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| IndexError(e.to_string()))?;
            Ok(rows.into_iter().map(whitelisted_row).collect())
        }

        async fn preimage(
            &self,
            chain_id: &str,
            proposal_hash: &str,
        ) -> Result<Option<super::PreimageRow>, IndexError> {
            // a decoded row wins over missing/undecodable attempts
            let row: Option<(
                String,
                i64,
                String,
                String,
                Option<String>,
                Option<serde_json::Value>,
                Option<String>,
                Option<i64>,
                Option<i64>,
            )> = sqlx::query_as(
                "select proposal_hash, len, decode_status, source, call_summary, \
                        decoded_call, note, spec_version, fetched_at_height \
                 from gov.preimages \
                 where chain_id = $1 and proposal_hash = $2 \
                 order by (decode_status = 'decoded') desc, len desc limit 1",
            )
            .bind(chain_id)
            .bind(proposal_hash)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| IndexError(e.to_string()))?;
            Ok(row.map(
                |(proposal_hash, len, decode_status, source, call_summary, decoded_call, note, spec, height)| {
                    super::PreimageRow {
                        proposal_hash,
                        len: len as u64,
                        decode_status,
                        source,
                        call_summary,
                        decoded_call,
                        note,
                        spec_version: spec.map(|s| s as u64),
                        fetched_at_height: height.map(|h| h as u64),
                    }
                },
            ))
        }

        async fn referendum_votes(
            &self,
            chain_id: &str,
            class: &str,
            id: u64,
            limit: u64,
        ) -> Result<Vec<super::VoteRow>, IndexError> {
            let rows: Vec<VoteTuple> = sqlx::query_as(&format!(
                "select {VOTE_COLS} from gov.vote_positions \
                 where chain_id = $1 and class = $2 and referendum_id = $3 \
                 order by (aye_votes + nay_votes) desc, voter limit $4"
            ))
            .bind(chain_id)
            .bind(class)
            .bind(id as i64)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| IndexError(e.to_string()))?;
            Ok(rows.into_iter().map(vote_from_row).collect())
        }

        async fn account_votes(
            &self,
            chain_id: &str,
            voter: &[u8],
            limit: u64,
        ) -> Result<Vec<super::VoteRow>, IndexError> {
            let rows: Vec<VoteTuple> = sqlx::query_as(&format!(
                "select {VOTE_COLS} from gov.vote_positions \
                 where chain_id = $1 and voter = $2 \
                 order by status_height desc, referendum_id desc limit $3"
            ))
            .bind(chain_id)
            .bind(voter)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| IndexError(e.to_string()))?;
            Ok(rows.into_iter().map(vote_from_row).collect())
        }

        async fn account_delegations(
            &self,
            chain_id: &str,
            delegator: &[u8],
        ) -> Result<Vec<super::DelegationRow>, IndexError> {
            let rows: Vec<(String, i32, Vec<u8>, Option<Vec<u8>>, bool, i64)> = sqlx::query_as(
                "select class, track_id, delegator, target, active, status_height \
                 from gov.delegations \
                 where chain_id = $1 and delegator = $2 \
                 order by class, track_id",
            )
            .bind(chain_id)
            .bind(delegator)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| IndexError(e.to_string()))?;
            Ok(rows
                .into_iter()
                .map(|(class, track_id, delegator, target, active, height)| {
                    super::DelegationRow {
                        class,
                        track_id: track_id as u32,
                        delegator: format!("0x{}", super::hex_lower(&delegator)),
                        target: target.map(|t| format!("0x{}", super::hex_lower(&t))),
                        active,
                        height: height as u64,
                    }
                })
                .collect())
        }

        async fn voting_anchors(
            &self,
            chain_id: &str,
            account: &[u8],
        ) -> Result<Vec<super::VotingAnchorRow>, IndexError> {
            let rows: Vec<(
                String,
                i32,
                i64,
                String,
                Option<Vec<u8>>,
                Option<String>,
                Option<String>,
                Option<String>,
                Option<String>,
                Option<i64>,
                Option<String>,
            )> = sqlx::query_as(
                "select class, track_id, block_height, mode, delegating_target, \
                        delegating_balance::text, delegating_conviction_label, \
                        delegations_votes::text, delegations_capital::text, \
                        spec_version, note \
                 from gov.voting_anchors \
                 where chain_id = $1 and account_id = $2 \
                 order by track_id, block_height",
            )
            .bind(chain_id)
            .bind(account)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| IndexError(e.to_string()))?;
            Ok(rows
                .into_iter()
                .map(
                    |(
                        class,
                        track_id,
                        height,
                        mode,
                        target,
                        balance,
                        conviction_label,
                        votes,
                        capital,
                        spec,
                        note,
                    )| super::VotingAnchorRow {
                        class,
                        track_id: track_id as u32,
                        height: height as u64,
                        mode,
                        delegating_target: target
                            .map(|t| format!("0x{}", super::hex_lower(&t))),
                        delegating_balance: balance,
                        delegating_conviction_label: conviction_label,
                        delegations_votes: votes,
                        delegations_capital: capital,
                        spec_version: spec.map(|s| s as u64),
                        note,
                    },
                )
                .collect())
        }
    }

    /// Postgres-backed treasury reads over `treasury.*`.
    pub struct PgTreasuryIndex {
        pool: PgPool,
    }

    impl PgTreasuryIndex {
        pub fn new(pool: PgPool) -> Self {
            Self { pool }
        }
    }

    /// `asset_ref` folds the slice-6 columns into ONE jsonb value rather than
    /// two more tuple elements — sqlx's `FromRow` for tuples stops at 16, and a
    /// spend row that cannot grow another column is a schema with a cliff in it.
    ///
    /// Note the round trip this relies on: `asset_location` is stored as jsonb,
    /// whose key order is Postgres's (by length, then bytes), NOT ours. It
    /// comes back through serde_json, whose Map is a BTreeMap, so re-rendering
    /// it yields the SAME canonical string `core.assets.location_key` holds.
    /// The pg integration test asserts that join rather than assuming it.
    const SPEND_COLS: &str = "instance, spend_kind, spend_id, status, amount::text, \
                              slashed::text, asset_kind, beneficiary, beneficiary_location, \
                              payment_id, valid_from, expire_at, first_seen_height, status_height, \
                              case when asset_location is null and asset_key is null then null \
                                   else jsonb_build_object('location', asset_location, \
                                                           'key', asset_key) end";

    type SpendTuple = (
        String,
        String,
        i64,
        String,
        Option<String>,
        Option<String>,
        Option<serde_json::Value>,
        Option<Vec<u8>>,
        Option<serde_json::Value>,
        Option<String>,
        Option<i64>,
        Option<i64>,
        i64,
        i64,
        Option<serde_json::Value>,
    );

    fn spend_from_row(
        (
            instance,
            spend_kind,
            spend_id,
            status,
            amount,
            slashed,
            asset_kind,
            beneficiary,
            beneficiary_location,
            payment_id,
            valid_from,
            expire_at,
            first_seen_height,
            status_height,
            asset_ref,
        ): SpendTuple,
    ) -> super::SpendRow {
        super::SpendRow {
            instance,
            spend_kind,
            spend_id: spend_id as u64,
            status,
            amount,
            slashed,
            asset_kind,
            beneficiary: beneficiary.map(|b| format!("0x{}", super::hex_lower(&b))),
            beneficiary_location,
            payment_id,
            valid_from: valid_from.map(|v| v as u64),
            expire_at: expire_at.map(|v| v as u64),
            first_seen_height: first_seen_height as u64,
            status_height: status_height as u64,
            asset_ref,
        }
    }

    type SpendEventTuple = (
        i64,
        Option<DateTime<Utc>>,
        i32,
        String,
        Option<String>,
        serde_json::Value,
    );

    fn spend_event_from_row(
        (height, timestamp, event_index, kind, amount, data): SpendEventTuple,
    ) -> super::SpendEventRow {
        super::SpendEventRow {
            height: height as u64,
            timestamp,
            event_index: event_index as u32,
            kind,
            amount,
            data,
        }
    }

    #[async_trait]
    impl super::TreasuryIndex for PgTreasuryIndex {
        async fn spends(
            &self,
            chain_id: &str,
            instance: &str,
            status: Option<&str>,
            spend_kind: Option<&str>,
            limit: u64,
        ) -> Result<Vec<super::SpendRow>, IndexError> {
            let rows: Vec<SpendTuple> = sqlx::query_as(&format!(
                "select {SPEND_COLS} from treasury.spends \
                 where chain_id = $1 and instance = $2 \
                   and ($3::text is null or status = $3) \
                   and ($4::text is null or spend_kind = $4) \
                 order by spend_kind, spend_id desc limit $5"
            ))
            .bind(chain_id)
            .bind(instance)
            .bind(status)
            .bind(spend_kind)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| IndexError(e.to_string()))?;
            Ok(rows.into_iter().map(spend_from_row).collect())
        }

        async fn spend(
            &self,
            chain_id: &str,
            instance: &str,
            spend_kind: &str,
            spend_id: u64,
        ) -> Result<Option<super::SpendRow>, IndexError> {
            let row: Option<SpendTuple> = sqlx::query_as(&format!(
                "select {SPEND_COLS} from treasury.spends \
                 where chain_id = $1 and instance = $2 and spend_kind = $3 and spend_id = $4"
            ))
            .bind(chain_id)
            .bind(instance)
            .bind(spend_kind)
            .bind(spend_id as i64)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| IndexError(e.to_string()))?;
            Ok(row.map(spend_from_row))
        }

        async fn spend_events(
            &self,
            chain_id: &str,
            instance: &str,
            spend_kind: &str,
            spend_id: u64,
        ) -> Result<Vec<super::SpendEventRow>, IndexError> {
            let rows: Vec<SpendEventTuple> = sqlx::query_as(
                "select e.block_height, b.timestamp, e.event_index, e.kind, e.amount::text, e.data \
                 from treasury.spend_events e \
                 left join core.blocks b \
                   on b.chain_id = e.chain_id and b.height = e.block_height \
                 where e.chain_id = $1 and e.instance = $2 \
                   and e.spend_kind = $3 and e.spend_id = $4 \
                 order by e.block_height, e.event_index",
            )
            .bind(chain_id)
            .bind(instance)
            .bind(spend_kind)
            .bind(spend_id as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| IndexError(e.to_string()))?;
            Ok(rows.into_iter().map(spend_event_from_row).collect())
        }

        async fn pot_events(
            &self,
            chain_id: &str,
            instance: &str,
            limit: u64,
        ) -> Result<Vec<super::SpendEventRow>, IndexError> {
            let rows: Vec<SpendEventTuple> = sqlx::query_as(
                "select e.block_height, b.timestamp, e.event_index, e.kind, e.amount::text, e.data \
                 from treasury.spend_events e \
                 left join core.blocks b \
                   on b.chain_id = e.chain_id and b.height = e.block_height \
                 where e.chain_id = $1 and e.instance = $2 and e.spend_id is null \
                 order by e.block_height desc, e.event_index desc limit $3",
            )
            .bind(chain_id)
            .bind(instance)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| IndexError(e.to_string()))?;
            Ok(rows.into_iter().map(spend_event_from_row).collect())
        }

        async fn accounts(
            &self,
            network: &str,
        ) -> Result<Vec<super::TreasuryAccountRow>, IndexError> {
            type Row = (
                String,
                Vec<u8>,
                String,
                Option<String>,
                String,
                Option<String>,
                String,
                Option<String>,
            );
            let rows: Vec<Row> = sqlx::query_as(
                "select chain_id, account_id, role, instance, label, derivation, source, ss58 \
                 from treasury.treasury_accounts where network = $1 and active \
                 order by chain_id, role, label",
            )
            .bind(network)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| IndexError(e.to_string()))?;
            Ok(rows
                .into_iter()
                .map(
                    |(chain_id, account_id, role, instance, label, derivation, source, ss58)| {
                        super::TreasuryAccountRow {
                            chain_id,
                            account_id,
                            role,
                            instance,
                            label,
                            derivation,
                            source,
                            ss58,
                        }
                    },
                )
                .collect())
        }
    }

    /// Postgres-backed bounty reads over `treasury.bounties` /
    /// `treasury.bounty_events`.
    pub struct PgBountyIndex {
        pool: PgPool,
    }

    impl PgBountyIndex {
        pub fn new(pool: PgPool) -> Self {
            Self { pool }
        }
    }

    /// Fifteen columns, and the asset triple folded into ONE jsonb value —
    /// sqlx's tuple `FromRow` stops at 16, and a bounty row that cannot grow
    /// another column is a schema with a cliff in it (the lesson SPEND_COLS
    /// learned at 15).
    const BOUNTY_COLS: &str = "instance, bounty_id, child_id, status, value::text, \
                               paid_out::text, bond::text, curator, beneficiary, \
                               beneficiary_location, payment_id, account_id, \
                               first_seen_height, status_height, \
                               case when asset_location is null and asset_key is null \
                                     and asset_kind is null then null \
                                    else jsonb_build_object('location', asset_location, \
                                                            'key', asset_key, \
                                                            'kind', asset_kind) end";

    type BountyTuple = (
        String,
        i64,
        i64,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<Vec<u8>>,
        Option<Vec<u8>>,
        Option<serde_json::Value>,
        Option<String>,
        Option<Vec<u8>>,
        i64,
        i64,
        Option<serde_json::Value>,
    );

    fn bounty_from_row(
        (
            instance,
            bounty_id,
            child_id,
            status,
            value,
            paid_out,
            bond,
            curator,
            beneficiary,
            beneficiary_location,
            payment_id,
            account_id,
            first_seen_height,
            status_height,
            asset_ref,
        ): BountyTuple,
    ) -> super::BountyRow {
        super::BountyRow {
            instance,
            bounty_id: bounty_id as u64,
            // THE SENTINEL STOPS HERE. Every read path renders -1 back to null
            // (migration 0011); child 0 is a real child and must survive.
            child_id: (child_id >= 0).then_some(child_id as u64),
            status,
            value,
            paid_out,
            bond,
            curator: curator.map(|c| format!("0x{}", super::hex_lower(&c))),
            beneficiary: beneficiary.map(|b| format!("0x{}", super::hex_lower(&b))),
            beneficiary_location,
            payment_id,
            account_id: account_id.map(|a| format!("0x{}", super::hex_lower(&a))),
            first_seen_height: first_seen_height as u64,
            status_height: status_height as u64,
            asset_ref,
        }
    }

    /// `None` → the parent's sentinel. Bound as a plain i64 so the query can
    /// use `=` and hit the primary key, which an `is not distinct from` on a
    /// nullable column could not.
    ///
    /// Spelled here rather than imported: the api crate does not depend on any
    /// adapter (Invariant 4 — even the address parser is injected). A test
    /// asserts this equals `adapter_substrate::bounties::PARENT_SENTINEL`, so
    /// the two copies cannot drift apart in silence.
    fn child_key(child_id: Option<u64>) -> i64 {
        child_id.map_or(super::PARENT_SENTINEL, |c| c as i64)
    }

    #[async_trait]
    impl super::BountyIndex for PgBountyIndex {
        async fn bounties(
            &self,
            chain_id: &str,
            instance: &str,
            status: Option<&str>,
            limit: u64,
        ) -> Result<Vec<super::BountyRow>, IndexError> {
            // ordered exactly as MemoryBountyIndex orders, so `limit` returns
            // the same subset from either backend (the slice-6 defect)
            let rows: Vec<BountyTuple> = sqlx::query_as(&format!(
                "select {BOUNTY_COLS} from treasury.bounties \
                 where chain_id = $1 and instance = $2 \
                   and ($3::text is null or status = $3) \
                 order by bounty_id desc, child_id desc limit $4"
            ))
            .bind(chain_id)
            .bind(instance)
            .bind(status)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| IndexError(e.to_string()))?;
            Ok(rows.into_iter().map(bounty_from_row).collect())
        }

        async fn bounty(
            &self,
            chain_id: &str,
            instance: &str,
            bounty_id: u64,
            child_id: Option<u64>,
        ) -> Result<Option<super::BountyRow>, IndexError> {
            let row: Option<BountyTuple> = sqlx::query_as(&format!(
                "select {BOUNTY_COLS} from treasury.bounties \
                 where chain_id = $1 and instance = $2 and bounty_id = $3 and child_id = $4"
            ))
            .bind(chain_id)
            .bind(instance)
            .bind(bounty_id as i64)
            .bind(child_key(child_id))
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| IndexError(e.to_string()))?;
            Ok(row.map(bounty_from_row))
        }

        async fn bounty_events(
            &self,
            chain_id: &str,
            instance: &str,
            bounty_id: u64,
            child_id: Option<u64>,
        ) -> Result<Vec<super::BountyEventRow>, IndexError> {
            type Row = (
                i64,
                Option<DateTime<Utc>>,
                i32,
                String,
                Option<String>,
                serde_json::Value,
            );
            let rows: Vec<Row> = sqlx::query_as(
                "select e.block_height, b.timestamp, e.event_index, e.kind, e.amount::text, e.data \
                 from treasury.bounty_events e \
                 left join core.blocks b \
                   on b.chain_id = e.chain_id and b.height = e.block_height \
                 where e.chain_id = $1 and e.instance = $2 \
                   and e.bounty_id = $3 and e.child_id = $4 \
                 order by e.block_height, e.event_index",
            )
            .bind(chain_id)
            .bind(instance)
            .bind(bounty_id as i64)
            .bind(child_key(child_id))
            .fetch_all(&self.pool)
            .await
            .map_err(|e| IndexError(e.to_string()))?;
            Ok(rows
                .into_iter()
                .map(
                    |(height, timestamp, event_index, kind, amount, data)| {
                        super::BountyEventRow {
                            height: height as u64,
                            timestamp,
                            event_index: event_index as u32,
                            kind,
                            amount,
                            data,
                        }
                    },
                )
                .collect())
        }
    }

    /// `gov.vote_positions` columns, in the order `vote_from_row` expects.
    /// NUMERICs come back as text (plancks exceed u64/f64, no decimal dep).
    const VOTE_COLS: &str = "class, referendum_id, voter, active, vote_type, \
                             aye_balance::text, nay_balance::text, abstain_balance::text, \
                             conviction, conviction_label, aye_votes::text, nay_votes::text, \
                             support::text, status_height";

    type VoteTuple = (
        String,
        i64,
        Vec<u8>,
        bool,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i16>,
        Option<String>,
        String,
        String,
        String,
        i64,
    );

    fn vote_from_row(
        (
            class,
            referendum_id,
            voter,
            active,
            vote_type,
            aye_balance,
            nay_balance,
            abstain_balance,
            conviction,
            conviction_label,
            aye_votes,
            nay_votes,
            support,
            height,
        ): VoteTuple,
    ) -> super::VoteRow {
        super::VoteRow {
            class,
            referendum_id: referendum_id as u64,
            voter: format!("0x{}", super::hex_lower(&voter)),
            active,
            vote_type,
            aye_balance,
            nay_balance,
            abstain_balance,
            conviction: conviction.map(|c| c as u8),
            conviction_label,
            aye_votes,
            nay_votes,
            support,
            height: height as u64,
        }
    }
}

#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<Registry>,
    pub blocks: Arc<dyn BlockIndex>,
    pub labels: Arc<dyn LabelIndex>,
    pub balances: Arc<dyn BalanceIndex>,
    pub gov: Arc<dyn GovIndex>,
    pub treasury: Arc<dyn TreasuryIndex>,
    pub bounties: Arc<dyn BountyIndex>,
    pub assets: Arc<dyn AssetIndex>,
    pub sim: Arc<dyn SimIndex>,
    pub xcm: Arc<dyn XcmIndex>,
    pub parse_account: AccountParser,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/chains", get(list_chains))
        .route("/v1/blocks/{chain}/{height}", get(get_block))
        .route("/v1/accounts/{chain}/{account}/labels", get(get_labels))
        .route("/v1/balances/{network}/{account}/history", get(get_balance_history))
        .route("/v1/gov/{network}/referenda", get(list_gov_referenda))
        .route("/v1/gov/{network}/referenda/{id}", get(get_gov_referendum))
        .route("/v1/gov/{network}/referenda/{id}/votes", get(get_gov_referendum_votes))
        .route("/v1/gov/{network}/accounts/{account}/votes", get(get_gov_account_votes))
        .route("/v1/gov/{network}/tracks", get(get_gov_tracks))
        .route("/v1/gov/{network}/whitelist", get(list_gov_whitelist))
        .route("/v1/gov/{network}/whitelist/{hash}", get(get_gov_whitelisted_call))
        .route("/v1/treasury/{network}/spends", get(list_treasury_spends))
        .route("/v1/treasury/{network}/spends/{id}", get(get_treasury_spend))
        .route("/v1/treasury/{network}/pot", get(get_treasury_pot))
        .route("/v1/treasury/{network}/holdings", get(get_treasury_holdings))
        .route("/v1/bounties/{network}", get(list_bounties))
        .route("/v1/bounties/{network}/{id}", get(get_bounty))
        .route("/v1/assets/{chain}", get(list_assets))
        .route("/v1/sim/{chain}/calls/{call_hash}", get(get_simulations))
        .route("/v1/xcm/{chain}/messages", get(list_xcm_messages))
        .route("/v1/xcm/messages/{message_id}", get(get_xcm_message))
        .route("/v1/xcm/journeys/{message_id}", get(get_xcm_journey))
        .route("/v1/search", get(get_search))
        .route("/v1/domains/{network}/{domain}", get(resolve_domain))
        .with_state(state)
}

async fn health() -> &'static str {
    "ok"
}

async fn list_chains(State(state): State<AppState>) -> Json<serde_json::Value> {
    let now = Utc::now();
    let chains: Vec<serde_json::Value> = state
        .registry
        .chains()
        .map(|c| {
            serde_json::json!({
                "id": c.id,
                "name": c.name,
                "family": c.family,
                "network": c.network,
                "para_id": c.para_id,
                "relay": c.relay,
                "status": c.status_at(now),
                "modules": c.modules,
            })
        })
        .collect();
    Json(serde_json::json!({ "chains": chains }))
}

async fn get_block(
    State(state): State<AppState>,
    Path((chain, height)): Path<(String, u64)>,
) -> Response {
    if state.registry.chain(&chain).is_none() {
        return error(StatusCode::NOT_FOUND, format!("unknown chain: {chain}"));
    }
    match state.blocks.get(&chain, height).await {
        Ok(Some(block)) => Json(block).into_response(),
        Ok(None) => error(
            StatusCode::NOT_FOUND,
            format!("block {chain}/{height} not indexed"),
        ),
        Err(e) => error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

/// Account labels: `{account}` is SS58 or 0x-hex (adapter-injected parser).
/// The Phase 1 exit-criterion surface: system accounts appear NAMED here.
async fn get_labels(
    State(state): State<AppState>,
    Path((chain, account)): Path<(String, String)>,
) -> Response {
    if state.registry.chain(&chain).is_none() {
        return error(StatusCode::NOT_FOUND, format!("unknown chain: {chain}"));
    }
    let account_id = match (state.parse_account)(&account) {
        Ok(bytes) => bytes,
        Err(e) => return error(StatusCode::BAD_REQUEST, format!("bad account '{account}': {e}")),
    };
    match state.labels.labels_for(&chain, &account_id).await {
        Ok(labels) => Json(serde_json::json!({
            "chain": chain,
            "account_id": format!("0x{}", hex_lower(&account_id)),
            "labels": labels,
        }))
        .into_response(),
        Err(e) => error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

/// Tiny local hex (avoids a dep for one call site).
fn hex_lower(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[derive(Deserialize)]
struct AssetQuery {
    asset: Option<String>,
}

/// THE migration-boundary surface (Phase 1 exit criterion): one account's
/// balance history for a NETWORK, stitched across chains by domain residency.
/// Pre-2025-11-04 changes come from the relay, later ones from Asset Hub —
/// the caller never has to know the migration happened.
///
/// Running totals start from state anchors (balance read from System.Account
/// at a block, end-of-block semantics): each change after an anchor carries
/// `running_total`; changes with no preceding anchor carry null — coverage is
/// shown honestly, never guessed.
async fn get_balance_history(
    State(state): State<AppState>,
    Path((network, account)): Path<(String, String)>,
    Query(q): Query<AssetQuery>,
) -> Response {
    let asset = q.asset.unwrap_or_else(|| "native".to_string());
    let account_id = match (state.parse_account)(&account) {
        Ok(bytes) => bytes,
        Err(e) => return error(StatusCode::BAD_REQUEST, format!("bad account '{account}': {e}")),
    };
    let mut windows: Vec<&registry::ResidencyEntry> = state
        .registry
        .residency()
        .iter()
        .filter(|r| r.domain == "balances" && r.network == network)
        .collect();
    if windows.is_empty() {
        return error(
            StatusCode::NOT_FOUND,
            format!("no 'balances' domain residency for network '{network}'"),
        );
    }
    windows.sort_by_key(|r| r.from);

    let mut segments = Vec::with_capacity(windows.len());
    for w in windows {
        let changes = match state
            .balances
            .changes(&w.chain, &account_id, &asset, Some(w.from), w.to)
            .await
        {
            Ok(c) => c,
            Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
        let anchors = match state.balances.anchors(&w.chain, &account_id, &asset).await {
            Ok(a) => a,
            Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
        segments.push(serde_json::json!({
            "chain": w.chain,
            "from": w.from,
            "to": w.to,
            "anchors": anchors,
            "changes": changes_with_running_totals(&changes, &anchors),
        }));
    }
    Json(serde_json::json!({
        "network": network,
        "account_id": format!("0x{}", hex_lower(&account_id)),
        "asset": asset,
        "segments": segments,
    }))
    .into_response()
}

/// Attach running totals: anchors are end-of-block balances, so an anchor at
/// height H seeds the running value for changes at heights > H. Changes before
/// the first anchor get null (no anchor = no absolute truth to sum from).
fn changes_with_running_totals(
    changes: &[BalanceChangeRow],
    anchors: &[BalanceAnchorRow], // sorted ascending by height
) -> Vec<serde_json::Value> {
    let mut ai = 0usize;
    let mut running: Option<i128> = None;
    changes
        .iter()
        .map(|c| {
            while ai < anchors.len() && anchors[ai].height < c.height {
                running = anchors[ai].total.parse::<i128>().ok();
                ai += 1;
            }
            // a BalanceSet marker means the absolute value changed without a
            // derivable delta — running totals are unknowable until re-anchored
            if c.reason == "balance_set_unquantified" {
                running = None;
            }
            if let Some(r) = running.as_mut() {
                match c.delta.parse::<i128>() {
                    Ok(d) => *r += d,
                    Err(_) => running = None,
                }
            }
            serde_json::json!({
                "height": c.height,
                "timestamp": c.timestamp,
                "event_index": c.event_index,
                "delta": c.delta,
                "reason": c.reason,
                "counterparty": c.counterparty,
                "running_total": running.map(|r| r.to_string()),
            })
        })
        .collect()
}

// ---------------------------------------------------------------- gov routes

#[derive(Deserialize)]
struct GovQuery {
    /// Referenda instance; defaults to public OpenGov.
    class: Option<String>,
    limit: Option<u64>,
    /// RFC3339; tracks endpoint only — the migration-aware knob.
    at: Option<DateTime<Utc>>,
}

/// Residency windows carrying one referenda CLASS on a network, in time order.
/// Empty = nothing configured for it (404, not a guess).
///
/// The class → domain step is registry DATA (`referenda_classes` in the seeds),
/// so the public instance stitches relay → Asset Hub across the Nov-2025
/// migration while the fellowship instance resolves to Collectives — and no
/// caller ever names a chain (Invariant 2).
fn gov_windows<'a>(
    registry: &'a Registry,
    network: &str,
    class: &str,
) -> Vec<&'a registry::ResidencyEntry> {
    let domain = registry.domain_for_class(class);
    let mut windows: Vec<&registry::ResidencyEntry> = registry
        .residency()
        .iter()
        .filter(|r| r.domain == domain && r.network == network)
        .collect();
    windows.sort_by_key(|r| r.from);
    windows
}

/// Every chain hosting ANY registered referenda instance on this network — the
/// account surface, where a Fellow's votes live on a different chain from their
/// token votes.
///
/// Starts from the DEFAULT domain, so a residency seed that predates the
/// `referenda_classes` map (or a network whose file omits it) still serves the
/// public instance instead of 404-ing (reviewer catch: the class-scoped paths
/// have that fallback, this one silently did not).
///
/// Deduplicated by chain. That means if two classes ever share a chain, the
/// segment carries the FIRST class's window bounds — fine while the public and
/// fellowship instances live apart, worth revisiting if they converge.
pub(crate) fn all_gov_windows<'a>(
    registry: &'a Registry,
    network: &str,
) -> Vec<&'a registry::ResidencyEntry> {
    // default instance first, then every registered one (deduped)
    let mut classes: Vec<&str> = vec![registry::DEFAULT_REFERENDA_CLASS];
    for c in registry.referenda_classes() {
        if !classes.contains(&c.class.as_str()) {
            classes.push(&c.class);
        }
    }
    let mut seen: Vec<&str> = Vec::new();
    let mut windows: Vec<&'a registry::ResidencyEntry> = Vec::new();
    for class in classes {
        for w in gov_windows(registry, network, class) {
            if !seen.contains(&w.chain.as_str()) {
                seen.push(&w.chain);
                windows.push(w);
            }
        }
    }
    windows
}

/// Merge a later residency window's projection over an earlier one:
/// status comes from the later window UNLESS it only saw info events
/// ('unknown' — e.g. a post-migration deposit refund for a relay-decided
/// referendum must not erase the relay's terminal status); info fields
/// coalesce; submission is the earliest observation.
fn merge_referendum(prev: ReferendumRow, later: ReferendumRow) -> ReferendumRow {
    let (status, status_height) = if later.status == "unknown" {
        (prev.status, prev.status_height)
    } else {
        (later.status, later.status_height)
    };
    ReferendumRow {
        class: later.class,
        referendum_id: later.referendum_id,
        track_id: later.track_id.or(prev.track_id),
        status,
        status_height,
        proposal: later.proposal.or(prev.proposal),
        proposal_hash: later.proposal_hash.or(prev.proposal_hash),
        proposal_len: later.proposal_len.or(prev.proposal_len),
        submitted_at_height: prev.submitted_at_height.or(later.submitted_at_height),
    }
}

#[derive(serde::Deserialize)]
struct SearchQuery {
    q: Option<String>,
}

/// THE resolver: given an arbitrary blob a user pasted, say what it is.
///
/// One endpoint, one grammar, one response shape — the omnibox is a CLIENT of
/// this, not a separate path, which is why the payload carries kind/chain/why
/// per candidate rather than a display string (ROADMAP §Phase 2).
///
/// A parse failure is a 400 with what WOULD have worked; zero candidates is a
/// 200 with an empty list, because "nothing indexed matches this" and "this
/// input made no sense" are different answers and a caller must be able to tell
/// them apart.
async fn get_search(State(state): State<AppState>, Query(q): Query<SearchQuery>) -> Response {
    let raw = q.q.unwrap_or_default();
    match search::parse(&raw, &state.registry) {
        Ok(query) => {
            let body = search::resolve(&query, &state, &raw).await;
            Json(body).into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "query": raw,
                "error": e.message,
                "expected": e.expected,
            })),
        )
            .into_response(),
    }
}

/// Normalize a user-supplied call hash to the form the tables store: 0x-prefixed
/// lowercase. A pasted hash from a UI arrives in either case and sometimes bare.
fn normalize_call_hash(raw: &str) -> String {
    // lower-case BEFORE stripping: `to_uppercase()` on a hash yields `0X…`,
    // and a case-sensitive strip would leave the prefix in place and return
    // `0x0x…`, which no lookup can ever match.
    let lower = raw.trim().to_ascii_lowercase();
    format!("0x{}", lower.strip_prefix("0x").unwrap_or(&lower))
}

/// What this endpoint deliberately does NOT claim, stated in the payload
/// rather than in a doc nobody reads — the `not_covered` contract the holdings
/// endpoint established.
///
/// THE STITCH TO THE TWO REFERENDA IS NOT HERE, and the reason is a decoder
/// fact rather than an oversight. Both directions were designed for this slice
/// and both turn out to be unresolvable by any search over `gov.preimages`:
///
///   * BACKWARD (which Fellowship referendum authorized this hash): the
///     Fellowship votes on Collectives and the decision reaches Asset Hub by
///     XCM. `whitelist.whitelist_call(hash)` therefore travels inside an XCM
///     `Transact`, whose payload is `DoubleEncoded<Call>` — a `Vec<u8>`.
///     `calls.rs` detects nested calls by matching a node's type id against the
///     runtime's `RuntimeCall` type, and a byte vector is not that type, so the
///     decoder correctly renders the inner call as opaque bytes and does not
///     recurse. There is no `whitelist.whitelist_call` node to find.
///
///   * FORWARD (which public referendum dispatched it): the track-1 referendum's
///     preimage decodes to `whitelist.dispatch_whitelisted_call_with_preimage
///     { call }`, which carries the CALL and no hash at all — the pallet derives
///     the hash by re-encoding and hashing it. Confirming the match therefore
///     needs a re-encode we cannot do from decoded JSON.
///
/// A text search would fail on both counts anyway: `call_node` renders a
/// `T::Hash` argument as a byte ARRAY, not as hex, so `decoded_call::text like
/// '%<hex>%'` matches nothing. Guessing a link here would produce an endpoint
/// that silently returns nothing and looks like an absence of whitelist
/// activity. It gets its own slice, designed against real decoded rows.
fn whitelist_not_covered() -> serde_json::Value {
    serde_json::json!([
        "the Fellowship referendum that authorized this hash is not linked: it lives on \
         Collectives and reaches this chain inside an XCM Transact, whose payload decodes \
         as opaque bytes rather than as a nested call",
        "the public referendum that dispatched this hash is not linked: \
         dispatch_whitelisted_call_with_preimage carries the call, not its hash, so the \
         match needs a re-encode that decoded JSON cannot supply",
        "a whitelisted call that was never dispatched may have failed with \
         UnavailablePreImage, UndecodableCall or InvalidCallWeightWitness — all three fail \
         the extrinsic with NO event, so this endpoint cannot distinguish them from a call \
         still awaiting its referendum",
        "authorized_call is null unless someone fetched that preimage by hand: \
         decode-preimages walks gov.referenda, and a whitelisted call hash is never \
         a referendum's proposal hash, so nothing populates it automatically yet"
    ])
}

/// One whitelisted call: its status, whether its dispatch actually WORKED, its
/// full event history, and — the answer that matters — what the Fellowship
/// authorized, joined from `gov.preimages` by the same hash.
///
/// The join itself is exact rather than heuristic — `whitelist_call` REQUESTS
/// the preimage of the hash it whitelists, so a whitelisted call hash IS a
/// preimage hash and `gov.preimages` is keyed by hash. But it is NOT populated
/// automatically, and pretending otherwise would be the slice-6 dead-join
/// defect again: `decode-preimages` walks `gov.referenda`, so it only ever
/// fetches hashes that are some REFERENDUM's proposal hash, and a whitelisted
/// call hash never is. Until a later slice walks `gov.whitelisted_calls` too,
/// this field is populated only for hashes someone fetched by hand — and
/// `fetch-preimage` needs a `len` that `CallWhitelisted` does not carry
/// (recoverable from `preimage.StatusFor`, which is keyed by hash alone).
/// Stated in `not_covered` rather than left to look like an absence of data.
async fn get_gov_whitelisted_call(
    State(state): State<AppState>,
    Path((network, hash)): Path<(String, String)>,
) -> Response {
    // EVERY governance chain, not just the default class's — the follower is
    // gated on the `governance` module, so it indexes Collectives too, and
    // walking only the `referenda` residency would write rows that can never
    // be read back.
    let windows = all_gov_windows(&state.registry, &network);
    if windows.is_empty() {
        return error(
            StatusCode::NOT_FOUND,
            format!("no governance residency for network '{network}'"),
        );
    }
    let call_hash = normalize_call_hash(&hash);

    let mut merged: Option<WhitelistedCallRow> = None;
    let mut preimage: Option<PreimageRow> = None;
    let mut segments = Vec::with_capacity(windows.len());
    for w in windows {
        let row = match state.gov.whitelisted_call(&w.chain, &call_hash).await {
            Ok(r) => r,
            Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
        let events = match state.gov.whitelist_events(&w.chain, &call_hash).await {
            Ok(ev) => ev,
            Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
        // A later residency window wins outright — unlike a referendum, a
        // whitelisted call has no fields to coalesce, and the same hash
        // whitelisted on the relay before the migration and again on Asset Hub
        // after is two separate authorizations, not one split record. The
        // per-chain segments below keep both visible.
        if let Some(r) = row {
            merged = Some(r);
        }
        if preimage.is_none() {
            preimage = match state.gov.preimage(&w.chain, &call_hash).await {
                Ok(p) => p.filter(|p| p.decode_status == "decoded"),
                Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            };
        }
        segments.push(serde_json::json!({
            "chain": w.chain,
            "from": w.from,
            "to": w.to,
            "events": events,
        }));
    }

    let Some(call) = merged else {
        return error(
            StatusCode::NOT_FOUND,
            format!("whitelisted call {network}/{call_hash} not indexed"),
        );
    };

    // Say what the two flags mean, in the payload, because "dispatched" reads
    // like success to anyone who has not read pallet-whitelist.
    let enacted = match (call.status.as_str(), call.dispatch_ok) {
        ("dispatched", Some(true)) => "the whitelisted call was dispatched and SUCCEEDED",
        ("dispatched", Some(false)) => {
            "the whitelisted call was dispatched and FAILED — pallet-whitelist swallows the \
             error, so the extrinsic still succeeded, the whitelist entry was still consumed \
             and the fee was still charged, but the call did not take effect"
        }
        ("dispatched", None) => {
            "dispatched, but the result could not be read — this should be unreachable, \
             because an unreadable result halts the mapper"
        }
        ("removed", None) => "the authorization was removed before it was ever dispatched",
        ("removed", Some(_)) => {
            "removed — but an EARLIER dispatch of this same hash is on record below; a hash \
             can be whitelisted, dispatched and whitelisted again, and the verdict shown is \
             that earlier attempt's"
        }
        _ => {
            "whitelisted and not yet dispatched — a legitimate terminal state, not \
             necessarily a pending one (see coverage.not_covered)"
        }
    };

    Json(serde_json::json!({
        "network": network,
        "call_hash": call.call_hash,
        "call": call,
        "enacted": enacted,
        "authorized_call": preimage,
        "segments": segments,
        "coverage": { "not_covered": whitelist_not_covered() },
    }))
    .into_response()
}

/// Recent whitelisted calls for a network, residency-stitched.
async fn list_gov_whitelist(
    State(state): State<AppState>,
    Path(network): Path<String>,
    Query(q): Query<GovQuery>,
) -> Response {
    let windows = all_gov_windows(&state.registry, &network);
    if windows.is_empty() {
        return error(
            StatusCode::NOT_FOUND,
            format!("no governance residency for network '{network}'"),
        );
    }
    let limit = q.limit.unwrap_or(50).min(500);

    let mut segments = Vec::with_capacity(windows.len());
    for w in windows {
        let calls = match state.gov.list_whitelisted_calls(&w.chain, limit).await {
            Ok(c) => c,
            Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
        segments.push(serde_json::json!({
            "chain": w.chain,
            "from": w.from,
            "to": w.to,
            "calls": calls,
        }));
    }
    Json(serde_json::json!({
        "network": network,
        "limit": limit,
        "segments": segments,
        "coverage": { "not_covered": whitelist_not_covered() },
    }))
    .into_response()
}

/// THE Phase 2 governance surface: one referendum's full story for a NETWORK,
/// stitched across chains by governance domain residency. A referendum
/// submitted on the relay and concluded on Asset Hub renders as ONE timeline —
/// the caller never has to know the migration happened (ARCHITECTURE §4).
async fn get_gov_referendum(
    State(state): State<AppState>,
    Path((network, id)): Path<(String, u64)>,
    Query(q): Query<GovQuery>,
) -> Response {
    let class = q
        .class
        .unwrap_or_else(|| registry::DEFAULT_REFERENDA_CLASS.to_string());
    let windows = gov_windows(&state.registry, &network, &class);
    if windows.is_empty() {
        return error(
            StatusCode::NOT_FOUND,
            format!(
                "no residency for referenda class '{class}' (domain '{}') on network '{network}'",
                state.registry.domain_for_class(&class)
            ),
        );
    }

    let mut merged: Option<ReferendumRow> = None;
    let mut segments = Vec::with_capacity(windows.len());
    for w in windows {
        let summary = match state.gov.referendum(&w.chain, &class, id).await {
            Ok(s) => s,
            Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
        let events = match state.gov.referendum_events(&w.chain, &class, id).await {
            Ok(ev) => ev,
            Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
        if let Some(s) = summary {
            merged = Some(match merged.take() {
                None => s,
                Some(prev) => merge_referendum(prev, s),
            });
        }
        segments.push(serde_json::json!({
            "chain": w.chain,
            "from": w.from,
            "to": w.to,
            "events": events,
        }));
    }
    let Some(referendum) = merged else {
        return error(
            StatusCode::NOT_FOUND,
            format!("referendum {network}/{class}/{id} not indexed"),
        );
    };

    // attach the decoded proposal (the "what does it DO" answer): first
    // decoded row across the residency chains wins; a relay-era referendum's
    // preimage may only exist on the relay row, a post-migration one on AH
    let mut preimage: Option<PreimageRow> = None;
    if let Some(hash) = &referendum.proposal_hash {
        for w in gov_windows(&state.registry, &network, &class) {
            match state.gov.preimage(&w.chain, hash).await {
                Ok(Some(p)) => {
                    let decoded = p.decode_status == "decoded";
                    if preimage.is_none() || decoded {
                        preimage = Some(p);
                    }
                    if decoded {
                        break;
                    }
                }
                Ok(None) => {}
                Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            }
        }
    }

    // …and any Tier 1 previews of it. THIS IS THE KILLER FLOW (PRODUCT.md gap
    // 4): the same page that says what a referendum DOES can now say what it
    // WOULD DO. Recorded runs only — rendering never triggers one — and they are
    // gathered across the same residency chains, since a call previewed on the
    // relay and on Asset Hub are two different answers.
    const SIM_PER_WINDOW: u32 = 10;
    let mut simulations: Vec<SimulationRow> = Vec::new();
    let mut sim_truncated = false;
    if let Some(hash) = &referendum.proposal_hash {
        for w in gov_windows(&state.registry, &network, &class) {
            match state.sim.simulations(&w.chain, hash, SIM_PER_WINDOW).await {
                Ok(rows) => {
                    sim_truncated |= rows.len() as u32 == SIM_PER_WINDOW;
                    simulations.extend(rows);
                }
                Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            }
        }
    }
    // Sort the MERGED list, not each window: concatenating windows leaves the
    // array grouped by chain, so `simulations[0]` would be the oldest chain's
    // newest run rather than the newest run. Same ordering the index promises.
    simulations.sort_by(|a, b| {
        b.at_height
            .cmp(&a.at_height)
            .then_with(|| a.input_hash.cmp(&b.input_hash))
            .then_with(|| a.at_block_hash.cmp(&b.at_block_hash))
            .then_with(|| a.tier.cmp(&b.tier))
    });

    // Three different situations that all produce an empty list, and they are
    // NOT the same claim. Saying "nobody previewed this" when we never had a
    // hash to look one up by would be asserting something we did not check.
    let sim_coverage = match (&referendum.proposal_hash, simulations.is_empty()) {
        (None, _) => serde_json::json!({
            "recorded_only": "this referendum has no proposal hash indexed yet, so no \
                              simulation could be looked up at all — run `decode-preimages` \
                              for this chain first (an Inline proposal gets its hash filled \
                              in there)",
            "not_covered": sim_not_covered(),
        }),
        (Some(_), true) => serde_json::json!({
            "recorded_only": "no Tier 1 preview has been run for this proposal; that is an \
                              absence of simulations, not a claim about the call",
            "not_covered": sim_not_covered(),
        }),
        (Some(_), false) => serde_json::json!({
            "truncated": sim_truncated,
            "not_covered": sim_not_covered(),
        }),
    };

    Json(serde_json::json!({
        "network": network,
        "class": class,
        "referendum_id": id,
        "referendum": referendum,
        "preimage": preimage,
        "simulations": simulations,
        "simulation_coverage": sim_coverage,
        "segments": segments,
    }))
    .into_response()
}

#[derive(Deserialize)]
struct SimQuery {
    limit: Option<u64>,
}

/// Recent XCM observations on one chain.
async fn list_xcm_messages(
    State(state): State<AppState>,
    Path(chain): Path<String>,
    Query(q): Query<SimQuery>,
) -> Response {
    if state.registry.chain(&chain).is_none() {
        return error(StatusCode::NOT_FOUND, format!("unknown chain '{chain}'"));
    }
    let limit = q.limit.unwrap_or(25).clamp(1, 200) as u32;
    let rows = match state.xcm.messages(&chain, limit).await {
        Ok(r) => r,
        Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    Json(serde_json::json!({
        "chain": chain,
        "messages": rows,
        "coverage": { "not_covered": xcm_not_covered() },
    }))
    .into_response()
}

/// Every observation carrying one id, on any chain.
///
/// THE SHAPE OF THIS RESPONSE IS THE POINT. It returns the halves we have and
/// labels each one's `id_kind`; it does NOT return a journey. Two rows here —
/// one `sent` on Asset Hub, one `received` on Hydration — are what a journey is
/// made of, and saying so is the correlation slice's job, not this endpoint's.
/// Until then the honest answer to "what happened to this message" is "here is
/// every place that id was seen".
async fn get_xcm_message(
    State(state): State<AppState>,
    Path(message_id): Path<String>,
) -> Response {
    let id = normalize_call_hash(&message_id);
    let rows = match state.xcm.by_message_id(&id).await {
        Ok(r) => r,
        Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    let sides: Vec<&str> = rows.iter().map(|r| r.side.as_str()).collect();
    let reads_as = match (
        sides.contains(&"sent"),
        sides.contains(&"received"),
        rows.is_empty(),
    ) {
        (_, _, true) => "no chain we index has seen this id — which is not the same as the \
                         message not existing, since a journey through an unindexed chain \
                         leaves no row here",
        (true, true, _) => "both halves are on record: one chain reported sending this id and \
                            another reported processing it. That is strong evidence of one \
                            message and is still not an assertion — see coverage.not_covered",
        (true, false, _) => "only the SENDING half is on record. That can mean the message is \
                             still in flight, that the receiving chain is not indexed here, or \
                             that it was dropped in transit — the three are indistinguishable \
                             from this side",
        (false, true, _) => "only the RECEIVING half is on record. The sending chain is either \
                             not indexed here or does not emit a sender event for forwarded \
                             messages",
        _ => "observations recorded, but neither a send nor a receive among them",
    };
    Json(serde_json::json!({
        "message_id": id,
        "observations": rows,
        "reads_as": reads_as,
        // The correlated view of the same id. Named here because this endpoint
        // deliberately does NOT expand aliases: asked for an id, it answers
        // about that id, and a wire hash whose journey is on record under the
        // topic will still return one row.
        "journey": format!("/v1/xcm/journeys/{id}"),
        "coverage": { "not_covered": xcm_not_covered() },
    }))
    .into_response()
}

/// How far an id's alias set is allowed to grow. A link is intra-block, so a
/// legitimate multi-hop journey adds two ids per hop; sixteen is four hops of
/// headroom and a hard bound on the fan-out, which is the same discipline
/// `api::search` holds itself to.
const XCM_ALIAS_LIMIT: usize = 16;

/// ONE MESSAGE, EVERY CHAIN THAT SAW IT — the correlation layer's read side
/// (Phase 3, slice 3).
///
/// This is the endpoint slice 2 refused to write, and what changed is not
/// confidence but MACHINERY: the stitch is now two mechanical rules with their
/// evidence attached, plus three checks that can come back `contradicted` in
/// public. `/v1/xcm/messages/{id}` still answers "where was this id seen"; this
/// answers "what happened", and shows its working.
///
/// THE ONE THING THAT MADE IT POSSIBLE is the alias expansion. A sender emits
/// two ids for one message and the receiver reports whichever one it got, so a
/// user pasting a wire hash and a user pasting a topic are asking about the same
/// journey and slice 2 could only answer one of them. `xcm.message_links` closes
/// that, and `aliases` in the response shows exactly which links were used and
/// on what evidence — a stitch you cannot audit is a stitch you cannot trust.
async fn get_xcm_journey(
    State(state): State<AppState>,
    Path(message_id): Path<String>,
) -> Response {
    let asked = normalize_call_hash(&message_id);

    // Expand to a fixpoint rather than one hop: starting from a wire hash, one
    // hop reaches its topic, and only a second reaches the NEXT chain's wire
    // hash when that chain re-emitted the same topic. Bounded by
    // XCM_ALIAS_LIMIT, and the bound is reported rather than hidden.
    let mut ids: Vec<String> = vec![asked.clone()];
    let mut links: Vec<XcmLinkRow> = Vec::new();
    let mut probed: usize = 0;
    let mut truncated = false;
    while probed < ids.len() {
        let id = ids[probed].clone();
        probed += 1;
        let found = match state.xcm.aliases(&id).await {
            Ok(l) => l,
            Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
        for l in found {
            for candidate in [&l.wire_hash, &l.topic] {
                if ids.iter().any(|i| i == candidate) {
                    continue;
                }
                // A dropped candidate is REPORTED, not silently forgotten: a
                // journey assembled from a truncated id set is a partial answer
                // and the caller has to be able to tell.
                if ids.len() >= XCM_ALIAS_LIMIT {
                    truncated = true;
                    continue;
                }
                ids.push(candidate.clone());
            }
            if !links.iter().any(|k| {
                (k.chain_id.as_str(), k.block_height, k.wire_event_index)
                    == (l.chain_id.as_str(), l.block_height, l.wire_event_index)
            }) {
                links.push(l);
            }
        }
    }

    let mut rows = match state.xcm.by_message_ids(&ids).await {
        Ok(r) => r,
        Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    // Block timestamp is the ONLY ordering two chains share. `is_none()` first
    // in the key puts undated steps LAST (Option's own Ord would put them
    // first), where they read as "we could not place this" rather than as the
    // start of the journey.
    rows.sort_by(|a, b| {
        (a.timestamp.is_none(), a.timestamp, &a.chain_id, a.block_height, a.event_index).cmp(
            &(b.timestamp.is_none(), b.timestamp, &b.chain_id, b.block_height, b.event_index),
        )
    });

    let sends: Vec<&XcmMessageRow> = rows.iter().filter(|r| r.side == "sent").collect();
    let receives: Vec<&XcmMessageRow> = rows.iter().filter(|r| r.side == "received").collect();
    let locals: Vec<&XcmMessageRow> = rows.iter().filter(|r| r.side == "local").collect();

    let shape = match (rows.is_empty(), sends.is_empty(), receives.is_empty()) {
        (true, _, _) => "unseen",
        (_, false, false) => "send_and_receive",
        (_, false, true) => "send_only",
        (_, true, false) => "receive_only",
        _ if !locals.is_empty() => "local_only",
        _ => "observations_only",
    };

    let reads_as = match shape {
        "unseen" => "no chain we index has seen this id, on either side. That is not the same \
                     as the message not existing: a journey through a chain we do not map \
                     leaves no row here"
            .to_string(),
        "send_and_receive" => format!(
            "one message, stitched across {} chain(s): {} sending observation(s) and {} \
             receiving one(s) carrying the same id. The stitch is id equality plus {} recorded \
             alias link(s) — see checks and aliases for what corroborates it",
            distinct_chains(&rows),
            sends.len(),
            receives.len(),
            links.len()
        ),
        "send_only" => "only the SENDING half is on record. The message may still be in flight, \
                        the receiving chain may not be indexed here, or it may have been \
                        dropped in transit — the three are indistinguishable from this side"
            .to_string(),
        "receive_only" => "only the RECEIVING half is on record. The sending chain is either \
                           not indexed here or emits no sender event for forwarded messages, \
                           which is what a chain with no XcmEventEmitter does"
            .to_string(),
        "local_only" => "this id names a LOCAL execution (pallet_xcm.execute) — an XCM this \
                         chain ran on itself. It has no counterparty and no journey by \
                         construction"
            .to_string(),
        _ => "observations recorded, but neither a send nor a receive among them".to_string(),
    };

    // ------------------------------------------------------------ the checks
    let latest_send = sends.iter().filter_map(|r| r.timestamp).max();
    let earliest_receive = receives.iter().filter_map(|r| r.timestamp).min();
    let time_order = match (latest_send, earliest_receive) {
        (Some(s), Some(r)) if r >= s => serde_json::json!({
            "status": "ok",
            "note": "every receiving observation is at or after the last sending one, which is \
                     what a real journey looks like",
            "sent_at": s, "received_at": r,
        }),
        (Some(s), Some(r)) => serde_json::json!({
            "status": "contradicted",
            "note": "a receiving observation PRECEDES the sending one. These are almost \
                     certainly two different messages that share an id — the likeliest cause \
                     is a wire hash colliding on identical bytes — and this journey should not \
                     be read as one operation",
            "sent_at": s, "received_at": r,
        }),
        _ => serde_json::json!({
            "status": "unknown",
            "note": "not both halves carry a block timestamp (core.blocks.timestamp is \
                     nullable), so nothing here orders them",
        }),
    };

    let mut mirror_pairs = Vec::new();
    let mut mirror_status = "unknown";
    for s in &sends {
        for r in &receives {
            if s.chain_id == r.chain_id {
                continue;
            }
            let (Some(from), Some(to)) = (
                state.registry.chain(&s.chain_id),
                state.registry.chain(&r.chain_id),
            ) else {
                continue;
            };
            let expect_sender_says = xcm_counterparty_name(from, to);
            let expect_receiver_says = xcm_counterparty_name(to, from);
            let verdict = match (
                &expect_sender_says,
                &s.counterparty,
                &expect_receiver_says,
                &r.counterparty,
            ) {
                (Some(es), Some(gs), Some(er), Some(gr)) if es == gs && er == gr => "corroborated",
                (Some(es), Some(gs), _, _) if es != gs => "contradicted",
                (_, _, Some(er), Some(gr)) if er != gr => "contradicted",
                _ => "unknown",
            };
            if verdict == "contradicted" || (verdict == "corroborated" && mirror_status != "contradicted")
            {
                mirror_status = verdict;
            }
            mirror_pairs.push(serde_json::json!({
                "from": s.chain_id, "to": r.chain_id, "verdict": verdict,
                "sender_says": s.counterparty, "sender_should_say": expect_sender_says,
                "receiver_says": r.counterparty, "receiver_should_say": expect_receiver_says,
            }));
        }
    }

    // Same chain, same side, same id, two different blocks: one id naming two
    // messages. Two different SENDING chains is NOT flagged — that is exactly
    // what a multi-hop with a propagated topic looks like.
    let mut contested = Vec::new();
    for r in &rows {
        let Some(id) = &r.message_id else { continue };
        let twins = rows
            .iter()
            .filter(|o| {
                o.message_id.as_ref() == Some(id)
                    && o.chain_id == r.chain_id
                    && o.side == r.side
                    && (o.block_height, o.event_index) != (r.block_height, r.event_index)
            })
            .count();
        if twins > 0 && !contested.contains(&(r.chain_id.clone(), r.side.clone(), id.clone())) {
            contested.push((r.chain_id.clone(), r.side.clone(), id.clone()));
        }
    }

    let steps: Vec<serde_json::Value> = rows
        .iter()
        .enumerate()
        .map(|(seq, r)| {
            serde_json::json!({
                "seq": seq,
                "chain": r.chain_id,
                "block_height": r.block_height,
                "event_index": r.event_index,
                "timestamp": r.timestamp,
                "side": r.side,
                "transport": r.transport,
                "counterparty": r.counterparty,
                "message_id": r.message_id,
                "id_kind": r.id_kind,
                "status": r.status,
                "success": r.success,
                "error": r.error,
                "forwarded": r.forwarded,
                "lineage": {
                    "runtime_version": r.runtime_version,
                    "mapper_version": r.mapper_version,
                },
            })
        })
        .collect();

    // The receiving side's verdict, said in the pallet's own terms rather than
    // ours — `success: true` is a claim about the QUEUE, never about intent.
    let outcome = receives
        .iter()
        .map(|r| r.success)
        .reduce(|a, b| match (a, b) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(true), Some(true)) => Some(true),
            _ => None,
        })
        .flatten()
        .map(|ok| if ok {
            serde_json::json!({
                "delivered": true, "receiver_success": true,
                "note": "the receiving chain's message queue treated this as handled and \
                         discarded it. pallet-message-queue's own doc says that is ALL it \
                         means — it is not a claim that the XCM achieved its intent",
            })
        } else {
            serde_json::json!({
                "delivered": true, "receiver_success": false,
                "note": "the message ARRIVED and its execution did not complete \
                         (Outcome::Incomplete). Delivery and intent are different facts and \
                         this journey separates them",
            })
        });

    Json(serde_json::json!({
        "message_id": asked,
        "shape": shape,
        "reads_as": reads_as,
        // Every id this journey was assembled from, the first being the one
        // asked for. A caller can re-derive the whole answer from these.
        "ids": ids,
        "alias_limit_reached": truncated,
        "aliases": links,
        "chains": distinct_chains(&rows),
        "steps": steps,
        "outcome": outcome,
        "checks": {
            "time_order": time_order,
            "counterparty_mirror": {
                "status": if mirror_pairs.is_empty() { "unknown" } else { mirror_status },
                "note": "the receiving chain names the QUEUE a message came from and the \
                         sending chain names where it addressed one; on a real journey the two \
                         are mirror images, derived here from registry para ids alone",
                "pairs": mirror_pairs,
            },
            "id_uniqueness": {
                "status": if contested.is_empty() { "ok" } else { "contested" },
                "note": "one id naming two messages on one chain and side. A topic cannot \
                         collide (frame_system::unique mixes intrablock entropy); a wire hash \
                         is a hash of content and can",
                "contested": contested
                    .iter()
                    .map(|(c, s, i)| serde_json::json!({"chain": c, "side": s, "message_id": i}))
                    .collect::<Vec<_>>(),
            },
        },
        "coverage": { "not_covered": xcm_journey_not_covered() },
    }))
    .into_response()
}

fn distinct_chains(rows: &[XcmMessageRow]) -> usize {
    let mut seen: Vec<&str> = Vec::new();
    for r in rows {
        if !seen.contains(&r.chain_id.as_str()) {
            seen.push(&r.chain_id);
        }
    }
    seen.len()
}

/// Recorded Tier 1 simulations of one call, on one chain.
///
/// CHAIN-SCOPED, not network-scoped, and unlike almost everything else here
/// that is not an oversight: a simulation is an answer a PARTICULAR runtime gave
/// about a PARTICULAR state, so the chain is part of the question rather than
/// something to resolve away. The same call previewed on the relay and on Asset
/// Hub is two different answers and both are worth having.
async fn get_simulations(
    State(state): State<AppState>,
    Path((chain, call_hash)): Path<(String, String)>,
    Query(q): Query<SimQuery>,
) -> Response {
    if state.registry.chain(&chain).is_none() {
        return error(
            StatusCode::NOT_FOUND,
            format!("unknown chain '{chain}'"),
        );
    }
    let hash = normalize_call_hash(&call_hash);
    // clamped at 1, not 0: `?limit=0` would return an empty list under a
    // coverage note that says "nobody has previewed this call", which would be
    // this endpoint stating something false about the data on the caller's own
    // instruction
    let limit = q.limit.unwrap_or(10).clamp(1, 100) as u32;
    let rows = match state.sim.simulations(&chain, &hash, limit).await {
        Ok(r) => r,
        Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    Json(serde_json::json!({
        "chain": chain,
        "call_hash": hash,
        "simulations": rows,
        "coverage": {
            "recorded_only": "this endpoint serves simulations that were RUN; it never \
                              starts one. An empty list means nobody has previewed this \
                              call at any state, not that the call does nothing",
            "not_covered": sim_not_covered(),
        },
    }))
    .into_response()
}

/// Latest referenda for a network, residency-merged (a referendum with rows on
/// both sides of the migration appears once, with its stitched summary).
async fn list_gov_referenda(
    State(state): State<AppState>,
    Path(network): Path<String>,
    Query(q): Query<GovQuery>,
) -> Response {
    let class = q
        .class
        .unwrap_or_else(|| registry::DEFAULT_REFERENDA_CLASS.to_string());
    let limit = q.limit.unwrap_or(25).min(200);
    let windows = gov_windows(&state.registry, &network, &class);
    if windows.is_empty() {
        return error(
            StatusCode::NOT_FOUND,
            format!(
                "no residency for referenda class '{class}' (domain '{}') on network '{network}'",
                state.registry.domain_for_class(&class)
            ),
        );
    }
    let mut by_id: std::collections::BTreeMap<u64, ReferendumRow> = std::collections::BTreeMap::new();
    for w in &windows {
        let rows = match state.gov.list_referenda(&w.chain, &class, limit).await {
            Ok(r) => r,
            Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
        for row in rows {
            let id = row.referendum_id;
            let merged = match by_id.remove(&id) {
                None => row,
                Some(prev) => merge_referendum(prev, row),
            };
            by_id.insert(id, merged);
        }
    }
    let referenda: Vec<&ReferendumRow> = by_id.values().rev().take(limit as usize).collect();
    Json(serde_json::json!({
        "network": network,
        "class": class,
        "referenda": referenda,
    }))
    .into_response()
}

/// Sum a column of decimal-string plancks. Overflow-free: the sum of every
/// vote on a referendum still fits u128 (total issuance is ~10^19 plancks).
fn sum_decimal(values: impl Iterator<Item = String>) -> String {
    values
        .filter_map(|v| v.parse::<u128>().ok())
        .fold(0u128, |a, b| a.saturating_add(b))
        .to_string()
}

/// WHO decided this referendum: current vote positions, residency-stitched,
/// with a direct-vote tally.
///
/// One voter can hold a position row on BOTH sides of the Nov-2025 migration
/// for the same referendum (voted on the relay, changed or withdrew on Asset
/// Hub) — the relay row then stays `active` forever, because no further relay
/// event will ever touch it. So positions are MERGED per voter, latest
/// residency window winning, exactly like `merge_referendum` does for the
/// summary (reviewer catch: without this the tally double-counts and resurrects
/// withdrawn votes).
///
/// COVERAGE IS EXPLICIT. `direct_tally` describes the merged rows below it and
/// nothing else:
///   - it does NOT match the on-chain tally when delegated power is involved —
///     no pallet event carries a delegation's weight (see
///     `/v1/gov/{network}/accounts/{account}/votes` for the state anchors);
///   - `truncated` says whether `limit` cut the vote list short;
///   - a segment with `votes_indexed: 0` on a chain that hosted governance
///     before the poll-index era is an honest gap, not "nobody voted".
async fn get_gov_referendum_votes(
    State(state): State<AppState>,
    Path((network, id)): Path<(String, u64)>,
    Query(q): Query<GovQuery>,
) -> Response {
    let class = q
        .class
        .unwrap_or_else(|| registry::DEFAULT_REFERENDA_CLASS.to_string());
    let limit = q.limit.unwrap_or(100).min(1000);
    let windows = gov_windows(&state.registry, &network, &class);
    if windows.is_empty() {
        return error(
            StatusCode::NOT_FOUND,
            format!(
                "no residency for referenda class '{class}' (domain '{}') on network '{network}'",
                state.registry.domain_for_class(&class)
            ),
        );
    }

    // windows are time-ordered, so a later insert overwrites an earlier one
    let mut merged: std::collections::BTreeMap<String, VoteRow> =
        std::collections::BTreeMap::new();
    let mut segments = Vec::with_capacity(windows.len());
    let mut truncated = false;
    for w in &windows {
        let votes = match state.gov.referendum_votes(&w.chain, &class, id, limit).await {
            Ok(v) => v,
            Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
        truncated |= votes.len() as u64 >= limit;
        segments.push(serde_json::json!({
            "chain": w.chain,
            "from": w.from,
            "to": w.to,
            "votes_indexed": votes.len(),
        }));
        for v in votes {
            merged.insert(v.voter.clone(), v);
        }
    }
    let votes: Vec<&VoteRow> = merged.values().collect();
    let active: Vec<&VoteRow> = merged.values().filter(|v| v.active).collect();
    Json(serde_json::json!({
        "network": network,
        "class": class,
        "referendum_id": id,
        "direct_tally": {
            "voters": active.len(),
            "ayes": sum_decimal(active.iter().map(|v| v.aye_votes.clone())),
            "nays": sum_decimal(active.iter().map(|v| v.nay_votes.clone())),
            "support": sum_decimal(active.iter().map(|v| v.support.clone())),
            "truncated": truncated,
            "note": "direct votes only — delegated power is not carried by any event",
        },
        "votes": votes,
        "segments": segments,
    }))
    .into_response()
}

/// One account's governance participation for a NETWORK: current vote
/// positions, delegation edges, and the VotingFor state anchors that carry the
/// numbers events never do (how much was delegated, and how much delegated
/// power the account received).
///
/// `?class=` filters to one referenda instance; omitted, every instance is
/// returned (an account can be both a token holder and a Fellow).
async fn get_gov_account_votes(
    State(state): State<AppState>,
    Path((network, account)): Path<(String, String)>,
    Query(q): Query<GovQuery>,
) -> Response {
    let limit = q.limit.unwrap_or(100).min(1000);
    let class = q.class;
    let account_id = match (state.parse_account)(&account) {
        Ok(bytes) => bytes,
        Err(e) => return error(StatusCode::BAD_REQUEST, format!("bad account '{account}': {e}")),
    };
    // no class given → walk every registered instance's chains, because an
    // account's Fellowship votes live on a different chain from its token
    // votes; with ?class= we walk only that instance's residency
    let windows = match class.as_deref() {
        Some(c) => gov_windows(&state.registry, &network, c),
        None => all_gov_windows(&state.registry, &network),
    };
    if windows.is_empty() {
        return error(
            StatusCode::NOT_FOUND,
            match class.as_deref() {
                Some(c) => format!(
                    "no residency for referenda class '{c}' (domain '{}') on network '{network}'",
                    state.registry.domain_for_class(c)
                ),
                None => format!("no governance residency for network '{network}'"),
            },
        );
    }

    let keep = |c: &str| class.as_deref().is_none_or(|want| want == c);
    let mut segments = Vec::with_capacity(windows.len());
    for w in &windows {
        let votes: Vec<VoteRow> = match state.gov.account_votes(&w.chain, &account_id, limit).await
        {
            Ok(v) => v.into_iter().filter(|r| keep(&r.class)).collect(),
            Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
        let delegations: Vec<DelegationRow> =
            match state.gov.account_delegations(&w.chain, &account_id).await {
                Ok(d) => d.into_iter().filter(|r| keep(&r.class)).collect(),
                Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            };
        let anchors: Vec<VotingAnchorRow> =
            match state.gov.voting_anchors(&w.chain, &account_id).await {
                Ok(a) => a.into_iter().filter(|r| keep(&r.class)).collect(),
                Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            };
        segments.push(serde_json::json!({
            "chain": w.chain,
            "from": w.from,
            "to": w.to,
            "votes": votes,
            "delegations": delegations,
            "voting_anchors": anchors,
        }));
    }
    Json(serde_json::json!({
        "network": network,
        "account_id": format!("0x{}", hex_lower(&account_id)),
        "segments": segments,
    }))
    .into_response()
}

/// Track definitions for the chain hosting a referenda CLASS at `at` (default:
/// the public instance, now) — decoded from that runtime's own metadata,
/// served as data. `?class=fellowship_referenda` returns the Fellowship's
/// tracks from Collectives; the class → domain step is the same registry data
/// every other gov endpoint uses (reviewer catch: this handler used to hardcode
/// the governance domain, which made the fellowship tracks it now syncs
/// unreachable).
async fn get_gov_tracks(
    State(state): State<AppState>,
    Path(network): Path<String>,
    Query(q): Query<GovQuery>,
) -> Response {
    let at = q.at.unwrap_or_else(Utc::now);
    let class = q
        .class
        .unwrap_or_else(|| registry::DEFAULT_REFERENDA_CLASS.to_string());
    let domain = state.registry.domain_for_class(&class);
    let chain = match state.registry.resolve_domain(domain, &network, at) {
        Ok(c) => c,
        Err(e) => return error(StatusCode::NOT_FOUND, e.to_string()),
    };
    // Resolving the CHAIN is not enough: Collectives runs three referenda
    // instances whose track ids collide (id 1 = "members" for the Fellowship,
    // "ambassador" for the Ambassador programme), so a chain-only answer is
    // ambiguous to join against a referendum's track_id. Filter by the class's
    // own pallet — registry data, so a new instance needs no code here.
    let pallet = state.registry.pallet_for_class(&class);
    match state.gov.tracks(&chain.id).await {
        Ok(tracks) => {
            let tracks: Vec<GovTrackRow> = match pallet {
                Some(p) => tracks.into_iter().filter(|t| t.pallet == p).collect(),
                None => tracks,
            };
            Json(serde_json::json!({
                "network": network,
                "class": class,
                "chain": chain.id,
                "at": at,
                // null = this class registered no pallet, so every instance on
                // the chain is listed and track ids may collide. Stated, not implied.
                "pallet": pallet,
                "tracks": tracks,
            }))
            .into_response()
        }
        Err(e) => error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

// ----------------------------------------------------------- treasury routes

#[derive(Deserialize)]
struct TreasuryQuery {
    /// Pallet instance; defaults to the main treasury.
    instance: Option<String>,
    /// Spend id space for the detail route: proposal | asset_spend.
    kind: Option<String>,
    status: Option<String>,
    limit: Option<u64>,
}

/// Residency windows carrying one treasury INSTANCE, in time order. Same
/// registry-driven step as the referenda classes: the main treasury stitches
/// relay → Asset Hub across Nov-2025, the Collectives sub-treasuries resolve to
/// their own chain, and no caller names either.
fn treasury_windows<'a>(
    registry: &'a Registry,
    network: &str,
    instance: &str,
) -> Vec<&'a registry::ResidencyEntry> {
    let domain = registry.domain_for_treasury_instance(instance);
    let mut windows: Vec<&registry::ResidencyEntry> = registry
        .residency()
        .iter()
        .filter(|r| r.domain == domain && r.network == network)
        .collect();
    windows.sort_by_key(|r| r.from);
    windows
}

fn treasury_instance(q: &TreasuryQuery) -> String {
    q.instance
        .clone()
        .unwrap_or_else(|| registry::DEFAULT_TREASURY_INSTANCE.to_string())
}

fn no_treasury_residency(instance: &str, network: &str, domain: &str) -> Response {
    error(
        StatusCode::NOT_FOUND,
        format!("no residency for treasury instance '{instance}' (domain '{domain}') on network '{network}'"),
    )
}

/// Merge one spend's rows from two residency windows.
///
/// This is NOT the vote-position rule (later window replaces): a vote row is
/// self-contained, a spend row is not. Only `AssetSpendApproved` carries the
/// amount, asset kind and beneficiary, so for a spend approved on the relay and
/// paid on Asset Hub the later row's value columns are all NULL. Replacing
/// wholesale would drop the amount from the answer to "what was promised"
/// (reviewer catch). Status and payment come from the later window; the value
/// columns fall back to the earlier one; the first sighting is the earlier
/// WINDOW's, taken structurally rather than by comparing the two heights —
/// relay (~32M) and Asset Hub (~19M) block numbers are different number lines,
/// so `min()` would answer "which chain numbers its blocks lower", exactly the
/// mistake the list-ordering comment below warns about. Windows are sorted by
/// `from`, so `prev` is the earlier one by construction. `merge_bounty` states
/// the same rule; there is one shape here, not two.
fn merge_spend(prev: SpendRow, later: SpendRow) -> SpendRow {
    SpendRow {
        instance: later.instance,
        spend_kind: later.spend_kind,
        spend_id: later.spend_id,
        status: later.status,
        status_height: later.status_height,
        amount: later.amount.or(prev.amount),
        slashed: later.slashed.or(prev.slashed),
        asset_kind: later.asset_kind.or(prev.asset_kind),
        beneficiary: later.beneficiary.or(prev.beneficiary),
        beneficiary_location: later.beneficiary_location.or(prev.beneficiary_location),
        payment_id: later.payment_id.or(prev.payment_id),
        valid_from: later.valid_from.or(prev.valid_from),
        expire_at: later.expire_at.or(prev.expire_at),
        asset_ref: later.asset_ref.or(prev.asset_ref),
        first_seen_height: prev.first_seen_height,
    }
}

/// WHAT THE TREASURY PROMISED: spends for one instance, residency-stitched.
///
/// A spend's id is unique only within (instance, id space), and the same id can
/// carry events on both sides of the Nov-2025 migration (the pallet's state
/// moved with it), so rows are MERGED per (kind, id) with the later window
/// winning — the same rule the vote positions use.
async fn list_treasury_spends(
    State(state): State<AppState>,
    Path(network): Path<String>,
    Query(q): Query<TreasuryQuery>,
) -> Response {
    let instance = treasury_instance(&q);
    let limit = q.limit.unwrap_or(50).min(500);
    let windows = treasury_windows(&state.registry, &network, &instance);
    if windows.is_empty() {
        return no_treasury_residency(
            &instance,
            &network,
            state.registry.domain_for_treasury_instance(&instance),
        );
    }

    let mut merged: std::collections::BTreeMap<(String, u64), SpendRow> =
        std::collections::BTreeMap::new();
    let mut segments = Vec::with_capacity(windows.len());
    for w in &windows {
        let rows = match state
            .treasury
            .spends(&w.chain, &instance, q.status.as_deref(), q.kind.as_deref(), limit)
            .await
        {
            Ok(r) => r,
            Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
        segments.push(serde_json::json!({
            "chain": w.chain,
            "from": w.from,
            "to": w.to,
            "spends_indexed": rows.len(),
        }));
        for r in rows {
            let key = (r.spend_kind.clone(), r.spend_id);
            let merged_row = match merged.remove(&key) {
                None => r,
                Some(prev) => merge_spend(prev, r),
            };
            merged.insert(key, merged_row);
        }
    }
    // sort by the id space, NOT by height: relay heights (~32M) and Asset Hub
    // heights (~19M) are different number lines, so sorting merged rows by
    // height would float every relay-era proposal above every recent spend
    let mut spends: Vec<&SpendRow> = merged.values().collect();
    spends.sort_by(|a, b| {
        a.spend_kind
            .cmp(&b.spend_kind)
            .then_with(|| b.spend_id.cmp(&a.spend_id))
    });
    spends.truncate(limit as usize);
    Json(serde_json::json!({
        "network": network,
        "instance": instance,
        "status": q.status,
        "spend_kind": q.kind,
        "spends": spends,
        "segments": segments,
    }))
    .into_response()
}

/// One spend's full story: the merged row plus its per-chain event timeline.
/// `?kind=` picks the id space (the legacy proposal counter and the modern
/// SpendIndex are different numbers that both start at 0), defaulting to the
/// modern one.
async fn get_treasury_spend(
    State(state): State<AppState>,
    Path((network, id)): Path<(String, u64)>,
    Query(q): Query<TreasuryQuery>,
) -> Response {
    let instance = treasury_instance(&q);
    let kind = q.kind.clone().unwrap_or_else(|| "asset_spend".to_string());
    let windows = treasury_windows(&state.registry, &network, &instance);
    if windows.is_empty() {
        return no_treasury_residency(
            &instance,
            &network,
            state.registry.domain_for_treasury_instance(&instance),
        );
    }

    let mut spend: Option<SpendRow> = None;
    let mut segments = Vec::with_capacity(windows.len());
    for w in &windows {
        match state.treasury.spend(&w.chain, &instance, &kind, id).await {
            // merge, never replace: the approval's value columns live in the
            // earlier window when a spend straddles the migration
            Ok(Some(s)) => {
                spend = Some(match spend.take() {
                    None => s,
                    Some(prev) => merge_spend(prev, s),
                })
            }
            Ok(None) => {}
            Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        }
        let events = match state.treasury.spend_events(&w.chain, &instance, &kind, id).await {
            Ok(e) => e,
            Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
        segments.push(serde_json::json!({
            "chain": w.chain,
            "from": w.from,
            "to": w.to,
            "events": events,
        }));
    }
    let Some(spend) = spend else {
        return error(
            StatusCode::NOT_FOUND,
            format!("treasury spend {network}/{instance}/{kind}/{id} not indexed"),
        );
    };
    // WHAT WAS THIS DENOMINATED IN? The spend names its asset as an XCM
    // location; `core.assets` knows that location's symbol and decimals. The
    // resolution is a join, not a lookup table — and it is done here, at read
    // time, because which chain's pallet index 50 is the assets pallet is a
    // property of a runtime, not of a spend.
    let asset = resolve_spend_asset(&state, &network, &windows, &spend).await;

    Json(serde_json::json!({
        "network": network,
        "instance": instance,
        "spend_kind": kind,
        "spend_id": id,
        "spend": spend,
        // null with a reason, never a silently missing field: an unresolved
        // asset is the difference between "20,895 USDT" and a number whose
        // unit the reader has to guess
        "asset": asset,
        "segments": segments,
    }))
    .into_response()
}

/// Resolve a spend's `asset_ref`. A thin wrapper: a bounty payout and a
/// treasury spend are denominated the same way, carry the identical
/// `{chain, asset}` shape, and must therefore resolve through ONE piece of
/// code — two copies would eventually disagree about what 83,760 means.
async fn resolve_spend_asset(
    state: &AppState,
    network: &str,
    windows: &[&registry::ResidencyEntry],
    spend: &SpendRow,
) -> serde_json::Value {
    resolve_asset_ref(
        state,
        network,
        windows,
        spend.asset_ref.as_ref(),
        spend.amount.as_deref(),
        // the legacy flow had no asset concept at all — native by
        // construction, not by assumption
        (spend.spend_kind == "proposal")
            .then_some("legacy proposal flow: always the chain's native token"),
        "no asset_location on this row — re-run treasury-range \
         (mapper_version 1 predates it)",
    )
    .await
}

/// Resolve an `asset_ref` against the asset registry of whichever chain its own
/// location names. Returns a resolution OBJECT even on failure, carrying the
/// reason — a treasury page must be able to say "we do not know what unit this
/// is in" out loud.
///
/// `native_by_construction` carries the REASON a missing asset_ref means native
/// rather than unknown; `rerun_hint` is what to say when it means neither.
async fn resolve_asset_ref(
    state: &AppState,
    network: &str,
    windows: &[&registry::ResidencyEntry],
    asset_ref: Option<&serde_json::Value>,
    amount: Option<&str>,
    native_by_construction: Option<&str>,
    rerun_hint: &str,
) -> serde_json::Value {
    let unresolved = |reason: &str| serde_json::json!({"resolved": false, "reason": reason});
    let Some(asset_ref) = asset_ref else {
        return match native_by_construction {
            Some(note) => serde_json::json!({
                "resolved": true, "asset": "native", "symbol": null, "decimals": null,
                "note": note,
            }),
            None => unresolved(rerun_hint),
        };
    };
    // `.filter(!is_null)` because a jsonb-built object HAS the key even when
    // the value is null — a normalization that half-failed must read as "names
    // no asset", not as a search for the literal string "null" (slice 6's
    // twice-repeated defect, said out loud here).
    let Some(asset_loc) = asset_ref.pointer("/location/asset").filter(|v| !v.is_null()) else {
        return unresolved("asset_location names no asset");
    };
    // An EMPTY interior names the holding chain's own currency. The mapper
    // resolves that to `native` only at parents 0; here we accept any parents,
    // and say so — on a system parachain the parent's token IS the local one,
    // and the day that stops being true (a chain with a token of its own,
    // Phase 3) the registry has to say which token `parents: 1` means.
    let native_named = asset_loc
        .get("interior")
        .and_then(|i| i.as_array())
        .is_some_and(|j| j.is_empty());
    // Re-render through serde_json (BTreeMap ⇒ sorted keys) so the string
    // matches `core.assets.location_key` exactly, whatever order Postgres's
    // jsonb chose to store it in.
    let wanted = asset_loc.to_string();
    // WHICH CHAIN holds it: the row says `Parachain(N)` or `Here`. `Here`
    // means the chain that emitted the event, so we look through this
    // instance's own residency windows rather than assuming one.
    let para = asset_ref
        .pointer("/location/chain/interior")
        .and_then(|i| i.as_array())
        .and_then(|js| js.first())
        .and_then(|j| j.get("Parachain"))
        .and_then(|p| p.as_u64());
    // …and the NETWORK matters: Kusama's Asset Hub is also para 1000, and
    // `chains()` iterates a map, so without this filter which one answered
    // would be nondeterministic the day Kusama is registered (reviewer catch).
    let mut candidates: Vec<String> = match para {
        Some(id) => state
            .registry
            .chains()
            .filter(|c| c.para_id == Some(id as u32) && c.network == network)
            .map(|c| c.id.clone())
            .collect(),
        None => windows.iter().map(|w| w.chain.clone()).collect(),
    };
    candidates.sort();
    candidates.dedup();
    for chain in &candidates {
        let Ok(assets) = state.assets.assets(chain).await else {
            continue;
        };
        if let Some(a) = assets.iter().find(|a| {
            a.location_key.as_deref() == Some(wanted.as_str())
                || (native_named && a.asset_key == "native")
        }) {
            return serde_json::json!({
                "resolved": true,
                "chain": chain,
                "asset": a.asset_key,
                "symbol": a.symbol,
                "decimals": a.decimals,
                "assumption": native_named.then_some(
                    "an empty interior is read as the holding chain's own currency, \
                     whatever its `parents` — true for a relay and its system \
                     parachains, and registry data the day it is not"
                ),
                "display": amount
                    .zip(a.decimals)
                    .and_then(|(amount, d)| format_units(amount, d)),
            });
        }
    }
    serde_json::json!({
        "resolved": false,
        "reason": "no asset in core.assets matches this location — run sync-assets \
                   on the chain that holds it",
        "looking_for": wanted,
        "chains_tried": candidates,
    })
}

/// Pot flows: money into and out of the treasury account that names no spend
/// (deposits, burns, rollovers, the spend-period bookkeeping). Newest first.
async fn get_treasury_pot(
    State(state): State<AppState>,
    Path(network): Path<String>,
    Query(q): Query<TreasuryQuery>,
) -> Response {
    let instance = treasury_instance(&q);
    let limit = q.limit.unwrap_or(50).min(500);
    let windows = treasury_windows(&state.registry, &network, &instance);
    if windows.is_empty() {
        return no_treasury_residency(
            &instance,
            &network,
            state.registry.domain_for_treasury_instance(&instance),
        );
    }
    let mut segments = Vec::with_capacity(windows.len());
    for w in &windows {
        let events = match state.treasury.pot_events(&w.chain, &instance, limit).await {
            Ok(e) => e,
            Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
        segments.push(serde_json::json!({
            "chain": w.chain,
            "from": w.from,
            "to": w.to,
            "events": events,
        }));
    }
    Json(serde_json::json!({
        "network": network,
        "instance": instance,
        "note": "pot flows only — these name no spend; see /spends for what was promised",
        "segments": segments,
    }))
    .into_response()
}

// ----------------------------------------------------------- bounty routes

#[derive(Deserialize)]
struct BountyQuery {
    /// bounties | child_bounties | multi_asset_bounties.
    instance: Option<String>,
    /// Child index. ABSENT means the parent bounty itself — the API's spelling
    /// of the -1 the table stores.
    child: Option<u64>,
    status: Option<String>,
    limit: Option<u64>,
}

fn bounty_instance(q: &BountyQuery) -> String {
    q.instance
        .clone()
        .unwrap_or_else(|| DEFAULT_BOUNTY_INSTANCE.to_string())
}

/// Residency windows for bounties: the TREASURY domain, explicitly.
///
/// Bounty funds live at sub-accounts of the treasury's pallet id and moved to
/// Asset Hub with it, so a bounty proposed on the relay and paid on Asset Hub
/// is one bounty with rows on two chains — exactly a spend's shape. Named
/// explicitly rather than by passing a bounty instance into
/// `domain_for_treasury_instance` (whose fallback would answer the same thing
/// by accident, which is not the same as answering it on purpose).
///
/// DEDUPLICATED BY CHAIN, and here that is correctness rather than economy.
/// The per-chain queries are not window-bounded, so a chain appearing in two
/// residency windows (a domain that left and came back) returns the same rows
/// twice — and `merge_bounty` ADDS `paid_out`, so the bounty would report
/// double what it paid. Every other merge in this file is idempotent under a
/// repeated chain; this one cannot be, so the repetition is removed here. The
/// FIRST window wins, so a deduped segment carries the earlier bounds; the
/// same trade `all_gov_windows` makes, and it also halves the queries.
fn bounty_windows<'a>(registry: &'a Registry, network: &str) -> Vec<&'a registry::ResidencyEntry> {
    let mut seen: Vec<&str> = Vec::new();
    let mut windows: Vec<&'a registry::ResidencyEntry> = Vec::new();
    for w in treasury_windows(registry, network, registry::DEFAULT_TREASURY_INSTANCE) {
        if !seen.contains(&w.chain.as_str()) {
            seen.push(&w.chain);
            windows.push(w);
        }
    }
    windows
}

/// Merge one bounty's rows from two residency windows. Same reasoning as
/// `merge_spend` — the later window owns the status, the earlier one may be the
/// only place a value column exists — with two additions.
///
/// `paid_out` is a RUNNING TOTAL per chain, so the two windows must be ADDED,
/// not chosen between. Choosing would silently halve a bounty that paid on both
/// sides of the migration. That also makes this the one merge in the file that
/// is NOT idempotent under a repeated chain, which is why `bounty_windows`
/// dedupes by chain before anybody loops over it.
///
/// And the `'unknown'` guard is `merge_referendum`'s, for the same reason: the
/// post-migration events that carry no status at all (`BountyExtended`,
/// `DepositPoked`, `BountyValueIncreased`) land as the sink's placeholder, and
/// without this guard an Asset Hub value raise would erase a relay verdict.
fn merge_bounty(prev: BountyRow, later: BountyRow) -> BountyRow {
    let (status, status_height) = if later.status == "unknown" {
        (prev.status, prev.status_height)
    } else {
        (later.status, later.status_height)
    };
    BountyRow {
        instance: later.instance,
        bounty_id: later.bounty_id,
        child_id: later.child_id,
        status,
        status_height,
        value: later.value.or(prev.value),
        paid_out: add_money(prev.paid_out, later.paid_out),
        bond: later.bond.or(prev.bond),
        curator: later.curator.or(prev.curator),
        beneficiary: later.beneficiary.or(prev.beneficiary),
        beneficiary_location: later.beneficiary_location.or(prev.beneficiary_location),
        payment_id: later.payment_id.or(prev.payment_id),
        account_id: later.account_id.or(prev.account_id),
        asset_ref: later.asset_ref.or(prev.asset_ref),
        // the EARLIER window's, never `min()`: relay heights (~32M) and Asset
        // Hub heights (~19M) are different number lines, so comparing them
        // picks the smaller number rather than the earlier sighting. The
        // windows are sorted by `from`, so `prev` IS the earlier one.
        first_seen_height: prev.first_seen_height,
    }
}

/// Add two decimal money strings. i128 rather than a decimal crate: the largest
/// number in this domain is total issuance (~1.5e19 planck), and i128 holds
/// ~1.7e38, so the headroom is nineteen orders of magnitude — and the add
/// saturates, like `sum_decimal` above, so a corrupt row cannot panic a
/// response. If EITHER side fails to parse there is no sum to report, so the
/// later window's value is returned unchanged: visibly one window's number
/// rather than an invisibly wrong total.
fn add_money(a: Option<String>, b: Option<String>) -> Option<String> {
    match (a, b) {
        (None, x) | (x, None) => x,
        (Some(a), Some(b)) => match (a.parse::<i128>(), b.parse::<i128>()) {
            (Ok(x), Ok(y)) => Some(x.saturating_add(y).to_string()),
            _ => Some(b),
        },
    }
}

/// WHERE THE TREASURY'S BOUNTY MONEY WENT — the outflow that leaves through the
/// `SpendFunds` hook and appears in no treasury table (migration 0009).
///
/// Rows are merged per (bounty, child) across residency windows, exactly like
/// spends, and `child_id` renders as null for a parent — the -1 in the table is
/// a primary-key device and never a value the API emits.
async fn list_bounties(
    State(state): State<AppState>,
    Path(network): Path<String>,
    Query(q): Query<BountyQuery>,
) -> Response {
    let instance = bounty_instance(&q);
    let limit = q.limit.unwrap_or(50).min(500);
    let windows = bounty_windows(&state.registry, &network);
    if windows.is_empty() {
        return no_treasury_residency(
            &instance,
            &network,
            state
                .registry
                .domain_for_treasury_instance(registry::DEFAULT_TREASURY_INSTANCE),
        );
    }

    let mut merged: std::collections::BTreeMap<(u64, i64), BountyRow> =
        std::collections::BTreeMap::new();
    let mut segments = Vec::with_capacity(windows.len());
    for w in &windows {
        let rows = match state
            .bounties
            .bounties(&w.chain, &instance, q.status.as_deref(), limit)
            .await
        {
            Ok(r) => r,
            Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
        segments.push(serde_json::json!({
            "chain": w.chain,
            "from": w.from,
            "to": w.to,
            "bounties_indexed": rows.len(),
        }));
        for r in rows {
            // the sentinel is fine as a MAP KEY (it sorts parents before their
            // children, which is the order a page wants); it just never
            // reaches the response
            let key = (r.bounty_id, r.child_id.map_or(PARENT_SENTINEL, |c| c as i64));
            let merged_row = match merged.remove(&key) {
                None => r,
                Some(prev) => merge_bounty(prev, r),
            };
            merged.insert(key, merged_row);
        }
    }
    let mut bounties: Vec<&BountyRow> = merged.values().collect();
    bounties.sort_by(|a, b| {
        b.bounty_id
            .cmp(&a.bounty_id)
            .then_with(|| b.child_id.cmp(&a.child_id))
    });
    bounties.truncate(limit as usize);
    Json(serde_json::json!({
        "network": network,
        "instance": instance,
        "status": q.status,
        "bounties": bounties,
        "segments": segments,
        "note": "bounty funding leaves the treasury through the SpendFunds hook and \
                 emits no treasury event — these rows are the only record of it. \
                 `paid_out` sums the payouts the pallets ANNOUNCE, and those are the \
                 beneficiary's share NET OF THE CURATOR FEE, which no event carries: \
                 a bounty spent more than this number says. The exact figures are on \
                 the bounty's own derived account, which pallet-bounties MINTS into \
                 when it funds the bounty — so `account_id` in /v1/balances, not \
                 `value` minus `paid_out`, is what a bounty has left; see also \
                 /v1/treasury/{network}/holdings.",
    }))
    .into_response()
}

/// One bounty's full story: the merged row, its per-chain event timeline, and
/// what its payouts were denominated in. `?child=N` addresses a child bounty;
/// without it the parent bounty itself.
async fn get_bounty(
    State(state): State<AppState>,
    Path((network, id)): Path<(String, u64)>,
    Query(q): Query<BountyQuery>,
) -> Response {
    let instance = bounty_instance(&q);
    let windows = bounty_windows(&state.registry, &network);
    if windows.is_empty() {
        return no_treasury_residency(
            &instance,
            &network,
            state
                .registry
                .domain_for_treasury_instance(registry::DEFAULT_TREASURY_INSTANCE),
        );
    }

    let mut bounty: Option<BountyRow> = None;
    let mut segments = Vec::with_capacity(windows.len());
    for w in &windows {
        match state.bounties.bounty(&w.chain, &instance, id, q.child).await {
            // merge, never replace: only some events carry a curator, a value
            // or a beneficiary, and they may sit in the earlier window
            Ok(Some(b)) => {
                bounty = Some(match bounty.take() {
                    None => b,
                    Some(prev) => merge_bounty(prev, b),
                })
            }
            Ok(None) => {}
            Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        }
        let events = match state
            .bounties
            .bounty_events(&w.chain, &instance, id, q.child)
            .await
        {
            Ok(e) => e,
            Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
        segments.push(serde_json::json!({
            "chain": w.chain,
            "from": w.from,
            "to": w.to,
            "events": events,
        }));
    }
    let Some(bounty) = bounty else {
        let which = match q.child {
            Some(c) => format!("{id}-{c}"),
            None => id.to_string(),
        };
        return error(
            StatusCode::NOT_FOUND,
            format!("bounty {network}/{instance}/{which} not indexed"),
        );
    };
    // WHAT WAS THIS PAID IN? The same join a treasury spend resolves through,
    // because a multi-asset bounty payout names its asset the same way. The two
    // legacy pallets are native-token-only by construction, so a missing
    // asset_ref there is an ANSWER, not a gap.
    let asset = resolve_asset_ref(
        &state,
        &network,
        &windows,
        bounty.asset_ref.as_ref(),
        bounty.paid_out.as_deref().or(bounty.value.as_deref()),
        (instance != "multi_asset_bounties").then_some(
            "pallet-bounties and pallet-child-bounties are native-token-only by \
             construction — they have no asset concept to record",
        ),
        "no asset_location on this row — a multi-asset bounty carries one only \
         once a payout has been processed",
    )
    .await;

    Json(serde_json::json!({
        "network": network,
        "instance": instance,
        "bounty_id": id,
        // null, never -1: the sentinel is a primary-key device
        "child_id": q.child,
        "bounty": bounty,
        "asset": asset,
        "segments": segments,
    }))
    .into_response()
}

// ------------------------------------------------------- assets + holdings

/// Every asset representation dotlens knows about on one CHAIN (not network:
/// assets are chain-local by definition — one logical USDC is several
/// representations, and saying which chain you mean is half the answer).
async fn list_assets(State(state): State<AppState>, Path(chain): Path<String>) -> Response {
    match state.assets.assets(&chain).await {
        Ok(assets) => Json(serde_json::json!({
            "chain": chain,
            "count": assets.len(),
            "note": "one row per REPRESENTATION on this chain; the identity graph \
                     linking representations of one logical asset across chains is \
                     not built yet (ARCHITECTURE §8, Phase 5)",
            "assets": assets,
        }))
        .into_response(),
        Err(e) => error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

/// Render an integer amount in an asset's own units, WITHOUT floats: string
/// surgery only, so 20895000000 at 6 decimals is exactly "20895.000000" and
/// never 20894.999999999996. Returns None when we do not know the decimals,
/// because a guess of 0 would misreport by a factor of a million.
fn format_units(amount: &str, decimals: u32) -> Option<String> {
    let (sign, digits) = match amount.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", amount),
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let d = decimals as usize;
    if d == 0 {
        return Some(format!("{sign}{digits}"));
    }
    let padded = if digits.len() <= d {
        format!("{}{}", "0".repeat(d - digits.len() + 1), digits)
    } else {
        digits.to_string()
    };
    let split = padded.len() - d;
    Some(format!("{sign}{}.{}", &padded[..split], &padded[split..]))
}

/// THE PHASE 2 EXIT SURFACE: an itemized "where the funds are", per account,
/// per asset, with the provenance of every number attached to it.
///
/// The shape of the answer is deliberate:
///   * accounts come from `treasury.treasury_accounts`, each carrying HOW it
///     was derived — a reader can re-derive `modl:py/trsry` themselves
///   * amounts come from a state ANCHOR plus the deltas since, and every row
///     says which block it was anchored at, against which spec_version, by
///     which command, and whether events have moved it since (`basis`)
///   * a pair we have only ever seen MOVING, never anchored, is reported with
///     a null amount instead of being left out
///   * `coverage` states what is NOT here. A treasury page that lists what it
///     found and stays quiet about the rest is how every existing tool
///     understates the treasury; ours says the words.
async fn get_treasury_holdings(
    State(state): State<AppState>,
    Path(network): Path<String>,
) -> Response {
    let accounts = match state.treasury.accounts(&network).await {
        Ok(a) => a,
        Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };

    // group by chain, preserving the (chain, role, label) order the index gave
    let mut chains: Vec<String> = Vec::new();
    for a in &accounts {
        if !chains.contains(&a.chain_id) {
            chains.push(a.chain_id.clone());
        }
    }

    let mut segments = Vec::with_capacity(chains.len());
    let mut anchored_assets = 0usize;
    let mut unanchored_pairs = 0usize;

    for chain in &chains {
        // ONE ROW PER ADDRESS. `treasury_accounts` is keyed (chain, account,
        // role), so an address that is both a derived pot and a registry seed
        // has two rows — and rendering both would report its balance twice and
        // count its positions twice in `coverage` (reviewer catch). Roles are
        // merged into a list instead.
        let mut on_chain: Vec<(&TreasuryAccountRow, Vec<String>)> = Vec::new();
        for a in accounts.iter().filter(|a| &a.chain_id == chain) {
            match on_chain
                .iter_mut()
                .find(|(first, _)| first.account_id == a.account_id)
            {
                Some((_, roles)) => roles.push(a.role.clone()),
                None => on_chain.push((a, vec![a.role.clone()])),
            }
        }
        let ids: Vec<Vec<u8>> = on_chain.iter().map(|(a, _)| a.account_id.clone()).collect();

        let holdings = match state.balances.holdings(chain, &ids, None).await {
            Ok(h) => h,
            Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
        let assets = match state.assets.assets(chain).await {
            Ok(a) => a,
            Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
        let asset_by_key: HashMap<&str, &AssetRow> =
            assets.iter().map(|a| (a.asset_key.as_str(), a)).collect();

        let mut account_rows = Vec::with_capacity(on_chain.len());
        for (account, roles) in &on_chain {
            let mut positions = Vec::new();
            let mut zero_assets = 0usize;
            for h in holdings.iter().filter(|h| h.account_id == account.account_id) {
                let amount = h.amount();
                // a zero position is not news; an UNKNOWN one is
                if amount.as_deref() == Some("0") {
                    zero_assets += 1;
                    continue;
                }
                if amount.is_none() {
                    unanchored_pairs += 1;
                } else {
                    anchored_assets += 1;
                }
                let asset = asset_by_key.get(h.asset.as_str());
                let decimals = asset.and_then(|a| a.decimals);
                positions.push(serde_json::json!({
                    "asset": h.asset,
                    "symbol": asset.and_then(|a| a.symbol.clone()),
                    "decimals": decimals,
                    // the exact integer, always — the rendered form only when
                    // the chain told us how many decimals it has
                    "amount": amount,
                    "display": amount
                        .as_deref()
                        .zip(decimals)
                        .and_then(|(a, d)| format_units(a, d)),
                    "basis": h.basis(),
                    "asset_status": asset.and_then(|a| a.status.clone()),
                    "provenance": {
                        "anchor_height": h.anchor_height,
                        "anchor_total": h.anchor_total,
                        "anchor_spec_version": h.anchor_spec_version,
                        "anchor_source": h.anchor_source,
                        "anchor_note": h.anchor_note,
                        "account_status": h.anchor_status,
                        "delta_sum_since_anchor": h.delta_sum,
                        "delta_count_since_anchor": h.delta_count,
                        "last_delta_height": h.last_delta_height,
                    },
                }));
            }
            account_rows.push(serde_json::json!({
                "account_id": format!("0x{}", hex_lower(&account.account_id)),
                "ss58": account.ss58,
                "label": account.label,
                "role": account.role,
                "roles": roles,
                "instance": account.instance,
                "derivation": account.derivation,
                "source": account.source,
                "positions": positions,
                "zero_balance_assets": zero_assets,
            }));
        }
        segments.push(serde_json::json!({
            "chain": chain,
            "accounts": account_rows,
            "assets_registered": assets.len(),
        }));
    }

    Json(serde_json::json!({
        "network": network,
        "segments": segments,
        "coverage": {
            "accounts": accounts.len(),
            "chains": chains,
            "positions_with_an_anchor": anchored_assets,
            "positions_without_an_anchor": unanchored_pairs,
            "valuation": "none — quantities only. No price source is consulted, \
                          so nothing here can be stale or unsourced; a USD view \
                          is a later slice with its own provenance",
            "not_covered": [
                // NOW PARTLY COVERED, and the line says exactly how far. A
                // bounty account appears here only once its bounty's EVENTS
                // were indexed and `sync-bounty-accounts` derived its address,
                // so a chain that has never been bounties-range'd shows none of
                // its bounty money and this endpoint must not imply otherwise.
                "bounty money is covered only as far as it has been INDEXED: a \
                 bounty account appears above once its events were mapped \
                 (bounties-range) and its address derived (sync-bounty-accounts). \
                 Bounties before the first indexed range, and child bounties \
                 whose events have not been seen, hold real money that is absent \
                 here — the funding itself leaves the treasury through the \
                 SpendFunds hook and emits no treasury event at all (migration \
                 0009), so nothing else would reveal them",
                "LEGACY CHILD BOUNTIES hold real money that is absent here on \
                 purpose: pallet-child-bounties ≤37.0.0 derived a child's \
                 account from a GLOBAL child id and 38.0.0 renumbered every \
                 child bounty per parent, transferring the balances to new \
                 addresses. This table records no era, so applying today's rule \
                 would name addresses that never existed — dotlens refuses to \
                 derive them rather than report a confident zero at a made-up \
                 address",
                "a concluded bounty (claimed, canceled, rejected) is registered \
                 INACTIVE and is not swept: its account is emptied and removed \
                 from pallet storage when the bounty ends. If one is ever \
                 refunded after conclusion, this endpoint will not see it",
                "positions on chains dotlens has not registered — notably the \
                 Hydration DCA accounts, the Omnipool POL and the money-market \
                 position (ECOSYSTEM §6 puts treasury assets across 7+ chains); \
                 Hydration is registered in Phase 3",
                "assets whose storage key has never been read by sync-assets: \
                 they are listed with a null amount, never as zero",
                "an asset being destroyed (status 'Destroying') zeroes holder \
                 balances with no per-account event — re-anchor after one",
                "NON-FUNGIBLES — this endpoint reads the pallet-assets \
                 instances only, so collection items held by a treasury \
                 account (pallet-nfts / pallet-uniques) are absent. The \
                 Polkadot treasury pot does hold some; they are not fungible \
                 value and are not counted as zero either, they are simply \
                 out of scope until an NFT slice exists",
            ],
        },
        "note": "amounts are anchor + deltas since; `basis` says which. Every \
                 number carries the block it was read at and the runtime it was \
                 decoded against.",
    }))
    .into_response()
}

#[derive(Deserialize)]
struct AtQuery {
    /// RFC3339; defaults to now. The migration-aware knob.
    at: Option<DateTime<Utc>>,
}

async fn resolve_domain(
    State(state): State<AppState>,
    Path((network, domain)): Path<(String, String)>,
    Query(q): Query<AtQuery>,
) -> Response {
    let at = q.at.unwrap_or_else(Utc::now);
    match state.registry.resolve_domain(&domain, &network, at) {
        Ok(chain) => Json(serde_json::json!({
            "domain": domain, "network": network, "at": at, "chain": chain.id,
        }))
        .into_response(),
        Err(e) => error(StatusCode::NOT_FOUND, e.to_string()),
    }
}

fn error(status: StatusCode, message: String) -> Response {
    (status, Json(serde_json::json!({ "error": message }))).into_response()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use std::path::Path as FsPath;
    use tower::util::ServiceExt;

    pub(crate) async fn test_state() -> AppState {
        let seeds = FsPath::new(env!("CARGO_MANIFEST_DIR")).join("../../registry-seeds");
        let registry = Arc::new(Registry::load_from_dir(&seeds).expect("seeds"));
        let blocks: Arc<dyn BlockIndex> = Arc::new(MemoryBlockIndex::new());

        // index the synthetic fixture through the real decode path
        let fixture = FsPath::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/synthetic/polkadot-asset-hub-19000001.json");
        let bytes = std::fs::read(&fixture).expect("fixture");
        let block =
            adapter_substrate::decode_block(&bytes, "raw/polkadot-asset-hub/0001900/19000001/block.json")
                .expect("decode");
        blocks.insert(block).await.expect("insert");

        // one derived label so the labels surface is testable end to end
        let labels = Arc::new(MemoryLabelIndex::new());
        let treasury = adapter_substrate::accounts::pallet_account(b"py/trsry");
        labels.insert(
            "polkadot-asset-hub",
            &treasury,
            canonical::AccountLabel {
                kind: "pallet".into(),
                label: "Treasury (py/trsry)".into(),
                derivation: Some("modl:py/trsry".into()),
                source: "derived".into(),
                ss58: Some(adapter_substrate::frame_decoder::ss58_encode(0, &treasury)),
                verified_at: None,
                verified_block: None,
                verified_note: None,
            },
        );

        // balance history spanning the Nov 2025 migration: relay changes
        // before the boundary, AH changes (+ an anchor) after it
        let balances = Arc::new(MemoryBalanceIndex::new());
        let treasury = adapter_substrate::accounts::pallet_account(b"py/trsry");
        let ts = |s: &str| s.parse::<DateTime<Utc>>().unwrap();
        balances.insert_change(
            "polkadot",
            &treasury,
            "native",
            BalanceChangeRow {
                height: 22_000_000,
                timestamp: Some(ts("2025-06-01T00:00:00Z")),
                event_index: 4,
                delta: "-1000".into(),
                reason: "transfer_out".into(),
                counterparty: None,
            },
        );
        balances.insert_anchor(
            "polkadot-asset-hub",
            &treasury,
            "native",
            BalanceAnchorRow {
                height: 10_000_000,
                free: "500".into(),
                reserved: "0".into(),
                total: "500".into(),
                spec_version: Some(2_000_006),
                source: "test".into(),
                note: None,
                status: None,
            },
        );
        balances.insert_change(
            "polkadot-asset-hub",
            &treasury,
            "native",
            BalanceChangeRow {
                height: 10_000_001,
                timestamp: Some(ts("2026-01-01T00:00:00Z")),
                event_index: 2,
                delta: "200".into(),
                reason: "transfer_in".into(),
                counterparty: None,
            },
        );

        // A Tier 1 preview of ref 1500's proposal, recorded on the chain the
        // referendum CONCLUDED on. Deliberately a dispatch_failed row: the
        // shape most likely to be mis-rendered as "no result" is the one worth
        // pinning in a route test.
        let sim = Arc::new(MemorySimIndex::new());
        sim.insert(SimulationRow {
            chain_id: "polkadot-asset-hub".into(),
            at_height: 19_000_500,
            at_block_hash: format!("0x{}", "cd".repeat(32)),
            input_hash: format!("0x{}", "01".repeat(32)),
            tier: "dry_run".into(),
            call_hash: format!("0x{}", "ab".repeat(32)),
            call_summary: Some("multiassetbounties.fund_bounty".into()),
            origin_spec: "Origins:MediumSpender".into(),
            origin: serde_json::json!({
                "resolved": "Origins:MediumSpender",
                "pallet_index": 20,
                "variant_index": 33,
                "account": null,
            }),
            xcm_version: 4,
            status: "dispatch_failed".into(),
            dispatch_ok: Some(false),
            dispatch_error: Some(serde_json::json!({
                "error": "assets.NoAccount",
                "raw": {"Module": [{"index": 50, "error": [1, 0, 0, 0]}]},
            })),
            emitted_events: serde_json::json!([
                {"name": "balances.Withdraw", "data": {"amount": "20895000000"}}
            ]),
            event_count: 1,
            local_xcm: None,
            forwarded_xcms: serde_json::json!([]),
            effects: serde_json::json!({"Ok": [{"emitted_events": []}]}),
            note: None,
            spec_version: 2_003_002,
            api_version: 2,
            metadata_version: 15,
            sim_version: 1,
            raw_location: "raw/polkadot-asset-hub/sim/cd/01/\
                           DryRunApi_dry_run_call.response.scale"
                .into(),
            observed_at: None,
        });

        // ONE JOURNEY, in the shape live data actually produced (Asset Hub
        // #19581756 → Hydration #13663124): the sending chain emits TWO ids for
        // one message — the router's wire hash first, then pallet-xcm's topic —
        // and the receiving chain reports the TOPIC, under the AMBIGUOUS id kind
        // because messageQueue never says which of the two it is holding.
        let xcm = Arc::new(MemoryXcmIndex::new());
        let xcm_row = |chain: &str, height: u64, side: &str, id_kind: &str| XcmMessageRow {
            chain_id: chain.into(),
            block_height: height,
            event_index: 4,
            side: side.into(),
            transport: "hrmp".into(),
            message_id: Some(format!("0x{}", "ee".repeat(32))),
            id_kind: id_kind.into(),
            counterparty: Some(if side == "sent" { "para:2034" } else { "para:1000" }.into()),
            origin_location: None,
            destination: None,
            message: None,
            forwarded: false,
            status: if side == "sent" { "sent" } else { "processed" }.into(),
            success: (side != "sent").then_some(true),
            error: None,
            weight_used: None,
            runtime_version: 2_003_002,
            mapper_version: 1,
            // The receive is AFTER the send in wall-clock. Two chains' heights
            // are not comparable, so this is the only thing that orders a
            // journey — and the only thing that can CONTRADICT one.
            timestamp: Some(
                if side == "sent" { "2026-08-17T09:00:00Z" } else { "2026-08-17T09:00:24Z" }
                    .parse()
                    .unwrap(),
            ),
        };
        xcm.insert(xcm_row("polkadot-asset-hub", 19_000_900, "sent", "topic"));
        xcm.insert(xcm_row("hydration", 7_000_100, "received", "ambiguous"));
        // The SAME Asset Hub message's transport-level record: a second id, one
        // event EARLIER in the same block, which is the order
        // `WithUniqueTopic::deliver` produces. Without the link below it is a
        // dead end — exactly what slice 2 measured on live data.
        xcm.insert(XcmMessageRow {
            event_index: 3,
            message_id: Some(format!("0x{}", "77".repeat(32))),
            counterparty: None, // the queue event names no recipient
            ..xcm_row("polkadot-asset-hub", 19_000_900, "sent", "wire_hash")
        });
        // an older, unrelated observation on the same chain, to pin ordering
        xcm.insert(XcmMessageRow {
            block_height: 19_000_100,
            message_id: Some(format!("0x{}", "22".repeat(32))),
            timestamp: Some("2026-08-17T08:00:00Z".parse().unwrap()),
            ..xcm_row("polkadot-asset-hub", 19_000_100, "sent", "wire_hash")
        });
        xcm.insert_link(XcmLinkRow {
            chain_id: "polkadot-asset-hub".into(),
            block_height: 19_000_900,
            wire_event_index: 3,
            topic_event_index: 4,
            wire_hash: format!("0x{}", "77".repeat(32)),
            topic: format!("0x{}", "ee".repeat(32)),
            transport: "hrmp".into(),
            rule: "unique_in_block".into(),
            confidence: "high".into(),
            evidence: serde_json::json!({
                "transport_candidates": 1, "ordinal": 0, "event_gap": 1,
                "block_sends": {"wire": 1, "topic": 1}
            }),
            runtime_version: 2_003_002,
            correlator_version: 1,
        });

        // governance stitched across the migration: ref 1500 submitted +
        // deciding on the relay, concluded on Asset Hub; ref 1400 decided on
        // the relay with only an info-event row ('unknown') on AH
        let gov = Arc::new(MemoryGovIndex::new());
        gov.insert_referendum(
            "polkadot",
            ReferendumRow {
                class: "referenda".into(),
                referendum_id: 1500,
                track_id: Some(34),
                status: "deciding".into(),
                status_height: 28_400_000,
                proposal: Some(serde_json::json!({"Lookup": {"hash": [vec![171u8; 32]], "len": 142}})),
                proposal_hash: Some(format!("0x{}", "ab".repeat(32))),
                proposal_len: Some(142),
                submitted_at_height: Some(28_399_000),
            },
        );
        gov.insert_event(
            "polkadot",
            "referenda",
            1500,
            ReferendumEventRow {
                height: 28_399_000,
                timestamp: Some(ts("2025-10-20T00:00:00Z")),
                event_index: 5,
                kind: "submitted".into(),
                data: serde_json::json!({"index": 1500, "track": 34}),
            },
        );
        gov.insert_event(
            "polkadot",
            "referenda",
            1500,
            ReferendumEventRow {
                height: 28_400_000,
                timestamp: Some(ts("2025-10-25T00:00:00Z")),
                event_index: 2,
                kind: "decision_started".into(),
                data: serde_json::json!({"index": 1500, "track": 34}),
            },
        );
        gov.insert_referendum(
            "polkadot-asset-hub",
            ReferendumRow {
                class: "referenda".into(),
                referendum_id: 1500,
                track_id: None,
                status: "approved".into(),
                status_height: 10_300_000,
                proposal: None,
                proposal_hash: None,
                proposal_len: None,
                submitted_at_height: None,
            },
        );
        gov.insert_event(
            "polkadot-asset-hub",
            "referenda",
            1500,
            ReferendumEventRow {
                height: 10_300_000,
                timestamp: Some(ts("2025-11-10T00:00:00Z")),
                event_index: 7,
                kind: "approved".into(),
                data: serde_json::json!({"index": 1500}),
            },
        );
        gov.insert_referendum(
            "polkadot",
            ReferendumRow {
                class: "referenda".into(),
                referendum_id: 1400,
                track_id: Some(33),
                status: "rejected".into(),
                status_height: 27_000_000,
                proposal: None,
                proposal_hash: None,
                proposal_len: None,
                submitted_at_height: None,
            },
        );
        // post-migration deposit refund only — must NOT erase the relay verdict
        gov.insert_referendum(
            "polkadot-asset-hub",
            ReferendumRow {
                class: "referenda".into(),
                referendum_id: 1400,
                track_id: None,
                status: "unknown".into(),
                status_height: 0,
                proposal: None,
                proposal_hash: None,
                proposal_len: None,
                submitted_at_height: None,
            },
        );
        // ref 1500's proposal, decoded on AH (fetched post-migration)
        gov.insert_preimage(
            "polkadot-asset-hub",
            PreimageRow {
                proposal_hash: format!("0x{}", "ab".repeat(32)),
                len: 142,
                decode_status: "decoded".into(),
                source: "state".into(),
                call_summary: Some("utility.batch".into()),
                decoded_call: Some(serde_json::json!({
                    "call": "utility.batch",
                    "args": {"calls": [{"call": "system.remark", "args": {"remark": [104, 105]}}]}
                })),
                note: None,
                spec_version: Some(2_003_002),
                fetched_at_height: Some(10_400_000),
            },
        );
        // A whitelisted call that was DISPATCHED AND FAILED, with its
        // authorized call available under the SAME hash in gov.preimages —
        // which is the exact join `whitelist_call`'s preimage request creates.
        let wl_hash = format!("0x{}", "7c".repeat(32));
        gov.insert_preimage(
            "polkadot-asset-hub",
            PreimageRow {
                proposal_hash: wl_hash.clone(),
                len: 83,
                decode_status: "decoded".into(),
                source: "state".into(),
                call_summary: Some("system.set_code".into()),
                decoded_call: Some(serde_json::json!({
                    "call": "system.set_code", "args": {"code": [1, 2, 3]}
                })),
                note: None,
                spec_version: Some(2_003_002),
                fetched_at_height: Some(19_000_100),
            },
        );
        gov.insert_whitelisted_call(
            "polkadot-asset-hub",
            WhitelistedCallRow {
                call_hash: wl_hash.clone(),
                status: "dispatched".into(),
                dispatch_ok: Some(false),
                dispatch_error: Some(serde_json::json!({"Module": {"index": 31, "error": "0x02000000"}})),
                dispatch_height: Some(19_000_200),
                first_seen_height: 19_000_100,
                whitelisted_height: Some(19_000_100),
                status_height: 19_000_200,
                runtime_version: 2_003_002,
                mapper_version: 1,
            },
        );
        gov.insert_whitelist_event(
            "polkadot-asset-hub",
            &wl_hash,
            WhitelistEventRow {
                block_height: 19_000_100,
                event_index: 4,
                kind: "whitelisted".into(),
                dispatch_ok: None,
                dispatch_error: None,
                data: serde_json::json!({}),
                runtime_version: 2_003_002,
            },
        );
        gov.insert_whitelist_event(
            "polkadot-asset-hub",
            &wl_hash,
            WhitelistEventRow {
                block_height: 19_000_200,
                event_index: 7,
                kind: "dispatched".into(),
                dispatch_ok: Some(false),
                dispatch_error: Some(serde_json::json!({"Module": {"index": 31, "error": "0x02000000"}})),
                data: serde_json::json!({}),
                runtime_version: 2_003_002,
            },
        );
        // …and one that was whitelisted and never dispatched, which is a
        // terminal state rather than a pending one.
        gov.insert_whitelisted_call(
            "polkadot-asset-hub",
            WhitelistedCallRow {
                call_hash: format!("0x{}", "5d".repeat(32)),
                status: "whitelisted".into(),
                dispatch_ok: None,
                dispatch_error: None,
                dispatch_height: None,
                first_seen_height: 19_000_050,
                whitelisted_height: Some(19_000_050),
                status_height: 19_000_050,
                runtime_version: 2_003_002,
                mapper_version: 1,
            },
        );
        gov.insert_track(
            "polkadot-asset-hub",
            GovTrackRow {
                pallet: "referenda".into(),
                track_id: 0,
                name: "root".into(),
                params: serde_json::json!({"max_deciding": 1}),
                spec_version: 2_003_002,
            },
        );
        gov.insert_track(
            "polkadot-collectives",
            GovTrackRow {
                pallet: "fellowshipreferenda".into(),
                track_id: 1,
                name: "members".into(),
                params: serde_json::json!({"max_deciding": 10}),
                spec_version: 1_005_001,
            },
        );
        // the collision that made the chain-only answer ambiguous: a SECOND
        // instance on the same chain, same track id, different meaning
        gov.insert_track(
            "polkadot-collectives",
            GovTrackRow {
                pallet: "ambassadorreferenda".into(),
                track_id: 1,
                name: "ambassador".into(),
                params: serde_json::json!({"max_deciding": 10}),
                spec_version: 1_005_001,
            },
        );

        // votes on ref 1500, also spanning the migration: A votes aye on the
        // relay and WITHDRAWS on Asset Hub (its relay row stays 'active'
        // forever — nothing on the relay will ever touch it again), while B
        // votes nay on Asset Hub. The merge must let the withdrawal win.
        let voter_a = adapter_substrate::accounts::para_sovereign(1000);
        let voter_b = adapter_substrate::accounts::para_sovereign(2034);
        let delegator = adapter_substrate::accounts::para_sovereign(2004);
        let vote_row = |voter: &[u8; 32], active: bool, aye: &str, nay: &str, height: u64| VoteRow {
            class: "referenda".into(),
            referendum_id: 1500,
            voter: format!("0x{}", hex_lower(voter)),
            active,
            vote_type: "standard".into(),
            aye_balance: (aye != "0").then(|| "1000".to_string()),
            nay_balance: (nay != "0").then(|| "1000".to_string()),
            abstain_balance: None,
            conviction: Some(if aye != "0" { 3 } else { 0 }),
            conviction_label: Some(if aye != "0" { "locked3x" } else { "none" }.into()),
            aye_votes: aye.into(),
            nay_votes: nay.into(),
            support: if aye != "0" { "1000".into() } else { "0".into() },
            height,
        };
        gov.insert_vote("polkadot", vote_row(&voter_a, true, "3000", "0", 28_400_100));
        gov.insert_vote("polkadot-asset-hub", vote_row(&voter_b, true, "0", "100", 10_290_000));
        gov.insert_vote("polkadot-asset-hub", vote_row(&voter_a, false, "3000", "0", 10_295_000));
        gov.insert_delegation(
            "polkadot-asset-hub",
            DelegationRow {
                class: "referenda".into(),
                track_id: 34,
                delegator: format!("0x{}", hex_lower(&delegator)),
                target: Some(format!("0x{}", hex_lower(&voter_b))),
                active: true,
                height: 10_280_000,
            },
        );
        // THE PLUG-AND-PLAY FIXTURE: a fellowship referendum and a
        // rank-weighted vote on Collectives. Nothing here is chain-specific —
        // the class resolves to its own residency domain, which happens to
        // point at a chain the API has never heard of.
        gov.insert_referendum(
            "polkadot-collectives",
            ReferendumRow {
                class: "fellowship_referenda".into(),
                referendum_id: 300,
                track_id: Some(1),
                status: "approved".into(),
                status_height: 5_000_000,
                proposal: None,
                proposal_hash: None,
                proposal_len: None,
                submitted_at_height: Some(4_999_000),
            },
        );
        gov.insert_event(
            "polkadot-collectives",
            "fellowship_referenda",
            300,
            ReferendumEventRow {
                height: 5_000_000,
                timestamp: Some(ts("2026-07-01T00:00:00Z")),
                event_index: 3,
                kind: "approved".into(),
                data: serde_json::json!({"index": 300}),
            },
        );
        gov.insert_vote(
            "polkadot-collectives",
            VoteRow {
                class: "fellowship_referenda".into(),
                referendum_id: 300,
                voter: format!("0x{}", hex_lower(&delegator)),
                active: true,
                vote_type: "ranked".into(),
                aye_balance: None,
                nay_balance: None,
                abstain_balance: None,
                conviction: None,
                conviction_label: None,
                aye_votes: "9".into(),
                nay_votes: "0".into(),
                support: "0".into(),
                height: 4_999_500,
            },
        );

        // the anchor carries what no event does: the delegated amount
        gov.insert_voting_anchor(
            "polkadot-asset-hub",
            &delegator,
            VotingAnchorRow {
                class: "referenda".into(),
                track_id: 34,
                height: 10_290_000,
                mode: "delegating".into(),
                delegating_target: Some(format!("0x{}", hex_lower(&voter_b))),
                delegating_balance: Some("5000".into()),
                delegating_conviction_label: Some("locked6x".into()),
                delegations_votes: Some("0".into()),
                delegations_capital: Some("0".into()),
                spec_version: Some(2_003_002),
                note: None,
            },
        );

        // treasury: one modern asset spend that STRADDLES the migration —
        // approved on the relay, paid on Asset Hub — plus a pot deposit that
        // names no spend, and a Fellowship sub-treasury spend on Collectives.
        let treasury = Arc::new(MemoryTreasuryIndex::new());
        let usdt = serde_json::json!({"V4": {"asset_id": {"parents": 0, "interior":
            {"X2": [{"PalletInstance": 50}, {"GeneralIndex": 1984}]}}}});
        let payee = adapter_substrate::accounts::para_sovereign(2034);
        let payee_hex = format!("0x{}", hex_lower(&payee));
        treasury.insert_spend(
            "polkadot",
            SpendRow {
                instance: "treasury".into(),
                spend_kind: "asset_spend".into(),
                spend_id: 313,
                status: "approved".into(),
                amount: Some("83760000000".into()),
                slashed: None,
                asset_kind: Some(usdt.clone()),
                beneficiary: Some(payee_hex.clone()),
                beneficiary_location: Some(serde_json::json!({"V4": {"parents": 0}})),
                payment_id: None,
                valid_from: Some(28_000_000),
                expire_at: Some(28_900_000),
                // the normalized halves the slice-6 mapper writes: USDT on
                // Asset Hub, named from the RELAY (chain = Parachain(1000))
                asset_ref: Some(serde_json::json!({
                    "location": {
                        "chain": {"parents": 0, "interior": [{"Parachain": 1000}]},
                        "asset": {"parents": 0, "interior": [
                            {"PalletInstance": 50}, {"GeneralIndex": 1984}
                        ]},
                    },
                    "key": null,
                })),
                first_seen_height: 28_000_000,
                status_height: 28_000_000,
            },
        );
        treasury.insert_event(
            "polkadot",
            "treasury",
            Some(("asset_spend".into(), 313)),
            SpendEventRow {
                height: 28_000_000,
                timestamp: Some(ts("2025-10-01T00:00:00Z")),
                event_index: 4,
                kind: "approved".into(),
                amount: Some("83760000000".into()),
                data: serde_json::json!({"index": 313}),
            },
        );
        // …and the Asset Hub row is what the INDEXER really produces for the
        // second half of a straddling spend: status and payment only. The
        // amount, asset kind and beneficiary exist solely on the relay row,
        // because only `AssetSpendApproved` carries them.
        treasury.insert_spend(
            "polkadot-asset-hub",
            SpendRow {
                instance: "treasury".into(),
                spend_kind: "asset_spend".into(),
                spend_id: 313,
                status: "paid".into(),
                amount: None,
                slashed: None,
                asset_kind: None,
                beneficiary: None,
                beneficiary_location: None,
                payment_id: Some("5551".into()),
                valid_from: None,
                expire_at: None,
                asset_ref: None,
                first_seen_height: 10_400_000,
                status_height: 10_400_000,
            },
        );
        treasury.insert_event(
            "polkadot-asset-hub",
            "treasury",
            Some(("asset_spend".into(), 313)),
            SpendEventRow {
                height: 10_400_000,
                timestamp: Some(ts("2026-01-05T00:00:00Z")),
                event_index: 2,
                kind: "paid".into(),
                amount: None,
                data: serde_json::json!({"index": 313, "payment_id": 5551}),
            },
        );
        treasury.insert_event(
            "polkadot-asset-hub",
            "treasury",
            None,
            SpendEventRow {
                height: 10_400_100,
                timestamp: Some(ts("2026-01-05T01:00:00Z")),
                event_index: 0,
                kind: "pot_deposit".into(),
                amount: Some("1204000000000".into()),
                data: serde_json::json!({"value": 1204000000000u64}),
            },
        );
        treasury.insert_spend(
            "polkadot-collectives",
            SpendRow {
                instance: "fellowship_treasury".into(),
                spend_kind: "asset_spend".into(),
                spend_id: 7,
                status: "processed".into(),
                amount: Some("1000000000".into()),
                slashed: None,
                asset_kind: None,
                beneficiary: None,
                beneficiary_location: None,
                payment_id: None,
                valid_from: None,
                expire_at: None,
                asset_ref: None,
                first_seen_height: 5_100_000,
                status_height: 5_100_500,
            },
        );

        // ---- assets + treasury accounts (slice 6) -----------------------
        // USDT as Asset Hub really names it: TrustBacked 1984, six decimals,
        // and the XCM location a treasury spend refers to it BY.
        let assets = Arc::new(MemoryAssetIndex::new());
        assets.insert(
            "polkadot-asset-hub",
            AssetRow {
                asset_key: "assets:1984".into(),
                representation_kind: "trust_backed".into(),
                symbol: Some("USDT".into()),
                name: Some("Tether USD".into()),
                decimals: Some(6),
                supply: Some("100000000000".into()),
                status: Some("Live".into()),
                location_key: Some(
                    adapter_substrate::assets::canonical_location(
                        &adapter_substrate::assets::local_asset_location(50, 1984),
                    )
                    .expect("canonical"),
                ),
                xcm_location: Some(adapter_substrate::assets::local_asset_location(50, 1984)),
            },
        );

        // the native token is an asset row too — otherwise a treasury's DOT
        // holding is an integer with no unit beside a USDT holding that has one
        assets.insert(
            "polkadot-asset-hub",
            AssetRow {
                asset_key: "native".into(),
                representation_kind: "native".into(),
                symbol: Some("DOT".into()),
                name: Some("DOT".into()),
                decimals: Some(10),
                supply: None,
                status: None,
                location_key: Some(r#"{"interior":[],"parents":0}"#.into()),
                xcm_location: Some(serde_json::json!({"parents": 0, "interior": []})),
            },
        );

        let holder = adapter_substrate::accounts::pallet_account(b"py/trsry");
        treasury.insert_account(
            "polkadot",
            TreasuryAccountRow {
                chain_id: "polkadot-asset-hub".into(),
                account_id: holder.to_vec(),
                role: "pot".into(),
                instance: Some("treasury".into()),
                label: "Treasury pot (py/trsry)".into(),
                derivation: Some("modl:py/trsry".into()),
                source: "derived".into(),
                ss58: Some("13UVJyLnbVp9RBZYFwFGyDvVd1y27Tt8tkntv6Q7JVPhFsTB".into()),
            },
        );
        // an anchored USDT position, plus one transfer out after the anchor…
        balances.insert_anchor(
            "polkadot-asset-hub",
            &holder,
            "assets:1984",
            BalanceAnchorRow {
                height: 19_400_000,
                free: "20895000000".into(),
                reserved: "0".into(),
                total: "20895000000".into(),
                spec_version: Some(2003002),
                source: "treasury-holdings".into(),
                note: None,
                status: Some("liquid".into()),
            },
        );
        balances.insert_change(
            "polkadot-asset-hub",
            &holder,
            "assets:1984",
            BalanceChangeRow {
                height: 19_400_100,
                timestamp: Some(ts("2026-08-01T00:00:00Z")),
                event_index: 3,
                delta: "-895000000".into(),
                reason: "transfer_out".into(),
                counterparty: None,
            },
        );
        // …and an asset this account has only ever been seen MOVING, never
        // anchored: it must appear with a null amount, not vanish
        balances.insert_change(
            "polkadot-asset-hub",
            &holder,
            "assets:1337",
            BalanceChangeRow {
                height: 19_400_050,
                timestamp: Some(ts("2026-08-01T00:00:00Z")),
                event_index: 1,
                delta: "4800000000".into(),
                reason: "transfer_in".into(),
                counterparty: None,
            },
        );

        // ---- bounties (slice 7) -----------------------------------------
        // Bounty 22 STRADDLES the migration: proposed on the relay (where it
        // also paid a child out once), active on Asset Hub. A child bounty of
        // it, and a modern multi-asset bounty paid in USDT — the three
        // instances a treasury page has to show as one thing.
        let bounties = Arc::new(MemoryBountyIndex::new());
        let curator = adapter_substrate::accounts::para_sovereign(1000);
        let bounty_account = adapter_substrate::accounts::sub_account(
            b"py/trsry",
            &[
                adapter_substrate::accounts::SubKey::Str("bt"),
                adapter_substrate::accounts::SubKey::Index(22),
            ],
        )
        .expect("derives");
        bounties.insert_bounty(
            "polkadot",
            BountyRow {
                instance: "bounties".into(),
                bounty_id: 22,
                child_id: None,
                status: "proposed".into(),
                value: Some("100000000000".into()),
                paid_out: Some("250".into()),
                bond: None,
                curator: None,
                beneficiary: None,
                beneficiary_location: None,
                payment_id: None,
                account_id: None,
                first_seen_height: 27_000_000,
                status_height: 27_000_000,
                asset_ref: None,
            },
        );
        bounties.insert_event(
            "polkadot",
            "bounties",
            (22, None),
            BountyEventRow {
                height: 27_000_000,
                timestamp: Some(ts("2025-09-01T00:00:00Z")),
                event_index: 1,
                kind: "proposed".into(),
                amount: None,
                data: serde_json::json!({"index": 22}),
            },
        );
        bounties.insert_bounty(
            "polkadot-asset-hub",
            BountyRow {
                instance: "bounties".into(),
                bounty_id: 22,
                child_id: None,
                status: "active".into(),
                value: None,
                paid_out: Some("1000".into()),
                bond: None,
                curator: Some(format!("0x{}", hex_lower(&curator))),
                beneficiary: None,
                beneficiary_location: None,
                payment_id: None,
                account_id: Some(format!("0x{}", hex_lower(&bounty_account))),
                first_seen_height: 10_500_000,
                status_height: 10_500_000,
                asset_ref: None,
            },
        );
        bounties.insert_event(
            "polkadot-asset-hub",
            "bounties",
            (22, None),
            BountyEventRow {
                height: 10_500_000,
                timestamp: Some(ts("2026-02-01T00:00:00Z")),
                event_index: 0,
                kind: "curator_accepted".into(),
                amount: None,
                data: serde_json::json!({"bounty_id": 22}),
            },
        );
        bounties.insert_bounty(
            "polkadot-asset-hub",
            BountyRow {
                instance: "child_bounties".into(),
                bounty_id: 22,
                child_id: Some(3),
                status: "claimed".into(),
                value: None,
                paid_out: Some("500".into()),
                bond: None,
                curator: None,
                beneficiary: Some(payee_hex.clone()),
                beneficiary_location: None,
                payment_id: None,
                account_id: None,
                first_seen_height: 10_600_000,
                status_height: 10_600_000,
                asset_ref: None,
            },
        );
        // the modern generation: one id space, asset-denominated, and its
        // payout names USDT exactly as a treasury spend does
        bounties.insert_bounty(
            "polkadot-asset-hub",
            BountyRow {
                instance: "multi_asset_bounties".into(),
                bounty_id: 1,
                child_id: None,
                status: "claimed".into(),
                value: Some("83760000000".into()),
                paid_out: Some("83760000000".into()),
                bond: None,
                curator: None,
                beneficiary: Some(payee_hex.clone()),
                beneficiary_location: None,
                payment_id: None,
                account_id: None,
                first_seen_height: 10_700_000,
                status_height: 10_700_000,
                asset_ref: Some(serde_json::json!({
                    "location": {
                        // `Here`: the chain that emitted the event holds it
                        "chain": {"parents": 0, "interior": []},
                        "asset": {"parents": 0, "interior": [
                            {"PalletInstance": 50}, {"GeneralIndex": 1984}
                        ]},
                    },
                    "key": null,
                    "kind": {"V4": {"asset_id": {"parents": 0, "interior": {"X2": [
                        {"PalletInstance": 50}, {"GeneralIndex": 1984}
                    ]}}}},
                })),
            },
        );

        AppState {
            registry,
            blocks,
            labels,
            balances,
            gov,
            treasury,
            bounties,
            assets,
            sim,
            xcm,
            parse_account: Arc::new(|s| {
                adapter_substrate::accounts::parse_account(s).map(|a| a.to_vec())
            }),
        }
    }

    async fn get_json(app: &Router, uri: &str) -> (StatusCode, serde_json::Value) {
        let res = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = res.status();
        let body = axum::body::to_bytes(res.into_body(), 1_000_000).await.unwrap();
        let json = if body.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null)
        };
        (status, json)
    }

    #[tokio::test]
    async fn fixture_block_roundtrips_with_lineage() {
        let app = router(test_state().await);
        let (status, json) = get_json(&app, "/v1/blocks/polkadot-asset-hub/19000001").await;
        assert_eq!(status, StatusCode::OK);
        // THE Phase 0 exit criterion: lineage visible at the API surface
        assert_eq!(json["lineage"]["runtime_version"], 2_000_006);
        assert_eq!(json["lineage"]["decoder_version"], 1);
        assert!(json["lineage"]["raw_location"]
            .as_str()
            .unwrap()
            .starts_with("raw/"));
        assert_eq!(json["transactions"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn unknown_chain_and_missing_block_are_404() {
        let app = router(test_state().await);
        let (s1, _) = get_json(&app, "/v1/blocks/no-such-chain/1").await;
        assert_eq!(s1, StatusCode::NOT_FOUND);
        let (s2, _) = get_json(&app, "/v1/blocks/polkadot/1").await;
        assert_eq!(s2, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn domain_resolution_is_migration_aware_over_http() {
        let app = router(test_state().await);
        let (_, before) =
            get_json(&app, "/v1/domains/polkadot/governance?at=2025-06-01T00:00:00Z").await;
        assert_eq!(before["chain"], "polkadot");
        let (_, after) =
            get_json(&app, "/v1/domains/polkadot/governance?at=2026-01-27T12:00:00Z").await;
        assert_eq!(after["chain"], "polkadot-asset-hub");
    }

    #[tokio::test]
    async fn treasury_account_appears_named_by_ss58_and_hex() {
        let app = router(test_state().await);
        // by SS58 (the ECOSYSTEM.md golden address)
        let (status, json) = get_json(
            &app,
            "/v1/accounts/polkadot-asset-hub/13UVJyLnbVp9RBZYFwFGyDvVd1y27Tt8tkntv6Q7JVPhFsTB/labels",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["labels"][0]["label"], "Treasury (py/trsry)");
        assert_eq!(json["labels"][0]["kind"], "pallet");

        // same account by 0x-hex resolves identically
        let hex_addr = json["account_id"].as_str().unwrap().to_string();
        let (s2, j2) =
            get_json(&app, &format!("/v1/accounts/polkadot-asset-hub/{hex_addr}/labels")).await;
        assert_eq!(s2, StatusCode::OK);
        assert_eq!(j2["labels"], json["labels"]);

        // corrupted address → 400, unknown-but-valid account → empty labels
        let (s3, _) = get_json(&app, "/v1/accounts/polkadot-asset-hub/13UVJyLnbVpXXX/labels").await;
        assert_eq!(s3, StatusCode::BAD_REQUEST);
        let (s4, j4) = get_json(
            &app,
            &format!("/v1/accounts/polkadot/{hex_addr}/labels"),
        )
        .await;
        assert_eq!(s4, StatusCode::OK);
        assert_eq!(j4["labels"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn balance_history_stitches_across_the_migration_boundary() {
        let app = router(test_state().await);
        let (status, json) = get_json(
            &app,
            "/v1/balances/polkadot/13UVJyLnbVp9RBZYFwFGyDvVd1y27Tt8tkntv6Q7JVPhFsTB/history",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let segments = json["segments"].as_array().unwrap();
        // two residency windows: relay until 2025-11-04, AH after — in order
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0]["chain"], "polkadot");
        assert_eq!(segments[1]["chain"], "polkadot-asset-hub");
        assert_eq!(segments[0]["to"], "2025-11-04T00:00:00Z");

        // relay-era change appears in the relay segment, no anchor → null running
        let relay_changes = segments[0]["changes"].as_array().unwrap();
        assert_eq!(relay_changes.len(), 1);
        assert_eq!(relay_changes[0]["delta"], "-1000");
        assert!(relay_changes[0]["running_total"].is_null());

        // AH segment: anchor (end of 10_000_000, total 500) seeds the running
        // total for the later change: 500 + 200 = 700
        let ah = &segments[1];
        assert_eq!(ah["anchors"][0]["total"], "500");
        let ah_changes = ah["changes"].as_array().unwrap();
        assert_eq!(ah_changes.len(), 1);
        assert_eq!(ah_changes[0]["delta"], "200");
        assert_eq!(ah_changes[0]["running_total"], "700");

        // unknown network is a 404, not an empty guess
        let (s2, _) = get_json(
            &app,
            "/v1/balances/nowhere/13UVJyLnbVp9RBZYFwFGyDvVd1y27Tt8tkntv6Q7JVPhFsTB/history",
        )
        .await;
        assert_eq!(s2, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn referendum_timeline_stitches_across_the_migration_boundary() {
        let app = router(test_state().await);
        let (status, json) = get_json(&app, "/v1/gov/polkadot/referenda/1500").await;
        assert_eq!(status, StatusCode::OK);

        // merged summary: AH verdict wins, relay-era submission facts coalesce
        let r = &json["referendum"];
        assert_eq!(r["status"], "approved");
        assert_eq!(r["track_id"], 34);
        assert_eq!(r["submitted_at_height"], 28_399_000);
        assert_eq!(r["proposal_hash"], format!("0x{}", "ab".repeat(32)));

        // segments in residency order: relay window then AH window
        let segments = json["segments"].as_array().unwrap();
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0]["chain"], "polkadot");
        assert_eq!(segments[0]["events"].as_array().unwrap().len(), 2);
        assert_eq!(segments[0]["events"][0]["kind"], "submitted");
        assert_eq!(segments[1]["chain"], "polkadot-asset-hub");
        assert_eq!(segments[1]["events"][0]["kind"], "approved");

        // the decoded call tree rides along (found on AH via the residency walk)
        assert_eq!(json["preimage"]["decode_status"], "decoded");
        assert_eq!(json["preimage"]["call_summary"], "utility.batch");
        assert_eq!(
            json["preimage"]["decoded_call"]["args"]["calls"][0]["call"],
            "system.remark"
        );

        // an 'unknown' post-migration row must not erase the relay verdict
        let (_, j1400) = get_json(&app, "/v1/gov/polkadot/referenda/1400").await;
        assert_eq!(j1400["referendum"]["status"], "rejected");
        assert_eq!(j1400["referendum"]["track_id"], 33);
        assert!(j1400["preimage"].is_null(), "no preimage row for 1400");

        // unindexed referendum → 404; unknown network → 404
        let (s2, _) = get_json(&app, "/v1/gov/polkadot/referenda/999999").await;
        assert_eq!(s2, StatusCode::NOT_FOUND);
        let (s3, _) = get_json(&app, "/v1/gov/nowhere/referenda/1500").await;
        assert_eq!(s3, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn a_referendum_carries_what_it_would_do_beside_what_it_does() {
        let app = router(test_state().await);
        let (status, json) = get_json(&app, "/v1/gov/polkadot/referenda/1500").await;
        assert_eq!(status, StatusCode::OK);

        // The request names polkadot and a referendum id — never a chain — yet
        // the preview recorded on Asset Hub is found, through the same residency
        // walk the preimage uses.
        let sims = json["simulations"].as_array().expect("simulations list");
        assert_eq!(sims.len(), 1);
        assert_eq!(sims[0]["chain_id"], "polkadot-asset-hub");
        assert_eq!(sims[0]["call_summary"], "multiassetbounties.fund_bounty");

        // A failed dispatch is a RESULT and must read as one: an answer with a
        // reason, not an empty response that looks like nothing happened.
        assert_eq!(sims[0]["status"], "dispatch_failed");
        assert_eq!(sims[0]["dispatch_ok"], false);
        assert_eq!(sims[0]["dispatch_error"]["error"], "assets.NoAccount");
        // and it carries its own lineage, like every other row we serve
        assert_eq!(sims[0]["spec_version"], 2_003_002);
        assert_eq!(sims[0]["api_version"], 2);
        assert_eq!(sims[0]["at_block_hash"], format!("0x{}", "cd".repeat(32)));
        assert_eq!(sims[0]["metadata_version"], 15);
        assert!(sims[0]["raw_location"]
            .as_str()
            .unwrap()
            .ends_with("DryRunApi_dry_run_call.response.scale"));

        // the limits ship WITH the answer, every time
        let gaps = json["simulation_coverage"]["not_covered"]
            .as_array()
            .expect("not_covered list");
        assert!(gaps.iter().any(|g| g.as_str().unwrap().contains("NOT the state at enactment")));
        assert!(gaps.iter().any(|g| g.as_str().unwrap().contains("proposal_origin")));

        // A referendum we HAVE no proposal hash for must not be described as one
        // nobody previewed: we never looked, and saying otherwise would assert
        // something unchecked. Ref 1400 has no hash on either chain.
        let (_, j1400) = get_json(&app, "/v1/gov/polkadot/referenda/1400").await;
        assert!(j1400["simulations"].as_array().unwrap().is_empty());
        let why = j1400["simulation_coverage"]["recorded_only"].as_str().unwrap();
        assert!(why.contains("no proposal hash indexed yet"), "{why}");
        assert!(
            !why.contains("no Tier 1 preview has been run"),
            "an empty list because we could not look is a different claim from an empty \
             list because we looked and found nothing: {why}"
        );
    }

    #[tokio::test]
    async fn an_xcm_id_returns_the_halves_we_have_and_refuses_to_call_them_a_journey() {
        let app = router(test_state().await);
        let id = format!("0x{}", "ee".repeat(32));

        // Both halves, on two chains, found by id alone — no chain named.
        let (status, json) = get_json(&app, &format!("/v1/xcm/messages/{}", id.to_uppercase())).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["message_id"], id, "0X… normalises like every other hash");
        let obs = json["observations"].as_array().unwrap();
        assert_eq!(obs.len(), 2);
        assert_eq!(obs[0]["chain_id"], "hydration", "chain-ordered, C collation");
        assert_eq!(obs[0]["side"], "received");
        assert_eq!(obs[1]["chain_id"], "polkadot-asset-hub");
        assert_eq!(obs[1]["side"], "sent");

        // THE CLAIM THIS ENDPOINT REFUSES TO MAKE: the two rows are evidence,
        // not an assertion that they are one message.
        let reads_as = json["reads_as"].as_str().unwrap();
        assert!(reads_as.contains("still not an assertion"), "{reads_as}");
        // and the two ids are labelled differently, because they ARE different
        assert_eq!(obs[1]["id_kind"], "topic");
        assert_eq!(obs[0]["id_kind"], "ambiguous");

        // An id nobody saw is not "the message does not exist".
        let (_, none) = get_json(&app, &format!("/v1/xcm/messages/0x{}", "99".repeat(32))).await;
        assert!(none["observations"].as_array().unwrap().is_empty());
        assert!(none["reads_as"].as_str().unwrap().contains("unindexed chain"));

        // Per-chain listing: newest first, and an unknown chain 404s.
        let (_, ah) = get_json(&app, "/v1/xcm/polkadot-asset-hub/messages").await;
        let rows = ah["messages"].as_array().unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0]["block_height"], 19_000_900);
        assert_eq!(rows[0]["event_index"], 3, "newest block first, then event order");
        assert_eq!(rows[2]["block_height"], 19_000_100);
        assert!(ah["coverage"]["not_covered"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s.as_str().unwrap().contains("one-sided OBSERVATIONS")));
        let (s, _) = get_json(&app, "/v1/xcm/nowhere/messages").await;
        assert_eq!(s, StatusCode::NOT_FOUND);
    }

    /// THE SLICE'S HEADLINE, and the thing slice 2 could not do: a wire hash and
    /// a topic are the same message, so they must resolve to the same journey —
    /// and the journey must show what it rests on rather than asserting it.
    #[tokio::test]
    async fn a_wire_hash_and_a_topic_resolve_to_one_journey_that_shows_its_working() {
        let app = router(test_state().await);
        let topic = format!("0x{}", "ee".repeat(32));
        let wire = format!("0x{}", "77".repeat(32));

        let (status, j) = get_json(&app, &format!("/v1/xcm/journeys/{}", topic.to_uppercase()))
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(j["message_id"], topic, "0X… normalises like every other hash");
        assert_eq!(j["shape"], "send_and_receive");
        assert_eq!(j["chains"], 2, "no request named a chain");

        // Ordered by BLOCK TIMESTAMP, which is the only clock two chains share.
        let steps = j["steps"].as_array().unwrap();
        assert_eq!(steps.len(), 3, "both sending ids plus the receiving half");
        assert_eq!((steps[0]["chain"].as_str(), steps[0]["event_index"].as_u64()),
                   (Some("polkadot-asset-hub"), Some(3)));
        assert_eq!(steps[0]["id_kind"], "wire_hash");
        assert_eq!(steps[1]["id_kind"], "topic");
        assert_eq!(steps[2]["chain"], "hydration");
        assert_eq!(steps[2]["id_kind"], "ambiguous");
        // Invariant 3: every step carries the lineage of the row it came from.
        assert!(steps.iter().all(|s| s["lineage"]["runtime_version"] == 2_003_002));

        // THE STITCH IS AUDITABLE. The wire row reached this journey through a
        // recorded link, and the link's rule, confidence and evidence ship with
        // the answer — a stitch you cannot check is a stitch you cannot trust.
        let aliases = j["aliases"].as_array().unwrap();
        assert_eq!(aliases.len(), 1);
        assert_eq!(aliases[0]["rule"], "unique_in_block");
        assert_eq!(aliases[0]["confidence"], "high");
        assert_eq!(aliases[0]["evidence"]["event_gap"], 1);
        let ids = j["ids"].as_array().unwrap();
        assert_eq!(ids.len(), 2);
        assert_eq!(j["alias_limit_reached"], false);

        // The checks can come back CONTRADICTED in public; here they do not.
        assert_eq!(j["checks"]["time_order"]["status"], "ok");
        assert_eq!(
            j["checks"]["counterparty_mirror"]["status"], "corroborated",
            "Asset Hub says para:2034 and Hydration says para:1000 — mirror images, from \
             registry para ids alone"
        );
        assert_eq!(j["checks"]["id_uniqueness"]["status"], "ok");

        // Delivery and intent are different facts, and the payload keeps them so.
        assert_eq!(j["outcome"]["delivered"], true);
        assert_eq!(j["outcome"]["receiver_success"], true);
        assert!(j["outcome"]["note"].as_str().unwrap().contains("not a claim"));

        // THE DEAD END SLICE 2 MEASURED, now closed: asking by the WIRE hash
        // returns the same journey, because the alias expansion runs first.
        let (_, by_wire) = get_json(&app, &format!("/v1/xcm/journeys/{wire}")).await;
        assert_eq!(by_wire["shape"], "send_and_receive");
        assert_eq!(by_wire["steps"].as_array().unwrap().len(), 3);
        assert_eq!(by_wire["message_id"], wire, "asked by the id you typed");
        // …while the plain observation endpoint still answers only about the id
        // it was given. Two endpoints, two questions, neither pretending.
        let (_, obs) = get_json(&app, &format!("/v1/xcm/messages/{wire}")).await;
        assert_eq!(obs["observations"].as_array().unwrap().len(), 1);
        assert_eq!(obs["journey"], format!("/v1/xcm/journeys/{wire}"));

        // A wire hash from a block that recorded NO link reaches only its own
        // half — the refusal is the feature, and it renders as send_only rather
        // than as a journey with a missing end.
        let (_, lone) = get_json(&app, &format!("/v1/xcm/journeys/0x{}", "22".repeat(32))).await;
        assert_eq!(lone["shape"], "send_only");
        assert!(lone["aliases"].as_array().unwrap().is_empty());
        assert_eq!(lone["ids"].as_array().unwrap().len(), 1);
        assert_eq!(lone["checks"]["time_order"]["status"], "unknown");

        // An id nobody saw is not "the message does not exist".
        let (_, unseen) = get_json(&app, &format!("/v1/xcm/journeys/0x{}", "99".repeat(32))).await;
        assert_eq!(unseen["shape"], "unseen");
        assert!(unseen["steps"].as_array().unwrap().is_empty());
        assert!(unseen["reads_as"].as_str().unwrap().contains("chain we do not map"));

        // The honest limits ship in the payload, including the rule we did NOT
        // write and why its window may be empty.
        let gaps = j["coverage"]["not_covered"].as_array().unwrap();
        assert!(gaps.iter().any(|g| g.as_str().unwrap().contains("NO hop rule")));
        assert!(gaps.iter().any(|g| g.as_str().unwrap().contains("hash of CONTENT")));
        // AND IT NEVER SENDS THE READER TO THE ENDPOINT THEY ARE ALREADY ON. The
        // observations endpoint's own first line says "nothing on THIS endpoint
        // asserts it — /v1/xcm/journeys/{id} is the endpoint that does", which is
        // correct there and wrong here in both halves. It shipped that way because
        // the two lists share a helper, which is precisely how the same class of
        // stale line reached the review one endpoint over. Assert the property, not
        // the wording, so a future edit to either list cannot reintroduce it.
        assert!(
            !gaps
                .iter()
                .any(|g| g.as_str().unwrap().contains("/v1/xcm/journeys/{id} is the endpoint")),
            "the journey endpoint's not_covered must not defer to the journey endpoint"
        );
    }

    #[tokio::test]
    async fn simulations_are_served_by_call_hash_and_never_started_by_a_get() {
        let app = router(test_state().await);
        // upper case, 0X prefix — normalized the same way the whitelist hash is
        let (status, json) = get_json(
            &app,
            &format!("/v1/sim/polkadot-asset-hub/calls/0X{}", "AB".repeat(32)),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["call_hash"], format!("0x{}", "ab".repeat(32)));
        assert_eq!(json["simulations"].as_array().unwrap().len(), 1);
        assert_eq!(json["simulations"][0]["origin_spec"], "Origins:MediumSpender");
        assert_eq!(json["simulations"][0]["event_count"], 1);

        // The same call on the chain it was NOT simulated on is empty — a
        // simulation belongs to one runtime and one state, and this endpoint
        // will not borrow another chain's answer.
        let (_, relay) = get_json(
            &app,
            &format!("/v1/sim/polkadot/calls/0x{}", "ab".repeat(32)),
        )
        .await;
        assert!(relay["simulations"].as_array().unwrap().is_empty());
        assert!(relay["coverage"]["recorded_only"]
            .as_str()
            .unwrap()
            .contains("never starts one"));

        let (s, _) = get_json(
            &app,
            &format!("/v1/sim/nowhere/calls/0x{}", "ab".repeat(32)),
        )
        .await;
        assert_eq!(s, StatusCode::NOT_FOUND, "an unknown chain is refused, not empty");
    }

    #[tokio::test]
    async fn referenda_list_merges_windows_and_orders_desc() {
        let app = router(test_state().await);
        let (status, json) = get_json(&app, "/v1/gov/polkadot/referenda?limit=10").await;
        assert_eq!(status, StatusCode::OK);
        let rows = json["referenda"].as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["referendum_id"], 1500);
        assert_eq!(rows[0]["status"], "approved");
        assert_eq!(rows[1]["referendum_id"], 1400);
        assert_eq!(rows[1]["status"], "rejected", "unknown must not clobber");
    }

    /// The slice's whole point, asserted rather than documented: a whitelisted
    /// call that was dispatched and REVERTED must not read as enacted.
    #[tokio::test]
    async fn a_dispatched_whitelisted_call_that_failed_is_never_reported_as_enacted() {
        let app = router(test_state().await);
        let hash = format!("0x{}", "7c".repeat(32));
        let (status, json) = get_json(&app, &format!("/v1/gov/polkadot/whitelist/{hash}")).await;
        assert_eq!(status, StatusCode::OK);

        // it WAS dispatched — storage was cleaned, the fee was charged
        assert_eq!(json["call"]["status"], "dispatched");
        // …and it did NOT work, which is a different fact
        assert_eq!(json["call"]["dispatch_ok"], false);
        assert!(
            json["enacted"].as_str().unwrap().contains("FAILED"),
            "the payload must say so in words, not leave it to a boolean nobody reads: {}",
            json["enacted"]
        );
        assert_eq!(json["call"]["dispatch_error"]["Module"]["index"], 31);

        // the authorized call is joined from gov.preimages BY THE SAME HASH —
        // exact, because whitelist_call requests the preimage of what it
        // whitelists, so a whitelisted hash IS a preimage hash
        assert_eq!(json["authorized_call"]["call_summary"], "system.set_code");
        assert_eq!(json["authorized_call"]["decoded_call"]["call"], "system.set_code");

        // full history, oldest first, on the chain governance currently lives on
        let seg = json["segments"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["chain"] == "polkadot-asset-hub")
            .expect("an asset hub segment");
        let events = seg["events"].as_array().unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["kind"], "whitelisted");
        assert_eq!(events[1]["kind"], "dispatched");
        assert_eq!(events[1]["dispatch_ok"], false);

        // and the gaps are stated in the payload, not in a doc
        let nc = json["coverage"]["not_covered"].as_array().unwrap();
        assert_eq!(nc.len(), 4);
        assert!(nc.iter().any(|s| s.as_str().unwrap().contains("XCM Transact")));
    }

    /// A hash that was whitelisted and never dispatched is TERMINAL, not
    /// pending — three failure modes emit no event at all. And the request
    /// never names a chain (Invariant 2): `polkadot` resolves through
    /// governance residency to Asset Hub.
    #[tokio::test]
    async fn a_whitelisted_call_with_no_dispatch_is_terminal_and_no_chain_is_named() {
        let app = router(test_state().await);
        let hash = format!("0x{}", "5d".repeat(32));
        // upper-case and bare forms must resolve identically to the stored form
        for form in [hash.clone(), hash.to_uppercase(), hash.trim_start_matches("0x").to_string()] {
            let (status, json) =
                get_json(&app, &format!("/v1/gov/polkadot/whitelist/{form}")).await;
            assert_eq!(status, StatusCode::OK, "form {form} should resolve");
            assert_eq!(json["call"]["status"], "whitelisted");
            assert_eq!(json["call"]["dispatch_ok"], serde_json::Value::Null);
            assert!(json["enacted"].as_str().unwrap().contains("terminal state"));
            assert_eq!(json["call_hash"], hash);
        }

        // the list surface, both rows, most recently moved first
        let (status, list) = get_json(&app, "/v1/gov/polkadot/whitelist").await;
        assert_eq!(status, StatusCode::OK);
        let calls = list["segments"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["chain"] == "polkadot-asset-hub")
            .expect("an asset hub segment")["calls"]
            .as_array()
            .unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0]["status_height"], 19_000_200);
        assert_eq!(calls[1]["status_height"], 19_000_050);

        // an unknown hash is a 404, not an empty success
        let (status, _) = get_json(
            &app,
            &format!("/v1/gov/polkadot/whitelist/0x{}", "00".repeat(32)),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn tracks_resolve_via_governance_residency() {
        let app = router(test_state().await);
        // now (2026): governance lives on Asset Hub
        let (status, json) = get_json(&app, "/v1/gov/polkadot/tracks").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["chain"], "polkadot-asset-hub");
        assert_eq!(json["tracks"][0]["track_id"], 0);
        assert_eq!(json["tracks"][0]["name"], "root");

        // pre-migration: resolves to the relay (which has no synced tracks here)
        let (_, before) = get_json(&app, "/v1/gov/polkadot/tracks?at=2025-06-01T00:00:00Z").await;
        assert_eq!(before["chain"], "polkadot");
        assert_eq!(before["tracks"].as_array().unwrap().len(), 0);

        // …and the fellowship instance resolves to ITS chain, which never
        // migrated — same endpoint, same residency machinery, different domain
        let (fs, fellowship) =
            get_json(&app, "/v1/gov/polkadot/tracks?class=fellowship_referenda").await;
        assert_eq!(fs, StatusCode::OK);
        assert_eq!(fellowship["chain"], "polkadot-collectives");
        assert_eq!(fellowship["class"], "fellowship_referenda");
        // Collectives hosts a SECOND instance whose track 1 is a different
        // thing entirely; the class's own pallet is what disambiguates
        assert_eq!(fellowship["pallet"], "fellowshipreferenda");
        let rows = fellowship["tracks"].as_array().unwrap();
        assert_eq!(rows.len(), 1, "the ambassador instance must not leak in");
        assert_eq!(rows[0]["pallet"], "fellowshipreferenda");
        assert_eq!(rows[0]["name"], "members");
    }

    #[tokio::test]
    async fn referendum_votes_stitch_windows_and_tally_only_active_votes() {
        let app = router(test_state().await);
        let (status, json) = get_json(&app, "/v1/gov/polkadot/referenda/1500/votes").await;
        assert_eq!(status, StatusCode::OK);

        // both residency windows reported, with per-window coverage
        let segments = json["segments"].as_array().unwrap();
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0]["chain"], "polkadot");
        assert_eq!(segments[0]["votes_indexed"], 1);
        assert_eq!(segments[1]["chain"], "polkadot-asset-hub");
        assert_eq!(segments[1]["votes_indexed"], 2);

        // THE cross-migration case: A voted aye on the relay and withdrew on
        // Asset Hub. Positions merge per voter (latest window wins), so the
        // relay row must NOT survive as an active aye.
        let votes = json["votes"].as_array().unwrap();
        assert_eq!(votes.len(), 2, "two voters, not three rows");
        let a_addr = format!("0x{}", hex_lower(&adapter_substrate::accounts::para_sovereign(1000)));
        let a = votes
            .iter()
            .find(|v| v["voter"] == a_addr)
            .expect("voter A merged");
        assert_eq!(a["active"], false, "the Asset Hub withdrawal wins");

        // the tally counts only ACTIVE merged positions: B's 100 nay
        assert_eq!(json["direct_tally"]["voters"], 1);
        assert_eq!(json["direct_tally"]["ayes"], "0");
        assert_eq!(json["direct_tally"]["nays"], "100");
        assert_eq!(json["direct_tally"]["support"], "0");
        assert_eq!(json["direct_tally"]["truncated"], false);
        // coverage is stated, never implied
        assert!(json["direct_tally"]["note"].as_str().unwrap().contains("delegated"));
    }

    #[tokio::test]
    async fn account_votes_carry_delegations_and_state_anchors() {
        let app = router(test_state().await);
        let delegator = adapter_substrate::accounts::para_sovereign(2004);
        let addr = format!("0x{}", hex_lower(&delegator));
        let (status, json) =
            get_json(&app, &format!("/v1/gov/polkadot/accounts/{addr}/votes")).await;
        assert_eq!(status, StatusCode::OK);
        let segments = json["segments"].as_array().unwrap();
        // with no ?class=, the account surface walks EVERY registered instance:
        // the public one (relay → Asset Hub) plus the Fellowship's Collectives
        assert_eq!(segments.len(), 3);
        assert_eq!(segments[2]["chain"], "polkadot-collectives");
        assert_eq!(segments[2]["votes"][0]["vote_type"], "ranked");
        // the delegator cast no votes; its edge + anchor live in the AH window
        assert_eq!(segments[0]["delegations"].as_array().unwrap().len(), 0);
        let ah = &segments[1];
        assert_eq!(ah["delegations"][0]["track_id"], 34);
        assert_eq!(ah["delegations"][0]["active"], true);
        // the ONLY place the delegated amount exists
        assert_eq!(ah["voting_anchors"][0]["delegating_balance"], "5000");
        assert_eq!(ah["voting_anchors"][0]["delegating_conviction_label"], "locked6x");

        let (bad, _) = get_json(&app, "/v1/gov/polkadot/accounts/13UVJyLnbVpXXX/votes").await;
        assert_eq!(bad, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn fellowship_class_resolves_to_collectives_with_no_chain_named() {
        // THE PLUG-AND-PLAY PROOF at the API surface: the fellowship instance
        // lives on a chain that never moved to Asset Hub, and the only thing
        // that knows it is registry data (referenda_classes → domain → chain).
        let app = router(test_state().await);
        let (status, json) =
            get_json(&app, "/v1/gov/polkadot/referenda/300?class=fellowship_referenda").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["referendum"]["status"], "approved");
        assert_eq!(json["referendum"]["class"], "fellowship_referenda");
        let segments = json["segments"].as_array().unwrap();
        assert_eq!(segments.len(), 1, "the fellowship has one residency window");
        assert_eq!(segments[0]["chain"], "polkadot-collectives");
        assert_eq!(segments[0]["events"][0]["kind"], "approved");

        // ranked votes come back through the same class-scoped residency
        let (vs, votes) =
            get_json(&app, "/v1/gov/polkadot/referenda/300/votes?class=fellowship_referenda").await;
        assert_eq!(vs, StatusCode::OK);
        assert_eq!(votes["votes"][0]["vote_type"], "ranked");
        assert_eq!(votes["direct_tally"]["ayes"], "9");

        // and it is NOT visible under the public class (different domain,
        // different chains) — instances stay separate without any filtering code
        let (s404, _) = get_json(&app, "/v1/gov/polkadot/referenda/300").await;
        assert_eq!(s404, StatusCode::NOT_FOUND);
    }

    fn payee_hex_expected() -> String {
        format!("0x{}", hex_lower(&adapter_substrate::accounts::para_sovereign(2034)))
    }

    #[tokio::test]
    async fn treasury_spend_stitches_the_migration_and_pot_flows_stay_separate() {
        let app = router(test_state().await);

        // spend 313 was approved on the relay and PAID on Asset Hub: one spend,
        // merged, with the later window's status winning
        let (status, json) = get_json(&app, "/v1/treasury/polkadot/spends/313").await;
        assert_eq!(status, StatusCode::OK);
        // status + payment from the LATER window…
        assert_eq!(json["spend"]["status"], "paid");
        assert_eq!(json["spend"]["payment_id"], "5551");
        // …and the value columns from the EARLIER one, where the approval was.
        // Replacing wholesale would answer "what was promised" with null.
        assert_eq!(json["spend"]["amount"], "83760000000");
        assert_eq!(json["spend"]["beneficiary"], payee_hex_expected());
        assert_eq!(json["spend"]["valid_from"], 28_000_000);
        // …including the FIRST SIGHTING, which is the relay approval. This line
        // read 10_400_000 while `merge_spend` used `min()`: Asset Hub simply
        // numbers its blocks lower than the relay, so the minimum of two
        // incomparable number lines answered "which chain counts smaller"
        // rather than "which happened first".
        assert_eq!(json["spend"]["first_seen_height"], 28_000_000);
        // the asset is NOT DOT — the spend carries its own asset kind
        assert_eq!(
            json["spend"]["asset_kind"]["V4"]["asset_id"]["interior"]["X2"][1]["GeneralIndex"],
            1984
        );
        let segments = json["segments"].as_array().unwrap();
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0]["events"][0]["kind"], "approved");
        assert_eq!(segments[1]["events"][0]["kind"], "paid");

        // the list surface merges the same spend to ONE row
        let (_, list) = get_json(&app, "/v1/treasury/polkadot/spends").await;
        let spends = list["spends"].as_array().unwrap();
        assert_eq!(spends.len(), 1, "one spend, not one per chain");
        assert_eq!(spends[0]["spend_id"], 313);
        assert_eq!(spends[0]["status"], "paid");
        assert_eq!(spends[0]["amount"], "83760000000", "merged, not replaced");

        // ?kind= filters the id space instead of being silently ignored
        let (_, legacy) = get_json(&app, "/v1/treasury/polkadot/spends?kind=proposal").await;
        assert_eq!(legacy["spends"].as_array().unwrap().len(), 0);
        assert_eq!(legacy["spend_kind"], "proposal");

        // pot flows are money that names no spend, and never leak into spends
        let (_, pot) = get_json(&app, "/v1/treasury/polkadot/pot").await;
        let ah = &pot["segments"][1];
        assert_eq!(ah["events"][0]["kind"], "pot_deposit");
        assert_eq!(ah["events"][0]["amount"], "1204000000000");

        // the legacy id space is a DIFFERENT number line: proposal 313 is not
        // asset spend 313
        let (s404, _) = get_json(&app, "/v1/treasury/polkadot/spends/313?kind=proposal").await;
        assert_eq!(s404, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn sub_treasuries_resolve_to_their_own_chain() {
        let app = router(test_state().await);
        let (status, json) =
            get_json(&app, "/v1/treasury/polkadot/spends?instance=fellowship_treasury").await;
        assert_eq!(status, StatusCode::OK);
        let segments = json["segments"].as_array().unwrap();
        assert_eq!(segments.len(), 1, "the sub-treasury has one residency window");
        assert_eq!(segments[0]["chain"], "polkadot-collectives");
        assert_eq!(json["spends"][0]["spend_id"], 7);
        assert_eq!(json["spends"][0]["status"], "processed");

        // and the main treasury does not see it (separate money, separate chain)
        let (_, main) = get_json(&app, "/v1/treasury/polkadot/spends").await;
        let ids: Vec<u64> = main["spends"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["spend_id"].as_u64().unwrap())
            .collect();
        assert!(!ids.contains(&7));

        // status filter reaches the index, not just the response
        let (_, filtered) =
            get_json(&app, "/v1/treasury/polkadot/spends?status=rejected").await;
        assert_eq!(filtered["spends"].as_array().unwrap().len(), 0);
    }

    /// THE PHASE 2 EXIT SURFACE. Three things must be true at once, and the
    /// third is the one incumbents get wrong: the amount is right, its
    /// provenance travels with it, and what is MISSING is stated.
    #[tokio::test]
    async fn treasury_holdings_itemize_positions_with_provenance_and_state_the_gaps() {
        let app = router(test_state().await);
        let (status, json) = get_json(&app, "/v1/treasury/polkadot/holdings").await;
        assert_eq!(status, StatusCode::OK);

        let seg = &json["segments"][0];
        assert_eq!(seg["chain"], "polkadot-asset-hub");
        let account = &seg["accounts"][0];
        // the account says WHY it is treasury money, re-derivably
        assert_eq!(account["derivation"], "modl:py/trsry");
        assert_eq!(account["role"], "pot");
        assert_eq!(account["instance"], "treasury");

        let positions = account["positions"].as_array().unwrap();
        let usdt = positions
            .iter()
            .find(|p| p["asset"] == "assets:1984")
            .expect("the anchored USDT position");
        // anchor 20895000000 minus a later 895000000 transfer out
        assert_eq!(usdt["amount"], "20000000000");
        assert_eq!(usdt["symbol"], "USDT");
        assert_eq!(usdt["decimals"], 6);
        // six decimals applied WITHOUT floats
        assert_eq!(usdt["display"], "20000.000000");
        assert_eq!(usdt["basis"], "anchor+deltas");
        assert_eq!(usdt["provenance"]["anchor_height"], 19_400_000);
        assert_eq!(usdt["provenance"]["anchor_spec_version"], 2003002);
        assert_eq!(usdt["provenance"]["anchor_source"], "treasury-holdings");
        assert_eq!(usdt["provenance"]["delta_sum_since_anchor"], "-895000000");
        assert_eq!(usdt["provenance"]["delta_count_since_anchor"], 1);
        assert_eq!(usdt["provenance"]["account_status"], "liquid");

        // an asset seen MOVING but never anchored is reported with a null
        // amount — present and honest, not omitted and tidy
        let unanchored = positions
            .iter()
            .find(|p| p["asset"] == "assets:1337")
            .expect("the unanchored USDC position");
        assert!(unanchored["amount"].is_null());
        assert!(unanchored["display"].is_null());
        assert_eq!(unanchored["basis"], "deltas_only");
        // …and with no registry entry, no symbol is invented for it
        assert!(unanchored["symbol"].is_null());
        assert!(unanchored["decimals"].is_null());

        let coverage = &json["coverage"];
        // the NATIVE position is here too, rendered in DOT's own decimals —
        // 500 anchored + 200 moved since, at 10 decimals
        let native = positions
            .iter()
            .find(|p| p["asset"] == "native")
            .expect("the native position");
        assert_eq!(native["amount"], "700");
        assert_eq!((native["symbol"].as_str(), native["decimals"].as_u64()), (Some("DOT"), Some(10)));
        assert_eq!(native["display"], "0.0000000700");

        assert_eq!(coverage["positions_with_an_anchor"], 2);
        assert_eq!(coverage["positions_without_an_anchor"], 1);
        assert!(coverage["valuation"].as_str().unwrap().contains("quantities only"));
        let gaps = coverage["not_covered"].as_array().unwrap();
        assert!(gaps.iter().any(|g| g.as_str().unwrap().contains("bounty")));
        assert!(gaps.iter().any(|g| g.as_str().unwrap().contains("Hydration")));
    }

    #[tokio::test]
    async fn assets_endpoint_lists_representations_of_one_chain() {
        let app = router(test_state().await);
        let (status, json) = get_json(&app, "/v1/assets/polkadot-asset-hub").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["count"], 2);
        // sorted by asset_key: "assets:1984" before "native"
        let a = &json["assets"][0];
        assert_eq!(a["asset_key"], "assets:1984");
        assert_eq!(a["representation_kind"], "trust_backed");
        assert_eq!(a["decimals"], 6);
        // the location a treasury spend names it by travels with it
        assert_eq!(a["xcm_location"]["interior"][1]["GeneralIndex"], 1984);
        // a chain with no registered assets is empty, not an error
        let (status, json) = get_json(&app, "/v1/assets/polkadot").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["count"], 0);
    }

    /// The slice-5 drill, done by the API instead of by hand: spend 313 is
    /// 83760000000 of SOMETHING, and only the asset registry can say that the
    /// something is USDT at six decimals rather than planck of DOT.
    #[tokio::test]
    async fn a_spend_resolves_to_the_asset_it_was_denominated_in() {
        let app = router(test_state().await);
        let (status, json) = get_json(&app, "/v1/treasury/polkadot/spends/313").await;
        assert_eq!(status, StatusCode::OK);
        let asset = &json["asset"];
        assert_eq!(asset["resolved"], true);
        // resolved on ASSET HUB, though the spend was approved on the relay:
        // the location said Parachain(1000) and the registry knew which chain
        // that is — no caller named a chain
        assert_eq!(asset["chain"], "polkadot-asset-hub");
        assert_eq!(asset["asset"], "assets:1984");
        assert_eq!(asset["symbol"], "USDT");
        assert_eq!(asset["decimals"], 6);
        assert_eq!(asset["display"], "83760.000000");

        // the fellowship spend carries no asset_location (a v1-mapper row), and
        // the response says exactly that instead of quietly showing nothing
        let (status, json) =
            get_json(&app, "/v1/treasury/polkadot/spends/7?instance=fellowship_treasury").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["asset"]["resolved"], false);
        assert!(json["asset"]["reason"].as_str().unwrap().contains("treasury-range"));
    }

    /// The list surface for the outflow no treasury table can see — and the
    /// rule migration 0011 makes a contract: the -1 is a primary-key device
    /// and must never reach a reader.
    #[tokio::test]
    async fn bounties_merge_across_the_migration_and_never_leak_the_parent_sentinel() {
        let app = router(test_state().await);
        let (status, json) = get_json(&app, "/v1/bounties/polkadot").await;
        assert_eq!(status, StatusCode::OK);
        // the default instance is STATED, not implied
        assert_eq!(json["instance"], "bounties");
        let list = json["bounties"].as_array().unwrap();
        assert_eq!(list.len(), 1, "one bounty, not one row per chain");
        assert_eq!(list[0]["bounty_id"], 22);
        // status from the LATER window…
        assert_eq!(list[0]["status"], "active");
        assert_eq!(list[0]["curator"], format!("0x{}", hex_lower(
            &adapter_substrate::accounts::para_sovereign(1000)
        )));
        // …value from the EARLIER one, where the proposal was
        assert_eq!(list[0]["value"], "100000000000");
        // paid_out is a per-chain RUNNING TOTAL, so the windows ADD: choosing
        // between them would report a bounty that paid on both sides of the
        // migration as having paid only once
        assert_eq!(list[0]["paid_out"], "1250");
        // the EARLIER WINDOW's sighting, structurally — not min(27_000_000,
        // 10_500_000), which would answer 10.5M because Asset Hub numbers its
        // blocks lower, not because anything happened there first
        assert_eq!(list[0]["first_seen_height"], 27_000_000);
        // the parent renders as null, and the sentinel appears nowhere in the
        // serialized body at all
        assert!(list[0]["child_id"].is_null());
        assert!(
            !serde_json::to_string(&json).unwrap().contains("\"child_id\":-1"),
            "the -1 sentinel must not survive serialization"
        );
        assert_eq!(json["segments"].as_array().unwrap().len(), 2);
    }

    /// A post-migration event that moves no status — a value raise, an
    /// extension, a deposit poke — lands as the sink's 'unknown' placeholder.
    /// Merging it naively would erase the relay's verdict, which is the same
    /// defect `merge_referendum` was given its guard for.
    #[test]
    fn an_info_only_asset_hub_row_never_erases_a_relay_verdict() {
        let row = |status: &str, height: u64, value: Option<&str>| BountyRow {
            instance: "bounties".into(),
            bounty_id: 22,
            child_id: None,
            status: status.into(),
            value: value.map(str::to_string),
            paid_out: None,
            bond: None,
            curator: None,
            beneficiary: None,
            beneficiary_location: None,
            payment_id: None,
            account_id: None,
            first_seen_height: height,
            status_height: height,
            asset_ref: None,
        };
        let merged = merge_bounty(
            row("active", 27_000_000, None),
            row("unknown", 10_500_000, Some("100000000000")),
        );
        assert_eq!(merged.status, "active", "a placeholder is not a verdict");
        assert_eq!(merged.status_height, 27_000_000, "the status keeps ITS height");
        // …and the placeholder row's information still arrives
        assert_eq!(merged.value.as_deref(), Some("100000000000"));
    }

    #[tokio::test]
    async fn a_child_bounty_is_addressed_by_child_and_is_not_its_parent() {
        let app = router(test_state().await);
        let (status, json) =
            get_json(&app, "/v1/bounties/polkadot/22?instance=child_bounties&child=3").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["bounty"]["child_id"], 3);
        assert_eq!(json["child_id"], 3);
        assert_eq!(json["bounty"]["status"], "claimed");
        assert_eq!(json["bounty"]["paid_out"], "500");
        assert_eq!(json["bounty"]["beneficiary"], payee_hex_expected());
        // a native-token pallet says so as an ANSWER, not as a gap
        assert_eq!(json["asset"]["resolved"], true);
        assert_eq!(json["asset"]["asset"], "native");
        assert!(json["asset"]["note"].as_str().unwrap().contains("native-token-only"));

        // the same id WITHOUT ?child= is the parent bounty, which this pallet
        // does not have — a child is not addressable as its parent
        let (missing, _) = get_json(&app, "/v1/bounties/polkadot/22?instance=child_bounties").await;
        assert_eq!(missing, StatusCode::NOT_FOUND);

        // …and the legacy parent carries its DERIVED account, which is where
        // the money actually is (value − paid_out is not a balance)
        let (_, parent) = get_json(&app, "/v1/bounties/polkadot/22").await;
        let expected = adapter_substrate::accounts::sub_account(
            b"py/trsry",
            &[
                adapter_substrate::accounts::SubKey::Str("bt"),
                adapter_substrate::accounts::SubKey::Index(22),
            ],
        )
        .unwrap();
        assert_eq!(
            parent["bounty"]["account_id"],
            format!("0x{}", hex_lower(&expected))
        );
    }

    /// The slice's payoff: a bounty payout is denominated exactly like a
    /// treasury spend and resolves through the same `core.assets` join, so
    /// 83760000000 reads as 83,760 USDT in both places.
    #[tokio::test]
    async fn a_multi_asset_bounty_payout_resolves_to_the_asset_it_was_paid_in() {
        let app = router(test_state().await);
        let (status, json) =
            get_json(&app, "/v1/bounties/polkadot/1?instance=multi_asset_bounties").await;
        assert_eq!(status, StatusCode::OK);
        assert!(json["bounty"]["child_id"].is_null(), "None IS the parent");
        let asset = &json["asset"];
        assert_eq!(asset["resolved"], true);
        // no caller named a chain: `Here` resolved through the treasury's own
        // residency windows
        assert_eq!(asset["chain"], "polkadot-asset-hub");
        assert_eq!(asset["asset"], "assets:1984");
        assert_eq!(asset["symbol"], "USDT");
        assert_eq!(asset["display"], "83760.000000");

        // the modern instance is a DIFFERENT number line: bounty 1 there is not
        // bounty 1 in the legacy pallet
        let (missing, _) = get_json(&app, "/v1/bounties/polkadot/1").await;
        assert_eq!(missing, StatusCode::NOT_FOUND);
    }

    /// The api crate depends on no adapter (Invariant 4), so the sentinel is
    /// spelled twice. This is the only thing stopping the two copies drifting.
    #[test]
    fn the_parent_sentinel_agrees_with_the_adapter() {
        assert_eq!(PARENT_SENTINEL, adapter_substrate::bounties::PARENT_SENTINEL);
    }

    #[test]
    fn units_are_formatted_by_string_surgery_never_by_floats() {
        assert_eq!(format_units("20895000000", 6).unwrap(), "20895.000000");
        // fewer digits than decimals must pad, not truncate
        assert_eq!(format_units("5", 6).unwrap(), "0.000005");
        assert_eq!(format_units("0", 10).unwrap(), "0.0000000000");
        assert_eq!(format_units("-1500", 2).unwrap(), "-15.00");
        assert_eq!(format_units("12", 0).unwrap(), "12");
        // a value no float could hold exactly, rendered exactly
        assert_eq!(
            format_units("243100255393737286", 10).unwrap(),
            "24310025.5393737286"
        );
        assert!(format_units("not-a-number", 6).is_none());
    }

    #[tokio::test]
    async fn chains_list_reflects_registry_only() {
        let app = router(test_state().await);
        let (status, json) = get_json(&app, "/v1/chains").await;
        assert_eq!(status, StatusCode::OK);
        let ids: Vec<&str> = json["chains"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["id"].as_str().unwrap())
            .collect();
        assert!(ids.contains(&"polkadot") && ids.contains(&"polkadot-asset-hub"));
    }
}
