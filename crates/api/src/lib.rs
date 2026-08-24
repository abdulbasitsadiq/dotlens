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
/// The coretime delta's arithmetic, as a PURE function with no database in it —
/// see the module header for why that is the design and not a convenience.
pub mod channels;
pub mod coretime_delta;
/// Per-module freshness — the lag stack, the four states and the halt, derived
/// as a PURE function so the operator surface and the per-response object
/// cannot drift apart.
pub mod freshness;
pub mod search;

pub use coretime_delta::{
    Check, CoreDelta, DeltaInput, DeltaReport, EntitlementRow, OccupancyCell, PARTS_WHOLE_CORE,
};

use registry::Registry;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Debug, thiserror::Error)]
#[error("block index error: {0}")]
pub struct IndexError(pub String);

/// Prefix marking the one read failure that is OURS rather than the database's:
/// a query cancelled by the serving `statement_timeout`.
///
/// A marker on our own message rather than a match on Postgres's English. The
/// database's wording is localised by `lc_messages` and is not a contract; the
/// SQLSTATE is, and it is read exactly once, in `From<sqlx::Error>` below.
/// Matching the message downstream would be this project's "match SQLSTATE,
/// never a name" rule broken one layer up.
pub const TIMEOUT_REFUSAL: &str = "query cancelled by the serving statement timeout";

impl IndexError {
    /// Was this the serving timeout, rather than a database fault?
    ///
    /// The distinction matters to a caller: a timeout means *narrow the
    /// question*, and everything else means *the answer is unavailable*. Without
    /// it both render as one opaque failure and the caller cannot tell which of
    /// the two actions is theirs to take.
    pub fn timed_out(&self) -> bool {
        self.0.starts_with(TIMEOUT_REFUSAL)
    }
}

/// The ONE place a driver error becomes a read failure.
///
/// Every Postgres reader converts through here, so the classification cannot be
/// forgotten at one call site out of sixty-three — which is the shape that
/// leaves a rule true in most places and quietly false in one.
///
/// SQLSTATE `57014` is `query_canceled`, which is what `statement_timeout`
/// raises. It is a REFUSAL and not a fault: the data is fine, the question was
/// too expensive, and the message says which so a caller knows that narrowing
/// the window is the fix rather than retrying the same thing harder.
#[cfg(feature = "pg")]
impl From<sqlx::Error> for IndexError {
    fn from(e: sqlx::Error) -> Self {
        if let Some(db) = e.as_database_error() {
            if db.code().as_deref() == Some("57014") {
                return IndexError(format!(
                    "{TIMEOUT_REFUSAL}: the window asked for is too expensive to serve \
                     under current load. Narrow it and retry — nothing is wrong with the \
                     data, and no partial answer was returned in place of a whole one. \
                     (operator: DOTLENS_SERVING_STATEMENT_TIMEOUT_SECS)"
                ));
            }
        }
        IndexError(e.to_string())
    }
}

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
    /// Sum of frozen locks where the runtime exposes them. Informational —
    /// total is free + reserved and does NOT subtract this. Null means the
    /// runtime did not tell us, which is different from zero.
    pub frozen: Option<String>,
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
                anchor_frozen: anchor.as_ref().and_then(|a| a.frozen.clone()),
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
    /// The OBSERVER-FREE name (migration 0019) — what makes this row and another
    /// chain's row for the same asset recognisable as one thing.
    ///
    /// `location_key` cannot do it and never could: a Location is relative to
    /// its observer, so Hydration spells USDT `{parents:1, X3[Parachain(1000),
    /// PalletInstance(50), GeneralIndex(1984)]}` and Asset Hub spells the same
    /// asset `{parents:0, X2[PalletInstance(50), GeneralIndex(1984)]}`.
    /// Version-stripping makes the SPELLINGS agree and cannot make the FRAMES
    /// agree. Verified on live data: those two rows carry **2 distinct
    /// `location_key`s and 1 distinct `absolute_key`**.
    ///
    /// NULL is common and is not a fault — 756 of Hydration's 1,437 registered
    /// assets have no absolute name, because XYK shares, StableSwap shares and
    /// Bonds are chain-local constructs with no XCM location at all. A null here
    /// means "cannot be named across chains", which is why the consolidation
    /// endpoint counts them rather than dropping them.
    pub absolute_key: Option<String>,
    pub absolute_location: Option<serde_json::Value>,
    /// The asset's own declared type where its registry has one (Token, Erc20,
    /// XYK, …). NULL for pallet-assets, which has no such concept. `Erc20` is
    /// the one a reader must act on: those balances live in `pallet_evm`
    /// storage, so they are named here and anchorable nowhere.
    pub asset_type: Option<String>,
}

/// One XCM observation (`xcm.messages`) — one chain's half of one message.
#[derive(Debug, Clone, serde::Serialize)]
pub struct XcmMessageRow {
    pub chain_id: String,
    pub block_height: u64,
    pub event_index: u32,
    /// sent | received | local
    pub side: String,
    /// hrmp | ump | dmp | local | remote | unknown. `remote` means the
    /// destination names another consensus system (a bridged message), where
    /// the FINAL destination and the first hop's transport are different things.
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
    /// unique_in_block | interleaved | remote_destination
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
        "the wire_hash<->topic link is only recorded where the block makes it unambiguous, by \
         one of three rules: one queued send and one `Sent` OF THAT TRANSPORT \
         (`unique_in_block`), n of each strictly alternating (`interleaved`), or — where the \
         `Sent` is addressed to ANOTHER CONSENSUS SYSTEM and so cannot agree about the local \
         transport at all — the queued send IMMEDIATELY BEFORE it in the block's send sequence \
         (`remote_destination`). A block with 2 queued sends and 1 `Sent` records NO link, so a \
         wire hash from that block reaches only its own half. `aliases` in this response is \
         every link found while expanding, with the rule and evidence each rests on — a \
         superset of those used when `alias_limit_reached` is true",
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
        "a leg addressed to ANOTHER CONSENSUS SYSTEM (a Snowbridge export, a bridged transfer) \
         stops here, and `leaves_consensus` says so rather than letting it read as an \
         in-flight or dropped message. What happens after the bridge needs the bridge tracer \
         (ARCHITECTURE §10: Snowbridge and ISMP lifecycles), which is not this module. The \
         wire hash still pairs with its topic — the queue pallet knows the first hop even \
         when the destination is Ethereum — PROVIDED the queued send is the one immediately \
         before it in the block; a `Sent` with another send between it and the nearest queued \
         one still records nothing",
        "a `remote:<consensus>` counterparty is NOT COMPARABLE against a registry that knows \
         only this network, so `checks.counterparty_mirror` reports those pairs as unknown. \
         That is the price of never rendering a foreign chain's para id as one of ours, which \
         is what rows written before XCM_MAPPER_VERSION 2 did",
        "steps are ordered by BLOCK TIMESTAMP, which is the only ordering two chains share; \
         `core.blocks.timestamp` is nullable, and a step without one falls back to chain and \
         height, which are not comparable across chains. `checks.time_order` says which case \
         this journey is in",
    ]);
    v
}

/// What a core-occupancy answer is not.
///
/// THE FIRST LINE IS THE SLICE'S WHOLE PRODUCT CLAIM. There are two ratios here
/// and they are different questions; a single `utilization` field would report
/// one and call it the other, throwing away the only number in this response
/// that no other tool has.
pub fn coretime_not_covered() -> Vec<&'static str> {
    vec![
        "there are TWO ratios and neither of them is 'utilization'. `cores_touched_ratio` is how \
         many declared cores produced ANYTHING; `slots_filled_ratio` is how many core-block \
         slots were actually filled. Measured over 1,000 contiguous relay blocks they read \
         47.0% and 34.69% — the gap IS the finding, because it says even the cores that are \
         used sit idle. Collapsing them into one number reports one and calls it the other",
        "the denominator is a DATED READING, not a constant. `num_cores` is host configuration \
         that moves at session boundaries, so `denominator.read_at_height` says which reading \
         this ratio divided by and `denominator.position` says whether it was read inside the \
         window, before it, after it, or that there is no reading at all — in which case both \
         ratios are null rather than computed against a guess. A ratio against a stale \
         denominator is silently wrong, so `denominator.stale_suspected` fires when a core index \
         at or above `num_cores` appears on ANY row in the window (inclusions, backings and \
         time-outs alike), which can only mean the reading predates a core count that grew. \
         `stable_across_window` is null when no reading falls INSIDE the window: nothing here \
         can then say whether the denominator moved during it, and that is not the same as \
         saying it held",
        "this endpoint is the USAGE half ONLY. The entitlement half — what each core was BOUGHT \
         or reserved to do — is `pallet-broker` on the Coretime chain and lands in \
         coretime.broker_events / coretime.core_assignments (slice 13). The delta between what \
         was PAID FOR and what was USED is a join across the two, and NOTHING ON THIS ENDPOINT \
         COMPUTES IT: the ratios here divide by every declared core, entitled or not, so a low \
         figure mixes 'nobody bought it' with 'somebody bought it and did not use it'. \
         /v1/coretime/{network}/delta is the endpoint that separates them",
        "bulk and on-demand cannot be distinguished from relay data AT ALL, and slice 13 measured \
         what that costs rather than leaving it as a caveat. A core used for a handful of blocks \
         looks like an on-demand core and looks equally like a bulk core whose chain was idle, \
         and nothing in `paraInclusion` says which — so slice 11's reading of two barely-used \
         cores as an on-demand signal was an INFERENCE, and the entitlement half REFUTED it: both \
         are Task-entitled bulk cores with a full mask, using 1.0% and 2.9% of what they bought. \
         Any such reading taken from this endpoint alone is a guess of the same kind",
        "the ratios count `included` rows ONLY. `backed` rows sit in the same table because \
         backed-without-included is the wasted-coretime signal, and counting them here would \
         roughly double every figure — async backing means almost every inclusion has a backing \
         2-6 blocks earlier. `by_kind` shows what is there beside the inclusions",
        "`timed_out` — the runtime saying outright that a core was occupied and produced \
         nothing — has ZERO live instances: not one row in 1,542 relay blocks, and no event \
         matching TimedOut anywhere in the events table. The second, independent signal is also \
         empty: matching backed to included on `pov_hash` inside window INTERIORS gives 48,932 \
         backed and 0 never included. So wasted coretime is expressible here and has never been \
         observed, and this response must not be read as evidence that it does not happen",
        "a block that is DECODED but not yet coretime-mapped counts in `blocks_indexed` and \
         contributes no occupancy, which under-reports the slot-fill ratio. Compare \
         `window.blocks_indexed` against `window.heights_with_occupancy` to see it — but read \
         the gap carefully, because it has THREE causes and this response cannot tell them \
         apart: the mapper's checkpoint being behind the window's end, blocks that genuinely \
         carried no candidate events, and (at the tip) nothing at all, since `blocks_indexed` \
         counts FINALIZED blocks only, matching the filter the mapper's own event source uses",
        "`window.blocks_indexed` is the slot-fill denominator, NOT the requested span. A window \
         with gaps must not be divided by its nominal width or the ratio drops with every block \
         nobody indexed; `window.contiguous` says which case this is",
        "core->para rotation is UNEXERCISED on live data. Every one of the 47 used cores in the \
         prep's 1,000-block sample served exactly one para, so a `paras` list with two entries \
         would be the first live instance of the time-ranged half of the core<->chain \
         many-to-many. The para->cores direction is well exercised (one para held 11 cores at \
         93.1% each), and the model rests on that half plus ECOSYSTEM's reasoning for the other",
        "`occupancy.lineage` is the aggregate's own Invariant-3 stamp, and MORE THAN ONE ENTRY \
         means the window was mapped under two rule sets — a `mapper_version` change is a change \
         to which events are mapped or how a field is read, so the counts above would be an \
         average of two different definitions of occupancy. An EMPTY list means no lineage was \
         recorded for this window, which happens when there are no rows in it at all",
        "occupancy is not THROUGHPUT. A filled slot says a candidate was included on that core, \
         not how much work it carried: PoV size, weight and transaction counts are not read \
         here, and `head_data` is deliberately not stored at all (20.0 MB across 17,763 rows in \
         a 555-block subset, and occupancy needs none of it)",
    ]
}

/// What the entitlement-vs-occupancy delta is not.
///
/// THE FIRST THREE LINES ARE THE THREE RULES `api::coretime_delta` refuses to
/// break, and they are stated to the reader rather than kept in the code that
/// enforces them: an unknown core is not an idle one, pool time is not waste,
/// and an assignment announced is not an assignment applied.
pub fn coretime_delta_not_covered() -> Vec<&'static str> {
    vec![
        "A CORE WITH NO ASSIGNMENT AT OR BEFORE THE ANCHOR IS `unknown`, NEVER `idle`. \
         `Broker.CoreAssigned` fires only at SALE BOUNDARIES — the sale governing the measured \
         1,000-block window sat ~157,000 coretime blocks (~335,000 relay blocks) before it — so \
         a core with no entitlement on record means 'our index does not reach the sale that \
         governs it', which is not 'nobody bought it'. `entitlement.unknown_cores` names them, \
         and the waste figure is WITHHELD entirely while any exist, because an unknown core may \
         be task-entitled and idle and omitting it under-states waste, which is the direction \
         that flatters the product",
        "POOL CORES ARE NOT WASTE. A `Pool` core was sold and donated to the instantaneous \
         market, so its time belongs to no purchaser BY CONSTRUCTION rather than by our \
         ignorance. 43 of 100 cores in the measured sale, i.e. 43,000 slots that would become \
         invented waste the moment somebody folded them in. `waste.pool_slots` reports them \
         beside the figure they are excluded from",
        "AN ASSIGNMENT ANNOUNCED IS NOT AN ASSIGNMENT APPLIED, and this endpoint reads the \
         announcement. `Broker.CoreAssigned` is emitted on the Coretime chain when the \
         instruction is SENT by XCM Transact; the relay emits its own `coretime.CoreAssigned` \
         only after `scheduler::assign_core` returns Ok. Measured at a sale boundary: 97 sent, \
         97 applied, ZERO unpartnered — so the failure mode is expressible here and has NO LIVE \
         INSTANCE, which is stated as unobserved rather than impossible. Nothing here reads the \
         relay's applied half",
        "THE TWO DENOMINATORS ARE DATED ON DIFFERENT NUMBER LINES AND ARE NOT COMPARABLE IN \
         TIME. `denominators.relay_num_cores` is read at a RELAY height; \
         `denominators.broker_core_count` is read at a CORETIME height. They are compared by \
         VALUE (both read 100, on two chains, from two storage items) and never ordered against \
         each other. So `denominators` carries a `relay_reading_position` for the relay half and \
         NOTHING of the kind for the broker half, and no `stable_across_window` for either — the \
         occupancy endpoint serves both for its own denominator and neither is answerable here. \
         (`entitlement.stable_across_window` is a DIFFERENT subject: whether the ASSIGNMENTS \
         moved inside the window, which is answerable and is served.) A disagreement WITHHOLDS \
         the waste figure rather than picking a winner, which is what migration 0025 requires of \
         this reader — and an ABSENT reading leaves `checks.denominators_agree` at `unknown`, \
         which is not `ok`",
        "`first_core` IS A DATED READING TOO, and it is taken from the NEWEST \
         `coretime.broker_config` row rather than from one aligned with the window — because it \
         is dated on the coretime chain's number line and cannot be positioned against a relay \
         window at all. It moves every sale. So the reserved-vs-market split is as of \
         `market.read_at_height` and NOT as of the window, and with no reading at all the split \
         is ABSENT rather than assumed: `first_core = 0` would move every reserved system core \
         into the market and put the waste on the wrong side of the boundary",
        "THE MASK DOES NOT CROSS. `pallet-broker`'s tick converts a region's 80-bit `CoreMask` \
         to the relay's ratio as `count_ones() * 720`, so the BIT COUNT crosses and the PATTERN \
         does not — two interlaced regions on one core assigned to the same task are \
         indistinguishable on the relay side forever. `parts` is the only surviving trace, this \
         reader never divides by it, and a MIXED or FRACTIONAL core withholds the waste figure \
         rather than counting a fraction of a core as a whole one. Measured three independent \
         ways as currently empty: every live assignment carries a full 57,600",
        "the occupancy side counts `included` rows ONLY, for the reason \
         /v1/coretime/{chain}/occupancy gives: async backing puts a `backed` row 2-6 blocks \
         before nearly every inclusion, so counting them would roughly double every figure",
        "THE DELTA IS COMPUTED PER REQUEST AND NEVER STORED. Both sides carry `runtime_version` \
         and `mapper_version`, so a materialised join would be the one copy WITHOUT lineage — \
         the fifth refusal of that shape in this project, after treasury.consolidated_position, \
         graph.cross_chain_operations, the stored forwarded-attribution and a logical_assets \
         join table. `entitlement.lineage` is the entitlement side's own Invariant-3 stamp; more \
         than one entry means the governing assignments were mapped under two rule sets",
        "A RENEWAL MOVES THE CORE INDEX, so the key here is (core, task, relay-block window) and \
         never the region. Measured at coretime 4919882: para 3428 renewed five cores and every \
         index changed (35->43, 36->44, 37->45, 40->46, 41->47). A core index therefore \
         identifies an entitlement only WITHIN one region, and following a tenant across sale \
         cycles by core index silently follows a different tenant after every sale",
        "`Broker.SaleInfo` is captured whole in coretime.broker_config and only `first_core` is \
         read from it. `cores_sold`, `end_price` and `sellout_price` move on every purchase with \
         NO EVENT, so the Dutch leadin price curve is not reconstructible from anything else \
         this project stores — but reconstructing it (against Configuration.leadin_length and \
         the sale geometry) is its own slice and nothing here does it. What a core COST is not \
         served by this endpoint at any point",
        "on-demand traffic is invisible from both sides. The relay cannot tell bulk from \
         on-demand at all, and a `Pool` core's occupancy is unattributable by construction, so a \
         pool core carrying blocks would appear here as `pool_used` with no way to say who \
         bought the time. Zero live instances: no pool core produced anything in the measured \
         window, which is also what refutes reading two barely-used bulk cores as on-demand",
        "`window.blocks_indexed` is the slot denominator, NOT the requested span. A GAP THEREFORE \
         DIVIDES OUT: it removes the block from the numerator and from `blocks_indexed` in the \
         same proportion, so a window with holes is not biased towards more waste — \
         `window.contiguous` says whether it has any, which is a different question. What DOES \
         bias the used ratio down is a block that is DECODED but not yet coretime-mapped: it \
         counts in `blocks_indexed` and contributes no occupancy. Compare \
         `window.blocks_indexed` against `window.heights_with_occupancy` to see it",
    ]
}

/// What an entitlement timeline is not.
pub fn entitlement_not_covered() -> Vec<&'static str> {
    vec![
        "this is the ANNOUNCEMENT stream, not the applied one — see the delta endpoint's own \
         coverage list. `assignments` are rows of coretime.core_assignments, i.e. what the \
         broker said it was sending to the relay",
        "A CORE INDEX IS NOT A DURABLE IDENTITY. Asking by `core` follows a SLOT across sale \
         cycles and therefore follows whichever tenant holds it; asking by `task` follows the \
         TENANT. Measured: para 3428's five renewals every one moved the index (35->43, 36->44, \
         37->45, 40->46, 41->47). If you are tracking a chain, ask by task",
        "18 of the 37 declared `Broker` variants name no core and 31 name no task, so a timeline \
         asked by either shows a SUBSET of what happened. The whole vocabulary for a block is in \
         coretime.broker_events keyed by (chain, height, event_index); this endpoint indexes it \
         by subject",
        "`data` is the variant's whole decoded payload, kept intact (schema-on-read). Two shapes \
         measured and worth knowing before querying it: the broker's core index renders as a \
         BARE u16 (`\"core\": 0`) where the relay's renders as `{\"core\":[0]}`, and \
         `RegionRecord.owner` is an `Option<AccountId32>` rendering THREE array layers deep",
        "31 of the 37 variants have NO LIVE INSTANCE in the sampled windows, so their field \
         lists are pinned against `pallet-broker` 0.28.0 rather than measured. `Purchased` is \
         among them, and its absence is a GAP IN THE SAMPLE and not a finding about the market: \
         653 blocks is a fraction of a 28-day cycle and purchases spread across a 14-day leadin",
        "region OWNERSHIP is not here. A region's owner lives in the storage VALUE \
         (`RegionRecord.owner`) and not in any event's id, and this project ships no reader for \
         it — so 'who holds this entitlement' is not a question these rows answer",
    ]
}

/// Reduce an observed counterparty to a form comparable with
/// [`xcm_counterparty_name`], given the network the observing chain is in.
///
/// `None` means NOT COMPARABLE, which the mirror check reports as `unknown` and
/// never as a contradiction. Since XCM_MAPPER_VERSION 2 a bridged destination
/// reads `remote:kusama/para:1000` rather than `para:1000` — deliberately, so a
/// foreign chain is never rendered as one of ours. The cost of that correctness
/// fix is exactly here: `remote:<other network>` cannot be checked against a
/// registry that only knows this one, and saying "unknown" is the whole point.
/// A location that names THIS network absolutely AND carries a para id
/// (`remote:polkadot/para:2034`) is comparable, and resolving it is why this
/// takes the network at all; a bare `remote:polkadot` has nothing to compare and
/// stays unknown.
fn comparable_counterparty<'a>(observed: &'a str, network: &str) -> Option<&'a str> {
    let Some(rest) = observed.strip_prefix("remote:") else {
        return Some(observed);
    };
    match rest.split_once('/') {
        Some((consensus, tail)) if consensus == network => Some(tail),
        _ => None,
    }
}

/// Does this counterparty name a consensus system other than `network`?
fn is_foreign_consensus(observed: &str, network: &str) -> bool {
    observed
        .strip_prefix("remote:")
        .map(|rest| rest.split('/').next().unwrap_or("") != network)
        .unwrap_or(false)
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
    /// `None` on a `fork` row: that tier calls no runtime API and so has no
    /// `result_xcms_version`.
    pub xcm_version: Option<u32>,
    /// executed | dispatch_failed | api_error on the dry_run tier;
    /// executed | dispatch_failed | not_dispatched on the fork tier. See the
    /// migrations on why `dispatch_failed` is a result rather than an error, and
    /// on why `not_dispatched` is not folded into it.
    pub status: String,
    pub dispatch_ok: Option<bool>,
    pub dispatch_error: Option<serde_json::Value>,
    pub emitted_events: serde_json::Value,
    pub event_count: u32,
    pub local_xcm: Option<serde_json::Value>,
    /// `None` = this tier does not produce a forwarded list at all. Not `[]`,
    /// which would read as "this call queues no messages" — a claim about the
    /// call where the truth is a fact about the tier. The two are also what
    /// decides whether `forwarded_attribution` is attached to this row.
    pub forwarded_xcms: Option<serde_json::Value>,
    pub effects: serde_json::Value,
    pub note: Option<String>,
    pub spec_version: u64,
    pub api_version: Option<u32>,
    pub metadata_version: u32,
    pub sim_version: u32,
    pub raw_location: String,
    pub observed_at: Option<DateTime<Utc>>,
    /// The no-op run at the same state whose `forwarded_xcms` is the ambient
    /// queue. `None` = no baseline, and every reader is told so rather than left
    /// to assume the forwarded list is this call's doing.
    pub baseline_input_hash: Option<String>,

    // ------------------------------------------------- Tier 2 (Phase 3, slice 8)
    /// The storage this run INJECTED, each entry carrying what the real chain
    /// held there. `None` = this row is not a counterfactual, and that
    /// distinction is why it is not an empty array.
    pub overrides: Option<serde_json::Value>,
    pub override_hash: Option<String>,
    pub storage_diff: Option<serde_json::Value>,
    pub storage_diff_count: Option<u32>,
    /// decoded | extrinsic_only | undecodable | unavailable | refused — see
    /// `sim::DIFF_STATUSES` and migration 0023. Read BESIDE `dispatch_route`:
    /// whether that scope reaches this row's own call is what `diff_covers`
    /// computes, and on a scheduled fork row it does not.
    pub diff_status: Option<String>,
    /// A block that exists ONLY ON THE FORK. No canonical chain has it and no
    /// explorer will find it — see `counterfactual.reads_as`.
    pub built_block_hash: Option<String>,
    pub harness: Option<serde_json::Value>,
    /// scheduled | dry_run_extrinsic. A fork row must name its route, because the
    /// two model different things and `tier_coverage` is chosen from it.
    pub dispatch_route: Option<String>,
    /// Which block-number line the scheduler counts on, and the evidence for it.
    pub agenda_anchor: Option<serde_json::Value>,
}

/// One recorded `dry_run_xcm` — what a chain would do with a program that
/// ARRIVED, as opposed to a call it dispatched (`sim.xcm_simulations`).
#[derive(Debug, Clone, serde::Serialize)]
pub struct XcmSimulationRow {
    pub chain_id: String,
    pub at_height: u64,
    pub at_block_hash: String,
    pub input_hash: String,
    pub tier: String,
    pub program_hash: String,
    pub program: serde_json::Value,
    pub program_summary: Option<String>,
    pub origin_location: serde_json::Value,
    pub origin_ref: String,
    /// complete | incomplete | not_started | api_error. `not_started` is
    /// upstream's `Outcome::Error` renamed: execution never began, which is a
    /// barrier rejection rather than a failure of the request — and the answer
    /// the SENDING chain structurally cannot give, since its own `Sent` would
    /// look perfectly successful.
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

    /// One row by its full key. Used to fetch a row's BASELINE, which is an
    /// ordinary recorded simulation and not a special kind of thing — the whole
    /// point of running the no-op through the same path.
    async fn simulation_at(
        &self,
        chain_id: &str,
        at_block_hash: &str,
        input_hash: &str,
        tier: &str,
    ) -> Result<Option<SimulationRow>, IndexError>;
}

/// Read side of `sim.xcm_simulations`.
#[async_trait]
pub trait XcmSimIndex: Send + Sync {
    /// Recorded previews of one PROGRAM on one chain, newest state first.
    async fn xcm_simulations(
        &self,
        chain_id: &str,
        program_hash: &str,
        limit: u32,
    ) -> Result<Vec<XcmSimulationRow>, IndexError>;

    /// One row by its full key — used to fetch a row's BASELINE, exactly as
    /// [`SimIndex::simulation_at`] does on the sending side.
    async fn xcm_simulation_at(
        &self,
        chain_id: &str,
        at_block_hash: &str,
        input_hash: &str,
        tier: &str,
    ) -> Result<Option<XcmSimulationRow>, IndexError>;

    /// The legs previewed FROM one call simulation — the stitch, read from the
    /// provenance columns rather than guessed at from program bytes. Two
    /// identical programs queued by two different calls are two facts, and only
    /// the source columns can tell them apart.
    async fn legs(
        &self,
        source_chain_id: &str,
        source_input_hash: &str,
        limit: u32,
    ) -> Result<Vec<XcmSimulationRow>, IndexError>;
}

/// Which of a run's forwarded messages are its own — rendered once, for both the
/// sending and the receiving side.
///
/// ONE BUILDER, TWO CALLERS, because the three shapes it distinguishes are the
/// whole honesty of the feature and they must not drift apart: no baseline at
/// all, a baseline link whose row is missing, and a real difference. The middle
/// one is not pedantry — a recorded link pointing at nothing is a bug, and
/// collapsing it into "no baseline" would hide it.
fn attribution_json(
    baseline_hash: Option<&str>,
    baseline: Option<(&serde_json::Value, Option<&str>)>,
    subject_forwarded: &serde_json::Value,
) -> serde_json::Value {
    let Some(hash) = baseline_hash else {
        return serde_json::json!({
            "baseline": null,
            "reads_as": "no no-op run was recorded at this state, so NOTHING in \
                         forwarded_xcms is attributable to this run — read it as 'messages \
                         present', never as 'this would send these'",
        });
    };
    let Some((baseline_forwarded, baseline_label)) = baseline else {
        return serde_json::json!({
            "baseline": hash,
            "reads_as": "this row names a baseline that is not in the index — the link is \
                         recorded and the row it points at is missing, so nothing is \
                         attributed here",
        });
    };
    let a = sim::attribute_forwarded(subject_forwarded, baseline_forwarded);
    let reads_as = if a.total_messages == 0 {
        "no forwarded messages were reported at all, so this run queues nothing of its own"
            .to_string()
    } else if a.attributed_messages == 0 {
        "every message in forwarded_xcms was already in flight at this state: this run \
         queues nothing of its own"
            .to_string()
    } else {
        format!(
            "{} of {} forwarded message(s) are this run's own; the other {} were already in \
             flight at this state",
            a.attributed_messages,
            a.total_messages,
            a.total_messages - a.attributed_messages
        )
    };
    serde_json::json!({
        "baseline": hash,
        "baseline_run": baseline_label,
        "attributed_messages": a.attributed_messages,
        "ambient_messages": a.ambient_messages,
        "total_messages": a.total_messages,
        "destinations": a.destinations,
        "reads_as": reads_as,
    })
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

    async fn simulation_at(
        &self,
        chain_id: &str,
        at_block_hash: &str,
        input_hash: &str,
        tier: &str,
    ) -> Result<Option<SimulationRow>, IndexError> {
        let rows = self.rows.read().map_err(|e| IndexError(e.to_string()))?;
        Ok(rows
            .iter()
            .find(|r| {
                r.chain_id == chain_id
                    && r.at_block_hash == at_block_hash
                    && r.input_hash == input_hash
                    && r.tier == tier
            })
            .cloned())
    }
}

/// The empty XCM-simulation backend — what a memory-mode node serves.
#[derive(Default)]
pub struct MemoryXcmSimIndex {
    rows: RwLock<Vec<XcmSimulationRow>>,
}

impl MemoryXcmSimIndex {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn insert(&self, row: XcmSimulationRow) {
        self.rows.write().expect("lock").push(row);
    }
}

#[async_trait]
impl XcmSimIndex for MemoryXcmSimIndex {
    async fn xcm_simulations(
        &self,
        chain_id: &str,
        program_hash: &str,
        limit: u32,
    ) -> Result<Vec<XcmSimulationRow>, IndexError> {
        let rows = self.rows.read().map_err(|e| IndexError(e.to_string()))?;
        let mut out: Vec<XcmSimulationRow> = rows
            .iter()
            .filter(|r| r.chain_id == chain_id && r.program_hash == program_hash)
            .cloned()
            .collect();
        sort_xcm_rows(&mut out);
        out.truncate(limit as usize);
        Ok(out)
    }

    async fn xcm_simulation_at(
        &self,
        chain_id: &str,
        at_block_hash: &str,
        input_hash: &str,
        tier: &str,
    ) -> Result<Option<XcmSimulationRow>, IndexError> {
        let rows = self.rows.read().map_err(|e| IndexError(e.to_string()))?;
        Ok(rows
            .iter()
            .find(|r| {
                r.chain_id == chain_id
                    && r.at_block_hash == at_block_hash
                    && r.input_hash == input_hash
                    && r.tier == tier
            })
            .cloned())
    }

    async fn legs(
        &self,
        source_chain_id: &str,
        source_input_hash: &str,
        limit: u32,
    ) -> Result<Vec<XcmSimulationRow>, IndexError> {
        let rows = self.rows.read().map_err(|e| IndexError(e.to_string()))?;
        let mut out: Vec<XcmSimulationRow> = rows
            .iter()
            .filter(|r| {
                r.source_chain_id.as_deref() == Some(source_chain_id)
                    && r.source_input_hash.as_deref() == Some(source_input_hash)
            })
            .cloned()
            .collect();
        // Legs are ordered by their position in the sender's own list, not by
        // height: they are a SEQUENCE the sender produced, and "newest first"
        // would shuffle a journey's own legs.
        out.sort_by(|a, b| {
            a.source_forwarded_index
                .cmp(&b.source_forwarded_index)
                .then_with(|| a.source_message_index.cmp(&b.source_message_index))
                .then_with(|| a.chain_id.cmp(&b.chain_id))
                .then_with(|| a.at_block_hash.cmp(&b.at_block_hash))
        });
        out.truncate(limit as usize);
        Ok(out)
    }
}

/// Byte-identical ordering to the Pg backend (Rust's String Ord is byte-wise,
/// which is what `collate "C"` asks Postgres for), full key as the tie-break —
/// without it `limit` returns different rows from the two backends.
fn sort_xcm_rows(rows: &mut [XcmSimulationRow]) {
    rows.sort_by(|a, b| {
        b.at_height
            .cmp(&a.at_height)
            .then_with(|| a.input_hash.cmp(&b.input_hash))
            .then_with(|| a.at_block_hash.cmp(&b.at_block_hash))
            .then_with(|| a.tier.cmp(&b.tier))
    });
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
         than predicting the enacted outcome. Tier 2 can be TOLD that state with an explicit \
         `--set` storage override; it does not discover or set it up on its own",
        "forwarded_xcms is not what the DESTINATION would do — that is dry_run_xcm ON THAT \
         CHAIN, and it is a separate run that somebody has to have made",
        "forwarded_xcms IS NOT ALWAYS ATTRIBUTABLE TO THE SIMULATED CALL, and a raw list on \
         a simulation row is never claimed to be: measured at relay #24448717 and #24448722, \
         a `system.remark` under Root — which queues nothing — returns 64 destinations \
         carrying 74 messages, including a real 8,935 DOT ReserveAssetDeposited bound for \
         parachain 2040, byte-identical across two different calls and two different blocks. \
         The list is a property of the STATE (the relay router enumerating every parachain's \
         existing downward queue), not of the call; Asset Hub returns an empty list for the \
         same shape of call. Read it as 'messages present at this state', never as 'this \
         call would send these' — /v1/sim/{chain}/calls/{call_hash} differences it against \
         the no-op baseline recorded beside it and reports what is attributable",
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

/// What a TIER 2 (fork) answer does not model.
///
/// A SEPARATE LIST FROM `sim_not_covered`, AND THAT IS THE POINT. This project
/// has four times shipped one shared `not_covered` whose first line was false on
/// its second consumer (slice 3's journey list, slice 4's boundary sentence,
/// slice 5's `legs`, slice 7's holdings line). Tier 1's list opens with "the call
/// is dispatched directly" — which is not what this tier does — so the two are
/// structurally separate and each row carries the one that is true of it, rather
/// than a shared list being carefully worded until it is true of both.
///
/// The first four lines are chopsticks' own FAQ answer to "what is mocked?",
/// quoted rather than paraphrased. The rest are this project's.
pub fn fork_not_covered() -> Vec<&'static str> {
    vec![
        "THE HARNESS MOCKS FOUR THINGS AND SAYS SO ITSELF: a mocked tx pool, no real block \
         finalization, mocked inherents, and simulated XCM channels. A diff that looks \
         authoritative while the inherents were mocked is the failure this tier is most likely \
         to produce, which is why the list is on the row as well as here",
        "`built_block_hash` is NULL on every row this tier writes today, because the live route \
         builds no block — it dry-runs one. Where an older row carries one it names a block no \
         canonical chain has, no explorer will find, and dotlens never wrote to core.blocks, and \
         it does NOT identify the counterfactual either: the harness builds every block with a \
         zero state root, so a faithful run and an injected one share it",
        "this models ENACTMENT and nothing before it — nothing here checks that a referendum \
         passed, that its track permits the origin it was given, or that its decision period \
         elapsed. Whether it would pass is the votes endpoint's question, not this one's",
        "the state is the state at the forked block, NOT the state at enactment. A referendum \
         that would enact in a week is previewed against today's balances and today's agenda",
        "WHERE THERE IS A DIFF AT ALL, IT COVERS THE `apply_extrinsic` PHASE ONLY. The harness \
         runs `Core_initialize_block` and the inherents and consumes each one's changes into a \
         storage layer it does not return, so what comes back is the writes of the single \
         extrinsic that was applied. `diff_status` says which of the five cases this row is and \
         `diff_covers` says whether that scope reaches this row's own call. Measured 2026-08-18 \
         from the harness's own source, after the previous version of this line claimed the only \
         omission was System.Events and that it was not a gap",
        "THE DIFF BELOW HAS NO System.Events ENTRY, and its absence is the reason \
         `emitted_events` is trustworthy on a row whose diff is not. The harness's raw answer \
         does carry that one key with the WHOLE block's events in it — `apply_extrinsic` reads \
         the existing list and appends to it, so what it writes includes everything \
         `on_initialize` emitted — and dotlens lifts it out into `emitted_events` and excludes \
         it from `storage_diff`, because its 'before' is last block's events and tells a reader \
         nothing",
        "a diff entry names what the metadata lets it name. A key argument under a NON-CONCAT \
         hasher (Blake2_128, Twox64, Twox128, Twox256, Blake2_256) keeps no copy of the value it \
         hashed, so those arguments are reported as unknown rather than guessed — `args_complete` \
         says which",
        "`diff_status` distinguishes five things a boolean could not: `decoded` (every phase of \
         the block was returned and read), `extrinsic_only` (the bytes were read and cover the \
         applied extrinsic and nothing before it — the ordinary case on the live route), \
         `undecodable` (a diff came back in a shape this version cannot read; the bytes are \
         archived and nothing is guessed), `unavailable` (this build of the harness has no diff \
         method at all) and `refused` (it HAS one and it failed on this block — the ordinary \
         case for the whole-block method on a PARACHAIN, where the harness builds a block \
         without set_validation_data and then cannot re-execute it). The events and the dispatch \
         verdict are read before the diff is asked for and are unaffected by any of them. 'We \
         did not look', 'nothing changed' and 'we looked at the wrong half' are never the same \
         value",
        "runtime CONSTANTS cannot be changed by this tier at all — the harness says so in its \
         own FAQ, and changing one needs a replacement wasm rather than a storage override",
        "the harness VERSION is recorded on the row and is NOT part of the cache key, so a \
         cached answer may have been produced by a different chopsticks than the one installed \
         now. `harness.version` says which. Unlike a Tier 1 row a fork row is not re-derivable \
         from archived bytes alone — re-deriving it means re-running it",
        "a spend that was approved and enacted and then FAILED AT PAYOUT is not this tier's \
         question and is already answered without simulating anything: \
         /v1/treasury/{network}/spends/{id} distinguishes SpendProcessed from paid and records \
         PaymentFailed. Simulation answers 'what WOULD happen', never 'what did'",
    ]
}

/// What a fork row does not model, CHOSEN BY ITS ROUTE.
///
/// The two routes differ in the one line Tier 1's list opens with. A scheduled
/// dispatch is not an extrinsic, so no transaction extension runs; an applied
/// extrinsic runs the whole pipeline with only the signature faked. Serving one
/// sentence for both would be false half the time, which is the shared-list
/// defect this project has now shipped five times — so the sentence is selected
/// from `dispatch_route` rather than worded to cover both.
pub fn fork_route_not_covered(route: Option<&str>) -> Vec<&'static str> {
    let mut out = fork_not_covered();
    match route {
        Some(sim::ROUTE_DRY_RUN_EXTRINSIC) => {
            out.push(
                "this row's call was APPLIED AS AN EXTRINSIC, so unlike every other simulation \
                 in this project the transaction extensions DID run — nonce, mortality, fee \
                 withdrawal and weight are real. Only the SIGNATURE is faked, and the fee came \
                 out of the signer's balance, which is why that account appears in the diff",
            );
            out.push(
                "no scheduler was involved and no agenda entry was written, so this row has no \
                 `agenda_anchor` and nothing was REPLACED in the chain's own schedule",
            );
            out.push(
                "ON THIS ROUTE AN `extrinsic_only` DIFF IS COMPLETE, not a limitation: the \
                 subject IS the applied extrinsic, so its whole effect is in the phase the diff \
                 covers. A whole-block diff here would be worse for attribution — it would mix \
                 in every pallet's on_initialize bookkeeping, which this call did not do",
            );
        }
        _ => {
            out.push(
                "this row's call was DISPATCHED BY THE SCHEDULER, not by the chain: one task was \
                 written into the agenda under the origin the caller named, at the height \
                 `agenda_anchor` names — which on a parachain whose scheduler is relay-anchored \
                 is on the RELAY's number line, and is the parent's value rather than the next \
                 one, because a scheduler's `now` in `on_initialize` has not seen the block's \
                 own inherents yet",
            );
            out.push(
                "so NO TRANSACTION EXTENSION RAN: no signature, no nonce, no mortality, no fee, \
                 no weight limit on the call itself, no tip, no priority and no pool admission. \
                 The extrinsic in the diff is a no-op whose only job was to make the block \
                 execute; its signer paid that fee and nothing else, and its keys are marked \
                 `from_harness`",
            );
            out.push(
                "AND THAT IS WHY AN `extrinsic_only` DIFF ON THIS ROW DOES NOT DESCRIBE ITS CALL. \
                 The scheduler dispatches in `on_initialize`, which is exactly the phase that \
                 scope omits, so the entries below would be the NO-OP VEHICLE's writes — its \
                 fee, its nonce, the block's own bookkeeping. Measured: between a run that \
                 dispatched nothing and a run that funded a bounty, the diff key set was \
                 identical while the events grew. `diff_covers` on this row says whether that is \
                 the case here; where it is, `emitted_events` is the complete record of what the \
                 call did, and nothing is synthesised to fill the gap",
            );
        }
    }
    out
}

/// The limits of a COUNTERFACTUAL — shipped only by rows that have one.
pub fn counterfactual_not_covered() -> Vec<&'static str> {
    vec![
        "EVERY NUMBER DOWNSTREAM OF AN OVERRIDDEN KEY IS FABRICATED. This row is not a record of \
         anything that happened; it is what a runtime would have done had its storage held what \
         the `overrides` below say it was told to hold",
        "each override carries `before`, read from the REAL chain at this block before anything \
         was forked, beside the value that was injected. A `before` of null means the key did \
         NOT EXIST on the real chain — the override created it, which is a stronger fabrication \
         than changing a number",
        "an override sets ONE key and reconciles nothing around it. Raising a balance does not \
         update the asset's total supply or its account count, and a runtime that checks those \
         invariants may behave in ways the real chain never would",
        "a diff entry marked `from_override: true` has this run's own INJECTED value as its \
         `before`, not what the chain held. It is a consequence of the counterfactual and reads \
         like history if the flag is ignored",
    ]
}

/// The limits of the ATTRIBUTION, shipped by the two endpoints that compute one
/// and by nothing else.
///
/// SEPARATE FROM `sim_not_covered` ON PURPOSE, and it carries no line about
/// `legs`. These lines name a field — `forwarded_attribution` — and a line that
/// describes a field belongs only to responses that HAVE the field. This project
/// has twice shipped a shared `not_covered` helper whose first line was false on
/// the second endpoint that served it (slice 3's journey list, slice 4's
/// boundary sentence); `legs` exists on the call endpoint alone and is described
/// there alone.
pub fn sim_attribution_not_covered() -> Vec<&'static str> {
    vec![
        "`forwarded_attribution` is forwarded_xcms MINUS a no-op run at the same state, and \
         it is the only half of this response that may be read as 'this run would send \
         these'. When it reports `baseline: null` no no-op was recorded and nothing here is \
         attributable at all",
        "attribution is a MULTISET DIFFERENCE by exact rendering, so a message this run \
         really sends that is byte-identical to one already in flight is counted as ambient \
         and drops out of the attributed set. That under-claims rather than over-claims — \
         and the raw forwarded_xcms is kept beside it so the discrepancy is visible",
    ]
}

/// What a PREVIEWED ARRIVAL does not model. Shipped with every `dry_run_xcm`
/// response and with every `legs` entry, because the receiving side has limits
/// the sending side does not — and two of them are about TIME rather than about
/// XCM.
pub fn xcm_sim_not_covered() -> Vec<&'static str> {
    vec![
        "the receiving chain is previewed at ITS OWN state now, not at the state the message \
         would actually arrive in. A cross-chain message takes blocks to travel and the two \
         chains do not share a clock, so a leg previewed against today's Hydration is a \
         statement about today's reserves, fees and asset registry",
        "delivery is ASSUMED. This answers 'if this program arrived, what would happen' — it \
         does not model the channel: a message that is never queued, dropped by a \
         weight-starved XCMP enqueue, or stuck behind an unopened HRMP channel would still \
         preview exactly like one that arrives",
        "a status of `not_started` is upstream's Outcome::Error and means execution never \
         began — usually a barrier rejection. It is the one outcome the SENDING chain cannot \
         see: its own `Sent` event, and therefore our own indexed sending half, would look \
         perfectly successful",
        "the origin location is what the caller (or the registry, for a followed leg) said the \
         sender is. Barriers and origin conversion turn on exactly that value, so a preview \
         from the wrong origin fails plausibly rather than obviously",
        "this row's own forwarded_xcms carries the same ambient-traffic caveat the call side \
         does. On /v1/sim/{chain}/xcm/{program_hash} it is differenced against the \
         EMPTY-PROGRAM run recorded at the same state and the result is \
         `forwarded_attribution`; anywhere a row is embedded WITHOUT that field — as a `leg` \
         of a call simulation, say — the list is raw, and reads as 'messages present at this \
         state' and never as 'this program would send these'",
        "the baseline is an empty program AT THE SUBJECT'S OWN XCM VERSION, because \
         dry_run_xcm renders its forwarded list in the version it was asked in. A subject at \
         V4 and a baseline at V5 would produce two lists that cannot be differenced, and \
         every ambient message would then be attributed to this program",
        "no fee estimate and no weight limit: XcmPaymentApi is not called by this tier yet, so \
         a program that would run out of purchased weight on arrival is not distinguished \
         here from one that would not",
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

    /// Every representation of ONE logical asset, across every chain, found by
    /// its observer-free name.
    ///
    /// **THIS IS THE READER THAT EARNS `assets_absolute_key_idx`**, and the
    /// distinction matters because migration 0019 deliberately withheld that
    /// index with the note that the query earning it "does not exist yet and
    /// should arrive WITH its index" (0013 is the precedent for what happens
    /// otherwise). Note which query it is: the CONSOLIDATION grouping does NOT
    /// earn it — that walks a known set of (chain, asset_key) pairs and reaches
    /// `core.assets` through its primary key. It is this REVERSE lookup —
    /// "given an absolute name, which representations exist" — that has no
    /// other path and would otherwise scan.
    async fn representations(
        &self,
        absolute_key: &str,
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

    async fn representations(
        &self,
        absolute_key: &str,
    ) -> Result<Vec<(String, AssetRow)>, IndexError> {
        let map = self.assets.read().map_err(|e| IndexError(e.to_string()))?;
        let mut out: Vec<(String, AssetRow)> = map
            .iter()
            .flat_map(|(chain, rows)| {
                rows.iter()
                    // EXACT match, never a prefix or a contains. An absolute key
                    // is a canonical rendering, so equality is the whole
                    // relation — and `search`'s own rule ("NEVER prefix-search
                    // a hash") applies here for the same reason: a prefix over
                    // this column is a range scan on an identifier.
                    .filter(|r| r.absolute_key.as_deref() == Some(absolute_key))
                    .map(move |r| (chain.clone(), r.clone()))
            })
            .collect();
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
    /// The frozen (locked) portion of a NATIVE-shaped anchor, where the runtime
    /// exposes one. Recorded since Phase 1 and read by NOTHING until now — which
    /// was a real gap on a treasury surface, because a position that is largely
    /// frozen is not a position that can be spent. Null for pallet-assets
    /// anchors, which have no such column, and null where the runtime does not
    /// expose it; never zero as a stand-in for either.
    pub anchor_frozen: Option<String>,
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

// ------------------------------------------------------------------- coretime

/// One core's work over a window: how many para-blocks it produced, and for
/// whom.
///
/// `included_blocks` counts `kind = 'included'` ROWS ONLY. `backed` rows sit in
/// the same table (they are the wasted-coretime signal) and counting them here
/// would roughly double every figure, because async backing means almost every
/// inclusion has a backing 2–6 blocks earlier.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct CoreOccupancyRow {
    pub core_index: u32,
    pub included_blocks: u64,
    /// Every para this core carried in the window, ascending.
    ///
    /// In the prep's 1,000-block sample every one of the 47 used cores served
    /// exactly ONE para — core→para rotation is UNEXERCISED on live data, so a
    /// list with two entries here would be the first live instance of the
    /// time-ranged half of the core↔chain many-to-many, and is worth noticing
    /// rather than averaging away.
    pub paras: Vec<u32>,
}

/// One dated reading of the denominator.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct CoreConfigRow {
    pub block_height: u64,
    pub num_cores: u32,
    pub runtime_version: u64,
}

/// How much of the requested window we actually hold.
///
/// BOTH NUMBERS MATTER AND THEY ANSWER DIFFERENT QUESTIONS. `blocks_indexed` is
/// the slot-fill DENOMINATOR — a window with gaps must not be divided by its
/// nominal span, or the ratio silently drops with every missing block.
/// `heights_with_occupancy` is the DIAGNOSTIC: a block that is decoded but not
/// yet coretime-mapped counts in the denominator and contributes nothing, and
/// the gap between these two is the only way to see that from outside.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct WindowCoverage {
    pub blocks_indexed: u64,
    pub heights_with_occupancy: u64,
}

#[async_trait]
pub trait CoretimeIndex: Send + Sync {
    /// Per-core inclusion counts over `[from, to]`, ascending by core index.
    async fn occupancy_by_core(
        &self,
        chain_id: &str,
        from: u64,
        to: u64,
    ) -> Result<Vec<CoreOccupancyRow>, IndexError>;
    /// Inclusion counts per (core, para) over `[from, to]`, ascending.
    ///
    /// PER (core, para) AND NOT PER CORE, because the delta attributes occupancy
    /// to the task that was ENTITLED to it. A core that carried two paras has
    /// part of its occupancy attributable and part not, and a per-core total
    /// cannot express that — it would force the reader to call the whole core
    /// agreeing or the whole core disagreeing, and either is a wrong number
    /// rather than a coarse one.
    ///
    /// Core->para rotation is unexercised on live data (all 47 used cores in the
    /// measured window served exactly one para), so this is the shape the reader
    /// is designed against rather than one anyone has seen. It needs NO NEW
    /// INDEX: `core_index` is unconstrained here, so the `(chain_id,
    /// block_height)` PK prefix serves the range directly, exactly as
    /// `occupancy_by_core` does.
    async fn occupancy_by_core_and_para(
        &self,
        chain_id: &str,
        from: u64,
        to: u64,
    ) -> Result<Vec<coretime_delta::OccupancyCell>, IndexError>;
    /// Row counts per `kind` — so a reader can see how much `backed` and
    /// `timed_out` sit beside the `included` rows the ratios are built from.
    async fn kind_counts(
        &self,
        chain_id: &str,
        from: u64,
        to: u64,
    ) -> Result<Vec<(String, u64)>, IndexError>;
    /// How much of the window is indexed, and how much of it carries occupancy.
    ///
    /// Reads `core.blocks` even though it lives on the coretime index: the
    /// slot-fill denominator is a coretime question, and answering it in the
    /// handler by way of a second index would let the two drift apart on which
    /// blocks "count".
    async fn window_coverage(
        &self,
        chain_id: &str,
        from: u64,
        to: u64,
    ) -> Result<WindowCoverage, IndexError>;
    /// The newest reading at or before `height`.
    async fn core_config_at_or_before(
        &self,
        chain_id: &str,
        height: u64,
    ) -> Result<Option<CoreConfigRow>, IndexError>;
    /// The oldest reading strictly after `height` — used ONLY when nothing was
    /// read at or before it, so a ratio can still be served while saying plainly
    /// that its denominator was read after the window it divides.
    async fn core_config_after(
        &self,
        chain_id: &str,
        height: u64,
    ) -> Result<Option<CoreConfigRow>, IndexError>;
    /// The highest core index seen in the window ACROSS EVERY KIND.
    ///
    /// Deliberately not the max over the inclusion counts: a core that appears
    /// only in `backed` or `timed_out` rows is still a core the runtime
    /// scheduled work onto, and the stale-denominator detector must see it. A
    /// detector that read only inclusions would claim to look at "the data"
    /// while ignoring rows the same response reports in `by_kind`.
    async fn max_core_index(
        &self,
        chain_id: &str,
        from: u64,
        to: u64,
    ) -> Result<Option<u32>, IndexError>;
    /// `(runtime_version, mapper_version, rows)` for the window, ascending.
    ///
    /// LINEAGE, WHICH INVARIANT 3 REQUIRES OF EVERY ROW AND THEREFORE OF EVERY
    /// AGGREGATE OVER ROWS. More than one `mapper_version` here means the window
    /// was mapped under two RULE SETS and the counts are an average of them —
    /// which is exactly the case where a ratio is not comparable with itself,
    /// and the one thing a bare percentage can never tell you.
    async fn occupancy_lineage(
        &self,
        chain_id: &str,
        from: u64,
        to: u64,
    ) -> Result<Vec<(u64, u32, u64)>, IndexError>;
    /// Every DISTINCT `num_cores` value observed inside `[from, to]`, ascending.
    ///
    /// More than one means the denominator MOVED inside the window — `num_cores`
    /// is host configuration that changes at session boundaries — and a single
    /// ratio across it is an average of two different questions. The handler
    /// reports it rather than picking a winner quietly.
    async fn num_cores_in_window(
        &self,
        chain_id: &str,
        from: u64,
        to: u64,
    ) -> Result<Vec<u32>, IndexError>;
}

#[derive(Default)]
pub struct MemoryCoretimeIndex {
    /// (chain, height, event_index) → (kind, core, para)
    rows: RwLock<Vec<(String, u64, u32, String, u32, u32)>>,
    configs: RwLock<Vec<(String, CoreConfigRow)>>,
    /// Heights present in `core.blocks`, as far as this index is concerned.
    indexed: RwLock<Vec<(String, u64)>>,
}

impl MemoryCoretimeIndex {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn insert_row(
        &self,
        chain_id: &str,
        height: u64,
        event_index: u32,
        kind: &str,
        core_index: u32,
        para_id: u32,
    ) {
        self.rows.write().expect("lock").push((
            chain_id.into(),
            height,
            event_index,
            kind.into(),
            core_index,
            para_id,
        ));
    }
    pub fn insert_config(&self, chain_id: &str, row: CoreConfigRow) {
        self.configs.write().expect("lock").push((chain_id.into(), row));
    }
    pub fn insert_indexed_height(&self, chain_id: &str, height: u64) {
        self.indexed.write().expect("lock").push((chain_id.into(), height));
    }
}

#[async_trait]
impl CoretimeIndex for MemoryCoretimeIndex {
    async fn occupancy_by_core(
        &self,
        chain_id: &str,
        from: u64,
        to: u64,
    ) -> Result<Vec<CoreOccupancyRow>, IndexError> {
        let rows = self.rows.read().map_err(|e| IndexError(e.to_string()))?;
        let mut by_core: std::collections::BTreeMap<u32, (u64, std::collections::BTreeSet<u32>)> = std::collections::BTreeMap::new();
        for (c, h, _, kind, core, para) in rows.iter() {
            if c != chain_id || *h < from || *h > to || kind != "included" {
                continue;
            }
            let e = by_core.entry(*core).or_insert_with(|| (0, std::collections::BTreeSet::new()));
            e.0 += 1;
            e.1.insert(*para);
        }
        Ok(by_core
            .into_iter()
            .map(|(core_index, (included_blocks, paras))| CoreOccupancyRow {
                core_index,
                included_blocks,
                paras: paras.into_iter().collect(),
            })
            .collect())
    }

    async fn occupancy_by_core_and_para(
        &self,
        chain_id: &str,
        from: u64,
        to: u64,
    ) -> Result<Vec<coretime_delta::OccupancyCell>, IndexError> {
        let rows = self.rows.read().map_err(|e| IndexError(e.to_string()))?;
        let mut by: std::collections::BTreeMap<(u32, u32), u64> = std::collections::BTreeMap::new();
        for (c, h, _, kind, core, para) in rows.iter() {
            if c != chain_id || *h < from || *h > to || kind != "included" {
                continue;
            }
            *by.entry((*core, *para)).or_default() += 1;
        }
        Ok(by
            .into_iter()
            .map(|((core_index, para_id), included_blocks)| coretime_delta::OccupancyCell {
                core_index,
                para_id,
                included_blocks,
            })
            .collect())
    }

    async fn kind_counts(
        &self,
        chain_id: &str,
        from: u64,
        to: u64,
    ) -> Result<Vec<(String, u64)>, IndexError> {
        let rows = self.rows.read().map_err(|e| IndexError(e.to_string()))?;
        let mut counts: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
        for (c, h, _, kind, _, _) in rows.iter() {
            if c != chain_id || *h < from || *h > to {
                continue;
            }
            *counts.entry(kind.clone()).or_default() += 1;
        }
        Ok(counts.into_iter().collect())
    }

    async fn window_coverage(
        &self,
        chain_id: &str,
        from: u64,
        to: u64,
    ) -> Result<WindowCoverage, IndexError> {
        let indexed = self.indexed.read().map_err(|e| IndexError(e.to_string()))?;
        let blocks: std::collections::BTreeSet<u64> = indexed
            .iter()
            .filter(|(c, h)| c == chain_id && *h >= from && *h <= to)
            .map(|(_, h)| *h)
            .collect();
        let rows = self.rows.read().map_err(|e| IndexError(e.to_string()))?;
        let with_rows: std::collections::BTreeSet<u64> = rows
            .iter()
            .filter(|(c, h, ..)| c == chain_id && *h >= from && *h <= to)
            .map(|(_, h, ..)| *h)
            .collect();
        Ok(WindowCoverage {
            blocks_indexed: blocks.len() as u64,
            heights_with_occupancy: with_rows.len() as u64,
        })
    }

    async fn core_config_at_or_before(
        &self,
        chain_id: &str,
        height: u64,
    ) -> Result<Option<CoreConfigRow>, IndexError> {
        let configs = self.configs.read().map_err(|e| IndexError(e.to_string()))?;
        Ok(configs
            .iter()
            .filter(|(c, r)| c == chain_id && r.block_height <= height)
            .max_by_key(|(_, r)| r.block_height)
            .map(|(_, r)| r.clone()))
    }

    async fn core_config_after(
        &self,
        chain_id: &str,
        height: u64,
    ) -> Result<Option<CoreConfigRow>, IndexError> {
        let configs = self.configs.read().map_err(|e| IndexError(e.to_string()))?;
        Ok(configs
            .iter()
            .filter(|(c, r)| c == chain_id && r.block_height > height)
            .min_by_key(|(_, r)| r.block_height)
            .map(|(_, r)| r.clone()))
    }

    async fn max_core_index(
        &self,
        chain_id: &str,
        from: u64,
        to: u64,
    ) -> Result<Option<u32>, IndexError> {
        let rows = self.rows.read().map_err(|e| IndexError(e.to_string()))?;
        Ok(rows
            .iter()
            .filter(|(c, h, ..)| c == chain_id && *h >= from && *h <= to)
            .map(|(_, _, _, _, core, _)| *core)
            .max())
    }

    async fn occupancy_lineage(
        &self,
        chain_id: &str,
        from: u64,
        to: u64,
    ) -> Result<Vec<(u64, u32, u64)>, IndexError> {
        // The memory index carries no per-row lineage — it exists to exercise
        // the HANDLER, and a fabricated runtime version would be worse than an
        // absence. The handler renders an empty list as "no lineage recorded".
        let _ = (chain_id, from, to);
        Ok(vec![])
    }

    async fn num_cores_in_window(
        &self,
        chain_id: &str,
        from: u64,
        to: u64,
    ) -> Result<Vec<u32>, IndexError> {
        let configs = self.configs.read().map_err(|e| IndexError(e.to_string()))?;
        let set: std::collections::BTreeSet<u32> = configs
            .iter()
            .filter(|(c, r)| c == chain_id && r.block_height >= from && r.block_height <= to)
            .map(|(_, r)| r.num_cores)
            .collect();
        Ok(set.into_iter().collect())
    }
}

// ------------------------------------------------------------------ broker

/// One `Broker.*` event, for the entitlement timeline.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct BrokerEventRow {
    pub block_height: u64,
    pub event_index: u32,
    /// The variant WITHOUT the pallet prefix, exactly as the runtime spells it.
    pub variant: String,
    pub core_index: Option<u32>,
    pub task_id: Option<u32>,
    /// The whole decoded payload, kept intact (schema-on-read).
    pub data: serde_json::Value,
    pub runtime_version: u64,
    pub mapper_version: u32,
}

/// One dated reading of the entitlement side's denominator and sale geometry.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct BrokerConfigRow {
    /// A CORETIME chain height. Not comparable with a relay height, and
    /// therefore never positioned against a relay window — see the delta's
    /// coverage list.
    pub block_height: u64,
    pub core_count: u32,
    /// `SaleInfo.first_core`: cores below it are reserved system cores. NULL
    /// when sales never started or when the reading predates 0026.
    pub first_core: Option<u32>,
    pub runtime_version: u64,
}

/// Reads over the ENTITLEMENT half — `coretime.broker_events`,
/// `coretime.core_assignments` and `coretime.broker_config`.
///
/// 0025 shipped all three tables and no reader for any of them, and dropped the
/// four indexes it had described because "the query that WOULD earn this index
/// does not exist yet and should arrive WITH its index". This trait is those
/// queries; migration 0026 is those indexes.
#[async_trait]
pub trait BrokerIndex: Send + Sync {
    /// The assignment GOVERNING each core at `relay_height` — the newest
    /// announcement at or before it, expanded into its `(kind, task, parts)`
    /// rows.
    ///
    /// THE LOOKBACK IS UNBOUNDED ON PURPOSE. `Broker.CoreAssigned` fires only at
    /// sale boundaries, and the sale governing the measured 1,000-block window
    /// sat ~157,000 coretime blocks before it — so "look recently" would find
    /// nothing and then have to decide what nothing means.
    ///
    /// ONE ANNOUNCEMENT PER CORE, not one relay block per core: two rows sharing
    /// a `relay_block` may come from two different announcing events (a
    /// re-announcement), and returning both would make one core look interlaced.
    /// The newest announcing coordinate wins.
    async fn entitlement_at(
        &self,
        chain_id: &str,
        relay_height: u64,
    ) -> Result<Vec<coretime_delta::EntitlementRow>, IndexError>;

    /// The newest `broker_config` reading for this chain, with its own height.
    ///
    /// NEWEST RATHER THAN AT-OR-BEFORE-THE-WINDOW, because there is no
    /// at-or-before to ask: this reading is dated on the CORETIME chain's number
    /// line and the window is relay heights. The reader states the height and
    /// refuses to imply the two are the same instant.
    async fn latest_broker_config(
        &self,
        chain_id: &str,
    ) -> Result<Option<BrokerConfigRow>, IndexError>;

    /// `Broker.*` events naming this core, newest first.
    async fn events_for_core(
        &self,
        chain_id: &str,
        core_index: u32,
        limit: u32,
    ) -> Result<Vec<BrokerEventRow>, IndexError>;
    /// `Broker.*` events naming this task, newest first.
    async fn events_for_task(
        &self,
        chain_id: &str,
        task_id: u32,
        limit: u32,
    ) -> Result<Vec<BrokerEventRow>, IndexError>;
    /// Assignments on this core, newest relay block first.
    async fn assignments_for_core(
        &self,
        chain_id: &str,
        core_index: u32,
        limit: u32,
    ) -> Result<Vec<coretime_delta::EntitlementRow>, IndexError>;
    /// Assignments naming this task, newest relay block first — the query that
    /// follows a TENANT across sale cycles, which following a core index cannot
    /// do because a renewal moves it.
    async fn assignments_for_task(
        &self,
        chain_id: &str,
        task_id: u32,
        limit: u32,
    ) -> Result<Vec<coretime_delta::EntitlementRow>, IndexError>;
}

#[derive(Default)]
pub struct MemoryBrokerIndex {
    events: RwLock<Vec<(String, BrokerEventRow)>>,
    assignments: RwLock<Vec<(String, u64, u32, coretime_delta::EntitlementRow)>>,
    configs: RwLock<Vec<(String, BrokerConfigRow)>>,
}

impl MemoryBrokerIndex {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn insert_event(&self, chain_id: &str, row: BrokerEventRow) {
        self.events.write().expect("lock").push((chain_id.into(), row));
    }
    /// `block_height` and `event_index` are the ANNOUNCING coordinate — the
    /// tie-break that keeps a re-announcement from looking like interlacing.
    pub fn insert_assignment(
        &self,
        chain_id: &str,
        block_height: u64,
        event_index: u32,
        row: coretime_delta::EntitlementRow,
    ) {
        self.assignments
            .write()
            .expect("lock")
            .push((chain_id.into(), block_height, event_index, row));
    }
    pub fn insert_config(&self, chain_id: &str, row: BrokerConfigRow) {
        self.configs.write().expect("lock").push((chain_id.into(), row));
    }
}

#[async_trait]
impl BrokerIndex for MemoryBrokerIndex {
    async fn entitlement_at(
        &self,
        chain_id: &str,
        relay_height: u64,
    ) -> Result<Vec<coretime_delta::EntitlementRow>, IndexError> {
        let rows = self.assignments.read().map_err(|e| IndexError(e.to_string()))?;
        // The winning ANNOUNCEMENT per core: newest relay_block, then newest
        // announcing coordinate. Mirrors the pg `distinct on` exactly, or the
        // two backends disagree about which entitlement governs.
        let mut best: std::collections::BTreeMap<u32, (u64, u64, u32)> =
            std::collections::BTreeMap::new();
        for (c, h, ei, r) in rows.iter() {
            if c != chain_id || r.relay_block > relay_height {
                continue;
            }
            let key = (r.relay_block, *h, *ei);
            best.entry(r.core_index)
                .and_modify(|cur| {
                    if key > *cur {
                        *cur = key;
                    }
                })
                .or_insert(key);
        }
        let mut out: Vec<coretime_delta::EntitlementRow> = rows
            .iter()
            .filter(|(c, h, ei, r)| {
                c == chain_id && best.get(&r.core_index) == Some(&(r.relay_block, *h, *ei))
            })
            .map(|(_, _, _, r)| r.clone())
            .collect();
        out.sort_by_key(|r| (r.core_index, r.assignment_index));
        Ok(out)
    }

    async fn latest_broker_config(
        &self,
        chain_id: &str,
    ) -> Result<Option<BrokerConfigRow>, IndexError> {
        let configs = self.configs.read().map_err(|e| IndexError(e.to_string()))?;
        Ok(configs
            .iter()
            .filter(|(c, _)| c == chain_id)
            .max_by_key(|(_, r)| r.block_height)
            .map(|(_, r)| r.clone()))
    }

    async fn events_for_core(
        &self,
        chain_id: &str,
        core_index: u32,
        limit: u32,
    ) -> Result<Vec<BrokerEventRow>, IndexError> {
        self.events_where(chain_id, limit, |r| r.core_index == Some(core_index))
    }

    async fn events_for_task(
        &self,
        chain_id: &str,
        task_id: u32,
        limit: u32,
    ) -> Result<Vec<BrokerEventRow>, IndexError> {
        self.events_where(chain_id, limit, |r| r.task_id == Some(task_id))
    }

    async fn assignments_for_core(
        &self,
        chain_id: &str,
        core_index: u32,
        limit: u32,
    ) -> Result<Vec<coretime_delta::EntitlementRow>, IndexError> {
        self.assignments_where(chain_id, limit, |r| r.core_index == core_index)
    }

    async fn assignments_for_task(
        &self,
        chain_id: &str,
        task_id: u32,
        limit: u32,
    ) -> Result<Vec<coretime_delta::EntitlementRow>, IndexError> {
        self.assignments_where(chain_id, limit, |r| r.task_id == Some(task_id))
    }
}

impl MemoryBrokerIndex {
    fn events_where(
        &self,
        chain_id: &str,
        limit: u32,
        pred: impl Fn(&BrokerEventRow) -> bool,
    ) -> Result<Vec<BrokerEventRow>, IndexError> {
        let rows = self.events.read().map_err(|e| IndexError(e.to_string()))?;
        let mut out: Vec<BrokerEventRow> = rows
            .iter()
            .filter(|(c, r)| c == chain_id && pred(r))
            .map(|(_, r)| r.clone())
            .collect();
        out.sort_by(|a, b| {
            (b.block_height, b.event_index).cmp(&(a.block_height, a.event_index))
        });
        out.truncate(limit as usize);
        Ok(out)
    }

    fn assignments_where(
        &self,
        chain_id: &str,
        limit: u32,
        pred: impl Fn(&coretime_delta::EntitlementRow) -> bool,
    ) -> Result<Vec<coretime_delta::EntitlementRow>, IndexError> {
        let rows = self.assignments.read().map_err(|e| IndexError(e.to_string()))?;
        let mut out: Vec<(u64, u32, coretime_delta::EntitlementRow)> = rows
            .iter()
            .filter(|(c, _, _, r)| c == chain_id && pred(r))
            .map(|(_, h, ei, r)| (*h, *ei, r.clone()))
            .collect();
        // relay_block desc, announcing coordinate desc, ordinal ASC — the same
        // ordering the pg query spells, because two backends that disagree
        // about ordering return different rows under `limit`.
        out.sort_by_key(|(h, ei, r)| {
            (
                std::cmp::Reverse(r.relay_block),
                std::cmp::Reverse(*h),
                std::cmp::Reverse(*ei),
                r.assignment_index,
            )
        });
        out.truncate(limit as usize);
        Ok(out.into_iter().map(|(_, _, r)| r).collect())
    }
}

// ------------------------------------------------------ hrmp channel readings

/// One reading of the HRMP channel graph — the header row that says we looked.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ChannelReadingRow {
    pub block_height: u64,
    pub session_index: u64,
    pub channel_count: u32,
    pub open_request_count: u32,
    pub topology_digest: String,
    pub spec_version: u64,
    pub source: String,
}

/// One directed edge as of one reading.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ChannelEdgeRow {
    pub sender: u32,
    pub recipient: u32,
    pub state: String,
    pub max_capacity: u32,
    pub max_total_size: u32,
    pub max_message_size: u32,
    pub sender_deposit: String,
    pub recipient_deposit: Option<String>,
    pub confirmed: Option<bool>,
}

/// Reading the HRMP channel graph out of `xcm.channel_readings` /
/// `xcm.channel_snapshots`.
///
/// Note what is NOT here: any method returning "when did this channel open".
/// That is a DIFFERENCE between readings, computed by [`channels::derive_history`]
/// from what `edge_observations` returns, and never stored — see 0027.
#[async_trait]
pub trait ChannelIndex: Send + Sync {
    /// Every reading on record for this chain, ascending by height. The coverage
    /// walk needs all of them, so this is deliberately unfiltered; the graph is
    /// ~2,190 readings/year and the whole of Polkadot's history is ~9,331.
    async fn readings(&self, chain_id: &str) -> Result<Vec<ChannelReadingRow>, IndexError>;

    /// The newest reading at or before `at`, or the newest of all when `at` is
    /// `None`. `None` means no reading is on record — which is NOT the same as
    /// an empty graph, and the endpoint says so.
    async fn reading_at(
        &self,
        chain_id: &str,
        at: Option<u64>,
    ) -> Result<Option<ChannelReadingRow>, IndexError>;

    /// The full edge set of one reading, ordered by `(sender, recipient)`.
    async fn edges_at(
        &self,
        chain_id: &str,
        block_height: u64,
    ) -> Result<Vec<ChannelEdgeRow>, IndexError>;

    /// One edge's state at EVERY reading, ascending by height — a left join, so
    /// a reading in which the edge was absent yields `state: None` rather than
    /// no row. That asymmetry is the whole point: the reading's presence is what
    /// makes the absence meaningful.
    async fn edge_observations(
        &self,
        chain_id: &str,
        sender: u32,
        recipient: u32,
    ) -> Result<Vec<channels::EdgeObservation>, IndexError>;
}

#[derive(Default)]
pub struct MemoryChannelIndex {
    readings: RwLock<Vec<(String, ChannelReadingRow)>>,
    edges: RwLock<Vec<(String, u64, ChannelEdgeRow)>>,
}

impl MemoryChannelIndex {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn insert_reading(&self, chain_id: &str, row: ChannelReadingRow) {
        self.readings.write().expect("lock").push((chain_id.into(), row));
    }
    pub fn insert_edge(&self, chain_id: &str, block_height: u64, row: ChannelEdgeRow) {
        self.edges
            .write()
            .expect("lock")
            .push((chain_id.into(), block_height, row));
    }
}

#[async_trait]
impl ChannelIndex for MemoryChannelIndex {
    async fn readings(&self, chain_id: &str) -> Result<Vec<ChannelReadingRow>, IndexError> {
        let rows = self.readings.read().map_err(|e| IndexError(e.to_string()))?;
        let mut out: Vec<ChannelReadingRow> = rows
            .iter()
            .filter(|(c, _)| c == chain_id)
            .map(|(_, r)| r.clone())
            .collect();
        out.sort_by_key(|r| r.block_height);
        Ok(out)
    }

    async fn reading_at(
        &self,
        chain_id: &str,
        at: Option<u64>,
    ) -> Result<Option<ChannelReadingRow>, IndexError> {
        let rows = self.readings(chain_id).await?;
        Ok(rows
            .into_iter()
            .filter(|r| at.is_none_or(|h| r.block_height <= h))
            .next_back())
    }

    async fn edges_at(
        &self,
        chain_id: &str,
        block_height: u64,
    ) -> Result<Vec<ChannelEdgeRow>, IndexError> {
        let rows = self.edges.read().map_err(|e| IndexError(e.to_string()))?;
        let mut out: Vec<ChannelEdgeRow> = rows
            .iter()
            .filter(|(c, h, _)| c == chain_id && *h == block_height)
            .map(|(_, _, r)| r.clone())
            .collect();
        // The same ordering the Pg backend uses, so `limit`-free listings agree
        // between backends rather than differing by insertion order.
        out.sort_by_key(|r| (r.sender, r.recipient));
        Ok(out)
    }

    async fn edge_observations(
        &self,
        chain_id: &str,
        sender: u32,
        recipient: u32,
    ) -> Result<Vec<channels::EdgeObservation>, IndexError> {
        let readings = self.readings(chain_id).await?;
        let edges = self.edges.read().map_err(|e| IndexError(e.to_string()))?;
        Ok(readings
            .into_iter()
            .map(|r| channels::EdgeObservation {
                block_height: r.block_height,
                session_index: r.session_index,
                state: edges
                    .iter()
                    .find(|(c, h, e)| {
                        c == chain_id
                            && *h == r.block_height
                            && e.sender == sender
                            && e.recipient == recipient
                    })
                    .map(|(_, _, e)| e.state.clone()),
            })
            .collect())
    }
}

// ------------------------------------------------------------------- freshness

/// Reads the two tables per-module freshness is derived from.
///
/// It returns ROWS and never a verdict: the verdict is
/// [`freshness::derive`], which has no database in it, so the operator surface
/// and the per-response object compute the same answer from the same function.
/// A backend that "helpfully" filtered or pre-classified here would be the
/// second implementation of that rule, and two implementations of one rule
/// diverge — the addability rule had already drifted `<= 1` vs `== 1` before
/// anybody noticed.
#[async_trait]
pub trait FreshnessIndex: Send + Sync {
    /// EVERY `core.indexer_state` row for the chain, frontiers included. Taken
    /// whole rather than filtered, because a caller that filters decides which
    /// modules exist and would report its own filter as the chain's silence.
    async fn checkpoints(
        &self,
        chain_id: &str,
    ) -> Result<Vec<freshness::CheckpointRow>, IndexError>;

    /// Every `core.module_halts` row for the chain, resolved ones included.
    /// Whether a halt still blocks is derived against the checkpoint and is
    /// deliberately not stored, so this reader cannot pre-filter on it.
    async fn halts(&self, chain_id: &str) -> Result<Vec<freshness::HaltRow>, IndexError>;
}

/// In-memory freshness rows, for DB-less runs and tests.
#[derive(Default)]
pub struct MemoryFreshnessIndex {
    checkpoints: RwLock<Vec<(String, freshness::CheckpointRow)>>,
    halts: RwLock<Vec<(String, freshness::HaltRow)>>,
}

impl MemoryFreshnessIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert_checkpoint(&self, chain_id: &str, row: freshness::CheckpointRow) {
        self.checkpoints
            .write()
            .expect("freshness checkpoints lock")
            .push((chain_id.to_string(), row));
    }

    pub fn insert_halt(&self, chain_id: &str, row: freshness::HaltRow) {
        self.halts
            .write()
            .expect("freshness halts lock")
            .push((chain_id.to_string(), row));
    }
}

#[async_trait]
impl FreshnessIndex for MemoryFreshnessIndex {
    async fn checkpoints(
        &self,
        chain_id: &str,
    ) -> Result<Vec<freshness::CheckpointRow>, IndexError> {
        let rows = self.checkpoints.read().map_err(|e| IndexError(e.to_string()))?;
        Ok(rows
            .iter()
            .filter(|(c, _)| c == chain_id)
            .map(|(_, r)| r.clone())
            .collect())
    }

    async fn halts(&self, chain_id: &str) -> Result<Vec<freshness::HaltRow>, IndexError> {
        let rows = self.halts.read().map_err(|e| IndexError(e.to_string()))?;
        Ok(rows
            .iter()
            .filter(|(c, _)| c == chain_id)
            .map(|(_, r)| r.clone())
            .collect())
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

    /// Per-module freshness rows: `core.indexer_state` (0001) and
    /// `core.module_halts` (0028).
    ///
    /// NO INDEX IS NEEDED BY EITHER QUERY, and that is 0028's own argument
    /// rather than an omission. Both select by `chain_id`, which is the leading
    /// column of each table's PRIMARY KEY, in the primary key's own order —
    /// `(chain_id, module)` and `(chain_id, module, height, event_index)`. An
    /// index arrives with its reader; so does the refusal to add a redundant
    /// one.
    pub struct PgFreshnessIndex {
        pool: PgPool,
    }

    impl PgFreshnessIndex {
        pub fn new(pool: PgPool) -> Self {
            Self { pool }
        }
    }

    #[async_trait]
    impl super::FreshnessIndex for PgFreshnessIndex {
        async fn checkpoints(
            &self,
            chain_id: &str,
        ) -> Result<Vec<super::freshness::CheckpointRow>, IndexError> {
            // No `where module in (…)`: the frontier rows and the domain rows
            // come back together because the derivation needs both, and a
            // filter here would decide which modules exist.
            let rows: Vec<(String, i64, DateTime<Utc>)> = sqlx::query_as(
                "select module, last_height, updated_at \
                 from core.indexer_state where chain_id = $1 order by module",
            )
            .bind(chain_id)
            .fetch_all(&self.pool)
            .await
            .map_err(IndexError::from)?;
            Ok(rows
                .into_iter()
                .map(|(module, height, updated_at)| super::freshness::CheckpointRow {
                    module,
                    height: height as u64,
                    updated_at,
                })
                .collect())
        }

        async fn halts(
            &self,
            chain_id: &str,
        ) -> Result<Vec<super::freshness::HaltRow>, IndexError> {
            // Resolved halts are returned too. "Still blocking" is
            // `height > last_height` and is derived, so filtering here would be
            // the stored `active` flag that 0028 refused, one layer up.
            let rows: Vec<(
                String,
                i64,
                i32,
                String,
                String,
                i64,
                i32,
                DateTime<Utc>,
                DateTime<Utc>,
                i64,
            )> = sqlx::query_as(
                "select module, height, event_index, event, reason, \
                        runtime_version, mapper_version, \
                        first_seen_at, last_seen_at, seen_count \
                 from core.module_halts where chain_id = $1 \
                 order by module, height, event_index",
            )
            .bind(chain_id)
            .fetch_all(&self.pool)
            .await
            .map_err(IndexError::from)?;
            Ok(rows
                .into_iter()
                .map(|r| super::freshness::HaltRow {
                    module: r.0,
                    height: r.1 as u64,
                    event_index: r.2 as u32,
                    event: r.3,
                    reason: r.4,
                    runtime_version: r.5 as u64,
                    mapper_version: r.6 as u32,
                    first_seen_at: r.7,
                    last_seen_at: r.8,
                    seen_count: r.9 as u64,
                })
                .collect())
        }
    }

    /// Postgres-backed HRMP channel graph over `xcm.channel_readings` /
    /// `xcm.channel_snapshots` (Phase 3, slice 16).
    ///
    /// Every query here rides an existing key: the two per-reading reads use
    /// `channel_readings`' primary key `(chain_id, block_height)` and
    /// `channel_snapshots`' `(chain_id, block_height, …)` prefix, and the
    /// per-edge walk is the one query that earns 0027's
    /// `channel_snapshots_edge_idx (chain_id, sender, recipient, block_height)`.
    /// That index arrives WITH this reader, which is 0019's rule.
    pub struct PgChannelIndex {
        pool: PgPool,
    }

    impl PgChannelIndex {
        pub fn new(pool: PgPool) -> Self {
            Self { pool }
        }
    }

    #[async_trait]
    impl super::ChannelIndex for PgChannelIndex {
        async fn readings(
            &self,
            chain_id: &str,
        ) -> Result<Vec<super::ChannelReadingRow>, IndexError> {
            let rows: Vec<(i64, i64, i32, i32, String, i64, String)> = sqlx::query_as(
                "select block_height, session_index, channel_count, open_request_count, \
                        topology_digest, spec_version, source \
                 from xcm.channel_readings where chain_id = $1 order by block_height",
            )
            .bind(chain_id)
            .fetch_all(&self.pool)
            .await
            .map_err(IndexError::from)?;
            Ok(rows.into_iter().map(reading_row).collect())
        }

        async fn reading_at(
            &self,
            chain_id: &str,
            at: Option<u64>,
        ) -> Result<Option<super::ChannelReadingRow>, IndexError> {
            let row: Option<(i64, i64, i32, i32, String, i64, String)> = sqlx::query_as(
                "select block_height, session_index, channel_count, open_request_count, \
                        topology_digest, spec_version, source \
                 from xcm.channel_readings \
                 where chain_id = $1 and ($2::bigint is null or block_height <= $2) \
                 order by block_height desc limit 1",
            )
            .bind(chain_id)
            .bind(at.map(|h| h as i64))
            .fetch_optional(&self.pool)
            .await
            .map_err(IndexError::from)?;
            Ok(row.map(reading_row))
        }

        async fn edges_at(
            &self,
            chain_id: &str,
            block_height: u64,
        ) -> Result<Vec<super::ChannelEdgeRow>, IndexError> {
            let rows: Vec<(i64, i64, String, i64, i64, i64, String, Option<String>, Option<bool>)> =
                sqlx::query_as(
                    "select sender, recipient, state, max_capacity, max_total_size, \
                            max_message_size, sender_deposit::text, recipient_deposit::text, \
                            confirmed \
                     from xcm.channel_snapshots \
                     where chain_id = $1 and block_height = $2 \
                     order by sender, recipient",
                )
                .bind(chain_id)
                .bind(block_height as i64)
                .fetch_all(&self.pool)
                .await
                .map_err(IndexError::from)?;
            Ok(rows
                .into_iter()
                .map(
                    |(s, r, state, cap, total, msg, sdep, rdep, confirmed)| {
                        super::ChannelEdgeRow {
                            sender: s as u32,
                            recipient: r as u32,
                            state,
                            max_capacity: cap as u32,
                            max_total_size: total as u32,
                            max_message_size: msg as u32,
                            sender_deposit: sdep,
                            recipient_deposit: rdep,
                            confirmed,
                        }
                    },
                )
                .collect())
        }

        async fn edge_observations(
            &self,
            chain_id: &str,
            sender: u32,
            recipient: u32,
        ) -> Result<Vec<super::channels::EdgeObservation>, IndexError> {
            // A LEFT JOIN, and that is the whole design: the READING drives the
            // row set, so a reading in which this edge was absent still produces
            // a row, with a null state. An inner join would silently turn "we
            // looked and it was not there" into "we did not look".
            let rows: Vec<(i64, i64, Option<String>)> = sqlx::query_as(
                "select r.block_height, r.session_index, s.state \
                 from xcm.channel_readings r \
                 left join xcm.channel_snapshots s \
                        on s.chain_id = $1 \
                       and s.block_height = r.block_height \
                       and s.sender = $2 and s.recipient = $3 \
                 where r.chain_id = $1 \
                 order by r.block_height",
            )
            .bind(chain_id)
            .bind(sender as i64)
            .bind(recipient as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(IndexError::from)?;
            Ok(rows
                .into_iter()
                .map(|(h, s, state)| super::channels::EdgeObservation {
                    block_height: h as u64,
                    session_index: s as u64,
                    state,
                })
                .collect())
        }
    }

    fn reading_row(
        (height, session, channels, requests, digest, spec, source): (
            i64,
            i64,
            i32,
            i32,
            String,
            i64,
            String,
        ),
    ) -> super::ChannelReadingRow {
        super::ChannelReadingRow {
            block_height: height as u64,
            session_index: session as u64,
            channel_count: channels as u32,
            open_request_count: requests as u32,
            topology_digest: digest,
            spec_version: spec as u64,
            source,
        }
    }

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
            .map_err(IndexError::from)?;
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
            .map_err(IndexError::from)?;
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
            let err = |e: sqlx::Error| IndexError::from(e);
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
            let err = |e: sqlx::Error| IndexError::from(e);
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
                .map_err(IndexError::from)?;
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
                .map_err(IndexError::from)?;
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
            // height, free, reserved, total, frozen, spec, source, note, status
            let rows: Vec<(
                i64,
                String,
                String,
                String,
                Option<String>,
                Option<i64>,
                String,
                Option<String>,
                Option<String>,
            )> = sqlx::query_as(
                "select block_height, free::text, reserved::text, total::text, \
                            frozen::text, spec_version, source, note, status \
                     from balances.balance_anchors \
                     where chain_id = $1 and account_id = $2 and asset = $3 \
                     order by block_height",
            )
            .bind(chain_id)
            .bind(account_id)
            .bind(asset)
            .fetch_all(&self.pool)
            .await
            .map_err(IndexError::from)?;
            Ok(rows
                .into_iter()
                .map(|(height, free, reserved, total, frozen, spec, source, note, status)| {
                    super::BalanceAnchorRow {
                        height: height as u64,
                        free,
                        reserved,
                        total,
                        frozen,
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
            // account_id, asset, total, height, spec, source, note, status,
            // frozen, delta_sum, delta_count, last_height
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
                               a.block_height, a.spec_version, a.source, a.note, a.status, \
                               a.frozen::text as frozen \
                          from pairs p \
                          left join balances.balance_anchors a \
                            on a.chain_id = $1 and a.account_id = p.account_id \
                           and a.asset = p.asset \
                           and ($3::bigint is null or a.block_height <= $3) \
                         order by p.account_id, p.asset, a.block_height desc nulls last \
                      ) \
                 select anch.account_id, anch.asset, anch.total, anch.block_height, \
                        anch.spec_version, anch.source, anch.note, anch.status, \
                        anch.frozen, \
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
            .map_err(IndexError::from)?;
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
                        frozen,
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
                        anchor_frozen: frozen,
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
            let err = |e: sqlx::Error| IndexError::from(e);
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
            let err = |e: sqlx::Error| IndexError::from(e);
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
            .map_err(IndexError::from)?;
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
            .map_err(IndexError::from)?;
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
            .map_err(IndexError::from)?;
            rows.iter().map(Self::link).collect()
        }
    }

    /// Postgres-backed core-occupancy reads over `coretime.core_occupancy` and
    /// `coretime.core_config`.
    ///
    /// EVERY QUERY HERE IS A RANGE SCAN ON AN INDEX 0024 CREATED WITH ITS
    /// READER, and this is that reader: `core_occupancy_core_idx (chain_id,
    /// core_index, block_height)` serves the per-core grouping, and the
    /// `core_config` probes ride its primary key.
    pub struct PgCoretimeIndex {
        pool: PgPool,
    }

    impl PgCoretimeIndex {
        pub fn new(pool: PgPool) -> Self {
            Self { pool }
        }
    }

    #[async_trait]
    impl super::CoretimeIndex for PgCoretimeIndex {
        async fn occupancy_by_core(
            &self,
            chain_id: &str,
            from: u64,
            to: u64,
        ) -> Result<Vec<super::CoreOccupancyRow>, IndexError> {
            // `kind = 'included'` IS THE WHOLE RATIO. `backed` rows live in the
            // same table on purpose (backed-without-included is the
            // wasted-coretime signal) and counting them here would roughly
            // double every figure, because async backing puts a backing 2-6
            // blocks before nearly every inclusion.
            let rows: Vec<(i32, i64, Vec<i32>)> = sqlx::query_as(
                "select core_index, count(*), array_agg(distinct para_id order by para_id) \
                 from coretime.core_occupancy \
                 where chain_id = $1 and block_height between $2 and $3 and kind = 'included' \
                 group by core_index order by core_index",
            )
            .bind(chain_id)
            .bind(from as i64)
            .bind(to as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(IndexError::from)?;
            Ok(rows
                .into_iter()
                .map(|(core, n, paras)| super::CoreOccupancyRow {
                    core_index: core as u32,
                    included_blocks: n as u64,
                    paras: paras.into_iter().map(|p| p as u32).collect(),
                })
                .collect())
        }

        async fn occupancy_by_core_and_para(
            &self,
            chain_id: &str,
            from: u64,
            to: u64,
        ) -> Result<Vec<super::coretime_delta::OccupancyCell>, IndexError> {
            // Same `kind = 'included'` filter and the same index as
            // `occupancy_by_core` — this is that query with `para_id` moved from
            // the aggregate into the grouping, because the delta attributes per
            // task and a per-core total cannot say which half of a two-para
            // core's occupancy was entitled.
            let rows: Vec<(i32, i32, i64)> = sqlx::query_as(
                "select core_index, para_id, count(*) \
                 from coretime.core_occupancy \
                 where chain_id = $1 and block_height between $2 and $3 and kind = 'included' \
                 group by core_index, para_id order by core_index, para_id",
            )
            .bind(chain_id)
            .bind(from as i64)
            .bind(to as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(IndexError::from)?;
            Ok(rows
                .into_iter()
                .map(|(core, para, n)| super::coretime_delta::OccupancyCell {
                    core_index: core as u32,
                    para_id: para as u32,
                    included_blocks: n as u64,
                })
                .collect())
        }

        async fn kind_counts(
            &self,
            chain_id: &str,
            from: u64,
            to: u64,
        ) -> Result<Vec<(String, u64)>, IndexError> {
            let rows: Vec<(String, i64)> = sqlx::query_as(
                "select kind, count(*) from coretime.core_occupancy \
                 where chain_id = $1 and block_height between $2 and $3 \
                 group by kind order by kind collate \"C\"",
            )
            .bind(chain_id)
            .bind(from as i64)
            .bind(to as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(IndexError::from)?;
            Ok(rows.into_iter().map(|(k, n)| (k, n as u64)).collect())
        }

        async fn window_coverage(
            &self,
            chain_id: &str,
            from: u64,
            to: u64,
        ) -> Result<super::WindowCoverage, IndexError> {
            // TWO COUNTS, ONE ROUND TRIP. The first is the slot-fill
            // denominator; the second is how a reader sees that the mapper is
            // behind the block index without being told.
            //
            // `and finalized` MATCHES THE NUMERATOR'S OWN FILTER. The event
            // source this module maps from reads finalized rows only, so
            // counting unfinalized tip blocks in the denominator would add 2-3
            // core-block slots that can never be filled by construction — a
            // ratio that sags at the tip for a reason nobody could see.
            let (blocks, with_rows): (i64, i64) = sqlx::query_as(
                "select \
                   (select count(*) from core.blocks \
                     where chain_id = $1 and height between $2 and $3 and finalized), \
                   (select count(distinct block_height) from coretime.core_occupancy \
                     where chain_id = $1 and block_height between $2 and $3)",
            )
            .bind(chain_id)
            .bind(from as i64)
            .bind(to as i64)
            .fetch_one(&self.pool)
            .await
            .map_err(IndexError::from)?;
            Ok(super::WindowCoverage {
                blocks_indexed: blocks as u64,
                heights_with_occupancy: with_rows as u64,
            })
        }

        async fn core_config_at_or_before(
            &self,
            chain_id: &str,
            height: u64,
        ) -> Result<Option<super::CoreConfigRow>, IndexError> {
            let row: Option<(i64, i32, i64)> = sqlx::query_as(
                "select block_height, num_cores, runtime_version from coretime.core_config \
                 where chain_id = $1 and block_height <= $2 \
                 order by block_height desc limit 1",
            )
            .bind(chain_id)
            .bind(height as i64)
            .fetch_optional(&self.pool)
            .await
            .map_err(IndexError::from)?;
            Ok(row.map(|(h, n, rv)| super::CoreConfigRow {
                block_height: h as u64,
                num_cores: n as u32,
                runtime_version: rv as u64,
            }))
        }

        async fn core_config_after(
            &self,
            chain_id: &str,
            height: u64,
        ) -> Result<Option<super::CoreConfigRow>, IndexError> {
            let row: Option<(i64, i32, i64)> = sqlx::query_as(
                "select block_height, num_cores, runtime_version from coretime.core_config \
                 where chain_id = $1 and block_height > $2 \
                 order by block_height limit 1",
            )
            .bind(chain_id)
            .bind(height as i64)
            .fetch_optional(&self.pool)
            .await
            .map_err(IndexError::from)?;
            Ok(row.map(|(h, n, rv)| super::CoreConfigRow {
                block_height: h as u64,
                num_cores: n as u32,
                runtime_version: rv as u64,
            }))
        }

        async fn max_core_index(
            &self,
            chain_id: &str,
            from: u64,
            to: u64,
        ) -> Result<Option<u32>, IndexError> {
            // NO `kind` FILTER, unlike `occupancy_by_core`. See the trait.
            let row: (Option<i32>,) = sqlx::query_as(
                "select max(core_index) from coretime.core_occupancy \
                 where chain_id = $1 and block_height between $2 and $3",
            )
            .bind(chain_id)
            .bind(from as i64)
            .bind(to as i64)
            .fetch_one(&self.pool)
            .await
            .map_err(IndexError::from)?;
            Ok(row.0.map(|n| n as u32))
        }

        async fn occupancy_lineage(
            &self,
            chain_id: &str,
            from: u64,
            to: u64,
        ) -> Result<Vec<(u64, u32, u64)>, IndexError> {
            let rows: Vec<(i64, i32, i64)> = sqlx::query_as(
                "select runtime_version, mapper_version, count(*) \
                 from coretime.core_occupancy \
                 where chain_id = $1 and block_height between $2 and $3 \
                 group by runtime_version, mapper_version \
                 order by runtime_version, mapper_version",
            )
            .bind(chain_id)
            .bind(from as i64)
            .bind(to as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(IndexError::from)?;
            Ok(rows
                .into_iter()
                .map(|(rv, mv, n)| (rv as u64, mv as u32, n as u64))
                .collect())
        }

        async fn num_cores_in_window(
            &self,
            chain_id: &str,
            from: u64,
            to: u64,
        ) -> Result<Vec<u32>, IndexError> {
            let rows: Vec<(i32,)> = sqlx::query_as(
                "select distinct num_cores from coretime.core_config \
                 where chain_id = $1 and block_height between $2 and $3 order by num_cores",
            )
            .bind(chain_id)
            .bind(from as i64)
            .bind(to as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(IndexError::from)?;
            Ok(rows.into_iter().map(|(n,)| n as u32).collect())
        }
    }

    /// Postgres-backed entitlement reads over `coretime.core_assignments`,
    /// `coretime.broker_events` and `coretime.broker_config`.
    ///
    /// EVERY QUERY HERE IS THE READER FOR AN INDEX MIGRATION 0026 CREATES, and
    /// 0026 exists because 0025 dropped those four indexes for lack of exactly
    /// this. `core_assignments_core_idx` serves the governing-assignment probe,
    /// `core_assignments_task_idx` the tenant timeline, and the two
    /// `broker_events_*_idx` the event timelines.
    pub struct PgBrokerIndex {
        pool: PgPool,
    }

    impl PgBrokerIndex {
        pub fn new(pool: PgPool) -> Self {
            Self { pool }
        }
    }

    /// The assignment columns, in one place so the name-read select and the
    /// tuple it decodes into cannot drift apart.
    const ASSIGNMENT_COLS: &str = "core_index, assignment_index, relay_block, assignment_kind, \
                                   task_id, parts, runtime_version, mapper_version";

    type AssignmentTuple = (i32, i32, i64, String, Option<i32>, i32, i64, i32);

    fn assignment_row(t: AssignmentTuple) -> super::coretime_delta::EntitlementRow {
        super::coretime_delta::EntitlementRow {
            core_index: t.0 as u32,
            assignment_index: t.1 as u32,
            relay_block: t.2 as u64,
            kind: t.3,
            task_id: t.4.map(|v| v as u32),
            parts: t.5 as u32,
            runtime_version: t.6 as u64,
            mapper_version: t.7 as u32,
        }
    }

    const BROKER_EVENT_COLS: &str = "block_height, event_index, variant, core_index, task_id, \
                                     data, runtime_version, mapper_version";

    type BrokerEventTuple = (
        i64,
        i32,
        String,
        Option<i32>,
        Option<i32>,
        serde_json::Value,
        i64,
        i32,
    );

    fn broker_event_row(t: BrokerEventTuple) -> super::BrokerEventRow {
        super::BrokerEventRow {
            block_height: t.0 as u64,
            event_index: t.1 as u32,
            variant: t.2,
            core_index: t.3.map(|v| v as u32),
            task_id: t.4.map(|v| v as u32),
            data: t.5,
            runtime_version: t.6 as u64,
            mapper_version: t.7 as u32,
        }
    }

    #[async_trait]
    impl super::BrokerIndex for PgBrokerIndex {
        async fn entitlement_at(
            &self,
            chain_id: &str,
            relay_height: u64,
        ) -> Result<Vec<super::coretime_delta::EntitlementRow>, IndexError> {
            // `distinct on (core_index)` picks ONE ANNOUNCEMENT per core — the
            // newest `relay_block`, tie-broken by the newest announcing
            // coordinate. The join then returns that announcement's WHOLE
            // assignment vector, so an interlaced core keeps both entitlements.
            //
            // The tie-break is not decoration: two rows can share a
            // `relay_block` and come from different events (a re-announcement),
            // and taking both would make one core look interlaced when it is
            // not — inventing a fractional entitlement, which withholds the
            // waste figure for a reason that never happened.
            // THE CTE'S COLUMNS ARE RENAMED, and that is not style. `a` and `g`
            // both expose `core_index` and `relay_block`, and `ASSIGNMENT_COLS`
            // is unqualified (it is shared with the two single-table queries
            // below), so an unaliased CTE makes every one of those references
            // ambiguous — Postgres 42702, at RUNTIME, on a query that compiles
            // perfectly because `query_as` is the untyped form.
            let rows: Vec<AssignmentTuple> = sqlx::query_as(&format!(
                "with governing as ( \
                   select distinct on (core_index) core_index as g_core, \
                          relay_block as g_relay, block_height as g_height, \
                          event_index as g_event \
                     from coretime.core_assignments \
                    where chain_id = $1 and relay_block <= $2 \
                    order by core_index, relay_block desc, block_height desc, event_index desc \
                 ) \
                 select {ASSIGNMENT_COLS} from coretime.core_assignments a \
                   join governing g on g.g_core = a.core_index \
                    and g.g_relay = a.relay_block and g.g_height = a.block_height \
                    and g.g_event = a.event_index \
                  where a.chain_id = $1 \
                  order by a.core_index, a.assignment_index"
            ))
            .bind(chain_id)
            .bind(relay_height as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(IndexError::from)?;
            Ok(rows.into_iter().map(assignment_row).collect())
        }

        async fn latest_broker_config(
            &self,
            chain_id: &str,
        ) -> Result<Option<super::BrokerConfigRow>, IndexError> {
            let row: Option<(i64, i32, Option<i32>, i64)> = sqlx::query_as(
                "select block_height, core_count, first_core, runtime_version \
                 from coretime.broker_config \
                 where chain_id = $1 order by block_height desc limit 1",
            )
            .bind(chain_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(IndexError::from)?;
            Ok(row.map(|(h, n, fc, rv)| super::BrokerConfigRow {
                block_height: h as u64,
                core_count: n as u32,
                first_core: fc.map(|v| v as u32),
                runtime_version: rv as u64,
            }))
        }

        async fn events_for_core(
            &self,
            chain_id: &str,
            core_index: u32,
            limit: u32,
        ) -> Result<Vec<super::BrokerEventRow>, IndexError> {
            // `core_index = $2` keeps the indexed column BARE on the left, which
            // is what lets Postgres prove the partial predicate `core_index is
            // not null` (0020's caveat, measured in slice 7's verification:
            // `= any($1)` still proves it, `lower(col) = $1` does not).
            self.events(
                &format!(
                    "select {BROKER_EVENT_COLS} from coretime.broker_events \
                     where chain_id = $1 and core_index = $2 \
                     order by block_height desc, event_index desc limit $3"
                ),
                chain_id,
                core_index,
                limit,
            )
            .await
        }

        async fn events_for_task(
            &self,
            chain_id: &str,
            task_id: u32,
            limit: u32,
        ) -> Result<Vec<super::BrokerEventRow>, IndexError> {
            self.events(
                &format!(
                    "select {BROKER_EVENT_COLS} from coretime.broker_events \
                     where chain_id = $1 and task_id = $2 \
                     order by block_height desc, event_index desc limit $3"
                ),
                chain_id,
                task_id,
                limit,
            )
            .await
        }

        async fn assignments_for_core(
            &self,
            chain_id: &str,
            core_index: u32,
            limit: u32,
        ) -> Result<Vec<super::coretime_delta::EntitlementRow>, IndexError> {
            self.assignments(
                &format!(
                    "select {ASSIGNMENT_COLS} from coretime.core_assignments \
                     where chain_id = $1 and core_index = $2 \
                     order by relay_block desc, block_height desc, event_index desc, \
                              assignment_index limit $3"
                ),
                chain_id,
                core_index,
                limit,
            )
            .await
        }

        async fn assignments_for_task(
            &self,
            chain_id: &str,
            task_id: u32,
            limit: u32,
        ) -> Result<Vec<super::coretime_delta::EntitlementRow>, IndexError> {
            self.assignments(
                &format!(
                    "select {ASSIGNMENT_COLS} from coretime.core_assignments \
                     where chain_id = $1 and task_id = $2 \
                     order by relay_block desc, block_height desc, event_index desc, \
                              assignment_index limit $3"
                ),
                chain_id,
                task_id,
                limit,
            )
            .await
        }
    }

    impl PgBrokerIndex {
        async fn events(
            &self,
            sql: &str,
            chain_id: &str,
            subject: u32,
            limit: u32,
        ) -> Result<Vec<super::BrokerEventRow>, IndexError> {
            let rows: Vec<BrokerEventTuple> = sqlx::query_as(sql)
                .bind(chain_id)
                .bind(subject as i32)
                .bind(limit as i64)
                .fetch_all(&self.pool)
                .await
                .map_err(IndexError::from)?;
            Ok(rows.into_iter().map(broker_event_row).collect())
        }

        async fn assignments(
            &self,
            sql: &str,
            chain_id: &str,
            subject: u32,
            limit: u32,
        ) -> Result<Vec<super::coretime_delta::EntitlementRow>, IndexError> {
            let rows: Vec<AssignmentTuple> = sqlx::query_as(sql)
                .bind(chain_id)
                .bind(subject as i32)
                .bind(limit as i64)
                .fetch_all(&self.pool)
                .await
                .map_err(IndexError::from)?;
            Ok(rows.into_iter().map(assignment_row).collect())
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

    /// Every column `SimulationRow` needs, named once so the two readers below
    /// cannot drift apart — a `select` that lists them in two places is a
    /// transposition waiting for a column of the same type to be added.
    const SIM_COLUMNS: &str = "chain_id, at_height, at_block_hash, input_hash, tier, call_hash, \
         call_summary, origin_spec, origin_json, xcm_version, status, dispatch_ok, \
         dispatch_error, emitted_events, event_count, local_xcm, forwarded_xcms, effects, note, \
         spec_version, api_version, metadata_version, sim_version, raw_location, observed_at, \
         baseline_input_hash, overrides, override_hash, storage_diff, storage_diff_count, \
         diff_status, built_block_hash, harness, dispatch_route, agenda_anchor";

    fn sim_row(r: &sqlx::postgres::PgRow) -> Result<super::SimulationRow, IndexError> {
        use sqlx::Row as _;
        let err = |e: sqlx::Error| IndexError::from(e);
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
            // Nullable since 0021 — read as Option, never defaulted: a NULL read
            // as 0 would put "XCM v0" and "DryRunApi v0" on a fork row that
            // called neither.
            xcm_version: r
                .try_get::<Option<i32>, _>("xcm_version")
                .map_err(err)?
                .map(|n| n as u32),
            status: r.try_get("status").map_err(err)?,
            dispatch_ok: r.try_get("dispatch_ok").map_err(err)?,
            dispatch_error: r.try_get("dispatch_error").map_err(err)?,
            emitted_events: r.try_get("emitted_events").map_err(err)?,
            event_count: r.try_get::<i32, _>("event_count").map_err(err)? as u32,
            local_xcm: r.try_get("local_xcm").map_err(err)?,
            // Nullable since 0021 — a fork row HAS no forwarded list, and reading
            // it as a bare Value makes sqlx error on the NULL. Option, never Some().
            forwarded_xcms: r.try_get("forwarded_xcms").map_err(err)?,
            effects: r.try_get("effects").map_err(err)?,
            note: r.try_get("note").map_err(err)?,
            spec_version: r.try_get::<i64, _>("spec_version").map_err(err)? as u64,
            api_version: r
                .try_get::<Option<i32>, _>("api_version")
                .map_err(err)?
                .map(|n| n as u32),
            metadata_version: r.try_get::<i32, _>("metadata_version").map_err(err)? as u32,
            sim_version: r.try_get::<i32, _>("sim_version").map_err(err)? as u32,
            raw_location: r.try_get("raw_location").map_err(err)?,
            observed_at: r.try_get("observed_at").map_err(err)?,
            baseline_input_hash: r.try_get("baseline_input_hash").map_err(err)?,
            overrides: r.try_get("overrides").map_err(err)?,
            override_hash: r.try_get("override_hash").map_err(err)?,
            storage_diff: r.try_get("storage_diff").map_err(err)?,
            storage_diff_count: r
                .try_get::<Option<i32>, _>("storage_diff_count")
                .map_err(err)?
                .map(|n| n as u32),
            diff_status: r.try_get("diff_status").map_err(err)?,
            built_block_hash: r.try_get("built_block_hash").map_err(err)?,
            harness: r.try_get("harness").map_err(err)?,
            dispatch_route: r.try_get("dispatch_route").map_err(err)?,
            agenda_anchor: r.try_get("agenda_anchor").map_err(err)?,
        })
    }

    #[async_trait]
    impl super::SimIndex for PgSimIndex {
        async fn simulation_at(
            &self,
            chain_id: &str,
            at_block_hash: &str,
            input_hash: &str,
            tier: &str,
        ) -> Result<Option<super::SimulationRow>, IndexError> {
            // The full primary key, so this is a single-row lookup on it.
            let row = sqlx::query(&format!(
                "select {SIM_COLUMNS} from sim.simulation_results \
                 where chain_id = $1 and at_block_hash = $2 and input_hash = $3 and tier = $4"
            ))
            .bind(chain_id)
            .bind(at_block_hash)
            .bind(input_hash)
            .bind(tier)
            .fetch_optional(&self.pool)
            .await
            .map_err(IndexError::from)?;
            row.as_ref().map(sim_row).transpose()
        }

        async fn simulations(
            &self,
            chain_id: &str,
            call_hash: &str,
            limit: u32,
        ) -> Result<Vec<super::SimulationRow>, IndexError> {
            // (chain_id, call_hash, at_height desc) is simulation_results_call_idx
            // verbatim — the index and its one reader ship together (0014).
            //
            // Read by COLUMN NAME rather than into a tuple: this row has 26
            // columns and sqlx only implements FromRow for tuples up to 16, so a
            // tuple here does not compile. Naming the columns is also the safer
            // shape for a row this wide — a reordered select cannot silently
            // transpose two same-typed fields.
            let rows = sqlx::query(&format!(
                "select {SIM_COLUMNS} from sim.simulation_results \
                 where chain_id = $1 and call_hash = $2 \
                 order by at_height desc, input_hash collate \"C\", \
                          at_block_hash collate \"C\", tier collate \"C\" \
                 limit $3"
            ))
            .bind(chain_id)
            .bind(call_hash)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(IndexError::from)?;

            rows.iter().map(sim_row).collect()
        }
    }

    /// Postgres-backed reads over `sim.xcm_simulations`.
    pub struct PgXcmSimIndex {
        pool: PgPool,
    }

    impl PgXcmSimIndex {
        pub fn new(pool: PgPool) -> Self {
            Self { pool }
        }
    }

    const XCM_SIM_COLUMNS: &str = "chain_id, at_height, at_block_hash, input_hash, tier, \
         program_hash, program, program_summary, origin_location, origin_ref, status, \
         weight_used, xcm_error, emitted_events, event_count, forwarded_xcms, \
         baseline_input_hash, effects, note, source_chain_id, source_at_block_hash, \
         source_input_hash, source_forwarded_index, source_message_index, spec_version, \
         api_version, metadata_version, sim_version, raw_location, observed_at";

    fn xcm_sim_row(r: &sqlx::postgres::PgRow) -> Result<super::XcmSimulationRow, IndexError> {
        use sqlx::Row as _;
        let err = |e: sqlx::Error| IndexError::from(e);
        Ok(super::XcmSimulationRow {
            chain_id: r.try_get("chain_id").map_err(err)?,
            at_height: r.try_get::<i64, _>("at_height").map_err(err)? as u64,
            at_block_hash: r.try_get("at_block_hash").map_err(err)?,
            input_hash: r.try_get("input_hash").map_err(err)?,
            tier: r.try_get("tier").map_err(err)?,
            program_hash: r.try_get("program_hash").map_err(err)?,
            program: r.try_get("program").map_err(err)?,
            program_summary: r.try_get("program_summary").map_err(err)?,
            origin_location: r.try_get("origin_location").map_err(err)?,
            origin_ref: r.try_get("origin_ref").map_err(err)?,
            status: r.try_get("status").map_err(err)?,
            weight_used: r.try_get("weight_used").map_err(err)?,
            xcm_error: r.try_get("xcm_error").map_err(err)?,
            emitted_events: r.try_get("emitted_events").map_err(err)?,
            event_count: r.try_get::<i32, _>("event_count").map_err(err)? as u32,
            forwarded_xcms: r.try_get("forwarded_xcms").map_err(err)?,
            baseline_input_hash: r.try_get("baseline_input_hash").map_err(err)?,
            effects: r.try_get("effects").map_err(err)?,
            note: r.try_get("note").map_err(err)?,
            source_chain_id: r.try_get("source_chain_id").map_err(err)?,
            source_at_block_hash: r.try_get("source_at_block_hash").map_err(err)?,
            source_input_hash: r.try_get("source_input_hash").map_err(err)?,
            source_forwarded_index: r
                .try_get::<Option<i32>, _>("source_forwarded_index")
                .map_err(err)?
                .map(|n| n as u32),
            source_message_index: r
                .try_get::<Option<i32>, _>("source_message_index")
                .map_err(err)?
                .map(|n| n as u32),
            spec_version: r.try_get::<i64, _>("spec_version").map_err(err)? as u64,
            api_version: r.try_get::<i32, _>("api_version").map_err(err)? as u32,
            metadata_version: r.try_get::<i32, _>("metadata_version").map_err(err)? as u32,
            sim_version: r.try_get::<i32, _>("sim_version").map_err(err)? as u32,
            raw_location: r.try_get("raw_location").map_err(err)?,
            observed_at: r.try_get("observed_at").map_err(err)?,
        })
    }

    #[async_trait]
    impl super::XcmSimIndex for PgXcmSimIndex {
        async fn xcm_simulation_at(
            &self,
            chain_id: &str,
            at_block_hash: &str,
            input_hash: &str,
            tier: &str,
        ) -> Result<Option<super::XcmSimulationRow>, IndexError> {
            let row = sqlx::query(&format!(
                "select {XCM_SIM_COLUMNS} from sim.xcm_simulations \
                 where chain_id = $1 and at_block_hash = $2 and input_hash = $3 and tier = $4"
            ))
            .bind(chain_id)
            .bind(at_block_hash)
            .bind(input_hash)
            .bind(tier)
            .fetch_optional(&self.pool)
            .await
            .map_err(IndexError::from)?;
            row.as_ref().map(xcm_sim_row).transpose()
        }

        async fn xcm_simulations(
            &self,
            chain_id: &str,
            program_hash: &str,
            limit: u32,
        ) -> Result<Vec<super::XcmSimulationRow>, IndexError> {
            // (chain_id, program_hash, at_height desc) is
            // xcm_simulations_program_idx's leading columns — it covers the
            // WHERE and the leading sort key, and the tail of the ORDER BY (the
            // rest of the primary key, which the two backends must agree on) is
            // an incremental sort over a tiny tied group. 0018 says the same.
            let rows = sqlx::query(&format!(
                "select {XCM_SIM_COLUMNS} from sim.xcm_simulations \
                 where chain_id = $1 and program_hash = $2 \
                 order by at_height desc, input_hash collate \"C\", \
                          at_block_hash collate \"C\", tier collate \"C\" \
                 limit $3"
            ))
            .bind(chain_id)
            .bind(program_hash)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(IndexError::from)?;
            rows.iter().map(xcm_sim_row).collect()
        }

        async fn legs(
            &self,
            source_chain_id: &str,
            source_input_hash: &str,
            limit: u32,
        ) -> Result<Vec<super::XcmSimulationRow>, IndexError> {
            // Ordered by the SENDER's own list positions: legs are a sequence
            // the sender produced, and "newest first" would shuffle them.
            //
            // `nulls first` matches Rust's `None < Some` in the Memory backend.
            // Unreachable today — the five source columns are written
            // all-or-nothing and this query filters on one of them being
            // non-null — but the two backends' orderings are a contract, and a
            // contract that holds by accident is one nobody will notice
            // breaking.
            let rows = sqlx::query(&format!(
                "select {XCM_SIM_COLUMNS} from sim.xcm_simulations \
                 where source_chain_id = $1 and source_input_hash = $2 \
                 order by source_forwarded_index nulls first, \
                          source_message_index nulls first, \
                          chain_id collate \"C\", at_block_hash collate \"C\" \
                 limit $3"
            ))
            .bind(source_chain_id)
            .bind(source_input_hash)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(IndexError::from)?;
            rows.iter().map(xcm_sim_row).collect()
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
                Option<String>,
                Option<serde_json::Value>,
                Option<String>,
            );
            let rows: Vec<Row> = sqlx::query_as(
                "select chain_id, asset_key, representation_kind, symbol, name, decimals, \
                        supply::text, status, location_key, xcm_location, \
                        absolute_key, absolute_location, asset_type \
                 from core.assets where lower(symbol) = lower($1) \
                 order by chain_id collate \"C\", asset_key collate \"C\"",
            )
            .bind(symbol)
            .fetch_all(&self.pool)
            .await
            .map_err(IndexError::from)?;
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
                        absolute_key,
                        absolute_location,
                        asset_type,
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
                                absolute_key,
                                absolute_location,
                                asset_type,
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
                Option<String>,
                Option<serde_json::Value>,
                Option<String>,
            );
            let rows: Vec<Row> = sqlx::query_as(
                "select asset_key, representation_kind, symbol, name, decimals, \
                        supply::text, status, location_key, xcm_location, \
                        absolute_key, absolute_location, asset_type \
                 from core.assets where chain_id = $1 order by asset_key collate \"C\"",
            )
            .bind(chain_id)
            .fetch_all(&self.pool)
            .await
            .map_err(IndexError::from)?;
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
                        absolute_key,
                        absolute_location,
                        asset_type,
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
                        absolute_key,
                        absolute_location,
                        asset_type,
                    },
                )
                .collect())
        }

        /// The reader migration 0020's index exists for. `absolute_key = $1` is
        /// spelled plainly — no `lower()`, no cast — so the plain btree index is
        /// usable; a functional index is only used when the query spells the
        /// expression the same way, which is why `assets_by_symbol` above says
        /// `lower(symbol)` and this does not.
        async fn representations(
            &self,
            absolute_key: &str,
        ) -> Result<Vec<(String, super::AssetRow)>, IndexError> {
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
                Option<String>,
                Option<serde_json::Value>,
                Option<String>,
            );
            let rows: Vec<Row> = sqlx::query_as(
                "select chain_id, asset_key, representation_kind, symbol, name, decimals, \
                        supply::text, status, location_key, xcm_location, \
                        absolute_key, absolute_location, asset_type \
                 from core.assets where absolute_key = $1 \
                 order by chain_id collate \"C\", asset_key collate \"C\"",
            )
            .bind(absolute_key)
            .fetch_all(&self.pool)
            .await
            .map_err(IndexError::from)?;
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
                        absolute_key,
                        absolute_location,
                        asset_type,
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
                                absolute_key,
                                absolute_location,
                                asset_type,
                            },
                        )
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
            .map_err(IndexError::from)?;
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
            .map_err(IndexError::from)?;
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
                .map_err(IndexError::from)?;
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
            .map_err(IndexError::from)?;
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
            .map_err(IndexError::from)?;
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
            .map_err(IndexError::from)?;
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
                .map_err(IndexError::from)?;
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
            .map_err(IndexError::from)?;
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
            .map_err(IndexError::from)?;
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
            .map_err(IndexError::from)?;
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
            .map_err(IndexError::from)?;
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
            .map_err(IndexError::from)?;
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
            .map_err(IndexError::from)?;
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
            .map_err(IndexError::from)?;
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
            .map_err(IndexError::from)?;
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
            .map_err(IndexError::from)?;
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
            .map_err(IndexError::from)?;
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
            .map_err(IndexError::from)?;
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
            .map_err(IndexError::from)?;
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
            .map_err(IndexError::from)?;
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
            .map_err(IndexError::from)?;
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
    pub xcm_sim: Arc<dyn XcmSimIndex>,
    pub xcm: Arc<dyn XcmIndex>,
    pub coretime: Arc<dyn CoretimeIndex>,
    pub broker: Arc<dyn BrokerIndex>,
    pub channels: Arc<dyn ChannelIndex>,
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
        .route(
            "/v1/treasury/{network}/consolidated",
            get(get_treasury_consolidated),
        )
        .route("/v1/assets/identity", get(get_asset_identity))
        .route("/v1/bounties/{network}", get(list_bounties))
        .route("/v1/bounties/{network}/{id}", get(get_bounty))
        .route("/v1/assets/{chain}", get(list_assets))
        .route("/v1/sim/{chain}/calls/{call_hash}", get(get_simulations))
        .route("/v1/sim/{chain}/xcm/{program_hash}", get(get_xcm_simulations))
        .route("/v1/xcm/{chain}/messages", get(list_xcm_messages))
        .route("/v1/xcm/messages/{message_id}", get(get_xcm_message))
        .route("/v1/xcm/journeys/{message_id}", get(get_xcm_journey))
        .route(
            "/v1/coretime/{chain}/occupancy",
            get(get_coretime_occupancy),
        )
        // NETWORK-SCOPED, unlike its two neighbours, and the difference is the
        // point: the delta spans TWO CHAINS and a request that named one would
        // have to name the other in code. Both are resolved from the registry.
        .route("/v1/coretime/{network}/delta", get(get_coretime_delta))
        .route(
            "/v1/coretime/{chain}/entitlement",
            get(get_coretime_entitlement),
        )
        .route("/v1/xcm/{network}/channels", get(get_xcm_channels))
        .route(
            "/v1/xcm/{network}/channels/history",
            get(get_xcm_channel_history),
        )
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
        Err(e) => read_failure(e),
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
        Err(e) => read_failure(e),
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
            Err(e) => return read_failure(e),
        };
        let anchors = match state.balances.anchors(&w.chain, &account_id, &asset).await {
            Ok(a) => a,
            Err(e) => return read_failure(e),
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
            Err(e) => return read_failure(e),
        };
        let events = match state.gov.whitelist_events(&w.chain, &call_hash).await {
            Ok(ev) => ev,
            Err(e) => return read_failure(e),
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
                Err(e) => return read_failure(e),
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
            Err(e) => return read_failure(e),
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
            Err(e) => return read_failure(e),
        };
        let events = match state.gov.referendum_events(&w.chain, &class, id).await {
            Ok(ev) => ev,
            Err(e) => return read_failure(e),
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
                Err(e) => return read_failure(e),
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
                Err(e) => return read_failure(e),
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

    // EVERY ROW GETS THE SAME ENRICHMENT IT GETS ON /v1/sim/…, through the same
    // function. Before slice 10 this page serialized the rows RAW: a fork row
    // appeared here with no `tier_coverage`, no `diff_covers` and — the sharp
    // one — a counterfactual's `overrides` with no `is_counterfactual` marker,
    // i.e. fabricated state rendered unlabelled on a governance page.
    let simulations: Vec<serde_json::Value> = simulations
        .iter()
        .map(|row| {
            let mut value = serde_json::to_value(row).unwrap_or_else(|_| serde_json::json!({}));
            if let Some(obj) = value.as_object_mut() {
                enrich_simulation(row, obj);
            }
            value
        })
        .collect();
    // A fork row on this page must not be described by the DRY-RUN list, whose
    // second line ("the call is dispatched directly") is false of this tier on
    // either route. Each row already carries its own in `tier_coverage`; this
    // says so rather than letting the response-level list read as complete.
    let has_fork = simulations
        .iter()
        .any(|s| s["tier"] == sim::TIER_FORK);
    let tiers_carry_their_own = has_fork.then_some(
        "at least one row below is a `fork` row, whose limits are different IN KIND from the \
         dry-run tier's — not a subset. `not_covered` here describes the dry-run tier only; \
         read each row's own `tier_coverage.not_covered`, and its `diff_covers` for whether \
         its storage diff describes the call at all",
    );

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
            // NOT "no Tier 1 preview": this list is untiered, so an empty one
            // means nobody previewed the call on ANY tier.
            "recorded_only": "no preview of any tier has been run for this proposal; that is \
                              an absence of simulations, not a claim about the call",
            "not_covered": sim_not_covered(),
        }),
        (Some(_), false) => serde_json::json!({
            "truncated": sim_truncated,
            // THE ATTRIBUTION IS NOT COMPUTED HERE, and saying so is the point.
            // Working out which forwarded messages a call is responsible for
            // costs a second read PER SIMULATION, and this response already
            // carries up to SIM_PER_WINDOW of them per residency window — so the
            // raw `forwarded_xcms` on each row below is exactly what the
            // not_covered list warns it is, and the endpoint that differences it
            // is named rather than implied.
            "attribution_elsewhere": "each DRY-RUN simulation here carries its raw forwarded_xcms \
                                      (a fork row has none at all), \
                                      which is NOT attributable to the call on its own. \
                                      /v1/sim/{chain}/calls/{call_hash} differences it \
                                      against the recorded no-op baseline and reports the \
                                      previewed arrivals",
            "not_covered": sim_not_covered(),
            // Emitted only when a fork row is actually present, because a line
            // that names a tier belongs on a response that has one.
            "tiers_carry_their_own": tiers_carry_their_own,
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
    /// dry_run | fork. 0014 deferred this knob with the note that it "ships with
    /// Tier 2, which is also what makes it selective" — this is that slice, and
    /// leaving it out would have made that comment the stale kind this project
    /// keeps having to correct. Absent = both tiers, which is the useful default:
    /// a call previewed two ways is two answers worth seeing together.
    tier: Option<String>,
}

/// `?from=&to=` — the window an occupancy ratio is computed over. Both are
/// required; see the handler for why there is no default.
#[derive(Deserialize)]
struct CoretimeQuery {
    from: Option<u64>,
    to: Option<u64>,
}

/// The widest window this endpoint will aggregate in one request.
///
/// The RESPONSE is bounded by `num_cores` however wide the window, so this is
/// not about payload size — it is about a `group by` over a partition scan that
/// nobody meant to ask for. The prep's own sample was 1,000 blocks.
const CORETIME_MAX_SPAN: u64 = 250_000;

/// Core occupancy over a relay window — TWO ratios, never one.
///
/// ---------------------------------------------------------------------------
/// THE TWO RATIOS ARE THE PRODUCT CLAIM, AND THEY ARE NOT THE SAME NUMBER.
///
/// Measured over the contiguous 1,000 relay blocks 32613537-32614536:
/// **47 of 100 declared cores produced anything (47.0%)** while only **34.69% of
/// core-block slots were filled** (34,690 of 1,000 x 100). The gap is the
/// finding — even the cores that are used sit idle — and a single `utilization`
/// field would report one of those and call it the other, throwing away the only
/// figure here that no marketplace view has.
///
/// So this handler serves both, each beside the two integers it was computed
/// from, and the per-core distribution underneath: 10 cores saturated, 19 at
/// 75-95%, 4 at 50-75%, 12 at 25-50%, 2 under 5%, and 53 producing nothing at
/// all. That shape is the answer to "is coretime working", and an average of it
/// is not.
///
/// ---------------------------------------------------------------------------
/// AND THE DENOMINATOR CARRIES ITS OWN PROVENANCE, because it moves.
///
/// `num_cores` is host configuration read at a BLOCK; the prep read it at
/// exactly one (#32614536 -> 100) and named a stale denominator as a live risk.
/// Every ratio here names which reading it divided by, where that reading sits
/// relative to the window, and whether the window contains readings that
/// DISAGREE. With no reading at all the counts are still served and the ratios
/// are `null` — refusing a ratio is this project's answer to not having a
/// denominator, and inventing one is not.
async fn get_coretime_occupancy(
    State(state): State<AppState>,
    Path(chain): Path<String>,
    Query(q): Query<CoretimeQuery>,
) -> Response {
    let Some(cfg) = state.registry.chain(&chain) else {
        return error(StatusCode::NOT_FOUND, format!("unknown chain '{chain}'"));
    };
    // A REGISTERED PARACHAIN IS A 404 WITH A REASON, not an empty window.
    // `paraInclusion` is a relay pallet, so a parachain has no candidate events
    // at all — and an empty response here would read as "this chain's cores did
    // nothing", which is a claim about the network rather than about us.
    if !cfg.has_module("coretime") {
        return error(
            StatusCode::NOT_FOUND,
            format!(
                "'{chain}' does not declare the `coretime` module. Core occupancy is read from \
                 `paraInclusion`, a RELAY pallet that names both the para id and the core index, \
                 so it is answered by the relay and not by the parachain that ran on the core"
            ),
        );
    }
    let (Some(from), Some(to)) = (q.from, q.to) else {
        return error(
            StatusCode::BAD_REQUEST,
            "both `from` and `to` are required: an occupancy ratio is only meaningful over a \
             stated window, and defaulting to one would put a window nobody chose underneath a \
             percentage somebody quotes"
                .to_string(),
        );
    };
    if from > to {
        return error(StatusCode::BAD_REQUEST, "`from` must not exceed `to`".to_string());
    }
    // `to - from` is already non-negative, but `?to=18446744073709551615` makes
    // `+ 1` overflow: a debug build panics inside a public GET and a release
    // build wraps to zero, passes the limit check and binds `to as i64 == -1`.
    let span = (to - from).saturating_add(1);
    if span > CORETIME_MAX_SPAN {
        return error(
            StatusCode::BAD_REQUEST,
            format!("window of {span} blocks exceeds the {CORETIME_MAX_SPAN}-block limit"),
        );
    }

    let ise = |e: IndexError| read_failure(e);
    let cores = match state.coretime.occupancy_by_core(&chain, from, to).await {
        Ok(c) => c,
        Err(e) => return ise(e),
    };
    let kinds = match state.coretime.kind_counts(&chain, from, to).await {
        Ok(k) => k,
        Err(e) => return ise(e),
    };
    let coverage = match state.coretime.window_coverage(&chain, from, to).await {
        Ok(c) => c,
        Err(e) => return ise(e),
    };
    // The reading that DATES the ratio: the newest at or before the window's
    // end, because that is the configuration the window ran under.
    let at_or_before = match state.coretime.core_config_at_or_before(&chain, to).await {
        Ok(c) => c,
        Err(e) => return ise(e),
    };
    let (config, position) = match at_or_before {
        Some(c) if c.block_height >= from => (Some(c), "inside_window"),
        Some(c) => (Some(c), "before_window"),
        None => match state.coretime.core_config_after(&chain, to).await {
            Ok(Some(c)) => (Some(c), "after_window"),
            Ok(None) => (None, "none"),
            Err(e) => return ise(e),
        },
    };
    let readings_in_window = match state.coretime.num_cores_in_window(&chain, from, to).await {
        Ok(v) => v,
        Err(e) => return ise(e),
    };

    // ACROSS EVERY KIND, not just the inclusions the ratios count — see below.
    let max_core_seen = match state.coretime.max_core_index(&chain, from, to).await {
        Ok(m) => m,
        Err(e) => return ise(e),
    };
    let lineage = match state.coretime.occupancy_lineage(&chain, from, to).await {
        Ok(l) => l,
        Err(e) => return ise(e),
    };

    // NULL, NOT `true`, WHEN NOTHING WAS READ INSIDE THE WINDOW. "No reading
    // fell in this window" and "the readings that did agree" are different
    // claims, and rendering the first as the second would put a confident
    // stability flag on exactly the case the flag exists to warn about.
    let stable_across_window = if readings_in_window.is_empty() {
        None
    } else {
        Some(readings_in_window.len() <= 1)
    };

    let included: u64 = cores.iter().map(|c| c.included_blocks).sum();
    let cores_used = cores.len() as u64;
    let num_cores = config.as_ref().map(|c| c.num_cores);

    // A CORE INDEX AT OR ABOVE THE DENOMINATOR IS A CONTRADICTION, and it is the
    // one detector for the stale-denominator risk 0024 names that costs nothing:
    // the runtime scheduled work onto a core the reading says does not exist, so
    // the reading predates a core count that grew.
    //
    // It reads `max_core_index`, which spans EVERY kind. A `backed` row on a
    // core beyond the denominator is the same contradiction as an `included`
    // one, and a detector that ignored it would claim to look at "the data"
    // while skipping rows this very response reports in `by_kind`.
    let stale_suspected = matches!((max_core_seen, num_cores), (Some(m), Some(n)) if m >= n);

    // A WINDOW WE HOLD NO BLOCKS FOR HAS NEITHER RATIO, NO BANDS AND NO IDLE
    // COUNT — none of them are zero, they are undefined. 0/0 is not 0, and
    // "seven cores produced nothing" derived from having looked at nothing is a
    // positive claim about the network made from an absence of data.
    //
    // `cores_touched_ratio` is gated on this too, and that is the LESS obvious
    // half. Its denominator survives an empty window (100 cores are declared
    // whether or not we indexed anything), so 0/100 is arithmetically defined —
    // and factually it is "we did not look" rendered as "there is nothing
    // there", on one of the two headline numbers. The sibling ratio nulls out
    // to stop exactly that being quoted, and a band table of zeroes is refused
    // three lines down for the same reason; a bare 0.0 here would be the one
    // screenshot-able figure left saying the network sat idle.
    let have_blocks = coverage.blocks_indexed > 0;

    let slots = num_cores.map(|n| coverage.blocks_indexed * n as u64);
    let cores_touched_ratio = num_cores
        .filter(|n| *n > 0)
        .filter(|_| have_blocks)
        .map(|n| cores_used as f64 / n as f64);
    let slots_filled_ratio = slots.filter(|s| *s > 0).map(|s| included as f64 / s as f64);

    // The bands the prep measured, and the shape the endpoint exists to render.
    // Half-open (lo, hi] throughout, so the six of them partition (0, ∞)
    // exactly: a core sitting on a boundary lands in one band and they sum to
    // `cores_that_produced_anything`.
    //
    // The WHOLE OBJECT is null without blocks to divide by, rather than every
    // band reading zero — a fill ratio of 0/0 is undefined, and a band table
    // full of zeroes is a distribution somebody would screenshot.
    let bands = if have_blocks {
        let band = |lo: f64, hi: f64| -> u64 {
            cores
                .iter()
                .filter(|c| {
                    let f = c.included_blocks as f64 / coverage.blocks_indexed as f64;
                    f > lo && f <= hi
                })
                .count() as u64
        };
        serde_json::json!({
            "saturated_over_95": band(0.95, f64::INFINITY),
            "busy_75_to_95": band(0.75, 0.95),
            "half_50_to_75": band(0.50, 0.75),
            "light_25_to_50": band(0.25, 0.50),
            "sparse_5_to_25": band(0.05, 0.25),
            "barely_used_under_5": band(0.0, 0.05),
            // num_cores - used. NULL without a denominator, and ALSO null when
            // the denominator is contradicted by the data: subtracting a used
            // count from a core count the rows disprove yields a number that is
            // arithmetically coherent and factually meaningless.
            "producing_nothing": num_cores
                .filter(|_| !stale_suspected)
                .map(|n| (n as u64).saturating_sub(cores_used)),
        })
    } else {
        serde_json::Value::Null
    };

    let by_kind: serde_json::Map<String, serde_json::Value> = kinds
        .iter()
        .map(|(k, n)| (k.clone(), serde_json::json!(n)))
        .collect();

    // THREE ARMS, NOT TWO. The two-arm version said "no `num_cores` reading is
    // on record" whenever EITHER ratio was missing — and an empty window
    // reaches that arm with the reading right there in the payload beside it,
    // so the sentence would have been false about a field two lines away.
    let reads_as = match (cores_touched_ratio, slots_filled_ratio) {
        (Some(touched), Some(filled)) => format!(
            "{:.1}% of the {} declared cores produced something, while only {:.2}% of \
             core-block slots were filled. THOSE ARE DIFFERENT QUESTIONS: the first says how \
             much of the network's capacity is claimed at all, the second says how much of it \
             did work. The gap between them is idle time inside the cores that ARE used, and \
             it is the number no marketplace view can show.",
            touched * 100.0,
            num_cores.unwrap_or(0),
            filled * 100.0
        ),
        _ if num_cores.is_none() => {
            "NO RATIO IS SERVED because no `num_cores` reading is on record for this chain — \
             run `sync-core-config`. The counts below are complete and the denominator is \
             missing, which is not the same as a low utilization figure, and inventing a core \
             count to divide by is exactly the kind of number this module exists not to produce."
                .to_string()
        }
        _ => format!(
            "BOTH RATIOS ARE UNDEFINED, NOT ZERO: this window holds {} indexed block(s), so \
             there are no core-block slots to fill, {}/0 has no value, and a share of cores \
             that produced something cannot be read off blocks we do not hold. The denominator \
             ({} cores, read at #{}) is on record and named below; what is missing is the \
             window. Backfill and decode the range, then run `coretime-range`.",
            coverage.blocks_indexed,
            included,
            num_cores.unwrap_or(0),
            config.as_ref().map(|c| c.block_height).unwrap_or(0)
        ),
    };

    Json(serde_json::json!({
        "chain": chain,
        "window": {
            "from": from,
            "to": to,
            "span": span,
            "blocks_indexed": coverage.blocks_indexed,
            "contiguous": coverage.blocks_indexed == span,
            "heights_with_occupancy": coverage.heights_with_occupancy,
        },
        "denominator": {
            "num_cores": num_cores,
            "read_at_height": config.as_ref().map(|c| c.block_height),
            "runtime_version": config.as_ref().map(|c| c.runtime_version),
            "position": position,
            "distinct_num_cores_in_window": readings_in_window,
            "stable_across_window": stable_across_window,
            "stale_suspected": stale_suspected,
            "max_core_index_observed": max_core_seen,
        },
        "occupancy": {
            "cores_that_produced_anything": cores_used,
            "cores_touched_ratio": cores_touched_ratio,
            "included_candidates": included,
            "core_block_slots": slots,
            "slots_filled_ratio": slots_filled_ratio,
            // Invariant 3 applies to an aggregate as much as to a row: two
            // entries here mean the window was mapped under two rule sets and
            // the counts above are an average of them.
            "lineage": lineage
                .iter()
                .map(|(rv, mv, n)| serde_json::json!({
                    "runtime_version": rv, "mapper_version": mv, "rows": n
                }))
                .collect::<Vec<_>>(),
            "reads_as": reads_as,
        },
        "by_kind": by_kind,
        "bands": bands,
        "cores": &cores,
        "coverage": { "not_covered": coretime_not_covered() },
    }))
    .into_response()
}

// -------------------------------------------------------------- the delta

/// The two chains carrying the two halves of coretime on one network, resolved
/// from the registry and named nowhere in this file.
///
/// REFUSES RATHER THAN PICKS when the answer is not exactly one of each. Zero
/// leaves the reader with nothing and a decision about what nothing means — the
/// one decision 0025 forbids it to get wrong — and two leaves it choosing. Both
/// are seed errors, and a registry test asserts the shipped seeds give exactly
/// one of each.
fn coretime_pair<'a>(
    registry: &'a Registry,
    network: &str,
) -> Result<(&'a registry::ChainConfig, &'a registry::ChainConfig), String> {
    let pick = |module: &str, half: &str, why: &str| -> Result<&'a registry::ChainConfig, String> {
        let found = registry.chains_with_module(network, module);
        match found.len() {
            1 => Ok(found[0]),
            0 => Err(format!(
                "no chain on network '{network}' declares the `{module}` module, so the {half} \
                 half of coretime cannot be read at all. {why}"
            )),
            n => Err(format!(
                "{n} chains on network '{network}' declare the `{module}` module ({}), so the \
                 {half} half is ambiguous. This reader refuses to pick one: a delta computed \
                 against the wrong chain's rows would be a wrong number rather than a missing \
                 one. Fix the registry seeds",
                found
                    .iter()
                    .map(|c| c.id.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
        }
    };
    let occupancy = pick(
        "coretime",
        "OCCUPANCY",
        "Occupancy is read from `paraInclusion`, a relay pallet that names both the para id and \
         the core index",
    )?;
    let entitlement = pick(
        "broker",
        "ENTITLEMENT",
        "Entitlement is `pallet-broker`, which lives on the coretime parachain and nowhere else",
    )?;
    Ok((occupancy, entitlement))
}

/// The one chain in this network that carries the HRMP channel graph.
///
/// Refuses on zero or two rather than picking, which is [`coretime_pair`]'s rule
/// and for the same reason: a graph read from the wrong chain is a WRONG answer
/// rather than a missing one.
fn hrmp_chain<'a>(
    registry: &'a Registry,
    network: &str,
) -> Result<&'a registry::ChainConfig, String> {
    let found = registry.chains_with_module(network, "hrmp");
    match found.len() {
        1 => Ok(found[0]),
        0 => Err(format!(
            "no chain on network '{network}' declares the `hrmp` module, so the channel graph \
             cannot be read at all. HRMP channel state lives in the relay's `Hrmp` pallet and \
             nowhere else — a parachain sees only an abridged view of its own channels"
        )),
        n => Err(format!(
            "{n} chains on network '{network}' declare the `hrmp` module ({}), so the channel \
             graph is ambiguous. This reader refuses to pick one. Fix the registry seeds",
            found.iter().map(|c| c.id.as_str()).collect::<Vec<_>>().join(", ")
        )),
    }
}

#[derive(Debug, Deserialize)]
pub struct ChannelGraphQuery {
    /// The graph as of the newest reading at or before this height. Absent means
    /// the newest reading of all.
    pub at: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct ChannelEdgeQuery {
    pub sender: Option<u32>,
    pub recipient: Option<u32>,
}

/// The HRMP channel graph as of one reading, with the sessions nobody read.
async fn get_xcm_channels(
    State(state): State<AppState>,
    Path(network): Path<String>,
    Query(q): Query<ChannelGraphQuery>,
) -> Response {
    let cfg = match hrmp_chain(&state.registry, &network) {
        Ok(c) => c,
        Err(e) => {
            let answerable: Vec<String> = search::known_networks(&state.registry)
                .into_iter()
                .filter(|n| hrmp_chain(&state.registry, n).is_ok())
                .collect();
            return error(
                StatusCode::NOT_FOUND,
                format!(
                    "{e}. Networks this endpoint can answer for: {}",
                    if answerable.is_empty() {
                        "none".to_string()
                    } else {
                        answerable.join(", ")
                    }
                ),
            );
        }
    };
    let ise = |e: IndexError| read_failure(e);

    let all = match state.channels.readings(&cfg.id).await {
        Ok(r) => r,
        Err(e) => return ise(e),
    };
    let reading = match state.channels.reading_at(&cfg.id, q.at).await {
        Ok(r) => r,
        Err(e) => return ise(e),
    };
    let edges = match &reading {
        Some(r) => match state.channels.edges_at(&cfg.id, r.block_height).await {
            Ok(e) => e,
            Err(e) => return ise(e),
        },
        None => Vec::new(),
    };

    let unread = channels::unread_intervals(
        &all.iter()
            .map(|r| (r.block_height, r.session_index))
            .collect::<Vec<_>>(),
    );

    // A cheap integrity check the header exists to make possible: the reading
    // says how big the graph was, and the detail rows are counted separately. A
    // disagreement means a partial write survived, and it is reported rather
    // than smoothed over — in `reads_as` as well as in the boolean, because a
    // boolean nobody reads is not "loud".
    let counts_agree = reading
        .as_ref()
        .map(|r| (r.channel_count + r.open_request_count) as usize == edges.len());

    // FOUR ARMS, because "nobody read this chain", "no reading covers the height
    // you asked about", "we read it and the graph was empty" and "here is the
    // graph" are four different facts that all render as a short payload.
    let mut reads_as = match (&reading, all.is_empty()) {
        (None, true) => format!(
            "NO READING of the HRMP channel graph is on record for '{}'. That is a statement about \
             OUR INDEX and not about the chain — the graph almost certainly exists. Run \
             `channels-range {} <from> <to>` to record one reading per session boundary.",
            cfg.id, cfg.id
        ),
        (None, false) => format!(
            "{} reading(s) are on record for '{}', but none of them is at or before the height you \
             asked about. The earliest reading is #{}; this endpoint never extrapolates backwards \
             from a later one.",
            all.len(),
            cfg.id,
            all.first().map(|r| r.block_height).unwrap_or(0)
        ),
        (Some(r), _) if r.channel_count == 0 && r.open_request_count == 0 => format!(
            "Read at #{} (session {}), and the graph was EMPTY — no open channel and no pending \
             request. That is a reading, not a gap.",
            r.block_height, r.session_index
        ),
        (Some(r), _) => format!(
            "The graph as read at #{} (session {}): {} open channel(s) and {} pending request(s). \
             Channel existence changes only at session boundaries, so this is exact for the WHOLE \
             of session {} — every height inside that session has this graph. It says NOTHING \
             about any later session: the graph can move at every boundary, and this endpoint does \
             not extrapolate forward any more than it extrapolates backward.",
            r.block_height, r.session_index, r.channel_count, r.open_request_count, r.session_index
        ),
    };

    // `unread` is a windows(2) walk, so it can only ever describe gaps BETWEEN
    // readings. Sessions after the last reading are not in it and never can be,
    // and a sentence pointing at it for them would point at an empty list.
    if let Some(last) = all.last() {
        if reading.as_ref().is_some_and(|r| r.block_height == last.block_height) {
            reads_as.push_str(&format!(
                " This is the NEWEST reading on record (session {}); every session after it is \
                 unread, and `coverage.unread` does not list those — it covers only the gaps \
                 BETWEEN readings.",
                last.session_index
            ));
        }
    }

    if counts_agree == Some(false) {
        reads_as.push_str(&format!(
            " DEFECT: the reading's own header says {} edge(s) but {} detail row(s) are on record. \
             The two are written in one transaction, so a disagreement means rows were removed \
             afterwards or the header was written by something other than this pipeline. Treat \
             every count above as unreliable.",
            reading
                .as_ref()
                .map(|r| r.channel_count + r.open_request_count)
                .unwrap_or(0),
            edges.len()
        ));
    }

    Json(serde_json::json!({
        "network": network,
        "chain": cfg.id,
        "reading": reading,
        "edges": edges,
        "reads_as": reads_as,
        "coverage": {
            "readings_on_record": all.len(),
            "first_reading": all.first().map(|r| serde_json::json!({
                "block_height": r.block_height, "session_index": r.session_index })),
            "last_reading": all.last().map(|r| serde_json::json!({
                "block_height": r.block_height, "session_index": r.session_index })),
            "unread": unread,
            "unread_sessions": unread.iter().map(|u| u.sessions).sum::<u64>(),
            "header_matches_detail": counts_agree,
            "not_covered": channels::channel_not_covered(),
        }
    }))
    .into_response()
}

/// One directed edge's open/close history, DERIVED from the readings.
async fn get_xcm_channel_history(
    State(state): State<AppState>,
    Path(network): Path<String>,
    Query(q): Query<ChannelEdgeQuery>,
) -> Response {
    let cfg = match hrmp_chain(&state.registry, &network) {
        Ok(c) => c,
        Err(e) => return error(StatusCode::NOT_FOUND, e),
    };
    let (Some(sender), Some(recipient)) = (q.sender, q.recipient) else {
        return error(
            StatusCode::BAD_REQUEST,
            "both `sender` and `recipient` are required. A channel is DIRECTIONAL — (A -> B) and \
             (B -> A) are two separate channels opened, closed and deposited for independently — \
             so there is no honest way to answer for a pair without being told which direction"
                .to_string(),
        );
    };

    let obs = match state
        .channels
        .edge_observations(&cfg.id, sender, recipient)
        .await
    {
        Ok(o) => o,
        Err(e) => return read_failure(e),
    };
    let history = channels::derive_history(&obs);

    Json(serde_json::json!({
        "network": network,
        "chain": cfg.id,
        "edge": { "sender": sender, "recipient": recipient },
        "history": history,
        "coverage": {
            "derived": "Open and close are the DIFFERENCE between two readings, computed per \
                        request and never stored. A stored 'channel opened' row would be the one \
                        copy without lineage.",
            "not_covered": channels::channel_not_covered(),
        }
    }))
    .into_response()
}

/// Entitlement purchased vs occupancy realized, over one relay-block window.
///
/// THE CLAIM NOBODY ELSE CAN MAKE, and the reason both halves were built.
/// RegionX and Lastic draw core grids; both are MARKETPLACES, so they render the
/// LEASE. `/v1/coretime/{chain}/occupancy` renders the USAGE. This renders the
/// difference, and the difference is a number slice 11 could not compute at all:
/// it could only say "34.69% of ALL slots", which silently adds "nobody bought
/// it" to "somebody bought it and did not use it".
///
/// NETWORK-SCOPED, and both chains come from the registry. The path segment is a
/// NETWORK where its two neighbours on `/v1/coretime/` take a chain, because a
/// join across two chains cannot name one of them in a URL without naming the
/// other in code.
async fn get_coretime_delta(
    State(state): State<AppState>,
    Path(network): Path<String>,
    Query(q): Query<CoretimeQuery>,
) -> Response {
    let (occ_chain, ent_chain) = match coretime_pair(&state.registry, &network) {
        Ok(pair) => pair,
        Err(e) => {
            // 404 rather than 400: the request is well formed and this network
            // is not one we can answer for. The message names the networks that
            // are, derived from the registry so it cannot drift.
            // The list is filtered by the SAME predicate that just refused, so
            // it names networks this endpoint can actually answer for rather
            // than every network in the registry — which is true today only
            // because there is one.
            let answerable: Vec<String> = search::known_networks(&state.registry)
                .into_iter()
                .filter(|n| coretime_pair(&state.registry, n).is_ok())
                .collect();
            return error(
                StatusCode::NOT_FOUND,
                format!(
                    "{e}. Networks this endpoint can answer for: {}",
                    if answerable.is_empty() {
                        "none".to_string()
                    } else {
                        answerable.join(", ")
                    }
                ),
            );
        }
    };
    let (Some(from), Some(to)) = (q.from, q.to) else {
        return error(
            StatusCode::BAD_REQUEST,
            "both `from` and `to` are required, in RELAY block numbers. A delta is only \
             meaningful over a stated window, and defaulting to one would put a window nobody \
             chose underneath a percentage somebody quotes"
                .to_string(),
        );
    };
    if from > to {
        return error(StatusCode::BAD_REQUEST, "`from` must not exceed `to`".to_string());
    }
    let span = (to - from).saturating_add(1);
    if span > CORETIME_MAX_SPAN {
        return error(
            StatusCode::BAD_REQUEST,
            format!("window of {span} blocks exceeds the {CORETIME_MAX_SPAN}-block limit"),
        );
    }

    let ise = |e: IndexError| read_failure(e);
    let occupancy = match state
        .coretime
        .occupancy_by_core_and_para(&occ_chain.id, from, to)
        .await
    {
        Ok(o) => o,
        Err(e) => return ise(e),
    };
    let coverage = match state.coretime.window_coverage(&occ_chain.id, from, to).await {
        Ok(c) => c,
        Err(e) => return ise(e),
    };
    // The relay's denominator, dated EXACTLY the way the occupancy endpoint
    // dates it — including the `core_config_after` fallback, which is not
    // optional.
    //
    // WITHOUT THE FALLBACK THE TWO ENDPOINTS DISAGREE ON LIVE DATA TODAY. Slice
    // 11's verification recorded `position` coming back `after_window` (the
    // reading is at #32620900, its window ends #32614536), so on exactly the
    // window this slice's own drill uses, an at-or-before-only probe returns
    // NOTHING: `/occupancy` would report 100 declared cores and `/delta` would
    // report null, over the same blocks, in the same minute.
    let at_or_before = match state.coretime.core_config_at_or_before(&occ_chain.id, to).await {
        Ok(c) => c,
        Err(e) => return ise(e),
    };
    let (relay_config, relay_position) = match at_or_before {
        Some(c) if c.block_height >= from => (Some(c), "inside_window"),
        Some(c) => (Some(c), "before_window"),
        None => match state.coretime.core_config_after(&occ_chain.id, to).await {
            Ok(Some(c)) => (Some(c), "after_window"),
            Ok(None) => (None, "none"),
            Err(e) => return ise(e),
        },
    };
    // The broker's denominator, and there is no at-or-before to ask for: this
    // reading is dated on the CORETIME chain's number line, which cannot be
    // ordered against a relay window at all. Newest, with its height stated.
    let broker_config = match state.broker.latest_broker_config(&ent_chain.id).await {
        Ok(c) => c,
        Err(e) => return ise(e),
    };
    // THE ANCHOR IS THE WINDOW'S END, and the entitlement is whatever was
    // governing then. `relay_block <= to` is the constraint slice 13's own
    // VERIFY doc omitted, and omitting it produces a join that matches on core
    // index alone — which proves the two INDEX SPACES align and not that the
    // entitlement agreed with the usage at a shared instant.
    let entitlement = match state.broker.entitlement_at(&ent_chain.id, to).await {
        Ok(e) => e,
        Err(e) => return ise(e),
    };

    let occupancy_lineage = match state.coretime.occupancy_lineage(&occ_chain.id, from, to).await {
        Ok(l) => l,
        Err(e) => return ise(e),
    };

    let report = coretime_delta::compute(coretime_delta::DeltaInput {
        occupancy: &occupancy,
        entitlement: &entitlement,
        blocks_indexed: coverage.blocks_indexed,
        window_from: from,
        anchor_relay_block: to,
        relay_num_cores: relay_config.as_ref().map(|c| c.num_cores),
        broker_core_count: broker_config.as_ref().map(|c| c.core_count),
        first_core: broker_config.as_ref().and_then(|c| c.first_core),
    });

    // Invariant 3 applies to an aggregate as much as to a row, and the
    // entitlement side needs its own stamp: two entries mean the governing
    // assignments were mapped under two RULE SETS.
    let mut ent_lineage: std::collections::BTreeMap<(u64, u32), u64> =
        std::collections::BTreeMap::new();
    for r in &entitlement {
        *ent_lineage.entry((r.runtime_version, r.mapper_version)).or_default() += 1;
    }

    Json(serde_json::json!({
        "network": network,
        "chains": {
            "occupancy": occ_chain.id,
            "entitlement": ent_chain.id,
            "reads_as": "the occupancy half is the RELAY's candidate events and the entitlement \
                         half is `pallet-broker` on the coretime chain. Both were resolved from \
                         the registry; neither id appears in this reader's code.",
        },
        "window": {
            "from": from,
            "to": to,
            "span": span,
            "blocks_indexed": coverage.blocks_indexed,
            "contiguous": coverage.blocks_indexed == span,
            "heights_with_occupancy": coverage.heights_with_occupancy,
            "unit": "RELAY block numbers. `core_assignments.relay_block` is on this same line \
                     (it is `CoreAssigned.when`, an exact multiple of 80), which is what makes \
                     this a join and not a correlation.",
        },
        "entitlement": {
            "anchor_relay_block": report.anchor_relay_block,
            "governing_relay_blocks": report.governing_relay_blocks,
            "stable_across_window": report.entitlement_stable_across_window,
            "changed_in_window": report.changed_in_window,
            "cores_with_entitlement": report.cores_with_entitlement,
            "cores_without_entitlement": report.cores_without_entitlement,
            "unknown_cores": report.unknown_cores,
            "task_entitled_cores": report.task_entitled_cores,
            "pool_cores": report.pool_cores,
            "idle_cores": report.idle_cores,
            "unattributable_cores": report.unattributable_cores,
            "lineage": ent_lineage
                .iter()
                .map(|((rv, mv), n)| serde_json::json!({
                    "runtime_version": rv, "mapper_version": mv, "rows": n
                }))
                .collect::<Vec<_>>(),
        },
        "attribution": {
            "agree_cores": report.agree_cores,
            "disagree_cores": report.disagree_cores,
            "pool_cores_with_occupancy": report.pool_cores_with_occupancy,
            "idle_cores_with_occupancy": report.idle_cores_with_occupancy,
            "unknown_cores_with_occupancy": report.unknown_cores_with_occupancy,
            "candidates_total": report.candidates_total,
            "attributed_candidates": report.attributed_candidates,
            "unattributed_candidates": report.unattributed_candidates,
            "unattributed_by_reason": report.unattributed_by_reason,
        },
        "waste": report.waste,
        "waste_withheld_because": report.waste_withheld_because,
        "denominators": {
            "relay_num_cores": report.relay_num_cores,
            "relay_read_at_height": relay_config.as_ref().map(|c| c.block_height),
            "relay_runtime_version": relay_config.as_ref().map(|c| c.runtime_version),
            // Where the relay's reading sits relative to the window. There is
            // deliberately no counterpart for the broker's: that one is dated on
            // the coretime chain's own heights and cannot be positioned against
            // a relay window at all.
            "relay_reading_position": relay_position,
            "broker_core_count": report.broker_core_count,
            "broker_read_at_height": broker_config.as_ref().map(|c| c.block_height),
            "broker_runtime_version": broker_config.as_ref().map(|c| c.runtime_version),
            // TWO ARMS, because the one-arm version declared two numbers on a
            // payload that could be serving one. On a DB where `sync-core-config`
            // has not run — or where its only reading sits after the window and
            // the fallback finds nothing — the cross-check 0025 calls "what
            // makes the join believable" simply did not happen, and a sentence
            // saying otherwise would hide that.
            "reads_as": if report.relay_num_cores.is_some() && report.broker_core_count.is_some() {
                "TWO CHAINS, TWO STORAGE ITEMS, ONE NUMBER — and the two heights are on DIFFERENT \
                 NUMBER LINES. `relay_read_at_height` is a relay block and `broker_read_at_height` \
                 is a coretime block; they are compared by VALUE and never ordered against each \
                 other or against the window. A disagreement means one half is being counted \
                 against the wrong denominator, and this reader withholds the waste figure rather \
                 than picking one."
            } else {
                "ONE OF THE TWO DENOMINATORS IS ABSENT, so the cross-check that makes this join \
                 believable DID NOT HAPPEN — `checks.denominators_agree` reads `unknown`, which \
                 is not `ok`. Run `sync-core-config` on the occupancy chain and \
                 `sync-broker-config` on the entitlement chain; until both are on record, nothing \
                 here has compared the two chains' core counts at all."
            },
        },
        "market": {
            "first_core": report.first_core,
            "read_at_height": broker_config.as_ref().map(|c| c.block_height),
            "reads_as": "cores below `first_core` are reserved system cores and cores at or \
                         above it are the bulk market. It is `SaleInfo.first_core`, read at the \
                         CORETIME height above and NOT aligned with this window — it moves every \
                         sale. NULL means sales never started or no reading was taken, and the \
                         split is then ABSENT rather than assumed: `first_core = 0` would move \
                         every reserved core into the market.",
        },
        "checks": report.checks,
        "occupancy_lineage": occupancy_lineage
            .iter()
            .map(|(rv, mv, n)| serde_json::json!({
                "runtime_version": rv, "mapper_version": mv, "rows": n
            }))
            .collect::<Vec<_>>(),
        "reads_as": report.reads_as,
        "cores": report.cores,
        "coverage": { "not_covered": coretime_delta_not_covered() },
    }))
    .into_response()
}

/// `?core=` | `?task=` — the entitlement timeline for one subject.
#[derive(Deserialize)]
struct EntitlementQuery {
    core: Option<u32>,
    task: Option<u32>,
    limit: Option<u32>,
}

const ENTITLEMENT_DEFAULT_LIMIT: u32 = 50;
const ENTITLEMENT_MAX_LIMIT: u32 = 500;

/// What happened to one core, or to one task.
///
/// TWO SUBJECTS AND NOT ONE, because they are different questions and the
/// difference is measured: a renewal MOVES the core index (para 3428's five
/// renewals moved every one — 35→43, 36→44, 37→45, 40→46, 41→47), so asking by
/// core follows a SLOT and asking by task follows the TENANT. A single endpoint
/// that quietly accepted either and answered one would be the second kind of
/// wrong number this module exists to avoid.
///
/// This is what earns migration 0026's other three indexes.
async fn get_coretime_entitlement(
    State(state): State<AppState>,
    Path(chain): Path<String>,
    Query(q): Query<EntitlementQuery>,
) -> Response {
    let Some(cfg) = state.registry.chain(&chain) else {
        return error(StatusCode::NOT_FOUND, format!("unknown chain '{chain}'"));
    };
    if !cfg.has_module("broker") {
        return error(
            StatusCode::NOT_FOUND,
            format!(
                "'{chain}' does not declare the `broker` module. Entitlement is `pallet-broker`, \
                 which lives on the coretime parachain — the relay carries the OCCUPANCY half \
                 (/v1/coretime/{chain}/occupancy) and no broker events whatsoever, so an empty \
                 answer here would read as 'nobody bought a core' rather than as 'wrong chain'"
            ),
        );
    }
    let subject = match (q.core, q.task) {
        (Some(c), None) => Ok(("core", c)),
        (None, Some(t)) => Ok(("task", t)),
        (Some(_), Some(_)) => Err(
            "pass `core` OR `task`, not both: a core index identifies an entitlement only within \
             one region while a task is durable across sale cycles, so a combined filter would \
             answer a question with no stable meaning"
                .to_string(),
        ),
        (None, None) => Err(
            "one of `core` or `task` is required. Ask by `task` to follow a CHAIN across sale \
             cycles and by `core` to follow a SLOT — a renewal moves the index, so the two \
             diverge by construction"
                .to_string(),
        ),
    };
    let (kind, id) = match subject {
        Ok(s) => s,
        Err(e) => return error(StatusCode::BAD_REQUEST, e),
    };
    let limit = q
        .limit
        .unwrap_or(ENTITLEMENT_DEFAULT_LIMIT)
        .clamp(1, ENTITLEMENT_MAX_LIMIT);

    let ise = |e: IndexError| read_failure(e);
    let (events, assignments) = if kind == "core" {
        (
            state.broker.events_for_core(&chain, id, limit).await,
            state.broker.assignments_for_core(&chain, id, limit).await,
        )
    } else {
        (
            state.broker.events_for_task(&chain, id, limit).await,
            state.broker.assignments_for_task(&chain, id, limit).await,
        )
    };
    let events = match events {
        Ok(e) => e,
        Err(e) => return ise(e),
    };
    let assignments = match assignments {
        Ok(a) => a,
        Err(e) => return ise(e),
    };

    Json(serde_json::json!({
        "chain": chain,
        "subject": { "kind": kind, "id": id },
        "limit": limit,
        // PER LIST, because one boolean over two lists cannot say which was cut.
        "truncated": {
            "events": events.len() as u32 == limit,
            "assignments": assignments.len() as u32 == limit,
        },
        "events": events,
        "assignments": assignments,
        "reads_as": if kind == "task" {
            "asked by TASK, so this follows the tenant across sale cycles even when its core \
             index moves — which a renewal does."
        } else {
            "asked by CORE, so this follows a SLOT. A renewal moves the index, so a tenant may \
             leave this timeline and continue on another core; ask by `task` to follow it."
        },
        "coverage": { "not_covered": entitlement_not_covered() },
    }))
    .into_response()
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
        Err(e) => return read_failure(e),
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
        Err(e) => return read_failure(e),
    };
    let sides: Vec<&str> = rows.iter().map(|r| r.side.as_str()).collect();
    // "one chain … and another" is a CLAIM, and on live data it can be false:
    // Asset Hub #19407624 received a message over HRMP and forwarded it onward
    // to Ethereum in the same block, with the topic propagating across the hop,
    // so both halves of that id sit on ONE chain. Verified Phase 3 slice 4.
    let one_chain = rows
        .iter()
        .all(|r| rows.first().is_some_and(|f| f.chain_id == r.chain_id));
    let reads_as = match (
        sides.contains(&"sent"),
        sides.contains(&"received"),
        rows.is_empty(),
    ) {
        (_, _, true) => "no chain we index has seen this id — which is not the same as the \
                         message not existing, since a journey through an unindexed chain \
                         leaves no row here",
        (true, true, _) if one_chain => {
            "both halves are on record ON ONE CHAIN, which is what a HOP looks like: this \
             chain processed the id and then sent it on under the same id, because the topic \
             propagates across a hop. One message passing through, not a round trip — see \
             coverage.not_covered"
        }
        (true, true, _) => "both halves are on record: one chain reported sending this id and \
                            another reported processing it. That is strong evidence of one \
                            message and is still not an assertion — see coverage.not_covered",
        (true, false, _) => "only the SENDING half is on record. That can mean the message is \
                             still in flight, that the receiving chain is not indexed here, or \
                             that it was dropped in transit — indistinguishable from this side \
                             UNLESS the row's counterparty names another consensus system, in \
                             which case the journey endpoint says so outright",
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
            Err(e) => return read_failure(e),
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
        Err(e) => return read_failure(e),
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

    // Did any leg address a consensus system OTHER than its own chain's? A
    // Snowbridge export does; a Location that names this network absolutely
    // (`remote:polkadot/para:2034`) does NOT, which is why the chain's own
    // network has to be resolved rather than the `remote:` prefix trusted.
    let remote_destinations: Vec<String> = {
        let mut seen: Vec<String> = Vec::new();
        for r in &rows {
            let (Some(cp), Some(chain)) = (
                r.counterparty.as_deref(),
                state.registry.chain(&r.chain_id),
            ) else {
                continue;
            };
            if is_foreign_consensus(cp, &chain.network) && !seen.contains(&cp.to_string()) {
                // the WHOLE counterparty, because `remote:kusama/para:1000` and
                // `remote:kusama/para:2000` are different destinations and
                // collapsing them to the consensus would drop the para id
                seen.push(cp.to_string());
            }
        }
        seen
    };
    let leaves_consensus = !remote_destinations.is_empty();
    // "dotlens does not index it" is DERIVED, not assumed. It is true of every
    // remote destination today because one network is registered — and the day a
    // Kusama seed lands it stops being true, at which point a hard-coded
    // sentence would claim a journey is unfollowable while its receiving half
    // sits in the same database.
    let remote_is_indexed = remote_destinations.iter().all(|d| {
        let consensus = d
            .strip_prefix("remote:")
            .unwrap_or(d.as_str())
            .split('/')
            .next()
            .unwrap_or("");
        state.registry.chains().any(|c| c.network == consensus)
    });

    // WHERE A REMOTE LEG GOES, phrased once because more than one shape can
    // carry one. DERIVED from the registry, never assumed — see
    // `remote_is_indexed`.
    let remote_reach = if remote_is_indexed {
        "a consensus system this chain is not part of — its own half is indexed here but \
         nothing ties the two id spaces together"
    } else {
        "which dotlens does not index at all"
    };

    let reads_as = match shape {
        "unseen" => "no chain we index has seen this id, on either side. That is not the same \
                     as the message not existing: a journey through a chain we do not map \
                     leaves no row here"
            .to_string(),
        // A bridged leg's `send_only` is EXPECTED, not a gap, and conflating the
        // two would be the honest-coverage doctrine failing on the one case
        // where the answer is knowable.
        "send_only" if leaves_consensus => format!(
            "the sending half is on record and the journey STOPS HERE ON PURPOSE: this message \
             is addressed to {}, {}. That is a boundary, not a missing row — following it \
             needs the bridge tracer (ARCHITECTURE §10), which reads Snowbridge and ISMP \
             lifecycles rather than XCM ids",
            remote_destinations.join(", "),
            remote_reach
        ),
        // A JOURNEY CAN BOTH BE STITCHED AND STILL LEAVE, and live data says so
        // rather than theory: Asset Hub #19407624 — the block this slice was
        // written for — is a RELAY HOP (Hydration sent it, Asset Hub received it
        // over HRMP and forwarded it on to Ethereum with the topic propagating
        // across the hop), so its shape is `send_and_receive`. Keying the
        // boundary sentence on `send_only` alone left the only bridged journey
        // this index actually holds reading as a complete story in prose while
        // `leaves_consensus` said otherwise one field away.
        "send_and_receive" => format!(
            "one message, stitched across {} chain(s): {} sending observation(s) and {} \
             receiving one(s) carrying the same id. The stitch is id equality plus {} recorded \
             alias link(s) — see checks and aliases for what corroborates it{}",
            distinct_chains(&rows),
            sends.len(),
            receives.len(),
            links.len(),
            if leaves_consensus {
                format!(
                    ". The journey then STOPS ON PURPOSE: its onward leg is addressed to {}, \
                     {} — a boundary rather than a missing row, and following it needs the \
                     bridge tracer (ARCHITECTURE §10) rather than XCM ids",
                    remote_destinations.join(", "),
                    remote_reach
                )
            } else {
                String::new()
            }
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
            // A counterparty naming another consensus is NOT COMPARABLE against
            // a registry that only knows this one — reported as unknown, never
            // as a contradiction. Getting this wrong would turn slice 4's
            // correctness fix into a check that fails on the very messages it
            // was written to describe.
            let said_sender = s
                .counterparty
                .as_deref()
                .and_then(|c| comparable_counterparty(c, &from.network))
                .map(str::to_string);
            let said_receiver = r
                .counterparty
                .as_deref()
                .and_then(|c| comparable_counterparty(c, &to.network))
                .map(str::to_string);
            let verdict = match (
                &expect_sender_says,
                &said_sender,
                &expect_receiver_says,
                &said_receiver,
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
        // Whether any leg addressed a consensus system dotlens does not index.
        // A `send_only` that leaves the ecosystem is a BOUNDARY, and a consumer
        // that cannot tell it from an in-flight message would read every
        // Snowbridge export as a dropped one.
        "leaves_consensus": leaves_consensus,
        "remote_destinations": remote_destinations,
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

/// Attach to one serialized simulation everything that is TRUE OF THAT ROW
/// rather than of the endpoint serving it: the tier's own coverage list, the
/// anchor decision, what its storage diff covers, and whether its state was
/// fabricated.
///
/// SHARED BY BOTH SURFACES THAT SERVE A SIMULATION, and that is the whole reason
/// it is a function. It lived inline in `/v1/sim/{chain}/calls/{hash}` and the
/// REFERENDUM page — the killer flow, the page somebody actually reads — served
/// the same rows raw: a fork row appeared there with the Tier 1 coverage list
/// ("the call is dispatched directly", false of this tier on either route), with
/// its `storage_diff` and no `diff_covers` to say the entries are the vehicle's,
/// and — worst — a COUNTERFACTUAL row rendered its `overrides` with no
/// `is_counterfactual` marker at all. Fabricated state on a governance page,
/// unlabelled. Two renderings of one row that could disagree is the defect class
/// this project keeps finding; this is that class on the surface that matters
/// most.
///
/// `attribution` and `legs` are deliberately NOT here: they need an async index
/// read per row, and the endpoint that can afford one attaches them itself.
fn enrich_simulation(row: &SimulationRow, obj: &mut serde_json::Map<String, serde_json::Value>) {
    // EACH ROW CARRIES THE COVERAGE THAT IS TRUE OF ITS OWN TIER. A
    // response-level `not_covered` describes the DRY-RUN tier; a fork row's
    // limits are different in kind (see `fork_not_covered`), and wording one
    // shared list until it is true of both is exactly how this project has
    // shipped five false lines.
    obj.insert(
        "tier_coverage".into(),
        serde_json::json!({
            "tier": row.tier,
            "dispatch_route": row.dispatch_route,
            "not_covered": if row.tier == sim::TIER_FORK {
                fork_route_not_covered(row.dispatch_route.as_deref())
            } else {
                sim_not_covered()
            },
        }),
    );
    // A COUNTERFACTUAL SAYS SO ON ITS OWN ROW, not only in a coverage
    // list somebody may not read. `overrides` is NULL on a faithful run
    // and never `[]`, so this block appears exactly when the state was
    // fabricated.
    // WHERE THE SCHEDULER WAS TOLD TO LOOK. Surfaced unconditionally on a
    // scheduled row, because the single most likely wrong answer this
    // tier can give is `not_dispatched`, and the difference between "the
    // chain declined" and "the agenda was written on the wrong number
    // line" is exactly this object.
    if let Some(anchor) = &row.agenda_anchor {
        obj.insert("agenda_anchor".into(), anchor.clone());
    }
    // WHAT THE DIFF IS ABOUT, BESIDE THE DIFF.
    //
    // On a scheduled row `storage_diff` describes the no-op vehicle
    // extrinsic and NOT the call, because the dispatch happens in
    // `on_initialize` and the harness returns the `apply_extrinsic`
    // phase only. Twelve decoded entries under a heading that says
    // "storage diff" is the most confidently wrong thing this tier can
    // serve, so the row says what its own bytes cover rather than
    // leaving it to a coverage list somebody may not read — the same
    // argument that put `is_counterfactual` on the row instead of only
    // in `not_covered`.
    //
    // COMPUTED, NEVER STORED: it is a function of `diff_status` and
    // `dispatch_route`, both already on the row, and a stored third copy
    // would be the one that can go stale.
    if row.tier == sim::TIER_FORK {
        // NO DEFAULTS. A row recorded before 0022/0023 may carry NULL in
        // either column, and `unwrap_or("unavailable")` there would say
        // "this build of the harness has no diff method at all" — a
        // POSITIVE claim, about a run nobody made it of. That is the
        // conflation the column exists to refuse, so an absent column is
        // rendered absent and the row says it does not know.
        let scope = match (row.diff_status.as_deref(), row.dispatch_route.as_deref()) {
            (Some(status), Some(route)) => {
                let covers = sim::diff_covers_subject(status, route);
                serde_json::json!({
                    "diff_status": status,
                    "dispatch_route": route,
                    "covers_this_call": covers,
                    "read_instead": (!covers).then_some("emitted_events"),
                    // MATCHED ON THE STATUS AS WELL AS THE VERDICT.
                    // `covers == true` is reached two ways — an extrinsic
                    // whose own phase is returned, and a whole-block diff
                    // on either route — and one sentence for both would
                    // tell a scheduled `decoded` row that its call "was
                    // applied as an extrinsic", which is the second-
                    // consumer defect this slice is otherwise fixing.
                    "reads_as": match (status, covers) {
                        (sim::DIFF_STATUS_DECODED, _) =>
                            "every phase of the block was returned, `on_initialize` \
                             included — so wherever this call ran, its writes are among \
                             the entries in `storage_diff`",
                        (sim::DIFF_STATUS_EXTRINSIC_ONLY, true) =>
                            "the entries in `storage_diff` are this call's own writes: \
                             the call was applied as an extrinsic, so the phase the \
                             harness returns is the phase it ran in",
                        (sim::DIFF_STATUS_EXTRINSIC_ONLY, false) =>
                            "THE ENTRIES IN `storage_diff` ARE NOT THIS CALL'S. The call \
                             was dispatched by the scheduler in `on_initialize`, and the \
                             harness returns the applied extrinsic's phase only — so \
                             what is listed is the no-op vehicle's writes. \
                             `emitted_events` is the complete record of what the call \
                             did, and nothing was synthesised to fill the gap",
                        _ =>
                            "there is no storage diff on this row at all — see \
                             `diff_status` for whether the harness has no diff method, \
                             declined this block, or answered in a shape this version \
                             cannot read. None of those means the call changed nothing",
                    },
                })
            }
            (status, route) => serde_json::json!({
                "diff_status": status,
                "dispatch_route": route,
                "covers_this_call": serde_json::Value::Null,
                "read_instead": "emitted_events",
                "reads_as": "this row does not record what its diff covers — it predates \
                             the columns that say so. Read `emitted_events`, which every \
                             row of this tier has always carried in full",
            }),
        };
        obj.insert("diff_covers".into(), scope);
    }
    if let Some(overrides) = &row.overrides {
        obj.insert(
            "counterfactual".into(),
            serde_json::json!({
                "is_counterfactual": true,
                "override_hash": row.override_hash,
                "override_count": overrides.as_array().map(|a| a.len()).unwrap_or(0),
                "reads_as": "THIS IS NOT WHAT THE CHAIN DID. Storage was injected before \
                             the call ran, so every figure below is what WOULD have \
                             happened had the chain held the values in `overrides` — each \
                             of which carries `before`, the value the real chain actually \
                             held at this block",
                "not_covered": counterfactual_not_covered(),
            }),
        );
    } else if row.tier == sim::TIER_FORK {
        // SELECTED BY ROUTE, for the same reason `fork_route_not_covered`
        // is. "What was injected is the harness's own setup — the scheduled
        // task…the agenda slot…entries marked `from_harness`" is true of the
        // SCHEDULED route and false of every clause on the extrinsic one:
        // there is no scheduled task, `agenda_anchor` is NULL (so the text
        // pointed at an absent column), and no diff entry is ever
        // `from_harness`. One faithful-fork sentence for both routes is the
        // shared-list defect this slice already refused one field over.
        let reads_as = match row.dispatch_route.as_deref() {
            Some(sim::ROUTE_SCHEDULED) => {
                "a faithful fork: no storage was injected to CHANGE the \
                 chain's state. What was injected is the harness's own \
                 setup — the scheduled task that dispatches the call, and \
                 (for a call too large to inline) its preimage and request \
                 status. The agenda slot it went into (see `agenda_anchor` \
                 for which height, and on which number line) is REPLACED \
                 rather than appended to, so any task the chain really had \
                 scheduled there did not run here; diff entries marked \
                 `from_harness` are that setup and not effects of the call"
            }
            Some(sim::ROUTE_DRY_RUN_EXTRINSIC) => {
                "a faithful fork: no storage was injected at all. The call \
                 was applied as an ordinary extrinsic, so there is no \
                 scheduled task, no agenda slot and no harness setup in the \
                 diff — every entry is the call's own doing. What IS mocked \
                 is the signature: dotlens holds no key, so the run proves \
                 nothing about whether the signer could have authorised it"
            }
            // Legacy or absent: claim NEITHER route's specifics rather than
            // defaulting to one of them, which is how a row acquires a
            // sentence about machinery it never used.
            _ => {
                "a faithful fork: no storage was injected to CHANGE the \
                 chain's state. This row does not record which route it \
                 took, so what the harness set up on its own behalf cannot \
                 be stated here — `storage_diff` marks any such entry \
                 `from_harness`"
            }
        };
        obj.insert(
            "counterfactual".into(),
            serde_json::json!({ "is_counterfactual": false, "reads_as": reads_as }),
        );
    }
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
    // Clamped at 1, not 0: `?limit=0` would return an empty list under a
    // coverage note that says "nobody has previewed this call", which would be
    // this endpoint stating something false about the data on the caller's own
    // instruction.
    //
    // AND CLAMPED AT 25 RATHER THAN 100 SINCE SLICE 5, because the endpoint went
    // from one query to 2N+1: every row now costs a baseline lookup and a legs
    // lookup. Both are single-key reads on indexes that exist, but 201 round
    // trips on a public GET is not a shape to leave lying around.
    let limit = q.limit.unwrap_or(10).clamp(1, 25) as u32;
    let rows = match state.sim.simulations(&chain, &hash, limit).await {
        Ok(r) => r,
        Err(e) => return read_failure(e),
    };
    // FILTERED AFTER THE LIMIT, and said out loud rather than left to be found in
    // an EXPLAIN: `simulation_results_call_idx` does not carry `tier`, so a
    // tier-filtered page of N may return fewer than N. Pushing it into the index
    // is a migration, and this reader is not yet the one that earns it.
    let rows: Vec<SimulationRow> = match &q.tier {
        None => rows,
        Some(t) => rows.into_iter().filter(|r| &r.tier == t).collect(),
    };

    // Each row is enriched with the two things a raw forwarded list cannot say
    // on its own: WHICH of those messages this call is responsible for, and what
    // the destinations did with the ones anybody previewed. Both are reads over
    // rows that already carry lineage — the attribution is a pure difference of
    // two stored columns and is deliberately not materialised anywhere, the same
    // argument that kept `treasury.consolidated_position` and the XCM journey
    // out of the schema.
    //
    // COST, stated because it is two extra queries PER ROW: the limit is clamped
    // to 100 and defaults to 10, and both reads are single-key lookups on
    // indexes that exist. This endpoint is not the omnibox.
    let mut simulations = Vec::with_capacity(rows.len());
    for row in rows {
        // ATTRIBUTION IS ATTACHED ONLY TO ROWS THAT HAVE A FORWARDED LIST.
        // A fork row does not call `dry_run_call` and produces none, and
        // `attribution_json` would answer "no no-op was recorded at this state"
        // — true of the data and misleading about the tier, since a fork has no
        // baseline concept at all. A field that describes a column belongs on
        // rows that have the column; this is the same rule that split
        // `sim_attribution_not_covered` out in slice 5.
        let attribution = match &row.forwarded_xcms {
            None => None,
            Some(forwarded) => {
                let baseline = match &row.baseline_input_hash {
                    None => None,
                    Some(h) => {
                        match state
                            .sim
                            .simulation_at(&chain, &row.at_block_hash, h, &row.tier)
                            .await
                        {
                            Ok(b) => b,
                            Err(e) => {
                                return read_failure(e)
                            }
                        }
                    }
                };
                Some(attribution_json(
                    row.baseline_input_hash.as_deref(),
                    baseline.as_ref().and_then(|b| {
                        b.forwarded_xcms
                            .as_ref()
                            .map(|f| (f, b.call_summary.as_deref()))
                    }),
                    forwarded,
                ))
            }
        };

        // `legs` DESCRIBES `forwarded_xcms` — they are the previewed arrivals of
        // this row's own forwarded messages — so it belongs only on rows that
        // have one, for the same reason `forwarded_attribution` does. A fork row
        // carrying `legs: []` beside a coverage note saying "an empty list means
        // nobody followed them" would be the FIFTH false line this project has
        // shipped on a second consumer, in the slice that names the class.
        let legs = match &row.forwarded_xcms {
            None => None,
            Some(_) => match state.xcm_sim.legs(&chain, &row.input_hash, 25).await {
                Ok(l) => Some(l),
                Err(e) => return read_failure(e),
            },
        };
        // NO SILENT DEFAULT HERE. `unwrap_or(json!({}))` would serve a
        // simulation with no chain, no status and no lineage, and
        // `unwrap_or_default()` on the legs would render `null` where `[]` means
        // "nobody followed them" — a distinction this endpoint's own coverage
        // text turns on. Unreachable today, which is exactly when such a default
        // gets written and then survives a shape change (the same argument
        // `SimRecord::new` makes about `event_count` beside an empty list).
        let (mut value, legs) = match (
            serde_json::to_value(&row),
            serde_json::to_value(legs.unwrap_or_default()),
        ) {
            (Ok(v), Ok(l)) => (v, l),
            _ => {
                return error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "a recorded simulation could not be serialized".to_string(),
                )
            }
        };
        if let Some(obj) = value.as_object_mut() {
            if let Some(attribution) = attribution {
                obj.insert("forwarded_attribution".into(), attribution);
                obj.insert("legs".into(), legs);
            }
            enrich_simulation(&row, obj);
        }
        simulations.push(value);
    }

    Json(serde_json::json!({
        "chain": chain,
        "call_hash": hash,
        "simulations": simulations,
        "coverage": {
            "recorded_only": "this endpoint serves simulations that were RUN; it never \
                              starts one. An empty list means nobody has previewed this \
                              call at any state, not that the call does nothing",
            // `legs` lives on THIS endpoint only, so its caveat does too rather
            // than riding in a shared list that another response also serves.
            "legs_are_recorded_too": "`legs` holds the previewed ARRIVALS of this call's own \
                                      forwarded messages, run by `simulate-forwarded`. An \
                                      empty list means nobody followed them; a destination \
                                      dotlens does not index is followed to that boundary \
                                      and no further, and leaves no leg behind. A leg is \
                                      rendered here with its RAW forwarded_xcms — the \
                                      difference for a leg's own onward messages is on \
                                      /v1/sim/{chain}/xcm/{program_hash}",
            "not_covered": sim_not_covered(),
            "tiers_carry_their_own": "`not_covered` here describes the DRY-RUN tier. A row \
                                      carries the list that is true of ITS tier in \
                                      `tier_coverage.not_covered` — a fork row's limits are \
                                      different in kind, not a subset",
            "attribution_not_covered": sim_attribution_not_covered(),
            "arrival_not_covered": xcm_sim_not_covered(),
        },
    }))
    .into_response()
}

/// Recorded Tier 1 previews of one arriving PROGRAM, on one chain.
///
/// The program hash is blake2b-256 of the encoded `VersionedXcm`, so anyone
/// holding the same bytes can recompute it and ask this question without
/// dotlens' help — the same property `call_hash` has on the sending side.
async fn get_xcm_simulations(
    State(state): State<AppState>,
    Path((chain, program_hash)): Path<(String, String)>,
    Query(q): Query<SimQuery>,
) -> Response {
    if state.registry.chain(&chain).is_none() {
        return error(StatusCode::NOT_FOUND, format!("unknown chain '{chain}'"));
    }
    let hash = normalize_call_hash(&program_hash);
    let limit = q.limit.unwrap_or(10).clamp(1, 25) as u32;
    let rows = match state.xcm_sim.xcm_simulations(&chain, &hash, limit).await {
        Ok(r) => r,
        Err(e) => return read_failure(e),
    };

    // The SAME difference the call side gets, over the same builder. Shipping it
    // on only one of the two tables would have left the identical defect one hop
    // along the journey this slice exists to follow — which is what the
    // migration's own comment promises, and a promise made in a schema is kept
    // in a reader or not at all.
    let mut simulations = Vec::with_capacity(rows.len());
    for row in rows {
        let baseline = match &row.baseline_input_hash {
            None => None,
            Some(h) => {
                match state
                    .xcm_sim
                    .xcm_simulation_at(&chain, &row.at_block_hash, h, &row.tier)
                    .await
                {
                    Ok(b) => b,
                    Err(e) => return read_failure(e),
                }
            }
        };
        let attribution = attribution_json(
            row.baseline_input_hash.as_deref(),
            baseline
                .as_ref()
                .map(|b| (&b.forwarded_xcms, b.program_summary.as_deref())),
            &row.forwarded_xcms,
        );
        let Ok(mut value) = serde_json::to_value(&row) else {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "a recorded preview could not be serialized".to_string(),
            );
        };
        if let Some(obj) = value.as_object_mut() {
            obj.insert("forwarded_attribution".into(), attribution);
        }
        simulations.push(value);
    }

    Json(serde_json::json!({
        "chain": chain,
        "program_hash": hash,
        "simulations": simulations,
        "coverage": {
            "recorded_only": "this endpoint serves previews that were RUN; it never starts \
                              one. An empty list means nobody has previewed this program on \
                              this chain, not that the program does nothing",
            "not_covered": xcm_sim_not_covered(),
            "attribution_not_covered": sim_attribution_not_covered(),
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
            Err(e) => return read_failure(e),
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
            Err(e) => return read_failure(e),
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
            Err(e) => return read_failure(e),
        };
        let delegations: Vec<DelegationRow> =
            match state.gov.account_delegations(&w.chain, &account_id).await {
                Ok(d) => d.into_iter().filter(|r| keep(&r.class)).collect(),
                Err(e) => return read_failure(e),
            };
        let anchors: Vec<VotingAnchorRow> =
            match state.gov.voting_anchors(&w.chain, &account_id).await {
                Ok(a) => a.into_iter().filter(|r| keep(&r.class)).collect(),
                Err(e) => return read_failure(e),
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
        Err(e) => read_failure(e),
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
            Err(e) => return read_failure(e),
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
            Err(e) => return read_failure(e),
        }
        let events = match state.treasury.spend_events(&w.chain, &instance, &kind, id).await {
            Ok(e) => e,
            Err(e) => return read_failure(e),
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
            Err(e) => return read_failure(e),
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
            Err(e) => return read_failure(e),
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
            Err(e) => return read_failure(e),
        }
        let events = match state
            .bounties
            .bounty_events(&w.chain, &instance, id, q.child)
            .await
        {
            Ok(e) => e,
            Err(e) => return read_failure(e),
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
            "note": "one row per REPRESENTATION on this chain. The FUNGIBLE half of \
                     the identity graph now exists as data — `absolute_key` is the \
                     observer-free name, and /v1/assets/identity?key= lists every \
                     representation of one logical asset across every chain. What \
                     is still Phase 5 is the NON-fungible half and the bridged \
                     ERC-20 ↔ foreign-asset ↔ precompile links, which are not \
                     derivable from a Location and need a real graph \
                     (ARCHITECTURE §8)",
            "assets": assets,
        }))
        .into_response(),
        Err(e) => read_failure(e),
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
        Err(e) => return read_failure(e),
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
            Err(e) => return read_failure(e),
        };
        let assets = match state.assets.assets(chain).await {
            Ok(a) => a,
            Err(e) => return read_failure(e),
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
                // DERIVED, not asserted. An earlier draft said flatly that no
                // account on Hydration is registered — which the same response
                // would have contradicted the moment one was, and this project
                // has shipped a `not_covered` line that was false on its own
                // page three times now.
                hydration_coverage_line(&accounts),
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

// ------------------------------------------------- cross-chain consolidation

#[derive(Deserialize)]
struct IdentityQuery {
    /// The observer-free name, exactly as `core.assets.absolute_key` holds it.
    key: String,
}

/// EVERY REPRESENTATION OF ONE LOGICAL ASSET, ACROSS EVERY CHAIN — PRODUCT.md
/// gap 6, and the question no chain-shaped explorer can ask.
///
/// The key is an ugly string (a JSON junction array) and that is deliberate: it
/// is the CANONICAL NAME, so a reader can re-derive it themselves from a chain's
/// own location and the registry's path for that chain. A short opaque id would
/// be prettier and un-checkable, which is the trade this project keeps refusing.
///
/// EXACT MATCH ONLY. A prefix over this column is a range scan on an identifier
/// — the same rule the search resolver states as "NEVER prefix-search a hash",
/// for the same reason.
async fn get_asset_identity(
    State(state): State<AppState>,
    Query(q): Query<IdentityQuery>,
) -> Response {
    let reps = match state.assets.representations(&q.key).await {
        Ok(r) => r,
        Err(e) => return read_failure(e),
    };

    // DECIMALS ARE THE ADDABILITY TEST, and it is checked here rather than
    // assumed. Two representations of one asset should agree on decimals — they
    // are the same asset, moved by XCM, so one unit on either side is one unit.
    // A disagreement means either a registry error or that the absolute name has
    // collided two different things, and in both cases summing them would
    // produce a number that is wrong by a power of ten.
    let addable = shared_decimals(reps.iter().map(|(_, a)| a.decimals)).is_some();

    Json(serde_json::json!({
        "absolute_key": q.key,
        "absolute_location": reps.first().and_then(|(_, a)| a.absolute_location.clone()),
        "representation_count": reps.len(),
        // deduped, like the consolidated endpoint's field of the same name —
        // two representations on ONE chain (a foreign row and a pool row) would
        // otherwise print that chain twice under a key its sibling dedups
        "chains": reps.iter().map(|(c, _)| c.clone())
            .collect::<std::collections::BTreeSet<_>>(),
        "representations": reps
            .iter()
            .map(|(chain, a)| serde_json::json!({
                "chain": chain,
                "asset_key": a.asset_key,
                "representation_kind": a.representation_kind,
                "asset_type": a.asset_type,
                "symbol": a.symbol,
                "name": a.name,
                "decimals": a.decimals,
                "supply": a.supply,
                "status": a.status,
                // the SELF-RELATIVE name, kept beside the absolute one on
                // purpose: these two differing is the entire finding, and a
                // reader who only ever sees the absolute key would have to take
                // the normalizer on trust
                "location_key": a.location_key,
            }))
            .collect::<Vec<_>>(),
        "addable": addable,
        "reads_as": if reps.is_empty() {
            "no asset dotlens has indexed carries this absolute name. That is not \
             the same as the asset not existing — it may be on a chain that is \
             not registered, or on one whose sync-assets has not run".to_string()
        } else if reps.len() == 1 {
            "one representation. Either this asset exists on one chain only, or \
             the others are on chains dotlens does not index".to_string()
        } else if addable {
            format!(
                "{} representations of ONE asset across {} chains, all agreeing \
                 on decimals — so their quantities are directly addable",
                reps.len(),
                reps.iter().map(|(c, _)| c).collect::<std::collections::BTreeSet<_>>().len()
            )
        } else {
            "these representations do NOT agree on decimals (or some are \
             unknown), so their quantities must NOT be added. Reported as \
             separate lines rather than summed into a wrong number".to_string()
        },
    }))
    .into_response()
}

/// THE CROSS-CHAIN TREASURY POSITION — one logical asset at a time, summed
/// across every chain that holds it, with every contributing number's provenance
/// still attached.
///
/// This is what the asset-identity work in slice 6 was FOR. Before it, a
/// treasury page could list Asset Hub's USDT and Hydration's USDT and had no
/// honest way to say they were the same thing: their `location_key`s genuinely
/// differ, because a Location is relative to its observer. `absolute_key` is
/// what makes them one row here.
///
/// FOUR RULES, each of which is a refusal:
///   1. **No stored table.** Computed per request from anchors + deltas +
///      core.assets, all of which carry lineage. See 0020.
///   2. **No prices, no cross-asset total.** Each logical asset sums in its OWN
///      units. Adding DOT to USDT needs a price, and a price with no source,
///      timestamp and method is how a treasury dashboard starts lying.
///   3. **Decimals must agree or the line is not summed.** Two representations
///      of one asset are the same asset moved by XCM, so a disagreement means a
///      registry error or an absolute-name collision — either way, summing is
///      wrong by a power of ten.
///   4. **Everything that cannot be consolidated is COUNTED**, not dropped. A
///      holding whose asset has no absolute name is real money; it appears as an
///      unconsolidated line with the reason attached.
async fn get_treasury_consolidated(
    State(state): State<AppState>,
    Path(network): Path<String>,
) -> Response {
    // An unknown network must NOT render as "this treasury holds nothing" —
    // that is a claim, and a wrong one. Every other surface refuses an unknown
    // subject (search 400s an unknown chain, the referendum endpoint 404s an
    // unindexed id) and this is the surface where a confident empty answer would
    // be read as a fact about the money.
    let networks = crate::search::known_networks(&state.registry);
    if !networks.contains(&network) {
        return error(
            StatusCode::NOT_FOUND,
            format!("unknown network '{network}' — registered networks: {networks:?}"),
        );
    }
    let accounts = match state.treasury.accounts(&network).await {
        Ok(a) => a,
        Err(e) => return read_failure(e),
    };

    let mut chains: Vec<String> = Vec::new();
    for a in &accounts {
        if !chains.contains(&a.chain_id) {
            chains.push(a.chain_id.clone());
        }
    }

    /// One chain's contribution to one logical asset.
    ///
    /// **`amount` IS AN OPTION, AND THAT IS THE FIX FOR THE WORST DEFECT THIS
    /// SLICE COULD HAVE SHIPPED.** The first draft classified an unanchored
    /// holding as "unconsolidated" and dropped it BEFORE grouping — so a chain
    /// whose `treasury-holdings` sweep had not run contributed nothing, and the
    /// position rendered a clean `total` across the remaining chains with no
    /// hint that a leg was missing. That is not a gap, it is a wrong number that
    /// reads like a fact and sums like a fact, and Hydration is in exactly that
    /// state today (slice 6 verified `treasury-holdings hydration` reports 0
    /// accounts). A leg with a known asset and an unknown balance now JOINS its
    /// group and suppresses the group's total.
    struct Leg {
        chain: String,
        asset_key: String,
        account_label: String,
        account_ss58: Option<String>,
        account_role: String,
        account_derivation: Option<String>,
        account_source: String,
        /// None = this pair has been seen MOVING but never read from state.
        amount: Option<i128>,
        decimals: Option<u32>,
        symbol: Option<String>,
        basis: &'static str,
        anchor_height: Option<u64>,
        anchor_spec_version: Option<u64>,
        anchor_source: Option<String>,
        frozen_at_anchor: Option<String>,
        asset_status: Option<String>,
        reason_unknown: Option<&'static str>,
    }

    let mut by_absolute: std::collections::BTreeMap<String, Vec<Leg>> =
        std::collections::BTreeMap::new();
    let mut unconsolidated: Vec<serde_json::Value> = Vec::new();
    let mut unanchored = 0usize;
    let mut erc20_unanchorable = 0usize;
    let mut zero_positions = 0usize;
    let mut account_addresses = 0usize;

    for chain in &chains {
        let mut seen: Vec<&TreasuryAccountRow> = Vec::new();
        for a in accounts.iter().filter(|a| &a.chain_id == chain) {
            if !seen.iter().any(|s| s.account_id == a.account_id) {
                seen.push(a);
            }
        }
        let ids: Vec<Vec<u8>> = seen.iter().map(|a| a.account_id.clone()).collect();
        account_addresses += seen.len();

        let holdings = match state.balances.holdings(chain, &ids, None).await {
            Ok(h) => h,
            Err(e) => return read_failure(e),
        };
        let assets = match state.assets.assets(chain).await {
            Ok(a) => a,
            Err(e) => return read_failure(e),
        };
        let by_key: HashMap<&str, &AssetRow> =
            assets.iter().map(|a| (a.asset_key.as_str(), a)).collect();

        for h in &holdings {
            let Some(account) = seen.iter().find(|a| a.account_id == h.account_id) else {
                continue;
            };
            let asset = by_key.get(h.asset.as_str());
            let amount_str = h.amount();
            let amount = amount_str.as_deref().and_then(|a| a.parse::<i128>().ok());

            // a zero position is not news, but it is COUNTED — "we looked and
            // there is nothing" and "we did not look" must stay distinguishable,
            // which is the rule the holdings sweep's own output states
            if amount == Some(0) {
                zero_positions += 1;
                continue;
            }

            // AN Erc20 ASSET IS UNREADABLE IN PRINCIPLE, and it is classified
            // FIRST for two reasons a reviewer caught. (a) Its balance lives in
            // `pallet_evm` storage, so `treasury-holdings` skips it BY DESIGN —
            // it can never have an anchor, so a counter placed after the
            // anchor check could never fire, and `coverage.erc20_positions`
            // would have read 0 forever while claiming to measure the largest
            // uncovered position dotlens knows about. (b) The generic
            // "run treasury-holdings to anchor it" advice is guaranteed to do
            // nothing here, and a remedy that cannot work is worse than none.
            if asset.and_then(|a| a.asset_type.as_deref()) == Some("Erc20") {
                erc20_unanchorable += 1;
                unconsolidated.push(serde_json::json!({
                    "chain": chain,
                    "asset_key": h.asset,
                    "absolute_key": asset.and_then(|a| a.absolute_key.clone()),
                    "symbol": asset.and_then(|a| a.symbol.clone()),
                    "account": account.label,
                    "amount": null,
                    // THE EVENT-STREAM TOTAL, which is `delta_sum` and NOT
                    // `amount()`. `amount()` is anchor + deltas and returns None
                    // with no anchor — and an Erc20 can never HAVE an anchor, so
                    // reading it here made `movement_only` structurally null
                    // forever, one field over from the counter that was a
                    // blocker for exactly that reason. The `not_covered` line
                    // promises "any figure shown beside one is `movement_only`,
                    // a total from the event stream"; this is the figure that
                    // makes the promise keepable.
                    "movement_only": h.delta_sum,
                    "movement_event_count": h.delta_count,
                    "reason": "this asset is declared Erc20: its balance lives in \
                               `pallet_evm` storage, not in any pallet dotlens \
                               reads, so NO command will ever anchor it. Any \
                               figure beside it is a movement total from the event \
                               stream, never a position",
                }));
                continue;
            }

            // THE HOLDING WHOSE ASSET HAS NO ABSOLUTE NAME. Real money that
            // cannot be added to anything, because nothing on another chain can
            // be recognised as the same asset. Two very different causes, so two
            // different reasons rather than one generic line.
            let Some(abs) = asset.and_then(|a| a.absolute_key.clone()) else {
                if amount.is_none() {
                    unanchored += 1;
                }
                unconsolidated.push(serde_json::json!({
                    "chain": chain,
                    "asset_key": h.asset,
                    "absolute_key": null,
                    "symbol": asset.and_then(|a| a.symbol.clone()),
                    "account": account.label,
                    "account_derivation": account.derivation,
                    "amount": amount.map(|a| a.to_string()),
                    "basis": h.basis(),
                    "provenance": {
                        "anchor_height": h.anchor_height,
                        "anchor_spec_version": h.anchor_spec_version,
                        "anchor_source": h.anchor_source,
                    },
                    "reason": if asset.is_none() {
                        "this asset is not in core.assets on this chain — run \
                         sync-assets"
                    } else {
                        "this asset has no absolute (observer-free) name, so no \
                         representation on another chain can be recognised as the \
                         same asset. Usually correct and permanent: a chain-local \
                         construct (XYK/StableSwap share, Bond) has no XCM \
                         location at all. It can also mean the chain's seed omits \
                         `native_token` — see coverage.chains_without_native_token"
                    },
                }));
                continue;
            };

            // AND HERE IS THE ONE THAT USED TO BE DROPPED. A named asset with an
            // unknown balance JOINS its group and suppresses the group's total,
            // rather than quietly leaving a smaller total looking whole.
            if amount.is_none() {
                unanchored += 1;
            }
            by_absolute.entry(abs).or_default().push(Leg {
                chain: chain.clone(),
                asset_key: h.asset.clone(),
                account_label: account.label.clone(),
                account_ss58: account.ss58.clone(),
                account_role: account.role.clone(),
                account_derivation: account.derivation.clone(),
                account_source: account.source.clone(),
                amount,
                decimals: asset.and_then(|a| a.decimals),
                symbol: asset.and_then(|a| a.symbol.clone()),
                basis: h.basis(),
                anchor_height: h.anchor_height,
                anchor_spec_version: h.anchor_spec_version,
                anchor_source: h.anchor_source.clone(),
                frozen_at_anchor: h.anchor_frozen.clone(),
                asset_status: asset.and_then(|a| a.status.clone()),
                reason_unknown: amount.is_none().then_some(
                    "no anchor: this pair has been seen MOVING but never read \
                     from state. Run treasury-holdings on this chain",
                ),
            });
        }
    }

    // A CHAIN WHOSE SEED OMITS `native_token` gets a NULL absolute name for its
    // native currency (0019), which means its DOT — or whatever it holds —
    // silently fails to join anything here. That was a stated gap with nothing
    // enforcing it; this endpoint is the first reader that would under-report
    // because of it, so it is the one that names it.
    // SCOPED TO EVERY REGISTERED CHAIN, not just the ones with a treasury
    // account — a reviewer pointed out that a seed omitting `native_token` is
    // most likely on a chain whose account is not registered YET, which is
    // exactly when this check would have stayed empty and read as "all clear".
    // And a chain the registry does not know at all counts as missing, not as
    // fine: `is_none_or` is the honest polarity.
    let chains_without_native_token: Vec<&str> = state
        .registry
        .chains()
        .filter(|c| c.native_token.is_none())
        .map(|c| c.id.as_str())
        .collect();

    let mut positions = Vec::with_capacity(by_absolute.len());
    let mut not_addable = 0usize;
    let mut incomplete = 0usize;
    for (absolute_key, legs) in &by_absolute {
        // ONE implementation of the addability rule, shared with the identity
        // endpoint. Two implementations of "are these the same unit" that could
        // disagree is the defect class that made `api` depend on `sim` in slice
        // 5 rather than re-implement thirty lines of set arithmetic — and the
        // two drafts here HAD already diverged on the empty case (`<= 1` vs
        // `== 1`), which a reviewer caught.
        let decimals = shared_decimals(legs.iter().map(|l| l.decimals));
        let addable = decimals.is_some();
        if !addable {
            not_addable += 1;
        }
        // A GROUP WITH A LEG OF UNKNOWN SIZE HAS NO TOTAL. Summing the known
        // legs would render a smaller number that looks complete, which is
        // precisely the failure this endpoint exists to end.
        let complete = legs.iter().all(|l| l.amount.is_some());
        if !complete {
            incomplete += 1;
        }
        let summable = addable && complete;
        let total: i128 = legs.iter().filter_map(|l| l.amount).sum();
        let decimals = if addable { decimals } else { None };
        // the symbol is a LABEL, not the identity — representations may spell it
        // differently ("USDt" vs "USDT"), so take the first and keep every
        // spelling visible on the legs
        let symbol = legs.iter().find_map(|l| l.symbol.clone());

        positions.push(serde_json::json!({
            "absolute_key": absolute_key,
            "symbol": symbol,
            "decimals": decimals,
            // THE SUM IS OMITTED, not zeroed, when the legs disagree on units OR
            // when any leg's size is unknown. A null total beside a populated
            // leg list is unmistakable; a wrong number is not.
            "total": summable.then(|| total.to_string()),
            "display": summable
                .then(|| decimals.and_then(|d| format_units(&total.to_string(), d)))
                .flatten(),
            "addable": addable,
            "complete": complete,
            "legs_of_unknown_size": legs.iter().filter(|l| l.amount.is_none()).count(),
            "chains": legs.iter().map(|l| l.chain.clone())
                .collect::<std::collections::BTreeSet<_>>(),
            "legs": legs.iter().map(|l| serde_json::json!({
                "chain": l.chain,
                "asset_key": l.asset_key,
                "symbol": l.symbol,
                "decimals": l.decimals,
                "amount": l.amount.map(|a| a.to_string()),
                "display": l.amount.zip(l.decimals)
                    .and_then(|(a, d)| format_units(&a.to_string(), d)),
                "unknown_because": l.reason_unknown,
                // ROADMAP criterion 2 is "every account holding network-treasury
                // money is listed WITH its derivation", and this is the surface
                // people will actually read — so the derivation travels with the
                // leg rather than only on the holdings page
                "account": l.account_label,
                "account_ss58": l.account_ss58,
                "account_role": l.account_role,
                "account_derivation": l.account_derivation,
                "account_source": l.account_source,
                "asset_status": l.asset_status,
                // THE LOCKED PORTION, surfaced for the first time — and named
                // `frozen_at_anchor` rather than `frozen` on purpose. It is read
                // from the ANCHOR while `amount` is anchor + deltas, so the two
                // are as-of different heights; and it is NOT subtracted from any
                // total (`balance_anchors.total` is free + reserved). A reader
                // who saw a bare `frozen` beside `amount` would subtract it.
                "frozen_at_anchor": l.frozen_at_anchor,
                "basis": l.basis,
                "provenance": {
                    "anchor_height": l.anchor_height,
                    "anchor_spec_version": l.anchor_spec_version,
                    "anchor_source": l.anchor_source,
                },
            })).collect::<Vec<_>>(),
            "identity": format!("/v1/assets/identity?key={}",
                                urlencode(absolute_key)),
        }));
    }

    // biggest first is meaningless across assets with different decimals and no
    // prices, so order by the one thing that IS comparable: how many chains a
    // position spans, then its name. Deterministic, and it puts the genuinely
    // cross-chain positions — the ones this endpoint exists for — at the top.
    positions.sort_by(|a, b| {
        let span = |v: &serde_json::Value| v["chains"].as_array().map(|c| c.len()).unwrap_or(0);
        span(b)
            .cmp(&span(a))
            .then_with(|| a["absolute_key"].as_str().cmp(&b["absolute_key"].as_str()))
    });

    let cross_chain = positions
        .iter()
        .filter(|p| p["chains"].as_array().map(|c| c.len()).unwrap_or(0) > 1)
        .count();

    Json(serde_json::json!({
        "network": network,
        "positions": positions,
        "unconsolidated": unconsolidated,
        "coverage": {
            "chains": chains,
            // ADDRESSES, not `treasury_accounts` rows: the table is keyed
            // (chain, account, role), so an address that is both a derived pot
            // and a registry seed has two rows and is one account. On a surface
            // whose contract is "counted, never estimated", the row count would
            // inflate it.
            "accounts": account_addresses,
            "logical_assets": by_absolute.len(),
            "spanning_more_than_one_chain": cross_chain,
            "not_addable_decimals_disagree": not_addable,
            "positions_with_a_leg_of_unknown_size": incomplete,
            "unconsolidated_positions": unconsolidated.len(),
            "positions_without_an_anchor": unanchored,
            "zero_positions": zero_positions,
            "erc20_positions": erc20_unanchorable,
            "chains_without_native_token": chains_without_native_token,
            "valuation": "none — quantities only, each logical asset in its OWN \
                          units. There is deliberately no cross-asset total: \
                          adding DOT to USDT needs a price, and a price with no \
                          source, timestamp and method is how a treasury \
                          dashboard starts lying quietly (migration 0010's rule, \
                          which consolidation makes more important, not less)",
            "not_covered": consolidation_not_covered(),
        },
        "note": "one row per LOGICAL asset, summed across chains by its \
                 observer-free name. Every leg keeps the block it was anchored \
                 at, the runtime it was decoded against, and which command read \
                 it — the sum is re-derivable from the legs, and the legs are \
                 re-derivable from the chain.",
    }))
    .into_response()
}

/// The ADDABILITY RULE, in one place.
///
/// `Some(d)` only when every representation states its decimals AND they all
/// state the same one — which is what makes two quantities of "the same asset"
/// literally addable. Empty is NOT addable: a claim of addability about nothing
/// is how `/v1/assets/identity` on an unknown key reported `"addable": true`
/// beside `"representation_count": 0` in the first draft.
///
/// Shared by both readers deliberately. Two implementations of "are these the
/// same unit" that could disagree is the defect class that made `api` depend on
/// `sim` in slice 5 — and these two HAD already diverged (`<= 1` vs `== 1`)
/// before a reviewer caught it.
fn shared_decimals(decimals: impl Iterator<Item = Option<u32>>) -> Option<u32> {
    let mut seen: Option<u32> = None;
    let mut any = false;
    for d in decimals {
        any = true;
        match (d, seen) {
            (None, _) => return None,
            (Some(d), None) => seen = Some(d),
            (Some(d), Some(prev)) if d == prev => {}
            (Some(_), Some(_)) => return None,
        }
    }
    if any {
        seen
    } else {
        None
    }
}

/// Minimal percent-encoding for the one place a canonical key is put into a URL.
/// Not a general-purpose encoder — it escapes everything outside the unreserved
/// set, which is correct if verbose for a JSON array.
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 3);
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// The chains-we-cannot-fully-see line, DERIVED from the account list rather
/// than written down — because what is true depends on which accounts happen to
/// be registered, and a hard-coded sentence about that goes stale silently.
fn hydration_coverage_line(accounts: &[TreasuryAccountRow]) -> String {
    let derived_elsewhere = accounts
        .iter()
        .any(|a| a.chain_id != "polkadot" && a.derivation.is_none());
    let mut line = String::from(
        "positions on chains dotlens has not registered (ECOSYSTEM §6 puts treasury assets across 7+ chains). Where a chain IS registered, its money is only as visible as its ACCOUNTS: a treasury position held at a location-derived (HashedDescription) sovereign address is not derivable by dotlens, so it appears here only if a reviewed registry seed names it",
    );
    if derived_elsewhere {
        line.push_str(
            ". Some accounts above are seeded rather than derived, and carry a null `derivation` to say so",
        );
    }
    line.push_str(
        ". A money-market position is additionally unreadable in principle: those are Erc20 assets whose balances live in pallet_evm storage",
    );
    line
}

/// What a consolidated position does NOT include.
///
/// SEPARATE FROM the holdings endpoint's list on purpose, and the project has
/// twice shipped a shared `not_covered` helper that was false on its second
/// consumer (slices 3 and 4, then again in slice 5). The two surfaces have
/// genuinely different gaps: holdings is per account and per chain, so
/// "positions on unregistered chains" is its blind spot; consolidation is per
/// logical asset, so ITS blind spot is anything that cannot be NAMED across
/// chains — a different set, with a different fix.
fn consolidation_not_covered() -> Vec<&'static str> {
    vec![
        "assets with no absolute (observer-free) name cannot be consolidated and \
         are listed under `unconsolidated` instead of being dropped. Usually this \
         is correct and permanent — a chain-local construct (XYK share, \
         StableSwap share, Bond) has no XCM location at all, and on Hydration \
         that is 756 of 1,437 registered assets",
        "Erc20 assets are named and located here and their BALANCES are not \
         readable at all: they live in `pallet_evm` storage, not in a pallet this \
         indexer reads, so NO command will ever anchor one — `treasury-holdings` \
         skips them by design and says so. `erc20_positions` counts them and they \
         are listed under `unconsolidated` with a null amount; any figure shown \
         beside one is `movement_only`, a total from the event stream, never a \
         position",
        "an account that holds treasury money but is not REGISTERED contributes \
         nothing here and is not counted, because an unregistered account is one \
         we do not know to look for. The known instance is the Polkadot \
         treasury's position on Hydration, which sits at a location-derived \
         (HashedDescription) sovereign address dotlens does not derive — an \
         account listed for that chain came from a reviewed registry seed, not \
         from a derivation",
        "`frozen_at_anchor` is read from the ANCHOR while `amount` is anchor + \
         deltas, so the two are as-of different heights; it is NOT subtracted \
         from any leg or total (an anchor's total is free + reserved), and null \
         means the runtime did not expose it, never zero",
        "every number here is the ANCHOR + DELTAS reconstruction. The independent \
         state re-read that would make it agree two ways — ROADMAP's amended \
         Phase 3 criterion 1 — is what `treasury-holdings` does at a given \
         height; this endpoint does not run it, so a disagreement between the \
         two would not show up here",
        "no price is consulted, so there is no total across assets and no USD \
         figure. Each logical asset sums in its own units only",
        "a sum is only as current as its least current leg: legs are anchored at \
         DIFFERENT blocks (each chain has its own height), so a consolidated \
         total is 'as of the anchors named on the legs', not as of one instant. \
         Re-run treasury-holdings on every chain to tighten it",
        "two representations that disagree on decimals are NOT summed — the \
         position is reported with a null total and every leg intact, because \
         summing them would be wrong by a power of ten",
    ]
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

/// Render a read failure, distinguishing a REFUSAL from a FAULT.
///
/// A query cancelled by the serving `statement_timeout` is not the database
/// being broken — the data is fine and the question was too expensive. Those are
/// different facts and they ask the caller for different things: **narrow the
/// window** versus **the answer is unavailable, try later**. Collapsing both
/// into one opaque 500 leaves the caller unable to tell which of the two actions
/// is theirs to take, and a monitor unable to tell load from breakage.
///
/// `503` rather than `504`: nothing upstream timed out, WE declined to spend
/// more of a shared resource on one question. That is a capacity refusal, which
/// is what 503 means.
///
/// Generic over `Display` rather than taking `IndexError`, because these call
/// sites bind whatever their match arm produced and a signature that only
/// accepted one error type would push the others back to a hand-written 500 —
/// which is how a rule ends up true in forty-seven places and false in two.
///
/// It matches with `contains` and not `starts_with`, and that is not
/// interchangeable here: `IndexError`'s `Display` prefixes `"block index
/// error: "`, so the marker is never at position zero by the time it reaches
/// this function. `IndexError::timed_out` inspects the inner string directly and
/// can use `starts_with`; this one cannot. Both are pinned by tests.
fn read_failure<E: std::fmt::Display>(e: E) -> Response {
    let message = e.to_string();
    if message.contains(TIMEOUT_REFUSAL) {
        error(StatusCode::SERVICE_UNAVAILABLE, message)
    } else {
        error(StatusCode::INTERNAL_SERVER_ERROR, message)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use std::path::Path as FsPath;
    use tower::util::ServiceExt;

    /// An `IndexError` shaped exactly as `From<sqlx::Error>` builds one for
    /// SQLSTATE 57014. The MESSAGE is not what these tests pin — that mapping
    /// needs a real cancelled query and lives in the pg integration test. What
    /// they pin is that the classification downstream reads the marker the
    /// producer writes, using the shared constant on both sides rather than two
    /// hand-written strings that could drift apart.
    fn timeout_error() -> IndexError {
        IndexError(format!("{TIMEOUT_REFUSAL}: the window asked for is too expensive"))
    }

    #[test]
    fn a_serving_timeout_is_a_refusal_and_everything_else_is_a_fault() {
        assert_eq!(
            read_failure(timeout_error()).status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "a query we declined to keep spending on is a capacity refusal, not a fault"
        );
        assert_eq!(
            read_failure(IndexError("connection reset by peer".into())).status(),
            StatusCode::INTERNAL_SERVER_ERROR,
            "an actual database fault must NOT be dressed up as a refusal — that would \
             tell a caller to narrow a window that was never the problem"
        );
    }

    #[test]
    fn the_marker_is_not_at_position_zero_by_the_time_it_reaches_read_failure() {
        // THE ONE THAT WOULD SHIP SILENTLY. `IndexError`'s Display prefixes
        // "block index error: ", so a `starts_with` in `read_failure` would
        // classify every timeout as a fault and the refusal would never be
        // reachable — a gate nobody has checked, in the shape where it still
        // compiles and still returns a plausible status code.
        let rendered = timeout_error().to_string();
        assert!(
            !rendered.starts_with(TIMEOUT_REFUSAL),
            "if this ever becomes true, read_failure's `contains` can be tightened; \
             until then `starts_with` there is a silent bug: {rendered}"
        );
        assert!(rendered.contains(TIMEOUT_REFUSAL));
        // ...while the inner string DOES start with it, which is what
        // `timed_out` inspects. The two predicates look interchangeable and are
        // not.
        assert!(timeout_error().timed_out());
        assert!(!IndexError("connection reset by peer".into()).timed_out());
    }

    /// A `VersionedLocation` and a `VersionedXcm` in the shapes this decoder
    /// really produces — the attribution below compares them by exact rendering,
    /// so hand-writing two different shapes on the two sides would make the
    /// difference pass for the wrong reason.
    fn sim_destination(para: u32) -> serde_json::Value {
        serde_json::json!({"V4": [{"parents": 1, "interior":
            {"X1": [[{"Parachain": [para]}]]}}]})
    }
    fn sim_msg(n: u32) -> serde_json::Value {
        serde_json::json!({"V4": [[[{"Transact": {"call": {"encoded": [n]}}}]]]})
    }

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
                frozen: None,
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
            xcm_version: Some(4),
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
            // TWO messages to one destination, of which ONE was already in
            // flight — the relay's shape in miniature, so the difference below
            // is a real subtraction rather than a copy.
            forwarded_xcms: Some(serde_json::json!([
                {"destination": sim_destination(2034), "messages": [sim_msg(1), sim_msg(9)]}
            ])),
            effects: serde_json::json!({"Ok": [{"emitted_events": []}]}),
            note: None,
            spec_version: 2_003_002,
            api_version: Some(2),
            metadata_version: 15,
            sim_version: 2,
            raw_location: "raw/polkadot-asset-hub/sim/cd/01/\
                           DryRunApi_dry_run_call.response.scale"
                .into(),
            observed_at: None,
            baseline_input_hash: Some(format!("0x{}", "02".repeat(32))),
            overrides: None,
            override_hash: None,
            storage_diff: None,
            storage_diff_count: None,
            diff_status: None,
            built_block_hash: None,
            harness: None,
            // A dry_run row has no route and no anchor: it dispatches through a
            // runtime API, not through the scheduler.
            dispatch_route: None,
            agenda_anchor: None,
        });
        // THE BASELINE: an ordinary recorded row, which is the whole design —
        // `system.remark()` at the same state, whose forwarded list is the
        // ambient queue. It is its own baseline.
        sim.insert(SimulationRow {
            chain_id: "polkadot-asset-hub".into(),
            at_height: 19_000_500,
            at_block_hash: format!("0x{}", "cd".repeat(32)),
            input_hash: format!("0x{}", "02".repeat(32)),
            tier: "dry_run".into(),
            call_hash: format!("0x{}", "ba".repeat(32)),
            call_summary: Some("system.remark".into()),
            origin_spec: "root".into(),
            origin: serde_json::json!({"resolved": "system:Root"}),
            xcm_version: Some(4),
            status: "executed".into(),
            dispatch_ok: Some(true),
            dispatch_error: None,
            emitted_events: serde_json::json!([]),
            event_count: 0,
            local_xcm: None,
            forwarded_xcms: Some(serde_json::json!([
                {"destination": sim_destination(2034), "messages": [sim_msg(1)]}
            ])),
            effects: serde_json::json!({"Ok": []}),
            note: None,
            spec_version: 2_003_002,
            api_version: Some(2),
            metadata_version: 15,
            sim_version: 2,
            raw_location: "raw/polkadot-asset-hub/sim/cd/02/\
                           DryRunApi_dry_run_call.response.scale"
                .into(),
            observed_at: None,
            baseline_input_hash: Some(format!("0x{}", "02".repeat(32))),
            overrides: None,
            override_hash: None,
            storage_diff: None,
            storage_diff_count: None,
            diff_status: None,
            built_block_hash: None,
            harness: None,
            // A dry_run row has no route and no anchor: it dispatches through a
            // runtime API, not through the scheduler.
            dispatch_route: None,
            agenda_anchor: None,
        });
        // A THIRD row with NO baseline at all — every row recorded before slice
        // 5 looks like this, and the endpoint must say so rather than let the
        // raw list read as attributed.
        sim.insert(SimulationRow {
            chain_id: "polkadot-asset-hub".into(),
            at_height: 19_000_400,
            at_block_hash: format!("0x{}", "ce".repeat(32)),
            input_hash: format!("0x{}", "03".repeat(32)),
            tier: "dry_run".into(),
            call_hash: format!("0x{}", "cc".repeat(32)),
            call_summary: Some("system.remark".into()),
            origin_spec: "root".into(),
            origin: serde_json::json!({"resolved": "system:Root"}),
            xcm_version: Some(4),
            status: "executed".into(),
            dispatch_ok: Some(true),
            dispatch_error: None,
            emitted_events: serde_json::json!([]),
            event_count: 0,
            local_xcm: None,
            forwarded_xcms: Some(serde_json::json!([
                {"destination": sim_destination(2040), "messages": [sim_msg(5)]}
            ])),
            effects: serde_json::json!({"Ok": []}),
            note: None,
            spec_version: 2_003_002,
            api_version: Some(2),
            metadata_version: 15,
            sim_version: 1,
            raw_location: "raw/polkadot-asset-hub/sim/ce/03/\
                           DryRunApi_dry_run_call.response.scale"
                .into(),
            observed_at: None,
            baseline_input_hash: None,
            overrides: None,
            override_hash: None,
            storage_diff: None,
            storage_diff_count: None,
            diff_status: None,
            built_block_hash: None,
            harness: None,
            // A dry_run row has no route and no anchor: it dispatches through a
            // runtime API, not through the scheduler.
            dispatch_route: None,
            agenda_anchor: None,
        });

        // ---------------------------------------------------------------------
        // TWO FORK ROWS, ONE PER ROUTE — and until slice 10 there were NONE.
        //
        // Slice 8 shipped this tier and its own verification recorded that
        // "nothing in crates/api or pg_integration tests a fork row at all, so
        // every claim about how a fork row RENDERS rests on this drill and on
        // nothing offline". Slice 9 promised two route tests and wrote zero.
        // These are them.
        //
        // THEY CARRY THEIR OWN CALL HASH, deliberately. Adding a row under the
        // existing `ab…` hash would have lengthened the list two shipped tests
        // assert the length of — which is exactly how slice 2's registration of
        // Hydration silently satisfied a test's precondition and moved its
        // subject, and how slice 7's new fixture account moved `segments[0]`.
        let fork_hash = format!("0x{}", "f0".repeat(32));
        let fork_row = |input: &str, height: u64, route: &str, status: &str| SimulationRow {
            chain_id: "polkadot-asset-hub".into(),
            at_height: height,
            at_block_hash: format!("0x{}", "8e".repeat(32)),
            input_hash: input.into(),
            tier: sim::TIER_FORK.into(),
            call_hash: fork_hash.clone(),
            call_summary: Some("multiassetbounties.fund_bounty".into()),
            origin_spec: if route == sim::ROUTE_SCHEDULED {
                "Origins:MediumSpender".into()
            } else {
                "signed:13UVJyLnbVp9RBZYFwkxtG4qWk5QkHc1Jgu85QifAbobheY9".into()
            },
            origin: if route == sim::ROUTE_SCHEDULED {
                serde_json::json!({"resolved": "Origins:MediumSpender"})
            } else {
                serde_json::json!({"resolved": "system:Signed", "account": "0xd6a3eadc"})
            },
            // A FORK ROW CALLS NEITHER RUNTIME API, so both version columns are
            // NULL. Nullable since 0021 — and reading a NULL as 0 would put "XCM
            // v0" and "DryRunApi v0" on a row that asked neither question.
            xcm_version: None,
            api_version: None,
            status: status.into(),
            dispatch_ok: Some(status == "executed"),
            // A FAILED DISPATCH KEEPS THE ERROR THE CHAIN GAVE. A fixture that
            // paired `dispatch_failed` with no error would teach a shape this
            // tier cannot produce — slice 7's green-test-with-a-wrong-number,
            // one column over.
            dispatch_error: (status != "executed")
                .then(|| serde_json::json!({"raw": {"Token": [{"FundsUnavailable": []}]}})),
            emitted_events: serde_json::json!([
                {"name": "system.NewAccount", "data": {}},
                {"name": "assets.Transferred", "data": {"asset_id": 1984, "amount": "83760000000"}},
                {"name": "multiassetbounties.BountyCreated", "data": {"index": 2}}
            ]),
            event_count: 3,
            local_xcm: None,
            // NULL, NOT `[]` — a fork row has no forwarded list at all, and this
            // is the exact column whose non-null read would have made every fork
            // row fail to load through Pg (slice 8 caught that by compiling, on
            // the one surface the tier exists to serve).
            forwarded_xcms: None,
            effects: serde_json::json!({"diff_method": "dev_dryRun"}),
            note: None,
            spec_version: 2_003_002,
            metadata_version: 15,
            sim_version: 2,
            raw_location: format!(
                "raw/polkadot-asset-hub/sim/8e/{}/chopsticks_fork.response.json",
                input.trim_start_matches("0x")
            ),
            observed_at: None,
            // No baseline: a fork run has no forwarded list to difference.
            baseline_input_hash: None,
            overrides: None,
            override_hash: None,
            storage_diff: Some(serde_json::json!([
                {"key": "0x26aa394e", "readable": "System.Account(0x…)", "change": "changed"}
            ])),
            storage_diff_count: Some(1),
            // WHAT SLICE 9 RECORDED AS `decoded`. The bytes came from
            // `dev_dryRun`, which returns the applied extrinsic's phase and
            // nothing before it.
            diff_status: Some(sim::DIFF_STATUS_EXTRINSIC_ONLY.into()),
            // NULL on the live route: no block is built.
            built_block_hash: None,
            harness: Some(serde_json::json!({
                "tool": "chopsticks",
                "version": "1.5.1",
                "mocked": ["mocked tx pool", "mocked signature host"],
            })),
            dispatch_route: Some(route.into()),
            agenda_anchor: (route == sim::ROUTE_SCHEDULED).then(|| {
                serde_json::json!({
                    "provider": "relay",
                    "at_parent": 32_519_445u64,
                    "written_at": 32_519_445u64,
                    "system_number": 19_368_576u64,
                    "relay_number": 32_519_445u64,
                    "agenda_keys_observed": 35,
                    "decided_by": "agenda key range",
                })
            }),
        };
        // The scheduled row sits HIGHER, so `at_height desc` puts it first and
        // the pair is ordered rather than incidental.
        sim.insert(fork_row(
            &format!("0x{}", "f1".repeat(32)),
            19_368_576,
            sim::ROUTE_SCHEDULED,
            "executed",
        ));
        sim.insert(fork_row(
            &format!("0x{}", "f2".repeat(32)),
            19_368_575,
            sim::ROUTE_DRY_RUN_EXTRINSIC,
            "dispatch_failed",
        ));

        // ONE PREVIEWED ARRIVAL, stitched to the subject above by its source
        // columns: the message the call really queued, previewed on the chain it
        // was addressed to — and REJECTED AT THE BARRIER, which is the outcome
        // the sending chain structurally cannot see.
        let xcm_sim = Arc::new(MemoryXcmSimIndex::new());
        xcm_sim.insert(XcmSimulationRow {
            chain_id: "hydration".into(),
            at_height: 13_663_124,
            at_block_hash: format!("0x{}", "de".repeat(32)),
            input_hash: format!("0x{}", "04".repeat(32)),
            tier: "dry_run".into(),
            program_hash: format!("0x{}", "aa".repeat(32)),
            program: sim_msg(9),
            program_summary: Some("ReserveAssetDeposited → BuyExecution → DepositAsset".into()),
            origin_location: serde_json::json!({"V4": [{"parents": 1, "interior":
                {"X1": [[{"Parachain": [1000]}]]}}]}),
            origin_ref: "para:1000".into(),
            status: "not_started".into(),
            weight_used: None,
            xcm_error: Some(serde_json::json!({"index": 0, "error": {"Barrier": []}})),
            emitted_events: serde_json::json!([]),
            event_count: 0,
            forwarded_xcms: serde_json::json!([]),
            baseline_input_hash: Some(format!("0x{}", "05".repeat(32))),
            effects: serde_json::json!({"Ok": []}),
            note: Some("execution NEVER STARTED (Outcome::Error)".into()),
            source_chain_id: Some("polkadot-asset-hub".into()),
            source_at_block_hash: Some(format!("0x{}", "cd".repeat(32))),
            source_input_hash: Some(format!("0x{}", "01".repeat(32))),
            source_forwarded_index: Some(0),
            source_message_index: Some(1),
            spec_version: 435,
            api_version: 2,
            metadata_version: 15,
            sim_version: 2,
            raw_location: "raw/hydration/sim/de/04/DryRunApi_dry_run_xcm.response.scale".into(),
            observed_at: None,
        });
        // ITS BASELINE: the empty program at the same state, whose forwarded
        // list is Hydration's ambient queue. An ordinary row, its own baseline —
        // and the reason the arrival endpoint can difference anything at all.
        xcm_sim.insert(XcmSimulationRow {
            chain_id: "hydration".into(),
            at_height: 13_663_124,
            at_block_hash: format!("0x{}", "de".repeat(32)),
            input_hash: format!("0x{}", "05".repeat(32)),
            tier: "dry_run".into(),
            program_hash: format!("0x{}", "bb".repeat(32)),
            program: serde_json::json!({"V4": [[[]]]}),
            program_summary: Some("(empty program)".into()),
            origin_location: serde_json::json!({"V4": [{"parents": 1, "interior":
                {"X1": [[{"Parachain": [1000]}]]}}]}),
            origin_ref: "para:1000".into(),
            status: "complete".into(),
            weight_used: Some(serde_json::json!({"ref_time": 0, "proof_size": 0})),
            xcm_error: None,
            emitted_events: serde_json::json!([]),
            event_count: 0,
            forwarded_xcms: serde_json::json!([
                {"destination": sim_destination(2030), "messages": [sim_msg(3)]}
            ]),
            baseline_input_hash: Some(format!("0x{}", "05".repeat(32))),
            effects: serde_json::json!({"Ok": []}),
            note: None,
            source_chain_id: None,
            source_at_block_hash: None,
            source_input_hash: None,
            source_forwarded_index: None,
            source_message_index: None,
            spec_version: 435,
            api_version: 2,
            metadata_version: 15,
            sim_version: 2,
            raw_location: "raw/hydration/sim/de/05/DryRunApi_dry_run_xcm.response.scale".into(),
            observed_at: None,
        });

        // ONE JOURNEY, in the shape live data actually produced (Asset Hub
        // #19581756 → Hydration #13663124): the sending chain emits TWO ids for
        // one message — the router's wire hash first, then pallet-xcm's topic —
        // and the receiving chain reports the TOPIC, under the AMBIGUOUS id kind
        // because messageQueue never says which of the two it is holding.
        // Core occupancy on the relay, shaped so the TWO ratios cannot come out
        // equal by accident: 10 blocks, 10 declared cores, and 3 cores that
        // produce 10 / 6 / 1 blocks. Cores touched = 3/10 = 30%; slots filled =
        // 17/100 = 17%. The three fill levels also land in three different
        // bands, so the DISTRIBUTION is exercised rather than just the totals.
        // If a future change collapses the two ratios into one number, this
        // fixture is what makes the collapse visible.
        let coretime = Arc::new(MemoryCoretimeIndex::new());
        for h in 100..=109u64 {
            coretime.insert_indexed_height("polkadot", h);
            coretime.insert_row("polkadot", h, 0, "included", 0, 2004);
            if h < 106 {
                coretime.insert_row("polkadot", h, 1, "included", 1, 2034);
            }
            if h == 100 {
                coretime.insert_row("polkadot", h, 2, "included", 2, 3344);
            }
            // A `backed` row in the same window, on a core that is otherwise
            // idle. It must NOT reach either ratio — async backing means nearly
            // every inclusion has one, and counting them would roughly double
            // every figure this endpoint serves.
            coretime.insert_row("polkadot", h, 3, "backed", 7, 2004);
        }
        coretime.insert_config(
            "polkadot",
            CoreConfigRow { block_height: 109, num_cores: 10, runtime_version: 2_003_002 },
        );
        let coretime: Arc<dyn CoretimeIndex> = coretime;

        // THE ENTITLEMENT HALF FOR THE SAME TEN BLOCKS, shaped so that every
        // number the delta serves is DIFFERENT from every number the occupancy
        // endpoint serves over the identical window — which is the whole product
        // claim, and a fixture where they coincided would let a collapse of the
        // two pass every assertion.
        //
        // 10 declared cores: core 0 task 2004 (fully used), core 1 task 2034
        // (used 6 of 10), core 2 task 3344 (used 1 of 10), core 3 task 3388
        // (ENTITLED AND IDLE — the waste), cores 4..9 pool (idle, and NOT
        // waste).
        //
        // The entitled denominator is 4 cores x 10 blocks = 40 slots against
        // occupancy's 100, so the delta reads 42.5% where the occupancy
        // endpoint reads 17% over the IDENTICAL window. Core 7 carries a
        // `backed` row and no inclusion, so it must also prove the delta counts
        // inclusions only.
        let broker = Arc::new(MemoryBrokerIndex::new());

        // ------------------------------------------------- HRMP channel graph
        // Four readings on ONE chain, built so the interesting cases cannot be
        // reached by accident:
        //   session 10 (#100)   — 1000->2034 absent
        //   session 11 (#2500)  — 1000->2034 REQUESTED
        //   session 12 (#4900)  — 1000->2034 OPEN
        //   session 20 (#30000) — 1000->2034 absent again, and EIGHT sessions
        //                         later, so the close is NOT exactly dated and
        //                         the coverage list has something real in it.
        // 2034->1000 is open throughout, which is what makes the directional
        // assertions bite: a reader that folded the pair would report the
        // reverse edge's history for the forward one.
        let channels = Arc::new(MemoryChannelIndex::new());
        let reading = |height: u64, session: u64, open: u32, requested: u32| ChannelReadingRow {
            block_height: height,
            session_index: session,
            channel_count: open,
            open_request_count: requested,
            topology_digest: format!("0x{:064x}", height),
            spec_version: 2003002,
            source: "channels-range".into(),
        };
        let edge = |sender: u32, recipient: u32, state: &str| ChannelEdgeRow {
            sender,
            recipient,
            state: state.into(),
            max_capacity: 1000,
            max_total_size: 102400,
            max_message_size: 102400,
            sender_deposit: "100000000000".into(),
            recipient_deposit: if state == "open" {
                Some("100000000000".into())
            } else {
                None
            },
            confirmed: if state == "open" { None } else { Some(true) },
        };
        channels.insert_reading("polkadot", reading(100, 10, 1, 0));
        channels.insert_edge("polkadot", 100, edge(2034, 1000, "open"));
        channels.insert_reading("polkadot", reading(2500, 11, 1, 1));
        channels.insert_edge("polkadot", 2500, edge(2034, 1000, "open"));
        channels.insert_edge("polkadot", 2500, edge(1000, 2034, "requested"));
        channels.insert_reading("polkadot", reading(4900, 12, 2, 0));
        channels.insert_edge("polkadot", 4900, edge(2034, 1000, "open"));
        channels.insert_edge("polkadot", 4900, edge(1000, 2034, "open"));
        channels.insert_reading("polkadot", reading(30000, 20, 1, 0));
        channels.insert_edge("polkadot", 30000, edge(2034, 1000, "open"));
        let assign = |core: u32, kind: &str, task: Option<u32>| EntitlementRow {
            core_index: core,
            assignment_index: 0,
            // BELOW the window (100..=109), which is the ordinary case: a sale
            // boundary sits far from the window it governs.
            relay_block: 80,
            kind: kind.into(),
            task_id: task,
            parts: PARTS_WHOLE_CORE,
            runtime_version: 2_003_002,
            mapper_version: 1,
        };
        for (i, (core, kind, task)) in [
            (0u32, "task", Some(2004u32)),
            (1, "task", Some(2034)),
            (2, "task", Some(3344)),
            (3, "task", Some(3388)),
            (4, "pool", None),
            (5, "pool", None),
            (6, "pool", None),
            (7, "pool", None),
            (8, "pool", None),
            (9, "pool", None),
        ]
        .into_iter()
        .enumerate()
        {
            broker.insert_assignment("polkadot-coretime", 7, i as u32, assign(core, kind, task));
        }
        broker.insert_config(
            "polkadot-coretime",
            BrokerConfigRow {
                block_height: 4_927_655,
                core_count: 10,
                // Cores 0 and 1 are RESERVED; 2 upward are the bulk market. So
                // the one idle entitled core (3) is market-side, which is the
                // shape slice 13 measured at scale (all ten idle cores >= 11).
                first_core: Some(2),
                runtime_version: 2_003_002,
            },
        );
        broker.insert_event(
            "polkadot-coretime",
            BrokerEventRow {
                block_height: 7,
                event_index: 0,
                variant: "CoreAssigned".into(),
                core_index: Some(0),
                task_id: None,
                data: serde_json::json!({
                    "core": 0, "when": 80, "assignment": [[{"Task": [2004]}, 57600]]
                }),
                runtime_version: 2_003_002,
                mapper_version: 1,
            },
        );
        // A renewal that MOVED a core index, so the two timeline subjects
        // genuinely diverge: task 2004 appears on an event whose core is 9.
        broker.insert_event(
            "polkadot-coretime",
            BrokerEventRow {
                block_height: 9,
                event_index: 0,
                variant: "AutoRenewalEnabled".into(),
                core_index: Some(9),
                task_id: Some(2004),
                data: serde_json::json!({ "core": 9, "task": 2004 }),
                runtime_version: 2_003_002,
                mapper_version: 1,
            },
        );
        let broker: Arc<dyn BrokerIndex> = broker;

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
            mapper_version: 2,
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
        // A SNOWBRIDGE EXPORT (Phase 3, slice 4), in the shape of live Asset Hub
        // #19407624 — the single refusal in 181 live sends before the
        // correlator learned to pair across a transport disagreement. The queue
        // pallet says hrmp (the hop to Bridge Hub); the destination says
        // Ethereum, which is no local transport at all.
        xcm.insert(XcmMessageRow {
            event_index: 6,
            message_id: Some(format!("0x{}", "b1".repeat(32))),
            counterparty: None,
            timestamp: Some("2026-08-17T07:00:00Z".parse().unwrap()),
            ..xcm_row("polkadot-asset-hub", 19_000_050, "sent", "wire_hash")
        });
        xcm.insert(XcmMessageRow {
            event_index: 7,
            message_id: Some(format!("0x{}", "b2".repeat(32))),
            transport: "remote".into(),
            counterparty: Some("remote:ethereum:1".into()),
            timestamp: Some("2026-08-17T07:00:00Z".parse().unwrap()),
            ..xcm_row("polkadot-asset-hub", 19_000_050, "sent", "topic")
        });
        // …AND THE SHAPE LIVE DATA ACTUALLY PRODUCED, which is not that one.
        // Asset Hub #19407624 is a RELAY HOP: Hydration sent the message, Asset
        // Hub received it over HRMP, and its executor forwarded it onward to
        // Ethereum — with the TOPIC PROPAGATING across the hop, so the receive
        // and the onward send carry the SAME id and the journey's shape is
        // `send_and_receive`. The forwarded leg's program is empty (`[[]]`),
        // which is what `forwarded` keys on. A boundary sentence keyed only on
        // `send_only` never fired on the one bridged journey the index held.
        xcm.insert(XcmMessageRow {
            event_index: 6,
            message_id: Some(format!("0x{}", "c1".repeat(32))),
            counterparty: None,
            timestamp: Some("2026-08-17T06:00:00Z".parse().unwrap()),
            ..xcm_row("polkadot-asset-hub", 19_000_040, "sent", "wire_hash")
        });
        xcm.insert(XcmMessageRow {
            event_index: 7,
            message_id: Some(format!("0x{}", "c2".repeat(32))),
            transport: "remote".into(),
            counterparty: Some("remote:ethereum:1".into()),
            forwarded: true,
            timestamp: Some("2026-08-17T06:00:00Z".parse().unwrap()),
            ..xcm_row("polkadot-asset-hub", 19_000_040, "sent", "topic")
        });
        xcm.insert(XcmMessageRow {
            event_index: 10,
            // THE SAME id as the onward send: the topic crossed the hop.
            message_id: Some(format!("0x{}", "c2".repeat(32))),
            counterparty: Some("para:2034".into()),
            timestamp: Some("2026-08-17T06:00:00Z".parse().unwrap()),
            ..xcm_row("polkadot-asset-hub", 19_000_040, "received", "ambiguous")
        });
        xcm.insert_link(XcmLinkRow {
            chain_id: "polkadot-asset-hub".into(),
            block_height: 19_000_040,
            wire_event_index: 6,
            topic_event_index: 7,
            wire_hash: format!("0x{}", "c1".repeat(32)),
            topic: format!("0x{}", "c2".repeat(32)),
            transport: "hrmp".into(),
            rule: "remote_destination".into(),
            confidence: "medium".into(),
            evidence: serde_json::json!({
                "event_gap": 1, "topic_transport": "remote", "remote_topics": 1,
                "block_sends": {"wire": 1, "topic": 1}
            }),
            runtime_version: 2_003_002,
            correlator_version: 2,
        });
        xcm.insert_link(XcmLinkRow {
            chain_id: "polkadot-asset-hub".into(),
            block_height: 19_000_050,
            wire_event_index: 6,
            topic_event_index: 7,
            wire_hash: format!("0x{}", "b1".repeat(32)),
            topic: format!("0x{}", "b2".repeat(32)),
            // FROM THE WIRE SIDE — the destination named another consensus and
            // cannot say how this chain queued the message.
            transport: "hrmp".into(),
            rule: "remote_destination".into(),
            confidence: "medium".into(),
            evidence: serde_json::json!({
                "transport_candidates": 1, "ordinal": 0, "event_gap": 1,
                "topic_transport": "remote", "block_sends": {"wire": 1, "topic": 1}
            }),
            runtime_version: 2_003_002,
            correlator_version: 2,
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
            correlator_version: 2,
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
        // built through the real normalizer rather than hand-written, so these
        // are the strings production would produce (slice 6's lesson: a test
        // that hand-writes both sides of an identity comparison proves nothing)
        let ah_path = adapter_substrate::orml::chain_path("polkadot", Some(1000));
        let ah_usdt_absolute = adapter_substrate::orml::absolutize(
            &ah_path,
            &adapter_substrate::assets::local_asset_location(50, 1984),
        )
        .expect("usdt absolutizes");
        let relay_dot_absolute = adapter_substrate::orml::absolutize(
            &ah_path,
            &registry::NativeToken::Relay.location(),
        )
        .expect("AH's native token is the relay's DOT");
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
                // the observer-free name Asset Hub gives its own asset 1984
                absolute_key: Some(ah_usdt_absolute.to_string()),
                absolute_location: Some(ah_usdt_absolute.clone()),
                asset_type: None,
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
                // xcm_location stays SELF-RELATIVE (this chain's own currency);
                // absolute_key says the RELAY, because Asset Hub issues nothing
                // and its native token is DOT — the distinction slice 6's review
                // caught, and the reason `native_token: relay` is a seed field
                location_key: Some(r#"{"interior":[],"parents":0}"#.into()),
                xcm_location: Some(serde_json::json!({"parents": 0, "interior": []})),
                absolute_key: Some(relay_dot_absolute.to_string()),
                absolute_location: Some(relay_dot_absolute.clone()),
                asset_type: None,
            },
        );

        // ---- the HYDRATION leg of the SAME logical asset (slice 7) --------
        // This is what makes the consolidation test real rather than a
        // one-chain sum wearing a cross-chain name. Note the location: Hydration
        // observes Asset Hub's USDT ONE HOP AWAY, so its `location_key` genuinely
        // differs from Asset Hub's — and its `absolute_key` genuinely matches.
        let hydra_path = adapter_substrate::orml::chain_path("polkadot", Some(2034));
        let hydra_usdt_location = serde_json::json!({
            "parents": 1,
            "interior": {"X3": [[
                {"Parachain": [1000]}, {"PalletInstance": [50]}, {"GeneralIndex": [1984]}
            ]]}
        });
        let hydra_usdt_absolute =
            adapter_substrate::orml::absolutize(&hydra_path, &hydra_usdt_location)
                .expect("usdt absolutizes from hydration too");
        assets.insert(
            "hydration",
            AssetRow {
                asset_key: "tokens:10".into(),
                representation_kind: "orml".into(),
                symbol: Some("USDT".into()),
                name: Some("Tether USD".into()),
                decimals: Some(6),
                supply: None,
                status: None,
                location_key: Some(
                    adapter_substrate::assets::canonical_location(&hydra_usdt_location)
                        .expect("canonical"),
                ),
                xcm_location: Some(hydra_usdt_location.clone()),
                absolute_key: Some(hydra_usdt_absolute.to_string()),
                absolute_location: Some(hydra_usdt_absolute.clone()),
                asset_type: Some("Token".into()),
            },
        );
        // HDX — Hydration's OWN token, so it absolutizes to Hydration itself and
        // must NOT land in the same group as anybody's DOT
        let hdx_absolute =
            adapter_substrate::orml::absolutize(&hydra_path, &registry::NativeToken::Own.location())
                .expect("hdx absolutizes");
        assets.insert(
            "hydration",
            AssetRow {
                asset_key: "native".into(),
                representation_kind: "native".into(),
                symbol: Some("HDX".into()),
                name: Some("HDX".into()),
                decimals: Some(12),
                supply: None,
                status: None,
                location_key: Some(r#"{"interior":[],"parents":0}"#.into()),
                xcm_location: Some(serde_json::json!({"parents": 0, "interior": []})),
                absolute_key: Some(hdx_absolute.to_string()),
                absolute_location: Some(hdx_absolute.clone()),
                asset_type: Some("Token".into()),
            },
        );
        // an Erc20 asset: named, located, and unanchorable by construction
        assets.insert(
            "hydration",
            AssetRow {
                asset_key: "tokens:1001".into(),
                representation_kind: "orml".into(),
                symbol: Some("aDOT".into()),
                name: Some("aDOT".into()),
                decimals: Some(10),
                supply: None,
                status: None,
                location_key: None,
                xcm_location: None,
                absolute_key: None,
                absolute_location: None,
                asset_type: Some("Erc20".into()),
            },
        );

        // USDC on Hydration: a REGISTERED asset with an absolute name, whose
        // balance has been seen MOVING and never read from state. This is what
        // an un-swept chain looks like, and it is the shape that used to be
        // dropped from its group (see the test below).
        let hydra_usdc_location = serde_json::json!({
            "parents": 1,
            "interior": {"X3": [[
                {"Parachain": [1000]}, {"PalletInstance": [50]}, {"GeneralIndex": [1337]}
            ]]}
        });
        let hydra_usdc_absolute =
            adapter_substrate::orml::absolutize(&hydra_path, &hydra_usdc_location)
                .expect("usdc absolutizes");
        assets.insert(
            "hydration",
            AssetRow {
                asset_key: "tokens:22".into(),
                representation_kind: "orml".into(),
                symbol: Some("USDC".into()),
                name: Some("USD Coin".into()),
                decimals: Some(6),
                supply: None,
                status: None,
                location_key: Some(
                    adapter_substrate::assets::canonical_location(&hydra_usdc_location)
                        .expect("canonical"),
                ),
                xcm_location: Some(hydra_usdc_location),
                absolute_key: Some(hydra_usdc_absolute.to_string()),
                absolute_location: Some(hydra_usdc_absolute),
                asset_type: Some("Token".into()),
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
                frozen: None,
                spec_version: Some(2003002),
                source: "treasury-holdings".into(),
                note: None,
                status: Some("liquid".into()),
            },
        );
        // ---- Hydration: a SECOND chain holding the SAME logical asset -----
        // (the USDC movement below is added after `hydra_holder` exists)
        // Seeded rather than derived, and honestly so: the real Polkadot
        // treasury position on Hydration sits at a location-derived
        // (HashedDescription) sovereign address dotlens cannot derive, which is
        // this slice's stated gap. The fixture registers an account so the
        // CONSOLIDATION path is exercised; it does not pretend the derivation
        // problem is solved.
        let hydra_holder = adapter_substrate::accounts::sibling_sovereign(1000);
        treasury.insert_account(
            "polkadot",
            TreasuryAccountRow {
                chain_id: "hydration".into(),
                account_id: hydra_holder.to_vec(),
                role: "seeded".into(),
                instance: None,
                label: "Treasury position on Hydration".into(),
                derivation: None,
                source: "registry".into(),
                ss58: None,
            },
        );
        balances.insert_anchor(
            "hydration",
            &hydra_holder,
            "tokens:10",
            BalanceAnchorRow {
                height: 13_652_000,
                free: "5000000000".into(),
                reserved: "0".into(),
                total: "5000000000".into(),
                // THE FROZEN HALF, recorded since Phase 1 and read by nothing
                // until this slice. A position that is largely locked is not a
                // position that can be spent.
                frozen: Some("1000000000".into()),
                spec_version: Some(435),
                source: "treasury-holdings".into(),
                note: None,
                status: None,
            },
        );
        // AND AN HDX HOLDING. Without it the HDX-vs-DOT assertion below finds no
        // HDX position and silently does nothing — which would leave slice 6's
        // sharpest review finding (Asset Hub's native token absolutizing to the
        // RELAY, not to Asset Hub) protected by a test that cannot fail. Swapping
        // `NativeToken::Own`/`Relay` in the seeds must turn this suite red.
        balances.insert_anchor(
            "hydration",
            &hydra_holder,
            "native",
            BalanceAnchorRow {
                height: 13_652_000,
                free: "700000000000000".into(),
                reserved: "0".into(),
                total: "700000000000000".into(),
                frozen: None,
                spec_version: Some(435),
                source: "treasury-holdings".into(),
                note: None,
                status: None,
            },
        );
        // MOVEMENT WITH NO ANCHOR — a balance we have watched change and never
        // read. Deliberately left un-anchored: it is the test subject.
        balances.insert_change(
            "hydration",
            &hydra_holder,
            "tokens:22",
            BalanceChangeRow {
                height: 13_652_500,
                timestamp: None,
                event_index: 0,
                delta: "4800000000".into(),
                reason: "deposit".into(),
                counterparty: None,
            },
        );

        // AN Erc20 MOVEMENT, so `coverage.erc20_positions` is EXERCISED rather
        // than merely reachable. The counter was a shipped blocker precisely
        // because it sat behind the anchor check and an Erc20 can never have an
        // anchor; moving it in front fixed it, but nothing asserted it could
        // ever be non-zero — and a fixture that registers an Erc20 asset no
        // holding references leaves the fix protected by the same emptiness
        // that hid the bug. Erc20 balances DO reach `balance_changes` (slice 6
        // measured assets 222/4444/55 arriving as orml Unreserved/Withdrawn),
        // so a movement with no anchor is the honest live shape.
        balances.insert_change(
            "hydration",
            &hydra_holder,
            "tokens:1001",
            BalanceChangeRow {
                height: 13_652_600,
                timestamp: None,
                event_index: 0,
                delta: "123000000000".into(),
                reason: "withdraw".into(),
                counterparty: None,
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
            xcm_sim,
            xcm,
            coretime,
            broker,
            channels,
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

        // EVERY ROW ON THIS PAGE IS ENRICHED THE SAME WAY IT IS ON /v1/sim/…,
        // through the same function. Until slice 10 this page serialized rows
        // RAW, so a fork row appeared here with the DRY-RUN coverage list, no
        // `diff_covers` to say its storage diff describes the vehicle rather
        // than the call, and — the sharp one — a counterfactual's `overrides`
        // with no `is_counterfactual` marker: fabricated state on a governance
        // page, unlabelled. This asserts the enrichment RUNS here; that it is
        // correct per tier is `a_fork_rows_coverage_is_chosen_by_its_route…`.
        assert_eq!(
            sims[0]["tier_coverage"]["tier"], "dry_run",
            "the referendum page must carry each row's own tier coverage, not only the \
             response-level list"
        );
        // …and a dry-run row genuinely has no diff to describe, so it gets no
        // `diff_covers` — a block that names a field belongs to rows that have one.
        assert!(sims[0]["diff_covers"].is_null());
        // The response-level hint fires only when a fork row is present, and
        // there is none here.
        assert!(json["simulation_coverage"]["tiers_carry_their_own"].is_null());

        // A referendum we HAVE no proposal hash for must not be described as one
        // nobody previewed: we never looked, and saying otherwise would assert
        // something unchecked. Ref 1400 has no hash on either chain.
        let (_, j1400) = get_json(&app, "/v1/gov/polkadot/referenda/1400").await;
        assert!(j1400["simulations"].as_array().unwrap().is_empty());
        let why = j1400["simulation_coverage"]["recorded_only"].as_str().unwrap();
        assert!(why.contains("no proposal hash indexed yet"), "{why}");
        assert!(
            !why.contains("no preview of any tier has been run"),
            "an empty list because we could not look is a different claim from an empty \
             list because we looked and found nothing: {why}"
        );
        // …and the sentence for "we looked and found nothing" must not name a
        // TIER, because the lookup is untiered: an empty list there means nobody
        // previewed the call on any tier, not that Tier 1 specifically is absent.
        assert!(
            !why.contains("Tier 1"),
            "this list is not tier-filtered, so no claim about it may name one: {why}"
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
        assert_eq!(rows.len(), 8);
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
        // A purely local journey must NOT claim it left the ecosystem.
        assert_eq!(j["leaves_consensus"], false);
        assert!(j["remote_destinations"].as_array().unwrap().is_empty());
    }

    /// A BRIDGED SEND (Phase 3, slice 4): the wire hash and the topic still pair
    /// — across a transport disagreement that is structural rather than a defect
    /// — and the journey then STOPS at the consensus boundary and says so,
    /// instead of reading like a message that was dropped in flight.
    #[tokio::test]
    async fn a_journey_that_leaves_the_ecosystem_says_so_rather_than_looking_dropped() {
        let app = router(test_state().await);
        let wire = format!("0x{}", "b1".repeat(32));

        let (status, j) = get_json(&app, &format!("/v1/xcm/journeys/{wire}")).await;
        assert_eq!(status, StatusCode::OK);
        // The pair holds, and takes its transport from the WIRE event: the
        // destination named Ethereum and cannot say how this chain queued it.
        let aliases = j["aliases"].as_array().unwrap();
        assert_eq!(aliases.len(), 1);
        assert_eq!(aliases[0]["rule"], "remote_destination");
        assert_eq!(aliases[0]["confidence"], "medium");
        assert_eq!(aliases[0]["transport"], "hrmp");
        assert_eq!(aliases[0]["correlator_version"], 2);
        assert_eq!(j["steps"].as_array().unwrap().len(), 2, "two ids, one message");

        // THE POINT: `send_only` here is a BOUNDARY, and the payload separates
        // it from the three-way "in flight / unindexed / dropped" it would
        // otherwise read as.
        assert_eq!(j["shape"], "send_only");
        assert_eq!(j["leaves_consensus"], true);
        assert_eq!(
            j["remote_destinations"][0], "remote:ethereum:1",
            "the whole destination, so two chains in one foreign consensus stay distinct"
        );
        let reads_as = j["reads_as"].as_str().unwrap();
        assert!(reads_as.contains("STOPS HERE ON PURPOSE"), "{reads_as}");
        assert!(reads_as.contains("bridge tracer"), "{reads_as}");

        // No receiving half, so nothing to mirror — and crucially a foreign
        // counterparty produces no CONTRADICTION anywhere.
        assert_eq!(j["checks"]["counterparty_mirror"]["status"], "unknown");
        assert!(j["checks"]["counterparty_mirror"]["pairs"].as_array().unwrap().is_empty());
        assert!(j["coverage"]["not_covered"]
            .as_array()
            .unwrap()
            .iter()
            .any(|g| g.as_str().unwrap().contains("ANOTHER CONSENSUS SYSTEM")));
    }

    /// THE SHAPE LIVE DATA ACTUALLY PRODUCED (verified, Phase 3 slice 4): Asset
    /// Hub #19407624 is not a lone bridged send, it is a RELAY HOP — Hydration
    /// sent it, Asset Hub received it over HRMP and forwarded it to Ethereum,
    /// and the topic PROPAGATED across the hop, so plain id equality already
    /// stitches the receive to the onward send and the shape is
    /// `send_and_receive`. The boundary must still be stated: a journey can be
    /// fully stitched AND still leave the ecosystem, and keying the sentence on
    /// `send_only` alone left this one reading as a complete story.
    #[tokio::test]
    async fn a_stitched_journey_that_still_leaves_the_ecosystem_says_both() {
        let app = router(test_state().await);
        let wire = format!("0x{}", "c1".repeat(32));

        let (status, j) = get_json(&app, &format!("/v1/xcm/journeys/{wire}")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(j["shape"], "send_and_receive", "the topic crossed the hop, so this stitches");
        assert_eq!(j["steps"].as_array().unwrap().len(), 3);
        assert_eq!(j["leaves_consensus"], true);
        assert_eq!(j["remote_destinations"][0], "remote:ethereum:1");

        let reads_as = j["reads_as"].as_str().unwrap();
        assert!(reads_as.contains("stitched across"), "{reads_as}");
        assert!(
            reads_as.contains("STOPS ON PURPOSE") && reads_as.contains("bridge tracer"),
            "a stitched journey that leaves consensus must still say so: {reads_as}"
        );

        // Same chain on both sides, so there is no mirror to check — and a
        // foreign counterparty must never produce a CONTRADICTION.
        assert_eq!(j["checks"]["counterparty_mirror"]["status"], "unknown");
        // One id on two SIDES of one chain is a hop, not a collision.
        assert_eq!(j["checks"]["id_uniqueness"]["status"], "ok");

        // AND THE OBSERVATIONS ENDPOINT MUST NOT CLAIM TWO CHAINS FOR IT. Its
        // both-halves sentence read "one chain reported sending this id and
        // another reported processing it" — false on a hop, where both halves
        // sit on the same chain.
        let topic = format!("0x{}", "c2".repeat(32));
        let (_, m) = get_json(&app, &format!("/v1/xcm/messages/{topic}")).await;
        assert_eq!(m["observations"].as_array().unwrap().len(), 2);
        let hop = m["reads_as"].as_str().unwrap();
        assert!(hop.contains("ON ONE CHAIN") && hop.contains("HOP"), "{hop}");

        // …while a genuine two-chain journey still says what it always said.
        let two = format!("0x{}", "ee".repeat(32));
        let (_, m2) = get_json(&app, &format!("/v1/xcm/messages/{two}")).await;
        let across = m2["reads_as"].as_str().unwrap();
        assert!(across.contains("one chain reported sending this id and another"), "{across}");
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

    /// Pull the two fork rows out of the response by their ROUTE rather than by
    /// index. Slice 7 lost a test to exactly this: a new fixture row moved
    /// `segments[0]` and the assertion then described a different subject.
    fn fork_rows(json: &serde_json::Value) -> (serde_json::Value, serde_json::Value) {
        let rows = json["simulations"].as_array().expect("a list");
        let by = |route: &str| {
            rows.iter()
                .find(|r| r["dispatch_route"] == route)
                .unwrap_or_else(|| panic!("no {route} fork row in the response"))
                .clone()
        };
        (by(sim::ROUTE_SCHEDULED), by(sim::ROUTE_DRY_RUN_EXTRINSIC))
    }

    #[tokio::test]
    async fn a_fork_rows_coverage_is_chosen_by_its_route_and_says_what_its_diff_covers() {
        let app = router(test_state().await);
        let (status, json) = get_json(
            &app,
            &format!("/v1/sim/polkadot-asset-hub/calls/0x{}", "f0".repeat(32)),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (scheduled, extrinsic) = fork_rows(&json);

        let covers = |v: &serde_json::Value| -> String {
            v["tier_coverage"]["not_covered"]
                .as_array()
                .expect("a list")
                .iter()
                .map(|l| l.as_str().unwrap_or_default())
                .collect::<Vec<_>>()
                .join(" ")
        };
        // LOWERCASED BEFORE MATCHING. The two arms deliberately shout different
        // parts of their sentences, and an assertion that depends on which words
        // are capitalised is an assertion about typography.
        let (sched_text, extr_text) = (
            covers(&scheduled).to_lowercase(),
            covers(&extrinsic).to_lowercase(),
        );

        // THE TWO LISTS ARE STRUCTURALLY DIFFERENT, asserted as a PROPERTY and
        // not as wording: a shared list carefully worded until it is true of both
        // is the defect this project shipped five times before splitting them.
        assert_ne!(
            sched_text, extr_text,
            "the two routes model different things and must not share one coverage list"
        );
        assert!(
            extr_text.contains("transaction extensions did run"),
            "an applied extrinsic runs the whole pipeline with only the signature faked"
        );
        assert!(
            sched_text.contains("no transaction extension"),
            "a scheduled dispatch is not an extrinsic"
        );
        // ...and neither list may make the OTHER route's claim.
        assert!(!sched_text.contains("transaction extensions did run"));
        assert!(!extr_text.contains("no transaction extension"));

        // THE MEASURED DEFECT, on the row. The same `extrinsic_only` bytes are
        // the call's own writes on one route and the no-op vehicle's on the
        // other, and the row says which rather than leaving it to a coverage
        // list somebody may not read.
        assert_eq!(scheduled["diff_status"], sim::DIFF_STATUS_EXTRINSIC_ONLY);
        assert_eq!(extrinsic["diff_status"], sim::DIFF_STATUS_EXTRINSIC_ONLY);
        assert_eq!(
            scheduled["diff_covers"]["covers_this_call"], false,
            "a scheduled dispatch happens in on_initialize, which an extrinsic-scoped diff omits"
        );
        assert_eq!(
            extrinsic["diff_covers"]["covers_this_call"], true,
            "on the extrinsic route the subject IS the extrinsic, so the same scope is complete"
        );
        assert_eq!(
            scheduled["diff_covers"]["read_instead"], "emitted_events",
            "a row whose diff does not describe its call must name what does"
        );
        assert!(
            extrinsic["diff_covers"]["read_instead"].is_null(),
            "nothing to redirect to when the diff already covers the call"
        );
        // The diff is STILL SERVED on both — it is scope that differs, not
        // readability, and blanking it would lose the vehicle's real writes.
        assert_eq!(scheduled["storage_diff_count"], 1);
        assert_eq!(extrinsic["storage_diff_count"], 1);

        // And the coverage list carries the same fact, so a consumer reading
        // either one is not told a different story.
        assert!(
            sched_text.contains("does not describe its call"),
            "the scheduled route's list names the diff blindness: {sched_text}"
        );
        assert!(
            !sched_text.contains("head+1"),
            "the agenda height is decided from data and recorded in `agenda_anchor`; head+1 was \
             slice 8's assumption and was wrong by ~13.15M blocks: {sched_text}"
        );
        assert_eq!(
            scheduled["agenda_anchor"]["provider"], "relay",
            "the anchor decision is surfaced unconditionally on a scheduled row"
        );

        // ...AND THE FAITHFUL-FORK NOTE IS SELECTED BY ROUTE TOO, which is the
        // same defect one field over: `counterfactual.reads_as` described "the
        // scheduled task", "the agenda slot (see `agenda_anchor`)" and "entries
        // marked `from_harness`" on EVERY faithful fork row — every clause of
        // which is false on the extrinsic route, where `agenda_anchor` is NULL
        // and no entry is ever `from_harness`. Asserted as a PROPERTY: the
        // extrinsic arm may not describe scheduler machinery it never used.
        assert_eq!(scheduled["counterfactual"]["is_counterfactual"], false);
        assert_eq!(extrinsic["counterfactual"]["is_counterfactual"], false);
        let extr_faithful = extrinsic["counterfactual"]["reads_as"]
            .as_str()
            .expect("a faithful fork row explains that it is one")
            .to_lowercase();
        // The precise defect was DIRECTING THE READER AT FIELDS THAT ARE EMPTY
        // on this route: `agenda_anchor` is NULL on every extrinsic row and no
        // diff entry is ever `from_harness`. Denying the machinery in prose is
        // fine and useful; citing the columns as if they held something is not,
        // so the assertion is on the field pointers rather than on the words.
        for absent_field in ["agenda_anchor", "from_harness"] {
            assert!(
                !extr_faithful.contains(absent_field),
                "the extrinsic route leaves `{absent_field}` empty, so its faithful-fork note \
                 must not send a reader to it: {extr_faithful}"
            );
        }
        assert_ne!(
            extr_faithful,
            scheduled["counterfactual"]["reads_as"]
                .as_str()
                .expect("a string")
                .to_lowercase(),
            "the two routes inject different things and must not share one faithful-fork note"
        );
        assert!(
            scheduled["counterfactual"]["reads_as"]
                .as_str()
                .expect("a string")
                .to_lowercase()
                .contains("agenda"),
            "the scheduled route DOES replace an agenda slot and must keep saying so"
        );
    }

    #[tokio::test]
    async fn a_fork_row_carries_no_forwarded_list_and_therefore_no_legs_or_attribution() {
        let app = router(test_state().await);
        let (_, json) = get_json(
            &app,
            &format!("/v1/sim/polkadot-asset-hub/calls/0x{}", "f0".repeat(32)),
        )
        .await;
        let (scheduled, extrinsic) = fork_rows(&json);

        for row in [&scheduled, &extrinsic] {
            let keys: Vec<&String> = row.as_object().expect("an object").keys().collect();
            // ASSERTED ON `keys()`, NOT ON A NULL. `legs: []` beside a coverage
            // line saying "an empty list means nobody followed them" would be
            // false of a row where nothing COULD be followed — the fifth
            // recurrence of a line being wrong on its second consumer, which is
            // why these two fields are attached only to rows that have a
            // forwarded list at all.
            assert!(
                !keys.iter().any(|k| k.as_str() == "legs"),
                "a fork row must not carry `legs`: {keys:?}"
            );
            assert!(
                !keys.iter().any(|k| k.as_str() == "forwarded_attribution"),
                "a fork row has no forwarded_xcms to attribute: {keys:?}"
            );
            assert!(
                row["forwarded_xcms"].is_null(),
                "NULL, never [] — and reading this column as a bare value is what would make \
                 every fork row fail to load through Pg"
            );
            // Both runtime-API version columns are absent for the same reason:
            // this tier calls neither.
            assert!(row["xcm_version"].is_null());
            assert!(row["api_version"].is_null());
        }

        // The dry-run row one call hash over DOES carry both, so this is a
        // property of the tier and not of the endpoint having dropped them.
        let (_, dry) = get_json(
            &app,
            &format!("/v1/sim/polkadot-asset-hub/calls/0x{}", "ab".repeat(32)),
        )
        .await;
        let keys: Vec<&String> = dry["simulations"][0].as_object().unwrap().keys().collect();
        assert!(keys.iter().any(|k| k.as_str() == "legs"));
        assert!(keys.iter().any(|k| k.as_str() == "forwarded_attribution"));
    }

    #[tokio::test]
    async fn a_forwarded_list_is_differenced_against_its_baseline_and_the_leg_is_stitched() {
        let app = router(test_state().await);
        let (status, json) = get_json(
            &app,
            &format!("/v1/sim/polkadot-asset-hub/calls/0x{}", "ab".repeat(32)),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let sim = &json["simulations"][0];

        // THE RAW LIST IS UNCHANGED and still says two messages — nothing is
        // hidden, the difference is reported BESIDE it.
        assert_eq!(sim["forwarded_xcms"][0]["messages"].as_array().unwrap().len(), 2);

        let a = &sim["forwarded_attribution"];
        assert_eq!(a["total_messages"], 2);
        assert_eq!(a["ambient_messages"], 1);
        assert_eq!(
            a["attributed_messages"], 1,
            "one of the two was already in flight at this state and belongs to nobody here"
        );
        assert_eq!(a["baseline_run"], "system.remark");
        assert_eq!(a["destinations"][0]["destination_index"], 0);
        assert_eq!(
            a["destinations"][0]["messages"][0]["message_index"], 1,
            "the index is the position in the SUBJECT's own list — what a follower needs \
             to ask for the right bytes"
        );
        assert!(a["reads_as"].as_str().unwrap().contains("1 of 2"));

        // THE LEG: the message this call really queued, previewed where it was
        // addressed, and REJECTED AT THE BARRIER — the outcome the sending chain
        // cannot see, since its own Sent event would look perfectly successful.
        let legs = sim["legs"].as_array().expect("legs list");
        assert_eq!(legs.len(), 1);
        assert_eq!(legs[0]["chain_id"], "hydration");
        assert_eq!(legs[0]["status"], "not_started");
        assert_eq!(
            legs[0]["origin_ref"], "para:1000",
            "Hydration is told the sender is para 1000 — the MIRROR of the destination \
             Asset Hub addressed, not the destination itself"
        );
        assert_eq!(legs[0]["source_message_index"], 1, "it is the ATTRIBUTED message");
        assert_eq!(legs[0]["xcm_error"]["error"], serde_json::json!({"Barrier": []}));

        // THE COVERAGE LISTS ARE PINNED BY PROPERTY, not by wording, because
        // this project has twice shipped a shared not_covered helper whose first
        // line was false on the second endpoint that served it. The rule: a line
        // that names a field belongs only to a response that HAS the field.
        let covers = |v: &serde_json::Value| -> String {
            v.as_array()
                .expect("a list")
                .iter()
                .map(|l| l.as_str().unwrap_or_default())
                .collect::<Vec<_>>()
                .join(" ")
        };
        let arrival = covers(&json["coverage"]["arrival_not_covered"]);
        assert!(
            arrival.contains("not_started") && arrival.contains("barrier"),
            "an arrival's own limits ship with it — the receiving side has limits the \
             sending side does not: {arrival}"
        );
        assert!(
            !covers(&json["coverage"]["attribution_not_covered"]).contains("`legs`"),
            "the attribution list is served by the arrival endpoint too, which has no legs"
        );

        // …and the REFERENDUM response, which computes no attribution, must not
        // describe fields it does not carry.
        let (_, referendum) = get_json(&app, "/v1/gov/polkadot/referenda/1500").await;
        let gaps = covers(&referendum["simulation_coverage"]["not_covered"]);
        assert!(
            !gaps.contains("forwarded_attribution") && !gaps.contains("`legs`"),
            "the shared list must not name fields only /v1/sim/.../calls/... carries: {gaps}"
        );
        assert!(referendum["simulation_coverage"]["attribution_elsewhere"]
            .as_str()
            .unwrap()
            .contains("/v1/sim/"));

        // A ROW WITH NO BASELINE claims nothing at all, which is every row
        // recorded before this slice.
        let (_, older) = get_json(
            &app,
            &format!("/v1/sim/polkadot-asset-hub/calls/0x{}", "cc".repeat(32)),
        )
        .await;
        let a = &older["simulations"][0]["forwarded_attribution"];
        assert_eq!(a["baseline"], serde_json::Value::Null);
        assert!(a["attributed_messages"].is_null(), "nothing is counted without a baseline");
        assert!(a["reads_as"].as_str().unwrap().contains("messages present"));
        assert!(
            older["simulations"][0]["legs"].as_array().unwrap().is_empty(),
            "nobody followed it"
        );
    }

    #[tokio::test]
    async fn an_arriving_program_is_served_by_its_program_hash() {
        let app = router(test_state().await);
        let (status, json) = get_json(
            &app,
            &format!("/v1/sim/hydration/xcm/0X{}", "AA".repeat(32)),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["program_hash"], format!("0x{}", "aa".repeat(32)));
        let rows = json["simulations"].as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["status"], "not_started");
        assert_eq!(rows[0]["origin_ref"], "para:1000");
        assert_eq!(rows[0]["source_chain_id"], "polkadot-asset-hub");
        assert!(rows[0]["raw_location"]
            .as_str()
            .unwrap()
            .contains("DryRunApi_dry_run_xcm"));

        // THE ARRIVAL SIDE GETS THE SAME DIFFERENCE the call side does — the
        // migration promises it, and a promise made in a schema is kept in a
        // reader or not at all. This leg forwards nothing of its own, and
        // Hydration's ambient queue holds one message that is nobody's here.
        let a = &rows[0]["forwarded_attribution"];
        assert_eq!(a["baseline"], format!("0x{}", "05".repeat(32)));
        assert_eq!(a["baseline_run"], "(empty program)");
        assert_eq!(a["ambient_messages"], 1);
        assert_eq!(a["total_messages"], 0);
        assert_eq!(a["attributed_messages"], 0);
        assert!(a["reads_as"].as_str().unwrap().contains("nothing of its own"));

        // The same program on a chain nobody previewed it on is empty, not
        // borrowed from another runtime's answer.
        let (_, ah) = get_json(
            &app,
            &format!("/v1/sim/polkadot-asset-hub/xcm/0x{}", "aa".repeat(32)),
        )
        .await;
        assert!(ah["simulations"].as_array().unwrap().is_empty());
        let (s, _) = get_json(&app, &format!("/v1/sim/nowhere/xcm/0x{}", "aa".repeat(32))).await;
        assert_eq!(s, StatusCode::NOT_FOUND);
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

        // BY CHAIN, NOT BY INDEX. The treasury index orders by (chain, role,
        // label), so adding a Hydration account in slice 7 moved this segment
        // from position 0 — the same shape as Phase 3 slice 2, where registering
        // Hydration silently satisfied a test's precondition and moved its
        // subject. Selecting by name cannot drift again.
        let seg = json["segments"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["chain"] == "polkadot-asset-hub")
            .expect("the Asset Hub segment");
        assert_eq!(seg["chain"], "polkadot-asset-hub");
        let account = seg["accounts"]
            .as_array()
            .unwrap()
            .iter()
            .find(|a| a["role"] == "pot")
            .expect("the treasury pot");
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

        // 3 anchored: AH's USDT and native, plus Hydration's USDT (slice 7's
        // fixture addition). These are GLOBAL counters across every segment, so
        // they move when the fixture gains a chain — updated deliberately rather
        // than discovered as a failure.
        // the Hydration segment is asserted rather than merely present, so a
        // fixture that stops registering it fails here instead of quietly
        // shrinking every count below
        let hydra = json["segments"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["chain"] == "hydration")
            .expect("the Hydration segment");
        let hydra_usdt = hydra["accounts"][0]["positions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p["asset"] == "tokens:10")
            .expect("Hydration's orml USDT position");
        assert_eq!(hydra_usdt["amount"], "5000000000");
        assert_eq!(hydra_usdt["display"], "5000.000000");

        // 4 anchored: AH's USDT and native, Hydration's USDT and native. These
        // are GLOBAL counters across every segment, so they move when the
        // fixture gains a chain — updated deliberately rather than discovered as
        // a failure.
        assert_eq!(coverage["positions_with_an_anchor"], 4);
        // 2 unanchored: AH's USDC (no registry entry at all) and Hydration's
        // USDC (registered, absolute-named, never swept) — different causes,
        // and the consolidation endpoint treats them differently. The THIRD is
        // Hydration's Erc20 aDOT movement: this endpoint has no Erc20 branch, so
        // it counts one as plainly unanchored (which it is), while
        // /consolidated classifies it BEFORE the anchor check and holds its own
        // unanchored count at 2. The two surfaces disagreeing here is the design
        // working — "never swept" and "unanchorable in principle" are different
        // failures with different remedies, and only one of them has a remedy.
        assert_eq!(coverage["positions_without_an_anchor"], 3);
        assert!(coverage["valuation"].as_str().unwrap().contains("quantities only"));
        let gaps = coverage["not_covered"].as_array().unwrap();
        assert!(gaps.iter().any(|g| g.as_str().unwrap().contains("bounty")));

        // THE ACCOUNT-VISIBILITY LINE IS DERIVED FROM THE ACCOUNT LIST, and the
        // assertion here has to be on the PROPERTY rather than the wording.
        // Slice 6 asserted `contains("Hydration")` against a line that read
        // "positions on chains dotlens has not registered — notably the
        // Hydration …"; registering Hydration made that line false on its own
        // page, slice 7 replaced it with a derived one that names no chain, and
        // this assertion went stale with it. Asserting the wording is how a
        // check outlives the thing it was checking.
        let visibility = gaps
            .iter()
            .map(|g| g.as_str().unwrap())
            .find(|g| g.contains("only as visible as its ACCOUNTS"))
            .expect("the derived account-visibility line");
        // The fixture seeds a Hydration account with a NULL derivation, so the
        // line must say some accounts are seeded. Neuter the derivation and
        // this clause disappears and this fails — which is the point.
        assert!(
            visibility.contains("seeded rather than derived"),
            "an account with a null `derivation` is listed above, so the derived \
             line must say so instead of implying every account was derived: \
             {visibility}"
        );

        // …and the defect that forced the rewrite must not come back in any
        // wording: no gap may call a chain unregistered while this same
        // response lists accounts on it. Chain ids are lowercase and the prose
        // capitalises, so the comparison folds case.
        let listed: Vec<&str> = json["segments"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|s| !s["accounts"].as_array().unwrap().is_empty())
            .map(|s| s["chain"].as_str().unwrap())
            .collect();
        assert!(listed.contains(&"hydration"), "the fixture must list Hydration accounts");
        for gap in gaps.iter().map(|g| g.as_str().unwrap().to_lowercase()) {
            if !gap.contains("not registered") {
                continue;
            }
            for chain in &listed {
                assert!(
                    !gap.contains(chain),
                    "`{chain}` has accounts in this very response, so no coverage \
                     line may describe it as unregistered: {gap}"
                );
            }
        }
    }

    /// THE SLICE'S HEADLINE: one logical asset, two chains, one number — and
    /// the negative half asserted first, because without it this test would be
    /// summing two rows that were never distinguishable in the first place.
    #[tokio::test]
    async fn one_asset_on_two_chains_consolidates_into_a_single_addable_position() {
        let app = router(test_state().await);
        let (status, json) = get_json(&app, "/v1/treasury/polkadot/consolidated").await;
        assert_eq!(status, StatusCode::OK);

        let positions = json["positions"].as_array().unwrap();
        let usdt = positions
            .iter()
            .find(|p| p["symbol"] == "USDT")
            .expect("the consolidated USDT position");

        // THE NEGATIVE HALF FIRST. The two legs' SELF-RELATIVE names differ —
        // Hydration sees Asset Hub's USDT one hop away — so `location_key`
        // could never have joined them. If these are ever equal, everything
        // below proves nothing.
        let legs = usdt["legs"].as_array().unwrap();
        assert_eq!(legs.len(), 2);
        let state = test_state().await;
        let ah_rows = state.assets.assets("polkadot-asset-hub").await.unwrap();
        let hy_rows = state.assets.assets("hydration").await.unwrap();
        let ah_usdt = ah_rows.iter().find(|a| a.asset_key == "assets:1984").unwrap();
        let hy_usdt = hy_rows.iter().find(|a| a.asset_key == "tokens:10").unwrap();
        assert_ne!(
            ah_usdt.location_key, hy_usdt.location_key,
            "a Location is relative to its observer — version-stripping CANNOT \
             reconcile two frames, which is the whole reason absolute_key exists"
        );
        assert_eq!(
            ah_usdt.absolute_key, hy_usdt.absolute_key,
            "…and absolutizing them does"
        );

        // the sum, and the units it is summed in
        assert_eq!(usdt["addable"], true);
        assert_eq!(usdt["decimals"], 6);
        assert_eq!(usdt["total"], "25000000000", "20,000 on AH + 5,000 on Hydration");
        assert_eq!(usdt["display"], "25000.000000");
        let chains: Vec<&str> = usdt["chains"].as_array().unwrap()
            .iter().map(|c| c.as_str().unwrap()).collect();
        assert_eq!(chains, vec!["hydration", "polkadot-asset-hub"]);
        // cross-chain positions sort first — the ones this endpoint exists for
        assert_eq!(positions[0]["symbol"], "USDT");
        assert_eq!(json["coverage"]["spanning_more_than_one_chain"], 1);

        // EVERY LEG KEEPS ITS PROVENANCE, so the sum is re-derivable and the
        // legs are re-derivable from the chain. A total whose parts cannot be
        // checked is what every incumbent already offers.
        let hydra_leg = legs.iter().find(|l| l["chain"] == "hydration").unwrap();
        assert_eq!(hydra_leg["amount"], "5000000000");
        assert_eq!(hydra_leg["provenance"]["anchor_height"], 13_652_000);
        assert_eq!(hydra_leg["provenance"]["anchor_spec_version"], 435);
        // THE FROZEN HALF, surfaced for the first time since Phase 1 — and the
        // field is `frozen_at_anchor`, not `frozen`, on purpose: it is read from
        // the ANCHOR while `amount` is anchor + deltas, so the two are as-of
        // different heights and it is NOT subtracted from any total. A bare
        // `frozen` beside `amount` invites exactly that subtraction.
        assert_eq!(hydra_leg["frozen_at_anchor"], "1000000000");
        assert!(
            hydra_leg.get("frozen").is_none(),
            "the un-qualified name must not come back: it is what a reader \
             would subtract"
        );

        // HDX MUST NOT JOIN ANYBODY'S DOT. Hydration issues its own token, so it
        // absolutizes to Hydration itself; Asset Hub issues nothing, so its
        // native row absolutizes to the RELAY. Two different logical assets, and
        // getting this wrong was slice 6's sharpest review finding.
        let hdx = positions
            .iter()
            .find(|p| p["symbol"] == "HDX")
            .expect("Hydration's own token is a position of its own");
        let dot = positions
            .iter()
            .find(|p| p["symbol"] == "DOT")
            .expect("Asset Hub's native token");
        assert_ne!(
            hdx["absolute_key"], dot["absolute_key"],
            "HDX is Hydration's own token and absolutizes to Hydration; Asset \
             Hub issues NOTHING, so its native token is the relay's DOT and \
             absolutizes to the relay. Merging them would be the defect slice \
             6's review caught, one endpoint later"
        );
        assert!(
            dot["absolute_key"].as_str().unwrap().contains("Parachain") == false,
            "the relay's DOT names no parachain: {}", dot["absolute_key"]
        );

        // NOTHING IS SUMMED ACROSS ASSETS and the response says why. Assert the
        // two load-bearing CLAIMS rather than a phrase: that a cross-asset total
        // is refused, and that a price is the reason. (The first draft of this
        // test asserted `contains("no price")` against a string that says
        // "needs a price" — a wording check that fails on wording alone.)
        let valuation = json["coverage"]["valuation"].as_str().unwrap();
        assert!(valuation.contains("no cross-asset total"), "{valuation}");
        assert!(valuation.contains("price"), "{valuation}");
        assert!(json.get("total_usd").is_none());
        assert!(json.get("total").is_none());
        // and no valuation leaks in beside a quantity anywhere in the payload
        let body = json.to_string().to_lowercase();
        for forbidden in ["usd_est", "\"usd\"", "price_usd"] {
            assert!(
                !body.contains(forbidden),
                "a quantity is exact and self-provenanced; a price is neither, \
                 and `{forbidden}` appears in the payload"
            );
        }
    }

    /// What CANNOT be consolidated is listed, not dropped — the amended Phase 3
    /// criterion 3 ("enumerated and counted, never estimated") as an assertion.
    #[tokio::test]
    async fn what_cannot_be_consolidated_is_listed_with_its_reason() {
        let app = router(test_state().await);
        let (_, json) = get_json(&app, "/v1/treasury/polkadot/consolidated").await;

        let un = json["unconsolidated"].as_array().unwrap();
        // the USDC position was seen MOVING and never anchored: real money, an
        // unknown balance, and it must not silently make the totals look complete
        let unanchored = un
            .iter()
            .find(|u| u["asset_key"] == "assets:1337")
            .expect("the unanchored position is listed, not dropped");
        assert!(unanchored["amount"].is_null());
        assert!(unanchored["reason"].as_str().unwrap().contains("not in core.assets"));
        // BOTH unanchored positions are counted, but only ONE of them lands
        // here: the other has an absolute name, so it joins its group and
        // suppresses that group's total instead (see the test above). The two
        // are different failures and the endpoint does not blur them.
        assert_eq!(json["coverage"]["positions_without_an_anchor"], 2);
        // TWO unconsolidated rows for two different reasons — Hydration's USDC
        // (registered and absolute-named, simply never swept: a remedy exists)
        // and Hydration's Erc20 aDOT (no remedy exists at all). Counting them
        // together would hide the distinction the Erc20 line is written to make.
        assert_eq!(un.len(), 2);

        // and the gaps are NAMED, including the one this slice cannot close
        let gaps = json["coverage"]["not_covered"].as_array().unwrap();
        assert!(gaps.iter().any(|g| g.as_str().unwrap().contains("Erc20")));

        // THE Erc20 COUNTER MUST BE ABLE TO FIRE. It was a shipped blocker for
        // sitting behind the anchor check — an Erc20 can never HAVE an anchor,
        // so it read 0 forever while claiming to measure the largest uncovered
        // position dotlens knows about. Moving it in front fixed the code; this
        // asserts the fix, because "reachable by inspection" is what the
        // original was too. Move it back behind the anchor check and this fails.
        assert_eq!(json["coverage"]["erc20_positions"], 1);
        let erc20 = un
            .iter()
            .find(|u| u["asset_key"] == "tokens:1001")
            .expect("the Erc20 position is LISTED, not dropped");
        assert!(erc20["amount"].is_null(), "an Erc20 has no readable position");
        assert_eq!(
            erc20["movement_only"], "123000000000",
            "the event-stream total is shown as movement, never as a position"
        );
        assert!(erc20["reason"].as_str().unwrap().contains("pallet_evm"));
        // and the remedy that cannot work is not offered
        assert!(
            !erc20["reason"].as_str().unwrap().contains("run treasury-holdings"),
            "advising a sweep that skips Erc20 by design is worse than no advice"
        );
        assert!(gaps
            .iter()
            .any(|g| g.as_str().unwrap().contains("HashedDescription")));
        assert!(gaps
            .iter()
            .any(|g| g.as_str().unwrap().contains("DIFFERENT blocks")),
            "a consolidated total is as-of its anchors, not as of one instant");

        // every registered chain declares `native_token`, so nothing is silently
        // under-reported for that reason — and the endpoint says so rather than
        // leaving it to be assumed
        assert_eq!(
            json["coverage"]["chains_without_native_token"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
    }

    /// THE DEFECT A REVIEWER CAUGHT, as a test — and it is the one that would
    /// have produced a wrong number rather than a missing one.
    ///
    /// A leg whose asset IS consolidatable but whose balance has never been read
    /// from state was, in the first draft, classified as "unconsolidated" and
    /// dropped BEFORE grouping. Its position then rendered a clean total across
    /// the remaining chains with nothing to say a leg was missing. Hydration is
    /// in exactly that state on live data today — slice 6 verified
    /// `treasury-holdings hydration` reports 0 accounts — so this is the shape
    /// production would have hit first.
    #[tokio::test]
    async fn a_leg_of_unknown_size_suppresses_its_total_instead_of_vanishing() {
        let app = router(test_state().await);
        let (_, json) = get_json(&app, "/v1/treasury/polkadot/consolidated").await;

        let usdc = json["positions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p["symbol"] == "USDC")
            .expect("USDC is a REGISTERED asset with an absolute name, so it \
                     belongs in `positions` even though its balance is unknown");

        // the leg JOINED its group rather than being filed away elsewhere…
        assert_eq!(usdc["legs"].as_array().unwrap().len(), 1);
        assert_eq!(usdc["legs_of_unknown_size"], 1);
        assert_eq!(usdc["complete"], false);
        // …and the group therefore has NO total, rather than one that omits a
        // leg and looks whole. A null beside a populated leg list is
        // unmistakable; a smaller number would not have been.
        assert!(
            usdc["total"].is_null(),
            "a total over only the KNOWN legs would be wrong AND look right"
        );
        assert!(usdc["display"].is_null());
        // the units still agree, so `addable` stays true — "can these be added"
        // and "do we know all the numbers" are different questions and are
        // reported separately rather than collapsed into one flag
        assert_eq!(usdc["addable"], true);
        assert_eq!(json["coverage"]["positions_with_a_leg_of_unknown_size"], 1);

        // and the unknown leg says WHY, in the payload rather than in a doc
        let leg = &usdc["legs"][0];
        assert!(leg["amount"].is_null());
        assert_eq!(leg["basis"], "deltas_only");
        assert!(leg["unknown_because"]
            .as_str()
            .unwrap()
            .contains("never read from state"));

        // CONTRAST, in the same response: the fully-anchored USDT position DOES
        // carry a total. Without this the test would pass on an endpoint that
        // simply never totals anything.
        let usdt = json["positions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p["symbol"] == "USDT")
            .expect("the USDT position");
        assert_eq!(usdt["complete"], true);
        assert_eq!(usdt["total"], "25000000000");
    }

    /// The reader that earns `assets_absolute_key_idx` (migration 0020).
    #[tokio::test]
    async fn asset_identity_lists_every_representation_and_says_if_they_are_addable() {
        let state = test_state().await;
        let ah = state.assets.assets("polkadot-asset-hub").await.unwrap();
        let key = ah
            .iter()
            .find(|a| a.asset_key == "assets:1984")
            .unwrap()
            .absolute_key
            .clone()
            .unwrap();

        let app = router(test_state().await);
        let (status, json) = get_json(
            &app,
            &format!("/v1/assets/identity?key={}", urlencode(&key)),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["representation_count"], 2);
        assert_eq!(json["addable"], true);
        let reps = json["representations"].as_array().unwrap();
        assert_eq!(reps[0]["chain"], "hydration");
        assert_eq!(reps[0]["asset_key"], "tokens:10");
        assert_eq!(reps[1]["chain"], "polkadot-asset-hub");
        assert_eq!(reps[1]["asset_key"], "assets:1984");
        // the two self-relative names are kept beside the absolute one, because
        // their DIFFERING is the finding and a reader should not have to take
        // the normalizer on trust
        assert_ne!(reps[0]["location_key"], reps[1]["location_key"]);
        assert!(json["reads_as"].as_str().unwrap().contains("directly addable"));

        // an unknown key is an honest empty answer, not a 404 — "we have not
        // indexed this" and "this does not exist" are different claims
        let (s2, j2) = get_json(&app, "/v1/assets/identity?key=%5B%5D").await;
        assert_eq!(s2, StatusCode::OK);
        assert_eq!(j2["representation_count"], 0);
        assert_eq!(
            j2["addable"], false,
            "a claim of addability about nothing — the first draft returned true \
             here, because `distinct.len() <= 1` is vacuously satisfied by an \
             empty set"
        );
        assert!(j2["reads_as"].as_str().unwrap().contains("not the same as"));
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

    /// THE DELTA IS A DIFFERENT NUMBER FROM THE OCCUPANCY RATIO OVER THE SAME
    /// WINDOW, and that difference is the whole slice.
    ///
    /// Asserted as arithmetic rather than as wording: the identical window
    /// returns 17% from `/occupancy` (17 of 100 declared core-block slots) and
    /// 42.5% from `/delta` (17 of 40 ENTITLED slots), because 6 of the 10 cores
    /// were pooled and never entitled to a task at all. A change that quietly
    /// divided by the declared count again would keep every other assertion here
    /// green and would report 25,000 of the measured window's 43,000 pool slots
    /// as waste nobody bought.
    #[tokio::test]
    async fn the_delta_divides_by_what_was_bought_and_never_by_every_declared_core() {
        let app = router(test_state().await);

        // THE REQUEST NAMES A NETWORK AND NEITHER CHAIN. Both are resolved from
        // the registry — Invariant 2 on this surface, and the reason the path
        // segment differs in kind from its two neighbours.
        let (status, d) = get_json(&app, "/v1/coretime/polkadot/delta?from=100&to=109").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(d["chains"]["occupancy"], "polkadot");
        assert_eq!(d["chains"]["entitlement"], "polkadot-coretime");

        // Attribution: three cores produced, every candidate belongs to the task
        // entitled to it.
        assert_eq!(d["attribution"]["agree_cores"], 3);
        assert_eq!(d["attribution"]["disagree_cores"], 0);
        assert_eq!(d["attribution"]["candidates_total"], 17);
        assert_eq!(d["attribution"]["attributed_candidates"], 17);
        assert_eq!(d["attribution"]["unattributed_candidates"], 0);

        // Census: 4 task-entitled, 6 pooled, nothing unknown.
        assert_eq!(d["entitlement"]["task_entitled_cores"], 4);
        assert_eq!(d["entitlement"]["pool_cores"], 6);
        assert_eq!(d["entitlement"]["cores_without_entitlement"], 0);
        assert_eq!(d["entitlement"]["stable_across_window"], true);

        // THE COMPARISON. 17/40 against 17/100 over the same ten blocks.
        let w = &d["waste"];
        assert_eq!(w["task_entitled_slots"], 40);
        assert_eq!(w["used_by_entitled_task"], 17);
        assert_eq!(w["unused"], 23);
        let entitled_ratio = w["used_ratio"].as_f64().expect("a served waste ratio");
        let (_, o) = get_json(&app, "/v1/coretime/polkadot/occupancy?from=100&to=109").await;
        let slots_filled = o["occupancy"]["slots_filled_ratio"].as_f64().unwrap();
        assert!((entitled_ratio - 0.425).abs() < 1e-9, "{entitled_ratio}");
        assert!((slots_filled - 0.17).abs() < 1e-9, "{slots_filled}");
        assert!(
            entitled_ratio > slots_filled,
            "dividing by what was BOUGHT must not give the same answer as dividing by every \
             declared core: {entitled_ratio} vs {slots_filled}"
        );

        // POOL TIME IS REPORTED AND IS NOT IN THE DENOMINATOR. 60 slots sat
        // beside 40 entitled ones and contributed nothing to the ratio.
        assert_eq!(w["pool_slots"], 60);
        assert_eq!(w["idle_task_cores"], 1);
        // The one idle entitled core is core 3, at or above first_core = 2, so
        // the waste is market-side and no reserved core is idle.
        assert_eq!(w["idle_task_cores_market"], 1);
        assert_eq!(w["idle_task_cores_reserved"], 0);
        // 6 pool + 1 idle task = 7 = the cores producing nothing. The identity
        // slice 11 recorded (43 + 10 = 53) without being able to say why.
        assert_eq!(w["cores_producing_nothing"], 7);

        // A `backed` row is not occupancy. Core 7 carries one and no inclusion,
        // so it must read as an unused POOL core rather than as a used one.
        let core = |i: u64| {
            d["cores"]
                .as_array()
                .unwrap()
                .iter()
                .find(|c| c["core_index"] == i)
                .unwrap_or_else(|| panic!("core {i} present"))
                .clone()
        };
        assert_eq!(core(7)["verdict"], "pool_unused");
        assert_eq!(core(7)["included_blocks"], 0);
        // The per-core ratio that refutes an on-demand reading: core 2 bought a
        // whole core and used one block of ten.
        assert_eq!(core(2)["entitlement_kind"], "task");
        assert_eq!(core(2)["parts"], 57_600);
        assert!((core(2)["used_ratio"].as_f64().unwrap() - 0.1).abs() < 1e-9);
        assert_eq!(core(3)["verdict"], "entitled_unused");

        // Two chains, two storage items, one number — and two heights on two
        // number lines, which the payload states rather than reconciling.
        assert_eq!(d["denominators"]["relay_num_cores"], 10);
        assert_eq!(d["denominators"]["broker_core_count"], 10);
        assert_eq!(d["denominators"]["relay_read_at_height"], 109);
        assert_eq!(d["denominators"]["relay_reading_position"], "inside_window");
        assert_eq!(d["denominators"]["broker_read_at_height"], 4_927_655);
        assert!(
            d["denominators"]["reads_as"].as_str().unwrap().contains("ONE NUMBER"),
            "both readings are present, so the cross-check DID happen"
        );
        assert_eq!(d["checks"]["denominators_agree"], "ok");
        assert_eq!(d["checks"]["entitlement_stable"], "ok");
        assert_eq!(d["checks"]["cores_within_denominator"], "ok");
        assert_eq!(d["checks"]["cores_account_for_the_denominator"], "ok");
        assert!(d["waste_withheld_because"].as_array().unwrap().is_empty());

        // EVERY FIELD THE COVERAGE LIST NAMES MUST EXIST IN THE PAYLOAD. Slice
        // 10's FIX 3 in a new place: denying the machinery in prose is honest
        // and useful, citing fields that are not there is not — a consumer
        // greps for the name and finds nothing. These are the pointers
        // `coretime_delta_not_covered()` sends a reader to.
        for (parent, key) in [
            ("entitlement", "unknown_cores"),
            ("entitlement", "lineage"),
            ("entitlement", "stable_across_window"),
            ("denominators", "relay_num_cores"),
            ("denominators", "broker_core_count"),
            ("market", "read_at_height"),
            ("waste", "pool_slots"),
            ("window", "blocks_indexed"),
            ("window", "heights_with_occupancy"),
            ("window", "contiguous"),
            ("checks", "denominators_agree"),
        ] {
            assert!(
                d[parent].get(key).is_some(),
                "coverage names `{parent}.{key}` and the payload has no such field"
            );
        }

        // An unknown network is a 404 that lists the ones we can answer for,
        // never an empty delta reading as "this network wasted nothing".
        let (status, e) = get_json(&app, "/v1/coretime/kusama/delta?from=1&to=2").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(e["error"].as_str().unwrap().contains("polkadot"));
        // And an unbounded request is refused for the reason its sibling gives.
        let (status, _) = get_json(&app, "/v1/coretime/polkadot/delta").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    /// A CORE WITH NO ASSIGNMENT IS `unknown`, AND THE WASTE FIGURE REFUSES.
    ///
    /// This is the rule 0025 wrote down and nothing enforced until slice 14, and
    /// it is asserted on the RENDERED payload rather than only in the pure
    /// function — because the failure it prevents is a screenshot: a page saying
    /// ten cores were bought and idle when the truth is that our index does not
    /// reach the sale that sold them.
    #[tokio::test]
    async fn cores_with_no_assignment_render_unknown_and_withhold_the_waste_figure() {
        let mut state = test_state().await;
        // The entitlement half indexed for none of the window's cores — exactly
        // what a shallow `broker-range` leaves behind, since `CoreAssigned`
        // fires only at sale boundaries.
        state.broker = Arc::new(MemoryBrokerIndex::new());
        let app = router(state);

        let (status, d) = get_json(&app, "/v1/coretime/polkadot/delta?from=100&to=109").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(d["entitlement"]["cores_without_entitlement"], 10);
        assert_eq!(d["entitlement"]["task_entitled_cores"], 0);

        // NOT IDLE, ANYWHERE IN THE PAYLOAD. The two words are the difference
        // between "we did not look" and "nobody bought it".
        for c in d["cores"].as_array().unwrap() {
            assert_eq!(c["entitlement_kind"], "unknown", "core {}", c["core_index"]);
            assert_ne!(c["verdict"], "entitled_unused");
        }
        assert_eq!(d["cores"].as_array().unwrap().len(), 10);

        // The waste figure is absent and SAYS SO, rather than reading as zero
        // waste — which would be this endpoint's most flattering wrong answer.
        assert!(d["waste"].is_null());
        let why = d["waste_withheld_because"].as_array().unwrap();
        assert!(
            why.iter()
                .any(|s| s.as_str().unwrap().contains("NO assignment at or before the anchor")),
            "{why:?}"
        );
        assert!(d["reads_as"].as_str().unwrap().contains("NO WASTE FIGURE IS SERVED"));

        // The ATTRIBUTION is still served — it is a count over rows we hold, and
        // "0 of 17 attributed" is itself the coverage statement.
        assert_eq!(d["attribution"]["candidates_total"], 17);
        assert_eq!(d["attribution"]["attributed_candidates"], 0);
        assert_eq!(d["attribution"]["unattributed_by_reason"]["unknown"], 17);
        assert_eq!(d["attribution"]["unknown_cores_with_occupancy"], 3);
    }

    /// ASKING BY CORE AND ASKING BY TASK ARE DIFFERENT QUESTIONS, because a
    /// renewal moves the index — measured at coretime 4919882, where para 3428
    /// renewed five cores and every one changed (35->43, 36->44, 37->45, 40->46,
    /// 41->47).
    ///
    /// The fixture puts task 2004's renewal on core 9 while its assignment sits
    /// on core 0, so a reader that answered either question with the other's
    /// rows fails here.
    #[tokio::test]
    async fn an_entitlement_timeline_follows_a_task_even_when_its_core_index_moves() {
        let app = router(test_state().await);

        let (status, t) =
            get_json(&app, "/v1/coretime/polkadot-coretime/entitlement?task=2004").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(t["subject"]["kind"], "task");
        let events = t["events"].as_array().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["variant"], "AutoRenewalEnabled");
        assert_eq!(events[0]["core_index"], 9, "the tenant is on a DIFFERENT core now");
        // Its assignment is still keyed on the core it held.
        assert_eq!(t["assignments"].as_array().unwrap().len(), 1);
        assert_eq!(t["assignments"][0]["core_index"], 0);
        assert_eq!(t["assignments"][0]["parts"], 57_600);

        let (status, c) =
            get_json(&app, "/v1/coretime/polkadot-coretime/entitlement?core=0").await;
        assert_eq!(status, StatusCode::OK);
        let events = c["events"].as_array().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["variant"], "CoreAssigned");
        assert!(
            !events.iter().any(|e| e["variant"] == "AutoRenewalEnabled"),
            "asking by core must not return the renewal that moved the tenant off it"
        );

        // Both, or neither, is a refusal rather than a silent choice — the
        // defect class where a query parameter is accepted and ignored.
        for uri in [
            "/v1/coretime/polkadot-coretime/entitlement?core=0&task=2004",
            "/v1/coretime/polkadot-coretime/entitlement",
        ] {
            let (status, _) = get_json(&app, uri).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}");
        }

        // THE RELAY IS A 404 WITH A REASON. It carries the occupancy half and no
        // broker events at all, so an empty answer here would read as "nobody
        // bought a core" rather than as "wrong chain".
        let (status, e) = get_json(&app, "/v1/coretime/polkadot/entitlement?core=0").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(e["error"].as_str().unwrap().contains("occupancy"));
        let (status, _) = get_json(&app, "/v1/coretime/nosuchchain/entitlement?core=0").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    /// THE SLICE'S PRODUCT CLAIM, ASSERTED AS A PROPERTY RATHER THAN AS WORDING.
    ///
    /// Two ratios, never one. The fixture is built so they cannot come out equal
    /// by accident — 3 of 10 cores produce, filling 17 of 100 slots — and the
    /// test asserts BOTH exist, that they DIFFER, and that no field anywhere in
    /// the payload is called `utilization`. That last assertion is the one that
    /// catches the failure this endpoint exists to prevent: a later change that
    /// helpfully collapses the two into "the" utilization number would keep
    /// every other assertion here green.
    #[tokio::test]
    async fn core_occupancy_serves_two_ratios_and_never_collapses_them_into_one() {
        let app = router(test_state().await);
        let (status, json) = get_json(&app, "/v1/coretime/polkadot/occupancy?from=100&to=109").await;
        assert_eq!(status, StatusCode::OK);

        let occ = &json["occupancy"];
        let touched = occ["cores_touched_ratio"].as_f64().expect("cores touched ratio");
        let filled = occ["slots_filled_ratio"].as_f64().expect("slots filled ratio");
        assert!((touched - 0.30).abs() < 1e-9, "3 of 10 declared cores produced: {touched}");
        assert!((filled - 0.17).abs() < 1e-9, "17 of 100 core-block slots: {filled}");
        assert!(
            touched > filled,
            "the gap between them IS the finding — even the cores that are used sit idle"
        );
        // Both numerator/denominator pairs travel with the ratios, so every
        // figure here is re-derivable rather than taken on trust.
        assert_eq!(occ["cores_that_produced_anything"], 3);
        assert_eq!(occ["included_candidates"], 17);
        assert_eq!(occ["core_block_slots"], 100);

        // `backed` rows are in the window and in `by_kind`, and in NEITHER
        // ratio: async backing means nearly every inclusion has one, so counting
        // them would roughly double every figure above.
        assert_eq!(json["by_kind"]["included"], 17);
        assert_eq!(json["by_kind"]["backed"], 10);

        // No key anywhere in the payload may be called `utilization`.
        fn keys_named(v: &serde_json::Value, name: &str) -> bool {
            match v {
                serde_json::Value::Object(m) => {
                    m.keys().any(|k| k == name) || m.values().any(|x| keys_named(x, name))
                }
                serde_json::Value::Array(a) => a.iter().any(|x| keys_named(x, name)),
                _ => false,
            }
        }
        assert!(
            !keys_named(&json, "utilization"),
            "one `utilization` field would report one of these two ratios and call it the other"
        );

        // The denominator DATES itself: which reading, from which block, and
        // where that block sits relative to the window.
        let den = &json["denominator"];
        assert_eq!(den["num_cores"], 10);
        assert_eq!(den["read_at_height"], 109);
        assert_eq!(den["position"], "inside_window");
        assert_eq!(den["stable_across_window"], true);
        assert_eq!(
            den["max_core_index_observed"], 7,
            "the detector spans EVERY kind: core 7 carries only a `backed` row and is still the \
             highest core the runtime scheduled work onto in this window"
        );
        assert_eq!(
            den["stale_suspected"], false,
            "7 is below the 10 the reading declares, so the denominator is not contradicted"
        );

        // The memory index carries no per-row lineage, so the list is EMPTY —
        // an honest absence rather than a fabricated runtime version. The pg
        // round-trip is what proves a real one comes through.
        assert_eq!(occ["lineage"].as_array().expect("a list, never null").len(), 0);

        // The distribution, which is the shape no marketplace view shows: one
        // saturated core (10/10), one at 60%, one at 10%, seven producing
        // nothing at all.
        let bands = &json["bands"];
        assert_eq!(bands["saturated_over_95"], 1);
        assert_eq!(bands["half_50_to_75"], 1);
        assert_eq!(bands["sparse_5_to_25"], 1);
        assert_eq!(bands["producing_nothing"], 7);

        // The window states what it was computed over, not just what was asked.
        assert_eq!(json["window"]["blocks_indexed"], 10);
        assert_eq!(json["window"]["contiguous"], true);
        assert_eq!(json["window"]["heights_with_occupancy"], 10);

        // A registered PARACHAIN is a 404 with a reason rather than an empty
        // window that would read as "this chain's cores did nothing".
        let (status, json) =
            get_json(&app, "/v1/coretime/polkadot-asset-hub/occupancy?from=100&to=109").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(
            json["error"].as_str().unwrap().contains("RELAY pallet"),
            "the 404 must say why, not just that: {}",
            json["error"]
        );

        // And an unbounded request is refused rather than given a default window
        // nobody chose.
        let (status, _) = get_json(&app, "/v1/coretime/polkadot/occupancy").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        // A WINDOW WE HOLD NO BLOCKS FOR IS UNDEFINED, NOT ZERO — the arm a
        // two-way match would have sent to "no `num_cores` reading is on
        // record" while naming the reading three lines below it.
        let (status, json) = get_json(&app, "/v1/coretime/polkadot/occupancy?from=900&to=910").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["window"]["blocks_indexed"], 0);
        assert!(
            json["occupancy"]["slots_filled_ratio"].is_null(),
            "0/0 has no value, and a fill ratio of zero would be a claim about the network"
        );
        // ITS SIBLING TOO, and this is the one that reads as defensible and is
        // not. `cores_touched_ratio`'s denominator SURVIVES an empty window —
        // 10 cores are declared whether or not we indexed a block — so 0/10 is
        // arithmetically fine and factually says the network sat idle over a
        // range nobody looked at. It is the last screenshot-able 0% on a
        // payload that nulls the fill ratio and the whole band table precisely
        // to prevent one.
        assert!(
            json["occupancy"]["cores_touched_ratio"].is_null(),
            "a touched ratio of zero over 0 indexed blocks is 'we did not look' rendered as \
             'there is nothing there': {}",
            json["occupancy"]
        );
        assert!(
            json["occupancy"]["reads_as"]
                .as_str()
                .unwrap()
                .contains("UNDEFINED, NOT ZERO"),
            "{}",
            json["occupancy"]["reads_as"]
        );
        assert!(
            json["bands"].is_null(),
            "a band table full of zeroes is a distribution somebody would screenshot"
        );
        // The denominator IS on record — it is the window that is missing, and
        // the two claims must not be confused.
        assert_eq!(json["denominator"]["num_cores"], 10);
        assert_eq!(json["denominator"]["position"], "before_window");
        assert!(
            json["denominator"]["stable_across_window"].is_null(),
            "no reading falls inside this window, so nothing here can say the denominator held"
        );
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

    #[tokio::test]
    async fn the_channel_graph_resolves_through_the_registry_with_no_chain_named() {
        let app = router(test_state().await);

        // INVARIANT 2: the caller names a NETWORK and never a chain, and the
        // registry resolves the one chain declaring `hrmp`. If this ever needed
        // a chain id in the URL, the graph would have stopped being a network
        // fact.
        let (status, body) = get_json(&app, "/v1/xcm/polkadot/channels").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["chain"], "polkadot");
        assert_eq!(body["reading"]["block_height"], 30000, "the NEWEST reading by default");
        assert_eq!(body["reading"]["session_index"], 20);
        assert_eq!(body["coverage"]["readings_on_record"], 4);
        assert_eq!(body["coverage"]["header_matches_detail"], true);

        // The gap between session 12 and session 20 is a VALUE, not a silence.
        assert_eq!(body["coverage"]["unread_sessions"], 7);
        assert_eq!(body["coverage"]["unread"][0]["after_session"], 12);
        assert_eq!(body["coverage"]["unread"][0]["before_session"], 20);

        // `at` selects the newest reading at or before a height, and it must not
        // extrapolate: asking at #5000 gives the #4900 reading, where the graph
        // had two channels rather than the one it has now.
        let (_, at) = get_json(&app, "/v1/xcm/polkadot/channels?at=5000").await;
        assert_eq!(at["reading"]["block_height"], 4900);
        assert_eq!(at["edges"].as_array().unwrap().len(), 2);

        // …and asking before the first reading is NOT the same as never having
        // read: the two say different things.
        let (_, early) = get_json(&app, "/v1/xcm/polkadot/channels?at=50").await;
        assert!(early["reading"].is_null());
        let reads_as = early["reads_as"].as_str().unwrap();
        assert!(reads_as.contains("none of them is at or before"), "{reads_as}");
        assert!(
            !reads_as.contains("NO READING"),
            "a covered chain with an early `at` must not claim the index is empty: {reads_as}"
        );

        // A network with no `hrmp` chain 404s rather than serving an empty graph
        // that reads as "this network has no channels".
        let (status, _) = get_json(&app, "/v1/xcm/nosuchnetwork/channels").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn an_edge_history_is_directional_and_dates_only_what_the_readings_pin() {
        let app = router(test_state().await);

        let (status, body) =
            get_json(&app, "/v1/xcm/polkadot/channels/history?sender=1000&recipient=2034").await;
        assert_eq!(status, StatusCode::OK);
        let h = &body["history"];
        // absent -> requested -> open -> absent
        assert_eq!(h["transitions"].as_array().unwrap().len(), 3);
        assert_eq!(h["transitions"][0]["to"], "requested");
        assert_eq!(h["transitions"][1]["from"], "requested");
        assert_eq!(h["transitions"][1]["to"], "open");

        // THE CLAIM THIS ENDPOINT EXISTS TO MAKE HONESTLY: the first two changes
        // sit between adjacent sessions and are pinned to one boundary; the
        // close spans eight sessions and is not.
        assert_eq!(h["transitions"][0]["exact"], true);
        assert_eq!(h["transitions"][1]["exact"], true);
        assert_eq!(h["transitions"][2]["exact"], false, "sessions 12 -> 20 pins nothing");
        assert_eq!(h["transitions"][2]["candidate_boundaries"], 8);
        assert!(
            h["reads_as"].as_str().unwrap().contains("opened AND CLOSED"),
            "a gap must warn that a whole channel lifetime can hide in it: {}",
            h["reads_as"]
        );
        assert!(h["state_at_last_reading"].is_null(), "closed by the newest reading");

        // DIRECTIONALITY, and it is asserted as a PROPERTY rather than as
        // wording: the reverse edge is open throughout and has NO transitions, so
        // a reader that folded the pair would return this instead.
        let (_, rev) =
            get_json(&app, "/v1/xcm/polkadot/channels/history?sender=2034&recipient=1000").await;
        assert!(
            rev["history"]["transitions"].as_array().unwrap().is_empty(),
            "2034 -> 1000 never changed state; folding the pair would have shown 3 changes"
        );
        assert_eq!(rev["history"]["state_at_last_reading"], "open");

        // An edge nobody ever saw says so ABOUT THE CHAIN, because the readings
        // exist — this is the arm that must not read like an unindexed chain.
        let (_, never) =
            get_json(&app, "/v1/xcm/polkadot/channels/history?sender=1000&recipient=9999").await;
        assert_eq!(never["history"]["readings"], 4);
        assert_eq!(never["history"]["readings_present"], 0);
        let reads_as = never["history"]["reads_as"].as_str().unwrap();
        assert!(reads_as.contains("ABSENT"), "{reads_as}");
        assert!(reads_as.contains("statement about the chain"), "{reads_as}");

        // Refusing a half-specified pair is the point, not friction.
        let (status, _) = get_json(&app, "/v1/xcm/polkadot/channels/history?sender=1000").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn the_two_arms_that_justify_the_header_table_both_fire() {
        // THE WHOLE REASON `channel_readings` EXISTS is that "nobody read this
        // chain" and "we read it and the graph was empty" are different facts
        // that both render as a short payload. Neither arm is reachable with the
        // default fixture, and an unreachable arm is one nobody has checked —
        // this project has caught that four times.

        // ARM 1: no reading at all. A statement about OUR INDEX.
        let mut state = test_state().await;
        state.channels = Arc::new(MemoryChannelIndex::new());
        let (status, body) = get_json(&router(state), "/v1/xcm/polkadot/channels").await;
        assert_eq!(status, StatusCode::OK, "an unread chain is not an error");
        assert_eq!(body["coverage"]["readings_on_record"], 0);
        assert!(body["reading"].is_null());
        let reads_as = body["reads_as"].as_str().unwrap();
        assert!(reads_as.contains("NO READING"), "{reads_as}");
        assert!(reads_as.contains("OUR INDEX"), "{reads_as}");
        assert!(reads_as.contains("channels-range"), "and it says what to run: {reads_as}");

        // ARM 3: a reading exists and the graph was empty. A statement about the
        // CHAIN, and it must not borrow arm 1's wording.
        let mut state = test_state().await;
        let empty = MemoryChannelIndex::new();
        empty.insert_reading(
            "polkadot",
            ChannelReadingRow {
                block_height: 7,
                session_index: 3,
                channel_count: 0,
                open_request_count: 0,
                topology_digest: "0x00".into(),
                spec_version: 2003002,
                source: "test".into(),
            },
        );
        state.channels = Arc::new(empty);
        let (_, body) = get_json(&router(state), "/v1/xcm/polkadot/channels").await;
        assert_eq!(body["coverage"]["readings_on_record"], 1);
        assert_eq!(body["edges"].as_array().unwrap().len(), 0);
        let reads_as = body["reads_as"].as_str().unwrap();
        assert!(reads_as.contains("graph was EMPTY"), "{reads_as}");
        assert!(reads_as.contains("not a gap"), "{reads_as}");
        assert!(!reads_as.contains("NO READING"), "the two arms must not share wording");
        assert_eq!(
            body["coverage"]["header_matches_detail"], true,
            "zero and zero agree"
        );
    }

    #[tokio::test]
    async fn a_header_that_disagrees_with_its_detail_says_so_in_prose() {
        // The migration calls a header/detail disagreement "a loud defect rather
        // than a silent one". A boolean buried in `coverage` is not loud, so the
        // sentence has to carry it too — otherwise the payload cheerfully prints
        // "3 open channel(s)" beside an empty edge list.
        let mut state = test_state().await;
        let lying = MemoryChannelIndex::new();
        lying.insert_reading(
            "polkadot",
            ChannelReadingRow {
                block_height: 7,
                session_index: 3,
                channel_count: 3,
                open_request_count: 0,
                topology_digest: "0x00".into(),
                spec_version: 2003002,
                source: "test".into(),
            },
        );
        // …and deliberately no edges.
        state.channels = Arc::new(lying);
        let (_, body) = get_json(&router(state), "/v1/xcm/polkadot/channels").await;
        assert_eq!(body["coverage"]["header_matches_detail"], false);
        let reads_as = body["reads_as"].as_str().unwrap();
        assert!(reads_as.contains("DEFECT"), "{reads_as}");
        assert!(
            reads_as.contains("unreliable"),
            "the prose must tell the reader not to trust the counts: {reads_as}"
        );
    }

    #[tokio::test]
    async fn the_newest_reading_says_that_later_sessions_are_unread() {
        // The blocker this endpoint shipped with: it claimed the graph was exact
        // "for every later session with no reading of its own" and pointed at
        // `coverage.unread`, which structurally cannot contain those sessions.
        // Extrapolating FORWARD is the same sin as extrapolating backward, which
        // the arm two above explicitly refuses.
        let app = router(test_state().await);
        let (_, body) = get_json(&app, "/v1/xcm/polkadot/channels").await;
        let reads_as = body["reads_as"].as_str().unwrap();
        assert!(
            reads_as.contains("WHOLE of session"),
            "a reading is exact for its own session: {reads_as}"
        );
        assert!(
            reads_as.contains("NOTHING about any later session"),
            "…and for no other: {reads_as}"
        );
        assert!(
            reads_as.contains("does not list those"),
            "and it must say `unread` cannot cover the sessions after the last reading: {reads_as}"
        );
        assert!(
            !reads_as.contains("exact for session"),
            "the old forward-extrapolating wording must be gone, not merely joined"
        );
    }

    #[tokio::test]
    async fn no_channel_payload_carries_a_message_count_or_a_backlog() {
        // The schema decision, enforced from OUTSIDE the schema. `msg_count`,
        // `total_size` and `mqc_head` are message throughput and were measured
        // moving 16 of 224 rows per session with the topology unchanged; a later
        // change that helpfully surfaced them would make every reading look like
        // a topology change, and every other assertion here would stay green.
        fn key_named(v: &serde_json::Value, names: &[&str]) -> Option<String> {
            match v {
                serde_json::Value::Object(m) => {
                    for (k, inner) in m {
                        if names.contains(&k.as_str()) {
                            return Some(k.clone());
                        }
                        if let Some(hit) = key_named(inner, names) {
                            return Some(hit);
                        }
                    }
                    None
                }
                serde_json::Value::Array(items) => items.iter().find_map(|i| key_named(i, names)),
                _ => None,
            }
        }
        let app = router(test_state().await);
        let banned = ["msg_count", "total_size", "mqc_head", "backlog", "queue_depth"];
        for uri in [
            "/v1/xcm/polkadot/channels",
            "/v1/xcm/polkadot/channels/history?sender=1000&recipient=2034",
        ] {
            let (_, body) = get_json(&app, uri).await;
            assert_eq!(
                key_named(&body, &banned),
                None,
                "{uri} must carry no throughput field"
            );
        }
    }
}
