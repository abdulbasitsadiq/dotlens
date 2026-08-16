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
}

#[derive(Default)]
pub struct MemoryTreasuryIndex {
    spends: RwLock<HashMap<(String, String, String, u64), SpendRow>>,
    events: RwLock<HashMap<(String, String), Vec<(Option<(String, u64)>, SpendEventRow)>>>,
}

impl MemoryTreasuryIndex {
    pub fn new() -> Self {
        Self::default()
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
    votes: RwLock<HashMap<String, Vec<VoteRow>>>,
    delegations: RwLock<HashMap<String, Vec<DelegationRow>>>,
    anchors: RwLock<HashMap<(String, String), Vec<VotingAnchorRow>>>,
}

impl MemoryGovIndex {
    pub fn new() -> Self {
        Self::default()
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
            let rows: Vec<(i64, String, String, String, Option<i64>, String, Option<String>)> =
                sqlx::query_as(
                    "select block_height, free::text, reserved::text, total::text, \
                            spec_version, source, note \
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
                .map(|(height, free, reserved, total, spec, source, note)| {
                    super::BalanceAnchorRow {
                        height: height as u64,
                        free,
                        reserved,
                        total,
                        spec_version: spec.map(|s| s as u64),
                        source,
                        note,
                    }
                })
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

    const SPEND_COLS: &str = "instance, spend_kind, spend_id, status, amount::text, \
                              slashed::text, asset_kind, beneficiary, beneficiary_location, \
                              payment_id, valid_from, expire_at, first_seen_height, status_height";

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
        .route("/v1/treasury/{network}/spends", get(list_treasury_spends))
        .route("/v1/treasury/{network}/spends/{id}", get(get_treasury_spend))
        .route("/v1/treasury/{network}/pot", get(get_treasury_pot))
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
fn all_gov_windows<'a>(
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

    Json(serde_json::json!({
        "network": network,
        "class": class,
        "referendum_id": id,
        "referendum": referendum,
        "preimage": preimage,
        "segments": segments,
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
/// columns fall back to the earlier one; the first sighting is the earliest.
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
        first_seen_height: prev.first_seen_height.min(later.first_seen_height),
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
    Json(serde_json::json!({
        "network": network,
        "instance": instance,
        "spend_kind": kind,
        "spend_id": id,
        "spend": spend,
        "segments": segments,
    }))
    .into_response()
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
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use std::path::Path as FsPath;
    use tower::util::ServiceExt;

    async fn test_state() -> AppState {
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
                first_seen_height: 5_100_000,
                status_height: 5_100_500,
            },
        );

        AppState {
            registry,
            blocks,
            labels,
            balances,
            gov,
            treasury,
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
        assert_eq!(json["spend"]["first_seen_height"], 10_400_000);
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
