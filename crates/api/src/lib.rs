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
}

#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<Registry>,
    pub blocks: Arc<dyn BlockIndex>,
    pub labels: Arc<dyn LabelIndex>,
    pub balances: Arc<dyn BalanceIndex>,
    pub parse_account: AccountParser,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/chains", get(list_chains))
        .route("/v1/blocks/{chain}/{height}", get(get_block))
        .route("/v1/accounts/{chain}/{account}/labels", get(get_labels))
        .route("/v1/balances/{network}/{account}/history", get(get_balance_history))
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

        AppState {
            registry,
            blocks,
            labels,
            balances,
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
