//! Postgres backends for the gov worker: timeline entries into
//! `gov.referendum_events` (insert-ignore) + the `gov.referenda` projection
//! (ordering-guarded upsert — replaying any range in any order converges),
//! atomically per block. Plus `sync_tracks`: decode every governance-capable
//! chain's track definitions from its own archived metadata into `gov.tracks`
//! (generate, don't curate — the sync_labels doctrine).
//!
//! The canonical-event SOURCE is `balances_pg::PgEventSource` — the gov worker
//! shares the balances worker's EventSource contract by design.

use adapter_substrate::gov::tracks_from_metadata;
use anyhow::{Context, Result};
use async_trait::async_trait;
use ingest::gov::{RefTimelineEntry, TimelineSink};
use raw_store::RawStore;
use registry::{ChainFamily, Registry};
use sqlx::PgPool;

pub struct PgTimelineSink {
    pool: PgPool,
}

impl PgTimelineSink {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl TimelineSink for PgTimelineSink {
    async fn write(
        &self,
        chain_id: &str,
        height: u64,
        runtime_version: u32,
        mapper_version: u32,
        rows: &[(u32, RefTimelineEntry)],
    ) -> Result<(), String> {
        let mut tx = self.pool.begin().await.map_err(|e| e.to_string())?;
        // deterministic upsert order (class, referendum, event): the projection
        // uses DO UPDATE (row locks) — concurrent gov-range + follower txs
        // touching the same referenda in different orders could deadlock;
        // a global lock order prevents it (review catch)
        let mut rows: Vec<&(u32, RefTimelineEntry)> = rows.iter().collect();
        rows.sort_by(|(ai, a), (bi, b)| {
            (&a.class, a.referendum_id, ai).cmp(&(&b.class, b.referendum_id, bi))
        });
        for (event_index, e) in rows {
            sqlx::query(
                "insert into gov.referendum_events \
                     (chain_id, class, referendum_id, block_height, event_index, \
                      kind, data, runtime_version, mapper_version) \
                 values ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
                 on conflict (chain_id, class, referendum_id, block_height, event_index) \
                 do nothing",
            )
            .bind(chain_id)
            .bind(&e.class)
            .bind(e.referendum_id as i64)
            .bind(height as i64)
            .bind(*event_index as i32)
            .bind(&e.kind)
            .bind(&e.data)
            .bind(runtime_version as i64)
            .bind(mapper_version as i32)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;

            let submitted_at = (e.kind == "submitted").then_some(height as i64);
            match &e.status {
                // status-bearing: the ordering guard — a strictly newer
                // (height, event_index) moves status; older/equal replays
                // leave it. 'unknown' placeholders sit at (0,0) so any real
                // status wins. Info fields coalesce (first observation sticks;
                // replay converges).
                Some(status) => {
                    sqlx::query(
                        "insert into gov.referenda \
                             (chain_id, class, referendum_id, track_id, proposal, \
                              proposal_hash, proposal_len, submitted_at_height, \
                              status, status_height, status_event_index, \
                              runtime_version, mapper_version) \
                         values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) \
                         on conflict (chain_id, class, referendum_id) do update set \
                             track_id = coalesce(gov.referenda.track_id, excluded.track_id), \
                             proposal = coalesce(gov.referenda.proposal, excluded.proposal), \
                             proposal_hash = coalesce(gov.referenda.proposal_hash, excluded.proposal_hash), \
                             proposal_len = coalesce(gov.referenda.proposal_len, excluded.proposal_len), \
                             submitted_at_height = coalesce(gov.referenda.submitted_at_height, excluded.submitted_at_height), \
                             status = case when (excluded.status_height, excluded.status_event_index) \
                                                > (gov.referenda.status_height, gov.referenda.status_event_index) \
                                           then excluded.status else gov.referenda.status end, \
                             status_height = case when (excluded.status_height, excluded.status_event_index) \
                                                       > (gov.referenda.status_height, gov.referenda.status_event_index) \
                                                  then excluded.status_height else gov.referenda.status_height end, \
                             status_event_index = case when (excluded.status_height, excluded.status_event_index) \
                                                            > (gov.referenda.status_height, gov.referenda.status_event_index) \
                                                       then excluded.status_event_index else gov.referenda.status_event_index end, \
                             runtime_version = case when (excluded.status_height, excluded.status_event_index) \
                                                         > (gov.referenda.status_height, gov.referenda.status_event_index) \
                                                    then excluded.runtime_version else gov.referenda.runtime_version end, \
                             mapper_version = case when (excluded.status_height, excluded.status_event_index) \
                                                        > (gov.referenda.status_height, gov.referenda.status_event_index) \
                                                   then excluded.mapper_version else gov.referenda.mapper_version end, \
                             updated_at = now()",
                    )
                    .bind(chain_id)
                    .bind(&e.class)
                    .bind(e.referendum_id as i64)
                    .bind(e.track_id.map(|t| t as i32))
                    .bind(&e.proposal)
                    .bind(&e.proposal_hash)
                    .bind(e.proposal_len.map(|l| l as i64))
                    .bind(submitted_at)
                    .bind(status)
                    .bind(height as i64)
                    .bind(*event_index as i32)
                    .bind(runtime_version as i64)
                    .bind(mapper_version as i32)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| e.to_string())?;
                }
                // informational: never moves status; ensures the row exists
                // (a refund can be the first thing we see for a ref whose
                // status events live in a not-yet-mapped range)
                None => {
                    sqlx::query(
                        "insert into gov.referenda \
                             (chain_id, class, referendum_id, track_id, proposal, \
                              proposal_hash, proposal_len, submitted_at_height, \
                              status, status_height, status_event_index, \
                              runtime_version, mapper_version) \
                         values ($1,$2,$3,$4,$5,$6,$7,$8,'unknown',0,0,$9,$10) \
                         on conflict (chain_id, class, referendum_id) do update set \
                             track_id = coalesce(gov.referenda.track_id, excluded.track_id), \
                             proposal = coalesce(gov.referenda.proposal, excluded.proposal), \
                             proposal_hash = coalesce(gov.referenda.proposal_hash, excluded.proposal_hash), \
                             proposal_len = coalesce(gov.referenda.proposal_len, excluded.proposal_len), \
                             submitted_at_height = coalesce(gov.referenda.submitted_at_height, excluded.submitted_at_height), \
                             updated_at = now()",
                    )
                    .bind(chain_id)
                    .bind(&e.class)
                    .bind(e.referendum_id as i64)
                    .bind(e.track_id.map(|t| t as i32))
                    .bind(&e.proposal)
                    .bind(&e.proposal_hash)
                    .bind(e.proposal_len.map(|l| l as i64))
                    .bind(submitted_at)
                    .bind(runtime_version as i64)
                    .bind(mapper_version as i32)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| e.to_string())?;
                }
            }
        }
        tx.commit().await.map_err(|e| e.to_string())
    }
}

// ---------------------------------------------------------------- preimages

/// One gov.preimages row, write-shaped.
#[derive(Debug, Clone)]
pub struct PreimageRecord {
    pub proposal_hash: String, // 0x-hex
    pub len: u64,
    pub bytes_location: Option<String>,
    pub decoded_call: Option<serde_json::Value>,
    pub call_summary: Option<String>,
    pub decode_status: String, // decoded | missing | undecodable
    pub source: String,        // state | inline
    pub note: Option<String>,
    pub spec_version: Option<u32>,
    pub decoder_version: u32,
    pub fetched_at_height: Option<u64>,
}

/// Upsert one preimage row. A 'decoded' row is never downgraded (a later
/// 'missing' read — preimages are cleared after enactment — must not erase a
/// successful decode); anything else is overwritten by the latest attempt.
pub async fn upsert_preimage(pool: &PgPool, chain_id: &str, r: &PreimageRecord) -> Result<()> {
    sqlx::query(
        "insert into gov.preimages \
             (chain_id, proposal_hash, len, bytes_location, decoded_call, call_summary, \
              decode_status, source, note, spec_version, decoder_version, fetched_at_height) \
         values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) \
         on conflict (chain_id, proposal_hash, len) do update set \
             bytes_location = coalesce(excluded.bytes_location, gov.preimages.bytes_location), \
             decoded_call = excluded.decoded_call, call_summary = excluded.call_summary, \
             decode_status = excluded.decode_status, source = excluded.source, \
             note = excluded.note, spec_version = excluded.spec_version, \
             decoder_version = excluded.decoder_version, \
             fetched_at_height = excluded.fetched_at_height, updated_at = now() \
         where gov.preimages.decode_status is distinct from 'decoded' \
            or excluded.decode_status = 'decoded'",
    )
    .bind(chain_id)
    .bind(&r.proposal_hash)
    .bind(r.len as i64)
    .bind(&r.bytes_location)
    .bind(&r.decoded_call)
    .bind(&r.call_summary)
    .bind(&r.decode_status)
    .bind(&r.source)
    .bind(&r.note)
    .bind(r.spec_version.map(|s| s as i64))
    .bind(r.decoder_version as i32)
    .bind(r.fetched_at_height.map(|h| h as i64))
    .execute(pool)
    .await
    .with_context(|| format!("upserting preimage {}", r.proposal_hash))?;
    Ok(())
}

/// Referenda whose proposals still need preimage work on this chain:
/// every row with a proposal, minus those already 'decoded' and minus legacy
/// sentinels (a democracy-era proposal can never reach 'decoded' — without
/// the exclusion it would churn on every run, review catch).
/// Returns (class, referendum_id, proposal JSON, proposal_hash, proposal_len,
/// submitted_at_height).
#[allow(clippy::type_complexity)]
pub async fn referenda_needing_preimages(
    pool: &PgPool,
    chain_id: &str,
) -> Result<Vec<(String, i64, serde_json::Value, Option<String>, Option<i64>, Option<i64>)>> {
    sqlx::query_as(
        "select r.class, r.referendum_id, r.proposal, r.proposal_hash, r.proposal_len, \
                r.submitted_at_height \
         from gov.referenda r \
         where r.chain_id = $1 and r.proposal is not null \
           and not exists (select 1 from gov.preimages p \
                           where p.chain_id = r.chain_id \
                             and p.proposal_hash = r.proposal_hash \
                             and (p.decode_status = 'decoded' \
                                  or (p.len = 0 and p.note like 'legacy%'))) \
         order by r.referendum_id",
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await
    .context("listing referenda needing preimages")
}

/// Fill proposal_hash/len for an Inline proposal after hashing its bytes —
/// coalesce-only (never overwrites a mapper-written value), so the preimage
/// join works for inline referenda too.
pub async fn fill_inline_proposal_hash(
    pool: &PgPool,
    chain_id: &str,
    class: &str,
    referendum_id: i64,
    hash: &str,
    len: u64,
) -> Result<()> {
    sqlx::query(
        "update gov.referenda set \
             proposal_hash = coalesce(proposal_hash, $4), \
             proposal_len = coalesce(proposal_len, $5), updated_at = now() \
         where chain_id = $1 and class = $2 and referendum_id = $3",
    )
    .bind(chain_id)
    .bind(class)
    .bind(referendum_id)
    .bind(hash)
    .bind(len as i64)
    .execute(pool)
    .await
    .with_context(|| format!("filling inline hash for {chain_id}/{class}/{referendum_id}"))?;
    Ok(())
}

// --------------------------------------------------------------- track sync

#[derive(Debug, Default)]
pub struct TrackSyncReport {
    pub tracks: usize,
    /// Chains skipped this sync: no archived metadata yet, blob unreadable, or
    /// Tracks walk failed (the log line distinguishes which) — their tracks
    /// appear on a later sync once the cause clears.
    pub chains_skipped: Vec<String>,
}

/// Decode + project track definitions for every substrate chain with the
/// `governance` module, from the latest archived metadata. Idempotent
/// upsert-only; runs at every persistent node start (like sync_labels).
/// A chain whose current runtime has no referenda pallet (e.g. the post-
/// migration relay) simply yields zero tracks — honest data, not an error.
///
/// NOTE (Collectives registration): this walks EVERY pallet with a `Tracks`
/// constant, so Collectives contributes `fellowshipreferenda` AND
/// `ambassadorreferenda` rows. The ambassador instance has no class in the
/// mapper and no residency entry yet, so its tracks are stored but not served —
/// honest extra data, and the asymmetry disappears when a slice maps it.
///
/// Accepted judgments (reviewed): (1) only the LATEST metadata is consulted —
/// historical relay-era tracks stay empty until a slice adds per-spec track
/// history (gov.tracks would need spec_version in its PK); (2) upsert-only —
/// a track removed by a runtime upgrade lingers with its old spec_version,
/// same doctrine as sync_labels (historic definitions stay valuable; OpenGov's
/// track set has been stable since 2023).
pub async fn sync_tracks(
    pool: &PgPool,
    registry: &Registry,
    raw: &dyn RawStore,
) -> Result<TrackSyncReport> {
    let mut report = TrackSyncReport::default();
    let mut tx = pool.begin().await.context("begin track sync")?;

    for chain in registry
        .chains()
        .filter(|c| c.family == ChainFamily::Substrate && c.has_module("governance"))
    {
        let row: Option<(i64, String)> = sqlx::query_as(
            "select spec_version, metadata_blob_location from substrate.runtime_versions \
             where chain_id = $1 and metadata_blob_location is not null \
             order by spec_version desc limit 1",
        )
        .bind(&chain.id)
        .fetch_optional(&mut *tx)
        .await
        .with_context(|| format!("looking up metadata for {}", chain.id))?;
        let Some((spec_version, location)) = row else {
            tracing::debug!(chain = %chain.id, "no archived metadata yet — tracks skipped");
            report.chains_skipped.push(chain.id.clone());
            continue;
        };
        let blob = match raw.get(&location) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(chain = %chain.id, %location, error = %e,
                    "metadata blob listed but unreadable — skipping tracks");
                report.chains_skipped.push(chain.id.clone());
                continue;
            }
        };
        // a walk failure on ONE chain must not block the node or other chains
        let tracks = match tracks_from_metadata(&blob) {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(chain = %chain.id, error = %e,
                    "Tracks walk failed — skipping tracks for this chain");
                report.chains_skipped.push(chain.id.clone());
                continue;
            }
        };
        for t in &tracks {
            sqlx::query(
                "insert into gov.tracks (chain_id, pallet, track_id, name, params, spec_version) \
                 values ($1, $2, $3, $4, $5, $6) \
                 on conflict (chain_id, pallet, track_id) do update set \
                     name = excluded.name, params = excluded.params, \
                     spec_version = excluded.spec_version, updated_at = now()",
            )
            .bind(&chain.id)
            .bind(&t.pallet)
            .bind(t.track_id as i32)
            .bind(&t.name)
            .bind(&t.params)
            .bind(spec_version)
            .execute(&mut *tx)
            .await
            .with_context(|| format!("upserting track {}/{}", chain.id, t.track_id))?;
            report.tracks += 1;
        }
    }

    tx.commit().await.context("commit track sync")?;
    Ok(report)
}
