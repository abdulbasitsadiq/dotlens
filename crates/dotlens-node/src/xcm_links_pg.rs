//! Postgres sink for XCM id links (Phase 3, slice 3).
//!
//! DELETE-then-INSERT per block, in one transaction — and it is the only sink in
//! the project that DELETEs. The reason is what a link IS: every other fact
//! table records something a chain said, so a re-run must be a no-op; this one
//! records something WE CONCLUDED under a rule that carries a version, so a
//! re-run after a rule change must be able to replace the conclusion. An
//! append-only link table would serve the old inference forever and the
//! `correlator_version` column would be decoration.
//!
//! WHICH IS EXACTLY WHY IT NEEDS THE ADVISORY LOCK the other sinks do not.
//! Insert-ignore is idempotent under concurrency for free; delete-then-insert is
//! not. Two writers on one block — `xcm-correlate <range>` run while
//! `XCM_CORRELATE_FOLLOW=1` covers the same height — would have the second
//! transaction take its DELETE snapshot before the first commits, delete zero
//! rows, and then collide with the first's committed primary key: a unique
//! violation that halts a worker rather than converging. Same serialization
//! point, and the same reasoning, as `PgBlockIndex::insert`'s unfinalized
//! replacement (Phase 1, slice 6).
//!
//! The one asymmetry, stated here and in migration 0016 rather than discovered:
//! the shared worker runtime does not call a sink with zero rows, so a rule that
//! stops firing on a block leaves that block's old link in place. Re-running a
//! NARROWED rule wants `delete from xcm.message_links where correlator_version <
//! <new>` first.

use anyhow::{Context, Result};
use async_trait::async_trait;
use ingest::xcm_correlate::{XcmLink, XcmLinkSink};
use sqlx::PgPool;

pub struct PgXcmLinkSink {
    pool: PgPool,
}

impl PgXcmLinkSink {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl XcmLinkSink for PgXcmLinkSink {
    async fn write(
        &self,
        chain_id: &str,
        height: u64,
        runtime_version: u32,
        correlator_version: u32,
        rows: &[(u32, XcmLink)],
    ) -> Result<(), String> {
        write_links(
            &self.pool,
            chain_id,
            height,
            runtime_version,
            correlator_version,
            rows,
        )
        .await
        .map_err(|e| e.to_string())
    }
}

pub async fn write_links(
    pool: &PgPool,
    chain_id: &str,
    height: u64,
    runtime_version: u32,
    correlator_version: u32,
    rows: &[(u32, XcmLink)],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    // The table is keyed by the wire event index and uniquely indexed on the
    // topic event index, so a rule that produced two links for one end would hit
    // a constraint mid-transaction and roll the whole block back. Refuse here
    // instead, where the message can name the rule that did it (the votes sink's
    // rule from Phase 2 slice 3).
    let mut wire_seen = std::collections::HashSet::new();
    let mut topic_seen = std::collections::HashSet::new();
    for (wire_index, link) in rows {
        anyhow::ensure!(
            wire_seen.insert(*wire_index),
            "two XCM links at {chain_id}/{height} wire event {wire_index} — one queued message \
             is delivered once, so the correlator produced a pairing that cannot be true"
        );
        anyhow::ensure!(
            topic_seen.insert(link.topic_event_index),
            "two XCM links at {chain_id}/{height} share topic event {} — a topic belongs to at \
             most one queued message",
            link.topic_event_index
        );
    }

    let mut tx = pool.begin().await.context("begin xcm link tx")?;
    // The serialization point for (chain, height) — see the module header. A
    // second writer must WAIT here, not take a snapshot that will make its
    // DELETE a no-op and its INSERT a unique violation.
    sqlx::query("select pg_advisory_xact_lock(hashtextextended($1, $2))")
        .bind(chain_id)
        .bind(height as i64)
        .execute(&mut *tx)
        .await
        .with_context(|| format!("locking xcm links {chain_id}/{height}"))?;
    // A block's links are a pure function of that block under the current rule.
    // Clearing first is what makes a re-run converge instead of accumulating.
    sqlx::query("delete from xcm.message_links where chain_id = $1 and block_height = $2")
        .bind(chain_id)
        .bind(height as i64)
        .execute(&mut *tx)
        .await
        .with_context(|| format!("clearing xcm links {chain_id}/{height}"))?;

    for (wire_event_index, l) in rows {
        sqlx::query(
            "insert into xcm.message_links ( \
                 chain_id, block_height, wire_event_index, topic_event_index, wire_hash, topic, \
                 transport, rule, confidence, evidence, runtime_version, correlator_version) \
             values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
        )
        .bind(chain_id)
        .bind(height as i64)
        .bind(*wire_event_index as i32)
        .bind(l.topic_event_index as i32)
        .bind(&l.wire_hash)
        .bind(&l.topic)
        .bind(&l.transport)
        .bind(&l.rule)
        .bind(&l.confidence)
        .bind(&l.evidence)
        .bind(runtime_version as i64)
        .bind(correlator_version as i32)
        .execute(&mut *tx)
        .await
        .with_context(|| format!("inserting xcm link {chain_id}/{height}/{wire_event_index}"))?;
    }
    tx.commit().await.context("commit xcm link tx")?;
    Ok(())
}
