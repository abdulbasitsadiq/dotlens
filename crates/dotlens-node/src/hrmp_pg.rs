//! Postgres writer for HRMP channel readings (Phase 3, slice 16).
//!
//! ONE TRANSACTION PER READING, header first, detail second. That order is
//! load-bearing rather than tidy: `xcm.channel_snapshots` carries a foreign key
//! onto `xcm.channel_readings`, so the detail structurally cannot exist without
//! the header that says we looked. A crash between the two leaves a header with
//! no detail, which would read as "the graph was empty" — hence the transaction,
//! and hence `channel_count` on the header, which a reader can compare against
//! the detail row count to catch exactly that.
//!
//! IMMUTABLE PER (chain, height), like every other dated reading in the project.
//! `on conflict do nothing` on both tables: a second read of the same historic
//! state can only agree, and if it would not agree that is a defect to find
//! rather than an update to apply. This is also what makes a re-run of
//! `channels-range` a genuine no-op, which every slice since Phase 1 verifies.
//!
//! There is no projection and no ordering guard, because there is nothing to
//! converge: unlike `treasury.spends` or `gov.referenda`, a reading is not a
//! subject accumulating verdicts over time. It is one observation. The
//! open/close HISTORY that a caller actually wants is the DIFFERENCE between
//! consecutive readings, and it is computed at read time and never stored —
//! 0027's header has the argument, which is the sixth outing for the one that
//! killed `treasury.consolidated_position`.

use anyhow::{Context, Result};
use sqlx::PgPool;

use adapter_substrate::hrmp::ChannelEdge;

/// Everything one reading records. Assembled by the caller from two enumerated
/// prefixes plus `Session.CurrentIndex`, all read at ONE block hash.
pub struct ChannelReading<'a> {
    pub chain_id: &'a str,
    pub block_height: u64,
    pub session_index: u64,
    pub topology_digest: &'a str,
    pub spec_version: u32,
    pub source: &'a str,
    pub edges: &'a [ChannelEdge],
}

/// Write one reading. Returns `true` if it was newly recorded, `false` if this
/// (chain, height) had already been read — so a caller can print "recorded" vs
/// "already on record" instead of guessing.
pub async fn insert_channel_reading(pool: &PgPool, reading: ChannelReading<'_>) -> Result<bool> {
    let ChannelReading {
        chain_id,
        block_height,
        session_index,
        topology_digest,
        spec_version,
        source,
        edges,
    } = reading;

    // Counted, never estimated — and counted from the SAME slice the detail rows
    // come from, so the header cannot disagree with its own detail.
    let channel_count = edges.iter().filter(|e| e.is_open()).count();
    let open_request_count = edges.len() - channel_count;

    let mut tx = pool.begin().await.context("opening the channel-reading transaction")?;

    let header = sqlx::query(
        "insert into xcm.channel_readings \
             (chain_id, block_height, session_index, channel_count, open_request_count, \
              topology_digest, spec_version, source) \
         values ($1,$2,$3,$4,$5,$6,$7,$8) \
         on conflict (chain_id, block_height) do nothing",
    )
    .bind(chain_id)
    .bind(block_height as i64)
    .bind(session_index as i64)
    .bind(channel_count as i32)
    .bind(open_request_count as i32)
    .bind(topology_digest)
    .bind(spec_version as i64)
    .bind(source)
    .execute(&mut *tx)
    .await
    .with_context(|| format!("recording the channel reading {chain_id}@{block_height}"))?;

    // A replay stops here — but NOT silently. The header comment above claims a
    // second read of the same historic state can only agree and that a
    // disagreement is a defect to find; that claim is worth nothing unless
    // something actually compares. So compare: the digest is a blake2_256 over
    // the sorted open edge set, and the two counts are counted from the same
    // slice the detail comes from, so between them they catch any real
    // difference in what the chain said.
    //
    // This is the raw store's rule applied to a table: `FsRawStore` refuses to
    // overwrite an immutable object and HALTS when the bytes differ, which is
    // how slice 8 discovered a port embedded in an archived answer. A reading
    // that disagrees with itself means the height was re-read against different
    // state, or the reader changed rules without bumping HRMP_READER_VERSION,
    // and either is worth stopping for.
    if header.rows_affected() == 0 {
        let existing: Option<(String, i32, i32)> = sqlx::query_as(
            "select topology_digest, channel_count, open_request_count \
             from xcm.channel_readings where chain_id = $1 and block_height = $2",
        )
        .bind(chain_id)
        .bind(block_height as i64)
        .fetch_optional(&mut *tx)
        .await
        .with_context(|| format!("re-reading the recorded channel reading {chain_id}@{block_height}"))?;
        tx.rollback().await.ok();

        if let Some((had_digest, had_channels, had_requests)) = existing {
            anyhow::ensure!(
                had_digest == topology_digest
                    && had_channels == channel_count as i32
                    && had_requests == open_request_count as i32,
                "the channel reading already on record for {chain_id}@{block_height} DISAGREES with \
                 what was just read from the chain. On record: digest {had_digest}, \
                 {had_channels} channel(s), {had_requests} request(s). Just read: digest \
                 {topology_digest}, {channel_count} channel(s), {open_request_count} request(s). \
                 A reading is immutable per (chain, height) because re-reading the same historic \
                 state can only agree — so this is a defect to find, not an update to apply. \
                 Either the two runs read different state at one height, or the reader's rules \
                 changed without HRMP_READER_VERSION moving."
            );
        }
        return Ok(false);
    }

    for e in edges {
        sqlx::query(
            "insert into xcm.channel_snapshots \
                 (chain_id, block_height, sender, recipient, state, \
                  max_capacity, max_total_size, max_message_size, \
                  sender_deposit, recipient_deposit, confirmed) \
             values ($1,$2,$3,$4,$5,$6,$7,$8,$9::numeric,$10::numeric,$11) \
             on conflict (chain_id, block_height, sender, recipient) do nothing",
        )
        .bind(chain_id)
        .bind(block_height as i64)
        // u32 into i64: a ParaId is a u32 and i32 cannot hold all of them. The
        // column is bigint for the same reason (0027), so no range check is
        // needed and none is written — a gate that can never fire is a gate
        // nobody has checked.
        .bind(e.sender as i64)
        .bind(e.recipient as i64)
        .bind(e.state)
        // u32 -> i64, not i32: the columns are bigint precisely so a limit above
        // 2^31 cannot store negative and read back as a plausible huge number.
        .bind(e.max_capacity as i64)
        .bind(e.max_total_size as i64)
        .bind(e.max_message_size as i64)
        // u128 has no sqlx binding; NUMERIC is bound as TEXT and cast, the same
        // way `balances.balance_changes` and `treasury.spends` carry planck.
        .bind(e.sender_deposit.to_string())
        .bind(e.recipient_deposit.map(|d| d.to_string()))
        .bind(e.confirmed)
        .execute(&mut *tx)
        .await
        .with_context(|| {
            format!(
                "recording channel {} -> {} ({}) at {chain_id}@{block_height}",
                e.sender, e.recipient, e.state
            )
        })?;
    }

    tx.commit().await.context("committing the channel reading")?;
    Ok(true)
}

/// The heights at which this chain's graph has already been read, ascending.
/// Used by `channels-range` to skip session boundaries already on record without
/// paying for a state read to discover it.
pub async fn read_heights(pool: &PgPool, chain_id: &str, from: u64, to: u64) -> Result<Vec<u64>> {
    let rows: Vec<(i64,)> = sqlx::query_as(
        "select block_height from xcm.channel_readings \
         where chain_id = $1 and block_height between $2 and $3 \
         order by block_height",
    )
    .bind(chain_id)
    .bind(from as i64)
    .bind(to as i64)
    .fetch_all(pool)
    .await
    .with_context(|| format!("listing channel readings for {chain_id}"))?;
    Ok(rows.into_iter().map(|(h,)| h as u64).collect())
}
