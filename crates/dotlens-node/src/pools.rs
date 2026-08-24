//! Two connection pools over one database, and the four settings that stop a
//! read flood from turning into stale data.
//!
//! ROADMAP's operational floor, item 3. The failure it names is specific to this
//! project and is not an outage: *"one Postgres serves reads AND takes
//! continuous writes from the followers and backfill, so read pressure shows up
//! as STALE DATA rather than as an outage — and Phase 3.5's own per-module
//! freshness is what would make that visible."*
//!
//! # WHY TWO POOLS AND NOT ONE BOUNDED ONE
//!
//! ROADMAP calls a `statement_timeout` plus a bounded pool "the cheap half" and
//! puts splitting serving from ingestion in the escape hatch. **The cheap half
//! does not actually close the failure it is named against, and that is worth
//! stating plainly rather than discovering later.** One pool of N shared between
//! the API and the followers means a sustained read flood can hold every
//! connection in it. A `statement_timeout` bounds how long any ONE query holds
//! one; it does not reserve any for the followers. Under sustained pressure the
//! followers still queue, their checkpoints still fall behind, and the symptom
//! is still stale data.
//!
//! Reserving capacity is what prevents it, and connection partitioning is the
//! cheapest form of that: two `PgPool`s over the SAME `DATABASE_URL`, sized
//! separately. This is not the escape hatch ROADMAP defers — that is a read
//! replica or a second database, which is a deployment change. This is fifteen
//! lines and no new infrastructure.
//!
//! It also makes each setting honest for its workload. One `statement_timeout`
//! across a shared pool has to serve both a 200 ms indexed API read and a
//! long ingestion statement; whichever number you pick is wrong for one of them.
//!
//! # THE FOUR SETTINGS, AND WHY EACH IS A DIFFERENT JOB
//!
//! Two are Postgres-side and two are sqlx-side, and they fail in different
//! places, which is why all four are here rather than just the one ROADMAP
//! names:
//!
//! | setting | stops |
//! |---|---|
//! | `max_connections` | one workload consuming every connection (the actual fix) |
//! | `statement_timeout` | one query pinning a connection while it runs |
//! | `idle_in_transaction_session_timeout` | a leaked transaction pinning one while it does NOTHING |
//! | `acquire_timeout` (sqlx) | pool exhaustion becoming unbounded latency instead of a fast refusal |
//!
//! The third is the one most often forgotten and is arguably the sharpest: a
//! statement that finishes inside its timeout but leaves its transaction open
//! holds its connection indefinitely AND holds back vacuum, and no
//! `statement_timeout` touches it.
//!
//! The fourth is what makes a bounded pool observable rather than merely bounded.
//! Without it, sqlx's default is to wait 30 seconds for a connection, so an
//! exhausted serving pool renders as every request being slow — which is the
//! same "reads as healthy" failure one layer up.
//!
//! **DELIBERATELY NOT SET: `lock_timeout` and `min_connections`.** A
//! `lock_timeout` on the ingestion pool would make startup migrations fail
//! whenever any reader holds a lock, which trades a rare stall for a common
//! failure; on the serving pool the `statement_timeout` already bounds the wait.
//! `min_connections` would keep connections warm at the cost of holding them
//! open on an idle box, and nothing has measured that the warm-up matters.
//!
//! # THE NUMBERS ARE TUNABLE GUESSES AND ARE LABELLED AS SUCH
//!
//! This project refuses to default a window or pick a denominator, because a
//! number nobody chose sitting underneath a figure somebody quotes is a lie in
//! waiting. A timeout cannot be refused the same way — it must have a value —
//! so it takes the treatment ARCHITECTURE §9d gave admission control's bounds:
//! **the SHAPE is decided and the NUMBERS are marked as guesses**, every one is
//! env-overridable, and the measurement that would replace them is named.
//!
//! That measurement is Phase 4's load test. **Nobody has ever run this API under
//! concurrent load**, so every figure below is reasoned rather than observed,
//! and the reasoning is written beside it so a later session can disagree with
//! the argument instead of just changing the number.

use std::time::Duration;

/// Serving: the API's read path. Read-only, every query window-scoped and
/// indexed, and a human is waiting.
pub const SERVING_MAX_CONNECTIONS: u32 = 8;
/// GUESS. The largest window the API will accept is `CORETIME_MAX_SPAN`,
/// 250,000 blocks, and nobody has measured that query. 10s is above anything
/// there is reason to expect and below the point where the caller has given up;
/// it is also longer than a whole follower poll (6s) and tip poll (3s), so a
/// serving query that outlives it is pathological by any reading.
pub const SERVING_STATEMENT_TIMEOUT_SECS: u64 = 10;
/// GUESS, and deliberately tight: nothing in the read path opens a transaction
/// that should stay open, so anything idle in one is a bug holding a connection.
pub const SERVING_IDLE_IN_TXN_TIMEOUT_SECS: u64 = 10;
/// GUESS. sqlx's default is 30s, which turns an exhausted pool into every
/// request being slow. Failing in 3 makes exhaustion visible as a refusal.
pub const SERVING_ACQUIRE_TIMEOUT_SECS: u64 = 3;

/// Ingestion: followers, backfill, decode, the one-shot `*-range` commands and
/// the startup migrations. Writes continuously, nobody is waiting, and some
/// statements are legitimately long.
///
/// UNCHANGED from the single pool this replaces (10), on purpose. Splitting
/// should not quietly reduce the capacity the pipeline had.
pub const INGEST_MAX_CONNECTIONS: u32 = 10;
/// GUESS. Five minutes is far above anything measured — decode runs ~20
/// blocks/s from raw and the per-block writes are milliseconds — and far below
/// "forever", so a genuinely stuck statement still releases its connection
/// instead of holding it until the process is killed.
pub const INGEST_STATEMENT_TIMEOUT_SECS: u64 = 300;
/// GUESS. Longer than serving's because the write paths DO hold transactions
/// open across several statements by design (the channel reading's header and
/// detail land in one), but still bounded.
pub const INGEST_IDLE_IN_TXN_TIMEOUT_SECS: u64 = 300;
/// sqlx's default. Queueing for a connection is the correct behaviour on this
/// side — a follower tick that waits is a follower tick that runs.
pub const INGEST_ACQUIRE_TIMEOUT_SECS: u64 = 30;

/// The two pools, over one `DATABASE_URL`.
pub struct Pools {
    /// Writes, migrations and every `*-range` command.
    pub ingest: sqlx::PgPool,
    /// The API's read path, and nothing else. Sized and timed so a read flood
    /// cannot reach the connections `ingest` depends on.
    pub serving: sqlx::PgPool,
}

/// Read a duration in seconds, refusing ZERO.
///
/// **`statement_timeout = '0s'` means NO LIMIT in Postgres, and
/// `idle_in_transaction_session_timeout = '0s'` likewise.** So a `0` here does
/// not tighten the setting to nothing, it silently removes it — wrong in the
/// flattering direction, and invisible afterwards because the pool still starts
/// and every query still works, right up until one does not finish.
///
/// A zero is therefore treated as junk rather than honoured: the default is used
/// and the attempt is logged. An operator who genuinely wants no limit can say
/// so with a large number, which leaves a legible value in the environment
/// instead of an absence that reads like a setting.
fn env_secs(key: &str, default: u64) -> u64 {
    match std::env::var(key).ok().map(|v| v.parse::<u64>()) {
        Some(Ok(0)) => {
            tracing::warn!(
                setting = key,
                default,
                "0 means NO LIMIT to Postgres, not 'no wait' — ignoring it and using \
                 the default; set a large number if you really want no bound"
            );
            default
        }
        Some(Ok(v)) => v,
        Some(Err(_)) => {
            tracing::warn!(setting = key, default, "not a number — using the default");
            default
        }
        None => default,
    }
}

/// Read a connection bound, refusing zero for a different reason: a pool with
/// `max_connections(0)` can never hand out a connection at all.
fn env_u32(key: &str, default: u32) -> u32 {
    match std::env::var(key).ok().map(|v| v.parse::<u32>()) {
        Some(Ok(0)) | Some(Err(_)) => {
            tracing::warn!(
                setting = key,
                default,
                "not a usable connection bound — using the default"
            );
            default
        }
        Some(Ok(v)) => v,
        None => default,
    }
}

/// The session settings, as SQL.
///
/// Built from parsed integers and never from raw env strings: these are
/// interpolated into a statement, and this is the one place in the file where
/// an operator's environment reaches SQL text. Parsing to `u64` first makes the
/// interpolation structurally safe rather than safe-by-inspection.
fn session_sql(statement_timeout_secs: u64, idle_in_txn_secs: u64) -> String {
    format!(
        "set statement_timeout = '{statement_timeout_secs}s'; \
         set idle_in_transaction_session_timeout = '{idle_in_txn_secs}s'"
    )
}

fn build(
    max_connections: u32,
    acquire_timeout_secs: u64,
    statement_timeout_secs: u64,
    idle_in_txn_secs: u64,
) -> sqlx::postgres::PgPoolOptions {
    let sql = session_sql(statement_timeout_secs, idle_in_txn_secs);
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(Duration::from_secs(acquire_timeout_secs))
        // Applied per CONNECTION rather than per query, so every statement on
        // this pool inherits it and no call site can forget. `execute` on a
        // `&str` uses the simple protocol, which is what allows the two `set`s
        // in one round trip.
        .after_connect(move |conn, _meta| {
            let sql = sql.clone();
            Box::pin(async move {
                use sqlx::Executor;
                (&mut *conn).execute(sql.as_str()).await?;
                Ok(())
            })
        })
}

/// Connect both pools.
///
/// The ingestion pool connects EAGERLY, because migrations run on it
/// immediately and a bad `DATABASE_URL` should fail at startup rather than at
/// the first request. The serving pool is LAZY: a `*-range` or `status`
/// invocation never serves anything, and opening connections it will not use is
/// waste on every CLI run.
pub async fn connect(db_url: &str) -> Result<Pools, sqlx::Error> {
    let ingest = build(
        env_u32("DOTLENS_INGEST_MAX_CONNECTIONS", INGEST_MAX_CONNECTIONS),
        env_secs(
            "DOTLENS_INGEST_ACQUIRE_TIMEOUT_SECS",
            INGEST_ACQUIRE_TIMEOUT_SECS,
        ),
        env_secs(
            "DOTLENS_INGEST_STATEMENT_TIMEOUT_SECS",
            INGEST_STATEMENT_TIMEOUT_SECS,
        ),
        env_secs(
            "DOTLENS_INGEST_IDLE_IN_TXN_TIMEOUT_SECS",
            INGEST_IDLE_IN_TXN_TIMEOUT_SECS,
        ),
    )
    .connect(db_url)
    .await?;

    let serving = build(
        env_u32("DOTLENS_SERVING_MAX_CONNECTIONS", SERVING_MAX_CONNECTIONS),
        env_secs(
            "DOTLENS_SERVING_ACQUIRE_TIMEOUT_SECS",
            SERVING_ACQUIRE_TIMEOUT_SECS,
        ),
        env_secs(
            "DOTLENS_SERVING_STATEMENT_TIMEOUT_SECS",
            SERVING_STATEMENT_TIMEOUT_SECS,
        ),
        env_secs(
            "DOTLENS_SERVING_IDLE_IN_TXN_TIMEOUT_SECS",
            SERVING_IDLE_IN_TXN_TIMEOUT_SECS,
        ),
    )
    .connect_lazy(db_url)?;

    Ok(Pools { ingest, serving })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_settings_sql_interpolates_only_parsed_integers() {
        let sql = session_sql(10, 30);
        assert_eq!(
            sql,
            "set statement_timeout = '10s'; \
             set idle_in_transaction_session_timeout = '30s'"
        );
        assert!(!sql.contains("--"), "no comment sequence can reach this");
    }

    #[test]
    fn serving_is_tighter_than_ingestion_on_every_axis_that_matters() {
        // The whole point of two pools is that the numbers DIFFER. If a later
        // edit made them equal, the split would still be there and would be
        // buying nothing, which is worse than not splitting — it looks solved.
        assert!(
            SERVING_STATEMENT_TIMEOUT_SECS < INGEST_STATEMENT_TIMEOUT_SECS,
            "a serving query and an ingestion statement have different budgets"
        );
        assert!(SERVING_IDLE_IN_TXN_TIMEOUT_SECS < INGEST_IDLE_IN_TXN_TIMEOUT_SECS);
        assert!(
            SERVING_ACQUIRE_TIMEOUT_SECS < INGEST_ACQUIRE_TIMEOUT_SECS,
            "a waiting reader should fail fast; a waiting follower should wait"
        );
    }

    #[test]
    fn the_pipeline_keeps_the_capacity_it_had_before_the_split() {
        // Splitting must not quietly halve what ingestion had. The pre-split
        // pool was 10 and shared; ingestion now has 10 to itself.
        assert_eq!(INGEST_MAX_CONNECTIONS, 10);
    }

    #[test]
    fn env_parsing_falls_back_rather_than_panicking_on_junk() {
        // A typo'd env var must not take the process down at startup.
        std::env::set_var("DOTLENS_TEST_JUNK_SECS", "not-a-number");
        assert_eq!(env_secs("DOTLENS_TEST_JUNK_SECS", 7), 7);
        std::env::remove_var("DOTLENS_TEST_JUNK_SECS");
        assert_eq!(env_secs("DOTLENS_TEST_ABSENT_SECS", 7), 7);
    }

    #[test]
    fn a_zero_timeout_is_refused_because_postgres_reads_it_as_no_limit() {
        // THE ONE THAT WOULD SHIP SILENTLY. `statement_timeout = '0s'` does not
        // mean "give up immediately", it means NO LIMIT — so honouring a 0 here
        // would remove the protection while leaving a setting in the
        // environment that reads as if it were tightened. Wrong in the
        // flattering direction, and invisible until a query does not finish.
        std::env::set_var("DOTLENS_TEST_ZERO_SECS", "0");
        assert_eq!(
            env_secs("DOTLENS_TEST_ZERO_SECS", 10),
            10,
            "a 0 must fall back to the default, not disable the timeout"
        );
        std::env::remove_var("DOTLENS_TEST_ZERO_SECS");

        // And a pool that can never hand out a connection is refused too.
        std::env::set_var("DOTLENS_TEST_ZERO_CONNS", "0");
        assert_eq!(env_u32("DOTLENS_TEST_ZERO_CONNS", 8), 8);
        std::env::remove_var("DOTLENS_TEST_ZERO_CONNS");
    }

    #[test]
    fn a_real_override_is_still_honoured() {
        // The guard above must not have made the settings unconfigurable — a
        // gate that refuses everything is as useless as one that refuses
        // nothing.
        std::env::set_var("DOTLENS_TEST_REAL_SECS", "45");
        assert_eq!(env_secs("DOTLENS_TEST_REAL_SECS", 10), 45);
        std::env::remove_var("DOTLENS_TEST_REAL_SECS");
    }
}
