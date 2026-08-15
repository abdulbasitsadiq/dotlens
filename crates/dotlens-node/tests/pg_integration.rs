//! Postgres integration tests: the Phase 1 "Pg wiring" proof.
//!
//! Each test creates its own throwaway database (from DATABASE_URL), runs the
//! real migrations, drives the real pipeline, and drops the database after.
//! Without DATABASE_URL the tests skip loudly — `cargo test` stays green on a
//! machine without compose up, and proves persistence on one with it.

#![cfg(feature = "pg")]

use api::pg::PgBlockIndex;
use api::BlockIndex;
use dotlens_node::pipeline::ingest_fixtures;
use dotlens_node::registry_sync::sync_registry;
use ingest::pg::{PgCheckpointStore, PgReceiptSink};
use ingest::{should_process, Checkpoint, CheckpointStore, IngestOutcome, ReceiptSink};
use raw_store::FsRawStore;
use registry::Registry;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::path::{Path, PathBuf};

struct TestDb {
    pool: PgPool,
    admin_url: String,
    name: String,
}

impl TestDb {
    /// None = no DATABASE_URL → caller skips the test (loudly).
    async fn create() -> Option<TestDb> {
        let _ = dotenvy::dotenv();
        let _ = dotenvy::from_path(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.env"));
        let Ok(admin_url) = std::env::var("DATABASE_URL") else {
            eprintln!("SKIP: DATABASE_URL not set (compose not up?) — pg integration test skipped");
            return None;
        };
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let name = format!("dotlens_test_{}_{}", std::process::id(), seq);

        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&admin_url)
            .await
            .expect("connect admin (is compose up?)");
        sqlx::query(&format!("create database {name}"))
            .execute(&admin)
            .await
            .expect("create test database");
        admin.close().await;

        // swap the database name, preserving any ?options suffix
        let (main, query) = match admin_url.split_once('?') {
            Some((m, q)) => (m, Some(q)),
            None => (admin_url.as_str(), None),
        };
        let (base, dbname) = main.rsplit_once('/').expect("db url has a path");
        assert!(
            !dbname.is_empty() && !dbname.contains('@') && !dbname.contains(':'),
            "DATABASE_URL must include a database name path"
        );
        let url = match query {
            Some(q) => format!("{base}/{name}?{q}"),
            None => format!("{base}/{name}"),
        };
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&url)
            .await
            .expect("connect test database");
        sqlx::migrate!("../../migrations")
            .run(&pool)
            .await
            .expect("migrations");
        Some(TestDb { pool, admin_url, name })
    }

    async fn drop_db(self) {
        self.pool.close().await;
        if let Ok(admin) = PgPoolOptions::new()
            .max_connections(1)
            .connect(&self.admin_url)
            .await
        {
            let _ = sqlx::query(&format!("drop database if exists {} with (force)", self.name))
                .execute(&admin)
                .await;
            admin.close().await;
        }
    }
}

fn seeds() -> Registry {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../registry-seeds");
    Registry::load_from_dir(&dir).expect("registry seeds")
}

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/synthetic")
}

/// Unique temp dir per call — tests run in parallel and must never share state.
fn tmp_raw(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("dotlens-pgtest-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

#[tokio::test]
async fn registry_sync_is_idempotent_and_creates_partitions() {
    let Some(db) = TestDb::create().await else { return };
    let reg = seeds();

    sync_registry(&db.pool, &reg).await.expect("first sync");
    sync_registry(&db.pool, &reg).await.expect("second sync (idempotent)");

    let (chains,): (i64,) = sqlx::query_as("select count(*) from core.chains")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(chains as usize, reg.chains().count());

    let (residency,): (i64,) = sqlx::query_as("select count(*) from core.domain_residency")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(residency as usize, reg.residency().len());

    // relay FK projected correctly
    let (relay,): (Option<String>,) =
        sqlx::query_as("select relay_id from core.chains where id = 'polkadot-asset-hub'")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(relay.as_deref(), Some("polkadot"));

    // per-chain partitions exist for every registered chain, on all three tables
    for table in ["blocks", "transactions", "events"] {
        let (parts,): (i64,) = sqlx::query_as(
            "select count(*) from pg_tables \
             where schemaname = 'core' and tablename like $1 || '\\_p\\_%'",
        )
        .bind(table)
        .fetch_one(&db.pool)
        .await
        .unwrap();
        assert_eq!(parts as usize, reg.chains().count(), "partitions for core.{table}");
    }

    db.drop_db().await;
}

#[tokio::test]
async fn pg_checkpoints_persist_across_store_instances() {
    let Some(db) = TestDb::create().await else { return };

    let store = PgCheckpointStore::new(db.pool.clone());
    store
        .advance(Checkpoint {
            chain_id: "polkadot".into(),
            module: "blocks".into(),
            last_height: 100,
            last_hash: "0xaa".into(),
            updated_at: chrono::Utc::now(),
        })
        .await
        .expect("advance");
    drop(store);

    // "restart": a brand-new store over the same database resumes correctly
    let store2 = PgCheckpointStore::new(db.pool.clone());
    let cp = store2.get("polkadot", "blocks").await.unwrap().expect("persisted");
    assert_eq!(cp.last_height, 100);
    assert_eq!(
        should_process(&store2, "polkadot", "blocks", 100).await.unwrap(),
        IngestOutcome::AlreadyProcessed
    );
    assert_eq!(
        should_process(&store2, "polkadot", "blocks", 101).await.unwrap(),
        IngestOutcome::Processed
    );
    // regression refused by the conditional upsert
    assert!(store2
        .advance(Checkpoint {
            chain_id: "polkadot".into(),
            module: "blocks".into(),
            last_height: 50,
            last_hash: "0xbb".into(),
            updated_at: chrono::Utc::now(),
        })
        .await
        .is_err());

    db.drop_db().await;
}

#[tokio::test]
async fn receipts_persist_once_first_fetch_wins() {
    let Some(db) = TestDb::create().await else { return };

    let sink = PgReceiptSink::new(db.pool.clone());
    let receipt = raw_store::IngestReceipt {
        key: "raw/polkadot/0000001/12345/block.json".into(),
        byte_len: 42,
        source: "fixture".into(),
        content_hash: raw_store::content_hash(b"hello"),
        fetched_at: chrono::Utc::now(),
    };
    sink.record(&receipt).await.expect("first record");

    // second record for the same key (different source): original wins
    let mut later = receipt.clone();
    later.source = "wss://some-endpoint".into();
    sink.record(&later).await.expect("re-record is a no-op");

    let rows: Vec<(String, i64, String, String)> = sqlx::query_as(
        "select key, byte_len, source, content_hash from core.ingest_receipts",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].2, "fixture"); // first-fetch provenance kept
    assert_eq!(rows[0].3, raw_store::content_hash(b"hello"));

    db.drop_db().await;
}

#[tokio::test]
async fn end_to_end_pg_pipeline_is_restart_safe() {
    let Some(db) = TestDb::create().await else { return };
    let reg = seeds();
    sync_registry(&db.pool, &reg).await.expect("registry sync");

    let raw_dir = tmp_raw("e2e");
    let raw = FsRawStore::new(&raw_dir);
    let checkpoints = PgCheckpointStore::new(db.pool.clone());
    let receipts = PgReceiptSink::new(db.pool.clone());
    let blocks = PgBlockIndex::new(db.pool.clone());

    // first run: the fixture is processed and lands durably in postgres
    let n = ingest_fixtures(&fixtures_dir(), &reg, &raw, &checkpoints, &receipts, &blocks)
        .await
        .expect("first ingest");
    assert_eq!(n, 1);

    let block = blocks
        .get("polkadot-asset-hub", 19_000_001)
        .await
        .unwrap()
        .expect("block round-trips through postgres");
    assert_eq!(block.lineage.runtime_version, 2_000_006);
    assert_eq!(block.lineage.decoder_version, 1);
    assert!(block.lineage.raw_location.starts_with("raw/"));
    assert_eq!(block.transactions.len(), 2);
    assert!(!block.events.is_empty());

    // second run, same everything: clean no-op
    let n = ingest_fixtures(&fixtures_dir(), &reg, &raw, &checkpoints, &receipts, &blocks)
        .await
        .expect("re-ingest");
    assert_eq!(n, 0, "re-ingest must be a no-op");

    // "restart": brand-new backend instances over the same database — the
    // checkpoint AND the data both survived, so nothing reprocesses and the
    // block is still served
    let checkpoints2 = PgCheckpointStore::new(db.pool.clone());
    let receipts2 = PgReceiptSink::new(db.pool.clone());
    let blocks2 = PgBlockIndex::new(db.pool.clone());
    let n = ingest_fixtures(&fixtures_dir(), &reg, &raw, &checkpoints2, &receipts2, &blocks2)
        .await
        .expect("post-restart ingest");
    assert_eq!(n, 0);
    assert!(blocks2.get("polkadot-asset-hub", 19_000_001).await.unwrap().is_some());
    assert_eq!(blocks2.count().await.unwrap(), 1);

    // exactly one receipt, with a real content hash
    let (receipts_n,): (i64,) = sqlx::query_as("select count(*) from core.ingest_receipts")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(receipts_n, 1);
    let (hash,): (String,) =
        sqlx::query_as("select content_hash from core.ingest_receipts limit 1")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert!(hash.starts_with("0x") && hash.len() == 66);

    // rows landed in the per-chain partition, not the default catch-all
    let (in_default,): (i64,) = sqlx::query_as("select count(*) from core.blocks_default")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(in_default, 0, "block should be routed to its chain partition");

    let _ = std::fs::remove_dir_all(&raw_dir);
    db.drop_db().await;
}
