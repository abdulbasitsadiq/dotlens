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

// ---------------------------------------------------------------- live (raw)

/// Minimal scripted source for the LIVE raw pipeline: two runtime eras
/// (v100 → v101 at height 6). No subxt, no network — the generic worker plus
/// real Pg sinks is exactly what this proves.
struct MockSource {
    finalized: u64,
}

#[async_trait::async_trait]
impl ingest::live::ChainSource for MockSource {
    async fn finalized_height(&self) -> Result<u64, ingest::live::SourceError> {
        Ok(self.finalized)
    }
    async fn fetch_block(
        &self,
        height: u64,
    ) -> Result<ingest::live::FetchedBlock, ingest::live::SourceError> {
        if height > self.finalized {
            return Err(ingest::live::SourceError::NotFound(height));
        }
        Ok(ingest::live::FetchedBlock {
            height,
            hash: format!("0x{height:064x}"),
            parent_hash: format!("0x{:064x}", height.saturating_sub(1)),
            runtime_version: if height <= 5 { 100 } else { 101 },
            transaction_version: Some(1),
            artifacts: vec![
                ingest::live::RawArtifact {
                    item: "block.json".into(),
                    // decode worker peeks spec_version; MockBlockDecoder reads height
                    bytes: format!(
                        "{{\"chain_id\":\"polkadot\",\"height\":{height},\"spec_version\":{}}}",
                        if height <= 5 { 100 } else { 101 }
                    )
                    .into_bytes(),
                },
                ingest::live::RawArtifact {
                    item: "events.scale".into(),
                    bytes: vec![height as u8],
                },
            ],
        })
    }
    async fn metadata_at(&self, height: u64) -> Result<Vec<u8>, ingest::live::SourceError> {
        let v: u32 = if height <= 5 { 100 } else { 101 };
        let mut blob = b"meta".to_vec();
        blob.push(15);
        blob.extend_from_slice(&v.to_le_bytes());
        Ok(blob)
    }
}

#[tokio::test]
async fn live_raw_pipeline_persists_lineage_and_resumes() {
    let Some(db) = TestDb::create().await else { return };
    let reg = seeds();
    sync_registry(&db.pool, &reg).await.expect("registry sync");

    let raw_dir = tmp_raw("live");
    let raw = FsRawStore::new(&raw_dir);
    let checkpoints = PgCheckpointStore::new(db.pool.clone());
    let receipts = PgReceiptSink::new(db.pool.clone());
    let versions = dotlens_node::runtime_versions::PgRuntimeVersionSink::new(db.pool.clone());
    let deps = ingest::live::IngestDeps {
        raw: &raw,
        checkpoints: &checkpoints,
        receipts: &receipts,
        runtime_versions: &versions,
    };
    let source = MockSource { finalized: 10 };

    // use a registered chain id so the runtime_versions FK to core.chains holds
    let n = ingest::live::ingest_range(
        "polkadot", &source, &deps, ingest::live::MODULE_BACKFILL, 1, 10, &mut None,
    )
    .await
    .expect("live range");
    assert_eq!(n, 10);

    // runtime lineage rows: one per era, correct boundaries + metadata info
    let rows: Vec<(i64, Option<i64>, Option<i32>, Option<String>, Option<i64>)> = sqlx::query_as(
        "select spec_version, transaction_version, metadata_version, \
                metadata_blob_location, first_block \
         from substrate.runtime_versions where chain_id = 'polkadot' \
         order by spec_version",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, 100);
    assert_eq!(rows[0].4, Some(1)); // first observed at height 1
    assert_eq!(rows[0].2, Some(15));
    assert!(rows[0].3.as_deref().unwrap().contains("/meta/100/"));
    assert_eq!(rows[1].0, 101);
    assert_eq!(rows[1].4, Some(6)); // era boundary detected

    // receipts: 2 artifacts × 10 blocks + 2 metadata blobs
    let (receipts_n,): (i64,) = sqlx::query_as("select count(*) from core.ingest_receipts")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(receipts_n, 22);

    // re-run: clean no-op (checkpoint resume, no new receipts)
    let n = ingest::live::ingest_range(
        "polkadot", &source, &deps, ingest::live::MODULE_BACKFILL, 1, 10, &mut None,
    )
    .await
    .expect("re-run");
    assert_eq!(n, 0);

    // "restart" with fresh sink instances: sink upserts stay idempotent and
    // first_block never regresses upward
    let versions2 = dotlens_node::runtime_versions::PgRuntimeVersionSink::new(db.pool.clone());
    let checkpoints2 = PgCheckpointStore::new(db.pool.clone());
    let deps2 = ingest::live::IngestDeps {
        raw: &raw,
        checkpoints: &checkpoints2,
        receipts: &receipts,
        runtime_versions: &versions2,
    };
    let source2 = MockSource { finalized: 15 };
    let n = ingest::live::ingest_range(
        "polkadot", &source2, &deps2, ingest::live::MODULE_BACKFILL, 1, 15, &mut None,
    )
    .await
    .expect("extended range");
    assert_eq!(n, 5, "resumes at 11, not 1");
    let (first_block,): (Option<i64>,) = sqlx::query_as(
        "select first_block from substrate.runtime_versions \
         where chain_id = 'polkadot' and spec_version = 101",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(first_block, Some(6), "first_block must not regress upward");

    let _ = std::fs::remove_dir_all(&raw_dir);
    db.drop_db().await;
}

/// Decode worker → PgBlockIndex sink: raw landed by the live pipeline gets
/// decoded (mock decoder — real SCALE is covered by adapter fixture tests)
/// into core tables with decoder_version-2 lineage, idempotently.
struct MockBlockDecoder;

impl ingest::decode::RawBlockDecoder for MockBlockDecoder {
    fn decode(
        &self,
        chain_id: &str,
        envelope: &[u8],
        _events: Option<&[u8]>,
        _metadata: &[u8],
        spec_version: u32,
        raw_location: &str,
    ) -> Result<canonical::CanonicalBlock, String> {
        let v: serde_json::Value = serde_json::from_slice(envelope).map_err(|e| e.to_string())?;
        let height = v["height"].as_u64().ok_or("no height")?;
        Ok(canonical::CanonicalBlock {
            chain_id: chain_id.to_string(),
            height,
            hash: format!("0x{height:064x}"),
            parent_hash: format!("0x{:064x}", height.saturating_sub(1)),
            timestamp: None,
            finalized: true,
            lineage: canonical::Lineage {
                runtime_version: spec_version,
                decoder_version: 2,
                raw_location: raw_location.to_string(),
            },
            transactions: vec![],
            events: vec![],
        })
    }
}

#[tokio::test]
async fn decode_worker_lands_canonical_rows_in_pg() {
    let Some(db) = TestDb::create().await else { return };
    let reg = seeds();
    sync_registry(&db.pool, &reg).await.expect("registry sync");

    // stage 1: raw ingest 1..=6 via the live pipeline (mock source, real Pg)
    let raw_dir = tmp_raw("decode");
    let raw = FsRawStore::new(&raw_dir);
    let checkpoints = PgCheckpointStore::new(db.pool.clone());
    let receipts = PgReceiptSink::new(db.pool.clone());
    let versions = dotlens_node::runtime_versions::PgRuntimeVersionSink::new(db.pool.clone());
    let live_deps = ingest::live::IngestDeps {
        raw: &raw,
        checkpoints: &checkpoints,
        receipts: &receipts,
        runtime_versions: &versions,
    };
    let source = MockSource { finalized: 6 };
    ingest::live::ingest_range(
        "polkadot", &source, &live_deps, ingest::live::MODULE_LIVE, 1, 6, &mut None,
    )
    .await
    .expect("raw ingest");

    // stage 2: decode 1..=6 through the generic worker into PgBlockIndex
    let index: std::sync::Arc<dyn BlockIndex> =
        std::sync::Arc::new(PgBlockIndex::new(db.pool.clone()));
    let sink = dotlens_node::pipeline::BlockIndexSink(index.clone());
    let decode_deps = ingest::decode::DecodeDeps {
        raw: &raw,
        checkpoints: &checkpoints,
        sink: &sink,
    };
    let n = ingest::decode::decode_range("polkadot", &MockBlockDecoder, &decode_deps, 1, 6)
        .await
        .expect("decode range");
    assert_eq!(n, 6);

    // canonical rows exist with decoder-2 lineage, in the chain partition
    let (rows, specs): ((i64,), Vec<(i64, i32)>) = (
        sqlx::query_as("select count(*) from core.blocks where chain_id = 'polkadot'")
            .fetch_one(&db.pool)
            .await
            .unwrap(),
        sqlx::query_as(
            "select runtime_version, decoder_version from core.blocks \
             where chain_id = 'polkadot' order by height",
        )
        .fetch_all(&db.pool)
        .await
        .unwrap(),
    );
    assert_eq!(rows.0, 6);
    assert_eq!(specs[0], (100, 2)); // era v100 (heights 1..=5)
    assert_eq!(specs[5], (101, 2)); // era v101 (height 6)

    // decode tick chases the raw checkpoint: raw advances to 9 → decode follows
    let source2 = MockSource { finalized: 9 };
    ingest::live::ingest_range(
        "polkadot", &source2, &live_deps, ingest::live::MODULE_LIVE, 7, 9, &mut None,
    )
    .await
    .expect("raw advance");
    let n = ingest::decode::decode_tick("polkadot", &MockBlockDecoder, &decode_deps)
        .await
        .expect("decode tick");
    assert_eq!(n, 3);

    // re-run both: clean no-ops
    assert_eq!(
        ingest::decode::decode_range("polkadot", &MockBlockDecoder, &decode_deps, 1, 9)
            .await
            .unwrap(),
        0
    );
    // blocks checkpoint at 9, distinct from raw_blocks
    let (h,): (i64,) = sqlx::query_as(
        "select last_height from core.indexer_state \
         where chain_id = 'polkadot' and module = 'blocks'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(h, 9);

    let _ = std::fs::remove_dir_all(&raw_dir);
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
