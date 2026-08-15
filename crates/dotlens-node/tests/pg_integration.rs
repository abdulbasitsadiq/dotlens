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
use raw_store::{FsRawStore, RawStore};
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

// -------------------------------------------------------- account labeling

#[tokio::test]
async fn account_labels_derive_from_registry_metadata_and_seeds() {
    use api::LabelIndex;

    let Some(db) = TestDb::create().await else { return };
    let reg = seeds();
    sync_registry(&db.pool, &reg).await.expect("registry sync");

    // stage archived metadata for AH exactly as live ingestion would have:
    // blob in the raw store + a substrate.runtime_versions row pointing at it
    let raw_dir = tmp_raw("labels");
    let raw = FsRawStore::new(&raw_dir);
    let meta_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/real/polkadot-asset-hub-19498783/metadata.scale");
    let metadata_staged = match std::fs::read(&meta_path) {
        Ok(blob) => {
            let key = raw_store::keys::metadata("polkadot-asset-hub", 2_003_002);
            raw.put(&key, &blob, "test").expect("stage metadata blob");
            sqlx::query(
                "insert into substrate.runtime_versions \
                     (chain_id, spec_version, metadata_version, metadata_blob_location) \
                 values ($1, $2, 14, $3) on conflict do nothing",
            )
            .bind("polkadot-asset-hub")
            .bind(2_003_002i64)
            .bind(&key)
            .execute(&db.pool)
            .await
            .expect("runtime_versions row");
            true
        }
        Err(_) => {
            eprintln!("NOTE: real fixture metadata absent — pallet-label assertions skipped");
            false
        }
    };

    let r1 = dotlens_node::labels::sync_labels(&db.pool, &reg, &raw)
        .await
        .expect("first label sync");
    assert!(r1.sovereign_labels >= 1, "AH para sovereign expected");
    assert_eq!(r1.seeded_labels, 2, "the two seeded AH treasury accounts");
    let (count1,): (i64,) = sqlx::query_as("select count(*) from core.account_labels")
        .fetch_one(&db.pool)
        .await
        .unwrap();

    // idempotent: second sync changes nothing
    dotlens_node::labels::sync_labels(&db.pool, &reg, &raw)
        .await
        .expect("second label sync");
    let (count2,): (i64,) = sqlx::query_as("select count(*) from core.account_labels")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(count1, count2, "label sync must be idempotent");

    // AH's para sovereign is labeled ON THE RELAY with the ECOSYSTEM.md golden
    let (ss58,): (Option<String>,) = sqlx::query_as(
        "select ss58 from core.account_labels \
         where kind = 'para_sovereign' and chain_scope = 'polkadot' and derivation = 'para:1000'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("para sovereign row on the relay");
    assert_eq!(
        ss58.as_deref(),
        Some("13YMK2edbuhwMBxeUWm9c643A2wyYHwSVh1bCM7tShtg7Dtk")
    );

    if metadata_staged {
        // THE exit-criterion label: Treasury (py/trsry), named, on AH
        let (label, ss58): (String, Option<String>) = sqlx::query_as(
            "select label, ss58 from core.account_labels \
             where kind = 'pallet' and chain_scope = 'polkadot-asset-hub' \
               and derivation = 'modl:py/trsry'",
        )
        .fetch_one(&db.pool)
        .await
        .expect("treasury pallet label on AH");
        assert!(label.ends_with("(py/trsry)"), "named label, got: {label}");
        assert_eq!(
            ss58.as_deref(),
            Some("13UVJyLnbVp9RBZYFwFGyDvVd1y27Tt8tkntv6Q7JVPhFsTB")
        );

        // and it surfaces through the API's label index
        let idx = api::pg::PgLabelIndex::new(db.pool.clone());
        let treasury = adapter_substrate::accounts::pallet_account(b"py/trsry");
        let labels = idx
            .labels_for("polkadot-asset-hub", &treasury)
            .await
            .expect("labels_for");
        assert!(labels.iter().any(|l| l.label.ends_with("(py/trsry)")));
    }

    // verification recording round-trips
    let rows = dotlens_node::labels::labels_for_chain(&db.pool, "polkadot")
        .await
        .expect("labels for relay");
    assert!(!rows.is_empty());
    dotlens_node::labels::record_verification(&db.pool, &rows[0], 12_345, true)
        .await
        .expect("record verification");
    let (note, block): (Option<String>, Option<i64>) = sqlx::query_as(
        "select verified_note, verified_block from core.account_labels \
         where account_id = $1 and kind = $2 and chain_scope = $3",
    )
    .bind(&rows[0].account_id[..])
    .bind(&rows[0].kind)
    .bind(&rows[0].chain_scope)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(note.as_deref(), Some("exists"));
    assert_eq!(block, Some(12_345));

    let _ = std::fs::remove_dir_all(&raw_dir);
    db.drop_db().await;
}

#[tokio::test]
async fn sibling_sovereign_labels_appear_when_a_chain_registers() {
    let Some(db) = TestDb::create().await else { return };

    // real seeds + one synthetic parachain — the plug-and-play path: adding a
    // chain is config only, and its sovereigns appear everywhere automatically
    let dir = std::env::temp_dir().join(format!("dotlens-labels-reg-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../registry-seeds");
    for entry in std::fs::read_dir(&src).unwrap() {
        let p = entry.unwrap().path();
        if p.extension().map(|e| e == "yaml").unwrap_or(false) {
            std::fs::copy(&p, dir.join(p.file_name().unwrap())).unwrap();
        }
    }
    std::fs::write(
        dir.join("test-para.yaml"),
        "id: test-para\nname: Test Para\nfamily: substrate\nrelay: polkadot\n\
         para_id: 2034\nnetwork: polkadot\nss58_prefix: 0\n",
    )
    .unwrap();
    let reg = Registry::load_from_dir(&dir).expect("temp registry loads");
    sync_registry(&db.pool, &reg).await.expect("registry sync");

    let raw_dir = tmp_raw("sibl");
    let raw = FsRawStore::new(&raw_dir);
    dotlens_node::labels::sync_labels(&db.pool, &reg, &raw)
        .await
        .expect("label sync");

    let exists = |kind: &'static str, scope: &'static str, derivation: &'static str| {
        let pool = db.pool.clone();
        async move {
            let (n,): (i64,) = sqlx::query_as(
                "select count(*) from core.account_labels \
                 where kind = $1 and chain_scope = $2 and derivation = $3",
            )
            .bind(kind)
            .bind(scope)
            .bind(derivation)
            .fetch_one(&pool)
            .await
            .unwrap();
            n == 1
        }
    };
    // test-para's sovereign: on the relay (para) and on AH (sibl)
    assert!(exists("para_sovereign", "polkadot", "para:2034").await);
    assert!(exists("sibl_sovereign", "polkadot-asset-hub", "sibl:2034").await);
    // and AH's sibling sovereign appears on test-para — both directions
    assert!(exists("sibl_sovereign", "test-para", "sibl:1000").await);

    // ss58 agrees with the adapter's own derivation (self-consistency)
    let expected = adapter_substrate::frame_decoder::ss58_encode(
        0,
        &adapter_substrate::accounts::sibling_sovereign(2034),
    );
    let (ss58,): (Option<String>,) = sqlx::query_as(
        "select ss58 from core.account_labels \
         where kind = 'sibl_sovereign' and chain_scope = 'polkadot-asset-hub' \
           and derivation = 'sibl:2034'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(ss58.as_deref(), Some(expected.as_str()));

    let _ = std::fs::remove_dir_all(&raw_dir);
    let _ = std::fs::remove_dir_all(&dir);
    db.drop_db().await;
}

// ------------------------------------------------------------- balances

#[tokio::test]
async fn balances_worker_maps_deltas_and_survives_the_boundary_filter() {
    use adapter_substrate::accounts::{pallet_account, para_sovereign};
    use adapter_substrate::balances::SubstrateDeltaMapper;
    use api::BalanceIndex as _;
    use canonical::{CanonicalBlock, CanonicalEvent, Lineage};

    let Some(db) = TestDb::create().await else { return };
    let reg = seeds();
    sync_registry(&db.pool, &reg).await.expect("registry sync");

    let treasury = pallet_account(b"py/trsry");
    let peer = para_sovereign(1000);
    let acct_json = |a: &[u8; 32]| serde_json::json!([a.to_vec()]);
    let block = |chain: &str, height: u64, ts: &str, events: Vec<CanonicalEvent>| CanonicalBlock {
        chain_id: chain.into(),
        height,
        hash: format!("0x{height:064x}"),
        parent_hash: format!("0x{:064x}", height - 1),
        timestamp: Some(ts.parse().unwrap()),
        finalized: true,
        lineage: Lineage {
            runtime_version: 100,
            decoder_version: 2,
            raw_location: format!("raw/{chain}/test/{height}"),
        },
        transactions: vec![],
        events,
    };
    let ev = |index: u32, name: &str, data: serde_json::Value| CanonicalEvent {
        index,
        transaction_index: Some(0),
        name: name.into(),
        data,
    };

    // relay block BEFORE the migration boundary: a >u64 transfer out of treasury
    let big = "36893488147419103232"; // 2^65 — must survive as numeric, not float
    let relay_block = block(
        "polkadot",
        100,
        "2025-06-01T00:00:00Z",
        vec![
            ev(0, "balances.Transfer", serde_json::json!({
                "from": acct_json(&treasury), "to": acct_json(&peer), "amount": big,
            })),
            ev(1, "system.ExtrinsicSuccess", serde_json::json!({})),
        ],
    );
    // AH block AFTER the boundary: a fee withdraw from the same account
    let ah_block = block(
        "polkadot-asset-hub",
        200,
        "2026-01-01T00:00:00Z",
        vec![ev(0, "balances.Withdraw", serde_json::json!({
            "who": acct_json(&treasury), "amount": 160000000u64,
        }))],
    );
    let index = PgBlockIndex::new(db.pool.clone());
    index.insert(relay_block).await.expect("insert relay block");
    index.insert(ah_block).await.expect("insert ah block");

    // map both chains through the real worker + Pg backends
    let checkpoints = PgCheckpointStore::new(db.pool.clone());
    let source = dotlens_node::balances_pg::PgEventSource::new(db.pool.clone());
    let sink = dotlens_node::balances_pg::PgDeltaSink::new(db.pool.clone());
    let deps = ingest::balances::BalancesDeps {
        checkpoints: &checkpoints,
        source: &source,
        sink: &sink,
    };
    let n1 = ingest::balances::balances_range("polkadot", &SubstrateDeltaMapper, &deps, 100, 100)
        .await
        .expect("relay range");
    let n2 = ingest::balances::balances_range(
        "polkadot-asset-hub", &SubstrateDeltaMapper, &deps, 200, 200,
    )
    .await
    .expect("ah range");
    assert_eq!((n1, n2), (1, 1));

    // double-entry landed with exact numerics, in the chain partition
    let rows: Vec<(Vec<u8>, String, String)> = sqlx::query_as(
        "select account_id, delta::text, reason from balances.balance_changes \
         where chain_id = 'polkadot' order by delta",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (treasury.to_vec(), format!("-{big}"), "transfer_out".into()));
    assert_eq!(rows[1], (peer.to_vec(), big.to_string(), "transfer_in".into()));
    let (in_default,): (i64,) =
        sqlx::query_as("select count(*) from balances.balance_changes_default")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(in_default, 0, "deltas must route to per-chain partitions");

    // idempotent: re-running the ranges changes nothing
    ingest::balances::balances_range("polkadot", &SubstrateDeltaMapper, &deps, 100, 100)
        .await
        .expect("relay rerun");
    let (total,): (i64,) = sqlx::query_as("select count(*) from balances.balance_changes")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(total, 3);

    // the API's index respects the migration boundary timestamp per chain
    let idx = api::pg::PgBalanceIndex::new(db.pool.clone());
    let boundary: chrono::DateTime<chrono::Utc> = "2025-11-04T00:00:00Z".parse().unwrap();
    let pre = idx
        .changes("polkadot", &treasury, "native", None, Some(boundary))
        .await
        .unwrap();
    assert_eq!(pre.len(), 1);
    assert_eq!(pre[0].delta, format!("-{big}"));
    assert!(idx
        .changes("polkadot", &treasury, "native", Some(boundary), None)
        .await
        .unwrap()
        .is_empty());
    let post = idx
        .changes("polkadot-asset-hub", &treasury, "native", Some(boundary), None)
        .await
        .unwrap();
    assert_eq!(post.len(), 1);
    assert_eq!(post[0].delta, "-160000000");

    // anchors: insert + read back (frozen None → null; insert-ignore on rerun)
    let ab = adapter_substrate::balances::AccountBalances {
        free: 500_000_000_000,
        reserved: 5_000_000_000,
        frozen: None,
    };
    for _ in 0..2 {
        dotlens_node::balances_pg::insert_anchor(
            &db.pool, "polkadot-asset-hub", &treasury, "native", 150,
            &ab, Some(2_003_002), "test", None,
        )
        .await
        .expect("anchor");
    }
    let anchors = idx.anchors("polkadot-asset-hub", &treasury, "native").await.unwrap();
    assert_eq!(anchors.len(), 1);
    assert_eq!(anchors[0].total, "505000000000");
    assert_eq!(anchors[0].free, "500000000000");
    assert!(anchors[0].note.is_none());

    db.drop_db().await;
}
