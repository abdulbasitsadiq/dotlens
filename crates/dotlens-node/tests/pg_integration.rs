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

    // per-chain partitions exist for every registered chain, on every
    // partitioned table — registering a chain is the ONLY step (adding
    // Collectives/People in the registration slice needed no code here)
    for (schema, table) in [
        ("core", "blocks"),
        ("core", "transactions"),
        ("core", "events"),
        ("balances", "balance_changes"),
        ("gov", "referendum_events"),
        ("gov", "votes"),
        ("gov", "delegation_events"),
    ] {
        let (parts,): (i64,) = sqlx::query_as(
            "select count(*) from pg_tables \
             where schemaname = $1 and tablename like $2 || '\\_p\\_%'",
        )
        .bind(schema)
        .bind(table)
        .fetch_one(&db.pool)
        .await
        .unwrap();
        assert_eq!(
            parts as usize,
            reg.chains().count(),
            "partitions for {schema}.{table}"
        );
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

    fn spec_version_of(&self, envelope: &[u8]) -> Result<u32, String> {
        let v: serde_json::Value =
            serde_json::from_slice(envelope).map_err(|e| e.to_string())?;
        v["spec_version"].as_u64().map(|s| s as u32).ok_or("no spec_version".into())
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
         para_id: 2999\nnetwork: polkadot\nss58_prefix: 0\n",
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
    assert!(exists("para_sovereign", "polkadot", "para:2999").await);
    assert!(exists("sibl_sovereign", "polkadot-asset-hub", "sibl:2999").await);
    // and AH's sibling sovereign appears on test-para — both directions
    assert!(exists("sibl_sovereign", "test-para", "sibl:1000").await);

    // the same mechanism, for the two chains the registration slice added:
    // no code anywhere knows 1001/1004 exist
    assert!(exists("para_sovereign", "polkadot", "para:1001").await);
    assert!(exists("para_sovereign", "polkadot", "para:1004").await);
    assert!(exists("sibl_sovereign", "polkadot-asset-hub", "sibl:1001").await);
    assert!(exists("sibl_sovereign", "polkadot-collectives", "sibl:1004").await);
    assert!(exists("sibl_sovereign", "polkadot-people", "sibl:1000").await);
    // …and for Hydration, the first NON-SYSTEM parachain, added by Phase 3
    // slice 2 as a seed file and nothing else. Its sovereign appears on every
    // sibling and on the relay because a registration is data.
    assert!(exists("para_sovereign", "polkadot", "para:2034").await);
    assert!(exists("sibl_sovereign", "polkadot-asset-hub", "sibl:2034").await);
    assert!(exists("sibl_sovereign", "hydration", "sibl:1000").await);

    // ss58 agrees with the adapter's own derivation (self-consistency)
    let expected = adapter_substrate::frame_decoder::ss58_encode(
        0,
        &adapter_substrate::accounts::sibling_sovereign(2999),
    );
    let (ss58,): (Option<String>,) = sqlx::query_as(
        "select ss58 from core.account_labels \
         where kind = 'sibl_sovereign' and chain_scope = 'polkadot-asset-hub' \
           and derivation = 'sibl:2999'",
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

    // anchors: insert + read back (insert-ignore on rerun).
    //
    // `frozen` IS NON-NULL HERE ON PURPOSE (slice 7). It has been written since
    // Phase 1 and read by nothing until the consolidation endpoint surfaced it,
    // so its column was never proven to round-trip — and slice 7 widened two
    // hand-maintained sqlx tuples to carry it, where an off-by-one maps the
    // wrong column SILENTLY. The value is the real one slice 6 measured on a
    // live Hydration anchor.
    let ab = adapter_substrate::balances::AccountBalances {
        free: 500_000_000_000,
        reserved: 5_000_000_000,
        frozen: Some(3_387_116_328_755_630_044),
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
    // the widened tuple maps the right column — and `total` is deliberately NOT
    // reduced by it (an anchor's total is free + reserved; frozen is a lock on
    // `free`, not a separate pot)
    assert_eq!(
        anchors[0].frozen.as_deref(),
        Some("3387116328755630044"),
        "frozen must survive the read path it was never exercised on"
    );
    assert_eq!(anchors[0].total, "505000000000");

    db.drop_db().await;
}

// ------------------------------------------------------------- governance

#[tokio::test]
async fn gov_worker_builds_timelines_projection_converges_and_tracks_sync() {
    use adapter_substrate::gov::SubstrateGovMapper;
    use api::GovIndex as _;
    use canonical::{CanonicalBlock, CanonicalEvent, Lineage};

    let Some(db) = TestDb::create().await else { return };
    let reg = seeds();
    sync_registry(&db.pool, &reg).await.expect("registry sync");

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
    let h256 = serde_json::json!([vec![0xabu8; 32]]);

    // THE migration story: ref 1500 submitted + deciding on the relay
    // (Oct 2025), confirmed + approved on Asset Hub (Nov 2025). Plus a
    // fellowship-instance event to prove class separation.
    let index = PgBlockIndex::new(db.pool.clone());
    index
        .insert(block("polkadot", 100, "2025-10-20T00:00:00Z", vec![
            ev(0, "referenda.Submitted", serde_json::json!({
                "index": 1500, "track": 34,
                "proposal": {"Lookup": {"hash": h256, "len": 142}},
            })),
            ev(1, "system.ExtrinsicSuccess", serde_json::json!({})),
        ]))
        .await
        .expect("relay 100");
    index
        .insert(block("polkadot", 110, "2025-10-25T00:00:00Z", vec![ev(
            0,
            "referenda.DecisionStarted",
            serde_json::json!({
                "index": 1500, "track": 34,
                "proposal": {"Lookup": {"hash": h256, "len": 142}},
                "tally": {"ayes": 0, "nays": 0, "support": 0},
            }),
        )]))
        .await
        .expect("relay 110");
    index
        .insert(block("polkadot-asset-hub", 200, "2025-11-10T00:00:00Z", vec![
            ev(0, "referenda.ConfirmStarted", serde_json::json!({"index": 1500})),
            ev(1, "referenda.Confirmed", serde_json::json!({
                "index": 1500, "tally": {"ayes": 9, "nays": 1, "support": 5},
            })),
            ev(2, "referenda.Approved", serde_json::json!({"index": 1500})),
            ev(3, "fellowshipreferenda.Approved", serde_json::json!({"index": 300})),
        ]))
        .await
        .expect("ah 200");
    index
        .insert(block("polkadot-asset-hub", 210, "2025-11-12T00:00:00Z", vec![ev(
            0,
            "referenda.SubmissionDepositRefunded",
            serde_json::json!({"index": 1500, "who": [vec![7u8; 32]], "amount": 10000000000u64}),
        )]))
        .await
        .expect("ah 210");

    let checkpoints = PgCheckpointStore::new(db.pool.clone());
    let source = dotlens_node::balances_pg::PgEventSource::new(db.pool.clone());
    let sink = dotlens_node::gov_pg::PgTimelineSink::new(db.pool.clone());
    let deps = ingest::gov::GovDeps {
        checkpoints: &checkpoints,
        source: &source,
        sink: &sink,
    };

    // OUT OF ORDER on purpose: the info-only refund block first — the
    // projection must hold an 'unknown' placeholder that any real status beats
    ingest::gov::gov_range("polkadot-asset-hub", &SubstrateGovMapper, &deps, 210, 210)
        .await
        .expect("ah refund-first range");
    let idx = api::pg::PgGovIndex::new(db.pool.clone());
    let placeholder = idx
        .referendum("polkadot-asset-hub", "referenda", 1500)
        .await
        .unwrap()
        .expect("placeholder row");
    assert_eq!(placeholder.status, "unknown");

    // now the status events (and the relay side)
    ingest::gov::gov_range("polkadot-asset-hub", &SubstrateGovMapper, &deps, 200, 200)
        .await
        .expect("ah status range");
    ingest::gov::gov_range("polkadot", &SubstrateGovMapper, &deps, 100, 110)
        .await
        .expect("relay range");

    // relay projection: deciding at (110, 0), submission facts captured
    let relay = idx
        .referendum("polkadot", "referenda", 1500)
        .await
        .unwrap()
        .expect("relay row");
    assert_eq!(relay.status, "deciding");
    assert_eq!(relay.status_height, 110);
    assert_eq!(relay.track_id, Some(34));
    assert_eq!(relay.proposal_hash.as_deref(), Some(&format!("0x{}", "ab".repeat(32))[..]));
    assert_eq!(relay.proposal_len, Some(142));
    assert_eq!(relay.submitted_at_height, Some(100));

    // AH projection: approved at (200, 2); the later refund never moved status
    let ah = idx
        .referendum("polkadot-asset-hub", "referenda", 1500)
        .await
        .unwrap()
        .expect("ah row");
    assert_eq!(ah.status, "approved");
    assert_eq!((ah.status_height, ah.track_id), (200, None));

    // fellowship instance is its own class
    let fellowship = idx
        .referendum("polkadot-asset-hub", "fellowship_referenda", 300)
        .await
        .unwrap()
        .expect("fellowship row");
    assert_eq!(fellowship.status, "approved");
    assert!(idx
        .referendum("polkadot-asset-hub", "referenda", 300)
        .await
        .unwrap()
        .is_none());

    // timeline reads join block timestamps, ordered
    let events = idx
        .referendum_events("polkadot-asset-hub", "referenda", 1500)
        .await
        .unwrap();
    assert_eq!(events.len(), 4);
    assert_eq!(events[0].kind, "confirm_started");
    assert_eq!(events[3].kind, "submission_deposit_refunded");
    assert!(events[0].timestamp.is_some());

    // partition routing: nothing in the default partition
    let (in_default,): (i64,) =
        sqlx::query_as("select count(*) from gov.referendum_events_default")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(in_default, 0, "timeline rows must route to per-chain partitions");

    // CONVERGENCE: replay everything (behind the frontier, mixed order) —
    // counts stable, statuses unchanged (older status events don't regress)
    ingest::gov::gov_range("polkadot", &SubstrateGovMapper, &deps, 100, 100)
        .await
        .expect("relay replay of submitted only");
    ingest::gov::gov_range("polkadot-asset-hub", &SubstrateGovMapper, &deps, 200, 210)
        .await
        .expect("ah replay");
    let (total_events,): (i64,) = sqlx::query_as("select count(*) from gov.referendum_events")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(total_events, 7, "insert-ignore: 2 relay + 4 ah + 1 fellowship");
    let relay2 = idx.referendum("polkadot", "referenda", 1500).await.unwrap().unwrap();
    assert_eq!(
        (relay2.status.as_str(), relay2.status_height),
        ("deciding", 110),
        "replaying an older status event must not regress the projection"
    );

    // list surface: newest first
    let listed = idx.list_referenda("polkadot-asset-hub", "referenda", 10).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].referendum_id, 1500);

    // ---- tracks: decoded from the runtime's own archived metadata ----------
    let raw_dir = tmp_raw("gov-tracks");
    let raw = FsRawStore::new(&raw_dir);
    let meta_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/real/polkadot-asset-hub-19498783/metadata.scale");
    match std::fs::read(&meta_path) {
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

            let r1 = dotlens_node::gov_pg::sync_tracks(&db.pool, &reg, &raw)
                .await
                .expect("first track sync");
            assert_eq!(r1.tracks, 16, "OpenGov's 16 tracks from AH metadata");
            // the relay has the governance module but no archived metadata here
            assert!(r1.chains_skipped.contains(&"polkadot".to_string()));

            // idempotent
            let r2 = dotlens_node::gov_pg::sync_tracks(&db.pool, &reg, &raw)
                .await
                .expect("second track sync");
            assert_eq!(r2.tracks, 16);
            let tracks = idx.tracks("polkadot-asset-hub").await.unwrap();
            assert_eq!(tracks.len(), 16);
            let root = tracks.iter().find(|t| t.track_id == 0).expect("track 0");
            assert_eq!((root.name.as_str(), root.pallet.as_str()), ("root", "referenda"));
            assert_eq!(root.spec_version, 2_003_002);
            assert!(root.params.get("decision_period").is_some());
        }
        Err(_) => eprintln!("NOTE: real fixture metadata absent — track-sync assertions skipped"),
    }

    let _ = std::fs::remove_dir_all(&raw_dir);
    db.drop_db().await;
}

#[tokio::test]
async fn preimage_rows_upsert_join_and_never_downgrade() {
    use api::GovIndex as _;
    use dotlens_node::gov_pg::{
        fill_inline_proposal_hash, referenda_needing_preimages, upsert_preimage, PreimageRecord,
    };

    let Some(db) = TestDb::create().await else { return };
    let reg = seeds();
    sync_registry(&db.pool, &reg).await.expect("registry sync");

    let hash = format!("0x{}", "cd".repeat(32));
    // two referenda on AH: one Lookup (hash known), one Inline (hash NULL
    // until decode-preimages hashes the bytes)
    sqlx::query(
        "insert into gov.referenda (chain_id, class, referendum_id, proposal, proposal_hash, \
             proposal_len, status, status_height, status_event_index, runtime_version, mapper_version) \
         values ('polkadot-asset-hub', 'referenda', 2000, $1, $2, 142, 'deciding', 100, 0, 100, 1), \
                ('polkadot-asset-hub', 'referenda', 2001, $3, null, null, 'submitted', 90, 0, 100, 1)",
    )
    .bind(serde_json::json!({"Lookup": {"hash": [vec![0xcdu8; 32]], "len": 142}}))
    .bind(&hash)
    .bind(serde_json::json!({"Inline": [0u8, 1]}))
    .execute(&db.pool)
    .await
    .expect("seed referenda");

    // both pending (the Inline row has no hash yet, the Lookup no preimage)
    let pending = referenda_needing_preimages(&db.pool, "polkadot-asset-hub")
        .await
        .expect("pending list");
    assert_eq!(pending.len(), 2);

    let rec = |status: &str, tree: Option<serde_json::Value>| PreimageRecord {
        proposal_hash: hash.clone(),
        len: 142,
        bytes_location: tree.is_some().then(|| "raw/test/preimage".to_string()),
        call_summary: tree.as_ref().map(|_| "utility.batch".to_string()),
        decoded_call: tree,
        decode_status: status.to_string(),
        source: "state".to_string(),
        note: None,
        spec_version: Some(2_003_002),
        decoder_version: 1,
        fetched_at_height: Some(500),
    };

    // missing → decoded upgrades; a later missing must NOT downgrade
    upsert_preimage(&db.pool, "polkadot-asset-hub", &rec("missing", None)).await.unwrap();
    upsert_preimage(
        &db.pool,
        "polkadot-asset-hub",
        &rec("decoded", Some(serde_json::json!({"call": "utility.batch", "args": {}}))),
    )
    .await
    .unwrap();
    upsert_preimage(&db.pool, "polkadot-asset-hub", &rec("missing", None)).await.unwrap();

    let idx = api::pg::PgGovIndex::new(db.pool.clone());
    let p = idx
        .preimage("polkadot-asset-hub", &hash)
        .await
        .unwrap()
        .expect("preimage row");
    assert_eq!(p.decode_status, "decoded", "decoded rows are never downgraded");
    assert_eq!(p.call_summary.as_deref(), Some("utility.batch"));
    assert_eq!(p.len, 142);

    // the decoded Lookup ref drops out of the pending list; Inline remains
    let pending = referenda_needing_preimages(&db.pool, "polkadot-asset-hub")
        .await
        .expect("pending after decode");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].1, 2001);

    // inline fill: hash lands, coalesce-only (a second fill can't overwrite)
    let inline_hash = format!("0x{}", "ee".repeat(32));
    fill_inline_proposal_hash(&db.pool, "polkadot-asset-hub", "referenda", 2001, &inline_hash, 2)
        .await
        .unwrap();
    fill_inline_proposal_hash(
        &db.pool, "polkadot-asset-hub", "referenda", 2001, "0xdeadbeef", 999,
    )
    .await
    .unwrap();
    let (got_hash, got_len): (Option<String>, Option<i64>) = sqlx::query_as(
        "select proposal_hash, proposal_len from gov.referenda \
         where chain_id = 'polkadot-asset-hub' and class = 'referenda' and referendum_id = 2001",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!((got_hash.as_deref(), got_len), (Some(inline_hash.as_str()), Some(2)));

    db.drop_db().await;
}

#[tokio::test]
async fn vote_facts_land_projections_converge_and_voting_anchors_roundtrip() {
    use adapter_substrate::votes::{SubstrateVoteMapper, VotingPosition};
    use api::GovIndex as _;
    use canonical::{CanonicalBlock, CanonicalEvent, Lineage};

    let Some(db) = TestDb::create().await else { return };
    let reg = seeds();
    sync_registry(&db.pool, &reg).await.expect("registry sync");

    let block = |chain: &str, height: u64, ts: &str, events: Vec<CanonicalEvent>| CanonicalBlock {
        chain_id: chain.into(),
        height,
        hash: format!("0x{height:064x}"),
        parent_hash: format!("0x{:064x}", height - 1),
        timestamp: Some(ts.parse().unwrap()),
        finalized: true,
        lineage: Lineage {
            runtime_version: 2_003_002,
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
    // `[vec![b; 32]]` — NOT `[[b; 32]]`: serde_json::json! rejects array-repeat
    // expressions in array position (the trap that bit slice 1)
    let acct = |b: u8| serde_json::json!([vec![b; 32]]);
    let voter_a = vec![0xa1u8; 32];
    let voter_b = vec![0xb2u8; 32];
    let delegator = vec![0xc3u8; 32];

    let index = PgBlockIndex::new(db.pool.clone());
    // relay, pre-migration: A votes aye with 1x conviction on ref 1500…
    index
        .insert(block("polkadot", 100, "2025-10-20T00:00:00Z", vec![
            ev(0, "convictionvoting.Voted", serde_json::json!({
                "who": acct(0xa1),
                // 2^64 plancks: exercises the NUMERIC path end to end
                "vote": {"Standard": {"vote": [0x81], "balance": "18446744073709551616"}},
                "poll_index": 1500,
            })),
            ev(1, "system.ExtrinsicSuccess", serde_json::json!({})),
        ]))
        .await
        .expect("relay 100");
    // …and a legacy-shape vote (no poll_index) that we can NOT attribute
    index
        .insert(block("polkadot", 105, "2025-10-21T00:00:00Z", vec![ev(
            0,
            "convictionvoting.Voted",
            serde_json::json!({
                "who": acct(0xb2),
                "vote": {"Standard": {"vote": [0x00], "balance": 1005}},
            }),
        )]))
        .await
        .expect("relay 105");
    // Asset Hub, post-migration: B votes nay, C delegates to B, a lock expires
    index
        .insert(block("polkadot-asset-hub", 200, "2025-11-10T00:00:00Z", vec![
            ev(0, "convictionvoting.Voted", serde_json::json!({
                "who": acct(0xb2),
                "vote": {"Standard": {"vote": [0x00], "balance": 1005}},
                "poll_index": 1500,
            })),
            ev(1, "convictionvoting.Delegated", serde_json::json!([acct(0xc3), acct(0xb2), 34])),
            ev(2, "convictionvoting.VoteUnlocked", serde_json::json!({"who": acct(0xa1), "class": 34})),
        ]))
        .await
        .expect("ah 200");
    // …then B withdraws its vote
    index
        .insert(block("polkadot-asset-hub", 210, "2025-11-12T00:00:00Z", vec![ev(
            0,
            "convictionvoting.VoteRemoved",
            serde_json::json!({
                "who": acct(0xb2),
                "vote": {"Standard": {"vote": [0x00], "balance": 1005}},
                "poll_index": 1500,
            }),
        )]))
        .await
        .expect("ah 210");

    let checkpoints = PgCheckpointStore::new(db.pool.clone());
    let source = dotlens_node::balances_pg::PgEventSource::new(db.pool.clone());
    let sink = dotlens_node::votes_pg::PgVoteSink::new(db.pool.clone());
    let deps = ingest::votes::VotesDeps {
        checkpoints: &checkpoints,
        source: &source,
        sink: &sink,
    };

    // OUT OF ORDER on purpose: the withdrawal first, then the vote it removed —
    // the ordering guard must keep the position inactive
    ingest::votes::votes_range("polkadot-asset-hub", &SubstrateVoteMapper, &deps, 210, 210)
        .await
        .expect("ah withdrawal first");
    ingest::votes::votes_range("polkadot-asset-hub", &SubstrateVoteMapper, &deps, 200, 200)
        .await
        .expect("ah vote + delegation");
    ingest::votes::votes_range("polkadot", &SubstrateVoteMapper, &deps, 100, 105)
        .await
        .expect("relay range");

    let idx = api::pg::PgGovIndex::new(db.pool.clone());

    // relay: A's aye position, weights exactly as the pallet tallies them
    let relay_votes = idx.referendum_votes("polkadot", "referenda", 1500, 50).await.unwrap();
    assert_eq!(relay_votes.len(), 1);
    let a = &relay_votes[0];
    assert!(a.active);
    assert_eq!(a.voter, format!("0x{}", "a1".repeat(32)));
    assert_eq!(a.conviction, Some(1));
    assert_eq!(a.aye_votes, "18446744073709551616", "1x conviction: votes = capital");
    assert_eq!(a.support, "18446744073709551616");
    assert_eq!(a.nay_votes, "0");

    // AH: B's position survives the out-of-order replay as INACTIVE
    let ah_votes = idx
        .referendum_votes("polkadot-asset-hub", "referenda", 1500, 50)
        .await
        .unwrap();
    assert_eq!(ah_votes.len(), 1);
    assert!(!ah_votes[0].active, "the later withdrawal wins over the earlier vote");
    assert_eq!(ah_votes[0].nay_votes, "100", "no conviction → capital/10");

    // the legacy unattributed vote: recorded, but never projected
    let (unattributed, ref_null): (i64, i64) = sqlx::query_as(
        "select count(*) filter (where attribution = 'unattributed'), \
                count(*) filter (where referendum_id is null) from gov.votes",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!((unattributed, ref_null), (1, 1));
    let (positions,): (i64,) = sqlx::query_as("select count(*) from gov.vote_positions")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(positions, 2, "A on the relay + B on AH; the legacy vote has no subject");

    // VoteUnlocked is lock bookkeeping: it must produce no fact at all
    let (vote_rows,): (i64,) = sqlx::query_as("select count(*) from gov.votes")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(vote_rows, 4, "2 relay + 1 AH vote + 1 AH withdrawal, no unlock row");

    // delegation edge + projection
    let delegations = idx.account_delegations("polkadot-asset-hub", &delegator).await.unwrap();
    assert_eq!(delegations.len(), 1);
    assert_eq!(delegations[0].track_id, 34);
    assert!(delegations[0].active);
    assert_eq!(delegations[0].target, Some(format!("0x{}", "b2".repeat(32))));

    // partition routing: nothing in the default partitions
    for table in ["gov.votes_default", "gov.delegation_events_default"] {
        let (n,): (i64,) = sqlx::query_as(&format!("select count(*) from {table}"))
            .fetch_one(&db.pool)
            .await
            .unwrap();
        assert_eq!(n, 0, "{table} must stay empty (per-chain partitions)");
    }

    // CONVERGENCE: replay everything behind the frontier — counts stable,
    // the withdrawal still wins
    ingest::votes::votes_range("polkadot-asset-hub", &SubstrateVoteMapper, &deps, 200, 210)
        .await
        .expect("ah replay");
    ingest::votes::votes_range("polkadot", &SubstrateVoteMapper, &deps, 100, 105)
        .await
        .expect("relay replay");
    let (vote_rows_again,): (i64,) = sqlx::query_as("select count(*) from gov.votes")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(vote_rows_again, 4, "insert-ignore: replay adds nothing");
    let ah_again = idx
        .referendum_votes("polkadot-asset-hub", "referenda", 1500, 50)
        .await
        .unwrap();
    assert!(!ah_again[0].active, "replaying the older vote must not reactivate it");

    // account surface: A's vote is findable by voter
    let a_votes = idx.account_votes("polkadot", &voter_a, 10).await.unwrap();
    assert_eq!(a_votes.len(), 1);
    assert_eq!(a_votes[0].referendum_id, 1500);
    assert!(idx.account_votes("polkadot", &voter_b, 10).await.unwrap().is_empty());

    // ---- voting anchors: the numbers no event carries ----------------------
    let position = VotingPosition {
        mode: "delegating".into(),
        delegating_target: Some(voter_b.clone()),
        delegating_balance: Some(5_000_000_000_000),
        delegating_conviction: Some(6),
        delegating_conviction_label: Some("locked6x".into()),
        casting_vote_count: None,
        delegations_votes: Some(0),
        delegations_capital: Some(0),
        prior_until: Some(0),
        prior_balance: Some(0),
        raw: serde_json::json!({"Delegating": {"balance": 5_000_000_000_000u64}}),
    };
    dotlens_node::votes_pg::insert_voting_anchor(
        &db.pool,
        "polkadot-asset-hub",
        &delegator,
        "referenda",
        34,
        200,
        &position,
        Some(2_003_002),
        "test",
        None,
    )
    .await
    .expect("anchor insert");
    // anchors are immutable observations: re-inserting is a no-op
    dotlens_node::votes_pg::insert_voting_anchor(
        &db.pool,
        "polkadot-asset-hub",
        &delegator,
        "referenda",
        34,
        200,
        &VotingPosition::empty(),
        Some(2_003_002),
        "test",
        Some("absent"),
    )
    .await
    .expect("anchor re-insert");

    let anchors = idx.voting_anchors("polkadot-asset-hub", &delegator).await.unwrap();
    assert_eq!(anchors.len(), 1, "same (account, class, track, height) → one row");
    assert_eq!(anchors[0].mode, "delegating", "the first observation is not overwritten");
    assert_eq!(anchors[0].delegating_balance.as_deref(), Some("5000000000000"));
    assert_eq!(anchors[0].delegating_conviction_label.as_deref(), Some("locked6x"));
    assert_eq!(anchors[0].delegating_target, Some(format!("0x{}", "b2".repeat(32))));

    db.drop_db().await;
}

#[tokio::test]
async fn treasury_spends_converge_and_pot_flows_stay_unprojected() {
    use adapter_substrate::treasury::SubstrateTreasuryMapper;
    use api::TreasuryIndex as _;
    use canonical::{CanonicalBlock, CanonicalEvent, Lineage};

    let Some(db) = TestDb::create().await else { return };
    let reg = seeds();
    sync_registry(&db.pool, &reg).await.expect("registry sync");

    let block = |chain: &str, height: u64, ts: &str, events: Vec<CanonicalEvent>| CanonicalBlock {
        chain_id: chain.into(),
        height,
        hash: format!("0x{height:064x}"),
        parent_hash: format!("0x{:064x}", height - 1),
        timestamp: Some(ts.parse().unwrap()),
        finalized: true,
        lineage: Lineage {
            runtime_version: 2_003_002,
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
    let payee = vec![0xd4u8; 32];
    // `[vec![b; 32]]`, never `[[b; 32]]` — the json! array-repeat trap
    let beneficiary = serde_json::json!({"V4": {"parents": 0, "interior": {"X1": [
        {"AccountId32": {"network": null, "id": payee.clone()}}
    ]}}});

    let index = PgBlockIndex::new(db.pool.clone());
    // approval, then a FAILED payment, then a successful retry, then processed
    index
        .insert(block("polkadot-asset-hub", 300, "2026-01-01T00:00:00Z", vec![
            ev(0, "treasury.AssetSpendApproved", serde_json::json!({
                "index": 313,
                "asset_kind": {"V4": {"asset_id": {"parents": 0, "interior": {"X2": [
                    {"PalletInstance": 50}, {"GeneralIndex": 1984}
                ]}}}},
                // > u64: exercises the NUMERIC path
                "amount": "18446744073709551616",
                "beneficiary": beneficiary,
                "valid_from": 300,
                "expire_at": 900,
            })),
            ev(1, "treasury.Deposit", serde_json::json!({"value": 1204000000000u64})),
        ]))
        .await
        .expect("ah 300");
    index
        .insert(block("polkadot-asset-hub", 310, "2026-01-02T00:00:00Z", vec![ev(
            0,
            "treasury.PaymentFailed",
            serde_json::json!({"index": 313, "payment_id": 42}),
        )]))
        .await
        .expect("ah 310");
    index
        .insert(block("polkadot-asset-hub", 320, "2026-01-03T00:00:00Z", vec![
            ev(0, "treasury.Paid", serde_json::json!({"index": 313, "payment_id": 43})),
            // a legacy-shape proposal in the SAME block: different id space,
            // same number — must not collide with asset spend 313
            ev(1, "treasury.Awarded", serde_json::json!({
                "proposal_index": 313, "award": 500, "account": [payee.clone()]
            })),
        ]))
        .await
        .expect("ah 320");
    // `check_status` runs in a LATER block than `payout` — keeping them apart
    // is what makes the payment_id ordering testable at all (in one block the
    // sink's own sort hides the bug)
    index
        .insert(block("polkadot-asset-hub", 325, "2026-01-04T00:00:00Z", vec![ev(
            0,
            "treasury.SpendProcessed",
            serde_json::json!({"index": 313}),
        )]))
        .await
        .expect("ah 325");

    let checkpoints = PgCheckpointStore::new(db.pool.clone());
    let source = dotlens_node::balances_pg::PgEventSource::new(db.pool.clone());
    let sink = dotlens_node::treasury_pg::PgSpendSink::new(db.pool.clone());
    let deps = ingest::treasury::TreasuryDeps {
        checkpoints: &checkpoints,
        source: &source,
        sink: &sink,
    };

    // OUT OF ORDER, and deliberately the WORST order: the terminal block
    // first (so a status-newer, payment-less event owns the status triplet),
    // then the failed payment, then the approval, then the successful retry.
    // Every column must still converge.
    for (from, to) in [(325, 325), (310, 310), (300, 300), (320, 320)] {
        ingest::treasury::treasury_range(
            "polkadot-asset-hub", &SubstrateTreasuryMapper, &deps, from, to,
        )
        .await
        .expect("out-of-order range");
    }

    let idx = api::pg::PgTreasuryIndex::new(db.pool.clone());
    let spend = idx
        .spend("polkadot-asset-hub", "treasury", "asset_spend", 313)
        .await
        .unwrap()
        .expect("asset spend 313");
    // (325,0) SpendProcessed is the newest status, and it does NOT claim success
    assert_eq!(spend.status, "processed");
    // …while the value columns survived the later, valueless events
    assert_eq!(spend.amount.as_deref(), Some("18446744073709551616"));
    assert_eq!(spend.beneficiary.as_deref(), Some(&format!("0x{}", "d4".repeat(32))[..]));
    assert!(spend.asset_kind.is_some(), "asset kind kept from the approval");
    // THE ordering trap: the failed attempt (310) was applied AFTER the
    // successful retry (320) in this run, and the terminal event (325) owns the
    // status. Only the payment's own coordinate gets this right.
    assert_eq!(spend.payment_id.as_deref(), Some("43"), "the successful retry's id");
    assert_eq!(spend.first_seen_height, 300, "least() keeps the earliest sighting");

    // the legacy proposal with the SAME number is a different row entirely
    let proposal = idx
        .spend("polkadot-asset-hub", "treasury", "proposal", 313)
        .await
        .unwrap()
        .expect("proposal 313");
    assert_eq!((proposal.status.as_str(), proposal.amount.as_deref()), ("awarded", Some("500")));

    // pot flows: recorded as facts, never projected
    let pot = idx.pot_events("polkadot-asset-hub", "treasury", 10).await.unwrap();
    assert_eq!(pot.len(), 1);
    assert_eq!((pot[0].kind.as_str(), pot[0].amount.as_deref()), ("pot_deposit", Some("1204000000000")));
    assert!(pot[0].timestamp.is_some(), "joined to the block timestamp");
    let (projected,): (i64,) = sqlx::query_as("select count(*) from treasury.spends")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(projected, 2, "one asset spend + one proposal; the deposit is not a spend");

    // full timeline for the spend, in order
    let events = idx
        .spend_events("polkadot-asset-hub", "treasury", "asset_spend", 313)
        .await
        .unwrap();
    let kinds: Vec<&str> = events.iter().map(|e| e.kind.as_str()).collect();
    assert_eq!(kinds, vec!["approved", "payment_failed", "paid", "processed"]);
    // pot figures say what they MEAN, so nobody sums a balance as a flow
    let (flows, snapshots): (i64, i64) = sqlx::query_as(
        "select count(*) filter (where figure_kind = 'flow'), \
                count(*) filter (where figure_kind = 'snapshot') \
         from treasury.spend_events where attribution = 'pot'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!((flows, snapshots), (1, 0), "the Deposit is a flow");

    // partition routing + convergence on replay
    let (in_default,): (i64,) = sqlx::query_as("select count(*) from treasury.spend_events_default")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(in_default, 0, "facts must route to per-chain partitions");
    ingest::treasury::treasury_range("polkadot-asset-hub", &SubstrateTreasuryMapper, &deps, 300, 325)
        .await
        .expect("replay");
    let (facts,): (i64,) = sqlx::query_as("select count(*) from treasury.spend_events")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(facts, 6, "insert-ignore: replay adds nothing");
    let after = idx
        .spend("polkadot-asset-hub", "treasury", "asset_spend", 313)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.status, "processed", "replaying older events must not regress");

    db.drop_db().await;
}

// ------------------------------------------------------------ reorg safety

#[tokio::test]
async fn unfinalized_rows_are_replaceable_finalized_rows_are_immutable() {
    use canonical::{CanonicalBlock, CanonicalEvent, CanonicalTransaction, Lineage};
    use ingest::tip::UnfinalizedStore as _;

    let Some(db) = TestDb::create().await else { return };
    let reg = seeds();
    sync_registry(&db.pool, &reg).await.expect("registry sync");

    let block = |height: u64, hash: &str, finalized: bool, n_events: u32| CanonicalBlock {
        chain_id: "polkadot".into(),
        height,
        hash: hash.into(),
        parent_hash: "0x00".into(),
        timestamp: None,
        finalized,
        lineage: Lineage {
            runtime_version: 100,
            decoder_version: 2,
            raw_location: format!("raw/polkadot/unfinalized/{height}/{hash}"),
        },
        transactions: vec![CanonicalTransaction {
            index: 0,
            hash: Some(format!("{hash}-tx0")),
            signer: None,
            call: "timestamp.set".into(),
            args: serde_json::json!({}),
            success: true,
        }],
        events: (0..n_events)
            .map(|i| CanonicalEvent {
                index: i,
                transaction_index: Some(0),
                name: "system.ExtrinsicSuccess".into(),
                data: serde_json::json!({}),
            })
            .collect(),
    };
    let index = PgBlockIndex::new(db.pool.clone());
    let store = dotlens_node::tip_pg::PgUnfinalizedStore::new(db.pool.clone());

    // 1. unfinalized fork A lands at height 50 (2 events)
    index.insert(block(50, "0xforkA", false, 2)).await.unwrap();
    assert_eq!(
        store.unfinalized_hash("polkadot", 50).await.unwrap().as_deref(),
        Some("0xforkA")
    );

    // 2. reorg: fork B replaces it entirely — block, transactions, AND events
    index.insert(block(50, "0xforkB", false, 3)).await.unwrap();
    let (hash, tx_hash): (String, String) = sqlx::query_as(
        "select b.hash, t.hash from core.blocks b \
         join core.transactions t on t.chain_id = b.chain_id and t.block_height = b.height \
         where b.chain_id = 'polkadot' and b.height = 50",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!((hash.as_str(), tx_hash.as_str()), ("0xforkB", "0xforkB-tx0"));
    let (ev_count,): (i64,) = sqlx::query_as(
        "select count(*) from core.events where chain_id = 'polkadot' and block_height = 50",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(ev_count, 3, "fork A's events fully replaced, never merged");

    // 3. finalization arrives with fork B → row becomes finalized
    index.insert(block(50, "0xforkB", true, 3)).await.unwrap();
    assert!(store.unfinalized_hash("polkadot", 50).await.unwrap().is_none());
    let (finalized,): (bool,) = sqlx::query_as(
        "select finalized from core.blocks where chain_id = 'polkadot' and height = 50",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert!(finalized);

    // 4. finalized rows are IMMUTABLE: neither a late tip fetch nor a
    // different-hash insert changes anything
    index.insert(block(50, "0xevil", false, 9)).await.unwrap();
    index.insert(block(50, "0xevil", true, 9)).await.unwrap();
    let (hash, ev_count): ((String,), (i64,)) = (
        sqlx::query_as("select hash from core.blocks where chain_id='polkadot' and height=50")
            .fetch_one(&db.pool)
            .await
            .unwrap(),
        sqlx::query_as(
            "select count(*) from core.events where chain_id='polkadot' and block_height=50",
        )
        .fetch_one(&db.pool)
        .await
        .unwrap(),
    );
    assert_eq!(hash.0, "0xforkB");
    assert_eq!(ev_count.0, 3);

    // 5. prune: unfinalized 51..53 vanish (children too), finalized 50 survives
    for h in 51..=53 {
        index.insert(block(h, &format!("0xtip{h}"), false, 1)).await.unwrap();
    }
    let pruned = store.prune_unfinalized_above("polkadot", 50).await.unwrap();
    assert_eq!(pruned, 3);
    let (blocks_left, orphan_events): ((i64,), (i64,)) = (
        sqlx::query_as("select count(*) from core.blocks where chain_id='polkadot'")
            .fetch_one(&db.pool)
            .await
            .unwrap(),
        sqlx::query_as(
            "select count(*) from core.events where chain_id='polkadot' and block_height > 50",
        )
        .fetch_one(&db.pool)
        .await
        .unwrap(),
    );
    assert_eq!(blocks_left.0, 1, "only the finalized block remains");
    assert_eq!(orphan_events.0, 0, "no orphaned children after prune");

    db.drop_db().await;
}

/// Phase 2, slice 6 — WHERE THE MONEY IS.
///
/// Four claims, each of which was a real risk while authoring:
///   1. asset deltas ride the EXISTING balances tables, through the existing
///      worker, keyed by an asset string — no new pipeline
///   2. the holdings read assembles anchor + deltas per (account, asset), and
///      reports a pair it has only ever seen MOVING with a null amount rather
///      than omitting it
///   3. `treasury.spends.asset_location` survives the jsonb round trip well
///      enough to JOIN `core.assets.location_key` — Postgres orders jsonb keys
///      by length, we order them lexicographically, and the join only works
///      because serde_json re-canonicalizes on the way out
///   4. the treasury ACCOUNT list derives itself from the chain's own metadata
#[tokio::test]
async fn asset_balances_share_the_balances_tables_and_holdings_join_their_assets() {
    use adapter_substrate::accounts::{pallet_account, para_sovereign};
    use adapter_substrate::assets as aa;
    use adapter_substrate::balances::SubstrateDeltaMapper;
    use api::{AssetIndex as _, BalanceIndex as _, TreasuryIndex as _};
    use canonical::{CanonicalBlock, CanonicalEvent, Lineage};

    let Some(db) = TestDb::create().await else { return };
    let reg = seeds();
    sync_registry(&db.pool, &reg).await.expect("registry sync");

    let treasury = pallet_account(b"py/trsry");
    let peer = para_sovereign(2034);
    let acct = |a: &[u8; 32]| serde_json::json!([a.to_vec()]);
    let ev = |index: u32, name: &str, data: serde_json::Value| CanonicalEvent {
        index,
        transaction_index: Some(0),
        name: name.into(),
        data,
    };
    // 2^65 — an asset amount that has no business fitting in a u64 either
    let big = "36893488147419103232";
    let block = CanonicalBlock {
        chain_id: "polkadot-asset-hub".into(),
        height: 500,
        hash: format!("0x{:064x}", 500),
        parent_hash: format!("0x{:064x}", 499),
        timestamp: Some("2026-08-01T00:00:00Z".parse().unwrap()),
        finalized: true,
        lineage: Lineage {
            runtime_version: 2_003_002,
            decoder_version: 2,
            raw_location: "raw/polkadot-asset-hub/test/500".into(),
        },
        transactions: vec![],
        events: vec![
            // USDT out of the treasury
            ev(0, "assets.Transferred", serde_json::json!({
                "asset_id": 1984, "from": acct(&treasury), "to": acct(&peer), "amount": big,
            })),
            // a bridged asset minted to the treasury, named by an XCM location
            // in the V4 NESTED-X1 spelling
            ev(1, "foreignassets.Issued", serde_json::json!({
                "asset_id": {"parents": 2, "interior": {"X1": [[
                    {"GlobalConsensus": {"Ethereum": {"chain_id": 1}}}
                ]]}},
                "owner": acct(&treasury), "amount": 7u64,
            })),
            // status, not money
            ev(2, "assets.Frozen", serde_json::json!({
                "asset_id": 1984, "who": acct(&treasury),
            })),
            // and the NATIVE mapper still works in the same pass
            ev(3, "balances.Withdraw", serde_json::json!({
                "who": acct(&treasury), "amount": 160000000u64,
            })),
        ],
    };
    let index = PgBlockIndex::new(db.pool.clone());
    index.insert(block).await.expect("insert block");

    let checkpoints = PgCheckpointStore::new(db.pool.clone());
    let source = dotlens_node::balances_pg::PgEventSource::new(db.pool.clone());
    let sink = dotlens_node::balances_pg::PgDeltaSink::new(db.pool.clone());
    let deps = ingest::balances::BalancesDeps {
        checkpoints: &checkpoints,
        source: &source,
        sink: &sink,
    };
    ingest::balances::balances_range(
        "polkadot-asset-hub",
        &SubstrateDeltaMapper,
        &deps,
        500,
        500,
    )
    .await
    .expect("balances range");

    // ---- 1. asset deltas landed in the balances tables ------------------
    let rows: Vec<(String, String, String, i32)> = sqlx::query_as(
        "select asset, delta::text, reason, mapper_version from balances.balance_changes \
         where chain_id = 'polkadot-asset-hub' and account_id = $1 order by asset, delta",
    )
    .bind(&treasury[..])
    .fetch_all(&db.pool)
    .await
    .expect("changes");
    let by_asset: std::collections::BTreeMap<&str, &(String, String, String, i32)> =
        rows.iter().map(|r| (r.0.as_str(), r)).collect();
    assert_eq!(
        by_asset["assets:1984"].1,
        format!("-{big}"),
        "the 2^65 asset amount survived as NUMERIC"
    );
    assert_eq!(by_asset["assets:1984"].2, "transfer_out");
    assert_eq!(
        by_asset["assets:1984"].3 as u32,
        adapter_substrate::balances::MAPPER_VERSION,
        "an asset delta carries the balances mapper's own version as lineage"
    );
    assert_eq!(by_asset["native"].1, "-160000000", "native mapping untouched");
    let foreign = rows
        .iter()
        .find(|r| r.0.starts_with("foreign:"))
        .expect("the foreign asset delta");
    assert!(
        foreign.0.contains("\"parents\":2"),
        "canonical, version-stripped key: {}",
        foreign.0
    );
    assert_eq!(foreign.1, "7");
    // three assets, four rows (the USDT transfer is double entry, and the peer
    // account holds the other leg), and NO row for the Frozen status event
    let (frozen_rows,): (i64,) = sqlx::query_as(
        "select count(*) from balances.balance_changes where reason like '%frozen%'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(frozen_rows, 0, "a status change is not a balance change");

    // ---- 2. the asset registry, including the constructed XCM name ------
    let usdt_location = aa::local_asset_location(50, 1984);
    let usdt_key = aa::canonical_location(&usdt_location).expect("canonical");
    let ah_path = adapter_substrate::orml::chain_path("polkadot", Some(1000));
    let usdt_absolute =
        adapter_substrate::orml::absolutize(&ah_path, &usdt_location).expect("absolutizes");
    let usdt_absolute_key = usdt_absolute.to_string();
    // the asset id EXACTLY as it sits inside the storage key
    let usdt_id_bytes = 1984u32.to_le_bytes();
    dotlens_node::assets_pg::upsert_asset(
        &db.pool,
        "polkadot-asset-hub",
        "assets:1984",
        "trust_backed",
        Some("1984"),
        Some(&usdt_location),
        Some(&usdt_key),
        // the observer-free name Asset Hub gives its own asset 1984 (0019) —
        // the same string Hydration's row for the same asset must produce
        Some(&usdt_absolute),
        Some(&usdt_absolute_key),
        None,
        Some(&usdt_id_bytes[..]),
        &aa::AssetMeta {
            name: Some("Tether USD".into()),
            symbol: Some("USDT".into()),
            decimals: Some(6),
        },
        &aa::AssetDetailsView {
            supply: Some(1_000_000_000_000),
            min_balance: Some(10_000),
            is_sufficient: Some(true),
            accounts: Some(42),
            status: Some("Live".into()),
        },
        Some(2_003_002),
        Some(500),
        "test",
    )
    .await
    .expect("upsert usdt");
    // an OLDER read must not overwrite newer facts (the observed_height guard)
    dotlens_node::assets_pg::upsert_asset(
        &db.pool,
        "polkadot-asset-hub",
        "assets:1984",
        "trust_backed",
        Some("1984"),
        None,
        None,
        None,
        None,
        None,
        None,
        &aa::AssetMeta { name: None, symbol: Some("STALE".into()), decimals: Some(0) },
        &Default::default(),
        Some(2_000_000),
        Some(100),
        "test-older",
    )
    .await
    .expect("upsert older");
    let assets = api::pg::PgAssetIndex::new(db.pool.clone());
    let listed = assets.assets("polkadot-asset-hub").await.expect("assets");
    let usdt = listed.iter().find(|a| a.asset_key == "assets:1984").unwrap();
    assert_eq!(usdt.symbol.as_deref(), Some("USDT"), "older read must not win");
    assert_eq!(usdt.decimals, Some(6));
    assert_eq!(usdt.location_key.as_deref(), Some(usdt_key.as_str()));

    // ---- 3. treasury accounts, DERIVED from the chain's own metadata ----
    let raw_dir = tmp_raw("holdings");
    let raw = FsRawStore::new(&raw_dir);
    let meta_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/real/polkadot-asset-hub-19498783/metadata.scale");
    let derived = match std::fs::read(&meta_path) {
        Ok(blob) => {
            let key = raw_store::keys::metadata("polkadot-asset-hub", 2_003_002);
            raw.put(&key, &blob, "test").expect("stage metadata");
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
            eprintln!("NOTE: real fixture metadata absent — derivation assertions skipped");
            false
        }
    };
    let report = dotlens_node::assets_pg::sync_treasury_accounts(&db.pool, &reg, &raw)
        .await
        .expect("treasury account sync");
    assert_eq!(report.seeded, 2, "the two seeded AH treasury accounts");
    if derived {
        assert!(report.pots >= 1, "py/trsry derived from AH's own metadata");
        let (label, instance, derivation): (String, Option<String>, Option<String>) =
            sqlx::query_as(
                "select label, instance, derivation from treasury.treasury_accounts \
                 where chain_id = 'polkadot-asset-hub' and account_id = $1 and role = 'pot'",
            )
            .bind(&treasury[..])
            .fetch_one(&db.pool)
            .await
            .expect("the treasury pot row");
        assert_eq!(instance.as_deref(), Some("treasury"));
        assert_eq!(derivation.as_deref(), Some("modl:py/trsry"));
        assert!(label.contains("py/trsry"), "{label}");
    }
    // idempotent
    let before: (i64,) = sqlx::query_as("select count(*) from treasury.treasury_accounts")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    dotlens_node::assets_pg::sync_treasury_accounts(&db.pool, &reg, &raw)
        .await
        .expect("second sync");
    let after: (i64,) = sqlx::query_as("select count(*) from treasury.treasury_accounts")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(before, after, "treasury account sync must be idempotent");

    // ---- 4. holdings: anchor + deltas, with the unanchored pair kept -----
    dotlens_node::assets_pg::insert_asset_anchor(
        &db.pool,
        "polkadot-asset-hub",
        &treasury[..],
        "assets:1984",
        499,
        50_000_000_000,
        Some("liquid"),
        Some(2_003_002),
        "test",
        None,
    )
    .await
    .expect("asset anchor");
    let balances = api::pg::PgBalanceIndex::new(db.pool.clone());
    let holdings = balances
        .holdings("polkadot-asset-hub", &[treasury.to_vec()], None)
        .await
        .expect("holdings");
    let usdt = holdings.iter().find(|h| h.asset == "assets:1984").unwrap();
    // anchored at 499, then the block-500 transfer took 2^65 out of it
    assert_eq!(usdt.anchor_total.as_deref(), Some("50000000000"));
    assert_eq!(usdt.anchor_status.as_deref(), Some("liquid"));
    assert_eq!(usdt.delta_count, 1);
    assert_eq!(usdt.delta_sum, format!("-{big}"));
    assert_eq!(usdt.basis(), "anchor+deltas");
    assert_eq!(
        usdt.amount().unwrap(),
        (50_000_000_000i128 - 36_893_488_147_419_103_232i128).to_string()
    );
    // native and the foreign asset have deltas and NO anchor: present, null
    let native = holdings.iter().find(|h| h.asset == "native").unwrap();
    assert!(native.anchor_total.is_none() && native.amount().is_none());
    assert_eq!(native.basis(), "deltas_only");
    assert!(holdings.iter().any(|h| h.asset.starts_with("foreign:")));
    // …and an `at_height` BEFORE the deltas sees the anchor alone
    let earlier = balances
        .holdings("polkadot-asset-hub", &[treasury.to_vec()], Some(499))
        .await
        .expect("holdings at 499");
    let usdt_then = earlier.iter().find(|h| h.asset == "assets:1984").unwrap();
    assert_eq!(usdt_then.delta_count, 0);
    assert_eq!(usdt_then.amount().unwrap(), "50000000000");
    assert_eq!(usdt_then.basis(), "anchor");
    assert!(
        !earlier.iter().any(|h| h.asset == "native"),
        "a pair whose only rows are above the height must not appear at all"
    );

    // ---- 5. the jsonb round trip that makes the spend → asset join work --
    sqlx::query(
        "insert into treasury.spends \
             (chain_id, instance, spend_kind, spend_id, status, amount, first_seen_height, \
              status_height, status_event_index, runtime_version, mapper_version, \
              asset_location, asset_key) \
         values ('polkadot-asset-hub','treasury','asset_spend',265,'paid',20895000000::numeric, \
                 500,500,0,2003002,2,$1,null)",
    )
    .bind(serde_json::json!({
        "chain": {"parents": 0, "interior": []},
        "asset": serde_json::from_str::<serde_json::Value>(&usdt_key).unwrap(),
    }))
    .execute(&db.pool)
    .await
    .expect("spend row");
    let treasury_index = api::pg::PgTreasuryIndex::new(db.pool.clone());
    let spend = treasury_index
        .spend("polkadot-asset-hub", "treasury", "asset_spend", 265)
        .await
        .expect("spend read")
        .expect("the spend");
    let round_tripped = spend.asset_ref.as_ref().unwrap().pointer("/location/asset")
        .expect("asset half")
        .to_string();
    assert_eq!(
        round_tripped, usdt_key,
        "jsonb orders keys by length, serde_json by bytes — the join depends on \
         the round trip re-canonicalizing, so this is asserted, not assumed"
    );
    assert_eq!(
        listed
            .iter()
            .find(|a| a.location_key.as_deref() == Some(round_tripped.as_str()))
            .map(|a| a.symbol.clone())
            .unwrap(),
        Some("USDT".into()),
        "and the join therefore names the unit: 20895000000 is 20,895 USDT"
    );

    db.drop_db().await;
}

// ------------------------------------------------------------------ bounties

/// THE OUTFLOW NEITHER 0009 NOR 0010 COULD SEE, end to end: three pallets into
/// one table, a parent and a child told apart by the sentinel, a projection
/// that converges when the claim is ingested before the proposal — and the one
/// column in this project that is NOT a pure function of the facts, proved to
/// accumulate exactly once under a replay of the same event.
#[tokio::test]
async fn bounty_facts_converge_and_derived_accounts_join_the_treasury_list() {
    use adapter_substrate::accounts::{para_sovereign, sub_account, SubKey};
    use adapter_substrate::bounties::SubstrateBountyMapper;
    use api::BountyIndex as _;
    use canonical::{CanonicalBlock, CanonicalEvent, Lineage};

    let Some(db) = TestDb::create().await else { return };
    let reg = seeds();
    sync_registry(&db.pool, &reg).await.expect("registry sync");

    let payee = para_sovereign(2034);
    // `[vec![b; 32]]`, never `[[b; 32]]` — the json! array-repeat trap
    let acct = |a: &[u8; 32]| serde_json::json!([a.to_vec()]);
    let ev = |index: u32, name: &str, data: serde_json::Value| CanonicalEvent {
        index,
        transaction_index: Some(0),
        name: name.into(),
        data,
    };
    let block = |height: u64, ts: &str, events: Vec<CanonicalEvent>| CanonicalBlock {
        chain_id: "polkadot-asset-hub".into(),
        height,
        hash: format!("0x{height:064x}"),
        parent_hash: format!("0x{:064x}", height - 1),
        timestamp: Some(ts.parse().unwrap()),
        finalized: true,
        lineage: Lineage {
            runtime_version: 2_003_002,
            decoder_version: 2,
            raw_location: format!("raw/polkadot-asset-hub/test/{height}"),
        },
        transactions: vec![],
        events,
    };
    // 2^65 planck: a payout that has no business fitting in a u64
    let big = "36893488147419103232";
    let index = PgBlockIndex::new(db.pool.clone());
    index
        .insert(block(600, "2026-01-01T00:00:00Z", vec![ev(
            0,
            "bounties.BountyProposed",
            serde_json::json!({"index": 22}),
        )]))
        .await
        .expect("ah 600");
    // an INFO-ONLY event: it names bounty 1 and carries its value, but moves
    // no status. The treasury sink would drop a fact like this; a bounty page
    // that dropped it would never know what any bounty is worth.
    index
        .insert(block(605, "2026-01-02T00:00:00Z", vec![ev(
            0,
            "multiassetbounties.BountyValueIncreased",
            serde_json::json!({"index": 1, "old_value": 100u64, "new_value": "83760000000"}),
        )]))
        .await
        .expect("ah 605");
    index
        .insert(block(610, "2026-01-03T00:00:00Z", vec![ev(
            0,
            "multiassetbounties.BountyPayoutProcessed",
            serde_json::json!({
                "index": 1,
                "child_index": {"None": []},
                "asset_kind": {"V4": [{
                    "location": {"parents": 0, "interior": {"Here": []}},
                    "asset_id": [{"parents": 0, "interior": {"X2": [[
                        {"PalletInstance": [50]}, {"GeneralIndex": [1984]}
                    ]]}}]
                }]},
                "value": "83760000000",
                "beneficiary": acct(&payee),
            }),
        )]))
        .await
        .expect("ah 610");
    index
        .insert(block(620, "2026-01-04T00:00:00Z", vec![
            ev(0, "bounties.BountyClaimed", serde_json::json!({
                "index": 22, "payout": big, "beneficiary": acct(&payee),
            })),
            // the SAME parent id in a different pallet: a child bounty, which
            // the sentinel is what distinguishes
            ev(1, "childbounties.Claimed", serde_json::json!({
                "index": 22, "child_index": 3, "payout": 500u64,
                "beneficiary": acct(&payee),
            })),
        ]))
        .await
        .expect("ah 620");

    let checkpoints = PgCheckpointStore::new(db.pool.clone());
    let source = dotlens_node::balances_pg::PgEventSource::new(db.pool.clone());
    // DELIBERATELY with no PalletId: this is a `bounties-range` run before any
    // metadata was archived, so `account_id` lands NULL and
    // `sync_bounty_accounts` has to converge it later
    let sink = dotlens_node::bounties_pg::PgBountySink::new(db.pool.clone(), None);
    let deps = ingest::bounties::BountyDeps {
        checkpoints: &checkpoints,
        source: &source,
        sink: &sink,
    };
    let idx = api::pg::PgBountyIndex::new(db.pool.clone());

    // OUT OF ORDER, worst first: the claim before the proposal, and the
    // value raise before the payout that gives the bounty a status at all.
    ingest::bounties::bounties_range("polkadot-asset-hub", &SubstrateBountyMapper, &deps, 620, 620)
        .await
        .expect("terminal first");
    ingest::bounties::bounties_range("polkadot-asset-hub", &SubstrateBountyMapper, &deps, 605, 605)
        .await
        .expect("info only");

    // ---- 1. the placeholder: a bounty known ONLY by an info event ---------
    let placeholder = idx
        .bounty("polkadot-asset-hub", "multi_asset_bounties", 1, None)
        .await
        .unwrap()
        .expect("the raise created a row");
    assert_eq!(
        placeholder.status, "unknown",
        "an info-only event records the bounty without claiming to know its state"
    );
    assert_eq!(placeholder.value.as_deref(), Some("83760000000"));
    assert!(placeholder.paid_out.is_none(), "a raise is not a payment");

    ingest::bounties::bounties_range("polkadot-asset-hub", &SubstrateBountyMapper, &deps, 600, 600)
        .await
        .expect("proposal, older than the claim");
    ingest::bounties::bounties_range("polkadot-asset-hub", &SubstrateBountyMapper, &deps, 610, 610)
        .await
        .expect("payout, newer than the raise");

    // ---- 2. three instances, one table; parent and child told apart -------
    let parent = idx
        .bounty("polkadot-asset-hub", "bounties", 22, None)
        .await
        .unwrap()
        .expect("legacy bounty 22");
    assert!(parent.child_id.is_none(), "the -1 sentinel never reaches a reader");
    assert_eq!(parent.status, "claimed", "the older proposal must not regress it");
    assert_eq!(parent.first_seen_height, 600, "least() keeps the earliest sighting");
    assert_eq!(parent.paid_out.as_deref(), Some(big), "2^65 survived as NUMERIC");

    let child = idx
        .bounty("polkadot-asset-hub", "child_bounties", 22, Some(3))
        .await
        .unwrap()
        .expect("child bounty 22-3");
    assert_eq!(child.child_id, Some(3));
    assert_eq!(child.paid_out.as_deref(), Some("500"));
    // …and the same numbers in the legacy instance are a DIFFERENT bounty
    assert!(idx
        .bounty("polkadot-asset-hub", "bounties", 22, Some(3))
        .await
        .unwrap()
        .is_none());

    let modern = idx
        .bounty("polkadot-asset-hub", "multi_asset_bounties", 1, None)
        .await
        .unwrap()
        .expect("multi-asset bounty 1");
    assert_eq!(modern.status, "claimed", "the payout moved it off the placeholder");
    assert_eq!(modern.value.as_deref(), Some("83760000000"));
    assert_eq!(modern.paid_out.as_deref(), Some("83760000000"));
    // the payout names its asset exactly as a treasury spend does, and
    // normalizes to the SAME canonical string core.assets holds for USDT
    let usdt_key = adapter_substrate::assets::canonical_location(
        &adapter_substrate::assets::local_asset_location(50, 1984),
    )
    .expect("canonical");
    assert_eq!(
        modern
            .asset_ref
            .as_ref()
            .and_then(|r| r.pointer("/location/asset"))
            .map(|a| a.to_string()),
        Some(usdt_key),
        "a bounty payout joins core.assets through the same key a spend does"
    );

    let (instances,): (i64,) =
        sqlx::query_as("select count(distinct instance) from treasury.bounties")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(instances, 3, "three pallets, one table");

    // ---- 3. partition routing --------------------------------------------
    let (in_default,): (i64,) =
        sqlx::query_as("select count(*) from treasury.bounty_events_default")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(in_default, 0, "facts must route to per-chain partitions");

    // ---- 4. THE NON-IDEMPOTENT COLUMN, replayed --------------------------
    // Every other projection here is a pure function of the facts, so a replay
    // is free. `paid_out` is a running total, and it is safe ONLY because the
    // sink adds a payout when the FACT ROW was really inserted. Replay the
    // whole span and the totals must not move.
    ingest::bounties::bounties_range("polkadot-asset-hub", &SubstrateBountyMapper, &deps, 600, 620)
        .await
        .expect("replay");
    let (facts,): (i64,) = sqlx::query_as("select count(*) from treasury.bounty_events")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(facts, 5, "insert-ignore: replay adds no facts");
    let replayed = idx
        .bounty("polkadot-asset-hub", "bounties", 22, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        replayed.paid_out.as_deref(),
        Some(big),
        "a payout counted twice is the whole reason this sink reads `returning`"
    );
    let modern_replayed = idx
        .bounty("polkadot-asset-hub", "multi_asset_bounties", 1, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(modern_replayed.paid_out.as_deref(), Some("83760000000"));
    assert_eq!(modern_replayed.status, "claimed", "replay must not regress it");

    // the timeline reads in order, and the child's events are its own
    let timeline = idx
        .bounty_events("polkadot-asset-hub", "bounties", 22, None)
        .await
        .unwrap();
    let kinds: Vec<&str> = timeline.iter().map(|e| e.kind.as_str()).collect();
    assert_eq!(kinds, vec!["proposed", "claimed"]);
    assert!(timeline[0].timestamp.is_some(), "joined to the block timestamp");

    // ---- 5. the accounts, which is what closes the holdings gap ----------
    let raw_dir = tmp_raw("bounties");
    let raw = FsRawStore::new(&raw_dir);
    let meta_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/real/polkadot-asset-hub-19498783/metadata.scale");
    let Ok(blob) = std::fs::read(&meta_path) else {
        eprintln!("NOTE: real fixture metadata absent — account derivation assertions skipped");
        db.drop_db().await;
        return;
    };
    let key = raw_store::keys::metadata("polkadot-asset-hub", 2_003_002);
    raw.put(&key, &blob, "test").expect("stage metadata");
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

    let report = dotlens_node::bounties_pg::sync_bounty_accounts(&db.pool, &reg, &raw)
        .await
        .expect("bounty account sync");
    // every bounty in this fixture is CLAIMED, i.e. terminal, so every account
    // that could be derived is registered INACTIVE: its funds are gone and the
    // holdings sweep (accounts × assets, zeros included) must not keep paying
    // for it. The rows still exist — being retired is not being deleted.
    assert_eq!(report.accounts, 0, "a claimed bounty is not swept");
    assert_eq!(report.deactivated, 2);
    // …and the legacy CHILD bounty is refused outright: its address depends on
    // whether it predates pallet-child-bounties 38.0.0's renumbering, which
    // this table does not record
    assert_eq!(report.underivable, 1, "a legacy child bounty is not derivable");
    assert_eq!(report.linked, 2, "every derivable row's null account_id converged");

    // the address is DERIVED, not curated: bounty 22's money lives at
    // modl ++ py/trsry ++ SCALE(("bt", 22u32))
    let expected = sub_account(b"py/trsry", &[SubKey::Str("bt"), SubKey::Index(22)])
        .expect("derives");
    let (label, derivation, network, active): (String, Option<String>, String, bool) =
        sqlx::query_as(
            "select label, derivation, network, active from treasury.treasury_accounts \
             where chain_id = 'polkadot-asset-hub' and account_id = $1 and role = 'bounty'",
        )
        .bind(&expected[..])
        .fetch_one(&db.pool)
        .await
        .expect("the bounty account row");
    assert_eq!(label, "Bounty 22");
    assert_eq!(derivation.as_deref(), Some("modl:py/trsry/bt/22"));
    assert!(!active, "bounty 22 is claimed, so its account is retired");
    // …and it is registered on the NETWORK the holdings endpoint queries by
    assert_eq!(network, "polkadot");
    // the projection's join column now points at the same address
    let linked = idx
        .bounty("polkadot-asset-hub", "bounties", 22, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        linked.account_id,
        Some(format!("0x{}", hex::encode(expected))),
        "the projection and the account list must name one address"
    );

    // idempotent, and `linked` drops to zero once nothing is null
    let again = dotlens_node::bounties_pg::sync_bounty_accounts(&db.pool, &reg, &raw)
        .await
        .expect("second sync");
    assert_eq!(again.linked, 0, "nothing left to converge");
    let (accounts,): (i64,) = sqlx::query_as(
        "select count(*) from treasury.treasury_accounts where role = 'bounty'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(accounts, 2, "bounty account sync must be idempotent");

    db.drop_db().await;
}

/// XCM facts are one-sided observations that land in the right partition, and
/// the two id SHAPES the chains emit must normalise to one joinable value
/// (Phase 3, slice 2).
#[tokio::test]
async fn xcm_facts_partition_by_chain_and_the_two_id_shapes_join() {
    use adapter_substrate::xcm::SubstrateXcmMapper;
    use api::XcmIndex as _;
    use canonical::{CanonicalBlock, CanonicalEvent, Lineage};

    let Some(db) = TestDb::create().await else { return };
    let reg = seeds();
    sync_registry(&db.pool, &reg).await.expect("registry sync");

    let topic = format!("0x{}", "ee".repeat(32));
    let ev = |index: u32, name: &str, data: serde_json::Value| CanonicalEvent {
        index,
        transaction_index: Some(0),
        name: name.into(),
        data,
    };
    // `[u8;32]` on the sending side, `H256` (one array deeper) on the receiving
    // side — the shape difference that would silently kill every correlation.
    let bytes32 = serde_json::json!(vec![0xeeu8; 32]);
    let h256 = serde_json::json!([vec![0xeeu8; 32]]);

    let block = |chain: &str, height: u64, events: Vec<CanonicalEvent>| CanonicalBlock {
        chain_id: chain.into(),
        height,
        hash: format!("0x{height:064x}"),
        parent_hash: format!("0x{:064x}", height - 1),
        timestamp: Some("2026-08-17T00:00:00Z".parse().unwrap()),
        finalized: true,
        lineage: Lineage {
            runtime_version: 2_003_002,
            decoder_version: 2,
            raw_location: format!("raw/{chain}/test/{height}"),
        },
        transactions: vec![],
        events,
    };

    let blocks = api::pg::PgBlockIndex::new(db.pool.clone());
    api::BlockIndex::insert(
        &blocks,
        block(
            "polkadot-asset-hub",
            900,
            vec![
                ev(0, "polkadotxcm.Sent", serde_json::json!({
                    "origin": {"parents": 0, "interior": {"Here": []}},
                    "destination": {"parents": 1, "interior": {"X1": [{"Parachain": [2034]}]}},
                    "message": [{"WithdrawAsset": []}],
                    "message_id": bytes32,
                })),
                // the SAME message's transport-level record, a second id
                ev(1, "xcmpqueue.XcmpMessageSent", serde_json::json!({
                    "message_hash": serde_json::json!(vec![0x77u8; 32]),
                })),
                ev(2, "balances.Transfer", serde_json::json!({})),
            ],
        ),
    )
    .await
    .expect("insert AH block");
    api::BlockIndex::insert(
        &blocks,
        block(
            "hydration",
            100,
            vec![ev(0, "messagequeue.Processed", serde_json::json!({
                "id": h256,
                "origin": {"Sibling": [1000]},
                "weight_used": {"ref_time": 1_000},
                "success": true,
            }))],
        ),
    )
    .await
    .expect("insert Hydration block");

    let source = dotlens_node::balances_pg::PgEventSource::new(db.pool.clone());
    let sink = dotlens_node::xcm_pg::PgXcmSink::new(db.pool.clone());
    let checkpoints = ingest::pg::PgCheckpointStore::new(db.pool.clone());
    let deps = ingest::xcm::XcmDeps { checkpoints: &checkpoints, source: &source, sink: &sink };
    ingest::xcm::xcm_range("polkadot-asset-hub", &SubstrateXcmMapper, &deps, 900, 900)
        .await
        .expect("map AH");
    ingest::xcm::xcm_range("hydration", &SubstrateXcmMapper, &deps, 100, 100)
        .await
        .expect("map Hydration");

    // balances.Transfer produced nothing; the two XCM events produced two rows.
    let (rows,): (i64,) = sqlx::query_as("select count(*) from xcm.messages")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(rows, 3);
    for (part, want) in [
        ("xcm.messages_p_polkadot_asset_hub", 2),
        ("xcm.messages_p_hydration", 1),
        ("xcm.messages_default", 0),
    ] {
        let (n,): (i64,) = sqlx::query_as(&format!("select count(*) from {part}"))
            .fetch_one(&db.pool)
            .await
            .unwrap_or_else(|e| panic!("counting {part}: {e}"));
        assert_eq!(n, want, "{part} — a new chain must get its own partition");
    }

    // THE JOIN THIS MODULE EXISTS FOR: one id, two chains, two sides — found
    // without naming a chain, and only because [u8;32] and H256 normalised to
    // the same hex.
    let index = api::pg::PgXcmIndex::new(db.pool.clone());
    let both = index.by_message_id(&topic).await.expect("by id");
    assert_eq!(both.len(), 2, "the sending and receiving halves");
    assert_eq!(both[0].chain_id, "hydration");
    assert_eq!((both[0].side.as_str(), both[0].id_kind.as_str()), ("received", "ambiguous"));
    assert_eq!(both[0].transport, "hrmp");
    assert_eq!(both[0].counterparty.as_deref(), Some("para:1000"));
    assert_eq!((both[1].side.as_str(), both[1].id_kind.as_str()), ("sent", "topic"));
    assert_eq!(both[1].counterparty.as_deref(), Some("para:2034"));
    assert!(!both[1].forwarded);

    // The transport-level row is a DIFFERENT id for the same message — recorded
    // separately on purpose, never merged into the topic.
    let wire = index
        .by_message_id(&format!("0x{}", "77".repeat(32)))
        .await
        .unwrap();
    assert_eq!(wire.len(), 1);
    assert_eq!(wire[0].id_kind, "wire_hash");

    // The checkpoint proves MODULE_XCM is this module's own key: get the
    // constant wrong (share another module's) and two workers corrupt one
    // checkpoint while every test still passes. `ingest::xcm` ships no worker
    // tests, so this is the only assertion that pins it.
    let cp = ingest::CheckpointStore::get(&checkpoints, "hydration", ingest::xcm::MODULE_XCM)
        .await
        .expect("checkpoint read")
        .expect("the xcm worker advanced its own checkpoint");
    assert_eq!(cp.last_height, 100);
    assert_eq!(cp.module, ingest::xcm::MODULE_XCM);

    // Re-mapping is a no-op — append-only, insert-ignore.
    ingest::xcm::xcm_range("polkadot-asset-hub", &SubstrateXcmMapper, &deps, 900, 900)
        .await
        .expect("replay");
    let (again,): (i64,) = sqlx::query_as("select count(*) from xcm.messages")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(again, 3, "re-running a range must not duplicate facts");

    db.drop_db().await;
}

/// The correlation layer end to end (Phase 3, slice 3): the two ids one message
/// carries are paired inside the block that emitted both, and the journey then
/// reads across two chains from either of them.
#[tokio::test]
async fn xcm_links_pair_the_two_sender_ids_and_a_journey_reads_from_either() {
    use adapter_substrate::xcm::SubstrateXcmMapper;
    use adapter_substrate::xcm_correlate::SubstrateXcmCorrelator;
    use api::XcmIndex as _;
    use canonical::{CanonicalBlock, CanonicalEvent, Lineage};

    let Some(db) = TestDb::create().await else { return };
    let reg = seeds();
    sync_registry(&db.pool, &reg).await.expect("registry sync");

    let topic = format!("0x{}", "ee".repeat(32));
    let wire = format!("0x{}", "77".repeat(32));
    let ev = |index: u32, name: &str, data: serde_json::Value| CanonicalEvent {
        index,
        transaction_index: None,
        name: name.into(),
        data,
    };
    let block = |chain: &str, height: u64, ts: &str, events: Vec<CanonicalEvent>| CanonicalBlock {
        chain_id: chain.into(),
        height,
        hash: format!("0x{height:064x}"),
        parent_hash: format!("0x{:064x}", height - 1),
        timestamp: Some(ts.parse().unwrap()),
        finalized: true,
        lineage: Lineage {
            runtime_version: 2_003_002,
            decoder_version: 2,
            raw_location: format!("raw/{chain}/test/{height}"),
        },
        transactions: vec![],
        events,
    };

    let blocks = api::pg::PgBlockIndex::new(db.pool.clone());
    // THE EVENT ORDER IS THE EVIDENCE, and it is the order live Asset Hub
    // #19581756 produced: the inner router deposits its hash FIRST, then
    // `WithUniqueTopic::deliver` throws that hash away and pallet-xcm deposits
    // the topic. Reverse these two and the correlator refuses to pair them.
    api::BlockIndex::insert(
        &blocks,
        block(
            "polkadot-asset-hub",
            900,
            "2026-08-17T09:00:00Z",
            vec![
                ev(0, "xcmpqueue.XcmpMessageSent", serde_json::json!({
                    "message_hash": vec![0x77u8; 32],
                })),
                ev(1, "polkadotxcm.Sent", serde_json::json!({
                    "origin": {"parents": 0, "interior": {"Here": []}},
                    "destination": {"parents": 1, "interior": {"X1": [[{"Parachain": [2034]}]]}},
                    "message": [[{"WithdrawAsset": []}]],
                    "message_id": vec![0xeeu8; 32],
                })),
            ],
        ),
    )
    .await
    .expect("insert AH block");
    api::BlockIndex::insert(
        &blocks,
        block(
            "hydration",
            100,
            "2026-08-17T09:00:24Z",
            vec![ev(0, "messagequeue.Processed", serde_json::json!({
                // H256 — one array layer deeper than the sender's [u8;32]
                "id": [vec![0xeeu8; 32]],
                "origin": {"Sibling": [1000]},
                "weight_used": {"ref_time": 1_000},
                "success": true,
            }))],
        ),
    )
    .await
    .expect("insert Hydration block");

    let source = dotlens_node::balances_pg::PgEventSource::new(db.pool.clone());
    let checkpoints = ingest::pg::PgCheckpointStore::new(db.pool.clone());
    let facts = dotlens_node::xcm_pg::PgXcmSink::new(db.pool.clone());
    let fact_deps =
        ingest::xcm::XcmDeps { checkpoints: &checkpoints, source: &source, sink: &facts };
    let links = dotlens_node::xcm_links_pg::PgXcmLinkSink::new(db.pool.clone());
    let link_deps = ingest::xcm_correlate::XcmCorrelateDeps {
        checkpoints: &checkpoints,
        source: &source,
        sink: &links,
    };
    for (chain, h) in [("polkadot-asset-hub", 900u64), ("hydration", 100)] {
        ingest::xcm::xcm_range(chain, &SubstrateXcmMapper, &fact_deps, h, h)
            .await
            .expect("map facts");
        ingest::xcm_correlate::xcm_correlate_range(
            chain,
            &SubstrateXcmCorrelator,
            &link_deps,
            h,
            h,
        )
        .await
        .expect("correlate");
    }

    // ONE link, on the sending chain only — Hydration saw one id and has
    // nothing to pair.
    for (part, want) in [
        ("xcm.message_links_p_polkadot_asset_hub", 1),
        ("xcm.message_links_p_hydration", 0),
        ("xcm.message_links_default", 0),
    ] {
        let (n,): (i64,) = sqlx::query_as(&format!("select count(*) from {part}"))
            .fetch_one(&db.pool)
            .await
            .unwrap_or_else(|e| panic!("counting {part}: {e}"));
        assert_eq!(n, want, "{part} — links partition by chain like every fact table");
    }

    let index = api::pg::PgXcmIndex::new(db.pool.clone());
    // The alias is findable from BOTH ends: that bidirectionality is what the
    // two indexes in 0016 exist for.
    for from in [&topic, &wire] {
        let found = index.aliases(from).await.expect("aliases");
        assert_eq!(found.len(), 1, "one link, reachable from either id");
        assert_eq!(found[0].wire_hash, wire);
        assert_eq!(found[0].topic, topic);
        assert_eq!(found[0].transport, "hrmp");
        assert_eq!(found[0].rule, "unique_in_block");
        assert_eq!(found[0].confidence, "high");
        assert_eq!(found[0].evidence["event_gap"], 1);
        assert_eq!(found[0].correlator_version, 2);
        assert_eq!(found[0].runtime_version, 2_003_002, "lineage: which runtime, which rule");
        assert_eq!(found[0].evidence["block_sends"]["wire"], 1);
        assert_eq!((found[0].wire_event_index, found[0].topic_event_index), (0, 1));
    }

    // The journey read: three observations across two chains, ordered by the
    // only clock they share — which the `core.blocks` join is what supplies.
    let rows = index
        .by_message_ids(&[topic.clone(), wire.clone()])
        .await
        .expect("by ids");
    assert_eq!(rows.len(), 3);
    assert!(
        rows.iter().all(|r| r.timestamp.is_some()),
        "the core.blocks join is what makes a cross-chain ordering possible"
    );
    let hydration = rows.iter().find(|r| r.chain_id == "hydration").unwrap();
    let ah_topic = rows
        .iter()
        .find(|r| r.chain_id == "polkadot-asset-hub" && r.id_kind == "topic")
        .unwrap();
    assert!(
        hydration.timestamp > ah_topic.timestamp,
        "the receive is after the send — the journey's time_order check in one line"
    );

    // Its own checkpoint key. Sharing `xcm`'s would make each worker's progress
    // silently skip the other's work, and nothing else in the suite pins it.
    let cp = ingest::CheckpointStore::get(
        &checkpoints,
        "polkadot-asset-hub",
        ingest::xcm_correlate::MODULE_XCM_CORRELATE,
    )
    .await
    .expect("checkpoint read")
    .expect("the correlator advanced its own checkpoint");
    assert_eq!(cp.last_height, 900);
    assert_ne!(
        ingest::xcm_correlate::MODULE_XCM_CORRELATE,
        ingest::xcm::MODULE_XCM
    );

    // Re-running converges rather than accumulating: this sink is
    // delete-then-insert, not insert-ignore, because a link is a conclusion the
    // current rule reached and a re-run must be able to replace it.
    ingest::xcm_correlate::xcm_correlate_range(
        "polkadot-asset-hub",
        &SubstrateXcmCorrelator,
        &link_deps,
        900,
        900,
    )
    .await
    .expect("replay");
    let (again,): (i64,) = sqlx::query_as("select count(*) from xcm.message_links")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(again, 1, "re-correlating a block must replace, never duplicate");

    db.drop_db().await;
}

/// Tier 1 results are immutable observations keyed by STATE + INPUT (Phase 3,
/// slice 1). The three properties that matter, and each has bitten a projection
/// elsewhere in this project: a recorded answer is never rewritten, two states
/// hold two answers, and the read path returns them newest-state first.
#[tokio::test]
async fn simulation_results_are_immutable_per_state_and_read_back_newest_first() {
    use api::SimIndex as _;
    use dotlens_node::sim_pg::{insert_simulation, simulation_at, PgSimStore};
    use sim::{SimRecord, SimStore as _};

    let Some(db) = TestDb::create().await else { return };
    let reg = seeds();
    sync_registry(&db.pool, &reg).await.expect("registry sync");

    let call_hash = format!("0x{}", "ab".repeat(32));
    // 2^65 planck: a dry-run event body carries the same big numbers a real one
    // does, and it must survive JSONB as a decimal STRING, not a float.
    let big = "36893488147419103232";
    let record = |block: &str, input: &str, height: u64, status: &str| SimRecord {
        chain_id: "polkadot-asset-hub".into(),
        at_block_hash: block.into(),
        input_hash: input.into(),
        at_height: height,
        tier: sim::TIER_DRY_RUN.into(),
        call_hash: call_hash.clone(),
        call_summary: Some("multiassetbounties.fund_bounty".into()),
        origin_spec: "Origins:MediumSpender".into(),
        origin_json: serde_json::json!({"resolved": "Origins:MediumSpender"}),
        xcm_version: Some(4),
        status: status.into(),
        // NULL for api_error, and the distinction is the point: "the call
        // failed" and "we never got to try" are different facts, and 0014 says
        // api_error means no dispatch was attempted. A fixture that writes
        // `false` there would teach the wrong shape (slice 7's green-test-with-
        // a-wrong-number, one column over).
        dispatch_ok: (status != "api_error").then(|| status == "executed"),
        dispatch_error: (status == "dispatch_failed")
            .then(|| serde_json::json!({"error": "assets.NoAccount"})),
        emitted_events: serde_json::json!([
            {"name": "balances.Withdraw", "data": {"amount": big}}
        ]),
        event_count: 1,
        local_xcm: None,
        forwarded_xcms: Some(serde_json::json!([])),
        effects: serde_json::json!({"Ok": [{"emitted_events": []}]}),
        note: None,
        spec_version: 2_003_002,
        api_version: Some(2),
        metadata_version: 15,
        sim_version: 1,
        raw_location: format!(
            "raw/polkadot-asset-hub/sim/{block}/{input}/DryRunApi_dry_run_call.response.scale"
        ),
        // No baseline on these rows: this test is about immutability and
        // ordering, and every row recorded before slice 5 looks exactly like
        // this. `a_previewed_arrival_is_its_own_row_and_the_baseline_link_is_a_key`
        // is where the link is exercised.
        baseline_input_hash: None,
        overrides: None,
        override_hash: None,
        storage_diff: None,
        storage_diff_count: None,
        diff_status: None,
        built_block_hash: None,
        harness: None,
        // dry_run: no scheduler, so no route and no anchor.
        dispatch_route: None,
        agenda_anchor: None,
    };

    let first = record("0xaa", "0x01", 19_000_000, "dispatch_failed");
    insert_simulation(&db.pool, &first).await.expect("insert");

    let back = simulation_at(&db.pool, "polkadot-asset-hub", "0xaa", "0x01", "dry_run")
        .await
        .expect("read")
        .expect("row exists");
    assert_eq!(back.status, "dispatch_failed");
    assert_eq!(back.event_count, 1);
    assert_eq!(back.spec_version, 2_003_002);
    assert_eq!(
        back.emitted_events[0]["data"]["amount"], big,
        "a u128 planck amount survives JSONB as a decimal string"
    );

    // Same state, same input, a DIFFERENT answer: refused silently, and the
    // first answer stands. Overwriting would erase the evidence that a runtime
    // gave two different answers to one question — which is the only thing that
    // could ever tell us something is wrong.
    let contradiction = record("0xaa", "0x01", 19_000_000, "executed");
    insert_simulation(&db.pool, &contradiction)
        .await
        .expect("second insert is a no-op, not an error");
    let back = simulation_at(&db.pool, "polkadot-asset-hub", "0xaa", "0x01", "dry_run")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        back.status, "dispatch_failed",
        "an immutable observation is never rewritten"
    );

    // The TIER is part of the key, so a fork answer at the same state and input
    // is a SEPARATE row — and, crucially, a Tier 2 lookup does not find the
    // Tier 1 row and report it as cached.
    let mut fork = record("0xaa", "0x01", 19_000_000, "executed");
    fork.tier = "fork".into();
    // A fork row must name its route — `simulation_results_fork_names_its_route`
    // enforces it, because the coverage list served with the row is selected
    // from this column.
    fork.dispatch_route = Some(sim::ROUTE_SCHEDULED.into());
    // …and it must say what its diff covers, for the same reason one column
    // over: `simulation_results_fork_names_its_diff_scope` (0023) makes "NULL
    // means this is not a fork row" a guarantee rather than a convention, so a
    // fork row with no diff scope is a shape production cannot produce — the
    // runner always records one of the five. `extrinsic_only` is what the live
    // scheduled route writes, and on THIS route it does not cover the call.
    fork.diff_status = Some(sim::DIFF_STATUS_EXTRINSIC_ONLY.into());
    insert_simulation(&db.pool, &fork)
        .await
        .expect("a different tier is a different row");
    assert_eq!(
        simulation_at(&db.pool, "polkadot-asset-hub", "0xaa", "0x01", "fork")
            .await
            .unwrap()
            .unwrap()
            .status,
        "executed",
        "the fork tier keeps its own answer"
    );

    // A DIFFERENT state is a different answer and gets its own row — this is
    // why the key is the block hash and not the height.
    insert_simulation(&db.pool, &record("0xbb", "0x01", 19_000_500, "executed"))
        .await
        .expect("insert at another state");
    // …and the same state with a different INPUT (another origin, say) too.
    insert_simulation(&db.pool, &record("0xbb", "0x02", 19_000_500, "api_error"))
        .await
        .expect("insert with another input");

    let (rows,): (i64,) = sqlx::query_as("select count(*) from sim.simulation_results")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(rows, 4);

    // The read path: newest state first, then the full key as tie-break, so the
    // order is TOTAL — two backends cannot disagree and `limit` cannot wobble.
    let index = api::pg::PgSimIndex::new(db.pool.clone());
    let all = index
        .simulations("polkadot-asset-hub", &call_hash, 10)
        .await
        .expect("read back");
    assert_eq!(all.len(), 4);
    assert_eq!(all[0].at_height, 19_000_500);
    assert_eq!(all[0].input_hash, "0x01", "ties break on input_hash, ascending");
    assert_eq!(all[1].input_hash, "0x02");
    assert_eq!(all[2].at_height, 19_000_000);
    assert_eq!(
        (all[2].tier.as_str(), all[3].tier.as_str()),
        ("dry_run", "fork"),
        "two tiers at one state are ordered, not arbitrary"
    );
    assert_eq!(all[0].metadata_version, 15, "lineage survives the round trip");
    let one = index
        .simulations("polkadot-asset-hub", &call_hash, 1)
        .await
        .unwrap();
    assert_eq!(one.len(), 1);
    assert_eq!(one[0].at_height, 19_000_500);

    // Another chain's answer is never borrowed, and an unknown call is empty.
    assert!(index
        .simulations("polkadot", &call_hash, 10)
        .await
        .unwrap()
        .is_empty());
    assert!(index
        .simulations("polkadot-asset-hub", &format!("0x{}", "cd".repeat(32)), 10)
        .await
        .unwrap()
        .is_empty());

    // The SimStore trait the orchestration drives goes through the same rows.
    let store = PgSimStore::new(db.pool.clone());
    assert!(store
        .get("polkadot-asset-hub", "0xaa", "0x01", "dry_run")
        .await
        .unwrap()
        .is_some());
    assert!(store
        .get("polkadot-asset-hub", "0xaa", "0xff", "dry_run")
        .await
        .unwrap()
        .is_none());

    db.drop_db().await;
}

/// A FORK ROW READ BACK THROUGH BOTH READERS, with every column a fork row
/// leaves NULL actually NULL (Phase 3, slice 10).
///
/// THIS IS THE TEST SLICE 8 NEEDED AND SLICE 9 PROMISED. Slice 8's verification
/// found `forwarded_xcms: Some(r.try_get(…)?)` in `PgSimIndex`'s row reader —
/// migration 0021 made that column nullable and every fork row leaves it NULL,
/// and `try_get::<Value, _>` on a NULL is a decode ERROR. So reading ANY fork row
/// through the API would have failed, on the one surface the tier exists to
/// serve, and it was caught only because the same change had to compile. Nothing
/// tested it. The test above inserts a fork row with `forwarded_xcms: Some([])`
/// inherited from a dry-run fixture, so it does not exercise the NULL at all.
///
/// The four nullable columns are asserted TOGETHER because they fail the same
/// way: a bare `try_get` on any of them is a runtime error no compiler sees, and
/// each one is null on a fork row for its own reason.
#[tokio::test]
async fn a_fork_row_round_trips_through_both_readers_with_its_null_columns_null() {
    use api::SimIndex as _;
    use dotlens_node::sim_pg::{insert_simulation, simulation_at};
    use sim::SimRecord;

    let Some(db) = TestDb::create().await else { return };
    let reg = seeds();
    sync_registry(&db.pool, &reg).await.expect("registry sync");

    let call_hash = format!("0x{}", "f0".repeat(32));
    let row = SimRecord {
        chain_id: "polkadot-asset-hub".into(),
        at_block_hash: format!("0x{}", "8e".repeat(32)),
        input_hash: format!("0x{}", "f1".repeat(32)),
        at_height: 19_368_576,
        tier: sim::TIER_FORK.into(),
        call_hash: call_hash.clone(),
        call_summary: Some("multiassetbounties.fund_bounty".into()),
        origin_spec: "Origins:MediumSpender".into(),
        origin_json: serde_json::json!({"resolved": "Origins:MediumSpender"}),
        // NULL #1 and #2: this tier calls no runtime API, so there is no
        // DryRunApi version and no `result_xcms_version`. Read as 0 they would
        // put "DryRunApi v0" on a row that asked nothing.
        xcm_version: None,
        api_version: None,
        status: "executed".into(),
        dispatch_ok: Some(true),
        dispatch_error: None,
        emitted_events: serde_json::json!([
            {"name": "assets.Transferred", "data": {"amount": "83760000000"}}
        ]),
        event_count: 1,
        local_xcm: None,
        // NULL #3 — THE ONE THAT BROKE. A fork run produces no forwarded list.
        forwarded_xcms: None,
        effects: serde_json::json!({"diff_method": "dev_dryRun"}),
        note: None,
        spec_version: 2_003_002,
        metadata_version: 15,
        sim_version: 2,
        raw_location: "raw/polkadot-asset-hub/sim/8e/f1/chopsticks_fork.response.json".into(),
        // NULL #4: a fork run has no forwarded list, so nothing to difference.
        baseline_input_hash: None,
        overrides: None,
        override_hash: None,
        storage_diff: Some(serde_json::json!([{"key": "0x26aa394e", "change": "changed"}])),
        storage_diff_count: Some(1),
        diff_status: Some(sim::DIFF_STATUS_EXTRINSIC_ONLY.into()),
        // NULL #5: no block is built on the live route.
        built_block_hash: None,
        harness: Some(serde_json::json!({"tool": "chopsticks", "version": "1.5.1"})),
        dispatch_route: Some(sim::ROUTE_SCHEDULED.into()),
        agenda_anchor: Some(serde_json::json!({
            "provider": "relay", "at_parent": 32_519_445u64, "written_at": 32_519_445u64,
        })),
    };
    insert_simulation(&db.pool, &row).await.expect("insert a fork row");

    // ---- READER 1: the runner's cache path. A failure here means a second run
    // at one state cannot find its own answer and re-runs a fork.
    let back = simulation_at(
        &db.pool,
        "polkadot-asset-hub",
        &row.at_block_hash,
        &row.input_hash,
        sim::TIER_FORK,
    )
    .await
    .expect("the fork row reads back")
    .expect("it is there");
    assert!(back.forwarded_xcms.is_none(), "NULL stays None, never Some(null)");
    assert!(back.xcm_version.is_none());
    assert!(back.api_version.is_none());
    assert!(back.built_block_hash.is_none());
    assert!(back.baseline_input_hash.is_none());
    assert_eq!(back.diff_status.as_deref(), Some(sim::DIFF_STATUS_EXTRINSIC_ONLY));
    assert_eq!(back.dispatch_route.as_deref(), Some(sim::ROUTE_SCHEDULED));
    assert_eq!(back.agenda_anchor.as_ref().unwrap()["provider"], "relay");
    assert_eq!(back.sim_version, 2, "lineage survives the round trip");

    // ---- READER 2: the API's, which is where the defect actually lived.
    let index = api::pg::PgSimIndex::new(db.pool.clone());
    let served = index
        .simulations("polkadot-asset-hub", &call_hash, 10)
        .await
        .expect("a fork row is READABLE through the surface this tier exists to serve");
    assert_eq!(served.len(), 1);
    let s = &served[0];
    assert!(s.forwarded_xcms.is_none());
    assert!(s.xcm_version.is_none());
    assert!(s.api_version.is_none());
    assert!(s.built_block_hash.is_none());
    assert_eq!(s.storage_diff_count, Some(1));
    assert_eq!(s.diff_status.as_deref(), Some(sim::DIFF_STATUS_EXTRINSIC_ONLY));

    // …and through the single-row lookup, which is a SECOND hand-maintained
    // column list. Two `select`s that name the same columns are a transposition
    // waiting for a column of the same type to be added.
    let one = index
        .simulation_at(
            "polkadot-asset-hub",
            &row.at_block_hash,
            &row.input_hash,
            sim::TIER_FORK,
        )
        .await
        .expect("point lookup")
        .expect("it is there");
    assert!(one.forwarded_xcms.is_none());
    assert_eq!(one.dispatch_route.as_deref(), Some(sim::ROUTE_SCHEDULED));

    // ---- THE VOCABULARY IS AN INTEGRITY GUARANTEE, proven in BOTH directions.
    // 0023's CHECK is what stops a typo'd status being accepted silently and then
    // read as "no diff" — a blank column beside a status claiming it was read.
    let bad = sqlx::query(
        "update sim.simulation_results set diff_status = 'partial' where tier = 'fork'",
    )
    .execute(&db.pool)
    .await;
    assert!(
        bad.is_err(),
        "a status outside the five-value vocabulary must be REJECTED by the database"
    );
    for ok in sim::DIFF_STATUSES {
        sqlx::query("update sim.simulation_results set diff_status = $1 where tier = 'fork'")
            .bind(ok)
            .execute(&db.pool)
            .await
            .unwrap_or_else(|e| panic!("'{ok}' is in the vocabulary and must be accepted: {e}"));
    }
    // AND NULL IS REFUSED ON A FORK ROW, which is what makes 0023's "NULL means
    // this is not a fork row" a guarantee rather than a sentence above a
    // constraint that permits the opposite. Without this the API had to invent a
    // value for a NULL, and the only available invention — "unavailable" — is a
    // POSITIVE claim about a run nobody made it of.
    let nulled = sqlx::query(
        "update sim.simulation_results set diff_status = null where tier = 'fork'",
    )
    .execute(&db.pool)
    .await;
    assert!(
        nulled.is_err(),
        "a fork row must name what its diff covers — see \
         simulation_results_fork_names_its_diff_scope"
    );
    // …while a NON-fork row is exactly where NULL belongs, which proves the
    // constraint is scoped to the TIER rather than to the column. Built through
    // the same writer as everything else, so the column list cannot drift from
    // the one production uses.
    let dry = SimRecord {
        input_hash: format!("0x{}", "dd".repeat(32)),
        tier: sim::TIER_DRY_RUN.into(),
        xcm_version: Some(4),
        api_version: Some(2),
        diff_status: None,
        storage_diff: None,
        storage_diff_count: None,
        dispatch_route: None,
        agenda_anchor: None,
        harness: None,
        forwarded_xcms: Some(serde_json::json!([])),
        ..row.clone()
    };
    insert_simulation(&db.pool, &dry)
        .await
        .expect("a dry_run row carries no diff_status at all, and NULL is what that means");

    db.drop_db().await;
}

/// The receiving side gets its own table, and the baseline link is a KEY into
/// the sending side's (Phase 3, slice 5).
///
/// Four properties, each of which would be invisible until it mattered: an
/// arrival is immutable per (state, input) like a call is; the baseline link
/// resolves through the ORDINARY simulation read, because a baseline is an
/// ordinary row; legs come back in the SENDER's list order rather than newest
/// first; and a preview of a program with no source is not mistaken for a leg of
/// anything.
#[tokio::test]
async fn a_previewed_arrival_is_its_own_row_and_the_baseline_link_is_a_key() {
    use api::{SimIndex as _, XcmSimIndex as _};
    use dotlens_node::sim_pg::{
        insert_simulation, insert_xcm_simulation, xcm_simulation_at, PgXcmSimStore,
    };
    use sim::{SimRecord, XcmSimRecord, XcmSimStore as _};

    let Some(db) = TestDb::create().await else { return };
    let reg = seeds();
    sync_registry(&db.pool, &reg).await.expect("registry sync");

    // ---- the sending side: a call and the no-op baseline beside it ----------
    let msg = |n: u32| serde_json::json!({"V4": [[[{"Transact": {"call": {"encoded": [n]}}}]]]});
    let dest = serde_json::json!({"V4": [{"parents": 1, "interior":
        {"X1": [[{"Parachain": [2034]}]]}}]});
    let call = |input: &str, summary: &str, messages: serde_json::Value, baseline: Option<&str>| {
        SimRecord {
            chain_id: "polkadot-asset-hub".into(),
            at_block_hash: "0xaa".into(),
            input_hash: input.into(),
            at_height: 19_000_000,
            tier: sim::TIER_DRY_RUN.into(),
            call_hash: format!("0xcall{input}"),
            call_summary: Some(summary.into()),
            origin_spec: "root".into(),
            origin_json: serde_json::json!({"resolved": "system:Root"}),
            xcm_version: Some(4),
            status: "executed".into(),
            dispatch_ok: Some(true),
            dispatch_error: None,
            emitted_events: serde_json::json!([]),
            event_count: 0,
            local_xcm: None,
            forwarded_xcms: Some(serde_json::json!([{"destination": dest, "messages": messages}])),
            effects: serde_json::json!({"Ok": []}),
            note: None,
            spec_version: 2_003_002,
            api_version: Some(2),
            metadata_version: 15,
            sim_version: 2,
            raw_location: format!("raw/polkadot-asset-hub/sim/aa/{input}/x.response.scale"),
            baseline_input_hash: baseline.map(str::to_string),
            overrides: None,
            override_hash: None,
            storage_diff: None,
            storage_diff_count: None,
            diff_status: None,
            built_block_hash: None,
            harness: None,
            // dry_run: no scheduler, so no route and no anchor.
            dispatch_route: None,
            agenda_anchor: None,
        }
    };
    // The baseline is its own baseline — a no-op differenced against itself is
    // the empty set, which is what a call that queues nothing sent.
    insert_simulation(
        &db.pool,
        &call("0xba", "system.remark", serde_json::json!([msg(1)]), Some("0xba")),
    )
    .await
    .expect("baseline insert");
    insert_simulation(
        &db.pool,
        &call(
            "0x01",
            "xcmpallet.send",
            serde_json::json!([msg(1), msg(9)]),
            Some("0xba"),
        ),
    )
    .await
    .expect("subject insert");

    // The link resolves through the ORDINARY read: a baseline is not a special
    // kind of row, which is exactly why running it through the same path was
    // worth doing.
    let sims = api::pg::PgSimIndex::new(db.pool.clone());
    let subject = sims
        .simulation_at("polkadot-asset-hub", "0xaa", "0x01", "dry_run")
        .await
        .unwrap()
        .expect("subject");
    let baseline_hash = subject.baseline_input_hash.clone().expect("link recorded");
    let baseline = sims
        .simulation_at("polkadot-asset-hub", "0xaa", &baseline_hash, "dry_run")
        .await
        .unwrap()
        .expect("baseline row");
    let attribution = sim::attribute_forwarded(
        subject.forwarded_xcms.as_ref().expect("a dry_run row has a forwarded list"),
        baseline.forwarded_xcms.as_ref().expect("a dry_run baseline has one too"),
    );
    assert_eq!(attribution.total_messages, 2);
    assert_eq!(attribution.ambient_messages, 1);
    assert_eq!(
        attribution.attributed_messages, 1,
        "the difference survives a round trip through JSONB — which it only does if both \
         sides render identically, and they do because the same decoder produced both"
    );
    assert_eq!(attribution.destinations[0].messages[0].message_index, 1);

    // ---- the receiving side -------------------------------------------------
    let arrival = |chain: &str, input: &str, status: &str, source: Option<(u32, u32)>| {
        XcmSimRecord {
            chain_id: chain.into(),
            at_block_hash: "0xdd".into(),
            input_hash: input.into(),
            at_height: 13_663_124,
            tier: sim::TIER_DRY_RUN.into(),
            program_hash: format!("0xprog{input}"),
            program: msg(9),
            program_summary: Some("Transact".into()),
            origin_location: serde_json::json!({"V4": [{"parents": 1, "interior":
                {"X1": [[{"Parachain": [1000]}]]}}]}),
            origin_ref: "para:1000".into(),
            status: status.into(),
            weight_used: (status != "not_started")
                .then(|| serde_json::json!({"ref_time": 1_000, "proof_size": 2_000})),
            xcm_error: (status != "complete")
                .then(|| serde_json::json!({"index": 0, "error": {"Barrier": []}})),
            emitted_events: serde_json::json!([
                {"name": "balances.Minted", "data": {"amount": "36893488147419103232"}}
            ]),
            event_count: 1,
            forwarded_xcms: serde_json::json!([]),
            baseline_input_hash: Some("0xempty".into()),
            effects: serde_json::json!({"Ok": []}),
            note: None,
            source_chain_id: source.map(|_| "polkadot-asset-hub".to_string()),
            source_at_block_hash: source.map(|_| "0xaa".to_string()),
            source_input_hash: source.map(|_| "0x01".to_string()),
            source_forwarded_index: source.map(|(d, _)| d),
            source_message_index: source.map(|(_, m)| m),
            spec_version: 435,
            api_version: 2,
            metadata_version: 15,
            sim_version: 2,
            raw_location: format!("raw/{chain}/sim/dd/{input}/DryRunApi_dry_run_xcm.response.scale"),
        }
    };

    insert_xcm_simulation(&db.pool, &arrival("hydration", "0x04", "not_started", Some((0, 1))))
        .await
        .expect("leg insert");
    // Immutable per (state, input), exactly like a call: a second, different
    // answer is refused silently and the first stands.
    let mut contradiction = arrival("hydration", "0x04", "complete", Some((0, 1)));
    contradiction.note = Some("rewritten".into());
    insert_xcm_simulation(&db.pool, &contradiction)
        .await
        .expect("no-op, not an error");
    let back = xcm_simulation_at(&db.pool, "hydration", "0xdd", "0x04", "dry_run")
        .await
        .unwrap()
        .expect("row");
    assert_eq!(back.status, "not_started", "an observation is never rewritten");
    assert!(back.note.is_none());
    assert_eq!(back.origin_ref, "para:1000");
    assert_eq!(
        back.emitted_events[0]["data"]["amount"], "36893488147419103232",
        "a u128 amount survives JSONB as a decimal string on this table too"
    );
    assert_eq!(back.source_message_index, Some(1));
    assert_eq!(back.source_forwarded_index, Some(0));
    // THE SAME-TYPED COLUMNS, read back explicitly. The insert binds
    // POSITIONALLY, so two adjacent jsonb columns or two adjacent integers can
    // be transposed by an edit that looks like a formatting change and by
    // nothing else — and only a round trip that names them catches it.
    assert_eq!(back.program, msg(9), "program is not origin_location or effects");
    assert!(
        back.weight_used.is_none(),
        "nothing ran, so no weight — and weight_used is not xcm_error"
    );
    assert_eq!(back.xcm_error.expect("a rejection names its reason")["error"],
        serde_json::json!({"Barrier": []}));
    assert_eq!(back.origin_location["V4"][0]["parents"], 1);
    assert_eq!(
        back.baseline_input_hash.as_deref(),
        Some("0xempty"),
        "the arrival's own baseline link round-trips too — it is a key on this table \
         exactly as it is on the sending one"
    );
    // Lineage is the RECEIVER's. Asserted AGAINST the sending row rather than
    // against a constant, because "435 is not 2003002" is only a claim about
    // these two rows if both numbers are in the comparison.
    assert_eq!(subject.spec_version, 2_003_002);
    assert_eq!(
        back.spec_version, 435,
        "a leg carries the chain it was previewed on, not the chain that queued it"
    );

    // A second leg, and one preview with NO source at all — a program someone
    // pasted by hand, which must never be mistaken for a leg of a journey.
    insert_xcm_simulation(&db.pool, &arrival("hydration", "0x06", "complete", Some((0, 0))))
        .await
        .expect("second leg");
    insert_xcm_simulation(&db.pool, &arrival("hydration", "0x07", "complete", None))
        .await
        .expect("hand-supplied preview");

    let xcm_sims = api::pg::PgXcmSimIndex::new(db.pool.clone());
    let legs = xcm_sims
        .legs("polkadot-asset-hub", "0x01", 10)
        .await
        .expect("legs");
    assert_eq!(legs.len(), 2, "the hand-supplied preview is not a leg of anything");
    assert_eq!(
        (legs[0].source_message_index, legs[1].source_message_index),
        (Some(0), Some(1)),
        "legs come back in the SENDER's list order — they are a sequence it produced, and \
         'newest first' would shuffle a journey's own legs"
    );
    // The other side of the nullable pair: a program that COMPLETED reports a
    // weight and no error, where the rejected one reported neither.
    assert_eq!(legs[0].status, "complete");
    assert_eq!(legs[0].weight_used.as_ref().expect("it ran")["ref_time"], 1_000);
    assert!(legs[0].xcm_error.is_none());

    // By program hash, and never borrowed from another chain's runtime.
    assert_eq!(
        xcm_sims
            .xcm_simulations("hydration", "0xprog0x04", 10)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(xcm_sims
        .xcm_simulations("polkadot-asset-hub", "0xprog0x04", 10)
        .await
        .unwrap()
        .is_empty());

    // The store trait the orchestration drives reads the same rows.
    let store = PgXcmSimStore::new(db.pool.clone());
    assert!(store
        .get("hydration", "0xdd", "0x04", "dry_run")
        .await
        .unwrap()
        .is_some());
    assert!(store
        .get("hydration", "0xdd", "0x04", "fork")
        .await
        .unwrap()
        .is_none(),
        "the tier is part of the key here too — a Tier 1 answer must not serve a Tier 2 ask"
    );

    db.drop_db().await;
}

/// PHASE 3, SLICE 6 — the Hydration money mapper, end to end through Postgres.
///
/// FOUR THINGS THIS PROVES THAT A UNIT TEST CANNOT, each of which is a place a
/// silent zero could have hidden:
///
///   1. An orml delta lands in `balances.balance_changes` on a `tokens:<id>`
///      key, in the SAME table as a native and a pallet-assets one, routed to
///      the right partition — the claim that "asset balances are not a new kind
///      of fact" holding for a THIRD vocabulary.
///   2. An orml ANCHOR keeps its reserved half. Migration 0010's asset writer
///      records `reserved = 0` by design; routing an orml holding through it
///      would drop a real reserved position silently, so the anchor is written
///      through the NATIVE writer and this asserts the split survives the round
///      trip.
///   3. **ONE ASSET, TWO CHAINS, ONE `absolute_key`** — the identity claim the
///      whole slice rests on. Asset Hub's `assets:1984` and Hydration's
///      `tokens:10` are the same USDT, they carry DIFFERENT `location_key`s
///      because a Location is relative to its observer, and a `group by
///      absolute_key` finds them as one thing. The negative half is asserted
///      first: without it, this test would pass on two rows that were never
///      distinguishable.
///   4. The native-alias rule: HDX exists as registry asset 0 AND as the
///      pallet_balances token, and there is exactly ONE row for it.
#[tokio::test]
async fn orml_balances_share_the_tables_and_one_asset_resolves_across_two_chains() {
    let Some(db) = TestDb::create().await else {
        return;
    };
    use adapter_substrate::assets as aa;
    use adapter_substrate::orml;

    let reg = seeds();
    sync_registry(&db.pool, &reg).await.expect("registry sync");

    // Hydration must be registered by the SEEDS, not by this test — if the seed
    // ever stops loading, this assertion is where we find out rather than in a
    // silently empty result set below.
    let hydration = reg.chain("hydration").expect("hydration is a registered chain");
    assert_eq!(hydration.para_id, Some(2034));
    assert!(
        hydration.has_module("balances"),
        "the orml mapper is why this module is on; with it off the worker never \
         starts and every number below would be zero for the wrong reason"
    );

    // ---- 1. the two observers' spellings of ONE asset --------------------
    let ah_path = orml::chain_path("polkadot", Some(1000));
    let hydra_path = orml::chain_path("polkadot", Some(2034));

    // Asset Hub names its own asset 1984 with no parents at all
    let ah_usdt = aa::local_asset_location(50, 1984);
    // Hydration's AssetRegistry stores the same asset one hop away, in the
    // decoder's real shape (newtype-wrapped junctions, nested X3)
    let hydra_usdt = serde_json::json!({
        "parents": 1,
        "interior": {"X3": [[
            {"Parachain": [1000]}, {"PalletInstance": [50]}, {"GeneralIndex": [1984]}
        ]]}
    });

    let ah_location_key = aa::canonical_location(&ah_usdt).expect("canonical");
    let hydra_location_key = aa::canonical_location(&hydra_usdt).expect("canonical");
    assert_ne!(
        ah_location_key, hydra_location_key,
        "THE NEGATIVE HALF: version-stripping cannot reconcile two observers' \
         frames. If these are ever equal, the assertion below proves nothing."
    );

    let ah_absolute = orml::absolutize(&ah_path, &ah_usdt).expect("absolutizes");
    let hydra_absolute = orml::absolutize(&hydra_path, &hydra_usdt).expect("absolutizes");

    dotlens_node::assets_pg::upsert_asset(
        &db.pool,
        "polkadot-asset-hub",
        "assets:1984",
        "trust_backed",
        Some("1984"),
        Some(&ah_usdt),
        Some(&ah_location_key),
        Some(&ah_absolute),
        Some(&ah_absolute.to_string()),
        None,
        Some(&1984u32.to_le_bytes()[..]),
        &aa::AssetMeta {
            name: Some("Tether USD".into()),
            symbol: Some("USDT".into()),
            decimals: Some(6),
        },
        &Default::default(),
        Some(2_003_002),
        Some(500),
        "test",
    )
    .await
    .expect("ah usdt");

    dotlens_node::assets_pg::upsert_asset(
        &db.pool,
        "hydration",
        "tokens:10",
        "orml",
        Some("10"),
        Some(&hydra_usdt),
        Some(&hydra_location_key),
        Some(&hydra_absolute),
        Some(&hydra_absolute.to_string()),
        Some("Token"),
        Some(&10u32.to_le_bytes()[..]),
        &aa::AssetMeta {
            name: Some("Tether USD".into()),
            symbol: Some("USDT".into()),
            decimals: Some(6),
        },
        &Default::default(),
        Some(435),
        Some(13_653_999),
        "test",
    )
    .await
    .expect("hydration usdt");

    // THE JOIN. Two chains, two representations, one asset — found by grouping
    // on a column, which is why 0019 adds no table.
    // fetch_ALL, not fetch_one: `fetch_one` returns the first row of a
    // multi-row result rather than erroring, so a second cross-chain group
    // would be silently ignored by an assertion claiming there is exactly one.
    let groups: Vec<(i64, String)> = sqlx::query_as(
        "select count(distinct chain_id), absolute_key from core.assets \
         where absolute_key is not null group by absolute_key \
         having count(distinct chain_id) > 1",
    )
    .fetch_all(&db.pool)
    .await
    .expect("cross-chain groups");
    assert_eq!(groups.len(), 1, "exactly one asset spans two chains here");
    let (chains, key) = groups.into_iter().next().unwrap();
    assert_eq!(chains, 2);
    assert_eq!(
        key,
        r#"[{"GlobalConsensus":{"Polkadot":[]}},{"Parachain":1000},{"PalletInstance":50},{"GeneralIndex":1984}]"#
    );

    // an Erc20 asset is registered, named and located — and VISIBLY
    // unanchorable, which is this slice's scope boundary living in the data
    dotlens_node::assets_pg::upsert_asset(
        &db.pool,
        "hydration",
        "tokens:1001",
        "orml",
        Some("1001"),
        None,
        None,
        None,
        None,
        Some("Erc20"),
        Some(&1001u32.to_le_bytes()[..]),
        &aa::AssetMeta { name: Some("aDOT".into()), symbol: Some("aDOT".into()), decimals: Some(10) },
        &Default::default(),
        Some(435),
        Some(13_653_999),
        "test",
    )
    .await
    .expect("adot");
    let (erc20,): (i64,) =
        sqlx::query_as("select count(*) from core.assets where asset_type = 'Erc20'")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(erc20, 1, "the money market is registered, not hidden");

    // ---- 2. HDX: one row, not two ---------------------------------------
    // The registry calls it asset 0 and pallet_balances calls it the native
    // token. `sync-assets` folds the registry's richer metadata ONTO the native
    // row; the mapper refuses a `tokens:0` event. Both halves of that rule mean
    // the same thing here: exactly one row for HDX.
    let holder_bytes_for_guard = adapter_substrate::accounts::sibling_sovereign(1000);
    let native_hdx = serde_json::json!({"parents": 0, "interior": []});
    let hdx_absolute = orml::absolutize(&hydra_path, &native_hdx).expect("a chain is its own name");
    dotlens_node::assets_pg::upsert_asset(
        &db.pool,
        "hydration",
        "native",
        "native",
        None,
        Some(&native_hdx),
        Some(&native_hdx.to_string()),
        Some(&hdx_absolute),
        Some(&hdx_absolute.to_string()),
        Some("Token"),
        None,
        &aa::AssetMeta { name: Some("HDX".into()), symbol: Some("HDX".into()), decimals: Some(12) },
        &Default::default(),
        Some(435),
        Some(13_653_999),
        "test",
    )
    .await
    .expect("hdx");
    let (hdx_rows,): (i64,) = sqlx::query_as(
        "select count(*) from core.assets where chain_id = 'hydration' \
         and (asset_key = 'native' or asset_key = 'tokens:0')",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(hdx_rows, 1, "HDX is one asset, however many pallets name it");
    // AND THE ASSERTION CAN FAIL, which it could not while nothing in this test
    // ever wrote `tokens:0`: the mapper is what forbids that key, so assert the
    // mapper — a `tokens.Deposited` on currency 0 must HALT rather than produce
    // a delta that a sink would then happily insert.
    let native_event = canonical::CanonicalEvent {
        index: 0,
        transaction_index: None,
        name: "tokens.Deposited".into(),
        data: serde_json::json!({
            "currency_id": 0, "who": [[holder_bytes_for_guard.to_vec()]], "amount": 1
        }),
    };
    assert!(
        adapter_substrate::orml::deltas_for_orml_event(&native_event).is_err(),
        "the second HDX row is prevented by a refusal, not by nobody trying"
    );
    assert_eq!(
        hdx_absolute.to_string(),
        r#"[{"GlobalConsensus":{"Polkadot":[]}},{"Parachain":2034}]"#,
        "a native token has no AssetLocations entry, and absolutizes to its \
         own chain — which is how it gets a name at all"
    );

    // ---- 3. deltas: three vocabularies, one table ------------------------
    let holder = adapter_substrate::accounts::sibling_sovereign(1000);
    let peer = adapter_substrate::accounts::para_sovereign(2034);
    let sink = dotlens_node::balances_pg::PgDeltaSink::new(db.pool.clone());
    let d = |account: &[u8; 32], asset: &str, magnitude: u128, negative: bool| {
        ingest::balances::BalanceDelta {
            account: account.to_vec(),
            magnitude,
            negative,
            reason: if negative { "transfer_out".into() } else { "transfer_in".into() },
            counterparty: None,
            asset: asset.to_string(),
        }
    };
    // the >u64 magnitude an 18-decimal orml asset really produces (4.5% of live
    // events), so the NUMERIC path is exercised rather than assumed
    let big: u128 = 36_893_488_147_419_103_232;
    ingest::balances::DeltaSink::write(
        &sink,
        "hydration",
        13_653_000,
        435,
        adapter_substrate::balances::MAPPER_VERSION,
        &[
            (0, d(&holder, "tokens:10", 20_895_000_000, true)),
            (1, d(&peer, "tokens:10", 20_895_000_000, false)),
            (2, d(&holder, "tokens:222", big, false)),
            (3, d(&holder, "native", 1_000, false)),
        ],
    )
    .await
    .expect("orml deltas land");

    let rows: Vec<(String, String, bool)> = sqlx::query_as(
        "select asset, delta::text, delta < 0 from balances.balance_changes \
         where chain_id = 'hydration' order by event_index",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0].0, "tokens:10");
    assert!(rows[0].2, "the out leg is negative");
    assert_eq!(rows[2].1, big.to_string(), "an 18-decimal magnitude survives");
    assert_eq!(rows[3].0, "native", "HDX still goes through pallet_balances");

    // the mapper version on every row is the bumped one, so a future rebuild
    // can tell orml-covered ranges from pre-orml ones
    // the VALUE, not merely that there is one of them — `count(distinct …)` on
    // rows written by one constant cannot fail, and this slice's whole claim
    // about lineage is that the number MOVED
    let (version,): (i32,) = sqlx::query_as(
        "select distinct mapper_version from balances.balance_changes \
         where chain_id = 'hydration'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("exactly one mapper_version on these rows");
    assert_eq!(
        version, adapter_substrate::balances::MAPPER_VERSION as i32,
        "orml rows must carry the bumped version, or a rebuild cannot tell \
         orml-covered ranges from pre-orml ones"
    );

    // partition routing exact, and the default partition EMPTY
    let (routed,): (i64,) =
        sqlx::query_as("select count(*) from balances.balance_changes_p_hydration")
            .fetch_one(&db.pool)
            .await
            .expect("hydration has its own partition");
    assert_eq!(routed, 4);
    let (defaulted,): (i64,) =
        sqlx::query_as("select count(*) from balances.balance_changes_default")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(defaulted, 0);

    // replay is a no-op (insert-ignore), same as every other module
    ingest::balances::DeltaSink::write(
        &sink,
        "hydration",
        13_653_000,
        435,
        adapter_substrate::balances::MAPPER_VERSION,
        &[(0, d(&holder, "tokens:10", 20_895_000_000, true))],
    )
    .await
    .expect("replay");
    let (after,): (i64,) =
        sqlx::query_as("select count(*) from balances.balance_changes where chain_id = 'hydration'")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(after, 4, "replay must not duplicate");

    // ---- 4. the anchor keeps its reserved half ---------------------------
    // THE ROUTING DECISION 0019 ARGUES FOR, asserted: an orml holding has a
    // free/reserved split, so it takes the NATIVE writer. Through
    // `insert_asset_anchor` the 700 below would be silently zero.
    let holding = orml::OrmlHolding { free: 5_000, reserved: 700, frozen: Some(100) };
    dotlens_node::balances_pg::insert_anchor(
        &db.pool,
        "hydration",
        &holder[..],
        "tokens:10",
        13_652_999,
        &holding.as_account_balances(),
        Some(435),
        "test",
        None,
    )
    .await
    .expect("orml anchor");
    let (free, reserved, total, status): (String, String, String, Option<String>) =
        sqlx::query_as(
            "select free::text, reserved::text, total::text, status \
             from balances.balance_anchors where chain_id = 'hydration' \
             and asset = 'tokens:10'",
        )
        .fetch_one(&db.pool)
        .await
        .expect("the orml anchor row");
    assert_eq!(free, "5000");
    assert_eq!(
        reserved, "700",
        "an orml position's reserved half must survive — the pallet-assets \
         anchor writer would have made this 0"
    );
    assert_eq!(total, "5700");
    assert_eq!(
        status, None,
        "orml has no per-account asset status; a plausible default would be a lie"
    );

    db.drop_db().await;
}

/// Core occupancy end to end (Phase 3, slice 11): candidate events become
/// occupancy rows, a relay parent HASH becomes a HEIGHT, the measured
/// one-candidate-per-core invariant is enforced by the database, and the
/// endpoint serves TWO ratios that are not the same number.
///
/// THE DECISIVE HALF IS THE RELAY-PARENT RESOLUTION, because it is the one thing
/// a pure mapper structurally cannot do and therefore the one thing only this
/// test covers. Async backing puts the parent 2-6 blocks back (measured: min 2,
/// avg 3.261, max 6, never 0 or 1), so inside a contiguously indexed window it
/// resolves and at every window EDGE it cannot — and a NULL there must read
/// "outside our data", never "lag zero". Both cases are asserted here, in the
/// same block, so one cannot pass by the other's luck.
#[tokio::test]
async fn occupancy_resolves_its_relay_parents_and_serves_two_ratios_that_differ() {
    use adapter_substrate::coretime::SubstrateOccupancyMapper;
    use api::CoretimeIndex as _;
    use canonical::{CanonicalBlock, CanonicalEvent, Lineage};

    let Some(db) = TestDb::create().await else { return };
    let reg = seeds();
    sync_registry(&db.pool, &reg).await.expect("registry sync");

    // A block hash as 32 bytes, so a relay_parent in the decoded shape
    // (`[[32 bytes]]` — H256 is a newtype over [u8;32], the layer this project
    // has met seven times) hexes to exactly the hash `core.blocks` carries.
    let hash_bytes = |h: u64| {
        let mut b = [0u8; 32];
        b[24..].copy_from_slice(&h.to_be_bytes());
        b
    };
    let parent_json = |h: u64| serde_json::json!([hash_bytes(h).to_vec()]);

    let candidate = |index: u32, variant: &str, core: u32, para: u32, parent: u64| {
        let mut fields = vec![
            serde_json::json!({
                "descriptor": {
                    "para_id": [para],
                    "relay_parent": parent_json(parent),
                    "pov_hash": [vec![(core + 1) as u8; 32]],
                },
                "commitments_hash": [vec![0u8; 32]]
            }),
            // head_data — present in the event and deliberately never stored
            serde_json::json!([1u8, 2, 3, 4]),
            serde_json::json!([core]),
        ];
        if variant != "CandidateTimedOut" {
            fields.push(serde_json::json!([13]));
        }
        CanonicalEvent {
            index,
            transaction_index: None,
            name: format!("parainclusion.{variant}"),
            data: serde_json::Value::Array(fields),
        }
    };

    let block = |height: u64, events: Vec<CanonicalEvent>| CanonicalBlock {
        chain_id: "polkadot".into(),
        height,
        hash: format!("0x{height:064x}"),
        parent_hash: format!("0x{:064x}", height - 1),
        timestamp: Some("2026-08-19T00:00:00Z".parse().unwrap()),
        finalized: true,
        lineage: Lineage {
            runtime_version: 2_003_002,
            decoder_version: 2,
            raw_location: format!("raw/polkadot/test/{height}"),
        },
        transactions: vec![],
        events,
    };

    let blocks = api::pg::PgBlockIndex::new(db.pool.clone());
    for h in [500u64, 501, 502] {
        api::BlockIndex::insert(&blocks, block(h, vec![]))
            .await
            .expect("insert empty relay block");
    }
    api::BlockIndex::insert(
        &blocks,
        block(
            503,
            vec![
                // parent #500 IS indexed — lag 3, the modal value in the sample
                candidate(0, "CandidateIncluded", 0, 2004, 500),
                // parent #497 is NOT indexed: a window edge, and the reason
                // relay_parent_height is nullable for a reason that is not
                // "we did not look"
                candidate(1, "CandidateIncluded", 5, 2034, 497),
                // A BACKING ON THE SAME CORE IN THE SAME BLOCK. This is the
                // shape the partial unique index is scoped for: at high
                // occupancy a core finishes one candidate and starts the next in
                // one block, so uniqueness applies to inclusions ONLY.
                candidate(2, "CandidateBacked", 0, 2004, 500),
                // the one named-field variant in this pallet — a deliberate ∅,
                // and the reason shape is decided per VARIANT, never per pallet
                CanonicalEvent {
                    index: 3,
                    transaction_index: None,
                    name: "parainclusion.UpwardMessagesReceived".into(),
                    data: serde_json::json!({ "from": [1005], "count": 1 }),
                },
            ],
        ),
    )
    .await
    .expect("insert block 503");
    api::BlockIndex::insert(
        &blocks,
        block(504, vec![candidate(0, "CandidateIncluded", 0, 2004, 501)]),
    )
    .await
    .expect("insert block 504");

    let source = dotlens_node::balances_pg::PgEventSource::new(db.pool.clone());
    let sink = dotlens_node::coretime_pg::PgOccupancySink::new(db.pool.clone());
    let checkpoints = ingest::pg::PgCheckpointStore::new(db.pool.clone());
    let deps = ingest::coretime::CoretimeDeps {
        checkpoints: &checkpoints,
        source: &source,
        sink: &sink,
    };
    ingest::coretime::coretime_range("polkadot", &SubstrateOccupancyMapper, &deps, 500, 504)
        .await
        .expect("map occupancy");

    // 3 inclusions + 1 backing; UpwardMessagesReceived produced nothing.
    let (rows,): (i64,) = sqlx::query_as("select count(*) from coretime.core_occupancy")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(rows, 4);
    for (part, want) in [
        ("coretime.core_occupancy_p_polkadot", 4),
        ("coretime.core_occupancy_default", 0),
    ] {
        let (n,): (i64,) = sqlx::query_as(&format!("select count(*) from {part}"))
            .fetch_one(&db.pool)
            .await
            .unwrap_or_else(|e| panic!("counting {part}: {e}"));
        assert_eq!(n, want, "{part} — a chain must land in its own partition");
    }

    // THE DECISIVE CHECK, both directions, in one block.
    let parents: Vec<(i32, Option<i64>, Option<String>)> = sqlx::query_as(
        "select core_index, relay_parent_height, relay_parent_hash \
         from coretime.core_occupancy \
         where block_height = 503 and kind = 'included' order by core_index",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    assert_eq!(parents.len(), 2);
    assert_eq!(
        (parents[0].0, parents[0].1),
        (0, Some(500)),
        "a relay parent inside the indexed window resolves to its height — lag 3"
    );
    assert_eq!(
        parents[1].1, None,
        "#497 is outside the window: NULL means 'the parent is not in our data', and a lag of \
         zero would be a different and impossible claim"
    );
    assert_eq!(
        parents[1].2,
        Some(format!("0x{:064x}", 497)),
        "the HASH survives even when the height cannot be resolved — a row that threw it away \
         could never be improved by a wider backfill"
    );

    // group_index is present on the four-field variants and would be NULL on
    // CandidateTimedOut, which carries three. No live instance of that exists
    // anywhere on Polkadot, which is why the mapper checks its arity separately.
    let (group,): (Option<i32>,) = sqlx::query_as(
        "select group_index from coretime.core_occupancy \
         where block_height = 503 and event_index = 0",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(group, Some(13));

    // The checkpoint proves MODULE_CORETIME is this module's own key. Share
    // another module's and two workers corrupt one checkpoint while every other
    // assertion here still passes; `ingest::coretime` ships no worker tests, so
    // this is the only thing that pins it.
    let cp = ingest::CheckpointStore::get(&checkpoints, "polkadot", ingest::coretime::MODULE_CORETIME)
        .await
        .expect("checkpoint read")
        .expect("the coretime worker advanced its own checkpoint");
    assert_eq!(cp.last_height, 504);
    assert_eq!(cp.module, ingest::coretime::MODULE_CORETIME);

    // Append-only: re-running a range is a no-op. This also proves the insert's
    // PK arbiter reaches the DO NOTHING path before the partial unique index
    // objects to the identical row — which is exactly what the next assertion
    // shows it does NOT do for a genuinely different row.
    ingest::coretime::coretime_range("polkadot", &SubstrateOccupancyMapper, &deps, 500, 504)
        .await
        .expect("replay");
    let (again,): (i64,) = sqlx::query_as("select count(*) from coretime.core_occupancy")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(again, 4, "re-running a range must not duplicate facts");

    // AND THE UNRESOLVED PARENT IS FILLABLE, which is what makes the NULL an
    // honest "outside our data" rather than "outside our data WHEN WE FIRST
    // LOOKED". Index #497 — the wider backfill — and re-run: the edge resolves
    // and the height that was already known does not move. A `do nothing` sink
    // would leave the first NULL forever and the column's meaning would drift
    // with the window without anything saying so.
    api::BlockIndex::insert(&blocks, block(497, vec![]))
        .await
        .expect("the backfill widens");
    ingest::coretime::coretime_range("polkadot", &SubstrateOccupancyMapper, &deps, 500, 504)
        .await
        .expect("re-run after the wider backfill");
    let filled: Vec<(i32, Option<i64>)> = sqlx::query_as(
        "select core_index, relay_parent_height from coretime.core_occupancy \
         where block_height = 503 and kind = 'included' order by core_index",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    assert_eq!(
        filled,
        vec![(0, Some(500)), (5, Some(497))],
        "the edge resolved on the second pass and the resolved height did not move"
    );
    let (still,): (i64,) = sqlx::query_as("select count(*) from coretime.core_occupancy")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(still, 4, "a monotone fill updates in place; it never adds a row");

    // THE MEASURED INVARIANT, ENFORCED AND NAMED. Zero collisions across 51,998
    // included candidates, so a second inclusion on one core in one block does
    // not mean "a duplicate to skip" — it means our reading of the runtime's
    // core assignment is wrong, and a ratio computed from it would double-count
    // a core. It must stop ingestion rather than be absorbed.
    let clash = ingest::coretime::OccupancyRow {
        kind: "included".into(),
        core_index: 0,
        para_id: 2004,
        group_index: Some(13),
        relay_parent_hash: None,
        pov_hash: None,
    };
    let err = ingest::coretime::OccupancySink::write(
        &sink,
        "polkadot",
        503,
        2_003_002,
        1,
        &[(9, clash)],
    )
    .await
    .expect_err("a second inclusion on core 0 at #503 must be refused");
    assert!(err.contains("TWO INCLUDED CANDIDATES ON CORE 0"), "{err}");
    // ...while a BACKING at the same coordinates is accepted, because the
    // uniqueness is scoped to inclusions and a core legitimately carries both in
    // one block.
    let backing = ingest::coretime::OccupancyRow {
        kind: "backed".into(),
        core_index: 0,
        para_id: 2004,
        group_index: Some(13),
        relay_parent_hash: None,
        pov_hash: None,
    };
    ingest::coretime::OccupancySink::write(&sink, "polkadot", 503, 2_003_002, 1, &[(10, backing)])
        .await
        .expect("a backing on a core that also had an inclusion is not a collision");

    // ---- the denominator, and the two ratios it divides -------------------
    //
    // TWO READINGS THAT DISAGREE, on purpose: `num_cores` is host configuration
    // that moves at session boundaries, and a window containing two readings is
    // a window where one ratio is an average of two different questions.
    dotlens_node::coretime_pg::insert_core_config(
        &db.pool,
        "polkadot",
        502,
        2,
        &serde_json::json!({ "num_cores": 2, "lookahead": 5 }),
        2_003_002,
    )
    .await
    .expect("earlier reading");
    dotlens_node::coretime_pg::insert_core_config(
        &db.pool,
        "polkadot",
        504,
        4,
        &serde_json::json!({ "num_cores": 4, "lookahead": 5 }),
        2_003_002,
    )
    .await
    .expect("the reading the window ran under");

    let index = api::pg::PgCoretimeIndex::new(db.pool.clone());
    let cores = index.occupancy_by_core("polkadot", 500, 504).await.unwrap();
    assert_eq!(cores.len(), 2, "cores 0 and 5 produced; the backing on core 0 is not a core used");
    assert_eq!((cores[0].core_index, cores[0].included_blocks), (0, 2));
    assert_eq!(cores[0].paras, vec![2004]);
    assert_eq!((cores[1].core_index, cores[1].included_blocks), (5, 1));

    let kinds = index.kind_counts("polkadot", 500, 504).await.unwrap();
    assert_eq!(
        kinds,
        vec![("backed".to_string(), 2), ("included".to_string(), 3)],
        "backed rows are IN the table and OUT of the ratios"
    );

    let cov = index.window_coverage("polkadot", 500, 504).await.unwrap();
    assert_eq!(
        cov.blocks_indexed, 5,
        "the slot-fill denominator is what we INDEXED, not the width somebody asked for"
    );
    assert_eq!(
        cov.heights_with_occupancy, 2,
        "three of the five blocks are empty — the gap is how a reader sees an unmapped range \
         without being told"
    );

    let chosen = index
        .core_config_at_or_before("polkadot", 504)
        .await
        .unwrap()
        .expect("a reading at or before the window's end");
    assert_eq!((chosen.block_height, chosen.num_cores), (504, 4));
    assert_eq!(
        index.num_cores_in_window("polkadot", 500, 504).await.unwrap(),
        vec![2, 4],
        "two readings that disagree: the denominator MOVED inside the window and a single ratio \
         across it averages two different questions"
    );

    // LINEAGE, which Invariant 3 requires of the aggregate as much as of the
    // rows: one entry, so every count above was produced by ONE rule set. Two
    // entries would mean the window was mapped under two and the ratios are an
    // average of two different definitions of occupancy.
    let lineage = index.occupancy_lineage("polkadot", 500, 504).await.unwrap();
    assert_eq!(lineage, vec![(2_003_002u64, 1u32, 5u64)], "one runtime, one mapper, five rows");

    // AND THE STALE DETECTOR SPANS EVERY KIND. `max_core_index` must see core 5
    // whether it arrived as an inclusion or a backing — a detector that read
    // only the rows the ratios count would claim to look at "the data" while
    // skipping rows the same response reports under `by_kind`.
    assert_eq!(
        index.max_core_index("polkadot", 500, 504).await.unwrap(),
        Some(5)
    );

    // The two ratios, computed the way the endpoint computes them — and they
    // are not the same number, which is the entire product claim:
    //   cores touched  = 2 of 4 declared = 50.0%
    //   slots filled   = 3 of (5 blocks x 4 cores) = 15.0%
    let touched = cores.len() as f64 / chosen.num_cores as f64;
    let filled = 3.0 / (cov.blocks_indexed * chosen.num_cores as u64) as f64;
    assert!((touched - 0.50).abs() < 1e-9, "{touched}");
    assert!((filled - 0.15).abs() < 1e-9, "{filled}");
    assert!(touched > filled, "even the cores that are used sit idle");

    // AND THE STALE-DENOMINATOR DETECTOR FIRES ON REAL ROWS. Core 5 carried work
    // while the reading declares 4 cores exist — a contradiction that can only
    // mean the reading predates a core count that grew, which is the live risk
    // 0024 names and the reason `num_cores` is a dated table rather than a
    // constant.
    let max_core = cores.iter().map(|c| c.core_index).max().unwrap();
    assert!(
        max_core >= chosen.num_cores,
        "core {max_core} against a declared {} — the endpoint reports this as \
         `stale_suspected` rather than clamping it away",
        chosen.num_cores
    );

    db.drop_db().await;
}

/// THE DEBT SLICE 13 RECORDED AND SLICE 14 PAYS: the broker sink has never been
/// tested offline.
///
/// Slice 13's own "known gaps" listed it verbatim — "the two-table transaction,
/// the duplicate-event-index refusal, partition routing, replay idempotence and
/// the `assignment_refused` 23514 branch are still uncovered offline (replay and
/// routing are now proven by DRILL, not by test)". This is that test, and it
/// also exercises the READER migration 0026 gives those tables, because the
/// governing-assignment probe is the one query the whole delta rests on and it
/// cannot be checked against a hand-built fixture: `distinct on` with a
/// three-column tie-break either picks one announcement per core or it does not.
#[tokio::test]
async fn broker_facts_land_in_two_tables_and_the_governing_assignment_is_one_announcement() {
    use adapter_substrate::broker::SubstrateBrokerMapper;
    use api::BrokerIndex as _;
    use canonical::{CanonicalBlock, CanonicalEvent, Lineage};
    use ingest::broker::{BrokerRow, BrokerSink, CoreAssignmentRow};

    let Some(db) = TestDb::create().await else { return };
    let reg = seeds();
    sync_registry(&db.pool, &reg).await.expect("registry sync");

    const CHAIN: &str = "polkadot-coretime";

    let ev = |index: u32, name: &str, data: serde_json::Value| CanonicalEvent {
        index,
        transaction_index: None,
        name: name.into(),
        data,
    };
    let block = |height: u64, events: Vec<CanonicalEvent>| CanonicalBlock {
        chain_id: CHAIN.into(),
        height,
        hash: format!("0x{height:064x}"),
        parent_hash: format!("0x{:064x}", height - 1),
        timestamp: Some("2026-08-19T00:00:00Z".parse().unwrap()),
        finalized: true,
        lineage: Lineage {
            runtime_version: 2_003_002,
            decoder_version: 2,
            raw_location: format!("raw/{CHAIN}/test/{height}"),
        },
        transactions: vec![],
        events,
    };

    let blocks = api::pg::PgBlockIndex::new(db.pool.clone());

    // A SALE BOUNDARY, in the shape the chain really emits: a bare u16 `core`, a
    // relay block in `when` (an exact multiple of 80), and a one-element vector
    // whose assignment is a NEWTYPE variant one array layer deep.
    api::BlockIndex::insert(
        &blocks,
        block(
            100,
            vec![
                ev(
                    0,
                    "broker.CoreAssigned",
                    serde_json::json!({
                        "core": 0, "when": 80u64,
                        "assignment": [[{"Task": [2004]}, 57600]]
                    }),
                ),
                ev(
                    1,
                    "broker.CoreAssigned",
                    serde_json::json!({"core": 1, "when": 80u64, "assignment": [[{"Pool": []}, 57600]]}),
                ),
                // AN INTERLACED CORE: one event, TWO assignment rows. Never seen
                // on live data, which is exactly why the expansion needs a test
                // — a reader that took the first element would drop the second
                // entitlement permanently.
                ev(
                    2,
                    "broker.CoreAssigned",
                    serde_json::json!({
                        "core": 2, "when": 80u64,
                        "assignment": [[{"Task": [2034]}, 28800], [{"Task": [3344]}, 28800]]
                    }),
                ),
                // A variant that is NOT the seam: it must land in broker_events
                // with its core promoted and produce no assignment row at all.
                // `Renewed` carries BOTH old_core and core, and the row takes
                // `core` — the renewal MOVED the index.
                ev(
                    3,
                    "broker.Renewed",
                    serde_json::json!({
                        "who": [vec![9u8; 32]], "price": "1000",
                        "old_core": 7, "core": 9, "begin": 322663, "duration": 5040,
                        "workload": []
                    }),
                ),
                // Names a TASK and a core — the row the task timeline finds.
                ev(
                    4,
                    "broker.AutoRenewalEnabled",
                    serde_json::json!({"core": 9, "task": 2004}),
                ),
            ],
        ),
    )
    .await
    .expect("insert sale-boundary block");

    // A LATER SALE for core 0 only, so `entitlement_at` has something to choose
    // between rather than something to find.
    api::BlockIndex::insert(
        &blocks,
        block(
            200,
            vec![ev(
                0,
                "broker.CoreAssigned",
                serde_json::json!({
                    "core": 0, "when": 160u64,
                    "assignment": [[{"Task": [3388]}, 57600]]
                }),
            )],
        ),
    )
    .await
    .expect("insert second-sale block");

    let source = dotlens_node::balances_pg::PgEventSource::new(db.pool.clone());
    let sink = dotlens_node::broker_pg::PgBrokerSink::new(db.pool.clone());
    let checkpoints = ingest::pg::PgCheckpointStore::new(db.pool.clone());
    let deps = ingest::broker::BrokerDeps {
        checkpoints: &checkpoints,
        source: &source,
        sink: &sink,
    };
    ingest::broker::broker_range(CHAIN, &SubstrateBrokerMapper, &deps, 100, 200)
        .await
        .expect("map entitlement");

    // ONE EVENT, TWO TABLES. Six events map to six `broker_events` rows; the
    // four `CoreAssigned` among them expand into five assignment rows, because
    // core 2 carries two.
    let (events,): (i64,) = sqlx::query_as("select count(*) from coretime.broker_events")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(events, 6, "one row per Broker event, seam or not");
    let (assignments,): (i64,) = sqlx::query_as("select count(*) from coretime.core_assignments")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(assignments, 5, "four CoreAssigned, one of them interlaced");

    // The non-seam variants produced NO assignment rows, and `Renewed` took the
    // NEW core.
    let (renewed_core,): (Option<i32>,) = sqlx::query_as(
        "select core_index from coretime.broker_events where variant = 'Renewed'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(renewed_core, Some(9), "the NEW core, never old_core");

    // PARTITION ROUTING, exact. A chain must land in its own partition and the
    // default must stay empty — a row in `_default` is a chain the registry sync
    // never created a partition for.
    for (part, want) in [
        ("coretime.broker_events_p_polkadot_coretime", 6),
        ("coretime.broker_events_default", 0),
        ("coretime.core_assignments_p_polkadot_coretime", 5),
        ("coretime.core_assignments_default", 0),
    ] {
        let (n,): (i64,) = sqlx::query_as(&format!("select count(*) from {part}"))
            .fetch_one(&db.pool)
            .await
            .unwrap_or_else(|e| panic!("counting {part}: {e}"));
        assert_eq!(n, want, "{part}");
    }

    // REPLAY IS A GENUINE NO-OP. `relay_block` is stated by the chain inside the
    // event, so a row written today and one written after a wider backfill are
    // byte-identical — which is why this sink does `do nothing` where the
    // occupancy sink one file over does a monotone fill.
    ingest::broker::broker_range(CHAIN, &SubstrateBrokerMapper, &deps, 100, 200)
        .await
        .expect("replay");
    let (again_events, again_assignments): (i64, i64) = sqlx::query_as(
        "select (select count(*) from coretime.broker_events), \
                (select count(*) from coretime.core_assignments)",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(
        (again_events, again_assignments),
        (6, 5),
        "re-running a range must not duplicate facts in either table"
    );
    let (dupes,): (i64,) = sqlx::query_as(
        "select count(*) from (select chain_id, block_height, event_index, assignment_index \
         from coretime.core_assignments group by 1,2,3,4 having count(*) > 1) d",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(dupes, 0);

    // The checkpoint is this module's own key. Sharing another module's would
    // corrupt one checkpoint while every other assertion here still passed, and
    // `ingest::broker` ships no worker tests, so this is the only thing that
    // pins it.
    let cp = ingest::CheckpointStore::get(&checkpoints, CHAIN, ingest::broker::MODULE_BROKER)
        .await
        .expect("checkpoint read")
        .expect("the broker worker advanced its own checkpoint");
    assert_eq!(cp.last_height, 200);
    assert_eq!(cp.module, ingest::broker::MODULE_BROKER);

    // ---------------------------------------------------------------- refusals

    // TWO FACTS AT ONE EVENT INDEX ARE REFUSED, not silently halved.
    // `broker_events` is keyed by it, so insert-ignore would keep the first and
    // drop the second without a word.
    let row = |variant: &str| BrokerRow {
        variant: variant.into(),
        core_index: Some(0),
        task_id: None,
        data: serde_json::json!({}),
        assignments: vec![],
    };
    let err = sink
        .write(CHAIN, 300, 2_003_002, 1, &[(0, row("Renewable")), (0, row("Renewed"))])
        .await
        .expect_err("two facts at one event index must be refused");
    assert!(err.contains("keyed by event index"), "{err}");

    // AND THE CHECK CONSTRAINT'S BOTH DIRECTIONS. 0025's
    // `core_assignments_task_names_its_para` is a BICONDITIONAL, so a `task`
    // with no para and a `pool` carrying one are both refused — and the sink
    // names the constraint rather than passing a bare 23514 through, because
    // what it means is specific: the delta attributes occupancy BY TASK, so a
    // task row with no task is an entitlement nobody can be credited with and a
    // pool row with one credits the wrong chain.
    for (kind, task_id, label) in [
        ("task", None, "a task assignment naming no para"),
        ("pool", Some(2004u32), "a pool assignment pretending to name one"),
    ] {
        let mut r = row("CoreAssigned");
        r.assignments = vec![CoreAssignmentRow {
            assignment_index: 0,
            core_index: 0,
            relay_block: 80,
            kind: kind.into(),
            task_id,
            parts: 57_600,
        }];
        let err = sink
            .write(CHAIN, 301, 2_003_002, 1, &[(0, r)])
            .await
            .unwrap_err();
        assert!(
            err.contains("ASSIGNMENT KIND AND TASK DISAGREE"),
            "{label}: {err}"
        );
    }
    // The refused batch left NOTHING behind — one event, two tables, one
    // transaction, so a failing assignment must roll the event row back too.
    let (orphans,): (i64,) = sqlx::query_as(
        "select count(*) from coretime.broker_events where block_height in (300, 301)",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(
        orphans, 0,
        "a seam row that fails must not leave its announcing event committed — an entitlement \
         with no provenance, or an event whose seam rows silently vanished from the delta"
    );

    // ------------------------------------------------------------- the reader

    let broker = api::pg::PgBrokerIndex::new(db.pool.clone());

    // THE GOVERNING ASSIGNMENT AT A RELAY HEIGHT, which is the one query the
    // whole delta rests on. At relay 80 core 0 is task 2004; at relay 160 the
    // later sale has taken over and it is task 3388 — while cores 1 and 2 are
    // unchanged, because their announcement is still the newest at or before.
    let at_80 = broker.entitlement_at(CHAIN, 80).await.expect("entitlement at 80");
    assert_eq!(at_80.len(), 4, "three cores, one of them interlaced into two");
    assert_eq!(at_80[0].core_index, 0);
    assert_eq!(at_80[0].task_id, Some(2004));
    assert_eq!(at_80[1].kind, "pool");
    assert_eq!(at_80[1].task_id, None);
    assert_eq!(
        (at_80[2].assignment_index, at_80[3].assignment_index),
        (0, 1),
        "an interlaced core keeps BOTH entitlements, in ordinal order"
    );
    assert_eq!(at_80[2].parts, 28_800);

    let at_160 = broker.entitlement_at(CHAIN, 160).await.expect("entitlement at 160");
    let core0: Vec<_> = at_160.iter().filter(|r| r.core_index == 0).collect();
    assert_eq!(core0.len(), 1, "one announcement governs, never two");
    assert_eq!(core0[0].task_id, Some(3388), "the LATER sale governs at 160");
    assert_eq!(core0[0].relay_block, 160);
    assert_eq!(at_160.len(), 4, "the other cores' older announcements still govern");

    // AND BELOW EVERY ANNOUNCEMENT THERE IS NOTHING, which is what the delta
    // renders as `unknown` and never as idle.
    assert!(broker.entitlement_at(CHAIN, 79).await.unwrap().is_empty());

    // THE TIE-BREAK IS NOT DECORATION. Two announcements can share a
    // `relay_block` and come from different events — a re-announcement — and
    // returning both would make one core look INTERLACED, which withholds the
    // delta's waste figure for a reason that never happened.
    let mut re = row("CoreAssigned");
    // The announcing event's own core must match the core it assigns — a real
    // `CoreAssigned` cannot say core 0 and assign core 1, and leaving them
    // inconsistent would quietly add a third row to `events_for_core(0)` and
    // weaken the timeline assertions below.
    re.core_index = Some(1);
    re.assignments = vec![CoreAssignmentRow {
        assignment_index: 0,
        core_index: 1,
        relay_block: 80,
        kind: "task".into(),
        task_id: Some(2222),
        parts: 57_600,
    }];
    sink.write(CHAIN, 150, 2_003_002, 1, &[(0, re)])
        .await
        .expect("a re-announcement at the same relay block is a legal row");
    let after = broker.entitlement_at(CHAIN, 80).await.unwrap();
    let core1: Vec<_> = after.iter().filter(|r| r.core_index == 1).collect();
    assert_eq!(core1.len(), 1, "one ANNOUNCEMENT per core, not one relay block per core");
    assert_eq!(
        core1[0].task_id,
        Some(2222),
        "the newest announcing coordinate wins the tie, so a re-announcement replaces rather \
         than doubling"
    );

    // THE TWO TIMELINE SUBJECTS DIVERGE, which is why the endpoint takes both.
    // Task 2004's auto-renewal sits on core 9 while its assignment sits on core
    // 0, so asking by core follows a SLOT and asking by task follows the TENANT.
    let by_task = broker.events_for_task(CHAIN, 2004, 50).await.unwrap();
    assert_eq!(by_task.len(), 1);
    assert_eq!(by_task[0].variant, "AutoRenewalEnabled");
    assert_eq!(by_task[0].core_index, Some(9));
    let by_core = broker.events_for_core(CHAIN, 0, 50).await.unwrap();
    assert!(
        by_core.iter().all(|e| e.variant == "CoreAssigned"),
        "asking by core 0 must not return the renewal that moved the tenant to core 9"
    );
    // Newest first, and `data` survives intact — the region ids, prices and
    // `old_core` that nothing reads yet all live in there.
    assert_eq!(by_core[0].block_height, 200);
    assert_eq!(by_core[0].data["core"], 0);
    let assigns = broker.assignments_for_task(CHAIN, 2004, 50).await.unwrap();
    assert_eq!(assigns.len(), 1);
    assert_eq!(assigns[0].core_index, 0);

    // ------------------------------------------- the denominator and 0026's columns

    dotlens_node::broker_pg::insert_broker_config(
        &db.pool,
        CHAIN,
        4_927_655,
        100,
        &serde_json::json!({"core_count": 100, "last_timeslice": 400_361}),
        &serde_json::json!({"region_length": 5040, "leadin_length": 100}),
        Some(11),
        Some(&serde_json::json!({"first_core": 11, "cores_sold": 41, "cores_offered": 89})),
        2_003_002,
    )
    .await
    .expect("record broker config");
    // An OLDER reading, to prove `latest_broker_config` picks by height rather
    // than by insertion order — and one with NO SaleInfo at all, which is the
    // honest shape before sales start and must not become `first_core = 0`.
    dotlens_node::broker_pg::insert_broker_config(
        &db.pool,
        CHAIN,
        1_000_000,
        50,
        &serde_json::json!({"core_count": 50}),
        &serde_json::json!({}),
        None,
        None,
        2_000_000,
    )
    .await
    .expect("record an older, sale-less reading");

    let cfg = broker
        .latest_broker_config(CHAIN)
        .await
        .unwrap()
        .expect("a reading is on record");
    assert_eq!(cfg.block_height, 4_927_655, "the NEWEST reading");
    assert_eq!(cfg.core_count, 100);
    assert_eq!(cfg.first_core, Some(11), "migration 0026's column round-trips");
    // `sale_info` is CAPTURED AND READ BY NOTHING, which 0026 states plainly —
    // but a column nobody reads is still a column whose storage must work, or
    // the Dutch price curve it exists to make reconstructible is not there when
    // somebody finally looks.
    let (si,): (Option<serde_json::Value>,) = sqlx::query_as(
        "select sale_info from coretime.broker_config where block_height = 4927655",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(
        si.expect("the whole record round-trips")["cores_sold"],
        41,
        "captured whole; nothing in this slice reads it, and that is the point"
    );
    let (early,): (Option<i32>,) = sqlx::query_as(
        "select first_core from coretime.broker_config where block_height = 1000000",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(
        early, None,
        "sales that never started leave a NULL, not a zero — a zero would move every reserved \
         system core into the bulk market"
    );
    // Immutable per (chain, height): a second reading at the same block is the
    // same observation and must not rewrite it.
    dotlens_node::broker_pg::insert_broker_config(
        &db.pool,
        CHAIN,
        4_927_655,
        99,
        &serde_json::json!({}),
        &serde_json::json!({}),
        Some(0),
        None,
        2_003_002,
    )
    .await
    .expect("re-recording is a no-op");
    let cfg = broker.latest_broker_config(CHAIN).await.unwrap().unwrap();
    assert_eq!((cfg.core_count, cfg.first_core), (100, Some(11)));

    db.drop_db().await;
}
