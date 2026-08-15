//! The ingestion pipeline: raw-store put (write-once) → receipt → checkpoint
//! guard → pure decode → block index → checkpoint advance.
//! Re-running is a no-op end to end. Fixture-driven in Phase 0/1 bring-up;
//! live fetchers will feed the same path.

use anyhow::{Context, Result};
use api::BlockIndex;
use ingest::{should_process, Checkpoint, CheckpointStore, IngestOutcome, ReceiptSink};
use raw_store::{keys, RawStore};
use registry::Registry;
use std::path::Path;

/// Ingest every fixture envelope in `dir`. Order: durable rows first, the
/// checkpoint LAST — a crash between the two re-processes the block (harmless,
/// idempotent) instead of silently skipping it (data loss).
pub async fn ingest_fixtures(
    dir: &Path,
    registry: &Registry,
    raw: &dyn RawStore,
    checkpoints: &dyn CheckpointStore,
    receipts: &dyn ReceiptSink,
    blocks: &dyn BlockIndex,
) -> Result<usize> {
    // read + peek everything first, then process in (chain, height) order so
    // the high-water-mark checkpoint never silently drops an out-of-order file
    let mut items: Vec<(String, u64, String, Vec<u8>)> = Vec::new();
    for entry in std::fs::read_dir(dir)
        .with_context(|| format!("reading fixtures dir {}", dir.display()))?
    {
        let path = entry?.path();
        if !path.extension().map(|x| x == "json").unwrap_or(false) {
            continue;
        }
        let bytes = std::fs::read(&path)?;
        let peek: serde_json::Value = serde_json::from_slice(&bytes)
            .with_context(|| format!("fixture {} is not valid JSON", path.display()))?;
        let chain_id = peek["chain_id"].as_str().unwrap_or_default().to_string();
        let height = peek["height"].as_u64().unwrap_or_default();
        let hash = peek["hash"].as_str().unwrap_or_default().to_string();
        if registry.chain(&chain_id).is_none() {
            tracing::warn!(%chain_id, fixture = %path.display(), "fixture chain not in registry — skipped");
            continue;
        }
        items.push((chain_id, height, hash, bytes));
    }
    items.sort_by(|a, b| (&a.0, a.1).cmp(&(&b.0, b.1)));

    let mut processed = 0usize;
    for (chain_id, height, hash, bytes) in items {
        let key = keys::block(&chain_id, height, "block.json");
        let receipt = raw
            .put(&key, &bytes, "fixture")
            .with_context(|| format!("raw put {key}"))?;
        receipts
            .record(&receipt)
            .await
            .with_context(|| format!("recording receipt for {key}"))?;
        tracing::debug!(key = %receipt.key, bytes = receipt.byte_len,
            hash = %receipt.content_hash, source = %receipt.source, "raw stored");

        match should_process(checkpoints, &chain_id, "blocks", height).await? {
            IngestOutcome::AlreadyProcessed => {
                tracing::debug!(%chain_id, height, "already processed — skipped");
                continue;
            }
            IngestOutcome::Processed => {}
        }

        let block = adapter_substrate::decode_block(&bytes, &key)
            .with_context(|| format!("decoding {key}"))?;
        blocks
            .insert(block)
            .await
            .with_context(|| format!("indexing {chain_id}/{height}"))?;

        checkpoints
            .advance(Checkpoint {
                chain_id: chain_id.clone(),
                module: "blocks".into(),
                last_height: height,
                last_hash: hash,
                updated_at: chrono::Utc::now(),
            })
            .await?;
        processed += 1;
        tracing::info!(%chain_id, height, "ingested");
    }
    Ok(processed)
}
