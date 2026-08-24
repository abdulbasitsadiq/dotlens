//! Compaction: turn per-object block artifacts into solid buckets.
//!
//! # Why this is a separate step and not something the writers do
//!
//! The hot path CANNOT bucket. A follower writes one block at a time as it is
//! finalized and has no next-999 to wait for; a backfill worker owns a chunk but
//! still writes each block as it arrives so a SIGKILL resumes from a checkpoint.
//! So compaction reads what is already durably stored and writes a NEW key
//! beside it. Nothing is rewritten in place, which is what keeps the write-once
//! discipline (Invariant 1) intact through a format change.
//!
//! # Consequence, stated with its size rather than as a worry
//!
//! Until the per-object copies are retired the store holds each block twice —
//! but the second copy is the compressed one, so the overhead is not 2x:
//!
//! ```text
//!   Asset Hub   originals 1.00x + bucket ~0.010x  = ~1.01x
//!   relay       originals 1.00x + bucket ~0.410x  = ~1.41x
//! ```
//!
//! i.e. compaction lag is free on a parachain and is the only reason retirement
//! is urgent on the relay — which is also the 98% of the archive. That is the
//! measurement that should drive the retirement schedule.
//!
//! # Retirement is deliberately NOT implemented here
//!
//! Deleting a per-object copy is the only irreversible operation this project
//! would ever perform on the raw store, in a store whose entire doctrine is that
//! bytes are immutable and re-readable. What this slice ships is its
//! PRECONDITION: `verify_bucket` re-hashes every member and compares against the
//! `content_hash` the manifest carries — which is the same hash
//! `core.ingest_receipts` recorded at ingestion, so the check is against the
//! receipt and not merely against the bucket's opinion of itself. The policy:
//!
//!   a per-object copy may be retired only when (1) the bucket containing it
//!   exists, (2) `verify_bucket` passes for that whole bucket, and (3) a human
//!   asks for it. Never automatically, never as a side effect of compaction.

use anyhow::{Context, Result};
use raw_store::{bucket, keys, IngestReceipt, RawStore};

#[derive(Debug, Default, PartialEq)]
pub struct CompactReport {
    pub buckets_written: u64,
    pub buckets_already_present: u64,
    pub members_packed: u64,
    pub heights_packed: u64,
    /// Heights in range with no per-object artifact at all. Reported rather than
    /// silently skipped: a hole here is the same fact `core.ingest_receipts`
    /// gap-accounting reports, and a compaction that quietly closed over one
    /// would make the bucket look complete.
    pub heights_absent: Vec<u64>,
    /// Heights whose per-object copy exists but which an ALREADY-PRESENT bucket
    /// does not hold. Buckets are write-once, so nothing will ever pack these:
    /// a bucket written while its span was still being backfilled under-covers
    /// that span permanently. Reported because the alternative is a silent
    /// `absent=0` on a range that is only half inside a bucket — "we did not
    /// look" rendering as "there is nothing there", which is the one shape this
    /// project keeps catching. Their per-object copies must never be retired.
    pub heights_unpacked: Vec<u64>,
    pub raw_bytes: u64,
    pub stored_bytes: u64,
}

impl CompactReport {
    pub fn ratio(&self) -> f64 {
        if self.stored_bytes == 0 {
            return 0.0;
        }
        self.raw_bytes as f64 / self.stored_bytes as f64
    }
}

/// Does `height` have a per-object block envelope on disk? Uses the same
/// definition as the packing loop: any one of `keys::BLOCK_ITEMS`.
fn has_per_object(raw: &dyn RawStore, chain_id: &str, height: u64) -> Result<bool> {
    for item in keys::BLOCK_ITEMS {
        if raw.exists(&keys::block(chain_id, height, item))? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Pack `from..=to` into aligned buckets of `bucket_blocks`.
///
/// Members are laid down ordered by (height, item) — which is not cosmetic. The
/// entire compression win is consecutive blocks matching each other, so a caller
/// that shuffled would get a correct object and a useless ratio.
pub async fn compact_range(
    raw: &dyn RawStore,
    receipts: &dyn ReceiptRecorder,
    chain_id: &str,
    from: u64,
    to: u64,
    bucket_blocks: u64,
    level: i32,
) -> Result<CompactReport> {
    anyhow::ensure!(from <= to, "from must be <= to");
    anyhow::ensure!(bucket_blocks > 0, "bucket_blocks must be positive");

    let mut report = CompactReport::default();
    let (first, _) = keys::bucket_bounds(from, bucket_blocks);
    let mut bstart = first;

    while bstart <= to {
        let (b_from, b_to) = keys::bucket_bounds(bstart, bucket_blocks);
        let key = keys::bucket(chain_id, b_from, b_to);

        // write-once: an existing bucket is left exactly as it is. Re-running
        // `compact-raw` over a range is therefore a no-op, like every other
        // range command in this project.
        if raw.exists(&key)? {
            report.buckets_already_present += 1;
            // Write-once: the bucket is left exactly as it is. But we still owe
            // the caller the coverage of the range they asked about, because an
            // existing bucket may hold only part of it — reading the manifest
            // costs no decompression (it sits outside the frame for exactly
            // this reason).
            let held: std::collections::BTreeSet<u64> = bucket::read_manifest(&raw.get(&key)?)
                .with_context(|| format!("reading manifest of {key}"))?
                .map(|m| m.heights().into_iter().collect())
                .unwrap_or_default();
            for h in b_from.max(from)..=b_to.min(to) {
                if held.contains(&h) {
                    continue;
                }
                if has_per_object(raw, chain_id, h)? {
                    report.heights_unpacked.push(h);
                } else {
                    report.heights_absent.push(h);
                }
            }
            bstart = b_to + 1;
            continue;
        }

        let mut members: Vec<(u64, String, Vec<u8>)> = Vec::new();
        // clamp to the caller's range: a partial bucket is written rather than
        // refused, because the alternative is refusing to compact a range that
        // does not happen to fall on a boundary. The manifest records exactly
        // which heights are inside, so a partial bucket is self-describing.
        for h in b_from.max(from)..=b_to.min(to) {
            let mut found = false;
            for item in keys::BLOCK_ITEMS {
                let k = keys::block(chain_id, h, item);
                if raw.exists(&k)? {
                    members.push((h, item.to_string(), raw.get(&k)?));
                    found = true;
                    break; // one envelope per height, newest generation wins
                }
            }
            let ek = keys::block(chain_id, h, keys::EVENTS_ITEM);
            if raw.exists(&ek)? {
                members.push((h, keys::EVENTS_ITEM.to_string(), raw.get(&ek)?));
            }
            if found {
                report.heights_packed += 1;
            } else {
                report.heights_absent.push(h);
            }
        }

        if members.is_empty() {
            bstart = b_to + 1;
            continue;
        }
        members.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));
        let raw_bytes: u64 = members.iter().map(|m| m.2.len() as u64).sum();

        let packed =
            bucket::pack(chain_id, &members, level).with_context(|| format!("packing {key}"))?;

        // Read it back through the same reader the decode path uses, BEFORE the
        // object is recorded as a receipt: a bucket that cannot be reopened is a
        // bucket nobody could ever decode from, and finding that out at read
        // time would mean finding it out after retirement.
        let opened = bucket::OpenBucket::open(&packed)
            .with_context(|| format!("reopening freshly packed {key}"))?;
        opened
            .verify()
            .with_context(|| format!("verifying freshly packed {key}"))?;

        let receipt = raw
            .put(&key, &packed, "compact")
            .with_context(|| format!("raw put {key}"))?;
        receipts.record(&receipt).await?;

        report.buckets_written += 1;
        report.members_packed += members.len() as u64;
        report.raw_bytes += raw_bytes;
        report.stored_bytes += packed.len() as u64;
        tracing::info!(
            chain = %chain_id, bucket = %key, members = members.len(),
            raw_bytes, stored = packed.len(),
            ratio = format!("{:.1}x", raw_bytes as f64 / packed.len() as f64),
            "bucket written"
        );
        bstart = b_to + 1;
    }
    Ok(report)
}

#[derive(Debug, Default, PartialEq)]
pub struct VerifyReport {
    pub buckets_checked: u64,
    pub buckets_missing: u64,
    pub members_verified: u64,
    /// Heights whose per-object copy and bucket member disagree byte-for-byte.
    /// This list being empty is the precondition for retiring anything.
    pub mismatched: Vec<(u64, String)>,
    pub retirable_heights: u64,
}

/// Re-hash every member of every bucket covering `from..=to`, and where the
/// per-object copy still exists compare the two byte-for-byte.
///
/// This is the retirement precondition, and it checks the stronger of the two
/// available properties: not just "the bucket is internally consistent" but "the
/// bucket holds exactly what the per-object copy holds".
pub async fn verify_range(
    raw: &dyn RawStore,
    chain_id: &str,
    from: u64,
    to: u64,
    bucket_blocks: u64,
) -> Result<VerifyReport> {
    let mut report = VerifyReport::default();
    let (first, _) = keys::bucket_bounds(from, bucket_blocks);
    let mut bstart = first;

    while bstart <= to {
        let (b_from, b_to) = keys::bucket_bounds(bstart, bucket_blocks);
        let key = keys::bucket(chain_id, b_from, b_to);
        if !raw.exists(&key)? {
            report.buckets_missing += 1;
            bstart = b_to + 1;
            continue;
        }
        let opened =
            bucket::OpenBucket::open(&raw.get(&key)?).with_context(|| format!("opening {key}"))?;
        opened
            .verify()
            .with_context(|| format!("verifying {key}"))?;
        report.buckets_checked += 1;
        report.members_verified += opened.manifest.members.len() as u64;

        let mut ok_heights = std::collections::BTreeSet::new();
        for m in &opened.manifest.members {
            let pk = keys::block(chain_id, m.height, &m.item);
            match raw.get(&pk) {
                Ok(bytes) => {
                    let inside = opened.get(m.height, &m.item).unwrap_or(&[]);
                    if bytes == inside {
                        ok_heights.insert(m.height);
                    } else {
                        report.mismatched.push((m.height, m.item.clone()));
                        ok_heights.remove(&m.height);
                    }
                }
                // already retired, or never existed per-object: the bucket is
                // the only copy and it verified against its own hashes above.
                Err(_) => {
                    ok_heights.insert(m.height);
                }
            }
        }
        for (h, _) in &report.mismatched {
            ok_heights.remove(h);
        }
        report.retirable_heights += ok_heights.len() as u64;
        bstart = b_to + 1;
    }
    Ok(report)
}

/// Receipt sink, narrowed to what compaction needs. `ingest::live::ReceiptSink`
/// is the production impl; this keeps `compact` testable without a database.
#[async_trait::async_trait]
pub trait ReceiptRecorder: Send + Sync {
    async fn record(&self, receipt: &IngestReceipt) -> Result<()>;
}

pub struct NoopReceipts;

#[async_trait::async_trait]
impl ReceiptRecorder for NoopReceipts {
    async fn record(&self, _: &IngestReceipt) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use raw_store::FsRawStore;
    use std::path::PathBuf;

    fn tmp(tag: &str) -> (FsRawStore, PathBuf) {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("dotlens-compact-{}-{tag}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        (FsRawStore::new(&dir), dir)
    }

    fn seed(raw: &FsRawStore, chain: &str, heights: impl Iterator<Item = u64>, item: &str) {
        for h in heights {
            raw.put(
                &keys::block(chain, h, item),
                format!("{{\"height\":{h},\"spec_version\":1}}").as_bytes(),
                "t",
            )
            .unwrap();
            raw.put(&keys::block(chain, h, keys::EVENTS_ITEM), &[h as u8], "t")
                .unwrap();
        }
    }

    #[tokio::test]
    async fn compaction_packs_aligned_buckets_reports_holes_and_is_a_no_op_on_replay() {
        let (raw, dir) = tmp("pack");
        // 0..=24 with 10 and 11 deliberately MISSING
        seed(
            &raw,
            "mock",
            (0..25).filter(|h| *h != 10 && *h != 11),
            keys::BLOCK_ITEM_V2,
        );

        let r = compact_range(&raw, &NoopReceipts, "mock", 0, 24, 10, 3)
            .await
            .unwrap();
        assert_eq!(r.buckets_written, 3, "0-9, 10-19, 20-24");
        assert_eq!(r.heights_packed, 23);
        assert_eq!(
            r.heights_absent,
            vec![10, 11],
            "holes are reported, never closed over"
        );
        assert_eq!(r.members_packed, 46, "one envelope + one events per height");

        // every height is readable through the bucket layer
        let bucketed = raw_store::BucketedStore::new(raw, 10);
        for h in (0..25).filter(|h| *h != 10 && *h != 11) {
            let got = bucketed
                .get(&keys::block("mock", h, keys::BLOCK_ITEM_V2))
                .unwrap();
            assert_eq!(
                got,
                format!("{{\"height\":{h},\"spec_version\":1}}").as_bytes()
            );
        }

        // replay writes nothing new — write-once, same as every range command
        let again = compact_range(bucketed.inner(), &NoopReceipts, "mock", 0, 24, 10, 3)
            .await
            .unwrap();
        assert_eq!(again.buckets_written, 0);
        assert_eq!(again.buckets_already_present, 3);
        // and a replay still reports the coverage of the range it was asked
        // about: an existing bucket must not turn a hole into silence.
        assert_eq!(
            again.heights_absent,
            vec![10, 11],
            "replay still sees the holes"
        );
        assert!(
            again.heights_unpacked.is_empty(),
            "nothing arrived after packing"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    /// The retirement precondition must compare the bucket against the
    /// PER-OBJECT copy, not merely against its own manifest — a bucket that
    /// verified only internally would certify bytes nobody had checked.
    #[tokio::test]
    async fn verify_compares_against_the_per_object_copy_and_survives_retirement() {
        let (raw, dir) = tmp("verify");
        seed(&raw, "mock", 100..110, keys::BLOCK_ITEM_V2);
        compact_range(&raw, &NoopReceipts, "mock", 100, 109, 10, 3)
            .await
            .unwrap();

        let v = verify_range(&raw, "mock", 100, 109, 10).await.unwrap();
        assert_eq!(v.buckets_checked, 1);
        assert_eq!(v.members_verified, 20);
        assert!(v.mismatched.is_empty());
        assert_eq!(v.retirable_heights, 10);

        // simulate retirement of one height: the bucket is now the only copy and
        // verification still passes, because the manifest hash still holds.
        std::fs::remove_file(dir.join(keys::block("mock", 105, keys::BLOCK_ITEM_V2))).unwrap();
        let v2 = verify_range(&raw, "mock", 100, 109, 10).await.unwrap();
        assert!(v2.mismatched.is_empty());
        assert_eq!(v2.retirable_heights, 10);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn a_range_with_no_bucket_is_reported_missing_rather_than_verified() {
        let (raw, dir) = tmp("missing");
        seed(&raw, "mock", 0..5, keys::BLOCK_ITEM_V2);
        let v = verify_range(&raw, "mock", 0, 9, 10).await.unwrap();
        assert_eq!(v.buckets_missing, 1);
        assert_eq!(v.buckets_checked, 0);
        assert_eq!(
            v.retirable_heights, 0,
            "nothing may be retired on the strength of a bucket that does not exist"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Both envelope generations compact into one bucket, and each height's
    /// newest generation is the one packed.
    /// A bucket written while its span was still being backfilled under-covers
    /// that span FOREVER, because buckets are write-once and nothing reopens
    /// them. The blocks that arrive later stay per-object — which is safe, and
    /// is exactly why it must not be reported as `absent=0`.
    #[tokio::test]
    async fn a_bucket_written_mid_backfill_reports_what_it_will_never_pack() {
        let (raw, dir) = tmp("undercover");
        // only the tail of bucket 0-9 has been backfilled so far
        seed(&raw, "mock", 5..10, keys::BLOCK_ITEM_V2);
        let first = compact_range(&raw, &NoopReceipts, "mock", 0, 9, 10, 3)
            .await
            .unwrap();
        assert_eq!(first.buckets_written, 1);
        assert_eq!(first.heights_packed, 5);
        assert_eq!(
            first.heights_absent,
            vec![0, 1, 2, 3, 4],
            "not yet backfilled"
        );

        // the rest of the span arrives afterwards
        seed(&raw, "mock", 0..5, keys::BLOCK_ITEM_V2);
        let after = compact_range(&raw, &NoopReceipts, "mock", 0, 9, 10, 3)
            .await
            .unwrap();

        assert_eq!(
            after.buckets_written, 0,
            "write-once: the bucket is not reopened"
        );
        assert_eq!(after.buckets_already_present, 1);
        assert!(
            after.heights_absent.is_empty(),
            "these heights are no longer absent — they are on disk"
        );
        assert_eq!(
            after.heights_unpacked,
            vec![0, 1, 2, 3, 4],
            "a per-object copy the existing bucket does not hold must be NAMED, not counted as \
             covered: nothing will ever pack it and it must never be retired"
        );

        // and the retirement precondition never over-claims for them
        let v = verify_range(&raw, "mock", 0, 9, 10).await.unwrap();
        assert_eq!(
            v.retirable_heights, 5,
            "only what the bucket actually holds"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn a_bucket_holds_both_envelope_generations() {
        let (raw, dir) = tmp("mixed");
        seed(&raw, "mock", 0..5, keys::BLOCK_ITEM_V1); // pre-format-slice
        seed(&raw, "mock", 5..10, keys::BLOCK_ITEM_V2); // post
        let r = compact_range(&raw, &NoopReceipts, "mock", 0, 9, 10, 3)
            .await
            .unwrap();
        assert_eq!(r.heights_packed, 10);

        let opened =
            bucket::OpenBucket::open(&raw.get(&keys::bucket("mock", 0, 9)).unwrap()).unwrap();
        assert!(opened.contains(0, keys::BLOCK_ITEM_V1));
        assert!(opened.contains(9, keys::BLOCK_ITEM_V2));
        assert!(!opened.contains(0, keys::BLOCK_ITEM_V2));
        let _ = std::fs::remove_dir_all(dir);
    }
}
