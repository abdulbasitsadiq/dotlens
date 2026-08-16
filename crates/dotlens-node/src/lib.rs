//! dotlens-node library: the wiring between registry, raw store, decode,
//! checkpoints, and the API's block index. Lives in a lib (not main.rs) so
//! integration tests drive the exact production paths.

pub mod pipeline;

#[cfg(feature = "pg")]
pub mod registry_sync;

#[cfg(feature = "pg")]
pub mod runtime_versions;

#[cfg(feature = "pg")]
pub mod labels;

#[cfg(feature = "pg")]
pub mod balances_pg;

#[cfg(feature = "pg")]
pub mod gov_pg;

#[cfg(feature = "pg")]
pub mod votes_pg;

#[cfg(feature = "pg")]
pub mod treasury_pg;

#[cfg(feature = "pg")]
pub mod tip_pg;

/// Deterministic contiguous chunking for concurrent backfill: same inputs →
/// same chunks → per-chunk checkpoints (`raw_backfill:{a}-{b}`) resume exactly
/// after any crash, whatever the worker count next run uses for OTHER ranges.
pub fn backfill_chunks(from: u64, to: u64, workers: u64) -> Vec<(u64, u64)> {
    assert!(from <= to && workers >= 1);
    let total = to - from + 1;
    let workers = workers.min(total);
    let base = total / workers;
    let extra = total % workers; // first `extra` chunks get one more block
    let mut chunks = Vec::with_capacity(workers as usize);
    let mut start = from;
    for i in 0..workers {
        let len = base + if i < extra { 1 } else { 0 };
        chunks.push((start, start + len - 1));
        start += len;
    }
    chunks
}

#[cfg(test)]
mod tests {
    use super::backfill_chunks;

    #[test]
    fn chunks_are_contiguous_exhaustive_and_deterministic() {
        for (from, to, workers) in [(1u64, 10u64, 3u64), (0, 999_999, 8), (5, 5, 4), (1, 2, 8)] {
            let chunks = backfill_chunks(from, to, workers);
            assert_eq!(chunks.first().unwrap().0, from);
            assert_eq!(chunks.last().unwrap().1, to);
            for w in chunks.windows(2) {
                assert_eq!(w[0].1 + 1, w[1].0, "contiguous, no gap/overlap");
            }
            let total: u64 = chunks.iter().map(|(a, b)| b - a + 1).sum();
            assert_eq!(total, to - from + 1);
            assert_eq!(chunks, backfill_chunks(from, to, workers), "deterministic");
        }
        // 1M drill shape: 8 workers over exactly 1M blocks
        let chunks = backfill_chunks(19_000_000, 19_999_999, 8);
        assert_eq!(chunks.len(), 8);
        assert!(chunks.iter().all(|(a, b)| b - a + 1 == 125_000));
    }
}
