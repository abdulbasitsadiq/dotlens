//! Treasury worker: walk canonical events → spend facts and pot flows
//! (ARCHITECTURE.md §9: own checkpoint, own schema, idempotent restarts).
//!
//! Structurally identical to the gov and votes workers — same `EventSource`
//! contract, same frontier discipline, same loud-mapper-halt rule — because
//! WHAT an event means for the treasury lives behind `TreasuryMapper` in
//! adapter crates (Invariant 4).
//!
//! Checkpoint module: `treasury`.

use crate::module::{self, impl_module_error, EventSource, FactWriter, Mapping, ModuleRun};
use crate::{CheckpointError, CheckpointStore};
use async_trait::async_trait;
use canonical::CanonicalEvent;

pub const MODULE_TREASURY: &str = "treasury";

#[derive(Debug, thiserror::Error)]
pub enum TreasuryWorkerError {
    #[error("event source: {0}")]
    Source(String),
    #[error("treasury sink: {0}")]
    Sink(String),
    #[error(
        "treasury mapper failed for {chain}/{height} event {event_index} ({event}): {reason} — \
         the treasury halts loudly rather than record money it cannot explain"
    )]
    Mapper {
        chain: String,
        height: u64,
        event_index: u32,
        event: String,
        reason: String,
    },
    #[error(transparent)]
    Checkpoint(#[from] CheckpointError),
}

impl_module_error!(TreasuryWorkerError);

/// One treasury fact from one event.
///
/// `spend_kind`/`spend_id` are OPTIONAL on purpose: pot events (Deposit, Burnt,
/// Spending, Rollover, UpdatedInactive) concern the treasury pot but name no
/// spend. They are recorded with `attribution = "pot"` and stay out of the
/// projection — honest coverage, never a silent drop and never a guess.
///
/// NOT ALL POT EVENTS ARE FLOWS: `Deposit`/`Burnt` are money moving, while
/// `Spending`/`Rollover` report the pot BALANCE at the start and end of a
/// spend period. `figure_kind` says which, so nobody sums a balance as a
/// flow (reviewer catch — doing so double-counts the pot twice per period).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpendFact {
    /// Pallet instance: "treasury" | "fellowship_treasury" | "ambassador_treasury".
    pub instance: String,
    /// "proposal" (legacy id space) | "asset_spend" (modern id space) | None (pot).
    pub spend_kind: Option<String>,
    pub spend_id: Option<u64>,
    /// proposed|approved|awarded|rejected|paid|payment_failed|processed|voided|
    /// pot_spending|pot_burnt|pot_rollover|pot_deposit|pot_inactive_updated.
    pub kind: String,
    /// The status this event moves the spend to, if status-bearing. None =
    /// the fact is recorded but the projection must not change (pot flows).
    pub status: Option<String>,
    /// The spend value, or the pot event's own figure.
    pub amount: Option<u128>,
    /// What `amount` means: "flow" (money moved) | "snapshot" (a balance
    /// reported at a point in time). None when there is no figure.
    pub figure_kind: Option<String>,
    /// Rejected.slashed — the PROPOSER'S BOND, never spending.
    pub slashed: Option<u128>,
    /// VersionedLocatableAsset for modern spends; None = native token.
    pub asset_kind: Option<serde_json::Value>,
    /// The same asset kind, NORMALIZED and split into the two things it
    /// actually names: `{"chain": <Location>, "asset": <Location>}` — which
    /// chain holds the asset, and which asset on it. Version-stripped, so the
    /// relay's V3 `Concrete` spelling and Asset Hub's V4/V5 spelling of one
    /// asset produce identical strings. This is the join handle to
    /// `core.assets.location_key`, i.e. the difference between reporting
    /// "20895000000" and reporting "20,895 USDT".
    pub asset_location: Option<serde_json::Value>,
    /// The resolved dotlens asset key, but ONLY where it can be resolved with
    /// no metadata (an empty interior = the holding chain's native currency).
    /// Everything else is resolved at read time; None here is "not resolvable
    /// without the chain's pallet indices", never "not an asset".
    pub asset_key: Option<String>,
    /// 32-byte beneficiary when derivable (legacy accounts, or a location whose
    /// junctions name an AccountId32).
    pub beneficiary: Option<Vec<u8>>,
    /// The beneficiary exactly as the event carried it.
    pub beneficiary_location: Option<serde_json::Value>,
    pub payment_id: Option<String>,
    pub valid_from: Option<u64>,
    pub expire_at: Option<u64>,
    /// "event" | "pot".
    pub attribution: String,
    pub data: serde_json::Value,
}

/// Event → treasury facts. Pure; errors are LOUD (an unknown treasury event
/// must halt, not silently drop money). `mapper_version` is lineage.
pub trait TreasuryMapper: Send + Sync {
    fn facts(&self, event: &CanonicalEvent) -> Result<Vec<SpendFact>, String>;
    fn mapper_version(&self) -> u32;
}

/// Where treasury facts land (node-side: treasury.spend_events insert-ignore +
/// the ordering-guarded treasury.spends projection, atomically per block).
///
/// CONTRACT: at most ONE fact per event index — the fact table is keyed
/// (chain, block, event) so a second fact from one event has nowhere to go.
/// Sinks must REFUSE such a batch loudly rather than let insert-ignore swallow
/// it (the rule the votes sink established).
#[async_trait]
pub trait TreasurySink: Send + Sync {
    async fn write(
        &self,
        chain_id: &str,
        height: u64,
        runtime_version: u32,
        mapper_version: u32,
        rows: &[(u32, SpendFact)],
    ) -> Result<(), String>;
}

pub struct TreasuryDeps<'a> {
    pub checkpoints: &'a dyn CheckpointStore,
    pub source: &'a dyn EventSource,
    pub sink: &'a dyn TreasurySink,
}

/// Bridges the treasury sink to the runtime's generic writer. The at-most-one-
/// fact-per-event CONTRACT above stays the SINK's to enforce.
struct SinkBridge<'a>(&'a dyn TreasurySink);

#[async_trait]
impl<'a> FactWriter<SpendFact> for SinkBridge<'a> {
    async fn write_facts(
        &self,
        chain_id: &str,
        height: u64,
        runtime_version: u32,
        mapper_version: u32,
        rows: &[(u32, SpendFact)],
    ) -> Result<(), String> {
        self.0
            .write(chain_id, height, runtime_version, mapper_version, rows)
            .await
    }
}

fn run<'a>(
    mapper: &'a dyn TreasuryMapper,
    deps: &'a TreasuryDeps<'a>,
) -> ModuleRun<'a, SpendFact> {
    ModuleRun {
        module: MODULE_TREASURY,
        checkpoints: deps.checkpoints,
        source: deps.source,
        map: Mapping::PerEvent(Box::new(move |ev: &CanonicalEvent| mapper.facts(ev))),
        mapper_version: mapper.mapper_version(),
        sink: Box::new(SinkBridge(deps.sink)),
    }
}

/// Map heights `from..=to`. Behind the frontier: reprocess freely (insert-ignore
/// + guarded upsert converge), checkpoint untouched. Past it: rows first,
/// checkpoint last (crash = re-map, never skip).
pub async fn treasury_range(
    chain_id: &str,
    mapper: &dyn TreasuryMapper,
    deps: &TreasuryDeps<'_>,
    from: u64,
    to: u64,
) -> Result<u64, TreasuryWorkerError> {
    module::run_range(chain_id, &run(mapper, deps), from, to).await
}

/// One follower step: chase the decode (`blocks`) checkpoint. First run starts
/// at the decode tip — history is `treasury-range`'s job.
pub async fn treasury_tick(
    chain_id: &str,
    mapper: &dyn TreasuryMapper,
    deps: &TreasuryDeps<'_>,
) -> Result<u64, TreasuryWorkerError> {
    module::run_tick(chain_id, &run(mapper, deps)).await
}

/// Follow forever, same backoff discipline as the other followers.
pub async fn treasury_follow(
    chain_id: &str,
    mapper: &dyn TreasuryMapper,
    deps: &TreasuryDeps<'_>,
    poll: std::time::Duration,
) {
    module::run_follow::<SpendFact, TreasuryWorkerError>(chain_id, &run(mapper, deps), poll).await
}

#[cfg(test)]
mod tests {
    use super::*;
    // Both moved out of the file header when `advance` did; the tests below are
    // otherwise untouched, which is the point of this refactor.
    use crate::decode::MODULE_DECODE;
    use crate::module::BlockEvents;
    use crate::Checkpoint;
    use crate::MemoryCheckpointStore;
    use std::collections::HashMap;
    use std::sync::Mutex;

    struct MemSource(HashMap<u64, Vec<CanonicalEvent>>);

    #[async_trait]
    impl EventSource for MemSource {
        async fn decoded_events(
            &self,
            _chain: &str,
            height: u64,
        ) -> Result<Option<BlockEvents>, String> {
            Ok(self.0.get(&height).map(|events| BlockEvents {
                runtime_version: 100,
                events: events.clone(),
            }))
        }
    }

    fn fact(id: Option<u64>, kind: &str, status: Option<&str>) -> SpendFact {
        SpendFact {
            instance: "treasury".into(),
            spend_kind: id.map(|_| "asset_spend".to_string()),
            spend_id: id,
            kind: kind.into(),
            status: status.map(str::to_string),
            amount: Some(100),
            figure_kind: Some("flow".into()),
            slashed: None,
            asset_kind: None,
            asset_location: None,
            asset_key: None,
            beneficiary: None,
            beneficiary_location: None,
            payment_id: None,
            valid_from: None,
            expire_at: None,
            attribution: if id.is_some() { "event".into() } else { "pot".into() },
            data: serde_json::json!({}),
        }
    }

    struct MockMapper;
    impl TreasuryMapper for MockMapper {
        fn facts(&self, event: &CanonicalEvent) -> Result<Vec<SpendFact>, String> {
            match event.name.as_str() {
                "mock.Bad" => Err("unmappable".into()),
                "mock.Spend" => {
                    let id = event.data["index"].as_u64().ok_or("no index")?;
                    Ok(vec![fact(Some(id), "approved", Some("approved"))])
                }
                "mock.Pot" => Ok(vec![fact(None, "pot_deposit", None)]),
                _ => Ok(vec![]),
            }
        }
        fn mapper_version(&self) -> u32 {
            1
        }
    }

    #[derive(Default)]
    struct MemSink(Mutex<Vec<(u64, u32, SpendFact)>>);
    #[async_trait]
    impl TreasurySink for MemSink {
        async fn write(
            &self,
            _chain: &str,
            height: u64,
            _rv: u32,
            _mv: u32,
            rows: &[(u32, SpendFact)],
        ) -> Result<(), String> {
            let mut g = self.0.lock().unwrap();
            for (idx, f) in rows {
                g.push((height, *idx, f.clone()));
            }
            Ok(())
        }
    }

    fn ev(index: u32, name: &str, data: serde_json::Value) -> CanonicalEvent {
        CanonicalEvent {
            index,
            transaction_index: Some(0),
            name: name.into(),
            data,
        }
    }

    #[tokio::test]
    async fn range_maps_spends_and_pot_flows_skips_gaps_and_advances() {
        let mut src = HashMap::new();
        src.insert(1, vec![ev(0, "mock.Spend", serde_json::json!({"index": 42}))]);
        // height 2 is a decode gap
        src.insert(
            3,
            vec![
                ev(0, "mock.Other", serde_json::json!({})),
                ev(1, "mock.Pot", serde_json::json!({})),
            ],
        );
        let checkpoints = MemoryCheckpointStore::new();
        let sink = MemSink::default();
        let deps = TreasuryDeps {
            checkpoints: &checkpoints,
            source: &MemSource(src),
            sink: &sink,
        };
        let n = treasury_range("mock", &MockMapper, &deps, 1, 3).await.unwrap();
        assert_eq!(n, 2, "two decoded heights, one gap skipped");
        let rows = sink.0.lock().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].2.spend_id, Some(42));
        assert_eq!(rows[1].2.attribution, "pot");
        assert_eq!(rows[1].2.spend_id, None);
        drop(rows);
        let cp = checkpoints.get("mock", MODULE_TREASURY).await.unwrap().unwrap();
        assert_eq!(cp.last_height, 3, "checkpoint advanced through the gap");
    }

    #[tokio::test]
    async fn rerun_behind_frontier_rewrites_without_moving_checkpoint() {
        let mut src = HashMap::new();
        src.insert(1, vec![ev(0, "mock.Spend", serde_json::json!({"index": 1}))]);
        src.insert(2, vec![]);
        let checkpoints = MemoryCheckpointStore::new();
        let sink = MemSink::default();
        let deps = TreasuryDeps {
            checkpoints: &checkpoints,
            source: &MemSource(src),
            sink: &sink,
        };
        treasury_range("mock", &MockMapper, &deps, 1, 2).await.unwrap();
        let n = treasury_range("mock", &MockMapper, &deps, 1, 1).await.unwrap();
        assert_eq!(n, 1, "behind-frontier reprocess is allowed (sink converges)");
        let cp = checkpoints.get("mock", MODULE_TREASURY).await.unwrap().unwrap();
        assert_eq!(cp.last_height, 2, "frontier untouched by reprocess");
    }

    #[tokio::test]
    async fn mapper_errors_halt_loudly_and_advance_nothing() {
        let mut src = HashMap::new();
        src.insert(1, vec![ev(0, "mock.Bad", serde_json::json!({}))]);
        let checkpoints = MemoryCheckpointStore::new();
        let sink = MemSink::default();
        let deps = TreasuryDeps {
            checkpoints: &checkpoints,
            source: &MemSource(src),
            sink: &sink,
        };
        let err = treasury_range("mock", &MockMapper, &deps, 1, 1).await.unwrap_err();
        assert!(matches!(err, TreasuryWorkerError::Mapper { height: 1, .. }));
        assert!(sink.0.lock().unwrap().is_empty());
        assert!(checkpoints.get("mock", MODULE_TREASURY).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn tick_chases_the_decode_checkpoint() {
        let mut src = HashMap::new();
        for h in 5..=9 {
            src.insert(h, vec![ev(0, "mock.Spend", serde_json::json!({"index": h}))]);
        }
        let checkpoints = MemoryCheckpointStore::new();
        let sink = MemSink::default();
        let deps = TreasuryDeps {
            checkpoints: &checkpoints,
            source: &MemSource(src),
            sink: &sink,
        };
        assert_eq!(treasury_tick("mock", &MockMapper, &deps).await.unwrap(), 0);

        let decode_cp = |h: u64| Checkpoint {
            chain_id: "mock".into(),
            module: MODULE_DECODE.into(),
            last_height: h,
            last_hash: "0x".into(),
            updated_at: chrono::Utc::now(),
        };
        checkpoints.advance(decode_cp(7)).await.unwrap();
        assert_eq!(treasury_tick("mock", &MockMapper, &deps).await.unwrap(), 1);
        checkpoints.advance(decode_cp(9)).await.unwrap();
        assert_eq!(treasury_tick("mock", &MockMapper, &deps).await.unwrap(), 2);
        let cp = checkpoints.get("mock", MODULE_TREASURY).await.unwrap().unwrap();
        assert_eq!(cp.last_height, 9);
        assert_eq!(treasury_tick("mock", &MockMapper, &deps).await.unwrap(), 0);
    }
}
