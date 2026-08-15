//! Chain registry: the declarative heart of dotlens.
//!
//! Invariant 2 (ARCHITECTURE.md §1): everything volatile — chains, capabilities,
//! lifecycle, domain residency — is DATA loaded from `registry-seeds/`, never code.
//! Adding a chain must never require a code change in any other crate.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("io error reading {path}: {source}")]
    Io {
        path: String,
        source: std::io::Error,
    },
    #[error("yaml error in {path}: {source}")]
    Yaml {
        path: String,
        source: serde_yaml::Error,
    },
    #[error("duplicate chain id: {0}")]
    DuplicateChain(String),
    #[error("residency references unknown chain: {0}")]
    UnknownResidencyChain(String),
    #[error("chain {chain}: relay '{relay}' is not a registered chain")]
    UnknownRelay { chain: String, relay: String },
    #[error("no residency entry for domain '{domain}' on network '{network}' at {at}")]
    NoResidency {
        domain: String,
        network: String,
        at: DateTime<Utc>,
    },
    #[error(
        "residency windows overlap or are inverted for domain '{domain}' on network '{network}'"
    )]
    ResidencyOverlap { domain: String, network: String },
}

/// Execution family. Selects the ChainAdapter. New families (jam, ...) are added
/// here and in an adapter crate — never as branches in module code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChainFamily {
    Substrate,
    Evm,
    Jam,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleStatus {
    Live,
    OnDemand,
    WindingDown,
    Migrated,
    Dead,
}

/// Lifecycle is an event log (chains leave: Moonbeam → Base, 2026-07-31).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleEvent {
    pub status: LifecycleStatus,
    pub from: DateTime<Utc>,
    #[serde(default)]
    pub migration_dest: Option<String>,
    #[serde(default)]
    pub note: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Endpoints {
    #[serde(default)]
    pub rpc: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainConfig {
    pub id: String,
    pub name: String,
    pub family: ChainFamily,
    #[serde(default)]
    pub relay: Option<String>,
    #[serde(default)]
    pub para_id: Option<u32>,
    pub network: String,
    #[serde(default)]
    pub ss58_prefix: Option<u16>,
    #[serde(default)]
    pub lifecycle: Vec<LifecycleEvent>,
    #[serde(default)]
    pub endpoints: Endpoints,
    /// Free-form capability map — schema-on-read by design. Modules query
    /// capabilities; they never test chain ids.
    #[serde(default)]
    pub capabilities: BTreeMap<String, serde_yaml::Value>,
    /// Enabled domain modules for this chain.
    #[serde(default)]
    pub modules: Vec<String>,
}

impl ChainConfig {
    pub fn status_at(&self, at: DateTime<Utc>) -> Option<LifecycleStatus> {
        self.lifecycle
            .iter()
            .filter(|e| e.from <= at)
            .max_by_key(|e| e.from)
            .map(|e| e.status)
    }

    pub fn has_capability(&self, name: &str) -> bool {
        match self.capabilities.get(name) {
            None => false,
            Some(serde_yaml::Value::Bool(b)) => *b,
            Some(_) => true, // structured capability (e.g. assets: {instances: [...]})
        }
    }

    pub fn has_module(&self, name: &str) -> bool {
        self.modules.iter().any(|m| m == name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResidencyEntry {
    pub domain: String,
    pub network: String,
    pub chain: String,
    pub from: DateTime<Utc>,
    #[serde(default)]
    pub to: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Deserialize)]
struct ResidencyFile {
    residency: Vec<ResidencyEntry>,
}

#[derive(Debug, Clone, Default)]
pub struct Registry {
    chains: BTreeMap<String, ChainConfig>,
    residency: Vec<ResidencyEntry>,
}

impl Registry {
    /// Load all `*.yaml` chain seeds plus `domain-residency.yaml` from a directory.
    pub fn load_from_dir(dir: &Path) -> Result<Self, RegistryError> {
        let mut reg = Registry::default();
        let read = |p: &Path| -> Result<String, RegistryError> {
            std::fs::read_to_string(p).map_err(|source| RegistryError::Io {
                path: p.display().to_string(),
                source,
            })
        };

        let mut entries: Vec<_> = std::fs::read_dir(dir)
            .map_err(|source| RegistryError::Io {
                path: dir.display().to_string(),
                source,
            })?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|e| e == "yaml" || e == "yml").unwrap_or(false))
            .collect();
        entries.sort();

        for path in entries {
            let text = read(&path)?;
            // any *residency*.yaml is a residency file (domain-residency.yaml,
            // kusama-residency.yaml, ...); everything else is a chain config
            let is_residency = path
                .file_stem()
                .and_then(|s| s.to_str())
                .map(|s| s.ends_with("residency"))
                .unwrap_or(false);
            if is_residency {
                let file: ResidencyFile =
                    serde_yaml::from_str(&text).map_err(|source| RegistryError::Yaml {
                        path: path.display().to_string(),
                        source,
                    })?;
                reg.residency.extend(file.residency);
            } else {
                let chain: ChainConfig =
                    serde_yaml::from_str(&text).map_err(|source| RegistryError::Yaml {
                        path: path.display().to_string(),
                        source,
                    })?;
                if reg.chains.contains_key(&chain.id) {
                    return Err(RegistryError::DuplicateChain(chain.id));
                }
                reg.chains.insert(chain.id.clone(), chain);
            }
        }
        reg.validate()?;
        Ok(reg)
    }

    fn validate(&self) -> Result<(), RegistryError> {
        for r in &self.residency {
            if !self.chains.contains_key(&r.chain) {
                return Err(RegistryError::UnknownResidencyChain(r.chain.clone()));
            }
        }
        for c in self.chains.values() {
            if let Some(relay) = &c.relay {
                if !self.chains.contains_key(relay) {
                    return Err(RegistryError::UnknownRelay {
                        chain: c.id.clone(),
                        relay: relay.clone(),
                    });
                }
            }
        }
        // residency windows per (domain, network): no inversion, no overlap —
        // exactly one owner at any instant is what resolve_domain relies on
        let mut groups: BTreeMap<(&str, &str), Vec<&ResidencyEntry>> = BTreeMap::new();
        for r in &self.residency {
            groups
                .entry((r.domain.as_str(), r.network.as_str()))
                .or_default()
                .push(r);
        }
        for ((domain, network), mut entries) in groups {
            entries.sort_by_key(|r| r.from);
            for (i, r) in entries.iter().enumerate() {
                let inverted = r.to.map(|t| t <= r.from).unwrap_or(false);
                let overlaps_next = match (r.to, entries.get(i + 1)) {
                    (Some(t), Some(next)) => next.from < t,
                    (None, Some(_)) => true, // open-ended window must be last
                    _ => false,
                };
                if inverted || overlaps_next {
                    return Err(RegistryError::ResidencyOverlap {
                        domain: domain.to_string(),
                        network: network.to_string(),
                    });
                }
            }
        }
        Ok(())
    }

    pub fn chain(&self, id: &str) -> Option<&ChainConfig> {
        self.chains.get(id)
    }

    pub fn chains(&self) -> impl Iterator<Item = &ChainConfig> {
        self.chains.values()
    }

    /// Which chain hosts `domain` on `network` at time `at`?
    /// THE query behind migration-aware lookups (ARCHITECTURE.md §4).
    pub fn resolve_domain(
        &self,
        domain: &str,
        network: &str,
        at: DateTime<Utc>,
    ) -> Result<&ChainConfig, RegistryError> {
        let entry = self
            .residency
            .iter()
            .find(|r| {
                r.domain == domain
                    && r.network == network
                    && r.from <= at
                    && r.to.map(|t| at < t).unwrap_or(true)
            })
            .ok_or_else(|| RegistryError::NoResidency {
                domain: domain.to_string(),
                network: network.to_string(),
                at,
            })?;
        Ok(self
            .chains
            .get(&entry.chain)
            .expect("validated at load time"))
    }

    pub fn residency(&self) -> &[ResidencyEntry] {
        &self.residency
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn seeds_dir() -> std::path::PathBuf {
        // crates/registry -> workspace root -> registry-seeds
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../registry-seeds")
    }

    #[test]
    fn loads_seeds_and_validates() {
        let reg = Registry::load_from_dir(&seeds_dir()).expect("seeds load");
        assert!(reg.chain("polkadot").is_some());
        let ah = reg.chain("polkadot-asset-hub").expect("ah registered");
        assert_eq!(ah.para_id, Some(1000));
        assert_eq!(ah.relay.as_deref(), Some("polkadot"));
        assert!(ah.has_capability("governance"));
        assert!(ah.has_capability("assets"));
        assert!(!reg.chain("polkadot").unwrap().has_capability("governance"));
    }

    #[test]
    fn governance_residency_crosses_the_migration() {
        let reg = Registry::load_from_dir(&seeds_dir()).unwrap();
        let before = Utc.with_ymd_and_hms(2025, 6, 1, 0, 0, 0).unwrap();
        let after = Utc.with_ymd_and_hms(2026, 1, 27, 12, 0, 0).unwrap();
        assert_eq!(reg.resolve_domain("governance", "polkadot", before).unwrap().id, "polkadot");
        assert_eq!(
            reg.resolve_domain("governance", "polkadot", after).unwrap().id,
            "polkadot-asset-hub"
        );
        // consensus never moved
        assert_eq!(reg.resolve_domain("consensus", "polkadot", after).unwrap().id, "polkadot");
    }

    #[test]
    fn unknown_domain_is_an_error_not_a_guess() {
        let reg = Registry::load_from_dir(&seeds_dir()).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 8, 15, 0, 0, 0).unwrap();
        assert!(reg.resolve_domain("identity", "polkadot", now).is_err());
    }

    #[test]
    fn overlapping_residency_windows_are_rejected_at_load() {
        let dir = std::env::temp_dir().join(format!("dotlens-reg-overlap-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("chain-a.yaml"),
            "id: a\nname: A\nfamily: substrate\nnetwork: testnet\n",
        )
        .unwrap();
        std::fs::write(
            dir.join("domain-residency.yaml"),
            concat!(
                "residency:\n",
                "  - {domain: gov, network: testnet, chain: a, from: \"2020-01-01T00:00:00Z\", to: \"2022-01-01T00:00:00Z\"}\n",
                "  - {domain: gov, network: testnet, chain: a, from: \"2021-06-01T00:00:00Z\", to: null}\n",
            ),
        )
        .unwrap();
        assert!(matches!(
            Registry::load_from_dir(&dir),
            Err(RegistryError::ResidencyOverlap { .. })
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn lifecycle_status_resolves_over_time() {
        let reg = Registry::load_from_dir(&seeds_dir()).unwrap();
        let dot = reg.chain("polkadot").unwrap();
        let t = Utc.with_ymd_and_hms(2026, 8, 15, 0, 0, 0).unwrap();
        assert_eq!(dot.status_at(t), Some(LifecycleStatus::Live));
        let pre_genesis = Utc.with_ymd_and_hms(2019, 1, 1, 0, 0, 0).unwrap();
        assert_eq!(dot.status_at(pre_genesis), None);
    }
}
