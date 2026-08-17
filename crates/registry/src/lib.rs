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

/// A well-known account that CANNOT be derived (location-derived treasury
/// accounts, exchange hot wallets, ...). Derivable accounts (modl/para/sibl)
/// must never be seeded here — the labeling engine generates those
/// (ECOSYSTEM.md §6: generate, don't curate).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountSeed {
    /// SS58 or 0x-hex. Checksum-validated at label sync (fails loudly there —
    /// this crate stays free of family-specific address logic).
    pub address: String,
    pub label: String,
    /// pallet|para_sovereign|sibl_sovereign|treasury|bounty|multisig|proxy|
    /// exchange|user_tagged (core.account_labels.kind).
    pub kind: String,
    #[serde(default)]
    pub note: Option<String>,
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
    /// Well-known non-derivable accounts to label (source = "registry").
    #[serde(default)]
    pub accounts: Vec<AccountSeed>,
    /// Short names a human may type for this chain in the search grammar's
    /// `on <chain>` suffix — `ah`, `relay`, `ppl`. DATA, never a code list, so
    /// a chain registered in Phase 3 brings its own aliases with it and the
    /// resolver needs no edit (Invariant 2). The chain's `id` and `name` always
    /// resolve too and need not be repeated here.
    #[serde(default)]
    pub aliases: Vec<String>,
}

/// Fold a chain token to its comparable form: lower-case, separators removed.
/// `Asset Hub`, `asset-hub`, `asset_hub` and `assethub` are one word.
fn normalize_alias(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_alphanumeric())
        .flat_map(|c| c.to_lowercase())
        .collect()
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

/// Which residency domain carries a referenda INSTANCE. Class names are
/// adapter vocabulary ("referenda", "fellowship_referenda"); this table maps
/// them to a domain so the API can stitch a class across chains without ever
/// naming one. Adding an instance is a seed edit (Invariant 2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReferendaClass {
    pub class: String,
    pub domain: String,
    /// Decoder-lowercased instance pallet ("referenda", "fellowshipreferenda"),
    /// matching `gov.tracks.pallet`. Required to disambiguate a chain that runs
    /// SEVERAL referenda instances with COLLIDING track ids — Collectives runs
    /// three, where track 1 is "members" (Fellowship) and "ambassador"
    /// (Ambassador) at once. Optional so a pre-existing seed still loads;
    /// absent = no pallet filter, i.e. the old ambiguous behaviour, and the
    /// tracks response says which it did.
    #[serde(default)]
    pub pallet: Option<String>,
}

/// The public-OpenGov domain: what an unlisted class resolves to.
pub const DEFAULT_GOV_DOMAIN: &str = "governance";

/// The referenda instance every gov endpoint serves when none is requested.
pub const DEFAULT_REFERENDA_CLASS: &str = "referenda";

/// Which residency domain carries a treasury pallet INSTANCE. Same shape and
/// purpose as [`ReferendaClass`]: instance names are adapter vocabulary
/// ("treasury", "fellowship_treasury"), the registry says where each lives, so
/// no query code names a chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreasuryInstance {
    pub instance: String,
    pub domain: String,
}

/// The treasury instance every treasury endpoint serves when none is requested,
/// and the domain an unregistered instance falls back to.
pub const DEFAULT_TREASURY_INSTANCE: &str = "treasury";
pub const DEFAULT_TREASURY_DOMAIN: &str = "treasury";

#[derive(Debug, Clone, Deserialize)]
struct ResidencyFile {
    residency: Vec<ResidencyEntry>,
    #[serde(default)]
    referenda_classes: Vec<ReferendaClass>,
    #[serde(default)]
    treasury_instances: Vec<TreasuryInstance>,
}

#[derive(Debug, Clone, Default)]
pub struct Registry {
    chains: BTreeMap<String, ChainConfig>,
    residency: Vec<ResidencyEntry>,
    referenda_classes: Vec<ReferendaClass>,
    treasury_instances: Vec<TreasuryInstance>,
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
                reg.referenda_classes.extend(file.referenda_classes);
                reg.treasury_instances.extend(file.treasury_instances);
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

    /// Resolve a human-typed chain token — an id, a name, or a seeded alias.
    /// Case- and separator-insensitive, because `Asset Hub`, `asset-hub` and
    /// `assethub` are the same word to a person typing quickly.
    ///
    /// Returns None rather than guessing: the search grammar treats an
    /// unrecognized `on <token>` as a parse failure it can report, never as a
    /// silently dropped filter (slice 4's finding, where a query parameter was
    /// accepted and ignored).
    pub fn chain_by_alias(&self, token: &str) -> Option<&ChainConfig> {
        let want = normalize_alias(token);
        if want.is_empty() {
            return None;
        }
        self.chains.values().find(|c| {
            normalize_alias(&c.id) == want
                || normalize_alias(&c.name) == want
                || c.aliases.iter().any(|a| normalize_alias(a) == want)
        })
    }

    /// Every token that resolves to a chain, for the grammar reference and for
    /// "did you mean" — generated from the registry, so it cannot drift.
    pub fn chain_aliases(&self) -> Vec<(&str, &str)> {
        let mut out: Vec<(&str, &str)> = Vec::new();
        for c in self.chains.values() {
            out.push((c.id.as_str(), c.id.as_str()));
            for a in &c.aliases {
                out.push((a.as_str(), c.id.as_str()));
            }
        }
        out.sort();
        out
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

    /// Every registered referenda instance. The gov API walks these when no
    /// class is specified, so a newly registered instance appears without a
    /// code change.
    pub fn referenda_classes(&self) -> &[ReferendaClass] {
        &self.referenda_classes
    }

    /// Residency domain carrying a referenda class. Unlisted classes fall back
    /// to the public-OpenGov domain — a new instance that nobody registered
    /// still resolves somewhere sensible instead of 404-ing.
    pub fn domain_for_class(&self, class: &str) -> &str {
        self.referenda_classes
            .iter()
            .find(|c| c.class == class)
            .map(|c| c.domain.as_str())
            .unwrap_or(DEFAULT_GOV_DOMAIN)
    }

    /// Every registered treasury instance. The API walks these when no
    /// instance is specified, so a newly registered one appears with no code.
    pub fn treasury_instances(&self) -> &[TreasuryInstance] {
        &self.treasury_instances
    }

    /// Residency domain carrying a treasury instance; unregistered instances
    /// fall back to the main treasury domain.
    pub fn domain_for_treasury_instance(&self, instance: &str) -> &str {
        self.treasury_instances
            .iter()
            .find(|t| t.instance == instance)
            .map(|t| t.domain.as_str())
            .unwrap_or(DEFAULT_TREASURY_DOMAIN)
    }

    /// The instance pallet a class's tracks live under, when registered.
    /// `None` = unfiltered (and callers must say so rather than imply a
    /// single-instance chain).
    pub fn pallet_for_class(&self, class: &str) -> Option<&str> {
        self.referenda_classes
            .iter()
            .find(|c| c.class == class)
            .and_then(|c| c.pallet.as_deref())
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
        // seeded well-known accounts (non-derivable only) parse from YAML
        assert_eq!(ah.accounts.len(), 2);
        assert!(ah.accounts.iter().all(|a| a.kind == "treasury"));
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
        assert!(reg.resolve_domain("sharding", "polkadot", now).is_err());
        // a registered domain on an unregistered network is still an error
        assert!(reg.resolve_domain("governance", "kusama", now).is_err());
    }

    #[test]
    fn collectives_and_people_register_with_zero_adapter_code() {
        let reg = Registry::load_from_dir(&seeds_dir()).unwrap();
        let collectives = reg.chain("polkadot-collectives").expect("collectives");
        assert_eq!(collectives.para_id, Some(1001));
        assert_eq!(collectives.relay.as_deref(), Some("polkadot"));
        assert!(collectives.has_module("governance"), "fellowship referenda + votes");
        assert!(collectives.has_capability("fellowship"));
        let people = reg.chain("polkadot-people").expect("people");
        assert_eq!(people.para_id, Some(1004));
        assert!(people.has_capability("identity"));
        // no identity MODULE yet (Phase 5) — the domain resolves anyway
        assert!(!people.has_module("identity"));
        assert!(people.endpoints.rpc.iter().all(|e| e.starts_with("wss://")));
    }

    /// The third plug-and-play test, and the first NON-SYSTEM parachain: a chain
    /// joins by adding one YAML file (Invariant 2).
    #[test]
    fn hydration_registers_as_data_with_no_adapter_change() {
        let reg = Registry::load_from_dir(&seeds_dir()).unwrap();
        let h = reg.chain("hydration").expect("hydration seeded");
        assert_eq!(h.para_id, Some(2034));
        assert_eq!(h.relay.as_deref(), Some("polkadot"));
        assert_eq!(h.family, ChainFamily::Substrate);
        // NOT 63 — the runtime constant moved to 0 in v38.0.0 (2025-05-13) with
        // the unified address format, and the ss58-registry is stale.
        assert_eq!(h.ss58_prefix, Some(0));
        assert!(h.endpoints.rpc.iter().all(|e| e.starts_with("wss://")));
        assert!(h.has_module("xcm"), "the xcm follower is gated on this");
        // `balances` is DELIBERATELY off: pallet-balances holds HDX only, and
        // every other asset lives in orml-tokens or in EVM storage, so enabling
        // it would report the treasury's position here as nothing.
        assert!(!h.has_module("balances"));
        // It runs its own OpenGov, whose pallet names collide with Polkadot's —
        // enabling the module before the class is scoped would merge two id
        // spaces.
        assert!(!h.has_module("governance"));
        // Every chain that can carry XCM traffic must declare the module, or its
        // follower silently never starts — the defect the treasury slice paid
        // for, and the reason this asserts the whole set rather than Hydration
        // alone. The relay is half the transport story: it RECEIVES every UMP
        // message and SENDS every DMP one.
        for chain in [
            "polkadot",
            "polkadot-asset-hub",
            "polkadot-collectives",
            "polkadot-people",
            "hydration",
        ] {
            assert!(
                reg.chain(chain).expect(chain).has_module("xcm"),
                "{chain} must declare the xcm module"
            );
        }
        for alias in ["hdx", "hydradx", "hydra"] {
            assert_eq!(
                reg.chain_by_alias(alias).map(|c| c.id.as_str()),
                Some("hydration"),
                "a chain brings its own aliases; api::search needs no edit"
            );
        }
    }

    #[test]
    fn fellowship_and_identity_residency_resolve() {
        let reg = Registry::load_from_dir(&seeds_dir()).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 8, 15, 0, 0, 0).unwrap();
        // the Fellowship did not follow governance to Asset Hub
        assert_eq!(
            reg.resolve_domain("fellowship", "polkadot", now).unwrap().id,
            "polkadot-collectives"
        );
        assert_eq!(
            reg.resolve_domain("governance", "polkadot", now).unwrap().id,
            "polkadot-asset-hub"
        );
        // identity: the OTHER migration, relay → People on 2024-07-25
        let before = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        assert_eq!(reg.resolve_domain("identity", "polkadot", before).unwrap().id, "polkadot");
        assert_eq!(
            reg.resolve_domain("identity", "polkadot", now).unwrap().id,
            "polkadot-people"
        );
    }

    #[test]
    fn referenda_classes_map_to_domains_with_a_default() {
        let reg = Registry::load_from_dir(&seeds_dir()).unwrap();
        assert_eq!(reg.domain_for_class("referenda"), "governance");
        assert_eq!(reg.domain_for_class("fellowship_referenda"), "fellowship");
        // an instance nobody registered still resolves to public OpenGov
        assert_eq!(reg.domain_for_class("ambassador_referenda"), DEFAULT_GOV_DOMAIN);

        // the pallet each class's tracks live under — the disambiguator for a
        // chain running several instances with colliding track ids
        assert_eq!(reg.pallet_for_class("referenda"), Some("referenda"));
        assert_eq!(
            reg.pallet_for_class("fellowship_referenda"),
            Some("fellowshipreferenda")
        );
        assert_eq!(reg.pallet_for_class("ambassador_referenda"), None);
        // every registered class points at a domain that actually has windows
        for c in reg.referenda_classes() {
            assert!(
                reg.residency().iter().any(|r| r.domain == c.domain),
                "class {} maps to domain {} with no residency window",
                c.class,
                c.domain
            );
        }
    }

    #[test]
    fn treasury_instances_map_to_domains_with_a_default() {
        let reg = Registry::load_from_dir(&seeds_dir()).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 8, 15, 0, 0, 0).unwrap();
        assert_eq!(reg.domain_for_treasury_instance("treasury"), "treasury");
        assert_eq!(
            reg.domain_for_treasury_instance("fellowship_treasury"),
            "fellowship_treasury"
        );
        // unregistered instance falls back to the main treasury domain
        assert_eq!(
            reg.domain_for_treasury_instance("some_future_treasury"),
            DEFAULT_TREASURY_DOMAIN
        );

        // the main treasury followed governance to Asset Hub; the Collectives
        // sub-treasuries did not move, because their chain did not
        assert_eq!(
            reg.resolve_domain("treasury", "polkadot", now).unwrap().id,
            "polkadot-asset-hub"
        );
        for domain in ["fellowship_treasury", "ambassador_treasury"] {
            assert_eq!(
                reg.resolve_domain(domain, "polkadot", now).unwrap().id,
                "polkadot-collectives",
                "{domain}"
            );
        }
        for t in reg.treasury_instances() {
            assert!(
                reg.residency().iter().any(|r| r.domain == t.domain),
                "treasury instance {} maps to domain {} with no residency window",
                t.instance,
                t.domain
            );
        }
        // every chain that hosts an instance must enable the module, or the
        // follower silently never starts for it
        for chain in ["polkadot", "polkadot-asset-hub", "polkadot-collectives"] {
            assert!(
                reg.chain(chain).unwrap().has_module("treasury"),
                "{chain} hosts a treasury instance but has no treasury module"
            );
        }
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
