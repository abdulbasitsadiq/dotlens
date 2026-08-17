//! SubstrateSource: the live fetch side of the Substrate adapter.
//! Implements `ingest::live::ChainSource` over the legacy JSON-RPC surface via
//! subxt, with multi-endpoint failover (registry endpoints, rotated on error).
//!
//! Parachain differences are CONFIG, not code: this source works unchanged for
//! the relay, system chains, and any Substrate parachain (Invariant 2).
//!
//! subxt 0.50 note: the legacy RPC surface lives in the `subxt-rpcs` crate
//! (re-exported as `subxt::rpcs`); `LegacyRpcMethods` is parameterized over
//! `subxt_rpcs::RpcConfig`, satisfied via the `RpcConfigFor<Config>` adapter.

use async_trait::async_trait;
use ingest::live::{ChainSource, FetchedBlock, RawArtifact, SourceError};
use ingest::tip::TipSource;
use subxt::config::RpcConfigFor;
use subxt::rpcs::{LegacyRpcMethods, RpcClient};
use subxt::PolkadotConfig;
use tokio::sync::Mutex;

/// twox128("System") ++ twox128("Events") — the storage key of System.Events.
/// Substrate-protocol knowledge; allowed here (adapters only — Invariant 4).
const SYSTEM_EVENTS_KEY: [u8; 32] = [
    0x26, 0xaa, 0x39, 0x4e, 0xea, 0x56, 0x30, 0xe0, 0x7c, 0x48, 0xae, 0x0c, 0x95, 0x58, 0xce,
    0xf7, 0x80, 0xd4, 0x1e, 0x5e, 0x16, 0x05, 0x67, 0x65, 0xbc, 0x84, 0x61, 0x85, 0x10, 0x72,
    0xc9, 0xd7,
];

type Methods = LegacyRpcMethods<RpcConfigFor<PolkadotConfig>>;

struct Connection {
    endpoint_index: usize,
    /// RpcClient is cheaply Clone; LegacyRpcMethods is rebuilt from it per
    /// call (avoids relying on a Clone impl for Methods).
    client: RpcClient,
}

/// The block-hash type callers hold between `block_hash` and the `*_at`
/// probes — re-exported so dotlens-node never needs a direct subxt dep.
pub type BlockHash = subxt::utils::H256;

pub struct SubstrateSource {
    chain_id: String,
    endpoints: Vec<String>,
    /// Current connection; None = will (re)connect on next use, starting at
    /// `next_index`. Rotated on any RPC failure.
    conn: Mutex<Option<Connection>>,
    next_index: std::sync::atomic::AtomicUsize,
}

impl SubstrateSource {
    pub fn new(chain_id: impl Into<String>, endpoints: Vec<String>) -> Result<Self, SourceError> {
        let chain_id = chain_id.into();
        if endpoints.is_empty() {
            return Err(SourceError::Exhausted(format!(
                "chain {chain_id} has no rpc endpoints in the registry"
            )));
        }
        Ok(Self {
            chain_id,
            endpoints,
            conn: Mutex::new(None),
            next_index: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    /// Run `op` against a connected endpoint; on failure rotate and retry until
    /// every endpoint has been tried once this call. All-fail → `Exhausted`.
    async fn with_failover<T, F, Fut>(&self, what: &str, op: F) -> Result<T, SourceError>
    where
        F: Fn(Methods) -> Fut,
        Fut: std::future::Future<Output = Result<T, subxt::rpcs::Error>>,
    {
        let mut last_err = String::new();
        for _attempt in 0..self.endpoints.len() {
            // connect (or reuse) under the lock, then release before the call
            let methods = {
                let mut guard = self.conn.lock().await;
                if guard.is_none() {
                    let idx = self
                        .next_index
                        .load(std::sync::atomic::Ordering::Relaxed)
                        % self.endpoints.len();
                    let url = &self.endpoints[idx];
                    match RpcClient::from_url(url).await {
                        Ok(client) => {
                            tracing::info!(chain = %self.chain_id, %url, "rpc connected");
                            *guard = Some(Connection {
                                endpoint_index: idx,
                                client,
                            });
                        }
                        Err(e) => {
                            last_err = format!("{url}: connect: {e}");
                            tracing::warn!(chain = %self.chain_id, %url, error = %e, "rpc connect failed — rotating");
                            self.next_index
                                .store(idx + 1, std::sync::atomic::Ordering::Relaxed);
                            continue;
                        }
                    }
                }
                LegacyRpcMethods::new(guard.as_ref().expect("just ensured").client.clone())
            };

            match op(methods).await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    let mut guard = self.conn.lock().await;
                    if let Some(conn) = guard.take() {
                        let url = &self.endpoints[conn.endpoint_index];
                        last_err = format!("{url}: {what}: {e}");
                        tracing::warn!(chain = %self.chain_id, %url, error = %e, "rpc call failed — rotating endpoint");
                        self.next_index
                            .store(conn.endpoint_index + 1, std::sync::atomic::Ordering::Relaxed);
                    } else {
                        last_err = format!("{what}: {e}");
                    }
                }
            }
        }
        Err(SourceError::Exhausted(format!(
            "chain {}: {last_err}",
            self.chain_id
        )))
    }

    async fn hash_at(&self, height: u64) -> Result<subxt::utils::H256, SourceError> {
        self.with_failover("chain_getBlockHash", |m| async move {
            m.chain_get_block_hash(Some(height.into())).await
        })
        .await?
        .ok_or(SourceError::NotFound(height))
    }

    /// Block hash at `height` — public so callers probing many keys at one
    /// height resolve the hash once (review catch: avoids N+1 RPC).
    pub async fn block_hash(&self, height: u64) -> Result<subxt::utils::H256, SourceError> {
        self.hash_at(height).await
    }

    /// Raw storage value at block `hash` (None = key absent in state).
    pub async fn storage_at(
        &self,
        key: &[u8],
        hash: subxt::utils::H256,
    ) -> Result<Option<Vec<u8>>, SourceError> {
        self.with_failover("state_getStorage", |m| {
            let key = key.to_vec();
            async move { m.state_get_storage(&key, Some(hash)).await }
        })
        .await
    }

    /// One page of storage KEYS under `prefix` at block `hash`. Paged because
    /// a map can hold millions of entries; the caller loops with the last key
    /// returned as `start_key` until a short page comes back.
    ///
    /// Enumerating keys is how the assets registry is built: with a concat
    /// hasher the encoded asset id is IN the key, so listing keys under
    /// `<Pallet>.Asset` yields every asset the chain holds, ids included,
    /// without a single assumption about what those ids look like.
    pub async fn storage_keys_paged(
        &self,
        prefix: &[u8],
        count: u32,
        start_key: Option<&[u8]>,
        hash: subxt::utils::H256,
    ) -> Result<Vec<Vec<u8>>, SourceError> {
        self.with_failover("state_getKeysPaged", |m| {
            let prefix = prefix.to_vec();
            let start = start_key.map(|s| s.to_vec());
            async move {
                m.state_get_keys_paged(&prefix, count, start.as_deref(), Some(hash))
                    .await
            }
        })
        .await
    }

    /// MANY storage values at one block in ONE request (`state_queryStorageAt`).
    /// Returns `(key, value)` pairs; a missing key comes back with `None`,
    /// which is meaningful data — an account that holds none of an asset has
    /// no storage entry at all, and that is a zero balance, not an error.
    ///
    /// This is what makes a treasury holdings snapshot cheap: every treasury
    /// account × every registered asset is a few hundred keys, i.e. a couple
    /// of round trips rather than a couple of thousand.
    pub async fn storage_batch_at(
        &self,
        keys: &[Vec<u8>],
        hash: subxt::utils::H256,
    ) -> Result<Vec<(Vec<u8>, Option<Vec<u8>>)>, SourceError> {
        if keys.is_empty() {
            return Ok(vec![]);
        }
        let sets = self
            .with_failover("state_queryStorageAt", |m| {
                let keys = keys.to_vec();
                async move {
                    m.state_query_storage_at(keys.iter().map(|k| k.as_slice()), Some(hash))
                        .await
                }
            })
            .await?;
        Ok(sets
            .into_iter()
            .flat_map(|set| set.changes)
            .map(|(k, v)| (k.0, v.map(|b| b.0)))
            .collect())
    }

    /// Does `key` exist in storage at block `hash`? Existence only (Some vs
    /// None) — used by verify-labels to confirm derived system accounts
    /// on-chain without needing to decode AccountInfo.
    pub async fn storage_contains_at(
        &self,
        key: &[u8],
        hash: subxt::utils::H256,
    ) -> Result<bool, SourceError> {
        Ok(self.storage_at(key, hash).await?.is_some())
    }

    /// The node's chain properties (`tokenSymbol`, `tokenDecimals`, `ss58Format`).
    ///
    /// NOTE this is the only fact in the assets path that is NOT block-scoped:
    /// `system_properties` reports the node's current view, with no block hash
    /// to pin it to. It is how the NATIVE token gets a symbol and decimals at
    /// all — without it, a treasury's DOT holding is an integer with no unit
    /// while its USDT holding has one, which is a worse lie than having
    /// neither. Rows written from it say `source = 'chain-properties'`.
    pub async fn chain_properties(&self) -> Result<serde_json::Value, SourceError> {
        let props = self
            .with_failover("system_properties", |m| async move {
                m.system_properties().await
            })
            .await?;
        Ok(serde_json::Value::Object(props.into_iter().collect()))
    }

    /// spec_version at block `hash` — anchors record the runtime they were
    /// decoded against (lineage).
    pub async fn runtime_version_at(
        &self,
        hash: subxt::utils::H256,
    ) -> Result<u32, SourceError> {
        Ok(self.runtime_version_info(hash).await?.spec_version)
    }

    /// spec_version PLUS the runtime's self-declared API list.
    ///
    /// `apis` is how a chain says which runtime APIs it implements and at which
    /// version — `[["0x…8 bytes", 2], …]`, the 8 bytes being blake2b-64 of the
    /// trait name. subxt's typed `RuntimeVersion` keeps only the two fields it
    /// needs and flattens the rest into `other`, so the list is read from there
    /// rather than being lost. This is the ONLY honest way to ask "can this
    /// chain dry-run" before trying: the alternative is to send a request and
    /// interpret an RPC error, which cannot distinguish "not implemented" from
    /// "the node is unwell".
    pub async fn runtime_version_info(
        &self,
        hash: subxt::utils::H256,
    ) -> Result<RuntimeVersionInfo, SourceError> {
        let rv = self
            .with_failover("state_getRuntimeVersion", |m| async move {
                m.state_get_runtime_version(Some(hash)).await
            })
            .await?;
        Ok(RuntimeVersionInfo {
            spec_version: rv.spec_version,
            apis: rv.other.get("apis").cloned().unwrap_or(serde_json::Value::Null),
        })
    }

    /// Execute a runtime API method against the state at `hash`.
    ///
    /// `function` is the wire name (`Trait_method`, e.g.
    /// `DryRunApi_dry_run_call`) and `params` is the plain concatenation of the
    /// SCALE-encoded arguments — runtime API parameters carry no length prefix
    /// and no tuple wrapper.
    ///
    /// READ-ONLY BY CONSTRUCTION, and worth being explicit about since this is
    /// the first place dotlens asks a runtime to EXECUTE something: `state_call`
    /// runs the wasm against a transient overlay the node discards, so nothing
    /// it writes reaches the chain, no signature is involved, nothing is
    /// gossiped and no key exists anywhere in this process (ARCHITECTURE §11a —
    /// dotlens never holds a key and never submits an extrinsic).
    pub async fn state_call(
        &self,
        function: &str,
        params: &[u8],
        hash: subxt::utils::H256,
    ) -> Result<Vec<u8>, SourceError> {
        self.with_failover("state_call", |m| {
            let function = function.to_string();
            let params = params.to_vec();
            async move {
                m.state_call(&function, Some(params.as_slice()), Some(hash))
                    .await
            }
        })
        .await
    }

    /// Metadata at an explicit VERSION, via `Metadata_metadata_at_version`.
    ///
    /// This pays the debt `metadata_at` has carried since Phase 1 slice 2:
    /// `state_getMetadata` returns v14 on every runtime we index, and v14 has no
    /// runtime-API section, so nothing that calls a runtime API can be built
    /// from it. `None` = this runtime does not offer that version, which is data
    /// (ask `metadata_versions` for what it does offer).
    ///
    /// THE RETURN IS DOUBLY ENCODED: the API returns `Option<OpaqueMetadata>`,
    /// and `OpaqueMetadata` is a newtype over `Vec<u8>` whose CONTENTS are the
    /// SCALE encoding of `RuntimeMetadataPrefixed` (the `meta` magic + version
    /// byte + body). Decoding once gives you bytes, not metadata.
    pub async fn metadata_at_version(
        &self,
        version: u32,
        hash: subxt::utils::H256,
    ) -> Result<Option<Vec<u8>>, SourceError> {
        use parity_scale_codec::{Decode, Encode};
        let raw = self
            .state_call("Metadata_metadata_at_version", &version.encode(), hash)
            .await?;
        Option::<Vec<u8>>::decode(&mut &raw[..]).map_err(|e| {
            SourceError::Rpc(format!(
                "Metadata_metadata_at_version({version}) did not return Option<OpaqueMetadata>: {e}"
            ))
        })
    }

    /// The metadata versions this runtime can produce (expect `[14, 15, 16]`).
    pub async fn metadata_versions(
        &self,
        hash: subxt::utils::H256,
    ) -> Result<Vec<u32>, SourceError> {
        use parity_scale_codec::Decode;
        let raw = self
            .state_call("Metadata_metadata_versions", &[], hash)
            .await?;
        Vec::<u32>::decode(&mut &raw[..]).map_err(|e| {
            SourceError::Rpc(format!(
                "Metadata_metadata_versions did not return Vec<u32>: {e}"
            ))
        })
    }
}

/// What `state_getRuntimeVersion` tells us, including the part subxt's typed
/// struct discards.
#[derive(Debug, Clone)]
pub struct RuntimeVersionInfo {
    pub spec_version: u32,
    /// The raw `apis` JSON: `[["0x…", version], …]`, or `Null` if the node did
    /// not send one. Interpreted by `dryrun::declared_api_version`.
    pub apis: serde_json::Value,
}

fn hex32(h: &subxt::utils::H256) -> String {
    format!("0x{}", hex::encode(h.as_ref()))
}

impl SubstrateSource {
    /// Shared fetch: the envelope's `finalized` flag is the ONLY difference
    /// between the finalized pipeline's fetches and the tip worker's — the
    /// decoder propagates it into the canonical row.
    async fn fetch_block_impl(
        &self,
        height: u64,
        finalized: bool,
    ) -> Result<FetchedBlock, SourceError> {
        let hash = self.hash_at(height).await?;

        let block = self
            .with_failover("chain_getBlock", |m| async move {
                m.chain_get_block(Some(hash)).await
            })
            .await?
            .ok_or(SourceError::NotFound(height))?;

        let runtime = self
            .with_failover("state_getRuntimeVersion", |m| async move {
                m.state_get_runtime_version(Some(hash)).await
            })
            .await?;

        let events = self
            .with_failover("state_getStorage(System.Events)", |m| async move {
                m.state_get_storage(&SYSTEM_EVENTS_KEY, Some(hash)).await
            })
            .await?;

        // Raw block artifact: deterministic JSON envelope; extrinsic bytes are
        // the SCALE hex exactly as returned by the node. (The JSON-RPC layer is
        // what "as received" means over this transport — noted in ARCHITECTURE §6.)
        let header = &block.block.header;
        let block_json = serde_json::json!({
            "chain_id": self.chain_id,
            "height": height,
            "hash": hex32(&hash),
            "parent_hash": hex32(&header.parent_hash),
            "state_root": hex32(&header.state_root),
            "extrinsics_root": hex32(&header.extrinsics_root),
            "spec_version": runtime.spec_version,
            "finalized": finalized,
            "extrinsics": block
                .block
                .extrinsics
                .iter()
                .map(|xt| format!("0x{}", hex::encode(&xt.0)))
                .collect::<Vec<_>>(),
        });
        let mut artifacts = vec![RawArtifact {
            item: "block.json".into(),
            bytes: serde_json::to_vec(&block_json)
                .map_err(|e| SourceError::Rpc(format!("serializing block envelope: {e}")))?,
        }];
        if let Some(ev) = events {
            artifacts.push(RawArtifact {
                item: "events.scale".into(),
                bytes: ev,
            });
        }

        Ok(FetchedBlock {
            height,
            hash: hex32(&hash),
            parent_hash: hex32(&header.parent_hash),
            runtime_version: runtime.spec_version,
            transaction_version: Some(runtime.transaction_version),
            artifacts,
        })
    }
}

#[async_trait]
impl TipSource for SubstrateSource {
    /// Best (unfinalized) head: chain_getHeader with no hash = current best.
    async fn best_height(&self) -> Result<u64, SourceError> {
        let header = self
            .with_failover("chain_getHeader(best)", |m| async move {
                m.chain_get_header(None).await
            })
            .await?
            .ok_or_else(|| SourceError::Rpc("no best header".into()))?;
        Ok(header.number as u64)
    }

    async fn canonical_hash_at(&self, height: u64) -> Result<String, SourceError> {
        Ok(hex32(&self.hash_at(height).await?))
    }

    async fn fetch_unfinalized(&self, height: u64) -> Result<FetchedBlock, SourceError> {
        self.fetch_block_impl(height, false).await
    }
}

#[async_trait]
impl ChainSource for SubstrateSource {
    async fn finalized_height(&self) -> Result<u64, SourceError> {
        let hash = self
            .with_failover("chain_getFinalizedHead", |m| async move {
                m.chain_get_finalized_head().await
            })
            .await?;
        let header = self
            .with_failover("chain_getHeader", |m| async move {
                m.chain_get_header(Some(hash)).await
            })
            .await?
            .ok_or_else(|| SourceError::Rpc("finalized head has no header".into()))?;
        Ok(header.number as u64)
    }

    async fn fetch_block(&self, height: u64) -> Result<FetchedBlock, SourceError> {
        self.fetch_block_impl(height, true).await
    }

    async fn metadata_at(&self, height: u64) -> Result<Vec<u8>, SourceError> {
        let hash = self.hash_at(height).await?;
        // state_getMetadata returns the highest version this path supports
        // (v14 on modern runtimes). v15/v16 via the Metadata runtime API is a
        // known debt for the frame-decode slice — the version byte we record
        // makes the difference visible, never guessed.
        let bytes = self
            .with_failover("state_getMetadata", |m| async move {
                m.state_get_metadata(Some(hash)).await
            })
            .await?;
        Ok(bytes.into_raw())
    }
}
