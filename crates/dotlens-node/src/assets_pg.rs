//! Postgres side of the assets + holdings slice:
//!
//!   `sync_assets`             the asset REGISTRY — every representation a
//!                             chain holds, discovered from its own storage
//!   `sync_treasury_accounts`  the ACCOUNT list — which accounts are treasury
//!                             money, and why
//!   `snapshot_holdings`       one state read per (treasury account, asset),
//!                             batched, landing in `balances.balance_anchors`
//!
//! Holdings are ANCHORS, not a third table. A position is "the latest anchor
//! at or before H, plus the deltas after it" — the same arithmetic the balance
//! history endpoint has done since Phase 1, asked per asset instead of per
//! account. Storing the answer separately would create a number with weaker
//! lineage than the two we already keep, and it would go stale silently.
//!
//! NUMERIC values are bound as text and cast server-side, the 0005 convention:
//! plancks and 6-decimal stablecoin units alike exceed u64, and this workspace
//! deliberately carries no decimal dependency.

use anyhow::{Context, Result};
use sqlx::PgPool;

// ------------------------------------------------------------ asset registry

#[derive(Debug, Default)]
pub struct AssetSyncReport {
    pub chain_id: String,
    pub height: u64,
    pub spec_version: u32,
    /// One entry per instance found, e.g. "assets=214".
    pub per_instance: Vec<(String, usize)>,
    /// Instances whose STATE we indexed but whose EVENTS this adapter has no
    /// vocabulary for. Reported loudly rather than logged at debug: it means
    /// balances for those assets will have anchors and no deltas.
    pub unmapped_instances: Vec<String>,
    /// Assets whose id could not be decoded back from its storage key. Zero
    /// is the only acceptable number; anything else means the key layout is
    /// not what metadata says it is.
    pub undecodable_ids: usize,
}

impl AssetSyncReport {
    pub fn total(&self) -> usize {
        self.per_instance.iter().map(|(_, n)| n).sum()
    }
}

/// Upsert one asset representation. Facts observed later never erase facts
/// observed earlier UNLESS this read is at least as recent: `observed_height`
/// is the guard, the same discipline the projections use.
#[allow(clippy::too_many_arguments)]
pub async fn upsert_asset(
    pool: &PgPool,
    chain_id: &str,
    asset_key: &str,
    representation_kind: &str,
    local_id: Option<&str>,
    xcm_location: Option<&serde_json::Value>,
    location_key: Option<&str>,
    raw_key_bytes: Option<&[u8]>,
    meta: &adapter_substrate::assets::AssetMeta,
    details: &adapter_substrate::assets::AssetDetailsView,
    spec_version: Option<u32>,
    observed_height: Option<u64>,
    source: &str,
) -> Result<()> {
    sqlx::query(
        "insert into core.assets \
             (chain_id, asset_key, representation_kind, local_id, xcm_location, \
              location_key, raw_key_bytes, symbol, name, decimals, supply, \
              min_balance, is_sufficient, accounts, status, spec_version, \
              observed_height, source) \
         values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::numeric,$12::numeric,$13,$14,$15,$16,$17,$18) \
         on conflict (chain_id, asset_key) do update set \
             representation_kind = excluded.representation_kind, \
             local_id = coalesce(excluded.local_id, core.assets.local_id), \
             xcm_location = coalesce(excluded.xcm_location, core.assets.xcm_location), \
             location_key = coalesce(excluded.location_key, core.assets.location_key), \
             raw_key_bytes = coalesce(excluded.raw_key_bytes, core.assets.raw_key_bytes), \
             symbol = case when excluded.observed_height is not null \
                            and excluded.observed_height >= \
                                coalesce(core.assets.observed_height, -1) \
                           then excluded.symbol else core.assets.symbol end, \
             name = case when excluded.observed_height is not null \
                          and excluded.observed_height >= \
                              coalesce(core.assets.observed_height, -1) \
                         then excluded.name else core.assets.name end, \
             decimals = case when excluded.observed_height is not null \
                              and excluded.observed_height >= \
                                  coalesce(core.assets.observed_height, -1) \
                             then excluded.decimals else core.assets.decimals end, \
             supply = case when excluded.observed_height is not null \
                            and excluded.observed_height >= \
                                coalesce(core.assets.observed_height, -1) \
                           then excluded.supply else core.assets.supply end, \
             min_balance = case when excluded.observed_height is not null \
                                 and excluded.observed_height >= \
                                     coalesce(core.assets.observed_height, -1) \
                                then excluded.min_balance else core.assets.min_balance end, \
             is_sufficient = case when excluded.observed_height is not null \
                                   and excluded.observed_height >= \
                                       coalesce(core.assets.observed_height, -1) \
                                  then excluded.is_sufficient else core.assets.is_sufficient end, \
             accounts = case when excluded.observed_height is not null \
                              and excluded.observed_height >= \
                                  coalesce(core.assets.observed_height, -1) \
                             then excluded.accounts else core.assets.accounts end, \
             status = case when excluded.observed_height is not null \
                            and excluded.observed_height >= \
                                coalesce(core.assets.observed_height, -1) \
                           then excluded.status else core.assets.status end, \
             spec_version = case when excluded.observed_height is not null \
                                  and excluded.observed_height >= \
                                      coalesce(core.assets.observed_height, -1) \
                                 then excluded.spec_version else core.assets.spec_version end, \
             observed_height = greatest(excluded.observed_height, \
                                        core.assets.observed_height), \
             source = case when excluded.observed_height is not null \
                            and excluded.observed_height >= \
                                coalesce(core.assets.observed_height, -1) \
                           then excluded.source else core.assets.source end, \
             updated_at = now()",
    )
    .bind(chain_id)
    .bind(asset_key)
    .bind(representation_kind)
    .bind(local_id)
    .bind(xcm_location)
    .bind(location_key)
    .bind(raw_key_bytes)
    .bind(meta.symbol.as_deref())
    .bind(meta.name.as_deref())
    .bind(meta.decimals.map(|d| d as i32))
    .bind(details.supply.map(|s| s.to_string()))
    .bind(details.min_balance.map(|m| m.to_string()))
    .bind(details.is_sufficient)
    .bind(details.accounts.map(|a| a as i64))
    .bind(details.status.as_deref())
    .bind(spec_version.map(|s| s as i64))
    .bind(observed_height.map(|h| h as i64))
    .bind(source)
    .execute(pool)
    .await
    .with_context(|| format!("upserting asset {chain_id}/{asset_key}"))?;
    Ok(())
}

/// One asset representation, as the holdings snapshot needs it.
#[derive(Debug, Clone)]
pub struct AssetRow {
    pub asset_key: String,
    pub representation_kind: String,
    /// The asset id's SCALE encoding, as lifted from its storage key. Present
    /// only for assets a `sync-assets` has actually seen on chain; an asset
    /// known only from an event has none, and therefore cannot be read from
    /// state until the next sync.
    pub raw_key_bytes: Option<Vec<u8>>,
    pub symbol: Option<String>,
    pub decimals: Option<i32>,
    pub status: Option<String>,
}

pub async fn assets_for_chain(pool: &PgPool, chain_id: &str) -> Result<Vec<AssetRow>> {
    let rows: Vec<(String, String, Option<Vec<u8>>, Option<String>, Option<i32>, Option<String>)> =
        sqlx::query_as(
            "select asset_key, representation_kind, raw_key_bytes, symbol, decimals, status \
             from core.assets where chain_id = $1 order by asset_key",
        )
        .bind(chain_id)
        .fetch_all(pool)
        .await
        .with_context(|| format!("listing assets for {chain_id}"))?;
    Ok(rows
        .into_iter()
        .map(
            |(asset_key, representation_kind, raw_key_bytes, symbol, decimals, status)| AssetRow {
                asset_key,
                representation_kind,
                raw_key_bytes,
                symbol,
                decimals,
                status,
            },
        )
        .collect())
}

/// Record one ASSET balance anchor. Deliberately separate from
/// `balances_pg::insert_anchor` rather than a longer parameter list on it: an
/// asset account has ONE balance and a status, and pretending otherwise by
/// passing `reserved: 0` through a native-shaped struct is how a schema starts
/// lying. The column mapping (free = balance, reserved = 0, total = balance)
/// is stated in migration 0010 and applied in exactly this one place.
#[allow(clippy::too_many_arguments)]
pub async fn insert_asset_anchor(
    pool: &PgPool,
    chain_id: &str,
    account_id: &[u8],
    asset_key: &str,
    height: u64,
    balance: u128,
    status: Option<&str>,
    spec_version: Option<u32>,
    source: &str,
    note: Option<&str>,
) -> Result<()> {
    sqlx::query(
        "insert into balances.balance_anchors \
             (chain_id, account_id, asset, block_height, free, reserved, frozen, \
              total, status, spec_version, source, note) \
         values ($1,$2,$3,$4,$5::numeric,0::numeric,null::numeric,$5::numeric,$6,$7,$8,$9) \
         on conflict (chain_id, account_id, asset, block_height) do nothing",
    )
    .bind(chain_id)
    .bind(account_id)
    .bind(asset_key)
    .bind(height as i64)
    .bind(balance.to_string())
    .bind(status)
    .bind(spec_version.map(|s| s as i64))
    .bind(source)
    .bind(note)
    .execute(pool)
    .await
    .with_context(|| format!("anchoring {asset_key} on {chain_id}"))?;
    Ok(())
}

// ------------------------------------------------------- live: registry sync

/// How many keys to ask for per `state_getKeysPaged` / `state_queryStorageAt`
/// call. Public RPCs cap both; 250 is comfortably under every cap observed on
/// the endpoints in the registry seeds and keeps a full Asset Hub asset sync
/// to a handful of round trips.
#[cfg(feature = "live")]
const PAGE: u32 = 250;

/// Build (or refresh) the asset registry for one chain from its own storage.
///
/// The whole routine is metadata-driven end to end: which pallets are assets
/// pallets comes from their storage SHAPE, the hashers come from the storage
/// entry, the asset ids come out of the keys themselves, and the symbol and
/// decimals come from the chain's `Metadata` map. Nothing about USDT being
/// 1984, or 1984 having six decimals, is written down anywhere in dotlens.
#[cfg(feature = "live")]
#[allow(clippy::too_many_arguments)]
pub async fn sync_assets(
    pool: &PgPool,
    raw: &dyn raw_store::RawStore,
    source: &adapter_substrate::source::SubstrateSource,
    chain_id: &str,
    height: Option<u64>,
) -> Result<AssetSyncReport> {
    use adapter_substrate::assets as aa;
    use ingest::live::ChainSource;

    let height = match height {
        Some(h) => h,
        None => source
            .finalized_height()
            .await
            .map_err(|e| anyhow::anyhow!(e))?,
    };
    let hash = source.block_hash(height).await.map_err(|e| anyhow::anyhow!(e))?;
    let spec = source
        .runtime_version_at(hash)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    let metadata = metadata_for(raw, source, chain_id, spec, height).await?;

    let pallets = aa::assets_pallets_from_metadata(&metadata)
        .map_err(|e| anyhow::anyhow!("{chain_id}: {e}"))?;
    let mut report = AssetSyncReport {
        chain_id: chain_id.to_string(),
        height,
        spec_version: spec,
        ..Default::default()
    };

    // THE NATIVE TOKEN FIRST. Without a row for it, a treasury's DOT holding
    // is an integer with no unit while its USDT holding has one — worse than
    // both being unlabelled, because the reader cannot tell which is which.
    // Its XCM name is the empty interior, which is exactly what a treasury
    // spend denominated in the native token normalizes to.
    let native_meta = match source.chain_properties().await {
        Ok(props) => aa::native_token_from_properties(&props),
        Err(e) => {
            tracing::warn!(chain = %chain_id, error = %e,
                "system_properties unavailable — native token registered without \
                 symbol/decimals (amounts will show as exact integers only)");
            Default::default()
        }
    };
    let native_location = serde_json::json!({"parents": 0, "interior": []});
    upsert_asset(
        pool,
        chain_id,
        "native",
        "native",
        None,
        Some(&native_location),
        Some(&native_location.to_string()),
        None,
        &native_meta,
        &Default::default(),
        Some(spec),
        Some(height),
        "chain-properties",
    )
    .await?;
    report.per_instance.push(("native".into(), 1));

    for pallet in &pallets {
        if pallet.representation == aa::Representation::Unmapped {
            // loud, not debug: state will be indexed and events will NOT be,
            // so this instance's balances get anchors with no deltas between
            // them. That is a real coverage hole and it must be visible.
            tracing::warn!(
                chain = %chain_id, pallet = %pallet.name, key_prefix = %pallet.key_prefix,
                "assets-shaped pallet with no event vocabulary — state indexed, \
                 EVENTS NOT MAPPED (add it to adapter_substrate::assets::representation_for_pallet)"
            );
            report.unmapped_instances.push(pallet.name.clone());
        }
        let asset_entry = aa::storage_entry_info(&metadata, &pallet.storage_prefix, "Asset")
            .map_err(|e| anyhow::anyhow!("{chain_id}/{}: {e}", pallet.name))?;
        let meta_entry = aa::storage_entry_info(&metadata, &pallet.storage_prefix, "Metadata")
            .map_err(|e| anyhow::anyhow!("{chain_id}/{}: {e}", pallet.name))?;

        // 1. enumerate every asset id, straight out of the storage keys
        let prefix = aa::map_prefix(&pallet.storage_prefix, "Asset");
        let mut ids: Vec<Vec<u8>> = Vec::new();
        let mut start: Option<Vec<u8>> = None;
        loop {
            let page = source
                .storage_keys_paged(&prefix, PAGE, start.as_deref(), hash)
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let short = page.len() < PAGE as usize;
            start = page.last().cloned();
            for key in &page {
                match aa::asset_id_bytes_from_key(key, &asset_entry.hashers) {
                    Ok(bytes) => ids.push(bytes),
                    // a key we cannot take apart is COUNTED, never skipped
                    // silently: it means the runtime's key layout is not what
                    // its own metadata says, which is a finding, not noise
                    Err(e) => {
                        tracing::warn!(chain = %chain_id, pallet = %pallet.name, error = %e,
                            "storage key not decomposable — asset skipped");
                        report.undecodable_ids += 1;
                    }
                }
            }
            if short {
                break;
            }
        }

        // 2. read AssetDetails + AssetMetadata for all of them, batched
        let mut count = 0usize;
        for chunk in ids.chunks(PAGE as usize / 2) {
            let mut keys: Vec<Vec<u8>> = Vec::with_capacity(chunk.len() * 2);
            for id in chunk {
                keys.push(
                    aa::asset_map_key(&pallet.storage_prefix, "Asset", &asset_entry.hashers, id)
                        .map_err(|e| anyhow::anyhow!(e))?,
                );
                keys.push(
                    aa::asset_map_key(&pallet.storage_prefix, "Metadata", &meta_entry.hashers, id)
                        .map_err(|e| anyhow::anyhow!(e))?,
                );
            }
            let values: std::collections::HashMap<Vec<u8>, Vec<u8>> = source
                .storage_batch_at(&keys, hash)
                .await
                .map_err(|e| anyhow::anyhow!(e))?
                .into_iter()
                .filter_map(|(k, v)| v.map(|v| (k, v)))
                .collect();

            for (i, id) in chunk.iter().enumerate() {
                let details = match values.get(&keys[i * 2]) {
                    Some(bytes) => aa::decode_asset_details_with(&asset_entry, bytes)
                        .map_err(|e| anyhow::anyhow!("{chain_id}/{}: {e}", pallet.name))?,
                    // the key was listed a moment ago; absent now means the
                    // asset was destroyed between the two calls at the same
                    // block hash, which cannot happen — but an empty default
                    // is honest either way
                    None => Default::default(),
                };
                let meta = match values.get(&keys[i * 2 + 1]) {
                    Some(bytes) => aa::decode_asset_metadata_with(&meta_entry, bytes)
                        .map_err(|e| anyhow::anyhow!("{chain_id}/{}: {e}", pallet.name))?,
                    // Metadata is a ValueQuery map: an asset with no metadata
                    // set has NO storage entry, and that is a real state —
                    // an unnamed asset, not a decode failure
                    None => Default::default(),
                };
                let id_json = aa::decode_asset_id_with(&asset_entry, id)
                    .map_err(|e| anyhow::anyhow!("{chain_id}/{}: {e}", pallet.name))?;
                let Some(asset_key) = aa::asset_key_from_id(&pallet.key_prefix, &id_json) else {
                    report.undecodable_ids += 1;
                    continue;
                };
                // integer-keyed assets get the XCM name CONSTRUCTED from the
                // pallet's runtime index; location-keyed ones already have one
                let local_id = asset_key
                    .strip_prefix(&format!("{}:", pallet.key_prefix))
                    .filter(|rest| rest.chars().all(|c| c.is_ascii_digit()))
                    .map(str::to_string);
                let location = match local_id.as_deref().and_then(|n| n.parse::<u128>().ok()) {
                    Some(n) => Some(aa::local_asset_location(pallet.index, n)),
                    None => aa::normalize_location(&id_json),
                };
                // through `canonical_location`, which is what migration 0010
                // documents as this column's producer — `.to_string()` on an
                // already-normalized value agrees today and would drift the
                // day normalization changes
                let location_key = location.as_ref().and_then(aa::canonical_location);
                upsert_asset(
                    pool,
                    chain_id,
                    &asset_key,
                    pallet.representation.kind_str(),
                    local_id.as_deref(),
                    location.as_ref(),
                    location_key.as_deref(),
                    Some(id),
                    &meta,
                    &details,
                    Some(spec),
                    Some(height),
                    "sync-assets",
                )
                .await?;
                count += 1;
            }
        }
        report.per_instance.push((pallet.key_prefix.clone(), count));
    }
    Ok(report)
}

/// Archived metadata for `spec`, fetched AND archived if we do not have it —
/// so an anchor's lineage stays reproducible from the raw store forever (the
/// `anchor-balance` rule).
#[cfg(feature = "live")]
async fn metadata_for(
    raw: &dyn raw_store::RawStore,
    source: &adapter_substrate::source::SubstrateSource,
    chain_id: &str,
    spec: u32,
    height: u64,
) -> Result<Vec<u8>> {
    use ingest::live::ChainSource;
    let key = raw_store::keys::metadata(chain_id, spec);
    match raw.get(&key) {
        Ok(blob) => Ok(blob),
        Err(raw_store::RawStoreError::NotFound(_)) => {
            let blob = source
                .metadata_at(height)
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            raw.put(&key, &blob, "sync-assets")?;
            tracing::info!(chain = %chain_id, spec, "metadata archived while syncing assets");
            Ok(blob)
        }
        Err(e) => Err(e.into()),
    }
}

// ------------------------------------------------------- live: holdings

#[derive(Debug, Default)]
pub struct HoldingsReport {
    pub chain_id: String,
    pub height: u64,
    pub spec_version: u32,
    pub accounts: usize,
    pub assets_probed: usize,
    /// Anchors written with a non-zero balance — the ones that will show up
    /// as holdings.
    pub non_zero: usize,
    /// Assets registered but unreadable because no `sync-assets` has ever seen
    /// their storage key. Counted, because "we did not look" and "there is
    /// nothing there" must never be the same number.
    pub skipped_no_key: usize,
}

/// Anchor every registered treasury account against every registered asset on
/// one chain, at one block. This is the "where the funds are" snapshot.
///
/// ONE BLOCK, ONE HASH, for the whole sweep — a position assembled from reads
/// at different heights is not a position, it is a collage. Every anchor from
/// one run therefore shares a `block_height`, and the holdings endpoint can
/// state the height it is reporting as of.
///
/// Absent storage is recorded as a ZERO anchor with note 'absent', the same
/// rule `anchor-balance` uses: an account that has never touched an asset has
/// no entry, and "no entry" is a balance of zero, stated rather than inferred.
#[cfg(feature = "live")]
pub async fn snapshot_holdings(
    pool: &PgPool,
    raw: &dyn raw_store::RawStore,
    source: &adapter_substrate::source::SubstrateSource,
    chain_id: &str,
    height: Option<u64>,
) -> Result<HoldingsReport> {
    use adapter_substrate::{accounts as acct, assets as aa, balances as ab};
    use ingest::live::ChainSource;

    let height = match height {
        Some(h) => h,
        None => source
            .finalized_height()
            .await
            .map_err(|e| anyhow::anyhow!(e))?,
    };
    let hash = source.block_hash(height).await.map_err(|e| anyhow::anyhow!(e))?;
    let spec = source
        .runtime_version_at(hash)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    let metadata = metadata_for(raw, source, chain_id, spec, height).await?;

    let accounts = treasury_accounts_on_chain(pool, chain_id).await?;
    let assets = assets_for_chain(pool, chain_id).await?;
    let pallets = aa::assets_pallets_from_metadata(&metadata)
        .map_err(|e| anyhow::anyhow!("{chain_id}: {e}"))?;
    // ONE storage-entry resolution per pallet, not per (account, asset):
    // `storage_entry_info` re-decodes the whole metadata blob and clones the
    // type registry, so resolving it in the inner loop turned a few RPC calls
    // into thousands of full metadata decodes (both reviewers caught it)
    let mut account_entries: std::collections::HashMap<String, aa::StorageEntryInfo> =
        std::collections::HashMap::new();
    for p in &pallets {
        let entry = aa::storage_entry_info(&metadata, &p.storage_prefix, "Account")
            .map_err(|e| anyhow::anyhow!("{chain_id}/{}: {e}", p.name))?;
        account_entries.insert(p.storage_prefix.clone(), entry);
    }

    let mut report = HoldingsReport {
        chain_id: chain_id.to_string(),
        height,
        spec_version: spec,
        accounts: accounts.len(),
        ..Default::default()
    };

    // build the whole key list first: (account, asset) → key, plus one native
    // System.Account key per account
    struct Probe {
        account: Vec<u8>,
        asset_key: String,
        storage_prefix: Option<String>, // None = native
    }
    let mut probes: Vec<Probe> = Vec::new();
    let mut keys: Vec<Vec<u8>> = Vec::new();

    for a in &accounts {
        let Ok(account32) = <[u8; 32]>::try_from(a.account_id.as_slice()) else {
            anyhow::bail!("treasury account on {chain_id} is not 32 bytes");
        };
        keys.push(acct::system_account_key(&account32));
        probes.push(Probe {
            account: a.account_id.clone(),
            asset_key: "native".into(),
            storage_prefix: None,
        });
        // A LEGACY BOUNTY ACCOUNT IS PROBED FOR THE NATIVE ASSET ONLY.
        // pallet-bounties and pallet-child-bounties are native-token-only by
        // construction — they have no asset concept at all — so their
        // (account × asset) product is hundreds of reads per bounty that can
        // only ever answer zero, and bounty accounts outnumber pots by two
        // orders of magnitude. Keyed on the INSTANCE, not on `role = 'bounty'`:
        // the modern multi-asset generation genuinely holds assets, and
        // skipping it would blind the endpoint to the very bounties this slice
        // exists to see.
        if a.role == "bounty"
            && matches!(a.instance.as_deref(), Some("bounties" | "child_bounties"))
        {
            continue;
        }
        for asset in &assets {
            // the native row is probed above through System.Account and has no
            // asset-pallet key by design — counting it as "skipped" would print
            // a scary warning on a perfectly healthy run
            if asset.asset_key == "native" {
                continue;
            }
            let Some(raw_id) = asset.raw_key_bytes.as_ref() else {
                report.skipped_no_key += 1;
                continue;
            };
            // which instance owns this key prefix — from metadata, so a
            // renamed pallet is caught here rather than mis-read
            let Some(pallet) = pallets
                .iter()
                .find(|p| asset.asset_key.starts_with(&format!("{}:", p.key_prefix)))
            else {
                report.skipped_no_key += 1;
                continue;
            };
            let entry = &account_entries[&pallet.storage_prefix];
            keys.push(
                aa::asset_account_key(&pallet.storage_prefix, &entry.hashers, raw_id, &account32)
                    .map_err(|e| anyhow::anyhow!(e))?,
            );
            probes.push(Probe {
                account: a.account_id.clone(),
                asset_key: asset.asset_key.clone(),
                storage_prefix: Some(pallet.storage_prefix.clone()),
            });
        }
    }
    report.assets_probed = probes
        .iter()
        .filter(|p| p.storage_prefix.is_some())
        .count();

    // read them in batches, then decode and anchor
    let mut values: std::collections::HashMap<Vec<u8>, Option<Vec<u8>>> =
        std::collections::HashMap::with_capacity(keys.len());
    for chunk in keys.chunks(PAGE as usize) {
        for (k, v) in source
            .storage_batch_at(chunk, hash)
            .await
            .map_err(|e| anyhow::anyhow!(e))?
        {
            values.insert(k, v);
        }
    }

    for (i, probe) in probes.iter().enumerate() {
        let value = values.get(&keys[i]).cloned().flatten();
        match &probe.storage_prefix {
            None => {
                let (balances, note) = match value {
                    Some(bytes) => (
                        ab::decode_account_info(&metadata, &bytes)
                            .map_err(|e| anyhow::anyhow!(e))?,
                        None,
                    ),
                    None => (
                        ab::AccountBalances { free: 0, reserved: 0, frozen: None },
                        Some("absent"),
                    ),
                };
                if balances.total() > 0 {
                    report.non_zero += 1;
                }
                crate::balances_pg::insert_anchor(
                    pool,
                    chain_id,
                    &probe.account,
                    "native",
                    height,
                    &balances,
                    Some(spec),
                    "treasury-holdings",
                    note,
                )
                .await?;
            }
            Some(prefix) => {
                let (holding, note) = match value {
                    Some(bytes) => (
                        aa::decode_asset_account_with(&account_entries[prefix], &bytes)
                            .map_err(|e| anyhow::anyhow!(e))?,
                        None,
                    ),
                    None => (
                        aa::AssetHolding { balance: 0, status: None },
                        Some("absent"),
                    ),
                };
                if holding.balance > 0 {
                    report.non_zero += 1;
                }
                insert_asset_anchor(
                    pool,
                    chain_id,
                    &probe.account,
                    &probe.asset_key,
                    height,
                    holding.balance,
                    holding.status.as_deref(),
                    Some(spec),
                    "treasury-holdings",
                    note,
                )
                .await?;
            }
        }
    }
    Ok(report)
}

// -------------------------------------------------------- treasury accounts

#[derive(Debug, Default)]
pub struct TreasuryAccountsReport {
    pub pots: usize,
    pub seeded: usize,
    /// Chains with no archived metadata yet — their pots appear on the next
    /// sync after live ingestion has run, exactly like pallet labels.
    pub chains_missing_metadata: Vec<String>,
}

/// Project the treasury account list from data that already exists: each
/// chain's own metadata (for pot accounts) and the registry seeds (for the
/// location-derived accounts that cannot be derived).
///
/// NOTHING IS HARDCODED HERE, and that is the point — a chain that gains a
/// treasury pallet contributes its pot the next time this runs, and the
/// Collectives sub-treasuries appear because Collectives' own runtime declares
/// their PalletIds, not because anybody typed `py/feltr`.
pub async fn sync_treasury_accounts(
    pool: &PgPool,
    registry: &registry::Registry,
    raw: &dyn raw_store::RawStore,
) -> Result<TreasuryAccountsReport> {
    use adapter_substrate::accounts as acct;
    use adapter_substrate::frame_decoder::ss58_encode;

    let mut report = TreasuryAccountsReport::default();
    let mut tx = pool.begin().await.context("begin treasury account sync")?;

    for chain in registry
        .chains()
        .filter(|c| c.family == registry::ChainFamily::Substrate)
    {
        let prefix = chain.ss58_prefix.unwrap_or(42);

        // ---- pots, from the chain's own metadata -----------------------
        let blob_loc: Option<(String,)> = sqlx::query_as(
            "select metadata_blob_location from substrate.runtime_versions \
             where chain_id = $1 and metadata_blob_location is not null \
             order by spec_version desc limit 1",
        )
        .bind(&chain.id)
        .fetch_optional(&mut *tx)
        .await
        .with_context(|| format!("looking up metadata for {}", chain.id))?;

        match blob_loc.and_then(|(loc,)| raw.get(&loc).ok()) {
            None => report.chains_missing_metadata.push(chain.id.clone()),
            Some(blob) => {
                // `continue` here would skip the SEEDED accounts below too —
                // a metadata problem must cost us the derived pots and nothing
                // else (reviewer catch)
                let pallet_ids = match acct::pallet_ids_from_metadata(&blob) {
                    Ok(ids) => ids,
                    Err(e) => {
                        tracing::warn!(chain = %chain.id, error = %e,
                            "PalletId walk failed — skipping treasury pots for this chain");
                        report.chains_missing_metadata.push(chain.id.clone());
                        Vec::new()
                    }
                };
                for pc in pallet_ids {
                    // the SAME pallet → instance vocabulary the treasury
                    // mapper uses, so the account list and the spend list can
                    // never disagree about what "fellowship_treasury" means
                    let Some(instance) =
                        adapter_substrate::treasury::instance_for_pallet(&pc.pallet.to_lowercase())
                    else {
                        continue;
                    };
                    let account = acct::pallet_account(&pc.id);
                    upsert_treasury_account(
                        &mut tx,
                        &chain.id,
                        &account,
                        "pot",
                        &chain.network,
                        Some(instance),
                        &format!("{} pot ({})", pc.pallet, acct::ascii_pallet_id(&pc.id)),
                        Some(&format!("modl:{}", acct::ascii_pallet_id(&pc.id))),
                        "derived",
                        &ss58_encode(prefix, &account),
                        None,
                        // a pot exists as long as its pallet does
                        true,
                    )
                    .await?;
                    report.pots += 1;
                }
            }
        }

        // ---- seeded: the accounts that cannot be derived ----------------
        for seed in chain.accounts.iter().filter(|a| a.kind == "treasury") {
            let account = acct::parse_account(&seed.address).map_err(|e| {
                anyhow::anyhow!("seed account '{}' on {} is invalid: {e}", seed.address, chain.id)
            })?;
            upsert_treasury_account(
                &mut tx,
                &chain.id,
                &account,
                "seeded",
                &chain.network,
                None,
                &seed.label,
                None,
                "registry",
                &ss58_encode(prefix, &account),
                seed.note.as_deref(),
                // a seeded account is in the registry because somebody decided
                // it is treasury money; only the registry retires it
                true,
            )
            .await?;
            report.seeded += 1;
        }
    }

    tx.commit().await.context("commit treasury account sync")?;
    Ok(report)
}

/// Shared with `bounties_pg::sync_bounty_accounts`: a bounty account is a
/// treasury account with a different `role`, and two writers of one table would
/// eventually disagree about its conflict rule.
///
/// `active` is a PARAMETER rather than a hardcoded `true` because until it was,
/// no caller could ever retire an account: the holdings sweep reads `where
/// active`, and a bounty that has been claimed holds nothing and is gone from
/// pallet storage, but would have been probed against every asset forever.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn upsert_treasury_account(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    chain_id: &str,
    account_id: &[u8; 32],
    role: &str,
    network: &str,
    instance: Option<&str>,
    label: &str,
    derivation: Option<&str>,
    source: &str,
    ss58: &str,
    note: Option<&str>,
    active: bool,
) -> Result<()> {
    sqlx::query(
        "insert into treasury.treasury_accounts \
             (chain_id, account_id, role, network, instance, label, derivation, \
              source, ss58, note, active) \
         values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) \
         on conflict (chain_id, account_id, role) do update set \
             network = excluded.network, instance = excluded.instance, \
             label = excluded.label, derivation = excluded.derivation, \
             source = excluded.source, ss58 = excluded.ss58, \
             note = coalesce(excluded.note, treasury.treasury_accounts.note), \
             active = excluded.active, updated_at = now()",
    )
    .bind(chain_id)
    .bind(&account_id[..])
    .bind(role)
    .bind(network)
    .bind(instance)
    .bind(label)
    .bind(derivation)
    .bind(source)
    .bind(ss58)
    .bind(note)
    .bind(active)
    .execute(&mut **tx)
    .await
    .with_context(|| format!("upserting treasury account '{label}' on {chain_id}"))?;
    Ok(())
}

/// One treasury account, as the holdings snapshot and the API need it.
#[derive(Debug, Clone)]
pub struct TreasuryAccountRow {
    pub chain_id: String,
    pub account_id: Vec<u8>,
    pub role: String,
    pub instance: Option<String>,
    pub label: String,
    pub derivation: Option<String>,
    pub source: String,
    pub ss58: Option<String>,
}

pub async fn treasury_accounts_on_chain(
    pool: &PgPool,
    chain_id: &str,
) -> Result<Vec<TreasuryAccountRow>> {
    let rows: Vec<(
        String,
        Vec<u8>,
        String,
        Option<String>,
        String,
        Option<String>,
        String,
        Option<String>,
    )> = sqlx::query_as(
        "select chain_id, account_id, role, instance, label, derivation, source, ss58 \
         from treasury.treasury_accounts where chain_id = $1 and active \
         order by role, label",
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await
    .with_context(|| format!("listing treasury accounts on {chain_id}"))?;
    Ok(rows
        .into_iter()
        .map(
            |(chain_id, account_id, role, instance, label, derivation, source, ss58)| {
                TreasuryAccountRow {
                    chain_id,
                    account_id,
                    role,
                    instance,
                    label,
                    derivation,
                    source,
                    ss58,
                }
            },
        )
        .collect())
}
