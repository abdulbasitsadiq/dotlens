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
    /// Assets that got an observer-free `absolute_key` (0019). Reported because
    /// the gap between this and `total()` is the population a cross-chain
    /// consolidation cannot yet add up, and that number should be looked at
    /// rather than assumed to be zero.
    pub absolutized: usize,
    /// Registry entries skipped because they name the chain's NATIVE token,
    /// which already has a `native` row. COUNTED, never silent: "we folded it
    /// in" and "there was nothing there" must not be the same number.
    pub native_alias_skipped: usize,
    /// Assets whose registry declares them Erc20 — registered, named and
    /// located here, and NOT anchorable by this slice because their balance
    /// lives in `pallet_evm` storage. The scope boundary, as a number.
    pub erc20_unanchorable: usize,
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
    // `absolute_*` is the observer-free name (0019), coalesced like its relative
    // sibling: a later read that could not absolutize must not erase one that
    // could. `asset_type` is the asset's own declared type where its registry
    // has one (Token, Erc20, …) — NULL for pallet-assets, which has no such
    // concept. (Plain `//`: rustc rejects `///` on a function parameter.)
    absolute_location: Option<&serde_json::Value>,
    absolute_key: Option<&str>,
    asset_type: Option<&str>,
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
              location_key, absolute_location, absolute_key, asset_type, \
              raw_key_bytes, symbol, name, decimals, supply, \
              min_balance, is_sufficient, accounts, status, spec_version, \
              observed_height, source) \
         values ($1,$2,$3,$4,$5,$6,$19,$20,$21,$7,$8,$9,$10,$11::numeric,$12::numeric,$13,$14,$15,$16,$17,$18) \
         on conflict (chain_id, asset_key) do update set \
             representation_kind = excluded.representation_kind, \
             local_id = coalesce(excluded.local_id, core.assets.local_id), \
             xcm_location = coalesce(excluded.xcm_location, core.assets.xcm_location), \
             location_key = coalesce(excluded.location_key, core.assets.location_key), \
             absolute_location = coalesce(excluded.absolute_location, \
                                          core.assets.absolute_location), \
             absolute_key = coalesce(excluded.absolute_key, core.assets.absolute_key), \
             asset_type = coalesce(excluded.asset_type, core.assets.asset_type), \
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
    // $19..$21 — appended rather than inserted mid-list so every existing
    // placeholder keeps its number and no bind silently shifts by one
    .bind(absolute_location)
    .bind(absolute_key)
    .bind(asset_type)
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
    /// The registry's own declared type (Token, Erc20, …). SELECTED, not
    /// decorative: without it the holdings sweep cannot tell an asset whose
    /// balance is in `pallet_evm` storage from one it simply does not hold, and
    /// would write a zero anchor for the money market — the exact conflation of
    /// "we did not look" with "there is nothing there" that `skipped_no_key`
    /// exists to prevent one struct below.
    pub asset_type: Option<String>,
}

// Wire form for one query, converted into `AssetRow` below. See `labels.rs`.
#[allow(clippy::type_complexity)]
pub async fn assets_for_chain(pool: &PgPool, chain_id: &str) -> Result<Vec<AssetRow>> {
    let rows: Vec<(
        String,
        String,
        Option<Vec<u8>>,
        Option<String>,
        Option<i32>,
        Option<String>,
        Option<String>,
    )> = sqlx::query_as(
        "select asset_key, representation_kind, raw_key_bytes, symbol, decimals, \
                    status, asset_type \
             from core.assets where chain_id = $1 order by asset_key",
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await
    .with_context(|| format!("listing assets for {chain_id}"))?;
    Ok(rows
        .into_iter()
        .map(
            |(
                asset_key,
                representation_kind,
                raw_key_bytes,
                symbol,
                decimals,
                status,
                asset_type,
            )| AssetRow {
                asset_key,
                representation_kind,
                raw_key_bytes,
                symbol,
                decimals,
                status,
                asset_type,
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
    cfg: &registry::ChainConfig,
    height: Option<u64>,
) -> Result<AssetSyncReport> {
    use adapter_substrate::assets as aa;
    use adapter_substrate::orml;
    use ingest::live::ChainSource;

    // TAKES THE WHOLE ChainConfig RATHER THAN AN ID, since 0019: absolutizing a
    // Location needs the OBSERVER'S OWN PATH, which is `network` + `para_id` —
    // registry data, so no chain is named in code and a chain registered later
    // absolutizes with no edit here (Invariant 2).
    let chain_id = cfg.id.as_str();
    let observer_path = orml::chain_path(&cfg.network, cfg.para_id);

    let height = match height {
        Some(h) => h,
        None => source
            .finalized_height()
            .await
            .map_err(|e| anyhow::anyhow!(e))?,
    };
    let hash = source
        .block_hash(height)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
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
    // `xcm_location` keeps the SELF-RELATIVE name it has always had — "this
    // chain's own currency, whatever that turns out to be" — which is what
    // `metadata_free_asset_key` resolves against and must not change.
    let native_location = serde_json::json!({"parents": 0, "interior": []});

    // THE ABSOLUTE NAME IS A DIFFERENT QUESTION, and a reviewer caught that
    // answering it from `{parents: 0, Here}` is WRONG on every system
    // parachain. "This chain's native currency" and "this chain as a location"
    // coincide only when the chain ISSUES its own token: Hydration issues HDX,
    // so HDX absolutizes to `[GC(Polkadot), Parachain(2034)]` — but Asset Hub
    // issues nothing, its native token is the RELAY's DOT, and absolutizing
    // `{parents: 0, Here}` there would produce a key naming the PARACHAIN. AH's
    // DOT would then join nothing, and the position that split off would be the
    // treasury's 24.3M DOT, which is the largest number the consolidator exists
    // to add up.
    //
    // `para_id` cannot answer it (Hydration and Asset Hub are both parachains)
    // and no runtime constant states it, so it is REGISTRY DATA. **Absent means
    // unknown, and unknown means NULL** — a wrong absolute name is worse than
    // none, and NULL is already 0019's documented "not absolutizable".
    let native_token_location = cfg.native_token.map(|k| k.location());
    if native_token_location.is_none() {
        tracing::warn!(chain = %chain_id,
            "seed declares no `native_token` — this chain's native currency gets \
             NO absolute name, so it cannot be recognised as the same asset on \
             any other chain");
    }
    let native_absolute = native_token_location
        .as_ref()
        .and_then(|l| orml::absolutize(&observer_path, l));
    let native_absolute_key = native_token_location
        .as_ref()
        .and_then(|l| orml::absolute_key(&observer_path, l));
    upsert_asset(
        pool,
        chain_id,
        "native",
        "native",
        None,
        Some(&native_location),
        Some(&native_location.to_string()),
        native_absolute.as_ref(),
        native_absolute_key.as_deref(),
        None,
        None,
        &native_meta,
        &Default::default(),
        Some(spec),
        Some(height),
        "chain-properties",
    )
    .await?;
    report.per_instance.push(("native".into(), 1));
    if native_absolute.is_some() {
        report.absolutized += 1;
    }

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
                // and the observer-free name, from the SAME location — which is
                // what lets this row and Hydration's row for the same asset be
                // recognised as one thing (0019)
                // through `orml::absolute_key`, which 0019 names as this
                // column's producer — `.to_string()` on an already-normalized
                // value agrees today and would drift the day the rendering
                // changes, which is verbatim the argument made for
                // `canonical_location` ten lines above
                let absolute = location
                    .as_ref()
                    .and_then(|l| orml::absolutize(&observer_path, l));
                let absolute_key = location
                    .as_ref()
                    .and_then(|l| orml::absolute_key(&observer_path, l));
                if absolute.is_some() {
                    report.absolutized += 1;
                }
                upsert_asset(
                    pool,
                    chain_id,
                    &asset_key,
                    pallet.representation.kind_str(),
                    local_id.as_deref(),
                    location.as_ref(),
                    location_key.as_deref(),
                    absolute.as_ref(),
                    absolute_key.as_deref(),
                    None, // pallet-assets has no asset_type concept
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

    sync_orml_registry(
        pool,
        source,
        &metadata,
        chain_id,
        &observer_path,
        hash,
        spec,
        height,
        &mut report,
    )
    .await?;
    Ok(report)
}

/// The ORML arm of the asset sync: `AssetRegistry.Assets` for identity and
/// `AssetRegistry.AssetLocations` for the XCM name, joined by asset id.
///
/// A no-op on a chain with no AssetRegistry-shaped pallet, which is every chain
/// dotlens indexed before Phase 3 slice 6 — the pallet is found by the SHAPE of
/// its storage (`Assets` + `AssetLocations`), so this needs no chain id and no
/// capability flag to decide whether to run.
///
/// THE KEY TRICK IS THE ONE SLICE 6 ALREADY BUILT, reused verbatim and it pays
/// off twice here: both AssetRegistry maps are `Blake2_128Concat`, so the asset
/// id falls straight out of a key's tail with no re-hashing — AND the same
/// 16-byte hash prefix can be reused against the `AssetLocations` prefix, so the
/// second map is read without hashing anything a second time.
#[cfg(feature = "live")]
#[allow(clippy::too_many_arguments)]
async fn sync_orml_registry(
    pool: &PgPool,
    source: &adapter_substrate::source::SubstrateSource,
    metadata: &[u8],
    chain_id: &str,
    observer_path: &[serde_json::Value],
    hash: adapter_substrate::source::BlockHash,
    spec: u32,
    height: u64,
    report: &mut AssetSyncReport,
) -> Result<()> {
    use adapter_substrate::assets as aa;
    use adapter_substrate::orml;

    let pallets = orml::orml_pallets_from_metadata(metadata)
        .map_err(|e| anyhow::anyhow!("{chain_id}: {e}"))?;
    let Some(reg) = pallets.registry else {
        return Ok(());
    };

    // THE GUESS, CHECKED. `orml::ORML_NATIVE_CURRENCY_ID` is a constant in a
    // PURE mapper that has no metadata to read, and this is the one place that
    // can afford to verify it. A runtime whose own declaration disagrees would
    // make the mapper refuse the wrong asset and double-count the right one, so
    // the sync REFUSES rather than proceeding with a mapper it knows is wrong.
    let declared = orml::native_currency_id_from_metadata(metadata)
        .map_err(|e| anyhow::anyhow!("{chain_id}: {e}"))?;
    match declared {
        Some(n) if n != orml::ORML_NATIVE_CURRENCY_ID => anyhow::bail!(
            "{chain_id} declares its native currency id as {n}, but the orml \
             mapper's guard is {} — mapping this chain would record its native \
             token twice under `{}:{}` while refusing an asset that is not \
             native. Fix ORML_NATIVE_CURRENCY_ID (and its measurement) before \
             indexing this chain",
            orml::ORML_NATIVE_CURRENCY_ID,
            orml::TOKENS_KEY_PREFIX,
            orml::ORML_NATIVE_CURRENCY_ID
        ),
        // The runtime declares neither `GetNativeCurrencyId` nor
        // `NativeAssetId`. We do NOT then assume 0 — the whole point of the
        // check is to stop assuming — but nor is an unverified guard a reason
        // to refuse a chain: it is a reason to say so loudly, once, where an
        // operator will see it.
        None => tracing::warn!(
            chain = %chain_id,
            "runtime declares no native-currency-id constant — the orml \
             mapper's `tokens:{}` guard is UNVERIFIED on this chain",
            orml::ORML_NATIVE_CURRENCY_ID
        ),
        Some(_) => {}
    }

    let asset_entry = aa::storage_entry_info(metadata, &reg.storage_prefix, "Assets")
        .map_err(|e| anyhow::anyhow!("{chain_id}/{}: {e}", reg.name))?;
    let loc_entry = aa::storage_entry_info(metadata, &reg.storage_prefix, "AssetLocations")
        .map_err(|e| anyhow::anyhow!("{chain_id}/{}: {e}", reg.name))?;

    // 1. enumerate every registered asset id, straight out of the storage keys
    let prefix = aa::map_prefix(&reg.storage_prefix, "Assets");
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
                Err(e) => {
                    tracing::warn!(chain = %chain_id, pallet = %reg.name, error = %e,
                        "AssetRegistry key not decomposable — asset skipped");
                    report.undecodable_ids += 1;
                }
            }
        }
        if short {
            break;
        }
    }

    // 2. read AssetDetails + AssetNativeLocation for all of them, batched
    let mut count = 0usize;
    for chunk in ids.chunks(PAGE as usize / 2) {
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(chunk.len() * 2);
        for id in chunk {
            keys.push(
                aa::asset_map_key(&reg.storage_prefix, "Assets", &asset_entry.hashers, id)
                    .map_err(|e| anyhow::anyhow!(e))?,
            );
            keys.push(
                aa::asset_map_key(
                    &reg.storage_prefix,
                    "AssetLocations",
                    &loc_entry.hashers,
                    id,
                )
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
            let Some(details_bytes) = values.get(&keys[i * 2]) else {
                // the key was listed a moment ago at this same block hash;
                // absent now cannot happen, and an empty default would invent
                // an asset with no name rather than report the impossibility
                anyhow::bail!(
                    "{chain_id}/{}: asset id {} was enumerated but has no value \
                     at the same block hash",
                    reg.name,
                    hex::encode(id)
                );
            };
            let asset = orml::decode_registry_asset(&asset_entry, details_bytes)
                .map_err(|e| anyhow::anyhow!("{chain_id}/{}: {e}", reg.name))?;

            let id_json = aa::decode_asset_id_with(&asset_entry, id)
                .map_err(|e| anyhow::anyhow!("{chain_id}/{}: {e}", reg.name))?;
            // through the mapper's OWN reader, so the sync and the mapper can
            // never disagree about what number a currency id is
            let Some(local_id) = orml::currency_id_from_json(&id_json) else {
                report.undecodable_ids += 1;
                continue;
            };

            // THE NATIVE ALIAS. On an orml chain the chain's own token is ALSO a
            // registry entry (HDX is asset 0), and giving it a `tokens:0` row
            // beside the `native` one would be the same money twice under two
            // keys — the mapper refuses the event side for exactly this reason.
            // Its METADATA is better than `system_properties`' though (a name,
            // an existential deposit), so it is folded ONTO the native row
            // rather than thrown away.
            if local_id == orml::ORML_NATIVE_CURRENCY_ID {
                report.native_alias_skipped += 1;
                // **`observed_height` IS DELIBERATELY None HERE, and that is the
                // whole design of this write.** With a height, every
                // `case when excluded.observed_height >= …` arm in
                // `upsert_asset` would take `excluded` — and `name`, `symbol`
                // and `decimals` are all `Option` on a registry asset and
                // genuinely absent on real ones, so this would ERASE the symbol
                // and decimals `system_properties` just supplied, and flip
                // `source` from `chain-properties` to `sync-assets` on a row
                // whose values did not come from here. With None, the guard
                // fails and only the plainly-coalesced columns move — which is
                // exactly the one fact this write has that the native read did
                // not: `asset_type`.
                upsert_asset(
                    pool,
                    chain_id,
                    "native",
                    "native",
                    None,
                    None,
                    None,
                    None,
                    None,
                    asset.asset_type.as_deref(),
                    None,
                    &Default::default(),
                    &Default::default(),
                    Some(spec),
                    None,
                    "sync-assets",
                )
                .await?;
                continue;
            }

            let asset_key = orml::asset_key_for_currency(local_id);

            // A MISSING LOCATION IS A FACT, NOT A GAP — see `decode_asset_location`.
            let location = match values.get(&keys[i * 2 + 1]) {
                Some(bytes) => Some(
                    orml::decode_asset_location(&loc_entry, bytes)
                        .map_err(|e| anyhow::anyhow!("{chain_id}/{}: {e}", reg.name))?,
                ),
                None => None,
            };
            let location_key = location.as_ref().and_then(aa::canonical_location);
            let absolute = location
                .as_ref()
                .and_then(|l| orml::absolutize(observer_path, l));
            // through the named producer, not `.to_string()` — see the identical
            // note on the pallet-assets arm
            let absolute_key = location
                .as_ref()
                .and_then(|l| orml::absolute_key(observer_path, l));
            if absolute.is_some() {
                report.absolutized += 1;
            }
            if asset.asset_type.as_deref() == Some("Erc20") {
                report.erc20_unanchorable += 1;
            }

            upsert_asset(
                pool,
                chain_id,
                &asset_key,
                // dotlens vocabulary for "an orml-tokens balance", parallel to
                // trust_backed/pool/foreign — NOT one of those, because the
                // storage, the key order and the free/reserved split all differ
                "orml",
                Some(&local_id.to_string()),
                location.as_ref(),
                location_key.as_deref(),
                absolute.as_ref(),
                absolute_key.as_deref(),
                asset.asset_type.as_deref(),
                Some(id),
                &aa::AssetMeta {
                    name: asset.name.clone(),
                    symbol: asset.symbol.clone(),
                    decimals: asset.decimals,
                },
                &aa::AssetDetailsView {
                    min_balance: asset.existential_deposit,
                    is_sufficient: asset.is_sufficient,
                    ..Default::default()
                },
                Some(spec),
                Some(height),
                "sync-assets",
            )
            .await?;
            count += 1;
        }
    }
    report
        .per_instance
        .push((orml::TOKENS_KEY_PREFIX.into(), count));
    Ok(())
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
    /// their storage key, OR because no indexed pallet owns their key prefix.
    /// Counted, because "we did not look" and "there is nothing there" must
    /// never be the same number.
    pub skipped_no_key: usize,
    /// Assets this sweep DELIBERATELY did not probe because their balance is not
    /// in any pallet it can read — today, the `Erc20` assets of an ORML chain's
    /// registry, whose balances live in `pallet_evm` storage.
    ///
    /// **THIS COUNTER IS THE FIX FOR A REAL DEFECT, not bookkeeping.** Probing
    /// them anyway returns nothing, and nothing was recorded as a ZERO anchor
    /// noted `absent` — so the treasury's ~5.7M DOT money-market position would
    /// have appeared in the data as a confident zero. A skipped probe that says
    /// so is the honest answer; an anchor of 0 is a wrong one.
    pub skipped_unanchorable: usize,
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
    cfg: &registry::ChainConfig,
    height: Option<u64>,
) -> Result<HoldingsReport> {
    use adapter_substrate::{accounts as acct, assets as aa, balances as ab, orml};
    use ingest::live::ChainSource;

    let chain_id = cfg.id.as_str();

    let height = match height {
        Some(h) => h,
        None => source
            .finalized_height()
            .await
            .map_err(|e| anyhow::anyhow!(e))?,
    };
    let hash = source
        .block_hash(height)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
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
    // The ORML side, resolved once for the same reason — and it is a SEPARATE
    // entry because the map is `Accounts` (plural) with the key halves in the
    // opposite order and a value carrying a free/reserved split. Nothing about
    // it can share a code path with pallet-assets without one of the two being
    // silently wrong.
    let orml_pallets = orml::orml_pallets_from_metadata(&metadata)
        .map_err(|e| anyhow::anyhow!("{chain_id}: {e}"))?;
    let orml_entry = match &orml_pallets.tokens {
        Some(t) => Some((
            t.storage_prefix.clone(),
            aa::storage_entry_info(&metadata, &t.storage_prefix, "Accounts")
                .map_err(|e| anyhow::anyhow!("{chain_id}/{}: {e}", t.name))?,
        )),
        None => None,
    };

    let mut report = HoldingsReport {
        chain_id: chain_id.to_string(),
        height,
        spec_version: spec,
        accounts: accounts.len(),
        ..Default::default()
    };

    // build the whole key list first: (account, asset) → key, plus one native
    // System.Account key per account
    /// WHICH READER decodes the value that comes back, and therefore which
    /// anchor writer records it. Three kinds rather than an `Option<prefix>`,
    /// because the third one is not a variation on the second: an orml holding
    /// has a free/reserved split and takes the NATIVE anchor writer, while a
    /// pallet-assets holding has one number and takes the asset one (0010's
    /// column mapping). Collapsing them would write `reserved = 0` over a real
    /// reserved position — the exact silent-loss shape this slice exists to end.
    enum Reader {
        Native,
        PalletAssets(String),
        Orml(String),
    }
    struct Probe {
        account: Vec<u8>,
        asset_key: String,
        reader: Reader,
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
            reader: Reader::Native,
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
            // AN ASSET WHOSE BALANCE IS NOT IN A PALLET WE READ IS SKIPPED,
            // LOUDLY-BY-COUNT, RATHER THAN PROBED AND ANCHORED AT ZERO. An
            // `Erc20` registry asset's balance is in `pallet_evm` storage; the
            // orml `Accounts` probe for it returns nothing, and nothing was
            // being written as a zero anchor noted `absent` — which would put
            // the treasury's money-market position into the data as a confident
            // zero. Keyed on the asset's OWN declared type, so it needs no chain
            // id and no list.
            if asset.asset_type.as_deref() == Some("Erc20") {
                report.skipped_unanchorable += 1;
                continue;
            }
            let Some(raw_id) = asset.raw_key_bytes.as_ref() else {
                report.skipped_no_key += 1;
                continue;
            };

            // THE ORML ARM, tried first because its key prefix (`tokens:`) is
            // this adapter's own word and cannot collide with a pallet-assets
            // instance's, which is taken from the runtime's pallet name.
            if let Some((prefix, entry)) = orml_entry.as_ref() {
                if asset
                    .asset_key
                    .starts_with(&format!("{}:", adapter_substrate::orml::TOKENS_KEY_PREFIX))
                {
                    let key = orml::accounts_key(prefix, &entry.hashers, &account32, raw_id)
                        .map_err(|e| anyhow::anyhow!(e))?;
                    // THE ORDER CHECK, run once per key and free: lift the two
                    // halves back out and require the account half to be the
                    // account we put in. orml keys `(account, currency)` where
                    // pallet-assets keys `(asset, account)`, and building it the
                    // wrong way round yields a well-formed key that matches
                    // nothing — which reads exactly like an empty account, i.e.
                    // like an entire chain's treasury position being zero.
                    match orml::accounts_key_parts(&key, &entry.hashers) {
                        Ok((back, _)) if back == account32 => {}
                        Ok((back, _)) => anyhow::bail!(
                            "{chain_id}: orml Accounts key does not round-trip — \
                             built for {} but lifts {}; the key halves are in \
                             the wrong order and every holding would read zero",
                            hex::encode(account32),
                            hex::encode(back)
                        ),
                        Err(e) => anyhow::bail!("{chain_id}: orml Accounts key: {e}"),
                    }
                    keys.push(key);
                    probes.push(Probe {
                        account: a.account_id.clone(),
                        asset_key: asset.asset_key.clone(),
                        reader: Reader::Orml(prefix.clone()),
                    });
                    continue;
                }
            }

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
                reader: Reader::PalletAssets(pallet.storage_prefix.clone()),
            });
        }
    }
    report.assets_probed = probes
        .iter()
        .filter(|p| !matches!(p.reader, Reader::Native))
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
        match &probe.reader {
            Reader::Native => {
                let (balances, note) = match value {
                    Some(bytes) => (
                        ab::decode_account_info(&metadata, &bytes)
                            .map_err(|e| anyhow::anyhow!(e))?,
                        None,
                    ),
                    None => (
                        ab::AccountBalances {
                            free: 0,
                            reserved: 0,
                            frozen: None,
                        },
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
            Reader::PalletAssets(prefix) => {
                let (holding, note) = match value {
                    Some(bytes) => (
                        aa::decode_asset_account_with(&account_entries[prefix], &bytes)
                            .map_err(|e| anyhow::anyhow!(e))?,
                        None,
                    ),
                    None => (
                        aa::AssetHolding {
                            balance: 0,
                            status: None,
                        },
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
            // THE NATIVE WRITER, on an ASSET key — which looks like a mistake
            // and is the correction. `orml_tokens::AccountData { free, reserved,
            // frozen }` is the pallet-balances shape, so an orml position can
            // have a real reserved half; `insert_asset_anchor` writes
            // `reserved = 0` by design (0010: a pallet-assets account genuinely
            // has one number) and would drop it silently. `status` stays NULL,
            // as it does for native anchors: orml has no per-account asset
            // status, and a plausible default is how a schema starts lying.
            Reader::Orml(prefix) => {
                let entry = &orml_entry
                    .as_ref()
                    .expect("an Orml probe implies an orml entry")
                    .1;
                debug_assert_eq!(
                    prefix,
                    &orml_entry.as_ref().expect("checked above").0,
                    "one orml tokens pallet per chain"
                );
                let (holding, note) = match value {
                    Some(bytes) => (
                        orml::decode_orml_account(entry, &bytes).map_err(|e| anyhow::anyhow!(e))?,
                        None,
                    ),
                    None => (
                        orml::OrmlHolding {
                            free: 0,
                            reserved: 0,
                            frozen: None,
                        },
                        Some("absent"),
                    ),
                };
                if holding.total() > 0 {
                    report.non_zero += 1;
                }
                crate::balances_pg::insert_anchor(
                    pool,
                    chain_id,
                    &probe.account,
                    &probe.asset_key,
                    height,
                    &holding.as_account_balances(),
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
                // A CHAIN HAVING A TREASURY PALLET DOES NOT MAKE ITS TREASURY
                // OURS — and this guard is what Phase 3 slice 6 had to add
                // before registering the first chain with its own governance.
                // Hydration runs `pallet_treasury` with the SAME PalletId as
                // the relay (`py/trsry`), so without it that pot is derived,
                // stamped `network = polkadot`, and served on
                // `/v1/treasury/polkadot/holdings` as Polkadot treasury money.
                // A wrong number, not a missing one — and the worst kind,
                // because it reads like a fact and sums like a fact.
                //
                // The predicate is registry data end to end (residency for a
                // registered treasury instance's domain), so no chain is named
                // here, and registering Hydration's OWN treasury later is a seed
                // edit rather than a code change.
                if !registry.carries_treasury_for_network(&chain.id, &chain.network) {
                    tracing::debug!(chain = %chain.id, network = %chain.network,
                        "no treasury residency on this network — pot derivation \
                         skipped (this chain's own treasury is not ours)");
                    continue;
                }
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
                anyhow::anyhow!(
                    "seed account '{}' on {} is invalid: {e}",
                    seed.address,
                    chain.id
                )
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

// Wire form for one query, converted into `TreasuryAccountRow` below. See
// `labels.rs`.
#[allow(clippy::type_complexity)]
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
