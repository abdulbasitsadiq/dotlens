//! Account-labeling engine (Phase 1, ROADMAP): derive + label every system
//! account for every registered chain, from DATA only (Invariant 2):
//!
//!   modl  — pallet accounts, discovered from each chain's own archived
//!           metadata (the runtime declares its PalletId constants; we never
//!           hand-maintain a pallet list)
//!   para  — parachain sovereigns, labeled ON THE RELAY, from registry para_ids
//!   sibl  — sibling sovereigns, labeled on every OTHER parachain of the same
//!           relay (appear automatically when a chain is registered — the
//!           Phase 2 plug-and-play test will show this with zero code changes)
//!
//! Plus registry-seeded well-known accounts (location-derived treasury
//! accounts etc. — things that CANNOT be derived).
//!
//! Sync is idempotent upsert-only: labels are registry data projected to
//! `core.account_labels`; nothing is deleted (historic labels stay valuable).
//! On-chain verification is a separate explicit step (verify-labels) that
//! records System.Account existence per label — see main.rs.

use adapter_substrate::accounts as acct;
use adapter_substrate::frame_decoder::ss58_encode;
use anyhow::{Context, Result};
use raw_store::RawStore;
use registry::{ChainConfig, ChainFamily, Registry};
use sqlx::{PgPool, Postgres, Transaction};

#[derive(Debug, Default)]
pub struct LabelSyncReport {
    pub sovereign_labels: usize,
    pub pallet_labels: usize,
    pub seeded_labels: usize,
    /// Chains with no archived metadata yet (no runtime_versions row) — their
    /// pallet accounts will appear on the next sync after live ingestion runs.
    pub chains_missing_metadata: Vec<String>,
}

fn is_substrate(c: &ChainConfig) -> bool {
    c.family == ChainFamily::Substrate
}

fn prefix_of(c: &ChainConfig) -> u16 {
    c.ss58_prefix.unwrap_or(42)
}

#[allow(clippy::too_many_arguments)]
async fn upsert_label(
    tx: &mut Transaction<'_, Postgres>,
    account_id: &[u8; 32],
    chain_id: &str,
    kind: &str,
    label: &str,
    derivation: Option<&str>,
    source: &str,
    ss58: &str,
) -> Result<()> {
    sqlx::query(
        "insert into core.account_labels \
             (account_id, chain_id, kind, label, derivation, source, ss58) \
         values ($1, $2, $3, $4, $5, $6, $7) \
         on conflict (account_id, kind, chain_scope) do update set \
             label = excluded.label, derivation = excluded.derivation, \
             source = excluded.source, ss58 = excluded.ss58",
    )
    .bind(&account_id[..])
    .bind(chain_id)
    .bind(kind)
    .bind(label)
    .bind(derivation)
    .bind(source)
    .bind(ss58)
    .execute(&mut **tx)
    .await
    .with_context(|| format!("upserting label '{label}' on {chain_id}"))?;
    Ok(())
}

/// Derive + project all labels. Requires registry_sync to have run (FKs on
/// core.chains are not involved, but runtime_versions reads assume synced ids).
pub async fn sync_labels(
    pool: &PgPool,
    registry: &Registry,
    raw: &dyn RawStore,
) -> Result<LabelSyncReport> {
    let mut report = LabelSyncReport::default();
    let mut tx = pool.begin().await.context("begin label sync")?;

    // ---- sovereigns: pure registry data --------------------------------
    for chain in registry.chains().filter(|c| is_substrate(c)) {
        let (Some(para_id), Some(relay_id)) = (chain.para_id, chain.relay.as_deref()) else {
            continue;
        };
        let relay = registry.chain(relay_id).expect("validated at load");

        // para sovereign — an account ON THE RELAY
        let para = acct::para_sovereign(para_id);
        upsert_label(
            &mut tx,
            &para,
            &relay.id,
            "para_sovereign",
            &format!("{} sovereign", chain.name),
            Some(&format!("para:{para_id}")),
            "derived",
            &ss58_encode(prefix_of(relay), &para),
        )
        .await?;
        report.sovereign_labels += 1;

        // sibling sovereign — the same account bytes on every sibling parachain
        let sibl = acct::sibling_sovereign(para_id);
        for sib in registry.chains().filter(|s| {
            is_substrate(s)
                && s.id != chain.id
                && s.para_id.is_some()
                && s.relay.as_deref() == Some(relay_id)
        }) {
            upsert_label(
                &mut tx,
                &sibl,
                &sib.id,
                "sibl_sovereign",
                &format!("{} sovereign (sibling)", chain.name),
                Some(&format!("sibl:{para_id}")),
                "derived",
                &ss58_encode(prefix_of(sib), &sibl),
            )
            .await?;
            report.sovereign_labels += 1;
        }
    }

    // ---- pallet accounts: from each chain's own archived metadata ------
    for chain in registry.chains().filter(|c| is_substrate(c)) {
        let blob_loc: Option<(String,)> = sqlx::query_as(
            "select metadata_blob_location from substrate.runtime_versions \
             where chain_id = $1 and metadata_blob_location is not null \
             order by spec_version desc limit 1",
        )
        .bind(&chain.id)
        .fetch_optional(&mut *tx)
        .await
        .with_context(|| format!("looking up metadata for {}", chain.id))?;
        let Some((location,)) = blob_loc else {
            report.chains_missing_metadata.push(chain.id.clone());
            continue;
        };
        let blob = match raw.get(&location) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(chain = %chain.id, %location, error = %e,
                    "metadata blob listed but unreadable — skipping pallet labels");
                report.chains_missing_metadata.push(chain.id.clone());
                continue;
            }
        };
        // a walk failure on ONE chain's metadata must not block the node (or
        // the other chains' labels) — record and move on; seeds below still
        // fail hard because those are reviewed data
        let pallet_ids = match acct::pallet_ids_from_metadata(&blob) {
            Ok(ids) => ids,
            Err(e) => {
                tracing::warn!(chain = %chain.id, error = %e,
                    "PalletId walk failed — skipping pallet labels for this chain");
                report.chains_missing_metadata.push(chain.id.clone());
                continue;
            }
        };
        // group by id: two pallets sharing one PalletId (runtime's choice, not
        // ours) must merge into one label, never silently last-writer-win
        let mut by_id: std::collections::BTreeMap<[u8; 8], Vec<String>> =
            std::collections::BTreeMap::new();
        for pc in pallet_ids {
            by_id.entry(pc.id).or_default().push(pc.pallet);
        }
        for (id, pallets) in &by_id {
            if pallets.len() > 1 {
                tracing::warn!(chain = %chain.id, id = %acct::ascii_pallet_id(id),
                    pallets = ?pallets, "multiple pallets share one PalletId — merging label");
            }
            let account = acct::pallet_account(id);
            let label = format!("{} ({})", pallets.join("+"), acct::ascii_pallet_id(id));
            upsert_label(
                &mut tx,
                &account,
                &chain.id,
                "pallet",
                &label,
                Some(&format!("modl:{}", acct::ascii_pallet_id(id))),
                "derived",
                &ss58_encode(prefix_of(chain), &account),
            )
            .await?;
            report.pallet_labels += 1;
        }
    }

    // ---- registry-seeded well-known accounts ---------------------------
    // family-filtered: parse_account/ss58 are Substrate-specific; a future
    // EVM/JAM chain's seeds need their own adapter's parser (review catch —
    // without this, one non-Substrate seed would brick every node start)
    for chain in registry.chains().filter(|c| is_substrate(c)) {
        for seed in &chain.accounts {
            // checksum-validated HERE (fails loudly: seeds are reviewed data,
            // a bad address is a bug, never something to skip)
            let account = acct::parse_account(&seed.address).map_err(|e| {
                anyhow::anyhow!(
                    "seed account '{}' on {} is invalid: {e}",
                    seed.address,
                    chain.id
                )
            })?;
            upsert_label(
                &mut tx,
                &account,
                &chain.id,
                &seed.kind,
                &seed.label,
                None,
                "registry",
                &ss58_encode(prefix_of(chain), &account),
            )
            .await?;
            report.seeded_labels += 1;
        }
    }

    tx.commit().await.context("commit label sync")?;
    Ok(report)
}

/// One labeled account scoped to a chain — the verify-labels work list.
#[derive(Debug, Clone)]
pub struct LabelRow {
    pub account_id: [u8; 32],
    pub kind: String,
    pub chain_scope: String,
    pub label: String,
    pub ss58: Option<String>,
}

/// Labels applying on `chain_id` (chain-scoped or '*').
pub async fn labels_for_chain(pool: &PgPool, chain_id: &str) -> Result<Vec<LabelRow>> {
    let rows: Vec<(Vec<u8>, String, String, String, Option<String>)> = sqlx::query_as(
        "select account_id, kind, chain_scope, label, ss58 from core.account_labels \
         where chain_scope = $1 or chain_scope = '*' order by kind, label",
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await
    .context("listing labels")?;
    rows.into_iter()
        .map(|(id, kind, chain_scope, label, ss58)| {
            let account_id = <[u8; 32]>::try_from(id.as_slice())
                .map_err(|_| anyhow::anyhow!("label '{label}': account_id is not 32 bytes"))?;
            Ok(LabelRow {
                account_id,
                kind,
                chain_scope,
                label,
                ss58,
            })
        })
        .collect()
}

/// Record the outcome of an on-chain existence probe for one label.
pub async fn record_verification(
    pool: &PgPool,
    row: &LabelRow,
    height: u64,
    exists: bool,
) -> Result<()> {
    sqlx::query(
        "update core.account_labels \
         set verified_at = now(), verified_block = $1, verified_note = $2 \
         where account_id = $3 and kind = $4 and chain_scope = $5",
    )
    .bind(height as i64)
    .bind(if exists { "exists" } else { "absent" })
    .bind(&row.account_id[..])
    .bind(&row.kind)
    .bind(&row.chain_scope)
    .execute(pool)
    .await
    .with_context(|| format!("recording verification for '{}'", row.label))?;
    Ok(())
}
