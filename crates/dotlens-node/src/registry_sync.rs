//! Registry → DB sync: project YAML seeds into the `core` registry tables.
//!
//! The YAML seeds remain the source of truth (Invariant 2); these tables are a
//! queryable projection for FKs (`substrate.runtime_versions` → `core.chains`)
//! and future API joins. Sync is idempotent — run at every node start.
//!
//! Also creates the per-chain list partitions for `core.blocks/transactions/
//! events` ("created when a chain is registered", migration 0001). If a
//! partition can't be created because rows for that chain already landed in the
//! DEFAULT partition, we log and continue: queries through the parent table
//! stay correct either way.

use anyhow::{Context, Result};
use registry::{ChainConfig, ChainFamily, Registry};
use sqlx::PgPool;

fn family_str(f: ChainFamily) -> &'static str {
    match f {
        ChainFamily::Substrate => "substrate",
        ChainFamily::Evm => "evm",
        ChainFamily::Jam => "jam",
    }
}

fn lifecycle_status_str(s: registry::LifecycleStatus) -> &'static str {
    match s {
        registry::LifecycleStatus::Live => "live",
        registry::LifecycleStatus::OnDemand => "on_demand",
        registry::LifecycleStatus::WindingDown => "winding_down",
        registry::LifecycleStatus::Migrated => "migrated",
        registry::LifecycleStatus::Dead => "dead",
    }
}

/// Chain ids are also used to name partitions — enforce the safe alphabet
/// here so an id can never smuggle SQL into DDL (identifiers can't be bound).
fn partition_safe(id: &str) -> bool {
    !id.is_empty()
        && id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

pub async fn sync_registry(pool: &PgPool, registry: &Registry) -> Result<()> {
    let mut tx = pool.begin().await.context("begin registry sync")?;

    // relays first so the self-referencing FK on core.chains is satisfiable
    let mut chains: Vec<&ChainConfig> = registry.chains().collect();
    chains.sort_by_key(|c| (c.relay.is_some(), c.id.clone()));

    for c in &chains {
        anyhow::ensure!(
            partition_safe(&c.id),
            "chain id '{}' is not partition-safe ([a-z0-9-] only)",
            c.id
        );
        sqlx::query(
            "insert into core.chains (id, name, family, relay_id, para_id, network, ss58_prefix) \
             values ($1, $2, $3, $4, $5, $6, $7) \
             on conflict (id) do update set \
                 name = excluded.name, family = excluded.family, \
                 relay_id = excluded.relay_id, para_id = excluded.para_id, \
                 network = excluded.network, ss58_prefix = excluded.ss58_prefix",
        )
        .bind(&c.id)
        .bind(&c.name)
        .bind(family_str(c.family))
        .bind(&c.relay)
        .bind(c.para_id.map(|p| p as i32))
        .bind(&c.network)
        .bind(c.ss58_prefix.map(|p| p as i32))
        .execute(&mut *tx)
        .await
        .with_context(|| format!("upserting chain {}", c.id))?;

        for ev in &c.lifecycle {
            sqlx::query(
                "insert into core.chain_lifecycle (chain_id, status, from_ts, migration_dest, note) \
                 values ($1, $2, $3, $4, $5) \
                 on conflict (chain_id, from_ts) do update set \
                     status = excluded.status, \
                     migration_dest = excluded.migration_dest, \
                     note = excluded.note",
            )
            .bind(&c.id)
            .bind(lifecycle_status_str(ev.status))
            .bind(ev.from)
            .bind(&ev.migration_dest)
            .bind(&ev.note)
            .execute(&mut *tx)
            .await
            .with_context(|| format!("upserting lifecycle for {}", c.id))?;
        }
    }

    for r in registry.residency() {
        sqlx::query(
            "insert into core.domain_residency (domain, network, chain_id, from_ts, to_ts) \
             values ($1, $2, $3, $4, $5) \
             on conflict (domain, network, from_ts) do update set \
                 chain_id = excluded.chain_id, to_ts = excluded.to_ts",
        )
        .bind(&r.domain)
        .bind(&r.network)
        .bind(&r.chain)
        .bind(r.from)
        .bind(r.to)
        .execute(&mut *tx)
        .await
        .with_context(|| format!("upserting residency {}/{}", r.domain, r.network))?;
    }

    tx.commit().await.context("commit registry sync")?;

    // Partition DDL outside the transaction: a failure here (rows already in
    // the default partition) must not roll back the registry rows.
    for c in &chains {
        for table in ["blocks", "transactions", "events"] {
            let part = format!("core.{table}_p_{}", c.id.replace('-', "_"));
            let ddl = format!(
                "create table if not exists {part} partition of core.{table} for values in ('{}')",
                c.id
            );
            if let Err(e) = sqlx::query(&ddl).execute(pool).await {
                tracing::warn!(
                    chain = %c.id, table, error = %e,
                    "partition not created (rows may already be in DEFAULT partition) — continuing"
                );
            }
        }
    }
    Ok(())
}
