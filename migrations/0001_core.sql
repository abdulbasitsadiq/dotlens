-- 0001_core: the generic `core` schema (ARCHITECTURE.md §8).
-- Conventions: append-only where possible; NUMERIC for anything monetary;
-- every decoded table carries lineage; partition-by-chain from day one.

create schema if not exists core;

-- ---------------------------------------------------------------- registry
-- Registry seeds (YAML) are the source of truth; these tables are the DB
-- projection (populated by dotlens-node registry sync — lands in Phase 1).

create table core.chains (
    id            text primary key,
    name          text not null,
    family        text not null,               -- substrate | evm | jam
    relay_id      text references core.chains (id),
    para_id       integer,
    network       text not null,
    ss58_prefix   integer,
    created_at    timestamptz not null default now()
);

-- lifecycle is an EVENT LOG (chains leave; never a mutable status column)
create table core.chain_lifecycle (
    chain_id       text not null references core.chains (id),
    status         text not null,              -- live|on_demand|winding_down|migrated|dead
    from_ts        timestamptz not null,
    migration_dest text,
    note           text,
    primary key (chain_id, from_ts)
);

-- which chain hosts which functional domain, over time (ARCHITECTURE.md §4)
create table core.domain_residency (
    domain    text not null,
    network   text not null,
    chain_id  text not null references core.chains (id),
    from_ts   timestamptz not null,
    to_ts     timestamptz,
    primary key (domain, network, from_ts)
);

-- ---------------------------------------------------------------- ledger data
-- Partitioned by chain from day one. Per-chain partitions are created when a
-- chain is registered; the DEFAULT partition catches anything else so writes
-- never fail on a missing partition.

create table core.blocks (
    chain_id         text not null,
    height           bigint not null,
    hash             text not null,
    parent_hash      text not null,
    timestamp        timestamptz,
    finalized        boolean not null default false,
    runtime_version  bigint not null,           -- lineage
    decoder_version  integer not null,          -- lineage
    raw_location     text not null,             -- lineage
    ingested_at      timestamptz not null default now(),
    primary key (chain_id, height)
) partition by list (chain_id);

create table core.blocks_default partition of core.blocks default;

create table core.transactions (
    chain_id         text not null,
    block_height     bigint not null,
    tx_index         integer not null,
    hash             text,
    signer           text,
    call_name        text not null,             -- "pallet.call", namespaced
    args             jsonb not null default '{}'::jsonb,
    success          boolean not null,
    runtime_version  bigint not null,
    decoder_version  integer not null,
    raw_location     text not null,
    primary key (chain_id, block_height, tx_index)
) partition by list (chain_id);

create table core.transactions_default partition of core.transactions default;

create table core.events (
    chain_id         text not null,
    block_height     bigint not null,
    event_index      integer not null,
    tx_index         integer,                   -- null = not attributable
    name             text not null,             -- "pallet.Event", namespaced
    data             jsonb not null default '{}'::jsonb,
    runtime_version  bigint not null,
    decoder_version  integer not null,
    primary key (chain_id, block_height, event_index)
) partition by list (chain_id);

create table core.events_default partition of core.events default;

create index events_name_idx on core.events (chain_id, name, block_height);
create index transactions_signer_idx on core.transactions (chain_id, signer, block_height);

-- ---------------------------------------------------------------- accounts

create table core.accounts (
    chain_id          text not null,
    account_id        bytea not null,           -- 32-byte public key (or family analog)
    ss58              text,
    first_seen_block  bigint,
    last_active_block bigint,
    primary key (chain_id, account_id)
);

-- labels are mostly DERIVED (modl/para/sibl derivations) — `source` says how.
-- `chain_scope` is a generated column so (account, kind, chain) stays unique
-- even when chain_id is null ('*' = applies on every chain).
create table core.account_labels (
    account_id  bytea not null,
    chain_id    text,                            -- null = label applies everywhere
    chain_scope text generated always as (coalesce(chain_id, '*')) stored,
    kind        text not null,                   -- pallet|para_sovereign|sibl_sovereign|
                                                 -- treasury|bounty|multisig|proxy|user_tagged
    label       text not null,
    derivation  text,                            -- e.g. 'modl:py/trsry', 'para:1000'
    source      text not null,                   -- derived|registry|user
    created_at  timestamptz not null default now(),
    primary key (account_id, kind, chain_scope)
);

-- ---------------------------------------------------------------- checkpoints

create table core.indexer_state (
    chain_id     text not null,
    module       text not null,
    last_height  bigint not null,
    last_hash    text not null,
    updated_at   timestamptz not null default now(),
    primary key (chain_id, module)
);

-- ---------------------------------------------------------------- raw receipts

create table core.ingest_receipts (
    key         text primary key,
    byte_len    bigint not null,
    source      text not null,
    fetched_at  timestamptz not null
);
