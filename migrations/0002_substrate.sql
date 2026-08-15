-- 0002_substrate: Substrate-family domain schema (runtime/metadata lineage).
-- Generic core stays family-agnostic; Substrate specifics live here (Invariant 5).

create schema if not exists substrate;

-- one row per (chain, spec_version) era; metadata blob lives in the raw store
create table substrate.runtime_versions (
    chain_id               text not null references core.chains (id),
    spec_version           bigint not null,
    transaction_version    bigint,
    metadata_version       integer,             -- 14 | 15 | 16
    metadata_hash          text,
    metadata_blob_location text,                -- raw-store key (keys::metadata)
    first_block            bigint,
    last_block             bigint,              -- null = current era
    first_seen_at          timestamptz not null default now(),
    primary key (chain_id, spec_version)
);

-- deprecation info surfaced by metadata v16 — UI badge data, not logic
create table substrate.deprecated_items (
    chain_id      text not null,
    spec_version  bigint not null,
    item_kind     text not null,                -- pallet|call|event|error|constant
    item_path     text not null,                -- e.g. 'treasury.propose_spend'
    note          text,
    primary key (chain_id, spec_version, item_kind, item_path),
    foreign key (chain_id, spec_version)
        references substrate.runtime_versions (chain_id, spec_version)
);
