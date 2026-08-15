-- 0005_balances: the balances domain schema (Phase 1, slice 5).
--
-- balance_changes: per-account TOTAL-balance deltas (free + reserved) derived
-- from canonical events by the adapter's mapper — rebuildable from core.events
-- (which rebuild from raw), lineage via runtime_version + mapper_version.
-- Reserving/freezing moves within an account don't change total → no row.
--
-- balance_anchors: absolute balances read from on-chain state
-- (System.Account) at a specific block, decoded against block-correct
-- metadata. History reconstruction = nearest anchor + sum of later deltas.
-- Anchors are how histories stay correct across events we can't see (e.g.
-- the Nov 2025 relay→AH migration's bulk account moves).

create schema if not exists balances;

create table balances.balance_changes (
    chain_id        text not null,
    account_id      bytea not null,
    asset           text not null default 'native',
    block_height    bigint not null,
    event_index     integer not null,
    delta           numeric not null,           -- signed, plancks
    reason          text not null,              -- transfer_in|transfer_out|deposit|
                                                -- withdraw|minted|burned|slashed|
                                                -- dust_lost|reserve_repatriated_*
    counterparty    bytea,                      -- transfer peer, if any
    runtime_version bigint not null,            -- lineage
    mapper_version  integer not null,           -- lineage (bump = rebuild)
    -- asset in the PK: one event may someday credit one account in TWO assets
    -- (AssetConversion swaps) — cheap now, migration-pain later (review catch)
    primary key (chain_id, block_height, event_index, account_id, asset)
) partition by list (chain_id);

create table balances.balance_changes_default
    partition of balances.balance_changes default;

create index balance_changes_account_idx
    on balances.balance_changes (chain_id, account_id, block_height);

create table balances.balance_anchors (
    chain_id     text not null,
    account_id   bytea not null,
    asset        text not null default 'native',
    block_height bigint not null,               -- state AT END of this block
    free         numeric not null,
    reserved     numeric not null,
    frozen       numeric,
    total        numeric not null,              -- free + reserved
    spec_version bigint,                        -- lineage: decoded against this
    source       text not null,                 -- endpoint url or 'test'
    note         text,                          -- e.g. 'absent' (no account row)
    captured_at  timestamptz not null default now(),
    primary key (chain_id, account_id, asset, block_height)
);
