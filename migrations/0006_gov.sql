-- 0006_gov: the governance domain schema (Phase 2, slice 1).
--
-- gov.referendum_events: the append-only STATUS TIMELINE — one row per
-- referenda-pallet event, mapped from canonical events by the adapter's gov
-- mapper. Rebuildable from core.events (which rebuild from raw); lineage via
-- runtime_version + mapper_version. `class` is the referenda INSTANCE the
-- event came from ('referenda' public OpenGov, 'fellowship_referenda' on
-- Collectives) — instances are data, never code branches (Invariant 2).
--
-- gov.referenda: a per-(chain, class, referendum) PROJECTION of the latest
-- state, maintained idempotently by the sink with an ordering guard
-- (status_height, status_event_index) so replaying any range in any order
-- converges. Rebuildable: truncate + gov-range re-derives it exactly.
-- Referendum numbering is continuous across the Nov 2025 relay→AH migration;
-- one referendum may have rows on BOTH chains (submitted on relay, concluded
-- on AH) — the API stitches them via governance domain residency.
--
-- gov.tracks: track definitions decoded from each runtime's OWN metadata
-- (the `Tracks` constant of every referenda-instance pallet) — generated,
-- never hand-curated, same doctrine as pallet-account labels. spec_version is
-- lineage: the runtime version whose metadata defined these parameters.

create schema if not exists gov;

create table gov.referendum_events (
    chain_id        text not null,
    class           text not null,              -- referenda instance
    referendum_id   bigint not null,
    block_height    bigint not null,
    event_index     integer not null,
    kind            text not null,              -- submitted|decision_deposit_placed|
                                                -- deciding|confirm_started|…|killed
    data            jsonb not null,             -- full event fields, schema-on-read
    runtime_version bigint not null,            -- lineage
    mapper_version  integer not null,           -- lineage (bump = rebuild)
    primary key (chain_id, class, referendum_id, block_height, event_index)
) partition by list (chain_id);

create table gov.referendum_events_default
    partition of gov.referendum_events default;

-- no secondary index: the PK (chain_id, class, referendum_id, block_height,
-- event_index) already serves the timeline query as a prefix (review catch —
-- a separate index would be pure write amplification on an append-only table)

create table gov.referenda (
    chain_id           text not null,
    class              text not null,
    referendum_id      bigint not null,
    track_id           integer,                 -- from Submitted/DecisionStarted
    origin             text,                    -- filled by a later slice (storage read)
    proposal           jsonb,                   -- Bounded<Call>: Lookup{hash,len}|Inline|Legacy
    proposal_hash      text,                    -- 0x… when known (Lookup/Legacy)
    proposal_len       bigint,
    submitted_at_height bigint,                 -- height of the Submitted event, if seen
    -- 'unknown' = a non-status event arrived before any status-bearing one
    -- (guarded at (0,0) so any real status wins)
    status             text not null,
    status_height      bigint not null,
    status_event_index integer not null,
    runtime_version    bigint not null,         -- lineage of the last applied event
    mapper_version     integer not null,
    updated_at         timestamptz not null default now(),
    primary key (chain_id, class, referendum_id)
);

create index referenda_status_idx on gov.referenda (chain_id, class, status);

create table gov.tracks (
    chain_id     text not null,
    pallet       text not null,                 -- lowercased instance pallet, e.g. 'referenda'
    track_id     integer not null,
    name         text not null,
    params       jsonb not null,                -- max_deciding, deposits, periods, curves
    spec_version bigint not null,               -- lineage: decoded from this runtime's metadata
    updated_at   timestamptz not null default now(),
    primary key (chain_id, pallet, track_id)
);
