-- 0009_treasury: treasury spends, payouts and pot flows (Phase 2, slice 5).
--
-- WHERE THE MONEY WAS PROMISED AND WHETHER IT ARRIVED. Same shape as the votes
-- slice: an append-only fact table keyed by EVENT (so a subject-less event is
-- still recorded honestly), plus an ordering-guarded projection.
--
-- TWO GENERATIONS OF SPEND COEXIST, and both are indexed (verified against
-- pallet-treasury 4.0.0 / 28.0.0 / 48.0.0 sources):
--   LEGACY proposal flow — `proposal_index` id space:
--     Proposed → SpendApproved → Awarded | Rejected
--     Present since 2020; `Proposed` and `Rejected` were REMOVED in v35, so
--     they only ever appear in relay-era history. Indexing the relay treasury
--     requires them; modern runtimes never emit them.
--   MODERN asset-spend flow — `SpendIndex` id space, disjoint from the above:
--     AssetSpendApproved → Paid → SpendProcessed | PaymentFailed | AssetSpendVoided
--     Carries an ASSET KIND (VersionedLocatableAsset — the spend may be USDT
--     on Asset Hub, not DOT) and a LOCATION beneficiary, neither of which
--     exists in the legacy flow. The beneficiary type differs BY CHAIN
--     (decoded from the committed metadata fixtures, spec 2003002): the relay
--     uses `xcm::VersionedLocation`, Asset Hub uses
--     `parachains_common::pay::VersionedLocatableAccount` = {location,
--     account_id} — two locations, because the payee may live on another
--     chain than the one paying.
-- Hence `spend_kind` + `spend_id`: two id spaces on one pallet must not
-- collide, and `(instance, spend_kind, spend_id)` is the real identity.
--
-- POT EVENTS (Spending / Burnt / Rollover / Deposit / UpdatedInactive) carry NO
-- subject at all. They are recorded with spend_kind/spend_id NULL and
-- attribution 'pot', never projected — the same honest-null rule the votes
-- slice uses for pre-poll-index events.
--
-- AND THEY ARE NOT ALL FLOWS. Read from pallet-treasury's `spend_funds()`:
-- `Deposit{value}` (inflow) and `Burnt{burnt_funds}` (outflow) are money
-- MOVING, but `Spending{budget_remaining}` is the pot balance at the START of
-- a spend period and `Rollover{rollover_balance}` is what is left at the END —
-- BALANCE SNAPSHOTS of the same pot. Summing them as flows double-counts the
-- entire treasury balance twice per period, which is exactly the "treasury
-- page that lies" failure this schema exists to prevent. `figure_kind` makes
-- the distinction DATA, not a naming convention a SQL author has to know:
-- sum only `figure_kind = 'flow'`.
--
-- INSTANCES are pallet instances, mapped from the pallet name by the adapter:
-- `treasury` (Asset Hub, and the relay before 2025-11-04),
-- `fellowship_treasury` and `ambassador_treasury` (Collectives). Which chain
-- hosts which is registry data (`treasury_instances` → residency domain), so
-- this schema names no chain.
--
-- NOT MODELLED HERE, deliberately (each needs its own vocabulary, and rushing
-- them is how you get wrong money): bounties — Asset Hub runs THREE bounty
-- pallets (`Bounties`, `ChildBounties`, and `MultiAssetBounties`, the newer
-- unified asset-denominated one that referendum 1930 funds). They get their
-- own slice and their own drill.
--
-- READ THIS BEFORE BUILDING A BALANCE SHEET ON THESE TABLES: bounty funding
-- leaves the treasury pot through the `SpendFunds` HOOK, which emits
-- `bounties.BountyBecameActive` — NOT `treasury.Awarded` and NOT a pot event.
-- So bounty outflow is invisible to BOTH tables here, and a pot balance built
-- from this slice alone will shrink with no matching outflow row. That is a
-- coverage gap, stated, not a bug to hunt.

create schema if not exists treasury;

-- One row per treasury-pallet event. PK is (chain, block, event) — NOT the
-- spend — so pot flows and any future subject-less event still land.
create table treasury.spend_events (
    chain_id        text not null,
    block_height    bigint not null,
    event_index     integer not null,
    instance        text not null,              -- treasury|fellowship_treasury|…
    spend_kind      text,                       -- proposal | asset_spend | NULL (pot)
    spend_id        bigint,                     -- proposal_index | SpendIndex | NULL
    kind            text not null,              -- proposed|approved|awarded|rejected|
                                                -- paid|payment_failed|processed|voided|
                                                -- pot_spending|pot_burnt|pot_rollover|
                                                -- pot_deposit|pot_inactive_updated
    -- the spend VALUE (SpendApproved.amount, AssetSpendApproved.amount,
    -- Awarded.award) or, for pot events, that event's own figure. NULL where
    -- the event carries no single number (UpdatedInactive carries two — see data).
    amount          numeric,
    -- what `amount` MEANS: 'flow' = money moved, 'snapshot' = a balance
    -- reported at a point in the spend period, NULL = no figure. Never sum
    -- across kinds.
    figure_kind     text,
    -- Rejected.slashed: the PROPOSER'S BOND being slashed, not a spend. Kept in
    -- its own column so it can never be summed as spending by accident.
    slashed         numeric,
    -- VersionedLocatableAsset. NULL on the legacy flow, which was always native.
    asset_kind      jsonb,
    -- The beneficiary as an account (32 bytes) when derivable, AND always the
    -- raw form. Modern spends address a LOCATION; we extract the AccountId32
    -- junction when there is one and leave this NULL when there is not, rather
    -- than inventing an account for a location that names none.
    beneficiary     bytea,
    beneficiary_location jsonb,
    payment_id      text,                       -- Paid / PaymentFailed
    valid_from      bigint,
    expire_at       bigint,
    attribution     text not null,              -- event | pot
    data            jsonb not null,             -- full event fields, schema-on-read
    runtime_version bigint not null,            -- lineage
    mapper_version  integer not null,           -- lineage (rebuild = delete + re-range)
    primary key (chain_id, block_height, event_index)
) partition by list (chain_id);

create table treasury.spend_events_default
    partition of treasury.spend_events default;

-- the PK cannot serve either read path (it is keyed by block), so both earn
-- their write cost: the spend timeline, and the pot-flow stream
create index spend_events_spend_idx
    on treasury.spend_events (chain_id, instance, spend_kind, spend_id, block_height, event_index);
-- the pot stream, ordered exactly as the query reads it. Predicated on
-- `attribution` rather than `spend_id is null`, so a future subject-less-but-
-- not-pot fact (the votes slice's 'unattributed' analogue) cannot leak in.
create index spend_events_pot_idx
    on treasury.spend_events (chain_id, instance, block_height desc, event_index desc)
    where attribution = 'pot';

-- Current state of every spend, converging under any replay order.
create table treasury.spends (
    chain_id        text not null,
    instance        text not null,
    spend_kind      text not null,              -- proposal | asset_spend
    spend_id        bigint not null,
    -- proposed|approved|awarded|rejected|paid|processed|payment_failed|voided.
    -- NOTE 'processed' does NOT assert success: the pallet emits SpendProcessed
    -- both when a payment completed and when the spend expired unclaimed
    -- (pallet-treasury 48.0.0 docs). The payout truth is `paid` + payment_id.
    status          text not null,
    amount          numeric,
    slashed         numeric,
    asset_kind      jsonb,
    beneficiary     bytea,
    beneficiary_location jsonb,
    payment_id      text,
    -- WHICH event supplied payment_id. Both `Paid` and `PaymentFailed` carry
    -- one, so the projection needs its own coordinate for them: without it, a
    -- replay that applies the failed attempt after the successful retry leaves
    -- the spend labelled with the failed payment's id (reviewer catch — the
    -- status coordinate cannot stand in, because `SpendProcessed` moves the
    -- status while carrying no payment at all).
    payment_height  bigint,
    payment_event_index integer,
    valid_from      bigint,
    expire_at       bigint,
    -- height of the first event we saw for this spend (approval, usually)
    first_seen_height bigint not null,
    status_height     bigint not null,          -- ordering guard
    status_event_index integer not null,
    runtime_version bigint not null,
    mapper_version  integer not null,
    updated_at      timestamptz not null default now(),
    primary key (chain_id, instance, spend_kind, spend_id)
);

create index spends_status_idx on treasury.spends (chain_id, instance, status);
-- NO beneficiary index yet: nothing reads by beneficiary in this slice, and an
-- unread index is pure write amplification (the rule that dropped
-- votes_voter_idx in 0008). The consolidation slice adds it WITH its reader —
-- and should add the beneficiary's CHAIN at the same time, since a modern
-- beneficiary is a location and 32 bytes alone do not say which chain holds
-- the account.
