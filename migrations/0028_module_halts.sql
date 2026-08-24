-- 0028_module_halts: the halt, recorded (Phase 3.5, operational floor item 2).
--
-- ROADMAP's operational floor names this item and names its metric: "Monitoring,
-- and the metric is FOLLOWER LAG PER MODULE — not CPU. You need to know a module
-- halted before a consumer tells you." It then says the alert is "a query against
-- a table that exists", which is true of the LAG and false of the HALT, and that
-- gap is what this migration closes.
--
-- ========================================================================
-- WHY A TABLE AT ALL, WHEN LAG IS ALREADY DERIVABLE
-- ========================================================================
--
-- `core.indexer_state` (0001) already holds every checkpoint, and the frontiers
-- it must be compared against are rows in that same table:
--
--     raw_blocks   MODULE_LIVE     how far raw ingestion has followed the chain
--     blocks       MODULE_DECODE   how far decode has got from raw
--     <domain>     e.g. balances   how far this module has got from decode
--
-- So the whole lag STACK is one table and needs no RPC, and `run_tick` already
-- computes the bottom step of it and throws it away (module.rs: `target` is the
-- decode checkpoint, `from` is the module's own). Nothing here stores lag, and
-- nothing may: "never store a derived thing — the copy would be the one WITHOUT
-- lineage" is this project's most-repeated argument (six times by slice 16).
--
-- What is NOT derivable is WHY a module stopped. A halt is a mapper refusing an
-- unmappable variant — it exists for the microseconds between `map()` returning
-- Err and the error reaching a `tracing::warn!`, and then it is gone. Today the
-- range path propagates it to `main` as anyhow and the FOLLOWER path only logs it
-- and retries forever with linear backoff. So the operator question "is this
-- module behind because a backfill is running, or because a human is needed" has
-- no answer in the database at all, and the two states look identical.
--
-- This table is therefore an OBSERVATION, not a derivation. It records that a
-- mapper refused, with the coordinates needed to go and look.
--
-- ========================================================================
-- WHAT IS DERIVED RATHER THAN STORED, AND IT IS THE LOAD-BEARING DECISION
-- ========================================================================
--
-- There is NO `active` column, NO `resolved_at`, and NO status enum. Whether a
-- recorded halt still blocks the module is:
--
--     halt.height > indexer_state.last_height        -- still ahead: blocking
--     halt.height <= indexer_state.last_height       -- the module got past it
--
-- one join, computed at read time. A stored `active` flag would be a second copy
-- of a fact the checkpoint already owns, and the two would disagree the first
-- time a mapper was fixed and a `*-range` re-run advanced the checkpoint without
-- anybody remembering to clear a row. That is the same instinct that refused
-- `treasury.consolidated_position` and `graph.cross_chain_operations`.
--
-- The escape clause is the usual one: materialise it the day a LISTING surface
-- needs it. A per-chain status page is not that day — it reads at most a few
-- dozen rows.
--
-- ========================================================================
-- WHY UPSERT-ON-COORDINATES AND NOT AN APPEND LOG
-- ========================================================================
--
-- The follower does not halt once. `run_follow` retries on a linear backoff
-- capped at 11x, so a module halted on an unmapped variant re-derives the SAME
-- refusal every tick, forever, until a human edits a mapper. An append-only log
-- of halt EVENTS would therefore grow without bound while carrying exactly one
-- bit of information, and the newest row would be the least interesting one.
--
-- So the primary key is the halt's own coordinates — (chain_id, module, height,
-- event_index) — and a repeat is an update, not a row. `first_seen_at` never
-- moves; `last_seen_at` only moves forward; `seen_count` counts observations.
-- That keeps the observation WINDOW (which is what tells you a halt is still
-- live rather than historical) without inventing a row per tick.
--
-- Ordering guard, because a row set that depends on write order is this
-- project's E1 defect (`merge_spend` blocker (c), then the identical one found
-- in shipped `merge_bounty`): a `*-range` re-run may re-observe an OLD halt after
-- the follower has recorded a NEWER one. `first_seen_at` therefore takes
-- `least()` and `last_seen_at` takes `greatest()`, so replay in any order
-- converges on the same row. Nothing about this table is order-dependent.
--
-- ========================================================================
-- THE BLOCKING HALT IS THE LOWEST ONE ABOVE THE CHECKPOINT
-- ========================================================================
--
-- A module can accumulate several halt rows: `balances-range 100 200` halts at
-- 150, then `balances-range 300 400` halts at 320. Neither is "the" halt on its
-- own. The one that stops the follower is the LOWEST height above the module's
-- checkpoint, because the runtime processes in order and can never reach the
-- others. The reader must therefore order by height and take the first, never
-- take the most recent by time — a `max(last_seen_at)` reader would name the
-- halt at 320 while the module is stuck at 150, sending whoever reads it at 3am
-- to the wrong block.
--
-- NO INDEX IS CREATED, deliberately, and this is 0019's rule rather than an
-- omission: that reader is `where chain_id = $1 and module = $2 and height > $3
-- order by height limit 1`, whose leading columns are the primary key's own, in
-- the primary key's own order. The PK's index serves it. An index arrives with
-- its reader (E3) — and so does the refusal to add a redundant one.
--
-- ========================================================================
-- WHAT IS STORED BESIDE THE COORDINATES, AND WHY EACH EARNS ITS COLUMN
-- ========================================================================
--
-- `event` and `reason` are the mapper's own words, and the halt WORDING is
-- per-module by deliberate design (module.rs: "'gaps in money', 'gaps in
-- referendum history' and 'money they cannot explain' are different warnings at
-- 3am"). Storing the module's sentence rather than a normalised code keeps that.
--
-- `runtime_version` and `mapper_version` are lineage (Invariant 3). An unmapped
-- variant is almost always a RUNTIME UPGRADE arriving — Phase 3 slice 6 met five
-- unmapped pallet-balances variants at once, three of which move money — so the
-- spec_version the refusal happened under is the first thing anyone will want,
-- and a halt row without it sends them to look it up by hand.
--
-- No partition, no new partitioned table, so `registry_sync` is untouched.

create table core.module_halts (
    chain_id         text        not null,
    -- The `indexer_state.module` key, so the join to the checkpoint is direct.
    module           text        not null,
    height           bigint      not null,
    event_index      integer     not null,
    -- "pallet.Variant", verbatim from the decoded event.
    event            text        not null,
    -- The mapper's own sentence. Per-module voice, deliberately not normalised.
    reason           text        not null,
    -- Lineage: an unmapped variant is usually an upgrade arriving.
    runtime_version  bigint      not null,
    mapper_version   integer     not null,
    first_seen_at    timestamptz not null default now(),
    last_seen_at     timestamptz not null default now(),
    seen_count       bigint      not null default 1,
    primary key (chain_id, module, height, event_index)
);

comment on table core.module_halts is
    'Observed mapper refusals. A halt is a FACT; whether it still blocks is '
    'derived by comparing height against core.indexer_state.last_height for the '
    'same (chain_id, module). There is deliberately no active/resolved column.';

comment on column core.module_halts.height is
    'The block the mapper refused. The BLOCKING halt for a module is the lowest '
    'such height above its checkpoint — never the most recently seen one.';

comment on column core.module_halts.seen_count is
    'Observations, not occurrences. The follower re-derives the same refusal '
    'every tick on a linear backoff, so this counts ticks that met it, and is '
    'read together with last_seen_at as an observation window rather than alone.';
