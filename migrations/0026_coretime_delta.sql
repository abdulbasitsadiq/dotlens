-- 0026_coretime_delta: THE READER (Phase 3, slice 14).
--
-- 0024 shipped occupancy. 0025 shipped entitlement and said, in its own header,
-- that it shipped no reader for it: "THE DELTA READER IS NOT IN THIS SLICE —
-- there is no `BrokerIndex`, no route and no `select` against either table
-- anywhere in the repo, only the writer below." It then DESCRIBED four queries
-- and deliberately created NO INDEX for any of them, on 0019's rule ("the query
-- that WOULD earn this index does not exist yet and should arrive WITH its
-- index"), which 0020 obeyed before it. (Slice 13's authoring wrote those four
-- and removed them again before shipping; a reader of 0025 as committed finds
-- nothing dropped, only four indexes described and withheld.)
--
-- This is that reader, so this is where the four arrive. Each one below names
-- the query that earns it and the endpoint that issues it — AND the plan
-- Postgres will actually produce, which is not always the flattering one.
--
-- ============================================================================
-- WHAT THIS MIGRATION DOES NOT ADD, AND WHY IT IS THE FIFTH REFUSAL OF THE SAME
-- SHAPE
-- ============================================================================
-- There is still no `coretime.delta` table, and there is not going to be one.
-- The delta is (entitlement rows JOIN occupancy rows) over a stated relay-block
-- window; both sides already carry `runtime_version` and `mapper_version`, so
-- materialising the join would create the one copy WITHOUT lineage. That is
-- verbatim what killed `treasury.consolidated_position` (P2 slice 6),
-- `graph.cross_chain_operations` (P3 slice 3), the stored forwarded-attribution
-- (P3 slice 5) and a `logical_assets` join table (0019). It is computed per
-- request by `api::coretime_delta`, which is a PURE function precisely so that
-- the arithmetic can be tested without a database and cannot drift from a
-- second copy of itself.
--
-- Materialise it the day a LISTING surface needs it — a page of windows rather
-- than one window — and not before.
--
-- ============================================================================
-- THE FOUR INDEXES, EACH WITH THE QUERY THAT EARNS IT
-- ============================================================================

-- ---------------------------------------------------------------------------
-- (1) "what was core N entitled to do at relay height H"
-- ---------------------------------------------------------------------------
-- THE DELTA'S OWN INDEX, and the one the whole endpoint rests on.
--
-- TWO READERS, AND ONLY ONE OF THEM GETS THE PLAN THIS COLUMN ORDER SUGGESTS.
-- `api::BrokerIndex::assignments_for_core` (`where chain_id = $1 and core_index
-- = $2 order by relay_block desc … limit $3`) IS a backwards index scan that
-- stops at the limit, and it is what earns the column order.
-- `entitlement_at`'s `distinct on (core_index) … order by core_index,
-- relay_block desc` over `relay_block <= $2` is NOT: **Postgres has no loose or
-- skip index scan**, so it reads every assignment row of that chain either way,
-- and what this index buys it is an index-ordered scan plus an incremental sort
-- instead of a full sort. Stated rather than glossed, because 0013 shipped an
-- "ONE index probe" claim that the plans then disproved, and CLAUDE.md records
-- it being corrected in place.
--
-- (A per-core `limit 1` lateral WOULD get the backwards-scan plan. It does not
-- exist because this reader derives its core universe AFTER the query, from the
-- declared count and from what it found — and inverting that would mean asking
-- the database a question shaped by an answer it has not given yet.)
--
-- THE LOOKBACK IS DELIBERATELY UNBOUNDED, and that is not an oversight — it is
-- the measured shape of the data. `Broker.CoreAssigned` fires only at SALE
-- BOUNDARIES, and slice 13's verification had to hunt the sale governing slice
-- 11's 1,000-block window back to coretime #4766580 (relay 32278800) against a
-- window at relay 32613537-32614536 — **~157,000 coretime blocks, ~335,000
-- relay blocks earlier**. (CLAUDE.md's slice-13 entry says 1.58M; that is the
-- distance between the two SALES the prep sampled — coretime 3188739 and
-- 4766580 — not the distance from the governing sale to the window, and it is
-- corrected in the same pass as this migration.) A reader that only looked
-- "recently" would find nothing and would then have to decide what nothing
-- means, which is the one decision 0025 forbids it to get wrong.
--
-- `core_index` is NOT NULL on this table, so no partial predicate is needed or
-- possible here.
create index core_assignments_core_idx
    on coretime.core_assignments (chain_id, core_index, relay_block);

-- ---------------------------------------------------------------------------
-- (2) "where did task P hold entitlement"
-- ---------------------------------------------------------------------------
-- Earned by `/v1/coretime/{chain}/entitlement?task=P`, whose assignment arm is
-- `where chain_id = $1 and task_id = $2 order by relay_block desc`.
--
-- THE TASK IS THE DURABLE IDENTITY AND THAT IS WHY THIS INDEX EXISTS SEPARATELY
-- FROM (1). At coretime 4919882 para 3428 renewed five cores and every index
-- moved (35→43, 36→44, 37→45, 40→46, 41→47), so following a tenant across sale
-- cycles by core index silently follows a different tenant after every sale.
-- Following it by task works, and needs its own access path.
--
-- PARTIAL, because `task_id` is NULL on every `pool` and `idle` row — 43 of 100
-- cores in the measured sale — and indexing those NULLs costs writes for rows no
-- equality lookup can ever reach. 0020's argument, with 0020's caveat: the
-- partial predicate is provable only while the query keeps the indexed column
-- bare on the left (`task_id = $2`, never `coalesce(task_id, …) = $2`), because
-- a strict operator is what lets Postgres prove `= $2` implies `is not null`.
-- Measured in slice 7's verification: `= any($1)` still proves it, `lower(col) =
-- $1` does not.
create index core_assignments_task_idx
    on coretime.core_assignments (chain_id, task_id, relay_block)
    where task_id is not null;

-- ---------------------------------------------------------------------------
-- (3) "what happened to core N"
-- ---------------------------------------------------------------------------
-- Earned by `/v1/coretime/{chain}/entitlement?core=N`, event arm:
-- `where chain_id = $1 and core_index = $2 order by block_height desc`.
--
-- PARTIAL for the reason 0025 spelled out and then had to correct: **18 of the
-- 37 declared variants leave `core_index` NULL** (19 name a core). Fifteen name
-- neither a core nor a task, which is a different and smaller count, and 0025's
-- first draft conflated the two in the one number that says how much of the
-- table this predicate excludes.
create index broker_events_core_idx
    on coretime.broker_events (chain_id, core_index, block_height)
    where core_index is not null;

-- ---------------------------------------------------------------------------
-- (4) "what happened to task P"
-- ---------------------------------------------------------------------------
-- Earned by the same endpoint's `?task=` event arm. Partial for the same reason,
-- and the exclusion here is far larger: only 6 of the 37 variants name a task at
-- all (`Assigned`, the three lease variants, and the two auto-renewal ones that
-- carry both).
create index broker_events_task_idx
    on coretime.broker_events (chain_id, task_id, block_height)
    where task_id is not null;

-- ============================================================================
-- `Broker.SaleInfo`: DECIDED IN THIS SLICE RATHER THAN DEFERRED AGAIN
-- ============================================================================
-- 0025 captured `Broker.Status` and `Broker.Configuration` and listed
-- `Broker.SaleInfo` as the one thing with a real deadline: `cores_sold` and
-- `first_core` move on EVERY PURCHASE with no event at all, so nothing about a
-- sale is reconstructible from what this project records, and a reading not
-- taken cannot be taken later. Slice 13's own "known gaps" called it "the one
-- worth adding before the Phase 4 backfill".
--
-- IT IS ADDED HERE, AND NOT BECAUSE THE DEADLINE ARGUMENT WON. The deadline
-- argument is real and it is not sufficient — this project's rule is that a
-- column arrives WITH its reader (0008, 0009, 0019, 0020), and "we will want it
-- later" is precisely the argument that rule exists to refuse. What settles it
-- is that **this slice has a reader for `first_core`, and the product claim
-- depends on it**:
--
--     the 10 idle Task-entitled cores measured in slice 13's verification are
--     cores 11, 14, 15, 23, 31, 33, 45, 55, 56, 58 — EVERY ONE index >= 11, so
--     the waste is entirely MARKET-SIDE and no reserved system core is idle.
--
-- **AND THE `11` IN THAT SENTENCE IS AN INFERENCE, WHICH MAKES THE CASE FOR THE
-- COLUMN STRONGER RATHER THAN WEAKER.** Slice 13 never captured
-- `Broker.SaleInfo` — its own known-gaps list says so in as many words — so
-- `first_core = 11` comes from `Broker.Reservations` holding 11 entries at a
-- different moment, and "every idle core is above first_core" is a conclusion
-- drawn across two readings that were never taken together. Calling that
-- measured would be the defect slice 13's own review caught one file over, where
-- a seed stated an unmeasured claim under the word "Measured:".
--
-- Replacing that inference with a DATED, RE-DERIVABLE reading is what this
-- column is for. Without it the endpoint could only make the claim by writing
-- `11` into Rust — a chain-specific constant Invariant 2 forbids outright, and a
-- number nobody could date, which is what `coretime.core_config` exists to
-- prevent one table over.
--
-- `cores_sold`, `end_price`, `sellout_price`, `region_begin`/`region_end` and
-- `cores_offered` ride along inside `sale_info` because they are the SAME READ
-- and the deadline is real for them too. **NOTHING READS THEM YET**, and that
-- is stated rather than implied: the Dutch leadin curve is evaluated against
-- `SaleInfo` + `Configuration` together, and reconstructing it is its own slice.
-- They are captured, not promoted; `first_core` is promoted because it is read.
--
-- BOTH COLUMNS ARE NULLABLE, and the null is load-bearing. `Broker.SaleInfo` is
-- an `OptionQuery` StorageValue: it is ABSENT before the first sale starts. A
-- chain that has never sold a core and a chain whose reading we never took must
-- not be the same row, so `sync-broker-config` records the absence explicitly
-- and the reader renders "reserved vs market cannot be separated" rather than
-- assuming `first_core = 0` — which would silently reclassify every reserved
-- system core as an unsold market core and put the waste on the wrong side of
-- the boundary this column exists to draw.
alter table coretime.broker_config
    add column if not exists first_core integer,
    add column if not exists sale_info jsonb;

comment on column coretime.broker_config.first_core is
    'SaleInfo.first_core — the index of the first core OFFERED FOR SALE, so '
    'cores [0, first_core) are reserved/leased system cores and '
    '[first_core, ...) are the bulk market. THE BOUNDARY THE WASTE FIGURE TURNS '
    'ON: slice 13 measured all ten idle Task-entitled cores at index >= 11, and '
    'INFERRED first_core = 11 from Broker.Reservations holding 11 entries — it '
    'never read SaleInfo at all. This column is what replaces that inference '
    'with a dated reading. NULL means SaleInfo was absent (sales never started) '
    'or no reading has been taken, and the reader must then say "cannot '
    'separate" rather than assume 0, which would move every reserved core into '
    'the market and put the waste on the wrong side.';

comment on column coretime.broker_config.sale_info is
    'The whole Broker.SaleInfo record, kept intact (schema-on-read): sale_start, '
    'leadin_length, end_price, sellout_price, region_begin, region_end, '
    'first_core, ideal_cores_sold, cores_offered, cores_sold. CAPTURED, NOT '
    'READ — only first_core above has a reader. It is here because cores_sold '
    'and first_core move on every purchase with NO EVENT, so the Dutch leadin '
    'price curve is not reconstructible from anything else this project stores, '
    'and a reading not taken cannot be taken later. Reconstructing the curve '
    '(against Configuration.leadin_length and the sale geometry) is its own '
    'slice.';

-- ============================================================================
-- ONE CORRECTION TO 0025 THAT IS MADE HERE RATHER THAN THERE.
-- ============================================================================
-- 0025's header says the dated-state-reading pattern stands at FIVE instances;
-- its own `comment on table coretime.broker_config` calls that table "the SIXTH
-- instance". Slice 13's review corrected the count to FIVE in three places and
-- missed the table comment, and `broker_pg.rs` says FIFTH. **IT IS THE FIFTH.**
--
-- The comment is not corrected in 0025 because editing an APPLIED migration
-- breaks its sqlx checksum and costs a one-row repair (`shasum -a 384` plus an
-- update against `_sqlx_migrations`, with the binary rebuilt FIRST because
-- migrations are embedded at compile time) — a price worth paying for a wrong
-- CONSTRAINT and not for a wrong ordinal. Recorded here so the next reader finds
-- the correction beside the contradiction rather than after it.

-- ============================================================================
-- A NOTE FOR WHOEVER QUERIES THESE TABLES BY HAND, because slice 13's own
-- VERIFY doc got this wrong and the error was in the doc rather than the code.
-- ============================================================================
-- `core_assignments.relay_block` is `CoreAssigned.when` — the event's OWN relay
-- block, always an exact multiple of 80 (one timeslice). It is NOT the coretime
-- block's relay PARENT. At coretime 3188739 those are 29053200 and 29053193
-- respectively, and only the first is a multiple of 80 (403485 x 80).
--
-- AND A JOIN WITH NO HEIGHT CONSTRAINT PROVES LESS THAN IT LOOKS. Slice 13's
-- verification ran the join as its doc spelled it — matching on core index alone
-- — and got 34,690 rows, which established that the two INDEX SPACES align and
-- NOT that entitlement agreed with usage at a shared instant: the two sides sat
-- ~3.56M relay blocks apart. The reader always constrains the entitlement side
-- to `relay_block <= <the window's end>` and reports, per core, the relay block
-- its governing assignment took effect at, so the distance is visible rather
-- than assumed away.
