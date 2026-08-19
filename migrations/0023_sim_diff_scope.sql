-- 0023_sim_diff_scope: what a fork row's storage diff actually covers
-- (Phase 3, slice 10).
--
-- No new table, no new column. Two CHECKs and two comments — because the defect
-- this migration exists for is not a missing fact, it is a column that has been
-- carrying a WRONG one.
--
-- ============================================================================
-- THE MEASUREMENT
-- ============================================================================
-- Slice 9 argued that driving the fork through `dev_dryRun` makes it
-- "structurally impossible for the events and the diff to describe different
-- executions". Its own verification measured the opposite. Between a
-- `not_dispatched` run and an `executed` run of the SAME subject:
--
--   * the events blob grew 464 → 862 characters
--   * the diff key set was IDENTICAL — the same 12 keys, nothing added
--
-- A run that emitted `assets.Transferred` and `system.NewAccount` showed no
-- `Assets.Account`, no `MultiAssetBounties` key, and no `Scheduler.Agenda`
-- deletion. The dispatch that the whole row is about left no trace in the
-- column that is supposed to describe it.
--
-- ============================================================================
-- THE CAUSE, READ FROM THE ENGINE'S OWN SOURCE RATHER THAN INFERRED
-- ============================================================================
-- chopsticks, `packages/core/src/blockchain/block-builder.ts`. `initNewBlock`
-- runs `Core_initialize_block` and then each inherent, and CONSUMES every
-- response into a storage layer:
--
--     const resp = await newBlock.call('Core_initialize_block', [header.toHex()])
--     newBlock.pushStorageLayer().setAll(resp.storageDiff)
--
-- `dryRunExtrinsic` then returns ONE `TaskCallResponse` — the one from
-- `BlockBuilder_apply_extrinsic` — and `.storageDiff` on it is that single
-- runtime call's writes. Every earlier phase is in the block's STATE, which is
-- why the runtime sees the injected scheduler task and dispatches it, and in no
-- returned value.
--
-- AND THAT DERIVES THE MEASUREMENT EXACTLY, which is what makes it a cause
-- rather than a story that fits. `apply_extrinsic` WRITES `System.Events`: it
-- reads the current list and appends its own. So the value it writes is the
-- whole cumulative list, `on_initialize`'s events included — one key carrying a
-- block, beside every other key carrying one extrinsic. That one key is why the
-- events grew while the key set did not.
--
-- NOTE THAT KEY NEVER REACHES `storage_diff`. dotlens lifts `System.Events` out
-- of the raw answer to decode the block's events and then EXCLUDES it, because
-- its `before` is last block's events and tells a reader nothing. So the stored
-- diff is uniformly extrinsic-scoped, and `emitted_events` beside it is the
-- whole block — which is the shape a reader has to be told, since the column
-- names do not suggest it.
--
-- THE OTHER VEHICLES WERE CHECKED AND DO NOT HELP. `dev_dryRun` takes four:
-- `extrinsic` (above) and `hrmp`/`dmp`/`ump`, which go through
-- `dryRunInherents` — and that merges `initNewBlock`'s `layers`, a list declared
-- AFTER the initialize call which collects the INHERENT layers only. There is no
-- `preimage` vehicle on the RPC at all: the CLI's `dryRunPreimage`, which DOES
-- get a whole-block diff by putting every phase into ONE `runTask`, writes an
-- HTML file and calls `process.exit(0)`.
--
-- ============================================================================
-- WHAT IS RECORDED, AND WHAT IS DELIBERATELY NOT
-- ============================================================================
-- NOT: a partial diff synthesised from the keys the decoded events imply. It was
-- designed and refused, for two reasons that are about this project's rules
-- rather than about effort. An `assets.Transferred` → `Assets.Account(id, who)`
-- mapping is per-pallet knowledge in the one file whose doctrine is that adding a
-- chain must never require new branches (Invariant 2), and it would be a table of
-- chain-specific rules wearing a data structure. And there is no post-state to
-- read the values FROM — `dev_dryRun` commits nothing — so every synthesised
-- entry would carry a key with no value and a `before` with no `after`, which is
-- a diff-shaped object that is not a diff. A fabricated entry beside twelve real
-- ones is worse than twelve real ones and a sentence.
--
-- SO: the status says what the bytes cover, and the reader is pointed at
-- `emitted_events`, which is a complete record of the dispatch and always was.
--
-- `diff_status` gains a FIFTH value, `extrinsic_only`, and does not gain a
-- sibling column. Scope was drafted as its own column and refused on this
-- project's own most-repeated rule: whether the diff covers the SUBJECT is a
-- function of `dispatch_route`, which is already a column, and a column that is a
-- function of another column is the derived copy that killed
-- `treasury.consolidated_position`, `graph.cross_chain_operations`, the
-- `logical_assets` join table and the stored forwarded-attribution. What is NOT
-- derivable is which method produced these particular bytes — a later harness, or
-- a route that reaches `dev_runBlock`, would return every phase on the scheduled
-- route too — so the row records what THIS run got. That is lineage, and lineage
-- is what `diff_status` already is.
--
-- ============================================================================
-- EXISTING ROWS ARE WRONG AND REBUILD WITHOUT A RE-RUN
-- ============================================================================
-- Every fork row written by slice 9 says `decoded`. They were produced by
-- `dev_dryRun` and are `extrinsic_only`. `FORK_VERSION` moves 1 → 2.
--
-- THE REBUILD IS AN UPDATE, NOT A RE-RUN, and the first draft of this comment
-- got that wrong in a way worth recording. `interpret_fork` IS pure and could
-- re-derive the status from the archived answer with no chain and no harness —
-- but nothing calls it standalone: `run_fork_simulation` checks the DB cache and
-- on a miss goes straight to `dispatch_fork`, which starts a Node process. So
-- "delete the row and re-run" deletes the only cache there is and then forks
-- again for an answer it already has.
--
-- Every slice-9 row is identifiable without reading a byte — it is the only
-- generation with `built_block_hash IS NULL` (0022 records exactly that), and
-- `dev_dryRun` is the only method that produced those rows:
--
--     update sim.simulation_results
--        set diff_status = 'extrinsic_only', sim_version = 2
--      where tier = 'fork' and sim_version = 1 and built_block_hash is null;
--     update sim.simulation_results
--        set sim_version = 2
--      where tier = 'fork' and sim_version = 1 and built_block_hash is not null;
--
-- The second statement is the slice-8 generation: those rows were produced by
-- `dev_runBlock`, which really does return every phase, so their `decoded` was
-- and remains correct — only the version stamp moves. `diff_scope_from_answer`
-- makes the same distinction on the same evidence when an answer is re-read, so
-- the SQL and the code cannot disagree about which generation a row is.
--
-- A `reinterpret-fork` command that reads `raw_location`, calls `interpret_fork`
-- and rewrites the row is the honest general answer and does not exist yet;
-- until it does, an interpretation change that cannot be expressed as SQL needs
-- a re-run. Stated as a gap rather than implied to be covered.
--
-- ---------------------------------------------------------------------------
-- 0022 IS EDITED BY THIS SLICE AND IS ALREADY APPLIED — REPAIR ITS CHECKSUM
-- ---------------------------------------------------------------------------
-- Two stale facts in 0022's header are corrected (the agenda is written at
-- `at_parent`, not `at_parent + 1`; and `now_at_built` was never emitted).
-- sqlx stores `checksum = SHA-384(file bytes)`, so deploying without the repair
-- fails with "migration 22 was previously applied but has been modified".
--
-- REBUILD THE BINARY FIRST — migrations are embedded at COMPILE time, so the
-- repair fails with a bare `Error: running migrations` until the binary is built
-- against the edited file. This project has paid that twice (slices 7 and 8):
--
--     cargo build -p dotlens-node
--     shasum -a 384 migrations/0022_sim_fork_anchor.sql
--     update _sqlx_migrations set checksum = decode('<hex>','hex') where version = 22;
--
-- No DB wipe is needed for either the repair or the rebuild above.
--
-- ---------------------------------------------------------------------------
-- THE ARCHIVED ANSWER'S KEY MOVES, AND THAT IS DELIBERATE
-- ---------------------------------------------------------------------------
-- The harness answer's SHAPE changed (`diff_status` out, `diff_method` in), and
-- it is archived under the write-once (chain, block, input) key. Bytes that
-- differ at a key that already holds the old shape are refused forever — which
-- is precisely what a port did to slice 8 and a clock did to slice 9, twice
-- leaving every previously-simulated state un-re-runnable.
--
-- So `fork::FORK_METHOD` now carries the shape version and the item name is
-- `chopsticks_fork.v2.response.scale`. A new shape writes a NEW file beside the
-- old one; existing rows keep pointing at theirs through `raw_location`, which
-- is why the UPDATE above does not touch that column; and the store's real job —
-- catching two DIFFERENT answers to one question in ONE shape — is untouched.
-- Nothing needs migrating, and old artifacts are not orphaned.

-- The vocabulary becomes an INTEGRITY GUARANTEE rather than a convention five
-- string literals across three crates have to remember. Until now nothing
-- constrained this column, so a typo'd status was accepted silently and then
-- read as "no diff" by the gate — a blank column beside a status claiming the
-- bytes were read. Cheap, and it makes the argument above enforceable.
--
-- NULL is permitted and means the row is not a fork row: a `dry_run` or XCM
-- simulation asks a runtime API and produces no storage diff at all.
alter table sim.simulation_results
    add constraint simulation_results_diff_status_vocabulary
    check (diff_status is null or diff_status in
           ('decoded', 'extrinsic_only', 'undecodable', 'unavailable', 'refused'));

-- ...and a FORK row must actually name one, so that "NULL means not a fork row"
-- is a guarantee rather than a sentence in a comment above a constraint that
-- permits the opposite. Without this the API had to invent a value for a NULL,
-- and the only honest invention is "unavailable" — a POSITIVE claim ("this build
-- of the harness has no diff method at all") about a run nobody made it of.
--
-- Modelled exactly on 0022's `simulation_results_fork_names_its_route`, and for
-- the same reason: a fork row that cannot say what its diff covers is a row
-- whose `diff_covers` cannot be computed.
--
-- EVERY EXISTING FORK ROW SATISFIES IT — slice 8's rows carry 'refused' and
-- slice 9's carry 'decoded' — so this validates without a rewrite. If a
-- deployment somehow holds one with NULL, the ALTER fails loudly and names the
-- row, which is the correct outcome and not something to work around.
alter table sim.simulation_results
    add constraint simulation_results_fork_names_its_diff_scope
    check (tier <> 'fork' or diff_status is not null);

comment on column sim.simulation_results.diff_status is
    'What the storage diff covers, and whether it could be read — five values, '
    'because a boolean could hold neither question. '
    'decoded: every phase of the block was returned and named against the '
    'runtime''s metadata (`dev_runBlock`, which runs Core_initialize_block as its '
    'own phase). '
    'extrinsic_only: the bytes were read and decoded and cover the '
    'apply_extrinsic phase ONLY — Core_initialize_block and the inherents are not '
    'in them. This is what `dev_dryRun` returns, measured 2026-08-18 from '
    'chopsticks'' own block-builder.ts, and it is the ordinary case on the live '
    'route. ON THE SCHEDULED ROUTE IT DOES NOT COVER THE CALL: a privileged '
    'dispatch happens in on_initialize, so the diff describes the no-op vehicle '
    'extrinsic and `emitted_events` is the record of what the call did. On the '
    'dry_run_extrinsic route the same bytes ARE complete, because the subject is '
    'the extrinsic. '
    'undecodable: a diff came back in a shape that version could not read; the '
    'bytes are archived and nothing is guessed. '
    'unavailable: this build of the harness exposes no diff method at all. '
    'refused: it HAS one and it failed on this block — the ordinary case for '
    '`dev_runBlock` on a parachain, where the harness builds a block without '
    'set_validation_data and then cannot re-execute it. '
    '"We did not look", "nothing changed" and "we looked at the wrong half" are '
    'never the same value.';

-- Read beside `dispatch_route`, which is what decides whether an `extrinsic_only`
-- diff is a limitation or the correct scope. Recorded here as well as in the API
-- because a row outlives the code that served it.
comment on column sim.simulation_results.storage_diff is
    'The decoded storage changes, in the scope `diff_status` names — NOT '
    'necessarily the whole block, and on a scheduled fork row NOT the effects of '
    'the call. NULL when diff_status carries no bytes; never [] in that case, '
    'because an empty array is a run that changed nothing.';
