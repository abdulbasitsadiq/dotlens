-- 0022_sim_fork_anchor: the three blockers slice 8's drill found (Phase 3, slice 9).
--
-- Slice 8 shipped a tier that WORKS and does not RUN. Its drill proved the hard
-- half by measurement — dotlens's own captured agenda bytes, written at the right
-- height, dispatched a MediumSpender-origin governance call on a fork and emitted
-- Tier 1's exact five-event footprint, `assets.Transferred(1984, treasury → mbt,
-- 83760000000)` included — and then failed on three things that are not about
-- SCALE at all. This migration carries the two of them that need a column.
--
-- ============================================================================
-- BLOCKER 1 — THE AGENDA IS ON A DIFFERENT NUMBER LINE, AND THE FIX IS A
-- DECISION RATHER THAN A CONSTANT
-- ============================================================================
-- `pallet_scheduler` on Asset Hub runs on a RELAY block-number provider, so
-- `Scheduler.Agenda` is keyed by relay numbers while `System.Number` counts
-- parachain blocks. Measured: AH's live agenda keys span 31,450,649–32,941,123
-- against a head of 19,621,120, and on the fork's own state
-- `ParachainSystem.LastRelayChainBlockNumber` = 32,519,449 against
-- `System.Number` = 19,368,577. Slice 8 wrote at `System.Number + 1` — the right
-- key, the right hasher, and a number ~13.15M blocks in the past.
--
-- THIS IS SLICE 5'S FINDING RECURRING IN A SECOND PALLET. AH treasury
-- `valid_from`/`expire_at` are relay numbers for exactly this reason, and that
-- was already written down when slice 8 was authored. The precedent existing and
-- not being applied is the reason the fix is a DECISION FROM DATA rather than a
-- second constant: a third pallet will do this again, and a constant would be
-- wrong again in the same silent way.
--
-- So the anchor is decided by reading the chain's OWN agenda key space and
-- asking which candidate it is consistent with, and the decision plus its
-- evidence is recorded on the row. `agenda_anchor` holds:
--
--   { "provider": "local" | "relay",
--     "at_parent": <the provider's value at the forked block>,
--     "written_at": <the height the task was written to>,
--     "system_number": …, "relay_number": …,
--     "agenda_keys_observed": <n>, "agenda_key_range": [lo, hi],
--     "decided_by": <free text: a sentence naming both candidates and the
--                     distance from each to the agenda's low key. NOT an enum —
--                     `fork::decide_agenda_anchor` has four wordings, and none
--                     of them is a short tag. A query written as
--                     `decided_by = 'agenda key range'` returns nothing,
--                     forever. Corrected in slice 10.> }
--
-- (This list said `now_at_built` when it was authored. Nothing ever wrote that
--  key: the provider's value inside the block being built is exactly what cannot
--  be observed from outside it, which is the reason the write is biased low
--  below rather than aimed at a number. Corrected in slice 10 rather than left
--  for somebody to query for.)
--
-- A row whose `not_dispatched` cannot be explained is now a row that says which
-- number line it used and what it saw — which is the difference between a
-- debugging session and reading a column.
--
-- AND THE WRITE IS BIASED LOW ON PURPOSE. `service_agendas` walks
-- `IncompleteSince ..= now`, so a task written ABOVE `now` is never reached while
-- one written at or below it is swept exactly once. chopsticks advances the relay
-- anchor by FOUR per built parachain block (measured; do not assume 1), and that
-- number is a property of a mock we do not control. So the task goes at
-- `at_parent` ITSELF and `Scheduler.IncompleteSince` is set to the same value —
-- after which any advance sweeps it, exactly once, without this code having to
-- know what the advance is.
--
-- CORRECTED IN SLICE 10, AND THE ORIGINAL VALUE HERE WAS WRONG BY ONE. This
-- comment said `at_parent + 1` when it was authored, and slice 9's verification
-- measured both: at `at_parent + 1` the run came back `not_dispatched` with the
-- agenda entry still present and no `ParachainSystem` key touched at all; at
-- `at_parent` the same run came back `executed`. The cause is structural rather
-- than a chopsticks quirk — `LastRelayChainBlockNumber` is written by the
-- `set_validation_data` INHERENT, inherents are extrinsics, and extrinsics run
-- AFTER `initialize_block` has already called every `on_initialize` hook, so the
-- relay number a scheduler sees is the PARENT's, always. `at_parent` is also
-- correct for a LOCAL provider (`System::Number` IS incremented at initialize, so
-- `now = at_parent + 1` there and a task at `at_parent` is still inside
-- `IncompleteSince ..= now`), which is why the value is not conditioned on the
-- provider. `fork::decide_agenda_anchor` has carried the corrected rule and its
-- measurement since slice 9's verification; this file did not, and a migration
-- comment is what somebody reads first.
--
-- ============================================================================
-- BLOCKER 2 — dev_newBlock AND dev_runBlock DO NOT COMPOSE ON A PARACHAIN
-- ============================================================================
-- Reproduced with zero dotlens code: a block built by `dev_newBlock` carries 11
-- bytes of `timestamp.set` where the real one carries ~51KB of
-- `parachainSystem.set_validation_data`, because chopsticks mocks parachain state
-- directly and omits the inherent — and `dev_runBlock` then re-applies extrinsics
-- for real and traps on `wasm 'unreachable'`. It is about the two primitives, not
-- about the chain: the same harness re-runs a REAL AH block happily.
--
-- THE FIX IS TO STOP USING BOTH OF THEM. `dev_dryRun` with an extrinsic runs
-- `Core_initialize_block` → the chain's OWN inherent providers →
-- `BlockBuilder_apply_extrinsic` → `finalize_block` and returns the raw storage
-- diff — which is exactly what chopsticks' own preimage plugin does internally,
-- and it therefore works on parachains, because the validation-data inherent is
-- CREATED rather than replayed.
--
-- That collapses two routes into one and makes the tier's two use cases the same
-- shape, differing only in what extrinsic is passed:
--
--   scheduled       — the origin is privileged, so the call goes into the agenda
--                     and fires in `on_initialize`; the extrinsic passed is a
--                     no-op whose only job is to make the dry run happen.
--   dry_run_extrinsic — the origin is `signed:`, so the call IS the extrinsic and
--                     no scheduler is involved at all.
--
-- `dispatch_route` records which, and it is not decoration: THE TWO MODEL
-- DIFFERENT THINGS. A scheduled dispatch runs no transaction extension (it is not
-- an extrinsic); an applied extrinsic runs the whole pipeline — nonce, mortality,
-- fee withdrawal, weight — with only the signature faked. Tier 1's `not_covered`
-- says "no transaction extension runs", and on the second route that sentence is
-- FALSE, so the coverage list is chosen per row from this column.
--
-- The consequence for `built_block_hash` is that it is now NULL on every fork row
-- and stays that way: no block is built. Which is a relief rather than a loss —
-- see below.
--
-- ============================================================================
-- THE FOURTH FINDING: built_block_hash NEVER IDENTIFIED THE COUNTERFACTUAL
-- ============================================================================
-- chopsticks builds every block with `stateRoot: 0x0000…0`, so the built hash is
-- a pure function of (parentHash, number, digest) and is COMPLETELY INDEPENDENT
-- OF STORAGE. Measured: the faithful run and the counterfactual with an injected
-- treasury balance both reported
-- `0x6fb2107dfc94548e647557a6ad6f028ae86d9d205604d0c16295d386f99a7df6`, and so
-- did a plain `dev_newBlock` with no injection at all in a separate process.
--
-- Slice 8's `built_` naming stopped somebody pasting it into an explorer, and did
-- NOT stop somebody reading two rows that differ in every injected byte as
-- different because they share it. The column now carries that fact in its own
-- comment, and on the new route it is NULL — an absent value that means nothing
-- rather than a present one that means less than it looks.
comment on column sim.simulation_results.built_block_hash is
    'The hash of a block that existed only on a fork, on the legacy dev_newBlock '
    'route. NULL on every row written by slice 9 onward, because no block is '
    'built. WHERE IT IS PRESENT IT IS NOT A COMMITMENT TO THE COUNTERFACTUAL: '
    'chopsticks builds every block with stateRoot 0x00…0, so the hash is a pure '
    'function of (parentHash, number, digest) and two rows that differ in every '
    'injected byte share it. Measured 2026-08-18.';

alter table sim.simulation_results
    -- scheduled | dry_run_extrinsic. NULL on a dry_run-tier row, which dispatches
    -- through neither.
    add column dispatch_route text,
    -- The anchor decision and the evidence for it. See the header.
    add column agenda_anchor jsonb;

-- A fork row must say how it dispatched, because the two routes model different
-- things and the coverage list served with the row is chosen from this column. A
-- fork row without it is a row whose `not_covered` cannot be selected — which is
-- the shared-list failure this project has now shipped five times, arriving by a
-- new door.
alter table sim.simulation_results
    add constraint simulation_results_fork_names_its_route
    check (tier <> 'fork' or dispatch_route is not null);

-- The account the vehicle extrinsic is signed as. On the scheduled route it is
-- NOT the origin — the call is dispatched by the scheduler under a privileged
-- origin, and this account merely pays for the no-op extrinsic that makes the
-- block execute. It is stored on the job because a queued job must carry
-- everything its run needs; it is NOT in `input_hash`, because who paid the
-- vehicle's fee does not change what the call does.
alter table sim.simulation_jobs
    add column signer bytea;

-- ---------------------------------------------------------------------------
-- BLOCKER 3 — A FORK ROW COULD NEVER BE RE-RUN, AND IT IS NOT A COLUMN
-- ---------------------------------------------------------------------------
-- The raw store is write-once and keyed on (chain, at_block_hash, input_hash);
-- slice 8 put `harness.command` — including `--port=58859`, the free port bound
-- per run — inside the archived ANSWER. So the bytes differed on every run by
-- construction and the re-put was refused forever, which surfaced as a hard
-- `refusing to overwrite immutable object` on an ordinary re-run.
--
-- The practical bite was sharp: `docker compose down -v` is this project's
-- standard drill, and after any wipe every previously-simulated (chain, block,
-- input) was permanently un-re-runnable.
--
-- FIXED IN THE RAW STORE'S KEY SPACE RATHER THAN HERE, and the split is the
-- point: the ANSWER artifact holds only what is a function of (state, input) —
-- the events, the diff, the anchor decision — and stays immutable and
-- re-readable; the run-specific facts (port, command line, pid, wall clock) move
-- to a sibling `…​.harness.json` keyed per RUN. `raw_location` still names the
-- answer, so a later FORK_VERSION re-derives from evidence exactly as before, and
-- the incidental facts stop poisoning it.
--
-- Slice 1's determinism argument still does not carry across — a dry run is a
-- pure function of state and a fork run is not — so the answer artifact is
-- write-once over things that SHOULD be stable, and a genuine difference at that
-- key remains the loud contradiction the store exists to catch. That is the
-- distinction slice 8 lost by putting a port in it.
