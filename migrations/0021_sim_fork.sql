-- 0021_sim_fork: Tier 2 — the chopsticks fork, the queue, and the counterfactual
-- (Phase 3, slice 8).
--
-- 0014 shipped Tier 1 and said, in its own opening comment, that
-- `simulation_jobs` "lands with Tier 2" because "Tier 1 has no queue — it is one
-- request and one response". This is that slice, and the queue is earned the
-- moment the work stops being one round trip: a Tier 2 run spawns a Node process
-- that executes a runtime WASM and pulls state over RPC from a public endpoint,
-- so it has to be capped, leased and resumable in a way a synchronous command
-- cannot express across processes.
--
-- ============================================================================
-- WHY THIS IS THE SAME TABLE AND NOT A SECOND ONE
-- ============================================================================
-- 0018 split `sim.xcm_simulations` out of `sim.simulation_results` on a rule
-- worth restating, because it points the other way here: split when the SUBJECT
-- differs. An XCM row is a PROGRAM FROM A LOCATION and would have left
-- `call_hash`, `origin_spec`, `origin_json`, `dispatch_ok`, `dispatch_error` and
-- `local_xcm` empty — six columns and the table's one earned index.
--
-- A Tier 2 row has the SAME subject as a Tier 1 row: a call, under an origin, at
-- a state. `call_hash`, `origin_spec`, `origin_json`, `status`, `dispatch_ok`,
-- `dispatch_error`, `emitted_events` and `event_count` all populate, and
-- `simulation_results_call_idx` serves it unchanged — which is precisely what
-- 0014 put `tier` in the PRIMARY KEY for: "two tiers can answer the same (state,
-- input) and they are NOT interchangeable … Left out of the key, the first Tier
-- 2 run at a state Tier 1 had already touched would hit `on conflict do nothing`,
-- be silently discarded, and then be SERVED as a cache hit: a fork simulation
-- that never forked." Honouring that column is the point of this migration.
--
-- ============================================================================
-- THE OVERRIDE SET IS PART OF THE KEY, AND THAT IS THE SAME ARGUMENT AGAIN
-- ============================================================================
-- Tier 2's reason to exist beyond Tier 1 is that it can answer a COUNTERFACTUAL:
-- run this call at this block, but with the treasury's USDC balance raised, and
-- see whether the payout that failed would have succeeded. The injected state is
-- USER INPUT, so two counterfactuals at one block with different injected
-- balances are two different questions with two different answers — and if the
-- override set is not folded into `input_hash` the second is silently served the
-- first one's row. Exactly the hazard `tier` was added to prevent, one field
-- over, and silent when wrong in exactly the same way.
--
-- So a fork run's `input_hash` is blake2b-256 over a CANONICAL REQUEST ENCODING
-- that carries all three inputs: origin bytes, call bytes, and the resolved
-- override set (sorted by key, each entry key ++ value-or-absent). It is prefixed
-- with the ASCII domain tag `dotlens.fork.v1` so a fork input hash can never
-- collide with a Tier 1 one even by accident — the two hash different things and
-- a reader joining on `input_hash` alone across tiers must not be able to mix
-- them.
--
-- ============================================================================
-- A COUNTERFACTUAL MUST BE STRUCTURALLY UN-MISTAKABLE FOR HISTORY
-- ============================================================================
-- A state diff produced from fabricated state is the one artifact in this
-- project that could be screenshotted as "what the chain did" and be wrong in a
-- way nobody could see. Four structural defences, all in this migration rather
-- than in a reader that could forget them:
--
--   1. `overrides` is stored ON THE ROW, and NULL means "not a counterfactual".
--      Not `[]` — an empty array would be indistinguishable from a counterfactual
--      with nothing in it, and the API's whole rendering turns on the difference.
--   2. Each override entry carries the value the REAL chain held at that block
--      (`before`, read over ordinary RPC from the real endpoint) beside the value
--      that was injected (`after`). The fabrication is legible from the row alone,
--      without re-running anything.
--   3. A CHECK constraint makes it impossible to record overrides without the
--      hash that keeps them out of another run's cache (see above).
--   4. `built_block_hash` is the hash of a block that EXISTS ONLY ON THE FORK.
--      It is deliberately not called `block_hash`, it is never written to
--      `core.blocks`, and every reader is told what it is — because pasting it
--      into a block explorer is the obvious next thing somebody does with a
--      32-byte hex string that a page called it a block hash.
--
-- ============================================================================
-- WHAT THIS TIER MOCKS, RECORDED WHERE THE COLUMN IS
-- ============================================================================
-- Tier 1 shipped saying "the call is dispatched directly, so no transaction
-- extension runs: no signature check, no nonce, no mortality, no fee withdrawal
-- and no length or weight limit". Tier 2 must meet that standard about its own
-- harness, and chopsticks states its limits in its own FAQ ("What is mocked?
-- … mocked tx pool / no real block finalization / mocked inherents / simulated
-- XCM channels"). Those four, plus the two this project adds — the scheduler
-- injection is OURS and models ENACTMENT rather than approval, and runtime
-- CONSTANTS cannot be changed at all — are the `not_covered` this tier ships on
-- every response. They are not a caveat about a corner: a diff that looks
-- authoritative while the inherents were mocked is the failure this tier is most
-- likely to produce.

-- ---------------------------------------------------------------------------
-- 1. Three columns stop being NOT NULL, because Tier 2 genuinely has no value
--    for them and writing a placeholder would be a claim.
-- ---------------------------------------------------------------------------

-- `result_xcms_version` is an argument of `DryRunApi::dry_run_call`. A fork does
-- not call that API at all, so a fork row has no such version. Writing 0 would
-- read as XCM v0, which is a real version and a wrong answer.
alter table sim.simulation_results
    alter column xcm_version drop not null;

-- Same reasoning, one step further: `api_version` records WHICH DryRunApi
-- version answered. On a fork row no runtime API was called, and recording the
-- version the runtime happens to declare would invite a reader to believe it was
-- used. NULL means "this tier does not call a runtime API".
--
-- `metadata_version` deliberately STAYS not null: a fork uses our archived
-- metadata to encode the origin, to build the injected storage, and to decode
-- both the events and the storage diff — so it is lineage in the Invariant 3
-- sense here just as much as on Tier 1.
alter table sim.simulation_results
    alter column api_version drop not null;

-- `forwarded_xcms` is produced by `dry_run_call`. This tier does not ask, and an
-- empty array would read as "this call queues no messages" — which is a claim
-- about the call, where the truth is a fact about the tier. NULL = not asked.
--
-- The consequence is carried into the API: `forwarded_attribution` is attached
-- ONLY to rows that have a forwarded list, because a field that describes a
-- column belongs on rows that have the column. That is the shared-`not_covered`
-- lesson (slices 3, 4 and 5 each shipped one line that was false on its second
-- consumer) applied before it can happen a fifth time.
alter table sim.simulation_results
    alter column forwarded_xcms drop not null;

comment on column sim.simulation_results.sim_version is
    'Our interpreter''s version. It names a DIFFERENT code path per tier — '
    'adapter_substrate::dryrun::DRY_RUN_VERSION on a dry_run row, '
    'adapter_substrate::fork::FORK_VERSION on a fork row — because the two tiers '
    'interpret different bytes. `tier` disambiguates; do not compare the number '
    'across tiers.';

-- ---------------------------------------------------------------------------
-- 2. The Tier 2 columns. All nullable, all NULL on a dry_run row.
-- ---------------------------------------------------------------------------
alter table sim.simulation_results
    -- The resolved override set: one entry per injected storage key, each
    -- carrying the spec as the caller wrote it, what it resolved to
    -- (`Assets.Account(1337, 0x…)`), the raw key, the injected value, and the
    -- value the REAL chain held there at this block. NULL = no overrides were
    -- injected, i.e. this row is a faithful fork and not a counterfactual.
    add column overrides jsonb,
    -- blake2b-256 over the canonical override encoding, folded into input_hash.
    -- Recorded separately so a reader can verify the fold without re-deriving
    -- the whole request, and so the CHECK below can enforce it.
    add column override_hash text,
    -- The consequence: storage that the DISPATCH changed, decoded against the
    -- runtime's own metadata. One entry per changed key:
    --   { key, pallet, item, args, args_readable, value_before, value_after,
    --     decoded_before, decoded_after, deleted, from_override }
    -- `from_override: true` marks a key this run itself fabricated, whose
    -- `value_before` is therefore the INJECTED value and not what the chain held
    -- — the one place a diff entry could read as history while being a
    -- consequence of the counterfactual.
    add column storage_diff jsonb,
    add column storage_diff_count integer,
    -- decoded    — the harness returned a raw diff and every entry was named
    --              against the runtime's metadata (arguments may still be
    --              unknown per entry; see below)
    -- undecodable— a raw diff came back in a shape this version cannot read. The
    --              bytes are archived; nothing is guessed.
    -- unavailable— the harness does not expose a diff at all (the plugin is
    --              absent or disabled). NOT the same as an empty diff, and the
    --              distinction is the whole reason this column is not a boolean:
    --              "we did not look" and "nothing changed" must never be one
    --              value. Same rule as `skipped_unanchorable` vs a zero balance.
    -- refused    — the harness HAS a diff method and it failed on this block.
    --              Added at verification, because it is the ordinary case on a
    --              PARACHAIN rather than an exceptional one: chopsticks'
    --              `dev_newBlock` builds a block WITHOUT `set_validation_data`
    --              (it mocks parachain state directly), and its own
    --              `dev_runBlock` re-applies extrinsics for real, so the
    --              runtime traps. The same harness re-runs a REAL Asset Hub
    --              block without complaint, so this is about the two primitives
    --              not composing, not about the chain. Deliberately NOT folded
    --              into `unavailable`: "this build has no plugin" and "the
    --              plugin declined this block" are different facts, and a row
    --              that blurred them would send somebody to reinstall a tool
    --              that is working. The events and the dispatch verdict are
    --              read BEFORE the diff is asked for, so a `refused` row still
    --              carries a real answer — which is why this is a status and
    --              not an error.
    add column diff_status text,
    -- The hash of the block the FORK built. It exists on the fork and nowhere
    -- else; no canonical chain has it, `core.blocks` never receives it, and a
    -- block explorer will not find it. Named `built_` rather than `block_` for
    -- exactly that reason.
    add column built_block_hash text,
    -- Lineage for a tool we do not control (Invariant 3, applied outward): the
    -- chopsticks version, the node version, the exact command, the endpoint it
    -- forked from, the sqlite cache it used, the list of surfaces it mocks, and
    -- the archived harness log.
    --
    -- IT IS RECORDED AND NOT KEYED, and the cost is stated rather than hidden: a
    -- cached fork answer may have been produced by a chopsticks version other
    -- than the one installed now. Folding it into `input_hash` would invalidate
    -- every cached answer on every upgrade of a tool that upgrades often, for a
    -- risk the row already makes visible. Unlike a Tier 1 row, a fork row is NOT
    -- re-derivable from archived bytes alone — re-deriving it needs a re-run —
    -- so this is the only lineage that says which engine spoke.
    add column harness jsonb;

-- A counterfactual cannot be recorded without the hash that keeps it out of
-- another run's cache. Cheap, and it makes the argument at the top of this file
-- an integrity guarantee rather than a convention somebody has to remember.
alter table sim.simulation_results
    add constraint simulation_results_counterfactual_is_hashed
    check ((overrides is null) = (override_hash is null));

-- Dropping `not null` above loosened those two columns for EVERY row, not just
-- for fork rows — so the guarantee Tier 1 had is restored where it is still
-- true. A dry_run row without a `result_xcms_version` or a DryRunApi version is
-- a row that cannot say what it asked, and 0014 was right to forbid it.
alter table sim.simulation_results
    add constraint simulation_results_dry_run_keeps_its_versions
    check (tier <> 'dry_run' or (xcm_version is not null and api_version is not null));

-- ---------------------------------------------------------------------------
-- 3. THE QUEUE
-- ---------------------------------------------------------------------------
-- A row here is a REQUEST, and it is mutable — which makes it the first table in
-- the `sim` schema that is not an immutable observation. That difference is the
-- reason it is a separate table rather than more columns: a result records what
-- a runtime answered and must never change; a job records what somebody asked
-- for and moves through states until it is answered.
create table sim.simulation_jobs (
    id            bigserial primary key,
    chain_id      text not null references core.chains (id),
    -- 'fork' today. In the key of nothing, because a job is identified by its
    -- id; carried so a worker can refuse a tier it cannot run rather than
    -- silently running a different one.
    tier          text not null,

    -- ------------------------------------------------------------------ the ask
    -- NULL = the chain's finalized head AT RUN TIME, resolved when the job runs
    -- and recorded on the result row. Deliberately not resolved at enqueue: a
    -- queued job that waits an hour should preview the state it actually runs
    -- against, and the result row's own `at_block_hash` is what pins it.
    at_height     bigint,
    -- The SCALE-encoded RuntimeCall, verbatim. Bytea rather than hex because it
    -- is bytes, and because the result row's `call_hash` is derived from exactly
    -- these bytes.
    call_bytes    bytea not null,
    call_hash     text not null,
    -- The origin expression as typed ("root", "Origins:MediumSpender",
    -- "signed:13UVJ…"). Resolved against the runtime's own type registry when
    -- the job runs, never here — the same rule Tier 1 follows, and for the same
    -- reason: the resolution is only true of a particular runtime.
    origin_spec   text not null,
    -- The override specs AS WRITTEN, before resolution — e.g.
    -- ["Assets.Account(1337,0x…)={\"balance\":\"…\"}"]. `[]` is the ordinary
    -- shape here (a job with no overrides is a faithful fork), which is why this
    -- column may be an empty array while `simulation_results.overrides` may not:
    -- there, NULL carries the meaning; here, the specs are a list of strings and
    -- an empty list of strings is simply empty.
    overrides     jsonb not null default '[]'::jsonb,
    -- Who asked. 'cli' | 'follower' | a named drill. NOT a user identity: this
    -- project has no accounts, and a column that looked like one would invite a
    -- reader to put one there.
    requested_by  text,
    note          text,

    -- ---------------------------------------------------------------- the queue
    -- queued  — waiting for a worker
    -- running — leased by a worker (see lease_expires_at)
    -- done    — a result row exists; result_* point at it
    -- failed  — the run was attempted and did not produce a result. `error`
    --           carries why, and the harness log is archived under the raw store
    --           whenever one was produced.
    -- refused — the request cannot be run at all (an origin this runtime does not
    --           have, call bytes that are not a RuntimeCall, a chain with no
    --           configured fork endpoint). Separated from `failed` because a
    --           refusal is DETERMINISTIC and retrying it is guaranteed to waste
    --           a Node process; a failure may be transient.
    status        text not null,
    attempts      integer not null default 0,
    -- ONE by default, and that is a judgement rather than a placeholder. A fork
    -- run costs a process, a WASM execution and a burst of RPC against a public
    -- endpoint that this project has already had one backfill killed by. Most
    -- Tier 2 failures are deterministic (bad bytes, absent origin, unsupported
    -- runtime), so retrying by default would spend real resources on a job that
    -- cannot succeed. Raise it per job when the failure is known to be transient.
    max_attempts  integer not null default 1,
    -- A crashed worker must not wedge a job forever. A lease that has expired is
    -- reclaimable, which is the only reason `running` is not a terminal state.
    lease_expires_at timestamptz,
    worker        text,
    error         text,

    -- --------------------------------------------------------------- the answer
    -- The full key of the result row, once there is one. Nullable, and the FK is
    -- satisfied while they are (Postgres MATCH SIMPLE: a composite FK with any
    -- NULL column is not enforced), so a queued job needs no placeholder.
    --
    -- It is a POINTER, not the answer: the answer is immutable and lives in
    -- `simulation_results`, and copying any of it here would create the second
    -- copy without lineage — verbatim the argument that killed
    -- `treasury.consolidated_position`, `graph.cross_chain_operations`, the
    -- stored forwarded-attribution and a `logical_assets` join table.
    result_at_block_hash text,
    result_input_hash    text,

    created_at    timestamptz not null default now(),
    started_at    timestamptz,
    finished_at   timestamptz,

    foreign key (chain_id, result_at_block_hash, result_input_hash, tier)
        references sim.simulation_results (chain_id, at_block_hash, input_hash, tier)
);

-- ONE index, and it serves both readers this slice has.
--
-- There is exactly ONE reader: the claim statement, which asks for the oldest
-- row that is either queued-with-attempts-left or running-with-an-expired-lease,
-- ordered by (created_at, id). STATED PRECISELY, because 0013's "ONE index
-- probe" comment had to be corrected for exactly this kind of overclaim: the
-- `OR` in that predicate means Postgres will NOT get an ordered index scan out
-- of this — it covers the two live statuses and no more, and the sort happens
-- over that small set. There is deliberately no separate lease sweep and no
-- second index for one: a Tier 2 job costs a Node process, so this table is
-- bounded by how much compute exists rather than by how much traffic arrives,
-- and at that cardinality a second index costs more on every write than it could
-- save on a poll.
--
-- Partial on the two live states, so the index does not carry the terminal rows
-- that accumulate forever and that neither reader ever looks at.
create index simulation_jobs_queue_idx
    on sim.simulation_jobs (status, created_at, id)
    where status in ('queued', 'running');

comment on table sim.simulation_jobs is
    'Tier 2 work requests. The only mutable table in the sim schema: a result '
    'records what a runtime answered and never changes; a job records what '
    'somebody asked for and moves through states until it is answered.';
