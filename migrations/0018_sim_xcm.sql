-- 0018_sim_xcm: the RECEIVING side of a Tier 1 preview (Phase 3, slice 5).
--
-- 0014 recorded what a CALL would do on the chain that dispatches it. This
-- records what an XCM PROGRAM would do on the chain that receives it —
-- `DryRunApi::dry_run_xcm(origin_location, xcm)`, the other half of the same
-- runtime API, and the answer to the question 0014's own `not_covered` had to
-- refuse: "forwarded_xcms is not what the DESTINATION would do".
--
-- IT IS A SEPARATE TABLE, and the reason is the subject rather than the columns.
-- The key shape is identical — (chain, block hash, input hash, tier) — but a row
-- here is a PROGRAM FROM A LOCATION, not a call under an origin, and its answer
-- is an XCM `Outcome` with three states rather than a dispatch `Result` with
-- two. Folded into `sim.simulation_results` these rows would leave `call_hash`,
-- `origin_spec`, `origin_json`, `dispatch_ok`, `dispatch_error` and `local_xcm`
-- empty — six columns and the table's ONE earned index
-- (`simulation_results_call_idx`, on `call_hash`) — while the columns that
-- matter here would have to be added nullable beside them. A table whose only
-- index cannot be populated by half its rows has been asked to hold two things.
--
-- WHAT IT SHARES IS THE DOCTRINE, not the schema: an immutable observation of
-- what one runtime, at one state, answered when asked one question; inserted
-- `on conflict do nothing`; the response bytes archived before they are read, so
-- a later `sim_version` re-derives from evidence rather than from a chain whose
-- state has been pruned.
create table sim.xcm_simulations (
    chain_id      text not null references core.chains (id),

    -- The state, named exactly (0014's argument verbatim: two forks at one
    -- height are two states, and a height key would force one answer to stand
    -- for the other).
    at_block_hash text not null,
    -- blake2b-256 of the exact parameter bytes: origin_location ++ xcm.
    --
    -- There is no `result_xcms_version` on this method — dry_run_xcm's signature
    -- is IDENTICAL in DryRunApi v1 and v2 (verified against xcm-runtime-apis
    -- 0.4.0, 0.6.0 and 0.7.0: only dry_run_call changed) — so unlike 0014 there
    -- is no third input to fold in. The version of the PROGRAM itself is inside
    -- these bytes, because a VersionedXcm carries its own version tag.
    input_hash    text not null,
    at_height     bigint not null,
    -- 'dry_run' today; 'fork' when the chopsticks tier lands. Part of the key
    -- for the same reason it is in 0014's: a fork models scheduled dispatch and
    -- a dry run does not, so one must never be served as the other.
    tier          text not null,

    -- ---------------------------------------------------------------- the ask
    -- blake2b-256 of the encoded VersionedXcm. The join key for "every time this
    -- program was previewed", and the thing a caller can recompute from bytes
    -- they hold.
    program_hash  text not null,
    -- The instruction list, decoded — schema-on-read, same rendering every other
    -- decoded XCM in this project gets.
    program       jsonb not null,
    -- "WithdrawAsset → BuyExecution → DepositAsset" — the list view's text, and
    -- the fastest way for a human to see whether the right thing was previewed.
    program_summary text,
    -- WHO the receiving chain is told this came from. It is a Location in the
    -- RECEIVER's frame, which is the mirror image of the destination the sender
    -- addressed: Asset Hub sending to `{parents:1, X1[Parachain(2034)]}` arrives
    -- on Hydration as `{parents:1, X1[Parachain(1000)]}`. Getting this wrong
    -- does not fail loudly — it previews a message from the wrong sender, and
    -- barriers and origin conversion are exactly what the receiving side turns
    -- on — so the token beside it records what it was MEANT to be.
    origin_location jsonb not null,
    -- 'parent' | 'para:1000' | 'here' — the same vocabulary
    -- `xcm.messages.counterparty` uses, so a previewed leg and an observed one
    -- are comparable without a translation layer.
    origin_ref    text not null,

    -- ------------------------------------------------------------- the answer
    -- complete    — the program ran to the end (`Outcome::Complete`)
    -- incomplete  — it STARTED and stopped partway (`Outcome::Incomplete`); from
    --               XCM v5 the payload names the failing instruction's index
    -- not_started — `Outcome::Error`: execution never began. THE UPSTREAM
    --               VARIANT IS NAMED `Error` AND THAT NAME IS MISLEADING HERE —
    --               it is not an error of ours and not an API failure, it is the
    --               single most valuable answer this table can hold, because a
    --               message rejected at the barrier is precisely the failure a
    --               sender cannot see from their own chain. Renamed on the way
    --               in so nobody reads it as "the request was bad".
    -- api_error   — the runtime API itself refused (Unimplemented /
    --               VersionedConversionFailed); nothing was attempted at all.
    --
    -- There is deliberately no boolean here. `complete` and `incomplete` are the
    -- same distinction `xcm.messages.success` carries for OBSERVED messages
    -- (Outcome::Incomplete is what slice 2 measured on a real arrival), and
    -- flattening three execution states into two would erase the one this
    -- endpoint exists to show.
    status        text not null,
    -- Weight reported as used. Present for complete and incomplete, absent for
    -- not_started — because nothing ran.
    weight_used   jsonb,
    -- The XCM error, kept in the shape the runtime rendered it: a bare `Error`
    -- on XCM v4, `InstructionError {index, error}` on v5. Not normalised into
    -- one shape, because the index is real information on v5 and inventing one
    -- for v4 would be a guess.
    xcm_error     jsonb,
    -- Named exactly as core.events names them ("balances.Transfer"), so a
    -- previewed arrival and an indexed one compare field by field.
    emitted_events jsonb not null,
    event_count   integer not null,
    -- What THIS chain would queue onward. The recursion is real and is the point
    -- of the whole slice: a hop's forwarded list is the next hop's program.
    --
    -- It carries the SAME ambient-traffic problem the call side does, and is
    -- given the same fix rather than a caveat: `baseline_input_hash` points at a
    -- run of the EMPTY PROGRAM from the same origin at the same state, and the
    -- attributable set is this row's list minus that one's. Shipping the fix on
    -- only one of the two tables would have left the identical defect one hop
    -- along the journey it exists to follow.
    forwarded_xcms jsonb not null,
    -- input_hash of the empty-program run at the same (chain_id, at_block_hash,
    -- tier). NULL = no baseline, so forwarded_xcms means "messages present",
    -- never "this program would send these". An empty program is its own
    -- baseline and points at itself.
    baseline_input_hash text,
    -- The whole decoded XcmDryRunEffects, so no future question has to re-run a
    -- simulation whose state no longer exists.
    effects       jsonb not null,
    note          text,

    -- ------------------------------------------------------------- provenance
    -- WHERE THIS PROGRAM CAME FROM, when it came from somewhere: the call
    -- simulation whose forwarded list held it, and its position in that list.
    -- NULL when a caller supplied the program by hand, which is a different
    -- claim and must not be indistinguishable from a stitched one.
    --
    -- This is the stitch — referendum → what it does here → what it does over
    -- there — and it is stored rather than derived because it is not derivable:
    -- the program bytes alone do not say which call queued them, and two
    -- identical programs from two different calls are genuinely two facts.
    -- BOTH indices, because a destination carries a LIST of messages: the
    -- position in `forwarded_xcms` and the position within that entry's
    -- messages. One index would name a destination and leave which of its
    -- messages was previewed to be guessed at.
    source_chain_id        text references core.chains (id),
    source_at_block_hash   text,
    source_input_hash      text,
    source_forwarded_index integer,
    source_message_index   integer,

    -- ------------------------------------------------------------ lineage (§3)
    spec_version  bigint not null,
    -- The DryRunApi version the runtime declared. Recorded even though
    -- dry_run_xcm's signature did not change between v1 and v2, because "the
    -- signature was the same in both" is a fact about today's upstream and the
    -- row has to be interpretable without it.
    api_version   integer not null,
    metadata_version integer not null,
    sim_version   integer not null,
    raw_location  text not null,
    observed_at   timestamptz not null default now(),

    primary key (chain_id, at_block_hash, input_hash, tier)
);

-- "every recorded preview of this program on this chain, newest state first" —
-- what a program hash pasted into search, and the program page, both ask.
--
-- STATED PRECISELY, the way 0014's own comment had to be corrected after 0013
-- over-claimed: this covers the WHERE clause and the leading `at_height desc`,
-- NOT the whole ORDER BY. The reader breaks ties on `input_hash, at_block_hash,
-- tier` — it must, or `limit` returns different rows from the Memory and Pg
-- backends — so Postgres adds an incremental sort over the tied group. That
-- group is tiny by construction; the index is doing the work that matters.
create index xcm_simulations_program_idx
    on sim.xcm_simulations (chain_id, program_hash, at_height desc);

-- "the legs previewed from THIS call simulation" — the stitch read, and the only
-- reason `source_input_hash` is a column rather than a note. Partial, because
-- hand-supplied programs have no source and would otherwise sit in the index
-- doing nothing.
--
-- IT SERVES THE WHERE CLAUSE ONLY. That reader orders by the SENDER's own list
-- positions (`source_forwarded_index, source_message_index, …`), which this
-- index does not carry, so the sort is done in memory over one call's legs — a
-- handful of rows. Said out loud rather than left for someone to discover in an
-- EXPLAIN, because the alternative is an index that looks like it covers a
-- query it does not.
create index xcm_simulations_source_idx
    on sim.xcm_simulations (source_chain_id, source_input_hash)
    where source_input_hash is not null;

-- ---------------------------------------------------------------------------
-- THE ATTRIBUTION FIX 0014 PROMISED, and it is a one-column change because the
-- answer was already a row in that table.
--
-- 0014's own comment records the measurement: on the Polkadot relay a Root
-- `system.remark` — a call that queues nothing — comes back with 64 destinations
-- carrying 74 real in-flight messages, byte-identical across two different calls
-- and across two different blocks. The list is a property of the STATE (the
-- relay's router enumerating every parachain's existing downward queue), not of
-- the call, and 0014 said the fix was "differencing against a no-op run at the
-- same state — a later slice, not a claim here". This is that slice, and the
-- differencing is no longer optional: feeding `forwarded_xcms` into dry_run_xcm
-- without it would preview 74 unrelated messages as if the referendum had sent
-- them.
--
-- THE BASELINE IS AN ORDINARY SIMULATION, which is what makes this cheap:
-- `system.remark()` with an empty payload, under Root, at the SAME block hash.
-- It is prepared, dispatched, archived and recorded exactly like any other row,
-- so it is cached by the same key and a second simulation at that state costs no
-- extra dispatch at all. On Asset Hub, where the list is empty, the baseline is
-- an empty list and the difference is a no-op — the machinery costs one cached
-- row and changes nothing, which is the correct outcome for a chain that never
-- had the problem.
--
-- The attribution ITSELF is deliberately not stored. It is the difference of two
-- `forwarded_xcms` columns that both already carry lineage, so a third copy
-- would be the one without it — verbatim the argument that killed
-- `treasury.consolidated_position` in Phase 2 slice 6 and
-- `graph.cross_chain_operations` in Phase 3 slice 3. The API computes it per
-- request from the two rows.
--
-- NULL means "no baseline was run at this state", which is the honest reading
-- for every row recorded before this migration. A no-op is ITS OWN baseline and
-- points at itself, so a subject that happens to be the same call as the
-- baseline still has a link, and differencing a list against itself gives the
-- empty set — which is the right answer for a call that queues nothing.
alter table sim.simulation_results
    add column baseline_input_hash text;

comment on column sim.simulation_results.baseline_input_hash is
    'input_hash of the no-op run at the same (chain_id, at_block_hash, tier) whose '
    'forwarded_xcms is the ambient queue at this state. The attributable set is '
    'this row''s forwarded_xcms minus that one''s. NULL = no baseline recorded, so '
    'forwarded_xcms means "messages present", never "this call would send these".';
