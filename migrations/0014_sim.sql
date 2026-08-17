-- 0014_sim: Tier 1 simulation results (Phase 3, slice 1).
--
-- ARCHITECTURE §11 splits simulation into two tiers behind one service:
--   Tier 1 — instant: DryRunApi::dry_run_call / dry_run_xcm against a live RPC,
--            no fork. "What would this call do, right now, under this origin."
--   Tier 2 — full fork: chopsticks jobs, state diffs, queued and resource-capped.
-- This migration ships ONLY what Tier 1 earns. The `simulation_jobs` table
-- ARCHITECTURE names is a QUEUE, and Tier 1 has no queue — it is one request and
-- one response — so creating it now would be a table with no writer, which is the
-- unearned-schema mistake 0008/0009 both had to correct. It lands with Tier 2.
--
-- A SIMULATION RESULT IS AN OBSERVATION, NOT A PROJECTION. Same category as
-- `balances.balance_anchors` and `gov.voting_anchors`: it records what a specific
-- runtime, at a specific state, answered when asked a specific question. It is
-- therefore immutable and inserted with `on conflict do nothing` — nothing about
-- a past answer can be improved by asking again, and if a re-run ever DID differ,
-- overwriting would destroy the evidence that it did.
create schema if not exists sim;

create table sim.simulation_results (
    chain_id      text not null references core.chains (id),

    -- THE PRIMARY KEY IS THE BLOCK HASH, NOT THE HEIGHT, and that is deliberate.
    -- A dry run is a pure function of (state, input), and the thing that names a
    -- state exactly is a block hash. Two forks at one height are two different
    -- states and must be able to hold two different answers; keying by height
    -- would force one of them to be discarded or, worse, silently kept as the
    -- answer for the other. `at_height` is carried beside it for ordering and
    -- for human readability, never as identity.
    at_block_hash text not null,
    -- blake2b-256 of the EXACT parameter bytes sent to the runtime API — origin
    -- ++ call ++ result_xcms_version, i.e. every input that can change the
    -- answer, in the encoding the chain actually received. Deriving the key from
    -- the wire bytes rather than from a struct of our own fields means a future
    -- field we forget to include in a hand-built key cannot silently collide.
    input_hash    text not null,
    at_height     bigint not null,

    -- 'dry_run' today; 'fork' when the chopsticks tier lands.
    --
    -- IT IS PART OF THE PRIMARY KEY, and that is not decoration. Two tiers can
    -- answer the same (state, input) and they are NOT interchangeable — the fork
    -- models scheduled dispatch and the dry run does not. Left out of the key,
    -- the first Tier 2 run at a state Tier 1 had already touched would hit
    -- `on conflict do nothing`, be silently discarded, and then be SERVED as a
    -- cache hit: a fork simulation that never forked. Cheap to get right while
    -- there is one writer; expensive later.
    --
    -- The READ side does not filter on it yet, and deliberately so: every row is
    -- 'dry_run' today, so a tier parameter on the API would be a knob with
    -- nothing to select — the unearned-surface mistake, one column over. It
    -- ships with Tier 2, which is also what makes it selective.
    tier          text not null,

    -- ---------------------------------------------------------------- the ask
    -- blake2b-256 of the call bytes. This is the join to gov.preimages
    -- (proposal_hash) and to gov.whitelisted_calls (call_hash), which is what
    -- makes "referendum → what would this DO" one query rather than a pipeline.
    call_hash     text not null,
    call_summary  text,
    -- The caller's origin expression, verbatim ("root", "signed:13UVJ…",
    -- "Origins:MediumSpender"), beside the variant path it resolved to in the
    -- runtime's own type registry. Both are kept because the expression is what
    -- a person types and the path is what the chain was asked.
    origin_spec   text not null,
    origin_json   jsonb not null,
    -- `result_xcms_version`: the XCM version the returned programs were rendered
    -- in. It changes the BYTES of the answer, so it is part of the input_hash and
    -- is recorded here in readable form.
    xcm_version   integer not null,

    -- ------------------------------------------------------------- the answer
    -- executed        — the runtime dispatched the call and it succeeded
    -- dispatch_failed — the runtime dispatched the call and it FAILED
    -- api_error       — the API itself refused (Unimplemented /
    --                   VersionedConversionFailed); no dispatch was attempted
    --
    -- 'dispatch_failed' is a RESULT, not an error of ours, and it is frequently
    -- the most valuable one this table holds — "this referendum would fail with
    -- BadOrigin" is the answer somebody needed before voting. Same doctrine as
    -- slice 9's whitelist flow, where "dispatched" had to stop meaning
    -- "succeeded"; here the outer Result being Ok must never be read as the call
    -- having worked. There is deliberately no 'undecodable' status: a response we
    -- cannot decode halts the runner loudly and writes NO row, because filing
    -- bytes we could not read as if they were a result is the one outcome that
    -- would make this table untrustworthy. The bytes are archived either way.
    status        text not null,
    dispatch_ok   boolean,
    -- The dispatch error, resolved through the runtime's own error metadata
    -- where possible: {"error":"assets.NoAccount", "raw":{…}} rather than an
    -- opaque module index and byte array.
    dispatch_error jsonb,
    -- Events named exactly as core.events names them ("balances.Transfer"), so a
    -- simulated outcome and an indexed outcome can be compared field by field
    -- rather than eyeballed across two vocabularies.
    emitted_events jsonb not null,
    event_count    integer not null,
    -- The XCM this call would execute locally, and the programs the runtime
    -- reported alongside it. `forwarded_xcms` is the seam the XCM slice reads.
    --
    -- CORRECTED AFTER MEASURING (this comment first claimed it was "the SENDING
    -- side of a journey, before the journey exists" — do not read it that way):
    -- the list is NOT reliably attributable to the simulated call. On the
    -- Polkadot relay a `system.remark` under Root, which queues nothing, comes
    -- back with 64 destinations and 74 messages — real in-flight downward
    -- messages, byte-identical across two different calls and across two
    -- different blocks, i.e. the relay's router enumerating every parachain's
    -- existing queue. Asset Hub returns an empty list for the same shape of
    -- call. Store it verbatim (it is what the runtime said), read it as
    -- "messages present at this state", and get attribution by differencing
    -- against a no-op run at the same state — a later slice, not a claim here.
    -- The API's not_covered says the same thing to callers.
    local_xcm      jsonb,
    forwarded_xcms jsonb not null,
    -- The whole decoded CallDryRunEffects, schema-on-read. The columns above are
    -- the query-critical projection of it; this is the part no future question
    -- has to re-run a simulation to answer.
    effects        jsonb not null,
    note           text,

    -- ------------------------------------------------------------ lineage (§3)
    spec_version  bigint not null,
    -- The DryRunApi version the runtime DECLARED in its RuntimeVersion.apis.
    -- v1 and v2 differ in arity (v2 added result_xcms_version), so a row's answer
    -- is only interpretable beside the version that produced it.
    api_version   integer not null,
    -- WHICH metadata version's type registry built the request and read the
    -- answer. A chain can offer v15 and v16 for one spec_version, and the same
    -- bytes read against a different registry are a different claim — so this is
    -- lineage in the Invariant 3 sense, not a curiosity.
    metadata_version integer not null,
    -- Our runner's own version: bump it when the interpretation of a response
    -- changes, and every row below it is rebuildable from the archived bytes.
    sim_version   integer not null,
    -- The archived RESPONSE bytes. Worth more here than anywhere else in the
    -- project: chain state is PRUNED, so a dry run at block H can never be
    -- reproduced once H falls out of the archive window. These bytes are the only
    -- surviving evidence of that answer, and a later sim_version re-derives from
    -- them instead of asking a chain that has moved on.
    raw_location  text not null,
    observed_at   timestamptz not null default now(),

    primary key (chain_id, at_block_hash, input_hash, tier)
);

-- The one earned index: "every recorded simulation of this call on this chain,
-- newest first" — what the referendum page asks when it renders [Simulate].
-- Nothing else is indexed; 0008/0009/0010's rule is that an index ships with the
-- query that reads it.
--
-- STATED PRECISELY, because 0013's "ONE index probe" comment had to be corrected
-- for exactly this kind of overclaim: this index covers the WHERE clause and the
-- leading `at_height desc`, not the whole ORDER BY. The reader breaks ties on
-- `input_hash, at_block_hash` (it must — two forks at one height share a height
-- AND an input hash, and an unstable order there would make `limit` return
-- different rows from the two backends), so Postgres adds an incremental sort
-- over the tied group. That group is tiny by construction; the index is doing
-- the work that matters.
create index simulation_results_call_idx
    on sim.simulation_results (chain_id, call_hash, at_height desc);
