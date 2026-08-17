-- 0012_gov_whitelist: the Fellowship's fast path, and whether it worked
-- (Phase 2, slice 9).
--
-- WHAT THE FLOW IS. A Fellowship referendum on Collectives authorizes a call
-- by HASH; XCM carries that decision to Asset Hub; `whitelist.whitelist_call`
-- records the hash; a PUBLIC referendum on track 1 (`whitelisted_caller` — a
-- short, low-support track that exists only because the call was already
-- vetted) then dispatches it with Root origin. Three legs, two chains, and
-- the join between them is a 32-byte CALL HASH rather than any id.
--
-- WHY IT GETS ITS OWN TABLE RATHER THAN RIDING gov.referendum_events: the
-- subject here is a call hash. It has no referendum id, no track, and no
-- class, so it cannot be part of that table's key. It is also not one
-- referendum's property — a whitelisted call is authorized by a referendum on
-- ONE chain and dispatched by a different referendum on ANOTHER, and the
-- stitch between them is a read-time join (see the API note below), not a
-- column, precisely because the two referenda are routinely indexed in either
-- order.
--
-- THE FINDING THAT SHAPED THIS SCHEMA, verified against pallet-whitelist's own
-- source across all 51 published versions: **`WhitelistedCallDispatched` does
-- NOT mean the call worked.** `clean_and_dispatch` removes the whitelist
-- entry, unrequests the preimage, dispatches, and then emits the event
-- carrying the inner call's `DispatchResultWithPostInfo` — but it DISCARDS the
-- error and returns `Ok` to the extrinsic regardless. So a whitelisted call
-- that reverted still produces `system.ExtrinsicSuccess`, still consumes its
-- whitelist entry, and still charges the fee. The pallet's own test
-- (`test_whitelist_call_and_execute_failing_call`) asserts exactly that.
-- Reading the event's presence as success would report a failed runtime
-- upgrade as an enacted one. Hence TWO facts, never one: `kind='dispatched'`
-- says the attempt happened and storage was cleaned; `dispatch_ok` says
-- whether the call the Fellowship approved actually took effect. This is the
-- same doctrine as slice 7's "an announced payout is not a payout".
--
-- THE SECOND FINDING, which the projection must not paper over: **a
-- `CallWhitelisted` need never be followed by anything.** Every dispatch
-- failure fails the extrinsic with NO event at all, leaving the whitelist entry
-- in place. Which failures are reachable depends on WHICH dispatch call is
-- used, and the two differ — scoped here rather than blurred:
--   * `dispatch_whitelisted_call` (call_index 2) fetches the preimage by
--     (hash, len) and checks a weight witness, so it can fail with
--     `UnavailablePreImage` (whitelisting only REQUESTS the preimage — keyed by
--     hash alone — and does not require anyone to have noted it; a wrong
--     witness LENGTH also misses, because the FETCH is length-keyed),
--     `UndecodableCall` (bytes noted against an older runtime whose call
--     indices have since moved), or `InvalidCallWeightWitness`. That last one
--     can strand a call PERMANENTLY: the witness is fixed when the referendum
--     is submitted, so a later runtime upgrade making the call heavier means
--     the dispatch fails forever until someone re-submits.
--   * `dispatch_whitelisted_call_with_preimage` (call_index 3) carries the call
--     inline — it fetches nothing, decodes nothing and checks no witness — so
--     NONE of those three apply to it. Its only pre-dispatch failure is
--     `CallIsNotWhitelisted`.
-- Either way `status='whitelisted'` is a legitimate TERMINAL state, not a row
-- waiting to be completed, and nothing here may infer a dispatch that was never
-- announced.
--
-- Vocabulary coverage is 100% and provably stable: the Event enum has carried
-- exactly three named-field variants — CallWhitelisted, WhitelistedCallRemoved,
-- WhitelistedCallDispatched — with the same names, the same field order and
-- the same implicit indices 0/1/2 in every published version from 4.0.0 to
-- 48.0.0. The only diff in the pallet's history was `PreimageHash` → `T::Hash`
-- at 23.0.0, and both are H256, so the SCALE encoding never changed. An
-- unpublished `master` adds three variants APPENDED at 3/4/5 (deferred
-- dispatch via relayers); the mapper's loud-halt arm is what catches them, and
-- existing indices do not shift.

create table gov.whitelist_events (
    chain_id        text not null,
    block_height    bigint not null,
    event_index     integer not null,
    -- 0x-hex 32 bytes. Every one of the three variants carries `call_hash` as
    -- its first field, so unlike treasury pot events there is no subject-less
    -- case and no sentinel is needed.
    call_hash       text not null,
    -- whitelisted | removed | dispatched
    kind            text not null,
    -- NULL for whitelisted/removed; for `dispatched`, the `result` variant:
    -- true = Ok, false = Err. See the header — this is the column that
    -- separates "was dispatched" from "worked".
    dispatch_ok     boolean,
    -- The decoded DispatchError, kept whole (schema-on-read) when dispatch_ok
    -- is false. NEVER decode its variant indices by hand: the enum grew by
    -- APPEND within this pallet's lifetime (RootNotAllowed at sp-runtime
    -- 24.0.0, Trie at 40.0.0) but grew by INSERTION before it (TooManyConsumers
    -- at 5.0.0), so it is not append-only by policy. Decode it from
    -- block-correct metadata, which is what the pipeline already does.
    -- Note `DispatchError::Other` carries `#[codec(skip)]` on its payload, so
    -- a failure of that shape gives no error text on chain at all.
    dispatch_error  jsonb,
    data            jsonb not null,             -- full event fields
    runtime_version bigint not null,            -- lineage
    mapper_version  integer not null,           -- lineage (bump = rebuild)
    primary key (chain_id, block_height, event_index)
) partition by list (chain_id);

create table gov.whitelist_events_default
    partition of gov.whitelist_events default;

-- The ONE earned index: the read path is "everything that ever happened to
-- this hash", and the PK is ordered by height, not by hash. A whitelisted call
-- may be whitelisted, removed and re-whitelisted, so this is a real timeline
-- rather than a point lookup.
create index whitelist_events_hash_idx
    on gov.whitelist_events (chain_id, call_hash, block_height, event_index);

create table gov.whitelisted_calls (
    chain_id        text not null,
    call_hash       text not null,
    -- whitelisted | removed | dispatched
    --
    -- No 'unknown' placeholder, and it is worth saying why this table does not
    -- need migration 0006's device: all three events are status-bearing, so
    -- there is no info-only fact that could arrive first. Out-of-order replay
    -- is still handled — by the (status_height, status_event_index) guard
    -- below — it just never has to invent a state it has not seen.
    status          text not null,
    -- The dispatch outcome gets its OWN coordinate rather than riding the
    -- status one. Slice 5 learned this the hard way with payment_id: a hash
    -- that is dispatched and then whitelisted AGAIN moves `status` backwards to
    -- 'whitelisted', and if dispatch_ok travelled with the status guard the
    -- earlier outcome would either be erased or, worse, re-attached to the new
    -- attempt. Later dispatch wins; the full history is in the fact table.
    dispatch_ok     boolean,
    dispatch_error  jsonb,
    dispatch_height bigint,
    dispatch_event_index integer,
    -- when this hash was first seen in ANY whitelist event on this chain
    first_seen_height  bigint not null,
    -- the coordinate of the most recent CallWhitelisted, which is the one the
    -- authorizing Fellowship referendum has to be found near
    whitelisted_height bigint,
    whitelisted_event_index integer,
    status_height      bigint not null,         -- ordering guard
    status_event_index integer not null,
    runtime_version bigint not null,
    mapper_version  integer not null,
    updated_at      timestamptz not null default now(),
    primary key (chain_id, call_hash)
);

-- No secondary index on the projection, deliberately (the rule 0008/0009/0010
-- established: an index ships WITH its reader or not at all). The PK prefix
-- `chain_id` serves the list query, and the row count here is in the dozens
-- for the network's entire history — the whitelist path is used a handful of
-- times a year, not a handful of times a block. Add an ordering index the day
-- a query needs one, and not before.
--
-- REBUILD RULE: every column here is a pure function of gov.whitelist_events
-- (nothing accumulates, unlike treasury.bounties.paid_out), so the projection
-- alone may be truncated and re-derived with `whitelist-range`. That is a
-- deliberate difference from 0011 and the reason this table is safe to rebuild
-- in isolation.
