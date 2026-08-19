-- 0025_broker: the coretime ENTITLEMENT half (Phase 3, slice 13).
--
-- 0024 shipped OCCUPANCY — what each core actually did, from the relay's own
-- candidate events — and said in its own header that the entitlement half "needs
-- a chain registered and backfilled and is its own slice". This is that slice.
-- ROADMAP's coretime bullet is the reason both exist: "the comparison —
-- entitlement purchased vs. occupancy realized — is the claim nobody else can
-- make. NEITHER HALF ALONE IS THE PRODUCT."
--
-- ============================================================================
-- EVERY DECISION BELOW IS A MEASUREMENT. Slice 13's prep pass, run against the
-- live Coretime chain (spec 2003002) and against slice 11's own indexed relay
-- window 32613537-32614536. No claim here is inherited from upstream source: the
-- prep read `pallet-broker` 0.6.0-0.18.0 to know what to look FOR, and then
-- measured what the runtime actually declares — which turned out to be 37
-- variants where the pinned upstream sources the prep read (0.6.0-0.18.0)
-- list 32.
-- ============================================================================
--
-- ---------------------------------------------------------------------------
-- THE JOIN IS CLEAN, MEASURED IN BOTH DIRECTIONS, WITH ZERO EXCEPTIONS
-- ---------------------------------------------------------------------------
-- This was the one measurement that could have changed the product rather than
-- the schema, and it held.
--
-- At the sale boundary (coretime block 3188739, relay parent 29053193) the
-- broker emits 97 `Broker.CoreAssigned`, each paired with a
-- `parachainsystem.UpwardMessageSent`; the relay then emits 97
-- `coretime.CoreAssigned` across relay 29053195-29053207. Measured:
--
--     set(broker cores) == set(relay cores), both 97 distinct, range 0..99,
--     min diff 0 / max diff 0 — NO OFFSET
--
-- and the broker's ascending core order is byte-for-byte the relay's arrival
-- order. The SDK's own comment says why there is no offset
-- (`polkadot-runtime-parachains::coretime::mod.rs`: "The broker pallet's
-- `CoreIndex` definition is `u16` but on the relay chain it's `struct
-- CoreIndex(u32)`"), and the conversion is a widening and nothing else.
--
-- The SEMANTIC half is exact too, against slice 11's own occupancy rows: all 100
-- `Broker.Workload` keys decoded at coretime block 4923417 (relay 32614537, ONE
-- BLOCK AFTER slice 11's window 32613537-32614536) give AGREE 47 cores / 34,690
-- candidates, DISAGREE 0 / 0, and no core in occupancy carries more than one
-- para. THE AGREEMENT RESTS ON ONE READING AT ONE HEIGHT: assignments were
-- stable across the window, but that stability is INFERRED from the absence of
-- relay `CoreAssigned` events in it rather than from a second reading. Independent
-- corroboration: `Broker.Status.core_count = 100` on the Coretime chain is
-- byte-identical to slice 11's `num_cores = 100` read from the RELAY's
-- `Configuration.ActiveConfig` — two chains, two storage items, one number.
--
-- ---------------------------------------------------------------------------
-- WHY `core_assignments` IS ITS OWN TABLE AND IS THE SEAM
-- ---------------------------------------------------------------------------
-- `Broker.CoreAssigned { core, when, assignment }` carries a RELAY BLOCK NUMBER
-- in `when` — always an exact multiple of 80 — so joining entitlement to
-- occupancy needs NO TIMESLICE ARITHMETIC AT ALL. That is the whole reason this
-- is a seam rather than a correlation problem: the two sides share a number line
-- the chain itself states.
--
-- One event expands to N rows because `assignment` is a
-- `Vec<(CoreAssignment, PartsOf57600)>`, so the PK carries an ordinal. Every
-- vector measured on live data has length 1 (see the interlacing note below),
-- but the shape is a vector and a schema that could not hold a second entry
-- would silently drop the first interlaced core the chain ever sells.
--
-- ---------------------------------------------------------------------------
-- WHAT THE RELAY NEVER LEARNS, AND WHY IT IS A COUNTED CASE RATHER THAN A
-- SILENT ONE
-- ---------------------------------------------------------------------------
-- `pallet-broker`'s tick converts a region's 80-bit `CoreMask` into the relay's
-- ratio as `mask.count_ones() * (57_600 / 80)`. THE BIT COUNT CROSSES; THE
-- PATTERN DOES NOT. So the relay knows "this task gets k/80 of this core" and
-- never "these particular slots", and two interlaced regions on one core
-- assigned to the SAME task are indistinguishable on the relay side forever.
--
-- MEASURED THREE INDEPENDENT WAYS, AND CURRENTLY EMPTY:
--   (a) all 100 `Workload` entries at 4923417 have an assignment vector of
--       length 1, a full 80-bit mask, no duplicate task, and no `Idle`;
--   (b) all 97 `CoreAssigned` at the boundary carry a vector of length 1 with
--       `PartsOf57600 = 57600` on every one (55 Task, 42 Pool);
--   (c) all 45 live `Regions` keys carry an 80-bit mask.
--
-- So `Interlaced` has never split a mask on this chain and the conversion
-- currently loses nothing. IT IS STILL A COUNTED CASE AND NOT A SILENT ONE,
-- because the day one region interlaces, two entitlements on one core become
-- indistinguishable and no later slice can recover the difference. `parts` is
-- stored for exactly that reason: it is the only surviving trace of the mask.
--
-- PARTITIONING *IS* EXERCISED and is a different thing: of 15 distinct region
-- `begin` values, 3 sit off the 5040-timeslice grid, i.e. regions split in TIME
-- by `Partitioned`. A time split keeps a full mask and one task per core at any
-- instant, so it costs the join nothing. Recorded because "interlacing is
-- unexercised" would otherwise read as "regions are never subdivided", which is
-- false.
--
-- THE UNATTRIBUTABLE SHARE IS NOT THE INTERLACING, IT IS THE POOL: 43 OF 100
-- CORES. A `Pool` core's time goes to whoever buys instantaneous coretime, so
-- its occupancy is not attributable to any purchaser BY CONSTRUCTION. Today that
-- costs nothing — no Pool core produced a single block in slice 11's window —
-- and the delta reader must say so rather than fold 43,000 pool slots into a
-- waste figure they are not part of.
--
-- ---------------------------------------------------------------------------
-- THE DURABLE IDENTITY IS THE TASK, NOT THE CORE INDEX
-- ---------------------------------------------------------------------------
-- At coretime 4919882 para 3428's sovereign renewed five cores and EVERY ONE
-- CHANGED INDEX: old_core 35 -> core 43, 36 -> 44, 37 -> 45, 40 -> 46, 41 -> 47.
-- So a core index identifies an entitlement only WITHIN ONE REGION, and a delta
-- keyed on core index alone silently follows a different tenant after every
-- sale. That is why `task_id` is promoted to a column on both tables below and
-- why the delta reader keys on (core, task, relay-block window).
--
-- ---------------------------------------------------------------------------
-- WHAT IS DELIBERATELY NOT HERE
-- ---------------------------------------------------------------------------
-- A `coretime.delta` table. The delta is (entitlement rows JOIN occupancy rows)
-- over a stated relay-block window, and both sides already carry lineage — so
-- materialising it would create the one copy WITHOUT lineage. That is verbatim
-- what killed `treasury.consolidated_position` (P2 slice 6),
-- `graph.cross_chain_operations` (P3 slice 3), the stored forwarded-attribution
-- (P3 slice 5) and a `logical_assets` join table (0019). It is computed per
-- request. Materialise it the day a LISTING surface needs it.
--
-- A `coretime.regions` table. A region is STATE with no event carrying its
-- current owner (`RegionRecord.owner` is in the storage VALUE, not in the id),
-- and this slice ships no reader for it. `Purchased`/`Transferred`/`Assigned`
-- land in `broker_events` with their region id inside `data`; the day something
-- reads ownership, it gets a table and its own dated readings.
--
-- Columns for the region id's three parts. `RegionId {begin, core, mask}` is a
-- STRUCT on the wire in every place that matters (measured: the packed `u128`
-- form appears in NO Broker event and NO Broker storage entry — it is the
-- nonfungible ItemId only), and `fork::StorageKeyIndex::describe` already reads
-- it back today. Promoting its parts would be three columns with no reader,
-- which is 0008/0009's rule read backwards.
create schema if not exists coretime;

-- ---------------------------------------------------------------------------
-- THE VOCABULARY, AS FACTS
-- ---------------------------------------------------------------------------
-- One `Broker.*` event, append-only, keyed by its own coordinate.
--
-- 6 OF THE 37 DECLARED VARIANTS FIRE; 31 DO NOT, and the zeroes are the finding.
-- Over 653 distinct coretime blocks (three contiguous windows plus one renewal
-- block), 1,599 deduped events: `CoreAssigned` 97, `HistoryInitialized` 18,
-- `Renewable` 11, `Renewed` 11, `AutoRenewalEnabled` 5, `SaleInitialized` 1.
-- ZERO of everything else — including `Purchased`, which is a GAP IN THE SAMPLE
-- AND NOT A FINDING ABOUT THE MARKET: 653 blocks is a fraction of a 28-day cycle
-- and purchases spread across a 14-day leadin.
--
-- THE RUNTIME DECLARES 37 VARIANTS WHERE THE UPSTREAM SOURCES THE PREP PINNED
-- (0.6.0-0.18.0) DECLARE 32. A later published version, 0.28.0, does declare 37
-- with the same names in the same order, and `adapter_substrate::broker` cites
-- it for the field lists of the 31 variants that never fire — but THAT
-- COMPARISON WAS MADE WHILE AUTHORING AND NOT IN THE PREP PASS, so it is
-- corroboration and not a measurement of this chain. Six sit beyond the newest crate's set, and BOTH PREDICTED RENAMES
-- ARE CONFIRMED ON THE LIVE RUNTIME: the variant is `PotentialRenewalDropped`
-- (not `AllowedRenewalDropped`) and the field is `SaleInitialized.end_price`
-- (not `regular_price`). A name-reading mapper built from the older sources would
-- have found both ABSENT, which is worse than an addition — an addition halts
-- loudly, an absence reads as a field that is simply not there.
create table coretime.broker_events (
    chain_id        text   not null references core.chains (id),
    block_height    bigint not null,
    event_index     integer not null,

    -- The variant name as the decoder spells it, WITHOUT the pallet prefix
    -- (`CoreAssigned`, not `broker.CoreAssigned`). The prefix is a constant for
    -- every row in the table and storing it 37 times over would be a column that
    -- never varies.
    variant         text   not null,

    -- The core this event is ABOUT, where it names one. NULL is honest and
    -- common: 19 of the 37 variants name a core, so **18 leave this column
    -- NULL** (`SaleInitialized`, `HistoryInitialized`, `CreditPurchased`, the
    -- revenue-claim family, the two lease variants that name only a task…).
    -- Fifteen of those 18 name NEITHER a core nor a task — that is a different
    -- count and the two must not be conflated, because this one is what the
    -- partial index below excludes.
    --
    -- On `Renewed` this is the NEW core, never `old_core` — the renewal moved
    -- the index (see the header) and the row's subject is where the entitlement
    -- IS, not where it was. `old_core` stays in `data` and the pair is what a
    -- cross-cycle reader needs.
    core_index      integer,

    -- The para id, where the event names one. Promoted because it is the
    -- DURABLE identity across sale cycles while the core index is not.
    task_id         integer,

    -- The variant's whole decoded payload, kept intact (schema-on-read). This is
    -- where the region id lives, where `old_core` lives, where every price and
    -- every workload lives. A column arrives with its reader; nothing here reads
    -- them yet.
    --
    -- SHAPE TRAPS MEASURED AND WORTH KNOWING BEFORE QUERYING THIS: the broker's
    -- core index renders as a bare `u16` (`"core": 0`) while the RELAY's renders
    -- as `{"core":[0]}`, because `polkadot_primitives::CoreIndex` is a newtype
    -- and the broker's is not. And `RegionRecord.owner` is `Option<AccountId32>`
    -- and renders THREE array layers deep — `{"Some":[[[…32 bytes…]]]}`.
    data            jsonb  not null,

    runtime_version bigint not null,
    mapper_version  integer not null,
    observed_at     timestamptz not null default now(),

    primary key (chain_id, block_height, event_index)
) partition by list (chain_id);

create table coretime.broker_events_default partition of coretime.broker_events default;

-- NO SECONDARY INDEXES ON THIS TABLE, DELIBERATELY, AND THE REASON IS THIS
-- PROJECT'S OWN RULE RATHER THAN AN OVERSIGHT.
--
-- 0019 stated it and 0020 obeyed it: "the query that WOULD earn this index does
-- not exist yet and should arrive WITH its index." **THE DELTA READER IS NOT IN
-- THIS SLICE** — there is no `BrokerIndex`, no route and no `select` against
-- either table anywhere in the repo, only the writer below. So the two queries
-- these tables exist for are DESCRIBED here and SERVED by nothing:
--
--   "what happened to core N"                -> (chain, core, height)
--   "what happened to task P"                -> (chain, task, height)
--   "what was core N entitled to do at H"    -> (chain, core, relay_block)
--   "where did task P hold entitlement"      -> (chain, task, relay_block)
--
-- Each will want a PARTIAL index (`core_index`/`task_id` are NULL on most rows,
-- and indexing 18 variants' worth of NULLs costs writes for rows no equality
-- lookup can reach — 0020's argument, with 0020's caveat that the partial
-- predicate is provable only while the query keeps the indexed column bare on
-- the left). They arrive together, with the reader.
--
-- The PRIMARY KEY prefix `(chain_id, block_height)` already serves the range
-- scans `broker-range` and any timeline query need, which is why this table is
-- usable today without them.

-- ---------------------------------------------------------------------------
-- THE SEAM
-- ---------------------------------------------------------------------------
-- One (core, task, ratio) triple from one `CoreAssigned`, with the RELAY block
-- it takes effect from.
--
-- This is the ONLY table in the entitlement half that the delta reads, and it is
-- separate from `broker_events` for three reasons: it expands one event into N
-- rows, it carries a relay height that nothing else here does, and its subject
-- is an ASSIGNMENT rather than an announcement.
--
-- AN ASSIGNMENT ANNOUNCED IS NOT AN ASSIGNMENT APPLIED, and this table records
-- the announcement. `Broker.CoreAssigned` is emitted on the Coretime chain when
-- the instruction is sent by XCM Transact; the relay emits its OWN
-- `coretime.CoreAssigned { core }` only after `scheduler::assign_core` returns
-- Ok. Measured at the boundary: 97 sent, 97 applied, ZERO broker events with no
-- relay partner — so the failure mode is expressible here and HAS NO LIVE
-- INSTANCE, which is stated as unobserved rather than impossible. Same doctrine
-- as slice 7's bounty 37, where an announced payout moved no money.
create table coretime.core_assignments (
    chain_id        text   not null references core.chains (id),
    block_height    bigint not null,
    event_index     integer not null,
    -- Ordinal within the event's `assignment` vector. Zero on every row measured
    -- so far, because every live vector has length 1.
    assignment_index integer not null,

    -- The broker's own `u16`, widened. Equal to `coretime.core_occupancy.
    -- core_index` with no offset — measured min diff 0 / max diff 0 over 97
    -- pairs. THIS EQUALITY IS THE PRODUCT; if it ever stops holding, the delta
    -- stops meaning anything and the reader must say so rather than divide.
    core_index      integer not null,

    -- `CoreAssigned.when`: the RELAY block number the assignment takes effect
    -- from, stated by the chain and always an exact multiple of 80 (one
    -- timeslice). It is what makes this a join and not a correlation, and it is
    -- a RELAY height sitting in a row whose `chain_id` is the COretime chain —
    -- the same cross-chain-number-line shape slice 5 met on Asset Hub treasury
    -- `valid_from` and slice 9 met on the scheduler's agenda.
    relay_block     bigint not null,

    -- idle | pool | task. Three values because `CoreAssignment` has three
    -- variants, and collapsing `idle` and `pool` into "not a task" would erase
    -- the distinction the waste figure turns on: an idle core is unsold, a pool
    -- core was sold and donated to the instantaneous market.
    assignment_kind text   not null check (assignment_kind in ('idle', 'pool', 'task')),

    -- Set exactly when `assignment_kind = 'task'`, enforced below rather than
    -- left as a convention.
    task_id         integer,

    -- `PartsOf57600`. 57600 is a whole core; a fraction is an interlaced region.
    -- Measured 57600 on every one of the 97 live assignments. THIS IS THE ONLY
    -- SURVIVING TRACE OF THE CORE MASK — the tick discards the pattern and keeps
    -- the bit count — so it is stored even though nothing divides by it yet.
    parts           integer not null,

    runtime_version bigint not null,
    mapper_version  integer not null,
    observed_at     timestamptz not null default now(),

    primary key (chain_id, block_height, event_index, assignment_index)
) partition by list (chain_id);

create table coretime.core_assignments_default partition of coretime.core_assignments default;

-- A task assignment must name its task and a non-task must not pretend to have
-- one. Written as a CHECK because "task_id is NULL means pool or idle" is the
-- kind of sentence that stays true only until somebody writes a row.
alter table coretime.core_assignments
    add constraint core_assignments_task_names_its_para
    check ((assignment_kind = 'task') = (task_id is not null));

-- THE DELTA'S OWN INDEX BELONGS HERE AND IS NOT CREATED YET, for the reason
-- given above `broker_events`: the reader that would earn it does not exist in
-- this slice. When it lands it wants `(chain_id, core_index, relay_block)`, so
-- that "the latest assignment at or before relay height H" is a backwards index
-- scan rather than a filter over every assignment the chain ever made.
--
-- AND THE READER MUST RENDER ONE CASE THIS TABLE CANNOT ANSWER, stated here
-- because it is a schema-shaped gap rather than a rendering choice.
-- `CoreAssigned` fires at SALE BOUNDARIES — the prep found zero relay
-- `CoreAssigned` inside slice 11's whole 1,000-block window — so a core with no
-- assignment at or before H means "our index does not reach back to the previous
-- sale", NOT "the core was idle". Those must never be the same answer: the
-- second reads as "nobody bought it", which is exactly the direction
-- `adapter_substrate::broker`'s halt message calls the worst one to be wrong in,
-- because it invents waste that did not happen. Same shape as 0024's NULL
-- `relay_parent_height`, which means "outside our data" and never "lag zero".

-- ---------------------------------------------------------------------------
-- THE FIFTH DATED STATE READING, HAND-ROLLED ON PURPOSE
-- ---------------------------------------------------------------------------
-- `balance_anchors`, `core.assets`, `coretime.core_config`, slice 12's
-- pool-level Omnipool/Stableswap reading and now this are FIVE instances of one
-- pattern: state with no event, read at a stated block, immutable per (subject,
-- height), carrying `runtime_version` and a source. `xcm.channels` wants a sixth
-- with a different consumer (DIFFED across session boundaries).
--
-- Slice 12's prep flagged the fourth as the moment to decide whether the pattern
-- becomes a shared mechanism. THE DECISION IS DELIBERATELY DEFERRED AGAIN, and
-- the reason is recorded here so it is not reopened every slice: the six differ
-- in SUBJECT SHAPE and only the unbuilt one needs diffing, so a shared table would
-- have to be generic over the subject and would buy a `jsonb` column where each
-- of these has real ones. Revisit when two of them want the same reader, not
-- when the count goes up.
--
-- WHAT IT IS FOR. `Broker.Status.core_count` is the entitlement half's
-- denominator, and it is the cross-check that makes the join believable: it read
-- 100 on the Coretime chain at the same time the RELAY's
-- `Configuration.ActiveConfig.scheduler_params.num_cores` read 100. Two chains,
-- two storage items, one number — and if they ever disagree, one of the two
-- halves is being counted against the wrong denominator and the delta reader
-- must refuse rather than pick one.
create table coretime.broker_config (
    chain_id        text   not null references core.chains (id),
    block_height    bigint not null,

    -- `Broker.Status.core_count`. The number of cores the broker believes it is
    -- selling, which is the entitlement-side twin of `core_config.num_cores`.
    core_count      integer not null,

    -- The whole `Status` record, kept intact: `private_pool_size`,
    -- `system_pool_size`, `last_committed_timeslice`, `last_timeslice`.
    status          jsonb  not null,

    -- The whole `Configuration` record, kept intact. It carries the sale
    -- geometry the price curve is evaluated against — `advance_notice`,
    -- `interlude_length`, `leadin_length`, `region_length`, `ideal_bulk_proportion`,
    -- `limit_cores_offered`, `renewal_bump`, `contribution_timeout` — and NOTHING
    -- HERE READS IT YET. It is captured rather than promoted because a sale price
    -- is not reconstructible without it and a reading not taken cannot be taken
    -- later.
    configuration   jsonb  not null,

    runtime_version bigint not null,
    observed_at     timestamptz not null default now(),

    primary key (chain_id, block_height)
);

comment on table coretime.broker_events is
    'Every `Broker.*` event, append-only — the ENTITLEMENT half of coretime '
    '(who bought, renewed, split, assigned or pooled what). 6 of the runtime''s '
    '37 declared variants fire in the sampled windows; the other 31 are recorded '
    'as zeroes in migration 0025''s header rather than assumed absent. Occupancy '
    'is coretime.core_occupancy, and the product is the delta.';

comment on table coretime.core_assignments is
    'The SEAM between entitlement and occupancy: one (core, task, parts) triple '
    'from one `Broker.CoreAssigned`, carrying the RELAY block it takes effect '
    'from. `core_index` equals coretime.core_occupancy.core_index with no offset '
    '(measured over 97 pairs, min diff 0 / max diff 0), and `relay_block` is '
    'stated by the chain, so the join needs no timeslice arithmetic.';

comment on column coretime.core_assignments.parts is
    'PartsOf57600 — 57600 is a whole core. The ONLY surviving trace of the '
    'region''s 80-bit CoreMask: pallet-broker''s tick converts the mask as '
    'count_ones() * 720 and discards the pattern, so the relay learns a RATIO and '
    'never a schedule. Measured 57600 on every live assignment, i.e. interlacing '
    'is unexercised today — but two interlaced regions on one core assigned to '
    'the same task are indistinguishable on the relay side forever, which is why '
    'this is a counted case and not a silent one.';

comment on column coretime.core_assignments.relay_block is
    'A RELAY block number, in a row whose chain_id is the Coretime chain. Always '
    'an exact multiple of 80 (one timeslice). Same cross-chain number-line shape '
    'as Asset Hub treasury valid_from (slice 5) and the scheduler agenda '
    '(slice 9): read it against core.blocks on the RELAY, never against this '
    'chain''s own heights.';

comment on table coretime.broker_config is
    'A dated reading of Broker.Status + Broker.Configuration — the SIXTH '
    'instance of the state-with-no-event pattern, hand-rolled deliberately (see '
    '0025''s header for why the shared mechanism is deferred again). core_count '
    'is the entitlement-side denominator and read 100 at the same time the '
    'relay''s num_cores read 100; a disagreement means one half is being counted '
    'against the wrong denominator.';
