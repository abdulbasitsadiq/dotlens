-- 0024_coretime: core OCCUPANCY, read from the relay (Phase 3, slice 11).
--
-- The half of coretime nobody else has. RegionX and Lastic already draw core
-- grids — do not claim otherwise — but both are MARKETPLACES: ownership-forward,
-- present tense, unit = the region, i.e. an ENTITLEMENT. A marketplace renders
-- the lease; only an archive can render the USAGE. This schema is the usage
-- half, and the product is eventually the delta between them (PRODUCT.md gap 10).
--
-- THE ENTITLEMENT HALF IS NOT HERE, as scoped — it landed in 0025 (slice 13),
-- which registered chain 1005 and mapped `pallet-broker` into
-- coretime.broker_events + coretime.core_assignments. The join between the two
-- halves is `core_assignments.core_index = core_occupancy.core_index` over the
-- relay-block window `CoreAssigned.when` states, and it was measured clean in
-- both directions with zero exceptions. Everything below reads
-- RELAY data that Phase 1 already backfilled — which is why this half is far
-- cheaper than it sounds and why it went first.
--
-- ============================================================================
-- EVERY DECISION BELOW IS A MEASUREMENT. Slice 11's prep pass, 1,542 relay
-- blocks across SIX runtimes (specs 9431 … 2003002, Dec-2023 → today),
-- 101,857 candidate events, decoded from the raw store with no RPC at all.
-- ============================================================================
--
-- ---------------------------------------------------------------------------
-- THE CORE INDEX COMES FROM THE EVENT, NEVER FROM THE DESCRIPTOR
-- ---------------------------------------------------------------------------
-- `paraInclusion.CandidateIncluded(receipt, head_data, CoreIndex, GroupIndex)`
-- carries the core index as its own third field. The candidate receipt's
-- DESCRIPTOR also carries one from RFC-103 (V2) onward, and it is tempting to
-- prefer it because it sits beside the para id. Measured, it is the wrong
-- choice, twice over:
--
--   * The descriptor's core index DOES NOT EXIST for the entire V1 era. Specs
--     9431 / 1000001 / 1003004 carry `collator` + `signature` and no
--     `core_index` and no `session_index` at all — 1,071 rows in the sample.
--     The EVENT's third field is present on all six specs.
--   * Where both exist THEY DISAGREE ON 14.4% OF ROWS — 14,672 of 101,857 —
--     and the split is exact: `reserved1`/`reserved2` all-zero → 87,185 rows,
--     87,185 agree, 0 disagree; otherwise → 14,672 rows, 0 agree, 14,672
--     disagree. Zero exceptions in either direction.
--
-- SO THE VALIDITY PREDICATE IS "BOTH RESERVED FIELDS ALL-ZERO", NOT
-- `version == 0`, and that distinction was caught misclassifying: two rows
-- (para 3338) passed a version check while carrying `desc_core = 35184` and
-- `session_index = 2973429320`. A V1 descriptor is structurally reinterpreted as
-- a V2 one, so those bytes are collator public-key material — and a pubkey
-- beginning 0x00 passes `version == 0` and yields garbage.
--
-- The conclusion is not "prefer the event's field"; it is that the descriptor's
-- adds NOTHING. It is absent on half the runtimes, it merely agrees when valid,
-- and it is garbage 14.4% of the time. It is therefore not stored, not even as a
-- disagreement column: what it records is a COLLATOR'S CLAIM, and what this
-- table is about is the RUNTIME'S ASSIGNMENT.
--
-- ---------------------------------------------------------------------------
-- WHY THE PK IS THE EVENT COORDINATE AND THE MEASURED INVARIANT IS AN INDEX
-- ---------------------------------------------------------------------------
-- `(chain_id, block_height, core_index)` has ZERO collisions across all 51,998
-- included candidates in the sample — one candidate per core per block, measured
-- rather than hoped. That is a real invariant and it is enforced below, as a
-- PARTIAL UNIQUE INDEX scoped to inclusions.
--
-- It is NOT the primary key, for the reason `gov.votes` is keyed the same way: a
-- row whose subject cannot be attributed must still be recordable. A future
-- runtime that emits a candidate event this mapper cannot place on a core would
-- have nowhere to go under a core-keyed PK, and dropping it silently is the one
-- outcome this project never accepts. The event coordinate is unique by
-- construction and always available.
--
-- The uniqueness is scoped to `kind = 'included'` because a core legitimately
-- carries an inclusion AND a backing in one block: async backing means the
-- candidate included at height H was backed at H−2 … H−6, so at 93% occupancy
-- the same core is finishing one candidate and starting the next in the same
-- block. Measured lag: min 2, avg 3.261, max 6 — never 0, never 1, never >6.
--
-- ---------------------------------------------------------------------------
-- WHAT IS DELIBERATELY NOT A COLUMN
-- ---------------------------------------------------------------------------
-- `head_data` is a parachain header: min 633 / avg 1,127 / max 15,658 chars of
-- JSON, 20.0 MB across 17,763 rows in a 555-block subset alone. Occupancy needs
-- none of it, and a mapper that stored it would put a header in every row.
--
-- A `utilization` column. See `coretime.core_config` below — there are TWO
-- ratios and they are not the same number.
create schema if not exists coretime;

-- One candidate event, on one core, at one relay block.
--
-- APPEND-ONLY AND PARTITIONED BY CHAIN, like every other domain fact table. The
-- subject is the RELAY's own observation of a parachain's block, so `chain_id`
-- is the relay — `para_id` is what the observation is about.
create table coretime.core_occupancy (
    chain_id        text   not null references core.chains (id),
    block_height    bigint not null,
    event_index     integer not null,

    -- included   — the candidate became available and was enacted. THIS IS THE
    --              OCCUPANCY FACT: a para-block produced on this core is now
    --              part of the chain.
    -- backed     — it entered the pipeline on this core. Kept because
    --              backed-without-included is the WASTED-CORETIME signal, and a
    --              table that cannot express it cannot detect it. See the note
    --              on `timed_out` for how empty that signal currently is.
    -- timed_out  — the core was occupied and produced nothing. The most
    --              interesting row type in the module and the rarest.
    kind            text   not null check (kind in ('included', 'backed', 'timed_out')),

    -- Read from the EVENT's own field, one newtype array layer peeled. See the
    -- header: `CoreIndex(pub u32)` renders as `[32]`, not `32`, and this is the
    -- SEVENTH time this project has met that layer.
    core_index      integer not null,
    para_id         integer not null,
    -- The validator group that backed it. Absent on `timed_out`, which carries
    -- three fields where the other two carry four — so a mapper that assumed one
    -- arity across the three tuple variants would read the core index out of a
    -- field that is not there.
    group_index     integer,

    -- ASYNC BACKING, as two columns rather than one derived number.
    --
    -- `relay_parent_hash` is what the descriptor states. `relay_parent_height`
    -- is that hash resolved against `core.blocks`, and it is NULLABLE FOR A
    -- REASON THAT IS NOT "we did not look": a relay parent 2–6 blocks back falls
    -- outside the indexed window at every window edge, so the lag is computable
    -- only inside a contiguously-indexed range. Measured: 50,221 of 51,998
    -- resolved (96.6%); the 1,777 that did not are window edges and always will
    -- be. A reader must be able to tell "outside our data" from "lag zero".
    --
    -- AND THE NULL IS FILLABLE, which is what keeps that reading honest as the
    -- index grows. The sink's conflict action is a MONOTONE FILL — it writes a
    -- height only where there is none and one is now resolvable — so re-running
    -- a range after a wider backfill resolves the edges and can never rewrite a
    -- height already resolved. Under a plain `do nothing` the column's meaning
    -- would silently drift to "outside the window WHEN WE FIRST LOOKED".
    relay_parent_hash   text,
    relay_parent_height bigint,

    -- The candidate's PoV hash, unique per candidate. It is what matches a
    -- `backed` row to its `included` one across the 2–6 block lag, and it is
    -- therefore the only way to detect a core that was occupied and produced
    -- nothing WITHOUT relying on `timed_out` firing. The prep used exactly this
    -- to build a second, independent wasted-coretime signal.
    pov_hash        text,

    -- Lineage (Invariant 3). `runtime_version` matters more here than usual:
    -- the descriptor inside field 0 changed shape TWICE across the six specs
    -- sampled, and a row that cannot say which runtime it was decoded against
    -- cannot be re-read.
    runtime_version bigint not null,
    mapper_version  integer not null,
    observed_at     timestamptz not null default now(),

    primary key (chain_id, block_height, event_index)
) partition by list (chain_id);

create table coretime.core_occupancy_default partition of coretime.core_occupancy default;

-- THE MEASURED INVARIANT, AS A GUARANTEE. Zero collisions across 51,998 included
-- candidates; one candidate per core per block follows from the protocol rather
-- than from luck. A violation is a genuine finding — our reading of the runtime
-- would be wrong — and it should stop ingestion loudly rather than produce an
-- occupancy figure that double-counts a core.
create unique index core_occupancy_one_candidate_per_core_idx
    on coretime.core_occupancy (chain_id, block_height, core_index)
    where kind = 'included';

-- The two queries this table exists for, and no others (0008/0009's rule: an
-- index arrives WITH its reader).
--
--   "how busy was core N over this window"  → (chain, core, height)
--   "where did para P run, and when"        → (chain, para, height)
create index core_occupancy_core_idx on coretime.core_occupancy (chain_id, core_index, block_height);
create index core_occupancy_para_idx on coretime.core_occupancy (chain_id, para_id, block_height);

-- ---------------------------------------------------------------------------
-- THE DENOMINATOR, WITH ITS OWN PROVENANCE
-- ---------------------------------------------------------------------------
-- A utilization ratio is (cores used ÷ cores that EXIST), and the second term is
-- `SchedulerParams.num_cores` — "how many cores are managed by the coretime
-- chain". Measured 100 at relay #32614536.
--
-- IT IS A TABLE AND NOT A CONSTANT BECAUSE IT MOVES. `num_cores` is host
-- configuration and changes at session boundaries; the prep read it at exactly
-- ONE block and said so, naming a stale denominator as a live risk. A ratio
-- computed against a number nobody can date is the kind of figure this project
-- exists not to produce — so every reading carries the block it was read at, and
-- a ratio names which reading it used.
--
-- Also measured, and worth recording because it contradicts a reasonable guess:
-- there is NO `on_demand_cores` field. On-demand shares the same 100 rather than
-- having a pool of its own, so "bulk cores" and "on-demand cores" are not two
-- denominators.
create table coretime.core_config (
    chain_id            text   not null references core.chains (id),
    block_height        bigint not null,
    num_cores           integer not null,
    -- The siblings that were read in the same decode, kept whole (schema-on-read)
    -- rather than given columns nothing queries yet: group_rotation_frequency,
    -- paras_availability_period, max_validators_per_core, lookahead (the field is
    -- `lookahead`, NOT `scheduling_lookahead`), on_demand_queue_max_size,
    -- on_demand_target_queue_utilization, on_demand_fee_variability,
    -- on_demand_base_fee.
    scheduler_params    jsonb  not null,
    runtime_version     bigint not null,
    observed_at         timestamptz not null default now(),
    primary key (chain_id, block_height)
);

comment on table coretime.core_occupancy is
    'What each core actually DID, from the relay''s own candidate events — the '
    'usage half of coretime. Entitlement (regions, renewals, sales) is a '
    'separate slice against the Coretime chain, and the product is the delta.';

comment on column coretime.core_occupancy.core_index is
    'From the EVENT''s own CoreIndex field, never from the candidate '
    'descriptor. The descriptor''s core index is absent on pre-RFC-103 runtimes '
    'and disagrees with the event''s on 14.4% of rows where both exist — it is a '
    'collator''s claim, while this is the runtime''s assignment. Measured '
    '2026-08-19 over 101,857 events across six specs.';

comment on column coretime.core_occupancy.relay_parent_height is
    'The descriptor''s relay_parent resolved against core.blocks. NULL means the '
    'parent falls OUTSIDE an indexed window — not that the lag is zero. Async '
    'backing puts it 2-6 blocks back (measured: min 2, avg 3.261, max 6, never 0 '
    'or 1), so every contiguous window has edges where this cannot be computed. '
    'Re-running coretime-range after a wider backfill FILLS a NULL here and '
    'never rewrites a resolved height (the sink''s conflict action is a monotone '
    'fill), so the column keeps meaning "outside our data" as the index grows.';
