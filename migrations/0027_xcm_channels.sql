-- 0027_xcm_channels: the HRMP channel graph (Phase 3, slice 16).
--
-- The half of the `xcm` module promised since Phase 3 opened and deferred by
-- 0015 with a note that has stood unchanged for fourteen slices:
--
--   > xcm.channels — HRMP channel history CANNOT be built from events. […] The
--   > honest construction is a storage snapshot of `HrmpChannels` diffed at each
--   > `session.NewSession` — a different kind of worker, and its own slice.
--
-- This is that slice, and `PREP-phase3-slice16.md` ran the measurements first.
-- Everything asserted below is either measured on live Polkadot (2026-08-19,
-- relay spec 2003002) or read from pinned upstream source, and is cited as one
-- or the other. Nothing here is inferred.
--
-- ============================================================================
-- WHY THIS IS TWO TABLES AND NOT ONE
-- ============================================================================
-- Because "we did not look" and "there was nothing there" must never be the
-- same value, and on this surface they are the same SHAPE: an absent row.
--
-- This project has now caught that confusion four times at FIELD level
-- (`stable_across_window` reading true on an empty list, `cores_touched_ratio`
-- reading 0.0 on an empty window — twice — and `reads_as` reporting the
-- network's silence as our own blind spot) and once at QUERY level (slice 15's
-- `ref … on hydration`). Every one of those was caught because the payload
-- carried a field to catch it with. A channel graph assembled from snapshots
-- carries no such field by default: if session 9,000 has no rows, that is
-- either "no channel existed" or "nobody read it", and the rows cannot tell you.
--
-- So the READING is a row of its own. A header in `channel_readings` means we
-- looked at that height; the detail in `channel_snapshots` is what we saw. A
-- missing header is a gap; a header with no detail is an empty graph. The
-- foreign key from detail to header makes the second table incapable of
-- existing without the first, so the distinction cannot be lost by a partial
-- write.
--
-- ============================================================================
-- WHAT IS STORED, AND THE THREE FIELDS THAT ARE DELIBERATELY NOT
-- ============================================================================
-- `HrmpChannel` has EIGHT fields (confirmed field-for-field against the live
-- runtime's own metadata at spec 2003002, in that order, with every one of 224
-- values consuming every byte and leaving zero trailing). Three of them are NOT
-- stored here, and that is the design decision this slice turns on:
--
--     msg_count, total_size, mqc_head   -- NOT STORED
--
-- They are message-throughput counters, not topology. `queue_outbound_hrmp`
-- (polkadot-sdk `polkadot-stable2503`, hrmp.rs:1380-1392) rewrites the channel
-- row PER MESSAGE SENT, and `prune_hrmp` (:1350) rewrites it on drain. So a
-- table that stored the whole struct and diffed it would report channels as
-- "changed" constantly, and the change would be traffic.
--
-- THAT IS MEASURED, NOT ARGUED, and the measurement is sharper than the source
-- pass predicted. Reading all 224 values at two heights and diffing field by
-- field:
--
--     gap                       rows changed   fields that moved   topology
--     10 blocks   (~1 min)      0              —                   unchanged
--     100 blocks  (~10 min)     3              mqc_head only       unchanged
--     2400 blocks (one session) 16 of 224      mqc_head only       unchanged
--
-- Sixteen false "changes" per session against zero real ones. And note WHICH
-- field moved: `msg_count`/`total_size` did NOT, because they are
-- send-minus-drain counters that return to zero between readings, while
-- `mqc_head` is a monotonic message-queue-chain head that never does. A schema
-- that kept all three would have been noisy for a reason its author guessed
-- wrong about.
--
-- The five fields that ARE stored change only at open (the three limits) or by
-- explicit extrinsic (the two deposits). Their measured distribution at head is
-- almost degenerate — only TWO distinct (max_capacity, max_total_size,
-- max_message_size) triples exist across all 224 channels, (1000,102400,102400)
-- ×189 and (25,102400,102400) ×35, and only two distinct deposit pairs,
-- (1e11,1e11) ×152 and (0,0) ×72 — but they are kept per row rather than
-- normalised, because a row that describes itself is worth more than 220 saved
-- integers.
--
-- A BACKLOG FIGURE IS NOT AVAILABLE FROM THIS TABLE and that is deliberate.
-- `Hrmp.HrmpChannelContents` is the live message QUEUE (measured at 3-9 keys,
-- cross-checked against exactly 9 channels with msg_count > 0), so it is a
-- point-in-time backlog and nothing more. It is not read at all; 0019's rule is
-- that a column arrives with its reader, and no reader wants one.
--
-- ============================================================================
-- WHY EVERY READING STORES ITS FULL EDGE SET, THOUGH ONLY 1.7% OF THEM DIFFER
-- ============================================================================
-- Measured: 180 readings at one-session (2400-block) spacing over 30 days gave
-- **3 changed intervals out of 179 — 1.7%**. At 2,190 sessions/year that is the
-- difference between ~490,560 detail rows/yr and ~37 readings/yr, a ~59x
-- multiple, and the obvious move is to store only the readings that changed.
--
-- IT IS REFUSED, and not on storage grounds. "Changed" is only meaningful
-- relative to a PREVIOUS reading, and the previous reading is whatever happens
-- to be in the database when the write runs. `channels-range` can be run over
-- any window in any order, and a historical backfill legitimately fills earlier
-- readings after later ones — so a changed-only detail table makes the ROW SET
-- A FUNCTION OF WRITE ORDER. That is the defect class this project has shipped
-- twice and caught at review twice more: `merge_spend` (slice 5 blocker c, whose
-- WHERE guard dropped an approval's amount forever on a terminal-first replay),
-- `merge_bounty` (slice 7, the same defect found in shipped code), and both
-- projections' CASE-ladder rewrites. Every domain slice since verifies "re-run
-- the range, counts unchanged"; a changed-only table cannot pass that test
-- honestly.
--
-- The cost of refusing is small and was measured rather than assumed: the
-- COMPLETE history is 9,331 sessions (the first HRMP channel ever is at relay
-- #10234022, found by binary search — #10234021 has none) against a graph that
-- grew 42 -> 224 over four years, i.e. roughly 1.4M detail rows for everything
-- Polkadot has ever done. `core.events` is larger than that after a day of
-- backfill.
--
-- So: full edge set per reading, the reader is a point lookup rather than a
-- chained walk through unchanged markers, and replay is idempotent by
-- construction. The 59x figure is recorded here so a later slice can revisit it
-- with the tradeoff stated rather than rediscovered.
--
-- ============================================================================
-- WHY A PER-SESSION CADENCE IS LOSSLESS, AND THE BOUND THAT IS NOT
-- ============================================================================
-- A channel's EXISTENCE changes in exactly seven places in the whole production
-- tree — one `insert` (hrmp.rs:1085) and one `take` (:1139) — reachable only
-- from `initializer_on_new_session` (:937, via three processors), three
-- `force_*` extrinsics, and genesis. There are no pallet hooks, no migrations
-- and no `on_runtime_upgrade`; `initializer_initialize` returns `Weight::zero()`.
-- Para offboarding goes through `paras::initializer_on_new_session`, i.e. the
-- same boundary.
--
-- So a reading per session cannot miss a topology change, BY CONSTRUCTION —
-- and the empirical test agrees with no exceptions. Eleven topology changes were
-- located by binary search across four years (sessions 5186 -> 13601) and every
-- one landed exactly on a session boundary:
--
--     11 on a session boundary / 0 on a visible force_* call
--      0 attributable-but-opaque / 0 UNEXPLAINED
--
-- Note the implication direction, because it is easy to state backwards:
-- changes are a SUBSET of boundaries, not the reverse. 176 of 179 sampled
-- boundaries carried no change at all.
--
-- THE BOUND THAT REMAINS, and it must not be written as "Root": the three
-- `force_*` extrinsics gate on `type ChannelManager: EnsureOrigin` (hrmp.rs:269),
-- and Polkadot wires it (polkadot-fellows/runtimes 0d02533,
-- relay/polkadot/src/lib.rs:1397) to
--     EitherOfDiverse<EitherOf<EnsureRoot, GeneralAdmin>,
--                     EnsureXcm<IsVoiceOfBody<AssetHubLocation, GeneralAdminBodyId>>>
-- so a mid-session change is an ordinary `GeneralAdmin` referendum away AND can
-- arrive as XCM from Asset Hub, where governance has lived since 2025-11-04. In
-- that case it is NOT visible as a relay extrinsic: the call sits inside a
-- `Transact`'s `DoubleEncoded<Call>`, which slice 9 established `calls.rs`
-- renders as opaque bytes and does not recurse into.
--
-- The honest sentence, which the API serves: a channel can open and close
-- between two readings only via a `ChannelManager` force call, and if that call
-- arrived by XCM we cannot name it. No such call has ever been observed on
-- Polkadot — all eleven measured changes were ordinary boundaries — so this is a
-- STATED BOUND WITH NO LIVE INSTANCE, the same class as slice 2's
-- `forwarded = true` and slice 11's `CandidateTimedOut`.
--
-- `ChannelManager` is a runtime TYPE PARAMETER and appears in no metadata, so
-- nothing dotlens reads from the chain will ever confirm it. That is the third
-- instance of the class that produced slice 9's blocker 1 (`pallet_scheduler`'s
-- `BlockNumberProvider`, ~13.15M blocks wrong) and slice 1's required `--origin`
-- (pallet-referenda's track->origin map). It is upstream corroboration, never a
-- reading of this chain, and it is not written into Rust anywhere.
--
-- ============================================================================
-- THE HEIGHT CONVENTION, AND IT IS NOT THE OBVIOUS ONE
-- ============================================================================
-- READ AT THE HEIGHT WHERE `session.NewSession` FIRES. A state read at block
-- hash H sees the POST-change channel set.
--
-- The mechanism is worth writing down because guessing it wrong is silent. The
-- parachains `initializer` BUFFERS session notifications: `on_new_session`
-- pushes to `BufferedSessionChanges` (initializer.rs:309-311) and applies
-- nothing, and the buffer is drained in `on_finalize` OF THE SAME BLOCK
-- (:204-214, upstream's own comment: "Apply buffered session changes as the last
-- thing"), which then calls `hrmp::initializer_on_new_session` (:284).
-- `pallet_session` deposits `NewSession` in that same block's `on_initialize`.
-- So within block H the event fires early and the channel set changes late, and
-- end-of-block state at H already carries the change.
--
-- CONFIRMED on live data at the largest change in four years: at hash H-1 both
-- `Session.CurrentIndex` and the channel set are OLD; at hash H both are NEW.
-- Getting it backwards would date every reading to the wrong session — the same
-- class as slice 9's `at_parent` vs `at_parent + 1` agenda anchor, which cost a
-- full verification cycle.
--
-- ============================================================================
-- NO SYNTHESISED CHANNEL EVENTS, FOR THE SIXTH TIME ON THE SAME ARGUMENT
-- ============================================================================
-- There is no `channel_events` table and there will not be one. An open or a
-- close is the DIFFERENCE between two readings that both carry lineage, so a
-- stored "channel opened at session N" row would be the one copy WITHOUT
-- lineage — verbatim the argument that killed `treasury.consolidated_position`
-- (P2 slice 6), `graph.cross_chain_operations` (P3 slice 3), the stored
-- forwarded-attribution (P3 slice 5), a `logical_assets` join table (0019) and
-- the coretime delta (0026). The history is computed per request.
--
-- The events would not help anyway, and this is stronger than 0015 knew. All
-- nine `deposit_event` sites in the pallet (lines 529, 546, 562, 667, 707, 751,
-- 852, 884, 891) sit OUTSIDE all four functions that create or destroy channels
-- (1033-1055, 1056-1122, 1123-1136, 1137+) — zero overlap, verified by
-- exhaustive grep. Worse than "no event fires": THREE events announce a
-- completion at REQUEST time —
--   * `HrmpSystemChannelOpened` (:751, :884, :891) — the channel opens next session
--   * `HrmpChannelForceOpened`  (:707) — and `force_open_hrmp_channel` does not
--                                        open anything; it creates the request pair
--   * `ChannelClosed`           (:562) — the key is removed next session
-- so an events-only table would be wrong about the DATE of every open and close
-- it did record, and would take three event names at their word.
--
-- Measured on the strongest possible case: relay block #32492253 destroyed 28
-- HRMP channels (every channel involving para 2004 — Moonbeam, which ECOSYSTEM
-- records as having left for Base in 2026-07, i.e. the offboarding teardown path
-- exercised rather than reasoned about) and emitted 94 EVENTS, ZERO of them
-- `hrmp.*`. Its only non-inherent events were `historical.RootStored` and
-- `session.NewSession`. Repo-wide there are zero `hrmp.*` events in the entire
-- relay index. An events-only table would not merely be incomplete here; it
-- would be empty.

-- ---------------------------------------------------------------------------
-- THE HEADER: that we looked, and what we saw the SIZE of.
--
-- NOT PARTITIONED, unlike `xcm.messages`. This is relay-only state — a
-- parachain sees only an `AbridgedHrmpChannel` subset of its OWN channels
-- through the relay state proof (primitives/src/v8/mod.rs:1341, and
-- cumulus relay_state_snapshot.rs:232-262 shows it reads only its own
-- ingress/egress index), so the complete graph exists on exactly one chain per
-- network. At one reading per session that is ~2,190 rows/year. Partitioning it
-- would be machinery for a table that will never need it, and it follows
-- `coretime.core_config` and `coretime.broker_config`, which are not partitioned
-- for the same reason.
-- ---------------------------------------------------------------------------
create table xcm.channel_readings (
    chain_id      text   not null references core.chains (id),
    -- The relay height the state was read at. THE PRIMARY COORDINATE, because
    -- it is what was actually done; `session_index` below is derived from it.
    block_height  bigint not null,

    -- `Session.CurrentIndex` read at the SAME hash, so the reading dates itself
    -- in the units topology changes in. Not unique: reading twice inside one
    -- session is legal and harmless (the graph provably cannot change between
    -- them), and forbidding it would forbid a spot check.
    session_index bigint not null,

    -- Counted, never estimated. `channel_count` is the number of OPEN channels;
    -- `open_request_count` the number of pending `HrmpOpenChannelRequests`.
    -- Both are redundant against `channel_snapshots` on purpose: they are what
    -- lets a coverage query answer "how big was the graph" without reading the
    -- detail, and a disagreement between the two is a loud defect rather than a
    -- silent one.
    channel_count      integer not null,
    open_request_count integer not null,

    -- blake2_256 over the sorted OPEN edge set, hex. The cheap "did the topology
    -- change" comparison, and itself re-derivable from the detail rows.
    --
    -- IT COVERS OPEN CHANNELS ONLY, deliberately. A pending request is not
    -- topology, and folding it in would make the digest move for a reason that
    -- is not a channel opening or closing. The field is named `topology_digest`
    -- rather than `digest` so that cannot be misread.
    topology_digest text not null,

    -- Lineage (Invariant 3). A reading not taken cannot be taken later.
    spec_version  bigint not null,
    source        text   not null,
    observed_at   timestamptz not null default now(),

    primary key (chain_id, block_height)
);

comment on table xcm.channel_readings is
    'One row per time the HRMP channel graph was READ. Its existence is the '
    'fact: a session with no row here was never looked at, which is a different '
    'thing from a session in which no channel existed. Immutable per '
    '(chain, height) — the sink inserts and never updates, because a second '
    'read of the same historic state can only agree or indicate a defect.';

comment on column xcm.channel_readings.topology_digest is
    'blake2_256 of the sorted open edge set. Two consecutive readings with equal '
    'digests had no topology change between them, which — given a per-session '
    'cadence — is a PROOF rather than an inference (channel existence mutates '
    'only at session boundaries; see this migration''s header). Pending open '
    'requests are excluded on purpose.';

-- ---------------------------------------------------------------------------
-- THE DETAIL: what the graph was.
--
-- ONE TABLE FOR OPEN CHANNELS AND PENDING REQUESTS, because the subject is the
-- same — an edge between two paras, as of one reading — and a caller asking
-- "what is the graph" wants both with the difference visible, not two queries
-- and two coverage stories. `HrmpOpenChannelRequest` and `HrmpChannel` share
-- four of their fields (the three limits and the sender's deposit) and differ in
-- one each, so the shared shape is real rather than forced.
--
-- `HrmpOpenChannelRequests` EARNS ITS PLACE and was nearly dropped: it is
-- persistently non-empty — 54 to 66 pending across twelve monthly readings, 54
-- today — a real standing population of "asked and never accepted" that a graph
-- endpoint should be able to show. `HrmpCloseChannelRequests` was measured
-- alongside it and is NOT stored, for a reason worth recording because the first
-- reading of it was wrong: it read zero in all twelve monthly readings, which
-- looked like "close requests never exist" and is a SAMPLING ARTIFACT — a close
-- request lives at most one session (~4h), so a monthly sample essentially
-- cannot catch one. Re-checked against a known close, the set held
-- [(2030,2032),(2032,2030)] for the entire preceding session and was consumed
-- exactly at the boundary. It is not stored because a per-session snapshot of a
-- one-session-lived value is a coin flip, and because THERE IS A SECOND CLOSE
-- PATH IT CANNOT SEE AT ALL: Moonbeam's offboarding teardown destroyed 28
-- channels with no close request ever existing. Two structurally different close
-- mechanisms, one of them invisible in that map — so shipping it would have
-- implied a completeness it does not have.
-- ---------------------------------------------------------------------------
create table xcm.channel_snapshots (
    chain_id     text   not null,
    block_height bigint not null,

    -- `HrmpChannelId { sender: ParaId, recipient: ParaId }`, and the channel is
    -- DIRECTIONAL — (1000 -> 2034) and (2034 -> 1000) are two separate channels
    -- that are opened, closed and deposited for independently. A reader that
    -- treats the pair as one undirected edge will halve the graph.
    --
    -- bigint rather than the `integer` used by `coretime.core_occupancy.para_id`:
    -- `ParaId` is a u32 and i32 cannot hold all of them. The cast is free at this
    -- scale, and it removes a range check that would never fire and therefore
    -- would never be tested.
    sender    bigint not null,
    recipient bigint not null,

    -- 'open' | 'requested'. An edge is one or the other and never both at one
    -- reading: `process_hrmp_open_channel_requests` removes the request in the
    -- same statement that inserts the channel (hrmp.rs:1085-1115), and
    -- `init_open_channel` refuses a request for a channel that already exists.
    -- That invariant is SOURCE-DERIVED AND UNEXERCISED — no live instance has
    -- ever contradicted it because none could — so the writer checks it in Rust
    -- and refuses loudly rather than letting the primary key silently arbitrate
    -- one row away.
    state text not null,

    -- bigint for the same reason `sender`/`recipient` are: these are u32s on
    -- chain and i32 cannot hold all of them. A limit above 2^31 would store
    -- NEGATIVE in an `integer` column and read back as a huge u32 with nothing
    -- to catch it. Widening the column removes the range check rather than
    -- adding one, and a check that can never fire is a check nobody has tested.
    -- (Measured today: only two distinct triples exist, both far below the
    -- boundary — which is exactly why a truncation here would go unnoticed.)
    max_capacity     bigint not null,
    max_total_size   bigint not null,
    max_message_size bigint not null,

    -- u128 planck. The sender's deposit exists in both states; the recipient's
    -- only once the channel is open, because it is supplied by `accept_open_channel`.
    --
    -- NOTE FOR ANYONE HAND-DECODING THIS FIELD FROM RAW STORAGE: `Balance` inside
    -- `HrmpChannel` is a FIXED 16-byte u128, NOT compact. The prep's first pass
    -- read it as compact and reported `sender_deposit=0 recipient_deposit=58` —
    -- well-formed nonsense, caught only by value-size arithmetic (values are
    -- exactly 53 or 85 bytes, and 20+1+16+16 / 20+1+32+16+16 close only for the
    -- fixed encoding). The shipped decoder cannot make this mistake because it
    -- decodes against the type the metadata declares.
    sender_deposit    numeric not null,
    recipient_deposit numeric,

    -- `HrmpOpenChannelRequest.confirmed` — the recipient has accepted but the
    -- session boundary has not yet run. Only meaningful for a pending request.
    confirmed boolean,

    primary key (chain_id, block_height, sender, recipient),

    -- The structural half of the header/detail split: detail CANNOT exist
    -- without the reading that produced it, so "no rows" is never ambiguous.
    foreign key (chain_id, block_height)
        references xcm.channel_readings (chain_id, block_height),

    constraint channel_snapshots_state_vocabulary
        check (state in ('open', 'requested')),
    -- Biconditionals, not one-way implications — the shape slice 13's
    -- `core_assignments_task_names_its_para` established. A 'requested' row
    -- carrying a recipient deposit is refused just as hard as an 'open' row
    -- missing one, because both mean the writer confused the two maps.
    constraint channel_snapshots_open_names_its_recipient_deposit
        check ((state = 'open') = (recipient_deposit is not null)),
    constraint channel_snapshots_request_names_its_confirmation
        check ((state = 'requested') = (confirmed is not null))
);

comment on table xcm.channel_snapshots is
    'The HRMP edge set as of one reading. Directional. Carries the five stable '
    'fields of HrmpChannel and omits msg_count/total_size/mqc_head, which are '
    'message-throughput counters — see this migration''s header for the '
    'measurement (16 of 224 rows move per session on mqc_head alone, with zero '
    'topology changes underneath).';

-- The per-edge history query — "when did 1000 -> 2034 open and close" — has no
-- other path: it walks one edge across every reading, and the header/detail
-- join is ordered by height. This index arrives WITH that reader, which is
-- 0019's rule and the one 0013 violated by shipping indexes nothing selected on.
--
-- The graph-at-a-height query is served by the primary key's own
-- (chain_id, block_height) prefix and earns nothing extra. `session_index` is
-- likewise NOT indexed: the coverage walk reads a chain's readings in height
-- order, and session order is height order, so the primary key serves it.
create index channel_snapshots_edge_idx
    on xcm.channel_snapshots (chain_id, sender, recipient, block_height desc);

-- ============================================================================
-- THE SIXTH DATED STATE READING — AND IT IS THE FIRST OF A DIFFERENT PATTERN
-- ============================================================================
-- 0025's header names five instances of "state with no event, read at a stated
-- block, immutable per (subject, height), carrying runtime_version and a
-- source": `balances.balance_anchors`, `core.assets`, `coretime.core_config`,
-- slice 12's pool-level Omnipool/Stableswap reading, and `broker_config`. It
-- then left a criterion rather than a deferral:
--
--   > Revisit when two of them want the same reader, not when the count goes up.
--
-- Applying it honestly rather than re-litigating it: THIS IS NOT THE SIXTH
-- INSTANCE OF THAT PATTERN. IT IS THE FIRST INSTANCE OF A DIFFERENT ONE. The
-- five existing readings are state ANCHORS, whose reader is "the newest row at
-- or before H". This is a state SERIES, whose reader is a pairwise diff over
-- consecutive readings. Those are not the same mechanism, and no amount of
-- counting makes them one — so it is hand-rolled again, and generalising at ONE
-- copy is the mistake this project avoids. `ingest::module` was extracted
-- because five identical copies were MEASURED.
--
-- THE OVERTURN CONDITION IS NAMED, AND IT IS ALREADY HALF MET. The obvious
-- second consumer of a diffing reader is `HostConfiguration` history, and it is
-- not hypothetical: `Configuration.ActiveConfig` was measured CHANGING IN 21 OF
-- 47 MONTHLY INTERVALS over the same four years, growing 188 -> 238 bytes. This
-- slice brushes it twice — a channel's limits are copied from the active config
-- AT OPEN TIME (so a config change does not retroactively move existing
-- channels), and Polkadot's system-channel defaults are a RATIO over the live
-- config (`ActiveConfigHrmpChannelSizeAndCapacityRatio`). When that history
-- lands, two consumers want the same reader and 0025's criterion fires. It is
-- recorded here so the criterion stays live rather than curdling into a
-- permanent no.
--
-- ============================================================================
-- A NOTE FOR WHOEVER JOINS THIS AGAINST xcm.messages
-- ============================================================================
-- The join key is the message's RELAY PARENT NUMBER, not its parachain height.
-- The channel graph is a relay-side fact; an `hrmp` row in `xcm.messages` sits
-- at a PARACHAIN height, and the two number lines are unrelated. This was
-- confirmed on live data (sample size ONE, stated as such): an AH `sent/hrmp`
-- row naming para 2000 at AH #19410076 carries `relay_parent_number` 32534216,
-- and at relay #32534216 both (1000->2000) and (2000->1000) are open.
--
-- It is the same class as slice 5's finding that Asset Hub treasury `valid_from`
-- is a relay block number, and slice 13's `when`-vs-relay-parent conflation.
-- A parachain block's relay parent lives at
-- `args->'data'->'validation_data'->>'relay_parent_number'` in core.transactions,
-- not one level up.
--
-- Also note the structural half of that check: THE RELAY NEVER CARRIES HRMP.
-- HRMP connects parachains to each other and the relay only routes, so mapping
-- relay blocks yields `received/ump` rows and nothing else. A corroboration of
-- this graph therefore needs a PARACHAIN index, which is why the check above has
-- a sample size of one.
