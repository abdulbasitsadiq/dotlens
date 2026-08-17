-- 0016_xcm_links: the XCM correlation layer (Phase 3, slice 3).
--
-- ONE TABLE, AND IT HOLDS THE ONLY LINK THAT CANNOT BE COMPUTED AT READ TIME.
--
-- Slice 2 recorded observations and refused to stitch them. This slice stitches
-- them — and the finding that shapes the whole schema is that the stitch is
-- mostly NOT a stored thing:
--
--   * ACROSS chains, a journey is plain EQUALITY on `xcm.messages.message_id`.
--     A sender's topic and a receiver's `messageQueue` id are the same 32 bytes
--     or they are not; there is nothing to record, nothing to infer, and a
--     stored edge would only be a cached join that can go stale against its own
--     inputs. Proven live on 2026-08-17: Asset Hub #19581756 sent topic
--     0x16a07252…c28d and Hydration #13663124 processed 0x16a07252…c28d.
--
--   * WITHIN one block on one chain, a message has TWO ids and no event says so.
--     `WithUniqueTopic::deliver` calls the inner router — which emits
--     `xcmpQueue.XcmpMessageSent` / `parachainSystem.UpwardMessageSent` carrying
--     blake2_256 of the queued bytes — then THROWS THAT HASH AWAY and returns
--     the topic, which `pallet_xcm` emits as `Sent.message_id`. Two rows, two
--     ids, one message, and the only evidence connecting them is that they
--     happened in the same block in that order. THAT is what this table stores.
--
-- Without it the wire hash is a dead end: slice 2 measured exactly that — "the
-- same message's wire hash returns only the SENDING half", because the
-- receiving chain reported the TOPIC. One row here turns that into a journey.
--
-- WHAT IS DELIBERATELY NOT HERE, so the next reader does not think it was
-- forgotten:
--
--   graph.cross_chain_operations / graph.operation_steps (ARCHITECTURE §8's
--   original plan). An operation is "every observation carrying an id in this
--   id's alias set, ordered by block timestamp" — a pure function of
--   `xcm.messages` and this table, both of which carry lineage. Materialising it
--   would be a third copy WITHOUT lineage, which is precisely the argument that
--   killed `treasury.consolidated_position` in Phase 2 slice 6 and amended §8
--   then. The journey is assembled per request instead; §8/§10 are amended to
--   match. Materialise it the day a LISTING surface (recent cross-chain
--   operations) needs it, not before.
--
--   A hop rule (an inbound message linked to a forwarded outbound send in the
--   same block). Its window of applicability is narrow enough that it may be
--   EMPTY: from staging-xcm-executor 20.0.0 (Jul 2025) the topic PROPAGATES
--   across the hop, so equality already stitches it and no rule is needed; below
--   19.1.0 the forwarded leg emits no `Sent` at all, so there is nothing to link
--   to. Only [19.1.0, 20.0.0) — and a chain that deliberately re-wraps with a
--   NEW topic — would need one. Slice 2 also measured zero `forwarded = true`
--   rows on live data. A rule with no observed input is a guess with a schema.
--
--   xcm.channels. Unchanged from 0015: channels open and close at SESSION
--   boundaries with no event at all, so it needs an `HrmpChannels` storage
--   snapshot diffed per session, which is a different worker.

create table xcm.message_links (
    chain_id      text not null,
    block_height  bigint not null,

    -- The two events in this block that `xcm.messages` also records, both
    -- `side = 'sent'`. A link never crosses a chain or a block, because the
    -- evidence for it does not either. NOTE there is no foreign key and there
    -- cannot usefully be one: the correlator has its own checkpoint and can run
    -- ahead of (or without) the `xcm` worker, so a link may name a coordinate
    -- whose observation row is not written yet. The journey endpoint renders
    -- that as an alias with fewer steps than it implies, which is honest.
    --
    -- wire_event_index is the row key: one queued message is delivered once, so
    -- a wire-hash row pairs at most once. The unique index below says the same
    -- of the topic row, in the other direction — without it a rule change could
    -- point two wire rows at one topic and no constraint would notice.
    wire_event_index  integer not null,
    topic_event_index integer not null,

    -- The ids themselves, denormalised on purpose: every read of this table is
    -- "expand this id into its alias set", and joining back to xcm.messages to
    -- discover what the ids WERE would make a one-probe lookup a three-probe
    -- one for no gain.
    wire_hash     text not null,
    topic         text not null,

    -- hrmp | ump. Never dmp: the relay's downward router computes a hash and
    -- discards it without an event, so a DMP send has no wire row to pair with
    -- (0015's `not_covered` line, still true). Never 'unknown' either — a
    -- destination we could not read is a destination we will not pair on.
    transport     text not null,

    -- ------------------------------------------------------------- the claim
    -- HOW this pair was established. A link is an INFERENCE and the schema says
    -- so in three columns rather than presenting one as a fact:
    --
    --   'unique_in_block' — exactly one wire-hash send and exactly one topic
    --                       send of this transport in the block. Nothing else it
    --                       could be. confidence 'high'.
    --   'interleaved'     — n of each, n > 1, strictly alternating in event
    --                       order (w1 < t1 < w2 < t2 …) with no candidate
    --                       between a pair. That is the emission order the
    --                       pallet produces, so the k-th topic belongs to the
    --                       k-th wire. confidence 'medium', because a block
    --                       where one send lost its `Sent` and another lost its
    --                       wire event can alternate by coincidence.
    --
    -- Any other shape records NO ROW. A block with 2 wire sends and 1 topic send
    -- is not a puzzle to solve, it is a question we cannot answer, and the
    -- honest output is that the wire hash reaches only its own half.
    rule          text not null,
    confidence    text not null,
    -- What the rule was decided on, so a reader can audit the claim without
    -- re-deriving the block: `transport_candidates` (n for this transport),
    -- `block_sends` (every wire/topic send in the block, ALL transports — the
    -- number that says how crowded the block was, which the per-transport count
    -- cannot), `ordinal` and `event_gap`.
    evidence      jsonb not null,

    -- Lineage (Invariant 3). correlator_version is the rebuild key: bump it on
    -- any rule change and re-run `xcm-correlate` over the range.
    runtime_version    bigint not null,
    correlator_version integer not null,

    primary key (chain_id, block_height, wire_event_index)
) partition by list (chain_id);

create table xcm.message_links_default partition of xcm.message_links default;

-- One topic row belongs to at most one wire row, per block. The partition-key
-- column `chain_id` is present, which is what makes a UNIQUE index legal on a
-- partitioned table (Postgres requires every partition-key column, and there is
-- exactly one — `block_height` is here because the constraint is per block, not
-- because the partitioning demands it).
create unique index message_links_topic_uidx
    on xcm.message_links (chain_id, block_height, topic_event_index);

-- The reader, in both directions: "what else is this id known as". Neither is
-- keyed on chain_id first, for 0013's reason — the caller pasted an id and has
-- no chain to put in a leading column.
create index message_links_wire_idx  on xcm.message_links (wire_hash);
create index message_links_topic_idx on xcm.message_links (topic);

-- REBUILD SEMANTICS, stated here because a DBA will look for them here.
--
-- The sink writes a block's links as a DELETE-then-INSERT inside one
-- transaction, so a block's links are a pure function of that block and a
-- re-run converges under any order. The one asymmetry worth knowing: the shared
-- worker runtime does not call a sink with zero rows, so a rule change that
-- makes a block produce FEWER links leaves the old ones in place. Re-running
-- with a narrowed rule is therefore:
--
--   delete from xcm.message_links where correlator_version < <new>;
--   -- then xcm-correlate the ranges again
--
-- which is exactly why correlator_version is on the row.
comment on table xcm.message_links is
    'wire_hash <-> topic aliases for one XCM message, inferred within one block on one chain. An INFERENCE with its rule, confidence and evidence attached — never presented as an event the chain emitted.';
