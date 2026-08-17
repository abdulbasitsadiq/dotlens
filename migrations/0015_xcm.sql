-- 0015_xcm: XCM message facts (Phase 3, slice 2).
--
-- ONE TABLE, AND IT DELIBERATELY DOES NOT STITCH ANYTHING. A journey is two or
-- more one-sided observations that may or may not be joinable, and the research
-- behind this slice established that on the chains dotlens actually indexes they
-- frequently are NOT (see the id rules below). So this migration records the
-- OBSERVATIONS — what one chain said it sent, what one chain said it processed —
-- and the correlation layer (ARCHITECTURE §10) is a later slice that reads them.
-- Shipping a `journeys` table now would mean shipping rows that silently mean
-- "we guessed", which is the one failure mode a forensics-adjacent product
-- cannot afford (PRODUCT.md: a trace that reaches an unindexed chain must STOP
-- AND SAY SO).
--
-- WHAT IS NOT HERE, AND WHY, so the next reader does not think it was forgotten:
--
--   xcm.channels — HRMP channel history CANNOT be built from events. The relay's
--   `Hrmp` pallet emits `OpenChannelRequested` / `OpenChannelAccepted` /
--   `ChannelClosed` from its EXTRINSICS, but the channel itself is created and
--   destroyed in `process_hrmp_open_channel_requests` /
--   `process_hrmp_close_channel_requests` / `clean_hrmp_after_outgoing` at the
--   next SESSION BOUNDARY, and none of those functions contains a single
--   `deposit_event`. Genesis `preopen_hrmp_channels` emit nothing ever, and a
--   para being offboarded has its channels torn down silently. An events-only
--   channel table would therefore be missing every genesis channel and every
--   teardown, and would date every open to the request block rather than to the
--   session it actually opened in. The honest construction is a storage snapshot
--   of `HrmpChannels` diffed at each `session.NewSession` — a different kind of
--   worker, and its own slice.
create schema if not exists xcm;

create table xcm.messages (
    chain_id      text not null,
    block_height  bigint not null,
    event_index   integer not null,

    -- 'sent' | 'received' | 'local'. NEVER a journey: one row is one chain's
    -- half of one message, and the two halves are joined (or honestly not
    -- joined) later. 'local' is `pallet_xcm.execute` — an XCM this chain ran on
    -- itself, which has no counterparty and no id, so it can never correlate.
    side          text not null,
    -- hrmp | ump | dmp | local | unknown. On the receiving side this is exact, because
    -- `messageQueue`'s origin discriminates it (`Parent` = DMP, `Sibling(id)` =
    -- HRMP, `Ump(Para(id))` = UMP). On the sending side it is exact only for the
    -- queue pallets (`xcmpQueue` = HRMP, `parachainSystem` = UMP); a
    -- `polkadotXcm.Sent` names a destination Location, not a transport, so it is
    -- 'unknown' unless the destination makes it obvious.
    transport     text not null,

    -- ------------------------------------------------------- the correlation
    -- 0x-hex, 32 bytes. THE HARD-WON PART OF THIS SCHEMA: there are TWO ids per
    -- message and they are not interchangeable.
    --
    --   'topic'      — `pallet_xcm.Sent.message_id`, which `WithUniqueTopic`
    --                  produced from `frame_system::unique` and appended to the
    --                  wire as a trailing `SetTopic`. It is NOT a hash of the
    --                  message: it mixes INTRABLOCK_ENTROPY, so it cannot be
    --                  recomputed later from anything. Capture it or lose it.
    --   'wire_hash'  — `xcmpQueue.XcmpMessageSent.message_hash` /
    --                  `parachainSystem.UpwardMessageSent.message_hash`, which is
    --                  blake2_256 over the encoded `VersionedXcm` actually queued.
    --   'ambiguous'  — `messageQueue.{Processed,ProcessingFailed}.id`, which is
    --                  the trailing SetTopic if the receiving runtime's barrier
    --                  is wrapped in `TrailingSetTopicAsId` AND the message
    --                  carried one, and otherwise blake2_256 of the bytes. THE
    --                  EVENT DOES NOT SAY WHICH. Recording it as 'topic' would
    --                  be a guess; a later join that succeeds is what proves it.
    --
    -- A sender emits BOTH ids for one HRMP/UMP message, in the same block, from
    -- two different pallets — `WithUniqueTopic::deliver` throws the inner
    -- router's hash away and returns the topic. Recording only one of them
    -- halves the join; treating them as one id double-counts every message.
    message_id    text,
    -- 'none' where the event carries no id at all (a local execute, or a
    -- pre-2023 `Sent` from before message ids existed).
    id_kind       text not null,

    -- ---------------------------------------------------------- the parties
    -- 'para:2034' | 'parent' | 'here' | null — the counterparty as this side
    -- knows it. On the receiving side it is the QUEUE the message came from,
    -- which is a channel, not a caller: the receiver never learns which account
    -- or pallet on the sending chain authorised anything.
    counterparty  text,
    -- Sender-only facts, kept whole (schema-on-read).
    origin_location jsonb,
    destination     jsonb,
    -- The instruction list, when the event carries one. EMPTY IS MEANINGFUL, not
    -- a decode failure: the executor deliberately passes `None` for forwarded
    -- sends ("Avoid logging the full XCM message…"), so pallet-originated sends
    -- have a message and executor-forwarded ones do not. That is exactly what
    -- `forwarded` records.
    message       jsonb,
    forwarded     boolean not null default false,

    -- ---------------------------------------------------------- the outcome
    -- sent | send_failed | processed | processing_failed | overweight_enqueued
    -- | attempted
    --
    -- `processed` DOES NOT MEAN THE XCM DID WHAT IT SAID. pallet-message-queue's
    -- own doc on `Processed.success`: "It *solely* means that the MQ pallet will
    -- treat this as a success condition and discard the message." `success:
    -- false` is `Outcome::Incomplete` — the message ran and did not finish. Same
    -- doctrine as slice 9's `WhitelistedCallDispatched` and slice 3's dry-run
    -- status: the transport succeeding is not the intent succeeding.
    status        text not null,
    -- NULL where the event carries no verdict (a bare `Sent`), which is not the
    -- same as false. Only `processed` rows have a meaningful boolean.
    success       boolean,
    error         jsonb,
    weight_used   jsonb,

    data          jsonb not null,
    runtime_version bigint not null,
    mapper_version  integer not null,

    primary key (chain_id, block_height, event_index)
) partition by list (chain_id);

create table xcm.messages_default partition of xcm.messages default;

-- The reader: "every XCM observation on this chain, newest first" and "find this
-- id anywhere". The second is what the correlation slice will join on and what
-- the search resolver's `xcm <hash>` codeword — refused since Phase 2 slice 10
-- with "XCM journeys land in Phase 3" — will finally answer.
--
-- NOT keyed on chain_id first, for the same reason 0013's hash indexes are not:
-- the question is "which chain saw this id", and the caller has no chain to put
-- in a leading column. One child index per partition, each an O(log n) probe.
create index messages_id_idx on xcm.messages (message_id) where message_id is not null;
create index messages_chain_height_idx on xcm.messages (chain_id, block_height desc, event_index);
