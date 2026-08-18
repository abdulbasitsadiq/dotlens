-- 0017_xcm_remote: the transport asymmetry (Phase 3, slice 4).
--
-- NO SCHEMA CHANGE. This migration exists to amend two applied ones — 0015 and
-- 0016 — without rewriting them, which is the project's own convention
-- (CLAUDE.md: "prefer editing migrations forward, never rewriting applied ones",
-- and sqlx stores a SHA-384 of the file bytes, so an edit in place costs a DB
-- wipe or a manual checksum update). The vocabulary those files document has
-- changed, and a comment that is silently stale is worse than one that is
-- superseded in a file a reader can find.
--
-- WHAT CHANGED, AND THE MEASUREMENT THAT FORCED IT.
--
-- Slice 3's drill correlated 20,681 live blocks: 181 queued sends, 180 linked,
-- 99.4%. The single refusal was Asset Hub #19407624 — event 6
-- `xcmpQueue.XcmpMessageSent` (transport `hrmp`), event 7 `polkadotXcm.Sent`
-- addressed to `{parents: 2, X1[GlobalConsensus(Ethereum{chain_id: 1})]}`. One
-- wire send, one topic send, adjacent, in order: a textbook `unique_in_block`
-- shape, refused for one reason only — the two sides reported different
-- transports.
--
-- THEY DISAGREE BY CONSTRUCTION AND NEITHER IS WRONG. A wire event's transport
-- comes from WHICH PALLET QUEUED IT: the immediate hop, here XCMP to Bridge Hub.
-- A `Sent`'s comes from the FINAL destination, which is Ethereum and is no local
-- transport at all. So the refusal was never about that block — it is every
-- message addressed beyond the immediate neighbour, i.e. every Snowbridge export
-- from Asset Hub. A systematic blind spot rather than an unlucky sample.
--
--   xcm.messages.transport      gains 'remote'
--   xcm.messages.counterparty   gains the 'remote:<consensus>' prefix
--   xcm.message_links.rule      gains 'remote_destination'
--
-- The counterparty half is a plain CORRECTNESS fix and is worth reading twice: a
-- bridged destination such as `{parents: 2, X2[GlobalConsensus(Kusama),
-- Parachain(1000)]}` CONTAINS a `Parachain` junction, so the old walk reported
-- `para:1000` — which in this index names POLKADOT's Asset Hub. A foreign chain
-- rendered as one of ours reads like a fact, joins like a fact, and would have
-- made the journey endpoint's counterparty mirror contradict itself the day
-- Kusama is registered.

comment on column xcm.messages.transport is
    'hrmp | ump | dmp | local | remote | unknown. Exact on the receiving side (the messageQueue origin discriminates it) and on the sending side for the queue pallets, which name the IMMEDIATE HOP. A polkadotXcm.Sent names a destination Location instead, so it is: remote when that Location crosses a consensus boundary (a GlobalConsensus junction — a positive fact, and the first hop is then whatever the queue pallet says), and unknown when the Location could not be classified at all (ignorance, which is a different claim).';

comment on column xcm.messages.counterparty is
    'para:<id> | parent | here | remote:<consensus>[/para:<id>] | null. The remote: prefix is load-bearing: a bridged destination contains a Parachain junction belonging to ANOTHER consensus, and rendering it bare would name one of our own chains. Searched by the GlobalConsensus KEY, never by network name — AccountId32/AccountIndex64/AccountKey20 each carry their own network field, so a local sibling destination can mention Ethereum without crossing anything.';

comment on column xcm.message_links.rule is
    'unique_in_block (one wire send + one topic send of that transport; confidence high) | interleaved (n of each, strictly alternating; medium) | remote_destination (the topic is addressed to another consensus system, so it cannot corroborate the transport and cannot contradict it either — the link takes its transport from the WIRE event, which is the side that knows how the message actually left; medium). An unreadable destination is NOT paired: remote is a positive fact, unknown is ignorance.';

-- ---------------------------------------------------------------- REBUILDING
--
-- Both versions moved, and they rebuild DIFFERENTLY. Stated here because the
-- asymmetry is easy to get wrong and silently half-apply:
--
--   XCM_CORRELATOR_VERSION 1 -> 2 (widened). `xcm.message_links` is written
--   DELETE-then-INSERT per block, so a plain re-run converges:
--
--       dotlens-node xcm-correlate <chain> <from> <to>
--
--   XCM_MAPPER_VERSION 1 -> 2 (reclassified). `xcm.messages` is APPEND-ONLY and
--   insert-ignore, so re-running `xcm-range` over an already-mapped range is a
--   no-op and the old rows keep their coarser transport and counterparty
--   forever. Re-mapping is explicitly two steps:
--
--       delete from xcm.messages where mapper_version < 2;
--       -- then xcm-range the ranges again, and xcm-correlate after them
--
--   Deleting first is safe by Invariant 1: every row is a pure function of raw
--   blocks that the raw store still holds, and slice 3 decoded 20,681 blocks
--   straight from raw at ~21 blocks/s with no RPC at all.
--
-- ORDER MATTERS ONE WAY ONLY: correlate AFTER mapping, or a link will name a
-- coordinate whose observation row has been deleted and not yet rewritten — the
-- journey endpoint renders that as an alias with fewer steps than it implies,
-- which is honest but confusing to read mid-rebuild.
