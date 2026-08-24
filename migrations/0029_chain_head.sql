-- 0029_chain_head: the chain's own head, OBSERVED (Phase 3.5, slice 1b).
--
-- Slice 1a shipped `GET /v1/freshness/{chain}` with `raw_behind_chain` hardcoded
-- null and its own `not_covered` naming the gap: "THE CHAIN'S OWN HEAD IS NOT
-- READ ... a raw follower that stopped an hour ago makes the whole stack look
-- internally consistent and current." This table closes it.
--
-- The live follower ALREADY reads the head every tick and throws it away:
-- `live::tick` computes `let target = source.finalized_height().await?` purely
-- as a loop bound. So the fact is free at the point it is observed, and reading
-- it in the API instead would put an RPC call in a read path.
--
-- ========================================================================
-- WHY THIS IS NOT A RESERVED MODULE KEY IN core.indexer_state
-- ========================================================================
--
-- That was the cheaper option and it was rejected on three counts, any one of
-- which is sufficient. `core.indexer_state` means PROCESSED UP TO; a head means
-- OBSERVED AT. They are different verbs and they want different guards.
--
--   1. `advance()` REFUSES a non-advancing write. Its upsert is guarded
--      `where core.indexer_state.last_height < excluded.last_height`, and the
--      store returns `CheckpointError::Regression` when zero rows are affected.
--      A head that has not moved since the last tick is the NORMAL case on any
--      chain whose block time exceeds the poll interval, so every such tick
--      would raise an error the follower logs as a failure and backs off on.
--
--   2. Even if the error were swallowed, that guard means `updated_at` DOES NOT
--      MOVE unless the height does. The whole point of this slice is that the
--      head's OWN age is reported beside the delta: a follower that died leaves
--      a frozen head, and a frozen head makes `raw_behind_chain` fall to 0,
--      which reads as "caught up with the chain" — wrong in the flattering
--      direction (PATTERNS A5). A storage that cannot record "I looked again
--      and it had not moved" cannot answer the question this table exists for.
--
--   3. `last_hash text not null` would have to be fabricated. `finalized_height`
--      returns a height and no hash; inventing a value to satisfy a NOT NULL is
--      "never fabricate a candidate, a derivation or a window" (A6) in its
--      smallest form. And a fake hash in the column every resume path reads is
--      the kind of thing a future generic sweep over `indexer_state` would
--      believe.
--
-- The same argument decided the WRITER's shape: the head is recorded through its
-- own `ChainHeadSink` rather than through `CheckpointStore`, because rejecting
-- the conflation at the table and then accepting it at the trait would leave the
-- conflation in place one layer up. The precedent it copies is
-- `RuntimeVersionSink` — a fact the live worker observes in passing, handed to a
-- durable sink, with a Noop for DB-less runs.
--
-- ========================================================================
-- LATEST WRITE WINS, DELIBERATELY, AND IT IS NOT DEFECT E1
-- ========================================================================
--
-- There is NO `greatest()` on the height and NO guard against it going down.
-- The stored row is not a merge of facts from several writers whose outcome must
-- not depend on arrival order (that is `merge_spend`, and E1 is about exactly
-- that shape). It is ONE observation, from one follower, and the row IS "the
-- latest observation". `greatest()` would manufacture a monotonic head that no
-- single observation supports, and it would make `observed_at` silently mean
-- "when we last saw a new maximum" — reintroducing the ambiguity that made
-- option (1) above wrong.
--
-- The head therefore CAN move down: `finalized_height` rotates endpoints, and a
-- lagging endpoint legitimately reports a lower finalized head than the previous
-- one did. That is why `raw_behind_chain` is SIGNED and never saturated — the
-- reader renders it negative and says, in the payload, that an observation is
-- behind us rather than the chain being behind us.
--
-- ========================================================================
-- WHAT IS NOT HERE
-- ========================================================================
--
-- NO HISTORY. One row per chain, primary key `chain_id`. A series of head
-- observations has no reader — "an index arrives with its reader" (E3) and so
-- does a table. Phase 4's chain-uptime item is derived from blocks, not from
-- these observations, and it wants four states this row cannot supply.
--
-- NO `first_seen_at` FOR THE HEIGHT. It would distinguish "the chain has not
-- produced a finalized block in ten minutes" from "we have not looked in ten
-- minutes", which is a real and different fact — but nothing on any surface
-- asks it yet, and both failures the freshness report DOES have to distinguish
-- are answered by (height, observed_at) alone.
--
-- NO BEST BLOCK. `finalized_height` is the finalized head, which is the correct
-- yardstick for a raw follower that ingests only finalized heights. The chain's
-- best block is ahead of it by the finality lag; `tip.rs` is the only worker
-- that reads it and it deliberately keeps no checkpoint ("the window is tiny and
-- every tick re-verifies it from scratch"), so nothing durable records it. The
-- freshness reader says so in `not_covered` rather than letting a page render
-- `raw_behind_chain: 0` as "at the tip".
--
-- NO INDEX beyond the primary key. The only reader is
-- `where chain_id = $1`, which is the primary key itself.

create table core.chain_head (
    chain_id          text        primary key,
    -- The FINALIZED head the source reported, not the best block.
    finalized_height  bigint      not null,
    -- When the follower LOOKED — bound by the caller, never `default now()`, so
    -- the memory and Postgres writers agree and a test can pin the clock. This
    -- column is rewritten on every tick even when the height has not changed,
    -- which is the opposite of `core.indexer_state.updated_at` and is the whole
    -- reason this is a separate table.
    observed_at       timestamptz not null
);

comment on table core.chain_head is
    'The latest OBSERVATION of each chain''s finalized head, recorded by the live '
    'follower from the value it already reads as a loop bound. One row per chain, '
    'no history. Latest write wins, including downward: a lagging endpoint may '
    'report a lower finalized head than the previous one, and the reader renders '
    'the resulting delta signed rather than clamping it to zero.';

comment on column core.chain_head.finalized_height is
    'The FINALIZED head. The chain''s best block is ahead of it by the finality '
    'lag and is not recorded anywhere, so a delta of 0 against this column means '
    '"level with the finalized head", never "at the tip".';

comment on column core.chain_head.observed_at is
    'When the follower last LOOKED, not when the height last CHANGED. Refreshed '
    'on every tick, including ticks with nothing to ingest — a caught-up follower '
    'is precisely when this row is the only evidence it is still alive. Read it '
    'beside the delta, never the delta alone.';
