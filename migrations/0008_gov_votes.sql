-- 0008_gov_votes: conviction votes + delegations (Phase 2, slice 3).
--
-- WHO decided, with how much, and who lent them the power. Three append-only
-- fact tables + two projections + one state-anchor table, all rebuildable from
-- core.events (which rebuild from raw) except the anchors, which are state
-- reads recorded as immutable observations.
--
-- HONEST COVERAGE IS A FIRST-CLASS COLUMN HERE. pallet-conviction-voting's
-- event vocabulary grew over time (read from the crates.io sources):
--   … ≤ v20 Delegated(who, target) / Undelegated(who) — no Voted event AT ALL,
--           which is how relay OpenGov ran for years
--   v38–v40 + Voted { who, vote } / VoteRemoved { who, vote } — no poll index
--   v41–v42 + VoteUnlocked { who, class }
--   v43+    poll_index on Voted/VoteRemoved; class on Delegated/Undelegated
-- Polkadot relay + Asset Hub at spec 2003002 are on the v43+ shape (checked in
-- the committed metadata fixtures: `poll_index` is present). Older eras are
-- therefore INCOMPLETE BY CONSTRUCTION, and we say so in the data instead of
-- guessing: `attribution` = 'event' (the event carried its own subject) or
-- 'unattributed' (it did not — referendum_id / track_id stay NULL). A later
-- slice can backfill those from the originating extrinsic's call args and flip
-- attribution to 'extrinsic'; nothing is silently dropped in the meantime.
--
-- Vote weights follow pallet-conviction-voting's Tally::add EXACTLY
-- (pallet-conviction-voting 49.0.0 src/types.rs, src/conviction.rs):
--   Standard{vote, balance}: votes = conviction==None ? balance/10 : balance*n
--                            ayes += votes (aye) | nays += votes (nay)
--                            support += balance, only when aye
--   Split{aye, nay}:         ayes += aye/10, nays += nay/10, support += aye
--   SplitAbstain{a, n, ab}:  ayes += a/10, nays += n/10, support += a + ab
-- Aggregate these rows LAST-WRITE-WINS per (voter, poll) — never as a sum:
-- the pallet emits no VoteRemoved when a voter CHANGES an existing vote (the
-- old one leaves the tally silently), so summing double-counts re-voters.
-- gov.vote_positions is that aggregate. Even it is the on-chain tally MINUS
-- delegated power, which the pallet adds from `Delegations` state, not events.

-- ------------------------------------------------------------------- votes

-- One row per conviction-voting (or ranked-collective) vote EVENT. The PK is
-- (chain, height, event) — not the referendum — precisely so an unattributable
-- legacy event can still be recorded with referendum_id NULL.
create table gov.votes (
    chain_id        text not null,
    block_height    bigint not null,
    event_index     integer not null,
    class           text not null,              -- referenda | fellowship_referenda
    referendum_id   bigint,                     -- NULL = unattributable (see above)
    voter           bytea not null,
    kind            text not null,              -- voted | vote_removed
    vote_type       text not null,              -- standard | split | split_abstain | ranked
    -- capital (plancks). NULL for 'ranked' votes, which are rank-weighted
    -- vote COUNTS with no balance behind them.
    aye_balance     numeric,
    nay_balance     numeric,
    abstain_balance numeric,
    conviction      smallint,                   -- 0..6; NULL for split/abstain/ranked
    conviction_label text,                      -- none|locked1x…locked6x
    -- post-conviction weights, exactly as the pallet tallies them
    aye_votes       numeric not null,
    nay_votes       numeric not null,
    support         numeric not null,
    attribution     text not null,              -- event | unattributed
    data            jsonb not null,             -- full event fields, schema-on-read
    runtime_version bigint not null,            -- lineage
    -- lineage. NOTE: a bump does NOT rebuild in place — the fact inserts are
    -- conflict-ignoring and the projections are guarded by a strict (height,
    -- event) comparison, so a re-map is a no-op. Rebuilding means deleting the
    -- four gov vote tables first, then re-running votes-range (same doctrine
    -- as gov.referenda).
    mapper_version  integer not null,
    primary key (chain_id, block_height, event_index)
) partition by list (chain_id);

create table gov.votes_default partition of gov.votes default;

-- the PK (chain, block, event) cannot serve the referendum read path, so this
-- index earns its write cost. There is deliberately NO by-voter index on the
-- FACT table: the account surface reads gov.vote_positions, which has its own
-- (chain, voter) index — an unused index is pure write amplification (0006).
create index votes_referendum_idx
    on gov.votes (chain_id, class, referendum_id, block_height, event_index);

-- Latest vote per (chain, class, referendum, voter) — the "current votes on
-- this referendum" projection, ordering-guarded like gov.referenda so replay
-- in any order converges. Unattributed events never reach it (no referendum
-- to attach to); they live in gov.votes only.
create table gov.vote_positions (
    chain_id        text not null,
    class           text not null,
    referendum_id   bigint not null,
    voter           bytea not null,
    -- false after a vote_removed (kept, not deleted: "X voted then withdrew"
    -- is history, and re-voting must converge under replay)
    active          boolean not null,
    vote_type       text not null,
    aye_balance     numeric,
    nay_balance     numeric,
    abstain_balance numeric,
    conviction      smallint,
    conviction_label text,
    aye_votes       numeric not null,
    nay_votes       numeric not null,
    support         numeric not null,
    status_height     bigint not null,          -- ordering guard
    status_event_index integer not null,
    runtime_version bigint not null,
    mapper_version  integer not null,
    updated_at      timestamptz not null default now(),
    primary key (chain_id, class, referendum_id, voter)
);

create index vote_positions_voter_idx on gov.vote_positions (chain_id, voter);

-- ------------------------------------------------------------- delegations

-- Delegation edges as they happen. The pallet event carries who → whom (and,
-- since v43, the track) but NEVER the amount or conviction: those live in
-- ConvictionVoting.VotingFor state, recorded in gov.voting_anchors.
create table gov.delegation_events (
    chain_id        text not null,
    block_height    bigint not null,
    event_index     integer not null,
    class           text not null,
    track_id        integer,                    -- NULL = pre-v43 event shape
    delegator       bytea not null,
    target          bytea,                      -- NULL for undelegated
    kind            text not null,              -- delegated | undelegated
    attribution     text not null,              -- event | unattributed
    data            jsonb not null,
    runtime_version bigint not null,
    mapper_version  integer not null,
    primary key (chain_id, block_height, event_index)
) partition by list (chain_id);

create table gov.delegation_events_default
    partition of gov.delegation_events default;

create index delegation_events_delegator_idx
    on gov.delegation_events (chain_id, delegator, block_height desc);

-- Current delegation per (chain, class, track, delegator). Track-less legacy
-- events are excluded (we will not invent the track they applied to).
create table gov.delegations (
    chain_id        text not null,
    class           text not null,
    track_id        integer not null,
    delegator       bytea not null,
    target          bytea,                      -- NULL once undelegated
    active          boolean not null,
    status_height     bigint not null,          -- ordering guard
    status_event_index integer not null,
    runtime_version bigint not null,
    mapper_version  integer not null,
    updated_at      timestamptz not null default now(),
    primary key (chain_id, class, track_id, delegator)
);

-- the account surface filters (chain, delegator) — the PK's leading columns
-- are (class, track), so it cannot serve that
create index delegations_delegator_idx on gov.delegations (chain_id, delegator);
-- "who delegates to me" — the reverse edge, for delegated-power resolution
create index delegations_target_idx
    on gov.delegations (chain_id, class, track_id, target);

-- --------------------------------------------------------- voting anchors

-- ConvictionVoting.VotingFor(account, track) read from state at a block and
-- decoded against block-correct metadata — end-of-block semantics, immutable
-- observations (same doctrine as balances.balance_anchors). This is the ONLY
-- honest source for (a) how much an account delegated and with what conviction
-- and (b) how much delegated power an account carries into its own votes:
-- neither number appears in any event.
create table gov.voting_anchors (
    chain_id        text not null,
    account_id      bytea not null,
    class           text not null,
    track_id        integer not null,
    block_height    bigint not null,
    mode            text not null,              -- casting | delegating
    delegating_target     bytea,
    delegating_balance    numeric,
    delegating_conviction smallint,
    delegating_conviction_label text,
    casting_vote_count    integer,              -- direct votes held in this class
    -- delegations RECEIVED (post-conviction votes + raw capital)
    delegations_votes     numeric,
    delegations_capital   numeric,
    prior_until     bigint,                     -- PriorLock: unlock block…
    prior_balance   numeric,                    -- …and the amount still locked
    raw             jsonb not null,             -- full decoded Voting, schema-on-read
    spec_version    bigint,                     -- metadata used (lineage)
    decoder_version integer not null,           -- adapter VOTING_DECODER_VERSION
    source          text not null,              -- anchor-voting
    note            text,                       -- e.g. 'absent' (storage default)
    created_at      timestamptz not null default now(),
    primary key (chain_id, account_id, class, track_id, block_height)
);
