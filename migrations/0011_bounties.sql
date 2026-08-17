-- 0011_bounties: bounties, child bounties and multi-asset bounties
-- (Phase 2, slice 7). THE OUTFLOW NEITHER 0009 NOR 0010 CAN SEE.
--
-- Migration 0009 said it plainly and this migration is the promised fix:
-- bounty funding leaves the treasury through the `SpendFunds` HOOK. Read from
-- pallet-bounties' own `spend_funds()`, the pot pays the bounty account and
-- emits `bounties.BountyBecameActive` — NOT `treasury.Awarded`, NOT a pot
-- event. So every DOT that became a bounty is invisible to `treasury.spends`,
-- invisible to the pot stream, and (until this slice) invisible to holdings.
-- ECOSYSTEM §6 puts ~$13M there.
--
-- THREE PALLETS, ONE TABLE, because they are three generations of one idea and
-- a page that shows them separately is showing an accident of history:
--
--   `bounties`             (pallet-bounties, 12 event variants at 48.0.0)
--       The original. Native-token only. Ids are a single `BountyIndex`.
--       Vocabulary verified across all 52 published versions: named fields
--       since 4.0.0, growth only — `BountyApproved`/`Curator{Proposed,
--       Unassigned,Accepted}` added at 24.0.0, `DepositPoked` at 40.0.0.
--       Nothing was ever removed or renamed after 4.0.0.
--
--   `child_bounties`       (pallet-child-bounties, 4 variants, STABLE since
--       4.0.0 through 48.0.0: Added/Awarded/Claimed/Canceled). Ids are a PAIR
--       `(index, child_index)`, both mandatory. Also native-token only.
--       ITS EVENT VOCABULARY IS STABLE; ITS ID SPACE AND ITS ACCOUNTS ARE NOT
--       — see the derivation table at the bottom of this file.
--
--   `multi_asset_bounties` (pallet-multi-asset-bounties, 13 variants;
--       vocabulary verified across 0.2.0..=0.7.0, where the event enum is
--       byte-identical)
--       The unified, ASSET-DENOMINATED generation — what referendum 1930
--       funds. Two things make it different in kind, and both shape this
--       schema:
--         1. It folds parent and child into ONE pallet and ONE id space:
--            almost every event carries `(index, child_index: Option<_>)`,
--            where None means the parent bounty itself. Only `BountyCreated`
--            (parent) and `ChildBountyCreated` (child) differ in shape.
--         2. `BountyPayoutProcessed` carries an `asset_kind`, so a bounty
--            payout is denominated exactly like a treasury spend is — and
--            resolves through the SAME `core.assets` join slice 6 built.
--       It also has the `Paid`/`PaymentFailed`/`payment_id` retry shape that
--       pallet-treasury's modern flow has, which is why the payment columns
--       below carry their OWN coordinate (0009's hard-won lesson: a status
--       coordinate cannot stand in for a payment one).
--
-- BECAUSE THE MODERN PALLET ALREADY MODELS PARENT AND CHILD AS ONE THING, the
-- schema adopts its shape and back-fits the legacy pair into it, rather than
-- inventing a fourth model. A legacy `child_bounties` row is simply one whose
-- `child_id` is set; a legacy `bounties` row is one whose `child_id` is not.
--
-- THE SENTINEL, stated rather than discovered: `child_id` is NOT NULL with -1
-- meaning "the parent bounty itself", because a NULL cannot participate in a
-- primary key and this projection's identity IS (instance, bounty, child).
-- Every read path renders -1 back to null; the API never leaks it. A check
-- constraint pins the range so the sentinel cannot be confused with data.
--
-- WHAT THIS SLICE STILL DOES NOT COVER, stated here so a balance sheet built
-- on it is honest: a bounty's *remaining* balance is its ACCOUNT balance, not
-- a number any event carries. That is why the accounts matter more than the
-- events (see the treasury_accounts note below) — the event stream says what
-- was promised and paid, the account says what is left.
--
-- AND THE ACCOUNT SAYS IT EXACTLY, which is worth stating rather than implying:
-- `pallet-bounties::spend_funds` funds a bounty with
-- `T::Currency::deposit_creating(&bounty_account_id(index), bounty.value)` — a
-- MINT into the derived account. So the funding, the curator fee and every
-- payout are ordinary `balances.*` rows on that account, already in
-- balances.balance_changes, at exactly the `BountyBecameActive` height.

-- ------------------------------------------------------- bounties.bounty_events
--
-- Append-only, partitioned by chain, keyed by EVENT — the same shape as
-- gov.votes and treasury.spend_events, and for the same reason: an event we
-- cannot attribute to a subject must still be recordable.
create table treasury.bounty_events (
    chain_id        text not null,
    block_height    bigint not null,
    event_index     integer not null,
    -- bounties | child_bounties | multi_asset_bounties (adapter vocabulary,
    -- mapped from the pallet name exactly as the treasury and gov mappers do)
    instance        text not null,
    bounty_id       bigint not null,
    -- -1 = the parent bounty itself. See the sentinel note above.
    child_id        bigint not null default -1,
    -- proposed|approved|became_active|awarded|claimed|canceled|extended|
    -- curator_proposed|curator_accepted|curator_unassigned|deposit_poked|
    -- created|child_created|payout_processed|funding_processed|
    -- refund_processed|paid|payment_failed|value_increased|rejected
    kind            text not null,
    -- the status this event moves the bounty to; NULL = information only
    -- (a curator change or a deposit poke does not move a bounty's state)
    status          text,
    -- the figure this event carries, and WHAT IT MEANS — 0009's `figure_kind`
    -- doctrine, which exists because summing the wrong rows is how a treasury
    -- page lies. Here: 'flow' for a payout or a funding, 'snapshot' for
    -- `BountyValueIncreased`'s new_value (a bounty's SIZE, not money moving).
    amount          numeric,
    figure_kind     text,
    -- `BountyRejected.bond` — the PROPOSER'S slashed bond. Its own column so
    -- it can never be summed as bounty spending (0009's rule for `slashed`).
    bond            numeric,
    curator         bytea,
    beneficiary     bytea,
    -- the modern pallet's beneficiary is a LOCATION (T::Beneficiary), so the
    -- 32-byte form is extracted only where the location actually names an
    -- account — never invented (0009's rule, and its reviewer's catch)
    beneficiary_location jsonb,
    -- MULTI-ASSET ONLY. Same three columns, same meanings and same producer as
    -- treasury.spends (migration 0010): the raw VersionedLocatableAsset, its
    -- normalized {chain, asset} halves, and the resolved key where no metadata
    -- is needed. A bounty payout and a treasury spend are denominated the same
    -- way and must be readable the same way.
    asset_kind      jsonb,
    asset_location  jsonb,
    asset_key       text,
    -- Paid / PaymentFailed. Retryable, exactly like a treasury payout.
    payment_id      text,
    data            jsonb not null,             -- full event, schema-on-read
    runtime_version bigint not null,            -- lineage
    mapper_version  integer not null,           -- lineage (rebuild = delete + re-range)
    primary key (chain_id, block_height, event_index),
    constraint bounty_events_child_id_range check (child_id >= -1)
) partition by list (chain_id);

create table treasury.bounty_events_default
    partition of treasury.bounty_events default;

-- the PK is keyed by block and cannot serve the one read that matters — a
-- single bounty's timeline — so this index earns its write cost. Ordered
-- exactly as the query reads it.
create index bounty_events_bounty_idx
    on treasury.bounty_events (chain_id, instance, bounty_id, child_id,
                               block_height, event_index);

-- ------------------------------------------------------------ treasury.bounties
--
-- The projection: current state of every bounty and child bounty, converging
-- under ANY replay order. Same machinery as treasury.spends — an ordering
-- guard on the status coordinate, first-non-null-wins on the value columns,
-- and a SEPARATE coordinate for the payment, because `PaymentFailed` and
-- `Paid` both carry a payment_id while other events move the status without
-- carrying one.
create table treasury.bounties (
    chain_id        text not null,
    instance        text not null,
    bounty_id       bigint not null,
    child_id        bigint not null default -1,
    -- proposed|approved|funded|curator_proposed|active|awarded|claimed|
    -- canceled|rejected|payment_failed|unknown
    --
    -- 'unknown' = a non-status event arrived before any status-bearing one
    -- (guarded at (0,0) so any real status wins) — migration 0006's device,
    -- with a reason of its own here: an info-only event may carry the bounty's
    -- ONLY known VALUE (`BountyValueIncreased` is the sole event in any of the
    -- three pallets that reports one), so dropping such a fact would drop the
    -- number, and recording it without a placeholder would claim a state we
    -- have not seen.
    --
    -- NOTE these are OUR words for what the events say, not a copy of the
    -- pallet's `BountyStatus` enum, which we never read: the legacy pallet's
    -- own statuses (Proposed/Approved/Funded/CuratorProposed/Active/
    -- PendingPayout/ApprovedWithCurator) live in storage, and this projection
    -- is built from the EVENT stream so it stays rebuildable from raw. Where
    -- the two could disagree, the event is what we report and the pallet's
    -- storage is what a state read would show — the same doctrine as slice 4's
    -- `scheduler.Dispatched` finding. Two transitions have NO event at all and
    -- so are structurally invisible here: `check_status` sends a child bounty
    -- whose curator is its parent's straight to Active, and `spend_funds` sends
    -- an ApprovedWithCurator bounty straight to CuratorProposed. There is no
    -- 'pending_payout' and no 'paid': `multiassetbounties.Paid` fires for
    -- funding, refund AND payout, and `retry_payment` fires it alone, so it
    -- moves no status — it carries a payment id and its own coordinate.
    status          text not null,
    -- what the bounty is WORTH (its value), which is not what it has PAID.
    -- A bounty's remaining balance is its ACCOUNT balance and lives in
    -- balances.balance_anchors via the derived bounty account — this column
    -- must never be read as "funds available".
    value           numeric,
    -- Sum of concluded payouts — AND IT IS NET OF THE CURATOR FEE, which no
    -- event carries. `claim_bounty` transfers the fee to the curator and
    -- `payout` to the beneficiary in the SAME call, and only `payout` is
    -- announced, so this column understates what the bounty spent by exactly
    -- the fees. The complete figure is on the bounty's own account (see the
    -- deposit_creating note in the header): every movement is a
    -- balances.balance_changes row there.
    --
    -- THE ONLY COLUMN IN THIS FILE THAT IS NOT A PURE FUNCTION OF THE FACTS.
    -- It ACCUMULATES, and is safe only because the sink adds a payout when the
    -- `treasury.bounty_events` row was really inserted. So the rebuild rule is
    -- not "delete the projection and re-range": treasury.bounty_events and
    -- treasury.bounties must be deleted TOGETHER. Deleting the projection alone
    -- leaves every fact already present, the insert-ignore returns nothing, and
    -- paid_out stays NULL forever — silently, because nothing errors.
    paid_out        numeric,
    bond            numeric,
    curator         bytea,
    beneficiary     bytea,
    beneficiary_location jsonb,
    asset_kind      jsonb,
    asset_location  jsonb,
    asset_key       text,
    payment_id      text,
    payment_height  bigint,                     -- the payment's OWN coordinate
    payment_event_index integer,
    -- the bounty's derived account (see adapter_substrate::accounts::
    -- sub_account). Stored because it is the join to balances/holdings and
    -- deriving it at read time in three places invites three answers.
    account_id      bytea,
    first_seen_height  bigint not null,
    status_height      bigint not null,         -- ordering guard
    status_event_index integer not null,
    runtime_version bigint not null,
    mapper_version  integer not null,
    updated_at      timestamptz not null default now(),
    primary key (chain_id, instance, bounty_id, child_id),
    constraint bounties_child_id_range check (child_id >= -1)
);

-- ONE index, and it is shaped to the ONE query that needs one: the list read is
-- `where chain_id = ? and instance = ? and (status = ? or ? is null)
--  order by bounty_id desc, child_id desc limit ?`.
--
-- The PK (chain_id, instance, bounty_id, child_id) already serves the
-- STATUS-LESS form completely — equality prefix plus a backward index scan for
-- the ordering — so an index on (chain_id, instance, status) alone would earn
-- nothing: it answers the filter and then hands the planner an unsorted set to
-- sort. Carrying the sort keys is what makes it worth its write cost, so it
-- does.
create index bounties_status_idx on treasury.bounties
    (chain_id, instance, status, bounty_id desc, child_id desc);

-- NO index by curator. Nothing reads by curator — there is no `?curator=`
-- filter on any route — and this project has already dropped four unearned
-- indexes (0008, 0009, 0010). Add it with the query, not before it.

-- --------------------------------------------------- treasury_accounts, closed
--
-- Migration 0010 left `role 'bounty'` as a stated future and the holdings
-- endpoint has been printing "bounty and child-bounty accounts — its own
-- slice" in `coverage.not_covered` ever since. This slice closes it, and
-- closes it by DERIVATION rather than by curation, which is the only way it
-- stays true as bounties are created:
--
--   legacy bounty        modl ++ <treasury PalletId> ++ SCALE(("bt", index))
--   legacy child bounty  ≤37.0.0: modl ++ <treasury PalletId> ++ SCALE(("cb", child))
--                        ≥38.0.0: modl ++ <treasury PalletId> ++ SCALE(("cb", parent, child))
--   multi-asset bounty   modl ++ <funding PalletId>  ++ SCALE((b"mbt", index))
--   multi-asset child    modl ++ <funding PalletId>  ++ SCALE((b"mcb", parent, child))
--
-- THE CHILD-BOUNTY ROW HAS TWO ERAS AND THIS TABLE RECORDS NEITHER, so dotlens
-- DERIVES NO legacy child-bounty account at all: it counts them `underivable`
-- and registers nothing. pallet-child-bounties ≤37.0.0 keyed a child's account
-- on a GLOBAL child id; 38.0.0 made the id PER-PARENT and shipped
-- `migration::v1::MigrateToV1Impl`, which renumbers the existing child bounties
-- and transfers each old account's balance to its new address. Polkadot has had
-- child bounties since 2022, so both eras are inside the history indexed here:
-- applying the 3-part rule to a pre-migration id names an address that never
-- existed, and one logical child bounty can appear in this table TWICE, under
-- its old global id and its new per-parent one. Lifting the refusal needs a
-- per-chain migration height or the block's spec_version — data, not a guess.
--
-- all zero-padded to 32 bytes — `into_sub_account_truncating`, the same
-- `modl` family as every other pallet account we already derive. Note the
-- modern pallet uses a fixed-size 3-byte prefix while the legacy pallets use a
-- `&str`, and SCALE encodes those DIFFERENTLY (a str carries a compact length
-- prefix, a [u8; 3] does not). That difference is load-bearing, is pinned by
-- golden vectors in adapter_substrate::accounts, and is verified on-chain by
-- `verify-labels` — a derived address that does not exist on a funded bounty
-- is the check failing, not the chain being odd.
--
-- No schema change is needed here: `treasury.treasury_accounts` already has
-- `role` and `active`, and the holdings sweep already walks whatever is in it
-- (`where active`). That is the payoff of 0010 having added the account LIST
-- rather than a position table.
--
-- `active` FINALLY EARNS ITS COLUMN, and it has to. A bounty in a terminal
-- status (claimed | canceled | rejected) is registered INACTIVE: its account is
-- emptied and removed from pallet storage when the bounty ends, while the
-- holdings sweep is accounts × assets INCLUDING zeros. Polkadot carries ~40
-- live parent bounties and several hundred children against ~900 registered
-- assets, so registering every concluded bounty active forever would turn one
-- sweep into ~10^5 probes and as many permanently-zero anchor rows. The row
-- itself stays — a concluded bounty's address is still a fact and its history
-- is still readable — and the two legacy instances are probed for the native
-- asset only, because pallet-bounties and pallet-child-bounties have no asset
-- concept at all.
