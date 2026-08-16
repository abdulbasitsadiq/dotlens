-- 0010_assets_holdings: asset identity + treasury holdings (Phase 2, slice 6).
--
-- WHERE THE MONEY ACTUALLY IS. Slice 5 stopped at what was PROMISED, and
-- measured the distance: over the spend period AH #18306127 → #19276490 the
-- treasury paid 20,895 USDT + 15,300 USDC ≈ $36,195, while every DOT-
-- denominated figure dotlens held — pot snapshots AND the account's own
-- pallet-balances history — changed by exactly ZERO. Asset-denominated spends
-- settle through the Assets pallet, which touches neither. A treasury page
-- built on slices 1–5 alone would show a treasury that never spends.
--
-- THREE DELIBERATE NON-DECISIONS, each of which would have been easy and wrong:
--
-- 1. NO NEW BALANCE TABLES. `balances.balance_changes` and
--    `balances.balance_anchors` have carried an `asset` column since migration
--    0005 (default 'native', and it is in the changes PK). Asset balances are
--    the SAME facts about a different asset, so they land in the same tables
--    through the same worker with the same lineage rules. This migration only
--    adds the one column the assets pallet needs that pallet-balances does not
--    (`status`), and the asset REGISTRY that gives an asset key a meaning.
--
-- 2. NO `treasury.consolidated_position` TABLE (ARCHITECTURE.md §8 sketched
--    one). A holdings row would be a third copy of a number we already store
--    twice — and the copy would be the one without lineage. A position is
--    "latest anchor at or before H, plus the deltas after it", which is
--    exactly what the balance-history endpoint already computes; the holdings
--    endpoint computes it per (account, asset) instead of per account. What
--    was genuinely missing is not a number store but the ACCOUNT LIST, so
--    that is what this migration adds. ARCHITECTURE.md §8 updated to match.
--
-- 3. NO USD. `consolidated_position` also sketched `usd_est`. Quantities here
--    are exact and self-provenanced; a price is neither, and a price column
--    with no stated source, timestamp and method is how a treasury dashboard
--    starts lying quietly. Valuation gets its own slice, its own provenance
--    columns, and its own honest error bars.

-- ---------------------------------------------------------------- core.assets
--
-- One row per asset REPRESENTATION on one chain. "Representation", not
-- "asset": ECOSYSTEM.md §4 — one logical USDC exists as an ERC-20 on Ethereum,
-- a foreign asset on Asset Hub, TrustBacked asset 1337 on Asset Hub, an
-- ERC-20 precompile, and an XC-representation on Hydration. This table is the
-- per-chain view; the identity GRAPH that links them (ARCHITECTURE.md §8's
-- `logical_assets`) is a Phase 5 feature and is deliberately NOT created here
-- as an empty table with an unearned foreign key — the same rule that dropped
-- the unread `votes_voter_idx` in 0008. `logical_asset_id` is reserved as a
-- plain nullable text column so that slice adds a table, not a rewrite.
--
-- `asset_key` is dotlens's canonical name for a representation and is what
-- `balances.*.asset` holds:
--     native            the chain's own currency (pallet-balances)
--     assets:1984       pallet-assets TrustBacked instance, integer id (USDT)
--     pool:12           pallet-assets Pool instance, integer id (LP shares)
--     foreign:<loc>     pallet-assets Foreign instance, keyed by XCM Location,
--                       rendered by adapter_substrate::assets::canonical_location
--                       — VERSION-STRIPPED, so the same asset seen as V3
--                       (`Concrete`, flat X1) and as V4/V5 (nested X1) is ONE
--                       key, not two. That normalization is pinned by tests
--                       against real decoded bytes, because the shape
--                       differences are real: slice 5 hit both.
create table core.assets (
    chain_id            text not null references core.chains(id),
    asset_key           text not null,
    -- NOTE ARCHITECTURE §8 calls the (chain, asset) pair `asset_uid`. It is NOT
    -- a stored column here: nothing reads it yet, and `chain_id || '/' ||
    -- asset_key` is a read-time expression away. Same rule as
    -- `logical_asset_id` below, and as the indexes this migration deliberately
    -- does not create — a column that costs every write and serves no reader
    -- is the thing 0008 and 0009 each removed once.
    -- native | trust_backed | pool | foreign
    representation_kind text not null,
    -- integer id as text for the integer-keyed instances, null for foreign
    local_id            text,
    -- normalized (version-stripped) XCM Location for foreign assets, and for
    -- any representation we can name as a location. Schema-on-read.
    --
    -- For the INTEGER-keyed instances this is CONSTRUCTED, not read: asset 1984
    -- in the pallet at index 50 is, in XCM terms, exactly
    -- [PalletInstance(50), GeneralIndex(1984)] — which is the form a treasury
    -- spend names it by. Constructing it is what lets a spend of
    -- "20895000000 of {PalletInstance 50, GeneralIndex 1984}" be rendered as
    -- 20,895 USDT instead of a number with no unit (slice 5's drill did that
    -- resolution BY HAND; this column is that hand-work made into data).
    xcm_location        jsonb,
    -- canonical string form of `xcm_location` — the join handle, because a
    -- jsonb equality join would depend on key order and numeric spelling.
    -- Produced by adapter_substrate::assets::canonical_location.
    location_key        text,
    -- The asset id EXACTLY as SCALE-encoded inside the storage key, lifted
    -- from a Blake2_128Concat key suffix. This is what makes per-account
    -- foreign-asset reads possible WITHOUT a metadata-driven SCALE encoder:
    -- concat hashers keep the raw key, so we hash bytes we already have
    -- instead of re-encoding a Location we only ever saw as JSON.
    -- Null until an on-chain `sync-assets` has seen the asset; an asset first
    -- observed in an EVENT is registered with the key but no bytes, and says so.
    raw_key_bytes       bytea,
    -- from <Pallet>.Metadata. NULL means unknown — never a guessed default.
    -- Without `decimals`, 20895000000 cannot honestly be rendered as 20,895
    -- USDT, which is the whole reason this table exists.
    symbol              text,
    name                text,
    decimals            integer,
    -- from <Pallet>.Asset (AssetDetails). Supply is chain-wide, not a holding.
    supply              numeric,
    min_balance         numeric,
    is_sufficient       boolean,
    accounts            bigint,
    -- AssetDetails.status: Live | Frozen | Destroying
    status              text,
    logical_asset_id    text,               -- reserved for the Phase 5 graph
    spec_version        bigint,             -- lineage: decoded against this
    observed_height     bigint,             -- state read at END of this block
    source              text not null,      -- sync-assets | chain-properties | test
    updated_at          timestamptz not null default now(),
    primary key (chain_id, asset_key)
);

-- NO SECONDARY INDEXES ON core.assets, on purpose. Both candidates were
-- written and then removed on review: every read in this slice fetches ALL of
-- one chain's assets (a few hundred rows) through the PK prefix and matches in
-- Rust — the symbol lookup belongs to the search slice and the location join
-- is done in the handler. Adding them now would be write amplification with no
-- reader, which is exactly what 0008 dropped `votes_voter_idx` and 0009
-- dropped the beneficiary index for. Add each WITH its query.
--
-- ONE index this slice DOES earn, on the table it actually strains: the
-- holdings query asks "which assets has this account ever touched", and
-- 0005's `balance_changes_account_idx (chain_id, account_id, block_height)`
-- cannot answer that without a heap fetch per row. On a treasury account that
-- takes a fee deposit every block, this is the difference between an index
-- scan and reading a spend period's worth of history twice per request.
create index balance_changes_asset_idx
    on balances.balance_changes (chain_id, account_id, asset, block_height);

-- --------------------------------------------------- treasury spends, joined
--
-- 0009 stored `asset_kind` as the raw decoded VersionedLocatableAsset and left
-- it at that, which is why reading spend 265 required decoding an XCM location
-- by hand to discover that 20895000000 meant 20,895 USDT and not 20,895 planck
-- of DOT — a factor-of-a-million question answered off-line, in a notebook.
--
-- These columns are that answer, made into data. `asset_location` is the
-- NORMALIZED pair the mapper can derive with no metadata and no I/O:
--     {"chain": <normalized Location>, "asset": <normalized Location>}
-- because a VersionedLocatableAsset names TWO things — WHICH CHAIN holds the
-- asset (`location`; `Here` on Asset Hub's own spends, `Parachain(1000)` on
-- the relay's) and WHICH ASSET on it (`asset_id`). Both are version-stripped,
-- so the relay's V3 `Concrete`-wrapped form and Asset Hub's V4/V5 form
-- normalize to the same strings — verified in slice 5 to be genuinely
-- different shapes for the same thing.
-- `asset_key` is the resolved dotlens key when the mapper can name it without
-- metadata (currently: `native` for an empty interior); anything needing the
-- chain's pallet indices is resolved at READ time against core.assets, and
-- stays NULL here rather than being half-guessed at write time.
--
-- Both are NULL for the legacy proposal flow, which was always native DOT and
-- carried no asset at all — a null here means "this generation had no asset
-- concept", not "we failed to parse".
--
-- BUMP: filling these requires TREASURY_MAPPER_VERSION 2. Existing rows keep
-- mapper_version 1 and NULL columns until their range is re-run; the lineage
-- column is what makes that a visible, deliberate rebuild rather than a silent
-- inconsistency.
alter table treasury.spend_events add column asset_location jsonb;
alter table treasury.spend_events add column asset_key text;
alter table treasury.spends add column asset_location jsonb;
alter table treasury.spends add column asset_key text;

-- ------------------------------------------------- balances.balance_anchors +
--
-- pallet-assets accounts have NO free/reserved split: `AssetAccount { balance,
-- status, reason, extra }` (stable 4.0.0 → 52.0.0). Forcing that into the
-- native shape would invent a distinction the pallet does not make, so the
-- rule is stated once here and enforced by the writer:
--     free  = balance, reserved = 0, frozen = null, total = balance
--     status = 'liquid' | 'frozen' | 'blocked'   (AccountStatus; pre-23.0.0
--              runtimes carry `is_frozen: bool` instead, mapped to the same
--              vocabulary so a historic anchor is comparable to a modern one)
-- `status` stays NULL for native anchors, where it would be meaningless.
--
-- NOT MODELLED, deliberately: pallet-assets 49.0.0 added a `Holder` hook, so a
-- future runtime can hold asset balance outside AssetAccount.balance. We
-- record what AssetAccount says and no more; the day that hook is used on a
-- chain we index, the anchor and the event stream will disagree, and that
-- disagreement is the signal to model it.
alter table balances.balance_anchors add column status text;

-- ------------------------------------------- treasury.treasury_accounts ------
--
-- THE ACCOUNT LIST — the "per-account provenance" half of the Phase 2 exit
-- criterion. Every row must be able to answer "why do you believe this account
-- is treasury money?", so `derivation` and `source` are not decoration.
--
-- Rows are DERIVED, not curated (ECOSYSTEM.md §6's rule):
--   role 'pot'    — a pallet account whose PalletId came out of the chain's own
--                   metadata AND whose pallet maps to a treasury instance the
--                   registry knows (`treasury_instances` in the seeds). So
--                   Collectives contributes its Fellowship and Ambassador pots
--                   with zero code, and a chain that gains a treasury pallet
--                   contributes its pot the next time labels sync.
--   role 'seeded' — a registry `accounts:` entry with kind 'treasury': the
--                   location-derived accounts that CANNOT be derived, e.g. the
--                   Fellowship expenditure account 16VcQ… and the legacy
--                   relay-treasury account 14xmw… on Asset Hub.
-- LATER: role 'bounty' (the bounties slice — bounty funds leave the pot
-- through the SpendFunds hook and sit in per-bounty accounts, so they are
-- treasury money that neither this table nor 0009 can see yet) and role
-- 'sovereign' (Phase 3, when Hydration is registered and the treasury's
-- DCA/POL positions become visible).
--
-- `active` is not deleted-on-disappear: an account that stops being treasury
-- money is still treasury history. Sync flips the flag, never removes the row.
create table treasury.treasury_accounts (
    chain_id    text not null references core.chains(id),
    account_id  bytea not null,
    role        text not null,              -- pot | seeded | (bounty, sovereign: later)
    network     text not null,              -- polkadot | kusama — the query axis
    instance    text,                       -- treasury | fellowship_treasury | … (pots)
    label       text not null,
    derivation  text,                       -- 'modl:py/trsry' — null for seeded
    source      text not null,              -- derived | registry
    ss58        text,
    note        text,
    active      boolean not null default true,
    updated_at  timestamptz not null default now(),
    primary key (chain_id, account_id, role)
);

-- the holdings endpoint asks "every treasury account on this network", which
-- the PK cannot serve (it leads with chain).
create index treasury_accounts_network_idx
    on treasury.treasury_accounts (network, chain_id) where active;
