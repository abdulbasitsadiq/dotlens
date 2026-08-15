-- 0004_label_verification: account-labeling engine columns (Phase 1, slice 4).
-- ss58 is a denormalized human-readable form (computed with the owning chain's
-- prefix at sync time); verified_* record the on-chain existence probe done by
-- `dotlens-node verify-labels <chain>` (System.Account key present at height).
-- "absent" is honest data, not an error: derived sovereigns with no funds have
-- no System.Account entry.

alter table core.account_labels
    add column ss58           text,
    add column verified_at    timestamptz,
    add column verified_block bigint,
    add column verified_note  text;

create index account_labels_chain_idx
    on core.account_labels (chain_scope, kind);
