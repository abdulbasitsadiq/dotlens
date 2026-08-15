-- 0003_receipt_hashes: receipts carry a content hash (ARCHITECTURE.md §6:
-- "what was fetched, from which endpoint, when, with hashes").
-- Forward-only: Phase 0 left this table empty, so no backfill is needed;
-- the column still defaults to '' rather than breaking on any stray row.

alter table core.ingest_receipts
    add column content_hash text not null default '';
