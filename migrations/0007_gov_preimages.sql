-- 0007_gov_preimages: decoded referendum proposals (Phase 2, slice 2).
--
-- gov.preimages: one row per (chain, proposal_hash, len) — the Bounded<Call>
-- a referendum executes, fetched raw-first (preimage.preimageFor state value
-- archived in the raw store, receipt recorded) and SCALE-decoded against
-- block-correct metadata into a CALL TREE (nested calls — batch/whitelist/
-- scheduler — embed as {"call": …, "args": …} nodes, detected by TYPE ID,
-- never by name heuristics).
--
-- decode_status is honest coverage:
--   decoded     — call tree present
--   missing     — state read returned nothing (preimages are CLEARED after
--                 enactment; historical referenda often have no bytes left —
--                 that is data, not an error)
--   undecodable — bytes archived but decode failed (note says why; raw bytes
--                 stay in the store for a future decoder version)
-- source: 'state' (fetched from preimage.preimageFor) | 'inline'
-- (Bounded::Inline bytes carried in the referendum's own proposal JSON).

create table gov.preimages (
    chain_id          text not null,
    proposal_hash     text not null,            -- 0x-hex H256, matches gov.referenda
    len               bigint not null,          -- preimage byte length
    bytes_location    text,                     -- raw store key (null when missing)
    decoded_call      jsonb,                    -- the call tree
    call_summary      text,                     -- root "pallet.call", for lists
    decode_status     text not null,            -- decoded | missing | undecodable
    source            text not null,            -- state | inline
    note              text,                     -- e.g. decode error, 'legacy: len unknown'
    spec_version      bigint,                   -- metadata used for decoding (lineage)
    decoder_version   integer not null,         -- adapter CALL_DECODER_VERSION (lineage)
    fetched_at_height bigint,                   -- state read height (null for inline)
    updated_at        timestamptz not null default now(),
    primary key (chain_id, proposal_hash, len)
);

-- no secondary index: the PK prefix (chain_id, proposal_hash) already serves
-- the API lookup and the pending-referenda join (review catch, same as 0006)
