//! The HRMP channel graph (Phase 3, slice 16) — the pure half.
//!
//! This module reads the relay's `Hrmp` pallet storage into a set of directed
//! edges. It performs no I/O: the caller enumerates keys and batches values, and
//! everything here is a pure function of (metadata blob, bytes).
//!
//! **THE CHANNEL GRAPH IS READABLE FROM KEYS ALONE**, which is the finding that
//! makes this cheap. All the HRMP maps are `Twox64Concat` — a CONCAT hasher, so
//! the encoded key sits in the key beside its hash — and `HrmpChannelId` is two
//! `ParaId(u32)` newtypes, i.e. 8 bytes of `sender ++ recipient` little-endian
//! with no length prefix and no discriminant. So a full key is
//! `twox128("Hrmp") ++ twox128("HrmpChannels") ++ twox64(id) ++ id` = **48
//! bytes**, and the edge set falls out of an enumerated key page with no values
//! read at all. Same trick as slice 6's Hydration asset registry and slice 13's
//! `Broker.Workload`.
//!
//! **NOTHING HERE HAND-PARSES SCALE.** Keys go through
//! [`fork::StorageKeyIndex::describe`] and values through
//! [`fork::StorageKeyIndex::decode_value`], both of which resolve against the
//! type the runtime's own metadata declares. That is not stylistic: the prep
//! pass hand-parsed one value, read `Balance` as compact when it is a **fixed
//! 16-byte u128**, and reported `sender_deposit = 0, recipient_deposit = 58` —
//! well-formed nonsense that no shape check would have caught, because it
//! decoded cleanly. It was found only by value-size arithmetic. A decoder driven
//! by the declared type cannot make that mistake.
//!
//! WHAT IS DELIBERATELY NOT READ, because three of `HrmpChannel`'s eight fields
//! are message throughput rather than topology:
//! `msg_count`, `total_size` and `mqc_head` are dropped after the shape check.
//! Measured over one session (2400 blocks) on live Polkadot: **16 of 224
//! channels' values changed, on `mqc_head` ALONE, with the topology unchanged.**
//! A table that kept them would report sixteen false "channel changed" events
//! per session against zero real ones. (`msg_count`/`total_size` did not move,
//! because they are send-minus-drain counters that return to zero between
//! readings while `mqc_head` is monotonic — the opposite of what the source pass
//! predicted, and the reason it was measured.)
//!
//! A SHAPE THIS VERSION DOES NOT RECOGNISE HALTS LOUDLY. The eight `HrmpChannel`
//! field names and the six `HrmpOpenChannelRequest` ones were confirmed against
//! the live runtime at spec 2003002, and a struct carrying different fields
//! means the runtime moved under us. Defaulting there would silently drop a
//! deposit or invent a limit, which is the direction this project refuses to be
//! wrong in.
//!
//! HRMP_READER_VERSION is lineage: bump on any rule change; rows rebuild from a
//! re-read, because unlike a mapper this reads STATE and a re-read at the same
//! height is the same answer or a defect.

use crate::assets::{map_prefix, StorageKeyHasher};
use crate::fork::StorageKeyIndex;

pub const HRMP_READER_VERSION: u32 = 1;

/// The relay's HRMP pallet storage prefix. Confirmed against the live runtime
/// rather than assumed from the pallet name — a storage prefix is not always the
/// pallet name, which is what made `sync-assets` correct on Collectives' two
/// pallet-treasury instances.
pub const HRMP_PALLET: &str = "Hrmp";
pub const CHANNELS_ENTRY: &str = "HrmpChannels";
pub const OPEN_REQUESTS_ENTRY: &str = "HrmpOpenChannelRequests";

/// `Session.CurrentIndex` — a Plain entry, so its key is the bare 32-byte
/// prefix. The reading dates itself in the units topology changes in.
pub const SESSION_PALLET: &str = "Session";
pub const CURRENT_INDEX_ENTRY: &str = "CurrentIndex";

/// The `state` vocabulary of `xcm.channel_snapshots`, shared so the writer and
/// the reader cannot drift. An edge is one of these and never both at one
/// reading.
pub const STATE_OPEN: &str = "open";
pub const STATE_REQUESTED: &str = "requested";

/// **A GOLDEN VECTOR SHIPPED BY UPSTREAM.** `polkadot/primitives/src/v8/mod.rs:303`
/// hard-codes this 32-byte prefix and builds the key as
/// `prefix ++ twox_64(id) ++ id`, so the derived [`channels_prefix`] has
/// something to be checked against that was not derived by us. Same device as
/// `accounts::SYSTEM_ACCOUNT_PREFIX` and `votes::VOTING_FOR_PREFIX`, and the one
/// that caught a wrong hasher in slice 7's drill — where a bad `System.Account`
/// prefix reported every derived bounty account as ABSENT and looked exactly
/// like a SCALE encoding bug.
pub const HRMP_CHANNELS_PREFIX: [u8; 32] = [
    0x6a, 0x0d, 0xa0, 0x5c, 0xa5, 0x99, 0x13, 0xbc, 0x38, 0xa8, 0x63, 0x05, 0x90, 0xf2, 0x62, 0x7c,
    0xb6, 0x60, 0x4c, 0xff, 0x82, 0x8a, 0x6e, 0x3f, 0x57, 0x9c, 0xa6, 0xc5, 0x9a, 0xce, 0x01, 0x3d,
];

/// `HrmpChannel`'s field list, in the runtime's own order. Confirmed
/// field-for-field at spec 2003002 with every one of 224 live values consuming
/// every byte and leaving zero trailing.
const CHANNEL_FIELDS: [&str; 8] = [
    "max_capacity",
    "max_total_size",
    "max_message_size",
    "msg_count",
    "total_size",
    "mqc_head",
    "sender_deposit",
    "recipient_deposit",
];

/// `HrmpOpenChannelRequest`'s field list. Note `_age` — the leading underscore
/// is in the metadata's own field name, because upstream deprecated the field
/// (requests became non-expiring) without removing it. It is checked for and
/// never read.
const OPEN_REQUEST_FIELDS: [&str; 6] = [
    "confirmed",
    "_age",
    "sender_deposit",
    "max_message_size",
    "max_capacity",
    "max_total_size",
];

/// One directed edge as of one reading. Open channels and pending requests share
/// this shape because they share four of their fields and differ in one each;
/// see 0027 for why they share a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelEdge {
    pub sender: u32,
    pub recipient: u32,
    /// [`STATE_OPEN`] or [`STATE_REQUESTED`].
    pub state: &'static str,
    pub max_capacity: u32,
    pub max_total_size: u32,
    pub max_message_size: u32,
    pub sender_deposit: u128,
    /// `Some` iff open — the recipient's deposit is supplied by
    /// `accept_open_channel`, which has not run for a pending request.
    pub recipient_deposit: Option<u128>,
    /// `Some` iff pending — the recipient has accepted but the session boundary
    /// has not yet applied it.
    pub confirmed: Option<bool>,
}

impl ChannelEdge {
    pub fn is_open(&self) -> bool {
        self.state == STATE_OPEN
    }
}

/// `twox128("Hrmp") ++ twox128("HrmpChannels")`, 32 bytes.
pub fn channels_prefix() -> Vec<u8> {
    map_prefix(HRMP_PALLET, CHANNELS_ENTRY)
}

/// `twox128("Hrmp") ++ twox128("HrmpOpenChannelRequests")`, 32 bytes.
pub fn open_requests_prefix() -> Vec<u8> {
    map_prefix(HRMP_PALLET, OPEN_REQUESTS_ENTRY)
}

/// `twox128("Session") ++ twox128("CurrentIndex")`, 32 bytes. A Plain entry's
/// key IS the bare prefix — nothing is appended. Getting that wrong returns
/// `None`, which reads exactly like "this chain has no sessions".
pub fn current_index_key() -> Vec<u8> {
    map_prefix(SESSION_PALLET, CURRENT_INDEX_ENTRY)
}

/// The 8-byte SCALE encoding of `HrmpChannelId { sender, recipient }`: two
/// `ParaId(u32)` newtypes, little-endian, no length prefix and no discriminant.
pub fn channel_id_bytes(sender: u32, recipient: u32) -> [u8; 8] {
    let mut out = [0u8; 8];
    out[..4].copy_from_slice(&sender.to_le_bytes());
    out[4..].copy_from_slice(&recipient.to_le_bytes());
    out
}

/// A full 48-byte `Hrmp.HrmpChannels` key for one directed edge.
///
/// Only used by tests and by a targeted single-channel probe; the sweep
/// enumerates the prefix instead, which is one call for the whole graph.
pub fn channel_key(sender: u32, recipient: u32) -> Vec<u8> {
    let mut key = channels_prefix();
    key.extend_from_slice(
        &StorageKeyHasher::Twox64Concat.hash(&channel_id_bytes(sender, recipient)),
    );
    key
}

/// Build the metadata-backed key/value index this module reads through.
pub fn key_index(metadata_blob: &[u8]) -> Result<StorageKeyIndex, String> {
    StorageKeyIndex::from_metadata(metadata_blob)
        .map_err(|e| format!("building the storage key index for the HRMP read: {e}"))
}

/// The declared value type id of one of this pallet's entries, plus a check that
/// the entry is the map we think it is.
fn map_value_type(index: &StorageKeyIndex, pallet: &str, item: &str) -> Result<u32, String> {
    let entry = index
        .entry(pallet, item)
        .ok_or_else(|| format!("this runtime declares no {pallet}.{item} — it is not a relay chain running the HRMP pallet, or the storage prefix moved"))?;
    if entry.hashers.len() != 1 {
        return Err(format!(
            "{pallet}.{item} declares {} hasher(s); this reader expects exactly 1 (a map keyed by HrmpChannelId)",
            entry.hashers.len()
        ));
    }
    if entry.hashers[0] != StorageKeyHasher::Twox64Concat {
        return Err(format!(
            "{pallet}.{item} is hashed with {:?}, not Twox64Concat — a non-concat hasher does not keep the key beside its hash, so the (sender, recipient) pair CANNOT be lifted back out and this reader would report edges it invented",
            entry.hashers[0]
        ));
    }
    Ok(entry.value_type)
}

/// Lift `(sender, recipient)` out of a concat-hashed key.
///
/// **THE MEASUREMENT MOST LIKELY TO BE QUIETLY WRONG LIVES HERE.** A wrong
/// offset yields a well-formed but WRONG pair — a plausible edge between two
/// real parachains that does not exist. That is slice 6's orml key-order trap
/// made worse: there a bad key matched nothing and read as an empty treasury,
/// here it reads as a real channel and joins like one. The defences are the
/// pinned prefix above, the `args_complete` check below, and the caller's
/// cross-derivation against the ingress/egress index maps, which store para ids
/// as VALUES and touch no hasher at all.
fn edge_from_key(index: &StorageKeyIndex, item: &str, key: &[u8]) -> Result<(u32, u32), String> {
    let d = index.describe(key);
    if d.pallet.as_deref() != Some(HRMP_PALLET) || d.item.as_deref() != Some(item) {
        return Err(format!(
            "key 0x{} does not belong to {HRMP_PALLET}.{item} (it describes as {}) — the enumerated prefix and the entry disagree",
            hex::encode(key),
            d.readable
        ));
    }
    if !d.args_complete {
        return Err(format!(
            "{HRMP_PALLET}.{item} key 0x{} did not yield complete arguments{} — an incomplete channel id is not an edge",
            hex::encode(key),
            d.args_note.map(|n| format!(": {n}")).unwrap_or_default()
        ));
    }
    let id = d.args.first().ok_or_else(|| {
        format!(
            "{HRMP_PALLET}.{item} key 0x{} decoded to zero arguments; a channel id is one argument",
            hex::encode(key)
        )
    })?;
    let sender = para_id_field(id, "sender").ok_or_else(|| {
        format!(
            "no readable `sender` in the channel id of key 0x{} (got {id})",
            hex::encode(key)
        )
    })?;
    let recipient = para_id_field(id, "recipient").ok_or_else(|| {
        format!(
            "no readable `recipient` in the channel id of key 0x{} (got {id})",
            hex::encode(key)
        )
    })?;
    Ok((sender, recipient))
}

/// `ParaId` is `Id(u32)`, a NEWTYPE, so the decoder renders it one array layer
/// deep: `{"sender":[1000]}` and never `{"sender":1000}`. Tenth recurrence of
/// this layer in the project; a reader treating it as a scalar gets a JSON array
/// and silently finds nothing.
fn para_id_field(id: &serde_json::Value, name: &str) -> Option<u32> {
    let v = newtype_inner(id.get(name)?);
    u32::try_from(v.as_u64()?).ok()
}

fn newtype_inner(v: &serde_json::Value) -> &serde_json::Value {
    match v {
        serde_json::Value::Array(items) if items.len() == 1 => &items[0],
        other => other,
    }
}

/// Refuse a struct whose field set is not the one this version was written
/// against. Both directions matter: a MISSING field means we would silently
/// default a deposit or a limit, and an EXTRA one means the runtime grew
/// something this reader is dropping without saying so.
fn expect_exact_fields(
    value: &serde_json::Value,
    expected: &[&str],
    what: &str,
) -> Result<(), String> {
    let obj = value.as_object().ok_or_else(|| {
        format!("{what} decoded to {value}, which is not a struct — this runtime's shape is not the one this reader was written against")
    })?;
    let mut missing: Vec<&str> = expected
        .iter()
        .copied()
        .filter(|f| !obj.contains_key(*f))
        .collect();
    let mut extra: Vec<&str> = obj
        .keys()
        .map(|k| k.as_str())
        .filter(|k| !expected.contains(k))
        .collect();
    if missing.is_empty() && extra.is_empty() {
        return Ok(());
    }
    missing.sort_unstable();
    extra.sort_unstable();
    Err(format!(
        "{what}'s fields are not the ones this reader was written against (missing: [{}], unexpected: [{}]). \
         The runtime changed shape; halting rather than defaulting, because a defaulted deposit or limit is a \
         wrong number that reads like a fact. Confirm the new shape and bump HRMP_READER_VERSION.",
        missing.join(", "),
        extra.join(", ")
    ))
}

fn u32_field(obj: &serde_json::Value, name: &str, what: &str) -> Result<u32, String> {
    let v = obj
        .get(name)
        .ok_or_else(|| format!("{what} has no `{name}`"))?;
    let n = crate::balances::json_u128(newtype_inner(v))
        .ok_or_else(|| format!("{what}.{name} is {v}, which is not a number"))?;
    u32::try_from(n).map_err(|_| format!("{what}.{name} = {n} does not fit a u32"))
}

fn u128_field(obj: &serde_json::Value, name: &str, what: &str) -> Result<u128, String> {
    let v = obj
        .get(name)
        .ok_or_else(|| format!("{what} has no `{name}`"))?;
    // `json_u128` is string-tolerant: `value_to_json` renders a u128 above
    // u64::MAX as a decimal STRING, and a deposit denominated in planck can
    // exceed it. Slice 6 measured 4.5% of Hydration amounts taking that path.
    crate::balances::json_u128(newtype_inner(v))
        .ok_or_else(|| format!("{what}.{name} is {v}, which is not a number"))
}

/// Decode one `HrmpChannels` (key, value) page into open edges.
pub fn channels_from_entries(
    index: &StorageKeyIndex,
    entries: &[(Vec<u8>, Vec<u8>)],
) -> Result<Vec<ChannelEdge>, String> {
    let value_type = map_value_type(index, HRMP_PALLET, CHANNELS_ENTRY)?;
    let mut out = Vec::with_capacity(entries.len());
    for (key, bytes) in entries {
        let (sender, recipient) = edge_from_key(index, CHANNELS_ENTRY, key)?;
        let v = index
            .decode_value(value_type, bytes)
            .map_err(|e| format!("HrmpChannel({sender} -> {recipient}) decode: {e}"))?;
        let what = format!("HrmpChannel({sender} -> {recipient})");
        expect_exact_fields(&v, &CHANNEL_FIELDS, &what)?;
        out.push(ChannelEdge {
            sender,
            recipient,
            state: STATE_OPEN,
            max_capacity: u32_field(&v, "max_capacity", &what)?,
            max_total_size: u32_field(&v, "max_total_size", &what)?,
            max_message_size: u32_field(&v, "max_message_size", &what)?,
            sender_deposit: u128_field(&v, "sender_deposit", &what)?,
            recipient_deposit: Some(u128_field(&v, "recipient_deposit", &what)?),
            confirmed: None,
        });
    }
    Ok(out)
}

/// Decode one `HrmpOpenChannelRequests` (key, value) page into pending edges.
pub fn open_requests_from_entries(
    index: &StorageKeyIndex,
    entries: &[(Vec<u8>, Vec<u8>)],
) -> Result<Vec<ChannelEdge>, String> {
    let value_type = map_value_type(index, HRMP_PALLET, OPEN_REQUESTS_ENTRY)?;
    let mut out = Vec::with_capacity(entries.len());
    for (key, bytes) in entries {
        let (sender, recipient) = edge_from_key(index, OPEN_REQUESTS_ENTRY, key)?;
        let v = index
            .decode_value(value_type, bytes)
            .map_err(|e| format!("HrmpOpenChannelRequest({sender} -> {recipient}) decode: {e}"))?;
        let what = format!("HrmpOpenChannelRequest({sender} -> {recipient})");
        expect_exact_fields(&v, &OPEN_REQUEST_FIELDS, &what)?;
        let confirmed = newtype_inner(
            v.get("confirmed")
                .ok_or_else(|| format!("{what} has no `confirmed`"))?,
        )
        .as_bool()
        .ok_or_else(|| format!("{what}.confirmed is not a boolean"))?;
        out.push(ChannelEdge {
            sender,
            recipient,
            state: STATE_REQUESTED,
            max_capacity: u32_field(&v, "max_capacity", &what)?,
            max_total_size: u32_field(&v, "max_total_size", &what)?,
            max_message_size: u32_field(&v, "max_message_size", &what)?,
            sender_deposit: u128_field(&v, "sender_deposit", &what)?,
            recipient_deposit: None,
            confirmed: Some(confirmed),
        });
    }
    Ok(out)
}

/// Combine the open channels and the pending requests into the edge set one
/// reading records, sorted by `(sender, recipient)`.
///
/// **REFUSES A DUPLICATE EDGE ACROSS THE TWO MAPS.** An edge is open or pending
/// and never both: `process_hrmp_open_channel_requests` removes the request in
/// the same statement that inserts the channel (hrmp.rs:1085-1115), and
/// `init_open_channel` refuses a request for a channel that already exists. That
/// invariant is SOURCE-DERIVED AND HAS NO LIVE COUNTEREXAMPLE — none could,
/// which is exactly why it is checked here rather than left to the primary key
/// to arbitrate one row away silently.
pub fn merge_edges(
    channels: Vec<ChannelEdge>,
    requests: Vec<ChannelEdge>,
) -> Result<Vec<ChannelEdge>, String> {
    let mut all = channels;
    all.extend(requests);
    all.sort_by_key(|e| (e.sender, e.recipient));
    for pair in all.windows(2) {
        if pair[0].sender == pair[1].sender && pair[0].recipient == pair[1].recipient {
            return Err(format!(
                "edge {} -> {} appears twice in one reading, as '{}' and '{}'. The runtime holds a channel and \
                 an open request for the same pair at one instant, which upstream's own guards say cannot \
                 happen (`init_open_channel` refuses a request for an existing channel, and \
                 `process_hrmp_open_channel_requests` removes the request in the same statement that inserts \
                 the channel). So either the node served INCONSISTENT STATE across the two prefix reads, or \
                 the key offset is wrong and one of these pairs is invented, or this reader's understanding \
                 of the pallet is. Refusing rather than dropping one silently.",
                pair[0].sender, pair[0].recipient, pair[0].state, pair[1].state
            ));
        }
    }
    Ok(all)
}

/// blake2_256 over the sorted OPEN edge set, hex with an `0x` prefix.
///
/// Covers open channels ONLY. A pending request is not topology, and folding it
/// in would move the digest for a reason that is not a channel opening or
/// closing — which is the one question the digest exists to answer cheaply.
/// Order-independent: the input is sorted before hashing, so two readings of the
/// same graph agree whatever order the pages came back in.
pub fn topology_digest(edges: &[ChannelEdge]) -> String {
    let mut open: Vec<(u32, u32)> = edges
        .iter()
        .filter(|e| e.is_open())
        .map(|e| (e.sender, e.recipient))
        .collect();
    open.sort_unstable();
    let mut buf = Vec::with_capacity(open.len() * 8);
    for (s, r) in open {
        buf.extend_from_slice(&s.to_le_bytes());
        buf.extend_from_slice(&r.to_le_bytes());
    }
    format!("0x{}", hex::encode(crate::calls::blake2_256(&buf)))
}

/// `Session.CurrentIndex` at the same hash the graph was read at.
///
/// A Plain entry, so a non-empty hasher list means the key this reader builds is
/// wrong — and a wrong key returns `None`, which is indistinguishable from a
/// chain that has no sessions.
pub fn decode_session_index(index: &StorageKeyIndex, bytes: &[u8]) -> Result<u64, String> {
    let entry = index
        .entry(SESSION_PALLET, CURRENT_INDEX_ENTRY)
        .ok_or_else(|| {
            format!("this runtime declares no {SESSION_PALLET}.{CURRENT_INDEX_ENTRY}")
        })?;
    if !entry.hashers.is_empty() {
        return Err(format!(
            "{SESSION_PALLET}.{CURRENT_INDEX_ENTRY} declares {} hasher(s): this runtime makes it a MAP, and a plain 32-byte key would read the wrong bytes",
            entry.hashers.len()
        ));
    }
    let value_type = entry.value_type;
    let v = index
        .decode_value(value_type, bytes)
        .map_err(|e| format!("Session.CurrentIndex decode: {e}"))?;
    // Confirmed on live data: `SessionIndex` is a plain type alias, NOT a
    // newtype, so the decoded value is a BARE NUMBER — unlike `ParaId`, which is
    // `Id(u32)` and renders one array layer deep. `newtype_inner` is a no-op on
    // a bare number, so applying it costs nothing and survives an alias becoming
    // a newtype upstream.
    crate::balances::json_u128(newtype_inner(&v))
        .and_then(|n| u64::try_from(n).ok())
        .ok_or_else(|| {
            format!("Session.CurrentIndex decoded to {v}, which is not a session number")
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn relay_metadata() -> Option<Vec<u8>> {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/real/polkadot-32566550/metadata.scale");
        match std::fs::read(&path) {
            Ok(b) => Some(b),
            Err(_) => {
                eprintln!(
                    "SKIP: real relay fixture metadata not present at {}",
                    path.display()
                );
                None
            }
        }
    }

    /// A `HrmpChannel` value, encoded the way the runtime encodes it:
    /// 5 x u32 LE, then `Option<H256>`, then TWO FIXED 16-BYTE u128s.
    /// 4*5 + 1 + 16 + 16 = 53 bytes with `mqc_head = None`, which is exactly the
    /// minimum value size measured across all 224 live channels.
    fn encode_channel(
        max_capacity: u32,
        max_total_size: u32,
        max_message_size: u32,
        msg_count: u32,
        total_size: u32,
        mqc_head: Option<[u8; 32]>,
        sender_deposit: u128,
        recipient_deposit: u128,
    ) -> Vec<u8> {
        let mut v = Vec::new();
        for n in [
            max_capacity,
            max_total_size,
            max_message_size,
            msg_count,
            total_size,
        ] {
            v.extend_from_slice(&n.to_le_bytes());
        }
        match mqc_head {
            None => v.push(0),
            Some(h) => {
                v.push(1);
                v.extend_from_slice(&h);
            }
        }
        v.extend_from_slice(&sender_deposit.to_le_bytes());
        v.extend_from_slice(&recipient_deposit.to_le_bytes());
        v
    }

    fn encode_open_request(
        confirmed: bool,
        age: u32,
        sender_deposit: u128,
        max_message_size: u32,
        max_capacity: u32,
        max_total_size: u32,
    ) -> Vec<u8> {
        let mut v = Vec::new();
        v.push(u8::from(confirmed));
        v.extend_from_slice(&age.to_le_bytes());
        v.extend_from_slice(&sender_deposit.to_le_bytes());
        v.extend_from_slice(&max_message_size.to_le_bytes());
        v.extend_from_slice(&max_capacity.to_le_bytes());
        v.extend_from_slice(&max_total_size.to_le_bytes());
        v
    }

    fn open_edge(sender: u32, recipient: u32) -> ChannelEdge {
        ChannelEdge {
            sender,
            recipient,
            state: STATE_OPEN,
            max_capacity: 1000,
            max_total_size: 102400,
            max_message_size: 102400,
            sender_deposit: 100_000_000_000,
            recipient_deposit: Some(100_000_000_000),
            confirmed: None,
        }
    }

    #[test]
    fn the_derived_prefix_reproduces_the_one_upstream_pins() {
        // The control that costs nothing and catches a wrong pallet prefix or a
        // broken twox128 before a single key is read. Upstream hard-codes this
        // in `polkadot/primitives/src/v8/mod.rs:303`; we derive it. If these ever
        // disagree, every edge below is invented.
        assert_eq!(
            channels_prefix()[..],
            HRMP_CHANNELS_PREFIX[..],
            "the derived Hrmp.HrmpChannels prefix must equal the one upstream pins"
        );
        assert_eq!(channels_prefix().len(), 32);
        // …and the full key is 48 bytes: 32 prefix + 8 twox64 + 8 encoded id.
        assert_eq!(channel_key(1000, 2034).len(), 48);
    }

    #[test]
    fn a_channel_id_encodes_as_eight_bare_little_endian_bytes() {
        // No length prefix, no discriminant — two ParaId(u32) newtypes back to
        // back. A compact or prefixed encoding here would build a key that
        // matches nothing, which reads exactly like a chain with no channels.
        assert_eq!(
            channel_id_bytes(1000, 2034),
            [0xe8, 0x03, 0x00, 0x00, 0xf2, 0x07, 0x00, 0x00]
        );
        // And the key really is prefix ++ twox64_concat(id), as upstream builds it.
        let key = channel_key(1000, 2034);
        assert_eq!(key[..32], HRMP_CHANNELS_PREFIX[..]);
        assert_eq!(
            key[40..],
            channel_id_bytes(1000, 2034)[..],
            "the concat hasher keeps the id in the key"
        );
    }

    #[test]
    fn every_map_this_reader_touches_is_concat_hashed_on_the_real_runtime() {
        let Some(blob) = relay_metadata() else { return };
        let index = key_index(&blob).expect("the relay metadata builds a key index");
        for item in [CHANNELS_ENTRY, OPEN_REQUESTS_ENTRY] {
            let entry = index
                .entry(HRMP_PALLET, item)
                .unwrap_or_else(|| panic!("the relay runtime declares {HRMP_PALLET}.{item}"));
            assert_eq!(entry.hashers.len(), 1, "{item} is a single-key map");
            assert_eq!(
                entry.hashers[0],
                StorageKeyHasher::Twox64Concat,
                "{item} must be concat-hashed or the channel id cannot be lifted from the key"
            );
        }
        // `Session.CurrentIndex` is the other half of a reading and is PLAIN.
        let session = index
            .entry(SESSION_PALLET, CURRENT_INDEX_ENTRY)
            .expect("the relay runtime declares Session.CurrentIndex");
        assert!(session.hashers.is_empty(), "a Plain entry has no hashers");
        assert_eq!(
            current_index_key().len(),
            32,
            "nothing is appended to a Plain key"
        );
    }

    #[test]
    fn a_real_key_round_trips_to_the_para_ids_that_built_it() {
        // The half of the key-lifting control that unit tests can do: build a key
        // from known ids and require the metadata-driven describe() to hand the
        // same ids back. The other half — cross-deriving the edge set from the
        // ingress/egress index maps, which touch no hasher — needs a chain and
        // is step 4 of the VERIFY doc.
        let Some(blob) = relay_metadata() else { return };
        let index = key_index(&blob).expect("key index");
        let key = channel_key(1000, 2034);
        let (sender, recipient) = edge_from_key(&index, CHANNELS_ENTRY, &key)
            .expect("a well-formed HrmpChannels key describes completely");
        assert_eq!((sender, recipient), (1000, 2034));

        // Directionality is not decoration: (1000 -> 2034) and (2034 -> 1000)
        // are two channels, and a reader that folds them halves the graph.
        let reverse = channel_key(2034, 1000);
        assert_ne!(key, reverse);
        assert_eq!(
            edge_from_key(&index, CHANNELS_ENTRY, &reverse).unwrap(),
            (2034, 1000)
        );
    }

    #[test]
    fn a_key_from_another_entry_is_refused_rather_than_read_as_a_channel() {
        let Some(blob) = relay_metadata() else { return };
        let index = key_index(&blob).expect("key index");
        // An open-request key has the same SHAPE as a channel key and a different
        // prefix. Reading one as the other would produce a real-looking edge in
        // the wrong state.
        let mut req = open_requests_prefix();
        req.extend_from_slice(&StorageKeyHasher::Twox64Concat.hash(&channel_id_bytes(1000, 2034)));
        let err = edge_from_key(&index, CHANNELS_ENTRY, &req).unwrap_err();
        assert!(err.contains("does not belong to"), "{err}");
    }

    #[test]
    fn the_deposits_are_fixed_width_and_a_compact_read_would_not_round_trip() {
        // THE PREP'S DECODE SLIP, pinned. A hand-parser read `Balance` as compact
        // and reported sender_deposit=0 / recipient_deposit=58 — well-formed
        // nonsense. Decoding against the declared type gives the real numbers,
        // and the value is exactly 53 bytes, which is the measured minimum across
        // all 224 live channels.
        let Some(blob) = relay_metadata() else { return };
        let index = key_index(&blob).expect("key index");
        let value = encode_channel(
            1000,
            102400,
            102400,
            0,
            0,
            None,
            100_000_000_000,
            100_000_000_000,
        );
        assert_eq!(
            value.len(),
            53,
            "5*u32 + Option::None + 2*u128 fixed = 53 bytes"
        );

        let edges = channels_from_entries(&index, &[(channel_key(1000, 2034), value)])
            .expect("a real HrmpChannel value decodes");
        assert_eq!(edges.len(), 1);
        let e = &edges[0];
        assert_eq!((e.sender, e.recipient), (1000, 2034));
        assert_eq!(e.state, STATE_OPEN);
        assert_eq!(e.max_capacity, 1000);
        assert_eq!(e.max_total_size, 102400);
        assert_eq!(e.max_message_size, 102400);
        assert_eq!(
            e.sender_deposit, 100_000_000_000,
            "not 0 — that is the compact misread"
        );
        assert_eq!(
            e.recipient_deposit,
            Some(100_000_000_000),
            "not 58 — that is the compact misread"
        );
        assert_eq!(e.confirmed, None, "an open channel carries no `confirmed`");

        // The `Some(mqc_head)` shape is the other measured size, 85 bytes, and it
        // must decode too — with mqc_head dropped rather than stored.
        let with_head = encode_channel(25, 102400, 102400, 3, 900, Some([7u8; 32]), 0, 0);
        assert_eq!(with_head.len(), 85, "the Some(H256) form is 85 bytes");
        let edges = channels_from_entries(&index, &[(channel_key(2000, 2034), with_head)])
            .expect("decodes");
        assert_eq!(edges[0].max_capacity, 25);
        assert_eq!(
            edges[0].sender_deposit, 0,
            "a system channel's deposits really are zero"
        );
    }

    #[test]
    fn a_truncated_value_halts_rather_than_yielding_a_plausible_channel() {
        let Some(blob) = relay_metadata() else { return };
        let index = key_index(&blob).expect("key index");
        let mut value = encode_channel(
            1000,
            102400,
            102400,
            0,
            0,
            None,
            100_000_000_000,
            100_000_000_000,
        );
        value.truncate(40);
        let err = channels_from_entries(&index, &[(channel_key(1000, 2034), value)]).unwrap_err();
        assert!(err.contains("decode"), "{err}");
    }

    #[test]
    fn an_open_request_decodes_and_carries_confirmed_but_no_recipient_deposit() {
        let Some(blob) = relay_metadata() else { return };
        let index = key_index(&blob).expect("key index");
        let mut req_key = open_requests_prefix();
        req_key
            .extend_from_slice(&StorageKeyHasher::Twox64Concat.hash(&channel_id_bytes(3000, 1000)));
        let value = encode_open_request(true, 0, 100_000_000_000, 102400, 1000, 102400);

        let edges = open_requests_from_entries(&index, &[(req_key, value)])
            .expect("a real HrmpOpenChannelRequest value decodes");
        assert_eq!(edges.len(), 1);
        let e = &edges[0];
        assert_eq!((e.sender, e.recipient), (3000, 1000));
        assert_eq!(e.state, STATE_REQUESTED);
        assert_eq!(
            e.confirmed,
            Some(true),
            "the recipient has accepted; the boundary has not run"
        );
        assert_eq!(
            e.recipient_deposit, None,
            "a pending request has no recipient deposit — accept_open_channel has not supplied one"
        );
        assert_eq!(e.sender_deposit, 100_000_000_000);
    }

    #[test]
    fn a_struct_this_version_does_not_recognise_halts_and_names_the_difference() {
        // Both directions: a missing field would silently default a deposit, an
        // extra one would silently drop something the runtime grew.
        let v = serde_json::json!({
            "max_capacity": 1, "max_total_size": 2, "max_message_size": 3,
            "msg_count": 0, "total_size": 0, "mqc_head": {"None": []},
            "sender_deposit": 0
            // recipient_deposit missing
        });
        let err = expect_exact_fields(&v, &CHANNEL_FIELDS, "HrmpChannel(1 -> 2)").unwrap_err();
        assert!(err.contains("recipient_deposit"), "{err}");
        assert!(
            err.contains("HRMP_READER_VERSION"),
            "the halt says what to do: {err}"
        );

        let mut grown = v.as_object().unwrap().clone();
        grown.insert("recipient_deposit".into(), serde_json::json!(0));
        grown.insert("brand_new_field".into(), serde_json::json!(1));
        let err = expect_exact_fields(
            &serde_json::Value::Object(grown),
            &CHANNEL_FIELDS,
            "HrmpChannel(1 -> 2)",
        )
        .unwrap_err();
        assert!(
            err.contains("brand_new_field"),
            "an added field halts too: {err}"
        );
    }

    #[test]
    fn one_edge_cannot_be_open_and_requested_in_the_same_reading() {
        let open = vec![open_edge(1000, 2034)];
        let requested = vec![ChannelEdge {
            state: STATE_REQUESTED,
            recipient_deposit: None,
            confirmed: Some(false),
            ..open_edge(1000, 2034)
        }];
        let err = merge_edges(open, requested).unwrap_err();
        assert!(err.contains("appears twice"), "{err}");
        assert!(
            err.contains("INCONSISTENT STATE") && err.contains("key offset is wrong"),
            "the halt must name BOTH plausible causes, not just the one the shipped caller \
             cannot produce: {err}"
        );

        // …while two genuinely different edges merge and come back sorted.
        let merged = merge_edges(
            vec![open_edge(2034, 1000), open_edge(1000, 2034)],
            vec![ChannelEdge {
                state: STATE_REQUESTED,
                recipient_deposit: None,
                confirmed: Some(false),
                ..open_edge(3000, 1000)
            }],
        )
        .expect("distinct edges merge");
        assert_eq!(
            merged
                .iter()
                .map(|e| (e.sender, e.recipient))
                .collect::<Vec<_>>(),
            vec![(1000, 2034), (2034, 1000), (3000, 1000)]
        );
    }

    #[test]
    fn the_digest_covers_open_channels_only_and_ignores_ordering() {
        let a = topology_digest(&[open_edge(1000, 2034), open_edge(2034, 1000)]);
        let b = topology_digest(&[open_edge(2034, 1000), open_edge(1000, 2034)]);
        assert_eq!(
            a, b,
            "the digest is sorted before hashing, so page order cannot move it"
        );

        // A pending request is NOT topology: adding one must not move the digest,
        // because the digest's only consumer asks "did a channel open or close".
        let with_request = topology_digest(&[
            open_edge(1000, 2034),
            open_edge(2034, 1000),
            ChannelEdge {
                state: STATE_REQUESTED,
                recipient_deposit: None,
                confirmed: Some(true),
                ..open_edge(3000, 1000)
            },
        ]);
        assert_eq!(
            a, with_request,
            "a pending request must not move the topology digest"
        );

        // …but a real open does.
        let with_open = topology_digest(&[
            open_edge(1000, 2034),
            open_edge(2034, 1000),
            open_edge(3000, 1000),
        ]);
        assert_ne!(a, with_open, "an opened channel must move the digest");

        // Directionality survives into the digest.
        assert_ne!(
            topology_digest(&[open_edge(1000, 2034)]),
            topology_digest(&[open_edge(2034, 1000)])
        );

        // An empty graph has a stable digest rather than an empty string, so
        // "read, and there was nothing" is a value like any other.
        assert!(topology_digest(&[]).starts_with("0x"));
        assert_eq!(topology_digest(&[]).len(), 66);
    }

    #[test]
    fn the_session_index_decodes_from_a_plain_entry_as_a_bare_number() {
        let Some(blob) = relay_metadata() else { return };
        let index = key_index(&blob).expect("key index");
        // SessionIndex is a plain u32 alias, so this is four little-endian bytes
        // and NOT the newtype array layer ParaId has.
        let n = decode_session_index(&index, &13601u32.to_le_bytes()).expect("decodes");
        assert_eq!(n, 13601);

        // A value this entry cannot hold is refused rather than silently zeroed.
        let err = decode_session_index(&index, &[1u8]).unwrap_err();
        assert!(err.contains("Session.CurrentIndex decode"), "{err}");
    }
}
