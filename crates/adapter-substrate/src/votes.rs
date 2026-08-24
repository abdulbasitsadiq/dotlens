//! Voting semantics of the Substrate family (Invariant 4: protocol specifics
//! live in adapters only). Three pure pieces:
//!
//! 1. `SubstrateVoteMapper` — canonical voting events → vote + delegation
//!    facts. Instances are recognized by pallet name (the decoder lowercases):
//!      convictionvoting     → class "referenda"            (public OpenGov)
//!      fellowshipcollective → class "fellowship_referenda"  (Collectives,
//!                             pallet-ranked-collective: rank-weighted votes)
//!    so registering Collectives later needs ZERO code here.
//!
//!    pallet-conviction-voting's EVENT VOCABULARY GREW OVER TIME (read from
//!    the published crate sources; versions in brackets are the ones actually
//!    checked):
//!      … ≤ v20  Delegated(who, target), Undelegated(who) — and NO Voted
//!               event at all. Relay OpenGov ran like this for years, so
//!               pre-2024 votes are simply NOT in the event stream.
//!      v38–v40  + Voted/VoteRemoved { who, vote } — still no poll index
//!      v41–v42  + VoteUnlocked { who, class }
//!      v43+     poll_index added to Voted/VoteRemoved; class added to
//!               Delegated/Undelegated
//!    Both chains we index today (spec 2003002) are on the v43+ shape — the
//!    committed metadata fixtures carry `poll_index`. Older runtimes are
//!    handled HONESTLY rather than guessed at: a vote whose event carries no
//!    poll index is still recorded, with `referendum_id = None` and
//!    `attribution = "unattributed"` (same for a delegation with no class).
//!    The projection ignores those rows; a later slice can recover the subject
//!    from the originating extrinsic's call args. Nothing is silently dropped.
//!
//!    Deliberately ∅ (documented, tested):
//!      convictionvoting.VoteUnlocked — lock bookkeeping after a conviction
//!        lock expires; changes no vote and no delegation.
//!      fellowshipcollective.{MemberAdded,MemberRemoved,RankChanged,
//!        MemberExchanged} — membership, not voting.
//!    UNKNOWN events of either pallet are ERRORS: a runtime upgrade that adds
//!    a vote-moving event must halt the mapper loudly.
//!
//!    Weights follow pallet-conviction-voting `Tally::add` EXACTLY
//!    (49.0.0 src/types.rs + src/conviction.rs):
//!      votes(balance, None)    = balance / 10   (integer division)
//!      votes(balance, LockedNx) = balance * N
//!      Standard: ayes|nays += votes; support += balance only when aye
//!      Split: ayes += aye/10, nays += nay/10, support += aye
//!      SplitAbstain: ayes += aye/10, nays += nay/10, support += aye + abstain
//!
//!    HOW TO AGGREGATE THESE ROWS (reviewer catch, verified in the pallet's
//!    `try_vote`/`try_remove_vote`): the rows are per-EVENT, and the pallet
//!    does NOT emit `VoteRemoved` when a voter CHANGES an existing vote (the
//!    old vote is removed from the tally internally, silently), nor when a
//!    vote is removed after the poll ended. So a correct tally is
//!    LAST-WRITE-WINS per (voter, poll) — which is exactly what the
//!    `gov.vote_positions` projection maintains — never a sum over the fact
//!    rows. Even then the result is the on-chain tally MINUS delegated power,
//!    which no event carries at all (see piece 3).
//!
//! 2. `voting_for_key` + `twox_64/twox_128` — the ConvictionVoting.VotingFor
//!    storage key. This is the first storage map we touch whose hasher is
//!    Twox64Concat, so the adapter grows a real xxHash64: it is pinned by
//!    tests that reproduce two constants already verified on-chain
//!    (`accounts::SYSTEM_ACCOUNT_PREFIX`, `gov::PREIMAGE_FOR_PREFIX`).
//!
//! 3. `decode_voting_for` — the VotingFor VALUE (`Voting<…>`) decoded against
//!    block-correct metadata: how much an account delegated and with what
//!    conviction, and how much delegated power it carries. NEITHER number
//!    appears in any event, so these state anchors are the only honest source
//!    (same doctrine as balance anchors across the Nov-2025 migration).
//!
//! VOTES_MAPPER_VERSION / VOTING_DECODER_VERSION are lineage: bump on ANY rule
//! change; rows rebuild from canonical events / re-read state.

use crate::frame_decoder::value_to_json;
use canonical::CanonicalEvent;
use frame_metadata::{RuntimeMetadata, RuntimeMetadataPrefixed};
use ingest::votes::{DelegationRecord, VoteFact, VoteMapper, VoteRecord};
use parity_scale_codec::Decode;

pub const VOTES_MAPPER_VERSION: u32 = 1;
pub const VOTING_DECODER_VERSION: u32 = 1;

pub struct SubstrateVoteMapper;

impl VoteMapper for SubstrateVoteMapper {
    fn facts(&self, event: &CanonicalEvent) -> Result<Vec<VoteFact>, String> {
        facts_for_event(event)
    }
    fn mapper_version(&self) -> u32 {
        VOTES_MAPPER_VERSION
    }
}

/// The pure mapping. Non-voting events map to ∅; malformed voting events are
/// ERRORS (a decision must never silently disappear).
/// KNOWN-UNMAPPED voting vocabularies, stated rather than implied (the
/// project's honest-coverage rule). These map to ∅ here and belong to later
/// slices, each needing its own model — not a rename of this one:
///   democracy.*              — the pre-OpenGov relay chain (2020–2022):
///                              aye/nay with conviction, but seconds, proxies
///                              and a totally different lifecycle
///   ambassadorcollective.*   — pallet-ranked-collective Instance2 on
///   secretarycollective.*      Collectives, and Instance3; both REACHABLE now
///                              that the chain is registered, both ∅ until a
///                              slice models their tallies
pub fn facts_for_event(event: &CanonicalEvent) -> Result<Vec<VoteFact>, String> {
    let Some((pallet, variant)) = event.name.split_once('.') else {
        return Ok(vec![]);
    };
    match pallet {
        "convictionvoting" => conviction_facts(variant, event),
        "fellowshipcollective" => ranked_facts(variant, event),
        _ => Ok(vec![]),
    }
}

// ------------------------------------------------------- conviction voting

fn conviction_facts(variant: &str, event: &CanonicalEvent) -> Result<Vec<VoteFact>, String> {
    let data = &event.data;
    let ctx = |what: &str| format!("{}: {what} (data: {data})", event.name);

    match variant {
        "Voted" | "VoteRemoved" => {
            let voter = field_account(data, "who", 0).ok_or_else(|| ctx("no who"))?;
            let vote = field(data, "vote", 1).ok_or_else(|| ctx("no vote"))?;
            let w = parse_account_vote(vote).map_err(|e| ctx(&e))?;
            // absent on pre-v43 runtimes: the vote is real, its subject is not
            // derivable from the event — recorded as unattributed, never guessed
            let referendum_id = field_u64(data, "poll_index", 2);
            let attribution = if referendum_id.is_some() {
                "event"
            } else {
                "unattributed"
            };
            Ok(vec![VoteFact::Vote(VoteRecord {
                class: "referenda".to_string(),
                referendum_id,
                voter: voter.to_vec(),
                kind: if variant == "Voted" {
                    "voted".to_string()
                } else {
                    "vote_removed".to_string()
                },
                vote_type: w.vote_type.to_string(),
                aye_balance: w.aye_balance,
                nay_balance: w.nay_balance,
                abstain_balance: w.abstain_balance,
                conviction: w.conviction,
                conviction_label: w.conviction.map(|c| conviction_label(c).to_string()),
                aye_votes: w.aye_votes,
                nay_votes: w.nay_votes,
                support: w.support,
                attribution: attribution.to_string(),
                data: data.clone(),
            })])
        }
        // tuple variants: [who, target, class?] / [who, class?]
        "Delegated" | "Undelegated" => {
            let delegator = field_account(data, "who", 0).ok_or_else(|| ctx("no who"))?;
            let (target, track_id) = if variant == "Delegated" {
                let target = field_account(data, "target", 1).ok_or_else(|| ctx("no target"))?;
                (Some(target.to_vec()), field_u64(data, "class", 2))
            } else {
                (None, field_u64(data, "class", 1))
            };
            let track_id = match track_id {
                Some(t) => Some(u32::try_from(t).map_err(|_| ctx("class exceeds u32"))?),
                None => None,
            };
            Ok(vec![VoteFact::Delegation(DelegationRecord {
                class: "referenda".to_string(),
                track_id,
                delegator: delegator.to_vec(),
                target,
                kind: if variant == "Delegated" {
                    "delegated".to_string()
                } else {
                    "undelegated".to_string()
                },
                attribution: if track_id.is_some() {
                    "event".to_string()
                } else {
                    "unattributed".to_string()
                },
                data: data.clone(),
            })])
        }
        // deliberate ∅: a conviction lock expired and funds unlocked. No vote
        // changed, no delegation changed — lock bookkeeping only.
        "VoteUnlocked" => Ok(vec![]),
        _ => Err(format!(
            "unknown conviction-voting event {} — votes mapper update required",
            event.name
        )),
    }
}

/// Weight decomposition of one `AccountVote`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct VoteWeights {
    vote_type: &'static str,
    aye_balance: Option<u128>,
    nay_balance: Option<u128>,
    abstain_balance: Option<u128>,
    conviction: Option<u8>,
    aye_votes: u128,
    nay_votes: u128,
    support: u128,
}

/// `AccountVote<Balance>` JSON → weights, mirroring `Tally::add`.
///   {"Standard": {"vote": [130], "balance": N}}
///   {"Split": {"aye": A, "nay": N}}
///   {"SplitAbstain": {"aye": A, "nay": N, "abstain": B}}
fn parse_account_vote(v: &serde_json::Value) -> Result<VoteWeights, String> {
    let obj = v
        .as_object()
        .ok_or_else(|| "AccountVote is not a variant object".to_string())?;

    if let Some(standard) = obj.get("Standard") {
        let vote = field(standard, "vote", 0).ok_or("Standard: no vote")?;
        let balance = field_u128(standard, "balance", 1).ok_or("Standard: no balance")?;
        let (aye, conviction) = parse_vote_byte(vote)?;
        let votes = conviction_votes(balance, conviction);
        return Ok(VoteWeights {
            vote_type: "standard",
            aye_balance: aye.then_some(balance),
            nay_balance: (!aye).then_some(balance),
            abstain_balance: None,
            conviction: Some(conviction),
            aye_votes: if aye { votes } else { 0 },
            nay_votes: if aye { 0 } else { votes },
            // support counts CAPITAL, and only from the aye side
            support: if aye { balance } else { 0 },
        });
    }
    if let Some(split) = obj.get("Split") {
        let aye = field_u128(split, "aye", 0).ok_or("Split: no aye")?;
        let nay = field_u128(split, "nay", 1).ok_or("Split: no nay")?;
        return Ok(VoteWeights {
            vote_type: "split",
            aye_balance: Some(aye),
            nay_balance: Some(nay),
            abstain_balance: None,
            conviction: None, // split votes carry no conviction, by design
            aye_votes: conviction_votes(aye, 0),
            nay_votes: conviction_votes(nay, 0),
            support: aye,
        });
    }
    if let Some(sa) = obj.get("SplitAbstain") {
        let aye = field_u128(sa, "aye", 0).ok_or("SplitAbstain: no aye")?;
        let nay = field_u128(sa, "nay", 1).ok_or("SplitAbstain: no nay")?;
        let abstain = field_u128(sa, "abstain", 2).ok_or("SplitAbstain: no abstain")?;
        return Ok(VoteWeights {
            vote_type: "split_abstain",
            aye_balance: Some(aye),
            nay_balance: Some(nay),
            abstain_balance: Some(abstain),
            conviction: None,
            aye_votes: conviction_votes(aye, 0),
            nay_votes: conviction_votes(nay, 0),
            // abstain counts toward SUPPORT (turnout) but neither side
            support: aye.saturating_add(abstain),
        });
    }
    Err(format!(
        "unknown AccountVote variant: {}",
        obj.keys().cloned().collect::<Vec<_>>().join(",")
    ))
}

/// `Vote` is a one-byte type whose hand-written TypeInfo declares a single
/// unnamed u8 field (`aye` in bit 7, conviction in bits 0–6) — identical in
/// every pallet version checked — so our decoder renders it as `[130]`.
/// Anything else is a runtime change we must hear about: no lenient fallback
/// shapes, the mapper halts loudly instead (project doctrine).
fn parse_vote_byte(v: &serde_json::Value) -> Result<(bool, u8), String> {
    let byte = json_single_number(v).ok_or_else(|| format!("Vote is not a byte: {v}"))?;
    let byte = u8::try_from(byte).map_err(|_| format!("Vote byte out of range: {byte}"))?;
    let aye = byte & 0b1000_0000 != 0;
    let conviction = byte & 0b0111_1111;
    if conviction > 6 {
        return Err(format!(
            "invalid conviction {conviction} in vote byte {byte}"
        ));
    }
    Ok((aye, conviction))
}

/// `Conviction::votes` — the pallet's exact arithmetic (integer division for
/// the no-conviction case; saturating where the pallet saturates to max).
fn conviction_votes(balance: u128, conviction: u8) -> u128 {
    match conviction {
        0 => balance / 10,
        n => balance.saturating_mul(n as u128),
    }
}

fn conviction_label(c: u8) -> &'static str {
    match c {
        0 => "none",
        1 => "locked1x",
        2 => "locked2x",
        3 => "locked3x",
        4 => "locked4x",
        5 => "locked5x",
        6 => "locked6x",
        _ => "unknown",
    }
}

fn conviction_from_name(name: &str) -> Option<u8> {
    match name {
        "None" => Some(0),
        "Locked1x" => Some(1),
        "Locked2x" => Some(2),
        "Locked3x" => Some(3),
        "Locked4x" => Some(4),
        "Locked5x" => Some(5),
        "Locked6x" => Some(6),
        _ => None,
    }
}

// --------------------------------------------------- ranked collective vote

/// pallet-ranked-collective (the Fellowship): `Voted { who, poll, vote, tally }`
/// where `vote` is `VoteRecord::Aye(votes) | Nay(votes)` — RANK-weighted vote
/// counts, no balance and no conviction behind them.
fn ranked_facts(variant: &str, event: &CanonicalEvent) -> Result<Vec<VoteFact>, String> {
    let data = &event.data;
    let ctx = |what: &str| format!("{}: {what} (data: {data})", event.name);

    match variant {
        "Voted" => {
            let voter = field_account(data, "who", 0).ok_or_else(|| ctx("no who"))?;
            let poll = field_u64(data, "poll", 1).ok_or_else(|| ctx("no poll"))?;
            let vote = field(data, "vote", 2).ok_or_else(|| ctx("no vote"))?;
            let obj = vote
                .as_object()
                .ok_or_else(|| ctx("vote is not a variant"))?;
            let (aye, votes) = if let Some(v) = obj.get("Aye") {
                (
                    true,
                    json_single_number(v).ok_or_else(|| ctx("Aye has no weight"))?,
                )
            } else if let Some(v) = obj.get("Nay") {
                (
                    false,
                    json_single_number(v).ok_or_else(|| ctx("Nay has no weight"))?,
                )
            } else {
                return Err(ctx("VoteRecord is neither Aye nor Nay"));
            };
            Ok(vec![VoteFact::Vote(VoteRecord {
                class: "fellowship_referenda".to_string(),
                referendum_id: Some(poll),
                voter: voter.to_vec(),
                kind: "voted".to_string(),
                vote_type: "ranked".to_string(),
                // no capital behind a rank-weighted vote
                aye_balance: None,
                nay_balance: None,
                abstain_balance: None,
                conviction: None,
                conviction_label: None,
                aye_votes: if aye { votes } else { 0 },
                nay_votes: if aye { 0 } else { votes },
                // `support` is a conviction-voting concept (capital turnout);
                // ranked tallies have no analogue — recorded as zero, not faked
                support: 0,
                attribution: "event".to_string(),
                data: data.clone(),
            })])
        }
        // deliberate ∅: membership changes, not votes
        "MemberAdded" | "MemberRemoved" | "RankChanged" | "MemberExchanged" => Ok(vec![]),
        _ => Err(format!(
            "unknown ranked-collective event {} — votes mapper update required",
            event.name
        )),
    }
}

// ------------------------------------------------------- JSON field plumbing
// Event data is schema-on-read JSON written by our own decoders: named fields
// as objects, positional (tuple variants) as arrays; AccountId32 as (nested)
// byte arrays, or SS58 strings for decoder-v1 fixture rows; u128 as numbers
// when small, decimal strings when big.

fn field<'a>(
    data: &'a serde_json::Value,
    name: &str,
    index: usize,
) -> Option<&'a serde_json::Value> {
    match data {
        serde_json::Value::Object(map) => map.get(name),
        serde_json::Value::Array(items) => items.get(index),
        _ => None,
    }
}

fn field_u64(data: &serde_json::Value, name: &str, index: usize) -> Option<u64> {
    match field(data, name, index)? {
        serde_json::Value::Number(n) => n.as_u64(),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn field_u128(data: &serde_json::Value, name: &str, index: usize) -> Option<u128> {
    json_u128(field(data, name, index)?)
}

fn json_u128(v: &serde_json::Value) -> Option<u128> {
    match v {
        serde_json::Value::Number(n) => n.as_u64().map(u128::from),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// A single integer that may be wrapped in newtype composites: `5`, `[5]`,
/// `[[5]]`, `{"0": 5}`.
fn json_single_number(v: &serde_json::Value) -> Option<u128> {
    match v {
        serde_json::Value::Number(_) | serde_json::Value::String(_) => json_u128(v),
        serde_json::Value::Array(items) if items.len() == 1 => json_single_number(&items[0]),
        serde_json::Value::Object(map) if map.len() == 1 => {
            json_single_number(map.values().next()?)
        }
        _ => None,
    }
}

fn json_account_bytes(v: &serde_json::Value) -> Option<[u8; 32]> {
    if let serde_json::Value::String(s) = v {
        return crate::accounts::parse_account(s).ok();
    }
    fn walk(v: &serde_json::Value, out: &mut Vec<u8>) -> bool {
        match v {
            serde_json::Value::Number(n) => match n.as_u64() {
                Some(b) if b <= 255 => {
                    out.push(b as u8);
                    true
                }
                _ => false,
            },
            serde_json::Value::Array(items) => items.iter().all(|i| walk(i, out)),
            serde_json::Value::Object(map) => map.values().all(|i| walk(i, out)),
            _ => false,
        }
    }
    let mut out = Vec::with_capacity(32);
    if walk(v, &mut out) {
        <[u8; 32]>::try_from(out.as_slice()).ok()
    } else {
        None
    }
}

fn field_account(data: &serde_json::Value, name: &str, index: usize) -> Option<[u8; 32]> {
    field(data, name, index).and_then(json_account_bytes)
}

// ------------------------------------------------------------------- xxhash
// Substrate's Twox64Concat/Twox128 hashers are xxHash64 with seeds 0 (and 1
// for the second half of twox128), little-endian. Implemented here rather
// than pulled in as a dependency: the algorithm is fixed forever, and the
// tests pin it against two prefixes already verified against live chain state
// (`accounts::SYSTEM_ACCOUNT_PREFIX`, `gov::PREIMAGE_FOR_PREFIX`).

const P1: u64 = 0x9E37_79B1_85EB_CA87;
const P2: u64 = 0xC2B2_AE3D_27D4_EB4F;
const P3: u64 = 0x1656_67B1_9E37_79F9;
const P4: u64 = 0x85EB_CA77_C2B2_AE63;
const P5: u64 = 0x27D4_EB2F_1656_67C5;

fn xxh_round(acc: u64, input: u64) -> u64 {
    acc.wrapping_add(input.wrapping_mul(P2))
        .rotate_left(31)
        .wrapping_mul(P1)
}

fn xxh_merge(acc: u64, val: u64) -> u64 {
    (acc ^ xxh_round(0, val)).wrapping_mul(P1).wrapping_add(P4)
}

fn read_u64_le(b: &[u8], i: usize) -> u64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&b[i..i + 8]);
    u64::from_le_bytes(buf)
}

fn read_u32_le(b: &[u8], i: usize) -> u32 {
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&b[i..i + 4]);
    u32::from_le_bytes(buf)
}

/// xxHash64 (canonical algorithm, little-endian input).
pub fn xxh64(data: &[u8], seed: u64) -> u64 {
    let n = data.len();
    let mut i = 0usize;
    let mut h: u64;
    if n >= 32 {
        let mut v1 = seed.wrapping_add(P1).wrapping_add(P2);
        let mut v2 = seed.wrapping_add(P2);
        let mut v3 = seed;
        let mut v4 = seed.wrapping_sub(P1);
        while n - i >= 32 {
            v1 = xxh_round(v1, read_u64_le(data, i));
            i += 8;
            v2 = xxh_round(v2, read_u64_le(data, i));
            i += 8;
            v3 = xxh_round(v3, read_u64_le(data, i));
            i += 8;
            v4 = xxh_round(v4, read_u64_le(data, i));
            i += 8;
        }
        h = v1
            .rotate_left(1)
            .wrapping_add(v2.rotate_left(7))
            .wrapping_add(v3.rotate_left(12))
            .wrapping_add(v4.rotate_left(18));
        h = xxh_merge(h, v1);
        h = xxh_merge(h, v2);
        h = xxh_merge(h, v3);
        h = xxh_merge(h, v4);
    } else {
        h = seed.wrapping_add(P5);
    }
    h = h.wrapping_add(n as u64);
    while n - i >= 8 {
        let k1 = xxh_round(0, read_u64_le(data, i));
        i += 8;
        h ^= k1;
        h = h.rotate_left(27).wrapping_mul(P1).wrapping_add(P4);
    }
    if n - i >= 4 {
        h ^= (read_u32_le(data, i) as u64).wrapping_mul(P1);
        i += 4;
        h = h.rotate_left(23).wrapping_mul(P2).wrapping_add(P3);
    }
    while i < n {
        h ^= (data[i] as u64).wrapping_mul(P5);
        i += 1;
        h = h.rotate_left(11).wrapping_mul(P1);
    }
    h ^= h >> 33;
    h = h.wrapping_mul(P2);
    h ^= h >> 29;
    h = h.wrapping_mul(P3);
    h ^= h >> 32;
    h
}

/// Substrate `twox_64`.
pub fn twox_64(data: &[u8]) -> [u8; 8] {
    xxh64(data, 0).to_le_bytes()
}

/// Substrate `twox_128` = xxh64(seed 0) ++ xxh64(seed 1), both little-endian.
pub fn twox_128(data: &[u8]) -> [u8; 16] {
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&xxh64(data, 0).to_le_bytes());
    out[8..].copy_from_slice(&xxh64(data, 1).to_le_bytes());
    out
}

fn twox_64_concat(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + data.len());
    out.extend_from_slice(&twox_64(data));
    out.extend_from_slice(data);
    out
}

// -------------------------------------------------------- VotingFor storage

/// The pallet whose storage the anchors read. Used BOTH for the key prefix and
/// for the metadata lookup in `decode_voting_for`, so the two can never drift
/// (reviewer catch: a wrong prefix reads as an honest-looking empty position,
/// while a wrong metadata lookup fails loudly — the asymmetry hides bugs).
pub const CONVICTION_VOTING_PALLET: &str = "ConvictionVoting";

/// twox128("ConvictionVoting") ++ twox128("VotingFor"), pinned like the other
/// storage prefixes in this crate (`accounts::SYSTEM_ACCOUNT_PREFIX`,
/// `gov::PREIMAGE_FOR_PREFIX`); the test below re-derives it from `twox_128`.
pub const VOTING_FOR_PREFIX: [u8; 32] = [
    0x07, 0x4b, 0x65, 0xe2, 0x62, 0xfc, 0xd5, 0xbd, 0x9c, 0x78, 0x5c, 0xaf, 0x7f, 0x42, 0xe0, 0x0a,
    0x29, 0xf2, 0xdc, 0x2b, 0x64, 0xe3, 0x54, 0x00, 0x2f, 0xae, 0xf2, 0xa8, 0x1f, 0x1a, 0xa8, 0xbb,
];

/// Full `ConvictionVoting.VotingFor(account, class)` storage key.
///
/// The entry is a StorageDoubleMap<Twox64Concat, AccountId, Twox64Concat,
/// Class, Voting, ValueQuery> — unchanged from pallet-conviction-voting 38.0.0
/// through 49.0.0 — so the key is the prefix ++ twox64_concat(account) ++
/// twox64_concat(class).
///
/// `class` is the TRACK id. Polkadot/Kusama governance encodes it as u16
/// (pallet-referenda `TrackId`); a family member using another width would
/// need this revisited — hence the explicit type here rather than a guess.
pub fn voting_for_key(account: &[u8; 32], class: u16) -> Vec<u8> {
    let mut key = Vec::with_capacity(32 + 40 + 10);
    key.extend_from_slice(&VOTING_FOR_PREFIX);
    key.extend_from_slice(&twox_64_concat(account));
    key.extend_from_slice(&twox_64_concat(&class.to_le_bytes()));
    key
}

/// One account's voting position in one track, read from state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VotingPosition {
    /// "casting" (voting directly) | "delegating".
    pub mode: String,
    pub delegating_target: Option<Vec<u8>>,
    pub delegating_balance: Option<u128>,
    pub delegating_conviction: Option<u8>,
    pub delegating_conviction_label: Option<String>,
    /// Direct votes currently held in this track.
    pub casting_vote_count: Option<u32>,
    /// Delegated power RECEIVED: post-conviction votes and raw capital.
    pub delegations_votes: Option<u128>,
    pub delegations_capital: Option<u128>,
    /// PriorLock: funds still locked until this block, and how much.
    pub prior_until: Option<u64>,
    pub prior_balance: Option<u128>,
    /// The full decoded value, schema-on-read.
    pub raw: serde_json::Value,
}

impl VotingPosition {
    /// The storage DEFAULT (`ValueQuery`): an account with no entry is casting
    /// zero votes. Recorded honestly with a note by the caller, never skipped.
    /// `raw` is byte-shape-identical to what `decode_voting_for` produces for
    /// `Voting::default()` — one JSONB column, one schema (reviewer catch).
    pub fn empty() -> Self {
        Self {
            mode: "casting".to_string(),
            delegating_target: None,
            delegating_balance: None,
            delegating_conviction: None,
            delegating_conviction_label: None,
            casting_vote_count: Some(0),
            delegations_votes: Some(0),
            delegations_capital: Some(0),
            prior_until: Some(0),
            prior_balance: Some(0),
            raw: serde_json::json!({"Casting": [{
                "votes": [[]],
                "delegations": {"votes": 0, "capital": 0},
                "prior": [0, 0]
            }]}),
        }
    }
}

/// Decode a raw `ConvictionVoting.VotingFor` storage VALUE against the
/// metadata blob archived for that block's spec_version. Pure.
pub fn decode_voting_for(
    metadata_blob: &[u8],
    value_bytes: &[u8],
) -> Result<VotingPosition, String> {
    let prefixed = RuntimeMetadataPrefixed::decode(&mut &metadata_blob[..])
        .map_err(|e| format!("metadata blob undecodable: {e}"))?;

    // $ver is an ident (v14/v15/v16), not a path fragment — `$path::More` in a
    // pattern is the classic macro parse trap (review catch from the balances
    // slice, repeated here deliberately).
    macro_rules! voting_value_type {
        ($m:expr, $ver:ident) => {{
            use frame_metadata::$ver::StorageEntryType;
            let pallet = $m
                .pallets
                .iter()
                .find(|p| p.name == CONVICTION_VOTING_PALLET)
                .ok_or("no ConvictionVoting pallet in metadata")?;
            let storage = pallet
                .storage
                .as_ref()
                .ok_or("ConvictionVoting has no storage")?;
            let entry = storage
                .entries
                .iter()
                .find(|e| e.name == "VotingFor")
                .ok_or("ConvictionVoting.VotingFor entry not found")?;
            match &entry.ty {
                // double maps are Map entries with two hashers and a tuple key
                StorageEntryType::Map { value, .. } => (value.id, $m.types.clone()),
                _ => return Err("ConvictionVoting.VotingFor is not a Map".into()),
            }
        }};
    }

    let (value_type_id, types) = match &prefixed.1 {
        RuntimeMetadata::V14(m) => voting_value_type!(m, v14),
        RuntimeMetadata::V15(m) => voting_value_type!(m, v15),
        RuntimeMetadata::V16(m) => voting_value_type!(m, v16),
        _ => return Err("unsupported metadata version (v14/v15/v16 only)".into()),
    };

    let mut cursor = value_bytes;
    let value = scale_value::scale::decode_as_type(&mut cursor, value_type_id, &types)
        .map_err(|e| format!("Voting decode: {e}"))?;
    let raw = value_to_json(&value.remove_context());
    voting_position_from_json(raw)
}

/// Unwrap exactly ONE newtype layer (`[x]` → `x`). `Voting::Casting(Casting)`
/// and `Voting::Delegating(Delegating)` are UNNAMED (newtype) variants, and
/// `BoundedVec<T, S>` is a newtype over `Vec<T>` — both render as a
/// one-element JSON array through `value_to_json` (reviewer catch: assuming
/// named objects silently NULLed every delegated-power number). Deliberately
/// NOT recursive: a one-element vote list `[[x]]` must unwrap to `[x]`, not
/// to `x`.
fn newtype_inner(v: &serde_json::Value) -> &serde_json::Value {
    match v {
        serde_json::Value::Array(items) if items.len() == 1 => &items[0],
        other => other,
    }
}

/// Walk the decoded `Voting` enum. Real decoder output (newtype variants +
/// BoundedVec, see `newtype_inner`):
///   {"Casting": [{votes: [[[poll, AccountVote], …]], delegations: {votes, capital}, prior: [until, balance]}]}
///   {"Delegating": [{balance, target, conviction, delegations, prior}]}
fn voting_position_from_json(raw: serde_json::Value) -> Result<VotingPosition, String> {
    let obj = raw
        .as_object()
        .ok_or_else(|| format!("Voting is not a variant object: {raw}"))?;

    let read_delegations = |v: Option<&serde_json::Value>| match v {
        Some(d) => (field_u128(d, "votes", 0), field_u128(d, "capital", 1)),
        None => (None, None),
    };
    let read_prior = |v: Option<&serde_json::Value>| match v {
        Some(p) => (
            field(p, "0", 0)
                .and_then(json_u128)
                .and_then(|n| u64::try_from(n).ok()),
            field(p, "1", 1).and_then(json_u128),
        ),
        None => (None, None),
    };

    if let Some(casting) = obj.get("Casting").map(newtype_inner) {
        // votes: BoundedVec<(PollIndex, AccountVote)> — one newtype layer over
        // the Vec, so unwrap before counting (else every account "has 1 vote")
        let votes = field(casting, "votes", 0)
            .map(newtype_inner)
            .and_then(|v| v.as_array())
            .map(|a| a.len() as u32);
        let (dv, dc) = read_delegations(field(casting, "delegations", 1));
        let (until, balance) = read_prior(field(casting, "prior", 2));
        return Ok(VotingPosition {
            mode: "casting".to_string(),
            delegating_target: None,
            delegating_balance: None,
            delegating_conviction: None,
            delegating_conviction_label: None,
            casting_vote_count: votes,
            delegations_votes: dv,
            delegations_capital: dc,
            prior_until: until,
            prior_balance: balance,
            raw,
        });
    }
    if let Some(delegating) = obj.get("Delegating").map(newtype_inner) {
        let balance = field_u128(delegating, "balance", 0);
        let target = field(delegating, "target", 1)
            .and_then(json_account_bytes)
            .map(|t| t.to_vec());
        let conviction = field(delegating, "conviction", 2).and_then(|c| match c {
            serde_json::Value::Object(m) => m.keys().next().and_then(|n| conviction_from_name(n)),
            serde_json::Value::Number(n) => n.as_u64().map(|n| n as u8),
            _ => None,
        });
        let (dv, dc) = read_delegations(field(delegating, "delegations", 3));
        let (until, prior_balance) = read_prior(field(delegating, "prior", 4));
        return Ok(VotingPosition {
            mode: "delegating".to_string(),
            delegating_target: target,
            delegating_balance: balance,
            delegating_conviction: conviction,
            delegating_conviction_label: conviction.map(|c| conviction_label(c).to_string()),
            casting_vote_count: None,
            delegations_votes: dv,
            delegations_capital: dc,
            prior_until: until,
            prior_balance,
            raw,
        });
    }
    Err(format!(
        "unknown Voting variant: {}",
        obj.keys().cloned().collect::<Vec<_>>().join(",")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounts::{pallet_account, para_sovereign};

    fn ev(name: &str, data: serde_json::Value) -> CanonicalEvent {
        CanonicalEvent {
            index: 0,
            transaction_index: None,
            name: name.into(),
            data,
        }
    }

    fn acct_json(a: &[u8; 32]) -> serde_json::Value {
        // the shape our decoder writes: newtype over the byte array
        serde_json::json!([a.to_vec()])
    }

    fn vote_byte(aye: bool, conviction: u8) -> serde_json::Value {
        // Vote's TypeInfo: one unnamed u8 field → a one-element array
        serde_json::json!([if aye { 0x80 | conviction } else { conviction }])
    }

    fn one_vote(f: Vec<VoteFact>) -> VoteRecord {
        match f.into_iter().next().expect("one fact") {
            VoteFact::Vote(v) => v,
            other => panic!("expected a vote, got {other:?}"),
        }
    }

    fn one_delegation(f: Vec<VoteFact>) -> DelegationRecord {
        match f.into_iter().next().expect("one fact") {
            VoteFact::Delegation(d) => d,
            other => panic!("expected a delegation, got {other:?}"),
        }
    }

    #[test]
    fn standard_aye_vote_weights_match_the_pallet() {
        let who = para_sovereign(2034);
        let e = ev(
            "convictionvoting.Voted",
            serde_json::json!({
                "who": acct_json(&who),
                "vote": {"Standard": {"vote": vote_byte(true, 3), "balance": "1000000000000"}},
                "poll_index": 1930
            }),
        );
        let v = one_vote(facts_for_event(&e).unwrap());
        assert_eq!(v.class, "referenda");
        assert_eq!(v.referendum_id, Some(1930));
        assert_eq!(v.kind, "voted");
        assert_eq!(v.vote_type, "standard");
        assert_eq!(v.voter, who.to_vec());
        assert_eq!(v.conviction, Some(3));
        assert_eq!(v.conviction_label.as_deref(), Some("locked3x"));
        assert_eq!(v.aye_balance, Some(1_000_000_000_000));
        assert_eq!(v.nay_balance, None);
        // 3x conviction: votes = balance * 3; support = capital
        assert_eq!(v.aye_votes, 3_000_000_000_000);
        assert_eq!(v.nay_votes, 0);
        assert_eq!(v.support, 1_000_000_000_000);
        assert_eq!(v.attribution, "event");
    }

    #[test]
    fn no_conviction_nay_vote_is_one_tenth_and_adds_no_support() {
        let who = para_sovereign(1000);
        let e = ev(
            "convictionvoting.Voted",
            serde_json::json!({
                "who": acct_json(&who),
                "vote": {"Standard": {"vote": vote_byte(false, 0), "balance": 1005}},
                "poll_index": 7
            }),
        );
        let v = one_vote(facts_for_event(&e).unwrap());
        assert_eq!(v.conviction, Some(0));
        assert_eq!(v.nay_balance, Some(1005));
        assert_eq!(v.aye_balance, None);
        assert_eq!(
            v.nay_votes, 100,
            "integer division, exactly like the pallet"
        );
        assert_eq!(v.aye_votes, 0);
        assert_eq!(v.support, 0, "nays never add support");
    }

    #[test]
    fn split_and_split_abstain_follow_tally_add() {
        let who = pallet_account(b"py/trsry");
        let split = ev(
            "convictionvoting.Voted",
            serde_json::json!({
                "who": acct_json(&who),
                "vote": {"Split": {"aye": 500, "nay": 250}},
                "poll_index": 11
            }),
        );
        let v = one_vote(facts_for_event(&split).unwrap());
        assert_eq!(v.vote_type, "split");
        assert_eq!((v.aye_votes, v.nay_votes, v.support), (50, 25, 500));
        assert_eq!(v.conviction, None);

        let sa = ev(
            "convictionvoting.Voted",
            serde_json::json!({
                "who": acct_json(&who),
                "vote": {"SplitAbstain": {"aye": 500, "nay": 250, "abstain": 1000}},
                "poll_index": 11
            }),
        );
        let v = one_vote(facts_for_event(&sa).unwrap());
        assert_eq!(v.vote_type, "split_abstain");
        assert_eq!(v.abstain_balance, Some(1000));
        // abstain counts toward support (turnout) but neither side
        assert_eq!((v.aye_votes, v.nay_votes, v.support), (50, 25, 1500));
    }

    #[test]
    fn vote_removed_keeps_the_weights_and_its_own_kind() {
        let who = para_sovereign(1000);
        let e = ev(
            "convictionvoting.VoteRemoved",
            serde_json::json!({
                "who": acct_json(&who),
                "vote": {"Standard": {"vote": vote_byte(true, 1), "balance": 42}},
                "poll_index": 3
            }),
        );
        let v = one_vote(facts_for_event(&e).unwrap());
        assert_eq!(v.kind, "vote_removed");
        assert_eq!(v.aye_votes, 42);
    }

    #[test]
    fn pre_v43_vote_without_poll_index_is_recorded_as_unattributed() {
        // the v38–v42 shape: Voted { who, vote } — real vote, unknown subject
        let who = para_sovereign(1000);
        let e = ev(
            "convictionvoting.Voted",
            serde_json::json!({
                "who": acct_json(&who),
                "vote": {"Standard": {"vote": vote_byte(true, 6), "balance": 10}}
            }),
        );
        let v = one_vote(facts_for_event(&e).unwrap());
        assert_eq!(v.referendum_id, None);
        assert_eq!(v.attribution, "unattributed");
        assert_eq!(v.aye_votes, 60, "weights are still exact");
    }

    #[test]
    fn delegation_shapes_old_and_new() {
        let who = para_sovereign(1000);
        let target = para_sovereign(2034);
        // v43+: tuple variant [who, target, class]
        let new = ev(
            "convictionvoting.Delegated",
            serde_json::json!([acct_json(&who), acct_json(&target), 34]),
        );
        let d = one_delegation(facts_for_event(&new).unwrap());
        assert_eq!(d.kind, "delegated");
        assert_eq!(d.track_id, Some(34));
        assert_eq!(d.target.as_deref(), Some(&target[..]));
        assert_eq!(d.attribution, "event");

        // ≤v42: [who, target] — the track is NOT derivable from the event
        let old = ev(
            "convictionvoting.Delegated",
            serde_json::json!([acct_json(&who), acct_json(&target)]),
        );
        let d = one_delegation(facts_for_event(&old).unwrap());
        assert_eq!(d.track_id, None);
        assert_eq!(d.attribution, "unattributed");

        let undel = ev(
            "convictionvoting.Undelegated",
            serde_json::json!([acct_json(&who), 34]),
        );
        let d = one_delegation(facts_for_event(&undel).unwrap());
        assert_eq!(d.kind, "undelegated");
        assert_eq!(d.target, None);
        assert_eq!(d.track_id, Some(34));
    }

    #[test]
    fn vote_unlocked_is_lock_bookkeeping_and_maps_to_nothing() {
        let who = para_sovereign(1000);
        let e = ev(
            "convictionvoting.VoteUnlocked",
            serde_json::json!({"who": acct_json(&who), "class": 34}),
        );
        assert!(facts_for_event(&e).unwrap().is_empty());
    }

    #[test]
    fn ranked_collective_votes_map_to_the_fellowship_class() {
        let who = para_sovereign(1001);
        let e = ev(
            "fellowshipcollective.Voted",
            serde_json::json!({
                "who": acct_json(&who),
                "poll": 300,
                "vote": {"Aye": [9]},
                "tally": {"bare_ayes": 3, "ayes": 9, "nays": 0}
            }),
        );
        let v = one_vote(facts_for_event(&e).unwrap());
        assert_eq!(v.class, "fellowship_referenda");
        assert_eq!(v.vote_type, "ranked");
        assert_eq!(
            (v.referendum_id, v.aye_votes, v.nay_votes),
            (Some(300), 9, 0)
        );
        assert_eq!(v.aye_balance, None, "rank-weighted votes have no capital");

        // membership events are not votes
        for name in [
            "MemberAdded",
            "MemberRemoved",
            "RankChanged",
            "MemberExchanged",
        ] {
            let m = ev(
                &format!("fellowshipcollective.{name}"),
                serde_json::json!({"who": acct_json(&who), "rank": 3}),
            );
            assert!(
                facts_for_event(&m).unwrap().is_empty(),
                "{name} must map to ∅"
            );
        }
    }

    #[test]
    fn other_pallets_are_not_voting_and_unknown_voting_events_are_loud() {
        for name in [
            "balances.Transfer",
            "referenda.Approved",
            "system.ExtrinsicSuccess",
            // known-unmapped voting vocabularies (documented on
            // facts_for_event): a different model, a later slice — ∅ here,
            // never a wrong row
            "democracy.Voted",
            "ambassadorcollective.Voted",
        ] {
            let e = ev(name, serde_json::json!({"index": 1}));
            assert!(
                facts_for_event(&e).unwrap().is_empty(),
                "{name} must map to ∅"
            );
        }
        let unknown = ev(
            "convictionvoting.SomeFutureEvent",
            serde_json::json!({"who": 1}),
        );
        assert!(facts_for_event(&unknown).is_err());
        let unknown_ranked = ev(
            "fellowshipcollective.SomeFutureEvent",
            serde_json::json!({}),
        );
        assert!(facts_for_event(&unknown_ranked).is_err());
    }

    #[test]
    fn malformed_voting_events_are_loud_errors() {
        let who = para_sovereign(1000);
        // no vote field
        let e = ev(
            "convictionvoting.Voted",
            serde_json::json!({"who": acct_json(&who)}),
        );
        assert!(facts_for_event(&e).is_err());
        // unknown AccountVote variant
        let e = ev(
            "convictionvoting.Voted",
            serde_json::json!({"who": acct_json(&who), "vote": {"Quantum": {}}, "poll_index": 1}),
        );
        assert!(facts_for_event(&e).is_err());
        // conviction byte out of range (7 is not a Conviction)
        let e = ev(
            "convictionvoting.Voted",
            serde_json::json!({
                "who": acct_json(&who),
                "vote": {"Standard": {"vote": [0x87], "balance": 1}},
                "poll_index": 1
            }),
        );
        assert!(facts_for_event(&e).is_err());
        // delegation with no target
        let e = ev(
            "convictionvoting.Delegated",
            serde_json::json!([acct_json(&who)]),
        );
        assert!(facts_for_event(&e).is_err());
    }

    // ------------------------------------------------------------- hashing

    #[test]
    fn xxhash_reproduces_prefixes_already_verified_on_chain() {
        // these two constants were derived independently and confirmed against
        // live chain state in earlier slices — reproducing them byte-for-byte
        // pins this xxHash64 implementation forever
        let mut system_account = [0u8; 32];
        system_account[..16].copy_from_slice(&twox_128(b"System"));
        system_account[16..].copy_from_slice(&twox_128(b"Account"));
        assert_eq!(system_account, crate::accounts::SYSTEM_ACCOUNT_PREFIX);

        let mut preimage_for = [0u8; 32];
        preimage_for[..16].copy_from_slice(&twox_128(b"Preimage"));
        preimage_for[16..].copy_from_slice(&twox_128(b"PreimageFor"));
        assert_eq!(preimage_for, crate::gov::PREIMAGE_FOR_PREFIX);

        // and the prefix this module pins, re-derived from the pallet names
        let mut voting_for = [0u8; 32];
        voting_for[..16].copy_from_slice(&twox_128(CONVICTION_VOTING_PALLET.as_bytes()));
        voting_for[16..].copy_from_slice(&twox_128(b"VotingFor"));
        assert_eq!(voting_for, VOTING_FOR_PREFIX);

        // canonical xxHash64 vectors (seed 0)
        assert_eq!(xxh64(b"", 0), 0xEF46_DB37_51D8_E999);
        assert_eq!(hex::encode(twox_64(b"")), "99e9d85137db46ef");
        assert_eq!(hex::encode(twox_64(b"a")), "5b6e8ca9f1c44ed2");
        assert_eq!(hex::encode(twox_64(b"abc")), "990977adf52cbc44");
        // >32 bytes exercises the four-accumulator path
        assert_eq!(hex::encode(twox_64(&[0u8; 32])), "f52c63705dbee9f6");
    }

    #[test]
    fn voting_for_key_is_prefix_plus_two_twox64_concat_keys() {
        let account = [0xabu8; 32];
        let key = voting_for_key(&account, 34);
        // golden vector, derived independently (reference xxhash) at authoring
        let expected = "074b65e262fcd5bd9c785caf7f42e00a29f2dc2b64e354002faef2a81f1aa8bb\
                        9f84d3d0450f50ef\
                        abababababababababababababababababababababababababababababababab\
                        7c466c66d061f169\
                        2200";
        assert_eq!(hex::encode(&key), expected.replace(['\n', ' '], ""));
        assert_eq!(key.len(), 32 + 40 + 10);
    }

    // -------------------------------------------------------- voting decode

    #[test]
    fn voting_position_from_casting_and_delegating_json() {
        // the REAL decoder shape: Voting's variants are newtypes, and
        // `votes` is a BoundedVec (another newtype over Vec) — one array
        // layer each (the bug this test now guards)
        let casting = serde_json::json!({"Casting": [{
            "votes": [[
                [1930, {"Standard": {"vote": [130], "balance": 5}}],
                [1931, {"Split": {"aye": 1, "nay": 2}}]
            ]],
            "delegations": {"votes": "36893488147419103232", "capital": 7},
            "prior": [19500000, 0]
        }]});
        let p = voting_position_from_json(casting).unwrap();
        assert_eq!(p.mode, "casting");
        assert_eq!(
            p.casting_vote_count,
            Some(2),
            "count the VOTES, not the BoundedVec"
        );
        assert_eq!(p.delegations_votes, Some(36_893_488_147_419_103_232));
        assert_eq!(p.delegations_capital, Some(7));
        assert_eq!(p.prior_until, Some(19_500_000));

        let target = para_sovereign(2034);
        let delegating = serde_json::json!({"Delegating": [{
            "balance": 1000,
            "target": acct_json(&target),
            "conviction": {"Locked6x": []},
            "delegations": {"votes": 0, "capital": 0},
            "prior": [0, 0]
        }]});
        let p = voting_position_from_json(delegating).unwrap();
        assert_eq!(p.mode, "delegating");
        assert_eq!(p.delegating_balance, Some(1000));
        assert_eq!(p.delegating_target.as_deref(), Some(&target[..]));
        assert_eq!(p.delegating_conviction, Some(6));
        assert_eq!(p.delegating_conviction_label.as_deref(), Some("locked6x"));
        assert_eq!(p.casting_vote_count, None);

        assert!(voting_position_from_json(serde_json::json!({"Sleeping": {}})).is_err());
    }

    #[test]
    fn empty_position_matches_the_decoded_storage_default() {
        // an absent entry and a decoded Voting::default() must be one shape
        let e = VotingPosition::empty();
        let decoded = voting_position_from_json(e.raw.clone()).expect("default shape decodes");
        assert_eq!(decoded, e, "empty() must equal what decode would return");
        assert_eq!(e.casting_vote_count, Some(0));
        assert_eq!(e.prior_until, Some(0));
    }

    #[test]
    fn voting_for_decodes_against_real_metadata() {
        // real committed AH metadata (spec 2003002) + hand-built SCALE bytes:
        //   Voting::Delegating {
        //     balance: u128, target: AccountId32, conviction: Conviction(u8),
        //     delegations: { votes: u128, capital: u128 },
        //     prior: PriorLock(BlockNumber u32, Balance u128)
        //   }
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/real/polkadot-asset-hub-19498783/metadata.scale");
        let Ok(blob) = std::fs::read(&path) else {
            eprintln!(
                "SKIP: real fixture metadata not present at {}",
                path.display()
            );
            return;
        };
        let target = [0x11u8; 32];
        let mut bytes = vec![1u8]; // enum index 1 = Delegating
        bytes.extend_from_slice(&500_000_000_000u128.to_le_bytes());
        bytes.extend_from_slice(&target);
        bytes.push(4); // Conviction::Locked4x
        bytes.extend_from_slice(&2_000_000_000_000u128.to_le_bytes()); // delegations.votes
        bytes.extend_from_slice(&500_000_000_000u128.to_le_bytes()); // delegations.capital
        bytes.extend_from_slice(&0u32.to_le_bytes()); // prior.0 (block number)
        bytes.extend_from_slice(&0u128.to_le_bytes()); // prior.1 (balance)

        let p = decode_voting_for(&blob, &bytes).expect("decodes");
        assert_eq!(p.mode, "delegating");
        assert_eq!(p.delegating_balance, Some(500_000_000_000));
        assert_eq!(p.delegating_target.as_deref(), Some(&target[..]));
        assert_eq!(p.delegating_conviction, Some(4));
        assert_eq!(p.delegations_votes, Some(2_000_000_000_000));
    }
}
