//! The XCM correlation rule (Phase 3, slice 3) — pairing the two ids that name
//! one message.
//!
//! Pure: one block's events → the id aliases inside it. Nothing here reaches a
//! chain, a database or another block, and that bound is the design rather than
//! a convenience — see below.
//!
//! WHAT THIS FILE DOES NOT DO, AND WHY THAT IS THE INTERESTING PART.
//!
//! It does not join a send to a receive. That join is `message_id = message_id`
//! across chains, computed at read time: the sender's topic and the receiver's
//! `messageQueue` id are the same 32 bytes or they are not. Storing that edge
//! would be caching an equality against its own inputs.
//!
//! What CANNOT be computed that way is the sender's own pair of ids, because no
//! event states it. `WithUniqueTopic::deliver` calls the inner router — which
//! deposits `xcmpQueue.XcmpMessageSent` / `parachainSystem.UpwardMessageSent`
//! carrying blake2_256 of the queued bytes — and then DISCARDS that hash and
//! returns the topic, which `pallet_xcm` deposits as `Sent.message_id`. Two
//! events, two ids, one message, and the connection between them exists only in
//! the fact that they were emitted in that order in that block.
//!
//! Slice 2 measured why this matters: the wire hash of a message whose journey
//! was fully on record still returned "only the SENDING half", because the
//! receiving chain had reported the TOPIC. One link fixes that. Guessing it
//! wrong stitches two unrelated messages into one journey, which is worse than
//! not stitching at all — so the rule refuses far more often than it fires.
//!
//! THE ORDERING CONSTRAINT IS MECHANISM, NOT OBSERVATION. `send_xcm` validates,
//! delivers (the router's event) and only then returns, after which the caller
//! deposits `Sent`. So the topic event's index is always GREATER than its wire
//! event's. A candidate pair in the other order is not a pair; it is two
//! messages missing a partner each, and it is refused.
//!
//! **XCM_CORRELATOR_VERSION 2 (Phase 3, slice 4)** adds one rule,
//! `remote_destination`, and it exists because slice 3's drill produced a
//! number: 180 of 181 queued sends paired across 20,681 blocks, and the single
//! refusal was not an unlucky block but a whole CLASS — every message addressed
//! beyond the immediate neighbour. See `pair_remote` for the mechanism.
//!
//! XCM_CORRELATOR_VERSION is lineage: bump on any rule change and re-run
//! `xcm-correlate` over the ranges. This bump WIDENS the rule, so the
//! delete-then-insert sink converges on a plain re-run; migration 0016 has the
//! extra delete a NARROWED rule would need.

use crate::xcm::facts_for_event;
use canonical::CanonicalEvent;
use ingest::module::BlockMapError;
use ingest::xcm_correlate::{XcmCorrelator, XcmLink};

pub const XCM_CORRELATOR_VERSION: u32 = 2;

pub struct SubstrateXcmCorrelator;

impl XcmCorrelator for SubstrateXcmCorrelator {
    fn links(&self, events: &[CanonicalEvent]) -> Result<Vec<(u32, XcmLink)>, BlockMapError> {
        links_for_block(events)
    }
    fn correlator_version(&self) -> u32 {
        XCM_CORRELATOR_VERSION
    }
}

/// One send, as a pairing candidate.
#[derive(Debug, Clone)]
struct Candidate {
    event_index: u32,
    id: String,
    transport: String,
}

/// The transports a pair can be established WITHIN — i.e. where the destination
/// and the queue pallet are talking about the same hop and can agree.
///
/// DMP is absent on purpose and it is not an oversight: the relay's
/// `ChildParachainRouter` computes a hash and drops it without an event, and
/// `parachains_dmp` deposits nothing at all, so a downward send has no wire row
/// to pair with. `unknown` is absent because a destination we could not read is
/// a destination we will not pair on. `remote` is absent because agreement is
/// the wrong test for it — see `pair_remote`, which pairs ACROSS the
/// disagreement instead.
const PAIRABLE: [&str; 2] = ["hrmp", "ump"];

/// One block's XCM events → its id aliases.
pub fn links_for_block(
    events: &[CanonicalEvent],
) -> Result<Vec<(u32, XcmLink)>, BlockMapError> {
    let mut wires: Vec<Candidate> = Vec::new();
    let mut topics: Vec<Candidate> = Vec::new();

    for ev in events {
        // Re-derived with the SAME mapper the `xcm` worker uses, so the two
        // tables can never disagree about what an event meant — and an unknown
        // variant halts here exactly as it halts there.
        let facts = facts_for_event(ev).map_err(|reason| BlockMapError {
            event_index: ev.index,
            event: ev.name.clone(),
            reason,
        })?;
        for f in facts {
            if f.side != "sent" {
                continue;
            }
            // A send that FAILED was never queued, so it has no wire hash by
            // construction. Counting it as a topic candidate would unbalance an
            // otherwise clean block and refuse a pair that is really there.
            if f.status == "send_failed" {
                continue;
            }
            let Some(id) = f.message_id.clone() else {
                continue;
            };
            let cand = Candidate {
                event_index: ev.index,
                id,
                transport: f.transport.clone(),
            };
            match f.id_kind.as_str() {
                "wire_hash" => wires.push(cand),
                "topic" => topics.push(cand),
                _ => {}
            }
        }
    }

    // The BLOCK-wide counts, before the transport split. They go into every
    // link's evidence because the per-transport counts alone are uninformative
    // — every rule that fires has n of each by construction, so a row that
    // reported only those would tell an auditor nothing they could not read off
    // `ordinal`. These say how much else was going on: `block_sends {wire: 3,
    // topic: 1}` on a link whose transport saw 1 and 1 is a much weaker-looking
    // situation than 1 and 1 overall, and the reader can see which they have.
    let block_sends = serde_json::json!({ "wire": wires.len(), "topic": topics.len() });

    let mut out = Vec::new();
    // BRIDGED SENDS FIRST, and the order is load-bearing. A `remote` topic
    // belongs to no transport group, so leaving it in place would make its
    // group's counts unequal and refuse the OTHER, perfectly clean pair in the
    // same block — one Snowbridge export would poison every ordinary send
    // beside it. Resolving it first removes both ends from the count.
    let mut consumed: Vec<u32> = Vec::new();
    pair_remote(&wires, &topics, &block_sends, &mut consumed, &mut out);
    for transport in PAIRABLE {
        let w: Vec<&Candidate> = wires
            .iter()
            .filter(|c| c.transport == transport && !consumed.contains(&c.event_index))
            .collect();
        let t: Vec<&Candidate> = topics
            .iter()
            .filter(|c| c.transport == transport && !consumed.contains(&c.event_index))
            .collect();
        pair(transport, &w, &t, &block_sends, &mut out);
    }
    // Deterministic output: the sink is keyed by the wire event index, and two
    // transports could otherwise emit in an order that depends on PAIRABLE.
    out.sort_by_key(|(index, _)| *index);
    Ok(out)
}

/// THE BRIDGED CASE (XCM_CORRELATOR_VERSION 2, Phase 3 slice 4) — the one
/// systematic refusal slice 3 measured, over 20,681 live blocks.
///
/// Asset Hub #19407624: `xcmpQueue.XcmpMessageSent` at event 6 (transport
/// `hrmp`), `polkadotXcm.Sent` at event 7 addressed to
/// `{parents: 2, X1[GlobalConsensus(Ethereum{chain_id: 1})]}`. One wire send,
/// one topic send, adjacent, in order — a textbook `unique_in_block` shape,
/// refused for one reason only: the two sides reported different transports.
///
/// **They disagree BY CONSTRUCTION and neither is wrong.** A wire event's
/// transport comes from WHICH PALLET QUEUED IT — the immediate hop, here XCMP to
/// Bridge Hub. A `Sent`'s comes from the FINAL destination, which is Ethereum
/// and is no local transport at all. So every message addressed beyond the
/// immediate neighbour refused, which makes this a blind spot rather than an
/// unlucky block: on Asset Hub it is every Snowbridge export.
///
/// The fix is to stop treating the destination as a transport CLAIM. The wire
/// event is the authority on how the first hop left; a `remote` destination
/// cannot corroborate that and — this is the point — cannot contradict it
/// either. `unknown` is deliberately NOT paired on: `remote` is the positive
/// fact "addressed to a named consensus system", while `unknown` is ignorance
/// about the destination, and pairing on ignorance is the guess this module
/// exists to refuse.
///
/// **THIS RULE PAIRS BY ADJACENCY, not by counting**, and that is a deliberate
/// difference from `pair`. A `remote` topic belongs to no transport group, so
/// there is nothing to count it against — but the mechanism still says exactly
/// where its wire event is: `send_xcm` delivers (the router's event) and its
/// caller deposits `Sent` immediately after, so the topic's own wire event is
/// the candidate IMMEDIATELY BEFORE it in the block's send sequence. Anything
/// else there — another topic, or a wire already claimed by a bridged pair —
/// means the shape is not what the mechanism produces, and the block records
/// nothing.
///
/// `pair` is left counting rather than converted to adjacency because it is the
/// rule that measured 99.4% on 20,681 live blocks; changing a proven rule needs
/// its own measurement, not a refactor.
fn pair_remote(
    wires: &[Candidate],
    topics: &[Candidate],
    block_sends: &serde_json::Value,
    consumed: &mut Vec<u32>,
    out: &mut Vec<(u32, XcmLink)>,
) {
    let remote: Vec<&Candidate> = topics.iter().filter(|c| c.transport == "remote").collect();
    if remote.is_empty() {
        return;
    }
    // Every send candidate in this block, in event order. One event yields at
    // most one fact, so an event index identifies a candidate uniquely.
    let mut sequence: Vec<&Candidate> = wires.iter().chain(topics.iter()).collect();
    sequence.sort_by_key(|c| c.event_index);

    for t in remote {
        let Some(position) = sequence.iter().position(|c| c.event_index == t.event_index) else {
            continue;
        };
        let Some(previous) = position.checked_sub(1).map(|p| sequence[p]) else {
            continue; // nothing before it: no wire event to belong to
        };
        let is_wire = wires.iter().any(|w| w.event_index == previous.event_index);
        if !is_wire || consumed.contains(&previous.event_index) || previous.id == t.id {
            continue;
        }
        consumed.push(previous.event_index);
        consumed.push(t.event_index);
        out.push((
            previous.event_index,
            XcmLink {
                wire_hash: previous.id.clone(),
                topic: t.id.clone(),
                topic_event_index: t.event_index,
                // FROM THE WIRE SIDE, always. The destination named another
                // consensus; the transport is how this chain actually queued it.
                transport: previous.transport.clone(),
                rule: "remote_destination".into(),
                confidence: "medium".into(),
                evidence: serde_json::json!({
                    // Not derivable from the row: how crowded the block was, and
                    // how many bridged sends it held.
                    "block_sends": block_sends,
                    "remote_topics": topics.iter().filter(|c| c.transport == "remote").count(),
                    "event_gap": t.event_index - previous.event_index,
                    "topic_transport": "remote",
                }),
            },
        ));
    }
}

/// The rule itself, for one transport's candidates.
fn pair(
    transport: &str,
    w: &[&Candidate],
    t: &[&Candidate],
    block_sends: &serde_json::Value,
    out: &mut Vec<(u32, XcmLink)>,
) {
    // Events arrive in index order from the canonical block, but the rule turns
    // on ordering, so it is asserted rather than assumed.
    let mut w: Vec<&Candidate> = w.to_vec();
    let mut t: Vec<&Candidate> = t.to_vec();
    w.sort_by_key(|c| c.event_index);
    t.sort_by_key(|c| c.event_index);

    if w.is_empty() || w.len() != t.len() {
        // 2 wires and 1 topic is not a puzzle to solve. Recording nothing means
        // the wire hash reaches only its own half, which is the true answer.
        return;
    }
    // Strict alternation w0 < t0 < w1 < t1 … — for n == 1 this is simply
    // "the topic followed its wire", which is the mechanism above.
    for i in 0..w.len() {
        if w[i].event_index >= t[i].event_index {
            return;
        }
        if i > 0 && t[i - 1].event_index >= w[i].event_index {
            return;
        }
        // A wire hash equal to a topic would mean the router's hash survived,
        // which `WithUniqueTopic` makes impossible. Refuse rather than record a
        // self-alias that would make an id its own alias set.
        if w[i].id == t[i].id {
            return;
        }
    }

    // BOTH rules can in principle be fooled the same way — by a block holding
    // one send that lost its `Sent` (a chain with no XcmEventEmitter) and
    // another that lost its wire event, which balances the counts by
    // coincidence. The review caught that the original comment pinned that
    // caveat to the n > 1 arm alone, where it is if anything WEAKER: at n == 1
    // the coincidence needs exactly one of each and no third send, while at
    // n > 1 it must also alternate perfectly. What actually separates the two is
    // how much of the block the claim depends on, so `high` is one pair with
    // nothing else it could be, and `medium` is a pattern across 2n events.
    let n = w.len();
    let (rule, confidence) = if n == 1 {
        ("unique_in_block", "high")
    } else {
        ("interleaved", "medium")
    };
    for i in 0..n {
        out.push((
            w[i].event_index,
            XcmLink {
                wire_hash: w[i].id.clone(),
                topic: t[i].id.clone(),
                topic_event_index: t[i].event_index,
                transport: transport.to_string(),
                rule: rule.to_string(),
                confidence: confidence.to_string(),
                evidence: serde_json::json!({
                    "transport_candidates": n,
                    "block_sends": block_sends,
                    "ordinal": i,
                    "event_gap": t[i].event_index - w[i].event_index,
                }),
            },
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(index: u32, name: &str, data: serde_json::Value) -> CanonicalEvent {
        CanonicalEvent {
            index,
            transaction_index: Some(0),
            name: name.into(),
            data,
        }
    }

    fn id(byte: u8) -> String {
        format!("0x{}", format!("{byte:02x}").repeat(32))
    }

    /// `pallet_xcm.Sent` to a sibling parachain. `message_id` is a bare
    /// `[u8;32]` and `X1` double-wraps — both shapes measured on live data.
    fn sent_hrmp(index: u32, byte: u8, para: u64) -> CanonicalEvent {
        ev(
            index,
            "polkadotxcm.Sent",
            serde_json::json!({
                "origin": {"parents": 0, "interior": {"Here": []}},
                "destination": {"parents": 1, "interior": {"X1": [[{"Parachain": [para]}]]}},
                "message": [[{"WithdrawAsset": []}]],
                "message_id": vec![byte; 32],
            }),
        )
    }

    fn sent_ump(index: u32, byte: u8) -> CanonicalEvent {
        ev(
            index,
            "polkadotxcm.Sent",
            serde_json::json!({
                "origin": {"parents": 0, "interior": {"Here": []}},
                "destination": {"parents": 1, "interior": {"Here": []}},
                "message": [[{"WithdrawAsset": []}]],
                "message_id": vec![byte; 32],
            }),
        )
    }

    fn wire_hrmp(index: u32, byte: u8) -> CanonicalEvent {
        ev(
            index,
            "xcmpqueue.XcmpMessageSent",
            serde_json::json!({ "message_hash": vec![byte; 32] }),
        )
    }

    /// `UpwardMessageSent.message_hash` is `Option<XcmHash>` — one wrapping
    /// level more than its XCMP sibling.
    fn wire_ump(index: u32, byte: u8) -> CanonicalEvent {
        ev(
            index,
            "parachainsystem.UpwardMessageSent",
            serde_json::json!({ "message_hash": {"Some": [vec![byte; 32]]} }),
        )
    }

    /// The live shape, from Asset Hub #19581756: the router's event at index 0,
    /// the pallet's at index 1, one message, two ids.
    #[test]
    fn the_two_ids_of_one_message_are_paired_in_the_block_that_emitted_both() {
        let links = links_for_block(&[
            wire_hrmp(0, 0x77),
            sent_hrmp(1, 0x16, 2034),
            ev(2, "balances.Transfer", serde_json::json!({})),
        ])
        .expect("no unknown events");
        assert_eq!(links.len(), 1);
        let (wire_index, link) = &links[0];
        assert_eq!(*wire_index, 0, "the link is keyed by the WIRE event");
        assert_eq!(link.wire_hash, id(0x77));
        assert_eq!(link.topic, id(0x16));
        assert_eq!(link.topic_event_index, 1);
        assert_eq!(link.transport, "hrmp");
        assert_eq!(link.rule, "unique_in_block");
        assert_eq!(link.confidence, "high");
        assert_eq!(link.evidence["event_gap"], 1);
        assert_eq!(link.evidence["block_sends"], serde_json::json!({"wire": 1, "topic": 1}));
    }

    /// The ordering constraint is the mechanism: `send_xcm` delivers before its
    /// caller deposits `Sent`, so a topic BEFORE its wire event is two messages
    /// each missing a partner, not one message.
    #[test]
    fn a_topic_that_precedes_its_wire_event_is_never_paired() {
        let links = links_for_block(&[sent_hrmp(0, 0x16, 2034), wire_hrmp(1, 0x77)]).unwrap();
        assert!(links.is_empty(), "order is evidence, not decoration");
    }

    #[test]
    fn two_sends_in_one_block_pair_by_interleaving() {
        let links = links_for_block(&[
            wire_hrmp(0, 0x11),
            sent_hrmp(1, 0xaa, 2034),
            wire_hrmp(2, 0x22),
            sent_hrmp(3, 0xbb, 2000),
        ])
        .unwrap();
        assert_eq!(links.len(), 2);
        assert_eq!((links[0].1.wire_hash.clone(), links[0].1.topic.clone()), (id(0x11), id(0xaa)));
        assert_eq!((links[1].1.wire_hash.clone(), links[1].1.topic.clone()), (id(0x22), id(0xbb)));
        assert_eq!(links[0].1.rule, "interleaved");
        assert_eq!(links[0].1.confidence, "medium", "n>1 is weaker evidence and says so");
        assert_eq!(links[0].1.evidence["ordinal"], 0);
        assert_eq!(links[1].1.evidence["ordinal"], 1);
        // The block-wide counts are what an auditor cannot re-derive from the
        // row: two sends of one transport, and nothing else in the block.
        assert_eq!(links[0].1.evidence["block_sends"]["wire"], 2);
        assert_eq!(links[0].1.evidence["transport_candidates"], 2);
    }

    /// Two queued messages and one `Sent` — which one lost its event? The rule
    /// does not know, so it records nothing rather than pick.
    #[test]
    fn an_unbalanced_block_pairs_nothing() {
        let links = links_for_block(&[
            wire_hrmp(0, 0x11),
            wire_hrmp(1, 0x22),
            sent_hrmp(2, 0xaa, 2034),
        ])
        .unwrap();
        assert!(links.is_empty());
    }

    /// Equal counts are NOT enough: w, w, t, t could be one send whose `Sent`
    /// went missing plus one whose wire event did.
    #[test]
    fn equal_counts_that_do_not_alternate_pair_nothing() {
        let links = links_for_block(&[
            wire_hrmp(0, 0x11),
            wire_hrmp(1, 0x22),
            sent_hrmp(2, 0xaa, 2034),
            sent_hrmp(3, 0xbb, 2000),
        ])
        .unwrap();
        assert!(links.is_empty());
    }

    /// An upward wire event and a sibling-bound topic are two different
    /// messages that happen to share a block.
    #[test]
    fn transports_never_cross() {
        let links = links_for_block(&[wire_ump(0, 0x11), sent_hrmp(1, 0xaa, 2034)]).unwrap();
        assert!(links.is_empty());
        // …and the matching pair in each transport pairs independently.
        let links = links_for_block(&[
            wire_ump(0, 0x11),
            sent_ump(1, 0xaa),
            wire_hrmp(2, 0x22),
            sent_hrmp(3, 0xbb, 2034),
        ])
        .unwrap();
        assert_eq!(links.len(), 2);
        assert_eq!(links[0].1.transport, "ump");
        assert_eq!(links[1].1.transport, "hrmp");
        // both are `unique_in_block` — the counts are per transport, not per
        // block, so one send each does not become "n = 2, interleaved"
        assert!(links.iter().all(|(_, l)| l.rule == "unique_in_block"));
    }

    /// `SendFailed` never reached a queue, so it is not a candidate — and
    /// excluding it is what keeps the real pair balanced.
    #[test]
    fn a_failed_send_is_not_a_pairing_candidate() {
        let failed = ev(
            2,
            "polkadotxcm.SendFailed",
            serde_json::json!({
                "origin": {"parents": 0, "interior": {"Here": []}},
                "destination": {"parents": 1, "interior": {"X1": [[{"Parachain": [2000]}]]}},
                "error": {"Unroutable": []},
                "message_id": vec![0xccu8; 32],
            }),
        );
        let links = links_for_block(&[wire_hrmp(0, 0x77), sent_hrmp(1, 0x16, 2034), failed])
            .unwrap();
        assert_eq!(links.len(), 1, "the delivered message still pairs");
        assert_eq!(links[0].1.topic, id(0x16));
    }

    /// The relay's own send: `parents: 0` + `Parachain` is a CHILD, i.e. DMP,
    /// and DMP has no wire event anywhere. Nothing to pair, and no panic.
    #[test]
    fn a_downward_send_has_nothing_to_pair_with() {
        let dmp = ev(
            0,
            "xcmpallet.Sent",
            serde_json::json!({
                "origin": {"parents": 0, "interior": {"Here": []}},
                "destination": {"parents": 0, "interior": {"X1": [[{"Parachain": [1005]}]]}},
                "message": [[{"WithdrawAsset": []}]],
                "message_id": vec![0x99u8; 32],
            }),
        );
        assert!(links_for_block(&[dmp]).unwrap().is_empty());
    }

    /// The correlator halts on the same events the mapper halts on, and names
    /// the offending one — a block-level mapper that only said "this block"
    /// would leave the reader to find the row.
    #[test]
    fn an_unknown_xcm_event_halts_and_names_it() {
        let err = links_for_block(&[
            wire_hrmp(0, 0x77),
            ev(1, "polkadotxcm.SomethingNewIn2027", serde_json::json!({})),
        ])
        .expect_err("a new variant must halt, never shrug");
        assert_eq!(err.event_index, 1);
        assert_eq!(err.event, "polkadotxcm.SomethingNewIn2027");
        assert!(err.reason.contains("unknown XCM event"));
    }

    /// `polkadotXcm.Sent` to another consensus system — a Snowbridge export.
    fn sent_remote(index: u32, byte: u8) -> CanonicalEvent {
        ev(
            index,
            "polkadotxcm.Sent",
            serde_json::json!({
                "origin": {"parents": 0, "interior": {"Here": []}},
                "destination": {
                    "parents": 2,
                    "interior": {"X1": [[{"GlobalConsensus": [{"Ethereum": {"chain_id": 1}}]}]]},
                },
                "message": [[{"WithdrawAsset": []}]],
                "message_id": vec![byte; 32],
            }),
        )
    }

    /// THE SLICE-4 CASE, in the exact shape of live Asset Hub #19407624: the
    /// only refusal in 181 live sends, and a whole class rather than one block.
    /// The queue pallet says `hrmp` (the hop to Bridge Hub), the destination
    /// says Ethereum, and the link takes its transport from the WIRE side —
    /// which is the one that actually knows how the message left.
    #[test]
    fn a_bridged_send_pairs_across_the_transport_disagreement() {
        let links = links_for_block(&[wire_hrmp(6, 0x73), sent_remote(7, 0x16)]).unwrap();
        assert_eq!(links.len(), 1, "before slice 4 this refused and the wire hash dead-ended");
        let (wire_index, link) = &links[0];
        assert_eq!(*wire_index, 6);
        assert_eq!(link.transport, "hrmp", "from the wire event, never the destination");
        assert_eq!(link.rule, "remote_destination");
        assert_eq!(link.confidence, "medium");
        assert_eq!(link.evidence["topic_transport"], "remote");
        assert_eq!(link.evidence["event_gap"], 1);
        assert_eq!(link.evidence["remote_topics"], 1);
    }

    /// THE REVIEW FINDING, and it is why this rule pairs by ADJACENCY rather
    /// than by counting: a bridged send does not belong to any transport group,
    /// so counting it into one made its group unequal and refused the ORDINARY
    /// send beside it as well. One Snowbridge export would have poisoned every
    /// clean pair in its block — a narrower version of the very class this slice
    /// exists to close.
    #[test]
    fn a_bridged_send_beside_an_ordinary_one_no_longer_poisons_it() {
        let links = links_for_block(&[
            wire_hrmp(0, 0x11),
            sent_hrmp(1, 0xaa, 2034),
            wire_hrmp(6, 0x73),
            sent_remote(7, 0x16),
        ])
        .unwrap();
        assert_eq!(links.len(), 2, "both sends pair: {links:?}");
        assert_eq!(links[0].1.rule, "unique_in_block", "the ordinary send is untouched");
        assert_eq!(links[0].1.topic, id(0xaa));
        assert_eq!(links[1].1.rule, "remote_destination");
        assert_eq!(links[1].1.topic, id(0x16));
        // …and the bridged pass ran FIRST, so the ordinary pair's transport
        // group counted 1:1 rather than 2:1.
        assert_eq!(links[0].1.evidence["block_sends"]["wire"], 2);
    }

    /// The ordering constraint holds across the bridge too — the mechanism does
    /// not care where the message was addressed.
    #[test]
    fn a_bridged_topic_before_its_wire_event_is_still_refused() {
        assert!(links_for_block(&[sent_remote(0, 0x16), wire_hrmp(1, 0x73)])
            .unwrap()
            .is_empty());
    }

    /// A destination we could not READ is ignorance, not a positive fact, so it
    /// never pairs on its own — but it does not BLOCK a bridged pair either,
    /// because adjacency resolves what counting could not: the wire event
    /// immediately before the bridged topic is the one that queued it, whatever
    /// else the block holds.
    #[test]
    fn an_unreadable_destination_never_pairs_but_does_not_block_adjacency() {
        let unreadable = ev(
            8,
            "polkadotxcm.Sent",
            serde_json::json!({
                "origin": {"parents": 0, "interior": {"Here": []}},
                "destination": {"parents": 2, "interior": {"Here": []}},
                "message": [[{"WithdrawAsset": []}]],
                "message_id": vec![0xaau8; 32],
            }),
        );
        // alone: nothing to pair with, and no pairing invented
        assert!(links_for_block(&[wire_hrmp(6, 0x73), unreadable.clone()]).unwrap().is_empty());
        // after a bridged send: the wire belongs to the candidate immediately
        // after it, and the unreadable one is simply a `Sent` with no wire event
        let links =
            links_for_block(&[wire_hrmp(6, 0x73), sent_remote(7, 0x16), unreadable.clone()])
                .unwrap();
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].1.topic, id(0x16));
        // BEFORE it, though, the unreadable send is what sits next to the wire —
        // so the bridged topic has no adjacent wire and nothing is recorded.
        let mut earlier = unreadable;
        earlier.index = 7;
        assert!(links_for_block(&[wire_hrmp(6, 0x73), earlier, sent_remote(8, 0x16)])
            .unwrap()
            .is_empty());
    }

    /// A bridged topic does not reach past an intervening send to claim a wire.
    #[test]
    fn a_bridged_topic_never_reaches_past_another_send_for_its_wire() {
        let links = links_for_block(&[
            wire_hrmp(0, 0x11),
            sent_hrmp(1, 0xaa, 2034),
            sent_remote(2, 0x16),
        ])
        .unwrap();
        assert_eq!(links.len(), 1, "the local pair holds; the bridged one has no wire left");
        assert_eq!(links[0].1.rule, "unique_in_block");
        assert_eq!(links[0].1.topic, id(0xaa));
    }

    #[test]
    fn a_block_with_no_xcm_events_produces_no_links() {
        let links = links_for_block(&[
            ev(0, "balances.Transfer", serde_json::json!({})),
            ev(1, "system.ExtrinsicSuccess", serde_json::json!({})),
        ])
        .unwrap();
        assert!(links.is_empty());
    }
}
