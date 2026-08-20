//! Deriving HRMP channel history from readings (Phase 3, slice 16).
//!
//! A PURE function with no database in it, for the same reason `coretime_delta`
//! is: every claim this file makes can then be tested against a fixture rather
//! than against a live index, and a reader that could only be checked against a
//! populated Postgres would have its central claim tested by nothing.
//!
//! WHAT IS DERIVED AND WHY IT IS NOT STORED. An "open" or a "close" is the
//! DIFFERENCE between two readings that both carry lineage, so a stored
//! `channel_opened` row would be the one copy WITHOUT lineage — the argument
//! that killed `treasury.consolidated_position`, `graph.cross_chain_operations`,
//! the stored forwarded-attribution, a `logical_assets` join table and the
//! coretime delta. This is its sixth outing and it has not weakened.
//!
//! THE HALF THAT MATTERS MORE THAN THE TRANSITIONS: a transition is only as
//! precise as the readings either side of it. Channel existence changes ONLY at
//! session boundaries, so two readings in ADJACENT sessions pin a change to
//! exactly one boundary — that is [`Transition::exact`]. Two readings forty
//! sessions apart pin it to a forty-session window, and worse, **a channel could
//! have opened AND closed entirely inside that window and left no trace here at
//! all.** That is not a caveat to bury in prose: it is
//! [`ChannelHistory::unread`], a first-class list, because "we did not look" and
//! "nothing happened" must never render the same way.

use serde::Serialize;

/// One reading, as a per-edge query sees it. The READING always exists — that is
/// what makes the absence meaningful — and `state` is `None` when that reading
/// covered this height and the edge was simply not in the graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeObservation {
    pub block_height: u64,
    pub session_index: u64,
    /// `Some("open")` | `Some("requested")` | `None` = absent from the graph.
    pub state: Option<String>,
}

/// A change in one edge's state between two consecutive readings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Transition {
    /// `null` means the edge did not exist in the earlier reading.
    pub from: Option<String>,
    /// `null` means it no longer exists in the later one.
    pub to: Option<String>,
    /// The last reading that still showed `from`.
    pub after_height: u64,
    pub after_session: u64,
    /// The first reading that showed `to`.
    pub at_height: u64,
    pub at_session: u64,
    /// TRUE iff the two readings sit in adjacent sessions, which pins the change
    /// to exactly one session boundary. Channel existence mutates only at
    /// boundaries, so adjacency is the whole difference between "this opened at
    /// session 13601" and "this opened somewhere in a forty-session window".
    pub exact: bool,
    /// How many session boundaries the change could have happened at. **Zero is
    /// a real and very loud value** — see `mid_session`.
    pub candidate_boundaries: u64,
    /// TRUE when both readings are in the SAME session, so the state changed
    /// with NO session boundary between them.
    ///
    /// **This is the single most interesting thing this module can observe.**
    /// Channel existence mutates only at session boundaries EXCEPT via
    /// `hrmp.force_process_hrmp_open` / `force_process_hrmp_close` /
    /// `force_clean_hrmp`, which gate on `ChannelManager` — Root, the
    /// `GeneralAdmin` track, or an XCM voice from Asset Hub. No such call has
    /// ever been observed on Polkadot: all eleven topology changes measured
    /// across four years landed on ordinary boundaries. A `true` here is either
    /// the first live instance of that path or a defect in the reader, and
    /// either way it must not render as an ordinary vague transition.
    pub mid_session: bool,
}

/// A run of sessions between two readings that nobody read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UnreadInterval {
    pub after_session: u64,
    pub before_session: u64,
    /// Sessions strictly between the two readings — the ones with no reading.
    pub sessions: u64,
    pub after_height: u64,
    pub before_height: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReadingRef {
    pub block_height: u64,
    pub session_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ChannelHistory {
    pub transitions: Vec<Transition>,
    /// Sessions between the first and last reading that were never read. A
    /// channel that opened and closed inside one of these is invisible here, and
    /// `reads_as` says so whenever this list is non-empty.
    pub unread: Vec<UnreadInterval>,
    pub readings: u64,
    /// Readings in which this edge was present in any state.
    pub readings_present: u64,
    pub first_reading: Option<ReadingRef>,
    pub last_reading: Option<ReadingRef>,
    /// The current state as of the newest reading — `null` if absent then, and
    /// `null` also when there are no readings at all. The two are distinguished
    /// by `readings`, and by `reads_as`.
    pub state_at_last_reading: Option<String>,
    pub reads_as: String,
}

/// Sessions between consecutive readings that were never read.
///
/// Takes `(block_height, session_index)` ascending by height. Two readings in
/// the SAME session produce no interval (nothing is missing between them), and
/// adjacent sessions produce none either.
pub fn unread_intervals(readings: &[(u64, u64)]) -> Vec<UnreadInterval> {
    let mut out = Vec::new();
    for pair in readings.windows(2) {
        let (ah, asess) = pair[0];
        let (bh, bsess) = pair[1];
        if bsess > asess + 1 {
            out.push(UnreadInterval {
                after_session: asess,
                before_session: bsess,
                sessions: bsess - asess - 1,
                after_height: ah,
                before_height: bh,
            });
        }
    }
    out
}

/// Derive one edge's open/close history from its observations.
///
/// `obs` must be ascending by `block_height`; both index backends order it that
/// way and a test pins that they agree.
pub fn derive_history(obs: &[EdgeObservation]) -> ChannelHistory {
    let readings = obs.len() as u64;
    let readings_present = obs.iter().filter(|o| o.state.is_some()).count() as u64;
    let unread = unread_intervals(
        &obs.iter()
            .map(|o| (o.block_height, o.session_index))
            .collect::<Vec<_>>(),
    );

    let mut transitions = Vec::new();
    for pair in obs.windows(2) {
        let (a, b) = (&pair[0], &pair[1]);
        if a.state == b.state {
            continue;
        }
        // Sessions strictly between the two readings, plus the boundary that
        // begins `b`'s session: that is how many boundaries the change could
        // have landed on. Adjacent sessions give exactly 1; two readings inside
        // ONE session give 0, which is not a rounding artefact but a claim that
        // the graph moved without a boundary.
        // `saturating_sub` would turn a DECREASING session index into a 0 and
        // therefore into the loud force-call diagnosis below — reporting a
        // row-ordering defect as the rarest event on the chain. Readings arrive
        // ascending by height and session index is monotonic in height, so a
        // decrease is impossible from either backend; if it ever happens it is
        // ours, and it is counted as an unknown number of boundaries rather than
        // as zero.
        let going_backwards = b.session_index < a.session_index;
        let candidate_boundaries = b.session_index.saturating_sub(a.session_index);
        transitions.push(Transition {
            from: a.state.clone(),
            to: b.state.clone(),
            after_height: a.block_height,
            after_session: a.session_index,
            at_height: b.block_height,
            at_session: b.session_index,
            exact: candidate_boundaries == 1 && !going_backwards,
            candidate_boundaries,
            mid_session: candidate_boundaries == 0 && !going_backwards,
        });
    }

    let first_reading = obs.first().map(|o| ReadingRef {
        block_height: o.block_height,
        session_index: o.session_index,
    });
    let last_reading = obs.last().map(|o| ReadingRef {
        block_height: o.block_height,
        session_index: o.session_index,
    });
    let state_at_last_reading = obs.last().and_then(|o| o.state.clone());

    let reads_as = history_reads_as(
        readings,
        readings_present,
        &transitions,
        &unread,
        state_at_last_reading.as_deref(),
    );

    ChannelHistory {
        transitions,
        unread,
        readings,
        readings_present,
        first_reading,
        last_reading,
        state_at_last_reading,
        reads_as,
    }
}

/// One sentence saying what the payload means, with a separate arm for every way
/// it can be empty.
///
/// FOUR ARMS, and the first two exist because they are DIFFERENT FACTS that look
/// identical from outside: an empty answer because nobody has read this chain's
/// graph is a statement about our index, and an empty answer because the edge
/// was absent in every reading is a statement about the chain. Slice 14's
/// blocker 3 was exactly this confusion in mirror image — a fully indexed window
/// in which nothing ran reported the network's silence as our own blind spot.
fn history_reads_as(
    readings: u64,
    readings_present: u64,
    transitions: &[Transition],
    unread: &[UnreadInterval],
    state_now: Option<&str>,
) -> String {
    let mut s = if readings == 0 {
        "NO READING of this chain's HRMP channel graph is on record, so this answer is empty for a \
         reason that has nothing to do with the edge you asked about. That is a statement about \
         OUR INDEX and not about the chain. Run `channels-range` over a height range to record \
         one reading per session boundary."
            .to_string()
    } else if readings_present == 0 {
        format!(
            "This edge was ABSENT from the graph in all {readings} reading(s) on record. That is a \
             statement about the chain: at every height we read, no channel and no pending request \
             existed between these two parachains."
        )
    } else if transitions.is_empty() && state_now == Some("requested") {
        // NOT the same as the arm below, and this is the commonest live shape:
        // 54-66 open requests sit pending across every monthly reading — a real
        // standing population of "asked and never accepted". Telling those that
        // "its opening is earlier than the first reading" would report a channel
        // that has never existed, in the flattering direction.
        format!(
            "This edge has been a PENDING REQUEST in all {readings} reading(s) on record and never \
             became a channel within them. The REQUEST predates our first reading; nothing here \
             says it was ever accepted, and a request can sit unaccepted indefinitely — upstream \
             made them non-expiring."
        )
    } else if transitions.is_empty() {
        format!(
            "This edge was '{}' in all {readings} reading(s) on record and never changed state \
             within them. Its opening is EARLIER than the first reading, so this history says when \
             we started looking rather than when the channel began.",
            state_now.unwrap_or("present")
        )
    } else {
        let exact = transitions.iter().filter(|t| t.exact).count();
        format!(
            "{} state change(s) across {readings} reading(s), of which {exact} are pinned to a \
             single session boundary. Channel existence mutates ONLY at session boundaries, so a \
             change between two readings in adjacent sessions is dated exactly; one across a wider \
             gap is dated to the window and no further.",
            transitions.len()
        )
    };

    if !unread.is_empty() {
        let missing: u64 = unread.iter().map(|u| u.sessions).sum();
        s.push_str(&format!(
            " COVERAGE: {missing} session(s) between the first and last reading were never read \
             (see `unread`). A channel that opened AND CLOSED entirely inside one of those windows \
             left no trace here at all — this history cannot see it, and does not claim to."
        ));
    }

    // The loudest arm, and it is deliberately last so it is the final thing read.
    let mid = transitions.iter().filter(|t| t.mid_session).count();
    if mid > 0 {
        s.push_str(&format!(
            " UNUSUAL: {mid} of these change(s) happened between two readings IN THE SAME SESSION, \
             with no session boundary between them. Channel existence otherwise mutates only at \
             boundaries, so this is either a `ChannelManager` force call \
             (`hrmp.force_process_hrmp_open` / `force_process_hrmp_close` / `force_clean_hrmp`, \
             reachable by Root, by the `GeneralAdmin` track, or by XCM from Asset Hub) or a defect \
             in this reader. No force call has ever been observed on Polkadot — all eleven topology \
             changes measured across four years landed on ordinary boundaries — so this is worth \
             investigating rather than reporting."
        ));
    }
    s
}

/// The coverage list every channel response carries. Each line is true of THIS
/// endpoint's payload; the second review question this project asks every time
/// is whether a shared coverage helper is true on every endpoint that serves it,
/// and these two lists are built separately for that reason.
pub fn channel_not_covered() -> Vec<String> {
    vec![
        "A reading is a STATE SNAPSHOT, not an event stream. The relay's `Hrmp` pallet emits no \
         event when a channel is created or destroyed — all nine of its `deposit_event` sites sit \
         outside the four functions that do it — so there is nothing to index and nothing to \
         replay. Relay block #32492253 destroyed 28 channels and emitted 94 events, none of them \
         `hrmp.*`."
            .to_string(),
        "THREE of the pallet's events announce a completion at REQUEST time and must not be read \
         as dates: `HrmpSystemChannelOpened`, `HrmpChannelForceOpened` (whose extrinsic does not \
         open anything — it creates the request pair) and `ChannelClosed`. This endpoint ignores \
         all of them and reports only what state said."
            .to_string(),
        "The three per-channel MESSAGE COUNTERS are deliberately not stored — `msg_count`, \
         `total_size` and `mqc_head`. They are throughput, not topology: over one session, 16 of \
         224 channels' values moved on `mqc_head` alone with the topology unchanged. No backlog or \
         message-volume figure can be answered from here."
            .to_string(),
        "A channel can change mid-session, outside any boundary, via `hrmp.force_process_hrmp_open` \
         / `force_process_hrmp_close` / `force_clean_hrmp`. Those gate on `ChannelManager`, which \
         Polkadot wires to Root OR the `GeneralAdmin` track OR an XCM voice from Asset Hub — and an \
         XCM-borne one is NOT visible as a relay extrinsic, because the call sits in a `Transact`'s \
         opaque payload. No such call has ever been observed on Polkadot; this is a stated bound \
         with no live instance."
            .to_string(),
        "Pending open requests are recorded; pending CLOSE requests are not. A close request lives \
         at most one session, so a per-session snapshot of it is a coin flip — and offboarding \
         tears channels down with no close request at all, so the map could never have been a \
         complete account of closes."
            .to_string(),
        "This is the relay's view and it is the only complete one. A parachain sees an \
         `AbridgedHrmpChannel` subset of its OWN channels through the relay state proof and cannot \
         see channels it is not an endpoint of."
            .to_string(),
        "A channel is DIRECTIONAL. (A -> B) and (B -> A) are two separate channels, opened, closed \
         and deposited for independently; this endpoint never folds them."
            .to_string(),
        "To join an `xcm.messages` row against this graph, key on the message's RELAY PARENT \
         NUMBER and not its parachain height — the graph is a relay-side fact and the two number \
         lines are unrelated."
            .to_string(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obs(height: u64, session: u64, state: Option<&str>) -> EdgeObservation {
        EdgeObservation {
            block_height: height,
            session_index: session,
            state: state.map(|s| s.to_string()),
        }
    }

    #[test]
    fn an_adjacent_pair_pins_a_change_to_one_boundary_and_a_gap_does_not() {
        // THE POINT OF THE WHOLE FILE. Channel existence mutates only at session
        // boundaries, so two readings one session apart date a change exactly;
        // two readings ten sessions apart date it to a ten-boundary window. If
        // these two rendered the same way, the history would be claiming a
        // precision it does not have.
        let exact = derive_history(&[
            obs(100, 10, None),
            obs(2500, 11, Some("open")),
        ]);
        assert_eq!(exact.transitions.len(), 1);
        assert!(exact.transitions[0].exact);
        assert_eq!(exact.transitions[0].candidate_boundaries, 1);
        assert_eq!(exact.transitions[0].from, None);
        assert_eq!(exact.transitions[0].to, Some("open".into()));

        let vague = derive_history(&[obs(100, 10, None), obs(30000, 20, Some("open"))]);
        assert_eq!(vague.transitions.len(), 1);
        assert!(!vague.transitions[0].exact, "ten sessions apart is not an exact date");
        assert_eq!(vague.transitions[0].candidate_boundaries, 10);
        assert_eq!(vague.unread.len(), 1);
        assert_eq!(vague.unread[0].sessions, 9, "sessions 11..=19 were never read");
        assert!(
            vague.reads_as.contains("opened AND CLOSED"),
            "a gap must warn that a whole channel lifetime can hide in it: {}",
            vague.reads_as
        );
        assert!(
            !exact.reads_as.contains("opened AND CLOSED"),
            "…and a gapless history must NOT carry that warning: {}",
            exact.reads_as
        );
    }

    #[test]
    fn an_empty_answer_says_which_of_the_two_things_it_means() {
        // The pair that must never render alike: nobody looked, versus we looked
        // and it was not there. Slice 14's blocker 3 in mirror image.
        let never_read = derive_history(&[]);
        assert_eq!(never_read.readings, 0);
        assert!(never_read.reads_as.contains("NO READING"), "{}", never_read.reads_as);
        assert!(
            never_read.reads_as.contains("OUR INDEX"),
            "it must name itself as a statement about the index: {}",
            never_read.reads_as
        );
        assert!(never_read.reads_as.contains("channels-range"), "and say what to run");

        let read_and_absent = derive_history(&[obs(100, 10, None), obs(2500, 11, None)]);
        assert_eq!(read_and_absent.readings, 2);
        assert_eq!(read_and_absent.readings_present, 0);
        assert!(read_and_absent.reads_as.contains("ABSENT"), "{}", read_and_absent.reads_as);
        assert!(
            read_and_absent.reads_as.contains("statement about the chain"),
            "this one IS about the chain: {}",
            read_and_absent.reads_as
        );
        assert!(
            !read_and_absent.reads_as.contains("NO READING"),
            "the two arms must not share wording"
        );
    }

    #[test]
    fn a_channel_present_throughout_says_its_opening_predates_our_looking() {
        // The flattering mistake this arm avoids: rendering "no transitions" as
        // "this channel has always existed". All we know is that it existed
        // before we started reading.
        let h = derive_history(&[obs(100, 10, Some("open")), obs(2500, 11, Some("open"))]);
        assert!(h.transitions.is_empty());
        assert_eq!(h.state_at_last_reading, Some("open".into()));
        assert!(h.reads_as.contains("EARLIER than the first reading"), "{}", h.reads_as);
    }

    #[test]
    fn an_edge_that_is_only_ever_a_pending_request_is_never_called_a_channel() {
        // THE COMMONEST LIVE SHAPE, and the arm that would otherwise lie about
        // it: 54-66 open requests sit pending across every monthly reading — a
        // standing population of "asked and never accepted". The no-transitions
        // arm used to tell all of them "its opening is EARLIER than the first
        // reading", which reports a channel that has never existed, in the
        // flattering direction.
        let h = derive_history(&[
            obs(100, 10, Some("requested")),
            obs(2500, 11, Some("requested")),
        ]);
        assert!(h.transitions.is_empty());
        assert_eq!(h.state_at_last_reading, Some("requested".into()));
        let reads_as = &h.reads_as;
        assert!(reads_as.contains("PENDING REQUEST"), "{reads_as}");
        assert!(
            reads_as.contains("never became a channel"),
            "it must deny the acceptance rather than merely omit it: {reads_as}"
        );
        assert!(
            !reads_as.contains("Its opening is EARLIER"),
            "a request has no opening to predate our looking: {reads_as}"
        );

        // …while an edge that really was OPEN throughout keeps that wording.
        let open = derive_history(&[obs(100, 10, Some("open")), obs(2500, 11, Some("open"))]);
        assert!(open.reads_as.contains("Its opening is EARLIER"), "{}", open.reads_as);
        assert!(!open.reads_as.contains("PENDING REQUEST"));
    }

    #[test]
    fn a_request_becoming_a_channel_is_a_transition_and_not_an_open_from_nothing() {
        // The lifecycle the two-state column exists for: requested -> open is one
        // change, and reporting it as "opened from absent" would lose the fact
        // that somebody asked first.
        let h = derive_history(&[
            obs(100, 10, None),
            obs(2500, 11, Some("requested")),
            obs(4900, 12, Some("open")),
            obs(7300, 13, None),
        ]);
        assert_eq!(h.transitions.len(), 3);
        assert_eq!(
            h.transitions
                .iter()
                .map(|t| (t.from.clone(), t.to.clone()))
                .collect::<Vec<_>>(),
            vec![
                (None, Some("requested".into())),
                (Some("requested".into()), Some("open".into())),
                (Some("open".into()), None),
            ]
        );
        assert!(h.transitions.iter().all(|t| t.exact), "all four readings are adjacent");
        assert!(h.unread.is_empty());
        assert_eq!(h.state_at_last_reading, None, "closed by the last reading");
    }

    #[test]
    fn a_change_with_no_boundary_between_the_readings_is_flagged_and_never_looks_ordinary() {
        // THE ARM THAT WOULD OTHERWISE BE SILENT. Two readings inside one session
        // with different states means the graph moved with no session boundary
        // between them — the `ChannelManager` force path, which has never been
        // observed on Polkadot. Rendering it as an ordinary vague transition
        // (exact=false, candidate_boundaries=0) would bury the one finding this
        // module is uniquely placed to make.
        let h = derive_history(&[
            obs(2400, 11, Some("open")),
            obs(2500, 11, None), // same session, channel gone
        ]);
        assert_eq!(h.transitions.len(), 1);
        let t = &h.transitions[0];
        assert!(t.mid_session, "a same-session change must be flagged");
        assert!(!t.exact, "zero boundaries is not an exact date, it is an impossible one");
        assert_eq!(t.candidate_boundaries, 0);
        assert!(h.unread.is_empty(), "one session spans no unread sessions");

        let reads_as = &h.reads_as;
        assert!(reads_as.contains("UNUSUAL"), "{reads_as}");
        assert!(
            reads_as.contains("force_process_hrmp_open"),
            "the message must name the mechanism a reader should go and look for: {reads_as}"
        );
        assert!(
            reads_as.contains("defect in this reader"),
            "…and admit the other explanation: {reads_as}"
        );

        // An ordinary adjacent change must NOT carry any of that.
        let ordinary = derive_history(&[obs(2400, 11, Some("open")), obs(4800, 12, None)]);
        assert!(!ordinary.transitions[0].mid_session);
        assert!(!ordinary.reads_as.contains("UNUSUAL"), "{}", ordinary.reads_as);
    }

    #[test]
    fn two_readings_in_one_session_leave_no_gap_and_neither_do_adjacent_ones() {
        // A spot check taken mid-session is legal, and it must not manufacture a
        // coverage hole. Nor must an adjacent pair.
        assert!(unread_intervals(&[(100, 10), (200, 10)]).is_empty());
        assert!(unread_intervals(&[(100, 10), (2500, 11)]).is_empty());
        assert_eq!(unread_intervals(&[(100, 10), (2500, 13)])[0].sessions, 2);
        assert!(unread_intervals(&[]).is_empty(), "no readings is not a gap, it is an absence");
        assert!(unread_intervals(&[(100, 10)]).is_empty(), "one reading spans nothing");
    }

    #[test]
    fn the_coverage_list_names_the_bound_it_cannot_detect() {
        let lines = channel_not_covered();
        let all = lines.join(" ");
        // The four claims that must survive any rewording of this list, because
        // each is a thing a reader would otherwise assume the opposite of.
        assert!(all.contains("ChannelManager"), "the mid-session bound must be named");
        assert!(
            all.contains("no live instance"),
            "…and stated as unobserved rather than impossible"
        );
        assert!(all.contains("mqc_head"), "the omitted counters must be named");
        assert!(all.contains("DIRECTIONAL"), "direction must be stated");
        assert!(
            all.contains("RELAY PARENT NUMBER"),
            "the xcm.messages join key must be stated — it is the one a caller gets wrong"
        );
        // And nothing here may claim the events are usable.
        assert!(!all.contains("event stream is complete"));
    }
}
