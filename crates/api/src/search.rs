//! `api::search` — the omnibox as a first-class API (Phase 2, slice 10).
//!
//! Grammar: `[network] <codeword|value> [on <chain>]`. Everything except the
//! value is optional, and the response always carries a `reads_as` readout so
//! the caller sees how their input was understood — a wrong guess the user can
//! SEE is a different failure from a wrong guess they cannot.
//!
//! THREE RULES SHAPE EVERY DECISION BELOW.
//!
//! 1. **Ambiguity is DATA, never a guess.** A 32-byte hash is up to six honest
//!    candidates (block, extrinsic, preimage, whitelisted call, account, and —
//!    since Phase 3 slice 3 — XCM message); a bare number is up to four (block,
//!    referendum, parachain, and — since Phase 3 slice 15 — a coretime core).
//!    Each new module makes the paste MORE ambiguous, not less, and that is the
//!    design working. The response lists them with the reason each was offered.
//!    Guessing looks decisive and is occasionally catastrophic — the whole
//!    point of an explorer is that you can trust what it tells you.
//!
//! 2. **Designed for the machine; the UI renders a subset.** Every candidate
//!    carries `kind`, the `chain` it was found on, `why` it was offered, and
//!    its `lineage`. A dropdown can throw most of that away for free; the
//!    reverse is not true, and the first API consumer who asks "why did this
//!    hash resolve to a preimage rather than a block" would otherwise force a
//!    second implementation.
//!
//! 3. **Never prefix-search a hash.** Partial-hash matching is a range scan and
//!    is the one thing here that genuinely does not scale. Full identifier or
//!    nothing. (ROADMAP §Phase 2.)
//!
//! WHAT THIS DELIBERATELY DOES NOT DO. Codewords ship in the phase of the
//! module that can answer them, so `sel` and `contract` are NOT accepted —
//! typing one gets a parse error naming the valid set, not a promise. But a
//! bare NAME is a SHAPE, not vocabulary: people paste display names without
//! being taught to, so `TEXT` resolves from day one to an honest "no name index
//! yet". That reservation is why identity search can land with the People-chain
//! module instead of needing a grammar change.
//!
//! AND ONE WORD IS REFUSED FOR A REASON THAT IS NOT A PHASE. `sale` was refused
//! from slice 10 with "coretime lands in Phase 3"; coretime landed four slices
//! ago and the word is STILL not in the grammar, because `pallet-broker` numbers
//! no sales — `SaleInitialized` carries a region and prices and no ordinal — so
//! `sale 42`, which reads as an ordinal to everybody who types it, would have to
//! be redefined against a coordinate nobody types (a relay block) and would then
//! parse into a confident empty answer about relay block 42. A refusal that says
//! why is better than an answer to a question nobody asked.
//! See `Codeword::refused`.
//!
//! And this is EXACT-IDENTIFIER resolution only. "Find the referendum about the
//! moderation bounty" is a different product with a different failure mode;
//! conflating them would make v1 look thin against a problem it never claimed.

use registry::Registry;

/// The `why` for a block reached by the height the caller actually typed. Named
/// because it is the answer at four call sites and a hash-reached block must not
/// borrow it — see `push_block`.
const HEIGHT_IS_INDEXED: &str = "this height is indexed on this chain";

/// A codeword the v1 grammar accepts. Each one names a module that exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Codeword {
    Ref,
    Acc,
    Block,
    Extrinsic,
    Preimage,
    Whitelist,
    Track,
    Spend,
    Bounty,
    Vote,
    Para,
    Asset,
    Xcm,
    /// A coretime core index. Answerable since Phase 3 slice 14, which shipped
    /// the READER and the indexes — slice 13 shipped the tables and a mapper and
    /// explicitly no reader at all, and every index it created was dropped again
    /// for want of one. See `push_core` below for what it can and cannot probe.
    Core,
}

impl Codeword {
    /// The accepted spellings. Synonyms are listed because a person types `tx`
    /// as readily as `ex`, and refusing one of them is a papercut with no
    /// upside.
    pub fn parse(word: &str) -> Option<Self> {
        Some(match word {
            "ref" | "referendum" => Self::Ref,
            "acc" | "account" | "address" => Self::Acc,
            "block" | "b" => Self::Block,
            "ex" | "extrinsic" | "tx" => Self::Extrinsic,
            "preimage" => Self::Preimage,
            "whitelist" | "whitelisted" => Self::Whitelist,
            "track" => Self::Track,
            "spend" => Self::Spend,
            "bounty" => Self::Bounty,
            "vote" | "votes" => Self::Vote,
            "para" | "chain" | "parachain" => Self::Para,
            "asset" | "token" => Self::Asset,
            "xcm" | "message" => Self::Xcm,
            "core" => Self::Core,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ref => "ref",
            Self::Acc => "acc",
            Self::Block => "block",
            Self::Extrinsic => "ex",
            Self::Preimage => "preimage",
            Self::Whitelist => "whitelist",
            Self::Track => "track",
            Self::Spend => "spend",
            Self::Bounty => "bounty",
            Self::Vote => "vote",
            Self::Para => "para",
            Self::Asset => "asset",
            Self::Xcm => "xcm",
            Self::Core => "core",
        }
    }

    /// Every codeword v1 serves, for the grammar reference and for the error
    /// message an unknown one produces.
    pub const ALL: [Codeword; 14] = [
        Self::Ref,
        Self::Acc,
        Self::Block,
        Self::Extrinsic,
        Self::Preimage,
        Self::Whitelist,
        Self::Track,
        Self::Spend,
        Self::Bounty,
        Self::Vote,
        Self::Para,
        Self::Asset,
        Self::Xcm,
        Self::Core,
    ];

    /// Words the grammar REFUSES, and why. Two different reasons, and conflating
    /// them is what this function was renamed to stop.
    ///
    /// 1. A module that has not shipped. The refusal names the PHASE, which is
    ///    the difference between a roadmap and a typo — and it is a promise with
    ///    an expiry date, so it must be paid off in the slice that ships the
    ///    module. `xcm` was paid in Phase 3 slice 3; `core` in slice 15.
    ///
    /// 2. A spelling that can never work, however many modules ship. `sale` is
    ///    the only one, and it is here because the refusal it USED to carry —
    ///    "coretime lands in Phase 3" — was still being served three slices after
    ///    coretime landed, telling readers to wait for something that already
    ///    existed. Naming a phase that has shipped is worse than naming one that
    ///    has not.
    ///
    /// WHY `sale <n>` IS NOT DEFINABLE, stated so nobody redefines it by
    /// accident. `pallet-broker` emits no sale identifier: `SaleInitialized`
    /// carries `region_begin`/`region_end`, `first_core` and prices, and a sale
    /// is identified by the REGION it sells or by the relay block its
    /// assignments take effect at — which this API already calls
    /// `governing_relay_block`. NEITHER IS AN ORDINAL, and that alone carries the
    /// refusal: nobody typing `sale 42` means relay block 42 or timeslice 42, so
    /// defining the codeword against either would parse a question the caller
    /// did not ask and answer it emptily. Minting an ordinal of our own is worse
    /// — it is the `bounty 999999` defect (a candidate fabricated for any id at
    /// all) with a coretime label on it.
    ///
    /// A SECOND ARGUMENT WAS CONSIDERED AND IS DELIBERATELY NOT LOAD-BEARING,
    /// because it turned out to be weaker than it first read. "No endpoint
    /// serves a whole sale" is true of the SUBJECT lines — `/entitlement` takes
    /// a core or a task, `/delta` takes a window — but `/delta?from=X&to=X` with
    /// `X` a governing relay block does return that sale's whole workload per
    /// core, so a relay-block-keyed candidate would in fact have somewhere to
    /// point. It is the ordinal that does not exist, not the destination.
    pub fn refused(word: &str) -> Option<&'static str> {
        Some(match word {
            "sale" => {
                "`pallet-broker` numbers no sales, so there is nothing for `sale <n>` to name. \
                 A sale is identified by the REGION it sells (`region_begin`/`region_end`, in \
                 timeslices) or by the relay block its assignments take effect at, which this \
                 API calls `governing_relay_block` — and neither is an ordinal, so `sale 42` \
                 would have to mean something nobody typing it means. Ask `core <n>` instead: \
                 a candidate carrying an assignment reports the `governing_relay_block` of the \
                 sale that assigned it"
            }
            "contract" | "sel" | "selector" => "contracts land in Phase 5",
            _ => return None,
        })
    }
}

/// What the input LOOKS like, when no codeword told us.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Shape {
    /// `0x` + 64 hex. The genuinely ambiguous one.
    Hash32(String),
    /// `0x` + 40 hex — an Ethereum-shaped address.
    Addr20(String),
    /// A decodable SS58 address.
    Ss58(String),
    /// `<height>-<index>`, the canonical extrinsic coordinate.
    ExtrinsicId { height: u64, index: u32 },
    /// A bare integer. Ambiguous across id spaces.
    Number(u64),
    /// Short, alphanumeric, no spaces — looks like an asset ticker.
    Symbol(String),
    /// Anything else. Reserved for the identity index; honest until then.
    Text(String),
}

/// One parsed query. Pure data — the resolver turns it into candidates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Query {
    /// Network the search runs against; defaults to the registry's only
    /// network when the user names none.
    pub network: String,
    /// `on <chain>` — scopes the search AND narrows ambiguity (a bare number
    /// on a named chain is a block height, because referendum and para ids are
    /// network-wide rather than chain-scoped).
    pub chain: Option<String>,
    pub term: Term,
    /// The parse readout. Ships WITH the response, always.
    pub reads_as: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Term {
    Codeword { word: Codeword, arg: String },
    Shape(Shape),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    pub message: String,
    /// What the user could have typed instead. Never empty.
    pub expected: Vec<String>,
}

/// Parse one raw query string against the registry (which supplies chain
/// aliases and network names, so a chain added in Phase 3 needs no edit here).
pub fn parse(raw: &str, registry: &Registry) -> Result<Query, ParseError> {
    let s = raw.trim();
    if s.is_empty() {
        return Err(ParseError {
            message: "empty query".into(),
            expected: vec![
                "ref 1930".into(),
                "a block number".into(),
                "an SS58 address".into(),
                "a 0x hash".into(),
            ],
        });
    }

    let networks = known_networks(registry);
    let default_network = networks
        .first()
        .cloned()
        .unwrap_or_else(|| "polkadot".to_string());

    // --- optional leading network: `ksm ref 500`
    let mut rest = s.to_string();
    let mut network = default_network.clone();
    if let Some((head, tail)) = rest.split_once(char::is_whitespace) {
        let tail = tail.trim();
        if !tail.is_empty() {
            if let Some(n) = match_network(head, &networks) {
                // ONLY when what follows is unambiguously a value. A network is
                // also a word people put in NAMES — "Polkadot Treasury" — and
                // eating the first word there turns a name search into a search
                // for "Treasury", silently. So the remainder must either start
                // with a codeword or be a single self-identifying token.
                let first = tail
                    .split_whitespace()
                    .next()
                    .unwrap_or("")
                    .to_ascii_lowercase();
                let looks_like_a_query = Codeword::parse(&first).is_some()
                    || Codeword::refused(&first).is_some()
                    || (tail.split_whitespace().count() == 1
                        && !matches!(infer_shape(tail), Shape::Text(_) | Shape::Symbol(_)));
                if looks_like_a_query {
                    network = n;
                    rest = tail.to_string();
                }
            }
        }
    }

    // --- optional trailing chain: `... on ah` / `... @ah`
    let mut chain: Option<String> = None;
    if let Some((head, tok)) = rest.rsplit_once('@') {
        match registry.chain_by_alias(tok.trim()) {
            Some(c) => {
                chain = Some(c.id.clone());
                rest = head.trim().to_string();
            }
            // same refusal as the ` on ` form below — two syntaxes for one
            // thing must fail the same way, or one of them is a trap
            None => {
                return Err(ParseError {
                    message: format!("unknown chain '{}'", tok.trim()),
                    expected: registry
                        .chain_aliases()
                        .into_iter()
                        .map(|(a, _)| a.to_string())
                        .collect(),
                })
            }
        }
    }
    if chain.is_none() {
        // ASCII fold ONLY: `to_lowercase()` is Unicode-aware and changes byte
        // LENGTH (U+212A KELVIN SIGN is 3 bytes, folds to a 1-byte `k`), so a
        // byte offset found in the folded string can land mid-character in the
        // original — a panic on a public GET. ` on ` is ASCII anyway.
        let lower = rest.to_ascii_lowercase();
        if let Some(pos) = lower.rfind(" on ") {
            let tok = rest[pos + 4..].trim();
            match registry.chain_by_alias(tok) {
                Some(c) => {
                    chain = Some(c.id.clone());
                    rest = rest[..pos].trim().to_string();
                }
                // An `on <token>` the registry does not know is a FAILURE, not
                // a filter to drop silently — slice 4's finding, where a query
                // parameter was accepted and ignored, cost a whole endpoint.
                None => {
                    return Err(ParseError {
                        message: format!("unknown chain '{tok}'"),
                        expected: registry
                            .chain_aliases()
                            .into_iter()
                            .map(|(a, _)| a.to_string())
                            .collect(),
                    })
                }
            }
        }
    }
    if rest.is_empty() {
        return Err(ParseError {
            message: "a chain was named but nothing was searched for".into(),
            expected: vec!["block 19532910 on ah".into()],
        });
    }

    // --- codeword, or a shape
    let term = match rest.split_once(char::is_whitespace) {
        Some((head, arg)) if !arg.trim().is_empty() => {
            let lower = head.to_lowercase();
            match Codeword::parse(&lower) {
                Some(word) => Term::Codeword {
                    word,
                    arg: arg.trim().to_string(),
                },
                None => match Codeword::refused(&lower) {
                    Some(why) => {
                        return Err(ParseError {
                            message: format!("'{lower}' is not in the grammar — {why}"),
                            expected: Codeword::ALL.iter().map(|c| c.as_str().into()).collect(),
                        })
                    }
                    // Not a codeword and not reserved: the whole string is a
                    // value that happens to contain a space, e.g. a name.
                    None => Term::Shape(infer_shape(&rest)),
                },
            }
        }
        _ => {
            let lower = rest.to_lowercase();
            // A bare word that IS a codeword is an incomplete query, not a name
            // search — say so rather than searching for the word "ref".
            if Codeword::parse(&lower).is_some() {
                return Err(ParseError {
                    message: format!("'{lower}' needs an argument"),
                    expected: vec![format!("{lower} <value>")],
                });
            }
            if let Some(why) = Codeword::refused(&lower) {
                return Err(ParseError {
                    message: format!("'{lower}' is not in the grammar — {why}"),
                    expected: Codeword::ALL.iter().map(|c| c.as_str().into()).collect(),
                });
            }
            Term::Shape(infer_shape(&rest))
        }
    };

    let reads_as = readout(&network, chain.as_deref(), &term);
    Ok(Query {
        network,
        chain,
        term,
        reads_as,
    })
}

/// Shape inference. Order matters: the most specific pattern wins, and the
/// catch-all is TEXT rather than an error, because a person pasting a display
/// name deserves "no name index yet" and not "invalid input".
pub fn infer_shape(s: &str) -> Shape {
    let t = s.trim();
    let bare = t.replace(',', "");

    if let Some(hex) = bare.strip_prefix("0x").or_else(|| bare.strip_prefix("0X")) {
        if hex.len() == 64 && hex.chars().all(|c| c.is_ascii_hexdigit()) {
            return Shape::Hash32(format!("0x{}", hex.to_ascii_lowercase()));
        }
        if hex.len() == 40 && hex.chars().all(|c| c.is_ascii_hexdigit()) {
            return Shape::Addr20(format!("0x{}", hex.to_ascii_lowercase()));
        }
    }

    // `19532910-4` — the extrinsic coordinate dotlens itself prints.
    if let Some((h, i)) = bare.split_once('-') {
        if let (Ok(height), Ok(index)) = (h.parse::<u64>(), i.parse::<u32>()) {
            return Shape::ExtrinsicId { height, index };
        }
    }

    if let Ok(n) = bare.parse::<u64>() {
        return Shape::Number(n);
    }

    // SS58: base58 alphabet, and the length band Substrate addresses occupy.
    // Checked by SHAPE here; the resolver hands it to the adapter's
    // checksum-verifying parser, which is the only thing that can say for sure.
    let is_b58 = |c: char| c.is_ascii_alphanumeric() && !"0OIl".contains(c);
    if (46..=50).contains(&t.len()) && t.chars().all(is_b58) {
        return Shape::Ss58(t.to_string());
    }

    // A short alphanumeric run with no spaces reads as a ticker. Deliberately
    // NOT exclusive: the resolver probes symbols AND reports the name-index gap,
    // because `DOT` is a symbol and `Polkadot Treasury` is a name, and a
    // two-character difference should not decide which question we answer.
    if t.len() <= 12
        && !t.contains(char::is_whitespace)
        && t.chars().all(|c| c.is_ascii_alphanumeric())
    {
        return Shape::Symbol(t.to_string());
    }

    Shape::Text(t.to_string())
}

/// Networks the registry knows.
///
/// The DEFAULT (index 0) is the network of a RELAY chain — a chain with no
/// `relay` of its own — not the alphabetically first. Sorting and then taking
/// the first would silently re-point every un-prefixed query at Kusama the day
/// Kusama is registered, which is a behaviour change nobody would have asked
/// for and nothing would have caught.
/// PUB(CRATE) since slice 7: `get_treasury_consolidated` refuses an unknown
/// network, and two implementations of "which networks exist" that could
/// disagree is the defect class this crate already avoided once by depending on
/// `sim` rather than re-deriving its set arithmetic.
pub(crate) fn known_networks(registry: &Registry) -> Vec<String> {
    let mut seen: Vec<String> = Vec::new();
    for c in registry.chains() {
        if !seen.iter().any(|n| n == &c.network) {
            seen.push(c.network.clone());
        }
    }
    seen.sort();
    if let Some(pos) = seen.iter().position(|n| {
        registry
            .chains()
            .any(|c| &c.network == n && c.relay.is_none())
    }) {
        seen.swap(0, pos);
    }
    seen
}

/// A leading token naming a network — its own name, or the ticker people type.
fn match_network(token: &str, networks: &[String]) -> Option<String> {
    let t = token.to_lowercase();
    let want = match t.as_str() {
        "dot" => "polkadot",
        "ksm" => "kusama",
        other => other,
    };
    networks.iter().find(|n| n.to_lowercase() == want).cloned()
}

/// The human-readable parse readout — "reads as: referendum 1930 on polkadot".
fn readout(network: &str, chain: Option<&str>, term: &Term) -> String {
    let body = match term {
        Term::Codeword { word, arg } => format!("{} {arg}", word.as_str()),
        Term::Shape(s) => match s {
            Shape::Hash32(h) => format!("a 32-byte hash {h}"),
            Shape::Addr20(a) => format!("a 20-byte address {a}"),
            Shape::Ss58(a) => format!("the account {a}"),
            Shape::ExtrinsicId { height, index } => format!("extrinsic {height}-{index}"),
            Shape::Number(n) => format!("the number {n}"),
            Shape::Symbol(s) => format!("the symbol {s}"),
            Shape::Text(t) => format!("the name \"{t}\""),
        },
    };
    match chain {
        Some(c) => format!("{body} on {c}"),
        None => format!("{body} on {network}"),
    }
}

// ---------------------------------------------------------------- resolution

/// One thing the input COULD be.
///
/// Every field except `kind` exists because the response is designed for the
/// machine (ROADMAP §Phase 2): a dropdown renders `title` and throws the rest
/// away at zero cost, while an API consumer asking "why did this hash resolve
/// to a preimage rather than a block" gets an answer without a second endpoint.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Candidate {
    /// referendum | block | extrinsic | account | preimage | whitelisted_call |
    /// asset | track | spend | bounty | para | xcm_message | coretime_core
    pub kind: &'static str,
    /// The chain it was FOUND on — never one the caller had to name.
    pub chain: Option<String>,
    /// Human title for a dropdown row.
    pub title: String,
    /// The endpoint that answers it in full.
    pub href: String,
    /// Why this candidate was offered. The honest-coverage doctrine applied to
    /// search: ambiguity is data, and so is the reason for it.
    pub why: &'static str,
    /// Whatever identifies the object, for a caller that wants to act on it.
    pub id: serde_json::Value,
    /// The lineage of the row this candidate came from — spec_version,
    /// decoder/mapper version, raw location — whatever the source carries.
    /// Named in the Phase 2 exit criterion, and the reason this response is
    /// designed for the machine: a client that wants to VERIFY a resolution
    /// needs to know which runtime decoded it and where the bytes are.
    /// None where the source row genuinely has none, never an empty object
    /// pretending to be provenance.
    pub lineage: Option<serde_json::Value>,
}

/// What the resolver could NOT do, in the payload rather than a doc.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SearchResponse {
    pub query: String,
    pub reads_as: String,
    pub network: String,
    pub chain: Option<String>,
    /// Empty is a legitimate answer and is NOT an error — "indexed nothing
    /// matching this" and "this input made no sense" are different failures and
    /// the caller must be able to tell them apart.
    pub candidates: Vec<Candidate>,
    pub not_covered: Vec<String>,
}

/// The bounded fan-out. Every probe below is a POINT lookup on an indexed key
/// — verified per key in migration 0013, which had to CREATE two of them, and
/// in migration 0026 for the two coretime keys this file gained in slice 15
/// (`core_assignments_core_idx`, `broker_events_core_idx`).
/// Nothing here scans, and nothing here prefix-matches a hash.
pub async fn resolve(q: &Query, state: &crate::AppState, raw: &str) -> SearchResponse {
    let mut candidates = Vec::new();
    let mut not_covered = Vec::new();

    match &q.term {
        Term::Codeword { word, arg } => {
            resolve_codeword(*word, arg, q, state, &mut candidates, &mut not_covered).await
        }
        Term::Shape(shape) => {
            resolve_shape(shape, q, state, &mut candidates, &mut not_covered).await
        }
    }

    // `on <chain>` SCOPES the result, and it has to be enforced in ONE place:
    // six probes each remembering to honour it is six chances to forget, and a
    // filter that is parsed and then dropped is exactly slice 4's defect. A
    // candidate with no chain (an account, a network-wide id) is network-wide
    // by construction and survives the scope rather than being filtered out.
    if let Some(want) = &q.chain {
        candidates.retain(|c| c.chain.as_ref().map(|c| c == want).unwrap_or(true));
    }

    SearchResponse {
        query: raw.to_string(),
        reads_as: q.reads_as.clone(),
        network: q.network.clone(),
        chain: q.chain.clone(),
        candidates,
        not_covered,
    }
}

async fn resolve_shape(
    shape: &Shape,
    q: &Query,
    state: &crate::AppState,
    out: &mut Vec<Candidate>,
    gaps: &mut Vec<String>,
) {
    match shape {
        // THE ambiguous case, and the exit criterion's headline: four honest
        // candidates rather than a guess. Each probe is one indexed lookup.
        Shape::Hash32(h) => {
            if let Ok(hits) = state.blocks.blocks_by_hash(h).await {
                for (chain, height) in hits {
                    push_block(
                        state,
                        &chain,
                        height,
                        "a block with this hash is indexed",
                        out,
                    )
                    .await;
                }
            }
            if let Ok(hits) = state.blocks.extrinsics_by_hash(h).await {
                for (chain, height, index) in hits {
                    push_extrinsic(state, &chain, height, index, out).await;
                }
            }
            for chain in gov_chains(state, &q.network) {
                if let Ok(Some(p)) = state.gov.preimage(&chain, h).await {
                    out.push(Candidate {
                        kind: "preimage",
                        chain: Some(chain.clone()),
                        title: p
                            .call_summary
                            .clone()
                            .unwrap_or_else(|| format!("preimage ({})", p.decode_status)),
                        href: format!("/v1/gov/{}/referenda", q.network),
                        why: "a preimage is stored under this hash",
                        id: serde_json::json!({ "proposal_hash": h, "len": p.len }),
                        lineage: Some(serde_json::json!({
                            "spec_version": p.spec_version,
                            "decode_status": p.decode_status,
                        })),
                    });
                }
                if let Ok(Some(w)) = state.gov.whitelisted_call(&chain, h).await {
                    out.push(Candidate {
                        kind: "whitelisted_call",
                        chain: Some(chain.clone()),
                        title: format!("whitelisted call ({})", w.status),
                        href: format!("/v1/gov/{}/whitelist/{h}", q.network),
                        why: "the Fellowship whitelisted a call with this hash",
                        id: serde_json::json!({ "call_hash": h }),
                        lineage: Some(serde_json::json!({
                            "runtime_version": w.runtime_version,
                            "mapper_version": w.mapper_version,
                        })),
                    });
                }
            }
            // The most common 32-byte hex a Polkadot user pastes is an ACCOUNT
            // public key, not a hash at all — and this API already accepts
            // 0x-hex accounts. Omitting it would have made the single most
            // likely paste return nothing.
            push_account(state, q, h, out, &mut Vec::new()).await;
            // An XCM id is also a 32-byte hash, and since Phase 3 slice 3 it is
            // a candidate rather than a gap line. ONE probe, no alias expansion:
            // a bare paste asks "what is this", and the journey endpoint each
            // candidate points at does the expanding.
            push_xcm(state, h, false, out).await;
        }

        // A bare number is ambiguous across id spaces — UNLESS a chain was
        // named, which makes it a block height, because referendum and para ids
        // are network-wide rather than chain-scoped.
        Shape::Number(n) => {
            if let Some(chain) = &q.chain {
                push_block(state, chain, *n, HEIGHT_IS_INDEXED, out).await;
                return;
            }
            for chain in gov_chains(state, &q.network) {
                push_block(state, &chain, *n, HEIGHT_IS_INDEXED, out).await;
            }
            push_referendum(state, q, *n, out, gaps).await;
            // A small number is also a plausible CORE INDEX, and since slice 14
            // there is an indexed key to ask. `explicit = false`: the probe is
            // silent when it misses, because a gap line on every bare number
            // saying "this might have been a core" would be noise on the most
            // common shape in the grammar. A miss produces nothing; a HIT
            // produces a candidate and the one caveat that candidate needs.
            if let Ok(core) = u32::try_from(*n) {
                push_core(state, q, core, false, out, gaps).await;
            }
            if let Some(c) = state
                .registry
                .chains()
                .find(|c| c.para_id == Some(*n as u32) && c.network == q.network)
            {
                out.push(Candidate {
                    kind: "para",
                    chain: Some(c.id.clone()),
                    title: format!("{} (para {n})", c.name),
                    href: "/v1/chains".to_string(),
                    why: "a registered parachain has this para id",
                    id: serde_json::json!({ "para_id": n, "chain": c.id }),
                    lineage: None,
                });
            }
        }

        Shape::Ss58(addr) => push_account(state, q, addr, out, gaps).await,
        // 20-byte 0x is an Ethereum-shaped address. Substrate accounts are 32
        // bytes, so this can only be a contract or a mapped account — neither
        // indexed yet. Honest rather than empty.
        Shape::Addr20(a) => gaps.push(format!(
            "{a} is a 20-byte address: a contract or a mapped account. The contracts module \
             lands in Phase 5, so nothing here can resolve it yet"
        )),
        Shape::ExtrinsicId { height, index } => {
            let chains: Vec<String> = match &q.chain {
                Some(c) => vec![c.clone()],
                None => state.registry.chains().map(|c| c.id.clone()).collect(),
            };
            for chain in chains {
                if let Ok(Some(b)) = state.blocks.get(&chain, *height).await {
                    if b.transactions.iter().any(|t| t.index == *index) {
                        out.push(Candidate {
                            kind: "extrinsic",
                            chain: Some(chain.clone()),
                            title: format!("extrinsic {height}-{index}"),
                            href: format!("/v1/blocks/{chain}/{height}"),
                            why: "this block carries an extrinsic at that index",
                            id: serde_json::json!({ "height": height, "index": index }),
                            lineage: None,
                        });
                    }
                }
            }
        }
        Shape::Symbol(sym) => {
            push_symbol(state, sym, out).await;
            // A short word is BOTH a plausible ticker and a plausible name, and
            // two characters should not decide which question we answer.
            gaps.push(name_gap(sym));
        }
        Shape::Text(t) => gaps.push(name_gap(t)),
    }
}

/// The reservation that lets identity search land with the People-chain module
/// instead of needing a grammar change.
fn name_gap(t: &str) -> String {
    format!(
        "\"{t}\" may be an on-chain identity display name or a labelled account name; there is \
         no name index yet (the identity module lands with People in Phase 5), so this is \
         reported rather than silently returning nothing"
    )
}

async fn resolve_codeword(
    word: Codeword,
    arg: &str,
    q: &Query,
    state: &crate::AppState,
    out: &mut Vec<Candidate>,
    gaps: &mut Vec<String>,
) {
    let num = arg.replace(',', "").parse::<u64>().ok();
    match word {
        Codeword::Ref => match num {
            Some(n) => push_referendum(state, q, n, out, gaps).await,
            None => gaps.push(format!("'ref {arg}' — a referendum id is a number")),
        },
        // The codeword refused since slice 10 with "coretime lands in Phase 3".
        // It landed — occupancy in slice 11, entitlement in 13, the delta in 14 —
        // and a refusal naming a phase that has SHIPPED tells a reader to wait
        // for something that already exists, which is worse than one naming a
        // phase that has not.
        Codeword::Core => match num.and_then(|n| u32::try_from(n).ok()) {
            Some(core) => {
                push_core(state, q, core, true, out, gaps).await;
            }
            None => gaps.push(format!(
                "'core {arg}' — a core index is a small non-negative number (the relay's \
                 `CoreIndex`, a u32). It is not a para id and not a task id: core 47 and \
                 parachain 47 are different id spaces that happen to share a number line"
            )),
        },
        Codeword::Block => match (num, &q.chain) {
            (Some(n), Some(chain)) => push_block(state, chain, n, HEIGHT_IS_INDEXED, out).await,
            (Some(n), None) => {
                for chain in gov_chains(state, &q.network) {
                    push_block(state, &chain, n, HEIGHT_IS_INDEXED, out).await;
                }
            }
            (None, _) => match infer_shape(arg) {
                Shape::Hash32(h) => {
                    if let Ok(hits) = state.blocks.blocks_by_hash(&h).await {
                        for (chain, height) in hits {
                            // Asked BY hash, so the reason is the hash — not the
                            // height, which the caller never typed.
                            push_block(
                                state,
                                &chain,
                                height,
                                "a block with this hash is indexed",
                                out,
                            )
                            .await;
                        }
                    }
                }
                _ => gaps.push(format!(
                    "'block {arg}' — expected a height or a 0x block hash"
                )),
            },
        },
        Codeword::Acc => push_account(state, q, arg, out, gaps).await,
        Codeword::Preimage | Codeword::Whitelist => match infer_shape(arg) {
            Shape::Hash32(h) => resolve_shape(&Shape::Hash32(h), q, state, out, gaps).await,
            _ => gaps.push(format!(
                "'{} {arg}' — expected a 32-byte 0x hash",
                word.as_str()
            )),
        },
        Codeword::Asset => {
            push_symbol(state, arg, out).await;
        }
        Codeword::Extrinsic => resolve_shape(&infer_shape(arg), q, state, out, gaps).await,
        // The codeword refused since Phase 2 slice 10 with "XCM journeys land in
        // Phase 3". They have landed, so it answers — and unlike the bare-hash
        // shape it expands ALIASES first, because someone who types `xcm` has
        // asked about the message rather than about the id, and a wire hash and
        // its topic are the same message.
        Codeword::Xcm => match infer_shape(arg) {
            Shape::Hash32(h) => push_xcm(state, &h, true, out).await,
            _ => gaps.push(format!(
                "'xcm {arg}' — an XCM id is a 32-byte 0x hash: a topic (pallet_xcm's \
                 message_id), a wire hash (blake2_256 of the queued bytes), or the \
                 messageQueue id, which is either"
            )),
        },
        Codeword::Para => {
            if let Some(c) = state.registry.chain_by_alias(arg).or_else(|| {
                state
                    .registry
                    .chains()
                    .find(|c| Some(arg) == c.para_id.map(|p| p.to_string()).as_deref())
            }) {
                out.push(Candidate {
                    kind: "para",
                    chain: Some(c.id.clone()),
                    title: c.name.clone(),
                    href: "/v1/chains".into(),
                    why: "a registered chain matches this para id or alias",
                    id: serde_json::json!({ "chain": c.id, "para_id": c.para_id }),
                    lineage: None,
                });
            }
        }
        // THESE FOUR ARE NOT PROBED, AND THEREFORE DO NOT PRODUCE CANDIDATES.
        //
        // A first draft emitted one anyway — a row with a plausible href and a
        // `why` that read like a finding — for any id at all, so `bounty 999999`
        // returned a bounty. That is a GUESS wearing a candidate's clothes, and
        // it violates the first rule in this file's header on the one surface
        // where the whole product's credibility rests. Their point lookups are
        // keyed by more than the id typed (a spend by instance AND id space, a
        // bounty by instance and child, a track by pallet), so probing them
        // properly is its own piece of work rather than a one-liner.
        //
        // Until then: say so, and name the endpoint that does answer it. An
        // honest gap is navigable; a fabricated candidate is not.
        Codeword::Spend | Codeword::Bounty | Codeword::Track | Codeword::Vote => {
            let href = match word {
                Codeword::Spend => format!("/v1/treasury/{}/spends/{arg}", q.network),
                Codeword::Bounty => format!("/v1/bounties/{}/{arg}", q.network),
                Codeword::Track => format!("/v1/gov/{}/tracks", q.network),
                _ => format!("/v1/gov/{}/accounts/{arg}/votes", q.network),
            };
            gaps.push(format!(
                "'{} {arg}' is not probed by the resolver yet — these ids are keyed by more \
                 than the number typed (an instance, an id space, a pallet), so v1 does not \
                 guess which was meant. {href} answers it directly.",
                word.as_str()
            ));
        }
    }
}

// ------------------------------------------------------------------ probes

/// XCM observations of one id → one candidate each.
///
/// ONE CANDIDATE PER OBSERVATION, not one per journey, and that is the file's
/// first rule rather than laziness: a journey spans chains and therefore has no
/// single `chain` and no single `lineage`, and a candidate that carried neither
/// would be the one row in the response with nothing to verify it by. Each
/// observation has both; they all point at the same journey.
///
/// `expand` follows recorded wire_hash<->topic links first. One extra probe, and
/// it is what makes `xcm <wire hash>` find the journey the receiving chain
/// reported under the TOPIC — the exact dead end slice 2 measured.
async fn push_xcm(state: &crate::AppState, id: &str, expand: bool, out: &mut Vec<Candidate>) {
    let mut ids = vec![id.to_string()];
    if expand {
        if let Ok(links) = state.xcm.aliases(id).await {
            for l in links {
                for other in [l.wire_hash, l.topic] {
                    if !ids.contains(&other) {
                        ids.push(other);
                    }
                }
            }
        }
    }
    let Ok(rows) = state.xcm.by_message_ids(&ids).await else {
        return;
    };
    for r in rows {
        let direct = r.message_id.as_deref() == Some(id);
        let where_to = match (r.side.as_str(), &r.counterparty) {
            ("sent", Some(c)) => format!("XCM sent to {c}"),
            ("received", Some(c)) => format!("XCM processed from {c}"),
            ("local", _) => "XCM executed locally".to_string(),
            // Any other side, with or without a counterparty. A side we do not
            // recognise has no known direction, so naming the counterparty here
            // would have to guess a preposition ("to" or "from") — the one thing
            // this file must not do. Report the side and let the journey say the
            // rest.
            (side, _) => format!("XCM {side}"),
        };
        out.push(Candidate {
            kind: "xcm_message",
            chain: Some(r.chain_id.clone()),
            title: format!("{where_to} ({}, {})", r.transport, r.status),
            href: format!("/v1/xcm/journeys/{id}"),
            why: if direct {
                "this id was observed as an XCM message on this chain"
            } else {
                "a recorded wire_hash<->topic link ties the id you typed to this observation — \
                 see the journey's `aliases` for the rule and evidence it rests on"
            },
            id: serde_json::json!({
                "message_id": r.message_id,
                "id_kind": r.id_kind,
                "side": r.side,
                "block_height": r.block_height,
                "event_index": r.event_index,
            }),
            lineage: Some(serde_json::json!({
                "runtime_version": r.runtime_version,
                "mapper_version": r.mapper_version,
            })),
        });
    }
}

/// Why a core candidate carrying an assignment was offered. Long, because the
/// caveat belongs ON the candidate and not only in `not_covered`: a dropdown that
/// renders candidates and throws coverage away must still not imply that a core
/// index is a durable identity.
/// TWO WORDS IN HERE ARE DELIBERATE. "ASSIGNED", never "sold": cores below
/// `SaleInfo.first_core` are RESERVED system cores set by governance and never
/// bought (measured `first_core = 11` on Polkadot, so cores 0-10 are all of that
/// kind), and this candidate does not read that boundary. "ANNOUNCED", because
/// `core_assignments` is the broker's own XCM-side announcement and the relay's
/// applied half is not read here — the first line of the entitlement endpoint's
/// own coverage list, which a candidate rendered without it would contradict.
const WHY_CORE_ASSIGNED: &str =
    "the coretime chain ANNOUNCED an assignment for this core index — a SLOT rather than a \
     tenant: a renewal MOVES the index, so this is what the core was assigned to do as of the \
     `governing_relay_block` in this candidate's id, and not necessarily what holds it now. \
     Announced is not applied: the relay's own applied half is not read here";

/// Why a core candidate with no assignment was offered. A different fact, and
/// the difference is the one migration 0025 forbids this project to blur: an
/// absent assignment is `unknown`, never `idle`.
const WHY_CORE_EVENT: &str =
    "a `pallet-broker` event on the coretime chain names this core index, though our index holds \
     no assignment for it — which means the sale that governs it is outside what we indexed, and \
     NOT that the core was unsold";

/// A coretime core index → the entitlement on record for it.
///
/// THIS PROBES ONE HALF OF CORETIME AND SAYS SO. Entitlement (what a core was
/// ASSIGNED to do — never "sold", see `WHY_CORE_ASSIGNED`) is a point lookup on
/// `core_assignments_core_idx` and needs no window; occupancy (what it actually
/// did) is a RATIO over a window, and both endpoints
/// that serve one refuse to invent a window for exactly the reason search would
/// have to invent one — "defaulting to one would put a window nobody chose
/// underneath a percentage somebody quotes". So the occupancy half and the delta
/// are named in `not_covered` with their query parameters, never guessed at.
///
/// `explicit` distinguishes the codeword from the bare-number probe. A bare
/// number is the commonest shape in the grammar and reaches here on every one;
/// annotating all of them with coretime caveats would be noise, so a silent miss
/// is right there and a stated one is right for `core <n>`, where the caller
/// asked the question. THE COST IS STATED RATHER THAN GLOSSED: a bare number
/// costs one indexed lookup on a hit and TWO on a miss, and a miss is the normal
/// case for a block height. Both are point lookups on
/// `core_assignments_core_idx` / `broker_events_core_idx` (migration 0026). The
/// EXPLICIT miss path makes a third read, `latest_broker_config`, which is an
/// ordered read of one row off the `(chain_id, block_height)` primary key rather
/// than a point lookup — still nothing that scans, and named here rather than
/// hidden under the file's opening claim.
///
/// BOTH CHAINS COME FROM THE REGISTRY through `coretime_pair`, which refuses on
/// zero or two rather than picking (Invariant 2 — no chain id appears here).
async fn push_core(
    state: &crate::AppState,
    q: &Query,
    core: u32,
    explicit: bool,
    out: &mut Vec<Candidate>,
    gaps: &mut Vec<String>,
) {
    let (occ_chain, ent_chain) = match crate::coretime_pair(&state.registry, &q.network) {
        Ok(pair) => pair,
        Err(e) => {
            if explicit {
                gaps.push(format!(
                    "'core {core}' cannot be resolved on network '{}': {e}",
                    q.network
                ));
            }
            return;
        }
    };

    // SCOPE IS CHECKED HERE AND NOT LEFT TO `resolve`, and that is a fix rather
    // than a preference. `resolve` filters CANDIDATES by `on <chain>` in one
    // place and never filters GAPS — correctly, since a gap is about the search
    // and not about a row — so a probe that pushes coverage describing a
    // candidate the filter is about to drop leaves a response whose
    // `not_covered` talks about a row that is not there. That is the defect
    // class this file's header is about, and `core 0 on ah` is the input that
    // reaches it. The single enforcement point is untouched; this refuses to
    // PRODUCE the mismatch rather than cleaning up after it.
    if let Some(want) = &q.chain {
        if want != &ent_chain.id {
            if explicit {
                gaps.push(format!(
                    "'core {core}' scoped to '{want}' can only be empty: entitlement is \
                     `pallet-broker`, which on this network lives on '{}' and nowhere else. \
                     Drop the scope or name that chain. ('{want}' may still have carried the \
                     core — what it did is OCCUPANCY, which is a different question and a \
                     different endpoint.)",
                    ent_chain.id
                ));
            }
            return;
        }
    }

    // THE PROBES KEEP THEIR `Result`. Every other probe in this file uses
    // `if let Ok(..)` and then makes no claim on failure; this one ends in a
    // sentence that says a core is NOT indexed, so swallowing the error with
    // `.ok()` would render a failed read as a confident absence — "we did not
    // look" as "there is nothing there", which this project has now caught four
    // times and never in search.
    //
    // `limit = 2` rather than 1: one row titles the candidate, and the SECOND
    // row is the only way to see that more than one assignment governs this core
    // at the same announcement. An interlaced or shared core is the state
    // `coretime_delta` withholds its entire waste figure for, and rendering it
    // as a confident single tenant would answer where the endpoint this
    // candidate points at refuses.
    let assignments = state
        .broker
        .assignments_for_core(&ent_chain.id, core, 2)
        .await;
    let rows: &[crate::EntitlementRow] = assignments.as_ref().map(|v| v.as_slice()).unwrap_or(&[]);
    let events = if rows.is_empty() {
        state.broker.events_for_core(&ent_chain.id, core, 1).await
    } else {
        Ok(Vec::new())
    };
    let read_failed = assignments.is_err() || events.is_err();

    let href = format!("/v1/coretime/{}/entitlement?core={core}", ent_chain.id);
    let mut shared = false;
    let pushed = if let Some(a) = rows.first() {
        // TWO independent signals that this core is not wholly one tenant's, and
        // they are different facts: a second row at the SAME `relay_block` means
        // the announcement itself assigned the core more than once (rows at
        // DIFFERENT relay blocks are just history and mean nothing of the kind),
        // while `parts` below a whole core means the region was interlaced. The
        // mask's PATTERN does not cross to the relay, so neither is recoverable
        // later; both are reported and neither is divided by.
        shared = rows.get(1).is_some_and(|b| b.relay_block == a.relay_block)
            || a.parts < crate::PARTS_WHOLE_CORE;
        out.push(Candidate {
            kind: "coretime_core",
            chain: Some(ent_chain.id.clone()),
            title: if shared {
                format!("core {core} — shared or interlaced (more than one entitlement)")
            } else {
                match (a.kind.as_str(), a.task_id) {
                    ("task", Some(t)) => format!("core {core} — assigned to task {t}"),
                    // "assigned idle" and not "idle": the chain really did
                    // announce Idle here, which is a different fact from NO
                    // assignment, and 0025 forbids this project to let those two
                    // words look alike.
                    ("idle", _) => format!("core {core} — assigned idle"),
                    ("pool", _) => format!("core {core} — pool (instantaneous market)"),
                    (k, _) => format!("core {core} — {k}"),
                }
            },
            // Moved, not cloned: the two arms are mutually exclusive and the
            // borrow checker knows it.
            href,
            why: WHY_CORE_ASSIGNED,
            id: serde_json::json!({
                "core_index": core,
                "chain": ent_chain.id,
                "assignment_kind": a.kind,
                "task_id": a.task_id,
                // `EntitlementRow.relay_block` under the name the delta endpoint
                // already gives it. It is the closest thing this chain has to a
                // SALE identifier, which is why `sale <n>` is refused and this
                // is what the refusal points at.
                "governing_relay_block": a.relay_block,
                "parts": a.parts,
            }),
            lineage: Some(serde_json::json!({
                "runtime_version": a.runtime_version,
                "mapper_version": a.mapper_version,
            })),
        });
        true
    } else if let Some(e) = events.as_ref().ok().and_then(|v| v.first()) {
        out.push(Candidate {
            kind: "coretime_core",
            chain: Some(ent_chain.id.clone()),
            title: format!("core {core} — {} (broker event)", e.variant),
            href,
            why: WHY_CORE_EVENT,
            id: serde_json::json!({
                "core_index": core,
                "chain": ent_chain.id,
                "block_height": e.block_height,
                "event_index": e.event_index,
            }),
            lineage: Some(serde_json::json!({
                "runtime_version": e.runtime_version,
                "mapper_version": e.mapper_version,
            })),
        });
        true
    } else {
        false
    };

    // THE DURABILITY CAVEAT IS GATED ON THE ASSIGNMENT ARM, because it cites a
    // field only that arm's candidate carries. Both reviewers found the
    // ungated version independently: an event-only candidate has no
    // `governing_relay_block` (deliberately — there is no assignment to take one
    // from), so the line would point at a field the row beside it does not have,
    // AND would say "this candidate identifies an entitlement" beside a `why`
    // saying our index holds no assignment for it. Two sentences in one response
    // asserting opposite things about one row: slice 14's fixed defect, verbatim.
    if !rows.is_empty() {
        gaps.push(format!(
            "A CORE INDEX IS A SLOT, NOT A TENANT. Measured at coretime block 4919882: para 3428 \
             renewed five cores and every index moved (35->43, 36->44, 37->45, 40->46, 41->47). \
             So this candidate identifies an entitlement only WITHIN one region — its \
             `governing_relay_block` says which — and following a chain across sale cycles by \
             core index silently follows a different tenant after every sale. To follow the \
             TENANT, ask /v1/coretime/{}/entitlement?task=<task id>",
            ent_chain.id
        ));
    }
    if shared {
        gaps.push(format!(
            "MORE THAN ONE ENTITLEMENT GOVERNS THIS CORE and only the newest assignment row is \
             shown here. Either the announcement assigned it more than once at one relay block, \
             or `parts` is below a whole core ({} = the whole mask), i.e. the region was \
             interlaced. `pallet-broker`'s tick converts an 80-bit `CoreMask` to the relay's \
             ratio as `count_ones() * 720`, so the bit COUNT crosses and the PATTERN does not — \
             two entitlements on one core are indistinguishable on the relay side forever. The \
             delta endpoint WITHHOLDS its waste figure entirely for such a core rather than \
             counting a fraction as a whole one, and nothing here divides by `parts` either. \
             /v1/coretime/{}/entitlement?core={core} shows every row",
            crate::PARTS_WHOLE_CORE,
            ent_chain.id
        ));
    }

    if explicit {
        gaps.push(format!(
            "THE OCCUPANCY HALF IS NOT PROBED HERE, and that is the endpoints' own rule rather \
             than a shortcut: what a core DID is a ratio, only meaningful over a stated window, \
             and /v1/coretime/{occ}/occupancy refuses to serve one without `from` and `to` \
             because defaulting to a window nobody chose puts it underneath a percentage \
             somebody quotes. A search box has no window to pass, so it resolves the \
             ENTITLEMENT — which needs none — and names the rest: \
             /v1/coretime/{occ}/occupancy?from=&to= for what the core did, and \
             /v1/coretime/{net}/delta?from=&to= for the difference between the two, which is \
             the number neither half gives alone",
            occ = occ_chain.id,
            net = q.network
        ));
        // GATED ON THE ASSIGNMENT ARM for the same reason the durability line
        // is, and the review caught it firing one arm too wide: this line says
        // "which is why the candidate says `assigned to`", and an event-only
        // candidate is titled "core 63 — Renewable (broker event)", which says
        // neither `assigned to` nor `sold to`. A coverage line describing a
        // property the row beside it does not have, again — so the gate is the
        // same expression, not a similar one.
        if !rows.is_empty() {
            gaps.push(format!(
                "WHETHER THIS CORE IS RESERVED OR MARKET-SIDE IS NOT READ HERE. Cores below \
                 `SaleInfo.first_core` are reserved system cores set by governance and never \
                 bought — measured `first_core = 11` on Polkadot, so cores 0-10 are all of that \
                 kind — which is why the candidate says `assigned to` and never `sold to`. That \
                 reading is dated on the CORETIME chain's own number line, moves every sale and \
                 cannot be aligned with a relay window at all, so it is omitted rather than \
                 quoted at a moment nobody chose. /v1/coretime/{}/delta reports it as \
                 `market.first_core` beside the height it was read at",
                q.network
            ));
        }
        if !pushed {
            // FIVE OUTCOMES, NOT ONE. An entitlement read that failed, a
            // configuration read that failed, a core above the declared count, a
            // core inside it with nothing indexed, and no configuration reading
            // at all are five different facts, and the single sentence this used
            // to be asserted one of them for all five. In particular the delta's
            // `unknown` clause is FALSE for a core above the declared count:
            // `coretime_delta` enumerates the universe from `num_cores` plus the
            // cores it actually saw, so such a core is never enumerated, never
            // renders `unknown` and withholds nothing on its account. That
            // clause now ships only where it is true.
            if read_failed {
                gaps.push(format!(
                    "the entitlement index on '{}' could not be read, so NOTHING here says \
                     whether core {core} is indexed — this is our failure and not an absence on \
                     chain",
                    ent_chain.id
                ));
            } else {
                // THE THIRD PROBE KEEPS ITS `Result` TOO. A first draft wrote
                // `.ok().flatten()` here — twelve lines below the comment
                // forbidding exactly that — so a failed config read fell into
                // the `None` arm and told an operator to run `sync-broker-config`
                // against an index that may be fully populated and merely
                // unreadable. Same defect class as the two probes above, one
                // probe over, inside the function that names it.
                let cfg = state.broker.latest_broker_config(&ent_chain.id).await;
                gaps.push(match cfg {
                    Err(e) => format!(
                        "nothing is indexed for core {core} on '{}', and the broker \
                         CONFIGURATION could not be read either ({e}) — so this cannot say \
                         whether that core index exists, and the absence above is our failure \
                         rather than a fact about the chain",
                        ent_chain.id
                    ),
                    Ok(Some(c)) if core >= c.core_count => format!(
                        "core {core} is at or above the declared core count of {} on '{}', read \
                         at coretime block {} — so on that reading there is no such core to \
                         resolve. The reading is DATED and the count moves (it is governance \
                         configuration, not a constant), so this says what was declared then \
                         and not what is declared now",
                        c.core_count, ent_chain.id, c.block_height
                    ),
                    Ok(Some(c)) => format!(
                        "core {core} is within the declared core count of {} on '{}' (read at \
                         coretime block {}) and NOTHING is indexed for it. THAT IS NOT 'the core \
                         was never sold': `Broker.CoreAssigned` fires only at SALE BOUNDARIES — \
                         the sale governing one measured 1,000-block window sat ~335,000 relay \
                         blocks before it — so an absence here means our index does not reach \
                         the sale that governs this core. The delta endpoint renders exactly \
                         this state as `unknown` and WITHHOLDS its waste figure while any such \
                         core exists, for the same reason",
                        c.core_count, ent_chain.id, c.block_height
                    ),
                    Ok(None) => format!(
                        "nothing is indexed for core {core} on '{}', AND no `pallet-broker` \
                         configuration reading is on record there — so this cannot even say \
                         whether that core index exists. Run `sync-broker-config` before reading \
                         the absence as a finding",
                        ent_chain.id
                    ),
                });
            }
        }
    }
}

fn gov_chains(state: &crate::AppState, network: &str) -> Vec<String> {
    state
        .registry
        .chains()
        .filter(|c| c.network == network)
        .map(|c| c.id.clone())
        .collect()
}

/// `why` is a parameter because the SAME block is reachable two ways — by height
/// and by hash — and the honest reason differs. What must NOT differ is the
/// lineage: a candidate found by hash is the same indexed row as one found by
/// height, so both carry spec_version/decoder_version/raw_location per
/// Invariant 3. Hand-building a second block candidate without lineage is
/// exactly how the header's promise and the exit criterion drifted apart once
/// already; there is one constructor so it cannot drift again.
async fn push_block(
    state: &crate::AppState,
    chain: &str,
    height: u64,
    why: &'static str,
    out: &mut Vec<Candidate>,
) {
    if let Ok(Some(b)) = state.blocks.get(chain, height).await {
        out.push(Candidate {
            kind: "block",
            chain: Some(chain.to_string()),
            title: format!("block {height}"),
            href: format!("/v1/blocks/{chain}/{height}"),
            why,
            id: serde_json::json!({ "height": height, "hash": b.hash }),
            lineage: Some(serde_json::json!({
                "runtime_version": b.lineage.runtime_version,
                "decoder_version": b.lineage.decoder_version,
                "raw_location": b.lineage.raw_location,
            })),
        });
    }
}

/// An extrinsic's lineage IS its containing block's: it was decoded against the
/// metadata at that block's spec_version, by that decoder version, from that
/// raw artifact. Reporting the coordinate without it would say where the
/// extrinsic is but not what decoded it, which is the one thing Invariant 3
/// exists to keep attached to a decoded row.
async fn push_extrinsic(
    state: &crate::AppState,
    chain: &str,
    height: u64,
    index: u32,
    out: &mut Vec<Candidate>,
) {
    let lineage = state
        .blocks
        .get(chain, height)
        .await
        .ok()
        .flatten()
        .map(|b| {
            serde_json::json!({
                "runtime_version": b.lineage.runtime_version,
                "decoder_version": b.lineage.decoder_version,
                "raw_location": b.lineage.raw_location,
            })
        });
    out.push(Candidate {
        kind: "extrinsic",
        chain: Some(chain.to_string()),
        title: format!("extrinsic {height}-{index}"),
        href: format!("/v1/blocks/{chain}/{height}"),
        why: "an extrinsic with this hash is indexed",
        id: serde_json::json!({ "height": height, "index": index }),
        lineage,
    });
}

async fn push_referendum(
    state: &crate::AppState,
    q: &Query,
    id: u64,
    out: &mut Vec<Candidate>,
    gaps: &mut Vec<String>,
) {
    // Deliberately via residency, so `ref 1930` never names a chain — the thing
    // that survived the Nov-2025 migration and the reason this is a moat rather
    // than a lookup (PRODUCT.md "Why the search box is a moat").
    // Classes come from the REGISTRY, never a code list — slice 4's finding
    // verbatim ("the fix must be registry data, not code"). Registering an
    // ambassador instance as a seed must make it searchable with no edit here.
    let mut classes: Vec<String> = vec![registry::DEFAULT_REFERENDA_CLASS.to_string()];
    for c in state.registry.referenda_classes() {
        if !classes.iter().any(|k| k == &c.class) {
            classes.push(c.class.clone());
        }
    }

    // ROADMAP's `ref 42 on hydration` scope axis, answered HONESTLY rather than
    // implemented. `resolve` filters every candidate by the named chain, so a
    // `ref` scoped to a chain that carries none of this network's governance
    // windows returns an empty list with nothing to say why — the silent-nothing
    // defect this project has caught at the FIELD level three times and never at
    // the QUERY level.
    //
    // ONE MESSAGE, NOT TWO, and that is a review finding rather than brevity. A
    // first draft branched on whether the chain declares the `governance` module
    // and told the "declares it" arm that the chain therefore "runs its OWN
    // referendum id space". Two things were wrong with it. The arm is
    // UNREACHABLE with the shipped seeds — every chain declaring the module also
    // carries a window — and a gate that never fires is a gate nobody has
    // checked. And the conclusion does not follow: a missing residency window is
    // equally consistent with an incomplete seed, which the registry cannot tell
    // apart from a chain with its own governance. So the message states the
    // registry FACT, reports the module bit as evidence, and names both causes
    // without picking one.
    //
    // NO CHAIN IS NAMED IN THIS CODE (Invariant 2): every chain in it is
    // formatted from the registry or from what the caller typed.
    if let Some(named) = &q.chain {
        let carries_a_window = crate::all_gov_windows(&state.registry, &q.network)
            .iter()
            .any(|w| &w.chain == named);
        if !carries_a_window {
            let declares = state
                .registry
                .chain(named)
                .is_some_and(|c| c.has_module("governance"));
            gaps.push(format!(
                "'{named}' carries none of the '{network}' network's governance residency \
                 windows — and it {module} the `governance` module — so this resolver has no \
                 referendum id space to search there and `ref {id} on {named}` can only ever be \
                 empty. TWO CAUSES REACH THAT STATE AND THE REGISTRY CANNOT TELL THEM APART, so \
                 nothing here picks: the chain may run its OWN governance, which needs the \
                 module enabled AND its class map scoped to the chain before anything can \
                 resolve to it (an INDEXING change, not a grammar one — its pallet names would \
                 otherwise merge into this network's class id space); or its residency seed may \
                 simply be incomplete. What did NOT happen is this quietly answering with the \
                 network's own referendum {id} instead",
                network = q.network,
                module = if declares {
                    "declares"
                } else {
                    "does not declare"
                },
            ));
        }
    }

    for w in crate::all_gov_windows(&state.registry, &q.network) {
        for class in &classes {
            let class = class.as_str();
            if let Ok(Some(r)) = state.gov.referendum(&w.chain, class, id).await {
                out.push(Candidate {
                    kind: "referendum",
                    chain: Some(w.chain.clone()),
                    title: format!("referendum {id} ({})", r.status),
                    href: format!("/v1/gov/{}/referenda/{id}?class={class}", q.network),
                    why: "a referendum with this id is indexed in this governance system",
                    id: serde_json::json!({ "referendum_id": id, "class": class }),
                    lineage: Some(serde_json::json!({ "status_height": r.status_height })),
                });
            }
        }
    }
    // M7: one referendum stitched across the migration is ONE object, not two
    // candidates. Everything else in this crate merges those windows; search
    // must not manufacture ambiguity where the rest of the API sees none.
    out.dedup_by(|a, b| a.kind == "referendum" && b.kind == "referendum" && a.id == b.id);
}

async fn push_account(
    state: &crate::AppState,
    q: &Query,
    addr: &str,
    out: &mut Vec<Candidate>,
    gaps: &mut Vec<String>,
) {
    // The adapter owns address parsing (Invariant 4) and is the only thing that
    // can verify a checksum — a shape test cannot.
    let Ok(bytes) = (state.parse_account)(addr) else {
        gaps.push(format!(
            "'{addr}' looks like an address but its checksum does not verify"
        ));
        return;
    };
    let mut labels = Vec::new();
    for chain in gov_chains(state, &q.network) {
        if let Ok(found) = state.labels.labels_for(&chain, &bytes).await {
            for l in found {
                labels.push(l.label);
            }
        }
    }
    let title = match labels.first() {
        Some(l) => format!("{l} — {addr}"),
        None => addr.to_string(),
    };
    out.push(Candidate {
        kind: "account",
        chain: None,
        title,
        href: format!("/v1/balances/{}/{addr}/history", q.network),
        why: "a valid address — accounts are network-wide, stitched across the migration",
        id: serde_json::json!({ "address": addr }),
        lineage: None,
    });
}

async fn push_symbol(state: &crate::AppState, symbol: &str, out: &mut Vec<Candidate>) {
    if let Ok(hits) = state.assets.assets_by_symbol(symbol).await {
        for (chain, a) in hits {
            out.push(Candidate {
                kind: "asset",
                chain: Some(chain.clone()),
                title: format!(
                    "{} ({}) — {}",
                    a.symbol.clone().unwrap_or_else(|| symbol.to_string()),
                    a.asset_key,
                    a.representation_kind
                ),
                href: format!("/v1/assets/{chain}"),
                why: "a representation on this chain carries this symbol",
                id: serde_json::json!({ "asset_key": a.asset_key, "chain": chain }),
                lineage: None,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn reg() -> Registry {
        let seeds = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../registry-seeds");
        Registry::load_from_dir(&seeds).expect("seeds")
    }

    /// The shared fixture with a purpose-built broker index swapped in. The
    /// unsizing coercion is spelled with a typed let, exactly as `test_state`
    /// spells it, so the two cannot drift.
    async fn state_with_broker(broker: crate::MemoryBrokerIndex) -> crate::AppState {
        let mut state = crate::tests::test_state().await;
        let broker: std::sync::Arc<dyn crate::BrokerIndex> = std::sync::Arc::new(broker);
        state.broker = broker;
        state
    }

    #[test]
    fn shapes_are_inferred_from_the_value_alone() {
        assert_eq!(infer_shape("1930"), Shape::Number(1930));
        assert_eq!(infer_shape("19,532,910"), Shape::Number(19_532_910));
        assert_eq!(
            infer_shape("19532910-4"),
            Shape::ExtrinsicId {
                height: 19_532_910,
                index: 4
            }
        );
        let h = format!("0x{}", "ab".repeat(32));
        assert_eq!(infer_shape(&h.to_uppercase()), Shape::Hash32(h.clone()));
        assert_eq!(
            infer_shape(&format!("0x{}", "cd".repeat(20))),
            Shape::Addr20(format!("0x{}", "cd".repeat(20)))
        );
        assert_eq!(
            infer_shape("13UVJyLnbVp9RBZYFwFGyDvVd1y27Tt8tkntv6Q7JVPhFsTB"),
            Shape::Ss58("13UVJyLnbVp9RBZYFwFGyDvVd1y27Tt8tkntv6Q7JVPhFsTB".into())
        );
        assert_eq!(infer_shape("USDT"), Shape::Symbol("USDT".into()));
        assert_eq!(
            infer_shape("Polkadot Treasury"),
            Shape::Text("Polkadot Treasury".into())
        );
    }

    #[test]
    fn a_hash_is_normalised_but_never_truncated_or_prefixed() {
        // the roadmap's hardest cost rule: partial hashes are a range scan and
        // must not be a shape at all
        let short = format!("0x{}", "ab".repeat(8));
        assert!(matches!(
            infer_shape(&short),
            Shape::Text(_) | Shape::Symbol(_)
        ));
    }

    #[test]
    fn codewords_and_the_chain_suffix_parse_together() {
        let r = reg();
        let q = parse("ref 1930", &r).unwrap();
        assert_eq!(q.network, "polkadot");
        assert_eq!(q.chain, None);
        assert_eq!(
            q.term,
            Term::Codeword {
                word: Codeword::Ref,
                arg: "1930".into()
            }
        );
        assert_eq!(q.reads_as, "ref 1930 on polkadot");

        // `on <alias>` resolves through the REGISTRY, not a code list
        for form in [
            "block 19532910 on ah",
            "block 19532910 @ah",
            "block 19532910 on assethub",
        ] {
            let q = parse(form, &r).unwrap();
            assert_eq!(q.chain.as_deref(), Some("polkadot-asset-hub"), "{form}");
        }
        // synonyms
        assert!(matches!(
            parse("tx 19532910-4", &r).unwrap().term,
            Term::Codeword {
                word: Codeword::Extrinsic,
                ..
            }
        ));
    }

    /// Slice 4's finding, applied to the grammar: a filter that is accepted and
    /// silently dropped is worse than one that is refused.
    #[test]
    fn an_unknown_chain_is_refused_rather_than_ignored() {
        let r = reg();
        let err = parse("block 100 on atlantis", &r).unwrap_err();
        assert!(err.message.contains("unknown chain"), "{}", err.message);
        assert!(!err.expected.is_empty(), "must say what WOULD work");
        assert!(err.expected.iter().any(|e| e == "ah"));
    }

    /// A codeword ships in the phase of the module that can answer it — and the
    /// refusal says WHEN, which is the difference between a roadmap and a typo.
    ///
    /// AND A PHASE THAT HAS SHIPPED IS NOT A REFUSAL, IT IS A LIE. This test
    /// used to assert `sale 42` says "Phase 3", which stayed true for exactly as
    /// long as coretime had not landed and then kept being served for three
    /// slices after it did. The shipped assertion is what made the lie
    /// invisible, which is why the fix has to move the test and not only the
    /// string: `sale` is refused for a reason that can never expire, and `core`
    /// is no longer refused at all.
    #[test]
    fn refused_codewords_say_why_and_sale_is_not_a_phase_away() {
        let r = reg();
        // `xcm` used to live here, and `core` used to live here. Both moved into
        // the grammar with the module that answers them, which is the rule
        // working rather than an exception to it.
        let err = parse("sel 0xa9059cbb", &r).unwrap_err();
        assert!(err.message.contains("Phase 5"), "{}", err.message);
        assert!(err.expected.iter().any(|e| e == "ref"));

        // `core` is IN the grammar now — the debt this slice pays.
        assert_eq!(
            parse("core 47", &r).unwrap().term,
            Term::Codeword {
                word: Codeword::Core,
                arg: "47".into()
            }
        );
        assert!(Codeword::ALL.contains(&Codeword::Core));
        assert!(
            Codeword::refused("core").is_none(),
            "core is answerable now"
        );

        // `sale` is still refused, and NOT because a phase is pending. Asserted
        // as a property — no phase claim of any kind — so that a later slice
        // cannot reintroduce one without this failing.
        let err = parse("sale 42", &r).unwrap_err();
        for phase in ["Phase 3", "Phase 4", "Phase 5", "lands in"] {
            assert!(
                !err.message.contains(phase),
                "`sale` must not promise a phase — the chain numbers no sales, ever: {}",
                err.message
            );
        }
        assert!(
            err.message.contains("numbers no sales"),
            "the refusal must say WHY: {}",
            err.message
        );
        assert!(
            err.message.contains("governing_relay_block"),
            "…and name the coordinate that DOES identify a sale: {}",
            err.message
        );
        // …and point at a codeword that exists, rather than at a phase.
        assert!(err.expected.iter().any(|e| e == "core"));
    }

    /// A bare NAME is a shape people paste without being taught to, so it must
    /// resolve from day one — that reservation is what lets identity search land
    /// later without a grammar change.
    #[test]
    fn a_bare_name_is_a_shape_not_an_error() {
        let r = reg();
        let q = parse("Polkadot Treasury", &r).unwrap();
        assert_eq!(q.term, Term::Shape(Shape::Text("Polkadot Treasury".into())));
        assert!(q.reads_as.contains("the name"));
    }

    /// THE PHASE 2 EXIT CRITERION (d), asserted end to end: `ref 1930`, a bare
    /// block number and a pasted SS58 all resolve from one input box, and an
    /// ambiguous 32-byte hash offers its candidates instead of guessing.
    #[tokio::test]
    async fn the_exit_criterion_resolves_from_one_box() {
        let state = crate::tests::test_state().await;

        // 1. `ref 1930` — and NO chain is named anywhere in the query
        let q = parse("ref 1500", &state.registry).unwrap();
        let r = resolve(&q, &state, "ref 1500").await;
        let refs: Vec<&Candidate> = r
            .candidates
            .iter()
            .filter(|c| c.kind == "referendum")
            .collect();
        assert!(
            !refs.is_empty(),
            "a referendum must resolve without a chain"
        );
        assert!(
            refs.iter().all(|c| c.chain.is_some()),
            "…and every candidate must say which chain it was FOUND on"
        );

        // 2. a bare block number
        let q = parse("19000001", &state.registry).unwrap();
        let r = resolve(&q, &state, "19000001").await;
        assert!(
            r.candidates.iter().any(|c| c.kind == "block"),
            "a bare number must offer the block reading: {:?}",
            r.candidates
        );

        // 3. a pasted SS58 — no codeword, no chain
        let treasury = "13UVJyLnbVp9RBZYFwFGyDvVd1y27Tt8tkntv6Q7JVPhFsTB";
        let q = parse(treasury, &state.registry).unwrap();
        let r = resolve(&q, &state, treasury).await;
        let acc = r
            .candidates
            .iter()
            .find(|c| c.kind == "account")
            .expect("account");
        assert!(
            acc.title.contains("Treasury"),
            "a labelled account must resolve labelled, not bare: {}",
            acc.title
        );

        // 4. the ambiguous hash — candidates, never a guess. This hash is BOTH
        // a stored preimage and a whitelisted call (slice 9), so it is a real
        // ambiguity rather than a contrived one.
        let hash = format!("0x{}", "7c".repeat(32));
        let q = parse(&hash, &state.registry).unwrap();
        let r = resolve(&q, &state, &hash).await;
        assert!(
            r.candidates.len() > 1,
            "an ambiguous hash must offer MORE THAN ONE reading, not pick one: {:?}",
            r.candidates
        );
        let kinds: Vec<&str> = r.candidates.iter().map(|c| c.kind).collect();
        assert!(kinds.contains(&"preimage"), "{kinds:?}");
        assert!(kinds.contains(&"whitelisted_call"), "{kinds:?}");

        // …and the block-hash probe works on a hash that IS a block's, proving
        // the fourth arm rather than assuming it
        let block = state
            .blocks
            .get("polkadot-asset-hub", 19_000_001)
            .await
            .unwrap()
            .unwrap();
        let q = parse(&block.hash, &state.registry).unwrap();
        let r = resolve(&q, &state, &block.hash).await;
        let by_hash = r
            .candidates
            .iter()
            .find(|c| c.kind == "block" && c.chain.as_deref() == Some("polkadot-asset-hub"))
            .unwrap_or_else(|| {
                panic!(
                    "a block hash must resolve to its block, naming the chain it was found on: {:?}",
                    r.candidates
                )
            });
        // THE ASSERTION THIS TEST WAS MISSING, and its absence is why a real
        // defect shipped: the comment below claimed "why + chain + lineage" while
        // only `why` was ever checked, so the bare-hash arm hand-built a block
        // candidate with lineage: None and the suite stayed green. A pasted hash
        // is the single most common search input, and it was the one reading that
        // could not say which decoder produced it.
        let lineage = by_hash
            .lineage
            .as_ref()
            .expect("a block found BY HASH must carry the same lineage as one found by height");
        for field in ["runtime_version", "decoder_version", "raw_location"] {
            assert!(
                lineage.get(field).is_some_and(|v| !v.is_null()),
                "lineage must carry {field} (Invariant 3): {lineage:?}"
            );
        }
        // …and it must be the SAME lineage, not merely present — the two grammar
        // forms reach one indexed row and must describe it identically.
        let q_h = parse("19000001", &state.registry).unwrap();
        let r_h = resolve(&q_h, &state, "19000001").await;
        let by_height = r_h
            .candidates
            .iter()
            .find(|c| c.kind == "block")
            .expect("block by height");
        assert_eq!(
            by_hash.lineage, by_height.lineage,
            "the same block reached by hash and by height must report one lineage"
        );
        // every candidate carries why + chain + lineage, for the machine
        assert!(r.candidates.iter().all(|c| !c.why.is_empty()));
        // The XCM gap line that used to be asserted here is GONE, replaced by a
        // probe: "an XCM topic is also a 32-byte hash and we cannot tell yet"
        // was true until the correlation slice, and leaving it in would be the
        // same one-line honesty regression the `xcm` codeword was.
        assert!(
            !r.not_covered.iter().any(|g| g.contains("XCM")),
            "XCM is probed now, not deferred: {:?}",
            r.not_covered
        );
    }

    /// A well-formed input that matches nothing indexed is a 200, never an
    /// error: "nothing indexed matches" and "this made no sense" are different
    /// answers and a caller must be able to tell them apart.
    ///
    /// It is NOT literally empty, and that is the honest outcome rather than a
    /// weakened assertion: any 32 bytes IS a valid AccountId32, so the account
    /// reading always survives. What must be absent is every reading that
    /// claims we indexed something.
    #[tokio::test]
    async fn an_unknown_but_well_formed_input_claims_nothing_it_did_not_find() {
        let state = crate::tests::test_state().await;
        let hash = format!("0x{}", "11".repeat(32));
        let q = parse(&hash, &state.registry).unwrap();
        let r = resolve(&q, &state, &hash).await;
        assert!(r.reads_as.contains("32-byte hash"));
        let kinds: Vec<&str> = r.candidates.iter().map(|c| c.kind).collect();
        for claimed in ["block", "extrinsic", "preimage", "whitelisted_call"] {
            assert!(!kinds.contains(&claimed), "nothing was indexed: {kinds:?}");
        }
    }

    /// A bare name resolves to the honest gap, not to nothing and not to an
    /// error — the reservation that lets identity search land later.
    #[tokio::test]
    async fn a_name_reports_the_missing_index_rather_than_failing() {
        let state = crate::tests::test_state().await;
        let q = parse("Polkadot Treasury", &state.registry).unwrap();
        let r = resolve(&q, &state, "Polkadot Treasury").await;
        assert!(r.candidates.is_empty());
        assert_eq!(r.not_covered.len(), 1);
        assert!(r.not_covered[0].contains("no name index yet"));
    }

    /// The `xcm` codeword was refused from Phase 2 slice 10 to Phase 3 slice 2
    /// with "XCM journeys land in Phase 3" — a refusal that stayed accurate
    /// until journeys shipped and then became a one-line honesty regression.
    /// This is that line being paid off, and the codeword doing more than the
    /// bare paste: it expands aliases, so a WIRE hash reaches the half the
    /// receiving chain reported under the TOPIC.
    #[tokio::test]
    async fn the_xcm_codeword_answers_and_a_wire_hash_reaches_the_other_chain() {
        let state = crate::tests::test_state().await;
        let wire = format!("0x{}", "77".repeat(32));

        let q = parse(&format!("xcm {wire}"), &state.registry).expect("no longer refused");
        assert_eq!(
            q.term,
            Term::Codeword {
                word: Codeword::Xcm,
                arg: wire.clone()
            }
        );
        let r = resolve(&q, &state, &wire).await;
        let xcm: Vec<&Candidate> = r
            .candidates
            .iter()
            .filter(|c| c.kind == "xcm_message")
            .collect();
        assert_eq!(
            xcm.len(),
            3,
            "both sending ids and the receiving half: {:?}",
            r.candidates
        );
        let other_chain = xcm
            .iter()
            .find(|c| c.chain.as_deref() == Some("hydration"))
            .expect("the alias expansion is the whole point");
        assert!(other_chain.why.contains("link"), "{}", other_chain.why);
        // Every candidate carries the lineage of the row it came from — a
        // journey has no single lineage, which is why this is one candidate per
        // OBSERVATION rather than one per journey.
        assert!(xcm.iter().all(|c| c.lineage.is_some() && c.chain.is_some()));
        assert!(xcm
            .iter()
            .all(|c| c.href == format!("/v1/xcm/journeys/{wire}")));

        // A bare paste is ONE probe and no expansion: it asks "what is this",
        // and the journey endpoint it points at does the expanding.
        let topic = format!("0x{}", "ee".repeat(32));
        let q = parse(&topic, &state.registry).unwrap();
        let r = resolve(&q, &state, &topic).await;
        let kinds: Vec<&str> = r.candidates.iter().map(|c| c.kind).collect();
        assert_eq!(
            kinds.iter().filter(|k| **k == "xcm_message").count(),
            2,
            "the two observations of THIS id, not of its alias: {kinds:?}"
        );
        // …and the account reading still survives, because any 32 bytes is one
        assert!(kinds.contains(&"account"));

        // A codeword with the wrong shape of argument is a gap, not a candidate.
        let q = parse("xcm 1930", &state.registry).unwrap();
        let r = resolve(&q, &state, "xcm 1930").await;
        assert!(r.candidates.iter().all(|c| c.kind != "xcm_message"));
        assert!(r.not_covered.iter().any(|g| g.contains("32-byte 0x hash")));
    }

    /// The coretime half of the grammar, and the caveat it must never drop.
    ///
    /// `core <n>` resolves the ENTITLEMENT, which needs no window, and states
    /// the occupancy half rather than inventing one — because both endpoints
    /// that serve a ratio refuse to default a window, and a search box that
    /// defaulted one would undermine that refusal from the outside.
    #[tokio::test]
    async fn the_core_codeword_answers_and_says_a_core_index_is_not_a_tenant() {
        let state = crate::tests::test_state().await;

        let q = parse("core 0", &state.registry).expect("no longer refused");
        assert_eq!(
            q.term,
            Term::Codeword {
                word: Codeword::Core,
                arg: "0".into()
            }
        );
        assert_eq!(q.reads_as, "core 0 on polkadot");
        let r = resolve(&q, &state, "core 0").await;

        let c = r
            .candidates
            .iter()
            .find(|c| c.kind == "coretime_core")
            .unwrap_or_else(|| panic!("core 0 must resolve: {:?}", r.candidates));
        // The chain comes from the REGISTRY (`coretime_pair`), and the caller
        // named none — the same Invariant-2 property `ref 1930` has.
        assert_eq!(c.chain.as_deref(), Some("polkadot-coretime"));
        assert_eq!(c.href, "/v1/coretime/polkadot-coretime/entitlement?core=0");
        // ASSIGNED, never SOLD: core 0 is below the fixture's `first_core = 2`,
        // i.e. a RESERVED system core that governance set and nobody bought. On
        // live Polkadot `first_core = 11`, so cores 0-10 are all of that kind and
        // "sold" would be wrong on every one of them.
        assert!(c.title.contains("assigned to task 2004"), "{}", c.title);
        assert!(
            !c.title.contains("sold"),
            "a reserved core was not sold: {}",
            c.title
        );
        // Lineage, on every candidate — slice 10 shipped two arms without it and
        // slice 14 shipped two coverage lines naming fields that did not exist.
        let lineage = c
            .lineage
            .as_ref()
            .expect("an entitlement row carries lineage");
        for field in ["runtime_version", "mapper_version"] {
            assert!(
                lineage.get(field).is_some_and(|v| !v.is_null()),
                "lineage must carry {field}: {lineage:?}"
            );
        }
        // The coordinate the `sale` refusal points at, present on the candidate
        // it points at — so the refusal is navigable rather than merely correct.
        assert_eq!(
            c.id.get("governing_relay_block").and_then(|v| v.as_u64()),
            Some(80)
        );
        // The caveat rides on the CANDIDATE, not only in coverage, because a
        // dropdown renders candidates and throws coverage away.
        assert!(c.why.contains("SLOT"), "{}", c.why);

        let gaps = r.not_covered.join(" | ");
        assert!(
            gaps.contains("35->43"),
            "the measured renewal evidence: {gaps}"
        );
        assert!(gaps.contains("occupancy?from=&to="), "{gaps}");
        assert!(gaps.contains("delta?from=&to="), "{gaps}");
        // The reserved-vs-market split is NOT read here, and the omission is
        // stated rather than left for a reader to assume the opposite.
        assert!(gaps.contains("first_core"), "{gaps}");

        // EVERY FIELD A COVERAGE LINE CITES MUST EXIST ON THE CANDIDATE IT
        // DESCRIBES. Slice 14 shipped two `not_covered` lines pointing at fields
        // that did not exist and had to add a walk like this one; the first draft
        // of THIS slice did it again, citing `governing_relay_block` on an arm
        // whose candidate deliberately has no such field.
        for field in ["governing_relay_block", "core_index", "parts"] {
            if gaps.contains(field) {
                assert!(
                    c.id.get(field).is_some(),
                    "coverage cites `{field}` but the candidate's id is {:?}",
                    c.id
                );
            }
        }

        // A core with nothing indexed fabricates NOTHING — the `bounty 999999`
        // defect. And the reason it gives is the one that is TRUE of this input:
        // 4242 is far above the fixture's declared core count of 10, so the
        // honest statement is "no such core was declared", NOT the sale-boundary
        // story (which is about a core that DOES exist and whose governing sale
        // we did not index). The delta's `unknown` clause must not appear either:
        // `coretime_delta` enumerates its universe from `num_cores` plus the
        // cores it saw, so a core above the count is never enumerated, never
        // renders `unknown` and withholds nothing.
        let q = parse("core 4242", &state.registry).unwrap();
        let r = resolve(&q, &state, "core 4242").await;
        assert!(
            r.candidates.iter().all(|c| c.kind != "coretime_core"),
            "no probe hit, so no candidate: {:?}",
            r.candidates
        );
        let gaps = r.not_covered.join(" | ");
        assert!(
            gaps.contains("at or above the declared core count of 10"),
            "{gaps}"
        );
        assert!(
            !gaps.contains("SALE BOUNDARIES") && !gaps.contains("WITHHOLDS"),
            "a core that was never declared is not a core whose sale we missed: {gaps}"
        );

        // A codeword with the wrong shape of argument is a gap, not a candidate.
        let q = parse("core notanumber", &state.registry).unwrap();
        let r = resolve(&q, &state, "core notanumber").await;
        assert!(r.candidates.iter().all(|c| c.kind != "coretime_core"));
        assert!(r
            .not_covered
            .iter()
            .any(|g| g.contains("core index is a small")));
    }

    /// Each module makes a bare number MORE ambiguous, and the probe is what
    /// keeps that honest: a hit is a candidate, a miss is silence. The silence
    /// is the assertion here — a coretime caveat on every bare number would be
    /// noise on the commonest shape in the grammar.
    #[tokio::test]
    async fn a_bare_number_offers_the_core_reading_only_when_it_is_probed() {
        let state = crate::tests::test_state().await;

        let q = parse("0", &state.registry).unwrap();
        let r = resolve(&q, &state, "0").await;
        let kinds: Vec<&str> = r.candidates.iter().map(|c| c.kind).collect();
        assert!(kinds.contains(&"coretime_core"), "{kinds:?}");
        // The candidate's caveat travels with it even unasked-for…
        assert!(r
            .not_covered
            .iter()
            .any(|g| g.contains("SLOT, NOT A TENANT")));
        // …but the endpoint tour does NOT, because the caller asked "what is 0",
        // not "tell me about core 0".
        assert!(
            !r.not_covered
                .iter()
                .any(|g| g.contains("occupancy?from=&to=")),
            "an unasked-for probe must not lecture: {:?}",
            r.not_covered
        );

        // The silent miss. NOTE the assertion above it is what keeps this one
        // honest: `not_covered` is empty for `4242` whatever happens, so this
        // clause alone would still pass with `push_core` deleted from the
        // bare-number path entirely. The `parse("0")` half is the wiring check.
        let q = parse("4242", &state.registry).unwrap();
        let r = resolve(&q, &state, "4242").await;
        assert!(r.candidates.iter().all(|c| c.kind != "coretime_core"));
        assert!(
            !r.not_covered.iter().any(|g| g.contains("core")),
            "a miss on an unasked-for probe is silent: {:?}",
            r.not_covered
        );
    }

    /// The five things a miss can mean, and they are five different facts. A
    /// first draft asserted ONE of them for all five — including for a core index
    /// above the declared count, where the delta's `unknown` clause it invoked is
    /// flatly false, because `coretime_delta` never enumerates such a core and
    /// therefore withholds nothing on its account.
    #[tokio::test]
    async fn a_core_that_misses_says_which_of_the_five_things_it_means() {
        // (a) declared, and nothing indexed for it: the sale-boundary story, and
        //     the ONLY case where the delta's `unknown`/withhold clause is true.
        let broker = crate::MemoryBrokerIndex::new();
        broker.insert_config(
            "polkadot-coretime",
            crate::BrokerConfigRow {
                block_height: 4_927_655,
                core_count: 100,
                first_core: Some(11),
                runtime_version: 2_003_002,
            },
        );
        let state = state_with_broker(broker).await;
        let q = parse("core 63", &state.registry).unwrap();
        let gaps = resolve(&q, &state, "core 63").await.not_covered.join(" | ");
        assert!(
            gaps.contains("within the declared core count of 100"),
            "{gaps}"
        );
        assert!(gaps.contains("SALE BOUNDARIES"), "{gaps}");
        assert!(gaps.contains("WITHHOLDS its waste figure"), "{gaps}");

        // (b) above the declared count: no such core, and the reading is dated.
        let q = parse("core 4242", &state.registry).unwrap();
        let gaps = resolve(&q, &state, "core 4242")
            .await
            .not_covered
            .join(" | ");
        assert!(
            gaps.contains("at or above the declared core count of 100"),
            "{gaps}"
        );
        assert!(
            gaps.contains("DATED"),
            "the count is configuration, not a constant: {gaps}"
        );
        assert!(!gaps.contains("SALE BOUNDARIES"), "{gaps}");

        // (c) no configuration reading at all: we cannot say whether the core
        //     even exists, and saying so is the whole point — this is the arm
        //     that must not read as "there is no such core".
        let state = state_with_broker(crate::MemoryBrokerIndex::new()).await;
        let q = parse("core 63", &state.registry).unwrap();
        let gaps = resolve(&q, &state, "core 63").await.not_covered.join(" | ");
        assert!(gaps.contains("sync-broker-config"), "{gaps}");
        assert!(
            !gaps.contains("declared core count") && !gaps.contains("SALE BOUNDARIES"),
            "with no reading, neither claim is available: {gaps}"
        );

        // (d) THE READ ITSELF FAILED. `MemoryBrokerIndex` cannot produce this —
        // it errors only on a poisoned lock — so the arm needs a stub, and a
        // gate that never fires is a gate nobody has checked. This is the whole
        // point of keeping the `Result`: a failed read must never render as a
        // confident absence, and must never send an operator to run
        // `sync-broker-config` against an index that is merely unreadable.
        let mut state = crate::tests::test_state().await;
        state.broker = std::sync::Arc::new(UnreadableBroker { config_only: false });
        let q = parse("core 63", &state.registry).unwrap();
        let gaps = resolve(&q, &state, "core 63").await.not_covered.join(" | ");
        assert!(gaps.contains("entitlement index on"), "{gaps}");
        assert!(
            gaps.contains("our failure and not an absence on chain"),
            "{gaps}"
        );
        assert!(
            !gaps.contains("sync-broker-config")
                && !gaps.contains("declared core count")
                && !gaps.contains("SALE BOUNDARIES"),
            "a failed read supports NONE of the absence claims: {gaps}"
        );

        // (e) the entitlement reads SUCCEEDED and were empty, but the
        // CONFIGURATION read failed. Its own arm, and it needs its own stub —
        // the previous one short-circuits before the config is ever consulted,
        // so without this the config `Err` arm would be unreachable and
        // therefore unchecked, which is the same objection as (d).
        let mut state = crate::tests::test_state().await;
        state.broker = std::sync::Arc::new(UnreadableBroker { config_only: true });
        let q = parse("core 63", &state.registry).unwrap();
        let gaps = resolve(&q, &state, "core 63").await.not_covered.join(" | ");
        assert!(gaps.contains("CONFIGURATION could not be read"), "{gaps}");
        assert!(
            !gaps.contains("sync-broker-config") && !gaps.contains("declared core count"),
            "an unreadable configuration supports neither claim: {gaps}"
        );
    }

    /// A `BrokerIndex` whose reads fail, so the two failure arms of `push_core`
    /// can be exercised at all. `MemoryBrokerIndex` returns `Err` only on a
    /// poisoned lock, which a test cannot arrange without poisoning one.
    ///
    /// `config_only` exists because the two arms are reached by DIFFERENT
    /// failures: an entitlement read that fails short-circuits before the
    /// configuration is consulted, so a stub that fails everything can only ever
    /// reach the first one.
    struct UnreadableBroker {
        config_only: bool,
    }

    impl UnreadableBroker {
        fn err<T>(&self) -> Result<Vec<T>, crate::IndexError> {
            if self.config_only {
                Ok(Vec::new())
            } else {
                Err(crate::IndexError("index unreadable".into()))
            }
        }
    }

    #[async_trait::async_trait]
    impl crate::BrokerIndex for UnreadableBroker {
        async fn entitlement_at(
            &self,
            _chain_id: &str,
            _relay_height: u64,
        ) -> Result<Vec<crate::EntitlementRow>, crate::IndexError> {
            self.err()
        }
        /// ALWAYS fails, on both settings — it is the read this stub exists for.
        async fn latest_broker_config(
            &self,
            _chain_id: &str,
        ) -> Result<Option<crate::BrokerConfigRow>, crate::IndexError> {
            Err(crate::IndexError("index unreadable".into()))
        }
        async fn events_for_core(
            &self,
            _chain_id: &str,
            _core_index: u32,
            _limit: u32,
        ) -> Result<Vec<crate::BrokerEventRow>, crate::IndexError> {
            self.err()
        }
        async fn events_for_task(
            &self,
            _chain_id: &str,
            _task_id: u32,
            _limit: u32,
        ) -> Result<Vec<crate::BrokerEventRow>, crate::IndexError> {
            self.err()
        }
        async fn assignments_for_core(
            &self,
            _chain_id: &str,
            _core_index: u32,
            _limit: u32,
        ) -> Result<Vec<crate::EntitlementRow>, crate::IndexError> {
            self.err()
        }
        async fn assignments_for_task(
            &self,
            _chain_id: &str,
            _task_id: u32,
            _limit: u32,
        ) -> Result<Vec<crate::EntitlementRow>, crate::IndexError> {
            self.err()
        }
    }

    /// A core carrying more than one entitlement is not one tenant's, and the
    /// candidate must not render it as though it were. The endpoint this
    /// candidate points at WITHHOLDS its whole waste figure for such a core, so a
    /// confident single-tenant title here would answer where the endpoint
    /// refuses. Interlacing has never been observed on this chain — measured
    /// three independent ways — which is exactly the standing this project states
    /// rather than omits.
    #[tokio::test]
    async fn a_core_with_more_than_one_entitlement_is_not_rendered_as_a_whole_one() {
        let broker = crate::MemoryBrokerIndex::new();
        let row = |ai: u32, task: u32, parts: u32| crate::EntitlementRow {
            core_index: 47,
            assignment_index: ai,
            relay_block: 32_278_800,
            kind: "task".into(),
            task_id: Some(task),
            parts,
            runtime_version: 2_003_002,
            mapper_version: 1,
        };
        // TWO assignments at ONE relay block — the announcement itself split the
        // core. (Two rows at DIFFERENT relay blocks are ordinary history and must
        // NOT trip this; the sibling assertion below pins that.)
        broker.insert_assignment("polkadot-coretime", 7, 0, row(0, 3428, 28_800));
        broker.insert_assignment("polkadot-coretime", 7, 1, row(1, 2034, 28_800));
        let state = state_with_broker(broker).await;

        let q = parse("core 47", &state.registry).unwrap();
        let r = resolve(&q, &state, "core 47").await;
        let c = r
            .candidates
            .iter()
            .find(|c| c.kind == "coretime_core")
            .expect("candidate");
        assert!(
            c.title.contains("shared or interlaced"),
            "one of two entitlements must not read as the whole core: {}",
            c.title
        );
        let gaps = r.not_covered.join(" | ");
        assert!(gaps.contains("MORE THAN ONE ENTITLEMENT"), "{gaps}");
        assert!(
            gaps.contains("count_ones"),
            "the mask's pattern does not cross: {gaps}"
        );

        // CONTROL: two assignments at DIFFERENT relay blocks are successive
        // sales, not a split core, and must render as an ordinary single tenant.
        let broker = crate::MemoryBrokerIndex::new();
        let mut older = row(0, 3428, crate::PARTS_WHOLE_CORE);
        older.relay_block = 31_875_600;
        let mut newer = row(0, 2034, crate::PARTS_WHOLE_CORE);
        newer.relay_block = 32_278_800;
        broker.insert_assignment("polkadot-coretime", 5, 0, older);
        broker.insert_assignment("polkadot-coretime", 7, 0, newer);
        let state = state_with_broker(broker).await;
        let q = parse("core 47", &state.registry).unwrap();
        let r = resolve(&q, &state, "core 47").await;
        let c = r
            .candidates
            .iter()
            .find(|c| c.kind == "coretime_core")
            .expect("candidate");
        assert!(
            c.title.contains("assigned to task 2034"),
            "the NEWEST sale wins and nothing is shared: {}",
            c.title
        );
        assert!(
            !r.not_covered
                .iter()
                .any(|g| g.contains("MORE THAN ONE ENTITLEMENT")),
            "history is not interlacing: {:?}",
            r.not_covered
        );
    }

    /// A `pallet-broker` event naming a core with NO assignment on record is a
    /// different fact from an assignment, and the candidate says which. The
    /// distinction is migration 0025's: an absent assignment is `unknown`, never
    /// `idle`, and a candidate that blurred them would put the delta endpoint's
    /// central refusal back on the table one surface over.
    #[tokio::test]
    async fn a_core_known_only_from_an_event_says_the_sale_is_outside_our_index() {
        let broker = crate::MemoryBrokerIndex::new();
        broker.insert_event(
            "polkadot-coretime",
            crate::BrokerEventRow {
                block_height: 11,
                event_index: 0,
                variant: "Renewable".into(),
                core_index: Some(63),
                task_id: None,
                data: serde_json::json!({ "core": 63 }),
                runtime_version: 2_003_002,
                mapper_version: 1,
            },
        );
        let state = state_with_broker(broker).await;

        let q = parse("core 63", &state.registry).unwrap();
        let r = resolve(&q, &state, "core 63").await;
        let c = r
            .candidates
            .iter()
            .find(|c| c.kind == "coretime_core")
            .unwrap_or_else(|| panic!("the event is evidence: {:?}", r.candidates));
        assert!(c.title.contains("Renewable"), "{}", c.title);
        assert!(
            c.why.contains("no assignment") && c.why.contains("NOT that the core was unsold"),
            "an absent assignment is unknown, never idle: {}",
            c.why
        );
        // Lineage from the EVENT row, not a hardcoded literal: assert the values
        // rather than the presence, or this passes by construction.
        let lineage = c.lineage.as_ref().expect("the event row carries lineage");
        assert_eq!(
            lineage.get("runtime_version").and_then(|v| v.as_u64()),
            Some(2_003_002)
        );
        assert_eq!(
            lineage.get("mapper_version").and_then(|v| v.as_u64()),
            Some(1)
        );
        assert_eq!(c.chain.as_deref(), Some("polkadot-coretime"));
        // No assignment, so no sale coordinate to offer — absent rather than a
        // zero pretending to be one.
        assert!(c.id.get("governing_relay_block").is_none(), "{:?}", c.id);
        // …AND NO COVERAGE LINE MAY CITE IT EITHER. This is the pointer walk in
        // its sharpest form: the first draft pushed the slot/tenant caveat on
        // both arms, so this response carried a line naming a field the row
        // beside it does not have — while that row's own `why` said we hold no
        // assignment at all. Two sentences, one response, opposite claims.
        assert!(
            !r.not_covered
                .iter()
                .any(|g| g.contains("governing_relay_block")),
            "coverage must not cite a field this arm's candidate has no way to carry: {:?}",
            r.not_covered
        );
        assert!(
            !r.not_covered
                .iter()
                .any(|g| g.contains("this candidate identifies an entitlement")),
            "we hold no assignment for this core: {:?}",
            r.not_covered
        );
    }

    /// The coretime chain's aliases were seeded in slice 13 and
    /// `Registry::chain_by_alias` is generic, so `on ct` resolves with NO code in
    /// this file. This VERIFIES that rather than claiming it as new work — and
    /// it also pins the scope enforcement, which lives in exactly one place.
    #[tokio::test]
    async fn coretime_chain_aliases_already_resolve_and_the_scope_is_enforced_once() {
        let state = crate::tests::test_state().await;
        for form in [
            "core 0 on ct",
            "core 0 on coretime",
            "core 0 on broker",
            "core 0 @ct",
            "core 0 on Polkadot Coretime",
            "core 0 on polkadot-coretime",
        ] {
            let q = parse(form, &state.registry).unwrap_or_else(|e| panic!("{form}: {e:?}"));
            assert_eq!(q.chain.as_deref(), Some("polkadot-coretime"), "{form}");
            let r = resolve(&q, &state, form).await;
            assert!(
                r.candidates.iter().any(|c| c.kind == "coretime_core"),
                "{form}: {:?}",
                r.candidates
            );
        }
        // …and scoping to a chain the entitlement does not live on returns
        // nothing AND SAYS SO. `resolve`'s single filter drops candidates and —
        // correctly — never touches gaps, so a probe that pushed coverage before
        // the filter ran would leave a response whose `not_covered` describes a
        // row that is not there. `push_core` therefore checks the scope itself
        // rather than producing the mismatch and hoping.
        let q = parse("core 0 on ah", &state.registry).unwrap();
        let r = resolve(&q, &state, "core 0 on ah").await;
        assert!(
            r.candidates.iter().all(|c| c.kind != "coretime_core"),
            "entitlement lives on the coretime chain, not on Asset Hub: {:?}",
            r.candidates
        );
        assert!(
            !r.not_covered.iter().any(|g| g.contains("this candidate")),
            "no candidate survived, so nothing may describe one: {:?}",
            r.not_covered
        );
        assert!(
            r.not_covered
                .iter()
                .any(|g| g.contains("polkadot-asset-hub") && g.contains("polkadot-coretime")),
            "an empty scoped result must name the chain that WOULD answer: {:?}",
            r.not_covered
        );
    }

    /// ROADMAP's `ref 42 on hydration` scope axis, answered honestly rather than
    /// implemented. `resolve` filters by the named chain, so scoping a `ref` to a
    /// chain that carries none of the network's governance windows returns an
    /// empty list — and an empty list with no explanation is the silent-nothing
    /// defect this project has caught at the field level three times and never at
    /// the query level.
    #[tokio::test]
    async fn a_referendum_scoped_to_a_chain_outside_the_governance_windows_says_so() {
        let state = crate::tests::test_state().await;

        let q = parse("ref 42 on hydration", &state.registry).unwrap();
        assert_eq!(q.chain.as_deref(), Some("hydration"));
        let r = resolve(&q, &state, "ref 42 on hydration").await;
        assert!(r.candidates.iter().all(|c| c.kind != "referendum"));
        let gaps = r.not_covered.join(" | ");
        // The chain, and the reason, and the module bit as EVIDENCE rather than
        // as a conclusion.
        assert!(gaps.contains("hydration"), "{gaps}");
        assert!(gaps.contains("governance residency windows"), "{gaps}");
        assert!(
            gaps.contains("does not declare"),
            "the module bit is reported: {gaps}"
        );
        // AND IT MUST NOT PICK A CAUSE. A first draft branched and told one arm
        // the chain "runs its OWN referendum id space" — a conclusion the
        // registry cannot support, since a missing residency window is equally
        // consistent with an incomplete seed. Both causes are named; neither is
        // asserted. Pinned so a later slice cannot quietly re-add the inference.
        assert!(
            gaps.contains("TWO CAUSES") && gaps.contains("residency seed may"),
            "a missing window has two causes and the registry cannot tell them apart: {gaps}"
        );
        // …and the thing that did NOT happen is stated, because answering with
        // the network's own referendum 42 is the failure this line exists for.
        assert!(gaps.contains("referendum 42 instead"), "{gaps}");

        // NEGATIVE CONTROL, taken from the registry rather than written down: a
        // chain that IS in the network's governance windows gets no such line.
        let windows = crate::all_gov_windows(&state.registry, "polkadot");
        let gov_chain = windows.first().expect("a governance window").chain.clone();
        let raw = format!("ref 1500 on {gov_chain}");
        let q = parse(&raw, &state.registry).unwrap();
        let r = resolve(&q, &state, &raw).await;
        assert!(
            !r.not_covered.iter().any(|g| g.contains("governance")),
            "{gov_chain} carries this network's governance: {:?}",
            r.not_covered
        );
    }

    #[test]
    fn a_network_prefix_selects_the_network_and_a_bare_codeword_asks_for_its_argument() {
        let r = reg();
        let err = parse("ref", &r).unwrap_err();
        assert!(err.message.contains("needs an argument"), "{}", err.message);
        // `dot` is the ticker people type for the polkadot network
        let q = parse("dot ref 1930", &r).unwrap();
        assert_eq!(q.network, "polkadot");
        assert_eq!(
            q.term,
            Term::Codeword {
                word: Codeword::Ref,
                arg: "1930".into()
            }
        );
    }
}
