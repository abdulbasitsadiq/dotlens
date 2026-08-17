//! `api::search` — the omnibox as a first-class API (Phase 2, slice 10).
//!
//! Grammar: `[network] <codeword|value> [on <chain>]`. Everything except the
//! value is optional, and the response always carries a `reads_as` readout so
//! the caller sees how their input was understood — a wrong guess the user can
//! SEE is a different failure from a wrong guess they cannot.
//!
//! THREE RULES SHAPE EVERY DECISION BELOW.
//!
//! 1. **Ambiguity is DATA, never a guess.** A 32-byte hash is four honest
//!    candidates; a bare number is three. The response lists them with the
//!    reason each was offered. Guessing looks decisive and is occasionally
//!    catastrophic — the whole point of an explorer is that you can trust what
//!    it tells you.
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
//! WHAT v1 DELIBERATELY DOES NOT DO. Codewords ship in the phase of the module
//! that can answer them, so `xcm`, `sel` and `contract` are NOT accepted —
//! typing one gets a parse error naming the valid set, not a promise. But a
//! bare NAME is a SHAPE, not vocabulary: people paste display names without
//! being taught to, so `TEXT` resolves from day one to an honest "no name index
//! yet". That reservation is why identity search can land with the People-chain
//! module instead of needing a grammar change.
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
        }
    }

    /// Every codeword v1 serves, for the grammar reference and for the error
    /// message an unknown one produces.
    pub const ALL: [Codeword; 12] = [
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
    ];

    /// Codewords reserved for modules that do not exist yet. Named explicitly
    /// so the parse error can say "Phase 3" rather than "unknown", which is the
    /// difference between a roadmap and a typo.
    pub fn planned(word: &str) -> Option<&'static str> {
        Some(match word {
            "xcm" => "XCM journeys land in Phase 3",
            "sale" | "core" => "coretime lands in Phase 3",
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
                let first = tail.split_whitespace().next().unwrap_or("").to_ascii_lowercase();
                let looks_like_a_query = Codeword::parse(&first).is_some()
                    || Codeword::planned(&first).is_some()
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
                None => match Codeword::planned(&lower) {
                    Some(when) => {
                        return Err(ParseError {
                            message: format!("'{lower}' is not in the v1 grammar — {when}"),
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
            if let Some(when) = Codeword::planned(&lower) {
                return Err(ParseError {
                    message: format!("'{lower}' is not in the v1 grammar — {when}"),
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
    if t.len() <= 12 && !t.contains(char::is_whitespace) && t.chars().all(|c| c.is_ascii_alphanumeric())
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
fn known_networks(registry: &Registry) -> Vec<String> {
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
    /// asset | track | spend | bounty | para
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
/// — verified per key in migration 0013, which had to CREATE two of them.
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
                    push_block(state, &chain, height, "a block with this hash is indexed", out)
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
            // Named so the caller knows the candidate set is bounded by what we
            // index, not by what a hash can be.
            gaps.push(
                "an XCM message topic is also a 32-byte hash; XCM lands in Phase 3, so a topic \
                 that matches nothing above is not distinguishable from an unknown hash yet"
                    .into(),
            );
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
            push_referendum(state, q, *n, out).await;
            if let Some(c) = state
                .registry
                .chains()
                .find(|c| c.para_id == Some(*n as u32) && c.network == q.network)
            {
                out.push(Candidate {
                    kind: "para",
                    chain: Some(c.id.clone()),
                    title: format!("{} (para {n})", c.name),
                    href: format!("/v1/chains"),
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
            Some(n) => push_referendum(state, q, n, out).await,
            None => gaps.push(format!("'ref {arg}' — a referendum id is a number")),
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
                _ => gaps.push(format!("'block {arg}' — expected a height or a 0x block hash")),
            },
        },
        Codeword::Acc => push_account(state, q, arg, out, gaps).await,
        Codeword::Preimage | Codeword::Whitelist => match infer_shape(arg) {
            Shape::Hash32(h) => resolve_shape(&Shape::Hash32(h), q, state, out, gaps).await,
            _ => gaps.push(format!("'{} {arg}' — expected a 32-byte 0x hash", word.as_str())),
        },
        Codeword::Asset => {
            push_symbol(state, arg, out).await;
        }
        Codeword::Extrinsic => resolve_shape(&infer_shape(arg), q, state, out, gaps).await,
        Codeword::Para => {
            if let Some(c) = state
                .registry
                .chain_by_alias(arg)
                .or_else(|| state.registry.chains().find(|c| Some(arg) == c.para_id.map(|p| p.to_string()).as_deref()))
            {
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
    let lineage = state.blocks.get(chain, height).await.ok().flatten().map(|b| {
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
        gaps.push(format!("'{addr}' looks like an address but its checksum does not verify"));
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

    #[test]
    fn shapes_are_inferred_from_the_value_alone() {
        assert_eq!(infer_shape("1930"), Shape::Number(1930));
        assert_eq!(infer_shape("19,532,910"), Shape::Number(19_532_910));
        assert_eq!(
            infer_shape("19532910-4"),
            Shape::ExtrinsicId { height: 19_532_910, index: 4 }
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
        assert!(matches!(infer_shape(&short), Shape::Text(_) | Shape::Symbol(_)));
    }

    #[test]
    fn codewords_and_the_chain_suffix_parse_together() {
        let r = reg();
        let q = parse("ref 1930", &r).unwrap();
        assert_eq!(q.network, "polkadot");
        assert_eq!(q.chain, None);
        assert_eq!(
            q.term,
            Term::Codeword { word: Codeword::Ref, arg: "1930".into() }
        );
        assert_eq!(q.reads_as, "ref 1930 on polkadot");

        // `on <alias>` resolves through the REGISTRY, not a code list
        for form in ["block 19532910 on ah", "block 19532910 @ah", "block 19532910 on assethub"] {
            let q = parse(form, &r).unwrap();
            assert_eq!(q.chain.as_deref(), Some("polkadot-asset-hub"), "{form}");
        }
        // synonyms
        assert!(matches!(
            parse("tx 19532910-4", &r).unwrap().term,
            Term::Codeword { word: Codeword::Extrinsic, .. }
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
    #[test]
    fn planned_codewords_are_refused_with_their_phase() {
        let r = reg();
        for (word, when) in [("xcm 0xabc", "Phase 3"), ("sel 0xa9059cbb", "Phase 5")] {
            let err = parse(word, &r).unwrap_err();
            assert!(err.message.contains(when), "{}: {}", word, err.message);
            assert!(err.expected.iter().any(|e| e == "ref"));
        }
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
        let refs: Vec<&Candidate> = r.candidates.iter().filter(|c| c.kind == "referendum").collect();
        assert!(!refs.is_empty(), "a referendum must resolve without a chain");
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
        let acc = r.candidates.iter().find(|c| c.kind == "account").expect("account");
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
        let block = state.blocks.get("polkadot-asset-hub", 19_000_001).await.unwrap().unwrap();
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
        let by_height = r_h.candidates.iter().find(|c| c.kind == "block").expect("block by height");
        assert_eq!(
            by_hash.lineage, by_height.lineage,
            "the same block reached by hash and by height must report one lineage"
        );
        // every candidate carries why + chain + lineage, for the machine
        assert!(r.candidates.iter().all(|c| !c.why.is_empty()));
        // and the bounded candidate set states what it cannot yet distinguish
        assert!(r.not_covered.iter().any(|g| g.contains("XCM")));
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

    #[test]
    fn a_network_prefix_selects_the_network_and_a_bare_codeword_asks_for_its_argument() {
        let r = reg();
        let err = parse("ref", &r).unwrap_err();
        assert!(err.message.contains("needs an argument"), "{}", err.message);
        // `dot` is the ticker people type for the polkadot network
        let q = parse("dot ref 1930", &r).unwrap();
        assert_eq!(q.network, "polkadot");
        assert_eq!(q.term, Term::Codeword { word: Codeword::Ref, arg: "1930".into() });
    }
}
