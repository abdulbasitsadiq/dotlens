//! The HTML surface — Phase 3.5's first rendered page.
//!
//! ROADMAP: *"After Phase 3 the API answers governance, treasury, XCM,
//! simulation and coretime, and **nothing consumes it**. … A differentiator
//! nobody can see is a differentiator nobody can check."* This module is the
//! consumer.
//!
//! # THE ONE ARCHITECTURAL DECISION, AND EVERYTHING ELSE FOLLOWS FROM IT
//!
//! **THE HTML SURFACE CONSUMES THE JSON SURFACE, IN PROCESS.** A page handler
//! calls the *same* `async fn` the JSON route calls, reads the bytes back, and
//! deserializes them into a narrow view struct. It never touches an index trait
//! and never re-derives a figure.
//!
//! ROADMAP states the rule this implements and calls it "the whole decision":
//!
//! > **THE DASHBOARD READS THE TYPED API AND NEVER THE TABLES.** dotlens's
//! > honesty lives in the READER … A panel pointed at `/v1/coretime/…/delta`
//! > inherits every one of them for free, including the nulls and
//! > `waste_withheld_because`. A panel pointed at `coretime.core_assignments`
//! > with SQL inherits none … **Same tables, opposite outcomes.**
//!
//! Reading the handler's own bytes is the strongest available form of that: the
//! page **cannot** render a number the API would not, because it never sees
//! anything else. It also means this slice refactors no producer — five
//! handlers assemble their bodies as `serde_json::json!` inline, and extracting
//! five producers in a session with no compiler is how a slice arrives broken.
//!
//! **The cost, stated rather than hidden:** one serialize→deserialize round trip
//! per panel per request. No query is repeated — the round trip is CPU on a
//! `Vec<u8>` that was already built. If that ever shows up in a profile, the fix
//! is to extract producers and have both surfaces call them, which is a
//! refactor this design does not block.
//!
//! # THE VIEW STRUCTS ARE THE CONTRACT
//!
//! Each panel deserializes into a `#[derive(Deserialize)]` struct naming only
//! the fields it renders. That is deliberate and it is a defect-class defence:
//! *"`jq` on a guessed key returns null and reads like a null-valued field"* is
//! on record here three times, and `body["postions"]` in Rust fails exactly the
//! same silent way. A view struct turns a renamed or removed field into a hard,
//! loud failure — which renders as this panel's refusal, naming the field.
//!
//! **The guarantee covers REQUIRED fields only, and that limit is real.** serde
//! routes a *missing* field through `missing_field`, whose `deserialize_option`
//! returns `None` — so a renamed `display` or `chain_head` deserializes to
//! `None` silently and renders as this page's own refusal wording ("total
//! withheld", "no chain head recorded") rather than as a parse failure. A
//! non-`Option` field that vanishes still fails loudly, which is most of them.
//! Closing the rest wants `Option<Option<T>>` with an explicit
//! `deserialize_with`, and it is named here rather than assumed away.
//!
//! # WHAT THIS PAGE MAY NOT DO
//!
//! `PREP-phase3.5-homepage.md` §7 is ten rules, each tracing to a defect this
//! project has already shipped once. The ones this file implements, by name:
//!
//! 1. **A withheld figure renders as a refusal WITH ITS REASON** — never an
//!    empty chart, a dash, a blank or a zero. [`Withheld`] and `.refusal`.
//! 2. **A count from a `limit`-capped list is a lie.** [`count_or_more`].
//! 7. **`at_decode_frontier` is not "current"** and is never coloured as if it
//!    were. See `style.rs`, `.state`.
//! 8. **A halted module names the variant and the height that halted it**, and
//!    it is the LOWEST refusal above the checkpoint.
//! 9. **`not_covered` is a panel element, not a footer.** [`Panel::notes`].
//! 10. **Nothing is conveyed by hover alone**, and the page is reviewed at 390px
//!    and 768px before it is called done.
//!
//! And ARCHITECTURE §9a.1's trap, which this page is the first thing that could
//! ever have fallen into: **the freshness strip and the data panels are two
//! different facts and are never merged into one "last updated" line.**

mod style;

use crate::AppState;
use axum::{
    extract::{Path, Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use maud::{html, Markup, DOCTYPE};
use serde::Deserialize;

/// Cap on a body read back from our own handler.
///
/// Not a security boundary — the bytes came from this process — but a guard
/// against a pathological consolidated payload turning a page render into an
/// allocation. Four times the 1 MB the JSON test helper uses, because
/// `/consolidated` fans out over every chain and account and is the one payload
/// here with no `limit` on it at all.
pub const BODY_LIMIT: usize = 4_000_000;

/// The HTML routes, merged into the API router.
///
/// Kept as a separate `Router` rather than as lines in `crate::router` so that
/// the whole surface can be added or removed in one place, and so a reader of
/// `router()` can see that the HTML routes exist without reading them.
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/", get(home))
        .route("/network/{network}", get(network_home))
        .route("/assets/dotlens.css", get(stylesheet))
}

// ===========================================================================
// The refusal
// ===========================================================================

/// A panel could not be rendered, and this is what the reader is told instead.
///
/// **It carries the API's OWN words**, not a sentence this module invented. When
/// `/v1/treasury/{n}/consolidated` 404s an unknown network, the panel says what
/// the endpoint said. A UI that substitutes its own phrasing for an API refusal
/// is a second implementation of the refusal, and the two drift.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Withheld {
    /// The status the inner handler returned, or `INTERNAL_SERVER_ERROR` when
    /// the failure was ours (a body we could not read or could not parse).
    pub status: StatusCode,
    pub message: String,
}

impl Withheld {
    fn ours(message: String) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message,
        }
    }

    /// Rendered inside the panel it belongs to — never as a page-level error,
    /// because one dead panel does not make the rest of the page untrue.
    fn render(&self) -> Markup {
        html! {
            div class="refusal" {
                b { "withheld" }
                " — " (self.message)
                span class="fix" { "status " (self.status.as_u16()) }
            }
        }
    }
}

/// The shape every JSON error in this API takes: `{"error": "..."}`.
#[derive(Deserialize)]
struct ApiError {
    error: String,
}

/// Call one of this crate's own JSON handlers and read its answer back.
///
/// **This is the whole "reads the typed API, never the tables" mechanism**, and
/// it is four lines because that is all it should be. A non-2xx becomes a
/// [`Withheld`] carrying the endpoint's own message; a body that does not fit
/// the view struct becomes a `Withheld` naming the parse failure, because a
/// silently-null field is the defect this design exists to prevent.
async fn read<T: serde::de::DeserializeOwned>(res: Response) -> Result<T, Withheld> {
    let status = res.status();
    let bytes = match axum::body::to_bytes(res.into_body(), BODY_LIMIT).await {
        Ok(b) => b,
        Err(e) => return Err(Withheld::ours(format!("reading our own response: {e}"))),
    };
    if !status.is_success() {
        let message = serde_json::from_slice::<ApiError>(&bytes)
            .map(|e| e.error)
            .unwrap_or_else(|_| String::from_utf8_lossy(&bytes).into_owned());
        return Err(Withheld { status, message });
    }
    serde_json::from_slice::<T>(&bytes).map_err(|e| {
        Withheld::ours(format!(
            "the API's answer did not fit the shape this panel renders — {e}. \
             This is a field that was renamed or removed, and it is reported \
             rather than rendered as an empty panel."
        ))
    })
}

// ===========================================================================
// Primitives: the panel, the notes, the honest count
// ===========================================================================

/// A figure, its coverage, and its link to the full page.
///
/// `PREP-phase3.5-homepage.md` §8 names three skeletons to build and only
/// three; this is the PANEL one. Every future metric lands in it, which is why
/// it takes `notes` rather than letting each panel invent its own footer.
struct Panel<'a> {
    title: &'a str,
    /// C8 — *"every panel title links to its own full page … the mechanism that
    /// makes 'choose what NOT to show' tractable"*. Where the full page does not
    /// exist yet, this points at the JSON endpoint, which is a real destination.
    /// It is never a dead link and never a "coming soon" (§8 DO-NOT 2).
    ///
    /// The href is an OWNED `String` because every one of them carries the
    /// network, and a borrowed `&format!(…)` is a temporary that does not live
    /// to the end of the statement.
    to: Option<(String, &'a str)>,
    body: Markup,
    /// The endpoint's own coverage lines, rendered INSIDE the panel.
    ///
    /// §7 rule 9: *"`not_covered` is a panel element, not a footer. The UI
    /// research found no prior art for this in ten walked references; there is
    /// nothing to copy and nothing to soften toward."*
    notes: Vec<String>,
}

impl Panel<'_> {
    fn render(self) -> Markup {
        html! {
            section class="panel" {
                div class="panel-head" {
                    h2 class="panel-title" { (self.title) }
                    @if let Some((href, label)) = self.to {
                        a class="panel-to" href=(href) { (label) " \u{2197}" }
                    }
                }
                div class="panel-body flush" { (self.body) }
                @if !self.notes.is_empty() {
                    div class="notes" {
                        div class="notes-h" { "what this does not cover" }
                        ul {
                            @for n in &self.notes { li { (n) } }
                        }
                    }
                }
            }
        }
    }
}

/// A magnitude that cannot overstate itself.
///
/// §7 rule 2: *"A count from a `limit`-capped list is a lie. Use the list, or
/// use `200+`. No list endpoint carries a `truncated` flag."*
///
/// **TWO REVIEWS WERE NEEDED TO GET THIS RIGHT AND BOTH FOUND A REAL BUG.**
/// The first version proved completeness from `rows < limit`, which is false
/// here: every list handler queries EACH residency window with the full limit
/// and then MERGES, so a window that returned exactly 50 can merge down to 47 —
/// capped, and reported as an exact 47. The second version therefore refused
/// any count across more than one window — and on `polkadot` the treasury
/// domain has TWO residency windows (relay, then Asset Hub from 2025-11-04),
/// permanently, so the bounties headline would have read `50+` for the rest of
/// time, including above an empty table. **A magnitude that is always `50+` is
/// "we did not look" wearing a number.**
///
/// The proof that actually holds is per segment: the endpoints publish how many
/// rows each window contributed, and a window that returned fewer than the
/// limit was not capped. If no window was capped, the merge cannot have dropped
/// anything, and the row count is exact.
///
/// An unreadable segment proves nothing and is treated as capped.
fn count_or_more(
    rows: usize,
    limit: u64,
    segments: &[serde_json::Value],
    count_key: &str,
) -> String {
    let any_capped = segments.iter().any(|s| match s[count_key].as_u64() {
        Some(n) => n >= limit,
        None => true,
    });
    if rows as u64 >= limit || any_capped {
        format!("{limit}+")
    } else {
        rows.to_string()
    }
}

/// Group the integer part in threes.
///
/// STYLE.md principle 4: *"Numbers are typeset like money … thousands
/// separators always"*. Grouping is PRESENTATION and belongs here — the API
/// returns `24310104.3351437286` and must, because a separator is a locale
/// decision and a decimal string is a fact. Inserting them is the one numeric
/// transformation this page performs, and it changes no digit.
fn group(display: &str) -> String {
    let (int, frac) = match display.split_once('.') {
        Some((i, f)) => (i, Some(f)),
        None => (display, None),
    };
    let (sign, digits) = match int.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", int),
    };
    // Anything that is not a plain digit string is passed through untouched
    // rather than mangled: this function may not invent a rendering for a value
    // it does not recognise.
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return display.to_string();
    }
    let mut out = String::with_capacity(display.len() + digits.len() / 3);
    out.push_str(sign);
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    if let Some(f) = frac {
        out.push('.');
        out.push_str(f);
    }
    out
}

/// A quantity with its unit, typeset like money (STYLE.md principle 4).
fn amount(display: &str, symbol: &str) -> Markup {
    html! {
        span class="mono" { (group(display)) }
        span class="unit" { (symbol) }
    }
}

// ===========================================================================
// The shell
// ===========================================================================

/// Which nav item is current.
///
/// A two-variant enum rather than a hardcoded `aria-current`, because the first
/// version stamped "page" on Overview from every route — including the 404 —
/// and an assistive technology reading that is being told something false.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Nav {
    Overview,
    Other,
}

/// Page chrome: a thin frame and nothing else.
///
/// STYLE.md principle 1 — *"no hero sections, no marketing air"* — and its
/// regression watch 4, *"marketing sections inside the product"*. There is no
/// tagline here on purpose.
fn shell(title: &str, current: Nav, head: Markup, body: Markup) -> Markup {
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title { (title) " · dotlens" }
                link rel="stylesheet" href="/assets/dotlens.css";
            }
            body {
                nav class="nav" {
                    div class="nav-in" {
                        a class="logo" href="/" { "dot" i { "lens" } }
                        div class="nav-links" {
                            @if matches!(current, Nav::Overview) {
                                a href="/" aria-current="page" { "Overview" }
                            } @else {
                                a href="/" { "Overview" }
                            }
                            a href="/v1/chains" { "Chains" }
                        }
                    }
                }
                main class="wrap" {
                    (head)
                    (body)
                }
                footer class="foot" {
                    // Every clause here has to be true of EVERY panel, because
                    // a page-level constant cannot be conditioned on a payload.
                    // The first version claimed "in each asset's own units",
                    // which two panels contradict by their own admission.
                    "Quantities only. No price source is consulted anywhere on this page, "
                    "and nothing here is converted between assets. Some figures are raw "
                    "integers in an unresolved unit and say so on the row. Every panel links "
                    "to the endpoint that produced it, with the same scope the panel used."
                }
            }
        }
    }
}

// ===========================================================================
// Routes
// ===========================================================================

async fn stylesheet() -> Response {
    (
        [
            (header::CONTENT_TYPE, "text/css; charset=utf-8"),
            // The stylesheet is compiled in, so it changes only when the binary
            // does. It is the one thing on this surface that is safe to hold.
            (header::CACHE_CONTROL, "public, max-age=3600"),
        ],
        style::CSS,
    )
        .into_response()
}

/// `GET /` — the homepage.
///
/// **It does not DEFAULT a network.** With one registered network it renders
/// that one; with several it renders the list and asks. That is the same
/// refusal the coretime readers make when they decline to pick a window —
/// *"defaulting to one would put a window nobody chose underneath a percentage
/// somebody quotes"* — applied to the largest scope on the page.
///
/// C2 says there is no chain SELECTOR on the homepage, which is a different
/// thing: chains are provenance and appear on the rows that came from them.
async fn home(State(state): State<AppState>) -> Response {
    let networks = crate::search::known_networks(&state.registry);
    match networks.len() {
        1 => render_network(state, networks[0].clone()).await,
        0 => shell(
            "dotlens",
            Nav::Overview,
            html! { div class="page-head" { h1 class="page-title" { "dotlens" } } },
            html! {
                div class="refusal" {
                    b { "no network is registered" }
                    " — the chain registry is empty, so there is nothing to report on. "
                    "This is a seeding question, not a pipeline one."
                }
            },
        )
        .into_response(),
        _ => shell(
            "dotlens",
            Nav::Overview,
            html! {
                div class="page-head" {
                    h1 class="page-title" { "dotlens" }
                    span class="scope" { (networks.len()) " networks registered" }
                }
            },
            html! {
                div class="panel" {
                    div class="panel-body" {
                        p class="muted" {
                            "More than one network is registered, and this page will not "
                            "pick one for you: every figure below the fold is scoped to a "
                            "network, and a scope nobody chose sitting under a number "
                            "somebody quotes is the thing this product refuses everywhere "
                            "else."
                        }
                        ul class="plain" {
                            @for n in &networks {
                                li {
                                    a class="mono" href=(format!("/network/{n}")) { (n) }
                                }
                            }
                        }
                    }
                }
            },
        )
        .into_response(),
    }
}

/// `GET /network/{network}` — composition A, "the Ledger", for one network.
async fn network_home(State(state): State<AppState>, Path(network): Path<String>) -> Response {
    let known = crate::search::known_networks(&state.registry);
    if !known.contains(&network) {
        // Same polarity as `/v1/treasury/{network}/consolidated`, deliberately:
        // an empty page for an unknown network reads as "this network has
        // nothing", which is a claim about the network.
        return (
            StatusCode::NOT_FOUND,
            shell(
                "not found",
                Nav::Other,
                html! { div class="page-head" { h1 class="page-title" { "unknown network" } } },
                html! {
                    div class="refusal" {
                        b { "unknown network '" (network) "'" }
                        " — registered networks: " (known.join(", "))
                    }
                },
            ),
        )
            .into_response();
    }
    render_network(state, network).await
}

async fn render_network(state: AppState, network: String) -> Response {
    let strip = freshness_strip(&state, &network).await;
    let treasury = treasury_panel(&state, &network).await;
    let referenda = referenda_panel(&state, &network).await;
    let paid = paid_panel(&state, &network).await;
    let bounties = bounties_panel(&state, &network).await;

    let page = shell(
        &network,
        Nav::Overview,
        html! {
            div class="page-head" {
                h1 class="page-title" { (network) }
                // C7: the page-level scope, pinned in the header, visible while
                // the numbers are read.
                span class="scope" { "state of account \u{00b7} quantities only" }
            }
        },
        html! {
            (strip)
            div class="grid" {
                div class="col" { (treasury) (referenda) }
                div class="col" { (paid) (bounties) }
            }
        },
    );

    // NOT `immutable`, and not a long TTL. ARCHITECTURE §9a.1 caps anything
    // carrying a provisional marker at a short TTL, and this page carries a
    // freshness strip, which is the definition of one. The data panels behind
    // it keep their own headers on their own routes; composing them does not
    // let the composition inherit the strongest one.
    ([(header::CACHE_CONTROL, "no-store")], page).into_response()
}

// ===========================================================================
// Panel: the freshness strip
// ===========================================================================

#[derive(Deserialize)]
struct FreshnessView {
    frontiers: FrontiersView,
    modules: Vec<ModuleView>,
    /// THE ENDPOINT'S OWN COVERAGE LINES, and deserializing them is a fix rather
    /// than a nicety. The first version of this strip rendered
    /// `raw 60 behind, observed 5s ago` and dropped the sentence the reader
    /// needs most: that `frontiers.raw` is a HIGH-WATER MARK, so a chain seeded
    /// a minute ago reports `raw_behind_chain: 0` while holding one block of
    /// history. CLAUDE.md names that line for the very next slice — *"read it
    /// rather than the zero"* — and Bridge Hub and Bulletin will both hit it on
    /// day one.
    not_covered: Vec<String>,
}

#[derive(Deserialize)]
struct FrontiersView {
    chain_head: Option<HeadView>,
    raw_behind_chain: Option<i64>,
}

#[derive(Deserialize)]
struct HeadView {
    finalized_height: u64,
    age_seconds: i64,
}

#[derive(Deserialize)]
struct ModuleView {
    module: String,
    state: String,
    blocking_halt: Option<HaltView>,
}

#[derive(Deserialize)]
struct HaltView {
    height: u64,
    event: String,
}

/// The oldest chain-head observation on a network, with the delta it bounds.
///
/// A named struct rather than a 4-tuple because two of its fields are heights
/// and two are not, and `(String, u64, i64, Option<i64>)` is the shape where a
/// later reader transposes two of them. This project has the same note filed
/// against `MemoryCoretimeIndex::rows`, a 6-tuple with `core`/`para` adjacent.
struct OldestHead {
    chain: String,
    finalized_height: u64,
    age_seconds: i64,
    raw_behind_chain: Option<i64>,
    /// This chain's own `not_covered`, carried so the strip can state what the
    /// delta beside it does not cover.
    not_covered: Vec<String>,
}

/// The strip: how far behind every module on every chain of this network is.
///
/// **It is a top strip rather than a foot, and §9 left that open.** PRODUCT's
/// legibility contract puts coverage at the foot — *"the thing you check after
/// reading, not the toll you pay before"* — but a HALTED module is not a
/// coverage statement, it is a warning, and a warning under the fold is not a
/// warning. So the strip is quiet when nothing is wrong and loud when something
/// is, and the coverage half still renders, below the counts.
///
/// **THE COUNTS ARE COMMENSURATE ON PURPOSE.** `41 modules · 1 halted · 12
/// never run` are all one-row-per-(chain, module), so the second and third are
/// subsets of the first and a reader can subtract them. The homepage sketch's
/// *"6 chains · 9 modules · 1 halted"* mixed a distinct-name count with a row
/// count, which cannot be read that way.
///
/// **A CHAIN WE COULD NOT READ IS NAMED, NEVER COUNTED AS ZERO** — PATTERNS A2
/// at container level. And the chain count says READ, not registered: a review
/// caught the first version labelling the registry's own count "indexed" while
/// the alarm block below it said those chains were excluded from the counts.
async fn freshness_strip(state: &AppState, network: &str) -> Markup {
    let registered: Vec<String> = state
        .registry
        .chains()
        .filter(|c| c.network == network)
        .map(|c| c.id.clone())
        .collect();

    let mut read_ok = 0usize;
    let mut modules = 0usize;
    let mut halted: Vec<(String, String, Option<HaltView>)> = Vec::new();
    let mut never_run = 0usize;
    let mut behind = 0usize;
    let mut unread: Vec<String> = Vec::new();
    let mut oldest_head: Option<OldestHead> = None;

    for chain in &registered {
        // ONE ROUND TRIP PER CHAIN, and it is the gap this page just found.
        // `VERIFY-phase3.5-freshness-route.md` §6 left the batched all-chains
        // variant open deliberately, *"because it should be decided from a page
        // rather than before one"*. This is that page, and the answer is that
        // the strip wants one request. It is N here, with N from the registry.
        let res = crate::get_freshness(State(state.clone()), Path(chain.clone())).await;
        let view: FreshnessView = match read(res).await {
            Ok(v) => v,
            Err(w) => {
                unread.push(format!(
                    "{chain} \u{2014} status {} \u{2014} {}",
                    w.status.as_u16(),
                    w.message
                ));
                continue;
            }
        };
        read_ok += 1;
        modules += view.modules.len();
        for m in &view.modules {
            match m.state.as_str() {
                "halted" => halted.push((
                    chain.clone(),
                    m.module.clone(),
                    // NOT defaulted to `(0, "unknown")`. A halt with no
                    // coordinates is unreachable today, and rendering
                    // `blocked at #0` if it ever happened would be rule 8's
                    // own failure — "naming 320 sends whoever reads it at 3am
                    // to the wrong block" — with a fabricated number.
                    m.blocking_halt.as_ref().map(|h| HaltView {
                        height: h.height,
                        event: h.event.clone(),
                    }),
                )),
                "never_run" => never_run += 1,
                "behind" => behind += 1,
                _ => {}
            }
        }
        if let Some(head) = &view.frontiers.chain_head {
            // The oldest observation on the network. It bounds the network's
            // AGE — not its delta: the chain with the stalest look is not
            // necessarily the chain furthest behind, which is why the chain is
            // named beside its own numbers rather than presented as a summary.
            let replace = match &oldest_head {
                None => true,
                Some(prev) => head.age_seconds > prev.age_seconds,
            };
            if replace {
                oldest_head = Some(OldestHead {
                    chain: chain.clone(),
                    finalized_height: head.finalized_height,
                    age_seconds: head.age_seconds,
                    raw_behind_chain: view.frontiers.raw_behind_chain,
                    not_covered: view.not_covered.clone(),
                });
            }
        }
    }

    // A NETWORK WHOSE EVERY MODULE HAS NEVER RUN MUST NOT RENDER CALM.
    // `ChainFreshness::never_run` exists one layer down for exactly this
    // reason — *"without it a fully-unstarted chain reads GREEN to anything
    // that alerts"* — and a strip that alarmed only on halts would reproduce
    // the defect at container level.
    let nothing_started = modules > 0 && never_run == modules;
    let alarm = !halted.is_empty() || !unread.is_empty() || nothing_started;

    html! {
        div class=(if alarm { "strip alarm" } else { "strip" }) {
            span class="strip-k" { "read" }
            span class="strip-v" { (read_ok) }
            span class="micro" { "of " (registered.len()) " registered chains" }
            span class="strip-sep" { "\u{00b7}" }
            span class="strip-v" { (modules) }
            span class="micro" { "modules" }

            @if !halted.is_empty() {
                span class="strip-sep" { "\u{00b7}" }
                span class="state halted" { (halted.len()) " HALTED" }
            }
            @if behind > 0 {
                span class="strip-sep" { "\u{00b7}" }
                span class="strip-v" { (behind) }
                span class="micro" { "behind" }
            }
            @if never_run > 0 {
                span class="strip-sep" { "\u{00b7}" }
                span class="strip-v" { (never_run) }
                span class="micro" { "never run" }
            }

            @if let Some(h) = &oldest_head {
                span class="strip-sep" { "\u{00b7}" }
                // ARCHITECTURE §9a.1: the delta and the AGE of the observation
                // it was measured against render together and are never merged
                // into one "last updated" line. A dead follower freezes the head
                // and the raw frontier at the same instant, so the delta falls
                // toward 0 while nothing is catching up.
                //
                // FINALIZED, said in the sentence: the best block is ahead of it
                // by the finality lag, so 0 means "level with the finalized
                // head" and never "at the tip".
                span class="micro" { "oldest chain-head observation" }
                span class="strip-v" { (h.chain) " #" (h.finalized_height) }
                @if let Some(b) = h.raw_behind_chain {
                    span class="micro" {
                        "raw " (b) " behind the finalized head, observed " (h.age_seconds) "s ago"
                    }
                } @else {
                    span class="micro" {
                        "observed " (h.age_seconds) "s ago; no raw frontier to place it against"
                    }
                }
            } @else {
                span class="strip-sep" { "\u{00b7}" }
                span class="micro" {
                    "no chain head recorded on any chain of this network \u{2014} \
                     nothing here is placed against the chain"
                }
            }

            // THE CHAIN WHOSE FIGURES ARE QUOTED, falling back to the first
            // registered one. `registered.first()` alone sent the reader to
            // whichever chain sorts first alphabetically, while the head, the
            // delta and the coverage heading all named a different one.
            @if let Some(first) = oldest_head
                .as_ref()
                .map(|h| h.chain.as_str())
                .or_else(|| registered.first().map(String::as_str))
            {
                a class="strip-to" href=(format!("/v1/freshness/{first}")) {
                    "per-module freshness for " (first) " \u{2197}"
                }
            }
        }

        @if nothing_started {
            div class="strip alarm block" {
                div class="strip-say" {
                    "every declared module on this network has NEVER RUN"
                }
                div class="micro" {
                    "Nothing below is a statement about the network. It is a statement that \
                     nothing has been indexed here yet."
                }
            }
        }

        @if !halted.is_empty() {
            div class="strip alarm block" {
                div class="strip-say" {
                    "a human is needed \u{2014} nothing below will move on its own"
                }
                // §7 rule 8: a halted module NAMES the variant and the height,
                // and it is the LOWEST refusal above the checkpoint.
                @for (chain, module, halt) in &halted {
                    div class="mono line" {
                        (chain) " / " (module)
                        @if let Some(h) = halt {
                            " \u{2014} blocked at #" (h.height) " on " (h.event)
                        } @else {
                            " \u{2014} halted, and the refusal that blocks it was not \
                             reported with this row"
                        }
                    }
                }
            }
        }

        @if !unread.is_empty() {
            div class="strip alarm block" {
                div class="strip-say" {
                    "not read \u{2014} these chains are in NONE of the counts above"
                }
                @for u in &unread { div class="mono line" { (u) } }
            }
        }

        // §7 rule 9 applied to the strip too, and this is the half the first
        // version dropped. The coverage shown is the OLDEST-observed chain's,
        // because that is the chain whose numbers are quoted above.
        @if let Some(h) = &oldest_head {
            @if !h.not_covered.is_empty() {
                div class="notes strip-notes" {
                    div class="notes-h" { "what the figures above do not cover (" (h.chain) ")" }
                    ul {
                        @for n in &h.not_covered { li { (n) } }
                    }
                }
            }
        }
    }
}

// ===========================================================================
// Panel: treasury
// ===========================================================================

#[derive(Deserialize)]
struct ConsolidatedView {
    positions: Vec<PositionView>,
    /// REAL TREASURY MONEY THAT COULD NOT BE CONSOLIDATED, and dropping it was
    /// the first version's worst defect: the panel's own rendered `not_covered`
    /// line says such assets *"are listed under `unconsolidated`"*, which was
    /// false of the page it sat on. The treasury then read as fully
    /// consolidated when it is not.
    unconsolidated: Vec<serde_json::Value>,
    coverage: CoverageView,
}

#[derive(Deserialize)]
struct CoverageView {
    valuation: String,
    positions_without_an_anchor: usize,
    erc20_positions: usize,
    not_covered: Vec<String>,
}

#[derive(Deserialize)]
struct PositionView {
    symbol: Option<String>,
    /// Present only when the legs are addable AND all their sizes are known.
    /// A null here is the refusal, not a zero.
    display: Option<String>,
    addable: bool,
    complete: bool,
    legs_of_unknown_size: usize,
    chains: Vec<String>,
    legs: Vec<LegView>,
}

#[derive(Deserialize)]
struct LegView {
    chain: String,
    amount: Option<String>,
    /// THE API'S OWN WORDS for why this leg has no size — *"no anchor: this
    /// pair has been seen MOVING but never read from state. Run
    /// treasury-holdings on this chain"*. Rendered rather than paraphrased,
    /// because a UI that rewrites an endpoint's refusal is a second
    /// implementation of it and the two drift.
    unknown_because: Option<String>,
    provenance: ProvenanceView,
}

#[derive(Deserialize)]
struct ProvenanceView {
    anchor_height: Option<u64>,
}

/// The treasury, consolidated across chains, one row per logical asset.
///
/// **THE WITHHELD ROW IS THE PRODUCT'S ARGUMENT** (PREP §3): a withheld total
/// with its reason and the command that would fix it, above the fold, on the
/// first screen a stranger sees. It is only possible because `/consolidated`
/// refuses rather than rounds.
///
/// **The per-leg anchor heights are a LIST and never one `as_of`** — ARCH §9c
/// rule 2 and §7 rule 5. A single "as of block N" over legs read at different
/// heights would be the quiet lie this whole panel exists to avoid.
async fn treasury_panel(state: &AppState, network: &str) -> Markup {
    let to = Some((
        format!("/v1/treasury/{network}/consolidated"),
        "every consolidated leg",
    ));
    let res =
        crate::get_treasury_consolidated(State(state.clone()), Path(network.to_string())).await;
    let view: ConsolidatedView = match read(res).await {
        Ok(v) => v,
        Err(w) => {
            // C8 applies HARDEST here: a panel that could not render is exactly
            // when the reader wants the endpoint's own answer, so the failed
            // arm keeps its destination rather than dropping it.
            return Panel {
                title: "Treasury",
                to,
                body: html! { div class="panel-body" { (w.render()) } },
                notes: Vec::new(),
            }
            .render();
        }
    };

    let body = html! {
        div class="scroll" {
            table class="sticky1" {
                thead {
                    tr {
                        th { "asset" }
                        th class="num" { "total" }
                        th { "across" }
                        th { "anchored at" }
                    }
                }
                tbody {
                    @for p in &view.positions {
                        tr {
                            td {
                                span class="mono" { (p.symbol.clone().unwrap_or_else(|| "\u{2014}".into())) }
                            }
                            td class="num" {
                                @if let Some(d) = &p.display {
                                    (amount(d, p.symbol.as_deref().unwrap_or("")))
                                } @else {
                                    // §7 rule 1. The reason is built from the
                                    // SAME fields the row is built from, so it
                                    // cannot be false about the row beside it.
                                    (withheld_total(p))
                                }
                            }
                            td {
                                span class="mono" { (p.chains.join(", ")) }
                            }
                            td {
                                // A LIST of anchors, never one.
                                @for leg in &p.legs {
                                    div class="mono anchor" {
                                        (leg.chain) " "
                                        @if let Some(h) = leg.provenance.anchor_height {
                                            "#" (h)
                                        } @else {
                                            span class="warn-ink" { "no anchor" }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    @if view.positions.is_empty() {
                        tr { td colspan="4" {
                            div class="refusal" {
                                b { "no consolidated position" }
                                @if view.unconsolidated.is_empty() {
                                    " \u{2014} nothing has been read here. It is not a statement \
                                     that the treasury is empty."
                                } @else {
                                    // A3: fixing the arm you were shown leaves
                                    // the class alive one field over. Rows WERE
                                    // read here; they could not be grouped, and
                                    // saying "nothing has been read" would be
                                    // false about the object beside it.
                                    " \u{2014} rows were read but none could be named across "
                                    "chains, so every one of them is under `unconsolidated` "
                                    "rather than here."
                                }
                            }
                        } }
                    }
                }
            }
        }
        div class="panel-body tight" {
            // `.sub` (12px, --t2), not `.micro` (11px, --t3): this is a
            // five-clause sentence, and style.rs states the rule it would
            // otherwise break — sentences get reading size.
            p class="sub" { (view.coverage.valuation) }
            // The counts the first version dropped. None of them is capped by a
            // `limit`, so each is an honest exact number rather than a `N+`.
            @if !view.unconsolidated.is_empty() || view.coverage.positions_without_an_anchor > 0 {
                // EACH CLAUSE IS TRUE OF ITS OWN NUMBER, which the first
                // version was not: `erc20_positions` is a SUBSET of
                // `unconsolidated`, not a third group, and
                // `positions_without_an_anchor` counts legs that DO appear
                // above with their totals withheld. Rendered as three
                // commensurate figures they read as a sum, which is the exact
                // misreading the strip's own header warns about.
                p class="sub" {
                    (view.unconsolidated.len()) " position(s) could not be consolidated at all "
                    "and are not in the rows above; " (view.coverage.erc20_positions)
                    " of those are ERC-20 balances the reader cannot value in principle. "
                    "Separately, " (view.coverage.positions_without_an_anchor)
                    " leg(s) have no anchor \u{2014} the ones with an absolute name ARE in the "
                    "rows above, with their totals withheld."
                }
            }
        }
    };

    Panel {
        title: "Treasury",
        to,
        body,
        notes: view.coverage.not_covered,
    }
    .render()
}

/// Why a total is missing, said in the row's own terms.
///
/// Two independent conditions and they are NOT the same failure: `addable`
/// false means the legs disagree about what a unit is, and `complete` false
/// means one of them has an unknown size. A reader who is told only "withheld"
/// cannot act; a reader told which one, and on which chain, can.
///
/// **The fix command is ONE LINE PER CHAIN.** The CLI is
/// `treasury-holdings <chain> [height]` — a single argument — so joining two
/// chains with a space printed a command that fails to parse. The API's own
/// words are the same rule one layer down: *"a remedy that cannot work is worse
/// than none"*.
fn withheld_total(p: &PositionView) -> Markup {
    let unknown: Vec<&LegView> = p.legs.iter().filter(|l| l.amount.is_none()).collect();
    html! {
        div class="refusal left" {
            b { "total withheld" }
            @if !p.addable {
                " \u{2014} the legs disagree on decimals, so adding them would "
                "produce a number in no unit at all."
            }
            @if !p.complete {
                " \u{2014} " (p.legs_of_unknown_size) " of " (p.legs.len())
                " legs have an unknown size, so any sum would be smaller than the truth."
            }
            @if p.addable && p.complete {
                // Unreachable while `display` is `Some` for every summable
                // position — but rule 1 is that a refusal carries a REASON, and
                // a bare "withheld" with no `@else` is that rule waiting on one
                // API change.
                " \u{2014} and this reader cannot say why: the endpoint reported the "
                "position addable and complete, then withheld the total anyway."
            }
            @for leg in &unknown {
                @if let Some(reason) = &leg.unknown_because {
                    span class="fix" { (leg.chain) ": " (reason) }
                } @else {
                    span class="fix" {
                        "dotlens-node treasury-holdings " (leg.chain)
                    }
                }
            }
        }
    }
}

// ===========================================================================
// Panel: referenda  (§8's FOURTH STATE lives here)
// ===========================================================================

/// **THE FOURTH STATE**, and building it was §8's first DO.
///
/// The surface brief gives per-chain pages three states — *module not declared*
/// / *declared, no data yet* / *declared, with data*. The metric inventory found
/// a fourth is needed: ***the network does this, dotlens does not model it***.
/// Without it, a module dotlens has no mapper for renders either as a lie about
/// the network or as a promise of a backfill that will never fix it.
///
/// It is **a sentence, not a slot** (§8 DO-NOT 2: no empty panels, no "coming
/// soon"), and it is reached from the REGISTRY rather than from a hardcoded
/// list — ROADMAP:315, *"the dashboard discovers chains from the REGISTRY … by
/// asking which chains declare a module or a capability, never by naming an
/// id. That is Invariant 2 reaching the UI."*
fn undeclared(title: &'static str, module: &str, network: &str) -> Markup {
    Panel {
        title,
        to: Some(("/v1/chains".to_string(), "what each chain declares")),
        body: html! {
            div class="panel-body" {
                div class="refusal" {
                    b { "not modelled here" }
                    " \u{2014} no chain registered on " (network) " declares a `" (module)
                    "` module, so dotlens indexes nothing for it. This is not a statement that "
                    (network) " has no " (module) ": it is a statement about this index's scope."
                }
            }
        },
        notes: Vec::new(),
    }
    .render()
}

/// Does any chain on this network declare `module`?
///
/// Registry data, asked as a question — never a chain id in a match arm.
pub(crate) fn network_declares(state: &AppState, network: &str, module: &str) -> bool {
    state
        .registry
        .chains()
        .any(|c| c.network == network && c.has_module(module))
}

#[derive(Deserialize)]
struct ReferendaView {
    referenda: Vec<ReferendumItem>,
}

#[derive(Deserialize)]
struct ReferendumItem {
    referendum_id: u64,
    track_id: Option<u32>,
    status: String,
    proposal: Option<serde_json::Value>,
    proposal_hash: Option<String>,
}

/// The governance queue, as a LIST.
///
/// **IT IS TITLED "RECENT REFERENDA" AND NOT "IN FLIGHT", AND THAT IS A FIX.**
/// PREP §3 sketched an *in flight* panel, and `list_gov_referenda` has no status
/// filter at all: it collects every referendum in the residency windows and
/// takes `.rev().take(limit)` — the highest ids, whatever their status. On live
/// Polkadot most of those are `approved`, `rejected` or `timed_out`. A panel
/// headed "In flight" over that list contradicts its own status column row by
/// row. Filtering here would be the page deciding what "in flight" means, which
/// is a producer's job; naming the panel for what the endpoint returns is not.
///
/// The interesting column is the one nobody else has: whether the preimage
/// DECODED. A referendum whose proposal is null is not a referendum without a
/// proposal — it is one whose preimage we were too late to fetch.
async fn referenda_panel(state: &AppState, network: &str) -> Markup {
    const LIMIT: u64 = 12;
    if !network_declares(state, network, "governance") {
        return undeclared("Recent referenda", "governance", network);
    }
    // The destination carries THE SAME limit the panel used. A link that serves
    // 25 rows under a panel showing 12 is a destination that disagrees with the
    // thing it is a destination for.
    let to = Some((
        format!("/v1/gov/{network}/referenda?limit={LIMIT}"),
        "all referenda",
    ));
    let res = crate::list_gov_referenda(
        State(state.clone()),
        Path(network.to_string()),
        Query(crate::GovQuery {
            class: None,
            limit: Some(LIMIT),
            at: None,
        }),
    )
    .await;
    let view: ReferendaView = match read(res).await {
        Ok(v) => v,
        Err(w) => {
            return Panel {
                title: "Recent referenda",
                to,
                body: html! { div class="panel-body" { (w.render()) } },
                notes: Vec::new(),
            }
            .render();
        }
    };

    let body = html! {
        div class="scroll" {
            table class="sticky1" {
                thead { tr {
                    th { "ref" }
                    th { "status" }
                    th { "track" }
                    th { "proposal" }
                } }
                tbody {
                    @for r in &view.referenda {
                        tr {
                            td { span class="mono" { (r.referendum_id) } }
                            td { span class="state" { (r.status) } }
                            td class="mono" {
                                @if let Some(t) = r.track_id { (t) } @else { span class="faint" { "\u{2014}" } }
                            }
                            td {
                                @if r.proposal.is_some() {
                                    "decoded"
                                } @else if r.proposal_hash.is_some() {
                                    span class="warn-ink" { "preimage MISSING" }
                                } @else {
                                    span class="faint" { "no proposal recorded" }
                                }
                            }
                        }
                    }
                    @if view.referenda.is_empty() {
                        tr { td colspan="4" {
                            div class="refusal" {
                                b { "no referenda indexed" }
                                " \u{2014} a governance module is declared on this network and has "
                                "produced no rows. Read this as 'not indexed', never as 'nothing "
                                "in flight'."
                            }
                        } }
                    }
                }
            }
        }
    };

    Panel {
        title: "Recent referenda",
        to,
        body,
        notes: vec![
            format!(
                "THESE ARE THE {LIMIT} HIGHEST-NUMBERED REFERENDA, NOT THE OPEN ONES. The \
                 endpoint carries no status filter, so decided, rejected and timed-out rows \
                 appear here beside live ones — read the status column, not the panel title."
            ),
            format!(
                "A LIST of at most {LIMIT}, never a count. This endpoint publishes no \
                 `truncated` flag and no per-chain `segments`, so nothing on this page derives \
                 a magnitude from it."
            ),
            "`preimage MISSING` is a statement about what dotlens could fetch, not about the \
             referendum: a preimage vanishes from state after enactment, so a proposal we were \
             too late to read is unrecoverable rather than absent."
                .to_string(),
        ],
    }
    .render()
}

// ===========================================================================
// Panel: recently paid
// ===========================================================================

#[derive(Deserialize)]
struct SpendsView {
    spends: Vec<SpendItem>,
    segments: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
struct SpendItem {
    spend_id: u64,
    /// **`spend_id` IS NOT UNIQUE WITHOUT THIS.** The list returns both id
    /// spaces (`proposal` and `asset_spend`) and the detail route defaults to
    /// `asset_spend`, so a link built without `?kind=` 404s on every legacy row
    /// — and a proposal 313 and an asset_spend 313 render as two rows with the
    /// same visible id. That is the defect `BountyItem.child_id` was added to
    /// fix, one panel over.
    spend_kind: String,
    status: String,
    amount: Option<String>,
}

/// Recent treasury spends.
///
/// **THE AMOUNTS ARE RAW AND THE REASON IS THIS PANEL'S CHOICE OF ENDPOINT, NOT
/// AN API GAP.** An earlier draft of this note claimed resolving them "is an API
/// change, not a UI one". That was wrong and a review caught it: the DETAIL
/// route `/v1/treasury/{network}/spends/{id}` already resolves the asset through
/// `resolve_spend_asset`, and PREP §3's element table names that route as this
/// element's source precisely for the resolved unit. The LIST does not resolve,
/// so this panel shows raw integers, says so, and links each row to the route
/// that can answer. Rendering `native` beside a raw planck integer — which the
/// first version did — was worse than bare: `20895000000 native` reads as 20.9
/// billion DOT.
async fn paid_panel(state: &AppState, network: &str) -> Markup {
    const LIMIT: u64 = 8;
    if !network_declares(state, network, "treasury") {
        return undeclared("Recently paid", "treasury", network);
    }
    let to = Some((
        format!("/v1/treasury/{network}/spends?status=paid&limit={LIMIT}"),
        "all spends",
    ));
    let res = crate::list_treasury_spends(
        State(state.clone()),
        Path(network.to_string()),
        Query(crate::TreasuryQuery {
            instance: None,
            kind: None,
            status: Some("paid".to_string()),
            limit: Some(LIMIT),
        }),
    )
    .await;
    let view: SpendsView = match read(res).await {
        Ok(v) => v,
        Err(w) => {
            return Panel {
                title: "Recently paid",
                to,
                body: html! { div class="panel-body" { (w.render()) } },
                notes: Vec::new(),
            }
            .render();
        }
    };

    let body = html! {
        div class="scroll" {
            table class="sticky1" {
                thead { tr {
                    th { "spend" }
                    th { "status" }
                    th class="num" { "amount, unresolved" }
                } }
                tbody {
                    @for s in &view.spends {
                        tr {
                            td {
                                a class="mono" href=(format!("/v1/treasury/{network}/spends/{}?kind={}", s.spend_id, s.spend_kind)) {
                                    (s.spend_id)
                                }
                                span class="unit" { (s.spend_kind) }
                            }
                            td { span class="state" { (s.status) } }
                            td class="num" {
                                @if let Some(a) = &s.amount {
                                    // NO UNIT LABEL. The integer is in an
                                    // unknown asset's smallest denomination and
                                    // this page will not name it.
                                    span class="mono" { (group(a)) }
                                } @else {
                                    span class="warn-ink" { "amount not recorded" }
                                }
                            }
                        }
                    }
                    @if view.spends.is_empty() {
                        tr { td colspan="3" {
                            div class="refusal" {
                                b { "no paid spend indexed" }
                                " \u{2014} nothing has been read here. It is not a statement that "
                                "the treasury has paid nothing."
                            }
                        } }
                    }
                }
            }
        }
    };

    Panel {
        title: "Recently paid",
        to,
        body,
        notes: vec![
            format!(
                "AMOUNTS HERE ARE RAW INTEGERS in each spend's own smallest unit, and are NOT \
                 comparable to each other. The LIST endpoint returns `amount` and `asset_ref` \
                 without decimals or a symbol; the per-spend route each row links to resolves \
                 them. This panel will not guess a unit, and there are {} chain segment(s) \
                 behind these rows.",
                view.segments.len()
            ),
            "`processed` does not assert success \u{2014} the pallet emits it for expiry too, \
             which is why this panel asks for `paid` rather than showing every terminal status \
             as money that moved."
                .to_string(),
        ],
    }
    .render()
}

// ===========================================================================
// Panel: bounties
// ===========================================================================

#[derive(Deserialize)]
struct BountiesView {
    bounties: Vec<BountyItem>,
    segments: Vec<serde_json::Value>,
    note: String,
}

#[derive(Deserialize)]
struct BountyItem {
    bounty_id: u64,
    /// **PRESENT BECAUSE THE LIST RETURNS CHILDREN TOO.** `list_bounties`
    /// deserializes `child` and never reads it — that parameter belongs to the
    /// detail route — so parents and children come back together, merged on
    /// `(bounty_id, child_id)`. Without this field a parent and its children
    /// render as several rows carrying the same visible id, and the first
    /// version labelled the total "parent bounties".
    child_id: Option<u64>,
    status: String,
    value: Option<String>,
}

/// Bounties — parents AND children, because that is what the endpoint returns.
async fn bounties_panel(state: &AppState, network: &str) -> Markup {
    const LIMIT: u64 = 50;
    if !network_declares(state, network, "treasury") {
        return undeclared("Bounties", "treasury", network);
    }
    let to = Some((
        format!("/v1/bounties/{network}?limit={LIMIT}"),
        "all bounties",
    ));
    let res = crate::list_bounties(
        State(state.clone()),
        Path(network.to_string()),
        Query(crate::BountyQuery {
            instance: None,
            child: None,
            status: None,
            limit: Some(LIMIT),
        }),
    )
    .await;
    let view: BountiesView = match read(res).await {
        Ok(v) => v,
        Err(w) => {
            return Panel {
                title: "Bounties",
                to,
                body: html! { div class="panel-body" { (w.render()) } },
                notes: Vec::new(),
            }
            .render();
        }
    };

    // `bounties_indexed`, not `spends_indexed` — the two endpoints spell their
    // per-segment count differently, and passing the wrong key would read every
    // segment as unreadable and pin the magnitude at `50+`.
    let magnitude = count_or_more(
        view.bounties.len(),
        LIMIT,
        &view.segments,
        "bounties_indexed",
    );
    let body = html! {
        div class="panel-body" {
            p {
                span class="mono big" { (magnitude) }
                // NOT "parent bounties": the list carries children too, and the
                // measured trap is on record — "Bounties.BountyCount = 7209 but
                // only 13 parent bounties live".
                span class="unit" { "bounty rows returned, parents and children" }
            }
        }
        div class="scroll" {
            table class="sticky1" {
                thead { tr {
                    th { "bounty" }
                    th { "status" }
                    th class="num" { "value, unresolved" }
                } }
                tbody {
                    @for b in &view.bounties {
                        tr {
                            td {
                                span class="mono" { (b.bounty_id) }
                                @if let Some(c) = b.child_id {
                                    span class="unit" { "child " (c) }
                                }
                            }
                            td { span class="state" { (b.status) } }
                            td class="num" {
                                @if let Some(v) = &b.value {
                                    span class="mono" { (group(v)) }
                                } @else {
                                    span class="faint" { "\u{2014}" }
                                }
                            }
                        }
                    }
                    @if view.bounties.is_empty() {
                        tr { td colspan="3" {
                            div class="refusal" {
                                b { "no bounty indexed" }
                                " \u{2014} nothing has been read here."
                            }
                        } }
                    }
                }
            }
        }
    };

    Panel {
        title: "Bounties",
        to,
        body,
        notes: vec![
            view.note,
            "Values are raw integers in the bounty's own smallest unit, unresolved for the same \
             reason as the spends panel above."
                .to_string(),
        ],
    }
    .render()
}

#[cfg(test)]
mod tests {
    use super::*;

    // ------------------------------------------------------- the honest count

    /// §7 rule 2, and the PAIR is the whole test: an assertion that only checks
    /// the capped case cannot fail if the function returns `format!("{n}+")`
    /// unconditionally.
    fn segs(counts: &[u64]) -> Vec<serde_json::Value> {
        counts
            .iter()
            .map(|n| serde_json::json!({ "bounties_indexed": n }))
            .collect()
    }

    #[test]
    fn a_count_is_exact_only_when_no_window_reached_its_cap() {
        // every window stopped short of the cap, so the merge dropped nothing.
        assert_eq!(
            count_or_more(13, 50, &segs(&[9, 4]), "bounties_indexed"),
            "13"
        );
        assert_eq!(count_or_more(0, 50, &segs(&[0]), "bounties_indexed"), "0");
        // the merged total reaching the cap proves nothing on its own.
        assert_eq!(
            count_or_more(50, 50, &segs(&[25, 25]), "bounties_indexed"),
            "50+"
        );
    }

    /// THE FIRST BUG A REVIEW CAUGHT: every list handler queries EACH residency
    /// window with the full limit and then MERGES, so a window that returned
    /// exactly 50 can merge down to 47 — capped, and reported as an exact 47.
    #[test]
    fn one_capped_window_makes_the_merged_total_inexact_however_small_it_is() {
        assert_eq!(
            count_or_more(7, 50, &segs(&[50, 3]), "bounties_indexed"),
            "50+"
        );
    }

    /// THE SECOND BUG, and it was worse: refusing any count across more than one
    /// window pinned `polkadot` at `50+` forever, because the treasury domain
    /// has two residency windows permanently (relay, then Asset Hub). A
    /// magnitude that is always `50+` — including above an empty table — is
    /// "we did not look" wearing a number.
    #[test]
    fn two_uncapped_windows_still_give_an_exact_count() {
        assert_eq!(
            count_or_more(1, 50, &segs(&[1, 0]), "bounties_indexed"),
            "1"
        );
        assert_eq!(
            count_or_more(0, 50, &segs(&[0, 0]), "bounties_indexed"),
            "0"
        );
    }

    /// A segment whose count cannot be read proves nothing, so it is treated as
    /// capped — and the wrong KEY makes every segment unreadable, which is why
    /// the caller passes the endpoint's own spelling.
    #[test]
    fn an_unreadable_segment_is_treated_as_capped_rather_than_ignored() {
        assert_eq!(count_or_more(3, 50, &segs(&[1]), "spends_indexed"), "50+");
        let malformed = vec![serde_json::json!({ "bounties_indexed": null })];
        assert_eq!(count_or_more(3, 50, &malformed, "bounties_indexed"), "50+");
    }

    // ------------------------------------------------- numbers typeset as money

    /// STYLE.md principle 4: *"thousands separators always"*. Grouping changes
    /// no digit, which is why it may be done here and a unit may not.
    #[test]
    fn integers_are_grouped_in_threes_without_touching_a_digit() {
        assert_eq!(group("24310104.3351437286"), "24,310,104.3351437286");
        assert_eq!(group("999"), "999");
        assert_eq!(group("1000"), "1,000");
        assert_eq!(group("-1234567"), "-1,234,567");
        assert_eq!(group("0.5"), "0.5");
    }

    /// A value this function does not recognise is passed through UNTOUCHED
    /// rather than mangled into something that looks like a number.
    #[test]
    fn an_unrecognised_value_is_passed_through_rather_than_reformatted() {
        for odd in ["", "abc", "1e9", "0x2a", "12,345"] {
            assert_eq!(group(odd), odd, "{odd}");
        }
    }

    // ---------------------------------------------------------- the refusals

    fn leg(chain: &str, amount: Option<&str>, why: Option<&str>) -> LegView {
        LegView {
            chain: chain.to_string(),
            amount: amount.map(str::to_string),
            unknown_because: why.map(str::to_string),
            provenance: ProvenanceView {
                anchor_height: Some(19_000_000),
            },
        }
    }

    fn position(addable: bool, complete: bool, legs: Vec<LegView>) -> PositionView {
        PositionView {
            symbol: Some("HDX".into()),
            display: None,
            addable,
            complete,
            legs_of_unknown_size: legs.iter().filter(|l| l.amount.is_none()).count(),
            chains: legs.iter().map(|l| l.chain.clone()).collect(),
            legs,
        }
    }

    /// §7 rule 1: a withheld figure renders as a refusal WITH ITS REASON. The
    /// two conditions are independent and are NOT the same failure, so the
    /// sentence has to say which one — a reader told only "withheld" cannot act.
    #[test]
    fn an_incomplete_position_names_the_reason_and_offers_a_runnable_command() {
        let p = position(true, false, vec![leg("hydration", None, None)]);
        let out = withheld_total(&p).into_string();
        assert!(out.contains("total withheld"), "{out}");
        assert!(out.contains("unknown size"), "{out}");
        assert!(
            out.contains("treasury-holdings hydration"),
            "the command that would fix it is the product's whole argument: {out}"
        );
        assert!(
            !out.contains("disagree on decimals"),
            "this position IS addable; naming the other refusal would be a line \
             false about the row beside it: {out}"
        );
    }

    /// **THE COMMAND MUST BE RUNNABLE.** `treasury-holdings` takes ONE chain
    /// (`<chain> [height]`, and the second argument is parsed as a u64), so
    /// joining two chains with a space printed a command that fails to parse —
    /// which the API's own words call worse than none. One line per chain.
    #[test]
    fn two_unknown_legs_print_two_commands_rather_than_one_that_cannot_parse() {
        let p = position(
            true,
            false,
            vec![leg("hydration", None, None), leg("polkadot", None, None)],
        );
        let out = withheld_total(&p).into_string();
        assert!(out.contains("treasury-holdings hydration"), "{out}");
        assert!(out.contains("treasury-holdings polkadot"), "{out}");
        assert!(
            !out.contains("treasury-holdings hydration polkadot"),
            "that command does not parse: {out}"
        );
    }

    /// F2: where the API already worded the refusal, the page repeats it rather
    /// than inventing a second phrasing that will drift from the first.
    #[test]
    fn a_leg_that_carries_its_own_reason_has_that_reason_rendered_verbatim() {
        let p = position(
            true,
            false,
            vec![leg(
                "hydration",
                None,
                Some("no anchor: this pair has been seen MOVING but never read from state"),
            )],
        );
        let out = withheld_total(&p).into_string();
        assert!(out.contains("seen MOVING but never read"), "{out}");
        assert!(
            !out.contains("dotlens-node treasury-holdings"),
            "the API said it better; saying both is two implementations: {out}"
        );
    }

    /// The other arm, and it must not borrow the first one's wording.
    #[test]
    fn an_unaddable_position_names_the_units_rather_than_a_missing_leg() {
        let p = position(
            false,
            true,
            vec![
                leg("polkadot-asset-hub", Some("1"), None),
                leg("hydration", Some("2"), None),
            ],
        );
        let out = withheld_total(&p).into_string();
        assert!(out.contains("disagree on decimals"), "{out}");
        assert!(
            !out.contains("unknown size"),
            "every leg's size is known here: {out}"
        );
        assert!(
            !out.contains("treasury-holdings"),
            "there is no holdings run that fixes a units disagreement, and \
             offering one would send someone to do nothing: {out}"
        );
    }

    /// Rule 1 has no `@else` hole: a refusal always carries a reason, even the
    /// one that should be impossible.
    #[test]
    fn a_withheld_total_with_neither_condition_set_still_says_why_it_cannot_say() {
        let p = position(true, true, vec![leg("hydration", Some("1"), None)]);
        let out = withheld_total(&p).into_string();
        assert!(out.contains("cannot say why"), "{out}");
    }

    /// A refusal carries the API's OWN words. A UI that rewrites an endpoint's
    /// refusal is a second implementation of it, and the two drift.
    #[test]
    fn a_withheld_panel_repeats_the_endpoints_own_message_and_its_status() {
        let w = Withheld {
            status: StatusCode::NOT_FOUND,
            message: "unknown network 'nope' — registered networks: [\"polkadot\"]".into(),
        };
        let out = w.render().into_string();
        assert!(out.contains("unknown network"), "{out}");
        assert!(out.contains("404"), "{out}");
    }

    // ------------------------------------------------------- the fourth state

    /// §8's first DO, and the one that prevents the revamp: a module the
    /// network does not declare renders as ***the network does this, dotlens
    /// does not model it*** — a SENTENCE, not an empty slot and not a promise.
    #[test]
    fn an_undeclared_module_gets_the_fourth_state_and_not_an_empty_slot() {
        let out = undeclared("Recent referenda", "governance", "kusama").into_string();
        assert!(out.contains("not modelled here"), "{out}");
        assert!(out.contains("governance"), "it names the module: {out}");
        assert!(out.contains("kusama"), "and the network: {out}");
        // The distinction the fourth state exists to draw.
        assert!(
            out.contains("not a statement that"),
            "it must not read as a claim about the network: {out}"
        );
        // §8 DO-NOT 2, checked rather than trusted.
        let lower = out.to_lowercase();
        for banned in ["coming soon", "not yet available", "under construction"] {
            assert!(!lower.contains(banned), "{banned} in {out}");
        }
        // and it still offers a destination (C8) — the registry itself.
        assert!(out.contains("/v1/chains"), "{out}");
    }

    // --------------------------------------------------------------- escaping

    /// maud escapes by default and this pins it, because the alternative is a
    /// chain that names itself `<script>` writing script into every page that
    /// lists chains. It is one assertion and it protects the whole surface.
    #[test]
    fn interpolated_chain_data_is_escaped_rather_than_injected() {
        let w = Withheld {
            status: StatusCode::BAD_REQUEST,
            message: "<script>alert(1)</script>".into(),
        };
        let out = w.render().into_string();
        assert!(!out.contains("<script>"), "{out}");
        assert!(out.contains("&lt;script&gt;"), "{out}");
    }

    // ------------------------------------------------------------ the tokens

    /// The stylesheet is TOKENS.md v1.1 §0 and not a palette this file invented.
    /// If a token changes there, this fails and someone has to look.
    #[test]
    fn the_stylesheet_carries_the_ledger_tokens_verbatim() {
        for token in [
            "--bg:#F4F4F3",
            "--accent:#7B0DAF",
            "--warn:#A34E22",
            "--up:#1E7F5C",
        ] {
            assert!(style::CSS.contains(token), "TOKENS.md v1.1 §0: {token}");
        }
        // STYLE.md's hard bans, checked rather than trusted.
        for banned in ["gradient", "backdrop-filter", "box-shadow:0", "Inter\""] {
            assert!(
                !style::CSS.contains(banned),
                "the anti-slop contract bans {banned}"
            );
        }
        // §8: hover may only ENHANCE, so a no-pointer path must exist.
        assert!(style::CSS.contains("@media (hover:none)"));
        // §8: the narrow-viewport contract, both breakpoints.
        assert!(style::CSS.contains("max-width:1023px"));
        assert!(style::CSS.contains("max-width:639px"));
        // §8: a data table SCROLLS rather than reflowing, which is what makes
        // the sticky identifier column reachable at all.
        assert!(style::CSS.contains("min-width:max-content"));
        assert!(style::CSS.contains("--tap-min"));
    }

    /// STYLE.md v1.1 §14, *"Touch targets >=44px"*. The sibling test above
    /// asserts only that `--tap-min` EXISTS, and it passed while six
    /// affordances sat at 20-26px, because the token was applied to the nav
    /// links alone — a token being defined is not a token being applied.
    /// Measured at 390px in a browser, which is the only way this was ever
    /// going to be found.
    ///
    /// Rows are covered separately by `--row-h:44px` in the same block.
    #[test]
    fn every_link_affordance_meets_the_tap_minimum_at_touch_density() {
        // the narrow block, which is where §14 sets touch density.
        let narrow = style::CSS
            .split("@media (max-width:639px){")
            .nth(1)
            .expect("§8 narrow breakpoint");
        let narrow = &narrow[..narrow.find("\n}").expect("the block closes")];

        assert!(narrow.contains("--row-h:44px"), "the ROWS: {narrow}");
        for affordance in [".logo", ".strip-to", ".panel-to"] {
            assert!(
                narrow.contains(affordance),
                "{affordance} is a touch target and is not in the narrow block"
            );
        }
        assert!(
            narrow.contains("min-height:var(--tap-min)"),
            "a height, not just the token: {narrow}"
        );
        // an inline anchor's box is its LINE box, so min-height does nothing
        // until it is laid out as a flex box. Without this the rule is present
        // and inert, which is the failure mode this test exists to catch.
        assert!(narrow.contains("display:inline-flex"), "{narrow}");

        // AND the no-pointer path, because the minimum is a claim about the
        // INPUT DEVICE: a tablet at 768px is above the narrow breakpoint and
        // still has no pointer. Width alone leaves it at 20px.
        let touch = style::CSS
            .split("@media (hover:none){")
            .nth(1)
            .expect("§8 the no-pointer path");
        let touch = &touch[..touch.find("\n}").expect("the block closes")];
        for affordance in [".logo", ".strip-to", ".panel-to", ".nav-links a"] {
            assert!(touch.contains(affordance), "{affordance}: {touch}");
        }
        assert!(touch.contains("min-height:var(--tap-min)"), "{touch}");
    }

    /// TOKENS.md §0's layout invariant, *"learned the hard way"*: without both
    /// halves a wide nowrap table forces the whole page wider than the viewport
    /// and the right edge becomes unreachable.
    #[test]
    fn the_layout_invariant_is_present_in_both_halves() {
        assert!(style::CSS.contains("minmax(0,1fr) 380px"));
        assert!(style::CSS.contains(".wrap>*{min-width:0}"));
        assert!(style::CSS.contains("overflow-x:auto"));
    }
}
