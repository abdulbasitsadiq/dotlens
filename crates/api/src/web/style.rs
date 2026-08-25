//! The stylesheet, as one `const`, served from `/assets/dotlens.css`.
//!
//! # WHY A `const` AND NOT A FILE ON DISK
//!
//! A file needs `tower-http`'s `ServeDir`, a path that resolves the same in a
//! container as in `cargo run`, and a deployment step that remembers to copy it.
//! A `const` is compiled in, has no path and cannot be missing at runtime. It is
//! also the only shape that keeps the whole surface inside `cargo test`.
//!
//! It is served `max-age=3600` and **not** by content hash, so a deploy can be
//! up to an hour ahead of a cached stylesheet. That is a real limitation and it
//! is stated rather than dressed up: nothing here emits an ETag, and the fix is
//! a hashed path, which is a slice of its own.
//!
//! # THIS IS TOKENS.md v1.1 §0, NOT A NEW PALETTE
//!
//! Every value below is copied from `style/TOKENS.md` §0 (the "Ledger" palette,
//! decided 2026-08-16) or from §5/§8. **The tokens are binding and this file may
//! not invent one.** Where TOKENS.md gives two generations of a value — §0 is
//! v1.1 and §1–§4 are v1.0 — §0 wins, because §0 says so in its own heading.
//!
//! **LIGHT ONLY, DELIBERATELY.** TOKENS.md §0 carries the line *"dark theme: NOT
//! yet re-derived for the ledger direction — §1–3 below are the v1.0 dark
//! values, to be revisited"*. Shipping a dark theme from the v1.0 values would
//! be shipping a palette the design language has withdrawn, and STYLE.md's own
//! ban is on *"neon-on-dark treatment"* rather than on dark itself. So there is
//! no `prefers-color-scheme` block here and no `[data-theme=dark]`: one theme,
//! stated, until the ledger dark values exist.
//!
//! # NO EXTERNAL FONT REQUEST — AND NOW NO FALLBACK EITHER
//!
//! `style/candidate-a-ledger.html` pulls Schibsted Grotesk from Google Fonts and
//! Server Mono from jsDelivr, which is right for a mockup and wrong for the
//! product: it would be the first third-party request on a surface whose whole
//! pitch is that you can verify what it tells you.
//!
//! **Both faces are now VENDORED and served from this origin** — see `fonts.rs`
//! for the licences and for why the URLs carry a revision. The stacks below keep
//! their fallbacks, but they are a degradation path rather than the normal case:
//! composition A shipped rendering in `system-ui` and generic monospace, which
//! is roughly the worst-case reading of a design whose whole character is its
//! typography.

/// `text/css` for the whole surface.
pub const CSS: &str = r#"
/* ---------------------------------------------------------------- tokens
   TOKENS.md v1.1 §0 — the Ledger palette. Light is the default (and, for now,
   the only) theme. Values are copied, not re-derived. */
:root{
  --bg:#F4F4F3; --surface:#FFFFFF; --inset:#F0F0EF;
  --line:#E3E3E1; --line-2:#CFCFCC;
  --t1:#191A1C; --t2:#5D6167; --t3:#95999F;
  --up:#1E7F5C; --down:#B3403B;
  --warn:#A34E22; --warn-bg:#FAF1EA;
  --accent:#7B0DAF;
  --link:#20509E;
  --mono:"Server Mono","IBM Plex Mono",ui-monospace,monospace;
  --sans:"Schibsted Grotesk",system-ui,sans-serif;

  /* §5 space + radius. 4px grid; nothing rounder than 8px (§0 card radius). */
  --sp-1:4px; --sp-2:8px; --sp-3:12px; --sp-4:16px; --sp-5:24px; --sp-6:32px;
  --row-h:30px; --gutter:32px; --card-pad:20px; --tap-min:44px;
  --safe-top:env(safe-area-inset-top,0px);
  --safe-bottom:env(safe-area-inset-bottom,0px);
}

/* ------------------------------------------------------------------ faces
   Self-hosted, OFL 1.1, served from this origin — never a CDN. `fonts.rs`
   carries the licence provenance and the revision argument.

   `font-display:swap` and never `block`: a data page may not render invisible
   text while a face downloads. The fallback stacks below stay in the tokens as
   a degradation path. */
@font-face{
  font-family:"Server Mono";
  src:url("/assets/fonts/server-mono-1.woff2") format("woff2");
  font-weight:400;font-style:normal;font-display:swap;
}
@font-face{
  /* VARIABLE weight axis, which is why 400/500/600/700 cost one file. */
  font-family:"Schibsted Grotesk";
  src:url("/assets/fonts/schibsted-grotesk-1.woff2") format("woff2");
  font-weight:400 700;font-style:normal;font-display:swap;
}

/* ------------------------------------------------------------------ reset */
*{box-sizing:border-box;margin:0;padding:0}
html{-webkit-text-size-adjust:100%}
body{
  background:var(--bg); color:var(--t1);
  font:400 15px/1.5 var(--sans);
  -webkit-font-smoothing:antialiased;
}
a{color:var(--link);text-decoration:none}
a:hover{text-decoration:underline}
/* Keyboard is a first-class citizen (STYLE.md principle 6): focus is never
   removed, only restyled, and it uses the accent because that is what the
   accent is for. */
:focus-visible{outline:2px solid var(--accent);outline-offset:2px;border-radius:2px}
b,strong{font-weight:600}
h1,h2,h3{font-weight:600;letter-spacing:-.2px}

/* Numbers are typeset like money (STYLE.md principle 4). */
.mono{font-family:var(--mono);font-size:13.5px;font-variant-numeric:tabular-nums}
.num{font-family:var(--mono);font-variant-numeric:tabular-nums;text-align:right;white-space:nowrap}
.muted{color:var(--t2)}
.faint{color:var(--t3)}
.micro{font-size:11px;color:var(--t3)}
.unit{font-size:11px;color:var(--t3);font-family:var(--sans);margin-left:3px}
/* `--up`/`--down` are TOKENS.md's delta colours and stay defined as tokens,
   but no figure on this page is a delta, so no utility class for them exists
   yet. A class arrives with its reader. */
.warn-ink{color:var(--warn)}

/* ----------------------------------------------------------------- chrome
   STYLE.md principle 1: a thin frame, no hero, no marketing air. */
.nav{background:var(--surface);border-bottom:1px solid var(--line);
     padding-top:var(--safe-top)}
.nav-in{max-width:1400px;margin:0 auto;display:flex;align-items:center;gap:10px;
        min-height:56px;padding:0 var(--gutter);flex-wrap:wrap}
.logo{font-family:var(--mono);font-size:17px;letter-spacing:-.5px;color:var(--t1)}
.logo i{font-style:normal;color:var(--accent)}
.nav-links{display:flex;gap:2px;margin-left:14px;flex-wrap:wrap}
.nav-links a{color:var(--t2);font-weight:500;font-size:14.5px;padding:7px 11px;
             border-radius:6px;display:inline-flex;align-items:center;
             min-height:var(--tap-min)}
.nav-links a:hover{color:var(--t1);background:var(--inset);text-decoration:none}
.nav-links a[aria-current="page"]{color:var(--t1);font-weight:600}

/* ------------------------------------------------------------------ page */
.wrap{max-width:1400px;margin:0 auto;padding:var(--sp-5) var(--gutter) 64px}
.wrap>*{min-width:0}          /* TOKENS.md §0 layout invariant */
.page-head{display:flex;align-items:baseline;gap:var(--sp-3);flex-wrap:wrap;
           margin-bottom:var(--sp-4)}
.page-title{font-size:25px;font-weight:700;letter-spacing:-.45px}
/* C7: a declared page-level scope, pinned in the header where it stays visible
   while the numbers are read. Never a hidden default. */
.scope{font-family:var(--mono);font-size:13px;color:var(--t2)}

/* ----------------------------------------------------------------- panels
   TOKENS.md §6 "Panel". Panels never nest panels (which is also STYLE.md's
   "cardocalypse" ban). */
.grid{display:grid;grid-template-columns:minmax(0,1fr) 380px;gap:var(--sp-5);align-items:start}
.col{display:flex;flex-direction:column;gap:var(--sp-5);min-width:0}
.panel{background:var(--surface);border:1px solid var(--line);border-radius:8px}
.panel-head{display:flex;align-items:baseline;gap:var(--sp-3);
            padding:14px var(--card-pad);border-bottom:1px solid var(--line)}
.panel-title{font-size:15px;font-weight:600;letter-spacing:-.1px}
/* C8: every panel title links to its own full page — the mechanism that makes
   "choose what NOT to show" tractable, because anything that does not fit
   becomes a destination rather than a compromise. */
.panel-to{margin-left:auto;font-size:13px;white-space:nowrap}
.panel-body{padding:var(--card-pad)}
.panel-body.flush{padding:0}

/* ------------------------------------------------------------------ notes
   PREP §7 rule 9: `not_covered` is a PANEL element, not a footer. The UI
   research found no prior art for this in ten walked references — there is
   nothing to copy and nothing to soften toward, so it is rendered as text
   inside the panel it is about, at reading size and not as a tooltip. */
.notes{border-top:1px solid var(--line);background:var(--inset);
       padding:12px var(--card-pad);border-radius:0 0 8px 8px}
.notes-h{font-size:11px;letter-spacing:.4px;text-transform:uppercase;
         color:var(--t2);font-weight:600;margin-bottom:6px}
.notes ul{list-style:none}
.notes li{font-size:12px;line-height:1.55;color:var(--t2);
          padding-left:14px;position:relative;margin-bottom:5px}
.notes li:last-child{margin-bottom:0}
.notes li:before{content:"—";position:absolute;left:0;color:var(--t3)}
/* The strip is not a panel, so its coverage block needs its own frame — as a
   bare `.notes` it has a top border only and reads as belonging to the panels
   below it rather than to the figures above. */
.strip-notes{border:1px solid var(--line);border-radius:8px;
             margin-top:calc(-1 * var(--sp-5) + var(--sp-1));
             margin-bottom:var(--sp-5)}

/* --------------------------------------------------------------- refusal
   PREP §7 rule 1: a withheld figure renders as a refusal WITH ITS REASON,
   never as an empty chart, a dash, a blank or a zero. This is the element
   that makes that possible, so it is deliberately conspicuous. */
.refusal{background:var(--warn-bg);border:1px solid #EAD2C2;border-radius:6px;
         padding:12px 14px;color:#8A3F17;font-size:13px;line-height:1.55}
.refusal b{color:var(--warn)}
/* The command that would fix it. `--warn` rather than a darker shade of it: a
   review found #7A3714 here and it appears in neither TOKENS.md nor the
   reference mockup, which makes it exactly the invented value this file's own
   header forbids. */
.refusal .fix{display:block;margin-top:6px;font-family:var(--mono);font-size:13px;
              color:var(--warn)}
.refusal .fix + .fix{margin-top:2px}
/* A refusal inside a table cell must WRAP, or the `white-space:nowrap` that
   makes data columns scroll would stretch a two-sentence reason into one
   enormous line and take the table with it. Prose wraps; data does not. */
td .refusal{white-space:normal;max-width:34ch;font-weight:400}
td .refusal .fix{white-space:normal;word-break:break-word}

/* ------------------------------------------------------------------ table
   TOKENS.md §6 "Table": micro uppercase header, --row-h rows, bottom hairline
   only, no verticals, numeric right-aligned tabular. */
.scroll{overflow-x:auto}       /* §0 invariant: tables scroll INSIDE the card */
table{border-collapse:collapse;width:100%;min-width:max-content}
th{font-size:11px;letter-spacing:.4px;text-transform:uppercase;color:var(--t2);
   font-weight:600;text-align:left;padding:8px var(--card-pad);
   border-bottom:1px solid var(--line);white-space:nowrap}
th.num{text-align:right}
td{padding:7px var(--card-pad);border-bottom:1px solid var(--line);
   font-size:13.5px;vertical-align:baseline;height:var(--row-h);
   /* the containing block for the touch-target overlay in the narrow and
      no-pointer blocks below. Declared HERE, ahead of `.sticky1`, so that
      rule still wins for the first column — a sticky box is a containing
      block too, so the overlay works in both columns without redeclaring
      stickiness inside a media query. No visual effect on its own. */
   position:relative;
   /* §8: a data table SCROLLS, it does not reflow. Without this the cells
      wrap at 390px, the table never exceeds the viewport, `.scroll` never
      overflows and the sticky first column never engages. */
   white-space:nowrap}
tr:last-child td{border-bottom:0}
/* §8: the first column stays identifiable while the numbers scroll. */
.sticky1 th:first-child,.sticky1 td:first-child{
  position:sticky;left:0;background:var(--surface);border-right:1px solid var(--line-2)}
.sub{font-size:12px;color:var(--t2);line-height:1.5;margin-top:3px}

/* ------------------------------------------------------------ state words
   STYLE.md principle 2: colour means something or is not used. These are the
   four freshness states, and `at_decode_frontier` is deliberately NOT green —
   PREP §7 rule 7, it is not "current" and must never render as it. */
.state{font-family:var(--mono);font-size:12px;padding:1px 6px;border-radius:4px;
       border:1px solid var(--line);background:var(--inset);color:var(--t2);
       white-space:nowrap}
.state.halted{background:var(--warn-bg);border-color:#EAD2C2;color:#8A3F17;font-weight:600}
/* No colour at all on the two non-alarming states: an `at_decode_frontier`
   badge tinted green would be the exact misreading the state word exists to
   prevent. */

/* ------------------------------------------------------------------ strip
   The freshness strip. §9's open question is whether it belongs at the top or
   the foot; PRODUCT's legibility contract says coverage goes at the FOOT, but
   a HALTED module is a warning rather than a coverage statement. Rendered top
   here, with the halt as the loud part — and the per-module detail is a
   destination, not a hover. */
.strip{background:var(--surface);border:1px solid var(--line);border-radius:8px;
       padding:12px var(--card-pad);display:flex;align-items:center;
       gap:var(--sp-3);flex-wrap:wrap;margin-bottom:var(--sp-5)}
.strip.alarm{background:var(--warn-bg);border-color:#EAD2C2}
.strip-k{font-size:11px;letter-spacing:.4px;text-transform:uppercase;
         color:var(--t2);font-weight:600}
/* A WARNING IS NOT A MICRO-LABEL. `--strip-k` is 11px uppercase, which is right
   for "indexed" and wrong for "a human is needed" — TOKENS.md §0 reserves
   11-12px for units, ticks and headers. Sentences get reading size. */
.strip-say{font-size:13.5px;font-weight:600;color:#8A3F17;margin-bottom:6px}
.strip-v{font-family:var(--mono);font-size:13.5px;font-variant-numeric:tabular-nums}
.strip-sep{color:var(--line-2)}
.strip-to{margin-left:auto;font-size:13px}

/* Small structural helpers. These exist so no template carries an inline
   `style=` attribute: a design system with escape hatches in the markup is a
   design system that drifts, and STYLE.md's regression watch is a list of
   exactly that happening. */
.block{display:block}
.line{padding:2px 0}
.big{font-size:20px}   /* TOKENS §0 hero-stat band (26-28px) scaled to a panel */
.tight{padding-top:12px}
.left{text-align:left}
.anchor{font-size:12px}   /* TOKENS §0 micro band: 11-12px */
.plain{list-style:none;margin-top:12px}
.plain li{padding:6px 0}

/* ------------------------------------------------------------- footer */
.foot{max-width:1400px;margin:0 auto;padding:var(--sp-5) var(--gutter);
      border-top:1px solid var(--line);color:var(--t2);font-size:12.5px;
      line-height:1.6;padding-bottom:calc(var(--sp-5) + var(--safe-bottom))}


/* =====================================================================
   NARROW VIEWPORTS — TOKENS.md §8. Token overrides only; no component is
   redesigned and no page gets a mobile-only variant. Review at 390 and 768.
   ===================================================================== */
@media (max-width:1023px){
  :root{--gutter:20px}
  .grid{grid-template-columns:minmax(0,1fr)}
}
@media (max-width:639px){
  :root{--gutter:16px;--card-pad:14px;--row-h:44px}
  /* §8: the type scale does NOT shrink on narrow. The page title is the one
     stated exception, 25px -> 21px. Micro stays 11px and mono data 13.5px. */
  .page-title{font-size:21px}
  td,th{padding-top:11px;padding-bottom:11px}
  .nav-in{min-height:auto;padding-top:10px;padding-bottom:10px}
  /* STYLE.md v1.1 §14 "Touch targets >=44px": `--row-h` covers the ROWS and
     `--tap-min` was on the nav links ONLY, so the wordmark and every "to"
     link kept their pointer-density 20-26px box at touch density. Measured
     at 390px, not reasoned about: six affordances failed the rule.
     `min-height` alone is not enough on an inline anchor, whose box is its
     line box — it has to be laid out as a flex box before a height applies.
     The 4px grid, the hairlines and the radii do not move (§14's proviso). */
  .logo,.strip-to,.panel-to{display:inline-flex;align-items:center;
     min-height:var(--tap-min)}
  /* A LINK INSIDE A TABLE CELL is the case the rule above cannot reach, and
     it did not exist to be measured until a populated table was rendered:
     every panel had only ever shown its refusal row. At 390px with real rows
     the spend ids render a 25x14 box inside a cell that is already 44px.
     `inline-flex` + `min-height` — the fix the affordances above needed —
     would push that cell to ~66px and spend the density TOKENS §5 asks for,
     so the HIT AREA is expanded to the cell the link already sits in rather
     than the box being grown. Assumes ONE link per cell: a second would sit
     under the first one's overlay. */
  td>a::after{content:"";position:absolute;inset:0}
}

/* STYLE.md's hard ban: information conveyed by hover alone. Hover may only
   ENHANCE, so where there is no pointer every hover-revealed control is
   permanently visible instead. */
@media (hover:none){
  /* Every link is already permanently visible — nothing on this surface is
     revealed by hover, which is the rule. What changes without a pointer is
     that link AFFORDANCE cannot be hover-discovered, so it is always drawn. */
  a{text-decoration:underline}
  /* §14's touch minimum is a statement about the INPUT DEVICE, not the
     viewport: a tablet at 768px sits above the narrow breakpoint and still
     has no pointer, so the width-scoped rule above would leave it at 20px.
     Measured: `(hover:none)` is false at 768px in a desktop browser, which
     is exactly why width alone cannot answer this. */
  .logo,.strip-to,.panel-to,.nav-links a{display:inline-flex;align-items:center;
     min-height:var(--tap-min)}
  /* A LINK INSIDE A TABLE CELL is the case the rule above cannot reach, and
     it did not exist to be measured until a populated table was rendered:
     every panel had only ever shown its refusal row. At 390px with real rows
     the spend ids render a 25x14 box inside a cell that is already 44px.
     `inline-flex` + `min-height` — the fix the affordances above needed —
     would push that cell to ~66px and spend the density TOKENS §5 asks for,
     so the HIT AREA is expanded to the cell the link already sits in rather
     than the box being grown. Assumes ONE link per cell: a second would sit
     under the first one's overlay. */
  td>a::after{content:"";position:absolute;inset:0}
}

/* Motion is feedback only, <=150ms (STYLE.md principle 8), and respects the
   user's stated preference. */
@media (prefers-reduced-motion:reduce){
  *{animation:none!important;transition:none!important}
}
"#;
