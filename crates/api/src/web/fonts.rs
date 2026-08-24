//! The two typefaces, compiled into the binary.
//!
//! # WHY SELF-HOSTED AND NOT A CDN
//!
//! `style/candidate-a-ledger.html` pulls Schibsted Grotesk from Google Fonts and
//! Server Mono from jsDelivr. That is right for a mockup and wrong for the
//! product: it would be **the first third-party request on a surface whose whole
//! pitch is that you can verify what it tells you**, and it makes the page's
//! appearance depend on two hosts that have no obligation to us. It is also a
//! privacy fact about every reader, decided by us, on their behalf.
//!
//! Same argument as `style.rs`'s: compiled in, no path to resolve, no deployment
//! step that can be forgotten, and the whole surface stays inside `cargo test`.
//!
//! # LICENCES — BOTH OFL 1.1, AND THE TEXT TRAVELS WITH THE BYTES
//!
//! STYLE.md's typography section says *"Everything OFL-licensed — safe for an
//! open-source repo. (Berkeley Mono considered and rejected: license
//! incompatible with open source.)"* Vendoring the binaries is what makes that
//! claim load-bearing rather than aspirational, so it was **checked rather than
//! trusted** before these files landed:
//!
//! - **Server Mono** — `internet-development/www-server-mono`, OFL-1.1 (GitHub's
//!   own licence detection reports `OFL-1.1 license`). Released 2024 by
//!   Internet Development Studio Company; designers Tim Vanhille and Matthieu
//!   Salvaggio. **Single weight**, which is exactly what TOKENS.md §0 asks for:
//!   *"single weight: hierarchy via size+ink"*.
//! - **Schibsted Grotesk** — Google Fonts, OFL-1.1. Variable weight axis, so one
//!   file covers the 400/500/600/700 the design language uses.
//!
//! **`assets/fonts/OFL.txt` ships beside them and is not optional**: OFL 1.1
//! requires the licence to accompany the fonts in any redistribution, and a
//! binary served over HTTP from our origin is redistribution.
//!
//! # THE FAMILY NAME IS OURS, AND IT IS NOT UPSTREAM'S
//!
//! Server Mono's own `@font-face` declares `font-family: 'ServerMono'`. Ours
//! declares `"Server Mono"`, because that is what TOKENS.md §0 names and the
//! token is the contract. Self-hosting is what makes that free — the family name
//! is whatever the `@font-face` says, and nothing downstream needs to know.
//!
//! # THE PATHS CARRY A VERSION, AND THAT IS §0d's LESSON APPLIED
//!
//! The composition-A verification lost time to exactly one thing:
//! `/assets/dotlens.css` is served `max-age=3600` under a stable path, so after
//! rebuilding, **the browser kept measuring the OLD stylesheet and
//! `location.reload(true)` did not clear it**. A font is far more cacheable than
//! a stylesheet and would be far worse to get stuck. So these paths carry a
//! version segment ([`FONT_REVISION`]) and are served `immutable`: **to change a
//! face, bump the revision and the URL changes with it.** No purge, no guessing
//! whether you are looking at the new one.

use axum::{
    http::header,
    response::{IntoResponse, Response},
};

/// Bump this when a font FILE changes, and every URL changes with it.
///
/// The revision is in the PATH rather than in a query string on purpose: some
/// caches and proxies ignore query strings when deciding identity, and the whole
/// point is that the new bytes are a different resource.
///
/// It is a `macro_rules!` rather than a `const` so that the paths below can be
/// built from it **at compile time** — `concat!` takes literals and not
/// constants. That is the difference between a revision that is SINGLE-SOURCED
/// and one that is merely DECLARED next to two hand-written paths somebody has
/// to remember to edit in step. Clippy found the earlier version: a `pub const`
/// in a private module that only the tests ever read is dead code, and dead code
/// is what a decorative constant IS.
macro_rules! font_revision {
    () => {
        "1"
    };
}

/// The data face. TOKENS.md §0: identifiers, numbers, tables, code.
pub const SERVER_MONO: &[u8] = include_bytes!("../../assets/fonts/ServerMono-Regular.woff2");

/// The UI face. TOKENS.md §0 v1.1 — Schibsted Grotesk, variable weight, which is
/// why 400/500/600/700 cost one file rather than four.
pub const SCHIBSTED_GROTESK: &[u8] =
    include_bytes!("../../assets/fonts/SchibstedGrotesk-Variable.woff2");

/// `/assets/fonts/server-mono-<rev>.woff2`, assembled from [`font_revision`].
pub const SERVER_MONO_PATH: &str =
    concat!("/assets/fonts/server-mono-", font_revision!(), ".woff2");
/// `/assets/fonts/schibsted-grotesk-<rev>.woff2`, assembled from [`font_revision`].
pub const SCHIBSTED_GROTESK_PATH: &str = concat!(
    "/assets/fonts/schibsted-grotesk-",
    font_revision!(),
    ".woff2"
);

/// A font, served immutable.
///
/// `immutable` is honest here in a way it is not for the stylesheet: the URL
/// contains the revision, so these bytes genuinely never change at this address.
/// That is the difference between a long TTL that is safe and one that is a trap.
fn woff2(bytes: &'static [u8]) -> Response {
    (
        [
            (header::CONTENT_TYPE, "font/woff2"),
            (header::CACHE_CONTROL, "public, max-age=31536000, immutable"),
        ],
        bytes,
    )
        .into_response()
}

pub async fn server_mono() -> Response {
    woff2(SERVER_MONO)
}

pub async fn schibsted_grotesk() -> Response {
    woff2(SCHIBSTED_GROTESK)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// **A FONT EMBEDDED IS NOT A FONT THAT ARRIVED**, which is §0a's lesson in
    /// a new place: the composition-A verification found six affordances under
    /// the 44px minimum while a test asserting `CSS.contains("--tap-min")`
    /// passed throughout, because *a token being DEFINED is not a token being
    /// APPLIED*.
    ///
    /// The same shape is available here and it is worse, because it is silent:
    /// rename a route or mistype a `src` and the browser falls back to
    /// `system-ui` with no error anywhere — the page simply looks wrong, and
    /// looking wrong is not something `cargo test` notices. So the CSS's `src`
    /// and the router's path are asserted to be the SAME STRING.
    #[test]
    fn the_stylesheet_asks_for_exactly_the_paths_the_router_serves() {
        assert!(
            super::super::style::CSS.contains(SERVER_MONO_PATH),
            "@font-face src must be the routed path: {SERVER_MONO_PATH}"
        );
        assert!(
            super::super::style::CSS.contains(SCHIBSTED_GROTESK_PATH),
            "@font-face src must be the routed path: {SCHIBSTED_GROTESK_PATH}"
        );
        // The two paths are BUILT from `font_revision!()` through `concat!`, so a
        // bump cannot leave one of them behind — that half is now structural
        // rather than asserted. The stylesheet's `src`, though, is a hand-written
        // literal, and it is precisely the half that can drift. The two asserts
        // above are the pin: bump the revision and they fail until the CSS moves
        // with it, which is the failure we want instead of a silent fallback.
        //
        // Nothing here may reference an UNREVISIONED path either.
        assert!(
            !super::super::style::CSS.contains("/assets/fonts/server-mono.woff2"),
            "an unrevisioned font path defeats `immutable`"
        );
    }

    /// The embedded bytes are actually woff2 and not, say, an HTML error page a
    /// `curl` without `-L` wrote to the file. `wOF2` is the magic number.
    ///
    /// This is the check that makes the download step in the runbook safe to
    /// follow without thinking: a redirect page, a 404 body or a `.ttf` renamed
    /// to `.woff2` all fail here rather than shipping.
    #[test]
    fn the_embedded_faces_are_woff2_and_not_something_that_downloaded_wrong() {
        for (name, bytes) in [
            ("Server Mono", SERVER_MONO),
            ("Schibsted Grotesk", SCHIBSTED_GROTESK),
        ] {
            assert!(
                bytes.len() > 4_000,
                "{name} is implausibly small — a 404 body?"
            );
            assert_eq!(
                &bytes[0..4],
                b"wOF2",
                "{name} is not woff2. A .ttf renamed, or an HTML page saved by a \
                 redirect that was not followed."
            );
        }
    }

    /// TOKENS.md §0 names the two families and this file may not rename them.
    #[test]
    fn the_declared_families_are_the_ones_the_tokens_name() {
        let css = super::super::style::CSS;
        assert!(css.contains("font-family:\"Server Mono\""));
        assert!(css.contains("font-family:\"Schibsted Grotesk\""));
        // `swap`, never `block`: a data page may not render invisible text while
        // a face downloads. STYLE.md's voice is terse, not absent.
        assert!(css.contains("font-display:swap"));
    }

    /// **THESE ARE THE BYTES THAT WERE CHECKED, NOT MERELY *SOME* WOFF2.**
    ///
    /// The runbook's first Schibsted download ended `| head -1`, and Google
    /// Fonts serves **one file per unicode-range** rather than one per family.
    /// The first block for this family is `latin-ext`, whose range starts at
    /// `U+0100`: no `A-Z`, no `a-z`, no digits. It is a perfectly valid 20 KB
    /// woff2 and **every other assertion in this module passed on it** — magic
    /// number, size floor, paths matching the router. The page would have
    /// rendered wholly in `system-ui`, having successfully downloaded a font to
    /// do it, with no error anywhere.
    ///
    /// So: a magic number proves a FORMAT and cannot prove the artefact is the
    /// RIGHT one. The exact length is pinned instead, verified against
    /// `fontTools` cmap coverage at vendoring time (`assets/fonts/README.md`
    /// records the measurement). A re-download that lands a different cut fails
    /// here rather than shipping.
    #[test]
    fn the_faces_are_the_exact_bytes_that_were_verified() {
        assert_eq!(
            SERVER_MONO.len(),
            24_812,
            "Server Mono is not the vendored file"
        );
        assert_eq!(
            SCHIBSTED_GROTESK.len(),
            46_864,
            "Schibsted Grotesk is not the vendored file. The `latin-ext` cut is \
             20,844 bytes and is exactly what taking the FIRST @font-face gives \
             you; the `latin` cut is this one."
        );
    }
}
