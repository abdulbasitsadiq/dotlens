# Vendored typefaces

Two files belong here, and the build will not compile without them —
`crates/api/src/web/fonts.rs` `include_bytes!`s both.

| file | face | licence |
|---|---|---|
| `ServerMono-Regular.woff2` | data face — identifiers, numbers, tables | OFL 1.1 |
| `SchibstedGrotesk-Variable.woff2` | UI face — labels, prose, navigation | OFL 1.1 |
| `OFL.txt` | the licence text | required |

`OFL.txt` is **not optional**. OFL 1.1 requires the licence to accompany the
fonts in any redistribution, and serving a binary over HTTP from our origin is
redistribution.

## Getting them

```bash
cd crates/api/assets/fonts

# Server Mono — internet-development/www-server-mono, OFL-1.1, single weight.
curl -fL -o ServerMono-Regular.woff2 \
  https://cdn.jsdelivr.net/gh/internet-development/www-server-mono@latest/public/fonts/ServerMono-Regular.woff2
curl -fL -o OFL.txt \
  https://raw.githubusercontent.com/internet-development/www-server-mono/main/LICENSE.md

# Schibsted Grotesk — Google Fonts, OFL-1.1, VARIABLE weight axis, which is why
# 400/500/600/700 cost one file. The UA matters: without a modern one the API
# serves .ttf instead of .woff2.
#
# READ THE NEXT SECTION BEFORE CHANGING THIS COMMAND. Google serves ONE FILE PER
# UNICODE RANGE and `latin` is NOT the first of them.
curl -fsL -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120 Safari/537.36' \
  'https://fonts.googleapis.com/css2?family=Schibsted+Grotesk:wght@400..700&display=swap' \
  | awk '/\/\* latin \*\//{f=1} f&&/https:.*\.woff2/{print; exit}' \
  | grep -oE 'https://fonts\.gstatic\.com[^)]+\.woff2' \
  | xargs curl -fL -o SchibstedGrotesk-Variable.woff2
```

**`-f` matters on every one of them.** Without it a 404 writes an HTML error page
to the target and the build embeds it. `cargo test` catches that — the tests in
`fonts.rs` assert the `wOF2` magic number and a plausible size — but the failure
is much easier to read at download time.

## THE SUBSET TRAP — the reason this file is longer than it looks

The first version of the command above ended `| head -1`, and **it selected the
wrong font.** Google Fonts does not serve one file per family; it serves **one
file per unicode-range**, and for this family the CSS lists `latin-ext` FIRST:

```
/* latin-ext */  unicode-range: U+0100-02BA, U+02BD-02C5, ...   <- 20,844 bytes
/* latin     */  unicode-range: U+0000-00FF, U+0131, ...        <- 46,864 bytes
```

`latin-ext` **excludes basic latin entirely** — no `A–Z`, no `a–z`, no `0–9`.
Vendoring it means every ordinary character on the page falls back to
`system-ui`, silently, while the file itself is a perfectly valid 20 KB woff2.

**All three tests in `fonts.rs` passed on that file.** Valid `wOF2` magic, well
over the size floor, paths matching the router. This is PATTERNS C5 in a third
costume — *a token being DEFINED is not a token being APPLIED* — with the twist
that here the artefact was not merely undersized but **the wrong artefact
altogether, and every assertion about it was true.**

So the command selects the `/* latin */` block by name, and `fonts.rs` pins the
exact byte length of each face (see `the_faces_are_the_exact_bytes_that_were_verified`).
A re-download that lands a different cut fails the build instead of shipping.

## Coverage, measured rather than assumed

Checked with `fontTools` at vendoring time, 2026-08-24:

| face | glyphs | codepoints | axes |
|---|---|---|---|
| Server Mono | 604 | 520 | none — single weight, as TOKENS.md §0 asks |
| Schibsted Grotesk | 287 | 227 | `wght 400–900` |

Both cover **all printable ASCII plus `—` and `·`**, which together are every
character the web surface renders today (`§` and `^` appear only in comments —
Server Mono has neither, and that costs nothing until something renders one).

Beyond that range — accented names, Cyrillic, CJK in on-chain identity or
referendum titles — the page falls back to `system-ui` mid-string. That is a
known, uniform policy rather than an oversight: no single file was ever going to
cover on-chain text, and `latin-ext` is an arbitrary place to stop. Add the
second file with a `unicode-range` descriptor when a reader actually needs it —
same rule as an index, a class arrives with its reader.

## Changing a face

Bump `font_revision!()` in `crates/api/src/web/fonts.rs` **and** the two paths in
`crates/api/src/web/style.rs`'s `@font-face` blocks, **and** the pinned byte
length. A test pins each of those against the others, so bumping one alone fails
rather than silently falling back to `system-ui`. The URLs are served
`immutable`, which is honest only because the revision is in the path.

## Subsetting

Not done by us, deliberately. Note that the Google file above **is** already a
subset — upstream's, not ours, which is the distinction that matters: the bytes
are what Google published at that URL. Cutting them further needs `fonttools` in
the build path and makes the vendored bytes something we produced, which is a
provenance cost for a size win nobody has measured yet. Revisit with a number.
