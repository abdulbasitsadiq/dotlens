# dotlens

The Polkadot databank: indexer, explorer, treasury intelligence, and governance
simulation. Specs live one level up: `../ARCHITECTURE.md` (read first),
`../ECOSYSTEM.md`, `../ROADMAP.md`, `../CLAUDE.md`, `../style/`.

## Status: Phase 0 (foundation skeleton)

What exists: workspace crates (`registry`, `canonical`, `raw-store`, `ingest`,
`adapter-substrate`, `api`, `dotlens-node`), registry seeds for Polkadot +
Asset Hub with domain residency across the Nov 2025 migration, core + substrate
migrations, write-once raw store, checkpointed idempotent ingestion, pure
fixture-driven decode with lineage, REST API.

## Quickstart

```sh
# 1. toolchain (once)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# 2. infra (optional in Phase 0 — node runs without a DB)
cp .env.example .env
docker compose up -d

# 3. tests — THE Phase 0 exit criterion
cargo test --workspace

# 4. run: migrations (if DATABASE_URL set) + fixture ingest + API
cargo run -p dotlens-node
curl localhost:8080/v1/blocks/polkadot-asset-hub/19000001   # note "lineage"
curl "localhost:8080/v1/domains/polkadot/governance?at=2025-06-01T00:00:00Z"
curl "localhost:8080/v1/domains/polkadot/governance"        # → asset hub
```

## Phase 0 exit criterion (ROADMAP.md)

`docker compose up` + `cargo test` green; a fixture block round-trips
raw → canonical → API with full lineage (runtime_version, decoder_version,
raw_location). Covered by `crates/api` tests + `cargo run -p dotlens-node`.

## Notes

- This workspace was authored in an offline sandbox: the first `cargo build`
  on a real machine may surface small dependency/API drift — fix forward,
  nothing here is load-bearing on exact versions.
- Real SCALE decoding (subxt ≥0.50 + frame-decode against archived metadata)
  replaces the Phase 0 envelope decoder in Phase 1 — same interfaces, bumped
  `DECODER_VERSION`. See `fixtures/README.md` for the real-fixture capture plan.
