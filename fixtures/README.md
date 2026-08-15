# fixtures/

Raw-block fixtures driving decode tests (ARCHITECTURE.md §15: decode determinism
is tested against fixtures for every supported runtime version).

## `synthetic/`
Hand-written raw-block envelopes used by Phase 0 to prove the pipeline shape
(raw → canonical → API with lineage). The `scale_hex` payloads in synthetic
fixtures are NOT valid SCALE — Phase 0's decoder consumes the envelope's
fixture-time decode fields. Addresses used are real, verified ones (e.g.
`13UVJ…hFsTB` = Treasury `py/trsry`) so downstream labeling tests are honest.

## Phase 1 TODO (blocked in this sandbox: RPC egress unavailable)
Capture REAL fixtures with the capture tool (to be built in Phase 1):
- polkadot: one block per spec_version era we support, starting with the
  migration boundary (last pre-migration governance block, 2025-11-04) and one
  recent minimal-relay block
- polkadot-asset-hub: first post-migration block, one ref-1828-era block
  (spec ~2.0.5+, revive live), one current block
- each fixture = original SCALE bytes (block + events) + metadata blob for its
  spec_version, stored under `real/{chain}/{spec_version}/`

Real fixtures replace the synthetic decode path; synthetic envelopes stay for
pipeline-shape tests.
