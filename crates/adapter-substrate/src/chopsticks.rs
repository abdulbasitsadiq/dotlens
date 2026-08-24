//! The JSON-RPC vocabulary of a running chopsticks fork (Phase 3, slice 8).
//!
//! A fork answers the ORDINARY Substrate RPC surface — so `SubstrateSource`
//! works against it unchanged, and that is how blocks, storage and runtime
//! versions are read back. What it adds is a handful of `dev_*` methods, and
//! those are what this file owns.
//!
//! IT IS A CLIENT, NOT A HARNESS. Spawning the process, waiting for it to come
//! up and killing it afterwards is wiring and lives in
//! `dotlens_node::fork_run`; the split is the same one `dryrun.rs` has under
//! `sim_run.rs`.
//!
//! ---------------------------------------------------------------------------
//! THE SIX FACTS THIS FILE IS BUILT ON, each read from chopsticks' own source
//! rather than from its README. (5) and (6) were added after a drill measured
//! the file's own claims to be wrong; (6) is the one that cost a slice.
//!
//!   1. `dev_setStorage([values, blockHash?])` accepts a TOP-LEVEL ARRAY of
//!      `[keyHex, valueHex | null]` pairs and applies them verbatim — no pallet
//!      names, no type resolution, no re-encoding. That raw form is the one this
//!      project uses, because every byte we inject was encoded against the
//!      runtime's own metadata by `adapter_substrate::fork` and handing it to a
//!      second encoder to re-derive would be two encoders that can disagree. It
//!      returns the hash of the block it layered onto.
//!   2. `dev_newBlock([{count: 1}])` builds one block and returns its hash. It
//!      returns NO diff, which is why (3) exists.
//!   3. `dev_runBlock([{parent, block: {header, extrinsics}, includeRaw: true}])`
//!      re-runs a block on top of a parent and returns per-phase RAW storage
//!      diffs. It is a PLUGIN, so a chopsticks built with plugins disabled does
//!      not have it — and that is reported as `diff_status: unavailable` rather
//!      than as a failure, because the events and the dispatch outcome are still
//!      real without it.
//!
//!      MEASURED AT VERIFICATION, and it is the tier's sharpest limit: on a
//!      PARACHAIN this method cannot read the block `dev_newBlock` just built.
//!      `dev_newBlock` omits `set_validation_data` from the extrinsic list (it
//!      mocks parachain state directly — the built block's first extrinsic is
//!      `timestamp.set`), while `dev_runBlock` re-applies extrinsics for real
//!      through `BlockBuilder_apply_extrinsic`, so the runtime traps with a
//!      wasm `unreachable`. Proven to be about composability and not about this
//!      client: the SAME harness re-runs a REAL Asset Hub block through the
//!      same method and returns four phases. That is `diff_status: refused`.
//!   4. `dev_runBlock` deliberately EXCLUDES `System.Events` and
//!      `System.ExtrinsicData` from the diff it returns. That is convenient
//!      rather than lossy — the events are read from the built block itself,
//!      through the same decoder every indexed block goes through — but it has
//!      to be said out loud, or a reader wonders why the one storage item they
//!      can name is missing from the diff.
//!   5. There is NO shutdown RPC and no timeout option. A harness is ended by
//!      killing the process, which is why `fork_run` owns its lifetime.
//!   6. **`dev_dryRun` RETURNS THE `apply_extrinsic` PHASE'S DIFF AND NOTHING
//!      ELSE**, which contradicts what slice 9 wrote here and is the reason
//!      slice 10 exists. Read from `blockchain/block-builder.ts`: `initNewBlock`
//!      calls `Core_initialize_block` and each inherent and CONSUMES every
//!      response into a storage layer (`newBlock.pushStorageLayer().setAll(
//!      resp.storageDiff)`); `dryRunExtrinsic` then returns the single
//!      `TaskCallResponse` from `BlockBuilder_apply_extrinsic`. Earlier phases
//!      are in the block's STATE — which is why the runtime sees an injected
//!      scheduler task and dispatches it — and in no returned value.
//!
//!      Slice 9's claim that this makes it "structurally impossible for the
//!      events and the diff to describe different executions" was exactly
//!      backwards, and the events are why it looked true: `apply_extrinsic`
//!      WRITES `System.Events`, reading the current list and appending, so the
//!      value in the diff is the whole block's events while every other key in
//!      it is one extrinsic's. Measured: the events blob grew 464 → 862
//!      characters between a `not_dispatched` and an `executed` run of the same
//!      subject while the diff key set stayed IDENTICAL at 12 keys.
//!
//!      The other three vehicles do not help and it is worth writing down so
//!      nobody tries: `hrmp`/`dmp`/`ump` go through `dryRunInherents`, which
//!      merges `initNewBlock`'s `layers` — and `layers` collects the INHERENT
//!      layers only, because it is declared after the initialize call. There is
//!      no `preimage` vehicle on the RPC at all (the CLI's `dryRunPreimage`,
//!      which DOES get a whole-block diff by putting every phase in ONE
//!      `runTask`, writes HTML and calls `process.exit(0)`).
//!
//!      Recorded as [`crate::fork::DIFF_STATUS_EXTRINSIC_ONLY`], not worked
//!      around: nothing here fabricates the missing phase.

use serde_json::Value;
// `RpcParams` lives under `subxt_rpcs::client`, NOT at the crate root (the
// root re-exports only `RpcClient`/`RpcClientT` and the two method sets).
use subxt::rpcs::{client::RpcParams, RpcClient};

pub const DEV_SET_STORAGE: &str = "dev_setStorage";
pub const DEV_NEW_BLOCK: &str = "dev_newBlock";
pub const DEV_RUN_BLOCK: &str = "dev_runBlock";
pub const DEV_DRY_RUN: &str = "dev_dryRun";
/// `state_getKeysPaged` — how the live agenda's key space is read, which is the
/// evidence `fork::decide_agenda_anchor` decides on.
pub const STATE_GET_KEYS_PAGED: &str = "state_getKeysPaged";

#[derive(Debug, thiserror::Error)]
pub enum ChopsticksError {
    #[error("chopsticks rpc {method}: {message}")]
    Rpc { method: String, message: String },
    /// The method is not there at all. Distinguished from any other RPC error
    /// because it is the difference between "the harness is broken" and "this
    /// build of the harness cannot produce a diff", and only the second one is
    /// survivable.
    #[error("chopsticks does not implement {0}")]
    MethodNotFound(String),
    #[error("chopsticks {method} answered in a shape this version cannot read: {detail}")]
    Shape { method: String, detail: String },
}

pub struct ChopsticksClient {
    url: String,
    client: RpcClient,
}

impl ChopsticksClient {
    pub async fn connect(url: &str) -> Result<Self, ChopsticksError> {
        let client = RpcClient::from_url(url)
            .await
            .map_err(|e| ChopsticksError::Rpc {
                method: "connect".into(),
                message: e.to_string(),
            })?;
        Ok(Self {
            url: url.to_string(),
            client,
        })
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    async fn call(&self, method: &str, params: Vec<Value>) -> Result<Value, ChopsticksError> {
        let mut rpc_params = RpcParams::new();
        for p in params {
            rpc_params.push(p).map_err(|e| ChopsticksError::Rpc {
                method: method.to_string(),
                message: format!("building parameters: {e}"),
            })?;
        }
        self.client
            .request::<Value>(method, rpc_params)
            .await
            .map_err(|e| {
                let message = e.to_string();
                // jsonrpsee renders an unknown method as -32601 / "Method not
                // found"; both spellings are matched because the exact rendering
                // is a property of a transport we do not control, and getting
                // this wrong turns a survivable absence into a hard failure.
                if message.contains("-32601")
                    || message.to_ascii_lowercase().contains("method not found")
                {
                    ChopsticksError::MethodNotFound(method.to_string())
                } else {
                    ChopsticksError::Rpc {
                        method: method.to_string(),
                        message,
                    }
                }
            })
    }

    /// A liveness probe. Used to decide the harness is up, in preference to
    /// parsing its log line — the log goes through pino-pretty and its exact
    /// rendering is not something to build a readiness check on.
    pub async fn system_chain(&self) -> Result<String, ChopsticksError> {
        let v = self.call("system_chain", vec![]).await?;
        Ok(v.as_str().unwrap_or_default().to_string())
    }

    /// Layer raw `(key, value)` pairs onto a block. `None` value = delete.
    ///
    /// Returns the hash of the block that was layered — which is the block we
    /// forked AT, unchanged, because `setStorage` pushes a storage layer onto an
    /// existing block rather than creating one. That is why a fork run's
    /// `at_block_hash` is still the REAL chain's hash at that height and means
    /// exactly what it means on a Tier 1 row.
    pub async fn set_storage(
        &self,
        writes: &[(Vec<u8>, Option<Vec<u8>>)],
    ) -> Result<String, ChopsticksError> {
        let pairs: Vec<Value> = writes
            .iter()
            .map(|(k, v)| {
                serde_json::json!([
                    format!("0x{}", hex::encode(k)),
                    v.as_ref().map(|b| format!("0x{}", hex::encode(b))),
                ])
            })
            .collect();
        let out = self
            .call(DEV_SET_STORAGE, vec![Value::Array(pairs)])
            .await?;
        out.as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| ChopsticksError::Shape {
                method: DEV_SET_STORAGE.into(),
                detail: format!("expected a block hash string, got {out}"),
            })
    }

    /// Build one block. Returns its hash — a hash that exists ONLY on this fork.
    pub async fn new_block(&self) -> Result<String, ChopsticksError> {
        let out = self
            .call(DEV_NEW_BLOCK, vec![serde_json::json!({ "count": 1 })])
            .await?;
        out.as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| ChopsticksError::Shape {
                method: DEV_NEW_BLOCK.into(),
                detail: format!("expected a block hash string, got {out}"),
            })
    }

    /// `chain_getBlock` — the built block exactly as the fork returns it, header
    /// and extrinsics included. Handed straight back to [`Self::run_block_raw_diff`]
    /// so nothing about the block is reconstructed on the way.
    pub async fn chain_get_block(&self, hash: &str) -> Result<Value, ChopsticksError> {
        let out = self
            .call("chain_getBlock", vec![Value::String(hash.to_string())])
            .await?;
        out.get("block")
            .cloned()
            .ok_or_else(|| ChopsticksError::Shape {
                method: "chain_getBlock".into(),
                detail: format!("no `block` in the answer: {out}"),
            })
    }

    /// One storage value at one block, on the FORK. Used for two things: the
    /// built block's `System.Events`, and the BEFORE side of every changed key.
    pub async fn storage_at(
        &self,
        key: &[u8],
        hash: &str,
    ) -> Result<Option<Vec<u8>>, ChopsticksError> {
        let out = self
            .call(
                "state_getStorage",
                vec![
                    Value::String(format!("0x{}", hex::encode(key))),
                    Value::String(hash.to_string()),
                ],
            )
            .await?;
        match out {
            Value::Null => Ok(None),
            Value::String(s) => hex::decode(s.trim_start_matches("0x"))
                .map(Some)
                .map_err(|e| ChopsticksError::Shape {
                    method: "state_getStorage".into(),
                    detail: format!("value is not hex: {e}"),
                }),
            other => Err(ChopsticksError::Shape {
                method: "state_getStorage".into(),
                detail: format!("expected hex or null, got {other}"),
            }),
        }
    }

    /// Keys under a prefix, one page. Used to read the live `Scheduler.Agenda`
    /// key space so the anchor is decided from the chain's own data rather than
    /// assumed (slice 9's blocker 1).
    pub async fn keys_paged(
        &self,
        prefix: &[u8],
        count: u32,
        at: &str,
    ) -> Result<Vec<Vec<u8>>, ChopsticksError> {
        let out = self
            .call(
                STATE_GET_KEYS_PAGED,
                vec![
                    Value::String(format!("0x{}", hex::encode(prefix))),
                    Value::from(count),
                    Value::Null,
                    Value::String(at.to_string()),
                ],
            )
            .await?;
        out.as_array()
            .ok_or_else(|| ChopsticksError::Shape {
                method: STATE_GET_KEYS_PAGED.into(),
                detail: format!("expected an array of keys, got {out}"),
            })?
            .iter()
            .map(|k| {
                k.as_str()
                    .ok_or_else(|| ChopsticksError::Shape {
                        method: STATE_GET_KEYS_PAGED.into(),
                        detail: "a key is not a string".into(),
                    })
                    .and_then(|s| {
                        hex::decode(s.trim_start_matches("0x")).map_err(|e| {
                            ChopsticksError::Shape {
                                method: STATE_GET_KEYS_PAGED.into(),
                                detail: format!("key '{s}' is not hex: {e}"),
                            }
                        })
                    })
            })
            .collect()
    }

    /// Apply ONE extrinsic on top of the current head and return the RAW storage
    /// diff — `initialize_block` → the chain's own inherents → `apply_extrinsic`
    /// → `finalize_block`.
    ///
    /// THIS IS THE ROUTE THAT WORKS ON A PARACHAIN, and slice 9 exists partly
    /// because of it. `dev_newBlock` + `dev_runBlock` do not compose there: the
    /// built block omits the ~51KB `parachainSystem.set_validation_data` inherent
    /// (chopsticks mocks parachain state directly), and `dev_runBlock` re-applies
    /// extrinsics for real and traps on `wasm 'unreachable'`. `dev_dryRun` never
    /// replays a built block — it CREATES the inherents through the chain's own
    /// providers, which is exactly what chopsticks' own preimage plugin does, and
    /// is therefore parachain-safe by construction.
    ///
    /// WHAT IT GIVES AND WHAT IT DOES NOT. `on_initialize` runs, so an injected
    /// scheduler task fires inside the dry run — but its WRITES are not in the
    /// returned diff, which covers `apply_extrinsic` only (module header, fact
    /// 6). Its EVENTS are: `apply_extrinsic` rewrites `System.Events` with the
    /// whole cumulative list, and unlike `dev_runBlock` this method does not
    /// exclude that key, so the events come out of the same answer rather than
    /// needing a second read. One key carrying a block, the rest carrying one
    /// extrinsic — which is exactly why the two look like one scope and are not.
    ///
    /// `address` must be a 0x-hex 32-byte public key: the RPC schema is stricter
    /// than the CLI's `--address`, which takes SS58. Passing SS58 here is
    /// rejected by a zod regex, not silently accepted.
    pub async fn dry_run_extrinsic_raw(
        &self,
        call: &[u8],
        address: &[u8; 32],
    ) -> Result<crate::RawStorageDiff, ChopsticksError> {
        let out = self
            .call(
                DEV_DRY_RUN,
                vec![serde_json::json!({
                    "raw": true,
                    "extrinsic": {
                        "call": format!("0x{}", hex::encode(call)),
                        "address": format!("0x{}", hex::encode(address)),
                    },
                })],
            )
            .await?;
        parse_dry_run_raw_pairs(&out).map_err(|detail| ChopsticksError::Shape {
            method: DEV_DRY_RUN.into(),
            detail,
        })
    }

    /// Re-run a block on its parent and return the RAW storage diff.
    ///
    /// The block is passed back as chopsticks itself returned it from
    /// `chain_getBlock`, so nothing about it is reconstructed: same header, same
    /// extrinsics (including the same timestamp inherent), same parent state.
    /// A re-execution that is given identical inputs produces identical output,
    /// which is what makes reading the diff from a second pass sound.
    pub async fn run_block_raw_diff(
        &self,
        parent: &str,
        header: &Value,
        extrinsics: &Value,
    ) -> Result<crate::RawStorageDiff, ChopsticksError> {
        let out = self
            .call(
                DEV_RUN_BLOCK,
                vec![serde_json::json!({
                    "parent": parent,
                    "block": { "header": header, "extrinsics": extrinsics },
                    "includeRaw": true,
                    "includeParsed": false,
                    "includeBlockDetails": false,
                })],
            )
            .await?;
        parse_run_block_diff(&out).map_err(|detail| ChopsticksError::Shape {
            method: DEV_RUN_BLOCK.into(),
            detail,
        })
    }
}

/// Pull the raw pairs out of a `dev_dryRun` answer with `raw: true`.
///
/// `raw: true` returns the BARE ARRAY of `[keyHex, valueHex | null]` — no
/// wrapper, and deliberately no `outcome`: the method throws instead when the
/// extrinsic could not be APPLIED at all. A dispatch that ran and reverted comes
/// back as a diff whose `System.Events` carries `ExtrinsicFailed`, which is a
/// result and not an error, and is read as one.
///
/// SEPARATED FROM THE CLIENT so it can be tested without a running fork — which
/// is the whole reason it exists as a function. Slice 9 promised this test and
/// shipped none, so the shape of the one answer the live route depends on had no
/// offline coverage at all: every claim about it rested on a drill.
///
/// A `[key]` pair with no second element and a `[key, null]` pair mean the same
/// thing — the key was DELETED — and both are accepted, because which one a
/// JSON serializer emits for a trailing null is not a contract.
pub fn parse_dry_run_raw_pairs(out: &Value) -> Result<crate::RawStorageDiff, String> {
    let arr = out.as_array().ok_or_else(|| {
        format!("raw:true should return an array of [key, value] pairs, got {out}")
    })?;
    let mut pairs = Vec::with_capacity(arr.len());
    for entry in arr {
        let pair = entry
            .as_array()
            .ok_or_else(|| format!("a diff entry is not a [key, value] pair: {entry}"))?;
        let key = pair
            .first()
            .and_then(|k| k.as_str())
            .ok_or_else(|| format!("a diff entry has no readable key: {entry}"))?;
        let key = hex::decode(key.trim_start_matches("0x"))
            .map_err(|e| format!("diff key '{key}' is not hex: {e}"))?;
        let value = match pair.get(1) {
            None | Some(Value::Null) => None,
            Some(Value::String(s)) => Some(
                hex::decode(s.trim_start_matches("0x"))
                    .map_err(|e| format!("diff value '{s}' is not hex: {e}"))?,
            ),
            Some(other) => {
                return Err(format!("a diff value is neither hex nor null: {other}"))
            }
        };
        pairs.push((key, value));
    }
    Ok(pairs)
}

/// Pull the raw pairs out of a `dev_runBlock` answer.
///
/// PARSED DEFENSIVELY AND REFUSED LOUDLY. Two shapes are accepted for a diff
/// entry — `{"raw": {"key": …, "value": …}}` and a bare `{"key": …, "value": …}`
/// — because the wrapper is an implementation detail of a tool we do not
/// control and matching only one of them would turn a harmless rename into a
/// silently empty diff. What is NOT accepted is an answer with no `phases` at
/// all: that is a shape change, and reporting it as "nothing changed" would be
/// the worst possible reading.
///
/// Separated from the client so it can be tested without a running fork.
pub fn parse_run_block_diff(out: &Value) -> Result<crate::RawStorageDiff, String> {
    let phases = out
        .get("phases")
        .and_then(|p| p.as_array())
        .ok_or_else(|| {
            format!(
                "no `phases` array in the answer — this version reads dev_runBlock's per-phase \
                 raw diffs, and an answer without them is a shape it does not know. Got keys: {}",
                out.as_object()
                    .map(|o| o.keys().cloned().collect::<Vec<_>>().join(", "))
                    .unwrap_or_else(|| "not an object".into())
            )
        })?;

    let mut out_pairs: crate::RawStorageDiff = Vec::new();
    let mut saw_entry = false;
    for phase in phases {
        let Some(entries) = phase.get("storageDiff").and_then(|d| d.as_array()) else {
            continue;
        };
        for entry in entries {
            saw_entry = true;
            let inner = entry.get("raw").unwrap_or(entry);
            let key = inner
                .get("key")
                .and_then(|k| k.as_str())
                .ok_or_else(|| format!("a storageDiff entry has no readable `key`: {entry}"))?;
            let key = hex::decode(key.trim_start_matches("0x"))
                .map_err(|e| format!("storageDiff key '{key}' is not hex: {e}"))?;
            let value = match inner.get("value") {
                None | Some(Value::Null) => None,
                Some(Value::String(s)) => Some(
                    hex::decode(s.trim_start_matches("0x"))
                        .map_err(|e| format!("storageDiff value '{s}' is not hex: {e}"))?,
                ),
                Some(other) => {
                    return Err(format!("a storageDiff value is neither hex nor null: {other}"))
                }
            };
            // LAST WRITE WINS, and it must: a key touched in more than one phase
            // (initialization then an extrinsic, say) appears more than once, and
            // the state at the end of the block is the last one. Keeping the
            // first would render a diff that never existed.
            if let Some(slot) = out_pairs.iter_mut().find(|(k, _)| *k == key) {
                slot.1 = value;
            } else {
                out_pairs.push((key, value));
            }
        }
    }
    // An answer with phases but no entries anywhere is a genuinely empty diff;
    // an answer whose phases carry no `storageDiff` key at all is not, and the
    // two are told apart here rather than downstream.
    if !saw_entry && phases.iter().all(|p| p.get("storageDiff").is_none()) && !phases.is_empty() {
        return Err(
            "every phase came back without a `storageDiff` field — this looks like a shape \
             change rather than a block that changed nothing"
                .into(),
        );
    }
    Ok(out_pairs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_raw_diff_is_read_from_either_shape_and_the_last_write_wins() {
        let out = json!({
            "phases": [
                { "storageDiff": [
                    { "raw": { "key": "0xaa", "value": "0x01" } },
                    { "raw": { "key": "0xbb", "value": null } }
                ]},
                // the bare shape, and a SECOND write to 0xaa — the end-of-block
                // value is the later one
                { "storageDiff": [ { "key": "0xaa", "value": "0x02" } ] }
            ]
        });
        let pairs = parse_run_block_diff(&out).expect("reads");
        assert_eq!(pairs.len(), 2, "one entry per key, not one per write");
        let aa = pairs.iter().find(|(k, _)| k == &vec![0xaa]).unwrap();
        assert_eq!(
            aa.1,
            Some(vec![0x02]),
            "a key written twice in one block ends at its LAST value"
        );
        let bb = pairs.iter().find(|(k, _)| k == &vec![0xbb]).unwrap();
        assert_eq!(bb.1, None, "a null value is a deletion, not an empty value");
    }

    #[test]
    fn a_shape_change_is_refused_rather_than_read_as_an_empty_diff() {
        // no `phases` at all
        let err = parse_run_block_diff(&json!({ "storageDiff": [] })).unwrap_err();
        assert!(err.contains("phases"), "{err}");

        // phases present, none of them carrying a diff field
        let err = parse_run_block_diff(&json!({ "phases": [ { "runtimeLogs": [] } ] }))
            .unwrap_err();
        assert!(err.contains("shape change"), "{err}");

        // ...but a phase whose diff is genuinely EMPTY is a real empty diff
        let pairs = parse_run_block_diff(&json!({ "phases": [ { "storageDiff": [] } ] }))
            .expect("an empty diff is data");
        assert!(pairs.is_empty());
    }

    #[test]
    fn a_dry_run_raw_answer_is_read_as_pairs_and_a_null_is_a_deletion() {
        // THE SHAPE THE LIVE ROUTE DEPENDS ON, and until slice 10 it had no
        // offline coverage at all: it was parsed inline in an async client
        // method, so every claim about it rested on a drill.
        let out = json!([
            ["0x26aa394e", "0x0102"],
            ["0xdeadbeef", null],
            // a pair with the value ELIDED rather than null — same meaning, and
            // which one a serializer emits for a trailing null is not a contract
            ["0xcafe"]
        ]);
        let pairs = parse_dry_run_raw_pairs(&out).expect("reads");
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0], (vec![0x26, 0xaa, 0x39, 0x4e], Some(vec![0x01, 0x02])));
        assert_eq!(
            pairs[1].1, None,
            "a null value is a DELETION, never an empty value"
        );
        assert_eq!(
            pairs[2].1, None,
            "an elided value is the same deletion as an explicit null"
        );

        // An empty diff is DATA — a run that changed nothing — and must not be
        // an error.
        assert!(parse_dry_run_raw_pairs(&json!([])).expect("empty is data").is_empty());
    }

    #[test]
    fn a_dry_run_answer_in_the_wrong_shape_is_refused_and_says_which_part() {
        // The wrapped form `dev_dryRun` returns WITHOUT `raw: true`. Accepting it
        // by walking into it would mean reading a decoded rendering as raw bytes.
        let err = parse_dry_run_raw_pairs(&json!({ "old": {}, "new": {}, "delta": {} }))
            .unwrap_err();
        assert!(err.contains("array of [key, value] pairs"), "{err}");

        let err = parse_dry_run_raw_pairs(&json!([["0xaa", { "decoded": 1 }]])).unwrap_err();
        assert!(err.contains("neither hex nor null"), "{err}");

        let err = parse_dry_run_raw_pairs(&json!([["not hex", "0x01"]])).unwrap_err();
        assert!(err.contains("not hex"), "{err}");

        let err = parse_dry_run_raw_pairs(&json!([[42, "0x01"]])).unwrap_err();
        assert!(err.contains("no readable key"), "{err}");
    }

    #[test]
    fn a_diff_value_that_is_neither_hex_nor_null_is_refused() {
        let err = parse_run_block_diff(&json!({
            "phases": [ { "storageDiff": [ { "key": "0xaa", "value": { "decoded": 1 } } ] } ]
        }))
        .unwrap_err();
        assert!(err.contains("neither hex nor null"), "{err}");
    }
}
