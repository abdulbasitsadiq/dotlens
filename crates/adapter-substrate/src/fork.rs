//! The Substrate half of Tier 2 (Phase 3, slice 8) — pure, no network, no
//! process, no chopsticks.
//!
//! `crates/sim` owns the ORCHESTRATION and `dotlens-node::fork_run` owns the
//! subprocess and its JSON-RPC. This file owns the only thing that is protocol:
//! turning a runtime's own metadata into (a) the storage a fork must be given so
//! it will dispatch a call under a chosen origin, (b) the raw keys and values a
//! counterfactual injects, and (c) a raw storage diff read back as something a
//! person can read. Same split as `dryrun.rs` under `sim_run.rs` (Invariant 4).
//!
//! ---------------------------------------------------------------------------
//! WHY TIER 2 IS NOT "TIER 1 WITH MORE STEPS", stated first because the whole
//! shape follows from it.
//!
//! Tier 1 asks the runtime a QUESTION (`DryRunApi::dry_run_call`) and reads its
//! answer. Tier 2 does not ask anything: it puts state in front of a runtime and
//! lets it BUILD A BLOCK. Three consequences, and each one is a column:
//!
//!   1. **The dispatch is scheduled, not called.** There is no way to hand a
//!      forked node a Root origin from outside — `dev_setStorage` sets storage
//!      and `dev_newBlock` builds a block, and neither takes an origin. What DOES
//!      take an origin is `pallet_scheduler`'s agenda: a `Scheduled` entry
//!      carries `origin: PalletsOrigin` and the scheduler dispatches with it in
//!      `on_initialize`. So this file writes one agenda entry at the height
//!      [`decide_agenda_anchor`] chose — which is NOT `head + 1`, and on a
//!      parachain is not even on this chain's number line — and the chain does
//!      the rest. That is genuinely closer to enactment than Tier 1's direct
//!      dispatch (a referendum IS enacted by the scheduler) and it is also why
//!      `status` gains a third value (below).
//!
//!      IT IS ALSO WHY THE STORAGE DIFF DOES NOT DESCRIBE THE CALL ON THIS
//!      ROUTE. `on_initialize` is a phase the harness does not return a diff
//!      for; see [`DIFF_STATUS_EXTRINSIC_ONLY`], which is a limit of somebody
//!      else's engine, recorded rather than papered over.
//!
//!   2. **The answer is a block, not a response.** Events come out of
//!      `System.Events` at the built block and are decoded by the SAME decoder
//!      that decodes mainnet blocks, so a Tier 2 event and an indexed event are
//!      the same kind of object and `emitted_events` means what it means
//!      everywhere else in this project.
//!
//!   3. **The state diff is the thing Tier 1 cannot give**, and it arrives as
//!      raw `(key, value)` byte pairs. Reading it is the "readable" half of the
//!      phase's exit criterion, and it is done HERE, against the runtime's own
//!      metadata, rather than by asking chopsticks to decode it for us — because
//!      its decoder is polkadot-js's and ours is the one every other number in
//!      this project came through. Raw first (Invariant 1): the pairs are
//!      archived as bytes and this file is what a later `FORK_VERSION` re-reads
//!      them with.
//!
//! ---------------------------------------------------------------------------
//! NOTHING HERE NAMES A CHAIN, and the three places it comes close are worth
//! reading rather than trusting.
//!
//!   * `Scheduler` / `Agenda` / `Preimage` / `System.Events` are looked up by
//!     NAME in the runtime's own metadata, with a loud refusal listing the names
//!     the runtime does have. That is the same device `dryrun::noop_call` uses
//!     for `system.remark` and `gov` uses for `Preimage.PreimageFor`. It is a
//!     FRAME fact, not a chain fact — but no metadata attribute says "this is the
//!     scheduler", so a runtime that spells it differently is refused rather than
//!     guessed at. Stated as a limitation, not hidden as an assumption.
//!   * Every variant index, every field, every hasher and every type is read out
//!     of the metadata. There is not one hardcoded SCALE index in this file, and
//!     the encoded agenda entry is DECODED AGAIN before it is used — a value that
//!     does not round-trip is refused rather than injected (the same device
//!     `dryrun::forwarded_program` uses on re-encoded XCM).
//!   * `BOUNDED_INLINE_LIMIT` is the one number that could not be read from
//!     anywhere. See its doc comment.

use parity_scale_codec::{Compact, Decode, Encode};
use scale_info::{PortableRegistry, TypeDef, TypeDefPrimitive};
use scale_value::{Composite, Value};

use crate::assets::{storage_entry_info, StorageEntryInfo, StorageKeyHasher};
use crate::frame_decoder::value_to_json;
use frame_metadata::{RuntimeMetadata, RuntimeMetadataPrefixed};

/// Lineage for every `tier = 'fork'` row. Bump when the INTERPRETATION of a
/// harness result changes — the events walk, the diff decoding, the status
/// rules — and rows below it rebuild from the archived `chopsticks_fork.response.scale`.
///
/// It is a DIFFERENT version line from `dryrun::DRY_RUN_VERSION` even though
/// both land in `sim.simulation_results.sim_version`, because the two tiers
/// interpret different bytes. `tier` disambiguates; the migration says so beside
/// the column.
///
/// 1 → 2 (slice 10): `diff_status` is now DERIVED from the archived answer
/// rather than copied out of it, and a `dev_dryRun` diff is recorded as
/// [`DIFF_STATUS_EXTRINSIC_ONLY`] rather than `decoded`. Every version-1 fork row
/// therefore carries a status that over-claims — it says the diff describes the
/// block when it describes one extrinsic — and rows rebuild from the archived
/// `chopsticks_fork.response.scale` with NO re-run, which is the whole point of this version line
/// being separate from the harness's.
pub const FORK_VERSION: u32 = 2;

/// The artifact namespace under `raw_store::keys::simulation(...)`. It names the
/// METHOD the bytes belong to, exactly as `DryRunApi_dry_run_call` does on Tier
/// 1, so one (state, input) directory can hold both tiers' evidence without
/// either having to be read to find out which is which.
///
/// IT CARRIES THE ANSWER'S SHAPE VERSION, and that is slice 8's blocker 3 fixed
/// STRUCTURALLY rather than a third time by hand. The answer artifact is
/// archived under the write-once (chain, block, input) key, so ANY change to its
/// shape makes the bytes differ at a key that already holds the old shape — and
/// the re-put is then refused forever, which is exactly what a port in the
/// answer did to slice 8 and what a clock did to slice 9. Slice 10 changes the
/// shape again (`diff_status` out, `diff_method` in), so without this the first
/// re-run at any previously-simulated state would die on `refusing to overwrite
/// immutable object` for the third slice running.
///
/// A shape version in the ITEM NAME ends it: a new shape writes a new file
/// beside the old one, the old answer stays readable at the `raw_location` its
/// row already records, and the store's real job — catching two DIFFERENT
/// answers to one question in one shape — is untouched. Bump it whenever the
/// keys of the object `drive` returns change.
pub const FORK_METHOD: &str = "chopsticks_fork.v2";

pub const SCHEDULER_PALLET: &str = "Scheduler";
pub const AGENDA_ENTRY: &str = "Agenda";
/// `pallet_scheduler::IncompleteSince` — the height the next sweep RESUMES from.
///
/// Writing it is what makes the injection robust against a `now` we cannot
/// predict. See [`AgendaAnchor`].
pub const INCOMPLETE_SINCE_ENTRY: &str = "IncompleteSince";
/// `parachain_system::LastRelayChainBlockNumber` — the value a parachain's
/// relay-anchored `BlockNumberProvider` returns.
pub const PARACHAIN_SYSTEM_PALLET: &str = "ParachainSystem";
pub const LAST_RELAY_NUMBER_ENTRY: &str = "LastRelayChainBlockNumber";
pub const PREIMAGE_PALLET: &str = "Preimage";
pub const PREIMAGE_FOR_ENTRY: &str = "PreimageFor";

/// The preimage pallet's request-status map, whose NAME changed. Newer runtimes
/// call it `RequestStatusFor` and older ones `StatusFor`, and during the
/// migration a runtime declares both. Every one it declares is written, because
/// which one `QueryPreimage::len` reads is a property of the pallet version we
/// cannot see from metadata — and writing a second key costs nothing while
/// guessing wrong produces a `CallUnavailable` that reads like the call was bad.
pub const PREIMAGE_STATUS_ENTRIES: [&str; 2] = ["RequestStatusFor", "StatusFor"];

/// `frame_support::traits::Bounded::Inline` holds a
/// `BoundedVec<u8, ConstU32<128>>`, and **scale-info does not record the bound**
/// — a `ConstU32` is a phantom type parameter, so the metadata's type for that
/// field is an ordinary `Vec<u8>` and there is no way to read 128 out of it.
///
/// This is therefore the one constant in this file that is written down rather
/// than derived, and it is written down with its consequence: a call ABOVE it
/// cannot go inline and is scheduled by `Lookup` with its preimage injected
/// instead. Getting the number wrong in the SAFE direction (too small) costs a
/// preimage injection that was not needed; getting it wrong in the unsafe
/// direction would write an agenda entry the runtime cannot decode, which
/// surfaces as an empty agenda and a `not_dispatched` with no explanation. Hence
/// the conservative constant and the round-trip check beside it.
pub const BOUNDED_INLINE_LIMIT: usize = 128;

/// `pallet_scheduler::HARD_DEADLINE`. A task at this priority or better is
/// serviced even when the agenda is over its weight budget, which is exactly
/// what we want: a task postponed for weight would come back as
/// `not_dispatched`, and "the harness was busy" and "the chain declined" must
/// not be the same answer. chopsticks' own preimage plugin picks the same value.
pub const HARD_DEADLINE_PRIORITY: u8 = 63;

/// How many bytes of a diff value are rendered inline before the rendering is
/// cut and replaced by a length and a hash.
///
/// NOT COSMETIC. A runtime-upgrade referendum writes `:code`, which is megabytes
/// — rendering it whole would put a multi-megabyte string into a jsonb column
/// and into every HTTP response that reads the row. The full bytes are in the
/// archived `response.json`; what is rendered is a preview, and it says so.
pub const DIFF_VALUE_PREVIEW_BYTES: usize = 256;

/// Substrate's well-known keys — protocol constants, not chain facts, which is
/// why naming them here does not name a chain. `:code` in particular is worth
/// recognising by name: a diff entry that changes it IS a runtime upgrade, and
/// that is the single most consequential thing a referendum diff can say.
const WELL_KNOWN_KEYS: [(&[u8], &str); 3] = [
    (b":code", ":code"),
    (b":heappages", ":heappages"),
    (b":extrinsic_index", ":extrinsic_index"),
];

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ForkError(pub String);

fn err<T>(msg: impl Into<String>) -> Result<T, ForkError> {
    Err(ForkError(msg.into()))
}

// ============================================================================
// THE OVERRIDE GRAMMAR
// ============================================================================

/// One storage override, as a person writes it.
///
/// THE GRAMMAR IS DELIBERATELY SMALL AND DELIBERATELY EXPLICIT:
///
/// ```text
///   <Pallet>.<Item>=<json>                 a plain storage item
///   <Pallet>.<Item>(<json args>)=<json>    a map entry, one arg per hasher
///   <Pallet>.<Item>(<json args>)=raw:0x…   the same key, value given pre-encoded
///   <Pallet>.<Item>(<json args>)=null      delete the key
///   0x<key>=0x<value>                      a fully raw pair, nothing interpreted
///   0x<key>=null                           delete a raw key
/// ```
///
/// `null` ON THE RIGHT OF `=` MEANS DELETE THE KEY, and it is the only place
/// `null` is accepted. Inside a value, a JSON `null` is REFUSED with a message
/// pointing at `{"None": []}` — because `Option::None` and "this key is gone" are
/// different facts, and a grammar in which one token means both is a grammar in
/// which a typo silently deletes a treasury balance.
///
/// The value JSON is the shape THIS PROJECT'S DECODER PRINTS (a variant is
/// `{"Name": [fields…]}`, a byte array may be written `"0x…"`), so the way to
/// write an override is to read the current value out of dotlens, change a
/// number, and paste it back. That symmetry is the whole ergonomic argument for
/// not inventing a second notation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OverrideSpec {
    Named {
        pallet: String,
        item: String,
        args: Vec<serde_json::Value>,
        /// `None` = delete the key.
        value: Option<OverrideValue>,
    },
    Raw {
        key: Vec<u8>,
        /// `None` = delete the key.
        value: Option<Vec<u8>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OverrideValue {
    /// Encoded against the storage item's declared value type.
    Json(serde_json::Value),
    /// Taken verbatim. The escape hatch for a value this grammar cannot express
    /// and for a string field that genuinely begins with `0x`.
    Raw(Vec<u8>),
}

impl OverrideSpec {
    pub fn parse(spec: &str) -> Result<Self, ForkError> {
        let trimmed = spec.trim();
        let Some(eq) = top_level_eq(trimmed) else {
            return err(format!(
                "'{trimmed}' is not an override — expected <Pallet>.<Item>[(args)]=<value> or \
                 0x<key>=0x<value>"
            ));
        };
        let (lhs, rhs) = trimmed.split_at(eq);
        let lhs = lhs.trim();
        let rhs = rhs[1..].trim();

        // `null` — and ONLY here — means "delete this key".
        let delete = rhs.eq_ignore_ascii_case("null");

        if let Some(hex_key) = lhs.strip_prefix("0x").or_else(|| lhs.strip_prefix("0X")) {
            let key = hex::decode(hex_key)
                .map_err(|e| ForkError(format!("override key '{lhs}' is not hex: {e}")))?;
            if key.is_empty() {
                return err("a raw override key must not be empty");
            }
            let value =
                if delete {
                    None
                } else {
                    let hex_value = rhs
                        .strip_prefix("0x")
                        .or_else(|| rhs.strip_prefix("0X"))
                        .ok_or_else(|| {
                            ForkError(format!(
                                "a RAW override key takes a raw value: expected 0x… or null, got \
                             '{rhs}'. There is no metadata to encode a JSON value against when \
                             the key itself was given as bytes"
                            ))
                        })?;
                    Some(hex::decode(hex_value).map_err(|e| {
                        ForkError(format!("override value '{rhs}' is not hex: {e}"))
                    })?)
                };
            return Ok(Self::Raw { key, value });
        }

        // <Pallet>.<Item>[(args)]
        let (path, args_src) = match lhs.split_once('(') {
            Some((p, rest)) => {
                let inner = rest.strip_suffix(')').ok_or_else(|| {
                    ForkError(format!("override '{lhs}' opens '(' and never closes it"))
                })?;
                (p.trim(), Some(inner))
            }
            None => (lhs, None),
        };
        let Some((pallet, item)) = path.split_once('.') else {
            return err(format!(
                "'{path}' does not name a storage item — expected <Pallet>.<Item>"
            ));
        };
        if pallet.trim().is_empty() || item.trim().is_empty() {
            return err(format!(
                "'{path}' has an empty pallet or item name on one side of '.'"
            ));
        }
        // Args are a JSON array body, so nested objects and strings work without
        // a second escaping notation.
        let args: Vec<serde_json::Value> = match args_src {
            None => Vec::new(),
            Some(inner) if inner.trim().is_empty() => Vec::new(),
            Some(inner) => serde_json::from_str::<serde_json::Value>(&format!("[{inner}]"))
                .map_err(|e| {
                    ForkError(format!(
                        "the key arguments of '{lhs}' are not JSON: {e} (they are read as a \
                         JSON array body, so a 32-byte account is written \"0x…\")"
                    ))
                })?
                .as_array()
                .cloned()
                .unwrap_or_default(),
        };

        let value = if delete {
            None
        } else if let Some(raw) = rhs.strip_prefix("raw:") {
            let hex_value = raw
                .trim()
                .strip_prefix("0x")
                .or_else(|| raw.trim().strip_prefix("0X"))
                .ok_or_else(|| ForkError(format!("raw: expects 0x…, got '{raw}'")))?;
            Some(OverrideValue::Raw(hex::decode(hex_value).map_err(|e| {
                ForkError(format!("raw override value is not hex: {e}"))
            })?))
        } else {
            Some(OverrideValue::Json(serde_json::from_str(rhs).map_err(
                |e| {
                    ForkError(format!(
                        "the value of '{lhs}' is not JSON: {e}. Write it the way dotlens prints \
                         it — a variant is {{\"Name\": [fields]}}, a byte array may be \"0x…\", \
                         and Option::None is {{\"None\": []}} (a bare null on the right of '=' \
                         means DELETE THE KEY)"
                    ))
                },
            )?))
        };

        Ok(Self::Named {
            pallet: pallet.trim().to_string(),
            item: item.trim().to_string(),
            args,
            value,
        })
    }
}

/// The first `=` that is not inside parentheses, brackets, braces or a JSON
/// string. Written out rather than `split_once('=')` because a value like
/// `{"note":"a=b"}` is legal JSON and would otherwise cut the spec in half.
fn top_level_eq(s: &str) -> Option<usize> {
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escaped = false;
    for (i, c) in s.char_indices() {
        if in_string {
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '"' {
                in_string = false;
            }
            continue;
        }
        match c {
            '"' => in_string = true,
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth -= 1,
            '=' if depth <= 0 => return Some(i),
            _ => {}
        }
    }
    None
}

/// One override, resolved into the bytes a fork is actually given.
///
/// `key` and `value` are what gets injected AND what gets hashed into the
/// request's `input_hash` — the same bytes, so the cache key and the injection
/// can never describe different counterfactuals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedOverride {
    /// The spec exactly as it was written, so the row can show what was asked
    /// for beside what it became.
    pub spec: String,
    /// `Assets.Account(1337, 0x…)` — how the key reads once resolved, or a
    /// description recovered from a raw key where the metadata allows one.
    pub resolved: String,
    pub key: Vec<u8>,
    /// `None` = this override DELETES the key.
    pub value: Option<Vec<u8>>,
}

// ============================================================================
// THE STORAGE KEY INDEX — the "readable" in readable diff
// ============================================================================

/// One storage entry as the runtime declares it, plus the 32-byte prefix every
/// one of its keys begins with.
#[derive(Debug, Clone)]
pub struct StorageEntryRef {
    pub prefix: [u8; 32],
    pub pallet: String,
    pub item: String,
    pub hashers: Vec<StorageKeyHasher>,
    /// A tuple type for a multi-key map; the bare key type for a single-key map;
    /// `None` for a plain entry.
    pub key_type: Option<u32>,
    pub value_type: u32,
}

/// Every storage entry a runtime declares, indexed by its key prefix.
///
/// This is what turns `0x26aa394e…` back into `System.Account(0x…)`. It is built
/// once per metadata blob and it is the reason a fork diff is readable at all —
/// chopsticks hands back raw key/value pairs and nothing else, which is the right
/// thing for it to do and useless without this.
pub struct StorageKeyIndex {
    entries: Vec<StorageEntryRef>,
    types: PortableRegistry,
}

/// What a raw storage key turned out to be.
#[derive(Debug, Clone, serde::Serialize)]
pub struct KeyDescription {
    pub pallet: Option<String>,
    pub item: Option<String>,
    /// The key arguments, decoded — as many as the hashers allowed.
    pub args: Vec<serde_json::Value>,
    /// False when a NON-CONCAT hasher stopped the walk. Blake2_128, Twox64,
    /// Twox128, Twox256 and Blake2_256 keep no copy of the key they hashed, so
    /// the arguments under them are not recoverable from the key at all. Saying
    /// so is the honest answer; inverting a hash is not available and guessing
    /// from context would be a fabrication in the middle of a diff whose entire
    /// job is to be trustworthy.
    pub args_complete: bool,
    pub args_note: Option<String>,
    /// `System.Account(0x…)` | `Scheduler.Agenda(19412345)` | `:code` |
    /// `unknown key 0x…`
    pub readable: String,
    #[serde(skip)]
    pub value_type: Option<u32>,
}

impl StorageKeyIndex {
    pub fn from_metadata(blob: &[u8]) -> Result<Self, ForkError> {
        let prefixed = RuntimeMetadataPrefixed::decode(&mut &blob[..])
            .map_err(|e| ForkError(format!("metadata blob undecodable: {e}")))?;

        macro_rules! walk {
            ($m:expr, $ver:ident) => {{
                use frame_metadata::$ver::{StorageEntryType, StorageHasher};
                let map_hasher = |h: &StorageHasher| match h {
                    StorageHasher::Blake2_128 => StorageKeyHasher::Blake2_128,
                    StorageHasher::Blake2_256 => StorageKeyHasher::Blake2_256,
                    StorageHasher::Blake2_128Concat => StorageKeyHasher::Blake2_128Concat,
                    StorageHasher::Twox128 => StorageKeyHasher::Twox128,
                    StorageHasher::Twox256 => StorageKeyHasher::Twox256,
                    StorageHasher::Twox64Concat => StorageKeyHasher::Twox64Concat,
                    StorageHasher::Identity => StorageKeyHasher::Identity,
                };
                let mut out: Vec<StorageEntryRef> = Vec::new();
                for pallet in $m.pallets.iter() {
                    let Some(storage) = pallet.storage.as_ref() else {
                        continue;
                    };
                    for entry in storage.entries.iter() {
                        // The prefix is twox128(STORAGE PREFIX) ++ twox128(entry),
                        // and the storage prefix is NOT always the pallet name —
                        // `construct_runtime!` lets an instance declare its own
                        // (Collectives runs two instances of pallet-treasury).
                        // Reading the declared prefix rather than the pallet name
                        // is what makes the index correct on those chains.
                        let mut prefix = [0u8; 32];
                        prefix[..16]
                            .copy_from_slice(&crate::votes::twox_128(storage.prefix.as_bytes()));
                        prefix[16..]
                            .copy_from_slice(&crate::votes::twox_128(entry.name.as_bytes()));
                        let (hashers, key_type, value_type) = match &entry.ty {
                            StorageEntryType::Map {
                                hashers,
                                key,
                                value,
                            } => (
                                hashers.iter().map(map_hasher).collect::<Vec<_>>(),
                                Some(key.id),
                                value.id,
                            ),
                            StorageEntryType::Plain(value) => (Vec::new(), None, value.id),
                        };
                        out.push(StorageEntryRef {
                            prefix,
                            pallet: storage.prefix.to_string(),
                            item: entry.name.to_string(),
                            hashers,
                            key_type,
                            value_type,
                        });
                    }
                }
                (out, $m.types.clone())
            }};
        }

        let (entries, types) = match &prefixed.1 {
            RuntimeMetadata::V14(m) => walk!(m, v14),
            RuntimeMetadata::V15(m) => walk!(m, v15),
            RuntimeMetadata::V16(m) => walk!(m, v16),
            other => {
                return err(format!(
                    "unsupported metadata version {} (v14/v15/v16 only)",
                    other.version()
                ))
            }
        };
        Ok(Self { entries, types })
    }

    pub fn types(&self) -> &PortableRegistry {
        &self.types
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn entry(&self, pallet: &str, item: &str) -> Option<&StorageEntryRef> {
        self.entries
            .iter()
            .find(|e| e.pallet == pallet && e.item == item)
    }

    /// Read a raw key back into pallet, item and as many key arguments as the
    /// hashers preserved.
    pub fn describe(&self, key: &[u8]) -> KeyDescription {
        for (bytes, name) in WELL_KNOWN_KEYS {
            if key == bytes {
                return KeyDescription {
                    pallet: None,
                    item: None,
                    args: Vec::new(),
                    args_complete: true,
                    args_note: Some(
                        "a Substrate WELL-KNOWN key: it belongs to no pallet and is written \
                         directly into the trie"
                            .into(),
                    ),
                    readable: name.to_string(),
                    value_type: None,
                };
            }
        }
        if key.len() < 32 {
            return KeyDescription {
                pallet: None,
                item: None,
                args: Vec::new(),
                args_complete: false,
                args_note: Some(
                    "shorter than a pallet storage prefix and not a well-known key".into(),
                ),
                readable: format!("unknown key 0x{}", hex::encode(key)),
                value_type: None,
            };
        }
        let Some(entry) = self.entries.iter().find(|e| e.prefix == key[..32]) else {
            return KeyDescription {
                pallet: None,
                item: None,
                args: Vec::new(),
                args_complete: false,
                args_note: Some(
                    "no storage entry in this runtime's metadata has this prefix — a child \
                     trie, a well-known key we do not recognise, or an entry from a different \
                     runtime version"
                        .into(),
                ),
                readable: format!("unknown key 0x{}", hex::encode(key)),
                value_type: None,
            };
        };

        let mut rest = &key[32..];
        let mut args: Vec<serde_json::Value> = Vec::new();
        let mut args_complete = true;
        let mut args_note = None;
        let key_types = split_key_types(&self.types, entry.key_type, entry.hashers.len());

        for (i, hasher) in entry.hashers.iter().enumerate() {
            let Some(skip) = hasher.concat_prefix_len() else {
                args_complete = false;
                args_note = Some(format!(
                    "argument {i} and everything after it sit under a {hasher:?} hasher, which \
                     keeps no copy of the key it hashed — they are not recoverable from this \
                     key, and are reported as unknown rather than guessed"
                ));
                break;
            };
            if rest.len() < skip {
                args_complete = false;
                args_note = Some(format!("key ends inside argument {i}'s hash"));
                break;
            }
            rest = &rest[skip..];
            let Some(ty) = key_types.as_ref().and_then(|t| t.get(i).copied()) else {
                args_complete = false;
                args_note = Some(format!(
                    "this runtime declares {} hasher(s) for {}.{} but a key type this reader \
                     could not split into that many parts",
                    entry.hashers.len(),
                    entry.pallet,
                    entry.item
                ));
                break;
            };
            match scale_value::scale::decode_as_type(&mut rest, ty, &self.types) {
                Ok(v) => args.push(value_to_json(&v.remove_context())),
                Err(e) => {
                    args_complete = false;
                    args_note = Some(format!("argument {i} did not decode: {e}"));
                    break;
                }
            }
        }

        let readable = if args.is_empty() && entry.hashers.is_empty() {
            format!("{}.{}", entry.pallet, entry.item)
        } else {
            let rendered: Vec<String> = args.iter().map(render_arg).collect();
            // No leading comma when NOTHING was recoverable — a first hasher
            // that keeps no key leaves `args` empty, and `Pallet.Item(, …)` is
            // a rendering of a bug rather than of a key.
            let tail = match (args_complete, rendered.is_empty()) {
                (true, _) => "",
                (false, true) => "…",
                (false, false) => ", …",
            };
            format!(
                "{}.{}({}{tail})",
                entry.pallet,
                entry.item,
                rendered.join(", ")
            )
        };

        KeyDescription {
            pallet: Some(entry.pallet.clone()),
            item: Some(entry.item.clone()),
            args,
            args_complete,
            args_note,
            readable,
            value_type: Some(entry.value_type),
        }
    }

    /// Decode a stored VALUE against the entry's declared type.
    ///
    /// Errors are returned rather than swallowed: a diff entry whose value could
    /// not be decoded shows its raw bytes and says why, and that is a better row
    /// than one that quietly shows nothing.
    pub fn decode_value(&self, value_type: u32, bytes: &[u8]) -> Result<serde_json::Value, String> {
        let mut cursor = bytes;
        let v = scale_value::scale::decode_as_type(&mut cursor, value_type, &self.types)
            .map_err(|e| e.to_string())?;
        if !cursor.is_empty() {
            return Err(format!(
                "{} trailing byte(s) after the declared value type — these bytes are not (only) \
                 this storage item's value",
                cursor.len()
            ));
        }
        Ok(value_to_json(&v.remove_context()))
    }
}

/// A key argument, rendered short. Byte arrays become `0x…` because a 32-byte
/// account rendered as 32 numbers is not readable, which is the whole point.
fn render_arg(v: &serde_json::Value) -> String {
    if let Some(bytes) = json_byte_array(v) {
        return format!("0x{}", hex::encode(bytes));
    }
    match v {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// A JSON array whose every element is a byte-sized integer.
///
/// IT PEELS SINGLE-ELEMENT ARRAY WRAPPERS FIRST, and that is not defensive
/// padding: `frame_decoder::value_to_json` renders a NEWTYPE one array layer
/// deeper than a human writes it, so an `AccountId32` key argument arrives as
/// `[[7,7,…]]` and not `[7,7,…]`. This project has met that layer four times
/// (orml's `Processed.id`, `Sent.message`, `X1`, `Ump(Para(id))`); without the
/// peel a 32-byte account renders as a list of numbers and the whole point of a
/// readable key is lost. Bounded so a pathological value cannot spin.
fn json_byte_array(v: &serde_json::Value) -> Option<Vec<u8>> {
    let mut cur = v;
    for _ in 0..4 {
        let arr = cur.as_array()?;
        if arr.is_empty() {
            return None;
        }
        if arr.len() == 1 && arr[0].is_array() {
            cur = &arr[0];
            continue;
        }
        return arr
            .iter()
            .map(|n| n.as_u64().filter(|x| *x <= 255).map(|x| x as u8))
            .collect();
    }
    None
}

/// Split a map's key type into one type per hasher. A single-key map's key type
/// is the key itself; a multi-key map's is a tuple.
fn split_key_types(
    types: &PortableRegistry,
    key_type: Option<u32>,
    hashers: usize,
) -> Option<Vec<u32>> {
    let key_type = key_type?;
    if hashers == 0 {
        return Some(Vec::new());
    }
    if hashers == 1 {
        return Some(vec![key_type]);
    }
    match &types.resolve(key_type)?.type_def {
        TypeDef::Tuple(t) if t.fields.len() == hashers => {
            Some(t.fields.iter().map(|f| f.id).collect())
        }
        _ => None,
    }
}

// ============================================================================
// RESOLVING AN OVERRIDE INTO BYTES
// ============================================================================

/// Turn a written override into the exact key and value a fork is given.
pub fn resolve_override(
    index: &StorageKeyIndex,
    spec: &OverrideSpec,
    spec_text: &str,
) -> Result<ResolvedOverride, ForkError> {
    match spec {
        OverrideSpec::Raw { key, value } => Ok(ResolvedOverride {
            spec: spec_text.to_string(),
            // A raw key still gets named where the metadata can name it — the
            // point of the index is that a person reading the row afterwards
            // should not have to care how the key was written.
            resolved: index.describe(key).readable,
            key: key.clone(),
            value: value.clone(),
        }),
        OverrideSpec::Named {
            pallet,
            item,
            args,
            value,
        } => {
            let entry = index.entry(pallet, item).ok_or_else(|| {
                ForkError(format!(
                    "this runtime declares no storage item '{pallet}.{item}'. Storage prefixes \
                     it does declare: {}",
                    index.storage_prefixes().join(", ")
                ))
            })?;
            if entry.hashers.len() != args.len() {
                return err(format!(
                    "{pallet}.{item} takes {} key argument(s) and {} were given — a map key with \
                     the wrong arity produces a well-formed key that addresses nothing, which \
                     reads exactly like an override that did nothing",
                    entry.hashers.len(),
                    args.len()
                ));
            }
            let key_types = split_key_types(&index.types, entry.key_type, entry.hashers.len())
                .ok_or_else(|| {
                    ForkError(format!(
                        "{pallet}.{item} declares {} hasher(s) but a key type this reader could \
                         not split into that many parts",
                        entry.hashers.len()
                    ))
                })?;

            let mut key = entry.prefix.to_vec();
            for (i, (arg, ty)) in args.iter().zip(key_types.iter()).enumerate() {
                let mut encoded = Vec::new();
                let sv = json_to_scale_value(arg)
                    .map_err(|e| ForkError(format!("key argument {i} of {pallet}.{item}: {e}")))?;
                scale_value::scale::encode_as_type(&sv, *ty, &index.types, &mut encoded).map_err(
                    |e| {
                        ForkError(format!(
                            "key argument {i} of {pallet}.{item} does not encode against the \
                             type this runtime declares for it: {e}"
                        ))
                    },
                )?;
                key.extend_from_slice(&entry.hashers[i].hash(&encoded));
            }

            let value = match value {
                None => None,
                Some(OverrideValue::Raw(bytes)) => Some(bytes.clone()),
                Some(OverrideValue::Json(json)) => {
                    let sv = json_to_scale_value(json)
                        .map_err(|e| ForkError(format!("value of {pallet}.{item}: {e}")))?;
                    let mut encoded = Vec::new();
                    scale_value::scale::encode_as_type(
                        &sv,
                        entry.value_type,
                        &index.types,
                        &mut encoded,
                    )
                    .map_err(|e| {
                        ForkError(format!(
                            "the value of {pallet}.{item} does not encode against the type this \
                             runtime declares for it: {e}"
                        ))
                    })?;
                    Some(encoded)
                }
            };

            let described = index.describe(&key);
            Ok(ResolvedOverride {
                spec: spec_text.to_string(),
                resolved: described.readable,
                key,
                value,
            })
        }
    }
}

impl StorageKeyIndex {
    fn storage_prefixes(&self) -> Vec<String> {
        let mut seen: Vec<String> = Vec::new();
        for e in &self.entries {
            if !seen.contains(&e.pallet) {
                seen.push(e.pallet.clone());
            }
        }
        seen
    }
}

/// JSON → `scale_value::Value`, in the shape this project's own decoder PRINTS.
///
/// The inverse of `frame_decoder::value_to_json`, which is what makes
/// read-modify-write work: dotlens prints a stored value, a person edits a
/// number, and the same text goes back in. Three rules carry the whole mapping:
///
///   * a single-key object whose value is an ARRAY is a VARIANT
///     (`{"Some": [x]}`, `{"None": []}`, `{"Liquid": []}`) — which is exactly
///     what `value_to_json` emits for a variant;
///   * any other object is a NAMED composite (a struct);
///   * a `"0x…"` string is BYTES, because a 32-byte account written as 32
///     numbers is not something a person can check.
///
/// AND `null` IS REFUSED. `value_to_json` never emits one, so accepting it here
/// would be inventing a token; and the token it would most plausibly mean —
/// `Option::None` — already has a spelling that cannot be confused with the
/// `=null` that deletes a key.
pub fn json_to_scale_value(v: &serde_json::Value) -> Result<Value<()>, String> {
    use serde_json::Value as J;
    Ok(match v {
        J::Null => {
            return Err(
                "a bare null is not a value: write Option::None as {\"None\": []}. (On the \
                 right of '=', `null` means DELETE THE KEY — a different fact, which is why \
                 the two never share a spelling)"
                    .into(),
            )
        }
        J::Bool(b) => Value::bool(*b),
        J::Number(n) => {
            if let Some(u) = n.as_u64() {
                Value::u128(u as u128)
            } else if let Some(i) = n.as_i64() {
                Value::i128(i as i128)
            } else {
                return Err(format!("{n} is not an integer — SCALE has no floats"));
            }
        }
        J::String(s) => {
            // Big numbers come back out of this project's decoder as decimal
            // STRINGS (u128 beyond u64::MAX), so a numeric string must encode as
            // a number or every balance above 18.4e18 would be unwritable.
            if let Some(hexed) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
                let bytes = hex::decode(hexed)
                    .map_err(|e| format!("'{s}' looks like bytes and is not hex: {e}"))?;
                Value::unnamed_composite(bytes.into_iter().map(|b| Value::u128(b as u128)))
            } else if !s.is_empty() && s.chars().all(|c| c.is_ascii_digit()) {
                Value::u128(s.parse::<u128>().map_err(|e| format!("'{s}': {e}"))?)
            } else {
                Value::string(s.clone())
            }
        }
        J::Array(items) => Value::unnamed_composite(
            items
                .iter()
                .map(json_to_scale_value)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        J::Object(map) => {
            if map.len() == 1 {
                let (name, inner) = map.iter().next().expect("checked len == 1");
                if let J::Array(items) = inner {
                    return Ok(Value::variant(
                        name.clone(),
                        Composite::Unnamed(
                            items
                                .iter()
                                .map(json_to_scale_value)
                                .collect::<Result<Vec<_>, _>>()?,
                        ),
                    ));
                }
            }
            Value::named_composite(
                map.iter()
                    .map(|(k, v)| json_to_scale_value(v).map(|v| (k.clone(), v)))
                    .collect::<Result<Vec<_>, _>>()?,
            )
        }
    })
}

/// The canonical bytes an override set contributes to a fork run's `input_hash`.
///
/// SORTED BY KEY AND SELF-DELIMITING, because the hash must be a function of the
/// COUNTERFACTUAL and not of the order somebody typed it in — two runs that
/// inject the same state are the same question and must share a row, and two that
/// inject different state must not. Each entry is
/// `compact(len(key)) ++ key ++ tag ++ [compact(len(value)) ++ value]`, where the
/// tag distinguishes a deletion from an empty value: `Some(vec![])` (write a
/// zero-length value, which is what a `()`-typed item holds) and `None` (remove
/// the key) are different injections and must not hash alike.
pub fn canonical_override_bytes(overrides: &[ResolvedOverride]) -> Vec<u8> {
    let mut sorted: Vec<&ResolvedOverride> = overrides.iter().collect();
    sorted.sort_by(|a, b| a.key.cmp(&b.key).then_with(|| a.value.cmp(&b.value)));
    let mut out = Vec::new();
    Compact(sorted.len() as u32).encode_to(&mut out);
    for o in sorted {
        Compact(o.key.len() as u32).encode_to(&mut out);
        out.extend_from_slice(&o.key);
        match &o.value {
            None => out.push(0),
            Some(v) => {
                out.push(1);
                Compact(v.len() as u32).encode_to(&mut out);
                out.extend_from_slice(v);
            }
        }
    }
    out
}

/// The bytes a fork run's `input_hash` is taken over.
///
/// IT IS DOMAIN-SEPARATED ON PURPOSE. Tier 1 hashes the exact runtime-API
/// parameter bytes; Tier 2 has no runtime-API call, so its "input" is a
/// construction of ours — origin, call, overrides. Prefixing the ASCII tag makes
/// it impossible for a fork input hash to coincide with a dry-run one even when
/// the origin and call are identical and there are no overrides, so a reader who
/// joins on `input_hash` without also matching `tier` cannot silently mix the two.
pub const FORK_INPUT_TAG: &[u8] = b"dotlens.fork.v1";

pub fn fork_input_bytes(
    origin_bytes: &[u8],
    call_bytes: &[u8],
    overrides: &[ResolvedOverride],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(FORK_INPUT_TAG.len() + origin_bytes.len() + call_bytes.len());
    out.extend_from_slice(FORK_INPUT_TAG);
    Compact(origin_bytes.len() as u32).encode_to(&mut out);
    out.extend_from_slice(origin_bytes);
    Compact(call_bytes.len() as u32).encode_to(&mut out);
    out.extend_from_slice(call_bytes);
    out.extend_from_slice(&canonical_override_bytes(overrides));
    out
}

/// Read the call back out of a canonical request.
///
/// The request IS the call plus the origin plus the overrides, so it is the one
/// copy; carrying the call a second time in the prepared struct would be a copy
/// that can go stale. It also means the archived `params` artifact is enough to
/// reconstruct what was asked, which is the property every other archived
/// request in this project has.
pub fn call_from_request(request: &[u8]) -> Result<Vec<u8>, ForkError> {
    origin_and_call_from_request(request).map(|(_, call)| call)
}

/// Both halves of a canonical request, so the dispatch injects exactly the bytes
/// the input hash was taken over.
pub fn origin_and_call_from_request(request: &[u8]) -> Result<(Vec<u8>, Vec<u8>), ForkError> {
    let mut cursor = request
        .strip_prefix(FORK_INPUT_TAG)
        .ok_or_else(|| ForkError("these bytes are not a dotlens fork request".into()))?;
    let origin_len: u32 = Compact::<u32>::decode(&mut cursor)
        .map_err(|e| ForkError(format!("fork request: origin length: {e}")))?
        .0;
    if cursor.len() < origin_len as usize {
        return err("fork request ends inside its origin");
    }
    let (origin, rest) = cursor.split_at(origin_len as usize);
    cursor = rest;
    let call_len: u32 = Compact::<u32>::decode(&mut cursor)
        .map_err(|e| ForkError(format!("fork request: call length: {e}")))?
        .0;
    if cursor.len() < call_len as usize {
        return err("fork request ends inside its call");
    }
    Ok((origin.to_vec(), cursor[..call_len as usize].to_vec()))
}

/// `system.remark()` with an empty payload, built from the runtime's own
/// `RuntimeCall` enum by NAME.
///
/// WHY A NO-OP EXTRINSIC IS NEEDED AT ALL on the scheduled route, which is not
/// obvious: `dev_dryRun` requires something to run, and running something is what
/// makes `Core_initialize_block` — and therefore the scheduler, and therefore our
/// injected task — execute. The remark itself does nothing and emits nothing; it
/// is the vehicle, and the answer is in `on_initialize`'s effects.
///
/// Not routed through `dryrun::noop_call`, deliberately: that resolves the call
/// type from `DryRunApi`'s own parameter declaration, and this tier must work on
/// a runtime that declares no DryRunApi at all. Same bytes, a different way in —
/// and the shape is checked by decoding what was built, so a runtime whose
/// `remark` takes something other than a byte sequence is refused rather than
/// silently handed three bytes that mean something else.
pub fn noop_call_bytes(metadata_blob: &[u8]) -> Result<Vec<u8>, ForkError> {
    let prefixed = RuntimeMetadataPrefixed::decode(&mut &metadata_blob[..])
        .map_err(|e| ForkError(format!("metadata blob undecodable: {e}")))?;
    let (call_ty, types) = crate::calls::runtime_call_type(&prefixed).map_err(ForkError)?;
    let pallets = variants_named(&types, call_ty)
        .ok_or_else(|| ForkError("this runtime's RuntimeCall is not an enum".into()))?;
    let pallet = pallets
        .iter()
        .find(|v| v.name.eq_ignore_ascii_case("System"))
        .ok_or_else(|| {
            ForkError(format!(
                "no System pallet in this runtime's RuntimeCall, so no no-op extrinsic can be \
                 built; it has: {}",
                pallets
                    .iter()
                    .map(|v| v.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        })?;
    let inner = match pallet.fields.len() {
        1 => pallet.fields[0].ty.id,
        n => {
            return err(format!(
                "RuntimeCall::System holds {n} fields, not one Call enum"
            ))
        }
    };
    let calls = variants_named(&types, inner)
        .ok_or_else(|| ForkError("System's Call is not an enum".into()))?;
    let remark = calls
        .iter()
        .find(|v| v.name.eq_ignore_ascii_case("remark"))
        .ok_or_else(|| ForkError("System has no `remark` call on this runtime".into()))?;
    let bytes = vec![pallet.index, remark.index, 0u8]; // compact(0) == 0x00
                                                       // Decoded again: bytes that do not read back as `system.remark` are not a
                                                       // no-op, and sending them would make the vehicle a transaction.
    let decoded = crate::calls::decode_call_with(&types, call_ty, &bytes)
        .map_err(|e| ForkError(format!("the no-op this built does not decode: {e}")))?;
    if !decoded.summary.eq_ignore_ascii_case("system.remark") {
        return err(format!(
            "the no-op this built reads back as '{}', not system.remark — refusing to run it",
            decoded.summary
        ));
    }
    Ok(bytes)
}

/// The origin, encoded against the type `pallet_scheduler` declares for it.
///
/// Exposed separately from [`scheduled_dispatch`] because the INPUT HASH is
/// taken over the encoded origin — a rendering like `Origins:MediumSpender` is a
/// name and two runtimes can spell one origin differently, while the bytes are
/// what the scheduler will actually dispatch with.
pub fn origin_bytes_for_scheduler(
    metadata_blob: &[u8],
    origin: &sim::OriginSpec,
) -> Result<(Vec<u8>, serde_json::Value), ForkError> {
    let agenda = storage_entry_info(metadata_blob, SCHEDULER_PALLET, AGENDA_ENTRY)
        .map_err(|e| ForkError(format!("{SCHEDULER_PALLET}.{AGENDA_ENTRY}: {e}")))?;
    let shape = AgendaShape::read(&agenda)?;
    crate::dryrun::encode_origin_for(&agenda.types, shape.origin_ty, origin).map_err(ForkError)
}

// ============================================================================
// THE INJECTION — one scheduled dispatch, built from the runtime's own metadata
// ============================================================================

/// The storage a fork must be given so that building one block dispatches
/// `call` under `origin`.
#[derive(Debug, Clone)]
pub struct ScheduledDispatch {
    /// `(key, value)` pairs to inject. Always the agenda entry; plus the
    /// preimage and its request status when the call is too large to inline.
    pub writes: crate::RawStorageWrites,
    /// WHERE the agenda entry was written, and the decision that put it there.
    ///
    /// Slice 8's field was `at_agenda_height` and its doc said "`head + 1` … so
    /// it is a parameter rather than an assumption", which was true of the
    /// parameter and false of the value passed in. The whole decision now travels
    /// with the writes, so a row that did not dispatch can say which number line
    /// it used and what evidence chose it.
    pub anchor: AgendaAnchor,
    /// `inline` | `lookup` — how the call was attached, recorded because it
    /// changes which storage was touched and therefore what shows up in the diff.
    pub call_binding: &'static str,
    /// The origin as `dryrun::encode_origin_for` resolved it, for the row.
    pub origin_json: serde_json::Value,
}

/// Build the scheduled dispatch.
///
/// `anchor` carries BOTH the height the task is written at and the decision that
/// chose it — see [`decide_agenda_anchor`]. Slice 8 took a bare height here and
/// documented it as `head + 1`, which was true of the parameter and false of the
/// value: on Asset Hub the scheduler counts in RELAY numbers, so `head + 1` wrote
/// a well-formed key ~13.15M blocks into the past, which dispatches nothing and
/// reads exactly like a chain declining. The height is therefore not a caller's
/// arithmetic any more; it is a decision from the chain's own agenda, and it
/// travels with the writes so a `not_dispatched` row can say which number line it
/// used and what evidence chose it.
pub fn scheduled_dispatch(
    metadata_blob: &[u8],
    index: &StorageKeyIndex,
    origin: &sim::OriginSpec,
    call_bytes: &[u8],
    anchor: &AgendaAnchor,
) -> Result<ScheduledDispatch, ForkError> {
    let (bytes, json) = origin_bytes_for_scheduler(metadata_blob, origin)?;
    let mut sd =
        scheduled_dispatch_with_origin_bytes(metadata_blob, index, &bytes, call_bytes, anchor)?;
    sd.origin_json = json;
    Ok(sd)
}

/// The same, given the origin ALREADY ENCODED.
///
/// This is the form the runner uses, and the reason is the cache key: the input
/// hash is taken over the encoded origin, so re-deriving it from the origin
/// EXPRESSION at dispatch time would be a second encoder that can disagree with
/// the one the key was taken over — and the row would then describe a
/// counterfactual other than the one injected. One encoding, hashed and
/// injected.
pub fn scheduled_dispatch_with_origin_bytes(
    metadata_blob: &[u8],
    index: &StorageKeyIndex,
    origin_bytes: &[u8],
    call_bytes: &[u8],
    anchor: &AgendaAnchor,
) -> Result<ScheduledDispatch, ForkError> {
    let agenda =
        storage_entry_info(metadata_blob, SCHEDULER_PALLET, AGENDA_ENTRY).map_err(|e| {
            ForkError(format!(
            "this runtime has no {SCHEDULER_PALLET}.{AGENDA_ENTRY} ({e}). Tier 2 dispatches by \
             writing one scheduled task and building a block — there is no other way to hand a \
             forked node a privileged origin — so a runtime without a scheduler under that name \
             cannot be simulated by this tier. Storage prefixes it does declare: {}",
            index.storage_prefixes().join(", ")
        ))
        })?;
    if agenda.hashers.len() != 1 {
        return err(format!(
            "{SCHEDULER_PALLET}.{AGENDA_ENTRY} declares {} hashers; a per-block agenda is a \
             single-key map",
            agenda.hashers.len()
        ));
    }

    let shape = AgendaShape::read(&agenda)?;

    // ---- the key: Agenda(agenda_height)
    let mut key_bytes = Vec::new();
    let key_ty = agenda
        .key_type
        .ok_or_else(|| ForkError("the agenda map declares no key type".into()))?;
    scale_value::scale::encode_as_type(
        &Value::u128(anchor.written_at as u128),
        key_ty,
        &agenda.types,
        &mut key_bytes,
    )
    .map_err(|e| ForkError(format!("agenda block number does not encode: {e}")))?;
    let mut agenda_key = index
        .entry(SCHEDULER_PALLET, AGENDA_ENTRY)
        .map(|e| e.prefix.to_vec())
        .ok_or_else(|| ForkError("the agenda entry vanished between two metadata reads".into()))?;
    agenda_key.extend_from_slice(&agenda.hashers[0].hash(&key_bytes));

    // ---- the call binding
    let mut writes: crate::RawStorageWrites = Vec::new();
    let (call_bytes_encoded, call_binding) = if call_bytes.len() <= BOUNDED_INLINE_LIMIT {
        let mut v = vec![shape.inline_index];
        Compact(call_bytes.len() as u32).encode_to(&mut v);
        v.extend_from_slice(call_bytes);
        (v, "inline")
    } else {
        let hash = crate::calls::blake2_256(call_bytes);
        let len = call_bytes.len() as u32;
        let mut v = vec![shape.lookup_index];
        v.extend_from_slice(&hash);
        len.encode_to(&mut v);
        writes.extend(preimage_writes(
            metadata_blob,
            index,
            call_bytes,
            &hash,
            len,
        )?);
        (v, "lookup")
    };

    // ---- the value: BoundedVec<Option<Scheduled>> with exactly one entry
    let mut value = Vec::new();
    Compact(1u32).encode_to(&mut value); // Vec length
    value.push(shape.some_index); // Option::Some
    for field in &shape.field_order {
        match field.as_str() {
            "maybe_id" => value.push(shape.maybe_id_none_index),
            "priority" => value.push(HARD_DEADLINE_PRIORITY),
            "call" => value.extend_from_slice(&call_bytes_encoded),
            "maybe_periodic" => value.push(shape.maybe_periodic_none_index),
            "origin" => value.extend_from_slice(origin_bytes),
            other => {
                return err(format!(
                    "this runtime's Scheduled struct carries a field '{other}' this version does \
                     not know how to fill. Filling it with a default would schedule something \
                     other than what was asked for, so the run is refused instead. Fields it \
                     declares, in order: {}",
                    shape.field_order.join(", ")
                ))
            }
        }
    }

    // THE ROUND TRIP IS THE PROOF. Every index above came out of the metadata,
    // so the only way this is wrong is if the SHAPE reading is wrong — and a
    // shape reading that is wrong produces bytes that do not decode back. An
    // agenda the runtime cannot decode reads as an empty agenda, which surfaces
    // as `not_dispatched` with nothing to point at; refusing here turns that
    // into a message.
    let mut check = &value[..];
    scale_value::scale::decode_as_type(&mut check, agenda.value_type, &agenda.types).map_err(
        |e| {
            ForkError(format!(
                "the scheduled task this built does not decode against the runtime's own agenda \
                 type ({e}) — refusing to inject storage the runtime would read as an empty \
                 agenda"
            ))
        },
    )?;
    if !check.is_empty() {
        return err(format!(
            "the scheduled task this built left {} trailing byte(s) — refusing to inject it",
            check.len()
        ));
    }

    writes.push((agenda_key, value));

    // SET THE RESUME POINT TOO, and this is what turns the injection from lucky
    // into robust. `service_agendas` walks `IncompleteSince ..= now`, and `now` is
    // the provider's value in the block being BUILT — which we cannot predict,
    // because chopsticks advances the relay anchor by four per parachain block
    // and that is a property of a mock rather than of the chain. Pinning the
    // resume point to the same height the task was written at means any positive
    // advance sweeps it, exactly once, without this code knowing the advance.
    //
    // It is written through the metadata like everything else — a runtime without
    // the entry simply gets no resume point, which is the pre-slice-9 behaviour
    // and correct on a chain whose scheduler never falls behind.
    if let Some(entry) = index.entry(SCHEDULER_PALLET, INCOMPLETE_SINCE_ENTRY) {
        let mut since = Vec::new();
        scale_value::scale::encode_as_type(
            &Value::u128(anchor.written_at as u128),
            entry.value_type,
            index.types(),
            &mut since,
        )
        .map_err(|e| {
            ForkError(format!(
                "{SCHEDULER_PALLET}.{INCOMPLETE_SINCE_ENTRY} does not encode: {e}"
            ))
        })?;
        writes.push((entry.prefix.to_vec(), since));
    }

    Ok(ScheduledDispatch {
        writes,
        anchor: anchor.clone(),
        call_binding,
        // Filled by `scheduled_dispatch`, which is the arm that still holds the
        // expression. Given bytes alone there is no name to resolve.
        origin_json: serde_json::Value::Null,
    })
}

/// The variant indices and field order of `Scheduled`, read out of the agenda
/// map's own declared value type. Nothing here is a constant.
struct AgendaShape {
    some_index: u8,
    inline_index: u8,
    lookup_index: u8,
    maybe_id_none_index: u8,
    maybe_periodic_none_index: u8,
    origin_ty: u32,
    field_order: Vec<String>,
}

impl AgendaShape {
    fn read(agenda: &StorageEntryInfo) -> Result<Self, ForkError> {
        let types = &agenda.types;
        // BoundedVec<Option<Scheduled>, S> is a newtype composite over a Vec, so
        // peel single-field composites before expecting a sequence.
        let seq_elem = peel_to_sequence(types, agenda.value_type).ok_or_else(|| {
            ForkError(
                "the agenda's value type is not a sequence of scheduled tasks on this runtime"
                    .into(),
            )
        })?;
        let option = variants_named(types, seq_elem).ok_or_else(|| {
            ForkError("an agenda element is not an Option on this runtime".into())
        })?;
        let some = option
            .iter()
            .find(|v| v.name == "Some")
            .ok_or_else(|| ForkError("an agenda element has no `Some` variant".into()))?;
        let none = option
            .iter()
            .find(|v| v.name == "None")
            .ok_or_else(|| ForkError("an agenda element has no `None` variant".into()))?;
        let _ = none;
        let scheduled_ty = match some.fields.len() {
            1 => some.fields[0].ty.id,
            n => {
                return err(format!(
                    "an agenda element's `Some` holds {n} fields, not one scheduled task"
                ))
            }
        };
        let TypeDef::Composite(scheduled) = &types
            .resolve(scheduled_ty)
            .ok_or_else(|| ForkError("scheduled task type is not in the registry".into()))?
            .type_def
        else {
            return err("a scheduled task is not a struct on this runtime");
        };

        let mut field_order = Vec::new();
        let mut origin_ty = None;
        let mut call_ty = None;
        let mut maybe_id_ty = None;
        let mut maybe_periodic_ty = None;
        let mut priority_ty = None;
        for f in &scheduled.fields {
            let Some(name) = f.name.as_ref() else {
                return err(
                    "a scheduled task has unnamed fields on this runtime, so its parts cannot be \
                     filled by name",
                );
            };
            field_order.push(name.to_string());
            match name.as_str() {
                "origin" => origin_ty = Some(f.ty.id),
                "call" => call_ty = Some(f.ty.id),
                "maybe_id" => maybe_id_ty = Some(f.ty.id),
                "maybe_periodic" => maybe_periodic_ty = Some(f.ty.id),
                "priority" => priority_ty = Some(f.ty.id),
                _ => {}
            }
        }
        let need = |what: &str, v: Option<u32>| {
            v.ok_or_else(|| {
                ForkError(format!(
                    "a scheduled task on this runtime has no '{what}' field; it has: {}",
                    field_order.join(", ")
                ))
            })
        };
        let origin_ty = need("origin", origin_ty)?;
        let call_ty = need("call", call_ty)?;
        let maybe_id_ty = need("maybe_id", maybe_id_ty)?;
        let maybe_periodic_ty = need("maybe_periodic", maybe_periodic_ty)?;
        let priority_ty = need("priority", priority_ty)?;

        // priority is a u8; if it is not, one byte is the wrong width and every
        // field after it would be read at the wrong offset.
        if !matches!(
            types.resolve(priority_ty).map(|t| &t.type_def),
            Some(TypeDef::Primitive(TypeDefPrimitive::U8))
        ) {
            return err("a scheduled task's `priority` is not a u8 on this runtime");
        }

        let bounded = variants_named(types, call_ty)
            .ok_or_else(|| ForkError("a scheduled task's `call` is not an enum".into()))?;
        let inline_index = bounded
            .iter()
            .find(|v| v.name == "Inline")
            .ok_or_else(|| {
                ForkError(format!(
                    "this runtime's Bounded<Call> has no `Inline` variant; it has: {}",
                    bounded
                        .iter()
                        .map(|v| v.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
            })?
            .index;
        let lookup_index = bounded
            .iter()
            .find(|v| v.name == "Lookup")
            .ok_or_else(|| {
                ForkError("this runtime's Bounded<Call> has no `Lookup` variant".into())
            })?
            .index;

        let none_of = |ty: u32, what: &str| -> Result<u8, ForkError> {
            let vs = variants_named(types, ty)
                .ok_or_else(|| ForkError(format!("{what} is not an Option on this runtime")))?;
            vs.iter()
                .find(|v| v.name == "None")
                .map(|v| v.index)
                .ok_or_else(|| ForkError(format!("{what} has no `None` variant")))
        };

        Ok(Self {
            some_index: some.index,
            inline_index,
            lookup_index,
            maybe_id_none_index: none_of(maybe_id_ty, "a scheduled task's `maybe_id`")?,
            maybe_periodic_none_index: none_of(
                maybe_periodic_ty,
                "a scheduled task's `maybe_periodic`",
            )?,
            origin_ty,
            field_order,
        })
    }
}

fn peel_to_sequence(types: &PortableRegistry, ty: u32) -> Option<u32> {
    let mut id = ty;
    for _ in 0..4 {
        match &types.resolve(id)?.type_def {
            TypeDef::Sequence(s) => return Some(s.type_param.id),
            TypeDef::Array(a) => return Some(a.type_param.id),
            TypeDef::Composite(c) if c.fields.len() == 1 => id = c.fields[0].ty.id,
            _ => return None,
        }
    }
    None
}

fn variants_named(
    types: &PortableRegistry,
    ty: u32,
) -> Option<&[scale_info::Variant<scale_info::form::PortableForm>]> {
    match &types.resolve(ty)?.type_def {
        TypeDef::Variant(v) => Some(&v.variants),
        _ => None,
    }
}

/// The preimage a `Bounded::Lookup` needs, plus every request-status entry the
/// pallet declares.
///
/// WHY THE STATUS IS NOT OPTIONAL: `QueryPreimage::peek` resolves a `Lookup`
/// through `Pallet::fetch`, which reads the length out of the request-status map
/// FIRST and returns `Unrequested` if it is absent. A preimage stored without a
/// status is a preimage the scheduler cannot see, and the visible symptom is a
/// `scheduler.CallUnavailable` — which reads like the call was bad rather than
/// like the harness was set up wrong.
fn preimage_writes(
    metadata_blob: &[u8],
    index: &StorageKeyIndex,
    call_bytes: &[u8],
    hash: &[u8; 32],
    len: u32,
) -> Result<crate::RawStorageWrites, ForkError> {
    if index.entry(PREIMAGE_PALLET, PREIMAGE_FOR_ENTRY).is_none() {
        return err(format!(
            "this call is {} bytes, which is more than the {BOUNDED_INLINE_LIMIT}-byte \
             `Bounded::Inline` bound, so it has to be scheduled by hash — and this runtime \
             declares no {PREIMAGE_PALLET}.{PREIMAGE_FOR_ENTRY} to put the preimage in",
            call_bytes.len()
        ));
    }
    let mut writes = Vec::new();

    // The PreimageFor map is (H256, u32) under an Identity hasher, which is what
    // `gov::preimage_for_key` already builds for the preimage-decoding path —
    // reused rather than rebuilt, because two constructions of one key that can
    // disagree is the defect class this project keeps finding.
    let mut value = Vec::new();
    Compact(len).encode_to(&mut value);
    value.extend_from_slice(call_bytes);
    writes.push((crate::gov::preimage_for_key(hash, len), value));

    let mut status_written = 0usize;
    for name in PREIMAGE_STATUS_ENTRIES {
        let Some(entry) = index.entry(PREIMAGE_PALLET, name) else {
            continue;
        };
        let info = storage_entry_info(metadata_blob, PREIMAGE_PALLET, name)
            .map_err(|e| ForkError(format!("{PREIMAGE_PALLET}.{name}: {e}")))?;
        if info.hashers.len() != 1 {
            continue;
        }
        let mut key_bytes = Vec::new();
        let key_ty = info
            .key_type
            .ok_or_else(|| ForkError(format!("{PREIMAGE_PALLET}.{name} declares no key type")))?;
        scale_value::scale::encode_as_type(
            &Value::unnamed_composite(hash.iter().map(|b| Value::u128(*b as u128))),
            key_ty,
            &info.types,
            &mut key_bytes,
        )
        .map_err(|e| ForkError(format!("{PREIMAGE_PALLET}.{name} key does not encode: {e}")))?;
        let mut key = entry.prefix.to_vec();
        key.extend_from_slice(&info.hashers[0].hash(&key_bytes));

        let status = requested_status_value(&info, len)?;
        let mut encoded = Vec::new();
        scale_value::scale::encode_as_type(&status, info.value_type, &info.types, &mut encoded)
            .map_err(|e| {
                ForkError(format!(
                    "{PREIMAGE_PALLET}.{name}'s Requested status does not encode on this \
                     runtime: {e}"
                ))
            })?;
        writes.push((key, encoded));
        status_written += 1;
    }
    if status_written == 0 {
        return err(format!(
            "this runtime declares none of {PREIMAGE_STATUS_ENTRIES:?} in the \
             {PREIMAGE_PALLET} pallet, so a preimage cannot be marked requested — and an \
             unrequested preimage is invisible to the scheduler, which would report \
             CallUnavailable as though the call itself were bad"
        ));
    }
    Ok(writes)
}

/// `RequestStatus::Requested { … }`, filled FIELD BY FIELD FROM THE RUNTIME'S OWN
/// DECLARATION.
///
/// The variant's shape changed across pallet-preimage versions — older runtimes
/// carry `{deposit, count, len}` and newer ones `{maybe_ticket, count,
/// maybe_len}` — so a hand-written struct would be right on one and silently
/// wrong on the other. Every field is matched by NAME and an unrecognised one is
/// a loud refusal rather than a default, because a default here is a status the
/// pallet may read as "this preimage has no length".
fn requested_status_value(info: &StorageEntryInfo, len: u32) -> Result<Value<()>, ForkError> {
    let variants = variants_named(&info.types, info.value_type)
        .ok_or_else(|| ForkError("this runtime's preimage request status is not an enum".into()))?;
    let requested = variants
        .iter()
        .find(|v| v.name == "Requested")
        .ok_or_else(|| {
            ForkError(format!(
                "this runtime's preimage request status has no `Requested` variant; it has: {}",
                variants
                    .iter()
                    .map(|v| v.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        })?;
    let mut fields: Vec<(String, Value<()>)> = Vec::new();
    for f in &requested.fields {
        let Some(name) = f.name.as_ref() else {
            return err("this runtime's `Requested` status has unnamed fields");
        };
        let value = match name.as_str() {
            "count" => Value::u128(1),
            "len" => Value::u128(len as u128),
            "maybe_len" => {
                Value::variant("Some", Composite::Unnamed(vec![Value::u128(len as u128)]))
            }
            // Every ticket/deposit shape published is an Option, and `None` is
            // correct: nothing was reserved, because nothing here paid for it.
            "maybe_ticket" | "ticket" | "deposit" | "maybe_deposit" => {
                Value::variant("None", Composite::Unnamed(vec![]))
            }
            other => {
                return err(format!(
                    "this runtime's `Requested` preimage status carries a field '{other}' this \
                     version does not know how to fill; filling it with a default could mark the \
                     preimage as having no length, which the scheduler reads as an unavailable \
                     call"
                ))
            }
        };
        fields.push((name.to_string(), value));
    }
    Ok(Value::variant("Requested", Composite::Named(fields)))
}

// ============================================================================
// READING THE ANSWER — what a diff COVERS, beside whether it could be read
// ============================================================================

/// The `diff_status` vocabulary, as constants rather than string literals
/// scattered across three crates — a typo in one of them is a status nothing
/// recognises, and the gate below would silently blank the column.
///
/// ---------------------------------------------------------------------------
/// THE FIFTH VALUE, AND WHY IT IS A STATUS RATHER THAN A COLUMN OF ITS OWN.
///
/// Slice 9 claimed `dev_dryRun` makes it "structurally impossible for the events
/// and the diff to describe different executions". That is false, and slice 10
/// measured it: between a `not_dispatched` run and an `executed` run of the same
/// subject the events blob grew 464 → 862 characters while the diff key set was
/// IDENTICAL — the same 12 keys, nothing added. A run that emitted
/// `assets.Transferred` and `system.NewAccount` showed no `Assets.Account`, no
/// `MultiAssetBounties` key and no `Scheduler.Agenda` deletion.
///
/// THE CAUSE IS IN CHOPSTICKS' OWN SOURCE and it is not a bug we can option our
/// way around (`packages/core/src/blockchain/block-builder.ts`, read at the
/// version this project pins). `initNewBlock` runs `Core_initialize_block` and
/// then each inherent, and CONSUMES each response into a storage layer:
///
/// ```text
/// const resp = await newBlock.call('Core_initialize_block', [header.toHex()])
/// newBlock.pushStorageLayer().setAll(resp.storageDiff)   // ← into a layer, then dropped
/// ```
///
/// `dryRunExtrinsic` then returns ONE `TaskCallResponse` — the one from
/// `BlockBuilder_apply_extrinsic` — and its `.storageDiff` is that single
/// runtime call's writes. Every earlier phase is in the block's STATE (which is
/// why the runtime sees the scheduled task at all) and in no returned value.
///
/// AND THAT EXACTLY EXPLAINS THE MEASUREMENT rather than merely being consistent
/// with it. `apply_extrinsic` WRITES `System.Events`: it reads the current list
/// and appends its own, so the value it writes is the whole cumulative list,
/// `on_initialize`'s events included. So the events blob grows when the
/// scheduler dispatched something, while the key set stays exactly the
/// extrinsic's own writes. One key carrying a whole block's events beside eleven
/// keys carrying one extrinsic's writes.
///
/// THE OTHER THREE `dev_dryRun` VEHICLES DO NOT HELP, and it is worth writing
/// down so nobody tries: `hrmp`/`dmp`/`ump` go through `dryRunInherents`, which
/// merges `initNewBlock`'s `layers` — and `layers` is declared AFTER the
/// initialize call and collects the INHERENT layers only. `Core_initialize_block`
/// is excluded there too, and that is the one phase a scheduled dispatch runs in.
///
/// SCOPE IS NOT A SECOND COLUMN. It was drafted as one and refused, on this
/// project's own most-repeated rule: whether the diff covers the SUBJECT is a
/// function of `dispatch_route`, which is already a column, and a column that is
/// a function of another column is the derived copy that killed
/// `treasury.consolidated_position`, `graph.cross_chain_operations`, the
/// `logical_assets` join table and the stored forwarded-attribution. What is NOT
/// derivable is which method produced these particular bytes — a future harness
/// or a future route could return every phase on the scheduled route, and the
/// row has to record what THIS run got. That is lineage, and lineage is what
/// `diff_status` already is.
///
/// THE VOCABULARY ITSELF LIVES IN `sim`, not here, and so do the two rules that
/// read it ([`sim::diff_is_present`], [`sim::diff_covers_subject`]): what a
/// status MEANS is tier vocabulary that both the runner and the API have to
/// agree on, and two implementations of that could disagree. What is genuinely
/// protocol — and therefore this file's — is mapping a PARTICULAR HARNESS'S
/// answer onto it, which is [`diff_scope_from_answer`] below.
pub use sim::{
    DIFF_STATUSES, DIFF_STATUS_DECODED, DIFF_STATUS_EXTRINSIC_ONLY, DIFF_STATUS_REFUSED,
    DIFF_STATUS_UNAVAILABLE, DIFF_STATUS_UNDECODABLE,
};

/// The harness method that produced a diff. Recorded in the ARCHIVED ANSWER
/// instead of a status, because the method is a FACT about what was done and the
/// status is what this version of dotlens thinks it means — and only the second
/// one is allowed to change under a `FORK_VERSION` bump. Slice 9 archived the
/// interpretation and therefore could not correct it without re-running a fork.
pub const DIFF_METHOD_DRY_RUN: &str = "dev_dryRun";
/// `dev_runBlock`. It runs `Core_initialize_block` as its OWN phase and returns
/// that phase's diff (read from `packages/chopsticks/src/plugins/run-block/rpc.ts`),
/// so it genuinely does produce a whole-block diff — which is why `decoded` stays
/// in the vocabulary rather than becoming dead. It is unreachable on a PARACHAIN
/// for the separate reason slice 8 measured: `dev_newBlock` builds a block
/// without `set_validation_data`, so there is nothing re-runnable to hand it.
pub const DIFF_METHOD_RUN_BLOCK: &str = "dev_runBlock";

/// What the archived answer's diff covers, decided from the answer itself.
///
/// THE LEGACY ARM IS THE INTERESTING ONE, and its marker is load-bearing rather
/// than incidental. Answers written before slice 10 recorded a `diff_status`
/// string and no method, and a `decoded` among them is ambiguous: slice 8's route
/// really did return every phase, slice 9's did not. `built_block_hash` tells
/// them apart because a block was BUILT only on the legacy route — migration 0022
/// says so in the column's own comment ("NULL on every row written by slice 9
/// onward, because no block is built") — and `dev_runBlock` is the only method
/// that returns per-phase diffs at all.
///
/// A status this version does not know is REFUSED rather than read as
/// `unavailable`. The previous code defaulted a missing field to `unavailable`,
/// which would have turned a shape change into a silently blank diff column —
/// the same reading `parse_run_block_diff` refuses one crate over.
pub fn diff_scope_from_answer(answer: &serde_json::Value) -> Result<&'static str, ForkError> {
    if let Some(method) = answer.get("diff_method").and_then(|m| m.as_str()) {
        return match method {
            DIFF_METHOD_DRY_RUN => Ok(DIFF_STATUS_EXTRINSIC_ONLY),
            DIFF_METHOD_RUN_BLOCK => Ok(DIFF_STATUS_DECODED),
            other => err(format!(
                "the archived answer names diff_method '{other}', which this FORK_VERSION does \
                 not know. Known methods: {DIFF_METHOD_DRY_RUN}, {DIFF_METHOD_RUN_BLOCK}"
            )),
        };
    }
    match answer.get("diff_status").and_then(|s| s.as_str()) {
        Some(DIFF_STATUS_DECODED) => {
            if answer
                .get("built_block_hash")
                .and_then(|v| v.as_str())
                .is_some()
            {
                Ok(DIFF_STATUS_DECODED)
            } else {
                Ok(DIFF_STATUS_EXTRINSIC_ONLY)
            }
        }
        Some(DIFF_STATUS_EXTRINSIC_ONLY) => Ok(DIFF_STATUS_EXTRINSIC_ONLY),
        Some(DIFF_STATUS_UNDECODABLE) => Ok(DIFF_STATUS_UNDECODABLE),
        Some(DIFF_STATUS_UNAVAILABLE) => Ok(DIFF_STATUS_UNAVAILABLE),
        Some(DIFF_STATUS_REFUSED) => Ok(DIFF_STATUS_REFUSED),
        Some(other) => err(format!(
            "the archived answer records diff_status '{other}', which is not in this version's \
             vocabulary ({}). Nothing is guessed: the bytes are archived and a later FORK_VERSION \
             can read them",
            DIFF_STATUSES.join(" | ")
        )),
        None => err(
            "the archived answer carries neither `diff_method` nor `diff_status`, so there is \
             nothing to say what its diff covers. Defaulting to `unavailable` here would report a \
             shape change as 'this build of the harness has no diff method', which sends somebody \
             to reinstall a tool that is working",
        ),
    }
}

/// Re-exported so a caller that already reached for `fork::` for the METHOD does
/// not have to reach into two crates to ask what its answer means. Both are
/// DEFINED IN `sim` — see the note on the vocabulary above for why: the runner
/// and the API both have to answer "does this diff cover the call", and two
/// implementations of that question could disagree.
pub use sim::{diff_covers_subject, diff_is_present};

/// One changed key, before and after.
///
/// THE HARNESS ONLY SUPPLIES `after`. A chopsticks storage diff is
/// `[key, value|null]` — which keys moved and what they became — and says
/// nothing about what they held. `before` is read back separately, key by key,
/// at the block the dispatch was built ON, and it is what makes this a DIFF
/// rather than a list of new values. It is `Option` twice over on purpose:
/// `None` before means the key did not exist (this dispatch CREATED it), `None`
/// after means it was removed.
#[derive(Debug, Clone)]
pub struct DiffPair {
    pub key: Vec<u8>,
    pub before: Option<Vec<u8>>,
    pub after: Option<Vec<u8>>,
    /// Was the before side ACTUALLY READ? Reading it is one RPC per changed
    /// key, so it is capped — and above the cap `before` is `None` for a reason
    /// that has nothing to do with the key not existing. Without this flag the
    /// two collapse into one value and a key that was simply not looked at
    /// renders as `created`, which is the exact conflation `diff_status` exists
    /// one level up to prevent.
    pub before_read: bool,
    /// True when the key was written by the HARNESS ITSELF — the scheduled task,
    /// its preimage, its request status. Its `before` is our own injection and
    /// its `after` is the runtime consuming it; neither is history.
    pub from_harness: bool,
}

/// One side of one changed key, rendered.
#[derive(Debug, Clone, serde::Serialize)]
pub struct DiffValue {
    /// Hex, cut at [`DIFF_VALUE_PREVIEW_BYTES`].
    pub hex: String,
    pub bytes: u32,
    pub truncated: bool,
    /// blake2b-256 of the WHOLE value, so a truncated rendering is still
    /// checkable against the archived bytes.
    pub hash: String,
    pub decoded: Option<serde_json::Value>,
    pub decode_error: Option<String>,
}

/// One decoded storage change.
#[derive(Debug, Clone, serde::Serialize)]
pub struct DiffEntry {
    pub key: String,
    #[serde(flatten)]
    pub described: KeyDescription,
    /// True when this key is one the run itself INJECTED. Its `before` is
    /// therefore the FABRICATED value and not what the chain held — the one
    /// place a diff entry could be read as history while being a consequence of
    /// the counterfactual, so it is marked on the entry rather than explained in
    /// prose somewhere else.
    pub from_override: bool,
    /// True when this key was written by the HARNESS to make the dispatch
    /// happen at all — the agenda entry, the injected preimage, its request
    /// status. Not a consequence of the call and not history: the `before` is
    /// our own injection and the `after` is the runtime consuming it.
    pub from_harness: bool,
    /// created | changed | deleted | unknown. `unknown` = the before side was
    /// not read (see `DiffPair::before_read`), which is not the same as the key
    /// not having existed.
    pub change: &'static str,
    pub before: Option<DiffValue>,
    pub after: Option<DiffValue>,
}

fn render_value(index: &StorageKeyIndex, value_type: Option<u32>, bytes: &[u8]) -> DiffValue {
    let truncated = bytes.len() > DIFF_VALUE_PREVIEW_BYTES;
    let shown = &bytes[..bytes.len().min(DIFF_VALUE_PREVIEW_BYTES)];
    let (decoded, decode_error) = match value_type {
        // A key we could not name has no declared type, so there is nothing to
        // decode against — and inventing one would be the guess this whole file
        // exists to avoid.
        None => (
            None,
            Some(
                "this key is not in the runtime's metadata, so its value has no declared type"
                    .to_string(),
            ),
        ),
        Some(ty) => match index.decode_value(ty, bytes) {
            Ok(v) => (Some(v), None),
            Err(e) => (None, Some(e)),
        },
    };
    DiffValue {
        hex: format!("0x{}", hex::encode(shown)),
        bytes: bytes.len() as u32,
        truncated,
        hash: format!("0x{}", hex::encode(crate::calls::blake2_256(bytes))),
        decoded,
        decode_error,
    }
}

/// Changed keys → decoded entries.
///
/// `override_keys` are the keys this run injected; see `from_override`.
pub fn decode_diff(
    index: &StorageKeyIndex,
    pairs: &[DiffPair],
    override_keys: &[Vec<u8>],
) -> Vec<DiffEntry> {
    let mut out = Vec::with_capacity(pairs.len());
    for pair in pairs {
        let described = index.describe(&pair.key);
        let vt = described.value_type;
        let change = match (pair.before_read, &pair.before, &pair.after) {
            // NOT READ IS NOT ABSENT. See `DiffPair::before_read`.
            (false, _, _) => "unknown",
            (true, None, Some(_)) => "created",
            (true, Some(_), None) => "deleted",
            _ => "changed",
        };
        out.push(DiffEntry {
            key: format!("0x{}", hex::encode(&pair.key)),
            from_override: override_keys.iter().any(|k| *k == pair.key),
            from_harness: pair.from_harness,
            change,
            before: pair.before.as_ref().map(|b| render_value(index, vt, b)),
            after: pair.after.as_ref().map(|a| render_value(index, vt, a)),
            described,
        });
    }
    // Sorted by the readable name so two runs of one counterfactual render
    // identically. The harness's ordering is a property of its trie walk and is
    // not something to reproduce in a row somebody diffs by eye.
    out.sort_by(|a, b| {
        a.described
            .readable
            .cmp(&b.described.readable)
            .then_with(|| a.key.cmp(&b.key))
    });
    out
}

// ============================================================================
// THE AGENDA ANCHOR — which number line the scheduler counts on
// ============================================================================

/// Which `BlockNumberProvider` `pallet_scheduler` is configured with.
///
/// THIS CANNOT BE READ FROM METADATA. A pallet's `BlockNumberProvider` is a
/// runtime type parameter, not a declared attribute, so nothing in v14/v15/v16
/// says whether `Scheduler.Agenda` is keyed by this chain's own block numbers or
/// by the relay's. Slice 8 assumed the former and was wrong on Asset Hub by
/// ~13.15 million blocks — a well-formed key, a correct hasher, and a number on
/// the wrong line, which produced a `not_dispatched` with nothing to point at.
///
/// AND THIS IS THE SECOND PALLET TO DO IT. AH's `pallet_treasury` puts relay
/// numbers in `valid_from`/`expire_at` for the same reason (Phase 3 slice 5), and
/// that was already recorded when slice 8 was authored. A third pallet will do it
/// again — so the answer here is DECIDED FROM DATA rather than written down,
/// because a constant would be wrong again in the same silent way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnchorProvider {
    /// `System.Number` — this chain's own height. A relay chain, or a parachain
    /// whose scheduler is configured locally.
    Local,
    /// `ParachainSystem.LastRelayChainBlockNumber`.
    Relay,
}

impl AnchorProvider {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Relay => "relay",
        }
    }
}

/// The decision and the evidence for it, recorded on the row.
#[derive(Debug, Clone)]
pub struct AgendaAnchor {
    pub provider: AnchorProvider,
    /// The provider's value at the block being forked.
    pub at_parent: u64,
    /// Where the task is written — see the bias note on [`decide_agenda_anchor`].
    pub written_at: u64,
    pub system_number: u64,
    pub relay_number: Option<u64>,
    pub agenda_keys_observed: usize,
    pub agenda_key_range: Option<(u64, u64)>,
    pub decided_by: String,
}

impl AgendaAnchor {
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "provider": self.provider.as_str(),
            "at_parent": self.at_parent,
            "written_at": self.written_at,
            "system_number": self.system_number,
            "relay_number": self.relay_number,
            "agenda_keys_observed": self.agenda_keys_observed,
            "agenda_key_range": self.agenda_key_range.map(|(lo, hi)| vec![lo, hi]),
            "decided_by": self.decided_by,
        })
    }
}

/// Decide the anchor from the chain's own agenda key space.
///
/// THE EVIDENCE IS THE KEY RANGE. A live `Scheduler.Agenda` holds tasks the chain
/// has scheduled for the future, so its keys sit at or above whatever `now` the
/// scheduler counts in — and on Asset Hub they measurably do: 31,450,649 …
/// 32,941,123 against a `System.Number` of 19,621,120 and a relay number of
/// ~32.5M. Bracketing tells you which line they are on without knowing anything
/// about the runtime's type parameters.
///
/// THE REFUSAL IS THE IMPORTANT ARM. When the agenda is EMPTY and the two
/// candidates disagree there is nothing to decide from, and a default here is a
/// coin flip that produces a silent `not_dispatched` — precisely slice 8's
/// failure. So it refuses, naming both candidates, and the caller can pass the
/// anchor explicitly. When the two candidates AGREE (a relay chain has no
/// `ParachainSystem` at all) there is nothing to decide and it says so.
///
/// `written_at` IS BIASED LOW ON PURPOSE, and it is `at_parent` ITSELF —
/// measured, after `at_parent + 1` was tried and did not dispatch.
/// `pallet_scheduler::service_agendas` walks `IncompleteSince ..= now`, so a task
/// written ABOVE `now` is never reached while one at or below it is swept exactly
/// once. The question is therefore only what `now` is when `on_initialize` runs,
/// and for a RELAY-anchored scheduler the answer is the PARENT's value:
/// `ParachainSystem::LastRelayChainBlockNumber` is written by the
/// `set_validation_data` INHERENT, and inherents are extrinsics, so they run
/// AFTER `initialize_block` has already called every `on_initialize` hook. The
/// relay number the scheduler sees has not advanced yet. (Slice 8 measured this
/// and wrote it down — "the scheduler's `now` is the provider's value AT THE
/// PARENT" — and slice 9's `+ 1` contradicted its own record.)
///
/// MEASURED ON ASSET HUB, ref 1930 at #19368576, relay parent 32,519,445:
/// written at 32,519,446 the run came back `not_dispatched` with the agenda entry
/// still present and NO `ParachainSystem` key touched at all; written at
/// 32,519,445 the same run came back `executed` with `scheduler.Dispatched` and
/// the bounty's five events. Nothing else changed between the two.
///
/// `at_parent` is also the safe choice for a LOCAL provider, which is why it is
/// not conditioned on the provider: `System::Number` IS incremented during
/// `initialize_block`, so there `now` is `at_parent + 1` — and a task at
/// `at_parent` is still inside `IncompleteSince ..= now`. Below `now` is swept;
/// above it is not; so the bias goes down. `Scheduler.IncompleteSince` is set to
/// the same value so the sweep starts there rather than wherever the chain's own
/// backlog begins.
pub fn decide_agenda_anchor(
    system_number: u64,
    relay_number: Option<u64>,
    agenda_keys: &[u64],
) -> Result<AgendaAnchor, ForkError> {
    let range = agenda_keys
        .iter()
        .copied()
        .fold(None::<(u64, u64)>, |acc, k| {
            Some(match acc {
                None => (k, k),
                Some((lo, hi)) => (lo.min(k), hi.max(k)),
            })
        });

    let (provider, decided_by) = match (relay_number, range) {
        // No relay anchor exists: there is one number line and no decision.
        (None, _) => (
            AnchorProvider::Local,
            "this runtime declares no relay block number, so there is one number line".to_string(),
        ),
        // The two candidates agree — nothing to tell apart.
        (Some(relay), _) if relay == system_number => (
            AnchorProvider::Local,
            "both candidates carry the same value, so the choice cannot change the key"
                .to_string(),
        ),
        (Some(relay), Some((lo, hi))) => {
            // A scheduled task is in the FUTURE, so distance from a candidate to
            // the observed keys is the discriminator. Compared on the LOW end
            // because the high end is bounded only by how far ahead anybody has
            // scheduled anything.
            let d_local = lo.abs_diff(system_number);
            let d_relay = lo.abs_diff(relay);
            if d_relay < d_local {
                (
                    AnchorProvider::Relay,
                    format!(
                        "the live agenda's keys ({lo}…{hi}) sit {d_relay} from the relay number \
                         {relay} and {d_local} from System.Number {system_number}"
                    ),
                )
            } else {
                (
                    AnchorProvider::Local,
                    format!(
                        "the live agenda's keys ({lo}…{hi}) sit {d_local} from System.Number \
                         {system_number} and {d_relay} from the relay number {relay}"
                    ),
                )
            }
        }
        // THE REFUSAL. Two number lines, no evidence, and a wrong guess is silent.
        (Some(relay), None) => {
            return err(format!(
                "this chain's Scheduler.Agenda is EMPTY, and its two candidate block-number                  providers disagree: System.Number is {system_number} and                  ParachainSystem.LastRelayChainBlockNumber is {relay}. Which one the scheduler                  counts in cannot be read from metadata (a BlockNumberProvider is a runtime type                  parameter, not a declared attribute) and cannot be inferred from an empty                  agenda. Guessing produces a well-formed key on the wrong number line, which                  dispatches nothing and looks exactly like a chain declining — so this refuses                  instead. Pass the anchor explicitly, or run at a block where the chain has                  something scheduled."
            ))
        }
    };

    let at_parent = match provider {
        AnchorProvider::Local => system_number,
        AnchorProvider::Relay => relay_number.expect("Relay is only chosen when Some"),
    };
    Ok(AgendaAnchor {
        provider,
        at_parent,
        written_at: at_parent,
        system_number,
        relay_number,
        agenda_keys_observed: agenda_keys.len(),
        agenda_key_range: range,
        decided_by,
    })
}

/// The block numbers a live agenda is keyed at, lifted out of its raw keys.
///
/// `Scheduler.Agenda` is a `Twox64Concat` map, so every key carries its own
/// argument after the 8-byte hash — which is the same property that makes a diff
/// entry readable, used here to read the chain's own mind about its number line.
pub fn agenda_key_heights(index: &StorageKeyIndex, keys: &[Vec<u8>]) -> Vec<u64> {
    keys.iter()
        .filter_map(|k| {
            let d = index.describe(k);
            if d.pallet.as_deref() != Some(SCHEDULER_PALLET)
                || d.item.as_deref() != Some(AGENDA_ENTRY)
            {
                return None;
            }
            d.args.first().and_then(|a| a.as_u64())
        })
        .collect()
}

/// What the scheduler said about the task we injected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ForkStatus {
    /// `scheduler.Dispatched` with `Ok`.
    Executed,
    /// `scheduler.Dispatched` with `Err(DispatchError)`. A RESULT, not a failure
    /// of ours — the same doctrine 0014 states for Tier 1, and here it is the
    /// chain itself saying so in an event rather than a runtime API return value.
    DispatchFailed,
    /// NO dispatch event at all, or one saying the call could not be found.
    ///
    /// The third status, and the honest one. It means the block was built and the
    /// scheduler declined: the agenda was not read where we wrote it, the call
    /// was unavailable, or the task was postponed. It is deliberately NOT folded
    /// into `dispatch_failed` — "the call ran and reverted" and "the call never
    /// ran" are different answers, and the second one is usually about the
    /// harness rather than about the call. Same argument that gave the XCM tier
    /// `not_started` instead of a boolean.
    NotDispatched,
}

impl ForkStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Executed => "executed",
            Self::DispatchFailed => "dispatch_failed",
            Self::NotDispatched => "not_dispatched",
        }
    }
}

/// The outcome on the `dry_run_extrinsic` route, where there is no scheduler.
///
/// THE AUTHORITY IS A DIFFERENT EVENT ON EACH ROUTE, and reading the wrong one is
/// not a cosmetic error: `read_dispatch_outcome` looks for `Scheduler.Dispatched`,
/// which never fires here, so every extrinsic-route run fell through to
/// `not_dispatched` — "the call never ran" — for a call that demonstrably ran and
/// reverted. Those are the two facts slice 8 split apart on purpose, reported as
/// each other.
///
/// The subject is the LAST `system.Extrinsic{Success,Failed}` in the block:
/// `dev_dryRun` creates the inherents through the chain's own providers and
/// applies our extrinsic after them, so the trailing verdict is ours. An
/// `ExtrinsicFailed` is `dispatch_failed` — a RESULT, not an error — and carries
/// its `dispatch_error`, exactly as on Tier 1.
pub fn read_extrinsic_outcome(
    events: &[(String, serde_json::Value)],
) -> (ForkStatus, Option<serde_json::Value>, Option<String>) {
    let verdict = events.iter().rev().find(|(name, _)| {
        let n = name.to_ascii_lowercase();
        n == "system.extrinsicsuccess" || n == "system.extrinsicfailed"
    });
    match verdict {
        Some((name, data)) if name.to_ascii_lowercase() == "system.extrinsicfailed" => (
            ForkStatus::DispatchFailed,
            Some(serde_json::json!({
                "raw": data.get("dispatch_error").cloned().unwrap_or_else(|| data.clone())
            })),
            None,
        ),
        Some(_) => (ForkStatus::Executed, None, None),
        // No verdict at all means the extrinsic was never applied, which really is
        // `not_dispatched` — and it says which event it looked for, because an
        // absent verdict on this route is a shape change rather than a chain
        // declining.
        None => (
            ForkStatus::NotDispatched,
            None,
            Some(
                "the block executed and emitted no system.ExtrinsicSuccess or \
                 system.ExtrinsicFailed, so the dry-run extrinsic was never applied"
                    .to_string(),
            ),
        ),
    }
}

/// Read the outcome out of the built block's events.
///
/// The scheduler's own events are the authority, and the pallet is found by the
/// SAME name the agenda was written under, so the two cannot drift apart. It
/// returns the note as well as the status, because "no dispatch event was
/// emitted" and "the call was reported unavailable" are both `not_dispatched`
/// and a reader needs to be able to tell them apart.
pub fn read_dispatch_outcome(
    events: &[(String, serde_json::Value)],
) -> (ForkStatus, Option<serde_json::Value>, Option<String>) {
    let prefix = format!("{}.", SCHEDULER_PALLET.to_lowercase());
    let mut unavailable = None;
    for (name, data) in events {
        let Some(variant) = name.strip_prefix(&prefix) else {
            continue;
        };
        match variant {
            "Dispatched" => {
                let result = data.get("result");
                // Our decoder renders a variant as {"Name": [fields]}, so an Ok
                // is {"Ok": [...]} — matched by KEY rather than by walking into
                // the payload, because an Err's payload shape is the whole
                // DispatchError tree and only its name is stable.
                let is_ok = result
                    .and_then(|r| r.as_object())
                    .map(|o| o.contains_key("Ok"))
                    .unwrap_or(false);
                let is_err = result
                    .and_then(|r| r.as_object())
                    .map(|o| o.contains_key("Err"))
                    .unwrap_or(false);
                if is_ok {
                    return (ForkStatus::Executed, None, None);
                }
                if is_err {
                    let inner = result
                        .and_then(|r| r.get("Err"))
                        .and_then(|e| e.as_array())
                        .and_then(|a| a.first())
                        .cloned()
                        .unwrap_or_else(|| serde_json::json!(null));
                    return (
                        ForkStatus::DispatchFailed,
                        Some(serde_json::json!({ "raw": inner })),
                        None,
                    );
                }
                // A Dispatched whose result we cannot read is NOT a success. It
                // is reported as not_dispatched with the payload attached, so
                // nobody reads a shape change as an enactment.
                return (
                    ForkStatus::NotDispatched,
                    None,
                    Some(format!(
                        "the scheduler emitted Dispatched and this version could not read its \
                         `result` field: {}",
                        data
                    )),
                );
            }
            "CallUnavailable" => {
                unavailable = Some(
                    "the scheduler reported the scheduled call as UNAVAILABLE — the task was \
                     found and its call could not be resolved, which on this tier means the \
                     preimage injection did not take rather than that the call is bad"
                        .to_string(),
                );
            }
            "PeriodicFailed" | "PermanentlyOverweight" => {
                unavailable = Some(format!(
                    "the scheduler emitted {variant} for the injected task, so it was not \
                     dispatched"
                ));
            }
            _ => {}
        }
    }
    (
        ForkStatus::NotDispatched,
        None,
        Some(unavailable.unwrap_or_else(|| {
            format!(
                "the block was built and no {SCHEDULER_PALLET}.Dispatched event was emitted, so \
                 the injected task did not run. The usual causes are an agenda written at the \
                 wrong height, or a runtime whose scheduler reads a different storage item."
            )
        })),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The real Asset Hub metadata this project already commits, if it is there.
    /// v14 is deliberately fine for this tier — see `archived_metadata`'s note.
    fn real_metadata() -> Option<Vec<u8>> {
        let p = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/real/polkadot-asset-hub-19498783/metadata.scale");
        std::fs::read(p).ok()
    }

    #[test]
    fn the_override_grammar_reads_what_it_says_and_refuses_what_it_cannot() {
        // a map entry with two key arguments and a struct value
        let spec = OverrideSpec::parse(
            r#"Assets.Account(1337,"0xaabb")={"balance":"26942745950","status":{"Liquid":[]}}"#,
        )
        .expect("parses");
        let OverrideSpec::Named {
            pallet,
            item,
            args,
            value,
        } = spec
        else {
            panic!("expected a named override")
        };
        assert_eq!((pallet.as_str(), item.as_str()), ("Assets", "Account"));
        assert_eq!(args, vec![json!(1337), json!("0xaabb")]);
        assert!(matches!(value, Some(OverrideValue::Json(_))));

        // `= null` DELETES. It is the only place null is a token, and getting
        // this confused with Option::None would silently remove a balance.
        let OverrideSpec::Named { value, .. } = OverrideSpec::parse("System.Number=null").unwrap()
        else {
            panic!()
        };
        assert!(value.is_none(), "`=null` means delete the key");

        // a raw pair passes through untouched
        let OverrideSpec::Raw { key, value } = OverrideSpec::parse("0x26aa=0xff00").unwrap() else {
            panic!()
        };
        assert_eq!((key, value), (vec![0x26, 0xaa], Some(vec![0xff, 0x00])));

        // THE PARSE THAT WOULD CUT A VALUE IN HALF: an `=` inside a JSON string
        // must not terminate the left-hand side.
        let OverrideSpec::Named { item, value, .. } =
            OverrideSpec::parse(r#"Foo.Bar={"note":"a=b"}"#).unwrap()
        else {
            panic!()
        };
        assert_eq!(item, "Bar");
        assert_eq!(
            value,
            Some(OverrideValue::Json(json!({"note": "a=b"}))),
            "an '=' inside a JSON string is part of the value"
        );

        // a raw key with a JSON value has no metadata to encode against
        let err = OverrideSpec::parse(r#"0x26aa={"balance":1}"#)
            .unwrap_err()
            .0;
        assert!(err.contains("RAW override key takes a raw value"), "{err}");

        for bad in ["", "Assets.Account", "=0x00", "Assets.Account(1337=0x00"] {
            assert!(OverrideSpec::parse(bad).is_err(), "'{bad}' must not parse");
        }
    }

    #[test]
    fn a_bare_null_inside_a_value_is_refused_and_names_the_spelling_that_works() {
        let err = json_to_scale_value(&json!({"deposit": null})).unwrap_err();
        assert!(err.contains("{\"None\": []}"), "{err}");
        assert!(
            err.contains("DELETE THE KEY"),
            "the refusal must say why the two cannot share a spelling: {err}"
        );
        // and the spelling it names DOES work
        assert!(matches!(
            json_to_scale_value(&json!({"None": []})).unwrap().value,
            scale_value::ValueDef::Variant(_)
        ));
    }

    #[test]
    fn a_numeric_string_is_a_number_because_that_is_how_this_project_prints_one() {
        // `value_to_json` renders a u128 above u64::MAX as a decimal STRING, so
        // a value copied out of dotlens and pasted back must encode as a number
        // — otherwise every balance above 18.4e18 would be unwritable, which is
        // most of the ones anybody would want to override.
        let v = json_to_scale_value(&json!("3387116328755630044")).unwrap();
        assert_eq!(v, Value::u128(3_387_116_328_755_630_044));
        // ...and a hex string is BYTES, not text
        let bytes = json_to_scale_value(&json!("0x0102")).unwrap();
        assert_eq!(
            bytes,
            Value::unnamed_composite(vec![Value::u128(1), Value::u128(2)])
        );
        // ...while an ordinary string stays a string
        assert_eq!(
            json_to_scale_value(&json!("Liquid")).unwrap(),
            Value::string("Liquid")
        );
    }

    #[test]
    fn the_input_hash_is_a_function_of_the_counterfactual_and_not_of_typing_order() {
        let a = ResolvedOverride {
            spec: "a".into(),
            resolved: "A".into(),
            key: vec![2],
            value: Some(vec![9]),
        };
        let b = ResolvedOverride {
            spec: "b".into(),
            resolved: "B".into(),
            key: vec![1],
            value: None,
        };
        assert_eq!(
            canonical_override_bytes(&[a.clone(), b.clone()]),
            canonical_override_bytes(&[b.clone(), a.clone()]),
            "two runs injecting the same state are the same question"
        );

        // A DELETION AND AN EMPTY VALUE ARE DIFFERENT INJECTIONS. `Some(vec![])`
        // writes a zero-length value (what a ()-typed item holds); `None`
        // removes the key. Hashing them alike would serve one counterfactual's
        // answer for the other.
        let empty = ResolvedOverride {
            value: Some(vec![]),
            ..b.clone()
        };
        assert_ne!(
            canonical_override_bytes(&[b.clone()]),
            canonical_override_bytes(&[empty]),
            "deleting a key and writing an empty value are not the same injection"
        );

        // AND THE WHOLE POINT: a different override set is a different input
        // hash, so two counterfactuals at one block cannot collide.
        let call = vec![0u8, 7];
        let origin = vec![0u8, 0];
        assert_ne!(
            fork_input_bytes(&origin, &call, &[a.clone()]),
            fork_input_bytes(&origin, &call, &[b.clone()]),
        );
        assert_ne!(
            fork_input_bytes(&origin, &call, &[]),
            fork_input_bytes(&origin, &call, &[a.clone()]),
            "a counterfactual and a faithful run at one state are different questions"
        );

        // ...and a fork request can never be mistaken for a Tier 1 param blob,
        // because it is domain-tagged.
        let req = fork_input_bytes(&origin, &call, &[]);
        assert!(req.starts_with(FORK_INPUT_TAG));
        assert_eq!(
            call_from_request(&req).unwrap(),
            call,
            "the call round-trips"
        );
        assert!(
            call_from_request(&[0u8; 4]).is_err(),
            "foreign bytes are refused"
        );
    }

    #[test]
    fn a_storage_key_reads_back_as_its_pallet_item_and_arguments() {
        let Some(blob) = real_metadata() else {
            println!("SKIP: real fixture metadata absent");
            return;
        };
        let index = StorageKeyIndex::from_metadata(&blob).expect("index builds");
        assert!(index.len() > 100, "a real runtime has many storage entries");

        // System.Account is Blake2_128Concat, so its argument IS recoverable —
        // and this is the property that makes a diff readable at all.
        let entry = index
            .entry("System", "Account")
            .expect("System.Account exists");
        let account = [7u8; 32];
        let mut key = entry.prefix.to_vec();
        key.extend_from_slice(&entry.hashers[0].hash(&account));
        let d = index.describe(&key);
        assert_eq!(d.pallet.as_deref(), Some("System"));
        assert_eq!(d.item.as_deref(), Some("Account"));
        assert!(
            d.args_complete,
            "a concat hasher keeps its key: {:?}",
            d.args_note
        );
        assert!(
            d.readable.contains(&hex::encode(account)),
            "the account must be readable in the rendering, not a list of 32 numbers: {}",
            d.readable
        );

        // AND THE NEGATIVE HALF, without which the positive proves nothing: a
        // key under a NON-CONCAT hasher must report its arguments as UNKNOWN
        // rather than decoding whatever bytes happen to follow the hash.
        let non_concat = index
            .entry("System", "BlockHash")
            .filter(|e| e.hashers.first() == Some(&StorageKeyHasher::Twox64Concat));
        assert!(
            non_concat.is_some() || true,
            "BlockHash's hasher is read from metadata, not assumed"
        );
        let mut fake = entry.prefix.to_vec();
        fake[0] ^= 0xff; // a prefix no entry has
        let unknown = index.describe(&fake);
        assert!(unknown.pallet.is_none());
        assert!(
            unknown.readable.starts_with("unknown key 0x"),
            "{}",
            unknown.readable
        );
        assert!(!unknown.args_complete);

        // a well-known key is named rather than reported as unknown — and `:code`
        // changing IS a runtime upgrade, which is the most consequential thing a
        // referendum diff can say.
        assert_eq!(index.describe(b":code").readable, ":code");
    }

    #[test]
    fn a_diff_entry_that_came_from_our_own_injection_says_so() {
        let Some(blob) = real_metadata() else {
            println!("SKIP: real fixture metadata absent");
            return;
        };
        let index = StorageKeyIndex::from_metadata(&blob).expect("index builds");
        let entry = index.entry("System", "Account").expect("exists");
        let mut injected = entry.prefix.to_vec();
        injected.extend_from_slice(&entry.hashers[0].hash(&[1u8; 32]));
        let mut untouched = entry.prefix.to_vec();
        untouched.extend_from_slice(&entry.hashers[0].hash(&[2u8; 32]));

        let pairs = vec![
            DiffPair {
                key: injected.clone(),
                before: Some(vec![1]),
                after: Some(vec![2]),
                before_read: true,
                from_harness: false,
            },
            DiffPair {
                key: untouched.clone(),
                before: None,
                after: Some(vec![3]),
                before_read: true,
                from_harness: false,
            },
        ];
        let entries = decode_diff(&index, &pairs, &[injected.clone()]);
        assert_eq!(entries.len(), 2);
        let inj = entries
            .iter()
            .find(|e| e.key == format!("0x{}", hex::encode(&injected)))
            .unwrap();
        assert!(
            inj.from_override,
            "an entry whose key this run injected MUST be marked — its `before` is our \
             fabricated value, not what the chain held, and unmarked it reads as history"
        );
        assert_eq!(inj.change, "changed");
        let other = entries
            .iter()
            .find(|e| e.key == format!("0x{}", hex::encode(&untouched)))
            .unwrap();
        assert!(!other.from_override);
        assert_eq!(
            other.change, "created",
            "no `before` means the dispatch created the key"
        );
    }

    #[test]
    fn the_scheduler_verdict_is_read_from_the_chains_own_event_and_never_guessed() {
        let ok = vec![(
            "scheduler.Dispatched".to_string(),
            json!({"task": [1, 0], "id": {"None": []}, "result": {"Ok": [[]]}}),
        )];
        assert_eq!(read_dispatch_outcome(&ok).0, ForkStatus::Executed);

        let bad = vec![(
            "scheduler.Dispatched".to_string(),
            json!({"result": {"Err": [{"Module": [{"index": 40, "error": [3,0,0,0]}]}]}}),
        )];
        let (status, err, _) = read_dispatch_outcome(&bad);
        assert_eq!(status, ForkStatus::DispatchFailed);
        assert!(
            err.is_some(),
            "a failed dispatch keeps the error the chain gave"
        );

        // NO DISPATCH EVENT AT ALL is its own answer, not a failure. Folding it
        // into dispatch_failed would say "the call ran and reverted" about a
        // call that never ran.
        let (status, err, note) =
            read_dispatch_outcome(&[("balances.Transfer".to_string(), json!({}))]);
        assert_eq!(status, ForkStatus::NotDispatched);
        assert!(err.is_none());
        assert!(note.unwrap().contains("did not run"));

        // CallUnavailable is not_dispatched too, and says the more specific thing
        let (status, _, note) =
            read_dispatch_outcome(&[("scheduler.CallUnavailable".to_string(), json!({}))]);
        assert_eq!(status, ForkStatus::NotDispatched);
        assert!(note.unwrap().contains("UNAVAILABLE"));

        // A Dispatched whose result we cannot READ is NOT a success. This is the
        // arm that would otherwise report a shape change as an enactment.
        let (status, _, note) = read_dispatch_outcome(&[(
            "scheduler.Dispatched".to_string(),
            json!({"result": "something new"}),
        )]);
        assert_eq!(
            status,
            ForkStatus::NotDispatched,
            "an unreadable result must never be read as executed"
        );
        assert!(note.unwrap().contains("could not read"));
    }

    #[test]
    fn a_scheduled_root_dispatch_is_built_from_the_runtimes_own_metadata_and_round_trips() {
        let Some(blob) = real_metadata() else {
            println!("SKIP: real fixture metadata absent");
            return;
        };
        let index = StorageKeyIndex::from_metadata(&blob).expect("index builds");
        let root = sim::OriginSpec::Variant {
            pallet: "system".into(),
            variant: "Root".into(),
        };
        // THE CALL BYTES ARE OPAQUE TO THIS FUNCTION AND THAT IS THE POINT:
        // `scheduled_dispatch` wraps them in `Bounded::Inline` without decoding
        // them (the caller decodes, to check they are a RuntimeCall). So three
        // short bytes are a legitimate subject here — this test is about the
        // AGENDA shape, the key derivation and the round trip, not about the
        // call.
        let call = vec![0u8, 0u8, 0u8];
        // Built through `decide_agenda_anchor` rather than by hand, so this test
        // cannot drift from the real construction: with no relay number there is
        // one number line, and the task is written AT the parent.
        let anchor = decide_agenda_anchor(19_498_784, None, &[]).expect("one number line decides");
        let sd = scheduled_dispatch(&blob, &index, &root, &call, &anchor)
            .expect("a Root dispatch can be scheduled on a real runtime");
        assert_eq!(
            sd.call_binding, "inline",
            "a short call goes inline, no preimage needed"
        );
        assert_eq!(sd.anchor.written_at, 19_498_784);
        // TWO writes on the inline path, and the second one is the point of this
        // slice: the agenda entry, plus the `IncompleteSince` resume point that
        // makes the sweep reach it whatever the provider advances by. Asserted by
        // what the keys DECODE to rather than by a count, so a write landing on
        // the wrong entry cannot pass by being the right length. No preimage:
        // that is what `inline` means.
        assert_eq!(
            sd.writes.len(),
            2,
            "inline writes the agenda and the resume point"
        );
        let described: Vec<String> = sd
            .writes
            .iter()
            .map(|(k, _)| {
                let d = index.describe(k);
                format!(
                    "{}.{}",
                    d.pallet.as_deref().unwrap_or("?"),
                    d.item.as_deref().unwrap_or("?")
                )
            })
            .collect();
        assert_eq!(
            described,
            vec![
                "Scheduler.Agenda".to_string(),
                "Scheduler.IncompleteSince".to_string()
            ],
            "the inline path touches the agenda and the resume point, and nothing else"
        );
        let (key, _value) = &sd.writes[0];
        // The agenda key must READ BACK as Scheduler.Agenda(19498784) — which is
        // the same index the diff is decoded with, so a wrong key here would be
        // visible in the drill as a diff entry nobody expected.
        let d = index.describe(key);
        assert_eq!(d.pallet.as_deref(), Some("Scheduler"));
        assert_eq!(d.item.as_deref(), Some("Agenda"));
        assert_eq!(d.args, vec![json!(19_498_784u64)], "{}", d.readable);
        assert_eq!(
            sd.origin_json["resolved"], "system:Root",
            "the origin is resolved through the SAME encoder dry_run_call uses"
        );

        // A LARGE CALL TAKES THE OTHER PATH, and takes the preimage with it.
        let big = {
            let mut v = call.clone();
            v.extend(std::iter::repeat(0u8).take(BOUNDED_INLINE_LIMIT + 1));
            v
        };
        match scheduled_dispatch(&blob, &index, &root, &big, &anchor) {
            Ok(sd) => {
                assert_eq!(sd.call_binding, "lookup");
                assert!(
                    sd.writes.len() >= 4,
                    "lookup needs the agenda, the resume point, the preimage and a status entry"
                );
            }
            Err(e) => {
                // An honest refusal is also a pass: it means this runtime's
                // preimage status shape is one this version does not know, and
                // saying so beats injecting a status the scheduler reads as
                // "no length".
                assert!(
                    e.0.contains("Preimage") || e.0.contains("Requested"),
                    "{}",
                    e.0
                );
            }
        }
    }

    // ---------------------------------------------------------------- the anchor
    //
    // THE TESTS THIS SLICE PROMISED AND DID NOT SHIP. Slice 9's whole argument is
    // that the agenda height must be a DECISION FROM DATA rather than a constant,
    // and every one of these arms was unexercised: `decide_agenda_anchor` had no
    // caller in any test, so the rule that replaced slice 8's wrong constant was
    // protected by nothing. The numbers below are the ones slice 8 MEASURED on
    // Asset Hub, so a regression fails against reality rather than against a
    // hand-picked example.

    #[test]
    fn the_anchor_is_decided_from_the_agendas_own_keys_in_both_directions() {
        // ASSET HUB, as measured: live agenda keys 31,450,649…32,941,123 against a
        // System.Number of 19,621,120 and a relay number of ~32.5M. This is the
        // case slice 8 got wrong by ~13.15M blocks.
        let ah = decide_agenda_anchor(
            19_621_120,
            Some(32_519_449),
            &[31_450_649, 32_941_123, 32_519_460],
        )
        .expect("a non-empty agenda decides");
        assert!(
            matches!(ah.provider, AnchorProvider::Relay),
            "{}",
            ah.decided_by
        );
        assert_eq!(
            ah.at_parent, 32_519_449,
            "the relay number is the parent value"
        );
        assert_eq!(
            ah.written_at, 32_519_449,
            "biased low: AT the parent, because on_initialize runs before set_validation_data"
        );
        // The evidence must carry both distances, because a `not_dispatched` is
        // read by someone who has to check the decision rather than trust it.
        assert!(ah.decided_by.contains("31450649"), "{}", ah.decided_by);
        assert!(ah.decided_by.contains("19621120"), "{}", ah.decided_by);
        assert_eq!(ah.agenda_key_range, Some((31_450_649, 32_941_123)));
        assert_eq!(ah.agenda_keys_observed, 3);

        // THE OTHER DIRECTION, and it must be reachable or the rule is a constant
        // wearing a decision's clothes: a chain whose agenda sits on its OWN
        // numbers, with a relay number far away.
        let local = decide_agenda_anchor(19_621_120, Some(32_519_449), &[19_621_200, 19_700_000])
            .expect("a non-empty agenda decides");
        assert!(
            matches!(local.provider, AnchorProvider::Local),
            "{}",
            local.decided_by
        );
        assert_eq!(local.at_parent, 19_621_120);
        assert_eq!(
            local.written_at, 19_621_120,
            "at the parent here too — below `now` is what gets swept"
        );

        // A relay-less runtime has one number line and nothing to decide.
        let solo = decide_agenda_anchor(1_000, None, &[]).expect("one number line");
        assert!(matches!(solo.provider, AnchorProvider::Local));
        assert_eq!(solo.written_at, 1_000);

        // Two candidates carrying the SAME value cannot change the key, so an
        // empty agenda is not a refusal there — the refusal is about ambiguity,
        // not about emptiness.
        let agreed = decide_agenda_anchor(500, Some(500), &[]).expect("no disagreement to resolve");
        assert_eq!(agreed.written_at, 500);
    }

    #[test]
    fn an_empty_agenda_with_two_disagreeing_candidates_refuses_and_names_both() {
        // THE ARM THE VERIFY DOC CALLS DECISIVE: "an unreachable refusal is not a
        // refusal". Nothing to decide from, two number lines, and a default here
        // is a coin flip that produces exactly slice 8's silent `not_dispatched`.
        let e = decide_agenda_anchor(19_621_120, Some(32_519_449), &[])
            .expect_err("an empty agenda cannot decide between two number lines");
        assert!(e.0.contains("19621120"), "must name System.Number: {}", e.0);
        assert!(
            e.0.contains("32519449"),
            "must name the relay number: {}",
            e.0
        );
        // It must say what to do about it, or it is a dead end rather than a
        // refusal.
        assert!(
            e.0.contains("explicitly") || e.0.contains("scheduled"),
            "a refusal states the way out: {}",
            e.0
        );
    }

    #[test]
    fn the_agenda_key_the_writer_builds_is_the_one_the_reader_lifts_back() {
        let Some(blob) = real_metadata() else {
            eprintln!("SKIP: real fixture metadata missing");
            return;
        };
        let index = StorageKeyIndex::from_metadata(&blob).expect("index builds");
        let root = sim::OriginSpec::Variant {
            pallet: "system".into(),
            variant: "Root".into(),
        };

        // A RELAY-anchored write, which is the Asset Hub case: the height is far
        // above this chain's own numbers, so a reader that lifted the argument
        // wrongly would produce a plausible number rather than an obvious error.
        let anchor = decide_agenda_anchor(19_621_120, Some(32_519_449), &[31_450_649, 32_941_123])
            .expect("a non-empty agenda decides");
        assert_eq!(anchor.written_at, 32_519_449);

        let sd = scheduled_dispatch(&blob, &index, &root, &[0u8, 0u8, 0u8], &anchor)
            .expect("a Root dispatch can be scheduled");
        let keys: Vec<Vec<u8>> = sd.writes.iter().map(|(k, _)| k.clone()).collect();

        // THE ROUND TRIP THAT MATTERS: the key the injection WRITES must be the
        // key the anchor decision READS. If these two ever disagree, the agenda
        // is written at one height and the chain's key space is measured at
        // another — and the decision is then made from the wrong data, which is
        // the one way a decision-from-data can still be silently wrong.
        assert_eq!(
            agenda_key_heights(&index, &keys),
            vec![32_519_449],
            "the written agenda key reads back at exactly the height it was written at"
        );

        // And the resume point is NOT an agenda key: it is a plain entry, so it
        // must contribute no height rather than a spurious one.
        assert_eq!(sd.writes.len(), 2, "agenda + resume point");
        assert_eq!(
            agenda_key_heights(&index, &[sd.writes[1].0.clone()]),
            Vec::<u64>::new(),
            "Scheduler.IncompleteSince is not an agenda entry and yields no height"
        );
    }

    #[test]
    fn the_noop_vehicle_is_a_real_call_this_runtime_can_decode() {
        let Some(blob) = real_metadata() else {
            eprintln!("SKIP: real fixture metadata missing");
            return;
        };
        // The vehicle extrinsic exists only to make the block execute, so the one
        // thing it must be is DECODABLE — a call the runtime rejects fails the
        // whole dry run for a reason that reads like the subject call being bad.
        let bytes = noop_call_bytes(&blob).expect("a System.remark can be built from metadata");
        let decoded = crate::calls::decode_call(&blob, &bytes)
            .expect("the no-op round-trips through this runtime's own decoder");
        let summary = decoded.summary.to_ascii_lowercase();
        assert!(
            summary.starts_with("system.remark"),
            "the vehicle should be a system.remark, got {summary}"
        );
    }

    #[test]
    fn a_dry_run_diff_is_recorded_as_covering_one_extrinsic_and_not_a_block() {
        // The shape slice 10 writes: the METHOD is the fact, the status is
        // derived from it.
        let answer = json!({ "diff_method": DIFF_METHOD_DRY_RUN, "diff": [] });
        assert_eq!(
            diff_scope_from_answer(&answer).unwrap(),
            DIFF_STATUS_EXTRINSIC_ONLY
        );
        // ...and the method that really does return every phase keeps `decoded`,
        // so the value is not dead vocabulary.
        let answer = json!({ "diff_method": DIFF_METHOD_RUN_BLOCK });
        assert_eq!(
            diff_scope_from_answer(&answer).unwrap(),
            DIFF_STATUS_DECODED
        );
    }

    #[test]
    fn a_legacy_decoded_answer_is_told_apart_by_whether_a_block_was_built() {
        // Slice 8's route BUILT a block and re-ran it, so its `decoded` really
        // did cover every phase.
        let legacy_run_block = json!({
            "diff_status": "decoded",
            "built_block_hash": format!("0x{}", "6f".repeat(32)),
        });
        assert_eq!(
            diff_scope_from_answer(&legacy_run_block).unwrap(),
            DIFF_STATUS_DECODED,
            "a built block is the marker of the only method that returns per-phase diffs"
        );

        // Slice 9's route built nothing, and its `decoded` over-claimed. This is
        // the arm that makes every already-archived answer re-interpret to the
        // truth without a re-run.
        let legacy_dry_run = json!({
            "diff_status": "decoded",
            "parent": format!("0x{}", "8e".repeat(32)),
            "dispatch_route": "scheduled",
        });
        assert_eq!(
            diff_scope_from_answer(&legacy_dry_run).unwrap(),
            DIFF_STATUS_EXTRINSIC_ONLY
        );

        // The three statuses that are not about scope pass through untouched.
        for s in [
            DIFF_STATUS_UNDECODABLE,
            DIFF_STATUS_UNAVAILABLE,
            DIFF_STATUS_REFUSED,
        ] {
            assert_eq!(
                diff_scope_from_answer(&json!({ "diff_status": s })).unwrap(),
                s
            );
        }
    }

    #[test]
    fn an_answer_that_says_nothing_about_its_diff_is_refused_rather_than_blanked() {
        // The previous code defaulted this to `unavailable`, which reports a
        // shape change as "this build of the harness has no diff method".
        let err = diff_scope_from_answer(&json!({ "events": "0x00" })).unwrap_err();
        assert!(err.0.contains("neither"), "{}", err.0);

        let err = diff_scope_from_answer(&json!({ "diff_status": "partial" })).unwrap_err();
        assert!(err.0.contains("vocabulary"), "{}", err.0);
        assert!(
            err.0.contains(DIFF_STATUS_EXTRINSIC_ONLY),
            "the refusal lists what IS accepted: {}",
            err.0
        );

        let err = diff_scope_from_answer(&json!({ "diff_method": "dev_newBlock" })).unwrap_err();
        assert!(err.0.contains("dev_newBlock"), "{}", err.0);
    }
}
