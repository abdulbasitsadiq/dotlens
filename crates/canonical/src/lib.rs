//! Canonical data model: the generic, family-agnostic representation every
//! adapter decodes into. NO I/O in this crate — types only.
//!
//! Invariant 3: every decoded item carries lineage (spec_version, decoder_version,
//! raw_location). Invariant 5: these types stay generic enough for JAM — nothing
//! here may assume FRAME, pallets, or metadata exist.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Where the *raw* bytes behind a decoded item live in the raw store.
pub type RawLocation = String;

/// Decoding provenance. If you can't fill this in, you may not write the row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lineage {
    /// Runtime context version. For Substrate: spec_version. For JAM (future):
    /// Gray Paper version. Adapters define the meaning; core just stores it.
    pub runtime_version: u32,
    pub decoder_version: u32,
    pub raw_location: RawLocation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockRef {
    pub chain_id: String,
    pub height: u64,
    pub hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalBlock {
    pub chain_id: String,
    pub height: u64,
    pub hash: String,
    pub parent_hash: String,
    /// Block time as claimed by the chain (Substrate: timestamp inherent).
    pub timestamp: Option<DateTime<Utc>>,
    pub finalized: bool,
    pub lineage: Lineage,
    pub transactions: Vec<CanonicalTransaction>,
    pub events: Vec<CanonicalEvent>,
}

/// Generic "submitted thing" (Substrate extrinsic / EVM tx / future JAM work item).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalTransaction {
    pub index: u32,
    pub hash: Option<String>,
    /// Signer, family-encoded (SS58, 0x…, service id). None for inherents.
    pub signer: Option<String>,
    /// Namespaced call identifier, e.g. "balances.transfer_keep_alive".
    pub call: String,
    /// Decoded arguments — schema-on-read.
    pub args: serde_json::Value,
    pub success: bool,
}

/// Generic "observed effect" (Substrate event / EVM log / JAM accumulate effect).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalEvent {
    pub index: u32,
    /// Which transaction produced it, if attributable.
    pub transaction_index: Option<u32>,
    /// Namespaced, e.g. "treasury.AssetSpendApproved".
    pub name: String,
    pub data: serde_json::Value,
}

impl CanonicalBlock {
    pub fn block_ref(&self) -> BlockRef {
        BlockRef {
            chain_id: self.chain_id.clone(),
            height: self.height,
            hash: self.hash.clone(),
        }
    }
}
