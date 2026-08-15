//! dotlens-node library: the wiring between registry, raw store, decode,
//! checkpoints, and the API's block index. Lives in a lib (not main.rs) so
//! integration tests drive the exact production paths.

pub mod pipeline;

#[cfg(feature = "pg")]
pub mod registry_sync;

#[cfg(feature = "pg")]
pub mod runtime_versions;
