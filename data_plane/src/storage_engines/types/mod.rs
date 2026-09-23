//! Data types used by the storage layer (and consumed cross-module
//! by `drivers/`, `precompute_engine/`, `query_engines/`).
//!
//! Renamed from `stores/schema/` in the 2026-05 reorg to avoid the
//! name-clash with `stores/sketch_db/schema/` (per-`agg_id` schema
//! lifecycle, a different concern). Tiny `pub use asap_types::X`
//! shim files were dropped; import directly from `asap_types`.

pub mod enums;
pub mod hot_reload_config;
pub mod precomputed_output;
pub mod storage_backend;
pub mod streaming_config;

pub use enums::*;
pub use hot_reload_config::*;
pub use asap_physical_operators::key_by_label_values::*;
pub use asap_physical_operators::measurement::*;
pub use precomputed_output::*;
pub use storage_backend::*;
pub use streaming_config::*;
pub use asap_physical_operators::traits::*;

// Cross-module re-export of asap_types data types so callers can
// write `crate::storage_engines::types::PrecomputeMaterialization` instead of
// reaching across crates.
pub use asap_types::aggregation_config::*;

// Re-export the query-side routing surface so existing call sites
// like `crate::storage_engines::types::BackendStorageRouting` keep compiling.
pub use crate::query_engines::routing::{
    classify_query_shape, BackendStorageRouting, HotReloadBackendStorageRouting,
    QueryOperatorShape, RoutingTarget,
};
