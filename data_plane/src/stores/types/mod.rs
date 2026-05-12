//! Data types used by the storage layer (and consumed cross-module
//! by `drivers/`, `precompute_engine/`, `query_engines/`).
//!
//! Renamed from `stores/schema/` in the 2026-05 reorg to avoid the
//! name-clash with `stores/sketch_db/schema/` (per-`agg_id` schema
//! lifecycle, a different concern). Tiny `pub use asap_types::X`
//! shim files were dropped; import directly from `asap_types`.

pub mod enums;
pub mod hot_reload_config;
pub mod key_by_label_values;
pub mod measurement;
pub mod precomputed_output;
pub mod traits;

pub use enums::*;
pub use hot_reload_config::*;
pub use key_by_label_values::*;
pub use measurement::*;
pub use precomputed_output::*;
pub use traits::*;

// Cross-module re-exports of asap_types data types so callers can
// write `crate::stores::types::StreamingConfig` instead of reaching
// across crates. (Previously these had per-type shim files like
// `aggregation_config.rs` containing only `pub use asap_types::...`.)
pub use asap_types::aggregation_config::*;
pub use asap_types::aggregation_reference::*;
pub use asap_types::inference_config::*;
pub use asap_types::promql_schema::*;
pub use asap_types::query_config::*;
pub use asap_types::streaming_config::*;

// Re-export the query-side routing surface so existing call sites
// like `crate::stores::types::BackendStorageRouting` keep compiling.
pub use crate::query_engines::routing::{
    classify_query_shape, BackendStorageRouting, HotReloadBackendStorageRouting, QueryShape,
    RoutingTarget,
};
