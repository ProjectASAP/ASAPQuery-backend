//! Per-metric storage-backend routing.
//!
//! This module is the dispatch boundary between the HTTP query
//! handler and the tier-co-located engines (warm sketch tier in
//! [`crate::engines::simple`], archive tier in
//! [`crate::engines::gorilla`]). Two cooperating pieces:
//!
//! * [`backend_storage_routing`] — config loader + multi-target
//!   per-metric lookup (`metric → [(backend, query-shape filter), ...]`).
//!   Loaded once at backend startup from
//!   `deploy/configs/backend-storage-routing.yaml`; queried on
//!   every HTTP request.
//! * [`engine_router`] — the engine dispatcher. Holds a small map
//!   of `data_source_id → Arc<dyn QueryEngine>` and walks the
//!   compatibility list returned by
//!   [`asap_types::compatible_storage_backends`] to pick which
//!   engine answers a given `(query, metric_storage)` pair.
//!
//! Step-1 of the JSONL deprecation refactor lifted these out of
//! `data_model/backend_storage_routing.rs` and `engines/router.rs`
//! into this dedicated `routing/` directory so the HTTP handler's
//! dispatch surface is a single import (`use crate::routing::*`)
//! instead of straddling two unrelated module trees.

pub mod backend_storage_routing;
pub mod engine_router;

pub use backend_storage_routing::{
    classify_query_shape, BackendStorageRouting, QueryShape, RoutingTarget,
};
pub use engine_router::{
    EngineCapabilities, EngineRouter, EngineRouterError, QueryEngine,
};
