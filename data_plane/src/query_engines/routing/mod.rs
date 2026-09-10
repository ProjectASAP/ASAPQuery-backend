//! Per-metric storage-backend routing.
//!
//! This module is the dispatch boundary between the HTTP query
//! handler and the tier-co-located engines (warm sketch tier in
//! [`crate::query_engines::asap_query_engine`], archive tier in
//! [`crate::query_engines::thanos_query_engine`]). Two cooperating pieces:
//!
//! * [`backend_storage_routing`] — config loader + multi-target
//!   per-metric lookup (`metric → [(backend, query-shape filter), ...]`).
//!   Loaded once at backend startup from
//!   `deploy/configs/backend-storage-routing.yaml`; queried on
//!   every HTTP request.
//! * [`query_engine_routing`] — the engine dispatcher. Holds a small map
//!   of `data_source_id → Arc<dyn QueryEngine>` and walks the
//!   compatibility list returned by
//!   [`capability_matching::compatible_storage_backends`] to pick which
//!   engine answers a given `(query, metric_storage)` pair.
//! * [`capability_matching`] — the storage-backend routing policy itself
//!   (`AccuracyTarget`, `compatible_storage_backends`). Split out of
//!   `asap_types`'s former `capability_matching` module. `StorageBackend`
//!   itself later moved into this crate too, alongside `StreamingConfig`
//!   (see `crate::storage_engines::types::storage_backend`'s module doc) —
//!   `control_plane` turned out to have zero real dependency on either.
//!
//! Step-1 of the JSONL deprecation refactor lifted these out of
//! `data_model/backend_storage_routing.rs` and `query-engines/router.rs`
//! into this dedicated `routing/` directory so the HTTP handler's
//! dispatch surface is a single import (`use crate::query_engines::routing::*`)
//! instead of straddling two unrelated module trees.

pub mod backend_storage_routing;
pub mod capability_matching;
pub mod freshness_probe_cache;
pub mod query_engine_routing;

pub use capability_matching::{compatible_storage_backends, AccuracyTarget};

pub use backend_storage_routing::{
    classify_query_shape, routing_table_hash, BackendStorageRouting,
    HotReloadBackendStorageRouting, QueryOperatorShape, RoutingTarget, DEFAULT_TENANT,
};
pub use freshness_probe_cache::{
    is_freshness_probe, now_ms as freshness_probe_now_ms, FreshnessProbeCache, ProbeSample,
};
pub use query_engine_routing::{
    EngineCapabilities, EngineRouter, EngineRouterError, QueryEngine, RangeTier,
};
