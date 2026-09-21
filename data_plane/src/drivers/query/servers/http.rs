use crate::drivers::query::adapters::{ParsedQueryRequest, ParsedRangeQueryRequest};
use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Form, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Json, Response},
    routing::{get, post},
    Router,
};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tracing::{debug, info, warn};

use crate::drivers::query::adapters::{AdapterConfig, HttpProtocolAdapter, PrometheusHttpAdapter};
use crate::drivers::query::servers::metrics as srv_metrics;
use crate::query_engines::routing::{
    AccuracyTarget, EngineRouter, EngineRouterError, FreshnessProbeCache, QueryEngine,
};
use crate::query_engines::ASAPQueryEngine;
use crate::storage_engines::types::StorageBackend;
use asap_types::Statistic;

/// Per-query engine override header (Phase-6 accuracy reducer).
///
/// When the client sets `X-ASAP-Engine: <data_source_id>` (or the
/// equivalent `?engine=<data_source_id>` query param), the HTTP layer
/// bypasses the per-metric `BackendStorageRouting` lookup and dispatches
/// the PromQL string straight to the named engine. Used by the
/// `accuracy_reduce.py` reducer to select a registered query engine.
///
/// Recognised values match `StorageBackend::data_source_id()` —
/// `asap_query`, `double_write`, `prometheus_remote`. An unknown
/// value returns 400.
pub const ENGINE_OVERRIDE_HEADER: &str = "X-ASAP-Engine";
pub const ENGINE_OVERRIDE_QUERY_PARAM: &str = "engine";
/// Per-request accuracy contract. `exact` forwards to the configured exact
/// backend; `approximate` (the default) tries ASAP execution first.
pub const ACCURACY_HEADER: &str = "X-ASAP-Accuracy";

fn extract_accuracy(headers: &HeaderMap) -> Result<AccuracyTarget, String> {
    let Some(value) = headers.get(ACCURACY_HEADER) else {
        return Ok(AccuracyTarget::Epsilon(0.01));
    };
    let value = value
        .to_str()
        .map_err(|_| format!("{ACCURACY_HEADER} must be valid UTF-8"))?
        .trim();
    if value.eq_ignore_ascii_case("exact") {
        Ok(AccuracyTarget::Exact)
    } else if value.eq_ignore_ascii_case("approximate") {
        Ok(AccuracyTarget::Epsilon(0.01))
    } else {
        Err(format!(
            "invalid {ACCURACY_HEADER} value {value:?}; expected `exact` or `approximate`"
        ))
    }
}

fn invalid_accuracy_response(error: String) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({
            "status": "error",
            "errorType": "bad_data",
            "error": error,
        })),
    )
        .into_response()
}

/// Per-request tenant header (per-tenant `BackendStorageRouting`,
/// follow-up to PR #333).
///
/// Multi-tenant deployments scope the per-metric routing table per
/// tenant. The HTTP query handler reads this header on every
/// request, snapshots that tenant's table off
/// `HotReloadBackendStorageRouting`, and falls back to the
/// `default` tenant's table when the header is missing or the
/// requested tenant has no entry. Sketch state is still global —
/// only routing is tenant-scoped.
///
/// **MVP scope:** the header is unauthenticated. Anyone can pick any
/// tenant by setting it. Tenant-aware AUTH is deferred to a follow-up
/// before any multi-tenant deploy is considered production-ready.
pub const TENANT_HEADER: &str = "X-ASAP-Tenant";

/// Extract the tenant id from the per-request `X-ASAP-Tenant`
/// header, or fall back to [`crate::query_engines::routing::DEFAULT_TENANT`] when
/// the header is missing / empty / not valid UTF-8. Used by the
/// instant-query and range-query handlers to scope per-tenant
/// routing-table lookup.
fn extract_tenant(headers: &axum::http::HeaderMap) -> String {
    headers
        .get(TENANT_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| crate::query_engines::routing::DEFAULT_TENANT.to_string())
}

/// Copy only request context that is safe and meaningful for the configured
/// Prometheus fallback. Hop-by-hop and client-controlled transport headers are
/// intentionally excluded.
fn extract_fallback_headers(headers: &HeaderMap) -> HashMap<String, String> {
    const FORWARDED: [&str; 3] = ["authorization", "x-scope-orgid", "x-asap-tenant"];
    FORWARDED
        .into_iter()
        .filter_map(|name| {
            headers
                .get(name)
                .and_then(|value| value.to_str().ok())
                .map(|value| (name.to_string(), value.to_string()))
        })
        .collect()
}

#[derive(Debug, Clone)]
pub struct HttpServerConfig {
    pub port: u16,
    pub handle_http_requests: bool,
    pub adapter_config: AdapterConfig,
}

#[derive(Clone)]
pub struct HttpServer {
    config: HttpServerConfig,
    adapter_override: Option<Arc<dyn HttpProtocolAdapter>>,
    query_engine: Arc<ASAPQueryEngine>,
    /// Phase-5/6 capability router. Built from `query_engine` at
    /// construction time (`ASAPQueryEngine` registered as the ASAP-tier
    /// `QueryEngine`) and extended via [`Self::with_query_engine`] —
    /// e.g. to plug in a `GorillaQueryEngine` for the cold archive
    /// tier. Instant-query dispatch consults this for metrics whose
    /// `StreamingConfig::storage_backend()` is anything other than
    /// `SketchStore`; ASAP-tier queries still take the direct
    /// `ASAPQueryEngine::handle_query` path so they keep the
    /// `KeyByLabelNames` Prometheus needs to populate the `metric`
    /// map. See `docs/design-gorilla-s3-cold-engine.md` §8.
    query_router: Arc<EngineRouter>,
    /// Sketch storage for runtime diagnostics.
    summary_store: Arc<crate::storage_engines::sketch_db::index::SketchStore>,
    /// Hot-reloadable `StreamingConfig` source. `None` when hot-reload
    /// is not wired up by the caller (unit tests, legacy binaries).
    hot_reload_config: Option<crate::storage_engines::types::StreamingConfigHandle>,
    /// Per-metric storage-backend routing table consulted by the HTTP
    /// instant-query handler at request time. When `Some(..)` and the
    /// query parses, the handler extracts the metric name from the
    /// PromQL AST, consults this table, and dispatches through
    /// `EngineRouter` for any per-metric override. When `None` the
    /// handler falls back to the pre-Phase-5 behaviour of consulting
    /// the streaming-config's single `storage_backend()` axis (which
    /// itself defaults to `SketchStore`). Wired by the binary via
    /// [`Self::with_backend_storage_routing`]; production deploys
    /// bootstrap from `deploy/configs/backend-storage-routing.yaml`
    /// (legacy form) or the control plane's first
    /// `POST /api/v1/storage_routing` push.
    ///
    /// `HotReloadBackendStorageRouting` is an atomic routing snapshot
    /// — an `ArcSwap`-backed wrapper that supports atomic at-runtime
    /// swap from the `POST /api/v1/storage_routing` endpoint. The
    /// existing read path snapshots the wrapper once per request
    /// (`handle.snapshot().lookup_with_shape(...)`); swap is observed
    /// by the next request without restart.
    backend_storage_routing: Option<crate::query_engines::routing::HotReloadBackendStorageRouting>,
    /// Backfill registry shared with the worker and HTTP job endpoints.
    backfill: Option<Arc<crate::storage_engines::sketch_db::BackfillRegistry>>,
    /// SketchStore data-retention horizon in millis, mirroring
    /// `--persistence-delete-older-than-secs` at the CLI. Used by the
    /// `POST /api/v1/db/backfill` handler to gate job creation via
    /// `BackfillRegistry::create_checked` (§10.5 Method B). `None`
    /// disables the retention precheck — the handler still enforces
    /// the §10.5 time-disjoint invariant.
    data_retention_ms: Option<u64>,
    /// Freshness-probe last-value cache (issue #46 ⑥). When `Some`,
    /// the query handler intercepts
    /// `last_over_time(<probe>[<range>])` for probe-shaped metric
    /// names and answers from RAM. The same cache is fed by the OTLP
    /// receiver — see `OtlpReceiver::with_probe_cache`. `None`
    /// disables the short-circuit; the dispatch falls through to the
    /// normal routing-table path, which observes the warm tier's
    /// 60–90 s flush gap.
    probe_cache: Option<Arc<FreshnessProbeCache>>,
    /// Serializes multi-document physical-plan publication so two control
    /// plane generations cannot interleave their plan projections and catalog.
    physical_plan_lock: Arc<tokio::sync::Mutex<()>>,
    active_physical_plan: Option<crate::storage_engines::types::ActivePhysicalPlanHandle>,
    physical_plan_lifecycle: Option<crate::storage_engines::types::PhysicalPlanLifecycle>,
    remote_write: Option<crate::drivers::ingest::PrometheusRemoteWriteReceiver>,
}

#[derive(Clone)]
struct AppState {
    config: HttpServerConfig,
    query_engine: Arc<ASAPQueryEngine>,
    /// See [`HttpServer::query_router`].
    query_router: Arc<EngineRouter>,
    /// Attach sketch storage for HTTP runtime diagnostics.
    summary_store: Arc<crate::storage_engines::sketch_db::index::SketchStore>,
    adapter: Arc<dyn HttpProtocolAdapter>,
    fallback: Option<Arc<dyn crate::drivers::query::fallback::FallbackClient>>,
    hot_reload_config: Option<crate::storage_engines::types::StreamingConfigHandle>,
    /// See [`HttpServer::backend_storage_routing`].
    backend_storage_routing: Option<crate::query_engines::routing::HotReloadBackendStorageRouting>,
    /// Backfill registry (sketch DB §10). See `HttpServer::backfill`.
    backfill: Option<Arc<crate::storage_engines::sketch_db::BackfillRegistry>>,
    /// See `HttpServer::data_retention_ms`.
    data_retention_ms: Option<u64>,
    /// See [`HttpServer::probe_cache`].
    probe_cache: Option<Arc<FreshnessProbeCache>>,
    physical_plan_lock: Arc<tokio::sync::Mutex<()>>,
    active_physical_plan: Option<crate::storage_engines::types::ActivePhysicalPlanHandle>,
    physical_plan_lifecycle: Option<crate::storage_engines::types::PhysicalPlanLifecycle>,
    remote_write: Option<crate::drivers::ingest::PrometheusRemoteWriteReceiver>,
}

impl HttpServer {
    pub fn new(
        config: HttpServerConfig,
        query_engine: Arc<ASAPQueryEngine>,
        summary_store: Arc<crate::storage_engines::sketch_db::index::SketchStore>,
    ) -> Self {
        // Bootstrap the capability router with `ASAPQueryEngine`
        // registered under its canonical query-engine id.
        let mut router = EngineRouter::new();
        router.register(query_engine.clone() as Arc<dyn QueryEngine>);
        let query_router = Arc::new(router);
        Self {
            config,
            adapter_override: None,
            query_engine,
            query_router,
            summary_store,
            hot_reload_config: None,
            backend_storage_routing: None,
            backfill: None,
            data_retention_ms: None,
            probe_cache: None,
            physical_plan_lock: Arc::new(tokio::sync::Mutex::new(())),
            active_physical_plan: None,
            physical_plan_lifecycle: None,
            remote_write: None,
        }
    }

    /// Use a language-specific protocol adapter without extending the shared
    /// protocol enum. This keeps VictoriaMetrics transport policy at its own
    /// listener boundary.
    pub fn with_protocol_adapter(mut self, adapter: Arc<dyn HttpProtocolAdapter>) -> Self {
        self.adapter_override = Some(adapter);
        self
    }

    /// Clone the fully configured query service onto another query listener.
    pub fn with_query_listener(mut self, port: u16, adapter_config: AdapterConfig) -> Self {
        self.config.port = port;
        self.config.adapter_config = adapter_config;
        self.remote_write = None;
        self
    }

    /// Enable Prometheus Remote Write v1 on the same public HTTP listener.
    pub fn with_remote_write(
        mut self,
        receiver: crate::drivers::ingest::PrometheusRemoteWriteReceiver,
    ) -> Self {
        self.remote_write = Some(receiver);
        self
    }

    #[cfg(test)]
    /// Plug an additional [`QueryEngine`] into the capability router.
    /// Used by the binary to register `GorillaQueryEngine` (cold
    /// archive) alongside the `ASAPQueryEngine` registered by `new`.
    /// Engines are keyed by their `data_source_id`; calling this with
    /// an engine whose id collides with an already-registered one
    /// replaces the previous registration (matches `EngineRouter`'s
    /// hot-swap contract).
    pub fn with_query_engine(mut self, engine: Arc<dyn QueryEngine>) -> Self {
        // The router stored here is the canonical one — callers always
        // hold an `Arc<EngineRouter>`, so we rebuild from a fresh
        // `EngineRouter::clone()` (cheap; the `engines` map clones the
        // inner `Arc`s, not the engines themselves).
        let mut router: EngineRouter = (*self.query_router).clone();
        router.register(engine);
        self.query_router = Arc::new(router);
        self
    }

    /// Attach a `StreamingConfigHandle` handle so the
    /// `GET/POST /api/v1/streaming-config` endpoints can read and
    /// swap the currently active config. Without this handle the
    /// endpoints return `503 Service Unavailable`.
    pub fn with_hot_reload_config(
        mut self,
        handle: crate::storage_engines::types::StreamingConfigHandle,
    ) -> Self {
        self.hot_reload_config = Some(handle);
        self
    }

    pub fn with_active_physical_plan(
        mut self,
        handle: crate::storage_engines::types::ActivePhysicalPlanHandle,
    ) -> Self {
        self.physical_plan_lifecycle = Some(
            crate::storage_engines::types::PhysicalPlanLifecycle::new(handle.clone()),
        );
        self.active_physical_plan = Some(handle);
        self
    }

    /// Attach a per-metric storage-backend routing table. The table is
    /// wrapped in a hot-reload handle internally so the
    /// `POST /api/v1/storage_routing` endpoint can swap it
    /// atomically without restart.
    ///
    /// Bootstrap typically comes from
    /// `BackendStorageRouting::from_yaml_file(...)` for legacy / dev
    /// deploys, or from `BackendStorageRouting::empty()` when the
    /// control plane will push the first table — the control plane's first
    /// `POST /api/v1/storage_routing` then fills in all the entries.
    ///
    /// When the wrapper is attached, every instant query consults the
    /// snapshot (after extracting the metric name from the PromQL AST)
    /// and dispatches through `EngineRouter` for any per-metric
    /// override. Without the wrapper the handler falls back to the
    /// pre-Phase-5 single-axis behaviour driven by
    /// `StreamingConfig::storage_backend()`.
    pub fn with_backend_storage_routing(
        mut self,
        routing: Arc<crate::storage_engines::types::BackendStorageRouting>,
    ) -> Self {
        self.backend_storage_routing =
            Some(crate::query_engines::routing::HotReloadBackendStorageRouting::from_arc(routing));
        self
    }

    /// Attach a pre-built hot-reload routing handle.
    /// Used by callers that want to share the same handle with other
    /// subsystems (e.g. the query-router for diagnostics) — the
    /// `with_backend_storage_routing` builder is the simpler entry
    /// point that wraps an `Arc<BackendStorageRouting>` for callers
    /// that don't.
    pub fn with_hot_reload_backend_storage_routing(
        mut self,
        handle: crate::query_engines::routing::HotReloadBackendStorageRouting,
    ) -> Self {
        self.backend_storage_routing = Some(handle);
        self
    }

    /// Attach a `BackfillRegistry` so the `/api/v1/db/backfill`
    /// HTTP endpoints can create and inspect jobs. Jobs
    /// remain `Queued` unless a backfill worker is enabled.
    pub fn with_backfill_registry(
        mut self,
        registry: Arc<crate::storage_engines::sketch_db::BackfillRegistry>,
    ) -> Self {
        self.backfill = Some(registry);
        self
    }

    /// Declare the SketchStore data-retention horizon (the value of
    /// `--persistence-delete-older-than-secs` * 1000). When set, the
    /// `POST /api/v1/db/backfill` handler runs `create_checked` with
    /// this bound, so jobs that would write windows older than the
    /// retention horizon are rejected up-front instead of being
    /// silently evicted right after write (§10.5 Method B).
    pub fn with_data_retention_ms(mut self, data_retention_ms: u64) -> Self {
        self.data_retention_ms = Some(data_retention_ms);
        self
    }

    /// Attach a [`FreshnessProbeCache`] so the HTTP query handler
    /// answers `last_over_time(<probe>[<range>])` from RAM. The same
    /// `Arc` should be handed to the OTLP receiver via
    /// `OtlpReceiver::with_probe_cache` so writes and reads see the
    /// same cache state. Without this call, probe queries fall
    /// through to the cold archive — fine for queries with a
    /// generous lookback (≥1m) but produces an empty result for the
    /// MVP demo's 10 s window. See issue #46 ⑥ for the failure
    /// mode.
    pub fn with_probe_cache(mut self, cache: Arc<FreshnessProbeCache>) -> Self {
        self.probe_cache = Some(cache);
        self
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        srv_metrics::register_all();

        let adapter = self.adapter_override.clone().unwrap_or_else(|| {
            Arc::new(PrometheusHttpAdapter::new(
                self.config.adapter_config.clone(),
            ))
        });

        let query_endpoint = adapter.get_query_endpoint();
        let runtime_info_path = adapter.get_runtime_info_path();
        info!(
            "Adapter '{}' configured for endpoint: {}",
            adapter.adapter_name(),
            query_endpoint
        );
        info!("Runtime info endpoint: {}", runtime_info_path);

        let request_body_limit = self
            .remote_write
            .as_ref()
            .map(|receiver| receiver.config().max_compressed_bytes)
            .unwrap_or(2 * 1024 * 1024);
        let app_state = AppState {
            config: self.config.clone(),
            query_engine: self.query_engine,
            query_router: self.query_router,
            summary_store: self.summary_store,
            adapter: adapter.clone(),
            fallback: self.config.adapter_config.fallback.clone(),
            hot_reload_config: self.hot_reload_config.clone(),
            backend_storage_routing: self.backend_storage_routing.clone(),
            backfill: self.backfill.clone(),
            data_retention_ms: self.data_retention_ms,
            probe_cache: self.probe_cache.clone(),
            physical_plan_lock: self.physical_plan_lock.clone(),
            active_physical_plan: self.active_physical_plan.clone(),
            physical_plan_lifecycle: self.physical_plan_lifecycle.clone(),
            remote_write: self.remote_write.clone(),
        };

        let range_query_endpoint = adapter.get_range_query_endpoint();

        let app = Router::new()
            .route(query_endpoint, get(handle_instant_query))
            .route(query_endpoint, post(handle_instant_query_post))
            .route(range_query_endpoint, get(handle_range_query))
            .route(range_query_endpoint, post(handle_range_query_post))
            .route(runtime_info_path, get(handle_runtime_info))
            .route(runtime_info_path, post(handle_runtime_info))
            .route("/metrics", get(handle_metrics))
            .route("/api/v1/health", get(handle_health))
            .route(
                "/api/v1/write",
                post(handle_prometheus_remote_write)
                    .layer(DefaultBodyLimit::max(request_body_limit)),
            )
            .route("/api/v1/store/metrics", get(handle_store_metrics))
            .route("/api/v1/precompute/drain", post(handle_precompute_drain))
            .route("/api/v1/physical-plan", post(handle_post_physical_plan))
            .route(
                "/api/v1/physical-plan/discard",
                post(handle_discard_physical_plan),
            )
            .route(
                "/api/v1/physical-plan/activate",
                post(handle_activate_physical_plan),
            )
            .route(
                "/api/v1/physical-plan/status",
                get(handle_physical_plan_status),
            )
            .route("/api/v1/summary-inventory", get(handle_summary_inventory))
            // control-plane-pushed `BackendStorageRouting`
            // table. POST replaces the current table atomically; GET
            // returns a JSON snapshot for operator diagnostics.
            .route(
                "/api/v1/storage_routing",
                get(handle_get_storage_routing).post(handle_post_storage_routing),
            )
            .route("/api/v1/db/schemas", get(handle_get_schemas))
            .route(
                "/api/v1/db/schemas/:sid/retire",
                post(handle_post_schema_retire),
            )
            .route(
                "/api/v1/db/schemas/:sid/expire",
                post(handle_post_schema_expire),
            )
            .route("/api/v1/db/timeline", get(handle_get_timeline))
            .route("/api/v1/db/backfill", post(handle_post_backfill_job))
            .route("/api/v1/db/backfill/jobs", get(handle_get_backfill_jobs))
            .route(
                "/api/v1/db/backfill/jobs/:job_id",
                get(handle_get_backfill_job).delete(handle_delete_backfill_job),
            );
        // Legacy partial configuration is available only when the distributed
        // profile explicitly supplies its hot-reload handle. The ASAPQuery
        // profile exposes only generation-scoped physical-plan publication.
        let app = if app_state.hot_reload_config.is_some() {
            app.route(
                "/api/v1/streaming-config",
                get(handle_get_streaming_config).post(handle_post_streaming_config),
            )
        } else {
            app
        };
        let app = if adapter.query_language() == asap_types::QueryLanguage::MetricsQl {
            app.route(
                "/select/:tenant/prometheus/api/v1/query",
                get(handle_vm_cluster_instant).post(handle_vm_cluster_instant_post),
            )
            .route(
                "/select/:tenant/prometheus/api/v1/query_range",
                get(handle_vm_cluster_range).post(handle_vm_cluster_range_post),
            )
        } else {
            app
        };
        let app = app.with_state(app_state);

        let listener = TcpListener::bind(format!("0.0.0.0:{}", self.config.port)).await?;
        info!("HTTP server listening on port {}", self.config.port);

        axum::serve(listener, app).await?;
        Ok(())
    }

    /// Start server for testing on a random available port. Returns the
    /// actual port number used.
    ///
    /// Intentionally not gated behind `#[cfg(test)]` — integration tests
    /// under `data_plane/tests/` are compiled separately from the lib's
    /// own unit tests and need this entry point. The name + doc-comment
    /// make the testing intent explicit; production callers should use
    /// the regular `start()` method.
    pub async fn start_test_server(&self) -> Result<u16, Box<dyn std::error::Error + Send + Sync>> {
        let adapter = self.adapter_override.clone().unwrap_or_else(|| {
            Arc::new(PrometheusHttpAdapter::new(
                self.config.adapter_config.clone(),
            ))
        });

        let query_endpoint = adapter.get_query_endpoint();
        let runtime_info_path = adapter.get_runtime_info_path();

        let request_body_limit = self
            .remote_write
            .as_ref()
            .map(|receiver| receiver.config().max_compressed_bytes)
            .unwrap_or(2 * 1024 * 1024);
        let app_state = AppState {
            config: self.config.clone(),
            query_engine: self.query_engine.clone(),
            query_router: self.query_router.clone(),
            summary_store: self.summary_store.clone(),
            adapter: adapter.clone(),
            fallback: self.config.adapter_config.fallback.clone(),
            hot_reload_config: self.hot_reload_config.clone(),
            backend_storage_routing: self.backend_storage_routing.clone(),
            backfill: self.backfill.clone(),
            data_retention_ms: self.data_retention_ms,
            probe_cache: self.probe_cache.clone(),
            physical_plan_lock: self.physical_plan_lock.clone(),
            active_physical_plan: self.active_physical_plan.clone(),
            physical_plan_lifecycle: self.physical_plan_lifecycle.clone(),
            remote_write: self.remote_write.clone(),
        };

        let range_query_endpoint = adapter.get_range_query_endpoint();

        let app = Router::new()
            .route("/api/v1/precompute/drain", post(handle_precompute_drain))
            .route(query_endpoint, get(handle_instant_query))
            .route(query_endpoint, post(handle_instant_query_post))
            .route(range_query_endpoint, get(handle_range_query))
            .route(range_query_endpoint, post(handle_range_query_post))
            .route(runtime_info_path, get(handle_runtime_info))
            .route("/metrics", get(handle_metrics))
            .route("/api/v1/health", get(handle_health))
            .route(
                "/api/v1/write",
                post(handle_prometheus_remote_write)
                    .layer(DefaultBodyLimit::max(request_body_limit)),
            )
            .route("/api/v1/physical-plan", post(handle_post_physical_plan))
            .route(
                "/api/v1/physical-plan/discard",
                post(handle_discard_physical_plan),
            )
            .route(
                "/api/v1/physical-plan/activate",
                post(handle_activate_physical_plan),
            )
            .route(
                "/api/v1/physical-plan/status",
                get(handle_physical_plan_status),
            )
            .route("/api/v1/summary-inventory", get(handle_summary_inventory))
            // control-plane-pushed `BackendStorageRouting`
            // table. POST replaces the current table atomically; GET
            // returns a JSON snapshot for operator diagnostics.
            .route(
                "/api/v1/storage_routing",
                get(handle_get_storage_routing).post(handle_post_storage_routing),
            )
            .route("/api/v1/db/schemas", get(handle_get_schemas))
            .route(
                "/api/v1/db/schemas/:sid/retire",
                post(handle_post_schema_retire),
            )
            .route(
                "/api/v1/db/schemas/:sid/expire",
                post(handle_post_schema_expire),
            )
            .route("/api/v1/db/timeline", get(handle_get_timeline))
            .route("/api/v1/db/backfill", post(handle_post_backfill_job))
            .route("/api/v1/db/backfill/jobs", get(handle_get_backfill_jobs))
            .route(
                "/api/v1/db/backfill/jobs/:job_id",
                get(handle_get_backfill_job).delete(handle_delete_backfill_job),
            );
        let app = if app_state.hot_reload_config.is_some() {
            app.route(
                "/api/v1/streaming-config",
                get(handle_get_streaming_config).post(handle_post_streaming_config),
            )
        } else {
            app
        };
        let app = if adapter.query_language() == asap_types::QueryLanguage::MetricsQl {
            app.route(
                "/select/:tenant/prometheus/api/v1/query",
                get(handle_vm_cluster_instant).post(handle_vm_cluster_instant_post),
            )
            .route(
                "/select/:tenant/prometheus/api/v1/query_range",
                get(handle_vm_cluster_range).post(handle_vm_cluster_range_post),
            )
        } else {
            app
        };
        let app = app.with_state(app_state);

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let actual_port = listener.local_addr()?.port();

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        // Give the server time to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        Ok(actual_port)
    }
}

/// Core query execution logic shared between GET and POST handlers.
///
/// `engine_override` (Phase-6 Fix 1): when `Some(data_source_id)` the
/// per-metric `BackendStorageRouting` lookup is bypassed and the query
/// is dispatched straight to the named engine. Set by the
/// `X-ASAP-Engine` request header (POST) or the `?engine=` query
/// parameter (GET / POST), both consumed at the handler boundary
/// before the request reaches this function. Used by the accuracy
/// reducer to query the same PromQL against warm sketch and Gorilla
/// archive for cross-tier rel-err computation.
async fn process_query_request(
    state: &AppState,
    parsed_request: &ParsedQueryRequest,
    start_time: Instant,
    headers: HashMap<String, String>,
    engine_override: Option<String>,
    accuracy: AccuracyTarget,
    tenant: &str,
) -> Response {
    // Check if handling is enabled
    if !state.config.handle_http_requests {
        debug!("HTTP request handling is disabled");
        if let Some(fallback) = &state.fallback {
            debug!("Forwarding to fallback due to disabled handling");
            return match fallback
                .execute_query_with_headers(parsed_request, headers)
                .await
            {
                Ok(response) => response.into_response(),
                Err(status) => status.into_response(),
            };
        } else {
            debug!("Returning error - both handling and forwarding disabled");
            use crate::drivers::query::adapters::AdapterError;
            return match state
                .adapter
                .format_error_response(&AdapterError::ProtocolError(
                    "Query handling is disabled".to_string(),
                ))
                .await
            {
                Ok(json) => json.into_response(),
                Err(status) => status.into_response(),
            };
        }
    }

    // Phase-6 Fix 1: per-query engine override.
    //
    // If the caller explicitly named an engine (via `X-ASAP-Engine`
    // header or `?engine=` query param) bypass `BackendStorageRouting`
    // and dispatch directly — the accuracy reducer uses this to pin one
    // engine per replay row.
    if let Some(override_id) = engine_override.as_deref() {
        return process_via_named_engine(state, parsed_request, start_time, override_id).await;
    }

    let language_identity = state.adapter.canonical_plan_identity(&parsed_request.query);
    if state.adapter.query_language() == asap_types::QueryLanguage::MetricsQl
        && language_identity.is_err()
    {
        if let Some(fallback) = &state.fallback {
            return match fallback
                .execute_query_with_headers(parsed_request, headers)
                .await
            {
                Ok(response) => response.into_response(),
                Err(status) => status.into_response(),
            };
        }
    }
    if let Ok(Some(identity)) = language_identity {
        let evaluation_ms = if parsed_request.time > 0.0 {
            (parsed_request.time * 1_000.0) as u64
        } else {
            unix_time_ms()
        };
        if let Ok(query_result) = state
            .query_engine
            .execute_metricsql_at(&identity, evaluation_ms)
            .await
        {
            let result = crate::drivers::query::adapters::QueryExecutionResult {
                query_output_labels: asap_types::KeyByLabelNames::default(),
                query_result,
            };
            return match state.adapter.format_success_response(&result).await {
                Ok(response) => {
                    annotate_data_source(response, StorageBackend::SketchStore.data_source_id())
                        .await
                }
                Err(status) => status.into_response(),
            };
        }
        if let Some(fallback) = &state.fallback {
            return match fallback
                .execute_query_with_headers(parsed_request, headers)
                .await
            {
                Ok(response) => response.into_response(),
                Err(status) => status.into_response(),
            };
        }
    }

    // Issue #46 ⑥ — freshness-probe short-circuit.
    //
    // The MVP demo's freshness criterion polls
    // `last_over_time(http_freshness_probe_warm[10s])` at 10 Hz. The
    // probe metric flows through the agent's
    // `[gorillas3 → ddsketch → batch] → backend OTLP` pipeline; the
    // cold tier's `gorillas3 → 60 s TSDB block → Thanos sync` path
    // adds 60–90 s of flush latency, so a 10 s lookback against the
    // cold archive returns an empty vector for the entire run
    // (replay client logged `attempted=600 got=0`). The OTLP receiver
    // captures the latest sample for every probe metric in
    // [`AppState::probe_cache`]; here we intercept the matching query
    // shape before it falls through to the routing table and answer
    // from RAM with sub-second freshness. When the cache has no entry
    // inside the lookback window the intercept returns `None` and
    // dispatch falls through to the normal path — preserving the
    // long-window queries (≥1m) that the cold archive still answers
    // correctly.
    if let Some(response) = try_answer_freshness_probe(state, parsed_request, start_time).await {
        return response;
    }

    // Step 2: Pick a dispatch path based on the metric's pinned
    // storage backend (Phase-5 capability routing).
    //
    // Routing precedence:
    //   (a) Per-metric `BackendStorageRouting` table (loaded from
    //       `backend-storage-routing.yaml` at startup). The PromQL
    //       query is parsed; the metric name is extracted from the
    //       AST and looked up in the table. This is the production
    //       path the issue-46 MVP relies on so non-default metrics
    //       actually route through the `EngineRouter`.
    //   (b) Single-axis `StreamingConfig::storage_backend()` from the
    //       hot-reload config (the pre-Phase-5 fallback). Pre-control-plane
    //       deploys ride this path; it always lands on `SketchStore`
    //       unless the YAML was hand-patched.
    //   (c) Default — `SketchStore`. Keeps the direct
    //       `ASAPQueryEngine::handle_query` path so the response carries
    //       the `KeyByLabelNames` the Prometheus adapter needs to
    //       populate the `metric` map.
    //
    // For non-`SketchStore` axes the dispatch goes through the
    // `EngineRouter`. Phase-6 (Gorilla MVP) returns a scalar with
    // empty labels, so dropping `KeyByLabelNames` is acceptable; the
    // response carries `accuracy` + `data_source` via the
    // wire-extension annotations.
    let metric_storage = resolve_metric_storage(state, &parsed_request.query, tenant);
    debug!(
        "Dispatch axis: metric_storage={:?} tenant={} (from backend-storage-routing: {}, hot-reload: {})",
        metric_storage,
        tenant,
        state.backend_storage_routing.is_some(),
        state.hot_reload_config.is_some(),
    );

    // `X-ASAP-Accuracy: exact` means "do not answer from the ε/δ-bounded
    // sketches". Since #746 removed the archive tier there is no exact
    // engine left in-process, so an exact request goes straight to the
    // Prometheus fallback (or the adapter's unsupported-query response
    // when no fallback is configured).
    if matches!(accuracy, AccuracyTarget::Exact) {
        debug!("Exact accuracy requested — bypassing the sketch tier");
        return forward_instant_to_fallback(state, parsed_request, headers).await;
    }

    if matches!(metric_storage, StorageBackend::SketchStore) {
        process_via_simple_engine(state, parsed_request, start_time, headers).await
    } else {
        process_via_router(state, parsed_request, start_time, metric_storage, headers).await
    }
}

/// Range sibling of [`forward_instant_to_fallback`].
async fn forward_range_to_fallback(
    state: &AppState,
    parsed_request: &ParsedRangeQueryRequest,
    headers: HashMap<String, String>,
) -> Response {
    if let Some(fallback) = &state.fallback {
        return match fallback
            .execute_range_query_with_headers(parsed_request, headers)
            .await
        {
            Ok(response) => response.into_response(),
            Err(status) => status.into_response(),
        };
    }
    match state.adapter.format_unsupported_query_response().await {
        Ok(json) => json.into_response(),
        Err(status) => status.into_response(),
    }
}

/// Forward an instant query to the Prometheus fallback, or render the
/// adapter's unsupported-query response when no fallback is configured.
///
/// The single exit for "the ASAP tier cannot serve this" since #746: an
/// exact-accuracy request, or a router that ran out of compatible
/// engines.
async fn forward_instant_to_fallback(
    state: &AppState,
    parsed_request: &ParsedQueryRequest,
    headers: HashMap<String, String>,
) -> Response {
    if let Some(fallback) = &state.fallback {
        return match fallback
            .execute_query_with_headers(parsed_request, headers)
            .await
        {
            Ok(response) => response.into_response(),
            Err(status) => status.into_response(),
        };
    }
    match state.adapter.format_unsupported_query_response().await {
        Ok(json) => json.into_response(),
        Err(status) => status.into_response(),
    }
}

/// Resolve the [`StorageBackend`] that should handle this query.
///
/// Routing precedence (see `process_query_request` for context):
/// 1. Per-metric `BackendStorageRouting` table — parse the PromQL,
///    pull the metric name out of the AST, look it up in the table.
///    This is the path issue #46's MVP demo relies on.
/// 2. Streaming-config single-axis fallback — preserves pre-Phase-5
///    behaviour for deploys that haven't loaded a routing table.
/// 3. Default `SketchStore`.
///
/// Parsing failures fall through to (2)/(3) so a malformed PromQL
/// doesn't surface as a routing 5xx (the engines themselves will
/// reject it with a clearer error).
///
/// v7: when the routing table has multi-target rows for the metric,
/// the parsed AST is also classified via
/// [`crate::storage_engines::types::classify_query_shape`] and the lookup picks
/// the target whose `applies_to_query_shape` matches. v6.1
/// single-target metrics keep their original semantics — every shape
/// resolves to the one configured backend.
fn resolve_metric_storage(state: &AppState, query: &str, tenant: &str) -> StorageBackend {
    // A non-bootstrap atomic CompiledPhysicalPlan owns routing. Every request first
    // enters the ASAP engine, where QueryPlan lookup either executes its
    // compiler-bound DAG or returns an explicit fallback reason. Consulting
    // the legacy shape/SID candidate heuristics here would bypass QueryPlan
    // (and can also discard the request's explicit evaluation timestamp).
    if state.active_physical_plan.as_ref().is_some_and(|active| {
        let snapshot = active.active_snapshot();
        snapshot.query_plan.plan_id != 0 && !snapshot.query_plan.entries.is_empty()
    }) {
        debug!(
            tenant,
            query, "resolve_metric_storage: active QueryPlan owns warm/fallback routing"
        );
        return StorageBackend::SketchStore;
    }

    if let Some(routing_handle) = state.backend_storage_routing.as_ref() {
        // snapshot the hot-reload handle once per request,
        // scoped to this request's tenant. The snapshot resolves to
        // the named tenant's table when present, else the
        // `default` tenant's table. Concurrent per-tenant swaps from
        // `POST /api/v1/storage_routing` produce a fresh `Arc`; this
        // snapshot remains valid for the rest of the dispatch (no
        // torn read).
        let routing = routing_handle.snapshot_for_tenant(tenant);
        match promql_parser::parser::parse(query) {
            Ok(expr) => {
                if let Some(metric_name) = first_metric_name(&expr) {
                    let shape = crate::storage_engines::types::classify_query_shape(&expr);
                    let mut backend = routing.lookup_with_shape(&metric_name, shape);

                    // ── ExactAgg(Sum) override for rate / topk shapes ──
                    // The control plane's `build_routing_entry` puts
                    // `RatePostHoc` and `Topk` (when no CountSketch is
                    // planned) on the archive's claim list — based on
                    // the assumption that warm-tier sketches can't
                    // serve them natively. For metrics backed by
                    // ExactAgg(Sum) sids the warm tier CAN serve
                    // `rate(metric[r])`, `sum by (gbk) (rate(...))`,
                    // and `topk(K, sum by (gbk) (rate(...)))` via the
                    // engine's `evaluate_exact_agg_rate` reducer +
                    // `try_topk_over_rate_fallback` engine path. When
                    // we see those shapes routed to a non-SketchStore
                    // backend but ExactAgg(Sum) sids exist for the
                    // metric, override to SketchStore so the asap
                    // engine answers natively.
                    if !matches!(backend, StorageBackend::SketchStore)
                        && matches!(
                            shape,
                            crate::storage_engines::types::QueryOperatorShape::RatePostHoc
                                | crate::storage_engines::types::QueryOperatorShape::Topk
                        )
                        && metric_has_exact_agg_sum_sid(&state.summary_store, &metric_name)
                    {
                        debug!(
                            "resolve_metric_storage: overriding {:?} → SketchStore \
                             for metric={} shape={:?} (ExactAgg(Sum) sid present, \
                             warm tier serves rate/topk via exact-agg reducer)",
                            backend, metric_name, shape,
                        );
                        backend = StorageBackend::SketchStore;
                    }

                    debug!(
                        "resolve_metric_storage: routing-table hit for tenant={} metric={} shape={:?} → {:?}",
                        tenant, metric_name, shape, backend,
                    );
                    return backend;
                }
                debug!(
                    "resolve_metric_storage: PromQL parsed but no metric name found in AST; falling back to streaming-config axis",
                );
            }
            Err(e) => {
                debug!(
                    "resolve_metric_storage: PromQL parse failed ({}); falling back to streaming-config axis",
                    e,
                );
            }
        }
    }

    state
        .hot_reload_config
        .as_ref()
        .map(|h| h.snapshot().storage_backend())
        .unwrap_or_default()
}

/// True when the sketch index carries at least one ExactAgg(Sum-family)
/// sid for `metric_name`. Used by [`resolve_metric_storage`] to override
/// the routing-table decision for `rate(...)` / `topk(...)` shapes when
/// the warm tier can answer them via the engine's
/// `evaluate_exact_agg_rate` reducer.
///
/// Subset-match-on-group-by-keys isn't needed here — we only care
/// whether the metric has ANY ExactAgg(Sum) sid; the engine's
/// `try_topk_over_rate_fallback` + per-candidate dispatch handle the
/// per-(group_by_keys) match downstream.
fn metric_has_exact_agg_sum_sid(
    idx: &crate::storage_engines::sketch_db::index::SketchStore,
    metric_name: &str,
) -> bool {
    use crate::storage_engines::sketch_db::data::AggregationType;
    use crate::storage_engines::sketch_db::index::Capability;
    let sids = idx.instances_matching(metric_name, &std::collections::BTreeSet::new());
    for sid in sids {
        if let Some(meta) = idx.instance(sid) {
            if let Some(cap) = meta.capability.as_ref() {
                if matches!(
                    cap,
                    Capability::ExactAgg(
                        AggregationType::Sum
                            | AggregationType::MultipleSum
                            | AggregationType::Increase
                            | AggregationType::MultipleIncrease
                    )
                ) {
                    return true;
                }
            }
        }
    }
    false
}

/// Walk a PromQL AST and return the first metric name we encounter.
/// Used by the routing-table lookup to pick a key. PromQL queries that
/// reference multiple metrics (e.g. `a / on(x) b`) are not currently
/// supported by the routing table — the first-encountered metric wins.
/// In practice the issue-46 demo replay queries each touch exactly one
/// metric, so this heuristic is correct for the MVP.
fn first_metric_name(expr: &promql_parser::parser::Expr) -> Option<String> {
    use promql_parser::parser::Expr;
    match expr {
        Expr::VectorSelector(vs) => vs.name.clone(),
        Expr::MatrixSelector(ms) => ms.vs.name.clone(),
        Expr::Call(call) => call.args.args.iter().find_map(|a| first_metric_name(a)),
        Expr::Aggregate(agg) => first_metric_name(&agg.expr),
        Expr::Binary(bin) => first_metric_name(&bin.lhs).or_else(|| first_metric_name(&bin.rhs)),
        Expr::Subquery(sq) => first_metric_name(&sq.expr),
        Expr::Paren(p) => first_metric_name(&p.expr),
        Expr::Unary(u) => first_metric_name(&u.expr),
        _ => None,
    }
}

/// Issue #46 ⑥ short-circuit — answer
/// `last_over_time(<probe>[<range>])` from the freshness probe cache.
///
/// Recognises queries of shape `last_over_time(<metric>[<range>])`
/// (also wrapped in `Paren` / `Unary`) where `<metric>` matches the
/// `http_freshness_probe_*` family. Returns `Some(response)` only when
/// the cache is configured AND has an entry whose `ts_ms` falls inside
/// the lookback window `[now − range, now]`. Every other shape and
/// every cache miss returns `None` — the caller falls through to the
/// normal routing-table dispatch unchanged.
///
/// Response shape mirrors `process_via_router`'s success path: a
/// Prometheus instant vector with one element (empty labels, scalar
/// value), a `data_source: asap_query` info-line so the wire format
/// is consistent with the ASAP-tier path the routing table comment
/// describes as the right home for the `_warm` probe.
async fn try_answer_freshness_probe(
    state: &AppState,
    parsed_request: &ParsedQueryRequest,
    start_time: Instant,
) -> Option<Response> {
    use crate::drivers::query::adapters::QueryExecutionResult;
    use crate::query_engines::query_result::{InstantVectorElement, QueryResult};
    use asap_types::KeyByLabelNames;

    let cache = state.probe_cache.as_ref()?;
    let (metric, range_ms) = parse_last_over_time_probe(&parsed_request.query)?;
    if !crate::query_engines::routing::is_freshness_probe(&metric) {
        return None;
    }

    // `parsed_request.time` is unix seconds (instant query). Convert
    // to ms to match the cache's storage scale. A `time` of `0.0` (the
    // adapter's default for "no time given") falls back to wall clock,
    // matching Prometheus's instant-query semantics.
    let now_ms = if parsed_request.time > 0.0 {
        (parsed_request.time * 1_000.0) as i64
    } else {
        crate::query_engines::routing::freshness_probe_now_ms()
    };
    let sample = cache.lookup(&metric, now_ms, range_ms)?;

    debug!(
        metric = %metric,
        sample_ts_ms = sample.ts_ms,
        sample_value = sample.value,
        now_ms,
        range_ms,
        "freshness-probe cache hit; answering last_over_time from RAM"
    );

    let element = InstantVectorElement::new(
        crate::storage_engines::types::KeyByLabelValues::new(),
        sample.value,
    );
    // The instant-vector timestamp is unix milliseconds — match the
    // adapter's expectations downstream (the Prometheus adapter
    // divides by 1000 to render the wire `value: [<unix_seconds>, ...]`).
    let query_result = QueryResult::vector(vec![element], now_ms as u64);
    let execution_result = QueryExecutionResult {
        query_output_labels: KeyByLabelNames::default(),
        query_result,
    };

    let total_duration = start_time.elapsed();
    debug!(
        "freshness-probe response built in {:.2}ms",
        total_duration.as_secs_f64() * 1000.0,
    );

    Some(
        match state
            .adapter
            .format_success_response(&execution_result)
            .await
        {
            Ok(response) => {
                annotate_data_source(response, StorageBackend::SketchStore.data_source_id()).await
            }
            Err(status) => status.into_response(),
        },
    )
}

/// Pull `(metric_name, range_ms)` out of a parsed
/// `last_over_time(<metric>[<range>])` PromQL expression. Returns
/// `None` for any other shape — that's the caller's signal to fall
/// through to the normal dispatch path. Tolerates leading `Paren` /
/// `Unary` wrappers so reasonable spellings parse the same way the
/// gorilla engine's `plan_from_ast` does.
fn parse_last_over_time_probe(query: &str) -> Option<(String, i64)> {
    use promql_parser::parser::{parse, Expr};
    let expr = parse(query).ok()?;
    fn unwrap<'a>(expr: &'a Expr) -> &'a Expr {
        match expr {
            Expr::Paren(p) => unwrap(&p.expr),
            Expr::Unary(u) => unwrap(&u.expr),
            other => other,
        }
    }
    let inner = unwrap(&expr);
    let call = match inner {
        Expr::Call(c) => c,
        _ => return None,
    };
    if !call.func.name.eq_ignore_ascii_case("last_over_time") {
        return None;
    }
    if call.args.args.len() != 1 {
        return None;
    }
    let arg = unwrap(&call.args.args[0]);
    let ms = match arg {
        Expr::MatrixSelector(ms) => ms,
        _ => return None,
    };
    let metric = ms.vs.name.clone()?;
    let range_ms = ms.range.as_millis() as i64;
    Some((metric, range_ms))
}

/// Execute through the query-engine trait and add `data_source: asap_query`
/// to the HTTP response. Trait results use the same label adaptation as
/// router dispatch.
async fn process_via_simple_engine(
    state: &AppState,
    parsed_request: &ParsedQueryRequest,
    start_time: Instant,
    headers: HashMap<String, String>,
) -> Response {
    let query_start_time = Instant::now();
    debug!(
        "About to call query_engine.execute with query='{}' and time={}",
        parsed_request.query, parsed_request.time
    );
    use crate::drivers::query::adapters::QueryExecutionResult;
    use crate::query_engines::routing::query_engine_routing::QueryEngine;
    let evaluation_ms = if parsed_request.time > 0.0 {
        (parsed_request.time * 1_000.0) as u64
    } else {
        unix_time_ms()
    };
    match state
        .query_engine
        .execute_at(&parsed_request.query, evaluation_ms)
        .await
    {
        Ok(query_result) => {
            let query_duration = query_start_time.elapsed();
            debug!(
                "Modern execute() succeeded for query='{}' in {:.2}ms",
                parsed_request.query,
                query_duration.as_secs_f64() * 1000.0
            );
            let execution_result = QueryExecutionResult {
                query_output_labels: asap_types::KeyByLabelNames::default(),
                query_result,
            };
            let total_duration = start_time.elapsed();
            debug!(
                "Total request processing took: {:.2}ms",
                total_duration.as_secs_f64() * 1000.0
            );
            match state
                .adapter
                .format_success_response(&execution_result)
                .await
            {
                Ok(response) => {
                    annotate_data_source(response, StorageBackend::SketchStore.data_source_id())
                        .await
                }
                Err(status) => status.into_response(),
            }
        }
        Err(error) => {
            debug!(
                "Modern execute() returned {error} for query='{}', falling through to fallback / unsupported",
                parsed_request.query,
            );
            let total_duration = start_time.elapsed();
            debug!(
                "Request capability-missed after: {:.2}ms",
                total_duration.as_secs_f64() * 1000.0
            );

            // Step 4: Handle unsupported query using fallback client
            if let Some(fallback) = &state.fallback {
                debug!("Query not supported locally, forwarding to fallback");
                // Fallback client handles the HTTP call and returns formatted response
                match fallback
                    .execute_query_with_headers(parsed_request, headers)
                    .await
                {
                    Ok(response) => response.into_response(),
                    Err(status) => status.into_response(),
                }
            } else {
                record_disabled_forwarding(state, "prometheus", "instant");
                debug!("Query not supported and forwarding disabled, returning error");
                // Adapter formats the unsupported query error for its protocol.
                // We still annotate `data_source: asap_query` so callers
                // see which tier the request was dispatched against —
                // the routing decision happened, the metric just had no
                // compatible aggregation. Mirrors the
                // ASAPQueryEngine-as-router-engine path where a
                // `EngineError::CapabilityMiss` response is still tagged.
                match state.adapter.format_unsupported_query_response().await {
                    Ok(response) => {
                        annotate_data_source(response, StorageBackend::SketchStore.data_source_id())
                            .await
                    }
                    Err(status) => status.into_response(),
                }
            }
        }
    }
}

/// Dispatch directly to the engine registered under `data_source_id`,
/// bypassing the `BackendStorageRouting` lookup. Set by the per-query
/// `X-ASAP-Engine` header (or `?engine=` query param). Used by the
/// accuracy reducer to query the same PromQL against the warm sketch
/// and the Gorilla archive on MinIO so it can compute apples-to-apples
/// relative error per replay row.
///
/// HTTP semantics:
/// * Unknown `data_source_id` → 400 with the list of registered ids.
/// * Engine returns `EngineError::Backend` → 500.
/// * Engine returns `EngineError::CapabilityMiss` → 404.
/// * Engine returns `Ok` → 2xx with `data_source: <id>` info-line.
async fn process_via_named_engine(
    state: &AppState,
    parsed_request: &ParsedQueryRequest,
    start_time: Instant,
    data_source_id: &str,
) -> Response {
    use crate::drivers::query::adapters::QueryExecutionResult;
    use crate::query_engines::EngineError;

    let query_start_time = Instant::now();
    debug!(
        "Dispatching via named engine override: query='{}' data_source_id={}",
        parsed_request.query, data_source_id,
    );

    let Some(engine) = state.query_router.engine_by_id(data_source_id) else {
        let registered: Vec<&'static str> = state.query_router.registered_ids().collect();
        warn!(
            requested = data_source_id,
            registered = ?registered,
            "named engine override: requested engine not registered",
        );
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
            "status": "error",
            "errorType": "bad_data",
            "error": format!(
                "no engine registered under data_source_id={data_source_id:?}; \
                 registered={registered:?}"
            )})),
        )
            .into_response();
    };

    let engine = engine.clone();
    let result = engine.execute(&parsed_request.query).await;

    let total_duration = start_time.elapsed();
    debug!(
        "Named engine dispatch took: {:.2}ms (total req {:.2}ms)",
        query_start_time.elapsed().as_secs_f64() * 1000.0,
        total_duration.as_secs_f64() * 1000.0,
    );

    match result {
        Ok(query_result) => {
            // Trait dispatch loses `KeyByLabelNames`, identical to
            // `process_via_router`. Default to empty so the
            // Prometheus adapter renders `metric: {}` for every
            // returned series.
            let query_output_labels = asap_types::KeyByLabelNames::default();
            let execution_result = QueryExecutionResult {
                query_output_labels,
                query_result,
            };
            // Resolve the `data_source` annotation from the engine's
            // own capabilities so the wire response stays in sync
            // even if the request used a typo'd casing of the id.
            let canonical_id = engine.capabilities().data_source_id;
            match state
                .adapter
                .format_success_response(&execution_result)
                .await
            {
                Ok(response) => annotate_data_source(response, canonical_id).await,
                Err(status) => status.into_response(),
            }
        }
        Err(EngineError::CapabilityMiss { .. }) => {
            warn!(
                data_source_id = data_source_id,
                "named engine: capability miss",
            );
            (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                "status": "error",
                "errorType": "bad_data",
                "error": format!(
                    "engine {data_source_id:?} could not serve this query (capability miss)"
                )})),
            )
                .into_response()
        }
        Err(EngineError::Backend { .. }) => {
            warn!(
                data_source_id = data_source_id,
                "named engine: backend failure",
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "status": "error",
                    "errorType": "internal",
                    "error": format!("engine {data_source_id:?} backend failed")})),
            )
                .into_response()
        }
    }
}

/// Dispatch through the [`EngineRouter`] — used for any metric whose
/// pinned `StorageBackend` is something other than `SketchStore`.
///
/// The router's `execute` API is `(&str, Statistic, AccuracyTarget,
/// StorageBackend) -> QueryResult`. Three of those four axes are
/// pinned by the request:
///
/// * `query` — straight from the parsed request.
/// * `metric_storage` — looked up from the hot-reload `StreamingConfig`
///   by the caller.
///
/// `Statistic` remains a shape-derived follow-up. `AccuracyTarget` is
/// supplied by the caller through [`ACCURACY_HEADER`] and defaults to
/// approximate when the header is absent.
///
/// Map `EngineRouterError` variants to HTTP statuses:
/// * `NoEngineRegistered` → 503 (configuration bug — restart with the
///   right engine plugged in).
/// * `AllFailed { last: CapabilityMiss }` → 404 (no engine in the
///   failover sequence could serve this query shape).
/// * `AllFailed { last: Backend }` → 500 (engines were all eligible
///   but their backends transiently failed).
async fn process_via_router(
    state: &AppState,
    parsed_request: &ParsedQueryRequest,
    start_time: Instant,
    metric_storage: StorageBackend,
    headers: HashMap<String, String>,
) -> Response {
    use crate::drivers::query::adapters::QueryExecutionResult;
    use crate::query_engines::EngineError;

    let query_start_time = Instant::now();
    debug!(
        "Dispatching via EngineRouter: query='{}' metric_storage={:?}",
        parsed_request.query, metric_storage,
    );

    // Statistic is not currently used by the routing policy. Accuracy is
    // the validated request contract threaded in by the HTTP handler.
    let stat = Statistic::Sum;
    let router_result = state
        .query_router
        .execute_routed(&parsed_request.query, stat, metric_storage)
        .await;

    match router_result {
        Ok((query_result, data_source_id)) => {
            let query_duration = query_start_time.elapsed();
            debug!(
                "EngineRouter dispatch took: {:.2}ms; result: {:?}",
                query_duration.as_secs_f64() * 1000.0,
                query_result
            );

            // The Phase-4 Gorilla MVP returns a scalar with empty
            // labels; trait dispatch loses the `KeyByLabelNames` shape
            // ASAPQueryEngine carries. Default to an empty `KeyByLabelNames`
            // — the Prometheus adapter renders an empty `metric: {}`,
            // which is a valid Prometheus shape (every label is just
            // unset) and matches `wrap_result`'s Phase-4 contract.
            let query_output_labels = asap_types::KeyByLabelNames::default();
            let execution_result = QueryExecutionResult {
                query_output_labels,
                query_result,
            };

            let total_duration = start_time.elapsed();
            debug!(
                "Total request processing took: {:.2}ms",
                total_duration.as_secs_f64() * 1000.0
            );
            debug!("=== RETURNING ROUTER SUCCESS RESPONSE ===");

            match state
                .adapter
                .format_success_response(&execution_result)
                .await
            {
                Ok(response) => annotate_data_source(response, data_source_id).await,
                Err(status) => status.into_response(),
            }
        }
        // No engine for the metric's tier is a deploy misconfig, not a
        // query the ASAP tier cannot serve — keep it fail-loud.
        Err(EngineRouterError::NoEngineRegistered { tried, registered }) => {
            warn!(
                tried = ?tried,
                registered = ?registered,
                "EngineRouter: no engine registered for any compatible backend",
            );
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({
                    "status": "error",
                    "errorType": "internal",
                    "error": format!(
                        "no engine registered for any compatible backend; tried {tried:?}, registered={registered:?}"
                    )})),
            )
                .into_response()
        }
        // A CapabilityMiss means the ASAP tier cannot serve this query;
        // forward it rather than answering "no result" (#746 — the archive
        // failover that used to absorb these is gone). A Backend error is a
        // real upstream failure, so it stays a 5xx.
        Err(EngineRouterError::AllFailed { last }) => {
            warn!(error = %last, "EngineRouter: all compatible engines failed");
            match &last {
                EngineError::CapabilityMiss { .. } => {
                    forward_instant_to_fallback(state, parsed_request, headers).await
                }
                EngineError::Backend { .. } => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({
                        "status": "error",
                        "errorType": "internal",
                        "error": last.to_string()})),
                )
                    .into_response(),
            }
        }
    }
}

fn extract_logical_provenance(
    value: &mut serde_json::Value,
) -> Option<Result<(u64, u64, u64, u64, u64, u64, u64), ()>> {
    let warnings = value.get_mut("warnings")?.as_array_mut()?;
    let mut route = None;
    let mut stats = None;
    let mut found = false;
    let mut malformed = false;
    warnings.retain(|warning| {
        let Some(text) = warning.as_str() else {
            return true;
        };
        if let Some(mode) = text.strip_prefix("asap_execution:") {
            found = true;
            if route.replace(mode.to_owned()).is_some() {
                malformed = true;
            }
            return false;
        }
        if let Some(text) = text.strip_prefix("asap_logical_stats:") {
            found = true;
            let values = text.split(',').collect::<Vec<_>>();
            let parse = |index: usize, prefix: &str| {
                values
                    .get(index)
                    .and_then(|v| v.strip_prefix(prefix))
                    .and_then(|v| v.parse::<u64>().ok())
            };
            let parsed = if matches!(values.len(), 3 | 5 | 6 | 7) {
                parse(0, "raw=")
                    .zip(parse(1, "summary="))
                    .zip(parse(2, "memo_hits="))
                    .zip(if values.len() >= 5 {
                        parse(3, "remote=")
                    } else {
                        Some(0)
                    })
                    .zip(if matches!(values.len(), 5 | 7) {
                        parse(4, "index_reads=")
                    } else {
                        Some(0)
                    })
                    .and_then(|((((raw, summary), memo), remote), indexes)| {
                        let rpcs = if values.len() == 6 {
                            parse(4, "remote_rpcs=")?
                        } else if values.len() == 7 {
                            parse(5, "remote_rpcs=")?
                        } else {
                            remote
                        };
                        let branches = if values.len() == 6 {
                            parse(5, "remote_branches=")?
                        } else if values.len() == 7 {
                            parse(6, "remote_branches=")?
                        } else {
                            remote
                        };
                        Some((raw, summary, memo, remote, indexes, rpcs, branches))
                    })
            } else {
                None
            };
            if stats.is_some() || parsed.is_none() {
                malformed = true;
            }
            stats = parsed;
            return false;
        }
        true
    });
    if !found {
        return None;
    }
    let Some((raw, summary, memo, remote, indexes, rpcs, branches)) = stats else {
        return Some(Err(()));
    };
    let expected = if raw > 0 {
        // Retain the field only to reject provenance from obsolete local-raw
        // executors. Installed plans cannot execute such a branch.
        "invalid"
    } else if remote > 0 && summary > 0 {
        "hybrid"
    } else if remote > 0 {
        "exact_dag"
    } else if summary > 0 {
        "asap"
    } else {
        "invalid"
    };
    if malformed || route.as_deref().is_some_and(|mode| mode != expected) {
        Some(Err(()))
    } else {
        Some(Ok((raw, summary, memo, remote, indexes, rpcs, branches)))
    }
}

/// Append a `data_source: <id>` info-line to the response JSON's
/// `infos` array (Prometheus 3.0-style, mirrors the wire-format
/// extension the `GorillaQueryEngine` documents in §6 of
/// `docs/design-gorilla-s3-cold-engine.md`). Best-effort: when the
/// adapter's response isn't a JSON object (or doesn't have an
/// `infos` array shape), this is a no-op and the response passes
/// through unchanged.
async fn annotate_data_source(response: Response, data_source_id: &'static str) -> Response {
    use axum::body::to_bytes;

    let (mut parts, body) = response.into_parts();
    // Adapter responses are bounded JSON objects; cap at 16 MiB to
    // bracket pathological cases without blowing memory.
    let bytes = match to_bytes(body, 16 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            warn!("annotate_data_source: failed to read response body: {e}");
            return Response::from_parts(parts, axum::body::Body::empty());
        }
    };
    let mut value: serde_json::Value = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(_) => {
            // Not a JSON body — pass through. Reconstruct the body
            // from the buffered bytes so the caller still sees the
            // original payload.
            return Response::from_parts(parts, axum::body::Body::from(bytes));
        }
    };
    if data_source_id == "asap_query"
        && (!parts.status.is_success()
            || value.get("status").and_then(serde_json::Value::as_str) != Some("success")
            || value.get("data").is_none_or(serde_json::Value::is_null))
    {
        // Dispatch to the local engine is not a successful warm execution.
        return Response::from_parts(parts, axum::body::Body::from(bytes));
    }
    if data_source_id == "asap_query" {
        if let Some(provenance) = extract_logical_provenance(&mut value) {
            let (route, detail) = match provenance {
                Ok((raw, summary, memo, remote, _legacy_indexes, rpcs, branches)) => {
                    for (name, count) in [
                        ("x-asap-raw-scan-evaluations", raw),
                        ("x-asap-summary-readout-evaluations", summary),
                        ("x-asap-memo-hits", memo),
                        ("x-asap-exact-subquery-evaluations", remote),
                        ("x-asap-exact-subquery-rpcs", rpcs),
                        ("x-asap-exact-branch-evaluations", branches),
                    ] {
                        parts.headers.insert(
                            name,
                            axum::http::HeaderValue::from_str(&count.to_string()).unwrap(),
                        );
                    }
                    if raw > 0 {
                        ("failed", "invalid_provenance")
                    } else if remote > 0 && summary > 0 {
                        ("hybrid", "hybrid")
                    } else if remote > 0 || summary == 0 {
                        (
                            "exact_fallback",
                            if remote > 0 {
                                "external_exact"
                            } else {
                                "invalid_provenance"
                            },
                        )
                    } else {
                        ("warm", "asap")
                    }
                }
                Err(()) => ("failed", "invalid_provenance"),
            };
            parts.headers.insert(
                "x-asap-execution",
                axum::http::HeaderValue::from_static(route),
            );
            parts.headers.insert(
                "x-asap-execution-detail",
                axum::http::HeaderValue::from_static(detail),
            );
            if let Some(map) = value.as_object_mut() {
                if let Some(infos) = map
                    .entry("infos")
                    .or_insert_with(|| serde_json::json!([]))
                    .as_array_mut()
                {
                    infos.push(serde_json::json!(format!("execution: {detail}")));
                }
            }
        }
    }
    // The body changed; an adapter's original Content-Length is no longer valid.
    parts.headers.remove(axum::http::header::CONTENT_LENGTH);
    if let serde_json::Value::Object(map) = &mut value {
        let infos_entry = map
            .entry("infos".to_string())
            .or_insert_with(|| serde_json::Value::Array(Vec::new()));
        if let serde_json::Value::Array(arr) = infos_entry {
            arr.push(serde_json::Value::String(format!(
                "data_source: {data_source_id}"
            )));
        }
    }
    let serialized = match serde_json::to_vec(&value) {
        Ok(v) => v,
        Err(e) => {
            warn!("annotate_data_source: failed to re-serialize: {e}");
            return Response::from_parts(parts, axum::body::Body::from(bytes));
        }
    };
    Response::from_parts(parts, axum::body::Body::from(serialized))
}

fn vm_tenant_headers(tenant: String, mut headers: HeaderMap) -> Result<HeaderMap, Response> {
    let value = axum::http::HeaderValue::from_str(&tenant)
        .map_err(|_| (StatusCode::BAD_REQUEST, "invalid VictoriaMetrics tenant").into_response())?;
    headers.insert(TENANT_HEADER, value);
    Ok(headers)
}

async fn handle_vm_cluster_instant(
    Path(tenant): Path<String>,
    query: Query<HashMap<String, String>>,
    headers: HeaderMap,
    state: State<AppState>,
) -> Response {
    let Ok(headers) = vm_tenant_headers(tenant, headers) else {
        return (StatusCode::BAD_REQUEST, "invalid VictoriaMetrics tenant").into_response();
    };
    handle_instant_query(query, headers, state).await
}

async fn handle_vm_cluster_instant_post(
    Path(tenant): Path<String>,
    state: State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Ok(headers) = vm_tenant_headers(tenant, headers) else {
        return (StatusCode::BAD_REQUEST, "invalid VictoriaMetrics tenant").into_response();
    };
    handle_instant_query_post(state, headers, body).await
}

async fn handle_vm_cluster_range(
    Path(tenant): Path<String>,
    query: Query<HashMap<String, String>>,
    headers: HeaderMap,
    state: State<AppState>,
) -> Response {
    let Ok(headers) = vm_tenant_headers(tenant, headers) else {
        return (StatusCode::BAD_REQUEST, "invalid VictoriaMetrics tenant").into_response();
    };
    handle_range_query(query, headers, state).await
}

async fn handle_vm_cluster_range_post(
    Path(tenant): Path<String>,
    state: State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Ok(headers) = vm_tenant_headers(tenant, headers) else {
        return (StatusCode::BAD_REQUEST, "invalid VictoriaMetrics tenant").into_response();
    };
    handle_range_query_post(state, headers, body).await
}

async fn handle_instant_query(
    query_params: Query<HashMap<String, String>>,
    headers: axum::http::HeaderMap,
    State(state): State<AppState>,
) -> Response {
    let _timer = srv_metrics::start_query_timer(srv_metrics::QUERY_TYPE_INSTANT);
    let start_time = Instant::now();
    debug!("=== INCOMING GET REQUEST ===");
    debug!("Raw query params: {:?}", query_params.0);

    // Phase-6 Fix 1: per-query engine override. Header takes
    // precedence over query param; either flips the dispatcher to
    // `process_via_named_engine`. Recorded before `parse_get_request`
    // strips the param out (it doesn't, but reading the source of
    // truth keeps this robust to adapter changes).
    let engine_override = extract_engine_override(&headers, &query_params.0);
    let accuracy = match extract_accuracy(&headers) {
        Ok(accuracy) => accuracy,
        Err(error) => return invalid_accuracy_response(error),
    };
    // Per-tenant `BackendStorageRouting`: read the tenant id from the
    // `X-ASAP-Tenant` header (default `"default"`). Captured before
    // `parse_get_request` consumes `query_params`.
    let tenant = extract_tenant(&headers);
    let forwarding_headers = extract_fallback_headers(&headers);

    let parsed_request = match state.adapter.parse_get_request(query_params).await {
        Ok(req) => {
            debug!(
                "Successfully parsed - query: '{}', time: {}",
                req.query, req.time
            );
            req
        }
        Err(parse_error) => {
            debug!("Failed to parse request: {:?}", parse_error);
            srv_metrics::record_query_outcome(
                srv_metrics::QUERY_TYPE_INSTANT,
                srv_metrics::QUERY_STATUS_ERROR,
            );
            return match state.adapter.format_error_response(&parse_error).await {
                Ok(json) => json.into_response(),
                Err(status) => status.into_response(),
            };
        }
    };

    let response = process_query_request(
        &state,
        &parsed_request,
        start_time,
        forwarding_headers,
        engine_override,
        accuracy,
        &tenant,
    )
    .await;
    srv_metrics::record_query_outcome(
        srv_metrics::QUERY_TYPE_INSTANT,
        query_status_label(&response),
    );
    response
}

/// Extract the per-query engine override (Phase-6 Fix 1).
///
/// Precedence: the `X-ASAP-Engine` header wins over the `?engine=`
/// query parameter when both are present. Returns `None` (default
/// dispatch via `BackendStorageRouting`) when neither is set.
fn extract_engine_override(
    headers: &axum::http::HeaderMap,
    query_params: &HashMap<String, String>,
) -> Option<String> {
    if let Some(v) = headers.get(ENGINE_OVERRIDE_HEADER) {
        if let Ok(s) = v.to_str() {
            let trimmed = s.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    query_params
        .get(ENGINE_OVERRIDE_QUERY_PARAM)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

async fn handle_instant_query_post(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Response {
    let _timer = srv_metrics::start_query_timer(srv_metrics::QUERY_TYPE_INSTANT);
    let start_time = Instant::now();
    debug!("=== INCOMING POST REQUEST ===");

    let forwarding_headers = extract_fallback_headers(&headers);
    let accuracy = match extract_accuracy(&headers) {
        Ok(accuracy) => accuracy,
        Err(error) => return invalid_accuracy_response(error),
    };

    // Check content type to determine how to parse the body
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    debug!("Content-Type: {}", content_type);

    // Phase-6 Fix 1: per-query engine override read from the header
    // before parsing the body. We don't yet know the form-decoded
    // params, so query-param fallback (rare for POST) is wired in
    // below after the form parse.
    let mut engine_override: Option<String> = headers
        .get(ENGINE_OVERRIDE_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    // Per-tenant `BackendStorageRouting` (POST path): read the
    // tenant id from the `X-ASAP-Tenant` header (default
    // `"default"`).
    let tenant = extract_tenant(&headers);

    let parsed_request = if content_type.contains("application/json") {
        // Handle JSON POST
        debug!("Parsing as JSON POST request");
        match state.adapter.parse_json_post_request(body).await {
            Ok(req) => {
                debug!(
                    "Successfully parsed JSON POST - query: '{}', time: {}",
                    req.query, req.time
                );
                req
            }
            Err(parse_error) => {
                debug!("Failed to parse JSON POST request: {:?}", parse_error);
                srv_metrics::record_query_outcome(
                    srv_metrics::QUERY_TYPE_INSTANT,
                    srv_metrics::QUERY_STATUS_ERROR,
                );
                return match state.adapter.format_error_response(&parse_error).await {
                    Ok(json) => json.into_response(),
                    Err(status) => status.into_response(),
                };
            }
        }
    } else {
        // Handle form-encoded POST
        debug!("Parsing as form-encoded POST request");

        // Parse the body as form data
        let body_str = match String::from_utf8(body.to_vec()) {
            Ok(s) => s,
            Err(e) => {
                debug!("Failed to parse body as UTF-8: {}", e);
                use crate::drivers::query::adapters::AdapterError;
                srv_metrics::record_query_outcome(
                    srv_metrics::QUERY_TYPE_INSTANT,
                    srv_metrics::QUERY_STATUS_ERROR,
                );
                return match state
                    .adapter
                    .format_error_response(&AdapterError::ParseError(format!(
                        "Invalid UTF-8 in request body: {}",
                        e
                    )))
                    .await
                {
                    Ok(json) => json.into_response(),
                    Err(status) => status.into_response(),
                };
            }
        };

        // Parse form parameters
        let params: HashMap<String, String> = form_urlencoded::parse(body_str.as_bytes())
            .into_owned()
            .collect();
        debug!("Form params extracted: {:?}", params);

        // Phase-6 Fix 1: form-body fallback for the engine override
        // (header still wins). Lets a curl POST send `engine=archive`
        // alongside `query=` and `time=`.
        if engine_override.is_none() {
            engine_override = params
                .get(ENGINE_OVERRIDE_QUERY_PARAM)
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty());
        }

        // Use adapter to parse POST request (handles form-encoded parameters)
        match state.adapter.parse_post_request(Form(params)).await {
            Ok(req) => {
                debug!(
                    "Successfully parsed POST - query: '{}', time: {}",
                    req.query, req.time
                );
                req
            }
            Err(parse_error) => {
                debug!("Failed to parse POST request: {:?}", parse_error);
                srv_metrics::record_query_outcome(
                    srv_metrics::QUERY_TYPE_INSTANT,
                    srv_metrics::QUERY_STATUS_ERROR,
                );
                return match state.adapter.format_error_response(&parse_error).await {
                    Ok(json) => json.into_response(),
                    Err(status) => status.into_response(),
                };
            }
        }
    };

    let result = process_query_request(
        &state,
        &parsed_request,
        start_time,
        forwarding_headers,
        engine_override,
        accuracy,
        &tenant,
    )
    .await;

    let total_duration = start_time.elapsed();
    debug!(
        "Total POST request processing took: {:.2}ms",
        total_duration.as_secs_f64() * 1000.0
    );

    srv_metrics::record_query_outcome(srv_metrics::QUERY_TYPE_INSTANT, query_status_label(&result));
    result
}

async fn handle_runtime_info(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> Result<Json<Value>, StatusCode> {
    debug!("Delegating runtime info request to adapter");

    // Extract headers we want to forward
    let mut forwarding_headers = HashMap::new();
    if let Some(auth) = headers.get(axum::http::header::AUTHORIZATION) {
        if let Ok(auth_str) = auth.to_str() {
            debug!("Found Authorization header for runtime info: {}", auth_str);
            forwarding_headers.insert("Authorization".to_string(), auth_str.to_string());
        }
    } else {
        debug!("No Authorization header found in runtime info request");
    }

    // Delegate to adapter for protocol-specific handling
    state
        .adapter
        .handle_runtime_info_with_headers(state.summary_store.clone(), forwarding_headers)
        .await
}

// Map a finished Response to an `asap_query_requests_total` status
// label. We distinguish 5xx (internal) errors from 4xx (client / parse)
// errors — both count as `error`, and 2xx / 3xx count as `ok`. A more
// granular split into `unsupported` / `fallback` would require the
// handlers to thread the outcome back out; the Response status is the
// tractable signal we have today.
fn query_status_label(response: &Response) -> &'static str {
    if response.status().is_success() || response.status().is_redirection() {
        srv_metrics::QUERY_STATUS_OK
    } else {
        srv_metrics::QUERY_STATUS_ERROR
    }
}

fn record_disabled_forwarding(state: &AppState, backend: &str, path: &str) {
    if state.config.adapter_config.fallback_blocked_by_policy
        && !state
            .config
            .adapter_config
            .query_forwarding_policy
            .allows_external_queries()
    {
        debug!(
            backend,
            path, "query forwarding disabled; external query blocked"
        );
        srv_metrics::record_query_forwarding_blocked(backend, path);
    }
}

// ============================================================
// Metrics Handler
// ============================================================

async fn handle_metrics(State(state): State<AppState>) -> impl IntoResponse {
    let encoder = prometheus::TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    prometheus::Encoder::encode(&encoder, &metric_families, &mut buffer)
        .unwrap_or_else(|e| tracing::error!("Failed to encode metrics: {}", e));
    let (populations, builds) = state
        .summary_store
        .current_series
        .lock()
        .expect("current-series state poisoned")
        .stats();
    buffer.extend_from_slice(format!(
        "# TYPE asap_current_series_populations gauge\nasap_current_series_populations {populations}\n# TYPE asap_current_series_cache_builds_total counter\nasap_current_series_cache_builds_total {builds}\n"
    ).as_bytes());
    if let Some(receiver) = state.remote_write.as_ref() {
        use std::sync::atomic::Ordering;
        let stats = receiver.stats();
        let extra = format!(
            "# TYPE asap_remote_write_requests_total counter\n\
             asap_remote_write_requests_total {}\n\
             # TYPE asap_remote_write_samples_total counter\n\
             asap_remote_write_samples_total {}\n\
             # TYPE asap_remote_write_stale_markers_total counter\n\
             asap_remote_write_stale_markers_total {}\n\
             # TYPE asap_remote_write_duplicates_total counter\n\
             asap_remote_write_duplicates_total {}\n\
             # TYPE asap_remote_write_rejected_requests_total counter\n\
             asap_remote_write_rejected_requests_total {}\n\
             # TYPE asap_remote_write_bytes_total counter\n\
             asap_remote_write_bytes_total {}\n",
            stats.requests.load(Ordering::Relaxed),
            stats.samples.load(Ordering::Relaxed),
            stats.stale_markers.load(Ordering::Relaxed),
            stats.duplicates.load(Ordering::Relaxed),
            stats.rejected_requests.load(Ordering::Relaxed),
            stats.bytes.load(Ordering::Relaxed),
        );
        buffer.extend_from_slice(extra.as_bytes());
    }
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        buffer,
    )
}

// ============================================================
// Range Query Handlers
// ============================================================

/// Core range query execution logic shared between GET and POST handlers
async fn process_range_query_request(
    state: &AppState,
    parsed_request: &ParsedRangeQueryRequest,
    start_time: Instant,
    forwarding_headers: HashMap<String, String>,
    accuracy: AccuracyTarget,
    tenant: &str,
) -> Response {
    // Check if handling is enabled
    if !state.config.handle_http_requests {
        debug!("HTTP request handling is disabled for range query");
        // For now, return error - fallback for range queries can be added later
        use crate::drivers::query::adapters::AdapterError;
        return match state
            .adapter
            .format_error_response(&AdapterError::ProtocolError(
                "Range query handling is disabled".to_string(),
            ))
            .await
        {
            Ok(json) => json.into_response(),
            Err(status) => status.into_response(),
        };
    }

    // Execute range query with engine
    let query_start_time = Instant::now();
    debug!(
        "Executing range query: '{}' from {} to {} step {}",
        parsed_request.query, parsed_request.start, parsed_request.end, parsed_request.step
    );

    // ASAP-first centralization refactor — the range path is now a
    // thin transport layer. Engine selection lives in
    // `EngineRouter::execute_range`, which walks the shared
    // `compatible_storage_backends` policy table. Since #746 that table
    // has one ASAP-managed answer (the sketch tier); a CapabilityMiss
    // forwards to the Prometheus fallback rather than to another tier.
    let start_ms = (parsed_request.start * 1000.0) as u64;
    let end_ms = (parsed_request.end * 1000.0) as u64;
    let step_ms = (parsed_request.step * 1000.0) as u64;

    let language_identity = state.adapter.canonical_plan_identity(&parsed_request.query);
    if state.adapter.query_language() == asap_types::QueryLanguage::MetricsQl
        && language_identity.is_err()
    {
        if let Some(fallback) = &state.fallback {
            return match fallback
                .execute_range_query_with_headers(parsed_request, forwarding_headers)
                .await
            {
                Ok(response) => response.into_response(),
                Err(status) => status.into_response(),
            };
        }
    }
    if let Ok(Some(identity)) = language_identity {
        if let Ok(result) = state
            .query_engine
            .execute_metricsql_range(&identity, start_ms, end_ms, step_ms)
            .await
        {
            return match state
                .adapter
                .format_range_success_response(&result, &asap_types::KeyByLabelNames::default())
                .await
            {
                Ok(response) => {
                    annotate_data_source(response, StorageBackend::SketchStore.data_source_id())
                        .await
                }
                Err(status) => status.into_response(),
            };
        }
        if let Some(fallback) = &state.fallback {
            return match fallback
                .execute_range_query_with_headers(parsed_request, forwarding_headers)
                .await
            {
                Ok(response) => response.into_response(),
                Err(status) => status.into_response(),
            };
        }
    }

    // `X-ASAP-Accuracy: exact` means "do not answer from the ε/δ-bounded
    // sketches" — with no archive tier left (#746) that is the Prometheus
    // fallback's job.
    if matches!(accuracy, AccuracyTarget::Exact) {
        debug!("Exact accuracy requested on range query — bypassing the sketch tier");
        return forward_range_to_fallback(state, parsed_request, forwarding_headers).await;
    }

    let metric_storage = resolve_metric_storage(state, &parsed_request.query, tenant);

    // Statistic is not currently used by the routing policy.
    let stat = Statistic::Sum;
    debug!(
        "Dispatching range query via EngineRouter: query='{}' metric_storage={:?} stat={:?}",
        parsed_request.query, metric_storage, stat,
    );

    let router_result = state
        .query_router
        .execute_range_routed(
            &parsed_request.query,
            stat,
            metric_storage,
            start_ms,
            end_ms,
            step_ms,
        )
        .await;

    match router_result {
        Ok((query_result, data_source_id)) => {
            let query_duration = query_start_time.elapsed();
            debug!(
                "EngineRouter range dispatch took: {:.2}ms",
                query_duration.as_secs_f64() * 1000.0
            );
            let total_duration = start_time.elapsed();
            debug!(
                "Total range query processing took: {:.2}ms",
                total_duration.as_secs_f64() * 1000.0
            );
            match state
                .adapter
                .format_range_success_response(
                    &query_result,
                    &asap_types::KeyByLabelNames::default(),
                )
                .await
            {
                Ok(response) => {
                    annotate_data_source(response.into_response(), data_source_id).await
                }
                Err(status) => status.into_response(),
            }
        }
        Err(EngineRouterError::NoEngineRegistered { tried, registered }) => {
            warn!(
                tried = ?tried,
                registered = ?registered,
                "EngineRouter (range): no engine registered for any compatible backend",
            );
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({
                    "status": "error",
                    "errorType": "internal",
                    "error": format!(
                        "no engine registered for any compatible backend; tried {tried:?}, registered={registered:?}"
                    )})),
            )
                .into_response()
        }
        Err(EngineRouterError::AllFailed { last }) => {
            use crate::query_engines::EngineError;
            warn!(error = %last, "EngineRouter (range): all compatible engines failed");
            // A terminal CapabilityMiss means no tier could serve the
            // range query — surface the adapter's "unsupported query"
            // response (the wire shape browsers / dashboards expect),
            // matching the pre-refactor fall-through. A Backend error
            // is a real upstream failure → 5xx.
            match &last {
                EngineError::CapabilityMiss { .. } => {
                    debug!(
                        "Range query CapabilityMiss across all tiers for query='{}', \
                         falling through to unsupported",
                        parsed_request.query
                    );
                    if let Some(fallback) = &state.fallback {
                        match fallback
                            .execute_range_query_with_headers(parsed_request, forwarding_headers)
                            .await
                        {
                            Ok(response) => response.into_response(),
                            Err(status) => status.into_response(),
                        }
                    } else {
                        record_disabled_forwarding(state, "prometheus", "range");
                        match state.adapter.format_unsupported_query_response().await {
                            Ok(json) => json.into_response(),
                            Err(status) => status.into_response(),
                        }
                    }
                }
                EngineError::Backend { .. } => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({
                        "status": "error",
                        "errorType": "internal",
                        "error": last.to_string()})),
                )
                    .into_response(),
            }
        }
    }
}

async fn handle_range_query(
    query_params: Query<HashMap<String, String>>,
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Response {
    let _timer = srv_metrics::start_query_timer(srv_metrics::QUERY_TYPE_RANGE);
    let start_time = Instant::now();
    debug!("=== INCOMING RANGE QUERY GET REQUEST ===");
    debug!("Raw query params: {:?}", query_params.0);

    let accuracy = match extract_accuracy(&headers) {
        Ok(accuracy) => accuracy,
        Err(error) => return invalid_accuracy_response(error),
    };
    let parsed_request = match state.adapter.parse_range_get_request(query_params).await {
        Ok(req) => {
            debug!(
                "Successfully parsed range query - query: '{}', start: {}, end: {}, step: {}",
                req.query, req.start, req.end, req.step
            );
            req
        }
        Err(parse_error) => {
            debug!("Failed to parse range query request: {:?}", parse_error);
            srv_metrics::record_query_outcome(
                srv_metrics::QUERY_TYPE_RANGE,
                srv_metrics::QUERY_STATUS_ERROR,
            );
            return match state.adapter.format_error_response(&parse_error).await {
                Ok(json) => json.into_response(),
                Err(status) => status.into_response(),
            };
        }
    };
    let tenant = extract_tenant(&headers);

    let response = process_range_query_request(
        &state,
        &parsed_request,
        start_time,
        extract_fallback_headers(&headers),
        accuracy,
        &tenant,
    )
    .await;
    srv_metrics::record_query_outcome(srv_metrics::QUERY_TYPE_RANGE, query_status_label(&response));
    response
}

async fn handle_range_query_post(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let _timer = srv_metrics::start_query_timer(srv_metrics::QUERY_TYPE_RANGE);
    let start_time = Instant::now();
    debug!("=== INCOMING RANGE QUERY POST REQUEST ===");
    let accuracy = match extract_accuracy(&headers) {
        Ok(accuracy) => accuracy,
        Err(error) => return invalid_accuracy_response(error),
    };

    // Parse the body as form data
    let body_str = match String::from_utf8(body.to_vec()) {
        Ok(s) => s,
        Err(e) => {
            debug!("Failed to parse body as UTF-8: {}", e);
            use crate::drivers::query::adapters::AdapterError;
            srv_metrics::record_query_outcome(
                srv_metrics::QUERY_TYPE_RANGE,
                srv_metrics::QUERY_STATUS_ERROR,
            );
            return match state
                .adapter
                .format_error_response(&AdapterError::ParseError(format!(
                    "Invalid UTF-8 in request body: {}",
                    e
                )))
                .await
            {
                Ok(json) => json.into_response(),
                Err(status) => status.into_response(),
            };
        }
    };

    // Parse form parameters
    let params: HashMap<String, String> = form_urlencoded::parse(body_str.as_bytes())
        .into_owned()
        .collect();
    debug!("Form params extracted: {:?}", params);

    let parsed_request = match state.adapter.parse_range_post_request(Form(params)).await {
        Ok(req) => {
            debug!(
                "Successfully parsed range POST - query: '{}', start: {}, end: {}, step: {}",
                req.query, req.start, req.end, req.step
            );
            req
        }
        Err(parse_error) => {
            debug!("Failed to parse range POST request: {:?}", parse_error);
            srv_metrics::record_query_outcome(
                srv_metrics::QUERY_TYPE_RANGE,
                srv_metrics::QUERY_STATUS_ERROR,
            );
            return match state.adapter.format_error_response(&parse_error).await {
                Ok(json) => json.into_response(),
                Err(status) => status.into_response(),
            };
        }
    };
    let tenant = extract_tenant(&headers);

    let response = process_range_query_request(
        &state,
        &parsed_request,
        start_time,
        extract_fallback_headers(&headers),
        accuracy,
        &tenant,
    )
    .await;
    srv_metrics::record_query_outcome(srv_metrics::QUERY_TYPE_RANGE, query_status_label(&response));
    response
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn no_result_error_is_not_annotated_as_warm() {
        use axum::response::IntoResponse;
        for (status, body) in [
            (
                StatusCode::OK,
                serde_json::json!({"status":"error","data":null,
                "errorType":"bad_data","error":"No result for query"}),
            ),
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                serde_json::json!({"status":"success","data":{"result":[]}}),
            ),
            (StatusCode::OK, serde_json::json!({"status":"success"})),
            (
                StatusCode::OK,
                serde_json::json!({"status":"success","data":null}),
            ),
        ] {
            let response = super::annotate_data_source(
                (status, axum::Json(body.clone())).into_response(),
                "asap_query",
            )
            .await;
            assert!(!response.headers().contains_key("x-asap-execution"));
            let bytes = axum::body::to_bytes(response.into_body(), 4096)
                .await
                .unwrap();
            let actual: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert!(actual.get("infos").is_none());
            assert_eq!(actual, body);
        }
    }
    use super::*;
    use crate::drivers::ingest::prometheus_remote_write::{
        Label, PrometheusRemoteWriteConfig, PrometheusRemoteWriteReceiver, Sample, TimeSeries,
        WriteRequest,
    };
    use crate::precompute_engine::ingest_handler::{IngestObservability, IngestState};
    use crate::precompute_engine::series_router::SeriesRouter;
    use crate::query_engines::ASAPQueryEngine;
    use crate::storage_engines::types::{StreamingConfig, StreamingConfigHandle};
    use prost::Message;
    use reqwest::Client;
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    async fn setup_test_server() -> u16 {
        setup_test_server_with_hot_reload(None).await
    }

    async fn setup_remote_write_test_server() -> (u16, PrometheusRemoteWriteReceiver) {
        use asap_types::producer_plan::{FrameIdentityContract, SequenceScope, TransmissionPlan};
        use control_plane::physical::compiler::{
            IngestContract, IngestProtocol, PlanEnvelope, PrecomputePlan, TimestampUnit,
            PLANNER_REVISION,
        };
        let streaming_config = Arc::new(StreamingConfig::default());
        let envelope = PlanEnvelope {
            plan_id: 7,
            plan_version: 1,
            generated_at_unix_ms: 0,
            activation_unix_ms: 1,
            expiry_unix_ms: None,
            backend_compat: asap_types::precompute_plan::BACKEND_COMPAT.into(),
            planner_revision: PLANNER_REVISION.into(),
            capability_snapshot_id: "test".into(),
        };
        let catalog = Arc::new(
            asap_types::summary_catalog::SummaryCatalog::from_materializations(7, 1, &[]).unwrap(),
        );
        let generation = catalog.reference().unwrap();
        let summary_store = Arc::new(crate::storage_engines::sketch_db::index::SketchStore::new());
        summary_store
            .install_summary_catalog(Arc::clone(&catalog))
            .unwrap();
        let active = crate::storage_engines::types::ActivePhysicalPlanHandle::new(
            crate::storage_engines::types::RuntimePhysicalPlan {
                envelope: envelope.clone(),
                summary_catalog: Some(Arc::clone(&catalog)),
                precompute_plan: PrecomputePlan {
                    summary_catalog: Some(generation.clone()),
                    envelope: envelope.clone(),
                    ingest: IngestContract {
                        protocol: IngestProtocol::PrometheusRemoteWriteV1,
                        endpoint_path: "/api/v1/write".into(),
                        timestamp_unit: TimestampUnit::UnixMilliseconds,
                        require_plan_identity: false,
                        require_summary_definition_identity: false,
                        require_registered_producer: false,
                    },
                    schemas: Vec::new(),
                    producers: Vec::new(),
                    executable_dags: Default::default(),
                    materializations: Vec::new(),
                },
                transmission_plan: TransmissionPlan {
                    summary_catalog: Some(generation),
                    envelope,
                    frame_identity: FrameIdentityContract {
                        identity_version: 1,
                        sequence_scope: SequenceScope::MaterializationSeriesProducerEpoch,
                        require_checkpoint_for_full: true,
                        require_base_checkpoint_for_delta: true,
                    },
                    rules: Vec::new(),
                },
                streaming_config: streaming_config.clone(),
                query_plan: Arc::new(asap_types::query_plan::QueryPlan {
                    plan_id: 7,
                    plan_version: 1,
                    clickhouse_context: None,
                    entries: Default::default(),
                }),
                storage_routing: Arc::new(
                    crate::storage_engines::types::BackendStorageRouting::empty(),
                ),
            },
        );
        let hot_reload = StreamingConfigHandle::from_active_physical_plan(active.clone());
        let (sender, _worker) = mpsc::channel(8);
        let ingest = Arc::new(IngestState {
            router: SeriesRouter::new(vec![sender]),
            samples_ingested: AtomicU64::new(0),
            samples_blocked_by_schema_barrier: AtomicU64::new(0),
            hot_reload_config: hot_reload,
            pass_raw_samples: false,
            sketch_snapshots: dashmap::DashMap::new(),
            series_resolver: Arc::new(crate::drivers::ingest::SeriesIdResolver::new()),
            summary_store: Arc::clone(&summary_store),
            observability: IngestObservability::default(),
        });
        let receiver =
            PrometheusRemoteWriteReceiver::new(PrometheusRemoteWriteConfig::default(), ingest);
        let adapter_config = AdapterConfig::prometheus_promql("http://127.0.0.1:9".into(), false);
        let server = HttpServer::new(
            HttpServerConfig {
                port: 0,
                handle_http_requests: true,
                adapter_config,
            },
            Arc::new(ASAPQueryEngine::new(15_000)),
            summary_store,
        )
        .with_active_physical_plan(active)
        .with_remote_write(receiver.clone());
        (server.start_test_server().await.unwrap(), receiver)
    }

    #[tokio::test]
    async fn remote_write_http_contract_accepts_v1_and_rejects_wrong_encoding() {
        let (port, receiver) = setup_remote_write_test_server().await;
        let request = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![Label {
                    name: "__name__".into(),
                    value: "up".into(),
                }],
                samples: vec![Sample {
                    value: 1.0,
                    timestamp: 123,
                }],
                exemplars: Vec::new(),
                histograms: Vec::new(),
            }],
        };
        let body = snap::raw::Encoder::new()
            .compress_vec(&request.encode_to_vec())
            .unwrap();
        let client = Client::new();
        let accepted = client
            .post(format!("http://127.0.0.1:{port}/api/v1/write"))
            .header("content-encoding", "snappy")
            .header("content-type", "application/x-protobuf")
            .header("x-prometheus-remote-write-version", "0.1.0")
            .body(body.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(accepted.status().as_u16(), StatusCode::NO_CONTENT.as_u16());
        let ready = client
            .get(format!("http://127.0.0.1:{port}/api/v1/health"))
            .send()
            .await
            .unwrap();
        assert_eq!(ready.status().as_u16(), StatusCode::OK.as_u16());
        assert_eq!(
            receiver
                .stats()
                .samples
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );

        let rejected = client
            .post(format!("http://127.0.0.1:{port}/api/v1/write"))
            .header("content-encoding", "gzip")
            .header("content-type", "application/x-protobuf")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(
            rejected.status().as_u16(),
            StatusCode::UNSUPPORTED_MEDIA_TYPE.as_u16()
        );
    }

    async fn setup_test_server_with_hot_reload(hot_reload: Option<StreamingConfigHandle>) -> u16 {
        let adapter_config = AdapterConfig::prometheus_promql(
            "http://127.0.0.1:9999".to_string(), // Unused for this test
            false,                               // forward_unsupported_queries
        );

        let config = HttpServerConfig {
            port: 0,
            handle_http_requests: true,
            adapter_config,
        };

        let streaming_config = Arc::new(StreamingConfig::default());
        let query_engine = Arc::new(ASAPQueryEngine::new(15000));

        let mut server = HttpServer::new(
            config,
            query_engine,
            Arc::new(crate::storage_engines::sketch_db::index::SketchStore::new()),
        );
        if let Some(handle) = hot_reload {
            server = server.with_hot_reload_config(handle);
        }
        server
            .start_test_server()
            .await
            .expect("Failed to start test server")
    }

    async fn setup_victoriametrics_test_server(fallback_url: String) -> u16 {
        use crate::drivers::query::adapters::VictoriaMetricsHttpAdapter;
        let adapter_config = AdapterConfig::victoriametrics_metricsql(fallback_url);
        let server = HttpServer::new(
            HttpServerConfig {
                port: 0,
                handle_http_requests: true,
                adapter_config: adapter_config.clone(),
            },
            Arc::new(ASAPQueryEngine::new(15_000)),
            Arc::new(crate::storage_engines::sketch_db::index::SketchStore::new()),
        )
        .with_protocol_adapter(Arc::new(VictoriaMetricsHttpAdapter::new(adapter_config)));
        server.start_test_server().await.unwrap()
    }

    #[tokio::test]
    async fn victoriametrics_cluster_routes_are_registered() {
        let port = setup_victoriametrics_test_server("http://127.0.0.1:9999".into()).await;
        let response = Client::new()
            .get(format!(
                "http://127.0.0.1:{port}/select/42/prometheus/api/v1/query"
            ))
            .query(&[("query", "default_rollup(cpu[5m])"), ("time", "1")])
            .send()
            .await
            .unwrap();
        assert_ne!(response.status(), reqwest::StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn promql_exact_request_forwards_to_prometheus() {
        let upstream = Router::new().route(
            "/api/v1/query",
            get(|Query(params): Query<HashMap<String, String>>| async move {
                Json(serde_json::json!({
                    "status": "success",
                    "data": {"received": params["query"]}
                }))
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });
        let config = AdapterConfig::prometheus_promql(format!("http://{address}"), true);
        let server = HttpServer::new(
            HttpServerConfig {
                port: 0,
                handle_http_requests: true,
                adapter_config: config,
            },
            Arc::new(ASAPQueryEngine::new(15_000)),
            Arc::new(crate::storage_engines::sketch_db::index::SketchStore::new()),
        );
        let port = server.start_test_server().await.unwrap();
        let response = Client::new()
            .get(format!("http://127.0.0.1:{port}/api/v1/query"))
            .header(ACCURACY_HEADER, "exact")
            .query(&[("query", "sum_over_time(foo[5m])"), ("time", "1")])
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let payload: serde_json::Value = response.json().await.unwrap();
        assert_eq!(payload["data"]["received"], "sum_over_time(foo[5m])");
    }

    #[tokio::test]
    async fn metricsql_exact_request_forwards_to_victoriametrics() {
        let upstream = Router::new().route(
            "/api/v1/query",
            get(|Query(params): Query<HashMap<String, String>>| async move {
                Json(serde_json::json!({
                    "status": "success",
                    "data": {"received": params["query"]}
                }))
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });
        let port = setup_victoriametrics_test_server(format!("http://{address}")).await;
        let response = Client::new()
            .get(format!("http://127.0.0.1:{port}/api/v1/query"))
            .header(ACCURACY_HEADER, "exact")
            .query(&[
                ("query", "rate(requests[5m]) keep_metric_names"),
                ("time", "1"),
            ])
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let payload: serde_json::Value = response.json().await.unwrap();
        assert_eq!(
            payload["data"]["received"],
            "rate(requests[5m]) keep_metric_names"
        );
    }

    #[tokio::test]
    async fn test_get_endpoint_plus_symbol_decoding() {
        // Enable debug logging for this test
        // let _ = tracing_subscriber::fmt()
        //     .with_env_filter("debug")
        //     .try_init();

        let server_port = setup_test_server().await;
        let client = Client::new();

        // Test query with + symbols that should become spaces
        let test_query = "quantile by (instance, job) (0.95, fake_metric_total)";

        println!("Sending query: {test_query}");

        let response = client
            .get(format!("http://127.0.0.1:{server_port}/api/v1/query"))
            .query(&[("query", test_query)])
            .send()
            .await
            .expect("Failed to send request");

        let status = response.status();
        let response_json: serde_json::Value = response.json().await.expect("Failed to parse JSON");

        println!("Response status: {status}");
        println!("Response JSON: {response_json}");

        // The debug logs should show what query was actually parsed
        assert!(status.is_success() || status == reqwest::StatusCode::OK);
    }

    #[tokio::test]
    async fn test_post_endpoint_form_decoding() {
        // let _ = tracing_subscriber::fmt()
        //     .with_env_filter("debug")
        //     .try_init();

        let server_port = setup_test_server().await;
        let client = Client::new();

        // Test the same query via POST with form encoding
        let test_query = "quantile+by+(instance,+job)+(0.95,+fake_metric_total)";

        println!("Sending POST with form data: {test_query}");

        let response = client
            .post(format!("http://127.0.0.1:{server_port}/api/v1/query"))
            .header("content-type", "application/x-www-form-urlencoded")
            .body(format!("query={test_query}&time=1758161478.205"))
            .send()
            .await
            .expect("Failed to send request");

        let status = response.status();
        let response_json: serde_json::Value = response.json().await.expect("Failed to parse JSON");

        println!("Response status: {status}");
        println!("Response JSON: {response_json}");

        assert!(status.is_success() || status == reqwest::StatusCode::OK);
    }

    // ── StreamingConfig hot-reload (PR E) ────────────────────────────────

    /// POST a YAML streaming-config and verify the active state via
    /// GET reflects the swap. Covers the full round-trip through
    /// `HttpServer::with_hot_reload_config`, the POST parse+swap, and
    /// the GET snapshot emission.
    #[tokio::test]
    async fn test_streaming_config_hot_reload_round_trip() {
        let hot_reload = StreamingConfigHandle::new(StreamingConfig::default());
        let server_port = setup_test_server_with_hot_reload(Some(hot_reload.clone())).await;
        let client = Client::new();

        // Initial GET: empty config, 0 entries.
        let initial = client
            .get(format!(
                "http://127.0.0.1:{server_port}/api/v1/streaming-config"
            ))
            .send()
            .await
            .expect("GET failed");
        assert!(initial.status().is_success());
        let initial_body: serde_json::Value = initial.json().await.unwrap();
        assert_eq!(initial_body["aggregation_count"], 0);

        // POST a new config with two aggregation_ids. The YAML shape
        // matches what `StreamingConfig::from_yaml_data` parses — see
        // `asap-common/dependencies/rs/asap_types/src/streaming_config.rs`.
        let new_config_yaml = r#"
aggregations:
  - aggregationId: 101
    aggregationType: Sum
    aggregationSubType: ''
    metric: cpu_usage
    labels:
      grouping: [host]
      rollup: []
      aggregated: []
    parameters: {}
    windowSize: 60
    windowType: tumbling
    spatialFilter: ''
  - aggregationId: 102
    aggregationType: Sum
    aggregationSubType: ''
    metric: mem_usage
    labels:
      grouping: [host, region]
      rollup: []
      aggregated: []
    parameters: {}
    windowSize: 120
    windowType: tumbling
    spatialFilter: ''
"#;
        let post_resp = client
            .post(format!(
                "http://127.0.0.1:{server_port}/api/v1/streaming-config"
            ))
            .header("content-type", "application/x-yaml")
            .body(new_config_yaml.to_string())
            .send()
            .await
            .expect("POST failed");
        let post_status = post_resp.status();
        let post_body: serde_json::Value = post_resp.json().await.unwrap();
        assert!(
            post_status.is_success(),
            "POST returned {post_status}: {post_body}"
        );
        assert_eq!(post_body["status"], "success");
        assert_eq!(post_body["new_aggregation_count"], 2);
        // PR 5: the YAML's `aggregationId` fields are silently
        // dropped — `agg_ids_added` carries fingerprint u64s.
        let added = post_body["agg_ids_added"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_u64().unwrap())
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(added.len(), 2, "exactly two distinct aggs were added");
        assert!(added.iter().all(|id| *id != 0), "fingerprints are non-zero");

        // GET again: should reflect the two new ids.
        let after = client
            .get(format!(
                "http://127.0.0.1:{server_port}/api/v1/streaming-config"
            ))
            .send()
            .await
            .expect("GET after swap failed");
        assert!(after.status().is_success());
        let after_body: serde_json::Value = after.json().await.unwrap();
        assert_eq!(after_body["aggregation_count"], 2);

        // The underlying StreamingConfigHandle handle (cloned into
        // the server at setup) also reflects the swap — proving that
        // downstream consumers that re-snapshot would see the new
        // state. PR 5: the map is keyed on fingerprints, so just
        // assert the entry count.
        let direct_snap = hot_reload.snapshot();
        assert_eq!(direct_snap.materializations_by_policy_fingerprint.len(), 2);
    }

    #[tokio::test]
    async fn test_streaming_config_route_is_absent_without_legacy_handle() {
        // A server without a legacy hot-reload handle does not expose this route.
        let server_port = setup_test_server().await;
        let client = Client::new();

        let get_resp = client
            .get(format!(
                "http://127.0.0.1:{server_port}/api/v1/streaming-config"
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(get_resp.status(), reqwest::StatusCode::NOT_FOUND);

        let post_resp = client
            .post(format!(
                "http://127.0.0.1:{server_port}/api/v1/streaming-config"
            ))
            .body("anything")
            .send()
            .await
            .unwrap();
        assert_eq!(post_resp.status(), reqwest::StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_streaming_config_hot_reload_rejects_bad_yaml() {
        let hot_reload = StreamingConfigHandle::new(StreamingConfig::default());
        let server_port = setup_test_server_with_hot_reload(Some(hot_reload)).await;
        let client = Client::new();

        let resp = client
            .post(format!(
                "http://127.0.0.1:{server_port}/api/v1/streaming-config"
            ))
            .body("not: : : valid: yaml: :")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "error");
    }

    /// Set up a test server wired with a hot-reload handle and a
    /// shared `SketchStore` (the sid catalog the new sid-level
    /// reconcile reads + writes). Returns `(port, sketch_index)` so
    /// tests can pre-register sids or inspect the catalog after a
    /// streaming-config swap. Schema retirement final cut: the
    /// legacy `SchemaRegistry` is gone, so there is no longer a
    /// `schemas` parameter — every reconcile decision is sid-level.
    async fn setup_test_server_with_hot_reload_and_sketch_index(
        hot_reload: StreamingConfigHandle,
        summary_store: Arc<crate::storage_engines::sketch_db::index::SketchStore>,
    ) -> u16 {
        let adapter_config =
            AdapterConfig::prometheus_promql("http://127.0.0.1:9999".to_string(), false);
        let config = HttpServerConfig {
            port: 0,
            handle_http_requests: true,
            adapter_config,
        };
        let streaming_config = Arc::new(StreamingConfig::default());
        let query_engine = Arc::new(ASAPQueryEngine::new(15000));
        let server =
            HttpServer::new(config, query_engine, summary_store).with_hot_reload_config(hot_reload);
        server
            .start_test_server()
            .await
            .expect("Failed to start test server")
    }

    /// Helper that mints a Precompute-`Sum` sid registered as Active
    /// against the supplied `(metric, group_by)` signature. Tests
    /// pre-populate the sid catalog so the streaming-config swap
    /// handler has something concrete to reconcile.
    fn register_precompute_sid(
        store: &crate::storage_engines::sketch_db::index::SketchStore,
        sid: u64,
        metric: &str,
        group_by: &[&str],
    ) {
        use crate::storage_engines::sketch_db::data::AggKind;
        use crate::storage_engines::sketch_db::index::SummarySeriesMetadata;
        use std::collections::BTreeSet;
        let group_by_keys: BTreeSet<String> = group_by.iter().map(|s| s.to_string()).collect();
        store.register(SummarySeriesMetadata {
            sid,
            metric_name: metric.to_string(),
            group_by_keys,
            capability: None,
            agg_kind: AggKind::ExactAgg {
                agg_type: asap_types::AggregationType::Sum,
                parameters_canonical: String::new(),
                spatial_filter_canonical: String::new(),
            },
            accuracy: None,
            first_seen_unix_ms: 0,
            retired_at_ms: None,
            expires_at_ms: None,
            policy_fp: asap_types::PolicyFingerprint::UNSET,
        });
    }

    #[tokio::test]
    async fn test_streaming_config_swap_drives_sid_reconcile() {
        // Schema retirement final cut: the swap handler now drives a
        // single sid-level reconcile (no `SchemaRegistry`). Sids that
        // already exist in the catalog and whose content signature
        // does not appear in the new config get force-retired; the
        // response surfaces them under `sids_retired`. There is no
        // `sids_added` — sids are minted lazily by the ingest path,
        // not by the swap handler.
        use crate::storage_engines::sketch_db::index::SketchStore;
        use crate::storage_engines::sketch_db::AggStatus;

        let hot_reload = StreamingConfigHandle::new(StreamingConfig::default());
        let summary_store = Arc::new(SketchStore::new());
        // Pre-register two Active sids whose signatures match the
        // first config below; only sid 1 will survive the second
        // swap.
        register_precompute_sid(&summary_store, 1, "cpu_usage", &["host"]);
        register_precompute_sid(&summary_store, 2, "mem_usage", &["host"]);
        let server_port = setup_test_server_with_hot_reload_and_sketch_index(
            hot_reload.clone(),
            summary_store.clone(),
        )
        .await;
        let client = Client::new();

        // POST a config whose signatures cover both pre-registered
        // sids. Nothing should retire.
        let yaml_two = r#"
aggregations:
  - aggregationId: 101
    aggregationType: Sum
    aggregationSubType: ''
    metric: cpu_usage
    labels:
      grouping: [host]
      rollup: []
      aggregated: []
    parameters: {}
    windowSize: 60
    windowType: tumbling
    spatialFilter: ''
  - aggregationId: 202
    aggregationType: Sum
    aggregationSubType: ''
    metric: mem_usage
    labels:
      grouping: [host]
      rollup: []
      aggregated: []
    parameters: {}
    windowSize: 60
    windowType: tumbling
    spatialFilter: ''
"#;
        let resp = client
            .post(format!(
                "http://127.0.0.1:{server_port}/api/v1/streaming-config"
            ))
            .header("content-type", "application/x-yaml")
            .body(yaml_two.to_string())
            .send()
            .await
            .expect("POST failed");
        assert!(resp.status().is_success());
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "success");
        let retired_ids = body["sids_retired"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_u64().unwrap())
            .collect::<Vec<_>>();
        assert!(
            retired_ids.is_empty(),
            "no sid should retire when every signature still appears in the new config; got {retired_ids:?}",
        );
        assert_eq!(
            summary_store.instance(1).unwrap().status(),
            AggStatus::Active
        );
        assert_eq!(
            summary_store.instance(2).unwrap().status(),
            AggStatus::Active
        );

        // Swap to a config that drops `mem_usage`. Sid 2's signature
        // is now orphaned; the handler must force-retire it.
        let yaml_one = r#"
aggregations:
  - aggregationId: 101
    aggregationType: Sum
    aggregationSubType: ''
    metric: cpu_usage
    labels:
      grouping: [host]
      rollup: []
      aggregated: []
    parameters: {}
    windowSize: 60
    windowType: tumbling
    spatialFilter: ''
"#;
        let resp2 = client
            .post(format!(
                "http://127.0.0.1:{server_port}/api/v1/streaming-config"
            ))
            .header("content-type", "application/x-yaml")
            .body(yaml_one.to_string())
            .send()
            .await
            .expect("POST failed");
        let body2: serde_json::Value = resp2.json().await.unwrap();
        let retired = body2["sids_retired"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_u64().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(retired, vec![2u64]);
        assert_eq!(
            summary_store.instance(1).unwrap().status(),
            AggStatus::Active
        );
        assert_eq!(
            summary_store.instance(2).unwrap().status(),
            AggStatus::Retired
        );
    }

    #[tokio::test]
    async fn test_streaming_config_swap_response_shape_with_empty_catalog() {
        // With no registered sids, the swap still works — it just
        // produces an empty `sids_retired` array. The `agg_ids_added`
        // / `agg_ids_removed` / `new_aggregation_count` fields are
        // driven purely by the diff of the two configs and are
        // independent of the sid catalog.
        //
        // PR 5: the YAML's `aggregationId: 42` is silently dropped at
        // parse time — the backend identity is content-addressed via
        // `PolicyFingerprint::from_config`. The `agg_ids_added` u64
        // in the HTTP response is the fingerprint's `as_u64()` form,
        // NOT the literal `42` the YAML once spelled out.
        let hot_reload = StreamingConfigHandle::new(StreamingConfig::default());
        let server_port = setup_test_server_with_hot_reload(Some(hot_reload)).await;
        let client = Client::new();

        let yaml = r#"
aggregations:
  - aggregationType: Sum
    aggregationSubType: ''
    metric: m
    labels:
      grouping: []
      rollup: []
      aggregated: []
    parameters: {}
    windowSize: 60
    windowType: tumbling
    spatialFilter: ''
"#;
        let resp = client
            .post(format!(
                "http://127.0.0.1:{server_port}/api/v1/streaming-config"
            ))
            .header("content-type", "application/x-yaml")
            .body(yaml.to_string())
            .send()
            .await
            .expect("POST failed");
        assert!(resp.status().is_success());
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "success");
        assert_eq!(body["new_aggregation_count"], 1);
        let added = body["agg_ids_added"].as_array().expect("array");
        assert_eq!(added.len(), 1, "exactly one agg was added");
        assert_ne!(
            added[0].as_u64().unwrap(),
            0,
            "agg id is not the 0 sentinel"
        );
        assert_eq!(body["agg_ids_removed"], serde_json::json!([]));
        // No pre-registered sids → nothing to retire.
        assert_eq!(body["sids_retired"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_get_schemas_returns_active_and_retired_sids_with_status_filter() {
        // Schema retirement final cut: `/api/v1/db/schemas` now
        // surfaces sid-catalog entries. Pre-register two sids, then
        // POST a streaming config that orphans one — the swap
        // handler force-retires it.
        use crate::storage_engines::sketch_db::index::SketchStore;

        let hot_reload = StreamingConfigHandle::new(StreamingConfig::default());
        let summary_store = Arc::new(SketchStore::new());
        register_precompute_sid(&summary_store, 1, "m1", &[]);
        register_precompute_sid(&summary_store, 2, "m2", &[]);
        let server_port = setup_test_server_with_hot_reload_and_sketch_index(
            hot_reload.clone(),
            summary_store.clone(),
        )
        .await;
        let client = Client::new();

        // Retire sid 2 by pushing a config covering only `m1`.
        let yaml_one = r#"
aggregations:
  - aggregationId: 1
    aggregationType: Sum
    aggregationSubType: ''
    metric: m1
    labels: { grouping: [], rollup: [], aggregated: [] }
    parameters: {}
    windowSize: 60
    windowType: tumbling
    spatialFilter: ''
"#;
        let resp = client
            .post(format!(
                "http://127.0.0.1:{server_port}/api/v1/streaming-config"
            ))
            .header("content-type", "application/x-yaml")
            .body(yaml_one.to_string())
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());

        // GET /api/v1/db/schemas (no filter = all).
        let resp = client
            .get(format!("http://127.0.0.1:{server_port}/api/v1/db/schemas"))
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "success");
        assert_eq!(body["count"], 2);
        let entries = body["schemas"].as_array().unwrap();
        // Sorted by sid — first is active, second is retired.
        assert_eq!(entries[0]["sid"], 1);
        assert_eq!(entries[0]["status"], "active");
        assert_eq!(entries[0]["metric_name"], "m1");
        assert!(entries[0]["retired_at_ms"].is_null());
        assert_eq!(entries[1]["sid"], 2);
        assert_eq!(entries[1]["status"], "retired");
        assert!(entries[1]["retired_at_ms"].is_u64());

        // Filter: active only.
        let resp = client
            .get(format!(
                "http://127.0.0.1:{server_port}/api/v1/db/schemas?status=active"
            ))
            .send()
            .await
            .unwrap();
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["count"], 1);
        assert_eq!(body["schemas"][0]["sid"], 1);

        // Filter: retired only.
        let resp = client
            .get(format!(
                "http://127.0.0.1:{server_port}/api/v1/db/schemas?status=retired"
            ))
            .send()
            .await
            .unwrap();
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["count"], 1);
        assert_eq!(body["schemas"][0]["sid"], 2);

        // Bogus filter → 400.
        let resp = client
            .get(format!(
                "http://127.0.0.1:{server_port}/api/v1/db/schemas?status=junk"
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_get_schemas_with_empty_catalog_returns_empty_array() {
        // Schema retirement final cut: the sid catalog is always
        // attached (every `HttpServer` carries one). With no
        // registered sids the endpoint reports an empty array, not
        // a 503.
        let hot_reload = StreamingConfigHandle::new(StreamingConfig::default());
        let server_port = setup_test_server_with_hot_reload(Some(hot_reload)).await;
        let client = Client::new();

        let resp = client
            .get(format!("http://127.0.0.1:{server_port}/api/v1/db/schemas"))
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "success");
        assert_eq!(body["count"], 0);
        assert_eq!(body["schemas"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_post_schema_retire_and_expire_endpoints_drive_sid_catalog() {
        // Coverage for `POST /api/v1/db/schemas/:sid/retire` and
        // `POST /api/v1/db/schemas/:sid/expire` after the schema
        // retirement final cut: both routes take `:sid` and drive
        // the sid catalog directly via `SketchStore::force_retire`
        // and `SketchStore::force_expire`. Unknown sid → 404.
        use crate::storage_engines::sketch_db::index::SketchStore;
        use crate::storage_engines::sketch_db::AggStatus;

        let hot_reload = StreamingConfigHandle::new(StreamingConfig::default());
        let summary_store = Arc::new(SketchStore::new());
        register_precompute_sid(&summary_store, 11, "cpu", &["host"]);
        register_precompute_sid(&summary_store, 22, "mem", &["host"]);
        let server_port = setup_test_server_with_hot_reload_and_sketch_index(
            hot_reload.clone(),
            summary_store.clone(),
        )
        .await;
        let client = Client::new();

        // Retire sid 11.
        let resp = client
            .post(format!(
                "http://127.0.0.1:{server_port}/api/v1/db/schemas/11/retire"
            ))
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "success");
        assert_eq!(body["schema"]["sid"], 11);
        assert_eq!(body["schema"]["status"], "retired");
        assert_eq!(
            summary_store.instance(11).unwrap().status(),
            AggStatus::Retired
        );

        // Expire sid 22.
        let resp = client
            .post(format!(
                "http://127.0.0.1:{server_port}/api/v1/db/schemas/22/expire"
            ))
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "success");
        assert_eq!(body["schema"]["sid"], 22);
        assert_eq!(body["schema"]["status"], "expired");
        assert_eq!(
            summary_store.instance(22).unwrap().status(),
            AggStatus::Expired
        );

        // Unknown sid → 404 for both routes.
        for path in [
            "/api/v1/db/schemas/9999/retire",
            "/api/v1/db/schemas/9999/expire",
        ] {
            let resp = client
                .post(format!("http://127.0.0.1:{server_port}{path}"))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND, "{path}");
        }
    }

    #[tokio::test]
    async fn test_get_timeline_missing_param_returns_400() {
        use crate::storage_engines::sketch_db::index::SketchStore;

        let hot_reload = StreamingConfigHandle::new(StreamingConfig::default());
        let summary_store = Arc::new(SketchStore::new());
        let server_port = setup_test_server_with_hot_reload_and_sketch_index(
            hot_reload.clone(),
            summary_store.clone(),
        )
        .await;
        let client = Client::new();

        // No metric param → 400.
        let resp = client
            .get(format!(
                "http://127.0.0.1:{server_port}/api/v1/db/timeline?start_ms=0&end_ms=100"
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

        // Missing start_ms.
        let resp = client
            .get(format!(
                "http://127.0.0.1:{server_port}/api/v1/db/timeline?metric=m&end_ms=100"
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

        // Non-numeric start_ms.
        let resp = client
            .get(format!(
                "http://127.0.0.1:{server_port}/api/v1/db/timeline?metric=m&start_ms=abc&end_ms=100"
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_get_timeline_with_no_sids_returns_empty_200() {
        // Schema retirement #2 — the `/api/v1/db/timeline` endpoint now
        // reads from the sid catalog (always attached) instead of the
        // optional `SchemaRegistry`. Empty catalog → empty segments,
        // not a 503.
        let hot_reload = StreamingConfigHandle::new(StreamingConfig::default());
        let server_port = setup_test_server_with_hot_reload(Some(hot_reload)).await;
        let client = Client::new();
        let resp = client
            .get(format!(
                "http://127.0.0.1:{server_port}/api/v1/db/timeline?metric=m&start_ms=0&end_ms=100"
            ))
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["count"], 0);
        assert_eq!(body["segments"].as_array().unwrap().len(), 0);
    }

    // ─── backfill HTTP endpoint tests ─────────────────────────────

    /// Build a server with a backfill registry and active sid fixtures. Test
    /// markers generate per-metric configs; the returned fingerprints let callers
    /// target those configs through HTTP.
    async fn setup_test_server_with_backfill_and_sids(
        registry: Arc<crate::storage_engines::sketch_db::BackfillRegistry>,
        active_agg_ids: &[u64],
    ) -> u16 {
        setup_test_server_with_backfill_and_sids_returning_fps(registry, active_agg_ids)
            .await
            .0
    }

    /// Same as `setup_test_server_with_backfill_and_sids` but also
    /// returns the marker→fingerprint mapping so tests can compute
    /// the fingerprint u64 they need to POST.
    async fn setup_test_server_with_backfill_and_sids_returning_fps(
        registry: Arc<crate::storage_engines::sketch_db::BackfillRegistry>,
        active_agg_ids: &[u64],
    ) -> (u16, std::collections::HashMap<u64, u64>) {
        use asap_types::aggregation_config::AggregationConfig;
        use asap_types::enums::WindowKind;
        use asap_types::AggregationType;
        use asap_types::KeyByLabelNames;
        use std::collections::HashMap;

        let adapter_config =
            AdapterConfig::prometheus_promql("http://127.0.0.1:9999".to_string(), false);
        let config = HttpServerConfig {
            port: 0,
            handle_http_requests: true,
            adapter_config,
        };
        // Build a StreamingConfig with one Sum agg per `active_agg_ids`
        // so the backfill handler's agg-config lookup (post-schema-
        // retirement) can find them. The matching sid in the catalog
        // is registered via the canonical ingest path so its content
        // hash matches what `create_checked` would compute.
        let mut agg_map = HashMap::new();
        let mut marker_to_fp = std::collections::HashMap::new();
        for marker in active_agg_ids {
            let metric = format!("metric_{marker}");
            let cfg = AggregationConfig {
                population_key_encoding: Default::default(),
                aggregation_type: AggregationType::Sum,
                aggregation_sub_type: String::new(),
                parameters: HashMap::new(),
                grouping_labels: KeyByLabelNames::empty().into(),
                aggregated_labels: KeyByLabelNames::empty(),
                rollup_labels: KeyByLabelNames::empty(),
                original_yaml: String::new(),
                window_size: 1,
                slide_interval: 1,
                window_type: WindowKind::Tumbling,
                window_layout: asap_types::WindowMaterializationLayout::Pane { pane_secs: 1 },
                pane_origin_ms: None,
                spatial_filter: String::new(),
                spatial_filter_normalized: String::new(),
                metric: metric.clone(),
                num_aggregates_to_retain: None,
                table_name: None,
                value_projection: None,
                table_population: None,
                derived_input: None,
                table_timestamp_column: None,
                partitioning: None,
                value_source_column: None,
            };
            // PR 5: streaming-config is keyed on the policy
            // fingerprint. Build a marker→fingerprint map so the test
            // POSTs the right id on the wire.
            let fp = cfg.policy_fp_u64();
            marker_to_fp.insert(*marker, fp);
            agg_map.insert(fp, cfg);
        }
        let streaming_config = Arc::new(StreamingConfig::new(agg_map));
        let hot_reload = StreamingConfigHandle::from_arc(streaming_config.clone());
        let query_engine = Arc::new(ASAPQueryEngine::new(15000));
        let summary_store = Arc::new(crate::storage_engines::sketch_db::index::SketchStore::new());
        for marker in active_agg_ids {
            let fp = marker_to_fp[marker];
            register_precompute_sid(&summary_store, fp, &format!("metric_{marker}"), &[]);
        }
        let server = HttpServer::new(config, query_engine, summary_store)
            .with_backfill_registry(registry)
            .with_hot_reload_config(hot_reload);
        let port = server
            .start_test_server()
            .await
            .expect("Failed to start test server");
        (port, marker_to_fp)
    }

    #[tokio::test]
    async fn test_backfill_full_lifecycle_through_http() {
        let registry = Arc::new(crate::storage_engines::sketch_db::BackfillRegistry::new());
        let (server_port, marker_to_fp) =
            setup_test_server_with_backfill_and_sids_returning_fps(registry.clone(), &[42]).await;
        let client = Client::new();
        // PR 5: the streaming-config key is the policy fingerprint;
        // POST the fingerprint, not the test marker.
        let fp_42 = marker_to_fp[&42];

        // POST creates a Queued job.
        let req = serde_json::json!({
            "agg_id": fp_42,
            "start_ms": 100,
            "end_ms": 500,
            "source": { "Prometheus": { "url": "http://prom.local" } },
            "windows_total": 4});
        let resp = client
            .post(format!("http://127.0.0.1:{server_port}/api/v1/db/backfill"))
            .json(&req)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
        let body: serde_json::Value = resp.json().await.unwrap();
        let job_id = body["job_id"].as_u64().expect("job_id in response");
        assert_eq!(body["status"], "success");

        // GET /jobs/:id returns the queued job.
        let resp = client
            .get(format!(
                "http://127.0.0.1:{server_port}/api/v1/db/backfill/jobs/{job_id}"
            ))
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["job"]["status"], "queued");
        assert_eq!(body["job"]["agg_id"].as_u64().unwrap(), fp_42);
        assert_eq!(body["job"]["start_ms"], 100);
        assert_eq!(body["job"]["windows_total"], 4);
        assert_eq!(body["job"]["progress"], 0.0);

        // GET /jobs (no filter) lists all.
        let resp = client
            .get(format!(
                "http://127.0.0.1:{server_port}/api/v1/db/backfill/jobs"
            ))
            .send()
            .await
            .unwrap();
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["count"], 1);

        // Filter by status=queued → 1.
        let resp = client
            .get(format!(
                "http://127.0.0.1:{server_port}/api/v1/db/backfill/jobs?status=queued"
            ))
            .send()
            .await
            .unwrap();
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["count"], 1);

        // Filter by status=running → 0.
        let resp = client
            .get(format!(
                "http://127.0.0.1:{server_port}/api/v1/db/backfill/jobs?status=running"
            ))
            .send()
            .await
            .unwrap();
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["count"], 0);

        // DELETE cancels the job.
        let resp = client
            .delete(format!(
                "http://127.0.0.1:{server_port}/api/v1/db/backfill/jobs/{job_id}"
            ))
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());
        assert_eq!(
            registry.get(job_id).unwrap().status,
            crate::storage_engines::sketch_db::BackfillStatus::Cancelled
        );

        // Second DELETE on already-cancelled job → 409 Conflict.
        let resp = client
            .delete(format!(
                "http://127.0.0.1:{server_port}/api/v1/db/backfill/jobs/{job_id}"
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_backfill_post_rejects_inverted_range() {
        let registry = Arc::new(crate::storage_engines::sketch_db::BackfillRegistry::new());
        let server_port = setup_test_server_with_backfill_and_sids(registry, &[1]).await;
        let client = Client::new();

        let req = serde_json::json!({
            "agg_id": 1,
            "start_ms": 500,
            "end_ms": 100,
            "source": { "Prometheus": { "url": "http://prom.local" } },
            "windows_total": 1});
        let resp = client
            .post(format!("http://127.0.0.1:{server_port}/api/v1/db/backfill"))
            .json(&req)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_backfill_get_unknown_job_returns_404() {
        let registry = Arc::new(crate::storage_engines::sketch_db::BackfillRegistry::new());
        let server_port = setup_test_server_with_backfill_and_sids(registry, &[]).await;
        let client = Client::new();
        let resp = client
            .get(format!(
                "http://127.0.0.1:{server_port}/api/v1/db/backfill/jobs/9999"
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_backfill_endpoints_503_without_registry() {
        // Build a server with NO backfill registry attached — every
        // backfill endpoint should 503.
        let hot_reload = StreamingConfigHandle::new(StreamingConfig::default());
        let server_port = setup_test_server_with_hot_reload(Some(hot_reload)).await;
        let client = Client::new();

        for (method, path) in [
            ("GET", "/api/v1/db/backfill/jobs"),
            ("GET", "/api/v1/db/backfill/jobs/1"),
        ] {
            let resp = client
                .request(
                    reqwest::Method::from_bytes(method.as_bytes()).unwrap(),
                    format!("http://127.0.0.1:{server_port}{path}"),
                )
                .send()
                .await
                .unwrap();
            assert_eq!(
                resp.status(),
                reqwest::StatusCode::SERVICE_UNAVAILABLE,
                "{method} {path}"
            );
        }

        // POST also.
        let req = serde_json::json!({
            "agg_id": 1,
            "start_ms": 0,
            "end_ms": 10,
            "source": { "Prometheus": { "url": "x" } },
            "windows_total": 1});
        let resp = client
            .post(format!("http://127.0.0.1:{server_port}/api/v1/db/backfill"))
            .json(&req)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_backfill_list_bogus_status_returns_400() {
        let registry = Arc::new(crate::storage_engines::sketch_db::BackfillRegistry::new());
        let server_port = setup_test_server_with_backfill_and_sids(registry, &[]).await;
        let client = Client::new();
        let resp = client
            .get(format!(
                "http://127.0.0.1:{server_port}/api/v1/db/backfill/jobs?status=junk"
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    }

    // §10.5: create_checked invariants surfacing through HTTP.

    #[tokio::test]
    async fn test_backfill_post_unknown_agg_returns_404() {
        let registry = Arc::new(crate::storage_engines::sketch_db::BackfillRegistry::new());
        // Empty schema registry — agg_id 42 is unknown.
        let server_port = setup_test_server_with_backfill_and_sids(registry, &[]).await;
        let client = Client::new();
        let req = serde_json::json!({
            "agg_id": 42,
            "start_ms": 100,
            "end_ms": 500,
            "source": { "Prometheus": { "url": "http://prom.local" } },
            "windows_total": 1});
        let resp = client
            .post(format!("http://127.0.0.1:{server_port}/api/v1/db/backfill"))
            .json(&req)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "error");
        assert!(body["error"]
            .as_str()
            .unwrap()
            .contains("unknown agg_id 42"));
    }

    #[tokio::test]
    async fn test_backfill_post_overlap_with_live_ingest_returns_409() {
        let registry = Arc::new(crate::storage_engines::sketch_db::BackfillRegistry::new());
        // Schema registered at `now` — any `end_ms` > created_at_ms
        // overlaps live ingest.
        let (server_port, marker_to_fp) =
            setup_test_server_with_backfill_and_sids_returning_fps(registry, &[7]).await;
        let client = Client::new();
        let future_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 3_600_000;
        let req = serde_json::json!({
            "agg_id": marker_to_fp[&7],
            "start_ms": 0,
            "end_ms": future_ms,
            "source": { "Prometheus": { "url": "http://prom.local" } },
            "windows_total": 1});
        let resp = client
            .post(format!("http://127.0.0.1:{server_port}/api/v1/db/backfill"))
            .json(&req)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::CONFLICT);
    }

    // HTTP routing tests verify configured engine dispatch and the response
    // `data_source` annotation.

    use crate::query_engines::routing::{EngineCapabilities, QueryEngine};
    use crate::query_engines::{EngineError, QueryResult};
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// In-test stub `QueryEngine`. Records call counts and returns a
    /// canned `QueryResult` keyed to the configured `data_source_id`
    /// so HTTP-level assertions can pin which engine answered.
    struct MockQueryEngine {
        caps: EngineCapabilities,
        calls: Arc<AtomicUsize>,
        outcome: MockOutcome,
    }

    #[derive(Clone)]
    enum MockOutcome {
        /// Empty instant vector — the Prometheus adapter still
        /// produces `status=success` with `data.result=[]`.
        OkEmpty,
        /// Force the engine to fail with `EngineError::Backend` so
        /// the router falls through to the next compatible backend.
        Backend,
    }

    impl MockQueryEngine {
        fn new(backend: StorageBackend, outcome: MockOutcome) -> (Arc<Self>, Arc<AtomicUsize>) {
            let calls = Arc::new(AtomicUsize::new(0));
            let engine = Arc::new(Self {
                caps: EngineCapabilities {
                    data_source_id: backend.data_source_id(),
                    storage_backend: backend,
                    supports_streams_above_bytes: 1024 * 1024,
                },
                calls: calls.clone(),
                outcome,
            });
            (engine, calls)
        }
    }

    #[async_trait]
    impl QueryEngine for MockQueryEngine {
        async fn execute(&self, _query: &str) -> Result<QueryResult, EngineError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self.outcome {
                MockOutcome::OkEmpty => Ok(QueryResult::vector(Vec::new(), 0)),
                MockOutcome::Backend => Err(EngineError::backend(
                    self.caps.data_source_id,
                    "simulated backend failure",
                )),
            }
        }
        fn capabilities(&self) -> EngineCapabilities {
            self.caps
        }

        async fn execute_range(
            &self,
            _query: &str,
            _start_ms: u64,
            _end_ms: u64,
            _step_ms: u64,
        ) -> Result<QueryResult, EngineError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self.outcome {
                MockOutcome::OkEmpty => Ok(QueryResult::matrix(Vec::new())),
                MockOutcome::Backend => Err(EngineError::backend(
                    self.caps.data_source_id,
                    "simulated backend failure",
                )),
            }
        }
    }

    /// Build an `HttpServer` whose router holds the supplied set of
    /// `QueryEngine`s. The hot-reload `StreamingConfig` is pinned at
    /// `metric_storage_backend` so query dispatch follows the
    /// requested capability axis. Returns the bound port + the
    /// `StreamingConfigHandle` handle so tests can swap the
    /// `storage_backend` mid-flight if they need to.
    async fn setup_test_server_with_router(
        metric_storage_backend: StorageBackend,
        extra_engines: Vec<Arc<dyn QueryEngine>>,
    ) -> u16 {
        setup_test_server_with_router_and_fallback(metric_storage_backend, extra_engines, false)
            .await
    }

    async fn setup_test_server_with_router_and_fallback(
        metric_storage_backend: StorageBackend,
        extra_engines: Vec<Arc<dyn QueryEngine>>,
        forward_unsupported: bool,
    ) -> u16 {
        let adapter_config = AdapterConfig::prometheus_promql(
            "http://127.0.0.1:9999".to_string(),
            forward_unsupported,
        );
        let config = HttpServerConfig {
            port: 0,
            handle_http_requests: true,
            adapter_config,
        };
        // Pin `storage_backend` on the streaming config so the http
        // dispatcher reads it back through the hot-reload handle.
        let streaming_cfg =
            StreamingConfig::with_storage_backend(Default::default(), metric_storage_backend);
        let streaming_arc = Arc::new(streaming_cfg);
        let hot_reload = StreamingConfigHandle::from_arc(streaming_arc.clone());
        let query_engine = Arc::new(ASAPQueryEngine::new(15000));
        let mut server = HttpServer::new(
            config,
            query_engine,
            Arc::new(crate::storage_engines::sketch_db::index::SketchStore::new()),
        )
        .with_hot_reload_config(hot_reload);
        for engine in extra_engines {
            server = server.with_query_engine(engine);
        }
        server
            .start_test_server()
            .await
            .expect("Failed to start test server")
    }

    /// Build an `HttpServer` wired with a per-metric
    /// `BackendStorageRouting` table — the **production path** the
    /// issue-46 MVP relies on. The streaming-config single axis stays
    /// at `SketchStore` (the realistic deploy state); the routing
    /// table is what flips per-metric dispatch over to the
    /// `EngineRouter`. This proves the production code path
    /// (`process_query_request → resolve_metric_storage → routing
    /// table lookup`), as opposed to the
    /// `setup_test_server_with_router` helper above which mocks the
    /// resolution by pinning `streaming_cfg.storage_backend` directly.
    async fn setup_test_server_with_routing_table(
        routing: crate::storage_engines::types::BackendStorageRouting,
        extra_engines: Vec<Arc<dyn QueryEngine>>,
    ) -> u16 {
        let adapter_config =
            AdapterConfig::prometheus_promql("http://127.0.0.1:9999".to_string(), false);
        let config = HttpServerConfig {
            port: 0,
            handle_http_requests: true,
            adapter_config,
        };
        // Streaming-config stays on the default `SketchStore` axis
        // — exactly what the production deploy looks like (the YAML
        // loader doesn't parse `storage_backend`). All routing
        // decisions must come from the per-metric routing table.
        let streaming_cfg = StreamingConfig::default();
        let streaming_arc = Arc::new(streaming_cfg);
        let hot_reload = StreamingConfigHandle::from_arc(streaming_arc.clone());
        let query_engine = Arc::new(ASAPQueryEngine::new(15000));
        let mut server = HttpServer::new(
            config,
            query_engine,
            Arc::new(crate::storage_engines::sketch_db::index::SketchStore::new()),
        )
        .with_hot_reload_config(hot_reload)
        .with_backend_storage_routing(Arc::new(routing));
        for engine in extra_engines {
            server = server.with_query_engine(engine);
        }
        server
            .start_test_server()
            .await
            .expect("Failed to start test server")
    }

    /// Build a server whose `EngineRouter` has zero registered
    /// engines. We can't reach this through the public API
    /// (`HttpServer::new` always registers `ASAPQueryEngine`), so the
    /// helper drops in a router by hand via the same builder
    /// surface — but registers nothing, then asks the router-path
    /// dispatch to route an archive metric. Used by the
    /// `503 NoEngineRegistered` test.
    async fn setup_test_server_with_empty_router(metric_storage_backend: StorageBackend) -> u16 {
        // `HttpServer::new` always registers ASAPQueryEngine for the
        // ASAP tier. To force `NoEngineRegistered` we point the
        // metric at a backend whose data_source_id doesn't match
        // any registered engine — since `HttpServer::new` only
        // registers ASAPQueryEngine (asap_query), routing a
        // `GorillaObjectStore`-only metric trips the empty path
        // (compatible_storage_backends = [GorillaObjectStore], no
        // engine registered for that id).
        setup_test_server_with_router(metric_storage_backend, Vec::new()).await
    }

    /// Asserts the `data_source: <expected>` info-line lands on the
    /// Prometheus response body's `infos` array. Pulled out so each
    /// dispatch-axis test reads the same way.
    fn assert_data_source(body: &serde_json::Value, expected: &str) {
        let infos = body
            .get("infos")
            .and_then(|v| v.as_array())
            .unwrap_or_else(|| panic!("expected `infos` array in response body, got {body}",));
        let want = format!("data_source: {expected}");
        assert!(
            infos.iter().any(|v| v.as_str() == Some(&want)),
            "expected `{want}` in infos, got {infos:?}",
        );
    }

    #[test]
    fn accuracy_header_parses_explicit_contract_and_rejects_typos() {
        let mut headers = HeaderMap::new();
        assert!(matches!(
            extract_accuracy(&headers),
            Ok(AccuracyTarget::Epsilon(_))
        ));
        headers.insert(ACCURACY_HEADER, "exact".parse().unwrap());
        assert!(matches!(
            extract_accuracy(&headers),
            Ok(AccuracyTarget::Exact)
        ));
        headers.insert(ACCURACY_HEADER, "approximate".parse().unwrap());
        assert!(matches!(
            extract_accuracy(&headers),
            Ok(AccuracyTarget::Epsilon(_))
        ));
        headers.insert(ACCURACY_HEADER, "best-effort".parse().unwrap());
        assert!(extract_accuracy(&headers).is_err());
    }

    /// #746: `exact` means "not from the sketches". With no archive tier
    /// left the request is forwarded to the Prometheus fallback, so no
    /// in-process engine runs. The test fallback points at a dead port, so
    /// the body is the forwarder's error — what matters is that the sketch
    /// tier was never consulted.
    #[tokio::test]
    async fn exact_accuracy_header_bypasses_the_sketch_tier() {
        let (sketch, sketch_calls) =
            MockQueryEngine::new(StorageBackend::SketchStore, MockOutcome::OkEmpty);
        let server_port = setup_test_server_with_router(
            StorageBackend::SketchStore,
            vec![sketch as Arc<dyn QueryEngine>],
        )
        .await;
        let response = Client::new()
            .get(format!("http://127.0.0.1:{server_port}/api/v1/query"))
            .header(ACCURACY_HEADER, "exact")
            .query(&[("query", "sum_over_time(foo[5m])"), ("time", "1700000000")])
            .send()
            .await
            .unwrap();
        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(
            sketch_calls.load(Ordering::SeqCst),
            0,
            "an exact request must never be answered from the sketch tier; got {body}",
        );
        assert_eq!(
            body["status"], "error",
            "the test fallback is unreachable, so the forward must surface an error; got {body}",
        );
    }

    #[tokio::test]
    async fn exact_accuracy_header_bypasses_the_sketch_tier_for_range_queries() {
        let (sketch, sketch_calls) =
            MockQueryEngine::new(StorageBackend::SketchStore, MockOutcome::OkEmpty);
        let server_port = setup_test_server_with_router(
            StorageBackend::SketchStore,
            vec![sketch as Arc<dyn QueryEngine>],
        )
        .await;
        let response = Client::new()
            .get(format!("http://127.0.0.1:{server_port}/api/v1/query_range"))
            .header(ACCURACY_HEADER, "exact")
            .query(&[
                ("query", "sum_over_time(foo[5m])"),
                ("start", "1699999940"),
                ("end", "1700000000"),
                ("step", "10"),
            ])
            .send()
            .await
            .unwrap();
        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(
            sketch_calls.load(Ordering::SeqCst),
            0,
            "an exact range request must never be answered from the sketch tier; got {body}",
        );
        assert_eq!(
            body["status"], "error",
            "the test fallback is unreachable, so the forward must surface an error; got {body}",
        );
    }

    #[tokio::test]
    async fn invalid_accuracy_header_returns_bad_request() {
        let server_port =
            setup_test_server_with_router(StorageBackend::SketchStore, Vec::new()).await;
        let response = Client::new()
            .get(format!("http://127.0.0.1:{server_port}/api/v1/query"))
            .header(ACCURACY_HEADER, "best-effort")
            .query(&[("query", "sum_over_time(foo[5m])")])
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn http_routes_asap_tier_metric_to_simple_engine() {
        // Default (no hot-reload) → `SketchStore`. The handler
        // takes the direct `ASAPQueryEngine::handle_query` path; the
        // empty local engine reports no result without a warm annotation.
        let server_port =
            setup_test_server_with_router(StorageBackend::SketchStore, Vec::new()).await;
        let client = Client::new();
        let resp = client
            .get(format!("http://127.0.0.1:{server_port}/api/v1/query"))
            .query(&[("query", "sum_over_time(foo[5m])"), ("time", "1700000000")])
            .send()
            .await
            .expect("Failed to send request");
        assert!(
            resp.status().is_success(),
            "ASAP-tier dispatch must return 2xx; got {}",
            resp.status()
        );
        let body: serde_json::Value = resp.json().await.unwrap();
        // Empty local fixtures exercise routing, not successful execution.
        assert_eq!(
            body,
            serde_json::json!({"status":"error", "data":null,
            "errorType":"bad_data", "error":"No result for query"})
        );
    }

    #[tokio::test]
    async fn http_routes_archive_spelled_metric_to_the_sketch_tier() {
        // #746: `GorillaObjectStore` survives as a stored-plan spelling but
        // has no engine of its own. A metric pinned to it dispatches through
        // the router to the sketch tier, so the response carries
        // `data_source: asap_query`.
        let (gorilla, gorilla_calls) =
            MockQueryEngine::new(StorageBackend::SketchStore, MockOutcome::OkEmpty);
        let server_port = setup_test_server_with_router(
            StorageBackend::DoubleWrite,
            vec![gorilla as Arc<dyn QueryEngine>],
        )
        .await;
        let client = Client::new();
        let resp = client
            .get(format!("http://127.0.0.1:{server_port}/api/v1/query"))
            .query(&[
                ("query", "sum_over_time(audit_events[1h])"),
                ("time", "1700000000"),
            ])
            .send()
            .await
            .expect("Failed to send request");
        assert!(
            resp.status().is_success(),
            "router dispatch must return 2xx; got {}",
            resp.status()
        );
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_data_source(&body, "asap_query");
        assert_eq!(
            gorilla_calls.load(Ordering::SeqCst),
            1,
            "the sketch-tier mock engine should have been hit exactly once",
        );
    }

    #[tokio::test]
    async fn http_query_with_no_storage_config_defaults_to_asap_tier() {
        // `StreamingConfig::default()` has `storage_backend =
        // SketchStore` (per the `#[serde(default)]` on the
        // field — see `streaming_config.rs`). A server set up
        // without a hot-reload handle still infers ASAP-tier and
        // takes the ASAPQueryEngine direct path. Back-compat for
        // pre-Phase-5 deploys whose YAML doesn't include the new
        // `storage_backend` key.
        let server_port = setup_test_server().await; // No hot-reload handle attached.
        let client = Client::new();
        let resp = client
            .get(format!("http://127.0.0.1:{server_port}/api/v1/query"))
            .query(&[("query", "sum_over_time(foo[5m])"), ("time", "1700000000")])
            .send()
            .await
            .expect("Failed to send request");
        assert!(
            resp.status().is_success(),
            "default-config dispatch must return 2xx; got {}",
            resp.status()
        );
        let body: serde_json::Value = resp.json().await.unwrap();
        // Empty local fixtures exercise routing, not successful execution.
        assert_eq!(
            body,
            serde_json::json!({"status":"error", "data":null,
            "errorType":"bad_data", "error":"No result for query"})
        );
    }

    #[tokio::test]
    async fn http_returns_503_when_no_engines_registered() {
        // PrometheusRemote has one eligible backend. Without its engine registered,
        // the router must return `NoEngineRegistered`, surfaced as HTTP 503.
        let server_port =
            setup_test_server_with_empty_router(StorageBackend::PrometheusRemote).await;
        let client = Client::new();
        let resp = client
            .get(format!("http://127.0.0.1:{server_port}/api/v1/query"))
            .query(&[("query", "sum_over_time(foo[5m])"), ("time", "1700000000")])
            .send()
            .await
            .expect("Failed to send request");
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
            "no-engine routing must surface as 503",
        );
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "error");
        let err = body["error"].as_str().unwrap_or_default();
        assert!(
            err.contains("no engine registered"),
            "503 body must explain the routing failure; got {err}",
        );
    }

    #[tokio::test]
    async fn range_returns_503_when_engine_missing_even_with_fallback() {
        let server_port = setup_test_server_with_router_and_fallback(
            StorageBackend::PrometheusRemote,
            Vec::new(),
            true,
        )
        .await;
        let response = Client::new()
            .get(format!("http://127.0.0.1:{server_port}/api/v1/query_range"))
            .query(&[
                ("query", "sum_over_time(foo[5m])"),
                ("start", "1699999940"),
                ("end", "1700000000"),
                ("step", "10"),
            ])
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        let body: serde_json::Value = response.json().await.unwrap();
        assert!(body["error"]
            .as_str()
            .unwrap_or_default()
            .contains("no engine registered"));
    }

    #[tokio::test]
    async fn http_passes_through_accuracy_envelope() {
        // The Phase-5 `QueryResult` carries an `accuracy:
        // AccuracyEnvelope` field; the HTTP adapter mirrors it onto
        // the response's `infos` array (`accuracy: ε=..., δ=...,
        // kind=exact`) so Grafana 11+ surfaces it inline. We verify
        // the router-path dispatch preserves that mirroring rather
        // than stripping the envelope on its way through.
        struct ExactStub;
        #[async_trait]
        impl QueryEngine for ExactStub {
            async fn execute(&self, _query: &str) -> Result<QueryResult, EngineError> {
                use crate::storage_engines::sketch_db::accuracy::{
                    AccuracyEnvelope, AccuracyProfile,
                };
                Ok(QueryResult::vector(Vec::new(), 0)
                    .with_accuracy(AccuracyEnvelope::single(AccuracyProfile::exact())))
            }
            fn capabilities(&self) -> EngineCapabilities {
                EngineCapabilities {
                    data_source_id: StorageBackend::SketchStore.data_source_id(),
                    storage_backend: StorageBackend::SketchStore,
                    supports_streams_above_bytes: 1024 * 1024,
                }
            }
        }
        let server_port = setup_test_server_with_router(
            StorageBackend::DoubleWrite,
            vec![Arc::new(ExactStub) as Arc<dyn QueryEngine>],
        )
        .await;
        let client = Client::new();
        let resp = client
            .get(format!("http://127.0.0.1:{server_port}/api/v1/query"))
            .query(&[("query", "sum_over_time(foo[1h])"), ("time", "1700000000")])
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());
        let body: serde_json::Value = resp.json().await.unwrap();
        // Both the `data_source` info-line AND the
        // `accuracy: ε=..., δ=..., kind=exact` summary must land on
        // the response — proving the router-path dispatch preserves
        // the engine's wire annotations.
        assert_data_source(&body, "asap_query");
        let infos = body["infos"].as_array().expect("infos array");
        assert!(
            infos
                .iter()
                .any(|v| v.as_str().unwrap_or("").contains("kind=exact")),
            "exact-accuracy summary must be present in infos; got {infos:?}",
        );
        // The structured `accuracy` field also round-trips.
        let accuracy = body
            .get("accuracy")
            .expect("accuracy field must round-trip on router path");
        assert_eq!(accuracy["epsilon"], 0.0);
        assert_eq!(accuracy["delta"], 0.0);
    }

    #[tokio::test]
    async fn http_router_serves_double_write_via_warm_head() {
        // Step-1 of the JSONL deprecation deleted the
        // `ColdJsonlFallback` last-resort slot; the surviving
        // failover surface is ASAP-tier sketch ↔ Gorilla-S3 archive.
        // The HTTP handler dispatches with default
        // `(Statistic::Sum, AccuracyTarget::Epsilon(_))`, so for a
        // `DoubleWrite` metric the compatibility list is
        // `[SketchStore, GorillaObjectStore]` and the ASAP-tier
        // mock answers first. The archive must NOT be hit (no
        // failover needed when the head succeeds).
        let (warm_ok, warm_calls) =
            MockQueryEngine::new(StorageBackend::SketchStore, MockOutcome::OkEmpty);
        let (archive, archive_calls) =
            MockQueryEngine::new(StorageBackend::DoubleWrite, MockOutcome::Backend);
        let server_port = setup_test_server_with_router(
            StorageBackend::DoubleWrite,
            vec![
                warm_ok as Arc<dyn QueryEngine>,
                archive as Arc<dyn QueryEngine>,
            ],
        )
        .await;
        let client = Client::new();
        let resp = client
            .get(format!("http://127.0.0.1:{server_port}/api/v1/query"))
            .query(&[("query", "sum_over_time(foo[5m])"), ("time", "1700000000")])
            .send()
            .await
            .unwrap();
        assert!(
            resp.status().is_success(),
            "double-write must answer 2xx; got {}",
            resp.status()
        );
        assert_eq!(warm_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            archive_calls.load(Ordering::SeqCst),
            0,
            "archive must not run when the ASAP-tier head answers cleanly",
        );
    }

    // ── Issue #46 production-path coverage: BackendStorageRouting ─────────────
    //
    // The tests above (e.g. `http_routes_archive_metric_to_gorilla_engine`)
    // mock the routing decision by pinning `streaming_cfg.storage_backend
    // = GorillaObjectStore` directly. That proves the dispatch BRANCH is
    // wired, but not the production code path — in real deploys the
    // streaming-config YAML loader drops `storage_backend` (it always
    // defaults to `SketchStore`), so the issue-46 v2 demo's queries
    // never reached the EngineRouter. The tests below exercise the
    // **production path** end-to-end: streaming config stays default,
    // a per-metric `BackendStorageRouting` table is loaded at startup
    // (mirroring `--backend-storage-routing` on `precompute_engine`),
    // and the handler must consult the table on every request.

    #[tokio::test]
    async fn http_production_path_routes_archive_metric_via_routing_table() {
        // Production path: streaming-config single axis stays on
        // `SketchStore` (the YAML loader's default), but the
        // per-metric routing table flips `http_requests_total` to
        // a non-default axis. The handler must extract the metric name
        // from the PromQL AST, look it up, and dispatch through the
        // EngineRouter. Since #746 that archive spelling resolves to the
        // sketch tier, so the response carries `data_source: asap_query`.
        let mut metrics = std::collections::HashMap::new();
        metrics.insert(
            "http_requests_total".to_string(),
            StorageBackend::DoubleWrite,
        );
        let routing = crate::storage_engines::types::BackendStorageRouting::new_from_single_targets(
            StorageBackend::SketchStore,
            metrics,
        );
        let (gorilla, gorilla_calls) =
            MockQueryEngine::new(StorageBackend::SketchStore, MockOutcome::OkEmpty);
        let server_port =
            setup_test_server_with_routing_table(routing, vec![gorilla as Arc<dyn QueryEngine>])
                .await;
        let client = Client::new();
        let resp = client
            .get(format!("http://127.0.0.1:{server_port}/api/v1/query"))
            .query(&[
                ("query", "count(http_requests_total)"),
                ("time", "1700000000"),
            ])
            .send()
            .await
            .expect("Failed to send request");
        assert!(
            resp.status().is_success(),
            "production-path archive dispatch must return 2xx; got {}",
            resp.status(),
        );
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_data_source(&body, "asap_query");
        assert_eq!(
            gorilla_calls.load(Ordering::SeqCst),
            1,
            "the sketch-tier engine must be hit exactly once on the production path",
        );
    }

    #[tokio::test]
    async fn http_production_path_unlisted_metric_falls_back_to_asap_tier() {
        // The same routing table only overrides `http_requests_total`;
        // a query against a different metric must take the ASAP-tier
        // direct-dispatch path (no `EngineRouter` round-trip).
        let mut metrics = std::collections::HashMap::new();
        metrics.insert(
            "http_requests_total".to_string(),
            StorageBackend::DoubleWrite,
        );
        let routing = crate::storage_engines::types::BackendStorageRouting::new_from_single_targets(
            StorageBackend::SketchStore,
            metrics,
        );
        let server_port = setup_test_server_with_routing_table(routing, Vec::new()).await;
        let client = Client::new();
        let resp = client
            .get(format!("http://127.0.0.1:{server_port}/api/v1/query"))
            .query(&[
                ("query", "sum_over_time(some_other_metric[5m])"),
                ("time", "1700000000"),
            ])
            .send()
            .await
            .expect("Failed to send request");
        assert!(
            resp.status().is_success(),
            "ASAP-tier fallback must return 2xx; got {}",
            resp.status(),
        );
        let body: serde_json::Value = resp.json().await.unwrap();
        // Empty local fixtures exercise routing, not successful execution.
        assert_eq!(
            body,
            serde_json::json!({"status":"error", "data":null,
            "errorType":"bad_data", "error":"No result for query"})
        );
    }

    #[tokio::test]
    async fn http_production_path_default_axis_routes_all_metrics() {
        // Routing table with no per-metric overrides but a non-default
        // top-level non-sketch `default` — every metric must route
        // through the router, which lands them on the sketch tier since
        // #746 removed the archive engine.
        let routing = crate::storage_engines::types::BackendStorageRouting::new_from_single_targets(
            StorageBackend::DoubleWrite,
            std::collections::HashMap::new(),
        );
        let (gorilla, gorilla_calls) =
            MockQueryEngine::new(StorageBackend::SketchStore, MockOutcome::OkEmpty);
        let server_port =
            setup_test_server_with_routing_table(routing, vec![gorilla as Arc<dyn QueryEngine>])
                .await;
        let client = Client::new();
        let resp = client
            .get(format!("http://127.0.0.1:{server_port}/api/v1/query"))
            .query(&[
                ("query", "count(any_metric_at_all)"),
                ("time", "1700000000"),
            ])
            .send()
            .await
            .expect("Failed to send request");
        assert!(resp.status().is_success());
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_data_source(&body, "asap_query");
        assert_eq!(gorilla_calls.load(Ordering::SeqCst), 1);
    }

    // ── v7 dual-routing production-path coverage ──────────────────────────────
    //
    // v7 lets one metric fan out to multiple `(backend,
    // applies_to_query_shape)` targets. The two tests below mirror
    // `http_production_path_routes_archive_metric_via_routing_table`
    // — same setup, but the routing table has TWO targets for
    // `http_requests_total`: a default ASAP-tier slot and a
    // cold-archive slot scoped to `[count, topk, rate_post_hoc]`.
    // A `count(...)` query must land on the archive; a
    // `quantile_over_time(...)` query must land on the ASAP tier.

    #[tokio::test]
    async fn http_v7_dual_routing_count_lands_on_archive() {
        use crate::storage_engines::types::{
            BackendStorageRouting, QueryOperatorShape, RoutingTarget,
        };
        let mut metrics = std::collections::HashMap::new();
        metrics.insert(
            "http_requests_total".to_string(),
            vec![
                RoutingTarget::always(StorageBackend::SketchStore),
                RoutingTarget::for_shapes(
                    StorageBackend::DoubleWrite,
                    vec![
                        QueryOperatorShape::Count,
                        QueryOperatorShape::Topk,
                        QueryOperatorShape::RatePostHoc,
                    ],
                ),
            ],
        );
        let routing = BackendStorageRouting::new(StorageBackend::SketchStore, metrics);

        let (gorilla, gorilla_calls) =
            MockQueryEngine::new(StorageBackend::SketchStore, MockOutcome::OkEmpty);
        let server_port =
            setup_test_server_with_routing_table(routing, vec![gorilla as Arc<dyn QueryEngine>])
                .await;

        let client = Client::new();
        let resp = client
            .get(format!("http://127.0.0.1:{server_port}/api/v1/query"))
            .query(&[
                ("query", "count(http_requests_total{service=\"payments\"})"),
                ("time", "1700000000"),
            ])
            .send()
            .await
            .expect("Failed to send request");
        assert!(
            resp.status().is_success(),
            "v7 dual-routing: count must dispatch and return 2xx; got {}",
            resp.status()
        );
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_data_source(&body, "asap_query");
        assert_eq!(
            gorilla_calls.load(Ordering::SeqCst),
            1,
            "v7 dual-routing: the archive-claimed shape still dispatches \
             through the router, now onto the sketch tier",
        );
    }

    #[tokio::test]
    async fn http_v7_dual_routing_quantile_stays_on_asap_tier() {
        use crate::storage_engines::types::{
            BackendStorageRouting, QueryOperatorShape, RoutingTarget,
        };
        let mut metrics = std::collections::HashMap::new();
        metrics.insert(
            "http_requests_total".to_string(),
            vec![
                RoutingTarget::always(StorageBackend::SketchStore),
                RoutingTarget::for_shapes(
                    StorageBackend::DoubleWrite,
                    vec![
                        QueryOperatorShape::Count,
                        QueryOperatorShape::Topk,
                        QueryOperatorShape::RatePostHoc,
                    ],
                ),
            ],
        );
        let routing = BackendStorageRouting::new(StorageBackend::SketchStore, metrics);

        // Register a Gorilla mock so a misroute would surface as a
        // failed assertion rather than a silent fall-through. The
        // mock starts with 0 calls; a quantile must NOT touch it.
        let (gorilla, gorilla_calls) =
            MockQueryEngine::new(StorageBackend::SketchStore, MockOutcome::OkEmpty);
        let server_port =
            setup_test_server_with_routing_table(routing, vec![gorilla as Arc<dyn QueryEngine>])
                .await;

        let client = Client::new();
        let resp = client
            .get(format!("http://127.0.0.1:{server_port}/api/v1/query"))
            .query(&[
                ("query", "quantile_over_time(0.99, http_requests_total[1m])"),
                ("time", "1700000000"),
            ])
            .send()
            .await
            .expect("Failed to send request");
        // Warm tier path returns 2xx with `data_source: asap_query`
        // (the ASAPQueryEngine returns None for this unconfigured
        // metric, but the handler still annotates the wire response
        // with the ASAP-tier source).
        assert!(
            resp.status().is_success(),
            "v7 dual-routing: quantile must dispatch and return 2xx; got {}",
            resp.status()
        );
        let body: serde_json::Value = resp.json().await.unwrap();
        // Empty local fixtures exercise routing, not successful execution.
        assert_eq!(
            body,
            serde_json::json!({"status":"error", "data":null,
            "errorType":"bad_data", "error":"No result for query"})
        );
        assert_eq!(
            gorilla_calls.load(Ordering::SeqCst),
            0,
            "v7 dual-routing: quantile must NOT hit the archive engine",
        );
    }

    // ── Phase-6 Fix 1: per-query engine override ─────────────────────────
    //
    // The accuracy reducer asks the same PromQL against both the warm
    // sketch and the Gorilla archive on MinIO so it can compute
    // apples-to-apples relative error. This requires a way to bypass
    // the per-metric `BackendStorageRouting` lookup and dispatch
    // straight to a named engine. Two surfaces are exposed:
    //
    // * `X-ASAP-Engine: <data_source_id>` request header (preferred)
    // * `?engine=<data_source_id>` query / form param (fallback)
    //
    // Both flip the dispatcher to `process_via_named_engine`.

    /// Header override flips a `quantile_over_time` query — which the
    /// v7 dual-routing table sends to the warm sketch — over to the
    /// Gorilla archive engine. Proves the override bypasses the
    /// shape-classifier and dispatches to the explicitly named engine.
    #[tokio::test]
    async fn http_engine_override_header_routes_to_named_engine() {
        use crate::storage_engines::types::{
            BackendStorageRouting, QueryOperatorShape, RoutingTarget,
        };

        let (gorilla, gorilla_calls) =
            MockQueryEngine::new(StorageBackend::DoubleWrite, MockOutcome::OkEmpty);

        // Dual-routing for `metric_warm`: quantiles → warm, count →
        // archive. Without the header the test query routes to warm.
        let mut metrics = std::collections::HashMap::new();
        metrics.insert(
            "metric_warm".to_string(),
            vec![
                RoutingTarget::for_shapes(
                    StorageBackend::SketchStore,
                    vec![QueryOperatorShape::Quantile],
                ),
                RoutingTarget::for_shapes(
                    StorageBackend::DoubleWrite,
                    vec![QueryOperatorShape::Count],
                ),
            ],
        );
        let routing = BackendStorageRouting::new(StorageBackend::SketchStore, metrics);
        let server_port =
            setup_test_server_with_routing_table(routing, vec![gorilla as Arc<dyn QueryEngine>])
                .await;

        let client = Client::new();
        // Ask a quantile query (default routing → warm) but override
        // to archive via the header.
        let resp = client
            .get(format!("http://127.0.0.1:{server_port}/api/v1/query"))
            .header(ENGINE_OVERRIDE_HEADER, "double_write")
            .query(&[
                ("query", "quantile_over_time(0.5, metric_warm[1m])"),
                ("time", "1700000000"),
            ])
            .send()
            .await
            .expect("Failed to send request");
        assert!(
            resp.status().is_success(),
            "header-override dispatch must return 2xx; got {}",
            resp.status()
        );
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_data_source(&body, "double_write");
        assert_eq!(
            gorilla_calls.load(Ordering::SeqCst),
            1,
            "header override must dispatch to the named engine exactly once",
        );
    }

    /// Without the override header the same query takes the default
    /// routing-table path. Proves the override is opt-in and
    /// backwards-compatible.
    #[tokio::test]
    async fn http_engine_override_missing_uses_default_routing() {
        use crate::storage_engines::types::{
            BackendStorageRouting, QueryOperatorShape, RoutingTarget,
        };

        let (gorilla, gorilla_calls) =
            MockQueryEngine::new(StorageBackend::DoubleWrite, MockOutcome::OkEmpty);

        let mut metrics = std::collections::HashMap::new();
        metrics.insert(
            "metric_warm".to_string(),
            vec![
                RoutingTarget::for_shapes(
                    StorageBackend::SketchStore,
                    vec![QueryOperatorShape::Quantile],
                ),
                RoutingTarget::for_shapes(
                    StorageBackend::DoubleWrite,
                    vec![QueryOperatorShape::Count],
                ),
            ],
        );
        let routing = BackendStorageRouting::new(StorageBackend::SketchStore, metrics);
        let server_port =
            setup_test_server_with_routing_table(routing, vec![gorilla as Arc<dyn QueryEngine>])
                .await;

        let client = Client::new();
        // No override → quantile shape routes to ASAP tier.
        let resp = client
            .get(format!("http://127.0.0.1:{server_port}/api/v1/query"))
            .query(&[
                ("query", "quantile_over_time(0.5, metric_warm[1m])"),
                ("time", "1700000000"),
            ])
            .send()
            .await
            .expect("Failed to send request");
        assert!(resp.status().is_success(), "default routing must still 2xx");
        let body: serde_json::Value = resp.json().await.unwrap();
        // Empty local fixtures exercise routing, not successful execution.
        assert_eq!(
            body,
            serde_json::json!({"status":"error", "data":null,
            "errorType":"bad_data", "error":"No result for query"})
        );
        assert_eq!(
            gorilla_calls.load(Ordering::SeqCst),
            0,
            "no override → archive engine must NOT be hit",
        );
    }

    /// Query-param fallback: `?engine=double_write` overrides the
    /// routing table when the header is absent.
    #[tokio::test]
    async fn http_engine_override_query_param_routes_to_named_engine() {
        let (gorilla, gorilla_calls) =
            MockQueryEngine::new(StorageBackend::DoubleWrite, MockOutcome::OkEmpty);

        let server_port = setup_test_server_with_router(
            StorageBackend::SketchStore,
            vec![gorilla as Arc<dyn QueryEngine>],
        )
        .await;

        let client = Client::new();
        let resp = client
            .get(format!("http://127.0.0.1:{server_port}/api/v1/query"))
            .query(&[
                ("query", "sum_over_time(foo[5m])"),
                ("time", "1700000000"),
                (ENGINE_OVERRIDE_QUERY_PARAM, "double_write"),
            ])
            .send()
            .await
            .expect("Failed to send request");
        assert!(resp.status().is_success(), "query-param override must 2xx");
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_data_source(&body, "double_write");
        assert_eq!(
            gorilla_calls.load(Ordering::SeqCst),
            1,
            "query-param override must hit the archive engine exactly once",
        );
    }

    /// Unknown engine id returns 400 with a useful error body listing
    /// the registered engines. Lets `accuracy_reduce.py` distinguish
    /// "engine not deployed" (config bug) from "archive miss" (the
    /// chunk hasn't landed yet — still 200 with an empty result).
    #[tokio::test]
    async fn http_engine_override_unknown_id_returns_400() {
        let server_port =
            setup_test_server_with_router(StorageBackend::SketchStore, Vec::new()).await;

        let client = Client::new();
        let resp = client
            .get(format!("http://127.0.0.1:{server_port}/api/v1/query"))
            .header(ENGINE_OVERRIDE_HEADER, "does_not_exist")
            .query(&[("query", "sum_over_time(foo[5m])"), ("time", "1700000000")])
            .send()
            .await
            .expect("Failed to send request");
        assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
        let body: serde_json::Value = resp.json().await.unwrap();
        let err = body.get("error").and_then(|v| v.as_str()).unwrap_or("");
        assert!(
            err.contains("does_not_exist"),
            "error must name the bad id; got {err}",
        );
        assert!(
            err.contains("asap_query"),
            "error must list registered engines; got {err}",
        );
    }

    /// POST + form-encoded body works the same way: header still
    /// wins, body's `engine=` is the fallback.
    #[tokio::test]
    async fn http_engine_override_post_header_routes_to_named_engine() {
        let (gorilla, gorilla_calls) =
            MockQueryEngine::new(StorageBackend::DoubleWrite, MockOutcome::OkEmpty);
        let server_port = setup_test_server_with_router(
            StorageBackend::SketchStore,
            vec![gorilla as Arc<dyn QueryEngine>],
        )
        .await;

        let client = Client::new();
        let form_body = "query=sum_over_time(foo%5B5m%5D)&time=1700000000";
        let resp = client
            .post(format!("http://127.0.0.1:{server_port}/api/v1/query"))
            .header(ENGINE_OVERRIDE_HEADER, "double_write")
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(form_body)
            .send()
            .await
            .expect("Failed to send request");
        assert!(
            resp.status().is_success(),
            "POST header-override must 2xx; got {}",
            resp.status()
        );
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_data_source(&body, "double_write");
        assert_eq!(gorilla_calls.load(Ordering::SeqCst), 1);
    }

    // ── BackendStorageRouting hot-reload HTTP integration ────

    /// Standard test wiring for the `/api/v1/storage_routing` endpoint:
    /// install an empty hot-reload routing handle, hold the handle so
    /// the test can introspect the swap result.
    async fn setup_test_server_for_storage_routing() -> (
        u16,
        crate::query_engines::routing::HotReloadBackendStorageRouting,
    ) {
        use crate::query_engines::routing::HotReloadBackendStorageRouting;
        use crate::storage_engines::types::{StreamingConfig, StreamingConfigHandle};

        let adapter_config =
            AdapterConfig::prometheus_promql("http://127.0.0.1:9999".to_string(), false);
        let config = HttpServerConfig {
            port: 0,
            handle_http_requests: true,
            adapter_config,
        };
        let streaming_cfg = StreamingConfig::default();
        let streaming_arc = Arc::new(streaming_cfg);
        let hot_reload = StreamingConfigHandle::from_arc(streaming_arc.clone());
        let query_engine = Arc::new(ASAPQueryEngine::new(15000));
        let routing_handle = HotReloadBackendStorageRouting::empty();
        let server = HttpServer::new(
            config,
            query_engine,
            Arc::new(crate::storage_engines::sketch_db::index::SketchStore::new()),
        )
        .with_hot_reload_config(hot_reload)
        .with_hot_reload_backend_storage_routing(routing_handle.clone());
        let port = server.start_test_server().await.expect("start ok");
        (port, routing_handle)
    }

    fn fixture_routing_json() -> serde_json::Value {
        serde_json::json!({
            "default_engine": "asap_query",
            "metrics": [
                {
                    "name": "http_requests_total",
                    "targets": [
                        { "engine": "asap_query" },
                        {
                            "engine": "double_write",
                            "applies_to_query_shape": [
                                "histogram_quantile", "delta", "absent",
                                "rate_post_hoc", "count"
                            ]
                        }
                    ]
                }
            ]
        })
    }

    #[tokio::test]
    async fn storage_routing_post_swaps_table_atomically() {
        let (port, handle) = setup_test_server_for_storage_routing().await;
        // Initial table is empty.
        assert_eq!(handle.snapshot().len(), 0);

        let client = Client::new();
        let resp = client
            .post(format!("http://127.0.0.1:{port}/api/v1/storage_routing"))
            .header("Content-Type", "application/json")
            .body(fixture_routing_json().to_string())
            .send()
            .await
            .expect("send ok");

        assert!(
            resp.status().is_success(),
            "swap must 2xx; got {}",
            resp.status()
        );
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "success");
        assert_eq!(body["metrics_count"], 1);
        let returned_hash = body["table_hash"].as_str().unwrap().to_string();
        assert!(!returned_hash.is_empty(), "hash must be non-empty");

        // Snapshot now reflects the new table — and the hash matches
        // what the response advertised.
        let snap = handle.snapshot();
        assert_eq!(snap.len(), 1);
        let live_hash = crate::query_engines::routing::routing_table_hash(snap.as_ref());
        assert_eq!(live_hash, returned_hash, "live hash must match advertised");
    }

    #[tokio::test]
    async fn storage_routing_post_rejects_invalid_json() {
        let (port, handle) = setup_test_server_for_storage_routing().await;
        let client = Client::new();

        // Garbage body — not even valid JSON.
        let resp = client
            .post(format!("http://127.0.0.1:{port}/api/v1/storage_routing"))
            .header("Content-Type", "application/json")
            .body("not json {{")
            .send()
            .await
            .expect("send ok");
        assert_eq!(resp.status().as_u16(), 400, "garbage body must 400");

        // Valid JSON but invalid schema (unknown engine).
        let bad = serde_json::json!({
            "default_engine": "asap_query",
            "metrics": [{
                "name": "x",
                "targets": [{ "engine": "not_a_real_engine" }]
            }]
        });
        let resp = client
            .post(format!("http://127.0.0.1:{port}/api/v1/storage_routing"))
            .header("Content-Type", "application/json")
            .body(bad.to_string())
            .send()
            .await
            .expect("send ok");
        assert_eq!(
            resp.status().as_u16(),
            400,
            "schema-invalid body must 400; got {}",
            resp.status(),
        );

        // Confirm the table was NOT swapped (still empty).
        assert_eq!(handle.snapshot().len(), 0);
    }

    #[tokio::test]
    async fn storage_routing_get_returns_current_snapshot() {
        let (port, handle) = setup_test_server_for_storage_routing().await;
        // Pre-load the table.
        let new = crate::storage_engines::types::BackendStorageRouting::from_json_payload(
            &fixture_routing_json(),
        )
        .expect("parse");
        handle.swap(new);

        let client = Client::new();
        let resp = client
            .get(format!("http://127.0.0.1:{port}/api/v1/storage_routing"))
            .send()
            .await
            .expect("send ok");
        assert!(resp.status().is_success());
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "success");
        assert_eq!(body["default_engine"], "asap_query");
        assert_eq!(body["metrics_count"], 1);
        let snap_hash = body["table_hash"].as_str().unwrap();
        let live_hash =
            crate::query_engines::routing::routing_table_hash(handle.snapshot().as_ref());
        assert_eq!(snap_hash, live_hash);
    }

    /// Per-tenant push: a body with `tenant: tenant-a` lands in the
    /// `tenant-a` slot; the `default` tenant table is unaffected.
    /// Subsequent GETs with `X-ASAP-Tenant: tenant-a` see the new
    /// table; GETs with no header (default tenant) see an empty
    /// table.
    #[tokio::test]
    async fn storage_routing_post_per_tenant_isolates_tenants() {
        let (port, handle) = setup_test_server_for_storage_routing().await;
        let client = Client::new();

        // Push tenant-a's table.
        let mut tenant_a_body = fixture_routing_json();
        tenant_a_body["tenant"] = serde_json::json!("tenant-a");
        let resp = client
            .post(format!("http://127.0.0.1:{port}/api/v1/storage_routing"))
            .header("Content-Type", "application/json")
            .body(tenant_a_body.to_string())
            .send()
            .await
            .expect("send ok");
        assert!(resp.status().is_success());
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["tenant"], "tenant-a");
        assert_eq!(body["metrics_count"], 1);

        // Default tenant table is unchanged (still empty).
        let snap_default =
            handle.snapshot_for_tenant(crate::query_engines::routing::DEFAULT_TENANT);
        assert_eq!(snap_default.len(), 0);
        // Tenant-a table has the new entry.
        let snap_a = handle.snapshot_for_tenant("tenant-a");
        assert_eq!(snap_a.len(), 1);
    }

    /// Per-tenant push via `X-ASAP-Tenant` header (when the body
    /// leaves `tenant` implicit). The header acts as a fallback
    /// signal when the control plane emits a tenant-agnostic body.
    #[tokio::test]
    async fn storage_routing_post_per_tenant_via_header_when_body_implicit() {
        let (port, handle) = setup_test_server_for_storage_routing().await;
        let client = Client::new();

        // Body has no `tenant` field — control plane emit shape today.
        let resp = client
            .post(format!("http://127.0.0.1:{port}/api/v1/storage_routing"))
            .header("Content-Type", "application/json")
            .header("X-ASAP-Tenant", "tenant-b")
            .body(fixture_routing_json().to_string())
            .send()
            .await
            .expect("send ok");
        assert!(resp.status().is_success());
        let body: serde_json::Value = resp.json().await.unwrap();
        // The header steered the swap to tenant-b.
        assert_eq!(body["tenant"], "tenant-b");
        let snap_b = handle.snapshot_for_tenant("tenant-b");
        assert_eq!(snap_b.len(), 1);
        // Default tenant is still empty.
        let snap_default =
            handle.snapshot_for_tenant(crate::query_engines::routing::DEFAULT_TENANT);
        assert_eq!(snap_default.len(), 0);
    }

    /// Per-tenant push when neither the body's `tenant` field nor
    /// the `X-ASAP-Tenant` header are set: the swap lands in the
    /// `default` tenant slot — preserves the legacy single-tenant
    /// contract for existing control planes that haven't been updated
    /// yet.
    #[tokio::test]
    async fn storage_routing_post_no_tenant_falls_back_to_default() {
        let (port, handle) = setup_test_server_for_storage_routing().await;
        let client = Client::new();
        let resp = client
            .post(format!("http://127.0.0.1:{port}/api/v1/storage_routing"))
            .header("Content-Type", "application/json")
            .body(fixture_routing_json().to_string())
            .send()
            .await
            .expect("send ok");
        assert!(resp.status().is_success());
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(
            body["tenant"],
            crate::query_engines::routing::DEFAULT_TENANT
        );
        let snap_default =
            handle.snapshot_for_tenant(crate::query_engines::routing::DEFAULT_TENANT);
        assert_eq!(snap_default.len(), 1);
    }

    /// GET reports the tenant scope inferred from the request's
    /// `X-ASAP-Tenant` header. Operators can issue
    /// `curl -H 'X-ASAP-Tenant: tenant-a' /api/v1/storage_routing`
    /// to inspect that one tenant's table; the `tenants` field
    /// always lists every registered tenant for fleet-level
    /// diagnostics.
    #[tokio::test]
    async fn storage_routing_get_per_tenant_lists_all_tenants() {
        let (port, _handle) = setup_test_server_for_storage_routing().await;
        let client = Client::new();
        // Push two tenants.
        for tenant in ["tenant-a", "tenant-b"] {
            let mut body = fixture_routing_json();
            body["tenant"] = serde_json::json!(tenant);
            let _ = client
                .post(format!("http://127.0.0.1:{port}/api/v1/storage_routing"))
                .header("Content-Type", "application/json")
                .body(body.to_string())
                .send()
                .await
                .unwrap();
        }
        // GET tenant-a's view.
        let resp = client
            .get(format!("http://127.0.0.1:{port}/api/v1/storage_routing"))
            .header("X-ASAP-Tenant", "tenant-a")
            .send()
            .await
            .expect("send ok");
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["tenant"], "tenant-a");
        assert_eq!(body["metrics_count"], 1);
        let tenants = body["tenants"].as_array().expect("tenants array");
        let names: Vec<String> = tenants
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
        assert!(names.contains(&"tenant-a".to_string()));
        assert!(names.contains(&"tenant-b".to_string()));
    }

    #[tokio::test]
    async fn storage_routing_swap_observed_by_subsequent_query_dispatch() {
        // End-to-end production-path test: POST a routing table, then
        // issue a `count(http_requests_total)` query. The handler must
        // see the freshly-swapped table and route the query through
        // the EngineRouter. We can't easily assert the response engine
        // without setting up a Gorilla mock, but we can verify the
        // swap landed by GETting the hash — that's the contract the
        // control plane relies on.
        let (port, handle) = setup_test_server_for_storage_routing().await;
        let client = Client::new();

        let resp = client
            .post(format!("http://127.0.0.1:{port}/api/v1/storage_routing"))
            .header("Content-Type", "application/json")
            .body(fixture_routing_json().to_string())
            .send()
            .await
            .expect("send ok");
        assert!(resp.status().is_success());

        // `lookup_with_shape` must reflect the swapped contents on
        // the very next read.
        let snap = handle.snapshot();
        assert_eq!(
            snap.lookup_with_shape(
                "http_requests_total",
                crate::storage_engines::types::QueryOperatorShape::Count,
            ),
            StorageBackend::DoubleWrite,
        );
        assert_eq!(
            snap.lookup_with_shape(
                "http_requests_total",
                crate::storage_engines::types::QueryOperatorShape::Quantile,
            ),
            StorageBackend::SketchStore,
        );
    }

    /// Build an `HttpServer` whose router holds the supplied engines.
    async fn setup_test_server_with_named_router(
        metric_storage_backend: StorageBackend,
        engines: Vec<Arc<dyn QueryEngine>>,
    ) -> u16 {
        let adapter_config =
            AdapterConfig::prometheus_promql("http://127.0.0.1:9999".to_string(), false);
        let config = HttpServerConfig {
            port: 0,
            handle_http_requests: true,
            adapter_config,
        };
        let streaming_cfg =
            StreamingConfig::with_storage_backend(Default::default(), metric_storage_backend);
        let streaming_arc = Arc::new(streaming_cfg);
        let hot_reload = StreamingConfigHandle::from_arc(streaming_arc.clone());
        let query_engine = Arc::new(ASAPQueryEngine::new(15000));
        let mut server = HttpServer::new(
            config,
            query_engine,
            Arc::new(crate::storage_engines::sketch_db::index::SketchStore::new()),
        )
        .with_hot_reload_config(hot_reload);
        for engine in engines {
            server = server.with_query_engine(engine);
        }
        server
            .start_test_server()
            .await
            .expect("Failed to start test server")
    }

    // ── Issue #46 ⑥ — freshness-probe last-value cache ────────────
    //
    // The MVP demo's freshness criterion polls
    // `last_over_time(http_freshness_probe_warm[10s])` against the
    // backend's HTTP query endpoint at 10 Hz. The cold-tier flush
    // gap (gorillas3 → 60 s TSDB block → Thanos sync) leaves a 10 s
    // lookback window empty, so the OTLP receiver captures the
    // latest probe sample in a `FreshnessProbeCache` and the HTTP
    // handler answers the matching query shape from RAM. These
    // tests pin the contract: a recorded sample inside the lookback
    // window comes back as a single-element instant vector with the
    // counter value the producer encoded, and a stale sample falls
    // through (returns no result) without crashing the handler.

    /// Spin up an `HttpServer` wired to a fresh `FreshnessProbeCache`
    /// and return both. The cache is shared with the server so the
    /// test can pre-populate it with a synthetic sample before
    /// hitting `/api/v1/query`. The router holds no cold-archive
    /// engine; the freshness probe short-circuit must answer
    /// without ever consulting the cold tier.
    async fn setup_test_server_with_probe_cache(
    ) -> (u16, Arc<crate::query_engines::routing::FreshnessProbeCache>) {
        let adapter_config =
            AdapterConfig::prometheus_promql("http://127.0.0.1:9999".to_string(), false);
        let config = HttpServerConfig {
            port: 0,
            handle_http_requests: true,
            adapter_config,
        };
        let streaming_arc = Arc::new(StreamingConfig::default());
        let query_engine = Arc::new(ASAPQueryEngine::new(15000));
        let cache = Arc::new(crate::query_engines::routing::FreshnessProbeCache::new());
        let server = HttpServer::new(
            config,
            query_engine,
            Arc::new(crate::storage_engines::sketch_db::index::SketchStore::new()),
        )
        .with_probe_cache(cache.clone());
        let port = server
            .start_test_server()
            .await
            .expect("Failed to start test server");
        (port, cache)
    }

    #[tokio::test]
    async fn freshness_probe_last_over_time_answers_from_cache() {
        let (port, cache) = setup_test_server_with_probe_cache().await;

        // Synthetic sample: probe encodes its emission unix_ms as the
        // counter value (matches `deploy/fake-exporter/probes.go`).
        // Record the sample at "now" so the 10 s lookback hits.
        let now_ms = crate::query_engines::routing::freshness_probe_now_ms();
        let probe_value_ms = now_ms - 50; // sample emitted 50 ms ago
        cache.record(
            "http_freshness_probe_warm",
            probe_value_ms,
            probe_value_ms as f64,
        );

        let client = Client::new();
        let resp = client
            .get(format!("http://127.0.0.1:{port}/api/v1/query"))
            .query(&[("query", "last_over_time(http_freshness_probe_warm[10s])")])
            .send()
            .await
            .expect("Failed to send request");
        assert!(
            resp.status().is_success(),
            "freshness-probe short-circuit must return 2xx; got {}",
            resp.status(),
        );
        let body: serde_json::Value = resp.json().await.expect("Failed to parse JSON");
        assert_eq!(body["status"], "success", "expected status=success: {body}");
        let result = &body["data"]["result"];
        assert!(
            result.is_array() && !result.as_array().unwrap().is_empty(),
            "expected non-empty result vector; got {body}",
        );
        // The element's value (string-encoded float, Prometheus-wire
        // format) should be the cumulative counter the producer
        // encoded — i.e. the unix_ms of the last emission.
        let value_str = result[0]["value"][1]
            .as_str()
            .expect("instant-vector value must be a string");
        let parsed: i64 = value_str.parse().expect("value must parse as integer");
        assert_eq!(
            parsed, probe_value_ms,
            "last_over_time must return the cumulative counter value (= unix_ms of emission)",
        );
    }

    #[tokio::test]
    async fn freshness_probe_last_over_time_falls_through_on_stale_sample() {
        let (port, cache) = setup_test_server_with_probe_cache().await;

        // Sample is older than the lookback window — the cache lookup
        // returns None and the handler falls through to the normal
        // routing path. The default routing landed on
        // `SketchStore`, which the test's empty `ASAPQueryEngine`
        // can't answer, so the response is a structured error or an
        // empty-result success — anything but a crash. The test
        // pins the no-crash contract; the exact error surface is
        // covered by the routing-table tests.
        let now_ms = crate::query_engines::routing::freshness_probe_now_ms();
        let stale_ts = now_ms - 60_000; // 60 s old, outside [now-10s, now]
        cache.record("http_freshness_probe_warm", stale_ts, stale_ts as f64);

        let client = Client::new();
        let resp = client
            .get(format!("http://127.0.0.1:{port}/api/v1/query"))
            .query(&[("query", "last_over_time(http_freshness_probe_warm[10s])")])
            .send()
            .await
            .expect("Failed to send request");
        // The response either succeeds with an empty vector (cache
        // miss → fall through → ASAPQueryEngine no-data) or returns a
        // 4xx/5xx with a structured error. Either is fine as long as
        // the handler did not panic.
        let body: serde_json::Value = resp.json().await.expect("Failed to parse JSON");
        assert!(
            body.get("status").is_some(),
            "response must carry a status field; got {body}",
        );
    }

    #[test]
    fn parse_last_over_time_probe_recognises_canonical_shape() {
        // Canonical shape — `last_over_time(metric[range])`. Returns
        // `(metric_name, range_ms)`.
        let parsed =
            super::parse_last_over_time_probe("last_over_time(http_freshness_probe_warm[10s])")
                .expect("canonical last_over_time must parse");
        assert_eq!(parsed.0, "http_freshness_probe_warm");
        assert_eq!(parsed.1, 10_000);

        // Different range — millis are extracted from the matrix
        // selector, not hard-coded.
        let parsed =
            super::parse_last_over_time_probe("last_over_time(http_freshness_probe_archive[5m])")
                .expect("5m range must parse");
        assert_eq!(parsed.1, 5 * 60_000);
    }

    #[test]
    fn parse_last_over_time_probe_rejects_other_shapes() {
        // Bare vector selector — not a function call.
        assert_eq!(
            super::parse_last_over_time_probe("http_freshness_probe_warm"),
            None,
        );
        // Different function name.
        assert_eq!(
            super::parse_last_over_time_probe("rate(http_freshness_probe_warm[10s])"),
            None,
        );
        // Wrong arg count for last_over_time (which takes one matrix
        // selector).
        assert_eq!(super::parse_last_over_time_probe("last_over_time()"), None,);
        // Aggregation around the call — outermost shape isn't a
        // bare `last_over_time` call.
        assert_eq!(
            super::parse_last_over_time_probe(
                "topk(1, last_over_time(http_freshness_probe_warm[10s]))"
            ),
            None,
        );
        // Garbage PromQL.
        assert_eq!(super::parse_last_over_time_probe("not promql"), None);
    }

    #[tokio::test]
    async fn freshness_probe_short_circuit_ignores_non_probe_metrics() {
        let (port, cache) = setup_test_server_with_probe_cache().await;
        let now_ms = crate::query_engines::routing::freshness_probe_now_ms();
        cache.record("http_freshness_probe_warm", now_ms, now_ms as f64);

        // Different metric — must NOT be served from the cache (the
        // short-circuit checks the metric name prefix). The handler
        // should fall through to normal routing; whatever happens
        // there is the test of those paths, not of the short-circuit.
        let client = Client::new();
        let resp = client
            .get(format!("http://127.0.0.1:{port}/api/v1/query"))
            .query(&[("query", "last_over_time(http_requests_total[10s])")])
            .send()
            .await
            .expect("Failed to send request");
        let body: serde_json::Value = resp.json().await.expect("Failed to parse JSON");
        // The cache hit would have produced a non-empty result with
        // value `now_ms`. Fall-through paths return either an empty
        // vector or an error — neither carries our probe value, so
        // we negative-assert: the body must NOT contain the probe
        // value as a stringified counter.
        let body_str = body.to_string();
        assert!(
            !body_str.contains(&now_ms.to_string()),
            "non-probe metric must NOT be answered from the freshness cache; \
             saw probe value {now_ms} leaked into response: {body}",
        );
    }
}

/// Health check endpoint for DataCollector controller to verify backend is alive.
async fn handle_prometheus_remote_write(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some(receiver) = state.remote_write.as_ref() else {
        return (StatusCode::NOT_FOUND, "Remote Write is disabled").into_response();
    };
    let content_encoding = headers
        .get(axum::http::header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok());
    if !content_encoding.is_some_and(|value| value.eq_ignore_ascii_case("snappy")) {
        return (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Content-Encoding must be snappy",
        )
            .into_response();
    }
    if let Some(content_type) = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
    {
        if !content_type
            .split(';')
            .next()
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/x-protobuf"))
        {
            return (
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "Content-Type must be application/x-protobuf",
            )
                .into_response();
        }
    }
    if let Some(version) = headers
        .get("x-prometheus-remote-write-version")
        .and_then(|value| value.to_str().ok())
    {
        if version != "0.1.0" {
            return (
                StatusCode::BAD_REQUEST,
                "only Prometheus Remote Write v1 (0.1.0) is supported",
            )
                .into_response();
        }
    }

    match receiver.accept(&body) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(crate::drivers::ingest::prometheus_remote_write::RemoteWriteError::InputClosed) =>
            (StatusCode::CONFLICT, "finite input is closed").into_response(),
        Err(
            crate::drivers::ingest::prometheus_remote_write::RemoteWriteError::CompressedTooLarge(
                _,
            )
            | crate::drivers::ingest::prometheus_remote_write::RemoteWriteError::DecompressedTooLarge(
                _,
            )
            | crate::drivers::ingest::prometheus_remote_write::RemoteWriteError::TooManySeries {
                ..
            }
            | crate::drivers::ingest::prometheus_remote_write::RemoteWriteError::TooManySamples(
                _,
            ),
        ) => (StatusCode::PAYLOAD_TOO_LARGE, "Remote Write request exceeds configured limits")
            .into_response(),
        Err(
            error @ (crate::drivers::ingest::prometheus_remote_write::RemoteWriteError::Backpressure(
                _,
            )
            | crate::drivers::ingest::prometheus_remote_write::RemoteWriteError::DedupCapacity(_)
            | crate::drivers::ingest::prometheus_remote_write::RemoteWriteError::InactivePhysicalPlan),
        ) => (StatusCode::SERVICE_UNAVAILABLE, error.to_string()).into_response(),
        Err(error) => (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    }
}

async fn handle_health(State(state): State<AppState>) -> axum::response::Response {
    use axum::response::IntoResponse;

    if state.remote_write.is_some() {
        let Some(active) = state
            .active_physical_plan
            .as_ref()
            .map(|handle| handle.active_snapshot())
        else {
            return (StatusCode::SERVICE_UNAVAILABLE, "no active PhysicalPlan").into_response();
        };
        let now = unix_time_ms();
        let lifecycle_ready = active.plan_id() != 0
            && active.activation_unix_ms() <= now
            && active.expiry_unix_ms().is_none_or(|expiry| now < expiry);
        let ingest_ready = matches!(
            active.precompute_plan.ingest.protocol,
            asap_types::precompute_plan::IngestProtocol::PrometheusRemoteWriteV1
        ) && active.precompute_plan.ingest.endpoint_path == "/api/v1/write";
        if !lifecycle_ready || !ingest_ready {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "active PhysicalPlan is not ready for Remote Write",
            )
                .into_response();
        }
    }
    (StatusCode::OK, "ok").into_response()
}

/// Explicit end-of-input; this seals Remote Write for the lifetime of the process.
async fn handle_precompute_drain(State(state): State<AppState>) -> Response {
    let Some(receiver) = state.remote_write.as_ref() else {
        return (StatusCode::NOT_FOUND, "Remote Write is disabled").into_response();
    };
    match receiver.drain().await {
        Ok(()) => (
            StatusCode::OK,
            axum::Json(serde_json::json!({
                "status": "success", "input_closed": true, "complete": true
            })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(serde_json::json!({
                "status": "error", "input_closed": true, "complete": false, "error": error
            })),
        )
            .into_response(),
    }
}

/// Return list of metrics currently in the store.
async fn handle_store_metrics(State(state): State<AppState>) -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    // Per-sid first-seen timestamps are available without I/O.
    let timestamps = state.summary_store.earliest_timestamps_per_series_id();
    let body = serde_json::json!({
        "status": "success",
        "sid_count": timestamps.len(),
        "approx_resident_bytes": state.summary_store.approx_resident_bytes(),
        "earliest_timestamps_per_series_id": timestamps});
    (StatusCode::OK, axum::Json(body)).into_response()
}

// Streaming configuration endpoints: GET returns the active snapshot; POST
// parses a replacement and atomically publishes it to new snapshot readers.
// In-flight operations retain their existing snapshot.

async fn handle_get_streaming_config(State(state): State<AppState>) -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    let Some(handle) = state.hot_reload_config else {
        let body = serde_json::json!({
            "status": "error",
            "error": "hot-reload handle not attached; backend was built without HttpServer::with_hot_reload_config"});
        return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body)).into_response();
    };
    let snap = handle.snapshot();
    let body = serde_json::json!({
        "status": "success",
        "aggregation_count": snap.materializations_by_policy_fingerprint.len(),
        "aggregation_ids": snap.materializations_by_policy_fingerprint.keys().copied().collect::<Vec<_>>(),
        "streaming_config": &*snap});
    (StatusCode::OK, axum::Json(body)).into_response()
}

async fn handle_post_streaming_config(
    State(state): State<AppState>,
    body: axum::body::Bytes,
) -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use std::collections::HashSet;

    let Some(handle) = state.hot_reload_config else {
        let body = serde_json::json!({
            "status": "error",
            "error": "hot-reload handle not attached; backend was built without HttpServer::with_hot_reload_config"});
        return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body)).into_response();
    };

    let yaml_text = match std::str::from_utf8(&body) {
        Ok(s) => s,
        Err(e) => {
            let body = serde_json::json!({
                "status": "error",
                "error": format!("request body is not valid UTF-8: {e}")});
            return (StatusCode::BAD_REQUEST, axum::Json(body)).into_response();
        }
    };
    let yaml_value: serde_yaml::Value = match serde_yaml::from_str(yaml_text) {
        Ok(v) => v,
        Err(e) => {
            let body = serde_json::json!({
                "status": "error",
                "error": format!("YAML parse error: {e}")});
            return (StatusCode::BAD_REQUEST, axum::Json(body)).into_response();
        }
    };
    let new_config =
        match crate::storage_engines::types::StreamingConfig::from_yaml_data(&yaml_value) {
            Ok(c) => c,
            Err(e) => {
                let body = serde_json::json!({
                    "status": "error",
                    "error": format!("StreamingConfig build error: {e}")});
                return (StatusCode::BAD_REQUEST, axum::Json(body)).into_response();
            }
        };

    let new_ids: HashSet<u64> = new_config
        .materializations_by_policy_fingerprint
        .keys()
        .copied()
        .collect();
    let old_arc = handle.swap(new_config);
    let old_ids: HashSet<u64> = old_arc
        .materializations_by_policy_fingerprint
        .keys()
        .copied()
        .collect();
    let added: Vec<u64> = new_ids.difference(&old_ids).copied().collect();
    let removed: Vec<u64> = old_ids.difference(&new_ids).copied().collect();

    if !removed.is_empty() {
        warn!(
            "streaming-config hot-reload removed agg_ids {:?} — any in-flight \
             precompute worker groups for these ids will continue with their \
             construction-time config until they close naturally (phase 1 \
             limitation; see StreamingConfigHandle module doc)",
            removed
        );
    }

    // Schema retirement final cut: the sid catalog is the only
    // lifecycle registry. The legacy per-`agg_id` `SchemaRegistry` is
    // gone, so the swap handler now drives a single sid-level
    // reconcile (`reconcile_from_streaming_config`) which force-retires
    // any sid whose content signature no longer appears in the new
    // config. There is no "added" set: sids are minted lazily at the
    // first ingest write under the new config (see
    // `SketchStore::ingest_precompute_for_agg_config`).
    let snap = handle.snapshot();
    let sid_summary = crate::storage_engines::sketch_db::lifecycle::reconcile_from_streaming_config(
        state.summary_store.as_ref(),
        snap.as_ref(),
        crate::storage_engines::sketch_db::DEFAULT_RETIREMENT_RETENTION,
    );

    let body = serde_json::json!({
        "status": "success",
        "agg_ids_added": added,
        "agg_ids_removed": removed,
        "new_aggregation_count": new_ids.len(),
        "sids_retired": sid_summary.retired});
    (StatusCode::OK, axum::Json(body)).into_response()
}

pub use asap_types::plan_publication::PhysicalPlanInstallRequest;

/// Decode and cross-validate every backend view before it can become visible.
/// Used by both startup artifact loading and the staged HTTP install path.
pub fn validate_and_build_runtime_plan(
    request: PhysicalPlanInstallRequest,
    default_routing: Arc<crate::storage_engines::types::BackendStorageRouting>,
) -> Result<crate::storage_engines::types::RuntimePhysicalPlan, String> {
    use std::collections::BTreeSet;
    request
        .precompute_plan
        .validate_against_catalog(&request.summary_catalog)
        .map_err(|error| format!("PrecomputePlan catalog validation error: {error}"))?;
    request
        .transmission_plan
        .validate_against_catalog(&request.summary_catalog)
        .map_err(|error| format!("TransmissionPlan catalog validation error: {error}"))?;
    request
        .query_plan
        .validate_against_catalog(&request.summary_catalog)
        .map_err(|error| format!("QueryPlan catalog validation error: {error}"))?;
    for collector in &request.collector_plans {
        collector
            .validate_against_catalog(&request.summary_catalog)
            .map_err(|error| format!("CollectorPlan catalog validation error: {error}"))?;
    }
    for entry in request.query_plan.entries.values() {
        for binding in entry.materialization_bindings() {
            let materialization = request
                .precompute_plan
                .materializations
                .iter()
                .find(|config| config.policy_fingerprint() == binding.materialization.fingerprint())
                .ok_or_else(|| "query binding has no precompute definition".to_string())?;
            if binding.window_ms != materialization.stored_window_ms() {
                return Err(
                    "query physical pane duration differs from installed precompute definition"
                        .into(),
                );
            }
            if binding.pane_origin_ms != materialization.pane_origin_ms {
                return Err(
                    "query physical pane origin differs from installed precompute definition"
                        .into(),
                );
            }
            // `full_window_slide_ms` is `#[serde(default)]`, so a publication from an
            // older controller -- or one replayed from a stored artifact -- arrives as
            // `None` on a FullWindow materialization. Without this gate the readout
            // silently takes the overlap-merging path and counts observations twice,
            // which is exactly what the full-window binding exists to prevent.
            let full_window_slide_ms = matches!(
                materialization.window_layout,
                asap_types::WindowMaterializationLayout::FullWindow
            )
            .then_some(materialization.slide_interval.saturating_mul(1_000));
            if binding.full_window_slide_ms != full_window_slide_ms {
                return Err(
                    "query window layout differs from installed precompute definition".into(),
                );
            }
        }
    }
    let runtime_materializations = request
        .precompute_plan
        .runtime_materializations()
        .map_err(|error| format!("PrecomputePlan validation error: {error}"))?;
    request
        .transmission_plan
        .validate(&request.precompute_plan)
        .map_err(|error| format!("TransmissionPlan validation error: {error}"))?;
    let envelope = &request.precompute_plan.envelope;
    if request.query_plan.plan_id != envelope.plan_id
        || request.query_plan.plan_version != envelope.plan_version
        || request.transmission_plan.envelope != *envelope
    {
        return Err("physical subplans have different plan identity/version".into());
    }
    let streaming_config =
        crate::storage_engines::types::StreamingConfig::new(runtime_materializations);
    let typed_fps: BTreeSet<_> = streaming_config
        .materializations_by_policy_fingerprint
        .keys()
        .copied()
        .map(asap_types::PolicyFingerprint)
        .collect();
    request
        .query_plan
        .validate(&typed_fps)
        .map_err(|error| format!("QueryPlan validation error: {error}"))?;
    let storage_routing = match request.storage_routing.as_ref() {
        Some(value) => Arc::new(
            crate::storage_engines::types::BackendStorageRouting::from_json_payload(value)
                .map_err(|error| format!("BackendStorageRouting build error: {error:#}"))?,
        ),
        None => default_routing,
    };
    Ok(crate::storage_engines::types::RuntimePhysicalPlan {
        envelope: envelope.clone(),
        summary_catalog: Some(Arc::new(request.summary_catalog)),
        precompute_plan: request.precompute_plan,
        transmission_plan: request.transmission_plan,
        streaming_config: Arc::new(streaming_config),
        query_plan: Arc::new(request.query_plan),
        storage_routing,
    })
}

/// Validate and stage all backend views. Staging never changes query routing;
/// a separate activation request performs the single snapshot swap.
async fn handle_post_physical_plan(
    State(state): State<AppState>,
    axum::Json(request): axum::Json<PhysicalPlanInstallRequest>,
) -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    let Some(active_handle) = state.active_physical_plan.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(serde_json::json!({
                "status": "error", "error": "physical-plan hot-reload handles are not attached"
            })),
        )
            .into_response();
    };
    let Some(lifecycle) = state.physical_plan_lifecycle.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(serde_json::json!({
                "status": "error", "error": "physical-plan lifecycle is not attached"
            })),
        )
            .into_response();
    };
    let current = active_handle.active_snapshot();
    if current.transmission_plan.envelope.plan_id != 0 {
        if let Err(error) = current.transmission_plan.authorize_successor(
            &request.transmission_plan,
            &request.adaptation_evidence,
            unix_time_ms(),
        ) {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                axum::Json(serde_json::json!({
                    "status": "error",
                    "error": format!("runtime adaptation authorization error: {error}")
                })),
            )
                .into_response();
        }
    }
    let _guard = state.physical_plan_lock.lock().await;
    let active = match validate_and_build_runtime_plan(request, current.storage_routing.clone()) {
        Ok(active) => active,
        Err(error) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                axum::Json(serde_json::json!({"status": "error", "error": error})),
            )
                .into_response();
        }
    };
    if state.remote_write.is_some()
        && (active.plan_id() == 0
            || !matches!(
                active.precompute_plan.ingest.protocol,
                asap_types::precompute_plan::IngestProtocol::PrometheusRemoteWriteV1
            )
            || active.precompute_plan.ingest.endpoint_path != "/api/v1/write")
    {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            axum::Json(serde_json::json!({
                "status": "error",
                "error": "Remote Write listener requires a non-bootstrap prometheus_remote_write_v1 plan at /api/v1/write"
            })),
        )
            .into_response();
    }
    let plan_id = active.plan_id();
    let materialization_count = active.precompute_plan.materializations.len();
    let metricsql_query_count = active
        .query_plan
        .entries
        .values()
        .filter(|entry| entry.language == asap_types::query_plan::QueryLanguage::MetricsQl)
        .count();
    let clickhouse_plan_count = active
        .query_plan
        .entries
        .values()
        .filter(|entry| entry.language == asap_types::query_plan::QueryLanguage::ClickHouseSql)
        .count();
    let plan_version = active.plan_version();
    let now = unix_time_ms();
    if let Err(error) = lifecycle.stage(active, now) {
        return (
            StatusCode::CONFLICT,
            axum::Json(serde_json::json!({"status": "error", "error": error.to_string()})),
        )
            .into_response();
    }
    (
        StatusCode::ACCEPTED,
        axum::Json(serde_json::json!({
            "status": "staged", "plan_id": plan_id, "plan_version": plan_version,
            "materialization_count": materialization_count,
            "metricsql_query_count": metricsql_query_count,
            "clickhouse_plan_count": clickhouse_plan_count
        })),
    )
        .into_response()
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ActivatePhysicalPlanRequest {
    plan_id: u64,
    plan_version: u64,
}

async fn handle_activate_physical_plan(
    State(state): State<AppState>,
    axum::Json(request): axum::Json<ActivatePhysicalPlanRequest>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let (Some(lifecycle), Some(active_handle)) = (
        state.physical_plan_lifecycle.as_ref(),
        state.active_physical_plan.as_ref(),
    ) else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(serde_json::json!({
                "status": "error", "error": "physical-plan lifecycle is not attached"
            })),
        )
            .into_response();
    };
    let _guard = state.physical_plan_lock.lock().await;
    let store = Arc::clone(&state.summary_store);
    let remote_write = state.remote_write.clone();
    let old = match lifecycle.activate_with_prepare(
        request.plan_id,
        request.plan_version,
        unix_time_ms(),
        move |plan| match plan.summary_catalog.as_ref() {
            Some(catalog) => {
                store
                    .install_summary_catalog(Arc::clone(catalog))
                    .map_err(|error| format!("SummaryCatalog install error: {error}"))?;
                if let Some(receiver) = &remote_write {
                    receiver.install_erp_observation_generation(
                        catalog.reference().map_err(|error| error.to_string())?,
                    );
                }
                Ok(())
            }
            None => Err("authoritative SummaryCatalog is unavailable".to_string()),
        },
    ) {
        Ok(old) => old,
        Err(error) => {
            return (
                StatusCode::CONFLICT,
                axum::Json(serde_json::json!({
                    "status": "error", "error": error.to_string()
                })),
            )
                .into_response()
        }
    };
    let activated = active_handle.active_snapshot();
    let clickhouse_plan_count = activated
        .query_plan
        .entries
        .values()
        .filter(|entry| entry.language == asap_types::query_plan::QueryLanguage::ClickHouseSql)
        .count();
    if old.plan_id() != 0 {
        let draining_id = old.plan_id();
        let draining_version = old.plan_version();
        let lifecycle = lifecycle.clone();
        tokio::spawn(async move {
            loop {
                if Arc::strong_count(&old) == 1 {
                    lifecycle.mark_drained_plan_retired(draining_id, draining_version);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
    }
    let snap = activated.streaming_config.clone();
    let retired = crate::storage_engines::sketch_db::lifecycle::reconcile_from_streaming_config(
        state.summary_store.as_ref(),
        snap.as_ref(),
        crate::storage_engines::sketch_db::DEFAULT_RETIREMENT_RETENTION,
    );
    (
        StatusCode::OK,
        axum::Json(serde_json::json!({
            "status": "active", "plan_id": request.plan_id, "plan_version": request.plan_version,
            "sids_retired": retired.retired,
            "clickhouse_plan_count": clickhouse_plan_count
        })),
    )
        .into_response()
}

async fn handle_summary_inventory(State(state): State<AppState>) -> axum::response::Response {
    use axum::response::IntoResponse;
    let Some(active) = state
        .active_physical_plan
        .as_ref()
        .map(|handle| handle.active_snapshot())
    else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(
                serde_json::json!({"status":"error","error":"physical plan is unavailable"}),
            ),
        )
            .into_response();
    };
    if active.summary_catalog.is_none() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(serde_json::json!({"status":"error","error":"authoritative SummaryCatalog is unavailable"})),
        )
            .into_response();
    }
    let producers = active
        .precompute_plan
        .producers
        .iter()
        .map(|producer| {
            (
                asap_types::sds::SummaryDefinitionId::from(producer.materialization),
                producer.producer_id.clone(),
            )
        })
        .collect();
    let reporter = std::env::var("HOSTNAME").unwrap_or_else(|_| "asapquery-backend".into());
    match state.summary_store.observed_summary_inventory(
        &reporter,
        &reporter,
        &producers,
        active.plan_version(),
        unix_time_ms() as i64,
    ) {
        Ok(inventory) => axum::Json(inventory).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(serde_json::json!({"status":"error","error":error})),
        )
            .into_response(),
    }
}

async fn handle_discard_physical_plan(
    State(state): State<AppState>,
    axum::Json(request): axum::Json<ActivatePhysicalPlanRequest>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let Some(lifecycle) = state.physical_plan_lifecycle.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "physical-plan lifecycle is not attached",
        )
            .into_response();
    };
    match lifecycle.discard_staged(request.plan_id, request.plan_version) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => (StatusCode::CONFLICT, error.to_string()).into_response(),
    }
}

async fn handle_physical_plan_status(State(state): State<AppState>) -> axum::response::Response {
    use axum::response::IntoResponse;
    let Some(lifecycle) = state.physical_plan_lifecycle.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(serde_json::json!({
                "status": "error", "error": "physical-plan lifecycle is not attached"
            })),
        )
            .into_response();
    };
    let materializations = state
        .active_physical_plan
        .as_ref()
        .map(|active| active.materialization_statuses())
        .unwrap_or_default();
    (
        StatusCode::OK,
        axum::Json(serde_json::json!({
            "status": "success",
            "plans": lifecycle.statuses(),
            "materializations": materializations
        })),
    )
        .into_response()
}

fn unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ── BackendStorageRouting hot-reload endpoints ────────────

/// `GET /api/v1/storage_routing` — return a JSON snapshot of the
/// currently-active per-metric `BackendStorageRouting` table.
///
/// Useful for operators to confirm a control plane push landed with the
/// expected entries. Returns 503 when the backend wasn't built with a
/// routing-table handle (legacy deploys that loaded the YAML directly
/// can still hit `/api/v1/streaming-config` — this endpoint is for
/// the Phase α JSON path).
async fn handle_get_storage_routing(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    let Some(handle) = state.backend_storage_routing.as_ref() else {
        let body = serde_json::json!({
            "status": "error",
            "error": "routing handle not attached; backend was built without HttpServer::with_backend_storage_routing"});
        return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body)).into_response();
    };
    // Per-tenant scope: the GET reports the tenant inferred from the
    // request's `X-ASAP-Tenant` header (default `"default"`). The
    // `tenants` field lists every tenant id currently registered so
    // operators can spot-check the multi-tenant map without a
    // separate endpoint.
    let tenant = extract_tenant(&headers);
    let snap = handle.snapshot_for_tenant(&tenant);
    let body = serde_json::json!({
        "status": "success",
        "tenant": tenant,
        "default_engine": snap.default_backend().data_source_id(),
        "metrics_count": snap.len(),
        "table_hash": crate::query_engines::routing::routing_table_hash(snap.as_ref()),
        "tenants": handle.tenant_ids()});
    (StatusCode::OK, axum::Json(body)).into_response()
}

/// `POST /api/v1/storage_routing` — replace the per-metric routing
/// table from a control-plane-emitted JSON document.
///
/// Body shape — see
/// `control_plane/src/emit/stage_config.rs::emit_backend_storage_routing`
/// (or `BackendStorageRouting::from_json_payload` in this crate for
/// the matching parser). On 2xx the response body carries the new
/// table's hash and entry count so the control plane can verify the
/// installed bytes match what it pushed.
///
/// Errors:
/// * 400 — body is not valid UTF-8, not valid JSON, or the JSON
///   fails the schema check (unknown engine / empty targets / etc.).
/// * 503 — backend wasn't built with a routing-table handle.
///
/// The swap is atomic: in-flight queries either see the entire old
/// table or the entire new table; never a half-applied state. Mirrors
/// the existing `POST /api/v1/streaming-config` swap contract.
async fn handle_post_storage_routing(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    let Some(handle) = state.backend_storage_routing.as_ref() else {
        let body = serde_json::json!({
            "status": "error",
            "error": "routing handle not attached; backend was built without HttpServer::with_backend_storage_routing"});
        return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body)).into_response();
    };

    let json_text = match std::str::from_utf8(&body) {
        Ok(s) => s,
        Err(e) => {
            let body = serde_json::json!({
                "status": "error",
                "error": format!("request body is not valid UTF-8: {e}")});
            return (StatusCode::BAD_REQUEST, axum::Json(body)).into_response();
        }
    };
    let json_value: serde_json::Value = match serde_json::from_str(json_text) {
        Ok(v) => v,
        Err(e) => {
            let body = serde_json::json!({
                "status": "error",
                "error": format!("JSON parse error: {e}")});
            return (StatusCode::BAD_REQUEST, axum::Json(body)).into_response();
        }
    };
    let new_table = match crate::storage_engines::types::BackendStorageRouting::from_json_payload(
        &json_value,
    ) {
        Ok(t) => t,
        Err(e) => {
            let body = serde_json::json!({
                "status": "error",
                "error": format!("BackendStorageRouting build error: {e:#}")});
            return (StatusCode::BAD_REQUEST, axum::Json(body)).into_response();
        }
    };

    // Per-tenant push: the new table's tenant id is the source of
    // truth (the body's `tenant` field, defaulting to `default`).
    // The `X-ASAP-Tenant` header is honoured as a fallback when the
    // body left the tenant field implicit — it's the control plane's
    // primary signal for "which tenant am I pushing for".
    let tenant_from_body = new_table.tenant().to_string();
    let tenant = if tenant_from_body == crate::query_engines::routing::DEFAULT_TENANT {
        // Body left it implicit; honour the header.
        extract_tenant(&headers)
    } else {
        tenant_from_body
    };

    let entries = new_table.len();
    let hash = crate::query_engines::routing::routing_table_hash(&new_table);
    let _old = handle.swap_tenant(&tenant, new_table);
    info!(
        tenant = %tenant,
        entries,
        table_hash = %hash,
        "storage-routing JSON hot-reload swap completed (per-tenant)",
    );

    let body = serde_json::json!({
        "status": "success",
        "tenant": tenant,
        "metrics_count": entries,
        "table_hash": hash});
    (StatusCode::OK, axum::Json(body)).into_response()
}

/// Expose sid metadata at `/api/v1/db/schemas` for API compatibility.
/// Filter with `status=active|retired|expired|all` (default `all`).
async fn handle_get_schemas(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> axum::response::Response {
    use crate::storage_engines::sketch_db::AggStatus;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    let filter = params.get("status").map(String::as_str).unwrap_or("all");
    let allowed: &[AggStatus] = match filter {
        "active" => &[AggStatus::Active],
        "retired" => &[AggStatus::Retired],
        "expired" => &[AggStatus::Expired],
        "all" => &[AggStatus::Active, AggStatus::Retired, AggStatus::Expired],
        other => {
            let body = serde_json::json!({
            "status": "error",
            "error": format!(
                "unknown status filter '{other}'; expected one of active|retired|expired|all",
            )});
            return (StatusCode::BAD_REQUEST, axum::Json(body)).into_response();
        }
    };

    let mut entries: Vec<serde_json::Value> = state
        .summary_store
        .snapshot_instances()
        .iter()
        .filter(|m| allowed.contains(&m.status()))
        .map(|metadata| {
            let descriptors = state.summary_store.descriptors_for_series_id(metadata.sid);
            sid_instance_to_json(metadata, descriptors.as_ref())
        })
        .collect();
    entries.sort_by_key(|v| v.get("sid").and_then(|x| x.as_u64()).unwrap_or(0));

    let body = serde_json::json!({
        "status": "success",
        "count": entries.len(),
        "schemas": entries});
    (StatusCode::OK, axum::Json(body)).into_response()
}

fn status_str(s: crate::storage_engines::sketch_db::AggStatus) -> &'static str {
    use crate::storage_engines::sketch_db::AggStatus;
    match s {
        AggStatus::Active => "active",
        AggStatus::Retired => "retired",
        AggStatus::Expired => "expired",
    }
}

/// Encode sid metadata, including identity, lifecycle timestamps, and status.
fn sid_instance_to_json(
    m: &crate::storage_engines::sketch_db::index::SummarySeriesMetadata,
    descriptors: Option<&(
        std::sync::Arc<crate::storage_engines::sketch_db::SummaryDescriptor>,
        std::sync::Arc<crate::storage_engines::sketch_db::DataDescriptor>,
    )>,
) -> serde_json::Value {
    let summary_descriptor_id = descriptors.map(|(summary, _)| summary.id.canonical());
    let data_descriptor_id = descriptors.map(|(_, data)| data.id.canonical());
    serde_json::json!({
        "sid": m.sid,
        "metric_name": m.metric_name,
        "status": status_str(m.status()),
        "first_seen_unix_ms": m.first_seen_unix_ms,
        "retired_at_ms": m.retired_at_ms,
        "expires_at_ms": m.expires_at_ms,
        "group_by_keys": m.group_by_keys.iter().collect::<Vec<_>>(),
        "agg_kind": format!("{:?}", m.agg_kind),
        "summary_descriptor_id": summary_descriptor_id,
        "data_descriptor_id": data_descriptor_id})
}

/// `POST /api/v1/db/schemas/:sid/retire` — manually transition an
/// Active sid to Retired (kicking off the retirement retention
/// clock). Idempotent: already-Retired or Expired sids return 200
/// with their current state unchanged. Returns 404 if the sid is
/// unknown.
async fn handle_post_schema_retire(
    State(state): State<AppState>,
    axum::extract::Path(sid): axum::extract::Path<u64>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    match state.summary_store.force_retire(
        sid,
        crate::storage_engines::sketch_db::DEFAULT_RETIREMENT_RETENTION,
    ) {
        Some(meta) => {
            let descriptors = state.summary_store.descriptors_for_series_id(sid);
            let body = serde_json::json!({
                "status": "success",
                "schema": sid_instance_to_json(&meta, descriptors.as_ref())});
            (StatusCode::OK, axum::Json(body)).into_response()
        }
        None => {
            let body = serde_json::json!({
                "status": "error",
                "error": format!("sid {sid} not found")});
            (StatusCode::NOT_FOUND, axum::Json(body)).into_response()
        }
    }
}

/// `POST /api/v1/db/schemas/:sid/expire` — manually transition a sid
/// to Expired immediately. The next `SchemaEvictionService` tick
/// drops the sid's data + removes the sid. Idempotent; 404 if the
/// sid is unknown.
async fn handle_post_schema_expire(
    State(state): State<AppState>,
    axum::extract::Path(sid): axum::extract::Path<u64>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    match state.summary_store.force_expire(sid) {
        Some(meta) => {
            let descriptors = state.summary_store.descriptors_for_series_id(sid);
            let body = serde_json::json!({
                "status": "success",
                "schema": sid_instance_to_json(&meta, descriptors.as_ref())});
            (StatusCode::OK, axum::Json(body)).into_response()
        }
        None => {
            let body = serde_json::json!({
                "status": "error",
                "error": format!("sid {sid} not found")});
            (StatusCode::NOT_FOUND, axum::Json(body)).into_response()
        }
    }
}

fn coverage_str(c: crate::storage_engines::sketch_db::TimelineCoverage) -> &'static str {
    use crate::storage_engines::sketch_db::TimelineCoverage;
    match c {
        TimelineCoverage::Sketch => "sketch",
        TimelineCoverage::Purged => "purged",
    }
}

/// §7 / §15.3 of the sketch DB design: expose the schema timeline
/// for a given metric over an `[start_ms, end_ms]` window. Useful
/// for debugging "which agg served this slice of history?" questions
/// without attaching a debugger, and for external tools that want
/// to reproduce the engine's per-segment dispatch.
///
/// Required query params:
///   `metric`    — metric name (string).
///   `start_ms`  — inclusive lower bound (u64 millis).
///   `end_ms`    — inclusive upper bound (u64 millis).
async fn handle_get_timeline(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    let Some(metric) = params.get("metric") else {
        let body = serde_json::json!({
            "status": "error",
            "error": "missing required query parameter 'metric'"});
        return (StatusCode::BAD_REQUEST, axum::Json(body)).into_response();
    };

    let parse_u64 = |key: &str| -> Result<u64, Box<axum::response::Response>> {
        match params.get(key) {
            Some(s) => s.parse::<u64>().map_err(|e| {
                let body = serde_json::json!({
                    "status": "error",
                    "error": format!("query parameter '{key}' is not a valid u64: {e}")});
                Box::new((StatusCode::BAD_REQUEST, axum::Json(body)).into_response())
            }),
            None => {
                let body = serde_json::json!({
                    "status": "error",
                    "error": format!("missing required query parameter '{key}'")});
                Err(Box::new(
                    (StatusCode::BAD_REQUEST, axum::Json(body)).into_response(),
                ))
            }
        }
    };
    let start_ms = match parse_u64("start_ms") {
        Ok(v) => v,
        Err(resp) => return *resp,
    };
    let end_ms = match parse_u64("end_ms") {
        Ok(v) => v,
        Err(resp) => return *resp,
    };

    // Schema retirement #2 — read the timeline from the sid catalog
    // directly. The `agg_id` field on each segment now carries a
    // content-derived signature id (xxh64 of metric + agg_kind +
    // group_by_keys), stable across restarts.
    let segments = crate::storage_engines::sketch_db::query::timeline::timeline_for_metric(
        &state.summary_store,
        metric,
        start_ms,
        end_ms,
    );
    let entries: Vec<serde_json::Value> = segments
        .iter()
        .map(|s| {
            serde_json::json!({
                "agg_id": s.agg_id,
                "start_ms": s.start_ms,
                "end_ms": s.end_ms,
                "status": status_str(s.status),
                "coverage": coverage_str(s.coverage)})
        })
        .collect();

    let body = serde_json::json!({
        "status": "success",
        "metric": metric,
        "start_ms": start_ms,
        "end_ms": end_ms,
        "count": entries.len(),
        "segments": entries});
    (StatusCode::OK, axum::Json(body)).into_response()
}

// ─── §10 / §15 backfill API ──────────────────────────────────────────────────

/// Request body for `POST /api/v1/db/backfill`.
#[derive(serde::Deserialize)]
struct CreateBackfillJobRequest {
    agg_id: u64,
    start_ms: u64,
    end_ms: u64,
    source: crate::storage_engines::sketch_db::BackfillSource,
    windows_total: u64,
}

fn backfill_status_str(s: &crate::storage_engines::sketch_db::BackfillStatus) -> &'static str {
    use crate::storage_engines::sketch_db::BackfillStatus;
    match s {
        BackfillStatus::Queued => "queued",
        BackfillStatus::Running => "running",
        BackfillStatus::Complete => "complete",
        BackfillStatus::Failed => "failed",
        BackfillStatus::Cancelled => "cancelled",
    }
}

fn backfill_job_to_json(job: &crate::storage_engines::sketch_db::BackfillJob) -> serde_json::Value {
    serde_json::json!({
        "job_id": job.job_id,
        "agg_id": job.agg_id,
        "start_ms": job.time_range.0,
        "end_ms": job.time_range.1,
        "source": job.source,
        "status": backfill_status_str(&job.status),
        "progress": job.progress(),
        "windows_done": job.windows_done,
        "windows_total": job.windows_total,
        "created_at_ms": job.created_at_ms,
        "started_at_ms": job.started_at_ms,
        "completed_at_ms": job.completed_at_ms,
        "error_message": job.error_message})
}

fn service_unavailable_no_backfill() -> axum::response::Response {
    use axum::response::IntoResponse;
    let body = serde_json::json!({
        "status": "error",
        "error": "backfill registry not attached; backend was built without HttpServer::with_backfill_registry"});
    (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body)).into_response()
}

/// `POST /api/v1/db/backfill` — create a queued backfill job.
///
/// Validates the request against the §10.5 invariants via
/// `BackfillRegistry::create_checked`:
/// * `agg_id` must be known to the schema registry → 404 on miss.
/// * `end_ms` must not extend past the agg's `created_at_ms` (no
///   race against live ingest) → 409 on overlap.
/// * `start_ms` must be within the SketchStore data-retention
///   window when one is configured (Method B) → 409 on stale range.
///
/// 400 on malformed body / inverted range; 503 when no registry or
/// schema registry is attached.
async fn handle_post_backfill_job(
    State(state): State<AppState>,
    body: axum::body::Bytes,
) -> axum::response::Response {
    use crate::storage_engines::sketch_db::CreateError;
    use axum::response::IntoResponse;

    let Some(registry) = state.backfill else {
        return service_unavailable_no_backfill();
    };

    let req: CreateBackfillJobRequest = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            let body = serde_json::json!({
                "status": "error",
                "error": format!("invalid request body: {e}")});
            return (StatusCode::BAD_REQUEST, axum::Json(body)).into_response();
        }
    };
    if req.start_ms >= req.end_ms {
        let body = serde_json::json!({
        "status": "error",
        "error": format!(
            "start_ms {} must be < end_ms {}",
            req.start_ms, req.end_ms
        )});
        return (StatusCode::BAD_REQUEST, axum::Json(body)).into_response();
    }

    // Resolve the aggregation from the current streaming-config snapshot, or
    // return 404 `UnknownAgg`. Its earliest matching sid timestamp bounds the
    // backfill range; with no ingested sid, use the current time.
    let Some(handle) = state.hot_reload_config.as_ref() else {
        let body = serde_json::json!({
            "status": "error",
            "error": "hot-reload streaming-config handle not attached; backfill agg lookup requires HttpServer::with_hot_reload_config"});
        return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body)).into_response();
    };
    let snapshot = handle.snapshot();
    let agg_cfg = match snapshot.get_aggregation_config(req.agg_id) {
        Some(c) => c.clone(),
        None => {
            let body = serde_json::json!({
                "status": "error",
                "error": format!("unknown agg_id {} (not in streaming-config snapshot)", req.agg_id)});
            return (StatusCode::NOT_FOUND, axum::Json(body)).into_response();
        }
    };
    let created_at_ms = {
        // Earliest live first_seen_unix_ms for this metric, if any
        // sid in the catalog has actually ingested data. A sid that
        // was registered without data has `first_seen_unix_ms == 0`,
        // which we treat as "no live ingest yet" rather than
        // "ingest started at the unix epoch" — backfill can then
        // cover up to wall-clock-now.
        let earliest = state
            .summary_store
            .snapshot_instances()
            .into_iter()
            .filter(|m| m.metric_name == agg_cfg.metric)
            .map(|m| m.first_seen_unix_ms.max(0) as u64)
            .filter(|t| *t > 0)
            .min();
        earliest.unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0)
        })
    };
    match registry.create_checked(
        &agg_cfg,
        created_at_ms,
        (req.start_ms, req.end_ms),
        req.source,
        req.windows_total,
        state.data_retention_ms,
    ) {
        Ok(job_id) => {
            let body = serde_json::json!({
                "status": "success",
                "job_id": job_id});
            (StatusCode::CREATED, axum::Json(body)).into_response()
        }
        Err(e @ CreateError::UnknownAgg { .. }) => {
            let body = serde_json::json!({
                "status": "error",
                "error": e.to_string()});
            (StatusCode::NOT_FOUND, axum::Json(body)).into_response()
        }
        Err(e @ (CreateError::Overlap { .. } | CreateError::OutOfRetention { .. })) => {
            let body = serde_json::json!({
                "status": "error",
                "error": e.to_string()});
            (StatusCode::CONFLICT, axum::Json(body)).into_response()
        }
    }
}

/// `GET /api/v1/db/backfill/jobs` — list all jobs with optional
/// `?status=queued|running|complete|failed|cancelled|all` filter.
async fn handle_get_backfill_jobs(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> axum::response::Response {
    use crate::storage_engines::sketch_db::BackfillStatus;
    use axum::response::IntoResponse;

    let Some(registry) = state.backfill else {
        return service_unavailable_no_backfill();
    };

    let filter = params.get("status").map(String::as_str).unwrap_or("all");
    let jobs = match filter {
        "queued" => registry.list_by_status(&BackfillStatus::Queued),
        "running" => registry.list_by_status(&BackfillStatus::Running),
        "complete" => registry.list_by_status(&BackfillStatus::Complete),
        "failed" => registry.list_by_status(&BackfillStatus::Failed),
        "cancelled" => registry.list_by_status(&BackfillStatus::Cancelled),
        "all" => registry.list(),
        other => {
            let body = serde_json::json!({
            "status": "error",
            "error": format!(
                "unknown status filter '{other}'; expected one of queued|running|complete|failed|cancelled|all",
            )});
            return (StatusCode::BAD_REQUEST, axum::Json(body)).into_response();
        }
    };
    let mut entries: Vec<serde_json::Value> = jobs.iter().map(backfill_job_to_json).collect();
    entries.sort_by_key(|v| v.get("job_id").and_then(|x| x.as_u64()).unwrap_or(0));

    let body = serde_json::json!({
        "status": "success",
        "count": entries.len(),
        "jobs": entries});
    (StatusCode::OK, axum::Json(body)).into_response()
}

/// `GET /api/v1/db/backfill/jobs/:job_id` — detail of a single job.
/// Returns 404 if not found, 503 if no registry.
async fn handle_get_backfill_job(
    State(state): State<AppState>,
    axum::extract::Path(job_id): axum::extract::Path<u64>,
) -> axum::response::Response {
    use axum::response::IntoResponse;

    let Some(registry) = state.backfill else {
        return service_unavailable_no_backfill();
    };
    match registry.get(job_id) {
        Some(job) => {
            let body = serde_json::json!({
                "status": "success",
                "job": backfill_job_to_json(&job)});
            (StatusCode::OK, axum::Json(body)).into_response()
        }
        None => {
            let body = serde_json::json!({
                "status": "error",
                "error": format!("job_id {job_id} not found")});
            (StatusCode::NOT_FOUND, axum::Json(body)).into_response()
        }
    }
}

/// `DELETE /api/v1/db/backfill/jobs/:job_id` — cancel the job if
/// non-terminal. Returns 200 on success, 404 if unknown, 409 if
/// the job is already terminal, 503 if no registry.
async fn handle_delete_backfill_job(
    State(state): State<AppState>,
    axum::extract::Path(job_id): axum::extract::Path<u64>,
) -> axum::response::Response {
    use axum::response::IntoResponse;

    let Some(registry) = state.backfill else {
        return service_unavailable_no_backfill();
    };
    let Some(job) = registry.get(job_id) else {
        let body = serde_json::json!({
            "status": "error",
            "error": format!("job_id {job_id} not found")});
        return (StatusCode::NOT_FOUND, axum::Json(body)).into_response();
    };
    if job.status.is_terminal() {
        let body = serde_json::json!({
        "status": "error",
        "error": format!(
            "job {job_id} already {}, cannot cancel",
            backfill_status_str(&job.status)
        )});
        return (StatusCode::CONFLICT, axum::Json(body)).into_response();
    }
    registry.cancel(job_id);
    let body = serde_json::json!({
        "status": "success",
        "job_id": job_id});
    (StatusCode::OK, axum::Json(body)).into_response()
}

#[cfg(test)]
mod logical_provenance_tests {
    use super::*;

    #[tokio::test]
    async fn hybrid_execution_has_its_own_route_with_measured_branch_counts() {
        // A successful mixed graph must never inherit the pure-ASAP route from its engine name.
        let response = Json(serde_json::json!({"status":"success", "warnings":[
            "asap_execution:hybrid", "asap_logical_stats:raw=0,summary=1,memo_hits=3,remote=2,remote_rpcs=1,remote_branches=2"
        ], "data":{"resultType":"vector", "result":[]}}))
        .into_response();
        let response = annotate_data_source(response, "asap_query").await;
        assert_eq!(response.headers()["x-asap-execution"], "hybrid");
        assert_eq!(response.headers()["x-asap-execution-detail"], "hybrid");
        assert_eq!(
            response.headers()["x-asap-summary-readout-evaluations"],
            "1"
        );
        let bytes = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(value["warnings"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn prometheus_exact_branch_is_hybrid_without_backend_scan() {
        let response = Json(serde_json::json!({"status":"success", "warnings":[
            "asap_execution:hybrid", "asap_logical_stats:raw=0,summary=1,memo_hits=0,remote=1,remote_rpcs=1,remote_branches=1"
        ], "data":{"resultType":"vector", "result":[]}})).into_response();
        let response = annotate_data_source(response, "asap_query").await;
        assert_eq!(response.headers()["x-asap-execution-detail"], "hybrid");
        assert_eq!(response.headers()["x-asap-raw-scan-evaluations"], "0");
        assert_eq!(response.headers()["x-asap-exact-subquery-evaluations"], "1");
    }

    #[tokio::test]
    async fn prometheus_only_dag_is_external_exact() {
        let response = Json(serde_json::json!({"status":"success", "warnings":[
            "asap_execution:exact_dag", "asap_logical_stats:raw=0,summary=0,memo_hits=0,remote=1,remote_rpcs=1,remote_branches=1"
        ], "data":{"resultType":"vector", "result":[]}})).into_response();
        let response = annotate_data_source(response, "asap_query").await;
        assert_eq!(response.headers()["x-asap-execution"], "exact_fallback");
        assert_eq!(
            response.headers()["x-asap-execution-detail"],
            "external_exact"
        );
    }

    #[test]
    fn partial_result_warning_survives_internal_metadata_extraction() {
        // Only internal metadata is removed; incomplete-result warnings still invalidate comparison.
        let mut value = serde_json::json!({"warnings":["partial data", "asap_execution:exact_dag", "asap_logical_stats:raw=0,summary=0,memo_hits=0,remote=1,remote_rpcs=1,remote_branches=1"]});
        assert_eq!(
            extract_logical_provenance(&mut value),
            Some(Ok((0, 0, 0, 1, 0, 1, 1)))
        );
        assert_eq!(value["warnings"], serde_json::json!(["partial data"]));
    }

    #[test]
    fn contradictory_provenance_is_not_warm() {
        // Any observed local raw branch invalidates a deployed plan.
        let mut value = serde_json::json!({"warnings":["asap_execution:asap", "asap_logical_stats:raw=1,summary=1,memo_hits=0"]});
        assert_eq!(extract_logical_provenance(&mut value), Some(Err(())));
    }
}

#[cfg(test)]
mod catalog_install_tests {
    use super::{validate_and_build_runtime_plan, PhysicalPlanInstallRequest};
    use std::sync::Arc;

    fn request() -> PhysicalPlanInstallRequest {
        let snapshot: control_plane::physical::compiler::BackendLocalPlanningInput =
            serde_json::from_str(include_str!(
                "../../../../../docs/examples/asapquery-compatibility-demo-snapshot.json"
            ))
            .unwrap();
        let plan = crate::tests::test_utilities::planning::quoted_snapshot(snapshot, false)
            .compile_promql()
            .unwrap();
        PhysicalPlanInstallRequest {
            summary_catalog: plan.summary_catalog,
            collector_plans: plan.collector_plans,
            precompute_plan: plan.precompute_plan,
            transmission_plan: plan.transmission_plan,
            query_plan: plan.query_plan,
            storage_routing: None,
            adaptation_evidence: vec![],
        }
    }
    fn install(
        request: PhysicalPlanInstallRequest,
    ) -> Result<crate::storage_engines::types::RuntimePhysicalPlan, String> {
        validate_and_build_runtime_plan(
            request,
            Arc::new(crate::storage_engines::types::BackendStorageRouting::empty()),
        )
    }

    #[test]
    fn invalid_clickhouse_entry_cannot_change_active_generation() {
        let active = install(request()).expect("baseline plan installs");
        let handle = crate::storage_engines::types::ActivePhysicalPlanHandle::new(active);
        let before = handle.active_snapshot();
        let mut candidate = request();
        candidate.query_plan.clickhouse_context =
            Some(asap_types::query_plan::ClickHousePlanningContext {
                window_templates: Default::default(),
                tables: Default::default(),
                accuracy: planner_types::types::AccuracyTarget::Exact,
            });
        let (_, mut entry) = candidate
            .query_plan
            .entries
            .pop_first()
            .expect("fixture has a query entry");
        entry.language = asap_types::query_plan::QueryLanguage::ClickHouseSql;
        entry.fixed_evaluation = Some(asap_types::query_plan::FixedEvaluationRange {
            start_ms: 0,
            end_ms: 1_000,
            cumulative: true,
        });
        let canonical = entry.canonical_query.clone();
        let binding = entry
            .nodes
            .values_mut()
            .find_map(|node| match node {
                asap_types::query_plan::QueryPlanNode::ReadMaterialization { binding } => {
                    Some(binding)
                }
                _ => None,
            })
            .expect("fixture query reads a materialization");
        binding.pane_origin_ms = Some(999);
        candidate
            .query_plan
            .entries
            .insert(format!("clickhouse:{canonical}"), entry);

        let error = install(candidate).expect_err("invalid SQL binding must fail staging");
        assert!(error.contains("pane origin"), "{error}");
        let after = handle.active_snapshot();
        assert_eq!(after.plan_id(), before.plan_id());
        assert_eq!(after.plan_version(), before.plan_version());
    }

    // Installing transports the exact supplied snapshot, rather than rebuilding it.
    #[test]
    fn catalog_install_preserves_authoritative_snapshot() {
        let request = request();
        let expected = request.summary_catalog.clone();
        let encoded = serde_json::to_vec(&request).unwrap();
        let active = install(serde_json::from_slice(&encoded).unwrap()).unwrap();
        assert_eq!(active.summary_catalog.as_deref(), Some(&expected));
        active
            .query_plan
            .validate_against_catalog(active.summary_catalog.as_deref().unwrap())
            .unwrap();
    }

    // Catalog-aware artifacts cannot silently downgrade to a legacy installation.
    #[test]
    fn catalog_install_requires_snapshot_and_matching_references() {
        let mut encoded = serde_json::to_value(request()).unwrap();
        encoded.as_object_mut().unwrap().remove("summary_catalog");
        assert!(serde_json::from_value::<PhysicalPlanInstallRequest>(encoded).is_err());
        let mut request = request();
        request.transmission_plan.summary_catalog = None;
        assert!(install(request)
            .unwrap_err()
            .contains("TransmissionPlan catalog"));
    }

    // A same-version snapshot replacement fails before the active generation changes.
    #[test]
    fn catalog_install_rejects_drift_without_replacing_active_snapshot() {
        let active = crate::storage_engines::types::ActivePhysicalPlanHandle::new(
            install(request()).unwrap(),
        );
        let before = active.active_snapshot();
        let mut changed = request();
        changed.summary_catalog.plan_version += 1;
        assert!(install(changed).is_err());
        assert!(Arc::ptr_eq(&before, &active.active_snapshot()));
        let mut changed = request();
        changed.summary_catalog.summary_descriptors.clear();
        assert!(install(changed).is_err());
        assert!(Arc::ptr_eq(&before, &active.active_snapshot()));
    }

    // Physical pane width cannot be replaced by the semantic lookback at install.
    #[test]
    fn catalog_install_rejects_query_pane_drift() {
        let mut request = request();
        let binding = request
            .query_plan
            .entries
            .values_mut()
            .flat_map(|entry| entry.nodes.values_mut())
            .find_map(|node| match node {
                asap_types::query_plan::QueryPlanNode::ReadMaterialization { binding } => {
                    Some(binding)
                }
                _ => None,
            })
            .expect("demo has maintained summaries");
        binding.window_ms += 1;
        assert!(install(request).unwrap_err().contains("pane duration"));
    }

    #[test]
    fn catalog_install_applies_pane_origin_validation_to_metricsql_entries() {
        let mut request = request();
        let key = request.query_plan.entries.keys().next().unwrap().clone();
        let mut entry = request.query_plan.entries.remove(&key).unwrap();
        entry.language = asap_types::query_plan::QueryLanguage::MetricsQl;
        let binding = entry
            .nodes
            .values_mut()
            .find_map(|node| match node {
                asap_types::query_plan::QueryPlanNode::ReadMaterialization { binding } => {
                    Some(binding)
                }
                _ => None,
            })
            .expect("demo has maintained summaries");
        binding.pane_origin_ms = Some(1);
        let key =
            asap_types::query_plan::QueryPlan::catalog_key(entry.language, &entry.canonical_query);
        request.query_plan.entries.insert(key, entry);
        let error = install(request).unwrap_err();
        assert!(error.contains("pane origin"), "{error}");
    }
}

#[deprecated(note = "Use validate_and_build_runtime_plan")]
pub use validate_and_build_runtime_plan as build_active_physical_plan;
