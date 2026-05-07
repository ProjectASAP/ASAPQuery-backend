use crate::drivers::query::adapters::{ParsedQueryRequest, ParsedRangeQueryRequest};
use axum::{
    body::Bytes,
    extract::{Form, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
    routing::{get, post},
    Router,
};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;
use tracing::{debug, info, warn};

use crate::drivers::query::adapters::{create_http_adapter, AdapterConfig, HttpProtocolAdapter};
use crate::drivers::query::servers::metrics as srv_metrics;
use crate::engines::{EngineRouter, EngineRouterError, QueryEngine, SimpleEngine};
use crate::query_tracker::QueryTracker;
use crate::stores::Store;
use asap_types::{AccuracyTarget, StorageBackend};
use promql_utilities::query_logics::enums::Statistic;

#[derive(Debug, Clone)]
pub struct HttpServerConfig {
    pub port: u16,
    pub handle_http_requests: bool,
    pub adapter_config: AdapterConfig,
}

#[derive(Clone)]
pub struct HttpServer {
    config: HttpServerConfig,
    query_engine: Arc<SimpleEngine>,
    /// Phase-5/6 capability router. Built from `query_engine` at
    /// construction time (`SimpleEngine` registered as the warm-tier
    /// `QueryEngine`) and extended via [`Self::with_query_engine`] —
    /// e.g. to plug in a `GorillaQueryEngine` for the cold archive
    /// tier. Instant-query dispatch consults this for metrics whose
    /// `StreamingConfig::storage_backend()` is anything other than
    /// `SketchWarmTier`; warm-tier queries still take the direct
    /// `SimpleEngine::handle_query` path so they keep the
    /// `KeyByLabelNames` Prometheus needs to populate the `metric`
    /// map. See `docs/design-gorilla-s3-cold-engine.md` §8.
    query_router: Arc<EngineRouter>,
    store: Arc<dyn Store>,
    query_tracker: Option<Arc<QueryTracker>>,
    /// Hot-reloadable `StreamingConfig` source. `None` when hot-reload
    /// is not wired up by the caller (unit tests, legacy binaries).
    hot_reload_config: Option<crate::data_model::HotReloadStreamingConfig>,
    /// Per-metric storage-backend routing table consulted by the HTTP
    /// instant-query handler at request time. When `Some(..)` and the
    /// query parses, the handler extracts the metric name from the
    /// PromQL AST, consults this table, and dispatches through
    /// `EngineRouter` for any per-metric override. When `None` the
    /// handler falls back to the pre-Phase-5 behaviour of consulting
    /// the streaming-config's single `storage_backend()` axis (which
    /// itself defaults to `SketchWarmTier`). Wired by the binary via
    /// [`Self::with_backend_storage_routing`]; production deploys
    /// load `deploy/configs/backend-storage-routing.yaml`.
    backend_storage_routing: Option<Arc<crate::data_model::BackendStorageRouting>>,
    /// Per-`agg_id` schema registry (sketch DB §6). `None` when the
    /// caller hasn't wired the precompute engine into the HTTP
    /// server — in that case the `POST /api/v1/streaming-config`
    /// handler still swaps the config but doesn't drive schema
    /// lifecycle transitions.
    schemas: Option<Arc<crate::stores::sketch_db::SchemaRegistry>>,
    /// Backfill registry (sketch DB §10). `None` until Phase 5e
    /// wires a worker pool; in the interim, jobs created via the
    /// HTTP endpoints stay `Queued` and are visible via the list
    /// endpoint — useful shadow-mode testing before workers exist.
    backfill: Option<Arc<crate::stores::sketch_db::BackfillRegistry>>,
    /// SimpleMapStore data-retention horizon in millis, mirroring
    /// `--persistence-delete-older-than-secs` at the CLI. Used by the
    /// `POST /api/v1/db/backfill` handler to gate job creation via
    /// `BackfillRegistry::create_checked` (§10.5 Method B). `None`
    /// disables the retention precheck — the handler still enforces
    /// the §10.5 time-disjoint invariant.
    data_retention_ms: Option<u64>,
}

#[derive(Clone)]
struct AppState {
    config: HttpServerConfig,
    query_engine: Arc<SimpleEngine>,
    /// See [`HttpServer::query_router`].
    query_router: Arc<EngineRouter>,
    store: Arc<dyn Store>,
    query_tracker: Option<Arc<QueryTracker>>,
    adapter: Arc<dyn HttpProtocolAdapter>,
    fallback: Option<Arc<dyn crate::drivers::query::fallback::FallbackClient>>,
    hot_reload_config: Option<crate::data_model::HotReloadStreamingConfig>,
    /// See [`HttpServer::backend_storage_routing`].
    backend_storage_routing: Option<Arc<crate::data_model::BackendStorageRouting>>,
    /// Per-`agg_id` schema registry (sketch DB §6). Phase 2b wires
    /// `POST /api/v1/streaming-config` to call `schemas.reconcile()`
    /// on every swap so schema lifecycle transitions happen
    /// event-driven instead of on every ingest batch. When absent,
    /// the swap handler leaves the registry alone (legacy
    /// per-batch reconcile still works).
    schemas: Option<Arc<crate::stores::sketch_db::SchemaRegistry>>,
    /// Backfill registry (sketch DB §10). See `HttpServer::backfill`.
    backfill: Option<Arc<crate::stores::sketch_db::BackfillRegistry>>,
    /// See `HttpServer::data_retention_ms`.
    data_retention_ms: Option<u64>,
}

impl HttpServer {
    pub fn new(
        config: HttpServerConfig,
        query_engine: Arc<SimpleEngine>,
        store: Arc<dyn Store>,
        query_tracker: Option<Arc<QueryTracker>>,
    ) -> Self {
        // Bootstrap the capability router with `SimpleEngine` registered
        // for the warm-tier (`sketch_warm`) `data_source_id`. Callers
        // wiring up additional engines (e.g. `GorillaQueryEngine` for
        // the cold archive) extend the router via `with_query_engine`.
        let mut router = EngineRouter::new();
        router.register(query_engine.clone() as Arc<dyn QueryEngine>);
        let query_router = Arc::new(router);
        Self {
            config,
            query_engine,
            query_router,
            store,
            query_tracker,
            hot_reload_config: None,
            backend_storage_routing: None,
            schemas: None,
            backfill: None,
            data_retention_ms: None,
        }
    }

    /// Plug an additional [`QueryEngine`] into the capability router.
    /// Used by the binary to register `GorillaQueryEngine` (cold
    /// archive) alongside the `SimpleEngine` registered by `new`.
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

    /// Attach a `HotReloadStreamingConfig` handle so the
    /// `GET/POST /api/v1/streaming-config` endpoints can read and
    /// swap the currently active config. Without this handle the
    /// endpoints return `503 Service Unavailable`.
    pub fn with_hot_reload_config(
        mut self,
        handle: crate::data_model::HotReloadStreamingConfig,
    ) -> Self {
        self.hot_reload_config = Some(handle);
        self
    }

    /// Attach a per-metric storage-backend routing table loaded from
    /// `backend-storage-routing.yaml`. When attached, every instant
    /// query consults this table (after extracting the metric name
    /// from the PromQL AST) and dispatches through `EngineRouter` for
    /// any per-metric override. Without this handle the handler falls
    /// back to the pre-Phase-5 single-axis behaviour driven by
    /// `StreamingConfig::storage_backend()`.
    ///
    /// This is the bridge from "warm-tier-only deploy" to
    /// "cold-archive-routed metrics" until the controller's plan-push
    /// pipeline lands per-metric `StorageBackend` updates.
    pub fn with_backend_storage_routing(
        mut self,
        routing: Arc<crate::data_model::BackendStorageRouting>,
    ) -> Self {
        self.backend_storage_routing = Some(routing);
        self
    }

    /// Attach the `SchemaRegistry` that the precompute engine's
    /// `IngestState` also holds. When attached, the
    /// `POST /api/v1/streaming-config` handler calls
    /// `schemas.reconcile(new_config)` after the ArcSwap store, so
    /// schema lifecycle transitions (§6 of the sketch DB design) are
    /// event-driven rather than per-ingest-batch. Without the handle
    /// the registry still gets reconciled on the next ingest batch,
    /// just less promptly.
    pub fn with_schemas(mut self, schemas: Arc<crate::stores::sketch_db::SchemaRegistry>) -> Self {
        self.schemas = Some(schemas);
        self
    }

    /// Attach a `BackfillRegistry` so the `/api/v1/db/backfill`
    /// HTTP endpoints (Phase 5d) can create and inspect jobs. Jobs
    /// stay `Queued` until Phase 5e's worker pool is wired; the
    /// endpoints are still useful for shadow-mode validation of the
    /// controller's REFRESH dispatch logic.
    pub fn with_backfill_registry(
        mut self,
        registry: Arc<crate::stores::sketch_db::BackfillRegistry>,
    ) -> Self {
        self.backfill = Some(registry);
        self
    }

    /// Declare the SimpleMapStore data-retention horizon (the value of
    /// `--persistence-delete-older-than-secs` * 1000). When set, the
    /// `POST /api/v1/db/backfill` handler runs `create_checked` with
    /// this bound, so jobs that would write windows older than the
    /// retention horizon are rejected up-front instead of being
    /// silently evicted right after write (§10.5 Method B).
    pub fn with_data_retention_ms(mut self, data_retention_ms: u64) -> Self {
        self.data_retention_ms = Some(data_retention_ms);
        self
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        srv_metrics::register_all();

        // Create adapter using factory
        let adapter = create_http_adapter(self.config.adapter_config.clone());

        let query_endpoint = adapter.get_query_endpoint();
        let runtime_info_path = adapter.get_runtime_info_path();
        info!(
            "Adapter '{}' configured for endpoint: {}",
            adapter.adapter_name(),
            query_endpoint
        );
        info!("Runtime info endpoint: {}", runtime_info_path);

        let app_state = AppState {
            config: self.config.clone(),
            query_engine: self.query_engine,
            query_router: self.query_router,
            store: self.store,
            query_tracker: self.query_tracker,
            adapter: adapter.clone(),
            fallback: self.config.adapter_config.fallback.clone(),
            hot_reload_config: self.hot_reload_config.clone(),
            backend_storage_routing: self.backend_storage_routing.clone(),
            schemas: self.schemas.clone(),
            backfill: self.backfill.clone(),
            data_retention_ms: self.data_retention_ms,
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
            // mvp/v5: dump the S3 cost-tracking counters as CSV.
            // The demo's `run_mvp_demo.sh` curls this for each
            // baseline; missing counters render as zeros.
            .route("/internal/s3_cost.csv", get(handle_s3_cost_csv))
            // Controller integration endpoints
            .route("/api/v1/precompute", post(handle_precompute_job))
            .route("/api/v1/health", get(handle_health))
            .route("/api/v1/store/metrics", get(handle_store_metrics))
            .route(
                "/api/v1/streaming-config",
                get(handle_get_streaming_config).post(handle_post_streaming_config),
            )
            .route("/api/v1/db/schemas", get(handle_get_schemas))
            .route(
                "/api/v1/db/schemas/:agg_id/retire",
                post(handle_post_schema_retire),
            )
            .route(
                "/api/v1/db/schemas/:agg_id/expire",
                post(handle_post_schema_expire),
            )
            .route("/api/v1/db/timeline", get(handle_get_timeline))
            .route("/api/v1/db/backfill", post(handle_post_backfill_job))
            .route("/api/v1/db/backfill/jobs", get(handle_get_backfill_jobs))
            .route(
                "/api/v1/db/backfill/jobs/:job_id",
                get(handle_get_backfill_job).delete(handle_delete_backfill_job),
            )
            .with_state(app_state);

        let listener = TcpListener::bind(format!("0.0.0.0:{}", self.config.port)).await?;
        info!("HTTP server listening on port {}", self.config.port);

        axum::serve(listener, app).await?;
        Ok(())
    }

    /// Start server for testing on a random available port
    /// Returns the actual port number used
    #[cfg(test)]
    pub async fn start_test_server(&self) -> Result<u16, Box<dyn std::error::Error + Send + Sync>> {
        // Create adapter using factory
        let adapter = create_http_adapter(self.config.adapter_config.clone());

        let query_endpoint = adapter.get_query_endpoint();
        let runtime_info_path = adapter.get_runtime_info_path();

        let app_state = AppState {
            config: self.config.clone(),
            query_engine: self.query_engine.clone(),
            query_router: self.query_router.clone(),
            store: self.store.clone(),
            query_tracker: self.query_tracker.clone(),
            adapter: adapter.clone(),
            fallback: self.config.adapter_config.fallback.clone(),
            hot_reload_config: self.hot_reload_config.clone(),
            backend_storage_routing: self.backend_storage_routing.clone(),
            schemas: self.schemas.clone(),
            backfill: self.backfill.clone(),
            data_retention_ms: self.data_retention_ms,
        };

        let range_query_endpoint = adapter.get_range_query_endpoint();

        let app = Router::new()
            .route(query_endpoint, get(handle_instant_query))
            .route(query_endpoint, post(handle_instant_query_post))
            .route(range_query_endpoint, get(handle_range_query))
            .route(range_query_endpoint, post(handle_range_query_post))
            .route(runtime_info_path, get(handle_runtime_info))
            .route(
                "/api/v1/streaming-config",
                get(handle_get_streaming_config).post(handle_post_streaming_config),
            )
            .route("/api/v1/db/schemas", get(handle_get_schemas))
            .route(
                "/api/v1/db/schemas/:agg_id/retire",
                post(handle_post_schema_retire),
            )
            .route(
                "/api/v1/db/schemas/:agg_id/expire",
                post(handle_post_schema_expire),
            )
            .route("/api/v1/db/timeline", get(handle_get_timeline))
            .route("/api/v1/db/backfill", post(handle_post_backfill_job))
            .route("/api/v1/db/backfill/jobs", get(handle_get_backfill_jobs))
            .route(
                "/api/v1/db/backfill/jobs/:job_id",
                get(handle_get_backfill_job).delete(handle_delete_backfill_job),
            )
            .with_state(app_state);

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

/// Core query execution logic shared between GET and POST handlers
async fn process_query_request(
    state: &AppState,
    parsed_request: &ParsedQueryRequest,
    start_time: Instant,
    headers: HashMap<String, String>,
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

    // Record query for passive auto-discovery (if tracker is enabled)
    if let Some(tracker) = &state.query_tracker {
        tracker.record_instant(&parsed_request.query, parsed_request.time);
    }

    // Step 2: Pick a dispatch path based on the metric's pinned
    // storage backend (Phase-5 capability routing).
    //
    // Routing precedence:
    //   (a) Per-metric `BackendStorageRouting` table (loaded from
    //       `backend-storage-routing.yaml` at startup). The PromQL
    //       query is parsed; the metric name is extracted from the
    //       AST and looked up in the table. This is the production
    //       path the issue-46 MVP relies on so cold-archive metrics
    //       (e.g. `http_requests_total` → `gorilla_archive`) actually
    //       route through the `EngineRouter`.
    //   (b) Single-axis `StreamingConfig::storage_backend()` from the
    //       hot-reload config (the pre-Phase-5 fallback). Pre-controller
    //       deploys ride this path; it always lands on `SketchWarmTier`
    //       unless the YAML was hand-patched.
    //   (c) Default — `SketchWarmTier`. Keeps the direct
    //       `SimpleEngine::handle_query` path so the response carries
    //       the `KeyByLabelNames` the Prometheus adapter needs to
    //       populate the `metric` map.
    //
    // For non-`SketchWarmTier` axes the dispatch goes through the
    // `EngineRouter`. Phase-6 (Gorilla MVP) returns a scalar with
    // empty labels, so dropping `KeyByLabelNames` is acceptable; the
    // response carries `accuracy` + `data_source` via the
    // wire-extension annotations.
    let metric_storage = resolve_metric_storage(state, &parsed_request.query);
    debug!(
        "Dispatch axis: metric_storage={:?} (from backend-storage-routing: {}, hot-reload: {})",
        metric_storage,
        state.backend_storage_routing.is_some(),
        state.hot_reload_config.is_some(),
    );

    if matches!(metric_storage, StorageBackend::SketchWarmTier) {
        process_via_simple_engine(state, parsed_request, start_time, headers).await
    } else {
        process_via_router(state, parsed_request, start_time, metric_storage).await
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
/// 3. Default `SketchWarmTier`.
///
/// Parsing failures fall through to (2)/(3) so a malformed PromQL
/// doesn't surface as a routing 5xx (the engines themselves will
/// reject it with a clearer error).
///
/// v7: when the routing table has multi-target rows for the metric,
/// the parsed AST is also classified via
/// [`crate::data_model::classify_query_shape`] and the lookup picks
/// the target whose `applies_to_query_shape` matches. v6.1
/// single-target metrics keep their original semantics — every shape
/// resolves to the one configured backend.
fn resolve_metric_storage(state: &AppState, query: &str) -> StorageBackend {
    if let Some(routing) = state.backend_storage_routing.as_ref() {
        match promql_parser::parser::parse(query) {
            Ok(expr) => {
                if let Some(metric_name) = first_metric_name(&expr) {
                    let shape = crate::data_model::classify_query_shape(&expr);
                    let backend = routing.lookup_with_shape(&metric_name, shape);
                    debug!(
                        "resolve_metric_storage: routing-table hit for metric={} shape={:?} → {:?}",
                        metric_name, shape, backend,
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

/// Direct `SimpleEngine::handle_query` dispatch — preserves the
/// `KeyByLabelNames` the Prometheus adapter needs to fill in the
/// `metric` map. Used for warm-tier metrics (the default) so the
/// response surface is byte-identical to the pre-router path. Adds a
/// `data_source: sketch_warm` info-line at the JSON layer so Phase-6
/// callers can byte-compare regardless of the dispatch path.
async fn process_via_simple_engine(
    state: &AppState,
    parsed_request: &ParsedQueryRequest,
    start_time: Instant,
    headers: HashMap<String, String>,
) -> Response {
    let query_start_time = Instant::now();
    debug!(
        "About to call query_engine.handle_query with query='{}' and time={}",
        parsed_request.query, parsed_request.time
    );
    match state
        .query_engine
        .handle_query(parsed_request.query.clone(), parsed_request.time)
    {
        Some((query_output_labels, query_result)) => {
            let query_duration = query_start_time.elapsed();
            debug!("=== QUERY ENGINE SUCCESS ===");
            debug!(
                "Query engine execution took: {:.2}ms",
                query_duration.as_secs_f64() * 1000.0
            );
            debug!("Query output labels: {:?}", query_output_labels);
            debug!("Query result: {:?}", query_result);

            // Step 3: Format success response using adapter
            // (Adapter handles protocol-specific formatting, e.g., convert_query_result_to_prometheus)
            use crate::drivers::query::adapters::QueryExecutionResult;
            let execution_result = QueryExecutionResult {
                query_output_labels,
                query_result,
            };

            let total_duration = start_time.elapsed();
            debug!(
                "Total request processing took: {:.2}ms",
                total_duration.as_secs_f64() * 1000.0
            );
            debug!("=== RETURNING SUCCESS RESPONSE ===");

            match state
                .adapter
                .format_success_response(&execution_result)
                .await
            {
                Ok(response) => annotate_data_source(
                    response,
                    StorageBackend::SketchWarmTier.data_source_id(),
                )
                .await,
                Err(status) => status.into_response(),
            }
        }
        None => {
            let total_duration = start_time.elapsed();
            debug!("=== QUERY ENGINE RETURNED NONE ===");
            debug!(
                "Request failed after: {:.2}ms",
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
                debug!("Query not supported and forwarding disabled, returning error");
                // Adapter formats the unsupported query error for its protocol.
                // We still annotate `data_source: sketch_warm` so callers
                // see which tier the request was dispatched against —
                // the routing decision happened, the metric just had no
                // compatible aggregation. Mirrors the
                // SimpleEngine-as-router-engine path where a
                // `EngineError::CapabilityMiss` response is still tagged.
                match state.adapter.format_unsupported_query_response().await {
                    Ok(response) => annotate_data_source(
                        response,
                        StorageBackend::SketchWarmTier.data_source_id(),
                    )
                    .await,
                    Err(status) => status.into_response(),
                }
            }
        }
    }
}

/// Dispatch through the [`EngineRouter`] — used for any metric whose
/// pinned `StorageBackend` is something other than `SketchWarmTier`.
///
/// The router's `execute` API is `(&str, Statistic, AccuracyTarget,
/// StorageBackend) -> QueryResult`. Three of those four axes are
/// pinned by the request:
///
/// * `query` — straight from the parsed request.
/// * `metric_storage` — looked up from the hot-reload `StreamingConfig`
///   by the caller.
///
/// The remaining two — `Statistic` + `AccuracyTarget` — would
/// normally be derived by parsing the PromQL AST. Pre-Phase-6 we don't
/// have an HTTP-side parser wired in; the router's
/// [`compatible_storage_backends`] consults them only for the
/// `DoubleWrite` head-selection heuristic (other deploy shapes
/// degenerate to a fixed list keyed only by `metric_storage`), so
/// defaulting to `(Sum, Approximate)` is safe for `GorillaS3Archive`-
/// only and `ColdJsonlFallback`-only deploys. A follow-up will thread
/// the real values through once the Phase-6 query-tracker exposes
/// them per request.
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
) -> Response {
    use crate::drivers::query::adapters::QueryExecutionResult;
    use crate::engines::EngineError;

    let query_start_time = Instant::now();
    debug!(
        "Dispatching via EngineRouter: query='{}' metric_storage={:?}",
        parsed_request.query, metric_storage,
    );

    // Default `(Sum, Approximate)` — see fn doc above. The router's
    // capability table only consults these axes for `DoubleWrite`
    // metrics; for `GorillaS3Archive`-only and `ColdJsonlFallback`-only
    // deploys the dispatch is a function of `metric_storage` alone.
    let stat = Statistic::Sum;
    let accuracy = AccuracyTarget::Approximate;

    let router_result = state
        .query_router
        .execute(&parsed_request.query, stat, accuracy, metric_storage)
        .await;

    match router_result {
        Ok(query_result) => {
            let query_duration = query_start_time.elapsed();
            debug!(
                "EngineRouter dispatch took: {:.2}ms; result: {:?}",
                query_duration.as_secs_f64() * 1000.0,
                query_result
            );

            // The Phase-4 Gorilla MVP returns a scalar with empty
            // labels; trait dispatch loses the `KeyByLabelNames` shape
            // SimpleEngine carries. Default to an empty `KeyByLabelNames`
            // — the Prometheus adapter renders an empty `metric: {}`,
            // which is a valid Prometheus shape (every label is just
            // unset) and matches `wrap_result`'s Phase-4 contract.
            let query_output_labels = promql_utilities::data_model::KeyByLabelNames::default();
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
                Ok(response) => annotate_data_source(
                    response,
                    metric_storage.data_source_id(),
                )
                .await,
                Err(status) => status.into_response(),
            }
        }
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
                    ),
                })),
            )
                .into_response()
        }
        Err(EngineRouterError::AllFailed { last }) => {
            warn!(error = %last, "EngineRouter: all compatible engines failed");
            let (status, error_type) = match &last {
                EngineError::CapabilityMiss { .. } => {
                    (StatusCode::NOT_FOUND, "bad_data")
                }
                EngineError::Backend { .. } => {
                    (StatusCode::INTERNAL_SERVER_ERROR, "internal")
                }
            };
            (
                status,
                Json(serde_json::json!({
                    "status": "error",
                    "errorType": error_type,
                    "error": last.to_string(),
                })),
            )
                .into_response()
        }
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

    let (parts, body) = response.into_parts();
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

async fn handle_instant_query(
    query_params: Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> Response {
    let _timer = srv_metrics::start_query_timer(srv_metrics::QUERY_TYPE_INSTANT);
    let start_time = Instant::now();
    debug!("=== INCOMING GET REQUEST ===");
    debug!("Raw query params: {:?}", query_params.0);

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

    let response = process_query_request(&state, &parsed_request, start_time, HashMap::new()).await;
    srv_metrics::record_query_outcome(
        srv_metrics::QUERY_TYPE_INSTANT,
        query_status_label(&response),
    );
    response
}

async fn handle_instant_query_post(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Response {
    let _timer = srv_metrics::start_query_timer(srv_metrics::QUERY_TYPE_INSTANT);
    let start_time = Instant::now();
    debug!("=== INCOMING POST REQUEST ===");

    // Extract headers we want to forward
    let mut forwarding_headers = HashMap::new();
    if let Some(auth) = headers.get(axum::http::header::AUTHORIZATION) {
        if let Ok(auth_str) = auth.to_str() {
            forwarding_headers.insert("Authorization".to_string(), auth_str.to_string());
        }
    }

    // Check content type to determine how to parse the body
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    debug!("Content-Type: {}", content_type);

    let parsed_request = if content_type.contains("application/json") {
        // Handle JSON POST (Elasticsearch)
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

    let result =
        process_query_request(&state, &parsed_request, start_time, forwarding_headers).await;

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
        .handle_runtime_info_with_headers(state.store.clone(), forwarding_headers)
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

// ============================================================
// Metrics Handler
// ============================================================

async fn handle_metrics() -> impl IntoResponse {
    let encoder = prometheus::TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    prometheus::Encoder::encode(&encoder, &metric_families, &mut buffer)
        .unwrap_or_else(|e| tracing::error!("Failed to encode metrics: {}", e));
    // mvp/v5: append the S3 cost counters in Prometheus text
    // exposition. Mirrors `/internal/s3_cost.csv` — the CSV is for
    // the demo, this is for live dashboards.
    let counters =
        crate::drivers::query::fallback::cold_store::global_s3_cost_counters();
    buffer.extend_from_slice(counters.render_prometheus().as_bytes());
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        buffer,
    )
}

/// mvp/v5: CSV dump of the S3 cost counters.
///
/// Renders ONE header row + ONE data row. Empty when no S3
/// operations have been issued (the counters default to zero, so
/// the CSV is still well-formed).
async fn handle_s3_cost_csv() -> impl IntoResponse {
    let counters =
        crate::drivers::query::fallback::cold_store::global_s3_cost_counters();
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/csv; charset=utf-8",
        )],
        counters.render_csv(),
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

    // Record query for passive auto-discovery (if tracker is enabled)
    if let Some(tracker) = &state.query_tracker {
        tracker.record_range(
            &parsed_request.query,
            parsed_request.start,
            parsed_request.end,
            parsed_request.step,
        );
    }

    // Execute range query with engine
    let query_start_time = Instant::now();
    debug!(
        "Executing range query: '{}' from {} to {} step {}",
        parsed_request.query, parsed_request.start, parsed_request.end, parsed_request.step
    );

    match state.query_engine.handle_range_query_promql(
        parsed_request.query.clone(),
        parsed_request.start,
        parsed_request.end,
        parsed_request.step,
    ) {
        Some((query_output_labels, query_result)) => {
            let query_duration = query_start_time.elapsed();
            debug!(
                "Range query execution took: {:.2}ms",
                query_duration.as_secs_f64() * 1000.0
            );

            let total_duration = start_time.elapsed();
            debug!(
                "Total range query processing took: {:.2}ms",
                total_duration.as_secs_f64() * 1000.0
            );

            // Format range success response
            match state
                .adapter
                .format_range_success_response(&query_result, &query_output_labels)
                .await
            {
                Ok(response) => response.into_response(),
                Err(status) => status.into_response(),
            }
        }
        None => {
            debug!("Range query returned None - query not supported");
            match state.adapter.format_unsupported_query_response().await {
                Ok(json) => json.into_response(),
                Err(status) => status.into_response(),
            }
        }
    }
}

async fn handle_range_query(
    query_params: Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> Response {
    let _timer = srv_metrics::start_query_timer(srv_metrics::QUERY_TYPE_RANGE);
    let start_time = Instant::now();
    debug!("=== INCOMING RANGE QUERY GET REQUEST ===");
    debug!("Raw query params: {:?}", query_params.0);

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

    let response = process_range_query_request(&state, &parsed_request, start_time).await;
    srv_metrics::record_query_outcome(srv_metrics::QUERY_TYPE_RANGE, query_status_label(&response));
    response
}

async fn handle_range_query_post(State(state): State<AppState>, body: Bytes) -> Response {
    let _timer = srv_metrics::start_query_timer(srv_metrics::QUERY_TYPE_RANGE);
    let start_time = Instant::now();
    debug!("=== INCOMING RANGE QUERY POST REQUEST ===");

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

    let response = process_range_query_request(&state, &parsed_request, start_time).await;
    srv_metrics::record_query_outcome(srv_metrics::QUERY_TYPE_RANGE, query_status_label(&response));
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_model::{HotReloadStreamingConfig, InferenceConfig, StreamingConfig};
    use crate::engines::SimpleEngine;
    use crate::stores::sketch_db::simple_map_store::SimpleMapStore;
    use reqwest::Client;
    use std::sync::Arc;

    async fn setup_test_server() -> u16 {
        setup_test_server_with_hot_reload(None).await
    }

    async fn setup_test_server_with_hot_reload(
        hot_reload: Option<HotReloadStreamingConfig>,
    ) -> u16 {
        let adapter_config = AdapterConfig::prometheus_promql(
            "http://127.0.0.1:9999".to_string(), // Unused for this test
            false,                               // forward_unsupported_queries
        );

        let config = HttpServerConfig {
            port: 0,
            handle_http_requests: true,
            adapter_config,
        };

        let inference_config = InferenceConfig::new(
            crate::data_model::QueryLanguage::promql,
            crate::data_model::CleanupPolicy::NoCleanup,
        );
        let streaming_config = Arc::new(StreamingConfig::default());
        let store = Arc::new(SimpleMapStore::new(
            streaming_config.clone(),
            crate::data_model::CleanupPolicy::NoCleanup,
        ));
        let query_engine = Arc::new(SimpleEngine::new(
            store.clone(),
            // None,
            inference_config,
            streaming_config.clone(),
            15000,
            crate::data_model::QueryLanguage::promql,
        ));

        let mut server = HttpServer::new(config, query_engine, store, None);
        if let Some(handle) = hot_reload {
            server = server.with_hot_reload_config(handle);
        }
        server
            .start_test_server()
            .await
            .expect("Failed to start test server")
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
        let hot_reload = HotReloadStreamingConfig::new(StreamingConfig::default());
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
        // `asap-common/dependencies/rs/asap_types/src/streaming_config.rs`
        // and the sample files in `asap-tools/execution-utilities/`.
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
        let added = post_body["agg_ids_added"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_u64().unwrap())
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(
            added,
            std::collections::HashSet::from([101u64, 102u64]),
            "expected both ids in added set"
        );

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

        // The underlying HotReloadStreamingConfig handle (cloned into
        // the server at setup) also reflects the swap — proving that
        // downstream consumers that re-snapshot would see the new
        // state.
        let direct_snap = hot_reload.snapshot();
        assert_eq!(direct_snap.aggregation_configs.len(), 2);
        assert!(direct_snap.aggregation_configs.contains_key(&101));
        assert!(direct_snap.aggregation_configs.contains_key(&102));
    }

    #[tokio::test]
    async fn test_streaming_config_hot_reload_missing_handle_503() {
        // setup_test_server() passes `None` for hot_reload → both
        // endpoints should return 503 with a clear error message.
        let server_port = setup_test_server().await;
        let client = Client::new();

        let get_resp = client
            .get(format!(
                "http://127.0.0.1:{server_port}/api/v1/streaming-config"
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(get_resp.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);

        let post_resp = client
            .post(format!(
                "http://127.0.0.1:{server_port}/api/v1/streaming-config"
            ))
            .body("anything")
            .send()
            .await
            .unwrap();
        assert_eq!(post_resp.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_streaming_config_hot_reload_rejects_bad_yaml() {
        let hot_reload = HotReloadStreamingConfig::new(StreamingConfig::default());
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

    /// Set up a test server with both a hot-reload handle AND a schema
    /// registry attached. Proves the Phase 2b wiring: a swap through
    /// the HTTP handler drives schema lifecycle transitions
    /// event-driven (sketch DB design §6).
    async fn setup_test_server_with_hot_reload_and_schemas(
        hot_reload: HotReloadStreamingConfig,
        schemas: Arc<crate::stores::sketch_db::SchemaRegistry>,
    ) -> u16 {
        let adapter_config =
            AdapterConfig::prometheus_promql("http://127.0.0.1:9999".to_string(), false);
        let config = HttpServerConfig {
            port: 0,
            handle_http_requests: true,
            adapter_config,
        };
        let inference_config = InferenceConfig::new(
            crate::data_model::QueryLanguage::promql,
            crate::data_model::CleanupPolicy::NoCleanup,
        );
        let streaming_config = Arc::new(StreamingConfig::default());
        let store = Arc::new(SimpleMapStore::new(
            streaming_config.clone(),
            crate::data_model::CleanupPolicy::NoCleanup,
        ));
        let query_engine = Arc::new(SimpleEngine::new(
            store.clone(),
            inference_config,
            streaming_config.clone(),
            15000,
            crate::data_model::QueryLanguage::promql,
        ));
        let server = HttpServer::new(config, query_engine, store, None)
            .with_hot_reload_config(hot_reload)
            .with_schemas(schemas);
        server
            .start_test_server()
            .await
            .expect("Failed to start test server")
    }

    #[tokio::test]
    async fn test_streaming_config_swap_drives_schema_reconcile() {
        use crate::stores::sketch_db::{AggStatus, SchemaRegistry};

        let hot_reload = HotReloadStreamingConfig::new(StreamingConfig::default());
        let schemas = Arc::new(SchemaRegistry::empty());
        let server_port =
            setup_test_server_with_hot_reload_and_schemas(hot_reload.clone(), schemas.clone())
                .await;
        let client = Client::new();

        // Empty registry at start.
        assert!(!schemas.is_writable(101));
        assert!(!schemas.is_writable(202));

        // POST a config with two agg_ids — the handler should swap
        // the config AND reconcile the registry.
        let yaml = r#"
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
            .body(yaml.to_string())
            .send()
            .await
            .expect("POST failed");
        assert!(resp.status().is_success());
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "success");
        // The new field from Phase 2b.
        let created = body["schemas_created"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_u64().unwrap())
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(
            created,
            std::collections::HashSet::from([101u64, 202u64]),
            "expected both agg_ids in schemas_created"
        );

        // Registry now has Active schemas for both ids.
        assert!(schemas.is_writable(101));
        assert!(schemas.is_writable(202));
        assert_eq!(schemas.get(101).unwrap().status(), AggStatus::Active);
        assert_eq!(schemas.get(202).unwrap().status(), AggStatus::Active);

        // Swap to a config that removes 101. Schema 101 should be
        // Retired (§6.3 barrier: is_writable(101) now false).
        let yaml2 = r#"
aggregations:
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
        let resp2 = client
            .post(format!(
                "http://127.0.0.1:{server_port}/api/v1/streaming-config"
            ))
            .header("content-type", "application/x-yaml")
            .body(yaml2.to_string())
            .send()
            .await
            .expect("POST failed");
        let body2: serde_json::Value = resp2.json().await.unwrap();
        let retired = body2["schemas_retired"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_u64().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(retired, vec![101u64]);

        assert!(
            !schemas.is_writable(101),
            "101 retired, should be unwritable"
        );
        assert!(schemas.is_writable(202), "202 still active");
        assert_eq!(schemas.get(101).unwrap().status(), AggStatus::Retired);
    }

    #[tokio::test]
    async fn test_streaming_config_swap_without_schemas_still_succeeds() {
        // If the HttpServer isn't wired with a schema registry, the
        // swap handler still works — it just omits schemas_created
        // and schemas_retired from the response.
        let hot_reload = HotReloadStreamingConfig::new(StreamingConfig::default());
        let server_port = setup_test_server_with_hot_reload(Some(hot_reload)).await;
        let client = Client::new();

        let yaml = r#"
aggregations:
  - aggregationId: 42
    aggregationType: Sum
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
        // Without a registry, the arrays are empty (not missing).
        assert_eq!(body["schemas_created"].as_array().unwrap().len(), 0);
        assert_eq!(body["schemas_retired"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_get_schemas_returns_active_and_retired_with_status_filter() {
        use crate::stores::sketch_db::SchemaRegistry;

        let hot_reload = HotReloadStreamingConfig::new(StreamingConfig::default());
        let schemas = Arc::new(SchemaRegistry::empty());
        let server_port =
            setup_test_server_with_hot_reload_and_schemas(hot_reload.clone(), schemas.clone())
                .await;
        let client = Client::new();

        // Push an initial config with two aggregations; then swap to
        // one, retiring the other. Exercises Active + Retired side by
        // side in the response.
        let yaml_two = r#"
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
  - aggregationId: 2
    aggregationType: Sum
    aggregationSubType: ''
    metric: m2
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
            .body(yaml_two.to_string())
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());

        // Retire agg 2 by pushing a config with only agg 1.
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
        // Sorted by agg_id — first is active, second is retired.
        assert_eq!(entries[0]["agg_id"], 1);
        assert_eq!(entries[0]["status"], "active");
        assert_eq!(entries[0]["metric_name"], "m1");
        assert!(entries[0]["retired_at_ms"].is_null());
        assert_eq!(entries[1]["agg_id"], 2);
        assert_eq!(entries[1]["status"], "retired");
        assert!(entries[1]["retired_at_ms"].is_u64());
        // Phase 6.4: accuracy_profile present on every schema. Sum
        // is exact → ε = δ = 0, kind = "exact".
        for e in entries {
            let ap = &e["accuracy_profile"];
            assert!(ap.is_object(), "accuracy_profile should be an object");
            assert_eq!(ap["kind"], "exact", "Sum agg → exact");
            assert_eq!(ap["epsilon"], 0.0);
            assert_eq!(ap["delta"], 0.0);
        }

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
        assert_eq!(body["schemas"][0]["agg_id"], 1);

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
        assert_eq!(body["schemas"][0]["agg_id"], 2);

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
    async fn test_get_schemas_without_registry_returns_503() {
        // No schema registry attached → 503.
        let hot_reload = HotReloadStreamingConfig::new(StreamingConfig::default());
        let server_port = setup_test_server_with_hot_reload(Some(hot_reload)).await;
        let client = Client::new();

        let resp = client
            .get(format!("http://127.0.0.1:{server_port}/api/v1/db/schemas"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_get_timeline_returns_segments_after_reconfigure() {
        use crate::stores::sketch_db::SchemaRegistry;

        let hot_reload = HotReloadStreamingConfig::new(StreamingConfig::default());
        let schemas = Arc::new(SchemaRegistry::empty());
        let server_port =
            setup_test_server_with_hot_reload_and_schemas(hot_reload.clone(), schemas.clone())
                .await;
        let client = Client::new();

        // Push initial config with agg 1 on metric "m". Then swap to
        // a config with agg 2 on the same metric — registry should
        // show timeline with agg 1 retired + agg 2 active.
        let post = |yaml: &str| {
            let yaml = yaml.to_string();
            let client = client.clone();
            async move {
                client
                    .post(format!(
                        "http://127.0.0.1:{server_port}/api/v1/streaming-config"
                    ))
                    .header("content-type", "application/x-yaml")
                    .body(yaml)
                    .send()
                    .await
                    .unwrap()
            }
        };
        let yaml = |id: u64| {
            format!(
                r#"
aggregations:
  - aggregationId: {id}
    aggregationType: Sum
    aggregationSubType: ''
    metric: m
    labels: {{ grouping: [], rollup: [], aggregated: [] }}
    parameters: {{}}
    windowSize: 60
    windowType: tumbling
    spatialFilter: ''
"#
            )
        };
        assert!(post(&yaml(1)).await.status().is_success());
        // Wait >1ms so the retire timestamp is strictly after agg 1's
        // creation; otherwise agg 1's ownership interval is zero-width
        // at ms resolution and timeline correctly skips it.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        assert!(post(&yaml(2)).await.status().is_success());

        // The ms range is effectively wall-clock; use [0, u64 far
        // future] to guarantee both segments fall in range.
        let resp = client
            .get(format!(
                "http://127.0.0.1:{server_port}/api/v1/db/timeline?metric=m&start_ms=0&end_ms=99999999999999"
            ))
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "success");
        assert_eq!(body["metric"], "m");
        // Two segments: retired agg 1 + active agg 2.
        assert_eq!(body["count"], 2);
        let segs = body["segments"].as_array().unwrap();
        assert_eq!(segs[0]["agg_id"], 1);
        assert_eq!(segs[0]["status"], "retired");
        assert_eq!(segs[0]["coverage"], "sketch");
        assert_eq!(segs[1]["agg_id"], 2);
        assert_eq!(segs[1]["status"], "active");
    }

    #[tokio::test]
    async fn test_get_timeline_missing_param_returns_400() {
        use crate::stores::sketch_db::SchemaRegistry;

        let hot_reload = HotReloadStreamingConfig::new(StreamingConfig::default());
        let schemas = Arc::new(SchemaRegistry::empty());
        let server_port =
            setup_test_server_with_hot_reload_and_schemas(hot_reload.clone(), schemas.clone())
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
    async fn test_get_timeline_without_registry_returns_503() {
        let hot_reload = HotReloadStreamingConfig::new(StreamingConfig::default());
        let server_port = setup_test_server_with_hot_reload(Some(hot_reload)).await;
        let client = Client::new();
        let resp = client
            .get(format!(
                "http://127.0.0.1:{server_port}/api/v1/db/timeline?metric=m&start_ms=0&end_ms=100"
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    }

    // ─── Phase 5d: backfill HTTP endpoint tests ─────────────────────────────

    /// Build a test server wired with a backfill registry and a
    /// `SchemaRegistry` that pre-registers the listed `agg_ids` as
    /// Active. `POST /api/v1/db/backfill` runs `create_checked`, which
    /// requires both registries — tests that hit that endpoint must
    /// populate the schema side here.
    async fn setup_test_server_with_backfill_and_schemas(
        registry: Arc<crate::stores::sketch_db::BackfillRegistry>,
        active_agg_ids: &[u64],
    ) -> u16 {
        let adapter_config =
            AdapterConfig::prometheus_promql("http://127.0.0.1:9999".to_string(), false);
        let config = HttpServerConfig {
            port: 0,
            handle_http_requests: true,
            adapter_config,
        };
        let inference_config = InferenceConfig::new(
            crate::data_model::QueryLanguage::promql,
            crate::data_model::CleanupPolicy::NoCleanup,
        );
        let streaming_config = Arc::new(StreamingConfig::default());
        let store = Arc::new(SimpleMapStore::new(
            streaming_config.clone(),
            crate::data_model::CleanupPolicy::NoCleanup,
        ));
        let query_engine = Arc::new(SimpleEngine::new(
            store.clone(),
            inference_config,
            streaming_config.clone(),
            15000,
            crate::data_model::QueryLanguage::promql,
        ));
        let schemas = {
            use asap_types::aggregation_config::AggregationConfig;
            use asap_types::enums::{AggregationType, WindowType};
            use promql_utilities::data_model::key_by_label_names::KeyByLabelNames;
            let mut map: std::collections::HashMap<u64, AggregationConfig> =
                std::collections::HashMap::new();
            for agg_id in active_agg_ids {
                let cfg = AggregationConfig::new(
                    *agg_id,
                    AggregationType::CountMinSketch,
                    String::new(),
                    std::collections::HashMap::new(),
                    KeyByLabelNames::empty(),
                    KeyByLabelNames::empty(),
                    KeyByLabelNames::empty(),
                    String::new(),
                    60,
                    60,
                    WindowType::Tumbling,
                    String::new(),
                    format!("metric_{agg_id}"),
                    None,
                    None,
                    None,
                    None,
                );
                map.insert(*agg_id, cfg);
            }
            let sc = StreamingConfig::new(map);
            Arc::new(crate::stores::sketch_db::SchemaRegistry::from_streaming_config(&sc))
        };
        let server = HttpServer::new(config, query_engine, store, None)
            .with_backfill_registry(registry)
            .with_schemas(schemas);
        server
            .start_test_server()
            .await
            .expect("Failed to start test server")
    }

    #[tokio::test]
    async fn test_backfill_full_lifecycle_through_http() {
        let registry = Arc::new(crate::stores::sketch_db::BackfillRegistry::new());
        let server_port =
            setup_test_server_with_backfill_and_schemas(registry.clone(), &[42]).await;
        let client = Client::new();

        // POST creates a Queued job.
        let req = serde_json::json!({
            "agg_id": 42,
            "start_ms": 100,
            "end_ms": 500,
            "source": { "Prometheus": { "url": "http://prom.local" } },
            "windows_total": 4,
        });
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
        assert_eq!(body["job"]["agg_id"], 42);
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
            crate::stores::sketch_db::BackfillStatus::Cancelled
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
        let registry = Arc::new(crate::stores::sketch_db::BackfillRegistry::new());
        let server_port = setup_test_server_with_backfill_and_schemas(registry, &[1]).await;
        let client = Client::new();

        let req = serde_json::json!({
            "agg_id": 1,
            "start_ms": 500,
            "end_ms": 100,
            "source": { "Prometheus": { "url": "http://prom.local" } },
            "windows_total": 1,
        });
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
        let registry = Arc::new(crate::stores::sketch_db::BackfillRegistry::new());
        let server_port = setup_test_server_with_backfill_and_schemas(registry, &[]).await;
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
        let hot_reload = HotReloadStreamingConfig::new(StreamingConfig::default());
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
            "windows_total": 1,
        });
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
        let registry = Arc::new(crate::stores::sketch_db::BackfillRegistry::new());
        let server_port = setup_test_server_with_backfill_and_schemas(registry, &[]).await;
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
        let registry = Arc::new(crate::stores::sketch_db::BackfillRegistry::new());
        // Empty schema registry — agg_id 42 is unknown.
        let server_port = setup_test_server_with_backfill_and_schemas(registry, &[]).await;
        let client = Client::new();
        let req = serde_json::json!({
            "agg_id": 42,
            "start_ms": 100,
            "end_ms": 500,
            "source": { "Prometheus": { "url": "http://prom.local" } },
            "windows_total": 1,
        });
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
        let registry = Arc::new(crate::stores::sketch_db::BackfillRegistry::new());
        // Schema registered at `now` — any `end_ms` > created_at_ms
        // overlaps live ingest.
        let server_port = setup_test_server_with_backfill_and_schemas(registry, &[7]).await;
        let client = Client::new();
        let future_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 3_600_000;
        let req = serde_json::json!({
            "agg_id": 7,
            "start_ms": 0,
            "end_ms": future_ms,
            "source": { "Prometheus": { "url": "http://prom.local" } },
            "windows_total": 1,
        });
        let resp = client
            .post(format!("http://127.0.0.1:{server_port}/api/v1/db/backfill"))
            .json(&req)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::CONFLICT);
    }

    // ── Phase-6 follow-up: EngineRouter wired into the HTTP query path ──────
    //
    // These tests cover the deliverable in the
    // `feat/http-server-wire-engine-router` PR — every query that
    // arrives through `/api/v1/query` now consults the
    // `StreamingConfig::storage_backend()` axis and dispatches via the
    // `EngineRouter` for non-warm-tier metrics. The wire response
    // carries a `data_source: <id>` info-line so dashboards / e2e
    // tests can byte-compare which engine answered.

    use crate::engines::{EngineCapabilities, EngineError, QueryEngine, QueryResult};
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
    }

    /// Build an `HttpServer` whose router holds the supplied set of
    /// `QueryEngine`s. The hot-reload `StreamingConfig` is pinned at
    /// `metric_storage_backend` so query dispatch follows the
    /// requested capability axis. Returns the bound port + the
    /// `HotReloadStreamingConfig` handle so tests can swap the
    /// `storage_backend` mid-flight if they need to.
    async fn setup_test_server_with_router(
        metric_storage_backend: StorageBackend,
        extra_engines: Vec<Arc<dyn QueryEngine>>,
    ) -> u16 {
        let adapter_config = AdapterConfig::prometheus_promql(
            "http://127.0.0.1:9999".to_string(),
            false,
        );
        let config = HttpServerConfig {
            port: 0,
            handle_http_requests: true,
            adapter_config,
        };
        let inference_config = InferenceConfig::new(
            crate::data_model::QueryLanguage::promql,
            crate::data_model::CleanupPolicy::NoCleanup,
        );
        // Pin `storage_backend` on the streaming config so the http
        // dispatcher reads it back through the hot-reload handle.
        let streaming_cfg =
            StreamingConfig::with_storage_backend(Default::default(), metric_storage_backend);
        let streaming_arc = Arc::new(streaming_cfg);
        let hot_reload = HotReloadStreamingConfig::from_arc(streaming_arc.clone());
        let store = Arc::new(SimpleMapStore::new(
            streaming_arc.clone(),
            crate::data_model::CleanupPolicy::NoCleanup,
        ));
        let query_engine = Arc::new(SimpleEngine::new(
            store.clone(),
            inference_config,
            streaming_arc,
            15000,
            crate::data_model::QueryLanguage::promql,
        ));
        let mut server = HttpServer::new(config, query_engine, store, None)
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
    /// at `SketchWarmTier` (the realistic deploy state); the routing
    /// table is what flips per-metric dispatch over to the
    /// `EngineRouter`. This proves the production code path
    /// (`process_query_request → resolve_metric_storage → routing
    /// table lookup`), as opposed to the
    /// `setup_test_server_with_router` helper above which mocks the
    /// resolution by pinning `streaming_cfg.storage_backend` directly.
    async fn setup_test_server_with_routing_table(
        routing: crate::data_model::BackendStorageRouting,
        extra_engines: Vec<Arc<dyn QueryEngine>>,
    ) -> u16 {
        let adapter_config = AdapterConfig::prometheus_promql(
            "http://127.0.0.1:9999".to_string(),
            false,
        );
        let config = HttpServerConfig {
            port: 0,
            handle_http_requests: true,
            adapter_config,
        };
        let inference_config = InferenceConfig::new(
            crate::data_model::QueryLanguage::promql,
            crate::data_model::CleanupPolicy::NoCleanup,
        );
        // Streaming-config stays on the default `SketchWarmTier` axis
        // — exactly what the production deploy looks like (the YAML
        // loader doesn't parse `storage_backend`). All routing
        // decisions must come from the per-metric routing table.
        let streaming_cfg = StreamingConfig::default();
        let streaming_arc = Arc::new(streaming_cfg);
        let hot_reload = HotReloadStreamingConfig::from_arc(streaming_arc.clone());
        let store = Arc::new(SimpleMapStore::new(
            streaming_arc.clone(),
            crate::data_model::CleanupPolicy::NoCleanup,
        ));
        let query_engine = Arc::new(SimpleEngine::new(
            store.clone(),
            inference_config,
            streaming_arc,
            15000,
            crate::data_model::QueryLanguage::promql,
        ));
        let mut server = HttpServer::new(config, query_engine, store, None)
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
    /// (`HttpServer::new` always registers `SimpleEngine`), so the
    /// helper drops in a router by hand via the same builder
    /// surface — but registers nothing, then asks the router-path
    /// dispatch to route an archive metric. Used by the
    /// `503 NoEngineRegistered` test.
    async fn setup_test_server_with_empty_router(
        metric_storage_backend: StorageBackend,
    ) -> u16 {
        // `HttpServer::new` always registers SimpleEngine for the
        // warm tier. To force `NoEngineRegistered` we point the
        // metric at a backend whose data_source_id doesn't match
        // any registered engine — since `HttpServer::new` only
        // registers SimpleEngine (sketch_warm), routing a
        // `ColdJsonlFallback`-only metric trips the empty path
        // (compatible_storage_backends = [ColdJsonlFallback], no
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
            .unwrap_or_else(|| {
                panic!(
                    "expected `infos` array in response body, got {body}",
                )
            });
        let want = format!("data_source: {expected}");
        assert!(
            infos.iter().any(|v| v.as_str() == Some(&want)),
            "expected `{want}` in infos, got {infos:?}",
        );
    }

    #[tokio::test]
    async fn http_routes_warm_tier_metric_to_simple_engine() {
        // Default (no hot-reload) → `SketchWarmTier`. The handler
        // takes the direct `SimpleEngine::handle_query` path; the
        // response's `infos` array carries `data_source: sketch_warm`
        // so callers can byte-compare which engine answered.
        let server_port =
            setup_test_server_with_router(StorageBackend::SketchWarmTier, Vec::new()).await;
        let client = Client::new();
        let resp = client
            .get(format!("http://127.0.0.1:{server_port}/api/v1/query"))
            .query(&[("query", "sum_over_time(foo[5m])"), ("time", "1700000000")])
            .send()
            .await
            .expect("Failed to send request");
        assert!(
            resp.status().is_success(),
            "warm-tier dispatch must return 2xx; got {}",
            resp.status()
        );
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_data_source(&body, "sketch_warm");
    }

    #[tokio::test]
    async fn http_routes_archive_metric_to_gorilla_engine() {
        // Pin `storage_backend = GorillaS3Archive` and register a
        // `MockQueryEngine` under that id. The handler must dispatch
        // through the router (not SimpleEngine) and the response's
        // `infos` array must carry `data_source: gorilla_archive`.
        let (gorilla, gorilla_calls) =
            MockQueryEngine::new(StorageBackend::GorillaS3Archive, MockOutcome::OkEmpty);
        let server_port = setup_test_server_with_router(
            StorageBackend::GorillaS3Archive,
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
            "archive dispatch must return 2xx; got {}",
            resp.status()
        );
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_data_source(&body, "gorilla_archive");
        assert_eq!(
            gorilla_calls.load(Ordering::SeqCst),
            1,
            "Gorilla mock engine should have been hit exactly once",
        );
    }

    #[tokio::test]
    async fn http_query_with_no_storage_config_defaults_to_warm_tier() {
        // `StreamingConfig::default()` has `storage_backend =
        // SketchWarmTier` (per the `#[serde(default)]` on the
        // field — see `streaming_config.rs`). A server set up
        // without a hot-reload handle still infers warm-tier and
        // takes the SimpleEngine direct path. Back-compat for
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
        assert_data_source(&body, "sketch_warm");
    }

    #[tokio::test]
    async fn http_returns_503_when_no_engines_registered() {
        // Pin `storage_backend = ColdJsonlFallback` but register no
        // engine for that id (only `SimpleEngine` is registered, and
        // it lives under `sketch_warm`). The router walks
        // `compatible_storage_backends = [ColdJsonlFallback]` and
        // bails out with `NoEngineRegistered`, which the HTTP layer
        // surfaces as 503.
        let server_port =
            setup_test_server_with_empty_router(StorageBackend::ColdJsonlFallback).await;
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
                use crate::stores::sketch_db::accuracy::{AccuracyEnvelope, AccuracyProfile};
                Ok(QueryResult::vector(Vec::new(), 0)
                    .with_accuracy(AccuracyEnvelope::single(AccuracyProfile::exact())))
            }
            fn capabilities(&self) -> EngineCapabilities {
                EngineCapabilities {
                    data_source_id: StorageBackend::GorillaS3Archive.data_source_id(),
                    storage_backend: StorageBackend::GorillaS3Archive,
                    supports_streams_above_bytes: 1024 * 1024,
                }
            }
        }
        let server_port = setup_test_server_with_router(
            StorageBackend::GorillaS3Archive,
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
        assert_data_source(&body, "gorilla_archive");
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
    async fn http_router_falls_through_to_jsonl_when_archive_fails() {
        // Optional (graceful fallback) — verifies that a
        // `DoubleWrite` deploy whose archive engine errors does NOT
        // surface a 5xx; the router walks the compatibility list and
        // ColdJsonlFallback answers. Pins the §8 behaviour of
        // `design-gorilla-s3-cold-engine.md`.
        let (gorilla_failing, gorilla_calls) =
            MockQueryEngine::new(StorageBackend::GorillaS3Archive, MockOutcome::Backend);
        let (jsonl_ok, jsonl_calls) =
            MockQueryEngine::new(StorageBackend::ColdJsonlFallback, MockOutcome::OkEmpty);
        // SimpleEngine is registered under `sketch_warm` by
        // `HttpServer::new`; for `DoubleWrite` + `Approximate` the
        // compatibility list is
        // `[SketchWarmTier, GorillaS3Archive, ColdJsonlFallback]`.
        // SimpleEngine is configured with no agg ids, so its
        // `handle_query` returns `None` → `EngineError::CapabilityMiss`,
        // which the router tolerates and falls through. Then Gorilla
        // fails with `Backend`, so JSONL must answer.
        let server_port = setup_test_server_with_router(
            StorageBackend::DoubleWrite,
            vec![
                gorilla_failing as Arc<dyn QueryEngine>,
                jsonl_ok as Arc<dyn QueryEngine>,
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
            "double-write fallback must answer 2xx; got {}",
            resp.status()
        );
        // We dispatched as `metric_storage = DoubleWrite`, so the
        // `data_source` info-line reflects the *requested* axis (the
        // router's `execute` doesn't expose which member of the
        // failover list answered). Verifying the fallback was
        // exercised happens via call counts.
        assert_eq!(gorilla_calls.load(Ordering::SeqCst), 1);
        assert_eq!(jsonl_calls.load(Ordering::SeqCst), 1);
    }

    // ── Issue #46 production-path coverage: BackendStorageRouting ─────────────
    //
    // The tests above (e.g. `http_routes_archive_metric_to_gorilla_engine`)
    // mock the routing decision by pinning `streaming_cfg.storage_backend
    // = GorillaS3Archive` directly. That proves the dispatch BRANCH is
    // wired, but not the production code path — in real deploys the
    // streaming-config YAML loader drops `storage_backend` (it always
    // defaults to `SketchWarmTier`), so the issue-46 v2 demo's queries
    // never reached the EngineRouter. The tests below exercise the
    // **production path** end-to-end: streaming config stays default,
    // a per-metric `BackendStorageRouting` table is loaded at startup
    // (mirroring `--backend-storage-routing` on `precompute_engine`),
    // and the handler must consult the table on every request.

    #[tokio::test]
    async fn http_production_path_routes_archive_metric_via_routing_table() {
        // Production path: streaming-config single axis stays on
        // `SketchWarmTier` (the YAML loader's default), but the
        // per-metric routing table flips `http_requests_total` to
        // `gorilla_archive`. The handler must extract the metric name
        // from the PromQL AST, look it up, and dispatch through the
        // EngineRouter — landing the `data_source: gorilla_archive`
        // info-line on the response.
        let mut metrics = std::collections::HashMap::new();
        metrics.insert(
            "http_requests_total".to_string(),
            StorageBackend::GorillaS3Archive,
        );
        let routing = crate::data_model::BackendStorageRouting::new_from_single_targets(
            StorageBackend::SketchWarmTier,
            metrics,
        );
        let (gorilla, gorilla_calls) =
            MockQueryEngine::new(StorageBackend::GorillaS3Archive, MockOutcome::OkEmpty);
        let server_port = setup_test_server_with_routing_table(
            routing,
            vec![gorilla as Arc<dyn QueryEngine>],
        )
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
        assert_data_source(&body, "gorilla_archive");
        assert_eq!(
            gorilla_calls.load(Ordering::SeqCst),
            1,
            "GorillaQueryEngine must be hit exactly once on the production path",
        );
    }

    #[tokio::test]
    async fn http_production_path_unlisted_metric_falls_back_to_warm_tier() {
        // The same routing table only overrides `http_requests_total`;
        // a query against a different metric must take the warm-tier
        // direct-dispatch path (no `EngineRouter` round-trip).
        let mut metrics = std::collections::HashMap::new();
        metrics.insert(
            "http_requests_total".to_string(),
            StorageBackend::GorillaS3Archive,
        );
        let routing = crate::data_model::BackendStorageRouting::new_from_single_targets(
            StorageBackend::SketchWarmTier,
            metrics,
        );
        let server_port =
            setup_test_server_with_routing_table(routing, Vec::new()).await;
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
            "warm-tier fallback must return 2xx; got {}",
            resp.status(),
        );
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_data_source(&body, "sketch_warm");
    }

    #[tokio::test]
    async fn http_production_path_default_axis_routes_all_metrics() {
        // Routing table with no per-metric overrides but a non-default
        // top-level `default: gorilla_s3_archive` — every metric must
        // route through the router. Pins the §8 "all-metrics-archive"
        // deploy mode.
        let routing = crate::data_model::BackendStorageRouting::new_from_single_targets(
            StorageBackend::GorillaS3Archive,
            std::collections::HashMap::new(),
        );
        let (gorilla, gorilla_calls) =
            MockQueryEngine::new(StorageBackend::GorillaS3Archive, MockOutcome::OkEmpty);
        let server_port = setup_test_server_with_routing_table(
            routing,
            vec![gorilla as Arc<dyn QueryEngine>],
        )
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
        assert_data_source(&body, "gorilla_archive");
        assert_eq!(gorilla_calls.load(Ordering::SeqCst), 1);
    }

    // ── v7 dual-routing production-path coverage ──────────────────────────────
    //
    // v7 lets one metric fan out to multiple `(backend,
    // applies_to_query_shape)` targets. The two tests below mirror
    // `http_production_path_routes_archive_metric_via_routing_table`
    // — same setup, but the routing table has TWO targets for
    // `http_requests_total`: a default warm-tier slot and a
    // cold-archive slot scoped to `[count, topk, rate_post_hoc]`.
    // A `count(...)` query must land on the archive; a
    // `quantile_over_time(...)` query must land on the warm tier.

    #[tokio::test]
    async fn http_v7_dual_routing_count_lands_on_archive() {
        use crate::data_model::{
            BackendStorageRouting, QueryShape, RoutingTarget,
        };
        let mut metrics = std::collections::HashMap::new();
        metrics.insert(
            "http_requests_total".to_string(),
            vec![
                RoutingTarget::always(StorageBackend::SketchWarmTier),
                RoutingTarget::for_shapes(
                    StorageBackend::GorillaS3Archive,
                    vec![QueryShape::Count, QueryShape::Topk, QueryShape::RatePostHoc],
                ),
            ],
        );
        let routing = BackendStorageRouting::new(StorageBackend::SketchWarmTier, metrics);

        let (gorilla, gorilla_calls) =
            MockQueryEngine::new(StorageBackend::GorillaS3Archive, MockOutcome::OkEmpty);
        let server_port = setup_test_server_with_routing_table(
            routing,
            vec![gorilla as Arc<dyn QueryEngine>],
        )
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
        assert_data_source(&body, "gorilla_archive");
        assert_eq!(
            gorilla_calls.load(Ordering::SeqCst),
            1,
            "v7 dual-routing: count must hit the archive engine",
        );
    }

    #[tokio::test]
    async fn http_v7_dual_routing_quantile_stays_on_warm_tier() {
        use crate::data_model::{
            BackendStorageRouting, QueryShape, RoutingTarget,
        };
        let mut metrics = std::collections::HashMap::new();
        metrics.insert(
            "http_requests_total".to_string(),
            vec![
                RoutingTarget::always(StorageBackend::SketchWarmTier),
                RoutingTarget::for_shapes(
                    StorageBackend::GorillaS3Archive,
                    vec![QueryShape::Count, QueryShape::Topk, QueryShape::RatePostHoc],
                ),
            ],
        );
        let routing = BackendStorageRouting::new(StorageBackend::SketchWarmTier, metrics);

        // Register a Gorilla mock so a misroute would surface as a
        // failed assertion rather than a silent fall-through. The
        // mock starts with 0 calls; a quantile must NOT touch it.
        let (gorilla, gorilla_calls) =
            MockQueryEngine::new(StorageBackend::GorillaS3Archive, MockOutcome::OkEmpty);
        let server_port = setup_test_server_with_routing_table(
            routing,
            vec![gorilla as Arc<dyn QueryEngine>],
        )
        .await;

        let client = Client::new();
        let resp = client
            .get(format!("http://127.0.0.1:{server_port}/api/v1/query"))
            .query(&[
                (
                    "query",
                    "quantile_over_time(0.99, http_requests_total[1m])",
                ),
                ("time", "1700000000"),
            ])
            .send()
            .await
            .expect("Failed to send request");
        // Warm tier path returns 2xx with `data_source: sketch_warm`
        // (the SimpleEngine returns None for this unconfigured
        // metric, but the handler still annotates the wire response
        // with the warm-tier source).
        assert!(
            resp.status().is_success(),
            "v7 dual-routing: quantile must dispatch and return 2xx; got {}",
            resp.status()
        );
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_data_source(&body, "sketch_warm");
        assert_eq!(
            gorilla_calls.load(Ordering::SeqCst),
            0,
            "v7 dual-routing: quantile must NOT hit the archive engine",
        );
    }

}

// ── Controller integration: PrecomputeJob execution ──────────────────────────

/// Request body from DataCollector controller's PrecomputeJob.
#[derive(serde::Deserialize)]
struct PrecomputeJobRequest {
    /// PromQL expression to evaluate against stored sketches.
    query_expr: String,
    /// Window granularity in seconds.
    #[serde(default)]
    granularity_secs: u64,
    /// Start timestamp (unix seconds). 0 = use earliest available.
    #[serde(default)]
    start: f64,
    /// End timestamp (unix seconds). 0 = use latest available.
    #[serde(default)]
    end: f64,
}

/// Execute a precompute job from the DataCollector controller.
///
/// POST /api/v1/precompute
///
/// The controller creates PrecomputeJobs when a query's upper sub-tree
/// (e.g., TopK, HistogramQuantile) requires evaluation on merged sketches.
/// This endpoint receives that job and runs it against the SimpleMapStore.
async fn handle_precompute_job(
    State(state): State<AppState>,
    axum::Json(req): axum::Json<PrecomputeJobRequest>,
) -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    let time = if req.end > 0.0 {
        req.end
    } else {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64()
    };

    info!(
        query = %req.query_expr,
        start = %req.start,
        end = %req.end,
        granularity_secs = %req.granularity_secs,
        time = %time,
        "Executing precompute job from controller"
    );

    match state.query_engine.handle_query_promql(req.query_expr, time) {
        Some((key_by, result)) => {
            let body = serde_json::json!({
                "status": "success",
                "data": {
                    "result_type": "precompute",
                    "key_by": format!("{:?}", key_by),
                    "result": format!("{:?}", result),
                }
            });
            (StatusCode::OK, axum::Json(body)).into_response()
        }
        None => {
            // Query not answerable by sketches — return 404 with hint
            let body = serde_json::json!({
                "status": "error",
                "error": "query not answerable by stored sketches",
                "hint": "ensure the metric has been ingested via OTLP/Kafka and a matching query_config exists"
            });
            (StatusCode::NOT_FOUND, axum::Json(body)).into_response()
        }
    }
}

/// Health check endpoint for DataCollector controller to verify backend is alive.
async fn handle_health() -> &'static str {
    "ok"
}

/// Return list of metrics currently in the store.
async fn handle_store_metrics(State(state): State<AppState>) -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    match state.store.get_earliest_timestamp_per_aggregation_id() {
        Ok(timestamps) => {
            let body = serde_json::json!({
                "status": "success",
                "aggregation_count": timestamps.len(),
                "earliest_timestamps": timestamps,
            });
            (StatusCode::OK, axum::Json(body)).into_response()
        }
        Err(e) => {
            let body = serde_json::json!({
                "status": "error",
                "error": format!("{}", e),
            });
            (StatusCode::INTERNAL_SERVER_ERROR, axum::Json(body)).into_response()
        }
    }
}

// ─── StreamingConfig hot-reload (PR E) ───────────────────────────────────
//
// `GET /api/v1/streaming-config`  — return the currently active config
//                                   as JSON (debug / verification).
// `POST /api/v1/streaming-config` — accept a YAML body, parse, and
//                                   atomically swap via ArcSwap.
//
// Phase 1 scope: the swap only takes effect for new readers that
// snapshot after the swap. `SimpleEngine`, the ingest router, and
// in-flight precompute workers all hold startup snapshots today and
// ignore the swap until they are rebuilt — see the module doc on
// `HotReloadStreamingConfig` for the full contract. Tests POST a new
// config and verify it via the GET endpoint; controller integration
// and per-query re-snapshot are phase 2.

async fn handle_get_streaming_config(State(state): State<AppState>) -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    let Some(handle) = state.hot_reload_config else {
        let body = serde_json::json!({
            "status": "error",
            "error": "hot-reload handle not attached; backend was built without HttpServer::with_hot_reload_config",
        });
        return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body)).into_response();
    };
    let snap = handle.snapshot();
    let body = serde_json::json!({
        "status": "success",
        "aggregation_count": snap.aggregation_configs.len(),
        "aggregation_ids": snap.aggregation_configs.keys().copied().collect::<Vec<_>>(),
        "streaming_config": &*snap,
    });
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
            "error": "hot-reload handle not attached; backend was built without HttpServer::with_hot_reload_config",
        });
        return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body)).into_response();
    };

    let yaml_text = match std::str::from_utf8(&body) {
        Ok(s) => s,
        Err(e) => {
            let body = serde_json::json!({
                "status": "error",
                "error": format!("request body is not valid UTF-8: {e}"),
            });
            return (StatusCode::BAD_REQUEST, axum::Json(body)).into_response();
        }
    };
    let yaml_value: serde_yaml::Value = match serde_yaml::from_str(yaml_text) {
        Ok(v) => v,
        Err(e) => {
            let body = serde_json::json!({
                "status": "error",
                "error": format!("YAML parse error: {e}"),
            });
            return (StatusCode::BAD_REQUEST, axum::Json(body)).into_response();
        }
    };
    let new_config =
        match asap_types::streaming_config::StreamingConfig::from_yaml_data(&yaml_value, None) {
            Ok(c) => c,
            Err(e) => {
                let body = serde_json::json!({
                    "status": "error",
                    "error": format!("StreamingConfig build error: {e}"),
                });
                return (StatusCode::BAD_REQUEST, axum::Json(body)).into_response();
            }
        };

    let new_ids: HashSet<u64> = new_config.aggregation_configs.keys().copied().collect();
    let old_arc = handle.swap(new_config);
    let old_ids: HashSet<u64> = old_arc.aggregation_configs.keys().copied().collect();
    let added: Vec<u64> = new_ids.difference(&old_ids).copied().collect();
    let removed: Vec<u64> = old_ids.difference(&new_ids).copied().collect();

    if !removed.is_empty() {
        warn!(
            "streaming-config hot-reload removed agg_ids {:?} — any in-flight \
             precompute worker groups for these ids will continue with their \
             construction-time config until they close naturally (phase 1 \
             limitation; see HotReloadStreamingConfig module doc)",
            removed
        );
    }

    // Phase 2b of the sketch DB design (docs/design-sketch-db.md §6):
    // drive schema lifecycle transitions event-driven from the swap
    // handler instead of running on every ingest batch. When attached,
    // the SchemaRegistry's reconcile adds new agg_ids as Active
    // schemas and retires removed agg_ids (scheduling their data for
    // expiry after the retirement retention).
    //
    // If `schemas` isn't attached (tests, legacy deployments), the
    // per-batch reconcile in IngestState still handles it — just
    // with up to one batch worth of latency.
    let (schema_added, schema_retired) = if let Some(schemas) = &state.schemas {
        let snap = handle.snapshot();
        let summary = schemas.reconcile(snap.as_ref());
        (summary.added, summary.retired)
    } else {
        (Vec::new(), Vec::new())
    };

    let body = serde_json::json!({
        "status": "success",
        "agg_ids_added": added,
        "agg_ids_removed": removed,
        "new_aggregation_count": new_ids.len(),
        "schemas_created": schema_added,
        "schemas_retired": schema_retired,
    });
    (StatusCode::OK, axum::Json(body)).into_response()
}

/// §15.2 of the sketch DB design: expose the `SchemaRegistry` over
/// HTTP so operators and the controller can inspect agg lifecycle
/// state without attaching a debugger. Filter by `?status=` —
/// `active` / `retired` / `expired` / `all` (default `all`).
async fn handle_get_schemas(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> axum::response::Response {
    use crate::stores::sketch_db::AggStatus;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    let Some(schemas) = state.schemas else {
        let body = serde_json::json!({
            "status": "error",
            "error": "schema registry not attached; backend was built without HttpServer::with_schemas",
        });
        return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body)).into_response();
    };

    let filter = params.get("status").map(String::as_str).unwrap_or("all");
    let statuses: &[AggStatus] = match filter {
        "active" => &[AggStatus::Active],
        "retired" => &[AggStatus::Retired],
        "expired" => &[AggStatus::Expired],
        "all" => &[AggStatus::Active, AggStatus::Retired, AggStatus::Expired],
        other => {
            let body = serde_json::json!({
                "status": "error",
                "error": format!(
                    "unknown status filter '{other}'; expected one of active|retired|expired|all",
                ),
            });
            return (StatusCode::BAD_REQUEST, axum::Json(body)).into_response();
        }
    };

    let mut entries: Vec<serde_json::Value> = Vec::new();
    for status in statuses {
        for s in schemas.list_by_status(*status) {
            entries.push(schema_to_json(&s));
        }
    }
    entries.sort_by_key(|v| v.get("agg_id").and_then(|x| x.as_u64()).unwrap_or(0));

    let body = serde_json::json!({
        "status": "success",
        "count": entries.len(),
        "schemas": entries,
    });
    (StatusCode::OK, axum::Json(body)).into_response()
}

fn status_str(s: crate::stores::sketch_db::AggStatus) -> &'static str {
    use crate::stores::sketch_db::AggStatus;
    match s {
        AggStatus::Active => "active",
        AggStatus::Retired => "retired",
        AggStatus::Expired => "expired",
    }
}

fn schema_to_json(s: &crate::stores::sketch_db::AggSchema) -> serde_json::Value {
    serde_json::json!({
        "agg_id": s.agg_id,
        "metric_name": s.metric_name,
        "status": status_str(s.status()),
        "created_at_ms": s.created_at_ms,
        "retired_at_ms": s.retired_at_ms,
        "expires_at_ms": s.expires_at_ms,
        "aggregation_type": format!("{:?}", s.config.aggregation_type),
        "accuracy_profile": s.accuracy_profile(),
    })
}

/// `POST /api/v1/db/schemas/:agg_id/retire` — manually transition an
/// Active schema to Retired (kicking off the retirement retention
/// clock). Idempotent: already-Retired or Expired schemas return 200
/// with their current state unchanged. Returns 404 if the agg_id is
/// unknown, 503 if no registry is attached.
async fn handle_post_schema_retire(
    State(state): State<AppState>,
    axum::extract::Path(agg_id): axum::extract::Path<u64>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let Some(schemas) = state.schemas else {
        let body = serde_json::json!({
            "status": "error",
            "error": "schema registry not attached",
        });
        return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body)).into_response();
    };
    match schemas.force_retire(agg_id) {
        Some(schema) => {
            let body = serde_json::json!({
                "status": "success",
                "schema": schema_to_json(&schema),
            });
            (StatusCode::OK, axum::Json(body)).into_response()
        }
        None => {
            let body = serde_json::json!({
                "status": "error",
                "error": format!("agg_id {agg_id} not found"),
            });
            (StatusCode::NOT_FOUND, axum::Json(body)).into_response()
        }
    }
}

/// `POST /api/v1/db/schemas/:agg_id/expire` — manually transition a
/// schema to Expired immediately. The next `SchemaEvictionService`
/// tick drops the agg's data + removes the schema. Idempotent;
/// 404 if the agg_id is unknown, 503 if no registry is attached.
async fn handle_post_schema_expire(
    State(state): State<AppState>,
    axum::extract::Path(agg_id): axum::extract::Path<u64>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let Some(schemas) = state.schemas else {
        let body = serde_json::json!({
            "status": "error",
            "error": "schema registry not attached",
        });
        return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body)).into_response();
    };
    match schemas.force_expire(agg_id) {
        Some(schema) => {
            let body = serde_json::json!({
                "status": "success",
                "schema": schema_to_json(&schema),
            });
            (StatusCode::OK, axum::Json(body)).into_response()
        }
        None => {
            let body = serde_json::json!({
                "status": "error",
                "error": format!("agg_id {agg_id} not found"),
            });
            (StatusCode::NOT_FOUND, axum::Json(body)).into_response()
        }
    }
}

fn coverage_str(c: crate::stores::sketch_db::TimelineCoverage) -> &'static str {
    use crate::stores::sketch_db::TimelineCoverage;
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

    let Some(schemas) = state.schemas else {
        let body = serde_json::json!({
            "status": "error",
            "error": "schema registry not attached; backend was built without HttpServer::with_schemas",
        });
        return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body)).into_response();
    };

    let Some(metric) = params.get("metric") else {
        let body = serde_json::json!({
            "status": "error",
            "error": "missing required query parameter 'metric'",
        });
        return (StatusCode::BAD_REQUEST, axum::Json(body)).into_response();
    };

    let parse_u64 = |key: &str| -> Result<u64, Box<axum::response::Response>> {
        match params.get(key) {
            Some(s) => s.parse::<u64>().map_err(|e| {
                let body = serde_json::json!({
                    "status": "error",
                    "error": format!("query parameter '{key}' is not a valid u64: {e}"),
                });
                Box::new((StatusCode::BAD_REQUEST, axum::Json(body)).into_response())
            }),
            None => {
                let body = serde_json::json!({
                    "status": "error",
                    "error": format!("missing required query parameter '{key}'"),
                });
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

    let segments = schemas.timeline_for_metric(metric, start_ms, end_ms);
    let entries: Vec<serde_json::Value> = segments
        .iter()
        .map(|s| {
            serde_json::json!({
                "agg_id": s.agg_id,
                "start_ms": s.start_ms,
                "end_ms": s.end_ms,
                "status": status_str(s.status),
                "coverage": coverage_str(s.coverage),
            })
        })
        .collect();

    let body = serde_json::json!({
        "status": "success",
        "metric": metric,
        "start_ms": start_ms,
        "end_ms": end_ms,
        "count": entries.len(),
        "segments": entries,
    });
    (StatusCode::OK, axum::Json(body)).into_response()
}

// ─── §10 / §15 backfill API ──────────────────────────────────────────────────

/// Request body for `POST /api/v1/db/backfill`.
#[derive(serde::Deserialize)]
struct CreateBackfillJobRequest {
    agg_id: u64,
    start_ms: u64,
    end_ms: u64,
    source: crate::stores::sketch_db::BackfillSource,
    windows_total: u64,
}

fn backfill_status_str(s: &crate::stores::sketch_db::BackfillStatus) -> &'static str {
    use crate::stores::sketch_db::BackfillStatus;
    match s {
        BackfillStatus::Queued => "queued",
        BackfillStatus::Running => "running",
        BackfillStatus::Complete => "complete",
        BackfillStatus::Failed => "failed",
        BackfillStatus::Cancelled => "cancelled",
    }
}

fn backfill_job_to_json(job: &crate::stores::sketch_db::BackfillJob) -> serde_json::Value {
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
        "error_message": job.error_message,
    })
}

fn service_unavailable_no_backfill() -> axum::response::Response {
    use axum::response::IntoResponse;
    let body = serde_json::json!({
        "status": "error",
        "error": "backfill registry not attached; backend was built without HttpServer::with_backfill_registry",
    });
    (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body)).into_response()
}

/// `POST /api/v1/db/backfill` — create a queued backfill job.
///
/// Validates the request against the §10.5 invariants via
/// `BackfillRegistry::create_checked`:
/// * `agg_id` must be known to the schema registry → 404 on miss.
/// * `end_ms` must not extend past the agg's `created_at_ms` (no
///   race against live ingest) → 409 on overlap.
/// * `start_ms` must be within the SimpleMapStore data-retention
///   window when one is configured (Method B) → 409 on stale range.
///
/// 400 on malformed body / inverted range; 503 when no registry or
/// schema registry is attached.
async fn handle_post_backfill_job(
    State(state): State<AppState>,
    body: axum::body::Bytes,
) -> axum::response::Response {
    use crate::stores::sketch_db::CreateError;
    use axum::response::IntoResponse;

    let Some(registry) = state.backfill else {
        return service_unavailable_no_backfill();
    };
    let Some(schemas) = state.schemas else {
        let body = serde_json::json!({
            "status": "error",
            "error": "schema registry not attached; backfill retention check requires HttpServer::with_schemas",
        });
        return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body)).into_response();
    };

    let req: CreateBackfillJobRequest = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            let body = serde_json::json!({
                "status": "error",
                "error": format!("invalid request body: {e}"),
            });
            return (StatusCode::BAD_REQUEST, axum::Json(body)).into_response();
        }
    };
    if req.start_ms >= req.end_ms {
        let body = serde_json::json!({
            "status": "error",
            "error": format!(
                "start_ms {} must be < end_ms {}",
                req.start_ms, req.end_ms
            ),
        });
        return (StatusCode::BAD_REQUEST, axum::Json(body)).into_response();
    }

    match registry.create_checked(
        &schemas,
        req.agg_id,
        (req.start_ms, req.end_ms),
        req.source,
        req.windows_total,
        state.data_retention_ms,
    ) {
        Ok(job_id) => {
            let body = serde_json::json!({
                "status": "success",
                "job_id": job_id,
            });
            (StatusCode::CREATED, axum::Json(body)).into_response()
        }
        Err(e @ CreateError::UnknownAgg { .. }) => {
            let body = serde_json::json!({
                "status": "error",
                "error": e.to_string(),
            });
            (StatusCode::NOT_FOUND, axum::Json(body)).into_response()
        }
        Err(e @ (CreateError::Overlap { .. } | CreateError::OutOfRetention { .. })) => {
            let body = serde_json::json!({
                "status": "error",
                "error": e.to_string(),
            });
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
    use crate::stores::sketch_db::BackfillStatus;
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
                ),
            });
            return (StatusCode::BAD_REQUEST, axum::Json(body)).into_response();
        }
    };
    let mut entries: Vec<serde_json::Value> = jobs.iter().map(backfill_job_to_json).collect();
    entries.sort_by_key(|v| v.get("job_id").and_then(|x| x.as_u64()).unwrap_or(0));

    let body = serde_json::json!({
        "status": "success",
        "count": entries.len(),
        "jobs": entries,
    });
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
                "job": backfill_job_to_json(&job),
            });
            (StatusCode::OK, axum::Json(body)).into_response()
        }
        None => {
            let body = serde_json::json!({
                "status": "error",
                "error": format!("job_id {job_id} not found"),
            });
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
            "error": format!("job_id {job_id} not found"),
        });
        return (StatusCode::NOT_FOUND, axum::Json(body)).into_response();
    };
    if job.status.is_terminal() {
        let body = serde_json::json!({
            "status": "error",
            "error": format!(
                "job {job_id} already {}, cannot cancel",
                backfill_status_str(&job.status)
            ),
        });
        return (StatusCode::CONFLICT, axum::Json(body)).into_response();
    }
    registry.cancel(job_id);
    let body = serde_json::json!({
        "status": "success",
        "job_id": job_id,
    });
    (StatusCode::OK, axum::Json(body)).into_response()
}
