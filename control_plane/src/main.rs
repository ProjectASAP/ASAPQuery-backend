use control_plane::accuracy;
use control_plane::backend_client;
use control_plane::emit;
use control_plane::intent_algebra;
use control_plane::metrics_exposer;
use control_plane::monitor;
use control_plane::opamp;
use control_plane::optimizer;
use control_plane::physical;
use control_plane::pipeline;
use control_plane::query_parser;
use control_plane::replan;
use control_plane::runtime_samples;
use control_plane::sketch_algebra;
use control_plane::store;
use control_plane::types;
use control_plane::types_v2;
use control_plane::workload;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde_json::json;
use tracing::{info, warn};

use optimizer::engine::QueryOptimizer;
use physical::allocator::SketchAllocator;
use pipeline::{Analyzer, QuerySpec};
use emit::{generate_agent_collector_config, build_precompute_engine_jobs};
use workload::WorkloadRegistry;
use emit::{AgentRuntime, emit_for_runtime};
use types::AgentCollectorConfig;
use monitor::{Endpoint, Scraper, ScrapedData, Thresholds, Violation};
use opamp::{AgentRole, OpampServer, RemoteConfig};
use optimizer::cost::CostModelPlanner;
use optimizer::baseline::BaselinePlanner;
use optimizer::cost::pareto::{ObjectiveWeights, pareto_frontier, select_best};
use optimizer::cost::online::{init_store as init_online_store, OnlineMetricsStore};
use optimizer::cost::online as online_cost_model;
use optimizer::cost::tco;
use query_parser::parse_query_expr_canonical;
use replan::Replanner;
use physical::colored_dag::emitter::BackendStageConfig;
use store::{PlanStore, WorkloadStore};
use types::StageResourceBudgets;
use workload::AggRole;

// ── Shared state ──────────────────────────────────────────────────────────────

#[derive(Clone)]
struct AppState {
    analyzer:          Arc<Analyzer>,
    planner:           Arc<BaselinePlanner>,
    store:             Arc<PlanStore>,
    workload_store:    Arc<WorkloadStore>,
    opamp:             Arc<OpampServer>,
    scraper:           Arc<Scraper>,
    replanner:         Arc<Replanner>,
    online_store:      OnlineMetricsStore,
    opamp_endpoint:    String,
    workload_registry: Arc<WorkloadRegistry>,
    /// Bounded ring buffer for runtime-sample push batches from
    /// agents' `sketch-runtime::PushExporter`. Read by decision
    /// loops in the replanner.
    runtime_samples:   Arc<runtime_samples::RuntimeSamplesStore>,
    /// Phase C (MVP v6): shared `BackendClient` for posting
    /// `StreamingConfig` JSON / YAML to the ASAPQuery-backend's
    /// `POST /api/v1/streaming-config` endpoint. Phase B had this
    /// only on the `Replanner`, so the typed L5 stage_split path in
    /// `handle_plan` could only `info!`-log the backend JSON it
    /// emitted. Sharing via `Arc` lets `AppState` and `Replanner`
    /// both push without owning a duplicate client. `None` when
    /// `CONTROLLER_BACKEND_ENDPOINT` is unset, matching the
    /// pre-existing fire-and-forget contract.
    backend_client:    Option<Arc<backend_client::BackendClient>>,
    /// Per-`(metric, role)` `BackendStageConfig` cache used to emit
    /// **cumulative** `StreamingConfig` AND `BackendStorageRouting`
    /// JSON documents on every plan-emit cycle.
    ///
    /// **Why this is (metric, role)-keyed** (B2 cumulative-emit follow-up
    /// to PR #283): a single metric can carry MULTIPLE [`AggRole`]
    /// entries (e.g. post-B2 the workload-registry pre-pop loop registers
    /// `http_requests_total` against BOTH a DDSketch-Quantile role from
    /// `quantile_over_time(...)` AND an ExactAgg-Sum role from
    /// `sum by (zone) (http_requests_total)`). Pre-fix the cache was
    /// keyed by metric name alone, so the second role's
    /// `BackendStageConfig` overwrote the first. The data plane's
    /// `POST /api/v1/streaming-config` handler is an atomic full
    /// `handle.swap(new_config)` (see
    /// `data_plane/src/drivers/query/servers/http.rs`), so the second
    /// per-role POST destroys the first role's aggregations on the
    /// backend → `sum by (zone) (http_requests_total)` returns
    /// `ExactAgg(Sum) capability not satisfied`.
    ///
    /// **Why this also matters for storage-routing**: `POST
    /// /api/v1/storage_routing` is similarly an atomic per-tenant SWAP
    /// — every push replaces the whole tenant's routing table. Pre-fix
    /// the per-metric cache emitted a single-element `metrics:[…]`
    /// document per `handle_plan` call, so when N metrics replanned in
    /// sequence only the last metric's entry survived → archive-shape
    /// queries fell to `default_engine: sketch_store` → `archive_miss`
    /// for the other N-1 metrics.
    ///
    /// **Cumulative emit semantics** (post-fix): on every plan-emit
    /// cycle the cache entry for the `(metric, role)` being planned is
    /// updated, then:
    ///   * Concatenate `aggregations` + `readouts` across ALL cache
    ///     entries into a single cumulative `BackendStageConfig`, and
    ///     post that one config to `/api/v1/streaming-config` so the
    ///     data plane's swap installs every role's aggregations
    ///     simultaneously.
    ///   * Group cache entries by metric name and merge each metric's
    ///     `BackendStageConfig`s (concat aggregations + readouts) into
    ///     one entry per metric. Pass that per-metric list to
    ///     `emit_backend_storage_routing` so a metric carrying both
    ///     DDSketch + ExactAgg routes both shape families correctly.
    backend_routing_cache: Arc<Mutex<HashMap<(String, AggRole), BackendStageConfig>>>,
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let api_addr   = std::env::var("CONTROLLER_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:8080".into());
    let opamp_addr = std::env::var("CONTROLLER_OPAMP_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:4320".into());
    // The default tracks the compose service name in
    // `ASAPCollector/deploy/mvp-singlenode/docker-compose/base.yml`,
    // which is still `controller:` post Phase-9 single-binary
    // refactor (both controller and backend ship from
    // `asap/query-backend:dev`, but they bind separate listeners
    // under separate compose service names). Using the post-reorg
    // crate name `control_plane` here doesn't resolve under the
    // canonical compose stack and bakes a broken endpoint into every
    // agent yaml the controller emits.
    let opamp_ep   = std::env::var("CONTROLLER_OPAMP_ENDPOINT")
        .unwrap_or_else(|_| "ws://controller:4320/v1/opamp".into());
    let scrape_interval = Duration::from_secs(
        std::env::var("CONTROLLER_SCRAPE_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(60u64),
    );
    let backend_endpoint = std::env::var("CONTROLLER_BACKEND_ENDPOINT").ok();

    // ── SP-5: Online EMA cost store ───────────────────────────────────────────
    let online_store = init_online_store();

    // ── SP-8: Prometheus scraper ──────────────────────────────────────────────
    // Violations are forwarded to the Replanner (built below).
    // We use an Arc<RwLock<Option<Arc<Replanner>>>> as a late-binding cell so
    // the scraper can hold a reference even though the Replanner is built after it.
    let replanner_cell: Arc<tokio::sync::RwLock<Option<Arc<Replanner>>>> =
        Arc::new(tokio::sync::RwLock::new(None));
    let registry_cell: Arc<tokio::sync::RwLock<Option<Arc<WorkloadRegistry>>>> =
        Arc::new(tokio::sync::RwLock::new(None));

    let scraper: Arc<Scraper> = {
        let ema  = Arc::clone(&online_store);
        let cell = Arc::clone(&replanner_cell);
        Arc::new(
            Scraper::new(
                vec![],
                Thresholds::default(),
                Arc::new(move |v: Violation| {
                    warn!(agent = %v.agent_id, kind = %v.kind,
                          observed = v.observed, threshold = v.threshold,
                          "SLA violation detected — triggering re-plan");
                    let cell = Arc::clone(&cell);
                    let agent_id = v.agent_id.clone();
                    tokio::spawn(async move {
                        if let Some(r) = cell.read().await.as_ref() {
                            r.handle_violation(&agent_id).await;
                        }
                    });
                }),
                scrape_interval,
            )
            .with_on_metrics(Arc::new(move |data: ScrapedData| {
                // Update EMA only when we know the sketch type and have a
                // CPU-per-sample estimate (requires at least 2 scrapes).
                if let (Some(st), Some(cpu)) = (data.sketch_type, data.cpu_micros_per_sample) {
                    let ema = Arc::clone(&ema);
                    tokio::spawn(async move {
                        online_cost_model::update(
                            &ema, &st, data.sketch_size_bytes, cpu,
                        ).await;
                    });
                }
            })),
        )
    };

    // ── OpAMP server with connect/disconnect hooks ────────────────────────────
    let opamp_srv: Arc<OpampServer> = {
        let sc = Arc::clone(&scraper);
        let sd = Arc::clone(&scraper);
        let connect_cell = Arc::clone(&replanner_cell);
        let connect_registry = Arc::clone(&registry_cell);
        Arc::new(
            OpampServer::new()
                .with_on_connect(move |agent_id, _role| {
                    // Convention: agent metrics endpoint at http://<agent_id>/metrics.
                    // Collectors should set their agent-id to "<host>:<port>" so this
                    // resolves correctly, or override CONTROLLER_METRICS_PATH.
                    let url     = format!("http://{agent_id}/metrics");
                    let sc      = Arc::clone(&sc);
                    let id_copy = agent_id.clone();
                    let cell    = Arc::clone(&connect_cell);
                    let reg     = Arc::clone(&connect_registry);
                    let aid     = agent_id.clone();
                    tokio::spawn(async move {
                        sc.add_endpoint(Endpoint::new(id_copy, url)).await;
                        if let Some(r) = cell.read().await.as_ref() {
                            // Push the current plan config if this agent has a prior assignment.
                            let pushed = r.push_config_to_agent(&aid).await;
                            // If the agent has no prior assignment, assign it a workload
                            // from the registry (if available).
                            //
                            // B2 (metric, role): bind the on-connect default to
                            // the first agent-role registry entry's CLASSIFIED
                            // role (via `derive_agg_role`) so the workload-store
                            // lookup in `push_config_to_agent` resolves.
                            if !pushed {
                                if let Some(registry) = reg.read().await.as_ref() {
                                    if let Some(entry) = registry.first_for_role("agent") {
                                        let role = control_plane::workload::derive_agg_role(entry);
                                        r.register_agent(&aid, &entry.metric_name, role).await;
                                        r.push_config_to_agent(&aid).await;
                                    }
                                }
                            }
                        }
                    });
                })
                .with_on_disconnect(move |agent_id| {
                    let sd = Arc::clone(&sd);
                    tokio::spawn(async move {
                        sd.remove_endpoint(&agent_id).await;
                    });
                }),
        )
    };

    // ── Sketch defaults (YAML-configurable) ────────────────────────────────
    let sketch_defaults_path = std::env::var("CONTROLLER_SKETCH_DEFAULTS")
        .unwrap_or_else(|_| "sketch_params_default.yml".into());
    let sketch_defaults = types::SketchDefaults::load(&sketch_defaults_path);
    info!(path = %sketch_defaults_path, "loaded sketch defaults");

    // ── BaselinePlanner backed by live EMA data ─────────────────────────────
    // Runs the full cost-model optimisation once per metric on the first
    // request, then locks in that plan as the baseline.  The Replanner resets
    // and re-optimises on SLA violation or plan expiry.
    let planner = Arc::new(BaselinePlanner::new(
        CostModelPlanner::new()
            .with_sketch_defaults(sketch_defaults)
            .with_online_store(Arc::clone(&online_store)),
    ));

    let plan_store     = Arc::new(PlanStore::new());
    let workload_store = Arc::new(WorkloadStore::new());

    // ── Declarative workload registry ────────────────────────────────────────
    let workloads_path = std::env::var("CONTROLLER_WORKLOADS")
        .unwrap_or_else(|_| "workloads.yaml".into());
    let workload_registry = Arc::new(WorkloadRegistry::load(&workloads_path));

    // Pre-populate PlanStore from the registry so agents get a config immediately.
    //
    // Critical: thread `sketch_family_override` from each registry entry
    // into the QuerySpec's `sketch_type` field — that's what populates
    // `QueryWorkload::sketch_type_override`, which the typed planner
    // (`bind_workload_typed`) reads to honour MVP §46 entries 5–8 (HLL /
    // CountSketch / CountMinSketch). Without this stitch the workloads
    // round-trip through the analyzer with a None override and the
    // capability-matched default fires, but for the metrics whose
    // statistic class doesn't match an `AggIntent` synthesizer (TopK in
    // particular for `top_endpoint_qps`) the metric-name fallback in
    // `classify_demo_metric` becomes the only path — and it works fine
    // when the override is also threaded as a belt-and-braces guarantee.
    {
        let analyzer = Analyzer::new();
        for entry in workload_registry.entries() {
            // MVP blocker B4 — let the analyzer parse `time_window` from
            // `query_string` (matrix-selector `[range]`) instead of
            // forcing a hardcoded "5m" default that overrides whatever
            // the user wrote. The analyzer falls back to its own 5m
            // default when the PromQL has no matrix selector (e.g.
            // `count(unique_users_per_min)`), so this is strictly an
            // improvement for queries that DO carry an explicit range.
            // Empty string here means "no override; trust the parsed
            // value or the analyzer's fallback".
            //
            // MVP blocker B3 — thread the WorkloadEntry's declarative
            // `grouping_labels` into `QuerySpec.group_by_labels`. The
            // analyzer merges these with any `by (...)` keys the
            // PromQL parser surfaces, populating `QueryWorkload.
            // group_by_labels`, which `collect_metric_to_grouping_labels`
            // then drops into `EdgeStageConfig.metric_to_grouping_labels`
            // so the agent's `keep_keys(datapoint.attributes, [...])`
            // OTTL processor strips wire attrs down to this list
            // BEFORE sketching.
            let spec = pipeline::QuerySpec {
                query_string:    entry.query_string.clone(),
                metric_name:     entry.metric_name.clone(),
                label_filters:   Default::default(),
                group_by_labels: entry.grouping_labels.clone(),
                aggregations:    vec!["quantile".into()],
                // Empty when the entry HAS a `query_string` (the parser
                // surfaces the matrix-selector range or its own 5m
                // fallback). For entries without a query_string we
                // can't trust the parser, so fall back to the
                // historical 5m default so the analyzer doesn't error
                // out at Step 4.
                time_window:     if entry.query_string.is_some() {
                    String::new()
                } else {
                    "5m".into()
                },
                repeat_every:    None,
                accuracy_sla:    entry.accuracy_sla,
                latency_sla:     None,
                sketch_type:     entry.sketch_family_override.clone(),
                workload:        types::WorkloadCharacteristics::default(),
                // design.md alignment: defaults preserve legacy behaviour.
                id:               None,
                language:         None,
                accuracy:         None,
                dollars:          None,
                deployment_model: None,
                shape:            types_v2::QueryShape::default(),
                data:             types_v2::DataShape::default(),
            };
            // B2 full restructure — derive the AggRole for this entry
            // BEFORE store insertion so collisions on metric name don't
            // overwrite a prior role's entry. The pre-B2 loop wrote
            // `set(metric, ...)` and silently dropped every entry but
            // the LAST one when a metric appeared multiple times in the
            // YAML — that's the bug that caused `sum by (zone)
            // (http_requests_total)` to return `ExactAgg(Sum)
            // capability not satisfied` (entries 2 + 3 of
            // mvp-workload.yaml were both Sum-shaped but only the
            // count(...) entry 4 survived, with DDSketch from the
            // unrelated `http_requests_total_latency_ms` quantile
            // entry).
            let role = control_plane::workload::derive_agg_role(entry);
            match analyzer.analyze(spec) {
                Ok(wl) => {
                    let wc = types::WorkloadCharacteristics::default();
                    let plan = planner.plan(&wl, Some(&wc));
                    let metric_name = wl.metric_name.clone();
                    plan_store.set(&metric_name, role, plan);
                    workload_store.set(&metric_name, role, wl, wc);
                }
                Err(e) => {
                    warn!(metric = %entry.metric_name, role = %role, error = %e,
                        "failed to pre-populate plan from workload registry");
                }
            }
        }
    }

    // ── Phase C: shared BackendClient ─────────────────────────────────────────
    // Built once at startup; shared between Replanner (existing path —
    // pushes the StreamingConfig YAML on every successful replan) and
    // AppState (Phase C — pushes the typed L5 backend JSON emitted by
    // `emit_backend_streaming_config_json` from `handle_plan`). `None` when
    // `CONTROLLER_BACKEND_ENDPOINT` is unset preserves the
    // fire-and-forget "skip silently" contract from Phase B.
    let backend_client_shared: Option<Arc<backend_client::BackendClient>> =
        backend_endpoint.as_ref().map(|endpoint| {
            info!(
                endpoint = %endpoint,
                "ASAPQuery-backend StreamingConfig push enabled"
            );
            Arc::new(backend_client::BackendClient::new(endpoint.clone()))
        });
    if backend_client_shared.is_none() {
        info!(
            "ASAPQuery-backend StreamingConfig push disabled \
             (set CONTROLLER_BACKEND_ENDPOINT=<url> to enable)"
        );
    }

    // ── Replanner — closes the SP-8 feedback loop ─────────────────────────────
    let replanner = {
        let mut r = Replanner::new(
            Arc::clone(&planner),
            Arc::clone(&plan_store),
            Arc::clone(&workload_store),
            Arc::clone(&opamp_srv),
            Arc::clone(&scraper),
            opamp_ep.clone(),
        );
        if let Some(client) = backend_client_shared.as_ref() {
            r = r.with_backend_client(Arc::clone(client));
        }
        // Wire the workload registry so the typed-emit path
        // (`USE_TYPED_STAGE_SPLIT`) can extend its edge stage config
        // with the same archive-tier metrics the bootstrap GET path
        // applies via `emit_bootstrap_typed`.
        r = r.with_workload_registry(Arc::clone(&workload_registry));
        Arc::new(r)
    };
    // Bind the late-binding cells so callbacks can reach the replanner and registry.
    *replanner_cell.write().await = Some(Arc::clone(&replanner));
    *registry_cell.write().await = Some(Arc::clone(&workload_registry));

    let replan_interval = Duration::from_secs(
        std::env::var("CONTROLLER_REPLAN_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(300u64), // re-check plan expiry every 5 minutes
    );

    let runtime_samples_store = runtime_samples::RuntimeSamplesStore::new(1024);
    let backend_routing_cache: Arc<Mutex<HashMap<(String, AggRole), BackendStageConfig>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let state = AppState {
        analyzer:          Arc::new(Analyzer::new()),
        planner,
        store:             Arc::clone(&plan_store),
        workload_store:    Arc::clone(&workload_store),
        opamp:             Arc::clone(&opamp_srv),
        scraper:           Arc::clone(&scraper),
        replanner:         Arc::clone(&replanner),
        online_store:      Arc::clone(&online_store),
        opamp_endpoint:    opamp_ep,
        workload_registry: Arc::clone(&workload_registry),
        runtime_samples:   Arc::clone(&runtime_samples_store),
        backend_client:    backend_client_shared,
        backend_routing_cache: Arc::clone(&backend_routing_cache),
    };

    // ── Background tasks ──────────────────────────────────────────────────────
    tokio::spawn(Arc::clone(&scraper).run());
    tokio::spawn(Arc::clone(&replanner).run_expiry_ticker(replan_interval));

    // ── OpAMP WebSocket listener ──────────────────────────────────────────────
    let opamp_router = Router::new()
        .route("/v1/opamp", get(OpampServer::ws_handler))
        .with_state(Arc::clone(&opamp_srv));

    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(&opamp_addr).await.unwrap();
        info!("OpAMP server listening on {opamp_addr}");
        axum::serve(listener, opamp_router).await.unwrap();
    });

    // ── gRPC runtime-samples service (was HTTP+JSONL) ─────────────────────────
    // Agents' `sketch-runtime::GrpcExporter` call
    // `asap.runtime.v1.RuntimeSamples.Push` on this port. See
    // commit message for the HTTP → gRPC pivot rationale.
    let runtime_samples_state = Arc::clone(&state.runtime_samples);
    let grpc_addr = std::env::var("CONTROLLER_GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:4321".into());
    let grpc_store = Arc::clone(&runtime_samples_state);
    tokio::spawn(async move {
        let addr: std::net::SocketAddr = grpc_addr.parse().expect("CONTROLLER_GRPC_ADDR");
        info!("runtime-samples gRPC server listening on {addr}");
        let svc = runtime_samples::RuntimeSamplesService::new(grpc_store).into_server();
        if let Err(e) = tonic::transport::Server::builder()
            .add_service(svc)
            .serve(addr)
            .await
        {
            tracing::error!(error = %e, "runtime-samples gRPC server exited");
        }
    });

    // /metrics — same exposer as before, unchanged. Prom scrapes
    // the control plane HTTP port; gRPC is the push side only.
    let metrics_registry = metrics_exposer::MetricsRegistry::new();
    let metrics_state = metrics_exposer::MetricsState {
        registry: Arc::clone(&metrics_registry),
        store: Arc::clone(&runtime_samples_state),
        stats: runtime_samples_state.stats_handle(),
        plan_store: Some(Arc::clone(&plan_store)),
    };
    let metrics_router = Router::new()
        .route(
            "/metrics",
            axum::routing::get(metrics_exposer::handle_metrics),
        )
        .with_state(metrics_state);

    let app = Router::new()
        .route("/api/v1/plan",                    post(handle_plan))
        .route("/api/v1/plan/pareto",             post(handle_pareto))
        .route("/api/v1/plan/:metric",            get(handle_get_plan))
        .route("/api/v1/plan/:metric/rollback",   post(handle_rollback))
        .route("/api/v1/plan/:metric/diff",       get(handle_plan_diff))
        .route("/api/v1/agents",                  get(handle_agents))
        .route("/api/v1/config/:metric",          get(handle_get_config))
        .route("/api/v1/collector-config/agent",  get(handle_bootstrap_agent_config))
        .route("/api/v1/cost-model",              get(handle_cost_model))
        .route("/api/v1/tco",                     post(handle_tco))
        .with_state(state)
        .merge(metrics_router);

    let listener = tokio::net::TcpListener::bind(&api_addr).await.unwrap();
    info!("control plane API listening on {api_addr}");
    axum::serve(listener, app).await.unwrap();
}

// ── Handlers ──────────────────────────────────────────────────────────────────

async fn handle_plan(
    State(st): State<AppState>,
    Json(spec): Json<QuerySpec>,
) -> impl IntoResponse {
    let wc           = spec.workload.clone();
    let query_string = spec.query_string.clone();
    let workload = match st.analyzer.analyze(spec) {
        Ok(w)  => w,
        Err(e) => return (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()).into_response(),
    };

    let mut plan = st.planner.plan(&workload, Some(&wc));

    // ── L1→L5: parse → optimise → bind → stage. One algebra pipeline. ───────
    // When the spec carries a `query_string`, this is the single place the
    // algebra runs: parse to canonical L3, optimise, bind to the L4
    // `PhysicalExpr`, and derive the L5 artifacts from that one tree —
    //   * `bound_physical` → the typed L5 stage-split (below) — the L5;
    //   * `plan_summary`   → the cost summary in the JSON response.
    // The SP-3 flat assignment in `plan` remains the fallback when there
    // is no `query_string`.
    let raw_bps = plan.transmission_cost_summary.raw_bytes_per_sec;
    let budgets = StageResourceBudgets::from_workload_chars(&wc);
    let mut bound_physical: Option<control_plane::sketch_algebra::PhysicalExpr> = None;
    let mut plan_summary = None;
    if let Some(ref qs) = query_string {
        match parse_query_expr_canonical(qs) {
            Err(e) => warn!(query = %qs, error = %e, "parse_query_expr_canonical failed; skipping algebra pipeline"),
            Ok(qe) => {
                let constraints = optimizer::engine::DeploymentConstraints::from_budgets(&budgets);
                let (opt_qe, _) = QueryOptimizer::with_constraints(raw_bps, constraints).optimize(qe);
                // L4 sketch binding: lower the optimised L3 tree to the
                // sketch-bound `PhysicalExpr` IR — the typed L5's input.
                let accuracy = if workload.accuracy_sla >= 1.0 {
                    control_plane::types_v2::AccuracyTarget::Exact
                } else {
                    control_plane::types_v2::AccuracyTarget::Epsilon(1.0 - workload.accuracy_sla)
                };
                bound_physical =
                    control_plane::sketch_algebra::bind_query_expr(&opt_qe, accuracy).ok();
                // Cost summary for the JSON response.
                let plan_node = SketchAllocator::new(budgets.clone(), raw_bps).allocate(opt_qe);
                plan_summary = Some(plan_node.summarise(raw_bps));
            }
        }
    }

    plan.precompute = build_precompute_engine_jobs(&workload, "backend:4317");
    // B2 (metric, role): derive the role from the request's
    // query_string + optional `sketch_type` override so the
    // store keys at (metric, role) granularity. Without the role
    // a second POST for the same metric with a different shape
    // (Quantile vs Sum) would silently overwrite the prior plan.
    let role = {
        let entry = control_plane::workload::WorkloadEntry {
            metric_name: workload.metric_name.clone(),
            query_string: query_string.clone(),
            accuracy_sla: workload.accuracy_sla,
            assign_to_role: String::from("agent"),
            sketch_family_override: workload.sketch_type_override.clone(),
            target_path: None,
            grouping_labels: workload.group_by_labels.clone(),
        };
        control_plane::workload::derive_agg_role(&entry)
    };
    st.store.set(&workload.metric_name, role, plan.clone());
    // Persist workload so the replanner can re-run plan() without the original spec.
    st.workload_store.set(&workload.metric_name, role, workload.clone(), wc);

    // ── Push agent config to agent-role collectors ────────────────────────────
    if let Ok(agent_yaml) = generate_agent_collector_config(&plan.agent_config, &st.opamp_endpoint) {
        let hash = short_hash(&agent_yaml);
        st.opamp.push_to_role(
            AgentRole::Agent,
            RemoteConfig { config_hash: hash, yaml: agent_yaml },
        ).await;
    }

    // ── Phase B (MVP v6): typed L5 stage_split → per-stage emitter ────────────
    // Behind the `USE_TYPED_STAGE_SPLIT` env-var gate so existing
    // control plane behaviour is unchanged unless explicitly opted in.
    //
    // The typed L5 stage-split is now fed by the real **L4 output**: when
    // the spec carries a `query_string`, `bound_physical` holds the
    // optimised L3 tree run through `sketch_algebra::bind_query_expr`.
    // For specs that supply only explicit fields (no `query_string` to
    // parse), there is no L3 tree to bind, so we fall back to
    // `bind_workload_typed`, which lowers the flat `QueryWorkload`
    // summary to a `PhysicalExpr` directly.
    if physical::stage_split::typed_stage_split_enabled() {
        let physical_expr =
            bound_physical.or_else(|| optimizer::rules::bind_workload_typed(&workload));
        if let Some(physical_expr) = physical_expr {
            if let Some(configs) = physical::stage_split::split_typed_three_stage(&physical_expr) {
                for (stage_id, stage_cfg) in configs {
                    match stage_cfg {
                        crate::physical::colored_dag::StageConfig::Edge(mut edge) => {
                            // MVP blocker B3 — patch per-metric grouping
                            // labels onto the edge cfg so the 5-sketch
                            // routing emitter prepends a
                            // `transform/keep_for_*` OTTL processor in
                            // front of each sketch pipeline. The typed
                            // L5 emitter leaves
                            // `metric_to_grouping_labels` empty by
                            // design (same rationale as the
                            // `agg.grouping = workload.group_by_labels`
                            // patch on the Backend stage below).
                            edge.metric_to_grouping_labels.insert(
                                workload.metric_name.clone(),
                                workload.group_by_labels.clone(),
                            );
                            // Issue #2: broadcast push — no single agent id
                            // in scope, so emit `$AGENT_ID` placeholder and
                            // rely on the agent container's env to expand it
                            // at boot. Per-agent re-pushes (push_config_to_agent
                            // / replan_metric inner loop) get the real id.
                            match emit::emit_edge_yaml(&edge, &st.opamp_endpoint, "$AGENT_ID") {
                                Ok(yaml) => {
                                    let hash = short_hash(&yaml);
                                    info!(
                                        stage = "edge", bytes = yaml.len(),
                                        "[USE_TYPED_STAGE_SPLIT] pushing typed edge YAML"
                                    );
                                    st.opamp.push_to_role(
                                        AgentRole::Agent,
                                        RemoteConfig { config_hash: hash, yaml },
                                    ).await;
                                }
                                Err(e) => warn!(error = %e, "emit_edge_yaml failed"),
                            }
                        }
                        crate::physical::colored_dag::StageConfig::Gateway(gw) => {
                            // Phase C: AgentRole::Gateway is now wired
                            // through the OpAMP role-routing path, so
                            // the gateway YAML is pushed to gateway-role
                            // collectors the same way the edge YAML is
                            // pushed to agent-role collectors above.
                            // Issue #2: gateway broadcast — `$AGENT_ID` placeholder.
                            match emit::emit_gateway_yaml(&gw, &st.opamp_endpoint, "$AGENT_ID") {
                                Ok(yaml) => {
                                    let hash = short_hash(&yaml);
                                    info!(
                                        stage = "gateway", bytes = yaml.len(),
                                        "[USE_TYPED_STAGE_SPLIT] pushing typed gateway YAML"
                                    );
                                    st.opamp.push_to_role(
                                        AgentRole::Gateway,
                                        RemoteConfig { config_hash: hash, yaml },
                                    ).await;
                                }
                                Err(e) => warn!(error = %e, "emit_gateway_yaml failed"),
                            }
                        }
                        crate::physical::colored_dag::StageConfig::Backend(mut be) => {
                            // Patch metric_name + grouping from the
                            // workload spec. The typed L5 emitter:
                            //   * sets `metric_name` from
                            //     `edge.source_metric`, which is
                            //     populated by `extract_edge_facts`
                            //     walking the `Logical(Scan{...})`
                            //     chain. The path-recovery isn't
                            //     guaranteed across every binder
                            //     output shape, so we belt-and-brace
                            //     it with `workload.metric_name`.
                            //   * leaves `grouping` empty because the
                            //     canonical L3 `QueryExpr::Aggregate.by`
                            //     is positional `ColumnId`s against a
                            //     synthesized schema with no label
                            //     columns (open-set label naming is
                            //     a Step γ TODO in
                            //     `intent_algebra::column_resolution`).
                            // `QueryWorkload` carries both unambiguously,
                            // and every aggregation under one workload
                            // shares them — so the patch is uniform.
                            for agg in &mut be.aggregations {
                                if agg.metric_name.is_empty() {
                                    agg.metric_name = workload.metric_name.clone();
                                }
                                if agg.window_secs == 0 {
                                    agg.window_secs = workload.time_window.as_secs();
                                }
                                agg.grouping = workload.group_by_labels.clone();
                            }
                            // B2 cumulative-emit follow-up: update the
                            // per-(metric, role) cache with THIS
                            // iteration's `be`, then BOTH the
                            // streaming-config emit and the
                            // storage-routing emit below derive their
                            // payload from the FULL cache. The
                            // single-iteration `be` is never sent on
                            // the wire on its own — every post is
                            // cumulative across all `(metric, role)`
                            // pairs the control plane has planned.
                            //
                            // Why: the data plane's
                            // `POST /api/v1/streaming-config` handler
                            // is `handle.swap(new_config)` (an atomic
                            // full replace) and the storage-routing
                            // handler is similarly an atomic per-tenant
                            // swap. Per-iteration posts overwrite
                            // siblings:
                            //   * streaming-config — drops the prior
                            //     role's `aggregations`, so a metric
                            //     with both DDSketch (Quantile) and
                            //     ExactAgg (Sum) loses one on the
                            //     backend → `sum by (zone)
                            //     (http_requests_total)` returns
                            //     `ExactAgg(Sum) capability not satisfied`
                            //     (the regression that motivates this
                            //     PR — direct follow-up to #283 which
                            //     made the workload/plan stores
                            //     (metric, role)-keyed but left the
                            //     emit path metric-only).
                            //   * storage-routing — drops other
                            //     metrics' entries → default
                            //     `sketch_store` engine → `archive_miss`.
                            let cumulative_entries: Vec<((String, AggRole), BackendStageConfig)> = {
                                let mut cache = st.backend_routing_cache.lock().await;
                                cache.insert((workload.metric_name.clone(), role), be.clone());
                                let mut v: Vec<((String, AggRole), BackendStageConfig)> = cache
                                    .iter()
                                    .map(|(k, v)| (k.clone(), v.clone()))
                                    .collect();
                                // Deterministic ordering so the emitted
                                // JSON body is reproducible across runs
                                // and across test invocations. HashMap
                                // iteration order would otherwise make
                                // captured-body regression assertions
                                // flaky.
                                v.sort_by(|(a_k, _), (b_k, _)| {
                                    a_k.0
                                        .cmp(&b_k.0)
                                        .then_with(|| a_k.1.as_str().cmp(b_k.1.as_str()))
                                });
                                v
                            };

                            // Cumulative streaming-config — one
                            // `BackendStageConfig` whose `aggregations`
                            // + `readouts` are the concatenation of
                            // every cache entry's. The data plane's
                            // swap installs this single
                            // multi-aggregation config atomically, so
                            // ALL roles for ALL metrics survive.
                            let cumulative_be = BackendStageConfig {
                                aggregations: cumulative_entries
                                    .iter()
                                    .flat_map(|(_, c)| c.aggregations.iter().cloned())
                                    .collect(),
                                readouts: cumulative_entries
                                    .iter()
                                    .flat_map(|(_, c)| c.readouts.iter().cloned())
                                    .collect(),
                            };

                            // Phase C: post the cumulative typed L5
                            // streaming-config JSON to ASAPQuery-backend
                            // via the shared BackendClient when
                            // configured. Without a configured endpoint
                            // this still no-ops silently — same
                            // fire-and-forget contract as the existing
                            // Replanner path.
                            match emit::emit_backend_streaming_config_json(&cumulative_be) {
                                Ok(json_doc) => {
                                    info!(
                                        stage = "backend",
                                        aggregations = cumulative_be.aggregations.len(),
                                        readouts = cumulative_be.readouts.len(),
                                        cumulative_pairs = cumulative_entries.len(),
                                        "[USE_TYPED_STAGE_SPLIT] posting typed backend JSON"
                                    );
                                    if let Some(client) = st.backend_client.as_ref() {
                                        let body = json_doc.to_string();
                                        match client.post_streaming_config_json(body).await {
                                            Ok(()) => info!(
                                                stage = "backend",
                                                endpoint = %client.endpoint(),
                                                "[USE_TYPED_STAGE_SPLIT] typed backend JSON push succeeded"
                                            ),
                                            Err(e) => warn!(
                                                stage = "backend",
                                                endpoint = %client.endpoint(),
                                                error = %e,
                                                "[USE_TYPED_STAGE_SPLIT] typed backend JSON push failed; \
                                                 next replan cycle will retry"
                                            ),
                                        }
                                    } else {
                                        info!(
                                            stage = "backend",
                                            "[USE_TYPED_STAGE_SPLIT] no backend client configured; \
                                             skipping JSON push (set CONTROLLER_BACKEND_ENDPOINT to enable)"
                                        );
                                    }
                                }
                                Err(e) => warn!(error = %e, "emit_backend_streaming_config_json failed"),
                            }

                            // Phase α (MVP) cumulative storage-routing.
                            // The routing classifier
                            // (`build_routing_entry` in
                            // `emit/stage_config.rs`) reads
                            // `cfg.aggregations` to derive shape
                            // routing, so we MUST merge every role's
                            // aggregations for one metric into a single
                            // `BackendStageConfig` before passing it
                            // through — otherwise a metric with both
                            // DDSketch (Quantile) and ExactAgg (Sum)
                            // would emit only the last-cached role's
                            // shape classifications and route the
                            // siblings to archive.
                            //
                            // `emit_backend_storage_routing`'s signature
                            // is `&[(String, &BackendStageConfig)]` —
                            // per-metric, NOT per-(metric, role) — so
                            // the merge happens at the call site (per
                            // the PR's no-signature-change constraint).
                            let mut by_metric: std::collections::BTreeMap<String, BackendStageConfig> =
                                std::collections::BTreeMap::new();
                            for ((m, _r), cfg) in &cumulative_entries {
                                let entry = by_metric.entry(m.clone()).or_insert_with(|| {
                                    BackendStageConfig {
                                        aggregations: Vec::new(),
                                        readouts: Vec::new(),
                                    }
                                });
                                entry.aggregations.extend(cfg.aggregations.iter().cloned());
                                entry.readouts.extend(cfg.readouts.iter().cloned());
                            }
                            let routing_owned: Vec<(String, BackendStageConfig)> =
                                by_metric.into_iter().collect();
                            let routing_input: Vec<(String, &BackendStageConfig)> = routing_owned
                                .iter()
                                .map(|(k, v)| (k.clone(), v))
                                .collect();
                            match emit::emit_backend_storage_routing(&routing_input) {
                                Ok(routing_doc) => {
                                    info!(
                                        stage = "backend",
                                        metric = %workload.metric_name,
                                        cumulative_metrics = routing_owned.len(),
                                        cumulative_pairs = cumulative_entries.len(),
                                        "[USE_TYPED_STAGE_SPLIT] posting cumulative storage-routing JSON"
                                    );
                                    if let Some(client) = st.backend_client.as_ref() {
                                        let body = routing_doc.to_string();
                                        match client.post_storage_routing_json(body).await {
                                            Ok(()) => info!(
                                                stage = "backend",
                                                metric = %workload.metric_name,
                                                "[USE_TYPED_STAGE_SPLIT] storage-routing JSON push succeeded"
                                            ),
                                            Err(e) => warn!(
                                                stage = "backend",
                                                metric = %workload.metric_name,
                                                error = %e,
                                                "[USE_TYPED_STAGE_SPLIT] storage-routing JSON push failed; \
                                                 next replan cycle will retry"
                                            ),
                                        }
                                    } else {
                                        info!(
                                            stage = "backend",
                                            "[USE_TYPED_STAGE_SPLIT] no backend client configured; \
                                             skipping storage-routing JSON push"
                                        );
                                    }
                                }
                                Err(e) => warn!(error = %e, "emit_backend_storage_routing failed"),
                            }

                            // Mention stage_id so `match` arms aren't
                            // collapsed into untagged log lines if the
                            // tracing filter drops the per-arm event.
                            let _ = stage_id;
                        }
                    }
                }
            } else {
                warn!(
                    metric = %workload.metric_name,
                    "[USE_TYPED_STAGE_SPLIT] split_typed_three_stage returned None; \
                     legacy plan output unaffected"
                );
            }
        }
    }

    // ── Update scrape-endpoint sketch types and agent→(metric, role) mapping ──
    let sketch_type = plan.agent_config.sketch_type.clone();
    for agent_id in st.opamp.connected_agents().await {
        st.scraper.set_sketch_type(&agent_id, sketch_type.clone()).await;
        st.replanner.register_agent(&agent_id, &workload.metric_name, role).await;
    }

    // `plan_summary` was computed in the single algebra pipeline above.

    let agents = st.opamp.connected_agents().await;
    let cost = &plan.transmission_cost_summary;
    (StatusCode::OK, Json(json!({
        "metric":              workload.metric_name,
        "sketch_type":         plan.agent_config.sketch_type.to_string(),
        "mode":                plan.agent_config.mode.to_string(),
        "aggregate_by":        plan.agent_config.aggregate_by,
        "valid_until":         plan.valid_until,
        "agents_notified":     agents.len(),
        "precompute_jobs":     plan.precompute.len(),
        "delta_decision":      plan.delta_decision,
        "transmission_costs": {
            "raw_bytes_per_sec":                   cost.raw_bytes_per_sec,
            "sketch_full_bytes_per_sec":            cost.sketch_full_bytes_per_sec,
            "sketch_delta_bytes_per_sec":           cost.sketch_delta_bytes_per_sec,
            "delta_cpu_overhead_micros_per_sample": cost.delta_cpu_overhead_micros_per_sample,
            "delta_memory_overhead_bytes":          cost.delta_memory_overhead_bytes,
            "estimated_fill_rate":                  cost.estimated_fill_rate,
            "flush_rate_hz":                        cost.flush_rate_hz,
        },
        "plan_summary": plan_summary,
    }))).into_response()
}

/// Request body for `POST /api/v1/plan/pareto`.
#[derive(serde::Deserialize)]
struct ParetoRequest {
    #[serde(flatten)]
    spec:    QuerySpec,
    #[serde(default)]
    weights: ObjectiveWeights,
}

/// Returns the Pareto frontier of collection plans for the given workload.
/// Each point is annotated with bandwidth, CPU, memory and accuracy objectives.
/// The caller can specify `weights` to get the frontier sorted by their
/// preferred trade-off.
async fn handle_pareto(
    State(st): State<AppState>,
    Json(req): Json<ParetoRequest>,
) -> impl IntoResponse {
    let wc = req.spec.workload.clone();
    let workload = match st.analyzer.analyze(req.spec) {
        Ok(w)  => w,
        Err(e) => return (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()).into_response(),
    };

    let frontier = pareto_frontier(&workload, &wc, req.weights, Some(&st.online_store));

    if frontier.is_empty() {
        return (StatusCode::UNPROCESSABLE_ENTITY,
            "no sketch meets the accuracy SLA for the given workload").into_response();
    }

    let best = select_best(&frontier, req.weights)
        .map(|p| p.sketch_type.to_string());

    let points: Vec<serde_json::Value> = frontier.iter().map(|p| json!({
        "sketch_type":             p.sketch_type.to_string(),
        "bandwidth_bytes_per_sec": p.bandwidth_bytes_per_sec,
        "cpu_micros_per_sample":   p.cpu_micros_per_sample,
        "memory_bytes":            p.memory_bytes,
        "estimated_error":         p.estimated_error,
    })).collect();

    (StatusCode::OK, Json(json!({
        "metric":   workload.metric_name,
        "frontier": points,
        "best":     best,
    }))).into_response()
}

async fn handle_get_plan(
    State(st): State<AppState>,
    Path(metric): Path<String>,
) -> impl IntoResponse {
    // B2 (metric, role): return every role's plan for this metric.
    // Wire shape (additive, no breaking change): when only one role is
    // registered, the response still carries the pre-B2 top-level
    // `sketch_type` / `valid_until` fields for backward compat. The
    // new `roles` array is always present so clients can opt in to
    // the multi-role view.
    let plans = st.store.get_all_for_metric(&metric);
    if plans.is_empty() {
        return (StatusCode::NOT_FOUND, format!("plan not found for metric {metric:?}"))
            .into_response();
    }
    let roles: Vec<serde_json::Value> = plans
        .iter()
        .map(|(role, plan)| {
            json!({
                "role": role.as_str(),
                "sketch_type": plan.agent_config.sketch_type.to_string(),
                "valid_until": plan.valid_until,
            })
        })
        .collect();
    let first = &plans[0].1;
    (
        StatusCode::OK,
        Json(json!({
            "metric":      metric,
            "sketch_type": first.agent_config.sketch_type.to_string(),
            "valid_until": first.valid_until,
            "roles":       roles,
        })),
    )
        .into_response()
}

async fn handle_rollback(
    State(st): State<AppState>,
    Path(metric): Path<String>,
) -> impl IntoResponse {
    // Reset the baseline so the next POST /api/v1/plan re-runs the cost
    // model and establishes a fresh baseline plan for this metric.
    //
    // B2 (metric, role): rollback ALL roles for this metric. The
    // response surfaces the per-role outcome so clients can see which
    // roles had a previous plan and which were no-ops. Pre-B2 callers
    // who fired a rollback on a metric got back `{rolled_back: true}`
    // unconditionally for a single role; the new shape stays additive
    // (carries `rolled_back: true` when ≥1 role rolled back).
    st.planner.reset(&metric);
    let plans = st.store.get_all_for_metric(&metric);
    if plans.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            format!("plan not found for metric {metric:?}"),
        )
            .into_response();
    }
    let mut per_role = Vec::with_capacity(plans.len());
    let mut any_rolled_back = false;
    for (role, _) in plans {
        match st.store.rollback(&metric, role) {
            Ok(plan) => {
                if let Ok(yaml) =
                    generate_agent_collector_config(&plan.agent_config, &st.opamp_endpoint)
                {
                    st.opamp
                        .push_to_role(
                            AgentRole::Agent,
                            RemoteConfig {
                                config_hash: short_hash(&yaml),
                                yaml,
                            },
                        )
                        .await;
                }
                per_role.push(json!({ "role": role.as_str(), "rolled_back": true }));
                any_rolled_back = true;
            }
            Err(e) => {
                per_role.push(json!({
                    "role": role.as_str(),
                    "rolled_back": false,
                    "reason": e.to_string(),
                }));
            }
        }
    }
    // Pre-B2 contract: return BAD_REQUEST when no role could roll
    // back (e.g. every role's plan has no `previous` slot). The
    // multi-role variants are surfaced in the `roles` array so
    // callers can distinguish "rolled back N of K" cases.
    let status = if any_rolled_back {
        StatusCode::OK
    } else {
        StatusCode::BAD_REQUEST
    };
    (
        status,
        Json(json!({
            "metric":       metric,
            "rolled_back":  any_rolled_back,
            "roles":        per_role,
        })),
    )
        .into_response()
}

async fn handle_agents(State(st): State<AppState>) -> impl IntoResponse {
    Json(st.opamp.connected_agents_with_roles().await)
}

/// Returns a complete OTel collector YAML for the named metric's current plan.
/// Collectors can use this with the HTTP config provider:
///   --config=http://control_plane:8080/api/v1/config/<metric>
async fn handle_get_config(
    State(st): State<AppState>,
    Path(metric): Path<String>,
) -> impl IntoResponse {
    // B2 (metric, role): the legacy `generate_agent_collector_config`
    // emits a SINGLE-pipeline YAML — when a metric has multiple roles,
    // pick the FIRST registered role's plan. The 5-sketch routing-
    // connector emit path (the `USE_TYPED_STAGE_SPLIT` typed pipeline)
    // is the supported multi-role wire shape; this endpoint stays
    // legacy-compat by picking one role's plan.
    let plan = st.store.get_all_for_metric(&metric).into_iter().next();
    let Some((_role, plan)) = plan else {
        return (
            StatusCode::NOT_FOUND,
            format!("plan not found for metric {metric:?}"),
        )
            .into_response();
    };
    match generate_agent_collector_config(&plan.agent_config, &st.opamp_endpoint) {
        Ok(yaml) => (
            StatusCode::OK,
            [("content-type", "application/yaml")],
            yaml,
        )
            .into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Bootstrap YAML config for agent collectors.
///
/// Collectors start with:
///   `./collector --config "http://control_plane:8080/api/v1/collector-config/agent"`
///
/// ## Behaviour matrix
///
/// | `USE_TYPED_STAGE_SPLIT` | path |
/// | --- | --- |
/// | unset / `0` | **legacy** — emit a default-DDSketch [`AgentCollectorConfig`] via [`generate_agent_collector_config`]. Backwards-compat with deployments that haven't migrated to the typed L5 emitters. |
/// | `1` / `true` / `yes` | **typed** — pick the agent's pinned workload (when `X-Agent-ID` is supplied and the replanner has a prior assignment), or fall back to the first agent-role entry in [`WorkloadRegistry`]. Run the typed L5 pipeline (`bind_workload_typed` → `split_typed_three_stage`) and emit the Edge stage config via [`emit_for_runtime`] — dispatched by the `X-Agent-Runtime` header (defaults to `AsapOtel`). When the typed path errors out (no workload, unsupported topology, no Edge stage in the per-stage map) it falls back to the legacy emitter so the bootstrap never returns a 500 just because the typed path has a gap. |
///
/// ## Why this matters
///
/// Without the typed path, fresh agents connecting at startup miss
/// Phase 3.2.5's `gorillas3` archive emit + warm-passthrough routing
/// processor, the per-runtime dispatch from Phase ε.1.5 (asap-otel vs
/// asap-otap vs asap-telegraf), and Phase ε.1's three operational
/// modes — they only see those once `handle_plan` is later invoked.
/// Mirroring `handle_plan`'s typed pipeline here means bootstrap and
/// plan-push converge on the same emitted YAML.
async fn handle_bootstrap_agent_config(
    State(st): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    // Phase ε.1.5 — runtime dispatch from the X-Agent-Runtime header.
    // Defaults to `AsapOtel` for legacy agents that don't send
    // the header so the existing OTel-collector contrib build keeps
    // working with no client-side changes.
    let runtime = headers
        .get("X-Agent-Runtime")
        .and_then(|v| v.to_str().ok())
        .map(AgentRuntime::from_header)
        .unwrap_or_default();

    // Optional X-Agent-ID — when present, look up any pinned workload
    // assignment via the replanner so bootstrap returns the same plan
    // a subsequent OpAMP push would pin to. Avoids drift between the
    // initial fetch and the first push.
    let pinned_metric: Option<String> = headers
        .get("X-Agent-ID")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    if physical::stage_split::typed_stage_split_enabled() {
        match emit_bootstrap_typed(&st, runtime, pinned_metric.as_deref()).await {
            Ok(yaml) => {
                info!(
                    runtime = ?runtime, bytes = yaml.len(),
                    "[USE_TYPED_STAGE_SPLIT] emitted bootstrap config from typed path"
                );
                return (
                    StatusCode::OK,
                    [("content-type", "application/yaml")],
                    yaml,
                ).into_response();
            }
            Err(e) => {
                warn!(
                    runtime = ?runtime, error = %e,
                    "[USE_TYPED_STAGE_SPLIT] typed bootstrap path failed; \
                     falling back to legacy generate_agent_collector_config"
                );
                // Fall through to legacy path below.
            }
        }
    }

    // Legacy path — default DDSketch bootstrap (unchanged Phase α
    // behaviour). Serves as the backwards-compat fallback when the
    // typed gate is off OR when the typed path can't satisfy the
    // request (no workloads registered, unsupported topology, etc.).
    let cfg = AgentCollectorConfig {
        output_mode:          types::OutputMode::Sketch,
        sketch_type:          types::SketchType::DDSketch,
        sketch_params:        types::SketchParams::default(),
        aggregate_by:         vec![],
        label_matchers:       vec![],
        window_duration:      Some(std::time::Duration::from_secs(60)),
        mode:                 types::ProcessorMode::Window,
        enable_self_monitoring: true,
        transmit_sketch:      true,
        drop_original:        true,
        delta_transmission:   false,
        delta_threshold:      0.0,
        enable_series_id:     false,
        series_id_ttl_secs:   300,
        data_sink:            types::AgentDataSink::default(),
    };
    match generate_agent_collector_config(&cfg, &st.opamp_endpoint) {
        Ok(yaml) => (
            StatusCode::OK,
            [("content-type", "application/yaml")],
            yaml,
        ).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Run the typed L5 pipeline against the workload registry / pinned plan
/// and emit the Edge stage YAML for the given runtime.
///
/// Resolution order for "which workload does this agent get":
///   1. `X-Agent-ID` lookup → `replanner.agent_to_metric()` mapping
///      (the replanner's record of what plan the agent is currently
///      pinned to). When this hits, bootstrap == replan-push.
///   2. First agent-role entry in [`WorkloadRegistry`] — the same
///      heuristic the OpAMP `on_connect` callback uses for unassigned
///      agents.
///
/// Returns `Err` when none of the resolution paths land on a workload
/// the typed path can bind, when `bind_workload_typed` declines the
/// shape (multi-intent, raw-required, no aggregations), when stage
/// allocation fails, or when the per-stage map has no `Edge` entry.
/// The caller falls back to the legacy emitter on any error.
async fn emit_bootstrap_typed(
    st: &AppState,
    runtime: AgentRuntime,
    pinned_agent_id: Option<&str>,
) -> anyhow::Result<String> {
    use anyhow::{anyhow, Context};

    // 1. Resolve the metric this bootstrap should target.
    //    When the agent has a prior pinned assignment we honour it
    //    (pre-existing on_connect contract). The replanner's
    //    `agent_to_metrics()` is the source of truth for this mapping.
    //
    //    B2 (metric, role): an agent may pin multiple `(metric, role)`
    //    pairs. The bootstrap returns a single edge YAML, so we pick
    //    the FIRST pair's metric — the 5-sketch routing-connector
    //    pipeline emitted below covers every metric in the registry,
    //    not just this one.
    let pinned_metric: Option<String> = if let Some(aid) = pinned_agent_id {
        st.replanner
            .agent_to_metrics()
            .read()
            .await
            .get(aid)
            .and_then(|v| v.first().map(|(m, _)| m.clone()))
    } else {
        None
    };

    // Candidate metric resolution. When the agent has a prior pin, we
    //    try it first — a pinned raw-passthrough metric (e.g.
    //    `http_requests_total`) declines the typed bind, but the
    //    bootstrap still needs to ship the 5-sketch routing-connector
    //    edge config so OTHER metrics in the registry get processed.
    //    Walk the registry until we find one the typed path accepts —
    //    that gives us the edge_cfg shape — then populate
    //    `metric_to_family` from the FULL registry (every binding
    //    metric, not just the chosen one) below.
    //
    //    If pinned metric exists, it's the FIRST candidate; otherwise
    //    walk every agent-role entry in the registry.
    let candidates: Vec<String> = {
        let mut v = Vec::new();
        if let Some(p) = pinned_metric.clone() {
            v.push(p);
        }
        for entry in st.workload_registry.entries() {
            if !entry.assign_to_role.eq_ignore_ascii_case("agent") {
                continue;
            }
            if !v.contains(&entry.metric_name) {
                v.push(entry.metric_name.clone());
            }
        }
        v
    };
    if candidates.is_empty() {
        return Err(anyhow!("no agent-role workload available for bootstrap"));
    }

    // 2-3. Walk candidates: first metric that pre-populated the
    //      workload store AND binds via the typed path provides the
    //      base edge_cfg shape.
    //
    //      B2 (metric, role): a metric may have multiple roles
    //      registered; we walk every role's workload entry until one
    //      binds. The Sum-shaped roles (raw passthrough) decline the
    //      typed bind, so for `http_requests_total` the
    //      Quantile-shaped role on `http_requests_total_latency_ms`
    //      stays the source of the edge config.
    let mut chosen: Option<(String, crate::sketch_algebra::PhysicalExpr)> = None;
    'outer: for cand in &candidates {
        for (_, wl, _) in st.workload_store.get_all_for_metric(cand) {
            if let Some(expr) = optimizer::rules::bind_workload_typed(&wl) {
                chosen = Some((cand.clone(), expr));
                break 'outer;
            }
        }
    }
    let (metric, physical_expr) = chosen.ok_or_else(|| {
        anyhow!(
            "no registry metric binds via the typed path (all {} candidates declined)",
            candidates.len()
        )
    })?;
    let configs = physical::stage_split::split_typed_three_stage(&physical_expr)
        .ok_or_else(|| anyhow!("split_typed_three_stage returned None for `{metric}`"))?;

    // 4. Pick the Edge stage config and emit per-runtime. The
    //    bootstrap caller IS the edge agent — Gateway / Backend
    //    configs go to other roles via OpAMP role-routing, not
    //    through this handler.
    let mut edge_cfg = configs.into_iter().find_map(|(_, cfg)| match cfg {
        crate::physical::colored_dag::StageConfig::Edge(edge) => Some(edge),
        _ => None,
    }).ok_or_else(|| anyhow!("typed three-stage map has no Edge entry for `{metric}`"))?;

    // 5. Bootstrap-only plumbing: extend the typed Edge config with
    //    metrics that the live planner doesn't see but the MVP demo
    //    needs the agent to handle:
    //
    //    - Freshness probes (`http_freshness_probe_warm`,
    //      `http_freshness_probe_archive`): demo plumbing, not user
    //      metrics. The replay client polls the backend with
    //      `last_over_time(http_freshness_probe_warm[10s])` to gauge
    //      criterion ⑥. Without warm-passthrough routing the
    //      DDSketch processor renames them to `_quantile`; without
    //      gorillas3 archive write the warm engine has nothing to
    //      look at.
    //    - All non-archive workload-registry metrics: accuracy_reduce.py
    //      asks the archive engine for the SAME PromQL the warm sketch
    //      answered (criterion ④, archive-tier ground truth). If the
    //      under-test metric isn't in the Gorilla-S3 archive, every
    //      ground-truth query returns `archive_miss`. Adding the
    //      metrics here makes the agent's gorillas3 processor write
    //      them so the Thanos store-gateway can serve them later.
    //
    //    Both extensions are bootstrap-scope only — the live planner
    //    stays free to plan per-metric without these defaults bleeding
    //    in. The actual extension lives in the shared
    //    [`emit::extend_edge_with_demo_plumbing`] helper so the
    //    typed-replan push path (`replan::Replanner::push_config_to_agent`)
    //    can apply the same extension without duplicating the logic.
    let registry_metrics = st
        .workload_registry
        .entries()
        .iter()
        .map(|e| e.metric_name.clone());
    emit::extend_edge_with_demo_plumbing(&mut edge_cfg, registry_metrics);

    // 6. Stitch PR #339 (planner) → PR #340 (5-sketch routing emitter).
    //
    //    `bind_workload_typed` is per-metric. The 5-sketch routing-
    //    connector edge wire shape needs every sketched metric mapped
    //    to its committed family up-front so the emitter can build the
    //    `routing` connector's per-metric OTTL condition
    //    statements. Walk the workload registry, classify each metric
    //    via the planner, and drop the resulting HashMap into the
    //    EdgeStageConfig before emit. Empty map ⇒ legacy single-
    //    pipeline emit (raw-only deployment, registry empty, etc.).
    //
    //    Why this entry point: bootstrap is the place that already has
    //    all of `(WorkloadRegistry, WorkloadStore, edge_cfg)` in scope.
    //    Pushing the multi-metric loop down into
    //    `split_typed_three_stage` would change its signature for one
    //    caller (this one) and break the OpAMP-on-connect contract
    //    where the agent IS pinned to a single metric. Replan path
    //    (`replan::Replanner::try_emit_typed_edge_yaml_for_workload`)
    //    applies the same stitch via the same shared helper.
    edge_cfg.metric_to_family = emit::collect_metric_to_family(
        &st.workload_registry,
        &st.workload_store,
    );
    // MVP blocker B3 — companion stitch: per-metric grouping labels so
    // the 5-sketch routing emitter can prepend a `transform/keep_for_*`
    // OTTL processor in front of every sketch pipeline, reducing wire
    // attrs to the streaming-config's `grouping_labels` BEFORE sketching.
    edge_cfg.metric_to_grouping_labels = emit::collect_metric_to_grouping_labels(
        &st.workload_registry,
        &st.workload_store,
    );

    // Issue #2: thread X-Agent-ID into the opamp block. Bootstrap GET
    // is per-agent when `pinned_agent_id` is set (the agent's own
    // X-Agent-ID header on the bootstrap request); otherwise fall back
    // to the `$AGENT_ID` placeholder for the agent container's env to
    // expand at boot.
    let agent_id_for_emit = pinned_agent_id.unwrap_or("$AGENT_ID");
    emit_for_runtime(runtime, &edge_cfg, &st.opamp_endpoint, None, agent_id_for_emit)
        .with_context(|| format!("emit_for_runtime failed for `{metric}`"))
}

/// Returns the diff between the current and previous plan for `metric`.
/// 404 if the metric has no plan, 200 with `null` data if no previous plan exists.
async fn handle_plan_diff(
    State(st): State<AppState>,
    Path(metric): Path<String>,
) -> impl IntoResponse {
    // B2 (metric, role): a metric may carry multiple roles; surface a
    // per-role `roles` array. The top-level `has_diff` is true iff at
    // least one role has a diff. Pre-B2 single-role clients see
    // `has_diff` and the `diff` field of the first role with one;
    // the new shape stays additive (no URL change, response keys
    // preserved).
    let plans = st.store.get_all_for_metric(&metric);
    if plans.is_empty() {
        return (
            StatusCode::NOT_FOUND,
            format!("plan not found for metric {metric:?}"),
        )
            .into_response();
    }
    let mut roles: Vec<serde_json::Value> = Vec::with_capacity(plans.len());
    let mut first_diff: Option<serde_json::Value> = None;
    let mut any_has_diff = false;
    for (role, _) in plans {
        match st.store.diff(&metric, role) {
            Ok(Some(diff)) => {
                let diff_json = serde_json::to_value(&diff).unwrap_or(serde_json::Value::Null);
                if first_diff.is_none() {
                    first_diff = Some(diff_json.clone());
                }
                any_has_diff = true;
                roles.push(json!({
                    "role": role.as_str(),
                    "has_diff": true,
                    "diff": diff_json,
                }));
            }
            Ok(None) => {
                roles.push(json!({ "role": role.as_str(), "has_diff": false }));
            }
            Err(e) => {
                roles.push(json!({
                    "role": role.as_str(),
                    "error": e.to_string(),
                }));
            }
        }
    }
    let body = if any_has_diff {
        json!({
            "metric":   metric,
            "has_diff": true,
            "diff":     first_diff,
            "roles":    roles,
        })
    } else {
        json!({
            "metric":   metric,
            "has_diff": false,
            "roles":    roles,
        })
    };
    (StatusCode::OK, Json(body)).into_response()
}

/// Returns the current EMA cost model state — blended benchmark + observed costs
/// per sketch type.  Useful for diagnosing whether the online cost model has
/// received sufficient observations to meaningfully influence plan selection.
async fn handle_cost_model(State(st): State<AppState>) -> impl IntoResponse {
    let table = online_cost_model::effective_table(&st.online_store);
    let raw   = st.online_store.try_read();

    let entries: Vec<serde_json::Value> = table.iter().map(|(sketch_type, costs)| {
        let observations = raw.as_ref().ok()
            .and_then(|m| m.get(sketch_type))
            .map(|o| o.observations)
            .unwrap_or(0);
        json!({
            "sketch_type":               sketch_type.to_string(),
            "bw_bytes_per_series_per_sec": costs.bytes_per_series_per_sec,
            "cpu_micros_per_sample":       costs.cpu_micros_per_sample,
            "base_memory_bytes":           costs.base_memory_bytes,
            "relative_error":              costs.relative_error_at_default,
            "observations":                observations,
        })
    }).collect();

    (StatusCode::OK, Json(json!({ "sketches": entries }))).into_response()
}

// ── TCO endpoint ─────────────────────────────────────────────────────────────

#[derive(serde::Deserialize)]
struct TcoRequest {
    workload: tco::TcoWorkload,
    pricing: Option<tco::CloudPricing>,
}

async fn handle_tco(Json(req): Json<TcoRequest>) -> impl IntoResponse {
    let pricing = req.pricing.unwrap_or_default();
    let estimate = tco::estimate_tco(&req.workload, &pricing);
    (StatusCode::OK, Json(estimate))
}

fn short_hash(s: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    s.hash(&mut h);
    format!("{:016x}", h.finish())
}

// ── Test helpers ──────────────────────────────────────────────────────────────

/// Builds a minimal `AppState` + `Router` for integration tests.
/// No background tasks are started; OpAMP/scraper hold no real connections.
#[cfg(test)]
fn test_app() -> (AppState, axum::Router) {
    test_app_with_backend(None)
}

/// Phase C test helper: build an `AppState` whose `backend_client` is
/// optionally set to a real `BackendClient` pointed at a mock URL. The
/// `None` arm is the legacy path used by every existing test;
/// `Some(url)` is the new entry point for Phase C tests that exercise
/// the typed L5 backend-JSON push.
#[cfg(test)]
fn test_app_with_backend(backend_url: Option<String>) -> (AppState, axum::Router) {
    let online_store   = init_online_store();
    let plan_store     = Arc::new(PlanStore::new());
    let workload_store = Arc::new(WorkloadStore::new());
    let opamp          = Arc::new(OpampServer::new());
    let scraper        = Arc::new(Scraper::new(
        vec![], Thresholds::default(), Arc::new(|_| {}), Duration::from_secs(60),
    ));
    let planner = Arc::new(BaselinePlanner::new(
        CostModelPlanner::new().with_online_store(Arc::clone(&online_store)),
    ));
    let replanner = Arc::new(Replanner::new(
        Arc::clone(&planner),
        Arc::clone(&plan_store),
        Arc::clone(&workload_store),
        Arc::clone(&opamp),
        Arc::clone(&scraper),
        "ws://ctrl:4320/v1/opamp",
    ));
    let backend_client = backend_url
        .map(|u| Arc::new(backend_client::BackendClient::new(u)));
    let state = AppState {
        analyzer:          Arc::new(Analyzer::new()),
        planner,
        store:             Arc::clone(&plan_store),
        workload_store:    Arc::clone(&workload_store),
        opamp,
        scraper,
        replanner,
        online_store,
        opamp_endpoint:    "ws://ctrl:4320/v1/opamp".into(),
        workload_registry: Arc::new(WorkloadRegistry::empty()),
        runtime_samples:   runtime_samples::RuntimeSamplesStore::new(64),
        backend_client,
        backend_routing_cache: Arc::new(Mutex::new(HashMap::new())),
    };
    let router = axum::Router::new()
        .route("/api/v1/plan",                  axum::routing::post(handle_plan))
        .route("/api/v1/plan/pareto",           axum::routing::post(handle_pareto))
        .route("/api/v1/plan/:metric",          axum::routing::get(handle_get_plan))
        .route("/api/v1/plan/:metric/rollback", axum::routing::post(handle_rollback))
        .route("/api/v1/plan/:metric/diff",     axum::routing::get(handle_plan_diff))
        .route("/api/v1/agents",                axum::routing::get(handle_agents))
        .route("/api/v1/cost-model",            axum::routing::get(handle_cost_model))
        .route("/api/v1/tco",                   axum::routing::post(handle_tco))
        .route("/api/v1/collector-config/agent", axum::routing::get(handle_bootstrap_agent_config))
        .with_state(state.clone());
    (state, router)
}

#[cfg(test)]
mod api_tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    async fn body_json(resp: axum::response::Response) -> serde_json::Value {
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    fn plan_spec(metric: &str) -> serde_json::Value {
        serde_json::json!({
            "metric_name":  metric,
            "aggregations": ["quantile"],
            "time_window":  "5m",
            "accuracy_sla": 0.01
        })
    }

    // ── Phase C: AppState.backend_client wiring ───────────────────────────────

    /// Default-constructed AppState (no `CONTROLLER_BACKEND_ENDPOINT`)
    /// must leave `backend_client` as `None` so the typed L5 backend
    /// JSON push silently no-ops, matching the Phase B fire-and-forget
    /// contract.
    #[test]
    fn app_state_backend_client_none_by_default() {
        let (state, _router) = test_app();
        assert!(state.backend_client.is_none(),
            "backend_client should default to None when no endpoint is configured");
    }

    /// When constructed with a backend URL (the production path takes
    /// it from `CONTROLLER_BACKEND_ENDPOINT`), the field is populated
    /// and ready for the Phase C `handle_plan` push.
    #[test]
    fn app_state_backend_client_some_when_constructed_with_url() {
        let (state, _router) = test_app_with_backend(
            Some("http://127.0.0.1:1/api/v1/streaming-config".into()),
        );
        let bc = state.backend_client.expect("backend_client must be Some");
        assert_eq!(bc.endpoint(), "http://127.0.0.1:1/api/v1/streaming-config");
    }

    // ── POST /api/v1/plan ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn plan_happy_path() {
        let (_, app) = test_app();
        let req = Request::builder()
            .method("POST")
            .uri("/api/v1/plan")
            .header("content-type", "application/json")
            .body(Body::from(plan_spec("latency").to_string()))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["metric"], "latency");
        assert!(body["sketch_type"].as_str().is_some());
        assert!(body["valid_until"].as_str().is_some());
    }

    #[tokio::test]
    async fn plan_invalid_spec_returns_422() {
        let (_, app) = test_app();
        let req = Request::builder()
            .method("POST")
            .uri("/api/v1/plan")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"metric_name":"","aggregations":["quantile"],"time_window":"5m","accuracy_sla":0.01}"#))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn plan_invalid_aggregation_returns_422() {
        let (_, app) = test_app();
        let bad = serde_json::json!({
            "metric_name": "m", "aggregations": ["histogram"],
            "time_window": "5m", "accuracy_sla": 0.01
        });
        let req = Request::builder()
            .method("POST").uri("/api/v1/plan")
            .header("content-type", "application/json")
            .body(Body::from(bad.to_string())).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    // ── GET /api/v1/plan/:metric ──────────────────────────────────────────────

    #[tokio::test]
    async fn get_plan_not_found_returns_404() {
        let (_, app) = test_app();
        let req = Request::builder()
            .uri("/api/v1/plan/nonexistent").body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn get_plan_after_post() {
        let (_, app) = test_app();
        // POST first
        let post_req = Request::builder()
            .method("POST").uri("/api/v1/plan")
            .header("content-type", "application/json")
            .body(Body::from(plan_spec("cpu").to_string())).unwrap();
        let post_resp = app.clone().oneshot(post_req).await.unwrap();
        assert_eq!(post_resp.status(), StatusCode::OK);
        // Then GET
        let get_req = Request::builder()
            .uri("/api/v1/plan/cpu").body(Body::empty()).unwrap();
        let get_resp = app.oneshot(get_req).await.unwrap();
        assert_eq!(get_resp.status(), StatusCode::OK);
        let body = body_json(get_resp).await;
        assert_eq!(body["metric"], "cpu");
    }

    // ── POST /api/v1/plan/:metric/rollback ────────────────────────────────────

    #[tokio::test]
    async fn rollback_no_previous_returns_400() {
        let (st, app) = test_app();
        // Seed one plan directly.
        use crate::optimizer::rules::RulesPlanner;
        let wl = crate::types::QueryWorkload {
            metric_name: "m".into(),
            label_filters: std::collections::HashMap::new(),
            group_by_labels: vec![],
            aggregations: vec![crate::types::AggType::Quantile],
            time_window: std::time::Duration::from_secs(300),
            repeat_every: None, accuracy_sla: 0.01, latency_sla: None,
            sketch_type_override: None, exact_required: false, quantiles: vec![],
        };
        st.store.set("m", control_plane::workload::AggRole::Quantile, RulesPlanner::new().plan(&wl));
        let req = Request::builder()
            .method("POST").uri("/api/v1/plan/m/rollback")
            .body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn rollback_not_found_returns_400() {
        let (_, app) = test_app();
        let req = Request::builder()
            .method("POST").uri("/api/v1/plan/ghost/rollback")
            .body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // ── GET /api/v1/plan/:metric/diff ─────────────────────────────────────────

    #[tokio::test]
    async fn diff_no_previous_returns_has_diff_false() {
        let (_, app) = test_app();
        // POST a plan once.
        let req = Request::builder()
            .method("POST").uri("/api/v1/plan")
            .header("content-type", "application/json")
            .body(Body::from(plan_spec("rtt").to_string())).unwrap();
        app.clone().oneshot(req).await.unwrap();
        // Diff should exist but has_diff=false (only one version).
        let req = Request::builder()
            .uri("/api/v1/plan/rtt/diff").body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["has_diff"], false);
    }

    #[tokio::test]
    async fn diff_not_found_returns_404() {
        let (_, app) = test_app();
        let req = Request::builder()
            .uri("/api/v1/plan/ghost/diff").body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // ── GET /api/v1/cost-model ────────────────────────────────────────────────

    #[tokio::test]
    async fn cost_model_returns_all_sketch_types() {
        let (_, app) = test_app();
        let req = Request::builder()
            .uri("/api/v1/cost-model").body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        let sketches = body["sketches"].as_array().unwrap();
        assert!(sketches.len() >= 4, "expected at least 4 sketch types");
        for s in sketches {
            assert!(s["sketch_type"].as_str().is_some());
            assert!(s["observations"].as_u64().is_some());
        }
    }

    // ── POST /api/v1/plan/pareto ──────────────────────────────────────────────

    #[tokio::test]
    async fn pareto_returns_frontier_for_quantile() {
        let (_, app) = test_app();
        let body = serde_json::json!({
            "metric_name": "latency", "aggregations": ["quantile"],
            "time_window": "5m", "accuracy_sla": 0.02,
            "weights": { "bandwidth": 0.7, "cpu": 0.2, "memory": 0.1 }
        });
        let req = Request::builder()
            .method("POST").uri("/api/v1/plan/pareto")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string())).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        let frontier = body["frontier"].as_array().unwrap();
        assert!(!frontier.is_empty(), "frontier should not be empty");
        assert!(body["best"].as_str().is_some(), "best sketch should be set");
    }

    // ── GET /api/v1/agents ────────────────────────────────────────────────────

    #[tokio::test]
    async fn agents_returns_empty_map_initially() {
        let (_, app) = test_app();
        let req = Request::builder()
            .uri("/api/v1/agents").body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        // No agents connected → empty object.
        assert_eq!(body, serde_json::json!({}));
    }

    // ── POST /api/v1/tco ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn tco_returns_valid_estimate() {
        let (_, app) = test_app();
        let body = serde_json::json!({
            "workload": {
                "series_count": 100000,
                "samples_per_sec": 1.0,
                "bytes_per_sample": 100,
                "scrape_interval_secs": 15,
                "queries_per_sec": 1.0,
                "query_window_secs": 300,
                "retention_days": 30,
                "sketch_compression_ratio": 0.05,
                "delta_compression_ratio": 0.3
            }
        });
        let req = Request::builder()
            .method("POST").uri("/api/v1/tco")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string())).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert!(body["before"]["total_dollars"].as_f64().unwrap() > 0.0);
        assert!(body["after"]["total_dollars"].as_f64().unwrap() > 0.0);
        assert!(body["savings_percent"].as_f64().unwrap() > 0.0);
    }

    // ── Integration: control plane ↔ collector wiring ─────────────────────────

    /// Helper: start an OpAMP WebSocket server on a random port.
    /// Returns the (server Arc, local addr string).
    async fn start_opamp_server(opamp: Arc<OpampServer>) -> String {
        let router = axum::Router::new()
            .route("/v1/opamp", axum::routing::get(OpampServer::ws_handler))
            .with_state(opamp);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        format!("127.0.0.1:{}", addr.port())
    }

    /// Connect a mock agent via WebSocket, returning the stream.
    async fn connect_agent(
        opamp_addr: &str,
        agent_id: &str,
        role: &str,
    ) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let url = format!("ws://{opamp_addr}/v1/opamp");
        let mut req = url.into_client_request().unwrap();
        req.headers_mut().insert("X-Agent-ID", agent_id.parse().unwrap());
        req.headers_mut().insert("X-Agent-Role", role.parse().unwrap());
        let (ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
        ws
    }

    /// Read the next binary WebSocket frame, decode as OpAMP ServerToAgent,
    /// and extract the YAML config body.
    async fn recv_config_yaml(
        ws: &mut tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    ) -> String {
        use tokio_tungstenite::tungstenite::Message;
        let msg = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            futures_util::StreamExt::next(ws),
        ).await.expect("timeout waiting for config push")
         .expect("stream ended")
         .expect("ws error");
        match msg {
            Message::Binary(data) => {
                let payload = if !data.is_empty() && data[0] == 0 {
                    &data[1..]
                } else {
                    data.as_slice()
                };
                let sta = <crate::opamp::opamp_proto::ServerToAgent as prost::Message>::decode(
                    payload,
                ).expect("decode ServerToAgent");
                let rc = sta.remote_config.expect("remote_config present");
                let cm = rc.config.expect("config present");
                let file = cm.config_map.get("").expect("empty-key config file");
                String::from_utf8(file.body.clone()).expect("yaml is utf8")
            }
            other => panic!("expected binary frame, got {other:?}"),
        }
    }

    /// Test 1: Agent connects with workloads.yaml pre-populated, receives config on connect.
    #[tokio::test]
    async fn agent_receives_config_on_connect_via_workload_registry() {
        // Build a full AppState with a workload registry entry.
        let online_store   = init_online_store();
        let plan_store     = Arc::new(PlanStore::new());
        let workload_store = Arc::new(WorkloadStore::new());
        let opamp          = Arc::new(OpampServer::new());
        let scraper        = Arc::new(Scraper::new(
            vec![], Thresholds::default(), Arc::new(|_| {}), Duration::from_secs(60),
        ));
        let planner = Arc::new(BaselinePlanner::new(
            CostModelPlanner::new().with_online_store(Arc::clone(&online_store)),
        ));

        // Pre-populate plan store (simulating what main() does with workload registry).
        let analyzer = Analyzer::new();
        let spec = pipeline::QuerySpec {
            query_string:    None,
            metric_name:     "http_latency".into(),
            label_filters:   Default::default(),
            group_by_labels: vec![],
            aggregations:    vec!["quantile".into()],
            time_window:     "5m".into(),
            repeat_every:    None,
            accuracy_sla:    0.01,
            latency_sla:     None,
            sketch_type:     None,
            workload:        types::WorkloadCharacteristics::default(),
            id:               None,
            language:         None,
            accuracy:         None,
            dollars:          None,
            deployment_model: None,
            shape:            types_v2::QueryShape::default(),
            data:             types_v2::DataShape::default(),
        };
        let wl = analyzer.analyze(spec).unwrap();
        let wc = types::WorkloadCharacteristics::default();
        let plan = planner.plan(&wl, Some(&wc));
        // B2 (metric, role): pre-populate using the same role the
        // on_connect callback's `derive_agg_role(entry)` will compute
        // for this test's workloads.yaml entry (no query_string + no
        // sketch_family_override → AggRole::Other). Without matching
        // the role, `push_config_to_agent`'s workload_store.get
        // returns None and the on_connect path silently bails.
        plan_store.set("http_latency", control_plane::workload::AggRole::Other, plan);
        workload_store.set("http_latency", control_plane::workload::AggRole::Other, wl, wc);

        // Build replanner and late-binding cells.
        let replanner_cell: Arc<tokio::sync::RwLock<Option<Arc<Replanner>>>> =
            Arc::new(tokio::sync::RwLock::new(None));
        let registry_cell: Arc<tokio::sync::RwLock<Option<Arc<WorkloadRegistry>>>> =
            Arc::new(tokio::sync::RwLock::new(None));

        let opamp_ep = "ws://127.0.0.1:0/v1/opamp".to_string();

        // Wire on_connect callback — same logic as main().
        let sc = Arc::clone(&scraper);
        let connect_cell = Arc::clone(&replanner_cell);
        let connect_registry = Arc::clone(&registry_cell);
        let opamp_srv = Arc::new(
            OpampServer::new()
                .with_on_connect(move |agent_id, _role| {
                    let url = format!("http://{agent_id}/metrics");
                    let sc = Arc::clone(&sc);
                    let id_copy = agent_id.clone();
                    let cell = Arc::clone(&connect_cell);
                    let reg = Arc::clone(&connect_registry);
                    let aid = agent_id.clone();
                    tokio::spawn(async move {
                        sc.add_endpoint(Endpoint::new(id_copy, url)).await;
                        if let Some(r) = cell.read().await.as_ref() {
                            let pushed = r.push_config_to_agent(&aid).await;
                            if !pushed {
                                if let Some(registry) = reg.read().await.as_ref() {
                                    if let Some(entry) = registry.first_for_role("agent") {
                                        let role = control_plane::workload::derive_agg_role(entry);
                                        r.register_agent(&aid, &entry.metric_name, role).await;
                                        r.push_config_to_agent(&aid).await;
                                    }
                                }
                            }
                        }
                    });
                }),
        );

        let replanner = Arc::new(Replanner::new(
            Arc::clone(&planner),
            Arc::clone(&plan_store),
            Arc::clone(&workload_store),
            Arc::clone(&opamp_srv),
            Arc::clone(&scraper),
            opamp_ep,
        ));

        // Build a workload registry with one entry matching the pre-populated plan.
        let registry = Arc::new(WorkloadRegistry::load("/nonexistent")); // empty
        // We'll create one inline with the correct metric name.
        let yaml = "- metric_name: http_latency\n  accuracy_sla: 0.01\n  assign_to_role: agent\n";
        let entries: Vec<crate::workload::WorkloadEntry> =
            serde_yaml::from_str(yaml).unwrap();
        // WorkloadRegistry doesn't have a public constructor from entries, so we
        // test via the first_for_role interface that the on_connect path uses.
        // Bind the cells.
        *replanner_cell.write().await = Some(Arc::clone(&replanner));
        // We need a registry that returns "http_latency". Load trick:
        let tmp_path = "/tmp/datacollector_test_workloads.yaml";
        std::fs::write(tmp_path, yaml).unwrap();
        let registry = Arc::new(WorkloadRegistry::load(tmp_path));
        *registry_cell.write().await = Some(Arc::clone(&registry));

        // Start OpAMP WS server.
        let addr = start_opamp_server(Arc::clone(&opamp_srv)).await;

        // Connect a mock agent.
        let mut ws = connect_agent(&addr, "test-agent-1", "agent").await;

        // The on_connect callback should assign the workload and push config.
        let yaml_config = recv_config_yaml(&mut ws).await;

        // Verify the config has the expected sketch processor.
        assert!(
            yaml_config.contains("ddsketch")
                || yaml_config.contains("KLL")
                || yaml_config.contains("KLL:"),
            "expected a sketch processor in the pushed config:\n{yaml_config}"
        );
        // Verify OpAMP extension is present.
        assert!(
            yaml_config.contains("opamp"),
            "pushed config should include opamp extension:\n{yaml_config}"
        );

        std::fs::remove_file(tmp_path).ok();
    }

    /// Test 2: Re-plan pushes config only to agents registered for that metric.
    #[tokio::test]
    async fn replan_pushes_only_to_registered_agent() {
        let online_store   = init_online_store();
        let plan_store     = Arc::new(PlanStore::new());
        let workload_store = Arc::new(WorkloadStore::new());
        let opamp_srv      = Arc::new(OpampServer::new());
        let scraper        = Arc::new(Scraper::new(
            vec![], Thresholds::default(), Arc::new(|_| {}), Duration::from_secs(60),
        ));
        let planner = Arc::new(BaselinePlanner::new(
            CostModelPlanner::new().with_online_store(Arc::clone(&online_store)),
        ));

        // Seed workload + plan for "metric_a".
        let analyzer = Analyzer::new();
        let spec = pipeline::QuerySpec {
            query_string:    None,
            metric_name:     "metric_a".into(),
            label_filters:   Default::default(),
            group_by_labels: vec![],
            aggregations:    vec!["quantile".into()],
            time_window:     "5m".into(),
            repeat_every:    None,
            accuracy_sla:    0.01,
            latency_sla:     None,
            sketch_type:     None,
            workload:        types::WorkloadCharacteristics::default(),
            id:               None,
            language:         None,
            accuracy:         None,
            dollars:          None,
            deployment_model: None,
            shape:            types_v2::QueryShape::default(),
            data:             types_v2::DataShape::default(),
        };
        let wl = analyzer.analyze(spec).unwrap();
        let wc = types::WorkloadCharacteristics::default();
        let plan = planner.plan(&wl, Some(&wc));
        plan_store.set("metric_a", control_plane::workload::AggRole::Quantile, plan);
        workload_store.set("metric_a", control_plane::workload::AggRole::Quantile, wl, wc);

        let replanner = Arc::new(Replanner::new(
            Arc::clone(&planner),
            Arc::clone(&plan_store),
            Arc::clone(&workload_store),
            Arc::clone(&opamp_srv),
            Arc::clone(&scraper),
            "ws://ctrl:4320/v1/opamp",
        ));

        // Start OpAMP server and connect two agents.
        let addr = start_opamp_server(Arc::clone(&opamp_srv)).await;
        let mut ws_a = connect_agent(&addr, "agent-a", "agent").await;
        let mut ws_b = connect_agent(&addr, "agent-b", "agent").await;
        // Let connections register.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Register agent-a for metric_a, agent-b is NOT registered for metric_a.
        replanner
            .register_agent("agent-a", "metric_a", control_plane::workload::AggRole::Quantile)
            .await;
        replanner
            .register_agent("agent-b", "metric_b", control_plane::workload::AggRole::Quantile)
            .await;

        // Trigger replan for metric_a.
        let ok = replanner.replan_metric("metric_a").await;
        assert!(ok, "replan should succeed");

        // agent-a should receive a config push.
        let yaml_a = recv_config_yaml(&mut ws_a).await;
        assert!(!yaml_a.is_empty(), "agent-a should have received config");

        // agent-b should NOT receive anything (timeout).
        let result_b = tokio::time::timeout(
            Duration::from_millis(500),
            futures_util::StreamExt::next(&mut ws_b),
        ).await;
        assert!(
            result_b.is_err(),
            "agent-b should NOT receive config for metric_a replan"
        );
    }

    /// Test 3: Generated agent YAML contains extensions.opamp with correct endpoint.
    #[tokio::test]
    async fn generated_agent_yaml_contains_opamp_extension() {
        let endpoint = "ws://my-controller:4320/v1/opamp";
        let cfg = AgentCollectorConfig {
            output_mode:          types::OutputMode::Sketch,
            sketch_type:          types::SketchType::DDSketch,
            sketch_params:        types::SketchParams::default(),
            aggregate_by:         vec![],
            label_matchers:       vec![],
            window_duration:      Some(Duration::from_secs(60)),
            mode:                 types::ProcessorMode::Window,
            enable_self_monitoring: true,
            transmit_sketch:      true,
            drop_original:        true,
            delta_transmission:   false,
            delta_threshold:      0.0,
            enable_series_id:     false,
            series_id_ttl_secs:   300,
            // This test asserts on `doc["exporters"]["prometheus"]`
            // (line ~1326). Keep the test semantics by pinning the
            // sink to the legacy prometheus exporter.
            data_sink:            types::AgentDataSink::PrometheusScrape {
                endpoint: "0.0.0.0:8889".to_string(),
            },
        };
        let yaml = generate_agent_collector_config(&cfg, endpoint).unwrap();

        // Parse the YAML to verify structure, not just substring matches.
        let doc: serde_yaml::Value = serde_yaml::from_str(&yaml).unwrap();

        // 1. extensions.opamp.server.ws.endpoint matches the parameter.
        let opamp_ext = &doc["extensions"]["opamp"];
        assert!(
            !opamp_ext.is_null(),
            "YAML missing extensions.opamp:\n{yaml}"
        );
        let ws_endpoint = opamp_ext["server"]["ws"]["endpoint"].as_str().unwrap();
        assert_eq!(
            ws_endpoint, endpoint,
            "OpAMP endpoint mismatch"
        );

        // 2. service.extensions list includes "opamp".
        let svc_exts = doc["service"]["extensions"].as_sequence().unwrap();
        let has_opamp = svc_exts.iter().any(|v| v.as_str() == Some("opamp"));
        assert!(
            has_opamp,
            "service.extensions should include 'opamp':\n{yaml}"
        );

        // 3. The YAML is complete: has receivers, processors, exporters, service.pipelines.
        assert!(doc["receivers"]["otlp"].is_mapping(), "missing receivers.otlp");
        assert!(doc["exporters"]["prometheus"].is_mapping(), "missing exporters.prometheus");
        let pipeline = &doc["service"]["pipelines"]["metrics"];
        assert!(pipeline["receivers"].is_sequence(), "missing pipeline receivers");
        assert!(pipeline["processors"].is_sequence(), "missing pipeline processors");
        assert!(pipeline["exporters"].is_sequence(), "missing pipeline exporters");
    }

    #[tokio::test]
    async fn tco_with_custom_pricing() {
        let (_, app) = test_app();
        let body = serde_json::json!({
            "workload": {
                "series_count": 50000,
                "samples_per_sec": 1.0,
                "bytes_per_sample": 100,
                "scrape_interval_secs": 15,
                "queries_per_sec": 1.0,
                "query_window_secs": 300,
                "retention_days": 30,
                "sketch_compression_ratio": 0.05,
                "delta_compression_ratio": 0.3
            },
            "pricing": {
                "grafana_per_1k_series_1dpm": 8.0,
                "s3_storage_per_gb_month": 0.023,
                "s3_put_per_1k": 0.005,
                "s3_get_per_1k": 0.0004,
                "s3_transfer_per_gb": 0.09,
                "ec2_sketch_instance_per_hour": 0.384
            }
        });
        let req = Request::builder()
            .method("POST").uri("/api/v1/tco")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string())).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        // With higher Grafana pricing, before cost should be higher.
        assert!(body["before"]["ingestion_dollars"].as_f64().unwrap() > 0.0);
        assert!(body["monthly_savings_dollars"].as_f64().unwrap() > 0.0);
    }

    // ── Phase ε.1.5+ — handle_bootstrap_agent_config typed path ────────────────
    //
    // These tests verify the deep fix that ports the bootstrap handler
    // off `generate_agent_collector_config` and onto the typed-stage-split emit
    // pipeline that `handle_plan` already uses. See the handler's
    // doc-comment for the legacy ↔ typed behaviour matrix.

    /// Serialises tests that mutate the `USE_TYPED_STAGE_SPLIT` env var
    /// — `cargo test` runs tests in parallel by default and
    /// `typed_stage_split_enabled()` reads the env on every call.
    static ENV_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// RAII helper: set `USE_TYPED_STAGE_SPLIT=<value>` for the
    /// lifetime of the returned guard, restoring the prior value
    /// (or unsetting) on drop. Holds the test-wide ENV_GUARD mutex
    /// so concurrent tests don't trample each other.
    struct EnvVarGuard {
        key: &'static str,
        previous: Option<String>,
        // Hold the mutex so concurrent tests serialise on env-var writes.
        _lock: std::sync::MutexGuard<'static, ()>,
    }
    impl EnvVarGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let lock = ENV_GUARD.lock().unwrap_or_else(|p| p.into_inner());
            let previous = std::env::var(key).ok();
            std::env::set_var(key, value);
            Self { key, previous, _lock: lock }
        }
        fn unset(key: &'static str) -> Self {
            let lock = ENV_GUARD.lock().unwrap_or_else(|p| p.into_inner());
            let previous = std::env::var(key).ok();
            std::env::remove_var(key);
            Self { key, previous, _lock: lock }
        }
    }
    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match &self.previous {
                Some(v) => std::env::set_var(self.key, v),
                None => std::env::remove_var(self.key),
            }
        }
    }

    /// Build an `AppState` whose `workload_registry` + `workload_store`
    /// + `plan_store` are pre-populated with one agent-role workload —
    /// matches what `main()` does at startup.
    ///
    /// Returns the (state, router, registry-tempfile-path) triple. The
    /// caller is responsible for cleaning up the tempfile.
    fn test_app_with_workload(metric: &str, accuracy: f64) -> (AppState, axum::Router, String) {
        // 1. Write a workload registry YAML to a tempfile so
        //    `WorkloadRegistry::load` produces a registry with the
        //    metric assigned to role=agent.
        let yaml = format!(
            "- metric_name: {metric}\n  accuracy_sla: {accuracy}\n  assign_to_role: agent\n",
        );
        let tmp_path = format!("/tmp/datacollector_bootstrap_test_{metric}.yaml");
        std::fs::write(&tmp_path, yaml).unwrap();
        let registry = Arc::new(WorkloadRegistry::load(&tmp_path));

        // 2. Build a stock test_app (empty registry + empty stores).
        let (mut state, _router) = test_app();

        // 3. Pre-populate workload_store + plan_store the same way
        //    main()'s startup loop does.
        let analyzer = Analyzer::new();
        let spec = pipeline::QuerySpec {
            query_string:    None,
            metric_name:     metric.to_string(),
            label_filters:   Default::default(),
            group_by_labels: vec![],
            aggregations:    vec!["quantile".into()],
            time_window:     "5m".into(),
            repeat_every:    None,
            accuracy_sla:    accuracy,
            latency_sla:     None,
            sketch_type:     None,
            workload:        types::WorkloadCharacteristics::default(),
            id:               None,
            language:         None,
            accuracy:         None,
            dollars:          None,
            deployment_model: None,
            shape:            types_v2::QueryShape::default(),
            data:             types_v2::DataShape::default(),
        };
        let wl = analyzer.analyze(spec).expect("analyze");
        let wc = types::WorkloadCharacteristics::default();
        let plan = state.planner.plan(&wl, Some(&wc));
        state.store.set(metric, control_plane::workload::AggRole::Quantile, plan);
        state.workload_store.set(metric, control_plane::workload::AggRole::Quantile, wl, wc);

        // 4. Swap in the populated registry.
        state.workload_registry = registry;

        // 5. Rebuild the router with the updated state.
        let router = axum::Router::new()
            .route("/api/v1/plan",                  axum::routing::post(handle_plan))
            .route("/api/v1/plan/pareto",           axum::routing::post(handle_pareto))
            .route("/api/v1/plan/:metric",          axum::routing::get(handle_get_plan))
            .route("/api/v1/plan/:metric/rollback", axum::routing::post(handle_rollback))
            .route("/api/v1/plan/:metric/diff",     axum::routing::get(handle_plan_diff))
            .route("/api/v1/agents",                axum::routing::get(handle_agents))
            .route("/api/v1/cost-model",            axum::routing::get(handle_cost_model))
            .route("/api/v1/tco",                   axum::routing::post(handle_tco))
            .route("/api/v1/collector-config/agent",
                axum::routing::get(handle_bootstrap_agent_config))
            .with_state(state.clone());
        (state, router, tmp_path)
    }

    /// Backwards-compat — when `USE_TYPED_STAGE_SPLIT` is unset the
    /// handler must keep its legacy `generate_agent_collector_config` shape
    /// (default DDSketch, `processors.ddsketch`, `processors.batch`)
    /// so deployments that haven't migrated keep working.
    #[tokio::test]
    async fn bootstrap_legacy_path_when_env_unset() {
        let _env = EnvVarGuard::unset(physical::stage_split::ENV_USE_TYPED_STAGE_SPLIT);

        let (_, app) = test_app();
        let req = Request::builder()
            .uri("/api/v1/collector-config/agent")
            .body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let yaml = String::from_utf8(body.to_vec()).unwrap();

        // Legacy bootstrap fingerprint: a `ddsketch:` processor block.
        assert!(
            yaml.contains("ddsketch:"),
            "legacy bootstrap should emit ddsketch processor; got:\n{yaml}"
        );
    }

    /// `USE_TYPED_STAGE_SPLIT=1` + a workload routed through the typed
    /// L5 emit → the YAML is the typed Edge config (ddsketch)
    /// rather than the legacy default DDSketch shape.
    #[tokio::test]
    async fn bootstrap_typed_path_when_env_set() {
        let _env = EnvVarGuard::set(physical::stage_split::ENV_USE_TYPED_STAGE_SPLIT, "1");

        let (_, app, tmp) = test_app_with_workload("http_latency", 0.01);
        let req = Request::builder()
            .uri("/api/v1/collector-config/agent")
            .body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let yaml = String::from_utf8(body.to_vec()).unwrap();
        std::fs::remove_file(&tmp).ok();

        // Typed Edge fingerprint: valid patched collector component id.
        assert!(
            yaml.contains("ddsketch:"),
            "typed bootstrap should emit `ddsketch:`:\n{yaml}"
        );
    }

    /// `X-Agent-Runtime: asap-otap` → emitter dispatches through
    /// `emit_otap_dag_yaml` rather than the OTel-collector emit. The
    /// output shape is YAML-but-not-OTel — we identify it by the
    /// otap-dataflow DAG version token.
    #[tokio::test]
    async fn bootstrap_typed_path_asap_otap_runtime_dispatch() {
        let _env = EnvVarGuard::set(physical::stage_split::ENV_USE_TYPED_STAGE_SPLIT, "1");

        let (_, app, tmp) = test_app_with_workload("rtt_otap", 0.01);
        let req = Request::builder()
            .uri("/api/v1/collector-config/agent")
            .header("X-Agent-Runtime", "asap-otap")
            .body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let yaml = String::from_utf8(body.to_vec()).unwrap();
        std::fs::remove_file(&tmp).ok();

        // Mirrors the assertion in `config::runtime_tests::emit_for_runtime_otap_yields_dag_yaml`.
        assert!(
            yaml.contains("otel_dataflow/v1"),
            "asap-otap runtime should produce the otap-dataflow DAG YAML:\n{yaml}"
        );
    }

    /// `X-Agent-Runtime: asap-telegraf` → emitter dispatches through
    /// `emit_telegraf_toml` and produces TOML rather than YAML.
    #[tokio::test]
    async fn bootstrap_typed_path_asap_telegraf_runtime_dispatch() {
        let _env = EnvVarGuard::set(physical::stage_split::ENV_USE_TYPED_STAGE_SPLIT, "1");

        let (_, app, tmp) = test_app_with_workload("rtt_tg", 0.01);
        let req = Request::builder()
            .uri("/api/v1/collector-config/agent")
            .header("X-Agent-Runtime", "asap-telegraf")
            .body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let toml = String::from_utf8(body.to_vec()).unwrap();
        std::fs::remove_file(&tmp).ok();

        // Telegraf fingerprint — see `config::runtime_tests::emit_for_runtime_telegraf_yields_toml`.
        assert!(
            toml.contains("[[inputs.opentelemetry]]"),
            "asap-telegraf runtime should produce Telegraf TOML:\n{toml}"
        );
    }

    /// `USE_TYPED_STAGE_SPLIT=1` but the registry is empty → typed
    /// path fails to resolve a workload and the handler falls back
    /// to the legacy `generate_agent_collector_config` emit. Bootstrap MUST
    /// NOT 500 just because the typed path hit a gap.
    #[tokio::test]
    async fn bootstrap_typed_path_falls_back_to_legacy_when_no_workload() {
        let _env = EnvVarGuard::set(physical::stage_split::ENV_USE_TYPED_STAGE_SPLIT, "1");

        let (_, app) = test_app(); // empty registry + empty stores
        let req = Request::builder()
            .uri("/api/v1/collector-config/agent")
            .body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let yaml = String::from_utf8(body.to_vec()).unwrap();

        // Legacy fingerprint — bare `ddsketch:` processor block.
        assert!(
            yaml.contains("ddsketch:"),
            "fallback path should emit legacy ddsketch processor:\n{yaml}"
        );
    }

    // ── MVP §46: planner ↔ 5-sketch emitter stitch (PR #339 ↔ PR #340) ─────────
    //
    // The acceptance contract: register the six contract metrics in the
    // workload registry, hit the bootstrap GET endpoint, and verify the
    // emitted YAML carries the 5-sketch routing-connector wire shape —
    // every sketched metric routed to its family-specific pipeline,
    // raw `http_requests_total` falling through to
    // `metrics/raw_passthrough`.
    //
    // Without the stitch wired in `emit_bootstrap_typed`, the
    // EdgeStageConfig.metric_to_family HashMap stays empty and the
    // emitter falls back to single-pipeline DDSketch — none of the
    // assertions below pass.

    /// Build an AppState whose workload registry carries all six MVP §46
    /// contract metrics, each pre-populated in the workload store with
    /// `aggregations=["quantile"]`. The planner classifies by metric
    /// name (`classify_demo_metric` wins over `aggregations[0]`) so the
    /// dummy aggregation is fine.
    ///
    /// Returns the (state, router, registry-tempfile-path) triple. The
    /// caller cleans up the tempfile.
    fn test_app_with_six_contract_metrics() -> (AppState, axum::Router, String) {
        // The 6 contract metrics from MVP §46.
        let metrics = [
            "http_requests_total",       // raw passthrough (no sketch)
            "http_latency_ms",           // DDSketch
            "request_size_bytes",        // KLL
            "unique_users_per_min",      // HLL
            "top_endpoint_qps",          // CountSketch
            "endpoint_request_freq",     // CountMinSketch
        ];

        // 1. Materialise a workload-registry YAML covering all six.
        let mut yaml = String::new();
        for m in metrics.iter() {
            yaml.push_str(&format!(
                "- metric_name: {m}\n  accuracy_sla: 0.01\n  assign_to_role: agent\n",
            ));
        }
        let tmp_path = "/tmp/datacollector_mvp46_six_metrics.yaml".to_string();
        std::fs::write(&tmp_path, yaml).unwrap();
        let registry = Arc::new(WorkloadRegistry::load(&tmp_path));

        // 2. Stock test_app with empty stores, then hand-populate.
        let (mut state, _router) = test_app();

        // 3. Pre-populate workload_store + plan_store the same way
        //    main()'s startup loop does.
        let analyzer = Analyzer::new();
        for m in metrics.iter() {
            let spec = pipeline::QuerySpec {
                query_string:    None,
                metric_name:     (*m).into(),
                label_filters:   Default::default(),
                group_by_labels: vec![],
                aggregations:    vec!["quantile".into()],
                time_window:     "5m".into(),
                repeat_every:    None,
                accuracy_sla:    0.01,
                latency_sla:     None,
                sketch_type:     None,
                workload:        types::WorkloadCharacteristics::default(),
                id:               None,
                language:         None,
                accuracy:         None,
                dollars:          None,
                deployment_model: None,
                shape:            types_v2::QueryShape::default(),
                data:             types_v2::DataShape::default(),
            };
            let wl = analyzer.analyze(spec).expect("analyze");
            let wc = types::WorkloadCharacteristics::default();
            let plan = state.planner.plan(&wl, Some(&wc));
            state.store.set(*m, control_plane::workload::AggRole::Quantile, plan);
            state.workload_store.set(*m, control_plane::workload::AggRole::Quantile, wl, wc);
        }

        // 4. Swap in the populated registry.
        state.workload_registry = registry;

        // 5. Rebuild router with updated state.
        let router = axum::Router::new()
            .route(
                "/api/v1/collector-config/agent",
                axum::routing::get(handle_bootstrap_agent_config),
            )
            .with_state(state.clone());
        (state, router, tmp_path)
    }

    /// Acceptance test: PR #339 (planner) ↔ PR #340 (emitter) stitch
    /// produces the 5-sketch routing-connector wire shape when the
    /// workload registry covers the six MVP §46 contract metrics.
    ///
    /// Asserts:
    ///   - All 5 sketch processors loaded under `processors:`.
    ///   - `routing` lives in `connectors:` (NOT `processors:`).
    ///   - All 6 named pipelines emitted (raw_passthrough + 5 sketches).
    ///   - Each metric routed to its expected pipeline via
    ///     `name == "..."`.
    #[tokio::test]
    async fn bootstrap_emits_5sketch_routing_for_six_contract_metrics() {
        let _env = EnvVarGuard::set(physical::stage_split::ENV_USE_TYPED_STAGE_SPLIT, "1");

        let (_, app, tmp) = test_app_with_six_contract_metrics();
        let req = Request::builder()
            .uri("/api/v1/collector-config/agent")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let yaml = String::from_utf8(body.to_vec()).unwrap();
        std::fs::remove_file(&tmp).ok();

        // ── Contract 1: all 5 sketch processors loaded ────────────────────
        for proc in [
            "ddsketch:",
            "KLL:",
            "HLL:",
            "countsketch:",
            "countmin:",
        ] {
            assert!(
                yaml.contains(proc),
                "missing top-level sketch processor `{proc}`\n{yaml}"
            );
        }

        // ── Contract 2: routing in connectors, not processors ─────────────
        let connectors_idx = yaml
            .find("connectors:")
            .expect("missing top-level connectors block");
        let after_conn = &yaml[connectors_idx..];
        assert!(
            after_conn.contains("routing:"),
            "missing `routing:` under connectors:\n{yaml}"
        );
        // Negative: routing is NOT under processors.
        let processors_idx = yaml.find("processors:").expect("processors:");
        let proc_end = yaml[processors_idx..]
            .find("\nconnectors:")
            .or_else(|| yaml[processors_idx..].find("\nexporters:"))
            .map(|x| processors_idx + x)
            .unwrap_or(yaml.len());
        let processors_section = &yaml[processors_idx..proc_end];
        assert!(
            !processors_section.contains("routing:"),
            "routing must NOT live under processors: (the v0.106 bug)\n\
             processors_section:\n{processors_section}"
        );

        // ── Contract 3: all 6 named pipelines ─────────────────────────────
        for pl in [
            "metrics:",                       // entry
            "metrics/raw_passthrough:",       // default for http_requests_total
            "metrics/ddsketch_path:",         // http_latency_ms
            "metrics/kll_path:",              // request_size_bytes
            "metrics/hll_path:",              // unique_users_per_min
            "metrics/countsketch_path:",      // top_endpoint_qps
            "metrics/countminsketch_path:",   // endpoint_request_freq
        ] {
            assert!(
                yaml.contains(pl),
                "missing pipeline `{pl}`\n{yaml}"
            );
        }

        // ── Contract 4: each sketched metric carries an OTTL condition ──
        // The 5 sketched metrics must each have a `name == "..."`
        // rule in the routing connector.
        // `http_requests_total` (raw) does NOT need a rule — it falls
        // through to the default `metrics/raw_passthrough` pipeline.
        for sketched in [
            "http_latency_ms",
            "request_size_bytes",
            "unique_users_per_min",
            "top_endpoint_qps",
            "endpoint_request_freq",
        ] {
            let needle = format!("name == \\\"{sketched}\\\"");
            let alt1 = format!("name == \"{sketched}\"");
            let alt2 = format!("name=='{sketched}'");
            assert!(
                yaml.contains(&needle) || yaml.contains(&alt1) || yaml.contains(&alt2),
                "missing routing rule for `{sketched}` — expected `name == \"{sketched}\"`\n{yaml}"
            );
        }
    }

    // ── Stitching-gap regression: live mvp-workload.yaml binds all 5 sketches ──
    //
    // Reproduces the live demo gap (3 of 6 contract metrics silently dropped
    // because `WorkloadEntry` didn't carry `sketch_family_override` and
    // `bind_workload_typed` early-returned on `exact_required` set by the
    // bare-VectorSelector → Sum path that PromQL parsing applies inside
    // `count(metric)` / `topk(K, metric)` / `rate(metric[5m])`).
    //
    // Loads workload entries shaped exactly like the deployed
    // `deploy/configs/mvp-workload.yaml` MVP §46 rows (entries 5–8), pre-pops
    // the workload store via the same code main() runs, and asserts the
    // routing table emitted by the bootstrap GET endpoint covers all five
    // sketched metrics.
    fn test_app_with_live_mvp_workload_metrics() -> (AppState, axum::Router, String) {
        // Mirror the YAML shape of `deploy/configs/mvp-workload.yaml` MVP §46
        // entries — these are the exact strings that crashed in the live demo.
        let yaml = r#"
- metric_name: http_requests_total_latency_ms
  query_string: "quantile_over_time(0.99, http_requests_total_latency_ms[1m])"
  accuracy_sla: 0.01
  assign_to_role: agent
- metric_name: http_requests_total
  query_string: "count(http_requests_total{service=\"payments\"})"
  accuracy_sla: 0.0
  assign_to_role: agent
- metric_name: request_size_bytes
  query_string: "quantile_over_time(0.99, request_size_bytes[1m])"
  accuracy_sla: 0.05
  assign_to_role: agent
  sketch_family_override: KLL
- metric_name: unique_users_per_min
  query_string: "count(unique_users_per_min)"
  accuracy_sla: 0.02
  assign_to_role: agent
  sketch_family_override: HLL
- metric_name: top_endpoint_qps
  query_string: "topk(5, top_endpoint_qps)"
  accuracy_sla: 0.05
  assign_to_role: agent
  sketch_family_override: CountSketch
- metric_name: endpoint_request_freq
  query_string: "rate(endpoint_request_freq[5m])"
  accuracy_sla: 0.05
  assign_to_role: agent
  sketch_family_override: CountMinSketch
"#;
        let tmp_path = "/tmp/datacollector_live_mvp46_workload.yaml".to_string();
        std::fs::write(&tmp_path, yaml).unwrap();
        let registry = Arc::new(WorkloadRegistry::load(&tmp_path));

        let (mut state, _router) = test_app();

        let analyzer = Analyzer::new();
        for entry in registry.entries() {
            let spec = pipeline::QuerySpec {
                query_string:    entry.query_string.clone(),
                metric_name:     entry.metric_name.clone(),
                label_filters:   Default::default(),
                group_by_labels: vec![],
                aggregations:    vec!["quantile".into()],
                time_window:     "5m".into(),
                repeat_every:    None,
                accuracy_sla:    entry.accuracy_sla,
                latency_sla:     None,
                sketch_type:     entry.sketch_family_override.clone(),
                workload:        types::WorkloadCharacteristics::default(),
                id:               None,
                language:         None,
                accuracy:         None,
                dollars:          None,
                deployment_model: None,
                shape:            types_v2::QueryShape::default(),
                data:             types_v2::DataShape::default(),
            };
            if let Ok(wl) = analyzer.analyze(spec) {
                let wc = types::WorkloadCharacteristics::default();
                let plan = state.planner.plan(&wl, Some(&wc));
                let metric_name = wl.metric_name.clone();
                let role = control_plane::workload::derive_agg_role(entry);
                state.store.set(&metric_name, role, plan);
                state.workload_store.set(&metric_name, role, wl, wc);
            }
        }

        state.workload_registry = registry;

        let router = axum::Router::new()
            .route(
                "/api/v1/collector-config/agent",
                axum::routing::get(handle_bootstrap_agent_config),
            )
            .with_state(state.clone());
        (state, router, tmp_path)
    }

    /// Pinning regression: the routing table emitted by the bootstrap
    /// endpoint must cover all 5 sketched contract metrics — DDSketch
    /// (`http_requests_total_latency_ms`), KLL (`request_size_bytes`),
    /// HLL (`unique_users_per_min`), CountSketch (`top_endpoint_qps`),
    /// CountMinSketch (`endpoint_request_freq`).
    ///
    /// Without the fix, this test fails with only 2 sketched routes
    /// (DDSketch + KLL); HLL / CountSketch / CountMinSketch silently drop.
    #[tokio::test]
    async fn bootstrap_routing_table_covers_all_five_sketches_for_live_mvp_yaml() {
        let _env = EnvVarGuard::set(physical::stage_split::ENV_USE_TYPED_STAGE_SPLIT, "1");

        let (state, app, tmp) = test_app_with_live_mvp_workload_metrics();

        // ── Direct check: collect_metric_to_family produces 5 entries ────
        let map = emit::collect_metric_to_family(
            &state.workload_registry,
            &state.workload_store,
        );
        assert_eq!(
            map.len(), 5,
            "metric_to_family should have 5 sketched entries (raw declines), got {map:?}",
        );
        for (metric, want_family) in &[
            ("http_requests_total_latency_ms", "DDSketch"),
            ("request_size_bytes",             "Kll"),
            ("unique_users_per_min",           "Hll"),
            ("top_endpoint_qps",               "CountSketch"),
            ("endpoint_request_freq",          "Cms"),
        ] {
            let got = map.get(*metric)
                .map(|k| format!("{k:?}"))
                .unwrap_or_else(|| "MISSING".into());
            assert_eq!(
                got, *want_family,
                "metric_to_family[{metric}] expected {want_family}, got {got}\nmap: {map:?}",
            );
        }

        // ── End-to-end check: routing rules in emitted YAML ───────────────
        let req = Request::builder()
            .uri("/api/v1/collector-config/agent")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let yaml = String::from_utf8(body.to_vec()).unwrap();
        std::fs::remove_file(&tmp).ok();

        for sketched in [
            "http_requests_total_latency_ms",
            "request_size_bytes",
            "unique_users_per_min",
            "top_endpoint_qps",
            "endpoint_request_freq",
        ] {
            let needle = format!("name == \\\"{sketched}\\\"");
            let alt1 = format!("name == \"{sketched}\"");
            let alt2 = format!("name=='{sketched}'");
            assert!(
                yaml.contains(&needle) || yaml.contains(&alt1) || yaml.contains(&alt2),
                "missing routing rule for `{sketched}`\n{yaml}"
            );
        }
    }

    // ── Regression: archive tier covers all 5 sketched metrics ────────────────
    //
    // The backend's `POST /api/v1/storage_routing` handler is an atomic
    // per-tenant SWAP — every push replaces the whole tenant's routing
    // table. Pre-fix, `handle_plan` posted a single-element
    // `metrics:[…]` document per call, so when the demo POSTed
    // `/api/v1/plan` for each of the 5 sketched contract metrics in
    // sequence, only the LAST metric's entry survived in the backend.
    // The other 4 metrics defaulted to `sketch_store` (which has
    // no ASAP-tier sketch state for archive-shape queries) → the
    // demo's accuracy reducer logged `archive_miss` for those metrics
    // even though gorillas3 wrote their TSDB blocks to MinIO and
    // Thanos had them indexed.
    //
    // The fix wires `state.backend_routing_cache` so each
    // `handle_plan` cycle posts the **cumulative** routing table.
    // This regression test replays the demo's per-metric POST sequence
    // against a mock backend, captures every body, and asserts the
    // final swap covers all 5 sketched metrics simultaneously.
    #[tokio::test]
    async fn storage_routing_cumulative_push_covers_all_5_sketched_metrics() {
        // Activate the typed-stage-split path (the only path that
        // emits storage-routing JSON; the legacy path no-ops).
        let _env = EnvVarGuard::set(physical::stage_split::ENV_USE_TYPED_STAGE_SPLIT, "1");

        // Mock backend that captures every storage-routing body.
        // We re-use the mock pattern from `backend_client::tests` —
        // an axum router that drains the request body into a shared
        // sink. Mounted at the canonical `/api/v1/storage_routing`
        // path so `BackendClient`'s URL-rewrite hits it directly.
        type SinkInner = std::sync::Mutex<Vec<String>>;
        let sink: Arc<SinkInner> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink_capture = Arc::clone(&sink);
        let mock_app = axum::Router::new()
            .route(
                "/api/v1/storage_routing",
                axum::routing::post(move |body: axum::body::Bytes| {
                    let sink = Arc::clone(&sink_capture);
                    async move {
                        let s = String::from_utf8_lossy(&body).to_string();
                        sink.lock().unwrap().push(s);
                        axum::http::StatusCode::OK
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, mock_app).await.unwrap();
        });
        // Brief settle so the bind is observable before the first POST.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Build an AppState with the backend pointed at the mock URL.
        // Use the streaming-config alias so `BackendClient` derives
        // the matching `/api/v1/storage_routing` URL.
        let backend_url = format!("http://{addr}/api/v1/streaming-config");
        let (state, _) = test_app_with_backend(Some(backend_url));

        // Mount only `/api/v1/plan` — that's the path the demo
        // exercises; we don't need bootstrap or other routes.
        let app = axum::Router::new()
            .route("/api/v1/plan", axum::routing::post(handle_plan))
            .with_state(state.clone());

        // The 5 sketched contract metrics from MVP §46. Each gets a
        // separate POST /api/v1/plan, mirroring the demo's
        // per-workload plan-emit cycle.
        let sketched = [
            "http_requests_total_latency_ms", // DDSketch
            "request_size_bytes",             // KLL
            "unique_users_per_min",           // HLL
            "top_endpoint_qps",               // CountSketch
            "endpoint_request_freq",          // CountMinSketch
        ];

        for m in &sketched {
            let app = app.clone();
            let req = Request::builder()
                .method("POST")
                .uri("/api/v1/plan")
                .header("content-type", "application/json")
                .body(Body::from(plan_spec(m).to_string()))
                .unwrap();
            let resp = app.oneshot(req).await.unwrap();
            assert_eq!(
                resp.status(),
                StatusCode::OK,
                "POST /api/v1/plan for `{m}` must return 200",
            );
        }

        // Drain the mock sink: every plan-emit must have produced
        // exactly one body (5 plans → 5 bodies).
        let bodies = sink.lock().unwrap().clone();
        assert_eq!(
            bodies.len(),
            sketched.len(),
            "expected one storage-routing POST per plan; got {} bodies",
            bodies.len(),
        );

        // The LAST captured body is the one the backend will leave
        // installed (the swap is destructive — last write wins). It
        // MUST list ALL 5 sketched metrics, otherwise the swap would
        // erase the routing entries for the metrics planned earlier
        // in the sequence and the backend would default them to
        // `sketch_store` → archive_miss for those metrics' archive
        // queries even though gorillas3's TSDB blocks are present in
        // MinIO and Thanos has them indexed.
        let last: serde_json::Value =
            serde_json::from_str(bodies.last().unwrap()).expect("last body is valid JSON");
        let metric_names: std::collections::BTreeSet<String> = last["metrics"]
            .as_array()
            .expect("metrics array")
            .iter()
            .map(|m| m["name"].as_str().unwrap().to_string())
            .collect();
        for m in &sketched {
            assert!(
                metric_names.contains(*m),
                "final cumulative storage-routing table missing metric `{m}`; \
                 contains only {metric_names:?}\nfull body: {}",
                bodies.last().unwrap(),
            );
        }

        // Each metric entry must carry a `thanos_query` target — the
        // archive-tier dispatch that lets backend forward archive-shape
        // queries to Thanos. Without this target the metric falls back
        // to `default_engine: sketch_store` and the archive miss
        // reproduces.
        for m in last["metrics"].as_array().unwrap() {
            let targets = m["targets"].as_array().expect("targets array");
            let engines: Vec<&str> = targets
                .iter()
                .map(|t| t["engine"].as_str().unwrap())
                .collect();
            assert!(
                engines.contains(&"thanos_query"),
                "metric `{}` missing `thanos_query` target; engines={engines:?}",
                m["name"].as_str().unwrap(),
            );
        }
    }

    // ── Regression: cumulative streaming-config across (metric, role) ─────────
    //
    // PR #283 made `WorkloadStore` and `PlanStore` (metric, role)-keyed,
    // so a single metric can carry MULTIPLE aggregation roles (e.g.
    // post-B2 `http_requests_total` has both a DDSketch-Quantile entry
    // from `quantile_over_time(...)` AND an ExactAgg-Sum entry from
    // `sum by (zone) (...)` in the workload store).
    //
    // Pre-this-fix the streaming-config emit path in `handle_plan` was
    // still metric-keyed and posted the CURRENT iteration's
    // `BackendStageConfig` alone. The data plane's
    // `POST /api/v1/streaming-config` handler is an atomic full
    // `handle.swap(new_config)`, so the second per-(metric, role) plan
    // POST destroyed the first one's aggregations on the backend and
    // `sum by (zone) (http_requests_total)` lands with
    // `ExactAgg(Sum) capability not satisfied`.
    //
    // This test replays the demo's per-metric plan-POST sequence
    // against a mock backend and captures every streaming-config body.
    // The LAST body (the one the data plane's swap installs) MUST
    // carry aggregations from EVERY prior plan POST, otherwise the
    // swap erases the earlier metrics' rows and the data plane can't
    // answer queries against them.
    //
    // The (metric, role) cache key is exercised in tandem by the live
    // mvp-workload.yaml pre-pop loop (the workload registry lists 3
    // entries for `http_requests_total`) → see the MVP smoke-test
    // pipeline. This in-process test exercises the cumulative-merge
    // plumbing in isolation against the same emit path used by both
    // the pre-pop loop and per-request replans.
    #[tokio::test]
    async fn streaming_config_cumulative_push_covers_all_planned_metrics() {
        // Activate the typed-stage-split path (the only path that emits
        // the typed streaming-config JSON; the legacy emit path no-ops).
        let _env = EnvVarGuard::set(physical::stage_split::ENV_USE_TYPED_STAGE_SPLIT, "1");

        // Mock backend that captures every streaming-config body. Same
        // pattern as the sibling `storage_routing_cumulative_push_...`
        // test — an axum router that drains the request body into a
        // shared sink. Mounted at the canonical
        // `/api/v1/streaming-config` path so `BackendClient`'s URL
        // forwarding hits it directly.
        type SinkInner = std::sync::Mutex<Vec<String>>;
        let sink: Arc<SinkInner> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink_capture = Arc::clone(&sink);
        let mock_app = axum::Router::new()
            .route(
                "/api/v1/streaming-config",
                axum::routing::post(move |body: axum::body::Bytes| {
                    let sink = Arc::clone(&sink_capture);
                    async move {
                        let s = String::from_utf8_lossy(&body).to_string();
                        sink.lock().unwrap().push(s);
                        axum::http::StatusCode::OK
                    }
                }),
            )
            // Sibling storage-routing endpoint stubbed so the
            // `handle_plan` cycle's second POST doesn't 404 and
            // pollute the test log (the assertion only inspects the
            // streaming-config sink).
            .route(
                "/api/v1/storage_routing",
                axum::routing::post(|| async { axum::http::StatusCode::OK }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, mock_app).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Build an AppState with the backend pointed at the mock URL.
        let backend_url = format!("http://{addr}/api/v1/streaming-config");
        let (state, _) = test_app_with_backend(Some(backend_url));
        let app = axum::Router::new()
            .route("/api/v1/plan", axum::routing::post(handle_plan))
            .with_state(state.clone());

        // The 5 sketched contract metrics from MVP §46 — the SAME
        // set the sibling `storage_routing_cumulative_push_...` test
        // exercises. Each gets a separate `POST /api/v1/plan` with
        // the metric-name → classified sketch family from
        // `classify_demo_metric`. Pre-fix the metric-only cache
        // would have collapsed sequential same-metric POSTs onto one
        // slot; this test uses 5 distinct metrics so the assertion
        // surfaces the cumulative-merge gap (every metric's row must
        // survive every other metric's swap).
        let sketched = [
            "http_requests_total_latency_ms",
            "request_size_bytes",
            "unique_users_per_min",
            "top_endpoint_qps",
            "endpoint_request_freq",
        ];

        for m in &sketched {
            let app = app.clone();
            let req = Request::builder()
                .method("POST")
                .uri("/api/v1/plan")
                .header("content-type", "application/json")
                .body(Body::from(plan_spec(m).to_string()))
                .unwrap();
            let resp = app.oneshot(req).await.unwrap();
            assert_eq!(
                resp.status(),
                StatusCode::OK,
                "POST /api/v1/plan for `{m}` must return 200",
            );
        }

        // Drain the mock sink: every plan-emit must have produced
        // exactly one streaming-config body (5 plans → 5 bodies).
        let bodies = sink.lock().unwrap().clone();
        assert_eq!(
            bodies.len(),
            sketched.len(),
            "expected one streaming-config POST per plan; got {} bodies",
            bodies.len(),
        );

        // The LAST body is the one the data plane's swap installs
        // (the swap is destructive — last write wins). It MUST list
        // aggregations for ALL 5 sketched metrics, otherwise the
        // swap erases the earlier metrics' rows and queries against
        // them fail with `…capability not satisfied` — the
        // streaming-config analogue of the storage-routing
        // `archive_miss` failure documented on the sibling test.
        let last: serde_json::Value =
            serde_json::from_str(bodies.last().unwrap()).expect("last body is valid JSON");
        let aggs = last["aggregations"]
            .as_array()
            .expect("aggregations array on cumulative streaming-config body");
        // Wire-format note: `build_backend_aggregation_json` writes
        // the field under key `metric` (NOT `metric_name`) — see
        // `emit/stage_config.rs::build_backend_aggregation_json`.
        let metric_names: std::collections::BTreeSet<String> = aggs
            .iter()
            .filter_map(|a| {
                a.get("metric")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .collect();
        for m in &sketched {
            assert!(
                metric_names.contains(*m),
                "final cumulative streaming-config missing aggregations \
                 for metric `{m}`; contains only {metric_names:?}\n\
                 full body: {}",
                bodies.last().unwrap(),
            );
        }
    }
}
