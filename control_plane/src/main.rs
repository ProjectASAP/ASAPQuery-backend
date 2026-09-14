#![allow(clippy::doc_lazy_continuation, clippy::type_complexity)]

use control_plane::backend_client;
use control_plane::clickhouse;
use control_plane::metrics_exposer;
use control_plane::opamp;
use control_plane::physical;
use control_plane::runtime_samples;
use control_plane::types;
use control_plane::workload;

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::info;

use opamp::OpampServer;
use physical::backend_stage::BackendStageConfig;
use physical::deployment_cost::online as online_cost_model;
use physical::deployment_cost::online::{init_store as init_online_store, OnlineMetricsStore};
use physical::deployment_cost::tco;
use workload::AggRole;

// ── Shared state ──────────────────────────────────────────────────────────────

#[derive(Clone)]
struct AppState {
    opamp: Arc<OpampServer>,
    online_store: OnlineMetricsStore,
    opamp_endpoint: String,
    /// Bounded ring buffer for runtime-sample push batches from
    /// agents' `sketch-runtime::PushExporter`. Read by decision
    /// loops in the replanner.
    runtime_samples: Arc<runtime_samples::RuntimeSamplesStore>,
    /// The successfully activated typed catalog is authoritative for live ERP
    /// input identity; incoming telemetry cannot supply its own descriptors.
    active_summary_catalog:
        Arc<tokio::sync::Mutex<Option<Arc<asap_types::summary_catalog::SummaryCatalog>>>>,
    /// Shared client for posting streaming configs from HTTP planning and replanning.
    /// `None` when `CONTROLLER_BACKEND_ENDPOINT` is unset; pushes are then skipped.
    backend_client: Option<Arc<backend_client::BackendClient>>,
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let api_addr = std::env::var("CONTROLLER_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".into());
    let opamp_addr =
        std::env::var("CONTROLLER_OPAMP_ADDR").unwrap_or_else(|_| "0.0.0.0:4320".into());
    // The default must match the compose service name `controller`; the crate
    // name `control_plane` is not a resolvable hostname in the canonical stack.
    let opamp_ep = std::env::var("CONTROLLER_OPAMP_ENDPOINT")
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

    // ── OpAMP server ──────────────────────────────────────────────────────────
    // Collector plans are published on demand by the physical-plan path; the
    // server itself carries no plan-push hooks.
    let opamp_srv: Arc<OpampServer> = Arc::new(OpampServer::new());

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

    let backend_routing_cache: Arc<Mutex<HashMap<(String, AggRole), BackendStageConfig>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let runtime_samples_store = runtime_samples::RuntimeSamplesStore::new(1024);
    let state = AppState {
        opamp: Arc::clone(&opamp_srv),
        online_store: Arc::clone(&online_store),
        opamp_endpoint: opamp_ep,
        runtime_samples: Arc::clone(&runtime_samples_store),
        active_summary_catalog: Arc::new(tokio::sync::Mutex::new(None)),
        backend_client: backend_client_shared,
    };

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
    let grpc_addr = std::env::var("CONTROLLER_GRPC_ADDR").unwrap_or_else(|_| "0.0.0.0:4321".into());
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
    };
    let metrics_router = Router::new()
        .route(
            "/metrics",
            axum::routing::get(metrics_exposer::handle_metrics),
        )
        .with_state(metrics_state);

    let app = Router::new()
        .route(
            "/api/v1/physical-plan/cost-manifests",
            post(handle_workload_cost_manifests),
        )
        .route(
            "/api/v1/metricsql/physical-plan/cost-manifests",
            post(handle_metricsql_workload_cost_manifests),
        )
        .route(
            "/api/v1/physical-plan/compile-and-publish",
            post(handle_compile_and_publish_physical_plan),
        )
        .route(
            "/api/v1/metricsql/physical-plan/compile-and-publish",
            post(handle_compile_and_publish_metricsql_physical_plan),
        )
        .route(
            "/api/v1/clickhouse-plan/compile-and-publish",
            post(handle_compile_and_publish_clickhouse_plan),
        )
        .route(
            "/api/v1/clickhouse-plan/automatic/compile-and-publish",
            post(handle_compile_and_publish_automatic_clickhouse_plan),
        )
        .route("/api/v1/cost-model", get(handle_cost_model))
        .route("/api/v1/tco", post(handle_tco))
        .with_state(state)
        .merge(metrics_router);

    let listener = tokio::net::TcpListener::bind(&api_addr).await.unwrap();
    info!("control plane API listening on {api_addr}");
    axum::serve(listener, app).await.unwrap();
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PhysicalPlanQueryRequest {
    query_id: String,
    query_string: String,
    metric: String,
    window_secs: u64,
    #[serde(default)]
    group_by: Vec<String>,
    accuracy: types::AccuracyTarget,
    lifecycle: physical::compiler::SummaryLifecyclePlanningInputs,
    window_cost_model: physical::compiler::WindowCostModel,
    evaluation_phase_ms: u64,
    #[serde(default)]
    runtime_policy: physical::compiler::RuntimeRulePolicy,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CompileAndPublishPhysicalPlanRequest {
    /// Opt-in additive response for quote preparation; the default remains a manifest array.
    #[serde(default)]
    explain: bool,
    #[serde(default = "default_physical_deployment_target")]
    target: physical::compiler::PhysicalDeploymentTarget,
    #[serde(default)]
    workload_cost_evidence: Option<physical::workload_cost::WorkloadCostEvidence>,
    queries: Vec<PhysicalPlanQueryRequest>,
    #[serde(rename = "collector_ids", alias = "target_collector_ids")]
    target_collector_ids: Vec<String>,
    capability_snapshot_id: String,
    #[serde(default)]
    evidence: HashMap<String, physical::compiler::TopKMembershipEvidence>,
    #[serde(default)]
    exact_composition_costs:
        HashMap<String, Vec<physical::post_asap::cost_model::ExactCompositionCostEvidence>>,
    #[serde(default)]
    erp: Option<physical::erp::ErpPlanningInput>,
    #[serde(default)]
    runtime_adaptation_evidence: Vec<physical::compiler::RuntimeAdaptationEvidence>,
    planner_revision: String,
    max_evidence_age_ms: u64,
    plan_version: u64,
    activation_unix_ms: u64,
    expiry_unix_ms: Option<u64>,
    backend_compat: String,
    #[serde(default = "default_physical_plan_timeout_ms")]
    apply_timeout_ms: u64,
}

fn default_physical_deployment_target() -> physical::compiler::PhysicalDeploymentTarget {
    physical::compiler::PhysicalDeploymentTarget::DistributedCollectors
}

fn default_physical_plan_timeout_ms() -> u64 {
    10_000
}

use physical::compiler::QueryFrontend;

#[derive(Debug, Serialize)]
struct CompileAndPublishPhysicalPlanResponse {
    cost_comparison: Option<physical::workload_cost::CandidatePlanSelectionReport>,
    #[serde(rename = "logical_selection", alias = "planner_selection_trace")]
    planner_selection_trace: Vec<serde_json::Value>,
    plan_id: u64,
    plan_version: u64,
    status: &'static str,
    generated_at_unix_ms: u64,
    #[serde(rename = "collector_ids", alias = "target_collector_ids")]
    target_collector_ids: Vec<String>,
    lifecycle_estimates: Vec<physical::compiler::MaterializationLifecycleEstimate>,
}

/// Compile one Planner IR decision into one catalog-backed physical plan and
/// install it atomically before publishing collector projections.
async fn handle_compile_and_publish_physical_plan(
    State(st): State<AppState>,
    Json(request): Json<CompileAndPublishPhysicalPlanRequest>,
) -> Response {
    compile_and_publish_physical_plan(st, request, QueryFrontend::PromQl).await
}

async fn handle_compile_and_publish_metricsql_physical_plan(
    State(st): State<AppState>,
    Json(request): Json<CompileAndPublishPhysicalPlanRequest>,
) -> Response {
    compile_and_publish_physical_plan(st, request, QueryFrontend::MetricsQl).await
}

async fn compile_and_publish_physical_plan(
    st: AppState,
    mut request: CompileAndPublishPhysicalPlanRequest,
    frontend: QueryFrontend,
) -> Response {
    // Serialize typed activations so an older response cannot overwrite the
    // catalog recorded after a newer backend activation.
    let mut active_catalog = st.active_summary_catalog.lock().await;
    if let Some(erp) = &mut request.erp {
        if let Err(error) = erp.hydrate_observed_shape(&st.runtime_samples) {
            return (StatusCode::UNPROCESSABLE_ENTITY, error).into_response();
        }
        let catalog = active_catalog.clone();
        erp.resolve_population_data_descriptor(catalog.as_deref());
    }
    let (bundle, target_collector_ids, apply_timeout, adaptation_evidence, _) =
        match compile_physical_plan_request(request, false, frontend) {
            Ok((Some(bundle), ids, timeout, adaptation, manifests)) => {
                (bundle, ids, timeout, adaptation, manifests)
            }
            Ok((None, ..)) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "publication requires a selected plan",
                )
                    .into_response()
            }
            Err(response) => return physical_compile_failure(response),
        };

    let Some(backend) = st.backend_client.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "CONTROLLER_BACKEND_ENDPOINT is required for physical-plan publication".to_string(),
        )
            .into_response();
    };
    if let Err(error) = st
        .opamp
        .ensure_collector_plan_targets(&bundle.collector_plans, apply_timeout)
        .await
    {
        return (
            StatusCode::BAD_GATEWAY,
            format!("collector physical-plan preflight failed: {error}"),
        )
            .into_response();
    }
    let publication = match bundle.to_publication_artifact() {
        Ok(publication) => publication,
        Err(error) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("invalid catalog publication: {error}"),
            )
                .into_response()
        }
    };
    if let Err(error) = backend
        .post_catalog_plan_typed(
            &publication,
            Some(bundle.storage_routing.clone()),
            &adaptation_evidence,
        )
        .await
    {
        return (
            StatusCode::BAD_GATEWAY,
            format!("backend rejected physical plan: {error}"),
        )
            .into_response();
    }
    if let Err(error) = st
        .opamp
        .publish_collector_plans(&bundle.collector_plans, apply_timeout)
        .await
    {
        let cleanup = backend
            .discard_staged_physical_plan(bundle.envelope.plan_id, bundle.envelope.plan_version)
            .await;
        return (
            StatusCode::BAD_GATEWAY,
            format!("collector physical-plan publication failed: {error}; staged backend cleanup: {cleanup:?}"),
        )
            .into_response();
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let activation_wait = bundle.envelope.activation_unix_ms.saturating_sub(now);
    if activation_wait > apply_timeout.as_millis() as u64 {
        return (
            StatusCode::GATEWAY_TIMEOUT,
            "activation time exceeds apply_timeout_ms; backend remains staged".to_string(),
        )
            .into_response();
    }
    if activation_wait > 0 {
        tokio::time::sleep(Duration::from_millis(activation_wait)).await;
    }
    if let Err(error) = backend
        .activate_physical_plan(bundle.envelope.plan_id, bundle.envelope.plan_version)
        .await
    {
        return (
            StatusCode::BAD_GATEWAY,
            format!("backend physical-plan activation failed: {error}"),
        )
            .into_response();
    }

    *active_catalog = Some(Arc::new(bundle.summary_catalog));

    Json(CompileAndPublishPhysicalPlanResponse {
        cost_comparison: bundle.cost_comparison,
        planner_selection_trace: bundle.planner_selection_trace,
        plan_id: bundle.envelope.plan_id,
        plan_version: bundle.envelope.plan_version,
        status: "active",
        generated_at_unix_ms: bundle.envelope.generated_at_unix_ms,
        target_collector_ids,
        lifecycle_estimates: bundle.lifecycle_estimates,
    })
    .into_response()
}

async fn handle_compile_and_publish_clickhouse_plan(
    State(state): State<AppState>,
    Json(request): Json<clickhouse::ClickHouseSqlWorkload>,
) -> impl IntoResponse {
    let publication = match clickhouse::compile_clickhouse_workload(&request).await {
        Ok(publication) => publication,
        Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    };
    publish_clickhouse_plan(&state, publication, None).await
}

async fn handle_compile_and_publish_automatic_clickhouse_plan(
    State(state): State<AppState>,
    Json(request): Json<clickhouse::ClickHouseSqlAutomaticWorkload>,
) -> impl IntoResponse {
    let (publication, trace) =
        match clickhouse::compile_automatic_clickhouse_workload(&request).await {
            Ok(publication) => publication,
            Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
        };
    publish_clickhouse_plan(&state, publication, Some(serde_json::json!(trace))).await
}

async fn publish_clickhouse_plan(
    state: &AppState,
    publication: physical::publication::PhysicalPlanPublication,
    selection_trace: Option<serde_json::Value>,
) -> axum::response::Response {
    let mut active_catalog = state.active_summary_catalog.lock().await;
    let plan_id = publication.summary_catalog.plan_id;
    let plan_version = publication.summary_catalog.plan_version;
    let Some(client) = state.backend_client.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "backend publication is not configured",
        )
            .into_response();
    };
    if let Err(error) = client
        .post_catalog_plan_typed(&publication, None, &[])
        .await
    {
        return (StatusCode::BAD_GATEWAY, error.to_string()).into_response();
    }
    if let Err(error) = client.activate_physical_plan(plan_id, plan_version).await {
        let _ = client
            .discard_staged_physical_plan(plan_id, plan_version)
            .await;
        return (StatusCode::BAD_GATEWAY, error.to_string()).into_response();
    }
    *active_catalog = Some(Arc::new(publication.summary_catalog));
    Json(serde_json::json!({
        "plan_id": plan_id,
        "plan_version": plan_version,
        "status": "active",
        "selection_trace": selection_trace,
    }))
    .into_response()
}

// Keep Planner's Rc-backed rewrite DAG outside the async handler's future.
// Only the Send-safe compiled bundle crosses an await point.
fn compile_physical_plan_request(
    request: CompileAndPublishPhysicalPlanRequest,
    manifests_only: bool,
    frontend: QueryFrontend,
) -> Result<
    (
        Option<physical::compiler::CompiledPhysicalPlan>,
        Vec<String>,
        Duration,
        Vec<physical::compiler::RuntimeAdaptationEvidence>,
        (
            Vec<physical::workload_cost::WorkloadCostManifest>,
            Vec<physical::workload_cost::CandidatePlanEvaluation>,
            Vec<serde_json::Value>,
        ),
    ),
    (StatusCode, serde_json::Value),
> {
    if request.queries.is_empty()
        || (request.target == physical::compiler::PhysicalDeploymentTarget::DistributedCollectors
            && request.target_collector_ids.is_empty())
        || (request.target == physical::compiler::PhysicalDeploymentTarget::BackendLocalRemoteWrite
            && !request.target_collector_ids.is_empty())
    {
        return Err((
            StatusCode::UNPROCESSABLE_ENTITY,
            "queries must be non-empty; distributed deployment requires collectors and backend-local deployment requires none".to_string().into(),
        ));
    }
    if request.max_evidence_age_ms == 0 || request.apply_timeout_ms == 0 {
        return Err((
            StatusCode::UNPROCESSABLE_ENTITY,
            "max_evidence_age_ms and apply_timeout_ms must be non-zero"
                .to_string()
                .into(),
        ));
    }
    if request.plan_version == 0
        || request.activation_unix_ms == 0
        || request.backend_compat != control_plane::physical::compiler::BACKEND_COMPAT
        || request
            .expiry_unix_ms
            .is_some_and(|expiry| expiry <= request.activation_unix_ms)
    {
        return Err((
            StatusCode::UNPROCESSABLE_ENTITY,
            "plan_version, activation_unix_ms, backend_compat and expiry are invalid"
                .to_string()
                .into(),
        ));
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let mut queries = Vec::with_capacity(request.queries.len());
    let mut canonical_roots = Vec::with_capacity(request.queries.len());
    let mut window_models = Vec::new();
    let mut workload_entries = Vec::new();
    for query in request.queries {
        if query.query_id.trim().is_empty()
            || query.metric.trim().is_empty()
            || query.window_secs == 0
        {
            return Err((
                StatusCode::UNPROCESSABLE_ENTITY,
                "query_id, metric, and window_secs must be non-empty/non-zero"
                    .to_string()
                    .into(),
            ));
        }
        let expr = match frontend.parse(&query.query_string, query.accuracy.clone()) {
            Ok(expr) => expr,
            Err(error) => return Err((StatusCode::UNPROCESSABLE_ENTITY, error.to_string().into())),
        };
        let post_asap = match control_plane::planner_selection::keep_pre_asap(&expr) {
            Ok(plan) => plan,
            Err(error) => return Err((StatusCode::UNPROCESSABLE_ENTITY, error.to_string().into())),
        };
        canonical_roots.push(std::rc::Rc::new(expr));
        window_models.push(query.window_cost_model);
        workload_entries.push(planner_types::workload::RepeatingEntry {
            query: planner_types::workload::Query(query.query_string.clone()),
            demand: planner_types::workload::RepeatedDemand::FixedIntervalAt {
                interval: planner_types::workload::RepetitionInterval(
                    query.lifecycle.evaluation_interval_ms,
                ),
                evaluation_phase: planner_types::workload::TimestampMs(query.evaluation_phase_ms),
            },
            requirements: planner_types::workload::QueryRequirements {
                accuracy: planner_types::workload::AccuracyRequirement::Explicit(
                    query.accuracy.clone(),
                ),
                ..Default::default()
            },
            predictability: planner_types::workload::Predictability::Predictable { known_at: None },
            time_selection: planner_types::workload::TimeSelection {
                lookback: Some(planner_types::workload::DurationMs(
                    query.window_secs.saturating_mul(1_000),
                )),
                ..Default::default()
            },
        });
        queries.push(physical::compiler::QueryCompilationInput {
            query_id: query.query_id,
            query_string: query.query_string,
            selected_plan_root: post_asap,
            legacy_query_source: planner_types::pre_asap::Source::TimeSeries {
                metric: query.metric,
            },
            query_lookback_seconds: query.window_secs,
            group_by_labels: query.group_by,
            accuracy_target: query.accuracy,
            summary_lifecycle_inputs: query.lifecycle,
            window_realization_candidates: Vec::new(),
            materialization_runtime_policy: query.runtime_policy,
        });
    }

    let planner_selection_trace = match physical::compiler::select_logical_roots_with_trace(
        &mut queries,
        canonical_roots,
        &request.evidence,
        &request.exact_composition_costs,
        request.erp.as_ref(),
    ) {
        Ok(trace) => trace,
        Err(error) => return Err((StatusCode::UNPROCESSABLE_ENTITY, error.to_string().into())),
    };

    for (query, model) in queries.iter_mut().zip(window_models) {
        physical::compiler::prepare_window_implementations(query, &model, request.target, 0)
            .map_err(|error| (StatusCode::UNPROCESSABLE_ENTITY, error.to_string().into()))?;
    }
    let compilation_request = physical::compiler::PhysicalCompilationRequest {
        planner_selection_trace,
        query_workload: Some(planner_types::workload::QueryWorkload {
            language: planner_types::workload::QueryLanguage::PromQL,
            query_batch: None,
            repeating_queries: Some(workload_entries),
            data_workload: None,
        }),
        queries,
        allow_mixed_summary_and_exact_execution: request.target
            == physical::compiler::PhysicalDeploymentTarget::BackendLocalRemoteWrite,
        enabled_materialization_keys: None,
        topk_membership_evidence_by_query_id: request.evidence,
        exact_composition_costs: request.exact_composition_costs,
        erp: request.erp,
        planner_revision: request.planner_revision,
        source_sample_interval_ms: None,
        query_retention_margin_ms: 0,
        retained_summary_memory_budget_bytes: None,
    };
    let environment = physical::compiler::PhysicalDeploymentContext {
        target: request.target,
        target_collector_ids: request.target_collector_ids.clone(),
        capability_snapshot_id: request.capability_snapshot_id,
        observed_at_unix_ms: now,
        max_evidence_age_ms: request.max_evidence_age_ms,
        plan_version: request.plan_version,
        activation_unix_ms: request.activation_unix_ms,
        expiry_unix_ms: request.expiry_unix_ms,
        backend_compat: request.backend_compat,
    };
    let candidates = physical::workload_cost::enumerate_exact_and_materialized_candidates(
        compilation_request.clone(),
    )
    .map_err(|error| (StatusCode::UNPROCESSABLE_ENTITY, error.to_string().into()))?;
    let planner_selection_trace = compilation_request.planner_selection_trace.clone();
    let (manifests, alternatives) = physical::workload_cost::compile_candidates_for_pricing(
        candidates.clone(),
        environment.clone(),
        frontend,
    );
    let apply_timeout = Duration::from_millis(request.apply_timeout_ms);
    // Quote preparation enumerates feasible bindings; it does not select the
    // default warm candidate, which may be unavailable while exact is valid.
    if manifests_only {
        if manifests.is_empty() {
            return Err((
                StatusCode::UNPROCESSABLE_ENTITY,
                serde_json::json!({"status": "all_infeasible", "alternatives": alternatives,
                    "logical_selection": compilation_request.planner_selection_trace}),
            ));
        }
        return Ok((
            None,
            request.target_collector_ids,
            apply_timeout,
            request.runtime_adaptation_evidence,
            (manifests, alternatives, planner_selection_trace),
        ));
    }
    let compiled = match request.workload_cost_evidence {
        Some(evidence) => match frontend {
            QueryFrontend::PromQl => physical::workload_cost::select_lowest_cost_candidate(
                candidates,
                environment,
                &evidence,
            ),
            QueryFrontend::MetricsQl => {
                physical::workload_cost::select_lowest_cost_metricsql_candidate(
                    candidates,
                    environment,
                    &evidence,
                )
            }
        },
        None => frontend.compile(compilation_request, environment),
    };
    let bundle = match compiled {
        Ok(bundle) => bundle,
        Err(physical::compiler::CompileError::Alternatives(report)) => {
            return Err((StatusCode::UNPROCESSABLE_ENTITY, report))
        }
        Err(error) => return Err((StatusCode::UNPROCESSABLE_ENTITY, error.to_string().into())),
    };
    Ok((
        Some(bundle),
        request.target_collector_ids,
        apply_timeout,
        request.runtime_adaptation_evidence,
        (manifests, alternatives, planner_selection_trace),
    ))
}

/// Read-only preparation: no OpAMP, staging, activation or data-plane writes.
async fn handle_workload_cost_manifests(
    Json(request): Json<CompileAndPublishPhysicalPlanRequest>,
) -> impl IntoResponse {
    workload_cost_manifests(request, QueryFrontend::PromQl)
}

async fn handle_metricsql_workload_cost_manifests(
    Json(request): Json<CompileAndPublishPhysicalPlanRequest>,
) -> impl IntoResponse {
    workload_cost_manifests(request, QueryFrontend::MetricsQl)
}

fn physical_compile_failure((status, report): (StatusCode, serde_json::Value)) -> Response {
    match report {
        serde_json::Value::String(message) => (status, message).into_response(),
        report => (status, Json(report)).into_response(),
    }
}

fn workload_cost_manifests(
    request: CompileAndPublishPhysicalPlanRequest,
    frontend: QueryFrontend,
) -> Response {
    if request.workload_cost_evidence.is_some() {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            "omit quotes when requesting manifests",
        )
            .into_response();
    }
    let explain = request.explain;
    match compile_physical_plan_request(request, true, frontend) {
        Ok((_, _, _, _, (manifests, alternatives, planner_selection_trace))) => {
            if explain {
                Json(serde_json::json!({"manifests": manifests, "alternatives": alternatives, "logical_selection": planner_selection_trace}))
                    .into_response()
            } else {
                Json(manifests).into_response()
            }
        }
        Err(error) => physical_compile_failure(error),
    }
}

// ── Handlers ──────────────────────────────────────────────────────────────────

/// Returns the current EMA cost model state — blended benchmark + observed costs
/// per sketch type.  Useful for diagnosing whether the online cost model has
/// received sufficient observations to meaningfully influence plan selection.
async fn handle_cost_model(State(st): State<AppState>) -> impl IntoResponse {
    let table = online_cost_model::effective_table(&st.online_store);
    let raw = st.online_store.try_read();

    let entries: Vec<serde_json::Value> = table
        .iter()
        .map(|(sketch_type, costs)| {
            let observations = raw
                .as_ref()
                .ok()
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
        })
        .collect();

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

// ── Test helpers ──────────────────────────────────────────────────────────────

/// Builds a minimal `AppState` + `Router` for integration tests.
/// No background tasks are started; OpAMP/scraper hold no real connections.
#[cfg(test)]
fn test_app() -> (AppState, axum::Router) {
    test_app_with_backend(None)
}

/// Build a test server with an optional mock backend URL. `None` disables
/// backend pushes; `Some(url)` exercises typed backend JSON delivery.
#[cfg(test)]
fn test_app_with_backend(backend_url: Option<String>) -> (AppState, axum::Router) {
    let online_store = init_online_store();
    let plan_store = Arc::new(PlanStore::new());
    let workload_store = Arc::new(WorkloadStore::new());
    let opamp = Arc::new(OpampServer::new());
    let scraper = Arc::new(Scraper::new(
        vec![],
        Thresholds::default(),
        Arc::new(|_| {}),
        Duration::from_secs(60),
    ));
    let planner = Arc::new(CachedDeploymentPlanner::new(
        DeploymentCostPlanner::new().with_online_store(Arc::clone(&online_store)),
    ));
    let replanner = Arc::new(Replanner::new(
        Arc::clone(&planner),
        Arc::clone(&plan_store),
        Arc::clone(&workload_store),
        Arc::clone(&opamp),
        Arc::clone(&scraper),
        "ws://ctrl:4320/v1/opamp",
    ));
    let backend_client = backend_url.map(|u| Arc::new(backend_client::BackendClient::new(u)));
    let state = AppState {
        opamp,
        replanner,
        online_store,
        opamp_endpoint: "ws://ctrl:4320/v1/opamp".into(),
        runtime_samples: runtime_samples::RuntimeSamplesStore::new(64),
        active_summary_catalog: Arc::new(tokio::sync::Mutex::new(None)),
        backend_client,
    };
    let router = axum::Router::new()
        .route("/api/v1/cost-model", axum::routing::get(handle_cost_model))
        .route("/api/v1/tco", axum::routing::post(handle_tco))
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

    // A missing warm implementation must not hide the executable exact quote.
    #[tokio::test]
    async fn cost_manifests_survive_unavailable_warm_candidate() {
        let snapshot: physical::compiler::BackendLocalPlanningInput = serde_json::from_str(
            include_str!("../../docs/examples/asapquery-planning-snapshot.json"),
        )
        .unwrap();
        let (planning, _) = snapshot
            .clone()
            .into_physical_compilation_request()
            .unwrap();
        let query = &planning.queries[0];
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let mut request_body = serde_json::json!({
            "queries": [{
                "query_id": query.query_id, "query_string": query.query_string,
                "metric": "m", "window_secs": 60, "accuracy": query.accuracy_target,
                "lifecycle": query.summary_lifecycle_inputs, "evaluation_phase_ms": 0, "window_cost_model": snapshot.physical_inputs.window_cost_model
            }],
            "collector_ids": ["test"], "capability_snapshot_id": "test",
            "planner_revision": physical::compiler::PLANNER_REVISION,
            "max_evidence_age_ms": 60000, "plan_version": 1,
            "activation_unix_ms": now, "backend_compat": control_plane::physical::compiler::BACKEND_COMPAT
        });
        for explain in [false, true] {
            request_body["explain"] = serde_json::json!(explain);
            let request = serde_json::from_value(request_body.clone()).unwrap();
            let response = handle_workload_cost_manifests(Json(request))
                .await
                .into_response();
            assert_eq!(response.status(), StatusCode::OK);
            let manifests = body_json(response).await;
            if explain {
                assert_eq!(manifests["manifests"].as_array().unwrap().len(), 1);
                let alternatives = manifests["alternatives"].as_array().unwrap();
                assert_eq!(alternatives.len(), 2);
                assert_eq!(alternatives[0]["status"], "bind_failed");
                assert!(alternatives[0]["unavailable_reason"].is_string());
                assert_eq!(alternatives[1]["status"], "bound");
                assert!(alternatives[1]["physical_alternative_id"].is_string());
                assert!(!manifests["logical_selection"]
                    .as_array()
                    .unwrap()
                    .is_empty());
            } else {
                assert_eq!(manifests.as_array().unwrap().len(), 1);
            }
        }
        request_body["planner_revision"] = serde_json::json!("unavailable-compiler");
        let response =
            handle_workload_cost_manifests(Json(serde_json::from_value(request_body).unwrap()))
                .await
                .into_response();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(response.headers()["content-type"], "application/json");
        let report = body_json(response).await;
        assert_eq!(report["status"], "all_infeasible");
        assert_eq!(report["alternatives"].as_array().unwrap().len(), 2);
        assert!(report["alternatives"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item["status"] == "bind_failed"));
    }

    #[test]
    fn backend_local_typed_request_compiles_without_collectors() {
        let snapshot: physical::compiler::BackendLocalPlanningInput = serde_json::from_str(
            include_str!("../../docs/examples/asapquery-compatibility-demo-snapshot.json"),
        )
        .unwrap();
        let (planning, _) = snapshot
            .clone()
            .into_physical_compilation_request()
            .unwrap();
        let mut query = planning.queries[0].clone();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        query.summary_lifecycle_inputs.evidence_observed_at_unix_ms = now;
        for implementation in &mut query.window_realization_candidates {
            implementation.cost.observed_at_unix_ms = now;
        }
        let planner_types::pre_asap::Source::TimeSeries { metric } = &query.legacy_query_source
        else {
            panic!("expected time series fixture");
        };
        let value = serde_json::json!({
            "target": "backend_local_remote_write",
            "queries": [{
                "query_id": query.query_id, "query_string": query.query_string,
                "metric": metric, "window_secs": query.query_lookback_seconds, "accuracy": query.accuracy_target,
                "lifecycle": query.summary_lifecycle_inputs, "evaluation_phase_ms": 0, "window_cost_model": { "implementation_id": "test", "cost": query.window_realization_candidates[0].cost }
            }],
            "collector_ids": [], "capability_snapshot_id": "test",
            "planner_revision": physical::compiler::PLANNER_REVISION,
            "max_evidence_age_ms": 60000, "plan_version": 1,
            "activation_unix_ms": now, "backend_compat": physical::compiler::BACKEND_COMPAT
        });
        let request = serde_json::from_value(value.clone()).unwrap();
        let (plan, collectors, _, _, _) =
            compile_physical_plan_request(request, false, QueryFrontend::PromQl).unwrap();
        let plan = plan.unwrap();
        assert!(collectors.is_empty());
        assert!(plan.collector_plans.is_empty());
        assert_eq!(
            plan.precompute_plan.ingest.protocol,
            physical::compiler::IngestProtocol::PrometheusRemoteWriteV1
        );
        assert!(!plan.precompute_plan.materializations.is_empty());
        let mut distributed = value;
        distributed["target"] = serde_json::json!("distributed_collectors");
        assert!(compile_physical_plan_request(
            serde_json::from_value(distributed).unwrap(),
            false,
            QueryFrontend::PromQl
        )
        .is_err());
    }

    async fn body_json(resp: axum::response::Response) -> serde_json::Value {
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    // ── AppState.backend_client wiring ───────────────────────────────

    /// Default-constructed AppState (no `CONTROLLER_BACKEND_ENDPOINT`)
    /// must leave `backend_client` as `None` so the typed L5 backend
    /// JSON push silently no-ops, matching the Phase B fire-and-forget
    /// contract.
    #[test]
    fn app_state_backend_client_none_by_default() {
        let (state, _router) = test_app();
        assert!(
            state.backend_client.is_none(),
            "backend_client should default to None when no endpoint is configured"
        );
    }

    /// When constructed with a backend URL (the production path takes
    /// it from `CONTROLLER_BACKEND_ENDPOINT`), the field is populated
    /// and ready for the Phase C `handle_plan` push.
    #[test]
    fn app_state_backend_client_some_when_constructed_with_url() {
        let (state, _router) =
            test_app_with_backend(Some("http://127.0.0.1:1/api/v1/streaming-config".into()));
        let bc = state.backend_client.expect("backend_client must be Some");
        assert_eq!(bc.endpoint(), "http://127.0.0.1:1/api/v1/streaming-config");
    }

    // ── POST /api/v1/plan ─────────────────────────────────────────────────────

    // ── GET /api/v1/plan/:metric ──────────────────────────────────────────────

    #[tokio::test]
    async fn get_plan_not_found_returns_404() {
        let (_, app) = test_app();
        let req = Request::builder()
            .uri("/api/v1/plan/nonexistent")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // ── GET /api/v1/cost-model ────────────────────────────────────────────────

    #[tokio::test]
    async fn cost_model_returns_all_sketch_types() {
        let (_, app) = test_app();
        let req = Request::builder()
            .uri("/api/v1/cost-model")
            .body(Body::empty())
            .unwrap();
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
            .method("POST")
            .uri("/api/v1/tco")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
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
    ) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>
    {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let url = format!("ws://{opamp_addr}/v1/opamp");
        let mut req = url.into_client_request().unwrap();
        req.headers_mut()
            .insert("X-Agent-ID", agent_id.parse().unwrap());
        req.headers_mut()
            .insert("X-Agent-Role", role.parse().unwrap());
        let (ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
        ws
    }

    /// Read the next binary WebSocket frame, decode as OpAMP ServerToAgent,
    /// and extract the YAML config body.
    async fn recv_config_yaml(
        ws: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    ) -> String {
        use tokio_tungstenite::tungstenite::Message;
        let msg = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            futures_util::StreamExt::next(ws),
        )
        .await
        .expect("timeout waiting for config push")
        .expect("stream ended")
        .expect("ws error");
        match msg {
            Message::Binary(data) => {
                let payload = if !data.is_empty() && data[0] == 0 {
                    &data[1..]
                } else {
                    data.as_slice()
                };
                let sta =
                    <crate::opamp::opamp_proto::ServerToAgent as prost::Message>::decode(payload)
                        .expect("decode ServerToAgent");
                let rc = sta.remote_config.expect("remote_config present");
                let cm = rc.config.expect("config present");
                let file = cm.config_map.get("").expect("empty-key config file");
                String::from_utf8(file.body.clone()).expect("yaml is utf8")
            }
            other => panic!("expected binary frame, got {other:?}"),
        }
    }

    /// Test 2: Re-plan pushes config only to agents registered for that metric.
    #[tokio::test]
    async fn replan_pushes_only_to_registered_agent() {
        let online_store = init_online_store();
        let plan_store = Arc::new(PlanStore::new());
        let workload_store = Arc::new(WorkloadStore::new());
        let opamp_srv = Arc::new(OpampServer::new());
        let scraper = Arc::new(Scraper::new(
            vec![],
            Thresholds::default(),
            Arc::new(|_| {}),
            Duration::from_secs(60),
        ));
        let planner = Arc::new(CachedDeploymentPlanner::new(
            DeploymentCostPlanner::new().with_online_store(Arc::clone(&online_store)),
        ));

        // Seed workload + plan for "metric_a".
        let analyzer = Analyzer::new();
        let spec = pipeline::QuerySpec {
            query_string: None,
            metric_name: "metric_a".into(),
            label_filters: Default::default(),
            group_by_labels: vec![],
            aggregations: vec!["quantile".into()],
            time_window: "5m".into(),
            repeat_every: None,
            accuracy_sla: 0.01,
            latency_sla: None,
            sketch_type: None,
            workload: types::WorkloadCharacteristics::default(),
            id: None,
            language: None,
            accuracy: None,
            dollars: None,
            deployment_model: None,
            shape: types::QueryShape::default(),
            data: types::DataShape::default(),
        };
        let wl = analyzer.analyze(spec).unwrap();

        let plan = planner.plan(&wl);
        plan_store.set("metric_a", control_plane::workload::AggRole::Quantile, plan);
        workload_store.set("metric_a", control_plane::workload::AggRole::Quantile, wl);

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
            .register_agent(
                "agent-a",
                "metric_a",
                control_plane::workload::AggRole::Quantile,
            )
            .await;
        replanner
            .register_agent(
                "agent-b",
                "metric_b",
                control_plane::workload::AggRole::Quantile,
            )
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
        )
        .await;
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
            output_mode: types::OutputMode::Sketch,
            sketch_type: types::SketchType::DDSketch,
            sketch_params: types::SketchParams::default(),
            aggregate_by: vec![],
            label_matchers: vec![],
            window_duration: Some(Duration::from_secs(60)),
            mode: types::ProcessorMode::Window,
            enable_self_monitoring: true,
            transmit_sketch: true,
            drop_original: true,
            delta_transmission: false,
            delta_threshold: 0.0,
            gos: None,
            enable_series_id: false,
            series_id_ttl_secs: 300,
            // This test asserts on `doc["exporters"]["prometheus"]`
            // (line ~1326). Keep the test semantics by pinning the
            // sink to the legacy prometheus exporter.
            data_sink: types::AgentDataSink::PrometheusScrape {
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
        assert_eq!(ws_endpoint, endpoint, "OpAMP endpoint mismatch");

        // 2. service.extensions list includes "opamp".
        let svc_exts = doc["service"]["extensions"].as_sequence().unwrap();
        let has_opamp = svc_exts.iter().any(|v| v.as_str() == Some("opamp"));
        assert!(
            has_opamp,
            "service.extensions should include 'opamp':\n{yaml}"
        );

        // 3. The YAML is complete: has receivers, processors, exporters, service.pipelines.
        assert!(
            doc["receivers"]["otlp"].is_mapping(),
            "missing receivers.otlp"
        );
        assert!(
            doc["exporters"]["prometheus"].is_mapping(),
            "missing exporters.prometheus"
        );
        let pipeline = &doc["service"]["pipelines"]["metrics"];
        assert!(
            pipeline["receivers"].is_sequence(),
            "missing pipeline receivers"
        );
        assert!(
            pipeline["processors"].is_sequence(),
            "missing pipeline processors"
        );
        assert!(
            pipeline["exporters"].is_sequence(),
            "missing pipeline exporters"
        );
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
            .method("POST")
            .uri("/api/v1/tco")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        // With higher Grafana pricing, before cost should be higher.
        assert!(body["before"]["ingestion_dollars"].as_f64().unwrap() > 0.0);
        assert!(body["monthly_savings_dollars"].as_f64().unwrap() > 0.0);
    }

    // Bootstrap and plan-push must use the same typed emission pipeline.

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
            Self {
                key,
                previous,
                _lock: lock,
            }
        }
        fn unset(key: &'static str) -> Self {
            let lock = ENV_GUARD.lock().unwrap_or_else(|p| p.into_inner());
            let previous = std::env::var(key).ok();
            std::env::remove_var(key);
            Self {
                key,
                previous,
                _lock: lock,
            }
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
}
