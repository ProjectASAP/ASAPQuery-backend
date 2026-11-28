use std::path::Path;
use std::time::Duration;

use asap_planner::{InstallablePlan, PlanningWorkload};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use control_plane::{plan_workload, DeploymentInputs};
use data_plane::engine::EngineOptions;
use data_plane::ingestion::IngestionOptions;
use data_plane::remote_write::{encode, TimeSeries, WriteRequest};
use data_plane::{DataPlane, EXECUTION_HEADER};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

/// The deployment inputs of a backend that keeps raw data or not.
#[allow(dead_code)]
pub fn inputs(raw_data_retained: bool) -> DeploymentInputs {
    let mut inputs = DeploymentInputs::default();
    inputs.deployment_capabilities.raw_data_retained = raw_data_retained;
    inputs
}

/// [`inputs`] for a deployment without multi-level window chains: plans
/// keep one level per chain, for tests about refresh groups' timing.
#[allow(dead_code)]
pub fn inputs_without_levels(raw_data_retained: bool) -> DeploymentInputs {
    let mut inputs = inputs(raw_data_retained);
    inputs.deployment_capabilities.excluded_candidates = serde_json::from_value(
        serde_json::json!([{"operator": "SummaryMergeWindows"}, {"operator": "TimeRangeCover"}]),
    )
    .unwrap();
    inputs
}

/// A #509 example workload in `docs/examples/`.
#[allow(dead_code)]
pub fn workload(example: &str) -> PlanningWorkload {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
        "../../docs/examples/planner-layering-{example}.json"
    ));
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

/// The selected plan of a #509 example workload, for a deployment that
/// keeps raw data or not.
#[allow(dead_code)]
pub fn plan(example: &str, raw_data_retained: bool) -> InstallablePlan {
    plan_workload(&workload(example), &inputs(raw_data_retained))
        .unwrap()
        .0
}

#[allow(dead_code)]
pub fn plan_json(example: &str, raw_data_retained: bool) -> String {
    serde_json::to_string(&plan(example, raw_data_retained)).unwrap()
}

/// Posts `timeseries` as one Remote Write request; it must be accepted.
#[allow(dead_code)]
pub async fn remote_write(app: &Router, timeseries: Vec<TimeSeries>) {
    let response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/write")
                .body(Body::from(encode(&WriteRequest { timeseries })))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

/// The status of an event-time barrier at `time_ms`.
#[allow(dead_code)]
pub async fn barrier_status(app: Router, time_ms: i64) -> StatusCode {
    app.oneshot(
        Request::post(format!("/api/v1/precompute/watermark?time_ms={time_ms}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap()
    .status()
}

/// Sets the event-time barrier at `time_ms`; it must be answered, within
/// two minutes, with 200.
#[allow(dead_code)]
pub async fn barrier(app: &Router, time_ms: i64) {
    let status = tokio::time::timeout(
        Duration::from_secs(120),
        barrier_status(app.clone(), time_ms),
    )
    .await
    .unwrap_or_else(|_| panic!("the barrier at {time_ms} was not answered"));
    assert_eq!(status, StatusCode::OK);
}

/// GETs `uri`: the status, the `x-asap-execution` header and the JSON body
/// (`null` if it is not JSON).
#[allow(dead_code)]
pub async fn get(app: &Router, uri: &str) -> (StatusCode, Option<String>, Value) {
    let response = app
        .clone()
        .oneshot(Request::get(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let execution = response
        .headers()
        .get(EXECUTION_HEADER)
        .map(|v| v.to_str().unwrap().to_string());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        execution,
        serde_json::from_slice(&body).unwrap_or(Value::Null),
    )
}

/// The instant query `query` at `time_s` (Unix seconds).
#[allow(dead_code)]
pub fn instant_uri(query: &str, time_s: i64) -> String {
    format!(
        "/api/v1/query?query={}&time={time_s}",
        form_urlencoded::byte_serialize(query.as_bytes()).collect::<String>()
    )
}

/// The value of the first `/metrics` line starting with `prefix`.
#[allow(dead_code)]
pub fn metric(state: &DataPlane, prefix: &str) -> f64 {
    let text = state.engine.render_metrics();
    text.lines()
        .find(|l| l.starts_with(prefix))
        .unwrap_or_else(|| panic!("{prefix} missing from\n{text}"))
        .rsplit(' ')
        .next()
        .unwrap()
        .parse()
        .unwrap()
}

/// An instant query result's value per `job`, sorted by job.
#[allow(dead_code)]
pub fn per_job(body: &Value) -> Vec<(String, f64)> {
    let mut rows: Vec<(String, f64)> = body["data"]["result"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| {
            (
                s["metric"]["job"].as_str().unwrap().to_string(),
                s["value"][1].as_str().unwrap().parse().unwrap(),
            )
        })
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

/// Every workload file in `docs/examples/` and `tests/workloads/` whose name
/// contains `only` (every one for `""`), by name, sorted.
#[allow(dead_code)]
pub fn workloads(only: &str) -> Vec<(String, PlanningWorkload)> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files: Vec<_> = ["../../docs/examples", "tests/workloads"]
        .iter()
        .flat_map(|dir| std::fs::read_dir(root.join(dir)).unwrap())
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|e| e == "json"))
        .collect();
    files.sort();
    files
        .into_iter()
        .filter(|path| path.file_stem().unwrap().to_string_lossy().contains(only))
        .map(|path| {
            let name = path.file_stem().unwrap().to_string_lossy().to_string();
            let text = std::fs::read_to_string(&path).unwrap();
            (name, serde_json::from_str(&text).unwrap())
        })
        .collect()
}

/// The plan of one PromQL `query` evaluated every 10 s over the gauge
/// `data` (`series` series, one sample a second), for a deployment that
/// keeps no raw data. `extra` adds fields to the query, such as
/// `requirements` or `range_evaluation`.
#[allow(dead_code)]
pub fn one_query_plan_json(query: &str, series: usize, extra: Value) -> String {
    let mut entry = serde_json::json!({
        "query": query,
        "demand": {"fixed_interval_at": {"interval": 10000, "evaluation_phase": 0}},
        "requirements": {},
        "predictability": {"predictable": {"known_at": null}},
    });
    if let Value::Object(extra) = extra {
        entry.as_object_mut().unwrap().extend(extra);
    }
    let workload = serde_json::from_value(serde_json::json!({
        "query_workload": {"language": "promql", "repeating_queries": [entry]},
        "data_workload": {"arrival": {"ingesting": {"rows_per_second": series as f64}},
          "sources": {"data": {"series_count": series, "sample_interval": 1000,
            "metric_type": "gauge", "column_signs": {"value": "non_negative"}}}},
    }))
    .unwrap();
    serde_json::to_string(&plan_workload(&workload, &inputs(false)).unwrap().0).unwrap()
}

/// The engine options of a data plane that never forwards a query and
/// refreshes every window: no allowed lateness, and the producers' stream
/// starting at 0.
#[allow(dead_code)]
pub fn no_forward_options() -> EngineOptions {
    EngineOptions {
        no_forward: true,
        ingestion: IngestionOptions {
            allowed_lateness_ms: 0,
            stream_start_ms: Some(0),
            ..IngestionOptions::default()
        },
        ..EngineOptions::default()
    }
}
