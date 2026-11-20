//! Materialized views persist across a restart: a data plane started over
//! the view files of an earlier one answers from the windows it stored,
//! without any sample written again; it never loads another plan's files
//! and never stores a window whose samples arrived before the restart.

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use control_plane::plan_workload;
use data_plane::engine::EngineOptions;
use data_plane::ingestion::IngestionOptions;
use data_plane::remote_write::{encode, Label, Sample, TimeSeries, WriteRequest};
use data_plane::{router, DataPlane};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;

const SUM: &str = "sum by (job) (sum_over_time(data[30s]))";

/// The plan of `query` every 10 s over `data` (two series), without raw
/// samples kept.
fn plan_json(query: &str) -> String {
    let workload = serde_json::from_value(json!({
        "query_workload": {"language": "promql", "repeating_queries": [{
            "query": query,
            "demand": {"fixed_interval_at": {"interval": 10000, "evaluation_phase": 0}},
            "requirements": {},
            "predictability": {"predictable": {"known_at": null}},
        }]},
        "data_workload": {"arrival": {"ingesting": {"rows_per_second": 2.0}},
          "sources": {"data": {"series_count": 2, "sample_interval": 1000,
            "metric_type": "gauge", "column_signs": {"value": "non_negative"}}}},
    }))
    .unwrap();
    serde_json::to_string(&plan_workload(&workload, &common::inputs(false)).unwrap().0).unwrap()
}

/// A fresh, empty directory for one test's view files.
fn view_dir(test: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("asap-views-{test}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn data_plane(plan: &str, view_dir: &Path) -> (Router, Arc<DataPlane>) {
    let options = EngineOptions {
        no_forward: true,
        view_dir: Some(view_dir.to_path_buf()),
        ingestion: IngestionOptions {
            allowed_lateness_ms: 0,
            stream_start_ms: Some(0),
            ..IngestionOptions::default()
        },
        ..EngineOptions::default()
    };
    let state = Arc::new(
        DataPlane::with_options(plan.into(), "http://127.0.0.1:1".into(), options).unwrap(),
    );
    (router(state.clone()), state)
}

/// Writes one sample per second in `(from_s, to_s]` for jobs `a` and `b`,
/// then closes every window through `to_s` with the barrier.
async fn write(app: &Router, metric: &str, from_s: i64, to_s: i64) {
    let timeseries = ["a", "b"]
        .iter()
        .map(|job| TimeSeries {
            labels: vec![
                Label {
                    name: "__name__".into(),
                    value: metric.into(),
                },
                Label {
                    name: "job".into(),
                    value: (*job).into(),
                },
            ],
            samples: (from_s + 1..=to_s)
                .map(|s| Sample {
                    timestamp: s * 1000,
                    value: (s % 97) as f64 + if *job == "a" { 0.5 } else { 3.25 },
                })
                .collect(),
        })
        .collect();
    let written = app
        .clone()
        .oneshot(
            Request::post("/api/v1/write")
                .body(Body::from(encode(&WriteRequest { timeseries })))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(written.status(), StatusCode::NO_CONTENT);
    let barrier = app
        .clone()
        .oneshot(
            Request::post(format!(
                "/api/v1/precompute/watermark?time_ms={}",
                to_s * 1000
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(barrier.status(), StatusCode::OK);
}

async fn query(app: &Router, query: &str, time_s: i64) -> (StatusCode, Value) {
    let uri = format!(
        "/api/v1/query?query={}&time={time_s}",
        form_urlencoded::byte_serialize(query.as_bytes()).collect::<String>()
    );
    let response = app
        .clone()
        .oneshot(Request::get(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}

/// The value of the metric line `name` in `state`'s `/metrics`.
fn metric(state: &DataPlane, name: &str) -> f64 {
    let text = state.engine.render_metrics();
    text.lines()
        .find_map(|line| line.strip_prefix(&format!("{name} ")))
        .unwrap_or_else(|| panic!("{name} missing"))
        .parse()
        .unwrap()
}

/// Answers `query` at 600 s, then again from a data plane restarted over
/// the same view files with no sample written; both answers must be equal.
async fn answers_alike_after_a_restart(test: &str, query_text: &str, plan: &str) {
    let dir = view_dir(test);
    let (app, state) = data_plane(plan, &dir);
    write(&app, "data", 0, 600).await;
    let (status, before) = query(&app, query_text, 600).await;
    assert_eq!(status, StatusCode::OK, "{before}");
    assert!(!before["data"]["result"].as_array().unwrap().is_empty());
    assert_eq!(metric(&state, "asap_view_windows_not_persisted_total"), 0.0);
    assert!(metric(&state, "asap_view_persisted_bytes_total") > 0.0);
    drop((app, state));

    let (app, state) = data_plane(plan, &dir);
    assert!(metric(&state, "asap_view_windows_loaded_total") > 0.0);
    let (status, after) = query(&app, query_text, 600).await;
    assert_eq!(status, StatusCode::OK, "{after}");
    assert_eq!(after, before, "{query_text}");
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Exact sums are loaded back and answer as before.
#[tokio::test(flavor = "multi_thread")]
async fn exact_views_answer_alike_after_a_restart() {
    answers_alike_after_a_restart("exact", SUM, &plan_json(SUM)).await;
}

/// DDSketch states are loaded back from their ASAPv1 bytes and answer as
/// before.
#[tokio::test(flavor = "multi_thread")]
async fn ddsketch_views_answer_alike_after_a_restart() {
    let query = "quantile_over_time(0.99, data[5m])";
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/workloads/sketch-plan-quantile-single.json");
    let workload = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let plan = plan_workload(&workload, &common::inputs(false)).unwrap().0;
    let plan = serde_json::to_string(&plan).unwrap();
    assert!(plan.contains("DDSketch"), "the plan builds no DDSketch");
    answers_alike_after_a_restart("ddsketch", query, &plan).await;
}

/// KLL states are loaded back from their ASAPv1 bytes and answer as
/// before.
#[tokio::test(flavor = "multi_thread")]
async fn kll_views_answer_alike_after_a_restart() {
    let query = "quantile_over_time(0.99, data[5m])";
    let workload = serde_json::from_value(json!({
        "query_workload": {"language": "promql", "repeating_queries": [{
            "query": query,
            "demand": {"fixed_interval_at": {"interval": 10000, "evaluation_phase": 0}},
            "requirements": {"accuracy": {"accuracy_metric": "rank", "max_error": 0.01,
                "failure_probability": 0.01, "accuracy_scope": "whole_result"}},
            "predictability": {"predictable": {"known_at": null}},
        }]},
        "data_workload": {"arrival": {"ingesting": {"rows_per_second": 1000.0}},
          "sources": {"data": {"series_count": 1000, "sample_interval": 1000}}},
    }))
    .unwrap();
    let plan = plan_workload(&workload, &common::inputs(false)).unwrap().0;
    let plan = serde_json::to_string(&plan).unwrap();
    assert!(plan.contains("Kll"), "the plan builds no KLL");
    answers_alike_after_a_restart("kll", query, &plan).await;
}

/// A data plane installing another plan over the same directory loads
/// none of the earlier plan's views and sets its files aside.
#[tokio::test(flavor = "multi_thread")]
async fn another_plans_view_files_are_not_loaded() {
    let dir = view_dir("plan-change");
    let (app, state) = data_plane(&plan_json(SUM), &dir);
    write(&app, "data", 0, 60).await;
    drop((app, state));

    let other = plan_json("sum by (job) (sum_over_time(data[20s]))");
    let (_app, state) = data_plane(&other, &dir);
    assert_eq!(metric(&state, "asap_view_windows_loaded_total"), 0.0);
    assert!(std::fs::read_dir(&dir).unwrap().any(|entry| entry
        .unwrap()
        .path()
        .to_string_lossy()
        .ends_with(".stale")));
    std::fs::remove_dir_all(&dir).unwrap();
}

/// After a restart, windows whose samples arrived before it are left
/// missing, not stored empty, even with a declared stream start: a query
/// reading them is not answered.
#[tokio::test(flavor = "multi_thread")]
async fn windows_before_a_restart_are_not_stored_empty() {
    let dir = view_dir("stream-start");
    let plan = plan_json(SUM);
    let (app, state) = data_plane(&plan, &dir);
    write(&app, "data", 0, 60).await;
    drop((app, state));

    let (app, _state) = data_plane(&plan, &dir);
    write(&app, "data", 120, 180).await;
    let (status, body) = query(&app, SUM, 100).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(
        body["error"].as_str().unwrap().contains("no rows tiling"),
        "{body}"
    );
    let (status, body) = query(&app, SUM, 180).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    std::fs::remove_dir_all(&dir).unwrap();
}
