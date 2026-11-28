//! Materialized views persist across a restart: a data plane started over
//! the view files of an earlier one answers from the windows it stored,
//! without any sample written again; it never loads another plan's files
//! and never stores a window whose samples arrived before the restart.

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::http::StatusCode;
use axum::Router;
use control_plane::plan_workload;
use data_plane::engine::EngineOptions;
use data_plane::{router, DataPlane};
use prometheus_remote_write::{Label, Sample, TimeSeries};
use serde_json::{json, Value};

const SUM: &str = "sum by (job) (sum_over_time(data[30s]))";

/// The plan of `query` every 10 s over `data` (two series), without raw
/// samples kept.
fn plan_json(query: &str) -> String {
    common::one_query_plan_json(query, 2, json!({}))
}

/// A fresh, empty directory for one test's view files.
fn view_dir(test: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("asap-views-{test}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn data_plane(plan: &str, view_dir: &Path) -> (Router, Arc<DataPlane>) {
    let options = EngineOptions {
        view_dir: Some(view_dir.to_path_buf()),
        ..common::no_forward_options()
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
    common::remote_write(app, timeseries).await;
    common::barrier(app, to_s * 1000).await;
}

async fn query(app: &Router, query: &str, time_s: i64) -> (StatusCode, Value) {
    let (status, _, body) = common::get(app, &common::instant_uri(query, time_s)).await;
    (status, body)
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
    assert_eq!(
        common::metric(&state, "asap_view_windows_not_persisted_total"),
        0.0
    );
    // The barrier answered once the windows were written.
    assert_eq!(common::metric(&state, "asap_persist_backlog_windows"), 0.0);
    assert!(common::metric(&state, "asap_view_persisted_bytes_total") > 0.0);
    drop((app, state));

    let (app, state) = data_plane(plan, &dir);
    assert!(common::metric(&state, "asap_view_windows_loaded_total") > 0.0);
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
    assert!(plan.contains("\"KLL\""), "the plan builds no KLL");
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
    assert_eq!(
        common::metric(&state, "asap_view_windows_loaded_total"),
        0.0
    );
    // The earlier plan's segments are set aside where they are, in their
    // group's retention directory.
    assert!(std::fs::read_dir(dir.join("group-0"))
        .unwrap()
        .flat_map(|entry| std::fs::read_dir(entry.unwrap().path()).unwrap())
        .any(|entry| entry.unwrap().path().to_string_lossy().ends_with(".stale")));
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
    let (status, after_restart) = query(&app, SUM, 180).await;
    assert_eq!(status, StatusCode::OK, "{after_restart}");
    drop(app);

    // The windows written after the first restart extend the files' text
    // dictionary; a second restart reads them back alike.
    let (app, _state) = data_plane(&plan, &dir);
    let (status, body) = query(&app, SUM, 180).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body, after_restart);
    std::fs::remove_dir_all(&dir).unwrap();
}

/// A data plane over `dir` with explicit persist options.
fn data_plane_with(
    plan: &str,
    dir: &Path,
    persist_threads: usize,
    persist_delay_ms: u64,
) -> (Router, Arc<DataPlane>) {
    let options = EngineOptions {
        view_dir: Some(dir.to_path_buf()),
        persist_threads,
        persist_delay_ms,
        ..common::no_forward_options()
    };
    let state = Arc::new(
        DataPlane::with_options(plan.into(), "http://127.0.0.1:1".into(), options).unwrap(),
    );
    (router(state.clone()), state)
}

/// A slow disk does not slow refreshes: with every window's write delayed,
/// the windows are refreshed and answer at once while their writes wait in
/// the persist queue; stopping the engine still writes every queued window,
/// and a restart answers alike from them.
#[tokio::test(flavor = "multi_thread")]
async fn a_slow_disk_does_not_hold_refreshes_and_stopping_writes_everything() {
    let dir = view_dir("slow-disk");
    let plan = plan_json(SUM);
    let (app, state) = data_plane_with(&plan, &dir, 1, 400);
    // Samples through 61 s close the windows through 60 s (a window closes
    // once event time is past its end); no barrier, which would wait for
    // the writes.
    let timeseries = vec![TimeSeries {
        labels: vec![
            Label {
                name: "__name__".into(),
                value: "data".into(),
            },
            Label {
                name: "job".into(),
                value: "a".into(),
            },
        ],
        samples: (1..=61)
            .map(|s| Sample {
                timestamp: s * 1000,
                value: s as f64,
            })
            .collect(),
    }];
    common::remote_write(&app, timeseries).await;
    let started = std::time::Instant::now();
    while state.engine.stats().windows_refreshed < 6 {
        assert!(
            started.elapsed().as_secs() < 5,
            "refreshes waited for the disk"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    // Six windows' writes take 2.4 s; the refreshes did not wait for them.
    assert!(
        started.elapsed().as_millis() < 2_000,
        "{:?}",
        started.elapsed()
    );
    assert!(common::metric(&state, "asap_persist_backlog_windows") > 0.0);
    let (status, before) = query(&app, SUM, 60).await;
    assert_eq!(status, StatusCode::OK, "{before}");

    state.engine.stop();
    let started = std::time::Instant::now();
    while common::metric(&state, "asap_persist_backlog_windows") > 0.0 {
        assert!(
            started.elapsed().as_secs() < 20,
            "queued windows were not written"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    drop((app, state));
    let (app, _state) = data_plane_with(&plan, &dir, 1, 0);
    let (status, after) = query(&app, SUM, 60).await;
    assert_eq!(status, StatusCode::OK, "{after}");
    assert_eq!(after, before);
    std::fs::remove_dir_all(&dir).unwrap();
}

/// With several persist threads, each refresh group's file has one writer,
/// so its records stay in window order: every query of a plan with several
/// refresh groups answers alike after a restart.
#[tokio::test(flavor = "multi_thread")]
async fn several_persist_threads_keep_each_files_window_order() {
    let dir = view_dir("persist-threads");
    let path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/workloads/random-mixed-seed-862-01.json");
    let workload = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let plan = plan_workload(&workload, &common::inputs(false)).unwrap().0;
    assert!(plan.ingestion_refresh_groups().unwrap().len() > 1);
    let queries = plan.query_texts.clone();
    let plan = serde_json::to_string(&plan).unwrap();

    let (app, state) = data_plane_with(&plan, &dir, 3, 0);
    write(&app, "data", 0, 1200).await;
    let mut before = Vec::new();
    for query_text in &queries {
        before.push(query(&app, query_text, 1200).await);
    }
    assert!(before.iter().any(|(status, _)| *status == StatusCode::OK));
    assert!(common::metric(&state, "asap_view_persisted_bytes_total") > 0.0);
    drop((app, state));

    let (app, _state) = data_plane_with(&plan, &dir, 3, 0);
    for (query_text, before) in queries.iter().zip(&before) {
        assert_eq!(&query(&app, query_text, 1200).await, before, "{query_text}");
    }
    std::fs::remove_dir_all(&dir).unwrap();
}
