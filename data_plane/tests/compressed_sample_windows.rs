//! Compressed-sample windows through the data plane: with raw data not
//! retained, an exact `quantile_over_time` and `max_over_time` are planned
//! as a compressed-samples chain read by `DecodeSamplesInRange`. The data
//! plane's ingestion runs build the chain's blocks with its sample-block
//! codec (`data_plane::sample_blocks`), query runs decode them, the view
//! files store them, and every answer equals the one computed from the
//! samples, also after a restart over the view files.

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

const MEDIAN: &str = "quantile_over_time(0.5, data[1m])";
const MAX: &str = "max_over_time(data[1m])";
const JOBS: [&str; 2] = ["a", "b"];

/// Both queries every 10 s over `data` (two series, 1 sample/s), exact
/// (no accuracy given), without raw samples kept.
fn plan() -> asap_planner::InstallablePlan {
    let query = |text: &str| {
        json!({
            "query": text,
            "demand": {"fixed_interval_at": {"interval": 10000, "evaluation_phase": 0}},
            "requirements": {},
            "predictability": {"predictable": {"known_at": null}},
        })
    };
    let workload = serde_json::from_value(json!({
        "query_workload": {"language": "promql", "repeating_queries": [query(MEDIAN), query(MAX)]},
        "data_workload": {"arrival": {"ingesting": {"rows_per_second": 2.0}},
          "sources": {"data": {"series_count": 2, "sample_interval": 1000,
            "metric_type": "gauge", "column_signs": {"value": "non_negative"}}}},
    }))
    .unwrap();
    plan_workload(&workload, &common::inputs(false)).unwrap().0
}

/// Job `job`'s sample at second `s`: a two-decimal gauge.
fn value(job: &str, s: i64) -> f64 {
    let offset = if job == "a" { 0.5 } else { 3.25 };
    ((s * 37) % 101) as f64 / 4.0 + offset
}

/// `query` at `time_s` per job, from the samples of `(time_s - 60, time_s]`.
fn expected(query: &str, time_s: i64) -> Vec<(String, f64)> {
    JOBS.iter()
        .map(|job| {
            let mut values: Vec<f64> = (time_s - 59..=time_s).map(|s| value(job, s)).collect();
            values.sort_by(f64::total_cmp);
            let answer = if query == MAX {
                *values.last().unwrap()
            } else {
                // PromQL's quantile: linear interpolation at rank q (n - 1).
                let rank = 0.5 * (values.len() - 1) as f64;
                let (low, high) = (rank.floor() as usize, rank.ceil() as usize);
                values[low] + (values[high] - values[low]) * (rank - low as f64)
            };
            (job.to_string(), answer)
        })
        .collect()
}

fn view_dir(test: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("asap-samples-{test}-{}", std::process::id()));
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

async fn post(app: &Router, uri: String, body: Vec<u8>) -> StatusCode {
    app.clone()
        .oneshot(Request::post(uri).body(Body::from(body)).unwrap())
        .await
        .unwrap()
        .status()
}

/// Writes one sample per second in `(0, to_s]` per job, then closes every
/// window through `to_s`.
async fn write(app: &Router, to_s: i64) {
    let timeseries = JOBS
        .iter()
        .map(|job| TimeSeries {
            labels: vec![
                Label {
                    name: "__name__".into(),
                    value: "data".into(),
                },
                Label {
                    name: "job".into(),
                    value: (*job).into(),
                },
            ],
            samples: (1..=to_s)
                .map(|s| Sample {
                    timestamp: s * 1000,
                    value: value(job, s),
                })
                .collect(),
        })
        .collect();
    let body = encode(&WriteRequest { timeseries });
    assert_eq!(
        post(app, "/api/v1/write".into(), body).await,
        StatusCode::NO_CONTENT
    );
    let barrier = format!("/api/v1/precompute/watermark?time_ms={}", to_s * 1000);
    assert_eq!(post(app, barrier, Vec::new()).await, StatusCode::OK);
}

async fn answer(app: &Router, query: &str, time_s: i64) -> Vec<(String, f64)> {
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
    let headers = format!("{:?}", response.headers());
    let body: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(
        status,
        StatusCode::OK,
        "{query} at {time_s}: {body} {headers}"
    );
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

/// The value of the metric line `name` in `state`'s `/metrics`.
fn metric(state: &DataPlane, name: &str) -> f64 {
    state
        .engine
        .render_metrics()
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{name} ")))
        .unwrap_or_else(|| panic!("{name} missing"))
        .parse()
        .unwrap()
}

/// The plan reads both queries through a compressed-samples chain at a
/// width that tiles their range (sealed reads); the data plane answers them
/// exactly at every 10 s step of the last minute, stores the chain's
/// windows in its view files (none left out for lack of an encoding), and
/// a data plane restarted over those files answers alike with no sample
/// written. A view file that cannot hold the blocks, or a decode that loses
/// or reorders a sample, fails it.
#[tokio::test(flavor = "multi_thread")]
async fn compressed_sample_windows_answer_exactly_and_survive_a_restart() {
    let plan = plan();
    let text = serde_json::to_string(&plan.dag).unwrap();
    assert!(text.contains("CompressedSamples"), "no compressed chain");
    assert!(text.contains("DecodeSamplesInRange"), "no decode");
    // The data plane serves sealed windows only (`backend_capabilities`).
    assert!(!text.contains(r#""reads_unsealed_window":true"#));
    let plan = serde_json::to_string(&plan).unwrap();

    let end_s = 600;
    let dir = view_dir("restart");
    let (app, state) = data_plane(&plan, &dir);
    write(&app, end_s).await;
    assert_eq!(state.engine.stats().windows_failed, 0);
    for query in [MEDIAN, MAX] {
        // Every 10 s step the views' retention covers.
        for time_s in (end_s - 60..=end_s).step_by(10) {
            assert_eq!(
                answer(&app, query, time_s).await,
                expected(query, time_s),
                "{query} at {time_s}"
            );
        }
    }
    assert_eq!(metric(&state, "asap_view_windows_not_persisted_total"), 0.0);
    assert!(metric(&state, "asap_view_persisted_bytes_total") > 0.0);
    drop((app, state));

    let (app, state) = data_plane(&plan, &dir);
    assert!(metric(&state, "asap_view_windows_loaded_total") > 0.0);
    for query in [MEDIAN, MAX] {
        assert_eq!(
            answer(&app, query, end_s).await,
            expected(query, end_s),
            "{query} after a restart"
        );
    }
    std::fs::remove_dir_all(&dir).unwrap();
}
