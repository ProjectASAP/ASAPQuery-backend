//! A planned range query is answered from the plan with one evaluation per
//! step: its answer at each step is the instant answer at that time.

mod common;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use control_plane::plan_workload;
use data_plane::engine::EngineOptions;
use data_plane::ingestion::IngestionOptions;
use data_plane::remote_write::{encode, Label, Sample, TimeSeries, WriteRequest};
use data_plane::{router, DataPlane, EXECUTION_HEADER};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;

const QUERY: &str = "sum by (job) (sum_over_time(data[10s]))";

/// The plan of `QUERY` every 10 s, each request a range over the last 30 s
/// at 10 s steps, without raw samples kept.
fn plan_json() -> String {
    let workload = serde_json::from_value(json!({
        "query_workload": {"language": "promql", "repeating_queries": [{
            "query": QUERY,
            "demand": {"fixed_interval_at": {"interval": 10000, "evaluation_phase": 0}},
            "requirements": {},
            "predictability": {"predictable": {"known_at": null}},
            "range_evaluation": {"step_ms": 10000, "span_ms": 30000},
        }]},
        "data_workload": {"arrival": {"ingesting": {"rows_per_second": 2.0}},
          "sources": {"data": {"series_count": 2, "sample_interval": 1000,
            "metric_type": "gauge", "column_signs": {"value": "non_negative"}}}},
    }))
    .unwrap();
    let plan = plan_workload(&workload, &common::inputs(false)).unwrap().0;
    serde_json::to_string(&plan).unwrap()
}

/// A data plane with two series of `data` sampled every second for 60 s,
/// every window closed.
async fn data_plane() -> Router {
    let options = EngineOptions {
        no_forward: true,
        ingestion: IngestionOptions {
            allowed_lateness_ms: 0,
            stream_start_ms: Some(0),
            ..IngestionOptions::default()
        },
        ..EngineOptions::default()
    };
    let state = Arc::new(
        DataPlane::with_options(plan_json(), "http://127.0.0.1:1".into(), options).unwrap(),
    );
    let app = router(state);
    let timeseries = ["a", "b"]
        .iter()
        .enumerate()
        .map(|(index, job)| TimeSeries {
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
            samples: (1..=60)
                .map(|s| Sample {
                    timestamp: s * 1000,
                    value: (s * (index as i64 + 1)) as f64,
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
            Request::post("/api/v1/precompute/watermark?time_ms=60000")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(barrier.status(), StatusCode::OK);
    app
}

async fn get(app: &Router, uri: String) -> (StatusCode, Option<String>, Value) {
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
    (status, execution, serde_json::from_slice(&body).unwrap())
}

fn encoded(query: &str) -> String {
    form_urlencoded::byte_serialize(query.as_bytes()).collect()
}

/// The range answer has one series per job, with a value at every step,
/// and each step's values are the instant answer at that time.
#[tokio::test(flavor = "multi_thread")]
async fn a_planned_range_query_answers_as_its_steps_do() {
    let app = data_plane().await;
    let (status, execution, body) = get(
        &app,
        format!(
            "/api/v1/query_range?query={}&start=30&end=60&step=10s",
            encoded(QUERY)
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(execution.as_deref(), Some("plan"));
    assert_eq!(body["data"]["resultType"], "matrix");
    let series = body["data"]["result"].as_array().unwrap();
    assert_eq!(series.len(), 2, "{body}");
    for time_s in [30, 40, 50, 60] {
        let (status, _, instant) = get(
            &app,
            format!("/api/v1/query?query={}&time={time_s}", encoded(QUERY)),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{instant}");
        let instant = instant["data"]["result"].as_array().unwrap();
        assert_eq!(instant.len(), 2);
        for sample in instant {
            let in_range = series
                .iter()
                .find(|s| s["metric"] == sample["metric"])
                .unwrap();
            let at_step = in_range["values"]
                .as_array()
                .unwrap()
                .iter()
                .find(|v| v[0] == json!(time_s as f64))
                .unwrap_or_else(|| panic!("no value at {time_s} in {in_range}"));
            assert_eq!(at_step[1], sample["value"][1], "at {time_s}");
        }
    }
}

/// With `--no-forward`, an unplanned range query is refused with the
/// reason, as an instant one is.
#[tokio::test]
async fn refuses_an_unplanned_range_query() {
    let app = data_plane().await;
    let (status, execution, body) = get(
        &app,
        "/api/v1/query_range?query=up&start=0&end=60&step=15".into(),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(execution.as_deref(), Some("refused"));
    assert!(
        body["error"].as_str().unwrap().contains("has no query"),
        "{body}"
    );
}

/// A range's steps are counted as range steps and its latency as a range
/// query's, not as instant queries.
#[tokio::test(flavor = "multi_thread")]
async fn range_steps_are_measured_apart_from_instant_queries() {
    let options = EngineOptions {
        no_forward: true,
        ingestion: IngestionOptions {
            allowed_lateness_ms: 0,
            stream_start_ms: Some(0),
            ..IngestionOptions::default()
        },
        ..EngineOptions::default()
    };
    let state = Arc::new(
        DataPlane::with_options(plan_json(), "http://127.0.0.1:1".into(), options).unwrap(),
    );
    let app = router(state.clone());
    let (status, _, body) = get(
        &app,
        format!(
            "/api/v1/query_range?query={}&start=30&end=60&step=10",
            encoded(QUERY)
        ),
    )
    .await;
    // Nothing written: every step is missing its windows, and the range
    // stops at the first one.
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    let metrics = state.engine.render_metrics();
    let line = |prefix: &str| {
        metrics
            .lines()
            .find(|l| l.starts_with(prefix))
            .unwrap_or_else(|| panic!("{prefix} missing from\n{metrics}"))
            .to_string()
    };
    let steps: u64 = line("asap_phase_runs_total{phase=\"query_range_step\"")
        .rsplit(' ')
        .next()
        .unwrap()
        .parse()
        .unwrap();
    assert!((1..=4).contains(&steps), "{steps} steps run");
    assert!(line("asap_phase_runs_total{phase=\"query\",").ends_with(" 0"));
    assert!(line("asap_query_latency_seconds_count{").ends_with(" 0"));
}
