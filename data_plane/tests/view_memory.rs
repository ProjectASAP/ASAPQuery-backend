//! What the materialized views hold, as `asap_view_bytes` reports it: a
//! window's sketch states stay counted after a query has read them.

mod common;

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

const QUANTILE: &str = "quantile_over_time(0.9, data[1m])";
const SERIES: usize = 50;

/// The plan of `QUANTILE` every 10 s over `SERIES` series of `data`,
/// without raw samples kept: per-series sketch windows.
fn plan_json() -> String {
    let workload = serde_json::from_value(json!({
        "query_workload": {"language": "promql", "repeating_queries": [{
            "query": QUANTILE,
            "demand": {"fixed_interval_at": {"interval": 10000, "evaluation_phase": 0}},
            "requirements": {"accuracy": {"max_error": 0.05, "failure_probability": 0.01,
              "accuracy_scope": "whole_result"}},
            "predictability": {"predictable": {"known_at": null}},
        }]},
        "data_workload": {"arrival": {"ingesting": {"rows_per_second": SERIES as f64}},
          "sources": {"data": {"series_count": SERIES, "sample_interval": 1000,
            "metric_type": "gauge", "column_signs": {"value": "non_negative"}}}},
    }))
    .unwrap();
    serde_json::to_string(&plan_workload(&workload, &common::inputs(false)).unwrap().0).unwrap()
}

fn data_plane() -> (Router, Arc<DataPlane>) {
    let options = EngineOptions {
        no_forward: true,
        view_dir: None,
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
    (router(state.clone()), state)
}

/// One sample per second in `(0, to_s]` for every series, then a barrier
/// closing every window through `to_s`.
async fn write(app: &Router, to_s: i64) {
    let timeseries = (0..SERIES)
        .map(|series| TimeSeries {
            labels: vec![
                Label {
                    name: "__name__".into(),
                    value: "data".into(),
                },
                Label {
                    name: "instance".into(),
                    value: format!("host-{series:04}.example.internal:9100"),
                },
            ],
            samples: (1..=to_s)
                .map(|s| Sample {
                    timestamp: s * 1000,
                    value: ((s * 7 + series as i64) % 101) as f64,
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

async fn query(app: &Router, time_s: i64) -> Value {
    let uri = format!(
        "/api/v1/query?query={}&time={time_s}",
        form_urlencoded::byte_serialize(QUANTILE.as_bytes()).collect::<String>()
    );
    let response = app
        .clone()
        .oneshot(Request::get(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap()
}

fn view_bytes(state: &DataPlane) -> f64 {
    state
        .engine
        .render_metrics()
        .lines()
        .find_map(|line| line.strip_prefix("asap_view_bytes "))
        .expect("asap_view_bytes")
        .parse()
        .unwrap()
}

/// A query reading the windows does not make their states disappear from
/// `asap_view_bytes` (the executor marks states a run reads as held
/// outside the run, which `Batch::memory_bytes` then leaves out).
#[tokio::test(flavor = "multi_thread")]
async fn view_bytes_keep_counting_states_a_query_read() {
    let (app, state) = data_plane();
    write(&app, 120).await;
    let before = view_bytes(&state);
    let answer = query(&app, 120).await;
    assert_eq!(answer["data"]["result"].as_array().unwrap().len(), SERIES);
    assert_eq!(view_bytes(&state), before);
}
