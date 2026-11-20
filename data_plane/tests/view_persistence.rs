//! Materialized views persist across a restart: a data plane started over
//! the view files of an earlier one answers from the windows it stored,
//! without any sample written again.

mod common;

use std::path::Path;
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

const QUERY: &str = "sum by (job) (sum_over_time(data[30s]))";

/// The plan of `QUERY` every 10 s without raw samples kept: it reads exact
/// sums stored per 10 s window.
fn plan_json() -> String {
    let workload = serde_json::from_value(json!({
        "query_workload": {"language": "promql", "repeating_queries": [{
            "query": QUERY,
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

fn data_plane(view_dir: &Path) -> (Router, Arc<DataPlane>) {
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
        DataPlane::with_options(plan_json(), "http://127.0.0.1:1".into(), options).unwrap(),
    );
    (router(state.clone()), state)
}

async fn query(app: &Router, time_s: i64) -> (StatusCode, Value) {
    let uri = format!(
        "/api/v1/query?query={}&time={time_s}",
        form_urlencoded::byte_serialize(QUERY.as_bytes()).collect::<String>()
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

/// The windows refreshed before a restart are loaded from the view files
/// and answer the same query the same way; the files hold one record per
/// window and the metrics count what was written and loaded.
#[tokio::test(flavor = "multi_thread")]
async fn views_are_loaded_back_after_a_restart() {
    let view_dir = std::env::temp_dir().join(format!("asap-views-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&view_dir);

    let (app, state) = data_plane(&view_dir);
    let timeseries = ["a", "b"]
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
            samples: (1..=60)
                .map(|s| Sample {
                    timestamp: s * 1000,
                    value: s as f64,
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
    let (status, before) = query(&app, 60).await;
    assert_eq!(status, StatusCode::OK, "{before}");
    assert_eq!(before["data"]["result"].as_array().unwrap().len(), 2);
    let metrics = state.engine.render_metrics();
    assert!(
        metrics.contains("asap_view_windows_not_persisted_total 0"),
        "{metrics}"
    );
    assert!(
        !metrics.contains("asap_view_persisted_bytes_total 0\n"),
        "{metrics}"
    );
    drop((app, state));

    let (app, state) = data_plane(&view_dir);
    let (status, after) = query(&app, 60).await;
    assert_eq!(status, StatusCode::OK, "{after}");
    assert_eq!(after, before);
    assert!(state
        .engine
        .render_metrics()
        .lines()
        .any(|line| line.starts_with("asap_view_windows_loaded_total ") && !line.ends_with(" 0")));
    std::fs::remove_dir_all(&view_dir).unwrap();
}
