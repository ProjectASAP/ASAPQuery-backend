//! Queries run beside ingestion: a query is answered from the views
//! already refreshed while the precompute thread is still refreshing newer
//! windows.

mod common;

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use control_plane::plan_workload;
use data_plane::engine::EngineOptions;
use data_plane::ingestion::IngestionOptions;
use data_plane::remote_write::{encode, Label, Sample, TimeSeries, WriteRequest};
use data_plane::{router, DataPlane};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

const QUERY: &str = "sum(sum_over_time(data[10s]))";

/// The plan of `QUERY` every 10 s over one series sampled every second,
/// without raw samples kept: the query reads stored views only.
fn plan_json() -> String {
    let workload = serde_json::from_str(&format!(
        r#"{{"query_workload": {{"language": "promql", "repeating_queries": [
              {{"query": {QUERY:?},
                "demand": {{"fixed_interval_at": {{"interval": 10000, "evaluation_phase": 0}}}},
                "requirements": {{}},
                "predictability": {{"predictable": {{"known_at": null}}}}}}]}},
            "data_workload": {{"arrival": {{"ingesting": {{"rows_per_second": 1.0}}}},
              "sources": {{"data": {{"series_count": 1, "sample_interval": 1000,
                "metric_type": "gauge", "column_signs": {{"value": "non_negative"}}}}}}}}}}"#
    ))
    .unwrap();
    let plan = plan_workload(&workload, &common::inputs(false)).unwrap().0;
    serde_json::to_string(&plan).unwrap()
}

/// Writes one sample of `value` every second in `(from_s, to_s]`.
async fn write(app: &Router, from_s: i64, to_s: i64, value: f64) {
    let samples = (from_s + 1..=to_s)
        .map(|s| Sample {
            timestamp: s * 1000,
            value,
        })
        .collect();
    let timeseries = vec![TimeSeries {
        labels: vec![Label {
            name: "__name__".into(),
            value: "data".into(),
        }],
        samples,
    }];
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

async fn barrier(app: Router, time_ms: i64) -> StatusCode {
    app.oneshot(
        Request::post(format!("/api/v1/precompute/watermark?time_ms={time_ms}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap()
    .status()
}

/// The status code and body of `QUERY` at `time_s`.
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

fn value(body: &Value) -> f64 {
    body["data"]["result"][0]["value"][1]
        .as_str()
        .unwrap()
        .parse()
        .unwrap()
}

/// While the precompute thread cannot finish a refresh, a query is
/// answered at once from the windows refreshed before, the newer windows
/// wait in the precompute backlog, and the barrier behind them waits too;
/// once the refresh finishes, the newer windows answer.
#[tokio::test(flavor = "multi_thread")]
// The test holds the precompute gate across awaits on purpose.
#[allow(clippy::await_holding_lock)]
async fn queries_are_answered_while_a_refresh_runs() {
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

    write(&app, 0, 20, 1.0).await;
    assert_eq!(barrier(app.clone(), 20_000).await, StatusCode::OK);
    let (status, body) = query(&app, 20).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(value(&body), 10.0);

    let held = state.engine.hold_precompute();
    write(&app, 20, 40, 2.0).await;
    let pending_barrier = tokio::spawn(barrier(app.clone(), 40_000));
    for _ in 0..500 {
        if state.engine.precompute_backlog() > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(state.engine.precompute_backlog() > 0);

    // Answered from the views refreshed before, without waiting.
    let (status, body) = tokio::time::timeout(Duration::from_secs(10), query(&app, 20))
        .await
        .expect("a query waited for the refresh");
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(value(&body), 10.0);
    // The newer window is not refreshed yet.
    let (status, _) = query(&app, 40).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(!pending_barrier.is_finished());

    drop(held);
    assert_eq!(pending_barrier.await.unwrap(), StatusCode::OK);
    assert_eq!(state.engine.precompute_backlog(), 0);
    let (status, body) = query(&app, 40).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(value(&body), 20.0);
}
