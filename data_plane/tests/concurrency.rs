//! Queries run beside ingestion: a query is answered from the views
//! already refreshed while the precompute thread is still refreshing newer
//! windows.

mod common;

use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use axum::Router;
use data_plane::remote_write::{Label, Sample, TimeSeries};
use data_plane::{router, DataPlane};
use serde_json::{json, Value};

const QUERY: &str = "sum(sum_over_time(data[10s]))";

/// The plan of `QUERY` every 10 s over one series sampled every second,
/// without raw samples kept: the query reads stored views only.
fn plan_json() -> String {
    common::one_query_plan_json(QUERY, 1, json!({}))
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
    common::remote_write(app, timeseries).await;
}

/// The status code and body of `QUERY` at `time_s`.
async fn query(app: &Router, time_s: i64) -> (StatusCode, Value) {
    let (status, _, body) = common::get(app, &common::instant_uri(QUERY, time_s)).await;
    (status, body)
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
    let options = common::no_forward_options();
    let state = Arc::new(
        DataPlane::with_options(plan_json(), "http://127.0.0.1:1".into(), options).unwrap(),
    );
    let app = router(state.clone());

    write(&app, 0, 20, 1.0).await;
    assert_eq!(
        common::barrier_status(app.clone(), 20_000).await,
        StatusCode::OK
    );
    let (status, body) = query(&app, 20).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(value(&body), 10.0);

    let held = state.engine.hold_precompute();
    write(&app, 20, 40, 2.0).await;
    let pending_barrier = tokio::spawn(common::barrier_status(app.clone(), 40_000));
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
