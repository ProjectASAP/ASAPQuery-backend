//! What the materialized views hold, as `asap_view_bytes` reports it: a
//! window's sketch states stay counted after a query has read them.

mod common;

use std::sync::Arc;

use axum::http::StatusCode;
use axum::Router;
use data_plane::engine::EngineOptions;
use data_plane::remote_write::{Label, Sample, TimeSeries};
use data_plane::{router, DataPlane};
use serde_json::{json, Value};

const QUANTILE: &str = "quantile_over_time(0.9, data[1m])";
const SERIES: usize = 50;

/// The plan of `QUANTILE` every 10 s over `SERIES` series of `data`,
/// without raw samples kept: per-series sketch windows.
fn plan_json() -> String {
    common::one_query_plan_json(
        QUANTILE,
        SERIES,
        json!({"requirements": {"accuracy": {"max_error": 0.05, "failure_probability": 0.01,
          "accuracy_scope": "whole_result"}}}),
    )
}

fn data_plane() -> (Router, Arc<DataPlane>) {
    let options = EngineOptions {
        view_dir: None,
        ..common::no_forward_options()
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
    common::remote_write(app, timeseries).await;
    common::barrier(app, to_s * 1000).await;
}

async fn query(app: &Router, time_s: i64) -> Value {
    let (status, _, body) = common::get(app, &common::instant_uri(QUANTILE, time_s)).await;
    assert_eq!(status, StatusCode::OK);
    body
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
