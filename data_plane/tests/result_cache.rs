//! The result cache keeps a planned query's final answer at each
//! evaluation time: a repeated range request runs only the steps it has
//! not answered finally before, an instant query at a step's time reads
//! that step's answer, and an answer computed before its inputs were
//! complete is never served later.

mod common;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use data_plane::engine::EngineOptions;
use data_plane::ingestion::IngestionOptions;
use data_plane::remote_write::{encode, Label, Sample, TimeSeries, WriteRequest};
use data_plane::{router, DataPlane};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;

/// Answered from materialized views only (no raw samples kept).
const VIEWS_QUERY: &str = "sum by (job) (sum_over_time(data[10s]))";

/// The plan of `VIEWS_QUERY` every 10 s, requested as ranges of 30 s.
fn views_plan_json() -> String {
    let workload = serde_json::from_value(json!({
        "query_workload": {"language": "promql", "repeating_queries": [{
            "query": VIEWS_QUERY,
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
    let plan = control_plane::plan_workload(&workload, &common::inputs(false))
        .unwrap()
        .0;
    serde_json::to_string(&plan).unwrap()
}

/// #509 Example 1 with raw data kept: its queries read raw samples at
/// query time.
const RAW_QUERY: &str = "sum by (job) (rate(http_requests_total[1m]))";

fn data_plane(plan_json: String, result_cache: bool) -> (Router, Arc<DataPlane>) {
    let options = EngineOptions {
        no_forward: true,
        result_cache,
        ingestion: IngestionOptions {
            allowed_lateness_ms: 0,
            stream_start_ms: Some(0),
            ..IngestionOptions::default()
        },
        ..EngineOptions::default()
    };
    let state =
        Arc::new(DataPlane::with_options(plan_json, "http://127.0.0.1:1".into(), options).unwrap());
    (router(state.clone()), state)
}

/// Writes one sample per series of `metric` (two jobs) at each of
/// `times_ms`, then sets the event-time barrier at `barrier_ms`.
async fn write(app: &Router, metric: &str, times_ms: &[i64], barrier_ms: i64) {
    let timeseries = (0..2)
        .map(|series| TimeSeries {
            labels: [
                ("__name__", metric.to_string()),
                ("job", format!("job{series}")),
            ]
            .into_iter()
            .map(|(name, value)| Label {
                name: name.into(),
                value,
            })
            .collect(),
            samples: times_ms
                .iter()
                .map(|&timestamp| Sample {
                    timestamp,
                    value: (timestamp / 1000 * (series + 1)) as f64,
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
            Request::post(format!("/api/v1/precompute/watermark?time_ms={barrier_ms}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(barrier.status(), StatusCode::OK);
}

async fn get(app: &Router, uri: String) -> Value {
    let response = app
        .clone()
        .oneshot(Request::get(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    body
}

fn range_uri(query: &str, start_s: i64, end_s: i64, step_s: i64) -> String {
    format!(
        "/api/v1/query_range?query={}&start={start_s}&end={end_s}&step={step_s}",
        form_urlencoded::byte_serialize(query.as_bytes()).collect::<String>()
    )
}

fn instant_uri(query: &str, time_s: i64) -> String {
    format!(
        "/api/v1/query?query={}&time={time_s}",
        form_urlencoded::byte_serialize(query.as_bytes()).collect::<String>()
    )
}

/// The value of the metric line starting with `prefix`.
fn metric(state: &DataPlane, prefix: &str) -> u64 {
    let text = state.engine.render_metrics();
    text.lines()
        .find(|l| l.starts_with(prefix))
        .unwrap_or_else(|| panic!("{prefix} missing from\n{text}"))
        .rsplit(' ')
        .next()
        .unwrap()
        .parse::<f64>()
        .unwrap() as u64
}

fn step_runs(state: &DataPlane) -> u64 {
    metric(state, "asap_phase_runs_total{phase=\"query_range_step\"")
}

/// A repeated range request runs none of its final steps again and
/// answers what it answered the first time, which is what a data plane
/// without the cache answers.
#[tokio::test(flavor = "multi_thread")]
async fn a_repeated_range_request_runs_only_the_steps_not_cached() {
    let (app, state) = data_plane(views_plan_json(), true);
    let times: Vec<i64> = (1..=60).map(|s| s * 1000).collect();
    write(&app, "data", &times, 60_000).await;
    let first = get(&app, range_uri(VIEWS_QUERY, 30, 60, 10)).await;
    assert_eq!(step_runs(&state), 4);
    let second = get(&app, range_uri(VIEWS_QUERY, 30, 60, 10)).await;
    assert_eq!(step_runs(&state), 4, "every step came from the cache");
    assert_eq!(metric(&state, "asap_range_query_steps_cached_total"), 4);
    assert_eq!(first, second);
    let (uncached_app, uncached) = data_plane(views_plan_json(), false);
    write(&uncached_app, "data", &times, 60_000).await;
    assert_eq!(
        first,
        get(&uncached_app, range_uri(VIEWS_QUERY, 30, 60, 10)).await
    );
    get(&uncached_app, range_uri(VIEWS_QUERY, 30, 60, 10)).await;
    assert_eq!(step_runs(&uncached), 8, "without the cache every step runs");
}

/// An instant query at a range step's time reads that step's answer.
#[tokio::test(flavor = "multi_thread")]
async fn an_instant_query_reads_the_range_step_at_its_time() {
    let (app, state) = data_plane(views_plan_json(), true);
    let times: Vec<i64> = (1..=60).map(|s| s * 1000).collect();
    write(&app, "data", &times, 60_000).await;
    let range = get(&app, range_uri(VIEWS_QUERY, 30, 60, 10)).await;
    let hits = metric(&state, "asap_result_cache_hits_total");
    let instant = get(&app, instant_uri(VIEWS_QUERY, 40)).await;
    assert_eq!(metric(&state, "asap_result_cache_hits_total"), hits + 1);
    assert_eq!(
        metric(&state, "asap_phase_runs_total{phase=\"query\","),
        0,
        "the instant query did not run"
    );
    let instant = instant["data"]["result"].as_array().unwrap();
    assert_eq!(instant.len(), 2);
    for sample in instant {
        let series = range["data"]["result"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["metric"] == sample["metric"])
            .unwrap();
        assert!(series["values"]
            .as_array()
            .unwrap()
            .contains(&json!([40.0, sample["value"][1]])));
    }
}

/// A step after the watermark, whose raw samples may still arrive, is
/// answered but not kept: once its samples arrive, the next request runs
/// it again and answers with them.
#[tokio::test(flavor = "multi_thread")]
async fn a_step_answered_before_its_samples_arrived_runs_again() {
    let (app, state) = data_plane(common::plan_json("example1", true), true);
    let early: Vec<i64> = (1..=16).map(|s| s * 15_000).collect();
    write(&app, "http_requests_total", &early, 240_000).await;
    let before = get(&app, range_uri(RAW_QUERY, 240, 300, 60)).await;
    assert_eq!(step_runs(&state), 2);
    let late: Vec<i64> = (17..=20).map(|s| s * 15_000).collect();
    write(&app, "http_requests_total", &late, 300_000).await;
    let after = get(&app, range_uri(RAW_QUERY, 240, 300, 60)).await;
    assert_eq!(
        step_runs(&state),
        3,
        "the step at 240 s came from the cache, the one at 300 s ran again"
    );
    let value_at = |body: &Value, time_s: f64| -> Vec<Value> {
        body["data"]["result"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|s| s["values"].as_array().unwrap().clone())
            .filter(|v| v[0] == json!(time_s))
            .collect()
    };
    assert_eq!(value_at(&before, 240.0), value_at(&after, 240.0));
    assert_ne!(value_at(&before, 300.0), value_at(&after, 300.0));
    // What a data plane that never answered early answers.
    let (fresh_app, _) = data_plane(common::plan_json("example1", true), false);
    let all: Vec<i64> = (1..=20).map(|s| s * 15_000).collect();
    write(&fresh_app, "http_requests_total", &all, 300_000).await;
    assert_eq!(
        after,
        get(&fresh_app, range_uri(RAW_QUERY, 240, 300, 60)).await
    );
}

/// A step whose window reaches raw samples already evicted is refused, not
/// answered from the samples left (a partial window), and not kept.
#[tokio::test(flavor = "multi_thread")]
async fn a_step_reaching_evicted_samples_is_refused() {
    let (app, state) = data_plane(common::plan_json("example1", true), true);
    let times: Vec<i64> = (1..=40).map(|s| s * 15_000).collect();
    write(&app, "http_requests_total", &times, 600_000).await;
    // The barrier evicts the samples the 1m lookback no longer reaches;
    // a later write runs after that eviction on every shard.
    write(&app, "http_requests_total", &[600_000], 600_000).await;
    let response = app
        .clone()
        .oneshot(
            Request::get(instant_uri(RAW_QUERY, 120))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        metric(&state, "asap_result_cache_entries"),
        0,
        "nothing cached"
    );
}
