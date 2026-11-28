//! A planned range query is answered from the plan with one evaluation per
//! step: its answer at each step is the instant answer at that time.

mod common;

use std::sync::Arc;

use axum::http::StatusCode;
use axum::Router;
use control_plane::plan_workload;
use data_plane::engine::EngineOptions;
use data_plane::{router, DataPlane};
use precompute::ingestion::IngestionOptions;
use prometheus_remote_write::{Label, Sample, TimeSeries};
use serde_json::json;

const QUERY: &str = "sum by (job) (sum_over_time(data[10s]))";

/// The plan of `QUERY` every 10 s, each request a range over the last 30 s
/// at 10 s steps, without raw samples kept.
fn plan_json() -> String {
    common::one_query_plan_json(
        QUERY,
        2,
        json!({"range_evaluation": {"step_ms": 10000, "span_ms": 30000}}),
    )
}

/// A data plane with two series of `data` sampled every second for 60 s,
/// every window closed.
async fn data_plane() -> Router {
    let options = common::no_forward_options();
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
    common::remote_write(&app, timeseries).await;
    common::barrier(&app, 60_000).await;
    app
}

fn encoded(query: &str) -> String {
    form_urlencoded::byte_serialize(query.as_bytes()).collect()
}

/// The range answer has one series per job, with a value at every step,
/// and each step's values are the instant answer at that time.
#[tokio::test(flavor = "multi_thread")]
async fn a_planned_range_query_answers_as_its_steps_do() {
    let app = data_plane().await;
    let (status, execution, body) = common::get(
        &app,
        &format!(
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
        let (status, _, instant) = common::get(
            &app,
            &format!("/api/v1/query?query={}&time={time_s}", encoded(QUERY)),
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
    let (status, execution, body) =
        common::get(&app, "/api/v1/query_range?query=up&start=0&end=60&step=15").await;
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
    let options = common::no_forward_options();
    let state = Arc::new(
        DataPlane::with_options(plan_json(), "http://127.0.0.1:1".into(), options).unwrap(),
    );
    let app = router(state.clone());
    let (status, _, body) = common::get(
        &app,
        &format!(
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

/// With the result cache declared (`final_query_results_cached`), the plan
/// keeps a range evaluation's inputs only for its refresh interval: a data
/// plane whose older steps are not cached cannot answer a 60 s span over
/// 10 s windows from the views alone, but each refresh caches the steps it
/// can answer, so within one span every step is answered (from the cache),
/// as the steps' instant answers. One query worker, so a range's steps run
/// one after another.
#[tokio::test(flavor = "multi_thread")]
async fn a_refreshed_range_reads_its_older_steps_from_the_result_cache() {
    let workload = serde_json::from_value(json!({
        "query_workload": {"language": "promql", "repeating_queries": [{
            "query": QUERY,
            "demand": {"fixed_interval_at": {"interval": 10000, "evaluation_phase": 0}},
            "requirements": {},
            "predictability": {"predictable": {"known_at": null}},
            "range_evaluation": {"step_ms": 10000, "span_ms": 60000},
        }]},
        "data_workload": {"arrival": {"ingesting": {"rows_per_second": 2.0}},
          "sources": {"data": {"series_count": 2, "sample_interval": 1000,
            "metric_type": "gauge", "column_signs": {"value": "non_negative"}}}},
    }))
    .unwrap();
    let inputs = common::inputs(false);
    assert!(inputs.deployment_capabilities.final_query_results_cached);
    let plan = serde_json::to_string(&plan_workload(&workload, &inputs).unwrap().0).unwrap();
    let start = || {
        let options = EngineOptions {
            no_forward: true,
            ingestion: IngestionOptions {
                allowed_lateness_ms: 0,
                stream_start_ms: Some(0),
                view_retention_margin_ms: 0,
                ..IngestionOptions::default()
            },
            query_threads: 1,
            ..EngineOptions::default()
        };
        router(Arc::new(
            DataPlane::with_options(plan.clone(), "http://127.0.0.1:1".into(), options).unwrap(),
        ))
    };
    // Each second's samples of jobs a (value s) and b (value 2 s), then a
    // barrier at `to_s`.
    let write = |app: Router, from_s: i64, to_s: i64| async move {
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
                samples: (from_s + 1..=to_s)
                    .map(|s| Sample {
                        timestamp: s * 1000,
                        value: (s * (index as i64 + 1)) as f64,
                    })
                    .collect(),
            })
            .collect();
        common::remote_write(&app, timeseries).await;
        common::barrier(&app, to_s * 1000).await;
    };
    let range = |end_s: i64| {
        format!(
            "/api/v1/query_range?query={}&start={}&end={end_s}&step=10",
            encoded(QUERY),
            end_s - 60
        )
    };

    // Started with a minute of data already in (as after a restart), then
    // refreshed every 10 s: the first requests fail on their uncached older
    // steps, yet cache their newer ones; after one span every step is
    // answered.
    let refreshed = start();
    write(refreshed.clone(), 0, 60).await;
    let (status, _, body) = common::get(&refreshed, &range(60)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    for to_s in (70..=120).step_by(10) {
        write(refreshed.clone(), to_s - 10, to_s).await;
        common::get(&refreshed, &range(to_s)).await;
    }
    let (status, _, body) = common::get(&refreshed, &range(120)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    for series in body["data"]["result"].as_array().unwrap() {
        let scale = if series["metric"]["job"] == "a" { 1 } else { 2 };
        let values = series["values"].as_array().unwrap();
        assert_eq!(values.len(), 7, "{series}");
        for point in values {
            let t = point[0].as_f64().unwrap() as i64;
            let expected: i64 = (t - 9..=t).map(|s| s * scale).sum();
            assert_eq!(point[1], json!(expected.to_string()), "{series}");
        }
    }

    // The same data written at once and asked once: the older steps'
    // windows are gone and nothing is cached.
    let cold = start();
    write(cold.clone(), 0, 120).await;
    let (status, _, body) = common::get(&cold, &range(120)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
}
