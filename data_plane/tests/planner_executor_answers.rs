//! The data plane answers what the Planner's executor answers, for the
//! plans the Planner selects without raw samples: DDSketch panes over zero
//! and signed samples (interpolated quantiles), top-k answers that keep the
//! chosen series' labels, counter top-k planned with accuracy guarantees
//! ignored, and queries planned jointly over one shared stored state.
//!
//! Each test plans with the backend's control plane, installs the plan with
//! `--no-forward` (every answer computed from the plan), writes the samples
//! by Remote Write, closes the panes with the event-time barrier, and
//! checks the answers against the exact ones, as the Planner's own runtime
//! tests do.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use asap_planner::{InstallablePlan, PlanningWorkload};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use control_plane::plan_workload;
use data_plane::engine::EngineOptions;
use data_plane::remote_write::{encode, Label, Sample, TimeSeries, WriteRequest};
use data_plane::{router, DataPlane, EXECUTION_HEADER};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

/// The evaluation time of every test.
const EVALUATION_MS: i64 = 3_600_000;

/// Labels including `__name__`.
type Labels = BTreeMap<String, String>;

/// One raw sample: its series' labels, its time and value.
struct Point {
    labels: Labels,
    timestamp_ms: i64,
    value: f64,
}

/// The selected plan of `workload` for a deployment that keeps no raw
/// samples, with accuracy guarantees ignored or not.
fn plan_without_raw_samples(
    workload: &PlanningWorkload,
    ignore_accuracy_guarantees: bool,
) -> InstallablePlan {
    let mut inputs = common::inputs(false);
    inputs.plan_selection.ignore_accuracy_guarantees = ignore_accuracy_guarantees;
    plan_workload(workload, &inputs).unwrap_or_else(|e| panic!("{e}"))
}

/// A data plane running `plan` with `--no-forward`, its panes built from
/// `points` written by Remote Write and closed by the barrier at
/// `EVALUATION_MS`.
async fn run(plan: &InstallablePlan, points: &[Point]) -> (Router, Arc<DataPlane>) {
    let options = EngineOptions {
        no_forward: true,
        ..EngineOptions::default()
    };
    let state = Arc::new(
        DataPlane::with_options(
            serde_json::to_string(plan).unwrap(),
            // Never asked: nothing is forwarded and no raw sample is read.
            "http://127.0.0.1:1".into(),
            options,
        )
        .unwrap(),
    );
    let app = router(state.clone());
    let mut by_series: BTreeMap<&Labels, Vec<Sample>> = BTreeMap::new();
    for point in points {
        by_series.entry(&point.labels).or_default().push(Sample {
            timestamp: point.timestamp_ms,
            value: point.value,
        });
    }
    let timeseries = by_series
        .into_iter()
        .map(|(labels, mut samples)| {
            samples.sort_by_key(|s| s.timestamp);
            TimeSeries {
                labels: labels
                    .iter()
                    .map(|(name, value)| Label {
                        name: name.clone(),
                        value: value.clone(),
                    })
                    .collect(),
                samples,
            }
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
    for _ in 0..1000 {
        if state.engine.queued_samples() == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let barrier = app
        .clone()
        .oneshot(
            Request::post(format!(
                "/api/v1/precompute/watermark?time_ms={EVALUATION_MS}"
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(barrier.status(), StatusCode::OK);
    (app, state)
}

/// The answer of `query` at `EVALUATION_MS`: each result series' labels and
/// value, sorted by labels. Panics unless the plan answered it.
async fn answer(app: &Router, query: &str) -> Vec<(Labels, f64)> {
    let uri = format!(
        "/api/v1/query?{}",
        form_urlencoded::Serializer::new(String::new())
            .append_pair("query", query)
            .append_pair("time", &(EVALUATION_MS as f64 / 1000.0).to_string())
            .finish()
    );
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
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(status, StatusCode::OK, "{query}: {body}");
    assert_eq!(execution.as_deref(), Some("plan"), "{query}: {body}");
    let mut rows: Vec<(Labels, f64)> = body["data"]["result"]
        .as_array()
        .unwrap()
        .iter()
        .map(|series| {
            let labels = series["metric"]
                .as_object()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_string()))
                .collect();
            let value = series["value"][1].as_str().unwrap().parse().unwrap();
            (labels, value)
        })
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

fn labels(pairs: &[(&str, &str)]) -> Labels {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// The MVP benchmark's p99 query with `outer` over the per-series p99s of a
/// gauge over 100 s, per (region, service, job), every second, within 0.05
/// at failure probability 0.3, over 1,000 `data` series sampled every
/// 100 ms, declared non-negative or not.
fn p99_workload(outer: &str, values_non_negative: bool) -> PlanningWorkload {
    quantile_workload(outer, 0.99, values_non_negative)
}

/// [`p99_workload`] with the quantile `q`.
fn quantile_workload(outer: &str, q: f64, values_non_negative: bool) -> PlanningWorkload {
    serde_json::from_str(&format!(
        r#"{{"query_workload": {{"language": "promql", "repeating_queries": [{{
              "query": "{outer} by (region, service, job) (quantile_over_time({q}, data[100s]))",
              "demand": {{"fixed_interval": 1000}},
              "requirements": {{"accuracy": {{"max_error": 0.05, "failure_probability": 0.3,
                  "accuracy_scope": "whole_result"}}}},
              "predictability": {{"predictable": {{"known_at": null}}}}}}]}},
            "data_workload": {{"arrival": {{"ingesting": {{"rows_per_second": 10000.0}}}},
              "sources": {{"data": {{"series_count": 1000, "metric_type": "gauge",
                  "sample_interval": 100, "values_non_negative": {values_non_negative}}}}}}}}}"#
    ))
    .unwrap()
}

/// Each (region, service, job) group's per-series windows
/// `(EVALUATION_MS - 100 s, EVALUATION_MS]`.
type Windows = BTreeMap<(String, String, String), Vec<Vec<f64>>>;

/// `data` samples of series `0..series`, `value(series, step)` every
/// `interval_ms` over the `steps` steps ending just before `EVALUATION_MS`,
/// two series per (region, service, job) group; and each group's windows.
fn data_samples(
    series: usize,
    steps: i64,
    interval_ms: i64,
    value: impl Fn(usize, i64) -> f64,
) -> (Vec<Point>, Windows) {
    let mut points = Vec::new();
    let mut windows: Windows = BTreeMap::new();
    for s in 0..series {
        let (region, service, job) = (format!("r{}", s % 2), format!("s{}", s / 4), "j".into());
        let series_labels = labels(&[
            ("__name__", "data"),
            ("region", &region),
            ("service", &service),
            ("job", "j"),
            ("instance", &format!("i{s}")),
        ]);
        let mut window = Vec::new();
        for step in 0..steps {
            let timestamp_ms = EVALUATION_MS - steps * interval_ms + 50 + step * interval_ms;
            let v = value(s, step);
            points.push(Point {
                labels: series_labels.clone(),
                timestamp_ms,
                value: v,
            });
            if timestamp_ms > EVALUATION_MS - 100_000 {
                window.push(v);
            }
        }
        windows
            .entry((region, service, job))
            .or_default()
            .push(window);
    }
    (points, windows)
}

/// PromQL's `quantile_over_time(q, ...)` of `window`: linear interpolation
/// at rank `q × (n − 1)`.
fn interpolated_quantile(q: f64, window: &[f64]) -> f64 {
    let mut sorted = window.to_vec();
    sorted.sort_by(f64::total_cmp);
    let rank = q * (sorted.len() - 1) as f64;
    let (low, high) = (rank.floor() as usize, rank.ceil() as usize);
    sorted[low] + (sorted[high] - sorted[low]) * (rank - low as f64)
}

/// Every group's answer is within the relative error 0.05 of `combine` of
/// its series' exact p99s.
fn assert_p99s_within_alpha(
    answers: &[(Labels, f64)],
    windows: &Windows,
    combine: impl Fn(&[f64]) -> f64,
) {
    assert_eq!(answers.len(), windows.len(), "{answers:?}");
    for (labels, value) in answers {
        let key = (
            labels["region"].clone(),
            labels["service"].clone(),
            labels["job"].clone(),
        );
        let p99s: Vec<f64> = windows[&key]
            .iter()
            .map(|window| interpolated_quantile(0.99, window))
            .collect();
        let exact = combine(&p99s);
        assert!(
            (value - exact).abs() <= 0.05 * exact.abs(),
            "{labels:?}: {value} vs exact {exact}"
        );
    }
}

fn average(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}

/// A non-negative gauge that reports exactly 0 is summarized by the
/// selected DDSketch panes (zero has its own bucket): each series' window
/// holds a third zeros, and every group's average of p99s is within 0.05 of
/// the exact average of PromQL's interpolated p99s.
#[tokio::test]
async fn ddsketch_panes_over_a_gauge_reporting_zero_answer_within_alpha() {
    let query = "avg by (region, service, job) (quantile_over_time(0.99, data[100s]))";
    let plan = plan_without_raw_samples(&p99_workload("avg", true), false);
    assert!(!plan.stored_outputs.is_empty(), "panes built at ingestion");
    let (points, windows) = data_samples(8, 1_100, 100, |s, step| {
        (s + 1) as f64 * [0.0, 3.0, 5.0][(step % 3) as usize]
    });
    let (app, _) = run(&plan, &points).await;
    assert_p99s_within_alpha(&answer(&app, query).await, &windows, average);
}

/// Signed samples (negative, zero and positive) are summarized by the
/// selected DDSketch panes through their negative store and zero bucket:
/// the `max` of the per-series p99s is within 0.05 of the exact value,
/// including groups whose samples are all negative.
#[tokio::test]
async fn ddsketch_panes_over_signed_samples_answer_within_alpha() {
    let query = "max by (region, service, job) (quantile_over_time(0.99, data[100s]))";
    let plan = plan_without_raw_samples(&p99_workload("max", false), false);
    assert!(!plan.stored_outputs.is_empty(), "panes built at ingestion");
    // Series 0..4 cross zero; series 4..8 are all negative.
    let (points, windows) = data_samples(8, 1_100, 100, |s, step| {
        let scale = (s + 1) as f64;
        if s < 4 {
            scale * [-7.0, -3.0, 0.0, 2.0, 4.0][(step % 5) as usize]
        } else {
            scale * [-9.0, -8.0, -5.0, -1.0][(step % 4) as usize]
        }
    });
    let (app, _) = run(&plan, &points).await;
    let max = |p99s: &[f64]| p99s.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    assert_p99s_within_alpha(&answer(&app, query).await, &windows, max);
}

/// A quantile of few samples interpolates between the two nearest, as
/// PromQL does: each series' window holds 1..=10 scaled by the series, so
/// its median is 5.5 × scale (rank 4.5), not a sample (5 or 6 × scale).
/// Every group's average of medians is within 0.05 of the exact one.
#[tokio::test]
async fn ddsketch_quantiles_interpolate_between_samples() {
    let query = "avg by (region, service, job) (quantile_over_time(0.5, data[100s]))";
    let plan = plan_without_raw_samples(&quantile_workload("avg", 0.5, true), false);
    assert!(!plan.stored_outputs.is_empty(), "panes built at ingestion");
    // Eleven steps: the first, before the window, opens the panes (the
    // first pane after startup is never built).
    let (points, windows) = data_samples(8, 11, 10_000, |s, step| (s + 1) as f64 * step as f64);
    assert!(windows.values().flatten().all(|window| window.len() == 10));
    let (app, _) = run(&plan, &points).await;
    let answers = answer(&app, query).await;
    assert_eq!(answers.len(), windows.len(), "{answers:?}");
    for (labels, value) in &answers {
        let key = (
            labels["region"].clone(),
            labels["service"].clone(),
            labels["job"].clone(),
        );
        let medians: Vec<f64> = windows[&key]
            .iter()
            .map(|window| interpolated_quantile(0.5, window))
            .collect();
        let exact = average(&medians);
        assert!(
            (value - exact).abs() <= 0.05 * exact,
            "{labels:?}: {value} vs exact {exact}"
        );
    }
}

/// `data` samples for the top-k tests: three per series inside the last
/// minute, each a third of its sum, after a sample (1,000) two minutes
/// earlier that opens the panes (the first pane after startup is never
/// built) and no answer reads. Series
/// `e1`..`e3` are in `east` with sums 6, 60 and 30; `w1`, `w2` in `west`
/// with sums 9 and 3.
fn topk_samples() -> Vec<Point> {
    let mut points = Vec::new();
    for (instance, region, service, sum) in [
        ("e1", "east", "api", 6.0),
        ("e2", "east", "web", 60.0),
        ("e3", "east", "db", 30.0),
        ("w1", "west", "api", 9.0),
        ("w2", "west", "web", 3.0),
    ] {
        for step in [8, 3, 2, 1] {
            points.push(Point {
                labels: labels(&[
                    ("__name__", "data"),
                    ("instance", instance),
                    ("region", region),
                    ("service", service),
                ]),
                timestamp_ms: EVALUATION_MS - step * 15_000,
                value: if step == 8 { 1000.0 } else { sum / 3.0 },
            });
        }
    }
    points
}

/// `query` every second within 0.05 at failure probability 0.3 over 1,000
/// `data` gauge series sampled every 100 ms: the benchmark's top-k shape.
/// Planned with accuracy guarantees ignored, as the benchmark plans it.
fn topk_workload(query: &str) -> PlanningWorkload {
    serde_json::from_str(&format!(
        r#"{{"query_workload": {{"language": "promql", "repeating_queries": [{{
              "query": {query:?},
              "demand": {{"fixed_interval": 1000}},
              "requirements": {{"accuracy": {{"max_error": 0.05, "failure_probability": 0.3,
                  "accuracy_scope": "whole_result"}}}},
              "predictability": {{"predictable": {{"known_at": null}}}}}}]}},
            "data_workload": {{"arrival": {{"ingesting": {{"rows_per_second": 10000.0}}}},
              "sources": {{"data": {{"series_count": 1000, "sample_interval": 100,
                  "metric_type": "gauge"}}}}}}}}"#
    ))
    .unwrap()
}

/// PromQL `topk` returns whole input series: `topk by (region) (2,
/// sum_over_time(data[1m]))` answers, per region, the two series with the
/// largest sums, each with its full label set (region, service and
/// instance; `sum_over_time` drops the metric name), not only `region`.
#[tokio::test]
async fn topk_answers_carry_the_chosen_series_labels() {
    let query = "topk by (region) (2, sum_over_time(data[1m]))";
    let plan = plan_without_raw_samples(&topk_workload(query), true);
    let (app, _) = run(&plan, &topk_samples()).await;
    let series = |instance: &str, region: &str, service: &str| {
        labels(&[
            ("instance", instance),
            ("region", region),
            ("service", service),
        ])
    };
    let expected = [
        (series("e2", "east", "web"), 60.0),
        (series("e3", "east", "db"), 30.0),
        (series("w1", "west", "api"), 9.0),
        (series("w2", "west", "web"), 3.0),
    ];
    let answered = answer(&app, query).await;
    assert_eq!(answered.len(), expected.len(), "{answered:?}");
    for ((labels, value), (want_labels, want)) in answered.iter().zip(&expected) {
        assert_eq!(labels, want_labels);
        assert!((value - want).abs() < 1e-9, "{labels:?}: {value}");
    }
}

/// Every top-k and bottom-k form, grouped or not, over sums, counts and
/// rates, answers whole input series.
#[tokio::test]
async fn every_topk_form_answers_whole_series() {
    for (query, expected) in [
        ("topk(2, sum_over_time(data[1m]))", 2),
        ("topk by (region) (1, count_over_time(data[1m]))", 2),
        ("topk by (region) (1, rate(data[1m]))", 2),
        ("bottomk by (region) (1, sum_over_time(data[1m]))", 2),
        ("bottomk(3, sum_over_time(data[1m]))", 3),
    ] {
        let plan = plan_without_raw_samples(&topk_workload(query), true);
        let (app, _) = run(&plan, &topk_samples()).await;
        let answered = answer(&app, query).await;
        assert_eq!(answered.len(), expected, "{query}: {answered:?}");
        for (labels, _) in &answered {
            assert!(
                labels.contains_key("instance") && labels.contains_key("service"),
                "{query}: {labels:?}"
            );
        }
    }
}

/// The per-second rate of series `instance` (1 or 2) of job `job` (1..=12):
/// distinct for every series and every job's sum.
fn counter_rate(job: i64, instance: i64) -> f64 {
    job as f64 + 0.1 * instance as f64
}

/// `data_total` samples of 12 jobs with 2 series each, every 10 s from
/// 595 s before `EVALUATION_MS`, each growing linearly at its rate, so its
/// 5-minute `increase` is 300 × its rate.
fn counter_samples() -> Vec<Point> {
    let mut points = Vec::new();
    for job in 1..=12 {
        for instance in 1..=2 {
            for step in 0..60 {
                points.push(Point {
                    labels: labels(&[
                        ("__name__", "data_total"),
                        ("instance", &format!("{job}-{instance}")),
                        ("job", &format!("j{job:02}")),
                    ]),
                    timestamp_ms: EVALUATION_MS - 595_000 + step * 10_000,
                    value: counter_rate(job, instance) * (5 + step * 10) as f64,
                });
            }
        }
    }
    points
}

/// The ASAPQuery workloads' counter top-k, `topk(10, sum by (job)
/// (<f>(data_total[5m])))` for `increase` and `rate`, planned with accuracy
/// guarantees ignored and no raw samples: the plan the Planner's executor
/// compiles runs here too and answers the exact top 10 jobs (3..=12).
#[tokio::test]
async fn counter_topk_planned_without_guarantees_answers_exactly() {
    for (function, seconds) in [("increase", 300.0), ("rate", 1.0)] {
        let query = format!("topk(10, sum by (job) ({function}(data_total[5m])))");
        let workload: PlanningWorkload = serde_json::from_str(&format!(
            r#"{{"query_workload": {{"language": "promql", "repeating_queries": [{{
                  "query": {query:?},
                  "demand": {{"fixed_interval": 60000}},
                  "requirements": {{"accuracy": {{"max_error": 0.05, "failure_probability": 0.3}}}},
                  "predictability": {{"predictable": {{"known_at": null}}}}}}]}},
                "data_workload": {{"arrival": {{"ingesting": {{"rows_per_second": 100000.0}}}},
                  "sources": {{"data_total": {{"series_count": 10000, "metric_type": "counter",
                      "sample_interval": 100}}}}}}}}"#
        ))
        .unwrap();
        let plan = plan_without_raw_samples(&workload, true);
        let (app, _) = run(&plan, &counter_samples()).await;
        let answered = answer(&app, &query).await;
        let expected: Vec<(Labels, f64)> = (3..=12)
            .map(|job| {
                (
                    labels(&[("job", &format!("j{job:02}"))]),
                    (counter_rate(job, 1) + counter_rate(job, 2)) * seconds,
                )
            })
            .collect();
        assert_eq!(answered.len(), expected.len(), "{function}: {answered:?}");
        for ((labels, value), (want_labels, want)) in answered.iter().zip(&expected) {
            assert_eq!(labels, want_labels, "{function}");
            assert!(
                (value - want).abs() <= 1e-9 * want,
                "{function} {labels:?}: {value} vs {want}"
            );
        }
    }
}

/// `queries` every 10 s at `accuracy` (JSON) over six ingesting series of
/// `data`, a non-negative gauge: one workload, planned jointly.
fn joint_workload(queries: &[&str], accuracy: &str) -> PlanningWorkload {
    let entries: Vec<String> = queries
        .iter()
        .map(|query| {
            format!(
                r#"{{"query": {query:?}, "demand": {{"fixed_interval": 10000}},
                  "requirements": {accuracy},
                  "predictability": {{"predictable": {{"known_at": null}}}}}}"#
            )
        })
        .collect();
    serde_json::from_str(&format!(
        r#"{{"query_workload": {{"language": "promql", "repeating_queries": [{}]}},
            "data_workload": {{"arrival": {{"ingesting": {{"rows_per_second": 0.6}}}},
              "sources": {{"data": {{"series_count": 6, "sample_interval": 10000,
                "metric_type": "gauge", "values_non_negative": true}}}}}}}}"#,
        entries.join(",")
    ))
    .unwrap()
}

/// Six series of `data`: `region` east/west, `service` api/web/db, `job`
/// batch/online; positive values, one every 10 s over the last two
/// minutes; and each series' window `(EVALUATION_MS - 100 s,
/// EVALUATION_MS]`.
fn grouped_series_samples() -> (Vec<Point>, Vec<(Labels, Vec<f64>)>) {
    let mut points = Vec::new();
    let mut windows = Vec::new();
    for (s, (region, service, job)) in [
        ("east", "api", "batch"),
        ("east", "web", "online"),
        ("east", "db", "batch"),
        ("west", "api", "online"),
        ("west", "web", "batch"),
        ("west", "db", "online"),
    ]
    .into_iter()
    .enumerate()
    {
        let series_labels = labels(&[
            ("__name__", "data"),
            ("region", region),
            ("service", service),
            ("job", job),
        ]);
        let mut window = Vec::new();
        for step in 0..12 {
            let timestamp_ms = EVALUATION_MS - 120_000 + 5_000 + step * 10_000;
            let value = (s as f64 + 1.0) * 10.0 + step as f64;
            points.push(Point {
                labels: series_labels.clone(),
                timestamp_ms,
                value,
            });
            if timestamp_ms > EVALUATION_MS - 100_000 {
                window.push(value);
            }
        }
        let mut answer_labels = series_labels;
        answer_labels.remove("__name__");
        windows.push((answer_labels, window));
    }
    (points, windows)
}

/// Quantile queries over one metric and window with different `by` labels
/// (ASAPQuery's seed-862 quantile queries), planned jointly, share one
/// per-series DDSketch, and each answers within 0.05 of its exact value.
#[tokio::test]
async fn jointly_planned_quantiles_share_one_sketch_and_answer_within_alpha() {
    let queries = [
        "quantile_over_time(0.9, data[100s])",
        "avg by (region) (quantile_over_time(0.99, data[100s]))",
    ];
    let accuracy = r#"{"accuracy": {"accuracy_metric": "relative", "max_error": 0.05,
        "failure_probability": 0.01, "accuracy_scope": "per_value"}}"#;
    let plan = plan_without_raw_samples(&joint_workload(&queries, accuracy), false);
    let sketch_definitions: std::collections::BTreeSet<&str> = plan
        .stored_outputs
        .iter()
        .filter(|output| {
            let node = plan
                .dag
                .dag
                .nodes
                .iter()
                .find(|n| n.id == output.node_id)
                .unwrap();
            serde_json::to_string(&node.payload)
                .unwrap()
                .contains("DDSketch")
        })
        .map(|output| output.coverage.definition_id.as_str())
        .collect();
    assert_eq!(sketch_definitions.len(), 1, "{sketch_definitions:?}");
    let (points, windows) = grouped_series_samples();
    let (app, _) = run(&plan, &points).await;

    let per_series = answer(&app, queries[0]).await;
    assert_eq!(per_series.len(), 6, "{per_series:?}");
    for (labels, value) in &per_series {
        let (_, window) = windows.iter().find(|(l, _)| l == labels).unwrap();
        let exact = interpolated_quantile(0.9, window);
        assert!(
            (value - exact).abs() <= 0.05 * exact,
            "{labels:?}: {value} vs {exact}"
        );
    }
    let per_region = answer(&app, queries[1]).await;
    assert_eq!(per_region.len(), 2, "{per_region:?}");
    for (labels, value) in &per_region {
        let p99s: Vec<f64> = windows
            .iter()
            .filter(|(l, _)| l["region"] == labels["region"])
            .map(|(_, window)| interpolated_quantile(0.99, window))
            .collect();
        let exact = average(&p99s);
        assert!(
            (value - exact).abs() <= 0.05 * exact,
            "{labels:?}: {value} vs {exact}"
        );
    }
}

/// A fine and a coarse sum over one metric and window (ASAPQuery's
/// sum-fine and sum-coarse), planned jointly, are both answered exactly
/// from the stored per-series sums.
#[tokio::test]
async fn jointly_planned_fine_and_coarse_sums_answer_exactly() {
    let queries = [
        "sum by (region, service, job) (sum_over_time(data[100s]))",
        "sum by (region) (sum_over_time(data[100s]))",
    ];
    let plan = plan_without_raw_samples(&joint_workload(&queries, "{}"), false);
    let (points, windows) = grouped_series_samples();
    let (app, _) = run(&plan, &points).await;
    let fine = answer(&app, queries[0]).await;
    assert_eq!(fine.len(), 6, "{fine:?}");
    for (labels, value) in &fine {
        let (_, window) = windows.iter().find(|(l, _)| l == labels).unwrap();
        let exact: f64 = window.iter().sum();
        assert!(
            (value - exact).abs() < 1e-9,
            "{labels:?}: {value} vs {exact}"
        );
    }
    let coarse = answer(&app, queries[1]).await;
    assert_eq!(coarse.len(), 2, "{coarse:?}");
    for (labels, value) in &coarse {
        let exact: f64 = windows
            .iter()
            .filter(|(l, _)| l["region"] == labels["region"])
            .flat_map(|(_, window)| window)
            .sum();
        assert!(
            (value - exact).abs() < 1e-9,
            "{labels:?}: {value} vs {exact}"
        );
    }
}
