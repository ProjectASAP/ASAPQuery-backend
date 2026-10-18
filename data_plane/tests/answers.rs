//! The data plane answers planned instant queries over HTTP, from stored
//! panes and from raw samples it fetches, and forwards what it cannot answer.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::Query;
use axum::http::{Request, StatusCode};
use axum::routing::get;
use axum::{Json, Router};
use data_plane::remote_write::{encode, Label, Sample, TimeSeries, WriteRequest};
use data_plane::{router, DataPlane, EXECUTION_HEADER, FORWARD_REASON_HEADER};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;

const MINUTE: i64 = 60_000;

/// `(labels, timestamp ms, value)`, labels including `__name__`.
type Point = (BTreeMap<String, String>, i64, f64);

fn series(metric: &str, labels: &[(&str, &str)]) -> BTreeMap<String, String> {
    let mut map = BTreeMap::from([("__name__".to_string(), metric.to_string())]);
    map.extend(labels.iter().map(|(k, v)| (k.to_string(), v.to_string())));
    map
}

/// A stand-in Prometheus holding `points`. It answers
/// `{__name__="m"}[Nms]` at `time` with the samples in `[time - N, time]`,
/// both ends included as Prometheus 2 does, and anything else with
/// `{"forwarded": query}`. Range queries are answered with
/// `{"forwarded_range": parameters}`.
async fn fake_prometheus(points: Vec<Point>) -> String {
    let points = Arc::new(points);
    let handler = move |Query(params): Query<BTreeMap<String, String>>| {
        let points = points.clone();
        async move {
            let query = &params["query"];
            let parsed = query
                .strip_prefix("{__name__=\"")
                .and_then(|rest| rest.split_once("\"}["))
                .and_then(|(metric, rest)| {
                    Some((metric, rest.strip_suffix("ms]")?.parse::<i64>().ok()?))
                });
            let Some((metric, range)) = parsed else {
                return Json(json!({ "forwarded": query }));
            };
            let time = (params["time"].parse::<f64>().unwrap() * 1000.0).round() as i64;
            let mut by_series: BTreeMap<&BTreeMap<String, String>, Vec<Value>> = BTreeMap::new();
            for (labels, t, v) in points.iter() {
                if labels["__name__"] == metric && time - range <= *t && *t <= time {
                    by_series
                        .entry(labels)
                        .or_default()
                        .push(json!([*t as f64 / 1000.0, v.to_string()]));
                }
            }
            let result: Vec<Value> = by_series
                .into_iter()
                .map(|(labels, values)| json!({"metric": labels, "values": values}))
                .collect();
            Json(json!({"status": "success", "data": {"resultType": "matrix", "result": result}}))
        }
    };
    let range = |Query(params): Query<BTreeMap<String, String>>| async move {
        Json(json!({ "forwarded_range": params }))
    };
    let app = Router::new()
        .route("/api/v1/query", get(handler))
        .route("/api/v1/query_range", get(range));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    url
}

struct Response {
    execution: Option<String>,
    reason: Option<String>,
    body: Value,
}

async fn instant(app: &Router, query: &str, time_ms: i64) -> Response {
    let time = (time_ms as f64 / 1000.0).to_string();
    get_json(app, "query", &[("query", query), ("time", &time)]).await
}

/// `GET /api/v1/query_range` with `params`, as given.
async fn range(app: &Router, params: &[(&str, &str)]) -> Response {
    get_json(app, "query_range", params).await
}

async fn get_json(app: &Router, endpoint: &str, params: &[(&str, &str)]) -> Response {
    let uri = format!(
        "/api/v1/{endpoint}?{}",
        form_urlencoded::Serializer::new(String::new())
            .extend_pairs(params)
            .finish()
    );
    let response = app
        .clone()
        .oneshot(Request::get(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let header = |name| {
        response
            .headers()
            .get(name)
            .map(|v: &axum::http::HeaderValue| v.to_str().unwrap().to_string())
    };
    let (execution, reason) = (header(EXECUTION_HEADER), header(FORWARD_REASON_HEADER));
    let body = response.into_body().collect().await.unwrap().to_bytes();
    Response {
        execution,
        reason,
        body: serde_json::from_slice(&body).unwrap(),
    }
}

/// `(job, value)` of each result series, by job.
fn by_job(body: &Value) -> Vec<(String, f64)> {
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

/// Example 1 runs at query time: the data plane fetches the last minute of
/// raw samples from Prometheus and runs the plan over them. A sample at the
/// range's start, which Prometheus 2 returns, is excluded as in PromQL.
#[tokio::test]
async fn answers_query_time_plans_from_raw_samples() {
    let mut points = Vec::new();
    for (job, instance) in [("a", "1"), ("a", "2"), ("b", "1")] {
        let labels = series(
            "http_requests_total",
            &[("job", job), ("instance", instance)],
        );
        for t in (0..10 * MINUTE).step_by(15_000) {
            points.push((labels.clone(), t, t as f64 / 1000.0));
        }
    }
    let state = DataPlane::new(
        common::plan_json("example1", true),
        fake_prometheus(points).await,
        0,
    )
    .unwrap();
    let app = router(Arc::new(state));
    let response = instant(
        &app,
        "sum by (job) (rate(http_requests_total[1m]))",
        5 * MINUTE,
    )
    .await;
    assert_eq!(
        response.execution.as_deref(),
        Some("plan"),
        "{:?}",
        response.body
    );
    let result = by_job(&response.body);
    let jobs: Vec<&str> = result.iter().map(|(job, _)| job.as_str()).collect();
    assert_eq!(jobs, ["a", "b"]);
    for ((_, value), expected) in result.iter().zip([2.0, 1.0]) {
        assert!((value - expected).abs() < 1e-9, "{result:?}");
    }
    // (4 min, 5 min] holds the samples at 4:15 .. 5:00: 255 + 270 + 285 + 300.
    // Including the one at 4:00 would add 240.
    let response = instant(
        &app,
        "topk by (job) (10, sum_over_time(http_requests_total[1m]))",
        5 * MINUTE,
    )
    .await;
    assert_eq!(
        response.execution.as_deref(),
        Some("plan"),
        "{:?}",
        response.body
    );
    let result = by_job(&response.body);
    assert_eq!(result.len(), 3, "{result:?}");
    assert!(
        result.iter().all(|(_, value)| *value == 1110.0),
        "{result:?}"
    );

    let response = instant(&app, "up", 5 * MINUTE).await;
    assert_eq!(response.execution.as_deref(), Some("forwarded"));
    assert_eq!(
        response.reason, None,
        "unplanned queries are simply forwarded"
    );
    assert_eq!(response.body, json!({"forwarded": "up"}));
}

/// Example 4b's data plane, with Remote Write samples every 15 s in
/// `[1 min, end)` for jobs a and b.
async fn example4b(end_ms: i64) -> (Arc<DataPlane>, Router) {
    let state = Arc::new(
        DataPlane::new(
            common::plan_json("example4b", false),
            fake_prometheus(vec![]).await,
            0,
        )
        .unwrap(),
    );
    let app = router(state.clone());
    let timeseries = ["a", "b"]
        .into_iter()
        .enumerate()
        .map(|(i, job)| TimeSeries {
            labels: vec![
                Label {
                    name: "__name__".into(),
                    value: "latency_ms".into(),
                },
                Label {
                    name: "job".into(),
                    value: job.into(),
                },
            ],
            samples: (MINUTE..end_ms)
                .step_by(15_000)
                .map(|t| Sample {
                    timestamp: t,
                    value: (i * 1000) as f64 + ((t / 15_000) % 240) as f64,
                })
                .collect(),
        })
        .collect();
    let write = app
        .clone()
        .oneshot(
            Request::post("/api/v1/write")
                .body(Body::from(encode(&WriteRequest { timeseries })))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(write.status(), StatusCode::NO_CONTENT);
    // Wait until the engine has ingested the samples.
    state.engine.stats().await;
    (state, app)
}

/// Example 4b reads six stored 10-minute KLL panes built from Remote Write;
/// at a time off the pane grid it forwards and says why.
#[tokio::test]
async fn answers_from_panes_built_from_remote_write() {
    let (state, app) = example4b(75 * MINUTE).await;
    // Panes ending at 20..70 min; the first, (0, 10 min], is skipped.
    assert_eq!(state.engine.stats().await.panes_built, 6);

    let query = "quantile_over_time(0.99, latency_ms[60m])";
    let response = instant(&app, query, 70 * MINUTE).await;
    assert_eq!(
        response.execution.as_deref(),
        Some("plan"),
        "{:?}",
        response.body
    );
    let result = by_job(&response.body);
    assert_eq!(result.len(), 2);
    for ((_, value), base) in result.iter().zip([0.0, 1000.0]) {
        assert!(
            *value >= base + 236.0 && *value <= base + 239.0,
            "{result:?}"
        );
    }

    let response = instant(&app, query, 70 * MINUTE + 1_000).await;
    assert_eq!(response.execution.as_deref(), Some("forwarded"));
    assert!(response.reason.unwrap().contains("pane grid"));
}

const P99: &str = "quantile_over_time(0.99, latency_ms[60m])";

/// A planned range query is answered with a Prometheus matrix, from `start`
/// to `end` inclusive by `step` (a duration or float seconds); each value
/// equals the instant query at its step.
#[tokio::test]
async fn answers_planned_range_queries() {
    let (_, app) = example4b(101 * MINUTE).await;
    for step in ["10m", "600", "600.0"] {
        let response = range(
            &app,
            &[
                ("query", P99),
                ("start", "5400"),
                ("end", "6000"),
                ("step", step),
            ],
        )
        .await;
        assert_eq!(
            response.execution.as_deref(),
            Some("plan"),
            "{step}: {:?}",
            response.reason
        );
        assert_eq!(response.body["status"], "success");
        assert_eq!(response.body["data"]["resultType"], "matrix");
        let result = response.body["data"]["result"].as_array().unwrap();
        assert_eq!(result.len(), 2, "{result:?}");
        for series in result {
            let job = series["metric"]["job"].as_str().unwrap();
            let values = series["values"].as_array().unwrap();
            let times: Vec<f64> = values.iter().map(|v| v[0].as_f64().unwrap()).collect();
            assert_eq!(times, vec![5400.0, 6000.0], "the end is included");
            for value in values {
                let t = (value[0].as_f64().unwrap() * 1000.0) as i64;
                let at_step = instant(&app, P99, t).await;
                assert_eq!(at_step.execution.as_deref(), Some("plan"));
                let expected = by_job(&at_step.body)
                    .into_iter()
                    .find(|(j, _)| j == job)
                    .unwrap()
                    .1;
                assert_eq!(value[1].as_str().unwrap().parse::<f64>().unwrap(), expected);
            }
        }
    }
}

/// A planned range query any of whose steps the plan cannot answer is
/// forwarded whole, with the reason: a step off the 10-minute pane grid, or
/// a step reading an evicted pane while the later steps could be answered.
#[tokio::test]
async fn forwards_whole_range_queries_a_step_cannot_answer() {
    let (_, app) = example4b(101 * MINUTE).await;
    for (start, step, reason) in [("5400", "5m", "pane grid"), ("4800", "10m", "not stored")] {
        let params = [
            ("query", P99),
            ("start", start),
            ("end", "6000"),
            ("step", step),
        ];
        let response = range(&app, &params).await;
        assert_eq!(response.execution.as_deref(), Some("forwarded"));
        let header = response.reason.unwrap_or_default();
        assert!(header.contains(reason), "{start} {step}: {header}");
        assert_eq!(
            response.body,
            json!({"forwarded_range": {"query": P99, "start": start, "end": "6000", "step": step}}),
            "forwarded unchanged"
        );
    }
}

/// Unplanned range queries, and range queries Prometheus would reject, are
/// forwarded unchanged with no reason: Prometheus answers them.
#[tokio::test]
async fn forwards_unplanned_and_invalid_range_queries_unchanged() {
    let (_, app) = example4b(101 * MINUTE).await;
    for params in [
        [
            ("query", "up"),
            ("start", "5400"),
            ("end", "6000"),
            ("step", "15s"),
        ],
        [
            ("query", P99),
            ("start", "5400"),
            ("end", "6000"),
            ("step", "0"),
        ],
        [
            ("query", P99),
            ("start", "6000"),
            ("end", "5400"),
            ("step", "10m"),
        ],
    ] {
        let response = range(&app, &params).await;
        assert_eq!(
            response.execution.as_deref(),
            Some("forwarded"),
            "{params:?}"
        );
        assert_eq!(response.reason, None, "{params:?}");
        let expected: BTreeMap<&str, &str> = params.into_iter().collect();
        assert_eq!(response.body, json!({ "forwarded_range": expected }));
    }
}
