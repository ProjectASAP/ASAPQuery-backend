//! The data plane answers planned instant queries over HTTP, from stored
//! panes and from raw samples it fetches, and forwards what it cannot answer.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::Query;
use axum::http::{HeaderMap, Request, StatusCode};
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
/// `{"forwarded": query}`.
async fn fake_prometheus(points: Vec<Point>) -> String {
    fake_prometheus_requiring(points, &[]).await
}

/// [`fake_prometheus`] that answers 401 to a request missing any of
/// `required` (header name, value).
async fn fake_prometheus_requiring(points: Vec<Point>, required: &[(&str, &str)]) -> String {
    let points = Arc::new(points);
    let required: Arc<Vec<(String, String)>> = Arc::new(
        required
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    );
    let handler = move |headers: HeaderMap, Query(params): Query<BTreeMap<String, String>>| {
        let points = points.clone();
        let required = required.clone();
        async move {
            let authorized = required.iter().all(|(name, value)| {
                headers
                    .get(name.as_str())
                    .is_some_and(|v| v == value.as_str())
            });
            if !authorized {
                return (StatusCode::UNAUTHORIZED, Json(json!({"status": "error"})));
            }
            let query = &params["query"];
            let parsed = query
                .strip_prefix("{__name__=\"")
                .and_then(|rest| rest.split_once("\"}["))
                .and_then(|(metric, rest)| {
                    Some((metric, rest.strip_suffix("ms]")?.parse::<i64>().ok()?))
                });
            let Some((metric, range)) = parsed else {
                return (StatusCode::OK, Json(json!({ "forwarded": query })));
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
            (
                StatusCode::OK,
                Json(
                    json!({"status": "success", "data": {"resultType": "matrix", "result": result}}),
                ),
            )
        }
    };
    let app = Router::new().route("/api/v1/query", get(handler));
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
    let uri = format!(
        "/api/v1/query?{}",
        form_urlencoded::Serializer::new(String::new())
            .append_pair("query", query)
            .append_pair("time", &(time_ms as f64 / 1000.0).to_string())
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

/// Example 4b reads six stored 10-minute KLL panes built from Remote Write;
/// at a time off the pane grid it forwards and says why.
#[tokio::test]
async fn answers_from_panes_built_from_remote_write() {
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
            samples: (MINUTE..75 * MINUTE)
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
    // Panes ending at 20..70 min; the first, (0, 10 min], is skipped.
    for _ in 0..500 {
        if state.engine.queued_samples() == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(state.engine.stats().panes_built, 6);

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

/// A planned query's raw-range reads carry the caller's auth and tenancy
/// headers, as a forwarded query does: a Prometheus that requires them
/// serves the read, and the plan answers.
#[tokio::test]
async fn raw_range_reads_forward_auth_and_tenancy_headers() {
    let labels = series("http_requests_total", &[("job", "a"), ("instance", "1")]);
    let points = (0..10 * MINUTE)
        .step_by(15_000)
        .map(|t| (labels.clone(), t, t as f64 / 1000.0))
        .collect();
    let required = [
        ("authorization", "Bearer secret"),
        ("x-scope-orgid", "tenant-a"),
    ];
    let state = DataPlane::new(
        common::plan_json("example1", true),
        fake_prometheus_requiring(points, &required).await,
        0,
    )
    .unwrap();
    let app = router(Arc::new(state));
    let uri = format!(
        "/api/v1/query?{}",
        form_urlencoded::Serializer::new(String::new())
            .append_pair("query", "sum by (job) (rate(http_requests_total[1m]))")
            .append_pair("time", "300")
            .finish()
    );
    let request = |with_headers: bool| {
        let mut request = Request::get(uri.clone());
        if with_headers {
            request = request
                .header("authorization", "Bearer secret")
                .header("x-scope-orgid", "tenant-a");
        }
        request.body(Body::empty()).unwrap()
    };
    let response = app.clone().oneshot(request(true)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(EXECUTION_HEADER).unwrap(),
        "plan",
        "the raw read was authorized"
    );
    // Without them the read is refused, so the query is forwarded, and
    // Prometheus refuses that too.
    let response = app.clone().oneshot(request(false)).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response.headers().get(EXECUTION_HEADER).unwrap(),
        "forwarded"
    );
    assert!(response
        .headers()
        .get(FORWARD_REASON_HEADER)
        .unwrap()
        .to_str()
        .unwrap()
        .contains("fetching raw samples"));
}

/// A planned query at a time Prometheus rejects (not finite, or beyond the
/// millisecond range) gets 400 like Prometheus, and the engine keeps
/// running.
#[tokio::test]
async fn rejects_evaluation_times_out_of_range() {
    let state = Arc::new(
        DataPlane::new(
            common::plan_json("example4b", true),
            fake_prometheus(vec![]).await,
            0,
        )
        .unwrap(),
    );
    let app = router(state.clone());
    for time in ["-1e300", "1e300", "NaN", "inf"] {
        let uri = format!(
            "/api/v1/query?{}",
            form_urlencoded::Serializer::new(String::new())
                .append_pair("query", "quantile_over_time(0.99, latency_ms[60m])")
                .append_pair("time", time)
                .finish()
        );
        let response = app
            .clone()
            .oneshot(Request::get(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "time={time}");
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["errorType"], "bad_data", "time={time}");
    }
    assert!(state.engine.is_running());
}
