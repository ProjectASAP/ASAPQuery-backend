//! Forward mode (`--raw-store-url`, docs/design/demo-plan.md §10.1, tests
//! F1–F8): accepted Remote Write bodies are delivered unchanged to a raw
//! store (VictoriaMetrics in the demo, a recording mock here); queries the
//! plan does not answer, and any other path, are proxied to it with
//! `x-asap-execution: forwarded`; a full forward queue refuses writes with
//! 503 before the engine sees them, so retries duplicate nothing; failed
//! deliveries are retried until the raw store takes them. Off, the data
//! plane behaves as before.

mod common;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::{Body, Bytes};
use axum::extract::{OriginalUri, State};
use axum::http::{HeaderMap, Request, StatusCode};
use axum::response::IntoResponse;
use axum::Router;
use control_plane::plan_workload;
use data_plane::engine::EngineOptions;
use data_plane::ingestion::IngestionOptions;
use data_plane::raw_forward::RawForwardOptions;
use data_plane::remote_write::{encode, Label, Sample, TimeSeries, WriteRequest};
use data_plane::{router, DataPlane, EXECUTION_HEADER};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tokio::sync::Notify;
use tower::ServiceExt;

const QUERY: &str = "sum by (job) (sum_over_time(data[30s]))";

fn plan_json() -> String {
    let workload = serde_json::from_value(json!({
        "query_workload": {"language": "promql", "repeating_queries": [{
            "query": QUERY,
            "demand": {"fixed_interval_at": {"interval": 10000, "evaluation_phase": 0}},
            "requirements": {},
            "predictability": {"predictable": {"known_at": null}},
        }]},
        "data_workload": {"arrival": {"ingesting": {"rows_per_second": 8.0}},
          "sources": {"data": {"series_count": 8, "sample_interval": 1000,
            "metric_type": "gauge", "column_signs": {"value": "non_negative"}}}},
    }))
    .unwrap();
    serde_json::to_string(&plan_workload(&workload, &common::inputs(false)).unwrap().0).unwrap()
}

fn options() -> EngineOptions {
    EngineOptions {
        ingest_threads: 2,
        ingestion: IngestionOptions {
            allowed_lateness_ms: 0,
            stream_start_ms: Some(0),
            ..IngestionOptions::default()
        },
        ..EngineOptions::default()
    }
}

/// A data plane; with `raw_store`, in forward mode to it.
fn data_plane(raw_store: Option<RawForwardOptions>) -> (Router, Arc<DataPlane>) {
    let state =
        DataPlane::with_options(plan_json(), "http://127.0.0.1:1".into(), options()).unwrap();
    let state = match raw_store {
        Some(options) => state.with_raw_forward(options).unwrap(),
        None => state,
    };
    let state = Arc::new(state);
    (router(state.clone()), state)
}

/// What the mock raw store does with a Remote Write body.
#[derive(Clone, Copy, Debug)]
enum Answer {
    Accept,
    /// Answer this status this many more times, then accept.
    Fail(StatusCode, usize),
    /// Hold the request until `release` is notified.
    Hang,
}

/// A raw store that records what it gets.
struct Mock {
    answer: Mutex<Answer>,
    release: Notify,
    /// Each Remote Write body and its `Content-Encoding`, accepted or not.
    writes: Mutex<Vec<(Bytes, String)>>,
    /// Bodies accepted (2xx).
    accepted: Mutex<Vec<Bytes>>,
    /// Each other request: path and query.
    reads: Mutex<Vec<String>>,
}

async fn mock_write(
    State(mock): State<Arc<Mock>>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let encoding = headers
        .get("content-encoding")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    mock.writes.lock().unwrap().push((body.clone(), encoding));
    let answer = *mock.answer.lock().unwrap();
    match answer {
        Answer::Accept => {
            mock.accepted.lock().unwrap().push(body);
            StatusCode::NO_CONTENT
        }
        Answer::Fail(status, left) => {
            *mock.answer.lock().unwrap() = if left > 1 {
                Answer::Fail(status, left - 1)
            } else {
                Answer::Accept
            };
            status
        }
        Answer::Hang => {
            mock.release.notified().await;
            mock.accepted.lock().unwrap().push(body);
            StatusCode::NO_CONTENT
        }
    }
}

async fn mock_read(
    State(mock): State<Arc<Mock>>,
    OriginalUri(uri): OriginalUri,
) -> impl IntoResponse {
    mock.reads.lock().unwrap().push(uri.to_string());
    axum::Json(
        json!({"status": "success", "data": {"answered_by": "raw store", "path": uri.path()}}),
    )
}

/// Serves a mock raw store on `listener`.
fn serve_mock(listener: tokio::net::TcpListener, answer: Answer) -> Arc<Mock> {
    let mock = Arc::new(Mock {
        answer: Mutex::new(answer),
        release: Notify::new(),
        writes: Mutex::new(Vec::new()),
        accepted: Mutex::new(Vec::new()),
        reads: Mutex::new(Vec::new()),
    });
    let app = Router::new()
        .route("/api/v1/write", axum::routing::post(mock_write))
        .fallback(mock_read)
        .with_state(mock.clone());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    mock
}

async fn mock(answer: Answer) -> (Arc<Mock>, String) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    (serve_mock(listener, answer), url)
}

/// Forward options for `url` with a short retry path for tests.
fn forward_to(url: &str) -> RawForwardOptions {
    let mut options = RawForwardOptions::new(url);
    options.timeout = Duration::from_secs(5);
    options
}

/// The Remote Write body of series `0..8` at `seconds`.
fn body(seconds: std::ops::RangeInclusive<i64>) -> Vec<u8> {
    let timeseries = (0..8)
        .map(|i| TimeSeries {
            labels: [
                ("__name__", "data".to_string()),
                ("instance", format!("i{i}")),
                ("job", format!("job{}", i % 4)),
            ]
            .into_iter()
            .map(|(name, value)| Label {
                name: name.into(),
                value,
            })
            .collect(),
            samples: seconds
                .clone()
                .map(|second| Sample {
                    timestamp: second * 1000,
                    value: (second * (i + 1) % 17) as f64,
                })
                .collect(),
        })
        .collect();
    encode(&WriteRequest { timeseries })
}

async fn send(app: &Router, request: Request<Body>) -> (StatusCode, HeaderMap, Bytes) {
    let response = app.clone().oneshot(request).await.unwrap();
    let (status, headers) = (response.status(), response.headers().clone());
    (
        status,
        headers,
        response.into_body().collect().await.unwrap().to_bytes(),
    )
}

async fn write(app: &Router, body: Vec<u8>) -> StatusCode {
    send(
        app,
        Request::post("/api/v1/write")
            .body(Body::from(body))
            .unwrap(),
    )
    .await
    .0
}

async fn get(app: &Router, uri: &str) -> (StatusCode, HeaderMap, Value) {
    let (status, headers, body) = send(app, Request::get(uri).body(Body::empty()).unwrap()).await;
    (
        status,
        headers,
        serde_json::from_slice(&body).unwrap_or(Value::Null),
    )
}

async fn barrier(app: &Router, time_ms: i64) {
    let request = Request::post(format!("/api/v1/precompute/watermark?time_ms={time_ms}"))
        .body(Body::empty())
        .unwrap();
    assert_eq!(send(app, request).await.0, StatusCode::OK);
}

async fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
    let started = Instant::now();
    while !done() {
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "timed out: {what}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn planned_query_uri(time_s: i64) -> String {
    format!(
        "/api/v1/query?query={}&time={time_s}",
        form_urlencoded::byte_serialize(QUERY.as_bytes()).collect::<String>()
    )
}

/// F1. Off: no other path is served (404), writes and planned queries are
/// as before, and no forward counter exists.
#[tokio::test(flavor = "multi_thread")]
async fn off_behaves_as_before() {
    let (app, state) = data_plane(None);
    assert_eq!(write(&app, body(1..=40)).await, StatusCode::NO_CONTENT);
    barrier(&app, 40_000).await;
    let (status, headers, _) = get(&app, &planned_query_uri(30)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers[EXECUTION_HEADER], "plan");
    let (status, _, _) = get(&app, "/api/v1/labels").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(state.raw_forward.is_none());
    let (_, _, metrics) = send(&app, Request::get("/metrics").body(Body::empty()).unwrap()).await;
    assert!(!String::from_utf8_lossy(&metrics).contains("asap_raw_forward"));
}

/// F2. Every accepted Remote Write body reaches the raw store byte for byte
/// (snappy-encoded), and the data plane answers as it does with forwarding
/// off.
#[tokio::test(flavor = "multi_thread")]
async fn accepted_writes_reach_the_raw_store_unchanged() {
    let (raw, url) = mock(Answer::Accept).await;
    let (app, state) = data_plane(Some(forward_to(&url)));
    let (off, _) = data_plane(None);
    let bodies = [body(1..=20), body(21..=40)];
    for b in &bodies {
        assert_eq!(write(&app, b.clone()).await, StatusCode::NO_CONTENT);
        assert_eq!(write(&off, b.clone()).await, StatusCode::NO_CONTENT);
    }
    wait_for("both bodies delivered", || {
        raw.accepted.lock().unwrap().len() == 2
    })
    .await;
    let accepted = raw.accepted.lock().unwrap().clone();
    assert_eq!(
        accepted,
        bodies
            .iter()
            .map(|b| Bytes::from(b.clone()))
            .collect::<Vec<_>>()
    );
    assert!(raw
        .writes
        .lock()
        .unwrap()
        .iter()
        .all(|(_, encoding)| encoding == "snappy"));
    barrier(&app, 40_000).await;
    barrier(&off, 40_000).await;
    for time_s in [30, 40] {
        let (_, headers, answer) = get(&app, &planned_query_uri(time_s)).await;
        assert_eq!(headers[EXECUTION_HEADER], "plan");
        assert_eq!(
            answer,
            get(&off, &planned_query_uri(time_s)).await.2,
            "at {time_s}"
        );
    }
    let status = state.raw_forward.as_ref().unwrap().status();
    assert_eq!(status["delivered_requests"], 2);
    assert_eq!(status["queued_bytes"], 0);
}

/// F3. Planned queries are answered by the plan (`plan`); unplanned queries
/// and every other path are proxied to the raw store (`forwarded`), with
/// their query string.
#[tokio::test(flavor = "multi_thread")]
async fn unplanned_queries_and_other_paths_are_proxied() {
    let (raw, url) = mock(Answer::Accept).await;
    let (app, _) = data_plane(Some(forward_to(&url)));
    assert_eq!(write(&app, body(1..=40)).await, StatusCode::NO_CONTENT);
    barrier(&app, 40_000).await;
    let (status, headers, _) = get(&app, &planned_query_uri(30)).await;
    assert_eq!(
        (status, &headers[EXECUTION_HEADER]),
        (StatusCode::OK, &"plan".parse().unwrap())
    );
    for uri in [
        "/api/v1/query?query=up&time=30",
        "/api/v1/query_range?query=up&start=0&end=30&step=10",
        "/api/v1/labels",
        "/api/v1/series?match%5B%5D=data",
        "/api/v1/label/job/values",
    ] {
        let (status, headers, answer) = get(&app, uri).await;
        assert_eq!(status, StatusCode::OK, "{uri}");
        assert_eq!(headers[EXECUTION_HEADER], "forwarded", "{uri}");
        assert_eq!(answer["data"]["answered_by"], "raw store", "{uri}");
    }
    let reads = raw.reads.lock().unwrap().clone();
    assert!(
        reads.contains(&"/api/v1/query?query=up&time=30".to_string()),
        "{reads:?}"
    );
    assert!(
        reads.contains(&"/api/v1/series?match%5B%5D=data".to_string()),
        "{reads:?}"
    );
}

/// F4. A delivery the raw store refuses with 503 or 429 is retried until it
/// is accepted, once.
#[tokio::test(flavor = "multi_thread")]
async fn refused_deliveries_are_retried_until_accepted() {
    for status in [
        StatusCode::SERVICE_UNAVAILABLE,
        StatusCode::TOO_MANY_REQUESTS,
    ] {
        let (raw, url) = mock(Answer::Fail(status, 2)).await;
        let (app, state) = data_plane(Some(forward_to(&url)));
        assert_eq!(write(&app, body(1..=10)).await, StatusCode::NO_CONTENT);
        wait_for("delivered after retries", || {
            raw.accepted.lock().unwrap().len() == 1
        })
        .await;
        let forwarder = state.raw_forward.as_ref().unwrap().status();
        assert_eq!(forwarder["delivered_requests"], 1, "{status}");
        assert_eq!(forwarder["retried_attempts"], 2, "{status}");
        assert_eq!(raw.writes.lock().unwrap().len(), 3, "{status}");
    }
}

/// F5. While the raw store is down, accepted bodies wait in the queue and
/// are delivered once it is up.
#[tokio::test(flavor = "multi_thread")]
async fn bodies_wait_for_a_raw_store_that_is_down() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let (app, state) = data_plane(Some(forward_to(&format!("http://{address}"))));
    assert_eq!(write(&app, body(1..=10)).await, StatusCode::NO_CONTENT);
    assert_eq!(write(&app, body(11..=20)).await, StatusCode::NO_CONTENT);
    let forwarder = state.raw_forward.as_ref().unwrap();
    wait_for("a failed attempt", || {
        forwarder.status()["retried_attempts"].as_u64() >= Some(1)
    })
    .await;
    assert_eq!(forwarder.status()["delivered_requests"], 0);
    let raw = serve_mock(
        tokio::net::TcpListener::bind(address).await.unwrap(),
        Answer::Accept,
    );
    wait_for("both delivered", || raw.accepted.lock().unwrap().len() == 2).await;
    assert_eq!(forwarder.status()["queued_bytes"], 0);
}

/// F6. Backpressure: with the queue full (the raw store holds every request),
/// a write is refused with 503 before the engine sees it; after the raw
/// store answers again, the retried write is accepted, and each store has
/// every sample exactly once.
#[tokio::test(flavor = "multi_thread")]
async fn a_full_queue_refuses_writes_without_duplicates() {
    let (raw, url) = mock(Answer::Hang).await;
    let mut options = forward_to(&url);
    options.max_queued_bytes = 1;
    options.concurrency = 1;
    let (app, state) = data_plane(Some(options));
    let (first, second) = (body(1..=10), body(11..=20));
    assert_eq!(write(&app, first.clone()).await, StatusCode::NO_CONTENT);
    assert_eq!(
        write(&app, second.clone()).await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    wait_for("the first body ingested", || {
        state.engine.stats().samples_accepted == 80
    })
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        state.engine.stats().samples_accepted,
        80,
        "the refused write reached the engine"
    );
    let forwarder = state.raw_forward.as_ref().unwrap();
    assert_eq!(forwarder.status()["refused_writes"], 1);
    *raw.answer.lock().unwrap() = Answer::Accept;
    raw.release.notify_one();
    wait_for("the first body delivered", || {
        raw.accepted.lock().unwrap().len() == 1
    })
    .await;
    wait_for("room in the queue", || {
        forwarder.status()["queued_bytes"] == 0
    })
    .await;
    assert_eq!(write(&app, second.clone()).await, StatusCode::NO_CONTENT);
    wait_for("both delivered", || raw.accepted.lock().unwrap().len() == 2).await;
    wait_for("both ingested", || {
        state.engine.stats().samples_accepted == 160
    })
    .await;
    assert_eq!(
        raw.accepted.lock().unwrap().clone(),
        vec![Bytes::from(first), Bytes::from(second)]
    );
}

/// F7. A body the raw store rejects for good (400) is dropped and counted,
/// and later bodies are still delivered.
#[tokio::test(flavor = "multi_thread")]
async fn rejected_bodies_are_dropped_and_counted() {
    let (raw, url) = mock(Answer::Fail(StatusCode::BAD_REQUEST, 1)).await;
    let mut options = forward_to(&url);
    options.concurrency = 1;
    let (app, state) = data_plane(Some(options));
    assert_eq!(write(&app, body(1..=10)).await, StatusCode::NO_CONTENT);
    let forwarder = state.raw_forward.as_ref().unwrap();
    wait_for("the first body dropped", || {
        forwarder.status()["dropped_requests"] == 1
    })
    .await;
    assert_eq!(write(&app, body(11..=20)).await, StatusCode::NO_CONTENT);
    wait_for("the second body delivered", || {
        raw.accepted.lock().unwrap().len() == 1
    })
    .await;
    let status = forwarder.status();
    assert_eq!(
        (
            status["dropped_requests"].as_u64(),
            status["retried_attempts"].as_u64()
        ),
        (Some(1), Some(0))
    );
    let (_, _, metrics) = send(&app, Request::get("/metrics").body(Body::empty()).unwrap()).await;
    let metrics = String::from_utf8_lossy(&metrics).to_string();
    assert!(
        metrics.contains("asap_raw_forward_requests_total{outcome=\"dropped\"} 1"),
        "{metrics}"
    );
    assert!(
        metrics.contains("asap_raw_forward_requests_total{outcome=\"delivered\"} 1"),
        "{metrics}"
    );
}

/// F8. Forward mode proxies unplanned queries, which `--no-forward` refuses:
/// the two cannot be combined.
#[tokio::test(flavor = "multi_thread")]
async fn forward_mode_refuses_no_forward() {
    let state = DataPlane::with_options(
        plan_json(),
        "http://127.0.0.1:1".into(),
        EngineOptions {
            no_forward: true,
            ..options()
        },
    )
    .unwrap();
    let error = match state.with_raw_forward(forward_to("http://127.0.0.1:1")) {
        Ok(_) => panic!("forward mode accepted --no-forward"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("--no-forward"), "{error}");
}
