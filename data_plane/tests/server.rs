mod common;

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::RawQuery;
use axum::http::{header, HeaderMap, Method, Request, StatusCode};
use axum::routing::get;
use axum::Router;
use data_plane::engine::EngineOptions;
use data_plane::remote_write::{encode, Label, Sample, TimeSeries, WriteRequest};
use data_plane::{router, DataPlane, EXECUTION_HEADER};
use http_body_util::BodyExt;
use tower::ServiceExt;

/// A stand-in Prometheus that echoes the method, query string and body it
/// received, so tests can check what was forwarded.
async fn fake_prometheus() -> String {
    serve(echo_router()).await
}

fn echo_router() -> Router {
    async fn echo(method: Method, RawQuery(query): RawQuery, body: Bytes) -> String {
        format!(
            "{method} {} {}",
            query.unwrap_or_default(),
            String::from_utf8_lossy(&body)
        )
    }
    Router::new()
        .route("/api/v1/query", get(echo).post(echo))
        .route("/api/v1/query_range", get(echo).post(echo))
}

async fn serve(app: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    url
}

/// A data plane for #509 Example 1 that forwards to `prometheus_url`.
fn data_plane(prometheus_url: String) -> Arc<DataPlane> {
    Arc::new(DataPlane::new(common::plan_json("example1", true), prometheus_url, 0).unwrap())
}

async fn send(app: &Router, request: Request<Body>) -> (StatusCode, Option<String>, String) {
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let execution = response
        .headers()
        .get(EXECUTION_HEADER)
        .map(|v| v.to_str().unwrap().to_string());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, execution, String::from_utf8(body.to_vec()).unwrap())
}

/// Instant and range queries, by GET or form POST, reach Prometheus
/// unchanged, and the response says it was forwarded.
#[tokio::test]
async fn forwards_queries_to_prometheus() {
    let state = data_plane(fake_prometheus().await);
    let app = router(state);
    let (status, execution, body) = send(
        &app,
        Request::get("/api/v1/query?query=up&time=10")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(execution.as_deref(), Some("forwarded"));
    assert_eq!(body, "GET query=up&time=10 ");
    let (_, execution, body) = send(
        &app,
        Request::post("/api/v1/query_range")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from("query=up&start=0&end=60&step=15"))
            .unwrap(),
    )
    .await;
    assert_eq!(execution.as_deref(), Some("forwarded"));
    assert_eq!(body, "POST  query=up&start=0&end=60&step=15");
}

/// Valid Remote Write requests are accepted and counted; malformed ones are
/// rejected with 400.
#[tokio::test]
async fn accepts_remote_write() {
    let state = data_plane("http://unused".into());
    let app = router(state.clone());
    let request = WriteRequest {
        timeseries: vec![TimeSeries {
            labels: vec![Label {
                name: "__name__".into(),
                value: "http_requests_total".into(),
            }],
            samples: vec![
                Sample {
                    value: 1.0,
                    timestamp: 1_000,
                },
                Sample {
                    value: 2.0,
                    timestamp: 16_000,
                },
            ],
        }],
    };
    let (status, _, _) = send(
        &app,
        Request::post("/api/v1/write")
            .body(Body::from(encode(&request)))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(state.samples.load(Ordering::Relaxed), 2);
    let (status, _, _) = send(
        &app,
        Request::post("/api/v1/write")
            .body(Body::from("not snappy"))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(state.samples.load(Ordering::Relaxed), 2);
}

/// Remote Write bodies above axum's 2 MB default but within
/// `Limits::max_compressed_bytes` are accepted; larger ones get 413.
#[tokio::test]
async fn accepts_remote_write_up_to_the_compressed_limit() {
    let state = data_plane("http://unused".into());
    let app = router(state.clone());
    // Pseudo-random hex label values do not compress, so the encoded body
    // stays near its decoded size.
    let mut seed: u64 = 1;
    let timeseries = (0..20_000)
        .map(|_| {
            let value: String = (0..128)
                .map(|_| {
                    seed = seed
                        .wrapping_mul(6_364_136_223_846_793_005)
                        .wrapping_add(1_442_695_040_888_963_407);
                    char::from_digit((seed >> 60) as u32, 16).unwrap()
                })
                .collect();
            TimeSeries {
                labels: vec![
                    Label {
                        name: "__name__".into(),
                        value: "m".into(),
                    },
                    Label {
                        name: "id".into(),
                        value,
                    },
                ],
                samples: vec![Sample {
                    value: 1.0,
                    timestamp: 1_000,
                }],
            }
        })
        .collect();
    let body = encode(&WriteRequest { timeseries });
    assert!(body.len() > 2 * 1024 * 1024, "body is {} bytes", body.len());
    assert!(body.len() < state.limits.max_compressed_bytes);
    let (status, _, error) = send(
        &app,
        Request::post("/api/v1/write")
            .body(Body::from(body))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{error}");
    assert_eq!(state.samples.load(Ordering::Relaxed), 20_000);

    let oversized = vec![0u8; state.limits.max_compressed_bytes + 1];
    let (status, _, _) = send(
        &app,
        Request::post("/api/v1/write")
            .body(Body::from(oversized))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(state.samples.load(Ordering::Relaxed), 20_000);
}

/// Auth and tenancy headers reach Prometheus; hop-by-hop headers and `Host`
/// do not.
#[tokio::test]
async fn forwards_auth_and_tenancy_headers() {
    async fn headers(headers: HeaderMap) -> String {
        let mut names: Vec<String> = headers
            .iter()
            .map(|(name, value)| format!("{name}={}", value.to_str().unwrap()))
            .collect();
        names.sort();
        names.join("\n")
    }
    let url = serve(Router::new().route("/api/v1/query", get(headers))).await;
    let app = router(data_plane(url));
    let (status, _, body) = send(
        &app,
        Request::get("/api/v1/query?query=up")
            .header(header::AUTHORIZATION, "Bearer secret")
            .header("x-scope-orgid", "tenant-a")
            .header(header::HOST, "asap.example")
            .header(header::CONNECTION, "keep-alive")
            .header("keep-alive", "timeout=5")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let received: Vec<&str> = body.lines().collect();
    assert!(received.contains(&"authorization=Bearer secret"), "{body}");
    assert!(received.contains(&"x-scope-orgid=tenant-a"), "{body}");
    assert!(
        !received.iter().any(|h| h.starts_with("host=asap.example")),
        "{body}"
    );
    assert!(
        !received.iter().any(|h| h.starts_with("keep-alive=")),
        "{body}"
    );
}

/// A forwarded query that outlives the timeout fails with 502.
#[tokio::test]
async fn forwarded_query_times_out() {
    async fn slow() -> &'static str {
        tokio::time::sleep(Duration::from_secs(5)).await;
        "late"
    }
    let url = serve(Router::new().route("/api/v1/query", get(slow))).await;
    let app = router(Arc::new(
        DataPlane::new(common::plan_json("example1", true), url, 0)
            .unwrap()
            .with_forward_timeout(Duration::from_millis(100)),
    ));
    let (status, _, body) = send(
        &app,
        Request::get("/api/v1/query?query=up")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
}

/// A Remote Write body of `count` samples of one Example 1 series.
fn write_body(count: usize) -> Vec<u8> {
    encode(&WriteRequest {
        timeseries: vec![TimeSeries {
            labels: vec![Label {
                name: "__name__".into(),
                value: "http_requests_total".into(),
            }],
            samples: (0..count)
                .map(|i| Sample {
                    value: i as f64,
                    timestamp: 1_000 * (i as i64 + 1),
                })
                .collect(),
        }],
    })
}

async fn write(app: &Router, count: usize) -> StatusCode {
    let request = Request::post("/api/v1/write")
        .body(Body::from(write_body(count)))
        .unwrap();
    send(app, request).await.0
}

/// GET `path` and parse the JSON body.
async fn get_json(app: &Router, path: &str) -> (StatusCode, serde_json::Value) {
    let (status, _, body) = send(app, Request::get(path).body(Body::empty()).unwrap()).await;
    (status, serde_json::from_str(&body).unwrap_or_default())
}

/// Remote Write that would queue more samples than `max_queued_samples` gets
/// 503 and is not counted; a request that fits is accepted.
#[tokio::test]
async fn refuses_remote_write_beyond_the_sample_queue() {
    let options = EngineOptions {
        max_queued_samples: 2,
        ..EngineOptions::default()
    };
    let state = Arc::new(
        DataPlane::with_options(
            common::plan_json("example1", true),
            "http://unused".into(),
            options,
        )
        .unwrap(),
    );
    let app = router(state.clone());
    assert_eq!(write(&app, 3).await, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(state.samples.load(Ordering::Relaxed), 0);
    assert_eq!(write(&app, 2).await, StatusCode::NO_CONTENT);
    assert_eq!(state.samples.load(Ordering::Relaxed), 2);
}

/// Once the engine thread has stopped, Remote Write gets 500 instead of a
/// 503 that senders would retry forever, and the data plane is not ready.
#[tokio::test]
async fn stopped_engine_fails_writes_and_readiness() {
    let state = data_plane("http://unused".into());
    let app = router(state.clone());
    let (status, _, _) = send(&app, Request::get("/-/ready").body(Body::empty()).unwrap()).await;
    assert_eq!(status, StatusCode::OK);
    state.engine.stop();
    for _ in 0..200 {
        if !state.engine.is_running() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!state.engine.is_running());
    assert_eq!(write(&app, 1).await, StatusCode::INTERNAL_SERVER_ERROR);
    let (status, _, _) = send(&app, Request::get("/-/ready").body(Body::empty()).unwrap()).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let (_, body) = get_json(&app, "/api/v1/status/asap").await;
    assert_eq!(body["engine_running"], false);
}

/// The status endpoint reports what the engine did with every sample.
#[tokio::test]
async fn reports_sample_counters() {
    let state = data_plane("http://unused".into());
    let app = router(state.clone());
    assert_eq!(write(&app, 3).await, StatusCode::NO_CONTENT);
    let mut body = serde_json::Value::Null;
    for _ in 0..200 {
        let (status, json) = get_json(&app, "/api/v1/status/asap").await;
        assert_eq!(status, StatusCode::OK);
        body = json;
        if body["samples_queued"] == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(body["engine_running"], true);
    assert_eq!(body["samples_queued"], 0);
    let counters = &body["precompute"];
    let total: u64 = [
        "samples_accepted",
        "samples_dropped_late",
        "samples_skipped_first_pane",
        "samples_dropped_future",
        "samples_unrouted",
    ]
    .iter()
    .map(|name| counters[name].as_u64().unwrap())
    .sum();
    assert_eq!(total, 3, "{body}");
}
