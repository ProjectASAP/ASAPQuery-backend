mod common;

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::RawQuery;
use axum::http::{header, HeaderMap, Method, Request, StatusCode};
use axum::routing::get;
use axum::Router;
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
