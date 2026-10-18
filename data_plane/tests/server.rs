use std::sync::atomic::Ordering;
use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::extract::RawQuery;
use axum::http::{header, Method, Request, StatusCode};
use axum::routing::get;
use axum::Router;
use data_plane::remote_write::{encode, Label, Sample, TimeSeries, WriteRequest};
use data_plane::{router, DataPlane, EXECUTION_HEADER};
use http_body_util::BodyExt;
use tower::ServiceExt;

/// A stand-in Prometheus that echoes the method, query string and body it
/// received, so tests can check what was forwarded.
async fn fake_prometheus() -> String {
    async fn echo(method: Method, RawQuery(query): RawQuery, body: Bytes) -> String {
        format!(
            "{method} {} {}",
            query.unwrap_or_default(),
            String::from_utf8_lossy(&body)
        )
    }
    let app = Router::new()
        .route("/api/v1/query", get(echo).post(echo))
        .route("/api/v1/query_range", get(echo).post(echo));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    url
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
    let state = Arc::new(DataPlane::new(vec![], fake_prometheus().await));
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
    let state = Arc::new(DataPlane::new(vec![], "http://unused".into()));
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
