//! The ClickHouse (`/clickhouse/query`) and Elasticsearch (`/_sql`,
//! `/_search`) query routes: each request goes through the dispatcher,
//! and what the plan does not answer reaches that protocol's server
//! through its `common::FallbackClient`. The control plane plans PromQL
//! only, so every such query is forwarded, even one whose text is a
//! planned PromQL query.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use axum::extract::{OriginalUri, State};
use axum::http::{header, HeaderMap, Request, StatusCode};
use axum::routing::post;
use axum::Router;
use clickhouse_http::fallback::ClickHouseHttpFallback;
use data_plane::engine::EngineOptions;
use data_plane::{router, DataPlane, EXECUTION_HEADER};
use elastic_http::fallback::ElasticHttpFallback;
use http_body_util::BodyExt;
use precompute::ingestion::IngestionOptions;
use prometheus_remote_write::{Label, Sample, TimeSeries};
use serde_json::{json, Value};
use tower::ServiceExt;

const QUERY: &str = "sum by (job) (sum_over_time(data[1m]))";

/// What the mock server received: path and query, `Authorization`, body.
type Received = Arc<Mutex<Vec<(String, Option<String>, String)>>>;

/// One server standing in for ClickHouse (`POST /`: a TabSeparated row, or
/// 404 with ClickHouse's exception for a query on `missing`) and for
/// Elasticsearch (`/_sql`, `/<index>/_search`: JSON naming the path). It
/// records each request; its base URL.
async fn upstream(received: Received) -> String {
    async fn record(
        State(received): State<Received>,
        OriginalUri(uri): OriginalUri,
        headers: HeaderMap,
        body: String,
    ) -> (StatusCode, [(header::HeaderName, &'static str); 1], String) {
        let authorization = headers
            .get(header::AUTHORIZATION)
            .map(|v| v.to_str().unwrap().to_string());
        let answer = if uri.path() != "/" {
            let body = format!("{{\"path\":\"{}\"}}", uri.path());
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json")],
                body,
            )
        } else if body.contains("missing") {
            let exception = "Code: 60. DB::Exception: Table default.missing does not exist.";
            (
                StatusCode::NOT_FOUND,
                [(header::CONTENT_TYPE, "text/plain; charset=UTF-8")],
                exception.into(),
            )
        } else {
            let tsv = "text/tab-separated-values; charset=UTF-8";
            (StatusCode::OK, [(header::CONTENT_TYPE, tsv)], "1\n".into())
        };
        received
            .lock()
            .unwrap()
            .push((uri.to_string(), authorization, body));
        answer
    }
    let app = Router::new()
        .route("/", post(record))
        .route("/_sql", post(record))
        .route("/:index/_search", post(record))
        .with_state(received);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{address}")
}

/// Forwarding allowed, every window refreshed.
fn options() -> EngineOptions {
    EngineOptions {
        ingestion: IngestionOptions {
            allowed_lateness_ms: 0,
            stream_start_ms: Some(0),
            ..IngestionOptions::default()
        },
        ..EngineOptions::default()
    }
}

fn data_plane() -> DataPlane {
    DataPlane::with_options(
        common::one_query_plan_json(QUERY, 2, json!({})),
        "http://127.0.0.1:1".into(),
        options(),
    )
    .unwrap()
}

/// A data plane serving both protocols, forwarding to `url`.
fn app(url: &str) -> Router {
    let timeout = Duration::from_secs(5);
    let state = data_plane()
        .with_clickhouse_fallback(ClickHouseHttpFallback::new(url, "metrics", timeout))
        .unwrap()
        .with_elastic_fallback(ElasticHttpFallback::new(url, "logs", timeout))
        .unwrap();
    router(Arc::new(state))
}

/// The response's status, `Content-Type`, `x-asap-execution` and body.
async fn send(
    app: &Router,
    request: Request<Body>,
) -> (StatusCode, Option<String>, Option<String>, Vec<u8>) {
    let response = app.clone().oneshot(request).await.unwrap();
    let header = |name: &str| {
        response
            .headers()
            .get(name)
            .map(|v| v.to_str().unwrap().to_string())
    };
    let (content_type, execution) = (header("content-type"), header(EXECUTION_HEADER));
    assert!(response.headers().get("x-asap-forward-reason").is_none());
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, content_type, execution, body.to_vec())
}

fn json_post(uri: &str, body: &str) -> Request<Body> {
    Request::post(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "ApiKey k")
        .body(Body::from(body.to_string()))
        .unwrap()
}

/// A ClickHouse query, from the URL query, a form or a plain SQL body, is
/// posted to
/// ClickHouse with the client's `Authorization`, and its answer, an
/// exception included, is returned unchanged; a request without a query
/// is refused without forwarding.
#[tokio::test]
async fn clickhouse_queries_are_answered_by_clickhouse() {
    let received = Received::default();
    let app = app(&upstream(received.clone()).await);
    let get = Request::get("/clickhouse/query?query=SELECT%201")
        .header(header::AUTHORIZATION, "Basic dTpw")
        .body(Body::empty())
        .unwrap();
    let tsv = Some("text/tab-separated-values; charset=UTF-8".to_string());
    let forwarded = Some("forwarded".to_string());
    assert_eq!(
        send(&app, get).await,
        (
            StatusCode::OK,
            tsv.clone(),
            forwarded.clone(),
            b"1\n".to_vec()
        )
    );
    let form = Request::post("/clickhouse/query")
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .body(Body::from("query=SELECT+*+FROM+missing"))
        .unwrap();
    let (status, _, execution, body) = send(&app, form).await;
    assert_eq!((status, execution), (StatusCode::NOT_FOUND, forwarded));
    assert!(body.starts_with(b"Code: 60. DB::Exception"));
    let plain = Request::post("/clickhouse/query")
        .body(Body::from("SELECT count() FROM t WHERE a=1"))
        .unwrap();
    assert_eq!(send(&app, plain).await.0, StatusCode::OK);
    let no_query = Request::get("/clickhouse/query")
        .body(Body::empty())
        .unwrap();
    let (status, _, _, body) = send(&app, no_query).await;
    assert_eq!(
        (status, body),
        (
            StatusCode::BAD_REQUEST,
            b"missing parameter \"query\"".to_vec()
        )
    );
    assert_eq!(
        *received.lock().unwrap(),
        [
            (
                "/?database=metrics".to_string(),
                Some("Basic dTpw".to_string()),
                "SELECT 1".to_string()
            ),
            (
                "/?database=metrics".to_string(),
                None,
                "SELECT * FROM missing".to_string()
            ),
            (
                "/?database=metrics".to_string(),
                None,
                "SELECT count() FROM t WHERE a=1".to_string()
            ),
        ]
    );
}

/// With the plan's views stored, the planned PromQL query is answered
/// from them on the Prometheus route, but the same text on the ClickHouse
/// route is not the plan's query: it is forwarded to ClickHouse.
#[tokio::test]
async fn a_planned_promql_text_is_not_answered_as_clickhouse_sql() {
    let received = Received::default();
    let app = app(&upstream(received.clone()).await);
    let series = |job: &str| TimeSeries {
        labels: [("__name__", "data"), ("job", job)]
            .into_iter()
            .map(|(name, value)| Label {
                name: name.into(),
                value: value.into(),
            })
            .collect(),
        samples: (1..=120)
            .map(|second| Sample {
                timestamp: second * 1000,
                value: 1.0,
            })
            .collect(),
    };
    common::remote_write(&app, vec![series("a"), series("b")]).await;
    common::barrier(&app, 120_000).await;
    let (status, execution, body) = common::get(&app, &common::instant_uri(QUERY, 120)).await;
    assert_eq!(
        (status, execution.as_deref()),
        (StatusCode::OK, Some("plan"))
    );
    assert_eq!(
        common::per_job(&body),
        [("a".to_string(), 60.0), ("b".to_string(), 60.0)]
    );
    let uri = format!(
        "/clickhouse/query?query={}",
        form_urlencoded::byte_serialize(QUERY.as_bytes()).collect::<String>()
    );
    let (status, _, execution, _) =
        send(&app, Request::get(uri).body(Body::empty()).unwrap()).await;
    assert_eq!(
        (status, execution.as_deref()),
        (StatusCode::OK, Some("forwarded"))
    );
    assert_eq!(received.lock().unwrap()[0].2, QUERY);
}

/// An Elasticsearch SQL query is posted to `/_sql` and a Query DSL search
/// to the index's `_search`, as sent, with the client's `Authorization`;
/// a body that does not parse is 400 `parsing_exception` and a `GET` 400
/// `illegal_argument_exception` (ASAPQuery's error bodies, sent with HTTP
/// 400 as Elasticsearch sends them), without forwarding.
#[tokio::test]
async fn elasticsearch_queries_are_answered_by_elasticsearch() {
    let received = Received::default();
    let app = app(&upstream(received.clone()).await);
    let json_type = Some("application/json".to_string());
    let forwarded = Some("forwarded".to_string());
    let sql = json_post("/_sql", r#"{"query": "SELECT COUNT(*) FROM logs;"}"#);
    assert_eq!(
        send(&app, sql).await,
        (
            StatusCode::OK,
            json_type.clone(),
            forwarded.clone(),
            br#"{"path":"/_sql"}"#.to_vec()
        )
    );
    let search = r#"{"query": {"match_all": {}}}"#;
    let (status, _, execution, body) = send(&app, json_post("/_search", search)).await;
    assert_eq!(
        (status, execution, body),
        (
            StatusCode::OK,
            forwarded,
            br#"{"path":"/logs/_search"}"#.to_vec()
        )
    );
    for (request, error_type) in [
        (json_post("/_search", "not json"), "parsing_exception"),
        (json_post("/_sql", "{}"), "parsing_exception"),
        (
            Request::get("/_search").body(Body::empty()).unwrap(),
            "illegal_argument_exception",
        ),
    ] {
        let (status, _, execution, body) = send(&app, request).await;
        assert_eq!((status, execution), (StatusCode::BAD_REQUEST, None));
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            (&body["error"]["type"], &body["status"]),
            (&json!(error_type), &json!(400))
        );
    }
    let received = received.lock().unwrap();
    assert_eq!(received.len(), 2);
    let (path, authorization, body) = &received[0];
    assert_eq!(
        (path.as_str(), authorization.as_deref()),
        ("/_sql", Some("ApiKey k"))
    );
    assert_eq!(
        serde_json::from_str::<Value>(body).unwrap(),
        json!({"query": "SELECT COUNT(*) FROM logs", "fetch_size": 1000})
    );
    assert_eq!(received[1].0, "/logs/_search");
    assert_eq!(received[1].2, search);
}

/// Without a server for a protocol its routes are not served (404, as
/// before); `--no-forward` refuses either server; an unreachable server is
/// 502.
#[tokio::test]
async fn protocol_routes_need_their_server() {
    let without = router(Arc::new(data_plane()));
    for uri in ["/clickhouse/query?query=SELECT%201", "/_sql", "/_search"] {
        let request = Request::post(uri).body(Body::empty()).unwrap();
        assert_eq!(
            send(&without, request).await.0,
            StatusCode::NOT_FOUND,
            "{uri}"
        );
    }
    let refusing = || {
        DataPlane::with_options(
            common::one_query_plan_json(QUERY, 2, json!({})),
            "http://127.0.0.1:1".into(),
            common::no_forward_options(),
        )
        .unwrap()
    };
    let timeout = Duration::from_secs(5);
    let clickhouse = ClickHouseHttpFallback::new("http://127.0.0.1:1", "default", timeout);
    assert!(refusing().with_clickhouse_fallback(clickhouse).is_err());
    let elastic = ElasticHttpFallback::new("http://127.0.0.1:1", "", timeout);
    assert!(refusing().with_elastic_fallback(elastic).is_err());
    let app = app("http://127.0.0.1:1");
    let request = Request::get("/clickhouse/query?query=SELECT%201")
        .body(Body::empty())
        .unwrap();
    let (status, _, execution, body) = send(&app, request).await;
    assert_eq!((status, execution), (StatusCode::BAD_GATEWAY, None));
    assert!(body.starts_with(b"forwarding to ClickHouse: "));
}

/// In forward mode a protocol's routes still go to its own server, and
/// every other path is still proxied to the raw store.
#[tokio::test]
async fn forward_mode_keeps_protocol_routes_and_proxies_the_rest() {
    let received = Received::default();
    let url = upstream(received.clone()).await;
    let state = data_plane()
        .with_raw_forward(data_plane::raw_forward::RawForwardOptions::new(url.clone()))
        .unwrap()
        .with_elastic_fallback(ElasticHttpFallback::new(
            &url,
            "logs",
            Duration::from_secs(5),
        ))
        .unwrap();
    let app = router(Arc::new(state));
    let search = r#"{"query": {"match_all": {}}}"#;
    let (status, _, execution, body) = send(&app, json_post("/_search", search)).await;
    assert_eq!(
        (status, execution.as_deref(), body),
        (
            StatusCode::OK,
            Some("forwarded"),
            br#"{"path":"/logs/_search"}"#.to_vec()
        )
    );
    // The mock has no such route: its 404, proxied.
    let labels = Request::get("/api/v1/labels").body(Body::empty()).unwrap();
    let (status, _, execution, _) = send(&app, labels).await;
    assert_eq!(
        (status, execution.as_deref()),
        (StatusCode::NOT_FOUND, Some("forwarded"))
    );
}
