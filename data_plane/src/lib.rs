//! Backend data plane: receives Prometheus Remote Write samples and serves
//! the Prometheus query API for an installed plan.
//!
//! The installed plan is ASAPPlanner's selected `PhysicalASAPDAG`. Executing
//! it is not implemented yet: Remote Write samples are validated, counted and
//! dropped (Prometheus must receive Remote Write itself), and every query is
//! forwarded to Prometheus.

pub mod remote_write;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, RawQuery, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;

/// Response header naming what answered a query.
pub const EXECUTION_HEADER: &str = "x-asap-execution";

/// How long a forwarded query may take by default, matching Prometheus's
/// default `--query.timeout` of two minutes.
pub const DEFAULT_FORWARD_TIMEOUT: Duration = Duration::from_secs(120);

pub struct DataPlane {
    /// The installed plan's queries, in root order. The plan itself is not
    /// `Send` (its operator payloads hold `Rc`s), so it stays with its loader.
    pub queries: Vec<String>,
    /// Base URL of the Prometheus that answers forwarded queries.
    pub prometheus_url: String,
    pub limits: remote_write::Limits,
    /// Samples accepted (and, until the precompute engine lands, dropped)
    /// since startup.
    pub samples: AtomicU64,
    client: reqwest::Client,
}

impl DataPlane {
    /// `forward_timeout` bounds each forwarded query, from sending the
    /// request to reading the whole response.
    pub fn new(queries: Vec<String>, prometheus_url: String, forward_timeout: Duration) -> Self {
        Self {
            queries,
            prometheus_url: prometheus_url.trim_end_matches('/').to_string(),
            limits: remote_write::Limits::default(),
            samples: AtomicU64::new(0),
            client: reqwest::Client::builder()
                .timeout(forward_timeout)
                .build()
                .expect("reqwest client with only a timeout set"),
        }
    }
}

pub fn router(state: Arc<DataPlane>) -> Router {
    // axum refuses bodies over 2 MB by default; Remote Write allows up to
    // `max_compressed_bytes` and answers larger bodies with 413.
    let write_body_limit = DefaultBodyLimit::max(state.limits.max_compressed_bytes);
    Router::new()
        .route("/api/v1/write", post(write).layer(write_body_limit))
        .route("/api/v1/query", get(query).post(query))
        .route("/api/v1/query_range", get(query_range).post(query_range))
        .route("/-/ready", get(|| async { StatusCode::OK }))
        .with_state(state)
}

async fn write(State(state): State<Arc<DataPlane>>, body: Bytes) -> Response {
    match remote_write::decode(&body, &state.limits) {
        Ok(request) => {
            let samples: usize = request.timeseries.iter().map(|s| s.samples.len()).sum();
            state.samples.fetch_add(samples as u64, Ordering::Relaxed);
            StatusCode::NO_CONTENT.into_response()
        }
        Err(error) => (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    }
}

async fn query(
    state: State<Arc<DataPlane>>,
    method: Method,
    query: RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    forward(state, "query", method, query, headers, body).await
}

async fn query_range(
    state: State<Arc<DataPlane>>,
    method: Method,
    query: RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    forward(state, "query_range", method, query, headers, body).await
}

/// Request headers not forwarded: hop-by-hop headers (RFC 9110 section 7.6.1),
/// `Host` and `Content-Length`, which the client sets for the new request,
/// and `Accept-Encoding`, because only the response's `Content-Type` is
/// copied back, so the response must not be compressed.
const NOT_FORWARDED: [header::HeaderName; 11] = [
    header::CONNECTION,
    header::HeaderName::from_static("keep-alive"),
    header::PROXY_AUTHENTICATE,
    header::PROXY_AUTHORIZATION,
    header::TE,
    header::TRAILER,
    header::TRANSFER_ENCODING,
    header::UPGRADE,
    header::HOST,
    header::CONTENT_LENGTH,
    header::ACCEPT_ENCODING,
];

/// Copies every request header except those in `NOT_FORWARDED` and any
/// named by `Connection`.
fn forwarded_headers(headers: &HeaderMap) -> HeaderMap {
    let named_by_connection: Vec<String> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(|name| name.trim().to_ascii_lowercase())
        .collect();
    let mut forwarded = HeaderMap::new();
    for (name, value) in headers {
        if NOT_FORWARDED.contains(name) || named_by_connection.iter().any(|n| n == name.as_str()) {
            continue;
        }
        forwarded.append(name.clone(), value.clone());
    }
    forwarded
}

/// Sends the request to Prometheus with its end-to-end headers (including
/// `Authorization` and tenancy headers such as `X-Scope-OrgID`) and returns
/// Prometheus's status, body and `Content-Type`.
async fn forward(
    State(state): State<Arc<DataPlane>>,
    endpoint: &str,
    method: Method,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let mut url = format!("{}/api/v1/{endpoint}", state.prometheus_url);
    if let Some(query) = query {
        url = format!("{url}?{query}");
    }
    let request = state
        .client
        .request(method, url)
        .headers(forwarded_headers(&headers))
        .body(body);
    let upstream = match request.send().await {
        Ok(upstream) => upstream,
        Err(error) => {
            return (
                StatusCode::BAD_GATEWAY,
                format!("forwarding to Prometheus: {error}"),
            )
                .into_response()
        }
    };
    let status = upstream.status();
    let content_type = upstream.headers().get(header::CONTENT_TYPE).cloned();
    let body = match upstream.bytes().await {
        Ok(body) => body,
        Err(error) => {
            return (
                StatusCode::BAD_GATEWAY,
                format!("reading Prometheus response: {error}"),
            )
                .into_response()
        }
    };
    let mut response = (status, body).into_response();
    if let Some(content_type) = content_type {
        response
            .headers_mut()
            .insert(header::CONTENT_TYPE, content_type);
    }
    response
        .headers_mut()
        .insert(EXECUTION_HEADER, HeaderValue::from_static("forwarded"));
    response
}
