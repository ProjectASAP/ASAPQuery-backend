//! Backend data plane: receives Prometheus Remote Write samples and serves
//! the Prometheus query API for an installed plan.
//!
//! The installed plan is ASAPPlanner's selected `PhysicalASAPDAG`. The
//! engine thread builds its ingestion-time outputs pane by pane; queries are
//! still forwarded to Prometheus.

pub mod engine;
pub mod precompute;
pub mod remote_write;
pub mod sds;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{RawQuery, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use control_plane::InstalledPlan;
use engine::EngineHandle;
use precompute::RawSample;

/// Prometheus' staleness marker: a NaN that ends a series, not a sample.
const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

/// Response header naming what answered a query.
pub const EXECUTION_HEADER: &str = "x-asap-execution";

pub struct DataPlane {
    /// The installed plan's queries, in root order.
    pub queries: Vec<String>,
    pub engine: EngineHandle,
    /// Base URL of the Prometheus that answers forwarded queries.
    pub prometheus_url: String,
    pub limits: remote_write::Limits,
    /// Samples accepted since startup.
    pub samples: AtomicU64,
    client: reqwest::Client,
}

impl DataPlane {
    /// Installs the plan serialized in `plan_json` (an [`InstalledPlan`]).
    /// Panes close `allowed_lateness_ms` after the newest sample passes them.
    pub fn new(
        plan_json: String,
        prometheus_url: String,
        allowed_lateness_ms: i64,
    ) -> anyhow::Result<Self> {
        let plan: InstalledPlan = serde_json::from_str(&plan_json)?;
        plan.validate()?;
        Ok(Self {
            queries: plan.queries,
            engine: EngineHandle::spawn(plan_json, allowed_lateness_ms)?,
            prometheus_url: prometheus_url.trim_end_matches('/').to_string(),
            limits: remote_write::Limits::default(),
            samples: AtomicU64::new(0),
            client: reqwest::Client::new(),
        })
    }
}

pub fn router(state: Arc<DataPlane>) -> Router {
    Router::new()
        .route("/api/v1/write", post(write))
        .route("/api/v1/query", get(query).post(query))
        .route("/api/v1/query_range", get(query_range).post(query_range))
        .route("/-/ready", get(|| async { StatusCode::OK }))
        .with_state(state)
}

async fn write(State(state): State<Arc<DataPlane>>, body: Bytes) -> Response {
    match remote_write::decode(&body, &state.limits) {
        Ok(request) => {
            let mut samples = Vec::new();
            for series in request.timeseries {
                let labels: Arc<BTreeMap<String, String>> = Arc::new(
                    series
                        .labels
                        .into_iter()
                        .map(|label| (label.name, label.value))
                        .collect(),
                );
                samples.extend(
                    series
                        .samples
                        .into_iter()
                        .filter(|s| s.value.to_bits() != STALE_NAN_BITS)
                        .map(|s| RawSample {
                            labels: labels.clone(),
                            timestamp_ms: s.timestamp,
                            value: s.value,
                        }),
                );
            }
            let count = samples.len() as u64;
            match state.engine.try_ingest(samples) {
                Ok(()) => {
                    state.samples.fetch_add(count, Ordering::Relaxed);
                    StatusCode::NO_CONTENT.into_response()
                }
                // Remote Write senders retry on 5xx.
                Err(busy) => (StatusCode::SERVICE_UNAVAILABLE, busy.to_string()).into_response(),
            }
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

/// Sends the request to Prometheus unchanged and returns its response.
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
    let mut request = state.client.request(method, url).body(body);
    if let Some(content_type) = headers.get(header::CONTENT_TYPE) {
        request = request.header(header::CONTENT_TYPE, content_type);
    }
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
