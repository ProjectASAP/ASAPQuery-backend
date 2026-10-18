//! Backend data plane: receives Prometheus Remote Write samples and serves
//! the Prometheus query API for an installed plan.
//!
//! The installed plan is ASAPPlanner's selected `PhysicalASAPDAG`. The
//! engine thread builds its ingestion-time outputs pane by pane and answers
//! the plan's instant and range queries from them; every other query, and
//! any planned query it cannot answer, is forwarded to Prometheus.

pub mod engine;
pub mod precompute;
pub mod prometheus;
pub mod query;
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
use prometheus::{InstantQuery, RangeQuery};
use query::Unanswered;

/// Prometheus' staleness marker: a NaN that ends a series, not a sample.
const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

/// Response header naming what answered a query: `plan` or `forwarded`.
pub const EXECUTION_HEADER: &str = "x-asap-execution";
/// Response header of a forwarded planned query: why the plan did not answer.
pub const FORWARD_REASON_HEADER: &str = "x-asap-forward-reason";

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

/// Answers a planned instant query from the plan; forwards anything else.
async fn query(
    state: State<Arc<DataPlane>>,
    method: Method,
    query: RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let answered = match InstantQuery::parse(query.0.as_deref(), &body) {
        Some(instant) => Some(answer(&state, instant).await),
        None => None,
    };
    respond(answered, state, "query", method, query, headers, body).await
}

/// Answers a planned range query from the plan, one evaluation per step;
/// forwards anything else, and the whole range query if any step cannot
/// be answered.
async fn query_range(
    state: State<Arc<DataPlane>>,
    method: Method,
    query: RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let answered = match RangeQuery::parse(query.0.as_deref(), &body) {
        Some(range) => Some(answer_range(&state, range).await),
        None => None,
    };
    respond(answered, state, "query_range", method, query, headers, body).await
}

/// The plan's answer with `x-asap-execution: plan`, or else the request
/// forwarded to `endpoint`, with the reason if the query is planned.
/// `answered` is `None` for a request the data plane does not parse.
async fn respond(
    answered: Option<Result<serde_json::Value, Unanswered>>,
    state: State<Arc<DataPlane>>,
    endpoint: &str,
    method: Method,
    query: RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let reason = match answered {
        Some(Ok(response)) => {
            let mut response = axum::Json(response).into_response();
            response
                .headers_mut()
                .insert(EXECUTION_HEADER, HeaderValue::from_static("plan"));
            return response;
        }
        None | Some(Err(Unanswered::NotPlanned(_))) => None,
        Some(Err(reason)) => {
            tracing::debug!(%reason, endpoint, "forwarding a planned query");
            HeaderValue::from_str(&reason.to_string()).ok()
        }
    };
    let mut response = forward(state, endpoint, method, query, headers, body).await;
    if let Some(reason) = reason {
        response.headers_mut().insert(FORWARD_REASON_HEADER, reason);
    }
    response
}

async fn answer(state: &DataPlane, query: InstantQuery) -> Result<serde_json::Value, Unanswered> {
    let time_ms = query.time_ms.unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as i64)
    });
    let (root, ranges) = state.engine.raw_ranges(query.query, time_ms).await?;
    let raw = fetch(state, &ranges).await?;
    let samples = state.engine.evaluate(root, time_ms, raw).await?;
    Ok(prometheus::vector_response(time_ms, &samples))
}

/// Answers every step of `query`, fetching each raw range once for all.
async fn answer_range(
    state: &DataPlane,
    query: RangeQuery,
) -> Result<serde_json::Value, Unanswered> {
    let steps = query.steps();
    let (root, ranges) = state
        .engine
        .raw_ranges_for_steps(query.query, steps.clone())
        .await?;
    let raw = fetch(state, &ranges).await?;
    let results = state
        .engine
        .evaluate_steps(root, steps.clone(), raw)
        .await?;
    Ok(prometheus::matrix_response(&steps, &results))
}

/// The samples of each of `ranges` from Prometheus, by node.
async fn fetch(
    state: &DataPlane,
    ranges: &[query::RawRange],
) -> Result<BTreeMap<u64, Vec<RawSample>>, Unanswered> {
    let mut raw = BTreeMap::new();
    for range in ranges {
        let samples = prometheus::fetch(&state.client, &state.prometheus_url, range)
            .await
            .map_err(|e| Unanswered::Execution(format!("fetching raw samples: {e}")))?;
        raw.insert(range.node, samples);
    }
    Ok(raw)
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
