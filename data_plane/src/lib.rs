//! Backend data plane: receives Prometheus Remote Write samples and serves
//! the Prometheus query API for an installed plan.
//!
//! The installed plan is ASAPPlanner's selected `PhysicalASAPDAG`. The
//! engine thread builds its ingestion-time outputs pane by pane and answers
//! the plan's instant queries from them; every other query, and any planned
//! query it cannot answer, is forwarded to Prometheus.

pub mod engine;
pub mod precompute;
pub mod prometheus;
pub mod query;
pub mod remote_write;
pub mod sds;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, RawQuery, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use engine::{EngineHandle, EngineOptions, IngestError};
use precompute::RawSample;
use prometheus::InstantQuery;
use query::Unanswered;

/// Response header naming what answered a query: `plan` or `forwarded`.
pub const EXECUTION_HEADER: &str = "x-asap-execution";
/// Response header of a forwarded planned query: why the plan did not answer.
pub const FORWARD_REASON_HEADER: &str = "x-asap-forward-reason";

/// How long a forwarded query may take by default, matching Prometheus's
/// default `--query.timeout` of two minutes.
pub const DEFAULT_FORWARD_TIMEOUT: Duration = Duration::from_secs(120);

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
    /// Installs the plan serialized in `plan_json` (a
    /// `asap_planner::InstallablePlan`) with [`EngineOptions::default`], except
    /// that panes close `allowed_lateness_ms` after the newest sample passes
    /// them.
    pub fn new(
        plan_json: String,
        prometheus_url: String,
        allowed_lateness_ms: u64,
    ) -> anyhow::Result<Self> {
        let mut options = EngineOptions::default();
        options.precompute.allowed_lateness_ms = allowed_lateness_ms;
        Self::with_options(plan_json, prometheus_url, options)
    }

    /// Installs the plan serialized in `plan_json` with `options`.
    pub fn with_options(
        plan_json: String,
        prometheus_url: String,
        options: EngineOptions,
    ) -> anyhow::Result<Self> {
        let (engine, queries) = EngineHandle::spawn(plan_json, options)?;
        Ok(Self {
            queries,
            engine,
            prometheus_url: prometheus_url.trim_end_matches('/').to_string(),
            limits: remote_write::Limits::default(),
            samples: AtomicU64::new(0),
            client: forward_client(DEFAULT_FORWARD_TIMEOUT),
        })
    }

    /// Bounds each forwarded query by `timeout`, from sending the request to
    /// reading the whole response (default [`DEFAULT_FORWARD_TIMEOUT`]).
    pub fn with_forward_timeout(mut self, timeout: Duration) -> Self {
        self.client = forward_client(timeout);
        self
    }
}

fn forward_client(timeout: Duration) -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .expect("reqwest client with only a timeout set")
}

pub fn router(state: Arc<DataPlane>) -> Router {
    // axum refuses bodies over 2 MB by default; Remote Write allows up to
    // `max_compressed_bytes` and answers larger bodies with 413.
    let write_body_limit = DefaultBodyLimit::max(state.limits.max_compressed_bytes);
    Router::new()
        .route("/api/v1/write", post(write).layer(write_body_limit))
        .route("/api/v1/query", get(query).post(query))
        .route("/api/v1/query_range", get(query_range).post(query_range))
        .route("/api/v1/status/asap", get(status))
        .route("/api/v1/precompute/watermark", post(watermark))
        .route("/-/ready", get(ready))
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
                // Stale markers are kept: the executor's sample selector
                // drops them from range windows and ends instant selectors.
                samples.extend(series.samples.into_iter().map(|s| RawSample {
                    labels: labels.clone(),
                    timestamp_ms: s.timestamp,
                    value: s.value,
                }));
            }
            let count = samples.len() as u64;
            if count == 0 {
                return StatusCode::NO_CONTENT.into_response();
            }
            match state.engine.try_ingest(samples) {
                Ok(()) => {
                    state.samples.fetch_add(count, Ordering::Relaxed);
                    StatusCode::NO_CONTENT.into_response()
                }
                // Remote Write senders retry on 503.
                Err(IngestError::Busy) => (
                    StatusCode::SERVICE_UNAVAILABLE,
                    IngestError::Busy.to_string(),
                )
                    .into_response(),
                Err(IngestError::Stopped) => {
                    tracing::error!("Remote Write refused: the engine has stopped");
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        IngestError::Stopped.to_string(),
                    )
                        .into_response()
                }
            }
        }
        Err(error) => (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    }
}

/// `POST /api/v1/precompute/watermark?time_ms=T`: the producer states that
/// every sample at or before `T` has been written. Once the engine has run
/// every write acknowledged before this request, every pane with
/// `end <= T` is closed, so the last pane of a series that stopped sending
/// is built without a later sample. `T` is capped at the wall clock; a
/// repeated or lower `T` does nothing. A sample that arrives later for a
/// pane the barrier closed counts as late. Input stays open. With several
/// producers, call it only after all of them have written through `T`.
/// Answers `{"barrier_ms": B}`, the barrier in force (`T` capped at the wall
/// clock, or a higher earlier barrier); 400 `bad_data` with the reason when
/// `time_ms` is missing or not an integer.
async fn watermark(State(state): State<Arc<DataPlane>>, RawQuery(query): RawQuery) -> Response {
    let time_ms = form_urlencoded::parse(query.unwrap_or_default().as_bytes())
        .find(|(name, _)| name == "time_ms")
        .map(|(_, value)| value.into_owned());
    let time_ms = match time_ms.as_deref().map(str::parse::<i64>) {
        Some(Ok(time_ms)) => time_ms,
        Some(Err(_)) => {
            let reason = format!(
                "invalid parameter \"time_ms\": {:?} is not an integer number of milliseconds",
                time_ms.unwrap_or_default()
            );
            return (
                StatusCode::BAD_REQUEST,
                axum::Json(prometheus::bad_data(&reason)),
            )
                .into_response();
        }
        None => {
            let reason = "missing parameter \"time_ms\" (integer milliseconds)";
            return (
                StatusCode::BAD_REQUEST,
                axum::Json(prometheus::bad_data(reason)),
            )
                .into_response();
        }
    };
    match state.engine.advance_watermark(time_ms).await {
        Ok(barrier_ms) => {
            axum::Json(serde_json::json!({ "barrier_ms": barrier_ms })).into_response()
        }
        Err(error) => {
            tracing::error!(%error, "event-time barrier refused");
            (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response()
        }
    }
}

/// Ready while the engine thread runs; a stopped engine never ingests
/// again, so the data plane must be restarted.
async fn ready(State(state): State<Arc<DataPlane>>) -> StatusCode {
    if state.engine.is_running() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// The data plane's counters, as JSON: the precompute engine's sample and
/// pane counters, the samples queued for it, and whether it runs.
async fn status(State(state): State<Arc<DataPlane>>) -> Response {
    let body = serde_json::json!({
        "engine_running": state.engine.is_running(),
        "samples_queued": state.engine.queued_samples(),
        "precompute": state.engine.stats(),
        "query_run": state.engine.query_run_stats(),
    });
    (
        [(header::CONTENT_TYPE, "application/json")],
        body.to_string(),
    )
        .into_response()
}

/// `text` as a header value: characters a header cannot carry (controls and
/// non-ASCII) become `?`.
fn header_value(text: &str) -> HeaderValue {
    let visible: String = text
        .chars()
        .map(|c| {
            if c == ' ' || c.is_ascii_graphic() {
                c
            } else {
                '?'
            }
        })
        .collect();
    HeaderValue::from_str(&visible).expect("visible ASCII is a valid header value")
}

/// Answers a planned instant query from the plan; forwards anything else.
async fn query(
    state: State<Arc<DataPlane>>,
    method: Method,
    query: RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let instant = match InstantQuery::parse(query.0.as_deref(), &body) {
        Ok(Some(instant)) => instant,
        Ok(None) => return forward(state, "query", method, query, headers, body).await,
        Err(bad) => {
            let body = axum::Json(prometheus::bad_data(&bad.to_string()));
            return (StatusCode::BAD_REQUEST, body).into_response();
        }
    };
    let reason = match answer(&state, instant, &headers).await {
        Ok(response) => {
            let mut response = axum::Json(response).into_response();
            response
                .headers_mut()
                .insert(EXECUTION_HEADER, HeaderValue::from_static("plan"));
            return response;
        }
        Err(Unanswered::NotPlanned(_)) => None,
        Err(reason) => {
            tracing::debug!(%reason, "forwarding a planned query");
            Some(header_value(&reason.to_string()))
        }
    };
    let mut response = forward(state, "query", method, query, headers, body).await;
    if let Some(reason) = reason {
        response.headers_mut().insert(FORWARD_REASON_HEADER, reason);
    }
    response
}

/// Runs a planned instant query. Bound raw-sample windows are read from Prometheus with
/// the caller's end-to-end `headers`, as a forwarded query would be.
async fn answer(
    state: &DataPlane,
    query: InstantQuery,
    headers: &HeaderMap,
) -> Result<serde_json::Value, Unanswered> {
    let time_ms = query.time_ms.unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as i64)
    });
    let prepared = state
        .engine
        .bound_raw_sample_windows(query.query, time_ms)
        .await?;
    let ranges = &prepared.windows;
    let mut fetch_headers = forwarded_headers(headers);
    // The raw read is a GET without the caller's body.
    fetch_headers.remove(header::CONTENT_TYPE);
    // The bound raw-sample windows are fetched concurrently.
    let fetches = ranges.iter().map(|range| async {
        prometheus::fetch(&state.client, &state.prometheus_url, &fetch_headers, range)
            .await
            .map(|samples| (range.node, samples))
    });
    let raw: BTreeMap<u64, Vec<RawSample>> = futures::future::try_join_all(fetches)
        .await
        .map_err(|e| Unanswered::Execution(format!("fetching raw samples: {e}")))?
        .into_iter()
        .collect();
    let samples = state.engine.evaluate(prepared, time_ms, raw).await?;
    Ok(prometheus::vector_response(time_ms, &samples))
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A forward reason with characters a header cannot carry is still sent,
    /// with those characters replaced.
    #[test]
    fn sanitizes_header_values() {
        assert_eq!(
            header_value("pane ending at 10 is missing"),
            "pane ending at 10 is missing"
        );
        assert_eq!(header_value("bad\nlabel é"), "bad?label ?");
    }
}
