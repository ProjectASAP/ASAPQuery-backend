//! Backend data plane: receives Prometheus Remote Write samples and serves
//! the Prometheus query API for an installed plan.
//!
//! The installed plan is ASAPPlanner's `InstallablePlan`: one selected
//! physical ASAP DAG. The engine's ingest thread stores the raw samples its
//! `Scan`s read, its precompute thread refreshes the materialized views in
//! ingestion runs, and query worker threads answer the plan's instant
//! queries with query runs, all through the Planner's executor
//! ([`engine`]); every other query, and any planned query it cannot
//! answer, is forwarded to Prometheus (or refused with `--no-forward`).

pub mod engine;
pub mod ingestion;
pub mod metrics;
pub mod persist;
pub mod prometheus;
pub mod query;
pub mod remote_write;
pub mod result_cache;
pub mod series_ids;
pub mod store;

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
use prometheus::{InstantQuery, RangeQuery};
use query::Unanswered;

/// Response header naming what answered a query: `plan` or `forwarded`, or
/// `refused` when `--no-forward` refused to forward it.
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
    /// Never forward a query to Prometheus: a query the plan cannot answer
    /// gets an error with the reason ([`EngineOptions::no_forward`]).
    pub no_forward: bool,
    client: reqwest::Client,
}

impl DataPlane {
    /// Installs the plan file `plan_json` (an `asap_planner::InstallablePlan`)
    /// with [`EngineOptions::default`], except that windows close
    /// `allowed_lateness_ms` after the newest sample passes them.
    pub fn new(
        plan_json: String,
        prometheus_url: String,
        allowed_lateness_ms: u64,
    ) -> anyhow::Result<Self> {
        let mut options = EngineOptions::default();
        options.ingestion.allowed_lateness_ms = allowed_lateness_ms;
        Self::with_options(plan_json, prometheus_url, options)
    }

    /// Installs the plan serialized in `plan_json` with `options`.
    pub fn with_options(
        plan_json: String,
        prometheus_url: String,
        options: EngineOptions,
    ) -> anyhow::Result<Self> {
        let no_forward = options.no_forward;
        let (engine, queries) = EngineHandle::spawn(plan_json, options)?;
        Ok(Self {
            no_forward,
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
        .route("/metrics", get(metrics))
        .route("/api/v1/precompute/watermark", post(watermark))
        .route("/-/ready", get(ready))
        .with_state(state)
}

async fn write(State(state): State<Arc<DataPlane>>, body: Bytes) -> Response {
    let timer = metrics::Timer::start();
    match remote_write::decode_samples(&body, &state.limits) {
        Ok(samples) => {
            let count = samples.len() as u64;
            state.engine.metrics().remote_write_decode.record(timer);
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
/// every write acknowledged before this request, every window with
/// `end <= T` is closed, so the last window of a series that stopped sending
/// is refreshed without a later sample. `T` is capped at the wall clock; a
/// repeated or lower `T` does nothing. A sample that arrives later for a
/// window the barrier closed counts as late. Input stays open. With several
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

/// Ready while the engine's ingest and precompute threads run; a stopped
/// engine never ingests again, so the data plane must be restarted.
async fn ready(State(state): State<Arc<DataPlane>>) -> StatusCode {
    if state.engine.is_running() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// The data plane's counters, as JSON: ingestion's sample and window
/// counters, the samples queued for it, the windows closed and not yet
/// refreshed, and whether it runs.
async fn status(State(state): State<Arc<DataPlane>>) -> Response {
    let body = serde_json::json!({
        "engine_running": state.engine.is_running(),
        "samples_queued": state.engine.queued_samples(),
        "precompute_backlog": state.engine.precompute_backlog(),
        "ingestion": state.engine.stats(),
    });
    (
        [(header::CONTENT_TYPE, "application/json")],
        body.to_string(),
    )
        .into_response()
}

/// The data plane's measurements in the Prometheus text format: per-phase
/// thread CPU and wall time, query latency, counters, backlogs, watermark
/// lag, bytes held and each thread's CPU time ([`metrics`]).
async fn metrics(State(state): State<Arc<DataPlane>>) -> Response {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.engine.render_metrics(),
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
        Ok(None) if state.no_forward => {
            return not_answered("the request has no query parameter");
        }
        Ok(None) => return forward(state, "query", method, query, headers, body).await,
        Err(bad) => {
            let body = axum::Json(prometheus::bad_data(&bad.to_string()));
            return (StatusCode::BAD_REQUEST, body).into_response();
        }
    };
    let reason = match answer(&state, instant).await {
        Ok(response) => {
            let mut response = axum::Json(response).into_response();
            response
                .headers_mut()
                .insert(EXECUTION_HEADER, HeaderValue::from_static("plan"));
            return response;
        }
        Err(reason) if state.no_forward => return not_answered(&reason.to_string()),
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

/// Runs a planned instant query at its time, or now.
async fn answer(
    state: &DataPlane,
    query: InstantQuery,
) -> Result<prometheus::VectorResponse, Unanswered> {
    let time_ms = query.time_ms.unwrap_or_else(ingestion::wall_clock_ms);
    let samples = match state.engine.cached(&query.query, time_ms) {
        Some(samples) => samples,
        None => state.engine.evaluate(query.query, time_ms).await?,
    };
    Ok(prometheus::VectorResponse { time_ms, samples })
}

/// Answers a planned range query from the plan, one evaluation per step;
/// forwards anything else.
async fn query_range(
    state: State<Arc<DataPlane>>,
    method: Method,
    query: RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let range = match RangeQuery::parse(query.0.as_deref(), &body) {
        Ok(Some(range)) => range,
        Ok(None) if state.no_forward => {
            return not_answered(
                "the request lacks query, start, end or step, or names a time that is not in Unix seconds",
            );
        }
        Ok(None) => return forward(state, "query_range", method, query, headers, body).await,
        Err(bad) => {
            let body = axum::Json(prometheus::bad_data(&bad.to_string()));
            return (StatusCode::BAD_REQUEST, body).into_response();
        }
    };
    let reason = match answer_range(&state, range).await {
        Ok(response) => {
            let mut response = axum::Json(response).into_response();
            response
                .headers_mut()
                .insert(EXECUTION_HEADER, HeaderValue::from_static("plan"));
            return response;
        }
        Err(reason) if state.no_forward => return not_answered(&reason.to_string()),
        Err(Unanswered::NotPlanned(_)) => None,
        Err(reason) => {
            tracing::debug!(%reason, "forwarding a planned range query");
            Some(header_value(&reason.to_string()))
        }
    };
    let mut response = forward(state, "query_range", method, query, headers, body).await;
    if let Some(reason) = reason {
        response.headers_mut().insert(FORWARD_REASON_HEADER, reason);
    }
    response
}

/// Runs a planned query at every step of `range`, the steps in parallel on
/// the query workers; unanswered if any step is.
async fn answer_range(
    state: &DataPlane,
    range: RangeQuery,
) -> Result<prometheus::MatrixResponse, Unanswered> {
    use futures::{StreamExt, TryStreamExt};
    let started = std::time::Instant::now();
    let query = &range.query;
    // The steps the result cache answers; only the others run.
    let cached: Vec<(i64, Option<_>)> = range
        .times_ms()
        .map(|time_ms| (time_ms, state.engine.cached(query, time_ms)))
        .collect();
    let from_cache = cached.iter().filter(|(_, hit)| hit.is_some()).count();
    // Every step reads the same snapshot, so the raw data they share is
    // converted once ([`store::RawReads`]).
    let snapshot = state.engine.snapshot();
    // At most one step per query worker in flight, so a long range neither
    // floods the workers' queue nor starves instant queries; the first
    // failed step, or the client going away, drops the rest unrun.
    let steps: Vec<_> = futures::stream::iter(cached)
        .map(|(time_ms, hit)| {
            let snapshot = snapshot.clone();
            async move {
                let samples = match hit {
                    Some(samples) => samples,
                    None => {
                        state
                            .engine
                            .evaluate_range_step(query.clone(), time_ms, snapshot)
                            .await?
                    }
                };
                Ok::<_, Unanswered>((time_ms, samples))
            }
        })
        .buffered(state.engine.query_threads())
        .try_collect()
        .await?;
    if let Some(metrics) = state.engine.metrics().query(query) {
        metrics
            .range_latency
            .observe(started.elapsed().as_secs_f64());
        metrics
            .range_steps_cached
            .fetch_add(from_cache as u64, Ordering::Relaxed);
        metrics
            .range_steps
            .fetch_add(steps.len() as u64, Ordering::Relaxed);
    }
    Ok(prometheus::MatrixResponse::new(&steps))
}

/// With `--no-forward`, the answer to a query the plan does not answer: a
/// Prometheus `execution` error (422) with `reason`, the reason a
/// forwarded query's `x-asap-forward-reason` would carry, and
/// `x-asap-execution: refused`. Prometheus receives no query.
fn not_answered(reason: &str) -> Response {
    let body = serde_json::json!({"status": "error", "errorType": "execution", "error": reason});
    let mut response = (StatusCode::UNPROCESSABLE_ENTITY, axum::Json(body)).into_response();
    response
        .headers_mut()
        .insert(EXECUTION_HEADER, HeaderValue::from_static("refused"));
    response
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
