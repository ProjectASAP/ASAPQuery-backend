//! Backend data plane: receives Prometheus Remote Write samples (and
//! VictoriaMetrics Remote Write and OTLP) and serves the Prometheus query
//! API for an installed plan (and, when their servers are given, the
//! ClickHouse and Elasticsearch query APIs).
//!
//! The protocols are adapters: `prometheus_remote_write`,
//! `victoriametrics_remote_write` and `otlp` decode ingest requests
//! ([`remote_write`] interns their series into the store), and
//! `prometheus_http`, `clickhouse_http` and `elastic_http` read query
//! requests, write responses and send what the plan does not answer to
//! their protocol's server.
//!
//! The installed plan is ASAPPlanner's `InstallablePlan`: one selected
//! physical ASAP DAG. The precompute engine's ingest shards store the raw
//! samples its `Scan`s read and its precompute pool refreshes the
//! materialized views in ingestion runs ([`precompute`]); the query
//! engine's worker threads answer the plan's instant and range queries
//! with query runs ([`query`]), all through the Planner's executor. The
//! HTTP routes run queries through the [`dispatcher`]: every other query,
//! and any planned query the plan cannot answer, is forwarded to
//! Prometheus (or refused with `--no-forward`).
//!
//! Forward mode (`--raw-store-url`, [`raw_forward`]): every accepted Remote
//! Write request is also delivered to a raw store (VictoriaMetrics), which
//! then answers every query the plan does not and every other API path
//! (labels, series, metadata), proxied with `x-asap-execution: forwarded`.

mod dispatcher;
pub mod engine;
mod installer;
mod metrics_endpoint;
pub mod raw_forward;
pub mod remote_write;
#[cfg(test)]
mod test_support;

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, OriginalUri, RawQuery, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use common::{FallbackClient, QueryRequest, QueryResult};
use dispatcher::{Fallback, NotAnswerableReason};
use engine::{EngineHandle, EngineOptions};
use precompute::engine::IngestError;
use prometheus_http::fallback::PrometheusHttpFallback;
use prometheus_http::{InstantQuery, RangeQuery};
use query::Unanswered;

/// Response header naming what answered a query: `plan` or `forwarded`, or
/// `refused` when `--no-forward` refused to forward it.
pub const EXECUTION_HEADER: &str = "x-asap-execution";
/// Response header of a forwarded planned query: why the plan did not answer.
pub(crate) const FORWARD_REASON_HEADER: &str = "x-asap-forward-reason";

/// How long a forwarded query may take by default, matching Prometheus's
/// default `--query.timeout` of two minutes.
pub const DEFAULT_FORWARD_TIMEOUT: Duration = Duration::from_secs(120);

pub struct DataPlane {
    /// The installed plan's queries, in root order.
    pub queries: Vec<String>,
    pub engine: EngineHandle,
    pub limits: prometheus_remote_write::Limits,
    /// How OTLP resource attributes become labels (`/v1/metrics`).
    otlp_label_options: otlp::LabelOptions,
    /// Never forward a query to Prometheus: a query the plan cannot answer
    /// gets an error with the reason ([`EngineOptions::no_forward`]).
    pub no_forward: bool,
    /// The Prometheus that answers forwarded queries (the raw store in
    /// forward mode).
    fallback: PrometheusHttpFallback,
    /// Forward mode: delivers accepted Remote Write bodies to the raw store,
    /// which also answers what the plan does not. `None`: off.
    pub raw_forward: Option<raw_forward::RawForwarder>,
    /// The ClickHouse server that answers `/clickhouse/query`; `None`: the
    /// route is not served.
    clickhouse_fallback: Option<Arc<dyn FallbackClient>>,
    /// The Elasticsearch server that answers `/_sql` and `/_search`;
    /// `None`: the routes are not served.
    elastic_fallback: Option<Arc<dyn FallbackClient>>,
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
            limits: prometheus_remote_write::Limits::default(),
            otlp_label_options: otlp::LabelOptions::default(),
            fallback: PrometheusHttpFallback::new(&prometheus_url, DEFAULT_FORWARD_TIMEOUT),
            raw_forward: None,
            clickhouse_fallback: None,
            elastic_fallback: None,
        })
    }

    /// Serves ClickHouse's query route, `/clickhouse/query`, forwarding
    /// every query the plan does not answer to `fallback`. Refused with
    /// `--no-forward`, which forbids forwarding.
    pub fn with_clickhouse_fallback(
        mut self,
        fallback: impl FallbackClient + 'static,
    ) -> anyhow::Result<Self> {
        if self.no_forward {
            anyhow::bail!(
                "--clickhouse-url forwards unplanned queries, which --no-forward refuses"
            );
        }
        self.clickhouse_fallback = Some(Arc::new(fallback));
        Ok(self)
    }

    /// Serves Elasticsearch's query routes, `/_sql` and `/_search`,
    /// forwarding every query the plan does not answer to `fallback`.
    /// Refused with `--no-forward`, which forbids forwarding.
    pub fn with_elastic_fallback(
        mut self,
        fallback: impl FallbackClient + 'static,
    ) -> anyhow::Result<Self> {
        if self.no_forward {
            anyhow::bail!("--elastic-url forwards unplanned queries, which --no-forward refuses");
        }
        self.elastic_fallback = Some(Arc::new(fallback));
        Ok(self)
    }

    /// Turns forward mode on: accepted Remote Write bodies are delivered to
    /// the raw store at `options.url`, and queries the plan does not answer,
    /// as well as any other path, are proxied to it (in place of the
    /// Prometheus URL). Refused with `--no-forward`, which forbids proxying.
    pub fn with_raw_forward(
        mut self,
        options: raw_forward::RawForwardOptions,
    ) -> anyhow::Result<Self> {
        if self.no_forward {
            anyhow::bail!("--raw-store-url proxies unplanned queries, which --no-forward refuses");
        }
        self.fallback = self.fallback.with_base_url(&options.url);
        self.raw_forward = Some(raw_forward::RawForwarder::start(options)?);
        Ok(self)
    }

    /// Labels OTLP series with the resource attributes `options` promotes
    /// (`--otlp-promote-resource-attributes`).
    pub fn with_otlp_label_options(mut self, options: otlp::LabelOptions) -> Self {
        self.otlp_label_options = options;
        self
    }

    /// Bounds each forwarded query by `timeout`, from sending the request to
    /// reading the whole response (default [`DEFAULT_FORWARD_TIMEOUT`]).
    pub fn with_forward_timeout(mut self, timeout: Duration) -> Self {
        self.fallback = self.fallback.with_timeout(timeout);
        self
    }
}

pub fn router(state: Arc<DataPlane>) -> Router {
    // axum refuses bodies over 2 MB by default; Remote Write allows up to
    // `max_compressed_bytes` and answers larger bodies with 413.
    let write_body_limit = DefaultBodyLimit::max(state.limits.max_compressed_bytes);
    let forward_mode = state.raw_forward.is_some();
    let router = Router::new()
        .route("/api/v1/write", post(write).layer(write_body_limit))
        .route("/api/v1/query", get(query).post(query))
        .route("/api/v1/query_range", get(query_range).post(query_range))
        .route("/api/v1/status/asap", get(status))
        .route("/metrics", get(metrics))
        .route("/api/v1/precompute/watermark", post(watermark))
        .route("/-/ready", get(ready));
    // Forward mode: any other path (labels, series, metadata, the raw
    // store's UI) is proxied to the raw store. Off: those paths are 404,
    // except the VictoriaMetrics Remote Write and OTLP ingest routes, served
    // only then: forward mode delivers Remote Write bodies alone to the raw
    // store, which would miss their samples.
    let router = if forward_mode {
        router.fallback(proxy)
    } else {
        router
            .route(
                "/api/v1/import",
                post(write_victoriametrics).layer(write_body_limit),
            )
            .route("/v1/metrics", post(write_otlp).layer(write_body_limit))
    };
    // Each protocol's query routes, served only with its fallback, so
    // without one the path is as before (404, or proxied in forward mode).
    let mut router = router.with_state(state.clone());
    if let Some(fallback) = state.clickhouse_fallback.clone() {
        let route = FallbackRoute {
            plane: state.clone(),
            fallback,
        };
        router = router.merge(
            Router::new()
                .route(
                    "/clickhouse/query",
                    get(clickhouse_query).post(clickhouse_query),
                )
                .with_state(route),
        );
    }
    if let Some(fallback) = state.elastic_fallback.clone() {
        let route = FallbackRoute {
            plane: state.clone(),
            fallback,
        };
        router = router.merge(
            Router::new()
                .route("/_sql", post(elastic_sql).get(refuse_elastic_get))
                .route("/_search", post(elastic_search).get(refuse_elastic_get))
                .with_state(route),
        );
    }
    router
}

/// The state of one protocol's query routes: the data plane, and the
/// server that answers what the plan does not.
#[derive(Clone)]
struct FallbackRoute {
    plane: Arc<DataPlane>,
    fallback: Arc<dyn FallbackClient>,
}

/// `GET` or `POST /clickhouse/query` (ASAPQuery's ClickHouse route): the
/// query ([`clickhouse_http::parse_query`]), evaluated now.
/// Answered from the plan as TabSeparated, or forwarded to ClickHouse
/// ([`answer_or_forward`]); 400 without a query.
async fn clickhouse_query(
    State(route): State<FallbackRoute>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let now_ms = precompute::ingestion::wall_clock_ms();
    let request = match clickhouse_http::parse_query(query.as_deref(), &body, now_ms) {
        Ok(request) => request,
        Err(missing) => return (StatusCode::BAD_REQUEST, missing.to_string()).into_response(),
    };
    answer_or_forward(&route, &request, &headers, |result| {
        let body = clickhouse_http::tab_separated(result).map_err(|e| e.to_string())?;
        let content_type = clickhouse_http::TAB_SEPARATED_CONTENT_TYPE;
        Ok(([(header::CONTENT_TYPE, content_type)], body).into_response())
    })
    .await
}

/// `POST /_sql` (ASAPQuery's Elasticsearch SQL route): the body's `query`,
/// evaluated now ([`elastic_query`]).
async fn elastic_sql(
    State(route): State<FallbackRoute>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    elastic_query(
        &route,
        elastic_http::parse_sql(&body, precompute::ingestion::wall_clock_ms()),
        &headers,
    )
    .await
}

/// `POST /_search` (ASAPQuery's Query DSL route): the body, evaluated now
/// ([`elastic_query`]).
async fn elastic_search(
    State(route): State<FallbackRoute>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    elastic_query(
        &route,
        elastic_http::parse_search(&body, precompute::ingestion::wall_clock_ms()),
        &headers,
    )
    .await
}

/// Answers a parsed Elasticsearch request from the plan as `_search` hits,
/// or forwards it to Elasticsearch ([`answer_or_forward`]); a body that
/// does not parse is 400 `parsing_exception`.
async fn elastic_query(
    route: &FallbackRoute,
    parsed: Result<QueryRequest, elastic_http::BadRequestBody>,
    headers: &HeaderMap,
) -> Response {
    let request = match parsed {
        Ok(request) => request,
        Err(bad) => {
            return (StatusCode::BAD_REQUEST, axum::Json(bad.error_body())).into_response();
        }
    };
    answer_or_forward(route, &request, headers, |result| {
        let body = elastic_http::search_response(result).map_err(|e| e.to_string())?;
        Ok(axum::Json(body).into_response())
    })
    .await
}

/// `GET /_sql` or `/_search`: 400 `illegal_argument_exception`;
/// Elasticsearch queries are posted as JSON.
async fn refuse_elastic_get() -> Response {
    let body = elastic_http::get_refused_body();
    (StatusCode::BAD_REQUEST, axum::Json(body)).into_response()
}

/// Answers `request` from the plan through the dispatcher, written by
/// `encode` (`x-asap-execution: plan`; 501 with the reason when the
/// protocol cannot express the result). Otherwise sends it to the route's
/// fallback with the client's `Authorization` (the one header ASAPQuery
/// forwards) and
/// answers with the server's status, `Content-Type` and body
/// (`x-asap-execution: forwarded`, and `x-asap-forward-reason` for a
/// planned query the plan did not answer); 502 with the reason when the
/// server cannot be reached or its response cannot be read.
async fn answer_or_forward(
    route: &FallbackRoute,
    request: &QueryRequest,
    headers: &HeaderMap,
    encode: impl FnOnce(&QueryResult) -> Result<Response, String>,
) -> Response {
    let reason = match route.plane.engine.dispatcher().dispatch(request).await {
        Ok(result) => {
            return match encode(&result) {
                Ok(mut response) => {
                    response
                        .headers_mut()
                        .insert(EXECUTION_HEADER, HeaderValue::from_static("plan"));
                    response
                }
                Err(reason) => (StatusCode::NOT_IMPLEMENTED, reason).into_response(),
            };
        }
        Err(reason) => {
            match Fallback::new(reason, &request.query, route.plane.no_forward) {
                Fallback::Forward { reason } => reason,
                // A route with a fallback is never served with --no-forward.
                Fallback::Refuse { reason } => return not_answered(&reason),
            }
        }
    };
    let authorization: Vec<(String, String)> = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .map(|value| (header::AUTHORIZATION.to_string(), value.to_string()))
        .into_iter()
        .collect();
    let upstream = match route.fallback.execute(request, &authorization).await {
        Ok(upstream) => upstream,
        Err(error) => return (StatusCode::BAD_GATEWAY, error.message).into_response(),
    };
    let status = StatusCode::from_u16(upstream.status_code).unwrap_or(StatusCode::BAD_GATEWAY);
    let mut response = (status, upstream.body).into_response();
    if let Ok(content_type) = HeaderValue::from_str(&upstream.content_type) {
        response
            .headers_mut()
            .insert(header::CONTENT_TYPE, content_type);
    }
    response
        .headers_mut()
        .insert(EXECUTION_HEADER, HeaderValue::from_static("forwarded"));
    with_forward_reason(response, reason)
}

/// Forward mode: proxies a request the data plane does not serve to the raw
/// store, with `x-asap-execution: forwarded`.
async fn proxy(
    state: State<Arc<DataPlane>>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    forward_path(
        state,
        uri.path(),
        method,
        RawQuery(uri.query().map(str::to_string)),
        headers,
        body,
    )
    .await
}

async fn write(State(state): State<Arc<DataPlane>>, body: Bytes) -> Response {
    let timer = internal_metrics::Timer::start();
    match remote_write::decode_samples(&body, &state.limits) {
        Ok(samples) => {
            state.engine.metrics().remote_write_decode.record(timer);
            if samples.is_empty() {
                return StatusCode::NO_CONTENT.into_response();
            }
            // Forward mode: room in the raw store's queue first, so a refused
            // request reaches neither store and a retry duplicates nothing.
            let reservation = match &state.raw_forward {
                Some(forwarder) => match forwarder.reserve(body.len() as u64) {
                    Some(reservation) => Some(reservation),
                    None => {
                        return (
                            StatusCode::SERVICE_UNAVAILABLE,
                            "the raw store's forward queue is full",
                        )
                            .into_response()
                    }
                },
                None => None,
            };
            if let Some(refused) = ingest(&state, samples, "Remote Write") {
                return refused.into_response();
            }
            if let Some(reservation) = reservation {
                reservation.commit(body);
            }
            StatusCode::NO_CONTENT.into_response()
        }
        Err(error) => (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    }
}

/// `POST /api/v1/import`: a VictoriaMetrics Remote Write body (the Remote
/// Write protobuf, zstd-compressed), ingested as `/api/v1/write` ingests a
/// Prometheus one; 204 once the engine has it. Not served in forward mode
/// (see [`router`]).
async fn write_victoriametrics(State(state): State<Arc<DataPlane>>, body: Bytes) -> Response {
    let samples = match remote_write::decode_victoriametrics_samples(&body, &state.limits) {
        Ok(samples) => samples,
        Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    };
    if !samples.is_empty() {
        if let Some(refused) = ingest(&state, samples, "VictoriaMetrics Remote Write") {
            return refused.into_response();
        }
    }
    StatusCode::NO_CONTENT.into_response()
}

/// `POST /v1/metrics`: an OTLP/HTTP metrics body (protobuf, gzip when its
/// `Content-Encoding` says so), each data point ingested as a sample, or,
/// under a collector view's metric, staged as that view's state
/// ([`otlp`], [`precompute::collector`]). Replies as OTLP/HTTP asks: 200 with
/// an `ExportMetricsServiceResponse` once the engine has the samples and
/// states, its `partial_success` counting the points rejected (sums with
/// DELTA temporality, states failing a check); 400 for a malformed body or
/// view identity, 415 for OTLP/JSON, which is not
/// decoded, and 503 or 500 as Remote Write, each with a `google.rpc.Status`
/// body, encoded as the request was. Not served in forward mode (see
/// [`router`]).
async fn write_otlp(
    State(state): State<Arc<DataPlane>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let header_text = |name| headers.get(name).and_then(|value| value.to_str().ok());
    let encoding = otlp::Encoding::of_content_type(header_text(header::CONTENT_TYPE));
    let failed = |status: StatusCode, message: &str| {
        (
            status,
            [(header::CONTENT_TYPE, encoding.content_type())],
            otlp::status_body(encoding, message),
        )
            .into_response()
    };
    if encoding == otlp::Encoding::Json {
        return failed(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "OTLP/JSON is not supported: send application/x-protobuf",
        );
    }
    let timer = internal_metrics::Timer::start();
    let decoded = match remote_write::decode_otlp_samples(
        &body,
        header_text(header::CONTENT_ENCODING),
        &state.otlp_label_options,
        &state.limits,
    ) {
        Ok(decoded) => decoded,
        Err(error) => return failed(StatusCode::BAD_REQUEST, &error.to_string()),
    };
    let metrics = state.engine.metrics();
    metrics.otlp_decode.record(timer);
    let mut rejected_points = decoded.rejected_points;
    let mut message = decoded.rejection_message();
    // Before the samples: a request refused for them (503) is retried
    // whole, and its states are then duplicates, which are ignored; the
    // other way round, a request refused for its states would ingest its
    // samples twice.
    if !decoded.view_states.is_empty() {
        match state
            .engine
            .ingest_collector_states(decoded.view_states)
            .await
        {
            Ok(report) if report.rejected_total() > 0 => {
                rejected_points += report.rejected_total();
                let states = format!(
                    "{} collector view states were rejected: {:?}",
                    report.rejected_total(),
                    report.rejected
                );
                message = Some(match message {
                    Some(points) => format!("{points}; {states}"),
                    None => states,
                });
            }
            Ok(_) => {}
            Err(IngestError::Busy) => {
                return failed(
                    StatusCode::SERVICE_UNAVAILABLE,
                    &IngestError::Busy.to_string(),
                )
            }
            Err(IngestError::Stopped) => {
                tracing::error!("OTLP collector states refused: the engine has stopped");
                return failed(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &IngestError::Stopped.to_string(),
                );
            }
        }
    }
    if !decoded.samples.is_empty() {
        if let Some((status, message)) = ingest(&state, decoded.samples, "OTLP") {
            return failed(status, &message);
        }
    }
    let reply = otlp::export_response_body(rejected_points, message.as_deref());
    // Counted once the request is accepted, so a retried 503 counts once.
    metrics
        .otlp_rejected_points
        .fetch_add(decoded.rejected_points, Ordering::Relaxed);
    ([(header::CONTENT_TYPE, encoding.content_type())], reply).into_response()
}

/// Hands decoded samples to the engine: `None` once it has them, or the
/// status and reason refusing them when it is busy (503, which senders
/// retry) or has stopped (500).
fn ingest(
    state: &DataPlane,
    samples: Vec<storage::store::RawSample>,
    protocol: &str,
) -> Option<(StatusCode, String)> {
    let refused = match state.engine.try_ingest(samples).err()? {
        IngestError::Busy => (
            StatusCode::SERVICE_UNAVAILABLE,
            IngestError::Busy.to_string(),
        ),
        IngestError::Stopped => {
            tracing::error!("{protocol} refused: the engine has stopped");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                IngestError::Stopped.to_string(),
            )
        }
    };
    Some(refused)
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
                axum::Json(prometheus_http::bad_data(&reason)),
            )
                .into_response();
        }
        None => {
            let reason = "missing parameter \"time_ms\" (integer milliseconds)";
            return (
                StatusCode::BAD_REQUEST,
                axum::Json(prometheus_http::bad_data(reason)),
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
        "raw_forward": state.raw_forward.as_ref().map(|f| f.status()),
    });
    (
        [(header::CONTENT_TYPE, "application/json")],
        body.to_string(),
    )
        .into_response()
}

/// The data plane's measurements in the Prometheus text format: per-phase
/// thread CPU and wall time, query latency, counters, backlogs, watermark
/// lag, bytes held and each thread's CPU time ([`metrics_endpoint`]).
async fn metrics(State(state): State<Arc<DataPlane>>) -> Response {
    let mut text = state.engine.render_metrics();
    if let Some(forwarder) = &state.raw_forward {
        forwarder.render(&mut text);
    }
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], text).into_response()
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
            return not_answered(
                "the request has no query parameter, or names Prometheus' min or max time",
            );
        }
        Ok(None) => return forward(state, "query", method, query, headers, body).await,
        Err(bad) => {
            let body = axum::Json(prometheus_http::bad_data(&bad.to_string()));
            return (StatusCode::BAD_REQUEST, body).into_response();
        }
    };
    // At its time, or now.
    let time_ms = instant
        .time_ms
        .unwrap_or_else(precompute::ingestion::wall_clock_ms);
    let dispatcher = state.engine.dispatcher();
    let reason = match dispatcher.instant(&instant.query, time_ms).await {
        Ok(samples) => {
            let response = prometheus_http::VectorResponse { time_ms, samples };
            let mut response = axum::Json(response).into_response();
            response
                .headers_mut()
                .insert(EXECUTION_HEADER, HeaderValue::from_static("plan"));
            return response;
        }
        Err(reason) => reason,
    };
    match Fallback::new(reason, &instant.query, state.no_forward) {
        Fallback::Refuse { reason } => not_answered(&reason),
        Fallback::Forward { reason } => {
            if let Some(reason) = &reason {
                tracing::debug!(%reason, "forwarding a planned query");
            }
            let response = forward(state, "query", method, query, headers, body).await;
            with_forward_reason(response, reason)
        }
    }
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
                "the request lacks query, start, end or step, or names Prometheus' min or max time",
            );
        }
        Ok(None) => return forward(state, "query_range", method, query, headers, body).await,
        Err(bad) => {
            let body = axum::Json(prometheus_http::bad_data(&bad.to_string()));
            return (StatusCode::BAD_REQUEST, body).into_response();
        }
    };
    let reason = match answer_range(&state, &range).await {
        Ok(body) => {
            let mut response = (
                [(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                )],
                body,
            )
                .into_response();
            response
                .headers_mut()
                .insert(EXECUTION_HEADER, HeaderValue::from_static("plan"));
            return response;
        }
        Err(reason) => reason,
    };
    match Fallback::new(reason, &range.query, state.no_forward) {
        Fallback::Refuse { reason } => not_answered(&reason),
        Fallback::Forward { reason } => {
            if let Some(reason) = &reason {
                tracing::debug!(%reason, "forwarding a planned range query");
            }
            let response = forward(state, "query_range", method, query, headers, body).await;
            with_forward_reason(response, reason)
        }
    }
}

/// A forwarded response, with why the plan did not answer its planned
/// query (`x-asap-forward-reason`).
fn with_forward_reason(mut response: Response, reason: Option<String>) -> Response {
    if let Some(reason) = reason {
        response
            .headers_mut()
            .insert(FORWARD_REASON_HEADER, header_value(&reason));
    }
    response
}

/// Runs a planned query at every step of `range` through the dispatcher
/// and writes the response body; why not, if any step is unanswered.
async fn answer_range(
    state: &DataPlane,
    range: &RangeQuery,
) -> Result<Vec<u8>, NotAnswerableReason> {
    let steps = state
        .engine
        .dispatcher()
        .range(&range.query, range.times_ms())
        .await?;
    // Grouping every step's answer by series and writing the JSON takes
    // long for a long range of many series (241 steps of 2,800 series:
    // about 0.85 s), all of it without an await, even when every step is
    // cached. On a runtime worker it would hold up the connections queued
    // on that worker, instant queries and Remote Write included, and with
    // every worker so busy no timer or socket is polled: built on the
    // blocking pool instead.
    let failed = |error: String| NotAnswerableReason::new(Unanswered::Execution(error));
    tokio::task::spawn_blocking(move || {
        prometheus_http::range_response_body(&steps)
            .map_err(|error| failed(format!("the range response: {error}")))
    })
    .await
    .map_err(|error| failed(format!("building the range response: {error}")))?
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

/// Forwards the request to Prometheus' `/api/v1/{endpoint}` ([`forward_path`]).
async fn forward(
    state: State<Arc<DataPlane>>,
    endpoint: &str,
    method: Method,
    query: RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let path = format!("/api/v1/{endpoint}");
    forward_path(state, &path, method, query, headers, body).await
}

/// Forwards the request, as received, to `path` of the Prometheus (or raw
/// store) server ([`PrometheusHttpFallback::forward`]) and answers with its
/// status, body and `Content-Type`; 502 with the reason when the server
/// cannot be reached or its response cannot be read.
async fn forward_path(
    State(state): State<Arc<DataPlane>>,
    path: &str,
    method: Method,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let upstream = match state
        .fallback
        .forward(method, path, query.as_deref(), &headers, body)
        .await
    {
        Ok(upstream) => upstream,
        Err(error) => return (StatusCode::BAD_GATEWAY, error.message).into_response(),
    };
    let mut response = (upstream.status, upstream.body).into_response();
    if let Some(content_type) = upstream.content_type {
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
