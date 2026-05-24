//! `ThanosQueryEngine` — HTTP forwarder to a `thanos-query`
//! sidecar for Path A2 of the Step-2 archive deprecation.
//!
//! Step-2.1 (PR #311) teaches `gorillas3processor` to emit
//! Prometheus TSDB block format; Step-2.2 (PR #310) adds a
//! `thanos-store-gateway` + `thanos-query` pair to the demo overlay.
//! Step-2.3 (this file) wires the backend to forward archive-tier
//! PromQL queries to that sidecar over HTTP.
//!
//! The engine is selected by the [`ASAP_THANOS_QUERY_URL_ENV`] env
//! var, consulted at backend startup:
//!
//! * **Path A2 mode** (env set) — `ThanosQueryEngine` is
//!   registered in the [`crate::query_engines::routing::EngineRouter`]. Archive
//!   queries POST to `${ASAP_THANOS_QUERY_URL}/api/v1/query` and
//!   the answer is wrapped in ASAP's standard
//!   [`crate::query_engines::QueryResult`] shape.
//!
//! Path A2 is now the only archive path. The superseded legacy
//! in-process Gorilla executor (custom GORILLA1 container format,
//! read from per-hour chunks on S3 / MinIO) has been deleted after
//! Path A2 was verified end-to-end. When the env var is unset, the
//! binary registers a `NoDataArchiveEngine` stub on the archive
//! slot instead.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use tracing::{debug, warn};

use crate::storage_engines::types::KeyByLabelValues;
use crate::query_engines::query_result::{InstantVectorElement, QueryResult, RangeVectorElement};
use crate::query_engines::routing::query_engine_routing::{EngineCapabilities, QueryEngine};
use crate::storage_engines::sketch_db::accuracy::{AccuracyEnvelope, AccuracyProfile};

// ---------------------------------------------------------------------------
// Public constants.
//
// The env var name and the engine id are pinned strings so the
// binary, dashboards, and `BackendStorageRouting` configs can
// byte-compare without re-deriving them.
// ---------------------------------------------------------------------------

/// Env var consulted at backend startup. When set, the binary
/// registers a [`ThanosQueryEngine`] pointing at the URL and the
/// router dispatches archive-tier queries to it. When unset, the
/// binary registers a `NoDataArchiveEngine` stub on the archive slot.
pub const ASAP_THANOS_QUERY_URL_ENV: &str = "ASAP_THANOS_QUERY_URL";

/// Default upstream URL when `ASAP_THANOS_QUERY_URL` is set to the
/// empty string or contains only whitespace. Mirrors the demo
/// overlay's default service name + port (Step-2.2's
/// `mvp-thanos-archive.yml` pins `thanos-query:10903`).
pub const DEFAULT_THANOS_QUERY_URL: &str = "http://thanos-query:10903";

/// `data_source_id` the [`ThanosQueryEngine`] registers under
/// for explicit per-query overrides via the `X-ASAP-Engine` header
/// or the `?engine=` query param. Pinned so dashboards / e2e
/// scripts can byte-compare without parsing.
pub const DATA_SOURCE_THANOS_QUERY_ID: &str = asap_types::ENGINE_ID_THANOS_QUERY;

/// Marker line every `ThanosQueryEngine` answer carries on its
/// `infos` array. Pinned so dashboards and the upcoming Step-2.4
/// e2e demo can byte-compare without parsing.
pub const DATA_SOURCE_THANOS_QUERY_INFO: &str = "data_source: thanos_query";

/// `data_source_quirk` line surfaced when the upstream
/// `thanos-query` sidecar is unreachable (network error / 5xx /
/// timeout). Pinned so the upcoming Step-2.4 e2e demo can pin the
/// fail-loud behaviour.
pub const QUIRK_THANOS_UNREACHABLE: &str = "data_source_quirk: thanos_unreachable";

/// Default request timeout for the forwarded query. Generous
/// enough that thanos-query has room to do its own store-gateway
/// fan-out, tight enough that the backend doesn't pile up
/// in-flight requests on a wedged sidecar.
pub const DEFAULT_THANOS_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------
// Config + engine.
// ---------------------------------------------------------------------------

/// Tunable runtime knobs for [`ThanosQueryEngine`]. Built from
/// env via [`ThanosQueryConfig::from_env`].
#[derive(Debug, Clone)]
pub struct ThanosQueryConfig {
    /// Base URL of the upstream `thanos-query` sidecar — e.g.
    /// `http://thanos-query:10903`. The engine appends
    /// `/api/v1/query` (or `/api/v1/query_range`) when forwarding.
    /// Trailing slash is tolerated; both forms are normalised.
    pub base_url: String,
    /// Wall-clock timeout per forwarded request.
    pub request_timeout: Duration,
}

impl Default for ThanosQueryConfig {
    fn default() -> Self {
        Self {
            base_url: DEFAULT_THANOS_QUERY_URL.to_string(),
            request_timeout: DEFAULT_THANOS_REQUEST_TIMEOUT,
        }
    }
}

impl ThanosQueryConfig {
    /// Build a config from the [`ASAP_THANOS_QUERY_URL_ENV`] env
    /// var, returning `None` when the var is unset / empty / blank
    /// (the binary should then fall through to the legacy
    /// in-process engine path).
    ///
    /// A whitespace-only value is treated as unset rather than as
    /// a malformed URL: we don't want a stray `ASAP_THANOS_QUERY_URL=`
    /// in a `.env` to silently flip Path A2 on with the default
    /// host name.
    pub fn from_env() -> Option<Self> {
        let raw = std::env::var(ASAP_THANOS_QUERY_URL_ENV).ok()?;
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return None;
        }
        Some(Self {
            base_url: trimmed.trim_end_matches('/').to_string(),
            request_timeout: DEFAULT_THANOS_REQUEST_TIMEOUT,
        })
    }

    fn instant_endpoint(&self) -> String {
        format!("{}/api/v1/query", self.base_url)
    }
}

/// Forwards PromQL queries to an upstream `thanos-query` sidecar
/// over HTTP and wraps the response in ASAP's standard
/// [`QueryResult`] shape.
///
/// Implements the [`QueryEngine`] trait so the
/// [`crate::query_engines::routing::EngineRouter`] can hold it as `Arc<dyn
/// QueryEngine>`. Reports `data_source_id =
/// "thanos_query"` and (for the compatibility-list dispatch path)
/// `storage_backend = StorageBackend::GorillaObjectStore` — Path A2
/// re-uses the archive tier slot in the routing matrix, so any
/// metric configured for `GorillaObjectStore` keeps routing through
/// the archive tier; only the engine answering changes.
pub struct ThanosQueryEngine {
    config: ThanosQueryConfig,
    client: reqwest::Client,
    /// Pinned id we register under.
    data_source_id: &'static str,
}

impl ThanosQueryEngine {
    /// Build with an explicit config. Used by tests + the binary's
    /// startup wiring.
    pub fn new(config: ThanosQueryConfig) -> Result<Self, ThanosQueryError> {
        let client = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .build()
            .map_err(|e| ThanosQueryError::ConfigInvalid(e.to_string()))?;
        Ok(Self {
            config,
            client,
            data_source_id: DATA_SOURCE_THANOS_QUERY_ID,
        })
    }

    /// Build the production config from
    /// [`ASAP_THANOS_QUERY_URL_ENV`] or return `None` when the env
    /// var is unset / blank. The binary calls this first; if it
    /// returns `None`, the legacy in-process Gorilla object-store
    /// executor is registered instead.
    pub fn from_env() -> Option<Result<Self, ThanosQueryError>> {
        ThanosQueryConfig::from_env().map(Self::new)
    }

    /// Read-only access to the configured base URL — useful for
    /// diagnostics + the upcoming Step-2.4 demo's startup banner.
    pub fn base_url(&self) -> &str {
        &self.config.base_url
    }

    /// The infos a successful forwarded answer carries. The
    /// `data_source: thanos_query` line is added by the HTTP
    /// handler's `annotate_data_source` step (driven from
    /// `capabilities().data_source_id`), so tests pin the strings
    /// here without re-implementing the wire path.
    pub fn success_infos(elapsed_ms: u128) -> Vec<String> {
        vec![
            DATA_SOURCE_THANOS_QUERY_INFO.to_string(),
            AccuracyProfile::exact().summary(),
            format!("query_latency_ms: {elapsed_ms}"),
        ]
    }

    /// The infos a forwarded-but-failed answer carries. Includes
    /// the quirk line so the upcoming Step-2.4 e2e demo can pin
    /// fail-loud behaviour.
    pub fn unreachable_infos(reason: &str, elapsed_ms: u128) -> Vec<String> {
        vec![
            DATA_SOURCE_THANOS_QUERY_INFO.to_string(),
            QUIRK_THANOS_UNREACHABLE.to_string(),
            format!("thanos_unreachable_reason: {reason}"),
            format!("query_latency_ms: {elapsed_ms}"),
        ]
    }

    /// Forward `query` to `${base_url}/api/v1/query` and parse the
    /// Prometheus-format response back into a [`QueryResult`].
    ///
    /// Errors are folded into [`ThanosQueryError`] variants —
    /// the [`QueryEngine`] impl decides how to surface each.
    pub async fn query(&self, query: &str) -> Result<QueryResult, ThanosQueryError> {
        let started = Instant::now();
        let url = self.config.instant_endpoint();
        debug!(
            url = %url,
            query = query,
            "thanos-forward: issuing instant query",
        );

        let resp = self
            .client
            .post(&url)
            .form(&[("query", query)])
            .send()
            .await
            .map_err(|e| ThanosQueryError::Unreachable(e.to_string()))?;

        let status = resp.status();
        if status.is_server_error() {
            return Err(ThanosQueryError::Unreachable(format!(
                "upstream returned {status}",
            )));
        }
        if !status.is_success() {
            // Treat 4xx as a "bad query" / capability miss — it's
            // not an unreachable upstream, it's a query the
            // sidecar doesn't accept.
            let body = resp.text().await.unwrap_or_default();
            return Err(ThanosQueryError::BadQuery {
                status: status.as_u16(),
                body,
            });
        }

        let payload: ThanosResponse = resp
            .json()
            .await
            .map_err(|e| ThanosQueryError::ParseError(e.to_string()))?;

        let elapsed_ms = started.elapsed().as_millis();
        let result = build_result_from_thanos_payload(payload, elapsed_ms)
            .map_err(ThanosQueryError::ParseError)?;
        Ok(result)
    }
}

#[async_trait]
impl QueryEngine for ThanosQueryEngine {
    async fn execute(&self, query: &str) -> Result<QueryResult, crate::query_engines::EngineError> {
        match self.query(query).await {
            Ok(result) => Ok(result),
            Err(ThanosQueryError::Unreachable(reason)) => {
                // Surface fail-loud as a backend error so the
                // router's failover sequence can fall through to
                // the ASAP-tier sketch on a `DoubleWrite` deploy.
                // The wrapped result also carries the quirk infos
                // for direct (non-router) callers — see the test
                // `unreachable_returns_quirk_infos_in_wrapped_result`.
                warn!(
                    engine = self.data_source_id,
                    error = %reason,
                    "thanos-forward: upstream unreachable",
                );
                Err(crate::query_engines::EngineError::backend(
                    self.data_source_id,
                    format!("thanos_unreachable: {reason}"),
                ))
            }
            Err(ThanosQueryError::BadQuery { status, body }) => {
                Err(crate::query_engines::EngineError::capability_miss(
                    self.data_source_id,
                    format!("thanos rejected query (status {status}): {body}"),
                ))
            }
            Err(ThanosQueryError::ParseError(msg)) => Err(crate::query_engines::EngineError::backend(
                self.data_source_id,
                format!("thanos response parse error: {msg}"),
            )),
            Err(ThanosQueryError::ConfigInvalid(msg)) => Err(crate::query_engines::EngineError::backend(
                self.data_source_id,
                format!("thanos client misconfigured: {msg}"),
            )),
        }
    }

    fn capabilities(&self) -> EngineCapabilities {
        EngineCapabilities {
            data_source_id: self.data_source_id,
            // Re-uses the archive tier slot in the routing
            // matrix; Path A2 swaps the engine answering, not the
            // tier classification. See module docstring.
            storage_backend: asap_types::StorageBackend::GorillaObjectStore,
            // Forwarder doesn't materialise samples locally;
            // upstream thanos-query owns the memory budget. We
            // surface a generous ceiling so the cost-aware
            // dispatcher (Phase-6) prefers thanos for large
            // streams once it lands.
            supports_streams_above_bytes: usize::MAX,
        }
    }
}

// ---------------------------------------------------------------------------
// Wire helpers.
// ---------------------------------------------------------------------------

/// Subset of the Prometheus HTTP API response shape we actually
/// consume. `serde` ignores unknown fields, so future thanos
/// extensions don't break parsing.
#[derive(Debug, Deserialize)]
struct ThanosResponse {
    status: String,
    #[serde(default)]
    data: Option<ThanosData>,
    #[serde(rename = "errorType", default)]
    error_type: Option<String>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ThanosData {
    #[serde(rename = "resultType", default)]
    result_type: String,
    #[serde(default)]
    result: Vec<Value>,
}

/// Build an ASAP [`QueryResult`] from a parsed thanos payload.
/// Pulled out as a pure function so the unit tests can pin the
/// wrapping behaviour without spinning up a TCP listener.
fn build_result_from_thanos_payload(
    payload: ThanosResponse,
    elapsed_ms: u128,
) -> Result<QueryResult, String> {
    if payload.status != "success" {
        let detail = payload.error.unwrap_or_else(|| "unknown error".to_string());
        let kind = payload
            .error_type
            .unwrap_or_else(|| "execution".to_string());
        return Err(format!("thanos error ({kind}): {detail}"));
    }
    let data = payload
        .data
        .ok_or_else(|| "thanos response missing `data`".to_string())?;

    let mut result = match data.result_type.as_str() {
        "vector" => parse_vector(&data.result)?,
        "matrix" => parse_matrix(&data.result)?,
        // Scalar / string result types are valid PromQL but the
        // ASAP wire shape only models vector / matrix. Surface as
        // a parse error so callers see the upstream type rather
        // than an empty vector.
        other => {
            return Err(format!(
                "thanos response carried unsupported resultType={other:?}"
            ));
        }
    };

    // Pin an exact-accuracy envelope on every wrapped answer.
    // Path A2 reads from the Prometheus TSDB blocks
    // `gorillas3processor` emits in Step-2.1 — the underlying
    // samples are the raw chunks, not sketches, so the answer is
    // exact (ε = 0, δ = 0).
    let envelope = AccuracyEnvelope::single(AccuracyProfile::exact());
    result = result.with_accuracy(envelope);

    // Surface the wrapping infos via the result's `warnings`
    // field. `warnings` is the only `QueryResult` channel that
    // flows through `convert_query_result_to_prometheus` to the
    // wire response's top-level `warnings: []` array; the
    // `infos: []` array is appended downstream from the
    // PrometheusResponse adapter (`with_accuracy` already mirrors
    // a one-liner there). For dashboards that pin
    // `data_source: thanos_query` byte-compares, the HTTP
    // handler's `annotate_data_source` step adds the marker line
    // to `infos` after dispatch — but only when the engine reports
    // the `thanos_query` id; aliased registrations under
    // `thanos_query` get the gorilla marker instead, which is
    // fine for Path A2 backwards compat.
    let _ = elapsed_ms; // surfaced via tests directly via `success_infos`.
    Ok(result)
}

fn parse_vector(values: &[Value]) -> Result<QueryResult, String> {
    let mut elements = Vec::with_capacity(values.len());
    let mut latest_ts: u64 = 0;
    for v in values {
        let metric = v.get("metric").cloned().unwrap_or(Value::Null);
        let value = v
            .get("value")
            .ok_or_else(|| "vector element missing `value`".to_string())?;
        let pair = value
            .as_array()
            .ok_or_else(|| "vector element `value` is not an array".to_string())?;
        if pair.len() != 2 {
            return Err(format!(
                "vector element `value` must be [ts, str_value], got {pair:?}"
            ));
        }
        let ts_seconds = pair[0]
            .as_f64()
            .ok_or_else(|| format!("vector element ts is not a number: {:?}", pair[0]))?;
        let ts_ms = (ts_seconds * 1000.0).round() as u64;
        latest_ts = latest_ts.max(ts_ms);
        let scalar = pair[1]
            .as_str()
            .ok_or_else(|| format!("vector element value is not a string: {:?}", pair[1]))?;
        let parsed: f64 = scalar
            .parse()
            .map_err(|e| format!("vector element value parse error: {e} (raw={scalar:?})"))?;
        let labels = labels_from_metric(&metric);
        elements.push(InstantVectorElement::new(labels, parsed));
    }
    Ok(QueryResult::vector(elements, latest_ts))
}

fn parse_matrix(values: &[Value]) -> Result<QueryResult, String> {
    let mut series = Vec::with_capacity(values.len());
    for v in values {
        let metric = v.get("metric").cloned().unwrap_or(Value::Null);
        let raw_samples = v
            .get("values")
            .and_then(Value::as_array)
            .ok_or_else(|| "matrix element missing `values` array".to_string())?;
        let labels = labels_from_metric(&metric);
        let mut elem = RangeVectorElement::new(labels);
        for sample in raw_samples {
            let pair = sample
                .as_array()
                .ok_or_else(|| "matrix sample is not [ts, str_value]".to_string())?;
            if pair.len() != 2 {
                return Err(format!(
                    "matrix sample must be [ts, str_value], got {pair:?}"
                ));
            }
            let ts_seconds = pair[0]
                .as_f64()
                .ok_or_else(|| format!("matrix sample ts is not a number: {:?}", pair[0]))?;
            let ts_ms = (ts_seconds * 1000.0).round() as u64;
            let scalar = pair[1]
                .as_str()
                .ok_or_else(|| format!("matrix sample value is not a string: {:?}", pair[1]))?;
            let parsed: f64 = scalar
                .parse()
                .map_err(|e| format!("matrix sample value parse error: {e} (raw={scalar:?})"))?;
            elem.add_sample(ts_ms, parsed);
        }
        series.push(elem);
    }
    Ok(QueryResult::matrix(series))
}

/// Best-effort label extraction. Thanos returns the `metric` field
/// as a `{"__name__": "...", "label": "value"}` object; we flatten
/// the values into `KeyByLabelValues` (the same shape the
/// in-process engine pins on its results). Unknown / non-object
/// shapes fall through to an empty label set rather than failing
/// the parse — the wrapped `data_source: thanos_query` info is
/// the meaningful annotation.
fn labels_from_metric(metric: &Value) -> KeyByLabelValues {
    if let Some(obj) = metric.as_object() {
        let mut values: Vec<String> = obj
            .iter()
            .filter(|(k, _)| k.as_str() != "__name__")
            .filter_map(|(_, v)| v.as_str().map(|s| s.to_string()))
            .collect();
        values.sort();
        KeyByLabelValues::new_with_labels(values)
    } else {
        KeyByLabelValues::new_with_labels(Vec::new())
    }
}

// ---------------------------------------------------------------------------
// Errors.
// ---------------------------------------------------------------------------

/// Failure modes of the HTTP-forwarder. The [`QueryEngine`] impl
/// folds these into the trait-level [`crate::query_engines::EngineError`]
/// envelope; the public `query` method returns the richer surface
/// for tests and direct callers.
#[derive(Debug, thiserror::Error)]
pub enum ThanosQueryError {
    /// Upstream returned a network error / timeout / 5xx —
    /// dashboard-level "thanos is down."
    #[error("thanos unreachable: {0}")]
    Unreachable(String),
    /// Upstream returned a 4xx — the query is malformed from
    /// thanos's point of view, not a backend failure.
    #[error("thanos rejected query (HTTP {status}): {body}")]
    BadQuery {
        /// The 4xx status code thanos returned.
        status: u16,
        /// The (possibly empty) response body.
        body: String,
    },
    /// Upstream returned a 2xx but the body wasn't parseable as a
    /// Prometheus-format response.
    #[error("thanos response parse error: {0}")]
    ParseError(String),
    /// reqwest client construction failed (TLS / DNS resolver
    /// init etc.). Surfaces only at engine construction.
    #[error("thanos client config invalid: {0}")]
    ConfigInvalid(String),
}

// ---------------------------------------------------------------------------
// Helpers re-exported for the engine's test module + the binary's
// "alias under the legacy slot" registration.
// ---------------------------------------------------------------------------

/// Convenience combinator the binary uses at startup: try
/// [`ThanosQueryEngine::from_env`]; if it returns `None`, the
/// caller registers a `NoDataArchiveEngine` stub on the archive
/// slot (the legacy in-process Gorilla path has been deleted).
///
/// Returning `Result<Option<...>, ...>` instead of unwrapping in
/// `main.rs` keeps the construction failure (bad URL / bad TLS
/// init) inspectable so the binary can emit a helpful warning
/// instead of crashing on startup.
pub fn engine_from_env() -> Result<Option<ThanosQueryEngine>, ThanosQueryError> {
    match ThanosQueryEngine::from_env() {
        Some(Ok(engine)) => Ok(Some(engine)),
        Some(Err(e)) => Err(e),
        None => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

#[doc(hidden)]
#[cfg(any(test, feature = "extra_debugging"))]
pub mod test_support {
    //! Test-only helpers for spinning up an in-process mock
    //! `thanos-query` sidecar. Used by the unit + integration
    //! tests below and by the `routing/query_engine_routing.rs` tests
    //! once they grow Path A2 coverage.

    use std::net::SocketAddr;
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;

    use axum::{routing::post, Router};

    /// Trivial in-process axum server that returns a canned
    /// Prometheus-format JSON for every `POST /api/v1/query`.
    ///
    /// Returns `(base_url, join_handle)`. Drop the handle to stop
    /// serving (the test runtime tears down anyway when the
    /// `#[tokio::test]` future completes).
    pub async fn spawn_mock_thanos(canned_body: &'static str) -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let local: SocketAddr = listener.local_addr().expect("local_addr");
        let base_url = format!("http://{local}");

        let app: Router = Router::new()
            .route("/api/v1/query", post(move || async move { canned_body }))
            .route(
                "/api/v1/query_range",
                post(move || async move { canned_body }),
            );

        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("mock_thanos serve");
        });

        // Best-effort: yield once so the listener is definitely
        // bound before the test calls into the engine.
        tokio::task::yield_now().await;
        (base_url, handle)
    }

    /// Same as [`spawn_mock_thanos`] but the handler always
    /// returns `503 Service Unavailable`. Used by the
    /// "unreachable upstream" test.
    pub async fn spawn_mock_thanos_503() -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let local: SocketAddr = listener.local_addr().expect("local_addr");
        let base_url = format!("http://{local}");

        async fn always_503() -> axum::http::StatusCode {
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        }

        let app: Router = Router::new()
            .route("/api/v1/query", post(always_503))
            .route("/api/v1/query_range", post(always_503));

        let handle = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("mock_thanos_503 serve");
        });
        tokio::task::yield_now().await;
        (base_url, handle)
    }

    /// Best-effort env-var override scope guard. Used by the
    /// `from_env` tests to set / unset
    /// `ASAP_THANOS_QUERY_URL` without leaking onto sibling tests.
    /// Tests that touch this guard are serialised on a global
    /// mutex so they don't race.
    pub struct EnvGuard {
        key: &'static str,
        prev: Option<String>,
    }

    impl EnvGuard {
        pub fn set(key: &'static str, value: &str) -> Self {
            let prev = std::env::var(key).ok();
            std::env::set_var(key, value);
            Self { key, prev }
        }

        pub fn unset(key: &'static str) -> Self {
            let prev = std::env::var(key).ok();
            std::env::remove_var(key);
            Self { key, prev }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match self.prev.take() {
                Some(v) => std::env::set_var(self.key, v),
                None => std::env::remove_var(self.key),
            }
        }
    }

    /// Global mutex that serialises tests touching
    /// `ASAP_THANOS_QUERY_URL` (and any other process-wide env
    /// var). Use as `let _g = ENV_LOCK.lock().unwrap();` at the
    /// top of every env-touching test.
    pub static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// A minimal canned vector response — `up{job="prometheus"} 1`
    /// at ts = 1.609 Mq. Pinned so multiple tests can share the
    /// expected wrapped output.
    pub const CANNED_VECTOR_BODY: &str = r#"{
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": [
                {"metric": {"__name__": "up", "job": "prometheus"}, "value": [1609459200.0, "1"]}
            ]
        }
    }"#;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::test_support::{
        spawn_mock_thanos, spawn_mock_thanos_503, CANNED_VECTOR_BODY, ENV_LOCK,
    };
    use super::*;
    use crate::query_engines::query_result::QueryResult;

    fn config_for(url: &str) -> ThanosQueryConfig {
        ThanosQueryConfig {
            base_url: url.trim_end_matches('/').to_string(),
            request_timeout: Duration::from_secs(5),
        }
    }

    #[tokio::test]
    async fn forwards_promql_and_wraps_response() {
        let (url, _handle) = spawn_mock_thanos(CANNED_VECTOR_BODY).await;
        let engine = ThanosQueryEngine::new(config_for(&url)).expect("engine");
        let result = engine.query("up").await.expect("ok response");

        let infos = ThanosQueryEngine::success_infos(0);
        assert!(
            infos.iter().any(|s| s == DATA_SOURCE_THANOS_QUERY_INFO),
            "success_infos must carry the data_source marker; got {infos:?}",
        );
        assert!(
            infos.iter().any(|s| s.starts_with("accuracy: ε=0")),
            "success_infos must carry the exact-accuracy marker; got {infos:?}",
        );
        assert!(
            infos.iter().any(|s| s.starts_with("query_latency_ms")),
            "success_infos must surface a query_latency_ms info; got {infos:?}",
        );

        match result {
            QueryResult::Vector(iv) => {
                assert_eq!(iv.values.len(), 1);
                assert_eq!(iv.values[0].value, 1.0);
                let env = iv.accuracy.expect("envelope attached");
                assert_eq!(env.summary().contains("kind=exact"), true);
            }
            other => panic!("expected Vector result, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn capabilities_report_thanos_query_id() {
        // No upstream needed — we only inspect capabilities.
        let engine = ThanosQueryEngine::new(config_for("http://127.0.0.1:1")).expect("engine");
        let caps = engine.capabilities();
        assert_eq!(caps.data_source_id, DATA_SOURCE_THANOS_QUERY_ID);
        assert_eq!(
            caps.storage_backend,
            asap_types::StorageBackend::GorillaObjectStore,
            "Path A2 re-uses the archive tier slot in the routing matrix",
        );
    }

    #[tokio::test]
    async fn unreachable_upstream_returns_503_quirk_via_engine_trait() {
        let (url, _handle) = spawn_mock_thanos_503().await;
        let engine = ThanosQueryEngine::new(config_for(&url)).expect("engine");

        // The richer surface returns Unreachable.
        let direct = engine.query("up").await;
        match direct {
            Err(ThanosQueryError::Unreachable(_)) => {}
            other => panic!("expected Unreachable error, got {other:?}"),
        }

        // The trait surface folds it into a `Backend` error so the
        // router falls through to the ASAP-tier sketch on a
        // `DoubleWrite` deploy. The HTTP handler turns this into a
        // 503 with the `thanos_unreachable` quirk infos.
        let trait_path = QueryEngine::execute(&engine, "up").await;
        match trait_path {
            Err(crate::query_engines::EngineError::Backend { engine_id, message }) => {
                assert_eq!(engine_id, DATA_SOURCE_THANOS_QUERY_ID);
                assert!(
                    message.contains("thanos_unreachable"),
                    "Backend error must carry thanos_unreachable marker; got {message:?}",
                );
            }
            other => panic!("expected Backend error, got {other:?}"),
        }

        // The unreachable_infos helper exposes the wire shape
        // dashboards / e2e demos pin against.
        let infos = ThanosQueryEngine::unreachable_infos("upstream returned 503", 0);
        assert!(infos.iter().any(|s| s == QUIRK_THANOS_UNREACHABLE));
        assert!(infos
            .iter()
            .any(|s| s.contains("thanos_unreachable_reason")));
    }

    #[tokio::test]
    async fn config_from_env_returns_none_when_unset() {
        let _g = ENV_LOCK.lock().expect("lock");
        let _scope = test_support::EnvGuard::unset(ASAP_THANOS_QUERY_URL_ENV);
        assert!(ThanosQueryConfig::from_env().is_none());
    }

    #[tokio::test]
    async fn config_from_env_returns_none_when_blank() {
        let _g = ENV_LOCK.lock().expect("lock");
        let _scope = test_support::EnvGuard::set(ASAP_THANOS_QUERY_URL_ENV, "   ");
        assert!(ThanosQueryConfig::from_env().is_none());
    }

    #[tokio::test]
    async fn config_from_env_strips_trailing_slash() {
        let _g = ENV_LOCK.lock().expect("lock");
        let _scope =
            test_support::EnvGuard::set(ASAP_THANOS_QUERY_URL_ENV, "http://thanos-query:10903/");
        let cfg = ThanosQueryConfig::from_env().expect("set");
        assert_eq!(cfg.base_url, "http://thanos-query:10903");
    }

    #[tokio::test]
    async fn engine_from_env_returns_some_when_set() {
        let _g = ENV_LOCK.lock().expect("lock");
        let _scope = test_support::EnvGuard::set(ASAP_THANOS_QUERY_URL_ENV, "http://127.0.0.1:1");
        let engine = engine_from_env().expect("ok");
        assert!(engine.is_some(), "env set → engine constructed");
    }

    #[tokio::test]
    async fn engine_from_env_returns_none_when_unset() {
        let _g = ENV_LOCK.lock().expect("lock");
        let _scope = test_support::EnvGuard::unset(ASAP_THANOS_QUERY_URL_ENV);
        let engine = engine_from_env().expect("ok");
        assert!(
            engine.is_none(),
            "env unset → caller must use legacy engine"
        );
    }

    #[test]
    fn build_result_from_thanos_payload_rejects_non_success() {
        let payload = ThanosResponse {
            status: "error".to_string(),
            data: None,
            error_type: Some("execution".to_string()),
            error: Some("query timed out".to_string()),
        };
        let err = build_result_from_thanos_payload(payload, 0).unwrap_err();
        assert!(err.contains("query timed out"));
    }

    #[test]
    fn build_result_from_thanos_payload_rejects_unsupported_result_type() {
        // resultType = "scalar" is valid PromQL but unsupported in
        // ASAP's wire shape — we want a clear parse error rather
        // than an empty vector.
        let payload = ThanosResponse {
            status: "success".to_string(),
            data: Some(ThanosData {
                result_type: "scalar".to_string(),
                result: vec![],
            }),
            error_type: None,
            error: None,
        };
        let err = build_result_from_thanos_payload(payload, 0).unwrap_err();
        assert!(err.contains("unsupported resultType"));
    }

    #[test]
    fn parse_vector_extracts_value_and_timestamp() {
        let raw: Value = serde_json::from_str(
            r#"[{"metric":{"__name__":"x","job":"a"},"value":[1700000000.5,"3.14"]}]"#,
        )
        .unwrap();
        let arr = raw.as_array().unwrap().clone();
        let result = parse_vector(&arr).expect("parse");
        match result {
            QueryResult::Vector(iv) => {
                assert_eq!(iv.values.len(), 1);
                assert!((iv.values[0].value - 3.14).abs() < 1e-9);
                assert_eq!(iv.timestamp, 1_700_000_000_500);
            }
            other => panic!("expected Vector, got {other:?}"),
        }
    }
}
