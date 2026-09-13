//! `PrometheusReader` — concrete `RawSampleReader` that reads
//! historical samples from a Prometheus-compatible HTTP API.
//!
//! Implements §10.2's `BackfillSource::Prometheus` variant. Also
//! works with any API-compatible alternative (VictoriaMetrics,
//! Thanos, Cortex, Mimir) — the /api/v1/query_range endpoint shape
//! is common across them.
//!
//! ## How it reads "raw samples"
//!
//! Prometheus does not expose a "give me all ingested samples"
//! API directly. The closest primitives are:
//!
//! 1. **`/api/v1/query_range`** — evaluates a PromQL expression at
//!    a fixed `step` across `[start, end]`, returning
//!    `[(ts, value), …]` per matched series. Returns what Prometheus
//!    considers the value AT each step, which for raw counters /
//!    gauges without rate / aggregation on them equals the sample
//!    at that timestamp (or the last sample ≤ that timestamp, per
//!    Prometheus's staleness rules).
//!
//! 2. **`/api/v1/read`** — the remote-read protobuf API, returns
//!    the actual raw samples. More accurate but heavier to
//!    implement.
//!
//! This reader uses option 1. With `step` set to the scrape
//! interval (default 15s), the returned values are the actual
//! ingested samples. Deployments that scrape at a different
//! cadence should pass their real scrape interval via
//! [`PrometheusReader::with_step`] before registering the reader.
//!
//! If a future release needs bit-identical replay down to the
//! sub-scrape-interval resolution, `/api/v1/read` is a drop-in
//! replacement — the trait contract is unchanged.
//!
//! ## Error policy
//!
//! Every network / parse failure is mapped to a
//! [`RawSampleReaderError`] variant. The backfill worker
//! stringifies the error into
//! `BackfillJob::error_message` and marks the job `Failed`, so a
//! broken Prometheus URL fails loud instead of quietly looping.

use async_trait::async_trait;
use serde::Deserialize;
use std::time::Duration;

use super::raw_sample_reader::{LabelFilter, RawSample, RawSampleReader, RawSampleReaderError};

/// Default evaluation step for `/api/v1/query_range` — matches the
/// typical Prometheus scrape interval. Deployments scraping at a
/// different cadence should override via `with_step`.
pub const DEFAULT_STEP: Duration = Duration::from_secs(15);

/// Reads historical samples from a Prometheus-compatible
/// `/api/v1/query_range` endpoint. See module doc for the
/// "raw samples" semantics.
pub struct PrometheusReader {
    base_url: String,
    step: Duration,
    http: reqwest::Client,
}

impl PrometheusReader {
    /// Build a reader pointing at `base_url` (e.g.
    /// `http://prom.svc:9090`). The `/api/v1/query_range`
    /// suffix is appended per-call; `base_url` should NOT include
    /// that path.
    ///
    /// The HTTP client has no timeout by default — set one via
    /// `with_timeout` if operating against a slow / flaky
    /// Prometheus. Phase 5c's `BackfillWorker` does not itself
    /// time out individual reads, so a hung Prometheus will
    /// block a whole job.
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            step: DEFAULT_STEP,
            http: reqwest::Client::new(),
        }
    }

    /// Override the evaluation step. Prometheus returns one
    /// `(ts, value)` point every `step` within the query window,
    /// so setting `step` below the scrape interval wastes
    /// bandwidth and setting it above drops samples.
    pub fn with_step(mut self, step: Duration) -> Self {
        self.step = step;
        self
    }

    /// Override the HTTP client timeout. Defaults to none.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        self
    }

    fn url(&self) -> String {
        format!("{}/api/v1/query_range", self.base_url.trim_end_matches('/'))
    }
}

#[async_trait]
impl RawSampleReader for PrometheusReader {
    async fn read_samples(
        &self,
        start_ms: u64,
        end_ms: u64,
        filter: &LabelFilter,
    ) -> Result<Vec<RawSample>, RawSampleReaderError> {
        if start_ms > end_ms {
            return Err(RawSampleReaderError::InvalidRange {
                reason: format!("start_ms {start_ms} > end_ms {end_ms}"),
            });
        }

        let query = build_promql(filter);
        // Prometheus API takes timestamps as seconds-with-fraction.
        let start_s = ms_to_fractional_seconds(start_ms);
        let end_s = ms_to_fractional_seconds(end_ms);
        let step_s = self.step.as_secs_f64();
        if step_s <= 0.0 {
            return Err(RawSampleReaderError::Other {
                reason: format!("step must be positive, got {:?} ({:?})", self.step, step_s),
            });
        }

        let response = self
            .http
            .get(self.url())
            .query(&[
                ("query", query.as_str()),
                ("start", &start_s),
                ("end", &end_s),
                ("step", &format!("{}s", step_s)),
            ])
            .send()
            .await
            .map_err(|e| RawSampleReaderError::Upstream {
                reason: format!("HTTP request failed: {e}"),
            })?;

        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|e| RawSampleReaderError::Upstream {
                reason: format!("failed to read response body: {e}"),
            })?;
        if !status.is_success() {
            return Err(RawSampleReaderError::Upstream {
                reason: format!(
                    "Prometheus returned HTTP {}: {}",
                    status,
                    body.chars().take(500).collect::<String>()
                ),
            });
        }

        let parsed: QueryRangeResponse =
            serde_json::from_str(&body).map_err(|e| RawSampleReaderError::Decode {
                reason: format!("failed to parse query_range JSON: {e}"),
            })?;
        if parsed.status != "success" {
            return Err(RawSampleReaderError::Upstream {
                reason: format!(
                    "Prometheus status={}, errorType={:?}, error={:?}",
                    parsed.status, parsed.error_type, parsed.error
                ),
            });
        }
        let Some(data) = parsed.data else {
            return Ok(Vec::new());
        };
        if data.result_type != "matrix" {
            return Err(RawSampleReaderError::Decode {
                reason: format!("expected resultType=matrix, got {:?}", data.result_type),
            });
        }

        let mut out = Vec::new();
        for series in data.result {
            let labels = render_series_key(filter.metric.as_str(), &series.metric);
            for point in series.values {
                let ts_ms = fractional_seconds_to_ms(point.timestamp);
                out.push(RawSample {
                    labels: labels.clone(),
                    timestamp_ms: ts_ms,
                    value: point.value,
                });
            }
        }
        // Prometheus returns per-series groupings with per-series
        // ordering by timestamp. For global ingest-order
        // determinism (§10.5) the worker relies on per-group
        // ordering, and each group's samples come from one series,
        // so no global sort is required. Validated in the
        // `ordering_preserved_per_series` test.
        Ok(out)
    }

    fn source_name(&self) -> &'static str {
        "PrometheusReader"
    }
}

/// Translate a `LabelFilter` into a PromQL matcher string like
/// `metric{k1="v1",k2="v2"}`. Values are wrapped in double quotes;
/// embedded double-quotes and backslashes are escaped per the
/// PromQL lexer rules.
fn build_promql(filter: &LabelFilter) -> String {
    if filter.equality.is_empty() {
        return filter.metric.clone();
    }
    let mut pairs: Vec<(&String, &String)> = filter.equality.iter().collect();
    // Stable order so the URL is deterministic (easier to debug,
    // easier to cache-hit at upstream proxies).
    pairs.sort_by(|a, b| a.0.cmp(b.0));
    let joined: String = pairs
        .iter()
        .map(|(k, v)| format!("{}=\"{}\"", k, escape_label_value(v)))
        .collect::<Vec<_>>()
        .join(",");
    format!("{}{{{}}}", filter.metric, joined)
}

fn escape_label_value(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for c in v.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
    out
}

/// Render a `metric{k1="v1",...}` series key from a Prometheus
/// response's `metric` object. Used to populate `RawSample.labels`
/// with the same shape the downstream sketch builder expects.
fn render_series_key(
    default_metric: &str,
    metric: &std::collections::HashMap<String, String>,
) -> String {
    let name = metric
        .get("__name__")
        .map(String::as_str)
        .unwrap_or(default_metric);
    if metric.len() <= 1 {
        // Only __name__ (or nothing) — bare metric name.
        return name.to_string();
    }
    let mut pairs: Vec<(&String, &String)> = metric
        .iter()
        .filter(|(k, _)| k.as_str() != "__name__")
        .collect();
    pairs.sort_by(|a, b| a.0.cmp(b.0));
    let joined: String = pairs
        .iter()
        .map(|(k, v)| format!("{}=\"{}\"", k, escape_label_value(v)))
        .collect::<Vec<_>>()
        .join(",");
    format!("{}{{{}}}", name, joined)
}

fn ms_to_fractional_seconds(ms: u64) -> String {
    // Prometheus accepts "1234567890.123". Use a simple divmod to
    // avoid locale-dependent float formatting surprises.
    let whole = ms / 1000;
    let frac = ms % 1000;
    format!("{}.{:03}", whole, frac)
}

fn fractional_seconds_to_ms(ts: f64) -> i64 {
    // Keep in i64 because `RawSample.timestamp_ms` is i64. Negative
    // timestamps are not meaningful for backfill but we don't
    // clamp — let the downstream sketch handle them if they appear.
    (ts * 1000.0).round() as i64
}

// ── Prometheus query_range response shape ───────────────────────────

#[derive(Debug, Deserialize)]
struct QueryRangeResponse {
    status: String,
    #[serde(default)]
    data: Option<QueryRangeData>,
    #[serde(default, rename = "errorType")]
    error_type: Option<String>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct QueryRangeData {
    #[serde(rename = "resultType")]
    result_type: String,
    #[serde(default)]
    result: Vec<MatrixSeries>,
}

#[derive(Debug, Deserialize)]
struct MatrixSeries {
    metric: std::collections::HashMap<String, String>,
    #[serde(default)]
    values: Vec<MatrixPoint>,
}

/// Prometheus returns each sample as the two-element array
/// `[timestamp_seconds, "value_string"]`. Custom deserialisation
/// parses the string into `f64`.
#[derive(Debug)]
struct MatrixPoint {
    timestamp: f64,
    value: f64,
}

impl<'de> Deserialize<'de> for MatrixPoint {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let (ts, val_str): (f64, String) = Deserialize::deserialize(deserializer)?;
        let value = val_str.parse::<f64>().map_err(serde::de::Error::custom)?;
        Ok(Self {
            timestamp: ts,
            value,
        })
    }
}

// ── Integration tests against a mock Prometheus HTTP server ─────────

#[cfg(test)]
mod integration_tests {
    use super::*;
    use axum::extract::Query;
    use axum::response::IntoResponse;
    use axum::{routing::get, Router};
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::Mutex;
    use tokio::net::TcpListener;

    /// Shared state captured by the mock handler so tests can
    /// assert on what the reader actually asked for.
    #[derive(Default, Clone)]
    struct MockState {
        last_params: Arc<Mutex<Option<HashMap<String, String>>>>,
        response_body: Arc<Mutex<String>>,
        response_status: Arc<Mutex<u16>>,
    }

    async fn handler(
        axum::extract::State(state): axum::extract::State<MockState>,
        Query(params): Query<HashMap<String, String>>,
    ) -> axum::response::Response {
        *state.last_params.lock().unwrap() = Some(params);
        let status = *state.response_status.lock().unwrap();
        let body = state.response_body.lock().unwrap().clone();
        let response = (
            axum::http::StatusCode::from_u16(status).unwrap(),
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            body,
        );
        response.into_response()
    }

    async fn spawn_mock(state: MockState) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let app = Router::new()
            .route("/api/v1/query_range", get(handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (addr, handle)
    }

    #[tokio::test]
    async fn happy_path_parses_samples() {
        let state = MockState::default();
        *state.response_status.lock().unwrap() = 200;
        *state.response_body.lock().unwrap() = r#"{
            "status": "success",
            "data": {
                "resultType": "matrix",
                "result": [
                    {
                        "metric": {"__name__": "latency", "svc": "a"},
                        "values": [
                            [10.0, "1.5"],
                            [25.0, "2.25"]
                        ]
                    },
                    {
                        "metric": {"__name__": "latency", "svc": "b"},
                        "values": [
                            [15.0, "9.0"]
                        ]
                    }
                ]
            }
        }"#
        .to_string();
        let (addr, handle) = spawn_mock(state.clone()).await;

        let reader = PrometheusReader::new(format!("http://{addr}"));
        let samples = reader
            .read_samples(0, 30_000, &LabelFilter::for_metric("latency"))
            .await
            .expect("read ok");

        assert_eq!(samples.len(), 3);
        // Series A's samples
        assert_eq!(samples[0].labels, r#"latency{svc="a"}"#);
        assert_eq!(samples[0].timestamp_ms, 10_000);
        assert_eq!(samples[0].value, 1.5);
        assert_eq!(samples[1].labels, r#"latency{svc="a"}"#);
        assert_eq!(samples[1].timestamp_ms, 25_000);
        assert_eq!(samples[1].value, 2.25);
        // Series B
        assert_eq!(samples[2].labels, r#"latency{svc="b"}"#);
        assert_eq!(samples[2].value, 9.0);

        // Verify the reader passed sensible params.
        let params = state.last_params.lock().unwrap().clone().unwrap();
        assert_eq!(params.get("query"), Some(&"latency".to_string()));
        assert_eq!(params.get("start"), Some(&"0.000".to_string()));
        assert_eq!(params.get("end"), Some(&"30.000".to_string()));
        assert!(params
            .get("step")
            .map(|s| s.ends_with('s'))
            .unwrap_or(false));

        handle.abort();
    }

    #[tokio::test]
    async fn filter_becomes_promql_matcher() {
        let state = MockState::default();
        *state.response_status.lock().unwrap() = 200;
        *state.response_body.lock().unwrap() = r#"{
            "status": "success",
            "data": {"resultType": "matrix", "result": []}
        }"#
        .to_string();
        let (addr, handle) = spawn_mock(state.clone()).await;

        let reader = PrometheusReader::new(format!("http://{addr}"));
        let filter = LabelFilter::for_metric("latency")
            .with_label("env", "prod")
            .with_label("svc", "a");
        let _ = reader.read_samples(0, 100, &filter).await.unwrap();

        let params = state.last_params.lock().unwrap().clone().unwrap();
        assert_eq!(
            params.get("query"),
            Some(&r#"latency{env="prod",svc="a"}"#.to_string())
        );
        handle.abort();
    }

    #[tokio::test]
    async fn http_5xx_maps_to_upstream_error() {
        let state = MockState::default();
        *state.response_status.lock().unwrap() = 503;
        *state.response_body.lock().unwrap() = "service unavailable".to_string();
        let (addr, handle) = spawn_mock(state.clone()).await;

        let reader = PrometheusReader::new(format!("http://{addr}"));
        let err = reader
            .read_samples(0, 100, &LabelFilter::for_metric("m"))
            .await
            .unwrap_err();
        match err {
            RawSampleReaderError::Upstream { reason } => {
                assert!(reason.contains("503"), "reason = {reason}");
            }
            other => panic!("expected Upstream, got {other}"),
        }
        handle.abort();
    }

    #[tokio::test]
    async fn prometheus_error_status_maps_to_upstream_error() {
        let state = MockState::default();
        *state.response_status.lock().unwrap() = 200;
        *state.response_body.lock().unwrap() = r#"{
            "status": "error",
            "errorType": "bad_data",
            "error": "parse error at char 8"
        }"#
        .to_string();
        let (addr, handle) = spawn_mock(state.clone()).await;

        let reader = PrometheusReader::new(format!("http://{addr}"));
        let err = reader
            .read_samples(0, 100, &LabelFilter::for_metric("m"))
            .await
            .unwrap_err();
        match err {
            RawSampleReaderError::Upstream { reason } => {
                assert!(reason.contains("parse error"), "reason = {reason}");
            }
            other => panic!("expected Upstream, got {other}"),
        }
        handle.abort();
    }

    #[tokio::test]
    async fn malformed_json_maps_to_decode_error() {
        let state = MockState::default();
        *state.response_status.lock().unwrap() = 200;
        *state.response_body.lock().unwrap() = "not json".to_string();
        let (addr, handle) = spawn_mock(state.clone()).await;

        let reader = PrometheusReader::new(format!("http://{addr}"));
        let err = reader
            .read_samples(0, 100, &LabelFilter::for_metric("m"))
            .await
            .unwrap_err();
        match err {
            RawSampleReaderError::Decode { .. } => {}
            other => panic!("expected Decode, got {other}"),
        }
        handle.abort();
    }

    #[tokio::test]
    async fn ordering_preserved_per_series() {
        // Prometheus returns per-series ordering by timestamp; our
        // reader preserves it. The worker's group-keying is per
        // series, so per-series ingest order is what §10.5 asks
        // for.
        let state = MockState::default();
        *state.response_status.lock().unwrap() = 200;
        *state.response_body.lock().unwrap() = r#"{
            "status": "success",
            "data": {
                "resultType": "matrix",
                "result": [{
                    "metric": {"__name__": "m"},
                    "values": [
                        [1.0, "10"],
                        [2.0, "20"],
                        [3.0, "30"]
                    ]
                }]
            }
        }"#
        .to_string();
        let (addr, handle) = spawn_mock(state.clone()).await;

        let reader = PrometheusReader::new(format!("http://{addr}"));
        let samples = reader
            .read_samples(0, 10_000, &LabelFilter::for_metric("m"))
            .await
            .unwrap();
        let ts: Vec<i64> = samples.iter().map(|s| s.timestamp_ms).collect();
        assert_eq!(ts, vec![1000, 2000, 3000]);

        handle.abort();
    }

    #[tokio::test]
    async fn empty_result_returns_empty_vec() {
        let state = MockState::default();
        *state.response_status.lock().unwrap() = 200;
        *state.response_body.lock().unwrap() = r#"{
            "status": "success",
            "data": {"resultType": "matrix", "result": []}
        }"#
        .to_string();
        let (addr, handle) = spawn_mock(state.clone()).await;

        let reader = PrometheusReader::new(format!("http://{addr}"));
        let samples = reader
            .read_samples(0, 100, &LabelFilter::for_metric("m"))
            .await
            .unwrap();
        assert!(samples.is_empty());
        handle.abort();
    }

    #[tokio::test]
    async fn wrong_result_type_returns_decode_error() {
        let state = MockState::default();
        *state.response_status.lock().unwrap() = 200;
        *state.response_body.lock().unwrap() = r#"{
            "status": "success",
            "data": {"resultType": "vector", "result": []}
        }"#
        .to_string();
        let (addr, handle) = spawn_mock(state.clone()).await;

        let reader = PrometheusReader::new(format!("http://{addr}"));
        let err = reader
            .read_samples(0, 100, &LabelFilter::for_metric("m"))
            .await
            .unwrap_err();
        match err {
            RawSampleReaderError::Decode { reason } => {
                assert!(reason.contains("matrix"), "reason = {reason}");
            }
            other => panic!("expected Decode, got {other}"),
        }
        handle.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn build_promql_bare_metric_when_no_filters() {
        let f = LabelFilter::for_metric("latency");
        assert_eq!(build_promql(&f), "latency");
    }

    #[test]
    fn build_promql_stable_ordering() {
        let f = LabelFilter::for_metric("m")
            .with_label("b", "2")
            .with_label("a", "1");
        // Sorted by key regardless of insertion order.
        assert_eq!(build_promql(&f), r#"m{a="1",b="2"}"#);
    }

    #[test]
    fn build_promql_escapes_quotes_and_backslashes() {
        let f = LabelFilter::for_metric("m").with_label("k", r#"val"with\slash"#);
        assert_eq!(build_promql(&f), r#"m{k="val\"with\\slash"}"#);
    }

    #[test]
    fn render_series_key_uses_name_and_sorts_labels() {
        let mut metric = HashMap::new();
        metric.insert("__name__".to_string(), "latency".to_string());
        metric.insert("svc".to_string(), "a".to_string());
        metric.insert("env".to_string(), "prod".to_string());
        assert_eq!(
            render_series_key("fallback", &metric),
            r#"latency{env="prod",svc="a"}"#
        );
    }

    #[test]
    fn render_series_key_handles_bare_metric() {
        let mut metric = HashMap::new();
        metric.insert("__name__".to_string(), "lone".to_string());
        assert_eq!(render_series_key("fallback", &metric), "lone");
    }

    #[test]
    fn render_series_key_falls_back_to_default_when_no_name() {
        let metric: HashMap<String, String> = HashMap::new();
        assert_eq!(render_series_key("fallback", &metric), "fallback");
    }

    #[test]
    fn fractional_seconds_round_trip() {
        assert_eq!(ms_to_fractional_seconds(0), "0.000");
        assert_eq!(ms_to_fractional_seconds(1_234_567), "1234.567");
        assert_eq!(fractional_seconds_to_ms(1.5), 1500);
        assert_eq!(fractional_seconds_to_ms(0.0), 0);
    }

    #[tokio::test]
    async fn read_samples_inverted_range_returns_invalid_range() {
        let r = PrometheusReader::new("http://unused");
        let err = r
            .read_samples(100, 50, &LabelFilter::for_metric("m"))
            .await
            .unwrap_err();
        match err {
            RawSampleReaderError::InvalidRange { .. } => {}
            other => panic!("expected InvalidRange, got {other}"),
        }
    }
}
