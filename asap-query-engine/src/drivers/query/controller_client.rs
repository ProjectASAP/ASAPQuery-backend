//! Client for notifying the DataCollector controller of query-side
//! capability misses — the query plane side of PR G.
//!
//! When `SimpleEngine` fails to match a query against any stored
//! aggregation (`find_compatible_aggregation` returns `None`), it
//! fires a fire-and-forget notification to the configured controller
//! with the `QueryRequirements` that failed to match. The controller
//! can then decide whether to generate a new sketch plan, push it
//! back to the backend via PR E's `POST /api/v1/streaming-config`
//! endpoint, and to the collector side via OpAMP. The query itself
//! is not retried — it falls through to the existing §5.2 fallback
//! (direct Prometheus read, SQL forwarding, etc.) and returns
//! whatever the fallback provides.
//!
//! ## Why fire-and-forget
//!
//! Retrying the query after the controller generates a plan would
//! require coordination that is out of scope for PR G:
//!   - the controller's plan generation is not instant
//!   - pushing the plan back to the backend takes a round trip
//!   - waiting for the plan to actually be reflected in the worker
//!     pool's precompute state takes at least one flush tick
//!
//! Instead PR G treats the notification as telemetry that closes a
//! feedback loop over multiple query events: the first query that
//! hits a capability miss returns a fallback answer AND kicks off
//! plan generation; subsequent queries benefit once the plan lands.
//! PR G's §5.2 fallback path remains the correctness anchor.

use std::sync::Arc;
use std::time::Duration;

use asap_types::query_requirements::QueryRequirements;
use async_trait::async_trait;
use serde::Serialize;
use tracing::{debug, warn};

/// Transport-agnostic controller notification interface.
#[async_trait]
pub trait ControllerClient: Send + Sync {
    /// Fire a capability-miss notification. The call is expected to
    /// be non-blocking for the caller in practice — the query hot
    /// path spawns this via `tokio::spawn` — but the trait method
    /// itself may perform network I/O.
    async fn notify_capability_miss(&self, requirements: &QueryRequirements) -> Result<(), String>;
}

/// Wire format for the capability-miss notification. Fields are a
/// flat, serde-friendly projection of `QueryRequirements` that does
/// not require adding `Serialize` derives to shared crates.
#[derive(Debug, Clone, Serialize)]
struct CapabilityMissPayload {
    /// Constant tag so the controller can route this payload across
    /// other notification kinds on the same endpoint in the future.
    kind: &'static str,
    metric: String,
    /// Statistic names in `Statistic::Display` form (e.g. "Sum",
    /// "Count", "Quantile"). Vec because some requirements need
    /// multiple statistics covered by a single aggregation.
    statistics: Vec<String>,
    /// Historical data range the query needs, in milliseconds.
    /// `None` for spatial-only queries.
    data_range_ms: Option<u64>,
    /// Grouping labels the query expects in its output.
    grouping_labels: Vec<String>,
    /// Normalized `{label="value"}` filter from the query.
    spatial_filter_normalized: String,
}

impl CapabilityMissPayload {
    fn from_requirements(requirements: &QueryRequirements) -> Self {
        Self {
            kind: "capability_miss",
            metric: requirements.metric.clone(),
            statistics: requirements
                .statistics
                .iter()
                .map(|s| format!("{s:?}"))
                .collect(),
            data_range_ms: requirements.data_range_ms,
            grouping_labels: requirements.grouping_labels.labels.clone(),
            spatial_filter_normalized: requirements.spatial_filter_normalized.clone(),
        }
    }
}

/// HTTP-backed `ControllerClient`. POSTs a JSON-encoded
/// `CapabilityMissPayload` to the configured endpoint with a bounded
/// timeout so a slow or unreachable controller can't stall the query
/// hot path's fire-and-forget task.
pub struct HttpControllerClient {
    endpoint: String,
    http: reqwest::Client,
}

impl HttpControllerClient {
    /// Construct a client pointing at the controller's plan endpoint.
    /// `endpoint` should be the full URL, e.g.
    /// `http://controller.svc.cluster.local:8080/api/v1/plan`.
    pub fn new(endpoint: String) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self { endpoint, http }
    }

    /// Construct with an explicit `reqwest::Client` — primarily used
    /// by tests that want to inject a mock-server URL without
    /// rebuilding the timeout setup.
    pub fn with_http(endpoint: String, http: reqwest::Client) -> Self {
        Self { endpoint, http }
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

#[async_trait]
impl ControllerClient for HttpControllerClient {
    async fn notify_capability_miss(&self, requirements: &QueryRequirements) -> Result<(), String> {
        let payload = CapabilityMissPayload::from_requirements(requirements);
        debug!(
            "capability-miss notification → {}: metric={}, stats={:?}",
            self.endpoint, payload.metric, payload.statistics
        );
        let resp = self
            .http
            .post(&self.endpoint)
            .json(&payload)
            .send()
            .await
            .map_err(|e| format!("controller POST send error: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!(
                "controller returned {} for capability-miss notification",
                resp.status()
            ));
        }
        Ok(())
    }
}

/// Map a capability-miss signal onto the DataCollector controller's
/// `POST /api/v1/plan` endpoint. The DC controller expects a
/// `QuerySpec`, not the generic `CapabilityMissPayload` that
/// [`HttpControllerClient`] sends — so this adapter translates on the
/// wire, keeping both sides agnostic of each other's internal shapes.
///
/// ## Translation rules
///
/// - `QueryRequirements.metric` → `QuerySpec.metric_name`
/// - `QueryRequirements.grouping_labels.labels` → `QuerySpec.group_by_labels`
/// - `QueryRequirements.data_range_ms` → `QuerySpec.time_window` (duration
///   string like `"60s"`; falls back to `"60s"` when the query has no
///   historical range, e.g. instant queries)
/// - `QueryRequirements.statistics` → `QuerySpec.aggregations`, mapped
///   via [`statistic_to_dc_aggregation`]
/// - `QuerySpec.accuracy_sla` is filled from
///   [`DcControllerConfig::accuracy_sla`] (default `0.95`)
///
/// ## What the DC controller does with the call
///
/// `handle_plan` runs cost-model planning, writes the resulting plan
/// into its plan store, and — if any agent/backend collectors are
/// attached via OpAMP — pushes their updated YAML config through the
/// OpAMP server. For Stage B verification this means: the backend's
/// fire-and-forget call will cause the controller to generate a new
/// `StagedPlan` visible via `GET /api/v1/plan/:metric` within ms.
pub struct DcControllerClient {
    endpoint: String,
    http: reqwest::Client,
    config: DcControllerConfig,
}

#[derive(Debug, Clone)]
pub struct DcControllerConfig {
    /// Accuracy SLA passed through to the DC controller's cost model.
    /// `[0.0, 1.0]`; DC rejects values outside this range.
    pub accuracy_sla: f64,
    /// Default `time_window` string when a `QueryRequirements` has no
    /// `data_range_ms` set (e.g. instant queries).
    pub default_time_window: String,
}

impl Default for DcControllerConfig {
    fn default() -> Self {
        Self {
            accuracy_sla: 0.95,
            default_time_window: "60s".to_string(),
        }
    }
}

impl DcControllerClient {
    pub fn new(endpoint: String) -> Self {
        Self::with_config(endpoint, DcControllerConfig::default())
    }

    pub fn with_config(endpoint: String, config: DcControllerConfig) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            endpoint,
            http,
            config,
        }
    }

    pub fn with_http(endpoint: String, http: reqwest::Client) -> Self {
        Self {
            endpoint,
            http,
            config: DcControllerConfig::default(),
        }
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Build the DC-flavoured `QuerySpec` JSON body. Exposed for tests.
    pub(crate) fn build_query_spec(&self, requirements: &QueryRequirements) -> serde_json::Value {
        let aggregations: Vec<String> = requirements
            .statistics
            .iter()
            .map(statistic_to_dc_aggregation)
            .collect();
        let time_window = match requirements.data_range_ms {
            Some(ms) if ms > 0 => format!("{}ms", ms),
            _ => self.config.default_time_window.clone(),
        };
        serde_json::json!({
            "metric_name": requirements.metric,
            "group_by_labels": requirements.grouping_labels.labels,
            "aggregations": aggregations,
            "time_window": time_window,
            "accuracy_sla": self.config.accuracy_sla,
        })
    }
}

/// Map a backend `Statistic` variant onto one of DC's accepted
/// aggregation type strings: `"quantile"`, `"cardinality"`, or
/// `"frequency"`. Anything count-like collapses to `"frequency"`
/// (which picks one of the CMS/CS sketches), `DistinctCount`-like
/// collapses to `"cardinality"` (HLL), and quantile-like collapses to
/// `"quantile"` (KLL/DDSketch).
fn statistic_to_dc_aggregation(stat: &promql_utilities::query_logics::enums::Statistic) -> String {
    use promql_utilities::query_logics::enums::Statistic::*;
    match stat {
        Quantile => "quantile".to_string(),
        Cardinality => "cardinality".to_string(),
        // Count / Sum / Increase / Rate / Min / Max / Topk all map to
        // "frequency", which in DC's planner picks one of the
        // CountMin / CountSketch family.
        Count | Sum | Increase | Rate | Min | Max | Topk => "frequency".to_string(),
    }
}

#[async_trait]
impl ControllerClient for DcControllerClient {
    async fn notify_capability_miss(&self, requirements: &QueryRequirements) -> Result<(), String> {
        let body = self.build_query_spec(requirements);
        debug!(
            "DC capability-miss notification → {}: body={}",
            self.endpoint, body
        );
        let resp = self
            .http
            .post(&self.endpoint)
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("DC controller POST send error: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!(
                "DC controller returned {} for /api/v1/plan",
                resp.status()
            ));
        }
        Ok(())
    }
}

/// Fire-and-forget helper used by the query hot path. Spawns the
/// notification on the current tokio runtime so the query return
/// path is not blocked on network I/O. Does nothing when
/// `client` is `None`.
///
/// Any notification error is logged at WARN level — capability
/// misses are best-effort and must never fail the query.
pub fn spawn_capability_miss_notify(
    client: &Option<Arc<dyn ControllerClient>>,
    requirements: &QueryRequirements,
) {
    let Some(client) = client.clone() else {
        return;
    };
    // Clone the `QueryRequirements` so the spawned task owns its own
    // copy — the hot-path reference does not outlive this stack
    // frame.
    let requirements = requirements.clone();
    // The caller is always on a tokio runtime (axum handlers,
    // precompute engine, etc.), so `tokio::spawn` is safe. If we
    // ever need to call this from a non-async context we will have
    // to carry a `Handle` explicitly.
    tokio::spawn(async move {
        if let Err(e) = client.notify_capability_miss(&requirements).await {
            warn!(
                "capability-miss controller notification failed: {} \
                 (metric={}, stats={:?})",
                e, requirements.metric, requirements.statistics
            );
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use promql_utilities::data_model::KeyByLabelNames;
    use promql_utilities::query_logics::enums::Statistic;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    fn test_requirements() -> QueryRequirements {
        QueryRequirements {
            metric: "http_requests_total".to_string(),
            statistics: vec![Statistic::Sum],
            data_range_ms: Some(60_000),
            grouping_labels: KeyByLabelNames::new(vec!["service".to_string()]),
            spatial_filter_normalized: r#"status="200""#.to_string(),
        }
    }

    #[test]
    fn payload_projects_all_requirement_fields() {
        let req = test_requirements();
        let payload = CapabilityMissPayload::from_requirements(&req);
        assert_eq!(payload.kind, "capability_miss");
        assert_eq!(payload.metric, "http_requests_total");
        assert_eq!(payload.statistics, vec!["Sum".to_string()]);
        assert_eq!(payload.data_range_ms, Some(60_000));
        assert_eq!(payload.grouping_labels, vec!["service".to_string()]);
        assert_eq!(payload.spatial_filter_normalized, r#"status="200""#);
        // Round-trip through JSON to confirm Serialize impl works.
        let json = serde_json::to_string(&payload).unwrap();
        assert!(json.contains("\"kind\":\"capability_miss\""));
        assert!(json.contains("\"metric\":\"http_requests_total\""));
        assert!(json.contains("\"grouping_labels\":[\"service\"]"));
    }

    /// Mock client that records calls, used by SimpleEngine unit
    /// tests in other modules to verify fire-and-forget wiring
    /// without needing an HTTP mock server.
    pub struct MockControllerClient {
        pub calls: Mutex<Vec<QueryRequirements>>,
        pub call_count: AtomicUsize,
    }

    impl MockControllerClient {
        pub fn new() -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                call_count: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl ControllerClient for MockControllerClient {
        async fn notify_capability_miss(
            &self,
            requirements: &QueryRequirements,
        ) -> Result<(), String> {
            self.call_count.fetch_add(1, Ordering::Relaxed);
            self.calls.lock().unwrap().push(requirements.clone());
            Ok(())
        }
    }

    #[tokio::test]
    async fn spawn_helper_is_noop_when_client_is_none() {
        let none: Option<Arc<dyn ControllerClient>> = None;
        let req = test_requirements();
        // Should not panic.
        spawn_capability_miss_notify(&none, &req);
    }

    #[tokio::test]
    async fn spawn_helper_invokes_client_via_tokio_spawn() {
        let mock = Arc::new(MockControllerClient::new());
        let client: Option<Arc<dyn ControllerClient>> =
            Some(mock.clone() as Arc<dyn ControllerClient>);
        let req = test_requirements();
        spawn_capability_miss_notify(&client, &req);
        // The spawn is fire-and-forget; yield to let it run.
        tokio::task::yield_now().await;
        // Spin briefly to cover runtime scheduling slop.
        for _ in 0..50 {
            if mock.call_count.load(Ordering::Relaxed) > 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(mock.call_count.load(Ordering::Relaxed), 1);
        let recorded = mock.calls.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].metric, "http_requests_total");
    }

    #[tokio::test]
    async fn http_client_reports_non_success_status() {
        use axum::routing::post;
        use axum::Router;
        let app = Router::new().route(
            "/api/v1/plan",
            post(|| async { (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "no plan") }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let client = HttpControllerClient::new(format!("http://{addr}/api/v1/plan"));
        let req = test_requirements();
        let result = client.notify_capability_miss(&req).await;
        assert!(result.is_err(), "expected Err on 500, got {result:?}");
        assert!(result.unwrap_err().contains("500"));
    }

    #[tokio::test]
    async fn http_client_success_path_round_trips_payload() {
        use axum::extract::State;
        use axum::routing::post;
        use axum::Router;
        use std::sync::Arc as StdArc;
        #[derive(Clone)]
        struct SharedSink(StdArc<Mutex<Vec<serde_json::Value>>>);
        let sink = SharedSink(StdArc::new(Mutex::new(Vec::new())));
        let sink_clone = sink.clone();
        let app = Router::new()
            .route(
                "/api/v1/plan",
                post(
                    |State(sink): State<SharedSink>, body: axum::body::Bytes| async move {
                        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
                        sink.0.lock().unwrap().push(v);
                        axum::http::StatusCode::OK
                    },
                ),
            )
            .with_state(sink_clone);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let client = HttpControllerClient::new(format!("http://{addr}/api/v1/plan"));
        client
            .notify_capability_miss(&test_requirements())
            .await
            .expect("notify ok");

        let received = sink.0.lock().unwrap();
        assert_eq!(received.len(), 1);
        let payload = &received[0];
        assert_eq!(payload["kind"], "capability_miss");
        assert_eq!(payload["metric"], "http_requests_total");
        assert_eq!(payload["statistics"], serde_json::json!(["Sum"]));
        assert_eq!(payload["data_range_ms"], 60_000);
    }

    // ------------------------------------------------------------------
    // DcControllerClient tests
    // ------------------------------------------------------------------

    #[test]
    fn dc_build_query_spec_projects_statistics_to_aggregations() {
        let client = DcControllerClient::new("http://unused/api/v1/plan".to_string());
        let req = test_requirements();
        let body = client.build_query_spec(&req);
        assert_eq!(body["metric_name"], "http_requests_total");
        assert_eq!(body["aggregations"], serde_json::json!(["frequency"]));
        assert_eq!(body["group_by_labels"], serde_json::json!(["service"]));
        assert_eq!(body["time_window"], "60000ms");
        assert_eq!(body["accuracy_sla"], 0.95);
    }

    #[test]
    fn dc_build_query_spec_defaults_time_window_for_instant_query() {
        let client = DcControllerClient::new("http://unused/api/v1/plan".to_string());
        let mut req = test_requirements();
        req.data_range_ms = None;
        let body = client.build_query_spec(&req);
        assert_eq!(body["time_window"], "60s");
    }

    #[test]
    fn dc_build_query_spec_maps_quantile_statistic() {
        use promql_utilities::query_logics::enums::Statistic;
        let client = DcControllerClient::new("http://unused/api/v1/plan".to_string());
        let mut req = test_requirements();
        req.statistics = vec![Statistic::Quantile];
        let body = client.build_query_spec(&req);
        assert_eq!(body["aggregations"], serde_json::json!(["quantile"]));
    }

    #[test]
    fn dc_build_query_spec_maps_cardinality_statistic() {
        use promql_utilities::query_logics::enums::Statistic;
        let client = DcControllerClient::new("http://unused/api/v1/plan".to_string());
        let mut req = test_requirements();
        req.statistics = vec![Statistic::Cardinality];
        let body = client.build_query_spec(&req);
        assert_eq!(body["aggregations"], serde_json::json!(["cardinality"]));
    }

    #[tokio::test]
    async fn dc_client_round_trips_against_mock_plan_endpoint() {
        // Mock the DC /api/v1/plan endpoint, capture the body, and
        // assert every QuerySpec field is present and shaped the way
        // DC's analyzer expects.
        use axum::extract::State;
        use axum::routing::post;
        use axum::Router;
        use std::sync::Arc as StdArc;
        #[derive(Clone)]
        struct SharedSink(StdArc<Mutex<Vec<serde_json::Value>>>);
        let sink = SharedSink(StdArc::new(Mutex::new(Vec::new())));
        let sink_clone = sink.clone();
        let app = Router::new()
            .route(
                "/api/v1/plan",
                post(
                    |State(sink): State<SharedSink>, body: axum::body::Bytes| async move {
                        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
                        sink.0.lock().unwrap().push(v);
                        axum::http::StatusCode::OK
                    },
                ),
            )
            .with_state(sink_clone);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let client = DcControllerClient::new(format!("http://{addr}/api/v1/plan"));
        client
            .notify_capability_miss(&test_requirements())
            .await
            .expect("notify ok");

        let received = sink.0.lock().unwrap();
        assert_eq!(received.len(), 1);
        let body = &received[0];
        assert_eq!(body["metric_name"], "http_requests_total");
        assert_eq!(body["aggregations"], serde_json::json!(["frequency"]));
        assert_eq!(body["group_by_labels"], serde_json::json!(["service"]));
        assert_eq!(body["time_window"], "60000ms");
        assert_eq!(body["accuracy_sla"], 0.95);
    }

    #[tokio::test]
    async fn dc_client_reports_non_success_status() {
        use axum::routing::post;
        use axum::Router;
        let app = Router::new().route(
            "/api/v1/plan",
            post(|| async {
                (
                    axum::http::StatusCode::UNPROCESSABLE_ENTITY,
                    "invalid accuracy_sla",
                )
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let client = DcControllerClient::new(format!("http://{addr}/api/v1/plan"));
        let result = client.notify_capability_miss(&test_requirements()).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("422"));
    }
}
