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

}
