//! HTTP client that pushes a freshly-generated `StreamingConfig` YAML
//! to the ASAPQuery-backend's `POST /api/v1/streaming-config` endpoint.
//!
//! This is the control-plane-side **producer** of the PR E phase 1 / phase 2
//! hot-reload contract that landed in ASAPQuery-backend PRs #10 and #12.
//! The replanner calls into this module immediately after generating a
//! new plan so the backend's active `StreamingConfig` is updated without
//! a restart and subsequent queries observe the new aggregation layout.
//!
//! The client is **fire-and-forget at the call site** — the replanner
//! awaits the POST but doesn't block its own return on the outcome.
//! Errors are logged at WARN; the control plane is expected to be tolerant
//! of transient backend unavailability because the next replan cycle
//! will try again with the latest plan.

use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Client;
use tracing::{debug, warn};

/// Minimal HTTP client for ASAPQuery-backend's streaming-config endpoint.
/// Built once at control-plane startup from the
/// `CONTROL_PLANE_BACKEND_ENDPOINT` environment variable and shared
/// via `Arc` with the replanner.
#[derive(Debug, Clone)]
pub struct BackendClient {
    endpoint: String,
    http: Client,
}

impl BackendClient {
    /// Construct a client pointing at the backend's plan-push endpoint.
    /// `endpoint` should be the full URL, e.g.
    /// `http://backend.svc:8088/api/v1/streaming-config`.
    ///
    /// A 5-second timeout bounds the duration a slow or unreachable
    /// backend can stall the replanner — consistent with the symmetric
    /// 5-second timeout on ASAPQuery-backend's `HttpControlPlaneClient`
    /// (the reverse direction in the same loop).
    pub fn new(endpoint: impl Into<String>) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap_or_else(|_| Client::new());
        Self {
            endpoint: endpoint.into(),
            http,
        }
    }

    /// Construct with an explicit `reqwest::Client`. Used by tests that
    /// need to inject a mock-server URL without reconfiguring the
    /// timeout setup.
    pub fn with_http(endpoint: impl Into<String>, http: Client) -> Self {
        Self {
            endpoint: endpoint.into(),
            http,
        }
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// POST the given `StreamingConfig` YAML to the backend. Returns
    /// `Ok(())` on any 2xx status, otherwise an error carrying the
    /// status code and response body. The caller (typically
    /// [`Replanner::replan_metric`]) logs the error and moves on — the
    /// next replan cycle will retry with the latest plan.
    pub async fn push_streaming_config(&self, yaml: String) -> Result<()> {
        debug!(
            endpoint = %self.endpoint,
            yaml_bytes = yaml.len(),
            "pushing streaming-config YAML to ASAPQuery-backend"
        );
        let resp = self
            .http
            .post(&self.endpoint)
            .header("content-type", "application/x-yaml")
            .body(yaml)
            .send()
            .await
            .context("failed to POST streaming-config to backend")?;

        let status = resp.status();
        if status.is_success() {
            Ok(())
        } else {
            let body = resp.text().await.unwrap_or_default();
            Err(anyhow::anyhow!(
                "backend returned {} for streaming-config POST: {}",
                status,
                body
            ))
        }
    }

    /// Phase C (MVP v6) variant of [`Self::push_streaming_config`]
    /// that POSTs `application/json`. The typed L5
    /// `emit_backend_streaming_config_json` emitter produces a `serde_json::Value`
    /// rather than a YAML document, and the ASAPQuery-backend's
    /// `/api/v1/streaming-config` endpoint accepts both content types
    /// (PR #297 / Phase B documents the JSON shape). Same 2xx-or-error
    /// contract as the YAML variant; same fire-and-forget semantics
    /// at the call site.
    pub async fn post_streaming_config_json(&self, json: String) -> Result<()> {
        debug!(
            endpoint = %self.endpoint,
            json_bytes = json.len(),
            "posting streaming-config JSON to ASAPQuery-backend"
        );
        let resp = self
            .http
            .post(&self.endpoint)
            .header("content-type", "application/json")
            .body(json)
            .send()
            .await
            .context("failed to POST streaming-config JSON to backend")?;

        let status = resp.status();
        if status.is_success() {
            Ok(())
        } else {
            let body = resp.text().await.unwrap_or_default();
            Err(anyhow::anyhow!(
                "backend returned {} for streaming-config JSON POST: {}",
                status,
                body
            ))
        }
    }

    /// Phase α (MVP) sibling of [`Self::post_streaming_config_json`]:
    /// POSTs the control-plane-emitted `BackendStorageRouting` JSON
    /// document to the backend's `POST /api/v1/storage_routing`
    /// endpoint. The backend hot-loads the routing table and the next
    /// instant query consults the new table.
    ///
    /// Endpoint resolution: the field [`Self::endpoint`] is the
    /// control plane's configured streaming-config endpoint (e.g.
    /// `http://backend.svc:8088/api/v1/streaming-config`). We rewrite
    /// the path component from `/api/v1/streaming-config` to
    /// `/api/v1/storage_routing` so operators only configure one
    /// `CONTROL_PLANE_BACKEND_ENDPOINT` env var and both pushes land
    /// at the same backend host. URLs that don't end in
    /// `/api/v1/streaming-config` are passed through unchanged
    /// (a test-mode escape hatch — the unit test below builds a
    /// mock URL ending in `/storage_routing` directly).
    pub async fn post_storage_routing_json(&self, json: String) -> Result<()> {
        let url = derive_storage_routing_url(&self.endpoint);
        debug!(
            endpoint = %url,
            json_bytes = json.len(),
            "posting storage-routing JSON to ASAPQuery-backend"
        );
        let resp = self
            .http
            .post(&url)
            .header("content-type", "application/json")
            .body(json)
            .send()
            .await
            .context("failed to POST storage-routing JSON to backend")?;

        let status = resp.status();
        if status.is_success() {
            Ok(())
        } else {
            let body = resp.text().await.unwrap_or_default();
            Err(anyhow::anyhow!(
                "backend returned {} for storage-routing JSON POST: {}",
                status,
                body
            ))
        }
    }
}

/// Map a streaming-config endpoint URL to the sibling storage-routing
/// endpoint by rewriting the trailing path component. URLs that don't
/// end with `/api/v1/streaming-config` (or `/api/v1/streaming_config` —
/// either spelling is supported) pass through unchanged so tests can
/// inject a mock-server URL directly.
fn derive_storage_routing_url(endpoint: &str) -> String {
    const STREAMING_PATH_DASH: &str = "/api/v1/streaming-config";
    const STREAMING_PATH_UNDERSCORE: &str = "/api/v1/streaming_config";
    const ROUTING_PATH: &str = "/api/v1/storage_routing";
    if let Some(stripped) = endpoint.strip_suffix(STREAMING_PATH_DASH) {
        return format!("{stripped}{ROUTING_PATH}");
    }
    if let Some(stripped) = endpoint.strip_suffix(STREAMING_PATH_UNDERSCORE) {
        return format!("{stripped}{ROUTING_PATH}");
    }
    endpoint.to_string()
}

/// Fire-and-forget convenience helper used by the replanner. Logs
/// errors at WARN and never propagates them — the replanner should
/// never fail an entire replan because the backend was temporarily
/// unreachable.
pub async fn push_or_log(client: &BackendClient, metric: &str, yaml: String) {
    match client.push_streaming_config(yaml).await {
        Ok(()) => {
            debug!(metric, endpoint = %client.endpoint, "streaming-config push succeeded");
        }
        Err(e) => {
            warn!(
                metric,
                endpoint = %client.endpoint,
                error = %e,
                "streaming-config push to ASAPQuery-backend failed; \
                 next replan cycle will retry"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::State;
    use axum::routing::post;
    use axum::Router;
    use std::sync::{Arc as StdArc, Mutex};

    #[derive(Clone)]
    struct SharedSink(StdArc<Mutex<Vec<String>>>);

    async fn start_mock_backend(sink: SharedSink, status: axum::http::StatusCode) -> String {
        let app = Router::new()
            .route(
                "/api/v1/streaming-config",
                post(
                    move |State(sink): State<SharedSink>, body: axum::body::Bytes| async move {
                        let yaml = String::from_utf8_lossy(&body).to_string();
                        sink.0.lock().unwrap().push(yaml);
                        status
                    },
                ),
            )
            .with_state(sink);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        format!("http://{addr}/api/v1/streaming-config")
    }

    #[tokio::test]
    async fn success_path_round_trips_yaml() {
        let sink = SharedSink(StdArc::new(Mutex::new(Vec::new())));
        let url = start_mock_backend(sink.clone(), axum::http::StatusCode::OK).await;

        let client = BackendClient::new(url);
        let yaml = "aggregations:\n  - aggregationId: 42\n    metric: cpu\n".to_string();
        client
            .push_streaming_config(yaml.clone())
            .await
            .expect("push ok");

        let received = sink.0.lock().unwrap();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0], yaml);
    }

    #[tokio::test]
    async fn non_2xx_status_is_reported_as_error() {
        let sink = SharedSink(StdArc::new(Mutex::new(Vec::new())));
        let url =
            start_mock_backend(sink.clone(), axum::http::StatusCode::INTERNAL_SERVER_ERROR).await;

        let client = BackendClient::new(url);
        let result = client.push_streaming_config("whatever".to_string()).await;
        assert!(result.is_err(), "expected error on 500, got {result:?}");
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("500"), "error msg should mention 500: {msg}");
    }

    #[tokio::test]
    async fn push_or_log_swallows_errors() {
        // Point at an unreachable port so the request fails fast.
        let client = BackendClient::new("http://127.0.0.1:1/api/v1/streaming-config");
        // Must not panic or propagate — fire-and-forget semantics.
        push_or_log(&client, "cpu_usage", "content".to_string()).await;
    }

    /// Phase C: the JSON variant POSTs the body verbatim, returns
    /// `Ok(())` on a 2xx, and surfaces non-2xx as `Err`. Mock backend
    /// captures the body so we can verify it round-trips.
    #[tokio::test]
    async fn json_post_round_trips_body() {
        let sink = SharedSink(StdArc::new(Mutex::new(Vec::new())));
        let url = start_mock_backend(sink.clone(), axum::http::StatusCode::OK).await;

        let client = BackendClient::new(url);
        let json = r#"{"aggregations":[{"aggregationId":7,"metric":"latency"}]}"#.to_string();
        client
            .post_streaming_config_json(json.clone())
            .await
            .expect("json post ok");

        let received = sink.0.lock().unwrap();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0], json);
    }

    /// Phase C: non-2xx from the backend surfaces as an error so the
    /// caller (handle_plan) can log + move on.
    #[tokio::test]
    async fn json_post_non_2xx_is_error() {
        let sink = SharedSink(StdArc::new(Mutex::new(Vec::new())));
        let url = start_mock_backend(sink.clone(), axum::http::StatusCode::BAD_REQUEST).await;

        let client = BackendClient::new(url);
        let result = client.post_streaming_config_json("{}".to_string()).await;
        assert!(result.is_err(), "expected error on 400, got {result:?}");
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("400"), "error msg should mention 400: {msg}");
    }

    /// Phase α: storage-routing-URL derivation rewrites the path
    /// component when the configured endpoint ends in
    /// `/api/v1/streaming-config`, leaving everything else untouched.
    #[test]
    fn storage_routing_url_rewrites_streaming_path() {
        assert_eq!(
            derive_storage_routing_url("http://backend:8088/api/v1/streaming-config"),
            "http://backend:8088/api/v1/storage_routing"
        );
        assert_eq!(
            derive_storage_routing_url("http://backend:8088/api/v1/streaming_config"),
            "http://backend:8088/api/v1/storage_routing"
        );
    }

    #[test]
    fn storage_routing_url_preserves_unknown_paths_for_tests() {
        // Test escape hatch — a mock-server URL pointing directly at
        // `/api/v1/storage_routing` already passes through unchanged.
        assert_eq!(
            derive_storage_routing_url("http://127.0.0.1:1/api/v1/storage_routing"),
            "http://127.0.0.1:1/api/v1/storage_routing"
        );
        // Unrelated path passes through too — no surprise rewriting.
        assert_eq!(derive_storage_routing_url("http://x/foo"), "http://x/foo");
    }

    /// Phase α: full happy path. A mock backend hosts the storage
    /// routing endpoint; the client POSTs the control-plane-emitted JSON
    /// and the body round-trips verbatim. Mirrors `json_post_round_trips_body`.
    async fn start_mock_routing_backend(
        sink: SharedSink,
        status: axum::http::StatusCode,
    ) -> String {
        let app = Router::new()
            .route(
                "/api/v1/storage_routing",
                post(
                    move |State(sink): State<SharedSink>, body: axum::body::Bytes| async move {
                        let json = String::from_utf8_lossy(&body).to_string();
                        sink.0.lock().unwrap().push(json);
                        status
                    },
                ),
            )
            .with_state(sink);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        // Return the streaming-config URL — the client will rewrite
        // the path before issuing the POST.
        format!("http://{addr}/api/v1/streaming-config")
    }

    #[tokio::test]
    async fn storage_routing_post_round_trips_body_via_url_rewrite() {
        let sink = SharedSink(StdArc::new(Mutex::new(Vec::new())));
        let url = start_mock_routing_backend(sink.clone(), axum::http::StatusCode::OK).await;

        let client = BackendClient::new(url);
        let json = r#"{"default_engine":"sketch_store","metrics":[{"name":"x","targets":[]}]}"#
            .to_string();
        client
            .post_storage_routing_json(json.clone())
            .await
            .expect("routing post ok");

        let received = sink.0.lock().unwrap();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0], json);
    }

    #[tokio::test]
    async fn storage_routing_post_non_2xx_is_error() {
        let sink = SharedSink(StdArc::new(Mutex::new(Vec::new())));
        let url =
            start_mock_routing_backend(sink.clone(), axum::http::StatusCode::INTERNAL_SERVER_ERROR)
                .await;
        let client = BackendClient::new(url);
        let result = client.post_storage_routing_json("{}".to_string()).await;
        assert!(result.is_err(), "expected error on 500, got {result:?}");
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("500"), "error msg should mention 500: {msg}");
    }
}
