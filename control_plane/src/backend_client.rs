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

/// Classification of an HTTP push failure used by the retry layer in
/// [`crate::emit::backend_push`]. Transient errors are safe to retry
/// (the controller raced ahead of the backend's route bind, the host
/// hasn't finished accepting TCP yet, a transient 5xx during backend
/// startup, etc.); permanent errors indicate the call itself is wrong
/// (bad payload, 4xx other than 404) and retrying just amplifies the
/// log noise without improving the outcome.
///
/// This type sits next to [`BackendClient`] because the classification
/// is a property of how the backend responded, not of how the caller
/// retries — keeping the classifier here lets every endpoint method
/// share the same transient/permanent definition.
#[derive(Debug)]
pub enum BackendPostError {
    /// Worth another attempt after backoff: connection refused,
    /// connect timeout, DNS resolution failure, 404 (route not yet
    /// registered), or any 5xx.
    Transient(anyhow::Error),
    /// Will fail again the same way: 4xx other than 404 (bad payload,
    /// auth, etc.). The retry layer surfaces these immediately.
    Permanent(anyhow::Error),
}

impl BackendPostError {
    pub fn is_transient(&self) -> bool {
        matches!(self, BackendPostError::Transient(_))
    }

    pub fn into_inner(self) -> anyhow::Error {
        match self {
            BackendPostError::Transient(e) | BackendPostError::Permanent(e) => e,
        }
    }
}

impl std::fmt::Display for BackendPostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackendPostError::Transient(e) => write!(f, "transient: {e}"),
            BackendPostError::Permanent(e) => write!(f, "permanent: {e}"),
        }
    }
}

impl std::error::Error for BackendPostError {}

/// Inspect a `reqwest::Error` and decide whether the failure is worth
/// retrying. Connection-level failures (no TCP socket / DNS / connect
/// timeout) and request-side timeouts are transient — the backend is
/// likely still coming up.
fn classify_reqwest_error(err: reqwest::Error) -> BackendPostError {
    if err.is_connect() || err.is_timeout() || err.is_request() {
        BackendPostError::Transient(anyhow::Error::new(err))
    } else {
        // body decode, redirect loops, etc. — these will not resolve
        // themselves on retry.
        BackendPostError::Permanent(anyhow::Error::new(err))
    }
}

/// Map a non-2xx HTTP status to a [`BackendPostError`]. 404 and 5xx
/// are transient; every other 4xx is permanent.
fn classify_http_status(status: reqwest::StatusCode, body: String, what: &str) -> BackendPostError {
    let err = anyhow::anyhow!("backend returned {} for {}: {}", status, what, body);
    if status == reqwest::StatusCode::NOT_FOUND || status.is_server_error() {
        BackendPostError::Transient(err)
    } else {
        BackendPostError::Permanent(err)
    }
}

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

    /// Typed sibling of [`Self::post_streaming_config_json`] for the
    /// retry layer. Returns the same `Ok(())` on 2xx, but on failure
    /// classifies the underlying cause as [`BackendPostError::Transient`]
    /// or [`BackendPostError::Permanent`] so the caller can decide
    /// whether another attempt is worthwhile.
    ///
    /// Public-interface preservation: the existing
    /// [`Self::post_streaming_config_json`] remains untouched — callers
    /// that don't need retry semantics keep their `anyhow::Result`
    /// shape. The retry layer in `emit::backend_push` uses this typed
    /// variant.
    pub async fn post_streaming_config_json_typed(
        &self,
        json: String,
    ) -> std::result::Result<(), BackendPostError> {
        debug!(
            endpoint = %self.endpoint,
            json_bytes = json.len(),
            "posting streaming-config JSON to ASAPQuery-backend (typed)"
        );
        let resp = self
            .http
            .post(&self.endpoint)
            .header("content-type", "application/json")
            .body(json)
            .send()
            .await
            .map_err(classify_reqwest_error)?;

        let status = resp.status();
        if status.is_success() {
            Ok(())
        } else {
            let body = resp.text().await.unwrap_or_default();
            Err(classify_http_status(
                status,
                body,
                "streaming-config JSON POST",
            ))
        }
    }

    /// Typed sibling of [`Self::post_storage_routing_json`] for the
    /// retry layer. Identical contract to
    /// [`Self::post_streaming_config_json_typed`].
    pub async fn post_storage_routing_json_typed(
        &self,
        json: String,
    ) -> std::result::Result<(), BackendPostError> {
        let url = derive_storage_routing_url(&self.endpoint);
        debug!(
            endpoint = %url,
            json_bytes = json.len(),
            "posting storage-routing JSON to ASAPQuery-backend (typed)"
        );
        let resp = self
            .http
            .post(&url)
            .header("content-type", "application/json")
            .body(json)
            .send()
            .await
            .map_err(classify_reqwest_error)?;

        let status = resp.status();
        if status.is_success() {
            Ok(())
        } else {
            let body = resp.text().await.unwrap_or_default();
            Err(classify_http_status(
                status,
                body,
                "storage-routing JSON POST",
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

    /// Publish one authoritative catalog generation and all plans that reference it.
    pub async fn post_catalog_plan_typed(
        &self,
        publication: &crate::physical::publication::PhysicalPlanPublication,
        storage_routing: Option<serde_json::Value>,
        adaptation_evidence: &[crate::physical::compiler::RuntimeAdaptationEvidence],
    ) -> std::result::Result<(), BackendPostError> {
        let body = publication
            .install_request(storage_routing, adaptation_evidence.to_vec())
            .map_err(|error| BackendPostError::Permanent(anyhow::anyhow!(error)))?;
        let response = self
            .http
            .post(derive_physical_plan_url(&self.endpoint))
            .json(&body)
            .send()
            .await
            .map_err(classify_reqwest_error)?;
        let status = response.status();
        if status.is_success() {
            Ok(())
        } else {
            let body = response.text().await.unwrap_or_default();
            Err(classify_http_status(
                status,
                body,
                "catalog physical-plan POST",
            ))
        }
    }

    pub async fn discard_staged_physical_plan(
        &self,
        plan_id: u64,
        plan_version: u64,
    ) -> std::result::Result<(), BackendPostError> {
        let response = self
            .http
            .post(format!(
                "{}/discard",
                derive_physical_plan_url(&self.endpoint)
            ))
            .json(&serde_json::json!({"plan_id": plan_id, "plan_version": plan_version}))
            .send()
            .await
            .map_err(classify_reqwest_error)?;
        let status = response.status();
        if status.is_success() {
            Ok(())
        } else {
            Err(classify_http_status(
                status,
                response.text().await.unwrap_or_default(),
                "PhysicalPlan discard POST",
            ))
        }
    }

    pub async fn activate_physical_plan(
        &self,
        plan_id: u64,
        plan_version: u64,
    ) -> std::result::Result<(), BackendPostError> {
        let url = format!("{}/activate", derive_physical_plan_url(&self.endpoint));
        let response = self
            .http
            .post(&url)
            .json(&serde_json::json!({
                "plan_id": plan_id,
                "plan_version": plan_version,
            }))
            .send()
            .await
            .map_err(classify_reqwest_error)?;
        let status = response.status();
        if status.is_success() {
            Ok(())
        } else {
            let body = response.text().await.unwrap_or_default();
            Err(classify_http_status(
                status,
                body,
                "PhysicalPlan activation POST",
            ))
        }
    }
}

fn derive_physical_plan_url(endpoint: &str) -> String {
    const DASH: &str = "/api/v1/streaming-config";
    const UNDERSCORE: &str = "/api/v1/streaming_config";
    const PHYSICAL: &str = "/api/v1/physical-plan";
    endpoint
        .strip_suffix(DASH)
        .or_else(|| endpoint.strip_suffix(UNDERSCORE))
        .map(|base| format!("{base}{PHYSICAL}"))
        .unwrap_or_else(|| endpoint.to_string())
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
        let yaml = "aggregations:\n  - metric: cpu\n".to_string();
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
        let json = r#"{"aggregations":[{"metric":"latency"}]}"#.to_string();
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

    #[tokio::test]
    async fn catalog_publication_posts_canonical_document_without_legacy_bytes() {
        let snapshot: crate::physical::compiler::BackendLocalPlanningSnapshot =
            serde_json::from_str(include_str!(
                "../../docs/examples/asapquery-planning-snapshot.json"
            ))
            .unwrap();
        let publication = crate::physical::compiler::tests::quoted_snapshot(snapshot, false)
            .compile()
            .unwrap()
            .publication()
            .unwrap();
        let hits: StdArc<Mutex<Vec<serde_json::Value>>> = StdArc::new(Mutex::new(Vec::new()));
        let route_hits = hits.clone();
        let app = Router::new().route(
            "/api/v1/physical-plan",
            post(move |axum::Json(body): axum::Json<serde_json::Value>| {
                let hits = route_hits.clone();
                async move {
                    hits.lock().unwrap().push(body);
                    axum::http::StatusCode::OK
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = BackendClient::new(format!("http://{addr}/api/v1/streaming-config"));
        client
            .post_catalog_plan_typed(&publication, None, &[])
            .await
            .unwrap();
        let bodies = hits.lock().unwrap();
        assert_eq!(bodies.len(), 1);
        let envelope: crate::physical::publication::PhysicalPlanInstallRequest =
            serde_json::from_value(bodies[0].clone()).unwrap();
        assert!(envelope.storage_routing.is_none());
        assert!(envelope.adaptation_evidence.is_empty());
        for field in [
            "summary_catalog",
            "precompute_plan",
            "collector_plans",
            "transmission_plan",
            "query_plan",
            "storage_routing",
            "adaptation_evidence",
        ] {
            assert!(bodies[0].get(field).is_some(), "missing {field}");
        }
        server.abort();
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

    /// Typed-variant classification: 404 from the backend is
    /// transient (route not yet registered during startup race).
    #[tokio::test]
    async fn typed_streaming_config_404_is_transient() {
        let sink = SharedSink(StdArc::new(Mutex::new(Vec::new())));
        let url = start_mock_backend(sink.clone(), axum::http::StatusCode::NOT_FOUND).await;

        let client = BackendClient::new(url);
        let err = client
            .post_streaming_config_json_typed("{}".to_string())
            .await
            .expect_err("404 should surface as Err");
        assert!(err.is_transient(), "404 must classify as transient: {err}");
    }

    /// Typed-variant classification: 500 is transient.
    #[tokio::test]
    async fn typed_streaming_config_500_is_transient() {
        let sink = SharedSink(StdArc::new(Mutex::new(Vec::new())));
        let url =
            start_mock_backend(sink.clone(), axum::http::StatusCode::INTERNAL_SERVER_ERROR).await;

        let client = BackendClient::new(url);
        let err = client
            .post_streaming_config_json_typed("{}".to_string())
            .await
            .expect_err("500 should surface as Err");
        assert!(err.is_transient(), "500 must classify as transient: {err}");
    }

    /// Typed-variant classification: 400 (other 4xx) is permanent —
    /// retrying won't fix a malformed payload.
    #[tokio::test]
    async fn typed_streaming_config_400_is_permanent() {
        let sink = SharedSink(StdArc::new(Mutex::new(Vec::new())));
        let url = start_mock_backend(sink.clone(), axum::http::StatusCode::BAD_REQUEST).await;

        let client = BackendClient::new(url);
        let err = client
            .post_streaming_config_json_typed("{}".to_string())
            .await
            .expect_err("400 should surface as Err");
        assert!(!err.is_transient(), "400 must classify as permanent: {err}");
    }

    /// Typed-variant classification: connection refused (no listener
    /// on the target port) is transient — the backend may still be
    /// binding.
    #[tokio::test]
    async fn typed_connection_refused_is_transient() {
        // Port 1 on loopback is reserved and refuses connections.
        let client = BackendClient::new("http://127.0.0.1:1/api/v1/streaming-config");
        let err = client
            .post_streaming_config_json_typed("{}".to_string())
            .await
            .expect_err("connection refused should surface as Err");
        assert!(
            err.is_transient(),
            "connection-refused must classify as transient: {err}"
        );
    }

    /// Typed-variant happy path: 2xx returns Ok.
    #[tokio::test]
    async fn typed_streaming_config_success_is_ok() {
        let sink = SharedSink(StdArc::new(Mutex::new(Vec::new())));
        let url = start_mock_backend(sink.clone(), axum::http::StatusCode::OK).await;
        let client = BackendClient::new(url);
        client
            .post_streaming_config_json_typed("{}".to_string())
            .await
            .expect("2xx must be Ok");
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
