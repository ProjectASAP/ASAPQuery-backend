//! Over-HTTP end-to-end test for the capability-miss feedback loop
//! (blocker #3 in the sketch-DB TODO).
//!
//! The in-process version of this loop already lives in
//! `simple_engine.rs::e2e_feedback_loop_tests` — it swaps the
//! `StreamingConfigHandle` handle directly from a mock
//! `ControlPlaneClient`. What was missing, and what this file adds,
//! is the **real HTTP round-trip**:
//!
//! ```text
//!  backend HTTP server
//!    │   1. PromQL instant query (capability miss)
//!    ▼
//!  ASAPQueryEngine.find_compatible_aggregation_with_miss_notify
//!    │   2. fire-and-forget HttpControlPlaneClient POST
//!    ▼
//!  mock control-plane HTTP server (this file)
//!    │   3. receive miss → craft config YAML
//!    ▼
//!  POST backend:/api/v1/streaming-config (real HTTP)
//!    │   4. StreamingConfigHandle.swap
//!    ▼
//!  GET backend:/api/v1/streaming-config
//!        → aggregation_count ≥ 1 (loop closed)
//! ```
//!
//! The test measures `t_plan_ready` (wall-clock from query issue
//! to the backend observing the new config) so the paper's
//! "control plane reacts to workload drift in T seconds" claim has
//! a concrete local floor.
//!
//! Scope note: we do not ingest samples here. The "next query actually
//! returns data" half is covered by the production-process E2E. This file
//! locks down the HTTP-boundary plan-arrival behavior only. A config alone
//! does not make a repeat query servable under the current lazy-SID model.

use crate::drivers::control_plane_client::{ControlPlaneClient, HttpControlPlaneClient};
use crate::drivers::query::adapters::AdapterConfig;
use crate::drivers::query::servers::http::{HttpServer, HttpServerConfig};
use crate::query_engines::ASAPQueryEngine;
#[cfg(test)]
use crate::storage_engines::types::{StreamingConfig, StreamingConfigHandle};
use axum::{extract::State, routing::post, Router};
use reqwest::Client;
use serde_json::Value;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::time::sleep;

/// State the mock-control-plane axum handler reaches into.
///
/// `backend_config_url` is held as `Mutex<Option<String>>` because
/// the control plane has to be started *before* the backend (so the
/// backend can be told where to send its miss notification), but
/// the control plane only needs the backend URL later, when it
/// actually handles a request. This lets us bring both servers
/// up in any order and set the URL once they're both listening.
#[derive(Clone)]
struct MockControlPlaneState {
    received_count: Arc<AtomicUsize>,
    pushed_plan_ts: Arc<Mutex<Option<Instant>>>,
    backend_config_url: Arc<Mutex<Option<String>>>,
    plan_yaml: Arc<String>,
    http: Client,
}

/// Hand-authored StreamingConfig the mock control plane pushes when
/// it receives the miss. Shape matches the backend's
/// `StreamingConfig::from_yaml_data` parser (see
/// `asap-query-engine/examples/promql/streaming_config.yaml`).
///
/// PR 5: `aggregationId` is silently dropped on read; the test derives
/// the expected fingerprint from the YAML's content.
fn canned_plan_yaml(_agg_id: u64, metric: &str) -> String {
    format!(
        "aggregations:
- aggregationType: Sum
  aggregationSubType: ''
  labels:
    grouping: []
    rollup: []
    aggregated: []
  metric: {metric}
  parameters: {{}}
  windowSize: 60
  windowType: tumbling
  spatialFilter: ''
"
    )
}

/// Compute the policy-fingerprint u64 the backend will derive when it
/// parses [`canned_plan_yaml`] with the given `metric`. Lets the e2e
/// test assert the exact id without coupling to the fingerprint algo.
fn expected_fp_for(metric: &str) -> u64 {
    let yaml = canned_plan_yaml(0, metric);
    let data: serde_yaml::Value = serde_yaml::from_str(&yaml).expect("yaml parses");
    let sc = crate::storage_engines::types::StreamingConfig::from_yaml_data(&data)
        .expect("yaml decodes");
    *sc.materializations_by_policy_fingerprint
        .keys()
        .next()
        .expect("one agg in the canned plan")
}

async fn mock_control_plane_plan_handler(
    State(state): State<MockControlPlaneState>,
    body: axum::body::Bytes,
) -> axum::http::StatusCode {
    state.received_count.fetch_add(1, Ordering::SeqCst);

    // Sanity: HttpControlPlaneClient tags the body with
    // `kind: capability_miss`.
    let parsed: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    assert_eq!(
        parsed["kind"], "capability_miss",
        "mock control plane received non-miss payload: {parsed:?}"
    );

    let Some(backend_url) = state.backend_config_url.lock().unwrap().clone() else {
        eprintln!("mock control plane: backend URL not yet set — test bug");
        return axum::http::StatusCode::INTERNAL_SERVER_ERROR;
    };

    match state
        .http
        .post(backend_url)
        .body(state.plan_yaml.to_string())
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => {
            *state.pushed_plan_ts.lock().unwrap() = Some(Instant::now());
            axum::http::StatusCode::OK
        }
        Ok(r) => {
            eprintln!(
                "mock control plane: backend rejected config: {}",
                r.status()
            );
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        }
        Err(e) => {
            eprintln!("mock control plane: backend POST failed: {e}");
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

async fn start_mock_control_plane(state: MockControlPlaneState) -> u16 {
    let app = Router::new()
        .route("/api/v1/plan", post(mock_control_plane_plan_handler))
        .with_state(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    sleep(Duration::from_millis(50)).await;
    port
}

async fn start_backend(control_plane_url: String, hot_reload: StreamingConfigHandle) -> u16 {
    let _streaming_config = hot_reload.snapshot();
    let engine = Arc::new(
        ASAPQueryEngine::new(15_000)
            .with_control_plane_client(Arc::new(HttpControlPlaneClient::new(control_plane_url))
                as Arc<dyn ControlPlaneClient>),
    );

    // No fallback — we want engine-miss to be visible to the
    // test and stay out of the hot-vs-cold routing question.
    let adapter_config = AdapterConfig::new(
        crate::storage_engines::types::enums::QueryProtocol::PrometheusHttp,
        crate::storage_engines::types::QueryLanguage::PromQl,
        None,
    );
    let config = HttpServerConfig {
        port: 0,
        handle_http_requests: true,
        adapter_config,
    };
    let idx = std::sync::Arc::new(crate::storage_engines::sketch_db::index::SketchStore::new());
    let server = HttpServer::new(config, engine, idx).with_hot_reload_config(hot_reload.clone());
    server
        .start_test_server()
        .await
        .expect("Failed to start backend test server")
}

async fn poll_until_plan_active(
    client: &Client,
    backend_url: &str,
    expected_agg_id: u64,
    timeout: Duration,
) -> Option<Instant> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let r = client
            .get(format!("{backend_url}/api/v1/streaming-config"))
            .send()
            .await;
        if let Ok(r) = r {
            if r.status().is_success() {
                let body: Value = r.json().await.unwrap_or(Value::Null);
                let ids = body["aggregation_ids"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default();
                if ids.iter().any(|v| v.as_u64() == Some(expected_agg_id)) {
                    return Some(Instant::now());
                }
            }
        }
        sleep(Duration::from_millis(10)).await;
    }
    None
}

/// Bring up a full control-plane+backend pair linked in both
/// directions, ready to answer queries. Returns the backend URL,
/// the control-plane state (for assertions), and the hot-reload
/// handle.
async fn spin_up_loop(
    metric: &str,
    expected_agg_id: u64,
) -> (String, MockControlPlaneState, StreamingConfigHandle) {
    let control_plane_state = MockControlPlaneState {
        received_count: Arc::new(AtomicUsize::new(0)),
        pushed_plan_ts: Arc::new(Mutex::new(None)),
        backend_config_url: Arc::new(Mutex::new(None)),
        plan_yaml: Arc::new(canned_plan_yaml(expected_agg_id, metric)),
        http: Client::new(),
    };

    // 1. control plane up (no backend URL yet)
    let control_plane_port = start_mock_control_plane(control_plane_state.clone()).await;
    let control_plane_url = format!("http://127.0.0.1:{control_plane_port}/api/v1/plan");

    // 2. backend up, with control-plane URL baked in
    let hot_reload = StreamingConfigHandle::new(StreamingConfig::default());
    let backend_port = start_backend(control_plane_url, hot_reload.clone()).await;
    let backend_url = format!("http://127.0.0.1:{backend_port}");

    // 3. tell the control plane where to push
    *control_plane_state.backend_config_url.lock().unwrap() =
        Some(format!("{backend_url}/api/v1/streaming-config"));

    (backend_url, control_plane_state, hot_reload)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_capability_miss_feedback_loop_closes_over_http() {
    let metric = "http_e2e_metric";
    // PR 5: the on-the-wire agg_id is the policy fingerprint of the
    // canned plan's content; derive it here so the assertions match.
    let expected_agg_id: u64 = expected_fp_for(metric);

    let (backend_url, control_plane_state, _hot_reload) =
        spin_up_loop(metric, expected_agg_id).await;
    let client = Client::new();

    // 1. Fire the capability-miss query.
    let t_query = Instant::now();
    let _ = client
        .get(format!("{backend_url}/api/v1/query"))
        .query(&[("query", format!("sum({metric})").as_str()), ("time", "0")])
        .send()
        .await
        .expect("query should send")
        .bytes()
        .await;

    // 2. Poll until the plan is live on the backend.
    let plan_ready = poll_until_plan_active(
        &client,
        &backend_url,
        expected_agg_id,
        Duration::from_secs(3),
    )
    .await
    .expect(
        "capability-miss feedback loop did not close within 3s over HTTP — \
         the control-plane-notify call, the /api/v1/plan handler, or the \
         /api/v1/streaming-config POST did not complete in time",
    );

    let t_plan_ready_ms = plan_ready.duration_since(t_query).as_millis();
    println!("http-e2e: time_to_plan_ready = {t_plan_ready_ms}ms (1 control-plane hop, localhost)");
    assert!(
        t_plan_ready_ms < 2_000,
        "time_to_plan_ready = {t_plan_ready_ms}ms, expected < 2000ms"
    );

    // 3. Shape check: the exact agg_id the control plane pushed is
    //    present on the backend now.
    let body: Value = client
        .get(format!("{backend_url}/api/v1/streaming-config"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["aggregation_count"], 1);
    let ids = body["aggregation_ids"].as_array().unwrap();
    assert_eq!(ids.len(), 1);
    assert_eq!(ids[0].as_u64().unwrap(), expected_agg_id);

    // 4. Mock control plane actually handled the notify (guards
    //    against silent short-circuit paths). The engine may
    //    fire the notify more than once per query — multiple
    //    code paths (timeline dispatch, capability matching,
    //    per-segment resolution) can each observe the miss
    //    before the plan lands — so we assert `>= 1` here.
    let count = control_plane_state.received_count.load(Ordering::SeqCst);
    assert!(
        count >= 1,
        "mock control plane should have received at least one miss, got {count}"
    );
    assert!(
        control_plane_state.pushed_plan_ts.lock().unwrap().is_some(),
        "mock control plane should have pushed plan back to backend"
    );
}
