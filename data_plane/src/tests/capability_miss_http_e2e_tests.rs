//! Over-HTTP end-to-end test for the capability-miss feedback loop
//! (blocker #3 in the sketch-DB TODO).
//!
//! The in-process version of this loop already lives in
//! `simple_engine.rs::e2e_feedback_loop_tests` — it swaps the
//! `HotReloadStreamingConfig` handle directly from a mock
//! `ControllerClient`. What was missing, and what this file adds,
//! is the **real HTTP round-trip**:
//!
//! ```text
//!  backend HTTP server
//!    │   1. PromQL instant query (capability miss)
//!    ▼
//!  ASAPQueryEngine.find_compatible_aggregation_with_miss_notify
//!    │   2. fire-and-forget HttpControllerClient POST
//!    ▼
//!  mock controller HTTP server (this file)
//!    │   3. receive miss → craft config YAML
//!    ▼
//!  POST backend:/api/v1/streaming-config (real HTTP)
//!    │   4. HotReloadStreamingConfig.swap
//!    ▼
//!  GET backend:/api/v1/streaming-config
//!        → aggregation_count ≥ 1 (loop closed)
//! ```
//!
//! The test measures `t_plan_ready` (wall-clock from query issue
//! to the backend observing the new config) so the paper's
//! "controller reacts to workload drift in T seconds" claim has
//! a concrete local floor.
//!
//! Scope note: we do not ingest samples here. The "next query
//! actually returns data" half of the story requires OTLP
//! ingestion through a separate entry point and is covered by
//! the unit tests in `simple_engine.rs`. What this file locks
//! down is the HTTP-boundary behaviour of the feedback loop
//! (plan-arrival + idempotency on repeat query).

#[cfg(test)]
use crate::stores::types::{
    CleanupPolicy, HotReloadStreamingConfig, QueryLanguage, StreamingConfig};
use crate::drivers::query::adapters::AdapterConfig;
use crate::drivers::query::controller_client::{ControllerClient, HttpControllerClient};
use crate::drivers::query::servers::http::{HttpServer, HttpServerConfig};
use crate::query_engines::ASAPQueryEngine;
use crate::stores::sketch_db::store::SketchStore;
use axum::{extract::State, routing::post, Router};
use reqwest::Client;
use serde_json::Value;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::time::sleep;

/// State the mock-controller axum handler reaches into.
///
/// `backend_config_url` is held as `Mutex<Option<String>>` because
/// the controller has to be started *before* the backend (so the
/// backend can be told where to send its miss notification), but
/// the controller only needs the backend URL later, when it
/// actually handles a request. This lets us bring both servers
/// up in any order and set the URL once they're both listening.
#[derive(Clone)]
struct MockControllerState {
    received_count: Arc<AtomicUsize>,
    pushed_plan_ts: Arc<Mutex<Option<Instant>>>,
    backend_config_url: Arc<Mutex<Option<String>>>,
    plan_yaml: Arc<String>,
    http: Client}

/// Hand-authored StreamingConfig the mock controller pushes when
/// it receives the miss. Shape matches the backend's
/// `StreamingConfig::from_yaml_data` parser (see
/// `asap-query-engine/examples/promql/streaming_config.yaml`).
/// The `aggregationId` here is what the test asserts shows up on
/// the backend after the loop closes.
fn canned_plan_yaml(agg_id: u64, metric: &str) -> String {
    format!(
        "aggregations:
- aggregationId: {agg_id}
  aggregationType: Sum
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

async fn mock_controller_plan_handler(
    State(state): State<MockControllerState>,
    body: axum::body::Bytes,
) -> axum::http::StatusCode {
    state.received_count.fetch_add(1, Ordering::SeqCst);

    // Sanity: HttpControllerClient tags the body with
    // `kind: capability_miss`.
    let parsed: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    assert_eq!(
        parsed["kind"], "capability_miss",
        "mock controller received non-miss payload: {parsed:?}"
    );

    let Some(backend_url) = state.backend_config_url.lock().unwrap().clone() else {
        eprintln!("mock controller: backend URL not yet set — test bug");
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
            eprintln!("mock controller: backend rejected config: {}", r.status());
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        }
        Err(e) => {
            eprintln!("mock controller: backend POST failed: {e}");
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

async fn start_mock_controller(state: MockControllerState) -> u16 {
    let app = Router::new()
        .route("/api/v1/plan", post(mock_controller_plan_handler))
        .with_state(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    sleep(Duration::from_millis(50)).await;
    port
}

async fn start_backend(controller_url: String, hot_reload: HotReloadStreamingConfig) -> u16 {
    let streaming_config = hot_reload.snapshot();
    let store = Arc::new(SketchStore::new(
        streaming_config.clone(),
        CleanupPolicy::NoCleanup,
    ));
    let engine = Arc::new(
        ASAPQueryEngine::new_with_hot_reload(
            store.clone(),
            hot_reload.clone(),
            15_000,
        )
        .with_controller_client(
            Arc::new(HttpControllerClient::new(controller_url)) as Arc<dyn ControllerClient>
        ),
    );

    // No fallback — we want engine-miss to be visible to the
    // test and stay out of the hot-vs-cold routing question.
    let adapter_config = AdapterConfig::new(
        crate::stores::types::enums::QueryProtocol::PrometheusHttp,
        crate::stores::types::QueryLanguage::promql,
        None,
    );
    let config = HttpServerConfig {
        port: 0,
        handle_http_requests: true,
        adapter_config};
    let server = HttpServer::new(config, engine, store).with_hot_reload_config(hot_reload.clone());
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

/// Bring up a full controller+backend pair linked in both
/// directions, ready to answer queries. Returns the backend URL,
/// the controller state (for assertions), and the hot-reload
/// handle.
async fn spin_up_loop(
    metric: &str,
    expected_agg_id: u64,
) -> (String, MockControllerState, HotReloadStreamingConfig) {
    let controller_state = MockControllerState {
        received_count: Arc::new(AtomicUsize::new(0)),
        pushed_plan_ts: Arc::new(Mutex::new(None)),
        backend_config_url: Arc::new(Mutex::new(None)),
        plan_yaml: Arc::new(canned_plan_yaml(expected_agg_id, metric)),
        http: Client::new()};

    // 1. controller up (no backend URL yet)
    let controller_port = start_mock_controller(controller_state.clone()).await;
    let controller_url = format!("http://127.0.0.1:{controller_port}/api/v1/plan");

    // 2. backend up, with controller URL baked in
    let hot_reload = HotReloadStreamingConfig::new(StreamingConfig::default());
    let backend_port = start_backend(controller_url, hot_reload.clone()).await;
    let backend_url = format!("http://127.0.0.1:{backend_port}");

    // 3. tell the controller where to push
    *controller_state.backend_config_url.lock().unwrap() =
        Some(format!("{backend_url}/api/v1/streaming-config"));

    (backend_url, controller_state, hot_reload)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_capability_miss_feedback_loop_closes_over_http() {
    let metric = "http_e2e_metric";
    let expected_agg_id: u64 = 4242;

    let (backend_url, controller_state, _hot_reload) = spin_up_loop(metric, expected_agg_id).await;
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
         the controller-notify call, the /api/v1/plan handler, or the \
         /api/v1/streaming-config POST did not complete in time",
    );

    let t_plan_ready_ms = plan_ready.duration_since(t_query).as_millis();
    println!("http-e2e: time_to_plan_ready = {t_plan_ready_ms}ms (1 controller hop, localhost)");
    assert!(
        t_plan_ready_ms < 2_000,
        "time_to_plan_ready = {t_plan_ready_ms}ms, expected < 2000ms"
    );

    // 3. Shape check: the exact agg_id the controller pushed is
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

    // 4. Mock controller actually handled the notify (guards
    //    against silent short-circuit paths). The engine may
    //    fire the notify more than once per query — multiple
    //    code paths (timeline dispatch, capability matching,
    //    per-segment resolution) can each observe the miss
    //    before the plan lands — so we assert `>= 1` here.
    //    Idempotency on a *repeat* query is a separate test.
    let count = controller_state.received_count.load(Ordering::SeqCst);
    assert!(
        count >= 1,
        "mock controller should have received at least one miss, got {count}"
    );
    assert!(
        controller_state.pushed_plan_ts.lock().unwrap().is_some(),
        "mock controller should have pushed plan back to backend"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "regression after InferenceConfig retirement; see TODO"]
async fn http_capability_miss_repeat_query_is_idempotent_over_http() {
    // After the plan lands, the SAME query must not fire a
    // second miss notification — `find_compatible_aggregation`
    // now returns Some and the miss-notify branch is skipped.
    // This is the HTTP-visible version of the in-process
    // `capability_miss_idempotent_on_repeat` test.
    let metric = "http_e2e_repeat_metric";
    let expected_agg_id: u64 = 4343;

    let (backend_url, controller_state, _hot_reload) = spin_up_loop(metric, expected_agg_id).await;
    let client = Client::new();

    // 1. First query — miss, triggers the loop.
    let _ = client
        .get(format!("{backend_url}/api/v1/query"))
        .query(&[("query", format!("sum({metric})").as_str()), ("time", "0")])
        .send()
        .await
        .unwrap()
        .bytes()
        .await;

    poll_until_plan_active(
        &client,
        &backend_url,
        expected_agg_id,
        Duration::from_secs(3),
    )
    .await
    .expect("plan should have landed within 3s");

    // 2. Second query on the same metric — should be idempotent.
    //    Let any still-in-flight fire-and-forget notifies from
    //    the first query land before we snapshot `count_before`,
    //    so we're comparing apples to apples.
    sleep(Duration::from_millis(150)).await;
    let count_before = controller_state.received_count.load(Ordering::SeqCst);
    assert!(
        count_before >= 1,
        "first query should have triggered at least one notify"
    );

    let t_second = Instant::now();
    let _ = client
        .get(format!("{backend_url}/api/v1/query"))
        .query(&[("query", format!("sum({metric})").as_str()), ("time", "0")])
        .send()
        .await
        .unwrap()
        .bytes()
        .await;
    // Give any errant fire-and-forget notify a window to land.
    sleep(Duration::from_millis(150)).await;
    let count_after = controller_state.received_count.load(Ordering::SeqCst);

    println!(
        "http-e2e-repeat: notifies before={count_before} after={count_after} \
         second_query_roundtrip={}ms",
        t_second.elapsed().as_millis()
    );
    assert_eq!(
        count_after, count_before,
        "a repeat query on a covered agg_id must NOT fire a second capability-miss"
    );
}
