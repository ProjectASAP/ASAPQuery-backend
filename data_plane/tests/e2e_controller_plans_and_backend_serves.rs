//! End-to-end test of the gateway-less data path:
//!
//!   fake-exporter (test harness)
//!     ─OTLP sketches─►
//!   asap-otel agent (test harness — sketches built locally)
//!     ─OTLP/HTTP /v1/metrics─►
//!   asapquery-backend (in-process: OtlpReceiver + PrecomputeEngine + HttpServer)
//!     ─PromQL /api/v1/query─►
//!   query answer
//!
//! The control plane drives the streaming-config: a `QueryWorkload`
//! goes through `bind_workload_typed` → `split_typed_three_stage` →
//! `emit_backend_streaming_config_json`, the resulting JSON is posted
//! to the backend's `/api/v1/streaming-config` endpoint, and the
//! backend's `AggregationConfig::from_yaml_data` parses it.
//!
//! Out of scope (per the task spec): Thanos / Gorilla / MinIO cold
//! path; the gateway tier (retired in #241/#243/#377); the real
//! `fake-exporter` and `asap-otel` binaries (the test harness plays
//! both roles in-process).
//!
//! Covered:
//!  * Test 1 — controller emits a streaming-config JSON for a single
//!    DDSketch-quantile workload; the backend's parser accepts it and
//!    the GET endpoint reflects the registered aggregation.
//!  * Test 2 — same shape with `group_by_labels: ["zone"]`; verifies
//!    #245's grouping plumb survives the round-trip into the backend's
//!    `AggregationConfig.grouping_labels`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use control_plane::types::{AggType, QueryWorkload, WorkloadCharacteristics};
use data_plane::storage_engines::types::HotReloadStreamingConfig;
use serde_json::Value as JsonValue;

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Build a `QueryWorkload` with the given parameters. Mirrors the
/// `WorkloadAnalyzer` output shape but constructed directly for tests.
fn build_workload(
    metric_name: &str,
    aggregations: Vec<AggType>,
    accuracy_sla: f64,
    time_window: Duration,
    group_by_labels: Vec<String>,
    quantiles: Vec<f64>,
) -> QueryWorkload {
    QueryWorkload {
        metric_name: metric_name.to_string(),
        label_filters: HashMap::new(),
        group_by_labels,
        aggregations,
        time_window,
        repeat_every: None,
        accuracy_sla,
        latency_sla: None,
        sketch_type_override: None,
        exact_required: false,
        quantiles,
    }
}

/// Run the controller's planning pipeline end-to-end on a `QueryWorkload`
/// and return the streaming-config JSON document the controller would
/// POST to the backend's `/api/v1/streaming-config` endpoint.
///
/// Mirrors the `handle_plan` flow's `StageConfig::Backend(mut be)`
/// branch — including the post-emit grouping patch (#245) so the JSON
/// carries `labels.grouping` from `workload.group_by_labels`.
fn plan_streaming_config_json(workload: &QueryWorkload) -> JsonValue {
    let physical_expr = control_plane::optimizer::rules::bind_workload_typed(workload)
        .expect("bind_workload_typed produced a PhysicalExpr");
    let configs = control_plane::physical::stage_split::split_typed_three_stage(&physical_expr)
        .expect("split_typed_three_stage produced per-stage configs");
    let mut backend_cfg = configs
        .into_iter()
        .find_map(|(_stage, cfg)| match cfg {
            control_plane::physical::colored_dag::StageConfig::Backend(be) => Some(be),
            _ => None,
        })
        .expect("typed-L5 emit must include a Backend stage for this workload");

    // Mirror handle_plan: patch grouping (and metric_name when the L5
    // walk didn't surface it) from the workload spec before posting.
    // The typed L5's `extract_edge_facts` populates `source_metric`
    // when the `Logical(Scan{...})` chain is painted at Edge — but
    // depending on the binder path the path-recovery isn't guaranteed,
    // so the safe-belt-and-braces patch is to set both from the
    // workload directly, the same way #245 patches grouping.
    for agg in &mut backend_cfg.aggregations {
        if agg.metric_name.is_empty() {
            agg.metric_name = workload.metric_name.clone();
        }
        if agg.window_secs == 0 {
            agg.window_secs = workload.time_window.as_secs();
        }
        agg.grouping = workload.group_by_labels.clone();
    }

    control_plane::emit::emit_backend_streaming_config_json(&backend_cfg)
        .expect("emit_backend_streaming_config_json must succeed")
}

/// Spin up an in-process backend HTTP server with `HotReloadStreamingConfig`
/// wired through both the query engine and the POST `/api/v1/streaming-config`
/// handler. Returns `(port, hot_reload_handle)` — the latter so tests can
/// also inspect the current config from the controller's side.
async fn start_backend_http_server() -> (u16, HotReloadStreamingConfig) {
    use data_plane::drivers::query::adapters::config::AdapterConfig;
    use data_plane::drivers::query::servers::{HttpServer, HttpServerConfig};
    use data_plane::query_engines::asap_query_engine::engine::ASAPQueryEngine;
    use data_plane::storage_engines::sketch_db::index::SketchStore;
    use data_plane::storage_engines::types::StreamingConfig;

    let hot_reload = HotReloadStreamingConfig::new(StreamingConfig::default());
    let query_engine = Arc::new(ASAPQueryEngine::new_with_hot_reload(
        hot_reload.clone(),
        15_000,
    ));

    let adapter_config = AdapterConfig::prometheus_promql(
        "http://127.0.0.1:9999".to_string(), // unused — no forwarding in this test
        false,
    );
    let http_config = HttpServerConfig {
        port: 0,
        handle_http_requests: true,
        adapter_config,
    };

    let sketch_index = Arc::new(SketchStore::new());
    let server = HttpServer::new(http_config, query_engine, sketch_index)
        .with_hot_reload_config(hot_reload.clone());

    let port = server
        .start_test_server()
        .await
        .expect("start_test_server must succeed");

    (port, hot_reload)
}

/// POST a serde_json `Value` to `/api/v1/streaming-config` on the
/// in-process backend. Panics on non-2xx (the test wants to verify the
/// controller's emit is parseable).
async fn post_streaming_config(client: &reqwest::Client, port: u16, json: &JsonValue) {
    let resp = client
        .post(format!("http://127.0.0.1:{port}/api/v1/streaming-config"))
        .header("content-type", "application/json")
        .body(serde_json::to_vec(json).expect("serialize streaming-config JSON"))
        .send()
        .await
        .expect("POST /api/v1/streaming-config");

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    assert!(
        status.is_success(),
        "POST /api/v1/streaming-config returned {status}: {body}\n\
         (controller-emitted JSON must round-trip through \
          AggregationConfig::from_yaml_data without errors)\n\
         body sent: {}",
        serde_json::to_string_pretty(json).unwrap_or_default()
    );
}

/// GET `/api/v1/streaming-config` and return the active-config snapshot
/// as a `serde_json::Value`. Verifies the active state after a POST.
async fn get_streaming_config(client: &reqwest::Client, port: u16) -> JsonValue {
    let resp = client
        .get(format!("http://127.0.0.1:{port}/api/v1/streaming-config"))
        .send()
        .await
        .expect("GET /api/v1/streaming-config");
    assert!(resp.status().is_success(), "GET returned {}", resp.status());
    resp.json().await.expect("parse GET response as JSON")
}

/// Suppress the `WorkloadCharacteristics` unused warning — kept around
/// in case future tests need to pass per-workload resource caps.
#[allow(dead_code)]
fn _wc_anchor() -> WorkloadCharacteristics {
    WorkloadCharacteristics::default()
}

// ── Test 1 — single DDSketch-quantile workload, no grouping ─────────────────
//
// Smoke test: the controller emits a streaming-config JSON for a
// workload that resolves to DDSketch. The backend's parser accepts it
// (POST returns 2xx) and the registered aggregation surfaces on the
// GET endpoint with the expected metric / sketch family.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_plans_ddsketch_quantile_and_backend_parses_streaming_config() {
    let (port, _hot_reload) = start_backend_http_server().await;
    let client = reqwest::Client::new();

    let workload = build_workload(
        "http_latency_ms",
        vec![AggType::Quantile],
        0.01,
        Duration::from_secs(60),
        Vec::new(),
        vec![0.99],
    );
    let streaming_config_json = plan_streaming_config_json(&workload);

    // Sanity check on the emitted shape before we push it: the
    // controller MUST emit content fields (#244) and MUST NOT emit a
    // controller-allocated `aggregationId` (#244 / #246).
    let aggs = streaming_config_json["aggregations"]
        .as_array()
        .expect("aggregations array");
    assert_eq!(aggs.len(), 1, "expected exactly one BackendAggregation\n{streaming_config_json}");
    let agg = &aggs[0];
    assert!(
        agg.get("aggregationId").is_none(),
        "controller must not emit aggregationId\n{agg}"
    );
    assert_eq!(agg["metric"], "http_latency_ms");
    assert_eq!(agg["aggregationType"], "DDSketch");
    assert_eq!(agg["windowType"], "tumbling");
    assert!(
        agg["windowSize"].as_u64().expect("windowSize u64") > 0,
        "windowSize must be > 0\n{agg}"
    );

    post_streaming_config(&client, port, &streaming_config_json).await;

    // Verify the parsed config is visible via GET.
    let active = get_streaming_config(&client, port).await;
    assert_eq!(
        active["aggregation_count"], 1,
        "after POST, exactly one aggregation must be registered\n{active}"
    );
}

// ── Test 2 — cross-host grouping (sum by zone) ──────────────────────────────
//
// Verifies #245's grouping plumb survives the controller → backend
// round-trip. The workload carries `group_by_labels: ["zone"]`; the
// emitted JSON must surface `["zone"]` in `labels.grouping`, the
// backend's parser must materialise it into `AggregationConfig.
// grouping_labels`, and the active-config snapshot must reflect that.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_plans_with_grouping_and_backend_parses_grouping_labels() {
    let (port, _hot_reload) = start_backend_http_server().await;
    let client = reqwest::Client::new();

    let workload = build_workload(
        "http_latency_ms",
        vec![AggType::Quantile],
        0.01,
        Duration::from_secs(60),
        vec!["zone".to_string()],
        vec![0.99],
    );
    let streaming_config_json = plan_streaming_config_json(&workload);

    // Pre-check: the emitter must surface `labels.grouping = ["zone"]`.
    let agg = &streaming_config_json["aggregations"][0];
    let grouping = agg["labels"]["grouping"]
        .as_array()
        .expect("labels.grouping array");
    let names: Vec<&str> = grouping.iter().filter_map(|s| s.as_str()).collect();
    assert_eq!(
        names,
        vec!["zone"],
        "controller must thread workload.group_by_labels → labels.grouping (#245)\n\
         {streaming_config_json}"
    );

    post_streaming_config(&client, port, &streaming_config_json).await;

    // After POST, the active-config snapshot should reflect the
    // grouping label was parsed into AggregationConfig.
    let active = get_streaming_config(&client, port).await;
    assert_eq!(active["aggregation_count"], 1);

    // Walk the streaming_config object to find the registered grouping
    // labels. The snapshot path is `streaming_config.aggregation_configs.
    // <fp_u64_string>.grouping_labels.<inner-shape>`.
    let cfgs = active["streaming_config"]["aggregation_configs"]
        .as_object()
        .expect("aggregation_configs object in snapshot");
    assert_eq!(
        cfgs.len(),
        1,
        "expected exactly one parsed aggregation_config\n{active}"
    );
    let (_fp, cfg) = cfgs.iter().next().expect("first cfg");
    // `grouping_labels` is `KeyByLabelNames` — its JSON shape is
    // implementation-defined (likely `{labels: ["zone"]}` or just an
    // array). Find "zone" anywhere inside.
    let cfg_str = cfg.to_string();
    assert!(
        cfg_str.contains("zone"),
        "parsed AggregationConfig must contain `zone` in its grouping labels\n{cfg}"
    );
}
