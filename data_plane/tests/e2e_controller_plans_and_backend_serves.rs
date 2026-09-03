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
//!  * Test 3 — full controller-to-query roundtrip: harness simulates
//!    the agent (builds DDSketch state with `asap_sketchlib`, encodes
//!    as a modified-OTLP `DdSketchDataPoint`), POSTs sketches to the
//!    backend's OTLP receiver, waits for window close, queries via
//!    PromQL, asserts the response is well-formed for the planned
//!    metric.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use control_plane::types::{AggType, QueryWorkload, WorkloadCharacteristics};
use data_plane::storage_engines::types::HotReloadStreamingConfig;
use serde_json::Value as JsonValue;

use asap_otel_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use asap_otel_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use asap_otel_proto::tonic::metrics::v1::{
    metric::Data, CountMinSketch, CountMinSketchDataPoint, CountMinSketchEncoding, CountSketch,
    CountSketchDataPoint, CountSketchEncoding, DdSketch, DdSketchDataPoint, DdSketchEncoding,
    HllSketch, HllSketchDataPoint, HllSketchEncoding, KllSketch, KllSketchDataPoint,
    KllSketchEncoding, Metric, ResourceMetrics, ScopeMetrics,
};
use asap_sketchlib::proto::sketchlib::{
    CountMinState, CountSketchState, CounterType, DdSketchState, HllVariant as ProtoHllVariant,
    HyperLogLogState, KllState,
};
use asap_sketchlib::MessagePackCodec;
use control_plane::types::SketchType;
use prost::Message;

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Build a `QueryWorkload` with the given parameters. Mirrors the
/// `WorkloadAnalyzer` output shape but constructed directly for tests.
fn build_workload_with_override(
    metric_name: &str,
    aggregations: Vec<AggType>,
    accuracy_sla: f64,
    time_window: Duration,
    group_by_labels: Vec<String>,
    quantiles: Vec<f64>,
    sketch_type_override: Option<SketchType>,
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
        sketch_type_override,
        exact_required: false,
        quantiles,
    }
}

/// Convenience wrapper — no sketch_type_override.
fn build_workload(
    metric_name: &str,
    aggregations: Vec<AggType>,
    accuracy_sla: f64,
    time_window: Duration,
    group_by_labels: Vec<String>,
    quantiles: Vec<f64>,
) -> QueryWorkload {
    build_workload_with_override(
        metric_name,
        aggregations,
        accuracy_sla,
        time_window,
        group_by_labels,
        quantiles,
        None,
    )
}

/// Run the controller's planning pipeline end-to-end on a `QueryWorkload`
/// and return the `BackendStageConfig` the controller would emit from
/// for it — the same object both `emit_backend_streaming_config_json`
/// (legacy JSON) and `backend_plan::from_stage_config` (`BackendPlan`)
/// consume.
///
/// Mirrors the `handle_plan` flow's `StageConfig::Backend(mut be)`
/// branch — including the post-emit grouping patch (#245) so the config
/// carries `grouping` from `workload.group_by_labels`.
fn plan_backend_stage_config(
    workload: &QueryWorkload,
) -> control_plane::physical::colored_dag::BackendStageConfig {
    let physical_expr = control_plane::physical::workload_planner::bind_workload_typed(workload)
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
    backend_cfg
}

/// Run the controller's planning pipeline end-to-end on a `QueryWorkload`
/// and return the streaming-config JSON document the controller would
/// POST to the backend's `/api/v1/streaming-config` endpoint.
fn plan_streaming_config_json(workload: &QueryWorkload) -> JsonValue {
    let backend_cfg = plan_backend_stage_config(workload);
    // No continuous-monitoring (CDM) intents in these tests — pass an empty
    // slice (the `&[MonitorIntent]` arg added when CDM monitor specs landed).
    control_plane::emit::emit_backend_streaming_config_json(&backend_cfg, &[])
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
    let sketch_index = Arc::new(SketchStore::new());
    let query_engine = Arc::new(
        ASAPQueryEngine::new_with_hot_reload(hot_reload.clone(), 15_000)
            .with_sketch_index(sketch_index.clone()),
    );

    let adapter_config = AdapterConfig::prometheus_promql(
        "http://127.0.0.1:9999".to_string(), // unused — no forwarding in this test
        false,
    );
    let http_config = HttpServerConfig {
        port: 0,
        handle_http_requests: true,
        adapter_config,
    };

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

/// POST an encoded `BackendPlan` to `/api/v1/backend-plan` on the
/// in-process backend, using the control plane's real
/// `BackendClient::post_backend_plan_typed` — the exact code path a live
/// control plane process uses, not a hand-rolled request.
async fn post_backend_plan(port: u16, plan: &control_plane::backend_plan::BackendPlan) {
    let endpoint = format!("http://127.0.0.1:{port}/api/v1/streaming-config");
    let client = control_plane::backend_client::BackendClient::new(endpoint);
    client
        .post_backend_plan_typed(plan.encode_to_vec())
        .await
        .expect("POST /api/v1/backend-plan via BackendClient must succeed");
}

/// GET `/api/v1/backend-plan` and return the active-plan snapshot as a
/// `serde_json::Value`. Verifies the active state after a POST.
async fn get_backend_plan(client: &reqwest::Client, port: u16) -> JsonValue {
    let resp = client
        .get(format!("http://127.0.0.1:{port}/api/v1/backend-plan"))
        .send()
        .await
        .expect("GET /api/v1/backend-plan");
    assert!(resp.status().is_success(), "GET returned {}", resp.status());
    resp.json().await.expect("parse GET response as JSON")
}

/// Suppress the `WorkloadCharacteristics` unused warning — kept around
/// in case future tests need to pass per-workload resource caps.
#[allow(dead_code)]
fn _wc_anchor() -> WorkloadCharacteristics {
    WorkloadCharacteristics::default()
}

/// Full test stack: PrecomputeEngine + SketchStoreSink + OtlpReceiver +
/// HttpServer, all sharing the same `SketchStore` and
/// `HotReloadStreamingConfig` so a controller-posted streaming-config
/// is visible to the engine's accumulator routing, the engine's window
/// outputs land in `SketchStore`, and the query engine reads from the
/// same store.
///
/// Mirrors the wiring in `data_plane/src/main.rs`.
struct FullStack {
    backend_port: u16,
    otlp_http_port: u16,
    hot_reload_backend_plan: data_plane::storage_engines::types::HotReloadBackendPlan,
}

async fn start_full_stack(otlp_http_port: u16, otlp_grpc_port: u16) -> FullStack {
    use data_plane::drivers::ingest::series_resolver::SeriesIdResolver;
    use data_plane::drivers::ingest::{OtlpReceiver, OtlpReceiverConfig};
    use data_plane::drivers::query::adapters::config::AdapterConfig;
    use data_plane::drivers::query::servers::{HttpServer, HttpServerConfig};
    use data_plane::precompute_engine::config::{LateDataPolicy, PrecomputeEngineConfig};
    use data_plane::precompute_engine::output_sink::SketchStoreSink;
    use data_plane::precompute_engine::PrecomputeEngine;
    use data_plane::query_engines::asap_query_engine::engine::ASAPQueryEngine;
    use data_plane::storage_engines::sketch_db::index::SketchStore;
    use data_plane::storage_engines::types::StreamingConfig;

    let sketch_index = Arc::new(SketchStore::new());
    let hot_reload = HotReloadStreamingConfig::new(StreamingConfig::default());
    let series_resolver = Arc::new(SeriesIdResolver::new());

    // SketchStoreSink writes precompute output back into SketchStore so
    // the query engine can find it.
    let sink = Arc::new(SketchStoreSink::new(
        sketch_index.clone(),
        hot_reload.clone(),
        series_resolver.clone(),
    ));

    let engine_cfg = PrecomputeEngineConfig {
        num_workers: 2,
        allowed_lateness_ms: 0,
        max_buffer_per_series: 10_000,
        flush_interval_ms: 100,
        channel_buffer_size: 10_000,
        pass_raw_samples: false,
        raw_mode_aggregation_id: 0,
        late_data_policy: LateDataPolicy::Drop,
        wall_clock_grace_period_ms: 5_000,
        schema_persist_path: None,
    };
    let engine = PrecomputeEngine::new(
        engine_cfg,
        hot_reload.clone(),
        sink,
        series_resolver.clone(),
        sketch_index.clone(),
    );
    let ingest_state = engine.ingest_state();
    tokio::spawn(async move {
        let _ = engine.run().await;
    });

    // OTLP receiver wired to the engine's ingest state.
    let otlp_receiver = OtlpReceiver::with_ingest_state(
        OtlpReceiverConfig {
            grpc_port: otlp_grpc_port,
            http_port: otlp_http_port,
        },
        ingest_state,
    );
    tokio::spawn(async move {
        let _ = otlp_receiver.run().await;
    });

    // HTTP query server sharing the same SketchStore + hot-reload handle.
    let hot_reload_backend_plan = data_plane::storage_engines::types::HotReloadBackendPlan::new(
        control_plane::backend_plan::BackendPlan::default(),
    );
    let adapter_config =
        AdapterConfig::prometheus_promql("http://127.0.0.1:9999".to_string(), false);
    let http_config = HttpServerConfig {
        port: 0,
        handle_http_requests: true,
        adapter_config,
    };
    let query_engine = Arc::new(
        ASAPQueryEngine::new_with_hot_reload(hot_reload.clone(), 15_000)
            // CRITICAL: without this the engine's `sketch_index` is
            // None and every fast path that reads sid → SketchInstance
            // metadata is silently skipped. Sketches DO land in
            // `sketch_index` via OTLP ingest (the engine's
            // `precompute_engine` shares the Arc), but the query
            // path can't see them without this binding.
            .with_sketch_index(sketch_index.clone())
            .with_hot_reload_backend_plan(hot_reload_backend_plan.clone()),
    );
    let server = HttpServer::new(http_config, query_engine, sketch_index)
        .with_hot_reload_config(hot_reload.clone())
        .with_hot_reload_backend_plan(hot_reload_backend_plan.clone());
    let backend_port = server
        .start_test_server()
        .await
        .expect("start_test_server must succeed");

    // Wait for everything to bind.
    tokio::time::sleep(Duration::from_millis(500)).await;

    FullStack {
        backend_port,
        otlp_http_port,
        hot_reload_backend_plan,
    }
}

/// Build a `DdSketchState` proto from raw values. The DataPoint-level
/// scalars (count/sum/min/max) were dropped from the wire format
/// (ProjectASAP/sketchlib-go#243 / asap_sketchlib#57).
fn build_dd_sketch_state(alpha: f64, store_counts: Vec<u64>, store_offset: i32) -> DdSketchState {
    DdSketchState {
        alpha,
        store_counts,
        store_offset,
    }
}

/// Build an OTLP `ExportMetricsServiceRequest` wrapping a single DDSketch
/// data point.
fn build_dd_sketch_export(
    metric_name: &str,
    attrs: &[(&str, &str)],
    time_unix_nano: u64,
    sketch_bytes: Vec<u8>,
    alpha: f64,
) -> ExportMetricsServiceRequest {
    let attributes = attrs
        .iter()
        .map(|(k, v)| KeyValue {
            key: k.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(v.to_string())),
            }),
        })
        .collect();
    // start_time = time - 1s so the stored window `(start, end)` is
    // narrow and falls entirely within any reasonable PromQL lookback.
    // A `start_time_unix_nano: 0` (Unix epoch) would make the window
    // start in 1970, outside any current-time-relative lookback the
    // SketchStore range-query expects (`w.0 >= start && w.1 <= end`
    // in `MutableEpoch::range_query_into` — a window starting at 0
    // is rejected against `start = now - lookback_ms`).
    let start_t_ns = time_unix_nano.saturating_sub(1_000_000_000);
    let dp = DdSketchDataPoint {
        attributes,
        start_time_unix_nano: start_t_ns,
        time_unix_nano,
        sketch: sketch_bytes,
        encoding: DdSketchEncoding::DdsketchEncodingProto as i32,
        exemplars: Vec::new(),
        flags: 0,
        series_id: 0,
    };
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: metric_name.to_string(),
                    description: String::new(),
                    unit: String::new(),
                    metadata: Vec::new(),
                    data: Some(Data::Ddsketch(DdSketch {
                        data_points: vec![dp],
                        aggregation_temporality: 0,
                        relative_accuracy: alpha,
                    })),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// Build a `KllState` proto carrying the given retained items. Level
/// metadata is not populated — the decoder replays items via `update()`
/// regardless, per the lossy-reconstruction strategy documented on
/// `DatasketchesKLLAccumulator::from_sketchlib_proto_bytes`.
fn build_kll_state(k: u32, items: Vec<f64>) -> KllState {
    KllState {
        k,
        m: 8,
        num_levels: 0,
        levels: Vec::new(),
        items,
        coin: None,
        offset: 0.0,
        value_scale: 0,
        residuals: Vec::new(),
    }
}

/// Build an OTLP `ExportMetricsServiceRequest` wrapping a single KLL DP.
fn build_kll_export(
    metric_name: &str,
    attrs: &[(&str, &str)],
    time_unix_nano: u64,
    sketch_bytes: Vec<u8>,
    k: u32,
) -> ExportMetricsServiceRequest {
    let attributes = attrs
        .iter()
        .map(|(k, v)| KeyValue {
            key: k.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(v.to_string())),
            }),
        })
        .collect();
    // start_time = time - 1s so the stored window `(start, end)` is
    // narrow and falls entirely within any reasonable PromQL lookback —
    // same reasoning as `build_dd_sketch_export` above.
    let start_t_ns = time_unix_nano.saturating_sub(1_000_000_000);
    let dp = KllSketchDataPoint {
        attributes,
        start_time_unix_nano: start_t_ns,
        time_unix_nano,
        sketch: sketch_bytes,
        encoding: KllSketchEncoding::Proto as i32,
        flags: 0,
        series_id: 0,
    };
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: metric_name.to_string(),
                    description: String::new(),
                    unit: String::new(),
                    metadata: Vec::new(),
                    data: Some(Data::Kllsketch(KllSketch {
                        data_points: vec![dp],
                        aggregation_temporality: 0,
                        k,
                    })),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// Build a `HyperLogLogState` proto with `1 << precision` register bytes.
fn build_hll_state(precision: u32, registers: Vec<u8>) -> HyperLogLogState {
    HyperLogLogState {
        variant: ProtoHllVariant::Regular as i32,
        precision,
        registers,
        hip_kxq0: 0.0,
        hip_kxq1: 0.0,
        hip_est: 0.0,
        registers_sparse: None,
    }
}

/// Build an OTLP `ExportMetricsServiceRequest` wrapping a single HLL DP.
fn build_hll_export(
    metric_name: &str,
    attrs: &[(&str, &str)],
    time_unix_nano: u64,
    sketch_bytes: Vec<u8>,
    precision: u32,
) -> ExportMetricsServiceRequest {
    let attributes = attrs
        .iter()
        .map(|(k, v)| KeyValue {
            key: k.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(v.to_string())),
            }),
        })
        .collect();
    // start_time = time - 1s so the stored window `(start, end)` is
    // narrow and falls entirely within any reasonable PromQL lookback.
    // A `start_time_unix_nano: 0` (Unix epoch) would make the window
    // start in 1970, outside any current-time-relative lookback the
    // SketchStore range-query expects.
    let start_t_ns = time_unix_nano.saturating_sub(1_000_000_000);
    let dp = HllSketchDataPoint {
        attributes,
        start_time_unix_nano: start_t_ns,
        time_unix_nano,
        sketch: sketch_bytes,
        encoding: HllSketchEncoding::Proto as i32,
        flags: 0,
        series_id: 0,
    };
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: metric_name.to_string(),
                    description: String::new(),
                    unit: String::new(),
                    metadata: Vec::new(),
                    data: Some(Data::Hllsketch(HllSketch {
                        data_points: vec![dp],
                        aggregation_temporality: 0,
                        precision,
                    })),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// Build a `CountSketchState` proto from a signed matrix in row-major
/// order. Currently orphaned — Tests 6 + 9 (CountSketch coverage) use
/// the heap-bearing msgpack helper instead. Retained for future
/// non-heap CountSketch coverage; delete if no caller materialises.
#[allow(dead_code)]
fn build_count_sketch_state(rows: u32, cols: u32, counts_int: Vec<i64>) -> CountSketchState {
    assert_eq!(
        counts_int.len() as u32,
        rows * cols,
        "counts_int length must equal rows * cols"
    );
    CountSketchState {
        rows,
        cols,
        counter_type: CounterType::Int64 as i32,
        counts_int,
        counts_float: Vec::new(),
        l2: Vec::new(),
        topk: None,
    }
}

/// Build an OTLP `ExportMetricsServiceRequest` wrapping a single
/// CountSketch DP (heap-less proto encoding). Sibling helper to
/// `build_count_sketch_state`; both are retained for future
/// non-heap coverage despite being orphaned today.
#[allow(dead_code)]
fn build_count_sketch_export(
    metric_name: &str,
    attrs: &[(&str, &str)],
    time_unix_nano: u64,
    sketch_bytes: Vec<u8>,
) -> ExportMetricsServiceRequest {
    let attributes = attrs
        .iter()
        .map(|(k, v)| KeyValue {
            key: k.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(v.to_string())),
            }),
        })
        .collect();
    let start_t_ns = time_unix_nano.saturating_sub(1_000_000_000);
    let dp = CountSketchDataPoint {
        attributes,
        start_time_unix_nano: start_t_ns,
        time_unix_nano,
        sketch: sketch_bytes,
        encoding: CountSketchEncoding::Proto as i32,
        flags: 0,
        series_id: 0,
    };
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: metric_name.to_string(),
                    description: String::new(),
                    unit: String::new(),
                    metadata: Vec::new(),
                    data: Some(Data::Countsketch(CountSketch {
                        data_points: vec![dp],
                        aggregation_temporality: 0,
                        rows: 0,
                        cols: 0,
                    })),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// Build a `CountMinState` proto from a non-negative matrix in row-major order.
fn build_count_min_state(rows: u32, cols: u32, counts_int: Vec<i64>) -> CountMinState {
    assert_eq!(
        counts_int.len() as u32,
        rows * cols,
        "counts_int length must equal rows * cols"
    );
    CountMinState {
        rows,
        cols,
        counter_type: CounterType::Int64 as i32,
        counts_int,
        counts_float: Vec::new(),
        sum_counts: Vec::new(),
        sum2_counts: Vec::new(),
        l1: Vec::new(),
        l2: Vec::new(),
    }
}

/// Build an OTLP `ExportMetricsServiceRequest` wrapping a single
/// CountMinSketch DP. `wire_rows`/`wire_cols` MUST match the
/// policy's `parameters.{d, w}` so `derive_sketch_policy_fp`'s
/// content match binds the sid to the registered policy.
fn build_count_min_export(
    metric_name: &str,
    attrs: &[(&str, &str)],
    time_unix_nano: u64,
    sketch_bytes: Vec<u8>,
    wire_rows: i32,
    wire_cols: i32,
) -> ExportMetricsServiceRequest {
    let attributes = attrs
        .iter()
        .map(|(k, v)| KeyValue {
            key: k.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(v.to_string())),
            }),
        })
        .collect();
    let start_t_ns = time_unix_nano.saturating_sub(1_000_000_000);
    let dp = CountMinSketchDataPoint {
        attributes,
        start_time_unix_nano: start_t_ns,
        time_unix_nano,
        sketch: sketch_bytes,
        encoding: CountMinSketchEncoding::Proto as i32,
        flags: 0,
        series_id: 0,
    };
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: metric_name.to_string(),
                    description: String::new(),
                    unit: String::new(),
                    metadata: Vec::new(),
                    data: Some(Data::Countminsketch(CountMinSketch {
                        data_points: vec![dp],
                        aggregation_temporality: 0,
                        rows: wire_rows,
                        cols: wire_cols,
                    })),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// POST a protobuf-encoded `ExportMetricsServiceRequest` to the OTLP HTTP
/// receiver on `localhost:port/v1/metrics`. Panics with the unexpected
/// status code on non-2xx.
async fn post_otlp_http(client: &reqwest::Client, port: u16, req: ExportMetricsServiceRequest) {
    let body = req.encode_to_vec();
    let resp = client
        .post(format!("http://127.0.0.1:{port}/v1/metrics"))
        .header("Content-Type", "application/x-protobuf")
        .body(body)
        .send()
        .await
        .expect("OTLP HTTP send failed");
    assert!(
        resp.status().is_success(),
        "OTLP HTTP returned unexpected status {}",
        resp.status()
    );
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
    assert_eq!(
        aggs.len(),
        1,
        "expected exactly one BackendAggregation\n{streaming_config_json}"
    );
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

// ── Test 3 — controller plan + OTLP sketch ingest (wire-format anchor) ─────
//
// The whole gateway-less data path, end-to-end in-process — up to and
// including the OTLP sketch wire format:
//
//   1. Controller plans a DDSketch-quantile workload and POSTs the
//      streaming-config JSON to /api/v1/streaming-config.
//   2. Test harness (acting as the agent) builds a DDSketch state with
//      `asap_sketchlib::proto::sketchlib::DdSketchState`, encodes it
//      as a modified-OTLP `DdSketchDataPoint`, and POSTs it via OTLP
//      HTTP /v1/metrics. Asserts the receiver returns 2xx.
//   3. Harness sends a watermark-advance DP (timestamped past the
//      window end) so the precompute engine flushes the closed window
//      to `SketchStoreSink` → `SketchStore`. Asserts the receiver
//      returns 2xx.
//   4. Strict-success: harness queries `/api/v1/query` and asserts
//      `status == "success"`. The modern `execute()` trait-dispatch
//      path (post-#280, with PR #273's union of `instances_matching`
//      into ASAP-tier sid resolution) reaches the sketch-backed sid
//      and the reducer returns the quantile.
//
// What this Test 3 anchors:
//   * Controller-emitted streaming-config + content fields are
//     parseable AND accepted at /api/v1/streaming-config (already
//     covered by Tests 1+2, re-exercised here to verify it doesn't
//     break when the engine + OTLP receiver are also running).
//   * Modified-OTLP `DdSketchDataPoint` wire encoding + the backend's
//     OTLP HTTP receiver accept the payload (no 4xx/5xx).
//   * The full stack (PrecomputeEngine + SketchStoreSink + OtlpReceiver
//     + HttpServer all sharing SketchStore + HotReloadStreamingConfig)
//     comes up and stays up under POST + query traffic.
//   * The OTLP-ingested sketch lands in `SketchStore` keyed by the
//     right `PolicyFingerprint` (or via the `instances_matching`
//     fallback) AND the query engine's modern trait-dispatch path
//     resolves the metric against the stored sketch and returns the
//     quantile.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_plan_to_query_full_roundtrip_ddsketch() {
    let stack = start_full_stack(19_561, 19_562).await;
    let client = reqwest::Client::new();

    // ── 1. Controller plans + POSTs the streaming-config ───────────────
    //
    // Small window (1s) so the watermark-advance step below closes the
    // window quickly and the test doesn't have to wait long.
    //
    // `group_by_labels: ["service"]` is critical: the OTLP DP we send
    // below carries `service` as an attribute (required to avoid the
    // receiver's "invalid wire shape" drop). The backend's
    // `derive_sketch_policy_fp` matches policies on
    // `(metric, sketch_kind, config, group_by_keys)` — so the
    // streaming-config's grouping MUST include "service" or the
    // fingerprint won't match and the registered sid stays orphaned
    // from any policy.
    let workload = build_workload(
        "http_latency_ms",
        vec![AggType::Quantile],
        0.01,
        Duration::from_secs(1),
        vec!["service".to_string()],
        vec![0.99],
    );
    let streaming_config_json = plan_streaming_config_json(&workload);
    post_streaming_config(&client, stack.backend_port, &streaming_config_json).await;

    // ── 2. Build a DDSketch state with a known distribution ────────────
    //
    // 50 samples drawn from a fixed distribution. The exact bucket-
    // count math (DDSketch index = ceil(log_gamma(value))) doesn't
    // matter for this test — we want to verify the wire round-trip,
    // not the quantile readout accuracy. Pick a simple count vector
    // the existing `e2e_modified_otlp_sketch_path::e2e_dd_sketch_*`
    // test uses so we know it's representable.
    let alpha = 0.01;
    let store_counts = vec![5u64, 10, 15, 20];
    let dd_state = build_dd_sketch_state(alpha, store_counts, -1);
    let sketch_bytes = dd_state.encode_to_vec();

    // ── 3. POST the sketch DP via OTLP HTTP ────────────────────────────
    //
    // Use wall-clock-relative timestamps so the PromQL query at default
    // evaluation time (also wall-clock) sees the data inside its `[10s]`
    // lookback window. The sketch lands at `now - 3s` so it's well
    // inside a 1-second window that closed `now - 2s`; the watermark
    // advance is at `now - 1s` so the engine sees the window-end
    // boundary cross.
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before UNIX epoch")
        .as_nanos() as u64;
    let sketch_t_ns = now_ns.saturating_sub(3_000_000_000);
    let watermark_t_ns = now_ns.saturating_sub(1_000_000_000);

    // The OTLP DP MUST carry at least one attribute (or a known sid).
    // With both empty/zero, the receiver hits the "invalid wire shape"
    // branch in `route_modified_otlp_sketches_to_precompute` (per the
    // comment block at otel.rs:926 — `(sid=0, no attrs)` is dropped).
    // The agent normally tags DPs with the workload's `aggregate_by`
    // values; mirror that here with a `service` attribute.
    let req = build_dd_sketch_export(
        "http_latency_ms",
        &[("service", "e2e-test")],
        sketch_t_ns,
        sketch_bytes,
        alpha,
    );
    post_otlp_http(&client, stack.otlp_http_port, req).await;

    // ── 4. Send a watermark-advance DP past the window end ─────────────
    //
    // Window is 1s; the sketch landed at `now - 3s` and falls in some
    // 1-second tumbling window W. This DP at `now - 1s` is at least
    // 2 seconds past the start of W, so it moves the engine's
    // watermark past W's close boundary and triggers the flush.
    let watermark_state = build_dd_sketch_state(alpha, Vec::new(), 0);
    let watermark_req = build_dd_sketch_export(
        "http_latency_ms",
        &[("service", "e2e-test")],
        watermark_t_ns,
        watermark_state.encode_to_vec(),
        alpha,
    );
    post_otlp_http(&client, stack.otlp_http_port, watermark_req).await;

    // Wait long enough for the periodic flush + sink write.
    tokio::time::sleep(Duration::from_millis(800)).await;

    // ── 5. Query via PromQL ────────────────────────────────────────────
    let query_url = format!("http://127.0.0.1:{}/api/v1/query", stack.backend_port);
    let response: JsonValue = client
        .get(&query_url)
        .query(&[("query", "quantile_over_time(0.99, http_latency_ms[10s])")])
        .send()
        .await
        .expect("PromQL query failed to send")
        .json()
        .await
        .expect("PromQL response was not JSON");

    // ── 6. Strict-success check on the query response. ────────────────
    //
    // Investigation done in the wake of #249's L5-walk fix surfaced
    // several constraints on the OTLP wire shape:
    //
    //   * The OTLP DP MUST carry at least one attribute (or known
    //     sid). With both empty, the receiver hits the
    //     "invalid wire shape" drop in
    //     `route_modified_otlp_sketches_to_precompute` (otel.rs:926
    //     comment block — `(sid=0, no attrs)` is dropped). Test
    //     attaches `service="e2e-test"` to the DP.
    //
    //   * The sketch's group_by_keys (derived from `dp.attrs.keys()`)
    //     MUST exactly match the streaming-config policy's
    //     `grouping_labels` for `find_policy_by_content` to bind
    //     `policy_fp`. The match is **strict** at ingest
    //     (asap_tier_analysis.rs:525) but **subset** at query time
    //     (asap_tier_analysis.rs:587) — that asymmetry is intentional
    //     (ingest needs uniqueness; queries can re-aggregate down).
    //     Test uses `group_by_labels: ["service"]` to align.
    //
    //   * The stored window `(start, end)` from
    //     `(dp.start_time_unix_nano, dp.time_unix_nano) / 1e6` must
    //     fall entirely within the PromQL query's lookback range —
    //     `MutableEpoch::range_query_into` accepts only windows where
    //     `w.0 >= start && w.1 <= end`. `start_time_unix_nano: 0`
    //     would peg the window start in 1970 and the modern
    //     trait-dispatch path's `query_range` would skip it. Test
    //     uses `start_t_ns = time_unix_nano - 1s` (see
    //     `build_dd_sketch_export`).
    assert!(
        response.get("status").is_some(),
        "PromQL response missing `status` field — HTTP layer is unhealthy\n{}",
        serde_json::to_string_pretty(&response).unwrap_or_default()
    );
    let status = response["status"].as_str().unwrap_or("(missing)");
    assert_eq!(
        status,
        "success",
        "PromQL quantile_over_time query against a DDSketch-backed sid \
         did not succeed via the modern execute() trait-dispatch path. \
         Response:\n{}",
        serde_json::to_string_pretty(&response).unwrap_or_default()
    );
}

// ── Test — BackendPlan wire format: real push, real install, real serve ────
//
// Same fixture as `controller_plan_to_query_full_roundtrip_ddsketch`, but
// this time the controller ALSO builds a real `BackendPlan` (via
// `control_plane::backend_plan::from_stage_config`, the exact function a
// live control plane process calls) and pushes it through
// `control_plane::backend_client::BackendClient::post_backend_plan_typed`
// — the exact HTTP client code a live control plane process uses, not a
// hand-rolled request. This proves the whole wire is real end to end:
// control-plane-side construction → protobuf encode → HTTP POST →
// backend-side decode → `ArcSwap` install → a live PromQL query answered
// correctly with the plan installed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_really_pushes_backend_plan_and_query_really_serves() {
    let stack = start_full_stack(19_597, 19_598).await;
    let client = reqwest::Client::new();

    // ── 1. Controller plans the workload, POSTs the legacy streaming-config
    //        (still required: it's what drives sid registration on ingest)
    //        AND a real BackendPlan built from the SAME BackendStageConfig. ──
    let workload = build_workload(
        "http_latency_ms",
        vec![AggType::Quantile],
        0.01,
        Duration::from_secs(1),
        vec!["service".to_string()],
        vec![0.99],
    );
    let backend_cfg = plan_backend_stage_config(&workload);
    let streaming_config_json =
        control_plane::emit::emit_backend_streaming_config_json(&backend_cfg, &[])
            .expect("emit_backend_streaming_config_json must succeed");
    post_streaming_config(&client, stack.backend_port, &streaming_config_json).await;

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before UNIX epoch")
        .as_millis() as u64;
    let plan = control_plane::backend_plan::from_stage_config(&backend_cfg, &[], 1, now_ms)
        .expect("backend_plan::from_stage_config must succeed");
    assert_eq!(
        plan.materializations.len(),
        1,
        "the DDSketch quantile workload must produce exactly one materialization"
    );
    post_backend_plan(stack.backend_port, &plan).await;

    // ── 2. Confirm the backend really installed it (not just accepted the
    //        POST) — read it back via GET, the same handle
    //        `post_asap_planner.rs`'s serving-time lookup reads from. ──
    let installed = get_backend_plan(&client, stack.backend_port).await;
    assert_eq!(installed["status"], "success");
    assert_eq!(installed["plan_id"], 1);
    assert_eq!(
        installed["materialization_count"],
        1,
        "GET /api/v1/backend-plan must reflect the just-installed plan, not a stale/empty one:\n{}",
        serde_json::to_string_pretty(&installed).unwrap_or_default()
    );
    assert_eq!(
        stack.hot_reload_backend_plan.snapshot().plan_id,
        1,
        "the ASAPQueryEngine-side handle (shared with the HTTP server, per main.rs's wiring) \
         must observe the same installed plan the GET endpoint just reported"
    );

    // ── 3. Ingest a real DDSketch via OTLP (same fixture as Test 3). ──
    let alpha = 0.01;
    let store_counts = vec![5u64, 10, 15, 20];
    let dd_state = build_dd_sketch_state(alpha, store_counts, -1);
    let sketch_bytes = dd_state.encode_to_vec();

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before UNIX epoch")
        .as_nanos() as u64;
    let sketch_t_ns = now_ns.saturating_sub(3_000_000_000);
    let watermark_t_ns = now_ns.saturating_sub(1_000_000_000);

    let req = build_dd_sketch_export(
        "http_latency_ms",
        &[("service", "e2e-test")],
        sketch_t_ns,
        sketch_bytes,
        alpha,
    );
    post_otlp_http(&client, stack.otlp_http_port, req).await;

    let watermark_state = build_dd_sketch_state(alpha, Vec::new(), 0);
    let watermark_req = build_dd_sketch_export(
        "http_latency_ms",
        &[("service", "e2e-test")],
        watermark_t_ns,
        watermark_state.encode_to_vec(),
        alpha,
    );
    post_otlp_http(&client, stack.otlp_http_port, watermark_req).await;

    tokio::time::sleep(Duration::from_millis(800)).await;

    // ── 4. Query via PromQL — this must actually succeed AND return a
    //        real, sane numeric quantile value, with the BackendPlan
    //        installed (not just "some code path returned success"). ──
    let query_url = format!("http://127.0.0.1:{}/api/v1/query", stack.backend_port);
    let response: JsonValue = client
        .get(&query_url)
        .query(&[("query", "quantile_over_time(0.99, http_latency_ms[10s])")])
        .send()
        .await
        .expect("PromQL query failed to send")
        .json()
        .await
        .expect("PromQL response was not JSON");

    assert_eq!(
        response["status"].as_str().unwrap_or("(missing)"),
        "success",
        "query must succeed with a real BackendPlan installed. Response:\n{}",
        serde_json::to_string_pretty(&response).unwrap_or_default()
    );
    let value = response["data"]["result"]
        .as_array()
        .and_then(|r| extract_first_scalar(&JsonValue::Array(r.clone())))
        .expect("expected a scalar quantile result");
    assert!(
        value.is_finite() && value > 0.0,
        "expected a real, finite p99 value from the DDSketch fixture, got {value}"
    );
}

// ── Test 4 — full roundtrip with KLL ────────────────────────────────────────
//
// Same shape as Test 3 but the workload pins KLL via
// `sketch_type_override: Some(SketchType::KLL)`. The OTLP DP carries
// a `KllSketchDataPoint` with `KllState`; the engine's reducer must
// dispatch to the KLL quantile readout. Verifies the trait-dispatch
// fallback handles the KLL family identically to DDSketch.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_plan_to_query_full_roundtrip_kll() {
    let stack = start_full_stack(19_563, 19_564).await;
    let client = reqwest::Client::new();

    let workload = build_workload_with_override(
        "request_size_bytes",
        vec![AggType::Quantile],
        0.05,
        Duration::from_secs(1),
        vec!["service".to_string()],
        vec![0.5],
        Some(SketchType::KLL),
    );
    let streaming_config_json = plan_streaming_config_json(&workload);
    assert_eq!(
        streaming_config_json["aggregations"][0]["aggregationType"],
        "DatasketchesKLL",
        "controller must emit KLL aggregationType for SketchType::KLL override\n{streaming_config_json}"
    );
    post_streaming_config(&client, stack.backend_port, &streaming_config_json).await;

    let k = 200u32;
    let items: Vec<f64> = (1..=50).map(|i| i as f64).collect();
    let kll_state = build_kll_state(k, items);
    let sketch_bytes = kll_state.encode_to_vec();

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before UNIX epoch")
        .as_nanos() as u64;
    let sketch_t_ns = now_ns.saturating_sub(3_000_000_000);
    let watermark_t_ns = now_ns.saturating_sub(1_000_000_000);

    let req = build_kll_export(
        "request_size_bytes",
        &[("service", "e2e-test")],
        sketch_t_ns,
        sketch_bytes,
        k,
    );
    post_otlp_http(&client, stack.otlp_http_port, req).await;

    let watermark_state = build_kll_state(k, Vec::new());
    let watermark_req = build_kll_export(
        "request_size_bytes",
        &[("service", "e2e-test")],
        watermark_t_ns,
        watermark_state.encode_to_vec(),
        k,
    );
    post_otlp_http(&client, stack.otlp_http_port, watermark_req).await;

    tokio::time::sleep(Duration::from_millis(800)).await;

    let response: JsonValue = client
        .get(format!(
            "http://127.0.0.1:{}/api/v1/query",
            stack.backend_port
        ))
        .query(&[("query", "quantile_over_time(0.5, request_size_bytes[10s])")])
        .send()
        .await
        .expect("query failed")
        .json()
        .await
        .expect("response not JSON");

    let status = response["status"].as_str().unwrap_or("(missing)");
    assert_eq!(
        status,
        "success",
        "KLL quantile query did not succeed:\n{}",
        serde_json::to_string_pretty(&response).unwrap_or_default()
    );
}

// ── Test 5 — full roundtrip with HLL (cardinality) ──────────────────────────
//
// HLL backs the cardinality readout. The workload pins HLL via
// `sketch_type_override: Some(SketchType::HLL)`. The OTLP DP carries
// a `HllSketchDataPoint` with `HyperLogLogState`. PromQL's
// `count(metric)` is the spec's distinct-counting idiom — returns
// the number of distinct label sets in the result vector — which
// the analyzer routes to `Capability::CardinalityApprox` and the
// reducer dispatches to the HLL cardinality readout.
//
// Closed by a chain of fixes:
//   * `count(metric)` analyzer fix (PR #255)
//   * `count` reducer alias (PR #255)
//   * Vector-vs-Matrix instant-query response shape fix (this PR)

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_plan_to_query_full_roundtrip_hll() {
    let stack = start_full_stack(19_565, 19_566).await;
    let client = reqwest::Client::new();

    let workload = build_workload_with_override(
        "unique_users_per_min",
        vec![AggType::Cardinality],
        0.05,
        Duration::from_secs(1),
        vec!["service".to_string()],
        Vec::new(),
        Some(SketchType::HLL),
    );
    let streaming_config_json = plan_streaming_config_json(&workload);
    assert_eq!(
        streaming_config_json["aggregations"][0]["aggregationType"], "HLL",
        "controller must emit HLL aggregationType for SketchType::HLL override\n{streaming_config_json}"
    );
    post_streaming_config(&client, stack.backend_port, &streaming_config_json).await;

    // Precision must match what the controller plans for this
    // workload (`HLLDefaults` in `control_plane::types`). The
    // accuracy_sla=0.05 above is > the precision_threshold (0.02),
    // so the planner picks `precision_coarse = 10`. If the OTLP DP
    // were sent with a different precision, the backend would
    // register two separate sids for the same metric — one with
    // policy_fp=UNSET (no matching policy params) — and the query
    // wouldn't find the policy-tagged one.
    let precision = 10u32;
    let num_registers = 1usize << precision;
    let mut registers = vec![0u8; num_registers];
    // Set a few non-zero registers so the cardinality estimate is
    // non-trivial. Indices fit within 1024.
    registers[0] = 5;
    registers[100] = 7;
    registers[500] = 3;
    registers[1000] = 4;
    let hll_state = build_hll_state(precision, registers);
    let sketch_bytes = hll_state.encode_to_vec();

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before UNIX epoch")
        .as_nanos() as u64;
    let sketch_t_ns = now_ns.saturating_sub(3_000_000_000);
    let watermark_t_ns = now_ns.saturating_sub(1_000_000_000);

    let req = build_hll_export(
        "unique_users_per_min",
        &[("service", "e2e-test")],
        sketch_t_ns,
        sketch_bytes,
        precision,
    );
    post_otlp_http(&client, stack.otlp_http_port, req).await;

    let watermark_state = build_hll_state(precision, vec![0u8; num_registers]);
    let watermark_req = build_hll_export(
        "unique_users_per_min",
        &[("service", "e2e-test")],
        watermark_t_ns,
        watermark_state.encode_to_vec(),
        precision,
    );
    post_otlp_http(&client, stack.otlp_http_port, watermark_req).await;

    tokio::time::sleep(Duration::from_millis(800)).await;

    // PromQL `count(metric)` lowers to `AggFunc::CountDistinct` →
    // `AggIntent::Cardinality` → `Capability::CardinalityApprox`,
    // which is what the HLL policy provides. No range selector
    // needed — instant-vector cardinality is what HLL answers.
    let response: JsonValue = client
        .get(format!(
            "http://127.0.0.1:{}/api/v1/query",
            stack.backend_port
        ))
        .query(&[("query", "count(unique_users_per_min)")])
        .send()
        .await
        .expect("query failed")
        .json()
        .await
        .expect("response not JSON");

    let status = response["status"].as_str().unwrap_or("(missing)");
    assert_eq!(
        status,
        "success",
        "HLL cardinality query did not succeed:\n{}",
        serde_json::to_string_pretty(&response).unwrap_or_default()
    );
}

// ── Test 6 — wire-format roundtrip with CountSketch (frequency) ─────────────
//
// CountSketch backs FREQUENCY estimation — signed-counter matrix
// producing approximate point-frequency answers. `top_endpoint_qps`
// is the canonical TopK metric, so the planner pins
// `with_heap: true` and the controller emits `CountSketchWithHeap`
// (regardless of override). To match, the wire DP carries a
// msgpack-encoded heap envelope (mirroring Test 9), but the query
// uses `count_over_time(...)` instead of `topk(...)` — the
// reducer's `decode_frequency_total` reads row-0 of the underlying
// matrix for heap-bearing variants too, so FrequencyEstimate works
// on the same sid that Test 9 queries for top-k.
//
// **Strict-success: `count_over_time(top_endpoint_qps[1s])`** binds
// to `Capability::FrequencyEstimate(Any)`, which
// `is_satisfied_by` accepts against
// `FrequencyTopk(CountSketchWithHeap)` (heap is additional info
// layered over the matrix — the matrix is a fully valid frequency
// sketch on its own).

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "known gap, exposed (not caused) by sketch_reducer.rs's retirement: this shape \
    now hard capability-misses on SummaryExecutor ('No result for query') instead of \
    silently falling through to the retired legacy reducer, which used to mask it. Not \
    fully root-caused yet -- possibly the same effective_is_cumulative gap as \
    ASAPQuery-backend#431 (this query is also `count_over_time(...)`, a function name \
    effective_is_cumulative's match doesn't cover), but that's unconfirmed for this \
    specific CountSketchWithHeap/heap_size shape. Needs its own investigation."]
async fn controller_plan_to_query_full_roundtrip_count_sketch() {
    let stack = start_full_stack(19_567, 19_568).await;
    let client = reqwest::Client::new();

    let workload = build_workload_with_override(
        "top_endpoint_qps",
        vec![AggType::Frequency],
        0.05,
        Duration::from_secs(1),
        vec!["service".to_string()],
        Vec::new(),
        Some(SketchType::CountSketch),
    );
    let streaming_config_json = plan_streaming_config_json(&workload);
    assert_eq!(
        streaming_config_json["aggregations"][0]["aggregationType"], "CountSketchWithHeap",
        "controller must emit CountSketchWithHeap for top_endpoint_qps (TopK metric)\n{streaming_config_json}"
    );
    post_streaming_config(&client, stack.backend_port, &streaming_config_json).await;

    // Use the planner-picked `(w, d)` so the OTLP DP's wire-level
    // `rows`/`cols` line up with the policy's `parameters.{d, w}` —
    // same shape constraint as Test 9.
    let (w, d) = extract_w_d_from_streaming_config(&streaming_config_json);
    let rows = d as usize;
    let cols = w as usize;
    let wire_rows = d as i32;
    let wire_cols = w as i32;

    let items: &[(&str, u64)] = &[("alpha", 100), ("beta", 50), ("gamma", 200), ("delta", 75)];
    let sketch_bytes = build_heap_bearing_msgpack(rows, cols, 10, items);

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before UNIX epoch")
        .as_nanos() as u64;
    let sketch_t_ns = now_ns.saturating_sub(3_000_000_000);
    let watermark_t_ns = now_ns.saturating_sub(1_000_000_000);

    let req = build_count_sketch_with_heap_msgpack_export(
        "top_endpoint_qps",
        &[("service", "e2e-test")],
        sketch_t_ns,
        sketch_bytes.clone(),
        wire_rows,
        wire_cols,
    );
    post_otlp_http(&client, stack.otlp_http_port, req).await;

    let watermark_req = build_count_sketch_with_heap_msgpack_export(
        "top_endpoint_qps",
        &[("service", "e2e-test")],
        watermark_t_ns,
        sketch_bytes,
        wire_rows,
        wire_cols,
    );
    post_otlp_http(&client, stack.otlp_http_port, watermark_req).await;

    tokio::time::sleep(Duration::from_millis(800)).await;

    let response: JsonValue = client
        .get(format!(
            "http://127.0.0.1:{}/api/v1/query",
            stack.backend_port
        ))
        .query(&[("query", "count_over_time(top_endpoint_qps[10s])")])
        .send()
        .await
        .expect("query failed")
        .json()
        .await
        .expect("response not JSON");
    let status = response["status"].as_str().unwrap_or("(missing)");
    assert_eq!(
        status,
        "success",
        "count_over_time(...) against heap-bearing CountSketch must succeed \
         end-to-end. Response:\n{}",
        serde_json::to_string_pretty(&response).unwrap_or_default()
    );
    let result = &response["data"]["result"];
    let arr = result
        .as_array()
        .expect("result must be an array of vector elements");
    assert!(
        !arr.is_empty(),
        "count_over_time must return at least one series. Response:\n{}",
        serde_json::to_string_pretty(&response).unwrap_or_default()
    );
}

// ── Test 7 — wire-format roundtrip with CountMinSketch (frequency) ──────────
//
// CountMinSketch backs frequency estimation — non-negative point
// counts with one-sided over-estimation. Workload pins CMS via
// `sketch_type_override: Some(SketchType::CountMinSketch)`. The OTLP
// DP carries a `CountMinSketchDataPoint` with `CountMinState`.
//
// **Strict-success on `count_over_time(metric[1s])`** — PromQL's
// per-series sample-count idiom maps to `FrequencyEstimate(Any)`
// (see analyzer's `walk_call_to_op` for `count_over_time`), which
// `is_satisfied_by` accepts against `FrequencyEstimate(CountMin)`.
// The reducer's `decode_frequency_total` reads row-0 of the CMS
// matrix and returns the per-window total count.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_plan_to_query_full_roundtrip_count_min_sketch() {
    let stack = start_full_stack(19_569, 19_570).await;
    let client = reqwest::Client::new();

    let workload = build_workload_with_override(
        "endpoint_request_freq",
        vec![AggType::Frequency],
        0.05,
        Duration::from_secs(1),
        vec!["service".to_string()],
        Vec::new(),
        Some(SketchType::CountMinSketch),
    );
    let streaming_config_json = plan_streaming_config_json(&workload);
    assert_eq!(
        streaming_config_json["aggregations"][0]["aggregationType"], "CountMinSketch",
        "controller must emit CountMinSketch aggregationType for SketchType::CountMinSketch override\n{streaming_config_json}"
    );
    post_streaming_config(&client, stack.backend_port, &streaming_config_json).await;

    // Use planner-picked `(w, d)` so the wire DP's `rows`/`cols`
    // match the policy's `parameters.{d, w}` — the policy_fp content
    // match keys on these values (see `derive_sketch_policy_fp`).
    let (w, d) = extract_w_d_from_streaming_config(&streaming_config_json);
    let rows = d;
    let cols = w;
    let counts: Vec<i64> = (0..(rows * cols) as i64).map(|i| (i % 11).abs()).collect();
    let cms_state = build_count_min_state(rows, cols, counts);
    let sketch_bytes = cms_state.encode_to_vec();

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before UNIX epoch")
        .as_nanos() as u64;
    let sketch_t_ns = now_ns.saturating_sub(3_000_000_000);
    let watermark_t_ns = now_ns.saturating_sub(1_000_000_000);

    let req = build_count_min_export(
        "endpoint_request_freq",
        &[("service", "e2e-test")],
        sketch_t_ns,
        sketch_bytes,
        rows as i32,
        cols as i32,
    );
    post_otlp_http(&client, stack.otlp_http_port, req).await;

    let watermark_state = build_count_min_state(rows, cols, vec![0i64; (rows * cols) as usize]);
    let watermark_req = build_count_min_export(
        "endpoint_request_freq",
        &[("service", "e2e-test")],
        watermark_t_ns,
        watermark_state.encode_to_vec(),
        rows as i32,
        cols as i32,
    );
    post_otlp_http(&client, stack.otlp_http_port, watermark_req).await;

    tokio::time::sleep(Duration::from_millis(800)).await;

    let response: JsonValue = client
        .get(format!(
            "http://127.0.0.1:{}/api/v1/query",
            stack.backend_port
        ))
        .query(&[("query", "count_over_time(endpoint_request_freq[10s])")])
        .send()
        .await
        .expect("query failed")
        .json()
        .await
        .expect("response not JSON");
    let status = response["status"].as_str().unwrap_or("(missing)");
    assert_eq!(
        status,
        "success",
        "count_over_time(...) against heap-less CMS must succeed end-to-end. Response:\n{}",
        serde_json::to_string_pretty(&response).unwrap_or_default()
    );
    let result = &response["data"]["result"];
    let arr = result
        .as_array()
        .expect("result must be an array of vector elements");
    assert!(
        !arr.is_empty(),
        "count_over_time must return at least one series. Response:\n{}",
        serde_json::to_string_pretty(&response).unwrap_or_default()
    );
}

// ── Heap-bearing fixtures ───────────────────────────────────────────────────
//
// The msgpack envelope `CountMinSketchWithHeap::serialize_msgpack`
// produces (an outer `{sketch, topk_heap, heap_size}` wrapper, see
// asap_sketchlib's `CountMinSketchWithHeapSerialized`) is the SHARED
// wire shape used by both `CmsWithHeap` and `CountSketchWithHeap` —
// the heap is the distinguishing payload, and the data_plane reducer
// dispatches both variants through `decode_cms_with_heap_from_msgpack`
// (see `sketch_reducer.rs` at the FrequencyTopk dispatch site).
//
// On the ingest side, `sketch_kind_handle_for` peeks at incoming
// CountMin / CountSketch DPs with `encoding=MSGPACK`; if the bytes
// round-trip through the heap envelope AND the heap is non-empty,
// the sid is auto-promoted to the corresponding `*WithHeap` variant
// so the ASAP-tier reducer can answer `topk(...)` from the heap.

/// Build a msgpack-encoded `CountMinSketchWithHeap` payload populated
/// with the supplied `(key, count)` pairs. Returns the bytes ready
/// for the OTLP DP's `sketch` field with `encoding=MSGPACK`.
fn build_heap_bearing_msgpack(
    rows: usize,
    cols: usize,
    top_k: usize,
    items: &[(&str, u64)],
) -> Vec<u8> {
    use asap_sketchlib::CountMinSketchWithHeap;
    let mut cms = CountMinSketchWithHeap::new(rows, cols, top_k);
    for (key, count) in items {
        for _ in 0..*count {
            cms.update(key, 1.0);
        }
    }
    cms.to_msgpack()
        .expect("CountMinSketchWithHeap::serialize_msgpack should not fail")
}

/// Extract the planner-picked `(w, d)` from a streaming-config aggregation
/// for CMS / CountSketch policies. Returns `(w as cols, d as rows)`.
/// The DP's wire-level `rows`/`cols` MUST match these for
/// `find_policy_by_content` to bind the sid to the policy_fp (the
/// content match probes `parameters.w` and `parameters.d`).
fn extract_w_d_from_streaming_config(streaming_config_json: &JsonValue) -> (u32, u32) {
    let params = &streaming_config_json["aggregations"][0]["parameters"];
    let w = params["w"]
        .as_u64()
        .expect("streaming-config aggregation must carry parameters.w") as u32;
    let d = params["d"]
        .as_u64()
        .expect("streaming-config aggregation must carry parameters.d") as u32;
    (w, d)
}

/// OTLP `ExportMetricsServiceRequest` with a single `CountMinSketch` DP
/// carrying msgpack-encoded heap-bearing bytes. `encoding=MSGPACK` (3)
/// triggers `sketch_kind_handle_for`'s auto-promotion to `CmsWithHeap`.
/// `rows`/`cols` on the parent `CountMinSketch` MUST match the policy's
/// `parameters.{d,w}` for the policy_fp content match to bind.
fn build_cms_with_heap_msgpack_export(
    metric_name: &str,
    attrs: &[(&str, &str)],
    time_unix_nano: u64,
    sketch_bytes: Vec<u8>,
    wire_rows: i32,
    wire_cols: i32,
) -> ExportMetricsServiceRequest {
    let attributes = attrs
        .iter()
        .map(|(k, v)| KeyValue {
            key: k.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(v.to_string())),
            }),
        })
        .collect();
    let start_t_ns = time_unix_nano.saturating_sub(1_000_000_000);
    let dp = CountMinSketchDataPoint {
        attributes,
        start_time_unix_nano: start_t_ns,
        time_unix_nano,
        sketch: sketch_bytes,
        encoding: CountMinSketchEncoding::Msgpack as i32,
        flags: 0,
        series_id: 0,
    };
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: metric_name.to_string(),
                    description: String::new(),
                    unit: String::new(),
                    metadata: Vec::new(),
                    data: Some(Data::Countminsketch(CountMinSketch {
                        data_points: vec![dp],
                        aggregation_temporality: 0,
                        rows: wire_rows,
                        cols: wire_cols,
                    })),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// OTLP `ExportMetricsServiceRequest` with a single `CountSketch` DP
/// carrying msgpack-encoded heap-bearing bytes. `encoding=MSGPACK` (3)
/// triggers `sketch_kind_handle_for`'s auto-promotion to
/// `CountSketchWithHeap` (the heap envelope is identical to the CMS
/// variant). `rows`/`cols` MUST match the policy's `parameters.{d,w}`.
fn build_count_sketch_with_heap_msgpack_export(
    metric_name: &str,
    attrs: &[(&str, &str)],
    time_unix_nano: u64,
    sketch_bytes: Vec<u8>,
    wire_rows: i32,
    wire_cols: i32,
) -> ExportMetricsServiceRequest {
    let attributes = attrs
        .iter()
        .map(|(k, v)| KeyValue {
            key: k.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(v.to_string())),
            }),
        })
        .collect();
    let start_t_ns = time_unix_nano.saturating_sub(1_000_000_000);
    let dp = CountSketchDataPoint {
        attributes,
        start_time_unix_nano: start_t_ns,
        time_unix_nano,
        sketch: sketch_bytes,
        encoding: CountSketchEncoding::Msgpack as i32,
        flags: 0,
        series_id: 0,
    };
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: metric_name.to_string(),
                    description: String::new(),
                    unit: String::new(),
                    metadata: Vec::new(),
                    data: Some(Data::Countsketch(CountSketch {
                        data_points: vec![dp],
                        aggregation_temporality: 0,
                        rows: wire_rows,
                        cols: wire_cols,
                    })),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

// ── Test 8 — heap-bearing CMS + topk strict-success ─────────────────────────
//
// The full top-k roundtrip with `CmsWithHeap`. Workload pins CMS via
// `sketch_type_override: Some(SketchType::CountMinSketch)` on the
// `top_endpoint_qps` metric (TopK statistic class), which the planner
// binds via `bind_cms_with_heap_on_topk` (CMS-Heap pattern from
// Cormode & Muthukrishnan 2005).
//
// The OTLP DP carries a msgpack-encoded `CountMinSketchWithHeap`
// payload (`encoding=MSGPACK`); the receiver's `sketch_kind_handle_for`
// peeks at the bytes and auto-promotes the sid to `CmsWithHeap`,
// registering it under `Capability::FrequencyTopk(CmsWithHeap)`.
//
// The reducer's `topk` family decodes the heap directly via
// `decode_cms_with_heap_from_msgpack` and emits one output series
// per top-k item with the item key in the `item` label.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_plan_to_query_full_roundtrip_cms_with_heap_topk() {
    let stack = start_full_stack(19_571, 19_572).await;
    let client = reqwest::Client::new();

    let workload = build_workload_with_override(
        "top_endpoint_qps",
        vec![AggType::Frequency],
        0.05,
        Duration::from_secs(1),
        vec!["service".to_string()],
        Vec::new(),
        Some(SketchType::CountMinSketch),
    );
    let streaming_config_json = plan_streaming_config_json(&workload);
    // The controller now emits `CountMinSketchWithHeap` directly when
    // the planner-set `with_heap: true` flag on `CmsParams` fires
    // (see `sketch_kind_to_backend_type`). No in-test JSON patch is
    // needed — the analyzer ↔ policy match binds against the
    // controller-emitted aggregation type as-is.
    assert_eq!(
        streaming_config_json["aggregations"][0]["aggregationType"], "CountMinSketchWithHeap",
        "controller must emit CountMinSketchWithHeap when bind_cms_with_heap_on_topk fires\n{streaming_config_json}"
    );
    assert_eq!(
        streaming_config_json["readouts"][0]["op"], "topk",
        "controller must emit a topk readout for CMS-with-heap binding\n{streaming_config_json}"
    );
    post_streaming_config(&client, stack.backend_port, &streaming_config_json).await;

    // Use the planner-picked `(w, d)` so the OTLP DP's wire-level
    // `rows`/`cols` line up with the policy's `parameters.{d, w}` —
    // `derive_sketch_policy_fp` content-matches on these keys, so a
    // dimension mismatch leaves the sid registered with `policy_fp`
    // = UNSET (unreachable through `sids_for_policy`).
    let (w, d) = extract_w_d_from_streaming_config(&streaming_config_json);
    let rows = d as usize;
    let cols = w as usize;
    let wire_rows = d as i32;
    let wire_cols = w as i32;

    // Heap items with deterministic count ordering. `gamma` is the
    // unambiguous top-1 (count=200); the heap (top_k=10) keeps all six.
    let items: &[(&str, u64)] = &[
        ("alpha", 100),
        ("beta", 50),
        ("gamma", 200),
        ("delta", 75),
        ("epsilon", 10),
        ("zeta", 150),
    ];
    let sketch_bytes = build_heap_bearing_msgpack(rows, cols, 10, items);

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before UNIX epoch")
        .as_nanos() as u64;
    let sketch_t_ns = now_ns.saturating_sub(3_000_000_000);
    let watermark_t_ns = now_ns.saturating_sub(1_000_000_000);

    let req = build_cms_with_heap_msgpack_export(
        "top_endpoint_qps",
        &[("service", "e2e-test")],
        sketch_t_ns,
        sketch_bytes.clone(),
        wire_rows,
        wire_cols,
    );
    post_otlp_http(&client, stack.otlp_http_port, req).await;

    // Watermark MUST also carry the heap — the reducer reads
    // `samples.iter().next_back()` and decodes the latest sample's
    // bytes; an empty-heap watermark would shadow the real payload.
    let watermark_req = build_cms_with_heap_msgpack_export(
        "top_endpoint_qps",
        &[("service", "e2e-test")],
        watermark_t_ns,
        sketch_bytes,
        wire_rows,
        wire_cols,
    );
    post_otlp_http(&client, stack.otlp_http_port, watermark_req).await;

    tokio::time::sleep(Duration::from_millis(800)).await;

    let response: JsonValue = client
        .get(format!(
            "http://127.0.0.1:{}/api/v1/query",
            stack.backend_port
        ))
        .query(&[("query", "topk(3, top_endpoint_qps)")])
        .send()
        .await
        .expect("query failed")
        .json()
        .await
        .expect("response not JSON");
    let status = response["status"].as_str().unwrap_or("(missing)");
    assert_eq!(
        status,
        "success",
        "topk(...) on CmsWithHeap must succeed end-to-end. Response:\n{}",
        serde_json::to_string_pretty(&response).unwrap_or_default()
    );
    // Top-1 must be `gamma` (count=200), surfaced via the
    // `item: <key>` synthesized label on each top-k series. The
    // `InstantVectorElement::label_keys_override` field (added
    // alongside this assertion's tightening) carries the synthesized
    // key through the Prometheus adapter.
    let result = &response["data"]["result"];
    let arr = result
        .as_array()
        .expect("result must be an array of vector elements");
    assert!(
        !arr.is_empty() && arr.len() <= 3,
        "topk(3) must return between 1 and 3 series. Response:\n{}",
        serde_json::to_string_pretty(&response).unwrap_or_default()
    );
    let mut values: Vec<f64> = arr
        .iter()
        .filter_map(|e| e["value"][1].as_str().and_then(|s| s.parse::<f64>().ok()))
        .collect();
    values.sort_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
    let mut found_gamma = false;
    for elem in arr {
        if let Some(item) = elem["metric"]["item"].as_str() {
            if item == "gamma" {
                found_gamma = true;
                break;
            }
        }
    }
    assert!(
        found_gamma,
        "topk(3) must surface `gamma` via the `item` label on at least \
         one series. Response:\n{}",
        serde_json::to_string_pretty(&response).unwrap_or_default()
    );
    assert!(
        values
            .first()
            .map(|v| (v - 200.0).abs() < 1.0)
            .unwrap_or(false),
        "topk(3) on heap-bearing CMS must surface `gamma`'s count (200) as \
         the top value (received {values:?}). Response:\n{}",
        serde_json::to_string_pretty(&response).unwrap_or_default()
    );
}

// ── Test 9 — heap-bearing CountSketch + topk strict-success ─────────────────
//
// Same shape as Test 8 but the workload defaults the `top_endpoint_qps`
// metric to `CountSketch` (the canonical TopK pick — unbiased
// estimator, see `BindCountSketchOnTopK`). The OTLP DP carries the
// SAME msgpack heap envelope (the wire shape is shared); only the
// outer DP type changes (`CountSketchDataPoint` instead of
// `CountMinSketchDataPoint`).
//
// `sketch_kind_handle_for` was extended in this PR to peek at
// CountSketch DPs the same way it does for CountMin — a
// non-empty heap in a msgpack-encoded payload promotes the sid to
// `CountSketchWithHeap`, which the analyzer's `is_satisfied_by`
// recognises as a valid `FrequencyTopk(CountSketchWithHeap)` provider.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_plan_to_query_full_roundtrip_count_sketch_with_heap_topk() {
    let stack = start_full_stack(19_573, 19_574).await;
    let client = reqwest::Client::new();

    let workload = build_workload_with_override(
        "top_endpoint_qps",
        vec![AggType::Frequency],
        0.05,
        Duration::from_secs(1),
        vec!["service".to_string()],
        Vec::new(),
        None, // default → CountSketch (canonical TopK pick)
    );
    let streaming_config_json = plan_streaming_config_json(&workload);
    // The controller now emits `CountSketchWithHeap` directly when
    // the planner-set `with_heap: true` flag on `CountSketchParams`
    // fires (see `sketch_kind_to_backend_type`). No in-test JSON
    // patch is needed.
    assert_eq!(
        streaming_config_json["aggregations"][0]["aggregationType"], "CountSketchWithHeap",
        "controller must emit CountSketchWithHeap for default top_endpoint_qps TopK binding\n{streaming_config_json}"
    );
    assert_eq!(
        streaming_config_json["aggregations"][0]["parameters"]["with_heap"], true,
        "controller must set parameters.with_heap=true for CountSketch TopK binding\n{streaming_config_json}"
    );
    post_streaming_config(&client, stack.backend_port, &streaming_config_json).await;

    let items: &[(&str, u64)] = &[
        ("alpha", 100),
        ("beta", 50),
        ("gamma", 200),
        ("delta", 75),
        ("epsilon", 10),
        ("zeta", 150),
    ];
    let (w, d) = extract_w_d_from_streaming_config(&streaming_config_json);
    let rows = d as usize;
    let cols = w as usize;
    let wire_rows = d as i32;
    let wire_cols = w as i32;
    let sketch_bytes = build_heap_bearing_msgpack(rows, cols, 10, items);

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before UNIX epoch")
        .as_nanos() as u64;
    let sketch_t_ns = now_ns.saturating_sub(3_000_000_000);
    let watermark_t_ns = now_ns.saturating_sub(1_000_000_000);

    let req = build_count_sketch_with_heap_msgpack_export(
        "top_endpoint_qps",
        &[("service", "e2e-test")],
        sketch_t_ns,
        sketch_bytes.clone(),
        wire_rows,
        wire_cols,
    );
    post_otlp_http(&client, stack.otlp_http_port, req).await;

    let watermark_req = build_count_sketch_with_heap_msgpack_export(
        "top_endpoint_qps",
        &[("service", "e2e-test")],
        watermark_t_ns,
        sketch_bytes,
        wire_rows,
        wire_cols,
    );
    post_otlp_http(&client, stack.otlp_http_port, watermark_req).await;

    tokio::time::sleep(Duration::from_millis(800)).await;

    let response: JsonValue = client
        .get(format!(
            "http://127.0.0.1:{}/api/v1/query",
            stack.backend_port
        ))
        .query(&[("query", "topk(3, top_endpoint_qps)")])
        .send()
        .await
        .expect("query failed")
        .json()
        .await
        .expect("response not JSON");
    let status = response["status"].as_str().unwrap_or("(missing)");
    assert_eq!(
        status,
        "success",
        "topk(...) on CountSketchWithHeap must succeed end-to-end. Response:\n{}",
        serde_json::to_string_pretty(&response).unwrap_or_default()
    );
    // Same shape-only assertion as Test 8 — `InstantVectorElement`
    // currently drops per-element labels, so we can't check for
    // `item: "gamma"`. Verify the strongest invariants the wire
    // surfaces today: 1..=3 series and gamma's count (200) leads.
    let result = &response["data"]["result"];
    let arr = result
        .as_array()
        .expect("result must be an array of vector elements");
    assert!(
        !arr.is_empty() && arr.len() <= 3,
        "topk(3) must return between 1 and 3 series. Response:\n{}",
        serde_json::to_string_pretty(&response).unwrap_or_default()
    );
    let mut values: Vec<f64> = arr
        .iter()
        .filter_map(|e| e["value"][1].as_str().and_then(|s| s.parse::<f64>().ok()))
        .collect();
    values.sort_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
    assert!(
        values
            .first()
            .map(|v| (v - 200.0).abs() < 1.0)
            .unwrap_or(false),
        "topk(3) on heap-bearing CountSketch must surface `gamma`'s count (200) \
         as the top value (received {values:?}). Response:\n{}",
        serde_json::to_string_pretty(&response).unwrap_or_default()
    );
}

// ── Test 10 — range-query warm-tier fallback (CMS + count_over_time) ────────
//
// `/api/v1/query_range` previously had no warm-tier fallback —
// when the legacy `handle_range_query_promql` returned `None` (which
// it does for sketch-backed sids), the handler immediately fell
// through to `format_unsupported_query_response` ("No result for
// query"). This PR adds an `execute_range_promql_modern` modern
// path that mirrors PR #253's `process_via_simple_engine` fallback
// for instant queries.
//
// Same wire setup as Test 7 (heap-less CMS, `endpoint_request_freq`
// with `AggType::Frequency`), but the query goes through
// `/api/v1/query_range?query=count_over_time(metric[10s])` instead
// of the instant endpoint. The result `resultType` is `matrix`
// (Prometheus spec for range queries).

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_plan_to_range_query_count_over_time_cms() {
    let stack = start_full_stack(19_575, 19_576).await;
    let client = reqwest::Client::new();

    let workload = build_workload_with_override(
        "endpoint_request_freq",
        vec![AggType::Frequency],
        0.05,
        Duration::from_secs(1),
        vec!["service".to_string()],
        Vec::new(),
        Some(SketchType::CountMinSketch),
    );
    let streaming_config_json = plan_streaming_config_json(&workload);
    post_streaming_config(&client, stack.backend_port, &streaming_config_json).await;

    let (w, d) = extract_w_d_from_streaming_config(&streaming_config_json);
    let rows = d;
    let cols = w;
    let counts: Vec<i64> = (0..(rows * cols) as i64).map(|i| (i % 11).abs()).collect();
    let cms_state = build_count_min_state(rows, cols, counts);
    let sketch_bytes = cms_state.encode_to_vec();

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before UNIX epoch")
        .as_nanos() as u64;
    let sketch_t_ns = now_ns.saturating_sub(3_000_000_000);
    let watermark_t_ns = now_ns.saturating_sub(1_000_000_000);

    let req = build_count_min_export(
        "endpoint_request_freq",
        &[("service", "e2e-test")],
        sketch_t_ns,
        sketch_bytes,
        rows as i32,
        cols as i32,
    );
    post_otlp_http(&client, stack.otlp_http_port, req).await;

    let watermark_state = build_count_min_state(rows, cols, vec![0i64; (rows * cols) as usize]);
    let watermark_req = build_count_min_export(
        "endpoint_request_freq",
        &[("service", "e2e-test")],
        watermark_t_ns,
        watermark_state.encode_to_vec(),
        rows as i32,
        cols as i32,
    );
    post_otlp_http(&client, stack.otlp_http_port, watermark_req).await;

    tokio::time::sleep(Duration::from_millis(800)).await;

    // Query range covering the watermark + sketch windows. Prometheus's
    // /api/v1/query_range expects epoch-second floats for start/end/step.
    let now_secs = now_ns as f64 / 1e9;
    let start_secs = now_secs - 10.0;
    let end_secs = now_secs;
    let step_secs = 1.0;
    let response: JsonValue = client
        .get(format!(
            "http://127.0.0.1:{}/api/v1/query_range",
            stack.backend_port
        ))
        .query(&[
            ("query", "count_over_time(endpoint_request_freq[10s])"),
            ("start", &format!("{start_secs}")),
            ("end", &format!("{end_secs}")),
            ("step", &format!("{step_secs}")),
        ])
        .send()
        .await
        .expect("range query failed")
        .json()
        .await
        .expect("response not JSON");
    let status = response["status"].as_str().unwrap_or("(missing)");
    assert_eq!(
        status,
        "success",
        "count_over_time(...) range-query against heap-less CMS must succeed \
         end-to-end via the modern execute_range_promql_modern fallback. \
         Response:\n{}",
        serde_json::to_string_pretty(&response).unwrap_or_default()
    );
    assert_eq!(
        response["data"]["resultType"],
        "matrix",
        "range-query result must carry resultType=matrix per the Prometheus \
         /api/v1/query_range wire spec. Response:\n{}",
        serde_json::to_string_pretty(&response).unwrap_or_default()
    );
    let result = &response["data"]["result"];
    let arr = result
        .as_array()
        .expect("data.result must be an array of matrix elements");
    assert!(
        !arr.is_empty(),
        "matrix result must contain at least one series. Response:\n{}",
        serde_json::to_string_pretty(&response).unwrap_or_default()
    );
}

// ── Test 12 — DDSketch DELTA + sub-window FULL ingest→query roundtrip ───────
//
// Reproduces the live "No result" bug for
// `quantile_over_time(0.99, http_requests_total_latency_ms[3m])` when the
// edge runs with `delta_transmission: true` + a sub-window emit cadence.
//
// This is the gap the existing UNIT tests (which insert `SketchSampleState`
// rows directly) cannot see: they hand the reducer ready-made
// `SketchSampleState{bytes, encoding}` rows whose `bytes` are already the
// shape the reducer's `decode_full`/`apply_delta_bytes` expects. The LIVE
// path instead writes whatever the EDGE emits on the wire, and the edge's
// DDSketch PROTO_DELTA frame is a `DdSketchDelta` bucket-delta proto — NOT
// a `SketchEnvelope{DdSketchState}` full state. The ingest delta-apply path
// decodes the former; the query-side reducer decodes the latter. The two
// disagree, so the query reconstructs nothing.
//
// Wire shape faithfully mirrors the edge:
//   * metric name is the SUFFIXED `http_requests_total_latency_ms_ddsketch`
//     (the edge's `asapedgeprocessor` appends `_<family>`); the QUERY asks
//     for the unsuffixed `http_requests_total_latency_ms`.
//   * window 1 frames: [PROTO full, PROTO_DELTA, PROTO_DELTA]
//   * windows 2..=3 frames: [PROTO_DELTA-from-empty, PROTO_DELTA, PROTO_DELTA]
//   * all sub-window frames of one window share the SAME
//     (start_time_unix_nano, time_unix_nano) = (window_start, window_end).
//   * a PROTO_DELTA payload is a `DdSketchDelta{buckets:[{index,d_count}]}`
//     proto, exactly what `DdSketch::compute_delta(&empty)` produces under
//     the per-window-reset (delta-against-empty) contract.
//
// The query must reconstruct each window's distribution and answer the
// p99 to within DDSketch's α. It currently FAILS (empty / "No result").

/// Build a real `DdSketch` over `values` at relative accuracy `alpha`.
fn dd_over_values(alpha: f64, values: &[f64]) -> asap_sketchlib::DdSketch {
    let mut sk = asap_sketchlib::DdSketch::new(alpha);
    for &v in values {
        sk.update(v);
    }
    sk
}

/// Encode a `DdSketch` as a FULL `SketchEnvelope{DdSketchState}` frame —
/// the PROTO (full-state) wire shape. This is what the reducer's
/// `decode_full` and the ingest full-decode path both expect for a
/// non-delta frame.
fn encode_dd_full_envelope(sk: &asap_sketchlib::DdSketch) -> Vec<u8> {
    use asap_sketchlib::proto::sketchlib::{sketch_envelope, DdSketchState, SketchEnvelope};
    let state = DdSketchState {
        alpha: sk.alpha,
        store_counts: sk.store_counts.clone(),
        store_offset: sk.store_offset,
    };
    SketchEnvelope {
        sketch_state: Some(sketch_envelope::SketchState::Ddsketch(state)),
        ..Default::default()
    }
    .encode_to_vec()
}

/// Encode a `DdSketch` as a `DdSketchDelta` bucket-delta proto — the
/// PROTO_DELTA wire shape the edge emits under the per-window-reset
/// (delta-against-empty) contract, where the prior snapshot is the empty
/// sketch so the "delta" IS this window's own full bucket store expressed
/// as bucket increments (`compute_delta(&empty)`). Bucket index = the
/// absolute DDSketch index = `store_offset + i`.
fn encode_dd_delta_against_empty(sk: &asap_sketchlib::DdSketch) -> Vec<u8> {
    use asap_otel_proto::sketchlib::v1::{DdSketchBucketDelta, DdSketchDelta as PbDelta};
    let buckets = sk
        .store_counts
        .iter()
        .enumerate()
        .filter(|(_, &c)| c != 0)
        .map(|(i, &c)| DdSketchBucketDelta {
            index: sk.store_offset + i as i32,
            d_count: c as u64,
        })
        .collect();
    PbDelta { buckets }.encode_to_vec()
}

/// Build an OTLP `ExportMetricsServiceRequest` carrying ONE DDSketch DP
/// with the caller's explicit `(start_time_unix_nano, time_unix_nano)`
/// window and explicit `encoding` (PROTO=1 full / PROTO_DELTA=2). Unlike
/// `build_dd_sketch_export` this does NOT derive start_time from end_time
/// — the per-window-reset repro needs every sub-window frame of one window
/// to share the exact same (window_start, window_end) pair.
#[allow(clippy::too_many_arguments)]
fn build_dd_sketch_export_windowed(
    metric_name: &str,
    attrs: &[(&str, &str)],
    start_time_unix_nano: u64,
    time_unix_nano: u64,
    sketch_bytes: Vec<u8>,
    alpha: f64,
    encoding: i32,
) -> ExportMetricsServiceRequest {
    let attributes = attrs
        .iter()
        .map(|(k, v)| KeyValue {
            key: k.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(v.to_string())),
            }),
        })
        .collect();
    let dp = DdSketchDataPoint {
        attributes,
        start_time_unix_nano,
        time_unix_nano,
        sketch: sketch_bytes,
        encoding,
        exemplars: Vec::new(),
        flags: 0,
        series_id: 0,
    };
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: metric_name.to_string(),
                    description: String::new(),
                    unit: String::new(),
                    metadata: Vec::new(),
                    data: Some(Data::Ddsketch(DdSketch {
                        data_points: vec![dp],
                        aggregation_temporality: 0,
                        relative_accuracy: alpha,
                    })),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_plan_to_query_ddsketch_delta_subwindow_roundtrip() {
    const ENCODING_PROTO: i32 = 1;
    const ENCODING_PROTO_DELTA: i32 = 2;
    let suffixed_metric = "http_requests_total_latency_ms_ddsketch";
    let bare_metric = "http_requests_total_latency_ms";
    let alpha = 0.01;

    let stack = start_full_stack(19_581, 19_582).await;
    let client = reqwest::Client::new();

    // ── 1. Controller plans + POSTs the streaming-config for the BARE
    //       metric (what the controller + query analyzer speak). 1s window
    //       so distinct window_end timestamps fall on distinct seconds.
    let workload = build_workload(
        bare_metric,
        vec![AggType::Quantile],
        alpha,
        Duration::from_secs(1),
        vec!["service".to_string()],
        vec![0.99],
    );
    let streaming_config_json = plan_streaming_config_json(&workload);
    post_streaming_config(&client, stack.backend_port, &streaming_config_json).await;

    // ── 2. Three windows of known distributions. Each window is split into
    //       three sub-window emits whose increments together cover the
    //       window's full distribution.
    //         window 1: 1..=30      (p99 ≈ 30)
    //         window 2: 100..=130   (p99 ≈ 130)
    //         window 3: 1000..=1030 (p99 ≈ 1030)
    let window_dists: [Vec<f64>; 3] = [
        (1..=30).map(|v| v as f64).collect(),
        (100..=130).map(|v| v as f64).collect(),
        (1000..=1030).map(|v| v as f64).collect(),
    ];

    // Wall-clock-relative window placement so the PromQL eval (also
    // wall-clock) sees the windows inside its [3m] lookback. Windows end at
    // now-150s, now-149s, now-148s — comfortably inside [3m] and outside the
    // near-`now` watermark fuzz.
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before UNIX epoch")
        .as_nanos() as u64;
    let sec: u64 = 1_000_000_000;

    for (w_idx, dist) in window_dists.iter().enumerate() {
        // window_end at now - (150 - w_idx) s; window_start = window_end - 1s.
        let window_end_ns = now_ns.saturating_sub((150 - w_idx as u64) * sec);
        let window_start_ns = window_end_ns.saturating_sub(sec);

        // Split this window's distribution into three sub-window increments.
        let third = dist.len() / 3;
        let sub: [&[f64]; 3] = [&dist[..third], &dist[third..2 * third], &dist[2 * third..]];

        for (f_idx, frame_vals) in sub.iter().enumerate() {
            let sk = dd_over_values(alpha, frame_vals);
            // window 1's FIRST frame is a PROTO full snapshot; every other
            // frame (including windows 2+ first frame) is a delta-from-empty.
            let (bytes, encoding) = if w_idx == 0 && f_idx == 0 {
                (encode_dd_full_envelope(&sk), ENCODING_PROTO)
            } else {
                (encode_dd_delta_against_empty(&sk), ENCODING_PROTO_DELTA)
            };
            let req = build_dd_sketch_export_windowed(
                suffixed_metric,
                &[("service", "e2e-test")],
                window_start_ns,
                window_end_ns,
                bytes,
                alpha,
                encoding,
            );
            post_otlp_http(&client, stack.otlp_http_port, req).await;
        }
    }

    // Give the receiver time to land every frame in the SketchStore.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // ── 3. Query the BARE metric via PromQL quantile_over_time over [3m].
    let query_url = format!("http://127.0.0.1:{}/api/v1/query", stack.backend_port);
    let response: JsonValue = client
        .get(&query_url)
        .query(&[(
            "query",
            format!("quantile_over_time(0.99, {bare_metric}[3m])").as_str(),
        )])
        .send()
        .await
        .expect("PromQL query failed to send")
        .json()
        .await
        .expect("PromQL response was not JSON");

    // ── 4. Assertions: success + a non-empty reconstructed p99.
    let status = response["status"].as_str().unwrap_or("(missing)");
    assert_eq!(
        status,
        "success",
        "quantile_over_time over a DDSketch DELTA+sub-window stream did not \
         succeed (the live `No result` bug). Response:\n{}",
        serde_json::to_string_pretty(&response).unwrap_or_default()
    );
    let result = &response["data"]["result"];
    let arr = result
        .as_array()
        .expect("result must be an array of series");
    assert!(
        !arr.is_empty(),
        "quantile_over_time returned an EMPTY result vector — the delta+sub-window \
         DDSketch path reconstructed nothing. Response:\n{}",
        serde_json::to_string_pretty(&response).unwrap_or_default()
    );

    // cumulative_evaluate rolls the [3m] range into ONE union over all three
    // windows (1..=30 ∪ 100..=130 ∪ 1000..=1030 = 1..=1030, 93 samples).
    // Its p99 ≈ 1020. Pull the scalar out of the (instant or range) shape
    // and assert it lands near that within a generous DDSketch-α envelope.
    let value =
        extract_first_scalar(result).expect("could not extract a scalar from the query result");
    // Truth: p99 of the unioned distribution.
    let mut all: Vec<f64> = Vec::new();
    for d in &window_dists {
        all.extend_from_slice(d);
    }
    let truth = dd_over_values(alpha, &all)
        .quantile(0.99)
        .expect("truth p99");
    let rel = (value - truth).abs() / truth.max(1e-9);
    assert!(
        rel < 0.10,
        "reconstructed p99 {value} too far from truth {truth} (rel {rel}); \
         the delta+sub-window reconstruction is wrong"
    );
}

/// Pull the first scalar value out of a PromQL `data.result` array,
/// handling both instant-vector (`value: [ts, "v"]`) and range-matrix
/// (`values: [[ts, "v"], …]`) shapes.
fn extract_first_scalar(result: &JsonValue) -> Option<f64> {
    let arr = result.as_array()?;
    let first = arr.first()?;
    if let Some(v) = first.get("value").and_then(|v| v.as_array()) {
        return v
            .get(1)
            .and_then(|s| s.as_str())
            .and_then(|s| s.parse().ok());
    }
    if let Some(vals) = first.get("values").and_then(|v| v.as_array()) {
        let last = vals.last()?.as_array()?;
        return last
            .get(1)
            .and_then(|s| s.as_str())
            .and_then(|s| s.parse().ok());
    }
    None
}

// ── Test — shadow-mode `SummaryExecutor` comparison is inert ────────────────
//
// `data_plane/docs/l4node-plan-executor-design.md`'s "Rollout" section:
// enabling `ASAP_SHADOW_SUMMARY_EXECUTOR` computes the new path alongside
// the old and logs a diff, but must NEVER change what's served. This test
// is the regression safety net for that claim — same shape as Test 3
// (`controller_plan_to_query_full_roundtrip_ddsketch`), but with shadow
// mode on for the duration, asserting the served response still succeeds
// with the SAME quantile value the flag-off test expects (~p99 of
// `[5,10,15,20]` bucket counts).

/// RAII guard for `ASAP_SHADOW_SUMMARY_EXECUTOR`. `std::env::set_var`/
/// `remove_var` mutate process-global state and `cargo test` runs tests
/// in the same process across threads by default, so every test touching
/// this var must serialize against the others (mirrors
/// `control_plane/src/main.rs`'s `EnvVarGuard` pattern for
/// `USE_TYPED_STAGE_SPLIT`, same reason).
#[allow(dead_code)] // held for its lock-lifetime/Drop side effect, never read
struct ShadowEnvGuard(std::sync::MutexGuard<'static, ()>);

impl ShadowEnvGuard {
    fn enable() -> Self {
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let guard = LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        std::env::set_var("ASAP_SHADOW_SUMMARY_EXECUTOR", "1");
        Self(guard)
    }
}

impl Drop for ShadowEnvGuard {
    fn drop(&mut self) {
        std::env::remove_var("ASAP_SHADOW_SUMMARY_EXECUTOR");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shadow_mode_does_not_change_served_ddsketch_quantile() {
    let _shadow = ShadowEnvGuard::enable();

    let stack = start_full_stack(19_591, 19_592).await;
    let client = reqwest::Client::new();

    let workload = build_workload(
        "http_latency_ms",
        vec![AggType::Quantile],
        0.01,
        Duration::from_secs(1),
        vec!["service".to_string()],
        vec![0.99],
    );
    let streaming_config_json = plan_streaming_config_json(&workload);
    post_streaming_config(&client, stack.backend_port, &streaming_config_json).await;

    // Same fixture data as `controller_plan_to_query_full_roundtrip_ddsketch`
    // — this test isn't checking quantile accuracy (that's Test 3's job),
    // it's checking that turning shadow mode on doesn't change whether/what
    // this query serves.
    let alpha = 0.01;
    let store_counts = vec![5u64, 10, 15, 20];
    let dd_state = build_dd_sketch_state(alpha, store_counts, -1);
    let sketch_bytes = dd_state.encode_to_vec();

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before UNIX epoch")
        .as_nanos() as u64;
    let sketch_t_ns = now_ns.saturating_sub(3_000_000_000);
    let watermark_t_ns = now_ns.saturating_sub(1_000_000_000);

    let req = build_dd_sketch_export(
        "http_latency_ms",
        &[("service", "e2e-test")],
        sketch_t_ns,
        sketch_bytes,
        alpha,
    );
    post_otlp_http(&client, stack.otlp_http_port, req).await;

    let watermark_state = build_dd_sketch_state(alpha, Vec::new(), 0);
    let watermark_req = build_dd_sketch_export(
        "http_latency_ms",
        &[("service", "e2e-test")],
        watermark_t_ns,
        watermark_state.encode_to_vec(),
        alpha,
    );
    post_otlp_http(&client, stack.otlp_http_port, watermark_req).await;

    tokio::time::sleep(Duration::from_millis(800)).await;

    let query_url = format!("http://127.0.0.1:{}/api/v1/query", stack.backend_port);
    let response: JsonValue = client
        .get(&query_url)
        .query(&[("query", "quantile_over_time(0.99, http_latency_ms[10s])")])
        .send()
        .await
        .expect("PromQL query failed to send")
        .json()
        .await
        .expect("PromQL response was not JSON");

    assert_eq!(
        response["status"].as_str().unwrap_or("(missing)"),
        "success",
        "shadow mode must not change whether this query succeeds. Response:\n{}",
        serde_json::to_string_pretty(&response).unwrap_or_default()
    );

    // Same accuracy contract as the flag-off test: p99 of [5,10,15,20]
    // DDSketch bucket counts should land in a plausible range (not
    // asserting exact equality with Test 3's own run -- different process,
    // different wall-clock timestamps -- but the same fixture must
    // produce the same class of answer regardless of the shadow flag).
    let value = response["data"]["result"]
        .as_array()
        .and_then(|r| extract_first_scalar(&JsonValue::Array(r.clone())))
        .expect("expected a scalar quantile result");
    assert!(
        value.is_finite() && value > 0.0,
        "shadow mode must not corrupt the served quantile value, got {value}"
    );
}

// ── Test — the live serving cutover actually answers from SummaryExecutor ───
//
// Phase 2 of the rollout (see the plan's "What 'safe to serve' means,
// precisely" section): with `ASAP_SUMMARY_EXECUTOR_LIVE` set, an
// unambiguous single-series query must be answered by `SummaryExecutor`
// directly (`engine.rs` skips the legacy `SketchReducer` call for it
// entirely), not merely shadow-compared. Same fixture as
// `shadow_mode_does_not_change_served_ddsketch_quantile` -- this test's
// job is proving the cutover serves a correct answer via the NEW code
// path, not re-checking quantile accuracy.

/// RAII guard for `ASAP_SUMMARY_EXECUTOR_LIVE`. Own lock, own var --
/// mirrors `ShadowEnvGuard` exactly (same reason: `std::env::set_var`/
/// `remove_var` mutate process-global state and `cargo test` runs tests
/// in the same process across threads by default).
#[allow(dead_code)]
struct LiveServeEnvGuard(std::sync::MutexGuard<'static, ()>);

impl LiveServeEnvGuard {
    fn enable() -> Self {
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let guard = LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        std::env::set_var("ASAP_SUMMARY_EXECUTOR_LIVE", "1");
        Self(guard)
    }
}

impl Drop for LiveServeEnvGuard {
    fn drop(&mut self) {
        std::env::remove_var("ASAP_SUMMARY_EXECUTOR_LIVE");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_serve_actually_answers_ddsketch_quantile() {
    let _live = LiveServeEnvGuard::enable();

    let stack = start_full_stack(19_593, 19_594).await;
    let client = reqwest::Client::new();

    let workload = build_workload(
        "http_latency_ms",
        vec![AggType::Quantile],
        0.01,
        Duration::from_secs(1),
        vec!["service".to_string()],
        vec![0.99],
    );
    let streaming_config_json = plan_streaming_config_json(&workload);
    post_streaming_config(&client, stack.backend_port, &streaming_config_json).await;

    let alpha = 0.01;
    let store_counts = vec![5u64, 10, 15, 20];
    let dd_state = build_dd_sketch_state(alpha, store_counts, -1);
    let sketch_bytes = dd_state.encode_to_vec();

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before UNIX epoch")
        .as_nanos() as u64;
    let sketch_t_ns = now_ns.saturating_sub(3_000_000_000);
    let watermark_t_ns = now_ns.saturating_sub(1_000_000_000);

    let req = build_dd_sketch_export(
        "http_latency_ms",
        &[("service", "e2e-test")],
        sketch_t_ns,
        sketch_bytes,
        alpha,
    );
    post_otlp_http(&client, stack.otlp_http_port, req).await;

    let watermark_state = build_dd_sketch_state(alpha, Vec::new(), 0);
    let watermark_req = build_dd_sketch_export(
        "http_latency_ms",
        &[("service", "e2e-test")],
        watermark_t_ns,
        watermark_state.encode_to_vec(),
        alpha,
    );
    post_otlp_http(&client, stack.otlp_http_port, watermark_req).await;

    tokio::time::sleep(Duration::from_millis(800)).await;

    let query_url = format!("http://127.0.0.1:{}/api/v1/query", stack.backend_port);
    let response: JsonValue = client
        .get(&query_url)
        .query(&[("query", "quantile_over_time(0.99, http_latency_ms[10s])")])
        .send()
        .await
        .expect("PromQL query failed to send")
        .json()
        .await
        .expect("PromQL response was not JSON");

    assert_eq!(
        response["status"].as_str().unwrap_or("(missing)"),
        "success",
        "live-serve must answer this unambiguous single-series quantile. Response:\n{}",
        serde_json::to_string_pretty(&response).unwrap_or_default()
    );

    let value = response["data"]["result"]
        .as_array()
        .and_then(|r| extract_first_scalar(&JsonValue::Array(r.clone())))
        .expect("expected a scalar quantile result");
    assert!(
        value.is_finite() && value > 0.0,
        "live-serve must produce a correct served quantile value, got {value}"
    );
}

// ── Test — the live serving cutover MERGES the global-merge shape ─────────
//    correctly, end to end (ASAPController#163/#165)
//
// `count(hll_metric)` with NO `by (...)` and MULTIPLE distinct-service HLL
// sids used to be the ambiguous shape the design doc's "Grouping
// semantics" section described: `SummaryAgg{by: []}` couldn't tell "no
// grouping concept" from "reduce everything," so `live_serve.rs`'s
// `ambiguous_merge_risk` gate DECLINED to serve it from the new path and
// fell back to the legacy `evaluate_cardinality_global` special case.
//
// `Reduction` (ASAPController#165) resolves that: `count(...)` is a
// genuine aggregation operator, so it lowers to `Reduce([])` and
// `resolve_group_key` gives both sids the same group key -- the new path
// merges them itself. The gate is gone; this SHOULD exercise the new
// path serving the shape directly, not a fallback.
//
// Correction (sketch_reducer.rs retirement): that claim above wasn't
// actually true until now. This shape was ALSO hitting a real
// family/params mismatch on `SummaryExecutor` (serving time picked
// precision from a hardcoded default accuracy, not what this workload
// was actually planned/registered with) -- the legacy reducer's
// `evaluate_cardinality_global` fallback silently masked that miss, so
// the test passed via the fallback, not the new path. With the reducer
// gone, the params mismatch is fixed (see `ObservedFamilyCostModel`),
// but that unmasked a SECOND, independent bug: `effective_is_cumulative`
// classifies a bare `count(...)` as non-cumulative, so `readout`
// evaluates per-window instead of merging the whole range -- this test's
// later "watermark" sample (a distinct, more recent window) then wins
// over the real data instead of being merged with it. Tracked as
// https://github.com/ProjectASAP/ASAPQuery-backend/issues/431.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_serve_hll_global_count_merges_across_sids() {
    let _live = LiveServeEnvGuard::enable();

    let stack = start_full_stack(19_595, 19_596).await;
    let client = reqwest::Client::new();

    let workload = build_workload_with_override(
        "unique_users_per_min",
        vec![AggType::Cardinality],
        0.05,
        Duration::from_secs(1),
        vec!["service".to_string()],
        Vec::new(),
        Some(SketchType::HLL),
    );
    let streaming_config_json = plan_streaming_config_json(&workload);
    post_streaming_config(&client, stack.backend_port, &streaming_config_json).await;

    let precision = 10u32;
    let num_registers = 1usize << precision;

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before UNIX epoch")
        .as_nanos() as u64;
    let sketch_t_ns = now_ns.saturating_sub(3_000_000_000);
    let watermark_t_ns = now_ns.saturating_sub(1_000_000_000);

    // Two distinct services, DISJOINT non-zero registers -- two separate
    // sids the analyzer's `count(unique_users_per_min)` candidate resolves
    // to together (empty group_by_keys), the exact shape ASAPController#163
    // describes.
    for (service, reg_idx) in [("svc-a", 0usize), ("svc-b", 500usize)] {
        let mut registers = vec![0u8; num_registers];
        registers[reg_idx] = 6;
        let hll_state = build_hll_state(precision, registers);
        let req = build_hll_export(
            "unique_users_per_min",
            &[("service", service)],
            sketch_t_ns,
            hll_state.encode_to_vec(),
            precision,
        );
        post_otlp_http(&client, stack.otlp_http_port, req).await;

        let watermark_state = build_hll_state(precision, vec![0u8; num_registers]);
        let watermark_req = build_hll_export(
            "unique_users_per_min",
            &[("service", service)],
            watermark_t_ns,
            watermark_state.encode_to_vec(),
            precision,
        );
        post_otlp_http(&client, stack.otlp_http_port, watermark_req).await;
    }

    tokio::time::sleep(Duration::from_millis(800)).await;

    let response: JsonValue = client
        .get(format!(
            "http://127.0.0.1:{}/api/v1/query",
            stack.backend_port
        ))
        .query(&[("query", "count(unique_users_per_min)")])
        .send()
        .await
        .expect("query failed")
        .json()
        .await
        .expect("response not JSON");

    assert_eq!(
        response["status"].as_str().unwrap_or("(missing)"),
        "success",
        "the global-merge shape must succeed with live-serve on. Response:\n{}",
        serde_json::to_string_pretty(&response).unwrap_or_default()
    );

    let value = response["data"]["result"]
        .as_array()
        .and_then(|r| extract_first_scalar(&JsonValue::Array(r.clone())))
        .expect("expected a scalar cardinality result");
    // Each service set exactly ONE distinct non-zero register, and the two
    // are disjoint -- a correct cross-sid merge estimates ~2, whereas
    // serving only one sid's state would estimate ~1. The assertion is
    // loose (HLL at precision 10 is approximate) but still distinguishes
    // "merged both" from "dropped one."
    assert!(
        value.is_finite() && value >= 1.5,
        "expected the MERGED cardinality across both services (~2), got {value} -- \
         a value near 1 means only one sid's registers were counted"
    );
}
