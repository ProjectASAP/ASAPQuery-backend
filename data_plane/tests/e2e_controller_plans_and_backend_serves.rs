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
    let adapter_config = AdapterConfig::prometheus_promql(
        "http://127.0.0.1:9999".to_string(),
        false,
    );
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
            .with_sketch_index(sketch_index.clone()),
    );
    let server = HttpServer::new(http_config, query_engine, sketch_index)
        .with_hot_reload_config(hot_reload.clone());
    let backend_port = server
        .start_test_server()
        .await
        .expect("start_test_server must succeed");

    // Wait for everything to bind.
    tokio::time::sleep(Duration::from_millis(500)).await;

    FullStack {
        backend_port,
        otlp_http_port,
    }
}

/// Build a `DdSketchState` proto from raw values.
fn build_dd_sketch_state(
    alpha: f64,
    store_counts: Vec<u64>,
    store_offset: i32,
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
) -> DdSketchState {
    DdSketchState {
        alpha,
        store_counts,
        store_offset,
        count,
        sum,
        min,
        max,
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
    let dp = DdSketchDataPoint {
        attributes,
        start_time_unix_nano: 0,
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
    let dp = KllSketchDataPoint {
        attributes,
        start_time_unix_nano: 0,
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

/// Build a `CountSketchState` proto from a signed matrix in row-major order.
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

/// Build an OTLP `ExportMetricsServiceRequest` wrapping a single CountSketch DP.
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

/// Build an OTLP `ExportMetricsServiceRequest` wrapping a single CountMinSketch DP.
fn build_count_min_export(
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

/// POST a protobuf-encoded `ExportMetricsServiceRequest` to the OTLP HTTP
/// receiver on `localhost:port/v1/metrics`. Panics with the unexpected
/// status code on non-2xx.
async fn post_otlp_http(
    client: &reqwest::Client,
    port: u16,
    req: ExportMetricsServiceRequest,
) {
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
//   4. Soft-check: harness queries `/api/v1/query`. Currently the query
//      returns `errorType: bad_data` (”No result for query”) — same
//      symptom that has the sibling `e2e_dd_sketch_modified_otlp_path`
//      test `#[ignore]`'d (”broken since proto refactor”). The
//      OTLP→precompute→`SketchStore` path is broken upstream from
//      this PR's scope, and tightening the query assertion is
//      deferred to whoever fixes the underlying proto path.
//
// What this PR's Test 3 anchors:
//   * Controller-emitted streaming-config + content fields are
//     parseable AND accepted at /api/v1/streaming-config (already
//     covered by Tests 1+2, re-exercised here to verify it doesn't
//     break when the engine + OTLP receiver are also running).
//   * Modified-OTLP `DdSketchDataPoint` wire encoding + the backend's
//     OTLP HTTP receiver accept the payload (no 4xx/5xx).
//   * The full stack (PrecomputeEngine + SketchStoreSink + OtlpReceiver
//     + HttpServer all sharing SketchStore + HotReloadStreamingConfig)
//     comes up and stays up under POST + query traffic.
//
// What it does NOT anchor (deferred):
//   * Whether the sketch state actually lands in `SketchStore` keyed
//     by the right `PolicyFingerprint`.
//   * Whether the query engine resolves the metric against the
//     stored sketch and returns the correct quantile.
// These hinge on the proto-refactor fix the existing
// `e2e_dd_sketch_modified_otlp_path` test is also waiting on.

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
    let dd_state = build_dd_sketch_state(alpha, store_counts, -1, 50, 150.0, 0.25, 8.0);
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
    let watermark_state = build_dd_sketch_state(alpha, Vec::new(), 0, 0, 0.0, 0.0, 0.0);
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
    let query_url = format!(
        "http://127.0.0.1:{}/api/v1/query",
        stack.backend_port
    );
    let response: JsonValue = client
        .get(&query_url)
        .query(&[(
            "query",
            "quantile_over_time(0.99, http_latency_ms[10s])",
        )])
        .send()
        .await
        .expect("PromQL query failed to send")
        .json()
        .await
        .expect("PromQL response was not JSON");

    // ── 6. Soft-check the query response. ──────────────────────────────
    //
    // Status field must exist (HTTP layer is healthy). Strict
    // `status == "success"` is still deferred. Investigation done in
    // the wake of #249's L5-walk fix surfaced a deeper gap further
    // along the query path:
    //
    //   * The OTLP DP MUST carry at least one attribute (or known
    //     sid). With both empty, the receiver hits the
    //     "invalid wire shape" drop in
    //     `route_modified_otlp_sketches_to_precompute` (otel.rs:926
    //     comment block — `(sid=0, no attrs)` is dropped). Test now
    //     attaches `service="e2e-test"` to the DP.
    //
    //   * The sketch's group_by_keys (derived from `dp.attrs.keys()`)
    //     MUST exactly match the streaming-config policy's
    //     `grouping_labels` for `find_policy_by_content` to bind
    //     `policy_fp`. The match is **strict** at ingest
    //     (asap_tier_analysis.rs:525) but **subset** at query time
    //     (asap_tier_analysis.rs:587) — that asymmetry is intentional
    //     (ingest needs uniqueness; queries can re-aggregate down).
    //     Test now uses `group_by_labels: ["service"]` to align.
    //
    //   * After both fixes, sketches DO reach `SketchStore` (the
    //     runtime-info `earliest_timestamp_per_sid` map is populated)
    //     but the query still returns `bad_data`/"No result for
    //     query". The remaining gap is between
    //     `SketchStore::instances_matching` and the engine's reducer
    //     dispatch — likely an asymmetry between the engine's
    //     candidate.required_capability and the policy_capability
    //     lookup, OR a sid-by-policy_fp reverse-index lookup failure.
    //     Untangling that requires deeper engine-path tracing not
    //     covered by this PR.
    assert!(
        response.get("status").is_some(),
        "PromQL response missing `status` field — HTTP layer is unhealthy\n{}",
        serde_json::to_string_pretty(&response).unwrap_or_default()
    );
    let status = response["status"].as_str().unwrap_or("(missing)");
    assert_eq!(
        status, "success",
        "PromQL query did not succeed after the modern-execute() fallback in \
         process_via_simple_engine. The legacy handle_query path can't read \
         sketch-backed sids (#252), but the fallback should now reach them via \
         the trait-dispatch path. Response:\n{}",
        serde_json::to_string_pretty(&response).unwrap_or_default()
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
        .query(&[(
            "query",
            "quantile_over_time(0.5, request_size_bytes[10s])",
        )])
        .send()
        .await
        .expect("query failed")
        .json()
        .await
        .expect("response not JSON");

    let status = response["status"].as_str().unwrap_or("(missing)");
    assert_eq!(
        status, "success",
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
        status, "success",
        "HLL cardinality query did not succeed:\n{}",
        serde_json::to_string_pretty(&response).unwrap_or_default()
    );
}

// ── Test 6 — wire-format roundtrip with CountSketch (frequency) ─────────────
//
// CountSketch backs FREQUENCY estimation — counting heavy hitters and
// producing approximate point-frequency answers. Workload pins
// CountSketch via `sketch_type_override: Some(SketchType::CountSketch)`.
// The OTLP DP carries a `CountSketchDataPoint` with `CountSketchState`.
//
// **Soft-check on the query (status field exists, no strict success).**
// Strict-success topk requires a heap-bearing variant
// (`CountSketchWithHeap` / `CmsWithHeap`) — `is_satisfied_by` rejects
// `FrequencyTopk` against `FrequencyEstimate`-only sids. Heap-bearing
// variants need msgpack-encoded payloads (per
// `sketch_kind_handle_for`'s detection path). Out of scope for this
// PR; tracked as the natural next step after wire-format coverage.
//
// Pure-PromQL has no first-class function for `FrequencyEstimate` (the
// reducer accepts `"frequency"` / `"frequency_estimate"` but those
// aren't valid PromQL). MetricsQL extensions in this area would be
// the queryable surface.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
        streaming_config_json["aggregations"][0]["aggregationType"], "CountSketch",
        "controller must emit CountSketch aggregationType for SketchType::CountSketch override\n{streaming_config_json}"
    );
    post_streaming_config(&client, stack.backend_port, &streaming_config_json).await;

    let rows = 5u32;
    let cols = 1024u32;
    let counts: Vec<i64> = (0..(rows * cols) as i64).map(|i| i % 7).collect();
    let cs_state = build_count_sketch_state(rows, cols, counts);
    let sketch_bytes = cs_state.encode_to_vec();

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before UNIX epoch")
        .as_nanos() as u64;
    let sketch_t_ns = now_ns.saturating_sub(3_000_000_000);
    let watermark_t_ns = now_ns.saturating_sub(1_000_000_000);

    let req = build_count_sketch_export(
        "top_endpoint_qps",
        &[("service", "e2e-test")],
        sketch_t_ns,
        sketch_bytes,
    );
    post_otlp_http(&client, stack.otlp_http_port, req).await;

    let watermark_state = build_count_sketch_state(rows, cols, vec![0i64; (rows * cols) as usize]);
    let watermark_req = build_count_sketch_export(
        "top_endpoint_qps",
        &[("service", "e2e-test")],
        watermark_t_ns,
        watermark_state.encode_to_vec(),
    );
    post_otlp_http(&client, stack.otlp_http_port, watermark_req).await;

    tokio::time::sleep(Duration::from_millis(800)).await;

    // Soft-check: `topk(5, ...)` won't succeed against heap-less
    // CountSketch (capability mismatch — see test doc), so we just
    // assert the response is well-formed JSON with a `status` field.
    // The real success signal is that the OTLP POSTs above returned
    // 2xx (the wire-format ingest works) and `runtime_info` would
    // show the sid registered.
    let response: JsonValue = client
        .get(format!(
            "http://127.0.0.1:{}/api/v1/query",
            stack.backend_port
        ))
        .query(&[("query", "topk(5, top_endpoint_qps)")])
        .send()
        .await
        .expect("query failed")
        .json()
        .await
        .expect("response not JSON");
    assert!(
        response.get("status").is_some(),
        "PromQL response missing `status` field — HTTP layer is unhealthy\n{}",
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
// **Soft-check on the query, same rationale as Test 6.** Strict-success
// topk needs `CountMinSketchWithHeap` (with msgpack-encoded heap) —
// out of scope for this PR.

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

    let rows = 5u32;
    let cols = 2048u32;
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
    );
    post_otlp_http(&client, stack.otlp_http_port, req).await;

    let watermark_state = build_count_min_state(rows, cols, vec![0i64; (rows * cols) as usize]);
    let watermark_req = build_count_min_export(
        "endpoint_request_freq",
        &[("service", "e2e-test")],
        watermark_t_ns,
        watermark_state.encode_to_vec(),
    );
    post_otlp_http(&client, stack.otlp_http_port, watermark_req).await;

    tokio::time::sleep(Duration::from_millis(800)).await;

    // Soft-check: pure-PromQL has no `frequency_estimate` function,
    // and `topk(...)` requires a heap-bearing CMS. Assert response
    // shape only; ingest 2xx already confirmed wire-format coverage.
    let response: JsonValue = client
        .get(format!(
            "http://127.0.0.1:{}/api/v1/query",
            stack.backend_port
        ))
        .query(&[("query", "topk(5, endpoint_request_freq)")])
        .send()
        .await
        .expect("query failed")
        .json()
        .await
        .expect("response not JSON");
    assert!(
        response.get("status").is_some(),
        "PromQL response missing `status` field — HTTP layer is unhealthy\n{}",
        serde_json::to_string_pretty(&response).unwrap_or_default()
    );
}
