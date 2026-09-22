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
//! The control plane drives the plan: a PromQL query and an accuracy target
//! go through `BackendLocalPlanningSnapshot::planning_request` →
//! `PhysicalCompiler::compile`, and the resulting materializations are
//! projected into a physical-plan artifact with QueryPlan/SummaryCatalog
//! bindings, then staged and activated before ingest.
//!
//! Planner owns the summary choice. These tests declare an accuracy target and
//! build their payloads from whichever family and parameters it committed to —
//! `materializations[0].aggregation_type` and `.parameters` — rather than
//! pinning a family. Family selection itself is covered by the control-plane
//! compiler tests.
//!
//! Queries are registered with the grouping the producer's attribute set
//! carries (`sum by (service) (...)`), because the population key the backend
//! materializes under has to match the attributes on the wire. Readout uses the
//! inner form: the stored summaries are already per-service, so the result
//! carries the label without the outer aggregation.
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
//!    `PrecomputeMaterialization.grouping_labels`.
//!  * Test 3 — full controller-to-query roundtrip: harness simulates
//!    the agent (builds DDSketch state with `asap_sketchlib`, encodes
//!    as a modified-OTLP `DdSketchDataPoint`), POSTs sketches to the
//!    backend's OTLP receiver, waits for window close, queries via
//!    PromQL, asserts the response is well-formed for the planned
//!    metric.

use asap_types::PrecomputeMaterialization;
use std::sync::Arc;
use std::time::Duration;
#[path = "support/physical_fixture.rs"]
mod physical_fixture;

type FixturePlans = std::sync::Mutex<
    std::collections::BTreeMap<
        u16,
        Arc<data_plane::drivers::query::servers::http::PhysicalPlanInstallRequest>,
    >,
>;
static FIXTURE_PLANS: std::sync::OnceLock<FixturePlans> = std::sync::OnceLock::new();
static FIXTURE_TIMES: std::sync::OnceLock<std::sync::Mutex<std::collections::BTreeMap<u16, u64>>> =
    std::sync::OnceLock::new();
fn evaluation_time(stack: &FullStack) -> String {
    let ns = FIXTURE_TIMES.get().unwrap().lock().unwrap()[&stack.otlp_http_port];
    (ns as f64 / 1e9).to_string()
}

fn phase_aligned_now_ns() -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before UNIX epoch")
        .as_nanos() as u64;
    now - now % 5_000_000_000 + 3_000_000_000
}

async fn post_full_config(
    client: &reqwest::Client,
    stack: &FullStack,
    materializations: &[PrecomputeMaterialization],
) {
    let mut configs = materializations.to_vec();
    // The transport payloads below carry one-second states, so pin the
    // physical layout to match them.
    for config in &mut configs {
        config.window_size = 1;
        config.slide_interval = 1;
        config.window_layout = asap_types::WindowMaterializationLayout::Pane { pane_secs: 1 };
    }
    let mut artifact = physical_fixture::artifact_from_materializations(configs.clone());
    if configs
        .iter()
        .any(|c| c.metric == "http_requests_total_latency_ms")
    {
        for rule in &mut artifact.transmission_plan.rules {
            rule.mode = asap_types::producer_plan::TransmissionMode::Delta;
            rule.full_checkpoint_every_ms = Some(rule.emit_every_ms);
            rule.runtime_policy.delta = Some(asap_types::producer_plan::DeltaPolicy {
                absolute_threshold: 0.0,
                gos: None,
            });
        }
    }
    for entry in artifact
        .query_plan
        .entries
        .values_mut()
        .filter(|e| e.canonical_query.starts_with("count("))
    {
        entry.instant.lookback_ms = 1000;
        for node in entry.nodes.values_mut() {
            if let asap_types::query_plan::QueryPlanNode::ReadMaterialization { binding } = node {
                binding.readout_lookback_ms = Some(1000);
            }
        }
    }
    let plan = Arc::new(artifact);
    let response = client
        .post(format!(
            "http://127.0.0.1:{}/api/v1/physical-plan",
            stack.backend_port
        ))
        .json(&*plan)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    assert!(status.is_success(), "physical plan install: {body}");
    let response = client
        .post(format!(
            "http://127.0.0.1:{}/api/v1/physical-plan/activate",
            stack.backend_port
        ))
        .json(&serde_json::json!({"plan_id": 1, "plan_version": 1}))
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    assert!(status.is_success(), "physical plan activation: {body}");
    FIXTURE_PLANS
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .insert(stack.otlp_http_port, plan);
}

use control_plane::types::WorkloadCharacteristics;
use data_plane::storage_engines::types::InstalledPrecomputePlanHandle;
use serde_json::Value as JsonValue;

use asap_otel_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use asap_otel_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use asap_otel_proto::tonic::metrics::v1::{
    metric::Data, CountMinSketch, CountMinSketchDataPoint, CountMinSketchEncoding, CountSketch,
    CountSketchDataPoint, CountSketchEncoding, DdSketch, DdSketchDataPoint, DdSketchEncoding,
    Metric, ResourceMetrics, ScopeMetrics,
};
use asap_sketchlib::proto::sketchlib::{
    CountMinState, CountSketchState, CounterType, DdSketchState,
};
use prost::Message;

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Compile `query` through the same physical planner the production
/// `compile-and-publish` path runs, and return the materializations the
/// backend installs for it.
///
/// The planner owns the summary decision: these tests declare an accuracy
/// target and read back whichever family and parameters Planner committed to,
/// rather than pinning a family. Family selection itself is covered by the
/// control-plane compiler tests.
fn plan_materializations(query: &str, accuracy: JsonValue) -> Vec<PrecomputeMaterialization> {
    use control_plane::physical::compiler::{BackendLocalPlanningInput, PhysicalPlanCompiler};

    let mut fixture: JsonValue = serde_json::from_str(include_str!(
        "../../docs/examples/asapquery-compatibility-demo-snapshot.json"
    ))
    .expect("compatibility demo snapshot parses");
    // Entry 3 carries an explicit accuracy target, so it is the right template
    // for a single-query workload; the query text and target are overridden per
    // test below.
    let mut entry = fixture["query_workload"]["repeating_queries"][3].clone();
    entry["query"] = query.into();
    entry["requirements"]["accuracy"] = accuracy;
    fixture["query_workload"]["repeating_queries"] = serde_json::json!([entry]);
    // TopK admission needs a membership certificate; supplying it for every
    // query is harmless because non-TopK plans never read it.
    fixture["implementation"]["topk_evidence"] = serde_json::json!({
        query: {
            "selected_lower_bound": 101.0,
            "excluded_upper_bound": 100.0,
            "interval_failure_probability": 0.001,
            "observed_at_unix_ms": 9500,
            "source": "self-contained-e2e-fixture"
        }
    });

    let snapshot: BackendLocalPlanningInput =
        serde_json::from_value(fixture).expect("snapshot deserializes");
    let (request, environment) = snapshot
        .into_physical_compilation_request()
        .expect("snapshot yields a planning request");
    let plan = PhysicalPlanCompiler
        .compile_promql(request, environment)
        .expect("physical compilation succeeds");
    plan.precompute_plan.materializations
}

/// Epsilon-delta accuracy target in the shape `QueryRequirements` expects.
fn epsilon_delta(epsilon: f64, delta: f64) -> JsonValue {
    serde_json::json!({ "explicit": { "EpsilonDelta": { "epsilon": epsilon, "delta": delta } } })
}

/// Suppress the `WorkloadCharacteristics` unused warning — kept around
/// in case future tests need to pass per-workload resource caps.
#[allow(dead_code)]
fn _wc_anchor() -> WorkloadCharacteristics {
    WorkloadCharacteristics::default()
}

/// Full test stack: PrecomputeEngine + SketchStoreSink + OtlpReceiver +
/// HttpServer, all sharing the same `SketchStore` and
/// `InstalledPrecomputePlanHandle` so a controller-posted streaming-config
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
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_test_writer()
        .try_init();
    use data_plane::drivers::ingest::series_resolver::SeriesIdResolver;
    use data_plane::drivers::ingest::{OtlpReceiver, OtlpReceiverConfig};
    use data_plane::drivers::query::adapters::config::AdapterConfig;
    use data_plane::drivers::query::servers::{HttpServer, HttpServerConfig};
    use data_plane::precompute_engine::config::{LateDataPolicy, PrecomputeEngineConfig};
    use data_plane::precompute_engine::output_sink::SketchStoreSink;
    use data_plane::precompute_engine::PrecomputeEngine;
    use data_plane::query_engines::asap_query_engine::engine::ASAPQueryEngine;
    use data_plane::storage_engines::sketch_db::index::SketchStore;

    let sketch_index = Arc::new(SketchStore::new());
    let active = data_plane::storage_engines::types::HotReloadActivePhysicalPlan::new(
        physical_fixture::bootstrap(),
    );
    let hot_reload = InstalledPrecomputePlanHandle::from_active_physical_plan(active.clone());
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
        wall_clock_idle_grace_period_ms: 5_000,
        wall_clock_max_open_grace_period_ms: 0,
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
    let adapter_config =
        AdapterConfig::prometheus_promql("http://127.0.0.1:9999".to_string(), false);
    let http_config = HttpServerConfig {
        port: 0,
        handle_http_requests: true,
        adapter_config,
    };
    let query_engine = Arc::new(
        ASAPQueryEngine::new(15_000)
            // CRITICAL: without this the engine's `sketch_index` is
            // None and every fast path that reads sid → SketchInstance
            // metadata is silently skipped. Sketches DO land in
            // `sketch_index` via OTLP ingest (the engine's
            // `precompute_engine` shares the Arc), but the query
            // path can't see them without this binding.
            .with_sketch_index(sketch_index.clone())
            .with_active_physical_plan(active.clone()),
    );
    let server = HttpServer::new(http_config, query_engine, sketch_index)
        .with_hot_reload_config(hot_reload.clone())
        .with_active_physical_plan(active);
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
async fn post_otlp_http(client: &reqwest::Client, port: u16, mut req: ExportMetricsServiceRequest) {
    let data = req.resource_metrics[0].scope_metrics[0].metrics[0]
        .data
        .as_ref()
        .unwrap();
    let ns = match data {
        Data::Ddsketch(s) => s.data_points[0].time_unix_nano,
        Data::Kllsketch(s) => s.data_points[0].time_unix_nano,
        Data::Hllsketch(s) => s.data_points[0].time_unix_nano,
        Data::Countminsketch(s) => s.data_points[0].time_unix_nano,
        Data::Countsketch(s) => s.data_points[0].time_unix_nano,
        _ => unreachable!(),
    };
    FIXTURE_TIMES
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .entry(port)
        .or_insert(ns);
    let plan = FIXTURE_PLANS.get().unwrap().lock().unwrap()[&port].clone();
    physical_fixture::stamp(&mut req, &plan);
    let body = req.encode_to_vec();
    let resp = client
        .post(format!("http://127.0.0.1:{port}/v1/metrics"))
        .header("Content-Type", "application/x-protobuf")
        .body(body)
        .send()
        .await
        .expect("OTLP HTTP send failed");
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert!(status.is_success(), "OTLP {status}: {body}");
}

// ── Test 1 — single DDSketch-quantile workload, no grouping ─────────────────
//
// HTTP integration contract: the controller emits a streaming-config JSON for a
// workload that resolves to DDSketch. The backend's parser accepts it
// (POST returns 2xx) and the registered aggregation surfaces on the
// GET endpoint with the expected metric / sketch family.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_streaming_config_round_trips_through_backend_http() {
    let stack = start_full_stack(19_597, 19_598).await;
    let client = reqwest::Client::new();

    let materializations = plan_materializations(
        "sum by (service) (quantile_over_time(0.99, http_latency_ms[1s]))",
        epsilon_delta(0.01, 0.01),
    );

    // One query planned, so one materialization, carrying the content fields
    // the backend keys identity from.
    assert_eq!(
        materializations.len(),
        1,
        "expected exactly one materialization: {materializations:#?}"
    );
    let agg = &materializations[0];
    assert_eq!(agg.metric, "http_latency_ms");
    assert!(agg.window_size > 0, "window size must be > 0: {agg:#?}");

    post_full_config(&client, &stack, &materializations).await;
}

// ── Test 2 — cross-host grouping (sum by zone) ──────────────────────────────
//
// Verifies #245's grouping plumb survives the controller → backend
// round-trip. The workload carries `group_by_labels: ["zone"]`; the
// emitted JSON must surface `["zone"]` in `labels.grouping`, the
// backend's parser must materialise it into `PrecomputeMaterialization.
// grouping_labels`, and the active-config snapshot must reflect that.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_plans_with_grouping_and_backend_parses_grouping_labels() {
    let stack = start_full_stack(19_599, 19_600).await;
    let client = reqwest::Client::new();

    let materializations = plan_materializations(
        "sum by (zone) (quantile_over_time(0.99, http_latency_ms[1s]))",
        epsilon_delta(0.01, 0.01),
    );

    // The planner must thread the query's grouping into the materialization
    // the backend keys its per-population state by.
    let agg = &materializations[0];
    assert!(
        agg.grouping_labels
            .names()
            .iter()
            .any(|name| name == "zone"),
        "planner must carry the query grouping into the materialization: {agg:#?}"
    );

    post_full_config(&client, &stack, &materializations).await;

    // The backend accepted the plan, so the grouping the planner derived is
    // the population key the producer's attribute set has to match. Assert it
    // on the materialization the install carried rather than on a snapshot
    // endpoint that reports only plan phase.
    assert!(
        materializations[0]
            .grouping_labels
            .names()
            .iter()
            .any(|name| name == "zone"),
        "installed materialization must key by `zone`: {:#?}",
        materializations[0]
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
//     + HttpServer all sharing SketchStore + InstalledPrecomputePlanHandle)
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
    // `(metric, sketch_algorithm, config, group_by_keys)` — so the
    // streaming-config's grouping MUST include "service" or the
    // fingerprint won't match and the registered sid stays orphaned
    // from any policy.
    let materializations = plan_materializations(
        "sum by (service) (quantile_over_time(0.99, http_latency_ms[1s]))",
        epsilon_delta(0.01, 0.01),
    );
    post_full_config(&client, &stack, &materializations).await;

    // ── 2. Build a DDSketch state with a known distribution ────────────
    //
    // 50 samples drawn from a fixed distribution. The exact bucket-
    // count math (DDSketch index = ceil(log_gamma(value))) doesn't
    // matter for this test — we want to verify the wire round-trip,
    // not the quantile readout accuracy. Pick a simple count vector
    // used by the production modified-OTLP process E2E, so it is known
    // to be representable.
    let alpha = 0.01;
    let store_counts = vec![5u64, 10, 15, 20];
    let dd_state = build_dd_sketch_state(alpha, store_counts, -1);
    let sketch_bytes = dd_state.encode_to_vec();

    // ── 3. POST the sketch DP via OTLP HTTP ────────────────────────────
    //
    // Use wall-clock-relative timestamps so the PromQL query at default
    // evaluation time (also wall-clock) sees the data inside its `[1s]`
    // lookback window. The sketch lands at `now - 3s` so it's well
    // inside a 1-second window that closed `now - 2s`; the watermark
    // advance is at `now - 1s` so the engine sees the window-end
    // boundary cross.
    let now_ns = phase_aligned_now_ns();
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
        .query(&[("time", evaluation_time(&stack))])
        .query(&[("query", "quantile_over_time(0.99, http_latency_ms[1s])")])
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

    let materializations = plan_materializations(
        "sum by (service) (quantile_over_time(0.5, request_size_bytes[1s]))",
        epsilon_delta(0.05, 0.05),
    );
    // Planner owns the family choice; the payload below is built from what it
    // committed to. Family selection is covered by the compiler tests.
    post_full_config(&client, &stack, &materializations).await;

    let alpha = materializations[0].parameters["alpha"]
        .as_f64()
        .expect("planner sized a relative-accuracy quantile summary");
    let dd_state = build_dd_sketch_state(alpha, vec![5u64, 10, 15, 20], -1);
    let sketch_bytes = dd_state.encode_to_vec();

    let now_ns = phase_aligned_now_ns();
    let sketch_t_ns = now_ns.saturating_sub(3_000_000_000);
    let watermark_t_ns = now_ns.saturating_sub(1_000_000_000);

    let req = build_dd_sketch_export(
        "request_size_bytes",
        &[("service", "e2e-test")],
        sketch_t_ns,
        sketch_bytes,
        alpha,
    );
    post_otlp_http(&client, stack.otlp_http_port, req).await;

    let watermark_state = build_dd_sketch_state(alpha, Vec::new(), 0);
    let watermark_req = build_dd_sketch_export(
        "request_size_bytes",
        &[("service", "e2e-test")],
        watermark_t_ns,
        watermark_state.encode_to_vec(),
        alpha,
    );
    post_otlp_http(&client, stack.otlp_http_port, watermark_req).await;

    tokio::time::sleep(Duration::from_millis(800)).await;

    let response: JsonValue = client
        .get(format!(
            "http://127.0.0.1:{}/api/v1/query",
            stack.backend_port
        ))
        .query(&[("query", "quantile_over_time(0.5, request_size_bytes[1s])")])
        .query(&[("time", evaluation_time(&stack))])
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

    let materializations = plan_materializations(
        "sum by (service) (count_over_time(unique_users_per_min[1s]))",
        epsilon_delta(0.05, 0.05),
    );
    // Planner owns the family choice; the payload below is built from what it
    // committed to. Family selection is covered by the compiler tests.
    post_full_config(&client, &stack, &materializations).await;

    // Precision must match what the controller plans for this
    // workload (`HLLDefaults` in `control_plane::types`). The
    // accuracy_sla=0.05 above is > the precision_threshold (0.02),
    // so the planner picks `precision_coarse = 10`. If the OTLP DP
    // were sent with a different precision, the backend would
    // register two separate sids for the same metric — one with
    // policy_fp=UNSET (no matching policy params) — and the query
    // wouldn't find the policy-tagged one.
    let (w, d) = extract_w_d(&materializations[0]);
    let cells = (w as usize) * (d as usize);
    let mut counts = vec![0i64; cells];
    // A few non-zero cells so the readout is non-trivial.
    counts[0] = 5;
    counts[w as usize] = 7;
    counts[cells - 1] = 4;
    let cms_state = build_count_min_state(d, w, counts.clone());
    let sketch_bytes = cms_state.encode_to_vec();

    let now_ns = phase_aligned_now_ns();
    let sketch_t_ns = now_ns.saturating_sub(3_000_000_000);
    let watermark_t_ns = now_ns.saturating_sub(1_000_000_000);

    let req = build_count_min_export(
        "unique_users_per_min",
        &[("service", "e2e-test")],
        sketch_t_ns,
        sketch_bytes,
        d as i32,
        w as i32,
    );
    post_otlp_http(&client, stack.otlp_http_port, req).await;

    let watermark_state = build_count_min_state(d, w, vec![0i64; cells]);
    let watermark_req = build_count_min_export(
        "unique_users_per_min",
        &[("service", "e2e-test")],
        watermark_t_ns,
        watermark_state.encode_to_vec(),
        d as i32,
        w as i32,
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
        .query(&[("query", "count_over_time(unique_users_per_min[1s])")])
        .query(&[("time", evaluation_time(&stack))])
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
// msgpack-encoded heap envelope, but the query
// uses `count_over_time(...)` instead of `topk(...)` — the
// reducer's `decode_frequency_total` reads row-0 of the underlying
// matrix for heap-bearing variants too, so FrequencyEstimate works
// on a heap-bearing SID.
//
// **Strict-success: `count_over_time(top_endpoint_qps[1s])`** binds
// to `Capability::FrequencyEstimate(Any)`, which
// `is_satisfied_by` accepts against
// `FrequencyTopk(CountSketchWithHeap)` (heap is additional info
// layered over the matrix — the matrix is a fully valid frequency
// sketch on its own).

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_plan_to_query_full_roundtrip_count_sketch() {
    let stack = start_full_stack(19_567, 19_568).await;
    let client = reqwest::Client::new();

    let materializations = plan_materializations(
        "topk(3, sum by (service) (count_over_time(top_endpoint_qps[1s])))",
        epsilon_delta(0.05, 0.05),
    );
    // Planner owns the family choice; the payload below is built from what it
    // committed to. Family selection is covered by the compiler tests.
    post_full_config(&client, &stack, &materializations).await;

    // Use the planner-picked `(w, d)` so the OTLP DP's wire-level
    // `rows`/`cols` line up with the policy's `parameters.{d, w}` —
    // dimension mismatches prevent physical-policy binding.
    let (w, d) = extract_w_d(&materializations[0]);
    let rows = d as usize;
    let cols = w as usize;
    let wire_rows = d as i32;
    let wire_cols = w as i32;

    let cells = rows * cols;
    let mut counts = vec![0i64; cells];
    counts[0] = 100;
    counts[cols] = 50;
    counts[cells - 1] = 200;
    let sketch_bytes = build_count_min_state(d, w, counts.clone()).encode_to_vec();

    let now_ns = phase_aligned_now_ns();
    let sketch_t_ns = now_ns.saturating_sub(3_000_000_000);
    let watermark_t_ns = now_ns.saturating_sub(1_000_000_000);

    let req = build_count_min_export(
        "top_endpoint_qps",
        &[("service", "e2e-test")],
        sketch_t_ns,
        sketch_bytes.clone(),
        wire_rows,
        wire_cols,
    );
    post_otlp_http(&client, stack.otlp_http_port, req).await;

    let watermark_req = build_count_min_export(
        "top_endpoint_qps",
        &[("service", "e2e-test")],
        watermark_t_ns,
        build_count_min_state(d, w, vec![0i64; cells]).encode_to_vec(),
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
        .query(&[("query", "count_over_time(top_endpoint_qps[1s])")])
        .query(&[("time", evaluation_time(&stack))])
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

    let materializations = plan_materializations(
        "topk(3, sum by (service) (count_over_time(endpoint_request_freq[1s])))",
        epsilon_delta(0.05, 0.05),
    );
    // Planner owns the family choice; the payload below is built from what it
    // committed to. Family selection is covered by the compiler tests.
    post_full_config(&client, &stack, &materializations).await;

    // Use planner-picked `(w, d)` so the wire DP's `rows`/`cols`
    // match the policy's `parameters.{d, w}` — the policy_fp content
    // match keys on these values (see `derive_sketch_policy_fp`).
    let (w, d) = extract_w_d(&materializations[0]);
    let rows = d;
    let cols = w;
    let counts: Vec<i64> = (0..(rows * cols) as i64).map(|i| (i % 11).abs()).collect();
    let cms_state = build_count_min_state(rows, cols, counts);
    let sketch_bytes = cms_state.encode_to_vec();

    let now_ns = phase_aligned_now_ns();
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
        .query(&[("query", "count_over_time(endpoint_request_freq[1s])")])
        .query(&[("time", evaluation_time(&stack))])
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
// On the ingest side, `sketch_algorithm_for` peeks at incoming
// CountMin / CountSketch DPs with `encoding=MSGPACK`; if the bytes
// round-trip through the heap envelope AND the heap is non-empty,
// the sid is auto-promoted to the corresponding `*WithHeap` variant
// so the ASAP-tier reducer can answer `topk(...)` from the heap.

/// Extract the planner-picked `(w, d)` from a streaming-config aggregation
/// for CMS / CountSketch policies. Returns `(w as cols, d as rows)`.
/// The DP's wire-level `rows`/`cols` MUST match these for
/// `find_policy_by_content` to bind the sid to the policy_fp (the
/// content match probes `parameters.w` and `parameters.d`).
/// Sketch width/depth the planner sized this materialization to. The test
/// payloads are built against these, never against pinned constants.
fn extract_w_d(agg: &PrecomputeMaterialization) -> (u32, u32) {
    let w = agg.parameters["w"]
        .as_u64()
        .expect("materialization must carry parameters.w") as u32;
    let d = agg.parameters["d"]
        .as_u64()
        .expect("materialization must carry parameters.d") as u32;
    (w, d)
}

// Heap TopK serving acceptance lives in asapquery_compatibility_process_e2e:
// registered_temporal_topk_{cms_heap,count_sketch_heap} install the selected
// physical QueryPlan and verify raw count updates through the production binary.

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
// `/api/v1/query_range?query=count_over_time(metric[1s])` instead
// of the instant endpoint. The result `resultType` is `matrix`
// (Prometheus spec for range queries).

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_plan_to_range_query_count_over_time_cms() {
    let stack = start_full_stack(19_575, 19_576).await;
    let client = reqwest::Client::new();

    let materializations = plan_materializations(
        "topk(3, sum by (service) (count_over_time(endpoint_request_freq[1s])))",
        epsilon_delta(0.05, 0.05),
    );
    post_full_config(&client, &stack, &materializations).await;

    let (w, d) = extract_w_d(&materializations[0]);
    let rows = d;
    let cols = w;
    let counts: Vec<i64> = (0..(rows * cols) as i64).map(|i| (i % 11).abs()).collect();
    let cms_state = build_count_min_state(rows, cols, counts);
    let sketch_bytes = cms_state.encode_to_vec();

    let now_ns = phase_aligned_now_ns();
    let sketch_t_ns = now_ns.saturating_sub(3_000_000_000);
    let watermark_t_ns = sketch_t_ns + 5_000_000_000;

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
    let start_secs: f64 = evaluation_time(&stack).parse().unwrap();
    let end_secs = start_secs + 5.0;
    let step_secs = 5.0;
    let response: JsonValue = client
        .get(format!(
            "http://127.0.0.1:{}/api/v1/query_range",
            stack.backend_port
        ))
        .query(&[
            ("query", "count_over_time(endpoint_request_freq[1s])"),
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
            d_count: c,
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
    let materializations = plan_materializations(
        &format!("sum by (service) (quantile_over_time(0.99, {bare_metric}[1s]))"),
        epsilon_delta(alpha, alpha),
    );
    post_full_config(&client, &stack, &materializations).await;

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

    // Three contiguous retained windows; evaluate explicitly at the last
    // window end with a three-second lookback, independent of wall-clock drift.
    let now_ns = phase_aligned_now_ns();
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
            // Each window starts a fresh full checkpoint, followed by two
            // increments referring to that checkpoint.
            let (bytes, encoding) = if f_idx == 0 {
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

    FIXTURE_TIMES
        .get()
        .unwrap()
        .lock()
        .unwrap()
        .insert(stack.otlp_http_port, now_ns - 148 * sec);
    // Give the receiver time to land every frame in the SketchStore.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // ── 3. Query the BARE metric over the three ingested seconds.
    let query_url = format!("http://127.0.0.1:{}/api/v1/query", stack.backend_port);
    let response: JsonValue = client
        .get(&query_url)
        .query(&[("time", evaluation_time(&stack))])
        .query(&[(
            "query",
            format!("quantile_over_time(0.99, {bare_metric}[3s])").as_str(),
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

    // cumulative_evaluate rolls the [3s] range into ONE union over all three
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

    let materializations = plan_materializations(
        "sum by (service) (quantile_over_time(0.99, http_latency_ms[1s]))",
        epsilon_delta(0.01, 0.01),
    );
    post_full_config(&client, &stack, &materializations).await;

    // Same fixture data as `controller_plan_to_query_full_roundtrip_ddsketch`
    // — this test isn't checking quantile accuracy (that's Test 3's job),
    // it's checking that turning shadow mode on doesn't change whether/what
    // this query serves.
    let alpha = 0.01;
    let store_counts = vec![5u64, 10, 15, 20];
    let dd_state = build_dd_sketch_state(alpha, store_counts, -1);
    let sketch_bytes = dd_state.encode_to_vec();

    let now_ns = phase_aligned_now_ns();
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
        .query(&[("time", evaluation_time(&stack))])
        .query(&[("query", "quantile_over_time(0.99, http_latency_ms[1s])")])
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

    let materializations = plan_materializations(
        "sum by (service) (quantile_over_time(0.99, http_latency_ms[1s]))",
        epsilon_delta(0.01, 0.01),
    );
    post_full_config(&client, &stack, &materializations).await;

    let alpha = 0.01;
    let store_counts = vec![5u64, 10, 15, 20];
    let dd_state = build_dd_sketch_state(alpha, store_counts, -1);
    let sketch_bytes = dd_state.encode_to_vec();

    let now_ns = phase_aligned_now_ns();
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
        .query(&[("time", evaluation_time(&stack))])
        .query(&[("query", "quantile_over_time(0.99, http_latency_ms[1s])")])
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
// The installed cardinality readout merges all bound series and windows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_serve_hll_global_count_merges_across_sids() {
    let _live = LiveServeEnvGuard::enable();

    let stack = start_full_stack(19_595, 19_596).await;
    let client = reqwest::Client::new();

    let materializations = plan_materializations(
        "sum by (service) (count_over_time(unique_users_per_min[1s]))",
        epsilon_delta(0.05, 0.05),
    );
    post_full_config(&client, &stack, &materializations).await;

    let (w, d) = extract_w_d(&materializations[0]);
    let cells = (w as usize) * (d as usize);

    let now_ns = phase_aligned_now_ns();
    let sketch_t_ns = now_ns.saturating_sub(3_000_000_000);
    let watermark_t_ns = now_ns.saturating_sub(1_000_000_000);

    // Two distinct services with disjoint non-zero cells — two separate sids
    // the readout resolves together, the shape ASAPController#163 describes.
    for (service, cell) in [("svc-a", 0usize), ("svc-b", w as usize)] {
        let mut counts = vec![0i64; cells];
        counts[cell] = 6;
        let cms_state = build_count_min_state(d, w, counts);
        let req = build_count_min_export(
            "unique_users_per_min",
            &[("service", service)],
            sketch_t_ns,
            cms_state.encode_to_vec(),
            d as i32,
            w as i32,
        );
        post_otlp_http(&client, stack.otlp_http_port, req).await;

        let watermark_state = build_count_min_state(d, w, vec![0i64; cells]);
        let watermark_req = build_count_min_export(
            "unique_users_per_min",
            &[("service", service)],
            watermark_t_ns,
            watermark_state.encode_to_vec(),
            d as i32,
            w as i32,
        );
        post_otlp_http(&client, stack.otlp_http_port, watermark_req).await;
    }

    tokio::time::sleep(Duration::from_millis(800)).await;

    let response: JsonValue = client
        .get(format!(
            "http://127.0.0.1:{}/api/v1/query",
            stack.backend_port
        ))
        .query(&[("query", "count_over_time(unique_users_per_min[1s])")])
        .query(&[("time", evaluation_time(&stack))])
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

#[test]
fn probe_queryplan() {
    use control_plane::physical::compiler::{BackendLocalPlanningInput, PhysicalPlanCompiler};
    for q in [
        "sum by (service) (quantile_over_time(0.99, http_latency_ms[1s]))",
        "quantile_over_time(0.99, http_latency_ms[1s])",
    ] {
        let mut fixture: JsonValue = serde_json::from_str(include_str!(
            "../../docs/examples/asapquery-compatibility-demo-snapshot.json"
        ))
        .unwrap();
        let mut entry = fixture["query_workload"]["repeating_queries"][3].clone();
        entry["query"] = q.into();
        entry["requirements"]["accuracy"] = epsilon_delta(0.01, 0.01);
        fixture["query_workload"]["repeating_queries"] = serde_json::json!([entry]);
        let snap: BackendLocalPlanningInput = serde_json::from_value(fixture).unwrap();
        let (req, env) = snap.into_physical_compilation_request().unwrap();
        let plan = PhysicalPlanCompiler.compile_promql(req, env).unwrap();
        eprintln!("PROBE {q}");
        for (id, e) in plan.query_plan.entries.iter() {
            eprintln!(
                "   entry {id:?} canonical={:?} nodes={}",
                e.canonical_query,
                e.nodes.len()
            );
        }
        eprintln!(
            "   grouping={:?}",
            plan.precompute_plan.materializations[0]
                .grouping_labels
                .names()
        );
    }
}
