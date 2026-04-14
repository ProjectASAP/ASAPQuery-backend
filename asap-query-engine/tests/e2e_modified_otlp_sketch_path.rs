//! End-to-end integration test for the modified-OTLP sketch hot path.
//!
//! Scope: the **CountMinSketch** path that landed in PR B. Other sketch
//! types (KLL / DDSketch / CountSketch / HLL) get e2e coverage as PR C
//! delivers each per-type decoder.
//!
//! What this test exercises:
//!   1. `OtlpReceiver::with_ingest_state` wired to a running
//!      `PrecomputeEngine`
//!   2. A real `ExportMetricsServiceRequest` carrying
//!      `Metric.data = CountMinSketch{…}` typed sketch data points,
//!      sent over OTLP HTTP at `/v1/metrics`
//!   3. `route_modified_otlp_sketches_to_precompute` decoding the typed
//!      `sketch` bytes via
//!      `CountMinSketchAccumulator::from_sketchlib_proto_bytes`
//!   4. The precompute engine's `sketch_panes` merging the incoming
//!      accumulator into the matching `(agg_id, group_key)` window
//!   5. Window close emitting to a `CapturingOutputSink`
//!   6. The captured `CountMinSketchAccumulator` matrix contents
//!      matching what was sent, confirming that PR A vendoring +
//!      PR B routing + PR B per-variant decoder all work end-to-end
//!
//! This is the correctness anchor for Phase 1's hot path. See
//! `docs/pipeline-query-catalog.md` §5.4 in the DataCollector repo for
//! the full architectural context.

use asap_otel_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use asap_otel_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use asap_otel_proto::tonic::metrics::v1::{
    metric::Data, CountMinSketch, CountMinSketchDataPoint, CountMinSketchEncoding, CountSketch,
    CountSketchDataPoint, CountSketchEncoding, Metric, ResourceMetrics, ScopeMetrics,
};
use asap_sketchlib::proto::sketchlib::{CountMinState, CountSketchState, CounterType};
use asap_types::aggregation_config::AggregationConfig;
use asap_types::enums::{AggregationType, WindowType};
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;

use query_engine_rust::data_model::StreamingConfig;
use query_engine_rust::drivers::ingest::{OtlpReceiver, OtlpReceiverConfig};
use query_engine_rust::precompute_engine::config::{LateDataPolicy, PrecomputeEngineConfig};
use query_engine_rust::precompute_engine::output_sink::CapturingOutputSink;
use query_engine_rust::precompute_engine::PrecomputeEngine;
use query_engine_rust::precompute_operators::{CountMinSketchAccumulator, CountSketchAccumulator};

/// Build a tumbling-window `CountMinSketch` aggregation for one metric,
/// grouped by a single label. Mirrors the helper in
/// `tests/e2e_precompute_equivalence.rs` but for CountMinSketch.
fn make_count_min_agg_config(
    id: u64,
    metric: &str,
    window_secs: u64,
    grouping: Vec<&str>,
    rows: usize,
    cols: usize,
) -> AggregationConfig {
    let mut params = HashMap::new();
    params.insert("row_num".to_string(), serde_json::Value::from(rows as u64));
    params.insert("col_num".to_string(), serde_json::Value::from(cols as u64));
    AggregationConfig::new(
        id,
        AggregationType::CountMinSketch,
        String::new(),
        params,
        promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(
            grouping.iter().map(|s| s.to_string()).collect(),
        ),
        promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
        promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
        String::new(),
        window_secs,
        0,
        WindowType::Tumbling,
        metric.to_string(),
        metric.to_string(),
        None,
        None,
        None,
        None,
    )
}

/// Engine config with a fast flush interval so the test does not have to
/// wait long after the watermark advances.
fn engine_config(precompute_port: u16) -> PrecomputeEngineConfig {
    PrecomputeEngineConfig {
        num_workers: 2,
        ingest_port: precompute_port,
        allowed_lateness_ms: 0,
        max_buffer_per_series: 10_000,
        flush_interval_ms: 100,
        channel_buffer_size: 10_000,
        pass_raw_samples: false,
        raw_mode_aggregation_id: 0,
        late_data_policy: LateDataPolicy::Drop,
    }
}

/// Build a `CountMinState` proto from a known matrix in row-major order.
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

/// Build an `ExportMetricsServiceRequest` carrying a single
/// `Metric.data = CountMinSketch{…}` payload with the given sketch bytes,
/// timestamped at `time_unix_nano` and labeled with `service`.
fn build_export_request(
    metric_name: &str,
    service_label: &str,
    time_unix_nano: u64,
    sketch_bytes: Vec<u8>,
) -> ExportMetricsServiceRequest {
    let dp = CountMinSketchDataPoint {
        attributes: vec![KeyValue {
            key: "service".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(service_label.to_string())),
            }),
        }],
        start_time_unix_nano: 0,
        time_unix_nano,
        sample_count: 0,
        sketch: sketch_bytes,
        encoding: CountMinSketchEncoding::Proto as i32,
        rows: 0,
        cols: 0,
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
                    })),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// POST a protobuf-encoded `ExportMetricsServiceRequest` to the OTLP HTTP
/// endpoint at `localhost:port/v1/metrics`. Returns `()` on success or
/// panics with the unexpected status code.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_count_min_sketch_modified_otlp_path() {
    // ─── 1. Topology ────────────────────────────────────────────────────
    let agg_id = 42u64;
    let metric_name = "http_requests_total";
    let service_label = "auth";
    let window_secs = 1u64;
    let rows = 2u32;
    let cols = 4u32;

    let precompute_port = 19500u16;
    let otlp_grpc_port = 19501u16;
    let otlp_http_port = 19502u16;

    let cms_config = make_count_min_agg_config(
        agg_id,
        metric_name,
        window_secs,
        vec!["service"],
        rows as usize,
        cols as usize,
    );
    let mut agg_map = HashMap::new();
    agg_map.insert(agg_id, cms_config);
    let streaming_config = Arc::new(StreamingConfig::new(agg_map));

    let sink = Arc::new(CapturingOutputSink::new());
    let engine = PrecomputeEngine::new(
        engine_config(precompute_port),
        streaming_config,
        sink.clone(),
    );
    let ingest_state = engine.ingest_state();

    // Spawn the precompute engine (spawns workers + Prometheus remote-write
    // ingest server; we only use the workers/router from it).
    tokio::spawn(async move {
        let _ = engine.run().await;
    });

    // Spawn the OTLP receiver wired to the engine's ingest state.
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

    // Wait for both the engine and the OTLP HTTP server to bind.
    tokio::time::sleep(tokio::time::Duration::from_millis(400)).await;

    // ─── 2. Send a sketch payload through OTLP ──────────────────────────
    // Known matrix in row-major order:
    //   row 0: [1, 2, 3, 4]
    //   row 1: [5, 6, 7, 8]
    let counts_int: Vec<i64> = vec![1, 2, 3, 4, 5, 6, 7, 8];
    let cms_state = build_count_min_state(rows, cols, counts_int.clone());
    let sketch_bytes = cms_state.encode_to_vec();

    let client = reqwest::Client::new();

    // First send: timestamped at the start of window 0 (ts = 100ms).
    let req = build_export_request(metric_name, service_label, 100_000_000, sketch_bytes);
    post_otlp_http(&client, otlp_http_port, req).await;

    // Second send: timestamped past the window end (ts > window_secs * 1_000ms)
    // so the precompute engine's watermark advances and closes window 0.
    // Use an empty 2x4 matrix so the closing point doesn't disturb anything
    // structurally.
    let zero_state = build_count_min_state(rows, cols, vec![0i64; (rows * cols) as usize]);
    let zero_bytes = zero_state.encode_to_vec();
    let watermark_advance_req = build_export_request(
        metric_name,
        service_label,
        2_000_000_000, // 2 s past epoch — past the 1 s window end
        zero_bytes,
    );
    post_otlp_http(&client, otlp_http_port, watermark_advance_req).await;

    // Wait long enough for the periodic flush to fire and the worker to
    // emit the closed window to the sink.
    tokio::time::sleep(tokio::time::Duration::from_millis(800)).await;

    // ─── 3. Drain and assert ────────────────────────────────────────────
    let captured = sink.drain();
    assert!(
        !captured.is_empty(),
        "expected at least one closed window output, got 0"
    );

    // Find the entry corresponding to window 0 (start_timestamp == 0).
    // The watermark-advance request also occupies a later window which the
    // engine may or may not have emitted yet; we only care about window 0.
    let (window0_output, window0_acc_box) = captured
        .iter()
        .find(|(out, _)| out.start_timestamp == 0)
        .expect("no captured output for window 0");

    assert_eq!(window0_output.aggregation_id, agg_id);
    assert_eq!(window0_output.end_timestamp, window_secs * 1_000);

    let window0_acc = window0_acc_box
        .as_any()
        .downcast_ref::<CountMinSketchAccumulator>()
        .expect("captured accumulator should be CountMinSketchAccumulator");

    let stored_matrix = window0_acc.inner.sketch();
    assert_eq!(stored_matrix.len(), rows as usize, "row count mismatch");
    for r in 0..rows as usize {
        let expected: Vec<f64> = counts_int[r * cols as usize..(r + 1) * cols as usize]
            .iter()
            .map(|&v| v as f64)
            .collect();
        assert_eq!(
            stored_matrix[r], expected,
            "matrix row {r} mismatch (expected {expected:?}, got {:?})",
            stored_matrix[r]
        );
    }
}

// ─── CountSketch path ────────────────────────────────────────────────────

/// Parallel to `make_count_min_agg_config` but for `CountSketch`. Uses
/// `AggregationType::CountSketch` added in PR C-CountSketch.
fn make_count_sketch_agg_config(
    id: u64,
    metric: &str,
    window_secs: u64,
    grouping: Vec<&str>,
    rows: usize,
    cols: usize,
) -> AggregationConfig {
    let mut params = HashMap::new();
    params.insert("row_num".to_string(), serde_json::Value::from(rows as u64));
    params.insert("col_num".to_string(), serde_json::Value::from(cols as u64));
    AggregationConfig::new(
        id,
        AggregationType::CountSketch,
        String::new(),
        params,
        promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(
            grouping.iter().map(|s| s.to_string()).collect(),
        ),
        promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
        promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
        String::new(),
        window_secs,
        0,
        WindowType::Tumbling,
        metric.to_string(),
        metric.to_string(),
        None,
        None,
        None,
        None,
    )
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

/// Build an `ExportMetricsServiceRequest` carrying a single
/// `Metric.data = CountSketch{…}` payload with the given sketch bytes,
/// timestamped at `time_unix_nano` and labeled with `service`.
fn build_count_sketch_export_request(
    metric_name: &str,
    service_label: &str,
    time_unix_nano: u64,
    sketch_bytes: Vec<u8>,
) -> ExportMetricsServiceRequest {
    let dp = CountSketchDataPoint {
        attributes: vec![KeyValue {
            key: "service".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(service_label.to_string())),
            }),
        }],
        start_time_unix_nano: 0,
        time_unix_nano,
        sketch: sketch_bytes,
        encoding: CountSketchEncoding::Proto as i32,
        dimension: String::new(),
        epsilon: 0.0,
        delta: 0.0,
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
                    })),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_count_sketch_modified_otlp_path() {
    // Same topology as the CountMin test, different ports, different
    // aggregation type, and signed counters.
    let agg_id = 43u64;
    let metric_name = "request_events_total";
    let service_label = "checkout";
    let window_secs = 1u64;
    let rows = 2u32;
    let cols = 4u32;

    let precompute_port = 19510u16;
    let otlp_grpc_port = 19511u16;
    let otlp_http_port = 19512u16;

    let cs_config = make_count_sketch_agg_config(
        agg_id,
        metric_name,
        window_secs,
        vec!["service"],
        rows as usize,
        cols as usize,
    );
    let mut agg_map = HashMap::new();
    agg_map.insert(agg_id, cs_config);
    let streaming_config = Arc::new(StreamingConfig::new(agg_map));

    let sink = Arc::new(CapturingOutputSink::new());
    let engine = PrecomputeEngine::new(
        engine_config(precompute_port),
        streaming_config,
        sink.clone(),
    );
    let ingest_state = engine.ingest_state();

    tokio::spawn(async move {
        let _ = engine.run().await;
    });

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

    tokio::time::sleep(tokio::time::Duration::from_millis(400)).await;

    // Signed counts — the whole point of Count Sketch is ±1 increments,
    // so the matrix contains negative values to distinguish it from
    // CountMin.
    //   row 0: [ 1, -2,  3, -4]
    //   row 1: [-5,  6, -7,  8]
    let counts_int: Vec<i64> = vec![1, -2, 3, -4, -5, 6, -7, 8];
    let cs_state = build_count_sketch_state(rows, cols, counts_int.clone());
    let sketch_bytes = cs_state.encode_to_vec();

    let client = reqwest::Client::new();

    // First send: window 0 payload with the known matrix.
    let req =
        build_count_sketch_export_request(metric_name, service_label, 100_000_000, sketch_bytes);
    post_otlp_http(&client, otlp_http_port, req).await;

    // Second send: watermark advance past window end.
    let zero_state = build_count_sketch_state(rows, cols, vec![0i64; (rows * cols) as usize]);
    let watermark_advance_req = build_count_sketch_export_request(
        metric_name,
        service_label,
        2_000_000_000,
        zero_state.encode_to_vec(),
    );
    post_otlp_http(&client, otlp_http_port, watermark_advance_req).await;

    tokio::time::sleep(tokio::time::Duration::from_millis(800)).await;

    let captured = sink.drain();
    assert!(
        !captured.is_empty(),
        "expected at least one closed window output, got 0"
    );

    let (window0_output, window0_acc_box) = captured
        .iter()
        .find(|(out, _)| out.start_timestamp == 0)
        .expect("no captured output for window 0");

    assert_eq!(window0_output.aggregation_id, agg_id);
    assert_eq!(window0_output.end_timestamp, window_secs * 1_000);

    let window0_acc = window0_acc_box
        .as_any()
        .downcast_ref::<CountSketchAccumulator>()
        .expect("captured accumulator should be CountSketchAccumulator");

    let stored_matrix = window0_acc.inner.sketch();
    assert_eq!(stored_matrix.len(), rows as usize, "row count mismatch");
    for r in 0..rows as usize {
        let expected: Vec<f64> = counts_int[r * cols as usize..(r + 1) * cols as usize]
            .iter()
            .map(|&v| v as f64)
            .collect();
        assert_eq!(
            stored_matrix[r], expected,
            "matrix row {r} mismatch (expected {expected:?}, got {:?})",
            stored_matrix[r]
        );
    }
}
