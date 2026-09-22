//! Production-process E2E oracle matrix for every supported sketch family.
//!
//! Each test starts the real `data_plane` binary, installs a streaming policy,
//! posts modified OTLP over HTTP, and queries the public Prometheus endpoint.
//! Sketch implementations are used only to encode raw fixtures. Expected
//! answers are independently computed from those raw fixtures.

use std::collections::{HashMap, HashSet};
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use asap_otel_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use asap_otel_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use asap_otel_proto::tonic::metrics::v1::{
    metric::Data, CountMinSketch as OtelCountMinSketch, CountMinSketchDataPoint,
    CountMinSketchEncoding, CountSketch as OtelCountSketch, CountSketchDataPoint,
    CountSketchEncoding, HllSketch as OtelHllSketch, HllSketchDataPoint, HllSketchEncoding,
    KllSketch as OtelKllSketch, KllSketchDataPoint, KllSketchEncoding, Metric, ResourceMetrics,
    ScopeMetrics,
};
use asap_sketchlib::proto::sketchlib::{HllVariant as ProtoHllVariant, HyperLogLogState, KllState};
use asap_sketchlib::{CountMinSketch, CountSketch, HllSketch, HllVariant, MessagePackCodec};
use prost::Message;
use serde_json::Value;

const SERVICE: &str = "oracle-e2e";
#[path = "support/physical_fixture.rs"]
mod physical_fixture;
// These parameters satisfy the production query path's LIVE_ACCURACY
// (`epsilon = 0.01`). ASAPPlanner validates an observed `SketchKind`
// against that accuracy before committing it, so the fixture must use the
// same contract as a real control-plane-generated materialization.
const K: u32 = 269;
const HLL_PRECISION: u32 = 14;
const CMS_ROWS: usize = 5;
const CMS_COLS: usize = 2048;
// ASAPPlanner's CountSketch guarantee is L2-based: epsilon=sqrt(3/width),
// with an odd Hoeffding-median depth. The current runtime's packed sign hash
// limits this width to five rows. The fixture installs these parameters explicitly.
const COUNT_SKETCH_ROWS: usize = 5;
const COUNT_SKETCH_COLS: usize = 32_768;

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct Backend {
    evaluation_ms: std::sync::atomic::AtomicU64,
    artifact: data_plane::drivers::query::servers::http::PhysicalPlanInstallRequest,
    _child: ChildGuard,
    client: reqwest::Client,
    query_base: String,
    otlp_url: String,
    _config: tempfile::NamedTempFile,
    _output_dir: tempfile::TempDir,
}

fn unused_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("reserve loopback port")
        .local_addr()
        .expect("read loopback address")
        .port()
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_secs()
        * 1_000_000_000
}

fn labels() -> Vec<KeyValue> {
    vec![KeyValue {
        key: "service".into(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(SERVICE.into())),
        }),
    }]
}

fn envelope(metric: &str, data: Data) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: metric.into(),
                    description: String::new(),
                    unit: String::new(),
                    metadata: Vec::new(),
                    data: Some(data),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

async fn start_backend(materialization: &asap_types::PrecomputeMaterialization) -> Backend {
    let query_port = unused_port();
    let otlp_http_port = unused_port();
    let otlp_grpc_port = unused_port();
    let output_dir = tempfile::tempdir().expect("create data-plane output directory");
    let mut config = tempfile::NamedTempFile::new().expect("create streaming config");
    let mut physical = tempfile::NamedTempFile::new().unwrap();
    let mut install =
        physical_fixture::artifact_from_materializations(vec![materialization.clone()]);
    let runtime = data_plane::storage_engines::types::StreamingConfig::from_precompute_plan(
        install.precompute_plan.clone(),
    )
    .unwrap();
    serde_yaml::to_writer(&mut config, &runtime).unwrap();
    for rule in &mut install.transmission_plan.rules {
        if matches!(
            install
                .precompute_plan
                .schemas
                .iter()
                .find(|s| s.materialization == rule.materialization)
                .unwrap()
                .family,
            control_plane::physical::compiler::StateFamilyContract::Sketch {
                algorithm: planner_types::post_asap::SketchAlgorithm::Cms
                    | planner_types::post_asap::SketchAlgorithm::CountSketch,
                ..
            }
        ) {
            rule.encoding = control_plane::physical::compiler::StateEncoding::SketchCoreMsgpackV1;
        }
    }
    serde_json::to_writer(&mut physical, &install).unwrap();

    let mut command = Command::new(env!("CARGO_BIN_EXE_data_plane"));
    command
        .arg("--streaming-config")
        .arg(config.path())
        .arg("--physical-plan")
        .arg(physical.path())
        .arg("--http-port")
        .arg(query_port.to_string())
        .arg("--output-dir")
        .arg(output_dir.path())
        .arg("--enable-otel-ingest")
        .arg("--otel-http-port")
        .arg(otlp_http_port.to_string())
        .arg("--otel-grpc-port")
        .arg(otlp_grpc_port.to_string())
        .arg("--precompute-allowed-lateness-ms")
        .arg("0")
        .arg("--precompute-flush-interval-ms")
        .arg("100")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let child = command.spawn().expect("start production data-plane binary");
    let mut child = ChildGuard(child);
    let client = reqwest::Client::new();
    let query_base = format!("http://127.0.0.1:{query_port}");
    let health = format!("{query_base}/api/v1/health");
    for _ in 0..100 {
        if let Some(status) = child.0.try_wait().expect("inspect data-plane process") {
            panic!("data-plane exited before readiness: {status}");
        }
        if client
            .get(&health)
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return Backend {
                artifact: install,
                evaluation_ms: std::sync::atomic::AtomicU64::new(0),
                _child: child,
                client,
                query_base,
                otlp_url: format!("http://127.0.0.1:{otlp_http_port}/v1/metrics"),
                _config: config,
                _output_dir: output_dir,
            };
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("data-plane did not become ready at {health}");
}

async fn post(backend: &Backend, mut request: ExportMetricsServiceRequest) {
    let data = request.resource_metrics[0].scope_metrics[0].metrics[0]
        .data
        .as_ref()
        .unwrap();
    let ns = match data {
        Data::Kllsketch(s) => s.data_points[0].time_unix_nano,
        Data::Hllsketch(s) => s.data_points[0].time_unix_nano,
        Data::Countminsketch(s) => s.data_points[0].time_unix_nano,
        Data::Countsketch(s) => s.data_points[0].time_unix_nano,
        _ => unreachable!(),
    };
    backend
        .evaluation_ms
        .store(ns / 1_000_000, std::sync::atomic::Ordering::Relaxed);
    physical_fixture::stamp(&mut request, &backend.artifact);
    let response = backend
        .client
        .post(&backend.otlp_url)
        .header("content-type", "application/x-protobuf")
        .body(request.encode_to_vec())
        .send()
        .await
        .expect("post modified OTLP");
    let status = response.status();
    let body = response.text().await.unwrap();
    assert!(status.is_success(), "OTLP {status}: {body}");
}

async fn query(backend: &Backend, promql: &str) -> Value {
    let mut response = Value::Null;
    for _ in 0..50 {
        response = backend
            .client
            .get(format!("{}/api/v1/query", backend.query_base))
            .query(&[("query", promql)])
            .query(&[(
                "time",
                (backend
                    .evaluation_ms
                    .load(std::sync::atomic::Ordering::Relaxed) as f64
                    / 1000.0)
                    .to_string(),
            )])
            .send()
            .await
            .expect("query production backend")
            .json()
            .await
            .expect("decode Prometheus JSON");
        if response["data"]["result"]
            .as_array()
            .is_some_and(|result| !result.is_empty())
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        response["infos"]
            .as_array()
            .is_some_and(|infos| infos.iter().any(|info| info
                .as_str()
                .is_some_and(|text| text.contains("data_source: asap_query")))),
        "query was not proven to execute in ASAPQuery: {response}"
    );
    assert_eq!(response["status"], "success", "PromQL response: {response}");
    response
}

fn scalar_values(response: &Value) -> Vec<(HashMap<String, String>, f64)> {
    response["data"]["result"]
        .as_array()
        .expect("Prometheus result array")
        .iter()
        .map(|series| {
            let labels = series["metric"]
                .as_object()
                .expect("metric labels")
                .iter()
                .map(|(key, value)| {
                    (
                        key.clone(),
                        value.as_str().expect("string label").to_string(),
                    )
                })
                .collect();
            let value = series["value"][1]
                .as_str()
                .expect("string sample value")
                .parse()
                .expect("numeric sample value");
            (labels, value)
        })
        .collect()
}

fn config(metric: &str, kind: &str, parameters: &str) -> asap_types::PrecomputeMaterialization {
    physical_fixture::materialization(
        metric,
        kind.parse().unwrap(),
        serde_yaml::from_str(parameters).unwrap(),
    )
}

fn kll_export(metric: &str, timestamp_ns: u64, raw: &[f64]) -> ExportMetricsServiceRequest {
    let state = KllState {
        k: K,
        m: 8,
        num_levels: 0,
        levels: Vec::new(),
        items: raw.to_vec(),
        coin: None,
        offset: 0.0,
        value_scale: 0,
        residuals: Vec::new(),
    };
    envelope(
        metric,
        Data::Kllsketch(OtelKllSketch {
            data_points: vec![KllSketchDataPoint {
                attributes: labels(),
                start_time_unix_nano: timestamp_ns.saturating_sub(1_000_000_000),
                time_unix_nano: timestamp_ns,
                sketch: state.encode_to_vec(),
                encoding: KllSketchEncoding::Proto as i32,
                flags: 0,
                series_id: 0,
            }],
            aggregation_temporality: 0,
            k: K,
        }),
    )
}

fn hll_export(metric: &str, timestamp_ns: u64, raw: &[&str]) -> ExportMetricsServiceRequest {
    let mut sketch = HllSketch::new(HllVariant::Regular, HLL_PRECISION);
    for value in raw {
        sketch.update(value.as_bytes());
    }
    let state = HyperLogLogState {
        variant: ProtoHllVariant::Regular as i32,
        precision: HLL_PRECISION,
        registers: sketch.registers,
        hip_kxq0: 0.0,
        hip_kxq1: 0.0,
        hip_est: 0.0,
        registers_sparse: None,
    };
    envelope(
        metric,
        Data::Hllsketch(OtelHllSketch {
            data_points: vec![HllSketchDataPoint {
                attributes: labels(),
                start_time_unix_nano: timestamp_ns.saturating_sub(1_000_000_000),
                time_unix_nano: timestamp_ns,
                sketch: state.encode_to_vec(),
                encoding: HllSketchEncoding::Proto as i32,
                flags: 0,
                series_id: 0,
            }],
            aggregation_temporality: 0,
            precision: HLL_PRECISION,
        }),
    )
}

fn cms_export(metric: &str, timestamp_ns: u64, raw: &[&str]) -> ExportMetricsServiceRequest {
    let mut sketch = CountMinSketch::new(CMS_ROWS, CMS_COLS);
    for key in raw {
        sketch.update(key, 1.0);
    }
    envelope(
        metric,
        Data::Countminsketch(OtelCountMinSketch {
            data_points: vec![CountMinSketchDataPoint {
                attributes: labels(),
                start_time_unix_nano: timestamp_ns.saturating_sub(1_000_000_000),
                time_unix_nano: timestamp_ns,
                sketch: sketch.to_msgpack().expect("encode CMS-with-heap"),
                encoding: CountMinSketchEncoding::Msgpack as i32,
                flags: 0,
                series_id: 0,
            }],
            aggregation_temporality: 0,
            rows: CMS_ROWS as i32,
            cols: CMS_COLS as i32,
        }),
    )
}

fn count_sketch_export(
    metric: &str,
    timestamp_ns: u64,
    raw: &[&str],
) -> ExportMetricsServiceRequest {
    let mut sketch = CountSketch::new(COUNT_SKETCH_ROWS, COUNT_SKETCH_COLS);
    for key in raw {
        sketch.update(key, 1.0);
    }
    envelope(
        metric,
        Data::Countsketch(OtelCountSketch {
            data_points: vec![CountSketchDataPoint {
                attributes: labels(),
                start_time_unix_nano: timestamp_ns.saturating_sub(1_000_000_000),
                time_unix_nano: timestamp_ns,
                sketch: sketch.to_msgpack().expect("encode CountSketch-with-heap"),
                encoding: CountSketchEncoding::Msgpack as i32,
                flags: 0,
                series_id: 0,
            }],
            aggregation_temporality: 0,
            rows: COUNT_SKETCH_ROWS as i32,
            cols: COUNT_SKETCH_COLS as i32,
        }),
    )
}

fn exact_quantile(raw: &[f64], q: f64) -> f64 {
    let mut sorted = raw.to_vec();
    sorted.sort_by(f64::total_cmp);
    let rank = q * (sorted.len() - 1) as f64;
    let low = rank.floor() as usize;
    let high = rank.ceil() as usize;
    sorted[low] + (sorted[high] - sorted[low]) * (rank - low as f64)
}

fn assert_frequency_total_oracle(response: &Value, raw: &[&str]) {
    let samples = scalar_values(response);
    assert_eq!(samples.len(), 1, "expected one grouped frequency total");
    assert_eq!(
        samples[0].0.get("service").map(String::as_str),
        Some(SERVICE)
    );
    assert_eq!(samples[0].1.round() as usize, raw.len());
}

fn assert_frequency_point_oracle(response: &Value, raw: &[&str], item: &str) {
    let samples = scalar_values(response);
    assert_eq!(samples.len(), 1, "expected one grouped frequency estimate");
    assert_eq!(
        samples[0].0.get("service").map(String::as_str),
        Some(SERVICE)
    );
    let expected = raw.iter().filter(|value| **value == item).count();
    assert_eq!(samples[0].1.round() as usize, expected);
}

#[tokio::test]
async fn production_kll_matches_raw_quantile_oracle() {
    let metric = "oracle_kll_latency";
    let backend = start_backend(&config(metric, "DatasketchesKLL", &format!("      k: {K}"))).await;
    let raw: Vec<f64> = (1..=101).map(f64::from).collect();
    let timestamp = now_ns().saturating_sub(2_000_000_000);
    post(&backend, kll_export(metric, timestamp, &raw)).await;
    post(
        &backend,
        kll_export(metric, timestamp + 1_000_000_000, &raw),
    )
    .await;
    let response = query(&backend, &format!("quantile_over_time(0.5, {metric}[2s])")).await;
    let samples = scalar_values(&response);
    assert_eq!(
        samples[0].0.get("service").map(String::as_str),
        Some(SERVICE)
    );
    let actual = samples[0].1;
    assert_eq!(actual, exact_quantile(&raw, 0.5));
}

#[tokio::test]
async fn production_hll_matches_raw_distinct_oracle() {
    let metric = "oracle_hll_users";
    let backend = start_backend(&config(
        metric,
        "HLL",
        &format!("      precision: {HLL_PRECISION}"),
    ))
    .await;
    let owned: Vec<String> = (0..2_000).map(|i| format!("user-{i}")).collect();
    let mut raw: Vec<&str> = owned.iter().map(String::as_str).collect();
    raw.extend(owned.iter().take(500).map(String::as_str));
    let exact = raw.iter().copied().collect::<HashSet<_>>().len() as f64;
    let timestamp = now_ns().saturating_sub(2_000_000_000);
    post(&backend, hll_export(metric, timestamp, &raw)).await;
    post(
        &backend,
        hll_export(metric, timestamp + 1_000_000_000, &raw),
    )
    .await;
    let response = query(&backend, &format!("count({metric})")).await;
    let actual = scalar_values(&response)[0].1;
    let relative_error = (actual - exact).abs() / exact;
    assert!(
        relative_error <= 0.10,
        "HLL differs from exact distinct oracle: exact={exact}, actual={actual}, error={relative_error}"
    );
}

fn frequency_fixture() -> Vec<&'static str> {
    let mut raw = Vec::new();
    for (key, count) in [("alpha", 100), ("beta", 50), ("gamma", 200), ("delta", 75)] {
        raw.extend(std::iter::repeat_n(key, count));
    }
    raw
}

#[tokio::test]
async fn production_cms_matches_raw_frequency_oracle() {
    let metric = "oracle_cms_frequency";
    let params = format!("      w: {CMS_COLS}\n      d: {CMS_ROWS}");
    let backend = start_backend(&config(metric, "CountMinSketch", &params)).await;
    let raw = frequency_fixture();
    let timestamp = now_ns().saturating_sub(2_000_000_000);
    post(&backend, cms_export(metric, timestamp, &raw)).await;
    post(
        &backend,
        cms_export(metric, timestamp + 1_000_000_000, &raw),
    )
    .await;
    let response = query(&backend, &format!("count_over_time({metric}[2s])")).await;
    let mut merged_raw = raw.clone();
    merged_raw.extend_from_slice(&raw);
    assert_frequency_total_oracle(&response, &merged_raw);
}

#[tokio::test]
async fn production_count_sketch_matches_raw_frequency_oracle() {
    let metric = "oracle_count_sketch_frequency";
    let params = format!("      w: {COUNT_SKETCH_COLS}\n      d: {COUNT_SKETCH_ROWS}");
    // Planner's CountSketch confidence model requires more rows than the
    // current packed-hash runtime can execute at this width. Keep the default
    // production SLA untouched and declare the weaker contract explicitly
    // for this isolated algorithm-oracle process.
    let backend = start_backend(&config(metric, "CountSketch", &params)).await;
    let raw = frequency_fixture();
    let timestamp = now_ns().saturating_sub(2_000_000_000);
    post(&backend, count_sketch_export(metric, timestamp, &raw)).await;
    post(
        &backend,
        count_sketch_export(metric, timestamp + 1_000_000_000, &raw),
    )
    .await;
    let response = query(
        &backend,
        &format!("count_over_time({metric}{{item=\"alpha\"}}[2s])"),
    )
    .await;
    let mut merged_raw = raw.clone();
    merged_raw.extend_from_slice(&raw);
    assert_frequency_point_oracle(&response, &merged_raw, "alpha");
}
