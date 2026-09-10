//! Production-process E2E oracle matrix for every supported sketch family.
//!
//! Each test starts the real `data_plane` binary, installs a streaming policy,
//! posts modified OTLP over HTTP, and queries the public Prometheus endpoint.
//! Sketch implementations are used only to encode raw fixtures. Expected
//! answers are independently computed from those raw fixtures.

use std::collections::{HashMap, HashSet};
use std::io::{Seek, Write};
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
use asap_sketchlib::proto::sketchlib::{
    CountMinState, CountSketchState, CounterType, HllVariant as ProtoHllVariant, HyperLogLogState,
    KllState,
};
use asap_sketchlib::{CountMinSketch, CountSketch, HllSketch, HllVariant};
use data_plane::SerializableToSink;
use prost::Message;
use serde_json::Value;

const SERVICE: &str = "oracle-e2e";
// These parameters satisfy the production query path's LIVE_ACCURACY
// (`epsilon = 0.01`). ASAPPlanner validates an observed `SketchKind`
// against that accuracy before committing it, so the fixture must use the
// same contract as a real control-plane-generated materialization.
const K: u32 = 269;
const HLL_PRECISION: u32 = 7;
const CMS_ROWS: usize = 5;
const CMS_COLS: usize = 2048;
// ASAPPlanner's CountSketch guarantee is L2-based: epsilon=sqrt(3/width),
// with an odd Hoeffding-median depth. The current runtime's packed sign hash
// limits this width to five rows, which satisfies the explicit delta=0.8
// contract used only by the CountSketch child process below.
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
    _child: ChildGuard,
    client: reqwest::Client,
    query_base: String,
    otlp_url: String,
    _config: tempfile::NamedTempFile,
    _streaming_config: tempfile::NamedTempFile,
    _output_dir: tempfile::TempDir,
    frame: Vec<(String, String)>,
    window_start_ns: u64,
    sketch_params: planner_types::post_asap::SketchParams,
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
        .as_nanos() as u64
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

async fn start_backend(
    config_yaml: &str,
    live_delta: Option<&str>,
    promql: &str,
    algorithm: planner_types::post_asap::SketchAlgorithm,
    accuracy: planner_types::types::AccuracyTarget,
) -> Backend {
    let query_port = unused_port();
    let otlp_http_port = unused_port();
    let otlp_grpc_port = unused_port();
    let output_dir = tempfile::tempdir().expect("create data-plane output directory");
    let mut streaming_config = tempfile::NamedTempFile::new().unwrap();
    streaming_config.write_all(config_yaml.as_bytes()).unwrap();
    streaming_config.flush().unwrap();
    let mut fixture: Value = serde_json::from_str(include_str!(
        "../../docs/examples/asapquery-compatibility-demo-snapshot.json"
    ))
    .unwrap();
    let mut demand = fixture["query_workload"]["repeating_queries"][0].clone();
    demand["query"] = promql.into();
    demand["time_selection"]["lookback"] = 10_000.into();
    fixture["query_workload"]["repeating_queries"] = serde_json::json!([demand]);
    fixture["implementation"]["topk_evidence"] = serde_json::json!({});
    let snapshot: control_plane::physical::compiler::BackendLocalPlanningSnapshot =
        serde_json::from_value(fixture).unwrap();
    let (mut request, mut environment) = snapshot.planning_request().unwrap();
    request.hybrid_execution = false;
    request.query_workload = None;
    request.queries[0].window_implementations[0].implementation_id = "collector-tumbling-v1".into();
    environment.target =
        control_plane::physical::compiler::PhysicalDeploymentTarget::DistributedCollectors;
    environment.collector_ids = vec!["oracle-e2e-collector".into()];
    request.queries[0].accuracy = accuracy.clone();
    request.queries[0].group_by = vec!["service".into()];
    let expr =
        control_plane::query_parser::parse_query_expr_canonical(promql, accuracy.clone()).unwrap();
    request.queries[0].post_asap = control_plane::planner_selection::select_summary(
        &expr,
        &control_plane::physical::post_asap::cost_model::ForcedFamilyCostModel::new(
            accuracy,
            algorithm.clone(),
        ),
    )
    .unwrap();
    let plan = control_plane::physical::compiler::PhysicalCompiler
        .compile(request, environment)
        .unwrap();
    let mut installed = plan.precompute_plan.materializations[0].serialize_to_json();
    installed["labels"] = serde_json::json!({
        "grouping": plan.precompute_plan.materializations[0].grouping_labels.labels,
        "rollup": [],
        "aggregated": []
    });
    streaming_config.as_file_mut().set_len(0).unwrap();
    streaming_config.as_file_mut().rewind().unwrap();
    serde_json::to_writer(
        &mut streaming_config,
        &serde_json::json!({"aggregations": [installed]}),
    )
    .unwrap();
    streaming_config.flush().unwrap();
    let schema = plan
        .precompute_plan
        .schemas
        .first()
        .expect("compiled state schema");
    let producer = plan.precompute_plan.producers.first().unwrap_or_else(|| {
        panic!(
            "compiled producer; schemas={:?}, materializations={:?}, collectors={:?}",
            plan.precompute_plan.schemas,
            plan.precompute_plan.materializations,
            plan.collector_plans
        )
    });
    let control_plane::physical::compiler::StateFamilyContract::Sketch {
        parameters: sketch_params,
        ..
    } = &schema.family
    else {
        panic!("oracle requires sketch schema")
    };
    let sketch_params = sketch_params.clone();
    let encoding = "sketchlib_protobuf_v1";
    let frame = vec![
        ("identity_version".into(), "1".into()),
        ("plan_id".into(), plan.envelope.plan_id.to_string()),
        (
            "plan_version".into(),
            plan.envelope.plan_version.to_string(),
        ),
        (
            "backend_compat".into(),
            plan.envelope.backend_compat.clone(),
        ),
        (
            "materialization".into(),
            schema.materialization.as_u64().to_string(),
        ),
        (
            "series_identity".into(),
            data_plane::drivers::ingest::canonical_attrs_fingerprint(&[("service", SERVICE)]),
        ),
        ("schema_id".into(), schema.schema_id.clone()),
        ("producer_id".into(), producer.producer_id.clone()),
        ("producer_epoch".into(), "oracle-e2e".into()),
        ("kind".into(), "full".into()),
        ("encoding".into(), encoding.into()),
    ];
    let artifact = data_plane::drivers::query::servers::http::PhysicalPlanInstallRequest {
        summary_catalog: plan.summary_catalog,
        collector_plans: plan.collector_plans,
        precompute_plan: plan.precompute_plan,
        transmission_plan: plan.transmission_plan,
        query_plan: plan.query_plan,
        storage_routing: None,
        adaptation_evidence: vec![],
    };
    let mut config = tempfile::NamedTempFile::new().expect("create physical plan");
    serde_json::to_writer(&mut config, &artifact).unwrap();
    config.flush().unwrap();

    let mut command = Command::new(env!("CARGO_BIN_EXE_data_plane"));
    command
        .arg("--physical-plan")
        .arg(config.path())
        .arg("--streaming-config")
        .arg(streaming_config.path())
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
    if let Some(delta) = live_delta {
        command.env("ASAP_SUMMARY_EXECUTOR_DELTA", delta);
    }
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
                _child: child,
                client,
                query_base,
                otlp_url: format!("http://127.0.0.1:{otlp_http_port}/v1/metrics"),
                _config: config,
                _streaming_config: streaming_config,
                _output_dir: output_dir,
                frame,
                window_start_ns: now_ns() / 10_000_000_000 * 10_000_000_000 - 10_000_000_000,
                sketch_params,
            };
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("data-plane did not become ready at {health}");
}

async fn post(backend: &Backend, mut request: ExportMetricsServiceRequest, sequence: u64) {
    let metric = &mut request.resource_metrics[0].scope_metrics[0].metrics[0];
    let attributes = match metric.data.as_mut().expect("metric data") {
        Data::Kllsketch(value) => &mut value.data_points[0].attributes,
        Data::Hllsketch(value) => &mut value.data_points[0].attributes,
        Data::Countminsketch(value) => &mut value.data_points[0].attributes,
        Data::Countsketch(value) => &mut value.data_points[0].attributes,
        other => panic!("unexpected oracle metric: {other:?}"),
    };
    let mut frame = backend.frame.clone();
    frame.extend([
        ("sequence".into(), sequence.to_string()),
        ("checkpoint_id".into(), format!("oracle-{sequence}")),
    ]);
    attributes.extend(frame.into_iter().map(|(key, value)| KeyValue {
        key: format!("asap.frame.{key}"),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value)),
        }),
    }));
    let response = backend
        .client
        .post(&backend.otlp_url)
        .header("content-type", "application/x-protobuf")
        .body(request.encode_to_vec())
        .send()
        .await
        .expect("post modified OTLP");
    let status = response.status();
    let body = response.text().await.expect("read modified OTLP response");
    assert!(
        status.is_success(),
        "production backend rejected modified OTLP ({status}): {body}"
    );
}

async fn query(backend: &Backend, promql: &str) -> Value {
    let mut response = Value::Null;
    for _ in 0..50 {
        response = backend
            .client
            .get(format!("{}/api/v1/query", backend.query_base))
            .query(&[
                ("query", promql.to_string()),
                (
                    "time",
                    ((backend.window_start_ns + 10_000_000_000) as f64 / 1e9).to_string(),
                ),
            ])
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

fn config(metric: &str, kind: &str, parameters: &str) -> String {
    format!(
        "aggregations:\n  - aggregationType: {kind}\n    aggregationSubType: ''\n    labels:\n      grouping: [service]\n      rollup: []\n      aggregated: []\n    metric: {metric}\n    parameters:\n{parameters}\n    windowSize: 1\n    windowType: tumbling\n    spatialFilter: ''\n"
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

fn hll_export(
    metric: &str,
    timestamp_ns: u64,
    raw: &[&str],
    precision: u32,
) -> ExportMetricsServiceRequest {
    let mut sketch = HllSketch::new(HllVariant::Regular, precision);
    for value in raw {
        sketch.update(value.as_bytes());
    }
    let state = HyperLogLogState {
        variant: ProtoHllVariant::Regular as i32,
        precision,
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
            precision,
        }),
    )
}

fn cms_export(
    metric: &str,
    timestamp_ns: u64,
    raw: &[&str],
    rows: usize,
    cols: usize,
) -> ExportMetricsServiceRequest {
    let mut sketch = CountMinSketch::new(rows, cols);
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
                sketch: CountMinState {
                    rows: rows as u32,
                    cols: cols as u32,
                    counter_type: CounterType::Float64 as i32,
                    counts_int: vec![],
                    counts_float: sketch.sketch().into_iter().flatten().collect(),
                    sum_counts: vec![],
                    sum2_counts: vec![],
                    l1: vec![],
                    l2: vec![],
                }
                .encode_to_vec(),
                encoding: CountMinSketchEncoding::Proto as i32,
                flags: 0,
                series_id: 0,
            }],
            aggregation_temporality: 0,
            rows: rows as i32,
            cols: cols as i32,
        }),
    )
}

fn count_sketch_export(
    metric: &str,
    timestamp_ns: u64,
    raw: &[&str],
    rows: usize,
    cols: usize,
) -> ExportMetricsServiceRequest {
    let mut sketch = CountSketch::new(rows, cols);
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
                sketch: CountSketchState {
                    rows: rows as u32,
                    cols: cols as u32,
                    counter_type: CounterType::Float64 as i32,
                    counts_int: vec![],
                    counts_float: sketch.matrix.into_iter().flatten().collect(),
                    l2: vec![],
                    topk: None,
                }
                .encode_to_vec(),
                encoding: CountSketchEncoding::Proto as i32,
                flags: 0,
                series_id: 0,
            }],
            aggregation_temporality: 0,
            rows: rows as i32,
            cols: cols as i32,
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
    let backend = start_backend(
        &config(metric, "DatasketchesKLL", &format!("      K: {K}")),
        None,
        &format!("quantile_over_time(0.5, {metric}[10s])"),
        planner_types::post_asap::SketchAlgorithm::Kll,
        planner_types::types::AccuracyTarget::EpsilonDelta {
            epsilon: 0.01,
            delta: 0.01,
        },
    )
    .await;
    let raw: Vec<f64> = (1..=101).map(f64::from).collect();
    let timestamp = backend.window_start_ns + 9_000_000_000;
    post(&backend, kll_export(metric, timestamp, &raw), 1).await;
    post(
        &backend,
        kll_export(metric, timestamp + 1_000_000_000, &raw),
        2,
    )
    .await;
    post(
        &backend,
        kll_export(metric, timestamp + 10_000_000_000, &raw),
        3,
    )
    .await;
    let response = query(&backend, &format!("quantile_over_time(0.5, {metric}[10s])")).await;
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
    let backend = start_backend(
        &config(metric, "HLL", &format!("      precision: {HLL_PRECISION}")),
        None,
        &format!("count({metric})"),
        planner_types::post_asap::SketchAlgorithm::Hll,
        planner_types::types::AccuracyTarget::Epsilon(0.1),
    )
    .await;
    let owned: Vec<String> = (0..2_000).map(|i| format!("user-{i}")).collect();
    let mut raw: Vec<&str> = owned.iter().map(String::as_str).collect();
    raw.extend(owned.iter().take(500).map(String::as_str));
    let exact = raw.iter().copied().collect::<HashSet<_>>().len() as f64;
    let planner_types::post_asap::SketchParams::Hll { precision } = &backend.sketch_params else {
        panic!("HLL plan")
    };
    let precision = u32::from(*precision);
    let timestamp = backend.window_start_ns + 9_000_000_000;
    post(&backend, hll_export(metric, timestamp, &raw, precision), 1).await;
    post(
        &backend,
        hll_export(metric, timestamp + 1_000_000_000, &raw, precision),
        2,
    )
    .await;
    post(
        &backend,
        hll_export(metric, timestamp + 10_000_000_000, &raw, precision),
        3,
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
    let backend = start_backend(
        &config(metric, "CountMinSketch", &params),
        None,
        &format!("count_over_time({metric}{{item=\"alpha\"}}[10s])"),
        planner_types::post_asap::SketchAlgorithm::Cms,
        planner_types::types::AccuracyTarget::EpsilonDelta {
            epsilon: 0.01,
            delta: 0.01,
        },
    )
    .await;
    let raw = frequency_fixture();
    let planner_types::post_asap::SketchParams::Cms { width, depth } = &backend.sketch_params
    else {
        panic!("CMS plan")
    };
    let (rows, cols) = (*depth as usize, *width as usize);
    let timestamp = backend.window_start_ns + 9_000_000_000;
    post(&backend, cms_export(metric, timestamp, &raw, rows, cols), 1).await;
    post(
        &backend,
        cms_export(metric, timestamp + 1_000_000_000, &raw, rows, cols),
        2,
    )
    .await;
    post(
        &backend,
        cms_export(metric, timestamp + 10_000_000_000, &raw, rows, cols),
        3,
    )
    .await;
    let response = query(
        &backend,
        &format!("count_over_time({metric}{{item=\"alpha\"}}[10s])"),
    )
    .await;
    let mut merged_raw = raw.clone();
    merged_raw.extend_from_slice(&raw);
    assert_frequency_point_oracle(&response, &merged_raw, "alpha");
}

#[tokio::test]
async fn production_count_sketch_matches_raw_frequency_oracle() {
    let metric = "oracle_count_sketch_frequency";
    let params = format!("      w: {COUNT_SKETCH_COLS}\n      d: {COUNT_SKETCH_ROWS}");
    // Planner's CountSketch confidence model requires more rows than the
    // current packed-hash runtime can execute at this width. Keep the default
    // production SLA untouched and declare the weaker contract explicitly
    // for this isolated algorithm-oracle process.
    let backend = start_backend(
        &config(metric, "CountSketch", &params),
        Some("0.8"),
        &format!("count_over_time({metric}{{item=\"alpha\"}}[10s])"),
        planner_types::post_asap::SketchAlgorithm::CountSketch,
        planner_types::types::AccuracyTarget::EpsilonDelta {
            epsilon: 0.03,
            delta: 0.8,
        },
    )
    .await;
    let raw = frequency_fixture();
    let planner_types::post_asap::SketchParams::CountSketch { width, depth } =
        &backend.sketch_params
    else {
        panic!("CountSketch plan")
    };
    let (rows, cols) = (*depth as usize, *width as usize);
    let timestamp = backend.window_start_ns + 9_000_000_000;
    post(
        &backend,
        count_sketch_export(metric, timestamp, &raw, rows, cols),
        1,
    )
    .await;
    post(
        &backend,
        count_sketch_export(metric, timestamp + 1_000_000_000, &raw, rows, cols),
        2,
    )
    .await;
    post(
        &backend,
        count_sketch_export(metric, timestamp + 10_000_000_000, &raw, rows, cols),
        3,
    )
    .await;
    let response = query(
        &backend,
        &format!("count_over_time({metric}{{item=\"alpha\"}}[10s])"),
    )
    .await;
    let mut merged_raw = raw.clone();
    merged_raw.extend_from_slice(&raw);
    assert_frequency_point_oracle(&response, &merged_raw, "alpha");
}
