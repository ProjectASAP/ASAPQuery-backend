//! Black-box PromQL differential E2E for the production data-plane process.
//!
//! A deterministic raw-value fixture is summarized into the same modified
//! OTLP DDSketch shape emitted by the collector runtime. The production
//! backend ingests that sketch, and its public instant/range PromQL responses
//! are compared with an independent exact quantile oracle over the raw values.

#[path = "support/physical_fixture.rs"]
mod physical_fixture;

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use asap_otel_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use asap_otel_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use asap_otel_proto::tonic::metrics::v1::{
    metric::Data, DdSketch, DdSketchDataPoint, DdSketchEncoding, Metric, ResourceMetrics,
    ScopeMetrics,
};
use asap_sketchlib::proto::sketchlib::{sketch_envelope, DdSketchState, SketchEnvelope};
use prost::Message;
use serde_json::Value;

const METRIC: &str = "differential_e2e_latency_ms";
const SERVICE: &str = "checkout";
const ALPHA: f64 = 0.01;
// Median keeps this stable DDSketch oracle focused on the sketch's documented
// relative-error contract. Small-sample p99 semantics are a separate product
// gap tracked by issue #492; this test does not claim to cover them.
const QUANTILE: f64 = 0.5;

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn unused_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve loopback port");
    listener.local_addr().expect("read loopback address").port()
}

fn exact_quantile(values: &[f64], quantile: f64) -> f64 {
    assert!(!values.is_empty());
    assert!((0.0..=1.0).contains(&quantile));
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let rank = quantile * (sorted.len() - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    if lower == upper {
        sorted[lower]
    } else {
        let fraction = rank - lower as f64;
        sorted[lower] + (sorted[upper] - sorted[lower]) * fraction
    }
}

fn ddsketch_export(metric: &str, timestamp_ns: u64, values: &[f64]) -> Vec<u8> {
    let mut sketch = asap_sketchlib::DdSketch::new(ALPHA);
    for value in values {
        sketch.update(*value);
    }
    let state = DdSketchState {
        alpha: sketch.wire_alpha(),
        store_counts: sketch.store_counts,
        store_offset: sketch.store_offset,
        ..Default::default()
    };
    let point = DdSketchDataPoint {
        attributes: vec![KeyValue {
            key: "service".into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(SERVICE.into())),
            }),
        }],
        start_time_unix_nano: timestamp_ns.saturating_sub(1_000_000_000),
        time_unix_nano: timestamp_ns,
        sketch: SketchEnvelope {
            format_version: 1,
            sketch_state: Some(sketch_envelope::SketchState::Ddsketch(state)),
            ..Default::default()
        }
        .encode_to_vec(),
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
                    name: metric.into(),
                    description: String::new(),
                    unit: String::new(),
                    metadata: Vec::new(),
                    data: Some(Data::Ddsketch(DdSketch {
                        data_points: vec![point],
                        aggregation_temporality: 0,
                        relative_accuracy: ALPHA,
                    })),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
    .encode_to_vec()
}

fn first_instant(response: &Value) -> Option<(&Value, f64, f64)> {
    let series = response["data"]["result"].as_array()?.first()?;
    let sample = series["value"].as_array()?;
    Some((
        &series["metric"],
        sample.first()?.as_f64()?,
        sample.get(1)?.as_str()?.parse().ok()?,
    ))
}

fn first_range_values(response: &Value) -> Option<(&Value, Vec<(f64, f64)>)> {
    let series = response["data"]["result"].as_array()?.first()?;
    let values = series["values"]
        .as_array()?
        .iter()
        .map(|sample| {
            let pair = sample.as_array()?;
            Some((
                pair.first()?.as_f64()?,
                pair.get(1)?.as_str()?.parse().ok()?,
            ))
        })
        .collect::<Option<Vec<_>>>()?;
    Some((&series["metric"], values))
}

fn assert_approx(reference: f64, actual: f64, context: &str) {
    let relative_error = (actual - reference).abs() / reference.abs().max(f64::EPSILON);
    assert!(
        relative_error <= ALPHA * 1.05,
        "{context}: reference={reference}, actual={actual}, relative_error={relative_error}, \
         allowed={}",
        ALPHA * 1.05
    );
}

async fn wait_until_ready(client: &reqwest::Client, url: &str, child: &mut Child) {
    for _ in 0..100 {
        if let Some(status) = child.try_wait().expect("inspect data-plane process") {
            panic!("data-plane exited before readiness: {status}");
        }
        if client
            .get(url)
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("data-plane did not become ready at {url}");
}

async fn get_json(client: &reqwest::Client, url: &str, params: &[(&str, String)]) -> Value {
    client
        .get(url)
        .query(params)
        .send()
        .await
        .expect("send PromQL request")
        .json()
        .await
        .expect("decode PromQL response")
}

#[tokio::test]
async fn production_backend_matches_raw_oracle_and_range_endpoint() {
    let query_port = unused_port();
    let otlp_http_port = unused_port();
    let otlp_grpc_port = unused_port();
    let output_dir = tempfile::tempdir().expect("create log directory");
    let install = physical_fixture::artifact_from_materializations(vec![ddsketch_config()]);
    let mut physical = tempfile::NamedTempFile::new().unwrap();
    serde_json::to_writer(&mut physical, &install).unwrap();

    let child = Command::new(env!("CARGO_BIN_EXE_data_plane"))
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
        .stderr(Stdio::null())
        .spawn()
        .expect("start production data-plane binary");
    let mut child = ChildGuard(child);

    let client = reqwest::Client::new();
    let query_base = format!("http://127.0.0.1:{query_port}");
    wait_until_ready(
        &client,
        &format!("{query_base}/api/v1/health"),
        &mut child.0,
    )
    .await;

    let raw_values = [10.0, 12.0, 15.0, 20.0, 30.0, 45.0, 60.0, 80.0, 100.0];
    let reference = exact_quantile(&raw_values, QUANTILE);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock");
    // Twenty-one complete panes cover every ten-second window in the
    // instant/range comparison. Repeating the distribution preserves its quantile.
    let sample_ns = (now - Duration::from_secs(2)).as_secs() * 1_000_000_000;
    for seconds_ago in (0..=20).rev() {
        let body = ddsketch_export(METRIC, sample_ns - seconds_ago * 1_000_000_000, &raw_values);
        client
            .post(format!("http://127.0.0.1:{otlp_http_port}/v1/metrics"))
            .header("content-type", "application/x-protobuf")
            .body({
                let mut request = ExportMetricsServiceRequest::decode(body.as_slice()).unwrap();
                physical_fixture::stamp(&mut request, &install);
                request.encode_to_vec()
            })
            .send()
            .await
            .expect("POST modified OTLP")
            .error_for_status()
            .expect("backend accepted modified OTLP");
    }

    let query = format!("quantile_over_time({QUANTILE}, {METRIC}[10s])");
    let instant_url = format!("{query_base}/api/v1/query");
    let mut instant = Value::Null;
    for _ in 0..50 {
        instant = get_json(
            &client,
            &instant_url,
            &[
                ("query", query.clone()),
                ("time", (sample_ns / 1_000_000_000).to_string()),
            ],
        )
        .await;
        if first_instant(&instant).is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(instant["status"], "success", "instant response: {instant}");
    assert_eq!(instant["data"]["resultType"], "vector");
    let (instant_labels, instant_timestamp, instant_value) =
        first_instant(&instant).unwrap_or_else(|| panic!("empty instant response: {instant}"));
    assert_eq!(instant_labels["service"], SERVICE);
    assert_approx(reference, instant_value, "instant query versus raw oracle");
    assert!(
        instant["infos"]
            .as_array()
            .is_some_and(|infos| infos.iter().any(|info| info
                .as_str()
                .is_some_and(|s| s.contains("data_source: asap_query")))),
        "query was not proven to come from ASAPQuery: {instant}"
    );

    let range_url = format!("{query_base}/api/v1/query_range");
    let range = get_json(
        &client,
        &range_url,
        &[
            ("query", query.clone()),
            ("start", (instant_timestamp - 10.0).to_string()),
            ("end", instant_timestamp.to_string()),
            ("step", "1".into()),
        ],
    )
    .await;
    assert_eq!(range["status"], "success", "range response: {range}");
    assert_eq!(range["data"]["resultType"], "matrix");
    let (range_labels, range_values) =
        first_range_values(&range).unwrap_or_else(|| panic!("empty range response: {range}"));
    assert_eq!(range_labels, instant_labels);
    assert!(
        !range_values.is_empty(),
        "range response had no values: {range}"
    );
    for (_, value) in &range_values {
        assert_approx(reference, *value, "range query versus raw oracle");
    }
    assert_approx(
        instant_value,
        range_values.last().expect("last range value").1,
        "instant/range endpoint consistency",
    );

    for (start, end, step, expected_error) in [
        ("10", "10", "1", "start must be before end"),
        ("10", "11", "0", "step must be positive"),
    ] {
        let invalid = get_json(
            &client,
            &range_url,
            &[
                ("query", query.clone()),
                ("start", start.into()),
                ("end", end.into()),
                ("step", step.into()),
            ],
        )
        .await;
        assert_eq!(invalid["status"], "error", "invalid range: {invalid}");
        assert!(
            invalid["error"]
                .as_str()
                .is_some_and(|error| error.contains(expected_error)),
            "unexpected validation response: {invalid}"
        );
    }
}

fn ddsketch_config() -> asap_types::PrecomputeMaterialization {
    use asap_types::{AggregationType, KeyByLabelNames, PrecomputeMaterialization, WindowKind};
    PrecomputeMaterialization::new(
        AggregationType::DDSketch,
        String::new(),
        std::collections::HashMap::from([("relative_accuracy".into(), serde_json::json!(ALPHA))]),
        KeyByLabelNames::new(vec!["service".into()]),
        KeyByLabelNames::empty(),
        KeyByLabelNames::empty(),
        String::new(),
        1,
        1,
        WindowKind::Tumbling,
        String::new(),
        METRIC.into(),
        None,
        None,
        None,
    )
}

// A real process must range-read the bound output, never a semantic substitute.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bound_sds_keeps_hot_and_rebuild_outputs_and_groups_isolated() {
    use asap_types::query_plan::QueryPlanNode;
    use asap_types::sds::StoredOutputId;
    let mut hot = ddsketch_config();
    hot.stored_output_id = Some(StoredOutputId(1001));
    let mut rebuild = hot.clone();
    rebuild.stored_output_id = Some(StoredOutputId(1002));
    let mut install = physical_fixture::artifact_from_materializations(vec![hot, rebuild]);
    assert_eq!(install.summary_catalog.definitions.len(), 1);
    assert_eq!(install.summary_catalog.outputs.len(), 2);
    for entry in install.query_plan.entries.values_mut() {
        let output = if entry.canonical_query.contains("0.99") {
            1002
        } else {
            1001
        };
        for node in entry.nodes.values_mut() {
            if let QueryPlanNode::ReadMaterialization { binding } = node {
                binding.materialization = StoredOutputId(output);
            }
        }
    }
    install
        .query_plan
        .bind_catalog(&install.summary_catalog)
        .unwrap();
    let query_port = unused_port();
    let otlp_http_port = unused_port();
    let directory = tempfile::tempdir().unwrap();
    let plan = directory.path().join("plan.json");
    std::fs::write(&plan, serde_json::to_vec(&install).unwrap()).unwrap();
    let mut child = ChildGuard(
        Command::new(env!("CARGO_BIN_EXE_data_plane"))
            .arg("--physical-plan")
            .arg(&plan)
            .arg("--http-port")
            .arg(query_port.to_string())
            .arg("--output-dir")
            .arg(directory.path())
            .arg("--enable-otel-ingest")
            .arg("--otel-http-port")
            .arg(otlp_http_port.to_string())
            .arg("--otel-grpc-port")
            .arg(unused_port().to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{query_port}");
    wait_until_ready(&client, &format!("{base}/api/v1/health"), &mut child.0).await;
    let end = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - 2;
    let send = |output: u64, second: u64, value: f64, service: &'static str| {
        let mut stamp_plan = install.clone();
        stamp_plan
            .precompute_plan
            .schemas
            .retain(|s| s.materialization == StoredOutputId(output));
        let client = client.clone();
        async move {
            let bytes = ddsketch_export(METRIC, second * 1_000_000_000, &[value; 32]);
            let mut message = ExportMetricsServiceRequest::decode(bytes.as_slice()).unwrap();
            let Some(Data::Ddsketch(sketch)) =
                &mut message.resource_metrics[0].scope_metrics[0].metrics[0].data
            else {
                panic!("fixture")
            };
            sketch.data_points[0].attributes[0].value = Some(AnyValue {
                value: Some(any_value::Value::StringValue(service.into())),
            });
            physical_fixture::stamp(&mut message, &stamp_plan);
            let response = client
                .post(format!("http://127.0.0.1:{otlp_http_port}/v1/metrics"))
                .header("content-type", "application/x-protobuf")
                .body(message.encode_to_vec())
                .send()
                .await
                .unwrap();
            let status = response.status();
            assert!(
                status.is_success(),
                "output {output}: {status}: {}",
                response.text().await.unwrap()
            );
        }
    };
    let query = |quantile: &str, range: &str| {
        let expression = format!("quantile_over_time({quantile}, {METRIC}[{range}])");
        let client = client.clone();
        let base = base.clone();
        async move {
            get_json(
                &client,
                &format!("{base}/api/v1/query"),
                &[("query", expression), ("time", end.to_string())],
            )
            .await
        }
    };
    for second in end - 2..=end {
        send(1002, second, 100.0, SERVICE).await;
    }
    let mut rebuilt = Value::Null;
    for _ in 0..50 {
        rebuilt = query("0.99", "3s").await;
        if first_instant(&rebuilt).is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_approx(
        100.0,
        first_instant(&rebuilt).expect("bound rebuild result").2,
        "rebuild",
    );
    assert!(
        first_instant(&query("0.5", "3s").await).is_none(),
        "hot must not substitute rebuild state"
    );
    send(1001, end - 2, 1.0, SERVICE).await;
    send(1001, end, 1.0, SERVICE).await;
    assert!(
        first_instant(&query("0.5", "3s").await).is_none(),
        "missing middle pane must not be an empty input"
    );
    send(1001, end - 1, 1.0, SERVICE).await;
    for second in end - 2..=end {
        send(1001, second, 200.0, "payments").await;
    }
    let mut answer = Value::Null;
    for _ in 0..50 {
        answer = query("0.5", "3s").await;
        if answer["data"]["result"]
            .as_array()
            .is_some_and(|r| r.len() == 2)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let rows = answer["data"]["result"]
        .as_array()
        .unwrap_or_else(|| panic!("{answer}"));
    assert_eq!(rows.len(), 2, "{answer}");
    for row in rows {
        let expected = if row["metric"]["service"] == SERVICE {
            1.0
        } else {
            assert_eq!(row["metric"]["service"], "payments");
            200.0
        };
        assert_approx(
            expected,
            row["value"][1].as_str().unwrap().parse().unwrap(),
            "isolated group",
        );
    }
}
