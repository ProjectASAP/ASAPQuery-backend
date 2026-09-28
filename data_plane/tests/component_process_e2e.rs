//! Black-box component E2E for the production data-plane binary.
//!
//! The child process loads its real file configuration, accepts a modified
//! OTLP DDSketch over HTTP, routes it through the precompute workers into the
//! SketchStore, and answers a PromQL query from that stored sketch.

#[path = "support/physical_fixture.rs"]
mod physical_fixture;

use std::io::Write;
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

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn unused_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve loopback port");
    let port = listener.local_addr().expect("read loopback address").port();
    drop(listener);
    port
}

fn ddsketch_export(metric: &str, timestamp_ns: u64, counts: Vec<u64>) -> Vec<u8> {
    let alpha = 0.01;
    let state = DdSketchState {
        alpha,
        store_counts: counts,
        store_offset: -1,
    };
    let point = DdSketchDataPoint {
        attributes: vec![KeyValue {
            key: "service".into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue("process-e2e".into())),
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
                        relative_accuracy: alpha,
                    })),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
    .encode_to_vec()
}

fn first_scalar(response: &serde_json::Value) -> Option<f64> {
    response["data"]["result"]
        .as_array()?
        .first()?
        .get("value")?
        .as_array()?
        .get(1)?
        .as_str()?
        .parse()
        .ok()
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

#[tokio::test]
async fn production_binary_ingests_ddsketch_and_answers_promql() {
    let query_port = unused_port();
    let otlp_http_port = unused_port();
    let otlp_grpc_port = unused_port();
    let output_dir = tempfile::tempdir().expect("create log directory");
    let mut config = tempfile::NamedTempFile::new().expect("create streaming config");
    write!(
        config,
        r#"aggregations:
  - aggregationType: DDSketch
    aggregationSubType: ''
    labels:
      grouping: [service]
      rollup: []
      aggregated: []
    metric: component_process_e2e_latency_ms
    parameters:
      relative_accuracy: 0.01
    windowSize: 1
    windowType: tumbling
    spatialFilter: ''
"#
    )
    .expect("write streaming config");

    let runtime = data_plane::storage_engines::types::StreamingConfig::from_yaml_data(
        &serde_yaml::from_slice(&std::fs::read(config.path()).unwrap()).unwrap(),
    )
    .unwrap();
    let install = physical_fixture::artifact(&runtime);
    let mut physical = tempfile::NamedTempFile::new().unwrap();
    serde_json::to_writer(&mut physical, &install).unwrap();

    let child = Command::new(env!("CARGO_BIN_EXE_data_plane"))
        .arg("--physical-plan")
        .arg(physical.path())
        .arg("--streaming-config")
        .arg(config.path())
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
    let base = format!("http://127.0.0.1:{query_port}");
    wait_until_ready(&client, &format!("{base}/api/v1/health"), &mut child.0).await;

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock")
        .as_secs()
        * 1_000_000_000;
    // Supply every pane in the queried ten-second interval.
    for seconds_ago in (1..=10).rev() {
        let body = ddsketch_export(
            "component_process_e2e_latency_ms",
            now_ns - seconds_ago * 1_000_000_000,
            vec![5, 10, 15, 20],
        );
        let response = client
            .post(format!("http://127.0.0.1:{otlp_http_port}/v1/metrics"))
            .header("content-type", "application/x-protobuf")
            .body({
                let mut request = ExportMetricsServiceRequest::decode(body.as_slice()).unwrap();
                physical_fixture::stamp(&mut request, &install);
                request.encode_to_vec()
            })
            .send()
            .await
            .expect("POST modified OTLP to production receiver");
        let status = response.status();
        let body = response.text().await.unwrap();
        assert!(status.is_success(), "OTLP {status}: {body}");
    }

    let query = "quantile_over_time(0.99, component_process_e2e_latency_ms[10s])";
    for _ in 0..50 {
        let response: serde_json::Value = client
            .get(format!("{base}/api/v1/query"))
            .query(&[
                ("query", query.to_string()),
                ("time", (now_ns / 1_000_000_000 - 1).to_string()),
            ])
            .send()
            .await
            .expect("query production data plane")
            .json()
            .await
            .expect("decode PromQL response");
        if let Some(value) = first_scalar(&response) {
            assert!(
                value.is_finite() && value > 0.0,
                "invalid quantile: {value}"
            );
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("production data plane never served the ingested DDSketch");
}
