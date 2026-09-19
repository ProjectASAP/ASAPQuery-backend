//! Whole-backend process E2E.
//!
//! Starts the production control-plane and data-plane executables, asks the
//! controller to plan a workload, observes the physical plan installed by the
//! data plane, sends a modified-OTLP DDSketch, and verifies the resulting
//! PromQL value. No server or planner is constructed in the test process.

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
use asap_sketchlib::proto::sketchlib::{sketch_envelope, SketchEnvelope as ProtoEnvelope};
use control_plane::opamp::{
    opamp_proto, CollectorPlanStatus, CollectorPlanStatusKind, COLLECTOR_PLAN_CAPABILITY,
    COLLECTOR_PLAN_MESSAGE, PLAN_STATUS_MESSAGE,
};
use control_plane::physical::compiler::PLANNER_REVISION;
use futures::{SinkExt, StreamExt};
use prost::Message;
use tokio_tungstenite::tungstenite::{http::Request, Message as WsMessage};

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn unused_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve loopback port");
    listener.local_addr().expect("loopback address").to_string()
}

fn port(address: &str) -> u16 {
    address
        .rsplit_once(':')
        .expect("host:port")
        .1
        .parse()
        .expect("numeric port")
}

async fn wait_http(client: &reqwest::Client, url: &str, child: &mut Child, name: &str) {
    for _ in 0..200 {
        if let Some(status) = child.try_wait().expect("inspect child process") {
            panic!("{name} exited before readiness: {status}");
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
    panic!("{name} did not become ready at {url}");
}

fn ddsketch_export(
    metric: &str,
    timestamp_ns: u64,
    values: &[f64],
    alpha: f64,
    plan: &serde_json::Value,
    sequence: u64,
) -> Vec<u8> {
    let decoded: asap_types::producer_plan::CollectorPlan =
        serde_json::from_value(plan.clone()).unwrap();
    assert_eq!(decoded.materializations.len(), 1);
    let mut sketch = asap_sketchlib::DdSketch::new(alpha);
    for value in values {
        sketch.update(*value);
    }
    assert_eq!(sketch.total_count(), values.len() as u64);
    let wire =
        ProtoEnvelope::decode(asap_sketch_codec::encode_ddsketch(&sketch).as_slice()).unwrap();
    let Some(sketch_envelope::SketchState::Ddsketch(state)) = wire.sketch_state else {
        panic!("expected DDSketch state")
    };
    let materialization = plan["materializations"][0]["materialization"]
        .as_u64()
        .unwrap();
    let mut attributes = vec![KeyValue {
        key: "service".into(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue("whole-e2e".into())),
        }),
    }];
    for (key, value) in [
        ("identity_version", "1".into()),
        ("plan_id", plan["envelope"]["plan_id"].to_string()),
        ("plan_version", plan["envelope"]["plan_version"].to_string()),
        (
            "backend_compat",
            control_plane::physical::compiler::BACKEND_COMPAT.into(),
        ),
        ("materialization", materialization.to_string()),
        (
            "series_identity",
            data_plane::drivers::ingest::canonical_attrs_fingerprint(&[("service", "whole-e2e")]),
        ),
        (
            "schema_id",
            format!(
                "{}:summary-state:v1:{materialization}",
                control_plane::physical::compiler::BACKEND_COMPAT
            ),
        ),
        ("producer_id", "whole-e2e-collector".into()),
        ("producer_epoch", "process-e2e".into()),
        ("sequence", sequence.to_string()),
        ("kind", "full".into()),
        ("encoding", "sketchlib_protobuf_v1".into()),
        ("checkpoint_id", format!("checkpoint-{sequence}")),
    ] {
        attributes.push(KeyValue {
            key: format!("asap.frame.{key}"),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value)),
            }),
        });
    }
    let point = DdSketchDataPoint {
        attributes,
        start_time_unix_nano: timestamp_ns.saturating_sub(1_000_000_000),
        time_unix_nano: timestamp_ns,
        sketch: state.encode_to_vec(),
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

fn exact_quantile(values: &[f64], quantile: f64) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let rank = quantile * (sorted.len() - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    let fraction = rank - lower as f64;
    sorted[lower] + (sorted[upper] - sorted[lower]) * fraction
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

async fn connect_collector(
    address: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let uri = format!("ws://{address}/v1/opamp");
    for _ in 0..100 {
        let request = Request::builder()
            .uri(&uri)
            .header("Host", address)
            .header("X-Agent-ID", "whole-e2e-collector")
            .header("X-Agent-Role", "agent")
            .header("Upgrade", "websocket")
            .header("Connection", "Upgrade")
            .header("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
            .header("Sec-WebSocket-Version", "13")
            .body(())
            .expect("build OpAMP request");
        if let Ok((socket, _)) = tokio_tungstenite::connect_async(request).await {
            return socket;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("collector could not connect to production OpAMP endpoint {uri}");
}

async fn send_agent_message(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    message: opamp_proto::AgentToServer,
) {
    let mut frame = vec![0];
    message
        .encode(&mut frame)
        .expect("encode OpAMP AgentToServer");
    socket
        .send(WsMessage::Binary(frame))
        .await
        .expect("send OpAMP AgentToServer");
}

type CollectorSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn apply_next_collector_plan(
    address: String,
    accept: bool,
) -> (serde_json::Value, CollectorSocket) {
    let mut socket = connect_collector(&address).await;
    send_agent_message(
        &mut socket,
        opamp_proto::AgentToServer {
            custom_capabilities: Some(opamp_proto::CustomCapabilities {
                capabilities: vec![COLLECTOR_PLAN_CAPABILITY.into()],
            }),
            ..Default::default()
        },
    )
    .await;
    respond_next_collector_plan(socket, accept).await
}

async fn respond_next_collector_plan(
    mut socket: CollectorSocket,
    accept: bool,
) -> (serde_json::Value, CollectorSocket) {
    let frame = tokio::time::timeout(Duration::from_secs(10), socket.next())
        .await
        .expect("controller did not publish a collector plan")
        .expect("controller closed the OpAMP connection")
        .expect("read collector-plan frame");
    let bytes = frame.into_data();
    let payload = bytes.strip_prefix(&[0]).unwrap_or(&bytes);
    let message = opamp_proto::ServerToAgent::decode(payload)
        .expect("decode production ServerToAgent protobuf");
    let custom = message
        .custom_message
        .expect("collector-plan custom message");
    assert_eq!(custom.capability, COLLECTOR_PLAN_CAPABILITY);
    assert_eq!(custom.r#type, COLLECTOR_PLAN_MESSAGE);
    let decoded: asap_types::producer_plan::CollectorPlan =
        serde_json::from_slice(&custom.data).expect("decode backend CollectorPlan");
    assert_eq!(decoded.materializations.len(), 1);
    let plan: serde_json::Value =
        serde_json::from_slice(&custom.data).expect("decode collector physical plan");
    let plan_id = plan["envelope"]["plan_id"]
        .as_u64()
        .expect("collector plan ID");
    let plan_version = plan["envelope"]["plan_version"]
        .as_u64()
        .expect("collector plan version");

    let status = serde_json::to_vec(&CollectorPlanStatus {
        plan_id,
        plan_version,
        status: if accept {
            CollectorPlanStatusKind::Staged
        } else {
            CollectorPlanStatusKind::Failed
        },
        error: (!accept).then(|| "injected Collector staging failure".into()),
    })
    .expect("encode applied status");
    send_agent_message(
        &mut socket,
        opamp_proto::AgentToServer {
            custom_message: Some(opamp_proto::CustomMessage {
                capability: COLLECTOR_PLAN_CAPABILITY.into(),
                r#type: PLAN_STATUS_MESSAGE.into(),
                data: status,
            }),
            ..Default::default()
        },
    )
    .await;
    (plan, socket)
}

async fn quote_workload(
    client: &reqwest::Client,
    control_base: &str,
    request: &mut serde_json::Value,
) {
    request
        .as_object_mut()
        .unwrap()
        .remove("workload_cost_evidence");
    let response = client
        .post(format!(
            "{control_base}/api/v1/physical-plan/cost-manifests"
        ))
        .json(&request)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    assert!(status.is_success(), "cost manifests {status}: {body}");
    let manifests: Vec<control_plane::physical::workload_cost::WorkloadCostManifest> =
        serde_json::from_str(&body).unwrap();
    assert_eq!(manifests.len(), 2);
    let quotes = manifests
        .into_iter()
        .enumerate()
        .map(|(index, manifest)| {
            let unit_costs = manifest
                .components
                .keys()
                .map(|key| (key.clone(), 1.0))
                .collect();
            control_plane::physical::workload_cost::WorkloadQuote {
                manifest,
                executable: index == 0,
                unit_costs,
            }
        })
        .collect();
    request["workload_cost_evidence"] = serde_json::to_value(
        control_plane::physical::workload_cost::WorkloadCostEvidence {
            backend_revision: control_plane::physical::compiler::BACKEND_REVISION.into(),
            planner_revision: control_plane::physical::compiler::PLANNER_REVISION.into(),
            data_snapshot_id: "whole-process-fixture-v1".into(),
            model_version: "test-only-unit-costs".into(),
            observed_at_unix_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            valid_for_ms: 60000,
            quotes,
        },
    )
    .unwrap();
}

#[tokio::test]
async fn production_control_plane_to_data_plane_otlp_to_promql() {
    let control_binary = std::env::var("ASAP_E2E_CONTROL_PLANE_BIN")
        .expect("ASAP_E2E_CONTROL_PLANE_BIN is set by scripts/e2e.sh whole");
    let data_api = unused_addr();
    let otlp_http = unused_addr();
    let otlp_grpc = unused_addr();
    let control_api = unused_addr();
    let control_opamp = unused_addr();
    let control_grpc = unused_addr();

    let output_dir = tempfile::tempdir().expect("create data-plane output directory");
    let mut bootstrap = tempfile::NamedTempFile::new().expect("create bootstrap config");
    writeln!(bootstrap, "aggregations: []").expect("write bootstrap config");

    let data_child = Command::new(env!("CARGO_BIN_EXE_data_plane"))
        .arg("--streaming-config")
        .arg(bootstrap.path())
        .arg("--http-port")
        .arg(port(&data_api).to_string())
        .arg("--output-dir")
        .arg(output_dir.path())
        .arg("--enable-otel-ingest")
        .arg("--otel-http-port")
        .arg(port(&otlp_http).to_string())
        .arg("--otel-grpc-port")
        .arg(port(&otlp_grpc).to_string())
        .arg("--precompute-allowed-lateness-ms")
        .arg("0")
        .arg("--precompute-flush-interval-ms")
        .arg("100")
        .env("RUST_LOG", "data_plane=debug")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start production data plane");
    let mut data_child = ChildGuard(data_child);

    let client = reqwest::Client::new();
    let data_base = format!("http://{data_api}");
    wait_http(
        &client,
        &format!("{data_base}/api/v1/health"),
        &mut data_child.0,
        "data plane",
    )
    .await;

    let control_child = Command::new(control_binary)
        .current_dir(env!("CARGO_MANIFEST_DIR").to_string() + "/../control_plane")
        .env("CONTROLLER_ADDR", &control_api)
        .env("CONTROLLER_OPAMP_ADDR", &control_opamp)
        .env("CONTROLLER_GRPC_ADDR", &control_grpc)
        .env(
            "CONTROLLER_BACKEND_ENDPOINT",
            format!("{data_base}/api/v1/streaming-config"),
        )
        .env(
            "CONTROLLER_WORKLOADS",
            "/definitely/missing/e2e-workloads.yaml",
        )
        .env("CONTROLLER_SKETCH_DEFAULTS", "sketch_params_default.yml")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start production control plane");
    let mut control_child = ChildGuard(control_child);
    let control_base = format!("http://{control_api}");
    wait_http(
        &client,
        &format!("{control_base}/api/v1/cost-model"),
        &mut control_child.0,
        "control plane",
    )
    .await;

    let collector = tokio::spawn(apply_next_collector_plan(control_opamp.clone(), true));
    let observed_at_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock")
        .as_millis() as u64;
    let mut request = serde_json::json!({
        "queries": [{
            "query_id": "whole-process-e2e-query",
            "query_string": "quantile_over_time(0.99, whole_process_e2e_latency_ms[1s])",
            "metric": "whole_process_e2e_latency_ms",
            "window_secs": 1,
            "group_by": ["service"],
            "accuracy": {"Epsilon": 0.01},
            "evaluation_phase_ms": 0,
            "window_cost_model": {
                "implementation_id": "collector-tumbling-v1",
                "cost": {
                    "model_version": "process-e2e-v1", "workload_fingerprint": "shared-quantiles",
                    "observed_at_unix_ms": observed_at_ms, "valid_for_ms": 60000,
                    "horizon_seconds": 300.0, "cpu_cost": 1.0, "weighted_cost": 1.0,
                    "peak_memory_bytes": 4096, "network_bytes": 1024, "storage_bytes": 2048,
                    "source_scan_bytes": 0
                }
            },
            "lifecycle": {
                "evaluation_interval_ms": 1000,
                "ingestion_rate_per_second": 100.0,
                "evidence_observed_at_unix_ms": observed_at_ms,
                "evidence_valid_for_ms": 60000,
                "horizon_seconds": 300.0,
                "costs": {
                    "build": 10.0,
                    "maintenance_per_update": 0.001,
                    "read": 0.1,
                    "retention_per_second": 0.001,
                    "retirement": 1.0
                }
            }
        }],
        "collector_ids": ["whole-e2e-collector"],
        "capability_snapshot_id": "whole-e2e-capabilities",
        "evidence": {},
        "planner_revision": PLANNER_REVISION,
        "max_evidence_age_ms": 60000,
        "plan_version": 1,
        "activation_unix_ms": observed_at_ms,
        "expiry_unix_ms": null,
        "backend_compat": control_plane::physical::compiler::BACKEND_COMPAT,
        "apply_timeout_ms": 10000
    });
    let workload: serde_json::Value = serde_json::from_str(include_str!(
        "../../docs/examples/asapquery-planning-snapshot.json"
    ))
    .unwrap();
    request["data_workload"] = workload["data_workload"].clone();
    request["data_workload"]["data_ingestion_interval"]["value"] = 1_000.into();
    let mut second = request["queries"][0].clone();
    second["query_id"] = "whole-process-e2e-median".into();
    second["query_string"] = "quantile_over_time(0.5, whole_process_e2e_latency_ms[1s])".into();
    request["queries"].as_array_mut().unwrap().push(second);
    // Obtain actual compiler requirements without publishing, then provide
    // deterministic test-only quotes. The native fallback is unavailable in
    // this deployment; a cost number alone must not make it executable.
    quote_workload(&client, &control_base, &mut request).await;
    let publication_response = client
        .post(format!(
            "{control_base}/api/v1/physical-plan/compile-and-publish"
        ))
        .json(&request)
        .send()
        .await
        .expect("request physical-plan publication");
    let publication_status = publication_response.status();
    let publication_body = publication_response
        .text()
        .await
        .expect("read publication response");
    assert!(
        publication_status.is_success(),
        "controller rejected physical plan ({publication_status}): {publication_body}"
    );
    let publication: serde_json::Value =
        serde_json::from_str(&publication_body).expect("decode publication response");
    assert_eq!(
        publication["cost_comparison"]["alternatives"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert!(publication["cost_comparison"]["alternatives"][1]["unavailable_reason"].is_string());
    let (collector_plan, collector_socket) = collector.await.expect("collector task completed");
    assert_eq!(
        publication["plan_id"],
        collector_plan["envelope"]["plan_id"]
    );
    assert_eq!(publication["collector_ids"][0], "whole-e2e-collector");

    let active: serde_json::Value = client
        .get(format!("{data_base}/api/v1/streaming-config"))
        .send()
        .await
        .expect("read installed streaming config")
        .json()
        .await
        .expect("decode installed streaming config");
    assert_eq!(
        active["aggregation_count"], 1,
        "physical plan was not installed: {active}"
    );
    let installed_aggregation = active["streaming_config"]["aggregation_configs"]
        .as_object()
        .and_then(|configs| configs.values().next())
        .expect("installed aggregation details");
    let planned_alpha = installed_aggregation["parameters"]["alpha"]
        .as_f64()
        .expect("controller emitted DDSketch alpha");
    let planned_window_secs = installed_aggregation["window_size"]
        .as_u64()
        .expect("controller emitted window size");
    let physical_plan_status: serde_json::Value = client
        .get(format!("{data_base}/api/v1/physical-plan/status"))
        .send()
        .await
        .expect("read installed physical plan")
        .json()
        .await
        .expect("decode installed physical plan status");
    assert_eq!(
        physical_plan_status["materializations"]
            .as_array()
            .map(Vec::len),
        Some(1)
    );

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock");
    let window_ms = planned_window_secs * 1000;
    let window_end_ms = (now.as_millis() as u64 / window_ms) * window_ms;
    let sample_ns = window_end_ms * 1_000_000;
    // These ranks are integral under both PromQL interpolation and sketch
    // order-statistic readout; interpolation coverage is a separate contract.
    let raw_values = (1..=101).map(|value| value as f64).collect::<Vec<_>>();
    let reference_p99 = exact_quantile(&raw_values, 0.99);
    let ingestion = client
        .post(format!("http://{otlp_http}/v1/metrics"))
        .header("content-type", "application/x-protobuf")
        .body(ddsketch_export(
            "whole_process_e2e_latency_ms",
            sample_ns,
            &raw_values,
            planned_alpha,
            &collector_plan,
            1,
        ))
        .send()
        .await
        .expect("POST OTLP to production data plane");
    let status = ingestion.status();
    let body = ingestion.text().await.unwrap();
    assert!(status.is_success(), "OTLP rejected: {status}: {body}");

    let query = "quantile_over_time(0.99, whole_process_e2e_latency_ms[1s])";
    let mut last_response = serde_json::Value::Null;
    for _ in 0..50 {
        let response: serde_json::Value = client
            .get(format!("{data_base}/api/v1/query"))
            .query(&[
                ("query", query.to_string()),
                ("time", (window_end_ms as f64 / 1000.0).to_string()),
            ])
            .send()
            .await
            .expect("query production data plane")
            .json()
            .await
            .expect("decode PromQL response");
        if let Some(value) = first_scalar(&response) {
            let relative_error = (value - reference_p99).abs() / reference_p99;
            assert!(
                value.is_finite() && relative_error <= planned_alpha * 1.05,
                "backend p99 {value} differs from raw-value oracle {reference_p99}; \
                 relative_error={relative_error}, allowed={}",
                planned_alpha * 1.05
            );
            assert_eq!(
                response["data"]["result"][0]["metric"]["service"],
                "whole-e2e"
            );
            let median: serde_json::Value = client
                .get(format!("{data_base}/api/v1/query"))
                .query(&[
                    (
                        "query",
                        "quantile_over_time(0.5, whole_process_e2e_latency_ms[1s])".to_string(),
                    ),
                    ("time", (window_end_ms as f64 / 1000.0).to_string()),
                ])
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            let median_value = first_scalar(&median).expect("second consumer is warm");
            let reference = exact_quantile(&raw_values, 0.5);
            assert!(
                (median_value - reference).abs() / reference <= planned_alpha * 1.05,
                "{median}"
            );
            // A rejected successor must not replace the active generation.
            request["plan_version"] = 2.into();
            request["activation_unix_ms"] = (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
                + 1000)
                .into();
            quote_workload(&client, &control_base, &mut request).await;
            let collector = tokio::spawn(respond_next_collector_plan(collector_socket, false));
            let failed = client
                .post(format!(
                    "{control_base}/api/v1/physical-plan/compile-and-publish"
                ))
                .json(&request)
                .send()
                .await
                .unwrap();
            let failed_status = failed.status().as_u16();
            let failed_body = failed.text().await.unwrap();
            assert_eq!(failed_status, 502, "{failed_body}");
            assert!(
                failed_body.contains("injected Collector staging failure"),
                "{failed_body}"
            );
            let (rejected_plan, collector_socket) = collector.await.unwrap();
            let rejected_frame = client
                .post(format!("http://{otlp_http}/v1/metrics"))
                .header("content-type", "application/x-protobuf")
                .body(ddsketch_export(
                    "whole_process_e2e_latency_ms",
                    sample_ns,
                    &[9999.0],
                    planned_alpha,
                    &rejected_plan,
                    1,
                ))
                .send()
                .await
                .unwrap();
            assert_eq!(
                rejected_frame.status().as_u16(),
                422,
                "inactive generation frame was accepted"
            );
            let still_active: serde_json::Value = client
                .get(format!("{data_base}/api/v1/physical-plan/status"))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(
                still_active["plans"], physical_plan_status["plans"],
                "failed rollout changed active plan"
            );
            assert_eq!(
                still_active["materializations"][0]["materialization"],
                physical_plan_status["materializations"][0]["materialization"],
                "failed rollout changed the installed materialization"
            );
            let still_warm: serde_json::Value = client
                .get(format!("{data_base}/api/v1/query"))
                .query(&[
                    ("query", query.to_string()),
                    ("time", (window_end_ms as f64 / 1000.0).to_string()),
                ])
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(first_scalar(&still_warm), Some(value));
            // Retry the staged successor while queries are in flight. Each
            // request must retain a complete active snapshot through cutover.
            request["activation_unix_ms"] = (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
                + 250)
                .into();
            let reader_client = client.clone();
            let reader_base = data_base.clone();
            let readers = tokio::spawn(async move {
                for _ in 0..40 {
                    let response: serde_json::Value = reader_client
                        .get(format!("{reader_base}/api/v1/query"))
                        .query(&[
                            ("query", query.to_string()),
                            ("time", (window_end_ms as f64 / 1000.0).to_string()),
                        ])
                        .send()
                        .await
                        .unwrap()
                        .json()
                        .await
                        .unwrap();
                    assert_eq!(
                        first_scalar(&response),
                        Some(value),
                        "torn serving snapshot: {response}"
                    );
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            });
            let collector = tokio::spawn(respond_next_collector_plan(collector_socket, true));
            let activated = client
                .post(format!(
                    "{control_base}/api/v1/physical-plan/compile-and-publish"
                ))
                .json(&request)
                .send()
                .await
                .unwrap();
            let status = activated.status();
            let body = activated.text().await.unwrap();
            assert!(
                status.is_success(),
                "successful successor rejected: {status}: {body}"
            );
            let activated: serde_json::Value = serde_json::from_str(&body).unwrap();
            assert_eq!(activated["plan_version"], 2);
            let (successor, _collector_socket) = collector.await.unwrap();
            readers.await.unwrap();
            let old_frame = client
                .post(format!("http://{otlp_http}/v1/metrics"))
                .header("content-type", "application/x-protobuf")
                .body(ddsketch_export(
                    "whole_process_e2e_latency_ms",
                    sample_ns,
                    &[9999.0],
                    planned_alpha,
                    &collector_plan,
                    2,
                ))
                .send()
                .await
                .unwrap();
            assert_eq!(
                old_frame.status().as_u16(),
                422,
                "retired generation accepted a write"
            );
            let new_frame = client
                .post(format!("http://{otlp_http}/v1/metrics"))
                .header("content-type", "application/x-protobuf")
                .body(ddsketch_export(
                    "whole_process_e2e_latency_ms",
                    sample_ns + 1_000_000_000,
                    &[200.0; 101],
                    planned_alpha,
                    &successor,
                    1,
                ))
                .send()
                .await
                .unwrap();
            assert!(new_frame.status().is_success());
            let new_result: serde_json::Value = client
                .get(format!("{data_base}/api/v1/query"))
                .query(&[
                    ("query", query.to_string()),
                    ("time", ((window_end_ms + 1000) as f64 / 1000.0).to_string()),
                ])
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            assert!(
                (first_scalar(&new_result).unwrap() - 200.0).abs() <= 200.0 * planned_alpha * 1.05,
                "{new_result}"
            );
            return;
        }
        last_response = response;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let store_metrics = client
        .get(format!("{data_base}/api/v1/store/metrics"))
        .send()
        .await
        .expect("read store metrics")
        .text()
        .await
        .expect("decode store metrics");
    let schemas = client
        .get(format!("{data_base}/api/v1/db/schemas"))
        .send()
        .await
        .expect("read schemas")
        .text()
        .await
        .expect("decode schemas");
    let logs = std::fs::read_to_string(output_dir.path().join("query_engine.log"))
        .unwrap_or_else(|error| format!("unable to read data-plane log: {error}"));
    let relevant_logs = logs
        .lines()
        .filter(|line| {
            line.contains("worker")
                || line.contains("Worker")
                || line.contains("flush")
                || line.contains("CapabilityMiss")
                || line.contains("post-ASAP")
                || line.contains("modified-proto")
                || line.contains("live:")
        })
        .collect::<Vec<_>>()
        .join("\n");
    panic!(
        "whole backend never served the controller-planned, OTLP-ingested sketch\n\
         active={active}\nstore_metrics={store_metrics}\nschemas={schemas}\n\
         last_query={last_response}\nlogs={relevant_logs}"
    );
}
