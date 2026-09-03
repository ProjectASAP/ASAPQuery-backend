//! Black-box component E2E for the production control-plane binary.
//!
//! A simulated collector connects to the production OpAMP WebSocket, a real
//! workload is planned through the public HTTP API, and the emitted collector
//! YAML is received over OpAMP. The same child process then accepts a runtime
//! sample over its production gRPC service and exposes the accepted record in
//! Prometheus metrics.

use futures_util::StreamExt;
use prost::Message;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

use control_plane::opamp::opamp_proto::ServerToAgent;
use control_plane::runtime_samples::feedback::{
    runtime_samples_client::RuntimeSamplesClient, PushBatch, RuntimeRecord,
};

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn unused_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve loopback port");
    let addr = listener.local_addr().expect("read loopback address");
    drop(listener);
    addr.to_string()
}

async fn wait_until_ready(client: &reqwest::Client, url: &str, child: &mut Child) {
    for _ in 0..100 {
        if let Some(status) = child.try_wait().expect("inspect control-plane process") {
            panic!("control-plane exited before readiness: {status}");
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
    panic!("control-plane did not become ready at {url}");
}

async fn connect_agent(
    address: &str,
    child: &mut Child,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    for _ in 0..100 {
        if let Some(status) = child.try_wait().expect("inspect control-plane process") {
            panic!("control-plane exited before OpAMP connection: {status}");
        }
        let mut request = format!("ws://{address}/v1/opamp")
            .into_client_request()
            .expect("build OpAMP request");
        request
            .headers_mut()
            .insert("X-Agent-ID", "process-e2e-agent".parse().unwrap());
        request
            .headers_mut()
            .insert("X-Agent-Role", "agent".parse().unwrap());
        if let Ok((stream, _)) = tokio_tungstenite::connect_async(request).await {
            return stream;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("production OpAMP listener did not accept a collector connection");
}

async fn push_runtime_sample(address: &str) {
    let endpoint = format!("http://{address}");
    let mut connected = None;
    for _ in 0..100 {
        match RuntimeSamplesClient::connect(endpoint.clone()).await {
            Ok(client) => {
                connected = Some(client);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
    let mut client = connected
        .unwrap_or_else(|| panic!("could not connect to production runtime service {endpoint}"));
    let response = client
        .push(PushBatch {
            records: vec![RuntimeRecord {
                source: "process-e2e-agent".into(),
                sketch: "ddsketch".into(),
                impl_name: "rust".into(),
                schema_version: 1,
                payload_json: serde_json::json!({
                    "schema_version": 1,
                    "bench": {"throughput_items_per_sec": {"mean": 42000.0}}
                })
                .to_string(),
            }],
        })
        .await
        .expect("push runtime sample to production gRPC service")
        .into_inner();
    assert_eq!(response.accepted, 1);
}

#[tokio::test]
async fn production_binary_plans_pushes_opamp_config_and_ingests_feedback() {
    let api_addr = unused_addr();
    let opamp_addr = unused_addr();
    let grpc_addr = unused_addr();

    let child = Command::new(env!("CARGO_BIN_EXE_control_plane"))
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env("CONTROLLER_ADDR", &api_addr)
        .env("CONTROLLER_OPAMP_ADDR", &opamp_addr)
        .env("CONTROLLER_GRPC_ADDR", &grpc_addr)
        .env(
            "CONTROLLER_WORKLOADS",
            "/definitely/missing/e2e-workloads.yaml",
        )
        .env("CONTROLLER_SKETCH_DEFAULTS", "sketch_params_default.yml")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start production control-plane binary");
    let mut child = ChildGuard(child);

    let client = reqwest::Client::new();
    let base = format!("http://{api_addr}");
    wait_until_ready(&client, &format!("{base}/api/v1/cost-model"), &mut child.0).await;
    let mut agent = connect_agent(&opamp_addr, &mut child.0).await;

    let response = client
        .post(format!("{base}/api/v1/plan"))
        .json(&serde_json::json!({
            "metric_name": "component_process_e2e_latency_ms",
            "aggregations": ["quantile"],
            "time_window": "1m",
            "accuracy_sla": 0.01
        }))
        .send()
        .await
        .expect("POST workload to production control plane");
    assert!(
        response.status().is_success(),
        "plan status: {}",
        response.status()
    );
    let body: serde_json::Value = response.json().await.expect("decode plan response");
    assert_eq!(body["metric"], "component_process_e2e_latency_ms");
    assert!(body["sketch_type"].as_str().is_some());
    assert!(body["valid_until"].as_str().is_some());
    assert_eq!(body["agents_notified"], 1);

    let frame = tokio::time::timeout(Duration::from_secs(5), agent.next())
        .await
        .expect("timed out waiting for OpAMP configuration")
        .expect("OpAMP connection closed")
        .expect("read OpAMP frame");
    let data = match frame {
        tokio_tungstenite::tungstenite::Message::Binary(data) => data,
        other => panic!("expected binary OpAMP frame, got {other:?}"),
    };
    let payload = if data.first() == Some(&0) {
        &data[1..]
    } else {
        &data
    };
    let message = ServerToAgent::decode(payload).expect("decode OpAMP ServerToAgent");
    let config = message
        .remote_config
        .and_then(|remote| remote.config)
        .expect("OpAMP response contains remote config");
    let yaml = String::from_utf8(
        config
            .config_map
            .get("")
            .expect("default OpAMP config file")
            .body
            .clone(),
    )
    .expect("collector config is UTF-8 YAML");
    let planned_sketch = body["sketch_type"]
        .as_str()
        .expect("plan contains sketch type")
        .to_ascii_lowercase();
    assert!(
        yaml.to_ascii_lowercase().contains(&planned_sketch)
            && yaml.contains("otlp/backend")
            && yaml.contains("service:"),
        "OpAMP YAML does not implement the selected {planned_sketch} plan:\n{yaml}"
    );

    push_runtime_sample(&grpc_addr).await;
    for _ in 0..50 {
        let metrics = client
            .get(format!("{base}/metrics"))
            .send()
            .await
            .expect("GET production metrics")
            .text()
            .await
            .expect("read production metrics");
        if metrics.contains("asap_runtime_samples_records_stored_total 1") {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("runtime sample was accepted but never surfaced in /metrics");
}
