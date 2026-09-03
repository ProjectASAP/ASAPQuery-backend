//! Black-box component E2E for the production control-plane binary.
//!
//! Unlike the router-level tests in `src/main.rs`, this test exercises CLI
//! startup, all three production listeners, TCP/HTTP transport, JSON decoding,
//! planning, and shutdown as a separate OS process.

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

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

#[tokio::test]
async fn production_binary_serves_cost_model_and_plans_a_workload() {
    let api_addr = unused_addr();
    let opamp_addr = unused_addr();
    let grpc_addr = unused_addr();

    let child = Command::new(env!("CARGO_BIN_EXE_control_plane"))
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env("CONTROLLER_ADDR", &api_addr)
        .env("CONTROLLER_OPAMP_ADDR", opamp_addr)
        .env("CONTROLLER_GRPC_ADDR", grpc_addr)
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
}
