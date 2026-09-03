//! Black-box component E2E for the production data-plane binary.
//!
//! This catches failures that in-process `HttpServer` tests cannot: CLI
//! parsing, file-based bootstrap configuration, logging setup, production
//! object wiring, TCP binding, and the public diagnostic HTTP contract.

use std::io::Write;
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

fn unused_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve loopback port");
    let port = listener.local_addr().expect("read loopback address").port();
    drop(listener);
    port
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
async fn production_binary_loads_config_and_serves_diagnostics() {
    let query_port = unused_port();
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
      relativeAccuracy: 0.01
    windowSize: 60
    windowType: tumbling
    spatialFilter: ''
"#
    )
    .expect("write streaming config");

    let child = Command::new(env!("CARGO_BIN_EXE_data_plane"))
        .arg("--streaming-config")
        .arg(config.path())
        .arg("--http-port")
        .arg(query_port.to_string())
        .arg("--output-dir")
        .arg(output_dir.path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start production data-plane binary");
    let mut child = ChildGuard(child);

    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{query_port}");
    wait_until_ready(&client, &format!("{base}/api/v1/health"), &mut child.0).await;

    let health = client
        .get(format!("{base}/api/v1/health"))
        .send()
        .await
        .expect("GET health")
        .text()
        .await
        .expect("read health body");
    assert_eq!(health, "ok");

    let config_response: serde_json::Value = client
        .get(format!("{base}/api/v1/streaming-config"))
        .send()
        .await
        .expect("GET installed streaming config")
        .json()
        .await
        .expect("decode streaming-config response");
    assert_eq!(config_response["status"], "success");
    assert_eq!(config_response["aggregation_count"], 1);
}
