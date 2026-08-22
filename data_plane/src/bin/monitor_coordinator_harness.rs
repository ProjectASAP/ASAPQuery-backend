//! Minimal CDM monitor-coordinator harness for the cross-language e2e
//! (deploy/mvp-multinode/scripts/monitor_e2e.sh in ASAPCollector). Runs the
//! tonic MonitorService with a single monitor and prints a line to stdout once
//! the coordinator has computed a REAL coordinated-sampling grant for the
//! connecting edge (i.e. the edge's periodic rate report reached the
//! coordinator and a non-trivial sample_p came back), then exits 0 — so a
//! driver script can assert the Go edge ⇄ Rust coordinator wire path
//! end-to-end without standing up the full data-plane.
//!
//! Global-threshold alerting is retired (see `data_plane::monitor` module
//! docs) — this harness no longer waits for or reports an alert.
//!
//! Usage: monitor_coordinator_harness <port> <agg_id> <tau> <window_ms> [timeout_secs] [key]
//!
//! `tau` is accepted for CLI/back-compat but unused. `key` is the monitor's
//! group key (the edge's canonical `k=v;k2=v2` label encoding). Scalar (Sum)
//! monitors register under their series-group key, so the harness config must
//! carry the same key or registrations are rejected as unconfigured. Empty
//! (default) = ungrouped.

use data_plane::monitor::{MonitorConfig, MonitorCoordinator, MonitorServiceImpl};

/// The edge_id the Go e2edriver connects as (asap-precompute-go/monitor/
/// grpcclient/cmd/e2edriver/main.go: `monitor.NewEngine("e2e-edge", ...)`).
const E2E_EDGE_ID: &str = "e2e-edge";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let port: u16 = args.next().and_then(|s| s.parse().ok()).unwrap_or(4319);
    let agg_id: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(1);
    let tau: f64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(100.0);
    let window_ms: u64 = args
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3_600_000);
    let timeout_secs: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(30);
    let key: Vec<u8> = args.next().map(|s| s.into_bytes()).unwrap_or_default();

    let cfg = MonitorConfig {
        agg_id,
        key: key.clone(),
        tau,
        epsilon: 0.05,
        window_ms,
        ..Default::default()
    };
    let coord = MonitorCoordinator::new(vec![cfg]);
    let svc = MonitorServiceImpl::new(coord.clone()).into_server();
    let addr = format!("0.0.0.0:{port}").parse()?;
    eprintln!("harness: serving MonitorService on {addr} (agg_id={agg_id}, window_ms={window_ms})");
    println!("HARNESS_READY {port}");

    // Poll for a real coordinated-sampling grant reaching this edge — the
    // proof that the edge's periodic rate report made the live gRPC round
    // trip and the coordinator answered with a computed sample_p.
    let poll = {
        let coord = coord.clone();
        let key = key.clone();
        async move {
            loop {
                if let Some(p) = coord.granted_sample_p(agg_id, &key, E2E_EDGE_ID).await {
                    println!("MONITOR_GRANT edge={E2E_EDGE_ID} agg_id={agg_id} sample_p={p}");
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    };

    tokio::select! {
        r = tonic::transport::Server::builder().add_service(svc).serve(addr) => {
            r?;
        }
        _ = poll => {
            eprintln!("harness: coordinated-sampling grant observed — exiting");
        }
        _ = tokio::time::sleep(std::time::Duration::from_secs(timeout_secs)) => {
            eprintln!("harness: timeout after {timeout_secs}s with no grant");
            std::process::exit(2);
        }
    }
    Ok(())
}
