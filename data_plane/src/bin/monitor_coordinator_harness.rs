//! Minimal CDM monitor-coordinator harness for the cross-language e2e
//! (deploy/mvp-multinode/scripts/monitor_e2e.sh in ASAPCollector). Runs ONLY
//! the tonic MonitorService with a single Sum monitor and prints a line to
//! stdout when the global threshold fires, then exits 0 — so a driver script
//! can assert the Go edge ⇄ Rust coordinator wire path end-to-end without
//! standing up the full data-plane.
//!
//! Usage: monitor_coordinator_harness <port> <agg_id> <tau> <window_ms> [timeout_secs]

use std::sync::Arc;

use data_plane::monitor::{AlertSink, MonitorConfig, MonitorCoordinator, MonitorServiceImpl};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let port: u16 = args.next().and_then(|s| s.parse().ok()).unwrap_or(4319);
    let agg_id: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(1);
    let tau: f64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(100.0);
    let window_ms: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(3_600_000);
    let timeout_secs: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(30);

    // Exit-on-alert: the sink prints a machine-greppable line and signals the
    // run to finish.
    let (done_tx, mut done_rx) = tokio::sync::mpsc::channel::<()>(1);
    let sink: AlertSink = Arc::new(move |v| {
        println!(
            "MONITOR_ALERT monitor={} observed={} threshold={}",
            v.agent_id, v.observed, v.threshold
        );
        let _ = done_tx.try_send(());
    });

    let cfg = MonitorConfig {
        agg_id,
        key: Vec::new(),
        tau,
        epsilon: 0.05,
        window_ms,
        ..Default::default()
    };
    let coord = MonitorCoordinator::new(vec![cfg], sink);
    let svc = MonitorServiceImpl::new(coord).into_server();
    let addr = format!("0.0.0.0:{port}").parse()?;
    eprintln!("harness: serving MonitorService on {addr} (agg_id={agg_id}, tau={tau}, window_ms={window_ms})");
    println!("HARNESS_READY {port}");

    tokio::select! {
        r = tonic::transport::Server::builder().add_service(svc).serve(addr) => {
            r?;
        }
        _ = done_rx.recv() => {
            eprintln!("harness: alert fired — exiting");
        }
        _ = tokio::time::sleep(std::time::Duration::from_secs(timeout_secs)) => {
            eprintln!("harness: timeout after {timeout_secs}s with no alert");
            std::process::exit(2);
        }
    }
    Ok(())
}
