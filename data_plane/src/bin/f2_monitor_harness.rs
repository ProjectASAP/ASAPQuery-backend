//! F2 / geometric distributed-monitoring coordinator harness for the
//! cross-language eval (deploy/mvp-multinode/scripts/f2_monitor_eval.sh).
//! Stands up the tonic `MonitorService` with a single whole-sketch F2 monitor in
//! either `distributed` or `geometric` mode, prints `MONITOR_ALERT` when the
//! global F2 crosses τ, and periodically (and on exit) prints an `F2_COMM` line
//! with the communication accounting so the eval can compare the two modes.
//!
//! Usage: f2_monitor_harness <port> <mode> <agg_id> <tau> <epsilon> <d> <w> <window_ms> [timeout_secs]
//!   mode = distributed | geometric

use std::sync::Arc;

use data_plane::monitor::{
    AlertSink, F2Mode, Functional, MonitorConfig, MonitorCoordinator, MonitorServiceImpl,
};

fn arg<T: std::str::FromStr>(args: &mut std::env::Args, default: T) -> T {
    args.next().and_then(|s| s.parse().ok()).unwrap_or(default)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args();
    let _ = args.next(); // argv[0]
    let port: u16 = arg(&mut args, 4320);
    let mode_s: String = arg(&mut args, "distributed".to_string());
    let agg_id: u64 = arg(&mut args, 1);
    let tau: f64 = arg(&mut args, 1_000_000.0);
    let epsilon: f64 = arg(&mut args, 0.1);
    let d: usize = arg(&mut args, 5);
    let w: usize = arg(&mut args, 256);
    let window_ms: u64 = arg(&mut args, 3_600_000);
    let timeout_secs: u64 = arg(&mut args, 30);
    let mode = F2Mode::from_name(&mode_s);

    let sink: AlertSink = Arc::new(move |v| {
        println!(
            "MONITOR_ALERT monitor={} observed={} threshold={}",
            v.agent_id, v.observed, v.threshold
        );
    });

    let cfg = MonitorConfig {
        agg_id,
        key: Vec::new(),
        tau,
        epsilon,
        window_ms,
        functional: Functional::F2,
        f2_d: d,
        f2_w: w,
        f2_mode: mode,
    };
    let coord = MonitorCoordinator::new(vec![cfg], sink);
    let svc = MonitorServiceImpl::new(coord.clone()).into_server();
    let addr = format!("0.0.0.0:{port}").parse()?;
    eprintln!(
        "f2_harness: MonitorService on {addr} (mode={mode_s} agg_id={agg_id} tau={tau} eps={epsilon} d={d} w={w})"
    );
    println!("HARNESS_READY {port}");

    // Periodic + final F2 communication accounting.
    {
        let coord = coord.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_millis(500));
            loop {
                tick.tick().await;
                let (bin, bout, ships, refs) = coord.f2_comm();
                println!(
                    "F2_COMM mode={mode_s} bytes_in={bin} bytes_out={bout} ships={ships} refs={refs} total_bytes={}",
                    bin + bout
                );
            }
        });
    }

    tokio::select! {
        r = tonic::transport::Server::builder().add_service(svc).serve(addr) => { r?; }
        _ = tokio::time::sleep(std::time::Duration::from_secs(timeout_secs)) => {
            let (bin, bout, ships, refs) = coord.f2_comm();
            println!(
                "F2_COMM_FINAL bytes_in={bin} bytes_out={bout} ships={ships} refs={refs} total_bytes={}",
                bin + bout
            );
            eprintln!("f2_harness: timeout after {timeout_secs}s — exiting");
        }
    }
    Ok(())
}
