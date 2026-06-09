//! End-to-end transport test for the CDM coordinator: a real tonic
//! `MonitorService` server on a loopback port, driven by a real
//! `MonitorServiceClient` over a bidi stream. Validates that registration +
//! reports flow over gRPC, grants come back, and the global-threshold alert
//! fires exactly once within ε when the scripted reports climb past τ — and
//! that staying below τ produces only grants, never an alert.
//!
//! The scripted "edge" here sends a fixed report sequence rather than
//! re-implementing the Go slack-countdown engine (which is unit-tested on its
//! own); the point of this test is the wire path: server ⇄ coordinator ⇄
//! dispatch ⇄ alert sink.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tokio_stream::StreamExt;
use tonic::transport::Server;

use asap_otel_proto::monitor::v1::{
    edge_to_coord, monitor_service_client::MonitorServiceClient, EdgeToCoord, MonitorRegister,
    MonitorReport,
};
use control_plane::monitor::{Violation, ViolationKind};
use data_plane::monitor::{AlertSink, MonitorConfig, MonitorCoordinator, MonitorServiceImpl};

async fn start_server(cfgs: Vec<MonitorConfig>) -> (String, Arc<Mutex<Vec<Violation>>>) {
    let alerts = Arc::new(Mutex::new(Vec::<Violation>::new()));
    let captured = alerts.clone();
    let sink: AlertSink = Arc::new(move |v| captured.lock().unwrap().push(v));

    let coord = MonitorCoordinator::new(cfgs, sink);
    let svc = MonitorServiceImpl::new(coord).into_server();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(svc)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    (format!("http://{addr}"), alerts)
}

fn report(value: f64, seq: u64) -> EdgeToCoord {
    EdgeToCoord {
        msg: Some(edge_to_coord::Msg::Report(MonitorReport {
            edge_id: "e1".into(),
            agg_id: 1,
            key: vec![],
            window_start_ms: 0,
            local_value: value,
            round: 0,
            seq,
        })),
    }
}

fn register() -> EdgeToCoord {
    EdgeToCoord {
        msg: Some(edge_to_coord::Msg::Reg(MonitorRegister {
            edge_id: "e1".into(),
            agg_id: 1,
            key: vec![],
            epoch_window_ms: 60_000,
            window_start_ms: 0,
        })),
    }
}

fn sum_cfg(tau: f64) -> MonitorConfig {
    MonitorConfig {
        agg_id: 1,
        key: vec![],
        tau,
        epsilon: 0.05,
        window_ms: 60_000,
    }
}

#[tokio::test]
async fn fires_alert_once_past_tau() {
    let (url, alerts) = start_server(vec![sum_cfg(100.0)]).await;
    let mut client = MonitorServiceClient::connect(url).await.unwrap();

    let (tx, rx) = mpsc::channel(16);
    let mut inbound = client
        .monitor(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    // Drain grants in the background so the stream stays healthy.
    tokio::spawn(async move { while let Some(Ok(_grant)) = inbound.next().await {} });

    tx.send(register()).await.unwrap();
    // Climb past τ=100. Alert fires when estimate ≥ (1-ε)τ = 95.
    for (i, v) in [20.0, 50.0, 80.0, 96.0].into_iter().enumerate() {
        tx.send(report(v, i as u64 + 1)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    let got = alerts.lock().unwrap();
    assert_eq!(
        got.len(),
        1,
        "expected exactly one alert, got {}",
        got.len()
    );
    assert_eq!(got[0].kind, ViolationKind::GlobalThresholdCrossed);
    assert_eq!(got[0].threshold, 100.0);
    assert!(
        got[0].observed >= 95.0,
        "alert fired at estimate {} which is below (1-ε)τ=95",
        got[0].observed
    );
}

#[tokio::test]
async fn stays_quiet_below_tau() {
    let (url, alerts) = start_server(vec![sum_cfg(100.0)]).await;
    let mut client = MonitorServiceClient::connect(url).await.unwrap();

    let (tx, rx) = mpsc::channel(16);
    let mut inbound = client
        .monitor(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    let grant_count = Arc::new(Mutex::new(0usize));
    let gc = grant_count.clone();
    tokio::spawn(async move {
        while let Some(Ok(_grant)) = inbound.next().await {
            *gc.lock().unwrap() += 1;
        }
    });

    tx.send(register()).await.unwrap();
    for (i, v) in [10.0, 20.0, 30.0].into_iter().enumerate() {
        tx.send(report(v, i as u64 + 1)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    assert!(
        alerts.lock().unwrap().is_empty(),
        "must not alert while below τ"
    );
    assert!(
        *grant_count.lock().unwrap() >= 1,
        "edge should have received at least the initial grant"
    );
}
