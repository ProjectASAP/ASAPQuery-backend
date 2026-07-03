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
            rate: 0.0,
            sketch: Vec::new(),
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
        ..Default::default()
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

fn register_edge(edge_id: &str) -> EdgeToCoord {
    EdgeToCoord {
        msg: Some(edge_to_coord::Msg::Reg(MonitorRegister {
            edge_id: edge_id.into(),
            agg_id: 1,
            key: vec![],
            epoch_window_ms: 60_000,
            window_start_ms: 0,
        })),
    }
}

fn report_edge(edge_id: &str, value: f64, seq: u64, rate: f64) -> EdgeToCoord {
    EdgeToCoord {
        msg: Some(edge_to_coord::Msg::Report(MonitorReport {
            edge_id: edge_id.into(),
            agg_id: 1,
            key: vec![],
            window_start_ms: 0,
            local_value: value,
            round: 0,
            seq,
            rate,
            sketch: Vec::new(),
        })),
    }
}

/// Live multi-edge coupling: two edges open real gRPC streams, report SKEWED
/// rates, and the coordinator ships back differentiated `SlackGrant.sample_p`
/// over the wire — the hot edge sampled harder (smaller p) than the quiet one,
/// both at/above the ε-derived coupling floor. τ is large so only grants flow.
#[tokio::test]
async fn two_edges_get_differentiated_sample_p_over_grpc() {
    use asap_otel_proto::monitor::v1::coord_to_edge;

    async fn connect_edge(url: &str) -> (mpsc::Sender<EdgeToCoord>, Arc<Mutex<Vec<f64>>>) {
        let mut client = MonitorServiceClient::connect(url.to_string()).await.unwrap();
        let (tx, rx) = mpsc::channel(32);
        let mut inbound = client
            .monitor(ReceiverStream::new(rx))
            .await
            .unwrap()
            .into_inner();
        let ps = Arc::new(Mutex::new(Vec::<f64>::new()));
        let psc = ps.clone();
        tokio::spawn(async move {
            while let Some(Ok(env)) = inbound.next().await {
                if let Some(coord_to_edge::Msg::Grant(g)) = env.msg {
                    psc.lock().unwrap().push(g.sample_p);
                }
            }
        });
        (tx, ps)
    }

    let (url, _alerts) = start_server(vec![sum_cfg(1_000_000.0)]).await;
    let (tx_hot, p_hot) = connect_edge(&url).await;
    let (tx_quiet, p_quiet) = connect_edge(&url).await;

    tx_hot.send(register_edge("e1")).await.unwrap();
    tx_quiet.send(register_edge("e2")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;

    // e1 hot (100k items/win), e2 quiet (1k/win); small local values stay far
    // below τ so only grants (carrying sample_p) flow, never an alert.
    for seq in 1..=4 {
        tx_hot.send(report_edge("e1", 1.0, seq, 100_000.0)).await.unwrap();
        tx_quiet.send(report_edge("e2", 1.0, seq, 1_000.0)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    tokio::time::sleep(Duration::from_millis(250)).await;

    let hot = *p_hot.lock().unwrap().last().expect("hot edge received a grant");
    let quiet = *p_quiet.lock().unwrap().last().expect("quiet edge received a grant");
    println!("LIVE coupling over gRPC: hot(e1,rate=100k) sample_p={hot} | quiet(e2,rate=1k) sample_p={quiet}");

    assert!(
        hot < quiet,
        "hot edge sample_p {hot} should be < quiet edge sample_p {quiet}"
    );
    assert!(hot > 0.0 && hot <= 1.0, "hot p out of range: {hot}");
    assert!(quiet > 0.0 && quiet <= 1.0, "quiet p out of range: {quiet}");
}
