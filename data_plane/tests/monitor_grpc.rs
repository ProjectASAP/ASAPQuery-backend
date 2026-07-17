//! End-to-end transport test for the coordinated-sampling coordinator: a real
//! tonic `MonitorService` server on a loopback port, driven by a real
//! `MonitorServiceClient` over a bidi stream. Validates that registration +
//! rate reports flow over gRPC and the coordinator ships back a computed
//! `SlackGrant.sample_p`.
//!
//! Global-threshold alerting is retired (see `data_plane::monitor` module
//! docs) — there is no more alert path to test here. The scripted "edge" here
//! sends a fixed report sequence rather than re-implementing the Go engine's
//! reportEveryN cadence (which is unit-tested on its own); the point of this
//! test is the wire path: server ⇄ coordinator ⇄ dispatch ⇄ grant.

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
use data_plane::monitor::{MonitorConfig, MonitorCoordinator, MonitorServiceImpl};

async fn start_server(cfgs: Vec<MonitorConfig>) -> String {
    let coord = MonitorCoordinator::new(cfgs);
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
    format!("http://{addr}")
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

fn report_edge(edge_id: &str, seq: u64, rate: f64) -> EdgeToCoord {
    EdgeToCoord {
        msg: Some(edge_to_coord::Msg::Report(MonitorReport {
            edge_id: edge_id.into(),
            agg_id: 1,
            key: vec![],
            window_start_ms: 0,
            local_value: 0.0, // vestigial (alerting retired) — unused by the coordinator
            round: 0,
            seq,
            rate,
        })),
    }
}

fn sum_cfg(epsilon: f64) -> MonitorConfig {
    MonitorConfig {
        agg_id: 1,
        key: vec![],
        tau: 0.0, // vestigial (alerting retired)
        epsilon,
        window_ms: 60_000,
        ..Default::default()
    }
}

async fn connect_edge(url: &str) -> (mpsc::Sender<EdgeToCoord>, Arc<Mutex<Vec<f64>>>) {
    use asap_otel_proto::monitor::v1::coord_to_edge;

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

/// A single edge reporting a real rate gets a real (< 1.0) sample_p grant —
/// the retired countdown special-cased "<2 edges ⇒ p=1" purely for its
/// alert-fire safety proof; the ε-floor sampling law never depended on edge
/// count, so a lone edge is sampled exactly like any other now.
#[tokio::test]
async fn single_edge_gets_sampled_over_grpc() {
    let url = start_server(vec![sum_cfg(0.05)]).await;
    let (tx, p) = connect_edge(&url).await;

    tx.send(register_edge("e1")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;
    tx.send(report_edge("e1", 1, 100_000.0)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;

    let got = *p.lock().unwrap().last().expect("edge received a grant");
    assert!(got > 0.0 && got < 1.0, "expected a real sampling grant, got {got}");
}

/// Live multi-edge coupling: two edges open real gRPC streams, report SKEWED
/// rates, and the coordinator ships back differentiated `SlackGrant.sample_p`
/// over the wire — the hot edge sampled harder (smaller p) than the quiet one,
/// both at/above the ε-derived coupling floor.
#[tokio::test]
async fn two_edges_get_differentiated_sample_p_over_grpc() {
    let url = start_server(vec![sum_cfg(0.05)]).await;
    let (tx_hot, p_hot) = connect_edge(&url).await;
    let (tx_quiet, p_quiet) = connect_edge(&url).await;

    tx_hot.send(register_edge("e1")).await.unwrap();
    tx_quiet.send(register_edge("e2")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;

    // e1 hot (100k items/win), e2 quiet (1k/win).
    for seq in 1..=4 {
        tx_hot.send(report_edge("e1", seq, 100_000.0)).await.unwrap();
        tx_quiet.send(report_edge("e2", seq, 1_000.0)).await.unwrap();
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

/// A report answers ONLY the reporting edge — unlike the retired countdown
/// (which re-broadcast to every edge on any report, since slack sizing
/// depended on the full edge set), a quiet edge that never reports should
/// never receive a grant it didn't ask for.
#[tokio::test]
async fn report_grants_only_the_reporting_edge_over_grpc() {
    let url = start_server(vec![sum_cfg(0.05)]).await;
    let (tx_a, p_a) = connect_edge(&url).await;
    let (tx_b, p_b) = connect_edge(&url).await;

    tx_a.send(register_edge("e1")).await.unwrap();
    tx_b.send(register_edge("e2")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;

    tx_a.send(report_edge("e1", 1, 50_000.0)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;

    assert!(!p_a.lock().unwrap().is_empty(), "e1 should have received its own grant");
    assert!(p_b.lock().unwrap().is_empty(), "e2 must not receive a grant it never reported for");
}
