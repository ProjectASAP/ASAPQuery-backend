//! Process-level E2E for the production monitor coordinator.
//!
//! Two simulated edges connect to the monitor listener hosted by the real
//! data-plane executable, register, report different rates, and receive
//! differentiated sampling grants over bidirectional gRPC streams.

use std::io::Write;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use asap_otel_proto::monitor::v1::{
    coord_to_edge, edge_to_coord, monitor_service_client::MonitorServiceClient, EdgeToCoord,
    MonitorRegister, MonitorReport,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn unused_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve loopback port");
    listener.local_addr().expect("loopback address").port()
}

async fn connect_edge(
    port: u16,
    edge_id: &str,
) -> (
    mpsc::Sender<EdgeToCoord>,
    tonic::Streaming<asap_otel_proto::monitor::v1::CoordToEdge>,
) {
    let endpoint = format!("http://127.0.0.1:{port}");
    let mut connected = None;
    for _ in 0..100 {
        match MonitorServiceClient::connect(endpoint.clone()).await {
            Ok(client) => {
                connected = Some(client);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
    let mut client = connected.unwrap_or_else(|| {
        panic!("edge could not connect to production monitor endpoint {endpoint}")
    });
    let (tx, rx) = mpsc::channel(16);
    let inbound = client
        .monitor(ReceiverStream::new(rx))
        .await
        .expect("open monitor stream")
        .into_inner();
    tx.send(EdgeToCoord {
        msg: Some(edge_to_coord::Msg::Reg(MonitorRegister {
            edge_id: edge_id.into(),
            agg_id: 1,
            key: Vec::new(),
            epoch_window_ms: 60_000,
            window_start_ms: 0,
        })),
    })
    .await
    .expect("register edge");
    (tx, inbound)
}

async fn report_and_receive(
    tx: &mpsc::Sender<EdgeToCoord>,
    inbound: &mut tonic::Streaming<asap_otel_proto::monitor::v1::CoordToEdge>,
    edge_id: &str,
    rate: f64,
) -> f64 {
    tx.send(EdgeToCoord {
        msg: Some(edge_to_coord::Msg::Report(MonitorReport {
            edge_id: edge_id.into(),
            agg_id: 1,
            key: Vec::new(),
            window_start_ms: 0,
            local_value: 0.0,
            round: 0,
            seq: 1,
            rate,
        })),
    })
    .await
    .expect("send monitor report");

    let message = tokio::time::timeout(Duration::from_secs(5), inbound.next())
        .await
        .expect("timed out waiting for sampling grant")
        .expect("monitor stream ended")
        .expect("receive sampling grant");
    match message.msg.expect("coordinator response payload") {
        coord_to_edge::Msg::Grant(grant) => grant.sample_p,
        other => panic!("expected sampling grant, got {other:?}"),
    }
}

#[tokio::test]
async fn production_coordinator_differentiates_edge_sampling_grants() {
    let query_port = unused_port();
    let monitor_port = unused_port();
    let output_dir = tempfile::tempdir().expect("create output directory");
    let mut config = tempfile::NamedTempFile::new().expect("create monitor config");
    write!(
        config,
        "aggregations: []\nmonitors:\n  - agg_id: 1\n    key: ''\n    tau: 5000.0\n    epsilon: 0.05\n    window_ms: 60000\n"
    )
    .expect("write monitor config");

    let child = Command::new(env!("CARGO_BIN_EXE_data_plane"))
        .arg("--streaming-config")
        .arg(config.path())
        .arg("--http-port")
        .arg(query_port.to_string())
        .arg("--output-dir")
        .arg(output_dir.path())
        .arg("--enable-monitor-coordinator")
        .arg("--monitor-grpc-port")
        .arg(monitor_port.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start production data-plane binary");
    let _child = ChildGuard(child);

    let (hot_tx, mut hot_inbound) = connect_edge(monitor_port, "hot-edge").await;
    let (quiet_tx, mut quiet_inbound) = connect_edge(monitor_port, "quiet-edge").await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let (hot, quiet) = tokio::join!(
        report_and_receive(&hot_tx, &mut hot_inbound, "hot-edge", 100_000.0),
        report_and_receive(&quiet_tx, &mut quiet_inbound, "quiet-edge", 1_000.0),
    );
    assert!(hot > 0.0 && hot < quiet, "hot={hot}, quiet={quiet}");
    assert!(quiet <= 1.0, "quiet grant out of range: {quiet}");
}
