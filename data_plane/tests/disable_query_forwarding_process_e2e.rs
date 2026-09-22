//! The production CLI's no-forwarding mode keeps query traffic inside the backend.

#[path = "support/empty_physical_plan.rs"]
mod empty_physical_plan;

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use axum::{routing::get, Router};

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn unused_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[tokio::test]
async fn cli_mode_blocks_instant_and_range_forwarding() {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream = format!("http://{}", listener.local_addr().unwrap());
    let capture = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route(
                    "/api/v1/query",
                    get({
                        let count = count.clone();
                        move || async move {
                            count.fetch_add(1, Ordering::SeqCst);
                            "unexpected instant query"
                        }
                    }),
                )
                .route(
                    "/api/v1/query_range",
                    get(move || async move {
                        count.fetch_add(1, Ordering::SeqCst);
                        "unexpected range query"
                    }),
                ),
        )
        .await
        .unwrap();
    });

    let mut config = tempfile::NamedTempFile::new().unwrap();
    serde_json::to_writer(&mut config, &empty_physical_plan::empty()).unwrap();
    let output = tempfile::tempdir().unwrap();
    let port = unused_port();
    let mut child = ChildGuard(
        Command::new(env!("CARGO_BIN_EXE_data_plane"))
            .arg("--physical-plan")
            .arg(config.path())
            .args([
                "--disable-query-forwarding",
                "--prometheus-server",
                &upstream,
            ])
            .args(["--http-port", &port.to_string()])
            .arg("--output-dir")
            .arg(output.path())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");
    let mut ready = false;
    for _ in 0..100 {
        if let Ok(response) = client.get(format!("{base}/api/v1/health")).send().await {
            if response.status().is_success() {
                ready = true;
                break;
            }
        }
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "backend exited before ready"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(ready, "backend did not become ready");

    let instant = client
        .get(format!("{base}/api/v1/query"))
        .query(&[("query", "rate(unplanned_metric[5m])")])
        .send()
        .await
        .unwrap();
    assert_eq!(instant.status(), reqwest::StatusCode::OK);
    assert_eq!(
        instant.json::<serde_json::Value>().await.unwrap()["status"],
        "error"
    );

    let range = client
        .get(format!("{base}/api/v1/query_range"))
        .query(&[
            ("query", "rate(unplanned_metric[5m])"),
            ("start", "100"),
            ("end", "200"),
            ("step", "15"),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(range.status(), reqwest::StatusCode::OK);
    assert_eq!(
        range.json::<serde_json::Value>().await.unwrap()["status"],
        "error"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    capture.abort();
}
