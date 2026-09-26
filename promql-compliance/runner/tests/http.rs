use promql_compliance::{
    input::{Dataset, Suite},
    runner, transport,
};
use prost::Message;
use serde_json::{json, Value};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

async fn server(
    responses: Vec<(String, String)>,
) -> (String, tokio::task::JoinHandle<Vec<Vec<u8>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let handle = tokio::spawn(async move {
        let mut requests = vec![];
        for (headers, body) in responses {
            let (mut stream, _) =
                tokio::time::timeout(std::time::Duration::from_secs(5), listener.accept())
                    .await
                    .unwrap()
                    .unwrap();
            let mut request = vec![];
            let mut buffer = [0u8; 4096];
            loop {
                let count = tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    stream.read(&mut buffer),
                )
                .await
                .unwrap()
                .unwrap();
                assert!(count > 0);
                request.extend_from_slice(&buffer[..count]);
                if let Some(end) = request.windows(4).position(|p| p == b"\r\n\r\n") {
                    let text = String::from_utf8_lossy(&request[..end]).to_ascii_lowercase();
                    let length = text
                        .lines()
                        .find_map(|l| l.strip_prefix("content-length:"))
                        .map(|n| n.trim().parse::<usize>().unwrap())
                        .unwrap_or(0);
                    if request.len() >= end + 4 + length {
                        break;
                    }
                }
            }
            requests.push(request);
            stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{}\r\n{}",body.len(),headers,body).as_bytes()).await.unwrap();
        }
        requests
    });
    (url, handle)
}

/// Real HTTP encoding carries identical Snappy protobuf bytes to each target.
#[tokio::test]
async fn remote_write_http_headers_payload_and_drain() {
    let data=Dataset::parse("name: tiny\nseries: [{metric: m, labels: {job: api}, samples: [{offset_seconds: 1, value: 3}]}]").unwrap();
    let body = transport::encode(&data, 1000).unwrap();
    let (url, server) = server(vec![(String::new(), "{}".into()); 3]).await;
    let client = transport::client().unwrap();
    transport::push(&client, &body, &[&url, &url])
        .await
        .unwrap();
    transport::drain(&client, &url).await.unwrap();
    let requests = server.await.unwrap();
    for request in &requests[..2] {
        let end = request.windows(4).position(|p| p == b"\r\n\r\n").unwrap();
        let headers = String::from_utf8_lossy(&request[..end]).to_ascii_lowercase();
        assert!(headers.contains("post /api/v1/write"));
        assert!(headers.contains("content-encoding: snappy"));
        assert!(headers.contains("x-prometheus-remote-write-version: 0.1.0"));
        assert_eq!(&request[end + 4..], body);
        let raw = snap::raw::Decoder::new()
            .decompress_vec(&request[end + 4..])
            .unwrap();
        let decoded = transport::WriteRequest::decode(raw.as_slice()).unwrap();
        assert_eq!(decoded.timeseries[0].samples[0].timestamp, 2000);
    }
    assert!(String::from_utf8_lossy(&requests[2]).contains("/api/v1/precompute/drain"));
}

/// Differential reports retain raw responses, range parity and forwarding failures.
#[tokio::test]
async fn differential_http_report_preserves_semantics_and_provenance() {
    let suite=Suite::parse("name: s\nqueries: [{name: q, expr: m, instant_offsets_seconds: [1], range: {start_offset_seconds: 1, end_offset_seconds: 2, step_seconds: 1}}]").unwrap();
    for (headers, source) in [
        (
            "X-ASAP-Execution: warm\r\nX-ASAP-Execution-Detail: asap\r\n",
            "asap_query",
        ),
        ("", "prometheus_fallback"),
        (
            "X-ASAP-Execution: hybrid\r\nX-ASAP-Execution-Detail: asap\r\n",
            "hybrid",
        ),
        (
            "X-ASAP-Execution: failed\r\nX-ASAP-Data-Source: asap_query\r\n",
            "prometheus_fallback",
        ),
        ("X-ASAP-Execution: warm\r\n", "prometheus_fallback"),
    ] {
        let range=json!({"status":"success","data":{"resultType":"matrix","result":[{"metric":{"job":"api"},"values":[[1,"3"],[2,"4"]]}]}}).to_string();
        let instant=json!({"status":"success","data":{"resultType":"vector","result":[{"metric":{"job":"api"},"value":[1,"3"]}]}}).to_string();
        let (url, server) = server(vec![
            (String::new(), range.clone()),
            (headers.into(), range),
            (String::new(), instant.clone()),
            (headers.into(), instant),
        ])
        .await;
        let result =
            runner::compare_suite(&transport::client().unwrap(), &url, &url, &suite, "d", 0).await;
        assert_eq!(result["passed"], source == "asap_query");
        assert_eq!(
            result["queries"][0]["referenceParity"][0]["comparison"]["passed"],
            true
        );
        assert_eq!(
            result["queries"][0]["instant"][0]["responses"]["backend"]["servedBy"],
            source
        );
        let requests = server.await.unwrap();
        assert!(String::from_utf8_lossy(&requests[0]).contains("start=1.000"));
        assert!(String::from_utf8_lossy(&requests[2]).contains("time=1.000"));
    }
}

/// CLI failures produce an artifact before launching any external services.
#[test]
fn cli_preserves_planning_failure_without_docker() {
    let directory = tempfile::tempdir().unwrap();
    let suite = directory.path().join("bad.yaml");
    let output = directory.path().join("report.json");
    std::fs::write(
        &suite,
        "name: broken\nqueries: [{name: q, expr: '(', instant_offsets_seconds: [1]}]",
    )
    .unwrap();
    let result = std::process::Command::new(env!("CARGO_BIN_EXE_differential-runner"))
        .args(["--dataset", "../datasets/single-rate.yaml", "--suite"])
        .arg(suite)
        .arg("--output")
        .arg(&output)
        .arg("--compose-file")
        .arg("nonexistent-compose.yaml")
        .output()
        .unwrap();
    assert!(!result.status.success());
    let report: Value = serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
    assert_eq!(report["passed"], false);
    assert!(report["error"]
        .as_str()
        .unwrap()
        .contains("compile unquoted workload snapshot"));
}
