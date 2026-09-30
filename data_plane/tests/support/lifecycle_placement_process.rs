//! The summary-store price, not an enumerated placement, decides whether a
//! state is precomputed at ingestion or rebuilt from raw series at query time.
use super::*;
use control_plane::physical::compiler::BackendLocalPlanningInput;

const QUERY: &str = "quantile_over_time(0.99, m[1m])";
const RAW_SELECTOR: &str = r#"{__name__="m"}[60000ms]"#;

struct Deployment {
    backend: String,
    requests: Arc<Mutex<Vec<HashMap<String, String>>>>,
    _child: ChildGuard,
    _output: tempfile::TempDir,
    prometheus: tokio::task::JoinHandle<()>,
}

/// Start the production binary from the fixture priced with `store` per
/// retained byte-second. Its Prometheus serves `samples` of `m` for raw reads.
async fn deploy(store: f64, samples: Vec<(i64, f64)>) -> Deployment {
    let mut fixture: Value = serde_json::from_str(include_str!(
        "../../../docs/examples/asapquery-planning-snapshot.json"
    ))
    .unwrap();
    fixture["implementation"]["scrape_interval_ms"] = 1000.into();
    fixture["data_workload"]["data_ingestion_interval"]["value"] = 1000.into();
    fixture["implementation"]["lifecycle_costs"]["store_per_byte_second"] = store.into();
    let snapshot: BackendLocalPlanningInput = serde_json::from_value(fixture).unwrap();
    let priced = quote_snapshot_for_test(snapshot);

    let requests = Arc::new(Mutex::new(Vec::<HashMap<String, String>>::new()));
    let recorded = requests.clone();
    let matrix = serde_json::json!({"status": "success", "data": {"resultType": "matrix",
        "result": [{"metric": {"__name__": "m", "instance": "a"},
            "values": samples.iter().map(|(ms, value)| serde_json::json!([*ms as f64 / 1000.0, value.to_string()])).collect::<Vec<_>>()}]}});
    let empty =
        serde_json::json!({"status": "success", "data": {"resultType": "vector", "result": []}});
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let prometheus_url = format!("http://{}", listener.local_addr().unwrap());
    let prometheus = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/-/healthy", get(|| async { "healthy" }))
                .route(
                    "/api/v1/query",
                    get(move |Query(params): Query<HashMap<String, String>>| {
                        let recorded = recorded.clone();
                        let raw = params.get("query").map(String::as_str) == Some(RAW_SELECTOR);
                        let response = if raw { matrix.clone() } else { empty.clone() };
                        async move {
                            recorded.lock().await.push(params);
                            Json(response)
                        }
                    }),
                ),
        )
        .await
        .unwrap();
    });
    let output = tempfile::tempdir().unwrap();
    let path = output.path().join("planning.json");
    std::fs::write(&path, serde_json::to_vec(&priced).unwrap()).unwrap();
    let port = unused_port();
    let mut child = ChildGuard(
        Command::new(env!("CARGO_BIN_EXE_data_plane"))
            .args(["--profile", "asapquery", "--planning-snapshot"])
            .arg(path)
            .args([
                "--prometheus-server",
                &prometheus_url,
                "--forward-unsupported-queries",
                "--precompute-allowed-lateness-ms",
                "0",
                "--precompute-flush-interval-ms",
                "25",
                "--http-port",
                &port.to_string(),
                "--output-dir",
            ])
            .arg(output.path())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let backend = format!("http://127.0.0.1:{port}");
    wait_until_ready(
        &reqwest::Client::new(),
        &format!("{backend}/api/v1/health"),
        &mut child.0,
    )
    .await;
    Deployment {
        backend,
        requests,
        _child: child,
        _output: output,
        prometheus,
    }
}

fn origin_ms() -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    now - now.rem_euclid(600_000) - 1_200_000
}

// A cheap summary store precomputes the sketch: the query is answered from
// maintained state and never reads raw series from Prometheus.
#[tokio::test]
async fn cheap_summary_store_answers_from_precomputed_state() {
    let origin = origin_ms();
    let deployment = deploy(0.0, vec![]).await;
    let client = reqwest::Client::new();
    let samples: Vec<_> = (1..=121)
        .map(|i| (origin + i * 1000, (1 + i % 7) as f64))
        .collect();
    let wire = WriteRequest {
        timeseries: vec![series_with_labels("m", &[("instance", "a")], &samples)],
    };
    assert_eq!(remote_write(&client, &deployment.backend, &wire).await, 204);
    let output = deployment._output.path().join("query_engine.log");
    let at = (origin + 120_000) as f64 / 1000.0;
    let response = wait_for_warm_instant(&client, &deployment.backend, QUERY, at, &output).await;
    assert!(first_value(&response, "value").is_some(), "{response}");
    assert!(
        !deployment
            .requests
            .lock()
            .await
            .iter()
            .any(|request| request.get("query").map(String::as_str) == Some(RAW_SELECTOR)),
        "precomputed state must not read raw series"
    );
    deployment.prometheus.abort();
}

// An expensive summary store rebuilds the state per query: the backend reads the
// range selector's raw series from Prometheus and computes the answer itself.
#[tokio::test]
async fn expensive_summary_store_rebuilds_state_from_raw_series() {
    let at_ms = origin_ms() + 120_000;
    let values = [5.0, 1.0, 4.0, 2.0, 3.0];
    let samples = values
        .iter()
        .enumerate()
        .map(|(i, value)| (at_ms - 50_000 + i as i64 * 10_000, *value))
        .collect();
    let deployment = deploy(1.0, samples).await;
    let response: Value = reqwest::Client::new()
        .get(format!("{}/api/v1/query", deployment.backend))
        .query(&[
            ("query", QUERY.to_string()),
            ("time", (at_ms as f64 / 1000.0).to_string()),
        ])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    // PromQL quantile_over_time(0.99) of 1..=5: rank 3.96 between 4 and 5.
    assert_eq!(first_value(&response, "value"), Some(4.96), "{response}");
    let requests = deployment.requests.lock().await;
    assert!(
        requests.iter().any(|request| {
            request.get("query").map(String::as_str) == Some(RAW_SELECTOR)
                && request.get("time").map(String::as_str)
                    == Some(format!("{:.3}", at_ms as f64 / 1000.0).as_str())
        }),
        "query time must read raw series: {requests:?}"
    );
    assert!(
        !requests
            .iter()
            .any(|request| request.get("query").map(String::as_str) == Some(QUERY)),
        "the backend, not Prometheus, answers the query: {requests:?}"
    );
    deployment.prometheus.abort();
}
