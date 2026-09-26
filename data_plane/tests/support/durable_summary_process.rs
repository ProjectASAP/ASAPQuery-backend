//! Authoritative catalog identity survives a real production process restart.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persisted_summary_restarts_without_live_reregistration() {
    let mut fixture: Value = serde_json::from_str(include_str!(
        "../../../docs/examples/asapquery-compatibility-demo-snapshot.json"
    ))
    .unwrap();
    let query = fixture["query_workload"]["repeating_queries"][2]["query"]
        .as_str()
        .unwrap()
        .to_owned();
    fixture["query_workload"]["repeating_queries"] =
        serde_json::json!([fixture["query_workload"]["repeating_queries"][2].clone()]);
    // This restart fixture persists one complete five-second population.
    fixture["query_workload"]["repeating_queries"][0]["demand"]["fixed_interval_at"]["interval"] =
        serde_json::json!(5000);
    let snapshot: control_plane::physical::compiler::BackendLocalPlanningInput =
        serde_json::from_value(fixture).unwrap();
    let plan = quote_snapshot_for_test(snapshot).compile_promql().unwrap();
    let install = data_plane::drivers::query::servers::http::PhysicalPlanInstallRequest {
        summary_catalog: plan.summary_catalog,
        collector_plans: plan.collector_plans,
        precompute_plan: plan.precompute_plan,
        transmission_plan: plan.transmission_plan,
        query_plan: plan.query_plan,
        storage_routing: None,
        adaptation_evidence: vec![],
    };
    let directory = tempfile::tempdir().unwrap();
    let artifact = directory.path().join("plan.json");
    let bootstrap = directory.path().join("bootstrap.json");
    let disk = directory.path().join("disk");
    std::fs::create_dir_all(&disk).unwrap();
    std::fs::write(&artifact, serde_json::to_vec(&install).unwrap()).unwrap();
    std::fs::write(&bootstrap, b"{\"aggregations\":[]}").unwrap();
    let spawn = |port: u16| {
        ChildGuard(
            Command::new(env!("CARGO_BIN_EXE_data_plane"))
                .arg("--physical-plan")
                .arg(&artifact)
                .arg("--streaming-config")
                .arg(&bootstrap)
                .arg("--http-port")
                .arg(port.to_string())
                .arg("--output-dir")
                .arg(directory.path())
                .arg("--enable-remote-write")
                .arg("--persistence-enabled")
                .arg("--persistence-dir")
                .arg(&disk)
                .args([
                    "--persistence-memory-limit-mb",
                    "1",
                    "--persistence-hot-window-secs",
                    "1",
                    "--persistence-delete-older-than-secs",
                    "0",
                    "--persistence-seal-window-count",
                    "1",
                    "--persistence-flush-interval-ms",
                    "10",
                    "--precompute-allowed-lateness-ms",
                    "0",
                    "--precompute-flush-interval-ms",
                    "25",
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap(),
        )
    };
    let client = reqwest::Client::new();
    let port = unused_port();
    let base = format!("http://127.0.0.1:{port}");
    let mut first = spawn(port);
    wait_until_ready(&client, &format!("{base}/api/v1/health"), &mut first.0).await;
    assert_eq!(
        remote_write(
            &client,
            &base,
            &WriteRequest {
                timeseries: vec![series(
                    "asap_demo_gauge",
                    &[
                        (1000, 1.0),
                        (2000, 2.0),
                        (3000, 3.0),
                        (4000, 4.0),
                        (5000, 5.0)
                    ]
                )],
            }
        )
        .await,
        204
    );
    drain_precompute(&client, &base).await;
    let before: Value = client
        .get(format!("{base}/api/v1/query"))
        .query(&[("query", query.as_str()), ("time", "5")])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(is_warm(&before), "{before}");
    let sidecar = disk.join("sketch_index/sid_metadata.json");
    for _ in 0..500 {
        let manifest_path = disk.join("sketch_index");
        if sidecar.exists()
            && manifest_path.join("parts_manifest.log").exists()
            && data_plane::storage_engines::sketch_db::index::persistence::Manifest::open_or_init(
                &manifest_path,
            )
            .is_ok_and(|manifest| manifest.live_parts().iter().any(|part| part.max_ts >= 5000))
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    if !sidecar.exists() {
        let retained = directory.keep();
        panic!(
            "missing flushed metadata; retained process artifacts at {}",
            retained.display()
        );
    }
    let metadata: Value = serde_json::from_slice(&std::fs::read(&sidecar).unwrap()).unwrap();
    assert!(metadata["bindings"]
        .as_object()
        .unwrap()
        .values()
        .all(|binding| !binding["stored_output_id"].is_null()
            && binding["summary_definition_id"]
                .as_str()
                .is_some_and(|id| id.starts_with("sds-v1:"))
            && !binding["catalog_generation_sha256"].is_null()));
    drop(first);
    let port = unused_port();
    let base = format!("http://127.0.0.1:{port}");
    let mut second = spawn(port);
    wait_until_ready(&client, &format!("{base}/api/v1/health"), &mut second.0).await;
    // No Remote Write, register call, or installation endpoint after restart.
    let after: Value = client
        .get(format!("{base}/api/v1/query"))
        .query(&[("query", query.as_str()), ("time", "5")])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(is_warm(&after), "{after}");
    assert_eq!(after["data"]["result"], before["data"]["result"]);
    assert_eq!(after["data"]["result"][0]["value"][1], "15");
}
