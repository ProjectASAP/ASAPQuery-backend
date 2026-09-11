//! Real selected DAG, finite source completion, and durable derived output.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_source_maintenance_is_automatic_and_durable() {
    const QUERY: &str = "quantile(0.9, sum_over_time(immutable_value[1m]))";
    let mut fixture: Value = serde_json::from_str(include_str!(
        "../../../docs/examples/asapquery-compatibility-demo-snapshot.json"
    ))
    .unwrap();
    let mut entry = fixture["query_workload"]["repeating_queries"][3].clone();
    entry["query"] = QUERY.into();
    entry["demand"]["fixed_interval_at"]["interval"] = 60_000.into();
    entry["demand"]["fixed_interval_at"]["evaluation_phase"] = 0.into();
    entry["time_selection"]["lookback"] = 60_000.into();
    fixture["query_workload"]["repeating_queries"] = serde_json::json!([entry]);
    let snapshot: control_plane::physical::compiler::BackendLocalPlanningSnapshot =
        serde_json::from_value(fixture).unwrap();
    let plan = snapshot.compile().unwrap();
    assert_eq!(plan.precompute_plan.materializations.len(), 2);
    let source = plan
        .precompute_plan
        .materializations
        .iter()
        .find(|m| m.derived_input.is_none())
        .unwrap();
    let derived = plan
        .precompute_plan
        .materializations
        .iter()
        .find(|m| m.derived_input.is_some())
        .unwrap();
    assert_eq!(source.window_size, 60);
    assert_eq!(derived.window_size, 60);
    assert_eq!(source.pane_origin_ms, Some(0));
    assert_eq!(derived.pane_origin_ms, Some(0));
    assert_eq!(
        derived.derived_input.as_ref().unwrap().inputs,
        std::collections::BTreeSet::from([source.policy_fingerprint().into()])
    );
    eprintln!(
        "IMMUTABLE_SELECTED {}",
        serde_json::json!({
            "catalog":plan.summary_catalog,"precompute":plan.precompute_plan,
            "query":plan.query_plan,"lifecycle":plan.lifecycle_estimates
        })
    );
    let install = data_plane::drivers::query::servers::http::PhysicalPlanInstallRequest {
        summary_catalog: plan.summary_catalog,
        collector_plans: plan.collector_plans,
        precompute_plan: plan.precompute_plan,
        transmission_plan: plan.transmission_plan,
        query_plan: plan.query_plan,
        storage_routing: None,
        adaptation_evidence: vec![],
    };
    // Two independent deployments: singleton is supported; a second physical
    // input series must never be mistaken for a complete singleton population.
    for count in [1, 2] {
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
                    .args(["--http-port", &port.to_string(), "--output-dir"])
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
        let backend = format!("http://127.0.0.1:{port}");
        let mut first = spawn(port);
        wait_until_ready(&client, &format!("{backend}/api/v1/health"), &mut first.0).await;
        let series = (0..count)
            .map(|i| {
                series_with_labels(
                    "immutable_value",
                    &[
                        ("instance", if i == 0 { "a" } else { "b" }),
                        ("job", "worker"),
                    ],
                    &[(1_000, 2.0), (2_000, 3.0), (60_000, 5.0)],
                )
            })
            .collect();
        assert_eq!(
            remote_write(&client, &backend, &WriteRequest { timeseries: series }).await,
            204
        );
        let drain = client
            .post(format!("{backend}/api/v1/precompute/drain"))
            .send()
            .await
            .unwrap();
        if count == 1 {
            assert!(
                drain.status().is_success(),
                "{}",
                drain.text().await.unwrap()
            );
        }
        let response: Value = client
            .get(format!("{backend}/api/v1/query"))
            .query(&[("query", QUERY), ("time", "60")])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if count == 2 {
            assert!(
                !is_warm(&response),
                "multi-series population was incorrectly admitted: {response}"
            );
            continue;
        }
        assert!(is_warm(&response), "{response}");
        assert_eq!(response["data"]["result"].as_array().unwrap().len(), 1);
        assert_eq!(
            response["data"]["result"][0]["metric"],
            serde_json::json!({})
        );
        assert_eq!(response["data"]["result"][0]["value"][1], "10");
        drop(first);
        let port = unused_port();
        let backend = format!("http://127.0.0.1:{port}");
        let mut restarted = spawn(port);
        wait_until_ready(
            &client,
            &format!("{backend}/api/v1/health"),
            &mut restarted.0,
        )
        .await;
        let after: Value = client
            .get(format!("{backend}/api/v1/query"))
            .query(&[("query", QUERY), ("time", "60")])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(is_warm(&after), "{after}");
        assert_eq!(after["data"]["result"], response["data"]["result"]);
        assert_ne!(
            remote_write(
                &client,
                &backend,
                &WriteRequest {
                    timeseries: vec![series_with_labels(
                        "immutable_value",
                        &[("instance", "new")],
                        &[(61_000, 1.0)]
                    )]
                }
            )
            .await,
            204,
            "closed generation accepted a new physical population after restart"
        );
    }
}
