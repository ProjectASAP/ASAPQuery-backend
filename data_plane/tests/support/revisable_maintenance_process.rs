//! Black-box Remote Write -> selected DAG -> revision checkpoint -> bound query.
use super::*;

/// Late input replaces both base and derived results, survives same-version
/// restart, and cannot leak a valid prefix from an expired mixed request.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn continuous_revisions_replace_results_and_recover() {
    run_revisions(false).await;
}

/// The same input snapshot feeds both arithmetic branches before building the
/// persisted sketch; revising A preserves the unchanged B contribution.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn continuous_revisions_keep_two_source_dag_consistent() {
    run_revisions(true).await;
}

async fn run_revisions(multi_source: bool) {
    let query = if multi_source {
        "quantile(0.9, sum_over_time(revisable_value[1m]) + sum_over_time(revisable_other[1m]))"
    } else {
        "quantile(0.9, sum_over_time(revisable_value[1m]))"
    };
    let mut fixture: Value = serde_json::from_str(include_str!(
        "../../../docs/examples/asapquery-compatibility-demo-snapshot.json"
    ))
    .unwrap();
    let mut entry = fixture["query_workload"]["repeating_queries"][3].clone();
    entry["query"] = query.into();
    entry["demand"]["fixed_interval_at"]["interval"] = 60_000.into();
    entry["demand"]["fixed_interval_at"]["evaluation_phase"] = 0.into();
    fixture["query_workload"]["repeating_queries"] = serde_json::json!([entry]);
    let compile = |fixture: Value| {
        let plan = quote_snapshot_for_test(serde_json::from_value(fixture).unwrap())
            .compile_promql()
            .unwrap();
        assert!(plan
            .precompute_plan
            .materializations
            .iter()
            .any(|m| m.derived_input.is_some()));
        data_plane::drivers::query::servers::http::PhysicalPlanInstallRequest {
            summary_catalog: plan.summary_catalog,
            collector_plans: plan.collector_plans,
            precompute_plan: plan.precompute_plan,
            transmission_plan: plan.transmission_plan,
            query_plan: plan.query_plan,
            storage_routing: None,
            adaptation_evidence: vec![],
        }
    };
    let directory = tempfile::tempdir().unwrap();
    let artifact = directory.path().join("plan.json");
    let installed = compile(fixture.clone());
    let derived = installed
        .precompute_plan
        .materializations
        .iter()
        .find(|m| m.derived_input.is_some())
        .unwrap();
    let relative_error = match derived.aggregation_type {
        asap_types::AggregationType::DDSketch => derived.parameters["alpha"].as_f64().unwrap(),
        asap_types::AggregationType::DatasketchesKLL => 0.0,
        ref other => panic!("singleton revision oracle missing for {other:?}"),
    };
    std::fs::write(&artifact, serde_json::to_vec(&installed).unwrap()).unwrap();
    let spawn = |port: u16| {
        ChildGuard(
            Command::new(env!("CARGO_BIN_EXE_data_plane"))
                .arg("--physical-plan")
                .arg(&artifact)
                .arg("--http-port")
                .arg(port.to_string())
                .arg("--output-dir")
                .arg(directory.path())
                .arg("--enable-remote-write")
                .arg("--remote-write-revision-dir")
                .arg(directory.path().join("revisions"))
                .args([
                    "--remote-write-correction-horizon-ms",
                    "300000",
                    "--remote-write-revision-freshness-ms",
                    "60000",
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
    let mut child = spawn(port);
    wait_until_ready(&client, &format!("{backend}/api/v1/health"), &mut child.0).await;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let start = (now / 60_000 - 1) * 60_000;
    let end = start + 60_000;
    let request = |samples: &[(i64, f64)]| {
        let mut timeseries = vec![series_with_labels(
            "revisable_value",
            &[("instance", "a"), ("job", "worker")],
            samples,
        )];
        if multi_source {
            timeseries.push(series_with_labels(
                "revisable_other",
                &[("instance", "a"), ("job", "worker")],
                &[(start + 1000, 10.)],
            ));
        }
        WriteRequest { timeseries }
    };
    let read = |base: String| {
        let client = client.clone();
        async move {
            client
                .get(format!("{base}/api/v1/query"))
                .query(&[
                    ("query", query.to_string()),
                    ("time", (end / 1000).to_string()),
                ])
                .send()
                .await
                .unwrap()
                .json::<Value>()
                .await
                .unwrap()
        }
    };
    let assert_value = |response: &Value, expected: f64| {
        let expected = expected + if multi_source { 10. } else { 0. };
        assert!(is_warm(response), "{response}");
        let value = response["data"]["result"][0]["value"][1]
            .as_str()
            .unwrap()
            .parse::<f64>()
            .unwrap();
        assert!(
            (value - expected).abs() <= expected * relative_error + 1e-12,
            "expected {expected}, got {response}"
        );
    };
    assert_eq!(
        remote_write(
            &client,
            &backend,
            &request(&[(start + 1000, 2.), (start + 2000, 3.)])
        )
        .await,
        204
    );
    assert_value(&read(backend.clone()).await, 5.);
    assert_eq!(
        remote_write(&client, &backend, &request(&[(start + 1500, 4.)])).await,
        204
    );
    assert_value(&read(backend.clone()).await, 9.);
    // A retry must not double count accepted input.
    assert_eq!(
        remote_write(&client, &backend, &request(&[(start + 1500, 4.)])).await,
        204
    );
    assert_value(&read(backend.clone()).await, 9.);
    assert_eq!(
        remote_write(
            &client,
            &backend,
            &request(&[(start + 3000, 7.), (start - 600_000, 99.)])
        )
        .await,
        400
    );
    assert_value(&read(backend.clone()).await, 9.);
    drop(child);
    let port = unused_port();
    let backend = format!("http://127.0.0.1:{port}");
    let mut child = spawn(port);
    wait_until_ready(&client, &format!("{backend}/api/v1/health"), &mut child.0).await;
    assert_value(&read(backend.clone()).await, 9.);
    assert_eq!(
        remote_write(&client, &backend, &request(&[(start + 3000, 7.)])).await,
        204
    );
    assert_value(&read(backend.clone()).await, 16.);
    drop(child);
    fixture["environment"]["plan_version"] = 2.into();
    std::fs::write(&artifact, serde_json::to_vec(&compile(fixture)).unwrap()).unwrap();
    let port = unused_port();
    let backend = format!("http://127.0.0.1:{port}");
    let mut child = spawn(port);
    wait_until_ready(&client, &format!("{backend}/api/v1/health"), &mut child.0).await;
    assert!(
        !is_warm(&read(backend.clone()).await),
        "new plan version reused prior revision"
    );
    assert_eq!(
        remote_write(&client, &backend, &request(&[(start + 1000, 11.)])).await,
        204
    );
    assert_value(&read(backend.clone()).await, 11.);
}
