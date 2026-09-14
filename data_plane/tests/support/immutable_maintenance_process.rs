//! Real selected DAG, finite source completion, and durable derived output.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_source_maintenance_is_automatic_and_durable() {
    run_maintenance_process(false, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_source_maintenance_is_automatic_and_durable() {
    run_maintenance_process(true, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn complete_group_maintenance_is_automatic_and_durable() {
    run_maintenance_process(true, true).await;
}

async fn run_maintenance_process(multi_source: bool, distinct_groups: bool) {
    let query = if multi_source {
        "quantile(0.9, sum_over_time(immutable_value[1m]) + sum_over_time(immutable_other[1m]))"
    } else {
        "quantile(0.9, sum_over_time(immutable_value[1m]))"
    };
    let base_expected = if multi_source { 20.0 } else { 10.0 };
    let mut fixture: Value = serde_json::from_str(include_str!(
        "../../../docs/examples/asapquery-compatibility-demo-snapshot.json"
    ))
    .unwrap();
    let mut entry = fixture["query_workload"]["repeating_queries"][3].clone();
    entry["query"] = query.into();
    entry["demand"]["fixed_interval_at"]["interval"] = 60_000.into();
    entry["demand"]["fixed_interval_at"]["evaluation_phase"] = 0.into();
    entry["time_selection"]["lookback"] = 60_000.into();
    fixture["query_workload"]["repeating_queries"] = serde_json::json!([entry]);
    let snapshot: control_plane::physical::compiler::BackendLocalPlanningInput =
        serde_json::from_value(fixture.clone()).unwrap();
    let plan = quote_snapshot_for_test(snapshot).compile_promql().unwrap();
    assert_eq!(
        plan.precompute_plan.materializations.len(),
        if multi_source { 3 } else { 2 }
    );
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
        plan.precompute_plan
            .materializations
            .iter()
            .filter(|m| m.derived_input.is_none())
            .map(|m| m.policy_fingerprint().into())
            .collect()
    );
    // The production cost model may choose DDSketch or KLL. Preserve that
    // choice and use its actual value contract for this singleton oracle.
    let max_relative_error = match derived.aggregation_type {
        asap_types::AggregationType::DDSketch => derived.parameters["alpha"].as_f64().unwrap(),
        asap_types::AggregationType::DatasketchesKLL => 0.0,
        ref other => panic!("singleton quantile oracle missing for {other:?}"),
    };
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
    // Complete canonical populations are supported; missing source/group sets
    // must fail closed before publishing any global output.
    for (count, missing_source) in [(1, true), (2, true), (1, false), (2, false)] {
        let expected = if distinct_groups && count == 2 {
            38.0
        } else {
            base_expected
        };
        if missing_source && !multi_source {
            continue;
        }
        eprintln!("IMMUTABLE_CASE count={count} missing_source_or_group={missing_source}");
        let mut directory = tempfile::tempdir().unwrap();
        eprintln!("IMMUTABLE_PROCESS_ARTIFACT {}", directory.path().display());
        directory.disable_cleanup(true);
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
        let mut series: Vec<_> = (0..count)
            .map(|i| {
                series_with_labels(
                    "immutable_value",
                    &[
                        ("instance", if i == 0 { "a" } else { "b" }),
                        ("job", "worker"),
                    ],
                    &[
                        (1_000, if distinct_groups && i == 1 { 4.0 } else { 2.0 }),
                        (2_000, if distinct_groups && i == 1 { 6.0 } else { 3.0 }),
                        (60_000, if distinct_groups && i == 1 { 10.0 } else { 5.0 }),
                    ],
                )
            })
            .collect();
        if multi_source && (!missing_source || count == 2) {
            for i in 0..if missing_source { 1 } else { count } {
                series.push(series_with_labels(
                    "immutable_other",
                    &[
                        ("instance", if i == 0 { "a" } else { "b" }),
                        ("job", "worker"),
                    ],
                    &[
                        (1_000, if distinct_groups && i == 1 { 4.0 } else { 2.0 }),
                        (2_000, if distinct_groups && i == 1 { 6.0 } else { 3.0 }),
                        (60_000, if distinct_groups && i == 1 { 10.0 } else { 5.0 }),
                    ],
                ));
            }
        }
        assert_eq!(
            remote_write(&client, &backend, &WriteRequest { timeseries: series }).await,
            204
        );
        let drain = client
            .post(format!("{backend}/api/v1/precompute/drain"))
            .send()
            .await
            .unwrap();
        if !missing_source {
            assert!(
                drain.status().is_success(),
                "{}",
                drain.text().await.unwrap()
            );
        }
        let response: Value = client
            .get(format!("{backend}/api/v1/query"))
            .query(&[("query", query), ("time", "60")])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if missing_source {
            assert!(
                !is_warm(&response),
                "incomplete source group set was incorrectly admitted: {response}"
            );
            continue;
        }
        assert!(is_warm(&response), "{response}");
        assert_eq!(
            response["data"]["result"].as_array().map(Vec::len),
            Some(1),
            "{response}"
        );
        assert_eq!(
            response["data"]["result"][0]["metric"],
            serde_json::json!({})
        );
        let estimate = response["data"]["result"][0]["value"][1]
            .as_str()
            .unwrap()
            .parse::<f64>()
            .unwrap();
        assert!(
            estimate.is_finite() && (estimate - expected).abs() / expected <= max_relative_error,
            "selected population quantile exceeded its value contract: {response}"
        );
        let part_ids = || {
            let mut ids = std::fs::read_dir(disk.join("sketch_index/parts"))
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .collect::<Vec<_>>();
            ids.sort();
            ids
        };
        let before_retry_parts = part_ids();
        let repeated = client
            .post(format!("{backend}/api/v1/precompute/drain"))
            .send()
            .await
            .unwrap();
        assert!(
            repeated.status().is_success(),
            "{}",
            repeated.text().await.unwrap()
        );
        assert_eq!(
            part_ids(),
            before_retry_parts,
            "repeated completion published extra parts"
        );
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
            .query(&[("query", query), ("time", "60")])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(is_warm(&after), "{after}");
        assert_eq!(after["data"]["result"], response["data"]["result"]);
        assert_eq!(
            part_ids(),
            before_retry_parts,
            "restart published extra immutable parts"
        );
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
        drop(restarted);
        // Reopening admission in a new generation must not expose the prior
        // singleton-derived output while new source populations can arrive.
        let mut next_fixture = fixture.clone();
        next_fixture["environment"]["plan_version"] = 2.into();
        let next = quote_snapshot_for_test(
            serde_json::from_value::<control_plane::physical::compiler::BackendLocalPlanningInput>(
                next_fixture,
            )
            .unwrap(),
        )
        .compile_promql()
        .unwrap();
        let next_install = data_plane::drivers::query::servers::http::PhysicalPlanInstallRequest {
            summary_catalog: next.summary_catalog,
            collector_plans: next.collector_plans,
            precompute_plan: next.precompute_plan,
            transmission_plan: next.transmission_plan,
            query_plan: next.query_plan,
            storage_routing: None,
            adaptation_evidence: vec![],
        };
        std::fs::write(&artifact, serde_json::to_vec(&next_install).unwrap()).unwrap();
        let port = unused_port();
        let backend = format!("http://127.0.0.1:{port}");
        let mut next_generation = spawn(port);
        wait_until_ready(
            &client,
            &format!("{backend}/api/v1/health"),
            &mut next_generation.0,
        )
        .await;
        let stale: Value = client
            .get(format!("{backend}/api/v1/query"))
            .query(&[("query", query), ("time", "60")])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(
            !is_warm(&stale),
            "new generation reused old singleton output: {stale}"
        );
        assert_eq!(
            remote_write(
                &client,
                &backend,
                &WriteRequest {
                    timeseries: vec![series_with_labels(
                        "immutable_value",
                        &[("instance", "next"), ("job", "worker")],
                        &[(1_000, 17.0)]
                    )]
                }
            )
            .await,
            204,
            "new generation must reopen raw admission"
        );
    }
}
