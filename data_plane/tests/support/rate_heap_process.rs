use super::*;
use control_plane::physical::{
    compiler::{
        BackendLocalPlanningInput, DeploymentPlanCompiler, BACKEND_REVISION, PLANNER_REVISION,
    },
    workload_cost::{
        enumerate_exact_and_materialized_candidates, manifest, WorkloadCostEvidence, WorkloadQuote,
    },
};

// Real Remote Write -> persisted counter windows -> exact Rate -> Planner heap
// -> HTTP. Restart reads the same bound SDS and retained physical program.
#[tokio::test]
async fn rate_countsketch_heap_survives_counter_reset_and_durable_restart() {
    run("CountSketchWithHeap", false).await;
}

#[tokio::test]
async fn rate_cms_heap_survives_counter_reset_and_durable_restart() {
    run("CmsWithHeap", false).await;
}

// The maintenance graph publishes the heap itself before HTTP readout and restart.
#[tokio::test]
async fn precomputed_rate_countsketch_heap_survives_durable_restart() {
    run("CountSketchWithHeap", true).await;
}
#[tokio::test]
async fn precomputed_rate_cms_heap_survives_durable_restart() {
    run("CmsWithHeap", true).await;
}

#[tokio::test]
async fn precomputed_grouped_rate_sum_survives_durable_restart() {
    run("Sum", true).await;
}
#[tokio::test]
async fn query_time_grouped_rate_sum_survives_durable_restart() {
    run("Sum", false).await;
}

async fn run(algorithm: &str, precomputed: bool) {
    let query = if algorithm == "Sum" {
        "sum by (job) (rate(requests_total[1m]))"
    } else {
        "topk by (job) (1, rate(requests_total[1m]))"
    };
    let mut wire: Value = serde_json::from_str(include_str!(
        "../../../docs/examples/asapquery-compatibility-demo-snapshot.json"
    ))
    .unwrap();
    let mut entry = wire["query_workload"]["repeating_queries"][3].clone();
    entry["query"] = query.into();
    entry["requirements"]["accuracy"] =
        serde_json::json!({"explicit":{"EpsilonDelta":{"epsilon":0.1,"delta":0.1}}});
    entry["demand"]["fixed_interval_at"]["interval"] = if algorithm == "Sum" {
        5_000.into()
    } else {
        60_000.into()
    };
    if algorithm == "Sum" {
        entry["requirements"]["accuracy"] = serde_json::json!({"explicit":"Exact"});
    }
    entry["demand"]["fixed_interval_at"]["evaluation_phase"] = 0.into();
    wire["query_workload"]["repeating_queries"] = serde_json::json!([entry]);
    wire["implementation"]["topk_evidence"] = serde_json::json!({});
    wire["implementation"]["data_snapshot_id"] = "rate-heap-process".into();
    // Fixture rates: winner >=500, excluded rates <=10, at most three series per ranking group.
    wire["implementation"]["accuracy_evidence"] = serde_json::json!({query:{
        "query_string":query,"data_snapshot_id":"rate-heap-process",
        "data_workload":wire["data_workload"],"source":"enforced-test-population",
        "observed_at_unix_ms":9500,"valid_for_ms":60000,"topk_max_distinct_items":3,
        "topk_selected_lower_bound":500.0,"topk_excluded_upper_bound":10.0,
        "topk_interval_failure_probability":0.001
    }});
    if algorithm == "Sum" {
        wire["implementation"]["accuracy_evidence"] = serde_json::json!({});
    }
    let mut snapshot: BackendLocalPlanningInput = serde_json::from_value(wire).unwrap();
    let (request, environment) = snapshot
        .clone()
        .into_physical_compilation_request()
        .unwrap();
    let mut saw_heap = false;
    let quotes = enumerate_exact_and_materialized_candidates(request)
        .unwrap()
        .into_iter()
        .filter_map(|candidate| {
            let plan = DeploymentPlanCompiler
                .compile_promql(candidate.clone(), environment.clone())
                .ok()?;
            let heap = plan.query_plan.entries.values().any(|entry| {
                entry
                    .physical_dag
                    .as_ref()
                    .is_some_and(|program| program.to_string().contains(algorithm))
            });
            let heap = heap
                && plan
                    .precompute_plan
                    .executable_dags
                    .values()
                    .any(|dag| !dag.native_programs.is_empty())
                    == precomputed;
            saw_heap |= heap;
            let manifest = manifest(&plan, &candidate.queries).unwrap();
            Some(WorkloadQuote {
                unit_costs: manifest
                    .components
                    .keys()
                    .map(|key| (key.clone(), if heap { 1.0 } else { 1e12 }))
                    .collect(),
                manifest,
                executable: true,
            })
        })
        .collect();
    assert!(saw_heap);
    snapshot.workload_cost_evidence = Some(WorkloadCostEvidence {
        backend_revision: BACKEND_REVISION.into(),
        planner_revision: PLANNER_REVISION.into(),
        data_snapshot_id: "rate-heap-process".into(),
        model_version: "test-only-heap-selection".into(),
        observed_at_unix_ms: 10000,
        valid_for_ms: 60000,
        quotes,
    });
    let plan = snapshot.clone().compile_promql().unwrap();
    let entry = plan.query_plan.entries.values().next().unwrap();
    assert!(entry
        .physical_dag
        .as_ref()
        .unwrap()
        .to_string()
        .contains(algorithm));
    entry.recover_vector_physical_dag().unwrap();
    assert_eq!(
        plan.precompute_plan.materializations.len(),
        if precomputed { 2 } else { 1 }
    );
    assert_eq!(
        plan.precompute_plan
            .executable_dags
            .values()
            .any(|dag| !dag.native_programs.is_empty()),
        precomputed
    );

    if precomputed {
        assert!(
            plan.precompute_plan
                .materializations
                .iter()
                .all(
                    |state| state.slide_interval == if algorithm == "Sum" { 5 } else { 60 }
                        && state.window_size == 60
                ),
            "maintenance must match the query cadence, not publish only every 60 seconds"
        );
    }

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
    let disk = directory.path().join("disk");
    std::fs::write(&artifact, serde_json::to_vec(&install).unwrap()).unwrap();
    let spawn = |port: u16| {
        ChildGuard(
            Command::new(env!("CARGO_BIN_EXE_data_plane"))
                .arg("--physical-plan")
                .arg(&artifact)
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
    let mut expected = Vec::new();
    for restart in [false, true] {
        let port = unused_port();
        let backend = format!("http://127.0.0.1:{port}");
        let mut child = spawn(port);
        wait_until_ready(&client, &format!("{backend}/api/v1/health"), &mut child.0).await;
        if !restart {
            let request = WriteRequest {
                timeseries: [
                    (
                        "a",
                        vec![
                            (1000, 1000.),
                            (20000, 20000.),
                            (40000, 40000.),
                            (60000, 60000.),
                            (61000, 61000.),
                            (80000, 61000.),
                            (100000, 61000.),
                            (120000, 61000.),
                        ],
                    ),
                    (
                        "b",
                        vec![
                            (1000, 1.),
                            (20000, 20.),
                            (40000, 40.),
                            (60000, 60.),
                            (61000, 1000.),
                            (80000, 20000.),
                            (100000, 50.),
                            (120000, 20050.),
                        ],
                    ),
                    (
                        "c",
                        vec![
                            (1000, 0.1),
                            (20000, 2.),
                            (40000, 4.),
                            (60000, 6.),
                            (61000, 6.1),
                            (80000, 8.),
                            (100000, 10.),
                            (120000, 12.),
                        ],
                    ),
                ]
                .into_iter()
                .map(|(id, samples)| {
                    series_with_labels(
                        "requests_total",
                        &[("job", "api"), ("unreferenced_instance", id)],
                        &samples,
                    )
                })
                .chain(std::iter::once(series_with_labels(
                    "requests_total",
                    &[("job", "worker"), ("unreferenced_instance", "d")],
                    &[
                        (1000, 2000.),
                        (20000, 40000.),
                        (40000, 80000.),
                        (60000, 120000.),
                        (61000, 122000.),
                        (80000, 160000.),
                        (100000, 200000.),
                        (120000, 240000.),
                    ],
                )))
                .chain((algorithm == "Sum").then(|| {
                    series_with_labels(
                        "requests_total",
                        &[("unreferenced_instance", "no-job")],
                        &[
                            (1000, 3.),
                            (20000, 60.),
                            (40000, 120.),
                            (60000, 180.),
                            (61000, 183.),
                            (80000, 240.),
                            (100000, 300.),
                            (120000, 360.),
                        ],
                    )
                }))
                .collect(),
            };
            assert_eq!(remote_write(&client, &backend, &request).await, 204);
            drain_precompute(&client, &backend).await;
        }
        let evaluations = if algorithm == "Sum" {
            vec![
                (60., "a", 1000.),
                (65., "a", 1000.),
                (120., "b", 39050. / 59.),
            ]
        } else {
            vec![(60., "a", 1000.), (120., "b", 39050. / 59.)]
        };
        for (index, (at, winner, score)) in evaluations.into_iter().enumerate() {
            let body = wait_for_warm_instant(
                &client,
                &backend,
                query,
                at,
                &directory.path().join("query_engine.log"),
            )
            .await;
            let rows = body["data"]["result"].as_array().unwrap();
            assert_eq!(rows.len(), if algorithm == "Sum" { 3 } else { 2 }, "{body}");
            if algorithm == "Sum" {
                let empty = rows
                    .iter()
                    .find(|row| {
                        row["metric"]
                            .as_object()
                            .is_some_and(|labels| labels.is_empty())
                    })
                    .unwrap_or_else(|| panic!("missing grouping labels must be omitted: {body}"));
                assert!(
                    (empty["value"][1].as_str().unwrap().parse::<f64>().unwrap() - 3.).abs() < 1e-8
                );
            }
            assert!(
                rows.iter()
                    .all(|row| row["metric"].get("__name__").is_none()),
                "Rate must drop the metric name: {body}"
            );
            let api = rows
                .iter()
                .find(|row| row["metric"]["job"] == "api")
                .unwrap();
            let worker = rows
                .iter()
                .find(|row| row["metric"]["job"] == "worker")
                .unwrap();
            if algorithm != "Sum" {
                assert_eq!(worker["metric"]["unreferenced_instance"], "d");
            }
            assert!(
                (worker["value"][1].as_str().unwrap().parse::<f64>().unwrap() - 2000.).abs() < 1e-8
            );
            if algorithm == "Sum" {
                assert!(
                    api["metric"].get("unreferenced_instance").is_none(),
                    "grouped Sum must drop ungrouped labels: {body}"
                );
            } else {
                assert_eq!(api["metric"]["unreferenced_instance"], winner, "{body}");
            }
            let value = api["value"][1].as_str().unwrap().parse::<f64>().unwrap();
            let score = score
                + if algorithm == "Sum" {
                    if at == 60. {
                        1.1
                    } else if at == 65. {
                        // b's zero-point extrapolation truncates the left edge:
                        // (980 * 45 / 41 + 20) / 60, plus c's constant 0.1.
                        (980. * 45. / 41. + 20.) / 60. + 0.1
                    } else {
                        0.1
                    }
                } else {
                    0.
                };
            assert!((value - score).abs() < 1e-8, "{body}, expected {score}");
            if restart {
                assert_eq!(body["data"]["result"], expected[index]);
            } else {
                expected.push(body["data"]["result"].clone());
            }
        }
        let missing: Value = client
            .get(format!("{backend}/api/v1/query"))
            .query(&[("query", query), ("time", "180")])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(
            !is_warm(&missing),
            "missing counter window was served as complete: {missing}"
        );
        // The drained source cohort is durable before the first process exits.
        assert!(disk.join("sketch_index/parts").is_dir());
    }
}
