use super::*;

// One deployed query ensemble reads shared counters. Adding a late series must
// revise both the persisted grouped sum and the query-time ranking, including
// after restart, without mixing snapshots or falling back to an external engine.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_rate_ensemble_revises_and_recovers() {
    run_native_ensemble(None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_cms_rate_ensemble_revises_and_recovers() {
    run_native_ensemble(Some("CmsWithHeap")).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_count_sketch_rate_ensemble_revises_and_recovers() {
    run_native_ensemble(Some("CountSketchWithHeap")).await;
}

async fn run_native_ensemble(family: Option<&str>) {
    use control_plane::physical::{
        compiler::{
            BackendLocalPlanningInput, DeploymentPlanCompiler, BACKEND_REVISION, PLANNER_REVISION,
        },
        workload_cost::{
            enumerate_exact_and_materialized_candidates, manifest, WorkloadCostEvidence,
            WorkloadQuote,
        },
    };
    let queries = [
        "sum by (job) (rate(native_revision_counter[1m]))",
        "topk by (job) (1, rate(native_revision_counter[1m]))",
    ];
    let mut fixture: Value = serde_json::from_str(include_str!(
        "../../../docs/examples/asapquery-compatibility-demo-snapshot.json"
    ))
    .unwrap();
    let template = fixture["query_workload"]["repeating_queries"][3].clone();
    fixture["query_workload"]["repeating_queries"] = Value::Array(
        queries
            .iter()
            .map(|query| {
                let mut entry = template.clone();
                entry["query"] = (*query).into();
                entry["demand"]["fixed_interval_at"]["interval"] = 60_000.into();
                entry["requirements"]["accuracy"] = serde_json::json!({"explicit":"Exact"});
                entry
            })
            .collect(),
    );
    fixture["implementation"]["data_snapshot_id"] = "native-revision-ensemble".into();
    if family.is_some() {
        fixture["query_workload"]["repeating_queries"][1]["requirements"]["accuracy"] =
            serde_json::json!({"explicit":{"EpsilonDelta":{"epsilon":0.1,"delta":0.1}}});
        fixture["implementation"]["accuracy_evidence"] = serde_json::json!({queries[1]: {
            "query_string": queries[1], "data_snapshot_id":"native-revision-ensemble",
            "data_workload": fixture["data_workload"], "source":"enforced-fixture-contract",
            "observed_at_unix_ms":9500, "valid_for_ms":60000,
            "topk_max_distinct_items":1000,
            "topk_selected_lower_bound":3.9, "topk_excluded_upper_bound":1.1,
            "topk_interval_failure_probability":0.001
        }});
    }
    let mut snapshot: BackendLocalPlanningInput = serde_json::from_value(fixture).unwrap();
    let (request, environment) = snapshot
        .clone()
        .into_physical_compilation_request()
        .unwrap();
    let preferred = |plan: &control_plane::physical::compiler::CompiledPhysicalPlan| {
        plan.precompute_plan.executable_dags.values().any(|dag| {
            dag.native_programs
                .values()
                .any(|program| family.is_none_or(|family| program.to_string().contains(family)))
        }) && plan.query_plan.entries.values().all(|entry| {
            !entry.nodes.values().any(|node| {
                matches!(
                    node,
                    asap_types::query_plan::QueryPlanNode::ExactFallback { .. }
                        | asap_types::query_plan::QueryPlanNode::ExternalExact { .. }
                )
            })
        })
    };
    let mut eligible = 0;
    let quotes = enumerate_exact_and_materialized_candidates(request)
        .unwrap()
        .into_iter()
        .filter_map(|candidate| {
            let plan = DeploymentPlanCompiler
                .compile_promql(candidate.clone(), environment.clone())
                .ok()?;
            let select = preferred(&plan);
            if select {
                eligible += 1;
            }
            let manifest = manifest(&plan, &candidate.queries).unwrap();
            Some(WorkloadQuote {
                unit_costs: manifest
                    .components
                    .keys()
                    .map(|key| (key.clone(), if select { 1.0 } else { 1e12 }))
                    .collect(),
                manifest,
                executable: true,
            })
        })
        .collect();
    assert!(
        eligible > 0,
        "ensemble has no feasible native precompute candidate"
    );
    snapshot.workload_cost_evidence = Some(WorkloadCostEvidence {
        backend_revision: BACKEND_REVISION.into(),
        planner_revision: PLANNER_REVISION.into(),
        data_snapshot_id: "native-revision-ensemble".into(),
        model_version: "native-ensemble-fixture-cost".into(),
        observed_at_unix_ms: 10000,
        valid_for_ms: 60000,
        quotes,
    });
    let plan = snapshot.compile_promql().unwrap();
    assert!(
        preferred(&plan),
        "synthetic costs must select the native ensemble"
    );
    assert_eq!(plan.query_plan.entries.len(), 2);
    let installed = data_plane::drivers::query::servers::http::PhysicalPlanInstallRequest {
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
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let start = (now / 60_000 - 1) * 60_000;
    let end = start + 60_000;
    let request = |instance: &str, last: f64| WriteRequest {
        timeseries: vec![series_with_labels(
            "native_revision_counter",
            &[("instance", instance), ("job", "worker")],
            &[(start + 1000, 100.), (end - 1000, last)],
        )],
    };
    let port = unused_port();
    let mut child = spawn(port);
    let backend = format!("http://127.0.0.1:{port}");
    wait_until_ready(&client, &format!("{backend}/api/v1/health"), &mut child.0).await;
    let (first, second, expected, winner) = if family.is_some() {
        (332., 158., [5., 4.], "a")
    } else {
        (158., 216., [3., 2.], "b")
    };
    assert_eq!(
        remote_write(&client, &backend, &request("a", first)).await,
        204
    );
    assert_ensemble(
        &client,
        &backend,
        &queries,
        end,
        if family.is_some() { [4., 4.] } else { [1., 1.] },
        "a",
    )
    .await;
    assert_eq!(
        remote_write(&client, &backend, &request("b", second)).await,
        204
    );
    assert_ensemble(&client, &backend, &queries, end, expected, winner).await;
    // Replay is idempotent; the newly admitted series must not be counted twice.
    assert_eq!(
        remote_write(&client, &backend, &request("b", second)).await,
        204
    );
    assert_ensemble(&client, &backend, &queries, end, expected, winner).await;
    drop(child);
    let port = unused_port();
    let mut child = spawn(port);
    let backend = format!("http://127.0.0.1:{port}");
    wait_until_ready(&client, &format!("{backend}/api/v1/health"), &mut child.0).await;
    assert_ensemble(&client, &backend, &queries, end, expected, winner).await;
}

async fn assert_ensemble(
    client: &reqwest::Client,
    backend: &str,
    queries: &[&str; 2],
    end: i64,
    expected: [f64; 2],
    winner: &str,
) {
    for (index, query) in queries.iter().enumerate() {
        let response = client
            .get(format!("{backend}/api/v1/query"))
            .query(&[
                ("query", query.to_string()),
                ("time", (end / 1000).to_string()),
            ])
            .send()
            .await
            .unwrap();
        let status = response.status();
        let body: Value = response.json().await.unwrap();
        assert!(status.is_success() && is_warm(&body), "{query}: {body}");
        assert_eq!(
            body["data"]["result"].as_array().unwrap().len(),
            1,
            "{body}"
        );
        let point = &body["data"]["result"][0];
        let value: f64 = point["value"][1].as_str().unwrap().parse().unwrap();
        assert!(
            (value - expected[index]).abs() < 1e-9,
            "{query}: expected {}, got {body}",
            expected[index]
        );
        assert_eq!(point["metric"]["job"], "worker");
        if index == 1 {
            assert_eq!(point["metric"]["instance"], winner);
        }
    }
}
