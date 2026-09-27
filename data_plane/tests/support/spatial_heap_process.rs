use super::*;
use control_plane::physical::{
    compiler::{
        BackendLocalPlanningInput, DeploymentPlanCompiler, BACKEND_REVISION, PLANNER_REVISION,
    },
    workload_cost::{
        enumerate_exact_and_materialized_candidates, manifest, WorkloadCostEvidence, WorkloadQuote,
    },
};

// Actual installed heap program: Remote Write -> current snapshot -> native
// CountSketch build/readout -> HTTP result. Updates never accumulate old weights.
#[tokio::test]
async fn spatial_heap_installs_and_serves_replacements_staleness_and_expiry() {
    let query = "topk by (job) (1, spatial_value)";
    let mut wire: Value = serde_json::from_str(include_str!(
        "../../../docs/examples/asapquery-compatibility-demo-snapshot.json"
    ))
    .unwrap();
    let mut entry = wire["query_workload"]["repeating_queries"][3].clone();
    entry["query"] = query.into();
    entry["requirements"]["accuracy"] =
        serde_json::json!({"explicit":{"EpsilonDelta":{"epsilon":0.1,"delta":0.1}}});
    wire["query_workload"]["repeating_queries"] = serde_json::json!([entry]);
    wire["implementation"]["topk_evidence"] = serde_json::json!({});
    wire["implementation"]["data_snapshot_id"] = "spatial-heap-process".into();
    // Across every tested snapshot, the winner is >=900, every excluded value
    // is <=10, and there are at most three complete series identities.
    wire["implementation"]["accuracy_evidence"] = serde_json::json!({query:{
        "query_string":query,"data_snapshot_id":"spatial-heap-process",
        "data_workload":wire["data_workload"],"source":"enforced-test-population",
        "observed_at_unix_ms":9500,"valid_for_ms":60000,"topk_max_distinct_items":3,
        "topk_selected_lower_bound":900.0,"topk_excluded_upper_bound":10.0,
        "topk_interval_failure_probability":0.001
    }});
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
                    .is_some_and(|program| program.to_string().contains("CountSketchWithHeap"))
            });
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
        data_snapshot_id: "spatial-heap-process".into(),
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
        .contains("CountSketchWithHeap"));
    entry.recover_population_physical_dag().unwrap();
    assert!(plan.precompute_plan.materializations.is_empty());

    let calls = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let observed = calls.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let fallback = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener,Router::new().route("/-/healthy",get(||async{"healthy"})).route("/api/v1/query",get(move ||{
            let observed=observed.clone();async move {
                observed.fetch_add(1,std::sync::atomic::Ordering::Relaxed);
                Json(serde_json::json!({"status":"success","data":{"resultType":"vector","result":[]}}))
            }
        }))).await.unwrap();
    });
    let output = tempfile::tempdir().unwrap();
    let path = output.path().join("snapshot.json");
    std::fs::write(&path, serde_json::to_vec(&snapshot).unwrap()).unwrap();
    let port = unused_port();
    let mut child = ChildGuard(
        Command::new(env!("CARGO_BIN_EXE_data_plane"))
            .args([
                "--profile",
                "asapquery",
                "--forward-unsupported-queries",
                "--planning-snapshot",
            ])
            .arg(path)
            .args([
                "--prometheus-server",
                &fallback,
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
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");
    wait_until_ready(&client, &format!("{base}/api/v1/health"), &mut child.0).await;
    let end = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let stale =
        f64::from_bits(data_plane::drivers::ingest::prometheus_remote_write::STALE_NAN_BITS);
    for (offset, samples, winner, score) in [
        (0, vec![("a", 1000.0), ("b", 10.0), ("c", 1.0)], "a", 1000.0),
        (1000, vec![("a", -5.0), ("b", 900.0)], "b", 900.0),
        (2000, vec![("b", stale), ("c", 1000.0)], "c", 1000.0),
        (7000, vec![("a", 900.0)], "a", 900.0),
    ] {
        let request = WriteRequest {
            timeseries: samples
                .into_iter()
                .map(|(instance, value)| {
                    series_with_labels(
                        "spatial_value",
                        &[("job", "api"), ("unreferenced_instance", instance)],
                        &if offset == 0 {
                            vec![(end - 5000, value), (end, value)]
                        } else {
                            vec![(end + offset, value)]
                        },
                    )
                })
                .collect(),
        };
        assert_eq!(remote_write(&client, &base, &request).await, 204);
        let body: Value = client
            .get(format!("{base}/api/v1/query"))
            .query(&[
                ("query", query.to_owned()),
                ("time", format!("{:.3}", (end + offset) as f64 / 1000.0)),
            ])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(is_warm(&body), "{body}");
        let rows = body["data"]["result"].as_array().unwrap();
        assert_eq!(rows.len(), 1, "{body}");
        assert_eq!(rows[0]["metric"]["unreferenced_instance"], winner, "{body}");
        let actual = rows[0]["value"][1]
            .as_str()
            .unwrap()
            .parse::<f64>()
            .unwrap();
        assert!((actual - score).abs() < 1e-8, "{body}");
    }
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 0);
    task.abort();
}
