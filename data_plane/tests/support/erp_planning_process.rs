use super::*;
use control_plane::physical::{compiler::BackendLocalPlanningSnapshot, erp::ErpShapeObserver};

fn measured_profiles(raw: &[f64]) -> Value {
    let mut records = Vec::new();
    for k in [32, 128] {
        let mut error = 0.0f64;
        let mut bytes = 0usize;
        for seed in 0..10 {
            let mut sketch = asap_sketchlib::KllSketch::with_seed(k, seed);
            for value in raw {
                sketch.update(*value);
            }
            bytes = bytes.max(sketch.sketch_bytes().len());
            for q in 1..100 {
                let q = q as f64 / 100.0;
                let estimate = sketch.quantile(q);
                let lower = raw.iter().filter(|v| **v < estimate).count() as f64 / raw.len() as f64;
                let upper =
                    raw.iter().filter(|v| **v <= estimate).count() as f64 / raw.len() as f64;
                error = error.max((lower - q).max(q - upper).max(0.0));
            }
        }
        records.push(serde_json::json!({
            "id": format!("process-test-k{k}"), "sketch": "kll-percall", "implementation": "lib",
            "parameters": {"k": k}, "trials": 10,
            "distribution": {"erp_shape": {"family": "zipf", "parameters": {"exponent": 1.0},
                "cardinality": 16, "benchmark_events": raw.len()}},
            "error_metrics": {"max_rank_err": error},
            "resources": {"memory_bytes": bytes, "update_cpu_seconds": 0.0,
                "query_cpu_seconds": 0.0, "merge_cpu_seconds": 0.0}
        }));
    }
    // This correctness fixture measures error and retained serialized state.
    // CPU is not the objective or a performance claim in this process test.
    serde_json::json!({"schema_version": 1,
        "producer_version": "process-test-measured-error-and-serialized-state-cpu-not-measured",
        "records": records})
}

#[tokio::test]
async fn observed_shape_selects_installed_parameters_and_executes_remote_write() {
    let fallback_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let fallback_url = format!("http://{}", fallback_listener.local_addr().unwrap());
    let fallback_task = tokio::spawn(async move {
        axum::serve(
            fallback_listener,
            Router::new().route("/-/healthy", get(|| async { "healthy" })),
        )
        .await
        .unwrap();
    });
    const QUERY: &str = "quantile_over_time(0.9, erp_latency[5s])";
    let training: Vec<f64> = (1..=16)
        .flat_map(|value| std::iter::repeat_n(value as f64, 512 / value))
        .collect();
    let raw: Vec<f64> = (1..=16)
        .rev()
        .flat_map(|value| std::iter::repeat_n(value as f64, 256 / value))
        .collect();
    let mut observer = ErpShapeObserver::new(16).unwrap();
    for (index, value) in raw.iter().enumerate() {
        observer.observe(&value.to_string(), index / 100).unwrap();
    }
    let observation = observer.snapshot().unwrap();
    let artifact = measured_profiles(&training);
    let mut chosen = Vec::new();
    for only_large in [false, true] {
        let mut evidence = artifact.clone();
        if only_large {
            evidence["records"]
                .as_array_mut()
                .unwrap()
                .retain(|row| row["parameters"]["k"] == 128);
        }
        let mut fixture: Value = serde_json::from_str(include_str!(
            "../../../docs/examples/asapquery-compatibility-demo-snapshot.json"
        ))
        .unwrap();
        let mut entry = fixture["query_workload"]["repeating_queries"][3].clone();
        entry["query"] = QUERY.into();
        entry["requirements"]["accuracy"] = serde_json::json!({"explicit": {"Epsilon": 0.2}});
        fixture["query_workload"]["repeating_queries"] = serde_json::json!([entry]);
        fixture["implementation"]["erp"] = serde_json::json!({
            "distribution": {"workload": {"external": {"dataset": "held-out-process-stream"}}},
            "artifact": evidence, "implementation": "lib", "error_metric": "max_rank_err",
            "min_trials": 10, "expected_updates": raw.len(), "expected_queries": 10.0,
            "expected_merges": 0.0, "retention_seconds": 60.0, "cpu_weight": 0.0,
            "byte_second_weight": 1e-9, "mode": "hybrid", "observed_shape": observation.observation,
            "shape_match": {"minimum_benchmark_events": 1000, "max_log2_cardinality_distance": 0.0,
                "max_parameter_distance": 0.1, "max_goodness_of_fit": 0.1,
                "minimum_confidence": 0.8, "minimum_confidence_margin": 0.05},
            "runtime": {"allowed_algorithms": ["Kll"], "max_memory_bytes": null}
        });
        let policy: control_plane::physical::erp::ErpPlanningInput =
            serde_json::from_value(fixture["implementation"]["erp"].clone()).unwrap();
        assert!(matches!(
            policy.select(
                planner_types::post_asap::SketchAlgorithm::Kll,
                0.2,
                planner_types::post_asap::SketchParams::Kll { k: 128 }
            ),
            control_plane::physical::erp::ErpParameterDecision::Empirical { .. }
        ));
        let snapshot: BackendLocalPlanningSnapshot =
            serde_json::from_value(fixture.clone()).unwrap();
        let plan = quote_snapshot_for_test(snapshot).compile().unwrap();
        assert_eq!(
            plan.precompute_plan.materializations.len(),
            1,
            "plan={plan:#?}; observation={observation:#?}; evidence={artifact}"
        );
        let expected_k = if only_large { 128 } else { 32 };
        assert_eq!(
            plan.precompute_plan.materializations[0].parameters["k"],
            expected_k
        );
        chosen.push(plan.precompute_plan.materializations[0].policy_fingerprint());
        eprintln!(
            "ERP_PLANNED {}",
            serde_json::json!({
                "query": QUERY, "available_profiles": policy.artifact.records,
            "parameter_decision": format!("{:?}", policy.select(planner_types::post_asap::SketchAlgorithm::Kll, 0.2, planner_types::post_asap::SketchParams::Kll { k: 128 })),
            "lifecycle_estimates": plan.lifecycle_estimates,
                "observation": policy.observed_shape,
                "selected_parameters": plan.precompute_plan.materializations[0].parameters,
                "materialization": chosen.last(),
                "partitioning": plan.precompute_plan.materializations[0].partitioning,
                "query_plan": plan.query_plan,
            })
        );
        let output = tempfile::tempdir().unwrap();
        let path = output.path().join("planning.json");
        let priced = quote_snapshot_for_test(serde_json::from_value(fixture.clone()).unwrap());
        std::fs::write(&path, serde_json::to_vec(&priced).unwrap()).unwrap();
        let port = unused_port();
        let mut child = ChildGuard(
            Command::new(env!("CARGO_BIN_EXE_data_plane"))
                .args([
                    "--forward-unsupported-queries",
                    "--prometheus-server",
                    &fallback_url,
                    "--profile",
                    "asapquery",
                    "--planning-snapshot",
                ])
                .arg(&path)
                .args(["--http-port", &port.to_string(), "--output-dir"])
                .arg(output.path())
                .args([
                    "--precompute-allowed-lateness-ms",
                    "0",
                    "--precompute-flush-interval-ms",
                    "25",
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap(),
        );
        let client = reqwest::Client::new();
        let backend = format!("http://127.0.0.1:{port}");
        wait_until_ready(&client, &format!("{backend}/api/v1/health"), &mut child.0).await;
        let config: Value = client
            .get(format!("{backend}/api/v1/physical-plan/status"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let config_text = serde_json::to_string(&config).unwrap();
        assert!(
            config_text.contains(&chosen.last().unwrap().0.to_string()),
            "installed ERP identity missing: {config}"
        );
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let base = now - now.rem_euclid(5000) - 20000;
        let samples: Vec<_> = raw
            .iter()
            .enumerate()
            .map(|(i, value)| (base + 1 + i as i64, *value))
            .collect();
        assert_eq!(
            remote_write(
                &client,
                &backend,
                &WriteRequest {
                    timeseries: vec![
                        series_with_labels("erp_latency", &[("instance", "a")], &samples),
                        series_with_labels(
                            "erp_latency",
                            &[("instance", "b")],
                            &samples
                                .iter()
                                .map(|(t, v)| (*t, *v + 1000.0))
                                .collect::<Vec<_>>()
                        ),
                    ]
                }
            )
            .await,
            204
        );
        assert_eq!(
            remote_write(
                &client,
                &backend,
                &WriteRequest {
                    timeseries: vec![
                        series_with_labels(
                            "erp_latency",
                            &[("instance", "a")],
                            &[(base + 15001, 1.0)]
                        ),
                        series_with_labels(
                            "erp_latency",
                            &[("instance", "b")],
                            &[(base + 15001, 1001.0)]
                        ),
                    ]
                }
            )
            .await,
            204
        );
        drain_precompute(&client, &backend).await;
        let result = wait_for_warm_instant(
            &client,
            &backend,
            QUERY,
            (base + 5000) as f64 / 1000.0,
            &output.path().join("query_engine.log"),
        )
        .await;
        let rows = result["data"]["result"].as_array().unwrap();
        assert_eq!(
            rows.len(),
            2,
            "per-series KLL states must not pool: {result}"
        );
        eprintln!(
            "ERP_WARM {}",
            serde_json::json!({"materialization": chosen.last(), "result": result})
        );
        for (instance, offset) in [("a", 0.0), ("b", 1000.0)] {
            let row = rows
                .iter()
                .find(|row| row["metric"]["instance"] == instance)
                .expect("source labels retained");
            let estimate = row["value"][1].as_str().unwrap().parse::<f64>().unwrap() - offset;
            let lower = raw.iter().filter(|v| **v < estimate).count() as f64 / raw.len() as f64;
            let upper = raw.iter().filter(|v| **v <= estimate).count() as f64 / raw.len() as f64;
            assert!((lower - 0.9).max(0.9 - upper).max(0.0) <= 0.2, "{result}");
        }
    }
    fallback_task.abort();
    assert_ne!(
        chosen[0], chosen[1],
        "changed evidence must change installed state identity"
    );
}
