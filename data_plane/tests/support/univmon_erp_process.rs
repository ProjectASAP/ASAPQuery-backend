use super::*;
use control_plane::physical::{compiler::BackendLocalPlanningSnapshot, erp::ErpShapeObserver};
use data_plane::precompute_engine::operators::univmon_accumulator::UnivMonAccumulator;
use data_plane::storage_engines::types::{AggregateCore, SerializableToSink};

fn values(offset: usize) -> Vec<f64> {
    (1..=128)
        .flat_map(|key| std::iter::repeat_n((key + offset) as f64, 256 / key))
        .collect()
}

fn truth(raw: &[f64]) -> [f64; 3] {
    let mut counts = std::collections::HashMap::<u64, usize>::new();
    for value in raw {
        *counts.entry(value.to_bits()).or_default() += 1;
    }
    let l2 = counts
        .values()
        .map(|n| (*n as f64).powi(2))
        .sum::<f64>()
        .sqrt();
    let entropy = counts
        .values()
        .map(|n| {
            let p = *n as f64 / raw.len() as f64;
            -p * p.log2()
        })
        .sum();
    [counts.len() as f64, l2, entropy]
}

fn measured_artifact() -> Value {
    let mut records = Vec::new();
    for (heap, cols) in [(8, 128), (128, 1024)] {
        let mut errors = [0.0f64; 3];
        let mut bytes = 0;
        // Independent key populations exercise fixed implementation hashes.
        // These are empirical trials, not a claimed tail-probability bound.
        for trial in 0..10 {
            let raw = values(trial * 1000);
            let exact = truth(&raw);
            let mut panes = [
                UnivMonAccumulator::new(heap, 5, cols, 4).unwrap(),
                UnivMonAccumulator::new(heap, 5, cols, 4).unwrap(),
            ];
            for (i, value) in raw.iter().enumerate() {
                panes[i % 2].insert_sample(*value).unwrap();
            }
            let other = panes[1].clone();
            panes[0].merge_in_place(&other).unwrap();
            bytes = bytes.max(panes[0].serialize_to_bytes().len());
            for (i, stat) in [
                asap_types::Statistic::Cardinality,
                asap_types::Statistic::FrequencyL2,
                asap_types::Statistic::FrequencyEntropy,
            ]
            .into_iter()
            .enumerate()
            {
                let estimate = panes[0]
                    .query_statistic(stat, &None, &Default::default())
                    .unwrap();
                assert!(estimate.is_finite());
                let error = (estimate - exact[i]).abs() / if i == 2 { 1.0 } else { exact[i] };
                errors[i] = errors[i].max(error);
            }
        }
        records.push(serde_json::json!({
            "id": format!("univmon-unit-frequency-h{heap}-c{cols}"),
            "sketch": "univmon", "implementation": "asap-sketchlib-univmon-standard-v1",
            "parameters": {"heap_size": heap, "sketch_rows": 5, "sketch_cols": cols, "layers": 4},
            "trials": 10,
            "distribution": {"erp_shape": {"family": "zipf", "parameters": {"exponent": 1.0}, "cardinality": 128, "benchmark_events": values(0).len()}},
            "error_metrics": {
                "max_cardinality_relative_error": errors[0],
                "max_frequency_l2_relative_error": errors[1],
                "max_frequency_entropy_absolute_bits_error": errors[2]
            },
            "resources": {"memory_bytes": bytes, "update_cpu_seconds": 0.0, "query_cpu_seconds": 0.0, "merge_cpu_seconds": 0.0}
        }));
    }
    serde_json::json!({"schema_version": 1, "producer_version": "backend-univmon-standard-unit-frequency-two-pane-error-fixture-cpu-not-measured", "records": records})
}

#[tokio::test]
async fn measured_readout_evidence_selects_and_executes_univmon() {
    let artifact = measured_artifact();
    eprintln!("UNIVMON_MEASURED {artifact}");
    let raw = values(100_000);
    let exact = truth(&raw);
    let mut observer = ErpShapeObserver::new(128).unwrap();
    for (i, value) in raw.iter().enumerate() {
        observer.observe(&value.to_string(), i / 100).unwrap();
    }
    let observation = observer.snapshot().unwrap();
    let queries = [
        "distinct_over_time(erp_frequency[5s])",
        "l2_over_time(erp_frequency[5s])",
        "entropy_over_time(erp_frequency[5s])",
    ];
    let mut fixture: Value = serde_json::from_str(include_str!(
        "../../../docs/examples/asapquery-compatibility-demo-snapshot.json"
    ))
    .unwrap();
    let template = fixture["query_workload"]["repeating_queries"][3].clone();
    fixture["query_workload"]["repeating_queries"] = queries
        .iter()
        .map(|query| {
            let mut entry = template.clone();
            entry["query"] = (*query).into();
            entry["requirements"]["accuracy"] = serde_json::json!({"explicit": {"Epsilon": 0.2}});
            entry
        })
        .collect::<Vec<_>>()
        .into();
    fixture["implementation"]["erp"] = serde_json::json!({
        "distribution": {"workload": {"external": {"dataset": "held-out-frequency-population"}}},
        "artifact": artifact, "implementation": null, "error_metric": "readout_specific",
        "min_trials": 10, "expected_updates": raw.len(), "expected_queries": 10.0,
        "expected_merges": 1.0, "retention_seconds": 60.0, "cpu_weight": 0.0,
        "byte_second_weight": 1e-9, "mode": "hybrid", "observed_shape": observation.observation,
        "shape_match": {"minimum_benchmark_events": 1000, "max_log2_cardinality_distance": 0.0,
            "max_parameter_distance": 0.2, "max_goodness_of_fit": 0.2,
            "minimum_confidence": 0.7, "minimum_confidence_margin": 0.05},
        "runtime": {"allowed_algorithms": ["Hll", "Kll", "UnivMon"], "max_memory_bytes": null}
    });
    let snapshot: BackendLocalPlanningSnapshot = serde_json::from_value(fixture.clone()).unwrap();
    let plan = snapshot.compile().unwrap();
    eprintln!(
        "UNIVMON_PLANNED {}",
        serde_json::json!({"query_plan": plan.query_plan, "materializations": plan.precompute_plan.materializations, "lifecycle_estimates": plan.lifecycle_estimates, "executable_dags": plan.precompute_plan.executable_dags, "observation": observation})
    );
    assert!(
        plan.precompute_plan
            .materializations
            .iter()
            .any(|m| m.aggregation_type == asap_types::AggregationType::UnivMon),
        "{plan:#?}"
    );
    // Removing only entropy evidence must leave the L2 path executable.
    let mut missing_entropy = fixture.clone();
    for row in missing_entropy["implementation"]["erp"]["artifact"]["records"]
        .as_array_mut()
        .unwrap()
    {
        row["error_metrics"]
            .as_object_mut()
            .unwrap()
            .remove("max_frequency_entropy_absolute_bits_error");
    }
    let missing = serde_json::from_value::<BackendLocalPlanningSnapshot>(missing_entropy)
        .unwrap()
        .compile()
        .unwrap();
    use control_plane::query_plan::{QueryPlanNode, QueryReadout};
    assert!(missing
        .query_plan
        .entries
        .values()
        .flat_map(|e| e.nodes.values())
        .any(|node| matches!(
            node,
            QueryPlanNode::SummaryEstimate {
                query: QueryReadout::FrequencyL2,
                ..
            }
        )));
    let entropy = missing
        .query_plan
        .entries
        .values()
        .find(|e| e.canonical_query.starts_with("entropy_over_time"))
        .unwrap();
    assert!(
        entropy.nodes.values().any(|node| matches!(
            node,
            QueryPlanNode::ExactFallback { .. } | QueryPlanNode::ExternalExact { .. }
        )),
        "{entropy:#?}"
    );
    assert!(!entropy.nodes.values().any(|node| matches!(
        node,
        QueryPlanNode::SummaryEstimate {
            query: QueryReadout::FrequencyEntropy,
            ..
        }
    )));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let fallback_url = format!("http://{}", listener.local_addr().unwrap());
    let fallback = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route("/-/healthy", get(|| async { "healthy" })),
        )
        .await
        .unwrap();
    });
    let runtime_samples = control_plane::runtime_samples::RuntimeSamplesStore::new(8);
    let runtime_port = unused_port();
    let runtime_endpoint = format!("http://127.0.0.1:{runtime_port}");
    let runtime_service =
        control_plane::runtime_samples::RuntimeSamplesService::new(runtime_samples.clone())
            .into_server();
    let runtime_task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(runtime_service)
            .serve(([127, 0, 0, 1], runtime_port).into())
            .await
            .unwrap();
    });
    let output = tempfile::tempdir().unwrap();
    let path = output.path().join("planning.json");
    std::fs::write(&path, serde_json::to_vec(&fixture).unwrap()).unwrap();
    let port = unused_port();
    let mut child = ChildGuard(
        Command::new(env!("CARGO_BIN_EXE_data_plane"))
            .args(["--erp-runtime-samples-endpoint", &runtime_endpoint])
            .args(["--profile", "asapquery", "--planning-snapshot"])
            .arg(&path)
            .args([
                "--prometheus-server",
                &fallback_url,
                "--forward-unsupported-queries",
                "--http-port",
                &port.to_string(),
                "--output-dir",
            ])
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
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let base = now - now.rem_euclid(5000) - 20_000;
    let mut samples: Vec<_> = raw
        .iter()
        .enumerate()
        .map(|(i, v)| (base + 1 + i as i64, *v))
        .collect();
    // Declared finite source includes the preceding boundary; this sample is
    // outside the query's left-open range and does not alter its truth.
    samples.insert(0, (base, 0.0));
    assert_eq!(
        remote_write(
            &client,
            &backend,
            &WriteRequest {
                timeseries: vec![series("erp_frequency", &samples)]
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
                timeseries: vec![series("erp_frequency", &[(base + 15001, 0.0)])]
            }
        )
        .await,
        204
    );
    drain_precompute(&client, &backend).await;
    let keys = runtime_samples.keys();
    assert!(
        !keys.is_empty(),
        "real worker inputs must reach RuntimeSamples after finite drain"
    );
    for key in keys {
        let record = runtime_samples.latest(&key).unwrap();
        let observed: asap_types::erp_observation::ErpPopulationObservations<
            control_plane::physical::erp::ErpObservedShape,
        > = serde_json::from_value(record.payload["erp_population_observations"].clone()).unwrap();
        assert!(observed.invalid_reason.is_none(), "{observed:?}");
        assert!(!observed.populations.is_empty());
        assert_eq!(observed.window_end_ms - observed.window_start_ms, 5000);
        assert!(plan
            .summary_catalog
            .materializations
            .contains_key(&observed.summary_definition_id));
        assert_eq!(
            observed.catalog_generation,
            plan.summary_catalog.reference().unwrap()
        );
        if key.sketch == "univmon" {
            let mut live_snapshot: BackendLocalPlanningSnapshot =
                serde_json::from_value(fixture.clone()).unwrap();
            let policy = live_snapshot.implementation.erp.as_mut().unwrap();
            policy.observed_shape_source =
                Some(control_plane::physical::erp::ErpObservedShapeSource {
                    source: key.source.clone(),
                    sketch: key.sketch.clone(),
                    implementation: key.impl_name.clone(),
                    population_scope: Some(
                        control_plane::physical::erp::ErpPopulationObservationScope {
                            catalog_generation: observed.catalog_generation.clone(),
                            summary_definition_id: observed.summary_definition_id,
                            input_semantics: observed.input_semantics,
                            freshness: asap_types::erp_observation::ErpObservationFreshness {
                                max_age_ms: 60_000,
                                max_future_skew_ms: 1000,
                            },
                        },
                    ),
                });
            policy.hydrate_observed_shape(&runtime_samples).unwrap();
            policy.resolve_population_data_descriptor(Some(&plan.summary_catalog));
            assert!(policy
                .observed_populations
                .as_ref()
                .unwrap()
                .invalid_reason
                .is_none());
            let replanned = live_snapshot.compile().unwrap();
            assert!(
                replanned
                    .precompute_plan
                    .materializations
                    .iter()
                    .any(|m| m.aggregation_type == asap_types::AggregationType::UnivMon),
                "actual producer evidence should reach normal Planner selection"
            );
        }
    }
    runtime_task.abort();

    for (i, query) in queries.iter().enumerate() {
        let result = wait_for_warm_instant(
            &client,
            &backend,
            query,
            (base + 5000) as f64 / 1000.0,
            &output.path().join("query_engine.log"),
        )
        .await;
        let estimate = first_value(&result, "value").unwrap();
        let error = (estimate - exact[i]).abs() / if i == 2 { 1.0 } else { exact[i] };
        assert!(
            error <= 0.2,
            "{query}: {result}, truth={}, error={error}",
            exact[i]
        );
        eprintln!(
            "UNIVMON_WARM {}",
            serde_json::json!({"query": query, "result": result, "truth": exact[i], "measured_error": error, "units": if i == 2 { "absolute_bits" } else { "relative" }})
        );
    }
    fallback.abort();
}
