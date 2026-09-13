//! Issue workloads execute their selected Planner DAG on the production HTTP path.
use super::*;
use control_plane::physical::{
    compiler::{
        BackendLocalPlanningSnapshot, PhysicalCompiler, BACKEND_REVISION, PLANNER_REVISION,
    },
    workload_cost::{self, WorkloadCostEvidence, WorkloadQuote},
};

// This fixture backfills fifteen minutes across many maintained populations.
// Its readiness budget covers ingestion, not a query latency benchmark.
async fn wait_for_issue_warm_instant(
    client: &reqwest::Client,
    base: &str,
    query: &str,
    at: f64,
    log_path: &std::path::Path,
) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let result: Value = client
            .get(format!("{base}/api/v1/query"))
            .query(&[("query", query.to_string()), ("time", at.to_string())])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if is_warm(&result) && first_value(&result, "value").is_some() {
            return result;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{query} did not become warm: {result}; log: {}",
            std::fs::read_to_string(log_path).unwrap_or_default()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn queries() -> Vec<(String, u64, u64)> {
    let mut queries = vec![];
    for q in [0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.9, 0.95, 0.99, 0.999] {
        queries.push((
            format!("quantile_over_time({q}, issue701_data[15m])"),
            900,
            60,
        ));
        queries.push((
            format!("quantile_over_time({q}, issue701_data[5m])"),
            300,
            if q == 0.9 { 30 } else { 10 },
        ));
        queries.push((format!("quantile by(job)({q}, issue701_data)"), 1, 1));
    }
    for operation in ["sum", "count", "avg", "min", "max"] {
        queries.push((format!("{operation}_over_time(issue701_data[5m])"), 300, 30));
    }
    for operation in ["sum", "count", "avg"] {
        queries.push((format!("{operation}(issue701_data)"), 1, 1));
        queries.push((format!("{operation} by(job)(issue701_data)"), 1, 1));
    }
    for operation in ["sum", "count"] {
        queries.push((
            format!("topk(5, {operation}_over_time(issue701_data[5m]))"),
            300,
            30,
        ));
    }
    queries.push((
        "sum by(job)(sum_over_time(issue701_data[5m]))".into(),
        300,
        30,
    ));
    queries.push((
        "quantile_over_time(0.9, issue701_data[5m]) / quantile_over_time(0.5, issue701_data[5m])"
            .into(),
        300,
        60,
    ));
    queries.push((
        "avg_over_time(issue701_data[5m]) / quantile_over_time(0.5, issue701_data[5m])".into(),
        300,
        30,
    ));
    queries
}

// A single mixed workload covers moving windows, current series, minimum/average,
// and ratio accuracy. Optional native URL adds a real Prometheus differential oracle.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn issue_workloads_execute_warm_at_successive_evaluations() {
    let native = std::env::var("ASAP_CURRENT_SERIES_PROMETHEUS_URL").ok();
    if let Some(url) = &native {
        let info: Value = reqwest::get(format!("{url}/api/v1/status/buildinfo"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let version = info["data"]["version"]
            .as_str()
            .expect("Prometheus version");
        assert!(
            version.starts_with("3."),
            "boundary-aligned oracle requires Prometheus 3.x left-open ranges, found {version}"
        );
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_url = format!("http://{}", listener.local_addr().unwrap());
    let _mock = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route("/-/healthy", get(|| async { "healthy" })),
        )
        .await
        .unwrap();
    });
    let queries = queries();
    let mut fixture: Value = serde_json::from_str(include_str!(
        "../../../docs/examples/asapquery-planning-snapshot.json"
    ))
    .unwrap();
    fixture["implementation"]["source_sample_interval_ms"] = 1000.into();
    fixture["implementation"]["horizon_seconds"] = 3600.into();
    fixture["implementation"]["implementation_cost"]["horizon_seconds"] = 3600.into();
    let template = fixture["query_workload"]["repeating_queries"][0].clone();
    fixture["query_workload"]["repeating_queries"] = queries
        .iter()
        .map(|(query, lookback, cadence)| {
            let mut entry = template.clone();
            entry["query"] = query.clone().into();
            entry["time_selection"]["lookback"] = (lookback * 1000).into();
            entry["demand"]["fixed_interval_at"]["interval"] = (cadence * 1000).into();
            if !query.contains("quantile") {
                entry["requirements"]["accuracy"] = serde_json::json!({"explicit":"Exact"});
            }
            entry
        })
        .collect();
    let mut snapshot: BackendLocalPlanningSnapshot = serde_json::from_value(fixture).unwrap();
    let (request, environment) = snapshot.clone().planning_request().unwrap();
    let candidates = workload_cost::with_exact_alternative(request).unwrap();
    let fully_warm = |plan: &control_plane::physical::compiler::PhysicalPlan| {
        plan.query_plan.entries.values().all(|entry| entry.nodes.values().all(|node| !matches!(node,
            control_plane::query_plan::QueryPlanNode::ExactFallback { .. }
            | control_plane::query_plan::QueryPlanNode::ExternalExact { .. }
            | control_plane::query_plan::QueryPlanNode::Logical {
                operator: control_plane::query_plan::logical::LogicalOperator::ExactSubquery { .. }, ..
            }
        )))
    };
    let mut errors = vec![];
    let mut found = false;
    let quotes = candidates
        .into_iter()
        .filter_map(|candidate| {
            let plan = match PhysicalCompiler.compile(candidate.clone(), environment.clone()) {
                Ok(plan) => plan,
                Err(error) => {
                    errors.push(error.to_string());
                    return None;
                }
            };
            let warm = fully_warm(&plan);

            found |= warm;
            let manifest = workload_cost::manifest(&plan, &candidate.queries).unwrap();
            Some(WorkloadQuote {
                unit_costs: manifest
                    .components
                    .keys()
                    .map(|key| (key.clone(), if warm { 1.0 } else { 1e12 }))
                    .collect(),
                manifest,
                executable: true,
            })
        })
        .collect();
    assert!(found, "no complete warm candidate: {errors:?}");
    snapshot.workload_cost_evidence = Some(WorkloadCostEvidence {
        backend_revision: BACKEND_REVISION.into(),
        planner_revision: PLANNER_REVISION.into(),
        data_snapshot_id: "issue-701-702-process".into(),
        model_version: "synthetic-correctness-quotes".into(),
        observed_at_unix_ms: environment.observed_at_unix_ms,
        valid_for_ms: environment.max_evidence_age_ms,
        quotes,
    });
    let plan = snapshot.clone().compile().unwrap();
    assert!(fully_warm(&plan));
    let output = tempfile::tempdir().unwrap();
    let path = output.path().join("snapshot.json");
    std::fs::write(&path, serde_json::to_vec(&snapshot).unwrap()).unwrap();
    let port = unused_port();
    let mut command = Command::new(env!("CARGO_BIN_EXE_data_plane"));
    command
        .args(["--profile", "asapquery", "--planning-snapshot"])
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
        .stderr(Stdio::inherit());
    command.args([
        "--prometheus-server",
        native.as_deref().unwrap_or(&mock_url),
        "--forward-unsupported-queries",
    ]);
    let mut child = ChildGuard(command.spawn().unwrap());
    let client = reqwest::Client::new();
    let backend = format!("http://127.0.0.1:{port}");
    wait_until_ready(&client, &format!("{backend}/api/v1/health"), &mut child.0).await;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let origin = now - now.rem_euclid(900_000) - 1_800_000;
    for (start, end) in [(1, 901), (902, 961)] {
        let wire = WriteRequest {
            timeseries: [("a", "api", 1.0), ("b", "api", 2.0), ("c", "db", 3.0)]
                .into_iter()
                .map(|(pod, job, factor)| {
                    let samples: Vec<_> = (start..=end)
                        .map(|i| (origin + i * 1000, factor * (1 + i % 31) as f64))
                        .collect();
                    series_with_labels("issue701_data", &[("pod", pod), ("job", job)], &samples)
                })
                .collect(),
        };
        if let Some(url) = &native {
            assert_eq!(remote_write(&client, url, &wire).await, 204);
        }
        assert_eq!(remote_write(&client, &backend, &wire).await, 204);
        for (query, _, _) in &queries {
            // Temporal reads trail the source watermark by one sample; current
            // populations are queried at the latest input, without historical replay.
            let evaluation = if query.contains('[') { end - 1 } else { end };
            let at = (origin + evaluation * 1000) as f64 / 1000.0;
            let actual = wait_for_issue_warm_instant(
                &client,
                &backend,
                query,
                at,
                &output.path().join("query_engine.log"),
            )
            .await;
            assert!(is_warm(&actual), "{query}: {actual}");
            if let Some(url) = &native {
                let expected: Value = client
                    .get(format!("{url}/api/v1/query"))
                    .query(&[("query", query.clone()), ("time", at.to_string())])
                    .send()
                    .await
                    .unwrap()
                    .json()
                    .await
                    .unwrap();
                let rows = |body: &Value| -> std::collections::BTreeMap<String, f64> {
                    body["data"]["result"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|row| {
                            (
                                serde_json::to_string(&row["metric"]).unwrap(),
                                row["value"][1].as_str().unwrap().parse().unwrap(),
                            )
                        })
                        .collect()
                };
                let (actual, expected) = (rows(&actual), rows(&expected));
                assert_eq!(
                    actual.keys().collect::<Vec<_>>(),
                    expected.keys().collect::<Vec<_>>(),
                    "{query}"
                );
                for (labels, truth) in expected {
                    let tolerance = if query.contains("quantile") {
                        0.01 * truth.abs()
                    } else {
                        1e-9 * truth.abs().max(1.0)
                    };
                    assert!(
                        (actual[&labels] - truth).abs() <= tolerance,
                        "{query}: {} vs {truth}",
                        actual[&labels]
                    );
                }
            }
        }
    }
    if let Some(url) = &native {
        let wire = WriteRequest {
            timeseries: [("a", "api"), ("b", "api"), ("c", "db")]
                .into_iter()
                .map(|(pod, job)| {
                    let samples: Vec<_> = (962..=1261).map(|i| (origin + i * 1000, 0.0)).collect();
                    series_with_labels("issue701_data", &[("pod", pod), ("job", job)], &samples)
                })
                .collect(),
        };
        assert_eq!(remote_write(&client, url, &wire).await, 204);
        assert_eq!(remote_write(&client, &backend, &wire).await, 204);
        let at = (origin + 1260 * 1000) as f64 / 1000.0;
        for (query, _, _) in queries.iter().filter(|(q, _, _)| q.contains(" / ")) {
            let params = [("query", query.clone()), ("time", at.to_string())];
            let expected: Value = client
                .get(format!("{url}/api/v1/query"))
                .query(&params)
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            let actual: Value = client
                .get(format!("{backend}/api/v1/query"))
                .query(&params)
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            assert!(
                !is_warm(&actual),
                "undefined relative error must fall back: {query}"
            );
            assert_eq!(
                actual["data"], expected["data"],
                "zero denominator: {query}"
            );
        }
    }
}

// Finite input can overflow sum; the installed average must fall back while zero stays warm.
#[tokio::test]
async fn temporal_average_overflow_falls_back_after_state_is_warm() {
    let native = std::env::var("ASAP_CURRENT_SERIES_PROMETHEUS_URL").ok();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_url = format!("http://{}", listener.local_addr().unwrap());
    let mock = tokio::spawn(async move {
        axum::serve(listener, Router::new()
            .route("/-/healthy", get(|| async { "healthy" }))
            .route("/api/v1/query", get(|| async { Json(serde_json::json!({"status":"success", "data":{"resultType":"vector", "result":[{"metric":{},"value":[0,"1e308"]}]}})) })))
            .await.unwrap();
    });
    let mut fixture: Value = serde_json::from_str(include_str!(
        "../../../docs/examples/asapquery-planning-snapshot.json"
    ))
    .unwrap();
    fixture["implementation"]["source_sample_interval_ms"] = 1000.into();
    let template = fixture["query_workload"]["repeating_queries"][0].clone();
    fixture["query_workload"]["repeating_queries"] = ["avg", "sum", "count"]
        .map(|op| {
            let mut entry = template.clone();
            entry["query"] = format!("{op}_over_time(average_overflow[5s])").into();
            entry["time_selection"]["lookback"] = 5000.into();
            entry["demand"]["fixed_interval_at"]["interval"] = 1000.into();
            entry["requirements"]["accuracy"] = serde_json::json!({"explicit":"Exact"});
            entry
        })
        .to_vec()
        .into();
    let snapshot = quote_snapshot_for_test(serde_json::from_value(fixture).unwrap());
    let plan = snapshot.clone().compile().unwrap();
    assert!(plan
        .query_plan
        .entries
        .values()
        .flat_map(|entry| entry.nodes.values())
        .any(|node| matches!(
            node,
            control_plane::query_plan::QueryPlanNode::Logical {
                operator: control_plane::query_plan::logical::LogicalOperator::Binary {
                    operation: control_plane::query_plan::logical::BinaryOperation::FiniteDiv,
                    ..
                },
                ..
            }
        )));
    let output = tempfile::tempdir().unwrap();
    let path = output.path().join("snapshot.json");
    std::fs::write(&path, serde_json::to_vec(&snapshot).unwrap()).unwrap();
    let port = unused_port();
    let mut command = Command::new(env!("CARGO_BIN_EXE_data_plane"));
    command
        .args(["--profile", "asapquery", "--planning-snapshot"])
        .arg(&path)
        .args(["--http-port", &port.to_string(), "--output-dir"])
        .arg(output.path())
        .args([
            "--precompute-allowed-lateness-ms",
            "0",
            "--precompute-flush-interval-ms",
            "25",
            "--prometheus-server",
            native.as_deref().unwrap_or(&mock_url),
            "--forward-unsupported-queries",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    let mut child = ChildGuard(command.spawn().unwrap());
    let client = reqwest::Client::new();
    let backend = format!("http://127.0.0.1:{port}");
    wait_until_ready(&client, &format!("{backend}/api/v1/health"), &mut child.0).await;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let origin = now - now.rem_euclid(1000) - 60_000;
    for (start, end, value) in [(1, 6, 0.0), (7, 12, 1e308)] {
        let samples: Vec<_> = (start..=end).map(|i| (origin + i * 1000, value)).collect();
        let wire = WriteRequest {
            timeseries: vec![series_with_labels("average_overflow", &[], &samples)],
        };
        if let Some(url) = &native {
            assert_eq!(remote_write(&client, url, &wire).await, 204);
        }
        assert_eq!(remote_write(&client, &backend, &wire).await, 204);
        let at = (origin + (end - 1) * 1000) as f64 / 1000.0;
        for op in ["sum", "count"] {
            wait_for_issue_warm_instant(
                &client,
                &backend,
                &format!("{op}_over_time(average_overflow[5s])"),
                at,
                &output.path().join("query_engine.log"),
            )
            .await;
        }
        let query = "avg_over_time(average_overflow[5s])";
        if value == 0.0 {
            let result = wait_for_issue_warm_instant(
                &client,
                &backend,
                query,
                at,
                &output.path().join("query_engine.log"),
            )
            .await;
            assert_eq!(first_value(&result, "value"), Some(0.0));
        } else {
            let params = [("query", query.to_string()), ("time", at.to_string())];
            let actual: Value = client
                .get(format!("{backend}/api/v1/query"))
                .query(&params)
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            assert!(
                !is_warm(&actual),
                "overflowed average must fall back: {actual}"
            );
            assert_eq!(first_value(&actual, "value"), Some(1e308), "{actual}");
            if let Some(url) = &native {
                let expected: Value = client
                    .get(format!("{url}/api/v1/query"))
                    .query(&params)
                    .send()
                    .await
                    .unwrap()
                    .json()
                    .await
                    .unwrap();
                assert_eq!(actual["data"], expected["data"]);
            }
        }
    }
    mock.abort();
}
