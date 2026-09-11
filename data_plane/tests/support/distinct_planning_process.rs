use super::*;
use control_plane::physical::compiler::BackendLocalPlanningSnapshot;

/// The production compiler, ingest engine and query DAG preserve distinct populations.
#[tokio::test]
async fn distinct_range_uses_planner_selected_hll_and_source_labels() {
    const QUERY: &str = "distinct_over_time(distinct_values{job=\"api\"}[5s])";
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
    let mut fixture: Value = serde_json::from_str(include_str!(
        "../../../docs/examples/asapquery-compatibility-demo-snapshot.json"
    ))
    .unwrap();
    let mut entry = fixture["query_workload"]["repeating_queries"][3].clone();
    entry["query"] = QUERY.into();
    entry["requirements"]["accuracy"] = serde_json::json!({"explicit": {"Epsilon": 0.05}});
    fixture["query_workload"]["repeating_queries"] = serde_json::json!([entry]);
    let plan = serde_json::from_value::<BackendLocalPlanningSnapshot>(fixture.clone())
        .unwrap()
        .compile()
        .unwrap();
    assert_eq!(plan.precompute_plan.materializations.len(), 1);
    assert_eq!(
        plan.precompute_plan.materializations[0].aggregation_type,
        asap_types::AggregationType::HLL
    );
    eprintln!(
        "DISTINCT_PLANNED {}",
        serde_json::json!({"materializations": plan.precompute_plan.materializations, "query_plan": plan.query_plan, "lifecycle_estimates": plan.lifecycle_estimates})
    );
    let output = tempfile::tempdir().unwrap();
    let path = output.path().join("planning.json");
    std::fs::write(&path, serde_json::to_vec(&fixture).unwrap()).unwrap();
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
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let base = now - now.rem_euclid(5000) - 20000;
    let mut series = Vec::new();
    for (instance, distinct, job) in [("a", 5, "api"), ("b", 13, "api"), ("excluded", 23, "other")]
    {
        let mut samples: Vec<_> = (0..100)
            .map(|i| (base + 1 + i, (i % distinct) as f64))
            .collect();
        samples.push((base + 15001, 1000.0));
        series.push(series_with_labels(
            "distinct_values",
            &[("instance", instance), ("job", job)],
            &samples,
        ));
    }
    assert_eq!(
        remote_write(&client, &backend, &WriteRequest { timeseries: series }).await,
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
    assert_eq!(rows.len(), 2, "{result}");
    for (instance, exact) in [("a", 5.0), ("b", 13.0)] {
        let row = rows
            .iter()
            .find(|row| row["metric"]["instance"] == instance)
            .unwrap();
        let estimate = row["value"][1].as_str().unwrap().parse::<f64>().unwrap();
        assert!((estimate - exact).abs() / exact <= 0.05, "{result}");
    }
    eprintln!("DISTINCT_WARM {result}");
    fallback_task.abort();
}
