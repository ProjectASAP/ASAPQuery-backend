use super::*;
use control_plane::physical::{
    compiler::{
        BackendLocalPlanningSnapshot, PhysicalCompiler, BACKEND_REVISION, PLANNER_REVISION,
    },
    workload_cost::{self, WorkloadCostEvidence, WorkloadQuote},
};

/// Remote Write updates one current population; quantiles and different TopK limits share it.
#[tokio::test]
async fn current_series_quantiles_topk_share_and_replace_values() {
    let calls = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let observed = calls.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let fallback = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener,Router::new()
        .route("/-/healthy",get(||async {"healthy"}))
        .route("/api/v1/query",get(move || {let observed=observed.clone();async move {
            observed.fetch_add(1,std::sync::atomic::Ordering::Relaxed);
            Json(serde_json::json!({"status":"success","data":{"resultType":"vector","result":[{"metric":{"fallback":"true"},"value":[0,"999"]}]}}))
        }}))).await.unwrap();
    });
    let native = std::env::var("ASAP_CURRENT_SERIES_PROMETHEUS_URL").ok();
    let fallback = native.clone().unwrap_or(fallback);
    let mut snapshot: BackendLocalPlanningSnapshot = serde_json::from_str(include_str!(
        "../../../docs/examples/asapquery-planning-snapshot.json"
    ))
    .unwrap();
    snapshot.snapshot_version = 2;
    snapshot.implementation.source_sample_interval_ms = Some(60_000);
    let template = snapshot.query_workload.repeating_queries.as_ref().unwrap()[0].clone();
    let mut queries = vec![];
    for q in [0.5, 0.9, 0.95, 0.99] {
        queries.push(format!("quantile({q}, a)"));
        queries.push(format!("quantile by (job) ({q}, a)"));
    }
    for k in [1, 2, 3] {
        queries.push(format!("topk({k}, a)"));
        queries.push(format!("topk by (job) ({k}, a)"));
    }
    for operation in ["sum", "count", "avg"] {
        queries.push(format!("{operation}(a)"));
        queries.push(format!("{operation} by (job) (a)"));
    }
    snapshot.query_workload.repeating_queries = Some(
        queries
            .iter()
            .map(|q| {
                let mut e = template.clone();
                e.query = planner_types::workload::Query(q.clone());
                e
            })
            .collect(),
    );
    let (request, env) = snapshot.clone().planning_request().unwrap();
    let candidates = workload_cost::with_exact_alternative(request).unwrap();
    let quotes = candidates
        .into_iter()
        .filter_map(|candidate| {
            let plan = PhysicalCompiler
                .compile(candidate.clone(), env.clone())
                .ok()?;
            let warm = candidate.queries.iter().all(|query| {
                matches!(
                    &query.post_asap.expr,
                    planner_types::post_asap::SummaryExpr::ValueOperation {
                        operation: planner_types::post_asap::ValueOperation::ReadPopulation { .. },
                        ..
                    }
                )
            });
            let manifest = workload_cost::manifest(&plan, &candidate.queries).unwrap();
            Some(WorkloadQuote {
                unit_costs: manifest
                    .components
                    .keys()
                    .map(|key| (key.clone(), if warm { 1. } else { 1e12 }))
                    .collect(),
                manifest,
                executable: true,
            })
        })
        .collect();
    snapshot.workload_cost_evidence = Some(WorkloadCostEvidence {
        backend_revision: BACKEND_REVISION.into(),
        planner_revision: PLANNER_REVISION.into(),
        data_snapshot_id: "current-series-process-test".into(),
        model_version: "synthetic-test-only".into(),
        observed_at_unix_ms: env.observed_at_unix_ms,
        valid_for_ms: env.max_evidence_age_ms,
        quotes,
    });
    let planned = snapshot.clone().compile().unwrap();
    assert!(
        planned
            .query_plan
            .entries
            .values()
            .all(|e| serde_json::to_string(e).unwrap().contains("current_series")),
        "non-current entries: {:?}",
        planned
            .query_plan
            .entries
            .values()
            .filter(|e| !serde_json::to_string(e).unwrap().contains("current_series"))
            .map(|e| (&e.canonical_query, &e.nodes))
            .collect::<Vec<_>>()
    );
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
            .arg(&path)
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
    let times: Vec<_> = (0..=5).map(|i| end - 300_000 + i * 60_000).collect();
    let wire = WriteRequest {
        timeseries: [
            ("x", "api", 1.),
            ("y", "api", 9.),
            ("z", "api", 5.),
            ("w", "db", 50.),
        ]
        .into_iter()
        .map(|(pod, job, value)| {
            series_with_labels(
                "a",
                &[("pod", pod), ("job", job)],
                &times.iter().map(|t| (*t, value)).collect::<Vec<_>>(),
            )
        })
        .collect(),
    };
    if let Some(url) = &native {
        assert_eq!(remote_write(&client, url, &wire).await, 204);
    }
    assert_eq!(remote_write(&client, &base, &wire).await, 204);
    async fn query(client: &reqwest::Client, base: &str, q: &str, at: i64) -> Value {
        client
            .get(format!("{base}/api/v1/query"))
            .query(&[
                ("query", q.to_string()),
                ("time", format!("{:.3}", at as f64 / 1000.)),
            ])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }
    async fn compare_native(
        client: &reqwest::Client,
        native: &Option<String>,
        q: &str,
        at: i64,
        actual: &Value,
    ) {
        if let Some(url) = native {
            let expected = query(client, url, q, at).await;
            let normalize = |body: &Value| {
                let mut rows: Vec<_> = body["data"]["result"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|row| {
                        (
                            serde_json::to_string(&row["metric"]).unwrap(),
                            row["value"][1].as_str().unwrap().parse::<f64>().unwrap(),
                        )
                    })
                    .collect();
                rows.sort_by(|a, b| a.0.cmp(&b.0));
                rows
            };
            let a = normalize(actual);
            let b = normalize(&expected);
            assert_eq!(a.len(), b.len(), "{q}: {actual} vs {expected}");
            for (a, b) in a.iter().zip(&b) {
                assert_eq!(a.0, b.0);
                assert!((a.1 - b.1).abs() < 1e-8, "{q}: {a:?} vs {b:?}");
            }
        }
    }
    for (q, global, grouped) in [
        (0.5, 7., 5.),
        (0.9, 37.7, 8.2),
        (0.95, 43.85, 8.6),
        (0.99, 48.77, 8.92),
    ] {
        for (text, expected) in [
            (format!("quantile({q}, a)"), global),
            (format!("quantile by (job) ({q}, a)"), grouped),
        ] {
            let body = query(&client, &base, &text, end).await;
            assert!(is_warm(&body), "{text}: {body}");
            compare_native(&client, &native, &text, end, &body).await;
            let value = body["data"]["result"][0]["value"][1]
                .as_str()
                .unwrap()
                .parse::<f64>()
                .unwrap();
            assert!((value - expected).abs() < 1e-8, "{text}: {body}");
        }
    }
    for k in [1, 2, 3] {
        let global = query(&client, &base, &format!("topk({k}, a)"), end).await;
        assert!(is_warm(&global), "{global}");
        compare_native(&client, &native, &format!("topk({k}, a)"), end, &global).await;
        assert_eq!(global["data"]["result"].as_array().unwrap().len(), k);
        let body = query(&client, &base, &format!("topk by (job) ({k}, a)"), end).await;
        assert!(is_warm(&body), "{body}");
        compare_native(
            &client,
            &native,
            &format!("topk by (job) ({k}, a)"),
            end,
            &body,
        )
        .await;
        assert_eq!(
            body["data"]["result"].as_array().unwrap().len(),
            k + 1,
            "{body}"
        );
    }
    for operation in ["sum", "count", "avg"] {
        for text in [
            format!("{operation}(a)"),
            format!("{operation} by (job) (a)"),
        ] {
            let body = query(&client, &base, &text, end).await;
            assert!(is_warm(&body), "{text}: {body}");
            compare_native(&client, &native, &text, end, &body).await;
        }
    }
    let metrics = client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        metrics.contains("asap_current_series_populations 2\n"),
        "{metrics}"
    );
    assert!(
        metrics.contains("asap_current_series_cache_builds_total 3\n"),
        "{metrics}"
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 0);
    // A decreasing value and a stale marker must promote a formerly excluded series.
    for (pod, value, offset, expected) in [
        ("y", -5., 1000, "z"),
        (
            "z",
            f64::from_bits(data_plane::drivers::ingest::prometheus_remote_write::STALE_NAN_BITS),
            2000,
            "x",
        ),
    ] {
        let update = WriteRequest {
            timeseries: vec![series_with_labels(
                "a",
                &[("pod", pod), ("job", "api")],
                &[(end + offset, value)],
            )],
        };
        if let Some(url) = &native {
            assert_eq!(remote_write(&client, url, &update).await, 204);
        }
        assert_eq!(remote_write(&client, &base, &update).await, 204);
        let body = query(&client, &base, "topk by (job) (1, a)", end + offset).await;
        assert!(is_warm(&body), "{body}");
        assert_eq!(
            body["data"]["result"][0]["metric"]["pod"], expected,
            "{body}"
        );
        compare_native(
            &client,
            &native,
            "topk by (job) (1, a)",
            end + offset,
            &body,
        )
        .await;
        for operation in ["sum", "count", "avg"] {
            for text in [
                format!("{operation}(a)"),
                format!("{operation} by (job) (a)"),
            ] {
                let body = query(&client, &base, &text, end + offset).await;
                assert!(is_warm(&body), "{text}: {body}");
                compare_native(&client, &native, &text, end + offset, &body).await;
            }
        }
        let body = query(&client, &base, "quantile by (job) (0.5, a)", end + offset).await;
        assert!(is_warm(&body), "{body}");
        compare_native(
            &client,
            &native,
            "quantile by (job) (0.5, a)",
            end + offset,
            &body,
        )
        .await;
    }
    // Historical reads cannot use a state already updated beyond their evaluation time.
    let body = query(&client, &base, "quantile(0.5, a)", end).await;
    assert!(!is_warm(&body), "historical read must fall back: {body}");
    if native.is_none() {
        assert_eq!(
            body["data"]["result"][0]["metric"]["fallback"], "true",
            "{body}"
        );
    }
    // Keep one series fresh while all members last seen at `end` hit the exact
    // left lookback boundary. Native Prometheus 3.5 must agree for every readout.
    let updates: Vec<_> = (60_000..=300_000)
        .step_by(60_000)
        .map(|offset| (end + offset, 9.0))
        .collect();
    let wire = WriteRequest {
        timeseries: vec![series_with_labels(
            "a",
            &[("pod", "y"), ("job", "api")],
            &updates,
        )],
    };
    if let Some(url) = &native {
        assert_eq!(remote_write(&client, url, &wire).await, 204);
    }
    assert_eq!(remote_write(&client, &base, &wire).await, 204);
    for text in &queries {
        let body = query(&client, &base, text, end + 300_000).await;
        assert!(is_warm(&body), "{text}: {body}");
        assert_eq!(
            body["data"]["result"].as_array().unwrap().len(),
            1,
            "{text}: {body}"
        );
        compare_native(&client, &native, text, end + 300_000, &body).await;
    }
    task.abort();
}
