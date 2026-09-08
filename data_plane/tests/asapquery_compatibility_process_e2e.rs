//! Black-box acceptance test for the collector-free ASAPQuery profile.
//!
//! Starts the production binary from a canonical workload snapshot, ingests
//! only Prometheus Remote Write v1, exercises every declared warm query family
//! through instant and range APIs, and verifies exact fallback request parity.

use std::collections::HashMap;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use axum::{extract::Query, http::HeaderMap, routing::get, Json, Router};
use data_plane::drivers::ingest::prometheus_remote_write::{
    Label, Sample, TimeSeries, WriteRequest,
};
use prost::Message;
use serde_json::Value;
use tokio::sync::Mutex;

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn unused_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve loopback port");
    listener.local_addr().expect("read loopback address").port()
}

async fn wait_until_ready(client: &reqwest::Client, url: &str, child: &mut Child) {
    for _ in 0..120 {
        if let Some(status) = child.try_wait().expect("inspect backend process") {
            panic!("backend exited before readiness: {status}");
        }
        if client
            .get(url)
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("backend did not become ready at {url}");
}

fn series(metric: &str, samples: &[(i64, f64)]) -> TimeSeries {
    series_with_labels(metric, &[], samples)
}

fn series_with_labels(metric: &str, labels: &[(&str, &str)], samples: &[(i64, f64)]) -> TimeSeries {
    let mut wire_labels = vec![Label {
        name: "__name__".into(),
        value: metric.into(),
    }];
    wire_labels.extend(labels.iter().map(|(name, value)| Label {
        name: (*name).into(),
        value: (*value).into(),
    }));
    TimeSeries {
        labels: wire_labels,
        samples: samples
            .iter()
            .map(|(timestamp, value)| Sample {
                value: *value,
                timestamp: *timestamp,
            })
            .collect(),
        exemplars: Vec::new(),
        histograms: Vec::new(),
    }
}

async fn remote_write(client: &reqwest::Client, base: &str, request: &WriteRequest) -> u16 {
    let body = snap::raw::Encoder::new()
        .compress_vec(&request.encode_to_vec())
        .expect("snappy encode");
    client
        .post(format!("{base}/api/v1/write"))
        .header("content-encoding", "snappy")
        .header("content-type", "application/x-protobuf")
        .header("x-prometheus-remote-write-version", "0.1.0")
        .body(body)
        .send()
        .await
        .expect("send Remote Write")
        .status()
        .as_u16()
}

fn first_value(response: &Value, field: &str) -> Option<f64> {
    let samples = response["data"]["result"]
        .as_array()?
        .first()?
        .get(field)?
        .as_array()?;
    let value = if field == "value" {
        samples.get(1)?
    } else {
        samples.last()?.as_array()?.get(1)?
    };
    value.as_str()?.parse().ok()
}

fn is_warm(response: &Value) -> bool {
    response["infos"].as_array().is_some_and(|infos| {
        infos.iter().any(|info| {
            info.as_str()
                .is_some_and(|line| line == "data_source: asap_query")
        })
    })
}

// Three registered consumers must observe one raw SUM/count producer, including
// uneven instance sample counts and Remote Write retries.
#[tokio::test]
async fn shared_exact_dashboard_executes_selected_workload() {
    let fallback_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let fallback_address = fallback_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(fallback_listener, Router::new()
            .route("/-/healthy", get(|| async { "healthy" }))
            .route("/api/v1/query", get(|| async { Json(serde_json::json!({"status":"success","data":{"resultType":"vector","result":[]}})) })))
            .await.unwrap();
    });
    let output_dir = tempfile::tempdir().unwrap();
    let mut snapshot: Value = serde_json::from_str(include_str!(
        "../../docs/examples/asapquery-compatibility-demo-snapshot.json"
    ))
    .unwrap();
    let sum = "sum by (service) (sum_over_time(asap_demo_gauge[5s]))";
    let count = "sum by (service) (count_over_time(asap_demo_gauge[5s]))";
    let mean = format!("{sum} / {count}");
    let mut entry = snapshot["query_workload"]["repeating_queries"][2].clone();
    snapshot["query_workload"]["repeating_queries"] = Value::Array(
        [sum, count, mean.as_str()]
            .into_iter()
            .map(|query| {
                entry["query"] = query.into();
                entry.clone()
            })
            .collect(),
    );
    let mut typed: control_plane::physical::compiler::BackendLocalPlanningSnapshot =
        serde_json::from_value(snapshot.clone()).unwrap();
    let (request, environment) = typed.clone().planning_request().unwrap();
    let candidates =
        control_plane::physical::workload_cost::with_exact_alternative(request).unwrap();
    let quotes = candidates
        .into_iter()
        .enumerate()
        .map(|(index, candidate)| {
            let plan = control_plane::physical::compiler::PhysicalCompiler
                .compile(candidate.clone(), environment.clone())
                .unwrap();
            let manifest =
                control_plane::physical::workload_cost::manifest(&plan, &candidate.queries)
                    .unwrap();
            let unit_costs = manifest
                .components
                .keys()
                .map(|key| (key.clone(), if index == 0 { 1.0 } else { 1000.0 }))
                .collect();
            control_plane::physical::workload_cost::WorkloadQuote {
                manifest,
                executable: true,
                unit_costs,
            }
        })
        .collect();
    typed.snapshot_version = 2;
    typed.workload_cost_evidence = Some(
        control_plane::physical::workload_cost::WorkloadCostEvidence {
            data_snapshot_id: "process-fixture-v1".into(),
            model_version: "test-only-unit-costs".into(),
            observed_at_unix_ms: environment.observed_at_unix_ms,
            valid_for_ms: environment.max_evidence_age_ms,
            quotes,
        },
    );
    snapshot = serde_json::to_value(&typed).unwrap();
    let plan = typed.compile().unwrap();
    assert!(plan.cost_comparison.is_some());
    assert_eq!(plan.precompute_plan.materializations.len(), 1);
    assert_eq!(plan.query_plan.entries.len(), 3);
    let snapshot_path = output_dir.path().join("snapshot.json");
    std::fs::write(&snapshot_path, serde_json::to_vec(&snapshot).unwrap()).unwrap();
    let port = unused_port();
    let child = Command::new(env!("CARGO_BIN_EXE_data_plane"))
        .args(["--profile", "asapquery", "--planning-snapshot"])
        .arg(&snapshot_path)
        .args([
            "--forward-unsupported-queries",
            "--prometheus-server",
            &format!("http://{fallback_address}"),
        ])
        .args(["--http-port", &port.to_string(), "--output-dir"])
        .arg(output_dir.path())
        .args([
            "--precompute-allowed-lateness-ms",
            "0",
            "--precompute-flush-interval-ms",
            "25",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let mut child = ChildGuard(child);
    let client = reqwest::Client::new();
    let backend = format!("http://127.0.0.1:{port}");
    wait_until_ready(&client, &format!("{backend}/api/v1/health"), &mut child.0).await;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let base = now - now.rem_euclid(5000) - 20000;
    let labeled = |service: &str, instance: &str, samples: &[(i64, f64)]| {
        let mut item = series("asap_demo_gauge", samples);
        item.labels.extend([
            Label {
                name: "service".into(),
                value: service.into(),
            },
            Label {
                name: "instance".into(),
                value: instance.into(),
            },
        ]);
        item
    };
    let request = WriteRequest {
        timeseries: vec![
            labeled("api", "a", &[(base + 500, 10.0)]),
            labeled(
                "api",
                "b",
                &[(base + 600, 2.0), (base + 1700, 4.0), (base + 2900, 8.0)],
            ),
            labeled("worker", "c", &[(base + 700, 9.0), (base + 1900, 15.0)]),
        ],
    };
    assert_eq!(remote_write(&client, &backend, &request).await, 204);
    assert_eq!(remote_write(&client, &backend, &request).await, 204);
    let advance = WriteRequest {
        timeseries: vec![
            labeled("api", "a", &[(base + 5500, 100.0)]),
            labeled("api", "b", &[(base + 5500, 100.0)]),
            labeled("worker", "c", &[(base + 5500, 100.0)]),
        ],
    };
    assert_eq!(remote_write(&client, &backend, &advance).await, 204);
    let final_advance = WriteRequest {
        timeseries: vec![
            labeled("api", "a", &[(base + 10500, 1000.0)]),
            labeled("api", "b", &[(base + 10500, 1000.0)]),
            labeled("worker", "c", &[(base + 10500, 1000.0)]),
        ],
    };
    assert_eq!(remote_write(&client, &backend, &final_advance).await, 204);
    for (query, expected) in [
        (sum, [24.0, 24.0]),
        (count, [4.0, 2.0]),
        (mean.as_str(), [6.0, 12.0]),
    ] {
        let result = wait_for_warm_instant(
            &client,
            &backend,
            query,
            (base + 5000) as f64 / 1000.0,
            &output_dir.path().join("query_engine.log"),
        )
        .await;
        let rows = result["data"]["result"].as_array().unwrap();
        assert_eq!(rows.len(), 2, "{query}: {result}");
        for row in rows {
            let service = row["metric"]["service"].as_str().unwrap();
            assert_eq!(row["metric"].as_object().unwrap().len(), 1);
            let value: f64 = row["value"][1].as_str().unwrap().parse().unwrap();
            assert_eq!(
                value,
                expected[usize::from(service == "worker")],
                "{query}: {result}"
            );
            assert_eq!(
                row["value"][0].as_f64().unwrap(),
                (base + 5000) as f64 / 1000.0
            );
        }
    }
    let result: Value = client
        .get(format!("{backend}/api/v1/query_range"))
        .query(&[
            ("query", mean.clone()),
            ("start", ((base + 5000) as f64 / 1000.0).to_string()),
            ("end", ((base + 10000) as f64 / 1000.0).to_string()),
            ("step", "5".into()),
        ])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(is_warm(&result), "{result}");
    for row in result["data"]["result"].as_array().unwrap() {
        let values = row["values"].as_array().unwrap();
        assert_eq!(values.len(), 2, "{result}");
        assert_eq!(values[1][1], "100", "{result}");
    }
    // Unaligned intervals cannot be answered by whole tumbling states.
    let result: Value = client
        .get(format!("{backend}/api/v1/query"))
        .query(&[
            ("query", mean),
            ("time", ((base + 5001) as f64 / 1000.0).to_string()),
        ])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        !is_warm(&result),
        "partial interval was incorrectly warm: {result}"
    );
}

async fn wait_for_warm_instant(
    client: &reqwest::Client,
    base: &str,
    query: &str,
    evaluation_seconds: f64,
    log_path: &std::path::Path,
) -> Value {
    let mut last = Value::Null;
    for _ in 0..80 {
        last = client
            .get(format!("{base}/api/v1/query"))
            .query(&[
                ("query", query.to_string()),
                ("time", evaluation_seconds.to_string()),
            ])
            .send()
            .await
            .expect("instant query")
            .json()
            .await
            .expect("instant JSON");
        if is_warm(&last) && first_value(&last, "value").is_some() {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let log = std::fs::read_to_string(log_path)
        .unwrap_or_else(|error| format!("log unavailable: {error}"));
    panic!("query never became warm: {query}: {last}\nbackend log:\n{log}");
}

#[tokio::test]
async fn collector_free_profile_serves_complete_matrix_and_falls_back_exactly() {
    let fallback_calls = Arc::new(Mutex::new(Vec::<(
        String,
        HashMap<String, String>,
        HeaderMap,
    )>::new()));
    let instant_calls = Arc::clone(&fallback_calls);
    let range_calls = Arc::clone(&fallback_calls);
    let fallback_app = Router::new()
        .route("/-/healthy", get(|| async { "Prometheus is Healthy." }))
        .route(
            "/api/v1/query",
            get(
                move |Query(params): Query<HashMap<String, String>>, headers: HeaderMap| {
                    let calls = Arc::clone(&instant_calls);
                    async move {
                        calls
                            .lock()
                            .await
                            .push(("instant".into(), params.clone(), headers));
                        let timestamp = params
                            .get("time")
                            .and_then(|value| value.parse::<f64>().ok())
                            .unwrap_or_default();
                        Json(serde_json::json!({
                            "status": "success",
                            "data": {"resultType": "vector", "result": [{
                                "metric": {"fallback": "true"},
                                "value": [timestamp, "42"]
                            }]}
                        }))
                    }
                },
            ),
        )
        .route(
            "/api/v1/query_range",
            get(
                move |Query(params): Query<HashMap<String, String>>, headers: HeaderMap| {
                    let calls = Arc::clone(&range_calls);
                    async move {
                        calls
                            .lock()
                            .await
                            .push(("range".into(), params.clone(), headers));
                        let start = params
                            .get("start")
                            .and_then(|value| value.parse::<f64>().ok())
                            .unwrap_or_default();
                        let end = params
                            .get("end")
                            .and_then(|value| value.parse::<f64>().ok())
                            .unwrap_or_default();
                        Json(serde_json::json!({
                            "status": "success",
                            "data": {"resultType": "matrix", "result": [{
                                "metric": {"fallback": "true"},
                                "values": [[start, "42"], [end, "43"]]
                            }]}
                        }))
                    }
                },
            ),
        );
    let fallback_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fallback");
    let fallback_address = fallback_listener.local_addr().expect("fallback address");
    tokio::spawn(async move {
        axum::serve(fallback_listener, fallback_app)
            .await
            .expect("serve fallback")
    });

    let backend_port = unused_port();
    let output_dir = tempfile::tempdir().expect("backend output directory");
    let snapshot = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../docs/examples/asapquery-compatibility-demo-snapshot.json"
    );
    let child = Command::new(env!("CARGO_BIN_EXE_data_plane"))
        .arg("--profile")
        .arg("asapquery")
        .arg("--planning-snapshot")
        .arg(snapshot)
        .arg("--prometheus-server")
        .arg(format!("http://{fallback_address}"))
        .arg("--forward-unsupported-queries")
        .arg("--http-port")
        .arg(backend_port.to_string())
        .arg("--output-dir")
        .arg(output_dir.path())
        .arg("--precompute-allowed-lateness-ms")
        .arg("0")
        .arg("--precompute-flush-interval-ms")
        .arg("25")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("start production backend");
    let mut child = ChildGuard(child);
    let client = reqwest::Client::new();
    let backend = format!("http://127.0.0.1:{backend_port}");
    wait_until_ready(&client, &format!("{backend}/api/v1/health"), &mut child.0).await;

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time")
        .as_millis() as i64;
    let base = now_ms - now_ms.rem_euclid(5_000) - 20_000;
    let request = WriteRequest {
        timeseries: vec![
            series(
                "asap_demo_counter_total",
                &[
                    (base + 500, 10.0),
                    (base + 1_700, 20.0),
                    (base + 2_900, 3.0),
                    (base + 4_200, 13.0),
                    (base + 5_400, 13.0),
                    (base + 6_600, 21.0),
                    (base + 8_100, 2.0),
                    (base + 9_400, 12.0),
                ],
            ),
            series_with_labels(
                "asap_demo_gauge",
                &[("job", "api")],
                &[
                    (base + 500, 1.0),
                    (base + 1_700, 2.0),
                    (base + 2_900, 3.0),
                    (base + 4_200, 4.0),
                    (base + 5_400, 5.0),
                    (base + 6_600, 6.0),
                    (base + 8_100, 7.0),
                    (base + 9_400, 8.0),
                ],
            ),
            series_with_labels(
                "asap_demo_gauge",
                &[("job", "worker")],
                &[(base + 700, 100.0), (base + 3_100, 100.0)],
            ),
            series_with_labels(
                "asap_demo_gauge",
                &[("job", "cron")],
                &[
                    (base + 900, 10.0),
                    (base + 2_100, 10.0),
                    (base + 3_700, 10.0),
                ],
            ),
            series(
                "asap_demo_latency_ms",
                &[
                    (base + 500, 10.0),
                    (base + 1_700, 20.0),
                    (base + 2_900, 30.0),
                    (base + 4_200, 40.0),
                    (base + 5_400, 15.0),
                    (base + 6_600, 25.0),
                    (base + 8_100, 35.0),
                    (base + 9_400, 45.0),
                ],
            ),
        ],
    };
    assert_eq!(remote_write(&client, &backend, &request).await, 204);
    let watermark_advance = WriteRequest {
        timeseries: vec![
            series("asap_demo_counter_total", &[(base + 10_500, 15.0)]),
            series_with_labels(
                "asap_demo_gauge",
                &[("job", "api")],
                &[(base + 10_500, 9.0)],
            ),
            series("asap_demo_latency_ms", &[(base + 10_500, 55.0)]),
        ],
    };
    assert_eq!(
        remote_write(&client, &backend, &watermark_advance).await,
        204
    );
    // A normal Prometheus retry must be accepted without changing sketches.
    assert_eq!(remote_write(&client, &backend, &request).await, 204);
    let corrupt = client
        .post(format!("{backend}/api/v1/write"))
        .header("content-encoding", "snappy")
        .header("content-type", "application/x-protobuf")
        .body(vec![1, 2, 3])
        .send()
        .await
        .expect("send corrupt request");
    assert_eq!(corrupt.status().as_u16(), 400);

    let first_eval = (base + 5_000) as f64 / 1_000.0;
    let second_eval = (base + 10_000) as f64 / 1_000.0;
    let backend_log = output_dir.path().join("query_engine.log");
    let rate = wait_for_warm_instant(
        &client,
        &backend,
        "rate(asap_demo_counter_total[5s])",
        first_eval,
        &backend_log,
    )
    .await;
    let increase = wait_for_warm_instant(
        &client,
        &backend,
        "increase(asap_demo_counter_total[5s])",
        first_eval,
        &backend_log,
    )
    .await;
    let sum = wait_for_warm_instant(
        &client,
        &backend,
        "sum_over_time(asap_demo_gauge[5s])",
        first_eval,
        &backend_log,
    )
    .await;
    let quantile = wait_for_warm_instant(
        &client,
        &backend,
        "quantile_over_time(0.5, asap_demo_latency_ms[5s])",
        first_eval,
        &backend_log,
    )
    .await;
    let topk_sum = wait_for_warm_instant(
        &client,
        &backend,
        "topk(1, sum_over_time(asap_demo_gauge[5s]))",
        first_eval,
        &backend_log,
    )
    .await;
    let topk_count = wait_for_warm_instant(
        &client,
        &backend,
        "topk(1, count_over_time(asap_demo_gauge[5s]))",
        first_eval,
        &backend_log,
    )
    .await;
    assert_eq!(
        first_value(&topk_sum, "value"),
        Some(200.0),
        "value-weighted Top-K must select worker: {topk_sum}"
    );
    assert_eq!(
        first_value(&topk_count, "value"),
        Some(4.0),
        "count-weighted Top-K must select api: {topk_count}"
    );
    assert_eq!(topk_sum["data"]["result"].as_array().map(Vec::len), Some(1));
    assert_eq!(
        topk_count["data"]["result"].as_array().map(Vec::len),
        Some(1)
    );
    assert_eq!(
        topk_sum["data"]["result"][0]["metric"]["item"],
        "asap_demo_gauge{job=\"worker\"}"
    );
    assert_eq!(
        topk_count["data"]["result"][0]["metric"]["item"],
        "asap_demo_gauge{job=\"api\"}"
    );
    let rate_value = first_value(&rate, "value").expect("rate value");
    let increase_value = first_value(&increase, "value").expect("increase value");
    assert!((rate_value * 5.0 - increase_value).abs() < 1e-9);
    assert!((first_value(&sum, "value").expect("sum value") - 240.0).abs() < 1e-9);
    let quantile_value = first_value(&quantile, "value").expect("quantile value");
    assert!(
        (19.0..=31.0).contains(&quantile_value),
        "unexpected p50: {quantile_value}; response={quantile}"
    );

    for query in [
        "rate(asap_demo_counter_total[5s])",
        "increase(asap_demo_counter_total[5s])",
        "sum_over_time(asap_demo_gauge[5s])",
        "quantile_over_time(0.5, asap_demo_latency_ms[5s])",
        "topk(1, sum_over_time(asap_demo_gauge[5s]))",
        "topk(1, count_over_time(asap_demo_gauge[5s]))",
    ] {
        let response: Value = client
            .get(format!("{backend}/api/v1/query_range"))
            .query(&[
                ("query", query.to_string()),
                ("start", first_eval.to_string()),
                ("end", second_eval.to_string()),
                ("step", "5".into()),
            ])
            .send()
            .await
            .expect("range query")
            .json()
            .await
            .expect("range JSON");
        assert_eq!(response["status"], "success", "{query}: {response}");
        assert!(
            is_warm(&response),
            "{query} did not use warm tier: {response}"
        );
        if query.starts_with("topk(") {
            let mut ranked_points = response["data"]["result"]
                .as_array()
                .unwrap_or_else(|| panic!("missing Top-K range result for {query}: {response}"))
                .iter()
                .flat_map(|series| {
                    series["values"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .map(|point| {
                            (
                                point[0].as_f64().expect("Top-K timestamp"),
                                point[1]
                                    .as_str()
                                    .expect("Top-K value")
                                    .parse::<f64>()
                                    .expect("numeric Top-K value"),
                            )
                        })
                })
                .collect::<Vec<_>>();
            ranked_points.sort_by(|left, right| left.0.total_cmp(&right.0));
            let expected = if query.contains("sum_over_time") {
                vec![(first_eval, 200.0), (second_eval, 26.0)]
            } else {
                vec![(first_eval, 4.0), (second_eval, 4.0)]
            };
            assert_eq!(
                ranked_points, expected,
                "wrong Top-K windows for {query}: {response}"
            );
            continue;
        }
        let values = response["data"]["result"][0]["values"]
            .as_array()
            .unwrap_or_else(|| panic!("missing range values for {query}: {response}"));
        assert_eq!(values.len(), 2, "wrong step count for {query}: {response}");
        assert_eq!(values[0][0], first_eval);
        assert_eq!(values[1][0], second_eval);
    }

    // Readiness polling may briefly reach the exact fallback before a newly
    // closed warm window is visible. Every planned query above was required
    // to converge to a warm answer; isolate the explicit fallback assertions.
    fallback_calls.lock().await.clear();

    let fallback_instant: Value = client
        .get(format!("{backend}/api/v1/query"))
        .header("authorization", "Bearer demo")
        .header("x-scope-orgid", "tenant-demo")
        .query(&[
            ("query", "max(asap_unplanned)"),
            ("time", &first_eval.to_string()),
            ("timeout", "7s"),
        ])
        .send()
        .await
        .expect("fallback instant")
        .json()
        .await
        .expect("fallback instant JSON");
    assert_eq!(
        fallback_instant["data"]["result"][0]["metric"]["fallback"],
        "true"
    );
    let fallback_range: Value = client
        .get(format!("{backend}/api/v1/query_range"))
        .header("authorization", "Bearer demo")
        .header("x-scope-orgid", "tenant-demo")
        .query(&[
            ("query", "max(asap_unplanned)"),
            ("start", &first_eval.to_string()),
            ("end", &second_eval.to_string()),
            ("step", "5"),
            ("timeout", "9s"),
        ])
        .send()
        .await
        .expect("fallback range")
        .json()
        .await
        .expect("fallback range JSON");
    assert_eq!(
        fallback_range["data"]["result"][0]["metric"]["fallback"],
        "true"
    );

    let calls = fallback_calls.lock().await;
    assert_eq!(
        calls.len(),
        2,
        "planned queries unexpectedly fell back: {calls:?}"
    );
    assert_eq!(calls[0].0, "instant");
    assert_eq!(calls[0].1["query"], "max(asap_unplanned)");
    assert_eq!(calls[0].1["time"], first_eval.to_string());
    assert_eq!(calls[0].1["timeout"], "7s");
    assert_eq!(calls[0].2["authorization"], "Bearer demo");
    assert_eq!(calls[0].2["x-scope-orgid"], "tenant-demo");
    assert_eq!(calls[1].0, "range");
    assert_eq!(calls[1].1["start"], first_eval.to_string());
    assert_eq!(calls[1].1["end"], second_eval.to_string());
    assert_eq!(calls[1].1["step"], "5");
    assert_eq!(calls[1].1["timeout"], "9s");
    drop(calls);

    let status: Value = client
        .get(format!("{backend}/api/v1/physical-plan/status"))
        .send()
        .await
        .expect("physical status")
        .json()
        .await
        .expect("physical status JSON");
    assert_eq!(status["status"], "success");
    let materializations = status["materializations"]
        .as_array()
        .expect("materialization statuses");
    assert_eq!(materializations.len(), 5);
    assert!(materializations
        .iter()
        .all(|entry| entry["phase"] == "serving"));

    let metrics = client
        .get(format!("{backend}/metrics"))
        .send()
        .await
        .expect("metrics request")
        .text()
        .await
        .expect("metrics body");
    assert!(metrics.contains("asap_remote_write_requests_total 4"));
    assert!(metrics.contains("asap_remote_write_samples_total 32"));
    assert!(metrics.contains("asap_remote_write_duplicates_total 29"));
    assert!(metrics.contains("asap_remote_write_rejected_requests_total 1"));
}
