//! Real VictoriaMetrics differential for the published MetricsQL DAG path.
//!
//! Run with a VictoriaMetrics instance, for example:
//! `VICTORIAMETRICS_URL=http://127.0.0.1:18428 cargo test -p data_plane --test victoriametrics_accelerated_e2e -- --ignored --nocapture`.

use data_plane::drivers::ingest::prometheus_remote_write::{
    Label, Sample, TimeSeries, WriteRequest,
};
use prost::Message;
use serde_json::Value;
use std::{
    io::Write,
    net::TcpListener,
    process::{Child, Command, Stdio},
    time::Duration,
};

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
fn value(body: &Value) -> f64 {
    body["data"]["result"][0]["value"][1]
        .as_str()
        .unwrap()
        .parse()
        .unwrap()
}
async fn write(client: &reqwest::Client, base: &str, metric: &str, samples: &[(i64, f64)]) {
    let request = WriteRequest {
        timeseries: vec![TimeSeries {
            labels: vec![Label {
                name: "__name__".into(),
                value: metric.into(),
            }],
            samples: samples
                .iter()
                .map(|(timestamp, value)| Sample {
                    timestamp: *timestamp,
                    value: *value,
                })
                .collect(),
            exemplars: vec![],
            histograms: vec![],
        }],
    };
    let body = snap::raw::Encoder::new()
        .compress_vec(&request.encode_to_vec())
        .unwrap();
    client
        .post(format!("{base}/api/v1/write"))
        .header("content-encoding", "snappy")
        .header("content-type", "application/x-protobuf")
        .header("x-prometheus-remote-write-version", "0.1.0")
        .body(body)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
}

#[tokio::test]
#[ignore = "requires a real VictoriaMetrics via VICTORIAMETRICS_URL"]
async fn real_victoriametrics_ingest_published_dag_is_differential() {
    let metric = format!("vm_accelerated_gauge_{}", std::process::id());
    let query = format!("sum(sum_over_time({metric}[5s]))");
    let unsupported_query = format!("sum(increase({metric}[5s]))");
    let upstream = std::env::var("VICTORIAMETRICS_URL").expect("VICTORIAMETRICS_URL");
    let client = reqwest::Client::new();
    let now_ms = chrono::Utc::now().timestamp_millis();
    let base = now_ms / 20_000 * 20_000 - 20_000;
    let samples = [
        (base + 500, 1.),
        (base + 1700, 2.),
        (base + 2900, 3.),
        (base + 4200, 4.),
        (base + 5400, 5.),
        (base + 6600, 6.),
        (base + 8100, 7.),
        (base + 9400, 8.),
        (base + 10500, 9.),
    ];
    let at = (base + 10_000) as f64 / 1000.;
    let import = samples
        .iter()
        .map(|(ts, v)| format!("{metric} {v} {}\n", *ts as f64 / 1000.0))
        .collect::<String>();
    client
        .post(format!("{upstream}/api/v1/import/prometheus"))
        .body(import)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    client
        .post(format!("{upstream}/internal/force_flush"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let mut direct_unsupported = None;
    for _ in 0..200 {
        let response = client
            .get(format!("{upstream}/api/v1/query"))
            .query(&[
                ("query", unsupported_query.as_str()),
                ("time", &at.to_string()),
            ])
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap();
        if response["data"]["result"]
            .as_array()
            .is_some_and(|r| !r.is_empty())
            && (value(&response) - 4.0).abs() < 1e-9
        {
            direct_unsupported = Some(response);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let direct_unsupported = direct_unsupported.expect("VictoriaMetrics import became visible");
    let mut fixture: Value = serde_json::from_str(include_str!(
        "../../docs/examples/asapquery-compatibility-demo-snapshot.json"
    ))
    .unwrap();
    let mut demand = fixture["query_workload"]["repeating_queries"][0].clone();
    demand["query"] = query.clone().into();
    demand["time_selection"]["lookback"] = 5_000.into();
    fixture["query_workload"]["repeating_queries"] = serde_json::json!([demand]);
    fixture["implementation"]["topk_evidence"] = serde_json::json!({});
    let snapshot: control_plane::physical::compiler::BackendLocalPlanningSnapshot =
        serde_json::from_value(fixture).unwrap();
    let (mut request, environment) = snapshot.planning_request().unwrap();
    request.hybrid_execution = false;
    let accuracy = request.queries[0].accuracy.clone();
    let expr = asap_frontend_metricsql::lower_metricsql(&query, accuracy.clone()).unwrap();
    request.queries[0].query_string = query.clone();
    request.queries[0].post_asap = control_plane::planner_selection::select_summary(
        &expr,
        &control_plane::physical::post_asap::cost_model::ForcedFamilyCostModel::new(
            accuracy,
            planner_types::post_asap::SketchAlgorithm::Kll,
        ),
    )
    .unwrap();
    let plan = control_plane::physical::compiler::PhysicalCompiler
        .compile_metricsql(request, environment)
        .unwrap();
    let metricsql_plan = plan.metricsql_plan.clone().expect("MetricsQL sidecar");
    let binding = metricsql_plan
        .entries
        .values()
        .next()
        .and_then(|entry| {
            entry
                .executable
                .materialization_bindings()
                .into_iter()
                .next()
        })
        .expect("published MetricsQL DAG has a bound SummaryScan");
    assert_eq!(binding.readout_lookback_ms, Some(5_000));
    let artifact = data_plane::drivers::query::servers::http::PhysicalPlanInstallRequest {
        summary_catalog: plan.summary_catalog,
        collector_plans: plan.collector_plans,
        precompute_plan: plan.precompute_plan,
        transmission_plan: plan.transmission_plan,
        query_plan: plan.query_plan,
        metricsql_plan: plan.metricsql_plan,
        clickhouse_sql: None,
        storage_routing: None,
        adaptation_evidence: vec![],
    };
    let artifact: data_plane::drivers::query::servers::http::PhysicalPlanInstallRequest =
        serde_json::from_value(serde_json::to_value(&artifact).unwrap()).unwrap();
    assert_eq!(
        artifact
            .metricsql_plan
            .as_ref()
            .unwrap()
            .entries
            .values()
            .next()
            .and_then(|entry| entry
                .executable
                .materialization_bindings()
                .into_iter()
                .next())
            .and_then(|binding| binding.readout_lookback_ms),
        Some(5_000),
        "MetricsQL effective lookback survives publication serialization"
    );
    let mut file = tempfile::NamedTempFile::new().unwrap();
    serde_json::to_writer(&mut file, &artifact).unwrap();
    file.flush().unwrap();
    let query_port = port();
    let vm_port = port();
    let output = tempfile::tempdir().unwrap();
    let mut child = ChildGuard(
        Command::new(env!("CARGO_BIN_EXE_data_plane"))
            .args([
                "--profile",
                "asapquery",
                "--physical-plan",
                file.path().to_str().unwrap(),
                "--http-port",
                &query_port.to_string(),
                "--victoriametrics-http-port",
                &vm_port.to_string(),
                "--victoriametrics-url",
                &upstream,
                "--forward-unsupported-queries",
                "--prometheus-server",
                &upstream,
                "--output-dir",
                output.path().to_str().unwrap(),
                "--precompute-allowed-lateness-ms",
                "0",
                "--precompute-flush-interval-ms",
                "100",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let ingest = format!("http://127.0.0.1:{query_port}");
    let proxy = format!("http://127.0.0.1:{vm_port}");
    let mut ready = false;
    for _ in 0..100 {
        if let Some(status) = child.0.try_wait().unwrap() {
            panic!("backend exited: {status}");
        }
        if client
            .get(format!("{proxy}/api/v1/health"))
            .send()
            .await
            .is_ok_and(|r| r.status().is_success())
        {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(ready, "VictoriaMetrics listener did not become ready");
    write(&client, &ingest, &metric, &samples[..8]).await;
    write(&client, &ingest, &metric, &samples[8..]).await;
    let mut fallback_pair = None;
    for _ in 0..200 {
        let fallback_response = client
            .get(format!("{proxy}/api/v1/query"))
            .query(&[
                ("query", unsupported_query.as_str()),
                ("time", &at.to_string()),
            ])
            .send()
            .await
            .unwrap();
        assert_eq!(
            fallback_response
                .headers()
                .get("x-asap-execution")
                .and_then(|value| value.to_str().ok()),
            Some("exact_fallback")
        );
        let fallback = fallback_response.json::<Value>().await.unwrap();
        if fallback["data"]["result"]
            .as_array()
            .is_some_and(|r| !r.is_empty())
            && (value(&fallback) - 4.0).abs() < 1e-9
        {
            fallback_pair = Some(fallback);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let fallback = fallback_pair.expect("fallback query became visible");
    assert_eq!(value(&fallback), value(&direct_unsupported));
    let mut accelerated = None;
    for _ in 0..60 {
        let response = client
            .get(format!("{proxy}/api/v1/query"))
            .query(&[("query", query.as_str()), ("time", &at.to_string())])
            .send()
            .await
            .unwrap();
        if response
            .headers()
            .get("x-asap-execution")
            .and_then(|v| v.to_str().ok())
            == Some("warm")
        {
            accelerated = Some(response.json::<Value>().await.unwrap());
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let accelerated = accelerated.expect("published MetricsQL DAG reached shared warm executor");
    let mut direct = None;
    for _ in 0..200 {
        let response: Value = client
            .get(format!("{upstream}/api/v1/query"))
            .query(&[("query", query.as_str()), ("time", &at.to_string())])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if response["data"]["result"]
            .as_array()
            .is_some_and(|rows| !rows.is_empty())
            && (value(&response) - 26.0).abs() < 1e-9
            && (value(&response) - value(&accelerated)).abs() < 1e-9
        {
            direct = Some(response);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let direct =
        direct.expect("VictoriaMetrics import became query-visible with differential result");
    assert!(
        (value(&accelerated) - value(&direct)).abs() < 1e-9,
        "accelerated={accelerated}, direct={direct}"
    );
    drop(child);
}
