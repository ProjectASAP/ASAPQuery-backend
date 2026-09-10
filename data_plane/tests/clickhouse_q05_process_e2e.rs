//! Optional OS-process E2E for a q05-class bounded SQL max query.
//! Run with `CLICKHOUSE_URL=http://127.0.0.1:8123 cargo test -p data_plane --test clickhouse_q05_process_e2e`.

use asap_types::{AggregationType, KeyByLabelNames, PrecomputeMaterialization, WindowKind};
use control_plane::physical::compiler::{PlanEnvelope, PrecomputePlan, TransmissionPlan};
use planner_types::pre_asap::{Column, DataType, Schema};
use std::io::Read;
use std::{collections::HashMap, process::Stdio, time::Duration};

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn clickhouse_auth(request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    match std::env::var("CLICKHOUSE_USER") {
        Ok(user) => request.basic_auth(user, std::env::var("CLICKHOUSE_PASSWORD").ok()),
        Err(_) => request,
    }
}

struct Child(std::process::Child);
impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn q05_sql_is_planned_backfilled_and_served_warm_by_backend_process() {
    let Ok(clickhouse) = std::env::var("CLICKHOUSE_URL") else {
        eprintln!("skipping q05 process E2E because CLICKHOUSE_URL is unset");
        return;
    };
    let client = reqwest::Client::new();
    let end_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let start_ms = end_ms - 43_200_000;
    let setup = [
        "CREATE DATABASE IF NOT EXISTS asap_q05_e2e".to_string(),
        "DROP TABLE IF EXISTS asap_q05_e2e.raw_samples".to_string(),
        "CREATE TABLE asap_q05_e2e.raw_samples(metric String, labels String, ts_ms Int64, value Float64) ENGINE=Memory".to_string(),
        format!("INSERT INTO asap_q05_e2e.raw_samples VALUES ('cache_refresh_lag_seconds','cache_refresh_lag_seconds',{},7),('cache_refresh_lag_seconds','cache_refresh_lag_seconds',{},11),('other','other',{},99)", start_ms + 1_000, start_ms + 2_000, start_ms + 2_000),
    ];
    for sql in setup {
        let response = clickhouse_auth(client.post(&clickhouse))
            .body(sql)
            .send()
            .await
            .unwrap();
        assert!(
            response.status().is_success(),
            "{}",
            response.text().await.unwrap()
        );
    }

    let sql = format!("SELECT max(value) AS value FROM raw_samples WHERE metric='cache_refresh_lag_seconds' AND ts_ms>={start_ms} AND ts_ms<{end_ms}");
    let mut config = PrecomputeMaterialization::new(
        AggregationType::MinMax,
        "max".into(),
        Default::default(),
        KeyByLabelNames::empty(),
        KeyByLabelNames::empty(),
        KeyByLabelNames::empty(),
        String::new(),
        43_200,
        43_200,
        WindowKind::Tumbling,
        String::new(),
        "cache_refresh_lag_seconds".into(),
        None,
        Some("raw_samples".into()),
        Some("value".into()),
    );
    config.pane_origin_ms = Some(start_ms as i64);
    let sds = control_plane::physical::summary_catalog::SummaryCatalog::from_materializations(
        73,
        1,
        &[config.clone()],
    )
    .unwrap();
    let envelope = PlanEnvelope {
        plan_id: 73,
        plan_version: 1,
        generated_at_unix_ms: 0,
        activation_unix_ms: 0,
        expiry_unix_ms: None,
        backend_compat: "test".into(),
        planner_revision: "test".into(),
        capability_snapshot_id: "test".into(),
    };
    let mut precompute =
        PrecomputePlan::build_backend_local(envelope.clone(), vec![config.clone()]).unwrap();
    precompute.summary_catalog = Some(sds.reference().unwrap());
    let mut transmission =
        TransmissionPlan::build(envelope, &precompute, &Default::default()).unwrap();
    transmission.summary_catalog = precompute.summary_catalog.clone();
    let schema = Schema::with_time_index(
        vec![
            Column::new("metric", DataType::Utf8, false),
            Column::new("labels", DataType::Utf8, false),
            Column::new("ts_ms", DataType::Timestamp, false),
            Column::new("value", DataType::Float64, false),
        ],
        2,
        vec![],
    );
    let compiled = control_plane::clickhouse::compile_clickhouse_workload(
        &control_plane::clickhouse::ClickHouseSqlWorkload {
            sds: sds.clone(),
            precompute_plan: precompute.clone(),
            transmission_plan: transmission.clone(),
            tables: HashMap::from([("raw_samples".into(), schema)]),
            accuracy: planner_types::types::AccuracyTarget::Exact,
            queries: vec![control_plane::clickhouse::ClickHouseSqlWorkloadEntry {
                sql: sql.clone(),
                start_ms,
                end_ms,
                cumulative: true,
            }],
        },
    )
    .await
    .unwrap();

    let http_port = free_port();
    let mut sql_port = free_port();
    while sql_port == http_port {
        sql_port = free_port();
    }
    let streaming_config = format!(
        "{}/examples/promql/streaming_config.yaml",
        env!("CARGO_MANIFEST_DIR")
    );
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_data_plane"));
    command.args([
        "--http-port",
        &http_port.to_string(),
        "--streaming-config",
        &streaming_config,
        "--clickhouse-http-port",
        &sql_port.to_string(),
        "--clickhouse-url",
        &clickhouse,
        "--clickhouse-database",
        "asap_q05_e2e",
        "--clickhouse-backfill-table",
        "raw_samples",
        "--clickhouse-backfill-database",
        "asap_q05_e2e",
        "--clickhouse-backfill-timestamp-column",
        "ts_ms",
        "--enable-backfill-worker",
        "--output-dir",
        "/tmp/asap-q05-process-e2e",
    ]);
    if let Ok(user) = std::env::var("CLICKHOUSE_USER") {
        command.args(["--clickhouse-user", &user]);
    }
    if let Ok(password) = std::env::var("CLICKHOUSE_PASSWORD") {
        command.args(["--clickhouse-password", &password]);
    }
    let mut child = Child(
        command
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let base = format!("http://127.0.0.1:{http_port}");
    for _ in 0..100 {
        if client.get(format!("{base}/health")).send().await.is_ok() {
            break;
        }
        if let Some(status) = child.0.try_wait().unwrap() {
            let mut stderr = String::new();
            child
                .0
                .stderr
                .as_mut()
                .unwrap()
                .read_to_string(&mut stderr)
                .unwrap();
            panic!("backend exited before ready ({status}): {stderr}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let install = data_plane::drivers::query::servers::http::PhysicalPlanInstallRequest {
        summary_catalog: sds,
        collector_plans: vec![],
        precompute_plan: precompute,
        transmission_plan: transmission,
        query_plan: control_plane::query_plan::QueryPlan {
            plan_id: 73,
            plan_version: 1,
            entries: Default::default(),
        },
        metricsql_plan: None,
        clickhouse_sql: Some(serde_json::to_value(compiled).unwrap()),
        storage_routing: None,
        adaptation_evidence: vec![],
    };
    let response = client
        .post(format!("{base}/api/v1/physical-plan"))
        .json(&install)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "{}",
        response.text().await.unwrap()
    );
    let response = client
        .post(format!("{base}/api/v1/physical-plan/activate"))
        .json(&serde_json::json!({"plan_id":73,"plan_version":1}))
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "{}",
        response.text().await.unwrap()
    );
    let agg_id = config.policy_fp_u64();
    let response: serde_json::Value = client
        .post(format!("{base}/api/v1/db/backfill"))
        .json(
            &serde_json::json!({"agg_id":agg_id,"start_ms":start_ms,"end_ms":end_ms,
            "source":{"Prometheus":{"url":"clickhouse://configured"}},"windows_total":1}),
        )
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let job = response["job_id"]
        .as_u64()
        .unwrap_or_else(|| panic!("backfill was not accepted: {response}"));
    for _ in 0..100 {
        let status: serde_json::Value = client
            .get(format!("{base}/api/v1/db/backfill/jobs/{job}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        match status["status"].as_str() {
            Some("complete") => break,
            Some("failed") => panic!("backfill failed: {status}"),
            _ => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    let warm = client
        .post(format!("http://127.0.0.1:{sql_port}/"))
        .body(sql)
        .send()
        .await
        .unwrap();
    assert_eq!(
        warm.headers()["x-asap-execution"],
        "warm",
        "fallback headers: {:?}",
        warm.headers()
    );
    let warm_value: f64 = warm.text().await.unwrap().trim().parse().unwrap();
    let exact_value: f64 = clickhouse_auth(client.post(&clickhouse))
        .body(format!("SELECT max(value) FROM asap_q05_e2e.raw_samples WHERE metric='cache_refresh_lag_seconds' AND ts_ms>={start_ms} AND ts_ms<{end_ms} FORMAT TabSeparated"))
        .send().await.unwrap().text().await.unwrap().trim().parse().unwrap();
    assert_eq!(warm_value, exact_value);
}
