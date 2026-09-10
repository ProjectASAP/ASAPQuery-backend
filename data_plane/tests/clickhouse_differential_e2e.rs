//! Optional real-ClickHouse protocol and Grafana smoke coverage.
//!
//! Set `CLICKHOUSE_URL` (for example `http://127.0.0.1:8123`) to run it.

use std::{
    collections::{BTreeMap, HashMap},
    io::Write,
    net::TcpListener,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};

use data_plane::query_engines::asap_clickhouse_query_engine::{
    ClickHouseHttpFallback, ClickHouseHttpServer,
};

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn unused_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn wait_http(client: &reqwest::Client, url: &str, child: &mut Child) {
    for _ in 0..200 {
        assert!(
            child.try_wait().unwrap().is_none(),
            "data plane exited early"
        );
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
    panic!("data plane did not become ready at {url}");
}

fn mixed_workload(
    sql: &str,
) -> (
    control_plane::clickhouse::ClickHouseSqlWorkload,
    asap_types::PrecomputeMaterialization,
) {
    use asap_types::{AggregationType, KeyByLabelNames, PrecomputeMaterialization, WindowKind};
    use control_plane::physical::compiler::{
        PlanEnvelope, PrecomputePlan, TransmissionPlan, BACKEND_COMPAT, PLANNER_REVISION,
    };
    use planner_types::pre_asap::{Column, DataType, Schema};

    let mut config = PrecomputeMaterialization::new(
        AggregationType::Sum,
        String::new(),
        HashMap::from([("variant".into(), serde_json::json!(1))]),
        KeyByLabelNames::empty(),
        KeyByLabelNames::empty(),
        KeyByLabelNames::empty(),
        String::new(),
        2,
        2,
        WindowKind::Tumbling,
        String::new(),
        "telemetry.value".into(),
        None,
        Some("telemetry".into()),
        Some("value".into()),
    );
    config.pane_origin_ms = Some(0);
    let sds = asap_types::summary_catalog::SummaryCatalog::from_materializations(
        72,
        1,
        &[config.clone()],
    )
    .unwrap();
    let envelope = PlanEnvelope {
        plan_id: 72,
        plan_version: 1,
        generated_at_unix_ms: 0,
        activation_unix_ms: 0,
        expiry_unix_ms: None,
        backend_compat: BACKEND_COMPAT.into(),
        planner_revision: PLANNER_REVISION.into(),
        capability_snapshot_id: "clickhouse-mixed-process-e2e".into(),
    };
    let mut precompute =
        PrecomputePlan::build_backend_local(envelope.clone(), vec![config.clone()]).unwrap();
    precompute.summary_catalog = Some(sds.reference().unwrap());
    let mut transmission =
        TransmissionPlan::build(envelope, &precompute, &BTreeMap::new()).unwrap();
    transmission.summary_catalog = Some(sds.reference().unwrap());
    let schema = |time: &str, value: &str| {
        Schema::with_time_index(
            vec![
                Column::new(time, DataType::Timestamp, false),
                Column::new(value, DataType::Float64, false),
            ],
            0,
            vec![],
        )
    };
    (
        control_plane::clickhouse::ClickHouseSqlWorkload {
            sds,
            precompute_plan: precompute,
            transmission_plan: transmission,
            tables: HashMap::from([
                ("telemetry".into(), schema("timestamp_ms", "value")),
                (
                    "divisors".into(),
                    Schema::with_time_index(
                        vec![
                            Column::new("timestamp", DataType::Int64, false),
                            Column::new("divisor", DataType::Float64, false),
                        ],
                        0,
                        vec![],
                    ),
                ),
            ]),
            accuracy: planner_types::types::AccuracyTarget::Exact,
            queries: vec![control_plane::clickhouse::ClickHouseSqlWorkloadEntry {
                sql: sql.into(),
                start_ms: 0,
                end_ms: 2_000,
                cumulative: true,
            }],
        },
        config,
    )
}

#[tokio::test]
async fn exact_proxy_matches_clickhouse_for_sql_and_grafana_smoke_queries() {
    let Ok(clickhouse_url) = std::env::var("CLICKHOUSE_URL") else {
        eprintln!("skipping real ClickHouse E2E because CLICKHOUSE_URL is unset");
        return;
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let fallback = Arc::new(ClickHouseHttpFallback::new(
        clickhouse_url.clone(),
        "default".into(),
    ));
    let app = ClickHouseHttpServer::router(fallback);
    let server = tokio::spawn(async move { axum::serve(listener, app).await });

    let client = reqwest::Client::new();
    let proxy_url = format!("http://{address}/");
    let queries = [
        "SELECT number, number * 2 AS doubled FROM numbers(10) ORDER BY number FORMAT JSONEachRow",
        "SELECT version() FORMAT TabSeparated",
        "SELECT name FROM system.databases ORDER BY name FORMAT JSONEachRow",
        "SELECT database, name FROM system.tables ORDER BY database, name FORMAT JSONEachRow",
    ];

    for sql in queries {
        let exact = client
            .get(&clickhouse_url)
            .query(&[("query", sql)])
            .send()
            .await
            .unwrap();
        let proxied = client
            .get(&proxy_url)
            .query(&[("query", sql)])
            .send()
            .await
            .unwrap();
        assert_eq!(proxied.status(), exact.status(), "status for {sql}");
        assert_eq!(
            proxied.bytes().await.unwrap(),
            exact.bytes().await.unwrap(),
            "body for {sql}"
        );
    }

    server.abort();
}

#[tokio::test]
async fn compiled_publication_executes_mixed_dag_in_data_plane_process() {
    let Ok(clickhouse_url) = std::env::var("CLICKHOUSE_URL") else {
        eprintln!("skipping mixed process E2E because CLICKHOUSE_URL is unset");
        return;
    };
    let user = std::env::var("CLICKHOUSE_USER").ok();
    let password = std::env::var("CLICKHOUSE_PASSWORD").ok();
    let client = reqwest::Client::new();
    let sql = "SELECT sums.timestamp, sums.total / divisors.divisor AS ratio FROM (SELECT 2000 AS timestamp, sum(value) AS total FROM telemetry WHERE timestamp_ms >= 0 AND timestamp_ms < 2000) AS sums INNER JOIN divisors ON sums.timestamp = divisors.timestamp";
    for statement in [
        "DROP TABLE IF EXISTS default.telemetry",
        "DROP TABLE IF EXISTS default.divisors",
        "CREATE TABLE default.telemetry(metric String, labels String, timestamp_ms Int64, value Float64) ENGINE=Memory",
        "CREATE TABLE default.divisors(timestamp Int64, divisor Float64) ENGINE=Memory",
        "INSERT INTO default.telemetry VALUES ('telemetry.value','telemetry.value',100,2),('telemetry.value','telemetry.value',1100,3)",
        "INSERT INTO default.divisors VALUES (2000,10)",
    ] {
        let mut request = client.post(&clickhouse_url).body(statement);
        if let Some(user) = &user {
            request = request.basic_auth(user, password.as_ref());
        }
        let response = request.send().await.unwrap();
        assert!(response.status().is_success(), "ClickHouse setup: {statement}");
    }
    let mut exact_request = client
        .post(&clickhouse_url)
        .body(format!("{sql} FORMAT TabSeparated"));
    if let Some(user) = &user {
        exact_request = exact_request.basic_auth(user, password.as_ref());
    }
    let exact = exact_request.send().await.unwrap().bytes().await.unwrap();

    let (workload, config) = mixed_workload(sql);
    let publication = control_plane::clickhouse::compile_clickhouse_workload(&workload)
        .await
        .unwrap();
    let entry = publication.query_plan.entries.values().next().unwrap();
    assert!(entry.nodes.values().any(|node| matches!(
        node,
        control_plane::query_plan::QueryPlanNode::ExternalExact { .. }
    )));
    assert!(entry.nodes.values().any(|node| matches!(
        node,
        control_plane::query_plan::QueryPlanNode::ReadMaterialization { .. }
    )));
    let install = publication.install_request(None, Vec::new()).unwrap();

    let api_port = unused_port();
    let sql_port = unused_port();
    let output = tempfile::tempdir().unwrap();
    let mut bootstrap = tempfile::NamedTempFile::new().unwrap();
    writeln!(bootstrap, "aggregations: []").unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_data_plane"));
    command
        .arg("--streaming-config")
        .arg(bootstrap.path())
        .arg("--http-port")
        .arg(api_port.to_string())
        .arg("--clickhouse-http-port")
        .arg(sql_port.to_string())
        .arg("--clickhouse-url")
        .arg(&clickhouse_url)
        .arg("--clickhouse-backfill-table")
        .arg("telemetry")
        .arg("--clickhouse-backfill-database")
        .arg("default")
        .arg("--enable-backfill-worker")
        .arg("--precompute-allowed-lateness-ms")
        .arg("0")
        .arg("--precompute-flush-interval-ms")
        .arg("50")
        .arg("--persistence-delete-older-than-secs")
        .arg("0")
        .arg("--output-dir")
        .arg(output.path())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .env("RUST_LOG", "data_plane=info");
    if let Some(user) = &user {
        command.arg("--clickhouse-user").arg(user);
    }
    if let Some(password) = &password {
        command.arg("--clickhouse-password").arg(password);
    }
    let mut process = ChildGuard(command.spawn().unwrap());
    wait_http(
        &client,
        &format!("http://127.0.0.1:{api_port}/api/v1/health"),
        &mut process.0,
    )
    .await;
    wait_http(
        &client,
        &format!("http://127.0.0.1:{sql_port}/ping"),
        &mut process.0,
    )
    .await;

    let response = client
        .post(format!("http://127.0.0.1:{api_port}/api/v1/physical-plan"))
        .json(&install)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    assert!(status.is_success(), "stage publication: {body}");
    let response = client
        .post(format!(
            "http://127.0.0.1:{api_port}/api/v1/physical-plan/activate"
        ))
        .json(&serde_json::json!({"plan_id": 72, "plan_version": 1}))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success(), "activate publication");

    let response = client
        .post(format!("http://127.0.0.1:{api_port}/api/v1/db/backfill"))
        .json(&serde_json::json!({
            "agg_id": config.policy_fp_u64(),
            "start_ms": 0,
            "end_ms": 2000,
            "source": {"ClickHouse": {"database": "default", "table": "telemetry"}},
            "windows_total": 1
        }))
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    assert!(status.is_success(), "queue backfill: {body}");
    let mut backfill_complete = false;
    for _ in 0..200 {
        let jobs: serde_json::Value = client
            .get(format!(
                "http://127.0.0.1:{api_port}/api/v1/db/backfill/jobs"
            ))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if jobs["jobs"][0]["status"] == "complete" {
            backfill_complete = true;
            break;
        }
        assert_ne!(jobs["jobs"][0]["status"], "failed", "{jobs}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        backfill_complete,
        "backfill did not complete before timeout"
    );

    let mut mutate = client.post(&clickhouse_url).body(
        "ALTER TABLE default.telemetry UPDATE value = 999 WHERE 1 SETTINGS mutations_sync = 2",
    );
    if let Some(user) = &user {
        mutate = mutate.basic_auth(user, password.as_ref());
    }
    let response = mutate.send().await.unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    assert!(status.is_success(), "ClickHouse mutation: {body}");

    let mut mutated_request = client
        .post(&clickhouse_url)
        .body(format!("{sql} FORMAT TabSeparated"));
    if let Some(user) = &user {
        mutated_request = mutated_request.basic_auth(user, password.as_ref());
    }
    let mutated = mutated_request.send().await.unwrap().bytes().await.unwrap();
    assert_ne!(
        mutated, exact,
        "mutation guard must change the full exact result"
    );

    let mut mixed = client
        .get(format!("http://127.0.0.1:{sql_port}/"))
        .query(&[("query", sql)]);
    if let Some(user) = &user {
        mixed = mixed.header("x-clickhouse-user", user);
    }
    if let Some(password) = &password {
        mixed = mixed.header("x-clickhouse-key", password);
    }
    let response = mixed.send().await.unwrap();
    let status = response.status();
    let execution = response
        .headers()
        .get("x-asap-execution")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("missing")
        .to_owned();
    let failure_reason = response
        .headers()
        .get("x-asap-failure-reason")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("missing")
        .to_owned();
    let actual = response.bytes().await.unwrap();
    assert!(status.is_success(), "mixed listener returned {status}");
    assert_eq!(execution, "hybrid", "failure reason: {failure_reason}");
    assert_eq!(
        actual, exact,
        "compiled mixed result must equal pre-mutation exact baseline"
    );
}
