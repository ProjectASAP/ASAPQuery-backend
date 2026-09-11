//! Optional real-ClickHouse protocol and Grafana smoke coverage.
//!
//! Set `CLICKHOUSE_URL` (for example `http://127.0.0.1:8123`) to run it.

use std::{
    collections::HashMap,
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

async fn spawn_backend(
    clickhouse_url: &str,
    user: Option<&str>,
    password: Option<&str>,
    output: &std::path::Path,
    bootstrap: &std::path::Path,
    api_port: u16,
    sql_port: u16,
) -> ChildGuard {
    let client = reqwest::Client::new();
    let mut command = Command::new(env!("CARGO_BIN_EXE_data_plane"));
    command
        .arg("--streaming-config")
        .arg(bootstrap)
        .arg("--http-port")
        .arg(api_port.to_string())
        .arg("--clickhouse-http-port")
        .arg(sql_port.to_string())
        .arg("--clickhouse-url")
        .arg(&clickhouse_url)
        .arg("--clickhouse-backfill-table")
        .arg("deployment_default_not_the_job_table")
        .arg("--clickhouse-backfill-database")
        .arg("default")
        .arg("--clickhouse-backfill-value-column")
        .arg("wrong_value")
        .arg("--enable-backfill-worker")
        .arg("--precompute-allowed-lateness-ms")
        .arg("0")
        .arg("--precompute-flush-interval-ms")
        .arg("50")
        .arg("--persistence-delete-older-than-secs")
        .arg("0")
        .arg("--output-dir")
        .arg(output)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .env("RUST_LOG", "data_plane=info");
    if let Some(user) = user {
        command.arg("--clickhouse-user").arg(user);
    }
    if let Some(password) = password {
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

    process
}

fn mixed_workload(sql: &str) -> control_plane::clickhouse::ClickHouseSqlAutomaticWorkload {
    use control_plane::physical::compiler::{PlanEnvelope, BACKEND_COMPAT, PLANNER_REVISION};
    use planner_types::pre_asap::{Column, DataType, Schema};
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
    let schema = |time: &str, value: &str| {
        Schema::with_time_index(
            vec![
                Column::new(time, DataType::Timestamp, false),
                Column::new(value, DataType::Float64, false),
                Column::new("metric", DataType::Utf8, false),
            ],
            0,
            vec![],
        )
    };
    control_plane::clickhouse::ClickHouseSqlAutomaticWorkload {
        envelope,
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
    }
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
    for aggregate in ["sum(value)", "count(*)", "max(value)"] {
        run_mixed_aggregate(aggregate).await;
    }
}

async fn run_mixed_aggregate(aggregate: &str) {
    let Ok(clickhouse_url) = std::env::var("CLICKHOUSE_URL") else {
        eprintln!("skipping mixed process E2E because CLICKHOUSE_URL is unset");
        return;
    };
    let user = std::env::var("CLICKHOUSE_USER").ok();
    let password = std::env::var("CLICKHOUSE_PASSWORD").ok();
    let client = reqwest::Client::new();
    eprintln!("checking mixed aggregate {aggregate}");
    let sql = format!("SELECT sums.timestamp, sums.total / divisors.divisor AS ratio FROM (SELECT 2000 AS timestamp, {aggregate} AS total FROM telemetry WHERE metric = 'requests' AND timestamp_ms >= 0 AND timestamp_ms < 2000) AS sums INNER JOIN divisors ON sums.timestamp = divisors.timestamp");
    let grouped = aggregate == "max(value)";
    let sql = if grouped {
        "SELECT labels, max(value) AS value FROM telemetry WHERE metric = 'requests' AND timestamp_ms > 1999 - 2000 AND timestamp_ms <= 1999 GROUP BY labels ORDER BY labels".to_string()
    } else {
        sql
    };
    let format = if grouped { "JSON" } else { "TabSeparated" };
    let value_type = if aggregate == "count(*)" {
        "Nullable(Float64)"
    } else {
        "Float64"
    };
    let create_telemetry = format!("CREATE TABLE default.telemetry(metric String, labels Map(String,String), timestamp_ms Int64, value {value_type}, wrong_value Float64) ENGINE=Memory");
    for statement in [
        "DROP TABLE IF EXISTS default.telemetry",
        "DROP TABLE IF EXISTS default.divisors",
        create_telemetry.as_str(),
        "CREATE TABLE default.divisors(timestamp Int64, divisor Float64) ENGINE=Memory",
        "INSERT INTO default.telemetry VALUES ('requests',map('member','a'),0,2,10000),('requests',map('member','b'),1100,3,10000),('errors',map('member','c'),1100,99999,10000),('requests',map('member','a'),2000,88888,10000)",
        "INSERT INTO default.divisors VALUES (2000,10)",
    ] {
        let mut request = client.post(&clickhouse_url).body(statement.to_owned());
        if let Some(user) = &user {
            request = request.basic_auth(user, password.as_ref());
        }
        let response = request.send().await.unwrap();
        assert!(response.status().is_success(), "ClickHouse setup: {statement}");
    }
    if aggregate == "count(*)" {
        let mut request = client.post(&clickhouse_url).body(
            "INSERT INTO default.telemetry VALUES ('requests',map('member','nullable'),1200,NULL,10000)",
        );
        if let Some(user) = &user {
            request = request.basic_auth(user, password.as_ref());
        }
        assert!(request.send().await.unwrap().status().is_success());
    }
    let mut exact_request = client
        .post(&clickhouse_url)
        .body(format!("{sql} FORMAT {format}"));
    if let Some(user) = &user {
        exact_request = exact_request.basic_auth(user, password.as_ref());
    }
    let exact = exact_request.send().await.unwrap().bytes().await.unwrap();

    let mut workload = mixed_workload(&sql);
    if grouped {
        use planner_types::pre_asap::{Column, DataType};
        workload
            .tables
            .get_mut("telemetry")
            .unwrap()
            .columns
            .push(Column::new(
                "labels",
                DataType::Map {
                    key: Box::new(DataType::Utf8),
                    value: Box::new(DataType::Utf8),
                    value_nullable: false,
                },
                false,
            ));
    }

    workload.tables.get_mut("telemetry").unwrap().columns[1].nullable = aggregate == "count(*)";
    if aggregate == "count(*)" {
        let mut nullable_count = mixed_workload(&sql.replace("count(*)", "count(value)"));
        nullable_count.tables.get_mut("telemetry").unwrap().columns[1].nullable = true;
        let result =
            control_plane::clickhouse::compile_automatic_clickhouse_workload(&nullable_count).await;
        assert!(
            result.is_err(),
            "nullable count(value) requires explicit null exclusion"
        );
    }
    let (publication, selection_trace) =
        control_plane::clickhouse::compile_automatic_clickhouse_workload(&workload)
            .await
            .unwrap();
    if let Ok(path) = std::env::var("CLICKHOUSE_PLANNING_ARTIFACT") {
        std::fs::write(
            path,
            serde_json::to_vec_pretty(&serde_json::json!({
                "publication": &publication, "selection_trace": &selection_trace,
            }))
            .unwrap(),
        )
        .unwrap();
    }
    assert_eq!(publication.precompute_plan.materializations.len(), 1);
    let config = publication.precompute_plan.materializations[0].clone();
    if aggregate == "count(*)" {
        assert_eq!(
            config.effective_value_projection(),
            &asap_types::sds::ValueProjectionIdentity::Constant {
                value: planner_types::pre_asap::ScalarValue::Int64(1),
            }
        );
    }
    let entry = publication.query_plan.entries.values().next().unwrap();
    assert_eq!(
        entry.nodes.values().any(|node| matches!(
            node,
            asap_types::query_plan::QueryPlanNode::ExternalExact { .. }
        )),
        !grouped
    );
    assert!(entry.nodes.values().any(|node| matches!(
        node,
        asap_types::query_plan::QueryPlanNode::ReadMaterialization { .. }
    )));
    let install = publication.install_request(None, Vec::new()).unwrap();

    let api_port = unused_port();
    let sql_port = unused_port();
    let output = tempfile::tempdir().unwrap();
    let mut bootstrap = tempfile::NamedTempFile::new().unwrap();
    writeln!(bootstrap, "aggregations: []").unwrap();
    let _process = spawn_backend(
        &clickhouse_url,
        user.as_deref(),
        password.as_deref(),
        output.path(),
        bootstrap.path(),
        api_port,
        sql_port,
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
        "ALTER TABLE default.telemetry DELETE WHERE metric = 'requests' AND timestamp_ms = 0 SETTINGS mutations_sync = 2",
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
        .body(format!("{sql} FORMAT {format}"));
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
        .query(&[("query", sql.as_str()), ("default_format", format)]);
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
    assert_eq!(
        execution,
        if grouped { "warm" } else { "hybrid" },
        "failure reason: {failure_reason}"
    );
    if grouped {
        let actual: serde_json::Value = serde_json::from_slice(&actual).unwrap();
        let exact: serde_json::Value = serde_json::from_slice(&exact).unwrap();
        assert_eq!(actual["meta"], exact["meta"]);
        let actual_rows = actual["data"].as_array().unwrap();
        let exact_rows = exact["data"].as_array().unwrap();
        assert_eq!(actual_rows.len(), exact_rows.len());
        for (actual, exact) in actual_rows.iter().zip(exact_rows) {
            assert_eq!(actual["labels"], exact["labels"]);
            assert_eq!(
                actual["value"].as_f64().unwrap().to_bits(),
                exact["value"].as_f64().unwrap().to_bits()
            );
        }
        assert_eq!(actual["data"].as_array().unwrap().len(), 2);
    } else {
        assert_eq!(
            actual, exact,
            "compiled mixed result must equal pre-mutation exact baseline"
        );
    }
}

#[tokio::test]
async fn array_sql_executes_local_element_after_typed_exact_leaf() {
    use asap_types::query_plan::QueryPlanNode;
    use planner_types::{
        post_asap::ValueOperation,
        pre_asap::{Column, DataType, QueryExpr, Schema},
    };
    let Ok(clickhouse_url) = std::env::var("CLICKHOUSE_URL") else {
        eprintln!("skipping collection process E2E because CLICKHOUSE_URL is unset");
        return;
    };
    let user = std::env::var("CLICKHOUSE_USER").ok();
    let password = std::env::var("CLICKHOUSE_PASSWORD").ok();
    let client = reqwest::Client::new();
    let table = format!("asap_collection_elements_{}", std::process::id());
    for sql in [
        format!("CREATE TABLE default.{table}(timestamp Int64, samples Array(Float64), nullable_samples Array(Nullable(Float64)), position Nullable(Int64)) ENGINE=Memory"),
        format!("INSERT INTO default.{table} VALUES (100,[10,20],[10,20],-1),(200,[3.5],[3.5],9),(300,[],[],1),(400,[7.5],[NULL],1),(500,[1],[1],NULL),(2000,[999],[999],1)"),
    ] {
        let mut request = client.post(&clickhouse_url).body(sql);
        if let Some(user) = &user { request = request.basic_auth(user, password.as_ref()); }
        let response = request.send().await.unwrap();
        let status = response.status();
        let body = response.text().await.unwrap();
        assert!(status.is_success(), "native collection fixture: {body}");
    }
    let sql = format!("SELECT arrayElement(samples, position) AS result, arrayElement(nullable_samples, position) AS nullable_result FROM {table} WHERE timestamp >= 0 AND timestamp < 2000 ORDER BY result NULLS FIRST");
    let mut workload = mixed_workload(&sql);
    workload.tables = HashMap::from([(
        table.clone(),
        Schema::with_time_index(
            vec![
                Column::new("timestamp", DataType::Int64, false),
                Column::new(
                    "samples",
                    DataType::List {
                        element: Box::new(Column::new("item", DataType::Float64, false)),
                    },
                    false,
                ),
                Column::new(
                    "nullable_samples",
                    DataType::List {
                        element: Box::new(Column::new("item", DataType::Float64, true)),
                    },
                    false,
                ),
                Column::new("position", DataType::Int64, true),
            ],
            0,
            vec![],
        ),
    )]);
    let (publication, trace) =
        control_plane::clickhouse::compile_automatic_clickhouse_workload(&workload)
            .await
            .unwrap();
    assert!(publication.precompute_plan.materializations.is_empty());
    let entry = publication.query_plan.entries.values().next().unwrap();
    assert!(entry
        .nodes
        .values()
        .any(|node| matches!(node, QueryPlanNode::ExternalExact { .. })));
    assert!(entry.nodes.values().any(|node| {
        let QueryPlanNode::Relational { operation, .. } = node else { return false; };
        let ValueOperation::Project { cols, .. } = serde_json::from_value(operation.clone()).unwrap() else { return false; };
        cols.iter().any(|column| matches!(&column.expr, QueryExpr::FunctionCall { name, .. } if name == "asap_element_access"))
    }), "Planner-selected DAG must preserve local element evaluation");
    eprintln!(
        "collection Planner selection: {}",
        serde_json::to_string(&trace).unwrap()
    );
    let install = publication.install_request(None, Vec::new()).unwrap();
    let api_port = unused_port();
    let sql_port = unused_port();
    let output = tempfile::tempdir().unwrap();
    let mut bootstrap = tempfile::NamedTempFile::new().unwrap();
    writeln!(bootstrap, "aggregations: []").unwrap();
    let _process = spawn_backend(
        &clickhouse_url,
        user.as_deref(),
        password.as_deref(),
        output.path(),
        bootstrap.path(),
        api_port,
        sql_port,
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
    assert!(status.is_success(), "stage collection publication: {body}");
    let response = client
        .post(format!(
            "http://127.0.0.1:{api_port}/api/v1/physical-plan/activate"
        ))
        .json(&serde_json::json!({"plan_id":72,"plan_version":1}))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let mut native = client
        .post(&clickhouse_url)
        .body(format!("{sql} FORMAT JSON"));
    if let Some(user) = &user {
        native = native.basic_auth(user, password.as_ref());
    }
    let expected: serde_json::Value = native.send().await.unwrap().json().await.unwrap();
    let mut actual_request = client
        .get(format!("http://127.0.0.1:{sql_port}/"))
        .query(&[("query", sql.as_str()), ("default_format", "JSON")]);
    if let Some(user) = &user {
        actual_request = actual_request.basic_auth(user, password.as_ref());
    }
    let response = actual_request.send().await.unwrap();
    assert!(response.status().is_success());
    assert_eq!(
        response.headers().get("x-asap-execution").unwrap(),
        "exact_fallback"
    );
    assert_eq!(
        response.headers().get("x-asap-execution-detail").unwrap(),
        "external_dag"
    );
    let actual: serde_json::Value = response.json().await.unwrap();
    for field in ["meta", "rows"] {
        assert_eq!(actual[field], expected[field], "{field}");
    }
    let actual_rows = actual["data"].as_array().unwrap();
    let expected_rows = expected["data"].as_array().unwrap();
    assert_eq!(actual_rows.len(), expected_rows.len());
    for (actual, expected) in actual_rows.iter().zip(expected_rows) {
        for field in ["result", "nullable_result"] {
            if expected[field].is_null() {
                assert!(actual[field].is_null());
            } else {
                // Both declared columns are Float64; JSON 20 and 20.0 encode
                // the same value despite different serde_json Number variants.
                assert_eq!(
                    actual[field].as_f64().unwrap().to_bits(),
                    expected[field].as_f64().unwrap().to_bits(),
                    "{field}"
                );
            }
        }
    }
    assert_eq!(
        actual["data"],
        serde_json::json!([{ "result":null, "nullable_result":null },{ "result":0.0, "nullable_result":null },{ "result":0.0, "nullable_result":null },{ "result":7.5, "nullable_result":null },{ "result":20.0, "nullable_result":20.0 }])
    );
    let mut cleanup = client
        .post(&clickhouse_url)
        .body(format!("DROP TABLE default.{table}"));
    if let Some(user) = &user {
        cleanup = cleanup.basic_auth(user, password.as_ref());
    }
    assert!(cleanup.send().await.unwrap().status().is_success());
}
