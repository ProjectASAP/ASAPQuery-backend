//! Optional OS-process E2E for a q05-class bounded SQL max query.
//! Run with `CLICKHOUSE_URL=http://127.0.0.1:8123 cargo test -p data_plane --test clickhouse_q05_process_e2e`.

#[path = "support/empty_physical_plan.rs"]
mod empty_physical_plan;

use control_plane::physical::compiler::{PlanEnvelope, BACKEND_COMPAT, PLANNER_REVISION};
use planner_types::pre_asap::{Column, DataType, Schema};
use std::io::Read;
use std::{collections::HashMap, process::Stdio, time::Duration};

fn process_snapshot(pid: u32) -> serde_json::Value {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
    let fields: Vec<_> = stat.split_whitespace().collect();
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap();
    let field = |name: &str| {
        status
            .lines()
            .find(|line| line.starts_with(name))
            .map(str::to_owned)
    };
    serde_json::json!({
        "pid": pid,
        "cpu_ticks": fields[13].parse::<u64>().unwrap() + fields[14].parse::<u64>().unwrap(),
        "rss_pages": fields[23].parse::<u64>().unwrap(),
        "vm_rss": field("VmRSS:"),
        "vm_hwm": field("VmHWM:"),
    })
}

fn directory_bytes(path: &std::path::Path) -> u64 {
    std::fs::read_dir(path)
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| {
            let path = entry.path();
            if path.is_dir() {
                directory_bytes(&path)
            } else {
                entry.metadata().map(|meta| meta.len()).unwrap_or(0)
            }
        })
        .sum()
}

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
    let end_ms = std::env::var("CLICKHOUSE_BENCH_END_MS")
        .ok()
        .map(|value| value.parse::<u64>().expect("valid benchmark end timestamp"))
        .unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
        });
    let start_ms = end_ms - 43_200_000;
    let input_path = std::env::var("CLICKHOUSE_BENCH_INPUT").ok();
    let metric = std::env::var("CLICKHOUSE_BENCH_METRIC")
        .unwrap_or_else(|_| "cache_refresh_lag_seconds".into());
    let clickhouse_pid = std::env::var("CLICKHOUSE_PID")
        .ok()
        .and_then(|value| value.parse::<u32>().ok());
    let clickhouse_initial = clickhouse_pid.map(process_snapshot);
    let experiment_started = std::time::Instant::now();
    let setup = [
        "CREATE DATABASE IF NOT EXISTS asap_q05_e2e".to_string(),
        "DROP TABLE IF EXISTS asap_q05_e2e.q05_samples".to_string(),
        "CREATE TABLE asap_q05_e2e.q05_samples(metric String, labels String, ts_ms Int64, value Float64) ENGINE=MergeTree ORDER BY ts_ms".to_string(),
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
    let rows = if let Some(path) = &input_path {
        std::fs::read_to_string(path).expect("read benchmark JSONEachRow input")
    } else {
        [start_ms + 1_000, start_ms + 2_000]
            .into_iter()
            .zip([7.0, 11.0])
            .map(|(ts_ms, value)| serde_json::json!({"metric": metric, "labels": metric, "ts_ms": ts_ms, "value": value}).to_string())
            .collect::<Vec<_>>()
            .join("\n")
    };
    let mut series = std::collections::BTreeSet::new();
    let mut input_samples = 0_u64;
    for line in rows.lines().filter(|line| !line.is_empty()) {
        let row: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(row["metric"].as_str(), Some(metric.as_str()));
        series.insert(row["labels"].as_str().unwrap().to_owned());
        input_samples += 1;
    }
    assert_eq!(
        series.len(),
        1,
        "scalar max probe requires one original series"
    );
    let inserted = clickhouse_auth(client.post(&clickhouse))
        .body(format!(
            "INSERT INTO asap_q05_e2e.q05_samples FORMAT JSONEachRow\n{rows}"
        ))
        .send()
        .await
        .unwrap();
    assert!(
        inserted.status().is_success(),
        "{}",
        inserted.text().await.unwrap()
    );
    drop(rows);
    let source_load_elapsed_ns = experiment_started.elapsed().as_nanos();
    let clickhouse_post_load = clickhouse_pid.map(process_snapshot);

    let aggregate = std::env::var("CLICKHOUSE_BENCH_AGGREGATE").unwrap_or_else(|_| "max".into());
    assert!(matches!(aggregate.as_str(), "max" | "sum"));
    let sql = format!(
        "SELECT {aggregate}(value) AS value FROM q05_samples WHERE ts_ms>={start_ms} AND ts_ms<{end_ms}"
    );
    let envelope = PlanEnvelope {
        plan_id: 73,
        plan_version: 1,
        generated_at_unix_ms: 0,
        activation_unix_ms: 0,
        expiry_unix_ms: None,
        backend_compat: BACKEND_COMPAT.into(),
        planner_revision: PLANNER_REVISION.into(),
        capability_snapshot_id: "test".into(),
    };
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
    let (compiled, selection_trace) =
        control_plane::clickhouse::compile_automatic_clickhouse_workload(
            &control_plane::clickhouse::ClickHouseSqlAutomaticWorkload {
                envelope,
                tables: HashMap::from([("q05_samples".into(), schema)]),
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

    assert_eq!(compiled.precompute_plan.materializations.len(), 1);
    let config = compiled.precompute_plan.materializations[0].clone();
    let planning =
        serde_json::json!({"selection_trace": selection_trace, "publication": &compiled});
    let http_port = free_port();
    let mut sql_port = free_port();
    while sql_port == http_port {
        sql_port = free_port();
    }
    let mut physical = tempfile::NamedTempFile::new().unwrap();
    serde_json::to_writer(&mut physical, &empty_physical_plan::empty()).unwrap();
    let physical_path = physical.path().to_str().unwrap();
    let output_dir = tempfile::tempdir().unwrap();
    let output_dir_arg = output_dir.path().to_str().unwrap().to_owned();
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_data_plane"));
    command.args([
        "--http-port",
        &http_port.to_string(),
        "--physical-plan",
        physical_path,
        "--clickhouse-http-port",
        &sql_port.to_string(),
        "--clickhouse-url",
        &clickhouse,
        "--clickhouse-database",
        "asap_q05_e2e",
        "--clickhouse-backfill-table",
        "q05_samples",
        "--clickhouse-backfill-database",
        "asap_q05_e2e",
        "--clickhouse-backfill-timestamp-column",
        "ts_ms",
        "--enable-backfill-worker",
        "--output-dir",
        &output_dir_arg,
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
    let mut ready = false;
    for _ in 0..100 {
        if client.get(format!("{base}/health")).send().await.is_ok() {
            ready = true;
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
    assert!(ready, "backend did not become ready before poll deadline");
    let install = data_plane::drivers::query::servers::http::PhysicalPlanInstallRequest {
        summary_catalog: compiled.summary_catalog,
        collector_plans: compiled.collector_plans,
        precompute_plan: compiled.precompute_plan,
        transmission_plan: compiled.transmission_plan,
        query_plan: compiled.query_plan,
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
    let backfill_started = std::time::Instant::now();
    let backend_pre_backfill = process_snapshot(child.0.id());
    let clickhouse_pre_backfill = clickhouse_pid.map(process_snapshot);
    let response: serde_json::Value = client
        .post(format!("{base}/api/v1/db/backfill"))
        .json(
            &serde_json::json!({"agg_id":agg_id,"start_ms":start_ms,"end_ms":end_ms,
            "source":{"ClickHouse":{"database":"asap_q05_e2e","table":"q05_samples"}},"windows_total":1}),
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
    let mut backfill_complete = false;
    let mut last_backfill_status = serde_json::Value::Null;
    for _ in 0..300 {
        let status: serde_json::Value = client
            .get(format!("{base}/api/v1/db/backfill/jobs/{job}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        last_backfill_status = status.clone();
        match status["job"]["status"].as_str() {
            Some("complete") => {
                backfill_complete = true;
                break;
            }
            Some("failed") => panic!("backfill failed: {status}"),
            _ => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    assert!(
        backfill_complete,
        "backfill did not complete before poll deadline: {last_backfill_status}"
    );
    let build_elapsed_ns = experiment_started.elapsed().as_nanos();
    let backfill_elapsed_ns = backfill_started.elapsed().as_nanos();
    let post_build = process_snapshot(child.0.id());
    let clickhouse_post_build = clickhouse_pid.map(process_snapshot);
    let first_warm_started = std::time::Instant::now();
    let warm = client
        .post(format!("http://127.0.0.1:{sql_port}/"))
        .body(sql.clone())
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
    let first_warm_ns = first_warm_started.elapsed().as_nanos();
    let first_exact_started = std::time::Instant::now();
    let exact_value: f64 = clickhouse_auth(client.post(&clickhouse))
        .body(format!("SELECT {aggregate}(value) FROM asap_q05_e2e.q05_samples WHERE ts_ms>={start_ms} AND ts_ms<{end_ms} FORMAT TabSeparated"))
        .send().await.unwrap().text().await.unwrap().trim().parse().unwrap();
    let first_exact_ns = first_exact_started.elapsed().as_nanos();
    assert_eq!(warm_value, exact_value);

    if let Ok(output) = std::env::var("CLICKHOUSE_BENCH_OUTPUT") {
        let exact_sql = format!("SELECT {aggregate}(value) FROM asap_q05_e2e.q05_samples WHERE ts_ms>={start_ms} AND ts_ms<{end_ms} FORMAT TabSeparated");
        for _ in 0..10 {
            let _ = client
                .post(format!("http://127.0.0.1:{sql_port}/"))
                .body(sql.clone())
                .send()
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            let _ = clickhouse_auth(client.post(&clickhouse))
                .body(exact_sql.clone())
                .send()
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
        }
        let pre_query = process_snapshot(child.0.id());
        let clickhouse_pre_query = clickhouse_pid.map(process_snapshot);
        let query_started = std::time::Instant::now();
        let mut requests = Vec::new();
        let iterations = std::env::var("CLICKHOUSE_BENCH_REPETITIONS")
            .ok()
            .map(|value| value.parse::<usize>().expect("valid repeat count"))
            .unwrap_or(100);
        assert!(iterations > 0);
        for iteration in 0..iterations {
            for route in if iteration % 2 == 0 {
                ["warm", "exact"]
            } else {
                ["exact", "warm"]
            } {
                let backend_before = process_snapshot(child.0.id());
                let clickhouse_before = clickhouse_pid.map(process_snapshot);
                let started = std::time::Instant::now();
                let response = if route == "warm" {
                    client
                        .post(format!("http://127.0.0.1:{sql_port}/"))
                        .body(sql.clone())
                        .send()
                        .await
                        .unwrap()
                } else {
                    clickhouse_auth(client.post(&clickhouse))
                        .body(exact_sql.clone())
                        .send()
                        .await
                        .unwrap()
                };
                let status = response.status().as_u16();
                let execution = response
                    .headers()
                    .get("x-asap-execution")
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned);
                let body = response.text().await.unwrap();
                let elapsed_ns = started.elapsed().as_nanos();
                assert_eq!(status, 200, "{route} request failed: {body}");
                assert_eq!(body.trim().parse::<f64>().unwrap(), exact_value);
                if route == "warm" {
                    assert_eq!(execution.as_deref(), Some("warm"));
                }
                let backend_after = process_snapshot(child.0.id());
                let clickhouse_after = clickhouse_pid.map(process_snapshot);
                requests.push(serde_json::json!({"iteration":iteration,"route":route,"elapsed_ns":elapsed_ns,"status":status,"execution":execution,"body":body,
                    "backend_before":backend_before,"backend_after":backend_after,
                    "clickhouse_before":clickhouse_before,"clickhouse_after":clickhouse_after}));
            }
        }
        let query_elapsed_ns = query_started.elapsed().as_nanos();
        let post_query = process_snapshot(child.0.id());
        let clickhouse_post_query = clickhouse_pid.map(process_snapshot);
        let clickhouse_storage_bytes = std::env::var("CLICKHOUSE_STORAGE")
            .ok()
            .map(|path| directory_bytes(std::path::Path::new(&path)));
        let table_stats = clickhouse_auth(client.post(&clickhouse))
            .body("SELECT sum(rows) AS rows, sum(bytes_on_disk) AS bytes_on_disk, sum(data_compressed_bytes) AS compressed_bytes FROM system.parts WHERE active AND database='asap_q05_e2e' AND table='q05_samples' FORMAT JSONEachRow")
            .send().await.unwrap().text().await.unwrap();
        let table_stats: serde_json::Value = serde_json::from_str(&table_stats).unwrap();
        let artifact = serde_json::json!({
            "schema_version": 1,
            "git_head": std::process::Command::new("git").args(["rev-parse", "HEAD"]).output().ok().and_then(|value| String::from_utf8(value.stdout).ok()).map(|value| value.trim().to_owned()),
            "clock_ticks_per_second": std::process::Command::new("getconf").arg("CLK_TCK").output().ok().and_then(|value| String::from_utf8(value.stdout).ok()).and_then(|value| value.trim().parse::<u64>().ok()),
            "query": sql,
            "input": {"path":input_path,"metric":metric,"samples":input_samples,"series":series,"start_ms":start_ms,"end_ms":end_ms},
            "planning_scope": "Real Planner selects the DAG and automatic control-plane binding constructs the catalog; fixed-window layout is not cost-optimized",
            "planning": planning,
            "classification_required": "warm",
            "build_phase": {"elapsed_ns":build_elapsed_ns,"backend":post_build,"backend_output_bytes":directory_bytes(output_dir.path()),"clickhouse_before":clickhouse_initial,"clickhouse_after":clickhouse_post_build},
            "source_load_phase": {"elapsed_ns":source_load_elapsed_ns,"clickhouse_before":clickhouse_initial,"clickhouse_after":clickhouse_post_load},
            "backfill_phase": {"elapsed_ns":backfill_elapsed_ns,"backend_before":backend_pre_backfill,"backend_after":post_build,"clickhouse_before":clickhouse_pre_backfill,"clickhouse_after":clickhouse_post_build},
            "first_query": {"warm_elapsed_ns":first_warm_ns,"exact_elapsed_ns":first_exact_ns},
            "query_phase": {"elapsed_ns":query_elapsed_ns,"backend_before":pre_query,"backend_after":post_query,"backend_output_bytes":directory_bytes(output_dir.path()),"clickhouse_before":clickhouse_pre_query,"clickhouse_after":clickhouse_post_query,"clickhouse_storage_bytes":clickhouse_storage_bytes},
            "clickhouse_table": table_stats,
            "requests": requests,
            "limitations": ["bounded single-series aggregate sensitivity; not full original o11y workload coverage", "CPU uses Linux scheduler ticks", "RSS is whole-process", "ClickHouse server is externally managed"],
        });
        std::fs::write(output, serde_json::to_vec_pretty(&artifact).unwrap()).unwrap();
    }
}
