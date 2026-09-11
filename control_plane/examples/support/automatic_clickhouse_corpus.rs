use control_plane::clickhouse::{ClickHouseSqlAutomaticWorkload, ClickHouseSqlWorkloadEntry};
use control_plane::physical::compiler::{PlanEnvelope, BACKEND_COMPAT, PLANNER_REVISION};
use planner_types::pre_asap::{Column, DataType, Schema};
use planner_types::types::AccuracyTarget;
use serde::Deserialize;
use serde_json::json;
use std::{fs, path::PathBuf};

#[derive(Deserialize)]
struct Corpus {
    queries: Vec<Row>,
}

#[derive(Deserialize)]
struct Row {
    id: String,
    clickhouse_sql: Option<String>,
    operators: Vec<String>,
    promql: String,
}

fn publication_inputs(
    schema: &Schema,
    sql: String,
    window_ms: u64,
) -> ClickHouseSqlAutomaticWorkload {
    ClickHouseSqlAutomaticWorkload {
        envelope: PlanEnvelope {
            plan_id: 27,
            plan_version: 1,
            generated_at_unix_ms: 1_788_891_296_000,
            activation_unix_ms: 1_788_891_296_000,
            expiry_unix_ms: None,
            backend_compat: BACKEND_COMPAT.into(),
            planner_revision: PLANNER_REVISION.into(),
            capability_snapshot_id: "sql27-automatic-eval".into(),
        },
        tables: std::collections::HashMap::from([("raw_samples".into(), schema.clone())]),
        accuracy: AccuracyTarget::Epsilon(0.01),
        queries: vec![ClickHouseSqlWorkloadEntry {
            sql,
            start_ms: 1_788_891_296_001 - window_ms,
            end_ms: 1_788_891_296_001,
            cumulative: false,
        }],
    }
}

pub async fn run() {
    let path = std::env::args_os()
        .nth(2)
        .map(PathBuf::from)
        .expect("usage: audit_clickhouse_corpus --automatic CORPUS.json");
    let corpus: Corpus = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    let schema = Schema::with_time_index(
        vec![
            Column::new("metric", DataType::Utf8, false),
            Column::new(
                "labels",
                DataType::Map {
                    key: Box::new(DataType::Utf8),
                    value: Box::new(DataType::Utf8),
                    value_nullable: false,
                },
                false,
            ),
            Column::new("ts_ms", DataType::Int64, false),
            Column::new("value", DataType::Float64, false),
        ],
        2,
        vec![],
    );
    let mut rows = Vec::new();
    for row in corpus.queries {
        let Some(sql) = row.clickhouse_sql else {
            rows.push(json!({
                "id": row.id,
                "operators": row.operators,
                "parser_planner": "failure",
                "reason": "missing ClickHouse SQL mapping"
            }));
            continue;
        };
        let sql = sql.replace("{eval_ms}", "1788891296000");
        // Evaluation span only: SQL source boundaries are still validated by the compiler.
        let window_ms = row
            .promql
            .split('[')
            .skip(1)
            .filter_map(|range| {
                let digits = range.chars().take_while(char::is_ascii_digit).count();
                let number = range[..digits].parse::<u64>().ok()?;
                let unit = range.as_bytes().get(digits)?;
                let multiplier = match unit {
                    b's' => 1_000,
                    b'm' => 60_000,
                    b'h' => 3_600_000,
                    b'd' => 86_400_000,
                    _ => return None,
                };
                Some(number * multiplier)
            })
            .max()
            .unwrap_or(300_000);
        match control_plane::clickhouse::compile_automatic_clickhouse_workload(&publication_inputs(
            &schema,
            sql.clone(),
            window_ms,
        ))
        .await
        {
            Ok((publication, traces)) => rows.push(json!({
                "id": row.id, "operators": row.operators, "sql": sql,
                "publication": "pass", "execution": "not_run",
                "selection_traces": traces,
                "window_start_ms": 1_788_891_296_001u64 - window_ms,
                "window_end_ms": 1_788_891_296_001u64,
                "install": publication.install_request(None, Vec::new()).unwrap(),
                "summary_catalog": publication.summary_catalog,
                "precompute_plan": publication.precompute_plan,
                "query_plan": publication.query_plan,
            })),
            Err(error) => rows.push(json!({
                "id": row.id, "operators": row.operators, "sql": sql,
                "publication": "failure", "execution": "not_run", "reason": error.to_string(),
            })),
        }
    }

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "baseline": env!("CARGO_PKG_VERSION"),
            "query_count": rows.len(),
            "queries": rows,
        }))
        .unwrap()
    );
}
