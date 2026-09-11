use asap_frontend_sql::SqlCatalog;
use asap_types::{AggregationType, KeyByLabelNames, PrecomputeMaterialization, WindowKind};
use control_plane::clickhouse::{ClickHouseSqlWorkload, ClickHouseSqlWorkloadEntry};
use control_plane::physical::compiler::{
    PlanEnvelope, PrecomputePlan, BACKEND_COMPAT, PLANNER_REVISION,
};
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
}

fn publication_inputs(schema: &Schema, sql: String) -> ClickHouseSqlWorkload {
    let mut materialization = PrecomputeMaterialization::new(
        AggregationType::MinMax,
        String::new(),
        std::collections::HashMap::from([("variant".into(), json!(2))]),
        KeyByLabelNames::new(vec!["labels".into()]),
        KeyByLabelNames::empty(),
        KeyByLabelNames::empty(),
        String::new(),
        60,
        60,
        WindowKind::Tumbling,
        String::new(),
        "raw_samples.value".into(),
        None,
        Some("raw_samples".into()),
        Some("value".into()),
    );
    materialization.pane_origin_ms = Some(0);
    let envelope = PlanEnvelope {
        plan_id: 27,
        plan_version: 1,
        generated_at_unix_ms: 1_788_891_296_000,
        activation_unix_ms: 1_788_891_296_000,
        expiry_unix_ms: None,
        backend_compat: BACKEND_COMPAT.into(),
        planner_revision: PLANNER_REVISION.into(),
        capability_snapshot_id: "sql27-main-eval".into(),
    };
    let mut precompute_plan =
        PrecomputePlan::build_backend_local(envelope.clone(), vec![materialization.clone()])
            .unwrap();
    let mut transmission_plan = control_plane::physical::compiler::compile_transmission_plan(
        envelope,
        &precompute_plan,
        &Default::default(),
    )
    .unwrap();
    let sds = asap_types::summary_catalog::SummaryCatalog::from_materializations(
        27,
        1,
        &[materialization],
    )
    .unwrap();
    let reference = sds.reference().unwrap();
    precompute_plan.summary_catalog = Some(reference.clone());
    transmission_plan.summary_catalog = Some(reference);
    ClickHouseSqlWorkload {
        sds,
        precompute_plan,
        transmission_plan,
        tables: std::collections::HashMap::from([("raw_samples".into(), schema.clone())]),
        accuracy: AccuracyTarget::Epsilon(0.01),
        queries: vec![ClickHouseSqlWorkloadEntry {
            sql,
            start_ms: 1_788_848_096_000,
            end_ms: 1_788_891_296_000,
            cumulative: false,
        }],
    }
}

#[tokio::main]
async fn main() {
    let path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .expect("usage: audit_clickhouse_corpus CORPUS.json");
    let corpus: Corpus = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
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
    let catalog = SqlCatalog::new().with_table("raw_samples", schema.clone());
    let accuracy = AccuracyTarget::Epsilon(0.01);
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
        match control_plane::clickhouse::plan_clickhouse_sql(&sql, &catalog, accuracy.clone()).await
        {
            Ok(planned) => {
                let publication = control_plane::clickhouse::compile_clickhouse_workload(
                    &publication_inputs(&schema, sql.clone()),
                )
                .await;
                match publication {
                    Ok(publication) => rows.push(json!({
                        "id": row.id,
                        "operators": row.operators,
                        "parser_planner": "pass",
                        "publication": "pass",
                        "classification": "publication_ready",
                        "query_plan_nodes": publication.query_plan.entries.values().next().map(|entry| &entry.nodes),
                        "canonical_sql": planned.canonical_sql,
                        "physical": format!("{:?}", planned.physical),
                        "sql": sql,
                    })),
                    Err(error) => rows.push(json!({
                        "id": row.id,
                        "operators": row.operators,
                        "parser_planner": "pass",
                        "publication": "failure",
                        "classification": "failure",
                        "reason": error.to_string(),
                        "canonical_sql": planned.canonical_sql,
                        "physical": format!("{:?}", planned.physical),
                        "sql": sql,
                    })),
                }
            }
            Err(error) => rows.push(json!({
                "id": row.id,
                "operators": row.operators,
                "parser_planner": "failure",
                "reason": error.to_string(),
                "sql": sql,
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
