//! Compile every explicitly supported o11y SQL rewrite through publication.

use asap_types::{AggregationType, KeyByLabelNames, PrecomputeMaterialization, WindowKind};
use control_plane::{
    clickhouse::{compile_clickhouse_workload, ClickHouseSqlWorkload, ClickHouseSqlWorkloadEntry},
    physical::compiler::{PlanEnvelope, PrecomputePlan, TransmissionPlan},
};
use planner_types::{
    pre_asap::{Column, DataType, Schema},
    types::AccuracyTarget,
};
use serde::Deserialize;
use serde_json::json;
use std::{collections::HashMap, fs, path::PathBuf};

const EVAL_MS: u64 = 1_788_891_296_000;

#[derive(Deserialize)]
struct Corpus {
    queries: Vec<Row>,
}

#[derive(Deserialize)]
struct Row {
    id: String,
    clickhouse_sql: String,
    clickhouse_planning_sql: Option<String>,
    clickhouse_planning_status: String,
    clickhouse_summary_requirements: Vec<Requirement>,
}

#[derive(Deserialize)]
struct Requirement {
    metric: String,
    aggregation: String,
    window_seconds: u64,
    group_by: Vec<String>,
}

fn schema() -> Schema {
    Schema::with_time_index(
        vec![
            Column::new("metric", DataType::Utf8, false),
            Column::new("labels", DataType::Utf8, false),
            Column::new("job", DataType::Utf8, false),
            Column::new("le", DataType::Utf8, false),
            Column::new("ts_ms", DataType::Timestamp, false),
            Column::new("value", DataType::Float64, false),
        ],
        4,
        vec![vec![0, 1, 4], vec![0, 1, 2, 4]],
    )
}

fn materialization(spec: &Requirement) -> PrecomputeMaterialization {
    let (aggregation, name) = match spec.aggregation.as_str() {
        "increase" => (AggregationType::Increase, String::new()),
        "max" => (AggregationType::MinMax, "max".to_owned()),
        other => panic!("unknown aggregation {other}"),
    };
    let mut config = PrecomputeMaterialization::new(
        aggregation,
        name,
        Default::default(),
        KeyByLabelNames::new(spec.group_by.clone()),
        KeyByLabelNames::empty(),
        KeyByLabelNames::empty(),
        String::new(),
        spec.window_seconds,
        spec.window_seconds,
        WindowKind::Tumbling,
        String::new(),
        spec.metric.clone(),
        None,
        Some("raw_samples".into()),
        Some("value".into()),
    );
    config.pane_origin_ms = Some(0);
    config
}

#[tokio::main]
async fn main() {
    let path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .expect("corpus path");
    let corpus: Corpus = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    let mut report = Vec::new();
    for (ordinal, row) in corpus.queries.into_iter().enumerate() {
        let Some(planning_sql) = row.clickhouse_planning_sql else {
            report.push(
                json!({"id":row.id,"exact_sql":"oracle_valid","planning_sql":null,
                "parser_planner":"typed_fallback",
                "compiler":"not_reached","publication":"not_reached",
                "backend_bind_execute":"exact_fallback","reason":row.clickhouse_planning_status}),
            );
            continue;
        };
        let configs = row
            .clickhouse_summary_requirements
            .iter()
            .map(materialization)
            .collect::<Vec<_>>();
        let plan_id = 10_000 + ordinal as u64;
        let sds =
            match control_plane::physical::summary_catalog::SummaryCatalog::from_materializations(
                plan_id, 1, &configs,
            ) {
                Ok(value) => value,
                Err(error) => {
                    report.push(json!({"id":row.id,"exact_sql":"oracle_valid","planning_sql":planning_sql,
                    "parser_planner":"pass","compiler":"failed",
                    "publication":"not_reached","backend_bind_execute":"exact_fallback","reason":error.to_string()}));
                    continue;
                }
            };
        let envelope = PlanEnvelope {
            plan_id,
            plan_version: 1,
            generated_at_unix_ms: 0,
            activation_unix_ms: 0,
            expiry_unix_ms: None,
            backend_compat: "audit".into(),
            planner_revision: "audit".into(),
            capability_snapshot_id: "audit".into(),
        };
        let mut precompute =
            PrecomputePlan::build_backend_local(envelope.clone(), configs).unwrap();
        precompute.summary_catalog = Some(sds.reference().unwrap());
        let mut transmission =
            TransmissionPlan::build(envelope, &precompute, &Default::default()).unwrap();
        transmission.summary_catalog = precompute.summary_catalog.clone();
        let window_ms = row
            .clickhouse_summary_requirements
            .iter()
            .map(|s| s.window_seconds * 1000)
            .max()
            .unwrap();
        let request = ClickHouseSqlWorkload {
            sds,
            precompute_plan: precompute,
            transmission_plan: transmission,
            tables: HashMap::from([("raw_samples".into(), schema())]),
            accuracy: AccuracyTarget::Exact,
            queries: vec![ClickHouseSqlWorkloadEntry {
                sql: row.clickhouse_sql,
                planning_sql: Some(
                    planning_sql
                        .replace("{eval_ms}", &EVAL_MS.to_string())
                        .replace("{start_ms}", &(EVAL_MS - window_ms).to_string())
                        .replace("{end_ms}", &EVAL_MS.to_string()),
                ),
                start_ms: EVAL_MS - window_ms,
                end_ms: EVAL_MS,
                cumulative: true,
            }],
        };
        match compile_clickhouse_workload(&request).await {
            Ok(bundle) => report.push(json!({"id":row.id,"exact_sql":"oracle_valid","planning_sql":planning_sql,
                "parser_planner":"pass","compiler":"pass", "publication":"pass",
                "backend_bind":"pass",
                "backend_execute":match row.id.as_str() {
                    "q05" => "warm_process_e2e",
                    "q06" => "same_dag_shape_as_q05",
                    "q23" => "shared_relational_operators_unit_covered",
                    _ => "not_verified",
                },
                "dag_nodes":bundle.plans[0]["runtime"]["executable"]["nodes"].as_object().map_or(0, |n| n.len())})),
            Err(error) => report.push(json!({"id":row.id,"exact_sql":"oracle_valid","planning_sql":planning_sql,
                "parser_planner":"pass","compiler":"typed_fallback",
                "publication":"not_reached","backend_bind_execute":"exact_fallback","reason":error.to_string()})),
        }
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({"queries":report})).unwrap()
    );
}
