use axum::{
    body::Bytes,
    http::{HeaderMap, Method},
};
use control_plane::{
    physical::compiler::{
        PlanEnvelope, PrecomputePlan, TransmissionPlan, BACKEND_COMPAT, PLANNER_REVISION,
    },
    query_plan::{ClickHousePlanningContext, QueryPlan},
};
use data_plane::{
    drivers::query::servers::http::{build_active_physical_plan, PhysicalPlanInstallRequest},
    query_engines::asap_clickhouse_query_engine::{
        accelerator::CatalogClickHouseAccelerator, ClickHouseAccelerationOutcome,
        ClickHouseAccelerator,
    },
    storage_engines::{
        sketch_db::index::SketchStore,
        types::{BackendStorageRouting, HotReloadActivePhysicalPlan},
    },
};
use planner_types::{
    pre_asap::{Column, DataType, Schema},
    types::AccuracyTarget,
};
use serde::Deserialize;
use serde_json::json;
use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::PathBuf,
    sync::Arc,
};

#[derive(Deserialize)]
struct Corpus {
    queries: Vec<Row>,
}

#[derive(Deserialize)]
struct Row {
    id: String,
    clickhouse_sql: Option<String>,
}

#[tokio::main]
async fn main() {
    let path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .expect("usage: audit_clickhouse_fallback CORPUS.json");
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
    let catalog =
        asap_types::summary_catalog::SummaryCatalog::from_materializations(27, 1, &[]).unwrap();
    let reference = catalog.reference().unwrap();
    let mut precompute_plan =
        PrecomputePlan::build_backend_local(envelope.clone(), vec![]).unwrap();
    precompute_plan.summary_catalog = Some(reference.clone());
    let mut transmission_plan =
        TransmissionPlan::build(envelope, &precompute_plan, &BTreeMap::new()).unwrap();
    transmission_plan.summary_catalog = Some(reference);
    let query_plan = QueryPlan {
        plan_id: 27,
        plan_version: 1,
        clickhouse_context: Some(ClickHousePlanningContext {
            tables: HashMap::from([("raw_samples".into(), schema)]),
            accuracy: AccuracyTarget::Epsilon(0.01),
        }),
        entries: BTreeMap::new(),
    };
    let active = build_active_physical_plan(
        PhysicalPlanInstallRequest {
            summary_catalog: catalog,
            collector_plans: vec![],
            precompute_plan,
            transmission_plan,
            query_plan,
            storage_routing: None,
            adaptation_evidence: vec![],
        },
        Arc::new(BackendStorageRouting::empty()),
    )
    .unwrap();
    let accelerator = CatalogClickHouseAccelerator::with_active_physical_plan(
        Arc::new(SketchStore::new()),
        HotReloadActivePhysicalPlan::new(active),
    );
    let mut rows = Vec::new();
    for row in corpus.queries {
        let sql = row
            .clickhouse_sql
            .unwrap_or_default()
            .replace("{eval_ms}", "1788891296000");
        let request = data_plane::query_engines::asap_clickhouse_query_engine::request::ClickHouseQueryRequest {
            method: Method::GET,
            sql,
            body: Bytes::new(),
            parameters: BTreeMap::new(),
            headers: HeaderMap::new(),
        };
        let outcome = accelerator.execute(&request).await;
        rows.push(match outcome {
            ClickHouseAccelerationOutcome::Fallback(reason) => json!({
                "id": row.id,
                "route": "exact_fallback",
                "reason": format!("{reason:?}"),
            }),
            ClickHouseAccelerationOutcome::Accelerated(_) => json!({
                "id": row.id,
                "route": "warm",
            }),
        });
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({"queries": rows})).unwrap()
    );
}
