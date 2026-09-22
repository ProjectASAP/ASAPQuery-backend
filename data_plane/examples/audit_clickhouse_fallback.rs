use axum::{
    body::Bytes,
    http::{HeaderMap, Method},
};
use control_plane::{
    physical::compiler::{PlanEnvelope, PrecomputePlan, BACKEND_COMPAT, PLANNER_REVISION},
    query_plan::{ClickHousePlanningContext, QueryPlan},
};
use data_plane::{
    drivers::query::servers::http::{validate_and_build_runtime_plan, PhysicalPlanInstallRequest},
    query_engines::asap_clickhouse_query_engine::{
        accelerator::CatalogClickHouseAccelerator, ClickHouseAccelerationOutcome,
        ClickHouseAccelerator, ClickHouseHttpFallback, ClickHouseHttpServer,
    },
    storage_engines::{
        sketch_db::index::SketchStore,
        types::{ActivePhysicalPlanHandle, BackendStorageRouting},
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
    let mut transmission_plan = control_plane::physical::compiler::build_transmission_plan(
        envelope,
        &precompute_plan,
        &BTreeMap::new(),
    )
    .unwrap();
    transmission_plan.summary_catalog = Some(reference);
    let query_plan = QueryPlan {
        plan_id: 27,
        plan_version: 1,
        clickhouse_context: Some(ClickHousePlanningContext {
            window_templates: Default::default(),
            tables: HashMap::from([("raw_samples".into(), schema)]),
            accuracy: AccuracyTarget::Epsilon(0.01),
        }),
        selected_dags: Default::default(),
        entries: BTreeMap::new(),
    };
    let active = validate_and_build_runtime_plan(
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
    let exact_url =
        std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://127.0.0.1:18123".into());
    let exact = Arc::new(ClickHouseHttpFallback::new(
        exact_url.clone(),
        std::env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| "default".into()),
    ));
    let accelerator = Arc::new(CatalogClickHouseAccelerator::with_active_physical_plan(
        Arc::new(SketchStore::new()),
        ActivePhysicalPlanHandle::new(active),
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = ClickHouseHttpServer::router_with_accelerator(exact, accelerator.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await });
    let client = reqwest::Client::new();
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
        let acceleration = accelerator.execute(&request).await;
        let mut outbound = client
            .post(format!("http://{address}/"))
            .body(request.sql.clone());
        if let Ok(user) = std::env::var("CLICKHOUSE_USER") {
            outbound = outbound.header("x-clickhouse-user", user);
        }
        if let Ok(password) = std::env::var("CLICKHOUSE_PASSWORD") {
            outbound = outbound.header("x-clickhouse-key", password);
        }
        let response = outbound.send().await.unwrap();
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let body = response.text().await.unwrap();
        let mut direct = client.post(&exact_url).body(request.sql.clone());
        if let Ok(user) = std::env::var("CLICKHOUSE_USER") {
            direct = direct.header("x-clickhouse-user", user);
        }
        if let Ok(password) = std::env::var("CLICKHOUSE_PASSWORD") {
            direct = direct.header("x-clickhouse-key", password);
        }
        let direct = direct.send().await.unwrap();
        let direct_status = direct.status().as_u16();
        let direct_body = direct.text().await.unwrap();
        let matches_direct_exact = status == direct_status && body == direct_body;
        rows.push(match acceleration {
            ClickHouseAccelerationOutcome::Fallback(reason) => json!({
                "id": row.id,
                "fallback_requested": true,
                "acceleration_reason": format!("{reason:?}"),
                "exact_executed": status != 502,
                "exact_success": (200..300).contains(&status) && matches_direct_exact,
                "exact_status": status,
                "direct_exact_status": direct_status,
                "matches_direct_exact": matches_direct_exact,
                "clickhouse_summary": headers.get("x-clickhouse-summary").and_then(|v| v.to_str().ok()),
                "clickhouse_exception_code": headers.get("x-clickhouse-exception-code").and_then(|v| v.to_str().ok()),
                "result": body,
            }),
            ClickHouseAccelerationOutcome::Accelerated(_) => json!({
                "id": row.id,
                "fallback_requested": false,
                "exact_executed": false,
                "exact_success": false,
                "exact_status": null,
                "direct_exact_status": direct_status,
                "matches_direct_exact": false,
                "result": body,
            }),
        });
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({"queries": rows})).unwrap()
    );
    server.abort();
}
