//! ClickHouse SQL planning entry point.
//!
//! ASAPPlanner owns SQL parsing and canonicalization. This module only joins
//! that frontend to the same post-ASAP physical mapping used by PromQL.

use asap_frontend_sql::{lower_sql_dialect, SqlCatalog};
use planner_types::pre_asap::QueryExpr;
use planner_types::types::AccuracyTarget;
use planner_types::workload::SqlDialect;

use crate::physical::post_asap::{cost_model::ControlPlaneCostModel, PhysicalExpr};
use crate::query_plan::{FallbackPolicy, InstantExecution, MaterializationBinding, QueryPlanEntry};
use asap_types::summary_catalog::SummaryCatalog;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};

#[derive(Debug, thiserror::Error)]
pub enum ClickHousePlanningError {
    #[error("SQL lowering failed: {0}")]
    Lower(String),
    #[error("physical mapping failed: {0}")]
    Bind(#[from] crate::planner_selection::SelectionError),
}

pub struct ClickHousePlannedQuery {
    pub canonical: QueryExpr,
    pub physical: PhysicalExpr,
}

pub async fn plan_clickhouse_sql(
    sql: &str,
    catalog: &SqlCatalog,
    accuracy: AccuracyTarget,
) -> Result<ClickHousePlannedQuery, ClickHousePlanningError> {
    let canonical = lower_sql_dialect(sql, catalog, SqlDialect::ClickhouseSQL, accuracy.clone())
        .await
        .map_err(|error| ClickHousePlanningError::Lower(error.to_string()))?;
    // SQL keeps relational parents such as Project and Filter above a
    // summary-capable Aggregate. Use ASAPPlanner's recursive selector here;
    // the PromQL deployment lowering retains its existing conservative rules.
    let cost_model = ControlPlaneCostModel::new(accuracy);
    let selected = crate::planner_selection::select_summary(&canonical, &cost_model)?;
    let physical = PhysicalExpr::committed(selected);
    Ok(ClickHousePlannedQuery {
        canonical,
        physical,
    })
}

pub use asap_frontend_sql::SqlCatalog as ClickHouseSqlCatalog;

#[derive(Debug, Deserialize)]
pub struct ClickHouseSqlWorkload {
    pub backend_endpoint: String,
    pub bearer_token: Option<String>,
    pub sds: SummaryCatalog,
    pub tables: HashMap<String, planner_types::pre_asap::Schema>,
    pub accuracy: AccuracyTarget,
    pub queries: Vec<ClickHouseSqlWorkloadEntry>,
}

#[derive(Debug, Deserialize)]
pub struct ClickHouseSqlWorkloadEntry {
    pub sql: String,
    pub start_ms: u64,
    pub end_ms: u64,
    pub cumulative: bool,
    pub binding: MaterializationBinding,
}

/// Wire bundle consumed by the independent backend SQL catalog.
#[derive(Debug, Serialize)]
pub struct ClickHouseCompiledBundle {
    pub sds: SummaryCatalog,
    pub tables: HashMap<String, planner_types::pre_asap::Schema>,
    pub accuracy: AccuracyTarget,
    pub plans: Vec<serde_json::Value>,
}

pub async fn compile_clickhouse_workload(
    request: &ClickHouseSqlWorkload,
) -> Result<ClickHouseCompiledBundle, ClickHousePlanningError> {
    let catalog = SqlCatalog {
        tables: request.tables.clone(),
    };
    let mut plans = Vec::with_capacity(request.queries.len());
    for (index, query) in request.queries.iter().enumerate() {
        let planned = plan_clickhouse_sql(&query.sql, &catalog, request.accuracy.clone()).await?;
        let PhysicalExpr::Committed(crate::physical::post_asap::PostAsapPlan::Summary(root)) =
            planned.physical
        else {
            return Err(ClickHousePlanningError::Lower(
                "SQL did not produce a summary DAG".into(),
            ));
        };
        let executable = QueryPlanEntry::compile_bound(
            format!("clickhouse-sql-{index}"),
            query.sql.trim().to_owned(),
            &root,
            InstantExecution {
                lookback_ms: query.end_ms.saturating_sub(query.start_ms),
                full_history: query.start_ms == 0,
                cumulative_readout: query.cumulative,
            },
            FallbackPolicy::ExactBackend,
            |_, _| Ok(query.binding.clone()),
        )
        .map_err(|error| ClickHousePlanningError::Lower(error.to_string()))?;
        if executable
            .nodes
            .values()
            .any(|node| matches!(node, crate::query_plan::QueryPlanNode::ExactFallback { .. }))
        {
            return Err(ClickHousePlanningError::Lower(
                "compiled SQL contains an unsupported operator; publication refused".into(),
            ));
        }
        let identity = request
            .sds
            .materializations
            .get(&query.binding.materialization)
            .ok_or_else(|| {
                ClickHousePlanningError::Lower("SQL binding is absent from SDS".into())
            })?;
        plans.push(serde_json::json!({
            "sql": query.sql,
            "runtime": { "start_ms": query.start_ms, "end_ms": query.end_ms,
                "cumulative": query.cumulative,
                "materializations": BTreeSet::from([query.binding.materialization.fingerprint()]),
                "executable": executable },
            "descriptors": { "summaries": [identity.summary_descriptor_id.clone()],
                "data": [identity.data_descriptor_id.clone()] }
        }));
    }
    Ok(ClickHouseCompiledBundle {
        sds: request.sds.clone(),
        tables: request.tables.clone(),
        accuracy: request.accuracy.clone(),
        plans,
    })
}
