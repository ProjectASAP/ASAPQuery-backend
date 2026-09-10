//! ClickHouse SQL planning entry point.
//!
//! ASAPPlanner owns SQL parsing and canonicalization. This module only joins
//! that frontend to the same post-ASAP physical mapping used by PromQL.

use asap_frontend_sql::{lower_sql_dialect, SqlCatalog};
use planner_types::pre_asap::QueryExpr;
use planner_types::types::AccuracyTarget;
use planner_types::workload::SqlDialect;

use crate::physical::post_asap::{cost_model::ControlPlaneCostModel, PhysicalExpr};

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
