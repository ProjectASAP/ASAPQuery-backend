//! ClickHouse SQL planning entry point.
//!
//! ASAPPlanner owns SQL parsing and canonicalization. This module only joins
//! that frontend to the same post-ASAP physical mapping used by PromQL.

use asap_frontend_sql::{lower_sql_dialect, SqlCatalog};
use planner_types::pre_asap::QueryExpr;
use planner_types::types::AccuracyTarget;
use planner_types::workload::SqlDialect;

use crate::physical::post_asap::{bind_query_expr, BindingError, PhysicalExpr};

#[derive(Debug, thiserror::Error)]
pub enum ClickHousePlanningError {
    #[error("SQL lowering failed: {0}")]
    Lower(String),
    #[error("physical mapping failed: {0}")]
    Bind(#[from] BindingError),
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
    let physical = bind_query_expr(&canonical, accuracy)?;
    Ok(ClickHousePlannedQuery {
        canonical,
        physical,
    })
}

pub use asap_frontend_sql::SqlCatalog as ClickHouseSqlCatalog;
