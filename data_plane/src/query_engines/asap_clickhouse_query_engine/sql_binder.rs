//! ClickHouse request binding through ASAPPlanner's SQL frontend.

use control_plane::clickhouse::{
    plan_clickhouse_sql, ClickHousePlannedQuery, ClickHousePlanningError, ClickHouseSqlCatalog,
};
use planner_types::types::AccuracyTarget;

#[derive(Clone)]
pub struct ClickHouseSqlBinder {
    catalog: ClickHouseSqlCatalog,
    accuracy: AccuracyTarget,
}

impl ClickHouseSqlBinder {
    pub fn new(catalog: ClickHouseSqlCatalog, accuracy: AccuracyTarget) -> Self {
        Self { catalog, accuracy }
    }

    /// Parse SQL to the canonical `QueryExpr` and invoke the same physical
    /// mapping used by PromQL. Summary selection remains planner-owned.
    pub async fn bind(&self, sql: &str) -> Result<ClickHousePlannedQuery, ClickHousePlanningError> {
        plan_clickhouse_sql(sql, &self.catalog, self.accuracy.clone()).await
    }

    pub async fn canonical_identity(&self, sql: &str) -> Result<String, ClickHousePlanningError> {
        control_plane::clickhouse::canonicalize_clickhouse_sql(
            sql,
            &self.catalog,
            self.accuracy.clone(),
        )
        .await
    }
}
