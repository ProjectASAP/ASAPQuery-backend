//! ClickHouse SQL planning entry point.
//!
//! ASAPPlanner owns SQL parsing and canonicalization. This module only joins
//! that frontend to the same post-ASAP physical mapping used by PromQL.

use asap_frontend_sql::{lower_sql_dialect, SqlCatalog};
use planner_types::pre_asap::QueryExpr;
use planner_types::types::AccuracyTarget;
use planner_types::workload::SqlDialect;

use crate::physical::compiler::{PrecomputePlan, TransmissionPlan};
use crate::physical::post_asap::{cost_model::ControlPlaneCostModel, PhysicalExpr};
use crate::query_plan::{
    FallbackPolicy, InstantExecution, MaterializationBinding, PhysicalGrouping, QueryPlanEntry,
};
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
    pub canonical_sql: String,
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
        canonical_sql: canonical_sql_identity(&canonical),
        canonical,
        physical,
    })
}

pub async fn canonicalize_clickhouse_sql(
    sql: &str,
    catalog: &SqlCatalog,
    accuracy: AccuracyTarget,
) -> Result<String, ClickHousePlanningError> {
    let canonical = lower_sql_dialect(sql, catalog, SqlDialect::ClickhouseSQL, accuracy)
        .await
        .map_err(|error| ClickHousePlanningError::Lower(error.to_string()))?;
    Ok(canonical_sql_identity(&canonical))
}

/// Identity derived from ASAPPlanner's resolved canonical AST. Equivalent SQL
/// formatting therefore maps to one catalog key without reparsing at serving.
pub fn canonical_sql_identity(canonical: &QueryExpr) -> String {
    format!("{canonical:?}")
}

pub use asap_frontend_sql::SqlCatalog as ClickHouseSqlCatalog;

#[derive(Debug, Deserialize)]
pub struct ClickHouseSqlWorkload {
    pub sds: SummaryCatalog,
    pub precompute_plan: PrecomputePlan,
    pub transmission_plan: TransmissionPlan,
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
}

/// Wire bundle consumed by the independent backend SQL catalog.
#[derive(Debug, Serialize)]
pub struct ClickHouseCompiledBundle {
    pub sds: SummaryCatalog,
    pub tables: HashMap<String, planner_types::pre_asap::Schema>,
    pub accuracy: AccuracyTarget,
    pub plans: Vec<serde_json::Value>,
    pub precompute_plan: PrecomputePlan,
    pub transmission_plan: TransmissionPlan,
}

pub async fn compile_clickhouse_workload(
    request: &ClickHouseSqlWorkload,
) -> Result<ClickHouseCompiledBundle, ClickHousePlanningError> {
    request
        .precompute_plan
        .validate_against_catalog(&request.sds)
        .map_err(|error| ClickHousePlanningError::Lower(error.to_string()))?;
    request
        .transmission_plan
        .validate(&request.precompute_plan)
        .map_err(|error| ClickHousePlanningError::Lower(error.to_string()))?;
    let catalog = SqlCatalog {
        tables: request.tables.clone(),
    };
    let mut plans = Vec::with_capacity(request.queries.len());
    for query in &request.queries {
        let planned = plan_clickhouse_sql(&query.sql, &catalog, request.accuracy.clone()).await?;
        let PhysicalExpr::Committed(crate::physical::post_asap::PostAsapPlan::Summary(root)) =
            planned.physical
        else {
            return Err(ClickHousePlanningError::Lower(
                "SQL did not produce a summary DAG".into(),
            ));
        };
        let executable = QueryPlanEntry::compile_bound_relational(
            &root,
            InstantExecution {
                lookback_ms: query.end_ms.saturating_sub(query.start_ms),
                full_history: query.start_ms == 0,
                cumulative_readout: query.cumulative,
            },
            FallbackPolicy::ExactBackend,
            |node, family| bind_selected_node(node, family, query, request),
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
        let bindings = executable.materialization_bindings();
        let materializations = bindings
            .iter()
            .map(|binding| binding.materialization.fingerprint())
            .collect::<BTreeSet<_>>();
        let identities = bindings
            .iter()
            .map(|binding| {
                request
                    .sds
                    .materializations
                    .get(&binding.materialization)
                    .ok_or_else(|| {
                        ClickHousePlanningError::Lower(
                            "compiled SQL binding is absent from SDS".into(),
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        plans.push(serde_json::json!({
            "sql": planned.canonical_sql,
            "runtime": { "start_ms": query.start_ms, "end_ms": query.end_ms,
                "cumulative": query.cumulative,
                "materializations": materializations,
                "executable": executable },
            "descriptors": { "summaries": identities.iter().map(|identity| identity.summary_descriptor_id.clone()).collect::<BTreeSet<_>>(),
                "data": identities.iter().map(|identity| identity.data_descriptor_id.clone()).collect::<BTreeSet<_>>() }
        }));
    }
    Ok(ClickHouseCompiledBundle {
        sds: request.sds.clone(),
        tables: request.tables.clone(),
        accuracy: request.accuracy.clone(),
        plans,
        precompute_plan: request.precompute_plan.clone(),
        transmission_plan: request.transmission_plan.clone(),
    })
}

fn bind_selected_node(
    node: &planner_types::post_asap::SummaryNode,
    family: &planner_types::post_asap::SummaryFamilyType,
    query: &ClickHouseSqlWorkloadEntry,
    request: &ClickHouseSqlWorkload,
) -> Result<MaterializationBinding, crate::query_plan::QueryPlanError> {
    let (metric, source_window, spatial_filter) =
        crate::physical::compiler::materialization_leaf_contract(node)
            .map_err(crate::query_plan::QueryPlanError::Invalid)?;
    let expected = crate::physical::compiler::physical_materialization_family(family);
    let selected = select_materialization(
        &request.precompute_plan.materializations,
        &metric,
        &spatial_filter,
        &expected,
        source_window.unwrap_or((query.end_ms.saturating_sub(query.start_ms)) / 1000),
    )?;
    Ok(MaterializationBinding {
        materialization: selected.policy_fingerprint().into(),
        output_grouping: PhysicalGrouping::Reduce(selected.grouping_labels.labels.clone()),
        window_ms: selected.slide_interval.saturating_mul(1000),
        pane_origin_ms: selected.pane_origin_ms,
        readout_lookback_ms: source_window.map(|seconds| seconds.saturating_mul(1000)),
    })
}

fn select_materialization<'a>(
    materializations: &'a [asap_types::PrecomputeMaterialization],
    metric: &str,
    spatial_filter: &str,
    expected: &planner_types::post_asap::SummaryFamilyType,
    semantic_window_seconds: u64,
) -> Result<&'a asap_types::PrecomputeMaterialization, crate::query_plan::QueryPlanError> {
    let mut matches = materializations.iter().filter(|candidate| {
        candidate.metric == metric
            && candidate.spatial_filter_normalized == spatial_filter
            && candidate
                .accumulator_spec()
                .ok()
                .is_some_and(|spec| spec.family == *expected)
            && semantic_window_seconds
                .checked_mul(1000)
                .is_some_and(|window| window % candidate.window_size.saturating_mul(1000) == 0)
    });
    let selected = matches.next().ok_or_else(|| {
        crate::query_plan::QueryPlanError::Invalid(format!(
            "no precompute materialization matches {metric}/{expected:?}"
        ))
    })?;
    if matches.next().is_some() {
        return Err(crate::query_plan::QueryPlanError::Invalid(format!(
            "ambiguous precompute materializations match {metric}/{expected:?}"
        )));
    }
    Ok(selected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_types::{AggregationType, KeyByLabelNames, PrecomputeMaterialization, WindowKind};

    fn materialization(
        agg: AggregationType,
        metric: &str,
        window: u64,
        slide: u64,
        parameter: (&str, serde_json::Value),
    ) -> PrecomputeMaterialization {
        let mut value = PrecomputeMaterialization::new(
            agg,
            String::new(),
            std::collections::HashMap::from([(parameter.0.into(), parameter.1)]),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            window,
            slide,
            WindowKind::Tumbling,
            String::new(),
            metric.into(),
            None,
            None,
            None,
        );
        value.pane_origin_ms = Some(0);
        value
    }

    #[test]
    fn selected_nodes_bind_unique_family_parameters_source_and_window() {
        let sum_60 = materialization(
            AggregationType::Sum,
            "requests",
            60,
            10,
            ("variant", serde_json::json!(1)),
        );
        let count_60 = materialization(
            AggregationType::MinMax,
            "requests",
            60,
            10,
            ("variant", serde_json::json!(2)),
        );
        let sum_300 = materialization(
            AggregationType::Sum,
            "requests",
            300,
            30,
            ("variant", serde_json::json!(3)),
        );
        let other = materialization(
            AggregationType::Sum,
            "latency",
            60,
            10,
            ("variant", serde_json::json!(1)),
        );
        let dd_2 = materialization(
            AggregationType::DDSketch,
            "requests",
            60,
            10,
            ("relativeAccuracy", serde_json::json!(0.02)),
        );
        let dd_5 = materialization(
            AggregationType::DDSketch,
            "requests",
            60,
            10,
            ("relativeAccuracy", serde_json::json!(0.05)),
        );
        let configs = vec![
            sum_60.clone(),
            count_60.clone(),
            sum_300,
            other,
            dd_2.clone(),
            dd_5,
        ];
        let sum_family = sum_60.accumulator_spec().unwrap().family;
        let count_family = count_60.accumulator_spec().unwrap().family;
        assert_eq!(
            select_materialization(&configs, "requests", "", &sum_family, 60)
                .unwrap()
                .policy_fingerprint(),
            sum_60.policy_fingerprint()
        );
        let dd_family = dd_2.accumulator_spec().unwrap().family;
        assert_eq!(
            select_materialization(&configs, "requests", "", &dd_family, 60)
                .unwrap()
                .policy_fingerprint(),
            dd_2.policy_fingerprint()
        );
        assert_eq!(
            select_materialization(&configs, "requests", "", &count_family, 60)
                .unwrap()
                .policy_fingerprint(),
            count_60.policy_fingerprint()
        );
        assert!(select_materialization(&configs, "missing", "", &sum_family, 60).is_err());
        let mut ambiguous = configs.clone();
        ambiguous.push(sum_60);
        assert!(
            select_materialization(&ambiguous, "requests", "", &sum_family, 60)
                .unwrap_err()
                .to_string()
                .contains("ambiguous")
        );
    }
}
