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
    ClickHousePlanningContext, FallbackPolicy, FixedEvaluationRange, InstantExecution,
    MaterializationBinding, PhysicalGrouping, QueryLanguage, QueryPlan, QueryPlanEntry,
};
use asap_types::summary_catalog::SummaryCatalog;
use serde::Deserialize;
use std::collections::HashMap;
use std::rc::Rc;

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
    let cost_model = ControlPlaneCostModel::new(accuracy.clone());
    let selected = crate::planner_selection::select_workload(
        vec![(0, Rc::new(canonical.clone()))],
        accuracy,
        &cost_model,
    )?
    .into_iter()
    .next()
    .map(|(_, node)| node)
    .ok_or_else(|| {
        crate::planner_selection::SelectionError::Workload(
            "SQL workload search returned no root".into(),
        )
    })?;
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

pub async fn compile_clickhouse_workload(
    request: &ClickHouseSqlWorkload,
) -> Result<crate::physical::publication::PhysicalPlanPublication, ClickHousePlanningError> {
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
    let mut entries = std::collections::BTreeMap::new();
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
            query.sql.clone(),
            planned.canonical_sql.clone(),
            &root,
            FixedEvaluationRange {
                start_ms: query.start_ms,
                end_ms: query.end_ms,
                cumulative: query.cumulative,
            },
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
        // Descriptor references are already represented by each DAG's
        // MaterializationBinding and validated through SummaryCatalog.
        let _descriptor_ids = identities
            .iter()
            .map(|identity| {
                (
                    &identity.summary_descriptor_id,
                    &identity.data_descriptor_id,
                )
            })
            .collect::<Vec<_>>();
        let identity = QueryPlan::catalog_key(QueryLanguage::ClickHouseSql, &planned.canonical_sql);
        if entries.insert(identity.clone(), executable).is_some() {
            return Err(ClickHousePlanningError::Lower(format!(
                "duplicate canonical SQL query identity `{identity}`"
            )));
        }
    }
    let publication = crate::physical::publication::PhysicalPlanPublication {
        summary_catalog: request.sds.clone(),
        precompute_plan: request.precompute_plan.clone(),
        collector_plans: Vec::new(),
        transmission_plan: request.transmission_plan.clone(),
        query_plan: QueryPlan {
            plan_id: request.sds.plan_id,
            plan_version: request.sds.plan_version,
            clickhouse_context: Some(ClickHousePlanningContext {
                tables: request.tables.clone(),
                accuracy: request.accuracy.clone(),
            }),
            entries,
        },
    };
    publication
        .validate()
        .map_err(ClickHousePlanningError::Lower)?;
    Ok(publication)
}

fn bind_selected_node(
    node: &planner_types::post_asap::SummaryNode,
    family: &planner_types::post_asap::SummaryFamilyType,
    query: &ClickHouseSqlWorkloadEntry,
    request: &ClickHouseSqlWorkload,
) -> Result<MaterializationBinding, crate::query_plan::QueryPlanError> {
    let (table_ref, value_column, source_window, spatial_filter) = clickhouse_materialization_leaf_contract(node)
        .map_err(crate::query_plan::QueryPlanError::Invalid)?;
    let expected = crate::physical::compiler::physical_materialization_family(family);
    let selected = select_materialization(
        &request.precompute_plan.materializations,
        &table_ref,
        &value_column,
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
        item_labels: selected.aggregated_labels.labels.clone(),
    })
}

fn clickhouse_materialization_leaf_contract(
    node: &planner_types::post_asap::SummaryNode,
) -> Result<(String, String, Option<u64>, String), String> {
    use planner_types::{
        post_asap::SummaryExpr,
        pre_asap::{CompareOpKind, QueryExpr, ScalarValue, Source},
    };
    let SummaryExpr::SummaryAgg { child, input, .. } = &node.expr else {
        return Err("SQL materialization leaf is not a summary aggregate".into());
    };
    let SummaryExpr::KeepPreAsap(expr) = &child.expr else {
        return Err("SQL materialization leaf has no tabular source".into());
    };
    fn table_scan(
        expr: &QueryExpr,
    ) -> Option<(
        &str,
        &[planner_types::pre_asap::Predicate],
        &planner_types::pre_asap::Schema,
    )> {
        match expr {
            QueryExpr::Project { child, .. } => table_scan(child),
            QueryExpr::Scan {
                source: Source::Table { table_ref },
                predicates,
                schema,
            } => Some((table_ref, predicates, schema)),
            _ => None,
        }
    }
    let (source, explicit_window) = match expr.as_ref() {
        QueryExpr::TimeRange { child, range }
            if range.as_millis() > 0 && range.as_millis() % 1_000 == 0 =>
        {
            (child.as_ref(), Some(range.as_secs()))
        }
        source => (source, None),
    };
    let Some((table_ref, predicates, schema)) = table_scan(source) else {
        return Err("SQL materialization leaf has no table scan".into());
    };
    let planner_types::post_asap::SummaryInputExpr::Column(value_input) = &input.weight else {
        return Err("SQL summary requires a column-valued update".into());
    };
    let value_column = match value_input {
        planner_types::pre_asap::ColumnRef::Named(name)
        | planner_types::pre_asap::ColumnRef::Qualified { name, .. } => name.clone(),
        _ => return Err("SQL summary requires a named value column".into()),
    };

    fn comparisons<'a>(expr: &'a QueryExpr, out: &mut Vec<&'a QueryExpr>) {
        if let QueryExpr::BoolAnd(children) = expr {
            for child in children {
                comparisons(child, out);
            }
        } else {
            out.push(expr);
        }
    }
    let mut lower_ms = None;
    let mut upper_ms = None;
    let mut leaves = Vec::new();
    for predicate in predicates {
        comparisons(&predicate.0, &mut leaves);
    }
    for leaf in leaves {
        let QueryExpr::Compare { left, op, right } = leaf else {
            return Err("SQL materialization predicate is not a comparison".into());
        };
        let QueryExpr::Column(column) = left.as_ref() else {
            return Err("SQL materialization predicate must reference a column".into());
        };
        let name = schema
            .columns
            .get(*column)
            .map(|column| column.name.as_str())
            .ok_or_else(|| {
                "SQL materialization predicate references an unknown column".to_string()
            })?;
        match (name, op, right.as_ref()) {
            (
                name,
                CompareOpKind::Gt | CompareOpKind::Ge,
                QueryExpr::Literal(ScalarValue::Int64(value)),
            ) if schema
                .time_index
                .is_some_and(|index| schema.columns[index].name == name) =>
            {
                lower_ms = Some(*value);
            }
            (
                name,
                CompareOpKind::Lt | CompareOpKind::Le,
                QueryExpr::Literal(ScalarValue::Int64(value)),
            ) if schema
                .time_index
                .is_some_and(|index| schema.columns[index].name == name) =>
            {
                upper_ms = Some(*value);
            }
            _ => {
                return Err(format!("SQL population predicate on {name} needs a canonical catalog filter"))
            }
        }
    }
    let inferred_window = match (lower_ms, upper_ms) {
        (Some(lower), Some(upper)) if upper > lower && (upper - lower) % 1_000 == 0 => {
            Some((upper - lower) as u64 / 1_000)
        }
        (None, None) => None,
        _ => {
            return Err("SQL timestamp range must provide compatible lower and upper bounds".into())
        }
    };
    let window_secs = explicit_window.or(inferred_window).ok_or_else(|| {
        "SQL table summary requires a positive whole-second timestamp range".to_string()
    })?;
    Ok((table_ref.to_owned(), value_column, Some(window_secs), String::new()))
}

fn select_materialization<'a>(
    materializations: &'a [asap_types::PrecomputeMaterialization],
    table_ref: &str,
    value_column: &str,
    spatial_filter: &str,
    expected: &planner_types::post_asap::SummaryFamilyType,
    semantic_window_seconds: u64,
) -> Result<&'a asap_types::PrecomputeMaterialization, crate::query_plan::QueryPlanError> {
    let mut matches = materializations.iter().filter(|candidate| {
        candidate.table_name.as_deref() == Some(table_ref)
            && candidate.value_column.as_deref() == Some(value_column)
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
            "no precompute materialization matches {table_ref}.{value_column}/{expected:?}"
        ))
    })?;
    if matches.next().is_some() {
        return Err(crate::query_plan::QueryPlanError::Invalid(format!(
            "ambiguous precompute materializations match {table_ref}.{value_column}/{expected:?}"
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
        value_column: &str,
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
            format!("telemetry.{value_column}"),
            None,
            Some("telemetry".into()),
            Some(value_column.into()),
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
            select_materialization(&configs, "telemetry", "requests", "", &sum_family, 60)
                .unwrap()
                .policy_fingerprint(),
            sum_60.policy_fingerprint()
        );
        let dd_family = dd_2.accumulator_spec().unwrap().family;
        assert_eq!(
            select_materialization(&configs, "telemetry", "requests", "", &dd_family, 60)
                .unwrap()
                .policy_fingerprint(),
            dd_2.policy_fingerprint()
        );
        assert_eq!(
            select_materialization(&configs, "telemetry", "requests", "", &count_family, 60)
                .unwrap()
                .policy_fingerprint(),
            count_60.policy_fingerprint()
        );
        assert!(select_materialization(&configs, "telemetry", "missing", "", &sum_family, 60).is_err());
        let mut ambiguous = configs.clone();
        ambiguous.push(sum_60);
        assert!(
            select_materialization(&ambiguous, "telemetry", "requests", "", &sum_family, 60)
                .unwrap_err()
                .to_string()
                .contains("ambiguous")
        );
    }
}
