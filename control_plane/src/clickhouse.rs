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
    // SQL commonly keeps relational operators (Project/Filter/Sort/Limit)
    // above the summary-capable aggregate. Workload search materializes those
    // residual parents around the selected inner summary; root-only candidate
    // selection incorrectly rejects such queries before recursive mapping.
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

/// Stable identity of the SQL text accepted by the ClickHouse endpoint.
///
/// This intentionally does not parse the request. ClickHouse's executable SQL
/// surface is larger than ASAPPlanner's planning surface (tuple fields, array
/// lambdas, and engine-specific functions are common examples). Publication
/// may bind such an exact request template to a separately supplied,
/// semantically equivalent planning SQL expression.
pub fn sql_request_template_identity(sql: &str) -> String {
    sql.trim().trim_end_matches(';').trim_end().to_owned()
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
    /// Exact SQL template received by the data plane and sent to ClickHouse on
    /// fallback.
    pub sql: String,
    /// Semantically equivalent SQL written against Planner's supported SQL
    /// surface. When absent, `sql` is planned directly.
    ///
    /// The control plane treats this as an explicit workload contract; it
    /// never guesses equivalence by simplifying engine-specific expressions.
    #[serde(default)]
    pub planning_sql: Option<String>,
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
        let planned = plan_clickhouse_sql(
            query.planning_sql.as_deref().unwrap_or(&query.sql),
            &catalog,
            request.accuracy.clone(),
        )
        .await?;
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
            "sql": sql_request_template_identity(&query.sql),
            "planning_sql": planned.canonical_sql,
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
    let (metric, source_window, spatial_filter) = clickhouse_materialization_leaf_contract(node)
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
        item_labels: Default::default(),
    })
}

/// Resolve the table-shaped SQL source without teaching the shared PromQL
/// materialization contract about SQL's metric and timestamp columns.
fn clickhouse_materialization_leaf_contract(
    node: &planner_types::post_asap::SummaryNode,
) -> Result<(String, Option<u64>, String), String> {
    use planner_types::{
        post_asap::SummaryExpr,
        pre_asap::{CompareOpKind, QueryExpr, ScalarValue, Source},
    };
    let SummaryExpr::SummaryAgg { child, .. } = &node.expr else {
        return crate::physical::compiler::materialization_leaf_contract(node);
    };
    let SummaryExpr::KeepPreAsap(expr) = &child.expr else {
        return crate::physical::compiler::materialization_leaf_contract(node);
    };
    fn table_scan(
        expr: &QueryExpr,
    ) -> Option<(
        &[planner_types::pre_asap::Predicate],
        &planner_types::pre_asap::Schema,
    )> {
        match expr {
            QueryExpr::Project { child, .. } => table_scan(child),
            QueryExpr::Scan {
                source: Source::Table { .. },
                predicates,
                schema,
            } => Some((predicates, schema)),
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
    let Some((predicates, schema)) = table_scan(source) else {
        return crate::physical::compiler::materialization_leaf_contract(node);
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
    let mut metric = None;
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
            ("metric", CompareOpKind::Eq, QueryExpr::Literal(ScalarValue::Utf8(value))) => {
                metric = Some(value.clone());
            }
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
                return Err(format!(
                    "unsupported SQL materialization predicate on {name}"
                ))
            }
        }
    }
    let metric = metric.ok_or_else(|| "SQL table summary requires metric='...'".to_string())?;
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
    Ok((metric, Some(window_secs), String::new()))
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
mod planning_tests {
    use super::*;

    #[test]
    fn request_identity_accepts_clickhouse_only_syntax_without_parsing() {
        let sql = "SELECT samples[1].1, arraySum(i -> samples[i].2, range(1, 3)) FROM raw";
        assert_eq!(sql_request_template_identity(&format!("  {sql};  ")), sql);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_types::{AggregationType, KeyByLabelNames, PrecomputeMaterialization, WindowKind};
    use planner_types::pre_asap::{Column, DataType, Schema};

    #[tokio::test]
    async fn relational_sql_root_recursively_selects_inner_summary() {
        let schema = Schema::with_time_index(
            vec![
                Column::new("metric", DataType::Utf8, false),
                Column::new("labels", DataType::Utf8, false),
                Column::new("ts_ms", DataType::Timestamp, false),
                Column::new("value", DataType::Float64, false),
            ],
            2,
            vec![vec![1]],
        );
        let catalog = SqlCatalog::new().with_table("raw_samples", schema);
        let planned = plan_clickhouse_sql(
            "SELECT labels, max(value) AS value FROM raw_samples \
             WHERE metric='cache_refresh_lag_seconds' \
             AND ts_ms>1788848096000 AND ts_ms<=1788891296000 \
             GROUP BY labels ORDER BY labels",
            &catalog,
            AccuracyTarget::Exact,
        )
        .await
        .expect("relational parents must be retained around the selected aggregate");
        assert!(matches!(planned.physical, PhysicalExpr::Committed(_)));
    }

    #[tokio::test]
    async fn temporal_sql_reuses_existing_rate_summary_intent() {
        let schema = Schema::with_time_index(
            vec![
                Column::new("metric", DataType::Utf8, false),
                Column::new("labels", DataType::Utf8, false),
                Column::new("ts_ms", DataType::Timestamp, false),
                Column::new("value", DataType::Float64, false),
            ],
            2,
            vec![vec![1, 2]],
        );
        let catalog = SqlCatalog::new().with_table("raw_samples", schema);
        let planned = plan_clickhouse_sql(
            "SELECT labels, asap_rate(value, ts_ms, 300000) AS value \
             FROM raw_samples WHERE metric='requests_total' GROUP BY labels",
            &catalog,
            AccuracyTarget::Exact,
        )
        .await
        .expect("explicit temporal SQL must use the shared rate DAG");
        let PhysicalExpr::Committed(crate::physical::post_asap::PostAsapPlan::Summary(root)) =
            planned.physical
        else {
            panic!("rate SQL did not produce a summary DAG")
        };
        let mut aggregate = root.as_ref();
        while let planner_types::post_asap::SummaryExpr::ValueOperation { child, .. } =
            &aggregate.expr
        {
            aggregate = child;
        }
        assert_eq!(
            clickhouse_materialization_leaf_contract(aggregate).unwrap(),
            ("requests_total".into(), Some(300), String::new())
        );

        let mut config = PrecomputeMaterialization::new(
            AggregationType::Increase,
            String::new(),
            Default::default(),
            KeyByLabelNames::new(vec!["labels".into()]),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            300,
            300,
            WindowKind::Tumbling,
            String::new(),
            "requests_total".into(),
            None,
            Some("raw_samples".into()),
            Some("value".into()),
        );
        config.pane_origin_ms = Some(0);
        let sds = SummaryCatalog::from_materializations(74, 1, &[config.clone()]).unwrap();
        let envelope = crate::physical::compiler::PlanEnvelope {
            plan_id: 74,
            plan_version: 1,
            generated_at_unix_ms: 0,
            activation_unix_ms: 0,
            expiry_unix_ms: None,
            backend_compat: "test".into(),
            planner_revision: "test".into(),
            capability_snapshot_id: "test".into(),
        };
        let mut precompute =
            PrecomputePlan::build_backend_local(envelope.clone(), vec![config]).unwrap();
        precompute.summary_catalog = Some(sds.reference().unwrap());
        let mut transmission =
            TransmissionPlan::build(envelope, &precompute, &Default::default()).unwrap();
        transmission.summary_catalog = precompute.summary_catalog.clone();
        let bundle = compile_clickhouse_workload(&ClickHouseSqlWorkload {
            sds,
            precompute_plan: precompute,
            transmission_plan: transmission,
            tables: HashMap::from([("raw_samples".into(), catalog.tables["raw_samples"].clone())]),
            accuracy: AccuracyTarget::Exact,
            queries: vec![ClickHouseSqlWorkloadEntry {
                sql: "SELECT labels, asap_rate(value, ts_ms, 300000) AS value FROM raw_samples WHERE metric='requests_total' GROUP BY labels".into(),
                planning_sql: None,
                start_ms: 0,
                end_ms: 300_000,
                cumulative: true,
            }],
        })
        .await
        .expect("rate SQL must compile into a publishable shared DAG");
        assert_eq!(bundle.plans.len(), 1);
    }

    #[tokio::test]
    async fn ratio_sql_compiles_two_rate_summaries_and_relational_join() {
        let schema = Schema::with_time_index(
            vec![
                Column::new("metric", DataType::Utf8, false),
                Column::new("labels", DataType::Utf8, false),
                Column::new("ts_ms", DataType::Timestamp, false),
                Column::new("value", DataType::Float64, false),
            ],
            2,
            vec![vec![2, 1]],
        );
        let configs = ["errors_total", "requests_total"].map(|metric| {
            let mut config = PrecomputeMaterialization::new(
                AggregationType::Increase,
                String::new(),
                Default::default(),
                KeyByLabelNames::new(vec!["labels".into()]),
                KeyByLabelNames::empty(),
                KeyByLabelNames::empty(),
                String::new(),
                300,
                300,
                WindowKind::Tumbling,
                String::new(),
                metric.into(),
                None,
                Some("raw_samples".into()),
                Some("value".into()),
            );
            config.pane_origin_ms = Some(0);
            config
        });
        let sds = SummaryCatalog::from_materializations(75, 1, &configs).unwrap();
        let envelope = crate::physical::compiler::PlanEnvelope {
            plan_id: 75,
            plan_version: 1,
            generated_at_unix_ms: 0,
            activation_unix_ms: 0,
            expiry_unix_ms: None,
            backend_compat: "test".into(),
            planner_revision: "test".into(),
            capability_snapshot_id: "test".into(),
        };
        let mut precompute =
            PrecomputePlan::build_backend_local(envelope.clone(), configs.to_vec()).unwrap();
        precompute.summary_catalog = Some(sds.reference().unwrap());
        let mut transmission =
            TransmissionPlan::build(envelope, &precompute, &Default::default()).unwrap();
        transmission.summary_catalog = precompute.summary_catalog.clone();
        let sql = "SELECT a.labels, a.v / b.v AS ratio FROM \
            (SELECT labels, asap_rate(value, ts_ms, 300000) AS v FROM raw_samples WHERE metric='errors_total' GROUP BY labels) a \
            INNER JOIN \
            (SELECT labels, asap_rate(value, ts_ms, 300000) AS v FROM raw_samples WHERE metric='requests_total' GROUP BY labels) b \
            ON a.labels=b.labels";
        let bundle = compile_clickhouse_workload(&ClickHouseSqlWorkload {
            sds,
            precompute_plan: precompute,
            transmission_plan: transmission,
            tables: HashMap::from([("raw_samples".into(), schema)]),
            accuracy: AccuracyTarget::Exact,
            queries: vec![ClickHouseSqlWorkloadEntry {
                sql: sql.into(),
                start_ms: 0,
                end_ms: 300_000,
                cumulative: true,
            }],
        })
        .await
        .expect("ratio SQL must compile");
        let plan: crate::query_plan::ExecutableQueryPlan =
            serde_json::from_value(bundle.plans[0]["runtime"]["executable"].clone()).unwrap();
        assert!(plan.nodes.values().any(|node| matches!(
            node,
            crate::query_plan::QueryPlanNode::RelationalJoin { .. }
        )));
        assert_eq!(plan.materialization_bindings().len(), 2);
    }

    #[tokio::test]
    async fn q05_max_over_time_sql_compiles_to_publishable_summary_dag() {
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
        let mut config = PrecomputeMaterialization::new(
            AggregationType::MinMax,
            "max".into(),
            Default::default(),
            KeyByLabelNames::new(vec!["labels".into()]),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            43_200,
            60,
            WindowKind::Tumbling,
            String::new(),
            "cache_refresh_lag_seconds".into(),
            None,
            None,
            None,
        );
        config.pane_origin_ms = Some(0);
        let sds = SummaryCatalog::from_materializations(71, 1, &[config.clone()]).unwrap();
        let envelope = crate::physical::compiler::PlanEnvelope {
            plan_id: 71,
            plan_version: 1,
            generated_at_unix_ms: 0,
            activation_unix_ms: 0,
            expiry_unix_ms: None,
            backend_compat: "test".into(),
            planner_revision: "test".into(),
            capability_snapshot_id: "test".into(),
        };
        let mut precompute_plan =
            PrecomputePlan::build_backend_local(envelope.clone(), vec![config]).unwrap();
        precompute_plan.summary_catalog = Some(sds.reference().unwrap());
        let mut transmission_plan =
            TransmissionPlan::build(envelope, &precompute_plan, &Default::default()).unwrap();
        transmission_plan.summary_catalog = precompute_plan.summary_catalog.clone();
        let request = ClickHouseSqlWorkload {
            sds,
            precompute_plan,
            transmission_plan,
            tables: HashMap::from([("raw_samples".into(), schema)]),
            accuracy: AccuracyTarget::Exact,
            queries: vec![ClickHouseSqlWorkloadEntry {
                sql: "SELECT labels, max(value) AS value FROM raw_samples WHERE metric='cache_refresh_lag_seconds' AND ts_ms>1788848096000 AND ts_ms<=1788891296000 GROUP BY labels ORDER BY labels".into(),
                planning_sql: None,
                start_ms: 1_788_848_096_000,
                end_ms: 1_788_891_296_000,
                cumulative: true,
            }],
        };
        let bundle = compile_clickhouse_workload(&request)
            .await
            .expect("q05 SQL must compile for atomic sidecar publication");
        assert_eq!(bundle.plans.len(), 1);
    }

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
