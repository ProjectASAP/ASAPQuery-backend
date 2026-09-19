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
    pub selection_trace: serde_json::Value,
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
    let (selected, selection_trace) =
        crate::planner_selection::select_workload_with_accuracy_model_and_trace(
            vec![(0, Rc::new(canonical.clone()))],
            accuracy,
            &cost_model,
            &asap_aware_mapping::NoAccuracyEvidence,
            &asap_aware_mapping::DefaultAccuracyModel,
        )?;
    let selected = selected
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
        selection_trace,
    })
}

#[cfg(test)]
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

/// Resolve a request once, retaining the fixed identity for older publications.
pub async fn bind_clickhouse_sql(
    sql: &str,
    catalog: &SqlCatalog,
    accuracy: AccuracyTarget,
) -> Result<(String, String, Option<(u64, u64)>), ClickHousePlanningError> {
    let canonical = lower_sql_dialect(sql, catalog, SqlDialect::ClickhouseSQL, accuracy)
        .await
        .map_err(|error| ClickHousePlanningError::Lower(error.to_string()))?;
    let fixed = format!("{canonical:?}");
    let binding = moving_window(&canonical);
    Ok(match binding {
        Some((identity, range)) => (fixed, identity, Some(range)),
        None => (fixed.clone(), fixed, None),
    })
}

/// Only the source bounds of a single-table aggregate are runtime parameters.
/// Expressions in projections, aggregate arguments and other predicates stay
/// in the identity, so changing them cannot reuse the installed computation.
pub fn canonical_sql_identity(canonical: &QueryExpr) -> String {
    moving_window(canonical)
        .map(|(identity, _)| identity)
        .unwrap_or_else(|| format!("{canonical:?}"))
}

fn moving_window(canonical: &QueryExpr) -> Option<(String, (u64, u64))> {
    use planner_types::pre_asap::{CompareOpKind, ScalarValue, Source};
    let mut template = canonical.clone();
    let aggregate = match &mut template {
        QueryExpr::Project { child, .. } => Rc::make_mut(child),
        expr => expr,
    };
    let QueryExpr::Aggregate { child, .. } = aggregate else {
        return None;
    };
    let QueryExpr::Scan {
        source: Source::Table { .. },
        predicates,
        schema,
    } = Rc::make_mut(child)
    else {
        return None;
    };
    let time = schema.time_index?;
    fn bounds(
        expr: &QueryExpr,
        time: usize,
        lower: &mut Option<i64>,
        upper: &mut Option<i64>,
    ) -> Option<()> {
        match expr {
            QueryExpr::BoolAnd(children) => {
                for child in children {
                    bounds(child, time, lower, upper)?;
                }
            }
            QueryExpr::Compare { left, op, right } if matches!(left.as_ref(), QueryExpr::Column(col) if *col == time) =>
            {
                let value = constant_int64(right)?;
                let (target, value) = match op {
                    CompareOpKind::Ge => (lower, value),
                    CompareOpKind::Gt => (lower, value.checked_add(1)?),
                    CompareOpKind::Lt => (upper, value),
                    CompareOpKind::Le => (upper, value.checked_add(1)?),
                    _ => return None,
                };
                // Redundant bounds are deliberately not generalized.
                if target.replace(value).is_some() {
                    return None;
                }
            }
            _ => {}
        }
        Some(())
    }
    let (mut lower, mut upper) = (None, None);
    for predicate in predicates.iter() {
        bounds(&predicate.0, time, &mut lower, &mut upper)?;
    }
    let (start, end) = (u64::try_from(lower?).ok()?, u64::try_from(upper?).ok()?);
    if start >= end {
        return None;
    }
    fn normalize(expr: &mut QueryExpr, time: usize, width: i64) {
        match expr {
            QueryExpr::BoolAnd(children) => {
                for child in children {
                    normalize(child, time, width);
                }
            }
            QueryExpr::Compare { left, op, right } if matches!(left.as_ref(), QueryExpr::Column(col) if *col == time) =>
            {
                // `bounds` has checked the integer expressions and overflow.
                // Preserve membership by converting both edges to [start,end).
                let value = match op {
                    CompareOpKind::Ge | CompareOpKind::Gt => {
                        *op = CompareOpKind::Ge;
                        0
                    }
                    CompareOpKind::Lt | CompareOpKind::Le => {
                        *op = CompareOpKind::Lt;
                        width
                    }
                    _ => return,
                };
                *right = Rc::new(QueryExpr::Literal(ScalarValue::Int64(value)));
            }
            _ => {}
        }
    }
    for predicate in predicates {
        normalize(Rc::make_mut(&mut predicate.0), time, (end - start) as i64);
    }
    Some((format!("moving-window-v1:{template:?}"), (start, end)))
}

pub use asap_frontend_sql::SqlCatalog as ClickHouseSqlCatalog;

#[derive(Debug, Deserialize)]
pub struct ClickHouseSqlWorkload {
    #[serde(rename = "sds", alias = "summary_catalog")]
    pub summary_catalog: SummaryCatalog,
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

/// Workload input without predeclared summary families or materialization IDs.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClickHouseSqlAutomaticWorkload {
    pub envelope: asap_types::precompute_plan::PlanEnvelope,
    pub tables: HashMap<String, planner_types::pre_asap::Schema>,
    pub accuracy: AccuracyTarget,
    pub queries: Vec<ClickHouseSqlWorkloadEntry>,
}

pub async fn compile_automatic_clickhouse_workload(
    request: &ClickHouseSqlAutomaticWorkload,
) -> Result<
    (
        crate::physical::publication::PhysicalPlanPublication,
        std::collections::BTreeMap<String, serde_json::Value>,
    ),
    ClickHousePlanningError,
> {
    let catalog = SqlCatalog {
        tables: request.tables.clone(),
    };
    let mut entries = std::collections::BTreeMap::new();
    let mut window_templates = std::collections::BTreeMap::<String, Vec<String>>::new();
    let mut installed_dags = std::collections::BTreeMap::new();
    let mut selected_dags = std::collections::BTreeMap::new();
    let mut materializations = std::collections::BTreeMap::new();
    let mut selection_traces = std::collections::BTreeMap::new();
    for query in &request.queries {
        validate_sql_evaluation(query)?;
        // Selection runs once. Compilation installs only SummaryAgg nodes
        // actually visited in this selected DAG, never a scripted family.
        let mut planned =
            plan_clickhouse_sql(&query.sql, &catalog, request.accuracy.clone()).await?;
        let selection_trace = std::mem::take(&mut planned.selection_trace);
        let template = planned.canonical_sql.clone();
        let (entry, installed) = compile_selected_sql(query, planned, |node, family| {
            let config = materialize_selected_sql(node, family, query)
                .map_err(crate::query_plan::QueryPlanError::Invalid)?;
            let binding = MaterializationBinding {
                full_window_slide_ms: matches!(
                    config.window_layout,
                    asap_types::WindowMaterializationLayout::FullWindow
                )
                .then_some(config.slide_interval.saturating_mul(1_000)),
                materialization: config.policy_fingerprint().into(),
                output_grouping: PhysicalGrouping::Reduce(config.grouping_labels.names()),
                window_ms: config.stored_window_ms(),
                pane_origin_ms: config.pane_origin_ms,
                readout_lookback_ms: Some(query.end_ms - query.start_ms),
                item_labels: config.aggregated_labels.labels.clone(),
            };
            materializations
                .entry(config.policy_fingerprint())
                .or_insert(config);
            Ok(binding)
        })?;
        index_sql_template(&mut window_templates, template, &entry);
        selection_traces.insert(entry.canonical_query.clone(), selection_trace);
        let key = QueryPlan::catalog_key(QueryLanguage::ClickHouseSql, &entry.canonical_query);
        if entries.insert(key, entry).is_some() {
            return Err(ClickHousePlanningError::Lower(
                "duplicate canonical SQL query identity".into(),
            ));
        }
        selected_dags.insert(query.sql.clone(), installed.document.clone());
        installed_dags.insert(
            query.sql.clone(),
            installed
                .maintenance_projection()
                .map_err(ClickHousePlanningError::Lower)?,
        );
    }
    let configs: Vec<_> = materializations.into_values().collect();
    let sds = SummaryCatalog::from_materializations(
        request.envelope.plan_id,
        request.envelope.plan_version,
        &configs,
    )
    .map_err(|error| ClickHousePlanningError::Lower(error.to_string()))?;
    let mut precompute = PrecomputePlan::build_backend_local(request.envelope.clone(), configs)
        .map_err(|error| ClickHousePlanningError::Lower(error.to_string()))?;
    precompute.summary_catalog = Some(
        sds.reference()
            .map_err(|error| ClickHousePlanningError::Lower(error.to_string()))?,
    );
    precompute.executable_dags = installed_dags;
    let mut transmission = crate::physical::compiler::build_transmission_plan(
        request.envelope.clone(),
        &precompute,
        &std::collections::BTreeMap::new(),
    )
    .map_err(|error| ClickHousePlanningError::Lower(error.to_string()))?;
    transmission.summary_catalog = precompute.summary_catalog.clone();
    let publication = crate::physical::publication::PhysicalPlanPublication {
        summary_catalog: sds,
        precompute_plan: precompute,
        collector_plans: Vec::new(),
        transmission_plan: transmission,
        query_plan: QueryPlan {
            plan_id: request.envelope.plan_id,
            plan_version: request.envelope.plan_version,
            clickhouse_context: Some(ClickHousePlanningContext {
                window_templates,
                tables: request.tables.clone(),
                accuracy: request.accuracy.clone(),
            }),
            selected_dags,
            entries,
        },
    };
    publication
        .validate()
        .map_err(ClickHousePlanningError::Lower)?;
    Ok((publication, selection_traces))
}

fn materialize_selected_sql(
    node: &planner_types::post_asap::SummaryNode,
    family: &planner_types::post_asap::SummaryFamilyType,
    query: &ClickHouseSqlWorkloadEntry,
) -> Result<asap_types::PrecomputeMaterialization, String> {
    use crate::physical::backend_stage::{AggregationInput, BackendAggregation};
    use planner_types::{post_asap::SummaryExpr, pre_asap::Reduction};
    let SummaryExpr::SummaryAgg {
        reduction: Reduction::Reduce(keys),
        child,
        ..
    } = &node.expr
    else {
        return Err("SQL materialization requires a supported reduction".into());
    };
    if keys.is_without() {
        return Err("SQL grouping exclusion requires a resolved projection".into());
    }
    let SummaryExpr::KeepPreAsap(source) = &child.expr else {
        return Err("SQL grouping requires a typed source subtree".into());
    };
    let source_schema = source.output_schema().map_err(|error| error.to_string())?;
    let mut columns = Vec::new();
    for key in keys.keys() {
        let mut column = source_schema
            .columns
            .get(*key)
            .cloned()
            .ok_or("SQL grouping column is absent from source schema")?;
        column.table = None;
        columns.push(column);
    }
    let grouping = asap_types::GroupingProjection::new(columns);
    grouping.validate_table_group_codec()?;
    let leaf = clickhouse_materialization_leaf_contract(node, query.start_ms, query.end_ms)?;
    let ClickHouseMaterializationLeaf {
        table,
        value,
        value_source_column,
        window_secs: window,
        population,
        timestamp_column: timestamp,
    } = leaf;
    let window_secs = window.ok_or("SQL materialization requires a bounded window")?;
    let aggregation = BackendAggregation {
        aggregation_id: String::new(),
        metric_name: format!("{table}.{}", value.column().unwrap_or("constant")),
        family: crate::physical::compiler::physical_materialization_family(family),
        window_secs,
        spatial_filter: String::new(),
        grouping: grouping.names(),
        item_label: None,
        heap_update_mode: None,
        aggregation_input: AggregationInput::Raw,
    };
    let mut config = crate::physical::compiler::aggregation_config_for_materialization(
        &aggregation,
        asap_types::QueryLanguage::ClickHouseSql,
    )
    .map_err(|error| error.to_string())?;
    config.table_name = Some(table);
    config.grouping_labels = grouping;
    config.value_projection = Some(value);
    config.table_timestamp_column = Some(timestamp);
    config.table_population = Some(population);
    config.value_source_column = value_source_column;
    config.partitioning = Some(asap_types::sds::PopulationPartitioning::Grouped);
    config.pane_origin_ms = Some(
        i64::try_from(query.start_ms)
            .map_err(|_| "SQL evaluation timestamp exceeds runtime range")?,
    );
    config.num_aggregates_to_retain = Some(2);
    Ok(config)
}

pub async fn compile_clickhouse_workload(
    request: &ClickHouseSqlWorkload,
) -> Result<crate::physical::publication::PhysicalPlanPublication, ClickHousePlanningError> {
    request
        .precompute_plan
        .validate_against_catalog(&request.summary_catalog)
        .map_err(|error| ClickHousePlanningError::Lower(error.to_string()))?;
    request
        .transmission_plan
        .validate(&request.precompute_plan)
        .map_err(|error| ClickHousePlanningError::Lower(error.to_string()))?;
    let catalog = SqlCatalog {
        tables: request.tables.clone(),
    };
    let mut entries = std::collections::BTreeMap::new();
    let mut window_templates = std::collections::BTreeMap::<String, Vec<String>>::new();
    let mut installed_dags = std::collections::BTreeMap::new();
    let mut selected_dags = std::collections::BTreeMap::new();
    for query in &request.queries {
        let planned = plan_clickhouse_sql(&query.sql, &catalog, request.accuracy.clone()).await?;
        let template = planned.canonical_sql.clone();
        let (executable, installed) = compile_selected_sql(query, planned, |node, family| {
            bind_selected_node(node, family, query, request)
        })?;
        index_sql_template(&mut window_templates, template, &executable);
        selected_dags.insert(query.sql.clone(), installed.document.clone());
        installed_dags.insert(
            query.sql.clone(),
            installed
                .maintenance_projection()
                .map_err(ClickHousePlanningError::Lower)?,
        );
        let identity =
            QueryPlan::catalog_key(QueryLanguage::ClickHouseSql, &executable.canonical_query);
        if entries.insert(identity.clone(), executable).is_some() {
            return Err(ClickHousePlanningError::Lower(format!(
                "duplicate canonical SQL query identity `{identity}`"
            )));
        }
    }
    let mut precompute_plan = request.precompute_plan.clone();
    precompute_plan.executable_dags = installed_dags;
    let publication = crate::physical::publication::PhysicalPlanPublication {
        summary_catalog: request.summary_catalog.clone(),
        precompute_plan,
        collector_plans: Vec::new(),
        transmission_plan: request.transmission_plan.clone(),
        query_plan: QueryPlan {
            plan_id: request.summary_catalog.plan_id,
            plan_version: request.summary_catalog.plan_version,
            clickhouse_context: Some(ClickHousePlanningContext {
                window_templates,
                tables: request.tables.clone(),
                accuracy: request.accuracy.clone(),
            }),
            selected_dags,
            entries,
        },
    };
    publication
        .validate()
        .map_err(ClickHousePlanningError::Lower)?;
    Ok(publication)
}

fn index_sql_template(
    templates: &mut std::collections::BTreeMap<String, Vec<String>>,
    template: String,
    entry: &QueryPlanEntry,
) {
    // External subqueries still contain fixed SQL literals.
    if template.starts_with("moving-window-v1:")
        && !entry
            .nodes
            .values()
            .any(|node| matches!(node, crate::query_plan::QueryPlanNode::ExternalExact { .. }))
    {
        templates
            .entry(template)
            .or_default()
            .push(entry.canonical_query.clone());
    }
}

fn compile_selected_sql<F>(
    query: &ClickHouseSqlWorkloadEntry,
    planned: ClickHousePlannedQuery,
    mut bind: F,
) -> Result<
    (
        QueryPlanEntry,
        crate::physical::executable_binding::InstalledPostAsapDag,
    ),
    ClickHousePlanningError,
>
where
    F: FnMut(
        &Rc<planner_types::post_asap::SummaryNode>,
        &planner_types::post_asap::SummaryFamilyType,
    ) -> Result<MaterializationBinding, crate::query_plan::QueryPlanError>,
{
    validate_sql_evaluation(query)?;
    let PhysicalExpr::Committed(crate::physical::post_asap::PostAsapPlan::Summary(root)) =
        planned.physical
    else {
        return Err(ClickHousePlanningError::Lower(
            "SQL did not produce a summary DAG".into(),
        ));
    };
    let semantic = planner_types::post_asap::compile_executable_dag_with_node_ids(&root)
        .map_err(|error| ClickHousePlanningError::Lower(error.to_string()))?;
    let mut materialization_nodes = std::collections::BTreeMap::new();
    let mut query_nodes = std::collections::BTreeMap::new();
    let executable = crate::query_plan::compile_bound_relational_mapped(
        query.sql.clone(),
        format!("{:?}", planned.canonical),
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
        |node, family| {
            let binding = bind(node, family)?;
            let id = semantic.node_ids.node_id(node).ok_or_else(|| {
                crate::query_plan::QueryPlanError::Invalid(
                    "selected SQL node is absent from semantic DAG".into(),
                )
            })?;
            materialization_nodes.insert(id, binding.materialization);
            Ok(binding)
        },
        |node, query_node| {
            if let Some(id) = semantic.node_ids.node_id(node) {
                query_nodes.insert(id, query_node);
            }
        },
    )
    .map_err(|error| ClickHousePlanningError::Lower(error.to_string()))?;
    let installed = crate::physical::executable_binding::install_selected_dag(
        query.sql.clone(),
        &semantic.dag,
        executable.root,
        |id| materialization_nodes.get(&id).copied(),
        |id| query_nodes.get(&id).copied(),
    )
    .map_err(ClickHousePlanningError::Lower)?;
    crate::physical::executable_binding::validate_query_plan(&installed, &executable)
        .map_err(ClickHousePlanningError::Lower)?;
    if executable
        .nodes
        .values()
        .any(|node| matches!(node, crate::query_plan::QueryPlanNode::ExactFallback { .. }))
    {
        return Err(ClickHousePlanningError::Lower(
            "compiled SQL contains an unsupported operator; publication refused".into(),
        ));
    }
    Ok((executable, installed))
}

fn validate_sql_evaluation(
    query: &ClickHouseSqlWorkloadEntry,
) -> Result<(), ClickHousePlanningError> {
    if query.start_ms >= query.end_ms || query.end_ms > i64::MAX as u64 {
        return Err(ClickHousePlanningError::Lower(
            "SQL evaluation requires start_ms < end_ms within signed Unix milliseconds".into(),
        ));
    }
    Ok(())
}

fn bind_selected_node(
    node: &planner_types::post_asap::SummaryNode,
    family: &planner_types::post_asap::SummaryFamilyType,
    query: &ClickHouseSqlWorkloadEntry,
    request: &ClickHouseSqlWorkload,
) -> Result<MaterializationBinding, crate::query_plan::QueryPlanError> {
    let ClickHouseMaterializationLeaf {
        table: table_ref,
        value: value_column,
        window_secs: source_window,
        population: spatial_filter,
        timestamp_column,
        ..
    } = clickhouse_materialization_leaf_contract(node, query.start_ms, query.end_ms)
        .map_err(crate::query_plan::QueryPlanError::Invalid)?;
    let expected = crate::physical::compiler::physical_materialization_family(family);
    let selected = select_materialization(
        &request.precompute_plan.materializations,
        &table_ref,
        &value_column,
        &spatial_filter.canonical(),
        &expected,
        source_window.unwrap_or((query.end_ms.saturating_sub(query.start_ms)) / 1000),
    )?;
    if selected.table_timestamp_column.as_deref() != Some(timestamp_column.as_str()) {
        return Err(crate::query_plan::QueryPlanError::Invalid(
            "SQL timestamp projection differs from the installed materialization".into(),
        ));
    }
    Ok(MaterializationBinding {
        full_window_slide_ms: matches!(
            selected.window_layout,
            asap_types::WindowMaterializationLayout::FullWindow
        )
        .then_some(selected.slide_interval.saturating_mul(1_000)),
        materialization: selected.policy_fingerprint().into(),
        output_grouping: PhysicalGrouping::Reduce(selected.grouping_labels.names()),
        window_ms: selected.stored_window_ms(),
        pane_origin_ms: selected.pane_origin_ms,
        readout_lookback_ms: source_window.map(|seconds| seconds.saturating_mul(1000)),
        item_labels: selected.aggregated_labels.labels.clone(),
    })
}

/// Evaluate only exact integer constant arithmetic at the installation boundary.
/// No SQL text rewrite, floating coercion, or runtime-column evaluation is allowed.
fn constant_int64(expr: &QueryExpr) -> Option<i64> {
    use planner_types::pre_asap::{ArithmeticOpKind, ScalarValue};
    match expr {
        QueryExpr::Literal(ScalarValue::Int64(value)) => Some(*value),
        QueryExpr::Arithmetic { op, left, right } => {
            let left = constant_int64(left)?;
            let right = constant_int64(right)?;
            match op {
                ArithmeticOpKind::Add => left.checked_add(right),
                ArithmeticOpKind::Sub => left.checked_sub(right),
                ArithmeticOpKind::Mul => left.checked_mul(right),
                _ => None,
            }
        }
        _ => None,
    }
}

/// The table leaf a SQL summary materialization is admitted against.
#[derive(Debug)]
struct ClickHouseMaterializationLeaf {
    table: String,
    value: asap_types::sds::ValueProjectionIdentity,
    /// Producer typing for a column projection: what the ingest path must know
    /// to read the column safely (integer exactness, NULL skipping). `None`
    /// for constant projections, which carry their own literal.
    value_source_column: Option<planner_types::pre_asap::Column>,
    window_secs: Option<u64>,
    population: asap_types::table_population::TablePopulation,
    timestamp_column: String,
}

fn clickhouse_materialization_leaf_contract(
    node: &planner_types::post_asap::SummaryNode,
    evaluation_start_ms: u64,
    evaluation_end_ms: u64,
) -> Result<ClickHouseMaterializationLeaf, String> {
    use planner_types::{
        post_asap::SummaryExpr,
        pre_asap::{CompareOpKind, QueryExpr, ScalarValue, Source},
    };
    let SummaryExpr::SummaryAgg {
        child,
        input,
        family,
        ..
    } = &node.expr
    else {
        return Err("SQL materialization leaf is not a summary aggregate".into());
    };
    if input.item.is_some() {
        return Err("SQL item/weight summary inputs require an explicit item projection".into());
    }
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
    use asap_types::sds::ValueProjectionIdentity;
    use planner_types::post_asap::SummaryInputExpr;
    let mut value_source_column = None;
    let value_projection = match &input.weight {
        SummaryInputExpr::Column(
            planner_types::pre_asap::ColumnRef::Wildcard
            | planner_types::pre_asap::ColumnRef::SampleValue,
        ) if matches!(
            family,
            planner_types::post_asap::SummaryFamilyType::ExactAggregate(
                planner_types::post_asap::ExactKind::Count,
                _
            )
        ) =>
        {
            ValueProjectionIdentity::Constant {
                value: ScalarValue::Int64(1),
            }
        }
        SummaryInputExpr::Column(
            planner_types::pre_asap::ColumnRef::Named(name)
            | planner_types::pre_asap::ColumnRef::Qualified { name, .. },
        ) => {
            let column = schema
                .columns
                .iter()
                .find(|column| column.name == *name)
                .ok_or("SQL summary value projection is not a source column")?;
            // Numeric source columns are admitted with their declared type and
            // nullability, which the ingest reader honours: integers get an
            // exactness guard on the way into f64 summary state, and NULL rows
            // are skipped the way a SQL aggregate skips them. Non-numeric
            // columns have no value semantics to summarise and stay refused.
            if !matches!(
                column.dtype,
                planner_types::pre_asap::DataType::Float64
                    | planner_types::pre_asap::DataType::Int64
            ) {
                return Err(format!(
                    "SQL value readout requires a numeric source column; `{name}` is {:?}",
                    column.dtype
                ));
            }
            let mut source_column = column.clone();
            source_column.table = None;
            value_source_column = Some(source_column);
            ValueProjectionIdentity::Column { name: name.clone() }
        }
        SummaryInputExpr::Constant(value) if value.is_finite() => {
            let value = if value.fract() == 0.0 && value.abs() <= (1_u64 << 53) as f64 {
                ScalarValue::Int64(*value as i64)
            } else {
                ScalarValue::Float64(*value)
            };
            ValueProjectionIdentity::Constant { value }
        }
        _ => return Err("SQL summary requires a named column or finite numeric update".into()),
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
    let mut population = asap_types::table_population::TablePopulation::default();
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
        let folded =
            constant_int64(right).map(|value| QueryExpr::Literal(ScalarValue::Int64(value)));
        let right = folded.as_ref().unwrap_or(right.as_ref());
        match (name, op, right) {
            (
                name,
                CompareOpKind::Gt | CompareOpKind::Ge,
                QueryExpr::Literal(ScalarValue::Int64(value)),
            ) if schema
                .time_index
                .is_some_and(|index| schema.columns[index].name == name) =>
            {
                let bound = if matches!(op, CompareOpKind::Gt) {
                    value
                        .checked_add(1)
                        .ok_or("SQL exclusive lower timestamp overflows")?
                } else {
                    *value
                };
                lower_ms = Some(lower_ms.map_or(bound, |previous: i64| previous.max(bound)));
            }
            (
                name,
                CompareOpKind::Lt | CompareOpKind::Le,
                QueryExpr::Literal(ScalarValue::Int64(value)),
            ) if schema
                .time_index
                .is_some_and(|index| schema.columns[index].name == name) =>
            {
                let bound = if matches!(op, CompareOpKind::Le) {
                    value
                        .checked_add(1)
                        .ok_or("SQL inclusive upper timestamp overflows")?
                } else {
                    *value
                };
                upper_ms = Some(upper_ms.map_or(bound, |previous: i64| previous.min(bound)));
            }
            (_, _, QueryExpr::Literal(value))
                if !schema
                    .time_index
                    .is_some_and(|index| schema.columns[index].name == name) =>
            {
                population
                    .predicates
                    .push(asap_types::table_population::TableColumnPredicate {
                        column: name.into(),
                        operator: op.clone(),
                        value: value.clone(),
                    });
            }
            _ => {
                return Err(format!(
                    "SQL population predicate on {name} needs a canonical catalog filter"
                ))
            }
        }
    }
    population.validate()?;
    if let (Some(lower), Some(upper)) = (lower_ms, upper_ms) {
        if u64::try_from(lower).ok() != Some(evaluation_start_ms)
            || u64::try_from(upper).ok() != Some(evaluation_end_ms)
        {
            return Err(
                "SQL source timestamp bounds differ from the fixed evaluation range".into(),
            );
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
    Ok(ClickHouseMaterializationLeaf {
        table: table_ref.to_owned(),
        value: value_projection,
        value_source_column,
        window_secs: Some(window_secs),
        population,
        timestamp_column: schema
            .time_index
            .and_then(|index| schema.columns.get(index))
            .ok_or("SQL summary source has no timestamp projection")?
            .name
            .clone(),
    })
}

fn select_materialization<'a>(
    materializations: &'a [asap_types::PrecomputeMaterialization],
    table_ref: &str,
    value_projection: &asap_types::sds::ValueProjectionIdentity,
    spatial_filter: &str,
    expected: &planner_types::post_asap::SummaryFamilyType,
    semantic_window_seconds: u64,
) -> Result<&'a asap_types::PrecomputeMaterialization, crate::query_plan::QueryPlanError> {
    let mut matches = materializations.iter().filter(|candidate| {
        candidate.table_name.as_deref() == Some(table_ref)
            && candidate.effective_value_projection() == value_projection
            && candidate.population_filter_canonical().ok().as_deref() == Some(spatial_filter)
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
            "no precompute materialization matches {table_ref}/{value_projection:?}/{expected:?}"
        ))
    })?;
    if matches.next().is_some() {
        return Err(crate::query_plan::QueryPlanError::Invalid(format!(
            "ambiguous precompute materializations match {table_ref}/{value_projection:?}/{expected:?}"
        )));
    }
    Ok(selected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_types::{AggregationType, KeyByLabelNames, PrecomputeMaterialization, WindowKind};
    use planner_types::pre_asap::{Column, DataType, Schema};

    // A dashboard refresh must reuse the installed identity without treating
    // value thresholds or window length as runtime parameters.
    #[tokio::test]
    async fn moving_sql_window_reuses_identity() {
        let catalog = SqlCatalog {
            tables: HashMap::from([(
                "telemetry".into(),
                Schema {
                    columns: vec![
                        Column::new("timestamp_ms", DataType::Int64, false),
                        Column::new("value", DataType::Float64, false),
                    ],
                    time_index: Some(0),
                    ..Default::default()
                },
            )]),
        };
        let identity = |start, end, threshold| {
            format!(
            "SELECT sum(value) FROM telemetry WHERE timestamp_ms >= {start} AND timestamp_ms < {end} AND value > {threshold}"
        )
        };
        let first =
            canonicalize_clickhouse_sql(&identity(1000, 3000, 5), &catalog, AccuracyTarget::Exact)
                .await
                .unwrap();
        let shifted =
            canonicalize_clickhouse_sql(&identity(2000, 4000, 5), &catalog, AccuracyTarget::Exact)
                .await
                .unwrap();
        assert_eq!(first, shifted);
        let wider =
            canonicalize_clickhouse_sql(&identity(1000, 4000, 5), &catalog, AccuracyTarget::Exact)
                .await
                .unwrap();
        let threshold =
            canonicalize_clickhouse_sql(&identity(2000, 4000, 6), &catalog, AccuracyTarget::Exact)
                .await
                .unwrap();
        assert_ne!(first, wider);
        assert_ne!(first, threshold);
        let (_, template, range) =
            bind_clickhouse_sql(&identity(2000, 4000, 5), &catalog, AccuracyTarget::Exact)
                .await
                .unwrap();
        assert_eq!(template, first);
        assert_eq!(range, Some((2000, 4000)));

        // Dashboard bounds may use arithmetic and an inclusive upper edge.
        let (_, inclusive, range) = bind_clickhouse_sql(
            "SELECT sum(value) FROM telemetry WHERE timestamp_ms > 3999 - 2000 AND timestamp_ms <= 3999 AND value > 5",
            &catalog,
            AccuracyTarget::Exact,
        ).await.unwrap();
        assert_eq!(inclusive, first);
        assert_eq!(range, Some((2000, 4000)));

        // Projection constants are query semantics, never time parameters.
        let projected = |start, end| {
            format!("SELECT sum(value), {start} AS window_start FROM telemetry WHERE timestamp_ms >= {start} AND timestamp_ms < {end}")
        };
        assert_ne!(
            canonicalize_clickhouse_sql(&projected(1000, 3000), &catalog, AccuracyTarget::Exact)
                .await
                .unwrap(),
            canonicalize_clickhouse_sql(&projected(2000, 4000), &catalog, AccuracyTarget::Exact)
                .await
                .unwrap(),
        );
        // Unsupported bound shapes and non-aggregate scans retain fixed keys.
        for sql in [
            "SELECT sum(value) FROM telemetry",
            "SELECT sum(value) FROM telemetry WHERE timestamp_ms >= 1000 AND timestamp_ms <= 9223372036854775807",
            "SELECT value FROM telemetry WHERE timestamp_ms >= 1000 AND timestamp_ms < 3000",
        ] {
            let (fixed, template, range) =
                bind_clickhouse_sql(sql, &catalog, AccuracyTarget::Exact)
                    .await
                    .unwrap();
            assert_eq!(fixed, template);
            assert_eq!(range, None);
        }
    }

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
        value.table_timestamp_column = Some("timestamp_ms".into());
        value
    }

    /// A table leaf whose value column has the given producer typing.
    fn typed_value_leaf(
        dtype: planner_types::pre_asap::DataType,
        nullable: bool,
    ) -> planner_types::post_asap::SummaryNode {
        use planner_types::post_asap::{
            SummaryExpr, SummaryInputExpr, SummaryNode, SummarySchema, SummaryUpdate,
        };
        use planner_types::pre_asap::{
            Column, ColumnRef, CompareOpKind, DataType, Predicate, QueryExpr, Reduction,
            ScalarValue, Schema, Source,
        };
        let schema = Schema::with_time_index(
            vec![
                Column::new("timestamp_ms", DataType::Timestamp, false),
                Column::new("value", dtype, nullable),
            ],
            0,
            Vec::new(),
        );
        let bound = |op: CompareOpKind, at: i64| {
            Predicate(std::rc::Rc::new(QueryExpr::Compare {
                left: std::rc::Rc::new(QueryExpr::Column(0)),
                op,
                right: std::rc::Rc::new(QueryExpr::Literal(ScalarValue::Int64(at))),
            }))
        };
        let scan = QueryExpr::Scan {
            source: Source::Table {
                table_ref: "telemetry".into(),
            },
            predicates: vec![
                bound(CompareOpKind::Ge, 0),
                bound(CompareOpKind::Lt, 60_000),
            ],
            schema,
        };
        let family = materialization(
            AggregationType::Sum,
            "value",
            60,
            60,
            ("variant", serde_json::json!(1)),
        )
        .accumulator_spec()
        .unwrap()
        .family;
        let summary_schema = SummarySchema {
            fields: vec![],
            time_index: None,
        };
        SummaryNode {
            expr: SummaryExpr::SummaryAgg {
                child: std::rc::Rc::new(SummaryNode {
                    expr: SummaryExpr::KeepPreAsap(std::rc::Rc::new(scan)),
                    schema: summary_schema.clone(),
                    guarantee: Default::default(),
                }),
                family,
                input: SummaryUpdate {
                    item: None,
                    weight: SummaryInputExpr::Column(ColumnRef::Named("value".into())),
                    weight_domain: Default::default(),
                },
                reduction: Reduction::Reduce(vec![].into()),
                grouping: Default::default(),
            },
            schema: summary_schema,
            guarantee: Default::default(),
        }
    }

    /// Numeric source columns are admitted with their producer typing, which
    /// the ingest reader needs to read them safely. Before this the contract
    /// took non-null `Float64` only, so an ordinary nullable or integer
    /// ClickHouse column could not be automatically materialized at all.
    #[test]
    fn numeric_value_columns_are_admitted_with_their_producer_typing() {
        use planner_types::pre_asap::DataType;
        for (dtype, nullable) in [
            (DataType::Float64, false),
            (DataType::Float64, true),
            (DataType::Int64, false),
            (DataType::Int64, true),
        ] {
            let leaf = clickhouse_materialization_leaf_contract(
                &typed_value_leaf(dtype.clone(), nullable),
                0,
                60_000,
            )
            .unwrap_or_else(|error| panic!("{dtype:?}/{nullable}: {error}"));
            assert_eq!(
                leaf.value,
                asap_types::sds::ValueProjectionIdentity::Column {
                    name: "value".into()
                }
            );
            let column = leaf
                .value_source_column
                .expect("a column projection carries its producer typing");
            assert_eq!(column.dtype, dtype);
            assert_eq!(column.nullable, nullable);
            // Typing is a read concern, not an identity one: the same column
            // is the same policy however it is declared.
            assert_eq!(column.table, None);
        }
    }

    /// Non-numeric columns have no value semantics to summarise. Refusing them
    /// protects the result; it is not a gap to be widened.
    #[test]
    fn non_numeric_value_columns_stay_refused() {
        use planner_types::pre_asap::DataType;
        for dtype in [DataType::Utf8, DataType::Bool, DataType::Timestamp] {
            let error = clickhouse_materialization_leaf_contract(
                &typed_value_leaf(dtype.clone(), false),
                0,
                60_000,
            )
            .unwrap_err();
            assert!(
                error.contains("numeric source column"),
                "{dtype:?}: {error}"
            );
        }
    }

    #[test]
    fn keyed_summary_input_is_not_replaced_by_its_unit_weight() {
        use planner_types::post_asap::{
            SummaryExpr, SummaryInputExpr, SummaryNode, SummarySchema, SummaryUpdate,
        };
        use planner_types::pre_asap::{ColumnRef, QueryExpr, Reduction, ScalarValue};
        let schema = SummarySchema {
            fields: vec![],
            time_index: None,
        };
        let family = materialization(
            AggregationType::Sum,
            "value",
            60,
            60,
            ("variant", serde_json::json!(1)),
        )
        .accumulator_spec()
        .unwrap()
        .family;
        let node = SummaryNode {
            expr: SummaryExpr::SummaryAgg {
                child: std::rc::Rc::new(SummaryNode {
                    expr: SummaryExpr::KeepPreAsap(std::rc::Rc::new(QueryExpr::Literal(
                        ScalarValue::Int64(1),
                    ))),
                    schema: schema.clone(),
                    guarantee: Default::default(),
                }),
                family,
                input: SummaryUpdate {
                    item: Some(SummaryInputExpr::Column(ColumnRef::Named("value".into()))),
                    weight: SummaryInputExpr::Constant(1.0),
                    weight_domain: Default::default(),
                },
                reduction: Reduction::Reduce(vec![].into()),
                grouping: Default::default(),
            },
            schema,
            guarantee: Default::default(),
        };
        assert!(clickhouse_materialization_leaf_contract(&node, 0, 60_000)
            .unwrap_err()
            .contains("explicit item projection"));
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
            AggregationType::Max,
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
            select_materialization(
                &configs,
                "telemetry",
                &asap_types::sds::ValueProjectionIdentity::Column {
                    name: "requests".into()
                },
                "",
                &sum_family,
                60
            )
            .unwrap()
            .policy_fingerprint(),
            sum_60.policy_fingerprint()
        );
        let dd_family = dd_2.accumulator_spec().unwrap().family;
        assert_eq!(
            select_materialization(
                &configs,
                "telemetry",
                &asap_types::sds::ValueProjectionIdentity::Column {
                    name: "requests".into()
                },
                "",
                &dd_family,
                60
            )
            .unwrap()
            .policy_fingerprint(),
            dd_2.policy_fingerprint()
        );
        assert_eq!(
            select_materialization(
                &configs,
                "telemetry",
                &asap_types::sds::ValueProjectionIdentity::Column {
                    name: "requests".into()
                },
                "",
                &count_family,
                60
            )
            .unwrap()
            .policy_fingerprint(),
            count_60.policy_fingerprint()
        );
        assert!(select_materialization(
            &configs,
            "telemetry",
            &asap_types::sds::ValueProjectionIdentity::Column {
                name: "missing".into()
            },
            "",
            &sum_family,
            60
        )
        .is_err());
        let mut ambiguous = configs.clone();
        ambiguous.push(sum_60);
        assert!(select_materialization(
            &ambiguous,
            "telemetry",
            &asap_types::sds::ValueProjectionIdentity::Column {
                name: "requests".into()
            },
            "",
            &sum_family,
            60
        )
        .unwrap_err()
        .to_string()
        .contains("ambiguous"));
    }

    #[test]
    fn constant_integer_boundaries_reject_overflow_and_dynamic_values() {
        use planner_types::pre_asap::{ArithmeticOpKind, ScalarValue};
        let literal = |value| QueryExpr::Literal(ScalarValue::Int64(value));
        let subtract = |left, right| QueryExpr::Arithmetic {
            op: ArithmeticOpKind::Sub,
            left: std::rc::Rc::new(left),
            right: std::rc::Rc::new(right),
        };
        assert_eq!(
            constant_int64(&subtract(literal(1_788_891_296_000), literal(43_200_000))),
            Some(1_788_848_096_000)
        );
        assert_eq!(
            constant_int64(&subtract(literal(i64::MIN), literal(1))),
            None
        );
        assert_eq!(
            constant_int64(&subtract(QueryExpr::Column(0), literal(1))),
            None
        );
        assert_eq!(
            constant_int64(&QueryExpr::Literal(ScalarValue::Float64(1.0))),
            None
        );
    }

    #[test]
    fn sql_evaluation_rejects_empty_reversed_and_unrepresentable_ranges() {
        for (start_ms, end_ms) in [(2, 1), (1, 1), (0, u64::MAX)] {
            assert!(validate_sql_evaluation(&ClickHouseSqlWorkloadEntry {
                sql: "SELECT sum(value) FROM telemetry".into(),
                start_ms,
                end_ms,
                cumulative: true,
            })
            .is_err());
        }
    }

    #[tokio::test]
    async fn compiles_summary_joined_with_exact_table_into_mixed_dag() {
        let config = materialization(
            AggregationType::Sum,
            "value",
            2,
            2,
            ("variant", serde_json::json!(1)),
        );
        let sds =
            SummaryCatalog::from_materializations(71, 1, std::slice::from_ref(&config)).unwrap();
        let envelope = crate::physical::compiler::PlanEnvelope {
            plan_id: 71,
            plan_version: 1,
            generated_at_unix_ms: 0,
            activation_unix_ms: 0,
            expiry_unix_ms: None,
            backend_compat: crate::physical::compiler::BACKEND_COMPAT.into(),
            planner_revision: crate::physical::compiler::PLANNER_REVISION.into(),
            capability_snapshot_id: "clickhouse-mixed-compile-test".into(),
        };
        let mut precompute =
            PrecomputePlan::build_backend_local(envelope.clone(), vec![config]).unwrap();
        precompute.summary_catalog = Some(sds.reference().unwrap());
        let mut transmission = crate::physical::compiler::build_transmission_plan(
            envelope,
            &precompute,
            &std::collections::BTreeMap::new(),
        )
        .unwrap();
        transmission.summary_catalog = Some(sds.reference().unwrap());
        let timestamped = |time_name: &str, value_name: &str| {
            Schema::with_time_index(
                vec![
                    Column::new(time_name, DataType::Timestamp, false),
                    Column::new(value_name, DataType::Float64, false),
                ],
                0,
                vec![],
            )
        };
        let mut request = ClickHouseSqlWorkload {
            summary_catalog: sds,
            precompute_plan: precompute,
            transmission_plan: transmission,
            tables: HashMap::from([
                ("telemetry".into(), timestamped("timestamp_ms", "value")),
                (
                    "divisors".into(),
                    Schema::with_time_index(
                        vec![
                            Column::new("timestamp", DataType::Int64, false),
                            Column::new("divisor", DataType::Float64, false),
                        ],
                        0,
                        vec![],
                    ),
                ),
            ]),
            accuracy: AccuracyTarget::Exact,
            queries: vec![ClickHouseSqlWorkloadEntry {
                sql: "SELECT sums.timestamp, sums.total / divisors.divisor AS ratio FROM (SELECT 2000 AS timestamp, sum(value) AS total FROM telemetry WHERE timestamp_ms >= 0 AND timestamp_ms < 2000) AS sums INNER JOIN divisors ON sums.timestamp = divisors.timestamp".into(),
                start_ms: 0,
                end_ms: 2_000,
                cumulative: true,
            }],
        };
        let publication = compile_clickhouse_workload(&request).await.unwrap();
        // A simple installed aggregate publishes the same key as a refresh.
        let simple_sql =
            "SELECT sum(value) FROM telemetry WHERE timestamp_ms >= 0 AND timestamp_ms < 2000";
        let simple = compile_clickhouse_workload(&ClickHouseSqlWorkload {
            summary_catalog: request.summary_catalog.clone(),
            precompute_plan: request.precompute_plan.clone(),
            transmission_plan: request.transmission_plan.clone(),
            tables: request.tables.clone(),
            accuracy: request.accuracy.clone(),
            queries: vec![ClickHouseSqlWorkloadEntry {
                sql: simple_sql.into(),
                start_ms: 0,
                end_ms: 2000,
                cumulative: true,
            }],
        })
        .await
        .unwrap();
        let shifted = canonicalize_clickhouse_sql(
            "SELECT sum(value) FROM telemetry WHERE timestamp_ms >= 2000 AND timestamp_ms < 4000",
            &SqlCatalog {
                tables: request.tables.clone(),
            },
            AccuracyTarget::Exact,
        )
        .await
        .unwrap();
        assert_eq!(
            simple
                .query_plan
                .clickhouse_context
                .as_ref()
                .unwrap()
                .window_templates[&shifted]
                .len(),
            1
        );
        assert!(shifted.starts_with("moving-window-v1:"));
        // Different windows remain distinct publications even with one template.
        let windows = || {
            vec![
            ClickHouseSqlWorkloadEntry { sql: simple_sql.into(), start_ms: 0, end_ms: 2000, cumulative: true },
            ClickHouseSqlWorkloadEntry {
                sql: "SELECT sum(value) FROM telemetry WHERE timestamp_ms >= 2000 AND timestamp_ms < 4000".into(),
                start_ms: 2000, end_ms: 4000, cumulative: true,
            },
        ]
        };
        let multiple = compile_clickhouse_workload(&ClickHouseSqlWorkload {
            summary_catalog: request.summary_catalog.clone(),
            precompute_plan: request.precompute_plan.clone(),
            transmission_plan: request.transmission_plan.clone(),
            tables: request.tables.clone(),
            accuracy: request.accuracy.clone(),
            queries: windows(),
        })
        .await
        .unwrap();
        assert_eq!(multiple.query_plan.entries.len(), 2);
        let (multiple_auto, multiple_traces) =
            compile_automatic_clickhouse_workload(&ClickHouseSqlAutomaticWorkload {
                envelope: request.precompute_plan.envelope.clone(),
                tables: request.tables.clone(),
                accuracy: request.accuracy.clone(),
                queries: windows(),
            })
            .await
            .unwrap();
        assert_eq!(multiple_auto.query_plan.entries.len(), 2);
        for plan in [&multiple.query_plan, &multiple_auto.query_plan] {
            let identities = &plan.clickhouse_context.as_ref().unwrap().window_templates[&shifted];
            assert_eq!(identities.len(), 2);
            assert_ne!(identities[0], identities[1]);
            for identity in identities {
                assert!(plan.lookup_clickhouse(identity).is_ok());
            }
        }
        assert_eq!(multiple_traces.len(), 2);
        assert_eq!(multiple_auto.precompute_plan.executable_dags.len(), 2);
        multiple_auto.validate().unwrap();
        let bindings = multiple_auto
            .query_plan
            .entries
            .values()
            .map(|entry| entry.materialization_bindings()[0].materialization)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            bindings.len(),
            2,
            "different pane origins must retain distinct materializations"
        );
        let mut invalid = multiple_auto.clone();
        invalid
            .query_plan
            .clickhouse_context
            .as_mut()
            .unwrap()
            .window_templates
            .values_mut()
            .next()
            .unwrap()
            .push("missing concrete query".into());
        assert!(invalid.validate().is_err());
        let (automatic, traces) =
            compile_automatic_clickhouse_workload(&ClickHouseSqlAutomaticWorkload {
                envelope: request.precompute_plan.envelope.clone(),
                tables: request.tables.clone(),
                accuracy: request.accuracy.clone(),
                queries: request
                    .queries
                    .iter()
                    .map(|query| ClickHouseSqlWorkloadEntry {
                        sql: query.sql.clone(),
                        start_ms: query.start_ms,
                        end_ms: query.end_ms,
                        cumulative: query.cumulative,
                    })
                    .collect(),
            })
            .await
            .unwrap();
        assert_eq!(automatic.precompute_plan.materializations.len(), 1);
        assert_eq!(traces.len(), 1);
        assert!(traces
            .values()
            .any(
                |trace| trace["groups"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|group| group["candidates"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|candidate| candidate["selected"] == true))
            ));
        assert_eq!(automatic.precompute_plan.executable_dags.len(), 1);
        automatic.validate().unwrap();
        let mut integer_source = ClickHouseSqlAutomaticWorkload {
            envelope: request.precompute_plan.envelope.clone(),
            tables: request.tables.clone(),
            accuracy: request.accuracy.clone(),
            queries: request
                .queries
                .iter()
                .map(|query| ClickHouseSqlWorkloadEntry {
                    sql: query.sql.clone(),
                    start_ms: query.start_ms,
                    end_ms: query.end_ms,
                    cumulative: query.cumulative,
                })
                .collect(),
        };
        // An Int64 source is admitted and carries its declared type into the
        // materialization, which is what lets the ingest reader widen the
        // column explicitly and fail loudly on a value beyond the exact
        // Float64 range instead of silently summarising a rounded one.
        integer_source.tables.get_mut("telemetry").unwrap().columns[1].dtype = DataType::Int64;
        let (integer_plan, _) = compile_automatic_clickhouse_workload(&integer_source)
            .await
            .expect("an Int64 source is admitted with its producer typing");
        assert_eq!(
            integer_plan.precompute_plan.materializations[0]
                .value_source_column
                .as_ref()
                .expect("column projections carry their producer typing")
                .dtype,
            DataType::Int64
        );
        integer_source.tables.get_mut("telemetry").unwrap().columns[1].dtype = DataType::Float64;
        integer_source.queries[0].sql = integer_source.queries[0].sql.replace(
            "FROM telemetry WHERE",
            "FROM (SELECT timestamp_ms, value * 2 AS value FROM telemetry) doubled WHERE",
        );
        assert!(
            compile_automatic_clickhouse_workload(&integer_source)
                .await
                .is_err(),
            "a producer projection must not be erased while binding its original table"
        );
        let installed = publication
            .precompute_plan
            .executable_dags
            .get(&request.queries[0].sql)
            .unwrap();
        installed.validate().unwrap();
        assert_eq!(installed.binding.precompute_sinks.len(), 1);
        assert_eq!(
            installed.binding.nodes.len(),
            installed.document.nodes.len()
        );
        assert!(!installed.binding.nodes.values().any(|binding| matches!(
            binding,
            crate::physical::executable_binding::BackendNodeBinding::Query { .. }
        )));
        assert!(
            publication.query_plan.selected_dags[&request.queries[0].sql]
                .nodes
                .len()
                > installed.document.nodes.len()
        );
        let entry = publication.query_plan.entries.values().next().unwrap();
        // External SQL retains its literal time range until it can be bound.
        assert!(!entry.canonical_query.starts_with("moving-window-v1:"));
        assert!(entry
            .nodes
            .values()
            .any(|node| matches!(node, crate::query_plan::QueryPlanNode::ExternalExact { .. })));
        assert!(entry.nodes.values().any(|node| matches!(
            node,
            crate::query_plan::QueryPlanNode::ReadMaterialization { .. }
        )));
        assert!(entry.nodes.values().any(|node| matches!(
            node,
            crate::query_plan::QueryPlanNode::RelationalJoin { .. }
        )));
        assert!(entry.nodes.values().any(|node| {
            let crate::query_plan::QueryPlanNode::Relational { operation, .. } = node else {
                return false;
            };
            matches!(
                serde_json::from_value::<planner_types::post_asap::ValueOperation>(
                    operation.clone()
                ),
                Ok(planner_types::post_asap::ValueOperation::Project { .. })
            )
        }));
        let original = request.queries[0].sql.clone();
        let schema = request.tables.get_mut("telemetry").unwrap();
        schema.columns[schema.time_index.unwrap()].name = "other_timestamp".into();
        request.queries[0].sql = original.replace("timestamp_ms", "other_timestamp");
        assert!(
            compile_clickhouse_workload(&request).await.is_err(),
            "a summary cannot bind a different timestamp projection"
        );
        let schema = request.tables.get_mut("telemetry").unwrap();
        schema.columns[schema.time_index.unwrap()].name = "timestamp_ms".into();
        request.queries[0].sql = original.replace("timestamp_ms < 2000", "timestamp_ms <= 1999");
        assert!(compile_clickhouse_workload(&request).await.is_ok());
        request.queries[0].sql = original.replace("timestamp_ms < 2000", "timestamp_ms <= 2000");
        assert!(compile_clickhouse_workload(&request).await.is_err());
        request.queries[0].sql = original
            .replace("timestamp_ms >= 0", "timestamp_ms >= 1000")
            .replace("timestamp_ms < 2000", "timestamp_ms < 3000");
        assert!(compile_clickhouse_workload(&request).await.is_err());

        request
            .tables
            .get_mut("telemetry")
            .unwrap()
            .columns
            .push(Column::new("metric", DataType::Utf8, false));
        request.queries[0].sql = original.replace(
            "WHERE timestamp_ms",
            "WHERE metric = 'requests' AND timestamp_ms",
        );
        assert!(
            compile_clickhouse_workload(&request).await.is_err(),
            "an unfiltered summary cannot satisfy a filtered query"
        );
        let mut config = request.precompute_plan.materializations[0].clone();
        config.table_population = Some(asap_types::table_population::TablePopulation {
            predicates: vec![asap_types::table_population::TableColumnPredicate {
                column: "metric".into(),
                operator: planner_types::pre_asap::CompareOpKind::Eq,
                value: planner_types::pre_asap::ScalarValue::Utf8("requests".into()),
            }],
        });
        request.summary_catalog =
            SummaryCatalog::from_materializations(71, 1, &[config.clone()]).unwrap();
        let envelope = request.precompute_plan.envelope.clone();
        request.precompute_plan =
            PrecomputePlan::build_backend_local(envelope.clone(), vec![config]).unwrap();
        request.precompute_plan.summary_catalog =
            Some(request.summary_catalog.reference().unwrap());
        request.transmission_plan = crate::physical::compiler::build_transmission_plan(
            envelope,
            &request.precompute_plan,
            &std::collections::BTreeMap::new(),
        )
        .unwrap();
        request.transmission_plan.summary_catalog =
            Some(request.summary_catalog.reference().unwrap());
        assert!(compile_clickhouse_workload(&request).await.is_ok());
    }
}
