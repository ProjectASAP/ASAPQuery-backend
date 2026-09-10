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
    BoundClickHouseQuery, ClickHousePlanningContext, FallbackPolicy, FixedEvaluationRange,
    InstantExecution, MaterializationBinding, PhysicalGrouping, QueryLanguage, QueryPlan,
    QueryPlanEntry,
};
use asap_types::summary_catalog::SummaryCatalog;
use serde::{Deserialize, Serialize};
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
    /// Exact subtree artifacts emitted alongside ASAPPlanner's selected DAG.
    #[serde(default)]
    pub exact_subtrees: Vec<ClickHouseExactSubtreeArtifact>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClickHouseExactSubtreeArtifact {
    pub canonical_subtree: serde_json::Value,
    pub canonical_subtree_fingerprint: u64,
    pub query: BoundClickHouseQuery,
}

pub fn canonical_subtree_fingerprint(
    canonical_subtree: &serde_json::Value,
) -> Result<u64, ClickHousePlanningError> {
    let encoded = serde_json::to_vec(canonical_subtree)
        .map_err(|error| ClickHousePlanningError::Lower(error.to_string()))?;
    Ok(xxhash_rust::xxh64::xxh64(&encoded, 0))
}

/// Physical-plan components produced for the normal atomic install path.
#[derive(Debug, Serialize)]
pub struct ClickHouseCompiledBundle {
    pub sds: SummaryCatalog,
    pub tables: HashMap<String, planner_types::pre_asap::Schema>,
    pub accuracy: AccuracyTarget,
    pub query_plan: QueryPlan,
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
        let mut fingerprints = std::collections::BTreeSet::new();
        for artifact in &query.exact_subtrees {
            let actual = canonical_subtree_fingerprint(&artifact.canonical_subtree)?;
            if actual != artifact.canonical_subtree_fingerprint {
                return Err(ClickHousePlanningError::Lower(
                    "exact SQL artifact fingerprint differs from its canonical subtree".into(),
                ));
            }
            if !fingerprints.insert(actual) {
                return Err(ClickHousePlanningError::Lower(
                    "duplicate exact SQL subtree artifact".into(),
                ));
            }
        }
        let mut used = std::collections::BTreeSet::new();
        let executable = QueryPlanEntry::compile_bound_relational_with_external(
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
            |cut, schema| {
                let canonical_subtree = serde_json::to_value(cut).map_err(|error| {
                    crate::query_plan::QueryPlanError::Invalid(error.to_string())
                })?;
                let fingerprint =
                    canonical_subtree_fingerprint(&canonical_subtree).map_err(|error| {
                        crate::query_plan::QueryPlanError::Invalid(error.to_string())
                    })?;
                let Some((index, artifact)) =
                    query
                        .exact_subtrees
                        .iter()
                        .enumerate()
                        .find(|(_, artifact)| {
                            artifact.canonical_subtree_fingerprint == fingerprint
                                && artifact.canonical_subtree == canonical_subtree
                        })
                else {
                    return Ok(None);
                };
                if &artifact.query.output_schema != schema {
                    return Err(crate::query_plan::QueryPlanError::Invalid(
                        "exact SQL artifact schema differs from the Planner cut".into(),
                    ));
                }
                used.insert(index);
                Ok(Some(artifact.query.clone()))
            },
        )
        .map_err(|error| ClickHousePlanningError::Lower(error.to_string()))?;
        if used.len() != query.exact_subtrees.len() {
            return Err(ClickHousePlanningError::Lower(
                "one or more exact SQL subtree artifacts were not used by the Planner DAG".into(),
            ));
        }
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
    Ok(ClickHouseCompiledBundle {
        sds: request.sds.clone(),
        tables: request.tables.clone(),
        accuracy: request.accuracy.clone(),
        query_plan: QueryPlan {
            plan_id: request.sds.plan_id,
            plan_version: request.sds.plan_version,
            clickhouse_context: Some(ClickHousePlanningContext {
                tables: request.tables.clone(),
                accuracy: request.accuracy.clone(),
            }),
            entries,
        },
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
        item_labels: selected.aggregated_labels.labels.clone(),
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

    #[tokio::test]
    async fn production_compile_publishes_only_the_planner_matched_exact_cut() {
        use crate::physical::compiler::{PlanEnvelope, BACKEND_COMPAT, PLANNER_REVISION};
        use planner_types::{
            post_asap::{SummaryExpr, SummaryNode},
            pre_asap::{Column, DataType, Schema},
        };
        use std::{collections::BTreeMap, rc::Rc};

        fn exact_cut(
            node: &Rc<SummaryNode>,
        ) -> Option<(&QueryExpr, &planner_types::post_asap::SummarySchema)> {
            match &node.expr {
                SummaryExpr::KeepPreAsap(expr) => Some((expr, &node.schema)),
                SummaryExpr::ValueOperation { child, .. } => exact_cut(child),
                _ => None,
            }
        }

        let table = Schema::with_time_index(
            vec![
                Column::new("timestamp", DataType::Timestamp, false),
                Column::new("value", DataType::Float64, false),
            ],
            0,
            vec![],
        );
        let tables = HashMap::from([("requests".into(), table)]);
        let planned = plan_clickhouse_sql(
            "SELECT timestamp, value FROM requests",
            &SqlCatalog {
                tables: tables.clone(),
            },
            AccuracyTarget::Exact,
        )
        .await
        .unwrap();
        let PhysicalExpr::Committed(crate::physical::post_asap::PostAsapPlan::Summary(root)) =
            planned.physical
        else {
            panic!("summary plan")
        };
        let (cut, schema) = exact_cut(&root).expect("Planner exact cut");
        let canonical_subtree = serde_json::to_value(cut).unwrap();
        let fingerprint = canonical_subtree_fingerprint(&canonical_subtree).unwrap();

        let sds = SummaryCatalog::from_materializations(91, 1, &[]).unwrap();
        let envelope = PlanEnvelope {
            plan_id: 91,
            plan_version: 1,
            generated_at_unix_ms: 0,
            activation_unix_ms: 0,
            expiry_unix_ms: None,
            backend_compat: BACKEND_COMPAT.into(),
            planner_revision: PLANNER_REVISION.into(),
            capability_snapshot_id: "sql-exact-cut".into(),
        };
        let mut precompute_plan = PrecomputePlan::build(envelope.clone(), vec![], &[]).unwrap();
        precompute_plan.summary_catalog = Some(sds.reference().unwrap());
        let mut transmission_plan =
            TransmissionPlan::build(envelope, &precompute_plan, &BTreeMap::new()).unwrap();
        transmission_plan.summary_catalog = Some(sds.reference().unwrap());
        let workload = ClickHouseSqlWorkload {
            sds: sds.clone(),
            precompute_plan,
            transmission_plan,
            tables,
            accuracy: AccuracyTarget::Exact,
            queries: vec![ClickHouseSqlWorkloadEntry {
                sql: "SELECT timestamp, value FROM requests".into(),
                start_ms: 0,
                end_ms: 2_000,
                cumulative: false,
                exact_subtrees: vec![ClickHouseExactSubtreeArtifact {
                    canonical_subtree,
                    canonical_subtree_fingerprint: fingerprint,
                    query: BoundClickHouseQuery {
                        sql: "SELECT timestamp, value FROM requests WHERE timestamp >= {from:Int64} AND timestamp < {to:Int64}".into(),
                        parameters: BTreeMap::new(),
                        start_parameter: Some("from".into()),
                        end_parameter: Some("to".into()),
                        output_schema: schema.clone(),
                    },
                }],
            }],
        };
        let bundle = compile_clickhouse_workload(&workload).await.unwrap();
        bundle.query_plan.validate_against_catalog(&sds).unwrap();
        assert!(bundle
            .query_plan
            .entries
            .values()
            .next()
            .unwrap()
            .nodes
            .values()
            .any(|node| matches!(
                node,
                crate::query_plan::QueryPlanNode::ExternalSqlLeaf { .. }
            )));

        let mut unused = workload;
        let duplicate = unused.queries[0].exact_subtrees[0].clone();
        unused.queries[0].exact_subtrees.push(duplicate);
        assert!(compile_clickhouse_workload(&unused).await.is_err());
        unused.queries[0].exact_subtrees.pop();
        unused.queries[0].exact_subtrees[0].canonical_subtree =
            serde_json::to_value(QueryExpr::<usize>::Literal(
                planner_types::pre_asap::ScalarValue::Float64(1.0),
            ))
            .unwrap();
        unused.queries[0].exact_subtrees[0].canonical_subtree_fingerprint =
            canonical_subtree_fingerprint(&unused.queries[0].exact_subtrees[0].canonical_subtree)
                .unwrap();
        assert!(compile_clickhouse_workload(&unused).await.is_err());
    }
}
