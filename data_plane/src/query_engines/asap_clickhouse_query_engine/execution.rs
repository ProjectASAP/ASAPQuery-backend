//! SQL boundary around the shared, compiler-bound physical DAG executor.
use super::{
    clickhouse_result_adapter::{from_series_rows, ClickHouseQueryResult},
    relational_adapter::{ClickHouseRelation, ClickHouseRelationalAdapter},
};
use crate::{
    query_engines::asap_query_engine::{
        catalog_resolver::validate_payload, post_asap_readout::execute_query_plan_from_readout,
    },
    storage_engines::sketch_db::index::SketchStore,
};
use asap_types::summary_catalog::SummaryCatalog;
use control_plane::query_plan::{QueryNodeId, QueryPlanEntry, QueryPlanNode};
use planner_types::post_asap::ValueOperation;
use std::collections::{BTreeMap, BTreeSet};

pub type PreparedExternalLeaves = BTreeMap<QueryNodeId, ClickHouseRelation>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClickHouseDagFallback {
    NoCandidates,
    UnsupportedPlan(String),
    IncompleteCoverage {
        requested: (u64, u64),
        observed: Option<(u64, u64)>,
    },
    ResultEncoding(String),
}

fn apply_relational_operation(
    operation: serde_json::Value,
    output_schema: &planner_types::post_asap::SummarySchema,
    relation: ClickHouseRelation,
) -> Result<ClickHouseRelation, String> {
    let adapter = ClickHouseRelationalAdapter;
    if let Some(filter) = operation.get("Filter") {
        let predicate = filter
            .get("pred")
            .cloned()
            .ok_or_else(|| "published Filter lacks pred".to_owned())
            .and_then(|value| serde_json::from_value(value).map_err(|error| error.to_string()))?;
        return adapter
            .apply_filter(&predicate, relation)
            .map_err(|error| error.to_string());
    }
    let operation: ValueOperation =
        serde_json::from_value(operation).map_err(|error| error.to_string())?;
    adapter
        .apply_operation(&operation, output_schema, relation)
        .map_err(|error| error.to_string())
}

fn execute_relation_subtree(
    index: &SketchStore,
    entry: &QueryPlanEntry,
    root: QueryNodeId,
    expected_schema: &planner_types::post_asap::SummarySchema,
    prepared: &PreparedExternalLeaves,
    t0_ms: u64,
    t1_ms: u64,
    is_cumulative: bool,
) -> Result<ClickHouseRelation, String> {
    match entry.nodes.get(&root) {
        Some(QueryPlanNode::ExternalExact { request, .. }) => {
            if request.language != asap_types::QueryLanguage::ClickHouseSql {
                return Err("ClickHouse DAG contains an external leaf for another language".into());
            }
            let control_plane::query_plan::ExternalExactOutput::Relation { schema } =
                &request.output
            else {
                return Err("ClickHouse external leaf must declare relation output".into());
            };
            let declared: planner_types::post_asap::SummarySchema =
                serde_json::from_value(schema.clone()).map_err(|error| error.to_string())?;
            if &declared != expected_schema {
                return Err("external exact leaf schema differs from its parent edge".into());
            }
            prepared
                .get(&root)
                .cloned()
                .ok_or_else(|| "published external exact leaf was not prepared".into())
        }
        Some(QueryPlanNode::Relational {
            input,
            operation,
            input_schema,
            output_schema,
        }) => {
            let input = execute_relation_subtree(
                index,
                entry,
                *input,
                input_schema,
                prepared,
                t0_ms,
                t1_ms,
                is_cumulative,
            )?;
            apply_relational_operation(operation.clone(), output_schema, input)
        }
        Some(QueryPlanNode::RelationalJoin {
            inputs,
            join_kind,
            pred,
            left_schema,
            right_schema,
            output_schema,
        }) => {
            if !matches!(join_kind, planner_types::pre_asap::JoinKind::Inner) {
                return Err("only inner relational joins are executable".into());
            }
            let left = execute_relation_subtree(
                index,
                entry,
                inputs[0],
                left_schema,
                prepared,
                t0_ms,
                t1_ms,
                is_cumulative,
            )?;
            let right = execute_relation_subtree(
                index,
                entry,
                inputs[1],
                right_schema,
                prepared,
                t0_ms,
                t1_ms,
                is_cumulative,
            )?;
            let pred = serde_json::from_value(pred.clone()).map_err(|error| error.to_string())?;
            ClickHouseRelationalAdapter
                .apply_inner_equi_join(&pred, output_schema, left, right)
                .map_err(|error| error.to_string())
        }
        Some(_) => {
            validate_reachable(entry, root)?;
            let outcome =
                execute_query_plan_from_readout(index, entry, root, t0_ms, t1_ms, is_cumulative)
                    .map_err(|error| format!("incomplete leaf coverage: {error:?}"))?;
            let reachable = entry
                .topological_order_from(root)
                .map_err(|error| format!("invalid leaf DAG: {error}"))?;
            for (leaf_id, binding) in reachable.iter().filter_map(|id| match entry.nodes.get(id) {
                Some(QueryPlanNode::ReadMaterialization { binding }) => Some((*id, binding)),
                _ => None,
            }) {
                let leaf_outcome = execute_query_plan_from_readout(
                    index,
                    entry,
                    leaf_id,
                    t0_ms,
                    t1_ms,
                    is_cumulative,
                )
                .map_err(|error| format!("incomplete leaf coverage: {error:?}"))?;
                let origin = binding
                    .pane_origin_ms
                    .ok_or_else(|| "incomplete leaf coverage: missing pane origin".to_owned())?;
                let start = i64::try_from(t0_ms)
                    .map_err(|_| "incomplete leaf coverage: start exceeds i64".to_owned())?;
                let end = i64::try_from(t1_ms)
                    .map_err(|_| "incomplete leaf coverage: end exceeds i64".to_owned())?;
                let pane = i64::try_from(binding.window_ms)
                    .map_err(|_| "incomplete leaf coverage: pane exceeds i64".to_owned())?;
                if pane <= 0
                    || (start - origin).rem_euclid(pane) != 0
                    || (end - origin).rem_euclid(pane) != 0
                    || !complete_pane_coverage(
                        leaf_outcome.coverage,
                        (t0_ms, t1_ms),
                        binding.window_ms,
                    )
                {
                    return Err(format!(
                        "incomplete leaf coverage: requested ({t0_ms}, {t1_ms}), observed {:?}, pane {} origin {}",
                        leaf_outcome.coverage, binding.window_ms, origin
                    ));
                }
            }
            ClickHouseRelation::from_series_rows(expected_schema, outcome.series, outcome.coverage)
                .map_err(|error| error.to_string())
        }
        None => Err(format!("published DAG references missing node {}", root.0)),
    }
}
pub enum ClickHouseDagOutcome {
    Accelerated(ClickHouseQueryResult),
    Fallback(ClickHouseDagFallback),
}

fn complete_pane_coverage(
    coverage: Option<(u64, u64)>,
    requested: (u64, u64),
    pane_ms: u64,
) -> bool {
    let Some((first_end, last_end)) = coverage else {
        return false;
    };
    pane_ms > 0 && first_end.saturating_sub(pane_ms) <= requested.0 && last_end >= requested.1
}

/// Executes only the published physical DAG; serving performs no parsing,
/// summary selection, or materialization candidate search.
pub fn execute_sql_dag(
    index: &SketchStore,
    entry: &QueryPlanEntry,
    sds: &SummaryCatalog,
    t0_ms: u64,
    t1_ms: u64,
    is_cumulative: bool,
) -> ClickHouseDagOutcome {
    execute_sql_dag_with_external(
        index,
        entry,
        sds,
        &PreparedExternalLeaves::new(),
        t0_ms,
        t1_ms,
        is_cumulative,
    )
}

pub fn execute_sql_dag_with_external(
    index: &SketchStore,
    entry: &QueryPlanEntry,
    sds: &SummaryCatalog,
    prepared: &PreparedExternalLeaves,
    t0_ms: u64,
    t1_ms: u64,
    is_cumulative: bool,
) -> ClickHouseDagOutcome {
    if let Err(error) = validate_payload(Some(sds), entry, sds.plan_id, sds.plan_version) {
        return ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::UnsupportedPlan(
            error.to_string(),
        ));
    }
    let has_relational_join = entry.topological_order().is_ok_and(|ids| {
        ids.iter().any(|id| {
            matches!(
                entry.nodes.get(id),
                Some(QueryPlanNode::RelationalJoin { .. } | QueryPlanNode::ExternalExact { .. })
            )
        })
    });
    if has_relational_join {
        let root_schema = match entry.nodes.get(&entry.root) {
            Some(QueryPlanNode::Relational { output_schema, .. })
            | Some(QueryPlanNode::RelationalJoin { output_schema, .. }) => output_schema.clone(),
            Some(QueryPlanNode::ExternalExact { request, .. }) => {
                let control_plane::query_plan::ExternalExactOutput::Relation { schema } =
                    &request.output
                else {
                    return ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::UnsupportedPlan(
                        "ClickHouse external root must declare relation output".into(),
                    ));
                };
                match serde_json::from_value(schema.clone()) {
                    Ok(schema) => schema,
                    Err(error) => {
                        return ClickHouseDagOutcome::Fallback(
                            ClickHouseDagFallback::UnsupportedPlan(error.to_string()),
                        )
                    }
                }
            }
            _ => {
                return ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::UnsupportedPlan(
                    "relational join plan root has no relation schema".into(),
                ))
            }
        };
        let relation = match execute_relation_subtree(
            index,
            entry,
            entry.root,
            &root_schema,
            prepared,
            t0_ms,
            t1_ms,
            is_cumulative,
        ) {
            Ok(relation) => relation,
            Err(error) if error.contains("incomplete leaf coverage") => {
                return ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::IncompleteCoverage {
                    requested: (t0_ms, t1_ms),
                    observed: None,
                })
            }
            Err(error) => {
                return ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::UnsupportedPlan(
                    error,
                ))
            }
        };
        let pane_ms = entry
            .materialization_bindings()
            .iter()
            .map(|binding| binding.window_ms)
            .max()
            .unwrap_or(0);
        if !complete_pane_coverage(relation.coverage, (t0_ms, t1_ms), pane_ms) {
            return ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::IncompleteCoverage {
                requested: (t0_ms, t1_ms),
                observed: relation.coverage,
            });
        }
        return match relation.into_result() {
            Ok(result) => ClickHouseDagOutcome::Accelerated(result),
            Err(error) => ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::ResultEncoding(
                error.to_string(),
            )),
        };
    }
    let mut base_root = entry.root;
    let mut relational = Vec::new();
    loop {
        match entry.nodes.get(&base_root) {
            Some(QueryPlanNode::Relational {
                input,
                operation,
                input_schema,
                output_schema,
            }) => {
                relational.push((
                    operation.clone(),
                    input_schema.clone(),
                    output_schema.clone(),
                ));
                base_root = *input;
            }
            _ => break,
        }
    }
    if let Err(detail) = validate_reachable(entry, base_root) {
        return ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::UnsupportedPlan(detail));
    }
    let outcome =
        match execute_query_plan_from_readout(index, entry, base_root, t0_ms, t1_ms, is_cumulative)
        {
            Ok(outcome) => outcome,
            Err(error) => {
                let detail = format!("{error:?}");
                if detail.contains("missing materialized pane")
                    || detail.contains("incomplete materialized panes")
                {
                    return ClickHouseDagOutcome::Fallback(
                        ClickHouseDagFallback::IncompleteCoverage {
                            requested: (t0_ms, t1_ms),
                            observed: None,
                        },
                    );
                }
                return ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::UnsupportedPlan(
                    detail,
                ));
            }
        };
    let pane_ms = entry
        .materialization_bindings()
        .iter()
        .map(|binding| binding.window_ms)
        .max()
        .unwrap_or(0);
    if !complete_pane_coverage(outcome.coverage, (t0_ms, t1_ms), pane_ms) {
        return ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::IncompleteCoverage {
            requested: (t0_ms, t1_ms),
            observed: outcome.coverage,
        });
    }
    if relational.is_empty() {
        return match from_series_rows(outcome.series) {
            Ok(result) => ClickHouseDagOutcome::Accelerated(result),
            Err(error) => ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::ResultEncoding(
                error.to_string(),
            )),
        };
    }
    let input_schema = &relational.last().expect("non-empty").1;
    let mut relation = match ClickHouseRelation::from_series_rows(
        input_schema,
        outcome.series,
        outcome.coverage,
    ) {
        Ok(relation) => relation,
        Err(error) => {
            return ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::ResultEncoding(
                error.to_string(),
            ))
        }
    };
    let adapter = ClickHouseRelationalAdapter;
    for (wire_operation, _, output_schema) in relational.into_iter().rev() {
        if let Some(filter) = wire_operation.get("Filter") {
            let predicate = filter
                .get("pred")
                .cloned()
                .ok_or_else(|| "published Filter lacks pred".to_owned())
                .and_then(|value| serde_json::from_value(value).map_err(|error| error.to_string()));
            relation = match predicate.and_then(|predicate| {
                adapter
                    .apply_filter(&predicate, relation)
                    .map_err(|error| error.to_string())
            }) {
                Ok(relation) => relation,
                Err(error) => {
                    return ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::UnsupportedPlan(
                        format!("invalid published Filter: {error}"),
                    ))
                }
            };
            continue;
        }
        let operation: ValueOperation = match serde_json::from_value(wire_operation) {
            Ok(operation) => operation,
            Err(error) => {
                return ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::UnsupportedPlan(
                    format!("invalid published relational operation: {error}"),
                ))
            }
        };
        relation = match adapter.apply_operation(&operation, &output_schema, relation) {
            Ok(relation) => relation,
            Err(error) => {
                return ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::UnsupportedPlan(
                    error.to_string(),
                ))
            }
        };
    }
    match relation.into_result() {
        Ok(result) => ClickHouseDagOutcome::Accelerated(result),
        Err(error) => {
            ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::ResultEncoding(error.to_string()))
        }
    }
}

fn validate_reachable(entry: &QueryPlanEntry, root: QueryNodeId) -> Result<(), String> {
    let mut pending = vec![root];
    let mut reachable = BTreeSet::new();
    while let Some(id) = pending.pop() {
        if !reachable.insert(id) {
            continue;
        }
        let node = entry
            .nodes
            .get(&id)
            .ok_or_else(|| format!("published DAG references missing node {}", id.0))?;
        pending.extend(node.inputs());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::complete_pane_coverage;
    #[test]
    fn exact_accumulator_window_end_coverage_includes_its_pane_start() {
        assert!(complete_pane_coverage(
            Some((1_000, 2_000)),
            (0, 2_000),
            1_000
        ));
        assert!(!complete_pane_coverage(
            Some((2_000, 2_000)),
            (0, 2_000),
            1_000
        ));
    }
}
