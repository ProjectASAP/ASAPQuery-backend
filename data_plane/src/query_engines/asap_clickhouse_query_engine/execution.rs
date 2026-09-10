//! SQL boundary around the shared, compiler-bound physical DAG executor.
use super::{
    clickhouse_result_adapter::{from_series_rows, ClickHouseQueryResult},
    relational_adapter::{ClickHouseRelation, ClickHouseRelationalAdapter},
};
use crate::{
    query_engines::asap_query_engine::{
        catalog_resolver::validate_payload, post_asap_readout::execute_query_plan_payload_readout,
    },
    storage_engines::sketch_db::index::SketchStore,
};
use asap_types::summary_catalog::SummaryCatalog;
use control_plane::query_plan::{ExecutableQueryPlan, QueryNodeId, QueryPlanNode};
use planner_types::post_asap::ValueOperation;
use std::collections::{BTreeMap, BTreeSet};

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
    entry: &ExecutableQueryPlan,
    sds: &SummaryCatalog,
    t0_ms: u64,
    t1_ms: u64,
    is_cumulative: bool,
) -> ClickHouseDagOutcome {
    if let Err(error) = validate_payload(Some(sds), entry, sds.plan_id, sds.plan_version) {
        return ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::UnsupportedPlan(
            error.to_string(),
        ));
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
    let executable = match reachable_entry(entry, base_root) {
        Ok(entry) => entry,
        Err(detail) => {
            return ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::UnsupportedPlan(detail))
        }
    };
    let outcome =
        match execute_query_plan_payload_readout(index, &executable, t0_ms, t1_ms, is_cumulative) {
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

fn reachable_entry(
    entry: &ExecutableQueryPlan,
    root: QueryNodeId,
) -> Result<ExecutableQueryPlan, String> {
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
    let nodes = reachable
        .into_iter()
        .map(|id| (id, entry.nodes[&id].clone()))
        .collect::<BTreeMap<_, _>>();
    Ok(ExecutableQueryPlan {
        root,
        nodes,
        ..entry.clone()
    })
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
