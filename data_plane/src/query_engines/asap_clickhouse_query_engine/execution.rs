//! SQL boundary around the shared post-ASAP executor.
use super::clickhouse_result_adapter::ClickHouseQueryResult;
use super::relational_adapter::ClickHouseRelationalAdapter;
use crate::{
    query_engines::{
        asap_query_engine::summary_executor::SummaryExecutorError,
        canonical::{
            context::QueryExecutionContext,
            executor::ExecError,
            relational::{execute_relational, RelationalExecError},
        },
    },
    storage_engines::sketch_db::index::SketchStore,
};
use asap_types::PolicyFingerprint;
use planner_types::post_asap::SummaryNode;
use std::collections::BTreeSet;

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

/// Executes summary nodes with the shared executor and evaluates the supported
/// planner-owned SQL relational wrappers over the resulting rows.
pub fn execute_sql_dag(
    index: &SketchStore,
    node: &SummaryNode,
    t0_ms: u64,
    t1_ms: u64,
    is_cumulative: bool,
    allowed_materializations: BTreeSet<PolicyFingerprint>,
) -> ClickHouseDagOutcome {
    let ctx = QueryExecutionContext {
        index,
        t0_ms,
        t1_ms,
        is_cumulative,
        allowed_materializations: Some(allowed_materializations),
    };
    match execute_relational(node, &ctx, &ClickHouseRelationalAdapter) {
        Err(RelationalExecError::Executor(ExecError::NoCandidates))
        | Err(RelationalExecError::Executor(ExecError::Executor(
            SummaryExecutorError::NoCandidates,
        ))) => ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::NoCandidates),
        Err(error) => ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::UnsupportedPlan(
            format!("{error:?}"),
        )),
        Ok(relation) => {
            if relation.coverage != Some((t0_ms, t1_ms)) {
                return ClickHouseDagOutcome::Fallback(ClickHouseDagFallback::IncompleteCoverage {
                    requested: (t0_ms, t1_ms),
                    observed: relation.coverage,
                });
            }
            match relation.into_result() {
                Ok(result) => ClickHouseDagOutcome::Accelerated(result),
                Err(error) => ClickHouseDagOutcome::Fallback(
                    ClickHouseDagFallback::ResultEncoding(error.to_string()),
                ),
            }
        }
    }
}
