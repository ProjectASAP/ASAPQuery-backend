//! Relational query-time operators over the shared summary executor.
//!
//! The legacy executor intentionally keeps `Value` opaque. This companion
//! adapter lets a language runtime interpret planner-owned `ValueOperation`
//! nodes without widening that trait or changing existing PromQL behavior.

use planner_types::post_asap::{SummaryExpr, SummaryNode, ValueOperation};

use super::executor::{execute, ExecError, ExecOutcome, SummaryExecutor};

pub trait RelationalAdapter<E: SummaryExecutor> {
    type Relation;
    type Error;

    fn relation_from_outcome(
        &self,
        node: &SummaryNode,
        outcome: ExecOutcome<E>,
    ) -> Result<Self::Relation, Self::Error>;

    fn apply(
        &self,
        node: &SummaryNode,
        operation: &ValueOperation,
        input: Self::Relation,
    ) -> Result<Self::Relation, Self::Error>;
}

#[derive(Debug)]
pub enum RelationalExecError<ExecutorError, AdapterError> {
    Executor(ExecError<ExecutorError>),
    Adapter(AdapterError),
}

/// Recursively evaluates query-time relational nodes and delegates every
/// summary/state node to the unchanged shared summary executor.
pub fn execute_relational<E, A>(
    node: &SummaryNode,
    executor: &E,
    adapter: &A,
) -> Result<A::Relation, RelationalExecError<E::Error, A::Error>>
where
    E: SummaryExecutor,
    A: RelationalAdapter<E>,
{
    match &node.expr {
        SummaryExpr::ValueOperation {
            child, operation, ..
        } => {
            let input = execute_relational(child, executor, adapter)?;
            adapter
                .apply(node, operation, input)
                .map_err(RelationalExecError::Adapter)
        }
        _ => {
            let outcome = execute(node, executor).map_err(RelationalExecError::Executor)?;
            adapter
                .relation_from_outcome(node, outcome)
                .map_err(RelationalExecError::Adapter)
        }
    }
}
