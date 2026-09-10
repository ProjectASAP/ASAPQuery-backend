//! Graph traversal for an installed physical QueryPlan.
//!
//! This module owns dependency ordering and memoization only. Physical node
//! definitions live in `control_plane`; store and operator semantics are
//! supplied by a runtime adapter.

use std::collections::BTreeMap;

use control_plane::query_plan::{QueryNodeId, QueryPlanEntry, QueryPlanNode};
use thiserror::Error;

pub trait QueryNodeRuntime {
    type Output: Clone;
    type Error;

    fn execute_node(
        &self,
        id: QueryNodeId,
        node: &QueryPlanNode,
        inputs: &[Self::Output],
    ) -> Result<Self::Output, Self::Error>;
}

#[derive(Debug, Error)]
pub enum DagExecutionError<E> {
    #[error("invalid physical query graph: {0}")]
    InvalidGraph(String),
    #[error("query node {node_id} failed")]
    Node { node_id: u64, source: E },
}

/// Execute each reachable node exactly once. A diamond-shaped DAG therefore
/// performs one store read for the shared leaf, not one read per parent path.
pub fn execute<R: QueryNodeRuntime>(
    entry: &QueryPlanEntry,
    runtime: &R,
) -> Result<R::Output, DagExecutionError<R::Error>> {
    let order = entry
        .topological_order()
        .map_err(|error| DagExecutionError::InvalidGraph(error.to_string()))?;
    let mut outputs = BTreeMap::<QueryNodeId, R::Output>::new();
    for id in order {
        let node = entry
            .nodes
            .get(&id)
            .ok_or_else(|| DagExecutionError::InvalidGraph(format!("missing node {}", id.0)))?;
        let inputs = node
            .inputs()
            .iter()
            .map(|input| {
                outputs.get(input).cloned().ok_or_else(|| {
                    DagExecutionError::InvalidGraph(format!(
                        "node {} ran before input {}",
                        id.0, input.0
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let output =
            runtime
                .execute_node(id, node, &inputs)
                .map_err(|source| DagExecutionError::Node {
                    node_id: id.0,
                    source,
                })?;
        outputs.insert(id, output);
    }
    outputs.remove(&entry.root).ok_or_else(|| {
        DagExecutionError::InvalidGraph(format!("root {} produced no output", entry.root.0))
    })
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::BTreeMap;

    use control_plane::query_plan::{FallbackPolicy, InstantExecution, QueryReadout};

    use super::*;

    struct CountingRuntime(RefCell<BTreeMap<QueryNodeId, usize>>);

    impl QueryNodeRuntime for CountingRuntime {
        type Output = usize;
        type Error = std::convert::Infallible;

        fn execute_node(
            &self,
            id: QueryNodeId,
            _node: &QueryPlanNode,
            inputs: &[usize],
        ) -> Result<usize, Self::Error> {
            *self.0.borrow_mut().entry(id).or_default() += 1;
            Ok(1 + inputs.iter().sum::<usize>())
        }
    }

    #[test]
    fn shared_node_is_executed_once() {
        let shared = QueryNodeId(0);
        let left = QueryNodeId(1);
        let right = QueryNodeId(2);
        let root = QueryNodeId(3);
        let nodes = [
            (
                shared,
                QueryPlanNode::ExactFallback {
                    reason: "leaf".into(),
                },
            ),
            (
                left,
                QueryPlanNode::SummaryEstimate {
                    input: shared,
                    query: QueryReadout::Cardinality,
                },
            ),
            (
                right,
                QueryPlanNode::SummaryEstimate {
                    input: shared,
                    query: QueryReadout::Cardinality,
                },
            ),
            (
                root,
                QueryPlanNode::SummaryMerge {
                    inputs: vec![left, right],
                },
            ),
        ]
        .into_iter()
        .collect();
        let entry = QueryPlanEntry {
            language: control_plane::query_plan::QueryLanguage::PromQl,
            query_id: "q".into(),
            canonical_query: "up".into(),
            root,
            nodes,
            instant: InstantExecution {
                lookback_ms: 0,
                full_history: false,
                cumulative_readout: false,
            },
            fallback: FallbackPolicy::Reject,
        };
        let runtime = CountingRuntime(RefCell::new(BTreeMap::new()));
        assert_eq!(execute(&entry, &runtime).unwrap(), 5);
        assert!(runtime.0.borrow().values().all(|count| *count == 1));
    }
}
