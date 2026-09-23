//! Graph traversal for an installed physical QueryPlan.
//!
//! This module owns dependency ordering and memoization only. Physical node
//! definitions live in `asap_types`; store and operator semantics are
//! supplied by a runtime adapter.

use std::collections::BTreeMap;

use asap_types::query_plan::{QueryNodeId, QueryPlanEntry, QueryPlanNode};
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

/// Async counterpart used when ordinary DAG leaves perform external exact
/// reads. Keeping I/O in the node adapter lets the graph scheduler preserve
/// the same dependency ordering and memoization as local summary nodes.
#[async_trait::async_trait]
pub trait AsyncQueryNodeRuntime {
    type Output: Clone + Send;
    type Error;

    async fn execute_node(
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
    #[error("query `{query_id}` node {node_id} failed")]
    Node {
        query_id: String,
        node_id: u64,
        source: E,
    },
}

/// Execute each reachable node exactly once. A diamond-shaped DAG therefore
/// performs one store read for the shared leaf, not one read per parent path.
pub fn execute<R: QueryNodeRuntime>(
    entry: &QueryPlanEntry,
    runtime: &R,
) -> Result<R::Output, DagExecutionError<R::Error>> {
    execute_from(entry, entry.root, runtime)
}

pub fn execute_from<R: QueryNodeRuntime>(
    entry: &QueryPlanEntry,
    root: QueryNodeId,
    runtime: &R,
) -> Result<R::Output, DagExecutionError<R::Error>> {
    let order = entry.topological_order_from(root).map_err(|error| {
        DagExecutionError::InvalidGraph(format!("query `{}`: {error}", entry.query_id))
    })?;
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
                    query_id: entry.query_id.clone(),
                    node_id: id.0,
                    source,
                })?;
        outputs.insert(id, output);
    }
    outputs.remove(&root).ok_or_else(|| {
        DagExecutionError::InvalidGraph(format!("root {} produced no output", root.0))
    })
}

pub async fn execute_async<R: AsyncQueryNodeRuntime + Sync>(
    entry: &QueryPlanEntry,
    runtime: &R,
) -> Result<R::Output, DagExecutionError<R::Error>> {
    execute_from_async(entry, entry.root, runtime).await
}

pub async fn execute_from_async<R: AsyncQueryNodeRuntime + Sync>(
    entry: &QueryPlanEntry,
    root: QueryNodeId,
    runtime: &R,
) -> Result<R::Output, DagExecutionError<R::Error>> {
    let order = entry.topological_order_from(root).map_err(|error| {
        DagExecutionError::InvalidGraph(format!("query `{}`: {error}", entry.query_id))
    })?;
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
        let output = runtime
            .execute_node(id, node, &inputs)
            .await
            .map_err(|source| DagExecutionError::Node {
                query_id: entry.query_id.clone(),
                node_id: id.0,
                source,
            })?;
        outputs.insert(id, output);
    }
    outputs.remove(&root).ok_or_else(|| {
        DagExecutionError::InvalidGraph(format!("root {} produced no output", root.0))
    })
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::BTreeMap;

    use asap_types::query_plan::{FallbackPolicy, InstantExecution, QueryReadout};

    use super::*;

    struct CountingRuntime(RefCell<BTreeMap<QueryNodeId, usize>>);

    struct FailingRuntime;

    impl QueryNodeRuntime for FailingRuntime {
        type Output = usize;
        type Error = &'static str;

        fn execute_node(
            &self,
            _id: QueryNodeId,
            _node: &QueryPlanNode,
            _inputs: &[usize],
        ) -> Result<usize, Self::Error> {
            Err("broken read")
        }
    }

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
            language: asap_types::query_plan::QueryLanguage::PromQl,
            query_id: "q".into(),
            canonical_query: "up".into(),
            fixed_evaluation: None,
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

    #[test]
    fn node_failure_identifies_the_installed_query_and_node() {
        let entry = QueryPlanEntry {
            language: asap_types::query_plan::QueryLanguage::PromQl,
            query_id: "latency-p50".into(),
            canonical_query: "latency".into(),
            fixed_evaluation: None,
            root: QueryNodeId(7),
            nodes: BTreeMap::from([(
                QueryNodeId(7),
                QueryPlanNode::ExactFallback {
                    reason: "fixture".into(),
                },
            )]),
            instant: InstantExecution {
                lookback_ms: 0,
                full_history: false,
                cumulative_readout: false,
            },
            fallback: FallbackPolicy::Reject,
        };

        let error = execute(&entry, &FailingRuntime).unwrap_err().to_string();
        assert!(error.contains("query `latency-p50` node 7 failed"));
    }

    struct AsyncCountingRuntime(tokio::sync::Mutex<BTreeMap<QueryNodeId, usize>>);

    #[async_trait::async_trait]
    impl AsyncQueryNodeRuntime for AsyncCountingRuntime {
        type Output = usize;
        type Error = std::convert::Infallible;

        async fn execute_node(
            &self,
            id: QueryNodeId,
            _node: &QueryPlanNode,
            inputs: &[usize],
        ) -> Result<usize, Self::Error> {
            *self.0.lock().await.entry(id).or_default() += 1;
            Ok(1 + inputs.iter().sum::<usize>())
        }
    }

    #[tokio::test]
    async fn async_runtime_preserves_shared_dependency_memoization() {
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
            language: asap_types::query_plan::QueryLanguage::PromQl,
            query_id: "q".into(),
            canonical_query: "up".into(),
            fixed_evaluation: None,
            root,
            nodes,
            instant: InstantExecution {
                lookback_ms: 0,
                full_history: false,
                cumulative_readout: false,
            },
            fallback: FallbackPolicy::Reject,
        };
        let runtime = AsyncCountingRuntime(tokio::sync::Mutex::new(BTreeMap::new()));
        assert_eq!(execute_async(&entry, &runtime).await.unwrap(), 5);
        assert!(runtime.0.lock().await.values().all(|count| *count == 1));
    }
}
