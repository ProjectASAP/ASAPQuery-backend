use asap_types::executable_plan::{BackendExecutableBinding, BackendNodeBinding};
use planner_types::post_asap::PostAsapNodeId;
use planner_types::post_asap::{
    EdgeRole, ExecutableDag, ExecutableDagNode, ExecutableOperatorPayload, ExecutionDataState,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MaterializationCommitKey {
    pub plan_id: u64,
    pub plan_version: u64,
    pub stored_output: asap_types::sds::StoredOutputId,
    pub window_start_ms: i64,
    pub window_end_ms: i64,
    /// Producer lineage identity, including source and immutable input payload.
    /// Production uses a domain-separated SHA-256 digest to avoid retaining
    /// another full copy of every source summary.
    pub input_lineage: Vec<u8>,
}

pub trait PrecomputeOperatorRegistry<V> {
    type Error;
    /// A materialized input is an execution frontier: its absorbed semantic
    /// dependencies have already run and must not be evaluated again.
    fn materialized_input(&self, _node: &ExecutableDagNode) -> Result<Option<V>, Self::Error> {
        Ok(None)
    }
    fn execute(&self, node: &ExecutableDagNode, inputs: &[Arc<V>]) -> Result<V, Self::Error>;
}

/// Atomic persistence boundary. Implementations must return the already
/// committed value when the same lineage key is replayed after a retry or
/// restart, and must never publish two values for one key.
pub trait IdempotentCommitSink<V> {
    type Error;
    fn get(&self, key: &MaterializationCommitKey) -> Result<Option<Arc<V>>, Self::Error>;
    fn commit_if_absent(
        &self,
        key: MaterializationCommitKey,
        value: Arc<V>,
    ) -> Result<Arc<V>, Self::Error>;
}

#[derive(Debug)]
pub enum ScheduleError<OperatorError, SinkError> {
    Invalid(String),
    Operator(OperatorError),
    Sink(SinkError),
}

/// Execute one precompute sink and its transitive dependencies in topological
/// order. Intermediates use `Arc`, so a shared upstream node is computed once
/// without copying summary payloads. Only the sink is committed; upstream
/// materialization sinks are committed by their own lineage-keyed invocation.
pub fn execute_precompute_sink<V, R, S>(
    dag: &ExecutableDag,
    binding: &BackendExecutableBinding,
    sink_node: PostAsapNodeId,
    key: MaterializationCommitKey,
    registry: &R,
    sink: &S,
) -> Result<Arc<V>, ScheduleError<R::Error, S::Error>>
where
    R: PrecomputeOperatorRegistry<V>,
    S: IdempotentCommitSink<V>,
{
    if !matches!(binding.node(sink_node), Some(BackendNodeBinding::Materialization { stored_output }) if *stored_output == key.stored_output)
    {
        return Err(ScheduleError::Invalid(format!(
            "commit key materialization {:?} does not match sink {}",
            key.stored_output, sink_node.0
        )));
    }
    binding
        .validate_maintenance(dag)
        .map_err(ScheduleError::Invalid)?;
    if !binding.precompute_sinks.contains(&sink_node)
        || !matches!(
            binding.node(sink_node),
            Some(BackendNodeBinding::Materialization { .. })
        )
    {
        return Err(ScheduleError::Invalid(
            "node is not a precompute sink".into(),
        ));
    }
    if let Some(committed) = sink.get(&key).map_err(ScheduleError::Sink)? {
        return Ok(committed);
    }
    let nodes = dag
        .nodes
        .iter()
        .map(|n| (n.id.0, n))
        .collect::<BTreeMap<_, _>>();
    let mut incoming = BTreeMap::<u32, Vec<_>>::new();
    for edge in &dag.edges {
        incoming.entry(edge.consumer.0).or_default().push(edge);
    }
    for node in &dag.nodes {
        if matches!(node.payload, ExecutableOperatorPayload::Binary { .. }) {
            incoming.entry(node.id.0).or_default();
        }
    }
    let mut inputs = BTreeMap::<u32, Vec<u32>>::new();
    for (consumer, edges) in incoming {
        let ordered = if nodes
            .get(&consumer)
            .is_some_and(|node| matches!(node.payload, ExecutableOperatorPayload::Binary { .. }))
        {
            // Wire order is not operand order. Preserve noncommutative binary
            // semantics even when a valid transport reorders its edge list.
            let left = edges
                .iter()
                .filter(|edge| edge.role == EdgeRole::Left)
                .collect::<Vec<_>>();
            let right = edges
                .iter()
                .filter(|edge| edge.role == EdgeRole::Right)
                .collect::<Vec<_>>();
            if edges.len() != 2 || left.len() != 1 || right.len() != 1 {
                return Err(ScheduleError::Invalid(
                    "binary maintenance input roles must be exactly Left and Right".into(),
                ));
            }
            vec![left[0].producer.0, right[0].producer.0]
        } else {
            edges.iter().map(|edge| edge.producer.0).collect()
        };
        inputs.insert(consumer, ordered);
    }
    let mut active = BTreeSet::new();
    let mut values = BTreeMap::<u32, Arc<V>>::new();
    fn visit<V, R, OE, SE>(
        id: u32,
        nodes: &BTreeMap<u32, &ExecutableDagNode>,
        inputs: &BTreeMap<u32, Vec<u32>>,
        active: &mut BTreeSet<u32>,
        values: &mut BTreeMap<u32, Arc<V>>,
        registry: &R,
    ) -> Result<(), ScheduleError<OE, SE>>
    where
        R: PrecomputeOperatorRegistry<V, Error = OE>,
    {
        if values.contains_key(&id) {
            return Ok(());
        }
        if !active.insert(id) {
            return Err(ScheduleError::Invalid("precompute subDAG cycle".into()));
        }
        let node = nodes
            .get(&id)
            .ok_or_else(|| ScheduleError::Invalid(format!("missing node {id}")))?;
        if node.output_state == ExecutionDataState::QUERY_ROWS {
            return Err(ScheduleError::Invalid(format!(
                "query-time node {id} in precompute dependency path"
            )));
        }
        if let Some(value) = registry
            .materialized_input(node)
            .map_err(ScheduleError::Operator)?
        {
            values.insert(id, Arc::new(value));
            active.remove(&id);
            return Ok(());
        }
        let child_ids = inputs.get(&id).cloned().unwrap_or_default();
        for child in &child_ids {
            visit(*child, nodes, inputs, active, values, registry)?;
        }
        let child_values = child_ids
            .iter()
            .map(|child| Arc::clone(&values[child]))
            .collect::<Vec<_>>();
        let value = registry
            .execute(node, &child_values)
            .map_err(ScheduleError::Operator)?;
        values.insert(id, Arc::new(value));
        active.remove(&id);
        Ok(())
    }
    visit(
        sink_node.0,
        &nodes,
        &inputs,
        &mut active,
        &mut values,
        registry,
    )?;
    sink.commit_if_absent(key, values.remove(&sink_node.0).unwrap())
        .map_err(ScheduleError::Sink)
}

#[cfg(test)]
mod tests {
    use super::*;
    use planner_types::post_asap::{EdgeRole, ExecutionDataState};
    use planner_types::post_asap::{
        ExecutableDagEdge, ExecutableOperatorPayload, GroupingEdgeCompatibility, SummarySchema,
        WindowEdgeCompatibility,
    };
    use std::sync::Mutex;

    fn binding() -> BackendExecutableBinding {
        BackendExecutableBinding {
            nodes: (0..4)
                .map(|id| {
                    (
                        PostAsapNodeId(id),
                        BackendNodeBinding::Materialization {
                            stored_output: asap_types::PolicyFingerprint(u64::from(id) + 1).into(),
                        },
                    )
                })
                .chain([(
                    PostAsapNodeId(4),
                    BackendNodeBinding::Query {
                        query_node: asap_types::query_plan::QueryNodeId(9),
                    },
                )])
                .collect(),
            query_sink: PostAsapNodeId(4),
            query_plan_sink: asap_types::query_plan::QueryNodeId(9),
            precompute_sinks: vec![PostAsapNodeId(3)],
        }
    }

    fn maintenance_only(
        mut dag: ExecutableDag,
        mut binding: BackendExecutableBinding,
        sink: PostAsapNodeId,
    ) -> (ExecutableDag, BackendExecutableBinding) {
        let retained = dag
            .nodes
            .iter()
            .filter(|node| {
                node.output_state.timing == planner_types::post_asap::ExecutionTiming::IngestionTime
            })
            .map(|node| node.id)
            .collect::<std::collections::BTreeSet<_>>();
        dag.nodes.retain(|node| retained.contains(&node.id));
        dag.edges
            .retain(|edge| retained.contains(&edge.producer) && retained.contains(&edge.consumer));
        binding.nodes.retain(|id, _| retained.contains(id));
        dag.root = sink;
        (dag, binding)
    }

    fn node(id: u32) -> ExecutableDagNode {
        ExecutableDagNode {
            id: PostAsapNodeId(id),
            payload: ExecutableOperatorPayload::SummarySubtract,
            output_state: ExecutionDataState::INGESTION_SUMMARY,
            output_schema: SummarySchema {
                fields: Vec::new(),
                time_index: None,
            },
            guarantee: None,
        }
    }

    fn edge(producer: u32, consumer: u32) -> ExecutableDagEdge {
        ExecutableDagEdge {
            producer: PostAsapNodeId(producer),
            consumer: PostAsapNodeId(consumer),
            role: EdgeRole::Input,
            intermediate_schema: SummarySchema {
                fields: Vec::new(),
                time_index: None,
            },
            data_state: ExecutionDataState::INGESTION_SUMMARY,
            grouping: GroupingEdgeCompatibility::Identical,
            window: WindowEdgeCompatibility::NotApplicable,
        }
    }

    #[derive(Default)]
    struct Registry(Mutex<BTreeMap<u32, usize>>);

    impl PrecomputeOperatorRegistry<u32> for Registry {
        type Error = String;

        fn execute(
            &self,
            node: &ExecutableDagNode,
            inputs: &[Arc<u32>],
        ) -> Result<u32, Self::Error> {
            *self.0.lock().unwrap().entry(node.id.0).or_default() += 1;
            Ok(node.id.0 + inputs.iter().map(|v| **v).sum::<u32>())
        }
    }

    #[derive(Default)]
    struct Sink(Mutex<BTreeMap<MaterializationCommitKey, Arc<u32>>>);

    impl IdempotentCommitSink<u32> for Sink {
        type Error = String;

        fn get(&self, key: &MaterializationCommitKey) -> Result<Option<Arc<u32>>, Self::Error> {
            Ok(self.0.lock().unwrap().get(key).cloned())
        }

        fn commit_if_absent(
            &self,
            key: MaterializationCommitKey,
            value: Arc<u32>,
        ) -> Result<Arc<u32>, Self::Error> {
            Ok(Arc::clone(
                self.0.lock().unwrap().entry(key).or_insert(value),
            ))
        }
    }

    fn key(node_id: u32) -> MaterializationCommitKey {
        MaterializationCommitKey {
            plan_id: 7,
            plan_version: 1,
            stored_output: asap_types::PolicyFingerprint(u64::from(node_id) + 1).into(),
            window_start_ms: 10,
            window_end_ms: 20,
            input_lineage: b"checkpoint:3".to_vec(),
        }
    }

    #[test]
    fn binary_operand_roles_survive_edge_reordering_and_reject_duplicates() {
        use planner_types::post_asap::BinaryOperator;
        use planner_types::pre_asap::{ArithmeticOpKind, BinaryOpKind};
        struct Subtract;
        impl PrecomputeOperatorRegistry<u32> for Subtract {
            type Error = String;
            fn execute(
                &self,
                node: &ExecutableDagNode,
                inputs: &[Arc<u32>],
            ) -> Result<u32, String> {
                match node.id.0 {
                    0 => Ok(10),
                    1 => Ok(3),
                    3 => Ok(*inputs[0] - *inputs[1]),
                    _ => Err("unexpected node".into()),
                }
            }
        }
        let mut binary = node(3);
        binary.payload = ExecutableOperatorPayload::Binary {
            operator: BinaryOperator {
                checked_relative_division: false,
                checked_finite_division: false,
                kind: BinaryOpKind::Arithmetic(ArithmeticOpKind::Sub),
                vector_match: None,
            },
        };
        let mut left = edge(0, 3);
        left.role = EdgeRole::Left;
        let mut right = edge(1, 3);
        right.role = EdgeRole::Right;
        let mut dag = ExecutableDag {
            nodes: vec![node(0), node(1), node(2), binary, {
                let mut query = node(4);
                query.output_state = ExecutionDataState::QUERY_ROWS;
                query
            }],
            edges: vec![right, left],
            root: PostAsapNodeId(3),
        };
        let execute = |dag: &ExecutableDag| {
            let (dag, binding) = maintenance_only(dag.clone(), binding(), PostAsapNodeId(3));
            execute_precompute_sink(
                &dag,
                &binding,
                PostAsapNodeId(3),
                key(3),
                &Subtract,
                &Sink::default(),
            )
        };
        assert_eq!(*execute(&dag).unwrap(), 7);
        dag.edges.reverse();
        assert_eq!(*execute(&dag).unwrap(), 7);
        dag.edges[1].role = EdgeRole::Left;
        assert!(matches!(execute(&dag), Err(ScheduleError::Invalid(_))));
        dag.edges.pop();
        assert!(matches!(execute(&dag), Err(ScheduleError::Invalid(_))));
        dag.edges.clear();
        assert!(matches!(execute(&dag), Err(ScheduleError::Invalid(_))));
    }

    #[test]
    fn shared_dependency_executes_once_and_replay_reads_committed_value() {
        // 0 is shared by 1 and 2; 3 consumes both branches.
        let dag = ExecutableDag {
            nodes: (0..4)
                .map(node)
                .chain([{
                    let mut query = node(4);
                    query.output_state = ExecutionDataState::QUERY_ROWS;
                    query
                }])
                .collect(),
            edges: vec![edge(0, 1), edge(0, 2), edge(1, 3), edge(2, 3), edge(3, 4)],
            root: PostAsapNodeId(4),
        };
        let registry = Registry::default();
        let sink = Sink::default();
        let (dag, binding) = maintenance_only(dag, binding(), PostAsapNodeId(3));
        let first =
            execute_precompute_sink(&dag, &binding, PostAsapNodeId(3), key(3), &registry, &sink)
                .unwrap();
        assert_eq!(*first, 6);
        assert_eq!(registry.0.lock().unwrap().values().sum::<usize>(), 4);

        let replay =
            execute_precompute_sink(&dag, &binding, PostAsapNodeId(3), key(3), &registry, &sink)
                .unwrap();
        assert!(Arc::ptr_eq(&first, &replay));
        assert_eq!(registry.0.lock().unwrap().values().sum::<usize>(), 4);
    }

    #[test]
    fn supplied_materialization_cuts_absorbed_dependencies_and_is_shared() {
        struct FrontierRegistry(Registry);
        impl PrecomputeOperatorRegistry<u32> for FrontierRegistry {
            type Error = String;
            fn materialized_input(&self, node: &ExecutableDagNode) -> Result<Option<u32>, String> {
                Ok((node.id == PostAsapNodeId(1)).then_some(10))
            }
            fn execute(
                &self,
                node: &ExecutableDagNode,
                inputs: &[Arc<u32>],
            ) -> Result<u32, String> {
                assert_ne!(
                    node.id,
                    PostAsapNodeId(0),
                    "absorbed source subtree must not execute"
                );
                self.0.execute(node, inputs)
            }
        }
        let mut raw = node(0);
        raw.output_state = ExecutionDataState::QUERY_ROWS;
        let mut query = node(4);
        query.output_state = ExecutionDataState::QUERY_ROWS;
        let dag = ExecutableDag {
            nodes: vec![raw, node(1), node(2), node(3), query],
            edges: vec![edge(0, 1), edge(1, 2), edge(1, 3), edge(2, 3), edge(3, 4)],
            root: PostAsapNodeId(4),
        };
        let mut bindings = binding();
        bindings
            .nodes
            .insert(PostAsapNodeId(0), BackendNodeBinding::QueryInput);
        let (dag, bindings) = maintenance_only(dag, bindings, PostAsapNodeId(3));
        let registry = FrontierRegistry(Registry::default());
        let sink = Sink::default();
        let result =
            execute_precompute_sink(&dag, &bindings, PostAsapNodeId(3), key(3), &registry, &sink)
                .unwrap();
        assert_eq!(*result, 25);
        assert_eq!(
            *registry.0 .0.lock().unwrap(),
            BTreeMap::from([(2, 1), (3, 1)])
        );
    }

    #[test]
    fn rejects_query_node_in_precompute_path_and_mismatched_lineage_key() {
        let mut query_child = node(0);
        query_child.output_state = ExecutionDataState::QUERY_ROWS;
        let dag = ExecutableDag {
            nodes: vec![query_child, node(1)],
            edges: vec![edge(0, 1)],
            root: PostAsapNodeId(0),
        };
        let registry = Registry::default();
        let sink = Sink::default();
        let invalid_path_binding = BackendExecutableBinding {
            nodes: [
                (
                    PostAsapNodeId(0),
                    BackendNodeBinding::Query {
                        query_node: asap_types::query_plan::QueryNodeId(1),
                    },
                ),
                (
                    PostAsapNodeId(1),
                    BackendNodeBinding::Materialization {
                        stored_output: asap_types::PolicyFingerprint(2).into(),
                    },
                ),
            ]
            .into_iter()
            .collect(),
            query_sink: PostAsapNodeId(0),
            query_plan_sink: asap_types::query_plan::QueryNodeId(1),
            precompute_sinks: vec![PostAsapNodeId(1)],
        };
        assert!(matches!(
            execute_precompute_sink(&dag, &invalid_path_binding, PostAsapNodeId(1), key(1), &registry, &sink),
            Err(ScheduleError::Invalid(message)) if message.contains("query-owned node")
        ));
        assert!(matches!(
            execute_precompute_sink(&dag, &invalid_path_binding, PostAsapNodeId(1), key(0), &registry, &sink),
            Err(ScheduleError::Invalid(message)) if message.contains("does not match")
        ));
    }
}
