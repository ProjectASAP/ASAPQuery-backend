use asap_physical_operators::dag as execution;
use asap_types::executable_plan::{BackendExecutableBinding, BackendNodeBinding};
use futures::StreamExt;
use planner_types::post_asap::PostAsapNodeId;
use planner_types::post_asap::{
    EdgeRole, ExecutableDag, ExecutableDagNode, ExecutableOperatorPayload, ExecutionDataState,
};
use std::{cell::RefCell, rc::Rc};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MaterializationCommitKey {
    pub plan_id: u64,
    pub plan_version: u64,
    pub summary_definition: asap_types::sds::SummaryDefinitionId,
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
    fn output_bytes(&self, _value: &V) -> usize {
        std::mem::size_of::<V>().max(1)
    }
    fn execute(
        &self,
        node: &ExecutableDagNode,
        inputs: &[Arc<V>],
        context: execution::RunContext,
    ) -> Result<V, Self::Error>;
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
    let mut outputs = execute_precompute_sinks(dag, binding, &[(sink_node, key)], registry, sink)?;
    Ok(outputs.remove(0))
}

fn node_syntax(payload: &ExecutableOperatorPayload) -> String {
    let details = match payload {
        ExecutableOperatorPayload::Fallback { .. } => String::new(),
        ExecutableOperatorPayload::Binary { timing, operator } => {
            format!("operator={operator:?} timing={timing:?}")
        }
        ExecutableOperatorPayload::MembershipFilter { .. } => String::new(),
        ExecutableOperatorPayload::Value { operation, timing } => {
            format!("operation={operation:?} timing={timing:?}")
        }
        ExecutableOperatorPayload::RelationalJoin { join_kind, .. } => {
            format!("join_kind={join_kind:?}")
        }
        ExecutableOperatorPayload::SummaryAgg {
            family,
            input,
            reduction,
            ..
        } => format!("family={family:?} input={input:?} reduction={reduction:?}"),
        ExecutableOperatorPayload::SummaryJoin { family, .. } => format!("family={family:?}"),
        ExecutableOperatorPayload::SummarySubtract => String::new(),
        ExecutableOperatorPayload::SummaryDelete { .. } => String::new(),
        ExecutableOperatorPayload::SummaryEstimate { query } => format!("readout={query:?}"),
        ExecutableOperatorPayload::SummaryMerge { .. } => String::new(),
    };
    details.chars().take(256).collect()
}

/// Evaluate all selected stored outputs with one dependency cache. Keys must
/// describe the same input revision and window; only their output identity may
/// differ. Validation finishes before executing or committing any output.
pub fn execute_precompute_sinks<V, R, S>(
    dag: &ExecutableDag,
    binding: &BackendExecutableBinding,
    outputs: &[(PostAsapNodeId, MaterializationCommitKey)],
    registry: &R,
    sink: &S,
) -> Result<Vec<Arc<V>>, ScheduleError<R::Error, S::Error>>
where
    R: PrecomputeOperatorRegistry<V>,
    S: IdempotentCommitSink<V>,
{
    let mut unique = BTreeSet::new();
    for (node, key) in outputs {
        if !unique.insert(node.0)
            || !binding.precompute_sinks.contains(node)
            || !matches!(binding.node(*node), Some(BackendNodeBinding::Materialization { summary_definition }) if *summary_definition == key.summary_definition)
        {
            return Err(ScheduleError::Invalid(
                "commit key does not match a unique stored output binding".into(),
            ));
        }
        let first = &outputs[0].1;
        if (
            key.plan_id,
            key.plan_version,
            key.window_start_ms,
            key.window_end_ms,
            &key.input_lineage,
        ) != (
            first.plan_id,
            first.plan_version,
            first.window_start_ms,
            first.window_end_ms,
            &first.input_lineage,
        ) {
            return Err(ScheduleError::Invalid(
                "stored outputs require one evaluation window and input revision".into(),
            ));
        }
    }
    binding
        .validate_maintenance(dag)
        .map_err(ScheduleError::Invalid)?;
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
    if outputs.is_empty() {
        return Ok(Vec::new());
    }
    let error = Rc::new(RefCell::new(None));
    let mut sources = BTreeMap::new();
    for (node, key) in outputs {
        if let Some(value) = sink.get(key).map_err(ScheduleError::Sink)? {
            sources.insert(node.0, value);
        }
    }
    let committed = sources.keys().copied().collect::<BTreeSet<_>>();
    let mut graph = execution::PhysicalDag::default();
    let mut pending = outputs.iter().map(|(id, _)| id.0).collect::<Vec<_>>();
    let mut added = BTreeSet::new();
    while let Some(id) = pending.pop() {
        if !added.insert(id) {
            continue;
        }
        let node = *nodes
            .get(&id)
            .ok_or_else(|| ScheduleError::Invalid(format!("missing node {id}")))?;
        if node.output_state.timing == planner_types::post_asap::ExecutionTiming::QueryTime {
            return Err(ScheduleError::Invalid(format!(
                "query-time node {id} in precompute dependency path"
            )));
        }
        let source = if let Some(value) = sources.remove(&id) {
            Some(value)
        } else {
            registry
                .materialized_input(node)
                .map_err(ScheduleError::Operator)?
                .map(Arc::new)
        };
        let children = if source.is_some() {
            vec![]
        } else {
            inputs.get(&id).cloned().unwrap_or_default()
        };
        let schemas = children
            .iter()
            .map(|child| {
                nodes
                    .get(child)
                    .map(|n| n.output_schema.clone())
                    .ok_or_else(|| ScheduleError::Invalid(format!("missing node {child}")))
            })
            .collect::<Result<Vec<_>, _>>()?;
        pending.extend(children.iter().copied());
        graph
            .add(
                u64::from(id),
                children.into_iter().map(u64::from).collect(),
                IngestionOperator {
                    node,
                    registry,
                    source,
                    schemas,
                    error: error.clone(),
                },
            )
            .map_err(|e| ScheduleError::Invalid(e.to_string()))?;
    }
    let key = &outputs[0].1;
    let context = execution::RunContext::new(
        execution::Scope::Ingestion {
            window_start_ms: key.window_start_ms,
            window_end_ms: key.window_end_ms,
            revision: key.plan_version,
        },
        execution::Limits::default(),
    )
    .map_err(|e| ScheduleError::Invalid(e.to_string()))?;
    let roots = outputs
        .iter()
        .map(|(id, _)| u64::from(id.0))
        .collect::<Vec<_>>();
    let streams = graph
        .execute(&roots, context)
        .map_err(|e| ScheduleError::Invalid(e.to_string()))?;
    futures::executor::block_on(futures::future::try_join_all(
        streams
            .into_iter()
            .zip(outputs)
            .map(|(mut stream, (node, key))| {
                let error = &error;
                let committed = &committed;
                async move {
                    let result = stream.next().await.ok_or_else(|| {
                        ScheduleError::Invalid("ingestion root produced no value".into())
                    })?;
                    let value = match result {
                        Ok(value) => Arc::clone(value.value()),
                        Err(failure) => {
                            return Err(match error.borrow_mut().take() {
                                Some(error) => ScheduleError::Operator(error),
                                None => ScheduleError::Invalid(failure.to_string()),
                            })
                        }
                    };
                    if committed.contains(&node.0) {
                        tracing::debug!(target: "asap_runtime_debug", sink_node_id = node.0, "precompute DAG reused committed sink");
                        Ok(value)
                    } else {
                        let value = sink.commit_if_absent(key.clone(), value).map_err(ScheduleError::Sink)?;
                        tracing::debug!(target: "asap_runtime_debug", sink_node_id = node.0, "precompute DAG sink commit completed");
                        Ok(value)
                    }
                }
            }),
    ))
}

struct IngestionOperator<'a, V, R: PrecomputeOperatorRegistry<V>> {
    node: &'a ExecutableDagNode,
    registry: &'a R,
    source: Option<Arc<V>>,
    schemas: Vec<planner_types::post_asap::SummarySchema>,
    error: Rc<RefCell<Option<R::Error>>>,
}
impl<V, R: PrecomputeOperatorRegistry<V>>
    execution::PhysicalOperator<Arc<V>, planner_types::post_asap::SummarySchema>
    for IngestionOperator<'_, V, R>
{
    fn name(&self) -> &str {
        "InstalledIngestionOperator"
    }
    fn input_schemas(&self) -> Vec<planner_types::post_asap::SummarySchema> {
        self.schemas.clone()
    }
    fn output_schema(&self) -> planner_types::post_asap::SummarySchema {
        self.node.output_schema.clone()
    }
    fn output_bytes(&self, value: &Arc<V>) -> usize {
        self.registry.output_bytes(value)
    }
    fn start<'a>(
        &'a self,
        inputs: Vec<execution::Input<'a, Arc<V>>>,
        context: execution::RunContext,
    ) -> Result<execution::OutputStream<'a, Arc<V>>, execution::Error> {
        Ok(futures::stream::once(async move {
            if let Some(source) = &self.source {
                tracing::debug!(target: "asap_runtime_debug", node_id = self.node.id.0, "precompute node used materialized input");
                return Ok(Arc::clone(source));
            }
            let inputs =
                futures::future::try_join_all(inputs.into_iter().map(|mut input| async move {
                    input.next().await.ok_or_else(|| {
                        execution::Error::Operator("ingestion input produced no value".into())
                    })?
                }))
                .await?;
            let values = inputs
                .iter()
                .map(|value| Arc::clone(value.value()))
                .collect::<Vec<_>>();
            let started = std::time::Instant::now();
            tracing::debug!(target: "asap_runtime_debug", node_id = self.node.id.0, syntax = %node_syntax(&self.node.payload), "precompute node started");
            self.registry
                .execute(self.node, &values, context)
                .map(|value| {
                    tracing::debug!(target: "asap_runtime_debug", node_id = self.node.id.0, elapsed_us = started.elapsed().as_micros() as u64, "precompute node completed");
                    Arc::new(value)
                })
                .map_err(|e| {
                    tracing::warn!(node_id = self.node.id.0, elapsed_us = started.elapsed().as_micros() as u64, "precompute node failed");
                    *self.error.borrow_mut() = Some(e);
                    execution::Error::Operator(format!("ingestion node {} failed", self.node.id.0))
                })
        })
        .boxed_local())
    }
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
                            summary_definition: asap_types::PolicyFingerprint(u64::from(id) + 1)
                                .into(),
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
            _context: execution::RunContext,
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
            summary_definition: asap_types::PolicyFingerprint(u64::from(node_id) + 1).into(),
            window_start_ms: 10,
            window_end_ms: 20,
            input_lineage: b"checkpoint:3".to_vec(),
        }
    }

    // Two stored sinks in one evaluation reuse their shared upstream work.
    #[test]
    fn stored_sinks_share_one_evaluation() {
        let mut query = node(4);
        query.output_state = ExecutionDataState::QUERY_ROWS;
        let dag = ExecutableDag {
            nodes: vec![node(0), node(1), node(2), node(3), query],
            edges: vec![edge(0, 1), edge(1, 2), edge(1, 3)],
            root: PostAsapNodeId(4),
        };
        let mut bindings = binding();
        bindings.precompute_sinks = vec![PostAsapNodeId(2), PostAsapNodeId(3)];
        let (dag, bindings) = maintenance_only(dag, bindings, PostAsapNodeId(3));
        let registry = Registry::default();
        let sink = Sink::default();
        execute_precompute_sinks(
            &dag,
            &bindings,
            &[(PostAsapNodeId(2), key(2)), (PostAsapNodeId(3), key(3))],
            &registry,
            &sink,
        )
        .unwrap();
        assert_eq!(
            *registry.0.lock().unwrap(),
            BTreeMap::from([(0, 1), (1, 1), (2, 1), (3, 1)])
        );
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
                _context: execution::RunContext,
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
                context: execution::RunContext,
            ) -> Result<u32, String> {
                assert_ne!(
                    node.id,
                    PostAsapNodeId(0),
                    "absorbed source subtree must not execute"
                );
                self.0.execute(node, inputs, context)
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
                        summary_definition: asap_types::PolicyFingerprint(2).into(),
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
        let mut summary_dag = dag.clone();
        summary_dag.nodes[0].output_state.primitive =
            planner_types::post_asap::DataPrimitive::SummaryState;
        assert!(matches!(
            execute_precompute_sink(&summary_dag, &invalid_path_binding, PostAsapNodeId(1), key(1), &registry, &sink),
            Err(ScheduleError::Invalid(message)) if message.contains("query-owned node")
        ));
        assert!(matches!(
            execute_precompute_sink(&dag, &invalid_path_binding, PostAsapNodeId(1), key(0), &registry, &sink),
            Err(ScheduleError::Invalid(message)) if message.contains("does not match")
        ));
    }
}
