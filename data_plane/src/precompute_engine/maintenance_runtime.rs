//! Production adapter from installed post-ASAP maintenance DAGs to summary state.

use super::output_sink::OutputSink;
use super::subdag_scheduler::{
    execute_precompute_sink, IdempotentCommitSink, MaterializationCommitKey,
    PrecomputeOperatorRegistry, ScheduleError,
};
use crate::storage_engines::types::{AggregateCore, HotReloadStreamingConfig, PrecomputedOutput};
use asap_types::executable_plan::{BackendExecutableBinding, BackendNodeBinding};
use planner_types::post_asap::{ExecutableDagNode, ExecutableOperatorPayload, PostAsapNodeId};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

type SummaryState = Arc<dyn AggregateCore>;

struct OperatorAdapter<'a> {
    binding: &'a BackendExecutableBinding,
    source_definition: asap_types::sds::SummaryDefinitionId,
    source: SummaryState,
}

impl PrecomputeOperatorRegistry<SummaryState> for OperatorAdapter<'_> {
    type Error = String;

    fn execute(
        &self,
        node: &ExecutableDagNode,
        inputs: &[Arc<SummaryState>],
    ) -> Result<SummaryState, Self::Error> {
        let is_source = matches!(
            self.binding.node(node.id),
            Some(BackendNodeBinding::Materialization { summary_definition })
                if *summary_definition == self.source_definition
        );
        if is_source && inputs.is_empty() {
            return Ok(Arc::clone(&self.source));
        }
        match &node.payload {
            ExecutableOperatorPayload::SummaryAgg { .. }
            | ExecutableOperatorPayload::SummaryMerge => merge_inputs(inputs),
            payload => Err(format!(
                "maintenance operator {:?} has no summary-state implementation",
                payload.operator()
            )),
        }
    }
}

fn merge_inputs(inputs: &[Arc<SummaryState>]) -> Result<SummaryState, String> {
    let Some(first) = inputs.first() else {
        return Err("summary maintenance node has no input state".into());
    };
    let mut merged: Box<dyn AggregateCore> = (**first).clone_boxed_core();
    for input in &inputs[1..] {
        merged = merged
            .merge_with(input.as_ref().as_ref())
            .map_err(|error| error.to_string())?;
    }
    Ok(Arc::from(merged))
}

struct CommittedState {
    value: Arc<SummaryState>,
    published: bool,
}

#[derive(Default)]
struct CommitRegistry(Mutex<BTreeMap<MaterializationCommitKey, CommittedState>>);

impl CommitRegistry {
    fn claim_publish(&self, key: &MaterializationCommitKey) -> Result<bool, String> {
        let mut commits = self.0.lock().map_err(|_| "commit registry poisoned")?;
        let committed = commits
            .get_mut(key)
            .ok_or_else(|| "maintenance result was not committed".to_string())?;
        if committed.published {
            Ok(false)
        } else {
            committed.published = true;
            Ok(true)
        }
    }
}

impl IdempotentCommitSink<SummaryState> for CommitRegistry {
    type Error = String;

    fn get(
        &self,
        key: &MaterializationCommitKey,
    ) -> Result<Option<Arc<SummaryState>>, Self::Error> {
        Ok(self
            .0
            .lock()
            .map_err(|_| "commit registry poisoned")?
            .get(key)
            .map(|committed| Arc::clone(&committed.value)))
    }

    fn commit_if_absent(
        &self,
        key: MaterializationCommitKey,
        value: Arc<SummaryState>,
    ) -> Result<Arc<SummaryState>, Self::Error> {
        let mut commits = self.0.lock().map_err(|_| "commit registry poisoned")?;
        let committed = commits.entry(key).or_insert_with(|| CommittedState {
            value,
            published: false,
        });
        Ok(Arc::clone(&committed.value))
    }
}

/// Decorates the ordinary store sink with installed maintenance DAG execution.
/// With no matching DAG, the source output is forwarded unchanged.
pub struct MaintenanceDagSink {
    inner: Arc<dyn OutputSink>,
    plans: HotReloadStreamingConfig,
    commits: CommitRegistry,
}

impl MaintenanceDagSink {
    pub fn new(inner: Arc<dyn OutputSink>, plans: HotReloadStreamingConfig) -> Self {
        Self {
            inner,
            plans,
            commits: CommitRegistry::default(),
        }
    }

    fn execute_one(
        &self,
        output: PrecomputedOutput,
        state: Box<dyn AggregateCore>,
    ) -> Result<Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>, String> {
        let Some(plan) = self.plans.physical_plan_snapshot() else {
            return Ok(vec![(output, state)]);
        };
        let source_definition: asap_types::sds::SummaryDefinitionId = output.policy_fp.into();
        let source: SummaryState = Arc::from(state);
        let mut derived = Vec::new();
        let mut matched = false;
        let mut lineage = Vec::new();
        let definition_bytes = source_definition.0 .0.to_be_bytes();
        lineage.extend_from_slice(&definition_bytes);
        let group_bytes = output
            .key
            .as_ref()
            .map(|key| key.serialize_to_bytes())
            .unwrap_or_default();
        lineage.extend_from_slice(&(group_bytes.len() as u64).to_be_bytes());
        lineage.extend_from_slice(&group_bytes);
        let state_bytes = source.serialize_to_bytes();
        lineage.extend_from_slice(&(state_bytes.len() as u64).to_be_bytes());
        lineage.extend_from_slice(&state_bytes);
        for installed in plan.precompute_plan.executable_dags.values() {
            let dag = installed.document.decode()?;
            let source_nodes = installed
                .binding
                .nodes
                .iter()
                .filter_map(|(id, binding)| matches!(binding, BackendNodeBinding::Materialization { summary_definition } if *summary_definition == source_definition).then_some(*id))
                .collect::<BTreeSet<_>>();
            if source_nodes.is_empty() {
                continue;
            }
            let adapter = OperatorAdapter {
                binding: &installed.binding,
                source_definition,
                source: Arc::clone(&source),
            };
            for sink_node in &installed.binding.precompute_sinks {
                if !depends_on_any(&dag, *sink_node, &source_nodes) {
                    continue;
                }
                let reachable = dependencies(&dag, *sink_node);
                let foreign_source = reachable.iter().any(|node| {
                    let has_input = dag.edges.iter().any(|edge| edge.consumer == *node);
                    !has_input
                        && matches!(
                            installed.binding.node(*node),
                            Some(BackendNodeBinding::Materialization { summary_definition })
                                if *summary_definition != source_definition
                        )
                });
                if foreign_source {
                    return Err(
                        "maintenance sink requires synchronized inputs from multiple summary definitions"
                            .into(),
                    );
                }
                if dag.edges.iter().any(|edge| {
                    reachable.contains(&edge.consumer)
                        && !matches!(
                            edge.grouping,
                            planner_types::post_asap::GroupingEdgeCompatibility::Identical
                                | planner_types::post_asap::GroupingEdgeCompatibility::NotApplicable
                        )
                }) {
                    return Err(
                        "maintenance sink requires a cross-group shuffle before summary composition"
                            .into(),
                    );
                }
                matched = true;
                let key = MaterializationCommitKey {
                    plan_id: plan.plan_id(),
                    plan_version: plan.plan_version(),
                    node_id: sink_node.0,
                    window_start_ms: output.start_timestamp as i64,
                    window_end_ms: output.end_timestamp as i64,
                    input_lineage: lineage.clone(),
                };
                let value = execute_precompute_sink(
                    &dag,
                    &installed.binding,
                    *sink_node,
                    key.clone(),
                    &adapter,
                    &self.commits,
                )
                .map_err(schedule_error)?;
                if !self.commits.claim_publish(&key)? {
                    continue;
                }
                let target = match installed.binding.node(*sink_node) {
                    Some(BackendNodeBinding::Materialization { summary_definition }) => {
                        asap_types::PolicyFingerprint::from(*summary_definition)
                    }
                    _ => return Err("precompute sink lacks materialization binding".into()),
                };
                let mut target_output = output.clone();
                target_output.policy_fp = target;
                derived.push((target_output, value.as_ref().as_ref().clone_boxed_core()));
            }
        }
        if matched {
            Ok(derived)
        } else {
            Ok(vec![(output, (*source).clone_boxed_core())])
        }
    }
}

fn schedule_error(error: ScheduleError<String, String>) -> String {
    match error {
        ScheduleError::Invalid(e) | ScheduleError::Operator(e) | ScheduleError::Sink(e) => e,
    }
}

fn depends_on_any(
    dag: &planner_types::post_asap::ExecutableDag,
    sink: PostAsapNodeId,
    sources: &BTreeSet<PostAsapNodeId>,
) -> bool {
    !dependencies(dag, sink).is_disjoint(sources)
}

fn dependencies(
    dag: &planner_types::post_asap::ExecutableDag,
    sink: PostAsapNodeId,
) -> BTreeSet<PostAsapNodeId> {
    let mut pending = vec![sink];
    let mut seen = BTreeSet::new();
    while let Some(node) = pending.pop() {
        if seen.insert(node) {
            pending.extend(
                dag.edges
                    .iter()
                    .filter(|edge| edge.consumer == node)
                    .map(|edge| edge.producer),
            );
        }
    }
    seen
}

impl OutputSink for MaintenanceDagSink {
    fn emit_batch(
        &self,
        outputs: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut transformed = Vec::new();
        for (output, state) in outputs {
            transformed.extend(
                self.execute_one(output, state)
                    .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })?,
            );
        }
        self.inner.emit_batch(transformed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::precompute_engine::operators::SumAccumulator;
    use planner_types::post_asap::{
        EdgeRole, ExecutableDag, ExecutableDagEdge, ExecutableOperator, GroupingEdgeCompatibility,
        SummarySchema, WindowEdgeCompatibility,
    };

    fn definition(value: u64) -> asap_types::sds::SummaryDefinitionId {
        asap_types::PolicyFingerprint(value).into()
    }

    fn node(id: u32) -> ExecutableDagNode {
        ExecutableDagNode {
            id: PostAsapNodeId(id),
            operator: ExecutableOperator::SummaryMerge,
            payload: ExecutableOperatorPayload::SummaryMerge,
            output_state: planner_types::post_asap::ExecutionDataState::MAINTENANCE_SUMMARY,
            output_schema: SummarySchema {
                fields: vec![],
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
                fields: vec![],
                time_index: None,
            },
            data_state: planner_types::post_asap::ExecutionDataState::MAINTENANCE_SUMMARY,
            grouping: GroupingEdgeCompatibility::Identical,
            window: WindowEdgeCompatibility::NotApplicable,
        }
    }

    fn sum(value: f64) -> SummaryState {
        let mut accumulator = SumAccumulator::new();
        accumulator.update(value);
        Arc::new(accumulator)
    }

    #[test]
    fn shared_summary_node_executes_once_and_summary_over_summary_merges() {
        // source 0 is shared by both branches; root therefore contains two
        // copies of its value while node 0 itself is evaluated once.
        let mut query = node(4);
        query.output_state = planner_types::post_asap::ExecutionDataState::READ_ROWS;
        let dag = ExecutableDag {
            nodes: (0..4).map(node).chain([query]).collect(),
            edges: vec![edge(0, 1), edge(0, 2), edge(1, 3), edge(2, 3), edge(3, 4)],
            root: PostAsapNodeId(4),
        };
        let binding = BackendExecutableBinding {
            nodes: (0..4)
                .map(|id| {
                    (
                        PostAsapNodeId(id),
                        BackendNodeBinding::Materialization {
                            summary_definition: definition(if id == 0 { 1 } else { id as u64 + 1 }),
                        },
                    )
                })
                .chain([(
                    PostAsapNodeId(4),
                    BackendNodeBinding::Query {
                        query_node: control_plane::query_plan::QueryNodeId(9),
                    },
                )])
                .collect(),
            query_sink: PostAsapNodeId(4),
            query_plan_sink: control_plane::query_plan::QueryNodeId(9),
            precompute_sinks: vec![PostAsapNodeId(3)],
        };
        let source = sum(2.0);
        let adapter = OperatorAdapter {
            binding: &binding,
            source_definition: definition(1),
            source,
        };
        let commits = CommitRegistry::default();
        let key = MaterializationCommitKey {
            plan_id: 7,
            plan_version: 2,
            node_id: 3,
            window_start_ms: 0,
            window_end_ms: 10,
            input_lineage: b"batch:1".to_vec(),
        };
        let result = execute_precompute_sink(
            &dag,
            &binding,
            PostAsapNodeId(3),
            key.clone(),
            &adapter,
            &commits,
        )
        .unwrap();
        assert_eq!(result.as_ref().aux_stats().sum, Some(4.0));
        assert!(commits.claim_publish(&key).unwrap());
        assert!(!commits.claim_publish(&key).unwrap());
    }

    #[test]
    fn unsupported_maintenance_operator_propagates_failure_without_commit() {
        let mut unsupported = node(1);
        unsupported.operator = ExecutableOperator::SummarySubtract;
        unsupported.payload = ExecutableOperatorPayload::SummarySubtract;
        let mut query = node(2);
        query.output_state = planner_types::post_asap::ExecutionDataState::READ_ROWS;
        let dag = ExecutableDag {
            nodes: vec![node(0), unsupported, query],
            edges: vec![edge(0, 1), edge(1, 2)],
            root: PostAsapNodeId(2),
        };
        let binding = BackendExecutableBinding {
            nodes: BTreeMap::from([
                (
                    PostAsapNodeId(0),
                    BackendNodeBinding::Materialization {
                        summary_definition: definition(1),
                    },
                ),
                (
                    PostAsapNodeId(1),
                    BackendNodeBinding::Materialization {
                        summary_definition: definition(2),
                    },
                ),
                (
                    PostAsapNodeId(2),
                    BackendNodeBinding::Query {
                        query_node: control_plane::query_plan::QueryNodeId(9),
                    },
                ),
            ]),
            query_sink: PostAsapNodeId(2),
            query_plan_sink: control_plane::query_plan::QueryNodeId(9),
            precompute_sinks: vec![PostAsapNodeId(1)],
        };
        let adapter = OperatorAdapter {
            binding: &binding,
            source_definition: definition(1),
            source: sum(2.0),
        };
        let commits = CommitRegistry::default();
        let key = MaterializationCommitKey {
            plan_id: 7,
            plan_version: 2,
            node_id: 1,
            window_start_ms: 0,
            window_end_ms: 10,
            input_lineage: b"batch:1".to_vec(),
        };
        assert!(matches!(
            execute_precompute_sink(
                &dag,
                &binding,
                PostAsapNodeId(1),
                key.clone(),
                &adapter,
                &commits
            ),
            Err(ScheduleError::Operator(_))
        ));
        assert!(commits.get(&key).unwrap().is_none());
    }
}
