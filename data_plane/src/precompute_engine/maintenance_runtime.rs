//! Production adapter from installed post-ASAP maintenance DAGs to summary state.

use super::output_sink::OutputSink;
use super::subdag_scheduler::{
    execute_precompute_sink, IdempotentCommitSink, MaterializationCommitKey,
    PrecomputeOperatorRegistry, ScheduleError,
};
use crate::storage_engines::types::{AggregateCore, HotReloadStreamingConfig, PrecomputedOutput};
use asap_types::executable_plan::{BackendExecutableBinding, BackendNodeBinding};
use planner_types::post_asap::{ExecutableDagNode, ExecutableOperatorPayload, PostAsapNodeId};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

type SummaryState = Arc<dyn AggregateCore>;
type PendingOutput = (
    Option<(MaterializationCommitKey, u64)>,
    PrecomputedOutput,
    Box<dyn AggregateCore>,
);

struct OperatorAdapter<'a> {
    binding: &'a BackendExecutableBinding,
    source_definition: asap_types::sds::SummaryDefinitionId,
    source: SummaryState,
}

impl PrecomputeOperatorRegistry<SummaryState> for OperatorAdapter<'_> {
    type Error = String;

    fn materialized_input(&self, node: &ExecutableDagNode) -> Result<Option<SummaryState>, String> {
        Ok(matches!(
            self.binding.node(node.id),
            Some(BackendNodeBinding::Materialization { summary_definition })
                if *summary_definition == self.source_definition
        )
        .then(|| Arc::clone(&self.source)))
    }

    fn execute(
        &self,
        node: &ExecutableDagNode,
        inputs: &[Arc<SummaryState>],
    ) -> Result<SummaryState, Self::Error> {
        match &node.payload {
            ExecutableOperatorPayload::SummaryMerge => merge_inputs(inputs),
            ExecutableOperatorPayload::SummaryAgg { .. } => Err(
                "maintenance SummaryAgg requires a typed update evaluator; merging input state does not execute its update expression".into(),
            ),
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
    value: Option<Arc<SummaryState>>,
    published: bool,
}

#[derive(Default)]
struct CommitRegistryState {
    generation: Option<(u64, u64)>,
    entries: BTreeMap<MaterializationCommitKey, CommittedState>,
    frontiers: BTreeMap<asap_types::sds::SummaryDefinitionId, (i64, u64)>,
    pending_batch: Option<[u8; 32]>,
    batch_has_published: bool,
}

impl CommitRegistryState {
    fn validate_key(&self, key: &MaterializationCommitKey) -> Result<(), String> {
        if self
            .generation
            .is_some_and(|generation| generation != (key.plan_id, key.plan_version))
        {
            return Err("maintenance retry belongs to an obsolete plan generation".into());
        }
        if self
            .frontiers
            .get(&key.summary_definition)
            .is_some_and(|(latest, horizon)| {
                key.window_end_ms
                    <= latest.saturating_sub(i64::try_from(*horizon).unwrap_or(i64::MAX))
            })
        {
            return Err(
                "maintenance retry is outside the materialization retention horizon".into(),
            );
        }
        Ok(())
    }
}

#[derive(Default)]
struct CommitRegistry(Mutex<CommitRegistryState>);

impl CommitRegistry {
    fn plan_snapshot(
        &self,
        plans: &HotReloadStreamingConfig,
    ) -> Result<Option<Arc<crate::storage_engines::types::ActivePhysicalPlan>>, String> {
        let mut state = self.0.lock().map_err(|_| "commit registry poisoned")?;
        // Read the authoritative generation while holding the registry lock,
        // so an old in-flight batch cannot restore an obsolete generation.
        let plan = plans.physical_plan_snapshot();
        let generation = plan
            .as_ref()
            .map(|plan| (plan.plan_id(), plan.plan_version()));
        if state.generation != generation {
            state.entries.clear();
            state.frontiers.clear();
            state.pending_batch = None;
            state.batch_has_published = false;
            state.generation = generation;
        }
        Ok(plan)
    }

    fn begin_batch(&self, digest: [u8; 32]) -> Result<(), String> {
        let mut state = self.0.lock().map_err(|_| "commit registry poisoned")?;
        match state.pending_batch {
            Some(pending) if pending != digest => Err(
                "maintenance batch retry is pending; retry that batch before submitting new work"
                    .into(),
            ),
            _ => {
                if state.pending_batch.is_none() {
                    state.batch_has_published = false;
                }
                state.pending_batch = Some(digest);
                Ok(())
            }
        }
    }

    fn finish_batch(&self, digest: [u8; 32]) -> Result<(), String> {
        self.complete_batch(digest, &[])
    }

    fn cancel_unpublished_batch(&self) {
        if let Ok(mut state) = self.0.lock() {
            if !state.batch_has_published {
                state.entries.retain(|_, entry| entry.published);
                state.pending_batch = None;
            }
        }
    }

    fn complete_batch(
        &self,
        digest: [u8; 32],
        completed: &[(MaterializationCommitKey, u64)],
    ) -> Result<(), String> {
        let mut state = self.0.lock().map_err(|_| "commit registry poisoned")?;
        if state.pending_batch != Some(digest) {
            return Err("maintenance batch generation changed before completion".into());
        }
        for (key, horizon) in completed {
            if *horizon == 0
                || state
                    .generation
                    .is_some_and(|generation| generation != (key.plan_id, key.plan_version))
            {
                return Err("maintenance batch has invalid completion lifecycle".into());
            }
            let frontier = state
                .frontiers
                .entry(key.summary_definition)
                .or_insert((key.window_end_ms, *horizon));
            frontier.0 = frontier.0.max(key.window_end_ms);
            frontier.1 = frontier.1.max(*horizon);
        }
        let frontiers = state.frontiers.clone();
        state.entries.retain(|key, _| {
            frontiers
                .get(&key.summary_definition)
                .is_none_or(|(latest, horizon)| {
                    key.window_end_ms
                        > latest.saturating_sub(i64::try_from(*horizon).unwrap_or(i64::MAX))
                })
        });
        state.pending_batch = None;
        state.batch_has_published = false;
        Ok(())
    }

    fn is_published(&self, key: &MaterializationCommitKey) -> Result<bool, String> {
        let state = self.0.lock().map_err(|_| "commit registry poisoned")?;
        state.validate_key(key)?;
        Ok(state.entries.get(key).is_some_and(|entry| entry.published))
    }
    fn publish(
        &self,
        key: &MaterializationCommitKey,
        emit: impl FnOnce() -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Serialize acknowledgement with publication so a concurrent replay
        // cannot skip an in-flight write that later fails.
        let mut commits = self.0.lock().map_err(|_| "commit registry poisoned")?;
        commits.validate_key(key)?;
        let committed = commits
            .entries
            .get_mut(key)
            .ok_or_else(|| "maintenance result was not committed".to_string())?;
        if committed.published {
            Ok(())
        } else {
            emit()?;
            committed.published = true;
            committed.value = None;
            commits.batch_has_published = true;
            Ok(())
        }
    }
}

impl IdempotentCommitSink<SummaryState> for CommitRegistry {
    type Error = String;

    fn get(
        &self,
        key: &MaterializationCommitKey,
    ) -> Result<Option<Arc<SummaryState>>, Self::Error> {
        let state = self.0.lock().map_err(|_| "commit registry poisoned")?;
        state.validate_key(key)?;
        Ok(state
            .entries
            .get(key)
            .and_then(|committed| committed.value.as_ref().map(Arc::clone)))
    }

    fn commit_if_absent(
        &self,
        key: MaterializationCommitKey,
        value: Arc<SummaryState>,
    ) -> Result<Arc<SummaryState>, Self::Error> {
        let mut commits = self.0.lock().map_err(|_| "commit registry poisoned")?;
        commits.validate_key(&key)?;
        let committed = commits
            .entries
            .entry(key)
            .or_insert_with(|| CommittedState {
                value: Some(Arc::clone(&value)),
                published: false,
            });
        Ok(committed.value.as_ref().map(Arc::clone).unwrap_or(value))
    }
}

/// Decorates the ordinary store sink with installed maintenance DAG execution.
/// With no matching DAG, the source output is forwarded unchanged.
pub struct MaintenanceDagSink {
    inner: Arc<dyn OutputSink>,
    plans: HotReloadStreamingConfig,
    commits: CommitRegistry,
    batch_guard: Mutex<()>,
}

impl MaintenanceDagSink {
    pub fn new(inner: Arc<dyn OutputSink>, plans: HotReloadStreamingConfig) -> Self {
        Self {
            inner,
            plans,
            commits: CommitRegistry::default(),
            batch_guard: Mutex::new(()),
        }
    }

    fn execute_one(
        &self,
        plan: &crate::storage_engines::types::ActivePhysicalPlan,
        output: PrecomputedOutput,
        state: Box<dyn AggregateCore>,
    ) -> Result<Vec<PendingOutput>, String> {
        let source_definition: asap_types::sds::SummaryDefinitionId = output.policy_fp.into();
        let source: SummaryState = Arc::from(state);
        let mut derived = Vec::new();
        let mut matched = false;
        let mut lineage = Sha256::new();
        lineage.update(b"asap-maintenance-lineage-v1");
        let definition_bytes = source_definition.0 .0.to_be_bytes();
        lineage.update(definition_bytes);
        let group_bytes = output
            .key
            .as_ref()
            .map(|key| key.serialize_to_bytes())
            .unwrap_or_default();
        lineage.update((group_bytes.len() as u64).to_be_bytes());
        lineage.update(&group_bytes);
        let state_bytes = source.serialize_to_bytes();
        lineage.update((state_bytes.len() as u64).to_be_bytes());
        lineage.update(&state_bytes);
        let lineage = lineage.finalize().to_vec();
        drop(state_bytes);
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
                let reachable = dependencies_until(&dag, *sink_node, &source_nodes);
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
                        && !source_nodes.contains(&edge.consumer)
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
                let target = match installed.binding.node(*sink_node) {
                    Some(BackendNodeBinding::Materialization { summary_definition }) => {
                        *summary_definition
                    }
                    _ => return Err("precompute sink lacks materialization binding".into()),
                };
                let key = MaterializationCommitKey {
                    plan_id: plan.plan_id(),
                    plan_version: plan.plan_version(),
                    summary_definition: target,
                    window_start_ms: output.start_timestamp as i64,
                    window_end_ms: output.end_timestamp as i64,
                    input_lineage: lineage.clone(),
                };
                let config = plan
                    .precompute_plan
                    .materializations
                    .iter()
                    .find(|config| config.policy_fingerprint() == target.fingerprint())
                    .ok_or("maintenance sink has no materialization lifecycle")?;
                // Keep replay receipts for the installed state-retention span,
                // or one complete window when no longer retention is declared.
                let horizon_ms = config
                    .num_aggregates_to_retain
                    .unwrap_or(1)
                    .saturating_mul(config.slide_interval)
                    .max(config.window_size)
                    .saturating_mul(1_000);
                if self.commits.is_published(&key)? {
                    continue;
                }
                let value = execute_precompute_sink(
                    &dag,
                    &installed.binding,
                    *sink_node,
                    key.clone(),
                    &adapter,
                    &self.commits,
                )
                .map_err(schedule_error)?;
                let mut target_output = output.clone();
                target_output.policy_fp = target.into();
                derived.push((
                    Some((key, horizon_ms)),
                    target_output,
                    value.as_ref().as_ref().clone_boxed_core(),
                ));
            }
        }
        if matched {
            Ok(derived)
        } else {
            Ok(vec![(None, output, (*source).clone_boxed_core())])
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
    dependencies_until(dag, sink, &BTreeSet::new())
}

fn dependencies_until(
    dag: &planner_types::post_asap::ExecutableDag,
    sink: PostAsapNodeId,
    frontier: &BTreeSet<PostAsapNodeId>,
) -> BTreeSet<PostAsapNodeId> {
    let mut pending = vec![sink];
    let mut seen = BTreeSet::new();
    while let Some(node) = pending.pop() {
        if seen.insert(node) && !frontier.contains(&node) {
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
        // One bounded pending batch may be retried. Do not let another worker
        // advance its frontier while a partially accepted batch is replayable.
        let _guard = self
            .batch_guard
            .lock()
            .map_err(|_| "maintenance batch lock poisoned")?;
        let plan = self.commits.plan_snapshot(&self.plans)?;
        let sinks = plan.as_ref().map_or(0, |plan| {
            plan.precompute_plan
                .executable_dags
                .values()
                .map(|dag| dag.binding.precompute_sinks.len())
                .sum::<usize>()
        });
        if sinks == 0 {
            return self.inner.emit_batch(outputs);
        }
        const MAX_BATCH_RECEIPTS: usize = 65_536;
        const MAX_BATCH_SOURCE_BYTES: usize = 64 * 1024 * 1024;
        if outputs.len().saturating_mul(sinks) > MAX_BATCH_RECEIPTS {
            return Err(
                "maintenance batch exceeds bounded receipt budget; split the input batch".into(),
            );
        }
        let mut digest = Sha256::new();
        digest.update(b"asap-maintenance-batch-v1");
        let mut source_bytes = 0usize;
        for (output, state) in &outputs {
            let bytes = state.serialize_to_bytes();
            let group = output
                .key
                .as_ref()
                .map(|key| key.serialize_to_bytes())
                .unwrap_or_default();
            source_bytes = source_bytes
                .saturating_add(bytes.len())
                .saturating_add(group.len());
            if source_bytes.saturating_mul(sinks) > MAX_BATCH_SOURCE_BYTES {
                return Err(
                    "maintenance batch exceeds serialized source budget; split the input batch"
                        .into(),
                );
            }
            digest.update(output.policy_fp.0.to_be_bytes());
            digest.update(output.start_timestamp.to_be_bytes());
            digest.update(output.end_timestamp.to_be_bytes());
            digest.update((group.len() as u64).to_be_bytes());
            digest.update(group);
            digest.update((bytes.len() as u64).to_be_bytes());
            digest.update(bytes);
        }
        let digest: [u8; 32] = digest.finalize().into();
        self.commits.begin_batch(digest)?;
        let mut transformed = Vec::new();
        for (output, state) in outputs {
            match self.execute_one(
                plan.as_ref()
                    .expect("maintenance DAG requires an active plan"),
                output,
                state,
            ) {
                Ok(outputs) => transformed.extend(outputs),
                Err(error) => {
                    self.commits.cancel_unpublished_batch();
                    return Err(error.into());
                }
            }
        }
        if transformed.iter().all(|(key, _, _)| key.is_none()) {
            self.inner.emit_batch(
                transformed
                    .into_iter()
                    .map(|(_, output, state)| (output, state))
                    .collect(),
            )?;
            self.commits.finish_batch(digest)?;
            return Ok(());
        }
        // The generic sink can partially accept a batch. Acknowledge each
        // maintained output independently so retries skip only accepted writes.
        let mut completed = Vec::new();
        for (key, output, state) in transformed {
            match key {
                Some((key, horizon)) => {
                    self.commits
                        .publish(&key, || self.inner.emit_batch(vec![(output, state)]))?;
                    completed.push((key, horizon));
                }
                None => self.inner.emit_batch(vec![(output, state)])?,
            }
        }
        // Publication and partial replay use the previous frontier. Advance
        // only after the complete batch was accepted, so an early pane cannot
        // lose its receipt merely because a later pane shares its batch.
        self.commits.complete_batch(digest, &completed)?;
        Ok(())
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
    fn summary_aggregation_does_not_silently_reuse_input_family() {
        use planner_types::post_asap::{
            ExactKind, ExactParams, GroupingStrategy, SummaryFamilyType, SummaryUpdate,
        };
        use planner_types::pre_asap::{ColumnRef, Reduction};

        let binding = BackendExecutableBinding {
            nodes: BTreeMap::new(),
            query_sink: PostAsapNodeId(2),
            query_plan_sink: asap_types::query_plan::QueryNodeId(2),
            precompute_sinks: vec![PostAsapNodeId(1)],
        };
        let adapter = OperatorAdapter {
            binding: &binding,
            source_definition: definition(1),
            source: sum(7.0),
        };
        let mut aggregate = node(1);
        aggregate.operator = ExecutableOperator::SummaryAgg;
        aggregate.payload = ExecutableOperatorPayload::SummaryAgg {
            family: SummaryFamilyType::ExactAggregate(ExactKind::Count, ExactParams::Count),
            input: SummaryUpdate::column(ColumnRef::SampleValue),
            reduction: Reduction::by(vec![]),
            grouping: GroupingStrategy::default(),
        };
        let error = adapter.execute(&aggregate, &[Arc::new(sum(7.0))]);
        assert!(matches!(error, Err(reason) if reason.contains("typed update evaluator")));
    }

    // Receipts follow the declared event-time horizon and never retain accepted
    // summary payloads; expired retries fail instead of becoming duplicate writes.
    #[test]
    fn maintenance_receipts_are_bounded_and_expired_retries_fail_closed() {
        let commits = CommitRegistry::default();
        let key = |end| MaterializationCommitKey {
            plan_id: 7,
            plan_version: 1,
            summary_definition: definition(2),
            window_start_ms: end - 10,
            window_end_ms: end,
            input_lineage: vec![0; 32],
        };
        for end in (10..=1_000).step_by(10) {
            let key = key(end);
            commits.begin_batch([0; 32]).unwrap();
            commits
                .commit_if_absent(key.clone(), Arc::new(sum(2.0)))
                .unwrap();
            commits.publish(&key, || Ok(())).unwrap();
            commits.complete_batch([0; 32], &[(key, 30)]).unwrap();
            let state = commits.0.lock().unwrap();
            assert!(state.entries.len() <= 3);
            assert!(state.entries.values().all(|entry| entry.value.is_none()));
        }
        assert!(commits.get(&key(970)).is_err());
        assert!(commits
            .publish(&key(970), || panic!(
                "expired output must not reach storage"
            ))
            .is_err());
        assert!(commits.is_published(&key(980)).unwrap());
        commits.0.lock().unwrap().generation = Some((7, 2));
        assert!(commits
            .commit_if_absent(key(1_000), Arc::new(sum(2.0)))
            .is_err());
    }

    // Local node IDs are reused in separate query DAGs. Receipts must be scoped
    // by materialization identity so neither DAG suppresses the other's output.
    #[test]
    fn different_materializations_have_independent_publication_receipts() {
        let commits = CommitRegistry::default();
        for target in [2, 3] {
            let key = MaterializationCommitKey {
                plan_id: 7,
                plan_version: 1,
                summary_definition: definition(target),
                window_start_ms: 0,
                window_end_ms: 10,
                input_lineage: vec![0; 32],
            };
            assert!(!commits.is_published(&key).unwrap());
            commits
                .commit_if_absent(key.clone(), Arc::new(sum(2.0)))
                .unwrap();
            commits.publish(&key, || Ok(())).unwrap();
        }
        assert_eq!(commits.0.lock().unwrap().entries.len(), 2);
    }

    // A failed downstream write must be retried, while accepted outputs remain
    // idempotent when the same maintenance lineage is replayed.
    #[test]
    fn downstream_failure_does_not_acknowledge_maintenance_publication() {
        use crate::storage_engines::types::{
            ActivePhysicalPlan, HotReloadActivePhysicalPlan, StreamingConfig,
        };
        use asap_types::executable_plan::{InstalledPostAsapDag, OwnedPostAsapDag};
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Default)]
        struct FailOnceSink {
            attempts: AtomicUsize,
            accepted: AtomicUsize,
            fail_at: usize,
        }
        impl OutputSink for FailOnceSink {
            fn emit_batch(
                &self,
                outputs: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>,
            ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                if outputs.is_empty() {
                    return Ok(());
                }
                if self.attempts.fetch_add(1, Ordering::SeqCst) == self.fail_at {
                    return Err("temporary store failure".into());
                }
                self.accepted.fetch_add(outputs.len(), Ordering::SeqCst);
                Ok(())
            }
        }

        let mut snapshot: serde_json::Value = serde_json::from_str(include_str!(
            "../../../docs/examples/asapquery-planning-snapshot.json"
        ))
        .unwrap();
        snapshot["query_workload"]["repeating_queries"][0]["query"] =
            "sum(sum_over_time(m[1m]))".into();
        let snapshot: control_plane::physical::compiler::BackendLocalPlanningSnapshot =
            serde_json::from_value(snapshot).unwrap();
        let mut bundle = snapshot.compile().unwrap();
        let target_config = &bundle.precompute_plan.materializations[0];
        let long_step = target_config.window_size.max(
            target_config.slide_interval * target_config.num_aggregates_to_retain.unwrap_or(1),
        ) * 1_000;
        let target_definition = bundle.precompute_plan.materializations[0]
            .policy_fingerprint()
            .into();
        let mut query = node(2);
        query.output_state = planner_types::post_asap::ExecutionDataState::READ_ROWS;
        let dag = ExecutableDag {
            nodes: vec![node(0), node(1), query],
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
                        summary_definition: target_definition,
                    },
                ),
                (
                    PostAsapNodeId(2),
                    BackendNodeBinding::Query {
                        query_node: asap_types::query_plan::QueryNodeId(9),
                    },
                ),
            ]),
            query_sink: PostAsapNodeId(2),
            query_plan_sink: asap_types::query_plan::QueryNodeId(9),
            precompute_sinks: vec![PostAsapNodeId(1)],
        };
        bundle.precompute_plan.executable_dags = BTreeMap::from([(
            "retry".into(),
            InstalledPostAsapDag {
                document: OwnedPostAsapDag::from_executable("retry".into(), &dag).unwrap(),
                binding,
            },
        )]);
        let active = ActivePhysicalPlan {
            envelope: bundle.precompute_plan.envelope.clone(),
            summary_catalog: Some(Arc::new(bundle.summary_catalog)),
            precompute_plan: bundle.precompute_plan,
            transmission_plan: bundle.transmission_plan,
            runtime_config: Arc::new(StreamingConfig::new(Default::default())),
            query_plan: Arc::new(bundle.query_plan),
            storage_routing: Arc::new(Default::default()),
        };
        // The long batch spans two retention horizons. Its accepted prefix
        // must remain replayable until the same whole batch completes.
        for (fail_at, count, step, replay_after_success) in
            [(0, 2, 10, true), (1, 2, 10, true), (2, 5, long_step, false)]
        {
            let downstream = Arc::new(FailOnceSink {
                fail_at,
                ..Default::default()
            });
            let sink = MaintenanceDagSink::new(
                downstream.clone(),
                HotReloadStreamingConfig::from_active(HotReloadActivePhysicalPlan::new(
                    active.clone(),
                )),
            );
            let batch = || {
                (0..count)
                    .map(|i| {
                        (
                            PrecomputedOutput::new(
                                i * step,
                                (i + 1) * step,
                                None,
                                asap_types::PolicyFingerprint(1),
                            ),
                            sum(2.0).clone_boxed_core(),
                        )
                    })
                    .collect()
            };
            if !replay_after_success {
                let oversized = (0..65_537)
                    .map(|_| {
                        (
                            PrecomputedOutput::new(0, step, None, asap_types::PolicyFingerprint(1)),
                            sum(2.0).clone_boxed_core(),
                        )
                    })
                    .collect();
                assert!(sink
                    .emit_batch(oversized)
                    .unwrap_err()
                    .to_string()
                    .contains("receipt budget"));
                assert_eq!(downstream.attempts.load(Ordering::SeqCst), 0);
            }
            assert!(sink.emit_batch(batch()).is_err());
            if !replay_after_success {
                assert!(sink
                    .emit_batch(vec![(
                        PrecomputedOutput::new(
                            999_000,
                            1_000_000,
                            None,
                            asap_types::PolicyFingerprint(1),
                        ),
                        sum(3.0).clone_boxed_core()
                    )])
                    .unwrap_err()
                    .to_string()
                    .contains("retry is pending"));
            }
            sink.emit_batch(batch()).unwrap();
            if replay_after_success {
                sink.emit_batch(batch()).unwrap();
            } else {
                assert!(sink
                    .emit_batch(batch())
                    .unwrap_err()
                    .to_string()
                    .contains("retention horizon"));
                assert!(sink.commits.0.lock().unwrap().entries.len() <= 2);
            }
            assert_eq!(downstream.accepted.load(Ordering::SeqCst), count as usize);
            assert_eq!(
                downstream.attempts.load(Ordering::SeqCst),
                count as usize + 1
            );
        }
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
                        query_node: asap_types::query_plan::QueryNodeId(9),
                    },
                )])
                .collect(),
            query_sink: PostAsapNodeId(4),
            query_plan_sink: asap_types::query_plan::QueryNodeId(9),
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
            summary_definition: definition(4),
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
        commits.publish(&key, || Ok(())).unwrap();
        assert!(
            commits.get(&key).unwrap().is_none(),
            "accepted payload must not remain in the retry registry"
        );
        assert!(commits.is_published(&key).unwrap());
        commits
            .publish(&key, || panic!("accepted lineage must not publish twice"))
            .unwrap();
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
                        query_node: asap_types::query_plan::QueryNodeId(9),
                    },
                ),
            ]),
            query_sink: PostAsapNodeId(2),
            query_plan_sink: asap_types::query_plan::QueryNodeId(9),
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
            summary_definition: definition(2),
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
