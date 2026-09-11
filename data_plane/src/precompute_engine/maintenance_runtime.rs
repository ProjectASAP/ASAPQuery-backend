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

#[derive(Clone)]
enum MaintenanceValue {
    Summary {
        state: SummaryState,
        family: Option<planner_types::post_asap::SummaryFamilyType>,
    },
    // A collection is retained until the DAG explicitly reduces it. Evaluating
    // the whole DAG once per source pane would change nested reductions.
    SummaryWindows {
        states: Arc<[(i64, SummaryState)]>,
        family: planner_types::post_asap::SummaryFamilyType,
    },
    Rows {
        values: Vec<(i64, f64)>,
        name: String,
    },
}

impl MaintenanceValue {
    fn summary(state: SummaryState) -> Self {
        Self::Summary {
            state,
            family: None,
        }
    }

    fn state(&self) -> Result<&SummaryState, String> {
        match self {
            Self::Summary { state, .. } => Ok(state),
            Self::SummaryWindows { states, .. } if states.len() == 1 => Ok(&states[0].1),
            Self::Rows { .. } | Self::SummaryWindows { .. } => {
                Err("maintenance sink requires an explicit reduction to one summary state".into())
            }
        }
    }
}
type PendingOutput = (
    Option<(MaterializationCommitKey, u64)>,
    PrecomputedOutput,
    Box<dyn AggregateCore>,
);

struct OperatorAdapter<'a> {
    binding: &'a BackendExecutableBinding,
    source_definition: asap_types::sds::SummaryDefinitionId,
    source: SummaryState,
    configs: &'a [asap_types::aggregation_config::AggregationConfig],
    immutable_windows: Option<Arc<[(i64, SummaryState)]>>,
    singleton_population_complete: bool,
}

impl PrecomputeOperatorRegistry<MaintenanceValue> for OperatorAdapter<'_> {
    type Error = String;

    fn materialized_input(
        &self,
        node: &ExecutableDagNode,
    ) -> Result<Option<MaintenanceValue>, String> {
        if !matches!(
            self.binding.node(node.id),
            Some(BackendNodeBinding::Materialization { summary_definition })
                if *summary_definition == self.source_definition
        ) {
            return Ok(None);
        }
        let family = node.output_schema.fields.iter().find_map(|field| {
            (!matches!(
                field.dtype,
                planner_types::post_asap::SummaryFamilyType::Plain(_)
            ))
            .then(|| field.dtype.clone())
        });
        if let Some(states) = &self.immutable_windows {
            return Ok(Some(MaintenanceValue::SummaryWindows {
                states: Arc::clone(states),
                family: family.ok_or("immutable source lacks a summary schema")?,
            }));
        }
        Ok(Some(MaintenanceValue::Summary {
            state: Arc::clone(&self.source),
            family,
        }))
    }

    fn execute(
        &self,
        node: &ExecutableDagNode,
        inputs: &[Arc<MaintenanceValue>],
    ) -> Result<MaintenanceValue, Self::Error> {
        match &node.payload {
            ExecutableOperatorPayload::SummaryMerge => merge_inputs(inputs),
            ExecutableOperatorPayload::Value {
                operation: planner_types::post_asap::ValueOperation::FinalizeExactAccumulator,
                timing: planner_types::post_asap::ExecutionTiming::MaintenanceTime,
            } => {
                if self.immutable_windows.is_none() {
                    return Err(
                        "maintenance finalization requires immutable completed input windows"
                            .into(),
                    );
                }
                finalize_exact(node, inputs)
            }
            ExecutableOperatorPayload::SummaryAgg { family, input, .. } => {
                let [value] = inputs else {
                    return Err("maintenance SummaryAgg requires exactly one row input".into());
                };
                let MaintenanceValue::Rows { values, name } = value.as_ref() else {
                    return Err("maintenance SummaryAgg requires a typed update evaluator; finalize summary state before applying an update".into());
                };
                if self.immutable_windows.is_none() {
                    return Err(
                        "maintenance aggregation requires immutable completed input windows".into(),
                    );
                }
                let target = match self.binding.node(node.id) {
                    Some(BackendNodeBinding::Materialization { summary_definition }) => {
                        summary_definition
                    }
                    _ => {
                        return Err(
                            "maintenance SummaryAgg lacks installed materialization binding".into(),
                        )
                    }
                };
                let config = self
                    .configs
                    .iter()
                    .find(|config| config.policy_fingerprint() == target.fingerprint())
                    .ok_or("maintenance SummaryAgg lacks installed accumulator configuration")?;
                let source_config = self
                    .configs
                    .iter()
                    .find(|config| {
                        config.policy_fingerprint() == self.source_definition.fingerprint()
                    })
                    .ok_or("maintenance input lacks installed source configuration")?;
                validate_maintenance_grouping(
                    config,
                    source_config,
                    node,
                    self.singleton_population_complete,
                )?;
                if config.accumulator_spec().map_err(|e| e.to_string())?.family != *family {
                    return Err(
                        "maintenance SummaryAgg family differs from installed configuration".into(),
                    );
                }
                if input.item.is_some() {
                    return Err(
                        "keyed maintenance updates require explicit row identity routing".into(),
                    );
                }
                let mut updater = super::accumulator_factory::create_accumulator_updater(config);
                if updater.is_keyed() {
                    return Err("keyed maintenance accumulator requires an item expression".into());
                }
                for (timestamp_ms, value) in values {
                    let weight = evaluate_weight(&input.weight, *value, name)?;
                    updater.update_single(weight, *timestamp_ms);
                }
                let timestamp = values
                    .iter()
                    .map(|(timestamp, _)| *timestamp)
                    .max()
                    .ok_or("maintenance aggregation has no input rows")?;
                Ok(MaintenanceValue::SummaryWindows {
                    states: vec![(timestamp, Arc::from(updater.into_accumulator()))].into(),
                    family: family.clone(),
                })
            }
            payload => Err(format!(
                "maintenance operator {:?} has no summary-state implementation",
                payload.operator()
            )),
        }
    }
}

fn validate_maintenance_grouping(
    target: &asap_types::PrecomputeMaterialization,
    source: &asap_types::PrecomputeMaterialization,
    node: &ExecutableDagNode,
    singleton_population_complete: bool,
) -> Result<(), String> {
    if target.grouping_labels == source.grouping_labels
        && target.partitioning == source.partitioning
    {
        return Ok(());
    }
    if singleton_population_complete
        && target.grouping_labels.is_empty()
        && target.partitioning == Some(asap_types::sds::PopulationPartitioning::Grouped)
        && source.partitioning == Some(asap_types::sds::PopulationPartitioning::PerEntity)
        && target.stored_window_ms() == source.stored_window_ms()
        && matches!(&node.payload, ExecutableOperatorPayload::SummaryAgg {
            reduction: planner_types::pre_asap::Reduction::Reduce(keys), ..
        } if keys.is_empty())
    {
        return Ok(());
    }
    Err("maintenance population reduction requires a complete singleton source or synchronized grouping".into())
}

fn evaluate_weight(
    expression: &planner_types::post_asap::SummaryInputExpr,
    value: f64,
    name: &str,
) -> Result<f64, String> {
    use planner_types::{post_asap::SummaryInputExpr, pre_asap::ColumnRef};
    let weight = match expression {
        SummaryInputExpr::Constant(value) => *value,
        SummaryInputExpr::Column(ColumnRef::SampleValue) => value,
        SummaryInputExpr::Column(ColumnRef::Named(column)) if column == name => value,
        _ => {
            return Err("maintenance update does not resolve against the supplied typed row".into())
        }
    };
    if !weight.is_finite() {
        return Err("maintenance update weight is not finite".into());
    }
    Ok(weight)
}

fn finalize_exact(
    node: &ExecutableDagNode,
    inputs: &[Arc<MaintenanceValue>],
) -> Result<MaintenanceValue, String> {
    use planner_types::post_asap::{ExactKind, SummaryFamilyType};
    let [input] = inputs else {
        return Err("exact maintenance finalization requires one summary input".into());
    };
    let (states, family): (Vec<(i64, &SummaryState)>, _) = match input.as_ref() {
        MaintenanceValue::Summary {
            state,
            family: Some(family),
        } => {
            if node.output_schema.time_index.is_some() {
                return Err("timestamped finalization requires source window timestamps".into());
            }
            // Single-state merge execution has no row timestamp. Immutable
            // execution supplies SummaryWindows with the actual window ends.
            (vec![(0, state)], family)
        }
        MaintenanceValue::SummaryWindows { states, family } => (
            states.iter().map(|(time, state)| (*time, state)).collect(),
            family,
        ),
        _ => {
            return Err("exact maintenance finalization requires a typed exact accumulator".into())
        }
    };
    let SummaryFamilyType::ExactAggregate(kind, _) = family else {
        return Err("exact maintenance finalization requires a typed exact accumulator".into());
    };
    let statistic = match kind {
        ExactKind::Sum => asap_types::Statistic::Sum,
        ExactKind::Count => asap_types::Statistic::Count,
        _ => {
            return Err(
                "exact maintenance readout requires explicit operator/time semantics".into(),
            )
        }
    };
    let fields = &node.output_schema.fields;
    let field = match node.output_schema.time_index {
        None if fields.len() == 1 => &fields[0],
        Some(time_index) if fields.len() == 2 && time_index < 2 => {
            let timestamp = &fields[time_index];
            let value = &fields[1 - time_index];
            if timestamp.nullable
                || timestamp.name == value.name
                || !matches!(
                    timestamp.dtype,
                    SummaryFamilyType::Plain(planner_types::pre_asap::DataType::Timestamp)
                )
            {
                return Err("finalization timestamp column differs from its typed schema".into());
            }
            value
        }
        _ => {
            return Err(
                "exact maintenance finalization requires one value and optional declared timestamp"
                    .into(),
            )
        }
    };
    if field.nullable
        || !matches!(
            field.dtype,
            SummaryFamilyType::Plain(planner_types::pre_asap::DataType::Float64)
        )
    {
        return Err(
            "exact maintenance finalization currently requires a Float64 output column".into(),
        );
    }
    let values = states
        .into_iter()
        .map(|(timestamp, state)| {
            let value = state
                .query_statistic(statistic, &None, &std::collections::HashMap::new())
                .map_err(|error| error.to_string())?;
            if !value.is_finite() {
                return Err("exact maintenance finalization produced a non-finite value".into());
            }
            Ok((timestamp, value))
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(MaintenanceValue::Rows {
        values,
        name: field.name.clone(),
    })
}

fn merge_inputs(inputs: &[Arc<MaintenanceValue>]) -> Result<MaintenanceValue, String> {
    let mut states = Vec::new();
    let mut family = None;
    let mut end_timestamp = None;
    for input in inputs {
        let input_family = match input.as_ref() {
            MaintenanceValue::Summary { state, family } => {
                states.push(state);
                family.as_ref()
            }
            MaintenanceValue::SummaryWindows {
                states: windows,
                family,
            } => {
                states.extend(windows.iter().map(|(_, state)| state));
                end_timestamp = end_timestamp
                    .into_iter()
                    .chain(windows.iter().map(|(time, _)| *time))
                    .max();
                Some(family)
            }
            MaintenanceValue::Rows { .. } => return Err("summary merge cannot consume rows".into()),
        };
        if let Some(input_family) = input_family {
            if family.as_ref().is_some_and(|family| family != input_family) {
                return Err("summary merge input families differ".into());
            }
            family = Some(input_family.clone());
        }
    }
    let Some((first, rest)) = states.split_first() else {
        return Err("summary maintenance node has no input state".into());
    };
    let mut merged = first.clone_boxed_core();
    for state in rest {
        merged = merged
            .merge_with(state.as_ref())
            .map_err(|e| e.to_string())?;
    }
    if let Some(timestamp) = end_timestamp {
        return Ok(MaintenanceValue::SummaryWindows {
            states: vec![(timestamp, Arc::from(merged))].into(),
            family: family.ok_or("merged immutable state lacks a family")?,
        });
    }
    Ok(MaintenanceValue::Summary {
        state: Arc::from(merged),
        family,
    })
}

fn frozen_cohort_lineage(
    inputs: &[crate::storage_engines::sketch_db::index::FrozenExactWindows],
    expected: &asap_types::derived_input::DerivedInputIdentity,
) -> Result<[u8; 32], String> {
    let generation = &inputs
        .first()
        .ok_or("immutable input cohort is empty")?
        .generation;
    if inputs
        .iter()
        .map(|input| input.definition)
        .collect::<BTreeSet<_>>()
        != expected.inputs
    {
        return Err("immutable lineage differs from installed input definitions".into());
    }
    let mut ordered: Vec<_> = inputs.iter().collect();
    ordered.sort_by(|left, right| {
        (left.definition, left.sid, &left.group).cmp(&(right.definition, right.sid, &right.group))
    });
    if ordered.windows(2).any(|pair| {
        (pair[0].definition, pair[0].sid, &pair[0].group)
            == (pair[1].definition, pair[1].sid, &pair[1].group)
    }) {
        return Err("immutable lineage repeats a physical population".into());
    }
    let multiple = ordered.len() > 1;
    let mut lineage = Sha256::new();
    if multiple {
        lineage.update(b"immutable-maintenance-input-v2");
        lineage.update((ordered.len() as u64).to_be_bytes());
    } else {
        // Preserve the existing durable single-input receipt identity.
        lineage.update(b"immutable-maintenance-input-v1");
    }
    for input in ordered {
        if &input.generation != generation || input.windows.is_empty() {
            return Err("immutable lineage has mixed generations or empty windows".into());
        }
        lineage.update(input.sid.to_be_bytes());
        let metadata =
            serde_json::to_vec(&(&input.definition, &input.generation, &input.group, expected))
                .map_err(|error| error.to_string())?;
        if multiple {
            lineage.update((metadata.len() as u64).to_be_bytes());
        }
        lineage.update(metadata);
        if multiple {
            lineage.update((input.windows.len() as u64).to_be_bytes());
        }
        for ((start, end), state) in &input.windows {
            lineage.update(start.to_be_bytes());
            lineage.update(end.to_be_bytes());
            let bytes = state.serialize_to_bytes();
            lineage.update((bytes.len() as u64).to_be_bytes());
            lineage.update(bytes);
        }
    }
    Ok(lineage.finalize().into())
}

/// Evaluate one installed maintenance sink over an immutable physical source
/// incarnation. Durable publication is a separate existing-store transaction;
/// the in-memory scheduler cache here never claims durable exactly-once writes.
fn prepare_frozen_maintenance_sink(
    installed: &asap_types::executable_plan::InstalledPostAsapDag,
    configs: &[asap_types::PrecomputeMaterialization],
    sink: PostAsapNodeId,
    input: &crate::storage_engines::sketch_db::index::FrozenExactWindows,
    output_window: (u64, u64),
) -> Result<
    (
        planner_types::post_asap::ExecutableDag,
        MaterializationCommitKey,
        Vec<(i64, SummaryState)>,
    ),
    String,
> {
    installed.validate()?;
    let target = match installed.binding.node(sink) {
        Some(BackendNodeBinding::Materialization { summary_definition }) => *summary_definition,
        _ => return Err("immutable sink lacks a materialization binding".into()),
    };
    let config = configs
        .iter()
        .find(|config| config.policy_fingerprint() == target.fingerprint())
        .ok_or("immutable sink lacks its installed configuration")?;
    let expected_input = config
        .derived_input
        .as_ref()
        .ok_or("immutable sink is not a derived materialization")?;
    if expected_input.inputs != BTreeSet::from([input.definition]) {
        return Err("immutable sink requires synchronized input definitions".into());
    }
    if output_window.0 >= output_window.1
        || output_window.1 - output_window.0 != config.stored_window_ms()
        || input
            .windows
            .keys()
            .any(|(start, end)| *start < output_window.0 || *end > output_window.1)
    {
        return Err("immutable inputs do not fit the installed output window".into());
    }
    let dag = installed.document.decode()?;
    let producers: Vec<_> = dag
        .edges
        .iter()
        .filter(|edge| edge.consumer == sink)
        .collect();
    let [producer] = producers.as_slice() else {
        return Err("immutable sink requires one input relation".into());
    };
    let frontiers = installed
        .binding
        .nodes
        .iter()
        .filter_map(|(node, binding)| match binding {
            BackendNodeBinding::Materialization { summary_definition }
                if *summary_definition == input.definition =>
            {
                Some((*node, *summary_definition))
            }
            _ => None,
        })
        .collect();
    let actual_input = asap_types::derived_input::DerivedInputIdentity::from_dag(
        &installed.document,
        producer.producer,
        &frontiers,
    )?;
    if &actual_input != expected_input {
        return Err("immutable sink input program differs from its catalog identity".into());
    }
    let digest = frozen_cohort_lineage(std::slice::from_ref(input), expected_input)?;
    let states = input
        .windows
        .iter()
        .map(|((_, end), state)| (*end as i64, Arc::clone(state)))
        .collect();
    let key = MaterializationCommitKey {
        plan_id: input.generation.plan_id,
        plan_version: input.generation.plan_version,
        summary_definition: target,
        window_start_ms: i64::try_from(output_window.0)
            .map_err(|_| "output window exceeds timestamp range")?,
        window_end_ms: i64::try_from(output_window.1)
            .map_err(|_| "output window exceeds timestamp range")?,
        input_lineage: digest.to_vec(),
    };
    Ok((dag, key, states))
}

fn execute_prepared_frozen_sink(
    installed: &asap_types::executable_plan::InstalledPostAsapDag,
    configs: &[asap_types::PrecomputeMaterialization],
    sink: PostAsapNodeId,
    input: &crate::storage_engines::sketch_db::index::FrozenExactWindows,
    dag: &planner_types::post_asap::ExecutableDag,
    key: MaterializationCommitKey,
    states: Vec<(i64, SummaryState)>,
) -> Result<SummaryState, String> {
    let first = states.first().ok_or("immutable input is empty")?;
    let adapter = OperatorAdapter {
        binding: &installed.binding,
        source_definition: input.definition,
        source: Arc::clone(&first.1),
        configs,
        immutable_windows: Some(states.into()),
        singleton_population_complete: input.singleton_population_complete,
    };
    let value = execute_precompute_sink(
        dag,
        &installed.binding,
        sink,
        key,
        &adapter,
        &CommitRegistry::default(),
    )
    .map_err(schedule_error)?;
    Ok(Arc::clone(value.state()?))
}

#[cfg(test)]
fn evaluate_frozen_maintenance_sink(
    installed: &asap_types::executable_plan::InstalledPostAsapDag,
    configs: &[asap_types::PrecomputeMaterialization],
    sink: PostAsapNodeId,
    input: &crate::storage_engines::sketch_db::index::FrozenExactWindows,
    output_window: (u64, u64),
) -> Result<(SummaryState, [u8; 32]), String> {
    let (dag, key, states) =
        prepare_frozen_maintenance_sink(installed, configs, sink, input, output_window)?;
    let digest = key
        .input_lineage
        .as_slice()
        .try_into()
        .map_err(|_| "invalid input digest")?;
    let state = execute_prepared_frozen_sink(installed, configs, sink, input, &dag, key, states)?;
    Ok((state, digest))
}

/// Execute an installed, single-population maintenance subDAG from frozen
/// base panes and publish its complete output through the durable part path.
/// This initial entry point accepts non-overlapping output windows; sliding
/// replacement and cross-population shuffles require their own scheduling
/// proof and are rejected, rather than treating corrections as observations.
#[allow(clippy::too_many_arguments)]
pub fn execute_completed_maintenance(
    store: &crate::storage_engines::sketch_db::index::SketchStore,
    installed: &asap_types::executable_plan::InstalledPostAsapDag,
    configs: &[asap_types::PrecomputeMaterialization],
    sink: planner_types::post_asap::PostAsapNodeId,
    source_sid: u64,
    target_sid: u64,
    window: (u64, u64),
    group: &BTreeMap<String, String>,
) -> Result<bool, String> {
    use asap_types::executable_plan::BackendNodeBinding;
    let target = match installed.binding.node(sink) {
        Some(BackendNodeBinding::Materialization { summary_definition }) => *summary_definition,
        _ => return Err("maintenance sink lacks an installed output identity".into()),
    };
    let target_config = configs
        .iter()
        .find(|config| config.policy_fingerprint() == target.fingerprint())
        .ok_or("maintenance output configuration is absent")?;
    let derived = target_config
        .derived_input
        .as_ref()
        .ok_or("maintenance output has no derived input")?;
    if derived.inputs.len() != 1 {
        return Err("maintenance execution requires synchronized multi-source scheduling".into());
    }
    let source = *derived.inputs.first().unwrap();
    let source_config = configs
        .iter()
        .find(|config| config.policy_fingerprint() == source.fingerprint())
        .ok_or("maintenance source configuration is absent")?;
    let source_width = source_config.stored_window_ms();
    let target_width = target_config.stored_window_ms();
    let origin = source_config.pane_origin_ms.unwrap_or(0);
    if source_width == 0
        || target_width == 0
        || window.0 >= window.1
        || window.1 - window.0 != target_width
        || target_width % source_width != 0
        || target_width / source_width > 65_536
        || source_config
            .slide_interval
            .checked_mul(1000)
            .is_none_or(|slide| slide < source_width)
        || target_config
            .slide_interval
            .checked_mul(1000)
            .is_none_or(|slide| slide < target_width)
        || window.1 > i64::MAX as u64
        || (window.0 as i128 - origin as i128).rem_euclid(source_width as i128) != 0
        || (window.0 as i128 - target_config.pane_origin_ms.unwrap_or(0) as i128)
            .rem_euclid(target_width as i128)
            != 0
    {
        return Err("maintenance window requires unsupported overlap, phase, or extent".into());
    }
    let expected = (0..target_width / source_width)
        .map(|index| {
            let start = window.0 + index * source_width;
            (start, start + source_width)
        })
        .collect();
    let generation = store
        .active_catalog_generation()
        .ok_or("maintenance requires an authoritative catalog")?;
    let mut cohort = store.read_frozen_exact_cohort(
        &generation,
        &derived.inputs,
        &[(source_sid, source, expected, group.clone())],
    )?;
    let frozen = cohort.pop().ok_or("immutable input cohort is empty")?;
    let (dag, key, states) =
        prepare_frozen_maintenance_sink(installed, configs, sink, &frozen, window)?;
    let digest = key
        .input_lineage
        .as_slice()
        .try_into()
        .map_err(|_| "invalid input digest")?;
    let target_node = dag
        .nodes
        .iter()
        .find(|node| node.id == sink)
        .ok_or("maintenance target node is absent")?;
    validate_maintenance_grouping(
        target_config,
        source_config,
        target_node,
        frozen.singleton_population_complete,
    )?;
    if store.recover_frozen_maintenance_output(
        target_sid,
        target_config,
        std::slice::from_ref(&frozen),
        digest,
        window,
    )? {
        return Ok(false);
    }
    let state = execute_prepared_frozen_sink(installed, configs, sink, &frozen, &dag, key, states)?;
    let mut output = crate::storage_engines::types::PrecomputedOutput::new(
        window.0,
        window.1,
        Some(crate::storage_engines::types::KeyByLabelValues {
            labels: target_config
                .grouping_labels
                .iter()
                .map(|name| {
                    group
                        .get(name)
                        .cloned()
                        .ok_or("maintenance population is missing an output grouping key")
                })
                .collect::<Result<Vec<_>, _>>()?,
        }),
        target.fingerprint(),
    );
    output.catalog_generation = Some(generation);
    store.publish_frozen_maintenance_output(
        target_sid,
        target_config,
        &output,
        state.as_ref(),
        std::slice::from_ref(&frozen),
        digest,
    )
}

/// Schedule retained, aligned completed windows from the installed DAG after
/// the finite source barrier. Missing panes remain unavailable to query reads.
pub(crate) fn execute_finite_maintenance(
    store: &crate::storage_engines::sketch_db::index::SketchStore,
    resolver: &crate::drivers::ingest::series_resolver::SeriesIdResolver,
    plan: &asap_types::precompute_plan::PrecomputePlan,
) -> Result<(), String> {
    let generation = plan
        .summary_catalog
        .as_ref()
        .ok_or("finite maintenance requires a catalog generation")?;
    for installed in plan.executable_dags.values() {
        for sink in &installed.binding.precompute_sinks {
            let Some(BackendNodeBinding::Materialization {
                summary_definition: target,
            }) = installed.binding.node(*sink)
            else {
                continue;
            };
            let config = plan
                .materializations
                .iter()
                .find(|config| config.policy_fingerprint() == target.fingerprint())
                .ok_or("maintenance target configuration is absent")?;
            let Some(derived) = &config.derived_input else {
                continue;
            };
            if derived.inputs.len() != 1 {
                return Err("finite maintenance requires one source definition".into());
            }
            let source = *derived.inputs.first().unwrap();
            let source_config = plan
                .materializations
                .iter()
                .find(|config| config.policy_fingerprint() == source.fingerprint())
                .ok_or("maintenance source configuration is absent")?;
            if source_config.derived_input.is_some() {
                return Err(
                    "multi-stage finite maintenance requires topological scheduling".into(),
                );
            }
            let width = config.stored_window_ms();
            let pane = source_config.stored_window_ms();
            if width == 0 || pane == 0 || width % pane != 0 || width / pane > 65_536 {
                return Err("finite maintenance window extent is unsupported".into());
            }
            let sources = store.completed_maintenance_coordinates(source, generation)?;
            if sources.len() > 1 || sources.values().any(|populations| populations.len() != 1) {
                return Err(
                    "finite maintenance requires exactly one physical source population".into(),
                );
            }
            let existing = store.completed_maintenance_coordinates(*target, generation)?;
            for (source_sid, populations) in sources {
                for (group, windows) in populations {
                    let output_group: BTreeMap<_, _> = config
                        .grouping_labels
                        .iter()
                        .map(|key| {
                            group
                                .get(key)
                                .cloned()
                                .map(|value| (key.clone(), value))
                                .ok_or("maintenance output grouping key is absent")
                        })
                        .collect::<Result<_, _>>()?;
                    let pairs: Vec<_> = output_group
                        .iter()
                        .map(|(key, value)| (key.as_str(), value.as_str()))
                        .collect();
                    let attrs = crate::drivers::ingest::canonical_attrs_fingerprint(&pairs);
                    let kind =
                        crate::storage_engines::sketch_db::data::materialization_kind_for_config(
                            config,
                        );
                    let target_sid = resolver.resolve_with_reactivation(
                        &config.metric,
                        &attrs,
                        &kind,
                        |sid| {
                            store.validate_routed_catalog_generation(Some(generation))?;
                            let activation = store.authorize_series_reactivation(sid, *target)?;
                            if activation
                                .as_deref()
                                .is_some_and(|actual| actual != generation)
                            {
                                return Err("finite maintenance generation changed".into());
                            }
                            Ok(activation)
                        },
                    )?;
                    for (start, _) in &windows {
                        if (*start as i128 - config.pane_origin_ms.unwrap_or(0) as i128)
                            .rem_euclid(width as i128)
                            != 0
                        {
                            continue;
                        }
                        let Some(end) = start.checked_add(width) else {
                            continue;
                        };
                        if !(0..width / pane).all(|offset| {
                            let begin = start + offset * pane;
                            windows.contains(&(begin, begin + pane))
                        }) {
                            continue;
                        }
                        if existing
                            .get(&target_sid)
                            .and_then(|groups| groups.get(&output_group))
                            .is_some_and(|present| present.contains(&(*start, end)))
                        {
                            continue;
                        }
                        execute_completed_maintenance(
                            &store,
                            installed,
                            &plan.materializations,
                            *sink,
                            source_sid,
                            target_sid,
                            (*start, end),
                            &group,
                        )?;
                    }
                }
            }
        }
    }
    Ok(())
}

struct CommittedState {
    value: Option<Arc<MaintenanceValue>>,
    published: bool,
}

#[derive(Default)]
struct CommitRegistryState {
    generation: Option<(u64, u64)>,
    entries: BTreeMap<MaterializationCommitKey, CommittedState>,
    frontiers: BTreeMap<asap_types::sds::SummaryDefinitionId, (i64, u64)>,
    pending_batch: Option<[u8; 32]>,
    batch_has_published: bool,
    admitted_keys: BTreeSet<MaterializationCommitKey>,
}

impl CommitRegistryState {
    fn validate_key(&self, key: &MaterializationCommitKey) -> Result<(), String> {
        if self
            .generation
            .is_some_and(|generation| generation != (key.plan_id, key.plan_version))
        {
            return Err("maintenance retry belongs to an obsolete plan generation".into());
        }
        if !self.admitted_keys.contains(key)
            && self
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
            state.admitted_keys.clear();
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
                state.admitted_keys.clear();
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
        state.admitted_keys.clear();
        state.batch_has_published = false;
        Ok(())
    }

    fn pin_admitted(&self, key: &MaterializationCommitKey) -> Result<(), String> {
        let mut state = self.0.lock().map_err(|_| "commit registry poisoned")?;
        if state.pending_batch.is_none()
            || state.generation != Some((key.plan_id, key.plan_version))
        {
            return Err("admitted maintenance output has no current batch".into());
        }
        state.admitted_keys.insert(key.clone());
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

impl IdempotentCommitSink<MaintenanceValue> for CommitRegistry {
    type Error = String;

    fn get(
        &self,
        key: &MaterializationCommitKey,
    ) -> Result<Option<Arc<MaintenanceValue>>, Self::Error> {
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
        value: Arc<MaintenanceValue>,
    ) -> Result<Arc<MaintenanceValue>, Self::Error> {
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
        if let Some(input) = &output.input_revision {
            if input.generation.plan_id != plan.plan_id()
                || input.generation.plan_version != plan.plan_version()
            {
                return Err("maintenance input belongs to an obsolete generation".into());
            }
            lineage.update(input.first_revision.to_be_bytes());
            lineage.update(input.revision.to_be_bytes());
        }
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
                configs: &plan.precompute_plan.materializations,
                immutable_windows: None,
                singleton_population_complete: false,
            };
            for sink_node in &installed.binding.precompute_sinks {
                // Derived summaries consume complete immutable windows at the
                // completion barrier, never additive worker fragments.
                if matches!(installed.binding.node(*sink_node),
                    Some(BackendNodeBinding::Materialization { summary_definition })
                        if plan.precompute_plan.materializations.iter().any(|config|
                            config.policy_fingerprint() == summary_definition.fingerprint()
                                && config.derived_input.is_some()))
                {
                    continue;
                }
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
                // This output was admitted before another worker advanced the
                // replay floor. The store validates its exact consumed receipt
                // before publication; it is not an unsolicited expired replay.
                if output.input_revision.is_some() {
                    self.commits.pin_admitted(&key)?;
                }
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
                target_output.series_id = None;
                derived.push((
                    Some((key, horizon_ms)),
                    target_output,
                    value.state()?.clone_boxed_core(),
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
            if let Some(input) = &output.input_revision {
                digest.update(input.first_revision.to_be_bytes());
                digest.update(input.revision.to_be_bytes());
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

    #[test]
    fn cohort_lineage_is_order_independent_and_binds_every_input() {
        use crate::storage_engines::sketch_db::index::FrozenExactWindows;
        let make = |sid, id, value| {
            let mut state = crate::precompute_engine::operators::SumAccumulator::new();
            state.update(value);
            FrozenExactWindows {
                sid,
                definition: definition(id),
                generation: Arc::new(asap_types::sds::CatalogGeneration {
                    schema_version: 2,
                    plan_id: 1,
                    plan_version: 1,
                    snapshot_sha256: "0".repeat(64),
                }),
                group: BTreeMap::from([("instance".into(), sid.to_string())]),
                windows: BTreeMap::from([((0, 1000), Arc::new(state) as SummaryState)]),
                singleton_population_complete: false,
            }
        };
        let expected = asap_types::derived_input::DerivedInputIdentity {
            inputs: BTreeSet::from([definition(1), definition(2)]),
            program_sha256: "1".repeat(64),
        };
        let baseline =
            frozen_cohort_lineage(&[make(10, 1, 3.0), make(20, 2, 5.0)], &expected).unwrap();
        assert_eq!(
            baseline,
            frozen_cohort_lineage(&[make(20, 2, 5.0), make(10, 1, 3.0)], &expected).unwrap()
        );
        assert_ne!(
            baseline,
            frozen_cohort_lineage(&[make(10, 1, 3.0), make(20, 2, 6.0)], &expected).unwrap()
        );
        assert_ne!(
            baseline,
            frozen_cohort_lineage(&[make(10, 1, 3.0), make(21, 2, 5.0)], &expected).unwrap()
        );
        let mut changed = make(20, 2, 5.0);
        changed.group.insert("instance".into(), "other".into());
        assert_ne!(
            baseline,
            frozen_cohort_lineage(&[make(10, 1, 3.0), changed], &expected).unwrap()
        );
        let mut changed = make(20, 2, 5.0);
        let state = changed.windows.remove(&(0, 1000)).unwrap();
        changed.windows.insert((1000, 2000), state);
        assert_ne!(
            baseline,
            frozen_cohort_lineage(&[make(10, 1, 3.0), changed], &expected).unwrap()
        );
        let mut changed = make(20, 2, 5.0);
        Arc::make_mut(&mut changed.generation).plan_version += 1;
        assert!(frozen_cohort_lineage(&[make(10, 1, 3.0), changed], &expected).is_err());
        assert!(frozen_cohort_lineage(&[make(10, 1, 3.0)], &expected).is_err());
        assert!(frozen_cohort_lineage(
            &[make(10, 1, 3.0), make(10, 1, 3.0), make(20, 2, 5.0)],
            &expected
        )
        .is_err());
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
    fn finalized_summary_update_builds_a_different_installed_family() {
        use planner_types::post_asap::{
            ExactKind, ExactParams, GroupingStrategy, SummaryFamilyType, SummaryField,
            SummaryInputExpr, SummaryUpdate,
        };
        use planner_types::pre_asap::{DataType, Reduction};
        let mut snapshot: serde_json::Value = serde_json::from_str(include_str!(
            "../../../docs/examples/asapquery-planning-snapshot.json"
        ))
        .unwrap();
        snapshot["query_workload"]["repeating_queries"][0]["query"] =
            "sum(sum_over_time(m[1m]))".into();
        let snapshot: control_plane::physical::compiler::BackendLocalPlanningSnapshot =
            serde_json::from_value(snapshot).unwrap();
        let bundle = snapshot.compile().unwrap();
        let mut source_config = bundle.precompute_plan.materializations[0].clone();
        source_config.window_size = 2;
        source_config.slide_interval = 2;
        source_config.window_layout =
            asap_types::aggregation_config::WindowMaterializationLayout::Pane { pane_secs: 1 };
        let source_definition = source_config.policy_fingerprint().into();
        let mut target_config = source_config.clone();
        target_config.window_layout =
            asap_types::aggregation_config::WindowMaterializationLayout::FullWindow;
        target_config.aggregation_type = asap_types::AggregationType::DatasketchesKLL;
        target_config.aggregation_sub_type = "quantile".into();
        target_config
            .parameters
            .insert("k".into(), serde_json::json!(200));
        let target = target_config.policy_fingerprint().into();
        let target_family = target_config.accumulator_spec().unwrap().family;
        let configs = [source_config, target_config];
        let binding = BackendExecutableBinding {
            nodes: BTreeMap::from([
                (
                    PostAsapNodeId(1),
                    BackendNodeBinding::Materialization {
                        summary_definition: source_definition,
                    },
                ),
                (PostAsapNodeId(2), BackendNodeBinding::MaintenanceInput),
                (
                    PostAsapNodeId(3),
                    BackendNodeBinding::Materialization {
                        summary_definition: target,
                    },
                ),
            ]),
            query_sink: PostAsapNodeId(3),
            query_plan_sink: control_plane::query_plan::QueryNodeId(3),
            precompute_sinks: vec![PostAsapNodeId(3)],
        };
        let adapter = OperatorAdapter {
            binding: &binding,
            source_definition,
            source: sum(7.0),
            configs: &configs,
            immutable_windows: Some(vec![(1000, sum(7.0))].into()),
            singleton_population_complete: false,
        };
        let mut read = node(2);
        read.payload = ExecutableOperatorPayload::Value {
            operation: planner_types::post_asap::ValueOperation::FinalizeExactAccumulator,
            timing: planner_types::post_asap::ExecutionTiming::MaintenanceTime,
        };
        read.output_schema.fields = vec![SummaryField {
            name: "value".into(),
            dtype: SummaryFamilyType::Plain(DataType::Float64),
            nullable: false,
        }];
        let source = Arc::new(MaintenanceValue::Summary {
            state: sum(7.0),
            family: Some(SummaryFamilyType::ExactAggregate(
                ExactKind::Sum,
                ExactParams::Sum,
            )),
        });
        let row = adapter.execute(&read, &[source]).unwrap();
        let mut aggregate = node(3);
        aggregate.payload = ExecutableOperatorPayload::SummaryAgg {
            family: target_family,
            input: SummaryUpdate {
                item: None,
                weight: SummaryInputExpr::Constant(3.0),
                weight_domain: Default::default(),
            },
            reduction: Reduction::by(vec![]),
            grouping: GroupingStrategy::default(),
        };
        let result = adapter.execute(&aggregate, &[Arc::new(row)]).unwrap();
        let mut kwargs = std::collections::HashMap::new();
        kwargs.insert("quantile".into(), "0.5".into());
        assert_eq!(
            result
                .state()
                .unwrap()
                .query_statistic(asap_types::Statistic::Quantile, &None, &kwargs)
                .unwrap(),
            3.0
        );
        // Exercise the same registry through the production topological
        // scheduler, including the precomputed source frontier and commit.
        let mut source_node = node(1);
        source_node.output_schema.fields = vec![SummaryField {
            name: "state".into(),
            dtype: SummaryFamilyType::ExactAggregate(ExactKind::Sum, ExactParams::Sum),
            nullable: false,
        }];
        read.operator = read.payload.operator();
        read.output_state = planner_types::post_asap::ExecutionDataState::MAINTENANCE_ROWS;
        aggregate.operator = aggregate.payload.operator();
        aggregate.output_schema.fields = vec![SummaryField {
            name: "state".into(),
            dtype: configs[1].accumulator_spec().unwrap().family,
            nullable: false,
        }];
        let mut query = node(4);
        query.output_state = planner_types::post_asap::ExecutionDataState::READ_ROWS;
        let mut first_edge = edge(1, 2);
        first_edge.intermediate_schema = source_node.output_schema.clone();
        let mut second_edge = edge(2, 3);
        second_edge.intermediate_schema = read.output_schema.clone();
        second_edge.data_state = read.output_state;
        let mut query_edge = edge(3, 4);
        query_edge.intermediate_schema = aggregate.output_schema.clone();
        let dag = ExecutableDag {
            nodes: vec![source_node, read, aggregate, query],
            edges: vec![first_edge, second_edge, query_edge],
            root: PostAsapNodeId(4),
        };
        let mut scheduled_binding = binding.clone();
        scheduled_binding.nodes.insert(
            PostAsapNodeId(1),
            BackendNodeBinding::Materialization {
                summary_definition: source_definition,
            },
        );
        scheduled_binding
            .nodes
            .insert(PostAsapNodeId(2), BackendNodeBinding::MaintenanceInput);
        scheduled_binding.nodes.insert(
            PostAsapNodeId(4),
            BackendNodeBinding::Query {
                query_node: control_plane::query_plan::QueryNodeId(4),
            },
        );
        scheduled_binding.query_sink = PostAsapNodeId(4);
        scheduled_binding.query_plan_sink = control_plane::query_plan::QueryNodeId(4);
        let scheduled_adapter = OperatorAdapter {
            binding: &scheduled_binding,
            ..adapter
        };
        let key = MaterializationCommitKey {
            plan_id: 1,
            plan_version: 1,
            summary_definition: target,
            window_start_ms: 0,
            window_end_ms: 1000,
            input_lineage: vec![1],
        };
        let committed = execute_precompute_sink(
            &dag,
            &scheduled_binding,
            PostAsapNodeId(3),
            key,
            &scheduled_adapter,
            &CommitRegistry::default(),
        )
        .unwrap();
        assert_eq!(
            committed
                .state()
                .unwrap()
                .query_statistic(asap_types::Statistic::Quantile, &None, &kwargs)
                .unwrap(),
            3.0
        );
        // The real store provides the completion proof. Execute, persist,
        // restart, and retry the same installed subDAG without additive append.
        use crate::storage_engines::sketch_db::index::{
            persistence::config::SketchStorePersistenceConfig, SketchStore,
        };
        use asap_types::executable_plan::{InstalledPostAsapDag, OwnedPostAsapDag};
        let document = OwnedPostAsapDag::from_executable("immutable-chain".into(), &dag).unwrap();
        let mut durable_configs = configs.to_vec();
        durable_configs[1].derived_input = Some(
            asap_types::derived_input::DerivedInputIdentity::from_dag(
                &document,
                PostAsapNodeId(2),
                &BTreeMap::from([(PostAsapNodeId(1), source_definition)]),
            )
            .unwrap(),
        );
        let mut durable_binding = scheduled_binding.clone();
        durable_binding.nodes.insert(
            PostAsapNodeId(3),
            BackendNodeBinding::Materialization {
                summary_definition: durable_configs[1].policy_fingerprint().into(),
            },
        );
        let installed = InstalledPostAsapDag {
            document,
            binding: durable_binding,
        };
        let catalog = Arc::new(
            asap_types::summary_catalog::SummaryCatalog::from_materializations(
                1,
                1,
                &durable_configs,
            )
            .unwrap(),
        );
        let directory = tempfile::tempdir().unwrap();
        let persistence_config = || {
            let mut config = SketchStorePersistenceConfig::with_memory_limit(
                1 << 24,
                directory.path().to_path_buf(),
            );
            config.delete_older_than_ms = None;
            config.hot_window_ms = None;
            config.flush_interval = std::time::Duration::from_millis(5);
            config
        };
        let expected = BTreeSet::from([(0, 1000), (1000, 2000)]);
        let mut store = Arc::new(SketchStore::new());
        store.install_summary_catalog(Arc::clone(&catalog)).unwrap();
        let mut persistence = store.start_persistence(persistence_config()).unwrap();
        let generation = store.active_catalog_generation().unwrap();
        for ((start, end), value) in [((0, 1000), 2.0), ((1000, 2000), 7.0)] {
            let coordinate = asap_types::sds::SummaryInstanceCoordinates {
                summary_definition_id: source_definition,
                time_range: asap_types::sds::HalfOpenTimeRange {
                    start_ms: start,
                    end_ms: end,
                },
                group_values: BTreeMap::new(),
            };
            let revision = store
                .admit_summary_updates(&generation, BTreeSet::from([coordinate.clone()]))
                .unwrap();
            let mut output = PrecomputedOutput::new(
                start as u64,
                end as u64,
                None,
                durable_configs[0].policy_fingerprint(),
            );
            output.catalog_generation = Some(Arc::clone(&generation));
            store
                .publish_admitted_summary_update(
                    &generation,
                    &coordinate,
                    revision,
                    revision,
                    120_000,
                    |writer| {
                        writer.ingest_precompute_with_series_id(
                            600,
                            &durable_configs[0],
                            &output,
                            sum(value).as_ref(),
                        )
                    },
                )
                .unwrap();
        }
        assert!(store
            .read_frozen_exact_windows(
                600,
                source_definition,
                &generation,
                &expected,
                &BTreeMap::new()
            )
            .is_err());
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !store.seal_finite_summary_input(&generation).unwrap() {
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let mut rejected_output =
            PrecomputedOutput::new(2000, 3000, None, durable_configs[0].policy_fingerprint());
        rejected_output.catalog_generation = Some(Arc::clone(&generation));
        rejected_output.input_revision = Some(Arc::new(
            crate::storage_engines::types::precomputed_output::SummaryInputRevision {
                generation: Arc::clone(&generation),
                first_revision: 1,
                revision: 1,
            },
        ));
        assert!(store
            .ingest_precompute_with_series_id(
                600,
                &durable_configs[0],
                &rejected_output,
                sum(99.0).as_ref()
            )
            .is_none());
        assert_eq!(
            store
                .completed_maintenance_coordinates(source_definition, &generation)
                .unwrap(),
            BTreeMap::from([(600, BTreeMap::from([(BTreeMap::new(), expected.clone())]))])
        );
        let frozen = store
            .read_frozen_exact_windows(
                600,
                source_definition,
                &generation,
                &expected,
                &BTreeMap::new(),
            )
            .unwrap();
        let (result, digest) = evaluate_frozen_maintenance_sink(
            &installed,
            &durable_configs,
            PostAsapNodeId(3),
            &frozen,
            (0, 2000),
        )
        .unwrap();
        let mut output =
            PrecomputedOutput::new(0, 2000, None, durable_configs[1].policy_fingerprint());
        output.catalog_generation = Some(Arc::clone(&generation));
        let log = persistence.manifest.log_path();
        let backup = log.with_extension("saved");
        std::fs::rename(&log, &backup).unwrap();
        std::fs::create_dir(&log).unwrap();
        assert!(execute_completed_maintenance(
            &store,
            &installed,
            &durable_configs,
            PostAsapNodeId(3),
            600,
            601,
            (0, 2000),
            &BTreeMap::new()
        )
        .is_err());
        std::fs::remove_dir(&log).unwrap();
        std::fs::rename(&backup, &log).unwrap();
        persistence.shutdown();
        drop(store);
        store = Arc::new(SketchStore::new());
        store.install_summary_catalog(Arc::clone(&catalog)).unwrap();
        persistence = store.start_persistence(persistence_config()).unwrap();
        // The already durable pending KLL part is completed before a new
        // randomized sketch can be built after restart.
        assert!(!execute_completed_maintenance(
            &store,
            &installed,
            &durable_configs,
            PostAsapNodeId(3),
            600,
            601,
            (0, 2000),
            &BTreeMap::new(),
        )
        .unwrap());
        assert!(!execute_completed_maintenance(
            &store,
            &installed,
            &durable_configs,
            PostAsapNodeId(3),
            600,
            601,
            (0, 2000),
            &BTreeMap::new()
        )
        .unwrap());
        assert_eq!(
            result
                .query_statistic(asap_types::Statistic::Quantile, &None, &kwargs)
                .unwrap(),
            3.0
        );
        let mut correction =
            PrecomputedOutput::new(0, 1000, None, durable_configs[0].policy_fingerprint());
        correction.catalog_generation = Some(Arc::clone(&generation));
        assert!(store
            .ingest_precompute_with_series_id(
                600,
                &durable_configs[0],
                &correction,
                sum(100.0).as_ref()
            )
            .is_none());
        persistence.shutdown();
        drop(store);
        let restored = Arc::new(SketchStore::new());
        restored.install_summary_catalog(catalog).unwrap();
        let mut persistence = restored.start_persistence(persistence_config()).unwrap();
        let frozen = restored
            .read_frozen_exact_windows(
                600,
                source_definition,
                &generation,
                &expected,
                &BTreeMap::new(),
            )
            .unwrap();
        let (_result, replay_digest) = evaluate_frozen_maintenance_sink(
            &installed,
            &durable_configs,
            PostAsapNodeId(3),
            &frozen,
            (0, 2000),
        )
        .unwrap();
        assert_eq!(digest, replay_digest);
        assert!(!execute_completed_maintenance(
            &restored,
            &installed,
            &durable_configs,
            PostAsapNodeId(3),
            600,
            601,
            (0, 2000),
            &BTreeMap::new()
        )
        .unwrap());
        assert!(restored
            .ingest_precompute_with_series_id(
                600,
                &durable_configs[0],
                &correction,
                sum(100.0).as_ref()
            )
            .is_none());
        let target_entries = persistence
            .manifest
            .live_parts()
            .iter()
            .map(|part| {
                let path = crate::storage_engines::sketch_db::persistence::part::part_dir_path(
                    &persistence.parts_root,
                    part.part_id,
                );
                crate::storage_engines::sketch_db::persistence::part::PartReader::open(&path)
                    .unwrap()
                    .index_records()
                    .into_iter()
                    .filter(|entry| entry.agg_id == 601)
                    .count()
            })
            .sum::<usize>();
        assert_eq!(target_entries, 1);
        let next_catalog = Arc::new(
            asap_types::summary_catalog::SummaryCatalog::from_materializations(
                1,
                2,
                &durable_configs,
            )
            .unwrap(),
        );
        restored.install_summary_catalog(next_catalog).unwrap();
        let next_generation = restored.active_catalog_generation().unwrap();
        assert!(restored
            .series_ids_for_policy(durable_configs[1].policy_fingerprint())
            .is_empty());
        assert!(restored
            .authorize_series_reactivation(601, durable_configs[1].policy_fingerprint().into())
            .unwrap()
            .is_some());
        let mut new_population =
            PrecomputedOutput::new(0, 1000, None, durable_configs[0].policy_fingerprint());
        new_population.catalog_generation = Some(next_generation);
        assert_eq!(
            restored.ingest_precompute_with_series_id(
                602,
                &durable_configs[0],
                &new_population,
                sum(20.0).as_ref()
            ),
            Some(602)
        );
        assert!(restored
            .series_ids_for_policy(durable_configs[1].policy_fingerprint())
            .is_empty());
        persistence.shutdown();
        assert!(evaluate_weight(
            &SummaryInputExpr::Column(planner_types::pre_asap::ColumnRef::Named("missing".into())),
            7.0,
            "value"
        )
        .is_err());
    }

    #[test]
    fn finalization_preserves_windows_until_an_explicit_merge() {
        use planner_types::post_asap::{ExactKind, ExactParams, SummaryFamilyType, SummaryField};
        let family = SummaryFamilyType::ExactAggregate(ExactKind::Sum, ExactParams::Sum);
        let inputs = Arc::new(MaintenanceValue::SummaryWindows {
            states: vec![(1_000, sum(2.0)), (2_000, sum(7.0))].into(),
            family,
        });
        let mut read = node(2);
        read.output_schema.fields = vec![SummaryField {
            name: "value".into(),
            dtype: SummaryFamilyType::Plain(planner_types::pre_asap::DataType::Float64),
            nullable: false,
        }];
        let MaintenanceValue::Rows { values, .. } =
            finalize_exact(&read, &[inputs.clone()]).unwrap()
        else {
            panic!("expected finalized rows")
        };
        assert_eq!(values, vec![(1_000, 2.0), (2_000, 7.0)]);
        // Merge is a semantic DAG operation, not an implicit batch optimization.
        // Finalizing after it emits exactly one value instead of two updates.
        let merged = Arc::new(merge_inputs(&[inputs]).unwrap());
        let MaintenanceValue::Rows { values, .. } = finalize_exact(&read, &[merged]).unwrap()
        else {
            panic!("expected finalized row")
        };
        assert_eq!(values, vec![(2_000, 9.0)]);
        read.output_schema.fields[0].dtype =
            SummaryFamilyType::Plain(planner_types::pre_asap::DataType::Int64);
        let integer_state = Arc::new(MaintenanceValue::Summary {
            state: sum(9.0),
            family: Some(SummaryFamilyType::ExactAggregate(
                ExactKind::Sum,
                ExactParams::Sum,
            )),
        });
        assert!(
            matches!(finalize_exact(&read, &[integer_state]), Err(error) if error.contains("Float64"))
        );
    }

    #[test]
    fn finalization_preserves_declared_timestamp_and_rejects_ambiguous_columns() {
        use planner_types::post_asap::{ExactKind, ExactParams, SummaryFamilyType, SummaryField};
        use planner_types::pre_asap::DataType;
        let input = Arc::new(MaintenanceValue::SummaryWindows {
            states: vec![(60_000, sum(10.0))].into(),
            family: SummaryFamilyType::ExactAggregate(ExactKind::Sum, ExactParams::Sum),
        });
        let mut read = node(2);
        read.output_schema.fields = vec![
            SummaryField {
                name: "ts".into(),
                dtype: SummaryFamilyType::Plain(DataType::Timestamp),
                nullable: false,
            },
            SummaryField {
                name: "value".into(),
                dtype: SummaryFamilyType::Plain(DataType::Float64),
                nullable: false,
            },
        ];
        read.output_schema.time_index = Some(0);
        let MaintenanceValue::Rows { values, name } =
            finalize_exact(&read, &[Arc::clone(&input)]).unwrap()
        else {
            panic!("expected typed rows")
        };
        assert_eq!(values, vec![(60_000, 10.0)]);
        assert_eq!(name, "value");
        let mut malformed = Vec::new();
        let mut copy = read.clone();
        copy.output_schema.time_index = None;
        malformed.push(copy);
        let mut copy = read.clone();
        copy.output_schema.time_index = Some(2);
        malformed.push(copy);
        let mut copy = read.clone();
        copy.output_schema.time_index = Some(1);
        malformed.push(copy);
        let mut copy = read.clone();
        copy.output_schema
            .fields
            .push(copy.output_schema.fields[1].clone());
        malformed.push(copy);
        let mut copy = read.clone();
        copy.output_schema.fields[1].nullable = true;
        malformed.push(copy);
        let mut copy = read.clone();
        copy.output_schema.fields[1].name = "ts".into();
        malformed.push(copy);
        for malformed in malformed {
            assert!(finalize_exact(&malformed, &[Arc::clone(&input)]).is_err());
        }
        let untimed = Arc::new(MaintenanceValue::Summary {
            state: sum(10.0),
            family: Some(SummaryFamilyType::ExactAggregate(
                ExactKind::Sum,
                ExactParams::Sum,
            )),
        });
        assert!(finalize_exact(&read, &[untimed]).is_err());
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
            configs: &[],
            immutable_windows: None,
            singleton_population_complete: false,
        };
        let mut aggregate = node(1);
        aggregate.operator = ExecutableOperator::SummaryAgg;
        aggregate.payload = ExecutableOperatorPayload::SummaryAgg {
            family: SummaryFamilyType::ExactAggregate(ExactKind::Count, ExactParams::Count),
            input: SummaryUpdate::column(ColumnRef::SampleValue),
            reduction: Reduction::by(vec![]),
            grouping: GroupingStrategy::default(),
        };
        let error = adapter.execute(&aggregate, &[Arc::new(MaintenanceValue::summary(sum(7.0)))]);
        assert!(matches!(error, Err(reason) if reason.contains("typed update evaluator")));
    }

    #[test]
    fn admitted_slow_worker_can_publish_behind_another_workers_replay_floor() {
        let commits = CommitRegistry::default();
        commits.0.lock().unwrap().generation = Some((7, 1));
        let key = |end| MaterializationCommitKey {
            plan_id: 7,
            plan_version: 1,
            summary_definition: definition(2),
            window_start_ms: end - 10,
            window_end_ms: end,
            input_lineage: vec![0; 32],
        };
        let fast = key(1000);
        commits.begin_batch([1; 32]).unwrap();
        commits
            .commit_if_absent(fast.clone(), Arc::new(MaintenanceValue::summary(sum(2.0))))
            .unwrap();
        commits.publish(&fast, || Ok(())).unwrap();
        commits.complete_batch([1; 32], &[(fast, 30)]).unwrap();
        let slow = key(10);
        assert!(commits.is_published(&slow).is_err());
        commits.begin_batch([2; 32]).unwrap();
        commits.pin_admitted(&slow).unwrap();
        commits
            .commit_if_absent(slow.clone(), Arc::new(MaintenanceValue::summary(sum(3.0))))
            .unwrap();
        commits.publish(&slow, || Ok(())).unwrap();
        commits
            .complete_batch([2; 32], &[(slow.clone(), 30)])
            .unwrap();
        assert!(commits.is_published(&slow).is_err());
        assert!(commits.0.lock().unwrap().admitted_keys.is_empty());
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
                .commit_if_absent(key.clone(), Arc::new(MaintenanceValue::summary(sum(2.0))))
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
            .commit_if_absent(key(1_000), Arc::new(MaintenanceValue::summary(sum(2.0))))
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
                .commit_if_absent(key.clone(), Arc::new(MaintenanceValue::summary(sum(2.0))))
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
            configs: &[],
            immutable_windows: None,
            singleton_population_complete: false,
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
        assert_eq!(result.state().unwrap().aux_stats().sum, Some(4.0));
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
            configs: &[],
            immutable_windows: None,
            singleton_population_complete: false,
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

/// Materializations affected by an admitted source update, following installed
/// semantic dependencies rather than assuming source and output identities match.
pub(crate) fn affected_materializations(
    plan: &asap_types::precompute_plan::PrecomputePlan,
    source: asap_types::sds::SummaryDefinitionId,
) -> BTreeSet<asap_types::sds::SummaryDefinitionId> {
    use asap_types::executable_plan::BackendNodeBinding;
    let mut affected = BTreeSet::from([source]);
    for installed in plan.executable_dags.values() {
        let mut reachable = installed.binding.nodes.iter().filter_map(|(node, binding)| {
            matches!(binding, BackendNodeBinding::Materialization { summary_definition } if *summary_definition == source).then_some(*node)
        }).collect::<BTreeSet<_>>();
        let mut frontier = reachable.iter().copied().collect::<Vec<_>>();
        while let Some(producer) = frontier.pop() {
            for edge in &installed.document.edges {
                let immutable = matches!(installed.binding.node(edge.consumer),
                    Some(BackendNodeBinding::Materialization { summary_definition })
                        if plan.materializations.iter().any(|config|
                            config.policy_fingerprint() == summary_definition.fingerprint()
                                && config.derived_input.is_some()));
                if edge.producer == producer && !immutable && reachable.insert(edge.consumer) {
                    frontier.push(edge.consumer);
                }
            }
        }
        for sink in &installed.binding.precompute_sinks {
            if reachable.contains(sink) {
                if let Some(BackendNodeBinding::Materialization { summary_definition }) =
                    installed.binding.nodes.get(sink)
                {
                    affected.insert(*summary_definition);
                }
            }
        }
    }
    affected
}
