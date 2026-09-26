//! Production adapter from installed post-ASAP maintenance DAGs to summary state.

use super::output_sink::OutputSink;
use super::subdag_scheduler::{
    execute_precompute_sink, execute_precompute_sinks, IdempotentCommitSink,
    MaterializationCommitKey, PrecomputeOperatorRegistry, ScheduleError,
};
use crate::storage_engines::types::{
    AggregateCore, InstalledPrecomputePlanHandle, PrecomputedOutput,
};
use asap_physical_operators::dag::RunContext;
use asap_types::executable_plan::{BackendExecutableBinding, BackendNodeBinding};
use planner_types::post_asap::{ExecutableDagNode, ExecutableOperatorPayload, PostAsapNodeId};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

type SummaryState = Arc<dyn AggregateCore>;
type Population = BTreeMap<String, String>;
type PopulationStates = BTreeMap<Population, Arc<[(i64, SummaryState)]>>;
type PopulationRows = BTreeMap<Population, Vec<(i64, f64)>>;

#[derive(Clone)]
enum MaintenanceValue {
    Summary {
        state: SummaryState,
        family: Option<planner_types::post_asap::SummaryFamilyType>,
    },
    // A collection is retained until the DAG explicitly reduces it. Evaluating
    // the whole DAG once per source pane would change nested reductions.
    SummaryWindows {
        states: PopulationStates,
        family: planner_types::post_asap::SummaryFamilyType,
    },
    Rows {
        values: PopulationRows,
        name: String,
        timestamped: bool,
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
            Self::SummaryWindows { states, .. }
                if states.len() == 1
                    && states
                        .values()
                        .next()
                        .is_some_and(|windows| windows.len() == 1) =>
            {
                Ok(&states.values().next().unwrap()[0].1)
            }
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

enum MaintenanceInputs<'a> {
    Live {
        definition: asap_types::sds::StoredOutputId,
        state: SummaryState,
    },
    Frozen(&'a [crate::storage_engines::sketch_db::index::FrozenExactWindows]),
    Complete(&'a crate::storage_engines::sketch_db::index::CompleteRawMaintenanceCohort),
}

impl MaintenanceInputs<'_> {
    fn frozen_inputs(
        &self,
    ) -> Option<&[crate::storage_engines::sketch_db::index::FrozenExactWindows]> {
        match self {
            Self::Live { .. } => None,
            Self::Frozen(inputs) => Some(inputs),
            Self::Complete(cohort) => Some(cohort.inputs()),
        }
    }
}

fn frozen_population_value(
    inputs: &[crate::storage_engines::sketch_db::index::FrozenExactWindows],
    definition: asap_types::sds::StoredOutputId,
    family: Option<planner_types::post_asap::SummaryFamilyType>,
    complete: bool,
) -> Result<Option<MaintenanceValue>, String> {
    let mut states = PopulationStates::new();
    for input in inputs.iter().filter(|input| input.definition == definition) {
        if !complete && !states.is_empty() {
            return Err("maintenance frontier requires explicit population routing".into());
        }
        let windows = input
            .windows
            .iter()
            .map(|((_, end), state)| (*end as i64, Arc::clone(state)))
            .collect::<Vec<_>>()
            .into();
        if states.insert(input.group.clone(), windows).is_some() {
            return Err("maintenance frontier repeats a logical population".into());
        }
    }
    if states.is_empty() {
        return Ok(None);
    }
    Ok(Some(MaintenanceValue::SummaryWindows {
        states,
        family: family.ok_or("immutable source lacks a summary schema")?,
    }))
}

struct OperatorAdapter<'a> {
    binding: &'a BackendExecutableBinding,
    inputs: MaintenanceInputs<'a>,
    configs: &'a [asap_types::aggregation_config::PrecomputeMaterialization],
}

impl PrecomputeOperatorRegistry<MaintenanceValue> for OperatorAdapter<'_> {
    type Error = String;

    fn materialized_input(
        &self,
        node: &ExecutableDagNode,
    ) -> Result<Option<MaintenanceValue>, String> {
        let definition = match self.binding.node(node.id) {
            Some(BackendNodeBinding::Materialization { stored_output }) => *stored_output,
            _ => return Ok(None),
        };
        let family = node.output_schema.fields.iter().find_map(|field| {
            (!matches!(
                field.dtype,
                planner_types::post_asap::SummaryFamilyType::Plain(_)
            ))
            .then(|| field.dtype.clone())
        });
        match &self.inputs {
            MaintenanceInputs::Live {
                definition: source,
                state,
            } if definition == *source => Ok(Some(MaintenanceValue::Summary {
                state: Arc::clone(state),
                family,
            })),
            MaintenanceInputs::Live { .. } => Ok(None),
            MaintenanceInputs::Frozen(inputs) => {
                frozen_population_value(inputs, definition, family, false)
            }
            MaintenanceInputs::Complete(cohort) => {
                frozen_population_value(cohort.inputs(), definition, family, true)
            }
        }
    }

    fn output_bytes(&self, value: &MaintenanceValue) -> usize {
        fn labels(group: &Population) -> usize {
            group.iter().map(|(k, v)| k.len() + v.len()).sum()
        }
        match value {
            MaintenanceValue::Summary { state, .. } => state.approx_memory_bytes(),
            MaintenanceValue::SummaryWindows { states, .. } => states
                .iter()
                .map(|(group, windows)| {
                    labels(group)
                        + windows
                            .iter()
                            .map(|(_, state)| 8 + state.approx_memory_bytes())
                            .sum::<usize>()
                })
                .sum(),
            MaintenanceValue::Rows { values, name, .. } => {
                name.len()
                    + values
                        .iter()
                        .map(|(group, rows)| {
                            labels(group) + rows.len() * std::mem::size_of::<(i64, f64)>()
                        })
                        .sum::<usize>()
            }
        }
    }

    fn execute(
        &self,
        node: &ExecutableDagNode,
        inputs: &[Arc<MaintenanceValue>],
        context: RunContext,
    ) -> Result<MaintenanceValue, Self::Error> {
        if node.output_state.timing != planner_types::post_asap::ExecutionTiming::IngestionTime {
            return Err("ingestion executor received a query-time node".into());
        }
        match &node.payload {
            ExecutableOperatorPayload::SummaryMerge => merge_inputs(inputs, &context),
            ExecutableOperatorPayload::Binary { operator } => {
                if !self.inputs.frozen_inputs().is_some()
                    || node.output_state
                        != planner_types::post_asap::ExecutionDataState::INGESTION_ROWS
                {
                    return Err("maintenance binary requires immutable completed row inputs".into());
                }
                evaluate_aligned_binary(node, operator, inputs, &context)
            }

            ExecutableOperatorPayload::Value {
                operation: planner_types::post_asap::ValueOperation::FinalizeExactAccumulator,
            } => {
                if !self.inputs.frozen_inputs().is_some() {
                    return Err(
                        "maintenance finalization requires immutable completed input windows"
                            .into(),
                    );
                }
                finalize_exact(node, inputs, &context)
            }
            ExecutableOperatorPayload::SummaryAgg {
                family,
                input,
                grouping,
                ..
            } => {
                let [value] = inputs else {
                    return Err("maintenance SummaryAgg requires exactly one row input".into());
                };
                let MaintenanceValue::Rows { values, name, .. } = value.as_ref() else {
                    return Err("maintenance SummaryAgg requires a typed update evaluator; finalize summary state before applying an update".into());
                };
                if !self.inputs.frozen_inputs().is_some() {
                    return Err(
                        "maintenance aggregation requires immutable completed input windows".into(),
                    );
                }
                let target = match self.binding.node(node.id) {
                    Some(BackendNodeBinding::Materialization { stored_output }) => stored_output,
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
                asap_types::precompute_plan::validate_maintenance_reduction(config, node)?;
                let sources = self
                    .inputs
                    .frozen_inputs()
                    .ok_or("maintenance aggregation requires frozen sources")?;
                for source in sources {
                    let source_config = self
                        .configs
                        .iter()
                        .find(|config| {
                            config.policy_fingerprint() == source.definition.fingerprint()
                        })
                        .ok_or("maintenance input lacks installed source configuration")?;
                    validate_maintenance_grouping(
                        config,
                        source_config,
                        node,
                        source.singleton_population_complete
                            || matches!(&self.inputs, MaintenanceInputs::Complete(_)),
                    )?;
                }
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
                let _ = grouping;
                let output_groups = values
                    .keys()
                    .map(|group| {
                        if config.partitioning
                            == Some(asap_types::sds::PopulationPartitioning::PerEntity)
                        {
                            return Ok(group.clone());
                        }
                        config
                            .grouping_labels
                            .names()
                            .into_iter()
                            .map(|key| {
                                group
                                    .get(&key)
                                    .cloned()
                                    .map(|value| (key.to_string(), value))
                                    .ok_or_else(|| {
                                        "maintenance output grouping key is absent".to_string()
                                    })
                            })
                            .collect::<Result<Population, String>>()
                    })
                    .collect::<Result<BTreeSet<_>, String>>()?;
                if output_groups.len() != 1 {
                    return Err(
                        "maintenance sink requires one explicitly reduced output population".into(),
                    );
                }
                use asap_physical_operators::dag::{operators::Operator, values::Value};
                let schema = native_schema(vec![
                    (
                        "value",
                        planner_types::post_asap::SummaryFamilyType::Plain(
                            planner_types::pre_asap::DataType::Float64,
                        ),
                    ),
                    (
                        "time",
                        planner_types::post_asap::SummaryFamilyType::Plain(
                            planner_types::pre_asap::DataType::Timestamp,
                        ),
                    ),
                ]);
                let rows = values
                    .values()
                    .flatten()
                    .map(|(time, value)| {
                        Ok(vec![
                            Value::Float64(evaluate_weight(&input.weight, *value, name)?),
                            Value::Timestamp(*time),
                        ])
                    })
                    .collect::<Result<Vec<_>, String>>()?;
                let builder =
                    Operator::summary_build(schema.clone(), family.clone(), 0, Some(1), vec![])
                        .map_err(|e| e.to_string())?;
                let mut result = native_rows(schema, rows, vec![builder], &context)?;
                let Some(Value::Summary { state, .. }) = result.pop().and_then(|mut row| row.pop())
                else {
                    return Err("native summary builder did not return state".into());
                };
                let timestamp = values
                    .values()
                    .flatten()
                    .map(|(timestamp, _)| *timestamp)
                    .max()
                    .ok_or("maintenance aggregation has no input rows")?;
                Ok(MaintenanceValue::SummaryWindows {
                    states: BTreeMap::from([(
                        output_groups.into_iter().next().unwrap(),
                        vec![(timestamp, state)].into(),
                    )]),
                    family: family.clone(),
                })
            }
            payload => Err(format!(
                "maintenance operator {payload:?} has no summary-state implementation"
            )),
        }
    }
}

// These functions translate deployment values into native batches. Window and
// catalog checks stay here; the library owns all computation on batch values.
fn native_schema(
    fields: Vec<(&str, planner_types::post_asap::SummaryFamilyType)>,
) -> asap_physical_operators::dag::values::Schema {
    Arc::new(planner_types::post_asap::SummarySchema {
        fields: fields
            .into_iter()
            .map(|(name, dtype)| planner_types::post_asap::SummaryField {
                name: name.into(),
                dtype,
                nullable: false,
            })
            .collect(),
        time_index: None,
    })
}
fn native_rows(
    schema: asap_physical_operators::dag::values::Schema,
    rows: Vec<Vec<asap_physical_operators::dag::values::Value>>,
    operators: Vec<asap_physical_operators::dag::operators::Operator>,
    context: &RunContext,
) -> Result<Vec<Vec<asap_physical_operators::dag::values::Value>>, String> {
    use asap_physical_operators::dag::{batch_execution::evaluate_batch, values::Batch};
    let input = Batch::try_new(schema, rows).map_err(|e| e.to_string())?;
    Ok(evaluate_batch(input, operators, context.clone())
        .map_err(|e| e.to_string())?
        .into_iter()
        .flat_map(|b| b.rows().to_vec())
        .collect())
}
fn native_arithmetic(
    op: &planner_types::post_asap::BinaryOperator,
    inputs: Vec<(i64, f64, f64)>,
    context: &RunContext,
) -> Result<Vec<(i64, f64)>, String> {
    use asap_physical_operators::dag::{
        operators::{Expression, Operator},
        values::Value,
    };
    use planner_types::{post_asap::SummaryFamilyType, pre_asap::DataType};
    let schema = native_schema(vec![
        ("time", SummaryFamilyType::Plain(DataType::Timestamp)),
        ("left", SummaryFamilyType::Plain(DataType::Float64)),
        ("right", SummaryFamilyType::Plain(DataType::Float64)),
    ]);
    let project = Operator::project(
        schema.clone(),
        vec![
            ("time".into(), Expression::Column(0)),
            (
                "value".into(),
                Expression::Binary {
                    operator: op.clone(),
                    left: Box::new(Expression::Column(1)),
                    right: Box::new(Expression::Column(2)),
                },
            ),
        ],
    )
    .map_err(|e| e.to_string())?;
    let rows = native_rows(
        schema,
        inputs
            .into_iter()
            .map(|(time, left, right)| {
                vec![
                    Value::Timestamp(time),
                    Value::Float64(left),
                    Value::Float64(right),
                ]
            })
            .collect(),
        vec![project],
        context,
    )?;
    rows.into_iter()
        .map(|row| match row.as_slice() {
            [Value::Timestamp(time), Value::Float64(value)] if value.is_finite() => {
                Ok((*time, *value))
            }
            _ => Err("maintenance binary produced a non-finite update".into()),
        })
        .collect()
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

// Rows carry only a value and its window timestamp. Reject any schema that
// would require silently dropping another column or manufacturing a timestamp.
fn maintenance_float64_column(
    node: &ExecutableDagNode,
) -> Result<&planner_types::post_asap::SummaryField, String> {
    use planner_types::post_asap::SummaryFamilyType;
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
    Ok(field)
}

fn evaluate_aligned_binary(
    node: &ExecutableDagNode,
    operator: &planner_types::post_asap::BinaryOperator,
    inputs: &[Arc<MaintenanceValue>],
    context: &RunContext,
) -> Result<MaintenanceValue, String> {
    use planner_types::pre_asap::BinaryOpKind;
    let BinaryOpKind::Arithmetic(_) = &operator.kind else {
        return Err("maintenance binary currently requires arithmetic".into());
    };
    if operator.vector_match.is_some() {
        return Err(
            "maintenance binary requires explicit population routing for vector matching".into(),
        );
    }
    let name = maintenance_float64_column(node)?.name.clone();
    if node.output_schema.time_index.is_none() {
        return Err("maintenance binary requires declared window timestamps".into());
    }
    let [left, right] = inputs else {
        return Err("maintenance binary requires two row inputs".into());
    };
    let (
        MaintenanceValue::Rows {
            values: left,
            timestamped: true,
            ..
        },
        MaintenanceValue::Rows {
            values: right,
            timestamped: true,
            ..
        },
    ) = (left.as_ref(), right.as_ref())
    else {
        return Err("maintenance binary requires finalized row inputs".into());
    };
    if left.is_empty() || left.keys().ne(right.keys()) {
        return Err("maintenance binary requires matching nonempty population sets".into());
    }
    let mut values = PopulationRows::new();
    for (group, left) in left {
        let right = &right[group];
        if left.is_empty() || left.len() != right.len() {
            return Err("maintenance binary requires matching nonempty timestamp sets".into());
        }
        // Canonical timestamp maps accept arrival-order differences, but never
        // collapse duplicate updates or pair unrelated source windows by position.
        let mut left_rows = BTreeMap::new();
        let mut right_rows = BTreeMap::new();
        for (rows, index) in [(left, &mut left_rows), (right, &mut right_rows)] {
            for &(timestamp, value) in rows {
                if !value.is_finite() || index.insert(timestamp, value).is_some() {
                    return Err(
                        "maintenance binary input has duplicate timestamps or non-finite values"
                            .into(),
                    );
                }
            }
        }
        let mut joined = Vec::with_capacity(left.len());
        for (timestamp, left) in left_rows {
            let right = right_rows
                .get(&timestamp)
                .ok_or("maintenance binary requires matching timestamp sets")?;
            joined.push((timestamp, left, *right));
        }
        let joined = native_arithmetic(operator, joined, context)?;
        values.insert(group.clone(), joined);
    }
    Ok(MaintenanceValue::Rows {
        values,
        name,
        timestamped: true,
    })
}

fn finalize_exact(
    node: &ExecutableDagNode,
    inputs: &[Arc<MaintenanceValue>],
    context: &RunContext,
) -> Result<MaintenanceValue, String> {
    use planner_types::post_asap::{ExactKind, SummaryFamilyType};
    let [input] = inputs else {
        return Err("exact maintenance finalization requires one summary input".into());
    };
    let (groups, family) = match input.as_ref() {
        MaintenanceValue::Summary {
            state,
            family: Some(family),
        } => {
            if node.output_schema.time_index.is_some() {
                return Err("timestamped finalization requires source window timestamps".into());
            }
            (vec![(Population::new(), vec![(0, state)])], family)
        }
        MaintenanceValue::SummaryWindows { states, family } => (
            states
                .iter()
                .map(|(group, windows)| {
                    (
                        group.clone(),
                        windows.iter().map(|(time, state)| (*time, state)).collect(),
                    )
                })
                .collect(),
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
    let field = maintenance_float64_column(node)?;
    let mut values = PopulationRows::new();
    for (group, states) in groups {
        use asap_physical_operators::dag::{operators::Operator, values::Value};
        let schema = native_schema(vec![("state", family.clone())]);
        let readout = Operator::readout(schema.clone(), 0, statistic, Default::default())
            .map_err(|e| e.to_string())?;
        let rows = native_rows(
            schema,
            states
                .iter()
                .map(|(_, state)| {
                    vec![Value::Summary {
                        family: family.clone(),
                        state: Arc::clone(state),
                    }]
                })
                .collect(),
            vec![readout],
            context,
        )?;
        let rows = states
            .into_iter()
            .zip(rows)
            .map(|((timestamp, _), row)| {
                let value =
                    match row.first() {
                        Some(Value::Float64(value)) => *value,
                        Some(Value::Int64(value)) if value.unsigned_abs() <= (1u64 << 53) => {
                            *value as f64
                        }
                        _ => return Err(
                            "exact readout cannot be represented by the installed Float64 schema"
                                .to_string(),
                        ),
                    };
                if !value.is_finite() {
                    return Err("exact maintenance finalization produced a non-finite value".into());
                }
                Ok((timestamp, value))
            })
            .collect::<Result<Vec<_>, String>>()?;
        values.insert(group, rows);
    }
    Ok(MaintenanceValue::Rows {
        values,
        name: field.name.clone(),
        timestamped: node.output_schema.time_index.is_some()
            && matches!(input.as_ref(), MaintenanceValue::SummaryWindows { .. }),
    })
}

fn merge_inputs(
    inputs: &[Arc<MaintenanceValue>],
    context: &RunContext,
) -> Result<MaintenanceValue, String> {
    let mut grouped = BTreeMap::<Population, (Vec<&SummaryState>, Option<i64>)>::new();
    let mut expected_groups = None;
    let mut family = None;
    for input in inputs {
        let (groups, input_family) = match input.as_ref() {
            MaintenanceValue::Summary { state, family } => (
                vec![(Population::new(), vec![(None, state)])],
                family.as_ref(),
            ),
            MaintenanceValue::SummaryWindows { states, family } => (
                states
                    .iter()
                    .map(|(group, windows)| {
                        (
                            group.clone(),
                            windows
                                .iter()
                                .map(|(time, state)| (Some(*time), state))
                                .collect(),
                        )
                    })
                    .collect(),
                Some(family),
            ),
            MaintenanceValue::Rows { .. } => return Err("summary merge cannot consume rows".into()),
        };
        let keys: BTreeSet<_> = groups.iter().map(|(group, _)| group.clone()).collect();
        if expected_groups
            .as_ref()
            .is_some_and(|expected| expected != &keys)
        {
            return Err("summary merge cannot collapse different populations".into());
        }
        expected_groups = Some(keys);
        for (group, windows) in groups {
            let (states, end) = grouped.entry(group).or_default();
            for (timestamp, state) in windows {
                states.push(state);
                *end = (*end).max(timestamp);
            }
        }
        if let Some(input_family) = input_family {
            if family.as_ref().is_some_and(|family| family != input_family) {
                return Err("summary merge input families differ".into());
            }
            family = Some(input_family.clone());
        }
    }
    if grouped.is_empty() {
        return Err("summary maintenance node has no input state".into());
    }
    let mut result = PopulationStates::new();
    for (group, (states, timestamp)) in grouped {
        let Some((first, rest)) = states.split_first() else {
            return Err("summary maintenance population has no input state".into());
        };
        let state_family = family.clone().or_else(|| {
            first.as_any().downcast_ref::<asap_physical_operators::summary_kernels::exact::ExactAccumulator>().map(|state| state.family().clone())
        }).or_else(|| first.as_any().is::<asap_physical_operators::summary_kernels::SumAccumulator>().then_some(
            planner_types::post_asap::SummaryFamilyType::ExactAggregate(planner_types::post_asap::ExactKind::Sum, planner_types::post_asap::ExactParams::Sum)
        )).ok_or("summary merge requires a registered state family")?;
        use asap_physical_operators::dag::{operators::Operator, values::Value};
        let schema = native_schema(vec![("state", state_family.clone())]);
        let rows = std::iter::once(*first)
            .chain(rest.iter().copied())
            .map(|state| {
                vec![Value::Summary {
                    family: state_family.clone(),
                    state: Arc::clone(state),
                }]
            })
            .collect();
        let merge =
            Operator::summary_merge(schema.clone(), 0, vec![]).map_err(|e| e.to_string())?;
        let mut output = native_rows(schema, rows, vec![merge], context)?;
        let Some(Value::Summary { state: merged, .. }) = output.pop().and_then(|mut row| row.pop())
        else {
            return Err("native summary merge did not return state".into());
        };
        let Some(timestamp) = timestamp else {
            return Ok(MaintenanceValue::Summary {
                state: merged,
                family,
            });
        };
        result.insert(group, vec![(timestamp, merged)].into());
    }
    Ok(MaintenanceValue::SummaryWindows {
        states: result,
        family: family.ok_or("merged immutable state lacks a family")?,
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
        (&left.stored_output_reference, &left.group)
            .cmp(&(&right.stored_output_reference, &right.group))
    });
    if ordered.windows(2).any(|pair| {
        (&pair[0].stored_output_reference, &pair[0].group)
            == (&pair[1].stored_output_reference, &pair[1].group)
    }) {
        return Err("immutable lineage repeats a stored-output population".into());
    }
    let multiple = ordered.len() > 1;
    let mut lineage = Sha256::new();
    lineage.update(b"immutable-stored-output-input-v3");
    lineage.update((ordered.len() as u64).to_be_bytes());
    for input in ordered {
        if &input.generation != generation || input.windows.is_empty() {
            return Err("immutable lineage has mixed generations or empty windows".into());
        }
        if input.stored_output_reference.stored_output_id != input.definition {
            return Err("immutable input output differs from its definition".into());
        }
        let metadata = serde_json::to_vec(&(
            &input.stored_output_reference,
            &input.generation,
            &input.group,
            expected,
        ))
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
    inputs: &[crate::storage_engines::sketch_db::index::FrozenExactWindows],
    output_window: (u64, u64),
) -> Result<
    (
        planner_types::post_asap::ExecutableDag,
        MaterializationCommitKey,
    ),
    String,
> {
    installed.validate()?;
    let target = match installed.binding.node(sink) {
        Some(BackendNodeBinding::Materialization { stored_output }) => *stored_output,
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
    if expected_input.inputs != inputs.iter().map(|input| input.definition).collect() {
        return Err("immutable sink requires synchronized input definitions".into());
    }
    if output_window.0 >= output_window.1
        || output_window.1 - output_window.0 != config.stored_window_ms()
        || inputs.iter().any(|input| {
            input
                .windows
                .keys()
                .any(|(start, end)| *start < output_window.0 || *end > output_window.1)
        })
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
            BackendNodeBinding::Materialization { stored_output }
                if expected_input.inputs.contains(stored_output) =>
            {
                Some((*node, *stored_output))
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
    let digest = frozen_cohort_lineage(inputs, expected_input)?;
    let generation = &inputs
        .first()
        .ok_or("immutable input cohort is empty")?
        .generation;
    let key = MaterializationCommitKey {
        plan_id: generation.plan_id,
        plan_version: generation.plan_version,
        stored_output: target,
        window_start_ms: i64::try_from(output_window.0)
            .map_err(|_| "output window exceeds timestamp range")?,
        window_end_ms: i64::try_from(output_window.1)
            .map_err(|_| "output window exceeds timestamp range")?,
        input_lineage: digest.to_vec(),
    };
    Ok((dag, key))
}

fn execute_prepared_frozen_sink(
    installed: &asap_types::executable_plan::InstalledPostAsapDag,
    configs: &[asap_types::PrecomputeMaterialization],
    sink: PostAsapNodeId,
    inputs: MaintenanceInputs<'_>,
    dag: &planner_types::post_asap::ExecutableDag,
    key: MaterializationCommitKey,
) -> Result<(SummaryState, Population), String> {
    let adapter = OperatorAdapter {
        binding: &installed.binding,
        inputs,
        configs,
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
    let group = match value.as_ref() {
        MaintenanceValue::SummaryWindows { states, .. } if states.len() == 1 => {
            states.keys().next().unwrap().clone()
        }
        MaintenanceValue::Summary { .. } => Population::new(),
        _ => return Err("maintenance sink has multiple or missing output populations".into()),
    };
    Ok((Arc::clone(value.state()?), group))
}

#[cfg(test)]
fn evaluate_frozen_maintenance_sink(
    installed: &asap_types::executable_plan::InstalledPostAsapDag,
    configs: &[asap_types::PrecomputeMaterialization],
    sink: PostAsapNodeId,
    input: &crate::storage_engines::sketch_db::index::FrozenExactWindows,
    output_window: (u64, u64),
) -> Result<(SummaryState, [u8; 32]), String> {
    let (dag, key) = prepare_frozen_maintenance_sink(
        installed,
        configs,
        sink,
        std::slice::from_ref(input),
        output_window,
    )?;
    let digest = key
        .input_lineage
        .as_slice()
        .try_into()
        .map_err(|_| "invalid input digest")?;
    let (state, _) = execute_prepared_frozen_sink(
        installed,
        configs,
        sink,
        MaintenanceInputs::Frozen(std::slice::from_ref(input)),
        &dag,
        key,
    )?;
    Ok((state, digest))
}

#[cfg(test)]
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
    let target = match installed.binding.node(sink) {
        Some(BackendNodeBinding::Materialization { stored_output }) => *stored_output,
        _ => return Err("maintenance sink lacks an installed output identity".into()),
    };
    let derived = configs
        .iter()
        .find(|config| config.policy_fingerprint() == target.fingerprint())
        .and_then(|config| config.derived_input.as_ref())
        .ok_or("maintenance output has no derived input")?;
    if derived.inputs.len() != 1 {
        return Err("maintenance execution requires synchronized multi-source scheduling".into());
    }
    let generation = store
        .active_catalog_generation()
        .ok_or("maintenance requires an authoritative catalog")?;
    execute_completed_maintenance_cohort(
        store,
        &generation,
        installed,
        configs,
        sink,
        &BTreeMap::from([(*derived.inputs.first().unwrap(), source_sid)]),
        target_sid,
        window,
        group,
    )
}

/// Execute one completed population per input definition at matching full
/// windows. All source populations must share the same explicit label map.
#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_completed_maintenance_cohort(
    store: &crate::storage_engines::sketch_db::index::SketchStore,
    generation: &Arc<asap_types::sds::CatalogGeneration>,
    installed: &asap_types::executable_plan::InstalledPostAsapDag,
    configs: &[asap_types::PrecomputeMaterialization],
    sink: PostAsapNodeId,
    source_sids: &BTreeMap<asap_types::sds::StoredOutputId, u64>,
    target_sid: u64,
    window: (u64, u64),
    group: &BTreeMap<String, String>,
) -> Result<bool, String> {
    use asap_types::executable_plan::BackendNodeBinding;
    let target = match installed.binding.node(sink) {
        Some(BackendNodeBinding::Materialization { stored_output }) => *stored_output,
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
    if derived.inputs != source_sids.keys().copied().collect() {
        return Err("maintenance source set differs from installed input definitions".into());
    }
    let source_configs = source_sids
        .keys()
        .map(|source| {
            configs
                .iter()
                .find(|config| config.policy_fingerprint() == source.fingerprint())
                .ok_or("maintenance source configuration is absent")
        })
        .collect::<Result<Vec<_>, _>>()?;
    if source_configs.len() > 1 {
        asap_types::precompute_plan::validated_source_window_cohort(target_config, &source_configs)
            .map_err(|error| error.to_string())?;
    }
    let mut requests = Vec::with_capacity(source_sids.len());
    for source_config in &source_configs {
        let source = source_config.policy_fingerprint().into();
        let source_sid = source_sids[&source];
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
        requests.push((source_sid, source, expected, group.clone()));
    }
    let cohort = store.read_frozen_exact_cohort(generation, &derived.inputs, &requests)?;
    if cohort.len() > 1
        && cohort
            .iter()
            .any(|source| !source.singleton_population_complete)
    {
        return Err(
            "multi-source maintenance requires a complete single population per source".into(),
        );
    }
    let (dag, key) = prepare_frozen_maintenance_sink(installed, configs, sink, &cohort, window)?;
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
    for frozen in &cohort {
        let source_config = source_configs
            .iter()
            .find(|config| config.policy_fingerprint() == frozen.definition.fingerprint())
            .ok_or("maintenance source configuration is absent")?;
        validate_maintenance_grouping(
            target_config,
            source_config,
            target_node,
            frozen.singleton_population_complete,
        )?;
    }
    if store.recover_frozen_maintenance_output(
        target_sid,
        target_config,
        &cohort,
        digest,
        window,
    )? {
        return Ok(false);
    }
    let (state, computed_group) = execute_prepared_frozen_sink(
        installed,
        configs,
        sink,
        MaintenanceInputs::Frozen(&cohort),
        &dag,
        key,
    )?;
    let expected_group =
        if target_config.partitioning == Some(asap_types::sds::PopulationPartitioning::PerEntity) {
            group.clone()
        } else {
            target_config
                .grouping_labels
                .names()
                .into_iter()
                .map(|key| {
                    group
                        .get(&key)
                        .cloned()
                        .map(|value| (key, value))
                        .ok_or("maintenance output grouping key is absent")
                })
                .collect::<Result<Population, _>>()?
        };
    if computed_group != expected_group {
        return Err("computed maintenance population differs from its output binding".into());
    }
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
    output.catalog_generation = Some(Arc::clone(generation));
    store.publish_frozen_maintenance_output(
        target_sid,
        target_config,
        &output,
        state.as_ref(),
        &cohort,
        digest,
    )
}

/// Schedule a complete common population across all raw input definitions.
/// Missing windows are unavailable; they are never interpreted as zero values.
fn execute_finite_source_cohort(
    store: &crate::storage_engines::sketch_db::index::SketchStore,
    resolver: &crate::drivers::ingest::series_resolver::SeriesIdResolver,
    plan: &asap_types::precompute_plan::PrecomputePlan,
    installed: &asap_types::executable_plan::InstalledPostAsapDag,
    sink: PostAsapNodeId,
) -> Result<(), String> {
    let generation = plan
        .summary_catalog
        .as_ref()
        .ok_or("finite maintenance requires a catalog generation")?;
    let Some(BackendNodeBinding::Materialization {
        stored_output: target,
    }) = installed.binding.node(sink)
    else {
        return Err("finite maintenance sink has no installed definition".into());
    };
    let config = plan
        .materializations
        .iter()
        .find(|config| config.policy_fingerprint() == target.fingerprint())
        .ok_or("finite maintenance target configuration is absent")?;
    let derived = config
        .derived_input
        .as_ref()
        .ok_or("finite maintenance target has no input program")?;
    let source_configs = derived
        .inputs
        .iter()
        .map(|source| {
            plan.materializations
                .iter()
                .find(|config| config.policy_fingerprint() == source.fingerprint())
                .ok_or("finite maintenance source configuration is absent")
        })
        .collect::<Result<Vec<_>, _>>()?;
    asap_types::precompute_plan::validated_source_window_cohort(config, &source_configs)
        .map_err(|error| error.to_string())?;
    let width = config.stored_window_ms();
    if config.slide_interval.checked_mul(1000) != Some(width) {
        return Err("finite source cohorts currently require non-overlapping full windows".into());
    }
    let active_generation = store
        .active_catalog_generation()
        .ok_or("finite source cohort requires an active catalog")?;
    if active_generation.as_ref() != generation {
        return Err("finite source cohort catalog generation changed".into());
    }
    let mut source_sids = BTreeMap::new();
    let mut common_group = None;
    let mut common_windows: Option<BTreeSet<(u64, u64)>> = None;
    for source in &derived.inputs {
        let populations = store.completed_maintenance_coordinates(*source, generation)?;
        if populations.is_empty() {
            return Ok(());
        }
        if populations.len() != 1 || populations.values().any(|groups| groups.len() != 1) {
            return Err(
                "finite source cohort requires one physical population per definition".into(),
            );
        }
        let (sid, groups) = populations.into_iter().next().unwrap();
        let (group, windows) = groups.into_iter().next().unwrap();
        if common_group
            .as_ref()
            .is_some_and(|expected| expected != &group)
        {
            return Err("finite source cohort requires matching explicit populations".into());
        }
        if windows
            .iter()
            .any(|(start, end)| end.checked_sub(*start) != Some(width))
        {
            return Err("finite source cohort contains a non-full source window".into());
        }
        common_group = Some(group);
        source_sids.insert(*source, sid);
        match &mut common_windows {
            Some(common) => common.retain(|window| windows.contains(window)),
            None => common_windows = Some(windows),
        }
    }
    let Some(group) = common_group else {
        return Ok(());
    };
    let output_group: BTreeMap<_, _> = config
        .grouping_labels
        .iter()
        .map(|key| {
            group
                .get(key)
                .cloned()
                .map(|value| (key.clone(), value))
                .ok_or("finite maintenance output grouping key is absent")
        })
        .collect::<Result<_, _>>()?;
    let existing = store.completed_maintenance_coordinates(*target, generation)?;
    for window in common_windows.unwrap_or_default() {
        if (window.0 as i128 - config.pane_origin_ms.unwrap_or(0) as i128).rem_euclid(width as i128)
            != 0
        {
            continue;
        }
        // All definitions and populations are proven before resolving any
        // target. The publication transaction revalidates every lifetime.
        let requests = source_sids
            .iter()
            .map(|(definition, sid)| (*sid, *definition, BTreeSet::from([window]), group.clone()))
            .collect::<Vec<_>>();
        let cohort =
            store.read_frozen_exact_cohort(&active_generation, &derived.inputs, &requests)?;
        if cohort
            .iter()
            .any(|input| !input.singleton_population_complete)
        {
            return Err("finite source cohort has incomplete population proof".into());
        }
        let pairs = output_group
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect::<Vec<_>>();
        let attrs = crate::drivers::ingest::population_attrs_fingerprint(
            config.population_key_encoding,
            &pairs,
        )?;
        let target_sid =
            store.resolve_output_storage_handle(resolver, *target, &attrs, Some(generation))?;
        if existing
            .get(&target_sid)
            .and_then(|groups| groups.get(&output_group))
            .is_some_and(|windows| windows.contains(&window))
        {
            continue;
        }
        execute_completed_maintenance_cohort(
            store,
            &active_generation,
            installed,
            &plan.materializations,
            sink,
            &source_sids,
            target_sid,
            window,
            &group,
        )?;
    }
    Ok(())
}

/// Evaluate every raw population before one global immutable publication.
/// Canonical source identities are mandatory; legacy plans retain their
/// existing singleton path and cannot silently reinterpret old resolver keys.
fn execute_finite_complete_populations(
    store: &crate::storage_engines::sketch_db::index::SketchStore,
    resolver: &crate::drivers::ingest::series_resolver::SeriesIdResolver,
    plan: &asap_types::precompute_plan::PrecomputePlan,
    installed: &asap_types::executable_plan::InstalledPostAsapDag,
    sink: PostAsapNodeId,
    generation: &Arc<asap_types::sds::CatalogGeneration>,
) -> Result<(), String> {
    let target = match installed.binding.node(sink) {
        Some(BackendNodeBinding::Materialization { stored_output }) => *stored_output,
        _ => return Err("complete maintenance target is not bound".into()),
    };
    let config = plan
        .materializations
        .iter()
        .find(|config| config.policy_fingerprint() == target.fingerprint())
        .ok_or("complete target config is absent")?;
    let derived = config
        .derived_input
        .as_ref()
        .ok_or("complete target is not derived")?;
    let sources = derived
        .inputs
        .iter()
        .map(|definition| {
            plan.materializations
                .iter()
                .find(|source| source.policy_fingerprint() == definition.fingerprint())
                .ok_or("complete source config is absent")
        })
        .collect::<Result<Vec<_>, _>>()?;
    asap_types::precompute_plan::validated_source_window_cohort(config, &sources)
        .map_err(|error| error.to_string())?;
    if std::iter::once(config)
        .chain(sources.iter().copied())
        .any(|config| {
            config.population_key_encoding != asap_types::PopulationKeyEncoding::CanonicalLabelsV1
                || config.slide_interval.checked_mul(1000) != Some(config.stored_window_ms())
        })
    {
        return Err(
            "complete population execution requires canonical nonoverlapping full windows".into(),
        );
    }
    let dag = installed.document.decode()?;
    let target_node = dag
        .nodes
        .iter()
        .find(|node| node.id == sink)
        .ok_or("complete target node is absent")?;
    asap_types::precompute_plan::validate_maintenance_reduction(config, target_node)?;
    if !matches!(&target_node.payload, ExecutableOperatorPayload::SummaryAgg {
        reduction: planner_types::pre_asap::Reduction::Reduce(keys), ..
    } if keys.is_empty())
    {
        return Err("complete population execution currently requires one global reduction".into());
    }
    let mut common_windows: Option<BTreeSet<(u64, u64)>> = None;
    let mut common_groups: Option<BTreeSet<Population>> = None;
    for source in &sources {
        let inventory = store
            .complete_raw_maintenance_population(source.policy_fingerprint().into(), generation)?;
        let mut groups = BTreeSet::new();
        for populations in inventory.values() {
            for (group, windows) in populations {
                if !groups.insert(group.clone()) {
                    return Err(
                        "complete source repeats a logical population across lifetimes".into(),
                    );
                }
                for (start, end) in windows {
                    let width = source.stored_window_ms();
                    if width == 0
                        || end.checked_sub(*start) != Some(width)
                        || *end > i64::MAX as u64
                        || (*start as i128 - source.pane_origin_ms.unwrap_or(0) as i128)
                            .rem_euclid(width as i128)
                            != 0
                    {
                        return Err(
                            "complete source window is not an aligned stored full window".into(),
                        );
                    }
                }
                common_windows = Some(match common_windows {
                    None => windows.clone(),
                    Some(previous) => previous.intersection(windows).copied().collect(),
                });
            }
        }
        if common_groups
            .as_ref()
            .is_some_and(|expected| expected != &groups)
        {
            return Err("complete arithmetic sources have different population sets".into());
        }
        common_groups = Some(groups);
    }
    for window in common_windows.unwrap_or_default() {
        let cohort =
            store.read_complete_raw_maintenance_cohort(generation, &derived.inputs, window)?;
        let (dag, key) = prepare_frozen_maintenance_sink(
            installed,
            &plan.materializations,
            sink,
            cohort.inputs(),
            window,
        )?;
        let digest: [u8; 32] = key
            .input_lineage
            .as_slice()
            .try_into()
            .map_err(|_| "complete maintenance digest is invalid")?;
        let output_group = Population::new();
        let attrs = crate::drivers::ingest::population_attrs_fingerprint(
            config.population_key_encoding,
            &[],
        )?;
        let target_sid =
            store.resolve_output_storage_handle(resolver, target, &attrs, Some(generation))?;
        if store
            .completed_maintenance_coordinates(target, generation)?
            .get(&target_sid)
            .and_then(|groups| groups.get(&output_group))
            .is_some_and(|windows| windows.contains(&window))
        {
            continue;
        }
        if store
            .recover_complete_raw_maintenance_output(target_sid, config, &cohort, digest, window)?
        {
            continue;
        }
        let (state, computed_group) = execute_prepared_frozen_sink(
            installed,
            &plan.materializations,
            sink,
            MaintenanceInputs::Complete(&cohort),
            &dag,
            key,
        )?;
        if computed_group != output_group {
            return Err("computed complete population differs from the bound global output".into());
        }
        let mut output = crate::storage_engines::types::PrecomputedOutput::new(
            window.0,
            window.1,
            Some(crate::storage_engines::types::KeyByLabelValues { labels: Vec::new() }),
            target.fingerprint(),
        );
        output.population_labels = Some(computed_group);
        output.catalog_generation = Some(Arc::clone(generation));
        store.publish_complete_raw_maintenance_output(
            target_sid,
            config,
            &output,
            state.as_ref(),
            &cohort,
            digest,
        )?;
    }
    Ok(())
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
    let active_generation = store
        .active_catalog_generation()
        .ok_or("finite maintenance requires an active catalog")?;
    if active_generation.as_ref() != generation {
        return Err("finite maintenance catalog generation changed".into());
    }
    for installed in plan.executable_dags.values() {
        for sink in &installed.binding.precompute_sinks {
            let Some(BackendNodeBinding::Materialization {
                stored_output: target,
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
            if config.population_key_encoding
                == asap_types::PopulationKeyEncoding::CanonicalLabelsV1
            {
                execute_finite_complete_populations(
                    store,
                    resolver,
                    plan,
                    installed,
                    *sink,
                    &active_generation,
                )?;
                continue;
            }
            if derived.inputs.len() > 1 {
                execute_finite_source_cohort(store, resolver, plan, installed, *sink)?;
                continue;
            }
            if derived.inputs.is_empty() {
                return Err("finite maintenance requires a source definition".into());
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
                    let attrs = crate::drivers::ingest::population_attrs_fingerprint(
                        config.population_key_encoding,
                        &pairs,
                    )?;
                    let target_sid = store.resolve_output_storage_handle(
                        resolver,
                        *target,
                        &attrs,
                        Some(generation),
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
                        execute_completed_maintenance_cohort(
                            store,
                            &active_generation,
                            installed,
                            &plan.materializations,
                            *sink,
                            &BTreeMap::from([(source, source_sid)]),
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
    frontiers: BTreeMap<asap_types::sds::StoredOutputId, (i64, u64)>,
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
                .get(&key.stored_output)
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
        plans: &InstalledPrecomputePlanHandle,
    ) -> Result<Option<Arc<crate::storage_engines::types::RuntimePhysicalPlan>>, String> {
        let mut state = self.0.lock().map_err(|_| "commit registry poisoned")?;
        // Read the authoritative generation while holding the registry lock,
        // so an old in-flight batch cannot restore an obsolete generation.
        let plan = plans.active_physical_plan_snapshot();
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
                .entry(key.stored_output)
                .or_insert((key.window_end_ms, *horizon));
            frontier.0 = frontier.0.max(key.window_end_ms);
            frontier.1 = frontier.1.max(*horizon);
        }
        let frontiers = state.frontiers.clone();
        state.entries.retain(|key, _| {
            frontiers
                .get(&key.stored_output)
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
    plans: InstalledPrecomputePlanHandle,
    commits: CommitRegistry,
    batch_guard: Mutex<()>,
}

impl MaintenanceDagSink {
    pub fn new(inner: Arc<dyn OutputSink>, plans: InstalledPrecomputePlanHandle) -> Self {
        Self {
            inner,
            plans,
            commits: CommitRegistry::default(),
            batch_guard: Mutex::new(()),
        }
    }

    fn execute_one(
        &self,
        plan: &crate::storage_engines::types::RuntimePhysicalPlan,
        output: PrecomputedOutput,
        state: Box<dyn AggregateCore>,
    ) -> Result<Vec<PendingOutput>, String> {
        let source_definition: asap_types::sds::StoredOutputId = output.policy_fp.into();
        let source: SummaryState = Arc::from(state);
        let mut derived = Vec::new();
        let mut matched = false;
        let mut lineage = Sha256::new();
        lineage.update(b"asap-maintenance-lineage-v1");
        let definition_bytes = source_definition.0.to_be_bytes();
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
                .filter_map(|(id, binding)| matches!(binding, BackendNodeBinding::Materialization { stored_output } if *stored_output == source_definition).then_some(*id))
                .collect::<BTreeSet<_>>();
            if source_nodes.is_empty() {
                continue;
            }
            let adapter = OperatorAdapter {
                binding: &installed.binding,
                inputs: MaintenanceInputs::Live {
                    definition: source_definition,
                    state: Arc::clone(&source),
                },
                configs: &plan.precompute_plan.materializations,
            };
            let mut selected_outputs = Vec::new();
            let mut horizons = Vec::new();
            for sink_node in &installed.binding.precompute_sinks {
                // Derived summaries consume complete immutable windows at the
                // completion barrier, never additive worker fragments.
                if matches!(installed.binding.node(*sink_node),
                    Some(BackendNodeBinding::Materialization { stored_output })
                        if plan.precompute_plan.materializations.iter().any(|config|
                            config.policy_fingerprint() == stored_output.fingerprint()
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
                            Some(BackendNodeBinding::Materialization { stored_output })
                                if *stored_output != source_definition
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
                    Some(BackendNodeBinding::Materialization { stored_output }) => *stored_output,
                    _ => return Err("precompute sink lacks materialization binding".into()),
                };
                let key = MaterializationCommitKey {
                    plan_id: plan.plan_id(),
                    plan_version: plan.plan_version(),
                    stored_output: target,
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
                selected_outputs.push((*sink_node, key));
                horizons.push(horizon_ms);
            }
            let values = execute_precompute_sinks(
                &dag,
                &installed.binding,
                &selected_outputs,
                &adapter,
                &self.commits,
            )
            .map_err(schedule_error)?;
            for (((_, key), horizon_ms), value) in
                selected_outputs.into_iter().zip(horizons).zip(values)
            {
                let target = key.stored_output;
                let mut target_output = output.clone();
                target_output.policy_fp = target.into();
                target_output.storage_handle = None;
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

/// Materializations affected by an admitted source update, following installed
/// semantic dependencies rather than assuming source and output identities match.
pub(crate) fn affected_materializations(
    plan: &asap_types::precompute_plan::PrecomputePlan,
    source: asap_types::sds::StoredOutputId,
) -> BTreeSet<asap_types::sds::StoredOutputId> {
    use asap_types::executable_plan::BackendNodeBinding;
    let mut affected = BTreeSet::from([source]);
    for installed in plan.executable_dags.values() {
        let mut reachable = installed.binding.nodes.iter().filter_map(|(node, binding)| {
            matches!(binding, BackendNodeBinding::Materialization { stored_output } if *stored_output == source).then_some(*node)
        }).collect::<BTreeSet<_>>();
        let mut frontier = reachable.iter().copied().collect::<Vec<_>>();
        while let Some(producer) = frontier.pop() {
            for edge in &installed.document.edges {
                let immutable = matches!(installed.binding.node(edge.consumer),
                    Some(BackendNodeBinding::Materialization { stored_output })
                        if plan.materializations.iter().any(|config|
                            config.policy_fingerprint() == stored_output.fingerprint()
                                && config.derived_input.is_some()));
                if edge.producer == producer && !immutable && reachable.insert(edge.consumer) {
                    frontier.push(edge.consumer);
                }
            }
        }
        for sink in &installed.binding.precompute_sinks {
            if reachable.contains(sink) {
                if let Some(BackendNodeBinding::Materialization { stored_output }) =
                    installed.binding.nodes.get(sink)
                {
                    affected.insert(*stored_output);
                }
            }
        }
    }
    affected
}

#[cfg(test)]
mod tests {
    fn test_context() -> asap_physical_operators::dag::RunContext {
        use asap_physical_operators::dag::{Limits, RunContext, Scope};
        RunContext::new(
            Scope::Ingestion {
                window_start_ms: 0,
                window_end_ms: 10_000,
                revision: 1,
            },
            Limits::default(),
        )
        .unwrap()
    }

    use super::*;
    use asap_physical_operators::summary_kernels::SumAccumulator;
    use planner_types::post_asap::{
        EdgeRole, ExecutableDag, ExecutableDagEdge, GroupingEdgeCompatibility, SummarySchema,
        WindowEdgeCompatibility,
    };

    fn definition(value: u64) -> asap_types::sds::StoredOutputId {
        asap_types::PolicyFingerprint(value).into()
    }

    fn maintenance_only(
        mut dag: ExecutableDag,
        mut binding: BackendExecutableBinding,
    ) -> (ExecutableDag, BackendExecutableBinding) {
        let retained = dag
            .nodes
            .iter()
            .filter(|node| {
                node.output_state.timing == planner_types::post_asap::ExecutionTiming::IngestionTime
            })
            .map(|node| node.id)
            .collect::<BTreeSet<_>>();
        dag.nodes.retain(|node| retained.contains(&node.id));
        dag.edges
            .retain(|edge| retained.contains(&edge.producer) && retained.contains(&edge.consumer));
        binding.nodes.retain(|id, _| retained.contains(id));
        dag.root = binding.precompute_sinks[0];
        (dag, binding)
    }

    #[test]
    fn cohort_lineage_is_order_independent_and_binds_every_input() {
        use crate::storage_engines::sketch_db::index::FrozenExactWindows;
        let make = |sid, id, value| {
            let mut state = asap_physical_operators::summary_kernels::SumAccumulator::new();
            state.update(value);
            FrozenExactWindows {
                stored_output_reference: asap_types::sds::StoredOutputReference::for_output(
                    definition(id),
                ),
                storage_handle: sid,
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
        let mut relocated = make(20, 2, 5.0);
        relocated.storage_handle = 999;
        assert_eq!(
            baseline,
            frozen_cohort_lineage(&[make(10, 1, 3.0), relocated], &expected).unwrap(),
            "local row relocation must not change stored-output lineage"
        );
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
        let mut changed_output = make(20, 2, 5.0);
        changed_output.stored_output_reference.stored_output_id.0 += 100;
        assert!(frozen_cohort_lineage(&[make(10, 1, 3.0), changed_output], &expected).is_err());
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

    // Ingestion adapters must execute native operators in the parent's scope.
    #[test]
    fn native_merge_uses_the_parent_budget_and_cancellation() {
        let input = Arc::new(MaintenanceValue::summary(Arc::new(
            SumAccumulator::with_sum(3.),
        )));
        let context = test_context();
        let output = merge_inputs(&[Arc::clone(&input)], &context).unwrap();
        assert_eq!(
            output
                .state()
                .unwrap()
                .query_statistic(asap_types::Statistic::Sum, &None, &Default::default())
                .unwrap(),
            3.
        );
        assert!(context.peak_bytes() > 0);
        context.cancel();
        assert!(merge_inputs(&[input], &context)
            .err()
            .unwrap()
            .contains("cancelled"));
    }

    fn node(id: u32) -> ExecutableDagNode {
        ExecutableDagNode {
            id: PostAsapNodeId(id),
            payload: ExecutableOperatorPayload::SummaryMerge,
            output_state: planner_types::post_asap::ExecutionDataState::INGESTION_SUMMARY,
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
            data_state: planner_types::post_asap::ExecutionDataState::INGESTION_SUMMARY,
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
    fn frozen_adapter_resolves_each_materialized_frontier_without_aliasing() {
        use planner_types::post_asap::{ExactKind, ExactParams, SummaryFamilyType, SummaryField};
        let make = |id| crate::storage_engines::sketch_db::index::FrozenExactWindows {
            stored_output_reference: asap_types::sds::StoredOutputReference::for_output(
                definition(id),
            ),
            storage_handle: id,
            definition: definition(id),
            generation: Arc::new(asap_types::sds::CatalogGeneration {
                schema_version: 2,
                plan_id: 1,
                plan_version: 1,
                snapshot_sha256: "0".repeat(64),
            }),
            group: BTreeMap::new(),
            windows: BTreeMap::from([((0, 1000), sum(id as f64))]),
            singleton_population_complete: false,
        };
        let inputs = [make(1), make(2)];
        let binding = BackendExecutableBinding {
            nodes: [(1, definition(1)), (2, definition(2)), (3, definition(3))]
                .into_iter()
                .map(|(id, stored_output)| {
                    (
                        PostAsapNodeId(id),
                        BackendNodeBinding::Materialization { stored_output },
                    )
                })
                .collect(),
            query_sink: PostAsapNodeId(3),
            query_plan_sink: asap_types::query_plan::QueryNodeId(3),
            precompute_sinks: vec![PostAsapNodeId(3)],
        };
        let adapter = OperatorAdapter {
            binding: &binding,
            inputs: MaintenanceInputs::Frozen(&inputs),
            configs: &[],
        };
        let source_node = |id| {
            let mut source = node(id);
            source.output_schema.fields = vec![SummaryField {
                name: "state".into(),
                dtype: SummaryFamilyType::ExactAggregate(ExactKind::Sum, ExactParams::Sum),
                nullable: false,
            }];
            source
        };
        let first = adapter
            .materialized_input(&source_node(1))
            .unwrap()
            .unwrap();
        let second = adapter
            .materialized_input(&source_node(2))
            .unwrap()
            .unwrap();
        assert!(adapter
            .materialized_input(&source_node(3))
            .unwrap()
            .is_none());
        let merged = adapter
            .execute(
                &node(3),
                &[Arc::new(first), Arc::new(second)],
                test_context(),
            )
            .unwrap();
        assert_eq!(
            merged
                .state()
                .unwrap()
                .query_statistic(
                    asap_types::Statistic::Sum,
                    &None,
                    &std::collections::HashMap::new()
                )
                .unwrap(),
            3.0
        );
        let ambiguous = [make(1), make(1)];
        let adapter = OperatorAdapter {
            binding: &binding,
            inputs: MaintenanceInputs::Frozen(&ambiguous),
            configs: &[],
        };
        assert!(adapter.materialized_input(&source_node(1)).is_err());
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
        let snapshot: control_plane::physical::compiler::BackendLocalPlanningInput =
            serde_json::from_value(snapshot).unwrap();
        let bundle = crate::tests::test_utilities::planning::quoted_snapshot(snapshot, false)
            .compile_promql()
            .unwrap();
        let mut source_config = bundle.precompute_plan.materializations[0].clone();
        // This operator fixture supplies global raw populations; its config
        // must agree with the explicit Reduce([]) below.
        source_config.partitioning = Some(asap_types::sds::PopulationPartitioning::Grouped);
        source_config.grouping_labels = std::iter::empty::<String>().collect();
        source_config.window_size = 2;
        source_config.slide_interval = 2;
        source_config.window_type = asap_types::WindowKind::Tumbling;
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
                        stored_output: source_definition,
                    },
                ),
                (PostAsapNodeId(2), BackendNodeBinding::MaintenanceInput),
                (
                    PostAsapNodeId(3),
                    BackendNodeBinding::Materialization {
                        stored_output: target,
                    },
                ),
            ]),
            query_sink: PostAsapNodeId(3),
            query_plan_sink: control_plane::query_plan::QueryNodeId(3),
            precompute_sinks: vec![PostAsapNodeId(3)],
        };
        let frozen_inputs = [
            crate::storage_engines::sketch_db::index::FrozenExactWindows {
                stored_output_reference: asap_types::sds::StoredOutputReference::for_output(
                    source_definition,
                ),
                storage_handle: 1,
                definition: source_definition,
                generation: Arc::new(asap_types::sds::CatalogGeneration {
                    schema_version: 2,
                    plan_id: 1,
                    plan_version: 1,
                    snapshot_sha256: "0".repeat(64),
                }),
                group: BTreeMap::new(),
                windows: BTreeMap::from([((0, 1000), sum(7.0))]),
                singleton_population_complete: false,
            },
        ];
        let adapter = OperatorAdapter {
            binding: &binding,
            inputs: MaintenanceInputs::Frozen(&frozen_inputs),
            configs: &configs,
        };
        let mut read = node(2);
        read.payload = ExecutableOperatorPayload::Value {
            operation: planner_types::post_asap::ValueOperation::FinalizeExactAccumulator,
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
        let row = adapter.execute(&read, &[source], test_context()).unwrap();
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
        let result = adapter
            .execute(&aggregate, &[Arc::new(row)], test_context())
            .unwrap();
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
        read.output_state = planner_types::post_asap::ExecutionDataState::INGESTION_ROWS;
        aggregate.output_schema.fields = vec![SummaryField {
            name: "state".into(),
            dtype: configs[1].accumulator_spec().unwrap().family,
            nullable: false,
        }];
        let mut query = node(4);
        query.output_state = planner_types::post_asap::ExecutionDataState::QUERY_ROWS;
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
                stored_output: source_definition,
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
        let (dag, scheduled_binding) = maintenance_only(dag, scheduled_binding);
        let scheduled_adapter = OperatorAdapter {
            binding: &scheduled_binding,
            ..adapter
        };
        let key = MaterializationCommitKey {
            plan_id: 1,
            plan_version: 1,
            stored_output: target,
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
        let mut document =
            OwnedPostAsapDag::from_executable("immutable-chain".into(), &dag).unwrap();
        let mut durable_configs = configs.to_vec();
        durable_configs[1].derived_input = Some(
            asap_types::derived_input::DerivedInputIdentity::from_dag(
                &document,
                PostAsapNodeId(2),
                &BTreeMap::from([(PostAsapNodeId(1), source_definition)]),
            )
            .unwrap(),
        );
        document.schema_version = asap_types::executable_plan::MAINTENANCE_DAG_SCHEMA_VERSION;
        let mut durable_binding = scheduled_binding.clone();
        durable_binding.nodes.insert(
            PostAsapNodeId(3),
            BackendNodeBinding::Materialization {
                stored_output: durable_configs[1].policy_fingerprint().into(),
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
                stored_output_id: source_definition,
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
        let deadline =
            crate::tests::test_utilities::timing::deadline(std::time::Duration::from_secs(5));
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
        exercise_two_source_completed_sink(&dag, &configs, &scheduled_binding, true, false);
        exercise_two_source_completed_sink(&dag, &configs, &scheduled_binding, false, false);
        exercise_two_source_completed_sink(&dag, &configs, &scheduled_binding, true, true);
        exercise_two_source_completed_sink(&dag, &configs, &scheduled_binding, false, true);
        assert!(evaluate_weight(
            &SummaryInputExpr::Column(planner_types::pre_asap::ColumnRef::Named("missing".into())),
            7.0,
            "value"
        )
        .is_err());
    }

    fn exercise_two_source_completed_sink(
        template: &ExecutableDag,
        configs: &[asap_types::PrecomputeMaterialization],
        binding: &BackendExecutableBinding,
        matching_windows: bool,
        complete_groups: bool,
    ) {
        // A bound operator fixture, not a claim that a frontend selected this
        // composition. Both actual durable sources are required before output.
        use crate::storage_engines::sketch_db::index::{
            persistence::config::SketchStorePersistenceConfig, SketchStore,
        };
        use asap_types::executable_plan::{InstalledPostAsapDag, OwnedPostAsapDag};
        let mut first = configs[0].clone();
        first.window_layout = asap_types::WindowMaterializationLayout::FullWindow;
        if complete_groups {
            first.population_key_encoding = asap_types::PopulationKeyEncoding::CanonicalLabelsV1;
            first.partitioning = Some(asap_types::sds::PopulationPartitioning::PerEntity);
        }
        let mut second = first.clone();
        second.metric = "second_maintenance_source".into();
        let first_id = first.policy_fingerprint().into();
        let second_id = second.policy_fingerprint().into();
        let mut dag = template.clone();
        let mut second_node = dag
            .nodes
            .iter()
            .find(|node| node.id == PostAsapNodeId(1))
            .unwrap()
            .clone();
        second_node.id = PostAsapNodeId(5);
        let mut merge = second_node.clone();
        merge.id = PostAsapNodeId(6);
        merge.payload = ExecutableOperatorPayload::SummaryMerge;
        dag.nodes.extend([second_node, merge]);
        let original = dag
            .edges
            .iter()
            .find(|edge| edge.producer == PostAsapNodeId(1) && edge.consumer == PostAsapNodeId(2))
            .unwrap()
            .clone();
        dag.edges.retain(|edge| {
            !(edge.producer == PostAsapNodeId(1) && edge.consumer == PostAsapNodeId(2))
        });
        for (producer, consumer) in [(1, 6), (5, 6), (6, 2)] {
            let mut edge = original.clone();
            edge.producer = PostAsapNodeId(producer);
            edge.consumer = PostAsapNodeId(consumer);
            dag.edges.push(edge);
        }
        if let ExecutableOperatorPayload::SummaryAgg { input, .. } = &mut dag
            .nodes
            .iter_mut()
            .find(|node| node.id == PostAsapNodeId(3))
            .unwrap()
            .payload
        {
            input.weight = planner_types::post_asap::SummaryInputExpr::Column(
                planner_types::pre_asap::ColumnRef::SampleValue,
            );
        }
        let mut document =
            OwnedPostAsapDag::from_executable("two-source-fixture".into(), &dag).unwrap();
        let mut target = configs[1].clone();
        if complete_groups {
            target.population_key_encoding = asap_types::PopulationKeyEncoding::CanonicalLabelsV1;
        }
        target.derived_input = Some(
            asap_types::derived_input::DerivedInputIdentity::from_dag(
                &document,
                PostAsapNodeId(2),
                &BTreeMap::from([
                    (PostAsapNodeId(1), first_id),
                    (PostAsapNodeId(5), second_id),
                ]),
            )
            .unwrap(),
        );
        document.schema_version = asap_types::executable_plan::MAINTENANCE_DAG_SCHEMA_VERSION;
        let mut binding = binding.clone();
        for (node, stored_output) in [
            (1, first_id),
            (5, second_id),
            (3, target.policy_fingerprint().into()),
        ] {
            binding.nodes.insert(
                PostAsapNodeId(node),
                BackendNodeBinding::Materialization { stored_output },
            );
        }
        binding
            .nodes
            .insert(PostAsapNodeId(6), BackendNodeBinding::MaintenanceInput);
        let installed = InstalledPostAsapDag { document, binding };
        let configs = [first, second, target];
        let catalog = Arc::new(
            asap_types::summary_catalog::SummaryCatalog::from_materializations(2, 1, &configs)
                .unwrap(),
        );
        let directory = tempfile::tempdir().unwrap();
        let persistence_config = || {
            let mut config =
                SketchStorePersistenceConfig::with_memory_limit(1 << 24, directory.path().into());
            config.delete_older_than_ms = None;
            config.hot_window_ms = None;
            // Keep the background flush cadence well inside the seal deadline.
            config.flush_interval = std::time::Duration::from_millis(5);
            config
        };
        let store = Arc::new(SketchStore::new());
        store.install_summary_catalog(Arc::clone(&catalog)).unwrap();
        let mut persistence = store.start_persistence(persistence_config()).unwrap();
        let generation = store.active_catalog_generation().unwrap();
        let sources = BTreeMap::from([(first_id, 800), (second_id, 801)]);
        // A bound scheduler fixture; actual frontend selection is tested separately.
        let mut plan = asap_types::precompute_plan::PrecomputePlan::build_backend_local(
            asap_types::precompute_plan::PlanEnvelope {
                plan_id: 2,
                plan_version: 1,
                generated_at_unix_ms: 0,
                activation_unix_ms: 0,
                expiry_unix_ms: None,
                backend_compat: asap_types::precompute_plan::BACKEND_COMPAT.into(),
                planner_revision: "scheduler-fixture".into(),
                capability_snapshot_id: "scheduler-fixture".into(),
            },
            configs[..2]
                .iter()
                .cloned()
                .map(|mut config| {
                    // This is a bound runtime fixture; actual canonical installation
                    // is tested with the real compiler/process integration separately.
                    config.population_key_encoding =
                        asap_types::PopulationKeyEncoding::LegacyDelimited;
                    config
                })
                .collect(),
        )
        .unwrap();
        plan.materializations = configs.to_vec();
        plan.summary_catalog = Some(generation.as_ref().clone());
        plan.executable_dags = BTreeMap::from([("cohort-fixture".into(), installed.clone())]);
        let resolver_path = directory.path().join("resolver.wal");
        let resolver =
            crate::drivers::ingest::series_resolver::SeriesIdResolver::open(resolver_path.clone())
                .unwrap();

        for (index, value) in [2.0, 7.0].into_iter().enumerate() {
            let config = &configs[index];
            let start = if !matching_windows && index == 1 {
                2000
            } else {
                0
            };
            for population_index in 0..if complete_groups { 2 } else { 1 } {
                let population = if complete_groups {
                    BTreeMap::from([("instance".to_string(), population_index.to_string())])
                } else {
                    BTreeMap::new()
                };
                let coordinate = asap_types::sds::SummaryInstanceCoordinates {
                    stored_output_id: config.policy_fingerprint().into(),
                    time_range: asap_types::sds::HalfOpenTimeRange {
                        start_ms: start,
                        end_ms: start + 2000,
                    },
                    group_values: population.clone(),
                };
                let revision = store
                    .admit_summary_updates(&generation, BTreeSet::from([coordinate.clone()]))
                    .unwrap();
                let mut output = PrecomputedOutput::new(
                    start as u64,
                    (start + 2000) as u64,
                    None,
                    config.policy_fingerprint(),
                );
                output.catalog_generation = Some(Arc::clone(&generation));
                if complete_groups {
                    output.population_labels = Some(population);
                }
                let sid = 800
                    + if complete_groups {
                        index * 2 + population_index
                    } else {
                        index
                    } as u64;
                store
                    .publish_admitted_summary_update(
                        &generation,
                        &coordinate,
                        revision,
                        revision,
                        4000,
                        |writer| {
                            writer.ingest_precompute_with_series_id(
                                sid,
                                config,
                                &output,
                                sum(value + population_index as f64 * 10.0).as_ref(),
                            )
                        },
                    )
                    .unwrap();
            }
        }
        assert!(execute_completed_maintenance_cohort(
            &store,
            &generation,
            &installed,
            &configs,
            PostAsapNodeId(3),
            &sources,
            1,
            (0, 2000),
            &BTreeMap::new()
        )
        .is_err());
        assert!(store
            .series_ids_for_policy(configs[2].policy_fingerprint())
            .is_empty());
        let deadline =
            crate::tests::test_utilities::timing::deadline(std::time::Duration::from_secs(5));
        while !store.seal_finite_summary_input(&generation).unwrap() {
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let source_parts = persistence.manifest.live_parts().len();
        execute_finite_maintenance(&store, &resolver, &plan).unwrap();
        if !matching_windows {
            // Complete inputs at different windows cannot create any target.
            assert!(store
                .series_ids_for_policy(configs[2].policy_fingerprint())
                .is_empty());
            assert_eq!(persistence.manifest.live_parts().len(), source_parts);
            assert!(persistence
                .flusher
                .metadata_store()
                .load_strict()
                .unwrap()
                .iter()
                .all(|record| record.storage_handle != 1));
            persistence.shutdown();
            return;
        }
        assert_eq!(
            store.series_ids_for_policy(configs[2].policy_fingerprint()),
            vec![1]
        );
        if complete_groups {
            let cohort = store
                .read_complete_raw_maintenance_cohort(
                    &generation,
                    &BTreeSet::from([first_id, second_id]),
                    (0, 2000),
                )
                .unwrap();
            assert_eq!(cohort.inputs().len(), 4);
            let (dag, key) = prepare_frozen_maintenance_sink(
                &installed,
                &configs,
                PostAsapNodeId(3),
                cohort.inputs(),
                (0, 2000),
            )
            .unwrap();
            let (state, group) = execute_prepared_frozen_sink(
                &installed,
                &configs,
                PostAsapNodeId(3),
                MaintenanceInputs::Complete(&cohort),
                &dag,
                key,
            )
            .unwrap();
            assert!(group.is_empty());
            assert_eq!(
                state
                    .query_statistic(
                        asap_types::Statistic::Quantile,
                        &None,
                        &std::collections::HashMap::from([("quantile".into(), "1.0".into())])
                    )
                    .unwrap(),
                29.0
            );
            let committed = persistence.manifest.live_parts().len();
            execute_finite_maintenance(&store, &resolver, &plan).unwrap();
            assert_eq!(persistence.manifest.live_parts().len(), committed);
            persistence.shutdown();
            let restored = Arc::new(SketchStore::new());
            restored.install_summary_catalog(catalog).unwrap();
            let mut persistence = restored.start_persistence(persistence_config()).unwrap();
            let resolver =
                crate::drivers::ingest::series_resolver::SeriesIdResolver::open(resolver_path)
                    .unwrap();
            execute_finite_maintenance(&restored, &resolver, &plan).unwrap();
            assert_eq!(persistence.manifest.live_parts().len(), committed);
            persistence.shutdown();
            return;
        }
        let requests = sources
            .iter()
            .map(|(definition, sid)| {
                (
                    *sid,
                    *definition,
                    BTreeSet::from([(0, 2000)]),
                    BTreeMap::new(),
                )
            })
            .collect::<Vec<_>>();
        let cohort = store
            .read_frozen_exact_cohort(
                &generation,
                &configs[2].derived_input.as_ref().unwrap().inputs,
                &requests,
            )
            .unwrap();
        let (dag, key) = prepare_frozen_maintenance_sink(
            &installed,
            &configs,
            PostAsapNodeId(3),
            &cohort,
            (0, 2000),
        )
        .unwrap();
        let (result, _) = execute_prepared_frozen_sink(
            &installed,
            &configs,
            PostAsapNodeId(3),
            MaintenanceInputs::Frozen(&cohort),
            &dag,
            key,
        )
        .unwrap();
        assert_eq!(
            result
                .query_statistic(
                    asap_types::Statistic::Quantile,
                    &None,
                    &std::collections::HashMap::from([("quantile".into(), "0.5".into())])
                )
                .unwrap(),
            9.0
        );
        assert!(!execute_completed_maintenance_cohort(
            &store,
            &generation,
            &installed,
            &configs,
            PostAsapNodeId(3),
            &sources,
            1,
            (0, 2000),
            &BTreeMap::new()
        )
        .unwrap());
        let parts = persistence.manifest.live_parts().len();
        assert!(!execute_completed_maintenance_cohort(
            &store,
            &generation,
            &installed,
            &configs,
            PostAsapNodeId(3),
            &sources,
            1,
            (0, 2000),
            &BTreeMap::new()
        )
        .unwrap());
        assert_eq!(persistence.manifest.live_parts().len(), parts);
        persistence.shutdown();
        drop(store);
        let restored = Arc::new(SketchStore::new());
        restored.install_summary_catalog(catalog).unwrap();
        let mut persistence = restored.start_persistence(persistence_config()).unwrap();
        drop(resolver);
        let resolver =
            crate::drivers::ingest::series_resolver::SeriesIdResolver::open(resolver_path).unwrap();
        execute_finite_maintenance(&restored, &resolver, &plan).unwrap();

        assert!(!execute_completed_maintenance_cohort(
            &restored,
            &generation,
            &installed,
            &configs,
            PostAsapNodeId(3),
            &sources,
            1,
            (0, 2000),
            &BTreeMap::new()
        )
        .unwrap());
        assert_eq!(persistence.manifest.live_parts().len(), parts);
        let next_catalog = Arc::new(
            asap_types::summary_catalog::SummaryCatalog::from_materializations(2, 2, &configs)
                .unwrap(),
        );
        restored.install_summary_catalog(next_catalog).unwrap();
        assert_ne!(generation, restored.active_catalog_generation().unwrap());
        // Stale scheduling fails at the captured catalog boundary, before any
        // new generation's completion state or payload can be substituted.
        let stale = execute_completed_maintenance_cohort(
            &restored,
            &generation,
            &installed,
            &configs,
            PostAsapNodeId(3),
            &sources,
            2,
            (0, 2000),
            &BTreeMap::new(),
        )
        .unwrap_err();
        assert!(stale.contains("stale producer"), "{stale}");
        assert!(execute_finite_maintenance(&restored, &resolver, &plan).is_err());
        assert_eq!(persistence.manifest.live_parts().len(), parts);
        assert!(persistence
            .flusher
            .metadata_store()
            .load_strict()
            .unwrap()
            .iter()
            .all(|record| record.storage_handle != 2));
        persistence.shutdown();
    }

    #[test]
    fn maintenance_binary_aligns_windows_and_rejects_incomplete_or_ambiguous_rows() {
        use planner_types::post_asap::{BinaryOperator, SummaryFamilyType, SummaryField};
        use planner_types::pre_asap::{ArithmeticOpKind, BinaryOpKind, DataType};
        let mut operation = node(10);
        operation.output_schema.fields = vec![
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
        operation.output_schema.time_index = Some(0);
        let mut operator = BinaryOperator {
            checked_relative_division: false,
            checked_finite_division: false,
            kind: BinaryOpKind::Arithmetic(ArithmeticOpKind::Sub),
            vector_match: None,
        };
        let rows = |values: Vec<(i64, f64)>| {
            Arc::new(MaintenanceValue::Rows {
                values: BTreeMap::from([(BTreeMap::new(), values)]),
                name: "value".into(),
                timestamped: true,
            })
        };
        let left = rows(vec![(2_000, 7.0), (1_000, 5.0)]);
        let right = rows(vec![(1_000, 2.0), (2_000, 3.0)]);
        // Arrival order cannot exchange windows, and subtraction retains edge order.
        let MaintenanceValue::Rows { values, .. } = evaluate_aligned_binary(
            &operation,
            &operator,
            &[left.clone(), right.clone()],
            &test_context(),
        )
        .unwrap() else {
            panic!("expected rows")
        };
        assert_eq!(values[&BTreeMap::new()], vec![(1_000, 3.0), (2_000, 4.0)]);
        // Equal timestamps in different populations are separate rows, never
        // added together before the DAG explicitly reduces those populations.
        let a = BTreeMap::from([("instance".to_string(), "a".to_string())]);
        let b = BTreeMap::from([("instance".to_string(), "b".to_string())]);
        let grouped = |values| {
            Arc::new(MaintenanceValue::Rows {
                values,
                name: "value".into(),
                timestamped: true,
            })
        };
        let grouped_left = grouped(BTreeMap::from([
            (a.clone(), vec![(1_000, 5.0)]),
            (b.clone(), vec![(1_000, 9.0)]),
        ]));
        let grouped_right = grouped(BTreeMap::from([
            (a.clone(), vec![(1_000, 2.0)]),
            (b.clone(), vec![(1_000, 4.0)]),
        ]));
        let MaintenanceValue::Rows { values, .. } = evaluate_aligned_binary(
            &operation,
            &operator,
            &[grouped_left.clone(), grouped_right],
            &test_context(),
        )
        .unwrap() else {
            panic!("expected grouped rows")
        };
        assert_eq!(
            values,
            BTreeMap::from([(a.clone(), vec![(1_000, 3.0)]), (b, vec![(1_000, 5.0)]),])
        );
        assert!(evaluate_aligned_binary(
            &operation,
            &operator,
            &[
                grouped_left,
                grouped(BTreeMap::from([(a, vec![(1_000, 2.0)])]))
            ],
            &test_context()
        )
        .is_err());
        let binding = BackendExecutableBinding {
            nodes: BTreeMap::new(),
            query_sink: PostAsapNodeId(10),
            query_plan_sink: asap_types::query_plan::QueryNodeId(10),
            precompute_sinks: vec![],
        };
        let frozen = OperatorAdapter {
            binding: &binding,
            inputs: MaintenanceInputs::Frozen(&[]),
            configs: &[],
        };
        operation.payload = ExecutableOperatorPayload::Binary {
            operator: operator.clone(),
        };
        operation.output_state = planner_types::post_asap::ExecutionDataState::INGESTION_ROWS;
        assert!(frozen
            .execute(&operation, &[left.clone(), right.clone()], test_context())
            .is_ok());
        let live = OperatorAdapter {
            binding: &binding,
            inputs: MaintenanceInputs::Live {
                definition: definition(1),
                state: sum(1.0),
            },
            configs: &[],
        };
        assert!(live
            .execute(&operation, &[left.clone(), right.clone()], test_context())
            .is_err());
        operation.payload = ExecutableOperatorPayload::Binary {
            operator: operator.clone(),
        };
        operation.output_state = planner_types::post_asap::ExecutionDataState::QUERY_ROWS;
        assert!(frozen
            .execute(&operation, &[left.clone(), right.clone()], test_context())
            .is_err());
        operation.output_state = planner_types::post_asap::ExecutionDataState::INGESTION_ROWS;

        for invalid in [
            rows(vec![]),
            rows(vec![(1_000, 2.0)]),
            rows(vec![(1_000, 2.0), (3_000, 3.0)]),
            rows(vec![(1_000, 2.0), (1_000, 3.0)]),
            rows(vec![(1_000, f64::NAN), (2_000, 3.0)]),
            Arc::new(MaintenanceValue::Rows {
                values: BTreeMap::from([(BTreeMap::new(), vec![(1_000, 2.0), (2_000, 3.0)])]),
                name: "value".into(),
                timestamped: false,
            }),
            Arc::new(MaintenanceValue::summary(sum(2.0))),
        ] {
            assert!(evaluate_aligned_binary(
                &operation,
                &operator,
                &[left.clone(), invalid],
                &test_context()
            )
            .is_err());
        }
        operator.kind = BinaryOpKind::Arithmetic(ArithmeticOpKind::Div);
        assert!(evaluate_aligned_binary(
            &operation,
            &operator,
            &[left.clone(), rows(vec![(1_000, 0.0), (2_000, 3.0)])],
            &test_context()
        )
        .is_err());
        operation.output_schema.fields[1].dtype = SummaryFamilyType::Plain(DataType::Int64);
        assert!(
            evaluate_aligned_binary(&operation, &operator, &[left, right], &test_context())
                .is_err()
        );
    }

    #[test]
    fn finalization_preserves_windows_until_an_explicit_merge() {
        use planner_types::post_asap::{ExactKind, ExactParams, SummaryFamilyType, SummaryField};
        let family = SummaryFamilyType::ExactAggregate(ExactKind::Sum, ExactParams::Sum);
        let inputs = Arc::new(MaintenanceValue::SummaryWindows {
            states: BTreeMap::from([(
                BTreeMap::new(),
                vec![(1_000, sum(2.0)), (2_000, sum(7.0))].into(),
            )]),
            family: family.clone(),
        });
        let different_group = Arc::new(MaintenanceValue::SummaryWindows {
            states: BTreeMap::from([(
                BTreeMap::from([("instance".into(), "other".into())]),
                vec![(1_000, sum(3.0))].into(),
            )]),
            family,
        });
        assert!(merge_inputs(&[inputs.clone(), different_group], &test_context()).is_err());
        let mut read = node(2);
        read.output_schema.fields = vec![SummaryField {
            name: "value".into(),
            dtype: SummaryFamilyType::Plain(planner_types::pre_asap::DataType::Float64),
            nullable: false,
        }];
        let MaintenanceValue::Rows { values, .. } =
            finalize_exact(&read, &[inputs.clone()], &test_context()).unwrap()
        else {
            panic!("expected finalized rows")
        };
        assert_eq!(values[&BTreeMap::new()], vec![(1_000, 2.0), (2_000, 7.0)]);
        // Merge is a semantic DAG operation, not an implicit batch optimization.
        // Finalizing after it emits exactly one value instead of two updates.
        let merged = Arc::new(merge_inputs(&[inputs], &test_context()).unwrap());
        let MaintenanceValue::Rows { values, .. } =
            finalize_exact(&read, &[merged], &test_context()).unwrap()
        else {
            panic!("expected finalized row")
        };
        assert_eq!(values[&BTreeMap::new()], vec![(2_000, 9.0)]);
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
            matches!(finalize_exact(&read, &[integer_state], &test_context()), Err(error) if error.contains("Float64"))
        );
    }

    #[test]
    fn finalization_preserves_declared_timestamp_and_rejects_ambiguous_columns() {
        use planner_types::post_asap::{ExactKind, ExactParams, SummaryFamilyType, SummaryField};
        use planner_types::pre_asap::DataType;
        let input = Arc::new(MaintenanceValue::SummaryWindows {
            states: BTreeMap::from([(BTreeMap::new(), vec![(60_000, sum(10.0))].into())]),
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
        let MaintenanceValue::Rows { values, name, .. } =
            finalize_exact(&read, &[Arc::clone(&input)], &test_context()).unwrap()
        else {
            panic!("expected typed rows")
        };
        assert_eq!(values[&BTreeMap::new()], vec![(60_000, 10.0)]);
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
            assert!(finalize_exact(&malformed, &[Arc::clone(&input)], &test_context()).is_err());
        }
        let untimed = Arc::new(MaintenanceValue::Summary {
            state: sum(10.0),
            family: Some(SummaryFamilyType::ExactAggregate(
                ExactKind::Sum,
                ExactParams::Sum,
            )),
        });
        assert!(finalize_exact(&read, &[untimed], &test_context()).is_err());
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
            inputs: MaintenanceInputs::Live {
                definition: definition(1),
                state: sum(7.0),
            },
            configs: &[],
        };
        let mut aggregate = node(1);
        aggregate.payload = ExecutableOperatorPayload::SummaryAgg {
            family: SummaryFamilyType::ExactAggregate(ExactKind::Count, ExactParams::Count),
            input: SummaryUpdate::column(ColumnRef::SampleValue),
            reduction: Reduction::by(vec![]),
            grouping: GroupingStrategy::default(),
        };
        let error = adapter.execute(
            &aggregate,
            &[Arc::new(MaintenanceValue::summary(sum(7.0)))],
            test_context(),
        );
        assert!(matches!(error, Err(reason) if reason.contains("typed update evaluator")));
    }

    #[test]
    fn summary_update_rejects_multiple_output_populations_before_updating() {
        use planner_types::post_asap::{GroupingStrategy, SummaryUpdate};
        use planner_types::pre_asap::{ColumnRef, Reduction};
        let snapshot: control_plane::physical::compiler::BackendLocalPlanningInput =
            serde_json::from_str(include_str!(
                "../../../docs/examples/asapquery-planning-snapshot.json"
            ))
            .unwrap();
        let mut config = crate::tests::test_utilities::planning::quoted_snapshot(snapshot, false)
            .compile_promql()
            .unwrap()
            .precompute_plan
            .materializations[0]
            .clone();
        config.aggregation_type = asap_types::AggregationType::Sum;
        config.aggregation_sub_type = "sum".into();
        config.grouping_labels = ["instance".to_string()].into_iter().collect();
        config.partitioning = Some(asap_types::sds::PopulationPartitioning::PerEntity);
        let family = config.accumulator_spec().unwrap().family;
        let binding = BackendExecutableBinding {
            nodes: BTreeMap::from([(
                PostAsapNodeId(1),
                BackendNodeBinding::Materialization {
                    stored_output: config.policy_fingerprint().into(),
                },
            )]),
            query_sink: PostAsapNodeId(1),
            query_plan_sink: asap_types::query_plan::QueryNodeId(1),
            precompute_sinks: vec![PostAsapNodeId(1)],
        };
        let configs = [config];
        let adapter = OperatorAdapter {
            binding: &binding,
            inputs: MaintenanceInputs::Frozen(&[]),
            configs: &configs,
        };
        let mut aggregate = node(1);
        aggregate.payload = ExecutableOperatorPayload::SummaryAgg {
            family,
            input: SummaryUpdate::column(ColumnRef::SampleValue),
            reduction: Reduction::PerEntity,
            grouping: GroupingStrategy::default(),
        };
        let rows = MaintenanceValue::Rows {
            values: ["a", "b"]
                .into_iter()
                .map(|name| {
                    (
                        BTreeMap::from([("instance".into(), name.into())]),
                        vec![(1_000, 5.0)],
                    )
                })
                .collect(),
            name: "value".into(),
            timestamped: true,
        };
        assert!(
            matches!(adapter.execute(&aggregate, &[Arc::new(rows)], test_context()),
            Err(error) if error.contains("one explicitly reduced output population"))
        );
    }

    #[test]
    fn dds_maintenance_rejects_nonpositive_population_before_returning_summary() {
        use planner_types::post_asap::{GroupingStrategy, SummaryUpdate};
        use planner_types::pre_asap::{ColumnRef, Reduction};
        let snapshot: control_plane::physical::compiler::BackendLocalPlanningInput =
            serde_json::from_str(include_str!(
                "../../../docs/examples/asapquery-planning-snapshot.json"
            ))
            .unwrap();
        let mut config = crate::tests::test_utilities::planning::quoted_snapshot(snapshot, false)
            .compile_promql()
            .unwrap()
            .precompute_plan
            .materializations[0]
            .clone();
        config.aggregation_type = asap_types::AggregationType::DDSketch;
        config.parameters.clear();
        config
            .parameters
            .insert("relative_accuracy".into(), "0.01".into());
        config.aggregation_sub_type.clear();
        config.grouping_labels = std::iter::empty::<String>().collect();
        config.partitioning = Some(asap_types::sds::PopulationPartitioning::Grouped);
        let family = config.accumulator_spec().unwrap().family;
        let binding = BackendExecutableBinding {
            nodes: BTreeMap::from([(
                PostAsapNodeId(1),
                BackendNodeBinding::Materialization {
                    stored_output: config.policy_fingerprint().into(),
                },
            )]),
            query_sink: PostAsapNodeId(1),
            query_plan_sink: asap_types::query_plan::QueryNodeId(1),
            precompute_sinks: vec![PostAsapNodeId(1)],
        };
        let configs = [config];
        let adapter = OperatorAdapter {
            binding: &binding,
            inputs: MaintenanceInputs::Frozen(&[]),
            configs: &configs,
        };
        let mut aggregate = node(1);
        aggregate.payload = ExecutableOperatorPayload::SummaryAgg {
            family,
            input: SummaryUpdate::column(ColumnRef::SampleValue),
            reduction: Reduction::by(vec![]),
            grouping: GroupingStrategy::default(),
        };
        for rejected in [-20.0, 0.0, f64::MAX] {
            let rows = MaintenanceValue::Rows {
                values: [("a", 20.0), ("b", rejected)]
                    .into_iter()
                    .map(|(group, value)| {
                        (
                            BTreeMap::from([("instance".into(), group.into())]),
                            vec![(1000, value)],
                        )
                    })
                    .collect(),
                name: "value".into(),
                timestamped: true,
            };
            assert!(
                matches!(adapter.execute(&aggregate, &[Arc::new(rows)], test_context()),
                Err(error) if error.contains("positive representable domain"))
            );
        }
    }

    #[test]
    fn admitted_slow_worker_can_publish_behind_another_workers_replay_floor() {
        let commits = CommitRegistry::default();
        commits.0.lock().unwrap().generation = Some((7, 1));
        let key = |end| MaterializationCommitKey {
            plan_id: 7,
            plan_version: 1,
            stored_output: definition(2),
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
            stored_output: definition(2),
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
                stored_output: definition(target),
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
            ActivePhysicalPlanHandle, InstalledPrecomputePlan, RuntimePhysicalPlan,
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
        let snapshot: control_plane::physical::compiler::BackendLocalPlanningInput =
            serde_json::from_value(snapshot).unwrap();
        let mut bundle = crate::tests::test_utilities::planning::quoted_snapshot(snapshot, false)
            .compile_promql()
            .unwrap();
        let target_config = &bundle.precompute_plan.materializations[0];
        let long_step = target_config.window_size.max(
            target_config.slide_interval * target_config.num_aggregates_to_retain.unwrap_or(1),
        ) * 1_000;
        let target_definition = bundle.precompute_plan.materializations[0]
            .policy_fingerprint()
            .into();
        let mut query = node(2);
        query.output_state = planner_types::post_asap::ExecutionDataState::QUERY_ROWS;
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
                        stored_output: definition(1),
                    },
                ),
                (
                    PostAsapNodeId(1),
                    BackendNodeBinding::Materialization {
                        stored_output: target_definition,
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
        let (dag, binding) = maintenance_only(dag, binding);
        bundle.precompute_plan.executable_dags = BTreeMap::from([(
            "retry".into(),
            InstalledPostAsapDag {
                document: {
                    let mut document =
                        OwnedPostAsapDag::from_executable("retry".into(), &dag).unwrap();
                    document.schema_version =
                        asap_types::executable_plan::MAINTENANCE_DAG_SCHEMA_VERSION;
                    document
                },
                binding,
            },
        )]);
        let active = RuntimePhysicalPlan {
            readout_programs: Default::default(),
            envelope: bundle.precompute_plan.envelope.clone(),
            summary_catalog: Some(Arc::new(bundle.summary_catalog)),
            precompute_plan: bundle.precompute_plan,
            transmission_plan: bundle.transmission_plan,
            installed_precompute_plan: Arc::new(InstalledPrecomputePlan::new(Default::default())),
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
                InstalledPrecomputePlanHandle::from_active_physical_plan(
                    ActivePhysicalPlanHandle::new(active.clone()),
                ),
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
        query.output_state = planner_types::post_asap::ExecutionDataState::QUERY_ROWS;
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
                            stored_output: definition(if id == 0 { 1 } else { id as u64 + 1 }),
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
        let (dag, binding) = maintenance_only(dag, binding);
        let source = sum(2.0);
        let adapter = OperatorAdapter {
            binding: &binding,
            inputs: MaintenanceInputs::Live {
                definition: definition(1),
                state: source,
            },
            configs: &[],
        };
        let commits = CommitRegistry::default();
        let key = MaterializationCommitKey {
            plan_id: 7,
            plan_version: 2,
            stored_output: definition(4),
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
        unsupported.payload = ExecutableOperatorPayload::SummarySubtract;
        let mut query = node(2);
        query.output_state = planner_types::post_asap::ExecutionDataState::QUERY_ROWS;
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
                        stored_output: definition(1),
                    },
                ),
                (
                    PostAsapNodeId(1),
                    BackendNodeBinding::Materialization {
                        stored_output: definition(2),
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
        let (dag, binding) = maintenance_only(dag, binding);
        let adapter = OperatorAdapter {
            binding: &binding,
            inputs: MaintenanceInputs::Live {
                definition: definition(1),
                state: sum(2.0),
            },
            configs: &[],
        };
        let commits = CommitRegistry::default();
        let key = MaterializationCommitKey {
            plan_id: 7,
            plan_version: 2,
            stored_output: definition(2),
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
