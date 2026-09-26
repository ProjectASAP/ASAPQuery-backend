//! Executes the installed typed logical DAG. No serving-time PromQL parsing.
mod native_values;
use crate::query_engines::{
    query_result::{InstantVectorElement, QueryResult},
    EngineError,
};
use crate::storage_engines::types::KeyByLabelValues;
use asap_physical_operators::dag as physical;
use asap_types::query_plan::residual::{
    Aggregation, BinaryOperation, Grouping, ResidualQueryOperator, TemporalOperation,
};
use asap_types::query_plan::{CandidateCompleteness, QueryNodeId, QueryPlanEntry, QueryPlanNode};
use futures::{FutureExt, StreamExt};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

type Labels = BTreeMap<String, String>;
type Vector = Vec<(Labels, f64)>;
type Matrix = Vec<(Labels, Vec<(i64, f64)>)>;
#[derive(Clone)]
pub(crate) enum Value {
    Scalar(f64),
    Vector(Vector),
    Matrix(Matrix, i64, i64),
}
#[derive(Debug, Default, Clone)]
pub struct ExecutionStats {
    /// Kept in execution provenance for compatibility; deployed DAGs cannot
    /// execute local raw scans, so this remains zero.
    pub raw_scan_evaluations: usize,
    pub summary_readout_evaluations: usize,
    pub memo_hits: usize,
    pub remote_evaluations: usize,
    pub remote_rpcs: usize,
    pub remote_branch_evaluations: usize,
}
fn miss(detail: impl Into<String>) -> EngineError {
    EngineError::capability_miss("installed_logical_dag", detail)
}
fn no_name(mut labels: Labels) -> Labels {
    labels.remove("__name__");
    labels
}
fn vector(value: Value) -> Result<Vector, EngineError> {
    let Value::Vector(values) = value else {
        return Err(miss("instant vector required"));
    };
    let mut seen = BTreeSet::new();
    if values.iter().any(|(labels, _)| !seen.insert(labels)) {
        return Err(miss("duplicate vector label sets"));
    }
    Ok(values)
}
pub(crate) fn from_result(result: QueryResult) -> Result<Value, EngineError> {
    let QueryResult::Vector(value) = result else {
        return Err(miss("bound readout must return instant vector"));
    };
    if !value.warnings.is_empty() {
        return Err(miss("partial bound readout cannot feed logical operator"));
    }
    let values = value
        .values
        .into_iter()
        .map(|point| {
            let keys = point
                .label_keys_override
                .ok_or_else(|| miss("bound readout must carry explicit label keys"))?;
            if keys.len() != point.labels.labels.len()
                || keys.iter().collect::<BTreeSet<_>>().len() != keys.len()
            {
                return Err(miss("bound readout label arity mismatch"));
            }
            Ok((
                keys.into_iter().zip(point.labels.labels).collect(),
                point.value,
            ))
        })
        .collect::<Result<Vector, EngineError>>()?;
    Ok(Value::Vector(vector(Value::Vector(values))?))
}

pub(crate) struct PreparedLeaf {
    pub value: Value,
    pub remote: bool,
    pub remote_evaluations: usize,
    pub remote_rpcs: usize,
}
pub(crate) type PreparedLeaves = BTreeMap<(QueryNodeId, i64), PreparedLeaf>;

pub(crate) fn execute_installed<F>(
    entry: &QueryPlanEntry,
    leaves: &PreparedLeaves,
    at: u64,
    callback: F,
) -> Result<(QueryResult, ExecutionStats), EngineError>
where
    F: FnMut(QueryNodeId, u64) -> Result<QueryResult, EngineError>,
{
    execute_values(entry, leaves, at, callback)
}

fn execute_values<F>(
    entry: &QueryPlanEntry,
    leaves: &PreparedLeaves,
    at: u64,
    callback: F,
) -> Result<(QueryResult, ExecutionStats), EngineError>
where
    F: FnMut(QueryNodeId, u64) -> Result<QueryResult, EngineError>,
{
    let at_signed = i64::try_from(at).map_err(|_| miss("evaluation timestamp overflow"))?;
    let runtime = RefCell::new(ValueRuntime {
        entry,
        leaves,
        callback,
        stats: ExecutionStats::default(),
        warnings: vec![],
    });
    let error = RefCell::new(None);
    let mut graph = physical::PhysicalDag::default();
    let mut identities = BTreeMap::from([((entry.root, at_signed), 0u64)]);
    let mut pending = vec![(entry.root, at_signed)];
    let mut depths = BTreeMap::from([((entry.root, at_signed), 1usize)]);
    while let Some((id, time)) = pending.pop() {
        let node = entry
            .nodes
            .get(&id)
            .ok_or_else(|| miss("missing installed node"))?;
        let dependencies = if leaves.contains_key(&(id, time)) {
            vec![]
        } else {
            expanded_inputs(node, time)?
        };
        let mut input_ids = Vec::new();
        for dependency in &dependencies {
            if let Some(id) = identities.get(dependency) {
                runtime.borrow_mut().stats.memo_hits += 1;
                tracing::debug!(target: "asap_runtime_debug", query_id = %entry.query_id, node_id = ?dependency.0, evaluation_ms = dependency.1, "installed query node reused within request");
                input_ids.push(*id);
            } else {
                if identities.len() >= 200_000 {
                    return Err(miss("installed DAG evaluation budget exceeded"));
                }
                let depth = depths[&(id, time)] + 1;
                if depth > 128 {
                    return Err(miss("installed DAG exceeds execution depth of 128"));
                }
                depths.insert(*dependency, depth);
                let id = identities.len() as u64;
                identities.insert(*dependency, id);
                pending.push(*dependency);
                input_ids.push(id);
            }
        }
        graph
            .add(
                identities[&(id, time)],
                input_ids,
                BoundValueOperator {
                    id,
                    time,
                    node,
                    dependencies,
                    runtime: &runtime,
                    error: &error,
                },
            )
            .map_err(|error| miss(error.to_string()))?;
    }
    let context = physical::RunContext::new(
        physical::Scope::Query {
            evaluation_time_ms: at_signed,
            revision: 0,
        },
        physical::Limits::default(),
    )
    .map_err(|error| miss(error.to_string()))?;
    let mut output = graph
        .execute(&[0], context)
        .map_err(|error| miss(error.to_string()))?
        .remove(0);
    // These adapters have synchronous callbacks and prepared I/O leaves. A
    // single poll avoids nesting a blocking futures executor inside a readout.
    let evaluated = match output.next().now_or_never().flatten() {
        Some(Ok(value)) => value.value().clone(),
        Some(Err(failure)) => {
            return Err(error
                .borrow_mut()
                .take()
                .unwrap_or_else(|| miss(failure.to_string())))
        }
        None => return Err(miss("synchronous query adapter did not produce a result")),
    };
    drop(output);
    drop(graph);
    let mut evaluator = runtime.into_inner();
    if matches!(evaluated, Value::Scalar(_)) {
        // QueryResult currently models vectors/matrices only. Preserve a scalar
        // root's HTTP type by routing it to native, while scalar intermediates
        // remain typed inside vector composition.
        return Err(miss("scalar root requires native response adapter"));
    }
    let result = vector(evaluated)?;
    let mut output = QueryResult::vector(
        result
            .into_iter()
            .map(|(labels, value)| {
                InstantVectorElement::new(
                    KeyByLabelValues::new_with_labels(labels.values().cloned().collect()),
                    value,
                )
                .with_label_keys_override(labels.into_keys().collect())
            })
            .collect(),
        at,
    );
    if let QueryResult::Vector(vector) = &mut output {
        vector.warnings.append(&mut evaluator.warnings);
    }
    Ok((output, evaluator.stats))
}

struct ValueRuntime<'a, F> {
    entry: &'a QueryPlanEntry,
    leaves: &'a PreparedLeaves,
    stats: ExecutionStats,
    callback: F,
    warnings: Vec<String>,
}
impl<F: FnMut(QueryNodeId, u64) -> Result<QueryResult, EngineError>> ValueRuntime<'_, F> {
    fn execute_node(
        &mut self,
        id: QueryNodeId,
        at: i64,
        node: &QueryPlanNode,
        inputs: &[&Value],
        dependencies: &[(QueryNodeId, i64)],
        context: &physical::RunContext,
    ) -> Result<Value, EngineError> {
        if let Some(leaf) = self.leaves.get(&(id, at)) {
            if leaf.remote {
                self.stats.remote_branch_evaluations += 1;
                self.stats.remote_evaluations += leaf.remote_evaluations;
                self.stats.remote_rpcs += leaf.remote_rpcs;
            } else {
                self.stats.summary_readout_evaluations += 1;
            }
            let value = leaf.value.clone();
            return Ok(value);
        }
        let value = match node.clone() {
            QueryPlanNode::Scalar { value } => Value::Scalar(native_scalar(value, context)?),
            QueryPlanNode::Logical {
                operator: ResidualQueryOperator::CurrentSeries { .. },
                ..
            } => {
                self.stats.summary_readout_evaluations += 1;
                from_result((self.callback)(
                    id,
                    u64::try_from(at).map_err(|_| miss("negative current-series timestamp"))?,
                )?)?
            }
            QueryPlanNode::Logical { operator, .. } => {
                if matches!(
                    operator,
                    ResidualQueryOperator::Scan { .. }
                        | ResidualQueryOperator::ExactSubquery { .. }
                        | ResidualQueryOperator::CandidateExactSubquery { .. }
                ) {
                    return Err(miss(
                        "installed Prometheus leaf was not prepared; backend raw execution is forbidden",
                    ));
                }
                self.logical(operator, inputs, dependencies, at, context)?
            }
            QueryPlanNode::RelationalJoin {
                inputs: _,
                join_kind: planner_types::pre_asap::JoinKind::Semi,
                pred,
                pruning,
                left_schema,
                right_schema,
                ..
            } => {
                let [values, candidates] = inputs else {
                    return Err(miss("semi-join requires two inputs"));
                };
                let values = vector((**values).clone())?;
                let candidates = vector((**candidates).clone())?;
                let predicate = serde_json::from_value(pred)
                    .map_err(|_| miss("invalid semi-join predicate"))?;
                let keys = asap_physical_operators::dag::planner::equijoin_keys(
                    &predicate,
                    &left_schema,
                    &right_schema,
                )
                .map_err(|error| miss(error.to_string()))?
                .into_iter()
                .map(|(left, right)| {
                    (
                        left_schema.fields[left].name.clone(),
                        right_schema.fields[right].name.clone(),
                    )
                })
                .collect::<Vec<_>>();
                let (selected, warning) =
                    semi_join(candidates, values, &keys, pruning.as_ref(), context)?;
                if let Some(warning) = warning {
                    self.warnings.push(warning);
                }
                Value::Vector(selected)
            }
            _ => {
                self.stats.summary_readout_evaluations += 1;
                from_result((self.callback)(
                    id,
                    u64::try_from(at).map_err(|_| miss("summary timestamp predates epoch"))?,
                )?)?
            }
        };
        Ok(value)
    }
    fn logical(
        &mut self,
        operator: ResidualQueryOperator,
        inputs: &[&Value],
        dependencies: &[(QueryNodeId, i64)],
        at: i64,
        context: &physical::RunContext,
    ) -> Result<Value, EngineError> {
        let input = |index: usize| {
            inputs
                .get(index)
                .map(|value| (**value).clone())
                .ok_or_else(|| miss("missing logical input"))
        };
        match operator {
            ResidualQueryOperator::ExactSubquery { .. }
            | ResidualQueryOperator::CandidateExactSubquery { .. } => {
                Err(miss("Prometheus exact leaf was not prepared"))
            }
            ResidualQueryOperator::CurrentSeries { .. } => Err(miss(
                "current-series leaf must use its installed node identity",
            )),
            ResidualQueryOperator::Scan { .. } => {
                Err(miss("local raw Scan is forbidden in deployed plans"))
            }
            ResidualQueryOperator::UnaryNegate => negate(input(0)?, context),
            ResidualQueryOperator::VectorToScalar => vector_to_scalar(vector(input(0)?)?, context),
            ResidualQueryOperator::Aggregate {
                operation,
                grouping,
            } => {
                let values = vector(input(0)?)?;
                Ok(Value::Vector(aggregate(
                    operation, &grouping, values, context,
                )?))
            }
            ResidualQueryOperator::Limit {
                n,
                offset,
                grouping,
            } => {
                let values = vector(input(0)?)?;
                Ok(Value::Vector(native_values::limit(
                    values, &grouping, n, offset, context,
                )?))
            }
            ResidualQueryOperator::Binary {
                operation,
                return_bool,
            } => {
                let left = input(0)?;
                let right = input(1)?;
                binary_in_context(operation, return_bool, left, right, context)
            }
            ResidualQueryOperator::Temporal { operation } => {
                let Value::Matrix(values, start, end) = input(0)? else {
                    return Err(miss("temporal operator requires range vector"));
                };
                let preserve_name = self.entry.language
                    == control_plane::query_plan::QueryLanguage::MetricsQl
                    && matches!(
                        operation,
                        TemporalOperation::Min | TemporalOperation::Max | TemporalOperation::Avg
                    );
                let result = native_temporal(values, operation, start, end, context)?;
                Ok(Value::Vector(
                    result
                        .into_iter()
                        .map(|(labels, value)| {
                            (
                                if preserve_name {
                                    labels
                                } else {
                                    no_name(labels)
                                },
                                value,
                            )
                        })
                        .collect(),
                ))
            }

            ResidualQueryOperator::Sort {
                descending,
                grouping,
            } => {
                let values = vector(input(0)?)?;
                Ok(Value::Vector(native_values::sort(
                    values, &grouping, descending, context,
                )?))
            }
            ResidualQueryOperator::HistogramQuantile => {
                let Value::Scalar(quantile) = input(0)? else {
                    return Err(miss("quantile requires scalar"));
                };
                let mut groups: BTreeMap<Labels, Vec<(f64, f64)>> = BTreeMap::new();
                for (mut labels, value) in vector(input(1)?)? {
                    if let Some(le) = labels.remove("le").and_then(|s| s.parse::<f64>().ok()) {
                        groups.entry(no_name(labels)).or_default().push((le, value));
                    }
                }
                let rows = groups
                    .into_iter()
                    .flat_map(|(labels, buckets)| {
                        buckets.into_iter().map(move |(bound, count)| {
                            vec![
                                native_labels(&labels),
                                physical::values::Value::Float64(bound),
                                physical::values::Value::Float64(count),
                            ]
                        })
                    })
                    .collect();
                Ok(Value::Vector(native_window(
                    rows,
                    planner_types::pre_asap::AggIntent::HistogramQuantile { q: quantile },
                    None,
                    context,
                )?))
            }
            ResidualQueryOperator::Subquery {
                range_ms,
                step_ms,
                offset_ms,
            } => {
                let (start, end, _) = subquery_grid(at, range_ms, step_ms, offset_ms)?;
                let mut values: BTreeMap<Labels, Vec<(i64, f64)>> = BTreeMap::new();
                if inputs.len() != dependencies.len() {
                    return Err(miss("subquery grid input mismatch"));
                }
                for (value, (_, time)) in inputs.iter().zip(dependencies) {
                    for (labels, value) in vector((**value).clone())? {
                        values.entry(labels).or_default().push((*time, value));
                    }
                }
                Ok(Value::Matrix(values.into_iter().collect(), start, end))
            }
        }
    }
}

fn subquery_grid(
    at: i64,
    range_ms: u64,
    step_ms: u64,
    offset_ms: i64,
) -> Result<(i64, i64, Vec<i64>), EngineError> {
    let end = at
        .checked_sub(offset_ms)
        .ok_or_else(|| miss("offset overflow"))?;
    let range = i64::try_from(range_ms).map_err(|_| miss("range overflow"))?;
    let step = i64::try_from(step_ms).map_err(|_| miss("step overflow"))?;
    if step <= 0 || range / step > 100_000 {
        return Err(miss("invalid or excessive subquery steps"));
    }
    let start = end
        .checked_sub(range)
        .ok_or_else(|| miss("range overflow"))?;
    let mut time = start
        .div_euclid(step)
        .checked_add(1)
        .and_then(|n| n.checked_mul(step))
        .ok_or_else(|| miss("subquery grid overflow"))?;
    let mut times = Vec::new();
    while time <= end {
        times.push(time);
        time = time
            .checked_add(step)
            .ok_or_else(|| miss("subquery time overflow"))?;
    }
    Ok((start, end, times))
}
fn expanded_inputs(node: &QueryPlanNode, at: i64) -> Result<Vec<(QueryNodeId, i64)>, EngineError> {
    match node {
        QueryPlanNode::Logical {
            operator:
                ResidualQueryOperator::Scan { .. }
                | ResidualQueryOperator::ExactSubquery { .. }
                | ResidualQueryOperator::CandidateExactSubquery { .. },
            ..
        } => Err(miss(
            "installed leaf was not prepared; local raw execution is forbidden",
        )),
        QueryPlanNode::Logical {
            operator:
                ResidualQueryOperator::Subquery {
                    range_ms,
                    step_ms,
                    offset_ms,
                },
            inputs,
        } => {
            let [input] = inputs.as_slice() else {
                return Err(miss("subquery requires one input"));
            };
            let (_, _, times) = subquery_grid(at, *range_ms, *step_ms, *offset_ms)?;
            Ok(times.into_iter().map(|time| (*input, time)).collect())
        }
        QueryPlanNode::Logical {
            operator: ResidualQueryOperator::CurrentSeries { .. },
            ..
        } => Ok(vec![]),
        QueryPlanNode::Logical { inputs, .. } => Ok(inputs.iter().map(|&id| (id, at)).collect()),
        QueryPlanNode::RelationalJoin { inputs, .. } => {
            Ok(inputs.iter().map(|&id| (id, at)).collect())
        }
        _ => Ok(vec![]),
    }
}
struct BoundValueOperator<'a, 'entry, F> {
    id: QueryNodeId,
    time: i64,
    node: &'entry QueryPlanNode,
    dependencies: Vec<(QueryNodeId, i64)>,
    runtime: &'a RefCell<ValueRuntime<'entry, F>>,
    error: &'a RefCell<Option<EngineError>>,
}
impl<F: FnMut(QueryNodeId, u64) -> Result<QueryResult, EngineError>>
    physical::PhysicalOperator<Value, ()> for BoundValueOperator<'_, '_, F>
{
    fn name(&self) -> &str {
        "InstalledValueOperator"
    }
    fn input_schemas(&self) -> Vec<()> {
        vec![(); self.dependencies.len()]
    }
    fn output_schema(&self) {}
    fn output_bytes(&self, value: &Value) -> usize {
        fn labels(value: &Labels) -> usize {
            value.iter().map(|(k, v)| k.len() + v.len()).sum()
        }
        match value {
            Value::Scalar(_) => 8,
            Value::Vector(values) => values.iter().map(|(key, _)| labels(key) + 8).sum(),
            Value::Matrix(values, ..) => values
                .iter()
                .map(|(key, points)| labels(key) + points.len() * 16)
                .sum(),
        }
    }
    fn start<'a>(
        &'a self,
        inputs: Vec<physical::Input<'a, Value>>,
        context: physical::RunContext,
    ) -> Result<physical::OutputStream<'a, Value>, physical::Error> {
        Ok(futures::stream::once(async move {
            let values =
                futures::future::try_join_all(inputs.into_iter().map(|mut input| async move {
                    input.next().await.ok_or_else(|| {
                        physical::Error::Operator("query input produced no value".into())
                    })?
                }))
                .await?;
            let values = values.iter().map(|v| v.value()).collect::<Vec<_>>();
            let started = std::time::Instant::now();
            tracing::debug!(target: "asap_runtime_debug", node_id = ?self.id, evaluation_ms = self.time, op = self.node.op_label(), syntax = %self.node.log_syntax(), "installed query node started");
            self.runtime
                .borrow_mut()
                .execute_node(
                    self.id,
                    self.time,
                    self.node,
                    &values,
                    &self.dependencies,
                    &context,
                )
                .inspect(|_| {
                    tracing::debug!(target: "asap_runtime_debug", node_id = ?self.id, op = self.node.op_label(), elapsed_us = started.elapsed().as_micros() as u64, "installed query node completed");
                })
                .map_err(|error| {
                    tracing::warn!(node_id = ?self.id, op = self.node.op_label(), elapsed_us = started.elapsed().as_micros() as u64, %error, "installed query node failed");
                    *self.error.borrow_mut() = Some(error);
                    physical::Error::Operator(format!(
                        "query node {} at {} failed",
                        self.id.0, self.time
                    ))
                })
        })
        .boxed_local())
    }
}

fn semi_join(
    candidates: Vector,
    values: Vector,
    keys: &[(String, String)],
    completeness: Option<&CandidateCompleteness>,
    context: &physical::RunContext,
) -> Result<(Vector, Option<String>), EngineError> {
    let left_key = |labels: &Labels| {
        keys.iter()
            .map(|(left, _)| labels.get(left).cloned().unwrap_or_default())
            .collect::<Vec<_>>()
    };
    let right_key = |labels: &Labels| {
        keys.iter()
            .map(|(_, right)| labels.get(right).cloned().unwrap_or_default())
            .collect::<Vec<_>>()
    };
    let available = values
        .iter()
        .map(|(labels, _)| left_key(labels))
        .collect::<std::collections::BTreeSet<_>>();
    let missing = candidates
        .iter()
        .map(|(labels, _)| right_key(labels))
        .filter(|key| !available.contains(key))
        .collect::<Vec<_>>();
    let selected = native_values::semi_join(values, &candidates, &left_key, &right_key, context)?;
    if !missing.is_empty() && matches!(completeness, Some(CandidateCompleteness::Certified { .. }))
    {
        return Err(miss("certified pruning key has no authoritative value"));
    }
    let warning = match completeness {
        None | Some(CandidateCompleteness::Certified { .. }) => None,
        Some(CandidateCompleteness::BestEffort { guarantee }) => Some(match guarantee {
            Some(guarantee) => format!(
                "ASAP membership pruning is approximate: {:?}",
                guarantee.metric
            ),
            None => "ASAP membership pruning is approximate and uncertified".into(),
        }),
    };
    Ok((selected, warning))
}

pub(super) fn native_scalar(
    value: f64,
    context: &physical::RunContext,
) -> Result<f64, EngineError> {
    use physical::{batch_execution::evaluate_source, operators::Operator, values::Value as Cell};
    let source = Operator::scalar(
        Cell::Float64(value),
        planner_types::pre_asap::DataType::Float64,
    )
    .map_err(|e| miss(e.to_string()))?;
    let batches = evaluate_source(source, context.clone()).map_err(|e| miss(e.to_string()))?;
    match batches
        .first()
        .and_then(|b| b.rows().first())
        .and_then(|r| r.first())
    {
        Some(Cell::Float64(value)) => Ok(*value),
        _ => Err(miss("native scalar source returned invalid output")),
    }
}

fn native_labels(labels: &Labels) -> physical::values::Value {
    physical::values::Value::Map(
        labels
            .iter()
            .map(|(k, v)| {
                (
                    physical::values::Value::Utf8(k.as_str().into()),
                    physical::values::Value::Utf8(v.as_str().into()),
                )
            })
            .collect::<Vec<_>>()
            .into(),
    )
}
fn native_vector_batch(
    values: Vector,
    grouping: &Grouping,
) -> Result<physical::values::Batch, EngineError> {
    use physical::values::{Batch, Value as Cell};
    use planner_types::{
        post_asap::{SummaryFamilyType, SummaryField, SummarySchema},
        pre_asap::DataType,
    };
    let label_type = DataType::Map {
        key: Box::new(DataType::Utf8),
        value: Box::new(DataType::Utf8),
        value_nullable: false,
    };
    let schema = std::sync::Arc::new(SummarySchema {
        fields: vec![
            ("labels", label_type.clone()),
            ("group", label_type),
            ("value", DataType::Float64),
        ]
        .into_iter()
        .map(|(name, dtype)| SummaryField {
            name: name.into(),
            dtype: SummaryFamilyType::Plain(dtype),
            nullable: false,
        })
        .collect(),
        time_index: None,
    });
    let rows = values
        .into_iter()
        .map(|(labels, value)| {
            vec![
                native_labels(&labels),
                native_labels(&grouping_key(&labels, grouping)),
                Cell::Float64(value),
            ]
        })
        .collect();
    Batch::try_new(schema, rows).map_err(|e| miss(e.to_string()))
}
fn native_batch_rows(
    batch: physical::values::Batch,
    ops: Vec<physical::operators::Operator>,
    context: &physical::RunContext,
) -> Result<Vec<Vec<physical::values::Value>>, EngineError> {
    physical::batch_execution::evaluate_batch(batch, ops, context.clone())
        .map(|batches| {
            batches
                .into_iter()
                .flat_map(|batch| batch.rows().to_vec())
                .collect()
        })
        .map_err(|e| miss(e.to_string()))
}
fn native_vector_output(
    rows: Vec<Vec<physical::values::Value>>,
    label_column: usize,
    value_column: usize,
) -> Result<Vector, EngineError> {
    use physical::values::Value as Cell;
    rows.into_iter()
        .map(|row| {
            let Some(Cell::Map(entries)) = row.get(label_column) else {
                return Err(miss("native operator returned invalid labels"));
            };
            let labels = entries
                .iter()
                .map(|(k, v)| match (k, v) {
                    (Cell::Utf8(k), Cell::Utf8(v)) => Ok((k.to_string(), v.to_string())),
                    _ => Err(miss("native label map is not Utf8")),
                })
                .collect::<Result<Labels, _>>()?;
            let value = match row.get(value_column) {
                Some(Cell::Float64(value)) => *value,
                Some(Cell::Int64(value)) if value.unsigned_abs() <= (1u64 << 53) => *value as f64,
                _ => {
                    return Err(miss(
                        "native result is not representable in the query Float64 protocol",
                    ))
                }
            };
            Ok((labels, value))
        })
        .collect()
}
fn aggregate(
    operation: Aggregation,
    grouping: &Grouping,
    values: Vector,
    context: &physical::RunContext,
) -> Result<Vector, EngineError> {
    use physical::operators::{Operator, Reduction};
    let batch = native_vector_batch(values, grouping)?;
    let reduction = match operation {
        Aggregation::Sum => Reduction::Sum(2),
        Aggregation::Avg => Reduction::Avg(2),
        Aggregation::Count => Reduction::Count,
        Aggregation::Max => Reduction::Max(2),
        Aggregation::Min => Reduction::Min(2),
    };
    let operator = Operator::aggregate(
        batch.schema().clone(),
        vec![1],
        vec![("value".into(), reduction)],
    )
    .map_err(|e| miss(e.to_string()))?;
    native_vector_output(native_batch_rows(batch, vec![operator], context)?, 0, 1)
}

fn negate(value: Value, context: &physical::RunContext) -> Result<Value, EngineError> {
    use physical::operators::{Expression, Operator};
    let scalar = matches!(value, Value::Scalar(_));
    let values = match value {
        Value::Scalar(v) => vec![(Labels::new(), v)],
        Value::Vector(v) => v,
        _ => return Err(miss("cannot negate range vector")),
    };
    let batch = native_vector_batch(
        values,
        &Grouping {
            labels: vec![],
            without: false,
        },
    )?;
    let operator = Operator::project(
        batch.schema().clone(),
        vec![
            ("labels".into(), Expression::Column(0)),
            (
                "value".into(),
                Expression::Negate(Box::new(Expression::Column(2))),
            ),
        ],
    )
    .map_err(|e| miss(e.to_string()))?;
    let result = native_vector_output(native_batch_rows(batch, vec![operator], context)?, 0, 1)?;
    Ok(if scalar {
        Value::Scalar(result[0].1)
    } else {
        Value::Vector(result)
    })
}
fn vector_to_scalar(values: Vector, context: &physical::RunContext) -> Result<Value, EngineError> {
    use physical::{operators::Operator, values::Value as Cell};
    let batch = native_vector_batch(
        values,
        &Grouping {
            labels: vec![],
            without: false,
        },
    )?;
    let operator =
        Operator::vector_to_scalar(batch.schema().clone(), 2).map_err(|e| miss(e.to_string()))?;
    let rows = native_batch_rows(batch, vec![operator], context)?;
    match rows.first().and_then(|row| row.first()) {
        Some(Cell::Float64(value)) => Ok(Value::Scalar(*value)),
        _ => Err(miss("native scalar conversion returned invalid output")),
    }
}

fn grouping_key(labels: &Labels, grouping: &Grouping) -> Labels {
    labels
        .iter()
        .filter(|(key, _)| {
            if grouping.without {
                key.as_str() != "__name__" && !grouping.labels.contains(key)
            } else {
                grouping.labels.contains(key)
            }
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

/// Select by the child sample value while retaining every selected series'
/// labels. NaN ranks below every numeric value, matching Prometheus' TOPK heap.
/// Stable sorting also leaves equal-valued series in the child's order.
#[cfg(test)]
fn topk_selection(
    k: u64,
    grouping: &Grouping,
    values: Vector,
    context: &physical::RunContext,
) -> Result<Vector, EngineError> {
    use physical::operators::{Operator, SortKey};
    let batch = native_vector_batch(values, grouping)?;
    let sort = Operator::sort(
        batch.schema().clone(),
        vec![SortKey {
            column: 2,
            descending: true,
            nulls_first: false,
        }],
        vec![1],
    )
    .map_err(|e| miss(e.to_string()))?;
    let limit = Operator::limit(sort.schema(), k, 0, vec![1]).map_err(|e| miss(e.to_string()))?;
    let mut output =
        native_vector_output(native_batch_rows(batch, vec![sort, limit], context)?, 0, 2)?;
    // The HTTP adapter preserves canonical label-group presentation; native Sort
    // already determined score order within each group.
    output.sort_by_key(|(labels, _)| grouping_key(labels, grouping));
    Ok(output)
}

#[cfg(test)]
fn binary(
    operation: BinaryOperation,
    boolean: bool,
    left: Value,
    right: Value,
) -> Result<Value, EngineError> {
    binary_in_context(operation, boolean, left, right, &test_native_context())
}
fn binary_in_context(
    operation: BinaryOperation,
    boolean: bool,
    left: Value,
    right: Value,
    context: &physical::RunContext,
) -> Result<Value, EngineError> {
    use physical::{
        operators::{Expression, Operator},
        values::{Batch, Value as Cell},
    };
    use planner_types::{
        post_asap::{BinaryOperator, SummaryFamilyType, SummaryField, SummarySchema},
        pre_asap::{ArithmeticOpKind as A, BinaryOpKind, CompareOpKind as C, DataType},
    };
    let kind = match operation {
        BinaryOperation::Add => BinaryOpKind::Arithmetic(A::Add),
        BinaryOperation::Sub => BinaryOpKind::Arithmetic(A::Sub),
        BinaryOperation::Mul => BinaryOpKind::Arithmetic(A::Mul),
        BinaryOperation::Div | BinaryOperation::CheckedDiv | BinaryOperation::FiniteDiv => {
            BinaryOpKind::Arithmetic(A::Div)
        }
        BinaryOperation::Mod => BinaryOpKind::Arithmetic(A::Mod),
        BinaryOperation::Pow => BinaryOpKind::Arithmetic(A::Pow),
        BinaryOperation::Equal => BinaryOpKind::Compare(C::Eq),
        BinaryOperation::NotEqual => BinaryOpKind::Compare(C::Ne),
        BinaryOperation::Less => BinaryOpKind::Compare(C::Lt),
        BinaryOperation::LessEqual => BinaryOpKind::Compare(C::Le),
        BinaryOperation::Greater => BinaryOpKind::Compare(C::Gt),
        BinaryOperation::GreaterEqual => BinaryOpKind::Compare(C::Ge),
    };
    let arithmetic = matches!(kind, BinaryOpKind::Arithmetic(_));
    let scalar_output = matches!((&left, &right), (Value::Scalar(_), Value::Scalar(_)));
    if scalar_output && !arithmetic && !boolean {
        return Err(miss("scalar comparison requires bool"));
    }
    if boolean
        && matches!(
            operation,
            BinaryOperation::CheckedDiv | BinaryOperation::FiniteDiv
        )
    {
        return Err(miss("checked division cannot return bool"));
    }
    // Matching and metric-name presentation are protocol bindings; all numeric
    // computation and checked arithmetic execute in the shared operator.
    let mut pairs = Vec::new();
    let scalar_left = matches!(left, Value::Scalar(_));
    match (left, right) {
        (Value::Scalar(a), Value::Scalar(b)) => pairs.push((Labels::new(), a, b)),
        (Value::Vector(values), Value::Scalar(b)) => {
            for (labels, a) in vector(Value::Vector(values))? {
                pairs.push((labels, a, b));
            }
        }
        (Value::Scalar(a), Value::Vector(values)) => {
            for (labels, b) in vector(Value::Vector(values))? {
                pairs.push((labels, a, b));
            }
        }
        (Value::Vector(left), Value::Vector(right)) => {
            let mut rhs = BTreeMap::new();
            for (labels, value) in right {
                if rhs.insert(no_name(labels), value).is_some() {
                    return Err(miss("duplicate vector matching labels"));
                }
            }
            let mut seen = BTreeSet::new();
            for (labels, value) in left {
                let key = no_name(labels.clone());
                if !seen.insert(key.clone()) {
                    return Err(miss("duplicate vector matching labels"));
                }
                if let Some(right) = rhs.get(&key) {
                    pairs.push((labels, value, *right));
                }
            }
        }
        _ => return Err(miss("binary matrix unsupported")),
    }
    let schema = std::sync::Arc::new(SummarySchema {
        fields: ["left", "right"]
            .into_iter()
            .map(|name| SummaryField {
                name: name.into(),
                dtype: SummaryFamilyType::Plain(DataType::Float64),
                nullable: false,
            })
            .collect(),
        time_index: None,
    });
    let batch = Batch::try_new(
        schema.clone(),
        pairs
            .iter()
            .map(|(_, a, b)| vec![Cell::Float64(*a), Cell::Float64(*b)])
            .collect(),
    )
    .map_err(|e| miss(e.to_string()))?;
    let operator = Operator::project(
        schema,
        vec![(
            "value".into(),
            Expression::Binary {
                operator: BinaryOperator {
                    kind,
                    vector_match: None,
                    checked_relative_division: operation == BinaryOperation::CheckedDiv,
                    checked_finite_division: operation == BinaryOperation::FiniteDiv,
                },
                left: Box::new(Expression::Column(0)),
                right: Box::new(Expression::Column(1)),
            },
        )],
    )
    .map_err(|e| miss(e.to_string()))?;
    let rows = native_batch_rows(batch, vec![operator], context)?;
    let mut output = Vec::new();
    for ((labels, a, b), row) in pairs.into_iter().zip(rows) {
        let value = match row.first() {
            Some(Cell::Float64(value)) => *value,
            Some(Cell::Bool(value)) if boolean => {
                if *value {
                    1.
                } else {
                    0.
                }
            }
            Some(Cell::Bool(true)) => {
                if scalar_left {
                    b
                } else {
                    a
                }
            }
            Some(Cell::Bool(false)) => continue,
            _ => return Err(miss("native binary result schema mismatch")),
        };
        output.push((
            if arithmetic || boolean {
                no_name(labels)
            } else {
                labels
            },
            value,
        ));
    }
    if scalar_output {
        return Ok(Value::Scalar(
            output
                .first()
                .ok_or_else(|| miss("missing scalar result"))?
                .1,
        ));
    }
    Ok(Value::Vector(vector(Value::Vector(output))?))
}

fn native_temporal(
    values: Matrix,
    operation: TemporalOperation,
    start: i64,
    end: i64,
    context: &physical::RunContext,
) -> Result<Vector, EngineError> {
    use planner_types::pre_asap::AggIntent;
    let intent = match operation {
        TemporalOperation::Rate => AggIntent::Rate,
        TemporalOperation::Increase => AggIntent::Increase,
        TemporalOperation::Sum => AggIntent::Sum { col: None },
        TemporalOperation::Avg => AggIntent::Avg { col: None },
        TemporalOperation::Min => AggIntent::Min { col: None },
        TemporalOperation::Max => AggIntent::Max { col: None },
        TemporalOperation::Count => AggIntent::Count {
            accuracy: planner_types::types::AccuracyTarget::Exact,
        },
    };
    let rows = values
        .into_iter()
        .flat_map(|(labels, points)| {
            points.into_iter().map(move |(time, value)| {
                vec![
                    native_labels(&labels),
                    physical::values::Value::Timestamp(time),
                    physical::values::Value::Float64(value),
                ]
            })
        })
        .collect();
    native_window(rows, intent, Some((start, end)), context)
}
fn native_window(
    rows: Vec<Vec<physical::values::Value>>,
    intent: planner_types::pre_asap::AggIntent<planner_types::pre_asap::ColumnRef>,
    window: Option<(i64, i64)>,
    context: &physical::RunContext,
) -> Result<Vector, EngineError> {
    use planner_types::{
        post_asap::{SummaryFamilyType, SummaryField, SummarySchema},
        pre_asap::DataType,
    };
    let schema = std::sync::Arc::new(SummarySchema {
        fields: vec![
            (
                "labels",
                DataType::Map {
                    key: Box::new(DataType::Utf8),
                    value: Box::new(DataType::Utf8),
                    value_nullable: false,
                },
            ),
            (
                "coordinate",
                if window.is_some() {
                    DataType::Timestamp
                } else {
                    DataType::Float64
                },
            ),
            ("value", DataType::Float64),
        ]
        .into_iter()
        .map(|(name, dtype)| SummaryField {
            name: name.into(),
            dtype: SummaryFamilyType::Plain(dtype),
            nullable: false,
        })
        .collect(),
        time_index: None,
    });
    let batch =
        physical::values::Batch::try_new(schema.clone(), rows).map_err(|e| miss(e.to_string()))?;
    let operator = physical::operators::Operator::window(schema, intent, 1, 2, vec![0], window)
        .map_err(|e| miss(e.to_string()))?;
    native_vector_output(native_batch_rows(batch, vec![operator], context)?, 0, 1)
}

#[cfg(test)]
fn test_native_context() -> physical::RunContext {
    physical::RunContext::new(
        physical::Scope::Query {
            evaluation_time_ms: 0,
            revision: 0,
        },
        physical::Limits::default(),
    )
    .unwrap()
}

#[cfg(test)]
mod topk_tests {
    use super::*;
    use asap_types::query_plan::{FallbackPolicy, InstantExecution};

    fn labels(items: &[(&str, &str)]) -> Labels {
        items
            .iter()
            .map(|(key, value)| ((*key).into(), (*value).into()))
            .collect()
    }

    // An overflowing sum cannot implement average, but zero/subnormal averages remain valid.
    #[test]
    fn finite_division_guards_temporal_average_without_rejecting_zero() {
        let mut sum = asap_physical_operators::summary_kernels::sum::SumAccumulator::new();
        sum.update(1e308);
        sum.update(1e308);
        assert!(binary(
            BinaryOperation::FiniteDiv,
            false,
            Value::Scalar(sum.sum),
            Value::Scalar(2.0)
        )
        .is_err());
        for (a, b, expected) in [
            (0.0, 2.0, 0.0),
            (10.0, 2.0, 5.0),
            (f64::MIN_POSITIVE, 2.0, f64::MIN_POSITIVE / 2.0),
        ] {
            let Value::Scalar(value) = binary(
                BinaryOperation::FiniteDiv,
                false,
                Value::Scalar(a),
                Value::Scalar(b),
            )
            .unwrap() else {
                panic!("scalar")
            };
            assert_eq!(value, expected);
        }
        assert!(binary(
            BinaryOperation::FiniteDiv,
            false,
            Value::Scalar(1.0),
            Value::Scalar(0.0)
        )
        .is_err());
    }

    // A conditional accuracy certificate must fall back rather than return an unbounded ratio.
    #[test]
    fn checked_relative_division_enforces_its_execution_domain() {
        for (a, b) in [
            (1., 0.),
            (0., 0.),
            (1., f64::INFINITY),
            (f64::NAN, 2.),
            (f64::MAX, f64::MIN_POSITIVE),
            (f64::MIN_POSITIVE, f64::MAX),
        ] {
            assert!(binary(
                BinaryOperation::CheckedDiv,
                false,
                Value::Scalar(a),
                Value::Scalar(b)
            )
            .is_err());
        }
        let Value::Scalar(value) = binary(
            BinaryOperation::CheckedDiv,
            false,
            Value::Scalar(5.),
            Value::Scalar(10.),
        )
        .unwrap() else {
            panic!("scalar");
        };
        assert_eq!(value, 0.5);
    }

    #[test]
    fn topk_selects_by_sample_value_and_preserves_series_labels() {
        let values = vec![
            (
                labels(&[("__name__", "cpu"), ("job", "api"), ("pod", "a")]),
                4.0,
            ),
            (
                labels(&[("__name__", "cpu"), ("job", "api"), ("pod", "b")]),
                9.0,
            ),
            (
                labels(&[("__name__", "cpu"), ("job", "db"), ("pod", "c")]),
                7.0,
            ),
            (
                labels(&[("__name__", "cpu"), ("job", "db"), ("pod", "d")]),
                2.0,
            ),
        ];
        let selected = topk_selection(
            1,
            &Grouping {
                labels: vec!["job".into()],
                without: false,
            },
            values,
            &test_native_context(),
        )
        .unwrap();
        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0].0["pod"], "b");
        assert_eq!(selected[0].1, 9.0);
        assert_eq!(selected[1].0["pod"], "c");
        assert_eq!(selected[1].1, 7.0);
        assert!(selected
            .iter()
            .all(|(labels, _)| labels.contains_key("__name__")));
    }

    #[test]
    fn topk_ranks_nan_below_numbers_and_keeps_exact_child_values() {
        let selected = topk_selection(
            2,
            &Grouping {
                labels: vec![],
                without: false,
            },
            vec![
                (labels(&[("series", "nan")]), f64::NAN),
                (labels(&[("series", "low")]), -1.0),
                (labels(&[("series", "high")]), 3.0),
            ],
            &test_native_context(),
        )
        .unwrap();
        let selected = topk_selection(
            2,
            &Grouping {
                labels: vec![],
                without: false,
            },
            selected,
            &test_native_context(),
        )
        .unwrap();
        assert_eq!(
            selected
                .iter()
                .map(|row| row.0["series"].as_str())
                .collect::<Vec<_>>(),
            vec!["high", "low"]
        );
        assert_eq!(
            selected.iter().map(|row| row.1).collect::<Vec<_>>(),
            vec![3.0, -1.0]
        );
    }

    #[test]
    fn installed_topk_combines_with_prometheus_exact_child() {
        let mut entry = control_plane::query_plan::residual::compile_logical(
            "hybrid-topk".into(),
            "topk(2, m)".into(),
            InstantExecution {
                lookback_ms: 300_000,
                full_history: false,
                cumulative_readout: false,
            },
            FallbackPolicy::ExactBackend,
        )
        .unwrap();
        control_plane::query_plan::residual::finalize_residuals(&mut entry).unwrap();
        let leaf = entry
            .nodes
            .iter()
            .find_map(|(id, node)| {
                matches!(
                    node,
                    QueryPlanNode::Logical {
                        operator: ResidualQueryOperator::ExactSubquery { .. },
                        ..
                    }
                )
                .then_some(*id)
            })
            .unwrap();
        let at = 1_000_u64;
        let leaves = [(
            (leaf, at as i64),
            PreparedLeaf {
                value: Value::Vector(vec![
                    (labels(&[("pod", "a")]), 1.0),
                    (labels(&[("pod", "b")]), 8.0),
                    (labels(&[("pod", "c")]), 5.0),
                ]),
                remote: true,
                remote_evaluations: 1,
                remote_rpcs: 1,
            },
        )]
        .into_iter()
        .collect();
        let (result, stats) = execute_installed(&entry, &leaves, at, |_, _| {
            panic!("summary callback must not run for an exact-child topk")
        })
        .unwrap();
        let QueryResult::Vector(result) = result else {
            panic!("instant vector expected")
        };
        assert_eq!(
            result
                .values
                .iter()
                .map(|point| point.value)
                .collect::<Vec<_>>(),
            vec![8.0, 5.0]
        );
        assert_eq!(stats.remote_branch_evaluations, 1);
        assert_eq!(stats.remote_rpcs, 1);
        assert_eq!(stats.raw_scan_evaluations, 0);
    }

    // A temporal operator over an external subquery follows the same language
    // policy as a summary readout; changing execution placement cannot drop names.
    #[test]
    fn metricsql_temporal_subdag_preserves_names_only_for_value_rollups() {
        use control_plane::query_plan::QueryLanguage;
        for language in [QueryLanguage::PromQl, QueryLanguage::MetricsQl] {
            for operation in [
                TemporalOperation::Max,
                TemporalOperation::Min,
                TemporalOperation::Avg,
                TemporalOperation::Sum,
                TemporalOperation::Count,
                TemporalOperation::Rate,
            ] {
                let entry = QueryPlanEntry {
                    language,
                    query_id: "labels".into(),
                    canonical_query: "test".into(),
                    fixed_evaluation: None,
                    root: QueryNodeId(1),
                    nodes: BTreeMap::from([
                        (
                            QueryNodeId(0),
                            QueryPlanNode::Logical {
                                operator: ResidualQueryOperator::ExactSubquery {
                                    query: "m[1s]".into(),
                                },
                                inputs: vec![],
                            },
                        ),
                        (
                            QueryNodeId(1),
                            QueryPlanNode::Logical {
                                operator: ResidualQueryOperator::Temporal { operation },
                                inputs: vec![QueryNodeId(0)],
                            },
                        ),
                    ]),
                    instant: InstantExecution {
                        lookback_ms: 1000,
                        full_history: false,
                        cumulative_readout: true,
                    },
                    fallback: FallbackPolicy::ExactBackend,
                };
                let leaves = BTreeMap::from([(
                    (QueryNodeId(0), 1000),
                    PreparedLeaf {
                        value: Value::Matrix(
                            vec![(
                                labels(&[("__name__", "m"), ("job", "api")]),
                                vec![(100, 1.), (900, 3.)],
                            )],
                            0,
                            1000,
                        ),
                        remote: true,
                        remote_evaluations: 1,
                        remote_rpcs: 1,
                    },
                )]);
                let (result, _) = execute_installed(&entry, &leaves, 1000, |_, _| {
                    panic!("external child supplied")
                })
                .unwrap();
                let QueryResult::Vector(result) = result else {
                    panic!("vector required")
                };
                let expected = language == QueryLanguage::MetricsQl
                    && matches!(
                        operation,
                        TemporalOperation::Max | TemporalOperation::Min | TemporalOperation::Avg
                    );
                assert_eq!(
                    result.values[0]
                        .label_keys_override
                        .as_ref()
                        .unwrap()
                        .iter()
                        .any(|name| name == "__name__"),
                    expected,
                    "{language:?} {operation:?}"
                );
            }
        }
    }

    #[test]
    fn installed_topk_ranks_exact_rate_summary_values() {
        let summary = QueryNodeId(0);
        let root = QueryNodeId(1);
        let entry = QueryPlanEntry {
            language: asap_types::query_plan::QueryLanguage::PromQl,
            query_id: "summary-rate-topk".into(),
            canonical_query: "topk(2, rate(requests_total[5m]))".into(),
            fixed_evaluation: None,
            root,
            nodes: BTreeMap::from([
                (
                    summary,
                    QueryPlanNode::ExactReadout {
                        input: QueryNodeId(99),
                        readout: asap_types::query_plan::ExactReadout::Rate,
                    },
                ),
                (
                    root,
                    QueryPlanNode::Logical {
                        operator: ResidualQueryOperator::Limit {
                            offset: 0,
                            n: 2,
                            grouping: Grouping {
                                labels: vec![],
                                without: false,
                            },
                        },
                        inputs: vec![QueryNodeId(98)],
                    },
                ),
                (
                    QueryNodeId(98),
                    QueryPlanNode::Logical {
                        operator: ResidualQueryOperator::Sort {
                            descending: true,
                            grouping: Grouping {
                                labels: vec![],
                                without: false,
                            },
                        },
                        inputs: vec![summary],
                    },
                ),
            ]),
            instant: InstantExecution {
                lookback_ms: 300_000,
                full_history: false,
                cumulative_readout: false,
            },
            fallback: FallbackPolicy::ExactBackend,
        };
        let (result, stats) = execute_installed(&entry, &BTreeMap::new(), 300_000, |id, at| {
            assert_eq!(id, summary);
            assert_eq!(at, 300_000);
            Ok(QueryResult::Vector(
                crate::query_engines::query_result::InstantVector {
                    values: vec![
                        InstantVectorElement::new(
                            KeyByLabelValues::new_with_labels(vec!["a".into()]),
                            0.4,
                        )
                        .with_label_keys_override(vec!["pod".into()]),
                        InstantVectorElement::new(
                            KeyByLabelValues::new_with_labels(vec!["b".into()]),
                            1.2,
                        )
                        .with_label_keys_override(vec!["pod".into()]),
                        InstantVectorElement::new(
                            KeyByLabelValues::new_with_labels(vec!["c".into()]),
                            0.8,
                        )
                        .with_label_keys_override(vec!["pod".into()]),
                    ],
                    timestamp: at,
                    warnings: vec![],
                    accuracy: None,
                    window_used: Some((0, at)),
                },
            ))
        })
        .unwrap();
        let QueryResult::Vector(result) = result else {
            panic!("instant vector expected")
        };
        assert_eq!(
            result
                .values
                .iter()
                .map(|point| point.value)
                .collect::<Vec<_>>(),
            vec![1.2, 0.8]
        );
        assert_eq!(stats.summary_readout_evaluations, 1);
        assert_eq!(stats.remote_branch_evaluations, 0);
    }

    fn topk_membership_guarantee() -> planner_types::post_asap::ResultGuarantee {
        use planner_types::post_asap::{BoundExpr, ErrorMetric, ProbabilityExpr, ResultGuarantee};
        ResultGuarantee {
            metric: ErrorMetric::TopKMembership,
            bound: BoundExpr::Zero,
            failure_probability: ProbabilityExpr::Constant { value: 0.01 },
            provenance: vec![],
        }
    }

    #[test]
    fn candidate_sidecar_intersects_then_reranks_exact_values() {
        let candidates = vec![
            (labels(&[("pod", "b")]), 99.0),
            (labels(&[("pod", "c")]), 50.0),
        ];
        let exact = vec![
            (labels(&[("pod", "a")]), 10.0),
            (labels(&[("pod", "b")]), 8.0),
            (labels(&[("pod", "c")]), 9.0),
        ];
        let (selected, warning) = semi_join(
            candidates,
            exact,
            &[("pod".into(), "pod".into())],
            Some(&CandidateCompleteness::Certified {
                guarantee: topk_membership_guarantee(),
            }),
            &test_native_context(),
        )
        .unwrap();
        let selected = topk_selection(
            2,
            &Grouping {
                labels: vec![],
                without: false,
            },
            selected,
            &test_native_context(),
        )
        .unwrap();
        assert_eq!(
            selected
                .iter()
                .map(|row| row.0["pod"].as_str())
                .collect::<Vec<_>>(),
            vec!["c", "b"]
        );
        assert!(warning.is_none());
    }

    #[test]
    fn installed_candidate_sidecar_reads_both_summary_inputs() {
        let candidate_id = QueryNodeId(0);
        let value_id = QueryNodeId(1);
        let filter = QueryNodeId(2);
        let root = QueryNodeId(3);
        let entry = QueryPlanEntry {
            language: asap_types::query_plan::QueryLanguage::PromQl,
            query_id: "candidate-topk".into(),
            canonical_query: "topk(1, rate(requests_total[5m]))".into(),
            fixed_evaluation: None,
            root,
            nodes: BTreeMap::from([
                (
                    candidate_id,
                    QueryPlanNode::ExactFallback {
                        reason: "prepared sketch readout".into(),
                    },
                ),
                (
                    value_id,
                    QueryPlanNode::ExactFallback {
                        reason: "prepared exact counter readout".into(),
                    },
                ),
                (filter, {
                    let schema = planner_types::post_asap::SummarySchema {
                        fields: vec![planner_types::post_asap::SummaryField {
                            name: "pod".into(),
                            dtype: planner_types::post_asap::SummaryFamilyType::Plain(
                                planner_types::pre_asap::DataType::Utf8,
                            ),
                            nullable: false,
                        }],
                        time_index: None,
                    };
                    QueryPlanNode::RelationalJoin {
                        inputs: [value_id, candidate_id],
                        join_kind: planner_types::pre_asap::JoinKind::Semi,
                        pred: serde_json::to_value(planner_types::pre_asap::Predicate(
                            std::rc::Rc::new(planner_types::pre_asap::QueryExpr::Compare {
                                left: std::rc::Rc::new(planner_types::pre_asap::QueryExpr::Column(
                                    0,
                                )),
                                op: planner_types::pre_asap::CompareOpKind::Eq,
                                right: std::rc::Rc::new(
                                    planner_types::pre_asap::QueryExpr::Column(1),
                                ),
                            }),
                        ))
                        .unwrap(),
                        pruning: Some(CandidateCompleteness::Certified {
                            guarantee: topk_membership_guarantee(),
                        }),
                        left_schema: schema.clone(),
                        right_schema: schema.clone(),
                        output_schema: schema,
                    }
                }),
                (
                    root,
                    QueryPlanNode::Logical {
                        operator: ResidualQueryOperator::Limit {
                            offset: 0,
                            n: 1,
                            grouping: Grouping {
                                labels: vec![],
                                without: false,
                            },
                        },
                        inputs: vec![QueryNodeId(98)],
                    },
                ),
                (
                    QueryNodeId(98),
                    QueryPlanNode::Logical {
                        operator: ResidualQueryOperator::Sort {
                            descending: true,
                            grouping: Grouping {
                                labels: vec![],
                                without: false,
                            },
                        },
                        inputs: vec![filter],
                    },
                ),
            ]),
            instant: InstantExecution {
                lookback_ms: 300_000,
                full_history: false,
                cumulative_readout: false,
            },
            fallback: FallbackPolicy::ExactBackend,
        };
        let at = 300_000_i64;
        let leaves = BTreeMap::from([
            (
                (candidate_id, at),
                PreparedLeaf {
                    value: Value::Vector(vec![
                        (labels(&[("pod", "b")]), 100.0),
                        (labels(&[("pod", "c")]), 1.0),
                    ]),
                    remote: false,
                    remote_evaluations: 0,
                    remote_rpcs: 0,
                },
            ),
            (
                (value_id, at),
                PreparedLeaf {
                    value: Value::Vector(vec![
                        (labels(&[("pod", "a")]), 2.0),
                        (labels(&[("pod", "b")]), 1.0),
                        (labels(&[("pod", "c")]), 3.0),
                    ]),
                    remote: false,
                    remote_evaluations: 0,
                    remote_rpcs: 0,
                },
            ),
        ]);
        let (result, stats) = execute_installed(&entry, &leaves, at as u64, |_, _| {
            panic!("both inputs are prepared")
        })
        .unwrap();
        let QueryResult::Vector(result) = result else {
            panic!("vector expected")
        };
        assert_eq!(result.values.len(), 1);
        assert_eq!(result.values[0].value, 3.0, "exact value is authoritative");
        assert_eq!(result.values[0].labels.labels, vec!["c"]);
        assert_eq!(stats.summary_readout_evaluations, 2);
        assert!(result.warnings.is_empty());
    }

    #[test]
    fn uncertified_candidate_sidecar_warns_or_falls_back_explicitly() {
        let candidates = vec![(labels(&[("pod", "a")]), 1.0)];
        let exact = vec![(labels(&[("pod", "a")]), 2.0)];
        let (_, warning) = semi_join(
            candidates.clone(),
            exact.clone(),
            &[("pod".into(), "pod".into())],
            Some(&CandidateCompleteness::BestEffort { guarantee: None }),
            &test_native_context(),
        )
        .unwrap();
        assert!(warning.unwrap().contains("approximate"));
        // Exact queries never lower an uncertified pruning semi-join. The Planner
        // emits its ordinary exact fallback instead; this runtime node is only
        // valid for certified or explicitly approximate plans.
        let certified = CandidateCompleteness::Certified {
            guarantee: topk_membership_guarantee(),
        };
        assert!(semi_join(
            vec![(labels(&[("pod", "missing")]), 1.0)],
            exact,
            &[("pod".into(), "pod".into())],
            Some(&certified),
            &test_native_context(),
        )
        .is_err());
    }
}

#[cfg(test)]
mod shared_runtime_tests {
    use super::*;
    use asap_types::query_plan::{FallbackPolicy, InstantExecution, QueryLanguage};

    fn entry() -> QueryPlanEntry {
        QueryPlanEntry {
            language: QueryLanguage::PromQl,
            query_id: "shared-grid".into(),
            canonical_query: "shared-grid".into(),
            fixed_evaluation: None,
            root: QueryNodeId(3),
            // The callback owns the absorbed summary dependencies. Only its
            // declared readout boundary participates in this value graph.
            nodes: BTreeMap::from([
                (
                    QueryNodeId(0),
                    QueryPlanNode::ExactReadout {
                        input: QueryNodeId(99),
                        readout: asap_types::query_plan::ExactReadout::Sum,
                    },
                ),
                (
                    QueryNodeId(1),
                    QueryPlanNode::Logical {
                        operator: ResidualQueryOperator::Subquery {
                            range_ms: 2000,
                            step_ms: 1000,
                            offset_ms: 0,
                        },
                        inputs: vec![QueryNodeId(0)],
                    },
                ),
                (
                    QueryNodeId(2),
                    QueryPlanNode::Logical {
                        operator: ResidualQueryOperator::Temporal {
                            operation: TemporalOperation::Sum,
                        },
                        inputs: vec![QueryNodeId(1)],
                    },
                ),
                (
                    QueryNodeId(3),
                    QueryPlanNode::Logical {
                        operator: ResidualQueryOperator::Binary {
                            operation: BinaryOperation::Add,
                            return_bool: false,
                        },
                        inputs: vec![QueryNodeId(2), QueryNodeId(2)],
                    },
                ),
            ]),
            instant: InstantExecution {
                lookback_ms: 2000,
                full_history: false,
                cumulative_readout: false,
            },
            fallback: FallbackPolicy::ExactBackend,
        }
    }

    // A shared time-grid node runs once per query; distinct times and runs stay isolated.
    #[test]
    fn shared_subquery_scopes_do_not_duplicate_or_leak_values() {
        let entry = entry();
        let mut calls = Vec::new();
        for (at, expected) in [(3000, 10.), (4000, 14.)] {
            let (result, stats) = execute_installed(&entry, &BTreeMap::new(), at, |id, time| {
                assert_eq!(id, QueryNodeId(0));
                calls.push(time);
                Ok(QueryResult::vector(
                    vec![InstantVectorElement::new(
                        KeyByLabelValues::new_with_labels(vec!["a".into()]),
                        time as f64 / 1000.,
                    )
                    .with_label_keys_override(vec!["pod".into()])],
                    time,
                ))
            })
            .unwrap();
            let QueryResult::Vector(result) = result else {
                panic!("vector required");
            };
            assert_eq!(result.values[0].value, expected);
            assert_eq!(stats.summary_readout_evaluations, 2);
            assert!(stats.memo_hits >= 1);
        }
        assert_eq!(calls, vec![2000, 3000, 3000, 4000]);
    }

    // Query adapters use native computation and its parent execution budget.
    #[test]
    fn native_scalar_and_aggregation_share_parent_resource_control() {
        let context = test_native_context();
        assert_eq!(native_scalar(7., &context).unwrap(), 7.);
        let output = aggregate(
            Aggregation::Sum,
            &Grouping {
                labels: vec![],
                without: false,
            },
            vec![(Labels::new(), 2.), (Labels::new(), 5.)],
            &context,
        )
        .unwrap();
        assert_eq!(output, vec![(Labels::new(), 7.)]);
        assert!(context.peak_bytes() > 0);
        context.cancel();
        assert!(native_scalar(7., &context).is_err());
        assert!(negate(Value::Scalar(1.), &context).is_err());
    }

    // Source failures keep their routing classification across the shared runtime.
    #[test]
    fn source_error_classification_survives_execution() {
        let error = execute_installed(&entry(), &BTreeMap::new(), 3000, |_, _| {
            Err(EngineError::capability_miss("source", "failed"))
        })
        .unwrap_err();
        assert!(matches!(error,EngineError::CapabilityMiss{engine_id,..} if engine_id=="source"));
    }
}
