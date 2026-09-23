//! Executes the installed typed logical DAG. No serving-time PromQL parsing.
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
        inputs: &[Value],
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
            QueryPlanNode::MembershipFilter { completeness, .. } => {
                let [candidates, values] = inputs else {
                    return Err(miss("membership operator requires two inputs"));
                };
                let candidates = vector(candidates.clone())?;
                let values = vector(values.clone())?;
                let (selected, warning) = membership_filter(candidates, values, &completeness)?;
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
        inputs: &[Value],
        dependencies: &[(QueryNodeId, i64)],
        at: i64,
        context: &physical::RunContext,
    ) -> Result<Value, EngineError> {
        let input = |index: usize| {
            inputs
                .get(index)
                .cloned()
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
            ResidualQueryOperator::TopKSelection { k, grouping } => {
                let values = vector(input(0)?)?;
                Ok(Value::Vector(topk_selection(
                    k, &grouping, values, context,
                )?))
            }
            ResidualQueryOperator::Binary {
                operation,
                return_bool,
            } => {
                let left = input(0)?;
                let right = input(1)?;
                binary(operation, return_bool, left, right)
            }
            ResidualQueryOperator::Temporal { operation } => {
                let Value::Matrix(values, start, end) = input(0)? else {
                    return Err(miss("temporal operator requires range vector"));
                };
                Ok(Value::Vector(
                    values
                        .into_iter()
                        .filter_map(|(labels, points)| {
                            if points.is_empty() {
                                return None;
                            }
                            let value = match operation {
                                TemporalOperation::Rate => rate(&points, start, end),
                                TemporalOperation::Increase => rate(&points, start, end)
                                    .map(|r| r * (end - start) as f64 / 1000.),
                                TemporalOperation::Sum => Some(points.iter().map(|p| p.1).sum()),
                                TemporalOperation::Avg => Some(
                                    points.iter().map(|p| p.1).sum::<f64>() / points.len() as f64,
                                ),
                                TemporalOperation::Count => Some(points.len() as f64),
                                TemporalOperation::Max => {
                                    Some(points.iter().fold(f64::NAN, |a, p| {
                                        if a.is_nan() || p.1 > a {
                                            p.1
                                        } else {
                                            a
                                        }
                                    }))
                                }
                                TemporalOperation::Min => {
                                    Some(points.iter().fold(f64::NAN, |a, p| {
                                        if a.is_nan() || p.1 < a {
                                            p.1
                                        } else {
                                            a
                                        }
                                    }))
                                }
                            };
                            value.map(|v| {
                                let preserve_name = self.entry.language
                                    == control_plane::query_plan::QueryLanguage::MetricsQl
                                    && matches!(
                                        operation,
                                        TemporalOperation::Min
                                            | TemporalOperation::Max
                                            | TemporalOperation::Avg
                                    );
                                (
                                    if preserve_name {
                                        labels
                                    } else {
                                        no_name(labels)
                                    },
                                    v,
                                )
                            })
                        })
                        .collect(),
                ))
            }
            ResidualQueryOperator::Sort { descending } => Ok(Value::Vector(sort_values(
                vector(input(0)?)?,
                descending,
                context,
            )?)),
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
                Ok(Value::Vector(
                    groups
                        .into_iter()
                        .map(|(labels, buckets)| (labels, bucket_quantile(quantile, buckets)))
                        .collect(),
                ))
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
                    for (labels, value) in vector(value.clone())? {
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
        QueryPlanNode::MembershipFilter { inputs, .. } => {
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
            let values = values.iter().map(|v| v.value().clone()).collect::<Vec<_>>();
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
                .map_err(|error| {
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

fn membership_filter(
    candidates: Vector,
    values: Vector,
    completeness: &CandidateCompleteness,
) -> Result<(Vector, Option<String>), EngineError> {
    let identity = |labels: &Labels| {
        let mut labels = labels.clone();
        labels.remove("__name__");
        labels
    };
    let (selected, missing) = asap_physical_operators::rows::membership_filter(
        candidates.iter().map(|(labels, _)| identity(labels)),
        values,
        |(labels, _)| identity(labels),
    );
    if !missing.is_empty() && matches!(completeness, CandidateCompleteness::Certified { .. }) {
        return Err(miss("certified membership key has no authoritative value"));
    }
    let warning = match completeness {
        CandidateCompleteness::Certified { .. } => None,
        CandidateCompleteness::BestEffort { guarantee } => Some(match guarantee {
            Some(guarantee) => format!(
                "ASAP membership pruning is approximate: {:?}",
                guarantee.metric
            ),
            None => "ASAP membership pruning is approximate and uncertified".into(),
        }),
    };
    Ok((selected, warning))
}

fn native_scalar(value: f64, context: &physical::RunContext) -> Result<f64, EngineError> {
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
fn sort_values(
    values: Vector,
    descending: bool,
    context: &physical::RunContext,
) -> Result<Vector, EngineError> {
    use physical::operators::{Operator, SortKey};
    let batch = native_vector_batch(
        values,
        &Grouping {
            labels: vec![],
            without: false,
        },
    )?;
    let operator = Operator::sort(
        batch.schema().clone(),
        vec![SortKey {
            column: 2,
            descending,
            nulls_first: false,
        }],
        vec![],
    )
    .map_err(|e| miss(e.to_string()))?;
    native_vector_output(native_batch_rows(batch, vec![operator], context)?, 0, 2)
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

fn binary(
    operation: BinaryOperation,
    boolean: bool,
    left: Value,
    right: Value,
) -> Result<Value, EngineError> {
    if matches!(
        operation,
        BinaryOperation::CheckedDiv | BinaryOperation::FiniteDiv
    ) {
        let valid = |value: &Value, denominator: bool| match value {
            Value::Scalar(v) => v.is_finite() && (!denominator || *v != 0.0),
            Value::Vector(rows) => rows
                .iter()
                .all(|(_, v)| v.is_finite() && (!denominator || *v != 0.0)),
            Value::Matrix(..) => false,
        };
        if boolean || !valid(&left, false) || !valid(&right, true) {
            return Err(miss(
                "checked division requires finite operands and a nonzero divisor",
            ));
        }
        let result = binary(BinaryOperation::Div, false, left, right)?;
        let valid_result = |v: &f64| {
            if operation == BinaryOperation::FiniteDiv {
                v.is_finite()
            } else {
                v.is_normal()
            }
        };
        let normal = match &result {
            Value::Scalar(v) => valid_result(v),
            Value::Vector(rows) => rows.iter().all(|(_, v)| valid_result(v)),
            Value::Matrix(..) => false,
        };
        return if normal {
            Ok(result)
        } else {
            Err(miss(
                "checked division result is outside the declared floating-point domain",
            ))
        };
    }
    let arithmetic = matches!(
        operation,
        BinaryOperation::Add
            | BinaryOperation::Sub
            | BinaryOperation::Mul
            | BinaryOperation::Div
            | BinaryOperation::Mod
            | BinaryOperation::Pow
    );
    let combine = |a: f64, b: f64| -> Option<f64> {
        Some(match operation {
            BinaryOperation::Add => a + b,
            BinaryOperation::Sub => a - b,
            BinaryOperation::Mul => a * b,
            BinaryOperation::Div => a / b,
            BinaryOperation::Mod => a % b,
            BinaryOperation::Pow => a.powf(b),
            _ => {
                let pass = match operation {
                    BinaryOperation::Equal => a == b,
                    BinaryOperation::NotEqual => a != b,
                    BinaryOperation::Less => a < b,
                    BinaryOperation::LessEqual => a <= b,
                    BinaryOperation::Greater => a > b,
                    BinaryOperation::GreaterEqual => a >= b,
                    _ => unreachable!(),
                };
                if boolean {
                    if pass {
                        1.
                    } else {
                        0.
                    }
                } else if pass {
                    a
                } else {
                    return None;
                }
            }
        })
    };
    let values = match (left, right) {
        (Value::Scalar(a), Value::Scalar(b)) => {
            if !arithmetic && !boolean {
                return Err(miss("scalar comparison requires bool"));
            }
            return Ok(Value::Scalar(combine(a, b).unwrap_or(0.)));
        }
        (Value::Vector(values), Value::Scalar(scalar)) => vector(Value::Vector(values))?
            .into_iter()
            .filter_map(|(labels, value)| {
                combine(value, scalar).map(|v| {
                    (
                        if arithmetic || boolean {
                            no_name(labels)
                        } else {
                            labels
                        },
                        v,
                    )
                })
            })
            .collect(),
        (Value::Scalar(scalar), Value::Vector(values)) => vector(Value::Vector(values))?
            .into_iter()
            .filter_map(|(labels, value)| {
                combine(scalar, value).map(|v| {
                    (
                        if arithmetic || boolean {
                            no_name(labels)
                        } else {
                            labels
                        },
                        if arithmetic || boolean { v } else { value },
                    )
                })
            })
            .collect(),
        (Value::Vector(left), Value::Vector(right)) => {
            let mut rhs = BTreeMap::new();
            for (labels, value) in right {
                if rhs.insert(no_name(labels), value).is_some() {
                    return Err(miss("duplicate vector matching labels"));
                }
            }
            let mut seen = BTreeSet::new();
            let mut out = Vec::new();
            for (labels, value) in left {
                let key = no_name(labels.clone());
                if !seen.insert(key.clone()) {
                    return Err(miss("duplicate vector matching labels"));
                }
                if let Some(right) = rhs.get(&key) {
                    if let Some(v) = combine(value, *right) {
                        out.push((if arithmetic || boolean { key } else { labels }, v));
                    }
                }
            }
            out
        }
        _ => return Err(miss("binary matrix unsupported")),
    };
    Ok(Value::Vector(vector(Value::Vector(values))?))
}

fn rate(points: &[(i64, f64)], start: i64, end: i64) -> Option<f64> {
    if points.len() < 2 {
        return None;
    }
    let (first_t, first) = points[0];
    let (last_t, last) = *points.last()?;
    let span = (last_t - first_t) as f64 / 1000.;
    if span <= 0. {
        return None;
    }
    let mut delta = last - first;
    for pair in points.windows(2) {
        if pair[1].1 < pair[0].1 {
            delta += pair[0].1;
        }
    }
    let average = span / (points.len() - 1) as f64;
    let mut to_start = (first_t - start) as f64 / 1000.;
    let mut to_end = (end - last_t) as f64 / 1000.;
    if to_start >= average * 1.1 {
        to_start = average / 2.;
    }
    // Apply the zero bound after the sparse-window half-interval cap.
    if delta > 0. && first >= 0. {
        to_start = to_start.min(span * first / delta);
    }
    if to_end >= average * 1.1 {
        to_end = average / 2.;
    }
    Some(delta * (span + to_start + to_end) / span / ((end - start) as f64 / 1000.))
}

fn bucket_quantile(q: f64, mut b: Vec<(f64, f64)>) -> f64 {
    if q.is_nan() {
        return f64::NAN;
    }
    if q < 0. {
        return f64::NEG_INFINITY;
    }
    if q > 1. {
        return f64::INFINITY;
    }
    b.retain(|p| !p.0.is_nan());
    b.sort_by(|a, b| a.0.total_cmp(&b.0));
    let mut buckets: Vec<(f64, f64)> = Vec::new();
    for p in b {
        if let Some(last) = buckets.last_mut() {
            if last.0 == p.0 {
                last.1 += p.1;
                continue;
            }
        }
        buckets.push(p);
    }
    if buckets.len() < 2 || buckets.last().unwrap().0 != f64::INFINITY {
        return f64::NAN;
    }
    let mut prev = buckets[0].1;
    for p in buckets.iter_mut().skip(1) {
        if p.1 < prev || (p.1 - prev).abs() <= 1e-12 * (p.1.abs() + prev.abs()) {
            p.1 = prev;
        }
        prev = p.1;
    }
    let count = buckets.last().unwrap().1;
    if count == 0. {
        return f64::NAN;
    }
    let rank = q * count;
    let idx = buckets[..buckets.len() - 1].partition_point(|p| p.1 < rank);
    if idx == buckets.len() - 1 {
        return buckets[idx - 1].0;
    }
    if idx == 0 && buckets[0].0 <= 0. {
        return buckets[0].0;
    }
    let (start, base) = if idx == 0 { (0., 0.) } else { buckets[idx - 1] };
    let (end, upper) = buckets[idx];
    start + (end - start) * (rank - base) / (upper - base)
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
        let mut sum = asap_physical_operators::accumulators::sum_accumulator::SumAccumulator::new();
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
                        operator: ResidualQueryOperator::TopKSelection {
                            k: 2,
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
        let (selected, warning) = membership_filter(
            candidates,
            exact,
            &CandidateCompleteness::Certified {
                guarantee: topk_membership_guarantee(),
            },
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
                (
                    filter,
                    QueryPlanNode::MembershipFilter {
                        inputs: [candidate_id, value_id],
                        completeness: CandidateCompleteness::Certified {
                            guarantee: topk_membership_guarantee(),
                        },
                    },
                ),
                (
                    root,
                    QueryPlanNode::Logical {
                        operator: ResidualQueryOperator::TopKSelection {
                            k: 1,
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
        let (_, warning) = membership_filter(
            candidates.clone(),
            exact.clone(),
            &CandidateCompleteness::BestEffort { guarantee: None },
        )
        .unwrap();
        assert!(warning.unwrap().contains("approximate"));
        // Exact queries never lower an uncertified MembershipFilter. The Planner
        // emits its ordinary exact fallback instead; this runtime node is only
        // valid for certified or explicitly approximate plans.
        let certified = CandidateCompleteness::Certified {
            guarantee: topk_membership_guarantee(),
        };
        assert!(membership_filter(
            vec![(labels(&[("pod", "missing")]), 1.0)],
            exact,
            &certified,
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
