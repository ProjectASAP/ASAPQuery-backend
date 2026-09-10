//! Executes the installed typed logical DAG. No serving-time PromQL parsing.
use crate::query_engines::{
    query_result::{InstantVectorElement, QueryResult},
    EngineError,
};
use crate::storage_engines::types::KeyByLabelValues;
use control_plane::query_plan::logical::{
    Aggregation, BinaryOperation, Grouping, LogicalOperator, TemporalOperation,
};
use control_plane::query_plan::{
    CandidateCompleteness, QueryNodeId, QueryPlanEntry, QueryPlanNode,
};
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
    let mut evaluator = Evaluator {
        entry,
        leaves,
        callback,
        stats: ExecutionStats::default(),
        memo: BTreeMap::new(),
        active: BTreeSet::new(),
        warnings: Vec::new(),
    };
    let at_signed = i64::try_from(at).map_err(|_| miss("evaluation timestamp overflow"))?;
    let evaluated = evaluator.eval(entry.root, at_signed)?;
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

struct Evaluator<'a, F> {
    entry: &'a QueryPlanEntry,
    leaves: &'a PreparedLeaves,
    stats: ExecutionStats,
    callback: F,
    memo: BTreeMap<(QueryNodeId, i64), Value>,
    active: BTreeSet<(QueryNodeId, i64)>,
    warnings: Vec<String>,
}
impl<F: FnMut(QueryNodeId, u64) -> Result<QueryResult, EngineError>> Evaluator<'_, F> {
    fn eval(&mut self, id: QueryNodeId, at: i64) -> Result<Value, EngineError> {
        if let Some(value) = self.memo.get(&(id, at)) {
            self.stats.memo_hits += 1;
            return Ok(value.clone());
        }
        if let Some(leaf) = self.leaves.get(&(id, at)) {
            if leaf.remote {
                self.stats.remote_branch_evaluations += 1;
                self.stats.remote_evaluations += leaf.remote_evaluations;
                self.stats.remote_rpcs += leaf.remote_rpcs;
            } else {
                self.stats.summary_readout_evaluations += 1;
            }
            let value = leaf.value.clone();
            self.memo.insert((id, at), value.clone());
            return Ok(value);
        }
        if self.active.len() >= 256 || !self.active.insert((id, at)) {
            return Err(miss("cyclic or excessively deep installed DAG"));
        }
        if self.memo.len() >= 200_000 {
            return Err(miss("installed DAG evaluation budget exceeded"));
        }
        let node = self
            .entry
            .nodes
            .get(&id)
            .ok_or_else(|| miss("missing installed node"))?
            .clone();
        let value = match node {
            QueryPlanNode::Scalar { value } => Value::Scalar(value),
            QueryPlanNode::Logical { operator, inputs } => {
                if matches!(
                    operator,
                    LogicalOperator::Scan { .. }
                        | LogicalOperator::ExactSubquery { .. }
                        | LogicalOperator::CandidateExactSubquery { .. }
                ) {
                    return Err(miss(
                        "installed Prometheus leaf was not prepared; backend raw execution is forbidden",
                    ));
                }
                self.logical(operator, &inputs, at)?
            }
            QueryPlanNode::CandidateTopK {
                inputs,
                k,
                grouping,
                completeness,
            } => {
                let candidates = vector(self.eval(inputs[0], at)?)?;
                let values = vector(self.eval(inputs[1], at)?)?;
                let (selected, warning) =
                    candidate_topk(k, &grouping, candidates, values, &completeness)?;
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
        self.active.remove(&(id, at));
        self.memo.insert((id, at), value.clone());
        Ok(value)
    }
    fn logical(
        &mut self,
        operator: LogicalOperator,
        inputs: &[QueryNodeId],
        at: i64,
    ) -> Result<Value, EngineError> {
        let input = |index: usize| {
            inputs
                .get(index)
                .copied()
                .ok_or_else(|| miss("missing logical input"))
        };
        match operator {
            LogicalOperator::ExactSubquery { .. }
            | LogicalOperator::CandidateExactSubquery { .. } => {
                Err(miss("Prometheus exact leaf was not prepared"))
            }
            LogicalOperator::Scan { .. } => {
                Err(miss("local raw Scan is forbidden in deployed plans"))
            }
            LogicalOperator::UnaryNegate => match self.eval(input(0)?, at)? {
                Value::Scalar(value) => Ok(Value::Scalar(-value)),
                Value::Vector(values) => Ok(Value::Vector(
                    values
                        .into_iter()
                        .map(|(labels, value)| (labels, -value))
                        .collect(),
                )),
                _ => Err(miss("cannot negate range vector")),
            },
            LogicalOperator::VectorToScalar => {
                let values = vector(self.eval(input(0)?, at)?)?;
                Ok(Value::Scalar(if values.len() == 1 {
                    values[0].1
                } else {
                    f64::NAN
                }))
            }
            LogicalOperator::Aggregate {
                operation,
                grouping,
            } => {
                let values = vector(self.eval(input(0)?, at)?)?;
                Ok(Value::Vector(aggregate(operation, &grouping, values)))
            }
            LogicalOperator::TopKSelection { k, grouping } => {
                let values = vector(self.eval(input(0)?, at)?)?;
                Ok(Value::Vector(topk_selection(k, &grouping, values)))
            }
            LogicalOperator::Binary {
                operation,
                return_bool,
            } => {
                let left = self.eval(input(0)?, at)?;
                let right = self.eval(input(1)?, at)?;
                binary(operation, return_bool, left, right)
            }
            LogicalOperator::Temporal { operation } => {
                let Value::Matrix(values, start, end) = self.eval(input(0)?, at)? else {
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
                            value.map(|v| (no_name(labels), v))
                        })
                        .collect(),
                ))
            }
            LogicalOperator::Sort { descending } => {
                let mut values = vector(self.eval(input(0)?, at)?)?;
                values.sort_by(|a, b| {
                    if a.1.is_nan() && b.1.is_nan() {
                        std::cmp::Ordering::Equal
                    } else if a.1.is_nan() {
                        std::cmp::Ordering::Greater
                    } else if b.1.is_nan() {
                        std::cmp::Ordering::Less
                    } else if descending {
                        b.1.total_cmp(&a.1)
                    } else {
                        a.1.total_cmp(&b.1)
                    }
                });
                Ok(Value::Vector(values))
            }
            LogicalOperator::HistogramQuantile => {
                let Value::Scalar(quantile) = self.eval(input(0)?, at)? else {
                    return Err(miss("quantile requires scalar"));
                };
                let mut groups: BTreeMap<Labels, Vec<(f64, f64)>> = BTreeMap::new();
                for (mut labels, value) in vector(self.eval(input(1)?, at)?)? {
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
            LogicalOperator::Subquery {
                range_ms,
                step_ms,
                offset_ms,
            } => {
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
                let mut t = start
                    .div_euclid(step)
                    .checked_add(1)
                    .and_then(|n| n.checked_mul(step))
                    .ok_or_else(|| miss("subquery grid overflow"))?;
                let mut values: BTreeMap<Labels, Vec<(i64, f64)>> = BTreeMap::new();
                while t <= end {
                    for (labels, value) in vector(self.eval(input(0)?, t)?)? {
                        values.entry(labels).or_default().push((t, value));
                    }
                    t = t
                        .checked_add(step)
                        .ok_or_else(|| miss("subquery time overflow"))?;
                }
                Ok(Value::Matrix(values.into_iter().collect(), start, end))
            }
        }
    }
}

fn candidate_topk(
    k: u64,
    grouping: &Grouping,
    candidates: Vector,
    values: Vector,
    completeness: &CandidateCompleteness,
) -> Result<(Vector, Option<String>), EngineError> {
    let identity = |labels: &Labels| {
        let mut labels = labels.clone();
        labels.remove("__name__");
        labels
    };
    let candidate_ids: BTreeSet<_> = candidates
        .iter()
        .map(|(labels, _)| identity(labels))
        .collect();
    let value_ids: BTreeSet<_> = values.iter().map(|(labels, _)| identity(labels)).collect();
    let dangling = candidate_ids
        .iter()
        .any(|candidate| !value_ids.contains(candidate));
    if dangling && matches!(completeness, CandidateCompleteness::Certified { .. }) {
        return Err(miss("certified TopK candidate has no exact counter value"));
    }
    let matched = values
        .into_iter()
        .filter(|(labels, _)| candidate_ids.contains(&identity(labels)))
        .collect();
    let selected = topk_selection(k, grouping, matched);
    let warning = match completeness {
        CandidateCompleteness::Certified { .. } => None,
        CandidateCompleteness::BestEffort { guarantee } => Some(match guarantee {
            Some(guarantee) => format!(
                "ASAP TopK candidate membership is approximate: {:?}",
                guarantee.metric
            ),
            None => "ASAP TopK candidate membership is approximate and uncertified".into(),
        }),
    };
    Ok((selected, warning))
}

fn aggregate(operation: Aggregation, grouping: &Grouping, values: Vector) -> Vector {
    let mut groups: BTreeMap<Labels, Vec<f64>> = BTreeMap::new();
    for (labels, value) in values {
        let key = labels
            .into_iter()
            .filter(|(key, _)| {
                if grouping.without {
                    key != "__name__" && !grouping.labels.contains(key)
                } else {
                    grouping.labels.contains(key)
                }
            })
            .collect();
        groups.entry(key).or_default().push(value);
    }
    groups
        .into_iter()
        .map(|(labels, values)| {
            let value = match operation {
                Aggregation::Sum => values.iter().sum(),
                Aggregation::Avg => values.iter().sum::<f64>() / values.len() as f64,
                Aggregation::Count => values.len() as f64,
                Aggregation::Max => {
                    values
                        .into_iter()
                        .fold(f64::NAN, |a, b| if a.is_nan() || b > a { b } else { a })
                }
                Aggregation::Min => {
                    values
                        .into_iter()
                        .fold(f64::NAN, |a, b| if a.is_nan() || b < a { b } else { a })
                }
            };
            (labels, value)
        })
        .collect()
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
fn topk_selection(k: u64, grouping: &Grouping, values: Vector) -> Vector {
    if k == 0 {
        return Vec::new();
    }
    let mut groups: BTreeMap<Labels, Vector> = BTreeMap::new();
    for (labels, value) in values {
        groups
            .entry(grouping_key(&labels, grouping))
            .or_default()
            .push((labels, value));
    }
    let limit = usize::try_from(k).unwrap_or(usize::MAX);
    groups
        .into_values()
        .flat_map(|mut group| {
            group.sort_by(|a, b| match (a.1.is_nan(), b.1.is_nan()) {
                (true, true) => std::cmp::Ordering::Equal,
                (true, false) => std::cmp::Ordering::Greater,
                (false, true) => std::cmp::Ordering::Less,
                (false, false) => b.1.total_cmp(&a.1),
            });
            group.truncate(limit);
            group
        })
        .collect()
}

fn binary(
    operation: BinaryOperation,
    boolean: bool,
    left: Value,
    right: Value,
) -> Result<Value, EngineError> {
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
mod topk_tests {
    use super::*;
    use control_plane::query_plan::{FallbackPolicy, InstantExecution};

    fn labels(items: &[(&str, &str)]) -> Labels {
        items
            .iter()
            .map(|(key, value)| ((*key).into(), (*value).into()))
            .collect()
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
        );
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
        );
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
        let mut entry = QueryPlanEntry::compile_logical(
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
        control_plane::query_plan::logical::finalize_residuals(&mut entry).unwrap();
        let leaf = entry
            .nodes
            .iter()
            .find_map(|(id, node)| {
                matches!(
                    node,
                    QueryPlanNode::Logical {
                        operator: LogicalOperator::ExactSubquery { .. },
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

    #[test]
    fn installed_topk_ranks_exact_rate_summary_values() {
        let summary = QueryNodeId(0);
        let root = QueryNodeId(1);
        let entry = QueryPlanEntry {
            query_id: "summary-rate-topk".into(),
            canonical_promql: "topk(2, rate(requests_total[5m]))".into(),
            root,
            nodes: BTreeMap::from([
                (
                    summary,
                    QueryPlanNode::ExactReadout {
                        input: QueryNodeId(99),
                        readout: control_plane::query_plan::ExactReadout::Rate,
                    },
                ),
                (
                    root,
                    QueryPlanNode::Logical {
                        operator: LogicalOperator::TopKSelection {
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
        let (selected, warning) = candidate_topk(
            2,
            &Grouping {
                labels: vec![],
                without: false,
            },
            candidates,
            exact,
            &CandidateCompleteness::Certified {
                guarantee: topk_membership_guarantee(),
            },
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
        let root = QueryNodeId(2);
        let entry = QueryPlanEntry {
            query_id: "candidate-topk".into(),
            canonical_promql: "topk(1, rate(requests_total[5m]))".into(),
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
                    root,
                    QueryPlanNode::CandidateTopK {
                        inputs: [candidate_id, value_id],
                        k: 1,
                        grouping: Grouping {
                            labels: vec![],
                            without: false,
                        },
                        completeness: CandidateCompleteness::Certified {
                            guarantee: topk_membership_guarantee(),
                        },
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
                    value: Value::Vector(vec![(labels(&[("pod", "b")]), 100.0)]),
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
        assert_eq!(result.values[0].value, 1.0, "exact value is authoritative");
        assert_eq!(result.values[0].labels.labels, vec!["b"]);
        assert_eq!(stats.summary_readout_evaluations, 2);
        assert!(result.warnings.is_empty());
    }

    #[test]
    fn uncertified_candidate_sidecar_warns_or_falls_back_explicitly() {
        let candidates = vec![(labels(&[("pod", "a")]), 1.0)];
        let exact = vec![(labels(&[("pod", "a")]), 2.0)];
        let (_, warning) = candidate_topk(
            1,
            &Grouping {
                labels: vec![],
                without: false,
            },
            candidates.clone(),
            exact.clone(),
            &CandidateCompleteness::BestEffort { guarantee: None },
        )
        .unwrap();
        assert!(warning.unwrap().contains("approximate"));
        // Exact queries never lower an uncertified CandidateTopK. The Planner
        // emits its ordinary exact fallback instead; this runtime node is only
        // valid for certified or explicitly approximate plans.
        let certified = CandidateCompleteness::Certified {
            guarantee: topk_membership_guarantee(),
        };
        assert!(candidate_topk(
            1,
            &Grouping {
                labels: vec![],
                without: false
            },
            vec![(labels(&[("pod", "missing")]), 1.0)],
            exact,
            &certified,
        )
        .is_err());
    }
}
