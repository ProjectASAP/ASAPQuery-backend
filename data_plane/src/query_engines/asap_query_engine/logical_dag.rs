//! Executes the installed typed logical DAG. No serving-time PromQL parsing.
use crate::drivers::ingest::prometheus_remote_write::CanonicalSample;
use crate::query_engines::{
    query_result::{InstantVectorElement, QueryResult},
    EngineError,
};
use crate::storage_engines::types::KeyByLabelValues;
use control_plane::query_plan::logical::{
    Aggregation, BinaryOperation, Grouping, LabelMatch, LabelMatcher, LogicalOperator,
    TemporalOperation,
};
use control_plane::query_plan::{QueryNodeId, QueryPlanEntry, QueryPlanNode};
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
struct Point {
    timestamp_ms: i64,
    value: Option<f64>,
}
struct Series {
    max_index: std::sync::OnceLock<super::range_max_index::RangeMaxIndex>,
    labels: Labels,
    points: Vec<Point>,
}
/// One immutable indexed raw-data snapshot; callers invalidate it after ingestion.
pub struct PreparedSamples {
    series: Vec<Series>,
}
#[derive(Debug, Default, Clone)]
pub struct ExecutionStats {
    pub raw_scan_evaluations: usize,
    pub summary_readout_evaluations: usize,
    pub memo_hits: usize,
    pub remote_evaluations: usize,
    pub remote_rpcs: usize,
    pub remote_branch_evaluations: usize,
    pub index_reads: usize,
}
impl PreparedSamples {
    pub fn new(samples: &[CanonicalSample]) -> Result<Self, EngineError> {
        let mut grouped: BTreeMap<Labels, Vec<Point>> = BTreeMap::new();
        for sample in samples {
            let mut labels: Labels = sample.labels.clone().into_iter().collect();
            labels.insert("__name__".into(), sample.metric.clone());
            grouped.entry(labels).or_default().push(Point {
                timestamp_ms: sample.timestamp_ms,
                value: sample.value,
            });
        }
        Self::from_grouped(grouped)
    }

    /// Build directly from grouped storage without duplicating labels per sample.
    /// Label maps include the `__name__` metric label.
    pub fn from_series(
        input: impl IntoIterator<Item = (BTreeMap<String, String>, Vec<(i64, Option<f64>)>)>,
    ) -> Result<Self, EngineError> {
        let mut grouped: BTreeMap<Labels, Vec<Point>> = BTreeMap::new();
        for (labels, points) in input {
            grouped
                .entry(labels)
                .or_default()
                .extend(points.into_iter().map(|(timestamp_ms, value)| Point {
                    timestamp_ms,
                    value,
                }));
        }
        Self::from_grouped(grouped)
    }

    fn from_grouped(grouped: BTreeMap<Labels, Vec<Point>>) -> Result<Self, EngineError> {
        let mut series = Vec::with_capacity(grouped.len());
        for (labels, mut points) in grouped {
            points.sort_by_key(|point| point.timestamp_ms);
            for pair in points.windows(2) {
                if pair[0].timestamp_ms == pair[1].timestamp_ms
                    && pair[0].value.map(f64::to_bits) != pair[1].value.map(f64::to_bits)
                {
                    return Err(miss(
                        "conflicting raw samples for one label set and timestamp",
                    ));
                }
            }
            points.dedup_by_key(|point| point.timestamp_ms);
            series.push(Series {
                labels,
                points,
                max_index: Default::default(),
            });
        }
        Ok(Self { series })
    }
    pub fn prepare_range_max_indexes(&self, metrics: &BTreeSet<String>) {
        for series in &self.series {
            if series
                .labels
                .get("__name__")
                .is_some_and(|metric| metrics.contains(metric))
            {
                series.max_index.get_or_init(|| {
                    super::range_max_index::RangeMaxIndex::new(
                        series.points.iter().map(|point| point.value),
                    )
                });
            }
        }
    }
    pub fn range_max_index_bytes(&self) -> usize {
        self.series
            .iter()
            .filter_map(|series| series.max_index.get())
            .map(|index| index.estimated_bytes())
            .sum()
    }
    pub fn estimated_bytes(&self) -> usize {
        self.series
            .iter()
            .map(|series| {
                std::mem::size_of::<Series>()
                    + series
                        .max_index
                        .get()
                        .map_or(0, |index| index.estimated_bytes())
                    + series.points.capacity() * std::mem::size_of::<Point>()
                    + series
                        .labels
                        .iter()
                        .map(|(key, value)| key.capacity() + value.capacity())
                        .sum::<usize>()
            })
            .sum()
    }
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

pub fn execute(
    entry: &QueryPlanEntry,
    samples: &[CanonicalSample],
    at: u64,
) -> Result<QueryResult, EngineError> {
    execute_with_summary(entry, samples, at, |_, _| {
        Err(miss("summary callback required"))
    })
}

pub fn execute_with_summary<F>(
    entry: &QueryPlanEntry,
    samples: &[CanonicalSample],
    at: u64,
    callback: F,
) -> Result<QueryResult, EngineError>
where
    F: FnMut(QueryNodeId, u64) -> Result<QueryResult, EngineError>,
{
    execute_prepared(entry, &PreparedSamples::new(samples)?, at, callback)
}

pub fn execute_prepared<F>(
    entry: &QueryPlanEntry,
    samples: &PreparedSamples,
    at: u64,
    callback: F,
) -> Result<QueryResult, EngineError>
where
    F: FnMut(QueryNodeId, u64) -> Result<QueryResult, EngineError>,
{
    execute_prepared_with_stats(entry, samples, at, callback).map(|(result, _)| result)
}

pub fn execute_prepared_with_stats<F>(
    entry: &QueryPlanEntry,
    samples: &PreparedSamples,
    at: u64,
    callback: F,
) -> Result<(QueryResult, ExecutionStats), EngineError>
where
    F: FnMut(QueryNodeId, u64) -> Result<QueryResult, EngineError>,
{
    execute_values(entry, &samples.series, None, at, callback)
}

pub(crate) struct PreparedLeaf {
    pub value: Value,
    pub remote: bool,
    pub remote_evaluations: usize,
    pub remote_rpcs: usize,
    pub index_reads: usize,
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
    execute_values(entry, &[], Some(leaves), at, callback)
}

fn execute_values<F>(
    entry: &QueryPlanEntry,
    series: &[Series],
    leaves: Option<&PreparedLeaves>,
    at: u64,
    callback: F,
) -> Result<(QueryResult, ExecutionStats), EngineError>
where
    F: FnMut(QueryNodeId, u64) -> Result<QueryResult, EngineError>,
{
    let mut evaluator = Evaluator {
        entry,
        series,
        leaves,
        callback,
        stats: ExecutionStats::default(),
        memo: BTreeMap::new(),
        active: BTreeSet::new(),
        regexes: BTreeMap::new(),
    };
    let at_signed = i64::try_from(at).map_err(|_| miss("evaluation timestamp overflow"))?;
    let evaluated = evaluator.eval(entry.root, at_signed)?;
    if leaves.is_some() && matches!(evaluated, Value::Scalar(_)) {
        // QueryResult currently models vectors/matrices only. Preserve a scalar
        // root's HTTP type by routing it to native, while scalar intermediates
        // remain typed inside vector composition.
        return Err(miss("scalar root requires native response adapter"));
    }
    let result = vector(evaluated)?;
    Ok((
        QueryResult::vector(
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
        ),
        evaluator.stats,
    ))
}

struct Evaluator<'a, F> {
    entry: &'a QueryPlanEntry,
    series: &'a [Series],
    leaves: Option<&'a PreparedLeaves>,
    stats: ExecutionStats,
    callback: F,
    memo: BTreeMap<(QueryNodeId, i64), Value>,
    active: BTreeSet<(QueryNodeId, i64)>,
    regexes: BTreeMap<String, regex::Regex>,
}
impl<F: FnMut(QueryNodeId, u64) -> Result<QueryResult, EngineError>> Evaluator<'_, F> {
    fn eval(&mut self, id: QueryNodeId, at: i64) -> Result<Value, EngineError> {
        if let Some(value) = self.memo.get(&(id, at)) {
            self.stats.memo_hits += 1;
            return Ok(value.clone());
        }
        if let Some(leaf) = self.leaves.and_then(|leaves| leaves.get(&(id, at))) {
            if leaf.remote {
                self.stats.remote_branch_evaluations += 1;
                self.stats.remote_evaluations += leaf.remote_evaluations;
                self.stats.remote_rpcs += leaf.remote_rpcs;
            } else {
                self.stats.summary_readout_evaluations += 1;
            }
            self.stats.index_reads += leaf.index_reads;
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
                if self.leaves.is_some()
                    && matches!(
                        operator,
                        LogicalOperator::Scan { .. }
                            | LogicalOperator::ReadRangeMaxIndex { .. }
                            | LogicalOperator::ReadRangeCounterIndex { .. }
                            | LogicalOperator::ExactSubquery { .. }
                    )
                {
                    return Err(miss(
                        "installed leaf was not prepared; local raw execution is forbidden",
                    ));
                }
                self.logical(operator, &inputs, at)?
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
            | LogicalOperator::ReadRangeCounterIndex { .. } => Err(miss(
                "remote/index leaf requires prepared installed execution",
            )),
            LogicalOperator::ReadRangeMaxIndex {
                metric,
                matchers,
                range_ms,
                ..
            } => self.range_max_index(&metric, &matchers, range_ms, at),
            LogicalOperator::Scan {
                metric,
                matchers,
                range_ms,
                offset_ms,
            } => self.scan(metric.as_deref(), &matchers, range_ms, offset_ms, at),
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
            LogicalOperator::Aggregate {
                operation,
                grouping,
            } => {
                let values = vector(self.eval(input(0)?, at)?)?;
                Ok(Value::Vector(aggregate(operation, &grouping, values)))
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
    fn range_max_index(
        &mut self,
        metric: &str,
        matchers: &[LabelMatcher],
        range_ms: u64,
        at: i64,
    ) -> Result<Value, EngineError> {
        for matcher in matchers {
            if matches!(matcher.operation, LabelMatch::Regex | LabelMatch::NotRegex)
                && !self.regexes.contains_key(&matcher.value)
            {
                let regex = regex::Regex::new(&format!("(?s)^(?:{})$", matcher.value))
                    .map_err(|e| miss(format!("unsupported regex: {e}")))?;
                self.regexes.insert(matcher.value.clone(), regex);
            }
        }
        let start = at
            .checked_sub(i64::try_from(range_ms).map_err(|_| miss("range overflow"))?)
            .ok_or_else(|| miss("range overflow"))?;
        let mut values = Vec::new();
        for series in self.series {
            if series.labels.get("__name__").map(String::as_str) != Some(metric)
                || !matchers.iter().all(|matcher| {
                    let value = series
                        .labels
                        .get(&matcher.name)
                        .map(String::as_str)
                        .unwrap_or("");
                    match matcher.operation {
                        LabelMatch::Equal => value == matcher.value,
                        LabelMatch::NotEqual => value != matcher.value,
                        LabelMatch::Regex => self.regexes[&matcher.value].is_match(value),
                        LabelMatch::NotRegex => !self.regexes[&matcher.value].is_match(value),
                    }
                })
            {
                continue;
            }
            let lo = series.points.partition_point(|p| p.timestamp_ms <= start);
            let hi = series.points.partition_point(|p| p.timestamp_ms <= at);
            let index = series.max_index.get_or_init(|| {
                super::range_max_index::RangeMaxIndex::new(series.points.iter().map(|p| p.value))
            });
            // Count only real index access, never a generic scan or successful binding.
            self.stats.summary_readout_evaluations += 1;
            if let Some(value) = index.query(lo, hi) {
                values.push((no_name(series.labels.clone()), value));
            }
        }
        Ok(Value::Vector(values))
    }
    fn scan(
        &mut self,
        metric: Option<&str>,
        matchers: &[LabelMatcher],
        range_ms: Option<u64>,
        offset_ms: i64,
        at: i64,
    ) -> Result<Value, EngineError> {
        self.stats.raw_scan_evaluations += 1;
        for matcher in matchers {
            if matches!(matcher.operation, LabelMatch::Regex | LabelMatch::NotRegex)
                && !self.regexes.contains_key(&matcher.value)
            {
                let pattern = regex::Regex::new(&format!("(?s)^(?:{})$", matcher.value))
                    .map_err(|e| miss(format!("unsupported regex: {e}")))?;
                self.regexes.insert(matcher.value.clone(), pattern);
            }
        }
        let end = at
            .checked_sub(offset_ms)
            .ok_or_else(|| miss("offset overflow"))?;
        let range =
            i64::try_from(range_ms.unwrap_or(300_000)).map_err(|_| miss("range overflow"))?;
        let start = end
            .checked_sub(range)
            .ok_or_else(|| miss("range overflow"))?;
        let mut instant = Vec::new();
        let mut matrix = Vec::new();
        for series in self.series {
            if metric.is_some_and(|m| series.labels.get("__name__").map(String::as_str) != Some(m))
                || !matchers.iter().all(|matcher| {
                    let value = series
                        .labels
                        .get(&matcher.name)
                        .map(String::as_str)
                        .unwrap_or("");
                    match matcher.operation {
                        LabelMatch::Equal => value == matcher.value,
                        LabelMatch::NotEqual => value != matcher.value,
                        LabelMatch::Regex => self.regexes[&matcher.value].is_match(value),
                        LabelMatch::NotRegex => !self.regexes[&matcher.value].is_match(value),
                    }
                })
            {
                continue;
            }
            let hi = series
                .points
                .partition_point(|point| point.timestamp_ms <= end);
            if range_ms.is_some() {
                let lo = series
                    .points
                    .partition_point(|point| point.timestamp_ms <= start);
                let points: Vec<_> = series.points[lo..hi]
                    .iter()
                    .filter_map(|point| {
                        point
                            .value
                            .filter(|v| v.to_bits() != 0x7ff0000000000002)
                            .map(|v| (point.timestamp_ms, v))
                    })
                    .collect();
                if !points.is_empty() {
                    matrix.push((series.labels.clone(), points));
                }
            } else if hi > 0 {
                let point = &series.points[hi - 1];
                if point.timestamp_ms >= start {
                    if let Some(value) = point.value.filter(|v| v.to_bits() != 0x7ff0000000000002) {
                        instant.push((series.labels.clone(), value));
                    }
                }
            }
        }
        Ok(if range_ms.is_some() {
            Value::Matrix(matrix, start, end)
        } else {
            Value::Vector(instant)
        })
    }
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
mod tests {
    use super::*;
    use control_plane::query_plan::{FallbackPolicy, InstantExecution};
    fn entry(query: &str) -> QueryPlanEntry {
        QueryPlanEntry::compile_logical(
            "q".into(),
            query.into(),
            InstantExecution {
                lookback_ms: 300_000,
                full_history: false,
                cumulative_readout: false,
            },
            FallbackPolicy::Reject,
        )
        .unwrap()
    }
    fn sample(metric: &str, job: &str, timestamp_ms: i64, value: Option<f64>) -> CanonicalSample {
        CanonicalSample {
            metric: metric.into(),
            labels: [("job".into(), job.into())].into(),
            series_key: format!("{metric}:{job}"),
            timestamp_ms,
            value,
        }
    }
    fn values(query: &str, samples: &[CanonicalSample], at: u64) -> Vec<f64> {
        let QueryResult::Vector(result) = execute(&entry(query), samples, at).unwrap() else {
            panic!()
        };
        result.values.into_iter().map(|p| p.value).collect()
    }
    #[test]
    fn indexed_max_preserves_filters_labels_boundaries_and_real_readout_provenance() {
        let mut samples = vec![
            sample(
                "service_retry_queue_depth",
                "user-service",
                1_000,
                Some(999.),
            ),
            sample("service_retry_queue_depth", "user-service", 1_001, Some(4.)),
            sample("service_retry_queue_depth", "user-service", 2_056, Some(8.)),
            sample(
                "service_retry_queue_depth",
                "order-service",
                2_000,
                Some(30.),
            ),
        ];
        let mut extra = sample(
            "service_retry_queue_depth",
            "user-service",
            1_500,
            Some(11.),
        );
        extra.labels.insert("instance".into(), "other".into());
        samples.push(extra);
        let prepared = PreparedSamples::new(&samples).unwrap();
        assert_eq!(prepared.range_max_index_bytes(), 0);
        for selector in [
            r#"job="user-service""#,
            r#"job=~".+""#,
            r#"job!="order-service",absent="""#,
            r#"job!~"order.*""#,
        ] {
            let query = format!("max_over_time(service_retry_queue_depth{{{selector}}}[1056ms])");
            let raw = entry(&query);
            let mut indexed = raw.clone();
            let scan = raw
                .nodes
                .values()
                .find_map(|node| match node {
                    QueryPlanNode::Logical {
                        operator:
                            LogicalOperator::Scan {
                                metric: Some(metric),
                                matchers,
                                range_ms: Some(range_ms),
                                ..
                            },
                        ..
                    } => Some(LogicalOperator::ReadRangeMaxIndex {
                        metric: metric.clone(),
                        matchers: matchers.clone(),
                        range_ms: *range_ms,
                        retention_ms: *range_ms,
                    }),
                    _ => None,
                })
                .unwrap();
            indexed.nodes = [(
                indexed.root,
                QueryPlanNode::Logical {
                    operator: scan,
                    inputs: vec![],
                },
            )]
            .into();
            for at in [2056, 2057, 2500, 4000] {
                let (expected, _) =
                    execute_prepared_with_stats(&raw, &prepared, at, |_, _| unreachable!())
                        .unwrap();
                let (actual, stats) =
                    execute_prepared_with_stats(&indexed, &prepared, at, |_, _| unreachable!())
                        .unwrap();
                assert_eq!(
                    serde_json::to_value(actual).unwrap(),
                    serde_json::to_value(expected).unwrap(),
                    "{query} at {at}"
                );
                assert_eq!(stats.raw_scan_evaluations, 0);
                assert!(stats.summary_readout_evaluations > 0);
            }
        }
        let bytes = prepared.range_max_index_bytes();
        assert!(bytes > 0 && bytes < prepared.estimated_bytes());
    }

    // Regex alternation remains fully anchored and missing labels compare as empty.
    #[test]
    fn anchored_matchers_and_missing_labels() {
        let samples = [
            sample("up", "xorder", 1000, Some(9.)),
            sample("up", "user", 1000, Some(2.)),
        ];
        assert_eq!(
            values(r#"sum(up{job=~"user|order",absent=""})"#, &samples, 1000),
            vec![2.]
        );
    }
    // Stale markers stop instant lookup; range windows are left-open and ignore stale points.
    #[test]
    fn stale_and_range_boundaries() {
        let samples = [
            sample("up", "a", 0, Some(99.)),
            sample("up", "a", 1000, Some(2.)),
            sample("up", "a", 2000, None),
        ];
        assert!(values("up", &samples, 2000).is_empty());
        assert_eq!(values("sum_over_time(up[2s])", &samples, 2000), vec![2.]);
    }
    // A shared child must be evaluated separately at each subquery grid time.
    #[test]
    fn shared_subquery_uses_time_in_memo_key() {
        let samples = [
            sample("up", "a", 1000, Some(2.)),
            sample("up", "a", 2000, Some(4.)),
        ];
        assert_eq!(
            values("avg_over_time((up + up)[2s:1s])", &samples, 2000),
            vec![6.]
        );
    }
    // Scalar-left filtering returns the vector's original value and metric name.
    #[test]
    fn scalar_left_comparison_preserves_vector_value() {
        let samples = [sample("up", "a", 1000, Some(4.))];
        assert_eq!(values("2 < up", &samples, 1000), vec![4.]);
        assert_eq!(values("2 < bool up", &samples, 1000), vec![1.]);
    }
    // Materialized readouts can feed residual operators without parsing their query text.
    #[test]
    fn summary_callback_is_memoized_and_composed() {
        let mut graph = entry("up + up");
        let leaf = graph
            .nodes
            .iter()
            .find_map(|(id, node)| {
                matches!(
                    node,
                    QueryPlanNode::Logical {
                        operator: LogicalOperator::Scan { .. },
                        ..
                    }
                )
                .then_some(*id)
            })
            .unwrap();
        graph
            .nodes
            .insert(leaf, QueryPlanNode::SummaryMerge { inputs: vec![] });
        let mut calls = 0;
        let result = execute_with_summary(&graph, &[], 1000, |id, at| {
            assert_eq!(id, leaf);
            assert_eq!(at, 1000);
            calls += 1;
            Ok(QueryResult::vector(
                vec![InstantVectorElement::new(
                    KeyByLabelValues::new_with_labels(vec!["a".into()]),
                    3.,
                )
                .with_label_keys_override(vec!["job".into()])],
                at,
            ))
        })
        .unwrap();
        let QueryResult::Vector(result) = result else {
            panic!()
        };
        assert_eq!(result.values[0].value, 6.);
        assert_eq!(calls, 1);
    }
    // Reusing the immutable index keeps raw provenance and shares the same-time leaf.
    #[test]
    fn prepared_snapshot_preserves_route_counts() {
        let data = PreparedSamples::new(&[sample("up", "a", 1000, Some(2.))]).unwrap();
        let (_, stats) = execute_prepared_with_stats(&entry("up + up"), &data, 1000, |_, _| {
            Err(miss("unexpected summary"))
        })
        .unwrap();
        assert_eq!(stats.raw_scan_evaluations, 1);
        assert_eq!(stats.summary_readout_evaluations, 0);
        assert_eq!(stats.memo_hits, 1);
        assert!(data.estimated_bytes() > 0);
    }
    // Conflicting values cannot be hidden by different transport series-key strings.
    #[test]
    fn duplicate_identity_rejected() {
        let first = sample("up", "a", 1000, Some(1.));
        let mut second = sample("up", "a", 1000, Some(2.));
        second.series_key = "different".into();
        assert!(execute(&entry("up"), &[first, second], 1000).is_err());
    }
}
