//! `SummaryNode` lowering + execution + conversion into `ASAPTierResult`'s
//! `(series, coverage)` shape — the core `live_serve.rs` (the serving
//! cutover) calls into.

use std::collections::BTreeMap;

use crate::query_engines::asap_query_engine::summary_exec::{execute, ExecOutcome};
use asap_types::query_plan::{QueryNodeId, QueryPlanNode};
use control_plane::types_v2::AccuracyTarget;

use crate::query_engines::asap_query_engine::physical_dag::{self, QueryNodeRuntime};
use crate::query_engines::asap_query_engine::post_asap_planner::{
    execution_hints, plan_promql_to_post_asap, LoweringSkip,
};
use crate::query_engines::asap_query_engine::summary_executor::{
    GroupState, QueryExecutionContext, SummaryExecutorError, SummaryValue,
};
use crate::storage_engines::sketch_db::index::SketchStore;

/// Mirrors `ASAPTierResult.series`'s row shape — `(label_values, samples)`
/// where `samples` is `(window_end_unix_ms, value)`.
pub type SeriesRows = Vec<(BTreeMap<String, String>, Vec<(i64, f64)>)>;

/// The result of lowering + executing a query through
/// `SummaryExecutor`, converted into the same shape `ASAPTierResult`
/// uses, regardless of whether the answer came from the sketch
/// (`ExecOutcome::Value`) or `ExactAgg` (`ExecOutcome::State`) side —
/// callers that only care about "did this answer the query, and is it
/// safe to trust" don't need to know which.
/// NOTE — this used to carry an `ambiguous_merge_risk` flag, and
/// `live_serve.rs` used it to DECLINE to serve an ambiguous shape.
/// That gate is now removed, because the ambiguity it guarded against no
/// longer exists.
///
/// It existed because an empty `by: Vec<ColumnId>` was indistinguishable
/// between "no grouping concept applies" (the group split is correct) and
/// "an aggregation operator asked to reduce everything" (the groups
/// should have been merged) — exactly
/// [ASAPController#163](https://github.com/ProjectASAP/ASAPController/issues/163).
/// Unable to tell which, the safe move was to fall back to the legacy
/// path whenever an empty `by` produced >1 group.
///
/// ASAPController#165 removed that ambiguity at the source by making the
/// reduction kind explicit (`Reduction::{PerEntity, Reduce(GroupKeys)}`),
/// and `summary_executor.rs::resolve_group_key` now acts on it directly.
/// Both branches are resolved correctly BEFORE reaching here:
///
/// * `PerEntity` — the multi-group split is definitionally right (one row
///   per entity, never merged), so it was never a "risk" to begin with.
/// * `Reduce([])` — every candidate shares one group key, so the outcome
///   has exactly ONE group and the old `values.len() > 1` trigger cannot
///   fire at all.
///
/// The flag would therefore be unconditionally `false` today; keeping it
/// would mean keeping a heuristic that can only ever misfire (declining
/// correct `PerEntity` answers) now that the real signal is available.
pub struct PostAsapReadoutOutcome {
    pub series: SeriesRows,
    pub coverage: Option<(u64, u64)>,
}

/// Ask ASAPPlanner for the post-ASAP representation of `query`, execute it
/// against `index` over `[t0_ms, t1_ms]`, and
/// convert the result into `PostAsapReadoutOutcome`. `Err` covers every reason
/// this couldn't produce a trustworthy answer — see `LoweringSkip`'s
/// variants; every one of them means "fall back to the legacy path,"
/// never "the legacy path is wrong."
pub fn execute_post_asap_readout(
    index: &SketchStore,
    query: &str,
    t0_ms: u64,
    t1_ms: u64,
    is_cumulative: bool,
    accuracy: AccuracyTarget,
) -> Result<PostAsapReadoutOutcome, LoweringSkip> {
    let node = plan_promql_to_post_asap(index, query, accuracy.clone())?;
    execute_planned_post_asap(index, &node, query, accuracy, t0_ms, t1_ms, is_cumulative)
}

/// Execute an already-bound QueryPlan entry.  This is the production serving
/// path: no PromQL lowering, planner cost model, observed-family lookup, or
/// Installed QueryPlan materialization resolution occurs before this legacy test helper.
pub fn execute_query_plan_readout(
    index: &SketchStore,
    entry: &asap_types::query_plan::QueryPlanEntry,
    t0_ms: u64,
    t1_ms: u64,
    is_cumulative: bool,
) -> Result<PostAsapReadoutOutcome, LoweringSkip> {
    execute_physical_query_payload(index, entry, entry.root, t0_ms, t1_ms, is_cumulative)
}

pub fn execute_query_plan_from_readout(
    index: &SketchStore,
    entry: &asap_types::query_plan::QueryPlanEntry,
    root: asap_types::query_plan::QueryNodeId,
    t0_ms: u64,
    t1_ms: u64,
    is_cumulative: bool,
) -> Result<PostAsapReadoutOutcome, LoweringSkip> {
    execute_physical_query_payload(index, entry, root, t0_ms, t1_ms, is_cumulative)
}

pub fn execute_query_plan_instant(
    index: &SketchStore,
    entry: &asap_types::query_plan::QueryPlanEntry,
    now_ms: u64,
) -> Result<(PostAsapReadoutOutcome, u64), LoweringSkip> {
    let t0_ms = if entry.instant.full_history {
        0
    } else {
        now_ms.saturating_sub(entry.instant.lookback_ms)
    };
    if entry
        .materialization_bindings()
        .iter()
        .any(|binding| binding.window_ms == 0)
    {
        return Err(LoweringSkip::MaterializationNotReady(
            "materialized pane width is zero".into(),
        ));
    }
    let outcome = execute_physical_query_plan(
        index,
        entry,
        t0_ms,
        now_ms,
        entry.instant.cumulative_readout,
    )?;
    Ok((outcome, t0_ms))
}

#[derive(Clone)]
enum PhysicalQueryOutput {
    Scalar(f64),
    State {
        groups: Vec<(BTreeMap<String, String>, GroupState)>,
        item_labels: Vec<String>,
    },
    Value(
        Vec<(BTreeMap<String, String>, SummaryValue)>,
        Option<(u64, u64)>,
    ),
}

#[derive(Debug, thiserror::Error)]
enum PhysicalNodeError {
    #[error("materialization/store operation failed: {0:?}")]
    Store(SummaryExecutorError),
    #[error("node expected summary state input")]
    ExpectedState,
    #[error("physical fallback requested: {0}")]
    Fallback(String),
}

struct PhysicalQueryRuntime<'a> {
    language: control_plane::query_plan::QueryLanguage,
    catalog: Option<std::sync::Arc<asap_types::summary_catalog::SummaryCatalog>>,
    context: QueryExecutionContext<'a>,
}

impl QueryNodeRuntime for PhysicalQueryRuntime<'_> {
    type Output = PhysicalQueryOutput;
    type Error = PhysicalNodeError;

    fn execute_node(
        &self,
        _id: QueryNodeId,
        node: &QueryPlanNode,
        inputs: &[Self::Output],
    ) -> Result<Self::Output, Self::Error> {
        match node {
            QueryPlanNode::Scalar { value } => Ok(PhysicalQueryOutput::Scalar(*value)),
            QueryPlanNode::Binary { operator, .. } => {
                let [lhs, rhs] = inputs else {
                    return Err(PhysicalNodeError::ExpectedState);
                };
                binary_values(operator, lhs, rhs)
            }
            QueryPlanNode::ReduceSum { grouping, .. } => {
                let [PhysicalQueryOutput::Value(values, coverage)] = inputs else {
                    return Err(PhysicalNodeError::ExpectedState);
                };
                reduce_sum_values(grouping, values, *coverage)
            }
            QueryPlanNode::ReadMaterialization { binding } => {
                let mut groups = self
                    .context
                    .read_bound_materialization(binding)
                    .map_err(PhysicalNodeError::Store)?;
                // Metric identity belongs to the shared DataDescriptor, not to
                // the population labels or a reconstructed query string.
                if self.language == control_plane::query_plan::QueryLanguage::MetricsQl
                    && binding.output_grouping
                        == control_plane::query_plan::PhysicalGrouping::PerEntity
                {
                    let metric = self
                        .catalog
                        .as_ref()
                        .and_then(|catalog| {
                            let definition =
                                catalog.materializations.get(&binding.materialization)?;
                            catalog
                                .data_descriptors
                                .get(&definition.data_descriptor_id)?
                                .time_series_metric()
                        })
                        .ok_or_else(|| {
                            PhysicalNodeError::Fallback(
                                "MetricsQL per-series readout requires catalog metric identity"
                                    .into(),
                            )
                        })?;
                    for (labels, _) in &mut groups {
                        labels.insert("__name__".into(), metric.into());
                    }
                }
                Ok(PhysicalQueryOutput::State {
                    groups,
                    item_labels: binding.item_labels.clone(),
                })
            }
            QueryPlanNode::SummaryEstimate { query, .. } => {
                let [PhysicalQueryOutput::State {
                    groups,
                    item_labels,
                }] = inputs
                else {
                    return Err(PhysicalNodeError::ExpectedState);
                };
                let query: planner_types::post_asap::SketchQuery = query.clone().into();
                let mut values = Vec::new();
                let mut coverage = None;
                for (key, state) in groups {
                    let value = self
                        .context
                        .readout_bound(state, &query)
                        .map_err(PhysicalNodeError::Store)?;
                    let (mut rows, row_coverage) = expand_item_readout(key, value, item_labels)?;
                    if self.language == control_plane::query_plan::QueryLanguage::MetricsQl
                        && !matches!(
                            query,
                            planner_types::post_asap::SketchQuery::Quantile { .. }
                        )
                    {
                        for (labels, _) in &mut rows {
                            labels.remove("__name__");
                        }
                    }
                    fold_coverage(&mut coverage, row_coverage);
                    values.extend(rows);
                }
                Ok(PhysicalQueryOutput::Value(values, coverage))
            }
            QueryPlanNode::ExactReadout { readout, .. } => {
                if self.language == control_plane::query_plan::QueryLanguage::MetricsQl
                    && matches!(
                        readout,
                        control_plane::query_plan::ExactReadout::Rate
                            | control_plane::query_plan::ExactReadout::Increase
                    )
                {
                    return Err(PhysicalNodeError::Fallback(
                        "native MetricsQL counter semantics require external exact execution"
                            .into(),
                    ));
                }
                let [PhysicalQueryOutput::State { groups, .. }] = inputs else {
                    return Err(PhysicalNodeError::ExpectedState);
                };
                let mut coverage = None;
                let values = groups
                    .iter()
                    .map(|(key, state)| {
                        fold_coverage(&mut coverage, state.exact_coverage());
                        state
                            .exact_value_for(
                                *readout,
                                &None,
                                self.context.t0_ms,
                                self.context.t1_ms,
                            )
                            .map(|value| {
                                (
                                    {
                                        let mut labels = key.clone();
                                        if self.language
                                            == control_plane::query_plan::QueryLanguage::MetricsQl
                                            && *readout
                                                != control_plane::query_plan::ExactReadout::Max
                                        {
                                            labels.remove("__name__");
                                        }
                                        labels
                                    },
                                    SummaryValue::Points(
                                        vec![(self.context.t1_ms as i64, value)],
                                        state.exact_coverage(),
                                    ),
                                )
                            })
                            .ok_or(PhysicalNodeError::ExpectedState)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(PhysicalQueryOutput::Value(values, coverage))
            }
            QueryPlanNode::SummaryMerge { .. } => {
                let mut by_group: BTreeMap<BTreeMap<String, String>, Vec<GroupState>> =
                    BTreeMap::new();
                let mut merged_item_labels: Option<Vec<String>> = None;
                for input in inputs {
                    let PhysicalQueryOutput::State {
                        groups,
                        item_labels,
                    } = input
                    else {
                        return Err(PhysicalNodeError::ExpectedState);
                    };
                    if merged_item_labels
                        .as_ref()
                        .is_some_and(|labels| labels != item_labels)
                    {
                        return Err(PhysicalNodeError::Fallback(
                            "merged summaries disagree on item labels".into(),
                        ));
                    }
                    merged_item_labels.get_or_insert_with(|| item_labels.clone());
                    for (key, state) in groups {
                        by_group.entry(key.clone()).or_default().push(state.clone());
                    }
                }
                if by_group.is_empty() {
                    return Err(PhysicalNodeError::ExpectedState);
                }
                by_group
                    .into_iter()
                    .map(|(key, states)| {
                        self.context
                            .merge_bound_states(states)
                            .map(|state| (key, state))
                            .map_err(PhysicalNodeError::Store)
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map(|groups| PhysicalQueryOutput::State {
                        groups,
                        item_labels: merged_item_labels.unwrap_or_default(),
                    })
            }
            QueryPlanNode::Logical { .. }
            | QueryPlanNode::CandidateTopK { .. }
            | QueryPlanNode::Relational { .. }
            | QueryPlanNode::ExternalExact { .. }
            | QueryPlanNode::RelationalJoin { .. } => Err(PhysicalNodeError::Fallback(
                "logical node requires installed logical runtime".into(),
            )),
            QueryPlanNode::ExactFallback { reason } => {
                Err(PhysicalNodeError::Fallback(reason.clone()))
            }
        }
    }
}

fn expand_item_rows(
    group_key: &BTreeMap<String, String>,
    value: SummaryValue,
    item_labels: &[String],
) -> Result<Vec<(BTreeMap<String, String>, SummaryValue)>, PhysicalNodeError> {
    let SummaryValue::TopK(ranked_per_ts, coverage) = value else {
        return Ok(vec![(group_key.clone(), value)]);
    };
    let [item_label] = item_labels else {
        if item_labels.is_empty() {
            return Ok(vec![(
                group_key.clone(),
                SummaryValue::TopK(ranked_per_ts, coverage),
            )]);
        }
        return Err(PhysicalNodeError::Fallback(
            "multi-label keyed sketch readout is unsupported".into(),
        ));
    };
    let mut by_item: BTreeMap<String, Vec<(i64, f64)>> = BTreeMap::new();
    for (timestamp, items) in ranked_per_ts {
        for (item, value) in items {
            by_item.entry(item).or_default().push((timestamp, value));
        }
    }
    Ok(by_item
        .into_iter()
        .map(|(item, points)| {
            let mut labels = group_key.clone();
            labels.insert(item_label.clone(), item);
            (labels, SummaryValue::Points(points, coverage))
        })
        .collect())
}

fn expand_item_readout(
    group_key: &BTreeMap<String, String>,
    value: SummaryValue,
    item_labels: &[String],
) -> Result<
    (
        Vec<(BTreeMap<String, String>, SummaryValue)>,
        Option<(u64, u64)>,
    ),
    PhysicalNodeError,
> {
    let coverage = value.coverage();
    Ok((expand_item_rows(group_key, value, item_labels)?, coverage))
}

fn arithmetic(operator: &planner_types::pre_asap::ArithmeticOpKind, left: f64, right: f64) -> f64 {
    use planner_types::pre_asap::ArithmeticOpKind::*;
    match operator {
        Add => left + right,
        Sub => left - right,
        Mul => left * right,
        Div => left / right,
        Mod => left % right,
        Pow => left.powf(right),
        Atan2 => left.atan2(right),
    }
}

fn binary_values(
    operator: &planner_types::pre_asap::ArithmeticOpKind,
    lhs: &PhysicalQueryOutput,
    rhs: &PhysicalQueryOutput,
) -> Result<PhysicalQueryOutput, PhysicalNodeError> {
    type Labels = BTreeMap<String, String>;
    type Samples = BTreeMap<(Labels, i64), (f64, Option<(u64, u64)>)>;
    fn flatten(values: &[(Labels, SummaryValue)]) -> Result<Samples, PhysicalNodeError> {
        let mut result = BTreeMap::new();
        for (labels, value) in values {
            let SummaryValue::Points(points, coverage) = value else {
                return Err(PhysicalNodeError::ExpectedState);
            };
            let mut labels = labels.clone();
            labels.remove("__name__");
            for (timestamp, value) in points {
                if result
                    .insert((labels.clone(), *timestamp), (*value, *coverage))
                    .is_some()
                {
                    return Err(PhysicalNodeError::Fallback(
                        "ambiguous default vector matching".into(),
                    ));
                }
            }
        }
        Ok(result)
    }
    fn points(values: Samples) -> PhysicalQueryOutput {
        PhysicalQueryOutput::Value(
            values
                .into_iter()
                .map(|((labels, timestamp), (value, coverage))| {
                    (
                        labels,
                        SummaryValue::Points(vec![(timestamp, value)], coverage),
                    )
                })
                .collect(),
            None,
        )
    }
    match (lhs, rhs) {
        (PhysicalQueryOutput::Scalar(a), PhysicalQueryOutput::Scalar(b)) => {
            Ok(PhysicalQueryOutput::Scalar(arithmetic(operator, *a, *b)))
        }
        (PhysicalQueryOutput::Value(values, coverage), PhysicalQueryOutput::Scalar(scalar)) => {
            Ok({
                let mut output = points(
                    flatten(values)?
                        .into_iter()
                        .map(|(key, (value, coverage))| {
                            (key, (arithmetic(operator, value, *scalar), coverage))
                        })
                        .collect(),
                );
                if let PhysicalQueryOutput::Value(_, out_coverage) = &mut output {
                    *out_coverage = *coverage;
                }
                output
            })
        }
        (PhysicalQueryOutput::Scalar(scalar), PhysicalQueryOutput::Value(values, coverage)) => {
            Ok({
                let mut output = points(
                    flatten(values)?
                        .into_iter()
                        .map(|(key, (value, coverage))| {
                            (key, (arithmetic(operator, *scalar, value), coverage))
                        })
                        .collect(),
                );
                if let PhysicalQueryOutput::Value(_, out_coverage) = &mut output {
                    *out_coverage = *coverage;
                }
                output
            })
        }
        (
            PhysicalQueryOutput::Value(left, left_coverage),
            PhysicalQueryOutput::Value(right, right_coverage),
        ) => {
            let right = flatten(right)?;
            let mut output = points(
                flatten(left)?
                    .into_iter()
                    .filter_map(|(key, (left, coverage))| {
                        let (right, right_coverage) = right.get(&key)?;
                        Some((
                            key,
                            (
                                arithmetic(operator, left, *right),
                                intersect_coverage(coverage, *right_coverage),
                            ),
                        ))
                    })
                    .collect(),
            );
            if let PhysicalQueryOutput::Value(_, coverage) = &mut output {
                *coverage = intersect_coverage(*left_coverage, *right_coverage);
            }
            Ok(output)
        }
        _ => Err(PhysicalNodeError::ExpectedState),
    }
}

fn intersect_coverage(left: Option<(u64, u64)>, right: Option<(u64, u64)>) -> Option<(u64, u64)> {
    let (left, right) = (left?, right?);
    let result = (left.0.max(right.0), left.1.min(right.1));
    (result.0 <= result.1).then_some(result)
}

fn reduce_sum_values(
    grouping: &asap_types::query_plan::PhysicalGrouping,
    values: &[(BTreeMap<String, String>, SummaryValue)],
    coverage: Option<(u64, u64)>,
) -> Result<PhysicalQueryOutput, PhysicalNodeError> {
    let asap_types::query_plan::PhysicalGrouping::Reduce(keys) = grouping else {
        return Ok(PhysicalQueryOutput::Value(values.to_vec(), coverage));
    };
    let mut groups = BTreeMap::new();
    for (labels, value) in values {
        let SummaryValue::Points(points, coverage) = value else {
            return Err(PhysicalNodeError::ExpectedState);
        };
        let labels = labels
            .iter()
            .filter(|(name, _)| keys.contains(name))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<BTreeMap<_, _>>();
        for (timestamp, value) in points {
            groups
                .entry((labels.clone(), *timestamp))
                .and_modify(|(sum, cover): &mut (f64, Option<(u64, u64)>)| {
                    *sum += value;
                    *cover = intersect_coverage(*cover, *coverage);
                })
                .or_insert((*value, *coverage));
        }
    }
    Ok(PhysicalQueryOutput::Value(
        groups
            .into_iter()
            .map(|((labels, timestamp), (sum, coverage))| {
                (
                    labels,
                    SummaryValue::Points(vec![(timestamp, sum)], coverage),
                )
            })
            .collect(),
        coverage,
    ))
}

fn execute_physical_query_plan(
    index: &SketchStore,
    entry: &asap_types::query_plan::QueryPlanEntry,
    t0_ms: u64,
    t1_ms: u64,
    is_cumulative: bool,
) -> Result<PostAsapReadoutOutcome, LoweringSkip> {
    execute_physical_query_payload(index, entry, entry.root, t0_ms, t1_ms, is_cumulative)
}

fn execute_physical_query_payload(
    index: &SketchStore,
    entry: &asap_types::query_plan::QueryPlanEntry,
    root: asap_types::query_plan::QueryNodeId,
    t0_ms: u64,
    t1_ms: u64,
    is_cumulative: bool,
) -> Result<PostAsapReadoutOutcome, LoweringSkip> {
    let revision = index.summary_update_revision();
    let result = (|| {
        let runtime = PhysicalQueryRuntime {
            language: entry.language,
            catalog: index.summary_catalog_snapshot(),
            context: QueryExecutionContext {
                index,
                t0_ms,
                t1_ms,
                is_cumulative,
                allowed_materializations: None,
            },
        };
        let output = physical_dag::execute_from(entry, root, &runtime)
            .map_err(|error| LoweringSkip::ExecuteFailed(format!("{error:?}")))?;
        match output {
            PhysicalQueryOutput::Scalar(_) => Err(LoweringSkip::ExecuteFailed(
                "scalar-only query is not a warm vector result".into(),
            )),
            PhysicalQueryOutput::Value(values, coverage) => {
                let mut series = Vec::new();
                for (group_key, value) in &values {
                    series.extend(summary_value_to_series(group_key, value));
                }
                Ok(PostAsapReadoutOutcome { series, coverage })
            }
            PhysicalQueryOutput::State { groups, .. } => {
                let mut coverage = None;
                let mut series = Vec::new();
                for (group_key, state) in &groups {
                    fold_coverage(&mut coverage, state.exact_coverage());
                    if let Some(value) = state.exact_value(&None) {
                        series.push((group_key.clone(), vec![(t1_ms as i64, value)]));
                    }
                }
                Ok(PostAsapReadoutOutcome { series, coverage })
            }
        }
    })();
    if !revision.matches(index.summary_update_revision()) {
        return Err(LoweringSkip::ExecuteFailed(
            "summary input changed during query DAG evaluation".into(),
        ));
    }
    result
}

/// Plan and execute an instant query without consulting the legacy candidate
/// analyzer. Lookback and cumulative-vs-per-window behavior come from the
/// post-ASAP DAG itself.
pub fn execute_post_asap_instant(
    index: &SketchStore,
    query: &str,
    now_ms: u64,
    accuracy: AccuracyTarget,
) -> Result<(PostAsapReadoutOutcome, u64), LoweringSkip> {
    const DEFAULT_LOOKBACK_MS: u64 = 5 * 60 * 1000;
    let node = plan_promql_to_post_asap(index, query, accuracy.clone())?;
    let hints = execution_hints(&node);
    let t0_ms = if hints.full_history {
        0
    } else {
        now_ms.saturating_sub(hints.lookback_ms.unwrap_or(DEFAULT_LOOKBACK_MS))
    };
    let outcome = execute_planned_post_asap(
        index,
        &node,
        query,
        accuracy,
        t0_ms,
        now_ms,
        hints.cumulative_readout,
    )?;
    Ok((outcome, t0_ms))
}

fn execute_planned_post_asap(
    index: &SketchStore,
    node: &planner_types::post_asap::SummaryNode,
    query: &str,
    accuracy: AccuracyTarget,
    t0_ms: u64,
    t1_ms: u64,
    is_cumulative: bool,
) -> Result<PostAsapReadoutOutcome, LoweringSkip> {
    let _ = (query, accuracy);
    let allowed_materializations = None;
    let ctx = QueryExecutionContext {
        index,
        t0_ms,
        t1_ms,
        is_cumulative,
        allowed_materializations,
    };

    match execute(node, &ctx) {
        Ok(ExecOutcome::Value(values)) => {
            let mut coverage: Option<(u64, u64)> = None;
            let mut series = Vec::new();
            for (group_key, value) in &values {
                fold_coverage(&mut coverage, value.coverage());
                series.extend(summary_value_to_series(group_key, value));
            }
            Ok(PostAsapReadoutOutcome { series, coverage })
        }
        Ok(ExecOutcome::State(groups)) => {
            let mut coverage: Option<(u64, u64)> = None;
            let mut series = Vec::new();
            for (group_key, state, _family) in &groups {
                fold_coverage(&mut coverage, state.exact_coverage());
                let Some(value) = state.exact_value(&None) else {
                    continue;
                };
                series.push((group_key.clone(), vec![(t1_ms as i64, value)]));
            }
            Ok(PostAsapReadoutOutcome { series, coverage })
        }
        Err(e) => Err(LoweringSkip::ExecuteFailed(format!("{e:?}"))),
    }
}

/// `SummaryValue::Points`/`TopK` -> `ASAPTierResult.series`'s row shape.
/// `TopK`'s ranked-list-per-timestamp shape is pivoted into one row per
/// item (each row = the group's label map plus an `item` label, one point
/// per timestamp that item appeared in the ranked list) -- the SAME
/// convention `sketch_reducer.rs`'s own topk arm already uses, not a new
/// one invented here.
fn summary_value_to_series(
    group_key: &BTreeMap<String, String>,
    value: &SummaryValue,
) -> SeriesRows {
    match value {
        SummaryValue::Points(points, _coverage) => {
            vec![(group_key.clone(), points.clone())]
        }
        SummaryValue::TopK(ranked_per_ts, _coverage) => {
            let mut by_item: BTreeMap<String, Vec<(i64, f64)>> = BTreeMap::new();
            for (ts, items) in ranked_per_ts {
                for (item, val) in items {
                    by_item.entry(item.clone()).or_default().push((*ts, *val));
                }
            }
            by_item
                .into_iter()
                .map(|(item, points)| {
                    let mut lv = group_key.clone();
                    lv.insert("item".to_string(), item);
                    (lv, points)
                })
                .collect()
        }
    }
}

pub(crate) fn fold_coverage(coverage: &mut Option<(u64, u64)>, next: Option<(u64, u64)>) {
    let Some((lo, hi)) = next else { return };
    *coverage = Some(match *coverage {
        Some((clo, chi)) => (clo.min(lo), chi.max(hi)),
        None => (lo, hi),
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use planner_types::pre_asap::ArithmeticOpKind;

    fn exact_points(metric: &str, service: &str, value: f64) -> PhysicalQueryOutput {
        PhysicalQueryOutput::Value(
            vec![(
                BTreeMap::from([
                    ("__name__".into(), metric.into()),
                    ("service".into(), service.into()),
                ]),
                SummaryValue::Points(vec![(2000, value)], Some((1000, 2000))),
            )],
            Some((1000, 2000)),
        )
    }

    // Arithmetic follows default vector matching, not positional row pairing.
    #[test]
    fn exact_binary_matches_labels_and_preserves_scalar_orientation() {
        let left = exact_points("sum", "api", 24.0);
        let right = exact_points("count", "api", 3.0);
        let PhysicalQueryOutput::Value(result, _) =
            binary_values(&ArithmeticOpKind::Div, &left, &right).unwrap()
        else {
            panic!("expected vector")
        };
        assert_eq!(
            result[0].0,
            BTreeMap::from([("service".into(), "api".into())])
        );
        let SummaryValue::Points(points, coverage) = &result[0].1 else {
            panic!("expected points")
        };
        assert_eq!(points, &vec![(2000, 8.0)]);
        assert_eq!(*coverage, Some((1000, 2000)));
        let PhysicalQueryOutput::Value(result, _) = binary_values(
            &ArithmeticOpKind::Div,
            &PhysicalQueryOutput::Scalar(48.0),
            &left,
        )
        .unwrap() else {
            panic!("expected vector")
        };
        let SummaryValue::Points(points, _) = &result[0].1 else {
            panic!("expected points")
        };
        assert_eq!(points, &vec![(2000, 2.0)]);
        let PhysicalQueryOutput::Value(result, _) = binary_values(
            &ArithmeticOpKind::Div,
            &left,
            &exact_points("count", "worker", 3.0),
        )
        .unwrap() else {
            panic!("expected vector")
        };
        assert!(result.is_empty());
    }

    // Zero denominators remain IEEE results; duplicate matches must fall back.
    #[test]
    fn exact_binary_handles_zero_and_rejects_ambiguous_matches() {
        assert!(arithmetic(&ArithmeticOpKind::Div, 1.0, 0.0).is_infinite());
        assert!(arithmetic(&ArithmeticOpKind::Div, 0.0, 0.0).is_nan());
        let PhysicalQueryOutput::Value(mut values, coverage) = exact_points("a", "api", 1.0) else {
            unreachable!()
        };
        values.push(values[0].clone());
        assert!(binary_values(
            &ArithmeticOpKind::Add,
            &PhysicalQueryOutput::Value(values, coverage),
            &PhysicalQueryOutput::Scalar(1.0)
        )
        .is_err());
        assert_eq!(
            intersect_coverage(Some((1000, 2000)), Some((1500, 2500))),
            Some((1500, 2000))
        );
        assert_eq!(intersect_coverage(Some((1000, 2000)), None), None);
    }

    // Rollup adds partial counts rather than counting the number of series.
    #[test]
    fn exact_rollup_adds_uneven_observation_counts() {
        let values = [("a", 1.0), ("b", 3.0)]
            .into_iter()
            .map(|(instance, value)| {
                (
                    BTreeMap::from([
                        ("service".into(), "api".into()),
                        ("instance".into(), instance.into()),
                    ]),
                    SummaryValue::Points(vec![(2000, value)], Some((1000, 2000))),
                )
            })
            .collect::<Vec<_>>();
        let PhysicalQueryOutput::Value(result, _) = reduce_sum_values(
            &asap_types::query_plan::PhysicalGrouping::Reduce(vec!["service".into()]),
            &values,
            Some((1000, 2000)),
        )
        .unwrap() else {
            panic!("expected vector")
        };
        assert_eq!(result.len(), 1);
        let SummaryValue::Points(points, _) = &result[0].1 else {
            panic!("expected points")
        };
        assert_eq!(points, &vec![(2000, 4.0)]);
    }
    use crate::storage_engines::sketch_db::data::{AggKind, SketchConfig};
    use crate::storage_engines::sketch_db::index::{
        AccuracyBound, Capability, SketchAlgorithm, SketchInstanceMetadata, SketchSampleState,
    };

    fn accuracy() -> AccuracyTarget {
        AccuracyTarget::Epsilon(0.01)
    }

    fn register_hll(idx: &SketchStore, sid: u64, service: &str, items: &[&str]) {
        // precision 14 -- what `ControlPlaneCostModel` actually picks for
        // `AccuracyTarget::Epsilon(0.01)` (confirmed by inspecting the
        // bound tree directly); `find_candidates`'s exact-match contract
        // means a mismatched precision here would just silently produce
        // `NoCandidates`, not a wrong answer -- but that's not what these
        // tests are checking.
        let cfg = SketchConfig::Hll { precision: 14 };
        let mut group_by_keys = std::collections::BTreeSet::new();
        group_by_keys.insert("service".to_string());
        idx.register(SketchInstanceMetadata {
            sid,
            metric_name: "unique_users".to_string(),
            group_by_keys,
            capability: Some(Capability::CardinalityApprox),
            agg_kind: AggKind::Sketch {
                algorithm: SketchAlgorithm::Hll,
                config: cfg.clone(),
                spatial_filter_canonical: String::new(),
            },
            accuracy: Some(AccuracyBound::from_config(&cfg)),
            first_seen_unix_ms: 0,
            retired_at_ms: None,
            expires_at_ms: None,
            policy_fp: asap_types::PolicyFingerprint(123),
        });
        use asap_sketchlib::{HllSketch, HllVariant, MessagePackCodec};
        let mut sk = HllSketch::new(HllVariant::Regular, 14);
        for item in items {
            sk.update(item.as_bytes());
        }
        let mut labels = BTreeMap::new();
        labels.insert("service".to_string(), service.to_string());
        idx.append_sample(
            sid,
            labels,
            (1_000, 2_000),
            SketchSampleState {
                bytes: sk.to_msgpack().expect("encode HLL"),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::MsgpackFull,
            },
        );
    }

    fn ddsketch_fixture() -> SketchStore {
        let idx = SketchStore::new();
        // alpha 0.01 -- what `ControlPlaneCostModel` actually picks for
        // `quantile_over_time` at `AccuracyTarget::Epsilon(0.01)` (DDSketch,
        // not KLL -- confirmed by inspecting the bound tree directly).
        let cfg = SketchConfig::DDSketch {
            relative_accuracy: 0.01,
        };
        idx.register(SketchInstanceMetadata {
            sid: 1,
            metric_name: "latency_ms".to_string(),
            group_by_keys: std::collections::BTreeSet::new(),
            capability: Some(Capability::QuantileApprox(Some(SketchAlgorithm::DDSketch))),
            agg_kind: AggKind::Sketch {
                algorithm: SketchAlgorithm::DDSketch,
                config: cfg.clone(),
                spatial_filter_canonical: String::new(),
            },
            accuracy: Some(AccuracyBound::from_config(&cfg)),
            first_seen_unix_ms: 0,
            retired_at_ms: None,
            expires_at_ms: None,
            policy_fp: asap_types::PolicyFingerprint::UNSET,
        });
        use asap_sketchlib::{DdSketch, MessagePackCodec};
        let mut sk = DdSketch::new(0.01);
        for i in 1..=100 {
            sk.update(i as f64);
        }
        idx.append_sample(
            1,
            BTreeMap::new(),
            (1_000, 2_000),
            SketchSampleState {
                bytes: sk.to_msgpack().expect("encode DDSketch"),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::MsgpackFull,
            },
        );
        idx
    }

    // The same installed summary follows each language's metric-name semantics;
    // spatial reduction must not invent a source metric on the aggregate.
    #[test]
    fn metricsql_quantile_preserves_catalog_metric_name_only_per_entity() {
        use control_plane::query_plan::*;
        let config: asap_types::PrecomputeMaterialization =
            serde_json::from_value(serde_json::json!({
                "aggregation_type": "DDSketch", "aggregation_sub_type": "",
                "metric": "latency_ms", "window_size": 1, "slide_interval": 1,
                "window_type": "tumbling", "num_aggregates_to_retain": 3,
                "parameters": {"alpha": 0.01}, "pane_origin_ms": 0,
                "partitioning": "per_entity", "window_layout": {"kind": "pane", "pane_secs": 1},
                "grouping_labels": {"labels": []}, "aggregated_labels": {"labels": []},
                "rollup_labels": {"labels": []}, "spatial_filter": "",
                "spatial_filter_normalized": "", "original_yaml": ""
            }))
            .unwrap();
        let idx = ddsketch_fixture();
        let mut metadata = (*idx.instance(1).unwrap()).clone();
        metadata.policy_fp = config.policy_fingerprint();
        idx.install_summary_catalog(std::sync::Arc::new(
            asap_types::summary_catalog::SummaryCatalog::from_materializations(
                1,
                1,
                &[config.clone()],
            )
            .unwrap(),
        ))
        .unwrap();
        idx.register(metadata);
        let mut entry = QueryPlanEntry {
            language: QueryLanguage::MetricsQl,
            query_id: "quantile".into(),
            canonical_query: "quantile_over_time(0.9, latency_ms[1s])".into(),
            fixed_evaluation: None,
            root: QueryNodeId(0),
            nodes: BTreeMap::from([
                (
                    QueryNodeId(0),
                    QueryPlanNode::SummaryEstimate {
                        input: QueryNodeId(1),
                        query: QueryReadout::Quantile { q: 0.9 },
                    },
                ),
                (
                    QueryNodeId(1),
                    QueryPlanNode::ReadMaterialization {
                        binding: MaterializationBinding {
                            materialization: config.policy_fingerprint().into(),
                            output_grouping: PhysicalGrouping::PerEntity,
                            item_labels: vec![],
                            window_ms: 1000,
                            pane_origin_ms: Some(0),
                            readout_lookback_ms: Some(1000),
                        },
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
        let result = execute_query_plan_readout(&idx, &entry, 1000, 2000, true).unwrap();
        assert_eq!(
            result.series[0].0.get("__name__").map(String::as_str),
            Some("latency_ms")
        );
        entry.language = QueryLanguage::PromQl;
        let result = execute_query_plan_readout(&idx, &entry, 1000, 2000, true).unwrap();
        assert!(!result.series[0].0.contains_key("__name__"));
        entry.language = QueryLanguage::MetricsQl;
        let QueryPlanNode::ReadMaterialization { binding } =
            entry.nodes.get_mut(&QueryNodeId(1)).unwrap()
        else {
            unreachable!()
        };
        binding.output_grouping = PhysicalGrouping::Reduce(vec![]);
        let result = execute_query_plan_readout(&idx, &entry, 1000, 2000, true).unwrap();
        assert!(!result.series[0].0.contains_key("__name__"));
    }

    #[test]
    fn formal_query_plan_executes_only_its_bound_policy() {
        let idx = SketchStore::new();
        register_hll(&idx, 1, "api", &["a", "b"]);
        register_hll(&idx, 2, "worker", &["b", "c"]);
        let node = plan_promql_to_post_asap(&idx, "count(unique_users)", accuracy())
            .expect("compile-stage fixture");
        let canonical = asap_types::query_plan::canonical_promql("count(unique_users)").unwrap();
        let entry = control_plane::query_plan::compile_bound(
            "q-cardinality".into(),
            canonical,
            &node,
            asap_types::query_plan::InstantExecution {
                lookback_ms: 60_000,
                full_history: false,
                cumulative_readout: true,
            },
            asap_types::query_plan::FallbackPolicy::ExactBackend,
            |_node, _family| {
                Ok(asap_types::query_plan::MaterializationBinding {
                    item_labels: Vec::new(),
                    materialization: asap_types::PolicyFingerprint(123).into(),
                    output_grouping: asap_types::query_plan::PhysicalGrouping::PerEntity,
                    window_ms: 60_000,
                    pane_origin_ms: Some(2_000),
                    readout_lookback_ms: Some(60_000),
                })
            },
        )
        .unwrap();
        let result = execute_query_plan_readout(&idx, &entry, 1_000, 2_000, true)
            .expect("execute formal QueryPlan");
        assert!(!result.series.is_empty());
    }

    #[test]
    fn bare_range_function_keeps_one_series_per_entity() {
        let idx = ddsketch_fixture();
        let outcome = execute_post_asap_readout(
            &idx,
            "quantile_over_time(0.99, latency_ms[1m])",
            1_000,
            2_000,
            true,
            accuracy(),
        )
        .expect("should execute");
        assert_eq!(outcome.series.len(), 1);
    }

    #[test]
    fn global_merge_shape_now_merges_instead_of_being_declined() {
        // The exact ASAPController#163 shape: two HLL sids, no explicit
        // by(), an aggregation-operator query. This test previously
        // asserted `ambiguous_merge_risk == true` and TWO unmerged series
        // -- i.e. it pinned the old workaround, where an empty `by` left
        // `find_candidates` unable to tell "reduce everything" apart from
        // "no grouping concept," so `live_serve` declined to serve the
        // shape at all.
        //
        // With `Reduction` (ASAPController#165) that ambiguity is gone:
        // `count(...)` is a genuine aggregation operator, so it lowers to
        // `Reduce([])` and `resolve_group_key` gives every candidate the
        // SAME group key -- the two sids MERGE into one answer, which is
        // what the query actually asked for. No gate, no fallback.
        let idx = SketchStore::new();
        register_hll(&idx, 1, "svc-a", &["a", "b", "c"]);
        register_hll(&idx, 2, "svc-b", &["d", "e", "f"]);
        let outcome =
            execute_post_asap_readout(&idx, "count(unique_users)", 1_000, 2_000, true, accuracy())
                .expect("should execute");
        assert_eq!(
            outcome.series.len(),
            1,
            "a by-less count() is a full reduction -- both HLL sids must merge into ONE \
             series, not stay split (and not be declined), got {:?}",
            outcome.series
        );
        // Disjoint item sets {a,b,c} + {d,e,f} -> merged cardinality ~6.
        let (_group, points) = &outcome.series[0];
        let card = points[0].1;
        assert!(
            (4.0..=8.0).contains(&card),
            "merged cardinality {card} should be ~6 (both sids' disjoint items), not ~3"
        );
    }

    #[test]
    fn exact_agg_outcome_reports_window_end_coverage() {
        let idx = SketchStore::new();
        idx.register(
            crate::storage_engines::sketch_db::index::SketchInstanceMetadata {
                sid: 1,
                metric_name: "bytes_total".to_string(),
                group_by_keys: std::collections::BTreeSet::new(),
                capability: Some(Capability::ExactAgg(asap_types::AggregationType::Sum)),
                agg_kind: AggKind::ExactAgg {
                    agg_type: asap_types::AggregationType::Sum,
                    parameters_canonical: String::new(),
                    spatial_filter_canonical: String::new(),
                },
                accuracy: None,
                first_seen_unix_ms: 0,
                retired_at_ms: None,
                expires_at_ms: None,
                policy_fp: asap_types::PolicyFingerprint::UNSET,
            },
        );
        idx.append_precompute(
            1,
            BTreeMap::new(),
            (1_000, 2_000),
            Box::new(crate::precompute_engine::operators::SumAccumulator::with_sum(42.0)),
        );
        let outcome =
            execute_post_asap_readout(&idx, "sum(bytes_total)", 1_000, 2_000, true, accuracy())
                .expect("should execute");
        // Window-end-only coverage: a single window (1_000, 2_000) is
        // keyed by its end (2_000) alone, so both bounds equal 2_000 --
        // same semantics as `SummaryValue::coverage()`, reconfirmed for
        // `exact_coverage` by this module's A0 test in `summary_executor.rs`.
        assert_eq!(outcome.coverage, Some((2_000, 2_000)));
    }

    #[test]
    fn repeated_multi_pane_reads_exclude_expired_state_and_reject_gaps() {
        let idx = SketchStore::new();
        let policy = asap_types::PolicyFingerprint(777);
        idx.register(SketchInstanceMetadata {
            sid: 7,
            metric_name: "requests_total".into(),
            group_by_keys: std::collections::BTreeSet::new(),
            capability: Some(Capability::ExactAgg(asap_types::AggregationType::Sum)),
            agg_kind: AggKind::ExactAgg {
                agg_type: asap_types::AggregationType::Sum,
                parameters_canonical: String::new(),
                spatial_filter_canonical: String::new(),
            },
            accuracy: None,
            first_seen_unix_ms: 0,
            retired_at_ms: None,
            expires_at_ms: None,
            policy_fp: policy,
        });
        // Old panes remain stored; each advancing query must select only its lookback.
        for pane in 0..8 {
            idx.append_precompute(
                7,
                BTreeMap::new(),
                (pane * 10_000, (pane + 1) * 10_000),
                Box::new(
                    crate::precompute_engine::operators::SumAccumulator::with_sum(
                        (pane + 1) as f64,
                    ),
                ),
            );
        }

        let entry = asap_types::query_plan::QueryPlanEntry {
            language: asap_types::query_plan::QueryLanguage::PromQl,
            query_id: "q-rate".into(),
            canonical_query: "rate(requests_total[1m])".into(),
            fixed_evaluation: None,
            root: asap_types::query_plan::QueryNodeId(0),
            nodes: BTreeMap::from([
                (
                    asap_types::query_plan::QueryNodeId(0),
                    QueryPlanNode::ExactReadout {
                        input: asap_types::query_plan::QueryNodeId(1),
                        readout: asap_types::query_plan::ExactReadout::Sum,
                    },
                ),
                (
                    asap_types::query_plan::QueryNodeId(1),
                    QueryPlanNode::ReadMaterialization {
                        binding: asap_types::query_plan::MaterializationBinding {
                            item_labels: Vec::new(),
                            materialization: policy.into(),
                            output_grouping: asap_types::query_plan::PhysicalGrouping::PerEntity,
                            window_ms: 10_000,
                            pane_origin_ms: Some(0),
                            readout_lookback_ms: Some(60_000),
                        },
                    },
                ),
            ]),
            instant: asap_types::query_plan::InstantExecution {
                lookback_ms: 60_000,
                full_history: false,
                cumulative_readout: true,
            },
            fallback: asap_types::query_plan::FallbackPolicy::ExactBackend,
        };
        for (now, expected) in [(60_000, 21.0), (70_000, 27.0), (80_000, 33.0)] {
            let (outcome, _) = execute_query_plan_instant(&idx, &entry, now).unwrap();
            assert_eq!(outcome.series[0].1[0].1, expected);
        }
        assert!(
            execute_query_plan_instant(&idx, &entry, 70_001).is_err(),
            "partial additive pane must fall back"
        );
        assert!(
            execute_query_plan_instant(&idx, &entry, 90_000).is_err(),
            "open/missing trailing pane must fall back"
        );
        // Both endpoints exist, but the missing interior pane is not evidence of zero samples.
        let gap_idx = SketchStore::new();
        idx.with_instance(7, |meta| gap_idx.register(meta.clone()));
        for pane in [0, 1, 3, 4, 5] {
            gap_idx.append_precompute(
                7,
                BTreeMap::new(),
                (pane * 10_000, (pane + 1) * 10_000),
                Box::new(crate::precompute_engine::operators::SumAccumulator::with_sum(1.0)),
            );
        }
        assert!(
            execute_query_plan_instant(&gap_idx, &entry, 60_000).is_err(),
            "interior gap must fall back"
        );
    }

    #[test]
    fn exact_query_plan_rate_uses_reset_aware_readout() {
        let idx = SketchStore::new();
        let policy = asap_types::PolicyFingerprint(777);
        idx.register(SketchInstanceMetadata {
            sid: 7,
            metric_name: "requests_total".into(),
            group_by_keys: std::collections::BTreeSet::new(),
            capability: Some(Capability::ExactAgg(asap_types::AggregationType::Increase)),
            agg_kind: AggKind::ExactAgg {
                agg_type: asap_types::AggregationType::Increase,
                parameters_canonical: String::new(),
                spatial_filter_canonical: String::new(),
            },
            accuracy: None,
            first_seen_unix_ms: 0,
            retired_at_ms: None,
            expires_at_ms: None,
            policy_fp: policy,
        });
        use crate::storage_engines::types::Measurement;
        let mut accumulator = crate::precompute_engine::operators::IncreaseAccumulator::new(
            Measurement::new(10.0),
            10_000,
            Measurement::new(10.0),
            10_000,
        );
        accumulator.update(Measurement::new(20.0), 20_000);
        accumulator.update(Measurement::new(3.0), 30_000);
        accumulator.update(Measurement::new(13.0), 50_000);
        idx.append_precompute(7, BTreeMap::new(), (0, 60_000), Box::new(accumulator));

        let entry = asap_types::query_plan::QueryPlanEntry {
            language: asap_types::query_plan::QueryLanguage::PromQl,
            query_id: "q-rate".into(),
            canonical_query: "rate(requests_total[1m])".into(),
            fixed_evaluation: None,
            root: asap_types::query_plan::QueryNodeId(0),
            nodes: BTreeMap::from([
                (
                    asap_types::query_plan::QueryNodeId(0),
                    QueryPlanNode::ExactReadout {
                        input: asap_types::query_plan::QueryNodeId(1),
                        readout: asap_types::query_plan::ExactReadout::Rate,
                    },
                ),
                (
                    asap_types::query_plan::QueryNodeId(1),
                    QueryPlanNode::ReadMaterialization {
                        binding: asap_types::query_plan::MaterializationBinding {
                            item_labels: Vec::new(),
                            materialization: policy.into(),
                            output_grouping: asap_types::query_plan::PhysicalGrouping::PerEntity,
                            window_ms: 60_000,
                            pane_origin_ms: Some(0),
                            readout_lookback_ms: Some(60_000),
                        },
                    },
                ),
            ]),
            instant: asap_types::query_plan::InstantExecution {
                lookback_ms: 60_000,
                full_history: false,
                cumulative_readout: true,
            },
            fallback: asap_types::query_plan::FallbackPolicy::ExactBackend,
        };
        let outcome = execute_query_plan_readout(&idx, &entry, 0, 60_000, true)
            .expect("execute exact rate DAG");
        let value = outcome.series[0].1[0].1;
        assert!((value - 0.575).abs() < 1e-12, "reset-aware rate={value}");
        let native_runtime = PhysicalQueryRuntime {
            language: control_plane::query_plan::QueryLanguage::MetricsQl,
            catalog: None,
            context: QueryExecutionContext {
                index: &idx,
                t0_ms: 0,
                t1_ms: 60_000,
                is_cumulative: true,
                allowed_materializations: None,
            },
        };
        for readout in [
            control_plane::query_plan::ExactReadout::Rate,
            control_plane::query_plan::ExactReadout::Increase,
        ] {
            assert!(matches!(native_runtime.execute_node(QueryNodeId(0),
                &QueryPlanNode::ExactReadout { input:QueryNodeId(1),readout }, &[]),
                Err(PhysicalNodeError::Fallback(reason))
                    if reason.contains("native MetricsQL counter semantics require external exact execution")));
        }
        assert!(
            execute_query_plan_readout(&idx, &entry, 1, 60_000, true).is_err(),
            "a partial leading counter pane needs Prometheus boundary samples"
        );
        assert!(
            execute_query_plan_readout(&idx, &entry, 0, 59_999, true).is_err(),
            "a partial trailing counter pane needs Prometheus boundary samples"
        );
    }

    #[test]
    fn global_heap_items_keep_the_inner_aggregate_label_for_candidate_join() {
        let value = SummaryValue::TopK(
            vec![(60_000, vec![("payment".into(), 9.0), ("order".into(), 7.0)])],
            Some((0, 60_000)),
        );
        let rows = expand_item_rows(&BTreeMap::new(), value, &["job".into()]).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0.get("job").map(String::as_str), Some("order"));
        assert_eq!(rows[1].0.get("job").map(String::as_str), Some("payment"));
        assert!(rows.iter().all(|(labels, value)| {
            !labels.contains_key("item") && matches!(value, SummaryValue::Points(_, _))
        }));
    }

    #[test]
    fn empty_global_heap_keeps_complete_coverage_at_actual_evaluation_phase() {
        const EVALUATION_MS: u64 = 1_788_891_296_000;
        let coverage = Some((EVALUATION_MS - 3_600_000 + 60_000, EVALUATION_MS));
        let (rows, retained_coverage) = expand_item_readout(
            &BTreeMap::new(),
            SummaryValue::TopK(vec![(EVALUATION_MS as i64, vec![])], coverage),
            &["job".into()],
        )
        .unwrap();
        assert!(rows.is_empty());
        assert_eq!(retained_coverage, coverage);
        assert_eq!((EVALUATION_MS - 56_000) % 60_000, 0);
    }
}
