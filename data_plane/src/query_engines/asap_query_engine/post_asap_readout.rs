//! Execute installed query DAGs and project their values and coverage.

use std::collections::BTreeMap;

use asap_physical_operators::arithmetic::evaluate_float64_arithmetic as arithmetic;

use asap_types::query_plan::{QueryNodeId, QueryPlanNode};

use crate::query_engines::asap_query_engine::physical_dag::{self, QueryNodeRuntime};
/// A planned warm query cannot be served; callers may route to an exact backend.
#[derive(Debug)]
pub enum LoweringSkip {
    Disabled,
    QueryNotPlanned(String),
    InvalidQueryPlan(String),
    MaterializationNotReady(String),
    ExecuteFailed(String),
}
use crate::query_engines::asap_query_engine::summary_executor::{
    GroupState, QueryExecutionContext, SummaryExecutorError, SummaryValue,
};
use crate::storage_engines::sketch_db::index::SketchStore;

/// Mirrors `ASAPTierResult.series`'s row shape — `(label_values, samples)`
/// where `samples` is `(window_end_unix_ms, value)`.
pub type SeriesRows = Vec<(BTreeMap<String, String>, Vec<(i64, f64)>)>;

/// Summary execution result projected into `ASAPTierResult`, for both sketch
/// values and exact accumulator states.
///
/// Grouping is resolved before this projection: `PerEntity` preserves each
/// entity, while `Reduce([])` merges every candidate into a single group.
pub struct PostAsapReadoutOutcome {
    pub series: SeriesRows,
    pub coverage: Option<(u64, u64)>,
}

/// Execute the materializations and readouts bound in an installed QueryPlan entry.
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
                            let definition = catalog.definitions.get(&binding.materialization)?;
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
                                            && !matches!(
                                                readout,
                                                control_plane::query_plan::ExactReadout::Max
                                                    | control_plane::query_plan::ExactReadout::Min
                                            )
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
    use crate::query_engines::asap_query_engine::test_plan;
    use asap_types::query_plan::{ExactReadout, PhysicalGrouping, QueryReadout};
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
        AccuracyBound, Capability, SketchAlgorithm, SketchSampleState, SummarySeriesMetadata,
    };

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
        idx.register(SummarySeriesMetadata {
            storage_handle: sid,
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
        idx.register(SummarySeriesMetadata {
            storage_handle: 1,
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
                            full_window_slide_ms: None,
                            materialization: config.policy_fingerprint().into(),
                            stored_output_reference:
                                asap_types::sds::StoredOutputReference::for_definition(
                                    config.policy_fingerprint().into(),
                                ),
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
        let config = crate::query_engines::asap_query_engine::test_plan::materialization(
            "unique_users",
            "HLL",
            serde_json::json!({"precision": 14}),
            &["service"],
            1000,
        );
        let entry = crate::query_engines::asap_query_engine::test_plan::entry(
            "count(distinct_over_time(unique_users[1m]))",
            &config,
            asap_types::query_plan::PhysicalGrouping::PerEntity,
            1000,
            QueryPlanNode::SummaryEstimate {
                input: QueryNodeId(0),
                query: asap_types::query_plan::QueryReadout::Cardinality,
            },
        );
        crate::query_engines::asap_query_engine::test_plan::install(
            &idx,
            &[(config, vec![1])],
            vec![entry.clone()],
        );
        let result = execute_query_plan_readout(&idx, &entry, 1_000, 2_000, true)
            .expect("execute formal QueryPlan");
        assert_eq!(result.series.len(), 1, "unbound policy must not contribute");
        assert_eq!(
            result.series[0].0.get("service").map(String::as_str),
            Some("api")
        );
        assert!((result.series[0].1[0].1 - 2.0).abs() < 0.1);
    }

    #[test]
    fn bare_range_function_keeps_one_series_per_entity() {
        let idx = ddsketch_fixture();
        let config = test_plan::materialization(
            "latency_ms",
            "DDSketch",
            serde_json::json!({"alpha": 0.01}),
            &[],
            1000,
        );
        let entry = test_plan::entry(
            "quantile_over_time(0.99, latency_ms[1s])",
            &config,
            PhysicalGrouping::PerEntity,
            1000,
            QueryPlanNode::SummaryEstimate {
                input: QueryNodeId(0),
                query: QueryReadout::Quantile { q: 0.99 },
            },
        );
        test_plan::install(&idx, &[(config, vec![1])], vec![entry.clone()]);
        let outcome = execute_query_plan_readout(&idx, &entry, 1000, 2000, true).unwrap();
        assert_eq!(outcome.series.len(), 1);
    }

    #[test]
    fn global_merge_shape_now_merges_instead_of_being_declined() {
        // An ungrouped count reduces two HLL sids into one answer. `Reduce([])`
        // assigns both candidates the same group key so their states merge.
        let idx = SketchStore::new();
        register_hll(&idx, 1, "svc-a", &["a", "b", "c"]);
        register_hll(&idx, 2, "svc-b", &["d", "e", "f"]);
        let config = test_plan::materialization(
            "unique_users",
            "HLL",
            serde_json::json!({"precision": 14}),
            &["service"],
            1000,
        );
        let entry = test_plan::entry(
            "count(distinct_over_time(unique_users[1m]))",
            &config,
            PhysicalGrouping::Reduce(vec![]),
            1000,
            QueryPlanNode::SummaryEstimate {
                input: QueryNodeId(0),
                query: QueryReadout::Cardinality,
            },
        );
        test_plan::install(&idx, &[(config, vec![1, 2])], vec![entry.clone()]);
        let outcome = execute_query_plan_readout(&idx, &entry, 1000, 2000, true).unwrap();
        assert_eq!(
            outcome.series.len(),
            1,
            "a by-less distinct count is a full reduction -- both HLL sids must merge into \
             ONE series, not stay split (and not be declined), got {:?}",
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
            crate::storage_engines::sketch_db::index::SummarySeriesMetadata {
                storage_handle: 1,
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
            Box::new(asap_physical_operators::summary_kernels::SumAccumulator::with_sum(42.0)),
        );
        let config =
            test_plan::materialization("bytes_total", "Sum", serde_json::json!({}), &[], 1000);
        let entry = test_plan::entry(
            "sum(bytes_total)",
            &config,
            PhysicalGrouping::Reduce(vec![]),
            1000,
            QueryPlanNode::ExactReadout {
                input: QueryNodeId(0),
                readout: ExactReadout::Sum,
            },
        );
        test_plan::install(&idx, &[(config, vec![1])], vec![entry.clone()]);
        let outcome = execute_query_plan_readout(&idx, &entry, 1000, 2000, true).unwrap();
        // Window-end-only coverage: a single window (1_000, 2_000) is
        // keyed by its end (2_000) alone, so both bounds equal 2_000 --
        // same semantics as `SummaryValue::coverage()`, reconfirmed for
        // `exact_coverage` by this module's A0 test in `summary_executor.rs`.
        assert_eq!(outcome.coverage, Some((2_000, 2_000)));
    }

    // Exercise the compiled plan and runtime bucket assignment against raw integer samples.
    #[test]
    fn compiled_window_schedules_execute_exact_ranges() {
        use crate::precompute_engine::window_manager::WindowManager;
        use control_plane::physical::compiler::{
            BackendLocalPlanningInput, DeploymentPlanCompiler,
        };
        for evaluation_secs in [20, 45, 60, 120, 90] {
            for phase_ms in [0, 5_000] {
                for full in [false, true] {
                    if full && evaluation_secs == 60 {
                        continue;
                    }
                    let mut snapshot: serde_json::Value = serde_json::from_str(include_str!(
                        "../../../../docs/examples/asapquery-planning-snapshot.json"
                    ))
                    .unwrap();
                    let entry = &mut snapshot["query_workload"]["repeating_queries"][0];
                    entry["query"] = serde_json::json!("sum_over_time(a[1m])");
                    entry["requirements"]["accuracy"]["explicit"] = serde_json::json!("Exact");
                    entry["demand"]["fixed_interval_at"] = serde_json::json!({
                        "interval": evaluation_secs * 1_000, "evaluation_phase": phase_ms
                    });
                    let snapshot: BackendLocalPlanningInput =
                        serde_json::from_value(snapshot).unwrap();
                    let (mut request, env) = snapshot.into_physical_compilation_request().unwrap();
                    request.queries[0]
                        .window_realization_candidates
                        .retain(|c| {
                            matches!(
                                c.layout,
                                asap_types::WindowMaterializationLayout::FullWindow
                            ) == full
                        });
                    let plan = DeploymentPlanCompiler.compile_promql(request, env).unwrap();
                    let config = &plan.precompute_plan.materializations[0];
                    let manager = WindowManager::with_layout(
                        config.window_size,
                        config.slide_interval,
                        config.pane_origin_ms,
                        &config.window_layout,
                    );
                    let mut buckets = BTreeMap::<(u64, u64), f64>::new();
                    for second in 1..=800 {
                        for start in manager.stored_bucket_starts(second * 1_000 - 1) {
                            let (_, end) = manager.stored_bucket_bounds(start);
                            if start >= 0 && end <= 800_000 {
                                *buckets.entry((start as u64, end as u64)).or_default() +=
                                    second as f64;
                            }
                        }
                    }
                    let idx = SketchStore::new();
                    idx.register(SummarySeriesMetadata {
                        storage_handle: 7,
                        metric_name: "a".into(),
                        group_by_keys: Default::default(),
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
                        policy_fp: config.policy_fingerprint(),
                    });
                    for (bounds, sum) in buckets {
                        idx.append_precompute(
                            7,
                            BTreeMap::new(),
                            bounds,
                            Box::new(
                                asap_physical_operators::summary_kernels::SumAccumulator::with_sum(
                                    sum,
                                ),
                            ),
                        );
                    }
                    let entry = plan.query_plan.entries.values().next().unwrap();
                    for tick in 3..6 {
                        let end = phase_ms + evaluation_secs * 1_000 * tick;
                        let (outcome, _) = super::super::live_serve::serve_instant_from_query_plan(
                            &idx, entry, end,
                        )
                        .unwrap_or_else(|error| {
                            panic!("E={evaluation_secs} phase={phase_ms} full={full}: {error:?}")
                        });
                        let expected = ((end / 1_000 - 59)..=end / 1_000).sum::<u64>() as f64;
                        assert_eq!(outcome.series[0].1.last().unwrap().1, expected);
                        assert!(super::super::live_serve::serve_instant_from_query_plan(
                            &idx,
                            entry,
                            end + 1
                        )
                        .is_err());
                    }
                }
            }
        }
    }

    // Compile the two readouts, store one pane series, and execute the actual ratio.
    #[test]
    fn compiled_shared_sum_panes_preserve_each_lookback() {
        use control_plane::physical::compiler::{
            BackendLocalPlanningInput, DeploymentPlanCompiler,
        };
        let mut snapshot: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../docs/examples/asapquery-planning-snapshot.json"
        ))
        .unwrap();
        let entry = &mut snapshot["query_workload"]["repeating_queries"][0];
        entry["query"] = serde_json::json!("sum_over_time(a[1m]) / sum_over_time(a[10m])");
        entry["requirements"]["accuracy"]["explicit"] = serde_json::json!("Exact");
        entry["demand"]["fixed_interval_at"]["interval"] = serde_json::json!(60_000);
        let snapshot: BackendLocalPlanningInput = serde_json::from_value(snapshot).unwrap();
        let (request, env) = snapshot.into_physical_compilation_request().unwrap();
        let plan = DeploymentPlanCompiler.compile_promql(request, env).unwrap();
        assert_eq!(plan.precompute_plan.materializations.len(), 1);
        let config = &plan.precompute_plan.materializations[0];
        let policy = config.policy_fingerprint();
        let idx = SketchStore::new();
        idx.register(SummarySeriesMetadata {
            storage_handle: 7,
            metric_name: "a".into(),
            group_by_keys: Default::default(),
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
        for pane in 0..11 {
            idx.append_precompute(
                7,
                BTreeMap::new(),
                (pane * 60_000, (pane + 1) * 60_000),
                Box::new(
                    asap_physical_operators::summary_kernels::SumAccumulator::with_sum(
                        (pane + 1) as f64,
                    ),
                ),
            );
        }
        let entry = plan.query_plan.entries.values().next().unwrap();
        for (now, expected) in [(600_000, 10.0 / 55.0), (660_000, 11.0 / 65.0)] {
            let (outcome, stats) = super::super::logical_dag::execute_installed(
                entry,
                &BTreeMap::new(),
                now,
                |root, at| {
                    let mut subtree = entry.clone();
                    subtree.root = root;
                    let reachable = subtree.topological_order().unwrap();
                    subtree.nodes.retain(|id, _| reachable.contains(id));
                    subtree.instant.lookback_ms = subtree.materialization_bindings()[0]
                        .readout_lookback_ms
                        .unwrap();
                    super::super::live_serve::serve_instant_from_query_plan(&idx, &subtree, at)
                        .map(|(result, _)| {
                            use crate::query_engines::query_result::{InstantVectorElement, QueryResult};
                            let values = result.series.into_iter().map(|(labels, samples)| {
                                let (keys, values) = labels.into_iter().unzip();
                                InstantVectorElement::new(crate::storage_engines::types::KeyByLabelValues::new_with_labels(values), samples.last().unwrap().1)
                                    .with_label_keys_override(keys)
                            }).collect();
                            QueryResult::vector(values, at)
                        })
                        .map_err(|error| {
                            crate::query_engines::EngineError::capability_miss(
                                "test",
                                format!("{error:?}"),
                            )
                        })
                },
            )
            .unwrap();
            let crate::query_engines::query_result::QueryResult::Vector(outcome) = outcome else {
                panic!("expected vector");
            };
            assert_eq!(stats.summary_readout_evaluations, 2);
            assert_eq!(outcome.values.len(), 1);
            assert!((outcome.values[0].value - expected).abs() < 1e-12);
        }
    }

    #[test]
    fn repeated_multi_pane_reads_exclude_expired_state_and_reject_gaps() {
        let idx = SketchStore::new();
        let policy = asap_types::PolicyFingerprint(777);
        idx.register(SummarySeriesMetadata {
            storage_handle: 7,
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
                    asap_physical_operators::summary_kernels::SumAccumulator::with_sum(
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
                            full_window_slide_ms: None,
                            item_labels: Vec::new(),
                            materialization: policy.into(),
                            stored_output_reference:
                                asap_types::sds::StoredOutputReference::for_definition(
                                    policy.into(),
                                ),
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
                Box::new(asap_physical_operators::summary_kernels::SumAccumulator::with_sum(1.0)),
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
        idx.register(SummarySeriesMetadata {
            storage_handle: 7,
            metric_name: "requests_total".into(),
            group_by_keys: std::collections::BTreeSet::new(),
            capability: Some(Capability::ExactAgg(asap_types::AggregationType::Rate)),
            agg_kind: AggKind::ExactAgg {
                agg_type: asap_types::AggregationType::Rate,
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
        let mut accumulator = asap_physical_operators::summary_kernels::IncreaseAccumulator::new(
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
                            full_window_slide_ms: None,
                            item_labels: Vec::new(),
                            materialization: policy.into(),
                            stored_output_reference:
                                asap_types::sds::StoredOutputReference::for_definition(
                                    policy.into(),
                                ),
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
