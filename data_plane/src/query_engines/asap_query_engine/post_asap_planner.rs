//! PromQL string → `planner_types::post_asap::SummaryNode` bridge, shared by the actual
//! serving cutover (`live_serve.rs`, via `post_asap_readout.rs`).
//!
//! `control_plane` runs in-process with `data_plane` in this deployment
//! (see `data_plane/Cargo.toml`'s "Phase 9" comment), so this is a
//! same-binary library call, not a new planning implementation living
//! here.
//!
//! ## Serving time must not re-plan
//!
//! Parsing and canonicalization are safe to re-run at serving time: they are
//! pure, deterministic transformations of the query text. Selecting the
//! post-ASAP implementation (which summary family and parameters to use) is
//! a genuine planning decision, and planning already made it once, for
//! real, when this metric's workload was planned — that decision is what
//! `data_plane`'s ingest path actually registered in the `SketchStore`
//! (`AggKind::Sketch { algorithm: kind, config, .. }`). Serving time must reproduce
//! THAT decision, not independently re-derive a fresh one from a
//! hardcoded accuracy target: doing so picks whatever family/params an
//! accuracy-driven cost model prefers in the abstract (e.g. DDSketch
//! over Kll for quantiles, unconditionally), with no guarantee it matches
//! what's actually registered — and `SummaryExecutor::find_candidates`
//! requires an exact `(SketchAlgorithm, SketchParams)` match, by design (see
//! `summary_executor.rs::summary_params_match`'s doc: this deployment
//! chose strict equality over silently serving an answer under a looser
//! guarantee than what was planned).
//!
//! So before binding, [`plan_promql_to_post_asap`] looks up what's actually
//! registered for the query's target metric and constructs an
//! [`ObservedFamilyCostModel`] that echoes that back — the resulting
//! `SummaryNode` matches reality by construction, not by a coincidental
//! accuracy-target match. When nothing is registered for the metric (or
//! this deployment's family/param mapping doesn't recognize the
//! registered shape), `observed` is `None` and binding falls back to the
//! same accuracy-driven `ControlPlaneCostModel` behavior as before — it
//! won't find a match either way, so the outcome (`find_candidates` finds
//! nothing) is unchanged, just for a more honest reason.

use std::rc::Rc;

use planner_types::post_asap::{SketchAlgorithm, SketchParams, SummaryExpr, SummaryNode};

use control_plane::physical::post_asap::cost_model::ObservedFamilyCostModel;
use control_plane::physical::post_asap::{
    bind_query_expr_with_cost_model, BindingError, PhysicalExpr, PostAsapPlan,
};
use control_plane::types_v2::AccuracyTarget;

use crate::query_engines::asap_query_engine::summary_executor::find_metric_in_query_expr;
use crate::storage_engines::sketch_db::data::{AggKind, SketchConfig};
use crate::storage_engines::sketch_db::index::SketchStore;

/// Why a query couldn't be answered through the `SummaryNode`/`SummaryExecutor`
/// path — covers both `plan_promql_to_post_asap`'s own failure to produce a
/// tree, AND (via `post_asap_readout.rs`'s `execute_post_asap_readout`) a failure of
/// `crate::query_engines::asap_query_engine::summary_exec::execute()` on a tree that DID lower successfully.
/// None of these are errors in the alarming sense — every variant is an
/// expected, frequent outcome for *some* fraction of live traffic; the
/// caller's only obligation is "fall back to the legacy path," never
/// "log this as a problem."
#[derive(Debug)]
pub enum LoweringSkip {
    /// Operational kill switch disabled warm DAG execution.
    Disabled,
    /// No exact identity exists in the active, control-plane-compiled
    /// QueryPlan. This is a catalog miss, not an invitation to re-plan.
    QueryNotPlanned(String),
    /// The installed QueryPlan entry could not be reconstructed or failed
    /// its internal contract. The request must fail closed.
    InvalidQueryPlan(String),
    /// The QueryPlan binding exists, but its warm materializations do not yet
    /// cover the requested evaluation interval with complete, fresh windows.
    MaterializationNotReady(String),
    /// `parse_query_expr_canonical` failed — same failure mode the legacy
    /// parsing path tolerates and reports as an archive fallback reason.
    ParseFailed(String),
    /// The query contains `rate(...)`/`irate(...)` (`OuterFn::Rate`, per
    /// `control_plane::asap_tier_analysis`'s own candidate analysis).
    /// `lower.rs`'s `bind_recursive` rewrites `AggIntent::Rate` →
    /// `Increase` before binding, so this WOULD otherwise bind
    /// successfully to a valid `SummaryAgg{Increase}` tree — but
    /// `summary_executor.rs` has no rate-division logic (dividing by a
    /// coverage-clamped range), so comparing against it would produce a
    /// spurious mismatch, not a real one. Must be excluded before ever
    /// calling into `control_plane`'s binder, not just deprioritized.
    RateShape,
    /// An exact Sum materialization stores counter deltas and cannot answer
    /// PromQL's sum-over-cumulative-samples semantics.
    CounterSumOverTime,
    /// The current CMS/CountSketch serving adapter cannot safely resolve a
    /// string-keyed point lookup from the maintained state.
    KeyedFrequency,
    /// `bind_query_expr` itself failed (a genuine `BindingError`, e.g.
    /// post-ASAP implementation/schema-derivation failure).
    Implement(String),
    /// `bind_query_expr` returned a `PhysicalExpr` variant other than
    /// a committed post-ASAP summary. Per `bind_query_expr`'s own doc
    /// this shouldn't happen in practice (it never picks a Phase ε.1
    /// placement), but the match is kept exhaustive and defensive rather
    /// than assuming.
    UnsupportedPhysicalShape,
    /// The root node is `SummaryExpr::Logical(_)` — the query didn't
    /// realize to any sketch/exact-agg binding at all (e.g.
    /// `topk(K, sum by(...)(rate(m[r])))`: `implement_tree_in_with` only
    /// recurses through `Aggregate` nodes, so hitting the outer
    /// `Sort`/`Limit` wraps the WHOLE tree as one opaque `Logical` blob
    /// even though the inner aggregate would bind fine on its own — see
    /// this crate's design doc). Not an error: this is exactly the
    /// existing `SummaryExecutorError::Logical`/"no candidate bound"
    /// outcome, just detected one step earlier so the caller can skip
    /// without even constructing a `QueryExecutionContext`.
    NotRealized,
    /// ASAPPlanner produced a maintained-summary plan, but no installed
    /// QueryPlan binding has a ready materialization with the required
    /// source, family and parameters.
    NoWarmRoute(String),
    /// The tree lowered successfully, but `crate::query_engines::asap_query_engine::summary_exec::execute()`
    /// itself returned `Err` (`NoCandidates`, `MergeKindParamsMismatch`,
    /// a decode/merge failure surfaced from `summary_executor.rs`, ...).
    /// Always safe to just fall back — this means "can't answer this way
    /// right now" (e.g. the sid catalog doesn't have an exact
    /// `(SketchAlgorithm, SketchParams)` match), never "answered wrong."
    ExecuteFailed(String),
}

// `PostAsapPlan` and `PhysicalExpr` are backend compatibility/placement wrappers.
// The semantic tree returned by ASAPPlanner is `post_asap::SummaryNode`; this
// module does not claim or recreate an ASAPPlanner "L4" IR.

/// Map a registered sid's `(SketchAlgorithm, SketchConfig)` — the
/// durable record of what planning actually decided for this metric — to
/// the `(SketchAlgorithm, SketchParams)` pair `ObservedFamilyCostModel`
/// needs to reproduce that decision exactly. `None` for shapes this
/// deployment doesn't map (e.g. an unsupported algorithm, which is an
/// analysis-time wildcard that's never actually registered on a sid).
///
/// Heap-bearing kinds (`CmsWithHeap`/`CountSketchWithHeap`) reuse their
/// heap-less base's `SketchConfig` shape for identity (no `heap_size`
/// field exists on `SketchConfig` at all — mirrors `to_delta_kind`'s same
/// note), so `heap_size` here is a placeholder; `summary_params_match`
/// only compares `width`/`depth` for these kinds, so it doesn't affect
/// matching.
fn observed_summary_params(
    kind: SketchAlgorithm,
    config: &SketchConfig,
) -> Option<(SketchAlgorithm, SketchParams)> {
    const PLACEHOLDER_HEAP_SIZE: u32 = 100;
    match (kind, config) {
        (SketchAlgorithm::DDSketch, SketchConfig::DDSketch { relative_accuracy }) => Some((
            SketchAlgorithm::DDSketch,
            SketchParams::DDSketch {
                alpha: *relative_accuracy,
            },
        )),
        (SketchAlgorithm::Kll, SketchConfig::Kll { k }) => {
            Some((SketchAlgorithm::Kll, SketchParams::Kll { k: *k }))
        }
        (SketchAlgorithm::Hll, SketchConfig::Hll { precision }) => Some((
            SketchAlgorithm::Hll,
            SketchParams::Hll {
                precision: *precision as u8,
            },
        )),
        (SketchAlgorithm::Cms, SketchConfig::CountMin { rows, cols }) => Some((
            SketchAlgorithm::Cms,
            SketchParams::Cms {
                width: *cols as u32,
                depth: *rows as u32,
            },
        )),
        (SketchAlgorithm::CmsWithHeap, SketchConfig::CountMin { rows, cols }) => Some((
            SketchAlgorithm::CmsWithHeap,
            SketchParams::CmsWithHeap {
                width: *cols as u32,
                depth: *rows as u32,
                heap_size: PLACEHOLDER_HEAP_SIZE,
            },
        )),
        (SketchAlgorithm::CountSketch, SketchConfig::CountSketch { rows, cols }) => Some((
            SketchAlgorithm::CountSketch,
            SketchParams::CountSketch {
                width: *cols as u32,
                depth: *rows as u32,
            },
        )),
        (SketchAlgorithm::CountSketchWithHeap, SketchConfig::CountSketch { rows, cols }) => Some((
            SketchAlgorithm::CountSketchWithHeap,
            SketchParams::CountSketchWithHeap {
                width: *cols as u32,
                depth: *rows as u32,
                heap_size: PLACEHOLDER_HEAP_SIZE,
            },
        )),
        _ => None,
    }
}

/// Look up what family/params is ACTUALLY registered for `metric` in
/// `index` — the durable record of planning's real decision (see this
/// module's docs). Checks every sid registered for the metric (no
/// group-by filter — an empty `required_keys` set matches any
/// registration, since we only need to know the FAMILY here, not resolve
/// a specific series) and returns the first sketch-typed one found.
/// `None` when nothing is registered (or only `ExactAgg` sids are —
/// those never consult `CostModel` at all, so there's nothing to
/// observe for them).
fn observed_family_for_metric(
    index: &SketchStore,
    metric: &str,
) -> Option<(SketchAlgorithm, SketchParams)> {
    for sid in index.instances_matching(metric, &Default::default()) {
        let found = index.with_instance(sid, |m| match &m.agg_kind {
            AggKind::Sketch {
                algorithm: kind,
                config,
                ..
            } => observed_summary_params(kind.clone(), config),
            AggKind::ExactAgg { .. } => None,
        });
        if let Some(Some(observed)) = found {
            return Some(observed);
        }
    }
    None
}

fn query_expr_contains_time_range(qe: &planner_types::pre_asap::QueryExpr) -> bool {
    use planner_types::pre_asap::QueryExpr;
    match qe {
        QueryExpr::TimeRange { .. } => true,
        QueryExpr::Filter { child, .. }
        | QueryExpr::Project { child, .. }
        | QueryExpr::Aggregate { child, .. }
        | QueryExpr::Dedup { child, .. }
        | QueryExpr::Sort { child, .. }
        | QueryExpr::Limit { child, .. }
        | QueryExpr::PromqlSubquery { child, .. }
        | QueryExpr::TimeShift { child, .. }
        | QueryExpr::SQLWindowFunc { child, .. } => query_expr_contains_time_range(child),
        QueryExpr::Concat { children, .. } => children.iter().any(query_expr_contains_time_range),
        QueryExpr::Join { left, right, .. }
        | QueryExpr::SetOp { left, right, .. }
        | QueryExpr::BinaryOp {
            lhs: left,
            rhs: right,
            ..
        } => query_expr_contains_time_range(left) || query_expr_contains_time_range(right),
        _ => false,
    }
}

fn query_expr_contains_rate(qe: &planner_types::pre_asap::QueryExpr) -> bool {
    use planner_types::pre_asap::{AggIntent, QueryExpr};
    match qe {
        QueryExpr::Aggregate {
            measures, child, ..
        } => {
            measures.iter().any(|m| matches!(m, AggIntent::Rate)) || query_expr_contains_rate(child)
        }
        QueryExpr::Filter { child, .. }
        | QueryExpr::Project { child, .. }
        | QueryExpr::Dedup { child, .. }
        | QueryExpr::Sort { child, .. }
        | QueryExpr::Limit { child, .. }
        | QueryExpr::PromqlSubquery { child, .. }
        | QueryExpr::TimeRange { child, .. }
        | QueryExpr::TimeShift { child, .. }
        | QueryExpr::SQLWindowFunc { child, .. } => query_expr_contains_rate(child),
        QueryExpr::Concat { children, .. } => children.iter().any(query_expr_contains_rate),
        QueryExpr::Join { left, right, .. }
        | QueryExpr::SetOp { left, right, .. }
        | QueryExpr::BinaryOp {
            lhs: left,
            rhs: right,
            ..
        } => query_expr_contains_rate(left) || query_expr_contains_rate(right),
        _ => false,
    }
}

fn query_expr_has_filter(qe: &planner_types::pre_asap::QueryExpr) -> bool {
    use planner_types::pre_asap::QueryExpr;
    match qe {
        QueryExpr::Scan { predicates, .. } => !predicates.is_empty(),
        QueryExpr::Filter { .. } => true,
        QueryExpr::Project { child, .. }
        | QueryExpr::Aggregate { child, .. }
        | QueryExpr::Dedup { child, .. }
        | QueryExpr::Sort { child, .. }
        | QueryExpr::Limit { child, .. }
        | QueryExpr::PromqlSubquery { child, .. }
        | QueryExpr::TimeRange { child, .. }
        | QueryExpr::TimeShift { child, .. }
        | QueryExpr::SQLWindowFunc { child, .. } => query_expr_has_filter(child),
        QueryExpr::Concat { children, .. } => children.iter().any(query_expr_has_filter),
        QueryExpr::Join { left, right, .. }
        | QueryExpr::SetOp { left, right, .. }
        | QueryExpr::BinaryOp {
            lhs: left,
            rhs: right,
            ..
        } => query_expr_has_filter(left) || query_expr_has_filter(right),
        _ => false,
    }
}

fn query_expr_max_time_range_ms(qe: &planner_types::pre_asap::QueryExpr) -> Option<u64> {
    use planner_types::pre_asap::QueryExpr;
    let child_max =
        |child: &planner_types::pre_asap::QueryExpr| query_expr_max_time_range_ms(child);
    match qe {
        QueryExpr::TimeRange { range, child } => {
            Some((range.as_millis() as u64).max(child_max(child).unwrap_or_default()))
        }
        QueryExpr::PromqlSubquery { range, child, .. } => {
            Some((range.as_millis() as u64).max(child_max(child).unwrap_or_default()))
        }
        QueryExpr::Filter { child, .. }
        | QueryExpr::Project { child, .. }
        | QueryExpr::Aggregate { child, .. }
        | QueryExpr::Dedup { child, .. }
        | QueryExpr::Sort { child, .. }
        | QueryExpr::Limit { child, .. }
        | QueryExpr::TimeShift { child, .. }
        | QueryExpr::SQLWindowFunc { child, .. } => child_max(child),
        QueryExpr::Concat { children, .. } => children
            .iter()
            .filter_map(query_expr_max_time_range_ms)
            .max(),
        QueryExpr::Join { left, right, .. }
        | QueryExpr::SetOp { left, right, .. }
        | QueryExpr::BinaryOp {
            lhs: left,
            rhs: right,
            ..
        } => child_max(left).into_iter().chain(child_max(right)).max(),
        _ => None,
    }
}

fn summary_contains_time_range(node: &SummaryNode) -> bool {
    match &node.expr {
        SummaryExpr::KeepPreAsap(qe) => query_expr_contains_time_range(qe),
        SummaryExpr::SummaryAgg { child, .. } => summary_contains_time_range(child),
        SummaryExpr::SummaryEstimate { summary_input, .. } => {
            summary_contains_time_range(summary_input)
        }
        SummaryExpr::SummaryMerge { children } => {
            children.iter().any(|n| summary_contains_time_range(n))
        }
        _ => false,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PostAsapExecutionHints {
    pub lookback_ms: Option<u64>,
    pub cumulative_readout: bool,
    pub full_history: bool,
}

/// Derive execution-time window semantics from the Planner DAG rather than
/// from a second PromQL capability analyzer.
pub fn execution_hints(node: &SummaryNode) -> PostAsapExecutionHints {
    use planner_types::post_asap::{ExactKind, SketchQuery, SummaryFamilyType};

    fn visit(
        node: &SummaryNode,
        lookback_ms: &mut Option<u64>,
        has_cardinality: &mut bool,
        has_plain_sum: &mut bool,
    ) {
        match &node.expr {
            SummaryExpr::KeepPreAsap(qe) => {
                if let Some(range) = query_expr_max_time_range_ms(qe) {
                    *lookback_ms = Some(lookback_ms.unwrap_or_default().max(range));
                }
            }
            SummaryExpr::SummaryAgg { child, family, .. } => {
                if matches!(family, SummaryFamilyType::ExactAggregate(ExactKind::Sum, _)) {
                    *has_plain_sum = true;
                }
                visit(child, lookback_ms, has_cardinality, has_plain_sum);
            }
            SummaryExpr::SummaryEstimate {
                summary_input,
                query,
            } => {
                *has_cardinality |= matches!(query, SketchQuery::Cardinality);
                visit(summary_input, lookback_ms, has_cardinality, has_plain_sum);
            }
            SummaryExpr::SummaryMerge { children } => {
                for child in children {
                    visit(child, lookback_ms, has_cardinality, has_plain_sum);
                }
            }
            _ => {}
        }
    }

    let mut lookback_ms = None;
    let mut has_cardinality = false;
    let mut has_plain_sum = false;
    visit(
        node,
        &mut lookback_ms,
        &mut has_cardinality,
        &mut has_plain_sum,
    );
    PostAsapExecutionHints {
        lookback_ms,
        cumulative_readout: lookback_ms.is_some() || has_cardinality,
        full_history: lookback_ms.is_none() && has_plain_sum,
    }
}

/// Runtime support is derived from the Planner DAG itself. This replaces the
/// former PromQL candidate analyzer gate, so unsupported semantics cannot
/// diverge from the plan the executor will actually run.
fn ensure_warm_runtime_support(
    node: &SummaryNode,
    source_has_filter: bool,
) -> Result<(), LoweringSkip> {
    use planner_types::post_asap::{ExactKind, SketchQuery, SummaryFamilyType};
    match &node.expr {
        SummaryExpr::SummaryAgg { child, family, .. } => {
            match family {
                SummaryFamilyType::ExactAggregate(ExactKind::Rate, _) => {
                    return Err(LoweringSkip::RateShape)
                }
                SummaryFamilyType::ExactAggregate(ExactKind::Sum, _)
                    if summary_contains_time_range(child) =>
                {
                    return Err(LoweringSkip::CounterSumOverTime)
                }
                _ => {}
            }
            ensure_warm_runtime_support(child, source_has_filter)
        }
        SummaryExpr::SummaryEstimate {
            summary_input,
            query: SketchQuery::PointCount { value: None, .. },
        } if source_has_filter => Err(LoweringSkip::KeyedFrequency),
        SummaryExpr::SummaryEstimate { summary_input, .. } => {
            ensure_warm_runtime_support(summary_input, source_has_filter)
        }
        SummaryExpr::SummaryMerge { children } => {
            for child in children {
                ensure_warm_runtime_support(child, source_has_filter)?;
            }
            Ok(())
        }
        SummaryExpr::KeepPreAsap(_) => Ok(()),
        _ => Err(LoweringSkip::NoWarmRoute(
            "post-ASAP operator is not executable by the warm runtime".into(),
        )),
    }
}

/// Bind the conventional PromQL `item="..."` equality matcher to a frequency
/// point readout. The Planner DAG owns the `SketchQuery`; this adapter only
/// supplies the literal value that the PromQL frontend currently leaves as
/// `None`.
fn bind_point_count_filter(node: &mut Rc<SummaryNode>, key: &str, value: &str) -> bool {
    let node = Rc::make_mut(node);
    match &mut node.expr {
        SummaryExpr::SummaryEstimate {
            query:
                planner_types::post_asap::SketchQuery::PointCount {
                    key: point_key,
                    value: point_value,
                },
            ..
        } if point_value.is_none() => {
            *point_key = planner_types::pre_asap::ColumnRef::Named(key.to_string());
            *point_value = Some(value.to_string());
            true
        }
        SummaryExpr::SummaryAgg { child, .. } => bind_point_count_filter(child, key, value),
        SummaryExpr::SummaryEstimate { summary_input, .. } => {
            bind_point_count_filter(summary_input, key, value)
        }
        SummaryExpr::SummaryMerge { children } => children
            .iter_mut()
            .any(|child| bind_point_count_filter(child, key, value)),
        _ => false,
    }
}

/// Lower a raw PromQL query string to the `SummaryNode` tree
/// `crate::query_engines::asap_query_engine::summary_exec::execute`/`SummaryExecutor` needs — the actual
/// serving cutover (`live_serve.rs`). Returns `Err` for any shape serving
/// shouldn't attempt (parse failure, `rate()`, or anything that doesn't
/// realize to a concrete sketch/exact-agg binding) — see
/// `LoweringSkip`'s variants.
pub fn plan_promql_to_post_asap(
    index: &SketchStore,
    query: &str,
    accuracy: AccuracyTarget,
) -> Result<Rc<SummaryNode>, LoweringSkip> {
    let qe = control_plane::query_parser::parse_query_expr_canonical(query, accuracy.clone())
        .map_err(|e| LoweringSkip::ParseFailed(e.to_string()))?;
    if query_expr_contains_rate(&qe) {
        return Err(LoweringSkip::RateShape);
    }
    let source_has_filter = query_expr_has_filter(&qe);

    // This dynamic lowering path is retained for isolated executor tests.
    // Production serving executes the installed QueryPlan and resolves its
    // MaterializationId bindings through SummaryCatalog.
    let metric = find_metric_in_query_expr(&qe);
    let mut observed = Vec::new();
    if let Some(family) = metric
        .as_deref()
        .and_then(|metric| observed_family_for_metric(index, metric))
    {
        observed.push(family);
    }
    // No observed family means the test-only planner may use its
    // accuracy-driven default.
    let candidates: Vec<_> = if observed.is_empty() {
        vec![None]
    } else {
        observed.into_iter().map(Some).collect()
    };
    let mut last_skip = LoweringSkip::NotRealized;
    for observed in candidates {
        let cost_model = ObservedFamilyCostModel::new(accuracy.clone(), observed);
        let physical = match bind_query_expr_with_cost_model(&qe, &cost_model) {
            Ok(physical) => physical,
            Err(BindingError::Implement(
                control_plane::planner_selection::SelectionError::NoLegalCandidate,
            )) => {
                last_skip = LoweringSkip::NotRealized;
                continue;
            }
            Err(other) => {
                last_skip = LoweringSkip::Implement(other.to_string());
                continue;
            }
        };

        match physical {
            PhysicalExpr::Committed(PostAsapPlan::Summary(mut node)) => {
                if matches!(node.expr, SummaryExpr::KeepPreAsap(_)) {
                    last_skip = LoweringSkip::NotRealized;
                } else {
                    if let Ok(parsed) =
                        control_plane::query_parser::parse_query(query, accuracy.clone())
                    {
                        if parsed.label_filters.len() == 1 {
                            if let Some(value) = parsed.label_filters.get("item") {
                                bind_point_count_filter(&mut node, "item", value);
                            }
                        }
                    }
                    if let Err(skip) = ensure_warm_runtime_support(&node, source_has_filter) {
                        last_skip = skip;
                        continue;
                    }
                    return Ok(node);
                }
            }
            _ => last_skip = LoweringSkip::UnsupportedPhysicalShape,
        }
    }
    Err(last_skip)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn accuracy() -> AccuracyTarget {
        AccuracyTarget::Epsilon(0.01)
    }

    /// Empty index -- every test below exercises a shape that either
    /// self-excludes before ever consulting the `SketchStore`, or (for
    /// `frequency_intent_realizes_via_bind_query_expr`) relies on
    /// `ObservedFamilyCostModel` falling back to the accuracy-driven
    /// default when nothing is registered.
    fn empty_index() -> SketchStore {
        SketchStore::new()
    }

    #[test]
    fn rate_query_is_skipped_before_binding() {
        let idx = empty_index();
        let result = plan_promql_to_post_asap(&idx, "rate(http_requests_total[5m])", accuracy());
        assert!(
            matches!(result, Err(LoweringSkip::RateShape)),
            "expected RateShape, got {result:?}"
        );
    }

    #[test]
    fn irate_query_is_skipped_before_binding() {
        let idx = empty_index();
        let result = plan_promql_to_post_asap(&idx, "irate(http_requests_total[5m])", accuracy());
        assert!(
            matches!(result, Err(LoweringSkip::RateShape)),
            "expected RateShape, got {result:?}"
        );
    }

    #[test]
    fn unparseable_query_is_skipped() {
        let idx = empty_index();
        let result = plan_promql_to_post_asap(&idx, "this is not promql (((", accuracy());
        assert!(
            matches!(result, Err(LoweringSkip::ParseFailed(_))),
            "expected ParseFailed, got {result:?}"
        );
    }

    #[test]
    fn bare_selector_is_not_realized() {
        // L1 adoption (design-target-architecture.md Part B), accepted
        // behavior change: `asap_frontend_promql::lower_promql` no longer
        // wraps a bare selector in an implicit `Aggregate { Sum }` (see
        // control_plane's
        // `asap_tier_implement::bare_selector_has_no_aggregate_root_to_implement`
        // and `asap_tier_analysis::bare_selector_is_no_longer_asap_tier_answerable`
        // for the sibling fixes). With no `Aggregate` node anywhere in the
        // tree, `implement_tree_in_with` has nothing to bind and the whole
        // expression stays one opaque `Logical` blob, which this module
        // surfaces as `NotRealized`.
        let idx = empty_index();
        let result = plan_promql_to_post_asap(&idx, "http_requests_total", accuracy());
        assert!(
            matches!(result, Err(LoweringSkip::NotRealized)),
            "expected NotRealized, got {result:?}"
        );
    }

    #[test]
    fn frequency_intent_realizes_via_bind_query_expr() {
        // The exact shape `asap_tier_implement.rs`'s own
        // `implement_frequency_as_agg_test` pins as a KNOWN, documented gap
        // for `implement_promql_for_asap_tier`/`DefaultCostModel` (asserts
        // it stays `Logical` "for now"). `bind_query_expr`/
        // `ControlPlaneCostModel` is exactly the fix -- via
        // `realize_extension`/`readout_extension` (ASAPController#150) --
        // so this must realize to a real binding here, confirming this
        // module picked the seam that actually handles Frequency. No sid
        // is registered for this metric, so `ObservedFamilyCostModel`
        // falls back to the accuracy-driven default -- same outcome as
        // before this module started consulting the `SketchStore`.
        let idx = empty_index();
        let node =
            plan_promql_to_post_asap(&idx, "count_over_time(http_requests_total[5m])", accuracy())
                .expect("Frequency intent must realize via bind_query_expr/ControlPlaneCostModel");
        assert!(
            !matches!(node.expr, SummaryExpr::KeepPreAsap(_)),
            "expected a real SummaryAgg/SummaryEstimate binding, got Logical (the gap \
             this module exists to avoid): {:?}",
            node.expr
        );
    }

    #[test]
    fn execution_hints_come_from_post_asap_dag() {
        let idx = empty_index();
        let ranged =
            plan_promql_to_post_asap(&idx, "quantile_over_time(0.99, latency[5m])", accuracy())
                .expect("quantile plan");
        assert_eq!(
            execution_hints(&ranged),
            PostAsapExecutionHints {
                lookback_ms: Some(300_000),
                cumulative_readout: true,
                full_history: false,
            }
        );

        let plain_sum =
            plan_promql_to_post_asap(&idx, "sum(requests)", accuracy()).expect("sum plan");
        assert_eq!(
            execution_hints(&plain_sum),
            PostAsapExecutionHints {
                lookback_ms: None,
                cumulative_readout: false,
                full_history: true,
            }
        );
    }

    #[test]
    fn topk_over_rate_is_not_realized() {
        // The outer `Sort{Limit{Aggregate}}` shape: `implement_tree_in_with`
        // only recurses through `Aggregate`, so the whole tree wraps as
        // one opaque `Logical` blob -- self-excludes via `NotRealized`,
        // no special-case detection needed for this shape specifically.
        let idx = empty_index();
        let result = plan_promql_to_post_asap(
            &idx,
            "topk(5, sum by (host) (rate(http_requests_total[5m])))",
            accuracy(),
        );
        assert!(
            matches!(result, Err(LoweringSkip::NotRealized) | Err(LoweringSkip::RateShape)),
            "expected NotRealized or RateShape (both are valid skips for this shape), got {result:?}"
        );
    }
}
