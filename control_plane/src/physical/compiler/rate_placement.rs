//! Rate-placement variants of Planner heap and grouped Sum candidates.
//!
//! Planner no longer lists these: it treats placement as a lifecycle choice.
//! Until the compiler selects placement through lifecycle timing, it derives
//! the same two variants that Planner used to offer, with timing written into
//! the candidate root, and compiles them as before.
use asap_aware_mapping::{Replacement, ReplacementSubDAG};
use asap_physical_operators::physical_planner::{promql_rows, PhysicalCandidate};
use asap_physical_operators::Error;
use planner_types::post_asap::{
    compile_post_asap_dag, ExactKind, ExecutionTiming, PostAsapOperatorPayload, SketchAlgorithm,
    SummaryExpr, SummaryFamilyType, SummaryNode, ValueOperation,
};
use planner_types::pre_asap::{QueryExpr, Reduction};
use std::rc::Rc;

/// Compile a candidate whose Rate finalization timing is part of its root.
pub(crate) fn compile_fixed_window_rate_aggregation(
    selected: &Rc<SummaryNode>,
) -> Result<PhysicalCandidate, Error> {
    let dag = compile_post_asap_dag(selected).map_err(|e| Error::Invalid(e.to_string()))?;
    promql_rows::compile_fixed_window_rate_aggregation(&dag)
}

/// Fixed-window maintenance finalizes each series' counter state and builds a
/// fresh heap or grouped Sum for that evaluation window. Deployment must
/// provide a complete, synchronized population and bind the matching window;
/// this never adds one window's rates to another.
pub(crate) fn fixed_window_rate_candidates(
    direct: &[ReplacementSubDAG],
    root: &Rc<QueryExpr>,
) -> Vec<ReplacementSubDAG> {
    fn place(node: &Rc<SummaryNode>) -> Option<Rc<SummaryNode>> {
        let mut next = node.as_ref().clone();
        match &mut next.expr {
            SummaryExpr::ValueOperation {
                child,
                operation: ValueOperation::FinalizeExactAccumulator,
                timing,
            } if matches!(&child.expr, SummaryExpr::SummaryAgg {
                    family: SummaryFamilyType::ExactAggregate(ExactKind::Rate, _),
                    reduction: Reduction::PerEntity, child: source, ..
                } if matches!(&source.expr, SummaryExpr::KeepPreAsap(source) if matches!(source.as_ref(), QueryExpr::TimeRange { .. }))) =>
            {
                *timing = ExecutionTiming::IngestionTime;
            }
            SummaryExpr::ValueOperation { child, .. } | SummaryExpr::SummaryAgg { child, .. } => {
                *child = place(child)?
            }
            SummaryExpr::SummaryEstimate { summary_input, .. } => {
                *summary_input = place(summary_input)?
            }
            _ => return None,
        }
        Some(Rc::new(next))
    }
    let mut candidates = direct.to_vec();
    candidates.retain_mut(|candidate| {
        let Replacement::Summary(node) = &candidate.replacement else {
            return false;
        };
        let Ok(dag) = compile_post_asap_dag(node) else {
            return false;
        };
        if !dag.nodes.iter().any(|node| match &node.payload {
            PostAsapOperatorPayload::SummaryAgg {
                family: SummaryFamilyType::Sketch(kind, _),
                ..
            } => matches!(
                kind.algorithm(),
                SketchAlgorithm::CmsWithHeap | SketchAlgorithm::CountSketchWithHeap
            ),
            PostAsapOperatorPayload::SummaryAgg {
                family: SummaryFamilyType::ExactAggregate(ExactKind::Sum, _),
                ..
            } => true,
            _ => false,
        }) {
            return false;
        }
        let Some(placed) = place(node) else {
            return false;
        };
        if compile_post_asap_dag(&placed).is_err() {
            return false;
        }
        let Ok(placed) = asap_aware_mapping::replacement::finalize_query_candidate(placed, root)
        else {
            return false;
        };
        candidate.replacement = Replacement::Summary(placed);
        candidate
            .rationale
            .push_str("; fixed-window precompute over complete per-series counter states");
        true
    });
    candidates
}

/// Retain grouped Sum after a per-series Rate readout as a query-time
/// candidate alongside its complete-window maintenance placement.
pub(crate) fn query_time_rate_aggregation_candidates(
    direct: &[ReplacementSubDAG],
    root: &Rc<QueryExpr>,
) -> Vec<ReplacementSubDAG> {
    fn query_time(node: &Rc<SummaryNode>) -> Rc<SummaryNode> {
        let mut next = node.as_ref().clone();
        match &mut next.expr {
            SummaryExpr::ValueOperation {
                child,
                operation: ValueOperation::FinalizeExactAccumulator,
                timing,
            } if matches!(
                &child.expr,
                SummaryExpr::SummaryAgg {
                    family: SummaryFamilyType::ExactAggregate(ExactKind::Rate, _),
                    ..
                }
            ) =>
            {
                *timing = ExecutionTiming::QueryTime;
            }
            SummaryExpr::ValueOperation { child, .. } | SummaryExpr::SummaryAgg { child, .. } => {
                *child = query_time(child)
            }
            _ => {}
        }
        Rc::new(next)
    }
    let mut candidates = fixed_window_rate_candidates(direct, root);
    candidates.retain_mut(|candidate| {
        let Replacement::Summary(node) = &candidate.replacement else {
            return false;
        };
        if !matches!(&node.expr, SummaryExpr::ValueOperation { child, operation: ValueOperation::FinalizeExactAccumulator, .. }
            if matches!(&child.expr, SummaryExpr::SummaryAgg { family: SummaryFamilyType::ExactAggregate(ExactKind::Sum, _), .. }))
        {
            return false;
        }
        candidate.replacement = Replacement::Summary(query_time(node));
        candidate.rationale = "query-time grouped Sum over complete per-series Rate readouts".into();
        true
    });
    candidates
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_aware_mapping::{ReplacementStrategy, SketchAlgorithmStrategy, TargetSubDAG};
    use planner_types::types::AccuracyTarget;

    fn candidates(query: &str) -> (Vec<ReplacementSubDAG>, Vec<ReplacementSubDAG>) {
        let root =
            crate::query_parser::parse_query_expr_canonical(query, AccuracyTarget::Exact).unwrap();
        let typed = Rc::new(promql_rows::with_series_identity(&root).unwrap());
        let strategy =
            SketchAlgorithmStrategy::new(&asap_aware_mapping::cost_model::DefaultCostModel);
        let direct = strategy.propose(&TargetSubDAG::new(&typed)).candidates;
        (
            fixed_window_rate_candidates(&direct, &typed),
            query_time_rate_aggregation_candidates(&direct, &typed),
        )
    }

    fn rate_timing(node: &Rc<SummaryNode>) -> Option<ExecutionTiming> {
        match &node.expr {
            SummaryExpr::ValueOperation {
                child,
                operation: ValueOperation::FinalizeExactAccumulator,
                timing,
            } if matches!(
                &child.expr,
                SummaryExpr::SummaryAgg {
                    family: SummaryFamilyType::ExactAggregate(ExactKind::Rate, _),
                    ..
                }
            ) =>
            {
                Some(*timing)
            }
            SummaryExpr::ValueOperation { child, .. } | SummaryExpr::SummaryAgg { child, .. } => {
                rate_timing(child)
            }
            SummaryExpr::SummaryEstimate { summary_input, .. } => rate_timing(summary_input),
            _ => None,
        }
    }

    // Grouped Sum over Rate yields a fixed-window precompute placement and a
    // query-time placement of the same computation.
    #[test]
    fn grouped_rate_sum_has_both_placements() {
        let (fixed, query_time) = candidates("sum by (job) (rate(requests_total[1m]))");
        let [fixed] = fixed.as_slice() else {
            panic!("expected one fixed-window candidate, got {}", fixed.len());
        };
        let Replacement::Summary(fixed) = &fixed.replacement else {
            panic!("expected a summary replacement");
        };
        assert_eq!(rate_timing(fixed), Some(ExecutionTiming::IngestionTime));
        let physical = compile_fixed_window_rate_aggregation(fixed).unwrap();
        assert!(physical.precompute.is_some());

        let [query_time] = query_time.as_slice() else {
            panic!(
                "expected one query-time candidate, got {}",
                query_time.len()
            );
        };
        let Replacement::Summary(query_time) = &query_time.replacement else {
            panic!("expected a summary replacement");
        };
        assert_eq!(rate_timing(query_time), Some(ExecutionTiming::QueryTime));
        assert!(compile_fixed_window_rate_aggregation(query_time).is_err());
    }

    // Candidates without a heap or grouped Sum have no Rate placement variant.
    #[test]
    fn per_series_rate_has_no_placement_variant() {
        let (fixed, query_time) = candidates("rate(requests_total[1m])");
        assert!(fixed.is_empty());
        assert!(query_time.is_empty());
    }
}
