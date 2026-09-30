//! Installed query-time leaves: raw selectors, exact subtrees and
//! current-series readouts. Computation over their values is Planner-compiled.
pub use asap_types::query_plan::query_time::*;

use planner_types::post_asap::{ExactKind, SummaryExpr, SummaryFamilyType, SummaryNode};
use planner_types::pre_asap::{QueryExpr, Reduction, Source};

/// A per-series `max_over_time` over one plain range selector, the only
/// maximum state the backend maintains. Planner distinguishes Max from Min
/// in the family, so the selected node alone identifies it.
pub(crate) fn is_range_max_materialization(node: &SummaryNode) -> bool {
    let node = match &node.expr {
        SummaryExpr::ValueOperation {
            child,
            operation: planner_types::post_asap::ValueOperation::FinalizeExactAccumulator,
            ..
        } => child.as_ref(),
        _ => node,
    };
    let SummaryExpr::SummaryAgg {
        family: SummaryFamilyType::ExactAggregate(ExactKind::Max, _),
        reduction: Reduction::PerEntity,
        child,
        ..
    } = &node.expr
    else {
        return false;
    };
    matches!(
        &child.expr,
        SummaryExpr::KeepPreAsap(expr) if matches!(
            expr.as_ref(),
            QueryExpr::TimeRange { child, .. } if matches!(
                child.as_ref(),
                QueryExpr::Scan { source: Source::TimeSeries { metric }, .. } if !metric.is_empty()
            )
        )
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn selected(query: &str) -> std::rc::Rc<SummaryNode> {
        let original = crate::query_parser::parse_query_expr_canonical(
            query,
            planner_types::types::AccuracyTarget::Exact,
        )
        .unwrap();
        crate::planner_selection::plan_test_query(&original).unwrap()
    }

    // Plain gauge maxima are maintained; minima, offsets and nested windows are not.
    #[test]
    fn range_max_materialization_is_identified_from_the_selected_node() {
        for query in [
            r#"max_over_time(service_cache_refresh_lag_seconds{job="user-service"}[12h])"#,
            r#"max_over_time(service_retry_queue_depth{job=~".+"}[6h])"#,
        ] {
            assert!(is_range_max_materialization(&selected(query)), "{query}");
        }
        for query in [
            "min_over_time(m[1m])",
            "max_over_time(m[1m] offset 1m)",
            "max_over_time((m + m)[1m:1s])",
        ] {
            assert!(!is_range_max_materialization(&selected(query)), "{query}");
        }
    }
}
