use control_plane::{
    physical::post_asap::{bind_query_expr, PhysicalExpr, PostAsapPlan},
    query_parser::parse_query_expr_canonical,
    types_v2::AccuracyTarget,
};
use planner_types::post_asap::SummaryExpr;

/// Non-summary o11y roots preserve labels, ordering and comparison filtering
/// as the original exact IR, instead of failing summary candidate selection.
#[test]
fn o11y_non_summary_roots_preserve_exact_query_semantics() {
    for query in [
        "service_cache_refresh_lag_seconds{job=\"user-service\"}",
        "sort_desc(sum by (job) (rate(process_cpu_seconds_total{job=~\".+\"}[6h])))",
        "sum by (job) (increase(http_requests_total{status=~\"5..\",job=~\".+-service\"}[24h])) > 0",
    ] {
        for accuracy in [AccuracyTarget::Exact, AccuracyTarget::Epsilon(0.01)] {
            let expr = parse_query_expr_canonical(query, accuracy.clone()).unwrap();
            let result = bind_query_expr(&expr, accuracy)
                .unwrap_or_else(|error| panic!("{query}: {error}"));
            let PhysicalExpr::Committed(PostAsapPlan::Summary(node)) = result else {
                panic!("expected an explicit exact fallback for {query}");
            };
            let SummaryExpr::KeepPreAsap(original) = &node.expr else {
                panic!("expected original exact query for {query}");
            };
            assert_eq!(original.as_ref(), &expr, "query semantics changed: {query}");
            assert_eq!(node.schema.fields.len(), expr.output_schema().unwrap().columns.len());
            let executable = control_plane::query_plan::QueryPlanEntry::compile_bound(
                "fixture".into(), query.into(), &node,
                control_plane::query_plan::InstantExecution {
                    lookback_ms: 300_000, full_history: false, cumulative_readout: false,
                },
                control_plane::query_plan::FallbackPolicy::Reject,
                |_, _| panic!("exact fallback must not request summary materializations"),
            ).unwrap();
            assert!(matches!(executable.nodes[&executable.root],
                control_plane::query_plan::QueryPlanNode::ExactFallback { .. }));
            assert!(executable.materialization_bindings().is_empty());
        }
    }
}
