use super::*;
use control_plane::physical::compiler::BackendLocalPlanningInput;

/// HLL's relative standard error alone does not certify a confidence bound.
#[tokio::test]
async fn distinct_range_without_confidence_uses_exact_fallback() {
    let mut fixture: Value = serde_json::from_str(include_str!(
        "../../../docs/examples/asapquery-compatibility-demo-snapshot.json"
    ))
    .unwrap();
    let mut entry = fixture["query_workload"]["repeating_queries"][3].clone();
    entry["query"] = "distinct_over_time(distinct_values{job=\"api\"}[5s])".into();
    entry["requirements"]["accuracy"] = serde_json::json!({"explicit": {"Epsilon": 0.05}});
    fixture["query_workload"]["repeating_queries"] = serde_json::json!([entry]);
    let plan = quote_snapshot_for_test(
        serde_json::from_value::<BackendLocalPlanningInput>(fixture).unwrap(),
    )
    .compile_promql()
    .unwrap();
    assert!(plan.precompute_plan.materializations.is_empty());
    assert!(plan
        .query_plan
        .entries
        .values()
        .all(|entry| entry.nodes.values().any(|node| {
            matches!(
                node,
                control_plane::query_plan::QueryPlanNode::ExactFallback { .. }
            )
        })));
    assert!(plan.planner_selection_trace.iter().any(|trace| {
        trace["groups"].as_array().is_some_and(|groups| {
            groups.iter().any(|group| {
                group["candidates"].as_array().is_some_and(|candidates| {
                    candidates.iter().any(|candidate| {
                        candidate["accuracy_status"] == "unknown" && candidate["selected"] == false
                    })
                })
            })
        })
    }));
}
