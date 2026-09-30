//! Precompute-or-query-time placement follows the backend's lifecycle costs.
use control_plane::physical::compiler::{
    BackendLocalPlanningInput, CompiledPhysicalPlan, DeploymentPlanCompiler,
};
use control_plane::physical::workload_cost::enumerate_exact_and_materialized_candidates;
use control_plane::query_plan::{query_time::QueryTimeOperator, QueryPlanNode};
use serde_json::Value;

fn fixture(store_per_byte_second: f64, require_local: bool) -> BackendLocalPlanningInput {
    let mut wire: Value = serde_json::from_str(include_str!(
        "../../docs/examples/asapquery-planning-snapshot.json"
    ))
    .unwrap();
    wire["implementation"]["lifecycle_costs"]["store_per_byte_second"] =
        store_per_byte_second.into();
    wire["implementation"]["require_backend_local_execution"] = require_local.into();
    serde_json::from_value(wire).unwrap()
}

/// The Planner-selected candidate, compiled; the native exact alternative is last.
fn selected_plan(input: BackendLocalPlanningInput) -> CompiledPhysicalPlan {
    let (request, environment) = input.into_physical_compilation_request().unwrap();
    let candidate = enumerate_exact_and_materialized_candidates(request)
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert!(candidate.allow_mixed_summary_and_exact_execution);
    DeploymentPlanCompiler
        .compile_promql(candidate, environment)
        .unwrap()
}

fn raw_scans(plan: &CompiledPhysicalPlan) -> Vec<&QueryTimeOperator> {
    plan.query_plan
        .entries
        .values()
        .flat_map(|entry| entry.nodes.values())
        .filter_map(|node| match node {
            QueryPlanNode::Logical {
                operator: operator @ QueryTimeOperator::Scan { .. },
                ..
            } => Some(operator),
            _ => None,
        })
        .collect()
}

fn placements(plan: &CompiledPhysicalPlan) -> Vec<String> {
    plan.planner_selection_trace
        .iter()
        .filter(|entry| entry["stage"] == "deployment.lifecycle_placement")
        .map(|entry| entry["selected"].as_str().unwrap().to_owned())
        .collect()
}

// A cheap summary store keeps the quantile sketch continuously maintained.
#[test]
fn cheap_summary_store_precomputes_the_state() {
    let plan = selected_plan(fixture(0.0, false));
    assert_eq!(placements(&plan), ["continuously_maintained"]);
    assert_eq!(plan.precompute_plan.materializations.len(), 1);
    assert!(raw_scans(&plan).is_empty());
}

// An expensive summary store rebuilds the state per query from raw Prometheus series.
#[test]
fn expensive_summary_store_moves_the_state_to_query_time() {
    let plan = selected_plan(fixture(1.0, false));
    assert_eq!(placements(&plan), ["ephemeral"]);
    assert!(plan.precompute_plan.materializations.is_empty());
    let entry = plan.query_plan.entries.values().next().unwrap();
    entry.recover_vector_physical_dag().unwrap();
    assert_eq!(
        raw_scans(&plan),
        [&QueryTimeOperator::Scan {
            metric: Some("m".into()),
            matchers: vec![],
            range_ms: Some(60_000),
            offset_ms: 0,
        }]
    );
}

// Without a query-time raw source, the state stays maintained whatever the store costs.
#[test]
fn ephemeral_requires_a_bindable_raw_source() {
    let plan = selected_plan(fixture(1.0, true));
    assert_eq!(placements(&plan), ["continuously_maintained"]);
    assert_eq!(plan.precompute_plan.materializations.len(), 1);
    assert!(raw_scans(&plan).is_empty());
}

fn decisions(queries: &[&str]) -> Vec<Value> {
    let mut wire = serde_json::to_value(fixture(0.0, false)).unwrap();
    let template = wire["query_workload"]["repeating_queries"][0].clone();
    wire["query_workload"]["repeating_queries"] = queries
        .iter()
        .map(|query| {
            let mut entry = template.clone();
            entry["query"] = (*query).into();
            entry["requirements"]["accuracy"] = serde_json::json!({"explicit": "Exact"});
            entry
        })
        .collect();
    let plan = selected_plan(serde_json::from_value(wire).unwrap());
    plan.planner_selection_trace
        .iter()
        .filter(|entry| entry["stage"] == "deployment.lifecycle_placement")
        .cloned()
        .collect()
}

// A state shared by two queries is one lifecycle decision: maintenance and
// retention are charged once, while both queries' reads are counted.
#[test]
fn shared_state_is_priced_once_with_all_reads() {
    let [alone] = decisions(&["sum_over_time(m[1m])"]).try_into().unwrap();
    let [shared] = decisions(&["sum_over_time(m[1m])", "sort(sum_over_time(m[1m]))"])
        .try_into()
        .unwrap();
    assert_eq!(shared["query_ids"].as_array().unwrap().len(), 2);
    let cost = |decision: &Value, field: &str| decision[field].as_f64().unwrap();
    // Rebuilding scales with reads; maintaining adds only the extra read cost.
    let extra_reads = cost(&shared, "ephemeral_cost") / cost(&alone, "ephemeral_cost");
    assert_eq!(extra_reads, 2.0);
    assert!(
        cost(&shared, "continuously_maintained_cost")
            < 2.0 * cost(&alone, "continuously_maintained_cost")
    );
}
