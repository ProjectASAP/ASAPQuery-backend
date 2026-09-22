//! Issue #754 level 1: every shared workload query has a valid physical plan.
use control_plane::physical::compiler::{
    BackendLocalPlanningInput, PhysicalPlanCompiler, BACKEND_REVISION, PLANNER_REVISION,
};
use control_plane::physical::executable_binding::validate_query_plan;
use control_plane::physical::workload_cost::{
    enumerate_exact_and_materialized_candidates, manifest, WorkloadCostEvidence, WorkloadQuote,
};
use control_plane::query_plan::QueryPlanNode;
use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
struct Suite {
    queries: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    expr: String,
}

fn expected_summary(name: &str) -> Option<&'static str> {
    match name {
        "spatial-sum" | "temporal-sum" | "grouped-temporal-sum" => Some("Sum"),
        "spatial-quantile" | "temporal-quantile" => Some("DDSketch"),
        "temporal-rate" | "grouped-rate" | "topk-rate" => Some("Increase"),
        "spatial-topk" | "quantile-ratio" => None,
        other => panic!("no level-1 plan expectation for {other}"),
    }
}

/// The same ten expressions used by level 2 must compile to typed, connected plans.
#[test]
fn issue754_queries_have_valid_physical_plans() {
    let suite: Suite = serde_yaml::from_str(include_str!(
        "../../promql-compliance/suites/issue-754.yaml"
    ))
    .unwrap();
    assert_eq!(suite.queries.len(), 10, "the issue-754 contract changed");
    for case in suite.queries {
        let expected = expected_summary(&case.name);
        let mut snapshot: Value = serde_json::from_str(include_str!(
            "../../docs/examples/asapquery-planning-snapshot.json"
        ))
        .unwrap();
        snapshot["query_workload"]["repeating_queries"][0]["query"] = case.expr.clone().into();
        let mut input: BackendLocalPlanningInput = serde_json::from_value(snapshot).unwrap();
        let (request, environment) = input.clone().into_physical_compilation_request().unwrap();
        let candidates = enumerate_exact_and_materialized_candidates(request).unwrap();
        let mut valid_plans = Vec::new();
        let mut quotes = Vec::new();
        let mut errors = Vec::new();
        for candidate in candidates {
            match PhysicalPlanCompiler.compile_promql(candidate.clone(), environment.clone()) {
                Ok(plan) => {
                    let entry = plan.query_plan.lookup(&case.expr).unwrap();
                    assert_eq!(entry.canonical_query, case.expr);
                    assert!(
                        entry.nodes.contains_key(&entry.root),
                        "query root must exist"
                    );
                    if let Some(installed) =
                        plan.precompute_plan.executable_dags.get(&entry.query_id)
                    {
                        installed.validate().expect("typed DAG is valid");
                        validate_query_plan(installed, entry).expect("DAG/query bindings agree");
                    } else {
                        assert!(
                            plan.precompute_plan.materializations.is_empty(),
                            "summary plan must retain its Planner DAG"
                        );
                    }
                    let dot = control_plane::physical::plan_dot::render(&plan);
                    assert!(dot.contains("PrecomputePlan") && dot.contains("QueryPlan:"));
                    let cost = if valid_plans.is_empty() { 1.0 } else { 1e12 };
                    let manifest = manifest(&plan, &candidate.queries).unwrap();
                    quotes.push(WorkloadQuote {
                        unit_costs: manifest
                            .components
                            .keys()
                            .map(|key| (key.clone(), cost))
                            .collect(),
                        manifest,
                        executable: true,
                    });
                    valid_plans.push(plan);
                }
                Err(error) => errors.push(error.to_string()),
            }
        }
        assert!(
            !valid_plans.is_empty(),
            "{} has no valid physical plan: {errors:?}",
            case.name
        );
        if let Some(family) = expected {
            assert!(
                valid_plans.iter().any(|plan| {
                    plan.precompute_plan
                        .materializations
                        .iter()
                        .any(|m| format!("{:?}", m.aggregation_type) == family)
                        && plan.query_plan.entries.values().all(|entry| {
                            entry.nodes.values().any(|node| {
                                matches!(node, QueryPlanNode::ReadMaterialization { .. })
                            }) && !entry
                                .nodes
                                .values()
                                .any(|node| matches!(node, QueryPlanNode::ExactFallback { .. }))
                        })
                }),
                "{} lacks a readable {family} summary candidate: {errors:?}",
                case.name
            );
        }
        input.workload_cost_evidence = Some(WorkloadCostEvidence {
            backend_revision: BACKEND_REVISION.into(),
            planner_revision: PLANNER_REVISION.into(),
            data_snapshot_id: "issue-754-level1".into(),
            model_version: "deterministic-test-costs".into(),
            observed_at_unix_ms: environment.observed_at_unix_ms,
            valid_for_ms: environment.max_evidence_age_ms,
            quotes,
        });
        let selected = input
            .compile_promql()
            .unwrap_or_else(|error| panic!("{} selected plan failed: {error}", case.name));
        let selected_entry = selected.query_plan.lookup(&case.expr).unwrap();
        assert!(selected_entry.nodes.contains_key(&selected_entry.root));
        if let Some(family) = expected {
            assert!(
                selected
                    .precompute_plan
                    .materializations
                    .iter()
                    .any(|m| format!("{:?}", m.aggregation_type) == family),
                "{} selected plan lost the expected {family} summary",
                case.name
            );
            assert!(
                !selected_entry
                    .nodes
                    .values()
                    .any(|node| matches!(node, QueryPlanNode::ExactFallback { .. })),
                "{} silently fell back despite a selected summary",
                case.name
            );
        } else {
            assert!(selected.precompute_plan.materializations.is_empty());
            assert!(
                matches!(
                    selected_entry.nodes.get(&selected_entry.root),
                    Some(QueryPlanNode::ExactFallback { .. })
                ),
                "{} must expose an explicit exact fallback",
                case.name
            );
        }
        if let Some(installed) = selected
            .precompute_plan
            .executable_dags
            .get(&selected_entry.query_id)
        {
            installed.validate().unwrap();
            validate_query_plan(installed, selected_entry).unwrap();
        }
        if let Ok(directory) = std::env::var("ASAP_LEVEL1_ARTIFACT_DIR") {
            let plan = &selected;
            std::fs::create_dir_all(&directory).unwrap();
            let base = std::path::Path::new(&directory).join(&case.name);
            std::fs::write(
                base.with_extension("json"),
                serde_json::to_vec_pretty(plan).unwrap(),
            )
            .unwrap();
            std::fs::write(
                base.with_extension("dot"),
                control_plane::physical::plan_dot::render(plan),
            )
            .unwrap();
        }
    }
}
