//! Planner owns the snapshot heap program; Backend admits and prices it.
use control_plane::physical::{
    compiler::{
        BackendLocalPlanningInput, DeploymentPlanCompiler, BACKEND_REVISION, PLANNER_REVISION,
    },
    workload_cost::{
        enumerate_exact_and_materialized_candidates, manifest, WorkloadCostEvidence, WorkloadQuote,
    },
};
use serde_json::{json, Value};

fn fixture(certified: bool) -> BackendLocalPlanningInput {
    let mut wire: Value = serde_json::from_str(include_str!(
        "../../docs/examples/asapquery-compatibility-demo-snapshot.json"
    ))
    .unwrap();
    let query = "topk by (job) (1, spatial_value)";
    let mut entry = wire["query_workload"]["repeating_queries"][3].clone();
    entry["query"] = query.into();
    entry["requirements"]["accuracy"] =
        json!({"explicit":{"EpsilonDelta":{"epsilon":0.1,"delta":0.1}}});
    wire["query_workload"]["repeating_queries"] = json!([entry]);
    wire["implementation"]["topk_evidence"] = json!({});
    wire["implementation"]["data_snapshot_id"] = "snapshot-topk-test".into();
    if certified {
        wire["implementation"]["accuracy_evidence"] = json!({query: {
            "query_string": query, "data_snapshot_id":"snapshot-topk-test",
            "data_workload": wire["data_workload"], "source":"enforced-fixture-contract",
            "observed_at_unix_ms":9500, "valid_for_ms":60000,
            "topk_max_distinct_items":1000,
            "topk_selected_lower_bound":101.0, "topk_excluded_upper_bound":100.0,
            "topk_interval_failure_probability":0.001
        }});
    }
    serde_json::from_value(wire).unwrap()
}

fn heap(plan: &control_plane::physical::compiler::CompiledPhysicalPlan) -> bool {
    plan.query_plan.entries.values().any(|entry| {
        entry
            .physical_dag
            .as_ref()
            .is_some_and(|program| program.to_string().contains("CountSketchWithHeap"))
    })
}

// Same physical inventory, different deployment quotes: selection must reverse.
#[test]
fn snapshot_heap_and_exact_candidates_reach_deployment_cost_selection() {
    for prefer_heap in [false, true] {
        let mut input = fixture(true);
        let (request, environment) = input.clone().into_physical_compilation_request().unwrap();
        assert!(request
            .planner_selection_trace
            .iter()
            .any(|event| event["stage"] == "planner.physical_candidate"
                && event.get("physical_dag").is_some()));
        let candidates = enumerate_exact_and_materialized_candidates(request).unwrap();
        let mut saw_heap = false;
        let mut saw_exact = false;
        let quotes = candidates
            .into_iter()
            .filter_map(|candidate| {
                let plan = DeploymentPlanCompiler
                    .compile_promql(candidate.clone(), environment.clone())
                    .ok()?;
                let is_heap = heap(&plan);
                let local = plan
                    .query_plan
                    .entries
                    .values()
                    .all(|entry| entry.population_snapshot().is_some());
                saw_heap |= is_heap;
                saw_exact |= local && !is_heap;
                if is_heap {
                    assert!(plan.precompute_plan.materializations.is_empty());
                    let entry = plan.query_plan.entries.values().next().unwrap();
                    let restored: asap_types::query_plan::QueryPlanEntry =
                        serde_json::from_value(serde_json::to_value(entry).unwrap()).unwrap();
                    restored.recover_population_physical_dag().unwrap();
                }
                let manifest = manifest(&plan, &candidate.queries).unwrap();
                Some(WorkloadQuote {
                    unit_costs: manifest
                        .components
                        .keys()
                        .map(|key| {
                            (
                                key.clone(),
                                if local && is_heap == prefer_heap {
                                    1.0
                                } else {
                                    1e12
                                },
                            )
                        })
                        .collect(),
                    manifest,
                    executable: true,
                })
            })
            .collect();
        assert!(
            saw_heap,
            "certified signed heap candidate never reached pricing"
        );
        assert!(saw_exact, "exact population candidate disappeared");
        input.workload_cost_evidence = Some(WorkloadCostEvidence {
            backend_revision: BACKEND_REVISION.into(),
            planner_revision: PLANNER_REVISION.into(),
            data_snapshot_id: "snapshot-topk-test".into(),
            model_version: "fixture-cost-reversal".into(),
            observed_at_unix_ms: 10000,
            valid_for_ms: 60000,
            quotes,
        });
        assert_eq!(heap(&input.compile_promql().unwrap()), prefer_heap);
    }
}

// A priced physical graph is not an accuracy proof. Missing population/gap
// evidence must leave the exact candidate available and the heap uninstalled.
#[test]
fn unknown_snapshot_heap_guarantee_cannot_be_installed() {
    let (request, environment) = fixture(false).into_physical_compilation_request().unwrap();
    assert!(request
        .planner_selection_trace
        .iter()
        .any(|event| event["stage"] == "planner.physical_candidate"));
    for candidate in enumerate_exact_and_materialized_candidates(request).unwrap() {
        if let Ok(plan) = DeploymentPlanCompiler.compile_promql(candidate, environment.clone()) {
            assert!(!heap(&plan));
        }
    }
}
