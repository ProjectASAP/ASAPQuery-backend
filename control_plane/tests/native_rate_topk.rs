//! Planner owns Rate ranking; Backend binds durable state and prices candidates.
use asap_types::physical_plan_codec::PhysicalPlanCodec;
use control_plane::physical::{
    compiler::{BackendLocalPlanningInput, DeploymentPlanCompiler},
    workload_cost::enumerate_exact_and_materialized_candidates,
};
use serde_json::{json, Value};

fn fixture(certified: bool) -> BackendLocalPlanningInput {
    placed_fixture(certified, 0.0)
}

/// A positive summary-store price makes rebuilding a heap per query from the
/// retained counter readouts cheaper than maintaining it. Local-only execution
/// keeps the counter state itself precomputed rather than read raw.
fn placed_fixture(certified: bool, store_per_byte_second: f64) -> BackendLocalPlanningInput {
    let mut wire: Value = serde_json::from_str(include_str!(
        "../../docs/examples/asapquery-compatibility-demo-snapshot.json"
    ))
    .unwrap();
    let query = "topk by (job) (1, rate(requests_total[1m]))";
    let mut entry = wire["query_workload"]["repeating_queries"][3].clone();
    entry["query"] = query.into();
    entry["requirements"]["accuracy"] =
        json!({"explicit":{"EpsilonDelta":{"epsilon":0.1,"delta":0.1}}});
    wire["query_workload"]["repeating_queries"] = json!([entry]);
    wire["implementation"]["topk_evidence"] = json!({});
    wire["implementation"]["lifecycle_costs"]["store_per_byte_second"] =
        store_per_byte_second.into();
    wire["implementation"]["require_backend_local_execution"] =
        (store_per_byte_second > 0.0).into();
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

// An admitted Rate heap reads durable counter state; it must not become an
// external whole-query fallback or accumulate counter samples as heap weights.
#[test]
fn rate_heap_candidates_bind_durable_counter_windows() {
    let (request, environment) = placed_fixture(true, 1.0)
        .into_physical_compilation_request()
        .unwrap();
    let mut families = std::collections::BTreeSet::new();
    for candidate in enumerate_exact_and_materialized_candidates(request).unwrap() {
        let Ok(plan) = DeploymentPlanCompiler.compile_promql(candidate, environment.clone()) else {
            continue;
        };
        if plan
            .precompute_plan
            .executable_dags
            .values()
            .any(|dag| !dag.native_programs.is_empty())
        {
            continue;
        }
        let entry = plan.query_plan.entries.values().next().unwrap();
        let Some(program) = &entry.physical_dag else {
            continue;
        };
        for family in ["CmsWithHeap", "CountSketchWithHeap"] {
            if program.to_string().contains(family) {
                assert_eq!(entry.instant.lookback_ms, 60_000);
                assert!(entry.nodes.values().any(|node| matches!(
                    node,
                    asap_types::query_plan::QueryPlanNode::ExactReadout {
                        readout: asap_types::query_plan::ExactReadout::Rate,
                        ..
                    }
                )));
                assert_eq!(entry.materialization_bindings().len(), 1);
                assert_eq!(
                    entry.materialization_bindings()[0].readout_lookback_ms,
                    Some(60_000)
                );
                assert!(!plan.precompute_plan.materializations.is_empty());
                let mut restored: asap_types::query_plan::QueryPlanEntry =
                    serde_json::from_value(serde_json::to_value(entry).unwrap()).unwrap();
                restored.recover_vector_physical_dag().unwrap();
                restored.physical_dag = None;
                assert!(restored.recover_vector_physical_dag().is_err());
                let mut drifted = entry.clone();
                if let asap_types::query_plan::QueryPlanNode::Physical { source_nodes, .. } =
                    drifted.nodes.get_mut(&drifted.root).unwrap()
                {
                    source_nodes[0] += 100;
                }
                assert!(drifted.recover_vector_physical_dag().is_err());
                let mut wrong_window = entry.clone();
                wrong_window.instant.lookback_ms = 5_000;
                assert!(wrong_window.recover_vector_physical_dag().is_err());
                families.insert(family);
            }
        }
    }
    assert_eq!(
        families,
        std::collections::BTreeSet::from(["CmsWithHeap", "CountSketchWithHeap"])
    );
}

// Scoped evidence is required even when a physical heap can be compiled.
#[test]
fn missing_rate_heap_proof_leaves_exact_candidate_available() {
    let (request, environment) = fixture(false).into_physical_compilation_request().unwrap();
    let mut exact = false;
    for candidate in enumerate_exact_and_materialized_candidates(request).unwrap() {
        if let Ok(plan) = DeploymentPlanCompiler.compile_promql(candidate, environment.clone()) {
            for entry in plan.query_plan.entries.values() {
                if let Some(program) = &entry.physical_dag {
                    assert!(!program.to_string().contains("WithHeap"));
                    exact = true;
                }
            }
        }
    }
    assert!(exact);
}

// Provider costs may choose exact ranking, CMS or CountSketch over the same
// counter population; no family is selected before deployment costs exist.
#[test]
fn rate_heap_costs_can_select_each_compiled_candidate() {
    use control_plane::physical::{
        compiler::{BACKEND_REVISION, PLANNER_REVISION},
        workload_cost::{manifest, WorkloadCostEvidence, WorkloadQuote},
    };
    for preferred in ["exact", "CmsWithHeap", "CountSketchWithHeap"] {
        let mut input = placed_fixture(true, 1.0);
        let (request, environment) = input.clone().into_physical_compilation_request().unwrap();
        let quotes = enumerate_exact_and_materialized_candidates(request)
            .unwrap()
            .into_iter()
            .filter_map(|candidate| {
                let plan = DeploymentPlanCompiler
                    .compile_promql(candidate.clone(), environment.clone())
                    .ok()?;
                let entry = plan.query_plan.entries.values().next().unwrap();
                let program = entry
                    .physical_dag
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default();
                let preferred_plan = entry.physical_vector_binding().is_some()
                    && !plan
                        .precompute_plan
                        .executable_dags
                        .values()
                        .any(|dag| !dag.native_programs.is_empty())
                    && if preferred == "exact" {
                        !program.contains("WithHeap")
                    } else {
                        program.contains(preferred)
                    };
                let manifest = manifest(&plan, &candidate.queries).unwrap();
                Some(WorkloadQuote {
                    unit_costs: manifest
                        .components
                        .keys()
                        .map(|key| (key.clone(), if preferred_plan { 1.0 } else { 1e12 }))
                        .collect(),
                    manifest,
                    executable: true,
                })
            })
            .collect();
        input.workload_cost_evidence = Some(WorkloadCostEvidence {
            backend_revision: BACKEND_REVISION.into(),
            planner_revision: PLANNER_REVISION.into(),
            data_snapshot_id: "snapshot-topk-test".into(),
            model_version: "controlled-rate-ranking-cost".into(),
            observed_at_unix_ms: 10000,
            valid_for_ms: 60000,
            quotes,
        });
        let plan = input.compile_promql().unwrap();
        let entry = plan.query_plan.entries.values().next().unwrap();
        assert!(entry.physical_vector_binding().is_some());
        let program = entry.physical_dag.as_ref().unwrap().to_string();
        if preferred == "exact" {
            assert!(!program.contains("WithHeap"));
        } else {
            assert!(program.contains(preferred));
        }
    }
}

// Fixed-window candidates retain a native maintenance graph and read its heap
// state directly; removing that graph must make recovered installation invalid.
#[test]
fn fixed_window_rate_heap_candidates_install_both_physical_graphs() {
    let mut input = fixture(true);
    let mut wire = serde_json::to_value(&input).unwrap();
    wire["query_workload"]["repeating_queries"][0]["demand"]["fixed_interval_at"]["interval"] =
        60_000.into();
    wire["query_workload"]["repeating_queries"][0]["demand"]["fixed_interval_at"]
        ["evaluation_phase"] = 0.into();
    input = serde_json::from_value(wire).unwrap();
    let (request, environment) = input.into_physical_compilation_request().unwrap();
    let mut families = std::collections::BTreeSet::new();
    let mut errors = Vec::new();
    for candidate in enumerate_exact_and_materialized_candidates(request).unwrap() {
        let plan = match DeploymentPlanCompiler.compile_promql(candidate, environment.clone()) {
            Ok(plan) => plan,
            Err(error) => {
                errors.push(error.to_string());
                continue;
            }
        };
        for installed in plan.precompute_plan.executable_dags.values() {
            for sink in installed.native_programs.keys() {
                let program = installed.native_program(*sink).unwrap().unwrap();
                let encoded = String::from_utf8(program.encode().unwrap()).unwrap();
                for family in ["CmsWithHeap", "CountSketchWithHeap"] {
                    if encoded.contains(family) {
                        families.insert(family);
                    }
                }
                assert!(encoded.contains("Rate"));
                let entry = plan.query_plan.entries.values().next().unwrap();
                let query = entry.recover_vector_physical_dag().unwrap();
                assert!(!String::from_utf8(query.encode().unwrap())
                    .unwrap()
                    .contains("KeyedSummaryBuild"));
                let mut broken = plan.precompute_plan.clone();
                for installed in broken.executable_dags.values_mut() {
                    installed.native_programs.clear();
                }
                assert!(broken.validate().is_err());
            }
        }
    }
    assert_eq!(
        families,
        std::collections::BTreeSet::from(["CmsWithHeap", "CountSketchWithHeap"]),
        "{errors:#?}"
    );
}

// Rate precedes grouped Sum in both placements; the summary-store price
// chooses between maintaining the Sum and rebuilding it per query.
#[test]
fn grouped_rate_placement_follows_summary_store_cost() {
    for (store, maintained) in [(0.0, true), (1.0, false)] {
        let mut wire = serde_json::to_value(placed_fixture(false, store)).unwrap();
        let entry = &mut wire["query_workload"]["repeating_queries"][0];
        entry["query"] = "sum by(job)(rate(requests_total[1m]))".into();
        entry["requirements"]["accuracy"] = json!({"explicit":"Exact"});
        entry["demand"]["fixed_interval_at"]["interval"] = 5_000.into();
        entry["demand"]["fixed_interval_at"]["evaluation_phase"] = 0.into();
        wire["implementation"]["accuracy_evidence"] = json!({});
        let input: BackendLocalPlanningInput = serde_json::from_value(wire).unwrap();
        let (request, environment) = input.into_physical_compilation_request().unwrap();
        let mut placements = std::collections::BTreeSet::new();
        let mut errors = Vec::new();
        for candidate in enumerate_exact_and_materialized_candidates(request).unwrap() {
            let plan = match DeploymentPlanCompiler.compile_promql(candidate, environment.clone()) {
                Ok(plan) => plan,
                Err(error) => {
                    errors.push(error.to_string());
                    continue;
                }
            };
            let entry = plan.query_plan.entries.values().next().unwrap();
            let Some(program) = &entry.physical_dag else {
                continue;
            };
            if entry.physical_vector_binding().is_none() {
                continue;
            }
            entry.recover_vector_physical_dag().unwrap();
            let stored = plan
                .precompute_plan
                .executable_dags
                .values()
                .any(|dag| !dag.native_programs.is_empty());
            let rebuilt = program.to_string().contains("SummaryBuild");
            assert!(!(stored && rebuilt));
            // Planner also offers a relational Sum over the readouts, which has
            // no Sum state to place.
            if stored || rebuilt {
                placements.insert(stored);
            }
        }
        assert_eq!(
            placements,
            std::collections::BTreeSet::from([maintained]),
            "{errors:#?}"
        );
    }
}

// Deployment must install the exact retained Planner graphs, including roots,
// typed inputs and materialization frontiers, rather than compiling replacements.
#[test]
fn selected_native_graphs_survive_deployment_unchanged() {
    use asap_physical_operators::physical_planner::PhysicalCandidate;
    let (request, environment) = fixture(true).into_physical_compilation_request().unwrap();
    let mut checked = 0;
    let mut checked_precompute = 0;
    for candidate in enumerate_exact_and_materialized_candidates(request).unwrap() {
        let expected = candidate
            .queries
            .iter()
            .filter_map(|query| {
                query.physical_candidate.as_ref().map(|bytes| {
                    (
                        query.query_id.clone(),
                        PhysicalCandidate::decode(bytes).unwrap(),
                    )
                })
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let Ok(plan) = DeploymentPlanCompiler.compile_promql(candidate, environment.clone()) else {
            continue;
        };
        for entry in plan.query_plan.entries.values() {
            let Some(expected) = expected.get(&entry.query_id) else {
                continue;
            };
            let actual = entry
                .physical_dag
                .as_ref()
                .expect("retained candidate must be installed");
            let encoded: Value = serde_json::from_slice(&expected.query.encode().unwrap()).unwrap();
            assert_eq!(*actual, encoded);
            if let Some(precompute) = &expected.precompute {
                let encoded: Value = serde_json::from_slice(&precompute.encode().unwrap()).unwrap();
                assert!(plan.precompute_plan.executable_dags[&entry.query_id]
                    .native_programs
                    .values()
                    .any(|actual| actual == &encoded));
                checked_precompute += 1;
            }
            checked += 1;
        }
    }
    assert!(checked > 0 && checked_precompute > 0);
}
