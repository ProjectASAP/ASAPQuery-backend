//! Issue #754 level 1: every shared workload query has a valid physical plan.
use control_plane::physical::compiler::{
    BackendLocalPlanningInput, CompiledPhysicalPlan, PhysicalPlanCompiler, BACKEND_REVISION,
    PLANNER_REVISION,
};
use control_plane::physical::executable_binding::validate_query_plan;
use control_plane::physical::workload_cost::{
    enumerate_exact_and_materialized_candidates, manifest, WorkloadCostEvidence, WorkloadQuote,
};
use control_plane::query_plan::QueryPlanNode;
use planner_types::post_asap::{ExactKind, SketchAlgorithm, SummaryFamilyType};
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Deserialize)]
struct Suite {
    queries: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    expr: String,
}

struct ExpectedPlan {
    family: Option<ExpectedFamily>,
    partitioning: &'static str,
    readout: &'static str,
    root_operation: Option<&'static str>,
}

enum ExpectedFamily {
    Exact(ExactKind),
    QuantileSketch,
}

// These are semantic contracts for the installed query DAG, not a snapshot of
// generated node IDs or cost-dependent summary IDs.
fn expected_plan(name: &str) -> ExpectedPlan {
    match name {
        "spatial-sum" => ExpectedPlan {
            family: Some(ExpectedFamily::Exact(ExactKind::Sum)),
            partitioning: "grouped",
            readout: "sum",
            root_operation: None,
        },
        "spatial-topk" => ExpectedPlan {
            family: None,
            partitioning: "",
            readout: "",
            root_operation: None,
        },
        "spatial-quantile" => ExpectedPlan {
            family: Some(ExpectedFamily::QuantileSketch),
            partitioning: "grouped",
            readout: "quantile",
            root_operation: None,
        },
        "temporal-sum" => ExpectedPlan {
            family: Some(ExpectedFamily::Exact(ExactKind::Sum)),
            partitioning: "per_entity",
            readout: "sum",
            root_operation: None,
        },
        "temporal-quantile" => ExpectedPlan {
            family: Some(ExpectedFamily::QuantileSketch),
            partitioning: "per_entity",
            readout: "quantile",
            root_operation: None,
        },
        "temporal-rate" => ExpectedPlan {
            family: Some(ExpectedFamily::Exact(ExactKind::Rate)),
            partitioning: "per_entity",
            readout: "rate",
            root_operation: None,
        },
        "grouped-rate" => ExpectedPlan {
            family: Some(ExpectedFamily::Exact(ExactKind::Rate)),
            partitioning: "per_entity",
            readout: "rate",
            root_operation: Some("aggregate"),
        },
        "grouped-temporal-sum" => ExpectedPlan {
            family: Some(ExpectedFamily::Exact(ExactKind::Sum)),
            partitioning: "grouped",
            readout: "sum",
            root_operation: None,
        },
        "topk-rate" => ExpectedPlan {
            family: Some(ExpectedFamily::Exact(ExactKind::Rate)),
            partitioning: "per_entity",
            readout: "rate",
            root_operation: Some("top_k_selection"),
        },
        "quantile-ratio" => ExpectedPlan {
            family: None,
            partitioning: "",
            readout: "",
            root_operation: None,
        },
        other => panic!("no level-1 plan expectation for {other}"),
    }
}

fn family_matches(expected: &ExpectedFamily, actual: &SummaryFamilyType) -> bool {
    match (expected, actual) {
        (ExpectedFamily::Exact(expected), SummaryFamilyType::ExactAggregate(actual, _)) => {
            expected == actual
        }
        (ExpectedFamily::QuantileSketch, SummaryFamilyType::Sketch(kind, _)) => {
            matches!(
                kind.algorithm(),
                SketchAlgorithm::DDSketch | SketchAlgorithm::Kll
            )
        }
        _ => false,
    }
}

fn assert_selected_plan(name: &str, plan: &CompiledPhysicalPlan) -> Option<String> {
    let expected = expected_plan(name);
    let artifact = serde_json::to_value(plan).unwrap();
    let entries = artifact["query_plan"]["entries"].as_object().unwrap();
    assert_eq!(entries.len(), 1, "{name}: expected one query plan");
    let entry = entries.values().next().unwrap();
    let nodes = entry["nodes"].as_object().unwrap();
    let mut node = &nodes[&entry["root"].as_u64().unwrap().to_string()];
    let materializations = artifact["precompute_plan"]["materializations"]
        .as_array()
        .unwrap();
    if name == "spatial-topk" {
        assert_eq!(nodes.len(), 1, "{name}: unexpected query nodes");
        assert_eq!(node["op"], "logical", "{name}: expected a local readout");
        assert_eq!(node["operator"]["kind"], "current_series");
        assert_eq!(node["operator"]["population"]["metric"], "data");
        assert_eq!(
            node["operator"]["population"]["grouping"],
            json!({"labels":["label_0"],"without":false})
        );
        assert_eq!(node["operator"]["population"]["max_k"], 3);
        assert_eq!(node["operator"]["readout"], json!({"kind":"top_k","k":3}));
        assert!(
            materializations.is_empty(),
            "{name}: current-series readout has no summary producer"
        );
        return None;
    }
    if name == "grouped-temporal-sum" && node["op"] == "logical" {
        return Some("grouped-temporal-sum: Planner left a per-series temporal Sum and an outer exact grouped Sum; expected one time-and-label grouped Sum producer".into());
    }
    if name == "quantile-ratio" {
        if node["op"] == "exact_fallback" {
            return Some("quantile-ratio: expected two q=0.9/q=0.5 quantile sketch readouts followed by local division; Planner emitted exact fallback".into());
        }
        assert_eq!(node["op"], "binary", "{name}: expected local division");
        assert!(
            matches!(node["operator"].as_str(), Some("div" | "Div")),
            "{name}: wrong binary operator"
        );
        let inputs = node["inputs"].as_array().unwrap();
        assert_eq!(inputs.len(), 2);
        for (input, q) in inputs.iter().zip([0.9, 0.5]) {
            let readout = &nodes[&input.to_string()];
            assert_eq!(readout["op"], "summary_estimate");
            assert_eq!(readout["query"], json!({"kind":"quantile","q":q}));
            let leaf = &nodes[&readout["input"].to_string()];
            assert_eq!(leaf["op"], "read_materialization");
            assert_eq!(leaf["binding"]["output_grouping"]["mode"], "per_entity");
            assert_eq!(leaf["binding"]["readout_lookback_ms"], 60_000);
        }
        assert!(!materializations.is_empty());
        assert!(plan.precompute_plan.materializations.iter().all(|summary| {
            family_matches(
                &ExpectedFamily::QuantileSketch,
                &summary.accumulator_spec().unwrap().family,
            ) && summary.metric == "data"
                && summary.window_size == 60
        }));
        return None;
    }
    let Some(family) = expected.family.as_ref() else {
        panic!("{name}: no physical plan contract");
    };
    if let Some(operation) = expected.root_operation {
        assert_eq!(node["op"], "logical", "{name}: missing root operator");
        assert_eq!(
            node["operator"]["kind"], operation,
            "{name}: wrong root operator"
        );
        if operation == "aggregate" {
            assert_eq!(node["operator"]["operation"], "sum");
        } else {
            assert_eq!(node["operator"]["k"], 3, "{name}: wrong TopK limit");
        }
        assert_eq!(
            node["operator"]["grouping"],
            json!({"labels":["label_0"],"without":false}),
            "{name}: wrong grouping"
        );
        let inputs = node["inputs"].as_array().unwrap();
        assert_eq!(inputs.len(), 1, "{name}: root must have one input");
        node = &nodes[&inputs[0].to_string()];
    }
    assert_eq!(
        nodes.len(),
        if expected.root_operation.is_some() {
            3
        } else {
            2
        },
        "{name}: unexpected DAG nodes"
    );
    if expected.readout == "quantile" {
        assert_eq!(
            node["op"], "summary_estimate",
            "{name}: missing sketch readout"
        );
        assert_eq!(
            node["query"],
            json!({"kind":"quantile","q":0.9}),
            "{name}: wrong quantile"
        );
    } else {
        assert_eq!(node["op"], "exact_readout", "{name}: wrong readout node");
        assert_eq!(node["readout"], expected.readout, "{name}: wrong readout");
    }
    let leaf = &nodes[&node["input"].to_string()];
    assert_eq!(
        leaf["op"], "read_materialization",
        "{name}: missing summary read"
    );
    assert_eq!(
        materializations.len(),
        1,
        "{name}: expected one summary producer"
    );
    let summary = &materializations[0];
    let actual_family = plan.precompute_plan.materializations[0]
        .accumulator_spec()
        .unwrap()
        .family;
    assert!(
        family_matches(family, &actual_family),
        "{name}: wrong Planner family: {actual_family:?}"
    );
    assert_eq!(summary["metric"], "data", "{name}: wrong source metric");
    assert_eq!(
        summary["partitioning"], expected.partitioning,
        "{name}: wrong population partitioning"
    );
    let spatial = expected.partitioning == "grouped";
    assert_eq!(
        summary["window_size"],
        if spatial { 5 } else { 60 },
        "{name}: wrong summary window"
    );
    assert_eq!(
        leaf["binding"]["output_grouping"]["mode"],
        if spatial { "reduce" } else { "per_entity" },
        "{name}: wrong read grouping"
    );
    if spatial {
        assert_eq!(summary["grouping_labels"]["labels"], json!(["label_0"]));
        assert_eq!(
            leaf["binding"]["output_grouping"]["keys"],
            json!(["label_0"])
        );
    } else {
        assert_eq!(
            leaf["binding"]["readout_lookback_ms"], 60_000,
            "{name}: wrong PromQL range"
        );
    }
    None
}

/// The same ten expressions used by level 2 must compile to typed, connected plans.
#[test]
fn issue754_queries_have_valid_physical_plans() {
    let suite: Suite = serde_yaml::from_str(include_str!(
        "../../promql-compliance/suites/issue-754.yaml"
    ))
    .unwrap();
    assert_eq!(suite.queries.len(), 10, "the issue-754 contract changed");
    let mut missing_local_plans = Vec::new();
    for case in suite.queries {
        let expected = expected_plan(&case.name);
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
        for (candidate_index, candidate) in candidates.into_iter().enumerate() {
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
                    if let Ok(directory) = std::env::var("ASAP_LEVEL1_ARTIFACT_DIR") {
                        let base = std::path::Path::new(&directory)
                            .join("candidates")
                            .join(format!("{}-{candidate_index}", case.name));
                        std::fs::create_dir_all(base.parent().unwrap()).unwrap();
                        std::fs::write(
                            base.with_extension("json"),
                            serde_json::to_vec_pretty(&plan).unwrap(),
                        )
                        .unwrap();
                        std::fs::write(base.with_extension("dot"), &dot).unwrap();
                    }
                    // The level-1 acceptance target is a backend-local plan.
                    // Price explicit exact fallbacks above every local candidate.
                    let cost = if plan.query_plan.entries.values().any(|entry| {
                        entry
                            .nodes
                            .values()
                            .any(|node| matches!(node, QueryPlanNode::ExactFallback { .. }))
                    }) {
                        1e12
                    } else if plan.precompute_plan.materializations.is_empty() {
                        2.0
                    } else {
                        1.0
                    };
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
        if let Some(family) = expected.family.as_ref() {
            assert!(
                valid_plans.iter().any(|plan| {
                    plan.precompute_plan
                        .materializations
                        .iter()
                        .any(|m| family_matches(family, &m.accumulator_spec().unwrap().family))
                        && plan.query_plan.entries.values().all(|entry| {
                            entry.nodes.values().any(|node| {
                                matches!(node, QueryPlanNode::ReadMaterialization { .. })
                            }) && !entry
                                .nodes
                                .values()
                                .any(|node| matches!(node, QueryPlanNode::ExactFallback { .. }))
                        })
                }),
                "{} lacks a readable summary candidate: {errors:?}",
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
        if let Some(error) = assert_selected_plan(&case.name, &selected) {
            missing_local_plans.push(error);
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
    assert!(
        missing_local_plans.is_empty(),
        "{}",
        missing_local_plans.join("\n")
    );
}
