//! Issue #754 level 1: every shared workload query has a valid physical plan.
use asap_types::sds::SummaryOperator;
use control_plane::physical::compiler::{
    BackendLocalPlanningInput, CompiledPhysicalPlan, DeploymentPlanCompiler, BACKEND_REVISION,
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

// Expectations below are specific to this controlled fixture. Candidate
// semantics and cost-dependent placement are validated separately.
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
            root_operation: Some("limit"),
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
    for materialization in &plan.precompute_plan.materializations {
        let definition = plan
            .summary_catalog
            .outputs
            .get(&materialization.policy_fingerprint().into())
            .expect("precompute producer has no catalog definition");
        let semantics = &plan.summary_catalog.definitions[&definition.definition_id];
        assert_eq!(
            semantics.id().unwrap(),
            definition.definition_id,
            "{name}: stored output's semantic identity must match its persisted description"
        );
        let writer = plan
            .precompute_plan
            .schemas
            .iter()
            .find(|schema| {
                schema.materialization.fingerprint() == materialization.policy_fingerprint()
            })
            .unwrap();
        assert_eq!(
            writer.stored_output_reference.definition_id,
            definition.definition_id
        );
        let descriptor =
            &plan.summary_catalog.summary_descriptors[&definition.summary_descriptor_id];
        let SummaryOperator::Configured { family, .. } = &descriptor.operator else {
            panic!("{name}: producer catalog descriptor lacks Planner family");
        };
        assert_eq!(
            family,
            &materialization.accumulator_spec().unwrap().family,
            "{name}: precompute producer and catalog disagree about Planner family"
        );
    }
    let mut expected = expected_plan(name);
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
        // Both frontiers are legal: grouped maintained state, or maintained
        // per-series temporal state followed by a query-side grouped Sum.
        expected.partitioning = "per_entity";
        expected.root_operation = Some("aggregate");
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
            assert_eq!(node["operator"]["n"], 3, "{name}: wrong grouped limit");
            assert_eq!(node["operator"]["offset"], 0);
        }
        assert_eq!(
            node["operator"]["grouping"],
            json!({"labels":["label_0"],"without":false}),
            "{name}: wrong grouping"
        );
        let inputs = node["inputs"].as_array().unwrap();
        assert_eq!(inputs.len(), 1, "{name}: root must have one input");
        node = &nodes[&inputs[0].to_string()];
        if operation == "limit" {
            assert_eq!(node["op"], "logical");
            assert_eq!(node["operator"]["kind"], "sort");
            assert_eq!(node["operator"]["descending"], true);
            assert_eq!(
                node["operator"]["grouping"],
                json!({"labels":["label_0"],"without":false})
            );
            let inputs = node["inputs"].as_array().unwrap();
            assert_eq!(inputs.len(), 1);
            node = &nodes[&inputs[0].to_string()];
        }
    }
    assert_eq!(
        nodes.len(),
        if expected.root_operation == Some("limit") {
            4
        } else if expected.root_operation.is_some() {
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
    // Grouping does not shorten a temporal range. window_size is the semantic
    // window; the selected window_layout independently specifies stored panes.
    if name == "grouped-temporal-sum" {
        assert_eq!(leaf["binding"]["readout_lookback_ms"], 60_000);
        assert_eq!(
            leaf["binding"]["window_ms"],
            plan.precompute_plan.materializations[0].stored_window_ms()
        );
    }
    assert_eq!(
        summary["window_size"],
        if spatial && name != "grouped-temporal-sum" {
            5
        } else {
            60
        },
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
        if case.name == "quantile-ratio" {
            // The issue-754 generator defines the entire positive finite input
            // population. Supply its domain contract rather than certifying a
            // ratio from sample observations or weakening the admission rule.
            let fixture: Value = serde_yaml::from_str(include_str!(
                "../../promql-compliance/datasets/issue-754.yaml"
            ))
            .unwrap();
            let mut lower = f64::INFINITY;
            let mut upper = f64::NEG_INFINITY;
            let mut count = 0u64;
            for series in fixture["series"].as_array().unwrap() {
                let g = &series["generated_samples"];
                let n = |k: &str| g[k].as_f64().unwrap();
                assert!(n("multiplier") > 0.0 && n("modulo") > 0.0 && n("base") > 0.0);
                lower = lower.min(n("multiplier") * n("base"));
                upper = upper.max(n("multiplier") * (n("base") + n("modulo")));
                count += ((n("end_offset_seconds") - n("start_offset_seconds")) / n("step_seconds"))
                    .round() as u64
                    + 1;
            }
            let root = control_plane::query_parser::parse_query_expr_canonical(
                &case.expr,
                planner_types::types::AccuracyTarget::EpsilonDelta {
                    epsilon: 0.01,
                    delta: 0.01,
                },
            )
            .unwrap();
            let planner_types::pre_asap::QueryExpr::BinaryOp { lhs, rhs, .. } = root else {
                panic!("ratio fixture");
            };
            snapshot["implementation"]["data_snapshot_id"] = json!("issue-754-level1");
            snapshot["implementation"]["accuracy_evidence"][&case.expr] = json!({
                "query_string":case.expr,"data_snapshot_id":"issue-754-level1",
                "data_workload":snapshot["data_workload"],"source":"issue-754-finite-generator",
                "observed_at_unix_ms":9500,"valid_for_ms":60000,
                "quantile_operand_domains":([lhs,rhs].into_iter().map(|operand| json!({
                    "operand":operand,"lower":lower,"upper":upper,"max_samples":count,
                    "contract":"complete finite issue-754 generator population"})).collect::<Vec<_>>())
            });
        }
        let mut input: BackendLocalPlanningInput = serde_json::from_value(snapshot).unwrap();
        let (request, environment) = input.clone().into_physical_compilation_request().unwrap();
        let candidates = enumerate_exact_and_materialized_candidates(request).unwrap();
        let mut valid_plans = Vec::new();
        let mut quotes = Vec::new();
        let mut errors = Vec::new();
        for (candidate_index, candidate) in candidates.into_iter().enumerate() {
            match DeploymentPlanCompiler.compile_promql(candidate.clone(), environment.clone()) {
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
                    if matches!(case.name.as_str(), "grouped-rate" | "grouped-temporal-sum") {
                        let local = entry.nodes.values().all(|node| !matches!(node,
                            QueryPlanNode::ExactFallback { .. } | QueryPlanNode::Logical {
                                operator: control_plane::query_plan::residual::ResidualQueryOperator::ExactSubquery { .. }
                                    | control_plane::query_plan::residual::ResidualQueryOperator::CandidateExactSubquery { .. }, .. }));
                        if local {
                            assert_eq!(assert_selected_plan(&case.name, &plan), None,
                                "every admitted local candidate must preserve grouped/window semantics");
                        }
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
                    // Fixture-specific admission/selection costs. These do not
                    // establish a production optimum or require one split for
                    // every workload; the reversal test below covers placement.
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

/// Workload cost must reverse the admitted grouped temporal Sum split.
#[test]
fn grouped_temporal_sum_candidates_preserve_coverage_and_reverse_selection() {
    use asap_aware_mapping::cost_model::Cost;
    use asap_aware_mapping::{CostModel, Replacement, ReplacementSubDAG, TargetSubDAG};
    use planner_types::post_asap::SummaryExpr;
    use planner_types::pre_asap::{AggIntent, Reduction};
    struct PreferSplit {
        grouped: bool,
    }
    impl CostModel for PreferSplit {
        fn rank_candidates(
            &self,
            _: &AggIntent,
            candidates: &[SketchAlgorithm],
        ) -> Vec<SketchAlgorithm> {
            candidates.to_vec()
        }
        fn candidate_cost(
            &self,
            candidate: &ReplacementSubDAG,
            _: &TargetSubDAG<'_>,
        ) -> Option<Cost> {
            let grouped = matches!(&candidate.replacement, Replacement::Summary(node)
                if matches!(&node.expr, SummaryExpr::SummaryAgg { reduction: Reduction::Reduce(_), child, .. }
                    if matches!(child.expr, SummaryExpr::KeepPreAsap(_))));
            Some(Cost(if grouped == self.grouped { 1. } else { 1000. }))
        }
    }
    let expression = "sum by (label_0) (sum_over_time(data[1m]))";
    let canonical = control_plane::query_parser::parse_query_expr_canonical(
        expression,
        planner_types::types::AccuracyTarget::Exact,
    )
    .unwrap();
    let mut snapshot: Value = serde_json::from_str(include_str!(
        "../../docs/examples/asapquery-planning-snapshot.json"
    ))
    .unwrap();
    snapshot["query_workload"]["repeating_queries"][0]["query"] = json!(expression);
    let input: BackendLocalPlanningInput = serde_json::from_value(snapshot).unwrap();
    let (request, environment) = input.into_physical_compilation_request().unwrap();
    let mut plans = Vec::new();
    let mut candidates = Vec::new();
    for grouped in [false, true] {
        let mut candidate = request.clone();
        candidate.queries[0].selected_plan_root =
            control_plane::planner_selection::select_query(&canonical, &PreferSplit { grouped })
                .unwrap();
        let plan = DeploymentPlanCompiler
            .compile_promql(candidate.clone(), environment.clone())
            .unwrap();
        candidates.push(candidate);
        assert_eq!(assert_selected_plan("grouped-temporal-sum", &plan), None);
        // A stored five-second pane cannot shorten the semantic minute read.
        let mut incomplete = plan.clone();
        for entry in incomplete.query_plan.entries.values_mut() {
            for node in entry.nodes.values_mut() {
                if let QueryPlanNode::ReadMaterialization { binding } = node {
                    binding.readout_lookback_ms = Some(5_000);
                }
            }
        }
        assert!(std::panic::catch_unwind(|| assert_selected_plan(
            "grouped-temporal-sum",
            &incomplete
        ))
        .is_err());
        let entry = plan.query_plan.lookup(expression).unwrap();
        let root = &entry.nodes[&entry.root];
        let is_grouped = matches!(root, QueryPlanNode::ExactReadout { .. });
        assert_eq!(
            is_grouped, grouped,
            "controlled cost must change the executable split"
        );
        plans.push(plan);
    }
    assert_ne!(plans[0].query_plan, plans[1].query_plan);
    // Controlled scoped resource prices, not observed production measurements.
    // Maintenance-heavy grouped output loses in the first fixture; expensive
    // repeated query-side reduction makes it win in the second fixture.
    for prefer_grouped in [false, true] {
        let quotes = plans
            .iter()
            .zip(&candidates)
            .enumerate()
            .map(|(index, (plan, candidate))| {
                let manifest = manifest(plan, &candidate.queries).unwrap();
                let unit_costs = manifest
                    .components
                    .iter()
                    .map(|(key, demand)| {
                        let grouped_state = index == 1 && key.starts_with("state:");
                        let query_reduction =
                            demand.implementation["node"]["operator"]["kind"] == "aggregate";
                        let cost = if (!prefer_grouped && grouped_state)
                            || (prefer_grouped && query_reduction)
                        {
                            10_000.
                        } else {
                            1.
                        };
                        (key.clone(), cost)
                    })
                    .collect();
                WorkloadQuote {
                    manifest,
                    unit_costs,
                    executable: true,
                }
            })
            .collect();
        let evidence = WorkloadCostEvidence {
            backend_revision: BACKEND_REVISION.into(),
            planner_revision: PLANNER_REVISION.into(),
            data_snapshot_id: "grouped-sum-frontier-fixture".into(),
            model_version: "controlled-maintenance-query-costs".into(),
            observed_at_unix_ms: environment.observed_at_unix_ms,
            valid_for_ms: environment.max_evidence_age_ms,
            quotes,
        };
        let selected = control_plane::physical::workload_cost::select_lowest_cost_candidate(
            candidates.clone(),
            environment.clone(),
            &evidence,
        )
        .unwrap();
        assert_eq!(
            assert_selected_plan("grouped-temporal-sum", &selected),
            None
        );
        let entry = selected.query_plan.lookup(expression).unwrap();
        assert_eq!(
            matches!(entry.nodes[&entry.root], QueryPlanNode::ExactReadout { .. }),
            prefer_grouped
        );
        let report = selected.cost_comparison.unwrap();
        let selected_cost = report.component_costs.values().sum::<f64>();
        let minimum = report
            .candidate_evaluations
            .iter()
            .filter_map(|candidate| candidate.total_cost)
            .fold(f64::INFINITY, f64::min);
        assert_eq!(selected_cost, minimum);
    }
}
