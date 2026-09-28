//! Level 1: inspect every supported candidate and binding rejection, without cost selection.
use asap_types::sds::SummaryOperator;
use control_plane::physical::compiler::{
    CompiledPhysicalPlan, DeploymentPlanCompiler, QueryFrontend,
};
use control_plane::physical::executable_binding::validate_query_plan;
use control_plane::physical::workload_cost::{
    compile_candidates_for_pricing, enumerate_exact_and_materialized_candidates,
    CandidateEvaluationStatus,
};
use control_plane::query_plan::QueryPlanNode;
use planner_types::post_asap::{ExactKind, SketchAlgorithm, SummaryFamilyType};
use serde_json::{json, Value};

#[path = "support/issue754_workload.rs"]
mod workload;
use workload::Suite;

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

// Expectations describe semantic structure, never a cost-selected winner.
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

// Inspect the Planner expression, since the adapter only stores sort direction.
fn assert_rate_sort_expression(dag: &asap_types::executable_plan::OwnedPostAsapDag) {
    for sort in &dag.nodes {
        let Some(spec) = sort.payload["operation"].get("Sort") else {
            continue;
        };
        let inputs: Vec<_> = dag
            .edges
            .iter()
            .filter(|edge| edge.consumer == sort.id)
            .collect();
        assert_eq!(inputs.len(), 1, "rate ranking requires one producer");
        let producer = dag
            .nodes
            .iter()
            .find(|node| node.id == inputs[0].producer)
            .unwrap();
        assert_eq!(
            producer.payload["operation"], "FinalizeExactAccumulator",
            "ranking must consume finalized per-series rates"
        );
        let fields = producer.output_schema["fields"].as_array().unwrap();
        let value = fields
            .iter()
            .position(|field| field["name"] == "value")
            .unwrap();
        assert_eq!(
            spec["keys"],
            json!([{
                "ascending": false, "expr": {"Column": value}, "nulls_first": false
            }]),
            "TopK must rank rate values, not timestamps or labels"
        );
        let partition = fields
            .iter()
            .position(|field| field["name"] == "label_0")
            .unwrap();
        assert_eq!(spec["partition_by"], json!([partition]));
    }
}

// A descending sort over a timestamp or label must fail the Level 1 contract.
#[test]
fn topk_rate_sort_contract_rejects_wrong_value_expression() {
    let artifact: Value = serde_json::from_str(include_str!(
        "../../docs/evaluation/issue754-human-review/topk-rate.json"
    ))
    .unwrap();
    let dag: asap_types::executable_plan::OwnedPostAsapDag =
        serde_json::from_value(artifact["query_plan"]["selected_dags"]["compat-query-0"].clone())
            .unwrap();
    assert_rate_sort_expression(&dag);
    for column in [0, 2] {
        let mut wrong = dag.clone();
        let sort = wrong
            .nodes
            .iter_mut()
            .find(|node| node.payload["operation"].get("Sort").is_some())
            .unwrap();
        sort.payload["operation"]["Sort"]["keys"][0]["expr"] = json!({"Column": column});
        assert!(std::panic::catch_unwind(|| assert_rate_sort_expression(&wrong)).is_err());
    }
}

fn assert_native_ranking(installed: &asap_types::query_plan::QueryPlanEntry) {
    let physical = if installed.physical_vector_binding().is_some() {
        installed.recover_vector_physical_dag().unwrap()
    } else {
        installed.recover_population_physical_dag().unwrap()
    };
    let inputs = physical.input_contracts().collect::<Vec<_>>();
    assert_eq!(
        inputs.len(),
        1,
        "spatial ranking binds one complete population"
    );
    let input = &inputs[0].1.schema;
    assert!(input
        .fields
        .iter()
        .any(|field| field.name == "$promql_series_identity"));
    let value_column = input
        .fields
        .iter()
        .position(|field| field.name == "value")
        .unwrap();
    let group_column = input
        .fields
        .iter()
        .position(|field| field.name == "label_0")
        .unwrap();
    let program: Value = serde_json::from_slice(&physical.encode().unwrap()).unwrap();
    let operations: Vec<_> = program["nodes"]
        .as_object()
        .unwrap()
        .values()
        .filter_map(|node| node.get("Operator"))
        .collect();
    assert_eq!(operations.len(), 2);
    let sort = operations
        .iter()
        .find(|node| node["operator"]["kind"].get("Sort").is_some())
        .unwrap();
    assert_eq!(
        sort["operator"]["kind"]["Sort"]["keys"],
        json!([{"column":value_column,"descending":true,"nulls_first":false}])
    );
    assert_eq!(
        sort["operator"]["kind"]["Sort"]["groups"],
        json!([group_column])
    );
    let limit = operations
        .iter()
        .find(|node| node["operator"]["kind"].get("Limit").is_some())
        .unwrap();
    assert_eq!(
        limit["operator"]["kind"]["Limit"],
        json!({"n":3,"offset":0,"groups":[group_column]})
    );
    assert_eq!(sort["inputs"], json!([inputs[0].0]));
    let sort_id: u64 = program["nodes"]
        .as_object()
        .unwrap()
        .iter()
        .find(|(_, node)| node.get("Operator") == Some(*sort))
        .unwrap()
        .0
        .parse()
        .unwrap();
    assert_eq!(limit["inputs"], json!([sort_id]));
}

// Check the computation on each side of the persisted frontier, not just the
// presence of Sum: per-series Rate must be finalized before grouped aggregation.
fn assert_native_grouped_rate(plan: &CompiledPhysicalPlan) -> bool {
    let entry = plan.query_plan.entries.values().next().unwrap();
    let query = entry.recover_vector_physical_dag().unwrap();
    let query: Value = serde_json::from_slice(&query.encode().unwrap()).unwrap();
    let installed = &plan.precompute_plan.executable_dags[&entry.query_id];
    let stored = !installed.native_programs.is_empty();
    let maintenance;
    let aggregation = if stored {
        assert_eq!(installed.native_programs.len(), 1);
        let sink = *installed.native_programs.keys().next().unwrap();
        maintenance = serde_json::from_slice::<Value>(
            &installed
                .native_program(sink)
                .unwrap()
                .unwrap()
                .encode()
                .unwrap(),
        )
        .unwrap();
        &maintenance
    } else {
        &query
    };
    let nodes = aggregation["nodes"].as_object().unwrap();
    let sum = nodes
        .values()
        .filter_map(|node| node.get("Operator"))
        .find(|op| op["operator"]["kind"].get("SummaryBuild").is_some())
        .expect("grouped Rate candidate must explicitly build Sum");
    let build = &sum["operator"]["kind"]["SummaryBuild"];
    assert!(build["family"].to_string().contains("Sum"));
    assert_eq!(sum["inputs"].as_array().unwrap().len(), 1);
    let producer = &nodes[&sum["inputs"][0].to_string()];
    let fields = producer
        .get("Input")
        .map(|input| &input["schema"]["fields"])
        .unwrap_or(&producer["Operator"]["operator"]["output"]["fields"])
        .as_array()
        .unwrap();
    let groups: Vec<_> = build["groups"]
        .as_array()
        .unwrap()
        .iter()
        .map(|column| {
            fields[column.as_u64().unwrap() as usize]["name"]
                .as_str()
                .unwrap()
        })
        .collect();
    assert_eq!(groups, ["label_0"]);
    let value = build["value"].as_u64().unwrap() as usize;
    assert_eq!(fields[value]["dtype"], json!({"Plain":"float64"}));
    if stored {
        let rate = &producer["Operator"]["operator"]["kind"]["Readout"];
        assert_eq!(rate["statistic"], "Rate");
        assert_eq!(rate["parameters"]["logical_lookback_ms"], "60000");
        assert!(!query.to_string().contains("SummaryBuild"));
        assert!(query.to_string().contains("Readout"));
        assert!(
            plan.precompute_plan
                .materializations
                .iter()
                .all(|state| state.window_size == 60 && state.slide_interval == 10),
            "stored Sum must cover each 60-second query window at the fixture's 10-second cadence"
        );
    } else {
        assert!(
            producer.get("Input").is_some(),
            "Sum consumes the bound Rate vector"
        );
        let wire = serde_json::to_value(entry).unwrap();
        let bound_nodes = wire["nodes"].as_object().unwrap();
        let root = &bound_nodes[&wire["root"].to_string()];
        assert_eq!(root["op"], "physical");
        assert_eq!(root["inputs"].as_array().unwrap().len(), 1);
        let rate = &bound_nodes[&root["inputs"][0].to_string()];
        assert_eq!(rate["op"], "exact_readout");
        assert_eq!(rate["readout"], "rate");
        let state = &bound_nodes[&rate["input"].to_string()];
        assert_eq!(state["op"], "read_materialization");
        assert_eq!(state["binding"]["readout_lookback_ms"], 60_000);
        assert_eq!(state["binding"]["output_grouping"]["mode"], "per_entity");
    }
    stored
}

fn assert_candidate_plan(name: &str, plan: &CompiledPhysicalPlan) -> Option<String> {
    if name == "topk-rate" {
        for dag in plan.query_plan.selected_dags.values() {
            assert_rate_sort_expression(dag);
        }
    }
    for materialization in &plan.precompute_plan.materializations {
        let definition = plan
            .summary_catalog
            .outputs
            .get(&materialization.policy_fingerprint().into())
            .expect("precompute producer has no catalog definition");
        let semantics = &plan.summary_catalog.definitions[&definition.definition_id];
        if let asap_types::summary_semantics::SummarySemantics::Planner { fragment } = &semantics.semantics {
            assert!(fragment.dataset_identity.is_some(), "persisted Planner output lacks dataset identity");
            assert_eq!(fragment.dataset_identity, plan.precompute_plan.ingest.dataset_identity);
        }
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
    if name == "grouped-rate"
        && plan
            .query_plan
            .entries
            .values()
            .next()
            .unwrap()
            .physical_vector_binding()
            .is_some()
    {
        assert_native_grouped_rate(plan);
        return None;
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
    if node["operator"]["kind"] == "current_series"
        && matches!(name, "spatial-sum" | "spatial-quantile")
    {
        assert_eq!(nodes.len(), 1);
        assert!(materializations.is_empty());
        let population = &node["operator"]["population"];
        assert_eq!(population["metric"], "data");
        assert_eq!(
            population["grouping"],
            json!({"labels":["label_0"],"without":false})
        );
        assert_eq!(population["lookback_ms"], 5000);
        assert_eq!(
            node["operator"]["readout"],
            if name == "spatial-sum" {
                json!({"kind":"sum"})
            } else {
                json!({"kind":"quantile","q":0.9})
            }
        );
        return None;
    }
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
        assert_eq!(node["operator"]["readout"], json!({"kind":"snapshot"}));
        let installed = plan.query_plan.entries.values().next().unwrap();
        assert_native_ranking(installed);
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
    let native_rate = name == "topk-rate" && node["op"] == "physical";
    if native_rate {
        let installed = plan.query_plan.entries.values().next().unwrap();
        assert_native_ranking(installed);
        let inputs = node["inputs"].as_array().unwrap();
        assert_eq!(inputs.len(), 1);
        node = &nodes[&inputs[0].to_string()];
        expected.root_operation = None;
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
        if native_rate {
            3
        } else if expected.root_operation == Some("limit") {
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
    let suite: Suite = workload::suite();
    assert_eq!(suite.queries.len(), 10, "the issue-754 contract changed");
    for case in suite.queries {
        let expected = expected_plan(&case.name);
        let input = workload::input(&case);
        let (request, environment) = input.clone().into_physical_compilation_request().unwrap();
        if case.name == "spatial-quantile" {
            // The fixture's admissible sketch families must reach deployment
            // costing; the initially preferred family is not the inventory.
            let mut families = std::collections::BTreeSet::new();
            for forest in &request.planner_candidate_forests {
                for query in forest {
                    let dag =
                        planner_types::post_asap::compile_executable_dag(&query.selected_plan_root)
                            .unwrap();
                    for node in dag.nodes {
                        if let planner_types::post_asap::ExecutableOperatorPayload::SummaryAgg {
                            family: SummaryFamilyType::Sketch(kind, _),
                            ..
                        } = node.payload
                        {
                            families.insert(format!("{:?}", kind.algorithm()));
                        }
                    }
                }
            }
            assert!(
                families.contains("Kll")
                    || request
                        .planner_selection_trace
                        .iter()
                        .flat_map(|trace| trace["groups"].as_array().into_iter().flatten())
                        .flat_map(|group| group["rejected"].as_array().into_iter().flatten())
                        .any(|rejection| rejection["description"]
                            .as_str()
                            .is_some_and(|s| s.contains("Kll"))
                            && rejection["reason"]
                                .as_str()
                                .is_some_and(|s| s.contains("does not satisfy"))),
                "a missing KLL candidate needs an explicit accuracy rejection"
            );
            let mut relaxed = input.clone();
            relaxed.query_workload.repeating_queries.as_mut().unwrap()[0]
                .requirements
                .accuracy = planner_types::workload::AccuracyRequirement::Explicit(
                planner_types::types::AccuracyTarget::Epsilon(0.05),
            );
            let (relaxed, _) = relaxed.into_physical_compilation_request().unwrap();
            let roots = relaxed
                .planner_candidate_forests
                .iter()
                .flatten()
                .map(|query| format!("{:?}", query.selected_plan_root))
                .collect::<Vec<_>>();
            assert!(
                roots.iter().any(|root| root.contains("Kll")),
                "admissible KLL disappeared before deployment costing"
            );
            assert!(
                roots.iter().any(|root| root.contains("DDSketch")),
                "admissible DDSketch disappeared before deployment costing"
            );
            assert!(
                families.contains("DDSketch"),
                "DDSketch disappeared before deployment costing"
            );
            assert!(
                request.planner_selection_trace.iter().any(|trace| trace
                    ["computation_search_scope"]["joint_workload_search_exhaustive"]
                    == false),
                "root substitutions must not be reported as exhaustive joint search"
            );
        }
        let heap_families: &[&str] = match case.name.as_str() {
            "spatial-topk" => &["CountSketchWithHeap"],
            "topk-rate" => &["CmsWithHeap", "CountSketchWithHeap"],
            _ => &[],
        };
        let mut heap_roots =
            heap_families
                .iter()
                .map(|family| {
                    let trace = request
                        .planner_selection_trace
                        .iter()
                        .find(|trace| {
                            trace["stage"] == "planner.physical_candidate"
                                && trace["physical_dag"].to_string().contains(family)
                        })
                        .unwrap_or_else(|| {
                            panic!("{}: Planner must expose native {family}", case.name)
                        });
                    let program =
                        asap_physical_operators::physical_planner::CompiledPhysicalDag::decode(
                            &serde_json::to_vec(&trace["physical_dag"]).unwrap(),
                        )
                        .unwrap();
                    assert_eq!(program.input_contracts().count(), 1);
                    let encoded = trace["physical_dag"].to_string();
                    assert!(
                        encoded.contains("KeyedSummaryBuild") && encoded.contains("KeyedReadout")
                    );
                    assert!(encoded.contains("$promql_series_identity"));
                    assert!(
                        !encoded.contains("CurrentSeries"),
                        "the bound source supplies this evaluation's vector"
                    );
                    assert!(trace["guarantee"].to_string().contains("topk_max_distinct_items"),
                "fixture lacks an enforced bound; cardinality estimates cannot certify a heap");
                    trace["logical_root_id"].as_str().unwrap().to_owned()
                })
                .collect::<Vec<_>>();
        if case.name == "topk-rate" {
            for family in heap_families {
                let trace = request
                    .planner_selection_trace
                    .iter()
                    .find(|trace| {
                        trace["stage"] == "planner.physical_candidate"
                            && trace["physical_candidate"].to_string().contains(family)
                    })
                    .unwrap_or_else(|| panic!("missing fixed-window {family} candidate"));
                let split = asap_physical_operators::physical_planner::PhysicalCandidate::decode(
                    &serde_json::to_vec(&trace["physical_candidate"]).unwrap(),
                )
                .unwrap();
                let maintenance =
                    String::from_utf8(split.precompute.as_ref().unwrap().encode().unwrap())
                        .unwrap();
                let query = String::from_utf8(split.query.encode().unwrap()).unwrap();
                assert!(maintenance.contains("Rate") && maintenance.contains("KeyedSummaryBuild"));
                assert!(query.contains("KeyedReadout") && !query.contains("KeyedSummaryBuild"));
                assert_eq!(split.materialized_outputs.len(), 1);
                assert!(split
                    .query
                    .input_contracts()
                    .all(|(id, _)| split.materialized_outputs.contains_key(&id)));
                heap_roots.push(trace["logical_root_id"].as_str().unwrap().to_owned());
            }
        }
        let candidates = enumerate_exact_and_materialized_candidates(request).unwrap();
        let mut valid_plans = Vec::new();
        let mut grouped_rate_placements = std::collections::BTreeSet::new();
        let (_, admission) = compile_candidates_for_pricing(
            candidates.clone(),
            environment.clone(),
            QueryFrontend::PromQl,
        );
        assert_eq!(admission.len(), candidates.len());
        for result in &admission {
            assert!(
                result.total_cost.is_none(),
                "Level 1 must not price candidates"
            );
            match result.status {
                CandidateEvaluationStatus::AwaitingQuote => assert!(result.plan_id.is_some()),
                CandidateEvaluationStatus::CompilationFailed => assert!(result
                    .unavailable_reason
                    .as_ref()
                    .is_some_and(|reason| !reason.is_empty())),
                ref status => panic!("unexpected pre-pricing status: {status:?}"),
            }
        }
        if let Ok(directory) = std::env::var("ASAP_LEVEL1_ARTIFACT_DIR") {
            std::fs::create_dir_all(&directory).unwrap();
            std::fs::write(
                std::path::Path::new(&directory).join(format!("{}.admission.json", case.name)),
                serde_json::to_vec_pretty(&admission).unwrap(),
            )
            .unwrap();
        }
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
                    {
                        let local = entry.nodes.values().all(|node| !matches!(node,
                            QueryPlanNode::ExactFallback { .. } | QueryPlanNode::Logical {
                                operator: control_plane::query_plan::residual::ResidualQueryOperator::ExactSubquery { .. }
                                    | control_plane::query_plan::residual::ResidualQueryOperator::CandidateExactSubquery { .. }, .. }));
                        if local {
                            assert_eq!(
                                assert_candidate_plan(&case.name, &plan),
                                None,
                                "every admitted local candidate must preserve query semantics"
                            );
                        }
                    }
                    if case.name == "grouped-rate" && entry.physical_vector_binding().is_some() {
                        grouped_rate_placements.insert(assert_native_grouped_rate(&plan));
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
                    valid_plans.push(plan);
                }
                Err(error) => errors.push(error.to_string()),
            }
        }
        if case.name == "grouped-rate" {
            assert_eq!(
                grouped_rate_placements,
                std::collections::BTreeSet::from([false, true]),
                "Planner must expose query-time and precomputed grouped Rate/Sum: {errors:?}"
            );
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
        for root_id in heap_roots {
            let heap = admission
                .iter()
                .filter(|candidate| candidate.logical_root_ids.contains(&root_id))
                .collect::<Vec<_>>();
            assert!(
                !heap.is_empty(),
                "physical heap candidate disappeared before admission"
            );
            assert!(
                heap.iter().all(|candidate| candidate.status
                    == CandidateEvaluationStatus::CompilationFailed
                    && candidate.total_cost.is_none()
                    && candidate
                        .unavailable_reason
                        .as_ref()
                        .is_some_and(|reason| reason.contains("accuracy guarantee"))),
                "missing proof must be an explicit admission failure: {heap:?}"
            );
        }
    }
}

/// Ensembles retain every query and coherent shared producer bindings in each
/// exposed workload candidate. This does not claim exhaustive joint search.
#[test]
fn ensembles_preserve_all_queries_and_shared_output_identity() {
    for (name, cases) in workload::ensembles() {
        let (request, env) = workload::ensemble_input(&cases)
            .into_physical_compilation_request()
            .unwrap();
        assert_eq!(request.queries.len(), cases.len());
        let candidates = enumerate_exact_and_materialized_candidates(request).unwrap();
        let (_, admission) =
            compile_candidates_for_pricing(candidates.clone(), env.clone(), QueryFrontend::PromQl);
        assert_eq!(admission.len(), candidates.len());
        let mut bound = 0;
        let mut shared = false;
        for (index, candidate) in candidates.into_iter().enumerate() {
            let Ok(plan) = DeploymentPlanCompiler.compile_promql(candidate, env.clone()) else {
                assert_eq!(
                    admission[index].status,
                    CandidateEvaluationStatus::CompilationFailed
                );
                assert!(admission[index]
                    .unavailable_reason
                    .as_ref()
                    .is_some_and(|s| !s.is_empty()));
                continue;
            };
            bound += 1;
            assert_eq!(plan.query_plan.entries.len(), cases.len());
            assert_eq!(plan.precompute_plan.ingest.dataset_identity.as_ref(), Some(&env.dataset_identity));
            for definition in plan.summary_catalog.definitions.values() {
                if let asap_types::summary_semantics::SummarySemantics::Planner { fragment } = &definition.semantics {
                    assert_eq!(fragment.dataset_identity.as_ref(), Some(&env.dataset_identity));
                }
            }
            let mut consumers = std::collections::BTreeMap::new();
            for case in &cases {
                let entry = plan
                    .query_plan
                    .lookup(&case.expr)
                    .expect("ensemble query disappeared");
                assert!(entry.nodes.contains_key(&entry.root));
                if let Some(dag) = plan.precompute_plan.executable_dags.get(&entry.query_id) {
                    dag.validate().unwrap();
                    validate_query_plan(dag, entry).unwrap();
                }
                for binding in entry.materialization_bindings() {
                    let reference = &binding.stored_output_reference;
                    let output = &plan.summary_catalog.outputs[&reference.stored_output_id];
                    assert_eq!(reference.definition_id, output.definition_id);
                    consumers
                        .entry(reference.stored_output_id)
                        .or_insert_with(std::collections::BTreeSet::new)
                        .insert(entry.query_id.clone());
                }
            }
            let producers: std::collections::BTreeSet<_> = plan
                .precompute_plan
                .materializations
                .iter()
                .map(|m| m.policy_fingerprint())
                .collect();
            assert_eq!(producers.len(), plan.precompute_plan.materializations.len());
            shared |= consumers.values().any(|readers| readers.len() > 1);
            if let Ok(directory) = std::env::var("ASAP_LEVEL1_ARTIFACT_DIR") {
                let root = std::path::Path::new(&directory)
                    .join("ensembles")
                    .join(&name);
                std::fs::create_dir_all(&root).unwrap();
                std::fs::write(
                    root.join(format!("candidate-{index}.json")),
                    serde_json::to_vec_pretty(&plan).unwrap(),
                )
                .unwrap();
                std::fs::write(
                    root.join(format!("candidate-{index}.dot")),
                    control_plane::physical::plan_dot::render(&plan),
                )
                .unwrap();
                std::fs::write(
                    root.join("admission.json"),
                    serde_json::to_vec_pretty(&admission).unwrap(),
                )
                .unwrap();
            }
        }
        assert!(bound > 0, "{name}: no bound ensemble candidate");
        if name == "shared-rate" {
            assert!(shared, "Rate consumers never share a stored producer");
        }
    }
}
