//! Synthetic fixtures test integration decisions, not measured sketch benefits.
use asap_aware_mapping::{
    empirical_cost::{EmpiricalEvidenceProvider, EvidenceArtifact, EvidenceContext},
    CostModel,
};
use control_plane::{
    physical::post_asap::{
        bind_query_expr_with_cost_model, cost_model::ControlPlaneCostModel, PhysicalExpr,
        PostAsapPlan,
    },
    query_parser::parse_query_expr_canonical,
    types::AccuracyTarget,
};
use planner_types::{
    post_asap::{SketchAlgorithm, SketchParams, SummaryExpr, SummaryFamilyType, SummaryNode},
    pre_asap::AggIntent,
};
use serde_json::json;

fn frequency_comparison() -> (
    asap_aware_mapping::empirical_comparison::OfflineComparisonEvidence,
    asap_aware_mapping::empirical_comparison::OfflineComparisonRequest,
) {
    let (mut artifact, context) = fixture(&model(), &intent());
    let query =
        json!({"kind":"point_frequency", "value_type":"i64", "probe_set":"all_distinct_keys"});
    artifact.records[0].algorithm = SketchAlgorithm::Cms;
    artifact.records[0].params = SketchParams::Cms {
        width: 512,
        depth: 5,
    };
    artifact.records[0].error.as_mut().unwrap().mean = Some(0.2);
    artifact.records[1].algorithm = SketchAlgorithm::Cms;
    artifact.records[1].params = SketchParams::Cms {
        width: 1024,
        depth: 5,
    };
    artifact.records[1].error.as_mut().unwrap().mean = Some(0.001);
    for row in &mut artifact.records {
        row.error.as_mut().unwrap().query = query.clone();
        row.metrics.resources.cpu.build_cpu_ns =
            serde_json::from_value(json!({"value":1.0,"samples":3,"stddev":0.0})).unwrap();
        row.metrics.resources.cpu.read_cpu_ns = row.metrics.resources.cpu.build_cpu_ns.clone();
    }
    let bindings: Vec<_> = artifact
        .records
        .iter()
        .map(|r| json!({"record_id":r.id,"query":query}))
        .collect();
    let evidence = serde_json::from_value(json!({"schema_version":1,"timing_contract":"disjoint_live_state_v1",
        "sketch_evidence":artifact, "query_bindings":bindings,
        "exact_records":[{"id":"exact-test", "distribution":context.distribution,
            "environment":context.environment, "query":query,
            "measured_at_unix_seconds":100,"valid_until_unix_seconds":200,
            "provenance":{"command":"synthetic test","dataset":"test","source_revision":"test","repetitions":3},
            "metrics":{"empty_build_cpu_ns":{"value":1.0,"samples":3},
                "update_cpu_ns":{"value":100.0,"samples":3},
                "prepare_cpu_ns":{"value":1.0,"samples":3},
                "read_cpu_ns":{"value":100.0,"samples":3}}}]})).unwrap();
    let request = serde_json::from_value(json!({"context":context,
        "exact_environment":context.environment,"query":query,
        "accuracy":{"metric":"absolute relative frequency error","max_observed_mean":0.01,"minimum_trials":3},
        "workload":{"input_items_per_state":1000,"reads_per_state":100,"merges_per_state":0,"state_instances":1,"horizon_seconds":3600.0},
        "weights":{"cpu_ns_weight":1.0,"retained_byte_seconds_weight":0.0},"formal_minimums":null})).unwrap();
    (evidence, request)
}

fn point_frequency(accuracy: AccuracyTarget) -> AggIntent {
    control_plane::planner_selection::frequency(accuracy, Some(("key".into(), "7".into())))
}

/// A query-matched mean-error budget selects a larger measured CMS, while
/// preserving the formal workload bound and the requested point readout.
#[test]
fn offline_frequency_error_budget_changes_configuration() {
    let (evidence, request) = frequency_comparison();
    let empirical = model().with_offline_frequency_comparison(evidence, request);
    let intent = point_frequency(AccuracyTarget::Epsilon(0.01));
    let AggIntent::Extension { ext_kind, payload } = &intent else {
        unreachable!()
    };
    let asap_aware_mapping::Implementation::Sketch(selected) =
        empirical.realize_extension(ext_kind, payload)
    else {
        panic!(
            "expected a measured frequency sketch: {:?}",
            empirical.offline_frequency_recommendation(payload)
        );
    };
    assert_eq!(
        selected.params(),
        &SketchParams::Cms {
            width: 1024,
            depth: 5
        }
    );
    let recommendation = empirical.offline_frequency_recommendation(payload).unwrap();
    assert!(recommendation.checked_formal_minimums);
    assert!(recommendation.candidates[0]
        .rejection
        .as_ref()
        .unwrap()
        .contains("observed error"));

    let scan =
        parse_query_expr_canonical("offline_metric{key=~\".+\"}", AccuracyTarget::Exact).unwrap();
    let expr = planner_types::pre_asap::QueryExpr::Aggregate {
        reduction: planner_types::pre_asap::Reduction::PerEntity,
        measures: vec![intent],
        output_names: vec![],
        having: None,
        child: std::rc::Rc::new(scan),
    };
    let PhysicalExpr::Committed(PostAsapPlan::Summary(node)) =
        bind_query_expr_with_cost_model(&expr, &empirical).unwrap()
    else {
        panic!("expected summary")
    };
    assert_eq!(
        sketch(&node).1,
        &SketchParams::Cms {
            width: 1024,
            depth: 5
        }
    );
    assert!(
        matches!(&node.expr, SummaryExpr::SummaryEstimate { query: planner_types::post_asap::SketchQuery::PointCount {
        key: planner_types::pre_asap::ColumnRef::Named(key), value: Some(value)
    }, .. } if key == "key" && value == "7")
    );
}

/// An infeasible empirical budget, stale context, exact request, or missing
/// item filter must retain exact execution, never invent a default sketch.
#[test]
fn offline_frequency_comparison_falls_back_when_inapplicable() {
    for scenario in ["error", "stale", "exact", "missing_item", "formal_minimum"] {
        let (evidence, mut request) = frequency_comparison();
        if scenario == "error" {
            request.accuracy.max_observed_mean = 0.0;
        }
        if scenario == "stale" {
            request.context.now_unix_seconds = 201;
        }
        let empirical = model().with_offline_frequency_comparison(evidence, request);
        let intent = match scenario {
            "exact" => point_frequency(AccuracyTarget::Exact),
            "missing_item" => {
                control_plane::planner_selection::frequency(AccuracyTarget::Epsilon(0.01), None)
            }
            "formal_minimum" => point_frequency(AccuracyTarget::Epsilon(0.001)),
            _ => point_frequency(AccuracyTarget::Epsilon(0.01)),
        };
        let AggIntent::Extension { ext_kind, payload } = intent else {
            unreachable!()
        };
        assert!(
            matches!(
                empirical.realize_extension(&ext_kind, &payload),
                asap_aware_mapping::Implementation::PassThrough
            ),
            "{scenario}"
        );
    }
}

/// An unsupported cheap CMS width must not hide a valid measured alternative.
#[test]
fn offline_frequency_filters_backend_layout_before_comparison() {
    let (mut evidence, request) = frequency_comparison();
    let mut unsupported = evidence.sketch_evidence.records[1].clone();
    unsupported.id = "unsupported-cheap-width".into();
    unsupported.params = SketchParams::Cms {
        width: 1500,
        depth: 5,
    };
    unsupported
        .metrics
        .resources
        .cpu
        .update_cpu_ns
        .as_mut()
        .unwrap()
        .value = 0.001;
    let mut query_binding = evidence.query_bindings[1].clone();
    query_binding.record_id = unsupported.id.clone();
    evidence.query_bindings.push(query_binding);
    evidence.sketch_evidence.records.push(unsupported);
    let empirical = model().with_offline_frequency_comparison(evidence, request);
    let AggIntent::Extension { payload, .. } = point_frequency(AccuracyTarget::Epsilon(0.01))
    else {
        unreachable!()
    };
    let recommendation = empirical
        .offline_frequency_recommendation(&payload)
        .unwrap();
    assert_eq!(
        recommendation.selected_sketch().unwrap().params,
        SketchParams::Cms {
            width: 1024,
            depth: 5
        }
    );
    assert!(recommendation
        .candidates
        .iter()
        .all(|c| c.record_id != "unsupported-cheap-width"));
}

fn fixture(
    model: &ControlPlaneCostModel,
    intent: &AggIntent,
) -> (EvidenceArtifact, EvidenceContext) {
    let distribution = json!({"id":"synthetic-unit-test", "family":"uniform", "sample_count":1000,
        "distinct_count":100, "parameters":{"seed":42}});
    let environment = json!({"id":"test-machine", "cpu":"test", "os":"test", "runtime":"test",
        "implementation":"synthetic-test-fixture", "implementation_version":"v1"});
    let records: Vec<_> = [(SketchAlgorithm::Cms,20.0), (SketchAlgorithm::CountSketch,10.0), (SketchAlgorithm::UnivMon,30.0)].into_iter().map(|(algorithm,value)| {
        let params = model.size_params(algorithm.clone(), intent, 0.01, 0.01);
        json!({"id":format!("test-{algorithm:?}"), "algorithm":algorithm, "params":params,
            "distribution":distribution, "environment":environment,
            "measured_at_unix_seconds":100, "valid_until_unix_seconds":200,
            "provenance":{"command":"synthetic integration fixture, not a real benchmark", "dataset":"test", "source_revision":"test", "repetitions":3},
            "metrics":{"update_cpu_ns":{"value":value,"stddev":1.0,"samples":3}},
            "error":{"metric":"absolute relative frequency error", "mean":0.0, "max":null, "trials":3,
                "ground_truth_method":"synthetic fixture", "query":{"kind":"point_frequency"}}})
    }).collect();
    let artifact = serde_json::from_value(
        json!({"schema_version":1,"benchmark_version":"synthetic-test-v1",
        "model_version":"empirical-update-cpu-v1","records":records}),
    )
    .unwrap();
    let context = serde_json::from_value(
        json!({"distribution":distribution,"environment":environment,"now_unix_seconds":150}),
    )
    .unwrap();
    (artifact, context)
}

fn intent() -> AggIntent {
    AggIntent::Count {
        accuracy: AccuracyTarget::Epsilon(0.01),
    }
}
fn candidates() -> Vec<SketchAlgorithm> {
    vec![SketchAlgorithm::Cms, SketchAlgorithm::CountSketch]
}
fn model() -> ControlPlaneCostModel {
    ControlPlaneCostModel::new(AccuracyTarget::Epsilon(0.01))
}

fn bound(model: &ControlPlaneCostModel) -> std::rc::Rc<SummaryNode> {
    let query = parse_query_expr_canonical(
        "count_over_time(offline_metric[5m])",
        AccuracyTarget::Epsilon(0.01),
    )
    .unwrap();
    let PhysicalExpr::Committed(PostAsapPlan::Summary(node)) =
        bind_query_expr_with_cost_model(&query, model).unwrap()
    else {
        panic!("expected summary binding")
    };
    node
}

fn sketch(node: &SummaryNode) -> (&SketchAlgorithm, &SketchParams) {
    match &node.expr {
        SummaryExpr::SummaryEstimate { summary_input, .. } => sketch(summary_input),
        SummaryExpr::SummaryAgg {
            family: SummaryFamilyType::Sketch(kind, _),
            ..
        } => (kind.algorithm(), kind.params()),
        other => panic!("expected sketch, got {other:?}"),
    }
}

/// The actual control-plane parser/binder selects the lower measured update
/// cost while preserving the selected algorithm's normal parameter sizing.
#[test]
fn offline_update_evidence_changes_typed_binding() {
    let default = model();
    let (artifact, context) = fixture(&default, &intent());
    let empirical =
        model().with_offline_evidence(EmpiricalEvidenceProvider::new(artifact, context).unwrap());
    assert_eq!(
        default.rank_candidates(&intent(), &candidates()),
        candidates()
    );
    assert_eq!(
        empirical.rank_candidates(&intent(), &candidates()),
        vec![SketchAlgorithm::CountSketch, SketchAlgorithm::Cms]
    );
    let default_bound = bound(&default);
    let measured_bound = bound(&empirical);
    assert_eq!(sketch(&default_bound).0, &SketchAlgorithm::Cms);
    assert_eq!(sketch(&measured_bound).0, &SketchAlgorithm::CountSketch);
    assert_eq!(
        sketch(&measured_bound).1,
        &default.size_params(SketchAlgorithm::CountSketch, &intent(), 0.01, 0.01)
    );
    assert!(default_bound.guarantee.is_some());
    assert!(measured_bound.guarantee.is_some());
    // Observed zero point-frequency error has no effect on formal sizing.
    for kind in candidates() {
        assert_eq!(
            empirical.size_params(kind.clone(), &intent(), 0.01, 0.01),
            default.size_params(kind, &intent(), 0.01, 0.01)
        );
    }
}

/// Stale/missing/environment-mismatched data and parameters for another
/// workload accuracy all fall back to the deployment's existing preference.
#[test]
fn incompatible_evidence_preserves_deployment_behavior() {
    for scenario in ["missing", "stale", "environment", "workload_sizing"] {
        let (mut artifact, mut context) = fixture(&model(), &intent());
        let mut deployment = model();
        match scenario {
            "missing" => {
                artifact.records.remove(1);
            }
            "stale" => context.now_unix_seconds = 201,
            "environment" => context.environment.cpu = "different CPU".into(),
            "workload_sizing" => {
                deployment = ControlPlaneCostModel::new(AccuracyTarget::Epsilon(0.001))
            }
            _ => unreachable!(),
        }
        let empirical = deployment
            .with_offline_evidence(EmpiricalEvidenceProvider::new(artifact, context).unwrap());
        assert_eq!(
            empirical.rank_candidates(&intent(), &candidates()),
            candidates(),
            "{scenario}"
        );
        assert_eq!(
            sketch(&bound(&empirical)).0,
            &SketchAlgorithm::Cms,
            "{scenario}"
        );
    }
}

/// Binary operations over these approximate sketch values retain explicit
/// fallback even though the warm tier now supports exact additive binaries.
#[test]
fn binary_summary_has_explicit_warm_tier_fallback() {
    use control_plane::query_plan::{FallbackPolicy, InstantExecution, QueryPlanNode};
    use planner_types::{post_asap::BinaryOperator, pre_asap::BinaryOpKind};
    let child = bound(&model());
    let root = std::rc::Rc::new(SummaryNode {
        expr: SummaryExpr::BinaryOp {
            timing: planner_types::post_asap::ExecutionTiming::ReadTime,
            lhs: child.clone(),
            rhs: child.clone(),
            operator: BinaryOperator {
                checked_relative_division: false,
                checked_finite_division: false,
                kind: BinaryOpKind::Arithmetic(planner_types::pre_asap::ArithmeticOpKind::Div),
                vector_match: None,
            },
        },
        schema: child.schema.clone(),
        guarantee: None,
    });
    let plan = control_plane::query_plan::compile_bound_mapped(
        "test".into(),
        "left / right".into(),
        &root,
        InstantExecution {
            lookback_ms: 300000,
            full_history: false,
            cumulative_readout: false,
        },
        FallbackPolicy::Reject,
        |_, _| panic!("unsupported binary plan must not bind a materialization"),
        |_, _| {},
    )
    .unwrap();
    assert!(
        matches!(&plan.nodes[&plan.root], QueryPlanNode::ExactFallback { reason } if !reason.is_empty())
    );
    assert!(plan.materialization_bindings().is_empty());
}
