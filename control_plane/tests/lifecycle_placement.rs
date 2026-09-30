//! Precompute-or-query-time placement follows the backend's lifecycle costs.
use control_plane::physical::compiler::{
    BackendLocalPlanningInput, CompiledPhysicalPlan, DeploymentPlanCompiler,
    PhysicalCompilationRequest,
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
    selected_plan_with(input, |_| {})
}

/// [`selected_plan`] after `edit` adjusts the per-query compilation inputs.
fn selected_plan_with(
    input: BackendLocalPlanningInput,
    edit: impl FnOnce(&mut PhysicalCompilationRequest),
) -> CompiledPhysicalPlan {
    let (request, environment) = input.into_physical_compilation_request().unwrap();
    let mut candidate = enumerate_exact_and_materialized_candidates(request)
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert!(candidate.allow_mixed_summary_and_exact_execution);
    edit(&mut candidate);
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
    let queries: Vec<_> = queries.iter().map(|query| (*query, 10_000, 0.1)).collect();
    priced_decisions(&queries)
}

/// Lifecycle decisions for exact `(query, evaluation interval ms, read unit cost)`.
fn priced_decisions(queries: &[(&str, u32, f64)]) -> Vec<Value> {
    let queries: Vec<_> = queries
        .iter()
        .map(|&(query, interval, read)| (query, interval, 0, read))
        .collect();
    phased_decisions(&queries)
}

/// [`priced_decisions`] with each query's evaluation phase in ms.
fn phased_decisions(queries: &[(&str, u32, u64, f64)]) -> Vec<Value> {
    let mut wire = serde_json::to_value(fixture(0.0, false)).unwrap();
    let template = wire["query_workload"]["repeating_queries"][0].clone();
    wire["query_workload"]["repeating_queries"] = queries
        .iter()
        .map(|(query, interval, phase, _)| {
            let mut entry = template.clone();
            entry["query"] = (*query).into();
            entry["demand"]["fixed_interval_at"]["interval"] = (*interval).into();
            entry["demand"]["fixed_interval_at"]["evaluation_phase"] = (*phase).into();
            entry["requirements"]["accuracy"] = serde_json::json!({"explicit": "Exact"});
            entry
        })
        .collect();
    let plan = selected_plan_with(serde_json::from_value(wire).unwrap(), |request| {
        for (query, (_, _, _, read)) in request.queries.iter_mut().zip(queries) {
            query.summary_lifecycle_inputs.costs.read = *read;
        }
    });
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

fn decision(plan: &CompiledPhysicalPlan) -> Value {
    let [decision] = plan
        .planner_selection_trace
        .iter()
        .filter(|entry| entry["stage"] == "deployment.lifecycle_placement")
        .cloned()
        .collect::<Vec<_>>()
        .try_into()
        .unwrap();
    decision
}

fn cost(decision: &Value, field: &str) -> f64 {
    decision[field].as_f64().unwrap()
}

// Retention is priced for the panes the installed window layout retains,
// not for window / evaluation interval.
#[test]
fn retention_prices_the_installed_window_panes() {
    let plan = selected_plan(fixture(1e-12, false));
    let decision = decision(&plan);
    assert_eq!(decision["selected"], "continuously_maintained");
    let [installed] = plan.precompute_plan.materializations.as_slice() else {
        panic!("one installed state");
    };
    assert_eq!(
        decision["retained_states"].as_u64(),
        installed.num_aggregates_to_retain
    );
}

// Rebuilding runs the exact query-time program: every evaluation builds and
// retires a transient accumulator, folds each raw sample its Scan covers once,
// and reads the result once.
#[test]
fn ephemeral_cost_is_the_raw_program_over_its_scanned_samples() {
    let input = fixture(1.0, false);
    let costs = input.physical_inputs.lifecycle_costs.clone();
    let horizon = input.physical_inputs.horizon_seconds;
    let rate = input.data_workload.ingestion_rate.value.unwrap().0;
    let plan = selected_plan(input);
    let [QueryTimeOperator::Scan {
        range_ms: Some(range_ms),
        ..
    }] = raw_scans(&plan).as_slice()
    else {
        panic!("one raw range selector");
    };
    let evaluations = horizon / 10.0;
    let raw_samples = rate * *range_ms as f64 / 1_000.0;
    let expected = evaluations
        * (costs.build
            + raw_samples * costs.maintenance_per_update
            + costs.read
            + costs.retirement);
    let decision = decision(&plan);
    assert_eq!(decision["selected"], "ephemeral");
    assert!((cost(&decision, "ephemeral_cost") - expected).abs() < 1e-9);
}

// The store price at which the fixture's quantile sketch moves to query time:
// retention grows linearly with the price and flips just past the point where
// it overtakes the raw program's cost.
#[test]
fn store_price_flips_placement_at_the_break_even_price() {
    let free = decision(&selected_plan(fixture(0.0, false)));
    let priced = decision(&selected_plan(fixture(1.0, false)));
    let slope =
        cost(&priced, "continuously_maintained_cost") - cost(&free, "continuously_maintained_cost");
    let threshold =
        (cost(&free, "ephemeral_cost") - cost(&free, "continuously_maintained_cost")) / slope;
    // Over the 300 s horizon at one read per 10 s: the raw program costs
    // 30 * (10 + 100 samples/s * 60 s * 0.001 + 0.1 + 1) = 513; free retention costs
    // 10 + 300 * 100 * 0.001 + 30 * 0.1 + 300 * 0.001 + 1 = 44.3; each unit of
    // price adds 300 s * 7 retained panes * 65536 estimated sketch bytes.
    let expected = (513.0 - 44.3) / (300.0 * 7.0 * 65_536.0);
    assert!(
        (threshold - expected).abs() < expected * 1e-9,
        "{threshold}"
    );
    for (scale, selected) in [(0.99, "continuously_maintained"), (1.01, "ephemeral")] {
        let decision = decision(&selected_plan(fixture(threshold * scale, false)));
        assert_eq!(decision["selected"], selected);
    }
}

// Consumers whose evaluation cadences install separate windows are priced as
// the states they install, each consumer's reads at its own read unit cost.
#[test]
fn shared_state_combines_every_consumers_demand_and_costs() {
    let first = ("sum_over_time(m[1m])", 10_000, 0.1);
    let second = ("sort(sum_over_time(m[1m]))", 20_000, 0.7);
    let [alone_first] = priced_decisions(&[first]).try_into().unwrap();
    let [alone_second] = priced_decisions(&[second]).try_into().unwrap();
    let [shared] = priced_decisions(&[first, second]).try_into().unwrap();
    assert_eq!(shared["query_ids"].as_array().unwrap().len(), 2);
    for field in ["continuously_maintained_cost", "ephemeral_cost"] {
        let expected = cost(&alone_first, field) + cost(&alone_second, field);
        assert!((cost(&shared, field) - expected).abs() < 1e-9, "{field}");
    }
    assert_eq!(
        shared["retained_states"].as_u64().unwrap(),
        alone_first["retained_states"].as_u64().unwrap()
            + alone_second["retained_states"].as_u64().unwrap()
    );
}

// Consumers on one cadence but out of phase read different panes, so each
// installs and is priced as its own state.
#[test]
fn out_of_phase_consumers_are_priced_as_separate_installs() {
    let first = ("sum_over_time(m[1m])", 10_000, 0, 0.1);
    let second = ("sort(sum_over_time(m[1m]))", 10_000, 5_000, 0.1);
    let [alone_first] = phased_decisions(&[first]).try_into().unwrap();
    let [alone_second] = phased_decisions(&[second]).try_into().unwrap();
    let [shared] = phased_decisions(&[first, second]).try_into().unwrap();
    let expected = cost(&alone_first, "continuously_maintained_cost")
        + cost(&alone_second, "continuously_maintained_cost");
    assert!((cost(&shared, "continuously_maintained_cost") - expected).abs() < 1e-9);
}

/// Two per-series Rate states under ungrouped Sums; the query lookback is
/// the 10m window.
const MIXED_QUERY: &str = "sum(rate(a[1m])) + sum(rate(b[10m]))";

/// The fixture evaluating `query` under `store` per retained byte-second.
fn two_state_plan(query: &str, store: f64) -> CompiledPhysicalPlan {
    let mut wire = serde_json::to_value(fixture(store, false)).unwrap();
    wire["query_workload"]["repeating_queries"][0]["query"] = query.into();
    selected_plan(serde_json::from_value(wire).unwrap())
}

fn mixed_event(plan: &CompiledPhysicalPlan) -> Value {
    let [event] = plan
        .planner_selection_trace
        .iter()
        .filter(|entry| entry["stage"] == "deployment.mixed_placement")
        .cloned()
        .collect::<Vec<_>>()
        .try_into()
        .unwrap();
    event
}

// Where rebuilding the 1m Rate beats retaining it but the 10m branch is cheap
// as one complete window, the query rebuilds `a` from raw series beside the
// stored native batch of `sum(rate(b[10m]))`, the cheapest admissible choice.
#[test]
fn cheapest_assignment_retains_one_state_beside_a_rebuilt_one() {
    let plan = two_state_plan(MIXED_QUERY, 1e-3);
    assert_eq!(placements(&plan), ["ephemeral", "continuously_maintained"]);
    let event = mixed_event(&plan);
    assert_eq!(event["selected"], "mixed");
    assert!(cost(&event, "mixed_cost") < cost(&event, "group_cost"));
    let installed = &plan.precompute_plan.materializations;
    assert_eq!(installed.len(), 2, "b's Rate and its Sum batch");
    for state in installed {
        assert_eq!(state.metric, "b");
        assert_eq!(
            (
                state.window_size,
                state.slide_interval,
                &state.window_layout
            ),
            (
                600,
                10,
                &asap_types::WindowMaterializationLayout::FullWindow
            ),
            "a complete window sliding at the evaluation interval"
        );
    }
    assert!(installed.iter().any(|state| state.derived_input.is_some()));
    assert_eq!(
        raw_scans(&plan),
        [&QueryTimeOperator::Scan {
            metric: Some("a".into()),
            matchers: vec![],
            range_ms: Some(60_000),
            offset_ms: 0,
        }]
    );
    let entry = plan.query_plan.entries.values().next().unwrap();
    entry.recover_vector_physical_dag().unwrap();
    assert!(entry.mixes_raw_and_stored_inputs());
}

// Without a store price, retaining both states stays cheapest; the plan is
// the group decision and has no raw input.
#[test]
fn free_store_keeps_the_group_decision_for_two_states() {
    let plan = two_state_plan(MIXED_QUERY, 0.0);
    assert_eq!(
        placements(&plan),
        ["continuously_maintained", "continuously_maintained"]
    );
    assert_eq!(mixed_event(&plan)["selected"], "group");
    assert!(raw_scans(&plan).is_empty());
}

// Leaf Sum states are stored per population and a grouped Sum is no single
// batch, so neither query has an admissible mix: its states move together.
#[test]
fn inadmissible_mix_keeps_the_group_decision() {
    for query in [
        "sum(sum_over_time(a[10m])) + sum(sum_over_time(b[1m]))",
        "sum by (job) (rate(a[1m])) + sum by (job) (rate(b[10m]))",
    ] {
        let plan = two_state_plan(query, 1e-3);
        let placed = placements(&plan);
        assert!(placed.len() >= 2, "{query}: {placed:?}");
        assert!(
            placed.iter().all(|selected| selected == &placed[0]),
            "{query}: {placed:?}"
        );
        assert!(plan
            .planner_selection_trace
            .iter()
            .filter(|entry| entry["stage"] == "deployment.mixed_placement")
            .all(|event| event["selected"] == "group"));
        for entry in plan.query_plan.entries.values() {
            assert!(!entry.mixes_raw_and_stored_inputs(), "{query}");
        }
    }
}

// Explicit per-metric sample-rate evidence prices each selector's raw fold;
// absent evidence retains the conservative workload-wide rate.
#[test]
fn raw_selector_folds_use_their_own_source_rates() {
    let mut wire = serde_json::to_value(fixture(1.0, false)).unwrap();
    wire["query_workload"]["repeating_queries"][0]["query"] = MIXED_QUERY.into();
    let baseline = selected_plan(serde_json::from_value(wire.clone()).unwrap());
    let retained_cost = |plan: &CompiledPhysicalPlan| {
        plan.planner_selection_trace
            .iter()
            .filter(|event| event["stage"] == "deployment.lifecycle_placement")
            .map(|event| cost(event, "continuously_maintained_cost"))
            .sum::<f64>()
    };
    let mut a = wire["data_workload"]["ingestion_rate"].clone();
    a["value"] = 1.0.into();
    let mut b = a.clone();
    b["value"] = 2.0.into();
    wire["implementation"]["source_ingestion_rates"] = serde_json::json!({"a": a, "b": b});
    let input: BackendLocalPlanningInput = serde_json::from_value(wire).unwrap();
    let costs = input.physical_inputs.lifecycle_costs.clone();
    let evaluations = input.physical_inputs.horizon_seconds / 10.0;
    let plan = selected_plan(input);
    let priced: f64 = plan
        .planner_selection_trace
        .iter()
        .filter(|event| event["stage"] == "deployment.lifecycle_placement")
        .map(|event| cost(event, "ephemeral_cost"))
        .sum();
    let expected = evaluations
        * (2.0 * (costs.build + costs.read + costs.retirement)
            + (60.0 + 2.0 * 600.0) * costs.maintenance_per_update);
    assert!((priced - expected).abs() < 1e-9, "{priced} != {expected}");
    let expected_saving = evaluations * 10.0 * (200.0 - 3.0) * costs.maintenance_per_update;
    assert!((retained_cost(&baseline) - retained_cost(&plan) - expected_saving).abs() < 1e-9);
}

// Native lifecycle candidates price the complete windows their retained
// realization installs, including the configured retention margin.
#[test]
fn native_lifecycle_prices_complete_window_retention() {
    for (margin, expected) in [(0, 1), (25_000, 4)] {
        let mut wire = serde_json::to_value(fixture(1e-12, false)).unwrap();
        wire["query_workload"]["repeating_queries"][0]["query"] = "sum(rate(m[1m]))".into();
        wire["implementation"]["query_staleness_margin_ms"] = margin.into();
        let (request, _) = serde_json::from_value::<BackendLocalPlanningInput>(wire)
            .unwrap()
            .into_physical_compilation_request()
            .unwrap();
        let native: Vec<_> = request
            .planner_selection_trace
            .iter()
            .filter(|event| event["stage"] == "deployment.native_candidate_placement")
            .collect();
        assert!(!native.is_empty());
        for event in native {
            assert_eq!(event["retained_states"].as_u64(), Some(expected), "{event}");
        }
    }
}

// Stale per-metric evidence uses the workload rate, and invalid observations
// are rejected instead of allowing a negative raw execution cost.
#[test]
fn source_rate_evidence_requires_fresh_nonnegative_values() {
    let mut input = fixture(1.0, false);
    let baseline = decision(&selected_plan(input.clone()));
    let mut rate = input.data_workload.ingestion_rate.clone();
    rate.value = Some(planner_types::workload::Rate(1.0));
    rate.observed_at_ms = Some(input.environment.observed_at_unix_ms.saturating_sub(1));
    rate.valid_for_ms = Some(0);
    input
        .physical_inputs
        .source_ingestion_rates
        .insert("m".into(), rate.clone());
    let stale = decision(&selected_plan(input.clone()));
    assert_eq!(
        cost(&stale, "ephemeral_cost"),
        cost(&baseline, "ephemeral_cost")
    );
    rate.value = Some(planner_types::workload::Rate(-1.0));
    input
        .physical_inputs
        .source_ingestion_rates
        .insert("m".into(), rate);
    let error = input.into_physical_compilation_request().unwrap_err();
    assert!(error.to_string().contains("finite nonnegative rates"));
}

// Different logical lookbacks sharing one installed raw producer charge its
// maintenance and maximum retained pane population once, while keeping both reads.
#[test]
fn shared_physical_panes_are_priced_once_across_logical_lookbacks() {
    let plan = two_state_plan(
        "sum(sum_over_time(m[1m])) / sum(sum_over_time(m[10m]))",
        1e-12,
    );
    assert_eq!(plan.precompute_plan.materializations.len(), 1);
    let installed = plan.precompute_plan.materializations[0]
        .num_aggregates_to_retain
        .unwrap();
    assert_eq!(installed, 61);
    let priced: u64 = plan
        .planner_selection_trace
        .iter()
        .filter(|event| event["stage"] == "deployment.lifecycle_placement")
        .map(|event| event["retained_states"].as_u64().unwrap())
        .sum();
    assert_eq!(priced, installed);
    let retained_cost = |plan: &CompiledPhysicalPlan| {
        plan.planner_selection_trace
            .iter()
            .filter(|event| event["stage"] == "deployment.lifecycle_placement")
            .map(|event| cost(event, "continuously_maintained_cost"))
            .sum::<f64>()
    };
    let longest = two_state_plan("sum(sum_over_time(m[10m]))", 1e-12);
    // Same producer and 61 panes, plus the shorter logical read every 10s.
    assert!((retained_cost(&plan) - retained_cost(&longest) - 3.0).abs() < 1e-9);
}
