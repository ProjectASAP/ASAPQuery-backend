//! The collector view rule (docs/design/collector-sketch-ingest.md, §1):
//! which nodes of an installed plan the collector runs, from its declared
//! capabilities, and the collector-views file written from them.

use std::path::Path;

use asap_planner::{InstallablePlan, PhysicalASAPNodeId};
use control_plane::collector_views::{
    collector_sub_dag, CollectorCapabilities, CommitGrace, NotOnCollector,
};
use control_plane::{plan_workload, DeploymentInputs};
use serde_json::{json, Value};

/// Today's collector's capabilities, as the design document prints them.
fn capabilities() -> Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/collector-capabilities.json");
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn parse(capabilities: &Value) -> CollectorCapabilities {
    CollectorCapabilities::from_json(&capabilities.to_string()).unwrap()
}

/// A deployment without multi-level window chains, unless `levels`.
fn inputs(levels: bool) -> DeploymentInputs {
    let mut inputs = DeploymentInputs::default();
    inputs.deployment_capabilities.raw_data_retained = false;
    if !levels {
        inputs.deployment_capabilities.excluded_candidates = serde_json::from_value(json!(
            [{"operator": "SummaryMergeWindows"}, {"operator": "TimeRangeCover"}]
        ))
        .unwrap();
    }
    inputs
}

/// The plan of `queries`, evaluated every 10 s over the gauge `data`, each
/// quantile with a 5 % error target (a DDSketch chain).
fn plan(queries: &[&str], levels: bool) -> InstallablePlan {
    let entries: Vec<Value> = queries
        .iter()
        .map(|query| {
            let requirements = if query.starts_with("quantile") {
                json!({"accuracy": {"max_error": 0.05, "failure_probability": 0.3,
                    "accuracy_scope": "whole_result"}})
            } else {
                json!({})
            };
            json!({
                "query": query,
                "demand": {"fixed_interval_at": {"interval": 10000, "evaluation_phase": 0}},
                "requirements": requirements,
                "predictability": {"predictable": {"known_at": null}},
            })
        })
        .collect();
    let workload = serde_json::from_value(json!({
        "query_workload": {"language": "promql", "repeating_queries": entries},
        "data_workload": {"arrival": {"ingesting": {"rows_per_second": 100000.0}},
          "sources": {"data": {"series_count": 10000, "sample_interval": 100,
            "metric_type": "gauge", "column_signs": {"value": "non_negative"}}}},
    }))
    .unwrap();
    plan_workload(&workload, &inputs(levels)).unwrap().0
}

/// The plan's nodes of operator `operator` (`ASAP` or `NonASAP` payload
/// key), by id.
fn nodes_of(plan: &InstallablePlan, operator: &str) -> Vec<PhysicalASAPNodeId> {
    plan.dag
        .nodes
        .iter()
        .filter(|node| {
            let payload = serde_json::to_value(&node.payload).unwrap();
            ["ASAP", "NonASAP"]
                .iter()
                .any(|side| !payload[side][operator].is_null())
        })
        .map(|node| node.id)
        .collect()
}

/// The plan's DDSketch chain.
fn chain(plan: &InstallablePlan) -> PhysicalASAPNodeId {
    let chains = nodes_of(plan, "SummaryTumblingWindowChain");
    let [chain] = chains.as_slice() else {
        panic!("chains {chains:?}")
    };
    *chain
}

fn reason(
    reasons: &[(PhysicalASAPNodeId, NotOnCollector)],
    node: PhysicalASAPNodeId,
) -> Option<&NotOnCollector> {
    reasons.iter().find(|(id, _)| *id == node).map(|(_, r)| r)
}

/// A quantile's leaf DDSketch chain is the one collector view; its `Scan`
/// is on the collector and every query-time node is not. With only that
/// view reading `data`, the backend no longer reads `data` raw. The grace
/// is the collector's close delay, the export interval and the skew
/// allowance.
#[test]
fn a_leaf_sketch_chain_is_the_collector_view() {
    let plan = plan(&["quantile_over_time(0.99, data[5m])"], false);
    let grace = CommitGrace {
        export_interval_ms: 500,
        clock_skew_allowance_ms: 2_000,
    };
    let (file, reasons) = collector_sub_dag(&plan, &parse(&capabilities()), grace).unwrap();
    let chain = chain(&plan);
    assert_eq!(file.outputs, [chain]);
    assert_eq!(file.dag.roots, [chain]);
    assert_eq!(file.plan_id, plan.dag.plan_id());
    assert_eq!(file.commit_grace_ms, 6_000 + 500 + 2_000);
    assert_eq!(file.backend_reads_raw, [("data".to_string(), false)].into());
    let scans = nodes_of(&plan, "Scan");
    let mut on_collector: Vec<_> = file.dag.nodes.iter().map(|node| node.id).collect();
    on_collector.sort();
    let mut expected = vec![chain];
    expected.extend(&scans);
    expected.sort();
    assert_eq!(on_collector, expected);
    for node in &plan.dag.nodes {
        if !expected.contains(&node.id) {
            assert!(
                reason(&reasons, node.id).is_some(),
                "node {} has no reason",
                node.id
            );
        }
    }
}

/// With an exact view over the same metric (not in the capabilities), the
/// quantile's chain is still the one collector view, and `data` is still
/// read raw by the backend.
#[test]
fn a_metric_another_view_reads_stays_raw() {
    let plan = plan(
        &[
            "quantile_over_time(0.99, data[5m])",
            "sum_over_time(data[5m])",
        ],
        false,
    );
    let (file, reasons) =
        collector_sub_dag(&plan, &parse(&capabilities()), CommitGrace::default()).unwrap();
    let chains = nodes_of(&plan, "SummaryTumblingWindowChain");
    assert_eq!(chains.len(), 2);
    assert_eq!(file.outputs.len(), 1);
    let exact = chains.iter().find(|c| !file.outputs.contains(c)).unwrap();
    assert!(matches!(
        reason(&reasons, *exact),
        Some(NotOnCollector::NotInCapabilities(what)) if what.contains("ExactAggregate")
    ));
    assert_eq!(file.backend_reads_raw, [("data".to_string(), true)].into());
}

/// A sketch, a parameter range or a predicate op the collector lacks
/// leaves the chain in the backend, with the reason.
#[test]
fn what_the_capabilities_lack_stays_in_the_backend() {
    let quantile = plan(&["quantile_over_time(0.99, data[5m])"], false);
    let not_in = |capabilities: &Value, plan: &InstallablePlan, node| {
        let (file, reasons) =
            collector_sub_dag(plan, &parse(capabilities), CommitGrace::default()).unwrap();
        assert!(file.outputs.is_empty());
        match reason(&reasons, node) {
            Some(NotOnCollector::NotInCapabilities(what)) => what.clone(),
            other => panic!("{other:?}"),
        }
    };
    let mut no_ddsketch = capabilities();
    no_ddsketch["executor"]["supported_summaries"]
        .as_array_mut()
        .unwrap()
        .remove(0);
    assert!(not_in(&no_ddsketch, &quantile, chain(&quantile)).contains("DDSketch"));

    let mut finer_alpha_only = capabilities();
    finer_alpha_only["executor"]["supported_summaries"][0]["parameters"]["alpha"] =
        json!({"gt": 0, "lt": 0.01});
    assert!(not_in(&finer_alpha_only, &quantile, chain(&quantile)).contains("alpha"));

    let regex = plan(
        &[r#"quantile_over_time(0.99, data{job=~"a.*"}[5m])"#],
        false,
    );
    let selected = plan(&[r#"quantile_over_time(0.99, data{job="a"}[5m])"#], false);
    let (file, _) =
        collector_sub_dag(&selected, &parse(&capabilities()), CommitGrace::default()).unwrap();
    assert_eq!(
        file.outputs,
        [chain(&selected)],
        "an equality matcher is in the capabilities"
    );
    let (file, reasons) =
        collector_sub_dag(&regex, &parse(&capabilities()), CommitGrace::default()).unwrap();
    assert!(file.outputs.is_empty());
    assert!(
        reasons.iter().any(
            |(_, r)| matches!(r, NotOnCollector::NotInCapabilities(what) if what.contains("Regex"))
        ),
        "{reasons:?}"
    );
}

/// A chain reducing series to groups is local to a partition only when
/// each group is on one instance: never with series placement, and with
/// `{"labels": L}` when `L` is among its group keys. The plan's chain is
/// made a `Reduce` by its `job` label for the rule alone.
#[test]
fn a_reducing_chain_is_local_only_when_its_groups_are() {
    let mut plan = plan(&["quantile_over_time(0.99, data[5m])"], false);
    let chain = chain(&plan);
    let scan = nodes_of(&plan, "Scan")[0];
    let scan_columns: Vec<String> = plan
        .dag
        .nodes
        .iter()
        .find(|node| node.id == scan)
        .unwrap()
        .output_schema
        .fields
        .iter()
        .map(|field| field.name.clone())
        .collect();
    let node = plan
        .dag
        .nodes
        .iter_mut()
        .find(|node| node.id == chain)
        .unwrap();
    let mut payload = serde_json::to_value(&node.payload).unwrap();
    // Group by the scan's first column, named by the placement below.
    payload["ASAP"]["SummaryTumblingWindowChain"]["reduction"] = json!({"Reduce": [0]});
    node.payload = serde_json::from_value(payload).unwrap();
    let mut reducing = capabilities();
    reducing["operators"][2]["reductions"] = json!(["PerEntity", "Reduce"]);

    let (_, reasons) = collector_sub_dag(&plan, &parse(&reducing), CommitGrace::default()).unwrap();
    assert_eq!(
        reason(&reasons, chain),
        Some(&NotOnCollector::NotLocalToPartition)
    );
    reducing["placement"] = json!({"partitioned_by": {"labels": [scan_columns[0]]}});
    let (_, reasons) = collector_sub_dag(&plan, &parse(&reducing), CommitGrace::default()).unwrap();
    assert!(
        !matches!(
            reason(&reasons, chain),
            Some(NotOnCollector::NotLocalToPartition | NotOnCollector::NotInCapabilities(_))
        ),
        "{:?}",
        reason(&reasons, chain)
    );
    reducing["placement"] = json!({"partitioned_by": {"labels": ["no_such_label"]}});
    let (_, reasons) = collector_sub_dag(&plan, &parse(&reducing), CommitGrace::default()).unwrap();
    assert_eq!(
        reason(&reasons, chain),
        Some(&NotOnCollector::NotLocalToPartition)
    );
}

/// One more operator line moves the cut up: with `SummaryMergeWindows` in
/// the capabilities, a wider level merged from the chain at ingestion is a
/// collector view too.
#[test]
fn an_operator_line_moves_the_cut_up() {
    let plan = plan(&["quantile_over_time(0.99, data[5m])"], true);
    let levels = nodes_of(&plan, "SummaryMergeWindows");
    assert!(!levels.is_empty(), "the plan has no wider level");
    let (file, reasons) =
        collector_sub_dag(&plan, &parse(&capabilities()), CommitGrace::default()).unwrap();
    assert_eq!(file.outputs, [chain(&plan)]);
    assert!(matches!(
        reason(&reasons, levels[0]),
        Some(NotOnCollector::NotInCapabilities(what)) if what.contains("SummaryMergeWindows")
    ));
    let mut merging = capabilities();
    merging["operators"]
        .as_array_mut()
        .unwrap()
        .push(json!({"kind": "SummaryMergeWindows"}));
    let (file, _) = collector_sub_dag(&plan, &parse(&merging), CommitGrace::default()).unwrap();
    for level in &levels {
        assert!(file.outputs.contains(level), "{:?}", file.outputs);
    }
}

/// The written file reads back equal, and its sub-DAG is valid on its own:
/// its materialized views have the installed plan's view state keys.
#[test]
fn the_file_reads_back_and_its_views_keep_their_keys() {
    let plan = plan(&["quantile_over_time(0.99, data[5m])"], false);
    let (file, _) =
        collector_sub_dag(&plan, &parse(&capabilities()), CommitGrace::default()).unwrap();
    let text = serde_json::to_string(&file).unwrap();
    let read: common::CollectorViewsFile = serde_json::from_str(&text).unwrap();
    // Equal as written (the nodes' in-memory provenance is not in the file).
    assert_eq!(serde_json::to_string(&read).unwrap(), text);
    let installed = plan.materialized_views().unwrap();
    for view in read.dag.materialized_views().unwrap() {
        let same = installed
            .iter()
            .find(|v| v.node_id == view.node_id)
            .unwrap();
        assert_eq!(view.view_state_key(), same.view_state_key());
    }
}

/// Capabilities of another version, with an unknown field, or with an
/// operator limit that does not apply to its kind or names no variant, are
/// refused.
#[test]
fn unknown_capabilities_are_refused() {
    let mut newer = capabilities();
    newer["capabilities_version"] = json!(2);
    assert!(CollectorCapabilities::from_json(&newer.to_string()).is_err());
    let mut unknown = capabilities();
    unknown["operators"][0]["reductions_typo"] = json!([]);
    assert!(CollectorCapabilities::from_json(&unknown.to_string()).is_err());
    for (operator, limits) in [
        (2, json!({"reductions": ["PerEnity"]})),
        (0, json!({"reductions": ["PerEntity"]})),
        (1, json!({"sources": ["TimeSeries"]})),
    ] {
        let mut wrong = capabilities();
        for (limit, value) in limits.as_object().unwrap() {
            wrong["operators"][operator][limit] = value.clone();
        }
        assert!(
            CollectorCapabilities::from_json(&wrong.to_string()).is_err(),
            "{limits}"
        );
    }
    let mut twice = capabilities();
    twice["operators"]
        .as_array_mut()
        .unwrap()
        .push(json!({"kind": "Filter"}));
    assert!(CollectorCapabilities::from_json(&twice.to_string()).is_err());
}
