mod common;

use asap_executor::planner::ir::{NonASAPOp, Operator};
use asap_planner::StoredOutputBuild;
use common::plan;
use data_plane::sds::{
    DefinitionId, DefinitionKey, Sds, SdsError, Selection, SummaryInstanceKey, SummaryStore,
};

/// Example 4b's six ingestion-time KLL panes are one definition with a
/// 10-minute pane; each output reads the pane `i * 10min` before evaluation,
/// and the definition is kept for the largest retention among them.
#[test]
fn example4b_panes_share_one_definition() {
    let plan = plan("example4b", false);
    let sds = Sds::from_plan(&plan).unwrap();
    assert_eq!(sds.definitions.len(), 1);
    let definition = sds.definitions.values().next().unwrap();
    assert_eq!(definition.pane_ms, 600_000);
    let mut offsets: Vec<i64> = sds
        .outputs
        .values()
        .map(|(id, binding)| {
            assert_eq!(id, &definition.key());
            binding.offset_ms
        })
        .collect();
    offsets.sort();
    assert_eq!(offsets, (0..6).map(|i| i * 600_000).collect::<Vec<_>>());
    let largest = plan.stored_outputs.iter().map(|o| o.retention_ms).max();
    assert_eq!(Some(definition.retention_ms as u64), largest);
    let raw = plan
        .dag
        .dag
        .nodes
        .iter()
        .find(|n| Some(n.id as u64) == definition.raw_input())
        .unwrap();
    assert!(matches!(
        raw.payload,
        Operator::NonASAP(NonASAPOp::TimeRange { .. })
    ));
}

/// A definition is kept for the largest retention among its outputs, not
/// its first output's.
#[test]
fn retention_is_the_largest_among_outputs() {
    let mut plan = plan("example4b", false);
    let last = plan.stored_outputs.len() - 1;
    plan.stored_outputs[last].retention_ms = 7 * 3_600_000;
    let sds = Sds::from_plan(&plan).unwrap();
    let definition = sds.definitions.values().next().unwrap();
    assert_eq!(definition.retention_ms, 7 * 3_600_000);
}

/// A plan computed entirely at query time stores nothing.
#[test]
fn query_time_plans_have_no_definitions() {
    let plan = plan("example1", true);
    assert_eq!(Sds::from_plan(&plan).unwrap(), Sds::default());
}

/// A definition's identity is the plan's definition id, the same for every
/// output whose selection differs only in time.
#[test]
fn identity_is_the_plans_definition_id() {
    let plan = plan("example4b", false);
    let sds = Sds::from_plan(&plan).unwrap();
    let definition = sds.definitions.values().next().unwrap();
    for output in &plan.stored_outputs {
        assert_eq!(definition.id.0, output.coverage.definition_id);
    }
    assert_eq!(&*definition.labels, "", "no label constraints");
}

/// Outputs with the same definition id but other label constraints in
/// their selection are other definitions, and their summary instances other
/// keys: the plan's definition id leaves the selection out.
#[test]
fn label_constraints_separate_definitions_of_one_id() {
    let mut json = serde_json::to_value(common::workload("example4b")).unwrap();
    let queries = json["query_workload"]["repeating_queries"]
        .as_array_mut()
        .unwrap();
    let mut other = queries[0].clone();
    queries[0]["query"] = "quantile_over_time(0.99, latency_ms{job=\"a\"}[60m])".into();
    other["query"] = "quantile_over_time(0.99, latency_ms{job=\"b\"}[60m])".into();
    queries.push(other);
    let workload = serde_json::from_value(json).unwrap();
    let plan = control_plane::plan_workload(&workload, &common::inputs(false)).unwrap();
    let sds = Sds::from_plan(&plan).unwrap();
    assert_eq!(sds.definitions.len(), 2, "{:?}", sds.definitions.keys());
    let keys: Vec<&DefinitionKey> = sds.definitions.keys().collect();
    assert_eq!(keys[0].id, keys[1].id, "one definition id");
    assert_ne!(keys[0].labels, keys[1].labels);
    let mut store = SummaryStore::new();
    for (i, key) in keys.iter().enumerate() {
        let selection = sds.definitions[*key].selection_ending_at(600_000);
        store.insert(SummaryInstanceKey::new(key.id.clone(), selection), i);
    }
    assert_eq!(store.get_pane(keys[0], 600_000), Some(&0));
    assert_eq!(store.get_pane(keys[1], 600_000), Some(&1));
}

/// Stored outputs built in a `QueryRun` are stored summaries too: their
/// definition says so, and no ingestion run builds them.
#[test]
fn query_run_outputs_have_query_run_definitions() {
    let mut plan = plan("example4b", false);
    for output in &mut plan.stored_outputs {
        output.built_at = StoredOutputBuild::QueryRun;
    }
    let sds = Sds::from_plan(&plan).unwrap();
    assert_eq!(sds.definitions.len(), 1);
    let definition = sds.definitions.values().next().unwrap();
    assert_eq!(definition.built_at, StoredOutputBuild::QueryRun);
    assert_eq!(sds.outputs.len(), 6);
    assert!(sds.ingestion_build_order().unwrap().is_empty());
}

/// A definition shared by an `IngestionRun` and a `QueryRun` output is built
/// at ingestion.
#[test]
fn ingestion_wins_when_outputs_share_a_definition() {
    let mut plan = plan("example4b", false);
    plan.stored_outputs[0].built_at = StoredOutputBuild::QueryRun;
    let sds = Sds::from_plan(&plan).unwrap();
    let definition = sds.definitions.values().next().unwrap();
    assert_eq!(definition.built_at, StoredOutputBuild::IngestionRun);
    assert_eq!(sds.ingestion_build_order().unwrap(), vec![definition.key()]);
}

/// Every stored output reads raw samples today, so no definition has stored
/// inputs; a definition that read itself would be a cycle.
#[test]
fn build_order_follows_stored_inputs() {
    let mut sds = Sds::from_plan(&plan("example4b", false)).unwrap();
    let definition = sds.definitions.values_mut().next().unwrap();
    assert!(definition.stored_inputs.is_empty());
    let id = definition.key();
    definition.stored_inputs = vec![id.clone()];
    assert_eq!(sds.ingestion_build_order(), Err(SdsError::Cycle(id)));
}

/// Panes are found by definition and end, and evicted once no query needs
/// them.
#[test]
fn store_finds_and_evicts_panes() {
    let (a, b) = (
        DefinitionKey::unconstrained(DefinitionId("a".into())),
        DefinitionKey::unconstrained(DefinitionId("b".into())),
    );
    let mut store = SummaryStore::new();
    for end in [60, 120, 180] {
        store.insert(
            SummaryInstanceKey::new(a.id.clone(), Selection::ending_at(end)),
            format!("a{end}"),
        );
    }
    store.insert(
        SummaryInstanceKey::new(b.id.clone(), Selection::ending_at(60)),
        "b60".to_string(),
    );
    assert_eq!(store.get_pane(&a, 120).map(String::as_str), Some("a120"));
    assert_eq!(store.get_pane(&a, 90), None);
    store.evict_before(&a.id, &Selection::ending_at(180));
    assert_eq!(store.get_pane(&a, 120), None);
    assert_eq!(store.get_pane(&a, 180).map(String::as_str), Some("a180"));
    assert_eq!(store.get_pane(&b, 60).map(String::as_str), Some("b60"));
    assert_eq!(store.len(), 2);
}

/// `definition` under another identity, reading `stored_inputs`.
fn renamed(
    definition: &data_plane::sds::SummaryDefinition,
    name: &str,
    stored_inputs: &[&str],
) -> data_plane::sds::SummaryDefinition {
    let mut definition = definition.clone();
    definition.id = DefinitionId(name.into());
    definition.stored_inputs = stored_inputs
        .iter()
        .map(|input| DefinitionKey::unconstrained(DefinitionId(input.to_string())))
        .collect();
    definition
}

/// Several definitions are ordered after the ones they read, whatever
/// their identities' order: a rollup `a` over `c` and `b`, `b` over `c`.
#[test]
fn build_order_puts_inputs_first() {
    let base = Sds::from_plan(&plan("example4b", false)).unwrap();
    let template = base.definitions.values().next().unwrap();
    let mut sds = Sds::default();
    for definition in [
        renamed(template, "a", &["c", "b"]),
        renamed(template, "b", &["c"]),
        renamed(template, "c", &[]),
    ] {
        sds.definitions.insert(definition.key(), definition);
    }
    let order: Vec<String> = sds
        .ingestion_build_order()
        .unwrap()
        .into_iter()
        .map(|key| key.id.0)
        .collect();
    assert_eq!(order, ["c", "b", "a"]);
}

/// An `IngestionRun` definition cannot read a `QueryRun` one: that summary
/// is only built when an evaluation needs it.
#[test]
fn build_order_refuses_ingestion_reading_query_run() {
    let base = Sds::from_plan(&plan("example4b", false)).unwrap();
    let template = base.definitions.values().next().unwrap();
    let mut query_run = renamed(template, "q", &[]);
    query_run.built_at = StoredOutputBuild::QueryRun;
    let mut sds = Sds::default();
    for definition in [renamed(template, "i", &["q"]), query_run] {
        sds.definitions.insert(definition.key(), definition);
    }
    assert_eq!(
        sds.ingestion_build_order(),
        Err(SdsError::IngestionReadsQueryRun {
            definition: DefinitionKey::unconstrained(DefinitionId("i".into())),
            input: DefinitionKey::unconstrained(DefinitionId("q".into())),
        })
    );
}

/// B3's five 1-minute KLL panes, stored outputs built in a `QueryRun`
/// (`MatAtQueryKeptAcrossEvaluations`), are one `QueryRun` definition, each output reading the
/// summary `i * 1min` before evaluation. Nothing is built at ingestion.
#[test]
fn query_run_outputs_share_one_definition() {
    let sds = Sds::from_plan(&plan("example4-b3", true)).unwrap();
    assert!(sds.ingestion_build_order().unwrap().is_empty());
    let mut query_run = sds
        .definitions
        .values()
        .filter(|d| d.built_at == StoredOutputBuild::QueryRun);
    let definition = query_run.next().unwrap();
    assert!(query_run.next().is_none());
    assert_eq!(definition.pane_ms, 60_000);
    let mut offsets: Vec<i64> = sds
        .outputs
        .values()
        .filter(|(id, _)| id == &definition.key())
        .map(|(_, binding)| binding.offset_ms)
        .collect();
    offsets.sort();
    assert_eq!(offsets, (0..5).map(|i| i * 60_000).collect::<Vec<_>>());
}
