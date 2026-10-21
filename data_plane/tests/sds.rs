mod common;

use asap_executor::planner::ir::{NonASAPOp, Operator};
use asap_planner::{InstallablePlan, StoredOutputBuild};
use common::plan;
use data_plane::sds::{DefinitionId, Sds, SdsError, Selection, SummaryInstanceKey, SummaryStore};

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
            assert_eq!(id, &definition.id);
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
        .find(|n| n.id as u64 == definition.raw_input())
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

/// `plan` with every node id shifted by 100.
fn renumbered(plan: &InstallablePlan) -> InstallablePlan {
    let mut plan = plan.clone();
    let renumber = |id: &usize| id + 100;
    let dag = &mut plan.dag.dag;
    for node in &mut dag.nodes {
        node.id = renumber(&node.id);
        node.payload = node.payload.map_children(renumber);
    }
    for edge in &mut dag.edges {
        edge.producer = renumber(&edge.producer);
        edge.consumer = renumber(&edge.consumer);
    }
    dag.roots = dag.roots.iter().map(renumber).collect();
    for output in &mut plan.stored_outputs {
        output.node_id = renumber(&output.node_id);
        output.pane.time_range_node_id = renumber(&output.pane.time_range_node_id);
        output.pane.scan_node_id = renumber(&output.pane.scan_node_id);
    }
    plan
}

/// Identity depends on what is computed, not on how nodes are numbered.
#[test]
fn identity_ignores_node_numbering() {
    let plan = plan("example4b", false);
    let ids = |sds: Sds| sds.definitions.into_keys().collect::<Vec<_>>();
    assert_eq!(
        ids(Sds::from_plan(&renumbered(&plan)).unwrap()),
        ids(Sds::from_plan(&plan).unwrap())
    );
}

/// The same computation over another metric is another definition.
#[test]
fn identity_depends_on_the_source() {
    let plan = plan("example4b", false);
    let other: InstallablePlan = serde_json::from_str(
        &serde_json::to_string(&plan)
            .unwrap()
            .replace("latency_ms", "other"),
    )
    .unwrap();
    assert_ne!(
        Sds::from_plan(&other)
            .unwrap()
            .definitions
            .into_keys()
            .next(),
        Sds::from_plan(&plan)
            .unwrap()
            .definitions
            .into_keys()
            .next()
    );
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
    assert_eq!(
        sds.ingestion_build_order().unwrap(),
        vec![definition.id.clone()]
    );
}

/// Every stored output reads raw samples today, so no definition has stored
/// inputs; a definition that read itself would be a cycle.
#[test]
fn build_order_follows_stored_inputs() {
    let mut sds = Sds::from_plan(&plan("example4b", false)).unwrap();
    let definition = sds.definitions.values_mut().next().unwrap();
    assert!(definition.stored_inputs.is_empty());
    let id = definition.id.clone();
    definition.stored_inputs = vec![id.clone()];
    assert_eq!(sds.ingestion_build_order(), Err(SdsError::Cycle(id)));
}

/// Panes are found by definition and end, and evicted once no query needs
/// them.
#[test]
fn store_finds_and_evicts_panes() {
    let (a, b) = (DefinitionId("a".into()), DefinitionId("b".into()));
    let mut store = SummaryStore::new();
    for end in [60, 120, 180] {
        store.insert(
            SummaryInstanceKey::new(a.clone(), Selection::ending_at(end)),
            format!("a{end}"),
        );
    }
    store.insert(
        SummaryInstanceKey::new(b.clone(), Selection::ending_at(60)),
        "b60".to_string(),
    );
    assert_eq!(
        store.get(&a, Selection::ending_at(120)).map(String::as_str),
        Some("a120")
    );
    assert_eq!(store.get(&a, Selection::ending_at(90)), None);
    store.evict_before(&a, Selection::ending_at(180));
    assert_eq!(store.get(&a, Selection::ending_at(120)), None);
    assert_eq!(
        store.get(&a, Selection::ending_at(180)).map(String::as_str),
        Some("a180")
    );
    assert_eq!(
        store.get(&b, Selection::ending_at(60)).map(String::as_str),
        Some("b60")
    );
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
        .map(|input| DefinitionId(input.to_string()))
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
        sds.definitions.insert(definition.id.clone(), definition);
    }
    let order: Vec<String> = sds
        .ingestion_build_order()
        .unwrap()
        .into_iter()
        .map(|id| id.0)
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
        sds.definitions.insert(definition.id.clone(), definition);
    }
    assert_eq!(
        sds.ingestion_build_order(),
        Err(SdsError::IngestionReadsQueryRun {
            definition: DefinitionId("i".into()),
            input: DefinitionId("q".into()),
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
        .filter(|(id, _)| id == &definition.id)
        .map(|(_, binding)| binding.offset_ms)
        .collect();
    offsets.sort();
    assert_eq!(offsets, (0..5).map(|i| i * 60_000).collect::<Vec<_>>());
}
