mod common;

use asap_executor::physical_planner::frontier_from_timing;
use common::plan;
use data_plane::sds::{DefinitionId, Sds, SdsError, SummaryStore};
use planner_types::ir::physical_export::PhysicalASAPDAG;
use planner_types::ir::{NonASAPOp, Operator};
use planner_types::physical::{Materialization, Medium};

fn sds_of(dag: &PhysicalASAPDAG) -> Result<Sds, SdsError> {
    Sds::from_dag(dag, &frontier_from_timing(dag).unwrap())
}

/// Example 4b's six ingestion-time KLL panes are one definition with a
/// 10-minute pane; each output reads the pane `i * 10min` before evaluation.
#[test]
fn example4b_panes_share_one_definition() {
    let plan = plan("example4b", false);
    let sds = sds_of(&plan.dag.dag).unwrap();
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
    let raw = plan
        .dag
        .dag
        .nodes
        .iter()
        .find(|n| n.id as u64 == definition.raw_input)
        .unwrap();
    assert!(matches!(
        raw.payload,
        Operator::NonASAP(NonASAPOp::TimeRange { .. })
    ));
}

/// A plan computed entirely at query time stores nothing.
#[test]
fn query_time_plans_have_no_definitions() {
    let plan = plan("example1", true);
    assert_eq!(sds_of(&plan.dag.dag).unwrap(), Sds::default());
}

/// Identity depends on what is computed, not on how nodes are numbered.
#[test]
fn identity_ignores_node_numbering() {
    let dag = plan("example4b", false).dag.dag;
    let mut renumbered = dag.clone();
    let renumber = |id: &usize| id + 100;
    for node in &mut renumbered.nodes {
        node.id = renumber(&node.id);
        node.payload = node.payload.map_children(renumber);
    }
    for edge in &mut renumbered.edges {
        edge.producer = renumber(&edge.producer);
        edge.consumer = renumber(&edge.consumer);
    }
    renumbered.roots = renumbered.roots.iter().map(renumber).collect();
    let ids = |sds: Sds| sds.definitions.into_keys().collect::<Vec<_>>();
    assert_eq!(
        ids(sds_of(&renumbered).unwrap()),
        ids(sds_of(&dag).unwrap())
    );
}

/// The same computation over another metric is another definition.
#[test]
fn identity_depends_on_the_source() {
    let dag = plan("example4b", false).dag.dag;
    let other: PhysicalASAPDAG = serde_json::from_str(
        &serde_json::to_string(&dag)
            .unwrap()
            .replace("latency_ms", "other"),
    )
    .unwrap();
    assert_ne!(
        sds_of(&other).unwrap().definitions.into_keys().next(),
        sds_of(&dag).unwrap().definitions.into_keys().next()
    );
}

/// An ingestion-time output must read a pane of raw samples.
#[test]
fn rejects_outputs_without_a_pane() {
    let dag = plan("example4b", false).dag.dag;
    let scan = dag
        .nodes
        .iter()
        .find(|n| matches!(n.payload, Operator::NonASAP(NonASAPOp::Scan { .. })))
        .unwrap();
    let scan = scan.id as u64;
    assert_eq!(
        Sds::from_dag(&dag, &[scan]),
        Err(SdsError::UnsupportedShape {
            node: scan,
            reason: "no TimeRange".into()
        })
    );
}

/// A plan that keeps a query-time result across evaluations is not
/// installed: the data plane declares it cannot keep one.
#[test]
fn rejects_results_kept_across_evaluations() {
    let mut dag = plan("example4b", true).dag.dag;
    let node = &mut dag.nodes[0];
    node.materialization = Materialization::MatAtQueryKeptAcrossEvaluations(Medium::Memory);
    let id = node.id as u64;
    assert_eq!(
        Sds::from_dag(&dag, &[]),
        Err(SdsError::KeptAcrossEvaluations(id))
    );
}

/// Panes are found by definition and end, and evicted once no query needs them.
#[test]
fn store_finds_and_evicts_panes() {
    let (a, b) = (DefinitionId("a".into()), DefinitionId("b".into()));
    let mut store = SummaryStore::new();
    for end in [60, 120, 180] {
        store.insert(&a, end, format!("a{end}"));
    }
    store.insert(&b, 60, "b60".to_string());
    assert_eq!(store.get(&a, 120).map(String::as_str), Some("a120"));
    assert_eq!(store.get(&a, 90), None);
    store.evict_through(&a, 120);
    assert_eq!(store.get(&a, 120), None);
    assert_eq!(store.get(&a, 180).map(String::as_str), Some("a180"));
    assert_eq!(store.get(&b, 60).map(String::as_str), Some("b60"));
    assert_eq!(store.len(), 2);
}
