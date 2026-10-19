mod common;

use asap_executor::planner::ir::{NonASAPOp, Operator};
use asap_planner::{InstallablePlan, StoredOutputBuild};
use common::plan;
use data_plane::sds::{DefinitionId, Sds, SummaryStore};

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

/// B3's five query-time KLL panes kept across evaluations are one kept
/// definition, keyed like ingestion-time panes: a 1-minute pane, each output
/// reading the pane `i * 1min` before evaluation. Nothing is built at
/// ingestion time.
#[test]
fn kept_panes_share_one_definition() {
    let plan = plan("example4-b3", true);
    assert!(plan
        .stored_outputs
        .iter()
        .any(|o| o.built_at == StoredOutputBuild::QueryRun));
    let sds = Sds::from_plan(&plan).unwrap();
    assert!(sds.definitions.is_empty() && sds.outputs.is_empty());
    assert_eq!(sds.kept_definitions.len(), 1);
    let definition = sds.kept_definitions.values().next().unwrap();
    assert_eq!(definition.pane_ms, 60_000);
    let mut offsets: Vec<i64> = sds
        .kept_outputs
        .values()
        .map(|(id, binding)| {
            assert_eq!(id, &definition.id);
            binding.offset_ms
        })
        .collect();
    offsets.sort();
    assert_eq!(offsets, (0..5).map(|i| i * 60_000).collect::<Vec<_>>());
}

/// Panes are found by definition and end, and evicted once no query needs
/// them.
#[test]
fn store_finds_and_evicts_panes() {
    let (a, b) = (DefinitionId("a".into()), DefinitionId("b".into()));
    let store = SummaryStore::new();
    for end in [60, 120, 180] {
        store.insert(&a, end, format!("a{end}"));
    }
    store.insert(&b, 60, "b60".to_string());
    assert_eq!(
        store.get(&a, 120).as_deref().map(String::as_str),
        Some("a120")
    );
    assert_eq!(store.get(&a, 90), None);
    store.evict_before(&a, 180);
    assert_eq!(store.get(&a, 120), None);
    assert_eq!(
        store.get(&a, 180).as_deref().map(String::as_str),
        Some("a180")
    );
    assert_eq!(
        store.get(&b, 60).as_deref().map(String::as_str),
        Some("b60")
    );
    assert_eq!(store.len(), 2);
}

/// A read shares the stored pane instead of copying it, and a pane never
/// changes once stored: a second insert for the same pane keeps the first.
#[test]
fn store_shares_panes_and_never_rewrites_them() {
    let a = DefinitionId("a".into());
    let store = SummaryStore::new();
    let stored = store.insert(&a, 60, vec![1.0, 2.0]);
    let first = store.get(&a, 60).unwrap();
    assert!(std::sync::Arc::ptr_eq(&stored, &first));
    let again = store.insert(&a, 60, vec![9.0]);
    assert!(
        std::sync::Arc::ptr_eq(&again, &first),
        "the first pane stays"
    );
    assert_eq!(*store.get(&a, 60).unwrap(), vec![1.0, 2.0]);
}

/// Readers on many threads read the store while a writer inserts and
/// evicts, each read seeing a whole pane.
#[test]
fn store_is_read_concurrently() {
    let a = DefinitionId("a".into());
    let store = std::sync::Arc::new(SummaryStore::new());
    let writer = {
        let (store, a) = (store.clone(), a.clone());
        std::thread::spawn(move || {
            for end in 0..1_000 {
                store.insert(&a, end, vec![end; 16]);
                store.evict_before(&a, end - 10);
            }
        })
    };
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let (store, a) = (store.clone(), a.clone());
            std::thread::spawn(move || {
                for end in 0..1_000 {
                    if let Some(pane) = store.get(&a, end) {
                        assert_eq!(*pane, vec![end; 16]);
                    }
                }
            })
        })
        .collect();
    writer.join().unwrap();
    for reader in readers {
        reader.join().unwrap();
    }
    assert_eq!(store.len(), 11);
}
