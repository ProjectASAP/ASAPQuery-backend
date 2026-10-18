use std::path::{Path, PathBuf};

use control_plane::{
    plan_selected_at_query_time, plan_workload, DeploymentInputs, InstalledPlan, PlanError,
};
use planner_types::ir::{ASAPOp, Operator};
use planner_types::physical::Materialization;
use planner_types::workload::{PlanningWorkload, QueryLanguage, SqlDialect};

fn example1() -> PlanningWorkload {
    let path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../examples/planner-layering-example1.json");
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn temp_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("control-plane-{}-{name}", std::process::id()))
}

/// The Planner selects one physical DAG for #509 Example 1, with one root per
/// query, and the plan survives the file the data plane loads.
#[test]
fn plans_example1_and_round_trips_the_plan() {
    let workload = example1();
    let plan = plan_workload(&workload, &DeploymentInputs::default()).unwrap();
    assert_eq!(
        plan.queries,
        workload
            .query_workload
            .entries()
            .map(|entry| entry.query.0)
            .collect::<Vec<_>>()
    );
    assert_eq!(plan.dag.dag.roots.len(), 2);
    let path = temp_path("example1.json");
    plan.save(&path).unwrap();
    assert_eq!(InstalledPlan::load(&path).unwrap(), plan);
    std::fs::remove_file(path).unwrap();
}

/// Only PromQL workloads are planned; SQL support comes later.
#[test]
fn rejects_sql_workloads() {
    let mut workload = example1();
    workload.query_workload.language = QueryLanguage::SQL(SqlDialect::ClickhouseSQL);
    assert!(matches!(
        plan_workload(&workload, &DeploymentInputs::default()),
        Err(PlanError::UnsupportedLanguage(_))
    ));
}

/// A plan whose queries do not match the DAG's roots is not installed.
#[test]
fn load_rejects_a_plan_without_a_query_per_root() {
    let mut plan = plan_workload(&example1(), &DeploymentInputs::default()).unwrap();
    plan.queries.pop();
    let path = temp_path("mismatched.json");
    plan.save(&path).unwrap();
    assert!(matches!(
        InstalledPlan::load(&path),
        Err(PlanError::RootCount {
            roots: 2,
            queries: 1
        })
    ));
    std::fs::remove_file(path).unwrap();
}

fn example4b() -> PlanningWorkload {
    let path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../examples/planner-layering-example4b.json");
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

/// Without raw data the Planner builds Example 4b's panes at ingestion time,
/// installed as plain panes (no `PaneChain`); the reference plan of the same
/// selection materializes nothing.
#[test]
fn reference_plan_materializes_nothing() {
    let mut inputs = DeploymentInputs::default();
    inputs.deployment_capabilities.raw_data_retained = false;
    let installed = plan_workload(&example4b(), &inputs).unwrap();
    let materialized = |plan: &InstalledPlan| {
        plan.dag
            .dag
            .nodes
            .iter()
            .filter(|n| n.materialization != Materialization::NotMaterialized)
            .count()
    };
    assert_eq!(materialized(&installed), 6, "six ten-minute panes");
    assert!(installed
        .dag
        .dag
        .nodes
        .iter()
        .all(|n| !matches!(n.payload, Operator::ASAP(ASAPOp::PaneChain { .. }))));
    let reference = plan_selected_at_query_time(&example4b(), &inputs).unwrap();
    assert_eq!(materialized(&reference), 0);
    assert_eq!(reference.queries, installed.queries);
}
