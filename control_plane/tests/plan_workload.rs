use std::path::{Path, PathBuf};

use asap_planner::{InstallablePlanError, PlanningWorkload};
use control_plane::{load_plan, plan_workload, save_plan, DeploymentInputs, PlanError};

fn example(name: &str) -> PlanningWorkload {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(format!("../examples/planner-layering-{name}.json"));
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn temp_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("control-plane-{}-{name}", std::process::id()))
}

/// The Planner selects one physical DAG for #509 Example 1, with one root per
/// query, and the plan survives the file the data plane loads.
#[test]
fn plans_example1_and_round_trips_the_plan() {
    let workload = example("example1");
    let plan = plan_workload(&workload, &DeploymentInputs::default()).unwrap();
    assert_eq!(
        plan.queries
            .iter()
            .map(|q| q.query.clone())
            .collect::<Vec<_>>(),
        workload
            .query_workload
            .entries()
            .map(|entry| entry.query.0)
            .collect::<Vec<_>>()
    );
    assert_eq!(plan.dag.dag.roots.len(), 2);
    let path = temp_path("example1.json");
    save_plan(&plan, &path).unwrap();
    assert_eq!(load_plan(&path).unwrap(), plan);
    std::fs::remove_file(path).unwrap();
}

/// Example 4b without raw data stores its six 10-minute panes, built at
/// ingestion, each kept as long as the Planner states: 60 minutes back for
/// the oldest pane the 60-minute range reads.
#[test]
fn example4b_stores_panes_with_the_planners_retention() {
    let mut inputs = DeploymentInputs::default();
    inputs.deployment_capabilities.raw_data_retained = false;
    let plan = plan_workload(&example("example4b"), &inputs).unwrap();
    assert_eq!(plan.stored_outputs.len(), 6);
    let mut offsets: Vec<i64> = plan
        .stored_outputs
        .iter()
        .map(|output| {
            assert_eq!(output.pane.width_ms, 600_000);
            output.pane.end_offset_ms
        })
        .collect();
    offsets.sort();
    assert_eq!(offsets, (0..6).map(|i| i * 600_000).collect::<Vec<_>>());
    let retention = plan.stored_outputs.iter().map(|o| o.retention_ms).max();
    assert_eq!(retention, Some(3_600_000));
}

/// Only PromQL workloads are planned; the Planner's PromQL frontend refuses
/// SQL.
#[test]
fn rejects_sql_workloads() {
    let mut workload = example("example1");
    workload.query_workload.language =
        serde_json::from_str(r#"{"sql": "clickhouse_sql"}"#).unwrap();
    assert!(matches!(
        plan_workload(&workload, &DeploymentInputs::default()),
        Err(PlanError::Planning(_))
    ));
}

/// A plan whose queries do not match the DAG's roots is not installed.
#[test]
fn load_rejects_a_plan_without_a_query_per_root() {
    let mut plan = plan_workload(&example("example1"), &DeploymentInputs::default()).unwrap();
    plan.queries.pop();
    let path = temp_path("mismatched.json");
    save_plan(&plan, &path).unwrap();
    assert!(matches!(
        load_plan(&path),
        Err(PlanError::InvalidPlan(
            InstallablePlanError::QueriesDoNotMatchRoots { roots: 2 }
        ))
    ));
    std::fs::remove_file(path).unwrap();
}

/// The backend retains query-time results (`query_time_results_retained`),
/// so for a few series sampled every millisecond the Planner selects #509
/// Example 4's B3: five 1-minute KLL panes, stored outputs built in a
/// `QueryRun` (`MatAtQueryKeptAcrossEvaluations`).
#[test]
fn selects_stored_outputs_built_in_a_query_run() {
    let inputs = DeploymentInputs::default();
    assert!(inputs.deployment_capabilities.query_time_results_retained);
    let plan = plan_workload(&example("example4-b3"), &inputs).unwrap();
    let query_run: Vec<_> = plan
        .stored_outputs
        .iter()
        .filter(|o| o.built_at == asap_planner::StoredOutputBuild::QueryRun)
        .collect();
    assert_eq!(query_run.len(), 5, "five one-minute summaries");
    assert!(query_run.iter().all(|o| o.pane.width_ms == 60_000));
}
