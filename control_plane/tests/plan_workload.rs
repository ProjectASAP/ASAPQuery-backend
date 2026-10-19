use std::path::{Path, PathBuf};

use asap_planner::{InstallablePlan, PlanningWorkload, StoredOutputBuild};
use control_plane::{plan_workload, save_plan, DeploymentInputs, PlanError};

fn example(name: &str) -> PlanningWorkload {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(format!("../examples/planner-layering-{name}.json"));
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn temp_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("control-plane-{}-{name}", std::process::id()))
}

/// The Planner selects one physical DAG for #509 Example 1, with one root per
/// query bound to the query's text, and the plan survives the file the data
/// plane loads.
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
    let loaded: InstallablePlan =
        serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    loaded.validate().unwrap();
    assert_eq!(loaded, plan);
    std::fs::remove_file(path).unwrap();
}

/// Only PromQL workloads are planned: the Planner refuses a SQL workload
/// for the PromQL frontend.
#[test]
fn rejects_sql_workloads() {
    let mut workload: serde_json::Value = serde_json::to_value(example("example1")).unwrap();
    workload["query_workload"]["language"] = serde_json::json!({"sql": "clickhouse_sql"});
    let workload: PlanningWorkload = serde_json::from_value(workload).unwrap();
    assert!(matches!(
        plan_workload(&workload, &DeploymentInputs::default()),
        Err(PlanError::Planning(_))
    ));
}

/// Without raw data the Planner builds Example 4b's six ten-minute panes at
/// ingestion time and lists them as stored outputs, each with its pane and
/// a retention at least as long as its pane reaches back; its query-time
/// reference stores nothing and answers the same queries.
#[test]
fn stores_example4b_panes_and_references_them_at_query_time() {
    let mut inputs = DeploymentInputs::default();
    inputs.deployment_capabilities.raw_data_retained = false;
    let plan = plan_workload(&example("example4b"), &inputs).unwrap();
    assert_eq!(plan.stored_outputs.len(), 6, "six ten-minute panes");
    let mut offsets: Vec<i64> = plan
        .stored_outputs
        .iter()
        .map(|output| {
            assert_eq!(output.built_at, StoredOutputBuild::IngestionRun);
            assert_eq!(output.pane.width_ms, 600_000);
            assert!(output.retention_ms >= output.pane.reach_ms());
            output.pane.end_offset_ms
        })
        .collect();
    offsets.sort();
    assert_eq!(offsets, (0..6).map(|i| i * 600_000).collect::<Vec<_>>());
    assert!(plan.query_time_raw_windows.is_empty());
    let reference = plan.query_time_reference().unwrap();
    assert!(reference.stored_outputs.is_empty());
    assert_eq!(reference.queries, plan.queries);
}

/// The backend keeps query-time results across evaluations, so for a few
/// series sampled every millisecond the Planner selects #509 Example 4's
/// B3: five 1-minute KLL panes built at query time and kept.
#[test]
fn selects_panes_kept_across_evaluations() {
    let inputs = DeploymentInputs::default();
    assert!(inputs.deployment_capabilities.query_time_results_retained);
    let plan = plan_workload(&example("example4-b3"), &inputs).unwrap();
    let kept: Vec<_> = plan
        .stored_outputs
        .iter()
        .filter(|output| output.built_at == StoredOutputBuild::QueryRun)
        .collect();
    assert_eq!(kept.len(), 5, "five one-minute panes");
    assert!(kept.iter().all(|output| output.pane.width_ms == 60_000));
}
