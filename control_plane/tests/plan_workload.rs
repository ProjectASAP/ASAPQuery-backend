use std::path::{Path, PathBuf};

use asap_planner::{InstallablePlan, MaterializedViewRefresh};
use asap_planner::{InstallablePlanError, PlanningWorkload};
use control_plane::{load_plan, plan_workload, save_plan, DeploymentInputs, PlanError};

fn example(name: &str) -> PlanningWorkload {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(format!("../examples/planner-layering-{name}.json"));
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn plan(workload: &PlanningWorkload, inputs: &DeploymentInputs) -> InstallablePlan {
    plan_workload(workload, inputs).unwrap().0
}

fn temp_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("control-plane-{}-{name}", std::process::id()))
}

/// The Planner selects one physical DAG for #509 Example 1, with one root per
/// query, and the plan survives the file the data plane loads.
#[test]
fn plans_example1_and_round_trips_the_plan() {
    let workload = example("example1");
    let plan = plan(&workload, &DeploymentInputs::default());
    assert_eq!(
        plan.query_texts,
        workload
            .query_workload
            .entries()
            .map(|entry| entry.query.0)
            .collect::<Vec<_>>()
    );
    assert_eq!(plan.dag.roots.len(), 2);
    let path = temp_path("example1.json");
    save_plan(&plan, &path).unwrap();
    // The plan file carries what a runtime installs; in-memory provenance
    // (`selected_physical_candidate`) is not part of it.
    assert_eq!(
        serde_json::to_value(load_plan(&path).unwrap()).unwrap(),
        serde_json::to_value(&plan).unwrap()
    );
    std::fs::remove_file(path).unwrap();
}

/// Example 4b without raw data keeps its 60-minute range as a chain of
/// tumbling windows refreshed at ingestion, retained for the 60 minutes the
/// range reads back, and scans no raw data at query time.
#[test]
fn example4b_keeps_tumbling_windows_with_the_planners_retention() {
    let mut inputs = DeploymentInputs::default();
    inputs.deployment_capabilities.raw_data_retained = false;
    let plan = plan(&example("example4b"), &inputs);
    let views = plan.materialized_views().unwrap();
    assert!(!views.is_empty());
    assert!(views
        .iter()
        .all(|v| v.refresh() == MaterializedViewRefresh::AtIngestion));
    assert_eq!(
        views.iter().map(|v| v.retention_ms()).max(),
        Some(3_600_000)
    );
    assert!(plan.raw_data_retention().unwrap().is_empty());
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
    let mut plan = plan(&example("example1"), &DeploymentInputs::default());
    plan.query_texts.pop();
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

/// The data plane refreshes no view on first read, so the backend keeps
/// no query-time result across evaluations: #509 Example 4's B3 workload
/// gets no view refreshed on first read.
#[test]
fn plans_no_view_refreshed_on_first_read() {
    let inputs = DeploymentInputs::default();
    assert!(!inputs.deployment_capabilities.query_time_results_retained);
    let plan = plan(&example("example4-b3"), &inputs);
    assert!(plan
        .materialized_views()
        .unwrap()
        .iter()
        .all(|v| v.refresh() != MaterializedViewRefresh::OnFirstRead));
}

/// A workload is planned whole or refused: when one query has no plan the
/// data plane can execute (here a raw instant selector, without raw data at
/// query time and without compressed-sample windows), no plan is written,
/// so no query is left for Prometheus to compute.
#[test]
fn refuses_a_workload_with_a_query_it_cannot_plan() {
    let mut workload = example("example4b");
    let queries = workload.query_workload.repeating_queries.as_mut().unwrap();
    let mut unplannable = queries[0].clone();
    unplannable.query = serde_json::from_value(serde_json::json!("latency_ms")).unwrap();
    queries.push(unplannable);
    let inputs = without_raw_data_or_compressed_samples();
    let error = plan_workload(&workload, &inputs).unwrap_err();
    assert!(matches!(error, PlanError::Planning(_)), "{error}");
}

/// The MVP benchmark's p99 query over a gauge not declared non-negative:
/// without raw data and without compressed-sample windows, every candidate
/// fails a check (the exact ones scan raw samples at query time, the merged
/// DDSketch panes carry no bound through `avg`), so planning fails. With `ignore_accuracy_guarantees` the
/// cheapest sketch plan is selected: tumbling windows kept at ingestion.
#[test]
fn ignoring_accuracy_guarantees_plans_the_undeclared_p99_query() {
    let workload: PlanningWorkload = serde_json::from_str(
        r#"{"query_workload": {"language": "promql", "repeating_queries": [{
              "query": "avg by (region, service, job) (quantile_over_time(0.99, data[100s]))",
              "demand": {"fixed_interval_at": {"interval": 1000, "evaluation_phase": 0}},
              "requirements": {"accuracy": {"max_error": 0.05, "failure_probability": 0.3,
                  "accuracy_scope": "whole_result"}},
              "predictability": {"predictable": {"known_at": null}}}]},
            "data_workload": {"arrival": {"ingesting": {"rows_per_second": 10000.0}},
              "sources": {"data": {"series_count": 1000, "metric_type": "gauge",
                  "sample_interval": 100}}}}"#,
    )
    .unwrap();
    let mut inputs = without_raw_data_or_compressed_samples();
    let error = plan_workload(&workload, &inputs).unwrap_err();
    assert!(matches!(error, PlanError::Planning(_)), "{error}");

    inputs.ignore_accuracy_guarantees = true;
    let plan = plan(&workload, &inputs);
    assert!(!plan.materialized_views().unwrap().is_empty());
}

/// A backend that keeps neither raw samples nor compressed-sample windows:
/// an exact range read then has no producer.
fn without_raw_data_or_compressed_samples() -> DeploymentInputs {
    let mut inputs = DeploymentInputs::default();
    inputs.deployment_capabilities.raw_data_retained = false;
    inputs.deployment_capabilities.excluded_candidates =
        serde_json::from_value(serde_json::json!([{"summary_family": "compressed_samples"}]))
            .unwrap();
    inputs
}
