use std::path::Path;

use asap_planner::{InstallablePlan, PlanningWorkload};
use control_plane::{plan_workload, DeploymentInputs};

/// The deployment inputs of a backend that keeps raw data or not.
pub fn inputs(raw_data_retained: bool) -> DeploymentInputs {
    let mut inputs = DeploymentInputs::default();
    inputs.deployment_capabilities.raw_data_retained = raw_data_retained;
    inputs
}

/// A #509 example workload in `examples/`.
pub fn workload(example: &str) -> PlanningWorkload {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(format!("../examples/planner-layering-{example}.json"));
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

/// `workload` with every repeating query answered as a range request of
/// steps `step_ms` apart over `span_ms`, so the Planner keeps its stored
/// outputs for the oldest step too.
#[allow(dead_code)]
pub fn with_range_evaluation(
    workload: &PlanningWorkload,
    step_ms: u64,
    span_ms: u64,
) -> PlanningWorkload {
    let mut json = serde_json::to_value(workload).unwrap();
    for query in json["query_workload"]["repeating_queries"]
        .as_array_mut()
        .unwrap()
    {
        query["range_evaluation"] = serde_json::json!({"step_ms": step_ms, "span_ms": span_ms});
    }
    serde_json::from_value(json).unwrap()
}

/// The selected plan of a #509 example workload, for a deployment that
/// keeps raw data or not.
#[allow(dead_code)]
pub fn plan(example: &str, raw_data_retained: bool) -> InstallablePlan {
    plan_workload(&workload(example), &inputs(raw_data_retained)).unwrap()
}

#[allow(dead_code)]
pub fn plan_json(example: &str, raw_data_retained: bool) -> String {
    serde_json::to_string(&plan(example, raw_data_retained)).unwrap()
}
