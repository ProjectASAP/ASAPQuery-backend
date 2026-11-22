use std::path::Path;

use asap_planner::{InstallablePlan, PlanningWorkload};
use control_plane::{plan_workload, DeploymentInputs};

/// The deployment inputs of a backend that keeps raw data or not.
pub fn inputs(raw_data_retained: bool) -> DeploymentInputs {
    let mut inputs = DeploymentInputs::default();
    inputs.deployment_capabilities.raw_data_retained = raw_data_retained;
    inputs
}

/// [`inputs`] for a deployment without multi-level window chains: plans
/// keep one level per chain, for tests about refresh groups' timing.
#[allow(dead_code)]
pub fn inputs_without_levels(raw_data_retained: bool) -> DeploymentInputs {
    let mut inputs = inputs(raw_data_retained);
    inputs.deployment_capabilities.excluded_candidates = serde_json::from_value(
        serde_json::json!([{"operator": "SummaryMergeWindows"}, {"operator": "TimeRangeCover"}]),
    )
    .unwrap();
    inputs
}

/// A #509 example workload in `examples/`.
#[allow(dead_code)]
pub fn workload(example: &str) -> PlanningWorkload {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(format!("../examples/planner-layering-{example}.json"));
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

/// The selected plan of a #509 example workload, for a deployment that
/// keeps raw data or not.
#[allow(dead_code)]
pub fn plan(example: &str, raw_data_retained: bool) -> InstallablePlan {
    plan_workload(&workload(example), &inputs(raw_data_retained))
        .unwrap()
        .0
}

#[allow(dead_code)]
pub fn plan_json(example: &str, raw_data_retained: bool) -> String {
    serde_json::to_string(&plan(example, raw_data_retained)).unwrap()
}
