use std::path::Path;

use control_plane::{plan_workload, DeploymentInputs, InstalledPlan};

/// The deployment inputs of a backend that keeps raw data or not.
pub fn inputs(raw_data_retained: bool) -> DeploymentInputs {
    let mut inputs = DeploymentInputs::default();
    inputs.deployment_capabilities.raw_data_retained = raw_data_retained;
    inputs
}

/// The selected plan of a #509 example workload in `examples/`, for a
/// deployment that keeps raw data or not.
pub fn plan(example: &str, raw_data_retained: bool) -> InstalledPlan {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(format!("../examples/planner-layering-{example}.json"));
    let workload = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    plan_workload(&workload, &inputs(raw_data_retained)).unwrap()
}

#[allow(dead_code)]
pub fn plan_json(example: &str, raw_data_retained: bool) -> String {
    serde_json::to_string(&plan(example, raw_data_retained)).unwrap()
}
