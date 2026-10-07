use std::path::Path;

use control_plane::{backend_capabilities, plan_workload, InstalledPlan};

/// The selected plan of a #509 example workload in `examples/`, for a
/// deployment that keeps raw data or not.
pub fn plan(example: &str, raw_data_retained: bool) -> InstalledPlan {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(format!("../examples/planner-layering-{example}.json"));
    let workload = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let capabilities = planner_types::deployment::DeploymentCapabilities {
        raw_data_retained,
        ..backend_capabilities()
    };
    plan_workload(&workload, &capabilities).unwrap()
}

#[allow(dead_code)]
pub fn plan_json(example: &str, raw_data_retained: bool) -> String {
    serde_json::to_string(&plan(example, raw_data_retained)).unwrap()
}
