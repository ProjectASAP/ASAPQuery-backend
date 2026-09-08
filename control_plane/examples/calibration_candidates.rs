//! Export every bindable candidate for isolated measurement, without selecting a winner.
use control_plane::physical::{
    compiler::{BackendLocalPlanningSnapshot, PhysicalCompiler},
    workload_cost,
};
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: calibration_candidates SNAPSHOT.json")?;
    let snapshot: BackendLocalPlanningSnapshot = serde_json::from_slice(&std::fs::read(path)?)?;
    let (request, environment) = snapshot.planning_request()?;
    let mut results = Vec::new();
    for (index, candidate) in workload_cost::with_exact_alternative(request)?
        .into_iter()
        .enumerate()
    {
        let queries = candidate.queries.clone();
        let plan = match PhysicalCompiler.compile(candidate, environment.clone()) {
            Ok(plan) => plan,
            Err(error) => {
                results.push(
                    json!({"candidate_index": index, "unavailable_reason": error.to_string()}),
                );
                continue;
            }
        };
        let manifest = match workload_cost::manifest(&plan, &queries) {
            Ok(manifest) => manifest,
            Err(error) => {
                results.push(
                    json!({"candidate_index": index, "unavailable_reason": error.to_string()}),
                );
                continue;
            }
        };
        results.push(json!({
            "candidate_index": index,
            "manifest": manifest,
            "lifecycle_estimates": plan.lifecycle_estimates,
            "install_request": {
                "precompute_plan": plan.precompute_plan,
                "transmission_plan": plan.transmission_plan,
                "backend_plan": plan.backend_plan.encode_to_vec(),
                "query_plan": plan.query_plan,
                "storage_routing": null,
                "adaptation_evidence": []
            }
        }));
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({"purpose":"calibration_only", "candidates":results}))?
    );
    Ok(())
}
