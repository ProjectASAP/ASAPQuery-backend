//! Control-plane entry point: cost-select a workload and emit its atomic install request.
use control_plane::physical::compiler::BackendLocalPlanningSnapshot;
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: compile_workload_artifact SNAPSHOT.json")?;
    let snapshot: BackendLocalPlanningSnapshot = serde_json::from_slice(&std::fs::read(path)?)?;
    if snapshot.snapshot_version != 2 {
        return Err(
            "execution evaluation requires version 2 complete workload cost evidence".into(),
        );
    }
    let start = std::time::Instant::now();
    let plan = snapshot.compile()?;
    let elapsed = start.elapsed().as_nanos();
    let comparison = plan
        .cost_comparison
        .ok_or("missing complete-plan comparison")?;
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "schema_version": 1,
            "planning_elapsed_ns": elapsed,
            "envelope": plan.envelope,
            "cost_comparison": comparison,
            "lifecycle_estimates": plan.lifecycle_estimates,
            "install_request": {
                "summary_catalog": plan.summary_catalog,
                "collector_plans": plan.collector_plans,
                "precompute_plan": plan.precompute_plan,
                "transmission_plan": plan.transmission_plan,
                "backend_plan": plan.backend_plan.encode_to_vec(),
                "query_plan": plan.query_plan,
                "storage_routing": null,
                "adaptation_evidence": []
            }
        }))?
    );
    Ok(())
}
