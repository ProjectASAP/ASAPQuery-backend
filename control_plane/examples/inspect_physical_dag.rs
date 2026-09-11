//! Inspect an ordinary planning snapshot without treating demo costs as measurements.
use control_plane::physical::compiler::BackendLocalPlanningSnapshot;
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let path = args
        .next()
        .ok_or("usage: inspect_physical_dag SNAPSHOT.json [--metricsql]")?;
    let metricsql = match args.next().as_deref() {
        None => false,
        Some("--metricsql") => true,
        Some(_) => return Err("expected optional --metricsql".into()),
    };
    if args.next().is_some() {
        return Err("unexpected arguments".into());
    }
    let snapshot: BackendLocalPlanningSnapshot = serde_json::from_slice(&std::fs::read(path)?)?;
    let snapshot_version = snapshot.snapshot_version;
    let erp_supplied = snapshot.implementation.erp.is_some();
    let plan = if metricsql {
        snapshot.compile_metricsql()?
    } else {
        snapshot.compile()?
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "purpose": "inspection_only",
            "snapshot_version": snapshot_version,
            "erp_input_supplied": erp_supplied,
            "backend_revision": control_plane::physical::compiler::BACKEND_REVISION,
            "planner_revision": control_plane::physical::compiler::PLANNER_REVISION,
            "cost_comparison": plan.cost_comparison,
            "lifecycle_estimates": plan.lifecycle_estimates,
            "install_request": {
                "summary_catalog": plan.summary_catalog,
                "collector_plans": plan.collector_plans,
                "precompute_plan": plan.precompute_plan,
                "transmission_plan": plan.transmission_plan,
                "query_plan": plan.query_plan,
                "storage_routing": null,
                "adaptation_evidence": []
            }
        }))?
    );
    Ok(())
}
