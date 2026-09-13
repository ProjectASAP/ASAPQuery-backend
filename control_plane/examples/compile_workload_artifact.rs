//! Control-plane entry point: cost-select a workload and emit its atomic install request.
use control_plane::physical::compiler::BackendLocalPlanningInput;
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let path = args
        .next()
        .ok_or("usage: compile_workload_artifact SNAPSHOT.json [--metricsql]")?;
    let metricsql = match args.next().as_deref() {
        None => false,
        Some("--metricsql") => true,
        Some(_) => return Err("expected optional --metricsql".into()),
    };
    if args.next().is_some() {
        return Err("unexpected arguments".into());
    }
    let snapshot: BackendLocalPlanningInput = serde_json::from_slice(&std::fs::read(path)?)?;
    let start = std::time::Instant::now();
    let plan = if metricsql {
        snapshot.compile_metricsql()?
    } else {
        snapshot.compile_promql()?
    };
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
            "logical_selection": plan.planner_selection_trace,
            "backend_revision": control_plane::physical::compiler::BACKEND_REVISION,
            "planner_revision": control_plane::physical::compiler::PLANNER_REVISION,
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
