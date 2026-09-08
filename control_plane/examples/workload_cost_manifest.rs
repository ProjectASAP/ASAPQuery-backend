//! Emit pricing requirements; never fabricate quotes or publish a plan.
use control_plane::physical::{
    compiler::BackendLocalPlanningSnapshot, compiler::PhysicalCompiler, workload_cost,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: workload_cost_manifest SNAPSHOT.json")?;
    let snapshot: BackendLocalPlanningSnapshot =
        serde_json::from_str(&std::fs::read_to_string(path)?)?;
    let (request, environment) = snapshot.planning_request()?;
    let manifests = workload_cost::with_exact_alternative(request)?
        .into_iter()
        .map(|candidate| {
            let queries = candidate.queries.clone();
            PhysicalCompiler
                .compile(candidate, environment.clone())
                .and_then(|plan| workload_cost::manifest(&plan, &queries))
        })
        .collect::<Result<Vec<_>, _>>()?;
    println!("{}", serde_json::to_string_pretty(&manifests)?);
    Ok(())
}
