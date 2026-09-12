//! Compile reproducible comparison inputs without requiring a cost-optimality claim.
use control_plane::physical::compiler::{BackendLocalPlanningSnapshot, PLANNER_REVISION};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 2 {
        return Err(
            "usage: compile_comparison_workload prometheus|victoriametrics|clickhouse INPUT.json"
                .into(),
        );
    }
    let input = std::fs::read(&args[1])?;
    let start = std::time::Instant::now();
    let (install, selection) = match args[0].as_str() {
        "clickhouse" => {
            let workload = serde_json::from_slice(&input)?;
            let (publication, trace) =
                control_plane::clickhouse::compile_automatic_clickhouse_workload(&workload).await?;
            (
                serde_json::to_value(publication.install_request(None, Vec::new())?)?,
                json!(trace),
            )
        }
        engine @ ("prometheus" | "victoriametrics") => {
            let snapshot: BackendLocalPlanningSnapshot = serde_json::from_slice(&input)?;
            let plan = if engine == "victoriametrics" {
                snapshot.compile_metricsql()?
            } else {
                snapshot.compile()?
            };
            let selection = json!({"cost_comparison": plan.cost_comparison, "lifecycle_estimates": plan.lifecycle_estimates});
            (
                json!({"summary_catalog": plan.summary_catalog, "collector_plans": plan.collector_plans,
                    "precompute_plan": plan.precompute_plan, "transmission_plan": plan.transmission_plan,
                    "query_plan": plan.query_plan, "storage_routing": null, "adaptation_evidence": []}),
                selection,
            )
        }
        _ => return Err("unknown comparison engine".into()),
    };
    serde_json::to_writer_pretty(
        std::io::stdout(),
        &json!({
            "install": install, "selection": selection, "planner_revision": PLANNER_REVISION,
            "backend_revision": env!("ASAPQUERY_BACKEND_REVISION"),
            "planning_elapsed_ns": start.elapsed().as_nanos(),
            "scope": "compile supplied planning evidence; measured execution does not establish cost-model optimality"
        }),
    )?;
    Ok(())
}
