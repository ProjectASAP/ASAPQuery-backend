use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use control_plane::{backend_capabilities, plan_workload};
use planner_types::deployment::DeploymentCapabilities;
use planner_types::workload::PlanningWorkload;

/// Plan a workload with ASAPPlanner and write the selected plan.
#[derive(Parser)]
struct Args {
    /// Planner `PlanningWorkload` (JSON).
    #[arg(long)]
    workload: PathBuf,
    /// Planner `DeploymentCapabilities` (JSON); defaults to what this
    /// backend's data plane executes.
    #[arg(long)]
    capabilities: Option<PathBuf>,
    /// Where to write the plan the data plane installs.
    #[arg(long)]
    out: PathBuf,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
    let text = std::fs::read_to_string(&args.workload)
        .with_context(|| format!("reading {}", args.workload.display()))?;
    let workload: PlanningWorkload = serde_json::from_str(&text)
        .with_context(|| format!("parsing {}", args.workload.display()))?;
    let capabilities: DeploymentCapabilities = match &args.capabilities {
        Some(path) => serde_json::from_str(
            &std::fs::read_to_string(path)
                .with_context(|| format!("reading {}", path.display()))?,
        )
        .with_context(|| format!("parsing {}", path.display()))?,
        None => backend_capabilities(),
    };
    let plan = plan_workload(&workload, &capabilities)?;
    plan.save(&args.out)?;
    println!(
        "wrote {} ({} queries, {} nodes)",
        args.out.display(),
        plan.queries.len(),
        plan.dag.dag.nodes.len()
    );
    Ok(())
}
