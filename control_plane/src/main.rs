use std::path::{Path, PathBuf};

use anyhow::Context;
use asap_planner::PlanningWorkload;
use clap::Parser;
use control_plane::{plan_workload, save_plan, DeploymentInputs};

/// Plan a workload with ASAPPlanner and write the selected plan.
#[derive(Parser)]
struct Args {
    /// Planner `PlanningWorkload` (JSON).
    #[arg(long)]
    workload: PathBuf,
    /// Planner `DeploymentCapabilities` (JSON); defaults to what this
    /// backend's data plane executes and keeps.
    #[arg(long)]
    deployment_capabilities: Option<PathBuf>,
    /// Planner `MeasuredOperationCost` list (JSON) that Stage 3 prices
    /// sketch operations from; by default every node is priced from the
    /// Planner's formulas.
    #[arg(long)]
    measured_operation_costs: Option<PathBuf>,
    /// Where to write the plan the data plane installs.
    #[arg(long)]
    out: PathBuf,
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> anyhow::Result<T> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
    let workload: PlanningWorkload = read_json(&args.workload)?;
    let mut inputs = DeploymentInputs::default();
    if let Some(path) = &args.deployment_capabilities {
        inputs.deployment_capabilities = read_json(path)?;
    }
    if let Some(path) = &args.measured_operation_costs {
        inputs.plan_selection.measured_operation_costs = read_json(path)?;
    }
    let plan = plan_workload(&workload, &inputs)?;
    save_plan(&plan, &args.out)?;
    println!(
        "wrote {} ({} queries, {} nodes)",
        args.out.display(),
        plan.queries.len(),
        plan.dag.dag.nodes.len()
    );
    Ok(())
}
