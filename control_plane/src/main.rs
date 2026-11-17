use std::path::{Path, PathBuf};

use anyhow::Context;
use asap_planner::PlanningWorkload;
use clap::Parser;
use control_plane::{plan_workload, save_plan, save_report, DeploymentInputs};

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
    /// Planner `CostInputs` (JSON): Stage 3's cost model, resource speed
    /// and measured operation costs; an omitted field takes the Planner's
    /// default.
    #[arg(long)]
    cost_inputs: Option<PathBuf>,
    /// Planner `MeasuredOperationCosts` (JSON) that Stage 3 costs operators
    /// from, in place of the cost inputs' table; by default every node is
    /// costed from the Planner's formulas.
    #[arg(long)]
    measured_operation_costs: Option<PathBuf>,
    /// Accept every query output regardless of its accuracy guarantee:
    /// candidates are compared on cost alone; executability checks still
    /// apply.
    #[arg(long)]
    ignore_accuracy_guarantees: bool,
    /// Where to write the plan the data plane installs.
    #[arg(long)]
    out: PathBuf,
    /// Where to write Stage 3's report of the selected plan: its cost and
    /// predicted resource usage, per node and per query.
    #[arg(long)]
    report: Option<PathBuf>,
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
    if let Some(path) = &args.cost_inputs {
        inputs.cost_inputs = read_json(path)?;
    }
    if let Some(path) = &args.measured_operation_costs {
        inputs.cost_inputs.measured_operation_costs = read_json(path)?;
    }
    inputs.ignore_accuracy_guarantees = args.ignore_accuracy_guarantees;
    let (plan, report) = plan_workload(&workload, &inputs)?;
    save_plan(&plan, &args.out)?;
    if let Some(path) = &args.report {
        save_report(&report, path)?;
    }
    for fact in &report.assumed_data_facts {
        println!("assumed data fact: {fact:?}");
    }
    println!(
        "wrote {} ({} queries, {} nodes)",
        args.out.display(),
        plan.query_texts.len(),
        plan.dag.nodes.len()
    );
    Ok(())
}
