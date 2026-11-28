use std::path::{Path, PathBuf};

use anyhow::Context;
use asap_planner::{InstallablePlan, PlanningWorkload};
use clap::Parser;
use control_plane::collector_views::{collector_sub_dag, CollectorCapabilities, CommitGrace};
use control_plane::{load_plan, plan_workload, save_plan, save_report, DeploymentInputs};

/// Plan a workload with ASAPPlanner and write the selected plan; or, with
/// `--plan`, read a plan written before. With `--collector-capabilities`,
/// also write the part of the plan the collector runs
/// (`--emit-collector-views`).
#[derive(Parser)]
struct Args {
    /// Planner `PlanningWorkload` (JSON).
    #[arg(long, required_unless_present = "plan", requires = "out")]
    workload: Option<PathBuf>,
    /// A plan this command wrote before, in place of planning `--workload`:
    /// only its collector views are written.
    #[arg(
        long,
        requires = "collector_capabilities",
        conflicts_with_all = [
            "workload", "out", "report", "deployment_capabilities", "cost_inputs",
            "measured_operation_costs", "ignore_accuracy_guarantees",
        ]
    )]
    plan: Option<PathBuf>,
    /// What the collector runs (`asap_sketches --print-capabilities`):
    /// the views it can build in place of the data plane.
    #[arg(long, requires = "emit_collector_views")]
    collector_capabilities: Option<PathBuf>,
    /// Where to write the collector-views file the collector and the data
    /// plane (`--collector-views`) load.
    #[arg(long, requires = "collector_capabilities")]
    emit_collector_views: Option<PathBuf>,
    /// How often the collector's pipeline exports, in ms (0: each message
    /// at once): part of the commit grace of a collector view's window.
    #[arg(long, default_value_t = CommitGrace::default().export_interval_ms)]
    collector_export_interval_ms: u64,
    /// How far the collector's clock may run behind the data plane's, in
    /// ms: part of the commit grace.
    #[arg(long, default_value_t = CommitGrace::default().clock_skew_allowance_ms)]
    collector_clock_skew_allowance_ms: u64,
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
    out: Option<PathBuf>,
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
    let plan = match (&args.plan, &args.workload, &args.out) {
        (Some(path), _, _) => load_plan(path)?,
        (None, Some(workload), Some(out)) => plan(&args, workload, out)?,
        _ => anyhow::bail!("give --plan, or --workload and --out"),
    };
    if let (Some(capabilities), Some(out)) =
        (&args.collector_capabilities, &args.emit_collector_views)
    {
        let text = std::fs::read_to_string(capabilities)
            .with_context(|| format!("reading {}", capabilities.display()))?;
        let capabilities = CollectorCapabilities::from_json(&text)
            .with_context(|| format!("parsing {}", capabilities.display()))?;
        let grace = CommitGrace {
            export_interval_ms: args.collector_export_interval_ms,
            clock_skew_allowance_ms: args.collector_clock_skew_allowance_ms,
        };
        let (file, not_on_collector) = collector_sub_dag(&plan, &capabilities, grace)?;
        let text = serde_json::to_string_pretty(&file)?;
        std::fs::write(out, text).with_context(|| format!("writing {}", out.display()))?;
        for (node, reason) in &not_on_collector {
            println!("not on the collector: node {node}: {reason:?}");
        }
        println!(
            "wrote {} (collector views {:?}, backend reads raw {:?}, commit grace {} ms)",
            out.display(),
            file.outputs,
            file.backend_reads_raw,
            file.commit_grace_ms
        );
    }
    Ok(())
}

/// Plans `workload` as the arguments say and writes the plan to `out`.
fn plan(args: &Args, workload: &Path, out: &Path) -> anyhow::Result<InstallablePlan> {
    let workload: PlanningWorkload = read_json(workload)?;
    let mut inputs = DeploymentInputs::default();
    if let Some(path) = &args.deployment_capabilities {
        // The file's fields over this backend's own capabilities: a field it
        // leaves out keeps the backend's value, not the Planner's default.
        let mut capabilities = serde_json::to_value(control_plane::backend_capabilities())?;
        let given: serde_json::Value = read_json(path)?;
        if let (Some(capabilities), Some(given)) = (capabilities.as_object_mut(), given.as_object())
        {
            capabilities.extend(given.clone());
        }
        inputs.deployment_capabilities = serde_json::from_value(capabilities)
            .with_context(|| format!("parsing {}", path.display()))?;
    }
    if let Some(path) = &args.cost_inputs {
        inputs.cost_inputs = read_json(path)?;
    }
    if let Some(path) = &args.measured_operation_costs {
        inputs.cost_inputs.measured_operation_costs = read_json(path)?;
    }
    inputs.ignore_accuracy_guarantees = args.ignore_accuracy_guarantees;
    let (plan, report) = plan_workload(&workload, &inputs)?;
    save_plan(&plan, out)?;
    if let Some(path) = &args.report {
        save_report(&report, path)?;
    }
    for fact in &report.assumed_data_facts {
        println!("assumed data fact: {fact:?}");
    }
    println!(
        "wrote {} ({} queries, {} nodes)",
        out.display(),
        plan.query_texts.len(),
        plan.dag.nodes.len()
    );
    Ok(plan)
}
