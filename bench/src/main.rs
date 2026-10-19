//! The benefit experiment: one workload, run once on Prometheus alone and
//! once on the backend executing ASAPPlanner's selected plan, with the
//! resources, latency and answers of both measured.

mod compare;
mod generator;
mod rules;
mod run;
mod workloads;

use std::path::{Path, PathBuf};

use anyhow::Context;
use asap_plan_selection::{CostModel, ResourceUnitCost};
use clap::{Parser, Subcommand, ValueEnum};
use control_plane::DeploymentInputs;
use generator::{Generator, Scale};
use serde_json::{json, Value};

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// Where Stage 3's unit prices come from.
#[derive(Debug, Clone, Copy, ValueEnum)]
enum Prices {
    /// `ResourceUnitCost::illustrative()`.
    Illustrative,
    /// AWS Fargate us-east-1, in US dollars: ASAPPlanner's
    /// `ResourceUnitCost::aws_fargate_us_east_1` (main 4ef9f3e5), which the
    /// pinned Planner revision does not have yet.
    #[value(name = "aws-fargate-us-east-1")]
    AwsFargateUsEast1,
}

/// ASAPPlanner 4ef9f3e5's `ResourceUnitCost::aws_fargate_us_east_1(speed)`:
/// $0.000011244 per vCPU-second and $0.000001235 per GiB-second, charged
/// per CPU operation and scanned byte at `speed`. Disk and the provisioned
/// (cost model 2) prices are 0. Replace with the Planner function when the
/// pin moves past 4ef9f3e5.
fn aws_fargate_us_east_1(speed: asap_plan_selection::ResourceSpeed) -> ResourceUnitCost {
    const DOLLARS_PER_VCPU_SECOND: f64 = 0.000_011_244;
    const DOLLARS_PER_MEMORY_GB_SECOND: f64 = 0.000_001_235;
    const BYTES_PER_GIB: f64 = (1u64 << 30) as f64;
    ResourceUnitCost {
        cost_per_cpu_op: DOLLARS_PER_VCPU_SECOND * speed.ms_per_cpu_op / 1_000.0,
        cost_per_scanned_byte: DOLLARS_PER_VCPU_SECOND * speed.ms_per_scanned_byte / 1_000.0,
        cost_per_memory_byte_second: DOLLARS_PER_MEMORY_GB_SECOND / BYTES_PER_GIB,
        cost_per_provisioned_memory_byte_second: 0.0,
        cost_per_provisioned_cpu_core_second: 0.0,
        cost_per_disk_byte_second: 0.0,
    }
}

#[derive(clap::Args, Clone)]
struct PlanningArgs {
    /// Stage 3's unit prices, under cost model 1.
    #[arg(long, value_enum, default_value = "illustrative")]
    prices: Prices,
    /// Planner `MeasuredOperationCost` list (JSON).
    #[arg(long)]
    measured_operation_costs: Option<PathBuf>,
    /// Declare that the deployment keeps no raw data, so the Planner builds
    /// every input at ingestion time (a what-if: Prometheus still stores
    /// the samples, and forwarded queries still reach it).
    #[arg(long)]
    no_raw_data: bool,
}

#[derive(clap::Args, Clone)]
struct ScaleArgs {
    /// Multiplies instances, nodes and other scaled label cardinalities.
    #[arg(long, default_value_t = 1)]
    scale: usize,
    #[arg(long, default_value_t = 15_000)]
    sample_interval_ms: i64,
    /// Data generated, in minutes.
    #[arg(long, default_value_t = 60)]
    duration_minutes: i64,
    #[arg(long, default_value_t = 1)]
    seed: u64,
    #[arg(long, default_value_t = 1e-4)]
    counter_reset_probability: f64,
}

/// 2026-01-01T00:00:00Z: generated data starts here, on every pane grid.
const START_MS: i64 = 1_767_225_600_000;

impl ScaleArgs {
    fn scale(&self) -> Scale {
        Scale {
            factor: self.scale,
            sample_interval_ms: self.sample_interval_ms,
            start_ms: START_MS,
            duration_ms: self.duration_minutes * 60_000,
            seed: self.seed,
            counter_reset_probability: self.counter_reset_probability,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum WorkloadName {
    /// The planner-layering examples' recurring queries.
    Examples,
    /// The kept Kubernetes rules of a `rules` survey.
    Kube,
}

#[derive(clap::Args, Clone)]
struct WorkloadArgs {
    #[arg(long, value_enum)]
    workload: WorkloadName,
    /// `rules.json` written by `rules` (for `--workload kube`).
    #[arg(long)]
    rules: Option<PathBuf>,
    #[arg(long, default_value = "examples")]
    examples_dir: PathBuf,
    /// Accuracy target of every rule (for `--workload kube`).
    #[arg(long, default_value = EXACT)]
    accuracy: String,
    /// Only these kept rules, by name (for `--workload kube`): the Planner
    /// cannot plan every kept rule at once.
    #[arg(long, value_delimiter = ',')]
    only_rules: Vec<String>,
    #[command(flatten)]
    scale: ScaleArgs,
}

const EXACT: &str =
    r#"{"max_error":0.0,"failure_probability":0.0,"accuracy_scope":"whole_result"}"#;

impl WorkloadArgs {
    fn load(&self) -> anyhow::Result<workloads::Workload> {
        match self.workload {
            WorkloadName::Examples => workloads::examples(&self.examples_dir, self.scale.scale()),
            WorkloadName::Kube => {
                let path = self
                    .rules
                    .as_ref()
                    .context("--rules is required for --workload kube")?;
                let mut survey: Vec<rules::SurveyedRule> = read_json(path)?;
                if !self.only_rules.is_empty() {
                    survey.retain(|r| self.only_rules.contains(&r.rule.name));
                }
                workloads::kube_rules(
                    &survey,
                    self.scale.scale(),
                    &serde_json::from_str(&self.accuracy)?,
                )
            }
        }
    }
}

#[derive(clap::Args, Clone)]
struct RunArgs {
    #[arg(long, default_value = "prometheus")]
    prometheus_bin: PathBuf,
    #[arg(long, default_value = "target/release/data_plane")]
    data_plane_bin: PathBuf,
    /// `taskset -c` core list of Prometheus.
    #[arg(long, default_value = "0-3")]
    prometheus_cores: String,
    /// `taskset -c` core list of the data plane.
    #[arg(long, default_value = "4-7")]
    data_plane_cores: String,
    /// Queries are evaluated from this many minutes after the first sample.
    #[arg(long, default_value_t = 10)]
    eval_start_minutes: i64,
    #[arg(long, default_value_t = 29390)]
    prometheus_port: u16,
    #[arg(long, default_value_t = 28388)]
    data_plane_port: u16,
    /// The data plane's `--execution-memory-limit-mib`, also used by the
    /// harness' executor reference.
    #[arg(long, default_value_t = 4096)]
    execution_memory_limit_mib: usize,
}

#[derive(Subcommand)]
enum Command {
    /// Plan, run the baseline and the ASAP run, and compare them: writes
    /// `plan/`, `baseline/`, `asap/`, `report.json` and `summary.md`.
    Experiment {
        #[command(flatten)]
        workload: WorkloadArgs,
        #[command(flatten)]
        planning: PlanningArgs,
        #[command(flatten)]
        run: RunArgs,
        /// Reuse `plan/` from an earlier experiment in `out_dir`.
        #[arg(long)]
        reuse_plan: bool,
        /// Reuse `baseline/` and `asap/` from an earlier experiment: only compare.
        #[arg(long)]
        reuse_runs: bool,
        #[arg(long)]
        out_dir: PathBuf,
    },
    /// Plan a workload: write the Planner workload, the selected plan, the
    /// same plan with nothing materialized (the correctness reference), and
    /// Stage 3's predicted usage.
    Plan {
        #[command(flatten)]
        workload: WorkloadArgs,
        #[command(flatten)]
        planning: PlanningArgs,
        #[arg(long)]
        out_dir: PathBuf,
    },
    /// Survey the rule set: which rules the Planner plans and the data plane
    /// runs; write the kept rules' workload.
    Rules {
        /// `rule_trace.jsonl` (fields name, query, interval_seconds, type, group).
        #[arg(long)]
        trace: PathBuf,
        /// Accuracy target of every rule (Planner `AccuracyTarget` JSON).
        #[arg(long, default_value = EXACT)]
        accuracy: String,
        #[command(flatten)]
        scale: ScaleArgs,
        #[command(flatten)]
        planning: PlanningArgs,
        #[arg(long)]
        out_dir: PathBuf,
    },
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> anyhow::Result<T> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))
}

fn write_json(path: &Path, value: &impl serde::Serialize) -> anyhow::Result<()> {
    std::fs::write(path, serde_json::to_string_pretty(value)?)
        .with_context(|| format!("writing {}", path.display()))
}

fn deployment_inputs(args: &PlanningArgs) -> anyhow::Result<DeploymentInputs> {
    let mut inputs = DeploymentInputs::default();
    inputs.plan_selection.cost_model = match args.prices {
        Prices::Illustrative => CostModel::UsageOverTime(ResourceUnitCost::illustrative()),
        Prices::AwsFargateUsEast1 => {
            CostModel::UsageOverTime(aws_fargate_us_east_1(inputs.plan_selection.resource_speed))
        }
    };
    inputs.deployment_capabilities.raw_data_retained = !args.no_raw_data;
    if let Some(path) = &args.measured_operation_costs {
        inputs.plan_selection.measured_operation_costs = read_json(path)?;
    }
    Ok(inputs)
}

/// Stage 3's record for a report: the selected candidate, how its nodes
/// are materialized, and the predicted usage of it and of the same plan
/// with nothing materialized.
fn selection_summary(
    plan: &control_plane::InstalledPlan,
    selection: &asap_plan_selection::Selection,
    inputs: &DeploymentInputs,
) -> Value {
    let mut materializations = std::collections::BTreeMap::<String, usize>::new();
    for node in &plan.dag.dag.nodes {
        let m = serde_json::to_value(node.materialization).unwrap_or_default();
        let name = match &m {
            Value::String(s) => s.clone(),
            Value::Object(o) => o.keys().next().cloned().unwrap_or_default(),
            other => other.to_string(),
        };
        *materializations.entry(name).or_default() += 1;
    }
    let speed = inputs.plan_selection.resource_speed;
    let usage = |id: &str| {
        selection.costs.get(id).map(|c| {
            let u = &c.usage;
            let cpu_ms_per_s = u.cpu_ops_per_second * speed.ms_per_cpu_op
                + u.scanned_bytes_per_second * speed.ms_per_scanned_byte;
            json!({
                "candidate": id,
                "cost_per_second": c.cost_per_second,
                "cost_source": c.source,
                "cpu_ops_per_second": u.cpu_ops_per_second,
                "scanned_bytes_per_second": u.scanned_bytes_per_second,
                "cpu_core_seconds_per_second": cpu_ms_per_s / 1000.0,
                "average_memory_bytes": u.average_memory_bytes,
                "peak_memory_bytes": u.peak_memory_bytes,
                "peak_cpu_cores": u.peak_cpu_cores,
                "latency_ms_per_query": u.latency_per_query.iter().map(|l| l.total_ms()).collect::<Vec<_>>(),
            })
        })
    };
    let unmaterialized = selection
        .selected
        .split("-m")
        .next()
        .unwrap_or(&selection.selected)
        .to_string();
    let mut causes = std::collections::BTreeMap::<String, usize>::new();
    for r in &selection.rejected {
        *causes
            .entry(
                serde_json::to_value(r.cause)
                    .unwrap_or_default()
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
            )
            .or_default() += 1;
    }
    let mut ranked: Vec<(&String, f64)> = selection
        .costs
        .iter()
        .filter_map(|(id, c)| c.cost_per_second.map(|v| (id, v)))
        .collect();
    ranked.sort_by(|a, b| a.1.total_cmp(&b.1));
    let cheapest: Vec<Value> = ranked
        .iter()
        .take(5)
        .map(|(id, _)| usage(id).unwrap_or_default())
        .collect();
    let cheapest_materialized = ranked
        .iter()
        .find(|(id, _)| id.contains("-m"))
        .and_then(|(id, _)| usage(id));
    json!({
        "selected": selection.selected,
        "cheapest_valid_candidates": cheapest,
        "cheapest_materialized_candidate": cheapest_materialized,
        "valid_candidates": selection.costs.len(),
        "rejections_by_cause": causes,
        "node_materializations": materializations,
        "accepted_accuracy": selection.accepted.get(&selection.selected)
            .map(|a| a.iter().map(|x| x.to_string()).collect::<Vec<_>>()),
        "predicted_selected": usage(&selection.selected),
        "predicted_nothing_materialized": usage(&unmaterialized),
    })
}

fn plan_into(
    w: &workloads::Workload,
    inputs: &DeploymentInputs,
    dir: &Path,
) -> anyhow::Result<Value> {
    std::fs::create_dir_all(dir)?;
    write_json(&dir.join("workload.json"), &w.planning)?;
    write_json(&dir.join("queries.json"), &w.queries)?;
    let started = std::time::Instant::now();
    let (plan, selection) = control_plane::plan_and_select(&w.planning, inputs)?;
    let planning_seconds = started.elapsed().as_secs_f64();
    plan.save(&dir.join("plan.json"))?;
    let reference = control_plane::plan_selected_at_query_time(&w.planning, inputs)?;
    reference.save(&dir.join("reference_plan.json"))?;
    let mut summary = selection_summary(&plan, &selection, inputs);
    summary["planning_seconds"] = json!(planning_seconds);
    write_json(&dir.join("selection.json"), &summary)?;
    Ok(summary)
}

fn read_evaluations(path: &Path) -> anyhow::Result<Vec<run::Evaluation>> {
    std::fs::read_to_string(path)?
        .lines()
        .map(|l| Ok(serde_json::from_str(l)?))
        .collect()
}

fn experiment(
    workload: &WorkloadArgs,
    planning: &PlanningArgs,
    args: &RunArgs,
    reuse_plan: bool,
    reuse_runs: bool,
    out_dir: &Path,
) -> anyhow::Result<()> {
    std::fs::create_dir_all(out_dir)?;
    let out_dir = std::fs::canonicalize(out_dir)?;
    data_plane::precompute::set_execution_memory_limit(args.execution_memory_limit_mib << 20);
    let inputs = deployment_inputs(planning)?;
    let w = workload.load()?;
    let plan_dir = out_dir.join("plan");
    let selection: Value = if reuse_plan {
        read_json(&plan_dir.join("selection.json"))?
    } else {
        plan_into(&w, &inputs, &plan_dir)?
    };
    eprintln!(
        "selected {}: {}",
        selection["selected"], selection["node_materializations"]
    );
    let scale = workload.scale.scale();
    let wanted = |m: &str| w.metrics.iter().any(|x| x == m);
    let runtime = tokio::runtime::Runtime::new()?;
    let mut metrics = Vec::new();
    for mode in [run::Mode::Baseline, run::Mode::Asap] {
        if reuse_runs {
            let dir = if mode == run::Mode::Baseline {
                "baseline"
            } else {
                "asap"
            };
            metrics.push(read_json::<run::RunMetrics>(
                &out_dir.join(dir).join("metrics.json"),
            )?);
            continue;
        }
        let config = run::RunConfig {
            mode,
            prometheus_bin: args.prometheus_bin.clone(),
            data_plane_bin: args.data_plane_bin.clone(),
            plan: plan_dir.join("plan.json"),
            prometheus_cores: args.prometheus_cores.clone(),
            data_plane_cores: args.data_plane_cores.clone(),
            eval_start_ms: scale.start_ms + args.eval_start_minutes * 60_000,
            out_dir: out_dir.join(match mode {
                run::Mode::Baseline => "baseline",
                run::Mode::Asap => "asap",
            }),
            prometheus_port: args.prometheus_port,
            data_plane_port: args.data_plane_port,
            execution_memory_limit_mib: args.execution_memory_limit_mib,
        };
        let generator = Generator::new(scale, wanted);
        eprintln!(
            "{mode:?} run: {} series",
            generator.series_per_metric.values().sum::<u64>()
        );
        let m = runtime.block_on(run::run(&config, generator, &w.queries))?;
        eprintln!("{mode:?}: {}", serde_json::to_string(&m)?);
        metrics.push(m);
    }
    let baseline = read_evaluations(&out_dir.join("baseline/evaluations.jsonl"))?;
    let asap = read_evaluations(&out_dir.join("asap/evaluations.jsonl"))?;
    let index = compare::SampleIndex::new(Generator::new(scale, wanted));
    let reference_plan = control_plane::InstalledPlan::load(&plan_dir.join("reference_plan.json"))?;
    let mut reference = compare::Reference::new(&reference_plan, &index)?;
    let queries = compare::compare(&w.queries, &baseline, &asap, &mut reference, &index);
    let generator = Generator::new(scale, wanted);
    let report = json!({
        "scale": scale,
        "series_per_metric": generator.series_per_metric,
        "series_total": generator.series_per_metric.values().sum::<u64>(),
        "deployment_inputs": {
            "prices": format!("{:?}", planning.prices),
            "cost_model": "cost model 1 (usage over time)",
            "measured_operation_costs": planning.measured_operation_costs,
            "raw_data_retained": !planning.no_raw_data,
        },
        "selection": selection,
        "runs": metrics,
        "queries": queries,
    });
    write_json(&out_dir.join("report.json"), &report)?;
    let summary = summary_markdown(&report);
    std::fs::write(out_dir.join("summary.md"), &summary)?;
    println!("{summary}");
    Ok(())
}

/// The report's tables in Markdown.
fn summary_markdown(report: &Value) -> String {
    let mut md = String::new();
    let f = |v: &Value| v.as_f64().map_or("-".to_string(), |x| format!("{x:.4}"));
    let scale = &report["scale"];
    let duration_s = scale["duration_ms"].as_f64().unwrap_or(0.0) / 1000.0;
    md += &format!(
        "Scale: factor {}, {} series, one sample every {} ms, {} min of data, seed {}.\n\n",
        scale["factor"],
        report["series_total"],
        scale["sample_interval_ms"],
        duration_s / 60.0,
        scale["seed"]
    );
    md += "| Run | Process | Cores | CPU core-seconds | Peak RSS GiB | Mean RSS GiB | Wall s |\n|---|---|---|---|---|---|---|\n";
    for run in report["runs"].as_array().into_iter().flatten() {
        for p in run["processes"].as_array().into_iter().flatten() {
            md += &format!(
                "| {} | {} | {} | {} | {} | {} | {:.1} |\n",
                run["mode"].as_str().unwrap_or_default(),
                p["name"].as_str().unwrap_or_default(),
                p["cores"].as_str().unwrap_or_default(),
                f(&p["cpu_core_seconds"]),
                f(&p["peak_rss_gib"]),
                f(&p["mean_rss_gib"]),
                run["wall_seconds"].as_f64().unwrap_or(0.0)
            );
        }
    }
    md += "\n| Query | Evals | By plan | Forwarded | Baseline = reference | Plan = reference | ASAP meets target | Labels differ | Max abs err | Max rel err | Baseline p50/p99 ms | ASAP p50/p99 ms |\n|---|---|---|---|---|---|---|---|---|---|---|---|\n";
    for q in report["queries"].as_array().into_iter().flatten() {
        let forwarded: u64 = q["forwarded"]
            .as_object()
            .map_or(0, |o| o.values().filter_map(Value::as_u64).sum());
        md += &format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {:.3e} | {:.3e} | {:.2} / {:.2} | {:.2} / {:.2} |\n",
            q["name"].as_str().unwrap_or_default(), q["evaluations"], q["answered_by_plan"], forwarded,
            q["baseline_matches_reference"], q["plan_matches_reference"], q["asap_meets_target"], q["asap_labels_differ"],
            q["asap_max_abs_error"].as_f64().unwrap_or(0.0), q["asap_max_rel_error"].as_f64().unwrap_or(0.0),
            q["baseline_latency_ms_p50"].as_f64().unwrap_or(f64::NAN), q["baseline_latency_ms_p99"].as_f64().unwrap_or(f64::NAN),
            q["asap_latency_ms_p50"].as_f64().unwrap_or(f64::NAN), q["asap_latency_ms_p99"].as_f64().unwrap_or(f64::NAN),
        );
    }
    let sel = &report["selection"];
    md += &format!(
        "\nStage 3 selected `{}` ({}). Predicted over the {} min of data, against the measured run:\n\n",
        sel["selected"].as_str().unwrap_or_default(), sel["node_materializations"], duration_s / 60.0
    );
    md += "| Plan | Predicted CPU core-seconds | Predicted mean memory GiB | Predicted peak memory GiB |\n|---|---|---|---|\n";
    for key in ["predicted_selected", "predicted_nothing_materialized"] {
        let p = &sel[key];
        let gib = |v: &Value| {
            v.as_f64()
                .map_or("-".into(), |x| format!("{:.6}", x / (1u64 << 30) as f64))
        };
        md += &format!(
            "| {} (`{}`) | {:.4} | {} | {} |\n",
            key.trim_start_matches("predicted_").replace('_', " "),
            p["candidate"].as_str().unwrap_or_default(),
            p["cpu_core_seconds_per_second"].as_f64().unwrap_or(0.0) * duration_s,
            gib(&p["average_memory_bytes"]),
            gib(&p["peak_memory_bytes"])
        );
    }
    md
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    match Cli::parse().command {
        Command::Rules {
            trace,
            accuracy,
            scale,
            planning,
            out_dir,
        } => {
            std::fs::create_dir_all(&out_dir)?;
            let accuracy: Value = serde_json::from_str(&accuracy)?;
            let inputs = deployment_inputs(&planning)?;
            let catalog = generator::catalog_metrics();
            let generator = Generator::new(scale.scale(), |_| true);
            // The probe evaluates every rule once, at the end of the data.
            let probe_scale = scale.scale();
            let index = compare::SampleIndex::new(Generator::new(probe_scale, |_| true));
            let probe_ms = probe_scale.start_ms + probe_scale.duration_ms;
            let rules = rules::read_trace(&trace)?;
            let surveyed = rules::survey(
                &rules,
                &catalog,
                |metrics| generator.data_workload(metrics),
                &accuracy,
                &inputs,
                &index,
                probe_ms,
            );
            for r in &surveyed {
                eprintln!(
                    "{:<45} {}",
                    r.rule.name,
                    r.dropped.as_deref().unwrap_or("KEPT")
                );
            }
            let kept = surveyed.iter().filter(|r| r.dropped.is_none()).count();
            eprintln!("{kept} of {} rules kept", surveyed.len());
            write_json(&out_dir.join("rules.json"), &surveyed)?;
        }
        Command::Plan {
            workload,
            planning,
            out_dir,
        } => {
            std::fs::create_dir_all(&out_dir)?;
            let inputs = deployment_inputs(&planning)?;
            let w = workload.load()?;
            write_json(&out_dir.join("workload.json"), &w.planning)?;
            write_json(&out_dir.join("queries.json"), &w.queries)?;
            let (plan, selection) = control_plane::plan_and_select(&w.planning, &inputs)?;
            plan.save(&out_dir.join("plan.json"))?;
            let reference = control_plane::plan_selected_at_query_time(&w.planning, &inputs)?;
            reference.save(&out_dir.join("reference_plan.json"))?;
            let summary = selection_summary(&plan, &selection, &inputs);
            println!("{}", serde_json::to_string_pretty(&summary)?);
            write_json(&out_dir.join("selection.json"), &summary)?;
        }
        Command::Experiment {
            workload,
            planning,
            run,
            reuse_plan,
            reuse_runs,
            out_dir,
        } => experiment(&workload, &planning, &run, reuse_plan, reuse_runs, &out_dir)?,
    }
    Ok(())
}
