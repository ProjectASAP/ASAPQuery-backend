//! Full-row-distinct Alibaba call-observation dashboard comparison.
#[path = "alibaba_dashboard/execution.rs"]
mod execution;
#[path = "alibaba_dashboard/input.rs"]
mod input;
#[path = "alibaba_dashboard/planning.rs"]
mod planning;
#[path = "alibaba_dashboard/summaries.rs"]
mod summaries;
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::{fs::File, path::PathBuf, time::Instant};
use summaries::*;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    directory: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    stage: String,
    #[arg(long, default_value = "service")]
    workload: String,
    #[arg(long, default_value = "erp")]
    method: String,
    #[arg(long)]
    calibration: Option<PathBuf>,
    #[arg(long)]
    catalog: Option<PathBuf>,
    #[arg(long)]
    deployment: Option<PathBuf>,
    #[arg(long)]
    oracle_directory: Option<PathBuf>,
    #[arg(long)]
    write_oracle: bool,
    #[arg(long, default_value_t = 120)]
    calibration_files: usize,
    #[arg(long, default_value_t = 240)]
    total_files: usize,
    #[arg(long, default_value_t = 10_000_000)]
    calibration_events: usize,
    #[arg(long, default_value_t = 3)]
    profile_trials: usize,
    #[arg(long, default_value_t = 16_000_000_000u64)]
    memory_budget_bytes: u64,
    #[arg(long, default_value_t = 42)]
    seed: u64,
}
#[derive(Serialize, Deserialize)]
struct Calibration {
    panes: Vec<Vec<input::Event>>,
    source_events: u64,
    preparation_seconds: f64,
    seed: u64,
}

fn filter_calibration(data: &mut Calibration, workload: Workload) {
    for pane in &mut data.panes {
        pane.retain(|e| match workload {
            Workload::Service => true,
            Workload::Edge => e.upstream != u32::MAX,
            Workload::Latency => e.latency.is_finite() && e.latency >= 0.,
        });
    }
}

fn analytical(w: Workload) -> planning::Deployment {
    use asap_aware_mapping::replacement::default_size_params;
    use planner_types::{
        post_asap::{SketchAlgorithm, SketchParams},
        pre_asap::AggIntent,
        types::AccuracyTarget,
    };
    let t = Instant::now();
    let accuracy = AccuracyTarget::EpsilonDelta {
        epsilon: 0.01,
        delta: 0.01,
    };
    let config = if w == Workload::Latency {
        let intent = AggIntent::Quantile {
            col: None,
            q: 0.99,
            accuracy,
        };
        let SketchParams::Kll { k } =
            default_size_params(SketchAlgorithm::Kll, &intent, 0.01, 0.01)
        else {
            unreachable!()
        };
        Config::Kll {
            capacity: [128, 256, 512, 1024, 2048]
                .into_iter()
                .find(|n| *n >= k as usize)
                .expect("analytical KLL exceeds grid"),
        }
    } else {
        let intent = AggIntent::TopK { k: 3, accuracy };
        let SketchParams::CmsWithHeap {
            width,
            depth,
            heap_size,
        } = default_size_params(SketchAlgorithm::CmsWithHeap, &intent, 0.01, 0.01)
        else {
            unreachable!()
        };
        Config::Cms {
            width: (width as usize).next_power_of_two(),
            depth: depth as usize,
            heap: (heap_size as usize).max(16).next_power_of_two(),
        }
    };
    planning::Deployment {
        configs: vec![config],
        shared: true,
        planning_seconds: t.elapsed().as_secs_f64(),
        searches: vec![],
        reason: "ASAPPlanner analytical sizing; native bounds do not certify final query loss"
            .into(),
    }
}

fn write_json(path: &PathBuf, value: &impl Serialize) -> anyhow::Result<()> {
    let out = File::options().write(true).create_new(true).open(path)?;
    serde_json::to_writer_pretty(out, value)?;
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let a = Args::parse();
    anyhow::ensure!(
        a.calibration_files >= 20 && a.total_files > a.calibration_files && a.profile_trials > 0,
        "invalid geometry/trials"
    );
    let workload = match a.workload.as_str() {
        "service" => Workload::Service,
        "edge" => Workload::Edge,
        "latency" => Workload::Latency,
        _ => anyhow::bail!("unknown workload"),
    };
    if a.stage == "calibration" {
        let t = Instant::now();
        let (panes, source_events) = input::calibration(
            &a.directory,
            a.calibration_files,
            a.calibration_events,
            a.seed,
        )?;
        let data = Calibration {
            panes,
            source_events,
            preparation_seconds: t.elapsed().as_secs_f64(),
            seed: a.seed,
        };
        let out = File::options()
            .write(true)
            .create_new(true)
            .open(&a.output)?;
        bincode::serialize_into(std::io::BufWriter::new(out), &data)?;
        write_json(
            &a.output.with_extension("metadata.json"),
            &serde_json::json!({"source_events":source_events,"sample_events":data.panes.iter().map(Vec::len).sum::<usize>(),"preparation_seconds":data.preparation_seconds,"seed":a.seed,"calibration_files":a.calibration_files,"held_out_used":false}),
        )?;
        return Ok(());
    }
    if a.stage == "profile" {
        let input = File::open(
            a.calibration
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("--calibration required"))?,
        )?;
        let mut data: Calibration = bincode::deserialize_from(std::io::BufReader::new(input))?;
        filter_calibration(&mut data, workload);
        let catalog = planning::build(&data.panes, workload, a.seed, a.profile_trials)?;
        return write_json(&a.output, &catalog);
    }
    if a.stage == "plan" {
        let t = Instant::now();
        let mut deployment=match a.method.as_str(){
            "auto"=>{let file=File::open(a.calibration.as_ref().ok_or_else(||anyhow::anyhow!("--calibration required"))?)?;let mut data:Calibration=bincode::deserialize_from(std::io::BufReader::new(file))?;filter_calibration(&mut data,workload);planning::autosketch(&data.panes,workload,a.seed,a.memory_budget_bytes as usize)?},
            "erp"|"erp-no-sharing"=>{let file=File::open(a.catalog.as_ref().ok_or_else(||anyhow::anyhow!("--catalog required"))?)?;let catalog:planning::Catalog=serde_json::from_reader(file)?;anyhow::ensure!(catalog.workload==workload,"catalog workload mismatch");
                let local=planning::erp(&catalog,false,a.memory_budget_bytes as usize);
                if a.method=="erp-no-sharing"{local?}else{
                    let shared=planning::erp(&catalog,true,a.memory_budget_bytes as usize);
                    let predicted=|d:&planning::Deployment|d.searches.iter().filter_map(|s|s["predicted_retained_bytes"].as_f64()).sum::<f64>();
                    match(shared,local){(Ok(s),Ok(l))=>if predicted(&s)<=predicted(&l){s}else{l},(Ok(s),_)=>s,(_,Ok(l))=>l,(Err(e),_)=>return Err(e)}
                }
            },
            "analytical"=>analytical(workload),
            "exact-scan"|"exact-pane"=>planning::Deployment{configs:vec![Config::Exact],shared:true,planning_seconds:0.,searches:vec![],reason:a.method.clone()},
            "erp-existing"=>planning::Deployment{configs:vec![Config::Exact],shared:true,planning_seconds:0.,searches:vec![],reason:"explicit ERP miss: existing v2 catalog has incompatible windows/TopK/error contract; exact fallback, not a successful shape match".into()},
            _=>anyhow::bail!("unknown method")
        };
        deployment.planning_seconds = t.elapsed().as_secs_f64();
        return write_json(&a.output, &deployment);
    }
    if a.stage == "run" {
        let deployment: planning::Deployment = serde_json::from_reader(File::open(
            a.deployment
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("--deployment required"))?,
        )?)?;
        let t = Instant::now();
        match execution::execute(
            &a.directory,
            workload,
            deployment,
            a.calibration_files,
            a.total_files,
            a.memory_budget_bytes as usize,
            a.method == "exact-scan",
            a.oracle_directory.as_deref(),
            a.write_oracle,
        ) {
            Ok(result) => write_json(&a.output, &result),
            Err(error) => {
                write_json(
                    &a.output,
                    &serde_json::json!({"status":"failed","method":a.method,"workload":workload,"elapsed_seconds":t.elapsed().as_secs_f64(),"error":error.to_string()}),
                )?;
                Err(error)
            }
        }
    } else {
        anyhow::bail!("unknown stage")
    }
}
