//! Repeated-window execution comparison using the real ASAP CMS implementation.
use asap_aware_mapping::replacement::default_size_params;
use asap_sketchlib::CountMinSketch;
use clap::Parser;
use planner_types::{
    post_asap::{SketchAlgorithm, SketchParams},
    pre_asap::AggIntent,
    types::AccuracyTarget,
};
use serde::Serialize;
use std::{collections::VecDeque, hint::black_box, time::Instant};

#[derive(Clone, Debug, Parser, Serialize)]
struct Args {
    #[arg(long)]
    output: std::path::PathBuf,
    #[arg(long)]
    backend_revision: String,
    /// Optional `pane_index<TAB>dense_key_index` replay file.
    #[arg(long)]
    input_tsv: Option<std::path::PathBuf>,
    #[arg(long, default_value_t = 120)]
    panes: usize,
    #[arg(long, default_value_t = 2000)]
    events_per_pane: usize,
    #[arg(long, default_value_t = 1000)]
    cardinality: usize,
    /// Zero is uniform; positive values use a truncated Zipf distribution.
    #[arg(long, default_value_t = 0.0)]
    zipf: f64,
    #[arg(long, default_value_t = 256)]
    width: usize,
    #[arg(long, default_value_t = 4)]
    depth: usize,
    /// Accuracy target passed to ASAPPlanner's analytical sizing baseline.
    #[arg(long, default_value_t = 0.01)]
    epsilon: f64,
    #[arg(long, default_value_t = 0.01)]
    delta: f64,
    /// Common candidate-space payload limit per sketch.
    #[arg(long, default_value_t = 32768)]
    memory_budget_bytes: usize,
    #[arg(long, default_value_t = 5)]
    trials: usize,
    /// Repeat all four end-of-stream queries to model recurring executions.
    #[arg(long, default_value_t = 10)]
    query_repetitions: usize,
    #[arg(long, default_value_t = 42)]
    seed: u64,
    #[arg(long, value_delimiter = ',', default_value = "1,5,15,60")]
    window_panes: Vec<usize>,
    #[arg(long, default_value_t = 60)]
    pane_seconds: usize,
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut x = self.0;
        x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
        x ^ (x >> 31)
    }
}

fn workload(args: &Args, trial: usize) -> Vec<Vec<usize>> {
    let mut rng = Rng(args.seed.wrapping_add(trial as u64));
    let mut cumulative = Vec::with_capacity(args.cardinality);
    let mut total = 0.0;
    for rank in 1..=args.cardinality {
        total += (rank as f64).powf(-args.zipf);
        cumulative.push(total);
    }
    (0..args.panes)
        .map(|_| {
            (0..args.events_per_pane)
                .map(|_| {
                    let sample = (rng.next() >> 11) as f64 / ((1_u64 << 53) as f64) * total;
                    cumulative
                        .partition_point(|value| *value <= sample)
                        .min(args.cardinality - 1)
                })
                .collect()
        })
        .collect()
}

fn replay(path: &std::path::Path) -> Result<(Vec<Vec<usize>>, usize), Box<dyn std::error::Error>> {
    let contents = std::fs::read_to_string(path)?;
    let mut rows = Vec::new();
    let mut max_pane = 0;
    let mut max_key = 0;
    for (line_number, line) in contents.lines().enumerate() {
        let (pane, key) = line
            .split_once('\t')
            .ok_or_else(|| format!("invalid TSV at line {}", line_number + 1))?;
        let pane: usize = pane.parse()?;
        let key: usize = key.parse()?;
        max_pane = max_pane.max(pane);
        max_key = max_key.max(key);
        rows.push((pane, key));
    }
    if rows.is_empty() {
        return Err("empty replay input".into());
    }
    let mut panes = vec![Vec::new(); max_pane + 1];
    for (pane, key) in rows {
        panes[pane].push(key);
    }
    Ok((panes, max_key + 1))
}

#[derive(Debug, Serialize)]
struct ResultRow {
    method: &'static str,
    update_wall_seconds: f64,
    query_wall_seconds: f64,
    maintenance_updates: usize,
    retained_sketches: usize,
    logical_payload_bytes: usize,
    max_normalized_additive_error: f64,
}

fn truth(data: &[Vec<usize>], window: usize, cardinality: usize) -> Vec<u64> {
    let mut counts = vec![0; cardinality];
    for pane in data.iter().rev().take(window) {
        for &key in pane {
            counts[key] += 1;
        }
    }
    counts
}

fn evaluate(
    method: &'static str,
    states: &[VecDeque<CountMinSketch>],
    data: &[Vec<usize>],
    args: &Args,
    update_wall_seconds: f64,
    maintenance_updates: usize,
) -> ResultRow {
    let started = Instant::now();
    let mut max_error = 0.0_f64;
    for _ in 0..args.query_repetitions {
        for (query, &window) in args.window_panes.iter().enumerate() {
            let exact = truth(data, window, args.cardinality);
            let denominator = exact.iter().sum::<u64>().max(1) as f64;
            for (key, exact) in exact.into_iter().enumerate() {
                let name = format!("key-{key}");
                let estimate: f64 = states[query].iter().map(|s| s.estimate(&name)).sum();
                max_error = max_error.max((estimate - exact as f64).abs() / denominator);
                black_box(estimate);
            }
        }
    }
    let retained_sketches = states.iter().map(VecDeque::len).sum();
    ResultRow {
        method,
        update_wall_seconds,
        query_wall_seconds: started.elapsed().as_secs_f64(),
        maintenance_updates,
        retained_sketches,
        logical_payload_bytes: retained_sketches * args.width * args.depth * 8,
        max_normalized_additive_error: max_error,
    }
}

fn per_query(method: &'static str, data: &[Vec<usize>], args: &Args) -> ResultRow {
    let mut states: Vec<VecDeque<CountMinSketch>> =
        args.window_panes.iter().map(|_| VecDeque::new()).collect();
    let started = Instant::now();
    for pane in data {
        for (query, &retention) in args.window_panes.iter().enumerate() {
            let mut sketch = CountMinSketch::new(args.depth, args.width);
            for &key in pane {
                sketch.update(&format!("key-{key}"), 1.0);
            }
            states[query].push_back(sketch);
            if states[query].len() > retention {
                states[query].pop_front();
            }
        }
    }
    evaluate(
        method,
        &states,
        data,
        args,
        started.elapsed().as_secs_f64(),
        data.iter().map(Vec::len).sum::<usize>() * args.window_panes.len(),
    )
}

fn shared(method: &'static str, data: &[Vec<usize>], args: &Args) -> ResultRow {
    let mut panes = VecDeque::new();
    let started = Instant::now();
    for pane in data {
        let mut sketch = CountMinSketch::new(args.depth, args.width);
        for &key in pane {
            sketch.update(&format!("key-{key}"), 1.0);
        }
        panes.push_back(sketch);
        if panes.len() > *args.window_panes.last().unwrap() {
            panes.pop_front();
        }
    }
    let update = started.elapsed().as_secs_f64();
    let states: Vec<VecDeque<CountMinSketch>> = args
        .window_panes
        .iter()
        .map(|&window| panes.iter().rev().take(window).rev().cloned().collect())
        .collect();
    // Report physical shared storage, not the temporary query views above.
    let mut row = evaluate(
        method,
        &states,
        data,
        args,
        update,
        data.iter().map(Vec::len).sum(),
    );
    row.retained_sketches = panes.len();
    row.logical_payload_bytes = panes.len() * args.width * args.depth * 8;
    row
}

fn analytical_args(args: &Args) -> Result<Args, Box<dyn std::error::Error>> {
    let intent = AggIntent::Count {
        accuracy: AccuracyTarget::EpsilonDelta {
            epsilon: args.epsilon,
            delta: args.delta,
        },
    };
    let SketchParams::Cms { width, depth } =
        default_size_params(SketchAlgorithm::Cms, &intent, args.epsilon, args.delta)
    else {
        unreachable!("CMS analytical sizing must return CMS parameters")
    };
    // The comparison uses the same power-of-two width/depth grid as the
    // AutoSketch adaptation. Round theory upward so its guarantee is not weakened.
    let mut analytical = args.clone();
    analytical.width = usize::try_from(width)?.max(64).next_power_of_two();
    analytical.depth = usize::try_from(depth)?;
    if analytical.width * analytical.depth * 8 > args.memory_budget_bytes {
        return Err("ASAPPlanner analytical CMS exceeds the common memory budget".into());
    }
    Ok(analytical)
}

fn exact(data: &[Vec<usize>], args: &Args) -> ResultRow {
    let started = Instant::now();
    let retained: Vec<_> = data
        .iter()
        .rev()
        .take(*args.window_panes.last().unwrap())
        .rev()
        .flatten()
        .copied()
        .collect();
    let update = started.elapsed().as_secs_f64();
    let started = Instant::now();
    for _ in 0..args.query_repetitions {
        for &window in &args.window_panes {
            black_box(truth(data, window, args.cardinality));
        }
    }
    ResultRow {
        method: "exact_raw",
        update_wall_seconds: update,
        query_wall_seconds: started.elapsed().as_secs_f64(),
        maintenance_updates: data.iter().map(Vec::len).sum(),
        retained_sketches: 0,
        logical_payload_bytes: retained.len() * std::mem::size_of::<usize>(),
        max_normalized_additive_error: 0.0,
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = Args::parse();
    let replayed = match &args.input_tsv {
        Some(path) => {
            let (data, cardinality) = replay(path)?;
            args.panes = data.len();
            args.cardinality = cardinality;
            Some(data)
        }
        None => None,
    };
    if args.window_panes.is_empty()
        || args.window_panes.iter().any(|window| *window == 0)
        || !args.window_panes.windows(2).all(|pair| pair[0] < pair[1])
        || args.panes < *args.window_panes.last().unwrap()
        || (args.input_tsv.is_none() && (args.events_per_pane == 0 || args.cardinality == 0))
        || args.width == 0
        || args.depth == 0
        || !args.zipf.is_finite()
        || args.zipf < 0.0
        || !args.epsilon.is_finite()
        || args.epsilon <= 0.0
        || !args.delta.is_finite()
        || args.delta <= 0.0
        || args.delta >= 1.0
        || args.width * args.depth * 8 > args.memory_budget_bytes
        || args.trials == 0
        || args.query_repetitions == 0
        || args.backend_revision.trim().is_empty()
    {
        return Err("invalid arguments (panes must cover increasing non-zero windows)".into());
    }
    let output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&args.output)?;
    let mut trials = Vec::new();
    for trial in 0..args.trials {
        let data = match &replayed {
            Some(data) => data.clone(),
            None => workload(&args, trial),
        };
        if data.len() < *args.window_panes.last().unwrap() {
            return Err("replay does not cover the largest window".into());
        }
        let analytical = analytical_args(&args)?;
        let rows = vec![
            per_query("autosketch_per_query", &data, &args),
            per_query("asap_no_sharing", &data, &args),
            shared("asapplanner_erp", &data, &args),
            shared("asapplanner_analytical", &data, &analytical),
            exact(&data, &args),
        ];
        eprintln!("trial {trial} complete");
        trials.push(serde_json::json!({"trial":trial,"methods":rows}));
    }
    serde_json::to_writer_pretty(
        output,
        &serde_json::json!({
            "schema_version":1,"windows_minutes":args.window_panes.iter().map(|window| window * args.pane_seconds / 60).collect::<Vec<_>>(),
            "pane_seconds":args.pane_seconds,"args":args,
            "timing_metric":"wall seconds; methods execute sequentially and are not CPU profiles",
            "memory_metric":"logical retained payload; CMS counters or raw usize keys; excludes allocator overhead",
            "adaptation":"AutoSketch is deployed independently per recurring query; width/depth are selected by the companion Rust AutoSketch/ERP calibration runner; ASAP-NoSharing isolates sharing; ASAPPlanner-ERP shares panes; ASAPPlanner-Analytical calls Planner default_size_params and rounds upward to the common candidate grid",
            "trials":trials
        }),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn args() -> Args {
        Args {
            output: "unused".into(),
            backend_revision: "test".into(),
            input_tsv: None,
            panes: 60,
            events_per_pane: 10,
            cardinality: 20,
            zipf: 0.0,
            width: 64,
            depth: 2,
            epsilon: 0.01,
            delta: 0.01,
            memory_budget_bytes: 32768,
            trials: 1,
            query_repetitions: 2,
            seed: 42,
            window_panes: vec![1, 5, 15, 60],
            pane_seconds: 60,
        }
    }
    #[test]
    fn sharing_reduces_updates_and_storage_without_changing_answers() {
        let args = args();
        let data = workload(&args, 0);
        let per = per_query("autosketch_per_query", &data, &args);
        let full = shared("asapplanner_erp", &data, &args);
        assert_eq!(per.maintenance_updates, full.maintenance_updates * 4);
        assert_eq!(per.retained_sketches, 81);
        assert_eq!(full.retained_sketches, 60);
        assert_eq!(
            per.max_normalized_additive_error,
            full.max_normalized_additive_error
        );
    }

    #[test]
    fn replay_preserves_empty_panes_and_derives_cardinality() {
        let path = std::env::temp_dir().join(format!(
            "asap-replay-{}-{}.tsv",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        std::fs::write(&path, "0\t2\n2\t0\n2\t1\n").unwrap();
        let (panes, cardinality) = replay(&path).unwrap();
        std::fs::remove_file(path).unwrap();
        assert_eq!(panes, vec![vec![2], vec![], vec![0, 1]]);
        assert_eq!(cardinality, 3);
    }
}
