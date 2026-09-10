//! Repeated-window execution comparison using the real ASAP CMS implementation.
use asap_sketchlib::CountMinSketch;
use clap::Parser;
use serde::Serialize;
use std::{collections::VecDeque, hint::black_box, time::Instant};

#[derive(Debug, Parser, Serialize)]
struct Args {
    #[arg(long)]
    output: std::path::PathBuf,
    #[arg(long)]
    backend_revision: String,
    #[arg(long, default_value_t = 120)]
    panes: usize,
    #[arg(long, default_value_t = 2000)]
    events_per_pane: usize,
    #[arg(long, default_value_t = 1000)]
    cardinality: usize,
    #[arg(long, default_value_t = 256)]
    width: usize,
    #[arg(long, default_value_t = 4)]
    depth: usize,
    #[arg(long, default_value_t = 5)]
    trials: usize,
    /// Repeat all four end-of-stream queries to model recurring executions.
    #[arg(long, default_value_t = 10)]
    query_repetitions: usize,
    #[arg(long, default_value_t = 42)]
    seed: u64,
}

const WINDOWS: [usize; 4] = [1, 5, 15, 60];

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
    (0..args.panes)
        .map(|_| {
            (0..args.events_per_pane)
                .map(|_| rng.next() as usize % args.cardinality)
                .collect()
        })
        .collect()
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
        for (query, &window) in WINDOWS.iter().enumerate() {
            let exact = truth(data, window, args.cardinality);
            for (key, exact) in exact.into_iter().enumerate() {
                let name = format!("key-{key}");
                let estimate: f64 = states[query].iter().map(|s| s.estimate(&name)).sum();
                max_error = max_error
                    .max((estimate - exact as f64).abs() / (window * args.events_per_pane) as f64);
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
        WINDOWS.iter().map(|_| VecDeque::new()).collect();
    let started = Instant::now();
    for pane in data {
        for (query, &retention) in WINDOWS.iter().enumerate() {
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
        data.len() * args.events_per_pane * WINDOWS.len(),
    )
}

fn shared(data: &[Vec<usize>], args: &Args) -> ResultRow {
    let mut panes = VecDeque::new();
    let started = Instant::now();
    for pane in data {
        let mut sketch = CountMinSketch::new(args.depth, args.width);
        for &key in pane {
            sketch.update(&format!("key-{key}"), 1.0);
        }
        panes.push_back(sketch);
        if panes.len() > WINDOWS[WINDOWS.len() - 1] {
            panes.pop_front();
        }
    }
    let update = started.elapsed().as_secs_f64();
    let states: Vec<VecDeque<CountMinSketch>> = WINDOWS
        .iter()
        .map(|&window| panes.iter().rev().take(window).rev().cloned().collect())
        .collect();
    // Report physical shared storage, not the temporary query views above.
    let mut row = evaluate(
        "asap_full_shared",
        &states,
        data,
        args,
        update,
        data.len() * args.events_per_pane,
    );
    row.retained_sketches = panes.len();
    row.logical_payload_bytes = panes.len() * args.width * args.depth * 8;
    row
}

fn exact(data: &[Vec<usize>], args: &Args) -> ResultRow {
    let started = Instant::now();
    let retained: Vec<_> = data
        .iter()
        .rev()
        .take(60)
        .rev()
        .flatten()
        .copied()
        .collect();
    let update = started.elapsed().as_secs_f64();
    let started = Instant::now();
    for _ in 0..args.query_repetitions {
        for &window in &WINDOWS {
            black_box(truth(data, window, args.cardinality));
        }
    }
    ResultRow {
        method: "exact_raw",
        update_wall_seconds: update,
        query_wall_seconds: started.elapsed().as_secs_f64(),
        maintenance_updates: data.len() * args.events_per_pane,
        retained_sketches: 0,
        logical_payload_bytes: retained.len() * std::mem::size_of::<usize>(),
        max_normalized_additive_error: 0.0,
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if args.panes < 60
        || args.events_per_pane == 0
        || args.cardinality == 0
        || args.width == 0
        || args.depth == 0
        || args.trials == 0
        || args.query_repetitions == 0
        || args.backend_revision.trim().is_empty()
    {
        return Err("invalid arguments (panes must be at least 60)".into());
    }
    let output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&args.output)?;
    let mut trials = Vec::new();
    for trial in 0..args.trials {
        let data = workload(&args, trial);
        let rows = vec![
            per_query("autosketch_per_query", &data, &args),
            per_query("asap_no_sharing", &data, &args),
            shared(&data, &args),
            exact(&data, &args),
        ];
        eprintln!("trial {trial} complete");
        trials.push(serde_json::json!({"trial":trial,"methods":rows}));
    }
    serde_json::to_writer_pretty(
        output,
        &serde_json::json!({
            "schema_version":1,"args":args,"pane_seconds":60,"windows_minutes":WINDOWS,
            "timing_metric":"wall seconds; methods execute sequentially and are not CPU profiles",
            "memory_metric":"logical retained payload; CMS counters or raw usize keys; excludes allocator overhead",
            "adaptation":"AutoSketch is deployed independently per recurring query; ASAP-NoSharing intentionally has the same physical plan and isolates configuration; ASAP-Full shares one minute panes",
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
            panes: 60,
            events_per_pane: 10,
            cardinality: 20,
            width: 64,
            depth: 2,
            trials: 1,
            query_repetitions: 2,
            seed: 42,
        }
    }
    #[test]
    fn sharing_reduces_updates_and_storage_without_changing_answers() {
        let args = args();
        let data = workload(&args, 0);
        let per = per_query("autosketch_per_query", &data, &args);
        let full = shared(&data, &args);
        assert_eq!(per.maintenance_updates, full.maintenance_updates * 4);
        assert_eq!(per.retained_sketches, 81);
        assert_eq!(full.retained_sketches, 60);
        assert_eq!(
            per.max_normalized_additive_error,
            full.max_normalized_additive_error
        );
    }
}
