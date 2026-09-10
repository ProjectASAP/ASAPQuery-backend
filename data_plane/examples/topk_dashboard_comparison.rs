//! Recurring Top-K dashboard comparison for AutoSketch and ASAPPlanner.
use asap_aware_mapping::replacement::default_size_params;
use asap_sketchlib::{CountMinSketchWithHeap, CountSketchWithHeap};
use clap::{Parser, ValueEnum};
use planner_types::{
    post_asap::{SketchAlgorithm, SketchParams},
    pre_asap::AggIntent,
    types::AccuracyTarget,
};
use serde::Serialize;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    hint::black_box,
    time::{Duration, Instant},
};

const WINDOWS: [usize; 4] = [2, 10, 30, 120];
const K: usize = 10;

#[derive(Clone, Copy, Debug, Serialize, ValueEnum)]
enum Distribution {
    Uniform,
    Zipf,
}

#[derive(Clone, Debug, Parser, Serialize)]
struct Args {
    #[arg(long)]
    output: std::path::PathBuf,
    #[arg(long)]
    backend_revision: String,
    #[arg(long)]
    input_tsv: Option<std::path::PathBuf>,
    #[arg(long, default_value_t = 10_000_000)]
    total_events: usize,
    #[arg(long, default_value_t = 100_000)]
    cardinality: usize,
    #[arg(long, default_value_t = 220)]
    panes: usize,
    #[arg(long, default_value_t = 120)]
    calibration_panes: usize,
    #[arg(long, default_value_t = 100)]
    refreshes: usize,
    #[arg(long, default_value_t = 30)]
    refresh_seconds: usize,
    #[arg(long, value_enum, default_value_t = Distribution::Zipf)]
    distribution: Distribution,
    #[arg(long, default_value_t = 1.1)]
    zipf_exponent: f64,
    #[arg(long, default_value_t = 131_072)]
    memory_budget_bytes: usize,
    #[arg(long, default_value_t = 0.8)]
    min_recall_at_10: f64,
    #[arg(long, default_value_t = 3)]
    trials: usize,
    #[arg(long, default_value_t = 42)]
    seed: u64,
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
enum Family {
    Cms,
    CountSketch,
}

#[derive(Clone, Copy, Debug, Serialize)]
struct Config {
    family: Family,
    rows: usize,
    cols: usize,
    heap: usize,
}

#[derive(Clone)]
enum Sketch {
    Cms(CountMinSketchWithHeap),
    Cs(CountSketchWithHeap),
}

impl Sketch {
    fn new(c: Config) -> Self {
        match c.family {
            Family::Cms => Self::Cms(CountMinSketchWithHeap::new(c.rows, c.cols, c.heap)),
            Family::CountSketch => Self::Cs(CountSketchWithHeap::new(c.rows, c.cols, c.heap)),
        }
    }
    fn update(&mut self, key: &str, count: f64) {
        match self {
            Self::Cms(s) => s.update(key, count),
            Self::Cs(s) => s.update(key, count),
        }
    }
    fn merge(&mut self, other: &Self) -> Result<(), Box<dyn std::error::Error>> {
        match (self, other) {
            (Self::Cms(a), Self::Cms(b)) => a.merge(b).map_err(|e| e.to_string().into()),
            (Self::Cs(a), Self::Cs(b)) => a.merge(b).map_err(|e| e.to_string().into()),
            _ => Err("cannot merge different sketch families".into()),
        }
    }
    fn topk(&self) -> Vec<(String, f64)> {
        let mut items: Vec<(String, f64)> = match self {
            Self::Cms(s) => s
                .topk_heap_items()
                .into_iter()
                .map(|x| (x.key, x.value))
                .collect(),
            Self::Cs(s) => s
                .topk_heap_items()
                .into_iter()
                .map(|x| (x.key, x.value))
                .collect(),
        };
        items.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        items.truncate(K);
        items
    }
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

fn synthetic(a: &Args, trial: usize) -> Vec<Vec<u32>> {
    let mut rng = Rng(a.seed + trial as u64);
    let mut cumulative = Vec::with_capacity(a.cardinality);
    let mut sum = 0.0;
    for rank in 1..=a.cardinality {
        sum += match a.distribution {
            Distribution::Uniform => 1.0,
            Distribution::Zipf => (rank as f64).powf(-a.zipf_exponent),
        };
        cumulative.push(sum);
    }
    (0..a.panes)
        .map(|p| {
            let n = a.total_events / a.panes + usize::from(p < a.total_events % a.panes);
            (0..n)
                .map(|_| {
                    let v = (rng.next() >> 11) as f64 / (1u64 << 53) as f64 * sum;
                    cumulative
                        .partition_point(|x| *x <= v)
                        .min(a.cardinality - 1) as u32
                })
                .collect()
        })
        .collect()
}

fn replay(path: &std::path::Path) -> Result<Vec<Vec<u32>>, Box<dyn std::error::Error>> {
    let mut rows = Vec::new();
    let mut max = 0;
    for (line_no, line) in std::fs::read_to_string(path)?.lines().enumerate() {
        let (p, k) = line
            .split_once('\t')
            .ok_or_else(|| format!("invalid TSV line {}", line_no + 1))?;
        let p: usize = p.parse()?;
        let k: u32 = k.parse()?;
        max = max.max(p);
        rows.push((p, k));
    }
    if rows.is_empty() {
        return Err("empty replay".into());
    }
    let mut panes = vec![Vec::new(); max + 1];
    for (p, k) in rows {
        panes[p].push(k);
    }
    Ok(panes)
}

fn candidates(a: &Args) -> Vec<Config> {
    [Family::Cms, Family::CountSketch]
        .into_iter()
        .flat_map(|family| {
            [3, 5, 7].into_iter().flat_map(move |rows| {
                [128, 256, 512, 1024].into_iter().flat_map(move |cols| {
                    [16, 32, 64, 128, 256, 512, 1024]
                        .into_iter()
                        .map(move |heap| Config {
                            family,
                            rows,
                            cols,
                            heap,
                        })
                })
            })
        })
        .filter(|c| c.rows * c.cols * 8 + c.heap * 32 <= a.memory_budget_bytes)
        .collect()
}

fn pane_counts(pane: &[u32]) -> HashMap<u32, u32> {
    let mut h = HashMap::new();
    for &k in pane {
        *h.entry(k).or_insert(0) += 1;
    }
    h
}
fn build_pane(pane: &[u32], c: Config) -> Sketch {
    let mut s = Sketch::new(c);
    for (k, n) in pane_counts(pane) {
        s.update(&format!("key-{k}"), n as f64);
    }
    s
}

fn exact_topk(data: &[Vec<u32>], end: usize, window: usize) -> Vec<(u32, u64)> {
    let mut h = HashMap::new();
    for pane in &data[end - window..end] {
        for &k in pane {
            *h.entry(k).or_insert(0) += 1;
        }
    }
    let mut v: Vec<_> = h.into_iter().collect();
    v.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    if v.len() > K {
        let boundary = v[K - 1].1;
        v.retain(|(_, count)| *count >= boundary);
    }
    v
}
fn recall(pred: &[(String, f64)], truth: &[(u32, u64)]) -> f64 {
    let p: HashSet<_> = pred
        .iter()
        .filter_map(|(x, _)| x.strip_prefix("key-")?.parse::<u32>().ok())
        .collect();
    if truth.is_empty() {
        1.0
    } else {
        truth.iter().filter(|(k, _)| p.contains(k)).count() as f64 / truth.len().min(K) as f64
    }
}

fn precision(pred: &[(String, f64)], truth: &[(u32, u64)]) -> f64 {
    if pred.is_empty() {
        return f64::from(truth.is_empty());
    }
    let truth: HashSet<_> = truth.iter().map(|(key, _)| *key).collect();
    pred.iter()
        .filter_map(|(key, _)| key.strip_prefix("key-")?.parse::<u32>().ok())
        .filter(|key| truth.contains(key))
        .count() as f64
        / pred.len() as f64
}
fn ndcg(pred: &[(String, f64)], truth: &[(u32, u64)]) -> f64 {
    let rel: HashMap<_, _> = truth.iter().copied().collect();
    let dcg: f64 = pred
        .iter()
        .enumerate()
        .map(|(i, (s, _))| {
            let k = s.strip_prefix("key-").and_then(|x| x.parse::<u32>().ok());
            k.and_then(|x| rel.get(&x).copied()).unwrap_or(0) as f64 / ((i + 2) as f64).log2()
        })
        .sum();
    let ideal: f64 = truth
        .iter()
        .take(K)
        .enumerate()
        .map(|(i, (_, v))| *v as f64 / ((i + 2) as f64).log2())
        .sum();
    if ideal == 0.0 {
        1.0
    } else {
        dcg / ideal
    }
}

fn calibration_score(
    data: &[Vec<u32>],
    window: usize,
    c: Config,
) -> Result<f64, Box<dyn std::error::Error>> {
    let end = data.len();
    let start = end.saturating_sub(window);
    let states: Vec<_> = data[start..end].iter().map(|p| build_pane(p, c)).collect();
    let mut merged = states[0].clone();
    for s in &states[1..] {
        merged.merge(s)?;
    }
    Ok(recall(&merged.topk(), &exact_topk(data, end, end - start)))
}

fn autosketch(
    data: &[Vec<u32>],
    window: usize,
    a: &Args,
    offset: usize,
) -> Result<(Config, Duration), Box<dyn std::error::Error>> {
    let grid = candidates(a);
    let began = Instant::now();
    let mut frontier = VecDeque::new();
    for i in 0..8 {
        frontier.push_back((offset + i * 7) % grid.len());
    }
    let mut seen = HashSet::new();
    let mut best = None;
    while let Some(i) = frontier.pop_front() {
        if seen.len() >= 16 {
            break;
        }
        if !seen.insert(i) {
            continue;
        }
        let c = grid[i];
        let score = calibration_score(data, window, c)?;
        let feasible = score >= a.min_recall_at_10;
        if feasible
            && best.is_none_or(|b: Config| {
                (c.rows * c.cols * 8 + c.heap * 32) < (b.rows * b.cols * 8 + b.heap * 32)
            })
        {
            best = Some(c)
        }
        let next = if feasible {
            i.checked_sub(1)
        } else {
            (i + 1 < grid.len()).then_some(i + 1)
        };
        if let Some(j) = next {
            frontier.push_back(j)
        }
    }
    let mut best_observed = 0.0_f64;
    if best.is_none() {
        let mut by_resource = grid.clone();
        by_resource.sort_by_key(|c| c.rows * c.cols * 8 + c.heap * 32);
        for c in by_resource.into_iter().rev() {
            let score = calibration_score(data, window, c)?;
            best_observed = best_observed.max(score);
            if score >= a.min_recall_at_10 {
                best = Some(c);
                break;
            }
        }
    }
    Ok((
        best.ok_or_else(|| {
            format!(
                "AutoSketch found no feasible config for {window} panes; best recall={best_observed}"
            )
        })?,
        began.elapsed(),
    ))
}

#[derive(Default, Serialize)]
struct Timing {
    update_seconds: f64,
    eviction_seconds: f64,
    merge_seconds: f64,
    topk_readout_seconds: f64,
    exact_query_seconds: f64,
}
#[derive(Serialize)]
struct Row {
    method: String,
    planning_seconds: f64,
    timing: Timing,
    logical_payload_bytes: usize,
    maintenance_updates: u64,
    mean_recall_at_10: f64,
    mean_precision_at_10: f64,
    mean_ndcg_at_10: f64,
    accuracy_violations: usize,
    configs: Vec<Config>,
}

fn run_sketch(
    method: &str,
    data: &[Vec<u32>],
    start: usize,
    refreshes: usize,
    configs: Vec<Config>,
    shared: bool,
    planning: f64,
    min_recall: f64,
) -> Result<Row, Box<dyn std::error::Error>> {
    let mut timing = Timing::default();
    let mut recall_sum = 0.0;
    let mut precision_sum = 0.0;
    let mut ndcg_sum = 0.0;
    let mut violations = 0;
    let mut updates = 0u64;
    let mut stores: Vec<VecDeque<Sketch>> = (0..if shared { 1 } else { 4 })
        .map(|_| VecDeque::new())
        .collect();
    for p in 0..start + refreshes {
        let targets = if shared { 1 } else { 4 };
        for q in 0..targets {
            let c = if shared { configs[0] } else { configs[q] };
            let t = Instant::now();
            let s = build_pane(&data[p], c);
            timing.update_seconds += t.elapsed().as_secs_f64();
            updates += data[p].len() as u64;
            stores[q].push_back(s);
            let keep = if shared { WINDOWS[3] } else { WINDOWS[q] };
            let t = Instant::now();
            while stores[q].len() > keep {
                stores[q].pop_front();
            }
            timing.eviction_seconds += t.elapsed().as_secs_f64();
        }
        if p + 1 < start || p + 1 >= start + refreshes {
            continue;
        }
        for (q, &window) in WINDOWS.iter().enumerate() {
            let store = &stores[if shared { 0 } else { q }];
            let refs: Vec<_> = store.iter().rev().take(window).collect();
            let t = Instant::now();
            let mut merged = refs[0].clone();
            for s in refs.iter().skip(1) {
                merged.merge(s)?;
            }
            timing.merge_seconds += t.elapsed().as_secs_f64();
            let t = Instant::now();
            let pred = black_box(merged.topk());
            timing.topk_readout_seconds += t.elapsed().as_secs_f64();
            let t = Instant::now();
            let truth = exact_topk(data, p + 1, window);
            timing.exact_query_seconds += t.elapsed().as_secs_f64();
            let r = recall(&pred, &truth);
            recall_sum += r;
            precision_sum += precision(&pred, &truth);
            ndcg_sum += ndcg(&pred, &truth);
            if r < min_recall {
                violations += 1;
            }
        }
    }
    let queries = refreshes * 4;
    let bytes = stores
        .iter()
        .enumerate()
        .map(|(i, s)| {
            let c = if shared { configs[0] } else { configs[i] };
            s.len() * (c.rows * c.cols * 8 + c.heap * 32)
        })
        .sum();
    Ok(Row {
        method: method.into(),
        planning_seconds: planning,
        timing,
        logical_payload_bytes: bytes,
        maintenance_updates: updates,
        mean_recall_at_10: recall_sum / queries as f64,
        mean_precision_at_10: precision_sum / queries as f64,
        mean_ndcg_at_10: ndcg_sum / queries as f64,
        accuracy_violations: violations,
        configs,
    })
}

fn exact(data: &[Vec<u32>], start: usize, refreshes: usize) -> Row {
    let plan = Instant::now();
    let _layout = WINDOWS;
    let planning = plan.elapsed().as_secs_f64();
    let mut t = Timing::default();
    for p in start..start + refreshes {
        for &w in &WINDOWS {
            let q = Instant::now();
            black_box(exact_topk(data, p + 1, w));
            t.exact_query_seconds += q.elapsed().as_secs_f64();
        }
    }
    Row {
        method: "exact_production".into(),
        planning_seconds: planning,
        timing: t,
        logical_payload_bytes: data[start - WINDOWS[3] + 1..start + refreshes]
            .iter()
            .map(|x| x.len() * 8)
            .sum(),
        maintenance_updates: data
            .iter()
            .take(start + refreshes)
            .map(|x| x.len() as u64)
            .sum(),
        mean_recall_at_10: 1.0,
        mean_precision_at_10: 1.0,
        mean_ndcg_at_10: 1.0,
        accuracy_violations: 0,
        configs: vec![],
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut a = Args::parse();
    let replayed = if let Some(p) = &a.input_tsv {
        Some(replay(p)?)
    } else {
        None
    };
    if let Some(d) = &replayed {
        a.panes = d.len();
        a.total_events = d.iter().map(Vec::len).sum();
        a.cardinality = d.iter().flatten().copied().collect::<HashSet<_>>().len();
    }
    if a.calibration_panes < WINDOWS[3]
        || a.calibration_panes + a.refreshes > a.panes
        || a.trials == 0
        || a.backend_revision.trim().is_empty()
    {
        return Err("invalid calibration/refresh geometry".into());
    }
    let mut trials = Vec::new();
    for trial in 0..a.trials {
        let data = replayed.clone().unwrap_or_else(|| synthetic(&a, trial));
        let cal = &data[..a.calibration_panes];
        let mut auto_configs = Vec::new();
        let mut auto_plan = 0.0;
        for (i, &w) in WINDOWS.iter().enumerate() {
            let (c, t) = autosketch(cal, w, &a, i + trial)?;
            auto_configs.push(c);
            auto_plan += t.as_secs_f64();
        }
        let erp_start = Instant::now();
        let erp_config = candidates(&a)
            .into_iter()
            .filter(|c| c.family == Family::Cms && c.rows >= 5 && c.cols >= 512 && c.heap >= 32)
            .min_by_key(|c| c.rows * c.cols * 8 + c.heap * 32)
            .ok_or("no ERP candidate")?;
        let erp_plan = erp_start.elapsed().as_secs_f64();
        let analytical_start = Instant::now();
        let intent = AggIntent::TopK {
            k: K,
            accuracy: AccuracyTarget::EpsilonDelta {
                epsilon: 0.01,
                delta: 0.01,
            },
        };
        let analytical =
            match default_size_params(SketchAlgorithm::CmsWithHeap, &intent, 0.01, 0.01) {
                SketchParams::CmsWithHeap {
                    width,
                    depth,
                    heap_size,
                } => Config {
                    family: Family::Cms,
                    rows: depth as usize,
                    cols: width as usize,
                    heap: heap_size.max(K as u32) as usize,
                },
                other => {
                    return Err(format!("unexpected analytical Top-K params: {other:?}").into())
                }
            };
        let analytical_plan = analytical_start.elapsed().as_secs_f64();
        let rows = vec![
            run_sketch(
                "autosketch_per_query",
                &data,
                a.calibration_panes,
                a.refreshes,
                auto_configs,
                false,
                auto_plan,
                a.min_recall_at_10,
            )?,
            run_sketch(
                "asapplanner_erp_no_sharing",
                &data,
                a.calibration_panes,
                a.refreshes,
                vec![erp_config; 4],
                false,
                erp_plan,
                a.min_recall_at_10,
            )?,
            run_sketch(
                "asapplanner_erp",
                &data,
                a.calibration_panes,
                a.refreshes,
                vec![erp_config],
                true,
                erp_plan,
                a.min_recall_at_10,
            )?,
            run_sketch(
                "asapplanner_analytical",
                &data,
                a.calibration_panes,
                a.refreshes,
                vec![analytical],
                true,
                analytical_plan,
                a.min_recall_at_10,
            )?,
            exact(&data, a.calibration_panes, a.refreshes),
        ];
        trials.push(serde_json::json!({"trial":trial,"rows":rows}));
        eprintln!("trial {trial} complete");
    }
    let out = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&a.output)?;
    serde_json::to_writer_pretty(
        out,
        &serde_json::json!({"schema_version":1,"query":"topk(10, count_over_time(events[window]))","windows_minutes":[1,5,15,60],"dashboard_refresh_seconds":a.refresh_seconds,"args":a,"trials":trials}),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dashboard_advances_and_sharing_reduces_updates() {
        let a = Args {
            output: "x".into(),
            backend_revision: "test".into(),
            input_tsv: None,
            total_events: 22000,
            cardinality: 100,
            panes: 220,
            calibration_panes: 120,
            refreshes: 3,
            refresh_seconds: 30,
            distribution: Distribution::Zipf,
            zipf_exponent: 1.1,
            memory_budget_bytes: 32768,
            min_recall_at_10: 0.5,
            trials: 1,
            seed: 42,
        };
        let d = synthetic(&a, 0);
        let c = Config {
            family: Family::Cms,
            rows: 3,
            cols: 128,
            heap: 16,
        };
        let shared = run_sketch("shared", &d, 120, 3, vec![c], true, 0.0, 0.5).unwrap();
        let local = run_sketch("local", &d, 120, 3, vec![c; 4], false, 0.0, 0.5).unwrap();
        assert_eq!(local.maintenance_updates, shared.maintenance_updates * 4);
        assert!(shared.timing.merge_seconds > 0.0);
    }
}
