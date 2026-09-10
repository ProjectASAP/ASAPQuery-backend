//! Recurring Top-K dashboard comparison for AutoSketch and ASAPPlanner.
use asap_aware_mapping::replacement::default_size_params;
use asap_sketchlib::{CountMinSketchWithHeap, CountSketchWithHeap};
use clap::{Parser, ValueEnum};
use planner_types::{
    post_asap::{SketchAlgorithm, SketchParams},
    pre_asap::AggIntent,
    types::AccuracyTarget,
};
use serde::{Deserialize, Serialize};
#[path = "topk_dashboard/erp.rs"]
mod erp;
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
    /// Offline measured ERP catalog; required for comparisons.
    #[arg(long)]
    erp_catalog: Option<std::path::PathBuf>,
    /// Produce window-specific ERP evidence instead of running the comparison.
    #[arg(long)]
    build_erp: bool,
    #[arg(long, default_value_t = 16_777_216)]
    total_memory_budget_bytes: usize,
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

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
enum Family {
    Cms,
    CountSketch,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
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
                    [16, 32, 64].into_iter().map(move |heap| Config {
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

fn build_pane(pane: &[u32], c: Config) -> Sketch {
    let mut s = Sketch::new(c);
    for k in pane {
        s.update(&format!("key-{k}"), 1.0);
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
        f64::from(pred.is_empty())
    } else {
        let boundary = truth[truth.len().min(K) - 1].1;
        let strict = truth.iter().filter(|(_, count)| *count > boundary).count();
        let strict_hits = truth
            .iter()
            .filter(|(key, count)| *count > boundary && p.contains(key))
            .count();
        let tie_hits = truth
            .iter()
            .filter(|(key, count)| *count == boundary && p.contains(key))
            .count();
        (strict_hits + tie_hits.min(truth.len().min(K) - strict)) as f64 / truth.len().min(K) as f64
    }
}

fn precision(pred: &[(String, f64)], truth: &[(u32, u64)]) -> f64 {
    if pred.is_empty() {
        return f64::from(truth.is_empty());
    }
    recall(pred, truth) * truth.len().min(K) as f64 / pred.len() as f64
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
    let states: Vec<_> = data.iter().map(|p| build_pane(p, c)).collect();
    let ends: std::collections::BTreeSet<_> = (0..5)
        .map(|i| window + (data.len() - window) * i / 4)
        .collect();
    let mut worst = 1.0_f64;
    for end in ends {
        let mut merged = states[end - window].clone();
        for s in &states[end - window + 1..end] {
            merged.merge(s)?;
        }
        worst = worst.min(recall(&merged.topk(), &exact_topk(data, end, window)));
    }
    Ok(worst)
}

fn lhs(seed: u64, family: Family) -> Vec<Config> {
    let mut rng = Rng(seed);
    let mut axes = [vec![3, 5, 7], vec![128, 256, 512, 1024], vec![16, 32, 64]];
    for axis in &mut axes {
        for i in (1..axis.len()).rev() {
            axis.swap(i, rng.next() as usize % (i + 1));
        }
    }
    (0..3)
        .map(|i| Config {
            family,
            rows: axes[0][i],
            cols: axes[1][i],
            heap: axes[2][i],
        })
        .collect()
}

fn autosketch(
    data: &[Vec<u32>],
    window: usize,
    a: &Args,
    offset: usize,
) -> Result<(Config, Duration, usize, f64), Box<dyn std::error::Error>> {
    let grid = candidates(a);
    let began = Instant::now();
    let mut frontier = VecDeque::new();
    for family in [Family::Cms, Family::CountSketch] {
        for c in lhs(a.seed + offset as u64, family) {
            if grid.contains(&c) {
                frontier.push_back((c, None));
            }
        }
    }
    let mut seen = HashSet::new();
    let mut scores = HashMap::new();
    let mut best = None;
    while let Some((c, initial)) = frontier.pop_front() {
        if !seen.insert((c, initial)) {
            continue;
        }
        if best.is_some_and(|b: Config| erp::bytes(c) > erp::bytes(b)) {
            continue;
        }
        let score = if let Some(score) = scores.get(&c) {
            *score
        } else {
            let score = calibration_score(data, window, c)?;
            scores.insert(c, score);
            score
        };
        let feasible = score >= a.min_recall_at_10;
        if feasible
            && best.is_none_or(|b: Config| {
                (c.rows * c.cols * 8 + c.heap * 32) < (b.rows * b.cols * 8 + b.heap * 32)
            })
        {
            best = Some(c)
        }
        if initial.is_some_and(|direction| direction != feasible) {
            continue;
        }
        // Adjacent values in ONE numeric dimension; family is categorical.
        for axis in 0..3 {
            let values: &[usize] = match axis {
                0 => &[3, 5, 7],
                1 => &[128, 256, 512, 1024],
                _ => &[16, 32, 64],
            };
            let value = match axis {
                0 => c.rows,
                1 => c.cols,
                _ => c.heap,
            };
            let pos = values.iter().position(|x| *x == value).unwrap();
            let next = if feasible {
                pos.checked_sub(1)
            } else {
                (pos + 1 < values.len()).then_some(pos + 1)
            };
            if let Some(pos) = next {
                let mut n = c;
                match axis {
                    0 => n.rows = values[pos],
                    1 => n.cols = values[pos],
                    _ => n.heap = values[pos],
                };
                if grid.contains(&n) {
                    frontier.push_back((n, Some(initial.unwrap_or(feasible))));
                }
            }
        }
    }
    let best_observed = scores.values().copied().fold(0.0_f64, f64::max);
    // Algorithm 4 returns the highest-accuracy visited point if none pass.
    // The output records its calibration score so it cannot be mistaken for a
    // feasible selection or hidden by terminating the whole experiment.
    let chosen = best
        .or_else(|| {
            scores
                .iter()
                .max_by(|(a, x), (b, y)| {
                    x.total_cmp(y)
                        .then_with(|| erp::bytes(**b).cmp(&erp::bytes(**a)))
                })
                .map(|(c, _)| *c)
        })
        .ok_or("AutoSketch has no admissible initial candidates")?;
    let score = *scores.get(&chosen).unwrap_or(&best_observed);
    Ok((chosen, began.elapsed(), scores.len(), score))
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
    query_samples: Vec<serde_json::Value>,
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
    let mut samples = Vec::new();
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
        if p < start {
            continue;
        }
        for (q, &window) in WINDOWS.iter().enumerate() {
            let store = &stores[if shared { 0 } else { q }];
            let mut refs: Vec<_> = store.iter().rev().take(window).collect();
            // Heap reconciliation can depend on merge order. Match the
            // chronological order used for both calibration and ERP evidence.
            refs.reverse();
            let t = Instant::now();
            let mut merged = refs[0].clone();
            for s in refs.iter().skip(1) {
                merged.merge(s)?;
            }
            let merge_seconds = t.elapsed().as_secs_f64();
            timing.merge_seconds += merge_seconds;
            let t = Instant::now();
            let pred = black_box(merged.topk());
            let readout_seconds = t.elapsed().as_secs_f64();
            timing.topk_readout_seconds += readout_seconds;
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
            samples.push(serde_json::json!({"end_pane":p+1,"window_panes":window,"recall":r,"precision":precision(&pred,&truth),"merge_seconds":merge_seconds,"readout_seconds":readout_seconds}));
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
        query_samples: samples,
    })
}

fn exact(data: &[Vec<u32>], start: usize, refreshes: usize) -> Row {
    let plan = Instant::now();
    let layout: Vec<_> = WINDOWS.iter().map(|w| (*w, K)).collect();
    black_box(&layout);
    let planning = plan.elapsed().as_secs_f64();
    let mut t = Timing::default();
    let mut retained = VecDeque::new();
    let mut peak_bytes = 0;
    let mut samples = Vec::new();
    for p in 0..start + refreshes {
        let update = Instant::now();
        retained.push_back(data[p].clone());
        t.update_seconds += update.elapsed().as_secs_f64();
        let eviction = Instant::now();
        while retained.len() > WINDOWS[3] {
            retained.pop_front();
        }
        t.eviction_seconds += eviction.elapsed().as_secs_f64();
        peak_bytes = peak_bytes.max(
            retained
                .iter()
                .map(|p| p.len() * std::mem::size_of::<u32>())
                .sum::<usize>(),
        );
        if p < start {
            continue;
        }
        let retained = retained.make_contiguous();
        for &(w, _) in &layout {
            let q = Instant::now();
            black_box(exact_topk(retained, retained.len(), w));
            let elapsed = q.elapsed().as_secs_f64();
            t.exact_query_seconds += elapsed;
            samples
                .push(serde_json::json!({"end_pane":p+1,"window_panes":w,"query_seconds":elapsed}));
        }
    }
    Row {
        method: "exact_hash_scan".into(),
        planning_seconds: planning,
        timing: t,
        logical_payload_bytes: peak_bytes,
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
        query_samples: samples,
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
    if a.refresh_seconds != 30
        || a.cardinality == 0
        || !a.min_recall_at_10.is_finite()
        || !(0.0..=1.0).contains(&a.min_recall_at_10)
    {
        return Err("invalid workload parameters".into());
    }
    if a.build_erp {
        let catalog = erp::build(&a, replayed.as_ref())?;
        let out = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&a.output)?;
        serde_json::to_writer_pretty(out, &catalog)?;
        return Ok(());
    }
    let load_started = Instant::now();
    let catalog: erp::Catalog =
        serde_json::from_reader(std::fs::File::open(a.erp_catalog.as_ref().ok_or(
            "--erp-catalog is required; generate measured evidence using --build-erp",
        )?)?)?;
    let catalog_load_seconds = load_started.elapsed().as_secs_f64();
    let mut trials = Vec::new();
    for trial in 0..a.trials {
        let data = replayed.clone().unwrap_or_else(|| synthetic(&a, trial));
        let cal = &data[..a.calibration_panes];
        let mut auto_configs = Vec::new();
        let mut auto_plan = 0.0;
        let mut searches = Vec::new();
        for (i, &w) in WINDOWS.iter().enumerate() {
            let (c, t, evaluated, score) = autosketch(cal, w, &a, i + trial)?;
            auto_configs.push(c);
            auto_plan += t.as_secs_f64();
            searches.push(serde_json::json!({"window_panes":w,"planning_seconds":t.as_secs_f64(),"evaluated_candidates":evaluated,"minimum_calibration_recall":score,"calibration_feasible":score>=a.min_recall_at_10,"config":c}));
        }
        if auto_configs
            .iter()
            .zip(WINDOWS)
            .map(|(c, w)| erp::bytes(*c) * w)
            .sum::<usize>()
            > a.total_memory_budget_bytes
        {
            return Err("AutoSketch exceeds total memory budget".into());
        }
        let full_started = Instant::now();
        let erp_shared = erp::select(&catalog, cal, &a, true);
        let erp_local = erp::select(&catalog, cal, &a, false);
        let (mut erp_full, full_shared) = match (&erp_shared, &erp_local) {
            (Ok(s), Ok(l)) if l.retained_bytes < s.retained_bytes => (Ok(l.clone()), false),
            (Ok(s), _) => (Ok(s.clone()), true),
            (_, Ok(l)) => (Ok(l.clone()), false),
            _ => (Err("no feasible ERP layout".to_owned()), false),
        };
        if let Ok(d) = &mut erp_full {
            d.planning_seconds = full_started.elapsed().as_secs_f64();
        }
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
                    cols: (width as usize).next_power_of_two(),
                    heap: (heap_size.max(K as u32) as usize).next_power_of_two(),
                },
                other => {
                    return Err(format!("unexpected analytical Top-K params: {other:?}").into())
                }
            };
        let analytical_plan = analytical_start.elapsed().as_secs_f64();
        let mut rows = vec![
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
        let mut decisions = Vec::new();
        for (name, shared, decision) in [
            ("asapplanner_erp", full_shared, erp_full),
            (
                "asapplanner_erp_no_sharing",
                false,
                erp_local.map_err(|e| e.to_string()),
            ),
        ] {
            match decision {
                Ok(d) => {
                    rows.push(run_sketch(
                        name,
                        &data,
                        a.calibration_panes,
                        a.refreshes,
                        d.configs.clone(),
                        shared,
                        d.planning_seconds + catalog_load_seconds,
                        a.min_recall_at_10,
                    )?);
                    decisions
                        .push(serde_json::json!({"method":name,"selection":d,"shared":shared}));
                }
                Err(error) => {
                    let mut fallback = exact(&data, a.calibration_panes, a.refreshes);
                    fallback.method = format!("{name}_exact_fallback");
                    rows.push(fallback);
                    decisions.push(serde_json::json!({"method":name,"fallback_reason":error.to_string(),"reason":"theoretical additive bound does not certify Recall@10; exact fallback"}));
                }
            }
        }
        trials.push(serde_json::json!({"trial":trial,"rows":rows,"erp_decisions":decisions,"autosketch_searches":searches}));
        eprintln!("trial {trial} complete");
    }
    let out = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&a.output)?;
    serde_json::to_writer_pretty(
        out,
        &serde_json::json!({"schema_version":2,"query":"topk(10, count_over_time(events[window]))","windows_minutes":[1,5,15,60],"dashboard_refresh_seconds":a.refresh_seconds,"args":a,"trials":trials,"erp_generation_seconds":catalog.generation_seconds,"catalog_load_seconds":catalog_load_seconds,"memory_metric":"logical retained counters + heap entry proxy (32 bytes); excludes allocator and string overhead","timing_metric":"wall seconds; merge includes heap reconciliation"}),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    // A tied boundary cannot replace a strictly heavier mandatory key.
    #[test]
    fn ties_do_not_hide_missing_heavy_keys() {
        let mut truth = vec![(0, 100)];
        truth.extend((1..20).map(|k| (k, 1)));
        let pred: Vec<_> = (1..=10).map(|k| (format!("key-{k}"), 1.)).collect();
        assert_eq!(recall(&pred, &truth), 0.9);
    }
    // LHS samples each discrete dimension without replacement, within each family.
    #[test]
    fn lhs_covers_both_families_with_distinct_dimensions() {
        for family in [Family::Cms, Family::CountSketch] {
            let points = lhs(42, family);
            assert_eq!(
                points.iter().map(|p| p.rows).collect::<HashSet<_>>().len(),
                3
            );
            assert_eq!(
                points.iter().map(|p| p.cols).collect::<HashSet<_>>().len(),
                3
            );
            assert_eq!(
                points.iter().map(|p| p.heap).collect::<HashSet<_>>().len(),
                3
            );
            assert!(points.iter().all(|p| p.family == family));
        }
    }
    #[test]
    fn dashboard_advances_and_sharing_reduces_updates() {
        let a = Args {
            output: "x".into(),
            backend_revision: "test".into(),
            input_tsv: None,
            erp_catalog: None,
            build_erp: false,
            total_memory_budget_bytes: 16_777_216,
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
        let exact = exact(&d, 120, 3);
        // Only the latest 120 panes are retained, not the union of all query
        // histories; replay keys are u32 rather than machine-sized integers.
        assert_eq!(exact.logical_payload_bytes, 120 * 100 * 4);
        for row in [&shared, &local, &exact] {
            assert_eq!(row.query_samples.len(), 12);
            assert_eq!(row.query_samples.first().unwrap()["end_pane"], 121);
            assert_eq!(row.query_samples.last().unwrap()["end_pane"], 123);
        }
    }
}
