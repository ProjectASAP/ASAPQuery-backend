//! Memory-constrained frequency-sketch selection; not a whole-system benchmark.
use asap_aware_mapping::erp::{
    AccuracyMode, ErpArtifact, ErpRecord, ErpResourceProfile, ErpSelectionRequest,
    ERP_SCHEMA_VERSION,
};
use asap_sketchlib::{BloomFilter, CountMinSketch, CountSketch, CountingBloomFilter};
use clap::{Parser, ValueEnum};
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    time::Instant,
};

#[derive(Parser, Debug, Serialize)]
struct Args {
    /// New JSON artifact path; existing files are never overwritten.
    #[arg(long)]
    output: std::path::PathBuf,
    #[arg(long, default_value_t = 10000)]
    events: usize,
    #[arg(long, default_value_t = 1000)]
    cardinality: usize,
    #[arg(long, default_value_t = 10)]
    runs: u64,
    #[arg(long, default_value_t = 42)]
    seed: u64,
    #[arg(long, default_value_t = 0.01)]
    epsilon: f64,
    /// Zero is uniform; positive values generate a truncated Zipf distribution.
    #[arg(long, default_value_t = 0.0)]
    zipf: f64,
    /// Required provenance, obtained with git rev-parse HEAD.
    #[arg(long)]
    backend_revision: String,
    /// Hard cap on resident f64 counter payload per selected sketch (not RSS).
    #[arg(long, default_value_t = 32768)]
    memory_budget_bytes: usize,
    #[arg(
        long,
        value_enum,
        value_delimiter = ',',
        default_value = "cms,count-sketch"
    )]
    sketches: Vec<Family>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum Family {
    Cms,
    CountSketch,
    Bloom,
    CountingBloom,
}
impl Family {
    fn name(self) -> &'static str {
        match self {
            Self::Cms => "cms",
            Self::CountSketch => "count_sketch",
            Self::Bloom => "bloom",
            Self::CountingBloom => "counting_bloom",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
struct Config {
    family: Family,
    width: usize,
    depth: usize,
}
impl Config {
    fn bytes(self) -> usize {
        self.width * self.depth * std::mem::size_of::<f64>()
    }
    fn id(self) -> String {
        format!("{}-{:05}-{:02}", self.family.name(), self.width, self.depth)
    }
    fn legal(self) -> bool {
        // CMS selects Packed64/128/Rows automatically; portable CountSketch
        // hardcodes Packed64 and needs room for every column/sign bit.
        !matches!(self.family, Family::CountSketch)
            || self.depth * (self.width.ilog2() as usize + 1) <= 64
    }
}

fn grid() -> Vec<Config> {
    (0..7)
        .flat_map(|i| {
            (1..=8).map(move |depth| Config {
                family: Family::Cms,
                width: 64 << i,
                depth,
            })
        })
        .collect()
}

fn candidate_grid(families: &[Family], budget: usize) -> Vec<Config> {
    families
        .iter()
        .flat_map(|&family| grid().into_iter().map(move |c| Config { family, ..c }))
        .filter(|c| c.legal() && c.bytes() <= budget)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

// Small deterministic PRNG for workload sampling and LHS permutations. CMS
// itself uses the library's fixed hash implementation, not this seed.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut x = self.0;
        x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
        x ^ (x >> 31)
    }
    fn shuffle<T>(&mut self, values: &mut [T]) {
        for i in (1..values.len()).rev() {
            values.swap(i, self.next() as usize % (i + 1));
        }
    }
}

fn stream(events: usize, cardinality: usize, zipf: f64, seed: u64) -> Vec<usize> {
    let mut cumulative = Vec::with_capacity(cardinality);
    let mut sum = 0.0;
    for i in 1..=cardinality {
        sum += (i as f64).powf(-zipf);
        cumulative.push(sum);
    }
    let mut rng = Rng(seed);
    (0..events)
        .map(|_| {
            let u = (rng.next() >> 11) as f64 / ((1_u64 << 53) as f64) * sum;
            cumulative.partition_point(|v| *v <= u).min(cardinality - 1)
        })
        .collect()
}

#[derive(Debug, Serialize)]
struct Measurement {
    config: Config,
    max_normalized_additive_error: f64,
    counter_bytes: usize,
    update_wall_seconds: f64,
    query_wall_seconds: f64,
}

fn measure(config: Config, data: &[usize], keys: &[String]) -> Measurement {
    assert!(!data.is_empty());
    let mut exact = vec![0_u64; keys.len()];
    for &key in data {
        exact[key] += 1;
    }
    assert!(config.legal());
    let mut cms =
        (config.family == Family::Cms).then(|| CountMinSketch::new(config.depth, config.width));
    let mut cs = (config.family == Family::CountSketch)
        .then(|| CountSketch::new(config.depth, config.width));
    let mut counting_bloom = (config.family == Family::CountingBloom)
        .then(|| CountingBloomFilter::new(config.width, config.depth));
    let mut plain_bloom = (config.family == Family::Bloom)
        .then(|| BloomFilter::new(config.width, config.depth));
    let started = Instant::now();
    for &key in data {
        match (&mut cms, &mut cs, &mut counting_bloom, &mut plain_bloom) {
            (Some(sketch), _, _, _) => sketch.update(&keys[key], 1.0),
            (_, Some(sketch), _, _) => sketch.update(&keys[key], 1.0),
            (_, _, Some(sketch), _) => sketch.insert(&keys[key]),
            (_, _, _, Some(sketch)) => sketch.insert(&keys[key]),
            _ => unreachable!(),
        }
    }
    let update_wall_seconds = started.elapsed().as_secs_f64();
    let started = Instant::now();
    let estimates: Vec<_> = keys
        .iter()
        .map(|key| match (&cms, &cs, &counting_bloom, &plain_bloom) {
            (Some(sketch), _, _, _) => sketch.estimate(key),
            (_, Some(sketch), _, _) => sketch.estimate(key),
            (_, _, Some(sketch), _) => {
                if sketch.contains(key) {
                    1.0
                } else {
                    0.0
                }
            }
            (_, _, _, Some(sketch)) => {
                if sketch.contains(key) { 1.0 } else { 0.0 }
            }
            _ => unreachable!(),
        })
        .collect();
    let query_wall_seconds = started.elapsed().as_secs_f64();
    let error = estimates
        .iter()
        .zip(exact)
        .map(|(estimate, truth)| {
            assert!(estimate.is_finite());
            if matches!(config.family, Family::Bloom | Family::CountingBloom) {
                (estimate - f64::from(truth > 0)).abs()
            } else {
                (estimate - truth as f64).abs() / data.len() as f64
            }
        })
        .fold(0.0_f64, f64::max);
    Measurement {
        config,
        max_normalized_additive_error: error,
        counter_bytes: config.bytes(),
        update_wall_seconds,
        query_wall_seconds,
    }
}

fn lhs(seed: u64) -> Vec<Config> {
    let mut rng = Rng(seed);
    let mut widths: Vec<_> = (0..7).collect();
    let mut depths: Vec<_> = (1..=8).collect();
    rng.shuffle(&mut widths);
    rng.shuffle(&mut depths);
    widths
        .into_iter()
        .zip(depths)
        .map(|(w, depth)| Config {
            family: Family::Cms,
            width: 64 << w,
            depth,
        })
        .collect()
}

#[derive(Debug, Serialize)]
struct Search {
    selected: Option<Config>,
    visited: Vec<Config>,
    selection_wall_seconds: f64,
}

// Discrete software adaptation of Algorithm 4: depth +/-1, width x2 or /2.
// Each LHS path stops after crossing feasibility; duplicate evaluations are
// cached. Pruned feasible nodes may still lead to cheaper neighbors.
#[cfg(test)]
fn search(seed: u64, epsilon: f64, evaluate: impl FnMut(Config) -> f64) -> Search {
    search_candidates(
        seed,
        epsilon,
        &candidate_grid(&[Family::Cms], usize::MAX),
        evaluate,
    )
}

fn search_candidates(
    seed: u64,
    epsilon: f64,
    candidates: &[Config],
    mut evaluate: impl FnMut(Config) -> f64,
) -> Search {
    let started = Instant::now();
    let families: BTreeSet<_> = candidates.iter().map(|c| c.family).collect();
    let mut pending = VecDeque::new();
    for family in families {
        for point in lhs(seed) {
            let c = Config { family, ..point };
            if candidates.contains(&c) {
                pending.push_back((c, None));
            }
        }
        // A tight budget can exclude every LHS point. Keep a legal seed for
        // every family rather than silently removing it from sketch selection.
        if let Some(&minimum) = candidates
            .iter()
            .filter(|c| c.family == family)
            .min_by_key(|c| (c.bytes(), **c))
        {
            pending.push_back((minimum, None));
        }
    }
    let mut expanded = BTreeSet::new();
    let mut cache = BTreeMap::new();
    let mut visited = Vec::new();
    let mut best: Option<Config> = None;
    while let Some((c, initial_feasible)) = pending.pop_front() {
        if !expanded.insert((c, initial_feasible)) {
            continue;
        }
        let error = *cache.entry(c).or_insert_with(|| {
            visited.push(c);
            evaluate(c)
        });
        let feasible = error.is_finite() && error <= epsilon;
        if feasible && best.is_none_or(|b| (c.bytes(), c) < (b.bytes(), b)) {
            best = Some(c);
        }
        let direction = initial_feasible.unwrap_or(feasible);
        if direction != feasible {
            continue;
        }
        if !feasible && best.is_some_and(|b| c.bytes() >= b.bytes()) {
            continue;
        }
        let neighbors = if feasible {
            [
                c.width
                    .checked_div(2)
                    .filter(|w| *w >= 64)
                    .map(|width| Config { width, ..c }),
                c.depth
                    .checked_sub(1)
                    .filter(|d| *d >= 1)
                    .map(|depth| Config { depth, ..c }),
            ]
        } else {
            [
                (c.width < 4096).then_some(Config {
                    width: c.width * 2,
                    ..c
                }),
                (c.depth < 8).then_some(Config {
                    depth: c.depth + 1,
                    ..c
                }),
            ]
        };
        for next in neighbors.into_iter().flatten() {
            if candidates.contains(&next) {
                pending.push_back((next, Some(direction)));
            }
        }
    }
    Search {
        selected: best,
        visited,
        selection_wall_seconds: started.elapsed().as_secs_f64(),
    }
}

fn oracle(rows: &[Measurement], epsilon: f64) -> Option<Config> {
    rows.iter()
        .filter(|m| m.max_normalized_additive_error <= epsilon)
        .map(|m| m.config)
        .min_by_key(|c| (c.bytes(), *c))
}

fn artifact(rows: &[Measurement], distribution: serde_json::Value, revision: &str) -> ErpArtifact {
    ErpArtifact {
        schema_version: ERP_SCHEMA_VERSION,
        producer_version: revision.into(),
        records: rows
            .iter()
            .map(|m| ErpRecord {
                id: m.config.id(),
                sketch: m.config.family.name().into(),
                implementation: "asap_sketchlib-portable".into(),
                parameters: serde_json::json!({"width":m.config.width,"depth":m.config.depth}),
                distribution: distribution.clone(),
                trials: 1,
                error_metrics: BTreeMap::from([(
                    "max_normalized_additive_error".into(),
                    m.max_normalized_additive_error,
                )]),
                // Memory-only experiment. Timings are wall measurements, not CPU
                // profiles: deliberately do not mislabel them as ERP CPU evidence.
                resources: ErpResourceProfile {
                    memory_bytes: m.counter_bytes as f64,
                    update_cpu_seconds: 0.0,
                    query_cpu_seconds: 0.0,
                    merge_cpu_seconds: 0.0,
                },
            })
            .collect(),
    }
}

fn erp_select(
    profile: &ErpArtifact,
    distribution: serde_json::Value,
    epsilon: f64,
) -> Option<Config> {
    let request = ErpSelectionRequest {
        distribution,
        implementation: Some("asap_sketchlib-portable".into()),
        allowed_sketches: vec![
            "cms".into(),
            "count_sketch".into(),
            "bloom".into(),
            "counting_bloom".into(),
        ],
        error_metric: "max_normalized_additive_error".into(),
        max_error: epsilon,
        min_trials: 1,
        expected_updates: 0.0,
        expected_queries: 0.0,
        expected_merges: 0.0,
        retention_seconds: 1.0,
        cpu_weight: 0.0,
        byte_second_weight: 1.0,
        mode: AccuracyMode::Empirical,
    };
    match profile.select(&request) {
        Ok(point) => Some(Config {
            family: match point.record.sketch.as_str() {
                "cms" => Family::Cms,
                "count_sketch" => Family::CountSketch,
                _ => panic!("unknown family"),
            },
            width: point.record.parameters["width"].as_u64().unwrap() as usize,
            depth: point.record.parameters["depth"].as_u64().unwrap() as usize,
        }),
        Err(asap_aware_mapping::erp::ErpError::NoApplicableConfiguration) => None,
        Err(error) => panic!("invalid experiment profile: {error}"),
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if args.events == 0
        || args.cardinality == 0
        || args.runs == 0
        || !args.epsilon.is_finite()
        || args.epsilon < 0.0
        || !args.zipf.is_finite()
        || args.zipf < 0.0
        || args.backend_revision.trim().is_empty()
    {
        return Err("invalid experiment arguments".into());
    }
    // Fail before benchmarking if the destination already exists.
    let output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&args.output)?;
    let keys: Vec<_> = (0..args.cardinality).map(|i| format!("key-{i}")).collect();
    let mut runs = Vec::new();
    for run in 0..args.runs {
        let calibration_seed = args.seed.wrapping_add(run.wrapping_mul(2));
        let test_seed = calibration_seed.wrapping_add(1);
        let calibration = stream(args.events, args.cardinality, args.zipf, calibration_seed);
        let distribution = serde_json::json!({"generator":"splitmix64-truncated-zipf-v1",
            "events":args.events,"cardinality":args.cardinality,"zipf":args.zipf,"seed":calibration_seed});
        let start = Instant::now();
        let mut candidates = candidate_grid(&args.sketches, args.memory_budget_bytes);
        Rng(calibration_seed).shuffle(&mut candidates);
        let rows: Vec<_> = candidates
            .iter()
            .copied()
            .map(|c| measure(c, &calibration, &keys))
            .collect();
        let calibration_wall_seconds = start.elapsed().as_secs_f64();
        let profile = artifact(&rows, distribution.clone(), &args.backend_revision);
        let adapted = search_candidates(calibration_seed, args.epsilon, &candidates, |c| {
            rows.iter()
                .find(|m| m.config == c)
                .unwrap()
                .max_normalized_additive_error
        });
        let start = Instant::now();
        let erp = erp_select(&profile, distribution, args.epsilon);
        let erp_selection_wall_seconds = start.elapsed().as_secs_f64();
        let start = Instant::now();
        let exhaustive = oracle(&rows, args.epsilon);
        let oracle_selection_wall_seconds = start.elapsed().as_secs_f64();
        assert_eq!(
            erp, exhaustive,
            "same-table ERP must match the finite-grid oracle"
        );
        // Test data is generated only after all configuration decisions are made.
        let held_out = stream(args.events, args.cardinality, args.zipf, test_seed);
        let mut outcomes = Vec::new();
        for (name, selected) in [
            ("autosketch_adapted", adapted.selected),
            ("asapplanner_erp_selector", erp),
            ("grid_oracle", exhaustive),
        ] {
            let measured = selected.map(|c| measure(c, &held_out, &keys));
            let pass = measured
                .as_ref()
                .map(|m| m.max_normalized_additive_error <= args.epsilon);
            outcomes.push(serde_json::json!({"method":name,"selected":selected,
                "status":if selected.is_some(){"calibration_feasible"}else{"no_feasible_configuration"},
                "held_out_pass":pass,"held_out":measured}));
        }
        runs.push(serde_json::json!({"run":run,"calibration_seed":calibration_seed,"test_seed":test_seed,
            "calibration_wall_seconds":calibration_wall_seconds,"calibration_grid":rows,
            "erp_artifact":profile,"search":adapted,"erp_selection_wall_seconds":erp_selection_wall_seconds,
            "oracle_selection_wall_seconds":oracle_selection_wall_seconds,"outcomes":outcomes}));
        eprintln!(
            "run {run}: ERP={erp:?}, adapted={:?}, oracle={exhaustive:?}",
            adapted.selected
        );
    }
    serde_json::to_writer_pretty(
        output,
        &serde_json::json!({"schema_version":1,"args":args,
        "debug_assertions":cfg!(debug_assertions),
        "scope":"memory-constrained CMS/CountSketch family and parameter selection; not full ASAPPlanner or system evaluation",
        "memory_metric":"f64 counter payload bytes; excludes allocator and object overhead",
        "hash_seeds":"library fixed hash; only data/search seeds vary",
        "timing_metric":"wall seconds, not CPU seconds; calibration cost reported separately",
        "cargo_lock":include_str!("../../Cargo.lock"),"runs":runs}),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hard_budget_prunes_before_benchmark_and_empty_budget_fails_closed() {
        for budget in [0, 511, 512, 2048] {
            let candidates = candidate_grid(&[Family::Cms, Family::CountSketch], budget);
            let result = search_candidates(42, 0.01, &candidates, |c| {
                assert!(c.bytes() <= budget);
                assert!(c.legal());
                0.0
            });
            assert_eq!(result.selected.is_none(), budget < 512);
            assert_eq!(candidates.is_empty(), budget < 512);
            let rows: Vec<_> = candidates
                .into_iter()
                .map(|config| Measurement {
                    config,
                    max_normalized_additive_error: 0.0,
                    counter_bytes: config.bytes(),
                    update_wall_seconds: 0.0,
                    query_wall_seconds: 0.0,
                })
                .collect();
            let context = serde_json::json!({"test":"budget"});
            let selected = erp_select(&artifact(&rows, context.clone(), "test"), context, 0.01);
            assert_eq!(selected, oracle(&rows, 0.01));
            assert_eq!(selected.is_none(), budget < 512);
            assert!(selected.is_none_or(|c| c.bytes() <= budget));
        }
    }
    #[test]
    fn sketch_selection_can_choose_either_family() {
        let candidates = candidate_grid(&[Family::Cms, Family::CountSketch], 2048);
        for preferred in [Family::Cms, Family::CountSketch] {
            let rows: Vec<_> = candidates
                .iter()
                .map(|&config| Measurement {
                    config,
                    max_normalized_additive_error: if config.family == preferred {
                        0.0
                    } else {
                        1.0
                    },
                    counter_bytes: config.bytes(),
                    update_wall_seconds: 0.0,
                    query_wall_seconds: 0.0,
                })
                .collect();
            let result = search_candidates(42, 0.01, &candidates, |c| {
                rows.iter()
                    .find(|r| r.config == c)
                    .unwrap()
                    .max_normalized_additive_error
            });
            let context = serde_json::json!({"test":"family"});
            let selected = erp_select(&artifact(&rows, context.clone(), "test"), context, 0.01);
            assert_eq!(result.selected.unwrap().family, preferred);
            assert_eq!(selected.unwrap().family, preferred);
            assert_eq!(selected, oracle(&rows, 0.01));
        }
    }
    #[test]
    fn count_sketch_hash_limit_and_single_key_oracle() {
        let candidates = candidate_grid(&[Family::CountSketch], usize::MAX);
        assert!(candidates
            .iter()
            .all(|c| c.depth * (c.width.ilog2() as usize + 1) <= 64));
        assert!(!candidates.contains(&Config {
            family: Family::CountSketch,
            width: 4096,
            depth: 8
        }));
        for config in candidates {
            assert_eq!(
                measure(config, &[0; 100], &["only".into()]).max_normalized_additive_error,
                0.0
            );
        }
    }
    #[test]
    fn lhs_has_unique_coordinates_and_is_reproducible() {
        let sample = lhs(42);
        assert_eq!(sample, lhs(42));
        assert_eq!(
            sample
                .iter()
                .map(|c| c.width)
                .collect::<BTreeSet<_>>()
                .len(),
            7
        );
        assert_eq!(
            sample
                .iter()
                .map(|c| c.depth)
                .collect::<BTreeSet<_>>()
                .len(),
            7
        );
        assert!(sample.iter().all(|c| grid().contains(c)));
    }
    #[test]
    fn search_deduplicates_and_stays_in_grid() {
        let mut seen = BTreeSet::new();
        let result = search(42, 0.01, |c| {
            assert!(seen.insert(c));
            assert!(grid().contains(&c));
            1.0 / c.width as f64
        });
        assert!(result.selected.is_some());
        assert!(result.visited.len() <= 56);
    }
    #[test]
    fn all_feasible_reaches_smallest_state() {
        assert_eq!(
            search(42, 0.01, |_| 0.0).selected,
            Some(Config {
                family: Family::Cms,
                width: 64,
                depth: 1
            })
        );
    }
    #[test]
    fn no_feasible_is_not_best_effort_success() {
        assert_eq!(search(42, 0.01, |_| 1.0).selected, None);
        assert_eq!(search(42, 0.01, |_| f64::NAN).selected, None);
    }
    #[test]
    fn deterministic_streams_and_held_out_separation() {
        assert_eq!(stream(100, 20, 1.2, 42), stream(100, 20, 1.2, 42));
        assert_ne!(stream(100, 20, 1.2, 42), stream(100, 20, 1.2, 43));
    }
    #[test]
    fn actual_cms_single_key_has_zero_error() {
        let m = measure(
            Config {
                family: Family::Cms,
                width: 64,
                depth: 1,
            },
            &[0; 100],
            &["only".into()],
        );
        assert_eq!(m.max_normalized_additive_error, 0.0);
        assert_eq!(m.counter_bytes, 512);
    }
    #[test]
    fn real_erp_matches_oracle_and_fails_closed_on_context_miss() {
        let rows: Vec<_> = grid()
            .into_iter()
            .map(|config| Measurement {
                config,
                max_normalized_additive_error: 1.0 / config.width as f64,
                counter_bytes: config.bytes(),
                update_wall_seconds: 0.0,
                query_wall_seconds: 0.0,
            })
            .collect();
        let distribution = serde_json::json!({"seed":42});
        let profile = artifact(&rows, distribution.clone(), "test-revision");
        assert_eq!(
            erp_select(&profile, distribution.clone(), 0.01),
            oracle(&rows, 0.01)
        );
        assert_eq!(
            erp_select(&profile, serde_json::json!({"seed":43}), 0.01),
            None
        );
        assert_eq!(erp_select(&profile, distribution, 0.0), None);
    }
}
