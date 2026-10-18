//! Correctness and benefit: each run's answers against the exact answer
//! and the Planner executor's reference, and both runs' resources and
//! latency side by side.

use std::collections::BTreeMap;
use std::sync::Arc;

use asap_executor::physical_planner::frontier_from_timing;
use control_plane::InstalledPlan;
use data_plane::precompute::RawSample;
use data_plane::query::QueryEngine;
use data_plane::sds::{Sds, SummaryStore};
use serde::Serialize;
use serde_json::{json, Value};

use crate::generator::Generator;
use crate::run::Evaluation;
use crate::workloads::WorkloadQuery;

/// Every generated sample, by metric, in time order.
pub struct SampleIndex {
    by_metric: BTreeMap<String, Vec<RawSample>>,
}

impl SampleIndex {
    pub fn new(mut generator: Generator) -> Self {
        let mut by_metric: BTreeMap<String, Vec<RawSample>> = BTreeMap::new();
        let ticks: Vec<i64> = generator.ticks().collect();
        for t in ticks {
            for s in generator.step(t) {
                by_metric
                    .entry(s.labels["__name__"].clone())
                    .or_default()
                    .push(RawSample {
                        labels: Arc::clone(s.labels),
                        timestamp_ms: t,
                        value: s.value,
                    });
            }
        }
        Self { by_metric }
    }

    /// `metric`'s samples in `(start_ms, end_ms]`.
    pub fn range(&self, metric: &str, start_ms: i64, end_ms: i64) -> &[RawSample] {
        let Some(all) = self.by_metric.get(metric) else {
            return &[];
        };
        let from = all.partition_point(|s| s.timestamp_ms <= start_ms);
        let to = all.partition_point(|s| s.timestamp_ms <= end_ms);
        &all[from..to]
    }
}

type Answer = Vec<(BTreeMap<String, String>, f64)>;

/// The Planner executor's answer to `query` at `time_ms` when the selected
/// plan runs with nothing materialized (`reference` is that plan).
pub struct Reference<'a> {
    engine: QueryEngine,
    index: &'a SampleIndex,
}

impl<'a> Reference<'a> {
    pub fn new(reference: &InstalledPlan, index: &'a SampleIndex) -> anyhow::Result<Self> {
        let dag = &reference.dag.dag;
        let sds = Sds::from_dag(dag, &frontier_from_timing(dag)?)?;
        anyhow::ensure!(
            sds.definitions.is_empty(),
            "the reference plan materializes nothing"
        );
        Ok(Self {
            engine: QueryEngine::new(reference, &sds),
            index,
        })
    }

    pub fn answer(&mut self, query: &str, time_ms: i64) -> Result<Answer, String> {
        let store = SummaryStore::new();
        let (root, ranges) = self
            .engine
            .raw_ranges(query, time_ms, &store)
            .map_err(|e| e.to_string())?;
        let raw = ranges
            .iter()
            .map(|r| {
                (
                    r.node,
                    self.index.range(&r.metric, r.start_ms, r.end_ms).to_vec(),
                )
            })
            .collect();
        let mut samples = self
            .engine
            .evaluate(root, time_ms, &store, None, &raw)
            .map_err(|e| e.to_string())?;
        samples.sort_by(|a, b| a.labels.cmp(&b.labels));
        Ok(samples.into_iter().map(|s| (s.labels, s.value)).collect())
    }
}

/// How an answer's error is measured: the target's metric and bound.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Metric {
    Exact,
    Absolute(f64),
    Relative(f64),
    /// Rank error of `quantile_over_time(phi, metric[range])`.
    Rank(f64),
}

fn metric_of(target: &Value) -> Metric {
    let e = target["max_error"].as_f64().unwrap_or(0.0);
    if e == 0.0 {
        return Metric::Exact;
    }
    match target["accuracy_metric"].as_str() {
        Some("absolute") => Metric::Absolute(e),
        Some("rank") => Metric::Rank(e),
        _ => Metric::Relative(e),
    }
}

/// Float rounding allowed between two exact answers.
fn same_exact(a: f64, b: f64) -> bool {
    (a.is_nan() && b.is_nan()) || a == b || (a - b).abs() <= 1e-9 * a.abs().max(b.abs()).max(1.0)
}

/// The rank of `v` among sorted `values` as a fraction of `n - 1`, the
/// inverse of Prometheus' interpolated quantile: the φ-quantile Prometheus
/// returns has rank exactly φ, and the i-th smallest sample rank i/(n-1).
fn fractional_rank(values: &[f64], v: f64) -> f64 {
    let n = values.len();
    if n < 2 {
        return 0.0;
    }
    if v <= values[0] {
        return 0.0;
    }
    if v >= values[n - 1] {
        return 1.0;
    }
    let i = values.partition_point(|&x| x <= v) - 1;
    let h = i as f64 + (v - values[i]) / (values[i + 1] - values[i]);
    h / (n - 1) as f64
}

/// `quantile_over_time(phi, m[range])` parsed as `(phi, m, range_ms)`.
fn quantile_over_time(query: &str) -> Option<(f64, String, i64)> {
    let inner = query.trim().strip_prefix("quantile_over_time(")?;
    let (phi, rest) = inner.split_once(',')?;
    let phi: f64 = phi.trim().parse().ok()?;
    let (metric, rest) = rest.trim().split_once('[')?;
    let (range, _) = rest.split_once(']')?;
    let (count, unit_ms) = [("ms", 1), ("m", 60_000), ("h", 3_600_000), ("s", 1000)]
        .iter()
        .find_map(|(suffix, ms)| range.strip_suffix(suffix).map(|n| (n, *ms)))?;
    let ms = count.parse::<i64>().ok()? * unit_ms;
    Some((phi, metric.trim().to_string(), ms))
}

/// `actual` and `exact` with the same label names: when their label names
/// differ (the plan's output lacks labels Prometheus keeps), both are
/// projected onto the names they share and sorted by labels, then value.
/// Returns whether they were projected.
fn align(actual: &Answer, exact: &Answer) -> (Answer, Answer, bool) {
    let names = |a: &Answer| -> std::collections::BTreeSet<String> {
        a.iter().flat_map(|(l, _)| l.keys().cloned()).collect()
    };
    let (na, ne) = (names(actual), names(exact));
    if na == ne || actual.is_empty() || exact.is_empty() {
        return (actual.clone(), exact.clone(), false);
    }
    let common: std::collections::BTreeSet<String> = na.intersection(&ne).cloned().collect();
    let project = |a: &Answer| {
        let mut p: Answer = a
            .iter()
            .map(|(l, v)| {
                (
                    l.iter()
                        .filter(|(k, _)| common.contains(*k))
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
                    *v,
                )
            })
            .collect();
        p.sort_by(|x, y| x.0.cmp(&y.0).then(x.1.total_cmp(&y.1)));
        p
    };
    (project(actual), project(exact), true)
}

/// Per-series error of `actual` against `exact`, and whether every series
/// meets the target. Missing or extra series miss the target.
struct Error {
    max_abs: f64,
    max_rel: f64,
    max_rank: Option<f64>,
    meets: bool,
    labels_differ: bool,
}

fn error(
    query: &WorkloadQuery,
    time_ms: i64,
    actual: &Answer,
    exact: &Answer,
    index: &SampleIndex,
) -> Error {
    let metric = metric_of(&query.accuracy);
    let (actual, exact, labels_differ) = align(actual, exact);
    let mut e = Error {
        max_abs: 0.0,
        max_rel: 0.0,
        max_rank: None,
        meets: actual.len() == exact.len(),
        labels_differ,
    };
    // For a rank target: each series' sorted window samples, without
    // `__name__`, as `quantile_over_time` labels its result.
    let windows = match metric {
        Metric::Rank(_) => quantile_over_time(&query.query).map(|(_, m, range)| {
            let mut by_series: BTreeMap<BTreeMap<String, String>, Vec<f64>> = BTreeMap::new();
            for s in index.range(&m, time_ms - range, time_ms) {
                let mut labels = (*s.labels).clone();
                labels.remove("__name__");
                by_series.entry(labels).or_default().push(s.value);
            }
            by_series
                .values_mut()
                .for_each(|v| v.sort_by(f64::total_cmp));
            by_series
        }),
        _ => None,
    };
    for ((labels, a), (exact_labels, x)) in actual.iter().zip(&exact) {
        let x = *x;
        if labels != exact_labels {
            e.meets = false;
            continue;
        }
        let abs = if same_exact(*a, x) {
            0.0
        } else {
            (a - x).abs()
        };
        let rel = if abs == 0.0 {
            0.0
        } else {
            abs / x.abs().max(f64::MIN_POSITIVE)
        };
        e.max_abs = e.max_abs.max(abs);
        e.max_rel = e.max_rel.max(rel);
        let ok = match metric {
            Metric::Exact => abs == 0.0,
            Metric::Absolute(bound) => abs <= bound,
            Metric::Relative(bound) => rel <= bound,
            Metric::Rank(bound) => {
                let rank = windows.as_ref().map(|by_series| {
                    let values = by_series.get(labels).map(Vec::as_slice).unwrap_or_default();
                    (fractional_rank(values, *a) - fractional_rank(values, x)).abs()
                });
                match rank {
                    Some(r) => {
                        e.max_rank = Some(e.max_rank.unwrap_or(0.0).max(r));
                        r <= bound + 1e-9
                    }
                    None => false,
                }
            }
        };
        e.meets &= ok;
    }
    e
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let i = ((p * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len()) - 1;
    sorted[i]
}

#[derive(Debug, Default, Serialize)]
pub struct QueryReport {
    pub name: String,
    pub query: String,
    pub accuracy: Value,
    pub evaluations: usize,
    /// ASAP run: evaluations answered by the plan.
    pub answered_by_plan: usize,
    /// ASAP run: forwarded evaluations by reason.
    pub forwarded: BTreeMap<String, usize>,
    /// Baseline answers equal to the executor reference (exact targets), or
    /// within the target of it.
    pub baseline_matches_reference: usize,
    pub baseline_reference_mismatch_example: Option<Value>,
    /// Plan answers equal to the executor reference (exact targets) or
    /// within the target of the exact answer.
    pub plan_matches_reference: usize,
    pub plan_reference_mismatch_example: Option<Value>,
    /// ASAP answers (plan or forwarded) within the target of Prometheus'
    /// exact answer.
    pub asap_meets_target: usize,
    pub asap_max_abs_error: f64,
    pub asap_max_rel_error: f64,
    pub asap_max_rank_error: Option<f64>,
    /// ASAP answers whose label names differ from Prometheus' (compared on
    /// the names they share).
    pub asap_labels_differ: usize,
    pub baseline_latency_ms_p50: f64,
    pub baseline_latency_ms_p99: f64,
    pub asap_latency_ms_p50: f64,
    pub asap_latency_ms_p99: f64,
}

/// Compares the two runs' evaluations query by query.
pub fn compare(
    queries: &[WorkloadQuery],
    baseline: &[Evaluation],
    asap: &[Evaluation],
    reference: &mut Reference<'_>,
    index: &SampleIndex,
) -> Vec<QueryReport> {
    let mut reports: Vec<QueryReport> = queries
        .iter()
        .map(|q| QueryReport {
            name: q.name.clone(),
            query: q.query.clone(),
            accuracy: q.accuracy.clone(),
            ..Default::default()
        })
        .collect();
    let base: BTreeMap<(usize, i64), &Evaluation> =
        baseline.iter().map(|e| ((e.query, e.time_ms), e)).collect();
    let mut latencies: Vec<(Vec<f64>, Vec<f64>)> = vec![(Vec::new(), Vec::new()); queries.len()];
    for b in baseline {
        latencies[b.query].0.push(b.latency_ms);
    }
    for a in asap {
        let q = &queries[a.query];
        let r = &mut reports[a.query];
        latencies[a.query].1.push(a.latency_ms);
        r.evaluations += 1;
        let Some(b) = base.get(&(a.query, a.time_ms)) else {
            continue;
        };
        let reference_answer = reference.answer(&q.query, a.time_ms);
        let exact_target = metric_of(&q.accuracy) == Metric::Exact;
        // The baseline (exact) against the executor reference: equal for an
        // exact plan, within the target for a sketch.
        let mismatch = |answer: &Answer, reference: &Result<Answer, String>| -> Option<Value> {
            match reference {
                Err(e) => Some(json!({"time_ms": a.time_ms, "reference_error": e})),
                Ok(reference) => {
                    let ok = if exact_target {
                        let (answer, reference, _) = align(answer, reference);
                        answer.len() == reference.len()
                            && answer
                                .iter()
                                .zip(&reference)
                                .all(|(x, y)| x.0 == y.0 && same_exact(x.1, y.1))
                    } else {
                        error(q, a.time_ms, reference, answer, index).meets
                    };
                    (!ok).then(|| {
                        json!({
                            "time_ms": a.time_ms,
                            "answer": answer.iter().take(3).collect::<Vec<_>>(),
                            "reference": reference.iter().take(3).collect::<Vec<_>>(),
                            "answer_series": answer.len(),
                            "reference_series": reference.len(),
                        })
                    })
                }
            }
        };
        match mismatch(&b.result, &reference_answer) {
            None => r.baseline_matches_reference += 1,
            Some(m) => {
                r.baseline_reference_mismatch_example.get_or_insert(m);
            }
        }
        if a.execution == "plan" {
            r.answered_by_plan += 1;
            match mismatch(&a.result, &reference_answer) {
                None => r.plan_matches_reference += 1,
                Some(m) => {
                    r.plan_reference_mismatch_example.get_or_insert(m);
                }
            }
        } else {
            let reason = a
                .forward_reason
                .clone()
                .unwrap_or_else(|| "not planned".into());
            // Keep the reason's kind, not its pane times.
            let kind = reason
                .split(" ending at ")
                .next()
                .unwrap_or(&reason)
                .to_string();
            *r.forwarded.entry(kind).or_default() += 1;
        }
        let e = error(q, a.time_ms, &a.result, &b.result, index);
        r.asap_max_abs_error = r.asap_max_abs_error.max(e.max_abs);
        r.asap_max_rel_error = r.asap_max_rel_error.max(e.max_rel);
        if let Some(rank) = e.max_rank {
            r.asap_max_rank_error = Some(r.asap_max_rank_error.unwrap_or(0.0).max(rank));
        }
        if e.meets {
            r.asap_meets_target += 1;
        }
        if e.labels_differ {
            r.asap_labels_differ += 1;
        }
    }
    for (r, (mut b, mut a)) in reports.iter_mut().zip(latencies) {
        b.sort_by(f64::total_cmp);
        a.sort_by(f64::total_cmp);
        r.baseline_latency_ms_p50 = percentile(&b, 0.5);
        r.baseline_latency_ms_p99 = percentile(&b, 0.99);
        r.asap_latency_ms_p50 = percentile(&a, 0.5);
        r.asap_latency_ms_p99 = percentile(&a, 0.99);
    }
    reports
}
