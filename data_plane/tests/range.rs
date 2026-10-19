//! Range queries in the query engine: one evaluation per step, raw samples
//! fetched once for all steps, and stored and kept panes shared across
//! steps as they are across the evaluations of a recurring query.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use asap_executor::values::Batch;
use asap_planner::InstallablePlan;
use data_plane::precompute::{Precompute, RawSample};
use data_plane::query::{KeptResultStats, QueryEngine, RawRange, ResultSample, Unanswered};
use data_plane::sds::{Sds, SummaryStore};

const MINUTE: i64 = 60_000;

fn sds(plan: &InstallablePlan) -> Sds {
    Sds::from_plan(plan).unwrap()
}

/// Samples every 15 s in `[from, to)` of `metric` for each `service`, with
/// value `f(series index, t)`.
fn samples(
    metric: &str,
    services: &[&str],
    from: i64,
    to: i64,
    f: impl Fn(usize, i64) -> f64,
) -> Vec<RawSample> {
    let series: Vec<_> = services
        .iter()
        .map(|service| {
            Arc::new(BTreeMap::from([
                ("__name__".to_string(), metric.to_string()),
                ("service".to_string(), service.to_string()),
            ]))
        })
        .collect();
    let f = &f;
    (from..to)
        .step_by(15_000)
        .flat_map(|t| {
            series.iter().enumerate().map(move |(i, labels)| RawSample {
                labels: labels.clone(),
                timestamp_ms: t,
                value: f(i, t),
            })
        })
        .collect()
}

/// What a deployment fetches for each raw range: its metric's samples in
/// `(start, end]`.
fn fetch(ranges: &[RawRange], all: &[RawSample]) -> BTreeMap<u64, Vec<RawSample>> {
    ranges
        .iter()
        .map(|range| {
            let samples = all
                .iter()
                .filter(|s| {
                    s.labels["__name__"] == range.metric
                        && range.start_ms < s.timestamp_ms
                        && s.timestamp_ms <= range.end_ms
                })
                .cloned()
                .collect();
            (range.node, samples)
        })
        .collect()
}

fn sorted(mut samples: Vec<ResultSample>) -> Vec<ResultSample> {
    samples.sort_by(|a, b| a.labels.cmp(&b.labels));
    samples
}

/// `query` at `time_ms` as an instant query.
fn instant(
    engine: &mut QueryEngine,
    query: &str,
    time_ms: i64,
    store: &SummaryStore<Batch>,
    all: &[RawSample],
) -> Vec<ResultSample> {
    let (root, ranges) = engine.raw_ranges(query, time_ms, store).unwrap();
    sorted(
        engine
            .evaluate(root, time_ms, store, None, &fetch(&ranges, all))
            .unwrap(),
    )
}

/// `query` at every step as one range query once every sample in `all`
/// has arrived; also returns its raw ranges.
fn range(
    engine: &mut QueryEngine,
    query: &str,
    steps: &[i64],
    store: &SummaryStore<Batch>,
    all: &[RawSample],
) -> (Vec<Vec<ResultSample>>, Vec<RawRange>) {
    let watermark_ms = all.iter().map(|s| s.timestamp_ms).max();
    range_at_watermark(engine, query, steps, store, watermark_ms, all)
}

/// `query` at every step as one range query when the watermark is
/// `watermark_ms`; also returns its raw ranges.
fn range_at_watermark(
    engine: &mut QueryEngine,
    query: &str,
    steps: &[i64],
    store: &SummaryStore<Batch>,
    watermark_ms: Option<i64>,
    all: &[RawSample],
) -> (Vec<Vec<ResultSample>>, Vec<RawRange>) {
    let (root, ranges) = engine.raw_ranges_for_steps(query, steps, store).unwrap();
    let results = engine
        .evaluate_steps(root, steps, store, watermark_ms, &fetch(&ranges, all))
        .unwrap();
    (results.into_iter().map(sorted).collect(), ranges)
}

fn minutes(range: std::ops::RangeInclusive<i64>) -> Vec<i64> {
    range.map(|m| m * MINUTE).collect()
}

/// Example 1 runs at query time from raw samples. A range query fetches
/// each raw input once, `(start - 1m, end]`, and every step answers like
/// the same instant query.
#[test]
fn raw_samples_are_fetched_once_and_sliced_per_step() {
    let plan = common::plan("example1", true);
    let query = "sum by (job) (rate(http_requests_total[1m]))";
    let series: Vec<_> = [("a", "1"), ("a", "2"), ("b", "1")]
        .into_iter()
        .map(|(job, instance)| {
            Arc::new(BTreeMap::from([
                ("__name__".to_string(), "http_requests_total".to_string()),
                ("job".to_string(), job.to_string()),
                ("instance".to_string(), instance.to_string()),
            ]))
        })
        .collect();
    let all: Vec<RawSample> = (0..10 * MINUTE)
        .step_by(15_000)
        .flat_map(|t| {
            series.iter().enumerate().map(move |(i, labels)| RawSample {
                labels: labels.clone(),
                timestamp_ms: t,
                // Rates change over time, so each step's answer differs.
                value: (i as f64 + 1.0) * (t as f64 / 1000.0).powi(2),
            })
        })
        .collect();
    let store = SummaryStore::new();
    let mut engine = QueryEngine::new(&plan, &sds(&plan));
    // Steps 30 s apart in [3 min, 6 min], inclusive.
    let steps: Vec<i64> = (6..=12).map(|k| k * 30_000).collect();
    let (results, ranges) = range(&mut engine, query, &steps, &store, &all);
    assert_eq!(
        ranges
            .iter()
            .map(|r| (r.metric.as_str(), r.start_ms, r.end_ms))
            .collect::<Vec<_>>(),
        vec![("http_requests_total", 2 * MINUTE, 6 * MINUTE)],
        "one fetch covers every step"
    );
    assert_eq!(results.len(), steps.len());
    for (t, result) in steps.iter().zip(&results) {
        assert_eq!(result.len(), 2, "two jobs at {t}");
        assert_eq!(
            result,
            &instant(&mut engine, query, *t, &store, &all),
            "{t}"
        );
    }
    assert_ne!(results[0], results[1]);
}

/// Example 4b keeps no raw data: every step reads six stored 10-minute
/// panes, no raw samples, and answers like the same instant query. Steps
/// off the pane grid, or reading an evicted pane, forward the whole query.
#[test]
fn every_step_reads_stored_panes() {
    // Declared as range requests over 10 minutes, so the Planner keeps the
    // panes the first step reads.
    let workload = common::with_range_evaluation(&common::workload("example4b"), 600_000, 600_000);
    let plan = control_plane::plan_workload(&workload, &common::inputs(false)).unwrap();
    let query = "quantile_over_time(0.99, latency_ms[60m])";
    let sds = sds(&plan);
    let mut precompute = Precompute::new(&plan, &sds, 0).unwrap();
    // Values grow with time, so each hour's p99 differs.
    let all = samples("latency_ms", &["a", "b"], MINUTE, 101 * MINUTE, |i, t| {
        (i * 1000) as f64 + (t / 15_000) as f64
    });
    precompute.ingest(&all);
    let store = precompute.store();
    let mut engine = QueryEngine::new(&plan, &sds);
    let steps = minutes(9..=10)
        .into_iter()
        .map(|t| t * 10)
        .collect::<Vec<_>>();
    let (results, ranges) = range(&mut engine, query, &steps, store, &[]);
    assert!(
        ranges.is_empty(),
        "every input is a stored pane: {ranges:?}"
    );
    for (t, result) in steps.iter().zip(&results) {
        assert_eq!(result.len(), 2);
        assert_eq!(result, &instant(&mut engine, query, *t, store, &[]), "{t}");
    }
    assert_ne!(results[0], results[1]);

    let missing = |steps: &[i64]| match engine.raw_ranges_for_steps(query, steps, store) {
        Err(Unanswered::MissingPane(reason)) => reason,
        other => panic!("{steps:?}: {:?}", other.map(|(_, r)| r)),
    };
    // A step 5 minutes apart lands off the 10-minute grid.
    assert!(missing(&[90 * MINUTE, 95 * MINUTE]).contains("pane grid"));
    // 80 min reads the pane ending at 30 min, already evicted: the whole
    // range is forwarded even though its later steps could be answered.
    assert!(missing(
        &minutes(8..=10)
            .into_iter()
            .map(|t| t * 10)
            .collect::<Vec<_>>()
    )
    .contains("not stored"));
}

/// #509 Example 4's B3: five 1-minute KLL panes built at query time and
/// kept. A range query over 10..14 min, a step a minute, fetches the raw
/// samples of all its panes once, builds each of the 9 panes once and reuses
/// it at the later steps; every step answers like the Planner executor.
/// Kept panes are evicted as for instant evaluations, measured from the
/// newest step: the same range query again rebuilds the panes ending at
/// 6..9 min, which start before 14 min minus the plan's 5-minute retention.
#[test]
fn kept_panes_are_shared_across_steps() {
    let workload = common::workload("example4-b3");
    let inputs = common::inputs(true);
    let plan = control_plane::plan_workload(&workload, &inputs).unwrap();
    let reference_plan = plan.query_time_reference().unwrap();
    let query = "quantile_over_time(0.99, latency_ms[5m])";
    let all = samples("latency_ms", &["a", "b"], 0, 30 * MINUTE + 1, |i, t| {
        (i * 100) as f64 + (t / 15_000 * 25 % 101) as f64
    });
    let store = SummaryStore::new();
    let mut engine = QueryEngine::new(&plan, &sds(&plan));
    let mut reference = QueryEngine::new(&reference_plan, &sds(&reference_plan));
    let steps = minutes(10..=14);

    let (results, ranges) = range(&mut engine, query, &steps, &store, &all);
    assert_eq!(
        ranges
            .iter()
            .map(|r| (r.start_ms, r.end_ms))
            .collect::<Vec<_>>(),
        vec![(5 * MINUTE, 14 * MINUTE)],
        "one fetch for the kept panes of every step"
    );
    for (t, result) in steps.iter().zip(&results) {
        assert_eq!(result.len(), 2);
        assert_eq!(
            result,
            &instant(&mut reference, query, *t, &store, &all),
            "{t}"
        );
    }
    assert_ne!(results[0], results[1]);
    // Panes ending at 6..14 min are built once; each step after the first
    // reuses four of its five.
    assert_eq!(
        engine.kept.stats(),
        KeptResultStats {
            built: 9,
            reused: 16
        }
    );

    let (again, ranges) = range(&mut engine, query, &steps, &store, &all);
    assert_eq!(
        ranges
            .iter()
            .map(|r| (r.start_ms, r.end_ms))
            .collect::<Vec<_>>(),
        vec![(5 * MINUTE, 9 * MINUTE)],
        "only the evicted panes are fetched"
    );
    assert_eq!(again, results);
    // Panes 6..9 start before the newest evaluation's retention (14 min
    // minus 5), so each step reading one rebuilds it: 4 at 10 min, 3 at 11,
    // 2 at 12, 1 at 13.
    assert_eq!(
        engine.kept.stats(),
        KeptResultStats {
            built: 19,
            reused: 31
        }
    );
}

/// A pane still open at the request's watermark is computed for each step
/// that reads it and not kept: with the watermark at 12 min, the panes
/// ending at 13 and 14 min are recomputed at every step instead of reused,
/// and the answers still match the Planner executor.
#[test]
fn open_panes_are_recomputed_at_each_step() {
    let workload = common::workload("example4-b3");
    let inputs = common::inputs(true);
    let plan = control_plane::plan_workload(&workload, &inputs).unwrap();
    let reference_plan = plan.query_time_reference().unwrap();
    let query = "quantile_over_time(0.99, latency_ms[5m])";
    let all = samples("latency_ms", &["a", "b"], 0, 30 * MINUTE + 1, |i, t| {
        (i * 100) as f64 + (t / 15_000 * 25 % 101) as f64
    });
    let store = SummaryStore::new();
    let mut engine = QueryEngine::new(&plan, &sds(&plan));
    let mut reference = QueryEngine::new(&reference_plan, &sds(&reference_plan));
    let steps = minutes(10..=14);
    let (results, _) =
        range_at_watermark(&mut engine, query, &steps, &store, Some(12 * MINUTE), &all);
    for (t, result) in steps.iter().zip(&results) {
        assert_eq!(
            result,
            &instant(&mut reference, query, *t, &store, &all),
            "{t}"
        );
    }
    // Panes 6..12 are built once and kept; 13 is built at 13 and 14 min, 14
    // at 14 min. Reused: four panes at 11, 12 and 13 min, three at 14.
    assert_eq!(
        engine.kept.stats(),
        KeptResultStats {
            built: 10,
            reused: 15
        }
    );
    let definition = sds(&plan).kept_definitions.keys().next().unwrap().clone();
    let kept_ends: Vec<i64> = (0..=30)
        .filter(|m| engine.kept.panes.get(&definition, m * MINUTE).is_some())
        .collect();
    // 6..9 are evicted: they start before the newest step (14 min) minus
    // the plan's 5-minute retention.
    assert_eq!(kept_ends, vec![10, 11, 12]);
}
