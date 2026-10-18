mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use asap_executor::physical_planner::frontier_from_timing;
use control_plane::InstalledPlan;
use data_plane::precompute::{Precompute, RawSample};
use data_plane::query::{QueryEngine, RawRange, ResultSample, Unanswered};
use data_plane::sds::Sds;

const MINUTE: i64 = 60_000;

struct Fixture {
    precompute: Precompute,
    query: QueryEngine,
}

fn fixture(plan: InstalledPlan) -> Fixture {
    let dag = &plan.dag.dag;
    let sds = Sds::from_dag(dag, &frontier_from_timing(dag).unwrap()).unwrap();
    Fixture {
        precompute: Precompute::new(dag.clone(), &sds, 0).unwrap(),
        query: QueryEngine::new(&plan, &sds),
    }
}

/// Samples every 15 s in `[from, to)` for each `(metric, labels)`, with
/// value `f(series index, t)`.
fn samples(
    series: &[(&str, &[(&str, &str)])],
    from: i64,
    to: i64,
    f: impl Fn(usize, i64) -> f64,
) -> Vec<RawSample> {
    let series: Vec<_> = series
        .iter()
        .map(|(metric, labels)| {
            let mut map = BTreeMap::from([("__name__".to_string(), metric.to_string())]);
            map.extend(labels.iter().map(|(k, v)| (k.to_string(), v.to_string())));
            Arc::new(map)
        })
        .collect();
    (from..to)
        .step_by(15_000)
        .flat_map(|t| {
            series
                .iter()
                .enumerate()
                .map(move |(i, labels)| (i, labels.clone(), t))
        })
        .map(|(i, labels, timestamp_ms)| RawSample {
            labels,
            timestamp_ms,
            value: f(i, timestamp_ms),
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

fn answer(f: &mut Fixture, query: &str, time_ms: i64, all: &[RawSample]) -> Vec<ResultSample> {
    let (root, ranges) = f
        .query
        .raw_ranges(query, time_ms, &f.precompute.store)
        .unwrap();
    let mut result = f
        .query
        .evaluate(root, time_ms, &f.precompute.store, &fetch(&ranges, all))
        .unwrap();
    result.sort_by(|a, b| a.labels.cmp(&b.labels));
    result
}

const P99: &str = "quantile_over_time(0.99, latency_ms[60m])";

/// Example 4b's p99 merged from six stored 10-minute KLL panes equals one
/// query-time build over the same hour of raw samples (KLL is exact below k).
#[test]
fn stored_panes_answer_like_a_single_build() {
    let series: &[(&str, &[(&str, &str)])] = &[
        ("latency_ms", &[("job", "a")]),
        ("latency_ms", &[("job", "b")]),
    ];
    // Values cycle through 0..240 per series, offset by 1000 for series b.
    let all = samples(series, MINUTE, 75 * MINUTE, |i, t| {
        (i * 1000) as f64 + ((t / 15_000) % 240) as f64
    });
    let mut panes = fixture(common::plan("example4b", false));
    assert!(
        panes
            .query
            .raw_ranges(P99, 70 * MINUTE, &panes.precompute.store)
            .is_err(),
        "no panes before ingestion"
    );
    panes.precompute.ingest(&all);
    let (_, ranges) = panes
        .query
        .raw_ranges(P99, 70 * MINUTE, &panes.precompute.store)
        .unwrap();
    assert!(ranges.is_empty(), "every input is a stored pane");
    let from_panes = answer(&mut panes, P99, 70 * MINUTE, &all);

    let mut single = fixture(common::plan("example4b", true));
    let (_, ranges) = single
        .query
        .raw_ranges(P99, 70 * MINUTE, &single.precompute.store)
        .unwrap();
    assert_eq!(
        ranges
            .iter()
            .map(|r| (r.start_ms, r.end_ms))
            .collect::<Vec<_>>(),
        vec![(10 * MINUTE, 70 * MINUTE)],
        "one raw hour, left-open"
    );
    let from_raw = answer(&mut single, P99, 70 * MINUTE, &all);

    assert_eq!(from_panes, from_raw);
    assert_eq!(from_panes.len(), 2);
    for (sample, base) in from_panes.iter().zip([0.0, 1000.0]) {
        assert!(
            sample.value >= base + 236.0 && sample.value <= base + 239.0,
            "{sample:?}"
        );
    }
}

/// Example 1 runs entirely at query time over raw samples: each counter grows
/// by 1 per second, so `sum by (job) (rate(...[1m]))` is the series count.
#[test]
fn query_time_plans_read_raw_ranges() {
    let series: &[(&str, &[(&str, &str)])] = &[
        ("http_requests_total", &[("job", "a"), ("instance", "1")]),
        ("http_requests_total", &[("job", "a"), ("instance", "2")]),
        ("http_requests_total", &[("job", "b"), ("instance", "1")]),
    ];
    let all = samples(series, 0, 10 * MINUTE, |_, t| t as f64 / 1000.0);
    let mut f = fixture(common::plan("example1", true));
    let result = answer(
        &mut f,
        "sum by (job) (rate(http_requests_total[1m]))",
        5 * MINUTE,
        &all,
    );
    let by_job: Vec<(String, f64)> = result
        .iter()
        .map(|s| (s.labels["job"].clone(), s.value))
        .collect();
    assert_eq!(by_job.len(), 2);
    assert_eq!(by_job[0].0, "a");
    assert!((by_job[0].1 - 2.0).abs() < 1e-9, "{by_job:?}");
    assert_eq!(by_job[1].0, "b");
    assert!((by_job[1].1 - 1.0).abs() < 1e-9, "{by_job:?}");

    let topk = answer(
        &mut f,
        "topk by (job) (10, sum_over_time(http_requests_total[1m]))",
        5 * MINUTE,
        &all,
    );
    assert_eq!(
        topk.len(),
        3,
        "every series is in its job's top 10: {topk:?}"
    );
}

/// Queries outside the plan, and stored panes that are missing or off the
/// pane grid, are not answered, so the caller forwards them.
#[test]
fn reports_what_it_cannot_answer() {
    let mut f = fixture(common::plan("example4b", false));
    let store = &f.precompute.store;
    assert!(matches!(
        f.query.raw_ranges("up", 70 * MINUTE, store),
        Err(Unanswered::NotPlanned(_))
    ));
    assert!(matches!(
        f.query.raw_ranges(P99, 70 * MINUTE, store),
        Err(Unanswered::MissingPane(_))
    ));
    let all = samples(&[("latency_ms", &[])], MINUTE, 75 * MINUTE, |_, t| t as f64);
    f.precompute.ingest(&all);
    let store = &f.precompute.store;
    assert!(f.query.raw_ranges(P99, 70 * MINUTE, store).is_ok());
    assert!(matches!(
        f.query.raw_ranges(P99, 70 * MINUTE + 1, store),
        Err(Unanswered::MissingPane(_))
    ));
    // The pane ending at 10 min was skipped as possibly partial.
    assert!(matches!(
        f.query.raw_ranges(P99, 60 * MINUTE, store),
        Err(Unanswered::MissingPane(_))
    ));
}
