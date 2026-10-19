//! Query-time results kept across evaluations (`MatAtQueryKeptAcrossEvaluations`):
//! #509 Example 4's B3, five 1-minute KLL panes built at query time, each
//! kept for the next evaluations of `quantile_over_time(0.99, latency_ms[5m])`
//! every minute.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use asap_planner::InstallablePlan;
use data_plane::precompute::RawSample;
use data_plane::query::{KeptResultStats, QueryEngine, RawRange, ResultSample};
use data_plane::sds::{Sds, SummaryStore};

const MINUTE: i64 = 60_000;
const P99: &str = "quantile_over_time(0.99, latency_ms[5m])";

fn engine(plan: &InstallablePlan) -> QueryEngine {
    let sds = Sds::from_plan(plan).unwrap();
    assert!(sds.definitions.is_empty(), "nothing is built at ingestion");
    QueryEngine::new(plan, &sds)
}

/// Two series every 15 s in `[0, 30 min]`, the `n`th sample `25n mod 101`
/// (plus 100 for series b): each 5-minute window's p99 falls in an older
/// pane and changes every minute. KLL is exact below k.
fn samples() -> Vec<RawSample> {
    let series: Vec<_> = ["a", "b"]
        .iter()
        .map(|service| {
            Arc::new(BTreeMap::from([
                ("__name__".to_string(), "latency_ms".to_string()),
                ("service".to_string(), service.to_string()),
            ]))
        })
        .collect();
    (0..=30 * MINUTE)
        .step_by(15_000)
        .flat_map(|t| {
            series.iter().enumerate().map(move |(i, labels)| RawSample {
                labels: labels.clone(),
                timestamp_ms: t,
                value: (i * 100) as f64 + (t / 15_000 * 25 % 101) as f64,
            })
        })
        .collect()
}

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

/// Answers `P99` at `time_ms` once every sample in `all` has arrived (every
/// pane is closed), returning the answer and the raw ranges it read.
fn answer(
    engine: &mut QueryEngine,
    time_ms: i64,
    all: &[RawSample],
) -> (Vec<ResultSample>, Vec<RawRange>) {
    answer_at_watermark(engine, time_ms, Some(30 * MINUTE), all)
}

/// Answers `P99` at `time_ms` when the watermark is `watermark_ms`.
fn answer_at_watermark(
    engine: &mut QueryEngine,
    time_ms: i64,
    watermark_ms: Option<i64>,
    all: &[RawSample],
) -> (Vec<ResultSample>, Vec<RawRange>) {
    let store = SummaryStore::new();
    let (root, ranges) = engine.raw_ranges(P99, time_ms, &store).unwrap();
    let mut result = engine
        .evaluate(root, time_ms, &store, watermark_ms, &fetch(&ranges, all))
        .unwrap();
    result.sort_by(|a, b| a.labels.cmp(&b.labels));
    (result, ranges)
}

/// The installed B3 plan and the Planner executor's reference for it: the
/// same selection with nothing materialized.
fn plans() -> (InstallablePlan, InstallablePlan) {
    let plan = common::plan("example4-b3", true);
    let reference = plan.query_time_reference().unwrap();
    (plan, reference)
}

/// The first evaluation builds all five kept panes and answers like the
/// reference; the next one, a minute later, builds only the newest pane,
/// reads only its minute of raw samples, reuses the other four, and still
/// answers like the reference.
#[test]
fn kept_panes_answer_like_the_reference_and_are_reused() {
    let (plan, reference_plan) = plans();
    let all = samples();
    let mut kept = engine(&plan);
    let mut reference = engine(&reference_plan);

    let (first, ranges) = answer(&mut kept, 10 * MINUTE, &all);
    assert_eq!(ranges.len(), 5, "every pane is built: {ranges:?}");
    assert_eq!(first, answer(&mut reference, 10 * MINUTE, &all).0);
    assert_eq!(first.len(), 2);
    assert_eq!(
        kept.kept.stats(),
        KeptResultStats {
            built: 5,
            reused: 0
        }
    );

    let (second, ranges) = answer(&mut kept, 11 * MINUTE, &all);
    assert_eq!(
        ranges
            .iter()
            .map(|r| (r.start_ms, r.end_ms))
            .collect::<Vec<_>>(),
        vec![(10 * MINUTE, 11 * MINUTE)],
        "only the newest pane is read"
    );
    assert_eq!(second, answer(&mut reference, 11 * MINUTE, &all).0);
    assert_ne!(second, first, "the answer moves with the window");
    assert_eq!(
        kept.kept.stats(),
        KeptResultStats {
            built: 6,
            reused: 4
        }
    );

    // The same evaluation again recomputes nothing.
    let (again, ranges) = answer(&mut kept, 11 * MINUTE, &all);
    assert!(ranges.is_empty());
    assert_eq!(again, second);
    assert_eq!(
        kept.kept.stats(),
        KeptResultStats {
            built: 6,
            reused: 9
        }
    );
    assert_eq!(
        reference.kept.stats(),
        KeptResultStats::default(),
        "the reference keeps nothing"
    );
}

/// A kept pane is dropped once it starts before the plan's retention (5
/// min: the oldest output's offset, 4 min, plus its 1-minute pane), measured
/// from the newest evaluation.
#[test]
fn kept_panes_are_evicted_like_stored_panes() {
    let (plan, _) = plans();
    let sds = Sds::from_plan(&plan).unwrap();
    let definition = sds.kept_definitions.keys().next().unwrap().clone();
    let all = samples();
    let mut kept = engine(&plan);
    let ends = |engine: &QueryEngine| {
        (0..=30)
            .map(|m| m * MINUTE)
            .filter(|end| engine.kept.panes.get(&definition, *end).is_some())
            .map(|end| end / MINUTE)
            .collect::<Vec<_>>()
    };

    answer(&mut kept, 10 * MINUTE, &all);
    assert_eq!(ends(&kept), vec![6, 7, 8, 9, 10]);
    answer(&mut kept, 11 * MINUTE, &all);
    assert_eq!(
        ends(&kept),
        vec![7, 8, 9, 10, 11],
        "the pane ending at 6 min starts before 11 - 5"
    );
    answer(&mut kept, 12 * MINUTE, &all);
    assert_eq!(ends(&kept), vec![8, 9, 10, 11, 12]);
    // An evaluation far ahead builds its own five panes and drops the rest.
    answer(&mut kept, 20 * MINUTE, &all);
    assert_eq!(ends(&kept), vec![16, 17, 18, 19, 20]);
    // An older evaluation is answered, but its panes are not kept.
    let (old, ranges) = answer(&mut kept, 12 * MINUTE, &all);
    assert_eq!(ranges.len(), 5);
    assert_eq!(old.len(), 2);
    assert_eq!(ends(&kept), vec![16, 17, 18, 19, 20]);
    assert_eq!(kept.kept.panes.len(), 5);
}

/// A pane is kept only once it is closed (its end at or before the
/// watermark). A pane read before then is computed for that evaluation but
/// not kept, so a later evaluation computes it again and includes a sample
/// that arrived late, before the pane closed.
#[test]
fn panes_are_kept_only_once_closed() {
    let (plan, reference_plan) = plans();
    let sds = Sds::from_plan(&plan).unwrap();
    let definition = sds.kept_definitions.keys().next().unwrap().clone();
    let mut all = samples();
    let mut kept = engine(&plan);
    let mut reference = engine(&reference_plan);
    let ends = |engine: &QueryEngine| {
        (0..=30)
            .map(|m| m * MINUTE)
            .filter(|end| engine.kept.panes.get(&definition, *end).is_some())
            .map(|end| end / MINUTE)
            .collect::<Vec<_>>()
    };

    // Watermark at 9.5 min: the pane ending at 10 min is still open.
    let (first, ranges) = answer_at_watermark(&mut kept, 10 * MINUTE, Some(570_000), &all);
    assert_eq!(ranges.len(), 5);
    assert_eq!(first, answer(&mut reference, 10 * MINUTE, &all).0);
    assert_eq!(ends(&kept), vec![6, 7, 8, 9], "the open pane is not kept");

    // A late sample for the open pane arrives before it closes.
    let late = RawSample {
        timestamp_ms: 10 * MINUTE - 1_000,
        value: 1_000.0,
        ..all
            .iter()
            .find(|s| s.labels["service"] == "a")
            .unwrap()
            .clone()
    };
    all.push(late);
    let (again, ranges) = answer_at_watermark(&mut kept, 10 * MINUTE, Some(585_000), &all);
    assert_eq!(
        ranges
            .iter()
            .map(|r| (r.start_ms, r.end_ms))
            .collect::<Vec<_>>(),
        vec![(9 * MINUTE, 10 * MINUTE)],
        "the open pane is computed again"
    );
    assert_eq!(again, answer(&mut reference, 10 * MINUTE, &all).0);
    assert_ne!(again, first, "the late sample is included");

    // Once the watermark reaches 10 min, the pane is kept.
    let (_, ranges) = answer_at_watermark(&mut kept, 11 * MINUTE, Some(10 * MINUTE), &all);
    assert_eq!(ranges.len(), 2, "the 10- and 11-minute panes: {ranges:?}");
    assert_eq!(ends(&kept), vec![7, 8, 9, 10], "6 starts before 11 - 5");
    // Without any sample there is no watermark, and nothing is kept.
    let mut fresh = engine(&plan);
    answer_at_watermark(&mut fresh, 10 * MINUTE, None, &all);
    assert!(fresh.kept.panes.is_empty());
}
