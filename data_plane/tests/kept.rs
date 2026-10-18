//! Query-time results kept across evaluations (`MatAtQueryKeptAcrossEvaluations`):
//! #509 Example 4's B3, five 1-minute KLL panes built at query time, each
//! kept for the next evaluations of `quantile_over_time(0.99, latency_ms[5m])`
//! every minute.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use asap_executor::physical_planner::frontier_from_timing;
use control_plane::{plan_selected_at_query_time, InstalledPlan};
use data_plane::precompute::RawSample;
use data_plane::query::{KeptResultStats, QueryEngine, RawRange, ResultSample};
use data_plane::sds::{Sds, SummaryStore};

const MINUTE: i64 = 60_000;
const P99: &str = "quantile_over_time(0.99, latency_ms[5m])";

fn engine(plan: &InstalledPlan) -> QueryEngine {
    let dag = &plan.dag.dag;
    let sds = Sds::from_dag(dag, &frontier_from_timing(dag).unwrap()).unwrap();
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

/// Answers `P99` at `time_ms`, returning the answer and the raw ranges it read.
fn answer(
    engine: &mut QueryEngine,
    time_ms: i64,
    all: &[RawSample],
) -> (Vec<ResultSample>, Vec<RawRange>) {
    let store = SummaryStore::new();
    let (root, ranges) = engine.raw_ranges(P99, time_ms, &store).unwrap();
    let mut result = engine
        .evaluate(root, time_ms, &store, &fetch(&ranges, all))
        .unwrap();
    result.sort_by(|a, b| a.labels.cmp(&b.labels));
    (result, ranges)
}

/// The installed B3 plan and the Planner executor's reference for it: the
/// same selection with nothing materialized.
fn plans() -> (InstalledPlan, InstalledPlan) {
    let workload = common::workload("example4-b3");
    let inputs = common::inputs(true);
    (
        control_plane::plan_workload(&workload, &inputs).unwrap(),
        plan_selected_at_query_time(&workload, &inputs).unwrap(),
    )
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
        kept.kept_stats,
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
        kept.kept_stats,
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
        kept.kept_stats,
        KeptResultStats {
            built: 6,
            reused: 9
        }
    );
    assert_eq!(
        reference.kept_stats,
        KeptResultStats::default(),
        "the reference keeps nothing"
    );
}

/// A kept pane is dropped once it is older than the largest offset any
/// output reads (4 min) plus two pane widths, measured from the newest
/// evaluation: 6 min.
#[test]
fn kept_panes_are_evicted_like_stored_panes() {
    let (plan, _) = plans();
    let dag = &plan.dag.dag;
    let sds = Sds::from_dag(dag, &frontier_from_timing(dag).unwrap()).unwrap();
    let definition = sds.kept_definitions.keys().next().unwrap().clone();
    let all = samples();
    let mut kept = engine(&plan);
    let ends = |engine: &QueryEngine| {
        (0..=30)
            .map(|m| m * MINUTE)
            .filter(|end| engine.kept.get(&definition, *end).is_some())
            .map(|end| end / MINUTE)
            .collect::<Vec<_>>()
    };

    answer(&mut kept, 10 * MINUTE, &all);
    assert_eq!(ends(&kept), vec![6, 7, 8, 9, 10]);
    answer(&mut kept, 11 * MINUTE, &all);
    assert_eq!(
        ends(&kept),
        vec![6, 7, 8, 9, 10, 11],
        "6 is not older than 11 - 6"
    );
    answer(&mut kept, 12 * MINUTE, &all);
    assert_eq!(ends(&kept), vec![7, 8, 9, 10, 11, 12]);
    // An evaluation far ahead builds its own five panes and drops the rest.
    answer(&mut kept, 20 * MINUTE, &all);
    assert_eq!(ends(&kept), vec![16, 17, 18, 19, 20]);
    // An older evaluation is answered, but its panes are not kept.
    let (old, ranges) = answer(&mut kept, 12 * MINUTE, &all);
    assert_eq!(ranges.len(), 5);
    assert_eq!(old.len(), 2);
    assert_eq!(ends(&kept), vec![16, 17, 18, 19, 20]);
    assert_eq!(kept.kept.len(), 5);
}
