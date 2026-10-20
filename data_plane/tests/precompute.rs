mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use control_plane::{plan_workload, InstalledPlan};
use data_plane::precompute::{pane_end, Precompute, PrecomputeOptions, RawSample};
use data_plane::sds::Sds;
use planner_types::workload::{PlanningWorkload, Query};

const MINUTE: i64 = 60_000;
const PANE: i64 = 10 * MINUTE;

const DAY: i64 = 24 * 60 * MINUTE;
/// The Prometheus stale marker's bits.
const STALE_MARKER: u64 = 0x7ff0_0000_0000_0002;

fn precompute(plan: InstalledPlan) -> (Precompute, Sds) {
    precompute_with(plan, 0)
}

fn precompute_with(plan: InstalledPlan, allowed_lateness_ms: u64) -> (Precompute, Sds) {
    let dag = plan.dag.dag;
    let sds = Sds::from_dag(&dag, &Sds::ingestion_frontier(&dag).unwrap()).unwrap();
    let options = PrecomputeOptions {
        allowed_lateness_ms,
        ..PrecomputeOptions::default()
    };
    (Precompute::new(dag, &sds, options).unwrap(), sds)
}

fn sample(metric: &str, job: &str, timestamp_ms: i64, value: f64) -> RawSample {
    RawSample {
        labels: Arc::new(BTreeMap::from([
            ("__name__".to_string(), metric.to_string()),
            ("job".to_string(), job.to_string()),
        ])),
        timestamp_ms,
        value,
    }
}

/// One sample every 15 s for each job, at `from`, `from + 15 s`, ... before `to`.
fn samples(metric: &str, jobs: &[&str], from: i64, to: i64) -> Vec<RawSample> {
    (from..to)
        .step_by(15_000)
        .flat_map(|t| jobs.iter().map(move |job| sample(metric, job, t, t as f64)))
        .collect()
}

/// Panes are built once the watermark passes them, one state per series.
/// The first pane seen may be partial, so it is skipped.
#[test]
fn builds_panes_after_the_first_as_the_watermark_passes() {
    let (mut engine, sds) = precompute(common::plan("example4b", false));
    let id = sds.definitions.keys().next().unwrap().clone();
    // The first sample, at 1 min, opens pane (0, 10 min], which is skipped.
    engine.ingest(&samples("latency_ms", &["a", "b"], MINUTE, 35 * MINUTE));
    assert_eq!(engine.store.get(&id, PANE).map(|_| ()), None, "first pane");
    for end in [2 * PANE, 3 * PANE] {
        let pane = engine.store.get(&id, end).expect("built pane");
        assert_eq!(pane.num_rows(), 2, "one state per series");
    }
    assert_eq!(
        engine.store.get(&id, 4 * PANE).map(|_| ()),
        None,
        "open pane"
    );
    let stats = engine.stats;
    assert_eq!(stats.panes_built, 2);
    assert_eq!(stats.panes_failed, 0);
    // Samples in (0, 10 min] are dropped with the first pane.
    assert_eq!(stats.samples_skipped_first_pane, 2 * 37);
    assert_eq!(stats.samples_dropped_late, 0);
    assert_eq!(stats.samples_accepted, 2 * 99);
}

/// Samples of other metrics are ignored; samples for a built pane are dropped.
#[test]
fn ignores_other_metrics_and_drops_late_samples() {
    let (mut engine, _) = precompute(common::plan("example4b", false));
    engine.ingest(&samples("latency_ms", &["a"], 0, 25 * MINUTE));
    let before = engine.stats;
    engine.ingest(&[sample("other", "a", 25 * MINUTE, 1.0)]);
    assert_eq!(engine.stats.samples_unrouted, before.samples_unrouted + 1);
    assert_eq!(engine.stats.samples_accepted, before.samples_accepted);
    engine.ingest(&[sample("latency_ms", "a", 15 * MINUTE, 1.0)]);
    assert_eq!(
        engine.stats.samples_dropped_late,
        before.samples_dropped_late + 1
    );
    assert_eq!(
        engine.stats.samples_skipped_first_pane,
        before.samples_skipped_first_pane
    );
}

/// A pane without samples is stored empty, so a query can tell "no data"
/// from "not built".
#[test]
fn stores_empty_panes() {
    let (mut engine, sds) = precompute(common::plan("example4b", false));
    let id = sds.definitions.keys().next().unwrap().clone();
    engine.ingest(&samples("latency_ms", &["a"], 0, 20 * MINUTE));
    engine.ingest(&[sample("latency_ms", "a", 45 * MINUTE, 1.0)]);
    assert_eq!(engine.store.get(&id, 2 * PANE).unwrap().num_rows(), 1);
    assert_eq!(engine.store.get(&id, 3 * PANE).unwrap().num_rows(), 0);
    assert_eq!(engine.store.get(&id, 4 * PANE).unwrap().num_rows(), 0);
}

/// The plan's label matchers select which series a pane summarizes.
#[test]
fn applies_label_matchers() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../examples/planner-layering-example4b.json");
    let mut workload: PlanningWorkload =
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    workload.query_workload.repeating_queries.as_mut().unwrap()[0].query =
        Query("quantile_over_time(0.99, latency_ms{job=\"a\"}[60m])".into());
    let (mut engine, sds) = precompute(plan_workload(&workload, &common::inputs(false)).unwrap());
    let id = sds.definitions.keys().next().unwrap().clone();
    engine.ingest(&samples("latency_ms", &["a", "b"], 0, 25 * MINUTE));
    assert_eq!(engine.store.get(&id, 2 * PANE).unwrap().num_rows(), 1);
}

/// Panes are `(end - width, end]`, like PromQL ranges: a sample at a
/// boundary belongs to the pane it ends.
#[test]
fn panes_are_left_open() {
    assert_eq!(pane_end(1, PANE), Some(PANE));
    assert_eq!(pane_end(PANE, PANE), Some(PANE));
    assert_eq!(pane_end(PANE + 1, PANE), Some(2 * PANE));
    assert_eq!(pane_end(0, PANE), Some(0));
    assert_eq!(pane_end(-1, PANE), Some(0));
    assert_eq!(pane_end(-PANE, PANE), Some(-PANE));
}

/// The engine puts a sample at a pane boundary into the pane it ends, and a
/// sample one millisecond later into the next pane.
#[test]
fn engine_assigns_boundary_samples_to_the_pane_they_end() {
    let (mut engine, sds) = precompute(common::plan("example4b", false));
    let id = sds.definitions.keys().next().unwrap().clone();
    engine.ingest(&[
        // Opens the skipped first pane (0, 10 min].
        sample("latency_ms", "a", MINUTE, 1.0),
        sample("latency_ms", "a", 15 * MINUTE, 1.0),
        sample("latency_ms", "b", 2 * PANE, 1.0),
        sample("latency_ms", "c", 2 * PANE + 1, 1.0),
        // Moves the watermark past (20 min, 30 min].
        sample("latency_ms", "a", 45 * MINUTE, 1.0),
    ]);
    // (10 min, 20 min]: series a and b, the one at its end included.
    assert_eq!(engine.store.get(&id, 2 * PANE).unwrap().num_rows(), 2);
    // (20 min, 30 min]: only c, one millisecond after the boundary.
    assert_eq!(engine.store.get(&id, 3 * PANE).unwrap().num_rows(), 1);
}

/// Samples of a metric no definition reads do not move the watermark, even
/// far in the future: the next real sample is still accepted.
#[test]
fn unrouted_future_sample_does_not_close_panes() {
    let (mut engine, _) = precompute(common::plan("example4b", false));
    let now = 30 * MINUTE;
    engine.ingest_at(&samples("latency_ms", &["a"], 0, 25 * MINUTE), now);
    let built = engine.stats.panes_built;
    engine.ingest_at(&[sample("other_metric", "a", 30 * DAY, 1.0)], 30 * DAY);
    assert_eq!(engine.stats.panes_built, built, "no pane closed");
    assert_eq!(engine.stats.samples_unrouted, 1);
    engine.ingest_at(&[sample("latency_ms", "a", 26 * MINUTE, 1.0)], now);
    assert_eq!(engine.stats.samples_dropped_late, 0);
}

/// A routed sample further ahead of the wall clock than `max_future_ms` is
/// dropped and counted, and does not move the watermark.
#[test]
fn drops_samples_beyond_the_future_bound() {
    let (mut engine, _) = precompute(common::plan("example4b", false));
    let now = 30 * MINUTE;
    let bound = PrecomputeOptions::default().max_future_ms as i64;
    engine.ingest_at(&samples("latency_ms", &["a"], 0, 25 * MINUTE), now);
    let built = engine.stats.panes_built;
    engine.ingest_at(&[sample("latency_ms", "a", now + bound + 1, 1.0)], now);
    assert_eq!(engine.stats.samples_dropped_future, 1);
    assert_eq!(engine.stats.panes_built, built);
    // Exactly at the bound is accepted.
    engine.ingest_at(&[sample("latency_ms", "a", now + bound, 1.0)], now);
    assert_eq!(engine.stats.samples_dropped_future, 1);
}

/// When the watermark jumps by 30 days, only the panes still retained are
/// built; the rest are skipped and counted, not built and evicted.
#[test]
fn watermark_jump_builds_only_retained_panes() {
    let (mut engine, sds) = precompute(common::plan("example4b", false));
    let id = sds.definitions.keys().next().unwrap().clone();
    engine.ingest_at(&samples("latency_ms", &["a"], 0, 25 * MINUTE), 30 * DAY);
    let built = engine.stats.panes_built;
    engine.ingest_at(&[sample("latency_ms", "a", 30 * DAY, 1.0)], 30 * DAY);
    // Example 4b reads panes up to 50 min back, plus two panes: 70 min.
    assert_eq!(engine.stats.panes_built - built, 7);
    assert_eq!(
        engine.stats.panes_skipped_expired,
        (30 * DAY - 70 * MINUTE) as u64 / PANE as u64 - 2
    );
    assert!(engine.store.get(&id, 30 * DAY).is_some());
    assert!(engine.store.get(&id, 2 * PANE).is_none());
    assert_eq!(engine.store.len(), 7);
}

/// Timestamps at the ends of `i64` neither overflow nor loop.
#[test]
fn extreme_timestamps_do_not_overflow() {
    assert_eq!(pane_end(i64::MIN, PANE), None);
    assert_eq!(pane_end(i64::MAX, PANE), None);
    let (mut engine, _) = precompute(common::plan("example4b", false));
    engine.ingest_at(
        &[
            sample("latency_ms", "a", i64::MIN, 1.0),
            sample("latency_ms", "a", i64::MIN + 1, 1.0),
            sample("latency_ms", "a", 0, 1.0),
            sample("latency_ms", "a", i64::MAX - 1, 1.0),
            sample("latency_ms", "a", i64::MAX, 1.0),
        ],
        i64::MAX,
    );
    assert!(engine.stats.panes_built <= 7);
}

/// A pane of zero width is refused when the engine is built.
#[test]
fn rejects_zero_width_panes() {
    let plan = common::plan("example4b", false);
    let dag = plan.dag.dag;
    let mut sds = Sds::from_dag(&dag, &Sds::ingestion_frontier(&dag).unwrap()).unwrap();
    for definition in sds.definitions.values_mut() {
        definition.pane_ms = 0;
    }
    assert!(Precompute::new(dag, &sds, PrecomputeOptions::default()).is_err());
}

/// Panes older than the retention (largest offset plus two panes) are
/// evicted as the watermark advances.
#[test]
fn evicts_panes_past_retention() {
    let (mut engine, sds) = precompute(common::plan("example4b", false));
    let id = sds.definitions.keys().next().unwrap().clone();
    engine.ingest(&samples("latency_ms", &["a"], 0, 25 * MINUTE));
    assert!(engine.store.get(&id, 2 * PANE).is_some());
    // Watermark 100 min: panes ending at or before 30 min are evicted.
    engine.ingest(&[sample("latency_ms", "a", 100 * MINUTE, 1.0)]);
    assert!(engine.store.get(&id, 2 * PANE).is_none());
    assert!(engine.store.get(&id, 3 * PANE).is_none());
    assert!(engine.store.get(&id, 4 * PANE).is_some());
    assert!(engine.store.get(&id, 10 * PANE).is_some());
}

/// Out-of-order samples within the allowed lateness reach their pane.
#[test]
fn accepts_out_of_order_samples_within_lateness() {
    let (mut engine, sds) = precompute_with(common::plan("example4b", false), 5 * MINUTE as u64);
    let id = sds.definitions.keys().next().unwrap().clone();
    engine.ingest(&[sample("latency_ms", "a", MINUTE, 1.0)]);
    engine.ingest(&[sample("latency_ms", "a", 24 * MINUTE, 1.0)]);
    // Watermark 19 min: (10, 20 min] is still open.
    engine.ingest(&[sample("latency_ms", "b", 15 * MINUTE, 1.0)]);
    engine.ingest(&[sample("latency_ms", "a", 26 * MINUTE, 1.0)]);
    // Watermark 21 min: (10, 20 min] holds b's late sample.
    assert_eq!(engine.store.get(&id, 2 * PANE).unwrap().num_rows(), 1);
    assert_eq!(engine.stats.samples_dropped_late, 0);
    engine.ingest(&[sample("latency_ms", "c", 18 * MINUTE, 1.0)]);
    assert_eq!(engine.stats.samples_dropped_late, 1);
}

/// Stale markers are not samples: a series whose only sample in a pane is
/// a stale marker has no state, and one with a value and a marker keeps it.
#[test]
fn stale_markers_are_not_samples() {
    let (mut engine, sds) = precompute(common::plan("example4b", false));
    let id = sds.definitions.keys().next().unwrap().clone();
    let stale = f64::from_bits(STALE_MARKER);
    engine.ingest(&[
        sample("latency_ms", "a", MINUTE, 1.0),
        sample("latency_ms", "a", 12 * MINUTE, 1.0),
        sample("latency_ms", "a", 13 * MINUTE, stale),
        sample("latency_ms", "b", 14 * MINUTE, stale),
        sample("latency_ms", "a", 25 * MINUTE, 1.0),
    ]);
    assert_eq!(engine.store.get(&id, 2 * PANE).unwrap().num_rows(), 1);
}
