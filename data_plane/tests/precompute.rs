mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use asap_executor::planner::workload::{PlanningWorkload, Query};
use asap_planner::InstallablePlan;
use control_plane::plan_workload;
use data_plane::precompute::{pane_end, Precompute, RawSample};
use data_plane::sds::Sds;

const MINUTE: i64 = 60_000;
const PANE: i64 = 10 * MINUTE;

fn precompute(plan: InstallablePlan) -> (Precompute, Sds) {
    let sds = Sds::from_plan(&plan).unwrap();
    (Precompute::new(&plan, &sds, 0).unwrap(), sds)
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
        assert_eq!(pane.rows().len(), 2, "one state per series");
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
    assert_eq!(stats.dropped, 2 * 37);
    assert_eq!(stats.accepted, 2 * 99);
}

/// Samples of other metrics are ignored; samples for a built pane are dropped.
#[test]
fn ignores_other_metrics_and_drops_late_samples() {
    let (mut engine, _) = precompute(common::plan("example4b", false));
    engine.ingest(&samples("latency_ms", &["a"], 0, 25 * MINUTE));
    let before = engine.stats;
    engine.ingest(&[sample("other", "a", 25 * MINUTE, 1.0)]);
    assert_eq!(engine.stats, before);
    engine.ingest(&[sample("latency_ms", "a", 15 * MINUTE, 1.0)]);
    assert_eq!(engine.stats.dropped, before.dropped + 1);
}

/// A pane without samples is stored empty, so a query can tell "no data"
/// from "not built".
#[test]
fn stores_empty_panes() {
    let (mut engine, sds) = precompute(common::plan("example4b", false));
    let id = sds.definitions.keys().next().unwrap().clone();
    engine.ingest(&samples("latency_ms", &["a"], 0, 20 * MINUTE));
    engine.ingest(&[sample("latency_ms", "a", 45 * MINUTE, 1.0)]);
    assert_eq!(engine.store.get(&id, 2 * PANE).unwrap().rows().len(), 1);
    assert_eq!(engine.store.get(&id, 3 * PANE).unwrap().rows().len(), 0);
    assert_eq!(engine.store.get(&id, 4 * PANE).unwrap().rows().len(), 0);
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
    assert_eq!(engine.store.get(&id, 2 * PANE).unwrap().rows().len(), 1);
}

/// Panes are `(end - width, end]`, like PromQL ranges: a sample at a
/// boundary belongs to the pane it ends.
#[test]
fn panes_are_left_open() {
    assert_eq!(pane_end(1, PANE), PANE);
    assert_eq!(pane_end(PANE, PANE), PANE);
    assert_eq!(pane_end(PANE + 1, PANE), 2 * PANE);
    assert_eq!(pane_end(0, PANE), 0);
    assert_eq!(pane_end(-1, PANE), 0);
    assert_eq!(pane_end(-PANE, PANE), -PANE);
}

/// A pane stays stored while an output reads it: Example 4b's oldest output
/// reads the pane ending 50 min before the evaluation, so with the watermark
/// at 90 min the pane ending at 40 min is kept and the one at 30 min, which
/// starts before the plan's 60-minute retention, is evicted.
#[test]
fn keeps_panes_for_the_plans_retention() {
    let (mut engine, sds) = precompute(common::plan("example4b", false));
    let definition = sds.definitions.values().next().unwrap();
    assert_eq!(definition.retention_ms, 60 * MINUTE);
    let id = definition.id.clone();
    engine.ingest(&samples("latency_ms", &["a"], MINUTE, 90 * MINUTE + 1));
    assert!(
        engine.store.get(&id, 4 * PANE).is_some(),
        "read at offset 50 min"
    );
    assert!(
        engine.store.get(&id, 3 * PANE).is_none(),
        "older than the retention"
    );
}
