//! Ingestion updates each open pane's summary state per sample, through the
//! executor's `StateColumnUpdater`, and builds the pane from columns; the
//! panes it stores equal the plan's own build over the same samples.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use asap_executor::planner::ir::schema::{FieldDataType, SketchStatistic};
use asap_executor::summary_kernels::exact::ExactAccumulator;
use asap_executor::values::{Batch, Value};
use data_plane::precompute::{Precompute, RawSample};
use data_plane::sds::Sds;

const MINUTE: i64 = 60_000;

/// Samples every 15 s in `[from, to)` of `metric` for each `job`; values
/// grow with time and differ by job.
fn samples(metric: &str, jobs: usize, from: i64, to: i64) -> Vec<RawSample> {
    let series: Vec<_> = (0..jobs)
        .map(|j| {
            Arc::new(BTreeMap::from([
                ("__name__".to_string(), metric.to_string()),
                ("job".to_string(), format!("job{j}")),
                ("instance".to_string(), format!("{}", j % 3)),
            ]))
        })
        .collect();
    (from..to)
        .step_by(15_000)
        .flat_map(|t| {
            series.iter().enumerate().map(move |(j, labels)| RawSample {
                labels: labels.clone(),
                timestamp_ms: t,
                value: (j * 1000) as f64 + (t / 15_000 % 97) as f64 + t as f64 / 1e6,
            })
        })
        .collect()
}

/// The state in `value`, as comparable data: an exact accumulator's
/// serialized bytes, or a quantile sketch's quantiles at every percent.
/// KLL seeds its compaction randomly per sketch, so two builds of the same
/// samples agree on content, not on bytes; below k they hold every sample.
fn state_of(value: &Value) -> Vec<u8> {
    let Value::Summary { family, state } = value else {
        panic!("not a state: {value:?}");
    };
    if let Some(exact) = state.as_any().downcast_ref::<ExactAccumulator>() {
        return serde_json::to_vec(exact).unwrap();
    }
    assert!(matches!(family, FieldDataType::Sketch(..)), "{family:?}");
    (1..100)
        .flat_map(|p| {
            let quantile = SketchStatistic::Quantile {
                q: p as f64 / 100.0,
            };
            state.estimate(&quantile).unwrap().to_le_bytes()
        })
        .collect()
}

/// Every cell of `pane` as comparable data, its rows sorted: a pane built
/// on several workers holds one batch per worker. (Tests read rows to
/// compare them; the data plane does not.)
fn cells(pane: &[Batch]) -> Vec<Vec<Vec<u8>>> {
    let mut rows: Vec<Vec<Vec<u8>>> = pane
        .iter()
        .flat_map(Batch::to_rows)
        .map(|row| {
            row.iter()
                .map(|value| match value {
                    Value::Summary { .. } => state_of(value),
                    other => other.key().unwrap(),
                })
                .collect()
        })
        .collect();
    rows.sort();
    rows
}

/// For Example 1's counter rates and sums over 10 s panes, Example 3b's
/// 1-minute KLL panes and Example 4b's 10-minute KLL panes, on a deployment
/// without raw data: every pane built per sample, on one shard or four,
/// equals the executor's batch build of the same samples, row by row.
#[test]
fn per_sample_panes_equal_the_batch_build() {
    for (example, metric, to) in [
        ("example1", "http_requests_total", 12 * MINUTE),
        ("example3b", "latency_ms", 12 * MINUTE),
        ("example4b", "latency_ms", 75 * MINUTE),
    ] {
        let plan = common::plan(example, false);
        let sds = Sds::from_plan(&plan).unwrap();
        let all = samples(metric, 7, MINUTE, to + 1);
        let mut batch = Precompute::batch(&plan, &sds, 0).unwrap();
        batch.ingest(&all);
        assert_eq!(batch.per_sample_definitions(), 0);
        for shards in [1, 4] {
            let mut per_sample = Precompute::with_shards(&plan, &sds, 0, shards).unwrap();
            assert_eq!(
                per_sample.per_sample_definitions(),
                sds.definitions.len(),
                "{example}: every definition is built per sample"
            );
            per_sample.ingest(&all);
            assert_eq!(per_sample.stats(), batch.stats(), "{example}");
            let mut compared = 0;
            for definition in sds.definitions.values() {
                let mut end = 0;
                while end <= to {
                    let expected = batch.store().get(&definition.id, end);
                    let actual = per_sample.store().get(&definition.id, end);
                    assert_eq!(actual.is_some(), expected.is_some(), "{example} pane {end}");
                    if let (Some(actual), Some(expected)) = (actual, expected) {
                        for batch in actual.iter().chain(expected.iter()) {
                            assert_eq!(**batch.schema(), definition.output_schema);
                        }
                        assert_eq!(cells(&actual), cells(&expected), "{example} pane {end}");
                        compared += usize::from(expected.iter().any(|b| b.num_rows() > 0));
                    }
                    end += definition.pane_ms;
                }
            }
            assert!(compared > 0, "{example}: no pane compared");
        }
    }
}

/// The plan's label matchers apply per sample as in the batch build: only
/// the selected series get a state.
#[test]
fn per_sample_panes_apply_label_matchers() {
    let mut workload = common::workload("example4b");
    workload.query_workload.repeating_queries.as_mut().unwrap()[0].query =
        asap_executor::planner::workload::Query(
            "quantile_over_time(0.99, latency_ms{instance=\"1\"}[60m])".into(),
        );
    let plan = control_plane::plan_workload(&workload, &common::inputs(false)).unwrap();
    let sds = Sds::from_plan(&plan).unwrap();
    let id = sds.definitions.keys().next().unwrap().clone();
    let all = samples("latency_ms", 7, MINUTE, 25 * MINUTE);
    let mut per_sample = Precompute::new(&plan, &sds, 0).unwrap();
    assert_eq!(per_sample.per_sample_definitions(), 1);
    per_sample.ingest(&all);
    let mut batch = Precompute::batch(&plan, &sds, 0).unwrap();
    batch.ingest(&all);
    let (actual, expected) = (
        per_sample.store().get(&id, 20 * MINUTE).unwrap(),
        batch.store().get(&id, 20 * MINUTE).unwrap(),
    );
    assert_eq!(cells(&expected).len(), 2, "jobs 1 and 4 are instance 1");
    assert_eq!(cells(&actual), cells(&expected));
}
