//! Query-run summaries kept across evaluations
//! (`MatAtQueryKeptAcrossEvaluations`): #509 Example 4's B3, five 1-minute
//! KLL summaries built at query time and stored for the next evaluations of
//! `quantile_over_time(0.99, latency_ms[5m])` every minute.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use asap_executor::values::Batch;
use asap_planner::{InstallablePlan, StoredOutputBuild};
use data_plane::engine::{EngineHandle, EngineOptions};
use data_plane::precompute::RawSample;
use data_plane::query::{IngestionProgress, KeptResultStats, QueryEngine, RawRange, ResultSample};
use data_plane::sds::{DefinitionId, Sds, Selection, SummaryStore};

const MINUTE: i64 = 60_000;
const P99: &str = "quantile_over_time(0.99, latency_ms[5m])";

/// A query engine and the SDS store its query-run summaries go to.
struct Kept {
    engine: QueryEngine,
    store: SummaryStore<Batch>,
    definition: DefinitionId,
}

fn kept(plan: &InstallablePlan) -> Kept {
    let sds = Sds::from_plan(plan).unwrap();
    assert!(
        sds.ingestion_build_order().unwrap().is_empty(),
        "nothing is built at ingestion"
    );
    let definition = sds
        .definitions
        .values()
        .find(|d| d.built_at == StoredOutputBuild::QueryRun)
        .map(|d| d.id.clone())
        .unwrap_or_else(|| DefinitionId(String::new()));
    Kept {
        engine: QueryEngine::new(plan, &sds),
        store: SummaryStore::new(),
        definition,
    }
}

impl Kept {
    /// The ends, in minutes, of the stored query-run summaries.
    fn ends(&self) -> Vec<i64> {
        (0..=30)
            .map(|m| m * MINUTE)
            .filter(|end| {
                self.store
                    .get(&self.definition, Selection::ending_at(*end))
                    .is_some()
            })
            .map(|end| end / MINUTE)
            .collect()
    }
}

/// Two series every 15 s in `[0, 30 min]`, the `n`th sample `25n mod 101`
/// (plus 100 for series b): each 5-minute window's p99 falls in an older
/// pane and changes every minute. KLL is exact below k.
fn samples() -> Vec<RawSample> {
    let series: Vec<_> = ["a", "b"]
        .iter()
        .map(|job| {
            Arc::new(BTreeMap::from([
                ("__name__".to_string(), "latency_ms".to_string()),
                ("job".to_string(), job.to_string()),
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

/// Answers `P99` at `time_ms` with ingestion's watermark at `watermark_ms`
/// (every metric's newest sample there too), returning the answer and the
/// raw ranges it read.
fn answer_at_watermark(
    k: &mut Kept,
    time_ms: i64,
    watermark_ms: Option<i64>,
    all: &[RawSample],
) -> (Vec<ResultSample>, Vec<RawRange>) {
    let progress = IngestionProgress {
        watermark_ms,
        newest_sample_ms: watermark_ms
            .map(|w| BTreeMap::from([("latency_ms".to_string(), w)]))
            .unwrap_or_default(),
    };
    let prepared = k
        .engine
        .raw_ranges(P99, time_ms, &k.store, progress)
        .unwrap();
    let ranges = prepared.ranges.clone();
    let mut result = k
        .engine
        .evaluate(
            prepared.root,
            time_ms,
            &mut k.store,
            &prepared.progress,
            &fetch(&ranges, all),
        )
        .unwrap();
    result.sort_by(|a, b| a.labels.cmp(&b.labels));
    (result, ranges)
}

/// Answers `P99` at `time_ms` once every sample through `time_ms` has
/// arrived: the watermark is `time_ms`.
fn answer(k: &mut Kept, time_ms: i64, all: &[RawSample]) -> (Vec<ResultSample>, Vec<RawRange>) {
    answer_at_watermark(k, time_ms, Some(time_ms), all)
}

fn spans(ranges: &[RawRange]) -> Vec<(i64, i64)> {
    ranges
        .iter()
        .map(|r| (r.start_ms / MINUTE, r.end_ms / MINUTE))
        .collect()
}

/// The installed B3 plan and the Planner's reference for it: the same
/// selection with nothing materialized.
fn plans() -> (InstallablePlan, InstallablePlan) {
    let plan = common::plan("example4-b3", true);
    let reference = plan.query_time_reference().unwrap();
    (plan, reference)
}

/// The first evaluation builds all five summaries from one fetch and
/// answers like the reference; the next one, a minute later, fetches only
/// the newest minute, builds one summary, reuses four, and still answers
/// like the reference.
#[test]
fn kept_summaries_answer_like_the_reference_and_are_reused() {
    let (plan, reference_plan) = plans();
    let all = samples();
    let mut k = kept(&plan);
    let mut reference = kept(&reference_plan);

    let (first, ranges) = answer(&mut k, 10 * MINUTE, &all);
    assert_eq!(
        spans(&ranges),
        vec![(5, 10)],
        "one fetch for five summaries"
    );
    assert_eq!(first, answer(&mut reference, 10 * MINUTE, &all).0);
    assert_eq!(first.len(), 2);
    assert_eq!(
        k.engine.kept_stats(),
        KeptResultStats {
            built: 5,
            kept: 5,
            kept_while_metric_behind: 0,
            reused: 0
        }
    );

    let (second, ranges) = answer(&mut k, 11 * MINUTE, &all);
    assert_eq!(spans(&ranges), vec![(10, 11)], "only the newest summary");
    assert_eq!(second, answer(&mut reference, 11 * MINUTE, &all).0);
    assert_ne!(second, first, "the answer moves with the window");
    assert_eq!(
        (k.engine.kept_stats().built, k.engine.kept_stats().reused),
        (6, 4)
    );

    // The same evaluation again builds nothing.
    let (again, ranges) = answer(&mut k, 11 * MINUTE, &all);
    assert!(ranges.is_empty());
    assert_eq!(again, second);
    assert_eq!(
        (k.engine.kept_stats().built, k.engine.kept_stats().reused),
        (6, 9)
    );
    assert_eq!(
        reference.engine.kept_stats(),
        KeptResultStats::default(),
        "the reference keeps nothing"
    );
}

/// Stored query-run summaries are evicted by the plan's retention (5 min,
/// the oldest summary a 5-minute window reads), plus one pane, measured
/// from the newest closed coverage at or before the watermark, as for
/// ingestion-built summaries. A far-future evaluation does not move that
/// point: its summaries are built but not kept, and keeping resumes.
#[test]
fn kept_summaries_are_evicted_by_the_plans_retention() {
    let (plan, _) = plans();
    let retention = plan.stored_outputs.iter().map(|o| o.retention_ms).max();
    assert_eq!(retention, Some(5 * MINUTE as u64));
    let all = samples();
    let mut k = kept(&plan);
    answer(&mut k, 10 * MINUTE, &all);
    assert_eq!(k.ends(), vec![6, 7, 8, 9, 10]);
    answer(&mut k, 11 * MINUTE, &all);
    assert_eq!(k.ends(), vec![6, 7, 8, 9, 10, 11]);
    answer(&mut k, 12 * MINUTE, &all);
    assert_eq!(k.ends(), vec![7, 8, 9, 10, 11, 12]);
    // An evaluation far ahead of the watermark is answered, but its
    // summaries' coverages are not closed, so nothing is kept or evicted.
    let (far, ranges) = answer_at_watermark(&mut k, 20 * MINUTE, Some(12 * MINUTE), &all);
    assert_eq!(far.len(), 2);
    assert_eq!(spans(&ranges), vec![(15, 20)]);
    assert_eq!(k.ends(), vec![7, 8, 9, 10, 11, 12]);
    // Keeping resumes with the watermark.
    answer(&mut k, 13 * MINUTE, &all);
    assert_eq!(k.ends(), vec![8, 9, 10, 11, 12, 13]);
}

/// A summary is kept only once its coverage is closed (its end at or
/// before the watermark). One read before then is built for that
/// evaluation but not kept, so a later evaluation builds it again and
/// includes a sample that arrived late.
#[test]
fn summaries_are_kept_only_once_closed() {
    let (plan, reference_plan) = plans();
    let mut all = samples();
    let mut k = kept(&plan);
    let mut reference = kept(&reference_plan);

    // Watermark at 9.5 min: the summary ending at 10 min is still open.
    let (first, ranges) = answer_at_watermark(&mut k, 10 * MINUTE, Some(570_000), &all);
    assert_eq!(spans(&ranges), vec![(5, 10)]);
    assert_eq!(first, answer(&mut reference, 10 * MINUTE, &all).0);
    assert_eq!(k.ends(), vec![6, 7, 8, 9], "the open summary is not kept");

    // A late sample for the open coverage arrives before it closes.
    let late = RawSample {
        timestamp_ms: 10 * MINUTE - 1_000,
        value: 1_000.0,
        ..all.iter().find(|s| s.labels["job"] == "a").unwrap().clone()
    };
    all.push(late);
    let (again, ranges) = answer_at_watermark(&mut k, 10 * MINUTE, Some(585_000), &all);
    assert_eq!(
        spans(&ranges),
        vec![(9, 10)],
        "the open summary is built again"
    );
    assert_eq!(again, answer(&mut reference, 10 * MINUTE, &all).0);
    assert_ne!(again, first, "the late sample is included");

    // Once the watermark reaches 10 min, the summary is kept.
    answer_at_watermark(&mut k, 10 * MINUTE, Some(10 * MINUTE), &all);
    assert_eq!(k.ends(), vec![6, 7, 8, 9, 10]);
    // Without any sample there is no watermark, and nothing is kept.
    let mut fresh = kept(&plan);
    answer_at_watermark(&mut fresh, 10 * MINUTE, None, &all);
    assert!(fresh.store.is_empty());
}

/// The keep decision uses the watermark when the evaluation chose what to
/// fetch, not when it runs: ingestion that closes a coverage after the
/// fetch was chosen does not make the partial summary built from that
/// fetch permanent.
#[tokio::test]
async fn keeps_by_the_watermark_the_fetch_was_chosen_at() {
    let (plan, _) = plans();
    let options = EngineOptions {
        precompute: data_plane::precompute::PrecomputeOptions {
            allowed_lateness_ms: 0,
            ..Default::default()
        },
        ..EngineOptions::default()
    };
    let (engine, _) = EngineHandle::spawn(serde_json::to_string(&plan).unwrap(), options).unwrap();
    let all = samples();
    let through = |t: i64| -> Vec<RawSample> {
        all.iter()
            .filter(|s| s.timestamp_ms <= t)
            .cloned()
            .collect()
    };
    let settle = |engine: &EngineHandle| {
        let engine = engine.clone();
        async move {
            while engine.queued_samples() > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        }
    };
    engine.try_ingest(through(570_000)).unwrap();
    settle(&engine).await;
    let prepared = engine.raw_ranges(P99.into(), 10 * MINUTE).await.unwrap();
    assert_eq!(prepared.progress.watermark_ms, Some(570_000));
    // The fetch sees only what had arrived; then ingestion closes 10 min.
    let raw = fetch(&prepared.ranges, &through(570_000));
    engine.try_ingest(through(11 * MINUTE)).unwrap();
    settle(&engine).await;
    engine.evaluate(prepared, 10 * MINUTE, raw).await.unwrap();
    assert_eq!(engine.kept_stats().kept, 4, "6..9 kept, 10 not");
    // So the next evaluation builds the summary ending at 10 min again.
    let prepared = engine.raw_ranges(P99.into(), 10 * MINUTE).await.unwrap();
    assert_eq!(spans(&prepared.ranges), vec![(9, 10)]);
}

/// A summary kept while its own metric's newest sample has not reached its
/// end (the global watermark moved on another metric) is counted.
#[test]
fn counts_summaries_kept_while_their_metric_is_behind() {
    let (plan, _) = plans();
    let all = samples();
    let mut k = kept(&plan);
    let progress = IngestionProgress {
        watermark_ms: Some(10 * MINUTE),
        newest_sample_ms: BTreeMap::from([("latency_ms".to_string(), 8 * MINUTE)]),
    };
    let prepared = k
        .engine
        .raw_ranges(P99, 10 * MINUTE, &k.store, progress)
        .unwrap();
    let raw = fetch(&prepared.ranges, &all);
    k.engine
        .evaluate(
            prepared.root,
            10 * MINUTE,
            &mut k.store,
            &prepared.progress,
            &raw,
        )
        .unwrap();
    assert_eq!(k.engine.kept_stats().kept, 5);
    assert_eq!(
        k.engine.kept_stats().kept_while_metric_behind,
        2,
        "9 and 10"
    );
}
