//! The local raw buffer: recent raw samples, from which queries read their
//! query-time raw windows instead of asking Prometheus.
//!
//! It is fed by Remote Write through the ingestion workers and sharded like
//! them: each worker appends its shard's samples. It keeps each metric the
//! plan reads at query time for that metric's longest query-time read back
//! from the newest sample (its horizon), and no other metric; a read
//! further back takes the older part from Prometheus remote read.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, RwLock};

use asap_executor::planner::ir::operator::Source;
use asap_executor::planner::ir::{NonASAPOp, Operator};
use asap_planner::{InstallablePlan, StoredOutputBuild};

use crate::precompute::RawSample;
use crate::series_ids::Series;

/// No value yet in an `AtomicI64` slot.
const UNSET: i64 = i64::MIN;

/// One series and its samples in time order.
type SeriesSamples = (Arc<Series>, VecDeque<(i64, f64)>);

#[derive(Default)]
struct Shard {
    /// Each series' samples in time order, by metric and series id.
    series: ahash::HashMap<Arc<str>, ahash::HashMap<u32, SeriesSamples>>,
    /// By metric: samples at or before this time may have been dropped.
    trimmed_through_ms: ahash::HashMap<Arc<str>, i64>,
    samples: usize,
}

/// How long each metric is kept.
enum Horizons {
    /// Every metric, for the same time.
    All(i64),
    /// Only these metrics, each for its own time.
    PerMetric(BTreeMap<String, i64>),
}

impl Horizons {
    fn of(&self, metric: &str) -> Option<i64> {
        match self {
            Self::All(ms) => Some(*ms),
            Self::PerMetric(by_metric) => by_metric.get(metric).copied(),
        }
    }
}

pub struct RawBuffer {
    horizons: Horizons,
    shards: Vec<RwLock<Shard>>,
    /// The first sample's time: the buffer holds every sample after it
    /// that Remote Write delivered, until it is trimmed.
    start_ms: AtomicI64,
    newest_ms: AtomicI64,
}

/// What a buffer read gives: the samples it holds of the range, and the
/// time before which the range must be read from Prometheus instead.
pub struct BufferRead {
    /// Samples in `(older_than_ms, end]`, each series' together in order.
    pub samples: Vec<RawSample>,
    /// The buffer holds every sample after this time (at least the range's
    /// start).
    pub older_than_ms: i64,
}

impl RawBuffer {
    /// The longest a query of `plan` reads each metric's raw samples back
    /// from its evaluation time: over its query-time raw windows and the
    /// panes it builds at query time and keeps, `end_offset + width`, and
    /// for the latter their retention, which covers their range evaluations.
    pub fn horizons(plan: &InstallablePlan) -> BTreeMap<String, i64> {
        let dag = &plan.dag.dag;
        let metric = |scan| {
            dag.nodes
                .iter()
                .find(|n| n.id == scan)
                .and_then(|n| match &n.payload {
                    Operator::NonASAP(NonASAPOp::Scan {
                        source: Source::TimeSeries { metric },
                        ..
                    }) => Some(metric.clone()),
                    _ => None,
                })
        };
        let windows = plan
            .query_time_raw_windows
            .iter()
            .map(|w| (w.scan_node_id, w.end_offset_ms + w.width_ms));
        let kept = plan
            .stored_outputs
            .iter()
            .filter(|o| o.built_at == StoredOutputBuild::QueryRun)
            .map(|o| {
                (
                    o.pane.scan_node_id,
                    o.pane.reach_ms().max(o.retention_ms) as i64,
                )
            });
        let mut horizons = BTreeMap::new();
        for (scan, reach_ms) in windows.chain(kept) {
            if let Some(metric) = metric(scan) {
                let horizon = horizons.entry(metric).or_insert(0);
                *horizon = reach_ms.max(*horizon);
            }
        }
        horizons
    }

    /// A buffer that keeps every metric for `horizon_ms`.
    pub fn new(horizon_ms: i64, shards: usize) -> Self {
        Self::with(Horizons::All(horizon_ms), shards)
    }

    /// A buffer that keeps only the metrics of `horizons`, each for its own
    /// time ([`Self::horizons`]).
    pub fn with_horizons(horizons: BTreeMap<String, i64>, shards: usize) -> Self {
        Self::with(Horizons::PerMetric(horizons), shards)
    }

    fn with(horizons: Horizons, shards: usize) -> Self {
        Self {
            horizons,
            shards: (0..shards.max(1))
                .map(|_| RwLock::new(Shard::default()))
                .collect(),
            start_ms: AtomicI64::new(UNSET),
            newest_ms: AtomicI64::new(UNSET),
        }
    }

    /// Appends the samples of `samples`, all of shard `shard`, whose metric
    /// the buffer keeps, then drops the shard's samples older than each
    /// metric's horizon before the newest sample.
    pub fn append(&self, shard: usize, samples: &[RawSample]) {
        let (Some(first), Some(newest)) = (
            samples.iter().map(|s| s.timestamp_ms).min(),
            samples.iter().map(|s| s.timestamp_ms).max(),
        ) else {
            return;
        };
        let _ = self
            .start_ms
            .compare_exchange(UNSET, first, Ordering::AcqRel, Ordering::Acquire);
        let newest = self
            .newest_ms
            .fetch_max(newest, Ordering::AcqRel)
            .max(newest);
        let mut shard = self.shards[shard].write().expect("raw buffer lock");
        let Shard {
            series,
            trimmed_through_ms,
            samples: held,
        } = &mut *shard;
        // Samples of one series arrive together: look each series up once.
        for run in samples.chunk_by(|a, b| Arc::ptr_eq(&a.labels, &b.labels)) {
            let labels = &run[0].labels;
            let Some(horizon_ms) = self.horizons.of(&labels.metric) else {
                continue;
            };
            let trim_through = newest - horizon_ms;
            if run.iter().all(|s| s.timestamp_ms <= trim_through) {
                continue;
            }
            let (_, points) = series
                .entry(labels.metric.clone())
                .or_default()
                .entry(labels.id)
                .or_insert_with(|| (labels.clone(), VecDeque::new()));
            for sample in run.iter().filter(|s| s.timestamp_ms > trim_through) {
                let point = (sample.timestamp_ms, sample.value);
                match points.back() {
                    Some(&(time, _)) if time >= point.0 => {
                        let at = points.partition_point(|&(t, _)| t < point.0);
                        if points.get(at).is_some_and(|&(t, _)| t == point.0) {
                            continue;
                        }
                        points.insert(at, point);
                    }
                    _ => points.push_back(point),
                }
                *held += 1;
            }
        }
        for (metric, by_labels) in series.iter_mut() {
            let Some(horizon_ms) = self.horizons.of(metric) else {
                continue;
            };
            let trim_through = newest - horizon_ms;
            let trimmed = trimmed_through_ms.entry(metric.clone()).or_insert(UNSET);
            if trim_through <= *trimmed {
                continue;
            }
            *trimmed = trim_through;
            by_labels.retain(|_, (_, points)| {
                let old = points.partition_point(|&(t, _)| t <= trim_through);
                *held -= old;
                points.drain(..old);
                !points.is_empty()
            });
        }
    }

    /// `metric`'s samples in `(start_ms, end_ms]` that the buffer holds,
    /// and from when it holds all of them.
    pub fn read(&self, metric: &str, start_ms: i64, end_ms: i64) -> BufferRead {
        if self.horizons.of(metric).is_none() {
            return BufferRead {
                samples: Vec::new(),
                older_than_ms: end_ms,
            };
        }
        let first = self.start_ms.load(Ordering::Acquire);
        let mut older_than_ms = start_ms.max(if first == UNSET { end_ms } else { first });
        let mut parts = Vec::with_capacity(self.shards.len());
        for shard in &self.shards {
            let shard = shard.read().expect("raw buffer lock");
            if let Some(&trimmed) = shard.trimmed_through_ms.get(metric) {
                older_than_ms = older_than_ms.max(trimmed);
            }
            let mut samples = Vec::new();
            for (labels, points) in shard
                .series
                .get(metric)
                .into_iter()
                .flat_map(|m| m.values())
            {
                let from = points.partition_point(|&(t, _)| t <= start_ms);
                let to = points.partition_point(|&(t, _)| t <= end_ms);
                samples.extend(
                    points
                        .range(from..to)
                        .map(|&(timestamp_ms, value)| RawSample {
                            labels: labels.clone(),
                            timestamp_ms,
                            value,
                        }),
                );
            }
            parts.push(samples);
        }
        // A shard trimmed past another's cut may miss samples before it, so
        // every shard's samples before the latest cut come from Prometheus.
        let older_than_ms = older_than_ms.min(end_ms);
        let samples = parts
            .into_iter()
            .flatten()
            .filter(|s| s.timestamp_ms > older_than_ms)
            .collect();
        BufferRead {
            samples,
            older_than_ms,
        }
    }

    /// Series and samples held, for reporting.
    pub fn size(&self) -> (usize, usize) {
        self.shards.iter().fold((0, 0), |(series, samples), shard| {
            let shard = shard.read().expect("raw buffer lock");
            let n: usize = shard.series.values().map(HashMap::len).sum();
            (series + n, samples + shard.samples)
        })
    }
}
