//! The local raw buffer: recent raw samples, from which queries read their
//! query-time raw windows instead of asking Prometheus.
//!
//! It is fed by Remote Write through the ingestion workers and sharded like
//! them: each worker appends its shard's samples. It keeps the samples of
//! the plan's longest query-time read back from the newest sample (its
//! horizon); a read further back takes the older part from Prometheus
//! remote read.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, RwLock};

use asap_planner::{InstallablePlan, StoredOutputBuild};

use crate::precompute::RawSample;

type Labels = Arc<BTreeMap<String, String>>;

/// No value yet in an `AtomicI64` slot.
const UNSET: i64 = i64::MIN;

#[derive(Default)]
struct Shard {
    /// Each series' samples in time order, by metric and label set.
    series: HashMap<String, HashMap<Labels, VecDeque<(i64, f64)>>>,
    /// Samples at or before this time may have been dropped.
    trimmed_through_ms: i64,
    samples: usize,
}

pub struct RawBuffer {
    horizon_ms: i64,
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
    /// The longest a query of `plan` reads raw samples back from its
    /// evaluation time: over its query-time raw windows and the panes it
    /// builds at query time and keeps, `end_offset + width`, and for the
    /// latter their retention, which covers their range evaluations.
    pub fn horizon_ms(plan: &InstallablePlan) -> i64 {
        let windows = plan
            .query_time_raw_windows
            .iter()
            .map(|w| w.end_offset_ms + w.width_ms);
        let kept = plan
            .stored_outputs
            .iter()
            .filter(|o| o.built_at == StoredOutputBuild::QueryRun)
            .map(|o| (o.pane.reach_ms().max(o.retention_ms)) as i64);
        windows.chain(kept).max().unwrap_or(0)
    }

    pub fn new(horizon_ms: i64, shards: usize) -> Self {
        Self {
            horizon_ms,
            shards: (0..shards.max(1))
                .map(|_| {
                    RwLock::new(Shard {
                        trimmed_through_ms: UNSET,
                        ..Shard::default()
                    })
                })
                .collect(),
            start_ms: AtomicI64::new(UNSET),
            newest_ms: AtomicI64::new(UNSET),
        }
    }

    /// Appends `samples`, all of shard `shard`, then drops its samples older
    /// than the horizon before the newest sample.
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
        let trim_through = newest - self.horizon_ms;
        let mut shard = self.shards[shard].write().expect("raw buffer lock");
        let shard = &mut *shard;
        let mut added = 0usize;
        // Samples of one series arrive together: look each series up once.
        for run in samples.chunk_by(|a, b| Arc::ptr_eq(&a.labels, &b.labels)) {
            let labels = &run[0].labels;
            let metric = labels.get("__name__").cloned().unwrap_or_default();
            let series = shard
                .series
                .entry(metric)
                .or_default()
                .entry(labels.clone())
                .or_default();
            for sample in run.iter().filter(|s| s.timestamp_ms > trim_through) {
                let point = (sample.timestamp_ms, sample.value);
                match series.back() {
                    Some(&(time, _)) if time >= point.0 => {
                        let at = series.partition_point(|&(t, _)| t < point.0);
                        if series.get(at).is_some_and(|&(t, _)| t == point.0) {
                            continue;
                        }
                        series.insert(at, point);
                    }
                    _ => series.push_back(point),
                }
                added += 1;
            }
        }
        shard.samples += added;
        if trim_through > shard.trimmed_through_ms {
            shard.trimmed_through_ms = trim_through;
            let mut dropped = 0;
            for series in shard.series.values_mut() {
                series.retain(|_, points| {
                    let keep = points.partition_point(|&(t, _)| t <= trim_through);
                    dropped += keep;
                    points.drain(..keep);
                    !points.is_empty()
                });
            }
            shard.samples -= dropped;
        }
    }

    /// `metric`'s samples in `(start_ms, end_ms]` that the buffer holds,
    /// and from when it holds all of them.
    pub fn read(&self, metric: &str, start_ms: i64, end_ms: i64) -> BufferRead {
        let first = self.start_ms.load(Ordering::Acquire);
        let mut older_than_ms = start_ms.max(if first == UNSET { end_ms } else { first });
        let mut parts = Vec::with_capacity(self.shards.len());
        for shard in &self.shards {
            let shard = shard.read().expect("raw buffer lock");
            older_than_ms = older_than_ms.max(shard.trimmed_through_ms);
            let mut samples = Vec::new();
            for (labels, points) in shard.series.get(metric).into_iter().flatten() {
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
