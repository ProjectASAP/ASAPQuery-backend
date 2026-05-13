//! `BackfillWorker` — drives a single [`BackfillJob`] through the
//! registry state machine.
//!
//! Implements §10.3 (refresh as a separate worker pool) of the
//! sketch DB design ([`design-sketch-db.md`](../../../../../docs/design-sketch-db.md)).
//! Phase 5c scope: one job at a time, synchronous windowing loop,
//! pluggable processor. The multi-worker pool with priority +
//! isolation from live ingest (§11.4) lands in a follow-up.
//!
//! ## What the worker does
//!
//! 1. Calls [`BackfillRegistry::start`] to transition the job from
//!    `Queued` to `Running`. If that returns `false` (unknown job /
//!    non-queued), bails out.
//! 2. Splits the job's `[start_ms, end_ms)` range into
//!    `windows_total` equal windows. Rounding: any leftover ms from
//!    integer division is absorbed into the final window so the
//!    caller sees exactly `windows_total` windows and the full range
//!    is covered.
//! 3. For each window in order:
//!    * Reads matching samples via [`RawSampleReader::read_samples`].
//!    * Hands them to [`WindowProcessor::process_window`] along with
//!      the job's `agg_id` and the window's `(start, end)` range.
//!    * Calls [`BackfillRegistry::tick_progress`] so the §10.4
//!      coverage tracker sees progress as a monotonically-increasing
//!      fraction.
//!    * Re-checks the job status before the next window — if
//!      `Cancelled`, stops cleanly without `mark_failed`ing (the
//!      caller already terminalised it).
//! 4. On all windows succeeding, calls [`BackfillRegistry::mark_complete`].
//! 5. On any reader or processor error, calls
//!    [`BackfillRegistry::mark_failed`] with the error stringified
//!    and bubbles the error up so the caller can react.
//!
//! ## Phase 5c scope (what this file covers)
//!
//! * `WindowProcessor` trait — the per-window callback the worker
//!   invokes. Phase 5e implements the real sketch-building
//!   processor; for now a [`RecordingProcessor`] captures calls for
//!   tests.
//! * `BackfillWorker::run_job(job_id, filter, reader, processor)` —
//!   the one-job-at-a-time driver.
//! * Tests covering happy path, reader error, processor error,
//!   mid-run cancellation, single-window edge case, empty-samples
//!   window, and non-queued job rejection.
//!
//! ## Out of scope for 5c (future phases)
//!
//! * Multi-worker concurrency + job prioritisation (§11.4) — a
//!   `BackfillPool` wrapping N `BackfillWorker` tasks polling the
//!   registry, coming in Phase 5c-2 or 5d.
//! * Reader selection from `BackfillSource` — Phase 5e wires
//!   concrete readers per variant.
//! * Actual sketch construction inside the processor — Phase 5e.
//! * §6.3 schema barrier enforcement on backfill writes — Phase 5e
//!   (when the processor actually writes into the store).

use std::sync::Arc;

use async_trait::async_trait;

use super::{BackfillRegistry, BackfillStatus};
use super::raw_sample_reader::{LabelFilter, RawSample, RawSampleReader};

/// Per-window callback invoked by [`BackfillWorker`] after reading
/// samples for a window. Phase 5e implements a real processor that
/// feeds samples into a fresh accumulator and writes the resulting
/// precompute to the store; for now the trait exists so the worker's
/// orchestration logic is independently testable.
#[async_trait]
pub trait WindowProcessor: Send + Sync {
    async fn process_window(
        &self,
        agg_id: u64,
        window_range: (u64, u64),
        samples: Vec<RawSample>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

/// Errors the worker can surface. Terminology matches §10.3:
/// every variant below causes the worker to `mark_failed` the job
/// with the stringified error. The error is also returned to the
/// caller so higher-level orchestration (e.g. a retry loop) can
/// classify + react.
#[derive(Debug)]
pub enum BackfillWorkerError {
    /// `run_job` was called with a `job_id` that doesn't exist in
    /// the registry. Never alters job state (there is no state
    /// to alter).
    UnknownJob(u64),
    /// `run_job` was called on a job that isn't in `Queued`. Either
    /// already picked up by another worker or already terminal.
    /// Never alters job state.
    NotQueued { job_id: u64, status: BackfillStatus },
    /// The reader returned an error for some window. Job has been
    /// marked Failed.
    Reader {
        job_id: u64,
        window_range: (u64, u64),
        reason: String,
    },
    /// The processor returned an error for some window. Job has been
    /// marked Failed.
    Processor {
        job_id: u64,
        window_range: (u64, u64),
        reason: String,
    },
}

impl std::fmt::Display for BackfillWorkerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownJob(id) => write!(f, "unknown job_id {id}"),
            Self::NotQueued { job_id, status } => {
                write!(f, "job {job_id} is {status:?}, expected Queued")
            }
            Self::Reader {
                job_id,
                window_range,
                reason,
            } => write!(
                f,
                "reader failed on job {job_id} window {window_range:?}: {reason}"
            ),
            Self::Processor {
                job_id,
                window_range,
                reason,
            } => write!(
                f,
                "processor failed on job {job_id} window {window_range:?}: {reason}"
            ),
        }
    }
}

impl std::error::Error for BackfillWorkerError {}

/// Drives a single backfill job. Holds a shared reference to the
/// registry so lifecycle transitions stay coherent with external
/// readers (HTTP list endpoint in Phase 5d, coverage tracker in
/// Phase 5f).
pub struct BackfillWorker {
    registry: Arc<BackfillRegistry>,
}

impl BackfillWorker {
    pub fn new(registry: Arc<BackfillRegistry>) -> Self {
        Self { registry }
    }

    /// Run `job_id` to completion (or failure). The worker reads
    /// via `reader`, filtering with `filter`, and calls `processor`
    /// once per window. See the module doc for the full state-machine.
    pub async fn run_job<R, P>(
        &self,
        job_id: u64,
        filter: &LabelFilter,
        reader: &R,
        processor: &P,
    ) -> Result<(), BackfillWorkerError>
    where
        R: RawSampleReader + ?Sized,
        P: WindowProcessor + ?Sized,
    {
        let job = match self.registry.get(job_id) {
            Some(j) => j,
            None => return Err(BackfillWorkerError::UnknownJob(job_id)),
        };
        if !matches!(job.status, BackfillStatus::Queued) {
            return Err(BackfillWorkerError::NotQueued {
                job_id,
                status: job.status,
            });
        }
        // `start` returns `false` if another worker raced us; in
        // that case we must not continue — the other worker owns
        // the job. This is the only place the worker respects
        // inter-worker exclusion; the registry's write lock
        // serialises the transition.
        if !self.registry.start(job_id) {
            let current = self
                .registry
                .get(job_id)
                .map(|j| j.status)
                .unwrap_or(BackfillStatus::Queued);
            return Err(BackfillWorkerError::NotQueued {
                job_id,
                status: current,
            });
        }

        let (start_ms, end_ms) = job.time_range;
        let total_windows = job.windows_total.max(1);
        let total_span = end_ms.saturating_sub(start_ms);
        let base_window_size = total_span / total_windows;

        for i in 0..total_windows {
            // Check for external cancellation before starting the
            // next window. A cancelled job stops processing without
            // `mark_failed` — it's already terminal.
            if let Some(current) = self.registry.get(job_id) {
                if matches!(current.status, BackfillStatus::Cancelled) {
                    return Ok(());
                }
            }

            let w_start = start_ms + i * base_window_size;
            // Last window absorbs any leftover ms from integer division
            // so the worker emits exactly `total_windows` windows and
            // the full `[start_ms, end_ms)` range is covered.
            let w_end = if i + 1 == total_windows {
                end_ms
            } else {
                start_ms + (i + 1) * base_window_size
            };

            let samples = match reader.read_samples(w_start, w_end, filter).await {
                Ok(v) => v,
                Err(e) => {
                    let msg = e.to_string();
                    self.registry.mark_failed(job_id, &msg);
                    return Err(BackfillWorkerError::Reader {
                        job_id,
                        window_range: (w_start, w_end),
                        reason: msg,
                    });
                }
            };

            if let Err(e) = processor
                .process_window(job.agg_id, (w_start, w_end), samples)
                .await
            {
                let msg = e.to_string();
                self.registry.mark_failed(job_id, &msg);
                return Err(BackfillWorkerError::Processor {
                    job_id,
                    window_range: (w_start, w_end),
                    reason: msg,
                });
            }

            self.registry.tick_progress(job_id);
        }

        self.registry.mark_complete(job_id);
        Ok(())
    }
}

/// One recorded `process_window` call: `(agg_id, window_range, sample_count)`.
#[cfg(test)]
pub type RecordedCall = (u64, (u64, u64), usize);

/// Test processor that captures every `process_window` call. Lets
/// tests assert on the window sequence + sample counts after a
/// `run_job`. Also supports a "fail on nth call" mode for exercising
/// the worker's error path.
#[cfg(test)]
pub struct RecordingProcessor {
    calls: std::sync::Mutex<Vec<RecordedCall>>,
    fail_on_call: Option<usize>,
}

#[cfg(test)]
impl Default for RecordingProcessor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
impl RecordingProcessor {
    pub fn new() -> Self {
        Self {
            calls: std::sync::Mutex::new(Vec::new()),
            fail_on_call: None,
        }
    }

    pub fn failing_on(call_index: usize) -> Self {
        Self {
            calls: std::sync::Mutex::new(Vec::new()),
            fail_on_call: Some(call_index),
        }
    }

    pub fn calls(&self) -> Vec<RecordedCall> {
        self.calls.lock().unwrap().clone()
    }
}

#[cfg(test)]
#[async_trait]
impl WindowProcessor for RecordingProcessor {
    async fn process_window(
        &self,
        agg_id: u64,
        window_range: (u64, u64),
        samples: Vec<RawSample>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut calls = self.calls.lock().unwrap();
        let idx = calls.len();
        calls.push((agg_id, window_range, samples.len()));
        if self.fail_on_call == Some(idx) {
            return Err(format!("simulated processor failure at call {idx}").into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_engines::sketch_db::{BackfillSource, MockRawSampleReader};

    fn prom_source() -> BackfillSource {
        BackfillSource::Prometheus {
            url: "http://prom.local".to_string(),
        }
    }

    fn mk_sample(labels: &str, ts: i64, v: f64) -> RawSample {
        RawSample {
            labels: labels.to_string(),
            timestamp_ms: ts,
            value: v,
        }
    }

    #[tokio::test]
    async fn run_job_happy_path_transitions_to_complete() {
        let registry = Arc::new(BackfillRegistry::new());
        let job_id = registry.create(7, (0, 40), prom_source(), 4);
        let reader = MockRawSampleReader::new(vec![
            mk_sample("m", 5, 1.0),
            mk_sample("m", 15, 2.0),
            mk_sample("m", 25, 3.0),
            mk_sample("m", 35, 4.0),
        ]);
        let processor = RecordingProcessor::new();
        let worker = BackfillWorker::new(registry.clone());

        worker
            .run_job(job_id, &LabelFilter::for_metric("m"), &reader, &processor)
            .await
            .expect("happy path should succeed");

        let job = registry.get(job_id).unwrap();
        assert_eq!(job.status, BackfillStatus::Complete);
        assert_eq!(job.windows_done, 4);
        assert_eq!(job.agg_id, 7);
        assert!(job.completed_at_ms.is_some());

        let calls = processor.calls();
        assert_eq!(calls.len(), 4);
        // Windows are contiguous and non-overlapping; each gets
        // exactly the sample that falls in its range.
        assert_eq!(calls[0].1, (0, 10));
        assert_eq!(calls[1].1, (10, 20));
        assert_eq!(calls[2].1, (20, 30));
        assert_eq!(calls[3].1, (30, 40));
        assert_eq!(
            calls.iter().map(|c| c.2).collect::<Vec<_>>(),
            vec![1, 1, 1, 1]
        );
    }

    #[tokio::test]
    async fn run_job_last_window_absorbs_leftover_when_range_not_divisible() {
        // Range of 23ms split into 4 windows: each base window is
        // 23/4 = 5ms, and the last window absorbs the leftover 3ms
        // → windows [0,5), [5,10), [10,15), [15,23).
        let registry = Arc::new(BackfillRegistry::new());
        let job_id = registry.create(1, (0, 23), prom_source(), 4);
        let reader = MockRawSampleReader::new(vec![]);
        let processor = RecordingProcessor::new();
        let worker = BackfillWorker::new(registry.clone());

        worker
            .run_job(job_id, &LabelFilter::for_metric("m"), &reader, &processor)
            .await
            .unwrap();

        let calls = processor.calls();
        assert_eq!(calls.len(), 4);
        assert_eq!(calls[0].1, (0, 5));
        assert_eq!(calls[1].1, (5, 10));
        assert_eq!(calls[2].1, (10, 15));
        // Last window covers up to exact end_ms.
        assert_eq!(calls[3].1, (15, 23));
    }

    #[tokio::test]
    async fn run_job_empty_samples_still_counts_progress() {
        let registry = Arc::new(BackfillRegistry::new());
        let job_id = registry.create(1, (0, 30), prom_source(), 3);
        let reader = MockRawSampleReader::new(vec![]);
        let processor = RecordingProcessor::new();
        let worker = BackfillWorker::new(registry.clone());

        worker
            .run_job(job_id, &LabelFilter::for_metric("m"), &reader, &processor)
            .await
            .unwrap();

        let job = registry.get(job_id).unwrap();
        assert_eq!(job.status, BackfillStatus::Complete);
        assert_eq!(job.windows_done, 3);
        assert!(processor.calls().iter().all(|c| c.2 == 0));
    }

    #[tokio::test]
    async fn run_job_processor_error_marks_failed() {
        let registry = Arc::new(BackfillRegistry::new());
        let job_id = registry.create(1, (0, 30), prom_source(), 3);
        let reader = MockRawSampleReader::new(vec![]);
        // Fail on the second processor call.
        let processor = RecordingProcessor::failing_on(1);
        let worker = BackfillWorker::new(registry.clone());

        let err = worker
            .run_job(job_id, &LabelFilter::for_metric("m"), &reader, &processor)
            .await
            .expect_err("should propagate processor error");
        match err {
            BackfillWorkerError::Processor {
                job_id: got_id,
                window_range,
                ..
            } => {
                assert_eq!(got_id, job_id);
                assert_eq!(window_range, (10, 20));
            }
            other => panic!("expected Processor error, got {other}"),
        }

        let job = registry.get(job_id).unwrap();
        assert_eq!(job.status, BackfillStatus::Failed);
        assert!(
            job.error_message.is_some(),
            "error_message should be populated"
        );
        assert_eq!(
            job.windows_done, 1,
            "only first window ticked before failure"
        );
    }

    #[tokio::test]
    async fn run_job_reader_error_marks_failed() {
        // InvalidRange error from the reader when passed an inverted
        // range. Phase 5c worker should classify as Reader error
        // and mark the job Failed. Construct a job with inverted
        // time_range so the first window read fails.
        let registry = Arc::new(BackfillRegistry::new());
        let job_id = registry.create(1, (100, 10), prom_source(), 1);
        let reader = MockRawSampleReader::new(vec![]);
        let processor = RecordingProcessor::new();
        let worker = BackfillWorker::new(registry.clone());

        let err = worker
            .run_job(job_id, &LabelFilter::for_metric("m"), &reader, &processor)
            .await
            .expect_err("reader should reject inverted range");
        match err {
            BackfillWorkerError::Reader { .. } => {}
            other => panic!("expected Reader error, got {other}"),
        }
        assert_eq!(registry.get(job_id).unwrap().status, BackfillStatus::Failed);
    }

    #[tokio::test]
    async fn run_job_cancel_mid_run_stops_cleanly() {
        // Processor cancels the job on its first call; the worker
        // should see Cancelled before the next window and return Ok
        // without mark_failed.
        struct CancellingProcessor {
            registry: Arc<BackfillRegistry>,
            job_id: u64,
        }
        #[async_trait]
        impl WindowProcessor for CancellingProcessor {
            async fn process_window(
                &self,
                _agg_id: u64,
                _window_range: (u64, u64),
                _samples: Vec<RawSample>,
            ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                self.registry.cancel(self.job_id);
                Ok(())
            }
        }

        let registry = Arc::new(BackfillRegistry::new());
        let job_id = registry.create(1, (0, 30), prom_source(), 3);
        let reader = MockRawSampleReader::new(vec![]);
        let processor = CancellingProcessor {
            registry: registry.clone(),
            job_id,
        };
        let worker = BackfillWorker::new(registry.clone());

        worker
            .run_job(job_id, &LabelFilter::for_metric("m"), &reader, &processor)
            .await
            .expect("cancel mid-run should return Ok");

        let job = registry.get(job_id).unwrap();
        assert_eq!(job.status, BackfillStatus::Cancelled);
        // Progress is 0: processor cancelled the job mid-window, so
        // the subsequent `tick_progress` was rejected (job already
        // terminal). This is intentional — `windows_done` should
        // never lag behind the actual terminal state.
        assert_eq!(job.windows_done, 0);
    }

    #[tokio::test]
    async fn run_job_rejects_unknown_job_id() {
        let registry = Arc::new(BackfillRegistry::new());
        let worker = BackfillWorker::new(registry.clone());
        let reader = MockRawSampleReader::new(vec![]);
        let processor = RecordingProcessor::new();
        let err = worker
            .run_job(999, &LabelFilter::for_metric("m"), &reader, &processor)
            .await
            .unwrap_err();
        assert!(matches!(err, BackfillWorkerError::UnknownJob(999)));
    }

    #[tokio::test]
    async fn run_job_rejects_already_running_job() {
        // A second worker trying to pick up a `Running` job should
        // bail with NotQueued — no state mutation.
        let registry = Arc::new(BackfillRegistry::new());
        let job_id = registry.create(1, (0, 10), prom_source(), 1);
        // Pre-transition to Running.
        assert!(registry.start(job_id));
        let worker = BackfillWorker::new(registry.clone());
        let reader = MockRawSampleReader::new(vec![]);
        let processor = RecordingProcessor::new();

        let err = worker
            .run_job(job_id, &LabelFilter::for_metric("m"), &reader, &processor)
            .await
            .unwrap_err();
        match err {
            BackfillWorkerError::NotQueued { status, .. } => {
                assert_eq!(status, BackfillStatus::Running);
            }
            other => panic!("expected NotQueued, got {other}"),
        }
        // State unchanged.
        assert_eq!(
            registry.get(job_id).unwrap().status,
            BackfillStatus::Running
        );
    }

    #[tokio::test]
    async fn run_job_windows_total_zero_treated_as_single_window() {
        // Defensive: a bad caller might create a job with
        // windows_total=0. Worker normalises to 1 so we don't panic
        // on division-by-zero.
        let registry = Arc::new(BackfillRegistry::new());
        let job_id = registry.create(1, (0, 100), prom_source(), 0);
        let reader = MockRawSampleReader::new(vec![mk_sample("m", 50, 1.0)]);
        let processor = RecordingProcessor::new();
        let worker = BackfillWorker::new(registry.clone());

        worker
            .run_job(job_id, &LabelFilter::for_metric("m"), &reader, &processor)
            .await
            .unwrap();
        assert_eq!(processor.calls().len(), 1);
        assert_eq!(processor.calls()[0].1, (0, 100));
    }

    #[tokio::test]
    async fn run_job_forwards_filter_to_reader() {
        let registry = Arc::new(BackfillRegistry::new());
        let job_id = registry.create(1, (0, 10), prom_source(), 1);
        let reader = MockRawSampleReader::new(vec![
            mk_sample("m{env=\"prod\"}", 5, 1.0),
            mk_sample("m{env=\"stage\"}", 5, 2.0),
        ]);
        let processor = RecordingProcessor::new();
        let worker = BackfillWorker::new(registry.clone());

        let filter = LabelFilter::for_metric("m").with_label("env", "prod");
        worker
            .run_job(job_id, &filter, &reader, &processor)
            .await
            .unwrap();

        let calls = processor.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].2, 1, "filter should restrict to env=prod");
    }
}
