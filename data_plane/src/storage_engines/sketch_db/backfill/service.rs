//! `BackfillService` — tokio task that drains the `BackfillRegistry`
//! for `Queued` jobs and runs them via a `BackfillWorker` +
//! `BackfillWindowProcessor`.
//!
//! Implements the worker-pool side of §10.3 (refresh as a separate
//! worker pool) from the sketch DB design. Phase 5e-v1 runs **one
//! job at a time** — the multi-worker + priority / quota controls
//! from §11.4 land in a follow-up.
//!
//! ## Architecture
//!
//! ```text
//!   POST /api/v1/db/backfill     BackfillService task
//!      │                                │
//!      │ create()                       │ loop {
//!      ▼                                │   pick Queued job
//!   BackfillRegistry ◀─ tick / mark ────┤   reader = factory(source)
//!      (Queued)                         │   worker.run_job(
//!      (Running)                        │     job_id, filter,
//!      (Complete / Failed / Cancelled)  │     reader, processor)
//!                                       │   sleep(poll_interval)
//!                                       │ }
//! ```
//!
//! ## Reader factory
//!
//! The service doesn't know how to talk to Prometheus / S3
//! — those are network-backed and deployment-specific. Instead, the
//! caller provides a [`ReaderFactory`] that takes a `BackfillSource`
//! and returns an `Arc<dyn RawSampleReader>`. When no factory is
//! registered for a given source variant, the job fails with a clear
//! "no reader configured" message (better than hanging in Queued).
//!
//! Phase 5e-v1 doesn't ship a real PrometheusReader — that's Phase
//! 5h. Tests use `MockRawSampleReader`. Production deployments can
//! register a factory once the HTTP readers exist.
//!
//! ## Time-disjoint enforcement
//!
//! The service assumes jobs in the registry have passed
//! [`BackfillRegistry::create_checked`]'s time-disjoint validation.
//! It does NOT re-check the invariant — if a job snuck in via the
//! unchecked `create()` (tests only), it'll still run, and writes
//! could race live. The HTTP endpoint wiring is what enforces
//! "all production jobs go through `create_checked`".

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;
use tracing::{debug, info, warn};

use crate::storage_engines::types::HotReloadStreamingConfig;
use crate::storage_engines::sketch_db::backfill::{BackfillRegistry, BackfillSource, BackfillStatus};
use crate::storage_engines::sketch_db::backfill::processor::BackfillWindowProcessor;
use crate::storage_engines::sketch_db::backfill::worker::BackfillWorker;
use crate::storage_engines::sketch_db::backfill::raw_sample_reader::{LabelFilter, RawSampleReader};
use crate::storage_engines::sketch_db::schema::SchemaRegistry;

/// Given a `BackfillSource`, return a reader that can read raw
/// samples from it. Used by the service to pick a concrete reader
/// per job. Boxed fn because the caller will typically close over
/// deployment-specific config (HTTP clients, S3 creds) that can't
/// be reconstructed from `BackfillSource` alone.
pub type ReaderFactory = Arc<
    dyn Fn(
            &BackfillSource,
        ) -> Result<Arc<dyn RawSampleReader>, Box<dyn std::error::Error + Send + Sync>>
        + Send
        + Sync,
>;

/// Config for the [`BackfillService`] background task. Tunable
/// separately from the live-ingest pipeline since backfill should
/// not starve live.
#[derive(Clone, Debug)]
pub struct BackfillServiceConfig {
    /// How often to poll the registry for new Queued jobs when
    /// idle. Kept coarse (default 1s) — backfill is not
    /// latency-sensitive.
    pub poll_interval: Duration,
}

impl Default for BackfillServiceConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(1),
        }
    }
}

/// Long-running tokio task that drains Queued backfill jobs. Spawn
/// via [`Self::spawn`]; the returned handle can be used to stop
/// the loop gracefully.
pub struct BackfillService {
    registry: Arc<BackfillRegistry>,
    schemas: Arc<SchemaRegistry>,
    /// Phase 5 M2.3.6g — replayed batches land in `SketchStore` only;
    /// the legacy `Arc<dyn Store>` field is gone.
    sketch_index: Option<Arc<crate::storage_engines::sketch_db::store::SketchStore>>,
    config_source: HotReloadStreamingConfig,
    reader_factory: ReaderFactory,
    service_config: BackfillServiceConfig,
}

impl BackfillService {
    pub fn new(
        registry: Arc<BackfillRegistry>,
        schemas: Arc<SchemaRegistry>,
        config_source: HotReloadStreamingConfig,
        reader_factory: ReaderFactory,
        service_config: BackfillServiceConfig,
    ) -> Self {
        Self {
            registry,
            schemas,
            sketch_index: None,
            config_source,
            reader_factory,
            service_config,
        }
    }

    /// Attach a `SketchStore` so each replayed batch is also mirrored
    /// there. Builder-style; safe to omit (legacy tests).
    pub fn with_sketch_index(
        mut self,
        sketch_index: Arc<crate::storage_engines::sketch_db::store::SketchStore>,
    ) -> Self {
        self.sketch_index = Some(sketch_index);
        self
    }

    /// Spawn the service as a tokio task. Returns a `BackfillServiceHandle`
    /// with a `shutdown` oneshot so `main.rs` can stop it cleanly on
    /// ctrl-c.
    pub fn spawn(self) -> BackfillServiceHandle {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            self.run(shutdown_rx).await;
        });
        BackfillServiceHandle {
            task: Some(task),
            shutdown: Some(shutdown_tx),
        }
    }

    /// Main drain loop. Returns when `shutdown` is signalled.
    ///
    /// Pick-up policy: on each tick, take ONE queued job (sorted by
    /// `job_id` for determinism) and run it to completion. While
    /// running, the loop doesn't poll for new jobs — backfill
    /// serialisation is explicit in v1. Multi-worker parallelism
    /// is §11.4 follow-up work.
    async fn run(self, mut shutdown: oneshot::Receiver<()>) {
        info!(
            poll_interval_ms = self.service_config.poll_interval.as_millis(),
            "BackfillService starting drain loop"
        );
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => {
                    info!("BackfillService received shutdown signal");
                    return;
                }
                _ = tokio::time::sleep(self.service_config.poll_interval) => {}
            }

            let mut queued = self.registry.list_by_status(&BackfillStatus::Queued);
            if queued.is_empty() {
                continue;
            }
            queued.sort_by_key(|j| j.job_id);
            let job = queued.remove(0);

            debug!(
                job_id = job.job_id,
                agg_id = job.agg_id,
                start_ms = job.time_range.0,
                end_ms = job.time_range.1,
                "BackfillService picking up queued job"
            );

            let reader = match (self.reader_factory)(&job.source) {
                Ok(r) => r,
                Err(e) => {
                    let msg = format!("reader factory failed: {e}");
                    warn!(job_id = job.job_id, error = %msg, "marking job Failed");
                    self.registry.mark_failed(job.job_id, msg);
                    continue;
                }
            };

            // Build the per-job processor with the current config
            // snapshot. The processor snapshots again per window so
            // mid-job config swaps stay visible.
            let mut processor = BackfillWindowProcessor::new(
                self.config_source.clone(),
                self.schemas.clone(),
                self.registry.clone(),
                job.job_id,
            );
            if let Some(idx) = self.sketch_index.as_ref() {
                processor = processor.with_sketch_index(idx.clone());
            }
            let worker = BackfillWorker::new(self.registry.clone());

            let filter = LabelFilter::for_metric(
                self.config_source
                    .snapshot()
                    .get_aggregation_config(job.agg_id)
                    .map(|c| c.metric.clone())
                    .unwrap_or_default(),
            );
            match worker
                .run_job(job.job_id, &filter, reader.as_ref(), &processor)
                .await
            {
                Ok(()) => {
                    info!(
                        job_id = job.job_id,
                        status = ?self.registry.get(job.job_id).map(|j| j.status),
                        "BackfillService job complete"
                    );
                }
                Err(e) => {
                    warn!(
                        job_id = job.job_id,
                        error = %e,
                        "BackfillService job errored; worker already marked job Failed"
                    );
                }
            }
        }
    }
}

/// Handle returned by [`BackfillService::spawn`]. Dropping the
/// handle triggers shutdown via the oneshot (best-effort). Call
/// [`Self::shutdown`] to also await the task's exit.
pub struct BackfillServiceHandle {
    task: Option<tokio::task::JoinHandle<()>>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl BackfillServiceHandle {
    /// Signal the service to stop and await its exit. Idempotent.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for BackfillServiceHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        // Task is left to finish on its own — callers who want to
        // await should use `shutdown` instead of relying on Drop.
    }
}

/// A `ReaderFactory` that always returns
/// `Err("no reader registered for <source>")`. Useful as a default
/// in `main.rs` when no real readers are wired yet: posted jobs
/// get picked up, attempted, and fail fast with a clear reason.
pub fn noop_reader_factory() -> ReaderFactory {
    Arc::new(|source| {
        Err(format!("no RawSampleReader registered for source variant {source:?}").into())
    })
}

/// Production-ready `ReaderFactory` covering the source variants
/// whose readers ship in-tree as of Phase 5h:
///
/// * [`BackfillSource::Prometheus`] — routed to
///   [`super::prometheus_reader::PrometheusReader`].
///
/// All other variants (`S3Gorilla`, `OtherSketch`) return a clear
/// "not yet implemented" error, which the worker surfaces on
/// `BackfillJob::error_message` so the controller / operator sees
/// exactly which reader is missing.
pub fn default_reader_factory() -> ReaderFactory {
    Arc::new(|source| {
        match source {
        BackfillSource::Prometheus { url } => {
            let reader = super::prometheus_reader::PrometheusReader::new(url.clone());
            Ok(Arc::new(reader) as Arc<dyn RawSampleReader>)
        }
        BackfillSource::S3Gorilla { .. }
        | BackfillSource::OtherSketch { .. } => Err(format!(
            "reader for {source:?} not yet implemented; only Prometheus is wired in-tree as of Phase 5h"
        )
        .into()),
    }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_engines::types::StreamingConfig;
    use crate::storage_engines::sketch_db::backfill::raw_sample_reader::{MockRawSampleReader, RawSample};
    use asap_types::aggregation_config::AggregationConfig;
    use asap_types::enums::{AggregationType, WindowType};
    use promql_utilities::data_model::key_by_label_names::KeyByLabelNames;
    use std::sync::Mutex;

    fn sum_config(agg_id: u64, metric: &str) -> AggregationConfig {
        AggregationConfig::new(
            agg_id,
            AggregationType::Sum,
            String::new(),
            std::collections::HashMap::new(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            60,
            60,
            WindowType::Tumbling,
            String::new(),
            metric.to_string(),
            None,
            None,
            None,
        )
    }

    fn streaming_with(cfg: AggregationConfig) -> Arc<StreamingConfig> {
        let mut m = std::collections::HashMap::new();
        m.insert(cfg.aggregation_id, cfg);
        Arc::new(StreamingConfig::new(m))
    }

    async fn wait_for_status(
        registry: &BackfillRegistry,
        job_id: u64,
        target: BackfillStatus,
        timeout_ms: u64,
    ) -> BackfillStatus {
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
        loop {
            if let Some(j) = registry.get(job_id) {
                if j.status == target || j.status.is_terminal() {
                    return j.status;
                }
            }
            if std::time::Instant::now() >= deadline {
                return registry
                    .get(job_id)
                    .map(|j| j.status)
                    .unwrap_or(BackfillStatus::Queued);
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn service_drains_queued_job_to_complete() {
        let cfg = sum_config(1, "latency");
        let streaming = streaming_with(cfg);
        let hot = HotReloadStreamingConfig::from_arc(streaming.clone());
        let schemas = Arc::new(SchemaRegistry::from_streaming_config(&streaming));
        let registry = Arc::new(BackfillRegistry::new());

        // Factory returns a fresh mock reader per call — seeded with a
        // handful of samples that cover the job's range.
        let reader_factory: ReaderFactory = Arc::new(|_src| {
            Ok(Arc::new(MockRawSampleReader::new(vec![
                RawSample {
                    labels: "latency".into(),
                    timestamp_ms: 5,
                    value: 1.0,
                },
                RawSample {
                    labels: "latency".into(),
                    timestamp_ms: 15,
                    value: 2.0,
                },
            ])) as Arc<dyn RawSampleReader>)
        });

        let service = BackfillService::new(
            registry.clone(),
            schemas,
            hot,
            reader_factory,
            BackfillServiceConfig {
                poll_interval: Duration::from_millis(20),
            },
        );
        let handle = service.spawn();

        let job_id = registry.create(
            1,
            (0, 20),
            BackfillSource::Prometheus { url: "x".into() },
            2,
        );
        let status = wait_for_status(&registry, job_id, BackfillStatus::Complete, 2000).await;
        handle.shutdown().await;
        assert_eq!(status, BackfillStatus::Complete);
        assert_eq!(registry.windows_written_by(job_id).len(), 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn service_marks_job_failed_when_reader_factory_fails() {
        let cfg = sum_config(1, "latency");
        let streaming = streaming_with(cfg);
        let hot = HotReloadStreamingConfig::from_arc(streaming.clone());
        let schemas = Arc::new(SchemaRegistry::from_streaming_config(&streaming));
        let registry = Arc::new(BackfillRegistry::new());

        let service = BackfillService::new(
            registry.clone(),
            schemas,
            hot,
            noop_reader_factory(),
            BackfillServiceConfig {
                poll_interval: Duration::from_millis(20),
            },
        );
        let handle = service.spawn();

        let job_id = registry.create(
            1,
            (0, 20),
            BackfillSource::Prometheus { url: "x".into() },
            1,
        );
        let status = wait_for_status(&registry, job_id, BackfillStatus::Failed, 2000).await;
        handle.shutdown().await;
        assert_eq!(status, BackfillStatus::Failed);
        let job = registry.get(job_id).unwrap();
        assert!(job
            .error_message
            .unwrap()
            .contains("no RawSampleReader registered"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn service_processes_multiple_jobs_in_id_order() {
        let cfg = sum_config(1, "latency");
        let streaming = streaming_with(cfg);
        let hot = HotReloadStreamingConfig::from_arc(streaming.clone());
        let schemas = Arc::new(SchemaRegistry::from_streaming_config(&streaming));
        let registry = Arc::new(BackfillRegistry::new());

        // Factory records the order in which it's invoked.
        let invocations: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let invocations_clone = invocations.clone();
        let reader_factory: ReaderFactory = Arc::new(move |src| {
            invocations_clone.lock().unwrap().push(format!("{src:?}"));
            Ok(Arc::new(MockRawSampleReader::new(vec![])) as Arc<dyn RawSampleReader>)
        });

        let service = BackfillService::new(
            registry.clone(),
            schemas,
            hot,
            reader_factory,
            BackfillServiceConfig {
                poll_interval: Duration::from_millis(20),
            },
        );
        let handle = service.spawn();

        let job1 = registry.create(
            1,
            (0, 10),
            BackfillSource::Prometheus { url: "a".into() },
            1,
        );
        let job2 = registry.create(
            1,
            (0, 10),
            BackfillSource::Prometheus { url: "b".into() },
            1,
        );
        let job3 = registry.create(
            1,
            (0, 10),
            BackfillSource::Prometheus { url: "c".into() },
            1,
        );

        for id in [job1, job2, job3] {
            let _ = wait_for_status(&registry, id, BackfillStatus::Complete, 2000).await;
        }
        handle.shutdown().await;

        // All three completed.
        assert_eq!(registry.get(job1).unwrap().status, BackfillStatus::Complete);
        assert_eq!(registry.get(job2).unwrap().status, BackfillStatus::Complete);
        assert_eq!(registry.get(job3).unwrap().status, BackfillStatus::Complete);

        // Factory was invoked in job_id order (a, b, c).
        let got = invocations.lock().unwrap().clone();
        assert_eq!(got.len(), 3);
        assert!(got[0].contains("\"a\""), "first invocation: {:?}", got[0]);
        assert!(got[1].contains("\"b\""), "second invocation: {:?}", got[1]);
        assert!(got[2].contains("\"c\""), "third invocation: {:?}", got[2]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn service_shutdown_stops_the_loop() {
        let cfg = sum_config(1, "m");
        let streaming = streaming_with(cfg);
        let hot = HotReloadStreamingConfig::from_arc(streaming.clone());
        let schemas = Arc::new(SchemaRegistry::from_streaming_config(&streaming));
        let registry = Arc::new(BackfillRegistry::new());

        let service = BackfillService::new(
            registry,
            schemas,
            hot,
            noop_reader_factory(),
            BackfillServiceConfig {
                poll_interval: Duration::from_millis(50),
            },
        );
        let handle = service.spawn();
        handle.shutdown().await;
        // If this test doesn't hang, shutdown worked.
    }
}
