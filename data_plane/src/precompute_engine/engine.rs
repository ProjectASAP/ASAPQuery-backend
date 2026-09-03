use crate::precompute_engine::config::PrecomputeEngineConfig;
use crate::precompute_engine::ingest_handler::IngestState;
use crate::precompute_engine::output_sink::OutputSink;
use crate::precompute_engine::series_router::{SeriesRouter, WorkerMessage};
use crate::precompute_engine::worker::{Worker, WorkerRuntimeConfig};
use crate::storage_engines::types::HotReloadStreamingConfig;
use std::sync::atomic::{AtomicI64, AtomicUsize};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, warn};

/// Shared diagnostic counters readable from outside the engine.
pub struct PrecomputeWorkerDiagnostics {
    pub worker_group_counts: Vec<Arc<AtomicUsize>>,
    pub worker_watermarks: Vec<Arc<AtomicI64>>,
}

/// The top-level precompute engine orchestrator.
///
/// Creates worker threads and the series router. The ingest state
/// (router + hot-reload handle) is built eagerly in `new()` so that
/// ingest sources (currently OTLP) can hold a handle and push data
/// into the same worker pool. The legacy Prometheus / VictoriaMetrics
/// remote-write HTTP listener was deleted alongside the rest of the
/// remote-write ingest path — backend ingest is OTLP-only now.
pub struct PrecomputeEngine {
    config: PrecomputeEngineConfig,
    output_sink: Arc<dyn OutputSink>,
    diagnostics: Arc<PrecomputeWorkerDiagnostics>,
    ingest_state: Arc<IngestState>,
    hot_reload_config: HotReloadStreamingConfig,
    /// Worker receivers, one per worker. Taken by `run()` when spawning workers.
    receivers: Vec<mpsc::Receiver<WorkerMessage>>,
}

impl PrecomputeEngine {
    pub fn new(
        config: PrecomputeEngineConfig,
        hot_reload_config: HotReloadStreamingConfig,
        output_sink: Arc<dyn OutputSink>,
        series_resolver: Arc<crate::drivers::ingest::series_resolver::SeriesIdResolver>,
        sketch_index: Arc<crate::storage_engines::sketch_db::index::SketchStore>,
    ) -> Self {
        let worker_group_counts = (0..config.num_workers)
            .map(|_| Arc::new(AtomicUsize::new(0)))
            .collect();
        let worker_watermarks = (0..config.num_workers)
            .map(|_| Arc::new(AtomicI64::new(i64::MIN)))
            .collect();
        let diagnostics = Arc::new(PrecomputeWorkerDiagnostics {
            worker_group_counts,
            worker_watermarks,
        });

        // Build MPSC channels for each worker up front.
        let num_workers = config.num_workers;
        let channel_size = config.channel_buffer_size;
        let mut senders = Vec::with_capacity(num_workers);
        let mut receivers = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            let (tx, rx) = mpsc::channel::<WorkerMessage>(channel_size);
            senders.push(tx);
            receivers.push(rx);
        }

        // Build the router that owns the senders; it will be shared via IngestState.
        let router = SeriesRouter::new(senders);

        // Ingest state holds the hot-reload handle — it re-snapshots
        // agg_configs on each ingest batch, so new aggregations from
        // a config swap are visible immediately.
        //
        // The agg_id-keyed `SchemaRegistry` has been retired — sid-level
        // lifecycle status now lives on `SketchStore` and reconcile
        // runs against the same snapshot the ingest path consults.
        let ingest_state = Arc::new(IngestState {
            router,
            samples_ingested: std::sync::atomic::AtomicU64::new(0),
            samples_blocked_by_schema_barrier: std::sync::atomic::AtomicU64::new(0),
            hot_reload_config: hot_reload_config.clone(),
            pass_raw_samples: config.pass_raw_samples,
            sketch_snapshots: dashmap::DashMap::new(),
            series_resolver,
            sketch_index,
            observability: crate::precompute_engine::ingest_handler::IngestObservability::new(),
        });

        Self {
            config,
            output_sink,
            diagnostics,
            ingest_state,
            hot_reload_config,
            receivers,
        }
    }

    /// Get a handle to worker diagnostics, readable even after `run()` starts.
    pub fn diagnostics(&self) -> Arc<PrecomputeWorkerDiagnostics> {
        self.diagnostics.clone()
    }

    /// Get a clonable handle to the shared ingest state. Other ingest sources
    /// (OTLP, Kafka, etc.) call this before `run()` to push into the same
    /// worker pool.
    pub fn ingest_state(&self) -> Arc<IngestState> {
        self.ingest_state.clone()
    }

    /// Start the precompute engine. This spawns worker tasks and the
    /// periodic flush timer, then blocks until shutdown. The legacy
    /// Prometheus / VictoriaMetrics HTTP ingest listener has been
    /// removed; ingest now flows in via the OTLP receiver, which holds
    /// the same `IngestState` handle returned by `ingest_state()`.
    pub async fn run(mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let num_workers = self.config.num_workers;

        // Take ownership of receivers (they can only be used once).
        let receivers = std::mem::take(&mut self.receivers);

        // Spawn workers. Each worker holds a clone of the hot-reload
        // handle and reads config directly from ArcSwap — no
        // ConfigReload messages needed.
        let mut worker_handles = Vec::with_capacity(num_workers);
        for (id, rx) in receivers.into_iter().enumerate() {
            let worker = Worker::new(
                id,
                rx,
                self.output_sink.clone(),
                self.hot_reload_config.clone(),
                WorkerRuntimeConfig {
                    max_buffer_per_series: self.config.max_buffer_per_series,
                    allowed_lateness_ms: self.config.allowed_lateness_ms,
                    pass_raw_samples: self.config.pass_raw_samples,
                    raw_mode_aggregation_id: self.config.raw_mode_aggregation_id,
                    late_data_policy: self.config.late_data_policy,
                    wall_clock_idle_grace_period_ms: self.config.wall_clock_idle_grace_period_ms,
                    wall_clock_max_open_grace_period_ms: self
                        .config
                        .wall_clock_max_open_grace_period_ms,
                },
                self.diagnostics.worker_group_counts[id].clone(),
                self.diagnostics.worker_watermarks[id].clone(),
            );
            let handle = tokio::spawn(async move {
                worker.run().await;
            });
            worker_handles.push(handle);
        }

        info!("PrecomputeEngine started with {} workers", num_workers);

        let ingest_state = self.ingest_state.clone();

        // Start flush timer — pure flush, no config polling.
        let flush_state = ingest_state.clone();
        let flush_interval_ms = self.config.flush_interval_ms;
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_millis(flush_interval_ms));
            loop {
                interval.tick().await;
                if let Err(e) = flush_state.router.broadcast_flush().await {
                    warn!("Flush broadcast error: {}", e);
                    break;
                }
            }
        });

        // Wait for workers to finish (this only happens on shutdown).
        for handle in worker_handles {
            let _ = handle.await;
        }

        Ok(())
    }
}
