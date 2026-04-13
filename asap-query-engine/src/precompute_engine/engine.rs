use crate::data_model::StreamingConfig;
use crate::precompute_engine::config::PrecomputeEngineConfig;
use crate::precompute_engine::ingest_handler::{
    handle_prometheus_ingest, handle_victoriametrics_ingest, IngestState,
};
use crate::precompute_engine::output_sink::OutputSink;
use crate::precompute_engine::series_router::{SeriesRouter, WorkerMessage};
use crate::precompute_engine::worker::{Worker, WorkerRuntimeConfig};
use asap_types::aggregation_config::AggregationConfig;
use axum::{routing::post, Router};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicUsize};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{info, warn};

/// Shared diagnostic counters readable from outside the engine.
pub struct PrecomputeWorkerDiagnostics {
    pub worker_group_counts: Vec<Arc<AtomicUsize>>,
    pub worker_watermarks: Vec<Arc<AtomicI64>>,
}

/// The top-level precompute engine orchestrator.
///
/// Creates worker threads, the series router, and the Axum ingest server.
/// The ingest state (router + agg configs) is built eagerly in `new()` so
/// that other ingest sources (e.g. OTLP) can hold a handle and push data
/// into the same worker pool.
pub struct PrecomputeEngine {
    config: PrecomputeEngineConfig,
    output_sink: Arc<dyn OutputSink>,
    diagnostics: Arc<PrecomputeWorkerDiagnostics>,
    ingest_state: Arc<IngestState>,
    agg_configs_map: HashMap<u64, Arc<AggregationConfig>>,
    /// Worker receivers, one per worker. Taken by `run()` when spawning workers.
    receivers: Vec<mpsc::Receiver<WorkerMessage>>,
}

impl PrecomputeEngine {
    pub fn new(
        config: PrecomputeEngineConfig,
        streaming_config: Arc<StreamingConfig>,
        output_sink: Arc<dyn OutputSink>,
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

        // Resolve all aggregation configs from the streaming config. Wrap each
        // in Arc so workers can share one copy per aggregation.
        let agg_configs_map: HashMap<u64, Arc<AggregationConfig>> = streaming_config
            .get_all_aggregation_configs()
            .iter()
            .map(|(&id, cfg)| (id, Arc::new(cfg.clone())))
            .collect();
        let agg_configs_vec: Vec<Arc<AggregationConfig>> =
            agg_configs_map.values().cloned().collect();

        // Ingest state holds the router and all configs; this is the single
        // handle any ingest source uses to push work onto the worker pool.
        let ingest_state = Arc::new(IngestState {
            router,
            samples_ingested: std::sync::atomic::AtomicU64::new(0),
            agg_configs: agg_configs_vec,
            pass_raw_samples: config.pass_raw_samples,
        });

        Self {
            config,
            output_sink,
            diagnostics,
            ingest_state,
            agg_configs_map,
            receivers,
        }
    }

    /// Get a handle to worker diagnostics, readable even after `run()` starts.
    pub fn diagnostics(&self) -> Arc<PrecomputeWorkerDiagnostics> {
        self.diagnostics.clone()
    }

    /// Get a clonable handle to the shared ingest state. Other ingest sources
    /// (OTLP, Kafka, etc.) call this before `run()` to push into the same
    /// worker pool as the built-in Prometheus/VM HTTP server.
    pub fn ingest_state(&self) -> Arc<IngestState> {
        self.ingest_state.clone()
    }

    /// Start the precompute engine. This spawns worker tasks and the HTTP
    /// ingest server, then blocks until shutdown.
    pub async fn run(mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let num_workers = self.config.num_workers;

        // Take ownership of receivers (they can only be used once).
        let receivers = std::mem::take(&mut self.receivers);

        // Spawn workers.
        let mut worker_handles = Vec::with_capacity(num_workers);
        for (id, rx) in receivers.into_iter().enumerate() {
            let worker = Worker::new(
                id,
                rx,
                self.output_sink.clone(),
                self.agg_configs_map.clone(),
                WorkerRuntimeConfig {
                    max_buffer_per_series: self.config.max_buffer_per_series,
                    allowed_lateness_ms: self.config.allowed_lateness_ms,
                    pass_raw_samples: self.config.pass_raw_samples,
                    raw_mode_aggregation_id: self.config.raw_mode_aggregation_id,
                    late_data_policy: self.config.late_data_policy,
                },
                self.diagnostics.worker_group_counts[id].clone(),
                self.diagnostics.worker_watermarks[id].clone(),
                self.diagnostics.worker_watermarks.to_vec(),
            );
            let handle = tokio::spawn(async move {
                worker.run().await;
            });
            worker_handles.push(handle);
        }

        info!(
            "PrecomputeEngine started with {} workers on port {}",
            num_workers, self.config.ingest_port
        );

        let ingest_state = self.ingest_state.clone();

        // Start flush timer.
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

        // Start the Axum HTTP server for ingest (Prometheus + VictoriaMetrics).
        let app = Router::new()
            .route("/api/v1/write", post(handle_prometheus_ingest))
            .route("/api/v1/import", post(handle_victoriametrics_ingest))
            .with_state(ingest_state);

        let addr = format!("0.0.0.0:{}", self.config.ingest_port);
        info!("Ingest server listening on {}", addr);

        let listener = TcpListener::bind(&addr).await?;
        axum::serve(listener, app).await?;

        // Wait for workers to finish (this only happens on shutdown).
        for handle in worker_handles {
            let _ = handle.await;
        }

        Ok(())
    }
}
