use clap::Parser;
use query_engine_rust::data_model::QueryLanguage;
use query_engine_rust::data_model::{
    CleanupPolicy, InferenceConfig, LockStrategy, StreamingConfig,
};
use query_engine_rust::drivers::query::adapters::AdapterConfig;
use query_engine_rust::engines::SimpleEngine;
use query_engine_rust::precompute_engine::config::{LateDataPolicy, PrecomputeEngineConfig};
use query_engine_rust::precompute_engine::output_sink::{RawPassthroughSink, StoreOutputSink};
use query_engine_rust::precompute_engine::PrecomputeEngine;
use query_engine_rust::stores::SimpleMapStore;
use query_engine_rust::{HttpServer, HttpServerConfig};
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::fmt::format::FmtSpan;

#[derive(Parser, Debug)]
#[command(name = "precompute_engine")]
#[command(about = "Standalone precompute engine for SketchDB")]
struct Args {
    /// Path to streaming config YAML file
    #[arg(long)]
    streaming_config: String,

    /// Port for Prometheus remote write ingest
    #[arg(long, default_value_t = 9090)]
    ingest_port: u16,

    /// Number of worker threads
    #[arg(long, default_value_t = 4)]
    num_workers: usize,

    /// Maximum allowed lateness for out-of-order samples (ms)
    #[arg(long, default_value_t = 5000)]
    allowed_lateness_ms: i64,

    /// Maximum buffered samples per series
    #[arg(long, default_value_t = 10000)]
    max_buffer_per_series: usize,

    /// Flush interval for idle window detection (ms)
    #[arg(long, default_value_t = 1000)]
    flush_interval_ms: u64,

    /// MPSC channel buffer size per worker
    #[arg(long, default_value_t = 10000)]
    channel_buffer_size: usize,

    /// Port for the query HTTP server (0 to disable)
    #[arg(long, default_value_t = 8080)]
    query_port: u16,

    /// Lock strategy for the store
    #[arg(long, value_enum, default_value_t = LockStrategy::PerKey)]
    lock_strategy: LockStrategy,

    /// Skip aggregation and pass each raw sample directly to the store
    #[arg(long, default_value_t = false)]
    pass_raw_samples: bool,

    /// Aggregation ID to stamp on each raw-mode output
    #[arg(long, default_value_t = 0)]
    raw_mode_aggregation_id: u64,

    /// Policy for handling late samples that arrive after their window has closed
    #[arg(long, value_enum, default_value_t = LateDataPolicy::Drop)]
    late_data_policy: LateDataPolicy,

    // ---- SimpleMapStore persistence ----
    /// Enable the disk-backed persistence layer for SimpleMapStore.
    /// When set, forces LockStrategy::PerKey regardless of --lock-strategy.
    #[arg(long, default_value_t = false)]
    persistence_enabled: bool,

    /// Root directory for persistence (manifest + parts/).
    #[arg(long)]
    persistence_dir: Option<String>,

    /// Primary memory budget for in-memory sealed epochs, in MiB.
    #[arg(long, default_value_t = 2048)]
    persistence_memory_limit_mb: usize,

    /// Hot-window length in seconds. 0 disables time-based flushing.
    #[arg(long, default_value_t = 3600)]
    persistence_hot_window_secs: u64,

    /// Cold-tier TTL in seconds. 0 disables disk retention.
    #[arg(long, default_value_t = 604800)]
    persistence_delete_older_than_secs: u64,

    /// Cadence of the flusher loop, in milliseconds.
    #[arg(long, default_value_t = 1000)]
    persistence_flush_interval_ms: u64,

    /// Tier-2 part-cache byte budget, in MiB. 0 disables.
    #[arg(long)]
    persistence_part_cache_mb: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_span_events(FmtSpan::CLOSE)
        .init();

    let args = Args::parse();

    info!("Loading streaming config from: {}", args.streaming_config);
    let streaming_config = Arc::new(StreamingConfig::from_yaml_file(&args.streaming_config)?);

    info!(
        "Loaded {} aggregation configs",
        streaming_config.get_all_aggregation_configs().len()
    );

    // Create the store
    let store: Arc<dyn query_engine_rust::stores::Store> = if args.persistence_enabled {
        use query_engine_rust::stores::simple_map_store::persistence::SimpleMapStorePersistenceConfig;
        let disk_path = args
            .persistence_dir
            .clone()
            .expect("--persistence-enabled requires --persistence-dir");
        let memory_limit_bytes = args.persistence_memory_limit_mb * 1024 * 1024;
        let hot_window_ms = if args.persistence_hot_window_secs == 0 {
            None
        } else {
            Some(args.persistence_hot_window_secs * 1000)
        };
        let delete_older_than_ms = if args.persistence_delete_older_than_secs == 0 {
            None
        } else {
            Some(args.persistence_delete_older_than_secs * 1000)
        };
        let part_cache_bytes = args
            .persistence_part_cache_mb
            .map(|mb| mb * 1024 * 1024)
            .unwrap_or_else(|| {
                let ten_pct = (memory_limit_bytes / 10) as u64;
                ten_pct.min(512 * 1024 * 1024)
            });
        let persistence_cfg = SimpleMapStorePersistenceConfig {
            memory_limit_bytes,
            memory_low_watermark_bytes: memory_limit_bytes * 8 / 10,
            hard_cap_bytes: memory_limit_bytes * 125 / 100,
            hot_window_ms,
            delete_older_than_ms,
            flush_interval: std::time::Duration::from_millis(args.persistence_flush_interval_ms),
            disk_path: std::path::PathBuf::from(&disk_path),
            part_cache_bytes,
        };
        info!(
            "Persistence enabled: disk_path={}, memory_limit={} MiB, hot_window={:?} s, flush_interval={} ms",
            disk_path,
            args.persistence_memory_limit_mb,
            persistence_cfg.hot_window_ms.map(|ms| ms / 1000),
            args.persistence_flush_interval_ms,
        );
        Arc::new(
            SimpleMapStore::with_persistence_per_key(
                streaming_config.clone(),
                CleanupPolicy::CircularBuffer,
                persistence_cfg,
            )
            .expect("SimpleMapStore::with_persistence_per_key failed"),
        )
    } else {
        Arc::new(SimpleMapStore::new_with_strategy(
            streaming_config.clone(),
            CleanupPolicy::CircularBuffer,
            args.lock_strategy,
        ))
    };

    // Optionally start the query HTTP server
    if args.query_port > 0 {
        let inference_config =
            InferenceConfig::new(QueryLanguage::promql, CleanupPolicy::CircularBuffer);
        let query_engine = Arc::new(SimpleEngine::new(
            store.clone(),
            inference_config,
            streaming_config.clone(),
            15, // default prometheus scrape interval
            QueryLanguage::promql,
        ));
        let http_config = HttpServerConfig {
            port: args.query_port,
            handle_http_requests: true,
            adapter_config: AdapterConfig {
                protocol: query_engine_rust::data_model::QueryProtocol::PrometheusHttp,
                language: QueryLanguage::promql,
                fallback: None,
            },
        };
        let http_server = HttpServer::new(http_config, query_engine, store.clone(), None);
        tokio::spawn(async move {
            if let Err(e) = http_server.run().await {
                tracing::error!("Query server error: {}", e);
            }
        });
        info!("Query server started on port {}", args.query_port);
    }

    // Build the precompute engine config
    let engine_config = PrecomputeEngineConfig {
        num_workers: args.num_workers,
        ingest_port: args.ingest_port,
        allowed_lateness_ms: args.allowed_lateness_ms,
        max_buffer_per_series: args.max_buffer_per_series,
        flush_interval_ms: args.flush_interval_ms,
        channel_buffer_size: args.channel_buffer_size,
        pass_raw_samples: args.pass_raw_samples,
        raw_mode_aggregation_id: args.raw_mode_aggregation_id,
        late_data_policy: args.late_data_policy,
        schema_persist_path: None,
    };

    // Create the output sink (writes directly to the store)
    let output_sink: Arc<dyn query_engine_rust::precompute_engine::output_sink::OutputSink> =
        if args.pass_raw_samples {
            Arc::new(RawPassthroughSink::new(store))
        } else {
            Arc::new(StoreOutputSink::new(store))
        };

    // Build and run the engine
    let engine = PrecomputeEngine::new(
        engine_config,
        query_engine_rust::data_model::HotReloadStreamingConfig::from_arc(streaming_config),
        output_sink,
    );

    info!("Starting precompute engine...");
    engine.run().await?;

    Ok(())
}
