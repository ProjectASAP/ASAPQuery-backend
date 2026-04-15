use clap::Parser;
use query_engine_rust::data_model::QueryLanguage;
use std::fs;
use std::sync::Arc;
use tokio::signal;
use tracing::{error, info};

use sketch_core::config::{self, ImplMode};

use query_engine_rust::data_model::enums::{InputFormat, LockStrategy, StreamingEngine};
use query_engine_rust::drivers::AdapterConfig;
use query_engine_rust::precompute_engine::config::LateDataPolicy;
use query_engine_rust::precompute_engine::PrecomputeWorkerDiagnostics;
use query_engine_rust::utils::file_io::{read_inference_config, read_streaming_config};
use query_engine_rust::{
    HttpServer, HttpServerConfig, KafkaConsumer, KafkaConsumerConfig, OtlpReceiver,
    OtlpReceiverConfig, PrecomputeEngine, PrecomputeEngineConfig, Result, SimpleEngine,
    SimpleMapStore, StoreOutputSink,
};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Kafka topic to consume from (required when streaming-engine=arroyo)
    #[arg(long)]
    kafka_topic: Option<String>,

    /// Input format for Kafka messages (required when streaming-engine=arroyo)
    #[arg(long, value_enum)]
    input_format: Option<InputFormat>,

    /// Configuration file path
    #[arg(long)]
    config: String,

    /// File path for streaming_config
    #[arg(long)]
    streaming_config: String,

    /// Streaming engine to use
    #[arg(long, value_enum, default_value = "arroyo")]
    streaming_engine: StreamingEngine,

    /// Prometheus scrape interval in seconds
    #[arg(long)]
    prometheus_scrape_interval: u64,

    /// HTTP server port
    #[arg(long, default_value = "8088")]
    http_port: u16,

    /// Prometheus server URL
    #[arg(long, default_value = "http://localhost:9090")]
    prometheus_server: String,

    /// Forward unsupported queries to Prometheus
    #[arg(long)]
    forward_unsupported_queries: bool,

    /// Kafka broker address
    #[arg(long, default_value = "localhost:9092")]
    kafka_broker: String,

    /// Database path (currently unused, kept for compatibility)
    #[arg(long, default_value = "sketchdb.db")]
    db_path: String,

    /// Delete existing database (currently unused, kept for compatibility)
    #[arg(long)]
    delete_existing_db: bool,

    /// Output directory for logs
    #[arg(long)]
    output_dir: String,

    /// Log level
    #[arg(long, default_value = "INFO")]
    log_level: String,

    /// Enable profiling (currently unused, kept for compatibility)
    #[arg(long)]
    do_profiling: bool,

    /// Decompress JSON messages
    #[arg(long)]
    decompress_json: bool,

    /// Enable dumping received precomputes to files for debugging
    #[arg(long)]
    dump_precomputes: bool,

    /// Differentiate between query languages of input query
    #[arg(long, value_enum)]
    query_language: QueryLanguage,

    /// Lock strategy for SimpleMapStore: "global" for single mutex, "per-key" for fine-grained locking
    #[arg(long, value_enum)]
    lock_strategy: LockStrategy,

    /// Enable Prometheus remote write ingest endpoint
    #[arg(long)]
    enable_prometheus_remote_write: bool,

    /// Port for the Prometheus remote write endpoint
    #[arg(long, default_value = "9090")]
    prometheus_remote_write_port: u16,

    /// Path to promsketch configuration YAML file (optional; uses defaults if omitted)
    #[arg(long)]
    promsketch_config: Option<String>,

    /// Backend implementation for Count-Min Sketch (legacy | sketchlib)
    #[arg(long, value_enum, default_value_t = config::DEFAULT_CMS_IMPL)]
    sketch_cms_impl: ImplMode,

    /// Backend implementation for KLL Sketch (legacy | sketchlib)
    #[arg(long, value_enum, default_value_t = config::DEFAULT_KLL_IMPL)]
    sketch_kll_impl: ImplMode,

    /// Backend implementation for Count-Min-With-Heap (legacy | sketchlib)
    #[arg(long, value_enum, default_value_t = config::DEFAULT_CMWH_IMPL)]
    sketch_cmwh_impl: ImplMode,

    /// Enable OTLP metrics ingest (gRPC + HTTP)
    #[arg(long)]
    enable_otel_ingest: bool,

    /// OTLP gRPC listen port
    #[arg(long, default_value = "4317")]
    otel_grpc_port: u16,

    /// OTLP HTTP listen port
    #[arg(long, default_value = "4318")]
    otel_http_port: u16,

    /// Number of precompute engine worker threads
    #[arg(long, default_value = "4")]
    precompute_num_workers: usize,

    /// Maximum allowed lateness for out-of-order samples (milliseconds)
    #[arg(long, default_value = "5000")]
    precompute_allowed_lateness_ms: i64,

    /// Maximum buffered samples per series before eviction
    #[arg(long, default_value = "10000")]
    precompute_max_buffer_per_series: usize,

    /// Interval at which the flush timer fires (milliseconds)
    #[arg(long, default_value = "1000")]
    precompute_flush_interval_ms: u64,

    /// Capacity of the channel between router and each worker
    #[arg(long, default_value = "10000")]
    precompute_channel_buffer_size: usize,

    /// Enable automatic query tracking and planning
    #[arg(long)]
    enable_query_tracker: bool,

    /// Query tracker: observation window in seconds before triggering planning
    #[arg(long, default_value = "100")]
    tracker_observation_window_secs: u64,

    // ---- SimpleMapStore persistence ----
    //
    // When --persistence-enabled is set, the store is constructed via
    // SimpleMapStore::with_persistence_per_key with the other
    // --persistence-* flags as the config. Forces LockStrategy::PerKey
    // regardless of --lock-strategy; the Global variant is
    // intentionally left in-memory-only.
    /// Enable the disk-backed persistence layer for SimpleMapStore
    #[arg(long)]
    persistence_enabled: bool,

    /// Root directory for persistence (manifest + parts/). Required
    /// when --persistence-enabled.
    #[arg(long)]
    persistence_dir: Option<String>,

    /// Primary memory budget for in-memory sealed epochs, in MiB.
    /// When exceeded, the background flusher evicts oldest-first.
    #[arg(long, default_value = "2048")]
    persistence_memory_limit_mb: usize,

    /// Hot-window length in seconds. Any sealed epoch whose end_ts is
    /// older than (now - this) is flushed on the next flusher tick,
    /// regardless of memory pressure. 0 disables time-based flushing.
    #[arg(long, default_value = "3600")]
    persistence_hot_window_secs: u64,

    /// Cold-tier TTL in seconds. Parts whose max_ts is older than
    /// (now - this) are removed from disk on the next flusher tick.
    /// 0 disables disk retention.
    #[arg(long, default_value = "604800")]
    persistence_delete_older_than_secs: u64,

    /// Cadence of the background flusher loop, in milliseconds.
    #[arg(long, default_value = "1000")]
    persistence_flush_interval_ms: u64,

    /// Tier-2 part-cache byte budget, in MiB. 0 disables the cache.
    /// Defaults to `min(10% * memory_limit_mb, 512)`.
    #[arg(long)]
    persistence_part_cache_mb: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Configure sketch-core backends before any sketch operations.
    config::configure(
        args.sketch_cms_impl,
        args.sketch_kll_impl,
        args.sketch_cmwh_impl,
    )
    .expect("sketch backend already initialised");

    // Create output directory
    fs::create_dir_all(&args.output_dir)?;

    // Initialize logging similar to Python's create_loggers function
    // Keep the guard alive for the entire lifetime of the application
    let _log_guard = setup_logging(&args.output_dir, &args.log_level)?;

    info!("Starting Query Engine Rust");
    info!("Config file: {}", args.config);
    info!("Output directory: {}", args.output_dir);

    // Read config (equivalent to utils.file_io.read_inference_config)
    let inference_config = read_inference_config(&args.config, args.query_language)?;
    info!(
        "Loaded inference config with {} query configs",
        inference_config.query_configs.len()
    );
    info!("Inference config: {:?}", inference_config);

    let streaming_config = Arc::new(read_streaming_config(
        &args.streaming_config,
        &inference_config,
    )?);
    info!(
        "Loaded streaming config with {} entries",
        streaming_config.get_all_aggregation_configs().len()
    );
    info!("Streaming config: {:?}", streaming_config);

    // Wrap the streaming config in a hot-reload handle so the HTTP
    // server's `/api/v1/streaming-config` endpoints can swap it at
    // runtime (PR E phase 1). Existing consumers downstream
    // (SimpleEngine, PrecomputeEngine, Store) still take their
    // startup snapshot; hot-reload currently only affects the
    // control-plane GET/POST endpoint. Phase 2 will extend the swap
    // to query execution and ingest routing.
    let hot_reload_config =
        query_engine_rust::data_model::HotReloadStreamingConfig::from_arc(streaming_config.clone());

    // Setup store (equivalent to Python's SimpleMapStore())
    // Get cleanup policy from inference config
    let cleanup_policy = inference_config.cleanup_policy;
    info!("Using cleanup policy: {:?}", cleanup_policy);
    let store = if args.persistence_enabled {
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
            "Persistence enabled: disk_path={}, memory_limit={} MiB, hot_window={:?} s, delete_older_than={:?} s, flush_interval={} ms, part_cache={} MiB",
            disk_path,
            args.persistence_memory_limit_mb,
            persistence_cfg.hot_window_ms.map(|ms| ms / 1000),
            persistence_cfg.delete_older_than_ms.map(|ms| ms / 1000),
            args.persistence_flush_interval_ms,
            persistence_cfg.part_cache_bytes / (1024 * 1024),
        );
        if !matches!(args.lock_strategy, LockStrategy::PerKey) {
            info!("--persistence-enabled forces LockStrategy::PerKey (ignoring --lock-strategy)");
        }
        Arc::new(
            SimpleMapStore::with_persistence_per_key(
                streaming_config.clone(),
                cleanup_policy,
                persistence_cfg,
            )
            .expect("SimpleMapStore::with_persistence_per_key failed"),
        )
    } else {
        Arc::new(SimpleMapStore::new_with_strategy(
            streaming_config.clone(),
            cleanup_policy,
            args.lock_strategy,
        ))
    };

    // // Setup PromSketchStore (shared between engine and remote write server)
    // let promsketch_store = if args.enable_prometheus_remote_write {
    //     let promsketch_config = match &args.promsketch_config {
    //         Some(path) => {
    //             let cfg = read_promsketch_config(path)?;
    //             info!("Loaded promsketch config from {}: {:?}", path, cfg);
    //             cfg
    //         }
    //         None => {
    //             info!("Using default promsketch config");
    //             PromSketchConfig::default()
    //         }
    //     };
    //     info!("Prometheus remote write enabled: creating PromSketchStore");
    //     Some(Arc::new(PromSketchStore::new(promsketch_config)))
    // } else {
    //     None
    // };

    // Setup query engine
    let engine = Arc::new(SimpleEngine::new(
        store.clone(),
        // promsketch_store.clone(),
        inference_config,
        streaming_config.clone(),
        args.prometheus_scrape_interval,
        args.query_language,
    ));

    // Setup Kafka consumer (only when not using precompute engine as the streaming backend)
    let kafka_handle = if args.streaming_engine == StreamingEngine::Precompute {
        info!("Using precompute engine as streaming backend — skipping Kafka consumer");
        None
    } else {
        let kafka_topic = args.kafka_topic.clone().unwrap_or_else(|| {
            error!("--kafka-topic is required when --streaming-engine is not precompute");
            std::process::exit(1);
        });
        let input_format = args.input_format.unwrap_or_else(|| {
            error!("--input-format is required when --streaming-engine is not precompute");
            std::process::exit(1);
        });
        let kafka_config = KafkaConsumerConfig {
            broker: args.kafka_broker.clone(),
            topic: kafka_topic.clone(),
            group_id: "query-engine-rust".to_string(),
            auto_offset_reset: "beginning".to_string(),
            input_format,
            decompress_json: args.decompress_json,
            batch_size: 1000,
            poll_timeout_ms: 1000,
            streaming_engine: args.streaming_engine.clone(),
            dump_precomputes: args.dump_precomputes,
            dump_output_dir: if args.dump_precomputes {
                Some(args.output_dir.clone())
            } else {
                None
            },
        };

        let store_for_kafka = store.clone();
        let kafka_consumer_result =
            KafkaConsumer::new(kafka_config, store_for_kafka, streaming_config.clone());
        match kafka_consumer_result {
            Ok(mut consumer) => {
                info!("Starting Kafka consumer for topic: {}", kafka_topic);
                Some(tokio::spawn(async move {
                    if let Err(e) = consumer.run().await {
                        error!("Kafka consumer error: {}", e);
                    }
                }))
            }
            Err(e) => {
                error!("Failed to create Kafka consumer: {}", e);
                info!("Continuing without Kafka consumer");
                None
            }
        }
    };

    // Setup precompute engine (replaces standalone Prometheus remote write server)
    // Automatically enable when using precompute streaming engine.
    //
    // NOTE: precompute is constructed BEFORE the OTLP receiver so the receiver
    // can obtain an `Arc<IngestState>` handle and push OTLP metrics / sketches
    // into the same worker pool (and not just write directly to the store).
    let enable_precompute =
        args.enable_prometheus_remote_write || args.streaming_engine == StreamingEngine::Precompute;
    let (precompute_handle, precompute_ingest_state) = if enable_precompute {
        let precompute_config = PrecomputeEngineConfig {
            num_workers: args.precompute_num_workers,
            ingest_port: args.prometheus_remote_write_port,
            allowed_lateness_ms: args.precompute_allowed_lateness_ms,
            max_buffer_per_series: args.precompute_max_buffer_per_series,
            flush_interval_ms: args.precompute_flush_interval_ms,
            channel_buffer_size: args.precompute_channel_buffer_size,
            pass_raw_samples: false,
            raw_mode_aggregation_id: 0,
            late_data_policy: LateDataPolicy::Drop,
        };
        let output_sink = Arc::new(StoreOutputSink::new(store.clone()));
        let engine =
            PrecomputeEngine::new(precompute_config, streaming_config.clone(), output_sink);
        let worker_diagnostics = engine.diagnostics();
        let ingest_state = engine.ingest_state();
        info!(
            "Starting precompute engine on port {}",
            args.prometheus_remote_write_port
        );

        // Spawn periodic memory diagnostics logger
        let diag_store = store.clone();
        tokio::spawn(async move {
            spawn_memory_diagnostics(diag_store, Some(worker_diagnostics)).await;
        });

        let handle = tokio::spawn(async move {
            if let Err(e) = engine.run().await {
                error!("Precompute engine error: {}", e);
            }
        });
        (Some(handle), Some(ingest_state))
    } else {
        // Even without precompute, log store diagnostics
        let diag_store = store.clone();
        tokio::spawn(async move {
            spawn_memory_diagnostics(diag_store, None).await;
        });
        (None, None)
    };

    // Setup OTLP receiver (after precompute engine so it can share the ingest state)
    let otel_handle = if args.enable_otel_ingest {
        let otel_config = OtlpReceiverConfig {
            grpc_port: args.otel_grpc_port,
            http_port: args.otel_http_port,
        };
        let receiver = match precompute_ingest_state.clone() {
            Some(ingest_state) => {
                info!(
                    "Starting OTLP receiver wired to precompute engine \
                     (gRPC port {}, HTTP port {})",
                    args.otel_grpc_port, args.otel_http_port
                );
                OtlpReceiver::with_ingest_state(otel_config, ingest_state)
            }
            None => {
                info!(
                    "Starting OTLP receiver in log-only mode \
                     (precompute engine not enabled; gRPC port {}, HTTP port {})",
                    args.otel_grpc_port, args.otel_http_port
                );
                OtlpReceiver::new(otel_config)
            }
        };
        Some(tokio::spawn(async move {
            if let Err(e) = receiver.run().await {
                error!("OTLP receiver error: {}", e);
            }
        }))
    } else {
        None
    };

    //info!("=== TEMPORARY: Using ClickHouse HTTP adapter ===");
    //info!("ClickHouse endpoint will be available at: /clickhouse/query");
    //info!("ClickHouse fallback URL: http://localhost:8123/?database=default");

    //let adapter_config = AdapterConfig::clickhouse_sql(
    //    "http://localhost:8123".to_string(), // ClickHouse server URL
    //    "default".to_string(),               // Database name
    //    true,                                // Always forward (fallback for every query)
    //);

    // Original Prometheus config (commented out temporarily):
    let adapter_config = AdapterConfig::prometheus_promql(
        args.prometheus_server.clone(),
        args.forward_unsupported_queries,
    );

    let http_config = HttpServerConfig {
        port: args.http_port,
        handle_http_requests: true,
        adapter_config,
    };

    let query_tracker = if args.enable_query_tracker {
        use query_engine_rust::planner_client::LocalPlannerClient;
        use query_engine_rust::QueryTrackerConfig;

        let tracker_config = QueryTrackerConfig {
            observation_window_secs: args.tracker_observation_window_secs,
            prometheus_scrape_interval: args.prometheus_scrape_interval,
        };
        let runtime_options = asap_planner::RuntimeOptions {
            prometheus_scrape_interval: args.prometheus_scrape_interval,
            streaming_engine: asap_planner::StreamingEngine::Precompute,
            enable_punting: false,
            range_duration: 300,
            step: args.prometheus_scrape_interval,
        };
        let planner_client = Arc::new(LocalPlannerClient::new(
            runtime_options,
            args.query_language,
            args.prometheus_server.clone(),
        ));
        let tracker = Arc::new(query_engine_rust::QueryTracker::new(tracker_config));
        let _tracker_handle = tracker.start_background_loop(planner_client);
        info!(
            "Query tracker enabled (observation window: {}s)",
            args.tracker_observation_window_secs
        );
        Some(tracker)
    } else {
        None
    };

    let server = HttpServer::new(http_config, engine, store, query_tracker)
        .with_hot_reload_config(hot_reload_config);
    info!("Starting HTTP server on port {}", args.http_port);

    // Wait for shutdown signal
    tokio::select! {
        result = server.run() => {
            if let Err(e) = result {
                error!("HTTP server error: {}", e);
            }
        }
        _ = signal::ctrl_c() => {
            info!("Shutdown signal received");
        }
    }

    // Cleanup - gracefully shutdown background tasks
    if let Some(handle) = kafka_handle {
        info!("Shutting down Kafka consumer...");
        handle.abort();
        let _ = handle.await;
    }

    if let Some(handle) = otel_handle {
        info!("Shutting down OTLP receiver...");
        handle.abort();
        let _ = handle.await;
    }

    if let Some(handle) = precompute_handle {
        info!("Shutting down precompute engine...");
        handle.abort();
        let _ = handle.await;
    }

    info!("Shutdown complete");
    Ok(())
}

/// Periodic memory diagnostics logger — runs every 30 seconds.
async fn spawn_memory_diagnostics(
    store: Arc<SimpleMapStore>,
    worker_diagnostics: Option<Arc<PrecomputeWorkerDiagnostics>>,
) {
    use std::sync::atomic::Ordering;

    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
    loop {
        interval.tick().await;

        // 1. Store diagnostics
        let store_diag = store.diagnostic_info();
        info!(
            "[MEMORY_DIAG] Store: {} aggregation(s), {} total time_map entries, {:.2} KB total sketch bytes",
            store_diag.num_aggregations,
            store_diag.total_time_map_entries,
            store_diag.total_sketch_bytes as f64 / 1024.0,
        );
        for agg in &store_diag.per_aggregation {
            info!(
                "[MEMORY_DIAG]   agg_id={}: time_map_len={}, read_counts_len={}, aggregate_objects={}, sketch_bytes={:.2} KB",
                agg.aggregation_id,
                agg.time_map_len,
                agg.read_counts_len,
                agg.num_aggregate_objects,
                agg.sketch_bytes as f64 / 1024.0,
            );
        }

        // 2. Worker diagnostics (precompute engine only)
        if let Some(ref diag) = worker_diagnostics {
            let total_groups: usize = diag
                .worker_group_counts
                .iter()
                .map(|c| c.load(Ordering::Relaxed))
                .sum();
            info!(
                "[MEMORY_DIAG] PrecomputeEngine: {} total groups across {} workers",
                total_groups,
                diag.worker_group_counts.len(),
            );
            for (i, counter) in diag.worker_group_counts.iter().enumerate() {
                info!(
                    "[MEMORY_DIAG]   worker_{}: group_states_len={}",
                    i,
                    counter.load(Ordering::Relaxed),
                );
            }
        }
    }
}

fn setup_logging(
    output_dir: &str,
    log_level: &str,
) -> Result<tracing_appender::non_blocking::WorkerGuard> {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

    // Create env filter that respects RUST_LOG, with fallback to command line arg
    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(log_level))
        .unwrap_or_else(|_| EnvFilter::new("info"));

    // Create file appender for logging to file
    let file_appender = tracing_appender::rolling::never(output_dir, "query_engine.log");
    let (non_blocking_file, guard) = tracing_appender::non_blocking(file_appender);

    // Create console layer for stdout
    let console_layer = tracing_subscriber::fmt::layer()
        .with_file(true)
        .with_line_number(true)
        .with_target(true)
        .with_writer(std::io::stdout);

    // Create file layer for file output
    let file_layer = tracing_subscriber::fmt::layer()
        .with_file(true)
        .with_line_number(true)
        .with_target(true)
        .with_ansi(false) // Disable ANSI color codes in log file
        .with_writer(non_blocking_file);

    tracing_subscriber::registry()
        .with(env_filter)
        .with(console_layer)
        .with(file_layer)
        .init();

    info!("Logging initialized (respects RUST_LOG environment variable)");
    info!("Logs will be written to: {}/query_engine.log", output_dir);
    Ok(guard)
}
