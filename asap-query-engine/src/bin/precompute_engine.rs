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
use query_engine_rust::{HttpServer, HttpServerConfig, OtlpReceiver, OtlpReceiverConfig};
use std::sync::Arc;
use tracing::{info, warn};
use tracing_subscriber::fmt::format::FmtSpan;

#[derive(Parser, Debug)]
#[command(name = "precompute_engine")]
#[command(about = "Standalone precompute engine for SketchDB")]
struct Args {
    /// Path to streaming config YAML file
    #[arg(long)]
    streaming_config: String,

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

    // Step-1 of the JSONL deprecation refactor removed the
    // `--cold-store-root` / `ASAP_COLD_STORE_ROOT` flag. The §5.2
    // local-FS JSONL fallback was deleted at the same commit; the
    // surviving fallback chain is just Prometheus (gated by
    // `--forward-unsupported-queries`).

    /// Upstream Prometheus URL for the tail of the fallback chain.
    /// Only consulted when `--forward-unsupported-queries` is set.
    #[arg(long, default_value = "http://localhost:9090")]
    prometheus_server: String,

    /// Forward unsupported PromQL shapes to Prometheus rather than
    /// returning empty.
    #[arg(long, default_value_t = false)]
    forward_unsupported_queries: bool,

    /// Path to the inference config YAML — maps query patterns to
    /// aggregation IDs so the query engine can pick the right
    /// stored sketch for an incoming PromQL. Without it the query
    /// engine starts with an empty pattern table and every query
    /// "no matches" → falls through to cold/Prom fallback. Same
    /// schema as `query_engine_rust --config`.
    #[arg(long)]
    inference_config: Option<String>,

    /// Prometheus-equivalent scrape interval (seconds). Used by
    /// SimpleEngine when computing the instant-query lookback
    /// window: each `query` resolves the metric over the last
    /// `scrape_interval` seconds. For tumbling-window
    /// aggregations this MUST be ≥ the window size in
    /// `streaming-config`, otherwise a window's bucket end
    /// timestamp falls outside the lookback and the query
    /// returns empty even though data is in the store. Default 30
    /// matches the e2e harness's 30s window. Old default was 15
    /// (kept as a deprecated alias).
    #[arg(long, default_value_t = 30)]
    prometheus_scrape_interval: u64,

    /// Enable OTLP metrics ingest (gRPC + HTTP). Required for the
    /// e2e harness's warm-tier sketch path: `query_engine_rust`'s
    /// patched proto deserialiser handles `DDSketch` / `KLLSketch` /
    /// `CountSketch` / `CountMinSketch` / `HLLSketch` types that
    /// the stock OTel `prometheusremotewrite` exporter would
    /// otherwise drop.
    #[arg(long, default_value_t = false)]
    enable_otel_ingest: bool,

    /// OTLP gRPC listen port (only consulted when
    /// `--enable-otel-ingest` is set).
    #[arg(long, default_value_t = 4317)]
    otel_grpc_port: u16,

    /// OTLP HTTP listen port (only consulted when
    /// `--enable-otel-ingest` is set).
    #[arg(long, default_value_t = 4318)]
    otel_http_port: u16,

    /// Path to the per-metric backend storage routing YAML
    /// (`{metric_name: storage_backend}` map). Loaded once at
    /// startup; the HTTP query handler consults it on every PromQL
    /// query to decide which engine should answer (warm-tier
    /// SimpleEngine vs cold-archive GorillaQueryEngine vs JSONL
    /// fallback). Without this flag the handler falls back to the
    /// streaming-config single axis (which always defaults to
    /// `SketchWarmTier`), so cold-archive metrics never reach the
    /// `EngineRouter` — the bug issue #46 v2's `MVP_REPORT.md` flagged.
    /// Reads from `ASAP_BACKEND_STORAGE_ROUTING` so containerised
    /// deploys can wire it via env (matches the backend Docker
    /// image's pattern in `deploy/docker-compose/base.yml`).
    #[arg(long, env = "ASAP_BACKEND_STORAGE_ROUTING")]
    backend_storage_routing: Option<std::path::PathBuf>,
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

    // Phase ε.3 (Option-B validation): build ONE
    // `HotReloadStreamingConfig` handle and share it between the HTTP
    // server (so `POST /api/v1/streaming-config` can swap the active
    // config at runtime) and the precompute engine (so the swap
    // actually takes effect on subsequent ingest batches). Without
    // the shared handle, an HTTP swap would leave the engine reading
    // the original Arc and the controller's typed-stage-split push
    // would silently no-op.
    let hot_reload_streaming_config =
        query_engine_rust::data_model::HotReloadStreamingConfig::from_arc(
            streaming_config.clone(),
        );

    // Create the store
    let store: Arc<dyn query_engine_rust::stores::Store> = if args.persistence_enabled {
        use query_engine_rust::stores::sketch_db::simple_map_store::persistence::SimpleMapStorePersistenceConfig;
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
        let inference_config = match args.inference_config.as_deref() {
            Some(path) => query_engine_rust::utils::file_io::read_inference_config(
                path,
                QueryLanguage::promql,
            )?,
            None => InferenceConfig::new(QueryLanguage::promql, CleanupPolicy::CircularBuffer),
        };
        info!(
            "Loaded inference config with {} query configs",
            inference_config.query_configs.len()
        );
        let query_engine = Arc::new(SimpleEngine::new(
            store.clone(),
            inference_config,
            streaming_config.clone(),
            args.prometheus_scrape_interval, // default 30s (matches e2e window size)
            QueryLanguage::promql,
        ));
        // Step-1 of the JSONL deprecation: the only surviving
        // fallback path is Prometheus (gated by
        // `--forward-unsupported-queries`).
        let adapter_config = AdapterConfig::prometheus_promql(
            args.prometheus_server.clone(),
            args.forward_unsupported_queries,
        );
        let http_config = HttpServerConfig {
            port: args.query_port,
            handle_http_requests: true,
            adapter_config,
        };
        let mut http_server = HttpServer::new(http_config, query_engine, store.clone());

        // Phase ε.3 (Option-B validation): wire the hot-reload
        // streaming-config handle so `POST /api/v1/streaming-config`
        // can swap in the controller's emitted JSON at runtime. Without
        // this call the handler returns 503 ("hot-reload handle not
        // attached; backend was built without
        // HttpServer::with_hot_reload_config") and the controller's
        // typed-stage-split push from `handle_bootstrap_agent_config`
        // / `handle_plan` fails. Mirrors the wiring `src/main.rs`
        // already had — this is the deployed binary
        // (Dockerfile.backend builds `precompute_engine`), so this is
        // where it must live. The handle is shared with the engine
        // (built below) so an HTTP-driven swap reaches both surfaces.
        http_server =
            http_server.with_hot_reload_config(hot_reload_streaming_config.clone());

        // Per-metric storage-backend routing table (issue #46
        // criterion ⑤). When provided, the HTTP handler consults this
        // table on every PromQL query — extracting the metric name
        // from the AST and looking up its `StorageBackend`. Without
        // it the handler falls back to the streaming-config single
        // axis (which always defaults to `SketchWarmTier`) and the
        // `EngineRouter` is effectively bypassed.
        // Phase α (MVP): always install a hot-reload routing handle —
        // bootstrap from YAML when available, an empty table otherwise.
        // The `POST /api/v1/storage_routing` endpoint can then swap in
        // a controller-emitted table at runtime without restart.
        let bootstrap_routing = if let Some(routing_path) =
            args.backend_storage_routing.as_deref()
        {
            match query_engine_rust::data_model::BackendStorageRouting::from_yaml_file(
                routing_path,
            ) {
                Ok(routing) => {
                    info!(
                        "Loaded backend-storage-routing from {:?}: default={:?}, entries={}",
                        routing_path,
                        routing.default_backend(),
                        routing.len(),
                    );
                    routing
                }
                Err(e) => {
                    warn!(
                        "Failed to load backend-storage-routing from {:?}: {} — installing an empty routing table; the controller's first POST /api/v1/storage_routing push will fill it",
                        routing_path, e,
                    );
                    query_engine_rust::data_model::BackendStorageRouting::empty()
                }
            }
        } else {
            info!(
                "--backend-storage-routing not set — installing an empty routing table; the controller's first POST /api/v1/storage_routing push will fill it",
            );
            query_engine_rust::data_model::BackendStorageRouting::empty()
        };
        http_server = http_server.with_backend_storage_routing(Arc::new(bootstrap_routing));

        // Phase-5/6 + Step-2.3: register an archive-tier engine on
        // the capability router. Mirrors the block in `src/main.rs`
        // so the `precompute_engine` binary (used by the
        // deploy/docker image) matches the full backend's behaviour.
        //
        // * **Path A2 mode** — `ASAP_THANOS_QUERY_URL` is set →
        //   `ThanosForwardEngine` is registered under both
        //   `thanos_archive` and the legacy `gorilla_archive` slot;
        //   the in-process Gorilla path is skipped.
        // * **Legacy mode** — env unset → in-process
        //   `GorillaQueryEngine` is registered under
        //   `gorilla_archive`. Phase δ deletes this leg after
        //   Path A2 is verified end-to-end.
        match query_engine_rust::engines::gorilla::thanos_engine_from_env() {
            Ok(Some(thanos)) => {
                use query_engine_rust::engines::gorilla::DATA_SOURCE_THANOS_ARCHIVE_ID;
                use query_engine_rust::routing::QueryEngine;
                info!(
                    upstream = thanos.base_url(),
                    "Path A2: registering ThanosForwardEngine for the archive tier (data_source_id=thanos_archive, alias=gorilla_archive); legacy in-process GorillaQueryEngine skipped",
                );
                let thanos_arc: Arc<dyn QueryEngine> = Arc::new(thanos);
                http_server = http_server
                    .with_query_engine_aliased(
                        DATA_SOURCE_THANOS_ARCHIVE_ID,
                        thanos_arc.clone(),
                    )
                    .with_query_engine_aliased(
                        asap_types::StorageBackend::GorillaS3Archive.data_source_id(),
                        thanos_arc,
                    );
            }
            Ok(None) => match query_engine_rust::engines::gorilla::GorillaS3Config::from_env() {
                Ok(s3_cfg) => match query_engine_rust::engines::gorilla::GorillaS3Store::with_default_backend(s3_cfg) {
                    Ok(store) => {
                        use query_engine_rust::engines::{GorillaEngineConfig, GorillaQueryEngine};
                        use query_engine_rust::routing::QueryEngine;
                        let gorilla = Arc::new(GorillaQueryEngine::with_gorilla_s3(
                            Arc::new(store),
                            GorillaEngineConfig::default(),
                        ));
                        info!(
                            "Registering legacy in-process GorillaQueryEngine on the capability router (data_source_id=gorilla_archive); set ASAP_THANOS_QUERY_URL to switch to Path A2 thanos forwarding",
                        );
                        http_server = http_server.with_query_engine(gorilla as Arc<dyn QueryEngine>);
                    }
                    Err(e) => {
                        warn!(
                            "ASAP_GORILLA_S3_* env vars present but GorillaS3Store failed to build ({e}); router will not have an archive engine",
                        );
                    }
                },
                Err(_) => {
                    info!(
                        "ASAP_GORILLA_S3_* env vars not configured — router serves warm-tier metrics only (set ASAP_GORILLA_S3_BUCKET + ASAP_GORILLA_S3_REGION to enable archive routing, or set ASAP_THANOS_QUERY_URL to enable Path A2 thanos forwarding)",
                    );
                }
            },
            Err(e) => {
                warn!(
                    "ASAP_THANOS_QUERY_URL set but ThanosForwardEngine failed to build ({e}); router will not have an archive engine",
                );
            }
        }

        // Phase ε.2: register a `PrometheusForwardEngine` under the
        // `prometheus_remote` engine id when
        // `ASAP_PROMETHEUS_QUERY_URL` is set. Mirrors the block in
        // `src/main.rs` so the `precompute_engine` binary (used by
        // the deploy/docker image) matches the full backend's
        // behaviour.
        match query_engine_rust::engines::prometheus::prometheus_engine_from_env() {
            Ok(Some(prom)) => {
                use query_engine_rust::routing::QueryEngine;
                info!(
                    upstream = prom.base_url(),
                    "Phase ε.2: registering PrometheusForwardEngine on the capability router (data_source_id=prometheus_remote)",
                );
                http_server = http_server
                    .with_query_engine(Arc::new(prom) as Arc<dyn QueryEngine>);
            }
            Ok(None) => {
                info!(
                    "ASAP_PROMETHEUS_QUERY_URL not set — PrometheusForwardEngine skipped; routing-table entries referencing `prometheus_remote` will surface NoEngineRegistered",
                );
            }
            Err(e) => {
                warn!(
                    "ASAP_PROMETHEUS_QUERY_URL set but PrometheusForwardEngine failed to build ({e}); router will not have a prometheus_remote engine",
                );
            }
        }

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
        allowed_lateness_ms: args.allowed_lateness_ms,
        max_buffer_per_series: args.max_buffer_per_series,
        flush_interval_ms: args.flush_interval_ms,
        channel_buffer_size: args.channel_buffer_size,
        pass_raw_samples: args.pass_raw_samples,
        raw_mode_aggregation_id: args.raw_mode_aggregation_id,
        late_data_policy: args.late_data_policy,
        wall_clock_grace_period_ms: 5_000,
        schema_persist_path: None,
    };

    // Create the output sink (writes directly to the store)
    let output_sink: Arc<dyn query_engine_rust::precompute_engine::output_sink::OutputSink> =
        if args.pass_raw_samples {
            Arc::new(RawPassthroughSink::new(store))
        } else {
            Arc::new(StoreOutputSink::new(store))
        };

    // Build the engine. Snapshot `ingest_state` BEFORE starting the
    // engine — once `engine.run()` is awaited it owns the engine
    // and we can't pull the handle out for the OTLP receiver.
    //
    // Reuse the shared `hot_reload_streaming_config` so an HTTP
    // `POST /api/v1/streaming-config` swap reaches both the HTTP query
    // surface and the engine's ingest path (Phase ε.3 / Option B).
    let engine = PrecomputeEngine::new(
        engine_config,
        hot_reload_streaming_config,
        output_sink,
    );
    let ingest_state = if args.enable_otel_ingest {
        Some(engine.ingest_state())
    } else {
        None
    };

    // Spawn the OTLP receiver alongside the engine when requested.
    // Without it the warm-tier sketch path doesn't get fed: the
    // gateway's PRW translator drops `DDSketch` / `HLLSketch` types,
    // so the only way to deliver sketches to the backend is OTLP.
    let otel_handle = if let Some(ingest_state) = ingest_state {
        let otel_config = OtlpReceiverConfig {
            grpc_port: args.otel_grpc_port,
            http_port: args.otel_http_port,
        };
        info!(
            grpc_port = args.otel_grpc_port,
            http_port = args.otel_http_port,
            "Starting OTLP receiver wired to precompute engine",
        );
        let receiver = OtlpReceiver::with_ingest_state(otel_config, ingest_state);
        Some(tokio::spawn(async move {
            if let Err(e) = receiver.run().await {
                tracing::error!("OTLP receiver error: {}", e);
            }
        }))
    } else {
        None
    };

    info!("Starting precompute engine...");
    let run_result = engine.run().await;

    if let Some(h) = otel_handle {
        h.abort();
        let _ = h.await;
    }

    run_result?;
    Ok(())
}
