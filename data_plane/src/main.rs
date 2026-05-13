// Phase 9 (controller-into-backend refactor):
// The `controller` crate is now a path-dep of this binary
// (`../controller` in `asap-query-engine/Cargo.toml`). It is NOT yet
// started in-process here; the in-process OpAMP server + capability-map
// exposure are a follow-up that will land after Phase 4 (centralized
// series_id resolver). For now we only verify that the crate compiles
// inside this workspace and is importable from `main.rs`.
//
// When that follow-up lands, the OpAMP WS endpoint (port 4320) and the
// RuntimeSamples gRPC endpoint (port 4321) will be served from inside
// this same backend process — there is no longer a separate
// `asap-controller` container in `mvp-multinode/run_demo.sh`.
use clap::Parser;
use std::fs;
use std::sync::Arc;
use tokio::signal;
use tracing::{error, info, warn};

use data_plane::stores::types::enums::{CleanupPolicy, LockStrategy};
use data_plane::drivers::AdapterConfig;
use data_plane::precompute_engine::config::LateDataPolicy;
use data_plane::precompute_engine::PrecomputeWorkerDiagnostics;
use data_plane::utils::file_io::read_streaming_config;
use data_plane::{
    HttpServer, HttpServerConfig, OtlpReceiver, OtlpReceiverConfig, PrecomputeEngine,
    PrecomputeEngineConfig, Result, ASAPQueryEngine, SketchIndexSink, SketchStore,
};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// File path for streaming_config
    #[arg(long)]
    streaming_config: String,

    /// Cleanup policy for SketchStore retention.
    /// `circular_buffer`: keep the N most recent windows per agg
    /// (N comes from each aggregation's `numAggregatesToRetain`).
    /// `no_cleanup`: never evict.
    #[arg(long, value_enum, default_value = "circular_buffer")]
    cleanup_policy: CleanupPolicy,

    /// Prometheus scrape interval (seconds). Default 30 matches
    /// the e2e harness's 30s window. ASAPQueryEngine uses this as the
    /// instant-query lookback window — for tumbling-window
    /// aggregations it must be ≥ the window size in
    /// `streaming-config`.
    #[arg(long, default_value = "30")]
    prometheus_scrape_interval: u64,

    /// HTTP server port for the PromQL-compatible query surface.
    /// `--query-port` is accepted as an alias for compatibility with
    /// the legacy `precompute_engine` binary's flag (whose default
    /// was 8080). Compose stacks pass `--query-port=9091`.
    #[arg(long, alias = "query-port", default_value = "8088")]
    http_port: u16,

    /// Deprecated/no-op: the backend's only HTTP listener is the
    /// PromQL query surface (`--http-port` / `--query-port`). The
    /// old PRW ingest port was deleted in PR #100; this flag is
    /// accepted for compose backwards compatibility (some overlays
    /// still pass `--ingest-port=9090`) and silently ignored.
    #[arg(long, hide = true)]
    ingest_port: Option<u16>,

    /// Prometheus server URL
    #[arg(long, default_value = "http://localhost:9090")]
    prometheus_server: String,

    /// DataCollector controller endpoint for capability-miss
    /// notifications (PR G). When set, `ASAPQueryEngine` fires a
    /// fire-and-forget POST to this URL every time a query can't
    /// find a compatible stored aggregation, so the controller
    /// can generate a new sketch plan. When unset (default),
    /// capability misses fall through to the §5.2 fallback silently.
    /// Example: `http://controller.svc:8080/api/v1/plan`
    ///
    /// Falls back to the `ASAP_CONTROLLER_URL` env var when the flag
    /// is not passed — `deploy/docker-compose/base.yml` sets the env
    /// var so the MVP demo doesn't need a per-arg overlay.
    #[arg(long, env = "ASAP_CONTROLLER_URL")]
    controller_endpoint: Option<String>,

    /// Forward unsupported queries to Prometheus
    #[arg(long)]
    forward_unsupported_queries: bool,

    /// Database path (currently unused, kept for compatibility)
    #[arg(long, default_value = "sketchdb.db")]
    db_path: String,

    /// Delete existing database (currently unused, kept for compatibility)
    #[arg(long)]
    delete_existing_db: bool,

    /// Output directory for logs
    #[arg(long, default_value = "/var/log/asap")]
    output_dir: String,

    /// Log level
    #[arg(long, default_value = "INFO")]
    log_level: String,

    /// Enable profiling (currently unused, kept for compatibility)
    #[arg(long)]
    do_profiling: bool,

    /// Lock strategy for SketchStore: "global" for single mutex,
    /// "per-key" for fine-grained locking. Default `per-key`.
    #[arg(long, value_enum, default_value = "per-key")]
    lock_strategy: LockStrategy,

    /// Path to promsketch configuration YAML file (optional; uses defaults if omitted)
    #[arg(long)]
    promsketch_config: Option<String>,

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

    /// Maximum allowed lateness for out-of-order samples
    /// (milliseconds). `--allowed-lateness-ms` is accepted as an
    /// alias for compatibility with the legacy `precompute_engine`
    /// binary's flag spelling.
    #[arg(long, alias = "allowed-lateness-ms", default_value = "5000")]
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

    /// Optional path where the schema registry persists per-`agg_id`
    /// lifecycle state (created_at / retired_at / expires_at) across
    /// restarts (sketch DB Phase 2c). When unset, the registry is
    /// memory-only and the §7 schema timeline loses all pre-restart
    /// history.
    #[arg(long)]
    schema_persist_path: Option<std::path::PathBuf>,

    /// Optional path where the backfill job registry persists across
    /// restarts (sketch DB Phase 5g). When set, every job state
    /// transition rewrites this file atomically, so operators see
    /// recent backfill history even after a backend restart.
    /// Memory-only by default.
    #[arg(long)]
    backfill_persist_path: Option<std::path::PathBuf>,

    /// Spawn the Phase 5e backfill drain loop. When off (default),
    /// queued backfill jobs stay `Queued` forever — shadow-mode
    /// for controller REFRESH dispatch validation. When on, a
    /// background task picks up queued jobs and runs them through
    /// `BackfillWindowProcessor` (real sketch rebuild + store
    /// writes). Requires `--streaming-engine=precompute` so the
    /// schema registry is available; otherwise a warning is logged
    /// and the service stays down.
    #[arg(long)]
    enable_backfill_worker: bool,

    /// Spawn the Phase 5 schema eviction service. Periodically
    /// scans the schema registry for `Expired` schemas, cancels
    /// any in-flight backfill jobs targeting them, and drops the
    /// agg_id's data from the store. Requires the schema registry
    /// (same precompute-engine dependency as --enable-backfill-worker).
    #[arg(long)]
    enable_schema_eviction: bool,

    /// Poll interval for the schema eviction service, in seconds.
    /// Default 300s (5 minutes) — eviction is not latency-sensitive.
    #[arg(long, default_value = "300")]
    schema_eviction_poll_secs: u64,

    /// When true, the eviction service logs what it would drop but
    /// doesn't actually call `drop_agg_id` / `remove_schema`. Use
    /// to validate a new retention value before letting it delete
    /// anything.
    #[arg(long)]
    schema_eviction_dry_run: bool,

    // ---- SketchStore persistence ----
    //
    // When --persistence-enabled is set, the store is constructed via
    // SketchStore::with_persistence_per_key with the other
    // --persistence-* flags as the config. Forces LockStrategy::PerKey
    // regardless of --lock-strategy; the Global variant is
    // intentionally left in-memory-only.
    /// Enable the disk-backed persistence layer for SketchStore
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

    /// Path to the per-metric backend storage routing YAML
    /// (`{metric_name: storage_backend}` map). Loaded at startup and
    /// consulted by the HTTP query handler on every PromQL request to
    /// pick the right engine (`ASAPQueryEngine` for warm-tier sketches,
    /// `GorillaQueryEngine` for the cold archive, etc.). Without
    /// this flag the handler falls back to the streaming-config
    /// single axis (always `SketchStore`) and the EngineRouter is
    /// effectively bypassed — the issue-46 v2 demo's criterion ⑤
    /// failure mode. Mirrors the `precompute_engine` binary's flag
    /// of the same name.
    #[arg(long, env = "ASAP_BACKEND_STORAGE_ROUTING")]
    backend_storage_routing: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Create output directory
    fs::create_dir_all(&args.output_dir)?;

    // Initialize logging similar to Python's create_loggers function
    // Keep the guard alive for the entire lifetime of the application
    let _log_guard = setup_logging(&args.output_dir, &args.log_level)?;

    info!("Starting Query Engine Rust");
    info!("Output directory: {}", args.output_dir);

    if let Some(ingest_port) = args.ingest_port {
        warn!(
            "--ingest-port={ingest_port} is deprecated and ignored: the backend's only HTTP \
             listener is the PromQL query surface (PRW ingest was removed in PR #100). \
             Drop the flag from your compose `command:` block."
        );
    }

    let streaming_config = Arc::new(read_streaming_config(&args.streaming_config)?);
    info!(
        "Loaded streaming config with {} entries",
        streaming_config.get_all_aggregation_configs().len()
    );
    info!("Streaming config: {:?}", streaming_config);

    // Wrap the streaming config in a hot-reload handle so the HTTP
    // server's `/api/v1/streaming-config` endpoints can swap it at
    // runtime (PR E phase 1). Existing consumers downstream
    // (ASAPQueryEngine, PrecomputeEngine, Store) still take their
    // startup snapshot; hot-reload currently only affects the
    // control-plane GET/POST endpoint. Phase 2 will extend the swap
    // to query execution and ingest routing.
    let hot_reload_config =
        data_plane::stores::types::HotReloadStreamingConfig::from_arc(streaming_config.clone());

    // Setup store
    let cleanup_policy = args.cleanup_policy;
    info!("Using cleanup policy: {:?}", cleanup_policy);
    let store = if args.persistence_enabled {
        use data_plane::stores::sketch_db::store::persistence::SketchStorePersistenceConfig;
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
        let persistence_cfg = SketchStorePersistenceConfig {
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
            SketchStore::with_persistence_per_key(
                streaming_config.clone(),
                cleanup_policy,
                persistence_cfg,
            )
            .expect("SketchStore::with_persistence_per_key failed"),
        )
    } else {
        Arc::new(SketchStore::new_with_strategy(
            streaming_config.clone(),
            cleanup_policy,
            args.lock_strategy,
        ))
    };

    // Phase 4 + 5 wire-in (refactor 2026-05): allocate the shared
    // SeriesIdResolver + SketchIndex once. The OTLP receive path
    // (sid resolution + unknown_series_ids stamping; SketchIndex
    // .append_sample on every modified-OTLP sketch DP) AND the
    // ASAPQueryEngine query path (SketchIndex.classify / query_range
    // for warm-tier reads) hold clones of these Arcs. Allocated
    // here before BOTH the ASAPQueryEngine and the precompute engine
    // are constructed so both can be wired with a single canonical
    // instance — even when precompute is disabled, the engine still
    // needs the index for the Phase 6 archive failover trigger.
    let series_resolver =
        Arc::new(data_plane::drivers::ingest::series_resolver::SeriesIdResolver::new());
    let sketch_index =
        Arc::new(data_plane::stores::sketch_db::index::SketchIndex::new());

    // Setup query engine. ASAPQueryEngine shares the same
    // HotReloadStreamingConfig handle as the HTTP server, so a POST
    // to /api/v1/streaming-config is observable by the next query
    // (PR E phase 2). Without sharing the handle, ASAPQueryEngine
    // would take a one-time snapshot at construction and ignore
    // subsequent swaps.
    let mut engine = {
        let mut engine = ASAPQueryEngine::new_with_hot_reload(
            store.clone(),
            hot_reload_config.clone(),
            args.prometheus_scrape_interval,
        )
        // Phase 5 wire-in (refactor 2026-05): hand the warm-tier
        // SketchIndex to the query engine so SidLookup classification
        // drives the Phase 6 archive failover via
        // EngineError::CapabilityMiss when the warm tier is empty
        // / ghost / unknown.
        .with_sketch_index(sketch_index.clone());
        if let Some(controller_endpoint) = args.controller_endpoint.as_ref() {
            info!(
                "Capability-miss notifications enabled → {}",
                controller_endpoint
            );
            let client: Arc<
                dyn data_plane::drivers::query::controller_client::ControllerClient,
            > = Arc::new(
                data_plane::drivers::query::controller_client::HttpControllerClient::new(
                    controller_endpoint.clone(),
                ),
            );
            engine = engine.with_controller_client(client);
        } else {
            info!(
                "Capability-miss notifications disabled \
                 (pass --controller-endpoint=<url> to enable)"
            );
        }
        // `Arc::new(engine)` is deferred until after the precompute
        // engine is constructed so we can hand the same `SchemaRegistry`
        // (§7 timeline source) to both via `with_schema_registry`.
        engine
    };

    // Setup precompute engine. Backend ingest is OTLP-only — the
    // precompute engine no longer hosts an HTTP listener of its own; the
    // OTLP receiver below pushes envelopes / raw points into the worker
    // pool via the `IngestState` handle returned by `engine.ingest_state()`.
    //
    // NOTE: precompute is constructed BEFORE the OTLP receiver so the receiver
    // can obtain an `Arc<IngestState>` handle and push OTLP metrics / sketches
    // into the same worker pool (and not just write directly to the store).
    let (precompute_handle, precompute_ingest_state) = {
        let precompute_config = PrecomputeEngineConfig {
            num_workers: args.precompute_num_workers,
            allowed_lateness_ms: args.precompute_allowed_lateness_ms,
            max_buffer_per_series: args.precompute_max_buffer_per_series,
            flush_interval_ms: args.precompute_flush_interval_ms,
            channel_buffer_size: args.precompute_channel_buffer_size,
            pass_raw_samples: false,
            raw_mode_aggregation_id: 0,
            late_data_policy: LateDataPolicy::Drop,
            wall_clock_grace_period_ms: 5_000,
            schema_persist_path: args.schema_persist_path.clone(),
        };
        // M2.3.6 — sketch-only sink. Precompute writes now go to
        // `SketchIndex` exclusively; the legacy `SketchStore` no
        // longer receives traffic from either ingest (this sink) or
        // queries (engine M2.3.5b cut-over). The `store` Arc kept
        // below is for the eviction service + diagnostic plumbing
        // until subsequent M2.3.6 sub-PRs delete those too.
        let output_sink = Arc::new(SketchIndexSink::new(
            sketch_index.clone(),
            hot_reload_config.clone(),
        ));
        let engine = PrecomputeEngine::new(
            precompute_config,
            hot_reload_config.clone(),
            output_sink,
            series_resolver.clone(),
            sketch_index.clone(),
        );
        let worker_diagnostics = engine.diagnostics();
        let ingest_state = engine.ingest_state();
        info!("Starting precompute engine (OTLP-fed; no HTTP ingest port)");

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
    };

    // Hand the precompute engine's `SchemaRegistry` to the query
    // engine so both observe the same §7 timeline (design-sketch-db.md
    // §6 / §7). When precompute isn't enabled the engine keeps its
    // default empty registry — queries that need the timeline will
    // simply see no segments and fall through to the legacy path.
    if let Some(ingest_state) = precompute_ingest_state.as_ref() {
        engine = engine.with_schema_registry(ingest_state.schemas.clone());
    }
    let engine = Arc::new(engine);

    // Setup OTLP receiver (after precompute engine so it can share the ingest state)
    // Issue #46 ⑥ — freshness-probe last-value cache. Shared between
    // the OTLP receiver (write path) and the HTTP query handler (read
    // path) so `last_over_time(http_freshness_probe_*[<range>])` can
    // be answered from RAM instead of falling through to the cold
    // archive (which has a 60–90 s flush gap that would leave the
    // 10 s lookback window empty). Allocated unconditionally — non-
    // probe traffic doesn't touch the cache, so the cost is one
    // `RwLock<HashMap>` of three entries for the whole demo run.
    let probe_cache = Arc::new(data_plane::query_engines::routing::FreshnessProbeCache::new());

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
                    .with_probe_cache(probe_cache.clone())
            }
            None => {
                info!(
                    "Starting OTLP receiver in log-only mode \
                     (precompute engine not enabled; gRPC port {}, HTTP port {})",
                    args.otel_grpc_port, args.otel_http_port
                );
                OtlpReceiver::new(otel_config).with_probe_cache(probe_cache.clone())
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

    // Step-1 of the JSONL deprecation deleted the local-FS cold
    // store + the §5.2 `ColdFallback` adapter; the surviving
    // fallback chain is just Prometheus (when
    // `--forward-unsupported-queries` is set).
    let adapter_config = AdapterConfig::prometheus_promql(
        args.prometheus_server.clone(),
        args.forward_unsupported_queries,
    );

    let http_config = HttpServerConfig {
        port: args.http_port,
        handle_http_requests: true,
        adapter_config,
    };

    // The legacy in-backend query tracker / LocalPlannerClient was
    // removed in Phase γ (deletion of `asap-planner-rs`). The
    // ASAPCollector controller is now the sole emitter of streaming
    // configs / `BackendStorageRouting`; the backend is a pure
    // executor that consumes plans pushed via
    // `POST /api/v1/streaming-config` and `POST /api/v1/storage_routing`.

    // Forward the precompute engine's schema registry to the HTTP
    // server so `POST /api/v1/streaming-config` can drive schema
    // lifecycle transitions event-driven (Phase 2b of the sketch DB
    // design, §6). When precompute isn't enabled, the registry is
    // absent and the swap handler no-ops on schema reconciliation
    // (legacy per-batch reconcile in ingest still works).
    let mut server = HttpServer::new(http_config, engine, store.clone())
        .with_hot_reload_config(hot_reload_config.clone())
        .with_probe_cache(probe_cache.clone());

    // Per-metric storage-backend routing table (issue #46
    // criterion ⑤). Mirror the `precompute_engine` binary: load it
    // from `--backend-storage-routing` (or its env-var alias) so the
    // HTTP handler consults a per-metric `StorageBackend` map on
    // every PromQL query instead of bypassing the EngineRouter when
    // the streaming-config single axis defaults to `SketchStore`.
    //
    // Phase α (MVP): even when no static YAML is loaded, install an
    // empty hot-reload handle so the controller's first
    // `POST /api/v1/storage_routing` push lands without first-call 503
    // lossage. Operators can still hand-author the YAML for
    // dev / standalone — the YAML supplies the bootstrap, controller
    // pushes overwrite it.
    let bootstrap_routing = if let Some(routing_path) = args.backend_storage_routing.as_deref() {
        match data_plane::stores::types::BackendStorageRouting::from_yaml_file(routing_path) {
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
                data_plane::stores::types::BackendStorageRouting::empty()
            }
        }
    } else {
        info!(
            "--backend-storage-routing not set — installing an empty routing table; the controller's first POST /api/v1/storage_routing push will fill it",
        );
        data_plane::stores::types::BackendStorageRouting::empty()
    };
    server = server.with_backend_storage_routing(Arc::new(bootstrap_routing));

    // Phase-5/6 + Step-2.3: register the Thanos query engine on the
    // capability router. Two operating modes, selected at startup:
    //
    // * **Path A2 mode** — when `ASAP_THANOS_QUERY_URL` is set, the
    //   backend forwards archive-tier PromQL queries to a
    //   `thanos-query` sidecar via the
    //   [`ThanosQueryEngine`], registered under the single public id
    //   `thanos_query`. The legacy in-process `GorillaQueryEngine`
    //   is skipped in this mode.
    // * **Legacy mode** — when `ASAP_THANOS_QUERY_URL` is unset, the
    //   in-process `GorillaQueryEngine` answers archive queries
    //   from per-hour Gorilla chunks landed on S3 / MinIO via the
    //   `GorillaS3Store`. This is the dev path and is preserved
    //   verbatim until Phase δ deletes it after Path A2 is verified
    //   end-to-end.
    //
    // When neither env-var family is configured the binary registers
    // a `NoDataArchiveEngine` stub under `thanos_query`
    // so cold queries succeed with an empty result instead of
    // surfacing as `503 NoEngineRegistered`. Operators that want the
    // original fail-loud behaviour can opt back in by setting
    // `ASAP_REQUIRE_ARCHIVE_ENGINE=1`.
    let mut archive_registered = false;
    match data_plane::query_engines::thanos_query_engine::thanos_engine_from_env() {
        Ok(Some(thanos)) => {
            use data_plane::query_engines::routing::QueryEngine;
            info!(
                upstream = thanos.base_url(),
                "Path A2: registering ThanosQueryEngine for the archive tier (data_source_id=thanos_query); legacy in-process GorillaQueryEngine skipped",
            );
            let thanos_arc: Arc<dyn QueryEngine> = Arc::new(thanos);
            server = server.with_archive_query_engine(thanos_arc);
            archive_registered = true;
        }
        Ok(None) => match data_plane::stores::gorilla_object_store::GorillaS3Config::from_env() {
            Ok(s3_cfg) => {
                match data_plane::stores::gorilla_object_store::GorillaS3Store::with_default_backend(
                    s3_cfg,
                ) {
                    Ok(store) => {
                        use data_plane::stores::{GorillaEngineConfig, GorillaQueryEngine};
                        use data_plane::query_engines::routing::QueryEngine;
                        let gorilla = Arc::new(GorillaQueryEngine::with_gorilla_s3(
                            Arc::new(store),
                            GorillaEngineConfig::default(),
                        ));
                        info!(
                                "Registering legacy in-process GorillaQueryEngine on the archive slot (canonical data_source_id=thanos_query); set ASAP_THANOS_QUERY_URL to use the intended Thanos archive path",
                            );
                        server = server.with_archive_query_engine(gorilla as Arc<dyn QueryEngine>);
                        archive_registered = true;
                    }
                    Err(e) => {
                        warn!(
                                "ASAP_GORILLA_S3_* env vars present but GorillaS3Store failed to build ({e}); router will not have an archive engine",
                            );
                    }
                }
            }
            Err(_) => {
                info!(
                        "ASAP_GORILLA_S3_* env vars not configured — router serves warm-tier metrics only (set ASAP_GORILLA_S3_BUCKET + ASAP_GORILLA_S3_REGION to enable archive routing, or set ASAP_THANOS_QUERY_URL to enable Path A2 thanos forwarding)",
                    );
            }
        },
        Err(e) => {
            warn!(
                "ASAP_THANOS_QUERY_URL set but ThanosQueryEngine failed to build ({e}); router will not have an archive engine",
            );
        }
    }

    // No archive engine configured — register a `NoDataArchiveEngine`
    // stub under `thanos_query` so cold queries succeed
    // with an empty result. `ASAP_REQUIRE_ARCHIVE_ENGINE=1` opts back
    // into the original fail-loud (`503 NoEngineRegistered`) behaviour.
    if !archive_registered {
        let require_archive = std::env::var("ASAP_REQUIRE_ARCHIVE_ENGINE")
            .map(|v| matches!(v.as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false);
        if require_archive {
            warn!(
                "ASAP_REQUIRE_ARCHIVE_ENGINE=1 set and no archive engine configured — cold queries will return 503 NoEngineRegistered",
            );
        } else {
            use data_plane::query_engines::NoDataArchiveEngine;
            use data_plane::query_engines::routing::QueryEngine;
            info!(
                "Registering NoDataArchiveEngine stub on the archive slot (canonical data_source_id=thanos_query); set ASAP_REQUIRE_ARCHIVE_ENGINE=1 to disable",
            );
            let stub: Arc<dyn QueryEngine> = Arc::new(NoDataArchiveEngine::new());
            server = server.with_archive_query_engine(stub);
        }
    }

    if let Some(ingest_state) = precompute_ingest_state.as_ref() {
        server = server.with_schemas(ingest_state.schemas.clone());
    }
    if args.persistence_delete_older_than_secs > 0 {
        server = server.with_data_retention_ms(args.persistence_delete_older_than_secs * 1000);
    }
    // Backfill registry (sketch DB §10). A single `Arc` lives in
    // `main` so the HTTP endpoints (Phase 5d) can inspect / cancel
    // jobs and the upcoming worker pool (Phase 5e) can drain them.
    // Jobs stay `Queued` until 5e wires the worker — intentional
    // shadow-mode behaviour that lets operators validate the
    // controller's REFRESH dispatch logic before workers exist.
    // Phase 5g: when `--backfill-persist-path` is set, the registry
    // loads prior job records from disk and rewrites the file on
    // every state transition. When unset, the registry is
    // memory-only and restart wipes job history.
    let backfill_registry = Arc::new(match args.backfill_persist_path.as_ref() {
        Some(path) => {
            data_plane::stores::sketch_db::BackfillRegistry::load_or_new(path.clone())
        }
        None => data_plane::stores::sketch_db::BackfillRegistry::new(),
    });
    server = server.with_backfill_registry(backfill_registry.clone());

    // Phase 5e: spawn the backfill drain service if requested. When
    // enabled with `--enable-backfill-worker`, the service picks
    // up queued jobs and runs them through a
    // `BackfillWindowProcessor` (real sketch rebuild + store
    // writes). Without a reader factory configured (Phase 5h), all
    // production `BackfillSource` variants fail fast with a clear
    // "no reader" error — still a step up from the old shadow
    // mode, since the controller now gets signal that its REFRESH
    // dispatch was received but not executable.
    let backfill_service_handle = if let (true, Some(ingest_state)) = (
        args.enable_backfill_worker,
        precompute_ingest_state.as_ref(),
    ) {
        let schemas = ingest_state.schemas.clone();
        let service = data_plane::stores::sketch_db::BackfillService::new(
            backfill_registry.clone(),
            schemas,
            store.clone(),
            hot_reload_config.clone(),
            data_plane::stores::sketch_db::default_reader_factory(),
            data_plane::stores::sketch_db::BackfillServiceConfig::default(),
        );
        info!(
            "Spawning BackfillService drain loop (reader factory: default — Prometheus sources wired, S3/OtherSketch fail fast)"
        );
        Some(service.spawn())
    } else {
        if args.enable_backfill_worker {
            warn!(
                "--enable-backfill-worker was set but precompute engine isn't enabled; backfill service NOT spawned (it needs the schema registry)"
            );
        }
        None
    };

    // Phase 5: schema eviction service. On every poll interval,
    // scans the schema registry for `Expired` schemas, cancels any
    // in-flight backfills targeting them, and drops the agg_id's
    // data from the store. Complements the age-based data retention
    // in SketchStore — see `SchemaEvictionService` module doc for
    // the ordering rationale.
    let schema_eviction_handle = if let (true, Some(ingest_state)) = (
        args.enable_schema_eviction,
        precompute_ingest_state.as_ref(),
    ) {
        let data_retention_opt = if args.persistence_delete_older_than_secs == 0 {
            None
        } else {
            Some(std::time::Duration::from_secs(
                args.persistence_delete_older_than_secs,
            ))
        };
        data_plane::stores::sketch_db::warn_if_retention_inverted(
            data_retention_opt,
            ingest_state.schemas.retirement_retention(),
        );
        let svc = data_plane::stores::sketch_db::SchemaEvictionService::new(
            ingest_state.schemas.clone(),
            backfill_registry.clone(),
            store.clone(),
            data_plane::stores::sketch_db::SchemaEvictionConfig {
                poll_interval: std::time::Duration::from_secs(args.schema_eviction_poll_secs),
                dry_run: args.schema_eviction_dry_run,
            },
        );
        info!(
            poll_secs = args.schema_eviction_poll_secs,
            dry_run = args.schema_eviction_dry_run,
            "Spawning SchemaEvictionService"
        );
        Some(svc.spawn())
    } else {
        if args.enable_schema_eviction {
            warn!(
                "--enable-schema-eviction set but precompute engine isn't enabled; eviction service NOT spawned"
            );
        }
        None
    };

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
    if let Some(handle) = backfill_service_handle {
        info!("Shutting down backfill service...");
        handle.shutdown().await;
    }

    if let Some(handle) = schema_eviction_handle {
        info!("Shutting down schema eviction service...");
        handle.shutdown().await;
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
    store: Arc<SketchStore>,
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
                "[MEMORY_DIAG]   agg_id={}: time_map_len={}, aggregate_objects={}, sketch_bytes={:.2} KB",
                agg.aggregation_id,
                agg.time_map_len,
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

#[cfg(test)]
mod tests {
    use data_plane::drivers::AdapterConfig;

    // Step-1 of the JSONL deprecation refactor deleted the
    // §5.2 `ColdFallback` adapter and the
    // `from_prom_with_optional_cold` constructor. The surviving
    // fallback chain is just Prometheus (gated by
    // `forward_unsupported_queries`).

    #[test]
    fn no_forward_yields_no_fallback() {
        let cfg = AdapterConfig::prometheus_promql("http://prom:9090".into(), false);
        assert!(
            cfg.fallback.is_none(),
            "forward_unsupported=false must leave the fallback slot empty",
        );
    }

    #[test]
    fn forward_yields_prom_fallback() {
        let cfg = AdapterConfig::prometheus_promql("http://prom:9090".into(), true);
        assert!(
            cfg.fallback.is_some(),
            "forward_unsupported=true must install the Prom fallback",
        );
    }
}
