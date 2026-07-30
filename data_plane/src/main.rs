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

use data_plane::drivers::AdapterConfig;
use data_plane::precompute_engine::config::LateDataPolicy;
use data_plane::precompute_engine::PrecomputeWorkerDiagnostics;
use data_plane::storage_engines::types::enums::{CleanupPolicy, LockStrategy};
use data_plane::utils::file_io::read_streaming_config;
use data_plane::{
    ASAPQueryEngine, HttpServer, HttpServerConfig, OtlpReceiver, OtlpReceiverConfig,
    PrecomputeEngine, PrecomputeEngineConfig, Result, SketchStoreSink,
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

    /// Control-plane endpoint for capability-miss notifications
    /// (PR G). When set, `ASAPQueryEngine` fires a fire-and-forget
    /// POST to this URL every time a query can't find a compatible
    /// stored aggregation, so the control plane can generate a new
    /// sketch plan. When unset (default), capability misses fall
    /// through to the §5.2 fallback silently.
    /// Example: `http://control-plane.svc:8080/api/v1/plan`
    ///
    /// Falls back to the `ASAP_CONTROL_PLANE_URL` env var when the
    /// flag is not passed — `deploy/docker-compose/base.yml` sets
    /// the env var so the MVP demo doesn't need a per-arg overlay.
    #[arg(long, env = "ASAP_CONTROL_PLANE_URL")]
    control_plane_endpoint: Option<String>,

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

    /// Enable the continuous-monitoring (CDM) coordinator gRPC server. Serves
    /// the `monitors:` specs from the streaming-config; no-op if that list is
    /// empty.
    #[arg(long)]
    enable_monitor_coordinator: bool,

    /// CDM monitor coordinator gRPC listen port (edge MonitorService stream).
    #[arg(long, default_value = "4319")]
    monitor_grpc_port: u16,

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
    /// for control plane REFRESH dispatch validation. When on, a
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

    /// Idle-sid eviction (memory reclaim). Drop the in-memory state of any
    /// sketch sid with no writes for this many seconds AND whose state is
    /// fully flushed to disk, keeping its queryable metadata — the series
    /// stays answerable from the durable tier and rehydrates on the next
    /// write. Bounds resident registry memory when series churn / go stale
    /// (without it, stale sketch sids are pinned in RAM until config-driven
    /// retirement). 0 disables. Effective horizon is
    /// max(this, --persistence-hot-window-secs), since eviction waits for
    /// the sid's windows to seal+flush first.
    #[arg(long, env = "ASAP_IDLE_SID_EVICT_SECS", default_value = "0")]
    idle_sid_evict_secs: u64,

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

    /// Seal cadence in DISTINCT WINDOWS. The per-sid hot epoch is sealed
    /// into the (pending-flush) sealed ring once it accumulates this
    /// many distinct windows, giving the flusher sealed epochs to make
    /// durable. ~30s panes ⇒ 20 windows ≈ 10 min per part. 0 disables
    /// cadence sealing (no durable tier even with --persistence-enabled).
    #[arg(long, default_value = "20")]
    persistence_seal_window_count: usize,

    /// Path to the per-metric backend storage routing YAML
    /// (`{metric_name: storage_backend}` map). Loaded at startup and
    /// consulted by the HTTP query handler on every PromQL request to
    /// pick the right engine (`ASAPQueryEngine` for ASAP-tier sketches,
    /// `ThanosQueryEngine` for the cold archive, etc.). Without
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
    let hot_reload_config = data_plane::storage_engines::types::HotReloadStreamingConfig::from_arc(
        streaming_config.clone(),
    );

    // M2.3.6g — the legacy `SketchStore` construction is gone.
    // Production data lives in `SketchStore` (allocated below); the
    // single persistence flusher behind it is set up in the
    // `--persistence-enabled` block lower in main.rs via
    // `SketchStore::start_persistence`.
    let cleanup_policy = args.cleanup_policy;
    info!("Using cleanup policy: {:?}", cleanup_policy);
    let _ = (cleanup_policy, args.lock_strategy); // Both still parsed for backwards-compat CLI; no runtime effect.

    // Phase 4 + 5 wire-in (refactor 2026-05): allocate the shared
    // SeriesIdResolver + SketchStore once. The OTLP receive path
    // (sid resolution + unknown_series_ids stamping; SketchStore
    // .append_sample on every modified-OTLP sketch DP) AND the
    // ASAPQueryEngine query path (SketchStore.classify / query_range
    // for ASAP-tier reads) hold clones of these Arcs. Allocated
    // here before BOTH the ASAPQueryEngine and the precompute engine
    // are constructed so both can be wired with a single canonical
    // instance — even when precompute is disabled, the engine still
    // needs the index for the Phase 6 archive failover trigger.
    // Under --persistence-enabled, the resolver replays its WAL on
    // startup so the agent's cached sids stay valid across backend
    // restarts. Without persistence (tests, stateless deploys), every
    // restart drops the cache; agents recover via the existing
    // `unknown_series_ids` eviction primitive — one extra round trip
    // per identity on the first emit post-restart.
    let series_resolver = if args.persistence_enabled {
        use data_plane::drivers::ingest::series_resolver::SeriesIdResolver;
        let dir = args
            .persistence_dir
            .as_ref()
            .expect("--persistence-enabled requires --persistence-dir");
        let wal_path = std::path::PathBuf::from(dir).join("series_resolver.wal");
        info!("opening series-resolver WAL at {:?}", wal_path);
        Arc::new(SeriesIdResolver::open(wal_path)?)
    } else {
        Arc::new(data_plane::drivers::ingest::series_resolver::SeriesIdResolver::new())
    };
    let sketch_index = Arc::new(data_plane::storage_engines::sketch_db::index::SketchStore::new());

    // M2.3.6c — also start a persistence layer behind the SketchStore
    // when --persistence-enabled. SketchStore is now where all
    // precompute + sketch writes land (M2.3.6a), so flushing it to
    // disk is what makes Phase 5 ASAP-tier state survive restarts.
    // The legacy `SketchStore::with_persistence_per_key` flusher
    // constructed above is now a no-op (its source has no writes) —
    // it stays in place until subsequent M2.3.6 sub-PRs delete the
    // legacy SketchStore wholesale.
    let _sketch_index_persistence = if args.persistence_enabled {
        use data_plane::storage_engines::sketch_db::index::persistence::SketchStorePersistenceConfig;
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
        let index_persistence_dir = std::path::PathBuf::from(&disk_path).join("sketch_index");
        let cfg = SketchStorePersistenceConfig {
            memory_limit_bytes,
            memory_low_watermark_bytes: memory_limit_bytes * 8 / 10,
            hard_cap_bytes: memory_limit_bytes * 125 / 100,
            hot_window_ms,
            delete_older_than_ms,
            flush_interval: std::time::Duration::from_millis(args.persistence_flush_interval_ms),
            disk_path: index_persistence_dir.clone(),
            part_cache_bytes,
            seal_window_count: args.persistence_seal_window_count,
        };
        info!(
            "SketchStore persistence enabled: disk_path={:?}",
            index_persistence_dir
        );
        Some(
            sketch_index
                .start_persistence(cfg)
                .expect("SketchStore::start_persistence failed"),
        )
    } else {
        None
    };

    // BackendPlan wire format (design-backend-plan-wire-format.md):
    // install an empty hot-reload handle so `GET/POST
    // /api/v1/backend-plan` don't 503 before the control plane's first
    // push lands — same "install empty, let the first push fill it in"
    // pattern as `bootstrap_routing` below. Shared with both the query
    // engine (serving-time cutover, Phase 4) and the HTTP server (the
    // push target) so a POST is observable by the next query, same
    // sharing contract as `hot_reload_config`.
    let hot_reload_backend_plan = data_plane::storage_engines::types::HotReloadBackendPlan::new(
        control_plane::backend_plan::BackendPlan::default(),
    );

    // Setup query engine. ASAPQueryEngine shares the same
    // HotReloadStreamingConfig handle as the HTTP server, so a POST
    // to /api/v1/streaming-config is observable by the next query
    // (PR E phase 2). Without sharing the handle, ASAPQueryEngine
    // would take a one-time snapshot at construction and ignore
    // subsequent swaps.
    let engine = {
        let mut engine = ASAPQueryEngine::new_with_hot_reload(
            hot_reload_config.clone(),
            args.prometheus_scrape_interval,
        )
        // Phase 5 wire-in (refactor 2026-05): hand the ASAP-tier
        // SketchStore to the query engine so SidLookup classification
        // drives the Phase 6 archive failover via
        // EngineError::CapabilityMiss when the ASAP tier is empty
        // / ghost / unknown.
        .with_sketch_index(sketch_index.clone())
        .with_hot_reload_backend_plan(hot_reload_backend_plan.clone());
        if let Some(control_plane_endpoint) = args.control_plane_endpoint.as_ref() {
            info!(
                "Capability-miss notifications enabled → {}",
                control_plane_endpoint
            );
            let client: Arc<dyn data_plane::drivers::control_plane_client::ControlPlaneClient> =
                Arc::new(
                    data_plane::drivers::control_plane_client::HttpControlPlaneClient::new(
                        control_plane_endpoint.clone(),
                    ),
                );
            engine = engine.with_control_plane_client(client);
        } else {
            info!(
                "Capability-miss notifications disabled \
                 (pass --control-plane-endpoint=<url> to enable)"
            );
        }
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
        // `SketchStore` exclusively; the legacy `SketchStore` no
        // longer receives traffic from either ingest (this sink) or
        // queries (engine M2.3.5b cut-over). The `store` Arc kept
        // below is for the eviction service + diagnostic plumbing
        // until subsequent M2.3.6 sub-PRs delete those too.
        let output_sink = Arc::new(SketchStoreSink::new(
            sketch_index.clone(),
            hot_reload_config.clone(),
            series_resolver.clone(),
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

        // Spawn periodic memory diagnostics logger — M2.3.6g routes
        // through SketchStore now that SketchStore no longer holds
        // production data.
        let diag_index = sketch_index.clone();
        tokio::spawn(async move {
            spawn_memory_diagnostics(diag_index, Some(worker_diagnostics)).await;
        });

        let handle = tokio::spawn(async move {
            if let Err(e) = engine.run().await {
                error!("Precompute engine error: {}", e);
            }
        });
        (Some(handle), Some(ingest_state))
    };

    // Schema retirement #5 — agg_id-keyed `SchemaRegistry` is gone.
    // Both ingest and query observe the §7 timeline at the sid level
    // via the shared `SketchStore` (already passed in above).
    let engine = Arc::new(engine);

    // Idle-sid eviction sweep (memory reclaim) — opt-in via
    // --idle-sid-evict-secs. Drops the in-memory `SidStoreData` for
    // write-idle, fully-flushed sketch sids while keeping their queryable
    // metadata, bounding resident registry memory under series churn.
    if args.idle_sid_evict_secs > 0 {
        let evict_index = sketch_index.clone();
        let idle_ms = args.idle_sid_evict_secs.saturating_mul(1000);
        // Sweep a few times per idle horizon, clamped to a sane cadence.
        let sweep = std::time::Duration::from_secs(args.idle_sid_evict_secs.clamp(10, 60));
        info!(
            "Idle-sid eviction enabled: idle threshold {}s, sweep every {}s",
            args.idle_sid_evict_secs,
            sweep.as_secs()
        );
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(sweep);
            loop {
                interval.tick().await;
                let n = evict_index.evict_idle_series(idle_ms);
                if n > 0 {
                    info!(
                        "[IDLE_EVICT] evicted {} idle sid(s) from memory \
                         (still queryable from disk; rehydrate on next write)",
                        n
                    );
                }
            }
        });
    }

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

    // Coordinated-sampling monitor coordinator: bidi MonitorService gRPC that
    // answers each edge's periodic rate report with its whole-sketch ε-floor
    // sample_p grant. Global-threshold alerting is retired (see
    // data_plane::monitor module docs) — this coordinator never fires one.
    let monitor_handle = if args.enable_monitor_coordinator {
        use data_plane::monitor::{Functional, MonitorConfig, MonitorCoordinator, MonitorServiceImpl};
        let specs: Vec<MonitorConfig> = streaming_config
            .monitors()
            .iter()
            .map(|m| MonitorConfig {
                agg_id: m.agg_id,
                key: m.key.clone().into_bytes(),
                tau: m.tau,
                epsilon: m.epsilon,
                window_ms: m.window_ms,
                functional: Functional::from_name(&m.functional),
            })
            .collect();
        if specs.is_empty() {
            warn!("--enable-monitor-coordinator set but streaming-config has no `monitors:` yet — the coordinator will pick them up live when the control plane pushes a config (hot-reload)");
        }
        let coord = MonitorCoordinator::new(specs);

        // Hot-reload watcher: the coordinator reads `monitors:` once at boot, but
        // the control plane pushes the real config slightly AFTER boot via the
        // `/api/v1/streaming-config` POST (an ArcSwap in `hot_reload_config`).
        // Without this, a monitor that arrives post-boot never reaches the
        // coordinator and every edge registering for it is rejected as
        // "unconfigured". Watch the ArcSwap and re-apply its `monitors:` to the
        // live coordinator on each swap (cheap: an atomic load + pointer compare
        // every 2s; `reconfigure` is a no-op unless the spec set actually
        // changed). The same path covers controller-driven monitor add/remove.
        {
            let coord = coord.clone();
            let hot = hot_reload_config.clone();
            tokio::spawn(async move {
                let mut last = hot.snapshot();
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    let cur = hot.snapshot();
                    if Arc::ptr_eq(&last, &cur) {
                        continue;
                    }
                    last = cur.clone();
                    let specs: Vec<MonitorConfig> = cur
                        .monitors()
                        .iter()
                        .map(|m| MonitorConfig {
                            agg_id: m.agg_id,
                            key: m.key.clone().into_bytes(),
                            tau: m.tau,
                            epsilon: m.epsilon,
                            window_ms: m.window_ms,
                            functional: Functional::from_name(&m.functional),
                        })
                        .collect();
                    let (added, changed, removed) = coord.reconfigure(specs).await;
                    if added + changed + removed > 0 {
                        info!(
                            added,
                            changed,
                            removed,
                            "CDM monitor coordinator hot-reloaded monitors from pushed streaming-config"
                        );
                    }
                }
            });
        }

        let svc = MonitorServiceImpl::new(coord).into_server();
        let port = args.monitor_grpc_port;
        info!("Starting CDM monitor coordinator gRPC on 0.0.0.0:{port}");
        Some(tokio::spawn(async move {
            let addr = match format!("0.0.0.0:{port}").parse() {
                Ok(a) => a,
                Err(e) => {
                    error!("invalid monitor coordinator addr: {e}");
                    return;
                }
            };
            if let Err(e) = tonic::transport::Server::builder()
                .add_service(svc)
                .serve(addr)
                .await
            {
                error!("monitor coordinator server error: {e}");
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

    // Schema retirement #5 — the HTTP server no longer takes a
    // `SchemaRegistry`. `POST /api/v1/streaming-config` drives
    // lifecycle transitions at the sid level via the shared
    // `SketchStore` (already passed in below).
    let mut server = HttpServer::new(http_config, engine, sketch_index.clone())
        .with_hot_reload_config(hot_reload_config.clone())
        .with_hot_reload_backend_plan(hot_reload_backend_plan.clone())
        .with_probe_cache(probe_cache.clone());

    // Per-metric storage-backend routing table (issue #46
    // criterion ⑤). Mirror the `precompute_engine` binary: load it
    // from `--backend-storage-routing` (or its env-var alias) so the
    // HTTP handler consults a per-metric `StorageBackend` map on
    // every PromQL query instead of bypassing the EngineRouter when
    // the streaming-config single axis defaults to `SketchStore`.
    //
    // Phase α (MVP): even when no static YAML is loaded, install an
    // empty hot-reload handle so the control plane's first
    // `POST /api/v1/storage_routing` push lands without first-call 503
    // lossage. Operators can still hand-author the YAML for
    // dev / standalone — the YAML supplies the bootstrap, control plane
    // pushes overwrite it.
    let bootstrap_routing = if let Some(routing_path) = args.backend_storage_routing.as_deref() {
        match data_plane::storage_engines::types::BackendStorageRouting::from_yaml_file(
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
                    "Failed to load backend-storage-routing from {:?}: {} — installing an empty routing table; the control plane's first POST /api/v1/storage_routing push will fill it",
                    routing_path, e,
                );
                data_plane::storage_engines::types::BackendStorageRouting::empty()
            }
        }
    } else {
        info!(
            "--backend-storage-routing not set — installing an empty routing table; the control plane's first POST /api/v1/storage_routing push will fill it",
        );
        data_plane::storage_engines::types::BackendStorageRouting::empty()
    };
    server = server.with_backend_storage_routing(Arc::new(bootstrap_routing));

    // Phase-5/6 + Step-2.3: register the Thanos query engine on the
    // capability router. Path A2 is the only archive path now: when
    // `ASAP_THANOS_QUERY_URL` is set, the backend forwards
    // archive-tier PromQL queries to a `thanos-query` sidecar via the
    // [`ThanosQueryEngine`], registered under the single public id
    // `thanos_query`.
    //
    // The superseded legacy in-process `GorillaQueryEngine` /
    // `GorillaS3Store` leg (which read the custom GORILLA1 container
    // format from per-hour chunks on S3 / MinIO) has been deleted now
    // that Path A2 is verified end-to-end (agents emit XOR-chunk
    // fragments → backend gorilla-merger → TSDB blocks → S3 →
    // thanos-query).
    //
    // When `ASAP_THANOS_QUERY_URL` is not configured the binary
    // registers a `NoDataArchiveEngine` stub under `thanos_query`
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
                "Path A2: registering ThanosQueryEngine for the archive tier (data_source_id=thanos_query)",
            );
            let thanos_arc: Arc<dyn QueryEngine> = Arc::new(thanos);
            server = server.with_archive_query_engine(thanos_arc);
            archive_registered = true;
        }
        Ok(None) => {
            info!(
                "ASAP_THANOS_QUERY_URL not configured — router serves ASAP-tier metrics only (set ASAP_THANOS_QUERY_URL to enable Path A2 thanos archive forwarding)",
            );
        }
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
            use data_plane::query_engines::routing::QueryEngine;
            use data_plane::query_engines::NoDataArchiveEngine;
            info!(
                "Registering NoDataArchiveEngine stub on the archive slot (canonical data_source_id=thanos_query); set ASAP_REQUIRE_ARCHIVE_ENGINE=1 to disable",
            );
            let stub: Arc<dyn QueryEngine> = Arc::new(NoDataArchiveEngine::new());
            server = server.with_archive_query_engine(stub);
        }
    }

    if args.persistence_delete_older_than_secs > 0 {
        server = server.with_data_retention_ms(args.persistence_delete_older_than_secs * 1000);
    }
    // Backfill registry (sketch DB §10). A single `Arc` lives in
    // `main` so the HTTP endpoints (Phase 5d) can inspect / cancel
    // jobs and the upcoming worker pool (Phase 5e) can drain them.
    // Jobs stay `Queued` until 5e wires the worker — intentional
    // shadow-mode behaviour that lets operators validate the
    // control plane's REFRESH dispatch logic before workers exist.
    // Phase 5g: when `--backfill-persist-path` is set, the registry
    // loads prior job records from disk and rewrites the file on
    // every state transition. When unset, the registry is
    // memory-only and restart wipes job history.
    let backfill_registry = Arc::new(match args.backfill_persist_path.as_ref() {
        Some(path) => {
            data_plane::storage_engines::sketch_db::BackfillRegistry::load_or_new(path.clone())
        }
        None => data_plane::storage_engines::sketch_db::BackfillRegistry::new(),
    });
    server = server.with_backfill_registry(backfill_registry.clone());

    // Phase 5e: spawn the backfill drain service if requested. When
    // enabled with `--enable-backfill-worker`, the service picks
    // up queued jobs and runs them through a
    // `BackfillWindowProcessor` (real sketch rebuild + store
    // writes). Without a reader factory configured (Phase 5h), all
    // production `BackfillSource` variants fail fast with a clear
    // "no reader" error — still a step up from the old shadow
    // mode, since the control plane now gets signal that its REFRESH
    // dispatch was received but not executable.
    let backfill_service_handle = if let (true, Some(_ingest_state)) = (
        args.enable_backfill_worker,
        precompute_ingest_state.as_ref(),
    ) {
        // Schema retirement #5 — `BackfillService::new` no longer
        // takes a `SchemaRegistry`; it consults sid-level lifecycle on
        // `SketchStore` instead.
        let service = data_plane::storage_engines::sketch_db::BackfillService::new(
            backfill_registry.clone(),
            hot_reload_config.clone(),
            data_plane::storage_engines::sketch_db::default_reader_factory(),
            data_plane::storage_engines::sketch_db::BackfillServiceConfig::default(),
        )
        // M2.3.6e — replayed batches land in SketchStore (the only
        // destination after the M2.3.6g store retirement). Resolver
        // is the same shared mint authority as live ingest, so
        // backfilled precompute sids share the OTel namespace.
        .with_sketch_index(sketch_index.clone())
        .with_series_resolver(series_resolver.clone());
        info!(
            "Spawning BackfillService drain loop (reader factory: default — Prometheus sources wired, S3/OtherSketch fail fast)"
        );
        Some(service.spawn())
    } else {
        if args.enable_backfill_worker {
            warn!(
                "--enable-backfill-worker was set but precompute engine isn't enabled; backfill service NOT spawned"
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
    let schema_eviction_handle = if let (true, Some(_ingest_state)) = (
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
        // Schema retirement #5 — retention check now uses the
        // package-level default; per-registry retention overrides are
        // gone with the agg_id-keyed `SchemaRegistry`.
        data_plane::storage_engines::sketch_db::warn_if_retention_inverted(
            data_retention_opt,
            data_plane::storage_engines::sketch_db::DEFAULT_RETIREMENT_RETENTION,
        );
        let svc = data_plane::storage_engines::sketch_db::SchemaEvictionService::new(
            sketch_index.clone(),
            backfill_registry.clone(),
            data_plane::storage_engines::sketch_db::SchemaEvictionConfig {
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

    if let Some(handle) = monitor_handle {
        info!("Shutting down CDM monitor coordinator...");
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

/// Best-effort process resident-set size (RSS) in bytes, read from
/// `/proc/self/statm` (field 2 = resident pages × page size). Returns 0 if
/// unreadable (non-Linux / sandboxed) so the diagnostic degrades gracefully
/// rather than failing. This is the ground-truth counterpart to the
/// store's structural estimates in the memory diagnostic.
fn process_resident_bytes() -> usize {
    let Ok(statm) = std::fs::read_to_string("/proc/self/statm") else {
        return 0;
    };
    let Some(resident_pages) = statm.split_whitespace().nth(1) else {
        return 0;
    };
    let pages: usize = resident_pages.parse().unwrap_or(0);
    // `sysconf(_SC_PAGESIZE)` is 4 KiB on every platform this runs on.
    pages * 4096
}

/// Periodic memory diagnostics logger — runs every 30 seconds.
async fn spawn_memory_diagnostics(
    sketch_index: Arc<data_plane::storage_engines::sketch_db::index::SketchStore>,
    worker_diagnostics: Option<Arc<PrecomputeWorkerDiagnostics>>,
) {
    use data_plane::storage_engines::sketch_db::index::persistence::EpochSource;
    use std::sync::atomic::Ordering;

    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
    loop {
        interval.tick().await;

        // 1. SketchStore diagnostics (M2.3.6g — replaces the
        //    pre-M2.3 per-agg_id SketchStore::diagnostic_info).
        let instance_count = sketch_index.instance_count();
        let series_count = sketch_index.series_len();
        // `approx_memory_bytes` is the flusher's EVICTABLE-payload gauge:
        // it counts only live sketch payloads (current_epoch + sealed), so
        // it correctly reads ~0 once everything has been flushed to disk.
        // On its own it badly misrepresents the store's footprint — the
        // per-sid registry + intern caches stay resident and are not
        // flushable. Report all three: evictable payload, the structural
        // resident estimate, and the process RSS ground truth.
        let payload_bytes = sketch_index.approx_memory_bytes();
        let resident_bytes = sketch_index.approx_resident_bytes();
        let rss_bytes = process_resident_bytes();
        info!(
            "[MEMORY_DIAG] SketchStore: {} instance(s), {} sid(s) with state, \
             payload={:.2} KB (evictable, flusher gauge), \
             registry+intern\u{2248}{:.2} MB (resident, not flushable), \
             process RSS={:.1} MB",
            instance_count,
            series_count,
            payload_bytes as f64 / 1024.0,
            resident_bytes as f64 / (1024.0 * 1024.0),
            rss_bytes as f64 / (1024.0 * 1024.0),
        );

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
