use clap::{Parser, ValueEnum};
use std::fs;
use std::sync::Arc;
use thiserror::Error;
use tokio::signal;
use tracing::{error, info, warn};

use data_plane::drivers::AdapterConfig;
use data_plane::precompute_engine::config::LateDataPolicy;
use data_plane::precompute_engine::PrecomputeWorkerDiagnostics;
use data_plane::storage_engines::types::enums::{CleanupPolicy, LockStrategy};
use data_plane::{
    ASAPQueryEngine, HttpServer, HttpServerConfig, OtlpReceiver, OtlpReceiverConfig,
    PrecomputeEngine, PrecomputeEngineConfig, PrometheusRemoteWriteConfig,
    PrometheusRemoteWriteReceiver, Result, SketchStoreSink,
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
enum RuntimeProfile {
    #[default]
    Distributed,
    Asapquery,
}

fn unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Publish bounded materialization-input ERP observations after a verified finite-source drain.
    #[arg(long)]
    erp_runtime_samples_endpoint: Option<String>,
    /// Runtime component profile. `asapquery` enables backend ingest-time
    /// materialization and rejects Collector/OTLP-only components.
    #[arg(long, value_enum, default_value = "distributed")]
    profile: RuntimeProfile,

    /// Monitor coordinator settings, separate from executable computation plans.
    #[arg(long)]
    monitor_specs: Option<std::path::PathBuf>,

    /// JSON physical-plan artifact. Required by the backend-local profile;
    /// all runtime/query views are validated and installed as one snapshot.
    #[arg(long)]
    physical_plan: Option<std::path::PathBuf>,

    /// Versioned canonical QueryWorkload + DataWorkload and backend-local
    /// implementation evidence. The ASAPQuery profile invokes the pinned
    /// Planner and DeploymentPlanCompiler at startup when this is supplied.
    #[arg(long)]
    planning_snapshot: Option<std::path::PathBuf>,

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
    /// the installed precompute plan.
    #[arg(long, default_value = "30")]
    prometheus_scrape_interval: u64,

    /// HTTP server port for the PromQL-compatible query surface.
    /// `--query-port` is accepted as an alias for compatibility with
    /// the legacy `precompute_engine` binary's flag (whose default
    /// was 8080). Compose stacks pass `--query-port=9091`.
    #[arg(long, alias = "query-port", default_value = "8088")]
    http_port: u16,

    /// Independent VictoriaMetrics-compatible MetricsQL query listener.
    #[arg(long)]
    victoriametrics_http_port: Option<u16>,

    /// VictoriaMetrics base URL used for exact MetricsQL fallback.
    #[arg(long, default_value = "http://localhost:8428")]
    victoriametrics_url: String,

    /// Optional independent ClickHouse-compatible HTTP listener.
    #[arg(long, env = "ASAP_CLICKHOUSE_HTTP_PORT")]
    clickhouse_http_port: Option<u16>,

    /// Exact ClickHouse HTTP endpoint used by the SQL listener.
    #[arg(
        long,
        env = "ASAP_CLICKHOUSE_URL",
        default_value = "http://localhost:8123"
    )]
    clickhouse_url: String,

    /// Default database supplied when a ClickHouse request omits one.
    #[arg(long, env = "ASAP_CLICKHOUSE_DATABASE", default_value = "default")]
    clickhouse_database: String,

    /// Enable the configured ClickHouse connection for typed table backfill jobs.
    #[arg(long, env = "ASAP_CLICKHOUSE_BACKFILL_TABLE")]
    clickhouse_backfill_table: Option<String>,
    #[arg(
        long,
        env = "ASAP_CLICKHOUSE_BACKFILL_DATABASE",
        default_value = "default"
    )]
    clickhouse_backfill_database: String,
    #[arg(
        long,
        env = "ASAP_CLICKHOUSE_BACKFILL_METRIC_COLUMN",
        default_value = "metric"
    )]
    clickhouse_backfill_metric_column: String,
    #[arg(
        long,
        env = "ASAP_CLICKHOUSE_BACKFILL_LABELS_COLUMN",
        default_value = "labels"
    )]
    clickhouse_backfill_labels_column: String,
    #[arg(
        long,
        env = "ASAP_CLICKHOUSE_BACKFILL_TIMESTAMP_COLUMN",
        default_value = "timestamp_ms"
    )]
    clickhouse_backfill_timestamp_column: String,
    #[arg(
        long,
        env = "ASAP_CLICKHOUSE_BACKFILL_VALUE_COLUMN",
        default_value = "value"
    )]
    clickhouse_backfill_value_column: String,
    #[arg(long, env = "ASAP_CLICKHOUSE_USER")]
    clickhouse_user: Option<String>,
    #[arg(long, env = "ASAP_CLICKHOUSE_PASSWORD")]
    clickhouse_password: Option<String>,

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

    /// Forward unsupported queries to Prometheus
    #[arg(long)]
    forward_unsupported_queries: bool,

    /// Disable all external query forwarding for isolated tests.
    #[arg(long)]
    disable_query_forwarding: bool,

    /// Database path (currently unused, kept for compatibility)
    #[arg(long, default_value = "sketchdb.db")]
    db_path: String,

    /// Delete existing database (currently unused, kept for compatibility)
    #[arg(long)]
    delete_existing_db: bool,

    /// Output directory for logs
    #[arg(long, default_value = "/var/log/asap")]
    output_dir: String,

    #[command(flatten)]
    runtime: data_plane::runtime_config::RuntimeConfig,

    #[command(flatten)]
    logging: data_plane::runtime_config::LogConfig,

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

    /// Enable Prometheus Remote Write v1 at POST /api/v1/write. Automatically
    /// enabled by `--profile asapquery`.
    #[arg(long)]
    enable_remote_write: bool,

    /// Maximum compressed Remote Write request size.
    #[arg(long, default_value = "33554432")]
    remote_write_max_compressed_bytes: usize,

    /// Maximum Snappy-decompressed Remote Write request size.
    #[arg(long, default_value = "134217728")]
    remote_write_max_decompressed_bytes: usize,

    /// Maximum series in one Remote Write request.
    #[arg(long, default_value = "100000")]
    remote_write_max_timeseries: usize,

    /// Maximum samples in one Remote Write request.
    #[arg(long, default_value = "1000000")]
    remote_write_max_samples: usize,

    /// Bounded Remote Write idempotency horizon.
    #[arg(long, default_value = "600000")]
    remote_write_dedup_horizon_ms: u64,

    /// Expected maximum interval over which Prometheus can retry a request.
    #[arg(long, default_value = "60000")]
    remote_write_expected_retry_interval_ms: u64,

    /// Maximum retained (series,timestamp) idempotency keys.
    #[arg(long, default_value = "2000000")]
    remote_write_max_dedup_entries: usize,

    /// OTLP gRPC listen port
    #[arg(long, default_value = "4317")]
    otel_grpc_port: u16,

    /// OTLP HTTP listen port
    #[arg(long, default_value = "4318")]
    otel_http_port: u16,

    /// Enable the continuous-monitoring (CDM) coordinator gRPC server. Serves
    /// the specs from `--monitor-specs`; no-op if that list is
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

    /// Compatibility option for schema persistence. Sid lifecycle persistence is
    /// managed by the sketch store.
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
    /// pick the right engine (`ASAPQueryEngine` for ASAP-tier
    /// sketches). Without
    /// this flag the handler falls back to the installed storage view
    /// single axis (always `SketchStore`) and the EngineRouter is
    /// effectively bypassed — the issue-46 v2 demo's criterion ⑤
    /// failure mode. Mirrors the `precompute_engine` binary's flag
    /// of the same name.
    #[arg(long, env = "ASAP_BACKEND_STORAGE_ROUTING")]
    backend_storage_routing: Option<std::path::PathBuf>,
}

#[derive(Debug, Error)]
enum QueryForwardingConfigError {
    #[error("--disable-query-forwarding conflicts with --forward-unsupported-queries")]
    ConflictingFlags,
    #[error("--profile asapquery requires query forwarding and cannot be combined with --disable-query-forwarding")]
    AsapqueryRequiresForwarding,
    #[error("--disable-query-forwarding cannot be combined with --victoriametrics-http-port")]
    VictoriaMetricsListenerConfigured,
    #[error("--disable-query-forwarding cannot be combined with --clickhouse-http-port")]
    ClickHouseListenerConfigured,
}

fn validate_query_forwarding_configuration(
    args: &Args,
) -> std::result::Result<(), QueryForwardingConfigError> {
    if args.disable_query_forwarding {
        if args.forward_unsupported_queries {
            return Err(QueryForwardingConfigError::ConflictingFlags);
        }
        if args.profile == RuntimeProfile::Asapquery {
            return Err(QueryForwardingConfigError::AsapqueryRequiresForwarding);
        }
        if args.victoriametrics_http_port.is_some() {
            return Err(QueryForwardingConfigError::VictoriaMetricsListenerConfigured);
        }
        if args.clickhouse_http_port.is_some() {
            return Err(QueryForwardingConfigError::ClickHouseListenerConfigured);
        }
    }
    Ok(())
}

fn validate_profile(args: &Args) -> Result<()> {
    validate_query_forwarding_configuration(args)?;
    if args.profile != RuntimeProfile::Asapquery {
        if args.physical_plan.is_none() {
            return Err("the distributed profile requires --physical-plan".into());
        }
        if args.planning_snapshot.is_some() {
            return Err("--planning-snapshot is available only with --profile asapquery".into());
        }
        return Ok(());
    }
    if args.physical_plan.is_some() == args.planning_snapshot.is_some() {
        return Err(
            "--profile asapquery requires exactly one of --planning-snapshot or --physical-plan"
                .into(),
        );
    }
    let mut excluded = Vec::new();
    if args.enable_otel_ingest {
        excluded.push("--enable-otel-ingest");
    }
    if args.enable_monitor_coordinator {
        excluded.push("--enable-monitor-coordinator");
    }
    if args.enable_backfill_worker {
        excluded.push("--enable-backfill-worker");
    }
    if args.enable_schema_eviction {
        excluded.push("--enable-schema-eviction");
    }
    if args.persistence_enabled {
        excluded.push("--persistence-enabled");
    }
    if args.backend_storage_routing.is_some() {
        excluded.push("--backend-storage-routing");
    }
    if !excluded.is_empty() {
        return Err(format!(
            "--profile asapquery excludes distributed/durable components: {}",
            excluded.join(", ")
        )
        .into());
    }
    if !args.forward_unsupported_queries {
        return Err(
            "--profile asapquery requires --forward-unsupported-queries for exact fallback".into(),
        );
    }
    let required_horizon = (args.precompute_allowed_lateness_ms.max(0) as u64)
        .saturating_add(args.remote_write_expected_retry_interval_ms);
    if args.remote_write_dedup_horizon_ms < required_horizon {
        return Err(format!(
            "--remote-write-dedup-horizon-ms must cover allowed lateness plus the expected retry interval (at least {required_horizon}ms)"
        )
        .into());
    }
    if args.remote_write_max_compressed_bytes == 0
        || args.remote_write_max_decompressed_bytes == 0
        || args.remote_write_max_timeseries == 0
        || args.remote_write_max_samples == 0
        || args.remote_write_max_dedup_entries == 0
    {
        return Err("Remote Write resource limits must all be greater than zero".into());
    }
    if args.remote_write_max_decompressed_bytes < args.remote_write_max_compressed_bytes {
        return Err(
            "--remote-write-max-decompressed-bytes must be at least the compressed limit".into(),
        );
    }
    Ok(())
}

async fn verify_prometheus_fallback(base_url: &str) -> Result<()> {
    let url = format!("{}/-/healthy", base_url.trim_end_matches('/'));
    let response = reqwest::Client::new()
        .get(&url)
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await
        .map_err(|error| format!("Prometheus fallback health check {url} failed: {error}"))?;
    if !response.status().is_success() {
        return Err(format!(
            "Prometheus fallback health check {url} returned {}",
            response.status()
        )
        .into());
    }
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();
    let runtime = args.runtime.build()?;
    runtime.block_on(run(args))
}

async fn run(args: Args) -> Result<()> {
    validate_profile(&args)?;

    // Create output directory
    fs::create_dir_all(&args.output_dir)?;

    // Initialize logging similar to Python's create_loggers function
    // Keep the guard alive for the entire lifetime of the application
    let _log_guard = args.logging.init(std::path::Path::new(&args.output_dir))?;
    // Persist independently of the log filter, including when all logs are off.
    let configuration = serde_json::json!({
        "runtime_workers": tokio::runtime::Handle::current().metrics().num_workers(),
        "max_blocking_threads": args.runtime.runtime_max_blocking_threads,
        "precompute_worker_tasks": args.precompute_num_workers,
        "logging": args.logging, "effective_log_filter": args.logging.filter(),
        "process_at_startup": data_plane::runtime_config::process_snapshot()
    });
    fs::write(
        std::path::Path::new(&args.output_dir).join("runtime-config.json"),
        serde_json::to_vec_pretty(&configuration)?,
    )?;

    info!("Starting Query Engine Rust");
    info!("Output directory: {}", args.output_dir);

    if args.profile == RuntimeProfile::Asapquery {
        verify_prometheus_fallback(&args.prometheus_server).await?;
        info!("ASAPQuery compatibility profile: Prometheus fallback is healthy");
    }

    if let Some(ingest_port) = args.ingest_port {
        warn!(
            "--ingest-port={ingest_port} is deprecated and ignored: Remote Write and PromQL \
             share --http-port. Drop the flag from your compose `command:` block."
        );
    }

    let startup_artifact = if let Some(path) = args.planning_snapshot.as_ref() {
        let bytes = fs::read(path)?;
        let mut snapshot: control_plane::physical::compiler::BackendLocalPlanningInput =
            serde_json::from_slice(&bytes).map_err(|error| {
                format!(
                    "failed to decode planning snapshot {}: {error}",
                    path.display()
                )
            })?;
        let runtime_memory_budget = u64::try_from(args.persistence_memory_limit_mb)
            .unwrap_or(u64::MAX)
            .saturating_mul(1024 * 1024);
        snapshot
            .physical_inputs
            .retained_summary_memory_budget_bytes = snapshot
            .physical_inputs
            .retained_summary_memory_budget_bytes
            .min(runtime_memory_budget);
        let plan = snapshot
            .compile_promql()
            .map_err(|error| format!("startup planning failed for {}: {error}", path.display()))?;
        if let Some(comparison) = &plan.cost_comparison {
            info!(
                "Startup workload cost decision: {}",
                serde_json::to_string(comparison)?
            );
        }
        Some(
            data_plane::drivers::query::servers::http::PhysicalPlanInstallRequest {
                summary_catalog: plan.summary_catalog,
                collector_plans: plan.collector_plans,
                precompute_plan: plan.precompute_plan,
                transmission_plan: plan.transmission_plan,
                query_plan: plan.query_plan,
                storage_routing: None,
                adaptation_evidence: Vec::new(),
            },
        )
    } else if let Some(path) = args.physical_plan.as_ref() {
        let bytes = fs::read(path)?;
        Some(serde_json::from_slice(&bytes).map_err(|error| {
            format!(
                "failed to decode physical plan artifact {}: {error}",
                path.display()
            )
        })?)
    } else {
        None
    };
    let startup_physical_plan = if let Some(artifact) = startup_artifact {
        let active = data_plane::drivers::query::servers::http::validate_and_build_runtime_plan(
            artifact,
            Arc::new(data_plane::storage_engines::types::BackendStorageRouting::empty()),
        )
        .map_err(|error| format!("invalid startup PhysicalPlan: {error}"))?;
        if args.profile == RuntimeProfile::Asapquery
            && (active.plan_id() == 0
                || !matches!(
                    active.precompute_plan.ingest.protocol,
                    asap_types::precompute_plan::IngestProtocol::PrometheusRemoteWriteV1
                )
                || active.precompute_plan.ingest.endpoint_path != "/api/v1/write")
        {
            return Err("the asapquery profile requires a non-bootstrap physical plan declaring prometheus_remote_write_v1 at /api/v1/write".into());
        }
        let now = unix_time_ms();
        if active.activation_unix_ms() > now {
            return Err(format!(
                "physical plan activation {} is later than startup time {now}",
                active.activation_unix_ms()
            )
            .into());
        }
        if active.expiry_unix_ms().is_some_and(|expiry| expiry <= now) {
            return Err("physical plan artifact is expired".into());
        }
        Some(active)
    } else {
        None
    };
    let startup_physical_plan = startup_physical_plan.ok_or("startup requires a physical plan")?;
    let installed_precompute_plan = startup_physical_plan.installed_precompute_plan.clone();
    info!(
        "Installed precompute DAG with {} stored outputs",
        installed_precompute_plan.materializations().len()
    );

    // Share a hot-reload handle with HTTP configuration endpoints and consumers.

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
    let summary_store = Arc::new(data_plane::storage_engines::sketch_db::index::SketchStore::new());
    if let Some(catalog) = startup_physical_plan.summary_catalog.as_ref() {
        summary_store
            .install_precompute_plan(Arc::clone(catalog), &startup_physical_plan.precompute_plan)
            .map_err(std::io::Error::other)?;
    }

    // Enable persistence for the shared sketch store so precompute and sketch
    // state survive restarts.
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
            summary_store
                .start_persistence(cfg)
                .expect("SketchStore::start_persistence failed"),
        )
    } else {
        None
    };

    let initial_active_plan = startup_physical_plan;
    let active_physical_plan =
        data_plane::storage_engines::types::ActivePhysicalPlanHandle::new(initial_active_plan);
    let hot_reload_config =
        data_plane::storage_engines::types::InstalledPrecomputePlanHandle::from_active_physical_plan(
            active_physical_plan.clone(),
        );

    // Query execution reads generation-consistent runtime configuration from
    // the RuntimePhysicalPlan installed below.
    // Phase 5 wire-in (refactor 2026-05): hand the ASAP-tier SummaryStore to the
    // query engine so SeriesLookup classification drives the Phase 6 archive
    // failover via EngineError::CapabilityMiss when the ASAP tier is empty /
    // ghost / unknown.
    let query_forwarding_policy = if args.disable_query_forwarding {
        data_plane::query_engines::QueryForwardingPolicy::Disabled
    } else {
        data_plane::query_engines::QueryForwardingPolicy::Enabled
    };
    if !query_forwarding_policy.allows_external_queries() {
        info!("query forwarding disabled for this process");
    }
    let engine = ASAPQueryEngine::new(args.prometheus_scrape_interval)
        .with_sketch_index(summary_store.clone())
        .with_active_physical_plan(active_physical_plan.clone())
        .with_query_forwarding_policy(query_forwarding_policy)
        .with_exact_subquery_endpoint(args.prometheus_server.clone())
        .with_metricsql_exact_subquery_endpoint(args.victoriametrics_url.clone());

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
            late_data_policy: LateDataPolicy::ForwardToStore,
            wall_clock_idle_grace_period_ms: 5_000,
            wall_clock_max_open_grace_period_ms: 5_000,
            schema_persist_path: args.schema_persist_path.clone(),
        };
        // M2.3.6 — sketch-only sink. Precompute writes now go to
        // `SketchStore` exclusively; the legacy `SketchStore` no
        // longer receives traffic from either ingest (this sink) or
        // queries (engine M2.3.5b cut-over). The `store` Arc kept
        // below is for the eviction service + diagnostic plumbing
        // until subsequent M2.3.6 sub-PRs delete those too.
        let output_sink = Arc::new(SketchStoreSink::new(
            summary_store.clone(),
            hot_reload_config.clone(),
            series_resolver.clone(),
        ));
        let engine = PrecomputeEngine::new(
            precompute_config,
            hot_reload_config.clone(),
            output_sink,
            series_resolver.clone(),
            summary_store.clone(),
        );
        if let Some(endpoint) = args.erp_runtime_samples_endpoint.clone() {
            let generation = engine
                .ingest_state()
                .active_physical_plan_snapshot()
                .and_then(|plan| plan.precompute_plan.summary_catalog.clone())
                .ok_or_else(|| {
                    std::io::Error::other("ERP observation requires an installed catalog")
                })?;
            engine
                .ingest_state()
                .router
                .enable_erp_observation(endpoint, generation)
                .map_err(std::io::Error::other)?;
        }
        let worker_diagnostics = engine.diagnostics();
        let ingest_state = engine.ingest_state();
        info!("Starting precompute engine (ingest adapters share its bounded worker queues)");

        // Log memory diagnostics for the shared sketch store.
        let diag_index = summary_store.clone();
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

    // Sid lifecycle is owned by the shared sketch store.
    let engine = Arc::new(engine);

    // Idle-sid eviction sweep (memory reclaim) — opt-in via
    // --idle-sid-evict-secs. Drops the in-memory `SidStoreData` for
    // write-idle, fully-flushed sketch sids while keeping their queryable
    // metadata, bounding resident registry memory under series churn.
    if args.idle_sid_evict_secs > 0 {
        let evict_index = summary_store.clone();
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
        use data_plane::update_sampling::{
            Functional, MonitorConfig, MonitorCoordinator, MonitorServiceImpl,
        };
        let monitor_specs: Vec<asap_types::MonitorSpec> = match &args.monitor_specs {
            Some(path) => serde_yaml::from_slice(&fs::read(path)?)?,
            None => Vec::new(),
        };
        let specs = monitor_specs
            .into_iter()
            .map(|m| MonitorConfig {
                agg_id: m.agg_id,
                key: m.key.into_bytes(),
                tau: m.tau,
                epsilon: m.epsilon,
                window_ms: m.window_ms,
                functional: Functional::from_name(&m.functional),
            })
            .collect();
        let coord = MonitorCoordinator::new(specs);

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
    )
    .with_query_forwarding_policy(query_forwarding_policy);

    let http_config = HttpServerConfig {
        port: args.http_port,
        handle_http_requests: true,
        adapter_config,
    };

    // The backend consumes streaming configuration and storage routing pushed
    // by the control plane through their HTTP endpoints.

    // HTTP endpoints inspect lifecycle metadata in the shared sketch store.
    let mut server = HttpServer::new(http_config, engine, summary_store.clone())
        .with_active_physical_plan(active_physical_plan.clone())
        .with_probe_cache(probe_cache.clone());
    if args.profile == RuntimeProfile::Distributed {
        // Legacy partial-document endpoints remain available to distributed
        // deployments. The compatibility profile deliberately exposes only
        // the atomic CompiledPhysicalPlan stage/activate lifecycle.
        server = server.with_hot_reload_config(hot_reload_config.clone());
    }

    if args.enable_remote_write || args.profile == RuntimeProfile::Asapquery {
        let receiver = PrometheusRemoteWriteReceiver::new(
            PrometheusRemoteWriteConfig {
                max_compressed_bytes: args.remote_write_max_compressed_bytes,
                max_decompressed_bytes: args.remote_write_max_decompressed_bytes,
                max_timeseries: args.remote_write_max_timeseries,
                max_samples: args.remote_write_max_samples,
                dedup_horizon: std::time::Duration::from_millis(args.remote_write_dedup_horizon_ms),
                max_dedup_entries: args.remote_write_max_dedup_entries,
            },
            precompute_ingest_state
                .clone()
                .expect("precompute ingest state is always constructed"),
        );
        info!("Prometheus Remote Write v1 enabled at POST /api/v1/write");
        server = server.with_remote_write(receiver);
    }

    // Per-metric storage-backend routing table (issue #46
    // criterion ⑤). Mirror the `precompute_engine` binary: load it
    // from `--backend-storage-routing` (or its env-var alias) so the
    // HTTP handler consults a per-metric `StorageBackend` map on
    // every PromQL query instead of bypassing the EngineRouter when
    // the installed storage view single axis defaults to `SketchStore`.
    //
    // even when no static YAML is loaded, install an
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
    if active_physical_plan.active_snapshot().plan_id() == 0 {
        let current = active_physical_plan.active_snapshot();
        active_physical_plan.swap(data_plane::storage_engines::types::RuntimePhysicalPlan {
            readout_programs: Default::default(),
            envelope: current.envelope.clone(),
            summary_catalog: current.summary_catalog.clone(),
            precompute_plan: current.precompute_plan.clone(),
            transmission_plan: current.transmission_plan.clone(),
            installed_precompute_plan: current.installed_precompute_plan.clone(),
            query_plan: current.query_plan.clone(),
            storage_routing: Arc::new(bootstrap_routing),
        });
    }
    server = server.with_hot_reload_backend_storage_routing(
        data_plane::query_engines::routing::HotReloadBackendStorageRouting::from_active(
            active_physical_plan.clone(),
        ),
    );

    if args.persistence_delete_older_than_secs > 0 {
        server = server.with_data_retention_ms(args.persistence_delete_older_than_secs * 1000);
    }
    // Share the backfill registry between HTTP endpoints and the worker.
    // `--backfill-persist-path` enables recovery and atomic persistence of job
    // transitions; without it, job history is memory-only.
    let backfill_registry = Arc::new(match args.backfill_persist_path.as_ref() {
        Some(path) => {
            data_plane::storage_engines::sketch_db::BackfillRegistry::load_or_new(path.clone())
        }
        None => data_plane::storage_engines::sketch_db::BackfillRegistry::new(),
    });
    server = server.with_backfill_registry(backfill_registry.clone());

    // Start the backfill worker when enabled. Each source needs a reader factory;
    // unsupported sources fail the job with a missing-reader error.
    let backfill_service_handle = if let (true, Some(_ingest_state)) = (
        args.enable_backfill_worker,
        precompute_ingest_state.as_ref(),
    ) {
        // Backfill uses the installed precompute plan and sketch store.
        let reader_factory = match args.clickhouse_backfill_table.as_ref() {
            Some(table) => data_plane::storage_engines::sketch_db::clickhouse_reader_factory(
                data_plane::storage_engines::sketch_db::ClickHouseReaderConfig {
                    base_url: args.clickhouse_url.clone(),
                    database: args.clickhouse_backfill_database.clone(),
                    table: table.clone(),
                    metric_column: args.clickhouse_backfill_metric_column.clone(),
                    labels_column: args.clickhouse_backfill_labels_column.clone(),
                    timestamp_ms_column: args.clickhouse_backfill_timestamp_column.clone(),
                    value_column: args.clickhouse_backfill_value_column.clone(),
                    user: args.clickhouse_user.clone(),
                    password: args.clickhouse_password.clone(),
                },
            ),
            None => data_plane::storage_engines::sketch_db::default_reader_factory(),
        };
        let service = data_plane::storage_engines::sketch_db::BackfillService::new(
            backfill_registry.clone(),
            hot_reload_config.clone(),
            reader_factory,
            data_plane::storage_engines::sketch_db::BackfillServiceConfig::default(),
        )
        // Backfill and live ingest share the same sid resolver and sketch store.
        .with_sketch_index(summary_store.clone())
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

    // schema eviction service. On every poll interval,
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
        // Compare data retention against the configured retirement retention.
        data_plane::storage_engines::sketch_db::warn_if_retention_inverted(
            data_retention_opt,
            data_plane::storage_engines::sketch_db::DEFAULT_RETIREMENT_RETENTION,
        );
        let svc = data_plane::storage_engines::sketch_db::SchemaEvictionService::new(
            summary_store.clone(),
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

    let victoria_server = args.victoriametrics_http_port.map(|port| {
        use data_plane::drivers::query::adapters::VictoriaMetricsHttpAdapter;
        let config = AdapterConfig::victoriametrics_metricsql(args.victoriametrics_url.clone());
        info!("Starting VictoriaMetrics MetricsQL listener on port {port}");
        server
            .clone()
            .with_query_listener(port, config.clone())
            .with_protocol_adapter(Arc::new(VictoriaMetricsHttpAdapter::new(config)))
    });

    let victoria_task = victoria_server.map(|server| tokio::spawn(server.run()));

    let clickhouse_server_handle = args.clickhouse_http_port.map(|port| {
        let fallback = Arc::new(
            data_plane::query_engines::asap_clickhouse_query_engine::ClickHouseHttpFallback::new(
                args.clickhouse_url.clone(),
                args.clickhouse_database.clone(),
            ),
        );
        let accelerator = Arc::new(
            data_plane::query_engines::asap_clickhouse_query_engine::accelerator::CatalogClickHouseAccelerator::with_active_physical_plan_and_exact_backend(
                summary_store.clone(),
                active_physical_plan.clone(),
                fallback.clone(),
            ),
        );
        let clickhouse_server =
            data_plane::query_engines::asap_clickhouse_query_engine::ClickHouseHttpServer {
                listen_address: format!("0.0.0.0:{port}"),
                fallback,
            };
        info!("Starting ClickHouse-compatible HTTP proxy on port {port}");
        tokio::spawn(async move {
            let result = clickhouse_server.run_with_accelerator(accelerator).await;
            if let Err(error) = result {
                error!("ClickHouse HTTP server error: {error}");
            }
        })
    });
    fs::write(
        std::path::Path::new(&args.output_dir).join("runtime-ready.json"),
        serde_json::to_vec_pretty(&data_plane::runtime_config::process_snapshot())?,
    )?;
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

    if let Some(task) = victoria_task {
        task.abort();
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

    if let Some(handle) = clickhouse_server_handle {
        handle.abort();
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
    summary_store: Arc<data_plane::storage_engines::sketch_db::index::SketchStore>,
    worker_diagnostics: Option<Arc<PrecomputeWorkerDiagnostics>>,
) {
    use data_plane::storage_engines::sketch_db::index::persistence::EpochSource;
    use std::sync::atomic::Ordering;

    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
    loop {
        interval.tick().await;

        // Per-sid sketch-store diagnostics.
        let instance_count = summary_store.instance_count();
        let series_count = summary_store.series_len();
        // `approx_memory_bytes` is the flusher's EVICTABLE-payload gauge:
        // it counts only live sketch payloads (current_epoch + sealed), so
        // it correctly reads ~0 once everything has been flushed to disk.
        // On its own it badly misrepresents the store's footprint — the
        // per-sid registry + intern caches stay resident and are not
        // flushable. Report evictable payload, structural overhead, their
        // total store estimate, and process RSS ground truth separately.
        let payload_bytes = summary_store.approx_memory_bytes();
        let resident_bytes = summary_store.approx_resident_bytes();
        let structural_bytes = resident_bytes.saturating_sub(payload_bytes);
        let rss_bytes = process_resident_bytes();
        info!(
            "[MEMORY_DIAG] SketchStore: {} instance(s), {} sid(s) with state, \
             payload={:.2} KB (evictable, flusher gauge), \
             registry+intern\u{2248}{:.2} MB (structural), total-store\u{2248}{:.2} MB, \
             process RSS={:.1} MB",
            instance_count,
            series_count,
            payload_bytes as f64 / 1024.0,
            structural_bytes as f64 / (1024.0 * 1024.0),
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

#[cfg(test)]
mod tests {
    use super::{validate_profile, Args};
    use clap::Parser;
    use data_plane::drivers::AdapterConfig;

    // Every profile must bootstrap from the same validated physical artifact.
    #[test]
    fn distributed_accepts_physical_plan_without_streaming_config() {
        let args = Args::try_parse_from(["data_plane", "--physical-plan", "plan.json"]).unwrap();
        assert!(validate_profile(&args).is_ok());
    }

    #[test]
    fn asapquery_requires_atomic_physical_plan_not_streaming_config() {
        let valid = Args::try_parse_from([
            "data_plane",
            "--profile",
            "asapquery",
            "--physical-plan",
            "plan.json",
            "--forward-unsupported-queries",
        ])
        .unwrap();
        assert!(validate_profile(&valid).is_ok());

        assert!(
            Args::try_parse_from(["data_plane", "--streaming-config", "streaming.yaml"]).is_err()
        );

        let planned = Args::try_parse_from([
            "data_plane",
            "--profile",
            "asapquery",
            "--planning-snapshot",
            "workload.json",
            "--forward-unsupported-queries",
        ])
        .unwrap();
        assert!(validate_profile(&planned).is_ok());

        let ambiguous = Args::try_parse_from([
            "data_plane",
            "--profile",
            "asapquery",
            "--planning-snapshot",
            "workload.json",
            "--physical-plan",
            "plan.json",
            "--forward-unsupported-queries",
        ])
        .unwrap();
        assert!(validate_profile(&ambiguous)
            .unwrap_err()
            .to_string()
            .contains("exactly one"));
    }

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

    #[test]
    fn disable_query_forwarding_rejects_conflicting_flags() {
        let args = Args::try_parse_from([
            "data_plane",
            "--physical-plan",
            "streaming.yaml",
            "--disable-query-forwarding",
            "--forward-unsupported-queries",
        ])
        .unwrap();
        assert!(validate_profile(&args)
            .unwrap_err()
            .to_string()
            .contains("conflicts"));
    }

    #[test]
    fn disable_query_forwarding_rejects_forwarding_listeners() {
        let args = Args::try_parse_from([
            "data_plane",
            "--physical-plan",
            "streaming.yaml",
            "--disable-query-forwarding",
            "--victoriametrics-http-port",
            "8429",
        ])
        .unwrap();
        assert!(validate_profile(&args)
            .unwrap_err()
            .to_string()
            .contains("victoriametrics-http-port"));
    }

    #[test]
    fn disable_query_forwarding_rejects_clickhouse_listener() {
        let clickhouse = Args::try_parse_from([
            "data_plane",
            "--physical-plan",
            "streaming.yaml",
            "--disable-query-forwarding",
            "--clickhouse-http-port",
            "8124",
        ])
        .unwrap();
        assert!(validate_profile(&clickhouse)
            .unwrap_err()
            .to_string()
            .contains("clickhouse-http-port"));
    }

    #[test]
    fn disable_query_forwarding_rejects_asapquery_profile() {
        let args = Args::try_parse_from([
            "data_plane",
            "--profile",
            "asapquery",
            "--physical-plan",
            "plan.json",
            "--disable-query-forwarding",
        ])
        .unwrap();
        assert!(validate_profile(&args)
            .unwrap_err()
            .to_string()
            .contains("requires query forwarding"));
    }
}
