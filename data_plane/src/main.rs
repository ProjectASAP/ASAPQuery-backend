use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;

/// jemalloc: Remote Write decoding and query runs allocate many small,
/// short-lived objects across threads.
#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// jemalloc's options, read when it starts (`_RJEM_MALLOC_CONF` in the
/// environment still overrides them): background threads return freed
/// pages within about a second (`dirty_decay_ms`) and skip the muzzy
/// stage, and at most 8 arenas hold them. With the defaults (4 arenas per
/// core, 10 s decay) a data plane answering a dashboard kept 300–450 MiB
/// of freed pages resident at 10k series per metric.
#[allow(non_upper_case_globals)]
#[export_name = "_rjem_malloc_conf"]
static malloc_conf: &[u8] =
    b"background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:0,narenas:8\0";
use data_plane::engine::{EngineOptions, PrecomputeMode};
use data_plane::ingestion::IngestionOptions;
use data_plane::persist::ViewRead;
use data_plane::{router, DataPlane, DEFAULT_FORWARD_TIMEOUT};

/// Serve Remote Write ingestion and the Prometheus query API for a plan.
#[derive(Parser)]
struct Args {
    /// Plan written by the control plane.
    #[arg(long)]
    plan: PathBuf,
    /// Prometheus base URL that answers forwarded queries.
    #[arg(long, default_value = "http://localhost:9090")]
    prometheus_url: String,
    /// Seconds a forwarded query may take before it fails with 502.
    #[arg(long, default_value_t = DEFAULT_FORWARD_TIMEOUT.as_secs())]
    forward_timeout_seconds: u64,
    #[arg(long, default_value = "0.0.0.0:8088")]
    listen: String,
    /// How long after the newest sample passes a window's end it is refreshed.
    #[arg(long, default_value_t = IngestionOptions::default().allowed_lateness_ms)]
    allowed_lateness_ms: u64,
    /// Samples further ahead of the wall clock than this are dropped.
    #[arg(long, default_value_t = IngestionOptions::default().max_future_ms)]
    max_future_ms: u64,
    /// Where the producers' stream starts (ms since the epoch): every raw
    /// sample at or after it is written to the data plane, so a first window
    /// that starts at or after it is complete and is refreshed. By default
    /// the first window each refresh group sees is skipped.
    #[arg(long)]
    stream_start_ms: Option<i64>,
    /// How much longer than the plan's retention views and raw samples are
    /// kept, so a query this far behind the newest refreshed window is still
    /// answered.
    #[arg(long, default_value_t = IngestionOptions::default().view_retention_margin_ms)]
    view_retention_margin_ms: u64,
    /// Samples queued for the engine beyond which Remote Write gets 503.
    #[arg(long, default_value_t = EngineOptions::default().max_queued_samples)]
    max_queued_samples: u64,
    /// Refresh windows closed and not yet refreshed beyond which ingestion
    /// waits (and Remote Write then gets 503).
    #[arg(long, default_value_t = EngineOptions::default().max_precompute_backlog)]
    max_precompute_backlog: usize,
    /// Ingest shard threads: each routes and appends the samples of the
    /// series hashed to it. Default: a quarter of the cores, at least 1.
    #[arg(long, default_value_t = EngineOptions::default().ingest_threads)]
    ingest_threads: usize,
    /// How the views of a closed window are built: `push` (each ingest
    /// shard folds its samples into the open windows' pipelines as they
    /// arrive; groups that cannot be pushed, such as wider levels of a
    /// window chain, keep the window path) or `window` (one ingestion run
    /// per closed window over buffered raw samples).
    #[arg(long, default_value = "push", value_parser = ["window", "push"])]
    precompute_mode: String,
    /// Push mode: bytes of open-window state all shards may hold together.
    #[arg(long)]
    precompute_memory_budget_bytes: Option<u64>,
    /// Push mode: a shard folds its pending samples once this many are
    /// pending (0: fold every request's samples at once).
    #[arg(long, default_value_t = data_plane::push::MorselBatching::default().max_samples)]
    push_batch_samples: usize,
    /// Push mode: ... or once the oldest pending sample is this old.
    #[arg(long, default_value_t = data_plane::push::MorselBatching::default().max_age_ms)]
    push_batch_max_ms: u64,
    /// Threads computing closed refresh windows at once: windows of
    /// different refresh groups, and of one group, run in parallel and are
    /// stored in window order. 1 computes one window at a time. Default: a
    /// quarter of the cores, from 1 to 4.
    #[arg(long, default_value_t = EngineOptions::default().precompute_threads)]
    precompute_threads: usize,
    /// Threads that write the view files (each the files of every n-th
    /// refresh group). Default: a quarter of the cores, from 1 to 4.
    #[arg(long, default_value_t = EngineOptions::default().persist_threads)]
    persist_threads: usize,
    /// Bytes of refreshed windows queued for the persist threads beyond
    /// which further windows are counted; refreshes never wait for the disk.
    #[arg(long, default_value_t = EngineOptions::default().max_persist_backlog_bytes)]
    max_persist_backlog_bytes: u64,
    /// Never forward a query to Prometheus: refuse a plan with a query the
    /// data plane cannot run, and answer any query the plan does not answer
    /// with an error giving the reason.
    #[arg(long)]
    no_forward: bool,
    /// The most bytes one ingestion or query run may hold.
    #[arg(long, default_value_t = EngineOptions::default().run_memory_limit_bytes)]
    run_memory_limit_bytes: usize,
    /// Query worker threads, the most queries run at once (default: one per
    /// core). Queries may hold up to this many times the run memory limit.
    #[arg(long, default_value_t = EngineOptions::default().query_threads)]
    query_threads: usize,
    /// Compute a query-time OneEvaluation node in every query's run instead
    /// of once per evaluation time (for comparison).
    #[arg(long)]
    no_shared_results: bool,
    /// How long a query-time result is kept behind the watermark.
    #[arg(long, default_value_t = EngineOptions::default().query_time_results_retention_ms)]
    query_time_results_retention_ms: u64,
    /// The most bytes of query-time results kept.
    #[arg(long, default_value_t = EngineOptions::default().query_time_results_max_bytes)]
    query_time_results_max_bytes: usize,
    /// Do not keep final query answers: every request computes every step.
    #[arg(long)]
    no_result_cache: bool,
    /// The most bytes of final query answers the result cache keeps.
    #[arg(long, default_value_t = EngineOptions::default().result_cache_max_bytes)]
    result_cache_bytes: usize,
    /// Where the materialized views are persisted (time segments per
    /// refresh group and retention) and read back from: at startup, and for
    /// windows evicted from memory.
    #[arg(long, default_value = "asap-views")]
    view_dir: PathBuf,
    /// Keep the materialized views in memory only.
    #[arg(long)]
    no_view_persistence: bool,
    /// How often the view files are synced to disk: a refreshed window is
    /// durable within this long (0: after every window). The default is
    /// VictoriaMetrics' default -inmemoryDataFlushInterval.
    #[arg(long, default_value_t = EngineOptions::default().view_sync_interval_ms)]
    view_sync_interval_ms: u64,
    /// Bytes of decoded view windows kept in memory; the other windows are
    /// read from the view files (mapped segments, states read in place).
    /// 0: every read is served from the files. Default: from the cgroup's
    /// memory.max, or no limit. Needs view persistence.
    #[arg(long)]
    view_cache_max_bytes: Option<u64>,
    /// A decoded window not read for this long leaves memory (0: never).
    /// Default: 60 s.
    #[arg(long)]
    view_cache_idle_ms: Option<u64>,
    /// Milliseconds of windows per view file segment; segments past every
    /// view's retention are deleted.
    #[arg(long, default_value_t = EngineOptions::default().view_segment_ms)]
    view_segment_ms: i64,
    /// How cold view windows are read: `mmap` (sealed segments mapped) or
    /// `pread`.
    #[arg(long, default_value = "mmap", value_parser = ["mmap", "pread"])]
    view_read: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
    let plan = std::fs::read_to_string(&args.plan)?;
    let state = Arc::new(
        DataPlane::with_options(
            plan,
            args.prometheus_url,
            EngineOptions {
                ingestion: IngestionOptions {
                    allowed_lateness_ms: args.allowed_lateness_ms,
                    max_future_ms: args.max_future_ms,
                    stream_start_ms: args.stream_start_ms,
                    view_retention_margin_ms: args.view_retention_margin_ms,
                },
                max_queued_samples: args.max_queued_samples,
                max_precompute_backlog: args.max_precompute_backlog,
                ingest_threads: args.ingest_threads,
                precompute_threads: args.precompute_threads,
                persist_threads: args.persist_threads,
                max_persist_backlog_bytes: args.max_persist_backlog_bytes,
                no_forward: args.no_forward,
                run_memory_limit_bytes: args.run_memory_limit_bytes,
                query_threads: args.query_threads,
                share_one_evaluation_results: !args.no_shared_results,
                query_time_results_retention_ms: args.query_time_results_retention_ms,
                query_time_results_max_bytes: args.query_time_results_max_bytes,
                result_cache: !args.no_result_cache,
                result_cache_max_bytes: args.result_cache_bytes,
                view_dir: (!args.no_view_persistence).then_some(args.view_dir),
                view_sync_interval_ms: args.view_sync_interval_ms,
                view_cache_max_bytes: args.view_cache_max_bytes,
                view_cache_idle_ms: args.view_cache_idle_ms,
                view_segment_ms: args.view_segment_ms,
                view_read: match args.view_read.as_str() {
                    "pread" => ViewRead::Pread,
                    _ => ViewRead::Mmap,
                },
                precompute_mode: match args.precompute_mode.as_str() {
                    "push" => PrecomputeMode::Push,
                    _ => PrecomputeMode::Window,
                },
                precompute_memory_budget_bytes: args.precompute_memory_budget_bytes,
                push_batching: data_plane::push::MorselBatching {
                    max_samples: args.push_batch_samples,
                    max_age_ms: args.push_batch_max_ms,
                },
                ..EngineOptions::default()
            },
        )?
        .with_forward_timeout(Duration::from_secs(args.forward_timeout_seconds)),
    );
    tracing::info!(queries = state.queries.len(), "installed plan");
    let listener = tokio::net::TcpListener::bind(&args.listen).await?;
    tracing::info!(address = %listener.local_addr()?, "listening");
    axum::serve(listener, router(state)).await?;
    Ok(())
}
