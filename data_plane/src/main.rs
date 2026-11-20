use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use data_plane::engine::EngineOptions;
use data_plane::ingestion::IngestionOptions;
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
    /// How long a query-time result is kept behind the newest evaluation
    /// time computed.
    #[arg(long, default_value_t = EngineOptions::default().query_time_results_retention_ms)]
    query_time_results_retention_ms: u64,
    /// The most bytes of query-time results kept.
    #[arg(long, default_value_t = EngineOptions::default().query_time_results_max_bytes)]
    query_time_results_max_bytes: usize,
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
                no_forward: args.no_forward,
                run_memory_limit_bytes: args.run_memory_limit_bytes,
                query_threads: args.query_threads,
                share_one_evaluation_results: !args.no_shared_results,
                query_time_results_retention_ms: args.query_time_results_retention_ms,
                query_time_results_max_bytes: args.query_time_results_max_bytes,
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
