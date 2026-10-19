use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use data_plane::engine::EngineOptions;
use data_plane::{router, DataPlane};

// jemalloc, as ASAPQuery's engine uses: the executor allocates per row, and
// glibc's malloc and free took a third of the data plane's CPU.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Serve Remote Write ingestion and the Prometheus query API for a plan.
#[derive(Parser)]
struct Args {
    /// Plan written by the control plane.
    #[arg(long)]
    plan: PathBuf,
    /// Prometheus base URL that answers forwarded queries.
    #[arg(long, default_value = "http://localhost:9090")]
    prometheus_url: String,
    #[arg(long, default_value = "0.0.0.0:8088")]
    listen: String,
    /// How long after the newest sample passes a pane's end it is built.
    #[arg(long, default_value_t = 15_000)]
    allowed_lateness_ms: i64,
    /// Working memory one query or pane build may hold, in MiB (the
    /// executor's default is 64); an evaluation over it is forwarded.
    #[arg(long, default_value_t = 64)]
    execution_memory_limit_mib: usize,
    /// Ingestion workers series are sharded across; default one per core.
    #[arg(long)]
    ingestion_workers: Option<usize>,
    /// Query threads, each running one query at a time; default one per core.
    #[arg(long)]
    query_threads: Option<usize>,
    /// How far back from the newest sample to keep raw samples for
    /// query-time raw windows, in ms; default the plan's longest such read.
    /// 0 reads every raw window from Prometheus remote read.
    #[arg(long)]
    raw_buffer_ms: Option<i64>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
    data_plane::precompute::set_execution_memory_limit(args.execution_memory_limit_mib << 20);
    let plan = std::fs::read_to_string(&args.plan)?;
    let defaults = EngineOptions::default();
    let options = EngineOptions {
        ingestion_workers: args.ingestion_workers.unwrap_or(defaults.ingestion_workers),
        query_threads: args.query_threads.unwrap_or(defaults.query_threads),
        raw_buffer_ms: args.raw_buffer_ms,
    };
    let state = Arc::new(DataPlane::with_threads(
        plan,
        args.prometheus_url,
        args.allowed_lateness_ms,
        options,
    )?);
    tracing::info!(queries = state.queries.len(), "installed plan");
    let reporting = state.clone();
    tokio::spawn(async move {
        let mut every = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            every.tick().await;
            if let Some(buffer) = reporting.engine.raw_buffer() {
                let (series, samples) = buffer.size();
                tracing::info!(series, samples, "local raw buffer");
            }
        }
    });
    let listener = tokio::net::TcpListener::bind(&args.listen).await?;
    tracing::info!(address = %listener.local_addr()?, "listening");
    axum::serve(listener, router(state)).await?;
    Ok(())
}
