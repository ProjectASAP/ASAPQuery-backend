use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
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
    /// How long after the newest sample passes a pane's end it is built.
    #[arg(long, default_value_t = 15_000)]
    allowed_lateness_ms: i64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
    let plan = std::fs::read_to_string(&args.plan)?;
    let state = Arc::new(
        DataPlane::new(plan, args.prometheus_url, args.allowed_lateness_ms)?
            .with_forward_timeout(Duration::from_secs(args.forward_timeout_seconds)),
    );
    tracing::info!(queries = state.queries.len(), "installed plan");
    let listener = tokio::net::TcpListener::bind(&args.listen).await?;
    tracing::info!(address = %listener.local_addr()?, "listening");
    axum::serve(listener, router(state)).await?;
    Ok(())
}
