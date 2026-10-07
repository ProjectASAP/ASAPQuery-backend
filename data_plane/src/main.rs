use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use control_plane::InstalledPlan;
use data_plane::{router, DataPlane};

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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
    let plan = InstalledPlan::load(&args.plan)?;
    tracing::info!(
        queries = plan.queries.len(),
        nodes = plan.dag.dag.nodes.len(),
        "installed plan"
    );
    let state = Arc::new(DataPlane::new(plan.queries, args.prometheus_url));
    let listener = tokio::net::TcpListener::bind(&args.listen).await?;
    tracing::info!(address = %listener.local_addr()?, "listening");
    axum::serve(listener, router(state)).await?;
    Ok(())
}
