use clap::Parser;
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    promql_compliance::runner::run(promql_compliance::runner::Args::parse(), true).await
}
