use clap::Parser;
#[derive(Parser)]
struct Args {
    #[arg(long)]
    reports_dir: std::path::PathBuf,
}
fn main() -> anyhow::Result<()> {
    promql_compliance::runner::report_card(&Args::parse().reports_dir)
}
