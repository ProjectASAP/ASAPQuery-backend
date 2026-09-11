//! Compile a SQL workload through the real Planner without preset materializations.
use std::io::Read;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut input = String::new();
    std::io::stdin().read_to_string(&mut input)?;
    let workload: control_plane::clickhouse::ClickHouseSqlAutomaticWorkload =
        serde_json::from_str(&input)?;
    let (publication, selection_trace) =
        control_plane::clickhouse::compile_automatic_clickhouse_workload(&workload).await?;
    let install = publication.install_request(None, Vec::new())?;
    serde_json::to_writer_pretty(
        std::io::stdout(),
        &serde_json::json!({
            "publication": publication,
            "selection_trace": selection_trace,
            "install": install,
        }),
    )?;
    Ok(())
}
