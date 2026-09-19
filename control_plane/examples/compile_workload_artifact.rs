//! Control-plane entry point: cost-select a workload and emit its atomic install request.
use control_plane::physical::compiler::BackendLocalPlanningInput;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let path = args
        .next()
        .ok_or("usage: compile_workload_artifact SNAPSHOT.json [--metricsql] [--dot OUTPUT.dot]")?;
    let mut metricsql = false;
    let mut dot_path = None;
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--metricsql" => metricsql = true,
            "--dot" => {
                dot_path = Some(args.next().ok_or("--dot requires an output path")?);
            }
            _ => return Err(format!("unexpected argument `{argument}`").into()),
        }
    }
    let snapshot: BackendLocalPlanningInput = serde_json::from_slice(&std::fs::read(path)?)?;
    let plan = if metricsql {
        snapshot.compile_metricsql()?
    } else {
        snapshot.compile_promql()?
    };
    if let Some(path) = dot_path {
        std::fs::write(path, control_plane::physical::plan_dot::render(&plan))?;
    }
    println!("{}", serde_json::to_string_pretty(&plan)?);
    Ok(())
}
