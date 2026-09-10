use serde_json::json;

fn main() {
    let snapshot: control_plane::physical::compiler::BackendLocalPlanningSnapshot =
        serde_json::from_str(include_str!(
            "../../docs/examples/asapquery-compatibility-demo-snapshot.json"
        ))
        .expect("checked-in planning snapshot");
    let plan = snapshot.compile().expect("compile physical plan");
    let artifact = json!({
        "summary_catalog": plan.summary_catalog,
        "collector_plans": plan.collector_plans,
        "precompute_plan": plan.precompute_plan,
        "transmission_plan": plan.transmission_plan,
        "query_plan": plan.query_plan,
        "metricsql_plan": plan.metricsql_plan,
        "clickhouse_sql": null,
        "storage_routing": null,
        "adaptation_evidence": [],
    });
    serde_json::to_writer(std::io::stdout(), &artifact).expect("write artifact");
}
