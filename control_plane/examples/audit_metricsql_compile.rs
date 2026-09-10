use control_plane::physical::{
    compiler::{BackendLocalPlanningSnapshot, PhysicalCompiler},
    post_asap::cost_model::ForcedFamilyCostModel,
};
use planner_types::post_asap::SketchAlgorithm;
use serde::Deserialize;
use serde_json::{json, Value};
use std::fs;
#[derive(Deserialize)]
struct C {
    queries: Vec<R>,
}
#[derive(Deserialize)]
struct R {
    id: String,
    metricsql: String,
}
fn main() {
    let c: C =
        serde_json::from_slice(&fs::read(std::env::args().nth(1).unwrap()).unwrap()).unwrap();
    let template: Value = serde_json::from_str(include_str!(
        "../../docs/examples/asapquery-compatibility-demo-snapshot.json"
    ))
    .unwrap();
    let mut out = vec![];
    for row in c.queries {
        let mut fixture = template.clone();
        let mut demand = fixture["query_workload"]["repeating_queries"][0].clone();
        demand["query"] = row.metricsql.clone().into();
        fixture["query_workload"]["repeating_queries"] = json!([demand]);
        fixture["implementation"]["topk_evidence"] = json!({});
        let snapshot: BackendLocalPlanningSnapshot = serde_json::from_value(fixture).unwrap();
        let (mut req, env) = snapshot.planning_request().unwrap();
        req.hybrid_execution = false;
        let accuracy = req.queries[0].accuracy.clone();
        let expr = match asap_frontend_metricsql::lower_metricsql(&row.metricsql, accuracy.clone())
        {
            Ok(x) => x,
            Err(e) => {
                out.push(
                    json!({"id":row.id,"compile":{"status":"not_reached","reason":e.to_string()}}),
                );
                continue;
            }
        };
        if let Err(e) =
            control_plane::physical::compiler::validate_metricsql_acceleration_shape(&expr)
        {
            out.push(json!({"id":row.id,"compile":{"status":"not_reached","reason":e}}));
            continue;
        }
        req.queries[0].query_string = row.metricsql.clone();
        req.queries[0].post_asap = match control_plane::planner_selection::select_summary(
            &expr,
            &ForcedFamilyCostModel::new(accuracy, SketchAlgorithm::Kll),
        ) {
            Ok(x) => x,
            Err(e) => {
                out.push(json!({"id":row.id,"compile":{"status":"typed_fallback","reason":e.to_string()}}));
                continue;
            }
        };
        match PhysicalCompiler.compile_metricsql(req, env) {
            Ok(plan) => {
                let pub_ok = plan.publication().and_then(|p| {
                    serde_json::to_vec(&p)
                        .map(|_| ())
                        .map_err(|e| e.to_string())
                });
                out.push(json!({"id":row.id,"compile":{"status":"pass"},"publication":{"status":if pub_ok.is_ok(){"pass"}else{"failed"},"reason":pub_ok.err()}}))
            }
            Err(e) => out.push(
                json!({"id":row.id,"compile":{"status":"typed_fallback","reason":e.to_string()}}),
            ),
        }
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({"queries":out})).unwrap()
    )
}
