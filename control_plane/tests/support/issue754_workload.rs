//! Shared query/accuracy fixture; contains no candidate prices or expected winner.
use control_plane::physical::compiler::BackendLocalPlanningInput;
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Deserialize)]
pub struct Suite {
    pub queries: Vec<Case>,
}
#[derive(Deserialize)]
pub struct Case {
    pub name: String,
    pub expr: String,
}

pub fn suite() -> Suite {
    serde_yaml::from_str(include_str!(
        "../../../promql-compliance/suites/issue-754.yaml"
    ))
    .unwrap()
}

pub fn input(case: &Case) -> BackendLocalPlanningInput {
    let mut snapshot: Value = serde_json::from_str(include_str!(
        "../../../docs/examples/asapquery-planning-snapshot.json"
    ))
    .unwrap();
    snapshot["query_workload"]["repeating_queries"][0]["query"] = case.expr.clone().into();
    if case.name == "quantile-ratio" {
        // The issue-754 generator defines the entire positive finite input
        // population. Supply its domain contract rather than certifying a
        // ratio from sample observations or weakening the admission rule.
        let fixture: Value = serde_yaml::from_str(include_str!(
            "../../../promql-compliance/datasets/issue-754.yaml"
        ))
        .unwrap();
        let mut lower = f64::INFINITY;
        let mut upper = f64::NEG_INFINITY;
        let mut count = 0u64;
        for series in fixture["series"].as_array().unwrap() {
            let g = &series["generated_samples"];
            let n = |k: &str| g[k].as_f64().unwrap();
            assert!(n("multiplier") > 0.0 && n("modulo") > 0.0 && n("base") > 0.0);
            lower = lower.min(n("multiplier") * n("base"));
            upper = upper.max(n("multiplier") * (n("base") + n("modulo")));
            count += ((n("end_offset_seconds") - n("start_offset_seconds")) / n("step_seconds"))
                .round() as u64
                + 1;
        }
        let root = control_plane::query_parser::parse_query_expr_canonical(
            &case.expr,
            planner_types::types::AccuracyTarget::EpsilonDelta {
                epsilon: 0.01,
                delta: 0.01,
            },
        )
        .unwrap();
        let planner_types::pre_asap::QueryExpr::BinaryOp { lhs, rhs, .. } = root else {
            panic!("ratio fixture");
        };
        snapshot["implementation"]["data_snapshot_id"] = json!("issue-754-level1");
        snapshot["implementation"]["accuracy_evidence"][&case.expr] = json!({
            "query_string":case.expr,"data_snapshot_id":"issue-754-level1",
            "data_workload":snapshot["data_workload"],"source":"issue-754-finite-generator",
            "observed_at_unix_ms":9500,"valid_for_ms":60000,
            "quantile_operand_domains":([lhs,rhs].into_iter().map(|operand| json!({
                "operand":operand,"lower":lower,"upper":upper,"max_samples":count,
                "contract":"complete finite issue-754 generator population"})).collect::<Vec<_>>())
        });
    }
    serde_json::from_value(snapshot).unwrap()
}
