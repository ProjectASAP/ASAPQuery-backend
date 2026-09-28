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

/// Certified companion fixture for the two top-k queries.
///
/// The issue-754 generator cannot certify a heap: its per-series value domains
/// overlap, so no scalar `topk_selected_lower_bound > topk_excluded_upper_bound`
/// holds across the validity window, and these queries evaluate in `real_time`
/// scope rather than at one instant. `issue-754-certified-topk` gives each
/// series a decade of its own so the domains are disjoint by construction.
///
/// Every number below is derived from that dataset, never chosen: the bounds
/// come from the same `multiplier * base` .. `multiplier * (base + modulo)`
/// domain this file already uses for the quantile operands, and the distinct
/// item count is the series count. The assertion at the end is what makes this
/// a contract rather than a guess -- if the dataset stops separating, the
/// fixture fails instead of certifying something false.
pub fn certified_topk_input(case: &Case) -> BackendLocalPlanningInput {
    const K: usize = 3;
    let mut snapshot: Value = serde_json::from_str(include_str!(
        "../../../docs/examples/asapquery-planning-snapshot.json"
    ))
    .unwrap();
    snapshot["query_workload"]["repeating_queries"][0]["query"] = case.expr.clone().into();
    let fixture: Value = serde_yaml::from_str(include_str!(
        "../../../promql-compliance/datasets/issue-754-certified-topk.yaml"
    ))
    .unwrap();
    let series = fixture["series"].as_array().unwrap();
    let mut groups: std::collections::BTreeMap<String, Vec<(f64, f64)>> = Default::default();
    for entry in series {
        let g = &entry["generated_samples"];
        let n = |k: &str| g[k].as_f64().unwrap();
        assert!(n("multiplier") > 0.0 && n("modulo") > 0.0 && n("base") > 0.0);
        groups
            .entry(entry["labels"]["label_0"].as_str().unwrap().to_owned())
            .or_default()
            .push((
                n("multiplier") * n("base"),
                n("multiplier") * (n("base") + n("modulo")),
            ));
    }
    let mut selected_lower = f64::INFINITY;
    let mut excluded_upper = f64::NEG_INFINITY;
    for domains in groups.values_mut() {
        domains.sort_by(|a, b| b.0.total_cmp(&a.0));
        for (rank, (lower, upper)) in domains.iter().enumerate() {
            if rank < K {
                selected_lower = selected_lower.min(*lower);
            } else {
                excluded_upper = excluded_upper.max(*upper);
            }
        }
    }
    assert!(
        selected_lower > excluded_upper,
        "certified fixture does not separate the top-{K} boundary: \
         selected down to {selected_lower}, excluded up to {excluded_upper}"
    );
    let data_workload = snapshot["data_workload"].clone();
    snapshot["implementation"]["data_snapshot_id"] = json!("issue-754-certified-topk");
    snapshot["implementation"]["accuracy_evidence"][&case.expr] = json!({
        "query_string": case.expr,
        "data_snapshot_id": "issue-754-certified-topk",
        "data_workload": data_workload,
        "source": "issue-754-certified-topk-generator",
        "observed_at_unix_ms": 9500,
        "valid_for_ms": 60000,
        "topk_max_distinct_items": series.len() as u64,
        "topk_selected_lower_bound": selected_lower,
        "topk_excluded_upper_bound": excluded_upper,
        "topk_interval_failure_probability": 0.0,
    });
    serde_json::from_value(snapshot).unwrap()
}

pub fn ensembles() -> Vec<(String, Vec<Case>)> {
    let groups: [(&str, &[&str]); 3] = [
        (
            "shared-rate",
            &["temporal-rate", "grouped-rate", "topk-rate"],
        ),
        ("shared-quantiles", &["temporal-quantile", "quantile-ratio"]),
        ("all-ten", &[]),
    ];
    groups
        .into_iter()
        .map(|(name, names)| {
            (
                name.to_owned(),
                suite()
                    .queries
                    .into_iter()
                    .filter(|case| names.is_empty() || names.contains(&case.name.as_str()))
                    .collect(),
            )
        })
        .collect()
}

pub fn ensemble_input(cases: &[Case]) -> BackendLocalPlanningInput {
    let mut combined = serde_json::to_value(input(&cases[0])).unwrap();
    let mut queries = Vec::new();
    let mut evidence = serde_json::Map::new();
    for case in cases {
        let wire = serde_json::to_value(input(case)).unwrap();
        queries.extend(
            wire["query_workload"]["repeating_queries"]
                .as_array()
                .unwrap()
                .clone(),
        );
        if let Some(items) = wire["implementation"]["accuracy_evidence"].as_object() {
            evidence.extend(items.clone());
        }
    }
    combined["query_workload"]["repeating_queries"] = json!(queries);
    combined["implementation"]["data_snapshot_id"] = json!("issue-754-level1");
    combined["implementation"]["accuracy_evidence"] = json!(evidence);
    serde_json::from_value(combined).unwrap()
}
