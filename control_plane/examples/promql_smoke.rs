//! Run the smoke corpus through the backend's pinned PromQL planner bridge.
//! Binding is a capability diagnostic, not proof of execution or value correctness.
use control_plane::physical::post_asap::{bind_query_expr, PhysicalExpr, PostAsapPlan};
use control_plane::physical::promql_exact::ExactPromqlPlan;
use control_plane::query_parser::parse_query_expr_canonical;
use planner_types::post_asap::SummaryExpr;
use planner_types::types::AccuracyTarget;
use serde_json::{json, Value};
use std::collections::BTreeMap;

fn inspect(query: &Value) -> Value {
    let expr = query["expr"].as_str().expect("query expr must be a string");
    let mut row = json!({
        "id": query["id"], "category": query["category"],
        "function": query["function"], "experimental": query["experimental"],
        "expr": expr,
    });
    match parse_query_expr_canonical(expr, AccuracyTarget::Exact) {
        Err(error) => {
            row["status"] = json!("PARSE_REJECTED");
            row["error"] = json!(error.to_string());
        }
        Ok(tree) => {
            row["canonical"] = json!(format!("{tree:#?}"));
            match bind_query_expr(&tree, AccuracyTarget::Exact) {
                Ok(plan) => {
                    let logical_only = matches!(
                        &plan,
                        PhysicalExpr::Committed(PostAsapPlan::Summary(node))
                            if matches!(&node.expr, SummaryExpr::KeepPreAsap(_))
                    );
                    row["status"] = json!(if logical_only {
                        "LOGICAL_ONLY"
                    } else {
                        "BOUND"
                    });
                    row["plan"] = json!(format!("{plan:#?}"));
                }
                Err(error) => {
                    row["status"] = json!("BIND_REJECTED");
                    row["error"] = json!(error.to_string());
                }
            }
        }
    }
    row["summary_status"] = row["status"].take();
    row["summary_plan"] = row["plan"].take();
    row["summary_error"] = row["error"].take();
    match ExactPromqlPlan::bind(expr) {
        Ok(plan) => {
            row["status"] = json!("BOUND_EXACT");
            row["executor"] = json!("data_plane::query_engines::canonical::exact_promql");
            row["requires"] = json!("raw timestamped float samples");
            row["plan"] = json!(format!("{:#?}", plan.root()));
        }
        Err(error) => {
            row["status"] = json!("EXACT_BIND_REJECTED");
            row["error"] = json!(error.to_string());
        }
    }
    row
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let json_output = args.iter().any(|arg| arg == "--json");
    let require_bound = args.iter().any(|arg| arg == "--require-bound");
    for arg in &args {
        if arg.starts_with("--") && arg != "--json" && arg != "--require-bound" {
            return Err(format!("unknown option: {arg}").into());
        }
    }
    let path = args
        .iter()
        .find(|arg| !arg.starts_with("--"))
        .map(String::as_str)
        .unwrap_or("tools/promql-smoke/cases.json");
    let cases: Value = serde_json::from_slice(&std::fs::read(path)?)?;
    let mut results = Vec::new();
    let mut counts = BTreeMap::<String, usize>::new();
    for query in cases["queries"].as_array().ok_or("missing queries")? {
        // A panic is a failed case; continue to expose the other unsupported queries.
        let row = std::panic::catch_unwind(|| inspect(query)).unwrap_or_else(
            |_| json!({"id": query["id"], "expr": query["expr"], "status": "PANIC"}),
        );
        let status = row["status"].as_str().expect("case status");
        *counts.entry(status.to_owned()).or_default() += 1;
        if !json_output {
            println!(
                "{status} {}: {}{}",
                query["id"].as_str().unwrap_or("?"),
                query["expr"].as_str().unwrap_or("?"),
                row["error"]
                    .as_str()
                    .map(|e| format!(" — {e}"))
                    .unwrap_or_default(),
            );
        }
        results.push(row);
    }
    let has_unbound = results.iter().any(|row| row["status"] != "BOUND_EXACT");
    if json_output {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "accuracy": "Exact", "scope": "Canonical tree to native exact kernels; run promql_exact_execution for values",
                "summary": counts, "queries": results,
            }))?
        );
    } else {
        println!("Summary: {counts:?}; binding does not prove execution or correct values.");
    }
    if require_bound && has_unbound {
        std::process::exit(1);
    }
    Ok(())
}
