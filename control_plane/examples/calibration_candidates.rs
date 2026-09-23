//! Export every bindable candidate for isolated measurement, without selecting a winner.
use control_plane::physical::{
    compiler::{BackendLocalPlanningInput, PhysicalPlanCompiler},
    workload_cost,
};
use planner_types::post_asap::{SummaryExpr, SummaryNode};
use serde_json::{json, Value};
use std::{collections::BTreeMap, rc::Rc};

// Planner IR does not implement Serialize. Preserve actual DAG identity and
// typed variant/edges; leaf metadata uses explicitly labelled Debug encoding.
fn planner_forest(queries: &[control_plane::physical::compiler::QueryCompilationInput]) -> Value {
    fn visit(
        node: &Rc<SummaryNode>,
        seen: &mut BTreeMap<usize, usize>,
        nodes: &mut BTreeMap<usize, Value>,
    ) -> usize {
        let pointer = Rc::as_ptr(node) as usize;
        if let Some(id) = seen.get(&pointer) {
            return *id;
        }
        let id = seen.len();
        seen.insert(pointer, id);
        let (kind, children, detail): (&str, Vec<&Rc<SummaryNode>>, Value) = match &node.expr {
            SummaryExpr::KeepPreAsap(expr) => (
                "KeepPreAsap",
                vec![],
                json!({"query_expr_debug":format!("{expr:#?}")}),
            ),
            SummaryExpr::BinaryOp {
                lhs,
                rhs,
                operator,
                timing,
            } => (
                "BinaryOp",
                vec![lhs, rhs],
                json!({"operator_debug":format!("{operator:?}"),"timing_debug":format!("{timing:?}")}),
            ),

            SummaryExpr::ValueOperation {
                child,
                operation,
                timing,
            } => (
                "ValueOperation",
                vec![child],
                json!({"operation_debug":format!("{operation:?}"),"timing_debug":format!("{timing:?}")}),
            ),
            SummaryExpr::SummaryAgg {
                child,
                family,
                reduction,
                grouping,
                input,
                ..
            } => (
                "SummaryAgg",
                vec![child],
                json!({"family_debug":format!("{family:?}"),"reduction_debug":format!("{reduction:?}"),"grouping_debug":format!("{grouping:?}"),"input_debug":format!("{input:?}")}),
            ),
            SummaryExpr::SummaryJoin {
                outer,
                inner,
                key,
                family,
            } => (
                "SummaryJoin",
                vec![outer, inner],
                json!({"key_debug":format!("{key:?}"),"family_debug":format!("{family:?}")}),
            ),
            SummaryExpr::RelationalJoin {
                left,
                right,
                kind,
                pred,
                ..
            } => (
                "RelationalJoin",
                vec![left, right],
                json!({"kind_debug":format!("{kind:?}"),"predicate_debug":format!("{pred:?}")}),
            ),
            SummaryExpr::SummarySubtract { left, right } => {
                ("SummarySubtract", vec![left, right], json!({}))
            }
            SummaryExpr::SummaryDelete { summary_input, key } => (
                "SummaryDelete",
                vec![summary_input],
                json!({"key_debug":format!("{key:?}")}),
            ),
            SummaryExpr::SummaryEstimate {
                summary_input,
                query,
            } => (
                "SummaryEstimate",
                vec![summary_input],
                json!({"query_debug":format!("{query:?}")}),
            ),
            SummaryExpr::SummaryMerge { children, .. } => {
                ("SummaryMerge", children.iter().collect(), json!({}))
            }
        };
        let inputs: Vec<_> = children
            .into_iter()
            .map(|child| visit(child, seen, nodes))
            .collect();
        nodes.insert(id,json!({"kind":kind,"inputs":inputs,"detail":detail,"schema_debug":format!("{:?}",node.schema),"guarantee_debug":format!("{:?}",node.guarantee)}));
        id
    }
    let mut seen = BTreeMap::new();
    let mut nodes = BTreeMap::new();
    let roots:Vec<_>=queries.iter().map(|q|json!({"query_id":q.query_id,"original_promql":q.query_string,"root":visit(&q.selected_plan_root,&mut seen,&mut nodes)})).collect();
    json!({"encoding":"structured_graph_with_debug_metadata_v1","scope":"actual candidate Planner post-ASAP input before physical binding; not reconstructed from installed nodes","roots":roots,"nodes":nodes})
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: calibration_candidates SNAPSHOT.json [--metricsql]")?;
    let metricsql = std::env::args().skip(2).any(|arg| arg == "--metricsql");
    let snapshot: BackendLocalPlanningInput = serde_json::from_slice(&std::fs::read(path)?)?;
    let (request, environment) = snapshot.into_physical_compilation_request()?;
    let mut results = Vec::new();
    for (index, candidate) in workload_cost::enumerate_exact_and_materialized_candidates(request)?
        .into_iter()
        .enumerate()
    {
        let queries = candidate.queries.clone();
        let enabled_materialization_keys = candidate.enabled_materialization_keys.clone();
        let planner_selected_queries = planner_forest(&queries);
        let compiled = if metricsql {
            PhysicalPlanCompiler.compile_metricsql(candidate, environment.clone())
        } else {
            PhysicalPlanCompiler.compile_promql(candidate, environment.clone())
        };
        let plan = match compiled {
            Ok(plan) => plan,
            Err(error) => {
                results.push(
                    json!({"candidate_index": index, "materialization_policy": enabled_materialization_keys, "planner_selected_queries": planner_selected_queries, "unavailable_reason": error.to_string()}),
                );
                continue;
            }
        };
        let manifest = match workload_cost::manifest(&plan, &queries) {
            Ok(manifest) => manifest,
            Err(error) => {
                results.push(
                    json!({"candidate_index": index, "materialization_policy": enabled_materialization_keys, "planner_selected_queries": planner_selected_queries, "unavailable_reason": error.to_string()}),
                );
                continue;
            }
        };
        results.push(json!({
            "candidate_index": index,
            "materialization_policy": enabled_materialization_keys,
            "planner_selected_queries": planner_selected_queries,
            "manifest": manifest,
            "lifecycle_estimates": plan.lifecycle_estimates,
            "install_request": {
                "summary_catalog": plan.summary_catalog,
                "collector_plans": plan.collector_plans,
                "precompute_plan": plan.precompute_plan,
                "transmission_plan": plan.transmission_plan,
                "query_plan": plan.query_plan,
                "storage_routing": null,
                "adaptation_evidence": []
            }
        }));
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "purpose":"calibration_only",
            "compiler_identity": {
                "backend_revision": control_plane::physical::compiler::BACKEND_REVISION,
                "planner_revision": control_plane::physical::compiler::PLANNER_REVISION
            },
            "candidates":results
        }))?
    );
    Ok(())
}
