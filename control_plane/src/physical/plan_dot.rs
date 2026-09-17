//! Graphviz inspection rendering for one selected physical plan.
//!
//! This is intentionally a developer-facing view: JSON remains the complete
//! representation, while DOT keeps labels compact enough to follow execution.

use super::compiler::CompiledPhysicalPlan;
use crate::query_plan::QueryPlanNode;
use asap_types::query_plan::residual::ResidualQueryOperator;

/// Render the selected precompute and query execution DAGs as deterministic DOT.
pub fn render(plan: &CompiledPhysicalPlan) -> String {
    let mut dot = String::from(
        "digraph compiled_physical_plan {\n  rankdir=LR;\n  node [shape=box, fontname=Helvetica];\n",
    );

    dot.push_str("  subgraph cluster_precompute {\n    label=\"PrecomputePlan\";\n");
    for materialization in &plan.precompute_plan.materializations {
        let id = materialization.policy_fingerprint();
        let node = materialization_node(id.as_u64());
        let label = format!(
            "materialization\n{}\nmetric={}\nwindow={}s / {}s",
            id, materialization.metric, materialization.window_size, materialization.slide_interval,
        );
        emit_node(&mut dot, &node, &label, "shape=component");
    }
    for (dag_index, (query_id, installed)) in
        plan.precompute_plan.executable_dags.iter().enumerate()
    {
        dot.push_str(&format!(
            "    subgraph cluster_precompute_dag_{dag_index} {{\n      label=\"{}\";\n",
            escape(query_id)
        ));
        for node in &installed.document.nodes {
            let id = precompute_node(dag_index, node.id.0);
            let binding = installed
                .binding
                .nodes
                .get(&node.id)
                .map(|binding| format!("\n{:?}", binding))
                .unwrap_or_default();
            emit_node(
                &mut dot,
                &id,
                &format!("{:?} #{}{}", node.operator, node.id.0, binding),
                "",
            );
        }
        for edge in &installed.document.edges {
            dot.push_str(&format!(
                "      {} -> {} [label=\"{:?}\"];\n",
                precompute_node(dag_index, edge.producer.0),
                precompute_node(dag_index, edge.consumer.0),
                edge.role
            ));
        }
        dot.push_str("    }\n");
    }
    dot.push_str("  }\n");

    for (query_index, (query_key, entry)) in plan.query_plan.entries.iter().enumerate() {
        dot.push_str(&format!(
            "  subgraph cluster_query_{query_index} {{\n    label=\"QueryPlan: {}\";\n",
            escape(query_key)
        ));
        for (id, node) in &entry.nodes {
            let attributes = if *id == entry.root {
                "shape=doubleoctagon"
            } else {
                ""
            };
            emit_node(
                &mut dot,
                &query_node(query_index, id.0),
                &query_node_label(node),
                attributes,
            );
            if let QueryPlanNode::ReadMaterialization { binding } = node {
                dot.push_str(&format!(
                    "    {} -> {} [style=dashed, color=gray40, label=\"reads\"];\n",
                    materialization_node(binding.materialization.as_u64()),
                    query_node(query_index, id.0),
                ));
            }
        }
        for (id, node) in &entry.nodes {
            for input in node.inputs() {
                dot.push_str(&format!(
                    "    {} -> {};\n",
                    query_node(query_index, input.0),
                    query_node(query_index, id.0),
                ));
            }
        }
        dot.push_str("  }\n");
    }
    dot.push_str("}\n");
    dot
}

fn materialization_node(id: u64) -> String {
    format!("materialization_{id:016x}")
}

fn precompute_node(dag: usize, id: u32) -> String {
    format!("precompute_{dag}_{id}")
}

fn query_node(query: usize, id: u64) -> String {
    format!("query_{query}_{id}")
}

fn emit_node(dot: &mut String, id: &str, label: &str, attributes: &str) {
    dot.push_str(&format!("    {id} [label=\"{}\"", escape(label)));
    if !attributes.is_empty() {
        dot.push_str(&format!(", {attributes}"));
    }
    dot.push_str("];\n");
}

fn escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn query_node_label(node: &QueryPlanNode) -> String {
    match node {
        QueryPlanNode::RelationalJoin { .. } => "RelationalJoin".into(),
        QueryPlanNode::Relational { .. } => "Relational".into(),
        QueryPlanNode::Logical { operator, .. } => format!("Logical\n{}", residual_label(operator)),
        QueryPlanNode::Scalar { value } => format!("Scalar\n{value}"),
        QueryPlanNode::Binary { operator, .. } => format!("Binary\n{operator:?}"),
        QueryPlanNode::ReduceSum { .. } => "ReduceSum".into(),
        QueryPlanNode::ReadMaterialization { binding } => format!(
            "ReadMaterialization\n{}\nwindow={}ms\nlookback={:?}",
            binding.materialization.fingerprint(),
            binding.window_ms,
            binding.readout_lookback_ms
        ),
        QueryPlanNode::SummaryEstimate { query, .. } => format!("SummaryEstimate\n{query:?}"),
        QueryPlanNode::ExactReadout { readout, .. } => format!("ExactReadout\n{readout:?}"),
        QueryPlanNode::SummaryMerge { .. } => "SummaryMerge".into(),
        QueryPlanNode::CandidateTopK { k, .. } => format!("CandidateTopK\nk={k}"),
        QueryPlanNode::ExternalExact { .. } => "ExternalExact".into(),
        QueryPlanNode::ExactFallback { reason } => format!("ExactFallback\n{reason}"),
    }
}

fn residual_label(operator: &ResidualQueryOperator) -> &'static str {
    match operator {
        ResidualQueryOperator::CurrentSeries { .. } => "CurrentSeries",
        ResidualQueryOperator::ExactSubquery { .. } => "ExactSubquery",
        ResidualQueryOperator::CandidateExactSubquery { .. } => "CandidateExactSubquery",
        ResidualQueryOperator::Scan { .. } => "Scan",
        ResidualQueryOperator::UnaryNegate => "UnaryNegate",
        ResidualQueryOperator::VectorToScalar => "VectorToScalar",
        ResidualQueryOperator::Aggregate { .. } => "Aggregate",
        ResidualQueryOperator::TopKSelection { .. } => "TopKSelection",
        ResidualQueryOperator::Binary { .. } => "Binary",
        ResidualQueryOperator::Temporal { .. } => "Temporal",
        ResidualQueryOperator::Sort { .. } => "Sort",
        ResidualQueryOperator::HistogramQuantile => "HistogramQuantile",
        ResidualQueryOperator::Subquery { .. } => "Subquery",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::compiler::{BackendLocalPlanningInput, QueryFrontend};

    fn fixture() -> CompiledPhysicalPlan {
        let snapshot: BackendLocalPlanningInput = serde_json::from_str(include_str!(
            "../../../docs/examples/asapquery-planning-snapshot.json"
        ))
        .unwrap();
        crate::physical::compiler::tests::quoted_snapshot(snapshot, QueryFrontend::PromQl)
            .compile_promql()
            .unwrap()
    }

    #[test]
    fn render_links_query_reads_to_precompute_materializations() {
        let dot = render(&fixture());
        assert!(dot.contains("cluster_precompute"), "{dot}");
        assert!(dot.contains("cluster_query_"), "{dot}");
        assert!(dot.contains("style=dashed"), "{dot}");
        assert!(dot.contains("ReadMaterialization"), "{dot}");
    }

    #[test]
    fn render_uses_graphviz_line_breaks_in_labels() {
        let dot = render(&fixture());
        assert!(
            dot.contains("materialization\\npolicy_fp:"),
            "expected a Graphviz line break, not a literal backslash-n: {dot}"
        );
        assert!(
            !dot.contains("materialization\\\\npolicy_fp:"),
            "label double-escaped its Graphviz line break: {dot}"
        );
    }

    #[test]
    fn compiled_physical_plan_serializes_each_projection() {
        let artifact = serde_json::to_value(fixture()).unwrap();
        for field in [
            "envelope",
            "summary_catalog",
            "collector_plans",
            "precompute_plan",
            "transmission_plan",
            "query_plan",
            "storage_routing",
            "lifecycle_estimates",
            "cost_comparison",
            "planner_selection_trace",
        ] {
            assert!(
                artifact.get(field).is_some(),
                "missing `{field}`: {artifact}"
            );
        }
    }
}
