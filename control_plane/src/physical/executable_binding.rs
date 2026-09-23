//! Compiler checks relating the shared executable contract to QueryPlan.

pub use asap_types::executable_plan::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OperatorExecution {
    Maintenance,
    Query,
}

/// Keep the backend's ownership decision exhaustive over Planner's physical IR.
/// Adding a payload variant upstream must therefore choose an executor here.
fn operator_execution(
    node: &planner_types::post_asap::ExecutableDagNode,
) -> Result<OperatorExecution, String> {
    use planner_types::post_asap::{ExecutableOperatorPayload as Payload, ExecutionTiming};

    let declared = match &node.payload {
        Payload::Binary { timing, .. } | Payload::Value { timing, .. } => *timing,
        Payload::CandidateTopK { .. } | Payload::SummaryEstimate { .. } => {
            ExecutionTiming::ReadTime
        }
        Payload::SummaryAgg { .. }
        | Payload::SummaryJoin { .. }
        | Payload::SummarySubtract
        | Payload::SummaryDelete { .. }
        | Payload::SummaryMerge => ExecutionTiming::MaintenanceTime,
        // These operators can be placed on either side of the stored-state
        // boundary. Planner's validated output state is authoritative.
        Payload::Fallback { .. } | Payload::RelationalJoin { .. } => node.output_state.timing,
    };
    if declared != node.output_state.timing {
        return Err(format!(
            "post-ASAP node {:?} has operator timing {} but output state {}",
            node.id,
            declared.as_str(),
            node.output_state
        ));
    }
    Ok(match declared {
        ExecutionTiming::MaintenanceTime => OperatorExecution::Maintenance,
        ExecutionTiming::ReadTime => OperatorExecution::Query,
    })
}

/// Assign backend phases to a selected semantic DAG without changing its nodes.
pub fn install_selected_dag(
    query_id: String,
    dag: &planner_types::post_asap::ExecutableDag,
    query_plan_sink: QueryNodeId,
    materialization: impl Fn(
        planner_types::post_asap::PostAsapNodeId,
    ) -> Option<asap_types::sds::SummaryDefinitionId>,
    query_node: impl Fn(planner_types::post_asap::PostAsapNodeId) -> Option<QueryNodeId>,
) -> Result<InstalledPostAsapDag, String> {
    let mut nodes = std::collections::BTreeMap::new();
    let mut precompute_sinks = Vec::new();
    for node in &dag.nodes {
        if let planner_types::post_asap::ExecutableOperatorPayload::SummaryAgg {
            family,
            input,
            grouping,
            ..
        } = &node.payload
        {
            asap_physical_operators::capability::validate_summary_kernel(family, input, grouping)
                .map_err(|reason| format!("post-ASAP node {:?}: {reason}", node.id))?;
        }
        let execution = operator_execution(node)?;
        let binding = if let Some(summary_definition) = materialization(node.id) {
            precompute_sinks.push(node.id);
            BackendNodeBinding::Materialization { summary_definition }
        } else if execution == OperatorExecution::Maintenance {
            BackendNodeBinding::MaintenanceInput
        } else {
            query_node(node.id).map_or(BackendNodeBinding::QueryInput, |query_node| {
                BackendNodeBinding::Query { query_node }
            })
        };
        nodes.insert(node.id, binding);
    }
    precompute_sinks.sort();
    let installed = InstalledPostAsapDag {
        document: OwnedPostAsapDag::from_executable(query_id, dag)?,
        binding: BackendExecutableBinding {
            nodes,
            query_sink: dag.root,
            query_plan_sink,
            precompute_sinks,
        },
    };
    installed.binding.validate(&installed.document.decode()?)?;
    Ok(installed)
}

pub fn validate_query_plan(
    installed: &InstalledPostAsapDag,
    query: &crate::query_plan::QueryPlanEntry,
) -> Result<(), String> {
    if query.query_id != installed.document.query_id {
        return Err("post-ASAP document and query plan identity disagree".into());
    }
    if query.root != installed.binding.query_plan_sink {
        return Err("backend query-plan sink disagrees with installed query root".into());
    }
    for placement in installed.binding.nodes.values() {
        if let BackendNodeBinding::Query { query_node } = placement {
            if !query.nodes.contains_key(query_node) {
                return Err(format!(
                    "backend binding references missing query node {}",
                    query_node.0
                ));
            }
        }
    }
    Ok(())
}
