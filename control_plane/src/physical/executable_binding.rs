//! Compiler checks relating the shared executable contract to QueryPlan.

pub use asap_types::executable_plan::*;

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
        let binding = if let Some(summary_definition) = materialization(node.id) {
            precompute_sinks.push(node.id);
            BackendNodeBinding::Materialization { summary_definition }
        } else if node.output_state.timing
            == planner_types::post_asap::ExecutionTiming::IngestionTime
        {
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
