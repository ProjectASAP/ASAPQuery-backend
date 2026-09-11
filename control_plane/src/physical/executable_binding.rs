//! Compiler checks relating the shared executable contract to QueryPlan.

pub use asap_types::executable_plan::*;

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
