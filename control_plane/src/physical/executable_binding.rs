//! Compiler checks relating the shared executable contract to QueryPlan.

pub use asap_types::executable_plan::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OperatorExecution {
    Ingestion,
    Query,
}

/// Every physical operator uses its node's placement; payload kind does not
/// restrict execution phase. Runtime capability is checked separately.
fn operator_execution(node: &planner_types::post_asap::ExecutableDagNode) -> OperatorExecution {
    match node.output_state.timing {
        planner_types::post_asap::ExecutionTiming::IngestionTime => OperatorExecution::Ingestion,
        planner_types::post_asap::ExecutionTiming::QueryTime => OperatorExecution::Query,
    }
}

/// Assign backend phases to a selected semantic DAG without changing its nodes.
pub fn install_selected_dag(
    query_id: String,
    dag: &planner_types::post_asap::ExecutableDag,
    query_plan_sink: QueryNodeId,
    materialization: impl Fn(
        planner_types::post_asap::PostAsapNodeId,
    ) -> Option<asap_types::sds::StoredOutputId>,
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
        let execution = operator_execution(node);
        let binding = if let Some(stored_output) = materialization(node.id) {
            precompute_sinks.push(node.id);
            BackendNodeBinding::Materialization { stored_output }
        } else if execution == OperatorExecution::Ingestion {
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
        native_programs: std::collections::BTreeMap::new(),
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

/// Retain Planner's complete state-to-state graph before candidate pricing.
pub(super) fn compile_precompute_programs(
    installed: &mut asap_types::executable_plan::InstalledPostAsapDag,
    configs: &[asap_types::PrecomputeMaterialization],
) -> Result<(), String> {
    use asap_types::executable_plan::BackendNodeBinding;
    let dag = installed.document.decode()?;
    let mut groups = std::collections::BTreeMap::<_, Vec<u64>>::new();
    for sink in &installed.binding.precompute_sinks {
        if installed.native_programs.contains_key(sink) {
            continue;
        }
        let Some(BackendNodeBinding::Materialization { stored_output }) =
            installed.binding.node(*sink)
        else {
            continue;
        };
        let config = configs
            .iter()
            .find(|c| c.policy_fingerprint() == stored_output.fingerprint())
            .ok_or("precompute output configuration missing")?;
        let Some(derived) = &config.derived_input else {
            continue;
        };
        let frontiers = installed.binding.nodes.iter().filter_map(|(id, binding)| {
            matches!(binding, BackendNodeBinding::Materialization { stored_output } if derived.inputs.contains(stored_output)).then_some(u64::from(id.0))
        }).collect::<Vec<_>>();
        // Only co-schedule outputs with identical immutable input and window
        // contracts. Legacy population bindings are kept as individual runs.
        let separate = config.population_key_encoding.is_legacy().then_some(sink.0);
        groups
            .entry((
                frontiers,
                config.stored_window_ms(),
                config.slide_interval,
                config.pane_origin_ms,
                separate,
            ))
            .or_default()
            .push(u64::from(sink.0));
    }
    for ((frontiers, _, _, _, _), roots) in groups {
        let program = asap_physical_operators::physical_planner::precompute::compile(
            &dag, &frontiers, &roots,
        )
        .map_err(|e| e.to_string())?;
        let encoded: serde_json::Value =
            serde_json::from_slice(&program.encode().map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
        for root in roots {
            installed.native_programs.insert(
                planner_types::post_asap::PostAsapNodeId(
                    u32::try_from(root).map_err(|_| "physical output id overflow")?,
                ),
                encoded.clone(),
            );
        }
    }
    Ok(())
}
