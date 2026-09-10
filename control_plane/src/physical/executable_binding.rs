use std::collections::{BTreeMap, BTreeSet};

use asap_types::sds::SummaryDefinitionId;
use planner_types::post_asap::{ExecutableDag, ExecutionTiming, PostAsapNodeId};
use serde::{Deserialize, Serialize};

use crate::query_plan::QueryNodeId;

/// Backend-owned physical identities and sink placement for one Planner
/// semantic DAG. Planner node IDs remain stable semantic references; the
/// backend never assumes they equal its independently allocated query IDs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BackendExecutableBinding {
    pub nodes: BTreeMap<PostAsapNodeId, BackendNodeBinding>,
    pub query_sink: PostAsapNodeId,
    pub precompute_sinks: Vec<PostAsapNodeId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "placement", rename_all = "snake_case", deny_unknown_fields)]
pub enum BackendNodeBinding {
    Query {
        query_node: QueryNodeId,
    },
    MaintenanceInput,
    Materialization {
        summary_definition: SummaryDefinitionId,
    },
}

impl BackendExecutableBinding {
    pub fn node(&self, id: PostAsapNodeId) -> Option<&BackendNodeBinding> {
        self.nodes.get(&id)
    }

    pub fn validate(&self, dag: &ExecutableDag) -> Result<(), String> {
        let semantic = dag
            .nodes
            .iter()
            .map(|node| node.id.0)
            .collect::<BTreeSet<_>>();
        if !semantic.contains(&self.query_sink.0) {
            return Err("backend query sink is absent from semantic DAG".into());
        }
        if self.nodes.keys().map(|id| id.0).collect::<BTreeSet<_>>() != semantic {
            return Err("backend executable binding does not cover semantic DAG exactly".into());
        }
        for node in &dag.nodes {
            match (node.output_state.timing, self.node(node.id).unwrap()) {
                (ExecutionTiming::ReadTime, BackendNodeBinding::Query { .. })
                | (ExecutionTiming::MaintenanceTime, BackendNodeBinding::MaintenanceInput)
                | (ExecutionTiming::MaintenanceTime, BackendNodeBinding::Materialization { .. }) => {
                }
                _ => {
                    return Err(format!(
                        "backend placement disagrees with node {} mode",
                        node.id.0
                    ))
                }
            }
        }
        if !matches!(
            self.node(self.query_sink),
            Some(BackendNodeBinding::Query { .. })
        ) {
            return Err("backend query sink is not query-placed".into());
        }
        for sink in &self.precompute_sinks {
            if !matches!(
                self.node(*sink),
                Some(BackendNodeBinding::Materialization { .. })
            ) {
                return Err(format!(
                    "backend precompute sink {} is not materialized",
                    sink.0
                ));
            }
        }
        Ok(())
    }
}
