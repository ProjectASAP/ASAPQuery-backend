//! Shared backend installation contract for semantic DAGs and physical bindings.
//!
//! `OwnedPostAsapDag` is the Send/Sync installed representation of Planner IR;
//! it is distinct from Planner's `PostAsapDagDocument` transport envelope.
//! Planner payloads contain process-local `Rc` values, so they are decoded only
//! when executing or validating a DAG. Compilation and QueryPlan cross-checks
//! remain control-plane responsibilities.

use std::collections::{BTreeMap, BTreeSet};

use crate::sds::SummaryDefinitionId;
use planner_types::post_asap::{
    EdgeRole, ExecutableDag, ExecutableDagEdge, ExecutableDagNode, ExecutionDataState,
    ExecutionTiming, GroupingEdgeCompatibility, PostAsapNodeId, WindowEdgeCompatibility,
};
use serde::{Deserialize, Serialize};

/// Identity within an installed query entry, separate from semantic Planner IDs.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct QueryNodeId(pub u64);

pub const OWNED_POST_ASAP_DAG_SCHEMA_VERSION: u32 = 2;
pub const MAINTENANCE_DAG_SCHEMA_VERSION: u32 = 2;

/// Versioned, language-neutral Planner DAG persisted with an installed plan.
/// Plan lifecycle belongs to the enclosing `PrecomputePlan`; this document
/// carries semantic identity only and does not duplicate its envelope.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct OwnedPostAsapDag {
    pub schema_version: u32,
    pub query_id: String,
    pub nodes: Vec<OwnedPostAsapNode>,
    pub edges: Vec<OwnedPostAsapEdge>,
    pub root: PostAsapNodeId,
}

/// Send/Sync wire form of one Planner node. Planner's in-memory payload may
/// contain `Rc`; the canonical JSON payload preserves its tagged type without
/// leaking that process-local ownership choice into installed runtime state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct OwnedPostAsapNode {
    pub id: PostAsapNodeId,
    pub payload: serde_json::Value,
    pub output_state: ExecutionDataState,
    pub output_schema: serde_json::Value,
    pub guarantee: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct OwnedPostAsapEdge {
    pub producer: PostAsapNodeId,
    pub consumer: PostAsapNodeId,
    pub role: EdgeRole,
    pub intermediate_schema: serde_json::Value,
    pub data_state: ExecutionDataState,
    pub grouping: GroupingEdgeCompatibility,
    pub window: WindowEdgeCompatibility,
}

impl OwnedPostAsapDag {
    pub fn from_executable(query_id: String, dag: &ExecutableDag) -> Result<Self, String> {
        let nodes = dag
            .nodes
            .iter()
            .map(|node| {
                Ok(OwnedPostAsapNode {
                    id: node.id,
                    payload: serde_json::to_value(&node.payload).map_err(|e| e.to_string())?,
                    output_state: node.output_state,
                    output_schema: serde_json::to_value(&node.output_schema)
                        .map_err(|e| e.to_string())?,
                    guarantee: node
                        .guarantee
                        .as_ref()
                        .map(serde_json::to_value)
                        .transpose()
                        .map_err(|e| e.to_string())?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        let edges = dag
            .edges
            .iter()
            .map(|edge| {
                Ok(OwnedPostAsapEdge {
                    producer: edge.producer,
                    consumer: edge.consumer,
                    role: edge.role,
                    intermediate_schema: serde_json::to_value(&edge.intermediate_schema)
                        .map_err(|e| e.to_string())?,
                    data_state: edge.data_state,
                    grouping: edge.grouping,
                    window: edge.window,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(Self {
            schema_version: OWNED_POST_ASAP_DAG_SCHEMA_VERSION,
            query_id,
            nodes,
            edges,
            root: dag.root,
        })
    }

    pub fn decode(&self) -> Result<ExecutableDag, String> {
        let node_ids = self
            .nodes
            .iter()
            .map(|node| node.id)
            .collect::<BTreeSet<_>>();
        if node_ids.len() != self.nodes.len() {
            return Err("duplicate post-ASAP DAG node ID".into());
        }
        let edge_ids = self
            .edges
            .iter()
            .map(|edge| {
                (
                    edge.producer,
                    edge.consumer,
                    serde_json::to_string(&edge.role).expect("EdgeRole is serializable"),
                )
            })
            .collect::<BTreeSet<_>>();
        if edge_ids.len() != self.edges.len() {
            return Err("duplicate post-ASAP DAG edge".into());
        }
        let nodes = self
            .nodes
            .iter()
            .map(|node| {
                let payload: planner_types::post_asap::ExecutableOperatorPayload =
                    serde_json::from_value(node.payload.clone()).map_err(|e| e.to_string())?;
                Ok(ExecutableDagNode {
                    id: node.id,
                    payload,
                    output_state: node.output_state,
                    output_schema: serde_json::from_value(node.output_schema.clone())
                        .map_err(|e| e.to_string())?,
                    guarantee: node
                        .guarantee
                        .clone()
                        .map(serde_json::from_value)
                        .transpose()
                        .map_err(|e| e.to_string())?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        let edges = self
            .edges
            .iter()
            .map(|edge| {
                Ok(ExecutableDagEdge {
                    producer: edge.producer,
                    consumer: edge.consumer,
                    role: edge.role,
                    intermediate_schema: serde_json::from_value(edge.intermediate_schema.clone())
                        .map_err(|e| e.to_string())?,
                    data_state: edge.data_state,
                    grouping: edge.grouping,
                    window: edge.window,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(ExecutableDag {
            nodes,
            edges,
            root: self.root,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct InstalledPostAsapDag {
    pub document: OwnedPostAsapDag,
    pub binding: BackendExecutableBinding,
}

impl InstalledPostAsapDag {
    pub fn validate(&self) -> Result<(), String> {
        if self.document.query_id.trim().is_empty() {
            return Err("invalid post-ASAP DAG document identity/version".into());
        }
        if self.document.schema_version != MAINTENANCE_DAG_SCHEMA_VERSION {
            return Err("unsupported maintenance DAG document version".into());
        }
        self.binding.validate_maintenance(&self.document.decode()?)
    }

    /// Project the selected semantic DAG onto the maintenance ancestors of its
    /// stored outputs.
    pub fn maintenance_projection(mut self) -> Result<Self, String> {
        if self.document.schema_version != OWNED_POST_ASAP_DAG_SCHEMA_VERSION {
            return Err("selected DAG has an unsupported document version".into());
        }
        self.binding.validate(&self.document.decode()?)?;
        if self.binding.precompute_sinks.is_empty() {
            return Err("cannot project a DAG without maintenance sinks".into());
        }
        let mut included = self
            .binding
            .precompute_sinks
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let mut frontier = self.binding.precompute_sinks.clone();
        while let Some(consumer) = frontier.pop() {
            for edge in self
                .document
                .edges
                .iter()
                .filter(|edge| edge.consumer == consumer)
            {
                if included.insert(edge.producer) {
                    frontier.push(edge.producer);
                }
            }
        }
        self.document
            .nodes
            .retain(|node| included.contains(&node.id));
        self.document
            .edges
            .retain(|edge| included.contains(&edge.producer) && included.contains(&edge.consumer));
        self.binding.nodes.retain(|id, _| included.contains(id));
        self.document.root = *self.binding.precompute_sinks.first().unwrap();
        self.document.schema_version = MAINTENANCE_DAG_SCHEMA_VERSION;
        self.validate()?;
        Ok(self)
    }
}

/// Backend-owned physical identities and sink placement for one Planner
/// semantic DAG. Planner node IDs remain stable semantic references; the
/// backend never assumes they equal its independently allocated query IDs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BackendExecutableBinding {
    pub nodes: BTreeMap<PostAsapNodeId, BackendNodeBinding>,
    pub query_sink: PostAsapNodeId,
    pub query_plan_sink: QueryNodeId,
    pub precompute_sinks: Vec<PostAsapNodeId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "placement", rename_all = "snake_case", deny_unknown_fields)]
pub enum BackendNodeBinding {
    Query {
        query_node: QueryNodeId,
    },
    /// Semantic read node absorbed into a larger installed query node, such
    /// as an external exact subtree boundary.
    QueryInput,
    MaintenanceInput,
    Materialization {
        summary_definition: SummaryDefinitionId,
    },
}

impl BackendExecutableBinding {
    pub fn node(&self, id: PostAsapNodeId) -> Option<&BackendNodeBinding> {
        self.nodes.get(&id)
    }

    pub fn validate_maintenance(&self, dag: &ExecutableDag) -> Result<(), String> {
        let ids = dag
            .nodes
            .iter()
            .map(|node| node.id)
            .collect::<BTreeSet<_>>();
        if ids.is_empty() || self.nodes.keys().copied().collect::<BTreeSet<_>>() != ids {
            return Err("maintenance binding does not cover its projected DAG".into());
        }
        if self.precompute_sinks.is_empty()
            || self.precompute_sinks.iter().any(|id| !ids.contains(id))
        {
            return Err("maintenance projection has missing sinks".into());
        }
        if self.precompute_sinks.iter().any(|id| {
            !matches!(
                self.node(*id),
                Some(BackendNodeBinding::Materialization { .. })
            )
        }) {
            return Err("maintenance sink lacks a stored-output binding".into());
        }
        for node in &dag.nodes {
            match (node.output_state.timing, self.node(node.id)) {
                (
                    ExecutionTiming::MaintenanceTime,
                    Some(
                        BackendNodeBinding::MaintenanceInput
                        | BackendNodeBinding::Materialization { .. },
                    ),
                ) => {}
                _ => {
                    return Err(format!(
                        "query-owned node {} in maintenance projection",
                        node.id.0
                    ))
                }
            }
        }
        if dag
            .edges
            .iter()
            .any(|edge| !ids.contains(&edge.producer) || !ids.contains(&edge.consumer))
        {
            return Err("maintenance projection has dangling edges".into());
        }
        Ok(())
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
                | (ExecutionTiming::ReadTime, BackendNodeBinding::QueryInput)
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

#[cfg(test)]
mod tests {
    use super::*;

    // Shared installation metadata must be safe to retain in cross-thread
    // RuntimePhysicalPlan snapshots without importing the compiler crate.
    #[test]
    fn installed_contract_is_send_sync_and_preserves_wire_identity() {
        fn send_sync<T: Send + Sync>() {}
        send_sync::<InstalledPostAsapDag>();
        let wire = serde_json::json!({
            "schema_version": 1, "query_id": "q", "nodes": [], "edges": [], "root": 0
        });
        let document: OwnedPostAsapDag = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(document).unwrap(), wire);
        assert_eq!(serde_json::to_value(QueryNodeId(9)).unwrap(), 9);
    }

    #[test]
    fn installed_maintenance_dag_rejects_complete_dag_version() {
        let installed = InstalledPostAsapDag {
            document: OwnedPostAsapDag {
                schema_version: OWNED_POST_ASAP_DAG_SCHEMA_VERSION,
                query_id: "q".into(),
                nodes: Vec::new(),
                edges: Vec::new(),
                root: PostAsapNodeId(0),
            },
            binding: BackendExecutableBinding {
                nodes: BTreeMap::new(),
                query_sink: PostAsapNodeId(0),
                query_plan_sink: QueryNodeId(0),
                precompute_sinks: Vec::new(),
            },
        };
        assert!(installed
            .validate()
            .unwrap_err()
            .contains("unsupported maintenance DAG"));
    }
}
