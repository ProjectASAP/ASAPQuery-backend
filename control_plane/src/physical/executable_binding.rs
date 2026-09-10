use std::collections::{BTreeMap, BTreeSet};

use asap_types::sds::SummaryDefinitionId;
use planner_types::post_asap::{
    EdgeRole, ExecutableDag, ExecutableDagEdge, ExecutableDagNode, ExecutableOperator,
    ExecutionDataState, ExecutionTiming, GroupingEdgeCompatibility, PostAsapNodeId,
    WindowEdgeCompatibility,
};
use serde::{Deserialize, Serialize};

use crate::query_plan::{QueryNodeId, QueryPlanEntry};

pub const POST_ASAP_DAG_DOCUMENT_SCHEMA_VERSION: u32 = 1;

/// Versioned, language-neutral Planner DAG persisted with an installed plan.
/// Plan lifecycle belongs to the enclosing `PrecomputePlan`; this document
/// carries semantic identity only and does not duplicate its envelope.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PostAsapDagDocument {
    pub schema_version: u32,
    pub query_id: String,
    pub nodes: Vec<PostAsapDagNodeDocument>,
    pub edges: Vec<PostAsapDagEdgeDocument>,
    pub root: PostAsapNodeId,
}

/// Send/Sync wire form of one Planner node. Planner's in-memory payload may
/// contain `Rc`; the canonical JSON payload preserves its tagged type without
/// leaking that process-local ownership choice into installed runtime state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PostAsapDagNodeDocument {
    pub id: PostAsapNodeId,
    pub operator: ExecutableOperator,
    pub payload: serde_json::Value,
    pub output_state: ExecutionDataState,
    pub output_schema: serde_json::Value,
    pub guarantee: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PostAsapDagEdgeDocument {
    pub producer: PostAsapNodeId,
    pub consumer: PostAsapNodeId,
    pub role: EdgeRole,
    pub intermediate_schema: serde_json::Value,
    pub data_state: ExecutionDataState,
    pub grouping: GroupingEdgeCompatibility,
    pub window: WindowEdgeCompatibility,
}

impl PostAsapDagDocument {
    pub fn from_executable(query_id: String, dag: &ExecutableDag) -> Result<Self, String> {
        let nodes = dag
            .nodes
            .iter()
            .map(|node| {
                Ok(PostAsapDagNodeDocument {
                    id: node.id,
                    operator: node.operator,
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
                Ok(PostAsapDagEdgeDocument {
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
            schema_version: POST_ASAP_DAG_DOCUMENT_SCHEMA_VERSION,
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
                if payload.operator() != node.operator {
                    return Err(format!(
                        "post-ASAP node {} operator disagrees with payload",
                        node.id.0
                    ));
                }
                Ok(ExecutableDagNode {
                    id: node.id,
                    operator: node.operator,
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
    pub document: PostAsapDagDocument,
    pub binding: BackendExecutableBinding,
}

impl InstalledPostAsapDag {
    pub fn validate(&self) -> Result<(), String> {
        if self.document.schema_version != POST_ASAP_DAG_DOCUMENT_SCHEMA_VERSION
            || self.document.query_id.trim().is_empty()
        {
            return Err("invalid post-ASAP DAG document identity/version".into());
        }
        self.binding.validate(&self.document.decode()?)
    }

    pub fn validate_query_plan(&self, query: &QueryPlanEntry) -> Result<(), String> {
        if query.query_id != self.document.query_id {
            return Err("post-ASAP document and query plan identity disagree".into());
        }
        if query.root != self.binding.query_plan_sink {
            return Err("backend query-plan sink disagrees with installed query root".into());
        }
        for placement in self.binding.nodes.values() {
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
