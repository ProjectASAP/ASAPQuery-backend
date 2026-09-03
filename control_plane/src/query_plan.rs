//! Authoritative backend-executable query DAG.
//!
//! ASAPPlanner owns semantic post-ASAP IR. Physical compilation binds every
//! maintained-summary leaf to one materialization and lowers edges to stable
//! node IDs. Serving executes this graph without reconstructing Planner IR or
//! searching for compatible materializations.

use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use planner_types::post_asap::{SketchQuery, SummaryExpr, SummaryFamilyType, SummaryNode};
use planner_types::pre_asap::Reduction;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use asap_types::PolicyFingerprint;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct QueryPlan {
    pub plan_id: u64,
    pub plan_version: u64,
    pub entries: BTreeMap<String, QueryPlanEntry>,
}

impl QueryPlan {
    pub fn empty() -> Self {
        Self {
            plan_id: 0,
            plan_version: 0,
            entries: BTreeMap::new(),
        }
    }

    pub fn lookup(&self, promql: &str) -> Result<&QueryPlanEntry, QueryPlanError> {
        let identity = canonical_promql(promql)?;
        self.entries
            .get(&identity)
            .ok_or(QueryPlanError::QueryNotPlanned(identity))
    }

    pub fn validate(&self, available: &BTreeSet<PolicyFingerprint>) -> Result<(), QueryPlanError> {
        if self.plan_id != 0 && self.plan_version == 0 {
            return Err(QueryPlanError::Invalid(
                "non-bootstrap QueryPlan has zero plan_version".into(),
            ));
        }
        for (identity, entry) in &self.entries {
            if identity != &entry.canonical_promql {
                return Err(QueryPlanError::Invalid(format!(
                    "query map key `{identity}` differs from entry identity `{}`",
                    entry.canonical_promql
                )));
            }
            entry.validate(available)?;
        }
        Ok(())
    }
}

/// Stable identity inside one query entry. Edges are IDs so common
/// subexpressions remain shared after serialization.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct QueryNodeId(pub u64);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct QueryPlanEntry {
    pub query_id: String,
    pub canonical_promql: String,
    pub root: QueryNodeId,
    pub nodes: BTreeMap<QueryNodeId, QueryPlanNode>,
    pub instant: InstantExecution,
    pub fallback: FallbackPolicy,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InstantExecution {
    pub lookback_ms: u64,
    pub full_history: bool,
    pub cumulative_readout: bool,
}

impl QueryPlanEntry {
    pub fn compile_bound<F>(
        query_id: String,
        canonical_promql: String,
        root: &Rc<SummaryNode>,
        instant: InstantExecution,
        fallback: FallbackPolicy,
        mut bind: F,
    ) -> Result<Self, QueryPlanError>
    where
        F: FnMut(
            &SummaryNode,
            &SummaryFamilyType,
        ) -> Result<MaterializationBinding, QueryPlanError>,
    {
        let mut compiler = DagCompiler {
            next_id: 0,
            nodes: BTreeMap::new(),
            seen: BTreeMap::new(),
            bind: &mut bind,
        };
        let root = compiler.lower(root)?;
        Ok(Self {
            query_id,
            canonical_promql,
            root,
            nodes: compiler.nodes,
            instant,
            fallback,
        })
    }

    /// Validate references, bindings, reachability, and cycles before activation.
    pub fn validate(&self, available: &BTreeSet<PolicyFingerprint>) -> Result<(), QueryPlanError> {
        if !self.nodes.contains_key(&self.root) {
            return Err(QueryPlanError::Invalid(format!(
                "query `{}` has missing root {}",
                self.query_id, self.root.0
            )));
        }
        for (id, node) in &self.nodes {
            for input in node.inputs() {
                if !self.nodes.contains_key(input) {
                    return Err(QueryPlanError::Invalid(format!(
                        "query `{}` node {} references missing input {}",
                        self.query_id, id.0, input.0
                    )));
                }
            }
            if let QueryPlanNode::ReadMaterialization { binding } = node {
                if !available.contains(&binding.materialization) {
                    return Err(QueryPlanError::Invalid(format!(
                        "query `{}` node {} references absent materialization {}",
                        self.query_id, id.0, binding.materialization.0
                    )));
                }
            }
        }
        let order = self.topological_order()?;
        if order.len() != self.nodes.len() {
            return Err(QueryPlanError::Invalid(format!(
                "query `{}` contains unreachable nodes",
                self.query_id
            )));
        }
        Ok(())
    }

    /// Return reachable nodes with every input before its consumer.
    pub fn topological_order(&self) -> Result<Vec<QueryNodeId>, QueryPlanError> {
        fn visit(
            id: QueryNodeId,
            nodes: &BTreeMap<QueryNodeId, QueryPlanNode>,
            visiting: &mut BTreeSet<QueryNodeId>,
            visited: &mut BTreeSet<QueryNodeId>,
            out: &mut Vec<QueryNodeId>,
        ) -> Result<(), QueryPlanError> {
            if visited.contains(&id) {
                return Ok(());
            }
            if !visiting.insert(id) {
                return Err(QueryPlanError::Invalid(format!(
                    "cycle detected at query node {}",
                    id.0
                )));
            }
            let node = nodes
                .get(&id)
                .ok_or_else(|| QueryPlanError::Invalid(format!("missing query node {}", id.0)))?;
            for input in node.inputs() {
                visit(*input, nodes, visiting, visited, out)?;
            }
            visiting.remove(&id);
            visited.insert(id);
            out.push(id);
            Ok(())
        }
        let mut out = Vec::with_capacity(self.nodes.len());
        visit(
            self.root,
            &self.nodes,
            &mut BTreeSet::new(),
            &mut BTreeSet::new(),
            &mut out,
        )?;
        Ok(out)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FallbackPolicy {
    ExactBackend,
    Reject,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MaterializationBinding {
    pub materialization: PolicyFingerprint,
    pub metric: String,
    /// Exact label-key layout of the stored materialization.
    pub sid_grouping: Vec<String>,
    /// Query operator grouping applied while folding those SIDs.
    pub output_grouping: PhysicalGrouping,
    pub window_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "mode", content = "keys", rename_all = "snake_case")]
pub enum PhysicalGrouping {
    PerEntity,
    Reduce(Vec<String>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum QueryPlanNode {
    ReadMaterialization {
        binding: MaterializationBinding,
    },
    SummaryEstimate {
        input: QueryNodeId,
        query: QueryReadout,
    },
    SummaryMerge {
        inputs: Vec<QueryNodeId>,
    },
    ExactFallback {
        reason: String,
    },
}

impl QueryPlanNode {
    pub fn inputs(&self) -> &[QueryNodeId] {
        match self {
            Self::ReadMaterialization { .. } | Self::ExactFallback { .. } => &[],
            Self::SummaryEstimate { input, .. } => std::slice::from_ref(input),
            Self::SummaryMerge { inputs } => inputs,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum QueryReadout {
    Quantile {
        q: f64,
    },
    PointCount {
        key: planner_types::pre_asap::ColumnRef,
        value: Option<String>,
    },
    Cardinality,
    TopK {
        k: usize,
    },
}

impl From<SketchQuery> for QueryReadout {
    fn from(query: SketchQuery) -> Self {
        match query {
            SketchQuery::Quantile { q } => Self::Quantile { q },
            SketchQuery::PointCount { key, value } => Self::PointCount { key, value },
            SketchQuery::Cardinality => Self::Cardinality,
            SketchQuery::TopK { k } => Self::TopK { k },
        }
    }
}

impl From<QueryReadout> for SketchQuery {
    fn from(query: QueryReadout) -> Self {
        match query {
            QueryReadout::Quantile { q } => Self::Quantile { q },
            QueryReadout::PointCount { key, value } => Self::PointCount { key, value },
            QueryReadout::Cardinality => Self::Cardinality,
            QueryReadout::TopK { k } => Self::TopK { k },
        }
    }
}

struct DagCompiler<'a, F> {
    next_id: u64,
    nodes: BTreeMap<QueryNodeId, QueryPlanNode>,
    seen: BTreeMap<usize, QueryNodeId>,
    bind: &'a mut F,
}

impl<F> DagCompiler<'_, F>
where
    F: FnMut(&SummaryNode, &SummaryFamilyType) -> Result<MaterializationBinding, QueryPlanError>,
{
    fn lower(&mut self, node: &Rc<SummaryNode>) -> Result<QueryNodeId, QueryPlanError> {
        let identity = Rc::as_ptr(node) as usize;
        if let Some(id) = self.seen.get(&identity) {
            return Ok(*id);
        }
        let id = QueryNodeId(self.next_id);
        self.next_id += 1;
        self.seen.insert(identity, id);
        let physical = match &node.expr {
            SummaryExpr::KeepPreAsap(_) => QueryPlanNode::ExactFallback {
                reason: "post-ASAP node requires exact execution".into(),
            },
            SummaryExpr::SummaryAgg {
                family,
                reduction,
                child,
                ..
            } => {
                if !matches!(
                    family,
                    SummaryFamilyType::ExactAggregate(..) | SummaryFamilyType::Sketch(..)
                ) {
                    return Err(QueryPlanError::UnsupportedNode(format!(
                        "summary family {family:?}"
                    )));
                }
                let mut binding = (self.bind)(node, family)?;
                binding.output_grouping = physical_grouping(reduction, child)?;
                QueryPlanNode::ReadMaterialization { binding }
            }
            SummaryExpr::SummaryEstimate {
                summary_input,
                query,
            } => QueryPlanNode::SummaryEstimate {
                input: self.lower(summary_input)?,
                query: query.clone().into(),
            },
            SummaryExpr::SummaryMerge { children } => {
                if children.is_empty() {
                    return Err(QueryPlanError::UnsupportedNode(
                        "empty summary_merge".into(),
                    ));
                }
                QueryPlanNode::SummaryMerge {
                    inputs: children
                        .iter()
                        .map(|child| self.lower(child))
                        .collect::<Result<_, _>>()?,
                }
            }
            SummaryExpr::SummaryJoin { .. } => {
                return Err(QueryPlanError::UnsupportedNode("summary_join".into()))
            }
            SummaryExpr::SummarySubtract { .. } => {
                return Err(QueryPlanError::UnsupportedNode("summary_subtract".into()))
            }
            SummaryExpr::SummaryDelete { .. } => {
                return Err(QueryPlanError::UnsupportedNode("summary_delete".into()))
            }
        };
        self.nodes.insert(id, physical);
        Ok(id)
    }
}

fn physical_grouping(
    reduction: &Reduction,
    child: &SummaryNode,
) -> Result<PhysicalGrouping, QueryPlanError> {
    let Some(keys) = reduction.group_keys() else {
        return Ok(PhysicalGrouping::PerEntity);
    };
    let names = keys
        .keys()
        .iter()
        .map(|&id| {
            child
                .schema
                .fields
                .get(id)
                .map(|f| f.name.clone())
                .ok_or_else(|| QueryPlanError::Invalid(format!("unresolved grouping column {id}")))
        })
        .collect::<Result<_, _>>()?;
    Ok(PhysicalGrouping::Reduce(names))
}

#[derive(Debug, Error)]
pub enum QueryPlanError {
    #[error("invalid PromQL query identity: {0}")]
    InvalidPromql(String),
    #[error("query is absent from the active QueryPlan: {0}")]
    QueryNotPlanned(String),
    #[error("post-ASAP DAG cannot be represented by the query executor: {0}")]
    UnsupportedNode(String),
    #[error("invalid QueryPlan: {0}")]
    Invalid(String),
}

pub fn canonical_promql(query: &str) -> Result<String, QueryPlanError> {
    promql_parser::parser::parse(query.trim())
        .map(|expr| expr.to_string())
        .map_err(|error| QueryPlanError::InvalidPromql(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_identity_ignores_formatting() {
        assert_eq!(
            canonical_promql("sum by (service) ( rate(http_requests_total[5m]) )").unwrap(),
            canonical_promql("sum by(service)(rate(http_requests_total[5m]))").unwrap()
        );
    }

    #[test]
    fn graph_validation_rejects_cycles() {
        let mut nodes = BTreeMap::new();
        nodes.insert(
            QueryNodeId(0),
            QueryPlanNode::SummaryMerge {
                inputs: vec![QueryNodeId(0)],
            },
        );
        let entry = QueryPlanEntry {
            query_id: "q".into(),
            canonical_promql: "up".into(),
            root: QueryNodeId(0),
            nodes,
            instant: InstantExecution {
                lookback_ms: 0,
                full_history: false,
                cumulative_readout: false,
            },
            fallback: FallbackPolicy::Reject,
        };
        assert!(entry
            .validate(&BTreeSet::new())
            .unwrap_err()
            .to_string()
            .contains("cycle"));
    }
}
