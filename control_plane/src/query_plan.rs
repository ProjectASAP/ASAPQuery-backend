//! Authoritative backend-executable query DAG.
//!
//! ASAPPlanner owns semantic post-ASAP IR. Physical compilation binds every
//! maintained-summary leaf to one materialization and lowers edges to stable
//! node IDs. Serving executes this graph without reconstructing Planner IR or
//! searching for compatible materializations.

pub mod logical;

use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use planner_types::post_asap::{SketchQuery, SummaryExpr, SummaryFamilyType, SummaryNode};
use planner_types::pre_asap::Reduction;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use asap_types::{sds::SummaryDefinitionId, PolicyFingerprint};

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
        self.lookup_canonical(QueryLanguage::PromQl, &identity)
    }

    pub fn lookup_canonical(
        &self,
        language: QueryLanguage,
        identity: &str,
    ) -> Result<&QueryPlanEntry, QueryPlanError> {
        self.entries
            .get(identity)
            .filter(|entry| entry.language == language)
            .ok_or_else(|| QueryPlanError::QueryNotPlanned(identity.into()))
    }

    /// Validate semantic bindings against the authoritative snapshot before use.
    pub fn validate_against_catalog(
        &self,
        catalog: &crate::physical::summary_catalog::SummaryCatalog,
    ) -> Result<(), QueryPlanError> {
        catalog
            .validate()
            .map_err(|error| QueryPlanError::Invalid(error.to_string()))?;
        if self.plan_id != catalog.plan_id || self.plan_version != catalog.plan_version {
            return Err(QueryPlanError::Invalid(
                "QueryPlan and SummaryCatalog have different plan identity/version".into(),
            ));
        }
        let available = catalog
            .materializations
            .keys()
            .copied()
            .map(Into::into)
            .collect();
        self.validate(&available)?;
        for entry in self.entries.values() {
            for binding in entry.materialization_bindings() {
                let identity = catalog
                    .materializations
                    .get(&binding.materialization)
                    .ok_or_else(|| {
                        QueryPlanError::Invalid(
                            "query binding references absent catalog materialization".into(),
                        )
                    })?;
                let _data = &catalog.data_descriptors[&identity.data_descriptor_id];
                if binding.window_ms == 0 {
                    return Err(QueryPlanError::Invalid(
                        "zero physical pane duration".into(),
                    ));
                }
                if binding.pane_origin_ms != identity.pane_origin_ms {
                    return Err(QueryPlanError::Invalid(
                        "query pane origin differs from catalog definition".into(),
                    ));
                }
            }
            for node in entry.nodes.values() {
                let QueryPlanNode::ExactReadout { input, readout } = node else {
                    continue;
                };
                if !matches!(readout, ExactReadout::Increase | ExactReadout::Rate) {
                    continue;
                }
                let Some(QueryPlanNode::ReadMaterialization { binding }) = entry.nodes.get(input)
                else {
                    return Err(QueryPlanError::Invalid(
                        "counter readout must directly consume one catalog materialization".into(),
                    ));
                };
                let identity = &catalog.materializations[&binding.materialization];
                let descriptor = &catalog.summary_descriptors[&identity.summary_descriptor_id];
                if !matches!(
                    descriptor.fidelity,
                    asap_types::sds::FidelityGuarantee::ExactCounter {
                        full_pane_coverage_required: true,
                        ..
                    }
                ) {
                    return Err(QueryPlanError::Invalid(
                        "rate/increase binding does not reference an exact counter SDS".into(),
                    ));
                }
            }
        }
        Ok(())
    }

    pub fn validate(&self, available: &BTreeSet<PolicyFingerprint>) -> Result<(), QueryPlanError> {
        if self.plan_id != 0 && self.plan_version == 0 {
            return Err(QueryPlanError::Invalid(
                "non-bootstrap QueryPlan has zero plan_version".into(),
            ));
        }
        for (identity, entry) in &self.entries {
            if identity != &entry.canonical_query {
                return Err(QueryPlanError::Invalid(format!(
                    "query map key `{identity}` differs from entry identity `{}`",
                    entry.canonical_query
                )));
            }
            entry.validate(available)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QueryLanguage {
    #[default]
    PromQl,
    MetricsQl,
}

/// Stable identity inside one query entry. Edges are IDs so common
/// subexpressions remain shared after serialization.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct QueryNodeId(pub u64);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct QueryPlanEntry {
    #[serde(default)]
    pub language: QueryLanguage,
    pub query_id: String,
    #[serde(alias = "canonical_promql")]
    pub canonical_query: String,
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
    /// Materializations this executable DAG reads, in stable node order.
    /// Serving uses this set for readiness accounting; it never performs a
    /// catalog candidate search to reconstruct dependencies.
    pub fn materialization_bindings(&self) -> Vec<&MaterializationBinding> {
        self.nodes
            .values()
            .filter_map(|node| match node {
                QueryPlanNode::ReadMaterialization { binding } => Some(binding),
                _ => None,
            })
            .collect()
    }

    pub fn compile_bound<F>(
        query_id: String,
        canonical_query: String,
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
            logical_source: None,
        };
        let root = compiler.lower(root)?;
        Ok(Self {
            language: QueryLanguage::PromQl,
            query_id,
            canonical_query,
            root,
            nodes: compiler.nodes,
            instant,
            fallback,
        })
    }

    /// Compile selected summary nodes and verified native residuals into one DAG.
    /// This is a distinct physical alternative; native execution remains available.
    pub fn compile_bound_composable<F>(
        query_id: String,
        canonical_query: String,
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
            logical_source: Some(canonical_query.clone()),
        };
        let root = compiler.lower(root)?;
        let mut entry = Self {
            language: QueryLanguage::PromQl,
            query_id,
            canonical_query,
            root,
            nodes: compiler.nodes,
            instant,
            fallback,
        };
        logical::finalize_residuals(&mut entry)?;
        Ok(entry)
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
            if let QueryPlanNode::Logical { operator, inputs } = node {
                operator.validate(inputs.len())?;
            }
            if matches!(node, QueryPlanNode::Scalar { value } if !value.is_finite()) {
                return Err(QueryPlanError::Invalid("non-finite scalar constant".into()));
            }
            if let QueryPlanNode::CandidateTopK {
                k, completeness, ..
            } = node
            {
                if *k == 0 {
                    return Err(QueryPlanError::Invalid(
                        "CandidateTopK requires k > 0".into(),
                    ));
                }
                if matches!(
                    completeness,
                    CandidateCompleteness::Certified { guarantee }
                        if guarantee.metric
                            != planner_types::post_asap::ErrorMetric::TopKMembership
                            || guarantee.bound.evaluate().is_none()
                            || guarantee.failure_probability.evaluate().is_none()
                ) {
                    return Err(QueryPlanError::Invalid(
                        "invalid CandidateTopK completeness certificate".into(),
                    ));
                }
            }
            for input in node.inputs() {
                if !self.nodes.contains_key(input) {
                    return Err(QueryPlanError::Invalid(format!(
                        "query `{}` node {} references missing input {}",
                        self.query_id, id.0, input.0
                    )));
                }
            }
            if let QueryPlanNode::ReadMaterialization { binding } = node {
                if binding.readout_lookback_ms == Some(0) {
                    return Err(QueryPlanError::Invalid(
                        "zero semantic readout lookback".into(),
                    ));
                }
                if !available.contains(&binding.materialization.fingerprint()) {
                    return Err(QueryPlanError::Invalid(format!(
                        "query `{}` node {} references absent materialization {}",
                        self.query_id,
                        id.0,
                        binding.materialization.as_u64()
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
    pub materialization: SummaryDefinitionId,
    /// Query operator grouping applied while folding those SIDs.
    pub output_grouping: PhysicalGrouping,
    /// Labels whose values form an item identity inside a keyed sketch.
    #[serde(default, alias = "itemLabels", skip_serializing_if = "Vec::is_empty")]
    pub item_labels: Vec<String>,
    pub window_ms: u64,
    /// Unix millisecond timestamp on the materialized pane-boundary grid.
    /// Legacy plans deserialize this as unknown and fall back at read time.
    #[serde(
        default,
        alias = "paneOriginMs",
        skip_serializing_if = "Option::is_none"
    )]
    pub pane_origin_ms: Option<i64>,
    /// Semantic query lookback, independent of the physical pane duration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readout_lookback_ms: Option<u64>,
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
    Logical {
        operator: logical::LogicalOperator,
        inputs: Vec<QueryNodeId>,
    },
    Scalar {
        value: f64,
    },
    Binary {
        inputs: [QueryNodeId; 2],
        operator: planner_types::pre_asap::ArithmeticOpKind,
    },
    ReduceSum {
        input: QueryNodeId,
        grouping: PhysicalGrouping,
    },
    ReadMaterialization {
        binding: MaterializationBinding,
    },
    SummaryEstimate {
        input: QueryNodeId,
        query: QueryReadout,
    },
    ExactReadout {
        input: QueryNodeId,
        readout: ExactReadout,
    },
    SummaryMerge {
        inputs: Vec<QueryNodeId>,
    },
    /// Use an approximate heap only as a membership sidecar, then rerank the
    /// matching exact counter readouts. `inputs[0]` is candidate membership;
    /// `inputs[1]` is the authoritative exact value vector.
    CandidateTopK {
        inputs: [QueryNodeId; 2],
        k: u64,
        grouping: logical::Grouping,
        completeness: CandidateCompleteness,
    },
    ExactFallback {
        reason: String,
    },
}

impl QueryPlanNode {
    pub fn inputs(&self) -> &[QueryNodeId] {
        match self {
            Self::Scalar { .. } | Self::ReadMaterialization { .. } | Self::ExactFallback { .. } => {
                &[]
            }
            Self::Binary { inputs, .. } => inputs,
            Self::ReduceSum { input, .. }
            | Self::SummaryEstimate { input, .. }
            | Self::ExactReadout { input, .. } => std::slice::from_ref(input),
            Self::SummaryMerge { inputs } | Self::Logical { inputs, .. } => inputs,
            Self::CandidateTopK { inputs, .. } => inputs,
        }
    }
}

pub use planner_types::post_asap::CandidateCompleteness;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExactReadout {
    Sum,
    Count,
    Increase,
    Rate,
    Max,
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
    logical_source: Option<String>,
}

impl<F> DagCompiler<'_, F>
where
    F: FnMut(&SummaryNode, &SummaryFamilyType) -> Result<MaterializationBinding, QueryPlanError>,
{
    fn graft(
        &mut self,
        id: QueryNodeId,
        root: QueryNodeId,
        nodes: BTreeMap<QueryNodeId, QueryPlanNode>,
    ) -> Result<QueryNodeId, QueryPlanError> {
        let mut remap = BTreeMap::new();
        for local in nodes.keys() {
            let global = if *local == root {
                id
            } else {
                let next = QueryNodeId(self.next_id);
                self.next_id += 1;
                next
            };
            remap.insert(*local, global);
        }
        for (local, mut physical) in nodes {
            match &mut physical {
                QueryPlanNode::Logical { inputs, .. } | QueryPlanNode::SummaryMerge { inputs } => {
                    for input in inputs {
                        *input = remap[input];
                    }
                }
                QueryPlanNode::CandidateTopK { inputs, .. }
                | QueryPlanNode::Binary { inputs, .. } => {
                    for input in inputs {
                        *input = remap[input];
                    }
                }
                QueryPlanNode::SummaryEstimate { input, .. }
                | QueryPlanNode::ExactReadout { input, .. }
                | QueryPlanNode::ReduceSum { input, .. } => *input = remap[input],
                QueryPlanNode::Scalar { .. }
                | QueryPlanNode::ReadMaterialization { .. }
                | QueryPlanNode::ExactFallback { .. } => {}
            }
            self.nodes.insert(remap[&local], physical);
        }
        return Ok(id);
    }

    fn lower(&mut self, node: &Rc<SummaryNode>) -> Result<QueryNodeId, QueryPlanError> {
        let identity = Rc::as_ptr(node) as usize;
        if let Some(id) = self.seen.get(&identity) {
            return Ok(*id);
        }
        if let SummaryExpr::ValueOperation {
            child,
            operation: planner_types::post_asap::ValueOperation::FinalizeExactAccumulator,
            timing: planner_types::post_asap::ExecutionTiming::ReadTime,
        } = &node.expr
        {
            // SummaryAgg lowering already emits the family-specific ExactReadout.
            // Preserve the Planner's explicit state boundary without adding a
            // second runtime readout node.
            let child_id = self.lower(child)?;
            self.seen.insert(identity, child_id);
            return Ok(child_id);
        }
        let id = QueryNodeId(self.next_id);
        self.next_id += 1;
        self.seen.insert(identity, id);
        let residual = match (&self.logical_source, &node.expr) {
            (Some(original), SummaryExpr::KeepPreAsap(expr)) => {
                Some(logical::residual_nodes(original, expr)?)
            }
            (Some(original), SummaryExpr::SummaryAgg { child, .. })
                if matches!(child.expr, SummaryExpr::KeepPreAsap(_))
                    && !matches!(
                        crate::physical::compiler::materialization_leaf_contract(node),
                        Ok((_, Some(_), _))
                    ) =>
            {
                Some(logical::selected_residual_nodes(original, node)?)
            }
            _ => None,
        };
        if let Some((root, nodes)) = residual {
            return self.graft(id, root, nodes);
        }

        let physical = match &node.expr {
            SummaryExpr::ValueOperation {
                child,
                operation:
                    planner_types::post_asap::ValueOperation::Exact(
                        planner_types::post_asap::ExactOperation::Aggregate {
                            reduction,
                            measures,
                            having: None,
                            ..
                        },
                    ),
                timing: planner_types::post_asap::ExecutionTiming::ReadTime,
            } if measures.len() == 1 => {
                use planner_types::pre_asap::AggIntent;
                let operation = match &measures[0] {
                    AggIntent::Sum { .. } => logical::Aggregation::Sum,
                    AggIntent::Count { .. } => logical::Aggregation::Count,
                    AggIntent::Min { .. } => logical::Aggregation::Min,
                    AggIntent::Max { .. } => logical::Aggregation::Max,
                    AggIntent::Avg { .. } => logical::Aggregation::Avg,
                    _ => {
                        return Err(QueryPlanError::Invalid(
                            "unsupported exact value aggregation".into(),
                        ))
                    }
                };
                let keys = reduction.group_keys().ok_or_else(|| {
                    QueryPlanError::Invalid(
                        "per-entity exact value aggregation has no grouping".into(),
                    )
                })?;
                let labels = keys
                    .keys()
                    .iter()
                    .map(|&column| {
                        child
                            .schema
                            .fields
                            .get(column)
                            .map(|field| field.name.clone())
                            .ok_or_else(|| {
                                QueryPlanError::Invalid(
                                    "unresolved exact aggregation column".into(),
                                )
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                QueryPlanNode::Logical {
                    operator: logical::LogicalOperator::Aggregate {
                        operation,
                        grouping: logical::Grouping {
                            labels,
                            without: keys.is_without(),
                        },
                    },
                    inputs: vec![self.lower(child)?],
                }
            }
            SummaryExpr::ValueOperation {
                child: sort,
                operation: planner_types::post_asap::ValueOperation::Limit { n, offset: 0 },
                timing: planner_types::post_asap::ExecutionTiming::ReadTime,
            } => {
                let SummaryExpr::ValueOperation {
                    child,
                    operation: planner_types::post_asap::ValueOperation::Sort { keys, partition_by },
                    timing: planner_types::post_asap::ExecutionTiming::ReadTime,
                } = &sort.expr
                else {
                    return Err(QueryPlanError::Invalid(
                        "query-time Limit must consume a query-time Sort".into(),
                    ));
                };
                if keys.len() != 1 || keys[0].ascending {
                    return Err(QueryPlanError::Invalid(
                        "only descending value-ranked TopK is executable".into(),
                    ));
                }
                let planner_types::pre_asap::QueryExpr::Column(sort_column) = &keys[0].expr else {
                    return Err(QueryPlanError::Invalid(
                        "TopK sort key must reference the child value column".into(),
                    ));
                };
                if !matches!(
                    child
                        .schema
                        .fields
                        .get(*sort_column)
                        .map(|field| &field.dtype),
                    Some(SummaryFamilyType::Plain(
                        planner_types::pre_asap::DataType::Float64
                    )) | Some(SummaryFamilyType::ExactAggregate(..))
                ) {
                    return Err(QueryPlanError::Invalid(
                        "TopK sort key must produce a numeric value".into(),
                    ));
                }
                let labels = partition_by
                    .keys()
                    .iter()
                    .map(|&column| {
                        child
                            .schema
                            .fields
                            .get(column)
                            .map(|field| field.name.clone())
                            .ok_or_else(|| {
                                QueryPlanError::Invalid("unresolved TopK partition column".into())
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                QueryPlanNode::Logical {
                    operator: logical::LogicalOperator::TopKSelection {
                        k: u64::try_from(*n).map_err(|_| {
                            QueryPlanError::Invalid("TopK limit exceeds u64".into())
                        })?,
                        grouping: logical::Grouping {
                            labels,
                            without: partition_by.is_without(),
                        },
                    },
                    inputs: vec![self.lower(child)?],
                }
            }
            SummaryExpr::ValueOperation {
                child,
                operation: planner_types::post_asap::ValueOperation::Sort { keys, .. },
                timing: planner_types::post_asap::ExecutionTiming::ReadTime,
            } if keys.len() == 1 => QueryPlanNode::Logical {
                operator: logical::LogicalOperator::Sort {
                    descending: !keys[0].ascending,
                },
                inputs: vec![self.lower(child)?],
            },
            SummaryExpr::ValueOperation { .. } => QueryPlanNode::ExactFallback {
                reason: "unsupported post-ASAP value operation".into(),
            },
            SummaryExpr::CandidateTopK {
                candidates,
                values,
                k,
                grouping,
                completeness,
            } => {
                let labels = grouping
                    .keys()
                    .iter()
                    .map(|&column| {
                        values
                            .schema
                            .fields
                            .get(column)
                            .map(|field| field.name.clone())
                            .ok_or_else(|| {
                                QueryPlanError::Invalid(
                                    "unresolved CandidateTopK grouping column".into(),
                                )
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                QueryPlanNode::CandidateTopK {
                    inputs: [self.lower(candidates)?, self.lower(values)?],
                    k: u64::try_from(*k).map_err(|_| {
                        QueryPlanError::Invalid("CandidateTopK k exceeds u64".into())
                    })?,
                    grouping: logical::Grouping {
                        labels,
                        without: grouping.is_without(),
                    },
                    completeness: completeness.clone(),
                }
            }
            SummaryExpr::BinaryOp { lhs, rhs, operator } if self.logical_source.is_some() => {
                let operator = logical::binary_operator(operator)?;
                QueryPlanNode::Logical {
                    operator,
                    inputs: vec![self.lower(lhs)?, self.lower(rhs)?],
                }
            }

            SummaryExpr::SummaryAgg {
                family: SummaryFamilyType::ExactAggregate(kind, _),
                child,
                reduction,
                ..
            } if self.logical_source.is_some()
                && !matches!(child.expr, SummaryExpr::KeepPreAsap(_)) =>
            {
                if !matches!(
                    kind,
                    planner_types::post_asap::ExactKind::Sum
                        | planner_types::post_asap::ExactKind::Count
                ) {
                    let operator = logical::selected_aggregate_operator(
                        self.logical_source.as_deref().unwrap(),
                        node,
                    )?;
                    let input = self.lower(child)?;
                    self.nodes.insert(
                        id,
                        QueryPlanNode::Logical {
                            operator,
                            inputs: vec![input],
                        },
                    );
                    return Ok(id);
                }
                let operation = match kind {
                    planner_types::post_asap::ExactKind::Sum => logical::Aggregation::Sum,
                    planner_types::post_asap::ExactKind::Count => logical::Aggregation::Count,
                    _ => {
                        return Err(QueryPlanError::Invalid(
                            "unsupported aggregation over selected summary values".into(),
                        ))
                    }
                };
                let keys = reduction.group_keys().ok_or_else(|| {
                    QueryPlanError::Invalid(
                        "per-entity summary reduction requires a temporal operator".into(),
                    )
                })?;
                let labels = keys
                    .keys()
                    .iter()
                    .map(|&column| {
                        child
                            .schema
                            .fields
                            .get(column)
                            .map(|field| field.name.clone())
                            .ok_or_else(|| {
                                QueryPlanError::Invalid("unresolved logical grouping column".into())
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                QueryPlanNode::Logical {
                    operator: logical::LogicalOperator::Aggregate {
                        operation,
                        grouping: logical::Grouping {
                            labels,
                            without: keys.is_without(),
                        },
                    },
                    inputs: vec![self.lower(child)?],
                }
            }
            SummaryExpr::BinaryOp { lhs, rhs, operator } if exact_value_executable(node) => {
                let planner_types::pre_asap::BinaryOpKind::Arithmetic(operator) = &operator.kind
                else {
                    unreachable!()
                };
                QueryPlanNode::Binary {
                    inputs: [self.lower(lhs)?, self.lower(rhs)?],
                    operator: operator.clone(),
                }
            }
            SummaryExpr::KeepPreAsap(expr) if scalar_literal(expr).is_some() => {
                QueryPlanNode::Scalar {
                    value: scalar_literal(expr).unwrap(),
                }
            }
            SummaryExpr::SummaryAgg {
                family:
                    SummaryFamilyType::ExactAggregate(planner_types::post_asap::ExactKind::Sum, _),
                child,
                reduction,
                ..
            } if !matches!(child.expr, SummaryExpr::KeepPreAsap(_))
                && exact_value_executable(node) =>
            {
                QueryPlanNode::ReduceSum {
                    input: self.lower(child)?,
                    grouping: physical_grouping(reduction, child)?,
                }
            }
            SummaryExpr::SummaryAgg { child, .. }
                if !matches!(child.expr, SummaryExpr::KeepPreAsap(_)) =>
            {
                QueryPlanNode::ExactFallback {
                    reason: "unsupported exact operation over summary output".into(),
                }
            }
            SummaryExpr::SummaryAgg {
                family:
                    SummaryFamilyType::ExactAggregate(planner_types::post_asap::ExactKind::Count, _),
                ..
            } if !exact_value_executable(node) => QueryPlanNode::ExactFallback {
                reason: "only temporal observation counts are supported".into(),
            },
            SummaryExpr::BinaryOp { .. } => QueryPlanNode::ExactFallback {
                reason: "summary binary operation is not executable by the warm tier".into(),
            },
            SummaryExpr::KeepPreAsap(_) => QueryPlanNode::ExactFallback {
                reason: "post-ASAP node requires exact execution".into(),
            },
            SummaryExpr::SummaryAgg {
                family,
                reduction,
                child,
                ..
            } => match family {
                SummaryFamilyType::ExactAggregate(..) | SummaryFamilyType::Sketch(..) => {
                    let mut binding = match (self.bind)(node, family) {
                        Ok(binding) => binding,
                        Err(error) => {
                            if node.guarantee.as_ref().is_some_and(|g| g.is_exact()) {
                                if let Some(original) = &self.logical_source {
                                    let (root, nodes) =
                                        logical::selected_residual_nodes(original, node)?;
                                    return self.graft(id, root, nodes);
                                }
                            }
                            return Err(error);
                        }
                    };
                    binding.output_grouping = physical_grouping(reduction, child)?;
                    if let Some(readout) = exact_readout(family) {
                        let existing = self.nodes.iter().find_map(|(id, node)| {
                            matches!(node, QueryPlanNode::ReadMaterialization { binding: other } if other == &binding).then_some(*id)
                        });
                        let input = existing.unwrap_or_else(|| {
                            let input = QueryNodeId(self.next_id);
                            self.next_id += 1;
                            self.nodes
                                .insert(input, QueryPlanNode::ReadMaterialization { binding });
                            input
                        });
                        QueryPlanNode::ExactReadout { input, readout }
                    } else {
                        QueryPlanNode::ReadMaterialization { binding }
                    }
                }
                other => QueryPlanNode::ExactFallback {
                    reason: format!("summary family {other:?} is not executable by the warm tier"),
                },
            },
            SummaryExpr::SummaryEstimate {
                summary_input,
                query,
            } => QueryPlanNode::SummaryEstimate {
                input: self.lower(summary_input)?,
                query: query.clone().into(),
            },
            SummaryExpr::SummaryMerge { children } => {
                if children.is_empty() {
                    QueryPlanNode::ExactFallback {
                        reason: "empty summary_merge".into(),
                    }
                } else {
                    QueryPlanNode::SummaryMerge {
                        inputs: children
                            .iter()
                            .map(|child| self.lower(child))
                            .collect::<Result<_, _>>()?,
                    }
                }
            }
            SummaryExpr::SummaryJoin { .. } => QueryPlanNode::ExactFallback {
                reason: "summary_join is not executable by the warm tier".into(),
            },
            SummaryExpr::SummarySubtract { .. } => QueryPlanNode::ExactFallback {
                reason: "summary_subtract is not executable by the warm tier".into(),
            },
            SummaryExpr::SummaryDelete { .. } => QueryPlanNode::ExactFallback {
                reason: "summary_delete is not executable by the warm tier".into(),
            },
        };
        self.nodes.insert(id, physical);
        Ok(id)
    }
}

fn exact_readout(family: &SummaryFamilyType) -> Option<ExactReadout> {
    use planner_types::post_asap::ExactKind;
    match family {
        SummaryFamilyType::ExactAggregate(ExactKind::Sum, _) => Some(ExactReadout::Sum),
        SummaryFamilyType::ExactAggregate(ExactKind::Count, _) => Some(ExactReadout::Count),
        SummaryFamilyType::ExactAggregate(ExactKind::Increase, _) => Some(ExactReadout::Increase),
        SummaryFamilyType::ExactAggregate(ExactKind::Rate, _) => Some(ExactReadout::Rate),
        SummaryFamilyType::ExactAggregate(ExactKind::MinMax, _) => Some(ExactReadout::Max),
        _ => None,
    }
}

fn scalar_literal(expr: &planner_types::pre_asap::QueryExpr) -> Option<f64> {
    use planner_types::pre_asap::{QueryExpr, ScalarValue};
    let value = match expr {
        QueryExpr::PromqlScalarBridge(child) => return scalar_literal(child),
        QueryExpr::Literal(ScalarValue::Float64(value)) => *value,
        QueryExpr::Literal(ScalarValue::Int64(value)) => *value as f64,
        _ => return None,
    };
    value.is_finite().then_some(value)
}

/// The current exact arithmetic adapter is deliberately narrower than PromQL:
/// default vector matching, scalar literals and additive temporal readouts.
/// Unsupported operands make the complete expression fall back.
pub(crate) fn exact_value_executable(node: &SummaryNode) -> bool {
    use planner_types::post_asap::ExactKind;
    if !node
        .guarantee
        .as_ref()
        .is_some_and(|guarantee| guarantee.is_exact())
    {
        return false;
    }
    match &node.expr {
        SummaryExpr::KeepPreAsap(expr) => scalar_literal(expr).is_some(),
        SummaryExpr::BinaryOp { lhs, rhs, operator } => {
            matches!(
                operator.kind,
                planner_types::pre_asap::BinaryOpKind::Arithmetic(_)
            ) && operator.vector_match.is_none()
                && exact_value_executable(lhs)
                && exact_value_executable(rhs)
                && value_grouping(node).is_ok()
                && match (value_source(lhs), value_source(rhs)) {
                    (Some(left), Some(right)) => left == right,
                    _ => true,
                }
        }
        SummaryExpr::SummaryAgg {
            family: SummaryFamilyType::ExactAggregate(kind, _),
            child,
            reduction,
            ..
        } => {
            if matches!(child.expr, SummaryExpr::KeepPreAsap(_)) {
                matches!(&child.expr, SummaryExpr::KeepPreAsap(expr) if matches!(expr.as_ref(), planner_types::pre_asap::QueryExpr::TimeRange { child, .. } if matches!(child.as_ref(), planner_types::pre_asap::QueryExpr::Scan { .. })))
                    && matches!(reduction, Reduction::PerEntity)
                    && matches!(
                        kind,
                        ExactKind::Sum | ExactKind::Count | ExactKind::Increase | ExactKind::Rate
                    )
            } else {
                // Raw producer grouping may move through additive reductions,
                // but never through division or other value arithmetic.
                matches!(kind, ExactKind::Sum)
                    && matches!(child.expr, SummaryExpr::SummaryAgg { .. })
                    && exact_value_executable(child)
            }
        }
        _ => false,
    }
}

fn value_grouping(node: &SummaryNode) -> Result<Option<PhysicalGrouping>, QueryPlanError> {
    match &node.expr {
        SummaryExpr::KeepPreAsap(_) => Ok(None),
        SummaryExpr::SummaryAgg {
            reduction, child, ..
        } => physical_grouping(reduction, child).map(Some),
        SummaryExpr::BinaryOp { lhs, rhs, .. } => {
            let left = value_grouping(lhs)?;
            let right = value_grouping(rhs)?;
            match (left, right) {
                (Some(left), Some(right)) if left != right => Err(QueryPlanError::Invalid(
                    "arithmetic operands require different producer grouping contracts".into(),
                )),
                (left, right) => Ok(left.or(right)),
            }
        }
        _ => Err(QueryPlanError::Invalid(
            "unsupported exact value grouping".into(),
        )),
    }
}

// The MVP QueryPlan evaluates all operands over one interval. Different
// selectors/windows need per-operand time binding before they can be warm.
fn value_source(node: &SummaryNode) -> Option<&planner_types::pre_asap::QueryExpr> {
    match &node.expr {
        SummaryExpr::SummaryAgg { child, .. } => match &child.expr {
            SummaryExpr::KeepPreAsap(expr) => Some(expr),
            _ => value_source(child),
        },
        SummaryExpr::BinaryOp { lhs, rhs, .. } => value_source(lhs).or_else(|| value_source(rhs)),
        _ => None,
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
    fn language_tag_preserves_query_entry_serde() {
        let entry = QueryPlanEntry {
            language: crate::query_plan::QueryLanguage::PromQl,
            query_id: "q".into(),
            canonical_query: canonical_promql("up").unwrap(),
            root: QueryNodeId(0),
            nodes: BTreeMap::from([(
                QueryNodeId(0),
                QueryPlanNode::ExactFallback {
                    reason: "fixture".into(),
                },
            )]),
            instant: InstantExecution {
                lookback_ms: 1,
                full_history: false,
                cumulative_readout: false,
            },
            fallback: FallbackPolicy::ExactBackend,
        };
        let before = serde_json::to_value(&entry).unwrap();
        assert_eq!(before, serde_json::to_value(&entry).unwrap());
        assert!(before.get("canonical_query").is_some());
        assert!(before.get("executable").is_none());
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
            language: crate::query_plan::QueryLanguage::PromQl,
            query_id: "q".into(),
            canonical_query: "up".into(),
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

    #[test]
    fn candidate_topk_rejects_invalid_completeness_contract() {
        let leaf = QueryPlanNode::ExactFallback {
            reason: "prepared".into(),
        };
        let entry = QueryPlanEntry {
            language: crate::query_plan::QueryLanguage::PromQl,
            query_id: "q".into(),
            canonical_query: "topk(2, rate(m[5m]))".into(),
            root: QueryNodeId(2),
            nodes: BTreeMap::from([
                (QueryNodeId(0), leaf.clone()),
                (QueryNodeId(1), leaf),
                (
                    QueryNodeId(2),
                    QueryPlanNode::CandidateTopK {
                        inputs: [QueryNodeId(0), QueryNodeId(1)],
                        k: 2,
                        grouping: logical::Grouping {
                            labels: vec![],
                            without: false,
                        },
                        completeness: CandidateCompleteness::Certified {
                            guarantee: planner_types::post_asap::ResultGuarantee {
                                metric: planner_types::post_asap::ErrorMetric::Frequency,
                                bound: planner_types::post_asap::BoundExpr::Unknown {
                                    statistic: "membership margin".into(),
                                },
                                failure_probability:
                                    planner_types::post_asap::ProbabilityExpr::Unknown {
                                        statistic: "membership confidence".into(),
                                    },
                                provenance: vec![],
                            },
                        },
                    },
                ),
            ]),
            instant: InstantExecution {
                lookback_ms: 300_000,
                full_history: false,
                cumulative_readout: false,
            },
            fallback: FallbackPolicy::ExactBackend,
        };
        assert!(entry.validate(&BTreeSet::new()).is_err());
    }
}

#[cfg(test)]
mod catalog_binding_tests {
    use super::*;
    use crate::physical::summary_catalog::SummaryCatalog;
    use asap_types::{AggregationType, KeyByLabelNames, PrecomputeMaterialization, WindowKind};

    fn fixture() -> (QueryPlan, SummaryCatalog) {
        let mut config = PrecomputeMaterialization::new(
            AggregationType::Sum,
            String::new(),
            Default::default(),
            KeyByLabelNames::new(vec!["job".into()]),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            10,
            10,
            WindowKind::Tumbling,
            String::new(),
            "m".into(),
            None,
            None,
            None,
        );
        config.pane_origin_ms = Some(0);
        let catalog = SummaryCatalog::from_materializations(7, 2, &[config.clone()]).unwrap();
        let entry = QueryPlanEntry {
            language: crate::query_plan::QueryLanguage::PromQl,
            query_id: "q".into(),
            canonical_query: "sum_over_time(m[1m])".into(),
            root: QueryNodeId(1),
            nodes: BTreeMap::from([(
                QueryNodeId(1),
                QueryPlanNode::ReadMaterialization {
                    binding: MaterializationBinding {
                        item_labels: Vec::new(),
                        materialization: config.policy_fingerprint().into(),
                        output_grouping: PhysicalGrouping::PerEntity,
                        window_ms: 10_000,
                        pane_origin_ms: Some(0),
                        readout_lookback_ms: Some(60_000),
                    },
                },
            )]),
            instant: InstantExecution {
                lookback_ms: 60_000,
                full_history: false,
                cumulative_readout: true,
            },
            fallback: FallbackPolicy::ExactBackend,
        };
        (
            QueryPlan {
                plan_id: 7,
                plan_version: 2,
                entries: BTreeMap::from([(entry.canonical_query.clone(), entry)]),
            },
            catalog,
        )
    }
    fn binding(plan: &mut QueryPlan) -> &mut MaterializationBinding {
        let QueryPlanNode::ReadMaterialization { binding } = plan
            .entries
            .values_mut()
            .next()
            .unwrap()
            .nodes
            .values_mut()
            .next()
            .unwrap()
        else {
            panic!("fixture")
        };
        binding
    }

    // One pane ID is compatible with a longer semantic readout window.
    #[test]
    fn catalog_binding_round_trip_preserves_pane_and_readout_windows() {
        let (plan, catalog) = fixture();
        let mut decoded: QueryPlan =
            serde_json::from_slice(&serde_json::to_vec(&plan).unwrap()).unwrap();
        decoded.validate_against_catalog(&catalog).unwrap();
        assert_eq!(binding(&mut decoded).window_ms, 10_000);
        assert_eq!(binding(&mut decoded).readout_lookback_ms, Some(60_000));
    }

    // The catalog owns source and grouping; the binding owns only its stable ID.
    #[test]
    fn catalog_binding_rejects_source_grouping_and_identity_drift() {
        let (plan, catalog) = fixture();
        let mut broken = plan.clone();
        binding(&mut broken).materialization = PolicyFingerprint(123).into();
        assert!(broken.validate_against_catalog(&catalog).is_err());
        let mut broken = plan.clone();
        broken.plan_version += 1;
        assert!(broken.validate_against_catalog(&catalog).is_err());
        let mut broken = plan;
        binding(&mut broken).window_ms = 0;
        assert!(broken.validate_against_catalog(&catalog).is_err());
    }

    // Catalog descriptor corruption must fail even if the materialization exists.
    #[test]
    fn catalog_binding_rejects_broken_descriptor_reference() {
        let (plan, mut catalog) = fixture();
        catalog.summary_descriptors.clear();
        assert!(plan.validate_against_catalog(&catalog).is_err());
    }

    #[test]
    fn counter_readout_requires_counter_sds_fidelity() {
        fn as_rate_plan(mut plan: QueryPlan) -> QueryPlan {
            let entry = plan.entries.values_mut().next().unwrap();
            let read = entry.root;
            let root = QueryNodeId(2);
            entry.root = root;
            entry.nodes.insert(
                root,
                QueryPlanNode::ExactReadout {
                    input: read,
                    readout: ExactReadout::Rate,
                },
            );
            plan
        }

        let (sum_plan, sum_catalog) = fixture();
        assert!(as_rate_plan(sum_plan)
            .validate_against_catalog(&sum_catalog)
            .unwrap_err()
            .to_string()
            .contains("exact counter SDS"));

        let mut counter = PrecomputeMaterialization::new(
            AggregationType::Increase,
            String::new(),
            Default::default(),
            KeyByLabelNames::new(vec!["job".into()]),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            10,
            10,
            WindowKind::Tumbling,
            String::new(),
            "m".into(),
            None,
            None,
            None,
        );
        counter.pane_origin_ms = Some(0);
        let counter_catalog =
            SummaryCatalog::from_materializations(7, 2, &[counter.clone()]).unwrap();
        let (mut counter_plan, _) = fixture();
        binding(&mut counter_plan).materialization = counter.policy_fingerprint().into();
        as_rate_plan(counter_plan)
            .validate_against_catalog(&counter_catalog)
            .unwrap();
    }
}
