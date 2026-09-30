//! Control-plane lowering from Planner IR to the shared installed query DAG.
//! Serving consumes asap_types::query_plan; compilation stays in this component.
//!
//! The backend lowers only stored-state readouts itself. Query-time
//! computation over their decoded values is one Planner-compiled physical DAG
//! per maximal computation region.
use asap_types::physical_plan_codec::PhysicalPlanCodec;

mod clickhouse_exact;
pub mod query_time;

pub use asap_types::query_plan::*;
#[cfg(test)]
use asap_types::PolicyFingerprint;
use planner_types::post_asap::{SummaryExpr, SummaryFamilyType, SummaryNode};
use planner_types::pre_asap::Reduction;
use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::BTreeSet;
use std::rc::Rc;

pub fn compile_bound_mapped<F, G>(
    query_id: String,
    canonical_query: String,
    root: &Rc<SummaryNode>,
    instant: InstantExecution,
    fallback: FallbackPolicy,
    mut bind: F,
    mut lowered: G,
) -> Result<QueryPlanEntry, QueryPlanError>
where
    F: FnMut(
        &Rc<SummaryNode>,
        &SummaryFamilyType,
    ) -> Result<MaterializationBinding, QueryPlanError>,
    G: FnMut(&Rc<SummaryNode>, QueryNodeId),
{
    let mut compiler = DagCompiler {
        next_id: 0,
        nodes: BTreeMap::new(),
        seen: BTreeMap::new(),
        bind: &mut bind,
        preserve_relational: false,
        lowered: Some(&mut lowered),
    };
    let root = compiler.lower(root)?;
    Ok(QueryPlanEntry {
        physical_dag: None,
        language: QueryLanguage::PromQl,
        query_id,
        canonical_query,
        fixed_evaluation: None,
        root,
        nodes: compiler.nodes,
        instant,
        fallback,
    })
}

/// Preserve Planner-to-runtime node identities for installed SQL DAGs.
pub fn compile_bound_relational_mapped<F, G>(
    query_id: String,
    canonical_query: String,
    root: &Rc<SummaryNode>,
    fixed_evaluation: FixedEvaluationRange,
    instant: InstantExecution,
    fallback: FallbackPolicy,
    mut bind: F,
    mut lowered: G,
) -> Result<QueryPlanEntry, QueryPlanError>
where
    F: FnMut(
        &Rc<SummaryNode>,
        &SummaryFamilyType,
    ) -> Result<MaterializationBinding, QueryPlanError>,
    G: FnMut(&Rc<SummaryNode>, QueryNodeId),
{
    let mut compiler = DagCompiler {
        next_id: 0,
        nodes: BTreeMap::new(),
        seen: BTreeMap::new(),
        bind: &mut bind,
        preserve_relational: true,
        lowered: Some(&mut lowered),
    };
    let root = compiler.lower_relation(root)?;
    Ok(QueryPlanEntry {
        physical_dag: None,
        language: QueryLanguage::ClickHouseSql,
        query_id,
        canonical_query,
        fixed_evaluation: Some(fixed_evaluation),
        root,
        nodes: compiler.nodes,
        instant,
        fallback,
    })
}

/// Query-time computation over stored readouts, compiled once by Planner.
pub(crate) struct QueryComputation {
    pub(crate) physical: asap_physical_operators::physical_planner::CompiledPhysicalDag,
    /// Readout feeding each physical input contract, keyed by contract ID.
    frontier: BTreeMap<u64, Rc<SummaryNode>>,
    computed: Vec<Rc<SummaryNode>>,
    pruning: Option<PruningInputContract>,
}

/// Is this node decoded from stored state by the backend, rather than
/// computed over values? An exact aggregate over values is a query-time
/// reduction; a sketch over values is a derived stored output.
fn stored_readout(node: &SummaryNode) -> bool {
    match &node.expr {
        SummaryExpr::ValueOperation {
            operation: planner_types::post_asap::ValueOperation::FinalizeExactAccumulator,
            ..
        } => exact_accumulator_value_source(node)
            .is_some_and(|source| !std::ptr::eq(source, node) && stored_readout(source)),
        SummaryExpr::SummaryEstimate { .. }
        | SummaryExpr::SummaryMerge { .. }
        | SummaryExpr::SummaryJoin { .. }
        | SummaryExpr::SummarySubtract { .. }
        | SummaryExpr::SummaryDelete { .. } => true,
        SummaryExpr::SummaryAgg { child, family, .. } => {
            matches!(child.expr, SummaryExpr::KeepPreAsap(_))
                || !matches!(family, SummaryFamilyType::ExactAggregate(..))
        }
        _ => false,
    }
}

/// Does lowering `node` start a query-time computation region?
pub(crate) fn is_query_computation(node: &SummaryNode) -> bool {
    !matches!(
        node.expr,
        SummaryExpr::KeepPreAsap(_)
            | SummaryExpr::ValueOperation {
                operation: planner_types::post_asap::ValueOperation::MaintainPopulation { .. }
                    | planner_types::post_asap::ValueOperation::ReadPopulation { .. },
                ..
            }
    ) && !stored_readout(node)
}

/// Compile the maximal query-time region rooted at `root`. Its inputs are the
/// stored readouts below it; an exact PromQL selector inside the region has no
/// stored input, so the region is unsupported and the query runs exactly.
pub(crate) fn compile_query_computation(
    root: &Rc<SummaryNode>,
) -> Result<QueryComputation, QueryPlanError> {
    use asap_physical_operators::physical_planner::{compile, promql_fallback, InputContract};
    use planner_types::post_asap::{
        compile_post_asap_dag_with_node_ids, EdgeRole, PostAsapOperatorPayload as Payload,
    };
    let unsupported = |message: String| QueryPlanError::UnsupportedNode(message);
    let finalized = finalize_query_value(root);
    let compilation = compile_post_asap_dag_with_node_ids(&finalized)
        .map_err(|error| unsupported(error.to_string()))?;
    let dag = &compilation.dag;
    let mut pending = vec![dag.root];
    let mut visited = std::collections::BTreeSet::new();
    let mut contracts = BTreeMap::new();
    let mut frontier = BTreeMap::new();
    let mut computed = Vec::new();
    while let Some(id) = pending.pop() {
        if !visited.insert(id) {
            continue;
        }
        let node = dag
            .nodes
            .iter()
            .find(|node| node.id == id)
            .ok_or_else(|| unsupported("missing Planner node".into()))?;
        let semantic = compilation.node_ids.summary_node(id);
        let readout = id != dag.root && semantic.is_some_and(|semantic| stored_readout(semantic));
        if !readout {
            if let Payload::Fallback { expression } = &node.payload {
                if !promql_fallback::raw_series(expression)
                    .map_err(|error| unsupported(error.to_string()))?
                    .is_empty()
                {
                    return Err(unsupported(
                        "query-time computation reads an exact PromQL selector".into(),
                    ));
                }
            }
            if let Some(SummaryNode {
                expr: SummaryExpr::BinaryOp { .. },
                guarantee,
                ..
            }) = semantic.map(Rc::as_ref)
            {
                // Planner certifies the combined value; without that, the
                // backend must not publish the arithmetic result.
                if guarantee.as_ref().is_none_or(|g| g.has_unknown()) {
                    return Err(unsupported(
                        "query-time arithmetic has no accuracy guarantee".into(),
                    ));
                }
            }
            computed.extend(semantic.cloned());
            pending.extend(
                dag.edges
                    .iter()
                    .filter(|edge| edge.consumer == id)
                    .map(|edge| edge.producer),
            );
            continue;
        }
        let semantic = semantic.expect("readout has a semantic node");
        let source = exact_accumulator_value_source(semantic).unwrap_or(semantic);
        if matches!(
            semantic.expr,
            SummaryExpr::SummaryJoin { .. }
                | SummaryExpr::SummarySubtract { .. }
                | SummaryExpr::SummaryDelete { .. }
        ) || (matches!(
            source.expr,
            SummaryExpr::SummaryAgg {
                family: SummaryFamilyType::ExactAggregate(
                    planner_types::post_asap::ExactKind::Count,
                    _
                ),
                ..
            }
        ) && !exact_value_executable(source))
            || node
                .output_schema
                .fields
                .iter()
                .any(|field| !matches!(field.dtype, SummaryFamilyType::Plain(_)))
        {
            return Err(unsupported(
                "query-time computation consumes state that has no decoded readout".into(),
            ));
        }
        contracts.insert(
            u64::from(id.0),
            InputContract::bounded(std::sync::Arc::new(node.output_schema.clone())),
        );
        frontier.insert(u64::from(id.0), Rc::clone(semantic));
    }
    // A pruning contract is bound only at the fragment root.
    if dag.nodes.iter().any(|node| {
        node.id != dag.root
            && matches!(
                node.payload,
                Payload::RelationalJoin {
                    pruning: Some(_),
                    ..
                }
            )
    }) {
        return Err(unsupported(
            "a pruned join must be the root of its query-time region".into(),
        ));
    }
    let physical = compile(dag, contracts, &[u64::from(dag.root.0)])
        .map_err(|error| unsupported(error.to_string()))?;
    let pruning = match &root.expr {
        SummaryExpr::RelationalJoin {
            left,
            right,
            pred,
            pruning: Some(completeness),
            ..
        } => {
            let candidates = dag
                .edges
                .iter()
                .find(|edge| edge.consumer == dag.root && edge.role == EdgeRole::Right)
                .map(|edge| u64::from(edge.producer.0))
                .ok_or_else(|| unsupported("pruned join has no candidate input".into()))?;
            Some(PruningInputContract {
                candidate_input: physical
                    .input_contracts()
                    .position(|(id, _)| id == candidates)
                    .ok_or_else(|| {
                        unsupported("pruning candidates must be a stored readout".into())
                    })?,
                keys: asap_physical_operators::physical_planner::equijoin_keys(
                    pred,
                    &left.schema,
                    &right.schema,
                )
                .map_err(|error| unsupported(error.to_string()))?,
                completeness: completeness.clone(),
            })
        }
        _ => None,
    };
    Ok(QueryComputation {
        physical,
        frontier,
        computed,
        pruning,
    })
}

/// A query-time exact aggregate yields accumulator state; its PromQL value is
/// that state finalized, as Planner does for stored exact states.
fn finalize_query_value(root: &Rc<SummaryNode>) -> Rc<SummaryNode> {
    use planner_types::pre_asap::DataType;
    if !matches!(
        root.expr,
        SummaryExpr::SummaryAgg {
            family: SummaryFamilyType::ExactAggregate(..),
            ..
        }
    ) {
        return Rc::clone(root);
    }
    let mut schema = root.schema.clone();
    for field in &mut schema.fields {
        if matches!(field.dtype, SummaryFamilyType::ExactAggregate(..)) {
            field.dtype = SummaryFamilyType::Plain(DataType::Float64);
        }
    }
    Rc::new(SummaryNode {
        expr: SummaryExpr::ValueOperation {
            child: Rc::clone(root),
            operation: planner_types::post_asap::ValueOperation::FinalizeExactAccumulator,
            timing: planner_types::post_asap::ExecutionTiming::QueryTime,
        },
        schema,
        guarantee: root.guarantee.clone(),
    })
}

struct DagCompiler<'a, F> {
    next_id: u64,
    nodes: BTreeMap<QueryNodeId, QueryPlanNode>,
    seen: BTreeMap<usize, QueryNodeId>,
    bind: &'a mut F,
    preserve_relational: bool,
    lowered: Option<&'a mut dyn FnMut(&Rc<SummaryNode>, QueryNodeId)>,
}

impl<F> DagCompiler<'_, F>
where
    F: FnMut(
        &Rc<SummaryNode>,
        &SummaryFamilyType,
    ) -> Result<MaterializationBinding, QueryPlanError>,
{
    fn lower_relation(&mut self, root: &Rc<SummaryNode>) -> Result<QueryNodeId, QueryPlanError> {
        use asap_physical_operators::physical_planner::{compile, InputContract};
        use planner_types::post_asap::{
            compile_post_asap_dag_with_node_ids, PostAsapOperatorPayload as Payload,
        };
        let compilation = compile_post_asap_dag_with_node_ids(root)
            .map_err(|e| QueryPlanError::Invalid(e.to_string()))?;
        let mut pending = vec![compilation.dag.root];
        let mut visited = std::collections::BTreeSet::new();
        let mut contracts = BTreeMap::new();
        let mut bindings = BTreeMap::new();
        let mut computed = Vec::new();
        while let Some(id) = pending.pop() {
            if !visited.insert(id) {
                continue;
            }
            let node = compilation
                .dag
                .nodes
                .iter()
                .find(|n| n.id == id)
                .ok_or_else(|| QueryPlanError::Invalid("missing Planner physical node".into()))?;
            let relation = matches!(
                &node.payload,
                Payload::RelationalJoin { .. }
                    | Payload::Value {
                        operation: planner_types::post_asap::ValueOperation::Project { .. }
                            | planner_types::post_asap::ValueOperation::Filter { .. }
                            | planner_types::post_asap::ValueOperation::Sort { .. }
                            | planner_types::post_asap::ValueOperation::Limit { .. }
                            | planner_types::post_asap::ValueOperation::Exact(
                                planner_types::post_asap::ExactOperation::Aggregate { .. }
                            )
                    }
            );
            if relation {
                computed.push(id);
                pending.extend(
                    compilation
                        .dag
                        .edges
                        .iter()
                        .filter(|e| e.consumer == id)
                        .map(|e| e.producer),
                );
            } else {
                let semantic = compilation.node_ids.summary_node(id).ok_or_else(|| {
                    QueryPlanError::Invalid("missing Planner source identity".into())
                })?;
                let source = self.lower(semantic)?;
                bindings.insert(u64::from(id.0), source);
                contracts.insert(
                    u64::from(id.0),
                    InputContract::bounded(std::sync::Arc::new(node.output_schema.clone())),
                );
            }
        }
        if computed.is_empty() {
            return self.lower(root);
        }
        let physical = compile(
            &compilation.dag,
            contracts,
            &[u64::from(compilation.dag.root.0)],
        )
        .map_err(|e| QueryPlanError::Invalid(e.to_string()))?;
        let id = QueryNodeId(self.next_id);
        self.next_id += 1;
        self.nodes.insert(
            id,
            QueryPlanNode::PhysicalRelation {
                inputs: physical
                    .input_contracts()
                    .map(|(id, _)| bindings[&id])
                    .collect(),
                dag: physical
                    .encode()
                    .map_err(|e| QueryPlanError::Invalid(e.to_string()))?,
            },
        );
        for node in computed {
            if let Some(semantic) = compilation.node_ids.summary_node(node) {
                self.seen.insert(Rc::as_ptr(semantic) as usize, id);
                if let Some(lowered) = &mut self.lowered {
                    lowered(semantic, id);
                }
            }
        }
        Ok(id)
    }

    /// One Planner physical fragment for the region, bound to backend readouts.
    fn lower_computation(&mut self, root: &Rc<SummaryNode>) -> Result<QueryNodeId, QueryPlanError> {
        let computation = compile_query_computation(root)?;
        let mut inputs = Vec::new();
        for (id, _) in computation.physical.input_contracts() {
            inputs.push(self.lower(&computation.frontier[&id])?);
        }
        let physical = &computation.physical;
        let row_input = physical
            .row_source(physical.roots()[0])
            .and_then(|source| physical.input_contracts().position(|(id, _)| id == source));
        let id = QueryNodeId(self.next_id);
        self.next_id += 1;
        self.nodes.insert(
            id,
            QueryPlanNode::PhysicalFragment {
                inputs,
                dag: physical
                    .encode()
                    .map_err(|e| QueryPlanError::Invalid(e.to_string()))?,
                row_input,
                pruning: computation.pruning,
            },
        );
        for semantic in std::iter::once(root).chain(&computation.computed) {
            self.seen.insert(Rc::as_ptr(semantic) as usize, id);
            if let Some(lowered) = &mut self.lowered {
                lowered(semantic, id);
            }
        }
        Ok(id)
    }

    fn lower(&mut self, node: &Rc<SummaryNode>) -> Result<QueryNodeId, QueryPlanError> {
        let identity = Rc::as_ptr(node) as usize;
        if let Some(id) = self.seen.get(&identity) {
            return Ok(*id);
        }
        if let SummaryExpr::ValueOperation {
            child,
            operation: planner_types::post_asap::ValueOperation::FinalizeExactAccumulator,
            ..
        } = &node.expr
        {
            if exact_accumulator_value_source(node).is_none() {
                return Err(QueryPlanError::UnsupportedNode(
                    "exact finalization requires a direct exact accumulator".into(),
                ));
            }
            // SummaryAgg lowering already emits the family-specific ExactReadout.
            // Preserve the Planner's explicit state boundary without adding a
            // second runtime readout node.
            let child_id = self.lower(child)?;
            self.seen.insert(identity, child_id);
            if let Some(lowered) = &mut self.lowered {
                lowered(node, child_id);
            }
            return Ok(child_id);
        }
        if !self.preserve_relational && is_query_computation(node) {
            return self.lower_computation(node);
        }
        let id = QueryNodeId(self.next_id);
        self.next_id += 1;
        self.seen.insert(identity, id);

        let physical = match &node.expr {
            SummaryExpr::RelationalJoin { .. } if self.preserve_relational => {
                return self.lower_relation(node);
            }
            SummaryExpr::ValueOperation { operation, .. }
                if self.preserve_relational
                    && matches!(
                        operation,
                        planner_types::post_asap::ValueOperation::Project { .. }
                            | planner_types::post_asap::ValueOperation::Filter { .. }
                            | planner_types::post_asap::ValueOperation::Sort { .. }
                            | planner_types::post_asap::ValueOperation::Limit { .. }
                            | planner_types::post_asap::ValueOperation::Exact(
                                planner_types::post_asap::ExactOperation::Aggregate { .. }
                            )
                    ) =>
            {
                return self.lower_relation(node);
            }
            SummaryExpr::ValueOperation { .. } => QueryPlanNode::ExactFallback {
                reason: "unsupported post-ASAP value operation".into(),
            },
            SummaryExpr::RelationalJoin { .. } => QueryPlanNode::ExactFallback {
                reason: "unsupported join in vector adapter".into(),
            },
            SummaryExpr::KeepPreAsap(expr) if self.preserve_relational => {
                let mut expression =
                    clickhouse_exact::render(expr).map_err(QueryPlanError::UnsupportedNode)?;
                let mut bounded = false;
                if let planner_types::pre_asap::QueryExpr::Scan {
                    predicates, schema, ..
                } = expr.as_ref()
                {
                    if predicates.is_empty() {
                        if let Some(column) = schema.time_index.and_then(|i| schema.columns.get(i))
                        {
                            let name = format!("`{}`", column.name.replace('`', "``"));
                            expression.push_str(&format!(
                                " WHERE {name} >= {{from:UInt64}} AND {name} <= {{to:UInt64}}"
                            ));
                            bounded = true;
                        }
                    }
                }
                QueryPlanNode::ExternalExact {
                    request: ExternalExactRequest {
                        language: QueryLanguage::ClickHouseSql,
                        expression,
                        output: ExternalExactOutput::Relation {
                            schema: serde_json::to_value(&node.schema).map_err(|error| {
                                QueryPlanError::Invalid(format!(
                                    "cannot serialize external exact schema: {error}"
                                ))
                            })?,
                        },
                        parameters: BTreeMap::new(),
                        start_parameter: bounded.then(|| "from".into()),
                        end_parameter: bounded.then(|| "to".into()),
                        input_contracts: Vec::new(),
                    },
                    inputs: Vec::new(),
                }
            }
            SummaryExpr::SummaryAgg {
                child,
                family,
                reduction,
                ..
            } if !matches!(child.expr, SummaryExpr::KeepPreAsap(_)) => {
                // A compiled immutable dependency is already materialized. Its
                // query reads that binding instead of replaying maintenance.
                match (self.bind)(node, family) {
                    Ok(mut binding) => {
                        binding.output_grouping = physical_grouping(reduction, child)?;
                        if let Some(readout) = exact_readout(family) {
                            let input = QueryNodeId(self.next_id);
                            self.next_id += 1;
                            self.nodes
                                .insert(input, QueryPlanNode::ReadMaterialization { binding });
                            QueryPlanNode::ExactReadout { input, readout }
                        } else {
                            QueryPlanNode::ReadMaterialization { binding }
                        }
                    }
                    Err(_) => QueryPlanNode::ExactFallback {
                        reason: "unsupported exact operation over summary output".into(),
                    },
                }
            }
            // Relational count bindings validate their row population and value
            // projection in the SQL compiler; the temporal restriction belongs
            // to the PromQL observation-count path.
            SummaryExpr::SummaryAgg {
                family:
                    SummaryFamilyType::ExactAggregate(planner_types::post_asap::ExactKind::Count, _),
                ..
            } if !self.preserve_relational && !exact_value_executable(node) => {
                QueryPlanNode::ExactFallback {
                    reason: "only temporal observation counts are supported".into(),
                }
            }
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
                    // A selected state without a deployed binding leaves the
                    // query to the exact engine.
                    let mut binding = (self.bind)(node, family)
                        .map_err(|error| QueryPlanError::UnsupportedNode(error.to_string()))?;
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
            SummaryExpr::SummaryMerge { children, .. } => {
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
        if let Some(lowered) = &mut self.lowered {
            lowered(node, id);
        }
        Ok(id)
    }
}

/// Does PromQL keep `__name__` on this query's result series? Only a series
/// selector keeps it, through ordering, selection, filtering, relabeling,
/// subqueries, `first_`/`last_over_time` and the left side of `and`/`unless`;
/// other functions, aggregations and arithmetic drop it. A comparison keeps it
/// unless it has `bool`, which the IR does not record; Planner compiles no
/// comparison, so the rule treats them as dropping it.
pub fn result_keeps_metric_name(expr: &planner_types::pre_asap::QueryExpr) -> bool {
    use planner_types::pre_asap::{AggIntent, BinaryOpKind, PromQLVectorSetOpKind, QueryExpr};
    match expr {
        QueryExpr::Scan { .. } => true,
        QueryExpr::TimeRange { child, .. }
        | QueryExpr::TimeShift { child, .. }
        | QueryExpr::Sort { child, .. }
        | QueryExpr::Limit { child, .. }
        | QueryExpr::Filter { child, .. }
        | QueryExpr::PromqlSeriesSample { child, .. }
        | QueryExpr::PromqlRelabel { child, .. }
        | QueryExpr::PromqlSubquery { child, .. } => result_keeps_metric_name(child),
        QueryExpr::BinaryOp {
            op: BinaryOpKind::Set(PromQLVectorSetOpKind::And | PromQLVectorSetOpKind::Unless),
            lhs,
            ..
        } => result_keeps_metric_name(lhs),
        QueryExpr::Aggregate {
            child, measures, ..
        } if matches!(
            measures.as_slice(),
            [AggIntent::LastOverTime | AggIntent::FirstOverTime]
        ) =>
        {
            result_keeps_metric_name(child)
        }
        _ => false,
    }
}

/// Parameters of each `kind` operator inside a physical fragment, for tests.
#[cfg(test)]
pub(crate) fn operator_parameters(node: &QueryPlanNode, kind: &str) -> Vec<serde_json::Value> {
    let QueryPlanNode::PhysicalFragment { dag, .. } = node else {
        return vec![];
    };
    asap_physical_operators::physical_planner::CompiledPhysicalDag::decode(dag).unwrap();
    let document: serde_json::Value = serde_json::from_slice(dag).unwrap();
    document["nodes"]
        .as_object()
        .unwrap()
        .values()
        .filter_map(|node| {
            node.get("Operator")?
                .get("operator")?
                .get("kind")?
                .get(kind)
                .cloned()
        })
        .collect()
}

fn exact_readout(family: &SummaryFamilyType) -> Option<ExactReadout> {
    use planner_types::post_asap::ExactKind;
    match family {
        SummaryFamilyType::ExactAggregate(ExactKind::Sum, _) => Some(ExactReadout::Sum),
        SummaryFamilyType::ExactAggregate(ExactKind::Count, _) => Some(ExactReadout::Count),
        SummaryFamilyType::ExactAggregate(ExactKind::Increase, _) => Some(ExactReadout::Increase),
        SummaryFamilyType::ExactAggregate(ExactKind::Rate, _) => Some(ExactReadout::Rate),
        SummaryFamilyType::ExactAggregate(ExactKind::Min, _) => Some(ExactReadout::Min),
        SummaryFamilyType::ExactAggregate(ExactKind::Max, _) => Some(ExactReadout::Max),
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

/// A value edge may explicitly finalize an exact accumulator at either
/// execution time. Inspect only this typed boundary; other value operations
/// cannot be treated as transparent producer identity.
pub(crate) fn exact_accumulator_value_source(node: &SummaryNode) -> Option<&SummaryNode> {
    let source = match &node.expr {
        SummaryExpr::ValueOperation {
            child,
            operation: planner_types::post_asap::ValueOperation::FinalizeExactAccumulator,
            ..
        } => child.as_ref(),
        _ => node,
    };
    matches!(
        source.expr,
        SummaryExpr::SummaryAgg {
            family: SummaryFamilyType::ExactAggregate(..),
            ..
        }
    )
    .then_some(source)
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
        SummaryExpr::ValueOperation { .. } => {
            exact_accumulator_value_source(node).is_some_and(exact_value_executable)
        }
        SummaryExpr::KeepPreAsap(expr) => scalar_literal(expr).is_some(),
        SummaryExpr::BinaryOp {
            lhs,
            rhs,
            operator,
            timing: planner_types::post_asap::ExecutionTiming::QueryTime,
        } => {
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
                        ExactKind::Sum
                            | ExactKind::Count
                            | ExactKind::Increase
                            | ExactKind::Rate
                            | ExactKind::Min
                            | ExactKind::Max
                    )
            } else {
                // Raw producer grouping may move through additive reductions,
                // but never through division or other value arithmetic.
                matches!(kind, ExactKind::Sum)
                    && exact_accumulator_value_source(child).is_some()
                    && exact_value_executable(child)
            }
        }
        _ => false,
    }
}

fn value_grouping(node: &SummaryNode) -> Result<Option<PhysicalGrouping>, QueryPlanError> {
    if matches!(node.expr, SummaryExpr::ValueOperation { .. }) {
        if let Some(source) = exact_accumulator_value_source(node) {
            return value_grouping(source);
        }
    }
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
        SummaryExpr::ValueOperation { .. } => {
            exact_accumulator_value_source(node).and_then(value_source)
        }
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

#[cfg(test)]
mod catalog_binding_tests {
    use super::*;
    use crate::physical::summary_catalog::SummaryCatalog;
    use asap_types::{AggregationType, KeyByLabelNames, PrecomputeMaterialization, WindowKind};

    /// A catalog over one `m` output grouped by `job`, with `kind` state.
    fn output(kind: AggregationType) -> (PrecomputeMaterialization, SummaryCatalog) {
        let family = kind.planner_exact_family().unwrap();
        let mut config = PrecomputeMaterialization::new(
            "m",
            KeyByLabelNames::new(vec!["job".into()]),
            10,
            10,
            WindowKind::Tumbling,
        );
        config.pane_origin_ms = Some(0);
        config.allocate_stored_output_id(&family);
        let catalog =
            SummaryCatalog::from_outputs(7, 2, vec![(&config, &family, String::new())]).unwrap();
        (config, catalog)
    }

    fn fixture() -> (QueryPlan, SummaryCatalog) {
        let (config, catalog) = output(AggregationType::Sum);
        let entry = QueryPlanEntry {
            physical_dag: None,
            language: crate::query_plan::QueryLanguage::PromQl,
            query_id: "q".into(),
            canonical_query: "sum_over_time(m[1m])".into(),
            fixed_evaluation: None,
            root: QueryNodeId(1),
            nodes: BTreeMap::from([(
                QueryNodeId(1),
                QueryPlanNode::ReadMaterialization {
                    binding: MaterializationBinding {
                        full_window_slide_ms: None,
                        item_labels: Vec::new(),
                        materialization: config.policy_fingerprint().into(),
                        stored_output_reference: catalog
                            .output_reference(config.policy_fingerprint().into())
                            .unwrap(),
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
                clickhouse_context: None,
                selected_dags: Default::default(),
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
        let wire = serde_json::to_vec(&plan).unwrap();
        let mut decoded: QueryPlan = serde_json::from_slice(&wire).unwrap();
        decoded.validate_against_catalog(&catalog).unwrap();
        assert_eq!(
            decoded
                .lookup("sum_over_time(m[1m])")
                .unwrap()
                .canonical_query,
            "sum_over_time(m[1m])"
        );
        assert!(String::from_utf8(wire).unwrap().contains("canonical_query"));
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

        let (counter, counter_catalog) = output(AggregationType::Increase);
        let (mut counter_plan, _) = fixture();
        binding(&mut counter_plan).materialization = counter.policy_fingerprint().into();
        binding(&mut counter_plan).stored_output_reference = counter_catalog
            .output_reference(counter.policy_fingerprint().into())
            .unwrap();
        as_rate_plan(counter_plan)
            .validate_against_catalog(&counter_catalog)
            .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Planner compiles guarded per-series average division into a physical fragment.
    #[test]
    fn per_series_guarded_division_uses_planner_fragment() {
        let query = "avg_over_time(m[5m])";
        let canonical = crate::query_parser::parse_query_expr_canonical(
            query,
            planner_types::types::AccuracyTarget::Exact,
        )
        .unwrap();
        let root = crate::planner_selection::plan_test_query(&canonical).unwrap();
        let SummaryExpr::BinaryOp { operator, .. } = &root.expr else {
            panic!("expected the Planner's average rewrite");
        };
        assert!(operator.checked_finite_division);
        let entry = compile_bound_mapped(
            "guarded".into(),
            query.into(),
            &root,
            InstantExecution {
                lookback_ms: 300_000,
                full_history: false,
                cumulative_readout: false,
            },
            FallbackPolicy::ExactBackend,
            |_: &Rc<SummaryNode>, _: &SummaryFamilyType| {
                Ok(MaterializationBinding {
                    full_window_slide_ms: None,
                    materialization: PolicyFingerprint(7).into(),
                    stored_output_reference: asap_types::sds::StoredOutputReference::for_output(
                        PolicyFingerprint(7).into(),
                    ),
                    output_grouping: PhysicalGrouping::PerEntity,
                    window_ms: 300_000,
                    pane_origin_ms: Some(0),
                    readout_lookback_ms: Some(300_000),
                    item_labels: Vec::new(),
                })
            },
            |_, _| {},
        )
        .unwrap();
        let QueryPlanNode::PhysicalFragment { dag, .. } = &entry.nodes[&entry.root] else {
            panic!("expected compiled Planner fragment")
        };
        let document: serde_json::Value = serde_json::from_slice(dag).unwrap();
        assert!(document
            .to_string()
            .contains("\"checked_finite_division\":true"));
    }

    // Every o11y corpus query compiles as backend readouts under Planner
    // fragments, or forwards whole; no backend value operator remains.
    #[test]
    fn o11y_corpus_computes_only_through_planner_fragments() {
        use crate::physical::compiler::{BackendLocalPlanningInput, DeploymentPlanCompiler};
        let corpus: serde_json::Value =
            serde_json::from_str(include_str!("../tests/fixtures/o11y_queries.json")).unwrap();
        let mut fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../docs/examples/asapquery-planning-snapshot.json"
        ))
        .unwrap();
        let template = fixture["query_workload"]["repeating_queries"][0].clone();
        let queries = corpus["queries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["query"].as_str().unwrap())
            .collect::<BTreeSet<_>>();
        fixture["query_workload"]["repeating_queries"] = queries
            .iter()
            .map(|query| {
                let mut entry = template.clone();
                entry["query"] = (*query).into();
                entry["requirements"]["accuracy"] = serde_json::json!({"explicit":"Exact"});
                entry
            })
            .collect::<Vec<_>>()
            .into();
        let snapshot: BackendLocalPlanningInput = serde_json::from_value(fixture).unwrap();
        let (request, environment) = snapshot.into_physical_compilation_request().unwrap();
        let plan = DeploymentPlanCompiler
            .compile_promql(request, environment)
            .unwrap();
        assert_eq!(plan.query_plan.entries.len(), queries.len());
        let mut fragments = 0;
        for entry in plan.query_plan.entries.values() {
            for node in entry.nodes.values() {
                match node {
                    QueryPlanNode::PhysicalFragment { .. } => fragments += 1,
                    QueryPlanNode::ReadMaterialization { .. }
                    | QueryPlanNode::ExactReadout { .. }
                    | QueryPlanNode::SummaryEstimate { .. }
                    | QueryPlanNode::SummaryMerge { .. } => {}
                    QueryPlanNode::ExactFallback { .. } => assert_eq!(entry.nodes.len(), 1),
                    other => panic!("{}: unexpected {other:?}", entry.canonical_query),
                }
            }
        }
        assert!(fragments > 0);
    }

    // Instant counts have no local exact readout, so a computation over one
    // is not compiled locally; count_over_time keeps its readout.
    #[test]
    fn computation_over_instant_count_is_not_local() {
        let selected = |query: &str| {
            let canonical = crate::query_parser::parse_query_expr_canonical(
                query,
                planner_types::types::AccuracyTarget::Exact,
            )
            .unwrap();
            crate::planner_selection::plan_test_query(&canonical).unwrap()
        };
        let instant = selected("count(m) * 2");
        assert!(is_query_computation(&instant));
        let Err(error) = compile_query_computation(&instant) else {
            panic!("instant count computation compiled locally");
        };
        assert!(error.to_string().contains("no decoded readout"), "{error}");
        let temporal = selected("sum(count_over_time(m[5m])) * 2");
        assert!(is_query_computation(&temporal));
        compile_query_computation(&temporal).unwrap();
    }

    // PromQL keeps `__name__` only on selections of a series selector.
    #[test]
    fn metric_name_follows_promql_result_rules() {
        for (query, keeps) in [
            ("m", true),
            ("m offset 5m", true),
            ("sort(m)", true),
            ("topk(2, m)", true),
            ("last_over_time(m[5m])", true),
            ("m and n", true),
            ("rate(m[5m])", false),
            ("quantile_over_time(0.5, m[5m])", false),
            ("m * 2", false),
            ("sum by (job) (m)", false),
            ("topk(2, rate(m[5m]))", false),
            ("max_over_time(rate(m[1m])[5m:1m])", false),
        ] {
            let expr = crate::query_parser::parse_query_expr_canonical(
                query,
                planner_types::types::AccuracyTarget::Exact,
            )
            .unwrap();
            assert_eq!(result_keeps_metric_name(&expr), keeps, "{query}");
        }
    }

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
            physical_dag: None,
            language: crate::query_plan::QueryLanguage::PromQl,
            query_id: "q".into(),
            canonical_query: canonical_promql("up").unwrap(),
            fixed_evaluation: None,
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
    fn language_catalog_keys_keep_equal_query_text_distinct() {
        let base = QueryPlanEntry {
            physical_dag: None,
            language: QueryLanguage::PromQl,
            query_id: "prom".into(),
            canonical_query: "shared".into(),
            fixed_evaluation: None,
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
        let mut metricsql = base.clone();
        metricsql.language = QueryLanguage::MetricsQl;
        metricsql.query_id = "metrics".into();
        let mut clickhouse = base.clone();
        clickhouse.language = QueryLanguage::ClickHouseSql;
        clickhouse.query_id = "sql".into();
        clickhouse.fixed_evaluation = Some(FixedEvaluationRange {
            start_ms: 1,
            end_ms: 2,
            cumulative: true,
        });
        let plan = QueryPlan {
            plan_id: 0,
            plan_version: 0,
            clickhouse_context: Some(ClickHousePlanningContext {
                window_templates: Default::default(),
                tables: Default::default(),
                accuracy: planner_types::types::AccuracyTarget::Exact,
            }),
            selected_dags: Default::default(),
            entries: [base, metricsql, clickhouse]
                .into_iter()
                .map(|entry| (QueryPlan::catalog_key(entry.language, "shared"), entry))
                .collect(),
        };

        assert_eq!(plan.entries.len(), 3);
        assert_eq!(
            plan.lookup_canonical(QueryLanguage::PromQl, "shared")
                .unwrap()
                .query_id,
            "prom"
        );
        assert_eq!(
            plan.lookup_canonical(QueryLanguage::MetricsQl, "shared")
                .unwrap()
                .query_id,
            "metrics"
        );
        assert_eq!(plan.lookup_clickhouse("shared").unwrap().query_id, "sql");
    }

    // A protocol-vector binding cannot silently invent fields or reconstruct changed rows.
    #[test]
    fn physical_binding_rejects_invented_samples_and_changed_rows() {
        use asap_physical_operators::{
            operators::{Expression, Operator},
            physical_planner::{CompiledPhysicalDag, InputContract},
        };
        use planner_types::{
            post_asap::{SummaryFamilyType, SummaryField, SummarySchema},
            pre_asap::DataType,
        };
        for duplicate_sample in [false, true] {
            let schema = std::sync::Arc::new(SummarySchema {
                fields: (0..if duplicate_sample { 2 } else { 1 })
                    .map(|i| SummaryField {
                        name: format!("v{i}"),
                        dtype: SummaryFamilyType::Plain(DataType::Float64),
                        nullable: false,
                    })
                    .collect(),
                time_index: None,
            });
            let op = if duplicate_sample {
                Operator::limit(schema.clone(), 1, 0, vec![]).unwrap()
            } else {
                Operator::project(schema.clone(), vec![("v0".into(), Expression::Column(0))])
                    .unwrap()
            };
            let compiled = CompiledPhysicalDag::from_operators(
                [(0, InputContract::bounded(schema))].into(),
                [(1, (vec![0], op))].into(),
                vec![1],
            )
            .unwrap();
            let entry = QueryPlanEntry {
                physical_dag: None,
                language: QueryLanguage::PromQl,
                query_id: "q".into(),
                canonical_query: "topk(1, m)".into(),
                fixed_evaluation: None,
                root: QueryNodeId(1),
                nodes: BTreeMap::from([
                    (
                        QueryNodeId(0),
                        QueryPlanNode::ExactFallback {
                            reason: "prepared source".into(),
                        },
                    ),
                    (
                        QueryNodeId(1),
                        QueryPlanNode::PhysicalFragment {
                            inputs: vec![QueryNodeId(0)],
                            dag: compiled.encode().unwrap(),
                            row_input: Some(0),
                            pruning: None,
                        },
                    ),
                ]),
                instant: InstantExecution {
                    lookback_ms: 0,
                    full_history: false,
                    cumulative_readout: false,
                },
                fallback: FallbackPolicy::Reject,
            };
            let error = entry.validate(&BTreeSet::new()).unwrap_err().to_string();
            assert!(
                error.contains(if duplicate_sample {
                    "one numeric sample"
                } else {
                    "preserve its bound input rows"
                }),
                "{error}"
            );
        }
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
            physical_dag: None,
            language: crate::query_plan::QueryLanguage::PromQl,
            query_id: "q".into(),
            canonical_query: "up".into(),
            fixed_evaluation: None,
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
    fn semi_join_rejects_invalid_completeness_contract() {
        let leaf = QueryPlanNode::ExactFallback {
            reason: "prepared".into(),
        };
        let entry = QueryPlanEntry {
            physical_dag: None,
            language: crate::query_plan::QueryLanguage::PromQl,
            query_id: "q".into(),
            canonical_query: "topk(2, rate(m[5m]))".into(),
            fixed_evaluation: None,
            root: QueryNodeId(2),
            nodes: BTreeMap::from([
                (QueryNodeId(0), leaf.clone()),
                (QueryNodeId(1), leaf),
                (QueryNodeId(2), {
                    let schema = planner_types::post_asap::SummarySchema {
                        fields: vec![planner_types::post_asap::SummaryField {
                            name: "pod".into(),
                            dtype: planner_types::post_asap::SummaryFamilyType::Plain(
                                planner_types::pre_asap::DataType::Utf8,
                            ),
                            nullable: false,
                        }],
                        time_index: None,
                    };
                    {
                        let schemas = vec![
                            std::sync::Arc::new(schema.clone()),
                            std::sync::Arc::new(schema.clone()),
                        ];
                        let node = planner_types::post_asap::PostAsapDagNode {
                            id: planner_types::post_asap::PostAsapNodeId(2),
                            payload:
                                planner_types::post_asap::PostAsapOperatorPayload::RelationalJoin {
                                    join_kind: planner_types::pre_asap::JoinKind::Semi,
                                    pred: serde_json::from_value(
                                        serde_json::to_value(planner_types::pre_asap::Predicate(
                                            std::rc::Rc::new(
                                                planner_types::pre_asap::QueryExpr::Compare {
                                                    left: std::rc::Rc::new(
                                                        planner_types::pre_asap::QueryExpr::Column(
                                                            0,
                                                        ),
                                                    ),
                                                    op: planner_types::pre_asap::CompareOpKind::Eq,
                                                    right: std::rc::Rc::new(
                                                        planner_types::pre_asap::QueryExpr::Column(
                                                            1,
                                                        ),
                                                    ),
                                                },
                                            ),
                                        ))
                                        .unwrap(),
                                    )
                                    .unwrap(),
                                    pruning: None,
                                },
                            output_state: planner_types::post_asap::ExecutionDataState::QUERY_ROWS,
                            output_schema: schema,
                            guarantee: None,
                        };
                        let operator = asap_physical_operators::physical_planner::compile_node(
                            &node, &schemas,
                        )
                        .unwrap();
                        let compiled = asap_physical_operators::physical_planner::CompiledPhysicalDag::from_operators(
                schemas.into_iter().enumerate().map(|(id, schema)| (id as u64, asap_physical_operators::physical_planner::InputContract::bounded(schema))).collect(),
                [(2, ((0..2).collect(), operator))].into(), vec![2],
            ).unwrap();
                        QueryPlanNode::PhysicalFragment {
                            inputs: [QueryNodeId(1), QueryNodeId(0)].to_vec(),
                            dag: compiled.encode().unwrap(),
                            row_input: Some(0),
                            pruning: (Some(CandidateCompleteness::Certified {
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
                            }))
                            .map(|completeness| {
                                asap_types::query_plan::PruningInputContract {
                                    candidate_input: 1,
                                    keys: vec![(0, 0)],
                                    completeness,
                                }
                            }),
                        }
                    }
                }),
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
