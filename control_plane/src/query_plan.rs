//! Control-plane lowering from Planner IR to the shared installed query DAG.
//! Serving consumes asap_types::query_plan; compilation stays in this component.

mod clickhouse_exact;
pub mod logical;
pub use asap_types::query_plan::*;
#[cfg(test)]
use asap_types::PolicyFingerprint;
use planner_types::post_asap::{SummaryExpr, SummaryFamilyType, SummaryNode};
use planner_types::pre_asap::Reduction;
use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::BTreeSet;
use std::rc::Rc;

pub fn compile_bound<F>(
    query_id: String,
    canonical_query: String,
    root: &Rc<SummaryNode>,
    instant: InstantExecution,
    fallback: FallbackPolicy,
    bind: F,
) -> Result<QueryPlanEntry, QueryPlanError>
where
    F: FnMut(
        &Rc<SummaryNode>,
        &SummaryFamilyType,
    ) -> Result<MaterializationBinding, QueryPlanError>,
{
    compile_bound_mapped(
        query_id,
        canonical_query,
        root,
        instant,
        fallback,
        bind,
        |_, _| {},
    )
}

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
        logical_source: None,
        preserve_relational: false,
        lowered: Some(&mut lowered),
    };
    let root = compiler.lower(root)?;
    Ok(QueryPlanEntry {
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

pub fn compile_bound_relational<F>(
    query_id: String,
    canonical_query: String,
    root: &Rc<SummaryNode>,
    fixed_evaluation: FixedEvaluationRange,
    instant: InstantExecution,
    fallback: FallbackPolicy,
    bind: F,
) -> Result<QueryPlanEntry, QueryPlanError>
where
    F: FnMut(
        &Rc<SummaryNode>,
        &SummaryFamilyType,
    ) -> Result<MaterializationBinding, QueryPlanError>,
{
    compile_bound_relational_mapped(
        query_id,
        canonical_query,
        root,
        fixed_evaluation,
        instant,
        fallback,
        bind,
        |_, _| {},
    )
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
        logical_source: None,
        preserve_relational: true,
        lowered: Some(&mut lowered),
    };
    let root = compiler.lower(root)?;
    Ok(QueryPlanEntry {
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

/// Compile selected summary nodes and verified native residuals into one DAG.
/// This is a distinct physical alternative; native execution remains available.
pub fn compile_bound_composable<F>(
    query_id: String,
    canonical_query: String,
    root: &Rc<SummaryNode>,
    instant: InstantExecution,
    fallback: FallbackPolicy,
    bind: F,
) -> Result<QueryPlanEntry, QueryPlanError>
where
    F: FnMut(
        &Rc<SummaryNode>,
        &SummaryFamilyType,
    ) -> Result<MaterializationBinding, QueryPlanError>,
{
    compile_bound_composable_mapped(
        query_id,
        canonical_query,
        root,
        instant,
        fallback,
        bind,
        |_, _| {},
    )
}

/// Compile a composable query while exposing the stable mapping from
/// Planner semantic nodes to installed query nodes. The control-plane
/// physical compiler uses this to persist backend placement without
/// relying on pointer values or reconstructing query shape later.
pub fn compile_bound_composable_mapped<F, G>(
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
        logical_source: Some(canonical_query.clone()),
        preserve_relational: false,
        lowered: Some(&mut lowered),
    };
    let root = compiler.lower(root)?;
    let mut entry = QueryPlanEntry {
        language: QueryLanguage::PromQl,
        query_id,
        canonical_query,
        fixed_evaluation: None,
        root,
        nodes: compiler.nodes,
        instant,
        fallback,
    };
    logical::finalize_residuals(&mut entry)?;
    Ok(entry)
}

struct DagCompiler<'a, F> {
    next_id: u64,
    nodes: BTreeMap<QueryNodeId, QueryPlanNode>,
    seen: BTreeMap<usize, QueryNodeId>,
    bind: &'a mut F,
    logical_source: Option<String>,
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
                QueryPlanNode::Logical { inputs, .. }
                | QueryPlanNode::SummaryMerge { inputs }
                | QueryPlanNode::ExternalExact { inputs, .. } => {
                    for input in inputs {
                        *input = remap[input];
                    }
                }
                QueryPlanNode::CandidateTopK { inputs, .. }
                | QueryPlanNode::Binary { inputs, .. }
                | QueryPlanNode::RelationalJoin { inputs, .. } => {
                    for input in inputs {
                        *input = remap[input];
                    }
                }
                QueryPlanNode::SummaryEstimate { input, .. }
                | QueryPlanNode::ExactReadout { input, .. }
                | QueryPlanNode::ReduceSum { input, .. }
                | QueryPlanNode::Relational { input, .. } => *input = remap[input],
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
            let id = self.graft(id, root, nodes)?;
            if let Some(lowered) = &mut self.lowered {
                lowered(node, id);
            }
            return Ok(id);
        }

        let physical = match &node.expr {
            SummaryExpr::RelationalJoin {
                left,
                right,
                kind,
                pred,
            } if self.preserve_relational => QueryPlanNode::RelationalJoin {
                inputs: [self.lower(left)?, self.lower(right)?],
                join_kind: kind.clone(),
                pred: serde_json::to_value(pred).map_err(|error| {
                    QueryPlanError::Invalid(format!(
                        "cannot serialize relational join predicate: {error}"
                    ))
                })?,
                left_schema: left.schema.clone(),
                right_schema: right.schema.clone(),
                output_schema: node.schema.clone(),
            },
            SummaryExpr::RelationalJoin { .. } => QueryPlanNode::ExactFallback {
                reason: "read-time relational join requires the relational compiler".into(),
            },
            SummaryExpr::ValueOperation {
                child, operation, ..
            } if self.preserve_relational
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
                QueryPlanNode::Relational {
                    input: self.lower(child)?,
                    operation: serde_json::to_value(operation).map_err(|error| {
                        QueryPlanError::UnsupportedNode(format!(
                            "cannot serialize relational operation: {error}"
                        ))
                    })?,
                    input_schema: child.schema.clone(),
                    output_schema: node.schema.clone(),
                }
            }
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
                let candidate_input = self.lower(candidates)?;
                let value_input = if let Some(original) = &self.logical_source {
                    let parsed = promql_parser::parser::parse(original)
                        .map_err(|error| QueryPlanError::Invalid(error.to_string()))?;
                    let promql_parser::parser::Expr::Aggregate(aggregate) = parsed else {
                        return Err(QueryPlanError::Invalid(
                            "CandidateTopK requires a top-level PromQL aggregate".into(),
                        ));
                    };
                    if aggregate.op.to_string() != "topk" {
                        return Err(QueryPlanError::Invalid(
                            "CandidateTopK requires a topk source expression".into(),
                        ));
                    }
                    fn item_label(node: &SummaryNode) -> Option<String> {
                        match &node.expr {
                            SummaryExpr::SummaryEstimate { summary_input, .. } => {
                                item_label(summary_input)
                            }
                            SummaryExpr::SummaryAgg { input, .. } => match &input.item {
                                Some(planner_types::post_asap::SummaryInputExpr::Column(
                                    planner_types::pre_asap::ColumnRef::Named(label),
                                )) => Some(label.clone()),
                                Some(planner_types::post_asap::SummaryInputExpr::Column(
                                    planner_types::pre_asap::ColumnRef::Qualified { name, .. },
                                )) => Some(name.clone()),
                                _ => None,
                            },
                            _ => None,
                        }
                    }
                    let item_label = item_label(candidates).ok_or_else(|| {
                        QueryPlanError::Invalid(
                            "CandidateTopK membership has no named item label".into(),
                        )
                    })?;
                    let value_id = QueryNodeId(self.next_id);
                    self.next_id += 1;
                    self.nodes.insert(
                        value_id,
                        QueryPlanNode::ExternalExact {
                            request: ExternalExactRequest {
                                language: QueryLanguage::PromQl,
                                expression: aggregate.expr.to_string(),
                                output: ExternalExactOutput::InstantVector,
                                parameters: BTreeMap::new(),
                                start_parameter: None,
                                end_parameter: None,
                                input_contracts: vec![ExternalExactInput::CandidateMembership {
                                    item_label,
                                }],
                            },
                            inputs: vec![candidate_input],
                        },
                    );
                    value_id
                } else {
                    self.lower(values)?
                };
                QueryPlanNode::CandidateTopK {
                    inputs: [candidate_input, value_input],
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
            SummaryExpr::BinaryOp {
                lhs,
                rhs,
                operator,
                timing: planner_types::post_asap::ExecutionTiming::ReadTime,
            } if self.logical_source.is_some() => {
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
            SummaryExpr::BinaryOp {
                lhs,
                rhs,
                operator,
                timing: planner_types::post_asap::ExecutionTiming::ReadTime,
            } if exact_value_executable(node) => {
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
                    let mut binding = match (self.bind)(node, family) {
                        Ok(binding) => binding,
                        Err(error) => {
                            if let Some(original) = &self.logical_source {
                                let (root, nodes) =
                                    logical::selected_residual_nodes(original, node)?;
                                return self.graft(id, root, nodes);
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
        if let Some(lowered) = &mut self.lowered {
            lowered(node, id);
        }
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
            timing: planner_types::post_asap::ExecutionTiming::ReadTime,
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
                        ExactKind::Sum | ExactKind::Count | ExactKind::Increase | ExactKind::Rate
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
                tables: Default::default(),
                accuracy: planner_types::types::AccuracyTarget::Exact,
            }),
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
    fn candidate_topk_rejects_invalid_completeness_contract() {
        let leaf = QueryPlanNode::ExactFallback {
            reason: "prepared".into(),
        };
        let entry = QueryPlanEntry {
            language: crate::query_plan::QueryLanguage::PromQl,
            query_id: "q".into(),
            canonical_query: "topk(2, rate(m[5m]))".into(),
            fixed_evaluation: None,
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
            fixed_evaluation: None,
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
                clickhouse_context: None,
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
