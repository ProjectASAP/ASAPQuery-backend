//! Retain Planner-compiled scalar/vector fragments before publishing an installed plan.
use super::*;
use asap_physical_operators::physical_planner::{promql_values as physical, CompiledPhysicalDag};
use planner_types::{
    post_asap::BinaryOperator,
    pre_asap::{AggIntent, ArithmeticOpKind, BinaryOpKind, ColumnRef, CompareOpKind, GroupKeys},
};
use residual::{Aggregation, BinaryOperation, Grouping, ResidualQueryOperator as Operation};

fn invalid(error: impl std::fmt::Display) -> QueryPlanError {
    QueryPlanError::Invalid(error.to_string())
}
fn grouping(value: &Grouping) -> GroupKeys<ColumnRef> {
    let keys = value.labels.iter().cloned().map(ColumnRef::Named).collect();
    if value.without {
        GroupKeys::without(keys)
    } else {
        GroupKeys::by(keys)
    }
}
fn binary(operation: BinaryOperation) -> BinaryOperator {
    use ArithmeticOpKind as A;
    use BinaryOperation as O;
    use CompareOpKind as C;
    let kind = match operation {
        O::Add => BinaryOpKind::Arithmetic(A::Add),
        O::Sub => BinaryOpKind::Arithmetic(A::Sub),
        O::Mul => BinaryOpKind::Arithmetic(A::Mul),
        O::Div | O::CheckedDiv | O::FiniteDiv => BinaryOpKind::Arithmetic(A::Div),
        O::Mod => BinaryOpKind::Arithmetic(A::Mod),
        O::Pow => BinaryOpKind::Arithmetic(A::Pow),
        O::Equal => BinaryOpKind::Compare(C::Eq),
        O::NotEqual => BinaryOpKind::Compare(C::Ne),
        O::Less => BinaryOpKind::Compare(C::Lt),
        O::LessEqual => BinaryOpKind::Compare(C::Le),
        O::Greater => BinaryOpKind::Compare(C::Gt),
        O::GreaterEqual => BinaryOpKind::Compare(C::Ge),
    };
    BinaryOperator {
        kind,
        vector_match: None,
        checked_relative_division: operation == O::CheckedDiv,
        checked_finite_division: operation == O::FiniteDiv,
    }
}

pub fn compile(
    entry: &mut QueryPlanEntry,
) -> Result<BTreeMap<QueryNodeId, QueryNodeId>, QueryPlanError> {
    if entry.language == QueryLanguage::ClickHouseSql || entry.physical_dag.is_some() {
        return Ok(BTreeMap::new());
    }
    let mut scalars = BTreeMap::new();
    for id in entry.topological_order()? {
        let node = &entry.nodes[&id];
        let inputs = node.inputs().to_vec();
        let scalar_input = |position: usize| {
            inputs
                .get(position)
                .and_then(|id| scalars.get(id))
                .copied()
                .unwrap_or(false)
        };
        let mut scalar = false;
        let compiled = match node {
            QueryPlanNode::Scalar { .. } => {
                scalar = true;
                let QueryPlanNode::Scalar { value } = node else {
                    unreachable!()
                };
                Some(physical::compile_scalar(*value).map_err(invalid)?)
            }
            QueryPlanNode::Logical {
                operator:
                    Operation::ExactSubquery { query } | Operation::CandidateExactSubquery { query, .. },
                ..
            } => {
                scalar = promql_parser::parser::parse(query)
                    .map_err(invalid)?
                    .value_type()
                    == promql_parser::parser::value::ValueType::Scalar;
                None
            }
            QueryPlanNode::Logical { operator, .. } => match operator {
                Operation::Binary {
                    operation,
                    return_bool,
                } => {
                    scalar = scalar_input(0) && scalar_input(1);
                    Some(
                        physical::compile_binary(
                            &binary(*operation),
                            *return_bool,
                            scalar_input(0),
                            scalar_input(1),
                        )
                        .map_err(invalid)?,
                    )
                }
                Operation::UnaryNegate => {
                    scalar = scalar_input(0);
                    Some(physical::compile_negate(scalar).map_err(invalid)?)
                }
                Operation::VectorToScalar => {
                    scalar = true;
                    Some(physical::compile_vector_to_scalar().map_err(invalid)?)
                }
                Operation::Aggregate {
                    operation,
                    grouping: groups,
                } => {
                    let intent = match operation {
                        Aggregation::Sum => AggIntent::Sum { col: None },
                        Aggregation::Avg => AggIntent::Avg { col: None },
                        Aggregation::Min => AggIntent::Min { col: None },
                        Aggregation::Max => AggIntent::Max { col: None },
                        Aggregation::Count => AggIntent::Count {
                            accuracy: planner_types::types::AccuracyTarget::Exact,
                        },
                    };
                    Some(physical::compile_aggregate(&intent, &grouping(groups)).map_err(invalid)?)
                }
                Operation::Sort {
                    descending,
                    grouping: groups,
                } => Some(physical::compile_sort(*descending, &grouping(groups)).map_err(invalid)?),
                Operation::Limit {
                    n,
                    offset,
                    grouping: groups,
                } => {
                    Some(physical::compile_limit(*n, *offset, &grouping(groups)).map_err(invalid)?)
                }
                Operation::Temporal { operation } => {
                    use residual::TemporalOperation as T;
                    let intent = match operation {
                        T::Rate => AggIntent::Rate,
                        T::Increase => AggIntent::Increase,
                        T::Sum => AggIntent::Sum { col: None },
                        T::Avg => AggIntent::Avg { col: None },
                        T::Min => AggIntent::Min { col: None },
                        T::Max => AggIntent::Max { col: None },
                        T::Count => AggIntent::Count {
                            accuracy: planner_types::types::AccuracyTarget::Exact,
                        },
                    };
                    let preserve = entry.language == QueryLanguage::MetricsQl
                        && matches!(operation, T::Min | T::Max | T::Avg);
                    Some(physical::compile_temporal(&intent, preserve).map_err(invalid)?)
                }
                Operation::HistogramQuantile => {
                    Some(physical::compile_histogram_quantile().map_err(invalid)?)
                }
                _ => None,
            },
            QueryPlanNode::ReduceSum {
                grouping: groups, ..
            } => Some(match groups {
                PhysicalGrouping::Reduce(labels) => physical::compile_aggregate(
                    &AggIntent::Sum { col: None },
                    &GroupKeys::by(labels.iter().cloned().map(ColumnRef::Named).collect()),
                )
                .map_err(invalid)?,
                PhysicalGrouping::PerEntity => CompiledPhysicalDag::from_operators(
                    BTreeMap::from([(
                        0,
                        asap_physical_operators::physical_planner::InputContract::bounded(
                            physical::vector_schema(),
                        ),
                    )]),
                    BTreeMap::from([(
                        1,
                        (
                            vec![0],
                            asap_physical_operators::operators::Operator::project(
                                physical::vector_schema(),
                                vec![
                                    (
                                        "labels".into(),
                                        asap_physical_operators::expressions::Expression::Column(0),
                                    ),
                                    (
                                        "value".into(),
                                        asap_physical_operators::expressions::Expression::Column(1),
                                    ),
                                ],
                            )
                            .map_err(invalid)?,
                        ),
                    )]),
                    vec![1],
                )
                .map_err(invalid)?,
            }),
            QueryPlanNode::Binary { operator, .. } => {
                scalar = scalar_input(0) && scalar_input(1);
                Some(
                    physical::compile_binary(
                        &BinaryOperator {
                            kind: BinaryOpKind::Arithmetic(operator.clone()),
                            vector_match: None,
                            checked_relative_division: false,
                            checked_finite_division: false,
                        },
                        false,
                        scalar_input(0),
                        scalar_input(1),
                    )
                    .map_err(invalid)?,
                )
            }
            QueryPlanNode::PhysicalFragment { dag, .. } => {
                let compiled = CompiledPhysicalDag::decode(dag).map_err(invalid)?;
                scalar = compiled
                    .output_contract(compiled.roots()[0])
                    .map_err(invalid)?
                    .schema
                    == physical::scalar_schema();
                None
            }
            _ => None,
        };
        scalars.insert(id, scalar);
        if let Some(compiled) = compiled {
            entry.nodes.insert(
                id,
                QueryPlanNode::PhysicalFragment {
                    inputs,
                    dag: compiled.encode().map_err(invalid)?,
                    row_input: None,
                    pruning: None,
                },
            );
        }
    }
    combine(entry)
}

fn combine(
    entry: &mut QueryPlanEntry,
) -> Result<BTreeMap<QueryNodeId, QueryNodeId>, QueryPlanError> {
    use asap_physical_operators::physical_planner::InputContract;
    use std::collections::BTreeSet;
    let supported = |schema: &asap_physical_operators::values::Schema| {
        schema == &physical::scalar_schema()
            || schema == &physical::vector_schema()
            || schema == &physical::matrix_schema()
    };
    let mut programs = BTreeMap::new();
    for (&id, node) in &entry.nodes {
        if let QueryPlanNode::PhysicalFragment {
            dag,
            row_input: None,
            pruning: None,
            ..
        } = node
        {
            let graph = CompiledPhysicalDag::decode(dag).map_err(invalid)?;
            if graph.roots().len() == 1
                && graph
                    .input_contracts()
                    .all(|(_, input)| supported(&input.schema))
                && supported(
                    &graph
                        .output_contract(graph.roots()[0])
                        .map_err(invalid)?
                        .schema,
                )
            {
                programs.insert(id, graph);
            }
        }
    }
    let mut roots = BTreeSet::new();
    if programs.contains_key(&entry.root) {
        roots.insert(entry.root);
    }
    for (id, node) in &entry.nodes {
        if !programs.contains_key(id) {
            roots.extend(
                node.inputs()
                    .iter()
                    .filter(|id| programs.contains_key(id))
                    .copied(),
            );
        }
    }
    let reachable = |root: QueryNodeId, boundaries: &BTreeSet<QueryNodeId>| {
        let mut pending = vec![root];
        let mut seen = BTreeSet::new();
        while let Some(id) = pending.pop() {
            if !programs.contains_key(&id)
                || (id != root && boundaries.contains(&id))
                || !seen.insert(id)
            {
                continue;
            }
            pending.extend(entry.nodes[&id].inputs());
        }
        seen
    };
    // A producer consumed across an I/O boundary remains one separately scheduled
    // physical output, rather than being duplicated into both downstream graphs.
    let mut owners = BTreeMap::<QueryNodeId, usize>::new();
    for &root in &roots {
        for id in reachable(root, &BTreeSet::new()) {
            *owners.entry(id).or_default() += 1;
        }
    }
    roots.extend(
        owners
            .into_iter()
            .filter_map(|(id, count)| (count > 1).then_some(id)),
    );
    let mut replacements = BTreeMap::new();
    let mut remap = BTreeMap::new();
    for &root in &roots {
        let members = reachable(root, &roots);
        let mut sources = BTreeMap::<u64, InputContract>::new();
        let mut fragments = BTreeMap::new();
        for &id in &members {
            let graph = &programs[&id];
            let inputs = entry.nodes[&id].inputs();
            for ((_, contract), input) in graph.input_contracts().zip(inputs) {
                if !members.contains(input)
                    && sources
                        .insert(input.0, contract.clone())
                        .is_some_and(|previous| previous.schema != contract.schema)
                {
                    return Err(invalid("shared physical input has inconsistent schemas"));
                }
            }
            fragments.insert(
                id.0,
                (inputs.iter().map(|id| id.0).collect(), graph.clone()),
            );
            if id != root {
                remap.insert(id, root);
            }
        }
        let graph =
            CompiledPhysicalDag::compose(sources, fragments, vec![root.0]).map_err(invalid)?;
        replacements.insert(
            root,
            QueryPlanNode::PhysicalFragment {
                inputs: graph
                    .input_contracts()
                    .map(|(id, _)| QueryNodeId(id))
                    .collect(),
                dag: graph.encode().map_err(invalid)?,
                row_input: None,
                pruning: None,
            },
        );
    }
    for id in remap.keys() {
        entry.nodes.remove(id);
    }
    entry.nodes.extend(replacements);
    Ok(remap)
}

#[cfg(test)]
pub(crate) fn operator_parameters(node: &QueryPlanNode, kind: &str) -> Vec<serde_json::Value> {
    let QueryPlanNode::PhysicalFragment { dag, .. } = node else {
        return vec![];
    };
    CompiledPhysicalDag::decode(dag).unwrap();
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

#[cfg(test)]
mod tests {
    use super::*;

    // A sum over finalized summaries must be priced and installed as computation,
    // never reconstructed by the query worker.
    #[test]
    fn finalized_summary_rollup_is_retained_as_a_physical_graph() {
        for grouping in [
            PhysicalGrouping::PerEntity,
            PhysicalGrouping::Reduce(vec!["service".into()]),
        ] {
            let mut entry = QueryPlanEntry {
                physical_dag: None,
                language: QueryLanguage::PromQl,
                query_id: "rollup".into(),
                canonical_query: "sum by (service) (foo)".into(),
                fixed_evaluation: None,
                root: QueryNodeId(1),
                nodes: BTreeMap::from([
                    (
                        QueryNodeId(0),
                        QueryPlanNode::Logical {
                            operator: Operation::ExactSubquery {
                                query: "foo".into(),
                            },
                            inputs: vec![],
                        },
                    ),
                    (
                        QueryNodeId(1),
                        QueryPlanNode::ReduceSum {
                            input: QueryNodeId(0),
                            grouping,
                        },
                    ),
                ]),
                instant: InstantExecution {
                    lookback_ms: 1000,
                    full_history: false,
                    cumulative_readout: true,
                },
                fallback: FallbackPolicy::Reject,
            };
            compile(&mut entry).unwrap();
            let QueryPlanNode::PhysicalFragment { dag, inputs, .. } = &entry.nodes[&entry.root]
            else {
                panic!("rollup was left for Backend execution");
            };
            let compiled = CompiledPhysicalDag::decode(dag).unwrap();
            assert_eq!(inputs, &[QueryNodeId(0)]);
            assert_eq!(
                compiled
                    .output_contract(compiled.roots()[0])
                    .unwrap()
                    .schema,
                physical::vector_schema()
            );
        }
    }
}
