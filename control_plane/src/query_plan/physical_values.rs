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

pub fn compile(entry: &mut QueryPlanEntry) -> Result<(), QueryPlanError> {
    if entry.language == QueryLanguage::ClickHouseSql || entry.physical_dag.is_some() {
        return Ok(());
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
                None
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
                _ => None,
            },
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
    Ok(())
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
