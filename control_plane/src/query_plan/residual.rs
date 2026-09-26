//! Typed residual operations compiled once by the control plane, never parsed at serving time.
use super::{
    FallbackPolicy, InstantExecution, QueryNodeId, QueryPlanEntry, QueryPlanError, QueryPlanNode,
};
use promql_parser::{
    label::MatchOp,
    parser::{self, Expr, LabelModifier, Offset, VectorSelector},
};
use std::collections::BTreeMap;

pub use asap_types::query_plan::residual::*;

/// Stable identity of a Planner-authorized materializable DAG leaf. This is a
/// workload-selection key, not another physical materialization definition.
#[derive(Debug, Clone, serde::Serialize, PartialEq)]
struct MaterializationCandidateIdentity {
    metric: String,
    matchers: Vec<LabelMatcher>,
    range_ms: u64,
    offset_ms: i64,
    operation: TemporalOperation,
}

fn invalid(message: impl Into<String>) -> QueryPlanError {
    QueryPlanError::Invalid(message.into())
}
fn millis(duration: std::time::Duration) -> Result<u64, QueryPlanError> {
    u64::try_from(duration.as_millis()).map_err(|_| invalid("logical duration overflow"))
}
fn offset(value: &Option<Offset>) -> Result<i64, QueryPlanError> {
    match value {
        None => Ok(0),
        Some(Offset::Pos(d)) => i64::try_from(millis(*d)?).map_err(|_| invalid("offset overflow")),
        Some(Offset::Neg(d)) => i64::try_from(millis(*d)?)
            .map(|v| -v)
            .map_err(|_| invalid("offset overflow")),
    }
}

struct Lower {
    nodes: BTreeMap<QueryNodeId, QueryPlanNode>,
    seen: BTreeMap<String, QueryNodeId>,
}
impl Lower {
    fn add(&mut self, node: QueryPlanNode) -> Result<QueryNodeId, QueryPlanError> {
        let key = serde_json::to_string(&node).map_err(|e| invalid(e.to_string()))?;
        if let Some(id) = self.seen.get(&key) {
            return Ok(*id);
        }
        let id = QueryNodeId(self.nodes.len() as u64);
        self.nodes.insert(id, node);
        self.seen.insert(key, id);
        Ok(id)
    }
    fn operation(
        &mut self,
        operator: ResidualQueryOperator,
        inputs: Vec<QueryNodeId>,
    ) -> Result<QueryNodeId, QueryPlanError> {
        operator.validate(inputs.len())?;
        self.add(QueryPlanNode::Logical { operator, inputs })
    }
    fn scan(
        &mut self,
        s: &VectorSelector,
        range_ms: Option<u64>,
    ) -> Result<QueryNodeId, QueryPlanError> {
        if s.at.is_some() || !s.matchers.or_matchers.is_empty() {
            return Err(invalid("logical @/OR selector is not supported"));
        }
        let matchers = s
            .matchers
            .matchers
            .iter()
            .map(|m| LabelMatcher {
                name: m.name.clone(),
                value: m.value.clone(),
                operation: match m.op {
                    MatchOp::Equal => LabelMatch::Equal,
                    MatchOp::NotEqual => LabelMatch::NotEqual,
                    MatchOp::Re(_) => LabelMatch::Regex,
                    MatchOp::NotRe(_) => LabelMatch::NotRegex,
                },
            })
            .collect();
        self.operation(
            ResidualQueryOperator::Scan {
                metric: s.name.clone(),
                matchers,
                range_ms,
                offset_ms: offset(&s.offset)?,
            },
            vec![],
        )
    }
    fn lower(&mut self, expr: &Expr) -> Result<QueryNodeId, QueryPlanError> {
        match expr {
            Expr::NumberLiteral(n) if n.val.is_finite() => {
                self.add(QueryPlanNode::Scalar { value: n.val })
            }
            Expr::Paren(p) => self.lower(&p.expr),
            Expr::Unary(u) => {
                let input = self.lower(&u.expr)?;
                self.operation(ResidualQueryOperator::UnaryNegate, vec![input])
            }
            Expr::VectorSelector(s) => self.scan(s, None),
            Expr::MatrixSelector(s) => self.scan(&s.vs, Some(millis(s.range)?)),
            Expr::Subquery(s) => {
                if s.at.is_some() {
                    return Err(invalid("logical subquery @ is unsupported"));
                }
                let input = self.lower(&s.expr)?;
                self.operation(
                    ResidualQueryOperator::Subquery {
                        range_ms: millis(s.range)?,
                        // Prometheus uses its configured default evaluation
                        // interval when `[range:]` omits the resolution. The
                        // backend-local deployment uses the Prometheus default
                        // of one minute; unsupported subquery operands are
                        // externalized as one exact subtree before execution.
                        step_ms: millis(
                            s.step.unwrap_or_else(|| std::time::Duration::from_secs(60)),
                        )?,
                        offset_ms: offset(&s.offset)?,
                    },
                    vec![input],
                )
            }
            Expr::Aggregate(a) => {
                let grouping = match &a.modifier {
                    None => Grouping {
                        labels: vec![],
                        without: false,
                    },
                    Some(LabelModifier::Include(labels)) => Grouping {
                        labels: labels.labels.clone(),
                        without: false,
                    },
                    Some(LabelModifier::Exclude(labels)) => Grouping {
                        labels: labels.labels.clone(),
                        without: true,
                    },
                };
                if a.op.to_string() == "topk" {
                    let Some(Expr::NumberLiteral(parameter)) = a.param.as_deref() else {
                        return Err(invalid("topk requires a literal scalar parameter"));
                    };
                    if !parameter.val.is_finite() {
                        return Err(invalid("topk requires a finite scalar parameter"));
                    }
                    // Prometheus converts the scalar parameter to int64 before
                    // selection. Values below one produce an empty vector.
                    let k = parameter.val as i64;
                    // Keep the selection node local even when its operand has
                    // unsupported syntax (for example a subquery with an
                    // implicit resolution). Prometheus evaluates that maximal
                    // instant-vector child; the backend still performs topk.
                    let nodes_before = self.nodes.clone();
                    let seen_before = self.seen.clone();
                    let input = match self.lower(&a.expr) {
                        Ok(input) => input,
                        Err(_) => {
                            self.nodes = nodes_before;
                            self.seen = seen_before;
                            self.operation(
                                ResidualQueryOperator::ExactSubquery {
                                    query: a.expr.to_string(),
                                },
                                vec![],
                            )?
                        }
                    };
                    let sorted = self.operation(
                        ResidualQueryOperator::Sort {
                            descending: true,
                            grouping: grouping.clone(),
                        },
                        vec![input],
                    )?;
                    return self.operation(
                        ResidualQueryOperator::Limit {
                            n: u64::try_from(k).unwrap_or(0),
                            offset: 0,
                            grouping,
                        },
                        vec![sorted],
                    );
                }
                if a.param.is_some() {
                    return Err(invalid("unsupported parameterized aggregate"));
                }
                let operation = match a.op.to_string().as_str() {
                    "sum" => Aggregation::Sum,
                    "max" => Aggregation::Max,
                    "min" => Aggregation::Min,
                    "avg" => Aggregation::Avg,
                    "count" => Aggregation::Count,
                    other => return Err(invalid(format!("unsupported logical aggregate {other}"))),
                };
                let input = self.lower(&a.expr)?;
                self.operation(
                    ResidualQueryOperator::Aggregate {
                        operation,
                        grouping,
                    },
                    vec![input],
                )
            }
            Expr::Call(c) => {
                let operator = match c.func.name {
                    "scalar" => ResidualQueryOperator::VectorToScalar,
                    "histogram_quantile" => ResidualQueryOperator::HistogramQuantile,
                    "sort" => ResidualQueryOperator::Sort {
                        descending: false,
                        grouping: Grouping {
                            labels: vec![],
                            without: false,
                        },
                    },
                    "sort_desc" => ResidualQueryOperator::Sort {
                        descending: true,
                        grouping: Grouping {
                            labels: vec![],
                            without: false,
                        },
                    },
                    name => ResidualQueryOperator::Temporal {
                        operation: match name {
                            "rate" => TemporalOperation::Rate,
                            "increase" => TemporalOperation::Increase,
                            "avg_over_time" => TemporalOperation::Avg,
                            "max_over_time" => TemporalOperation::Max,
                            "min_over_time" => TemporalOperation::Min,
                            "sum_over_time" => TemporalOperation::Sum,
                            "count_over_time" => TemporalOperation::Count,
                            _ => {
                                return Err(invalid(format!("unsupported logical function {name}")))
                            }
                        },
                    },
                };
                let inputs = c
                    .args
                    .args
                    .iter()
                    .map(|e| self.lower(e))
                    .collect::<Result<Vec<_>, _>>()?;
                self.operation(operator, inputs)
            }
            Expr::Binary(b) => {
                if b.modifier.as_ref().is_some_and(|m| {
                    m.matching.is_some()
                        || !matches!(m.card, parser::VectorMatchCardinality::OneToOne)
                }) {
                    return Err(invalid("logical explicit vector matching unsupported"));
                }
                let operation = match b.op.to_string().as_str() {
                    "+" => BinaryOperation::Add,
                    "-" => BinaryOperation::Sub,
                    "*" => BinaryOperation::Mul,
                    "/" => BinaryOperation::Div,
                    "%" => BinaryOperation::Mod,
                    "^" => BinaryOperation::Pow,
                    "==" => BinaryOperation::Equal,
                    "!=" => BinaryOperation::NotEqual,
                    "<" => BinaryOperation::Less,
                    "<=" => BinaryOperation::LessEqual,
                    ">" => BinaryOperation::Greater,
                    ">=" => BinaryOperation::GreaterEqual,
                    other => return Err(invalid(format!("unsupported logical binary {other}"))),
                };
                let inputs = vec![self.lower(&b.lhs)?, self.lower(&b.rhs)?];
                self.operation(
                    ResidualQueryOperator::Binary {
                        operation,
                        return_bool: b.return_bool(),
                    },
                    inputs,
                )
            }
            _ => Err(invalid("unsupported logical expression")),
        }
    }
}

/// Lower a Planner-authorized native residual into typed backend operations.
/// Callers retain a separate external-native alternative for cost comparison.
pub fn compile_logical(
    query_id: String,
    canonical_query: String,
    instant: InstantExecution,
    fallback: FallbackPolicy,
) -> Result<QueryPlanEntry, QueryPlanError> {
    let expr = parser::parse(&canonical_query).map_err(|e| invalid(e.to_string()))?;
    let mut lower = Lower {
        nodes: BTreeMap::new(),
        seen: BTreeMap::new(),
    };
    let root = lower.lower(&expr)?;
    let entry = QueryPlanEntry {
        language: super::QueryLanguage::PromQl,
        query_id,
        canonical_query,
        fixed_evaluation: None,
        root,
        nodes: lower.nodes,
        instant,
        fallback,
    };
    entry.validate(&Default::default())?;
    Ok(entry)
}

fn horizons(expr: &planner_types::pre_asap::QueryExpr, out: &mut Vec<u64>) {
    use planner_types::pre_asap::QueryExpr;
    if let QueryExpr::TimeRange { range, .. } = expr {
        if let Ok(ms) = u64::try_from(range.as_millis()) {
            out.push(ms);
        }
    }
    match expr {
        QueryExpr::PromqlScalarBridge(child)
        | QueryExpr::PromqlVectorFromScalar(child)
        | QueryExpr::PromqlScalarFromVector(child)
        | QueryExpr::PromqlRelabel { child, .. }
        | QueryExpr::PromqlSeriesSample { child, .. }
        | QueryExpr::PromqlInfoEnrich { child, .. }
        | QueryExpr::Filter { child, .. }
        | QueryExpr::Project { child, .. }
        | QueryExpr::Aggregate { child, .. }
        | QueryExpr::Dedup { child, .. }
        | QueryExpr::Sort { child, .. }
        | QueryExpr::Limit { child, .. }
        | QueryExpr::PromqlSubquery { child, .. }
        | QueryExpr::TimeRange { child, .. }
        | QueryExpr::TimeShift { child, .. } => horizons(child, out),
        QueryExpr::BinaryOp { lhs, rhs, .. } => {
            horizons(lhs, out);
            horizons(rhs, out);
        }
        QueryExpr::Join { left, right, .. } | QueryExpr::SetOp { left, right, .. } => {
            horizons(left, out);
            horizons(right, out);
        }
        _ => {}
    }
}

/// Match residuals by semantic IR equality, not display text or source names.
/// This ensures a subtree parsed for physical lowering is the subtree Planner kept.
pub(super) fn residual_nodes(
    original: &str,
    residual: &planner_types::pre_asap::QueryExpr,
) -> Result<(QueryNodeId, BTreeMap<QueryNodeId, QueryPlanNode>), QueryPlanError> {
    fn visit<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
        out.push(expr);
        match expr {
            Expr::Paren(e) => visit(&e.expr, out),
            Expr::Unary(e) => visit(&e.expr, out),
            Expr::Subquery(e) => visit(&e.expr, out),
            Expr::Aggregate(e) => visit(&e.expr, out),
            Expr::Binary(e) => {
                visit(&e.lhs, out);
                visit(&e.rhs, out);
            }
            Expr::Call(e) => {
                for input in &e.args.args {
                    visit(input, out);
                }
            }
            _ => {}
        }
    }
    let original = parser::parse(original).map_err(|e| invalid(e.to_string()))?;
    let mut expressions = Vec::new();
    visit(&original, &mut expressions);
    // Reconstruct equality witnesses with the selected IR's source horizon,
    // not the compatibility parser's default. Explicit matrix ranges remain
    // query-owned and equality still checks the complete tree.
    let mut intervals = vec![1_000];
    horizons(residual, &mut intervals);
    intervals.sort_unstable();
    intervals.dedup();
    // Accuracy annotations select a candidate, but exact execution still
    // implements that candidate's computation. Reconstruct the same typed IR
    // before comparing it; do not erase operators or source predicates.
    let accuracy = match residual {
        planner_types::pre_asap::QueryExpr::Aggregate { measures, .. } => measures
            .iter()
            .find_map(|intent| {
                use planner_types::pre_asap::AggIntent;
                match intent {
                    AggIntent::Quantile { accuracy, .. }
                    | AggIntent::Cardinality { accuracy, .. }
                    | AggIntent::Count { accuracy }
                    | AggIntent::TopK { accuracy, .. }
                    | AggIntent::FrequencyL2 { accuracy, .. }
                    | AggIntent::FrequencyEntropy { accuracy, .. } => Some(accuracy.clone()),
                    _ => None,
                }
            })
            .unwrap_or(planner_types::types::AccuracyTarget::Exact),
        _ => planner_types::types::AccuracyTarget::Exact,
    };
    for expression in expressions {
        for interval in &intervals {
            if let Ok(candidate) = crate::query_parser::parse_query_expr_with_interval(
                &expression.to_string(),
                accuracy.clone(),
                *interval,
            ) {
                if &candidate == residual {
                    let mut lower = Lower {
                        nodes: BTreeMap::new(),
                        seen: BTreeMap::new(),
                    };
                    let root = lower.lower(expression)?;
                    return Ok((root, lower.nodes));
                }
            }
        }
    }
    Err(invalid(
        "Planner residual does not match any original query subtree",
    ))
}

pub(super) fn binary_operator(
    operator: &planner_types::post_asap::BinaryOperator,
) -> Result<ResidualQueryOperator, QueryPlanError> {
    if operator.checked_relative_division || operator.checked_finite_division {
        if (operator.checked_relative_division && operator.checked_finite_division)
            || operator.vector_match.is_some()
            || !matches!(
                operator.kind,
                planner_types::pre_asap::BinaryOpKind::Arithmetic(
                    planner_types::pre_asap::ArithmeticOpKind::Div
                )
            )
        {
            return Err(invalid("invalid Planner checked division contract"));
        }
        return Ok(ResidualQueryOperator::Binary {
            operation: if operator.checked_finite_division {
                BinaryOperation::FiniteDiv
            } else {
                BinaryOperation::CheckedDiv
            },
            return_bool: false,
        });
    }
    if operator.vector_match.is_some() {
        return Err(invalid("explicit residual vector matching unsupported"));
    }
    let operation = match operator.kind.to_string().as_str() {
        "+" => BinaryOperation::Add,
        "-" => BinaryOperation::Sub,
        "*" => BinaryOperation::Mul,
        "/" => BinaryOperation::Div,
        "%" => BinaryOperation::Mod,
        "^" => BinaryOperation::Pow,
        "=" | "==" => BinaryOperation::Equal,
        "<>" | "!=" => BinaryOperation::NotEqual,
        "<" => BinaryOperation::Less,
        "<=" => BinaryOperation::LessEqual,
        ">" => BinaryOperation::Greater,
        ">=" => BinaryOperation::GreaterEqual,
        other => {
            return Err(invalid(format!(
                "unsupported Planner binary operator {other}"
            )))
        }
    };
    Ok(ResidualQueryOperator::Binary {
        operation,
        return_bool: false,
    })
}

/// Prove a physical-native substitute represents exactly the selected summary leaf.
/// A second Planner invocation is an equality witness, not a replacement selection.
pub(crate) fn selected_residual_nodes(
    original: &str,
    selected: &planner_types::post_asap::SummaryNode,
) -> Result<(QueryNodeId, BTreeMap<QueryNodeId, QueryPlanNode>), QueryPlanError> {
    let expression = selected_native_expression(original, selected)?;
    let mut lower = Lower {
        nodes: BTreeMap::new(),
        seen: BTreeMap::new(),
    };
    let root = lower.lower(&expression)?;
    Ok((root, lower.nodes))
}

/// Resolve the selected exact subtree to a verified native expression before
/// binding an external input. Never substitute the top-level query's child.
pub(super) fn selected_native_expression(
    original: &str,
    selected: &planner_types::post_asap::SummaryNode,
) -> Result<Expr, QueryPlanError> {
    if !selected.guarantee.as_ref().is_some_and(|g| g.is_exact()) {
        return Err(invalid(
            "native residual substitution requires an exact selected value",
        ));
    }
    let selected = match &selected.expr {
        planner_types::post_asap::SummaryExpr::ValueOperation {
            child,
            operation: planner_types::post_asap::ValueOperation::FinalizeExactAccumulator,
            ..
        } => child.as_ref(),
        _ => selected,
    };
    fn visit<'a>(expr: &'a Expr, output: &mut Vec<&'a Expr>) {
        output.push(expr);
        match expr {
            Expr::Paren(e) => visit(&e.expr, output),
            Expr::Unary(e) => visit(&e.expr, output),
            Expr::Subquery(e) => visit(&e.expr, output),
            Expr::Aggregate(e) => visit(&e.expr, output),
            Expr::Binary(e) => {
                visit(&e.lhs, output);
                visit(&e.rhs, output);
            }
            Expr::Call(e) => {
                for input in &e.args.args {
                    visit(input, output);
                }
            }
            _ => {}
        }
    }
    let parsed = parser::parse(original).map_err(|e| invalid(e.to_string()))?;
    let mut expressions = Vec::new();
    visit(&parsed, &mut expressions);
    fn selected_horizons(node: &planner_types::post_asap::SummaryNode, out: &mut Vec<u64>) {
        use planner_types::post_asap::SummaryExpr;
        match &node.expr {
            SummaryExpr::KeepPreAsap(expr) => horizons(expr, out),
            SummaryExpr::ValueOperation { child, .. } | SummaryExpr::SummaryAgg { child, .. } => {
                selected_horizons(child, out)
            }
            SummaryExpr::SummaryEstimate { summary_input, .. }
            | SummaryExpr::SummaryDelete { summary_input, .. } => {
                selected_horizons(summary_input, out)
            }
            SummaryExpr::BinaryOp {
                lhs: left,
                rhs: right,
                ..
            }
            | SummaryExpr::RelationalJoin { left, right, .. }
            | SummaryExpr::SummaryJoin {
                outer: left,
                inner: right,
                ..
            }
            | SummaryExpr::SummarySubtract { left, right } => {
                selected_horizons(left, out);
                selected_horizons(right, out);
            }
            SummaryExpr::SummaryMerge { children, .. } => {
                for child in children {
                    selected_horizons(child, out);
                }
            }
        }
    }
    let mut intervals = vec![1_000];
    selected_horizons(selected, &mut intervals);
    intervals.sort_unstable();
    intervals.dedup();
    let mut matched = None;
    for expression in expressions {
        for interval in &intervals {
            let Ok(canonical) = crate::query_parser::parse_query_expr_with_interval(
                &expression.to_string(),
                planner_types::types::AccuracyTarget::Exact,
                *interval,
            ) else {
                continue;
            };
            // Match provenance against all exact candidates. Do not make a
            // second selection or assume the first enumerated candidate won.
            use asap_aware_mapping::{
                Replacement, ReplacementStrategy, SketchAlgorithmStrategy, TargetSubDAG,
            };
            let root = std::rc::Rc::new(canonical);
            let candidates = SketchAlgorithmStrategy::new(&asap_aware_mapping::DefaultCostModel)
                .replacements(&TargetSubDAG::new(&root));
            if candidates.iter().any(|candidate| matches!(&candidate.replacement, Replacement::Summary(node) if node.as_ref() == selected)) {
                let candidate = expression.clone();
                if matched
                    .as_ref()
                    .is_some_and(|previous| previous != &candidate)
                {
                    return Err(invalid(
                        "ambiguous original subtrees share a Planner summary representation",
                    ));
                }
                matched = Some(candidate);
            }
        }
    }
    matched.ok_or_else(|| {
        invalid("selected summary leaf has no semantically identical original subtree witness")
    })
}

/// Read the original aggregate operation only after proving its selected-node identity.
/// Min and max share a Planner accumulator family, so the family name alone is insufficient.
pub(super) fn selected_aggregate_operator(
    original: &str,
    selected: &planner_types::post_asap::SummaryNode,
) -> Result<ResidualQueryOperator, QueryPlanError> {
    let (root, nodes) = selected_residual_nodes(original, selected)?;
    match nodes.get(&root) {
        Some(QueryPlanNode::Logical {
            operator: operator @ ResidualQueryOperator::Aggregate { .. },
            ..
        }) => Ok(operator.clone()),
        _ => Err(invalid(
            "selected value aggregation has no verified original aggregate operator",
        )),
    }
}

#[cfg(test)]
mod hybrid_tests {
    use super::*;
    use crate::query_plan::{MaterializationBinding, PhysicalGrouping};
    #[test]
    fn external_binding_rejects_an_unrelated_selected_exact_subtree() {
        let exact = crate::query_parser::parse_query_expr_with_interval(
            "sum_over_time(other_metric[5m])",
            planner_types::types::AccuracyTarget::Exact,
            1_000,
        )
        .unwrap();
        let selected = crate::planner_selection::plan_test_query(&exact).unwrap();
        assert!(selected_native_expression("topk(2, sum_over_time(m[5m]))", &selected).is_err());
        assert_eq!(
            selected_native_expression("sum_over_time(other_metric[5m])", &selected)
                .unwrap()
                .to_string(),
            "sum_over_time(other_metric[5m])"
        );
    }

    #[test]
    fn selected_summary_and_filtered_residual_share_installed_binary() {
        // Both filtered and unfiltered leaves bind independently.
        let query = "sum_over_time(m[5m]) + sum_over_time(m{job=\"api\"}[5m])";
        let canonical = crate::query_parser::parse_query_expr_canonical(
            query,
            planner_types::types::AccuracyTarget::Exact,
        )
        .unwrap();
        let selected = crate::planner_selection::plan_test_query(&canonical).unwrap();
        let entry =
            crate::query_plan::compile_bound_composable_mapped(
                "hybrid".into(),
                query.into(),
                &selected,
                InstantExecution {
                    lookback_ms: 300_000,
                    full_history: false,
                    cumulative_readout: false,
                },
                FallbackPolicy::Reject,
                |node, _| {
                    let (_, _, spatial_filter) =
                        crate::physical::compiler::raw_materialization_input_contract(node)
                            .map_err(QueryPlanError::Invalid)?;
                    Ok(MaterializationBinding {
                        full_window_slide_ms: None,
                        item_labels: Vec::new(),
                        materialization: asap_types::PolicyFingerprint(
                            if spatial_filter.is_empty() { 7 } else { 8 },
                        )
                        .into(),
                        stored_output_reference: asap_types::sds::StoredOutputReference::for_output(
                            asap_types::PolicyFingerprint(if spatial_filter.is_empty() {
                                7
                            } else {
                                8
                            })
                            .into(),
                        ),
                        output_grouping: PhysicalGrouping::PerEntity,
                        window_ms: 300_000,
                        pane_origin_ms: Some(0),
                        readout_lookback_ms: Some(300_000),
                    })
                },
                |_, _| {},
            )
            .unwrap();
        assert_eq!(entry.materialization_bindings().len(), 2);
        assert!(!entry.nodes.values().any(|node| matches!(
            node,
            QueryPlanNode::Logical {
                operator: ResidualQueryOperator::ExactSubquery { .. },
                ..
            }
        )));
        assert!(!entry.nodes.values().any(|node| matches!(
            node,
            QueryPlanNode::Logical {
                operator: ResidualQueryOperator::Scan { .. },
                ..
            }
        )));
        assert!(matches!(
            entry.nodes[&entry.root],
            QueryPlanNode::Logical {
                operator: ResidualQueryOperator::Binary { .. },
                ..
            }
        ));
        entry
            .validate(
                &[
                    asap_types::PolicyFingerprint(7),
                    asap_types::PolicyFingerprint(8),
                ]
                .into_iter()
                .collect(),
            )
            .unwrap();
    }

    #[test]
    fn different_filter_cannot_witness_selected_residual() {
        // Equality includes filter predicates, not just family, source, or window.
        let canonical = crate::query_parser::parse_query_expr_canonical(
            "sum_over_time(m{job=\"api\"}[5m])",
            planner_types::types::AccuracyTarget::Exact,
        )
        .unwrap();
        let selected = crate::planner_selection::plan_test_query(&canonical).unwrap();
        assert!(
            selected_residual_nodes("sum_over_time(m{job=\"worker\"}[5m])", &selected).is_err()
        );
    }
}

#[cfg(test)]
mod planner_workload_tests {
    use super::*;
    use crate::physical::compiler::{BackendLocalPlanningInput, DeploymentPlanCompiler};

    fn compile_one(query: &str) -> crate::physical::compiler::CompiledPhysicalPlan {
        let mut fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../docs/examples/asapquery-planning-snapshot.json"
        ))
        .unwrap();
        let mut entry = fixture["query_workload"]["repeating_queries"][0].clone();
        entry["query"] = query.into();
        entry["requirements"]["accuracy"] = serde_json::json!({"explicit":"Exact"});
        fixture["query_workload"]["repeating_queries"] = vec![entry].into();
        let snapshot: BackendLocalPlanningInput = serde_json::from_value(fixture).unwrap();
        let (request, environment) = snapshot
            .into_physical_compilation_request()
            .unwrap_or_else(|error| panic!("{query}: {error}"));
        DeploymentPlanCompiler
            .compile_promql(request, environment)
            .unwrap_or_else(|error| panic!("{query}: {error}"))
    }

    #[test]
    fn evaluation_topk_queries_retain_a_local_selection_root() {
        for query in [
            "topk(2, sum by (job) (rate(backend_process_cpu_seconds_total[1h])))",
            "topk(2, sum by (job) (backend_process_resident_memory_bytes))",
            "topk(2, max_over_time(backend_retry_backlog_depth[6h]))",
            "topk(1, sum by (job) (rate(backend_process_cpu_seconds_total[6h])))",
            "topk(3, avg_over_time((sum by (job) (backend_process_resident_memory_bytes))[6h:]))",
        ] {
            let plan = compile_one(query);
            let entry = plan.query_plan.entries.values().next().unwrap();
            assert!(
                matches!(
                    entry.nodes[&entry.root],
                    QueryPlanNode::Logical {
                        operator: ResidualQueryOperator::Limit { .. },
                        ..
                    }
                ),
                "{query}: {:?}",
                entry.nodes[&entry.root]
            );
            assert!(
                !entry
                    .nodes
                    .values()
                    .any(|node| matches!(node, QueryPlanNode::ExactFallback { .. })),
                "{query}"
            );
        }
    }

    #[test]
    fn planner_value_topk_preserves_direct_summary_children() {
        for query in [
            "topk(2, rate(backend_process_cpu_seconds_total[1h]))",
            "topk(2, max_over_time(backend_retry_backlog_depth[6h]))",
        ] {
            let plan = compile_one(query);
            let entry = plan.query_plan.entries.values().next().unwrap();
            assert!(matches!(
                entry.nodes[&entry.root],
                QueryPlanNode::Logical {
                    operator: ResidualQueryOperator::Limit { .. },
                    ..
                }
            ));
            assert!(
                !entry.materialization_bindings().is_empty(),
                "{query} must retain its SummaryStore child: {:?}",
                entry.nodes
            );
        }
    }

    #[test]
    fn whole_o11y_planner_candidate_lowers_every_original_query() {
        // AST support alone is insufficient: the actual selected Planner forest must bind too.
        let corpus: serde_json::Value =
            serde_json::from_str(include_str!("../../tests/fixtures/o11y_queries.json")).unwrap();
        let mut fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../docs/examples/asapquery-planning-snapshot.json"
        ))
        .unwrap();
        let template = fixture["query_workload"]["repeating_queries"][0].clone();
        let mut seen = std::collections::BTreeSet::new();
        let mut entries = Vec::new();
        for row in corpus["queries"].as_array().unwrap() {
            let query = row["query"].as_str().unwrap();
            if !seen.insert(query.to_string()) {
                continue;
            }
            let mut entry = template.clone();
            entry["query"] = query.into();
            entry["requirements"]["accuracy"] = serde_json::json!({"explicit":"Exact"});
            entries.push(entry);
        }
        fixture["query_workload"]["repeating_queries"] = entries.into();
        let snapshot: BackendLocalPlanningInput = serde_json::from_value(fixture).unwrap();
        let (request, environment) = snapshot.into_physical_compilation_request().unwrap();
        assert!(request.allow_mixed_summary_and_exact_execution);
        let plan = DeploymentPlanCompiler
            .compile_promql(request, environment)
            .unwrap();
        assert_eq!(plan.query_plan.entries.len(), 24);
        assert!(plan.query_plan.entries.values().all(|entry| !entry
            .nodes
            .values()
            .any(|node| matches!(node, QueryPlanNode::ExactFallback { .. }))));
    }

    #[test]
    fn max_of_selected_values_uses_original_max_operator() {
        // MinMax storage type does not authorize choosing min or replacing the selected operand graph.
        let query = "max(sum_over_time(m[5m]) / 2)";
        let canonical = crate::query_parser::parse_query_expr_canonical(
            query,
            planner_types::types::AccuracyTarget::Exact,
        )
        .unwrap();
        use asap_aware_mapping::{
            Replacement, ReplacementStrategy, SketchAlgorithmStrategy, TargetSubDAG,
        };
        let root = std::rc::Rc::new(canonical);
        let candidates = SketchAlgorithmStrategy::new(&asap_aware_mapping::DefaultCostModel)
            .replacements(&TargetSubDAG::new(&root));
        let [candidate] = candidates.as_slice() else {
            panic!("expected one exact aggregate candidate")
        };
        let Replacement::Summary(selected) = &candidate.replacement else {
            panic!("expected exact summary fixture")
        };
        let operator = selected_aggregate_operator(query, selected).unwrap();
        assert!(matches!(
            operator,
            ResidualQueryOperator::Aggregate {
                operation: Aggregation::Max,
                ..
            }
        ));
    }

    #[test]
    fn ambiguous_extremum_witness_is_rejected() {
        // Different readouts over the same MinMax state cannot be resolved by taking the first AST match.
        let canonical = crate::query_parser::parse_query_expr_canonical(
            "min(m)",
            planner_types::types::AccuracyTarget::Exact,
        )
        .unwrap();
        let selected = crate::planner_selection::plan_test_query(&canonical).unwrap();
        let maximum = crate::query_parser::parse_query_expr_canonical(
            "max(m)",
            planner_types::types::AccuracyTarget::Exact,
        )
        .unwrap();
        let maximum = crate::planner_selection::plan_test_query(&maximum).unwrap();
        let result = selected_residual_nodes("min(m) + max(m)", &selected);
        if selected == maximum {
            assert!(result.is_err());
        } else {
            let (root, nodes) = result.unwrap();
            assert!(matches!(
                nodes[&root],
                QueryPlanNode::Logical {
                    operator: ResidualQueryOperator::Aggregate {
                        operation: Aggregation::Min,
                        ..
                    },
                    ..
                }
            ));
        }
    }
}

/// Preserve the exact original operator direction because MinMax family alone
/// does not distinguish min from max. The full Planner-node witness is required.
pub(crate) fn selected_range_max_materialization(
    original: &str,
    node: &planner_types::post_asap::SummaryNode,
) -> Result<Option<String>, QueryPlanError> {
    use planner_types::post_asap::{ExactKind, SummaryExpr, SummaryFamilyType};
    if !matches!(
        &node.expr,
        SummaryExpr::SummaryAgg {
            family: SummaryFamilyType::ExactAggregate(ExactKind::Max, _),
            reduction: planner_types::pre_asap::Reduction::PerEntity,
            ..
        }
    ) {
        return Ok(None);
    }
    let (root, nodes) = selected_residual_nodes(original, node)?;
    let Some(QueryPlanNode::Logical {
        operator:
            ResidualQueryOperator::Temporal {
                operation: TemporalOperation::Max,
            },
        inputs,
    }) = nodes.get(&root)
    else {
        return Ok(None);
    };
    if inputs.len() != 1 || nodes.len() != 2 {
        return Ok(None);
    }
    let Some(QueryPlanNode::Logical {
        operator:
            ResidualQueryOperator::Scan {
                metric: Some(metric),
                matchers,
                range_ms: Some(range_ms),
                offset_ms: 0,
            },
        ..
    }) = nodes.get(&inputs[0])
    else {
        return Ok(None);
    };
    Ok(Some(materialization_candidate_key(
        MaterializationCandidateIdentity {
            metric: metric.clone(),
            matchers: matchers.clone(),
            range_ms: *range_ms,
            offset_ms: 0,
            operation: TemporalOperation::Max,
        },
    )?))
}

#[cfg(test)]
mod range_max_materialization_tests {
    use super::*;
    #[test]
    fn real_gauge_queries_have_planner_authorized_exact_materializations() {
        for (query, metric, range_ms) in [
            (
                r#"max_over_time(service_cache_refresh_lag_seconds{job="user-service"}[12h])"#,
                "service_cache_refresh_lag_seconds",
                43_200_000,
            ),
            (
                r#"max_over_time(service_retry_queue_depth{job=~".+"}[6h])"#,
                "service_retry_queue_depth",
                21_600_000,
            ),
            (
                r#"max_over_time(service_retry_queue_depth{job="order-service"}[6h])"#,
                "service_retry_queue_depth",
                21_600_000,
            ),
        ] {
            let original = crate::query_parser::parse_query_expr_canonical(
                query,
                planner_types::types::AccuracyTarget::Exact,
            )
            .unwrap();
            let selected = crate::planner_selection::plan_test_query(&original).unwrap();
            let key = selected_range_max_materialization(query, &selected)
                .unwrap()
                .unwrap();
            assert!(key.contains(metric));
            assert!(key.contains(&range_ms.to_string()));
        }
    }
    #[test]
    fn min_and_shifted_or_nested_windows_do_not_become_max_materializations() {
        for query in [
            "min_over_time(m[1m])",
            "max_over_time(m[1m] offset 1m)",
            "max_over_time((m + m)[1m:1s])",
        ] {
            let original = crate::query_parser::parse_query_expr_canonical(
                query,
                planner_types::types::AccuracyTarget::Exact,
            )
            .unwrap();
            let selected = crate::planner_selection::plan_test_query(&original).unwrap();
            assert!(
                selected_range_max_materialization(query, &selected)
                    .unwrap()
                    .is_none(),
                "{query}"
            );
        }
    }
}

/// Stable contract identity used by priced physical candidates, independent of node IDs.
fn materialization_candidate_key(
    candidate: MaterializationCandidateIdentity,
) -> Result<String, QueryPlanError> {
    let mut value = serde_json::to_value(candidate).map_err(|e| invalid(e.to_string()))?;
    if let Some(object) = value.as_object_mut() {
        // Retention is a consumer lifetime requirement, not the materialization read's
        // semantics. Equivalent matcher conjunctions must share policy keys.
        object.remove("retention_ms");
        if let Some(matchers) = object.get_mut("matchers").and_then(|v| v.as_array_mut()) {
            matchers.sort_by_cached_key(|m| m.to_string());
        }
    }
    serde_json::to_string(&value).map_err(|e| invalid(e.to_string()))
}

fn counter_contract(
    root: QueryNodeId,
    nodes: &BTreeMap<QueryNodeId, QueryPlanNode>,
) -> Option<MaterializationCandidateIdentity> {
    let QueryPlanNode::Logical {
        operator: ResidualQueryOperator::Temporal { operation },
        inputs,
    } = nodes.get(&root)?
    else {
        return None;
    };
    if !matches!(
        operation,
        TemporalOperation::Rate | TemporalOperation::Increase
    ) || inputs.len() != 1
    {
        return None;
    }
    let QueryPlanNode::Logical {
        operator:
            ResidualQueryOperator::Scan {
                metric: Some(metric),
                matchers,
                range_ms: Some(range_ms),
                offset_ms,
            },
        ..
    } = nodes.get(&inputs[0])?
    else {
        return None;
    };
    Some(MaterializationCandidateIdentity {
        metric: metric.clone(),
        matchers: matchers.clone(),
        range_ms: *range_ms,
        offset_ms: *offset_ms,
        operation: *operation,
    })
}

pub(crate) fn selected_counter_materialization(
    original: &str,
    node: &planner_types::post_asap::SummaryNode,
) -> Result<Option<String>, QueryPlanError> {
    use planner_types::post_asap::{ExactKind, SummaryExpr, SummaryFamilyType};
    if !matches!(
        &node.expr,
        SummaryExpr::SummaryAgg {
            family: SummaryFamilyType::ExactAggregate(ExactKind::Rate | ExactKind::Increase, _),
            reduction: planner_types::pre_asap::Reduction::PerEntity,
            ..
        }
    ) {
        return Ok(None);
    }
    let (root, nodes) = selected_residual_nodes(original, node)?;
    counter_contract(root, &nodes)
        .map(materialization_candidate_key)
        .transpose()
}

fn prune(entry: &mut QueryPlanEntry) {
    let mut seen = std::collections::BTreeSet::new();
    let mut pending = vec![entry.root];
    while let Some(id) = pending.pop() {
        if seen.insert(id) {
            if let Some(node) = entry.nodes.get(&id) {
                pending.extend(node.inputs());
            }
        }
    }
    entry.nodes.retain(|id, _| seen.contains(id));
}

/// Finish the installed DAG by externalizing every residual raw subtree.
pub fn finalize_residuals(entry: &mut QueryPlanEntry) -> Result<(), QueryPlanError> {
    externalize_residuals(entry)?;
    assign_retention(entry)
}

pub fn eligible_materialization_keys(
    original: &str,
    selected: &std::rc::Rc<planner_types::post_asap::SummaryNode>,
) -> Result<std::collections::BTreeSet<String>, QueryPlanError> {
    use planner_types::post_asap::SummaryExpr;
    fn visit(
        original: &str,
        node: &std::rc::Rc<planner_types::post_asap::SummaryNode>,
        keys: &mut std::collections::BTreeSet<String>,
    ) -> Result<(), QueryPlanError> {
        if let Some(key) = selected_counter_materialization(original, node)?
            .or(selected_range_max_materialization(original, node)?)
        {
            keys.insert(key);
        }
        match &node.expr {
            SummaryExpr::BinaryOp { lhs, rhs, .. } => {
                visit(original, lhs, keys)?;
                visit(original, rhs, keys)?;
            }
            SummaryExpr::RelationalJoin { left, right, .. } => {
                visit(original, left, keys)?;
                visit(original, right, keys)?;
            }

            SummaryExpr::ValueOperation { child, .. } => visit(original, child, keys)?,
            SummaryExpr::SummaryAgg { child, .. } => visit(original, child, keys)?,
            SummaryExpr::SummaryEstimate { summary_input, .. }
            | SummaryExpr::SummaryDelete { summary_input, .. } => {
                visit(original, summary_input, keys)?
            }
            SummaryExpr::SummaryMerge { children, .. } => {
                for child in children {
                    visit(original, child, keys)?;
                }
            }
            SummaryExpr::SummaryJoin { outer, inner, .. } => {
                visit(original, outer, keys)?;
                visit(original, inner, keys)?;
            }
            SummaryExpr::SummarySubtract { left, right } => {
                visit(original, left, keys)?;
                visit(original, right, keys)?;
            }
            SummaryExpr::KeepPreAsap(_) => {}
        }
        Ok(())
    }
    let mut keys = std::collections::BTreeSet::new();
    visit(original, selected, &mut keys)?;
    Ok(keys)
}

fn expression_shape(
    id: QueryNodeId,
    nodes: &BTreeMap<QueryNodeId, QueryPlanNode>,
) -> Result<String, QueryPlanError> {
    let node = nodes
        .get(&id)
        .ok_or_else(|| invalid("missing expression node"))?;
    if let QueryPlanNode::Logical {
        operator: ResidualQueryOperator::ExactSubquery { query },
        ..
    } = node
    {
        let mut lower = Lower {
            nodes: BTreeMap::new(),
            seen: BTreeMap::new(),
        };
        let expr = parser::parse(query).map_err(|e| invalid(e.to_string()))?;
        let root = lower.lower(&expr)?;
        return expression_shape(root, &lower.nodes);
    }
    let value = match node {
        QueryPlanNode::Logical { operator, .. } => {
            serde_json::to_string(operator).map_err(|e| invalid(e.to_string()))?
        }
        QueryPlanNode::Scalar { value } => format!("scalar:{:x}", value.to_bits()),
        _ => return Err(invalid("summary node has no raw expression shape")),
    };
    let children = node
        .inputs()
        .iter()
        .map(|child| expression_shape(*child, nodes))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(format!("{value}({})", children.join(";")))
}

/// Collapse only maximal exact residual subtrees whose full typed expression is
/// witnessed in the original query. Matrix boundaries remain inside Prometheus.
pub fn externalize_residuals(entry: &mut QueryPlanEntry) -> Result<(), QueryPlanError> {
    fn gather(expr: &Expr, out: &mut Vec<Expr>) {
        if !matches!(expr, Expr::MatrixSelector(_) | Expr::Subquery(_)) {
            out.push(expr.clone());
        }
        match expr {
            Expr::Paren(e) => gather(&e.expr, out),
            Expr::Unary(e) => gather(&e.expr, out),
            Expr::Subquery(e) => gather(&e.expr, out),
            Expr::Aggregate(e) => gather(&e.expr, out),
            Expr::Binary(e) => {
                gather(&e.lhs, out);
                gather(&e.rhs, out);
            }
            Expr::Call(e) => {
                for arg in &e.args.args {
                    gather(arg, out);
                }
            }
            _ => {}
        }
    }
    let expr = parser::parse(&entry.canonical_query).map_err(|e| invalid(e.to_string()))?;
    let mut expressions = Vec::new();
    gather(&expr, &mut expressions);
    let mut witnesses = BTreeMap::new();
    for expression in expressions {
        let mut lower = Lower {
            nodes: BTreeMap::new(),
            seen: BTreeMap::new(),
        };
        if let Ok(root) = lower.lower(&expression) {
            witnesses.insert(
                expression_shape(root, &lower.nodes)?,
                expression.to_string(),
            );
        }
    }
    fn flags(id: QueryNodeId, nodes: &BTreeMap<QueryNodeId, QueryPlanNode>) -> (bool, bool) {
        let Some(node) = nodes.get(&id) else {
            return (true, false);
        };
        let mut indexed = !matches!(
            node,
            QueryPlanNode::Logical { .. } | QueryPlanNode::Scalar { .. }
        ) || matches!(
            node,
            QueryPlanNode::Logical {
                operator: ResidualQueryOperator::Limit { .. },
                ..
            }
        );
        let mut exact = false;
        if let QueryPlanNode::Logical { operator, .. } = node {
            exact = matches!(
                operator,
                ResidualQueryOperator::Scan { .. }
                    | ResidualQueryOperator::ExactSubquery { .. }
                    | ResidualQueryOperator::CandidateExactSubquery { .. }
            );
        }
        for child in node.inputs() {
            let (a, b) = flags(*child, nodes);
            indexed |= a;
            exact |= b;
        }
        (indexed, exact)
    }
    let mut pending = vec![entry.root];
    while let Some(id) = pending.pop() {
        if matches!(
            entry.nodes.get(&id),
            Some(QueryPlanNode::Logical {
                operator: ResidualQueryOperator::ExactSubquery { .. }
                    | ResidualQueryOperator::CandidateExactSubquery { .. },
                ..
            })
        ) {
            continue;
        }
        let (indexed, exact) = flags(id, &entry.nodes);
        if !indexed && exact {
            if let Ok(shape) = expression_shape(id, &entry.nodes) {
                if let Some(query) = witnesses.get(&shape) {
                    entry.nodes.insert(
                        id,
                        QueryPlanNode::Logical {
                            operator: ResidualQueryOperator::ExactSubquery {
                                query: query.clone(),
                            },
                            inputs: vec![],
                        },
                    );
                    continue;
                }
            }
        }
        pending.extend(entry.nodes[&id].inputs());
    }
    prune(entry);
    if entry.nodes.values().any(|node| {
        matches!(
            node,
            QueryPlanNode::Logical {
                operator: ResidualQueryOperator::Scan { .. },
                ..
            }
        )
    }) {
        return Err(invalid(
            "local Scan is not deployable; exact subtree requires a complete Prometheus boundary",
        ));
    }
    Ok(())
}

fn assign_retention(entry: &mut QueryPlanEntry) -> Result<(), QueryPlanError> {
    let mut pending = vec![(entry.root, 0u64)];
    let mut depths = BTreeMap::new();
    while let Some((id, depth)) = pending.pop() {
        if depths.get(&id).is_some_and(|prior| *prior >= depth) {
            continue;
        }
        depths.insert(id, depth);
        let node = entry
            .nodes
            .get_mut(&id)
            .ok_or_else(|| invalid("missing index ancestor"))?;
        let mut child_depth = depth;
        if let QueryPlanNode::Logical { operator, .. } = node {
            if let ResidualQueryOperator::Subquery {
                range_ms,
                offset_ms,
                ..
            } = operator
            {
                child_depth = depth
                    .checked_add(*range_ms)
                    .and_then(|v| v.checked_add((*offset_ms).max(0) as u64))
                    .ok_or_else(|| invalid("retention overflow"))?;
            }
        }
        pending.extend(node.inputs().iter().map(|child| (*child, child_depth)));
    }
    Ok(())
}

#[cfg(test)]
mod remote_boundary_regressions {
    use super::*;

    #[test]
    fn stored_output_identity_is_independent_of_matcher_order() {
        let first = LabelMatcher {
            name: "job".into(),
            value: "orders".into(),
            operation: LabelMatch::Equal,
        };
        let second = LabelMatcher {
            name: "status".into(),
            value: "5..".into(),
            operation: LabelMatch::Regex,
        };
        let a = MaterializationCandidateIdentity {
            metric: "requests".into(),
            matchers: vec![first.clone(), second.clone()],
            range_ms: 300_000,
            offset_ms: 0,
            operation: TemporalOperation::Rate,
        };
        let b = MaterializationCandidateIdentity {
            metric: "requests".into(),
            matchers: vec![second, first],
            range_ms: 300_000,
            offset_ms: 0,
            operation: TemporalOperation::Rate,
        };
        assert_eq!(
            materialization_candidate_key(a).unwrap(),
            materialization_candidate_key(b).unwrap()
        );
    }

    #[test]
    fn real_error_ratio_exposes_two_independent_materialization_candidates() {
        let query = "sum(rate(http_requests_total{job=\"order-service\",status=~\"5..\"}[5m])) / sum(rate(http_requests_total{job=\"order-service\"}[5m]))";
        let parsed = crate::query_parser::parse_query_expr_canonical(
            query,
            planner_types::types::AccuracyTarget::Exact,
        )
        .unwrap();
        let selected = crate::planner_selection::plan_test_query(&parsed).unwrap();
        assert_eq!(
            eligible_materialization_keys(query, &selected)
                .unwrap()
                .len(),
            2
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // A workload horizon changes the equality witness, never its filter or explicit range.
    #[test]
    fn workload_horizon_residual_keeps_semantic_equality() {
        let residual = crate::query_parser::parse_query_expr_with_interval(
            "sum(m{job=\"api\"})",
            planner_types::types::AccuracyTarget::Exact,
            5_000,
        )
        .unwrap();
        assert!(residual_nodes("sum(m{job=\"api\"})", &residual).is_ok());
        assert!(residual_nodes("sum(m{job=\"worker\"})", &residual).is_err());
        let range = crate::query_parser::parse_query_expr_with_interval(
            "sum_over_time(m[1m])",
            planner_types::types::AccuracyTarget::Exact,
            5_000,
        )
        .unwrap();
        assert!(residual_nodes("sum_over_time(m[2m])", &range).is_err());
    }

    fn instant() -> InstantExecution {
        InstantExecution {
            lookback_ms: 300_000,
            full_history: false,
            cumulative_readout: false,
        }
    }

    #[test]
    fn complete_o11y_corpus_lowers_to_serialized_operations() {
        // Every original workload occurrence must compile to an executable typed graph.
        let corpus: serde_json::Value =
            serde_json::from_str(include_str!("../../tests/fixtures/o11y_queries.json")).unwrap();
        for row in corpus["queries"].as_array().unwrap() {
            let query = row["query"].as_str().unwrap();
            let entry = crate::query_plan::residual::compile_logical(
                row["id"].as_str().unwrap().into(),
                query.into(),
                instant(),
                FallbackPolicy::Reject,
            )
            .unwrap_or_else(|error| panic!("{query}: {error}"));
            let encoded = serde_json::to_string(&entry).unwrap();
            let restored: QueryPlanEntry = serde_json::from_str(&encoded).unwrap();
            restored.validate(&Default::default()).unwrap();
            assert!(!restored
                .nodes
                .values()
                .any(|node| matches!(node, QueryPlanNode::ExactFallback { .. })));
        }
    }
    #[test]
    fn residual_mapping_preserves_filters_and_rejects_different_sources() {
        // Physical lowering must prove correspondence with the Planner-kept semantic subtree.
        let query = "sum(rate(requests_total{job=\"api\"}[5m]))";
        let residual = crate::query_parser::parse_query_expr_canonical(
            query,
            planner_types::types::AccuracyTarget::Exact,
        )
        .unwrap();
        let (_, nodes) = residual_nodes(query, &residual).unwrap();
        assert!(nodes.values().any(|node| matches!(node, QueryPlanNode::Logical { operator: ResidualQueryOperator::Scan { matchers, .. }, .. } if matchers.iter().any(|m| m.name == "job" && m.value == "api"))));
        assert!(residual_nodes("sum(rate(other_total[5m]))", &residual).is_err());
    }
    #[test]
    fn repeated_subexpressions_share_node_identity() {
        // Serialized edges must retain CSE rather than duplicating raw work.
        let entry = crate::query_plan::residual::compile_logical(
            "q".into(),
            "sum(up) / sum(up)".into(),
            instant(),
            FallbackPolicy::Reject,
        )
        .unwrap();
        let QueryPlanNode::Logical { inputs, .. } = &entry.nodes[&entry.root] else {
            panic!("binary expected")
        };
        assert_eq!(inputs[0], inputs[1]);
    }
    #[test]
    fn malformed_operator_arity_is_rejected_at_installation() {
        // A serialized graph cannot bypass the operation's input contract.
        assert!(ResidualQueryOperator::HistogramQuantile
            .validate(1)
            .is_err());
        assert!(ResidualQueryOperator::Subquery {
            range_ms: 60_000,
            step_ms: 0,
            offset_ms: 0
        }
        .validate(1)
        .is_err());
    }

    #[test]
    fn real_topk_queries_lower_to_value_selection() {
        for (query, k) in [
            (
                "topk(2, sum by (job) (rate(backend_process_cpu_seconds_total[1h])))",
                2,
            ),
            (
                "topk(2, sum by (job) (backend_process_resident_memory_bytes))",
                2,
            ),
            ("topk(2, max_over_time(backend_retry_backlog_depth[6h]))", 2),
            (
                "topk(1, sum by (job) (increase(backend_http_5xx_total[6h])) / sum by (job) (increase(backend_http_requests_total[6h])))",
                1,
            ),
            (
                "topk(3, avg_over_time((sum by (job) (backend_process_resident_memory_bytes))[6h:]))",
                3,
            ),
        ] {
            let entry = crate::query_plan::residual::compile_logical(
                "topk".into(),
                query.into(),
                instant(),
                FallbackPolicy::Reject,
            )
            .unwrap_or_else(|error| panic!("{query}: {error}"));
            assert!(matches!(
                entry.nodes[&entry.root],
                QueryPlanNode::Logical {
                    operator: ResidualQueryOperator::Limit { n: actual, .. },
                    ..
                } if actual == k
            ));
        }
    }

    #[test]
    fn topk_keeps_unsupported_child_as_exact_leaf() {
        let entry = crate::query_plan::residual::compile_logical(
            "topk-subquery".into(),
            "topk(3, label_replace(memory_bytes, \"dst\", \"$1\", \"src\", \"(.*)\"))".into(),
            instant(),
            FallbackPolicy::Reject,
        )
        .unwrap();
        assert!(matches!(
            entry.nodes[&entry.root],
            QueryPlanNode::Logical {
                operator: ResidualQueryOperator::Limit { n: 3, .. },
                ..
            }
        ));
        assert!(entry.nodes.values().any(|node| matches!(
            node,
            QueryPlanNode::Logical {
                operator: ResidualQueryOperator::ExactSubquery { .. },
                ..
            }
        )));
    }

    #[test]
    fn topk_preserves_by_and_without_partitioning() {
        for (query, labels, without) in [
            ("topk by (cluster) (2, m)", vec!["cluster"], false),
            ("topk without (pod) (2, m)", vec!["pod"], true),
        ] {
            let entry = crate::query_plan::residual::compile_logical(
                "topk-group".into(),
                query.into(),
                instant(),
                FallbackPolicy::Reject,
            )
            .unwrap();
            assert!(matches!(
                &entry.nodes[&entry.root],
                QueryPlanNode::Logical {
                    operator: ResidualQueryOperator::Limit { grouping, .. },
                    ..
                } if grouping.labels == labels && grouping.without == without
            ));
        }
    }
}
