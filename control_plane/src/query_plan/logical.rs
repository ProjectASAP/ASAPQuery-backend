//! Typed residual operations compiled once by the control plane, never parsed at serving time.
use super::{
    FallbackPolicy, InstantExecution, QueryNodeId, QueryPlanEntry, QueryPlanError, QueryPlanNode,
};
use promql_parser::{
    label::MatchOp,
    parser::{self, Expr, LabelModifier, Offset, VectorSelector},
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum LogicalOperator {
    /// A maximal exact scalar/vector subtree evaluated by Prometheus.
    ExactSubquery {
        query: String,
    },
    /// Exact reset-aware per-series counter state, with independent branch time binding.
    ReadRangeCounterIndex {
        metric: String,
        matchers: Vec<LabelMatcher>,
        range_ms: u64,
        offset_ms: i64,
        operation: TemporalOperation,
        retention_ms: u64,
    },
    /// Planner-selected exact per-series MinMax state, queried as max over an
    /// exact event-time interval. This is an installed index, not a raw scan.
    ReadRangeMaxIndex {
        metric: String,
        matchers: Vec<LabelMatcher>,
        range_ms: u64,
        retention_ms: u64,
    },
    Scan {
        metric: Option<String>,
        matchers: Vec<LabelMatcher>,
        range_ms: Option<u64>,
        offset_ms: i64,
    },
    UnaryNegate,
    Aggregate {
        operation: Aggregation,
        grouping: Grouping,
    },
    Binary {
        operation: BinaryOperation,
        return_bool: bool,
    },
    Temporal {
        operation: TemporalOperation,
    },
    Sort {
        descending: bool,
    },
    HistogramQuantile,
    Subquery {
        range_ms: u64,
        step_ms: u64,
        offset_ms: i64,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Grouping {
    pub labels: Vec<String>,
    pub without: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LabelMatcher {
    pub name: String,
    pub value: String,
    pub operation: LabelMatch,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LabelMatch {
    Equal,
    NotEqual,
    Regex,
    NotRegex,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Aggregation {
    Sum,
    Max,
    Min,
    Avg,
    Count,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BinaryOperation {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TemporalOperation {
    Rate,
    Increase,
    Avg,
    Max,
    Min,
    Sum,
    Count,
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

impl LogicalOperator {
    pub fn validate(&self, inputs: usize) -> Result<(), QueryPlanError> {
        let expected = match self {
            Self::Scan { .. }
            | Self::ReadRangeMaxIndex { .. }
            | Self::ReadRangeCounterIndex { .. }
            | Self::ExactSubquery { .. } => 0,
            Self::Binary { .. } | Self::HistogramQuantile => 2,
            _ => 1,
        };
        if inputs != expected {
            return Err(invalid("logical operator input arity mismatch"));
        }
        if matches!(
            self,
            Self::Scan {
                range_ms: Some(0),
                ..
            }
        ) {
            return Err(invalid("zero range"));
        }
        if let Self::ReadRangeMaxIndex {
            metric,
            range_ms,
            retention_ms,
            ..
        } = self
        {
            if metric.is_empty()
                || *range_ms == 0
                || *range_ms > i64::MAX as u64
                || retention_ms < range_ms
            {
                return Err(invalid("invalid exact range-max index contract"));
            }
        }
        if let Self::ReadRangeCounterIndex {
            metric,
            range_ms,
            operation,
            retention_ms,
            ..
        } = self
        {
            if metric.is_empty()
                || *range_ms == 0
                || *range_ms > i64::MAX as u64
                || retention_ms < range_ms
                || !matches!(
                    operation,
                    TemporalOperation::Rate | TemporalOperation::Increase
                )
            {
                return Err(invalid("invalid exact range-counter index contract"));
            }
        }
        if let Self::ExactSubquery { query } = self {
            let parsed = parser::parse(query).map_err(|e| invalid(e.to_string()))?;
            if matches!(parsed, Expr::MatrixSelector(_) | Expr::Subquery(_)) {
                return Err(invalid(
                    "exact subtree boundary must return scalar or instant vector",
                ));
            }
        }
        if let Self::Subquery {
            range_ms, step_ms, ..
        } = self
        {
            if *range_ms == 0 || *step_ms == 0 || range_ms / step_ms > 100_000 {
                return Err(invalid("invalid or excessive subquery grid"));
            }
        }
        Ok(())
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
        operator: LogicalOperator,
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
            LogicalOperator::Scan {
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
                self.operation(LogicalOperator::UnaryNegate, vec![input])
            }
            Expr::VectorSelector(s) => self.scan(s, None),
            Expr::MatrixSelector(s) => self.scan(&s.vs, Some(millis(s.range)?)),
            Expr::Subquery(s) => {
                if s.at.is_some() {
                    return Err(invalid("logical subquery @ is unsupported"));
                }
                let input = self.lower(&s.expr)?;
                self.operation(
                    LogicalOperator::Subquery {
                        range_ms: millis(s.range)?,
                        step_ms: millis(
                            s.step
                                .ok_or_else(|| invalid("explicit subquery step required"))?,
                        )?,
                        offset_ms: offset(&s.offset)?,
                    },
                    vec![input],
                )
            }
            Expr::Aggregate(a) => {
                if a.param.is_some() {
                    return Err(invalid("parameterized aggregate unsupported"));
                }
                let operation = match a.op.to_string().as_str() {
                    "sum" => Aggregation::Sum,
                    "max" => Aggregation::Max,
                    "min" => Aggregation::Min,
                    "avg" => Aggregation::Avg,
                    "count" => Aggregation::Count,
                    other => return Err(invalid(format!("unsupported logical aggregate {other}"))),
                };
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
                let input = self.lower(&a.expr)?;
                self.operation(
                    LogicalOperator::Aggregate {
                        operation,
                        grouping,
                    },
                    vec![input],
                )
            }
            Expr::Call(c) => {
                let operator = match c.func.name {
                    "histogram_quantile" => LogicalOperator::HistogramQuantile,
                    "sort" => LogicalOperator::Sort { descending: false },
                    "sort_desc" => LogicalOperator::Sort { descending: true },
                    name => LogicalOperator::Temporal {
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
                    LogicalOperator::Binary {
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

impl QueryPlanEntry {
    /// Lower a Planner-authorized native residual into typed backend operations.
    /// Callers retain a separate external-native alternative for cost comparison.
    pub fn compile_logical(
        query_id: String,
        canonical_promql: String,
        instant: InstantExecution,
        fallback: FallbackPolicy,
    ) -> Result<Self, QueryPlanError> {
        let expr = parser::parse(&canonical_promql).map_err(|e| invalid(e.to_string()))?;
        let mut lower = Lower {
            nodes: BTreeMap::new(),
            seen: BTreeMap::new(),
        };
        let root = lower.lower(&expr)?;
        let entry = Self {
            query_id,
            canonical_promql,
            root,
            nodes: lower.nodes,
            instant,
            fallback,
        };
        entry.validate(&Default::default())?;
        Ok(entry)
    }
    /// Promote only a wholly native entry; never discard selected summary bindings.
    pub fn lower_native_residual(&self) -> Result<Self, QueryPlanError> {
        if self.nodes.len() != 1
            || !matches!(
                self.nodes.get(&self.root),
                Some(QueryPlanNode::ExactFallback { .. })
            )
        {
            return Err(invalid(
                "logical residual promotion requires a whole native root",
            ));
        }
        Self::compile_logical(
            self.query_id.clone(),
            self.canonical_promql.clone(),
            self.instant,
            self.fallback,
        )
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
    for expression in expressions {
        if let Ok(candidate) = crate::query_parser::parse_query_expr_canonical(
            &expression.to_string(),
            planner_types::types::AccuracyTarget::Exact,
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
    Err(invalid(
        "Planner residual does not match any original query subtree",
    ))
}

pub(super) fn binary_operator(
    operator: &planner_types::post_asap::BinaryOperator,
) -> Result<LogicalOperator, QueryPlanError> {
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
    Ok(LogicalOperator::Binary {
        operation,
        return_bool: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
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
            let entry = QueryPlanEntry::compile_logical(
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
        assert!(nodes.values().any(|node| matches!(node, QueryPlanNode::Logical { operator: LogicalOperator::Scan { matchers, .. }, .. } if matchers.iter().any(|m| m.name == "job" && m.value == "api"))));
        assert!(residual_nodes("sum(rate(other_total[5m]))", &residual).is_err());
    }
    #[test]
    fn repeated_subexpressions_share_node_identity() {
        // Serialized edges must retain CSE rather than duplicating raw work.
        let entry = QueryPlanEntry::compile_logical(
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
        assert!(LogicalOperator::HistogramQuantile.validate(1).is_err());
        assert!(LogicalOperator::Subquery {
            range_ms: 60_000,
            step_ms: 0,
            offset_ms: 0
        }
        .validate(1)
        .is_err());
    }
}

/// Prove a physical-native substitute represents exactly the selected summary leaf.
/// A second Planner invocation is an equality witness, not a replacement selection.
pub(crate) fn selected_residual_nodes(
    original: &str,
    selected: &planner_types::post_asap::SummaryNode,
) -> Result<(QueryNodeId, BTreeMap<QueryNodeId, QueryPlanNode>), QueryPlanError> {
    if !selected.guarantee.as_ref().is_some_and(|g| g.is_exact()) {
        return Err(invalid(
            "native residual substitution requires an exact selected value",
        ));
    }
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
    let mut matched = None;
    for expression in expressions {
        let Ok(canonical) = crate::query_parser::parse_query_expr_canonical(
            &expression.to_string(),
            planner_types::types::AccuracyTarget::Exact,
        ) else {
            continue;
        };
        let Ok(witness) = crate::planner_selection::select_summary_default(&canonical) else {
            continue;
        };
        if witness.as_ref() == selected {
            let mut lower = Lower {
                nodes: BTreeMap::new(),
                seen: BTreeMap::new(),
            };
            let root = lower.lower(expression)?;
            let candidate = (root, lower.nodes);
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
    matched.ok_or_else(|| {
        invalid("selected summary leaf has no semantically identical original subtree witness")
    })
}

/// Read the original aggregate operation only after proving its selected-node identity.
/// Min and max share a Planner accumulator family, so the family name alone is insufficient.
pub(super) fn selected_aggregate_operator(
    original: &str,
    selected: &planner_types::post_asap::SummaryNode,
) -> Result<LogicalOperator, QueryPlanError> {
    let (root, nodes) = selected_residual_nodes(original, selected)?;
    match nodes.get(&root) {
        Some(QueryPlanNode::Logical {
            operator: operator @ LogicalOperator::Aggregate { .. },
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
    fn selected_summary_and_filtered_residual_share_installed_binary() {
        // Both filtered and unfiltered leaves bind independently.
        let query = "sum_over_time(m[5m]) + sum_over_time(m{job=\"api\"}[5m])";
        let canonical = crate::query_parser::parse_query_expr_canonical(
            query,
            planner_types::types::AccuracyTarget::Exact,
        )
        .unwrap();
        let selected = crate::planner_selection::select_summary_default(&canonical).unwrap();
        let entry =
            QueryPlanEntry::compile_bound_composable(
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
                        crate::physical::compiler::materialization_leaf_contract(node)
                            .map_err(QueryPlanError::Invalid)?;
                    Ok(MaterializationBinding {
                        materialization: asap_types::PolicyFingerprint(
                            if spatial_filter.is_empty() { 7 } else { 8 },
                        ),
                        metric: "m".into(),
                        sid_grouping: vec![],
                        output_grouping: PhysicalGrouping::PerEntity,
                        window_ms: 300_000,
                        readout_lookback_ms: Some(300_000),
                    })
                },
            )
            .unwrap();
        assert_eq!(entry.materialization_bindings().len(), 2);
        assert!(!entry.nodes.values().any(|node| matches!(
            node,
            QueryPlanNode::Logical {
                operator: LogicalOperator::ExactSubquery { .. },
                ..
            }
        )));
        assert!(!entry.nodes.values().any(|node| matches!(
            node,
            QueryPlanNode::Logical {
                operator: LogicalOperator::Scan { .. },
                ..
            }
        )));
        assert!(matches!(
            entry.nodes[&entry.root],
            QueryPlanNode::Logical {
                operator: LogicalOperator::Binary { .. },
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
        let selected = crate::planner_selection::select_summary_default(&canonical).unwrap();
        assert!(
            selected_residual_nodes("sum_over_time(m{job=\"worker\"}[5m])", &selected).is_err()
        );
    }
}

#[cfg(test)]
mod planner_workload_tests {
    use super::*;
    use crate::physical::compiler::{BackendLocalPlanningSnapshot, PhysicalCompiler};

    fn lookback(expr: &Expr) -> u64 {
        match expr {
            Expr::MatrixSelector(e) => e.range.as_millis() as u64,
            Expr::Subquery(e) => (e.range.as_millis() as u64).max(lookback(&e.expr)),
            Expr::Aggregate(e) => lookback(&e.expr),
            Expr::Paren(e) => lookback(&e.expr),
            Expr::Unary(e) => lookback(&e.expr),
            Expr::Binary(e) => lookback(&e.lhs).max(lookback(&e.rhs)),
            Expr::Call(e) => e.args.args.iter().map(|e| lookback(e)).max().unwrap_or(0),
            _ => 0,
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
            let window = lookback(&parser::parse(query).unwrap());
            entry["time_selection"]["lookback"] =
                (if window == 0 { 300_000 } else { window }).into();
            entries.push(entry);
        }
        fixture["query_workload"]["repeating_queries"] = entries.into();
        let snapshot: BackendLocalPlanningSnapshot = serde_json::from_value(fixture).unwrap();
        let (request, environment) = snapshot.planning_request().unwrap();
        assert!(request.local_raw_execution);
        let plan = PhysicalCompiler.compile(request, environment).unwrap();
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
        let selected = crate::planner_selection::select_summary_default(&canonical).unwrap();
        let operator = selected_aggregate_operator(query, &selected).unwrap();
        assert!(matches!(
            operator,
            LogicalOperator::Aggregate {
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
        let selected = crate::planner_selection::select_summary_default(&canonical).unwrap();
        let maximum = crate::query_parser::parse_query_expr_canonical(
            "max(m)",
            planner_types::types::AccuracyTarget::Exact,
        )
        .unwrap();
        let maximum = crate::planner_selection::select_summary_default(&maximum).unwrap();
        let result = selected_residual_nodes("min(m) + max(m)", &selected);
        if selected == maximum {
            assert!(result.is_err());
        } else {
            let (root, nodes) = result.unwrap();
            assert!(matches!(
                nodes[&root],
                QueryPlanNode::Logical {
                    operator: LogicalOperator::Aggregate {
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
) -> Result<Option<LogicalOperator>, QueryPlanError> {
    use planner_types::post_asap::{ExactKind, SummaryExpr, SummaryFamilyType};
    if !matches!(
        &node.expr,
        SummaryExpr::SummaryAgg {
            family: SummaryFamilyType::ExactAggregate(ExactKind::MinMax, _),
            reduction: planner_types::pre_asap::Reduction::PerEntity,
            ..
        }
    ) {
        return Ok(None);
    }
    let (root, nodes) = selected_residual_nodes(original, node)?;
    let Some(QueryPlanNode::Logical {
        operator:
            LogicalOperator::Temporal {
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
            LogicalOperator::Scan {
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
    Ok(Some(LogicalOperator::ReadRangeMaxIndex {
        metric: metric.clone(),
        matchers: matchers.clone(),
        range_ms: *range_ms,
        retention_ms: *range_ms,
    }))
}

#[cfg(test)]
mod range_max_index_tests {
    use super::*;
    #[test]
    fn real_gauge_queries_have_planner_authorized_exact_indexes() {
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
            let selected = crate::planner_selection::select_summary_default(&original).unwrap();
            let index = selected_range_max_materialization(query, &selected)
                .unwrap()
                .unwrap();
            assert!(
                matches!(index, LogicalOperator::ReadRangeMaxIndex { metric: ref actual, range_ms: actual_range, .. } if actual == metric && actual_range == range_ms)
            );
            index.validate(0).unwrap();
            assert!(index.validate(1).is_err());
        }
    }
    #[test]
    fn min_and_shifted_or_nested_windows_do_not_become_max_indexes() {
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
            let selected = crate::planner_selection::select_summary_default(&original).unwrap();
            assert!(
                selected_range_max_materialization(query, &selected)
                    .unwrap()
                    .is_none(),
                "{query}"
            );
        }
    }
}

/// Stable contract identity used by priced physical alternatives, independent of node IDs.
pub fn materialization_key(operator: &LogicalOperator) -> Result<String, QueryPlanError> {
    let mut value = serde_json::to_value(operator).map_err(|e| invalid(e.to_string()))?;
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
) -> Option<LogicalOperator> {
    let QueryPlanNode::Logical {
        operator: LogicalOperator::Temporal { operation },
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
            LogicalOperator::Scan {
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
    Some(LogicalOperator::ReadRangeCounterIndex {
        metric: metric.clone(),
        matchers: matchers.clone(),
        range_ms: *range_ms,
        offset_ms: *offset_ms,
        operation: *operation,
        retention_ms: range_ms.saturating_add((*offset_ms).max(0) as u64),
    })
}

pub(crate) fn selected_counter_materialization(
    original: &str,
    node: &planner_types::post_asap::SummaryNode,
) -> Result<Option<LogicalOperator>, QueryPlanError> {
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
    Ok(counter_contract(root, &nodes))
}

/// A kept logical subtree may contain rate operations. Prove each physical index
/// implementation with Planner at compile time, then match the retained typed
/// operator exactly. The installed parent DAG and its result combination stay intact.
pub(super) fn promote_counter_indexes(entry: &mut QueryPlanEntry) -> Result<(), QueryPlanError> {
    fn visit(expr: &Expr, calls: &mut Vec<String>) {
        match expr {
            Expr::Call(e) => {
                if matches!(e.func.name, "rate" | "increase") {
                    calls.push(expr.to_string());
                }
                for arg in &e.args.args {
                    visit(arg, calls);
                }
            }
            Expr::Paren(e) => visit(&e.expr, calls),
            Expr::Unary(e) => visit(&e.expr, calls),
            Expr::Subquery(e) => visit(&e.expr, calls),
            Expr::Aggregate(e) => visit(&e.expr, calls),
            Expr::Binary(e) => {
                visit(&e.lhs, calls);
                visit(&e.rhs, calls);
            }
            _ => {}
        }
    }
    let expression = parser::parse(&entry.canonical_promql).map_err(|e| invalid(e.to_string()))?;
    let mut calls = Vec::new();
    visit(&expression, &mut calls);
    let mut proven = std::collections::BTreeSet::new();
    for call in calls {
        let parsed = crate::query_parser::parse_query_expr_canonical(
            &call,
            planner_types::types::AccuracyTarget::Exact,
        )
        .map_err(|e| invalid(e.to_string()))?;
        let selected = crate::planner_selection::select_summary_default(&parsed)
            .map_err(|e| invalid(e.to_string()))?;
        if let Some(operator) = selected_counter_materialization(&call, &selected)? {
            proven.insert(materialization_key(&operator)?);
        }
    }
    let mut changes = Vec::new();
    for id in entry.nodes.keys() {
        if let Some(operator) = counter_contract(*id, &entry.nodes) {
            if proven.contains(&materialization_key(&operator)?) {
                changes.push((*id, operator));
            }
        }
    }
    for (id, operator) in changes {
        entry.nodes.insert(
            id,
            QueryPlanNode::Logical {
                operator,
                inputs: vec![],
            },
        );
    }
    prune(entry);
    Ok(())
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

/// Disabling one materialization leaf restores its Prometheus exact subtree, not the whole query.
pub fn apply_materialization_policy(
    entry: &mut QueryPlanEntry,
    policy: Option<&std::collections::BTreeSet<String>>,
) -> Result<(), QueryPlanError> {
    if let Some(policy) = policy {
        for node in entry.nodes.values_mut() {
            let QueryPlanNode::Logical { operator, inputs } = node else {
                continue;
            };
            if let Some(query) = operator.exact_promql()? {
                if !policy.contains(&materialization_key(operator)?) {
                    *operator = LogicalOperator::ExactSubquery { query };
                    inputs.clear();
                }
            }
        }
    }
    externalize_residuals(entry)?;
    assign_retention(entry)?;
    Ok(())
}

pub fn materialization_candidate_keys(
    original: &str,
    selected: &std::rc::Rc<planner_types::post_asap::SummaryNode>,
) -> Result<std::collections::BTreeSet<String>, QueryPlanError> {
    use planner_types::post_asap::SummaryExpr;
    fn visit(
        original: &str,
        node: &std::rc::Rc<planner_types::post_asap::SummaryNode>,
        keys: &mut std::collections::BTreeSet<String>,
    ) -> Result<(), QueryPlanError> {
        if let Some(operator) = selected_counter_materialization(original, node)?
            .or(selected_range_max_materialization(original, node)?)
        {
            keys.insert(materialization_key(&operator)?);
        }
        match &node.expr {
            SummaryExpr::BinaryOp { lhs, rhs, .. } => {
                visit(original, lhs, keys)?;
                visit(original, rhs, keys)?;
            }
            SummaryExpr::SummaryAgg { child, .. } => visit(original, child, keys)?,
            SummaryExpr::SummaryEstimate { summary_input, .. }
            | SummaryExpr::SummaryDelete { summary_input, .. } => {
                visit(original, summary_input, keys)?
            }
            SummaryExpr::SummaryMerge { children } => {
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

impl LogicalOperator {
    /// Exact fallback expression belongs to the installed contract, never reparsed from user input at serving time.
    pub fn exact_promql(&self) -> Result<Option<String>, QueryPlanError> {
        let (metric, matchers, range, shift, function) = match self {
            Self::ReadRangeCounterIndex {
                metric,
                matchers,
                range_ms,
                offset_ms,
                operation,
                ..
            } => (
                metric,
                matchers,
                range_ms,
                *offset_ms,
                match operation {
                    TemporalOperation::Rate => "rate",
                    TemporalOperation::Increase => "increase",
                    _ => return Err(invalid("invalid counter operation")),
                },
            ),
            Self::ReadRangeMaxIndex {
                metric,
                matchers,
                range_ms,
                ..
            } => (metric, matchers, range_ms, 0, "max_over_time"),
            _ => return Ok(None),
        };
        let labels = matchers
            .iter()
            .map(|m| {
                let op = match m.operation {
                    LabelMatch::Equal => "=",
                    LabelMatch::NotEqual => "!=",
                    LabelMatch::Regex => "=~",
                    LabelMatch::NotRegex => "!~",
                };
                Ok(format!(
                    "{}{op}{}",
                    m.name,
                    serde_json::to_string(&m.value).map_err(|e| invalid(e.to_string()))?
                ))
            })
            .collect::<Result<Vec<_>, QueryPlanError>>()?
            .join(",");
        let shifted = if shift == 0 {
            String::new()
        } else {
            format!(
                " offset {}{}ms",
                if shift < 0 { "-" } else { "" },
                shift.unsigned_abs()
            )
        };
        let query = format!("{function}({metric}{{{labels}}}[{range}ms]{shifted})");
        let parsed = parser::parse(&query).map_err(|e| invalid(e.to_string()))?;
        Ok(Some(parsed.to_string()))
    }
}

fn expression_shape(
    id: QueryNodeId,
    nodes: &BTreeMap<QueryNodeId, QueryPlanNode>,
) -> Result<String, QueryPlanError> {
    let node = nodes
        .get(&id)
        .ok_or_else(|| invalid("missing expression node"))?;
    if let QueryPlanNode::Logical {
        operator: LogicalOperator::ExactSubquery { query },
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

/// Collapse only maximal raw residual subtrees whose full typed expression is
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
    let expr = parser::parse(&entry.canonical_promql).map_err(|e| invalid(e.to_string()))?;
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
        );
        let mut exact = false;
        if let QueryPlanNode::Logical { operator, .. } = node {
            indexed |= matches!(
                operator,
                LogicalOperator::ReadRangeMaxIndex { .. }
                    | LogicalOperator::ReadRangeCounterIndex { .. }
            );
            exact = matches!(
                operator,
                LogicalOperator::Scan { .. } | LogicalOperator::ExactSubquery { .. }
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
        let (indexed, exact) = flags(id, &entry.nodes);
        if !indexed && exact {
            if let Ok(shape) = expression_shape(id, &entry.nodes) {
                if let Some(query) = witnesses.get(&shape) {
                    entry.nodes.insert(
                        id,
                        QueryPlanNode::Logical {
                            operator: LogicalOperator::ExactSubquery {
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
                operator: LogicalOperator::Scan { .. },
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
            match operator {
                LogicalOperator::Subquery {
                    range_ms,
                    offset_ms,
                    ..
                } => {
                    child_depth = depth
                        .checked_add(*range_ms)
                        .and_then(|v| v.checked_add((*offset_ms).max(0) as u64))
                        .ok_or_else(|| invalid("retention overflow"))?;
                }
                LogicalOperator::ReadRangeCounterIndex {
                    range_ms,
                    offset_ms,
                    retention_ms,
                    ..
                } => {
                    *retention_ms = depth
                        .checked_add(*range_ms)
                        .and_then(|v| v.checked_add((*offset_ms).max(0) as u64))
                        .ok_or_else(|| invalid("retention overflow"))?;
                }
                LogicalOperator::ReadRangeMaxIndex {
                    range_ms,
                    retention_ms,
                    ..
                } => {
                    *retention_ms = depth
                        .checked_add(*range_ms)
                        .ok_or_else(|| invalid("retention overflow"))?;
                }
                _ => {}
            }
        }
        pending.extend(node.inputs().iter().map(|child| (*child, child_depth)));
    }
    Ok(())
}

#[cfg(test)]
mod remote_boundary_regressions {
    use super::*;
    // Population conjunction order and consumer retention cannot create ghost mask keys.
    #[test]
    fn policy_identity_is_independent_of_matcher_order_and_retention() {
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
        let a = LogicalOperator::ReadRangeCounterIndex {
            metric: "requests".into(),
            matchers: vec![first.clone(), second.clone()],
            range_ms: 300_000,
            offset_ms: 0,
            operation: TemporalOperation::Rate,
            retention_ms: 300_000,
        };
        let b = LogicalOperator::ReadRangeCounterIndex {
            metric: "requests".into(),
            matchers: vec![second, first],
            range_ms: 300_000,
            offset_ms: 0,
            operation: TemporalOperation::Rate,
            retention_ms: 3_600_000,
        };
        assert_eq!(
            materialization_key(&a).unwrap(),
            materialization_key(&b).unwrap()
        );
    }
    // Both policies retain the original ratio but delegate only the disabled operand to Prometheus.
    #[test]
    fn real_error_ratio_has_remote_subtree_without_any_local_scan() {
        let query = "sum(rate(http_requests_total{job=\"order-service\",status=~\"5..\"}[5m])) / sum(rate(http_requests_total{job=\"order-service\"}[5m]))";
        let parsed = crate::query_parser::parse_query_expr_canonical(
            query,
            planner_types::types::AccuracyTarget::Exact,
        )
        .unwrap();
        let selected = crate::planner_selection::select_summary_default(&parsed).unwrap();
        let keys = materialization_candidate_keys(query, &selected).unwrap();
        assert_eq!(keys.len(), 2);
        for key in keys {
            let mut entry = QueryPlanEntry::compile_bound_composable(
                "ratio".into(),
                query.into(),
                &selected,
                InstantExecution {
                    lookback_ms: 300_000,
                    full_history: false,
                    cumulative_readout: true,
                },
                FallbackPolicy::ExactBackend,
                |_, _| Err(invalid("no pooled producer")),
            )
            .unwrap();
            apply_materialization_policy(&mut entry, Some(&[key].into_iter().collect())).unwrap();
            let count = |predicate: fn(&LogicalOperator) -> bool| {
                entry.nodes.values().filter(|node| matches!(node, QueryPlanNode::Logical { operator, .. } if predicate(operator))).count()
            };
            assert_eq!(
                count(|op| matches!(op, LogicalOperator::ReadRangeCounterIndex { .. })),
                1
            );
            assert_eq!(
                count(|op| matches!(op, LogicalOperator::ExactSubquery { .. })),
                1
            );
            assert_eq!(count(|op| matches!(op, LogicalOperator::Scan { .. })), 0);
            entry.validate(&Default::default()).unwrap();
        }
    }
}
