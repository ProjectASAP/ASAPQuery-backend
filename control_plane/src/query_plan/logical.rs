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
            Self::Scan { .. } => 0,
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
        // An unsupported filtered leaf must not discard its supported sibling's selected materialization.
        let query = "sum_over_time(m[5m]) + sum_over_time(m{job=\"api\"}[5m])";
        let canonical = crate::query_parser::parse_query_expr_canonical(
            query,
            planner_types::types::AccuracyTarget::Exact,
        )
        .unwrap();
        let selected = crate::planner_selection::select_summary_default(&canonical).unwrap();
        let entry = QueryPlanEntry::compile_bound_composable(
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
                crate::physical::compiler::materialization_leaf_contract(node)
                    .map_err(QueryPlanError::Invalid)?;
                Ok(MaterializationBinding {
                    materialization: asap_types::PolicyFingerprint(7),
                    metric: "m".into(),
                    sid_grouping: vec![],
                    output_grouping: PhysicalGrouping::PerEntity,
                    window_ms: 300_000,
                    readout_lookback_ms: Some(300_000),
                })
            },
        )
        .unwrap();
        assert_eq!(entry.materialization_bindings().len(), 1);
        assert!(entry.nodes.values().any(|node| matches!(node, QueryPlanNode::Logical { operator: LogicalOperator::Scan { matchers, .. }, .. } if matchers.iter().any(|m| m.name == "job" && m.value == "api"))));
        assert!(matches!(
            entry.nodes[&entry.root],
            QueryPlanNode::Logical {
                operator: LogicalOperator::Binary { .. },
                ..
            }
        ));
        entry
            .validate(&[asap_types::PolicyFingerprint(7)].into_iter().collect())
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
