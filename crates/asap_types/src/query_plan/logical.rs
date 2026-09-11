//! Typed installed residual operators; no Planner selection or AST lowering.
use super::QueryPlanError;
use promql_parser::parser::{self, Expr};
use serde::{Deserialize, Serialize};
fn invalid(message: impl Into<String>) -> QueryPlanError {
    QueryPlanError::Invalid(message.into())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum LogicalOperator {
    /// A maximal exact scalar/vector subtree evaluated by Prometheus.
    ExactSubquery {
        query: String,
    },
    /// Prometheus exact subtree whose selectors are restricted at runtime by
    /// the candidate vector produced by its single input.
    CandidateExactSubquery {
        query: String,
        item_label: String,
    },
    Scan {
        metric: Option<String>,
        matchers: Vec<LabelMatcher>,
        range_ms: Option<u64>,
        offset_ms: i64,
    },
    UnaryNegate,
    VectorToScalar,
    Aggregate {
        operation: Aggregation,
        grouping: Grouping,
    },
    /// PromQL `topk(k, vector)` selection over values produced by the child.
    /// This is distinct from a frequency-sketch TopK readout: any exact or
    /// summary-backed instant-vector child may feed this query-time operator.
    TopKSelection {
        k: u64,
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

impl LogicalOperator {
    pub fn validate(&self, inputs: usize) -> Result<(), QueryPlanError> {
        let expected = match self {
            Self::Scan { .. } | Self::ExactSubquery { .. } => 0,
            Self::CandidateExactSubquery { .. } => 1,
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
        if let Self::ExactSubquery { query } | Self::CandidateExactSubquery { query, .. } = self {
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
