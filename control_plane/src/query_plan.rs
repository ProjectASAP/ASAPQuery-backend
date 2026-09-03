//! Authoritative serving plan compiled from one selected post-ASAP DAG.
//!
//! A query request may be parsed only to obtain its stable identity.  Family
//! selection, materialization matching and execution-shape construction all
//! happen here, before publication.  The data plane therefore never invokes
//! ASAPPlanner or searches the materialization catalog while serving.

use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use planner_types::post_asap::{
    ExactKind, ExactParams, GroupingStrategy, SketchKind, SketchQuery, SummaryExpr,
    SummaryFamilyType, SummaryField, SummaryNode, SummarySchema,
};
use planner_types::pre_asap::{ColumnRef, DataType, QueryExpr, Reduction};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use asap_types::{PolicyFingerprint, SummaryKind, SummaryParams};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct QueryPlan {
    pub plan_id: u64,
    pub entries: BTreeMap<String, QueryPlanEntry>,
}

impl QueryPlan {
    pub fn empty() -> Self {
        Self {
            plan_id: 0,
            entries: BTreeMap::new(),
        }
    }

    pub fn lookup(&self, promql: &str) -> Result<&QueryPlanEntry, QueryPlanError> {
        let identity = canonical_promql(promql)?;
        self.entries
            .get(&identity)
            .ok_or(QueryPlanError::QueryNotPlanned(identity))
    }

    pub fn validate(
        &self,
        materializations: &BTreeSet<PolicyFingerprint>,
    ) -> Result<(), QueryPlanError> {
        for (identity, entry) in &self.entries {
            if identity != &entry.canonical_promql {
                return Err(QueryPlanError::Invalid(format!(
                    "query map key `{identity}` differs from entry identity `{}`",
                    entry.canonical_promql
                )));
            }
            if entry.materializations.is_empty() {
                return Err(QueryPlanError::Invalid(format!(
                    "query `{identity}` has no bound materialization"
                )));
            }
            if let Some(missing) = entry
                .materializations
                .iter()
                .find(|fingerprint| !materializations.contains(fingerprint))
            {
                return Err(QueryPlanError::Invalid(format!(
                    "query `{identity}` references absent materialization {}",
                    missing.0
                )));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct QueryPlanEntry {
    pub query_id: String,
    pub canonical_promql: String,
    pub root: QueryPlanNode,
    pub materializations: BTreeSet<PolicyFingerprint>,
    pub fallback: FallbackPolicy,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FallbackPolicy {
    ExactBackend,
    Reject,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum QueryPlanNode {
    KeepPreAsap {
        logical: serde_json::Value,
        schema_names: Vec<String>,
    },
    SummaryAgg {
        child: Box<QueryPlanNode>,
        kind: SummaryKind,
        params: SummaryParams,
        col: ColumnRef,
        reduction: Reduction,
        schema_names: Vec<String>,
    },
    SummaryEstimate {
        summary_input: Box<QueryPlanNode>,
        query: QueryReadout,
        schema_names: Vec<String>,
    },
    SummaryMerge {
        children: Vec<QueryPlanNode>,
        schema_names: Vec<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum QueryReadout {
    Quantile {
        q: f64,
    },
    PointCount {
        key: ColumnRef,
        value: Option<String>,
    },
    Cardinality,
    TopK {
        k: usize,
    },
}

#[derive(Debug, Error)]
pub enum QueryPlanError {
    #[error("invalid PromQL query identity: {0}")]
    InvalidPromql(String),
    #[error("query is absent from the active QueryPlan: {0}")]
    QueryNotPlanned(String),
    #[error("post-ASAP DAG cannot be represented by the MVP query executor: {0}")]
    UnsupportedNode(String),
    #[error("invalid QueryPlan: {0}")]
    Invalid(String),
    #[error("invalid serialized pre-ASAP leaf: {0}")]
    InvalidLogicalLeaf(String),
}

/// Canonical textual identity used by both compilation and request lookup.
/// PromQL's parser/formatter normalizes whitespace and matcher formatting;
/// semantically different expressions retain different keys.
pub fn canonical_promql(query: &str) -> Result<String, QueryPlanError> {
    promql_parser::parser::parse(query.trim())
        .map(|expr| expr.to_string())
        .map_err(|error| QueryPlanError::InvalidPromql(error.to_string()))
}

impl QueryPlanNode {
    pub fn compile(node: &SummaryNode) -> Result<Self, QueryPlanError> {
        let schema_names = node
            .schema
            .fields
            .iter()
            .map(|field| field.name.clone())
            .collect();
        match &node.expr {
            SummaryExpr::KeepPreAsap(logical) => Ok(Self::KeepPreAsap {
                logical: serde_json::to_value(logical.as_ref())
                    .map_err(|error| QueryPlanError::InvalidLogicalLeaf(error.to_string()))?,
                schema_names,
            }),
            SummaryExpr::SummaryAgg {
                child,
                family,
                col,
                reduction,
                ..
            } => {
                let (kind, params) = flatten_family(family)?;
                Ok(Self::SummaryAgg {
                    child: Box::new(Self::compile(child)?),
                    kind,
                    params,
                    col: col.clone(),
                    reduction: reduction.clone(),
                    schema_names,
                })
            }
            SummaryExpr::SummaryEstimate {
                summary_input,
                query,
            } => Ok(Self::SummaryEstimate {
                summary_input: Box::new(Self::compile(summary_input)?),
                query: query.clone().into(),
                schema_names,
            }),
            SummaryExpr::SummaryMerge { children } => Ok(Self::SummaryMerge {
                children: children
                    .iter()
                    .map(|child| Self::compile(child))
                    .collect::<Result<Vec<_>, _>>()?,
                schema_names,
            }),
            SummaryExpr::SummaryJoin { .. } => {
                Err(QueryPlanError::UnsupportedNode("summary_join".into()))
            }
            SummaryExpr::SummarySubtract { .. } => {
                Err(QueryPlanError::UnsupportedNode("summary_subtract".into()))
            }
            SummaryExpr::SummaryDelete { .. } => {
                Err(QueryPlanError::UnsupportedNode("summary_delete".into()))
            }
        }
    }

    /// Rebuild the planner-owned semantic node without making a planning
    /// decision.  Schema names are retained because group IDs are positional;
    /// other schema/guarantee details are irrelevant to execution.
    pub fn to_summary_node(&self) -> Result<Rc<SummaryNode>, QueryPlanError> {
        let (expr, names) = match self {
            Self::KeepPreAsap {
                logical,
                schema_names,
            } => {
                let logical: QueryExpr = serde_json::from_value(logical.clone())
                    .map_err(|error| QueryPlanError::InvalidLogicalLeaf(error.to_string()))?;
                (SummaryExpr::KeepPreAsap(Rc::new(logical)), schema_names)
            }
            Self::SummaryAgg {
                child,
                kind,
                params,
                col,
                reduction,
                schema_names,
            } => (
                SummaryExpr::SummaryAgg {
                    child: child.to_summary_node()?,
                    family: expand_family(*kind, params.clone())?,
                    col: col.clone(),
                    reduction: reduction.clone(),
                    grouping: GroupingStrategy::default(),
                },
                schema_names,
            ),
            Self::SummaryEstimate {
                summary_input,
                query,
                schema_names,
            } => (
                SummaryExpr::SummaryEstimate {
                    summary_input: summary_input.to_summary_node()?,
                    query: query.clone().into(),
                },
                schema_names,
            ),
            Self::SummaryMerge {
                children,
                schema_names,
            } => (
                SummaryExpr::SummaryMerge {
                    children: children
                        .iter()
                        .map(Self::to_summary_node)
                        .collect::<Result<Vec<_>, _>>()?,
                },
                schema_names,
            ),
        };
        Ok(Rc::new(SummaryNode {
            expr,
            schema: execution_schema(names),
            guarantee: None,
        }))
    }
}

fn execution_schema(names: &[String]) -> SummarySchema {
    SummarySchema {
        fields: names
            .iter()
            .map(|name| SummaryField {
                name: name.clone(),
                dtype: SummaryFamilyType::Plain(DataType::Utf8),
                nullable: true,
            })
            .collect(),
        time_index: None,
    }
}

fn flatten_family(
    family: &SummaryFamilyType,
) -> Result<(SummaryKind, SummaryParams), QueryPlanError> {
    match family {
        SummaryFamilyType::ExactAggregate(kind, params) => {
            Ok((kind.clone().into(), params.clone().into()))
        }
        SummaryFamilyType::Sketch(kind, _) => {
            Ok((kind.clone().into(), kind.params().clone().into()))
        }
        other => Err(QueryPlanError::UnsupportedNode(format!(
            "summary family {other:?}"
        ))),
    }
}

fn expand_family(
    kind: SummaryKind,
    params: SummaryParams,
) -> Result<SummaryFamilyType, QueryPlanError> {
    if kind.is_exact() {
        let exact = match (kind, params) {
            (SummaryKind::Sum, SummaryParams::Sum) => (ExactKind::Sum, ExactParams::Sum),
            (SummaryKind::Count, SummaryParams::Count) => (ExactKind::Count, ExactParams::Count),
            (SummaryKind::MinMax, SummaryParams::MinMax) => {
                (ExactKind::MinMax, ExactParams::MinMax)
            }
            (SummaryKind::Increase, SummaryParams::Increase) => {
                (ExactKind::Increase, ExactParams::Increase)
            }
            (SummaryKind::Rate, SummaryParams::Rate) => (ExactKind::Rate, ExactParams::Rate),
            (kind, params) => {
                return Err(QueryPlanError::Invalid(format!(
                    "mismatched exact family {kind:?}/{params:?}"
                )))
            }
        };
        return Ok(SummaryFamilyType::ExactAggregate(exact.0, exact.1));
    }
    let algorithm = kind
        .as_sketch_kind()
        .ok_or_else(|| QueryPlanError::Invalid(format!("not a sketch kind: {kind:?}")))?;
    let sketch_params = params
        .as_sketch_params()
        .ok_or_else(|| QueryPlanError::Invalid(format!("not sketch params: {params:?}")))?;
    Ok(SummaryFamilyType::Sketch(
        SketchKind::new(algorithm, sketch_params),
        GroupingStrategy::default(),
    ))
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
}
