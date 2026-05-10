//! Layer 3 IR — `QueryExpr` DAG (intent-only, language-orthogonal,
//! deployment-independent).
//!
//! Per `controller/docs/design.md` §6 "`core::intent_algebra` — Layer 3"
//! (around line ~269). Pure intent at this layer: no language-specific
//! operators (no `HistogramQuantile`, no `PromQLSubquery`), no sketch
//! types, no sketch parameters, no physical operator choice.
//!
//! Single-root tree per query. Multi-root DAGs (cross-query CSE fan-in)
//! live one level above in `WorkloadPlan` (`types_v2::WorkloadPlan`).
//! Within-query CTE / let-binding fan-in *is* expressible here via
//! [`QueryExpr::LetBinding`] + [`QueryExpr::Ref`].
//!
//! Variant set (Phase B subset): `Scan`, `Window`, `Aggregate`,
//! `LetBinding`, `Ref`. The full design.md list is larger (`Filter`,
//! `Project`, `Partition`, `Distinct`, `Merge`, `Join`, `SetOp`, `Sort`,
//! `Limit`, `Subquery`, `WindowFunc`, `BinaryOp`); they are deferred to
//! follow-up phases as the planner grows consumers for them. The shape
//! defined here is forward-compatible — adding more variants is purely
//! additive.

#![allow(dead_code)]

use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::intent_algebra::agg_intent::AggIntent;
use crate::intent_algebra::schema::{Column, ColumnId, DataType, Schema};
use crate::types_v2::BindingName;

/// Errors produced when the L3 IR is constructed or its output schema is
/// derived. Surfaced by the lowering function and any caller that walks
/// the DAG.
#[derive(Debug, Error)]
pub enum QueryExprError {
    /// `Ref(name)` did not resolve against any in-scope `LetBinding`.
    #[error("unresolved ref: {0}")]
    UnresolvedRef(String),
    /// `Aggregate { by, .. }` referenced a column position that is not
    /// in the input schema. Caught at schema-derivation time per the
    /// design's locally-checkable invariant.
    #[error("by-column id {0} out of range (input has {1} columns)")]
    InvalidGroupByColumn(ColumnId, usize),
    /// `Window` requires a `time_index` field on its input schema —
    /// `design.md` §6 schema-flow table.
    #[error("Window requires a time_index on input schema")]
    WindowMissingTimeIndex,
}

/// Streaming / time-window kind. PromQL `[5m]` is `Sliding`; SQL `TUMBLE`
/// is `Tumbling`; PromQL has no native `Session` window so it stays
/// unused for the DC + PromQL scope of this PR.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WindowKind {
    Tumbling,
    Sliding,
    Session,
}

/// Source of a `Scan`. Phase B ships `TimeSeries` (the only shape DC +
/// PromQL needs); `Table` is sketched out so future deployment models
/// (asap-fusion / OLAP) plug in without an enum-shape rev. Recursive
/// `Source::Join` is design.md §6 line ~378 territory and stays out of
/// scope for now.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Source {
    /// PromQL / DC lifecycle leaf — a metric stream identified by name +
    /// optional label filters; produces `(timestamp, value, *labels)`
    /// columns.
    TimeSeries {
        metric: String,
    },
    /// Tabular leaf — reserved for asap-fusion. Carries the table name;
    /// columns ride on the supplied `Schema`.
    Table {
        table_ref: String,
    },
}

/// Equality label filter on a `Scan`. PromQL `{service="api"}` → one of
/// these; richer match operators (`!=`, `=~`, `!~`) live in `Filter`'s
/// generic predicate per design.md §6 line ~296 and are deferred to the
/// follow-up phase that adds the `Filter` variant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LabelFilter {
    pub label: String,
    pub equals: String,
}

/// Optional HAVING-style predicate on `Aggregate`. Modeled as an opaque
/// expression string at L3 — Phase B doesn't have a typed predicate IR
/// yet; adding one is a separate PR (would also introduce the `Filter`
/// variant per design.md §6).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HavingPredicate(pub String);

/// L3 algebra node. See module doc for the variant subset rationale.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "node", rename_all = "snake_case")]
pub enum QueryExpr {
    /// Outermost leaf — a metric stream / table read. `schema` is the
    /// authoritative output of this scan, supplied by the lowering pass
    /// (which consults the source / DB schema catalog at L1→L2 time).
    Scan {
        source: Source,
        #[serde(default)]
        label_filters: Vec<LabelFilter>,
        schema: Schema,
    },
    /// Streaming / time-window. Defines the lifecycle (flush / reset
    /// bounds) of any aggregate in its sub-tree.
    Window {
        kind: WindowKind,
        size: Duration,
        #[serde(default)]
        slide: Option<Duration>,
        child: Box<QueryExpr>,
    },
    /// γ + α — GROUP BY + aggregate intents. `by` are positional
    /// references into `child.output_schema().columns`; `aggs` carry
    /// `AggIntent` (no sketch types — that's L4).
    Aggregate {
        by: Vec<ColumnId>,
        aggs: Vec<AggIntent>,
        #[serde(default)]
        having: Option<HavingPredicate>,
        child: Box<QueryExpr>,
    },
    /// SQL `WITH name AS (expr) SELECT ... FROM name` / PromQL recording-
    /// rule binding. Names a sub-expression; references via `Ref(name)`.
    /// Output schema = `child`'s output schema.
    LetBinding {
        name: BindingName,
        expr: Box<QueryExpr>,
        child: Box<QueryExpr>,
    },
    /// Reference a `LetBinding` by name. Resolved at plan time; output
    /// schema = the named binding's expression's output schema.
    Ref {
        name: BindingName,
    },
}

impl QueryExpr {
    /// Compute the output schema of this node. Walks the tree, resolving
    /// `Ref` against `LetBinding`s in scope. Errors propagate per
    /// [`QueryExprError`].
    ///
    /// Callers that want the schema of the *root* of a single query call
    /// `expr.output_schema(&BindingScope::default())`.
    pub fn output_schema(&self) -> Result<Schema, QueryExprError> {
        self.output_schema_in(&BindingScope::default())
    }

    /// Variant of [`Self::output_schema`] that takes an explicit binding
    /// scope. Used internally during DAG walks; exposed for callers that
    /// pre-populate bindings from a workload-level container.
    pub fn output_schema_in(&self, scope: &BindingScope) -> Result<Schema, QueryExprError> {
        match self {
            QueryExpr::Scan { schema, .. } => Ok(schema.clone()),
            QueryExpr::Window { child, .. } => {
                let in_schema = child.output_schema_in(scope)?;
                if in_schema.time_index.is_none() {
                    return Err(QueryExprError::WindowMissingTimeIndex);
                }
                // Window propagates row identity → carries unique_keys
                // verbatim. Synthetic `window_id` / `window_start/end`
                // columns (design.md §6 schema-flow row 6) are deferred
                // until the planner consumes them.
                Ok(in_schema)
            }
            QueryExpr::Aggregate {
                by, aggs, child, ..
            } => {
                let in_schema = child.output_schema_in(scope)?;
                // by-column ids must be in range.
                let mut out_cols: Vec<Column> = Vec::with_capacity(by.len() + aggs.len());
                for &id in by {
                    let c = in_schema.columns.get(id).ok_or(
                        QueryExprError::InvalidGroupByColumn(id, in_schema.columns.len()),
                    )?;
                    out_cols.push(c.clone());
                }
                // One new column per intent. PromQL convention:
                // intent applied to the synthetic `value` column when
                // present; otherwise to the first non-grouped column.
                let value_col_idx = in_schema
                    .column_id("value")
                    .or_else(|| (0..in_schema.columns.len()).find(|i| !by.contains(i)));
                let probe = value_col_idx
                    .and_then(|i| in_schema.columns.get(i))
                    .cloned()
                    .unwrap_or(Column {
                        name: "value".into(),
                        dtype: DataType::Float64,
                        nullable: false,
                    });
                for intent in aggs {
                    out_cols.push(intent.output_column(&probe));
                }
                // Output unique_keys = [by]. The group-by column tuple
                // is unique in the output by construction (design.md §6
                // schema-flow table).
                let unique_keys = if by.is_empty() {
                    Vec::new()
                } else {
                    vec![(0..by.len()).collect()]
                };
                // Aggregate strips the time axis — output is one row per
                // group, not one row per timestamp.
                Ok(Schema {
                    columns: out_cols,
                    time_index: None,
                    unique_keys,
                })
            }
            QueryExpr::LetBinding { name, expr, child } => {
                // Bind `name` to `expr`'s output schema, then evaluate
                // `child` in the extended scope.
                let bound = expr.output_schema_in(scope)?;
                let extended = scope.with(name.clone(), bound);
                child.output_schema_in(&extended)
            }
            QueryExpr::Ref { name } => scope
                .lookup(name)
                .cloned()
                .ok_or_else(|| QueryExprError::UnresolvedRef(name.as_str().into())),
        }
    }
}

/// Lexical scope for `LetBinding` / `Ref` resolution. A persistent map
/// from binding name to the bound expression's output schema. Built
/// during the schema-derivation walk; the caller usually starts with
/// [`BindingScope::default()`].
#[derive(Debug, Default, Clone)]
pub struct BindingScope {
    bindings: HashMap<String, Schema>,
}

impl BindingScope {
    /// Empty scope — no in-scope bindings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return a new scope with `name` bound to `schema`. The original
    /// scope is left unchanged (functional style — keeps recursion
    /// shadow semantics correct).
    pub fn with(&self, name: BindingName, schema: Schema) -> Self {
        let mut bindings = self.bindings.clone();
        bindings.insert(name.as_str().into(), schema);
        Self { bindings }
    }

    /// Look up `name` in the current scope. `None` if unbound.
    pub fn lookup(&self, name: &BindingName) -> Option<&Schema> {
        self.bindings.get(name.as_str())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent_algebra::schema::{Column, DataType};
    use crate::types_v2::AccuracyTarget;

    fn col(name: &str, dtype: DataType) -> Column {
        Column {
            name: name.into(),
            dtype,
            nullable: false,
        }
    }

    fn ts_scan() -> QueryExpr {
        QueryExpr::Scan {
            source: Source::TimeSeries {
                metric: "http_request_duration_seconds".into(),
            },
            label_filters: vec![LabelFilter {
                label: "service".into(),
                equals: "api".into(),
            }],
            schema: Schema::with_time_index(
                vec![
                    col("ts", DataType::Timestamp),
                    col("service", DataType::Utf8),
                    col("value", DataType::Float64),
                ],
                0,
                vec![vec![0, 1]],
            ),
        }
    }

    #[test]
    fn query_expr_simple_aggregate() {
        let expr = QueryExpr::Aggregate {
            by: vec![1], // service
            aggs: vec![AggIntent::Quantile {
                q: 0.99,
                accuracy: AccuracyTarget::Epsilon(0.01),
            }],
            having: None,
            child: Box::new(QueryExpr::Window {
                kind: WindowKind::Sliding,
                size: Duration::from_secs(300),
                slide: None,
                child: Box::new(ts_scan()),
            }),
        };
        let schema = expr.output_schema().unwrap();
        // Output: [service, quantile_0_99]
        assert_eq!(schema.columns.len(), 2);
        assert_eq!(schema.columns[0].name, "service");
        assert_eq!(schema.columns[1].name, "quantile_0_99");
        // unique_keys = [by] — the by columns project to positions [0..by.len())
        // in the output schema.
        assert_eq!(schema.unique_keys, vec![vec![0]]);
        // Aggregate strips the time axis.
        assert!(schema.time_index.is_none());
    }

    #[test]
    fn query_expr_let_binding_ref() {
        // LetBinding{name="w", expr=Window over Scan,
        //            child=Aggregate{ child=Ref{"w"} }}
        let expr = QueryExpr::LetBinding {
            name: BindingName::new("w"),
            expr: Box::new(QueryExpr::Window {
                kind: WindowKind::Sliding,
                size: Duration::from_secs(300),
                slide: None,
                child: Box::new(ts_scan()),
            }),
            child: Box::new(QueryExpr::Aggregate {
                by: vec![1],
                aggs: vec![AggIntent::Max],
                having: None,
                child: Box::new(QueryExpr::Ref {
                    name: BindingName::new("w"),
                }),
            }),
        };
        let schema = expr.output_schema().unwrap();
        assert_eq!(schema.columns[0].name, "service");
        assert_eq!(schema.columns[1].name, "max");
        assert_eq!(schema.unique_keys, vec![vec![0]]);
    }

    #[test]
    fn query_expr_unresolved_ref_errors() {
        let expr = QueryExpr::Ref {
            name: BindingName::new("nope"),
        };
        let err = expr.output_schema().unwrap_err();
        assert!(matches!(err, QueryExprError::UnresolvedRef(s) if s == "nope"));
    }

    #[test]
    fn query_expr_window_requires_time_index() {
        let bad_scan = QueryExpr::Scan {
            source: Source::Table {
                table_ref: "t".into(),
            },
            label_filters: vec![],
            // Tabular scan with no time index.
            schema: Schema::new(vec![col("a", DataType::Int64)]),
        };
        let expr = QueryExpr::Window {
            kind: WindowKind::Tumbling,
            size: Duration::from_secs(60),
            slide: None,
            child: Box::new(bad_scan),
        };
        let err = expr.output_schema().unwrap_err();
        assert!(matches!(err, QueryExprError::WindowMissingTimeIndex));
    }

    #[test]
    fn query_expr_aggregate_invalid_by_column() {
        let expr = QueryExpr::Aggregate {
            by: vec![99],
            aggs: vec![AggIntent::Sum],
            having: None,
            child: Box::new(ts_scan()),
        };
        let err = expr.output_schema().unwrap_err();
        assert!(matches!(err, QueryExprError::InvalidGroupByColumn(99, _)));
    }

    #[test]
    fn query_expr_serde_roundtrip() {
        let expr = QueryExpr::Aggregate {
            by: vec![1],
            aggs: vec![AggIntent::Quantile {
                q: 0.99,
                accuracy: AccuracyTarget::Epsilon(0.01),
            }],
            having: None,
            child: Box::new(ts_scan()),
        };
        let json = serde_json::to_string(&expr).unwrap();
        let back: QueryExpr = serde_json::from_str(&json).unwrap();
        assert_eq!(expr, back);
    }

    /// `unique_keys` is the load-bearing CSE-legality hook (design.md §6
    /// line ~1284). Two `Ref` consumers can share a producer iff the
    /// producer's output schema has at least one provable unique-key set;
    /// without it, the deduper has to be conservative and reuse drops on
    /// the floor.
    #[test]
    fn cse_substitution_legal_only_with_unique_keys() {
        // Producer 1: Scan → Window → Aggregate. Aggregate produces
        // `unique_keys = [by]` (a provable unique key). Two `Ref`
        // consumers can legally share this.
        let producer_with_uk = QueryExpr::Aggregate {
            by: vec![1],
            aggs: vec![AggIntent::Sum],
            having: None,
            child: Box::new(QueryExpr::Window {
                kind: WindowKind::Sliding,
                size: Duration::from_secs(300),
                slide: None,
                child: Box::new(ts_scan()),
            }),
        };
        let s1 = producer_with_uk.output_schema().unwrap();
        assert!(
            s1.has_unique_key(),
            "Aggregate must emit unique_keys = [by] per design.md §6 schema-flow"
        );

        // Producer 2: bare Scan with NO unique key declared. CSE deduper
        // would have to refuse to share this without further proof.
        let producer_without_uk = QueryExpr::Scan {
            source: Source::Table {
                table_ref: "t".into(),
            },
            label_filters: vec![],
            schema: Schema::new(vec![col("a", DataType::Int64)]),
        };
        let s2 = producer_without_uk.output_schema().unwrap();
        assert!(
            !s2.has_unique_key(),
            "no unique_keys → CSE deduper must conservatively refuse to share"
        );

        // The asymmetry is the design's claim: unique_keys is what makes
        // CSE substitution legal. Encoded here as a unit invariant so
        // downstream rewrites of the schema-flow rules can't silently
        // break it.
        assert_ne!(s1.has_unique_key(), s2.has_unique_key());
    }
}
