//! [`LanguageLogicalPlan`] — the L2 canonical representation.
//!
//! Per `controller/docs/design.md` §6 `core::logical_plan`, L2 is a
//! **per-language algebra tree** preserving language-specific
//! semantics (PromQL instant vs range vector, SQL window frames,
//! Elastic buckets) — `Aggregate { AggFunc }`, `Window`, `Filter`,
//! `Sort`, `Limit`. **No sketch names yet**.
//!
//! In DC's existing tree the equivalent representation is the
//! [`crate::intent_algebra::legacy_expr::QueryExpr`] tree produced by
//! `query_parser::parse_query_expr`. To stay consistent with the
//! design.md L2 contract while not duplicating the algebra:
//!
//! - The `PromQL` variant of [`LanguageLogicalPlan`] carries the
//!   already-built `QueryExpr` tree (which is the L2 representation
//!   for the PromQL language) **plus** the flat `ParsedQuery`
//!   summary needed by downstream analyzer/planner.
//! - Other languages get their own variants when implemented.
//!
//! The `intent_algebra` (L3) layer (Phase B, separate worktree) is
//! responsible for normalising this language-specific tree into the
//! language-orthogonal `QueryExpr` shape (e.g. dropping `PromQLSubquery`).
//! `histogram_quantile(...)` is no longer a legacy variant — Step γ5
//! substitutes it at the parser level into a plain `Aggregate{Quantile(φ)}`.

use std::collections::HashMap;
use std::time::Duration;

use crate::intent_algebra::legacy_expr::QueryExpr;
use crate::query_parser::{ParsedQuery, QueryHint};
use crate::types::AggType;
use crate::types_v2::QueryLanguage;

/// L2 canonical container — one variant per source language.
///
/// Each variant carries:
/// - the raw source string (for diagnostics + replan correlation),
/// - the language-specific algebra tree,
/// - a flat [`LanguageLogicalPlanSummary`] view that downstream
///   non-tree consumers (analyzer / planner / replan) can read
///   without walking the tree themselves.
#[derive(Debug, Clone)]
pub enum LanguageLogicalPlan {
    /// PromQL L2. Tree = `QueryExpr` from `query_parser::parse_query_expr`.
    PromQL {
        /// Original PromQL source.
        source: String,
        /// Algebra tree — the PromQL-flavored L2 shape.
        tree: QueryExpr,
        /// Flat summary projection of the tree.
        summary: LanguageLogicalPlanSummary,
    },
    // Future variants (kept commented to surface intent):
    // Sql        { source: String, tree: SqlTree,  summary: ... },
    // ElasticDsl { source: String, tree: EsTree,   summary: ... },
}

impl LanguageLogicalPlan {
    /// The originating [`QueryLanguage`] for this plan.
    pub fn language(&self) -> QueryLanguage {
        match self {
            LanguageLogicalPlan::PromQL { .. } => QueryLanguage::PromQL,
        }
    }

    /// Original source string this plan was lowered from.
    pub fn source(&self) -> &str {
        match self {
            LanguageLogicalPlan::PromQL { source, .. } => source,
        }
    }

    /// Borrow the flat summary projection.
    pub fn summary(&self) -> &LanguageLogicalPlanSummary {
        match self {
            LanguageLogicalPlan::PromQL { summary, .. } => summary,
        }
    }

    /// Borrow the algebra tree if the language is PromQL.
    pub fn as_promql_tree(&self) -> Option<&QueryExpr> {
        match self {
            LanguageLogicalPlan::PromQL { tree, .. } => Some(tree),
        }
    }
}

/// Flat, language-orthogonal projection of the L2 plan. This is the
/// shape downstream non-tree consumers (the legacy analyzer + planner
/// in DC) want.
///
/// It mirrors the fields on [`crate::query_parser::ParsedQuery`] so
/// adapters between the two layers stay trivial.
#[derive(Debug, Clone, Default)]
pub struct LanguageLogicalPlanSummary {
    /// Metric / table name extracted from the L2 tree.
    pub metric_name: String,
    /// Aggregation types present in the tree.
    pub aggregations: Vec<AggType>,
    /// `GROUP BY` / `by (dims)` keys.
    pub group_by_labels: Vec<String>,
    /// Equality label / `WHERE` filters.
    pub label_filters: HashMap<String, String>,
    /// Window size (PromQL `[w]` / SQL `TUMBLE`).
    pub time_window: Duration,
    /// Set when the query needs per-sample exact computation.
    pub exact_required: bool,
    /// Quantile φ values implied by the query.
    pub quantiles: Vec<f64>,
    /// Optional named pattern hint (DEBS classifier).
    pub hint: Option<QueryHint>,
}

impl LanguageLogicalPlanSummary {
    /// Lift a legacy [`ParsedQuery`] into a [`LanguageLogicalPlanSummary`].
    pub fn from_parsed_query(pq: &ParsedQuery) -> Self {
        Self {
            metric_name: pq.metric_name.clone(),
            aggregations: pq.aggregations.clone(),
            group_by_labels: pq.group_by_labels.clone(),
            label_filters: pq.label_filters.clone(),
            time_window: pq.time_window,
            exact_required: pq.exact_required,
            quantiles: pq.quantiles.clone(),
            hint: pq.hint.clone(),
        }
    }
}
