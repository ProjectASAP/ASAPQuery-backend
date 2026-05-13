//! `PromQLAst` — the L1 output of the PromQL backend.
//!
//! Thin wrapper that bundles the legacy parser's two outputs (the rich
//! `QueryExpr` algebra tree and the flat `ParsedQuery` summary) so
//! downstream L2 lowering can pick whichever shape it needs without
//! re-parsing.

use crate::intent_algebra::legacy_expr::QueryExpr;
use crate::query_parser::ParsedQuery;

/// PromQL parse output. Carries both the algebra tree and the flat
/// summary the legacy analyzer consumes.
///
/// We bundle both because the existing `query_parser::parse_query`
/// already produces them, and L2 lowering wants the structured tree
/// while back-compat callers (`Analyzer`) still want the flat summary.
#[derive(Debug, Clone)]
pub struct PromQLAst {
    /// Original PromQL source string (preserved for debugging / errors).
    pub source: String,
    /// Algebra tree — the existing `parse_query_expr` output.
    pub expr: QueryExpr,
    /// Flat summary — the existing `parse_query` output.
    pub summary: ParsedQuery,
}

impl PromQLAst {
    /// Construct from the legacy parser's two outputs.
    pub fn new(source: String, expr: QueryExpr, summary: ParsedQuery) -> Self {
        Self {
            source,
            expr,
            summary,
        }
    }

    /// Borrow the algebra tree.
    pub fn expr(&self) -> &QueryExpr {
        &self.expr
    }

    /// Borrow the flat summary.
    pub fn summary(&self) -> &ParsedQuery {
        &self.summary
    }
}
