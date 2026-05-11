//! L1 → L2 lowering — produces a [`LanguageLogicalPlan`] from a
//! [`crate::query_parser::language::LanguageAst`].
//!
//! Today only the PromQL backend has a working L1, so only the PromQL
//! variant of `LanguageAst` lowers to a real L2 tree. Other variants
//! return [`LoweringError::UnsupportedLanguage`] cleanly so callers
//! can surface a uniform error.

use crate::query_parser::language::{language_ast::LanguageAst, promql::PromQLAst};
use crate::types_v2::QueryLanguage;

use super::plan::{LanguageLogicalPlan, LanguageLogicalPlanSummary};

/// Errors surfaced by the L1 → L2 lowering pass.
#[derive(Debug, thiserror::Error)]
pub enum LoweringError {
    /// The L1 AST is for a language whose L2 lowering is not yet implemented.
    #[error("L2 lowering not implemented for {0:?}")]
    UnsupportedLanguage(QueryLanguage),
}

/// Lower an L1 [`LanguageAst`] into an L2 [`LanguageLogicalPlan`].
///
/// The PromQL backend's L2 representation is the existing
/// `algebra::expr::QueryExpr` tree (already produced by
/// `query_parser::parse_query_expr` and stashed inside [`PromQLAst`]).
/// We just re-tag it as the PromQL L2 variant and project the flat
/// summary; no re-parsing or re-walking is required.
pub fn lower_to_logical_plan(
    ast: &LanguageAst,
) -> Result<LanguageLogicalPlan, LoweringError> {
    match ast {
        LanguageAst::PromQL(p) => Ok(lower_promql(p)),
    }
}

fn lower_promql(ast: &PromQLAst) -> LanguageLogicalPlan {
    LanguageLogicalPlan::PromQL {
        source: ast.source.clone(),
        tree: ast.expr.clone(),
        summary: LanguageLogicalPlanSummary::from_parsed_query(&ast.summary),
    }
}
