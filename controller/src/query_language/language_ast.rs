//! [`LanguageAst`] — the discriminated union over per-language ASTs.
//!
//! One variant per [`crate::types_v2::QueryLanguage`]. The PromQL variant
//! is the only one carrying real data today; the rest are reserved for
//! future deployment models (asap-fusion, ElasticDSL).

use super::promql::ast::PromQLAst;

/// Tagged union of language-flavored ASTs returned by [`super::Language::parse`].
///
/// Adding a new language is mechanical: add a new variant here and wire
/// up a backend in `query_language::<lang>::`.
#[derive(Debug, Clone)]
pub enum LanguageAst {
    /// PromQL AST — wraps the existing `query_parser::ParsedQuery` plus
    /// the full `QueryExpr` from the legacy parser.
    PromQL(PromQLAst),
    // Future backends (kept commented to surface intent):
    // Sql(SqlAst),
    // DataFusion(DfAst),
    // ElasticDsl(EsAst),
}

impl LanguageAst {
    /// Return `true` when this AST belongs to the PromQL backend.
    pub fn is_promql(&self) -> bool {
        matches!(self, LanguageAst::PromQL(_))
    }

    /// Borrow the inner `PromQLAst` if the variant is PromQL.
    pub fn as_promql(&self) -> Option<&PromQLAst> {
        match self {
            LanguageAst::PromQL(a) => Some(a),
        }
    }
}
