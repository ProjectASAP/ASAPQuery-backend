//! L1 `Language` trait — abstract over the per-language parser.
//!
//! See `controller/docs/design.md` §6 `core::query_language`.
//!
//! Every backend (PromQL, SQL, DataFusion, ElasticDSL) implements
//! [`Language`] and returns a [`LanguageAst`] variant tagged with the
//! same [`QueryLanguage`] enum the public `QuerySpec` carries.

use crate::types_v2::QueryLanguage;

use super::language_ast::LanguageAst;

// ── Errors ────────────────────────────────────────────────────────────────────

/// Errors a [`Language`] backend can return from `parse`.
///
/// `Unimplemented` is used by the stubbed-out non-PromQL backends so the
/// type system stays uniform without committing to a parser implementation.
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    /// The underlying language parser rejected the source string.
    #[error("parse failed for {language:?}: {source_err}")]
    Backend {
        /// Which backend rejected the source.
        language: QueryLanguage,
        /// Free-form parser-specific message.
        source_err: String,
    },

    /// The backend is registered but not implemented in this build (DC
    /// deployment mode ships PromQL only).
    #[error("{0}")]
    Unimplemented(&'static str),
}

impl ParseError {
    /// Convenience: wrap any `Display` parser error into [`ParseError::Backend`].
    pub fn backend(language: QueryLanguage, e: impl std::fmt::Display) -> Self {
        ParseError::Backend { language, source_err: e.to_string() }
    }
}

// ── Trait ─────────────────────────────────────────────────────────────────────

/// Per-language L1 parser façade. One impl per [`QueryLanguage`] variant.
///
/// `parse` is the only required method; `id` is a static tag so callers
/// can cross-check the variant returned by [`LanguageAst`] without
/// downcasting.
pub trait Language: Send + Sync {
    /// The [`QueryLanguage`] variant this backend implements.
    fn id(&self) -> QueryLanguage;

    /// Parse `source` into a language-flavored AST.
    fn parse(&self, source: &str) -> Result<LanguageAst, ParseError>;
}
