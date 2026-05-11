// L1 scaffolding ships ahead of any in-tree call site (the
// downstream `pipeline` driver lands in a later phase). Dead-code
// warnings are silenced here, not on individual items, so the public
// surface is uncluttered.
#![allow(dead_code, unused_imports)]

//! Layer 1 — `query_parser::language`.
//!
//! Per-language parser façade. See `controller/docs/design.md` §6
//! `core::query_language` for the design contract. Refactor 2026-05:
//! the former top-level `query_language/` module was folded into
//! `query_parser/language/` so the L1 parser frontend lives under one
//! module path.
//!
//! # Module layout
//!
//! ```text
//! query_parser/language/
//!   ├── mod.rs            — Language trait + ParseError (this file)
//!   ├── language_ast.rs   — LanguageAst sum type
//!   ├── promql/           — PromQL backend (active; wraps query_parser::promql)
//!   ├── sql/              — SQL backend (stub; Unimplemented)
//!   └── elastic_dsl/      — ElasticDSL backend (stub; Unimplemented)
//! ```
//!
//! # DC deployment scope
//!
//! Only [`promql::PromQLLanguage`] is implemented. The other backends
//! return [`ParseError::Unimplemented`] so the type system is uniform;
//! adding a real backend later is a localised change to the relevant
//! sub-module.

pub mod language_ast;
pub mod promql;
pub mod sql;
pub mod elastic_dsl;

pub use language_ast::LanguageAst;
pub use promql::PromQLLanguage;
pub use sql::SqlLanguage;
pub use elastic_dsl::ElasticDslLanguage;

#[cfg(test)]
mod tests;

use crate::types_v2::QueryLanguage;

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
