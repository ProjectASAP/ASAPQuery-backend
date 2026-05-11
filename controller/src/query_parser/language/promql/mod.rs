//! PromQL L1 backend — wraps the existing `query_parser::promql` parser
//! behind the [`super::Language`] trait.
//!
//! No new parsing logic lives here; this is purely an adapter that
//! collects the legacy parser's two outputs into a [`PromQLAst`] and
//! tags it as [`LanguageAst::PromQL`].

pub mod ast;

use super::{Language, ParseError};
use super::language_ast::LanguageAst;
use crate::query_parser::{parse_query, parse_query_expr};
use crate::types_v2::QueryLanguage;

pub use ast::PromQLAst;

/// PromQL implementation of the [`Language`] trait.
///
/// Construct with `PromQLLanguage::default()`; the type is unit so it
/// is cheap to clone / store in a registry.
#[derive(Debug, Default, Clone, Copy)]
pub struct PromQLLanguage;

impl Language for PromQLLanguage {
    fn id(&self) -> QueryLanguage {
        QueryLanguage::PromQL
    }

    fn parse(&self, source: &str) -> Result<LanguageAst, ParseError> {
        // Delegate to the existing parser — both entry points re-parse the
        // same string today; the cost is negligible (microseconds) and we
        // get the legacy `ParsedQuery` for free for back-compat callers.
        let expr = parse_query_expr(source)
            .map_err(|e| ParseError::backend(QueryLanguage::PromQL, e))?;
        let summary = parse_query(source)
            .map_err(|e| ParseError::backend(QueryLanguage::PromQL, e))?;
        Ok(LanguageAst::PromQL(PromQLAst::new(source.to_string(), expr, summary)))
    }
}
