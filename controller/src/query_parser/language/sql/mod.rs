//! SQL L1 backend — stub for the DC deployment mode.
//!
//! The DC deployment ships PromQL only; the SQL parser path remains
//! addressable through the legacy `query_parser::sql` entry point but
//! is not exposed via the new [`Language`] trait yet. A real impl wraps
//! `sqlparser` and emits a `SqlAst` analogous to [`super::promql::PromQLAst`].

use super::{Language, ParseError};
use super::language_ast::LanguageAst;
use crate::types_v2::QueryLanguage;

/// SQL implementation of the [`Language`] trait. Currently stubbed.
#[derive(Debug, Default, Clone, Copy)]
pub struct SqlLanguage;

impl Language for SqlLanguage {
    fn id(&self) -> QueryLanguage {
        QueryLanguage::Sql
    }

    fn parse(&self, _source: &str) -> Result<LanguageAst, ParseError> {
        Err(ParseError::Unimplemented(
            "SQL parser not implemented in DC deployment mode",
        ))
    }
}
