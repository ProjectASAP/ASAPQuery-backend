//! ElasticDSL L1 backend — stub.
//!
//! Reserved for the future ElasticDSL deployment model
//! (`controller/docs/design.md` §3 row 1). No L1 parser is shipped in
//! the DC deployment build.

use super::{Language, ParseError};
use super::language_ast::LanguageAst;
use crate::types_v2::QueryLanguage;

/// ElasticDSL implementation of the [`Language`] trait. Currently stubbed.
#[derive(Debug, Default, Clone, Copy)]
pub struct ElasticDslLanguage;

impl Language for ElasticDslLanguage {
    fn id(&self) -> QueryLanguage {
        QueryLanguage::ElasticDsl
    }

    fn parse(&self, _source: &str) -> Result<LanguageAst, ParseError> {
        Err(ParseError::Unimplemented(
            "ElasticDSL parser not implemented in DC deployment mode",
        ))
    }
}
