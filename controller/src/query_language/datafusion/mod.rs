//! DataFusion L1 backend — stub.
//!
//! The asap-fusion deployment model consumes a pre-built DataFusion
//! `LogicalPlan` upstream (its L1 happens in the caller's
//! `SessionContext`); see `controller/docs/design.md` §3 row 1. This
//! stub exists so a future asap-fusion adapter only has to fill in the
//! `parse` body.

use super::language::{Language, ParseError};
use super::language_ast::LanguageAst;
use crate::types_v2::QueryLanguage;

/// DataFusion implementation of the [`Language`] trait. Currently stubbed.
#[derive(Debug, Default, Clone, Copy)]
pub struct DataFusionLanguage;

impl Language for DataFusionLanguage {
    fn id(&self) -> QueryLanguage {
        QueryLanguage::DataFusion
    }

    fn parse(&self, _source: &str) -> Result<LanguageAst, ParseError> {
        Err(ParseError::Unimplemented(
            "DataFusion parser not implemented in DC deployment mode",
        ))
    }
}
