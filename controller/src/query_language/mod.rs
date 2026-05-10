// Layer 1 scaffolding ships ahead of any in-tree call site (the
// downstream `pipeline` driver lands in a later phase). Dead-code
// warnings are silenced here, not on individual items, so the public
// surface is uncluttered.
#![allow(dead_code, unused_imports)]

//! Layer 1 — `query_language`.
//!
//! Per-language parser façade. See `controller/docs/design.md` §6
//! `core::query_language` for the design contract.
//!
//! # Module layout
//!
//! ```text
//! query_language/
//!   ├── language.rs       — Language trait + ParseError
//!   ├── language_ast.rs   — LanguageAst sum type
//!   ├── promql/           — PromQL backend (active; wraps query_parser::promql)
//!   ├── sql/              — SQL backend (stub; Unimplemented)
//!   └── elastic_dsl/      — ElasticDSL backend (stub; Unimplemented)
//! ```
//!
//! # DC deployment scope
//!
//! Only [`promql::PromQLLanguage`] is implemented. The other backends
//! return [`language::ParseError::Unimplemented`] so the type system is
//! uniform; adding a real backend later is a localised change to the
//! relevant sub-module.

pub mod language;
pub mod language_ast;

pub mod promql;
pub mod sql;
pub mod elastic_dsl;

pub use language::{Language, ParseError};
pub use language_ast::LanguageAst;

pub use promql::PromQLLanguage;
pub use sql::SqlLanguage;
pub use elastic_dsl::ElasticDslLanguage;

#[cfg(test)]
mod tests;
