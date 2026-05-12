//! Schema-driven column resolution for the legacy `QueryExpr` IR
//! (Step β of the legacy_expr migration).
//!
//! Step α (PR #138) replaced the legacy `AggIntent` enum with the canonical
//! [`crate::intent_algebra::agg_intent::AggIntent`]. The legacy IR still
//! uses [`crate::intent_algebra::legacy_expr::ColumnRef::Named(String)`] for
//! column references; the canonical IR uses positional
//! [`crate::intent_algebra::schema::ColumnId`] resolved against a per-node
//! [`crate::intent_algebra::schema::Schema`].
//!
//! Step β plumbs `Schema` through every consumer that walks the legacy
//! tree so Step γ can migrate variant-by-variant to positional column ids
//! without first having to acquire a Schema everywhere.
//!
//! ## Threading approach
//!
//! Approach **(c)** per the migration spec: each traversal function takes a
//! `parent_schema: &Schema` parameter (the schema in scope at that node).
//! The root walker derives the base schema via [`infer_schema_for_root`]
//! (which walks to the outermost `Source` leaf and synthesises a default
//! schema for it) and threads it downward.
//!
//! Legacy operators here are mostly pass-through with respect to schema —
//! they don't add columns, they just filter / partition / sort / window
//! the same rows. The few that DO transform the schema (`Aggregate`,
//! `Project`, `Distinct`) are flagged as Step γ TODOs; Step γ will
//! migrate them onto the canonical `QueryExpr::output_schema_in` path so
//! the schema is locally derivable from the variant fields.
//!
//! ## Why a synthesized default
//!
//! The legacy_expr migration plan's "Synthesise from metric name:
//! `(ts, value, *labels)`" decision applies here. There is no
//! `SchemaCatalog` in the controller today, so the source leaf has to
//! produce a schema purely from the metric / table name. This module
//! ships the minimal time-series schema that's correct for the DC + PromQL
//! use case (the only consumers that exist in tree today). Tabular
//! sources (`SourceSpec::Table { table_ref }`) inherit the same shape as
//! a placeholder; a real catalog lookup is a follow-up Step γ TODO.

use thiserror::Error;

use crate::intent_algebra::legacy_expr::{ColumnRef, QueryExpr, SourceSpec};
use crate::intent_algebra::schema::{Column, ColumnId, DataType, Schema};

/// Errors returned by [`resolve_column_ref`] / [`resolve_column_refs`].
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ResolveError {
    /// `ColumnRef::Named(name)` did not match any column in the supplied
    /// schema. Common cause: the schema was synthesized from a metric name
    /// but the name being resolved is a free-form label (label sets aren't
    /// representable in the canonical `Schema` today — see the module
    /// doc-comment).
    #[error(
        "column `{name}` not found in schema (have: {available:?}) \
         — Step γ TODO: label-set resolution against a real catalog"
    )]
    NotFound {
        name: String,
        available: Vec<String>,
    },
    /// `ColumnRef::SampleValue` was resolved against a schema that has no
    /// `value` column. The synthesized time-series schema always has one,
    /// so this only fires for tabular sources without the convention.
    #[error("ColumnRef::SampleValue has no `value` column in schema (have: {available:?})")]
    NoSampleValue { available: Vec<String> },
    /// `ColumnRef::Wildcard` cannot be resolved to a single
    /// [`ColumnId`] — by definition it refers to every row, not a specific
    /// column. Callers that hit this branch should special-case `Wildcard`
    /// instead of asking for a positional id.
    #[error("ColumnRef::Wildcard cannot be resolved to a single ColumnId")]
    WildcardNotPositional,
}

/// Synthesize a default time-series schema for a metric / table source.
///
/// The shape is the conventional PromQL leaf: `(ts: Timestamp, value:
/// Float64)`. Open-set labels are intentionally omitted — the canonical
/// [`Schema`] model uses a positional `Vec<Column>` and has no
/// representation for "any number of label columns whose names are
/// data-dependent." That's tracked as a Step γ TODO at the module level.
///
/// Per the legacy_expr migration plan's "Synthesise from metric name:
/// `(ts, value, *labels)`" decision — minus the `*labels` part the
/// canonical schema model can't express today.
pub fn infer_source_schema(_metric_or_table_name: &str) -> Schema {
    Schema::with_time_index(
        vec![
            Column {
                name: "ts".into(),
                dtype: DataType::Timestamp,
                nullable: false,
            },
            Column {
                name: "value".into(),
                dtype: DataType::Float64,
                nullable: false,
            },
        ],
        0,
        // No provable unique key without a catalog — leave empty so the
        // CSE gatekeeper (`cse_reuse_is_legal`) conservatively refuses to
        // share a producer across queries until a real catalog ships.
        Vec::new(),
    )
}

/// Walk the legacy `QueryExpr` tree to find the outermost (left-most)
/// `Source` leaf and synthesise its default schema via
/// [`infer_source_schema`]. This is the entry-point Schema that consumer
/// walkers thread downward through the tree.
///
/// Falls back to an empty `Schema` when the root has no `Source` leaf
/// (e.g. a bare `Ref(name)` that resolves outside the supplied tree).
/// Callers that need real Ref resolution should pre-resolve via a
/// `BindingScope` analogue — for Step β the empty fallback is fine
/// because consumers only read the schema in pass-through paths.
pub fn infer_schema_for_root(expr: &QueryExpr) -> Schema {
    match expr.source_name() {
        Some(name) => infer_source_schema(name),
        None => Schema::default(),
    }
}

/// Resolve a single [`ColumnRef`] against a schema, returning the
/// positional [`ColumnId`]. Step γ consumers will call this at the point
/// where they need a `ColumnId` instead of a `String` — for now the
/// helper exists so the migration is mechanical at that point.
///
/// Resolution rules (mirror canonical
/// [`crate::intent_algebra::query_expr::ColumnRef`] semantics where they
/// already exist, e.g. `Distinct { cols }` in `output_schema_in`):
///
/// * `ColumnRef::Named(name)` → `schema.column_id(name)`.
/// * `ColumnRef::SampleValue` → `schema.column_id("value")` (PromQL
///   convention; the canonical lowering emits a column literally named
///   `value` for time-series scans).
/// * `ColumnRef::Wildcard` → [`ResolveError::WildcardNotPositional`]
///   (callers must special-case).
pub fn resolve_column_ref(col: &ColumnRef, schema: &Schema) -> Result<ColumnId, ResolveError> {
    match col {
        ColumnRef::Named(name) => schema
            .column_id(name)
            .ok_or_else(|| ResolveError::NotFound {
                name: name.clone(),
                available: schema.columns.iter().map(|c| c.name.clone()).collect(),
            }),
        ColumnRef::SampleValue => {
            schema
                .column_id("value")
                .ok_or_else(|| ResolveError::NoSampleValue {
                    available: schema.columns.iter().map(|c| c.name.clone()).collect(),
                })
        }
        ColumnRef::Wildcard => Err(ResolveError::WildcardNotPositional),
    }
}

/// Slice-flavoured [`resolve_column_ref`]: resolves every entry,
/// short-circuiting on the first error. Used by `Distinct { cols }` and
/// the future `Aggregate { by }` migration when keys move from
/// `Vec<String>` to `Vec<ColumnId>`.
pub fn resolve_column_refs(
    cols: &[ColumnRef],
    schema: &Schema,
) -> Result<Vec<ColumnId>, ResolveError> {
    cols.iter().map(|c| resolve_column_ref(c, schema)).collect()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent_algebra::legacy_expr::SourceSpec;

    fn src(name: &str) -> QueryExpr {
        QueryExpr::Source(SourceSpec { name: name.into() })
    }

    #[test]
    fn source_schema_has_ts_and_value() {
        let s = infer_source_schema("http_requests_total");
        assert_eq!(s.columns.len(), 2);
        assert_eq!(s.columns[0].name, "ts");
        assert_eq!(s.columns[1].name, "value");
        assert_eq!(s.time_index, Some(0));
        // No provable unique key without a real catalog — conservative.
        assert!(!s.has_unique_key());
    }

    #[test]
    fn root_schema_via_walk() {
        let expr = QueryExpr::Filter {
            pred: crate::intent_algebra::legacy_expr::ScalarExpr::Literal(
                crate::intent_algebra::legacy_expr::LiteralValue::Bool(true),
            ),
            input: Box::new(src("cpu_usage")),
        };
        let s = infer_schema_for_root(&expr);
        assert_eq!(s.columns.len(), 2);
        assert_eq!(s.columns[1].name, "value");
    }

    #[test]
    fn resolve_named_against_value_column() {
        let s = infer_source_schema("m");
        let col = ColumnRef::Named("value".into());
        assert_eq!(resolve_column_ref(&col, &s), Ok(1));
    }

    #[test]
    fn resolve_sample_value() {
        let s = infer_source_schema("m");
        assert_eq!(resolve_column_ref(&ColumnRef::SampleValue, &s), Ok(1));
    }

    #[test]
    fn resolve_wildcard_errors() {
        let s = infer_source_schema("m");
        assert_eq!(
            resolve_column_ref(&ColumnRef::Wildcard, &s),
            Err(ResolveError::WildcardNotPositional)
        );
    }

    #[test]
    fn resolve_unknown_name_errors() {
        let s = infer_source_schema("m");
        let err = resolve_column_ref(&ColumnRef::Named("host".into()), &s).unwrap_err();
        assert!(matches!(err, ResolveError::NotFound { .. }));
    }

    #[test]
    fn resolve_slice_short_circuits() {
        let s = infer_source_schema("m");
        let cols = vec![
            ColumnRef::Named("value".into()),
            ColumnRef::Named("not_there".into()),
        ];
        let r = resolve_column_refs(&cols, &s);
        assert!(matches!(r, Err(ResolveError::NotFound { .. })));
    }

    #[test]
    fn root_schema_falls_back_when_no_source() {
        let expr = QueryExpr::Ref("dangling".into());
        let s = infer_schema_for_root(&expr);
        assert_eq!(s.columns.len(), 0);
    }
}

// Silence "field never read" on `SourceSpec` when this module is the only
// consumer in some configurations.
#[allow(dead_code)]
const _: fn(&SourceSpec) -> &str = |s| s.name.as_str();
