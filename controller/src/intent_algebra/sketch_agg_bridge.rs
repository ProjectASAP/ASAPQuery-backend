//! Step γ2 bridge: legacy `SketchAgg` node → canonical
//! `intent_algebra::query_expr::QueryExpr::Aggregate` shape data.
//!
//! Companion to the Step γ1 [`crate::intent_algebra::aggregate_bridge`]
//! module. Same approach **(c)** per the migration spec: the legacy
//! [`crate::intent_algebra::legacy_expr::QueryExpr::SketchAgg`] variant
//! remains the L2 emit shape (parsers, optimizer rules, allocator, stage
//! splitter still match on `SketchAgg` unchanged); this module adds a
//! one-way builder helper that converts the legacy single-intent /
//! single-column shape into the canonical-shape data —
//! `by: Vec<ColumnId>`, `aggs: Vec<AggIntent>` — that downstream
//! canonical-shape consumers (sketch_algebra `Bind*` rules, cost model,
//! …) already match on.
//!
//! ## Structural note: SketchAgg vs Aggregate
//!
//! `Aggregate { keys: Vec<String>, aggs: Vec<AggItem>, having, input }`
//! and `SketchAgg { op: AggIntent, col: ColumnRef, input }` carry the
//! same semantic shape on the canonical side: a tuple of group-by
//! columns + a list of intents. The differences on the legacy side:
//!
//! | Aggregate                          | SketchAgg                       |
//! |---|---|
//! | `keys: Vec<String>` (multi-col)    | `col: ColumnRef` (single-col)   |
//! | `aggs: Vec<AggItem>` w/ AggFunc   | `op: AggIntent` already canonical |
//! | `having: Option<ScalarExpr>`       | no HAVING — always `None`       |
//!
//! Because `SketchAgg.op` is already canonical [`AggIntent`] (Step α,
//! PR #138 — the variant lives in `legacy_expr` only because its
//! single-column / single-intent shape doesn't match the multi-column /
//! multi-intent canonical `Aggregate`; the per-field types are already
//! migrated), the bridge here is much thinner than γ1's:
//! no `AggFunc → AggIntent` translation, no fan-out (Step α F1 happens
//! at parse time before SketchAgg construction), no HAVING translation.
//!
//! ## Wildcard handling
//!
//! `SketchAgg { col: ColumnRef::Wildcard, .. }` is the COUNT(*) shape —
//! the sketch operates over every row, not on a specific column value.
//! In canonical-shape terms this is a global aggregate with an empty
//! `by`. The bridge surfaces this by emitting `by: vec![]` for the
//! Wildcard arm. `SampleValue` and `Named` arms resolve through
//! [`resolve_column_ref`] like any other positional column reference.
//!
//! ## What the bridge produces
//!
//! ```text
//! legacy::QueryExpr::SketchAgg { op, col, input }
//!         │
//!         │  with parent_schema: &Schema   ← Step β plumbing
//!         ▼
//! BridgedAggregate {
//!     by:     Vec<ColumnId>,                   // [resolve(col)] or [] for Wildcard
//!     aggs:   vec![op.clone()],                // single-intent
//!     having: None,                            // SketchAgg has no HAVING
//!     // child intentionally NOT carried — consumers walk legacy `input` directly.
//! }
//! ```

use crate::intent_algebra::agg_intent::AggIntent;
use crate::intent_algebra::aggregate_bridge::{BridgeError, BridgedAggregate};
use crate::intent_algebra::column_resolution::resolve_column_ref;
use crate::intent_algebra::legacy_expr::ColumnRef;
use crate::intent_algebra::schema::{ColumnId, Schema};

/// Translate the canonical-shape fields of a legacy
/// `legacy_expr::QueryExpr::SketchAgg { op, col, input }` against the
/// inherited schema.
///
/// Returns a [`BridgedAggregate`] — the same canonical-shape carrier the
/// γ1 Aggregate bridge produces — so downstream consumers can match on
/// `by` / `aggs` regardless of which legacy variant the data came from.
/// The legacy `input` subtree is left to the caller's own recursion.
///
/// ## Schema flow
///
/// `parent_schema` is the schema in scope at the legacy `SketchAgg` node
/// — its INPUT schema. The bridge resolves `col` against it. To get the
/// `SketchAgg`'s OUTPUT schema (which is what consumers should pass
/// downward when threading further down, or upward when the `SketchAgg`
/// is itself a child of another node), call
/// [`output_schema_for_sketch_agg`] on the `BridgedAggregate.by` /
/// `.aggs` together with `parent_schema`.
///
/// ## Resolution rules
///
/// * `ColumnRef::Named(name)` → `by = vec![schema.column_id(name)?]`.
/// * `ColumnRef::SampleValue` → `by = vec![schema.column_id("value")?]`.
/// * `ColumnRef::Wildcard`     → `by = vec![]` (global / row-count
///   semantics). NOT an error here, unlike the bare
///   [`resolve_column_ref`] which surfaces
///   [`crate::intent_algebra::column_resolution::ResolveError::WildcardNotPositional`].
///
/// ## Errors
///
/// Returns [`BridgeError::Key`] (wrapping `ResolveError::NotFound` or
/// `ResolveError::NoSampleValue`) when the named / sample-value column
/// is absent from the inherited schema. The conservative fallback:
/// callers keep the legacy SketchAgg at this site and log the deferral.
///
/// SketchAgg has no `having` clause, so [`BridgeError::HavingDeferred`]
/// is unreachable. `op` is already canonical [`AggIntent`] (Step α), so
/// [`BridgeError::NoCanonicalIntent`] is also unreachable.
pub fn bridge_sketch_agg_to_canonical(
    op:            &AggIntent,
    col:           &ColumnRef,
    parent_schema: &Schema,
) -> Result<BridgedAggregate, BridgeError> {
    // Resolve `col` against the inherited schema, special-casing
    // Wildcard → empty `by` (global / row-count semantics).
    let by: Vec<ColumnId> = match col {
        ColumnRef::Wildcard => Vec::new(),
        _ => vec![resolve_column_ref(col, parent_schema)?],
    };

    Ok(BridgedAggregate {
        by,
        aggs: vec![op.clone()],
        having: None,
    })
}

/// Output schema produced by `QueryExpr::SketchAgg { op, col, input }`
/// when its `input` carries `input_schema` as its output schema.
///
/// Thin wrapper over
/// [`crate::intent_algebra::column_resolution::output_schema_for_aggregate`]
/// — SketchAgg and Aggregate produce the same shape (group-by columns
/// preserved positionally + one column per intent; time axis stripped;
/// unique_keys = `[by]` when `by` is non-empty, otherwise empty). Kept
/// as a sibling helper so callers descending into a still-legacy
/// `SketchAgg.input` can derive the right schema for the child without
/// first having to translate the whole subtree to canonical, mirroring
/// the γ1 [`output_schema_for_aggregate`] pattern.
///
/// `by` ids are silently clamped to in-range — out-of-range ids are
/// dropped from the output. (Callers that need the strict check should
/// resolve `by` ids upstream via [`bridge_sketch_agg_to_canonical`]
/// which DOES surface `NotFound`.)
///
/// # Example
///
/// ```ignore
/// use controller::intent_algebra::{
///     column_resolution::infer_source_schema,
///     sketch_agg_bridge::output_schema_for_sketch_agg,
///     agg_intent::AggIntent,
/// };
/// let input = infer_source_schema("http_requests_total");
/// // SketchAgg over Wildcard (COUNT(*)-style) → global aggregate, by = [].
/// let aggs = vec![AggIntent::Sum];
/// let out = output_schema_for_sketch_agg(&input, &[], &aggs);
/// assert_eq!(out.columns.len(), 1);          // sum
/// assert!(out.time_index.is_none());         // time axis stripped
/// ```
pub fn output_schema_for_sketch_agg(
    input: &Schema,
    by:    &[ColumnId],
    aggs:  &[AggIntent],
) -> Schema {
    // SketchAgg's output shape is identical to Aggregate's — delegate.
    crate::intent_algebra::column_resolution::output_schema_for_aggregate(input, by, aggs)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent_algebra::column_resolution::{infer_source_schema, ResolveError};
    use crate::intent_algebra::schema::{Column, DataType};
    use crate::types_v2::AccuracyTarget;

    #[test]
    fn bridge_single_intent_sample_value() {
        // SketchAgg { op: Sum, col: SampleValue, .. } against a default
        // time-series schema → by = [1] (the `value` column), aggs = [Sum].
        let s = infer_source_schema("m");
        let b = bridge_sketch_agg_to_canonical(
            &AggIntent::Sum,
            &ColumnRef::SampleValue,
            &s,
        )
        .unwrap();
        assert_eq!(b.by, vec![1usize]);
        assert_eq!(b.aggs.len(), 1);
        assert!(matches!(b.aggs[0], AggIntent::Sum));
        assert!(b.having.is_none());
    }

    #[test]
    fn bridge_wildcard_emits_empty_by() {
        // Wildcard → global aggregate (by = []), single intent preserved.
        let s = infer_source_schema("m");
        let b = bridge_sketch_agg_to_canonical(
            &AggIntent::Count { accuracy: AccuracyTarget::Exact },
            &ColumnRef::Wildcard,
            &s,
        )
        .unwrap();
        assert!(b.by.is_empty());
        assert_eq!(b.aggs.len(), 1);
        assert!(matches!(b.aggs[0], AggIntent::Count { .. }));
        assert!(b.having.is_none());
    }

    #[test]
    fn bridge_named_column_resolves_positionally() {
        // SketchAgg { op: Max, col: Named("host"), .. } with host column
        // at position 2 → by = [2].
        let mut s = infer_source_schema("m");
        s.columns.push(Column {
            name: "host".into(),
            dtype: DataType::Utf8,
            nullable: false,
        });
        let b = bridge_sketch_agg_to_canonical(
            &AggIntent::Max,
            &ColumnRef::Named("host".into()),
            &s,
        )
        .unwrap();
        assert_eq!(b.by, vec![2usize]);
        assert_eq!(b.aggs.len(), 1);
        assert!(matches!(b.aggs[0], AggIntent::Max));
    }

    #[test]
    fn bridge_unresolvable_named_column_surfaces_key_error() {
        // Named column absent from schema → BridgeError::Key(NotFound).
        let s = infer_source_schema("m");
        let err = bridge_sketch_agg_to_canonical(
            &AggIntent::Sum,
            &ColumnRef::Named("missing".into()),
            &s,
        )
        .unwrap_err();
        assert!(matches!(err, BridgeError::Key(ResolveError::NotFound { .. })));
    }

    #[test]
    fn bridge_sample_value_missing_surfaces_key_error() {
        // SampleValue against a schema with no `value` column →
        // BridgeError::Key(NoSampleValue).
        let s = Schema {
            columns: vec![Column {
                name: "only".into(),
                dtype: DataType::Utf8,
                nullable: false,
            }],
            time_index: None,
            unique_keys: Vec::new(),
        };
        let err = bridge_sketch_agg_to_canonical(
            &AggIntent::Sum,
            &ColumnRef::SampleValue,
            &s,
        )
        .unwrap_err();
        assert!(matches!(err, BridgeError::Key(ResolveError::NoSampleValue { .. })));
    }

    #[test]
    fn bridge_preserves_canonical_intent_unchanged() {
        // The bridge passes `op` through verbatim — no fan-out (Step α
        // F1 happens at parse time before SketchAgg construction) and no
        // AggFunc translation (op is already canonical AggIntent).
        let s = infer_source_schema("m");
        let intent = AggIntent::Quantile {
            q: 0.95,
            accuracy: AccuracyTarget::Exact,
        };
        let b = bridge_sketch_agg_to_canonical(&intent, &ColumnRef::SampleValue, &s).unwrap();
        assert_eq!(b.aggs.len(), 1);
        match &b.aggs[0] {
            AggIntent::Quantile { q, .. } => assert!((*q - 0.95).abs() < 1e-9),
            other => panic!("expected Quantile, got {other:?}"),
        }
    }

    #[test]
    fn bridge_output_schema_matches_canonical_aggregate_shape() {
        // Verify the bridged-data → output_schema_for_sketch_agg path
        // produces the same shape the canonical
        // QueryExpr::Aggregate::output_schema_in would for an equivalent
        // single-intent / single-column input. The shared helper is
        // output_schema_for_aggregate, so this test pins the equivalence.
        let mut s = infer_source_schema("m");
        s.columns.push(Column {
            name: "host".into(),
            dtype: DataType::Utf8,
            nullable: false,
        });
        let b = bridge_sketch_agg_to_canonical(
            &AggIntent::Sum,
            &ColumnRef::Named("host".into()),
            &s,
        )
        .unwrap();
        let out = output_schema_for_sketch_agg(&s, &b.by, &b.aggs);
        // Output columns: host (preserved positionally), sum.
        assert_eq!(out.columns.len(), 2);
        assert_eq!(out.columns[0].name, "host");
        assert_eq!(out.columns[1].name, "sum");
        // Time axis stripped — SketchAgg produces one row per group.
        assert!(out.time_index.is_none());
        // unique_keys = [[0]] (the by tuple is unique by construction).
        assert_eq!(out.unique_keys, vec![vec![0]]);
    }

    #[test]
    fn bridge_output_schema_wildcard_global_has_no_unique_keys() {
        // Wildcard → by = [] → global aggregate → empty unique_keys.
        let s = infer_source_schema("m");
        let b = bridge_sketch_agg_to_canonical(
            &AggIntent::Count { accuracy: AccuracyTarget::Exact },
            &ColumnRef::Wildcard,
            &s,
        )
        .unwrap();
        let out = output_schema_for_sketch_agg(&s, &b.by, &b.aggs);
        assert_eq!(out.columns.len(), 1);
        assert_eq!(out.columns[0].name, "count");
        assert!(out.unique_keys.is_empty());
        assert!(out.time_index.is_none());
    }
}
