//! Step γ1 bridge: legacy `Aggregate` node → canonical
//! `intent_algebra::query_expr::QueryExpr::Aggregate` shape data.
//!
//! Approach **(c)** per the migration spec: we keep the legacy
//! `Aggregate` variant alive in [`crate::intent_algebra::legacy_expr`] as
//! the L2 emit shape (every parser, optimizer rule, and physical-stage
//! walker still consumes it unchanged), and add a one-way builder helper
//! that converts a legacy `Aggregate { keys, aggs, having, input }` into
//! the canonical-shape data — `by: Vec<ColumnId>`, `aggs: Vec<AggIntent>`,
//! `having: Option<HavingPredicate>` — that downstream consumers
//! (sketch_algebra `Bind*` rules, cost model, …) already match on. The
//! legacy variant retires only when every consumer entry point uses the
//! canonical shape, which is Step γ7 or later.
//!
//! ## Why not migrate construction sites
//!
//! Many `Aggregate.input` subtrees still hold legacy types (other not-
//! yet-migrated variants — `SketchAgg`, `WindowedAgg`, `TopK`,
//! `PromQLSubquery`). The canonical
//! `QueryExpr::Aggregate.child: Box<QueryExpr>` field expects a CANONICAL
//! `QueryExpr` — so until Steps γ2-γ7 migrate those variants, we cannot
//! build a fully-canonical `Aggregate` rooted at a legacy parser's emit
//! tree. Approach (c) sidesteps the issue: the bridge returns the
//! canonical-shape fields *without* the child tree, and consumers
//! recurse into the legacy `Aggregate.input` themselves.
//!
//! ## What the bridge produces
//!
//! ```text
//! legacy::QueryExpr::Aggregate { keys, aggs, having, input }
//!         │
//!         │  with parent_schema: &Schema   ← Step β plumbing
//!         ▼
//! BridgedAggregate {
//!     by:     Vec<ColumnId>,                   // resolved from keys
//!     aggs:   Vec<AggIntent>,                  // mapped from AggFunc
//!     having: Option<HavingPredicate>,         // via Predicate::from_legacy_scalar
//!     // child is intentionally NOT carried — consumers walk the legacy
//!     // `input` directly through their own recursion.
//! }
//! ```
//!
//! ## Deferred E-variants
//!
//! If `having` contains a `ScalarExpr` shape that
//! [`crate::intent_algebra::query_expr::from_legacy_scalar`] doesn't
//! support yet (`FunctionCall` / `ScalarSubquery` / `InList` /
//! `Between` — the E-classified leftovers from Batch 2), the bridge
//! returns [`BridgeError::HavingDeferred`] with the offending variant.
//! Callers can then EITHER keep using the legacy Aggregate at this site
//! OR drop the `having` clause and retry.

use thiserror::Error;

use crate::intent_algebra::agg_intent::AggIntent;
use crate::intent_algebra::column_resolution::{resolve_named_keys, ResolveError};
use crate::intent_algebra::legacy_expr::{AggItem, ScalarExpr};
use crate::intent_algebra::legacy_lower::agg_func_to_intents;
use crate::intent_algebra::query_expr::{from_legacy_scalar, HavingPredicate, QueryExprError};
use crate::intent_algebra::schema::{ColumnId, Schema};

/// Canonical-shape data extracted from a legacy `Aggregate` node — the
/// fields a canonical `QueryExpr::Aggregate` would carry, minus the
/// `child: Box<QueryExpr>` (consumers walk the legacy `input` directly).
#[derive(Debug, Clone)]
pub struct BridgedAggregate {
    /// Group-by columns resolved positionally against the inherited
    /// schema. Empty → global aggregate.
    pub by: Vec<ColumnId>,
    /// Canonical aggregate intents — one per legacy `AggItem`. The
    /// StdDev / Variance fan-out (Step α F1) is NOT performed here; this
    /// bridge produces one intent per `AggItem` and surfaces the fan-out
    /// count to the caller via [`Self::fanned_out_intents`] for advisory
    /// use. Real fan-out happens one layer up (sketch lowering), before
    /// the canonical-shape consumer sees the tree.
    pub aggs: Vec<AggIntent>,
    /// Optional HAVING predicate, converted via
    /// [`from_legacy_scalar`] and rendered to its
    /// [`HavingPredicate`] (string) form. `None` when the legacy node
    /// had no HAVING clause.
    pub having: Option<HavingPredicate>,
}

impl BridgedAggregate {
    /// Number of intents that would be produced if the StdDev / Variance
    /// fan-out (Step α F1) were applied. Equal to `self.aggs.len()` in
    /// every case except when one of the underlying `AggItem`s carried
    /// `AggFunc::StdDev` / `AggFunc::Variance` (which fan out to two
    /// quantile siblings). Advisory — sketch lowering performs the
    /// real fan-out before consumers see the tree.
    #[allow(dead_code)]
    pub fn fanned_out_intents(&self) -> usize {
        self.aggs.len()
    }
}

/// Errors returned by [`bridge_aggregate_to_canonical`].
///
/// Note: `PartialEq` isn't derived because [`QueryExprError`] (carried
/// by `HavingDeferred`) doesn't derive `PartialEq`. Callers compare via
/// `matches!(err, BridgeError::Variant(_))`.
#[derive(Debug, Error)]
pub enum BridgeError {
    /// One of the legacy `keys` (`Vec<String>`) didn't resolve against
    /// the inherited schema. The conservative fallback: callers keep the
    /// legacy Aggregate at this site and log the deferral.
    #[error("Aggregate key resolution failed: {0}")]
    Key(#[from] ResolveError),
    /// An `AggItem.func` mapped to an empty intent set — only
    /// `AggFunc::Custom(_)` triggers this today. The legacy `Aggregate`
    /// would survive lowering unchanged; the bridge surfaces the
    /// deferral so consumers know to keep matching the legacy shape.
    #[error(
        "AggItem `{alias}` uses non-canonical func ({func_dbg}) — no \
         canonical AggIntent equivalent (e.g. AggFunc::Custom)"
    )]
    NoCanonicalIntent { alias: String, func_dbg: String },
    /// The legacy `having` clause used an E-classified ScalarExpr
    /// variant that [`from_legacy_scalar`] doesn't translate yet
    /// (`FunctionCall` / `ScalarSubquery` / `InList` / `Between`).
    /// Callers can either keep the legacy Aggregate OR drop the
    /// `having` clause and retry.
    #[error("Aggregate HAVING clause uses deferred legacy ScalarExpr variant: {0}")]
    HavingDeferred(QueryExprError),
}

/// Translate the canonical-shape fields of a legacy
/// `legacy_expr::QueryExpr::Aggregate { keys, aggs, having, input }`
/// against the inherited schema.
///
/// Returns the bridged data; the legacy `input` subtree is left to the
/// caller's own recursion (see module doc-comment).
///
/// ## Schema flow
///
/// `parent_schema` is the schema in scope at the legacy `Aggregate` node
/// — its INPUT schema. The bridge resolves `keys` against it. To get the
/// `Aggregate`'s OUTPUT schema (which is what consumers should pass
/// downward when recursing into `input`'s siblings, or upward when the
/// `Aggregate` is itself a child of another node), call
/// [`crate::intent_algebra::column_resolution::output_schema_for_aggregate`]
/// on the `BridgedAggregate.by` / `.aggs` together with `parent_schema`.
///
/// ## Fan-out
///
/// Step α F1 fans `AggFunc::StdDev` / `AggFunc::Variance` out to two
/// `AggIntent::Quantile` siblings. The bridge MIRRORS that fan-out — if
/// any `AggItem.func` produces N intents, all N are appended to
/// `BridgedAggregate.aggs`. The canonical `QueryExpr::Aggregate.aggs:
/// Vec<AggIntent>` field accepts this directly.
pub fn bridge_aggregate_to_canonical(
    keys: &[String],
    aggs: &[AggItem],
    having: &Option<ScalarExpr>,
    parent_schema: &Schema,
) -> Result<BridgedAggregate, BridgeError> {
    // 1. Resolve group-by keys positionally.
    let by = resolve_named_keys(keys, parent_schema)?;

    // 2. Translate each AggItem.func to canonical AggIntent(s).
    let mut bridged_aggs: Vec<AggIntent> = Vec::with_capacity(aggs.len());
    for item in aggs {
        let intents = agg_func_to_intents(&item.func);
        if intents.is_empty() {
            return Err(BridgeError::NoCanonicalIntent {
                alias: item.alias.clone(),
                func_dbg: format!("{:?}", item.func),
            });
        }
        bridged_aggs.extend(intents);
    }

    // 3. Translate HAVING via Predicate::from_legacy_scalar, render to
    //    the string-typed HavingPredicate. The E-deferred case bubbles
    //    out via BridgeError::HavingDeferred so callers know to keep the
    //    legacy Aggregate at this site.
    let bridged_having = match having {
        None => None,
        Some(se) => match from_legacy_scalar(se) {
            Ok(pred) => Some(HavingPredicate(format!("{pred:?}"))),
            Err(e) => return Err(BridgeError::HavingDeferred(e)),
        },
    };

    Ok(BridgedAggregate {
        by,
        aggs: bridged_aggs,
        having: bridged_having,
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent_algebra::column_resolution::{
        infer_source_schema, output_schema_for_aggregate,
    };
    use crate::intent_algebra::legacy_expr::{
        AggFunc, BinaryOpKind as LegacyBinaryOpKind, ColumnRef as LegacyColumnRef,
        LiteralValue as LegacyLiteralValue, ScalarExpr as LegacyScalarExpr,
    };
    use crate::intent_algebra::schema::{Column, DataType};

    fn agg_item(alias: &str, func: AggFunc) -> AggItem {
        AggItem {
            alias: alias.into(),
            func,
            col: LegacyColumnRef::SampleValue,
            distinct: false,
        }
    }

    #[test]
    fn bridge_global_count() {
        let s = infer_source_schema("m");
        let aggs = vec![agg_item("c", AggFunc::Count)];
        let b = bridge_aggregate_to_canonical(&[], &aggs, &None, &s).unwrap();
        assert!(b.by.is_empty());
        assert_eq!(b.aggs.len(), 1);
        assert!(matches!(b.aggs[0], AggIntent::Frequency { .. }));
        assert!(b.having.is_none());
    }

    #[test]
    fn bridge_resolves_group_by_keys() {
        let mut s = infer_source_schema("m");
        s.columns.push(Column {
            name: "host".into(),
            dtype: DataType::Utf8,
            nullable: false,
        });
        let aggs = vec![agg_item("s", AggFunc::Sum)];
        let b = bridge_aggregate_to_canonical(&["host".to_string()], &aggs, &None, &s).unwrap();
        // "host" is at position 2 (after ts, value).
        assert_eq!(b.by, vec![2usize]);
        assert_eq!(b.aggs.len(), 1);
        assert!(matches!(b.aggs[0], AggIntent::Sum));
    }

    #[test]
    fn bridge_unknown_key_surfaces_resolve_error() {
        let s = infer_source_schema("m");
        let aggs = vec![agg_item("s", AggFunc::Sum)];
        let err = bridge_aggregate_to_canonical(&["missing".to_string()], &aggs, &None, &s)
            .unwrap_err();
        assert!(matches!(err, BridgeError::Key(ResolveError::NotFound { .. })));
    }

    #[test]
    fn bridge_custom_func_surfaces_no_canonical_intent() {
        let s = infer_source_schema("m");
        let aggs = vec![agg_item("u", AggFunc::Custom("my_udf".into()))];
        let err = bridge_aggregate_to_canonical(&[], &aggs, &None, &s).unwrap_err();
        assert!(matches!(err, BridgeError::NoCanonicalIntent { .. }));
    }

    #[test]
    fn bridge_stddev_fans_out_to_two_quantile_siblings() {
        // Step α F1: StdDev / Variance fan out to two AggIntent::Quantile
        // siblings (q=0.25, q=0.75). The bridge mirrors that.
        let s = infer_source_schema("m");
        let aggs = vec![agg_item("sd", AggFunc::StdDev { population: false })];
        let b = bridge_aggregate_to_canonical(&[], &aggs, &None, &s).unwrap();
        assert_eq!(b.aggs.len(), 2);
        let mut qs: Vec<f64> = b.aggs.iter().filter_map(|a| match a {
            AggIntent::Quantile { q, .. } => Some(*q),
            _ => None,
        }).collect();
        qs.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(qs, vec![0.25, 0.75]);
    }

    #[test]
    fn bridge_having_supported_translates_to_predicate_string() {
        let s = infer_source_schema("m");
        // HAVING value > 100
        let having = Some(LegacyScalarExpr::BinaryOp {
            op: LegacyBinaryOpKind::Gt,
            lhs: Box::new(LegacyScalarExpr::Column("value".into())),
            rhs: Box::new(LegacyScalarExpr::Literal(LegacyLiteralValue::Int(100))),
        });
        let aggs = vec![agg_item("s", AggFunc::Sum)];
        let b = bridge_aggregate_to_canonical(&[], &aggs, &having, &s).unwrap();
        let h = b.having.expect("having should be present");
        // String form should reference both operands. We don't assert on
        // exact formatting — that's a Predicate::Debug detail.
        assert!(h.0.contains("Column"));
        assert!(h.0.contains("100"));
    }

    #[test]
    fn bridge_having_deferred_e_variant_surfaces_error() {
        // HAVING uses FunctionCall — an E-classified ScalarExpr variant
        // that from_legacy_scalar doesn't translate.
        let s = infer_source_schema("m");
        let having = Some(LegacyScalarExpr::FunctionCall {
            name: "now".into(),
            args: vec![],
        });
        let aggs = vec![agg_item("s", AggFunc::Sum)];
        let err = bridge_aggregate_to_canonical(&[], &aggs, &having, &s).unwrap_err();
        assert!(matches!(err, BridgeError::HavingDeferred(_)));
    }

    #[test]
    fn bridge_output_schema_matches_canonical() {
        // Verify the bridged-data → output_schema_for_aggregate path
        // produces the same shape the canonical
        // QueryExpr::Aggregate::output_schema_in would.
        let mut s = infer_source_schema("m");
        s.columns.push(Column {
            name: "host".into(),
            dtype: DataType::Utf8,
            nullable: false,
        });
        let aggs = vec![agg_item("c", AggFunc::Sum)];
        let b = bridge_aggregate_to_canonical(&["host".to_string()], &aggs, &None, &s).unwrap();
        let out = output_schema_for_aggregate(&s, &b.by, &b.aggs);
        // Output columns: host (preserved positionally), sum.
        assert_eq!(out.columns.len(), 2);
        assert_eq!(out.columns[0].name, "host");
        assert_eq!(out.columns[1].name, "sum");
        assert!(out.time_index.is_none()); // time axis stripped
        assert_eq!(out.unique_keys, vec![vec![0]]); // by-tuple is unique
    }
}
