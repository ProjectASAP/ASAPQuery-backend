//! Step γ3 bridge: legacy `WindowedAgg { agg, window, col, input }` node
//! → canonical `Window { kind, child: Aggregate { aggs: [agg], by:
//! [col_resolved], having: None, child: input } }` shape data.
//!
//! Approach **(c)** per the migration spec (same as Step γ1's Aggregate
//! bridge — see [`crate::intent_algebra::aggregate_bridge`]): we keep the
//! legacy `WindowedAgg` variant alive in
//! [`crate::intent_algebra::legacy_expr`] as the L2 emit shape (every
//! parser, optimizer rule, and physical-stage walker still consumes it
//! unchanged), and add a one-way builder helper that extracts the
//! canonical-shape data — the outer `Window`'s
//! [`crate::intent_algebra::WindowKind`] / `size` / `slide` plus the
//! inner [`crate::intent_algebra::aggregate_bridge::BridgedAggregate`]
//! that downstream consumers (sketch_algebra `Bind*` rules, cost model,
//! …) already match on. The legacy variant retires only when every
//! consumer entry point uses the canonical shape — Step γ7 or later.
//!
//! ## Why a separate module
//!
//! `WindowedAgg` semantically fuses two canonical operators —
//! `Window` over `Aggregate` per design.md §3 line ~48 + §6 row 2 ("One
//! canonical form per plan — no `WindowedAgg` (use `Window` over
//! `Aggregate`)"). The bridge's return type therefore stacks the
//! canonical-shape data for both layers in one struct, and reuses the
//! γ1 [`BridgedAggregate`] for the inner Aggregate fields. Keeping it
//! alongside [`crate::intent_algebra::aggregate_bridge`] would mix the
//! single-layer and stacked-layer surfaces in one file; a separate
//! `windowed_agg_bridge.rs` keeps each bridge surface scoped to its
//! variant.
//!
//! ## The 20+ consumer concern (window-sketch lifecycle invariant)
//!
//! `WindowedAgg` bundles the window and the aggregation because in
//! sketch systems the window defines the sketch lifecycle (when to
//! flush / reset — see [`crate::intent_algebra::legacy_expr::QueryExpr::WindowedAgg`]
//! doc comment). 20+ consumers (allocator placement rules,
//! optimizer rewrite rules, the SQL parser, the stage_split walk + the
//! PromQL emitter, the physical planner, …) assume the fused shape's
//! lifecycle invariant: window's flush / reset boundary IS the
//! sketch's reset boundary.
//!
//! The bridge pattern SIDESTEPS this. Consumers continue matching on
//! `WindowedAgg` and seeing the fused shape — the lifecycle invariant
//! is preserved because no construction site changes. Only consumers
//! that want the canonical view CALL the bridge on demand and walk the
//! returned `BridgedWindowedAgg` (which separates the window-kind /
//! size / slide from the inner aggregate's `by` / `aggs` / `having`).
//!
//! ## What the bridge produces
//!
//! ```text
//! legacy::QueryExpr::WindowedAgg { agg, window, col, input }
//!         │
//!         │  with parent_schema: &Schema   ← Step β plumbing
//!         ▼
//! BridgedWindowedAgg {
//!     window_kind:  WindowKind,             // from window.kind
//!     window_size:  Duration,                // from window.kind
//!     window_slide: Option<Duration>,        // Some(slide) for Sliding
//!     inner: BridgedAggregate {
//!         by:     vec![resolve(col)],        // exactly one positional id
//!         aggs:   vec![agg.clone()],         // exactly one canonical intent
//!         having: None,                      // WindowedAgg has no HAVING
//!     },
//!     // child is intentionally NOT carried — consumers walk the legacy
//!     // `input` directly through their own recursion (γ1 convention).
//! }
//! ```
//!
//! ## WindowKind mapping
//!
//! Legacy [`crate::intent_algebra::legacy_expr::WindowKind`] is a
//! richer enum than canonical
//! [`crate::intent_algebra::WindowKind`]. The bridge maps:
//!
//! | legacy `WindowKind`           | canonical `WindowKind` | size      | slide       |
//! |-------------------------------|------------------------|-----------|-------------|
//! | `Tumbling { size }`           | `Tumbling`             | `size`    | `None`      |
//! | `Sliding { size, slide }`     | `Sliding`              | `size`    | `Some(slide)` |
//! | `Session { gap }`             | `Session`              | `gap`     | `None`      |
//! | `Unbounded`                   | bridge defers          | —         | —           |
//! | `Landmark`                    | bridge defers          | —         | —           |
//!
//! `Unbounded` and `Landmark` have no canonical equivalent today (the
//! canonical `WindowKind` enum is `Tumbling / Sliding / Session`) so the
//! bridge surfaces them as [`BridgeError::UnsupportedWindowKind`] — the
//! caller keeps the legacy `WindowedAgg` at that site.

use std::time::Duration;

use thiserror::Error;

use crate::intent_algebra::agg_intent::AggIntent;
use crate::intent_algebra::aggregate_bridge::BridgedAggregate;
use crate::intent_algebra::column_resolution::{resolve_column_ref, ResolveError};
use crate::intent_algebra::legacy_expr::{
    ColumnRef as LegacyColumnRef, WindowKind as LegacyWindowKind, WindowSpec,
};
use crate::intent_algebra::query_expr::WindowKind as CanonicalWindowKind;
use crate::intent_algebra::schema::{Column, ColumnId, DataType, Schema};

/// Canonical-shape data extracted from a legacy `WindowedAgg` node — the
/// stacked-layer fields a canonical
/// `QueryExpr::Window { child: Aggregate { .. } }` would carry, minus
/// the bottommost `child: Box<QueryExpr>` (consumers walk the legacy
/// `input` directly, mirroring the γ1 bridge convention).
#[derive(Debug, Clone)]
pub struct BridgedWindowedAgg {
    /// Outer `Window`'s kind, mapped from the legacy `WindowSpec.kind`.
    /// `Tumbling` / `Sliding` / `Session` only — see module doc on the
    /// `Unbounded` / `Landmark` deferral.
    pub window_kind: CanonicalWindowKind,
    /// Outer `Window`'s `size: Duration` — taken from `Tumbling.size` /
    /// `Sliding.size` / `Session.gap`.
    pub window_size: Duration,
    /// Outer `Window`'s `slide: Option<Duration>`. `Some(_)` only for
    /// the `Sliding` legacy kind; `None` everywhere else.
    pub window_slide: Option<Duration>,
    /// Inner `Aggregate` canonical-shape data: `by` = `[resolve(col)]`
    /// (exactly one positional id — `WindowedAgg` always groups by a
    /// single column), `aggs` = `[agg.clone()]` (exactly one canonical
    /// intent), `having` = `None` (`WindowedAgg` carries no HAVING).
    pub inner: BridgedAggregate,
}

/// Errors returned by [`bridge_windowed_agg_to_canonical`].
///
/// Note: `PartialEq` isn't derived because [`ResolveError`] is the only
/// payload here that's also pattern-matched — callers compare via
/// `matches!(err, BridgeError::Variant(_))`.
#[derive(Debug, Error)]
pub enum BridgeError {
    /// The legacy `col` (`ColumnRef`) didn't resolve against the
    /// inherited schema, OR it was `Wildcard` (which has no positional
    /// id by construction). The conservative fallback: callers keep the
    /// legacy `WindowedAgg` at this site and log the deferral.
    #[error("WindowedAgg col resolution failed: {0}")]
    Col(#[from] ResolveError),
    /// The legacy `WindowSpec.kind` was `Unbounded` or `Landmark` —
    /// neither has a canonical `WindowKind` equivalent today. Callers
    /// keep the legacy `WindowedAgg` at this site.
    #[error("WindowedAgg uses non-canonical WindowKind variant: {kind_dbg}")]
    UnsupportedWindowKind { kind_dbg: String },
}

/// Translate the canonical-shape fields of a legacy
/// `legacy_expr::QueryExpr::WindowedAgg { agg, window, col, input }`
/// against the inherited schema.
///
/// Returns the bridged data; the legacy `input` subtree is left to the
/// caller's own recursion (see module doc-comment).
///
/// ## Schema flow
///
/// `parent_schema` is the schema in scope at the legacy `WindowedAgg`
/// node — its INPUT schema. The bridge resolves `col` against it. To
/// get the `WindowedAgg`'s OUTPUT schema (which is what consumers
/// should pass downward when recursing into siblings, or upward when
/// the `WindowedAgg` is itself a child of another node), call
/// [`output_schema_for_windowed_agg`] on the resulting
/// `BridgedWindowedAgg.inner.by` / `.inner.aggs` together with
/// `parent_schema`.
///
/// ## Sketch-lifecycle invariant
///
/// The bridge produces a logical view; it does NOT change construction
/// sites or sketch lifecycle. The legacy `WindowedAgg` still bundles
/// window + agg at every parser / optimizer / allocator entry point,
/// preserving the "window's flush/reset == sketch's reset" invariant
/// that 20+ consumers rely on (see module doc).
pub fn bridge_windowed_agg_to_canonical(
    agg: &AggIntent,
    window: &WindowSpec,
    col: &LegacyColumnRef,
    parent_schema: &Schema,
) -> Result<BridgedWindowedAgg, BridgeError> {
    // 1. Translate the legacy WindowSpec.kind → canonical (kind, size, slide).
    let (window_kind, window_size, window_slide) = match &window.kind {
        LegacyWindowKind::Tumbling { size } => {
            (CanonicalWindowKind::Tumbling, *size, None)
        }
        LegacyWindowKind::Sliding { size, slide } => {
            (CanonicalWindowKind::Sliding, *size, Some(*slide))
        }
        LegacyWindowKind::Session { gap } => {
            (CanonicalWindowKind::Session, *gap, None)
        }
        other @ (LegacyWindowKind::Unbounded | LegacyWindowKind::Landmark) => {
            return Err(BridgeError::UnsupportedWindowKind {
                kind_dbg: format!("{other:?}"),
            });
        }
    };

    // 2. Resolve the legacy `col` ColumnRef → ColumnId positionally.
    //    `WindowedAgg` always groups by exactly one column — its `col`
    //    field. We wrap the result in a single-element Vec to match the
    //    canonical `Aggregate.by: Vec<ColumnId>` shape.
    let col_id = resolve_column_ref(col, parent_schema)?;

    // 3. Assemble the inner BridgedAggregate (always exactly one intent,
    //    always exactly one group-by key, never any HAVING).
    let inner = BridgedAggregate {
        by:     vec![col_id],
        aggs:   vec![agg.clone()],
        having: None,
    };

    Ok(BridgedWindowedAgg {
        window_kind,
        window_size,
        window_slide,
        inner,
    })
}

// ── Schema flow ──────────────────────────────────────────────────────────────

/// Output schema produced by `QueryExpr::WindowedAgg { agg, window, col, input }`
/// when its `input` carries `input_schema` as its output schema.
///
/// Mirrors the canonical `Window { child: Aggregate { .. } }` schema
/// flow per `design.md` §6 schema-flow table + the canonical
/// [`crate::intent_algebra::query_expr::QueryExpr::output_schema_in`]
/// implementation:
///
/// 1. The inner `Aggregate` produces the agg's output (same shape as
///    [`crate::intent_algebra::output_schema_for_aggregate`]: `by`
///    columns followed by one new column per `aggs` entry; time index
///    stripped; `unique_keys = [by]`).
/// 2. The outer `Window` passes that schema through verbatim (canonical
///    Window's schema rule is pass-through) — EXCEPT that the window
///    preserves its time bounds, so this helper restores the time
///    index when the inner Aggregate didn't strip its single group-by
///    column (which is rare; `WindowedAgg.col` is the sketch target,
///    not a time column, so the time axis still ends up stripped in
///    practice).
///
/// Schema preservation: the function returns the same shape as
/// [`crate::intent_algebra::output_schema_for_aggregate`] would for
/// the inner Aggregate, with the time-axis preservation explicitly
/// documented (see the per-field rationale below). Out-of-range `by`
/// ids are silently clamped (matches the γ1 convention).
///
/// # Example
///
/// ```ignore
/// use control_plane::intent_algebra::{
///     column_resolution::{infer_source_schema, resolve_column_ref},
///     windowed_agg_bridge::output_schema_for_windowed_agg,
///     agg_intent::AggIntent,
/// };
/// let input = infer_source_schema("http_requests_total");
/// let col = LegacyColumnRef::SampleValue;
/// let id = resolve_column_ref(&col, &input).unwrap();
/// let out = output_schema_for_windowed_agg(&input, &[id], &[AggIntent::Sum]);
/// assert_eq!(out.columns.len(), 2);    // value (the by col) + sum
/// assert!(out.time_index.is_none());   // inner Aggregate strips time
/// ```
pub fn output_schema_for_windowed_agg(
    input: &Schema,
    by: &[ColumnId],
    aggs: &[AggIntent],
) -> Schema {
    // Inner Aggregate's output: by columns + one column per intent.
    let mut out_cols: Vec<Column> = Vec::with_capacity(by.len() + aggs.len());
    for &id in by {
        if let Some(c) = input.columns.get(id) {
            out_cols.push(c.clone());
        }
        // out-of-range silently dropped — same convention as
        // output_schema_for_aggregate (γ1).
    }
    // One new column per intent, applied to the synthetic `value`
    // column when present; otherwise to the first non-grouped column.
    let value_col_idx = input
        .column_id("value")
        .or_else(|| (0..input.columns.len()).find(|i| !by.contains(i)));
    let probe = value_col_idx
        .and_then(|i| input.columns.get(i))
        .cloned()
        .unwrap_or(Column {
            name: "value".into(),
            dtype: DataType::Float64,
            nullable: false,
        });
    for intent in aggs {
        out_cols.push(intent.output_column(&probe));
    }
    // unique_keys = [by] when by is non-empty (the by tuple is unique
    // by construction in the post-aggregate frame).
    let unique_keys = if by.is_empty() {
        Vec::new()
    } else {
        vec![(0..by.len()).collect()]
    };
    // Inner Aggregate strips the time axis (one row per group per
    // window). The outer Window doesn't re-introduce it — canonical
    // Window's schema rule is pass-through of `child.output_schema()`,
    // which here is the Aggregate's output (time-stripped). Window
    // time bounds are carried on the Window node itself, not as a
    // schema column.
    //
    // If a future migration introduces synthetic `window_id` /
    // `window_start` / `window_end` columns (design.md §6 schema-flow
    // row 6), the canonical `output_schema_in` for `Window` will grow
    // them; this helper will follow.
    Schema {
        columns: out_cols,
        time_index: None,
        unique_keys,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent_algebra::column_resolution::infer_source_schema;
    use crate::intent_algebra::legacy_expr::{
        ColumnRef as LegacyColumnRef, WindowKind as LegacyWindowKind, WindowSpec,
    };
    use crate::intent_algebra::schema::{Column, DataType};
    use crate::types_v2::AccuracyTarget;

    fn tumbling_spec(secs: u64) -> WindowSpec {
        WindowSpec {
            kind: LegacyWindowKind::Tumbling {
                size: Duration::from_secs(secs),
            },
            time_col: None,
        }
    }

    fn sliding_spec(size_secs: u64, slide_secs: u64) -> WindowSpec {
        WindowSpec {
            kind: LegacyWindowKind::Sliding {
                size:  Duration::from_secs(size_secs),
                slide: Duration::from_secs(slide_secs),
            },
            time_col: None,
        }
    }

    fn session_spec(gap_secs: u64) -> WindowSpec {
        WindowSpec {
            kind: LegacyWindowKind::Session {
                gap: Duration::from_secs(gap_secs),
            },
            time_col: None,
        }
    }

    #[test]
    fn bridge_basic_tumbling_quantile_on_sample_value() {
        // The most common shape: tumbling window over the synthetic
        // `value` column, with a single Quantile intent.
        let s = infer_source_schema("http_requests_total");
        let agg = AggIntent::Quantile {
            q: 0.99,
            accuracy: AccuracyTarget::Epsilon(0.01),
        };
        let window = tumbling_spec(300);
        let col = LegacyColumnRef::SampleValue;
        let b = bridge_windowed_agg_to_canonical(&agg, &window, &col, &s).unwrap();

        assert_eq!(b.window_kind, CanonicalWindowKind::Tumbling);
        assert_eq!(b.window_size, Duration::from_secs(300));
        assert_eq!(b.window_slide, None);
        // SampleValue resolves to position 1 (after `ts`).
        assert_eq!(b.inner.by, vec![1usize]);
        assert_eq!(b.inner.aggs.len(), 1);
        assert!(matches!(b.inner.aggs[0], AggIntent::Quantile { .. }));
        assert!(b.inner.having.is_none());
    }

    #[test]
    fn bridge_sliding_window_carries_slide_duration() {
        // Sliding windows must propagate their `slide` field — this is
        // what differentiates a sliding window from a tumbling one in
        // the canonical shape.
        let s = infer_source_schema("m");
        let agg = AggIntent::Sum;
        let window = sliding_spec(600, 60);
        let col = LegacyColumnRef::SampleValue;
        let b = bridge_windowed_agg_to_canonical(&agg, &window, &col, &s).unwrap();

        assert_eq!(b.window_kind, CanonicalWindowKind::Sliding);
        assert_eq!(b.window_size, Duration::from_secs(600));
        assert_eq!(b.window_slide, Some(Duration::from_secs(60)));
        assert!(matches!(b.inner.aggs[0], AggIntent::Sum));
    }

    #[test]
    fn bridge_session_window_maps_to_canonical_session() {
        // Session windows map their `gap` onto the canonical `size`
        // slot (the canonical WindowKind::Session shape uses a single
        // Duration for the inactivity gap).
        let s = infer_source_schema("m");
        let agg = AggIntent::Count {
            accuracy: AccuracyTarget::Exact,
        };
        let window = session_spec(30);
        let col = LegacyColumnRef::SampleValue;
        let b = bridge_windowed_agg_to_canonical(&agg, &window, &col, &s).unwrap();

        assert_eq!(b.window_kind, CanonicalWindowKind::Session);
        assert_eq!(b.window_size, Duration::from_secs(30));
        assert_eq!(b.window_slide, None);
    }

    #[test]
    fn bridge_unbounded_window_defers() {
        // Unbounded has no canonical equivalent today — the bridge
        // surfaces UnsupportedWindowKind so callers keep the legacy
        // WindowedAgg at this site.
        let s = infer_source_schema("m");
        let agg = AggIntent::Sum;
        let window = WindowSpec {
            kind: LegacyWindowKind::Unbounded,
            time_col: None,
        };
        let col = LegacyColumnRef::SampleValue;
        let err = bridge_windowed_agg_to_canonical(&agg, &window, &col, &s).unwrap_err();
        assert!(matches!(err, BridgeError::UnsupportedWindowKind { .. }));
    }

    #[test]
    fn bridge_landmark_window_defers() {
        // Same deferral path as Unbounded.
        let s = infer_source_schema("m");
        let agg = AggIntent::Sum;
        let window = WindowSpec {
            kind: LegacyWindowKind::Landmark,
            time_col: None,
        };
        let col = LegacyColumnRef::SampleValue;
        let err = bridge_windowed_agg_to_canonical(&agg, &window, &col, &s).unwrap_err();
        assert!(matches!(err, BridgeError::UnsupportedWindowKind { .. }));
    }

    #[test]
    fn bridge_named_col_resolves_against_schema() {
        // When the legacy `col` is a Named ColumnRef, it must resolve
        // against the inherited schema. We add a third column and check
        // its positional id is what shows up in `inner.by`.
        let mut s = infer_source_schema("m");
        s.columns.push(Column {
            name: "host".into(),
            dtype: DataType::Utf8,
            nullable: false,
        });
        let agg = AggIntent::Cardinality {
            accuracy: AccuracyTarget::Epsilon(0.01),
        };
        let window = tumbling_spec(60);
        let col = LegacyColumnRef::Named("host".into());
        let b = bridge_windowed_agg_to_canonical(&agg, &window, &col, &s).unwrap();
        // "host" is at position 2 (after ts, value).
        assert_eq!(b.inner.by, vec![2usize]);
    }

    #[test]
    fn bridge_unknown_col_surfaces_resolve_error() {
        // A Named col that isn't in the schema surfaces ResolveError —
        // this matches the γ1 Aggregate-bridge's NotFound semantics
        // (callers should log + keep the legacy variant).
        let s = infer_source_schema("m");
        let agg = AggIntent::Sum;
        let window = tumbling_spec(60);
        let col = LegacyColumnRef::Named("nonexistent".into());
        let err = bridge_windowed_agg_to_canonical(&agg, &window, &col, &s).unwrap_err();
        assert!(matches!(err, BridgeError::Col(ResolveError::NotFound { .. })));
    }

    #[test]
    fn bridge_wildcard_col_surfaces_resolve_error() {
        // ColumnRef::Wildcard has no positional id — bridge surfaces
        // WildcardNotPositional via the Col variant.
        let s = infer_source_schema("m");
        let agg = AggIntent::Frequency {
            accuracy: AccuracyTarget::Exact,
        };
        let window = tumbling_spec(60);
        let col = LegacyColumnRef::Wildcard;
        let err = bridge_windowed_agg_to_canonical(&agg, &window, &col, &s).unwrap_err();
        assert!(matches!(
            err,
            BridgeError::Col(ResolveError::WildcardNotPositional)
        ));
    }

    #[test]
    fn output_schema_for_windowed_agg_strips_time_axis() {
        // Inner Aggregate strips the time index (one row per group per
        // window). The outer Window doesn't re-introduce it as a column
        // — window bounds live on the Window node, not the schema.
        let s = infer_source_schema("m");
        assert!(s.time_index.is_some()); // input has a time axis
        let out =
            output_schema_for_windowed_agg(&s, &[1usize], &[AggIntent::Sum]);
        assert!(out.time_index.is_none());
    }

    #[test]
    fn output_schema_for_windowed_agg_carries_by_col_and_agg_col() {
        // Output shape: the single by column (positional) followed by
        // one synthesized agg column per intent. unique_keys = [[0]].
        let mut s = infer_source_schema("m");
        s.columns.push(Column {
            name: "host".into(),
            dtype: DataType::Utf8,
            nullable: false,
        });
        let out = output_schema_for_windowed_agg(
            &s,
            &[2usize], // by = [host]
            &[AggIntent::Cardinality {
                accuracy: AccuracyTarget::Epsilon(0.01),
            }],
        );
        // Output columns: host, cardinality.
        assert_eq!(out.columns.len(), 2);
        assert_eq!(out.columns[0].name, "host");
        // unique_keys = [[0]] — the single by column is unique by
        // construction in the post-aggregate frame.
        assert_eq!(out.unique_keys, vec![vec![0usize]]);
    }

    #[test]
    fn output_schema_for_windowed_agg_drops_out_of_range_by_ids() {
        // Mirrors the γ1 output_schema_for_aggregate convention.
        let s = infer_source_schema("m");
        // schema only has columns 0..=1; ask for by=[7] which is out
        // of range.
        let out = output_schema_for_windowed_agg(&s, &[7usize], &[AggIntent::Sum]);
        // The out-of-range by id is silently dropped; output has only
        // the agg.
        assert_eq!(out.columns.len(), 1);
        assert_eq!(out.columns[0].name, "sum");
    }

    #[test]
    fn bridge_round_trips_through_output_schema_helper() {
        // End-to-end: build a WindowedAgg's canonical view, then feed
        // its inner `by` / `aggs` through the output_schema helper.
        // The resulting schema must match what a canonical
        // `Window { child: Aggregate {..} }` would produce.
        let s = infer_source_schema("requests");
        let agg = AggIntent::Quantile {
            q: 0.5,
            accuracy: AccuracyTarget::Epsilon(0.01),
        };
        let window = sliding_spec(300, 60);
        let col = LegacyColumnRef::SampleValue;
        let b = bridge_windowed_agg_to_canonical(&agg, &window, &col, &s).unwrap();
        let out =
            output_schema_for_windowed_agg(&s, &b.inner.by, &b.inner.aggs);
        // by = [1 (value)]; aggs = [Quantile]. Output: value, quantile_0_5.
        assert_eq!(out.columns.len(), 2);
        assert_eq!(out.columns[0].name, "value");
        // Time axis stripped; unique key = [[0]].
        assert!(out.time_index.is_none());
        assert_eq!(out.unique_keys, vec![vec![0usize]]);
    }
}
