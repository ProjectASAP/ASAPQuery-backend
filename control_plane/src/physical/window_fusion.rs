//! The `Window { child: Aggregate }` fusion recognizer.
//!
//! ## The invariant this protects
//!
//! In sketch systems the window defines the sketch's lifecycle (when to
//! flush / reset), so a windowed sketch aggregation must be planned as a
//! *single* fused physical node — `PhysicalOp::OtelSketchBuild { window }`
//! — where the window and the sketch build are resolved together.
//!
//! The canonical L3 IR keeps "one canonical form per plan" (design.md
//! §6): it has no fused windowed-aggregate variant — a windowed sketch is
//! the stacked `Window { child: Aggregate { measures: [one], .. } }` shape.
//! `lower` produces exactly that shape when it folds a
//! single-statistic sketchable `Aggregate` sitting over a `Window`. This
//! module is the planner-side **peephole recognizer** that puts the
//! window-defines-sketch-lifecycle invariant back: it matches the stacked
//! shape and reconstructs the fused physical node.
//!
//! [`recognize_windowed_sketch`] matches the stacked canonical shape and
//! returns a [`FusedWindowSketch`] view; [`fused_sketch_decision`]
//! reconstructs the `(PhysicalOp, Placement)` for the fused sketch build.
//! It is on the canonical planner's hot path; the `#[cfg(test)]`
//! equivalence harness below pins it end-to-end against `plan`.

#![allow(dead_code)]

use std::time::Duration;

use crate::intent_algebra::agg_intent::AggIntent;
use crate::intent_algebra::query_expr::{QueryExpr, WindowKind};
use crate::intent_algebra::schema::ColumnId;
use crate::physical::planner::{
    decide_sketch_placement, resolve, PhysicalOp, PhysicalPlannerConfig, PhysicalWindow, Placement,
};

/// Canonical-shape view of a fused window-over-sketch — the canonical
/// counterpart of a legacy `WindowedAgg` node, recognized out of the
/// stacked `Window { child: Aggregate { .. } }` form.
#[derive(Debug, Clone)]
pub struct FusedWindowSketch<'a> {
    /// The single aggregation intent the sketch computes.
    pub agg: &'a AggIntent,
    /// Outer `Window`'s kind.
    pub window_kind: WindowKind,
    /// Outer `Window`'s `size`.
    pub window_size: Duration,
    /// Outer `Window`'s `slide` (`Some` only for `Sliding`).
    pub window_slide: Option<Duration>,
    /// The inner `Aggregate`'s grouping columns -- empty both when it's a
    /// genuine empty-`by` reduction and when it's per-entity (no grouping
    /// concept at all, ASAPController#163/#165); this field doesn't
    /// distinguish the two, since nothing downstream currently needs to.
    pub by: &'a [ColumnId],
    /// The subtree below the inner `Aggregate` — the sketch's input.
    pub inner_child: &'a QueryExpr,
}

/// Recognize the canonical `Window { child: Aggregate { measures: [one],
/// having: None, .. } }` shape — the stacked form `lower`
/// folds a legacy `WindowedAgg` into — and return a [`FusedWindowSketch`]
/// view of it. Returns `None` for any other shape (a multi-intent
/// `Aggregate`, an `Aggregate` with a `having` clause, a `Window` over a
/// non-`Aggregate` child, etc.).
///
/// A bare canonical `Aggregate` with no enclosing `Window` is *not*
/// recognized here — that is the unfused `SketchAgg` case, which the
/// legacy planner already builds with `PhysicalWindow::None`. This
/// recognizer is specifically the window-defines-lifecycle peephole.
pub fn recognize_windowed_sketch(expr: &QueryExpr) -> Option<FusedWindowSketch<'_>> {
    // ASAPPlanner has no canonical `Window` node (ASAPPlanner#193 --
    // "nothing ever constructs canonical QueryExpr::Window", confirmed by
    // upstream's own corpus walk, true even at the previously-pinned
    // rev): `asap_frontend_promql::lower_promql` -- the real production
    // entry point since #428 -- has only ever emitted `TimeRange { range,
    // child }` for a range-vector selector. This recognizer used to match
    // `Window` regardless, which means it could never actually fire for
    // real `lower_promql`-produced trees; matching `TimeRange` here is a
    // fix, not just a rename. `TimeRange` carries no kind/slide -- real
    // PromQL has no syntax to request anything but the implicit tumbling
    // form, so `window_kind`/`window_slide` default to that (matching
    // every other real-traffic call site in this codebase; see
    // control_plane/docs/design-asapplanner-pin-migration.md).
    let QueryExpr::TimeRange { range, child } = expr else {
        return None;
    };
    let QueryExpr::Aggregate {
        reduction,
        measures,
        having,
        child: inner_child,
        ..
    } = child.as_ref()
    else {
        return None;
    };
    // The legacy `WindowedAgg` always carried exactly one intent and
    // never a HAVING clause — only that shape is the fused sketch.
    if measures.len() != 1 || having.is_some() {
        return None;
    }
    let by: &[ColumnId] = reduction
        .group_keys()
        .map(|keys| keys.keys())
        .unwrap_or(&[]);
    Some(FusedWindowSketch {
        agg: &measures[0],
        window_kind: WindowKind::Tumbling,
        window_size: *range,
        window_slide: None,
        by,
        inner_child,
    })
}

/// Resolve a canonical `(WindowKind, size, slide)` to a
/// [`PhysicalWindow`] for a given placement.
///
/// The canonical-IR twin of the legacy `WindowSpec`-based window
/// resolution. Every canonical `WindowKind` variant maps — the legacy
/// `WindowKind` is now `Tumbling` / `Sliding` / `Session` only (the
/// catalogue-less `Unbounded` / `Landmark` were retired in PR 12), so
/// the legacy → canonical `WindowKind` mapping is total.
pub fn resolve_window_canonical(
    kind: &WindowKind,
    size: Duration,
    _slide: Option<Duration>,
    placement: &Placement,
) -> PhysicalWindow {
    match (kind, placement) {
        (WindowKind::Tumbling, Placement::AgentCollector) => {
            PhysicalWindow::OtelTumblingFlush { duration: size }
        }
        (WindowKind::Tumbling, Placement::PromSketchStore) => PhysicalWindow::PromSketchEH {
            eh_k: 50,
            time_window: size,
        },
        (WindowKind::Sliding, Placement::PromSketchStore) => PhysicalWindow::PromSketchEH {
            eh_k: 50,
            time_window: size,
        },
        // Fallback: tumbling-at-size for any other (kind, placement)
        // combo — mirrors the legacy `resolve_window` fallback arm.
        _ => PhysicalWindow::OtelTumblingFlush { duration: size },
    }
}

/// Reconstruct the `(PhysicalOp, Placement)` the legacy
/// `physical::planner::plan_node` `WindowedAgg` arm produces, but from
/// the canonical [`FusedWindowSketch`] view.
///
/// This is the piece PR 8 calls on the hot path once the canonical
/// `plan_node` lands; PR 5 only exercises it from the equivalence
/// harness. It deliberately produces just the node's own decision (op +
/// placement) — the child subtree is the canonical planner's job, which
/// does not exist until PR 8.
pub fn fused_sketch_decision(
    fused: &FusedWindowSketch<'_>,
    config: &PhysicalPlannerConfig,
) -> (PhysicalOp, Placement) {
    let resolved = resolve(fused.agg);
    let placement = decide_sketch_placement(&resolved, config);
    let window = resolve_window_canonical(
        &fused.window_kind,
        fused.window_size,
        fused.window_slide,
        &placement,
    );
    let op = PhysicalOp::OtelSketchBuild {
        sketch_type: resolved.sketch_type.clone(),
        sketch_params: resolved.sketch_params.clone(),
        window,
        delta_encoding: false,
    };
    (op, placement)
}

// ── Equivalence harness ──────────────────────────────────────────────────────
//
// Pins `recognize_windowed_sketch` + `fused_sketch_decision` against the
// canonical planner: a `Window { Aggregate }` fused sketch — the shape
// `lower` folds a single-statistic sketchable `Aggregate`
// over a `Window` into — produces a resolved (non-`None`) physical window,
// and the planner's own decision matches `fused_sketch_decision`.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent_algebra::query_expr::Source;
    use crate::intent_algebra::schema::Schema;
    use crate::intent_algebra::{default_cardinality, default_frequency, default_quantile};
    use crate::optimizer::engine::DeploymentConstraints;
    use crate::physical::planner::{plan, PhysicalOp};
    use crate::types::StageResourceBudgets;

    fn config() -> PhysicalPlannerConfig {
        PhysicalPlannerConfig {
            budgets: StageResourceBudgets::default(),
            constraints: DeploymentConstraints::default(),
        }
    }

    /// A canonical `Scan` leaf, built directly — the pre-ASAPPlanner-pin
    /// version of this helper built it through `convert_root`
    /// (`intent_algebra::lower`), which was deleted alongside the L2 tree
    /// it converted from (see `intent_algebra::relational`'s module doc);
    /// `Scan` is simple enough to construct directly instead.
    fn canonical_scan(metric: &str) -> QueryExpr {
        QueryExpr::Scan {
            source: Source::TimeSeries {
                metric: metric.to_string(),
            },
            predicates: vec![],
            schema: Schema::with_time_index(vec![], 0, vec![]),
        }
    }

    /// The canonical `TimeRange { Aggregate { by: [], measures: [agg] } }`
    /// shape — what `asap_frontend_promql::lower_promql` (the real
    /// production entry point) emits for a windowed single-sketch
    /// aggregate. Real PromQL has no syntax for anything but the implicit
    /// tumbling form (see `recognize_windowed_sketch`'s doc), so unlike
    /// the pre-migration version of this helper, there is no `kind`/
    /// `slide` parameter to take anymore.
    fn windowed_sketch(agg: AggIntent, size: Duration) -> QueryExpr {
        QueryExpr::TimeRange {
            range: size,
            child: Box::new(QueryExpr::Aggregate {
                reduction: crate::intent_algebra::Reduction::by(vec![]),
                measures: vec![agg],
                output_names: Vec::new(),
                having: None,
                child: Box::new(canonical_scan("m")),
            }),
        }
    }

    /// End-to-end fusion assertion: the canonical `TimeRange { Aggregate }`
    /// shape, run through the recognizer + `fused_sketch_decision` and
    /// through the canonical planner, produces a *fused* `OtelSketchBuild`
    /// carrying a resolved (non-`None`) physical window — the
    /// window-defines-sketch-lifecycle invariant as a planner peephole.
    fn assert_equivalent(agg: AggIntent, size: Duration) {
        let cfg = config();
        let canonical = windowed_sketch(agg, size);

        let fused = recognize_windowed_sketch(&canonical)
            .expect("canonical TimeRange{Aggregate} should be recognized as a fused sketch");
        let (decided_op, _placement) = fused_sketch_decision(&fused, &cfg);
        let PhysicalOp::OtelSketchBuild {
            window: decided_window,
            ..
        } = &decided_op
        else {
            panic!("fused_sketch_decision did not produce OtelSketchBuild: {decided_op:?}");
        };
        assert!(
            !matches!(decided_window, PhysicalWindow::None),
            "fused windowed sketch must carry a resolved physical window, got None"
        );

        // End-to-end: the canonical planner produces the same fused
        // `OtelSketchBuild` — `recognize_windowed_sketch` is on its hot path.
        let node = plan(&canonical, &cfg);
        let PhysicalOp::OtelSketchBuild {
            window: planned_window,
            ..
        } = &node.op
        else {
            panic!(
                "canonical planner did not fuse TimeRange{{Aggregate}}: {:?}",
                node.op
            );
        };
        // PhysicalWindow has no PartialEq — compare its Debug form.
        assert_eq!(
            format!("{decided_window:?}"),
            format!("{planned_window:?}"),
            "planner's fused window diverged from fused_sketch_decision"
        );
    }

    #[test]
    fn equivalence_quantile() {
        assert_equivalent(default_quantile(0.99), Duration::from_secs(300));
    }

    #[test]
    fn equivalence_cardinality() {
        assert_equivalent(default_cardinality(), Duration::from_secs(60));
    }

    #[test]
    fn equivalence_frequency() {
        assert_equivalent(default_frequency(), Duration::from_secs(120));
    }

    #[test]
    fn equivalence_sum_intent() {
        assert_equivalent(AggIntent::Sum { col: None }, Duration::from_secs(300));
    }

    #[test]
    fn recognizer_rejects_bare_aggregate() {
        // A canonical `Aggregate` with no enclosing `TimeRange` is the
        // unfused sketch case — not a windowed sketch.
        let canonical = QueryExpr::Aggregate {
            reduction: crate::intent_algebra::Reduction::by(vec![]),
            measures: vec![AggIntent::Sum { col: None }],
            output_names: Vec::new(),
            having: None,
            child: Box::new(canonical_scan("m")),
        };
        assert!(recognize_windowed_sketch(&canonical).is_none());
    }

    #[test]
    fn recognizer_rejects_window_over_non_aggregate() {
        // `TimeRange` directly over a `Scan` — no inner `Aggregate` to fuse.
        let canonical = QueryExpr::TimeRange {
            range: Duration::from_secs(60),
            child: Box::new(canonical_scan("m")),
        };
        assert!(recognize_windowed_sketch(&canonical).is_none());
    }

    #[test]
    fn recognizer_accepts_windowed_agg() {
        // The `TimeRange { Aggregate { Quantile } }` shape
        // `lower_promql` emits for `quantile_over_time(...)` — exactly
        // the shape the recognizer matches.
        let canonical = windowed_sketch(default_quantile(0.99), Duration::from_secs(300));
        let fused = recognize_windowed_sketch(&canonical).expect("should recognize");
        assert_eq!(fused.window_kind, WindowKind::Tumbling);
        assert_eq!(fused.window_size, Duration::from_secs(300));
        assert!(fused.window_slide.is_none());
        assert!(matches!(fused.agg, AggIntent::Quantile { .. }));
    }
}
