//! Step γ7 safety net: the `Window { child: Aggregate }` fusion
//! recognizer.
//!
//! ## The invariant this protects
//!
//! The legacy IR fuses the window and the sketch aggregation into a
//! single `legacy_expr::QueryExpr::WindowedAgg` node, because in sketch
//! systems the window defines the sketch's lifecycle (when to flush /
//! reset). The legacy `physical::planner::plan_node` `WindowedAgg` arm
//! relies on that fused shape: it resolves the window into the *same*
//! `PhysicalOp::OtelSketchBuild { window }` node as the sketch build.
//!
//! The canonical L3 IR has no `WindowedAgg` — `legacy_to_canonical`
//! folds it into the stacked `Window { child: Aggregate { .. } }` shape
//! (design.md §6: "one canonical form per plan"). That moves the
//! window-defines-sketch-lifecycle invariant out of the *IR shape* and
//! into a planner-side **peephole recognizer** — this module.
//!
//! [`recognize_windowed_sketch`] matches the stacked canonical shape and
//! returns a [`FusedWindowSketch`] view; [`fused_sketch_decision`]
//! reconstructs the exact `(PhysicalOp, Placement)` the legacy
//! `WindowedAgg` arm produces. Nothing here is on the hot path yet — PR
//! 8 wires it into the canonical `plan_node`. The `#[cfg(test)]`
//! equivalence harness below pins the reconstruction against the live
//! legacy `WindowedAgg` arm *while that arm is still the source of
//! truth*, so the invariant is proven before any consumer flips.

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
    /// The inner `Aggregate`'s `by` columns.
    pub by: &'a [ColumnId],
    /// The subtree below the inner `Aggregate` — the sketch's input.
    pub inner_child: &'a QueryExpr,
}

/// Recognize the canonical `Window { child: Aggregate { aggs: [one],
/// having: None, .. } }` shape — the stacked form `legacy_to_canonical`
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
    let QueryExpr::Window {
        kind,
        size,
        slide,
        child,
    } = expr
    else {
        return None;
    };
    let QueryExpr::Aggregate {
        by,
        aggs,
        having,
        child: inner_child,
    } = child.as_ref()
    else {
        return None;
    };
    // The legacy `WindowedAgg` always carried exactly one intent and
    // never a HAVING clause — only that shape is the fused sketch.
    if aggs.len() != 1 || having.is_some() {
        return None;
    }
    Some(FusedWindowSketch {
        agg: &aggs[0],
        window_kind: kind.clone(),
        window_size: *size,
        window_slide: *slide,
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
        (WindowKind::Tumbling, Placement::PromSketchStore) => {
            PhysicalWindow::PromSketchEH {
                eh_k: 50,
                time_window: size,
            }
        }
        (WindowKind::Sliding, Placement::PromSketchStore) => PhysicalWindow::PromSketchEH {
            eh_k: 50,
            time_window: size,
        },
        (WindowKind::Tumbling, Placement::Database) => PhysicalWindow::SqlTimeBucket {
            interval: size,
            // Canonical `Window` carries no `time_col`; the legacy
            // `resolve_window` defaults the same way when it is `None`.
            time_col: "ts".into(),
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
// live legacy `WindowedAgg` arm of `physical::planner::plan_node`. While
// the legacy arm is still the source of truth (it is, until PR 8), this
// harness proves the canonical recognizer reconstructs exactly the same
// physical decision — the window-defines-sketch-lifecycle invariant
// survives the un-fused canonical IR shape.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent_algebra::convert_root;
    use crate::intent_algebra::legacy_expr::{
        default_cardinality, default_frequency, default_quantile, ColumnRef as LColumnRef,
        QueryExpr as LQueryExpr, SourceSpec, WindowKind as LWindowKind, WindowSpec,
    };
    use crate::optimizer::engine::DeploymentConstraints;
    use crate::physical::planner::{plan, PhysicalOp};
    use crate::types::StageResourceBudgets;

    fn config() -> PhysicalPlannerConfig {
        PhysicalPlannerConfig {
            budgets: StageResourceBudgets::default(),
            constraints: DeploymentConstraints::default(),
        }
    }

    /// Build a legacy `WindowedAgg { agg, window, SampleValue, Source }`.
    fn legacy_windowed_agg(agg: AggIntent, window: WindowSpec) -> LQueryExpr {
        LQueryExpr::WindowedAgg {
            agg,
            window,
            col: LColumnRef::SampleValue,
            input: Box::new(LQueryExpr::Source(SourceSpec { name: "m".into() })),
        }
    }

    fn tumbling(secs: u64) -> WindowSpec {
        WindowSpec {
            kind: LWindowKind::Tumbling {
                size: Duration::from_secs(secs),
            },
            time_col: None,
        }
    }

    fn sliding(size_secs: u64, slide_secs: u64) -> WindowSpec {
        WindowSpec {
            kind: LWindowKind::Sliding {
                size: Duration::from_secs(size_secs),
                slide: Duration::from_secs(slide_secs),
            },
            time_col: None,
        }
    }

    fn session(gap_secs: u64) -> WindowSpec {
        WindowSpec {
            kind: LWindowKind::Session {
                gap: Duration::from_secs(gap_secs),
            },
            time_col: None,
        }
    }

    /// End-to-end fusion assertion: a legacy `WindowedAgg`, converted to
    /// the canonical `Window { Aggregate }` shape and run through the
    /// canonical planner, produces a *fused* `OtelSketchBuild` carrying a
    /// resolved (non-`None`) physical window. This is the
    /// window-defines-sketch-lifecycle invariant — now a planner peephole
    /// (`recognize_windowed_sketch` + `fused_sketch_decision`, both wired
    /// onto the hot path by PR 8) rather than an IR-shape property.
    ///
    /// Before PR 8 this harness compared the recognizer against the live
    /// legacy `WindowedAgg` planner arm; that arm is now gone (the planner
    /// is canonical), so the assertion is the canonical end-to-end result.
    fn assert_equivalent(agg: AggIntent, window: WindowSpec) {
        let cfg = config();
        let legacy = legacy_windowed_agg(agg, window);
        let canonical = convert_root(&legacy).expect("convert");

        // The recognizer matches the converted `Window { Aggregate }` fold.
        let fused = recognize_windowed_sketch(&canonical)
            .expect("canonical Window{Aggregate} should be recognized as a fused sketch");
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
            panic!("canonical planner did not fuse Window{{Aggregate}}: {:?}", node.op);
        };
        // PhysicalWindow has no PartialEq — compare its Debug form.
        assert_eq!(
            format!("{decided_window:?}"),
            format!("{planned_window:?}"),
            "planner's fused window diverged from fused_sketch_decision"
        );
    }

    #[test]
    fn equivalence_tumbling_quantile() {
        assert_equivalent(default_quantile(0.99), tumbling(300));
    }

    #[test]
    fn equivalence_sliding_quantile() {
        assert_equivalent(default_quantile(0.5), sliding(600, 60));
    }

    #[test]
    fn equivalence_session_quantile() {
        assert_equivalent(default_quantile(0.95), session(30));
    }

    #[test]
    fn equivalence_tumbling_cardinality() {
        assert_equivalent(default_cardinality(), tumbling(60));
    }

    #[test]
    fn equivalence_tumbling_frequency() {
        assert_equivalent(default_frequency(), tumbling(120));
    }

    #[test]
    fn equivalence_sum_intent() {
        assert_equivalent(AggIntent::Sum, tumbling(300));
    }

    #[test]
    fn recognizer_rejects_bare_aggregate() {
        // A canonical Aggregate with no enclosing Window is the unfused
        // SketchAgg case — not a windowed sketch.
        let legacy = LQueryExpr::SketchAgg {
            op: AggIntent::Sum,
            col: LColumnRef::SampleValue,
            input: Box::new(LQueryExpr::Source(SourceSpec { name: "m".into() })),
        };
        let canonical = convert_root(&legacy).expect("convert");
        assert!(recognize_windowed_sketch(&canonical).is_none());
    }

    #[test]
    fn recognizer_rejects_window_over_non_aggregate() {
        // Window directly over a Scan — no inner Aggregate to fuse.
        let legacy = LQueryExpr::Window {
            duration: Duration::from_secs(60),
            slide: None,
            input: Box::new(LQueryExpr::Source(SourceSpec { name: "m".into() })),
        };
        let canonical = convert_root(&legacy).expect("convert");
        assert!(recognize_windowed_sketch(&canonical).is_none());
    }

    #[test]
    fn recognizer_accepts_converted_windowed_agg() {
        // Sanity: the shape legacy_to_canonical folds a WindowedAgg into
        // is exactly what the recognizer matches.
        let legacy = legacy_windowed_agg(default_quantile(0.99), tumbling(300));
        let canonical = convert_root(&legacy).expect("convert");
        let fused = recognize_windowed_sketch(&canonical).expect("should recognize");
        assert_eq!(fused.window_kind, WindowKind::Tumbling);
        assert_eq!(fused.window_size, Duration::from_secs(300));
        assert!(fused.window_slide.is_none());
        assert!(matches!(fused.agg, AggIntent::Quantile { .. }));
    }
}
