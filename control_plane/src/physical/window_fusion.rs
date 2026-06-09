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
//! the stacked `Window { child: Aggregate { aggs: [one], .. } }` shape.
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
    /// The inner `Aggregate`'s `by` columns.
    pub by: &'a [ColumnId],
    /// The subtree below the inner `Aggregate` — the sketch's input.
    pub inner_child: &'a QueryExpr,
}

/// Recognize the canonical `Window { child: Aggregate { aggs: [one],
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
    use crate::intent_algebra::relational::{
        AggFunc, AggItem, ColumnRef as LColumnRef, QueryExpr as LQueryExpr, SourceSpec,
    };
    use crate::intent_algebra::{
        convert_root, default_cardinality, default_frequency, default_quantile,
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

    /// A canonical `Scan` leaf — built through `convert_root` so it carries
    /// the same Binder-built schema a real converted tree would.
    fn canonical_scan(metric: &str) -> QueryExpr {
        convert_root(&LQueryExpr::Source(SourceSpec {
            name: metric.into(),
        }))
        .expect("convert source")
    }

    /// The canonical `Window { Aggregate { by: [], aggs: [agg] } }` shape —
    /// the fold `lower` produces for a windowed single-sketch
    /// aggregate.
    fn windowed_sketch(
        agg: AggIntent,
        kind: WindowKind,
        size: Duration,
        slide: Option<Duration>,
    ) -> QueryExpr {
        QueryExpr::Window {
            kind,
            size,
            slide,
            child: Box::new(QueryExpr::Aggregate {
                by: Vec::new(),
                aggs: vec![agg],
                having: None,
                child: Box::new(canonical_scan("m")),
            }),
        }
    }

    /// End-to-end fusion assertion: the canonical `Window { Aggregate }`
    /// shape, run through the recognizer + `fused_sketch_decision` and
    /// through the canonical planner, produces a *fused* `OtelSketchBuild`
    /// carrying a resolved (non-`None`) physical window — the
    /// window-defines-sketch-lifecycle invariant as a planner peephole.
    fn assert_equivalent(
        agg: AggIntent,
        kind: WindowKind,
        size: Duration,
        slide: Option<Duration>,
    ) {
        let cfg = config();
        let canonical = windowed_sketch(agg, kind, size, slide);

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
            panic!(
                "canonical planner did not fuse Window{{Aggregate}}: {:?}",
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
    fn equivalence_tumbling_quantile() {
        assert_equivalent(
            default_quantile(0.99),
            WindowKind::Tumbling,
            Duration::from_secs(300),
            None,
        );
    }

    #[test]
    fn equivalence_sliding_quantile() {
        assert_equivalent(
            default_quantile(0.5),
            WindowKind::Sliding,
            Duration::from_secs(600),
            Some(Duration::from_secs(60)),
        );
    }

    #[test]
    fn equivalence_session_quantile() {
        assert_equivalent(
            default_quantile(0.95),
            WindowKind::Session,
            Duration::from_secs(30),
            None,
        );
    }

    #[test]
    fn equivalence_tumbling_cardinality() {
        assert_equivalent(
            default_cardinality(),
            WindowKind::Tumbling,
            Duration::from_secs(60),
            None,
        );
    }

    #[test]
    fn equivalence_tumbling_frequency() {
        assert_equivalent(
            default_frequency(),
            WindowKind::Tumbling,
            Duration::from_secs(120),
            None,
        );
    }

    #[test]
    fn equivalence_sum_intent() {
        assert_equivalent(
            AggIntent::Sum,
            WindowKind::Tumbling,
            Duration::from_secs(300),
            None,
        );
    }

    #[test]
    fn recognizer_rejects_bare_aggregate() {
        // A canonical `Aggregate` with no enclosing `Window` is the unfused
        // sketch case — not a windowed sketch.
        let canonical = QueryExpr::Aggregate {
            by: Vec::new(),
            aggs: vec![AggIntent::Sum],
            having: None,
            child: Box::new(canonical_scan("m")),
        };
        assert!(recognize_windowed_sketch(&canonical).is_none());
    }

    #[test]
    fn recognizer_rejects_window_over_non_aggregate() {
        // `Window` directly over a `Scan` — no inner `Aggregate` to fuse.
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
        // The L2 `Aggregate { Quantile } over Window` shape the parsers
        // emit converts to canonical `Window { Aggregate }` — exactly the
        // shape the recognizer matches.
        let legacy = LQueryExpr::Aggregate {
            keys: vec![],
            aggs: vec![AggItem {
                alias: "q".into(),
                func: AggFunc::Quantile(0.99),
                col: LColumnRef::SampleValue,
                distinct: false,
            }],
            having: None,
            input: Box::new(LQueryExpr::Window {
                duration: Duration::from_secs(300),
                slide: None,
                input: Box::new(LQueryExpr::Source(SourceSpec { name: "m".into() })),
            }),
        };
        let canonical = convert_root(&legacy).expect("convert");
        let fused = recognize_windowed_sketch(&canonical).expect("should recognize");
        assert_eq!(fused.window_kind, WindowKind::Tumbling);
        assert_eq!(fused.window_size, Duration::from_secs(300));
        assert!(fused.window_slide.is_none());
        assert!(matches!(fused.agg, AggIntent::Quantile { .. }));
    }
}
