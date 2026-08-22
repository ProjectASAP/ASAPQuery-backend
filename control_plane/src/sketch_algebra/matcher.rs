//! The reference downstream implementation of [`asap_aware_mapping::Matcher`].
//!
//! `asap_aware_mapping::boundary::Matcher` is a trait with no default implementation
//! and no shipped instance — deliberately, per its crate doc: which
//! `Implementation`s are actually *available* anywhere is entirely a
//! downstream deployment's concern, and even the pure sketch-algebra
//! compatibility rules (a heap-bearing top-k sketch also satisfying a bare
//! frequency point-query) turned out to have deployment-specific competitors
//! (single-vs-multi-population re-aggregation) that don't reduce to a fact
//! about `SummaryKind` alone.
//!
//! [`SummaryFamilyMatcher`] restores exactly the family-compatibility logic
//! that briefly lived as a concrete `Implementation::is_satisfied_by` method
//! in `asap-plan` (PR #140) before it was converted into this trait (PR
//! #141) — the `SummaryFamily`/`summary_family` classifier below is a
//! verbatim port. It answers only the kind-family question: "is an
//! available `(SummaryKind, SummaryParams)` pair an acceptable substitute
//! for a required one." It deliberately does **not** attempt the
//! single-vs-multi-population re-aggregation question (e.g. "can a keyed
//! `Sum` accumulator serve an unkeyed `Sum` query") — `Implementation`
//! carries no grouping information at all (grouping lives beside the kind,
//! on whatever node carries it, not inside the kind — see
//! `crates/asap_types/src/key_by_label_names.rs`'s module doc for the same
//! design call made on the data-plane side), so a two-`Implementation`
//! `Matcher` cannot correctly answer that question. A caller needing that
//! richer, grouping-aware answer must check `AggregationType` compatibility
//! (analogous to `SummaryFamily` here) *and* `grouping_labels`
//! subset-compatibility side by side, composed at the call site rather than
//! inside a single `Matcher::is_satisfied_by`.
//!
//! Retirement note (2026-07) — reverted: this file was briefly gutted down
//! to just [`sketch_family_satisfied`] on the assumption `SummaryFamilyMatcher`
//! had no real consumer (true at the time — no `asap_plan` signature took
//! `impl`/`dyn Matcher` yet). That assumption no longer holds: this struct
//! is the intended `Matcher` implementation for the `SummaryExecutor`
//! serving-time rollout (`data_plane/docs/l4node-plan-executor-design.md`),
//! which is actively being built to replace
//! `storage_engines/sketch_db/query/sketch_reducer.rs`. Restored in full.

use asap_aware_mapping::{Implementation, Matcher};
use planner_types::post_asap::SketchKind;

/// [`Matcher`] impl covering pure sketch-family compatibility. See the
/// module doc for what this deliberately does not cover.
pub struct SummaryFamilyMatcher;

impl Matcher for SummaryFamilyMatcher {
    /// `required` is what an intent needs; `available` is what some
    /// downstream inventory (a sketch index, a policy registry, …)
    /// already has materialized somewhere. Rules:
    /// - `PassThrough` required is vacuously satisfied by anything — no
    ///   summary is needed at all, so there is nothing to match.
    /// - An `ExactAccumulator` is satisfied only by the exact same
    ///   `SummaryKind` — accumulators carry no notion of "family"; `Sum`
    ///   and `MinMax` answer different questions, full stop.
    /// - A `Sketch` is satisfied by an available sketch in the same family
    ///   (`Kll`/`DDSketch` are interchangeable quantile answers;
    ///   `Hll`/`Theta`/`Kmv` are interchangeable cardinality answers), with
    ///   one asymmetric exception: a heap-bearing top-k sketch
    ///   (`CmsWithHeap`/`CountSketchWithHeap`) also answers a bare
    ///   frequency point-query (`Cms`/`CountSketch`) — the heap is
    ///   additional info layered on top of the same underlying matrix —
    ///   but not the reverse (a heap-less sketch cannot enumerate top-k
    ///   items it never tracked).
    fn is_satisfied_by(&self, required: &Implementation, available: &Implementation) -> bool {
        // ASAPController#170 had merged `Sketch`/`ExactAccumulator` into
        // one `Summary { kind, params }` variant, recoverable via
        // `kind.is_exact()`; ASAPPlanner#218 split them back into
        // distinct `Sketch`/`ExactAggregate` variants (see
        // control_plane/docs/design-asapplanner-pin-migration.md). The
        // variant-tag mismatch this match falls through to `_ => false`
        // for (comparing a `Sketch` against an `ExactAggregate`) is now
        // just the natural consequence of them being separate variants
        // again, same behavior as the `is_exact()` mismatch this replaced.
        match (required, available) {
            (Implementation::PassThrough, _) => true,
            (
                Implementation::ExactAggregate { kind: required, .. },
                Implementation::ExactAggregate { kind: have, .. },
            ) => required == have,
            (
                Implementation::Sketch { kind: required, .. },
                Implementation::Sketch { kind: have, .. },
            ) => sketch_family_satisfied(required, have),
            _ => false,
        }
    }
}

/// Pure `SketchKind`-to-`SketchKind` family-compatibility check — the
/// same rule [`SummaryFamilyMatcher::is_satisfied_by`] applies in its
/// `Sketch` arm, exposed directly for callers that only have bare kinds
/// (no [`planner_types::post_asap::SketchParams`]) to compare.
/// `control_plane::sketch_algebra::capability::Capability::is_satisfied_by`
/// is the first such caller: its `SketchKindHandle` query-side dispatch
/// tag never carries params, so constructing a full
/// `Implementation::Sketch{kind, params}` just to discard the params
/// would mean fabricating meaningless param values. See that module's
/// doc for why `Capability`/`SketchKindHandle` themselves aren't deleted
/// outright (`scratchpad/artifacts/enum-unification-plan.md` §8 Step 4).
pub fn sketch_family_satisfied(required: &SketchKind, available: &SketchKind) -> bool {
    let req_family = summary_family(required);
    let have_family = summary_family(available);
    req_family.satisfied_by(have_family)
}

/// The family a [`SketchKind`] belongs to, for [`SummaryFamilyMatcher`].
/// Total now (every `SketchKind` variant is an approximate-sketch family
/// by construction, post ASAPPlanner#218's split — the exact-accumulator
/// kinds this used to also cover live in `ExactKind` now, a distinct type
/// this function never sees).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SummaryFamily {
    Quantile,
    Cardinality,
    /// Bare per-item frequency point-query — no heavy-hitter heap.
    Frequency,
    /// Frequency, augmented with a heavy-hitter heap for top-k extraction.
    FrequencyTopk,
}

impl SummaryFamily {
    /// True when `available` (this family) satisfies `self` (the required
    /// family). Same family always satisfies; the one asymmetric case is
    /// `FrequencyTopk` (heap-bearing) satisfying a bare `Frequency`
    /// requirement — never the reverse.
    fn satisfied_by(self, available: SummaryFamily) -> bool {
        self == available
            || (self == SummaryFamily::Frequency && available == SummaryFamily::FrequencyTopk)
    }
}

fn summary_family(kind: &SketchKind) -> SummaryFamily {
    match kind {
        SketchKind::Kll | SketchKind::DDSketch => SummaryFamily::Quantile,
        SketchKind::Hll | SketchKind::Theta | SketchKind::Kmv => SummaryFamily::Cardinality,
        SketchKind::Cms | SketchKind::CountSketch => SummaryFamily::Frequency,
        SketchKind::CmsWithHeap | SketchKind::CountSketchWithHeap => SummaryFamily::FrequencyTopk,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use planner_types::post_asap::{ExactKind, ExactParams, SketchParams};

    /// A valid `SketchParams` for `kind` — `is_satisfied_by` only matches
    /// on `kind`, never `params`, but the test values should still be
    /// real, constructible `(kind, params)` pairs rather than nonsense
    /// combinations (e.g. `Hll` paired with `Kll`'s params) that could
    /// never arise from real code.
    fn params_for(kind: &SketchKind) -> SketchParams {
        match kind {
            SketchKind::Kll => SketchParams::Kll { k: 200 },
            SketchKind::Cms => SketchParams::Cms {
                width: 100,
                depth: 5,
            },
            SketchKind::Hll => SketchParams::Hll { precision: 14 },
            SketchKind::DDSketch => SketchParams::DDSketch { alpha: 0.01 },
            SketchKind::CmsWithHeap => SketchParams::CmsWithHeap {
                width: 100,
                depth: 5,
                heap_size: 10,
            },
            SketchKind::Kmv => SketchParams::Kmv { k: 1024 },
            SketchKind::Theta => SketchParams::Theta { k: 1024 },
            SketchKind::CountSketch => SketchParams::CountSketch {
                width: 100,
                depth: 5,
            },
            SketchKind::CountSketchWithHeap => SketchParams::CountSketchWithHeap {
                width: 100,
                depth: 5,
                heap_size: 10,
            },
        }
    }

    fn exact_params_for(kind: &ExactKind) -> ExactParams {
        match kind {
            ExactKind::Sum => ExactParams::Sum,
            ExactKind::Count => ExactParams::Count,
            ExactKind::MinMax => ExactParams::MinMax,
            ExactKind::Increase => ExactParams::Increase,
            ExactKind::Rate => ExactParams::Rate,
        }
    }

    fn sketch(kind: SketchKind) -> Implementation {
        let params = params_for(&kind);
        Implementation::Sketch { kind, params }
    }

    fn accumulator(kind: ExactKind) -> Implementation {
        let params = exact_params_for(&kind);
        Implementation::ExactAggregate { kind, params }
    }

    #[test]
    fn same_sketch_kind_satisfies_itself() {
        let m = SummaryFamilyMatcher;
        assert!(m.is_satisfied_by(&sketch(SketchKind::Kll), &sketch(SketchKind::Kll)));
        assert!(m.is_satisfied_by(&sketch(SketchKind::Hll), &sketch(SketchKind::Hll)));
    }

    #[test]
    fn same_family_alternate_kind_satisfies() {
        let m = SummaryFamilyMatcher;
        // Kll / DDSketch are interchangeable quantile answers.
        assert!(m.is_satisfied_by(&sketch(SketchKind::Kll), &sketch(SketchKind::DDSketch)));
        // Hll / Theta / Kmv are interchangeable cardinality answers.
        assert!(m.is_satisfied_by(&sketch(SketchKind::Hll), &sketch(SketchKind::Theta)));
        assert!(m.is_satisfied_by(&sketch(SketchKind::Hll), &sketch(SketchKind::Kmv)));
    }

    #[test]
    fn cross_family_never_satisfies() {
        let m = SummaryFamilyMatcher;
        assert!(!m.is_satisfied_by(&sketch(SketchKind::Kll), &sketch(SketchKind::Hll)));
        assert!(!m.is_satisfied_by(&sketch(SketchKind::Hll), &sketch(SketchKind::Kll)));
        assert!(!m.is_satisfied_by(&sketch(SketchKind::Kll), &sketch(SketchKind::Cms)));
    }

    #[test]
    fn heap_bearing_available_satisfies_bare_frequency_required() {
        let m = SummaryFamilyMatcher;
        // A CmsWithHeap instance already carries the plain CMS matrix, so
        // it answers a bare frequency point-query too.
        assert!(m.is_satisfied_by(&sketch(SketchKind::Cms), &sketch(SketchKind::CmsWithHeap)));
        assert!(m.is_satisfied_by(
            &sketch(SketchKind::CountSketch),
            &sketch(SketchKind::CountSketchWithHeap)
        ));
    }

    #[test]
    fn bare_frequency_available_does_not_satisfy_topk_required() {
        let m = SummaryFamilyMatcher;
        // The reverse does not hold: a heap-less sketch never tracked the
        // heavy-hitter heap, so it cannot enumerate top-k items.
        assert!(!m.is_satisfied_by(&sketch(SketchKind::CmsWithHeap), &sketch(SketchKind::Cms)));
        assert!(!m.is_satisfied_by(
            &sketch(SketchKind::CountSketchWithHeap),
            &sketch(SketchKind::CountSketch)
        ));
    }

    #[test]
    fn cms_and_count_sketch_are_the_same_frequency_family() {
        let m = SummaryFamilyMatcher;
        assert!(m.is_satisfied_by(&sketch(SketchKind::Cms), &sketch(SketchKind::CountSketch)));
        assert!(m.is_satisfied_by(
            &sketch(SketchKind::CmsWithHeap),
            &sketch(SketchKind::CountSketchWithHeap)
        ));
    }

    #[test]
    fn exact_accumulator_requires_the_exact_same_kind() {
        let m = SummaryFamilyMatcher;
        assert!(m.is_satisfied_by(&accumulator(ExactKind::Sum), &accumulator(ExactKind::Sum)));
        assert!(!m.is_satisfied_by(
            &accumulator(ExactKind::Sum),
            &accumulator(ExactKind::MinMax)
        ));
        assert!(!m.is_satisfied_by(
            &accumulator(ExactKind::Increase),
            &accumulator(ExactKind::Rate)
        ));
    }

    #[test]
    fn sketch_and_accumulator_never_satisfy_each_other() {
        let m = SummaryFamilyMatcher;
        assert!(!m.is_satisfied_by(&sketch(SketchKind::Kll), &accumulator(ExactKind::Sum)));
        assert!(!m.is_satisfied_by(&accumulator(ExactKind::Sum), &sketch(SketchKind::Kll)));
    }

    #[test]
    fn pass_through_required_is_vacuously_satisfied() {
        let m = SummaryFamilyMatcher;
        assert!(m.is_satisfied_by(&Implementation::PassThrough, &sketch(SketchKind::Kll)));
        assert!(m.is_satisfied_by(&Implementation::PassThrough, &accumulator(ExactKind::Sum)));
        assert!(m.is_satisfied_by(&Implementation::PassThrough, &Implementation::PassThrough));
    }

    #[test]
    fn pass_through_available_never_satisfies_a_real_requirement() {
        let m = SummaryFamilyMatcher;
        assert!(!m.is_satisfied_by(&sketch(SketchKind::Kll), &Implementation::PassThrough));
        assert!(!m.is_satisfied_by(&accumulator(ExactKind::Sum), &Implementation::PassThrough));
    }

    // ── sketch_family_satisfied (the bare-kind entry point) ──────────────

    #[test]
    fn sketch_family_satisfied_matches_is_satisfied_by_on_the_sketch_arm() {
        // The free function is meant to be exactly the logic
        // `SummaryFamilyMatcher::is_satisfied_by` applies to its `Sketch`
        // arm, just without needing `SketchParams` to call it.
        assert!(sketch_family_satisfied(
            &SketchKind::Kll,
            &SketchKind::DDSketch
        ));
        assert!(sketch_family_satisfied(
            &SketchKind::Cms,
            &SketchKind::CmsWithHeap
        ));
        assert!(!sketch_family_satisfied(
            &SketchKind::CmsWithHeap,
            &SketchKind::Cms
        ));
        assert!(!sketch_family_satisfied(&SketchKind::Kll, &SketchKind::Hll));
    }

    // The old `sketch_family_satisfied_rejects_exact_accumulator_kinds`
    // test (passing `SummaryKind::Sum` -- an exact-accumulator kind -- to
    // this sketch-only function) no longer type-checks at all post
    // ASAPPlanner#218's split: `SketchKind` has no exact-accumulator
    // variants to construct in the first place, so the property that test
    // asserted is now enforced by the type system instead of at runtime.
}
