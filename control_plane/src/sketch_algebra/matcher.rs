//! The reference downstream implementation of [`asap_plan::Matcher`].
//!
//! `asap_plan::boundary::Matcher` is a trait with no default implementation
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

use asap_plan::{Implementation, Matcher};
use asap_sketch::SummaryKind;

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
        // ASAPController#170 merged `Sketch`/`ExactAccumulator` into one
        // `Summary { kind, params }` variant, recoverable via
        // `kind.is_exact()`. The variant-tag mismatch that used to fall
        // through to `_ => false` (comparing a `Sketch` against an
        // `ExactAccumulator`) is now an explicit `is_exact()` mismatch
        // between the two sides, still falling through the same way.
        match (required, available) {
            (Implementation::PassThrough, _) => true,
            (
                Implementation::Summary { kind: required, .. },
                Implementation::Summary { kind: have, .. },
            ) if required.is_exact() && have.is_exact() => required == have,
            (
                Implementation::Summary { kind: required, .. },
                Implementation::Summary { kind: have, .. },
            ) if !required.is_exact() && !have.is_exact() => sketch_family_satisfied(required, have),
            _ => false,
        }
    }
}

/// Pure `SummaryKind`-to-`SummaryKind` family-compatibility check — the
/// same rule [`SummaryFamilyMatcher::is_satisfied_by`] applies in its
/// `Sketch` arm, exposed directly for callers that only have bare kinds
/// (no [`asap_sketch::SummaryParams`]) to compare.
/// `control_plane::sketch_algebra::capability::Capability::is_satisfied_by`
/// is the first such caller: its `SketchKindHandle` query-side dispatch
/// tag never carries params, so constructing a full
/// `Implementation::Sketch{kind, params}` just to discard the params
/// would mean fabricating meaningless param values. See that module's
/// doc for why `Capability`/`SketchKindHandle` themselves aren't deleted
/// outright (`scratchpad/artifacts/enum-unification-plan.md` §8 Step 4).
pub fn sketch_family_satisfied(required: &SummaryKind, available: &SummaryKind) -> bool {
    match (summary_family(required), summary_family(available)) {
        (Some(req_family), Some(have_family)) => req_family.satisfied_by(have_family),
        _ => false,
    }
}

/// The family a [`SummaryKind`] belongs to, for [`SummaryFamilyMatcher`].
/// `None` for the exact-accumulator kinds, which aren't grouped into
/// families (see [`SummaryFamilyMatcher::is_satisfied_by`]'s doc).
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

fn summary_family(kind: &SummaryKind) -> Option<SummaryFamily> {
    match kind {
        SummaryKind::Kll | SummaryKind::DDSketch => Some(SummaryFamily::Quantile),
        SummaryKind::Hll | SummaryKind::Theta | SummaryKind::Kmv => {
            Some(SummaryFamily::Cardinality)
        }
        SummaryKind::Cms | SummaryKind::CountSketch => Some(SummaryFamily::Frequency),
        SummaryKind::CmsWithHeap | SummaryKind::CountSketchWithHeap => {
            Some(SummaryFamily::FrequencyTopk)
        }
        SummaryKind::Sum
        | SummaryKind::Count
        | SummaryKind::MinMax
        | SummaryKind::Increase
        | SummaryKind::Rate => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_sketch::SummaryParams;

    /// A valid `SummaryParams` for `kind` — `is_satisfied_by` only matches
    /// on `kind`, never `params`, but the test values should still be
    /// real, constructible `(kind, params)` pairs rather than nonsense
    /// combinations (e.g. `Hll` paired with `Kll`'s params) that could
    /// never arise from real code.
    fn params_for(kind: &SummaryKind) -> SummaryParams {
        match kind {
            SummaryKind::Sum => SummaryParams::Sum,
            SummaryKind::Count => SummaryParams::Count,
            SummaryKind::MinMax => SummaryParams::MinMax,
            SummaryKind::Increase => SummaryParams::Increase,
            SummaryKind::Rate => SummaryParams::Rate,
            SummaryKind::Kll => SummaryParams::Kll { k: 200 },
            SummaryKind::Cms => SummaryParams::Cms {
                width: 100,
                depth: 5,
            },
            SummaryKind::Hll => SummaryParams::Hll { precision: 14 },
            SummaryKind::DDSketch => SummaryParams::DDSketch { alpha: 0.01 },
            SummaryKind::CmsWithHeap => SummaryParams::CmsWithHeap {
                width: 100,
                depth: 5,
                heap_size: 10,
            },
            SummaryKind::Kmv => SummaryParams::Kmv { k: 1024 },
            SummaryKind::Theta => SummaryParams::Theta { k: 1024 },
            SummaryKind::CountSketch => SummaryParams::CountSketch {
                width: 100,
                depth: 5,
            },
            SummaryKind::CountSketchWithHeap => SummaryParams::CountSketchWithHeap {
                width: 100,
                depth: 5,
                heap_size: 10,
            },
        }
    }

    fn sketch(kind: SummaryKind) -> Implementation {
        let params = params_for(&kind);
        Implementation::Summary { kind, params }
    }

    fn accumulator(kind: SummaryKind) -> Implementation {
        let params = params_for(&kind);
        Implementation::Summary { kind, params }
    }

    #[test]
    fn same_sketch_kind_satisfies_itself() {
        let m = SummaryFamilyMatcher;
        assert!(m.is_satisfied_by(&sketch(SummaryKind::Kll), &sketch(SummaryKind::Kll)));
        assert!(m.is_satisfied_by(&sketch(SummaryKind::Hll), &sketch(SummaryKind::Hll)));
    }

    #[test]
    fn same_family_alternate_kind_satisfies() {
        let m = SummaryFamilyMatcher;
        // Kll / DDSketch are interchangeable quantile answers.
        assert!(m.is_satisfied_by(&sketch(SummaryKind::Kll), &sketch(SummaryKind::DDSketch)));
        // Hll / Theta / Kmv are interchangeable cardinality answers.
        assert!(m.is_satisfied_by(&sketch(SummaryKind::Hll), &sketch(SummaryKind::Theta)));
        assert!(m.is_satisfied_by(&sketch(SummaryKind::Hll), &sketch(SummaryKind::Kmv)));
    }

    #[test]
    fn cross_family_never_satisfies() {
        let m = SummaryFamilyMatcher;
        assert!(!m.is_satisfied_by(&sketch(SummaryKind::Kll), &sketch(SummaryKind::Hll)));
        assert!(!m.is_satisfied_by(&sketch(SummaryKind::Hll), &sketch(SummaryKind::Kll)));
        assert!(!m.is_satisfied_by(&sketch(SummaryKind::Kll), &sketch(SummaryKind::Cms)));
    }

    #[test]
    fn heap_bearing_available_satisfies_bare_frequency_required() {
        let m = SummaryFamilyMatcher;
        // A CmsWithHeap instance already carries the plain CMS matrix, so
        // it answers a bare frequency point-query too.
        assert!(m.is_satisfied_by(&sketch(SummaryKind::Cms), &sketch(SummaryKind::CmsWithHeap)));
        assert!(m.is_satisfied_by(
            &sketch(SummaryKind::CountSketch),
            &sketch(SummaryKind::CountSketchWithHeap)
        ));
    }

    #[test]
    fn bare_frequency_available_does_not_satisfy_topk_required() {
        let m = SummaryFamilyMatcher;
        // The reverse does not hold: a heap-less sketch never tracked the
        // heavy-hitter heap, so it cannot enumerate top-k items.
        assert!(!m.is_satisfied_by(&sketch(SummaryKind::CmsWithHeap), &sketch(SummaryKind::Cms)));
        assert!(!m.is_satisfied_by(
            &sketch(SummaryKind::CountSketchWithHeap),
            &sketch(SummaryKind::CountSketch)
        ));
    }

    #[test]
    fn cms_and_count_sketch_are_the_same_frequency_family() {
        let m = SummaryFamilyMatcher;
        assert!(m.is_satisfied_by(&sketch(SummaryKind::Cms), &sketch(SummaryKind::CountSketch)));
        assert!(m.is_satisfied_by(
            &sketch(SummaryKind::CmsWithHeap),
            &sketch(SummaryKind::CountSketchWithHeap)
        ));
    }

    #[test]
    fn exact_accumulator_requires_the_exact_same_kind() {
        let m = SummaryFamilyMatcher;
        assert!(m.is_satisfied_by(
            &accumulator(SummaryKind::Sum),
            &accumulator(SummaryKind::Sum)
        ));
        assert!(!m.is_satisfied_by(
            &accumulator(SummaryKind::Sum),
            &accumulator(SummaryKind::MinMax)
        ));
        assert!(!m.is_satisfied_by(
            &accumulator(SummaryKind::Increase),
            &accumulator(SummaryKind::Rate)
        ));
    }

    #[test]
    fn sketch_and_accumulator_never_satisfy_each_other() {
        let m = SummaryFamilyMatcher;
        assert!(!m.is_satisfied_by(&sketch(SummaryKind::Kll), &accumulator(SummaryKind::Sum)));
        assert!(!m.is_satisfied_by(&accumulator(SummaryKind::Sum), &sketch(SummaryKind::Kll)));
    }

    #[test]
    fn pass_through_required_is_vacuously_satisfied() {
        let m = SummaryFamilyMatcher;
        assert!(m.is_satisfied_by(&Implementation::PassThrough, &sketch(SummaryKind::Kll)));
        assert!(m.is_satisfied_by(&Implementation::PassThrough, &accumulator(SummaryKind::Sum)));
        assert!(m.is_satisfied_by(&Implementation::PassThrough, &Implementation::PassThrough));
    }

    #[test]
    fn pass_through_available_never_satisfies_a_real_requirement() {
        let m = SummaryFamilyMatcher;
        assert!(!m.is_satisfied_by(&sketch(SummaryKind::Kll), &Implementation::PassThrough));
        assert!(!m.is_satisfied_by(&accumulator(SummaryKind::Sum), &Implementation::PassThrough));
    }

    // ── sketch_family_satisfied (the bare-kind entry point) ──────────────

    #[test]
    fn sketch_family_satisfied_matches_is_satisfied_by_on_the_sketch_arm() {
        // The free function is meant to be exactly the logic
        // `SummaryFamilyMatcher::is_satisfied_by` applies to its `Sketch`
        // arm, just without needing `SummaryParams` to call it.
        assert!(sketch_family_satisfied(
            &SummaryKind::Kll,
            &SummaryKind::DDSketch
        ));
        assert!(sketch_family_satisfied(
            &SummaryKind::Cms,
            &SummaryKind::CmsWithHeap
        ));
        assert!(!sketch_family_satisfied(
            &SummaryKind::CmsWithHeap,
            &SummaryKind::Cms
        ));
        assert!(!sketch_family_satisfied(
            &SummaryKind::Kll,
            &SummaryKind::Hll
        ));
    }

    #[test]
    fn sketch_family_satisfied_rejects_exact_accumulator_kinds() {
        // Exact-accumulator kinds have no family (`summary_family` returns
        // `None` for them) — never satisfied by anything via this path.
        assert!(!sketch_family_satisfied(
            &SummaryKind::Sum,
            &SummaryKind::Sum
        ));
        assert!(!sketch_family_satisfied(
            &SummaryKind::Kll,
            &SummaryKind::Sum
        ));
    }
}
