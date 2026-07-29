//! Sketch-family compatibility — "is an available `SummaryKind` an
//! acceptable substitute for a required one."
//!
//! Retirement note (2026-07): this file originally also defined
//! `SummaryFamilyMatcher`, a concrete `impl asap_plan::Matcher` — the
//! reference downstream implementation of `asap_plan::boundary::Matcher`,
//! a trait with no default implementation and no shipped instance
//! upstream (deliberately: which `Implementation`s are actually
//! *available* anywhere is a downstream deployment's concern). It was
//! never actually passed to any `asap_plan` API expecting `impl Matcher`
//! / `dyn Matcher` — re-verified against the current pinned rev, no
//! function signature in `asap-plan` takes one — so it was removed as
//! dead code. Recoverable from git history
//! (`chore/retire-tier2-scaffolding`, 2026-07) if a real `Matcher`
//! consumer appears upstream.
//!
//! What's left, [`sketch_family_satisfied`], is the load-bearing part:
//! the actual family-compatibility algorithm the (now-removed) `Matcher`
//! impl's `Sketch` arm delegated to, exposed directly for callers that
//! only have bare kinds (no `asap_sketch::SummaryParams`) to compare.
//! `control_plane::sketch_algebra::capability::Capability::is_satisfied_by`
//! is its real caller: its `SketchKindHandle` query-side dispatch tag
//! never carries params, so constructing a full
//! `Implementation::Sketch{kind, params}` just to discard the params
//! would mean fabricating meaningless param values.
//!
//! It deliberately does **not** attempt the single-vs-multi-population
//! re-aggregation question (e.g. "can a keyed `Sum` accumulator serve an
//! unkeyed `Sum` query") — grouping lives beside the kind, on whatever
//! node carries it, not inside the kind (see
//! `crates/asap_types/src/key_by_label_names.rs`'s module doc for the
//! same design call made on the data-plane side). A caller needing that
//! richer, grouping-aware answer must check `AggregationType`
//! compatibility (analogous to `SummaryFamily` here) *and*
//! `grouping_labels` subset-compatibility side by side, composed at the
//! call site.

use asap_sketch::SummaryKind;

/// Pure `SummaryKind`-to-`SummaryKind` family-compatibility check.
pub fn sketch_family_satisfied(required: &SummaryKind, available: &SummaryKind) -> bool {
    match (summary_family(required), summary_family(available)) {
        (Some(req_family), Some(have_family)) => req_family.satisfied_by(have_family),
        _ => false,
    }
}

/// The family a [`SummaryKind`] belongs to. `None` for the
/// exact-accumulator kinds, which aren't grouped into families.
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

    #[test]
    fn same_family_alternate_kind_satisfies() {
        // Kll / DDSketch are interchangeable quantile answers.
        assert!(sketch_family_satisfied(&SummaryKind::Kll, &SummaryKind::DDSketch));
        // Hll / Theta / Kmv are interchangeable cardinality answers.
        assert!(sketch_family_satisfied(&SummaryKind::Hll, &SummaryKind::Theta));
        assert!(sketch_family_satisfied(&SummaryKind::Hll, &SummaryKind::Kmv));
    }

    #[test]
    fn cross_family_never_satisfies() {
        assert!(!sketch_family_satisfied(&SummaryKind::Kll, &SummaryKind::Hll));
        assert!(!sketch_family_satisfied(&SummaryKind::Hll, &SummaryKind::Kll));
        assert!(!sketch_family_satisfied(
            &SummaryKind::Kll,
            &SummaryKind::Cms
        ));
    }

    #[test]
    fn heap_bearing_available_satisfies_bare_frequency_required() {
        // A CmsWithHeap instance already carries the plain CMS matrix, so
        // it answers a bare frequency point-query too.
        assert!(sketch_family_satisfied(
            &SummaryKind::Cms,
            &SummaryKind::CmsWithHeap
        ));
        assert!(sketch_family_satisfied(
            &SummaryKind::CountSketch,
            &SummaryKind::CountSketchWithHeap
        ));
    }

    #[test]
    fn bare_frequency_available_does_not_satisfy_topk_required() {
        // The reverse does not hold: a heap-less sketch never tracked the
        // heavy-hitter heap, so it cannot enumerate top-k items.
        assert!(!sketch_family_satisfied(
            &SummaryKind::CmsWithHeap,
            &SummaryKind::Cms
        ));
        assert!(!sketch_family_satisfied(
            &SummaryKind::CountSketchWithHeap,
            &SummaryKind::CountSketch
        ));
    }

    #[test]
    fn cms_and_count_sketch_are_the_same_frequency_family() {
        assert!(sketch_family_satisfied(
            &SummaryKind::Cms,
            &SummaryKind::CountSketch
        ));
        assert!(sketch_family_satisfied(
            &SummaryKind::CmsWithHeap,
            &SummaryKind::CountSketchWithHeap
        ));
    }

    #[test]
    fn rejects_exact_accumulator_kinds() {
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
