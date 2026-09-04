//! Inverse of [`capability_for`](crate::physical::runtime_capability::capability_for):
//! given a required [`Capability`], enumerate the concrete [`SketchType`]
//! families that can satisfy it.
//!
//! This is the keystone of autonomous allocation. `capability_for` lowers an
//! `AggIntent` (parsed from a query) to the *capability* it needs; this module
//! closes the loop by naming the *sketches* that provide that capability, so a
//! planner handed a set of queries can decide which sketches to allocate
//! without a hand-written workload YAML.
//!
//! Policy encoded here (matches `Capability::is_satisfied_by` on the matching
//! side, `physical::runtime_capability`):
//!   * `QuantileApprox`   → DDSketch | KLL
//!   * `CardinalityApprox`→ HLL
//!   * `FrequencyEstimate`→ CountSketch | CountMinSketch   (heap-less point query)
//!   * `FrequencyTopk`    → CountSketch | CountMinSketch   (heap layered on the same matrix)
//!   * `ExactAgg`         → ∅  (served by the exact-aggregation / archive path, not a sketch)
//!
//! When a capability binds a *concrete* `SketchAlgorithm` (not `Any`), the
//! result is exactly that one family; `Any` expands to the full candidate set.
//!
//! Moved out of `physical::post_asap` (Stage 4 of the `physical::post_asap`
//! re-layering) — this is a query-planning concern (its one caller is
//! [`crate::query_planning`]), not L4 IR.

use crate::physical::runtime_capability::{Capability, SketchAlgorithm};
use crate::types::SketchType;

/// Map a sketch handle to the allocatable control-plane [`SketchType`].
///
/// Heap-bearing dispatch hints collapse to their underlying matrix family
/// (`CmsWithHeap` → CountMinSketch, `CountSketchWithHeap` → CountSketch) — the
/// heap is an allocation detail layered on the same sketch, not a distinct
/// allocatable family. `Any` is an analysis-time wildcard with no single
/// concrete family, so it returns `None`.
pub fn sketch_type_for_algorithm(h: SketchAlgorithm) -> Option<SketchType> {
    match h {
        SketchAlgorithm::DDSketch => Some(SketchType::DDSketch),
        SketchAlgorithm::Kll => Some(SketchType::KLL),
        SketchAlgorithm::Hll => Some(SketchType::HLL),
        SketchAlgorithm::CountSketch | SketchAlgorithm::CountSketchWithHeap => {
            Some(SketchType::CountSketch)
        }
        SketchAlgorithm::Cms | SketchAlgorithm::CmsWithHeap => Some(SketchType::CountMinSketch),
        SketchAlgorithm::Kmv | SketchAlgorithm::Theta => None,
    }
}

/// The sketch families that satisfy a required [`Capability`].
///
/// A concrete handle pins exactly one family; `Any` expands to the capability
/// class's full candidate set. `ExactAgg` returns an empty vec — it is served
/// by the exact-aggregation / archive path, so "allocate a sketch for it" is a
/// no-op (the caller routes it to cold/exact instead).
pub fn sketch_families_for_capability(cap: &Capability) -> Vec<SketchType> {
    match cap {
        Capability::QuantileApprox(h) => concrete_or(h, &[SketchType::DDSketch, SketchType::KLL]),
        Capability::CardinalityApprox => vec![SketchType::HLL],
        Capability::FrequencyEstimate(h) | Capability::FrequencyTopk(h) => {
            concrete_or(h, &[SketchType::CountSketch, SketchType::CountMinSketch])
        }
        Capability::ExactAgg(_) => Vec::new(),
    }
}

/// Union the sketch families required by a set of capabilities, de-duplicated
/// and order-stable (first appearance wins). This is what a planner calls
/// after lowering a query set to capabilities: the result is the set of
/// sketches that, allocated together, answer every query in the set.
pub fn required_sketches_for_capabilities<'a, I>(caps: I) -> Vec<SketchType>
where
    I: IntoIterator<Item = &'a Capability>,
{
    let mut out: Vec<SketchType> = Vec::new();
    for cap in caps {
        for fam in sketch_families_for_capability(cap) {
            if !out.contains(&fam) {
                out.push(fam);
            }
        }
    }
    out
}

fn concrete_or(h: &Option<SketchAlgorithm>, any_set: &[SketchType]) -> Vec<SketchType> {
    match h.clone().and_then(sketch_type_for_algorithm) {
        Some(t) => vec![t],
        None => any_set.to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_types::AggregationType;

    #[test]
    fn quantile_any_expands_to_ddsketch_and_kll() {
        let fams = sketch_families_for_capability(&Capability::QuantileApprox(None));
        assert_eq!(fams, vec![SketchType::DDSketch, SketchType::KLL]);
    }

    #[test]
    fn quantile_concrete_handle_pins_one_family() {
        assert_eq!(
            sketch_families_for_capability(&Capability::QuantileApprox(Some(
                SketchAlgorithm::DDSketch
            ))),
            vec![SketchType::DDSketch]
        );
        assert_eq!(
            sketch_families_for_capability(&Capability::QuantileApprox(Some(SketchAlgorithm::Kll))),
            vec![SketchType::KLL]
        );
    }

    #[test]
    fn cardinality_is_hll() {
        assert_eq!(
            sketch_families_for_capability(&Capability::CardinalityApprox),
            vec![SketchType::HLL]
        );
    }

    #[test]
    fn frequency_families_and_heap_collapse() {
        // bare frequency: Any -> both matrix families
        assert_eq!(
            sketch_families_for_capability(&Capability::FrequencyEstimate(None)),
            vec![SketchType::CountSketch, SketchType::CountMinSketch]
        );
        // heap-bearing handles collapse to their matrix family
        assert_eq!(
            sketch_families_for_capability(&Capability::FrequencyTopk(Some(
                SketchAlgorithm::CmsWithHeap
            ))),
            vec![SketchType::CountMinSketch]
        );
        assert_eq!(
            sketch_families_for_capability(&Capability::FrequencyTopk(Some(
                SketchAlgorithm::CountSketchWithHeap
            ))),
            vec![SketchType::CountSketch]
        );
    }

    #[test]
    fn exact_agg_allocates_no_sketch() {
        assert!(
            sketch_families_for_capability(&Capability::ExactAgg(AggregationType::Sum)).is_empty()
        );
    }

    #[test]
    fn union_dedups_and_preserves_order() {
        // a query set needing {quantile, cardinality, quantile-again} ->
        // DDSketch, KLL, HLL with no duplicate DDSketch/KLL.
        let caps = vec![
            Capability::QuantileApprox(None),
            Capability::CardinalityApprox,
            Capability::QuantileApprox(Some(SketchAlgorithm::DDSketch)),
        ];
        let got = required_sketches_for_capabilities(&caps);
        assert_eq!(
            got,
            vec![SketchType::DDSketch, SketchType::KLL, SketchType::HLL]
        );
    }
}
