//! Layer 4 sketch-state schema.
//!
//! Per `controller/docs/design.md` §6.4 ("Per-node input/output spec for
//! `SketchExpr`", around line ~618). L4 introduces a new field type into
//! `Schema`: `DataType::Sketch(SketchKind, SketchParams)`. This module
//! defines that extension as a parallel typed layer that the L4 type
//! checker consults.
//!
//! Keeping this in a parallel struct (rather than mutating the L3
//! `intent_algebra::DataType`) avoids touching the recently-shipped L3
//! IR. The L4 type checker asks: "for this `SketchExpr` node, what is
//! the input sketch-state schema, and what is the output?" Each `Bind*`
//! rule populates an [`SketchStateSchema`] when it produces a
//! sketch-state-bearing node.
//!
//! Local-checkable invariants (per design.md §6.4 line ~643):
//! 1. **Sketch-family mismatch is a plan-time error.** A `SketchMerge`
//!    over inputs with mismatched `(kind, params)` fails at L4.
//! 2. **Catalog capability flags gate which nodes can fire.** The
//!    capability flags live on the [`SketchStateSchema`] so the type
//!    checker has them locally without re-consulting the catalog.

#![allow(dead_code)]

use serde::{Deserialize, Serialize};

use crate::sketch_algebra::params::{SketchKind, SketchParams};

/// Sketch-state schema annotation on a [`crate::sketch_algebra::SketchExpr`]
/// edge. Carries the family + params + the catalog-capability flags so
/// the L4 type checker can decide locally whether a downstream node may
/// merge / subtract / delete from the state.
///
/// Mirrors the per-node input/output spec table in design.md §6.4: a
/// `SketchAgg` produces a sketch-state schema; a `SketchEstimate`
/// consumes one and emits a regular row schema; a `SketchMerge` consumes
/// N matching sketch-state schemas and emits one of the same family.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SketchStateSchema {
    /// Sketch family of the state.
    pub kind: SketchKind,
    /// Parameter payload — must match across all inputs to a `SketchMerge`.
    pub params: SketchParams,
    /// Capability flags from the sketch catalog.
    pub caps: SketchStateMetadata,
}

/// L4 type-system catalog flags for a sketch state. Populated from the
/// sketch catalog at `Bind*`-rule time. `mergeable` gates `SketchMerge`;
/// `subtractable` gates `SketchSubtract`; `deletable` gates
/// `SketchDelete`. See design.md §6 line ~646 ("catalog is the single
/// source of truth for these flags; binding rules consult it before
/// producing the node").
///
/// Renamed from `SketchCapabilities` in May 2026 to disambiguate from
/// [`crate::sketch_algebra::capability::SketchCapability`] (perf /
/// cost-model profile). The two structs live side-by-side: this one is
/// the **L4 type-system / plan-time** surface — sealed onto every
/// `SketchExpr` edge by the binding rule and consulted by the type
/// checker. `SketchCapability` is the **perf / feasibility / intent-
/// routing** surface — consumed by the optimizer and cost model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SketchStateMetadata {
    /// Whether two states of this family + params can be unioned —
    /// catalog default for KLL / HLL / DDSketch / CMS / CountSketch.
    pub mergeable: bool,
    /// Whether `SketchSubtract` is meaningful for this family — true for
    /// CMS / count-based sketches, false for KLL / HLL / DDSketch.
    pub subtractable: bool,
    /// Whether `SketchDelete` is meaningful — true for deletable Bloom
    /// filters and CMS (with -1 update); false for KLL / HLL / DDSketch.
    pub deletable: bool,
}

impl SketchStateSchema {
    /// Construct the catalog-default schema for a given family + params.
    /// Capability flags follow the design.md §6 catalog defaults; if a
    /// future catalog reorder changes them, this is the single point of
    /// truth that downstream rules must consult.
    pub fn for_kind(kind: SketchKind, params: SketchParams) -> Self {
        let caps = match kind {
            SketchKind::Kll => SketchStateMetadata {
                mergeable: true,
                subtractable: false,
                deletable: false,
            },
            SketchKind::DDSketch => SketchStateMetadata {
                mergeable: true,
                subtractable: false,
                deletable: false,
            },
            SketchKind::Hll => SketchStateMetadata {
                mergeable: true,
                subtractable: false,
                deletable: false,
            },
            SketchKind::Cms => SketchStateMetadata {
                mergeable: true,
                subtractable: true,
                deletable: true,
            },
            SketchKind::CountSketch => SketchStateMetadata {
                mergeable: true,
                subtractable: true,
                deletable: false,
            },
        };
        SketchStateSchema { kind, params, caps }
    }

    /// Whether two sketch-state schemas can be `SketchMerge`-d. Per
    /// design.md §6.4 invariant 1: the family + params must match
    /// exactly, and the family must be `mergeable`.
    pub fn is_compatible_for_merge(&self, other: &Self) -> bool {
        self.kind == other.kind && self.params == other.params && self.caps.mergeable
    }

    /// Whether two sketch-state schemas can be `SketchSubtract`-ed.
    pub fn is_compatible_for_subtract(&self, other: &Self) -> bool {
        self.kind == other.kind && self.params == other.params && self.caps.subtractable
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sketch_algebra::params::{CmsParams, KllParams};

    #[test]
    fn kll_default_caps() {
        let s =
            SketchStateSchema::for_kind(SketchKind::Kll, SketchParams::Kll(KllParams { k: 200 }));
        assert!(s.caps.mergeable);
        assert!(!s.caps.subtractable);
        assert!(!s.caps.deletable);
    }

    #[test]
    fn cms_supports_subtract_and_delete() {
        let s = SketchStateSchema::for_kind(
            SketchKind::Cms,
            SketchParams::Cms(CmsParams { w: 2048, d: 5 }),
        );
        assert!(s.caps.mergeable);
        assert!(s.caps.subtractable);
        assert!(s.caps.deletable);
    }

    #[test]
    fn merge_compatibility_requires_matching_params() {
        let a =
            SketchStateSchema::for_kind(SketchKind::Kll, SketchParams::Kll(KllParams { k: 200 }));
        let b =
            SketchStateSchema::for_kind(SketchKind::Kll, SketchParams::Kll(KllParams { k: 200 }));
        let c =
            SketchStateSchema::for_kind(SketchKind::Kll, SketchParams::Kll(KllParams { k: 400 }));
        assert!(a.is_compatible_for_merge(&b));
        assert!(!a.is_compatible_for_merge(&c)); // different k
    }

    #[test]
    fn merge_compatibility_rejects_family_mismatch() {
        let kll =
            SketchStateSchema::for_kind(SketchKind::Kll, SketchParams::Kll(KllParams { k: 200 }));
        let cms = SketchStateSchema::for_kind(
            SketchKind::Cms,
            SketchParams::Cms(CmsParams { w: 2048, d: 5 }),
        );
        assert!(!kll.is_compatible_for_merge(&cms));
    }
}
