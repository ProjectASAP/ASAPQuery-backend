//! L5 stage identifiers + topology descriptors.
//!
//! Per `control_plane/docs/design.md` §6 `core::physical` (around line ~765):
//!
//! - [`StageId`] — the categorical *tier* in the data lifecycle. Topology
//!   declares which stages exist; allocator paints `PhysicalExpr` nodes
//!   with one of these.
//! - [`Topology`] — the deployment-model topology shape. Phase E surfaces
//!   only [`Topology::ThreeStage`] (DC lifecycle: edge → gateway →
//!   backend) per the orchestrator's scope reduction. Single-stage and
//!   zero-stage are reserved for asap-query / asap-fusion deployments
//!   not in this phase.
//!
//! `StageId` is intentionally an enum (not the `pub struct StageId(pub
//! String)` shape from design.md line 787): Phase E ships only the
//! 3-stage DC topology, so the variants are closed and exhaustive
//! `match`es catch typos at compile time. The string-shaped form from
//! design.md is the right surface once a deployment model registers a
//! custom stage role; until that happens, the enum is the safer choice.

#![allow(dead_code)]

use serde::{Deserialize, Serialize};

/// L5 stage role — a categorical tier in the data lifecycle.
///
/// Per design.md §3 (line ~120): "Stage (`StageId`) — a categorical tier
/// in the data lifecycle (edge / gateway / backend / in-process). The
/// topology declares which stages exist (3-stage / 1-stage / 0-stage).
/// Stages are roles, not instances."
///
/// Multiple `Executor`s may share a `StageId` (e.g. a 50-host edge fleet
/// has 50 executors all carrying `StageId::Edge`). Phase E operates at
/// stage granularity; per-executor fan-out is downstream (Phase G+).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StageId {
    /// Edge / agent collector — per-host scrape + per-host sketch
    /// building. Bandwidth claim: KLL / DDSketch / HLL build at the
    /// edge ships state across the cut, not raw samples.
    Edge,
    /// Gateway / aggregation collector — receives N edge streams and
    /// merges them. `SketchMerge` lives here under `Topology::ThreeStage`.
    Gateway,
    /// Backend / readout — query-engine wiring; `SketchEstimate` final
    /// readouts; final aggregation roots.
    Backend,
}

impl StageId {
    /// Stable lowercase identifier for diagnostics + emitter routing
    /// keys. Matches `crate::opamp::AgentRole` strings where possible
    /// (`"agent"` ↔ `Edge`; `"backend"` ↔ `Backend`).
    pub fn as_str(&self) -> &'static str {
        match self {
            StageId::Edge => "edge",
            StageId::Gateway => "gateway",
            StageId::Backend => "backend",
        }
    }
}

impl std::fmt::Display for StageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Deployment-model topology descriptor.
///
/// Per design.md §6 `core::physical::topology` (line ~779):
/// ```ignore
/// pub mod topology {
///     pub struct ThreeStage { /* edge → gateway → backend */ }
///     pub struct SingleStage { /* backend-only */ }
///     pub struct ZeroStage;   /* in-process */
/// }
/// ```
///
/// Phase E surfaces only `ThreeStage` (DC lifecycle scope-reduction);
/// the other variants are reserved as future-proofing — adding them is
/// purely additive.
///
/// (`clippy::enum_variant_names` is silenced — the `*Stage` suffix is
/// part of the design.md naming, not a typo carrying redundant
/// prefix.)
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Topology {
    /// DC lifecycle topology: edge → gateway → backend.
    ThreeStage,
    /// Reserved for asap-query (backend-only).
    SingleStage,
    /// Reserved for asap-fusion (in-process).
    ZeroStage,
}

impl Topology {
    /// The set of `StageId`s declared by this topology, in pipeline
    /// flow order (upstream first). The allocator uses this to validate
    /// every node lands on a declared stage.
    pub fn stages(&self) -> &'static [StageId] {
        match self {
            Topology::ThreeStage => &[StageId::Edge, StageId::Gateway, StageId::Backend],
            Topology::SingleStage => &[StageId::Backend],
            Topology::ZeroStage => &[],
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stage_id_strings_are_stable() {
        assert_eq!(StageId::Edge.as_str(), "edge");
        assert_eq!(StageId::Gateway.as_str(), "gateway");
        assert_eq!(StageId::Backend.as_str(), "backend");
    }

    #[test]
    fn three_stage_topology_lists_three_stages_in_order() {
        let s = Topology::ThreeStage.stages();
        assert_eq!(s, &[StageId::Edge, StageId::Gateway, StageId::Backend]);
    }

    #[test]
    fn single_and_zero_stage_topology_shapes() {
        assert_eq!(Topology::SingleStage.stages(), &[StageId::Backend]);
        assert!(Topology::ZeroStage.stages().is_empty());
    }

    #[test]
    fn stage_id_serde_roundtrip() {
        for s in [StageId::Edge, StageId::Gateway, StageId::Backend] {
            let json = serde_json::to_string(&s).unwrap();
            let back: StageId = serde_json::from_str(&json).unwrap();
            assert_eq!(s, back);
        }
    }
}
