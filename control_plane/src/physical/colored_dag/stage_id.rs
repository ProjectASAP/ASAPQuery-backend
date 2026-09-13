//! Stage roles and deployment topologies. Roles are closed enum variants so
//! allocation matches are exhaustive. Only three-stage allocation is supported.

#![allow(dead_code)]

use serde::{Deserialize, Serialize};

/// A categorical tier, not an executor instance. Multiple executors may share
/// one role; per-executor fan-out happens downstream of stage allocation.
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

/// Deployment topology. Single-stage and zero-stage variants are reserved;
/// the allocator currently supports only `ThreeStage`.
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
