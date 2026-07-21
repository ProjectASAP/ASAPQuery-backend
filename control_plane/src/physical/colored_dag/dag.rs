//! L5 colored DAG — `PhysicalExpr` nodes painted with `StageId`.
//!
//! Per `control_plane/docs/design.md` §3 (line ~123): "A 'stage assignment'
//! is a colouring of the L4-bound `PhysicalExpr` DAG by `StageId`, with
//! sketch-merge / data-shipping nodes inserted on the cut edges."
//!
//! [`ColoredDag`] is the IR the [`crate::physical::colored_dag::Emitter`]
//! consumes. It carries:
//!
//! - the `nodes` vector (every visited `PhysicalExpr` node keyed by
//!   [`NodeId`], with its assigned `StageId`),
//! - the `edges` vector (parent → child references from the DAG walk),
//!   carried so future Phase G+ work can detect cross-stage cut edges
//!   for explicit data-shipping op insertion.
//!
//! Today the emitter consumes the per-stage *bucket* of nodes (`nodes
//! filtered by stage`) plus the root sketch-state metadata; cut-edge
//! reasoning is left as a Phase G hook (the `edges` vector is the data
//! it needs).

#![allow(dead_code)]

use serde::{Deserialize, Serialize};

use crate::physical::colored_dag::stage_id::{StageId, Topology};
use crate::sketch_algebra::PhysicalExpr;

/// Stable position-based identifier for a node within a `ColoredDag`.
/// `NodeId(0)` is the root; depth-first walk order otherwise.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeId(pub usize);

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "n{}", self.0)
    }
}

/// One entry in the colored DAG: a `PhysicalExpr` node + its assigned
/// `StageId`.
///
/// `expr` is a clone of the node's surface variant (children are NOT
/// recursively cloned — the `child` payload is replaced with a sentinel
/// to keep the colored-DAG flat; structural information lives in
/// [`ColoredDag::edges`]). Test-friendly variant: when callers want the
/// full sub-tree they can rebuild from the original `PhysicalExpr` using
/// `NodeId` as the index.
#[derive(Debug, Clone, PartialEq)]
pub struct ColoredNode {
    /// Position-based identifier — index into `ColoredDag::nodes`.
    pub id: NodeId,
    /// The `PhysicalExpr` node (full sub-tree as originally walked — Phase
    /// E does not strip children, so emitters can read what they need).
    pub expr: PhysicalExpr,
    /// Stage this node was painted with.
    pub stage: StageId,
}

/// L5 colored DAG. The output of [`crate::physical::colored_dag::StageAllocator::allocate`].
///
/// Per design.md §6: "L5 colors the DAG by `StageId` and emits
/// per-executor configs. Same `PhysicalExpr` input; topology and emitter
/// differ per deployment model."
///
/// `Default::default()` returns the empty `Topology::ThreeStage` shape —
/// that's the only topology Phase E ships, so the default is safe and
/// useful for the allocator's incremental-build path.
#[derive(Debug, Clone)]
pub struct ColoredDag {
    /// Topology this colouring was produced under.
    pub topology: Topology,
    /// Node table — depth-first walk order, root at index 0.
    pub nodes: Vec<ColoredNode>,
    /// Parent → child edges (DAG structure). Carried for future
    /// cut-edge analysis; today the emitter consumes per-stage buckets.
    pub edges: Vec<(NodeId, NodeId)>,
}

impl ColoredDag {
    /// Empty colored DAG for the supplied topology — the allocator
    /// builds the contents.
    pub fn new(topology: Topology) -> Self {
        Self {
            topology,
            nodes: Vec::new(),
            edges: Vec::new(),
        }
    }
}

impl Default for ColoredDag {
    fn default() -> Self {
        ColoredDag::new(Topology::ThreeStage)
    }
}

impl ColoredDag {
    /// Root node (the original `PhysicalExpr` root). `None` only for the
    /// degenerate empty DAG.
    pub fn root(&self) -> Option<&ColoredNode> {
        self.nodes.first()
    }

    /// All nodes painted with `stage`.
    pub fn nodes_in_stage(&self, stage: StageId) -> impl Iterator<Item = &ColoredNode> {
        self.nodes.iter().filter(move |n| n.stage == stage)
    }

    /// Set of `StageId`s actually present in this colouring (subset of
    /// `topology.stages()`).
    pub fn occupied_stages(&self) -> Vec<StageId> {
        let mut seen: Vec<StageId> = Vec::new();
        for n in &self.nodes {
            if !seen.contains(&n.stage) {
                seen.push(n.stage);
            }
        }
        seen
    }

    /// Cut edges — edges where parent and child end up on different
    /// stages. These are the wire-format hops (OpAMP push / OTLP /
    /// `StreamingConfig`) the emitter must materialize. Phase E reports
    /// them; Phase G+ inserts explicit data-shipping ops on them.
    pub fn cut_edges(&self) -> Vec<(NodeId, NodeId)> {
        self.edges
            .iter()
            .copied()
            .filter(|(p, c)| {
                let ps = self.nodes.get(p.0).map(|n| n.stage);
                let cs = self.nodes.get(c.0).map(|n| n.stage);
                match (ps, cs) {
                    (Some(a), Some(b)) => a != b,
                    _ => false,
                }
            })
            .collect()
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent_algebra::QueryExpr;
    use crate::sketch_algebra::physical_expr::EstimateOp;
    use crate::sketch_algebra::PhysicalExpr;
    use asap_sketch::{SummaryKind, SummaryParams};

    fn dummy_logical() -> PhysicalExpr {
        PhysicalExpr::Logical(QueryExpr::Ref {
            name: crate::types_v2::BindingName::new("dummy"),
        })
    }

    fn dummy_agg() -> PhysicalExpr {
        PhysicalExpr::SketchAgg {
            sketch_type: SummaryKind::Kll,
            params: SummaryParams::Kll { k: 200 },
            child: Box::new(dummy_logical()),
        }
    }

    fn dummy_estimate() -> PhysicalExpr {
        PhysicalExpr::SketchEstimate {
            op: EstimateOp::Quantile { q: 0.99 },
            child: Box::new(dummy_agg()),
        }
    }

    #[test]
    fn empty_dag_has_no_root() {
        let d = ColoredDag::new(Topology::ThreeStage);
        assert!(d.root().is_none());
        assert!(d.occupied_stages().is_empty());
        assert!(d.cut_edges().is_empty());
    }

    #[test]
    fn occupied_stages_dedupe() {
        let mut d = ColoredDag::new(Topology::ThreeStage);
        d.nodes.push(ColoredNode {
            id: NodeId(0),
            expr: dummy_estimate(),
            stage: StageId::Backend,
        });
        d.nodes.push(ColoredNode {
            id: NodeId(1),
            expr: dummy_agg(),
            stage: StageId::Edge,
        });
        d.nodes.push(ColoredNode {
            id: NodeId(2),
            expr: dummy_agg(),
            stage: StageId::Edge,
        });
        let stages = d.occupied_stages();
        assert!(stages.contains(&StageId::Edge));
        assert!(stages.contains(&StageId::Backend));
        assert_eq!(stages.len(), 2);
    }

    #[test]
    fn cut_edges_detected_across_stages() {
        let mut d = ColoredDag::new(Topology::ThreeStage);
        d.nodes.push(ColoredNode {
            id: NodeId(0),
            expr: dummy_estimate(),
            stage: StageId::Backend,
        });
        d.nodes.push(ColoredNode {
            id: NodeId(1),
            expr: dummy_agg(),
            stage: StageId::Edge,
        });
        d.edges.push((NodeId(0), NodeId(1)));
        let cut = d.cut_edges();
        assert_eq!(cut, vec![(NodeId(0), NodeId(1))]);
    }
}
