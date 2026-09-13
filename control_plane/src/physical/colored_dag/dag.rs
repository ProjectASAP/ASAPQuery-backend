//! Physical DAG nodes assigned to stages. [`ColoredDag`] retains parent-child
//! edges, including cross-stage edges, alongside the per-stage node buckets
//! consumed by the emitter.

#![allow(dead_code)]

use serde::{Deserialize, Serialize};

use crate::physical::colored_dag::stage_id::{StageId, Topology};
use crate::physical::post_asap::PhysicalExpr;

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
#[derive(Debug, Clone)]
pub struct ColoredNode {
    /// Position-based identifier — index into `ColoredDag::nodes`.
    pub id: NodeId,
    /// The `PhysicalExpr` node (full sub-tree as originally walked — Phase
    /// E does not strip children, so emitters can read what they need).
    pub expr: PhysicalExpr,
    /// Stage this node was painted with.
    pub stage: StageId,
}

/// Output of [`crate::physical::colored_dag::StageAllocator::allocate`].
/// The default is an empty three-stage DAG for incremental construction.
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

    #[cfg(test)]
    /// Edges crossing stage boundaries. They describe the required transport hops.
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
    use std::rc::Rc;

    use super::*;
    use crate::physical::post_asap::PhysicalExpr;
    use planner_types::pre_asap::{Column, DataType};
    use planner_types::pre_asap::{QueryExpr, Schema, Source};

    // These three dummies only need to be *structurally valid* and
    // distinct `PhysicalExpr` values — the tests below only inspect
    // `ColoredNode::stage`, never `expr`'s internal shape.
    fn dummy_scan() -> QueryExpr {
        QueryExpr::Scan {
            source: Source::TimeSeries {
                metric: "dummy_metric".into(),
            },
            predicates: vec![],
            schema: Schema::with_time_index(
                vec![
                    Column {
                        name: "ts".into(),
                        dtype: DataType::Timestamp,
                        nullable: false,
                        table: None,
                    },
                    Column {
                        name: "value".into(),
                        dtype: DataType::Float64,
                        nullable: false,
                        table: None,
                    },
                ],
                0,
                vec![vec![0]],
            ),
        }
    }

    fn dummy_logical() -> PhysicalExpr {
        PhysicalExpr::committed(crate::planner_selection::keep_pre_asap(&dummy_scan()).unwrap())
    }

    fn dummy_agg() -> PhysicalExpr {
        let q = QueryExpr::Aggregate {
            reduction: planner_types::pre_asap::Reduction::by(vec![]),
            measures: vec![planner_types::pre_asap::AggIntent::Sum { col: None }],
            output_names: Vec::new(),
            having: None,
            child: Rc::new(dummy_scan()),
        };
        PhysicalExpr::committed(crate::planner_selection::select_summary_default(&q).unwrap())
    }

    fn dummy_estimate() -> PhysicalExpr {
        let q = QueryExpr::Aggregate {
            reduction: planner_types::pre_asap::Reduction::by(vec![]),
            measures: vec![planner_types::pre_asap::AggIntent::Quantile {
                col: None,
                q: 0.99,
                accuracy: crate::types::AccuracyTarget::Epsilon(0.01),
            }],
            output_names: Vec::new(),
            having: None,
            child: Rc::new(dummy_scan()),
        };
        PhysicalExpr::committed(crate::planner_selection::select_summary_default(&q).unwrap())
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
