//! L5 stage allocator — colours a `PhysicalExpr` DAG by `StageId`.
//!
//! Per `control_plane/docs/design.md` §6 (line ~810):
//!
//! ```ignore
//! // generic stage allocator — given a PhysicalExpr tree + a topology, decide which
//! // ops land on which stage subject to constraints. Stage-level only; per-executor
//! // fan-out happens in the deployment model's PhysicalPlanner using the executor
//! // list from `DeploymentConstraints::executors()`.
//! pub struct StageAllocator;
//! impl StageAllocator {
//!     pub fn allocate<T: TopologyDescriptor>(
//!         &self, exprs: &[QueryExpr], topology: &T, c: &DeploymentConstraints,
//!     ) -> Result<Vec<StageAssignment>, PlanError>;
//! }
//! ```
//!
//! Phase E surfaces the `Topology::ThreeStage` colouring; the rules
//! mirror design.md §6 batched-queries example (line ~1380):
//!
//! | Node | StageId | Why |
//! |---|---|---|
//! | `Logical(Scan)` | Edge | scrape happens at the agent host |
//! | `Logical(Window)` | Edge | windowing at edge keeps bandwidth low |
//! | `SketchAgg` | Edge | sketch building at the edge — the bandwidth claim |
//! | `Logical(Aggregate{exact})` over `Window` | Edge | per-row state; same logic as `SketchAgg` |
//! | `SketchMerge` | Gateway | merge edge sketches across hosts |
//! | `Logical(Aggregate{exact})` over `Merge`-shape | Backend | final readout (root of q3) |
//! | `SketchEstimate` | Backend | the query-readout side |
//! | `LetBinding` / `Ref` | (color of bound expr) | scope-resolved |
//!
//! The allocator does not own constraint logic (no memory budget / cost
//! threshold inputs in Phase E) — those come back as Phase G's
//! `DeploymentConstraints` plumbing. The Phase E colouring is purely
//! structural per the design.md table.

#![allow(dead_code)]

use std::collections::HashMap;

use crate::physical::colored_dag::dag::{ColoredDag, ColoredNode, NodeId};
use crate::physical::colored_dag::stage_id::{StageId, Topology};
use crate::sketch_algebra::PhysicalExpr;
use crate::types_v2::BindingName;

/// Errors surfaced by [`StageAllocator::allocate`].
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum AllocateError {
    /// The supplied topology is not implemented in Phase E.
    #[error("unsupported topology in Phase E (only ThreeStage is implemented): {0:?}")]
    UnsupportedTopology(Topology),
    /// `Ref(name)` did not resolve against any in-scope `LetBinding`.
    #[error("unresolved Ref: {0}")]
    UnresolvedRef(String),
}

/// L5 stage allocator. Stateless — Phase E exposes a unit struct so the
/// API matches design.md (`pub struct StageAllocator;`).
#[derive(Debug, Default, Clone, Copy)]
pub struct StageAllocator;

impl StageAllocator {
    /// Colour `expr` against `topology`. Returns the colored DAG ready
    /// for emitter consumption.
    ///
    /// Phase E only implements `Topology::ThreeStage`; other variants
    /// return [`AllocateError::UnsupportedTopology`].
    pub fn allocate(
        &self,
        expr: &PhysicalExpr,
        topology: Topology,
    ) -> Result<ColoredDag, AllocateError> {
        match topology {
            Topology::ThreeStage => {
                let mut walker = ThreeStageWalker::default();
                walker.dag.topology = topology;
                walker.visit(expr)?;
                Ok(walker.dag)
            }
            other => Err(AllocateError::UnsupportedTopology(other)),
        }
    }
}

// ── Three-stage colouring walker ──────────────────────────────────────────────

#[derive(Default)]
struct ThreeStageWalker {
    dag: ColoredDag,
    /// Lexical scope: binding name → colored stage of the bound expression's
    /// root.
    scope: HashMap<String, StageId>,
}

impl ThreeStageWalker {
    /// Recursively visit `expr`, append its colored node to the DAG,
    /// and return its `(NodeId, StageId)`.
    fn visit(&mut self, expr: &PhysicalExpr) -> Result<(NodeId, StageId), AllocateError> {
        // Reserve a slot for this node up-front so child IDs are
        // strictly larger than the parent's; downstream `cut_edges`
        // analysis assumes parents come before children in `nodes`.
        let id = NodeId(self.dag.nodes.len());
        self.dag.nodes.push(ColoredNode {
            id,
            expr: expr.clone(),
            // Placeholder — overwritten below once children are coloured.
            stage: StageId::Edge,
        });

        let stage = match expr {
            // ── Logical pass-through — colour by inspecting the wrapped
            // L3 QueryExpr. `Scan` / `Window` always land on edge;
            // `Aggregate{exact}` lands on edge if its child is an edge
            // (scrape locality); `Ref` resolves through the lexical
            // scope map.
            PhysicalExpr::Logical(qe) => self.colour_logical(qe)?,

            // ── SketchAgg: always edge per design.md §6 batched-queries
            // table. The "SketchAgg whose child is a Scan MUST be on
            // Edge" invariant is automatically satisfied.
            PhysicalExpr::SketchAgg { child, .. } => {
                let (cid, _) = self.visit(child)?;
                self.dag.edges.push((id, cid));
                StageId::Edge
            }

            // ── SketchEstimate: always backend per design.md §6.
            // The "SketchEstimate MUST be on the same stage as its
            // consumers (typically Backend)" invariant is satisfied
            // because consumers above SketchEstimate are also backend.
            PhysicalExpr::SketchEstimate { child, .. } => {
                let (cid, child_stage) = self.visit(child)?;
                self.dag.edges.push((id, cid));
                // If child is on edge or gateway, this is a cross-stage
                // edge — that's expected (the wire-format hop).
                let _ = child_stage;
                StageId::Backend
            }

            // ── SketchMerge: gateway under three-stage. Children are
            // edge SketchAgg outputs.
            PhysicalExpr::SketchMerge { children, .. } => {
                for child in children {
                    let (cid, _) = self.visit(child)?;
                    self.dag.edges.push((id, cid));
                }
                StageId::Gateway
            }

            // ── LetBinding: colour by the bound expression's stage,
            // and bring the binding into scope before walking the body.
            PhysicalExpr::LetBinding { name, expr, child } => {
                let (eid, expr_stage) = self.visit(expr)?;
                self.dag.edges.push((id, eid));
                self.scope.insert(name.as_str().to_string(), expr_stage);
                let (bid, _) = self.visit(child)?;
                self.dag.edges.push((id, bid));
                expr_stage
            }

            // ── Ref: colour matches the binding's stage. Unresolved
            // refs bubble up as `AllocateError::UnresolvedRef`.
            PhysicalExpr::Ref { name } => self
                .scope
                .get(name.as_str())
                .copied()
                .ok_or_else(|| AllocateError::UnresolvedRef(name.as_str().to_string()))?,

            // ── Phase ε.1 Mode 2: raw at edge, sketch built at backend.
            // Edge ships raw OTLP — we stage as Edge so the L5 emitter's
            // edge-side YAML pipeline picks it up; the sketch construction
            // itself happens at the backend (no edge sketch processor).
            PhysicalExpr::RawAtEdgeSketchAtBackend { child, .. } => {
                let (cid, _) = self.visit(child)?;
                self.dag.edges.push((id, cid));
                StageId::Edge
            }

            // ── Phase ε.1 Mode 3: raw at edge, ships directly to
            // Prometheus's native OTLP receiver. The agent pipeline picks
            // this up via `asap.mode=prometheus_archive` routing.
            PhysicalExpr::RawAtEdgePrometheusArchive { .. } => StageId::Edge,

            // ── ExactAgg (PR-6 follow-up): same shape as SketchAgg —
            // produces typed state at the edge. The accumulator runs on
            // the edge precompute pipeline; the backend's
            // `SketchStoreSink::append_to_index` writes the final
            // (sid, window, accumulator) tuples it ships. Coloured
            // Edge to match the sketch path's locality.
            PhysicalExpr::ExactAgg { child, .. } => {
                let (cid, _) = self.visit(child)?;
                self.dag.edges.push((id, cid));
                StageId::Edge
            }
        };

        // Patch in the resolved stage now that children have been visited.
        self.dag.nodes[id.0].stage = stage;

        Ok((id, stage))
    }

    /// Colour a `Logical(QueryExpr)` node per the three-stage rules.
    /// Per design.md §6: Scan / Window → Edge; Aggregate over Window →
    /// Edge (per-row exact aggregation, e.g. `Max`); Aggregate over a
    /// gateway-coloured input → Backend (final readout root).
    /// `LetBinding`/`Ref` at the L3 level reuse the same scope map.
    fn colour_logical(
        &mut self,
        qe: &crate::intent_algebra::QueryExpr,
    ) -> Result<StageId, AllocateError> {
        use crate::intent_algebra::QueryExpr as QE;
        match qe {
            QE::Scan { .. } => Ok(StageId::Edge),
            QE::Window { .. } => Ok(StageId::Edge),
            // Aggregate at L3-in-L4: the design.md L5 table says
            // `Aggregate{exact}` (e.g. `Max`) → Edge, and the *root of
            // q3* (the same Aggregate after a SketchMerge / Merge) →
            // Backend. Phase E's Logical wrapper does not surface a
            // PhysicalExpr-level Merge over exact streams, so the L3
            // Aggregate node reachable here is always the per-window
            // edge form. Final-readout placement happens at the
            // PhysicalExpr-level (root of q3 wrapped in a SketchMerge
            // sibling structure) — Phase G+ adds an explicit
            // `Logical(Merge)` PhysicalExpr variant for the gateway hop.
            QE::Aggregate { .. } => Ok(StageId::Edge),
            // Lexical scope for L3 LetBinding / Ref — mirrors the
            // PhysicalExpr-level handling.
            QE::LetBinding { name, expr, child } => {
                let expr_stage = self.colour_logical(expr)?;
                self.scope.insert(name.as_str().to_string(), expr_stage);
                self.colour_logical(child)
            }
            QE::Ref { name } => self
                .scope
                .get(name.as_str())
                .copied()
                .ok_or_else(|| AllocateError::UnresolvedRef(name.as_str().to_string())),
            // A-variants lifted in Batch 2 of the relational migration.
            // No colored-DAG consumer constructs them today; conservatively
            // route to the Edge stage (matches the per-row Scan/Window
            // policy) so the build is total. The proper stage-placement
            // rules for Filter/Project/Partition/Distinct/Merge/Join/SetOp/
            // Sort/Limit/BinaryOp land alongside their consumers in
            // follow-up batches.
            QE::Filter { .. }
            | QE::Project { .. }
            | QE::Partition { .. }
            | QE::Distinct { .. }
            | QE::Sort { .. }
            | QE::Limit { .. }
            | QE::Subquery { .. } => Ok(StageId::Edge),
            QE::Merge { .. } | QE::Join { .. } | QE::SetOp { .. } | QE::BinaryOp { .. } => {
                Ok(StageId::Backend)
            }
        }
    }
}

// Convenience helper used by tests / external callers that only need a
// stage lookup keyed by binding name.
pub(crate) fn binding_stage(dag: &ColoredDag, name: &BindingName) -> Option<StageId> {
    dag.nodes.iter().find_map(|n| match &n.expr {
        PhysicalExpr::LetBinding { name: n2, .. } if n2 == name => Some(n.stage),
        _ => None,
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent_algebra::schema::{Column, DataType};
    use crate::intent_algebra::{QueryExpr, Schema, Source, WindowKind};
    use crate::sketch_algebra::params::{KllParams, SketchKind, SketchParams};
    use crate::sketch_algebra::physical_expr::EstimateOp;
    use std::time::Duration;

    fn ts_scan() -> QueryExpr {
        QueryExpr::Scan {
            source: Source::TimeSeries {
                metric: "http_request_duration_seconds".into(),
            },
            label_filters: vec![],
            schema: Schema::with_time_index(
                vec![
                    Column {
                        name: "ts".into(),
                        dtype: DataType::Timestamp,
                        nullable: false,
                    },
                    Column {
                        name: "value".into(),
                        dtype: DataType::Float64,
                        nullable: false,
                    },
                ],
                0,
                vec![vec![0]],
            ),
        }
    }

    fn windowed_scan() -> QueryExpr {
        QueryExpr::Window {
            kind: WindowKind::Sliding,
            size: Duration::from_secs(300),
            slide: None,
            child: Box::new(ts_scan()),
        }
    }

    #[test]
    fn allocate_unsupported_topology_errors() {
        let leaf = PhysicalExpr::Logical(ts_scan());
        let err = StageAllocator
            .allocate(&leaf, Topology::SingleStage)
            .unwrap_err();
        assert_eq!(
            err,
            AllocateError::UnsupportedTopology(Topology::SingleStage)
        );
    }

    #[test]
    fn three_stage_quantile_dag_basic() {
        let expr = PhysicalExpr::estimate_over_agg(
            EstimateOp::Quantile { q: 0.99 },
            SketchKind::Kll,
            SketchParams::Kll(KllParams { k: 200 }),
            windowed_scan(),
        );
        let dag = StageAllocator
            .allocate(&expr, Topology::ThreeStage)
            .unwrap();
        // root = SketchEstimate → Backend
        assert_eq!(dag.root().unwrap().stage, StageId::Backend);
        // node 1 = SketchAgg → Edge
        assert_eq!(dag.nodes[1].stage, StageId::Edge);
        // node 2 = Logical(Window) → Edge
        assert_eq!(dag.nodes[2].stage, StageId::Edge);
    }
}
